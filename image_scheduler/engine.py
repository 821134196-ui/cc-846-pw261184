"""仿真引擎：时间步进、源选择、EDF 调度、带宽分配与重调度。

关键不变量（UI 会实时校验，tests/ 也会断言）：
  1. 每个 layer 全局最多有 1 个 registry 回源传输（同层其余节点 WAIT_SEED，
     首个种子落地后改走 P2P）—— 共享层在仓库出口上只传一次。
  2. LOCAL 命中不产生任何传输字节。
  3. 所有在途流共享的资源（仓库出口 / 机架上联 / 机架 LAN / 节点入带宽）
     任一时刻总分配不超过容量。
"""

from __future__ import annotations

import threading
from collections import defaultdict
from dataclasses import dataclass, field

from .model import (
    Image,
    JobState,
    Node,
    PullJob,
    Rack,
    Registry,
    Source,
    TaskState,
    TransferRecord,
    TransferTask,
)
from .topology import Scenario

HISTORY_LIMIT = 600
EVENT_LIMIT = 200


@dataclass
class LayerLedger:
    layer_id: str
    size_mb: float
    registry_pulls: int = 0
    peer_rack_pulls: int = 0
    peer_cross_pulls: int = 0
    local_hits: int = 0
    nodes_with: set[str] = field(default_factory=set)
    registry_bytes_mb: float = 0.0
    peer_bytes_mb: float = 0.0


class SimEngine:
    def __init__(self, scenario: Scenario):
        self._lock = threading.RLock()
        self.scenario = scenario
        self.topo = scenario.topology
        self.tick = 0
        self.jobs: dict[str, PullJob] = {}
        self.tasks: dict[str, TransferTask] = {}
        self.records: list[TransferRecord] = []
        self.events: list[dict] = []
        self.ledger: dict[str, LayerLedger] = {
            lid: LayerLedger(lid, layer.size_mb)
            for lid, layer in self.topo.layers.items()
        }
        self.util_history: list[dict] = []
        self._job_seq = 0
        self._task_seq = 0
        self._scheduled_jobs: dict[int, list] = defaultdict(list)
        self._scheduled_registry: dict[int, float] = {}
        self._reschedule_logged: set[str] = set()

        # 预置缓存计入 ledger
        for nid, layer_ids in scenario.preseed.items():
            node = self.topo.nodes[nid]
            for lid in layer_ids:
                node.cache.add(lid)
                self.ledger[lid].nodes_with.add(nid)
                self.ledger[lid].local_hits += 1

        for offset, factor in scenario.registry_events:
            self._scheduled_registry[offset] = factor
        for spec in scenario.jobs:
            self._scheduled_jobs[spec.submit_offset].append(spec)

    # ------------------------------------------------------------------ 对外 API

    def step(self) -> dict:
        """推进一个 tick，返回本 tick 的摘要快照。"""
        with self._lock:
            self._step_locked()
            return self.snapshot()

    def set_registry_factor(self, factor: float, reason: str = "手动调整") -> None:
        with self._lock:
            self._apply_registry_factor(factor, reason)

    def add_job(self, node_id: str, image_name: str, deadline_offset: int) -> str:
        with self._lock:
            from .topology import JobSpec
            return self._submit_job(
                JobSpec(node_id, image_name, deadline_offset), self.tick
            )

    def is_done(self) -> bool:
        with self._lock:
            return all(
                j.state in (JobState.DONE_ON_TIME, JobState.DONE_LATE)
                for j in self.jobs.values()
            ) and not self._scheduled_jobs

    def snapshot(self) -> dict:
        """供 Web/UI 使用的全量只读状态。"""
        with self._lock:
            return self._snapshot_locked()

    # ------------------------------------------------------------------ tick 主流程

    def _step_locked(self) -> None:
        # 1. 定时事件：仓库降速 / 恢复
        if self.tick in self._scheduled_registry:
            self._apply_registry_factor(
                self._scheduled_registry.pop(self.tick), "定时场景事件"
            )

        # 2. 定时提交作业
        for spec in self._scheduled_jobs.pop(self.tick, []):
            self._submit_job(spec, self.tick)

        # 3. 为未开始的任务重新选择来源（仓库变慢 / 新种子出现都会在这里生效）
        self._reselect_sources()

        # 4. EDF 优先序 + 资源约束的带宽分配
        allocations, resources_used = self._allocate()

        # 5. 推进传输、处理完成
        self._advance(allocations)

        # 6. 作业状态与风险
        self._update_jobs()

        # 7. 记录利用率曲线
        self._record_utilization(resources_used)
        self.tick += 1

    # ------------------------------------------------------------------ 作业提交

    def _submit_job(self, spec, now: int) -> str:
        self._job_seq += 1
        job_id = f"J{self._job_seq:02d}"
        image: Image = self.topo.images[spec.image_name]
        node: Node = self.topo.nodes[spec.node_id]
        job = PullJob(
            id=job_id,
            node_id=node.id,
            image_name=image.name,
            layer_ids=image.layer_ids,
            submitted_tick=now,
            deadline_tick=now + spec.deadline_offset,
        )
        self.jobs[job_id] = job
        self._log("job_submit", f"{job_id} @ {node.id} 拉取 {image.name}",
                  f"交付时限 T+{spec.deadline_offset}（tick {job.deadline_tick}）")

        for lid in image.layer_ids:
            self._task_seq += 1
            tid = f"T{self._task_seq:03d}"
            task = TransferTask(
                id=tid,
                job_id=job_id,
                node_id=node.id,
                layer_id=lid,
                size_mb=self.topo.layer(lid).size_mb,
            )
            self.tasks[tid] = task
            if lid in node.cache:
                # 本地缓存命中：零字节、即时完成
                task.state = TaskState.HIT
                task.source = Source.LOCAL
                task.start_tick = now
                task.end_tick = now
                ledger = self.ledger[lid]
                ledger.local_hits += 1
                ledger.nodes_with.add(node.id)
                self._log("layer_hit", f"{tid} {node.id} 命中 {lid}",
                          f"本地缓存，跳过传输（{task.size_mb:.0f} MB）", level="good")

        self._update_job_state(job)
        return job_id

    # ------------------------------------------------------------------ 源选择

    def _reselect_sources(self) -> None:
        # 同层是否已有 registry 回源在途（全局去重）。
        # ACTIVE（上一 tick 已在传）与本 tick 刚选中 REGISTRY 的 PENDING
        # 都计入，避免同一 tick 内两个同层任务同时回源。
        registry_inflight: set[str] = set()
        for t in self.tasks.values():
            if t.source == Source.REGISTRY and t.state in (
                TaskState.ACTIVE, TaskState.PENDING
            ):
                registry_inflight.add(t.layer_id)

        # 只重排"尚未开始"的任务；进行中的传输不抢占
        for t in self.tasks.values():
            if t.state not in (TaskState.PENDING, TaskState.WAIT_SEED):
                continue
            node = self.topo.nodes[t.node_id]
            old_source = t.source
            old_peer = t.peer_node_id
            chosen, peer, reason = self._pick_source(node, t.layer_id,
                                                     registry_inflight)
            if chosen is None:
                t.state = TaskState.WAIT_SEED
                t.source = None
                t.peer_node_id = None
                t.wait_reason = reason
            else:
                t.state = TaskState.PENDING
                t.source = chosen
                t.peer_node_id = peer
                t.wait_reason = ""
                if chosen == Source.REGISTRY:
                    # 占位：后续同层任务本 tick 必须 WAIT_SEED
                    registry_inflight.add(t.layer_id)
                if old_source is not None and (
                    old_source != chosen or old_peer != peer
                ):
                    self._log(
                        "reschedule",
                        f"{t.id} 重新排队 {t.layer_id}",
                        f"{old_source.value} → {chosen.value}"
                        + (f"（种子 {peer}）" if peer else ""),
                        level="warn",
                    )
                    self._reschedule_logged.add(t.id)

    def _pick_source(self, node: Node, layer_id: str,
                     registry_inflight: set[str]):
        """返回 (source, peer_node_id, wait_reason)。None 表示本 tick 不可用。"""
        seeders = sorted(
            (n for n in self.topo.nodes.values()
             if layer_id in n.cache and n.id != node.id),
            key=lambda n: n.id,
        )
        if seeders:
            same_rack = [n for n in seeders if n.rack_id == node.rack_id]
            pool = same_rack or seeders
            peer = self._least_loaded_peer(pool)
            if same_rack:
                return Source.PEER_RACK, peer.id, ""
            return Source.PEER_CROSS, peer.id, ""

        # 全集群都没有种子，只能回源；同层回源在途则等待首个种子
        if layer_id in registry_inflight:
            return None, None, f"等待 {layer_id} 首个仓库回源完成后 P2P 接力"
        return Source.REGISTRY, None, ""

    def _least_loaded_peer(self, candidates: list[Node]) -> Node:
        """选当前上传流最少的节点，tie 按 id 稳定排序。"""
        uploads: dict[str, int] = defaultdict(int)
        for t in self.tasks.values():
            if t.state == TaskState.ACTIVE and t.peer_node_id:
                uploads[t.peer_node_id] += 1
        return min(candidates, key=lambda n: (uploads[n.id], n.id))

    # ------------------------------------------------------------------ 优先级

    def _priority_key(self, t: TransferTask):
        """EDF：截止期最早优先；同截止期优先解锁更多同层等待者的任务。"""
        job = self.jobs[t.job_id]
        waiter_count = sum(
            1
            for other in self.tasks.values()
            if other.layer_id == t.layer_id
            and other.state in (TaskState.PENDING, TaskState.WAIT_SEED,
                                TaskState.ACTIVE)
            and other.id != t.id
        )
        # registry 回源是同层所有等待者的前置，给额外加权
        seed_bonus = -waiter_count if t.source == Source.REGISTRY else 0
        return (
            job.deadline_tick,
            job.submitted_tick,
            seed_bonus,
            -waiter_count,
            t.id,
        )

    def _flow_resources(self, t: TransferTask) -> tuple[str, ...]:
        node = self.topo.nodes[t.node_id]
        ingress = f"in:{node.id}"
        if t.source == Source.REGISTRY:
            return ("reg", ingress)
        if t.source == Source.PEER_RACK:
            return (f"lan:{node.rack_id}", ingress)
        peer = self.topo.nodes[t.peer_node_id]
        return (f"up:{node.rack_id}", f"up:{peer.rack_id}", ingress)

    # ------------------------------------------------------------------ 带宽分配

    def _allocate(self) -> tuple[dict[str, float], dict[str, float]]:
        caps: dict[str, float] = {
            "reg": self.topo.registry.effective_cap,
        }
        for rid, rack in self.topo.racks.items():
            caps[f"up:{rid}"] = rack.uplink_cap_mbps
            caps[f"lan:{rid}"] = rack.lan_cap_mbps
        for nid, node in self.topo.nodes.items():
            caps[f"in:{nid}"] = node.ingress_cap_mbps

        remaining = dict(caps)
        eligible = [
            t for t in self.tasks.values()
            if t.state in (TaskState.PENDING, TaskState.ACTIVE)
            and t.source in (Source.REGISTRY, Source.PEER_RACK, Source.PEER_CROSS)
        ]
        eligible.sort(key=self._priority_key)

        allocations: dict[str, float] = {}
        used: dict[str, float] = defaultdict(float)
        for t in eligible:
            rate = t.remaining_mb  # 1 tick 内完成所需速率上限
            for res in self._flow_resources(t):
                rate = min(rate, remaining[res])
            rate = max(0.0, rate)
            t.last_allocated_mbps = t.allocated_mbps
            t.allocated_mbps = rate
            allocations[t.id] = rate
            if rate > 0:
                for res in self._flow_resources(t):
                    remaining[res] -= rate
                    used[res] += rate
                if t.state == TaskState.PENDING:
                    t.state = TaskState.ACTIVE
                    t.start_tick = self.tick
                    self._log_transfer_start(t)
            elif t.state == TaskState.PENDING:
                # 资源饱和，本 tick 排队
                t.wait_reason = self._saturation_reason(t, caps)
        return allocations, used

    def _saturation_reason(self, t: TransferTask, caps) -> str:
        names = {
            "reg": "仓库出口",
        }
        for rid in self.topo.racks:
            names[f"up:{rid}"] = f"{rid} 上联"
            names[f"lan:{rid}"] = f"{rid} 机架内 LAN"
        for nid in self.topo.nodes:
            names[f"in:{nid}"] = f"{nid} 入带宽"
        busy = [names[r] for r in self._flow_resources(t)]
        return "带宽饱和：" + " / ".join(busy)

    # ------------------------------------------------------------------ 传输推进

    def _advance(self, allocations: dict[str, float]) -> None:
        for tid, rate in allocations.items():
            t = self.tasks[tid]
            if rate <= 0:
                continue
            before = t.progress_mb
            t.progress_mb = min(t.size_mb, t.progress_mb + rate)
            transferred = t.progress_mb - before
            if t.progress_mb >= t.size_mb - 1e-9:
                self._complete(t, transferred)

    def _complete(self, t: TransferTask, last_bytes: float) -> None:
        node = self.topo.nodes[t.node_id]
        t.state = TaskState.DONE
        t.end_tick = self.tick + 1
        node.cache.add(t.layer_id)

        ledger = self.ledger[t.layer_id]
        ledger.nodes_with.add(node.id)
        if t.source == Source.REGISTRY:
            ledger.registry_pulls += 1
            ledger.registry_bytes_mb += t.size_mb
        else:
            if t.source == Source.PEER_RACK:
                ledger.peer_rack_pulls += 1
            else:
                ledger.peer_cross_pulls += 1
            ledger.peer_bytes_mb += t.size_mb

        self.records.append(TransferRecord(
            layer_id=t.layer_id,
            target_node_id=node.id,
            source=t.source,
            peer_node_id=t.peer_node_id,
            bytes_mb=t.size_mb,
            start_tick=t.start_tick,
            end_tick=t.end_tick,
        ))
        duration = max(1, t.end_tick - t.start_tick)
        self._log(
            "transfer_done",
            f"{t.id} {node.id} 完成 {t.layer_id}",
            f"{t.source.value}"
            + (f" ← {t.peer_node_id}" if t.peer_node_id else "")
            + f"，{t.size_mb:.0f} MB / {duration} tick",
            level="good",
        )

    # ------------------------------------------------------------------ 作业状态

    def _update_jobs(self) -> None:
        for job in self.jobs.values():
            if job.state in (JobState.DONE_ON_TIME, JobState.DONE_LATE):
                continue
            self._update_job_state(job)
            if job.state not in (JobState.DONE_ON_TIME, JobState.DONE_LATE):
                self._assess_risk(job)

    def _update_job_state(self, job: PullJob) -> None:
        tasks = [t for t in self.tasks.values() if t.job_id == job.id]
        if all(t.state in (TaskState.DONE, TaskState.HIT) for t in tasks):
            end = max(
                (t.end_tick for t in tasks if t.end_tick is not None),
                default=self.tick,
            )
            job.state = (
                JobState.DONE_ON_TIME if end <= job.deadline_tick
                else JobState.DONE_LATE
            )
            level = "good" if job.state == JobState.DONE_ON_TIME else "critical"
            self._log(
                "job_done",
                f"{job.id} 拉取完成（tick {end}）",
                ("按时交付" if job.state == JobState.DONE_ON_TIME
                 else f"超出时限（deadline tick {job.deadline_tick}）"),
                level=level,
            )
        elif any(t.state == TaskState.ACTIVE for t in tasks):
            job.state = JobState.ACTIVE

    def _assess_risk(self, job: PullJob) -> None:
        remaining = sum(
            t.remaining_mb
            for t in self.tasks.values()
            if t.job_id == job.id
            and t.state in (TaskState.PENDING, TaskState.WAIT_SEED,
                            TaskState.ACTIVE)
        )
        node = self.topo.nodes[job.node_id]
        # 乐观下界：节点入带宽打满时的最短耗时
        eta = remaining / node.ingress_cap_mbps if remaining > 0 else 0.0
        slack = job.deadline_tick - self.tick
        risky = remaining > 0 and (self.tick > job.deadline_tick
                                   or eta > max(0, slack))
        if risky and not job.at_risk:
            self._log(
                "risk",
                f"{job.id} 存在延期风险",
                f"剩余 {remaining:.0f} MB，乐观 ETA {eta:.1f} tick，"
                f"时限余量 {slack} tick",
                level="warn",
            )
        job.at_risk = risky

    # ------------------------------------------------------------------ 利用率记录

    def _record_utilization(self, used: dict[str, float]) -> None:
        reg_cap = self.topo.registry.effective_cap
        point = {
            "tick": self.tick,
            "registry": {
                "used": round(used.get("reg", 0.0), 2),
                "cap": round(reg_cap, 2),
            },
            "racks": {},
            "queue": sum(
                1 for t in self.tasks.values()
                if t.state in (TaskState.PENDING, TaskState.WAIT_SEED)
            ),
            "active": sum(
                1 for t in self.tasks.values() if t.state == TaskState.ACTIVE
            ),
        }
        for rid, rack in self.topo.racks.items():
            point["racks"][rid] = {
                "up_used": round(used.get(f"up:{rid}", 0.0), 2),
                "up_cap": rack.uplink_cap_mbps,
                "lan_used": round(used.get(f"lan:{rid}", 0.0), 2),
                "lan_cap": rack.lan_cap_mbps,
            }
        self.util_history.append(point)
        if len(self.util_history) > HISTORY_LIMIT:
            del self.util_history[:-HISTORY_LIMIT]

    # ------------------------------------------------------------------ 仓库降速

    def _apply_registry_factor(self, factor: float, reason: str) -> None:
        old = self.topo.registry.factor
        self.topo.registry.factor = factor
        self._log(
            "registry",
            f"仓库速率系数 {old:.2f} → {factor:.2f}",
            f"有效出口 {self.topo.registry.effective_cap:.0f} MB/s（{reason}）",
            level="warn" if factor < old else "good",
        )

    # ------------------------------------------------------------------ 事件

    def _log_transfer_start(self, t: TransferTask) -> None:
        node = self.topo.nodes[t.node_id]
        src = t.source.value
        peer = f" ← {t.peer_node_id}" if t.peer_node_id else ""
        self._log("transfer_start",
                  f"{t.id} {node.id} 开始拉取 {t.layer_id}",
                  f"来源 {src}{peer}")

    def _log(self, kind: str, title: str, detail: str = "",
             level: str = "info") -> None:
        self.events.append({
            "tick": self.tick,
            "kind": kind,
            "level": level,
            "title": title,
            "detail": detail,
        })
        if len(self.events) > EVENT_LIMIT:
            del self.events[:-EVENT_LIMIT]

    # ------------------------------------------------------------------ 快照

    def _snapshot_locked(self) -> dict:
        reg = self.topo.registry
        ledger_rows = []
        for lid in sorted(self.ledger):
            lg = self.ledger[lid]
            total_delivered = lg.registry_bytes_mb + lg.peer_bytes_mb
            ledger_rows.append({
                "layer_id": lid,
                "size_mb": lg.size_mb,
                "registry_pulls": lg.registry_pulls,
                "peer_rack_pulls": lg.peer_rack_pulls,
                "peer_cross_pulls": lg.peer_cross_pulls,
                "local_hits": lg.local_hits,
                "nodes_with": sorted(lg.nodes_with),
                "registry_bytes_mb": round(lg.registry_bytes_mb, 1),
                "peer_bytes_mb": round(lg.peer_bytes_mb, 1),
                "delivered_mb": round(total_delivered, 1),
                "fanout": len(lg.nodes_with),
                "single_backhaul_ok": lg.registry_pulls <= 1,
            })

        jobs_out = []
        for jid in sorted(self.jobs):
            j = self.jobs[jid]
            tasks = sorted(
                (t for t in self.tasks.values() if t.job_id == jid),
                key=lambda t: t.id,
            )
            jobs_out.append({
                "id": j.id,
                "node_id": j.node_id,
                "image_name": j.image_name,
                "submitted_tick": j.submitted_tick,
                "deadline_tick": j.deadline_tick,
                "state": j.state.value,
                "at_risk": j.at_risk,
                "tasks": [self._task_view(t) for t in tasks],
            })

        queue = [
            self._task_view(t)
            for t in sorted(self.tasks.values(), key=self._priority_key)
            if t.state in (TaskState.PENDING, TaskState.WAIT_SEED)
        ]
        active = [
            self._task_view(t)
            for t in self.tasks.values() if t.state == TaskState.ACTIVE
        ]

        totals = {
            "registry_bytes": round(
                sum(l.registry_bytes_mb for l in self.ledger.values()), 1),
            "peer_bytes": round(
                sum(l.peer_bytes_mb for l in self.ledger.values()), 1),
            "local_hit_layers": sum(l.local_hits for l in self.ledger.values()),
            "transfer_count": len(self.records),
            "jobs_total": len(self.jobs),
            "jobs_done": sum(
                1 for j in self.jobs.values()
                if j.state in (JobState.DONE_ON_TIME, JobState.DONE_LATE)
            ),
            "jobs_late": sum(
                1 for j in self.jobs.values()
                if j.state == JobState.DONE_LATE
            ),
        }
        # 若没有共享去重，每个节点需要的层都得从仓库回源
        naive_registry_bytes = round(sum(
            t.size_mb for t in self.tasks.values()
            if t.state in (TaskState.DONE, TaskState.HIT, TaskState.ACTIVE,
                           TaskState.PENDING, TaskState.WAIT_SEED)
        ), 1)
        totals["naive_registry_bytes"] = naive_registry_bytes
        totals["saved_bytes"] = round(
            naive_registry_bytes - totals["registry_bytes"], 1)

        checks = {
            "single_backhaul": all(
                row["single_backhaul_ok"] for row in ledger_rows
            ),
            "local_hit_zero_bytes": all(
                t.source != Source.LOCAL or t.progress_mb == 0.0
                for t in self.tasks.values()
            ),
            # 每个已完成的层任务都有合法来源（未开始的层不计入）
            "all_layers_covered": all(
                (t.source is not None)
                for t in self.tasks.values()
                if t.state in (TaskState.DONE, TaskState.HIT)
            ),
        }

        return {
            "tick": self.tick,
            "scenario": self.scenario.name,
            "scenario_description": self.scenario.description,
            "image_names": sorted(self.topo.images),
            "registry": {
                "base_cap_mbps": reg.egress_cap_mbps,
                "factor": reg.factor,
                "effective_cap_mbps": round(reg.effective_cap, 2),
            },
            "racks": {
                rid: {
                    "uplink_cap_mbps": r.uplink_cap_mbps,
                    "lan_cap_mbps": r.lan_cap_mbps,
                }
                for rid, r in self.topo.racks.items()
            },
            "nodes": {
                nid: {
                    "rack_id": n.rack_id,
                    "ingress_cap_mbps": n.ingress_cap_mbps,
                    "cache": sorted(n.cache),
                }
                for nid, n in self.topo.nodes.items()
            },
            "jobs": jobs_out,
            "queue": queue,
            "active": active,
            "ledger": ledger_rows,
            "util_history": self.util_history[-120:],
            "events": list(reversed(self.events[-80:])),
            "totals": totals,
            "checks": checks,
            "is_done": self.is_done(),
        }

    def _task_view(self, t: TransferTask) -> dict:
        job = self.jobs[t.job_id]
        return {
            "id": t.id,
            "job_id": t.job_id,
            "node_id": t.node_id,
            "layer_id": t.layer_id,
            "size_mb": t.size_mb,
            "state": t.state.value,
            "source": t.source.value if t.source else None,
            "peer_node_id": t.peer_node_id,
            "progress_mb": round(t.progress_mb, 1),
            "remaining_mb": round(t.remaining_mb, 1),
            "allocated_mbps": round(t.allocated_mbps, 1),
            "pct": round(100.0 * t.progress_mb / t.size_mb, 1),
            "start_tick": t.start_tick,
            "end_tick": t.end_tick,
            "deadline_tick": job.deadline_tick,
            "wait_reason": t.wait_reason,
            "priority": self._priority_key(t),
        }
