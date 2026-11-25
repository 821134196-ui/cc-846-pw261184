"""引擎核心不变量测试。"""

import unittest

from image_scheduler import SimEngine, build_scenario
from image_scheduler.model import JobState, Source, TaskState


def run_to_end(engine: SimEngine, max_ticks: int = 400) -> None:
    for _ in range(max_ticks):
        engine.step()
        if engine.is_done():
            return
    raise AssertionError("仿真在 max_ticks 内未结束")


class SharedLayerTests(unittest.TestCase):
    def test_each_layer_backhauled_at_most_once(self):
        """共享层全局只从仓库回源一次，其余节点全部 P2P/缓存。"""
        engine = SimEngine(build_scenario("burst"))
        run_to_end(engine)
        snap = engine.snapshot()

        for row in snap["ledger"]:
            with self.subTest(layer=row["layer_id"]):
                self.assertLessEqual(row["registry_pulls"], 1)
        self.assertTrue(snap["checks"]["single_backhaul"])

        # L1 是 9 个节点都要的基础层：1 次回源 + 8 次 P2P
        l1 = next(r for r in snap["ledger"] if r["layer_id"] == "L1")
        self.assertEqual(l1["registry_pulls"], 1)
        self.assertEqual(
            l1["peer_rack_pulls"] + l1["peer_cross_pulls"], 8
        )

    def test_registry_inflight_dedup_during_run(self):
        """运行过程中同一时刻同层不能有两个 registry 流。"""
        engine = SimEngine(build_scenario("burst"))
        seen = set()
        for _ in range(120):
            engine.step()
            active_reg = [
                t for t in engine.tasks.values()
                if t.state == TaskState.ACTIVE and t.source == Source.REGISTRY
            ]
            ids = {t.layer_id for t in active_reg}
            self.assertEqual(len(ids), len(active_reg))
            seen.update(ids)
            if engine.is_done():
                break
        self.assertTrue(seen)  # 确实发生过回源

    def test_peer_transfer_bytes_never_leave_registry_account(self):
        records = [r for r in engine_records("burst")
                   if r.source != Source.REGISTRY]
        self.assertTrue(records)  # P2P 确实发生
        # 审计流水与 ledger 统计一致
        engine = SimEngine(build_scenario("burst"))
        run_to_end(engine)
        peer_bytes = sum(
            r.bytes_mb for r in engine.records
            if r.source in (Source.PEER_RACK, Source.PEER_CROSS)
        )
        self.assertAlmostEqual(
            peer_bytes,
            sum(l.peer_bytes_mb for l in engine.ledger.values()),
            places=5,
        )


class CacheTests(unittest.TestCase):
    def test_local_hit_is_instant_and_zero_bytes(self):
        engine = SimEngine(build_scenario("prewarmed"))
        # 直接向已有缓存的 R1-N1 提交其已缓存镜像：提交即命中，无需推进 tick
        engine.add_job("R1-N1", "web-api", deadline_offset=12)
        hits = [
            t for t in engine.tasks.values() if t.state == TaskState.HIT
        ]
        self.assertEqual(len(hits), 3)
        for t in hits:
            self.assertEqual(t.source, Source.LOCAL)
            self.assertEqual(t.progress_mb, 0.0)
            self.assertEqual(t.end_tick, 0)
        engine.step()
        run_to_end(engine)
        # 预温节点的 web-api 层零回源
        for lid in ("L1", "L2", "L3"):
            row = next(r for r in engine.snapshot()["ledger"]
                       if r["layer_id"] == lid)
            targets = {
                rec.target_node_id for rec in engine.records
                if rec.layer_id == lid and rec.source == Source.REGISTRY
            }
            self.assertNotIn("R1-N1", targets)
            self.assertNotIn("R2-N1", targets)

    def test_same_rack_seeder_preferred(self):
        engine = SimEngine(build_scenario("prewarmed"))
        run_to_end(engine)
        # R1-N2/N3 与种子 R1-N1 同机架，应全部走 PEER_RACK
        for rec in engine.records:
            if rec.target_node_id in ("R1-N2", "R1-N3"):
                self.assertEqual(
                    rec.source, Source.PEER_RACK,
                    msg=f"{rec.target_node_id} 应同机架获取 {rec.layer_id}",
                )
                self.assertEqual(rec.peer_node_id, "R1-N1")


class BandwidthTests(unittest.TestCase):
    def test_resource_caps_never_exceeded(self):
        engine = SimEngine(build_scenario("slow_registry"))
        for _ in range(200):
            engine.step()
            snap = engine.snapshot()
            reg = snap["util_history"][-1]
            self.assertLessEqual(
                reg["registry"]["used"],
                reg["registry"]["cap"] + 1e-6,
            )
            for rid, rv in reg["racks"].items():
                self.assertLessEqual(
                    rv["up_used"], rv["up_cap"] + 1e-6, msg=rid)
                self.assertLessEqual(
                    rv["lan_used"], rv["lan_cap"] + 1e-6, msg=rid)
            for t in engine.tasks.values():
                if t.state == TaskState.ACTIVE:
                    self.assertLessEqual(
                        t.allocated_mbps,
                        engine.topo.nodes[t.node_id].ingress_cap_mbps + 1e-6,
                    )
            if engine.is_done():
                break

    def test_registry_slowdown_shrinks_allocation_and_reschedules_queue(self):
        engine = SimEngine(build_scenario("slow_registry"))
        # 走到 tick 6 降速事件之后
        for _ in range(8):
            engine.step()
        self.assertAlmostEqual(engine.topo.registry.factor, 0.25)
        self.assertLessEqual(
            engine.topo.registry.effective_cap, 25.0 + 1e-9
        )
        # 有重调度事件产生（未开始任务改走 P2P 或继续等待）
        kinds = {e["kind"] for e in engine.events}
        self.assertIn("registry", kinds)
        # 恢复后系数回到 1.0
        for _ in range(10):
            engine.step()
        self.assertEqual(engine.topo.registry.factor, 1.0)


class DeadlineTests(unittest.TestCase):
    def test_edf_ordering_tight_deadline_scheduled_first(self):
        engine = SimEngine(build_scenario("mixed_deadlines"))
        engine.step()
        # 所有可调度任务按引擎优先键排序后，deadline 必须单调不减
        eligible = [
            t for t in engine.tasks.values()
            if t.state in (TaskState.PENDING, TaskState.ACTIVE)
        ]
        eligible.sort(key=engine._priority_key)
        deadlines = [engine.jobs[t.job_id].deadline_tick for t in eligible]
        self.assertEqual(deadlines, sorted(deadlines))
        # 最紧时限（T+8）的任务必须排在最前
        self.assertEqual(deadlines[0], min(deadlines))

    def test_late_jobs_marked_and_all_jobs_finish(self):
        engine = SimEngine(build_scenario("mixed_deadlines"))
        run_to_end(engine)
        snap = engine.snapshot()
        states = [j["state"] for j in snap["jobs"]]
        self.assertTrue(
            all(s in ("done_on_time", "done_late") for s in states)
        )
        self.assertEqual(
            snap["totals"]["jobs_done"], snap["totals"]["jobs_total"]
        )

    def test_burst_saves_registry_bytes_vs_naive(self):
        engine = SimEngine(build_scenario("burst"))
        run_to_end(engine)
        totals = engine.snapshot()["totals"]
        # 无去重时全部层都走仓库；去重后仓库出口流量应显著更低
        self.assertLess(
            totals["registry_bytes"], totals["naive_registry_bytes"]
        )
        saved_ratio = totals["saved_bytes"] / totals["naive_registry_bytes"]
        self.assertGreater(saved_ratio, 0.4)


def engine_records(scenario: str):
    engine = SimEngine(build_scenario(scenario))
    run_to_end(engine)
    return engine.records


if __name__ == "__main__":
    unittest.main()
