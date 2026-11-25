"""领域模型：层 / 镜像 / 仓库 / 机架 / 节点 / 传输任务 / 拉取作业。"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class Source(str, Enum):
    """层的获取来源。"""

    REGISTRY = "registry"      # 仓库回源（跨 WAN，受仓库出口限制）
    PEER_RACK = "peer_rack"    # 同机架节点对传（走机架内 LAN）
    PEER_CROSS = "peer_cross"  # 跨机架节点对传（走两侧机架上联）
    LOCAL = "local"            # 节点本地缓存命中，零传输


class TaskState(str, Enum):
    PENDING = "pending"        # 已入队，等待分配带宽
    WAIT_SEED = "wait_seed"    # 同层已有仓库回源在途，等待首个种子完成
    ACTIVE = "active"          # 传输中
    DONE = "done"              # 传输完成
    HIT = "hit"                # 本地缓存直接命中


class JobState(str, Enum):
    QUEUED = "queued"
    ACTIVE = "active"
    DONE_ON_TIME = "done_on_time"
    DONE_LATE = "done_late"


@dataclass(frozen=True)
class Layer:
    id: str
    size_mb: float


@dataclass(frozen=True)
class Image:
    name: str
    layer_ids: tuple[str, ...]


@dataclass
class Registry:
    """模拟镜像仓库。factor 为动态降速系数（1.0 = 满速）。"""

    egress_cap_mbps: float
    factor: float = 1.0

    @property
    def effective_cap(self) -> float:
        return self.egress_cap_mbps * self.factor


@dataclass
class Rack:
    id: str
    uplink_cap_mbps: float   # 跨机架上联带宽（双向共享）
    lan_cap_mbps: float      # 机架内 LAN 带宽（同机架对传共享）


@dataclass
class Node:
    id: str
    rack_id: str
    ingress_cap_mbps: float  # 节点入带宽（所有流入流共享）
    cache: set[str] = field(default_factory=set)


@dataclass
class TransferTask:
    """单个 (节点, 层) 的拉取任务。"""

    id: str
    job_id: str
    node_id: str
    layer_id: str
    size_mb: float
    state: TaskState = TaskState.PENDING
    source: Source | None = None
    peer_node_id: str | None = None
    progress_mb: float = 0.0
    allocated_mbps: float = 0.0
    start_tick: int | None = None
    end_tick: int | None = None
    # 上一 tick 的分配，供 UI 展示抖动与重调度
    last_allocated_mbps: float = 0.0
    wait_reason: str = ""

    @property
    def remaining_mb(self) -> float:
        return max(0.0, self.size_mb - self.progress_mb)


@dataclass
class PullJob:
    """节点对一个镜像的完整拉取作业，带交付时限。"""

    id: str
    node_id: str
    image_name: str
    layer_ids: tuple[str, ...]
    submitted_tick: int
    deadline_tick: int          # 绝对 tick
    state: JobState = JobState.QUEUED
    at_risk: bool = False


@dataclass
class TransferRecord:
    """完成的传输流水，是"共享层只传一次"的审计依据。"""

    layer_id: str
    target_node_id: str
    source: Source
    peer_node_id: str | None
    bytes_mb: float
    start_tick: int
    end_tick: int
