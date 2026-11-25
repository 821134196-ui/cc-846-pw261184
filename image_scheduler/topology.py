"""拓扑与场景构建：镜像、层、机架、节点及预设场景。"""

from __future__ import annotations

from dataclasses import dataclass, field

from .model import Image, Layer, Node, Rack, Registry


@dataclass
class Topology:
    layers: dict[str, Layer]
    images: dict[str, Image]
    registry: Registry
    racks: dict[str, Rack]
    nodes: dict[str, Node]

    def layer(self, layer_id: str) -> Layer:
        return self.layers[layer_id]


def build_topology() -> Topology:
    """固定的基准拓扑。

    3 个机架 × 每架 3 节点；仓库出口 100 MB/s，机架上联 90 MB/s，
    机架内 LAN 200 MB/s，节点入带宽 60 MB/s。
    4 个镜像共 8 个层，基础层 L1 在所有镜像间共享（最大的复用机会）。
    """
    layers = {
        f"L{i}": Layer(f"L{i}", size)
        for i, size in enumerate(
            (120.0, 90.0, 70.0, 60.0, 45.0, 40.0, 30.0, 25.0), start=1
        )
    }
    images = {
        "web-api":    Image("web-api",    ("L1", "L2", "L3")),
        "checkout":   Image("checkout",   ("L1", "L2", "L4", "L5")),
        "recommend":  Image("recommend",  ("L1", "L3", "L6", "L7")),
        "edge-proxy": Image("edge-proxy", ("L1", "L4", "L8")),
    }
    registry = Registry(egress_cap_mbps=100.0)
    racks = {
        "R1": Rack("R1", uplink_cap_mbps=90.0, lan_cap_mbps=200.0),
        "R2": Rack("R2", uplink_cap_mbps=90.0, lan_cap_mbps=200.0),
        "R3": Rack("R3", uplink_cap_mbps=90.0, lan_cap_mbps=200.0),
    }
    nodes: dict[str, Node] = {}
    for rack in racks:
        for k in range(1, 4):
            nid = f"{rack}-N{k}"
            nodes[nid] = Node(nid, rack, ingress_cap_mbps=60.0)
    return Topology(layers, images, registry, racks, nodes)


@dataclass
class JobSpec:
    node_id: str
    image_name: str
    deadline_offset: int           # 相对提交 tick 的时限（tick 数）
    submit_offset: int = 0         # 相对场景起点的提交延迟


@dataclass
class Scenario:
    name: str
    description: str
    topology: Topology
    jobs: list[JobSpec] = field(default_factory=list)
    # (触发 tick, 仓库速率系数) ；模拟仓库变慢 / 恢复
    registry_events: list[tuple[int, float]] = field(default_factory=list)
    preseed: dict[str, list[str]] = field(default_factory=dict)


SCENARIOS = {
    "burst": "新节点批量扩容：9 节点同时拉取，基础层 L1 全员共享",
    "slow_registry": "拉取进行到一半仓库降速至 25%，未开始任务重新排队",
    "mixed_deadlines": "同一批镜像但交付时限分三级，验证 EDF 优先序",
    "prewarmed": "部分节点已有缓存，验证本地命中与同机架种子分发",
}


def build_scenario(name: str) -> Scenario:
    topo = build_topology()
    node_ids = list(topo.nodes)

    if name == "burst":
        jobs = [
            JobSpec(node_ids[i], image, deadline_offset=dl)
            for i, (image, dl) in enumerate([
                ("web-api", 18), ("checkout", 20), ("recommend", 22),
                ("web-api", 19), ("edge-proxy", 16), ("checkout", 24),
                ("recommend", 21), ("web-api", 17), ("edge-proxy", 23),
            ])
        ]
        return Scenario(name, SCENARIOS[name], topo, jobs)

    if name == "slow_registry":
        jobs = [
            JobSpec(nid, "checkout", deadline_offset=30) for nid in node_ids
        ] + [
            JobSpec(nid, "recommend", deadline_offset=34) for nid in node_ids[:4]
        ]
        return Scenario(
            name, SCENARIOS[name], topo, jobs,
            registry_events=[(6, 0.25), (14, 1.0)],
        )

    if name == "mixed_deadlines":
        plan = [
            ("edge-proxy", 8), ("web-api", 30), ("checkout", 12),
            ("recommend", 26), ("web-api", 10), ("edge-proxy", 28),
            ("checkout", 14), ("recommend", 9), ("web-api", 24),
        ]
        jobs = [
            JobSpec(node_ids[i], image, deadline_offset=dl)
            for i, (image, dl) in enumerate(plan)
        ]
        return Scenario(name, SCENARIOS[name], topo, jobs)

    if name == "prewarmed":
        # R1-N1、R2-N1 已缓存 web-api 的全部层；R3-N1 有共享基础层
        preseed = {
            "R1-N1": list(topo.images["web-api"].layer_ids),
            "R2-N1": list(topo.images["web-api"].layer_ids),
            "R3-N1": ["L1"],
        }
        jobs = [
            JobSpec("R1-N2", "web-api", deadline_offset=12),
            JobSpec("R1-N3", "web-api", deadline_offset=12),
            JobSpec("R2-N2", "web-api", deadline_offset=12),
            JobSpec("R3-N2", "web-api", deadline_offset=12),
            JobSpec("R3-N3", "checkout", deadline_offset=20),
        ]
        return Scenario(name, SCENARIOS[name], topo, jobs, preseed=preseed)

    raise KeyError(f"未知场景: {name}")
