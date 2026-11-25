"""镜像拉取调度模拟器 —— 离散事件时间步进仿真。

公共入口：
    engine.SimEngine        仿真引擎（状态机 + tick 推进）
    topology.build_scenario 场景预设构建
"""

from .engine import SimEngine
from .topology import build_scenario, SCENARIOS

__all__ = ["SimEngine", "build_scenario", "SCENARIOS"]
