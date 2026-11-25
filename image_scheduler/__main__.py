"""命令行入口：python -m image_scheduler [scenario] [--tick N] [--serve]。"""

from __future__ import annotations

import argparse

from .engine import SimEngine
from .topology import SCENARIOS, build_scenario


def run_headless(name: str, max_ticks: int) -> None:
    engine = SimEngine(build_scenario(name))
    print(f"场景: {name} —— {SCENARIOS[name]}")
    for _ in range(max_ticks):
        engine.step()
        if engine.is_done():
            break
    snap = engine.snapshot()
    t = snap["totals"]
    print(f"\n结束于 tick {snap['tick']}")
    print(f"作业 {t['jobs_done']}/{t['jobs_total']} 完成，延期 {t['jobs_late']} 个")
    print(f"仓库出口 {t['registry_bytes']:.0f} MB | P2P {t['peer_bytes']:.0f} MB | "
          f"本地命中 {t['local_hit_layers']} 层")
    print(f"无去重基线 {t['naive_registry_bytes']:.0f} MB → 节省 "
          f"{t['saved_bytes']:.0f} MB "
          f"({t['saved_bytes'] / t['naive_registry_bytes']:.0%})")
    print("\n逐层审计（每层仓库回源必须 ≤ 1）:")
    print(f"{'层':<4}{'大小MB':>7}{'回源':>5}{'同架':>5}{'跨架':>5}{'命中':>5}"
          f"{'扇出':>5}  校验")
    for r in snap["ledger"]:
        if r["fanout"] == 0:
            continue
        ok = "✓" if r["single_backhaul_ok"] else "✗"
        print(f"{r['layer_id']:<4}{r['size_mb']:>7.0f}{r['registry_pulls']:>5}"
              f"{r['peer_rack_pulls']:>5}{r['peer_cross_pulls']:>5}"
              f"{r['local_hits']:>5}{r['fanout']:>5}  {ok}")
    print("\n不变量校验:",
          {k: ("✓" if v else "✗") for k, v in snap["checks"].items()})


def main() -> None:
    parser = argparse.ArgumentParser(description="镜像拉取调度模拟器")
    parser.add_argument("scenario", nargs="?", default="burst",
                        choices=sorted(SCENARIOS))
    parser.add_argument("--tick", type=int, default=400,
                        help="最大推进 tick 数（默认 400）")
    parser.add_argument("--serve", action="store_true",
                        help="启动 Web 仪表盘（默认端口 8000）")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()

    if args.serve:
        from .webserver import serve
        serve(port=args.port)
    else:
        run_headless(args.scenario, args.tick)


if __name__ == "__main__":
    main()
