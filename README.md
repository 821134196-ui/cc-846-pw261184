# 镜像拉取调度模拟器

模拟新节点批量扩容时的镜像拉取：镜像仓库、节点本地缓存、机架内/跨机架 P2P、
共享层去重，以及一个把**交付时限（deadline）**与**跨机架带宽**纳入约束的拉取调度器。
仓库变慢后，**尚未开始的下载会自动重新选源、重新排队**；进行中的传输不抢占。

## 运行

仅依赖 Python 3.10+ 标准库，无需安装任何包。

```bash
# Web 仪表盘（默认 http://localhost:8000）
python3 -m image_scheduler --serve
python3 -m image_scheduler --serve --port 9000

# 无头批量运行，输出逐层审计
python3 -m image_scheduler burst
python3 -m image_scheduler slow_registry
python3 -m image_scheduler mixed_deadlines
python3 -m image_scheduler prewarmed

# 测试
python3 -m unittest discover -s tests
```

## 模型（1 tick = 1 秒）

| 资源 | 容量 | 说明 |
|---|---|---|
| 仓库出口（WAN） | 100 MB/s | 可动态注入降速系数（×0.5 / ×0.25 / 恢复） |
| 机架上联（双向共享） | 90 MB/s | 跨机架 P2P 同时占用两侧机架上联 |
| 机架内 LAN | 200 MB/s | 同机架 P2P 共享 |
| 节点入带宽 | 60 MB/s | 该节点所有流入流共享 |

4 个镜像共 8 个层，基础层 L1 被所有镜像共享；3 机架 × 3 节点。

## 调度策略

1. **源选择（每 tick 对未开始任务重算）**
   - 节点本地已有层 → `LOCAL` 命中，零字节、即时完成；
   - 否则集群内有种子节点 → 同机架种子优先（走 LAN，不占上联），
     同机架没有才跨机架，种子节点按当前上传流数做最小负载选择；
   - 全集群无种子 → 仓库回源。
2. **共享层全局去重**：每个层全局最多允许 **1 个**仓库回源流；同层其余节点进入
   `WAIT_SEED`，首个种子落地后下一 tick 自动改走 P2P 接力。
3. **EDF 带宽分配**：可调度任务按 `(作业截止期, 提交时刻, 解锁等待者数量)` 排序，
   依次贪心分配仓库出口 / 上联 / LAN / 节点入带宽，任何资源不允许超分；
   资源不足的任务留在队列并记录饱和原因。
4. **重调度**：仓库降速或新种子出现只影响 `PENDING/WAIT_SEED` 任务，
   `ACTIVE` 传输保持不抢占，界面事件流中可以看到 `registry → peer_*` 的改道记录。

## 界面显示与验证

仪表盘（`image_scheduler/web/`，无框架原生 JS）包含：

- **KPI 行**：作业完成/延期、仓库出口流量、相对"无去重基线"节省的字节与比例、
  P2P 流量、本地命中、在途/排队数、去重校验状态；
- **共享层"只传一次"实时校验卡**：三条不变量（每层回源 ≤1、本地命中零字节、
  已完成任务均有合法来源），任何一条被破坏立即变红；
- **层命中与来源审计表**：每层的回源/同架 P2P/跨架 P2P/本地命中计数、扇出节点、
  回源流量 vs 无去重流量，悬停显示当前持有该层的节点；
- **链路利用率小多图**：仓库出口与 3 个机架的 meter + sparkline + 容量参考线；
- **作业与层任务**：EDF 排序的作业卡、每层进度条、来源标签、速率与截止期；
- **调度队列与事件流**：含 `WAIT_SEED` 等待原因、降速、重调度、风险预警事件；
- **节点缓存拓扑**：9 个节点 × 8 层的缓存点阵，层在节点间扩散过程可视化。

典型结果（burst 场景，9 节点同时拉取）：仓库出口流量 2400 MB → **480 MB（省 80%）**，
L1 仅回源 1 次、P2P 接力 8 次，全部作业按时完成。

## 代码结构

```
image_scheduler/
  model.py       领域模型：层/镜像/仓库/机架/节点/任务/作业/传输流水
  topology.py    基准拓扑与 4 个场景（含定时降速事件、预温缓存）
  engine.py      仿真引擎：源选择、去重、EDF 分配、重调度、审计快照
  webserver.py   stdlib HTTP 服务 + 后台自动步进线程 + JSON API
  web/           index.html / styles.css / app.js 仪表盘
  __main__.py    CLI 入口
tests/
  test_engine.py 10 个 unittest：去重不变量、缓存命中、带宽上限、降速重调度、EDF
```

## 主要 API

| 方法 | 作用 |
|---|---|
| `GET /api/state` | 全量只读快照（引擎状态 + 校验结果 + 利用率历史） |
| `GET /api/scenarios` | 场景列表 |
| `POST /api/control` | `tick` / `play` / `pause` / `reset` / `speed` / `registry` / `add_job` |
