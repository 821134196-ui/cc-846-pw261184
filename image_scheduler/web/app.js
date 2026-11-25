/* 镜像拉取调度模拟器仪表盘：轮询 /api/state 并渲染。
   所有外部数据一律走 textContent / createElement，不拼 innerHTML。 */
"use strict";

const SOURCE_META = {
  registry:   { label: "仓库回源", cls: "registry" },
  peer_rack:  { label: "同架 P2P", cls: "peer_rack" },
  peer_cross: { label: "跨架 P2P", cls: "peer_cross" },
  local:      { label: "本地命中", cls: "local" },
};
const LAYER_COLORS = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100",
  "#e87ba4", "#008300", "#6250d6", "#e34948"];
const STATE_LABEL = {
  queued: "排队中", active: "传输中", done_on_time: "按时完成", done_late: "延期完成",
};
const TASK_STATE_LABEL = {
  pending: "待调度", wait_seed: "等种子", active: "传输中",
  done: "完成", hit: "缓存命中",
};

const $ = (sel) => document.querySelector(sel);
const tooltip = $("#tooltip");
let state = null;
let scenarios = [];
let pollTimer = null;

// ------------------------------------------------------------- 基础 DOM 工具

function el(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined) node.textContent = text;
  return node;
}

function showTooltip(htmlLike, x, y) {
  tooltip.replaceChildren();
  if (typeof htmlLike === "string") {
    tooltip.textContent = htmlLike;
  } else {
    htmlLike.forEach((n) => tooltip.appendChild(n));
  }
  tooltip.hidden = false;
  const pad = 14;
  const rect = tooltip.getBoundingClientRect();
  tooltip.style.left = `${Math.min(x + pad, window.innerWidth - rect.width - 10)}px`;
  tooltip.style.top = `${Math.min(y + pad, window.innerHeight - rect.height - 10)}px`;
}
function hideTooltip() { tooltip.hidden = true; }

function attachTip(target, build) {
  target.addEventListener("pointermove", (e) => showTooltip(build(), e.clientX, e.clientY));
  target.addEventListener("pointerleave", hideTooltip);
}

function fmtMb(v) {
  if (v >= 1000) return `${(v / 1000).toFixed(1)}K`;
  return `${Math.round(v)}`;
}

// ------------------------------------------------------------- 数据获取

async function fetchJSON(url, opts) {
  const res = await fetch(url, opts);
  if (!res.ok) throw new Error(`${url} -> ${res.status}`);
  return res.json();
}

async function postControl(payload) {
  await fetchJSON("/api/control", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  await refresh();
}

async function refresh() {
  state = await fetchJSON("/api/state");
  render();
}

// ------------------------------------------------------------- 渲染入口

function render() {
  if (!state) return;
  renderHeader();
  renderKpis();
  renderChecks();
  renderLayerTable();
  renderUtils();
  renderJobs();
  renderQueue();
  renderEvents();
  renderTopology();
}

// ------------------------------------------------------------- 顶栏 / KPI

function renderHeader() {
  $("#clock").textContent = `T${state.tick}`;
  $("#scenario-desc").textContent = state.scenario_description;
  const f = state.registry.factor;
  const chip = $("#reg-chip");
  chip.textContent = `仓库 ${Math.round(f * 100)}% · ${state.registry.effective_cap_mbps} MB/s`;
  chip.className = "clock-chip" + (f <= 0.25 ? " critical" : f < 1 ? " slow" : "");
  $("#btn-play").textContent = state.auto ? "⏸ 暂停" : "▶ 播放";
}

function renderKpis() {
  const t = state.totals;
  $("#kpi-jobs").textContent = `${t.jobs_done}/${t.jobs_total}`;
  const late = $("#kpi-late");
  late.textContent = t.jobs_late ? `${t.jobs_late} 个延期` : "全部按时";
  late.className = "stat-delta " + (t.jobs_late ? "critical" : "good");

  $("#kpi-registry").textContent = `${fmtMb(t.registry_bytes)} MB`;
  const ratio = t.naive_registry_bytes
    ? Math.round((t.saved_bytes / t.naive_registry_bytes) * 100) : 0;
  $("#kpi-saved").textContent = `较无去重省 ${fmtMb(t.saved_bytes)} MB（${ratio}%）`;

  $("#kpi-peer").textContent = `${fmtMb(t.peer_bytes)} MB`;
  $("#kpi-peer-sub").textContent = "同架 + 跨架节点对传";
  $("#kpi-hits").textContent = String(t.local_hit_layers);

  const activeN = state.active.length;
  const queueN = state.queue.length;
  $("#kpi-flow").textContent = `${activeN} / ${queueN}`;
  $("#kpi-flow-sub").textContent = queueN ? "存在带宽争用排队" : "队列已清空";

  const ok = state.checks.single_backhaul;
  const check = $("#kpi-check");
  check.textContent = ok ? "✓ 通过" : "✗ 异常";
  check.style.color = ok ? "var(--status-good)" : "var(--status-critical)";
}

// ------------------------------------------------------------- 实时校验

function renderChecks() {
  const wrap = $("#verify-list");
  wrap.replaceChildren();
  const checks = [
    { ok: state.checks.single_backhaul,
      label: `每层仓库回源 ≤ 1 次（${state.ledger.length} 个层）` },
    { ok: state.checks.local_hit_zero_bytes,
      label: "本地命中零传输字节" },
    { ok: state.checks.all_layers_covered,
      label: "所有被请求的层均有来源覆盖" },
  ];
  checks.forEach((c) => {
    const pill = el("span", "verify-pill " + (c.ok ? "ok" : "bad"));
    pill.appendChild(el("span", "icon", c.ok ? "✓" : "✗"));
    pill.appendChild(el("span", null, c.label));
    wrap.appendChild(pill);
  });
}

// ------------------------------------------------------------- 层命中表

function renderLayerTable() {
  const tbody = $("#layer-table tbody");
  tbody.replaceChildren();
  state.ledger.forEach((row, idx) => {
    const tr = el("tr");
    const layerCell = el("td");
    const chip = el("span", "layer-chip");
    chip.style.background = LAYER_COLORS[idx % LAYER_COLORS.length];
    layerCell.appendChild(chip);
    layerCell.appendChild(document.createTextNode(row.layer_id));
    tr.appendChild(layerCell);

    tr.appendChild(el("td", "num", `${Math.round(row.size_mb)} MB`));
    tr.appendChild(countCell(row.registry_pulls, "reg"));
    tr.appendChild(countCell(row.peer_rack_pulls, "peer-r"));
    tr.appendChild(countCell(row.peer_cross_pulls, "peer-x"));
    tr.appendChild(countCell(row.local_hits, "hit"));
    tr.appendChild(el("td", "num", `${row.fanout} 节点`));

    const naive = row.size_mb * row.fanout;
    tr.appendChild(el("td", "num",
      `${fmtMb(row.registry_bytes_mb)} / ${fmtMb(naive)} MB`));

    const checkCell = el("td");
    checkCell.appendChild(el("span",
      row.single_backhaul_ok ? "check-ok" : "check-bad",
      row.single_backhaul_ok ? "✓ 1 次" : "✗ 多次"));
    tr.appendChild(checkCell);

    attachTip(tr, () => [
      el("strong", null, `${row.layer_id}（${row.size_mb} MB）持有节点`),
      el("div", "tt-row", row.nodes_with.join(", ") || "尚无节点缓存"),
      el("div", "tt-row",
        `仓库 ${row.registry_bytes_mb} MB · P2P ${row.peer_bytes_mb} MB`),
    ]);
    tbody.appendChild(tr);
  });
}

function countCell(n, cls) {
  const td = el("td", "num");
  td.appendChild(el("span", `badge ${cls}${n ? "" : " zero"}`, String(n)));
  return td;
}

// ------------------------------------------------------------- 利用率小多图 + sparkline

function renderUtils() {
  const wrap = $("#util-charts");
  wrap.replaceChildren();
  const hist = state.util_history;
  const last = hist[hist.length - 1];
  if (!last) return;

  const regCard = utilCard("仓库出口（WAN）", last.registry.used,
    last.registry.cap, hist.map((h) => h.registry.used),
    hist.map((h) => h.registry.cap));
  wrap.appendChild(regCard);

  Object.entries(last.racks).forEach(([rid, rv]) => {
    const used = rv.up_used + rv.lan_used;
    const cap = rv.up_cap + rv.lan_cap;
    const card = utilCard(`${rid} 上联+LAN`, used, cap,
      hist.map((h) => h.racks[rid].up_used + h.racks[rid].lan_used),
      hist.map((h) => h.racks[rid].up_cap + h.racks[rid].lan_cap));
    wrap.appendChild(card);
  });
  renderUtilTable(last);
}

function renderUtilTable(last) {
  const tbody = $("#util-table tbody");
  tbody.replaceChildren();
  const rows = [
    ["仓库出口（WAN）", last.registry.used, last.registry.cap],
    ...Object.entries(last.racks).map(([rid, rv]) =>
      [rid, rv.up_used + rv.lan_used, rv.up_cap + rv.lan_cap]),
  ];
  rows.forEach(([name, used, cap]) => {
    const tr = el("tr");
    tr.appendChild(el("td", null, name));
    tr.appendChild(el("td", "num", used.toFixed(1)));
    tr.appendChild(el("td", "num", cap.toFixed(0)));
    tr.appendChild(el("td", "num",
      `${cap ? ((used / cap) * 100).toFixed(0) : 0}%`));
    tbody.appendChild(tr);
  });
}

function utilCard(title, used, cap, series, capSeries) {
  const card = el("div", "util-card");
  const pct = cap ? Math.min(100, (used / cap) * 100) : 0;
  const head = el("div", "util-title");
  head.appendChild(el("span", null, title));
  head.appendChild(el("strong", null, `${used.toFixed(0)}/${cap.toFixed(0)} MB/s`));
  card.appendChild(head);

  const meter = el("div", "meter" + (pct > 98 ? " over" : ""));
  meter.appendChild(el("i"));
  meter.firstChild.style.width = `${pct}%`;
  card.appendChild(meter);
  card.appendChild(el("div", "meter-caption", `利用率 ${pct.toFixed(0)}%`));

  const svg = buildSparkline(series, capSeries);
  card.appendChild(svg);

  attachTip(card, () => [
    el("strong", null, title),
    el("div", "tt-row", `当前 ${used.toFixed(1)} / 容量 ${cap.toFixed(1)} MB/s`),
    el("div", "tt-row", `利用率 ${pct.toFixed(1)}%`),
  ]);
  return card;
}

function buildSparkline(series, capSeries) {
  const w = 240, h = 44, pad = 2;
  const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
  svg.setAttribute("viewBox", `0 0 ${w} ${h}`);
  svg.setAttribute("class", "spark");
  const maxV = Math.max(1, ...capSeries, ...series) * 1.05;
  const n = series.length;
  if (n < 2) return svg;
  const x = (i) => pad + (i / (n - 1)) * (w - 2 * pad);
  const y = (v) => h - pad - (v / maxV) * (h - 2 * pad);

  //  recessive gridlines
  [0.25, 0.5, 0.75].forEach((g) => {
    const line = document.createElementNS(svg.namespaceURI, "line");
    line.setAttribute("class", "grid");
    line.setAttribute("x1", pad); line.setAttribute("x2", w - pad);
    line.setAttribute("y1", h - g * (h - 2 * pad));
    line.setAttribute("y2", h - g * (h - 2 * pad));
    svg.appendChild(line);
  });

  // 容量参考线
  const capY = y(capSeries[capSeries.length - 1]);
  const capLine = document.createElementNS(svg.namespaceURI, "line");
  capLine.setAttribute("class", "cap-line");
  capLine.setAttribute("x1", pad); capLine.setAttribute("x2", w - pad);
  capLine.setAttribute("y1", capY); capLine.setAttribute("y2", capY);
  svg.appendChild(capLine);

  const pts = series.map((v, i) => `${x(i)},${y(v)}`).join(" ");
  const area = document.createElementNS(svg.namespaceURI, "polygon");
  area.setAttribute("class", "area");
  area.setAttribute("points",
    `${x(0)},${h - pad} ${pts} ${x(n - 1)},${h - pad}`);
  svg.appendChild(area);

  const line = document.createElementNS(svg.namespaceURI, "polyline");
  line.setAttribute("class", "line");
  line.setAttribute("points", pts);
  svg.appendChild(line);

  // 末端圆点（surface ring 由 CSS 同色描边模拟）
  const dot = document.createElementNS(svg.namespaceURI, "circle");
  dot.setAttribute("cx", x(n - 1)); dot.setAttribute("cy", y(series[n - 1]));
  dot.setAttribute("r", 3.5);
  dot.setAttribute("fill", "var(--series-1)");
  dot.setAttribute("stroke", "var(--surface-1)");
  dot.setAttribute("stroke-width", 2);
  svg.appendChild(dot);
  return svg;
}

// ------------------------------------------------------------- 作业与任务

function renderJobs() {
  const wrap = $("#jobs");
  wrap.replaceChildren();
  state.jobs.forEach((job) => {
    const card = el("div",
      "job" + (job.at_risk ? " risk" : "") +
      (job.state === "done_late" ? " late" : ""));
    const head = el("div", "job-head");
    const left = el("div");
    left.appendChild(el("span", "job-name",
      `${job.id} · ${job.node_id} · ${job.image_name}`));
    left.appendChild(el("span", "job-meta",
      `  提交 T${job.submitted_tick} · 时限 T${job.deadline_tick}`));
    head.appendChild(left);
    head.appendChild(el("span", `state-pill ${job.state}`,
      STATE_LABEL[job.state] || job.state));
    card.appendChild(head);

    job.tasks.forEach((t) => card.appendChild(taskRow(t)));
    wrap.appendChild(card);
  });
  if (!state.jobs.length) wrap.appendChild(el("div", "queue-empty", "暂无作业"));
}

function taskRow(t) {
  const row = el("div", "task-row");
  row.appendChild(el("span", "task-id", t.id));
  row.appendChild(el("span", "task-layer", t.layer_id));

  const meta = SOURCE_META[t.source] || { label: TASK_STATE_LABEL[t.state] || "—", cls: "none" };
  const tag = el("span", `src-tag ${meta.cls}`, meta.label);
  row.appendChild(tag);

  const bar = el("div", "bar" + (t.state === "hit" ? " hit" : ""));
  bar.appendChild(el("i"));
  bar.firstChild.style.width = `${t.pct}%`;
  row.appendChild(bar);

  const detail = t.state === "hit"
    ? "命中 0 MB"
    : t.state === "active"
      ? `${t.pct.toFixed(0)}% · ${t.allocated_mbps.toFixed(0)} MB/s`
      : t.state === "done"
        ? "完成"
        : `${t.remaining_mb.toFixed(0)} MB 待传`;
  row.appendChild(el("span", "task-pct", detail));

  const tipLines = [
    el("strong", null, `${t.id} · ${t.node_id} · ${t.layer_id}`),
    el("div", "tt-row",
      `状态 ${TASK_STATE_LABEL[t.state] || t.state} · 来源 ${meta.label}`),
  ];
  if (t.peer_node_id) tipLines.push(el("div", "tt-row", `种子节点 ${t.peer_node_id}`));
  if (t.wait_reason) tipLines.push(el("div", "tt-row", t.wait_reason));
  tipLines.push(el("div", "tt-row",
    `${t.progress_mb}/${t.size_mb} MB · 截止 T${t.deadline_tick}`));
  attachTip(row, () => tipLines);
  return row;
}

// ------------------------------------------------------------- 队列

function renderQueue() {
  const wrap = $("#queue");
  wrap.replaceChildren();
  if (!state.queue.length) {
    wrap.appendChild(el("div", "queue-empty", "队列为空 —— 所有层任务均已在途或完成"));
    return;
  }
  state.queue.forEach((t) => {
    const item = el("div",
      "queue-item" + (t.state === "wait_seed" ? " wait" : ""));
    item.appendChild(el("span", null, t.id));
    item.appendChild(el("span", null, `${t.node_id} ${t.layer_id}`));
    const meta = SOURCE_META[t.source] || null;
    item.appendChild(el("span", null, meta
      ? `${meta.label}${t.peer_node_id ? " ← " + t.peer_node_id : ""}`
      : TASK_STATE_LABEL[t.state]));
    item.appendChild(el("span", "job-meta", `截止 T${t.deadline_tick}`));
    if (t.wait_reason) item.appendChild(el("div", "queue-reason", t.wait_reason));
    attachTip(item, () => [
      el("strong", null, `${t.id}（EDF 优先序）`),
      el("div", "tt-row",
        `剩余 ${t.remaining_mb} MB · 大小 ${t.size_mb} MB · 截止 T${t.deadline_tick}`),
      el("div", "tt-row", t.wait_reason || "等待带宽资源"),
    ]);
    wrap.appendChild(item);
  });
}

// ------------------------------------------------------------- 事件流

function renderEvents() {
  const wrap = $("#events");
  wrap.replaceChildren();
  state.events.forEach((e) => {
    const row = el("div", "event");
    row.appendChild(el("span", "event-tick", `T${e.tick}`));
    row.appendChild(el("span", `event-level ${e.level === "info" ? "" : e.level}`));
    const body = el("div");
    body.appendChild(el("div", "event-title", e.title));
    if (e.detail) body.appendChild(el("div", "event-detail", e.detail));
    row.appendChild(body);
    wrap.appendChild(row);
  });
}

// ------------------------------------------------------------- 拓扑 / 缓存

function renderTopology() {
  const wrap = $("#topology");
  wrap.replaceChildren();
  const rackIds = Object.keys(state.racks);
  const allLayerIds = state.ledger.map((r) => r.layer_id);
  rackIds.forEach((rid) => {
    const rack = el("div", "rack");
    const cap = state.racks[rid];
    rack.appendChild(el("div", "rack-title",
      `${rid} 机架`));
    rack.firstChild.appendChild(el("span", null,
      `上联 ${cap.uplink_cap_mbps} · LAN ${cap.lan_cap_mbps} MB/s`));

    Object.entries(state.nodes)
      .filter(([, n]) => n.rack_id === rid)
      .forEach(([nid, n]) => {
        const row = el("div", "node-row");
        row.appendChild(el("span", "node-name", nid));
        const dots = el("div", "layer-dots");
        allLayerIds.forEach((lid, i) => {
          const has = n.cache.includes(lid);
          const dot = el("span", "dot", lid.replace("L", ""));
          if (has) {
            dot.style.background = LAYER_COLORS[i % LAYER_COLORS.length];
          } else {
            dot.style.background = "transparent";
            dot.style.color = "var(--text-muted)";
            dot.style.border = "1px dashed var(--baseline)";
          }
          attachTip(dot, () => [
            el("strong", null, `${nid} · ${lid}`),
            el("div", "tt-row", has ? "已缓存（本地命中可用）" : "未缓存"),
          ]);
          dots.appendChild(dot);
        });
        row.appendChild(dots);
        rack.appendChild(row);
      });
    wrap.appendChild(rack);
  });
}

// ------------------------------------------------------------- 控件初始化

async function initControls() {
  scenarios = (await fetchJSON("/api/scenarios")).scenarios;
  const sel = $("#scenario-select");
  scenarios.forEach((s) => {
    const opt = el("option", null, s.name);
    opt.value = s.name;
    opt.title = s.description;
    sel.appendChild(opt);
  });
  sel.value = "burst";
  sel.addEventListener("change", () =>
    postControl({ action: "reset", scenario: sel.value }));

  $("#btn-play").addEventListener("click", () =>
    postControl({ action: state.auto ? "pause" : "play" }));
  $("#btn-tick").addEventListener("click", () =>
    postControl({ action: "tick" }));
  $("#btn-reset").addEventListener("click", () =>
    postControl({ action: "reset", scenario: sel.value }));
  $("#speed-select").addEventListener("change", (e) =>
    postControl({ action: "speed", interval: Number(e.target.value) }));

  document.querySelectorAll("[data-factor]").forEach((btn) =>
    btn.addEventListener("click", () =>
      postControl({ action: "registry", factor: Number(btn.dataset.factor) })));

  // 动态加单下拉
  const nodeSel = $("#job-node");
  const imageSel = $("#job-image");
  const snap0 = await fetchJSON("/api/state");
  Object.keys(snap0.nodes).forEach((nid) =>
    nodeSel.appendChild(el("option", null, nid)));
  (snap0.image_names || []).forEach((img) =>
    imageSel.appendChild(el("option", null, img)));
  $("#btn-add-job").addEventListener("click", () =>
    postControl({
      action: "add_job",
      node_id: nodeSel.value,
      image: imageSel.value,
      deadline: Number($("#job-deadline").value || 12),
    }));

  // 主题
  const themeBtn = $("#btn-theme");
  themeBtn.addEventListener("click", () => {
    const root = document.documentElement;
    const dark = root.dataset.theme === "dark";
    root.dataset.theme = dark ? "light" : "dark";
  });

  // 数据表视图
  $("#table-toggle").addEventListener("click", (e) => {
    e.preventDefault();
    const on = document.body.classList.toggle("data-view");
    e.target.textContent = on ? "返回图形视图" : "显示数据表视图";
  });
}

// ------------------------------------------------------------- 启动

(async function main() {
  await initControls();
  await refresh();
  pollTimer = setInterval(refresh, 700);
})().catch((err) => {
  console.error(err);
  $("#scenario-desc").textContent = `加载失败：${err.message}`;
});
