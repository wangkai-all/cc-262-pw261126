"use strict";

const $ = (sel, root = document) => root.querySelector(sel);
const $$ = (sel, root = document) => Array.from(root.querySelectorAll(sel));

const state = {
  uploads: [],
  selectedUpload: null,
  secondUpload: null,
  jobs: [],
  events: [],
  cursor: 0,
  detailId: null,
  busy: false,
};

// ------------------------------------------------------------------ helpers

function fmtBytes(n) {
  if (n === null || n === undefined || Number.isNaN(n)) return "—";
  n = Number(n);
  if (Math.abs(n) < 1024) return `${n} B`;
  const units = ["KiB", "MiB", "GiB", "TiB"];
  let i = -1;
  do { n /= 1024; i++; } while (Math.abs(n) >= 1024 && i < units.length - 1);
  return `${n.toFixed(n >= 100 ? 0 : 1)} ${units[i]}`;
}

function pct(a, b) {
  if (!b) return 0;
  return Math.max(0, Math.min(100, (a / b) * 100));
}

function fmtTime(ts) {
  if (!ts) return "—";
  const d = new Date(ts * 1000);
  return d.toLocaleTimeString("zh-CN", { hour12: false });
}

function el(tag, attrs = {}, ...children) {
  const node = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (v === null || v === undefined || v === false) continue;
    if (k === "class") node.className = v;
    else if (k === "dataset") Object.assign(node.dataset, v);
    else if (k.startsWith("on")) node.addEventListener(k.slice(2), v);
    else node.setAttribute(k, v);
  }
  for (const child of children.flat()) {
    if (child === null || child === undefined || child === false) continue;
    node.append(child instanceof Node ? child : document.createTextNode(String(child)));
  }
  return node;
}

async function api(path, opts = {}) {
  const res = await fetch(path, {
    headers: { "Content-Type": "application/json", ...(opts.headers || {}) },
    ...opts,
  });
  let body = null;
  const text = await res.text();
  try { body = text ? JSON.parse(text) : {}; } catch { body = { raw: text }; }
  if (!res.ok) throw new Error(body.error || `HTTP ${res.status}`);
  return body;
}

function say(msg, kind = "") {
  const node = $("#submit-msg");
  node.textContent = msg;
  node.style.color = kind === "error" ? "var(--danger)" : kind === "ok" ? "var(--accent-2)" : "";
}

// ------------------------------------------------------------------ state

async function refresh() {
  try {
    const data = await api(`/api/state?after=0`);
    state.uploads = data.uploads || [];
    state.jobs = data.jobs || [];
    state.events = data.events || [];
    state.cursor = data.event_cursor || 0;
    render(data);
    $("#conn").classList.remove("bad");
    $("#conn").title = `轮询正常 · ${new Date().toLocaleTimeString("zh-CN", { hour12: false })}`;
  } catch (err) {
    $("#conn").classList.add("bad");
    $("#conn").title = String(err.message || err);
  }
}

function render(data) {
  const disk = data.ledger.disk;
  const mem = data.ledger.memory;
  const cfg = data.config;

  setText("disk-budget", fmtBytes(disk.budget));
  setText("disk-reserved", fmtBytes(disk.reserved));
  setText("disk-actual", fmtBytes(disk.actual));
  setText("disk-kept", fmtBytes(disk.kept));
  setText("disk-committed", fmtBytes(disk.committed));
  setText("disk-admit", fmtBytes(disk.admission_share));
  setText("disk-free", fmtBytes(disk.free_on_volume));
  setText("disk-hwm", fmtBytes(cfg.pressure_high_watermark * disk.budget));
  setText("disk-num", `${fmtBytes(disk.committed)} / ${fmtBytes(disk.budget)} · ${(disk.pressure * 100).toFixed(1)}%`);
  $("[data-k=disk-fill]").style.width = `${pct(disk.committed, disk.budget)}%`;
  $("[data-k=disk-marker]").style.left = `${cfg.pressure_high_watermark * 100}%`;

  setText("mem-budget", fmtBytes(mem.budget));
  setText("mem-reserved", fmtBytes(mem.reserved));
  setText("mem-actual", fmtBytes(mem.actual));
  setText("mem-admit", fmtBytes(mem.admission_share));
  setText("mem-num", `${fmtBytes(mem.actual)} / ${fmtBytes(mem.budget)}`);
  $("[data-k=mem-fill]").style.width = `${pct(mem.actual, mem.budget)}%`;

  const active = state.jobs.filter((j) => ["running", "finishing"].includes(j.status)).length;
  const queued = state.jobs.filter((j) => j.status === "queued").length;
  const paused = state.jobs.filter((j) => j.status === "paused_budget").length;
  const kept = state.jobs.filter((j) => j.status === "succeeded" && j.result_path).length;
  const debt = state.jobs.reduce((acc, j) => acc + ((j.reservation && j.reservation.debt_granted) || 0), 0);
  setText("active-jobs", String(active));
  setText("queued-jobs", String(queued));
  setText("paused-jobs", String(paused));
  setText("kept-count", String(kept));
  setText("debt-total", fmtBytes(debt));
  $("#policy-tag").textContent = policyLabel(data.policy);
  $("#policy").value = data.policy;
  $("#fairness-note").textContent = policyNote(data.policy);

  renderUploads(data.uploads || []);
  renderJobs(data.jobs || []);
  renderEvents(data.events || []);
  renderLedger(data.ledger_entries || []);
  if (data.recovery && Object.keys(data.recovery).length) {
    $("#recovery-report").textContent = JSON.stringify(data.recovery, null, 2);
  }
}

function setText(key, value) {
  $$(`[data-k="${key}"]`).forEach((n) => { n.textContent = value; });
}

function policyLabel(p) {
  return p === "throughput" ? "吞吐优先" : p === "adaptive" ? "自适应" : "公平优先";
}
function policyNote(p) {
  if (p === "throughput") return "单通道 FIFO：顺序 I/O 最省，总耗时最短，但长作业前排队的短作业要等很久。";
  if (p === "adaptive") return "磁盘压力低于高水位时按 FIFO 单通道；接近上限时切成公平多通道，优先腾出空间。";
  return "短作业优先 + 老化：小作业低延迟完成，等待超过老化窗口的大作业会被提升，避免饿死；代价是交错 I/O 略降吞吐。";
}

// ------------------------------------------------------------------ uploads

function renderUploads(uploads) {
  const list = $("#uploads");
  list.replaceChildren();
  if (!uploads.length) {
    list.append(el("li", { class: "muted" }, "没有输入文件。"));
  }
  for (const up of uploads) {
    list.append(el("li", {},
      el("span", {}, el("strong", {}, up.name), " ", el("span", { class: "muted" }, `${fmtBytes(up.bytes)} · ${up.id}`)),
      up.job_id ? el("span", { class: "muted" }, "作业引用中") : el("span", { class: "muted" }, "空闲"),
    ));
  }
  const sel = $("#second-upload");
  const current = sel.value;
  sel.replaceChildren(...uploads.map((u) => el("option", { value: u.id }, `${u.name} · ${fmtBytes(u.bytes)}`)));
  if (current) sel.value = current;
}

// ------------------------------------------------------------------ jobs

function renderJobs(jobs) {
  const showDone = $("#show-done").checked;
  const tbody = $("#jobs tbody");
  tbody.replaceChildren();
  const visible = jobs.filter((j) => showDone || !["succeeded", "cancelled", "purged", "failed"].includes(j.status));
  $("#jobs-empty").hidden = visible.length > 0;
  for (const job of visible) tbody.append(jobRow(job));
}

function jobRow(job) {
  const plan = job.plan || {};
  const hb = job.heartbeat;
  const progress = hb ? Math.round((hb.progress || 0) * 100) : 0;
  const nameCell = el("td", {},
    el("div", { class: "job-name", onclick: () => openDetail(job.id) }, job.name),
    el("div", { class: "job-id" }, job.id),
  );
  const status = el("td", {},
    el("span", { class: `pill ${job.status}` }, statusLabel(job.status)),
    el("div", { class: "stage" }, job.stage || "—"),
    job.scheduler_note ? el("div", { class: "stage" }, job.scheduler_note) : null,
  );
  const input = el("td", {},
    fmtBytes(job.source_bytes),
    job.second_bytes ? el("div", { class: "muted" }, `+ ${fmtBytes(job.second_bytes)}`) : null,
  );
  const planCell = el("td", {},
    el("div", {}, plan.kind || "待规划"),
    el("div", { class: "plan-note" }, job.plan_summary || plan.reason || "采样后决定分片/重分区"),
  );
  const res = job.reservation;
  const resCell = el("td", {},
    res ? fmtBytes(res.amount) : "—",
    res && res.debt_granted ? el("div", { class: "debt" }, `债务 ${fmtBytes(res.debt_granted)}`) : null,
    hb ? el("div", { class: "muted" }, `RSS ${fmtBytes(hb.rss_bytes)}`) : null,
  );
  const progCell = el("td", {},
    el("div", { class: "mini" }, el("i", { style: `width:${progress}%` })),
    el("div", { class: "muted" }, hb ? `${progress}% · ${hb.message || hb.stage}` : (job.error || "—")),
  );
  const ops = el("td", {}, el("div", { class: "ops" },
    canCancel(job.status) ? el("button", { onclick: () => act(job.id, "cancel") }, "取消") : null,
    canResume(job.status) ? el("button", { onclick: () => act(job.id, "resume") }, "恢复") : null,
    job.result_path ? el("a", { href: `/api/jobs/${job.id}/result`, class: "pill succeeded" }, "下载") : null,
    job.result_path ? el("button", { onclick: () => act(job.id, "purge") }, "删除结果") : null,
  ));
  return el("tr", {}, nameCell, el("td", {}, job.kind), status, input, planCell, resCell, progCell, ops);
}

function statusLabel(s) {
  return {
    uploading: "上传中", queued: "排队", running: "执行", finishing: "收尾",
    paused_budget: "预算停放", cancelling: "取消中", succeeded: "完成",
    failed: "失败", cancelled: "已取消", crashed: "崩溃(待恢复)", purged: "结果已删除",
  }[s] || s;
}
function canCancel(s) { return ["queued", "running", "paused_budget", "crashed", "finishing"].includes(s); }
function canResume(s) { return ["paused_budget", "failed", "cancelled"].includes(s); }

async function act(jobId, action) {
  try {
    await api(`/api/jobs/${jobId}/${action}`, { method: "POST", body: "{}" });
    await refresh();
  } catch (err) {
    alert(`操作失败: ${err.message}`);
  }
}

// ------------------------------------------------------------------ ledger

function renderLedger(entries) {
  const tbody = $("#ledger tbody");
  if (!entries) return;
  tbody.replaceChildren(...entries.slice(0, 60).map((e) => {
    const rd = e.reserved_delta;
    const ad = e.actual_delta;
    return el("tr", {},
      el("td", {}, fmtTime(e.ts)),
      el("td", { class: "job-id" }, e.job_id ? e.job_id.slice(0, 14) : "—"),
      el("td", {}, e.kind),
      el("td", { class: rd > 0 ? "pos" : rd < 0 ? "neg" : "" }, rd ? `${rd > 0 ? "+" : ""}${fmtBytes(rd)}` : "—"),
      el("td", { class: ad > 0 ? "pos" : ad < 0 ? "neg" : "" }, ad ? `${ad > 0 ? "+" : ""}${fmtBytes(ad)}` : "—"),
      el("td", { class: "muted" }, `${fmtBytes(e.balance_reserved)} / ${fmtBytes(e.balance_actual)}`),
      el("td", { class: "reason" }, e.reason),
    );
  }));
}

// ------------------------------------------------------------------ events

function renderEvents(events) {
  const ol = $("#events");
  ol.replaceChildren();
  $("#event-count").textContent = `${events.length} 条（新⇒旧）`;
  for (const ev of [...events].reverse().slice(0, 120)) {
    ol.append(el("li", {},
      el("span", { class: "t" }, fmtTime(ev.ts)),
      el("span", {}, el("span", { class: "k" }, ev.kind), " ", ev.message,
        ev.job_id ? el("span", { class: "muted" }, ` · ${ev.job_id.slice(0, 10)}`) : null),
    ));
  }
}

// ------------------------------------------------------------------ detail

async function openDetail(jobId) {
  state.detailId = jobId;
  const dialog = $("#detail");
  $("#detail-title").textContent = "作业详情";
  $("#detail-body").replaceChildren(el("p", { class: "muted" }, "加载中…"));
  dialog.showModal();
  await loadDetail();
}

async function loadDetail() {
  if (!state.detailId) return;
  try {
    if (!state.detailId) return;
    const job = await api(`/api/jobs/${state.detailId}`);
    const preview = await api(`/api/jobs/${state.detailId}/preview`).catch(() => ({ lines: [] }));
    renderDetail(job, preview.lines || []);
  } catch (err) {
    $("#detail-body").replaceChildren(el("p", { class: "muted" }, String(err.message)));
  }
}

function renderDetail(job, preview) {
  $("#detail-title").textContent = `${job.name} · ${job.id}`;
  const body = $("#detail-body");
  body.replaceChildren();
  const plan = job.plan || {};
  const res = job.reservation || {};
  const kv = el("div", { class: "kv" });
  const pairs = [
    ["状态", statusLabel(job.status)],
    ["阶段", job.stage],
    ["类型", job.kind],
    ["输入", fmtBytes(job.source_bytes)],
    ["第二输入", job.second_bytes ? fmtBytes(job.second_bytes) : "—"],
    ["磁盘预留", fmtBytes(res.amount || 0)],
    ["已核算", fmtBytes(res.used || 0)],
    ["债务", fmtBytes(res.debt_granted || 0)],
    ["结果", job.result_bytes ? fmtBytes(job.result_bytes) : "—"],
    ["尝试次数", job.attempts],
    ["创建", fmtTime(job.created_at)],
    ["完成", fmtTime(job.finished_at)],
  ];
  for (const [k, v] of pairs) kv.append(el("div", {}, el("span", {}, k), el("span", {}, String(v ?? "—"))));
  body.append(el("h3", {}, "概要"), kv);

  body.append(el("h3", {}, "计划"), el("div", {},
    el("div", {}, el("strong", {}, plan.kind || "—"), " · fan-in ", String(plan.fan_in ?? "—"),
      " · runs ", String(plan.run_count ?? "—"), " · partitions ", String(plan.partitions ?? "—")),
    el("p", { class: "muted" }, plan.reason || ""),
    plan.warnings && plan.warnings.length ? el("ul", {}, plan.warnings.map((w) => el("li", { class: "debt" }, w))) : null,
    el("pre", {}, JSON.stringify(plan.estimates || {}, null, 2)),
  ));

  if (job.stats && Object.keys(job.stats).length) {
    body.append(el("h3", {}, "统计"), el("pre", {}, JSON.stringify(job.stats, null, 2)));
  }

  body.append(el("h3", {}, "检查点"),
    el("div", {}, job.checkpoint ? `阶段 ${job.checkpoint.stage} · 引用产物 ${job.checkpoint.artifact_count}` : "无（未开始或已结束）"));

  body.append(el("h3", {}, "产物与引用"),
    el("table", {}, el("thead", {}, el("tr", {},
      ...["ID", "角色", "状态", "字节", "记录", "引用数", "路径"].map((h) => el("th", {}, h)))),
      el("tbody", {}, job.artifacts_detail.map((a) => el("tr", {},
        el("td", { class: "job-id" }, a.id),
        el("td", {}, a.role),
        el("td", {}, a.state),
        el("td", { class: "num" }, fmtBytes(a.accounted_bytes)),
        el("td", { class: "num" }, String(a.records)),
        el("td", { class: "num" }, String((a.refs || []).length)),
        el("td", { class: "muted" }, a.path),
      )))));

  body.append(el("h3", {}, "账本流水"),
    el("table", {}, el("thead", {}, el("tr", {},
      ...["时间", "动作", "预留Δ", "实际Δ", "余额", "原因"].map((h) => el("th", {}, h)))),
      el("tbody", {}, job.ledger_entries.map((e) => el("tr", {},
        el("td", {}, fmtTime(e.ts)),
        el("td", {}, e.kind),
        el("td", { class: "num" }, fmtBytes(e.reserved_delta)),
        el("td", { class: "num" }, fmtBytes(e.actual_delta)),
        el("td", { class: "muted" }, `${fmtBytes(e.balance_reserved)} / ${fmtBytes(e.balance_actual)}`),
        el("td", { class: "muted" }, e.reason),
      )))));

  if (preview.length) {
    body.append(el("h3", {}, "结果预览"), el("pre", {}, preview.join("\n")));
  }
  body.append(el("h3", {}, "事件"), el("pre", {}, job.events.map((e) =>
    `${fmtTime(e.ts)} ${e.kind}: ${e.message}`).join("\n")));
}

// ------------------------------------------------------------------ submit

function selectFile(file) {
  if (!file) return;
  state.selectedUpload = { file, name: file.name, size: file.size };
  const info = $("#upload-info");
  info.textContent = `已选 ${file.name} · ${fmtBytes(file.size)}（提交时上传）`;
}

async function uploadFile(file) {
  if (!file) throw new Error("没有选择文件");
  const res = await fetch("/api/uploads", {
    method: "POST",
    headers: { "X-Filename": encodeURIComponent(file.name), "Content-Type": "application/octet-stream" },
    body: file,
  });
  const body = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(body.error || `HTTP ${res.status}`);
  return body.upload;
}

async function submit() {
  const kind = $("#kind").value;
  const name = $("#job-name").value.trim();
  say("提交中…");
  try {
    let sourceId;
    if (state.selectedUpload) {
      const up = await uploadFile(state.selectedUpload.file);
      sourceId = up.id;
      state.selectedUpload = null;
      $("#upload-info").textContent = `已上传 ${up.name} · ${fmtBytes(up.bytes)}`;
    } else {
      const idle = state.uploads.filter((u) => !u.job_id);
      if (!idle.length) throw new Error("请选择文件或先生成合成数据");
      sourceId = idle[0].id;
    }
    let secondId = null;
    if (kind === "join") {
      secondId = $("#second-upload").value;
      if (!secondId) throw new Error("关联需要第二个输入");
      if (secondId === sourceId) {
        const other = state.uploads.find((u) => !u.job_id && u.id !== sourceId);
        if (!other) throw new Error("关联需要两个不同的输入文件");
        secondId = other.id;
      }
    }
    const fault = $("#fault-select").value;
    if (fault) await api("/api/faults", { method: "POST", body: JSON.stringify({ points: [fault] }) });
    const out = await api("/api/jobs", {
      method: "POST",
      body: JSON.stringify({ kind, source_upload: sourceId, second_upload: secondId, name: name || undefined }),
    });
    say(`已提交 ${out.job.id}`, "ok");
    $("#job-name").value = "";
    await refresh();
  } catch (err) {
    say(`失败：${err.message}`, "error");
  }
}

// ------------------------------------------------------------------ events

function wire() {
  $("#pick").addEventListener("click", () => $("#file").click());
  $("#file").addEventListener("change", (e) => selectFile(e.target.files[0]));
  const drop = $("#drop");
  drop.addEventListener("click", (e) => { if (e.target === drop) $("#file").click(); });
  for (const type of ["dragenter", "dragover"]) {
    drop.addEventListener(type, (e) => { e.preventDefault(); drop.classList.add("drag"); });
  }
  for (const type of ["dragleave", "drop"]) {
    drop.addEventListener(type, (e) => { e.preventDefault(); drop.classList.remove("drag"); });
  }
  drop.addEventListener("drop", (e) => selectFile(e.dataTransfer.files[0]));

  $("#gen-demo").addEventListener("click", async () => {
    say("生成合成数据…");
    try {
      const out = await api("/api/demo", {
        method: "POST",
        body: JSON.stringify({
          records: Number($("#demo-records").value) || 5000,
          skew: Number($("#demo-skew").value) || 20,
          sorted: $("#demo-sorted").checked,
        }),
      });
      say(`已生成 ${out.upload.name} · ${fmtBytes(out.upload.bytes)}`, "ok");
      state.selectedUpload = null;
      await refresh();
    } catch (err) { say(`生成失败：${err.message}`, "error"); }
  });

  $("#kind").addEventListener("change", () => {
    $("#second-wrap").hidden = $("#kind").value !== "join";
  });
  $("#submit").addEventListener("click", submit);
  $("#show-done").addEventListener("change", refresh);
  $("#policy").addEventListener("change", async (e) => {
    try { await api("/api/policy", { method: "POST", body: JSON.stringify({ policy: e.target.value }) }); await refresh(); }
    catch (err) { alert(err.message); }
  });
  for (const id of ["#recover", "#recover2"]) {
    $(id).addEventListener("click", async () => {
      try {
        const report = await api("/api/recover", { method: "POST", body: "{}" });
        $("#recovery-report").textContent = JSON.stringify(report, null, 2);
        await refresh();
      } catch (err) { alert(err.message); }
    });
  }
  $("#reset").addEventListener("click", async () => {
    if (!confirm("清空全部作业、账本、事件与文件？")) return;
    try { await api("/api/reset", { method: "POST", body: "{}" }); await refresh(); }
    catch (err) { alert(err.message); }
  });
  $("#fault-select").addEventListener("change", async (e) => {
    const points = e.target.value ? [e.target.value] : [];
    try { await api("/api/faults", { method: "POST", body: JSON.stringify({ points }) }); }
    catch (err) { alert(err.message); }
  });
  $("#detail").addEventListener("close", () => { state.detailId = null; });
}

wire();
refresh();
setInterval(refresh, 1000);
setInterval(loadDetail, 1500);
