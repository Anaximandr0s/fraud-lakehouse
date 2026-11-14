"use strict";

const SVG = "http://www.w3.org/2000/svg";
const H = 64;

// x, y, width of each box in the 1188 × 740 diagram.
const LAYOUT = {
  csv: [24, 96, 150], producer: [214, 96, 150], kafka: [214, 206, 150],
  "spark-streaming": [404, 206, 170], dbt: [404, 316, 170], airflow: [404, 426, 170],
  bronze: [614, 96, 170], silver: [614, 206, 170], gold: [614, 316, 170],
  "spark-thrift": [824, 206, 150], metabase: [1014, 206, 150], jupyter: [1014, 316, 150],
  "spark-master": [214, 560, 150], "spark-worker": [404, 560, 170], minio: [614, 560, 170],
  "hive-metastore": [824, 560, 150], postgres: [1014, 560, 150],
  cadvisor: [214, 664, 150], prometheus: [404, 664, 170], grafana: [614, 664, 170],
};

const LANES = [
  [24, 150, "Source"], [214, 150, "Ingestion"], [404, 170, "Processing"],
  [614, 170, "Lakehouse"], [824, 150, "Query"], [1014, 150, "Consumers"],
];

const ICONS = {
  file: "M3 1.5h6.5L13 5v9.5H3z M9.5 1.5V5H13",
  send: "M2 8h9 M8 4.5L11.5 8 8 11.5 M13.5 3v10",
  log: "M2 3.5h12 M2 6.5h12 M2 9.5h12 M2 12.5h8",
  stream: "M1.5 5c2-2 4 2 6.5 0s4.5 2 6.5 0 M1.5 11c2-2 4 2 6.5 0s4.5 2 6.5 0",
  server: "M2 2.5h12v4H2z M2 9.5h12v4H2z M4.5 4.5h1 M4.5 11.5h1",
  table: "M2 2.5h12v11H2z M2 6h12 M2 9.5h12 M6.5 6v7.5",
  bucket: "M2.5 4h11l-1.3 9.5H3.8z M2.5 4c0-1.2 11-1.2 11 0",
  db: "M3 3.5c0-1.5 10-1.5 10 0v9c0 1.5-10 1.5-10 0z M3 3.5c0 1.5 10 1.5 10 0 M3 8c0 1.5 10 1.5 10 0",
  dag: "M2 3h4v3H2z M10 3h4v3h-4z M6 10h4v3H6z M4 6v2h8V6 M8 8v2",
  sql: "M5 3L1.5 8 5 13 M11 3l3.5 5-3.5 5 M9.5 2.5l-3 11",
  chart: "M2 13.5h12 M4 11V7 M7 11V4 M10 11V8 M13 11V5.5",
  notebook: "M3.5 1.5h9v13h-9z M6 5h4.5 M6 7.5h4.5",
  gauge: "M2 11a6 6 0 0 1 12 0 M8 11l3-4",
  panel: "M2 2.5h12v11H2z M2 6.5h12 M7 6.5v7",
  box: "M2 4.5l6-3 6 3v7l-6 3-6-3z M2 4.5l6 3 6-3 M8 7.5v7",
};

const ICON_OF = {
  csv: "file", producer: "send", kafka: "log", "spark-streaming": "stream", dbt: "sql",
  airflow: "dag", bronze: "table", silver: "table", gold: "table", "spark-thrift": "server",
  metabase: "chart", jupyter: "notebook", "spark-master": "server", "spark-worker": "server",
  minio: "bucket", "hive-metastore": "db", postgres: "db", cadvisor: "box",
  prometheus: "gauge", grafana: "panel",
};

const TIER = { bronze: "var(--bronze)", silver: "var(--silver)", gold: "var(--gold)" };

const FIXED_SUB = {
  bronze: "append only", silver: "one row per trans_num", gold: "aggregates · features",
  dbt: "SQL via Thrift",
};

const NOTES = {
  producer: "With monitoring on, a pause sends “Target down” after 1 min and “No streaming input” after about 5 min.",
  airflow: "While it is stopped, Gold stops refreshing; Bronze and Silver keep filling.",
  prometheus: "Stopped, Grafana alert rules cannot query and report an error by email.",
  cadvisor: "Stopped, Grafana emails “Target down” after 1 min.",
};

const running = (s, key) => status(s, key) === "running";
// The container's own state, health aside: an unhealthy container is still up and can be stopped.
const up = (s, key) => Boolean(s && s.services[key] && s.services[key].state === "running");
const coreUp = (s) => ["kafka", "postgres", "minio", "spark-thrift"].every((k) => up(s, k));
const streamingIn = (s) => (s.streaming.input_per_second || 0) > 0 && running(s, "spark-streaming");
const producing = (s) => s.producer.rate > 0;

// [path, live(state), label(state), label position, dotted]
const EDGES = [
  ["M174 128H214", producing],
  ["M289 160V206", producing, (s) => `${fmtRate(s.producer.rate)}/s`, [296, 188]],
  ["M364 238H404", streamingIn, (s) => `${fmtRate(s.streaming.input_per_second)}/s`, [367, 230]],
  ["M574 228H594V128H614", streamingIn],
  ["M699 160V206", streamingIn, () => "MERGE", [706, 188]],
  ["M699 270V316", (s) => running(s, "airflow"), () => "staging view", [706, 298]],
  ["M574 348H614", (s) => running(s, "airflow")],
  ["M489 426V380", (s) => running(s, "airflow"), () => "every 10 min", [496, 408]],
  ["M784 230H824", (s) => running(s, "metabase")],
  ["M784 340H804V250H824", (s) => running(s, "metabase")],
  ["M974 238H1014", (s) => running(s, "metabase"), () => "JDBC", [979, 230]],
  ["M784 356H1014", (s) => running(s, "jupyter"), () => "PySpark", [880, 350]],
  ["M364 696H404", (s) => running(s, "cadvisor") && running(s, "prometheus")],
  ["M574 696H614", (s) => running(s, "prometheus") && running(s, "grafana")],
  ["M784 696H824", (s) => running(s, "grafana"), () => "email alerts", [830, 700]],
  ["M699 392V560", null, () => "data and metadata files", [706, 480], true],
  ["M899 560V270", null, () => "catalog", [905, 420], true],
  ["M974 592H1014", null, null, null, true],
  ["M364 592H404", null, null, null, true],
];

let catalog = [];
let state = null;
let selected = null;
let watchingJob = false;
let actedAt = 0;
let panelHtml = "";

const $ = (id) => document.getElementById(id);

function el(name, attrs = {}, parent) {
  const node = document.createElementNS(SVG, name);
  for (const [k, v] of Object.entries(attrs)) node.setAttribute(k, v);
  if (parent) parent.appendChild(node);
  return node;
}

function text(parent, x, y, cls, value) {
  const t = el("text", { x, y, class: cls }, parent);
  t.textContent = value;
  return t;
}

function fmtInt(n) { return Number(n || 0).toLocaleString("en-US"); }
function fmtRate(n) { return n == null ? "–" : n >= 100 ? Math.round(n).toString() : Number(n).toFixed(1); }
function fmtDuration(sec) {
  if (sec == null) return "";
  const h = Math.floor(sec / 3600), m = Math.round((sec % 3600) / 60);
  return h ? `${h} h ${m} min` : `${m} min`;
}

function status(s, key) {
  const svc = s && s.services[key];
  if (!svc) return "stopped";
  if (svc.state === "running") {
    if (svc.health === "starting") return "starting";
    if (svc.health === "unhealthy") return "failed";
    return "running";
  }
  if (svc.state === "restarting" || svc.state === "dead") return "failed";
  return "stopped";
}

function drawStatic() {
  const lanes = $("lanes");
  for (const [x, w, name] of LANES) {
    text(lanes, x, 50, "lane-title", name.toUpperCase());
    el("line", { x1: x, x2: x + w, y1: 62, y2: 62, class: "lane-rule" }, lanes);
  }
  el("rect", { x: 600, y: 82, width: 198, height: 310, class: "frame" }, lanes);
  text(lanes, 600, 410, "frame-label", "Iceberg · s3a://lakehouse/warehouse");
  for (const [y, name] of [[530, "Platform"], [634, "Observability"]]) {
    el("line", { x1: 24, x2: 1164, y1: y, y2: y, class: "lane-rule" }, lanes);
    text(lanes, 24, y + 22, "row-title", name.toUpperCase());
  }
  const edges = $("edges");
  for (const [d, , , pos, dotted] of EDGES) {
    const path = el("path", { d, class: dotted ? "edge dotted" : "edge" }, edges);
    if (!dotted) path.setAttribute("marker-end", "url(#head)");
    if (pos) text(edges, pos[0], pos[1], "edge-label", "");
  }
  const nodes = $("nodes");
  for (const node of catalog) {
    const [x, y, w] = LAYOUT[node.key];
    const g = el("g", { class: node.container ? "node" : "node data", "data-key": node.key,
                        tabindex: 0, role: "button", "aria-label": node.label }, nodes);
    el("rect", { x, y, width: w, height: H, rx: 2, class: "card" }, g);
    if (TIER[node.key]) el("rect", { x, y, width: 4, height: H, class: "tier", fill: TIER[node.key] }, g);
    const icon = el("g", { transform: `translate(${x + 12} ${y + 13})` }, g);
    el("path", { d: ICONS[ICON_OF[node.key]], class: "icon" }, icon);
    text(g, x + 36, y + 26, "name", node.label);
    text(g, x + 12, y + 50, "sub", "");
    if (node.container) el("rect", { x: x + w - 16, y: y + 8, width: 8, height: 8, class: "status stopped" }, g);
    g.addEventListener("click", () => openPanel(node.key));
    g.addEventListener("keydown", (e) => { if (e.key === "Enter") openPanel(node.key); });
  }
}

function subText(node, s) {
  if (node.key === "csv") return `${fmtInt(s.producer.total)} rows`;
  if (FIXED_SUB[node.key]) return FIXED_SUB[node.key];
  const st = status(s, node.key);
  if (node.key === "producer" && st === "running") {
    return s.producer.finished ? "whole CSV sent" : `${fmtRate(s.producer.rate)}/s · ${s.producer.percent} %`;
  }
  if (node.key === "spark-streaming" && st === "running" && s.streaming.batch_ms != null) {
    return `batch ${(s.streaming.batch_ms / 1000).toFixed(1)} s`;
  }
  const stats = s.stats[node.key];
  if (st === "running" && stats) return stats.memory.replace(/\s*\/\s*/, " / ");
  return st === "stopped" ? (node.profile ? `stopped · ${node.profile}` : "stopped") : st;
}

function render() {
  if (!state) return;
  for (const node of catalog) {
    const g = document.querySelector(`.node[data-key="${node.key}"]`);
    g.querySelector(".sub").textContent = subText(node, state);
    if (!node.container) continue;
    const st = status(state, node.key);
    g.querySelector(".status").setAttribute("class", `status ${st}`);
    g.classList.toggle("down", st === "stopped");
  }
  const paths = document.querySelectorAll("#edges path");
  const labels = document.querySelectorAll("#edges text");
  let li = 0;
  EDGES.forEach(([, live, label, pos, dotted], i) => {
    const on = !dotted && live && live(state);
    if (!dotted) {
      paths[i].setAttribute("class", on ? "edge live" : "edge");
      paths[i].setAttribute("marker-end", on ? "url(#head-live)" : "url(#head)");
    }
    if (pos) {
      labels[li].textContent = label ? label(state) : "";
      labels[li].setAttribute("class", on ? "edge-label live" : "edge-label");
      li += 1;
    }
  });
  renderFooter();
  if (selected) openPanel(selected, true);
  if (!$("docker").hidden) renderDocker();
}

function renderFooter() {
  const p = state.producer;
  const isUp = up(state, "producer");
  const label = !state.services.producer ? "not created" : isUp ? (p.finished ? "finished" : "running") : "paused";
  const badge = $("producer-state");
  badge.textContent = label;
  badge.className = `badge ${label.replace(" ", "-")}`;
  $("producer-bar").style.width = p.known ? `${p.percent}%` : "0";
  let line = p.known ? `${fmtInt(p.position)} / ${fmtInt(p.total)} rows · ${p.percent} %` : "position unknown until the producer exists: press Start all";
  if (p.known && label === "running") line += ` · ${fmtRate(p.rate)} ev/s` + (p.eta_seconds ? ` · ${fmtDuration(p.eta_seconds)} left` : "");
  $("producer-text").textContent = line;
  const toggle = $("producer-toggle");
  toggle.textContent = isUp ? "Pause" : "Resume";
  toggle.dataset.action = isUp ? "stop" : "start";
  toggle.disabled = !isUp && !coreUp(state);
  const sm = state.streaming;
  $("streaming-text").textContent = running(state, "spark-streaming")
    ? `Spark in ${fmtRate(sm.input_per_second)}/s · processed ${fmtRate(sm.processed_per_second)}/s · last batch ${sm.batch_ms == null ? "–" : (sm.batch_ms / 1000).toFixed(1) + " s"}`
    : "Spark streaming stopped";
}

function esc(v) {
  return String(v).replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" })[c]);
}

function openPanel(key, refresh = false) {
  const node = catalog.find((n) => n.key === key);
  if (key !== selected) panelHtml = null;
  selected = key;
  document.querySelectorAll(".node").forEach((g) => g.classList.toggle("selected", g.dataset.key === key));
  const svc = state && state.services[key];
  const stats = state && state.stats[key];
  const st = status(state, key);
  const rows = [];
  if (node.container) {
    rows.push(["State", svc ? `${svc.state}${svc.health ? " · " + svc.health : ""}` : "not created"]);
    if (svc) rows.push(["Image", svc.image]);
    if (stats) rows.push(["Memory", `${stats.memory} (${stats.memory_percent.toFixed(0)} %)`], ["CPU", `${stats.cpu_percent.toFixed(1)} %`]);
    if (node.profile) rows.push(["Profile", node.profile]);
  }
  if (node.ports.length) rows.push(["Ports", node.ports.join("<br>")]);
  let action = "";
  if (node.stoppable) {
    const verb = up(state, key) ? "stop" : "start";
    const name = key === "producer" ? (verb === "stop" ? "Pause producer" : "Resume producer") : `${verb === "stop" ? "Stop" : "Start"} ${node.label}`;
    const blocked = verb === "start" && !coreUp(state);
    action = `<button class="${verb === "stop" ? "" : "primary"}" data-action="${verb}" data-key="${key}"${blocked ? " disabled" : ""}>${name}</button>
      ${blocked ? `<p class="note">Core services are down: press Start all first.</p>` : ""}
      <p class="note">Ingestion (Kafka to Silver) never waits on it, so it can be stopped during a demo.${NOTES[key] ? " " + NOTES[key] : ""}</p>`;
  } else if (node.container) {
    action = `<p class="note">Core service: the pipeline depends on it, so it cannot be stopped from here.</p>`;
  }
  const info = `
    <div class="layer">${esc(node.layer)}</div>
    <h2>${esc(node.label)}</h2>
    <p class="role">${esc(node.role)}</p>
    ${rows.length ? `<dl>${rows.map(([k, v]) => `<dt>${k}</dt><dd>${k === "Ports" ? v : esc(v)}</dd>`).join("")}</dl>` : ""}
    ${node.details.length ? `<ul>${node.details.map((d) => `<li>${esc(d)}</li>`).join("")}</ul>` : ""}
`;
  const links = node.links.map((l) => `<a href="${l.url}" target="_blank" rel="noopener">${esc(l.label)}<span>${esc(l.url.replace("http://", "").split("/")[0])} ↗</span></a>`).join("");
  const stable = (links ? `<div class="links">${links}</div>` : "") + action;
  $("panel-info").innerHTML = info;
  // Rewritten only on change, or a refresh between mousedown and mouseup would swallow a click.
  if (stable !== panelHtml) {
    $("panel-action").innerHTML = stable;
    panelHtml = stable;
  }
  $("panel").hidden = false;
  if (!refresh) $("panel").scrollTop = 0;
}

function closePanel() {
  selected = null;
  panelHtml = "";
  $("panel").hidden = true;
  document.querySelectorAll(".node.selected").forEach((g) => g.classList.remove("selected"));
}

function renderDocker() {
  const keys = Object.keys(state.services).sort();
  $("docker-rows").innerHTML = keys.map((k) => {
    const s = state.services[k], m = state.stats[k];
    return `<tr><td>${esc(k)}</td><td>${esc(s.state)}</td><td>${esc(s.health || "–")}</td><td>${esc(s.image)}</td>
      <td class="num">${m ? esc(m.memory) : "–"}</td><td class="num">${m ? m.memory_percent.toFixed(1) : "–"}</td>
      <td class="num">${m ? m.cpu_percent.toFixed(1) : "–"}</td></tr>`;
  }).join("") || `<tr><td colspan="7">No container: press Start all.</td></tr>`;
}

const TITLES = {
  "start-all": "Starting every service", "stop-all": "Stopping every service", reset: "Deleting every volume",
};

function jobTitle(job) {
  if (TITLES[job.action]) return TITLES[job.action];
  const node = catalog.find((n) => n.key === job.key);
  return `${job.action === "stop" ? "Stopping" : "Starting"} ${node ? node.label : job.key}`;
}

function showJob(job) {
  $("busy").hidden = false;
  $("busy-title").textContent = jobTitle(job);
  $("busy-log").textContent = job.log.join("\n");
  $("busy-log").scrollTop = $("busy-log").scrollHeight;
  const elapsed = Math.round(((job.finished || Date.now() / 1000) - job.started));
  $("busy-time").textContent = `${elapsed} s`;
  const spinner = document.querySelector("#busy .spinner");
  spinner.className = job.running ? "spinner" : job.ok ? "spinner done" : "spinner error";
  $("busy-close").hidden = job.running || job.ok;
  if (!job.running && job.ok) setTimeout(() => { $("busy").hidden = true; }, 1200);
  if (!job.running && !job.ok) $("busy-title").textContent += " failed";
}

async function refresh() {
  try {
    const res = await fetch("/api/state", { cache: "no-store" });
    state = await res.json();
  } catch {
    return;
  }
  render();
  // A state requested before the click still describes the previous job.
  if (watchingJob && !(state.job.started >= actedAt - 1)) return;
  if (state.job.running || watchingJob) {
    showJob(state.job);
    watchingJob = state.job.running;
  }
}

function confirmAction(button) {
  return new Promise((resolve) => {
    $("confirm-title").textContent = button.textContent.trim();
    $("confirm-text").textContent = button.dataset.confirm;
    const typed = button.dataset.type;
    $("confirm-type-label").hidden = !typed;
    $("confirm-type").value = "";
    $("confirm").hidden = false;
    const finish = (ok) => {
      $("confirm").hidden = true;
      $("confirm-ok").onclick = $("confirm-cancel").onclick = null;
      resolve(ok);
    };
    $("confirm-ok").onclick = () => {
      if (typed && $("confirm-type").value.trim() !== typed) { $("confirm-type").focus(); return; }
      finish(true);
    };
    $("confirm-cancel").onclick = () => finish(false);
    (typed ? $("confirm-type") : $("confirm-ok")).focus();
  });
}

async function act(button) {
  if (button.dataset.confirm && !(await confirmAction(button))) return;
  const key = button.dataset.key || (button.id === "producer-toggle" ? "producer" : null);
  let res;
  try {
    res = await fetch("/api/action", {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-Demo": "1" },
      body: JSON.stringify({ action: button.dataset.action, key }),
    });
  } catch {
    alert("The demo server does not answer: is make demo still running?");
    return;
  }
  if (res.status === 202) {
    actedAt = Date.now() / 1000;
    watchingJob = true;
    showJob({ running: true, action: button.dataset.action, key, log: [], started: Date.now() / 1000 });
  } else {
    alert((await res.json()).error);
  }
}

document.addEventListener("click", (e) => {
  const button = e.target.closest("button[data-action]");
  if (button) act(button);
});
document.querySelector("#panel .close").addEventListener("click", closePanel);
$("containers").addEventListener("click", () => { $("docker").hidden = false; if (state) renderDocker(); });
document.querySelector("#docker .close").addEventListener("click", () => { $("docker").hidden = true; });
$("busy-close").addEventListener("click", () => { $("busy").hidden = true; watchingJob = false; });
document.addEventListener("keydown", (e) => {
  if (e.key !== "Escape") return;
  if (!$("docker").hidden) $("docker").hidden = true;
  else if (!$("panel").hidden) closePanel();
});

(async () => {
  catalog = await (await fetch("/api/catalog")).json();
  drawStatic();
  await refresh();
  setInterval(refresh, 2000);
})();
