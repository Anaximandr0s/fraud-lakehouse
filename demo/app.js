"use strict";

const SVG = "http://www.w3.org/2000/svg";

const STAGE_TITLE = {
  ingest: "1 · Ingest", process: "2 · Process", store: "3 · Store", serve: "4 · Serve",
  platform: "Platform", observability: "Observability",
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
const goldRefreshing = (s) => running(s, "airflow");

// [path, live(state) or "meta", label, [x, y, anchor]] in the 1920 × 1060 diagram.
const EDGES = [
  ["M205 545H286", producing],
  ["M430 545H466", producing],
  ["M610 545H646", producing],
  ["M810 545H871", streamingIn],
  ["M1015 545H1036", streamingIn],
  ["M1180 545H1201", goldRefreshing],
  ["M1345 545H1411", (s) => running(s, "spark-thrift")],
  ["M1555 545H1580V445H1601", (s) => running(s, "metabase")],
  ["M1580 545V645H1601", (s) => running(s, "spark-thrift")],
  ["M1725 445H1801", (s) => running(s, "metabase")],
  ["M1725 645H1801", (s) => running(s, "spark-thrift")],
  ["M1370 690H1440V835H1601", (s) => running(s, "jupyter"), "PySpark", [1450, 770, "start"]],
  ["M1725 835H1801", (s) => running(s, "jupyter")],
  ["M945 375V215H1016", "meta"],
  ["M1110 375V274", "meta"],
  ["M1275 375V215H1204", "meta"],
  ["M1205 165H1801", "meta"],
  ["M910 165H1016", "meta"],
  ["M1110 790V729", "meta", "Parquet + metadata", [1120, 765, "start"]],
  ["M1295 790V729", goldRefreshing, "builds Gold", [1305, 765, "start"]],
  ["M1295 915V874", goldRefreshing, "every 10 min", [1312, 898, "start"]],
  ["M730 610V672", "meta", "runs on", [740, 645, "start"]],
  ["M730 746V762", "meta"],
  ["M430 915H461", (s) => running(s, "cadvisor") && running(s, "prometheus")],
  ["M595 915H626", (s) => running(s, "prometheus") && running(s, "grafana")],
];

const CADVISOR = "M2 4.5l6-3 6 3v7l-6 3-6-3z M2 4.5l6 3 6-3 M8 7.5v7";
const TERMINAL = "M1.5 2.5h13v11h-13z M4 6l2.5 2L4 10 M8 10.5h4";

// [kind, ...geometry, icon, extra]: card x y w h icon · cyl cx top w h icon labelY · tier x y colour role.
const SHAPES = {
  csv: ["cyl", 128, 482, 150, 150, null, 445],
  producer: ["card", 290, 495, 140, 100, "python"],
  kafka: ["card", 470, 495, 140, 100, "kafka"],
  "spark-streaming": ["card", 650, 480, 160, 130, "spark"],
  "spark-master": ["card", 650, 676, 160, 70, "spark"],
  "spark-worker": ["card", 650, 766, 160, 70, "spark"],
  bronze: ["tier", 875, 440, "var(--bronze)", "RAW EVENTS"],
  silver: ["tier", 1040, 440, "var(--silver)", "CLEANED"],
  gold: ["tier", 1205, 440, "var(--gold)", "BUSINESS"],
  "hive-metastore": ["cyl", 1110, 110, 180, 165, "hive", 78],
  postgres: ["card", 760, 120, 150, 90, "postgresql"],
  minio: ["card", 1010, 790, 200, 120, "minio"],
  dbt: ["card", 1235, 790, 120, 84, "dbt"],
  airflow: ["card", 1235, 915, 120, 84, "airflow"],
  "spark-thrift": ["cyl", 1485, 475, 140, 140, "spark", 438],
  metabase: ["card", 1605, 400, 120, 90, "metabase"],
  beeline: ["card", 1605, 600, 120, 90, TERMINAL],
  jupyter: ["card", 1605, 790, 120, 90, "jupyter"],
  cadvisor: ["card", 300, 870, 130, 90, CADVISOR],
  prometheus: ["card", 465, 870, 130, 90, "prometheus"],
  grafana: ["card", 630, 870, 130, 90, "grafana"],
};

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
function fmtBytes(n) {
  if (n == null) return "–";
  const units = ["B", "kB", "MB", "GB"];
  let i = 0;
  while (n >= 1000 && i < units.length - 1) { n /= 1000; i += 1; }
  return `${n >= 10 || i === 0 ? Math.round(n) : n.toFixed(1)} ${units[i]}`;
}
function fmtDuration(sec) {
  if (sec == null) return "";
  const h = Math.floor(sec / 3600), m = Math.round((sec % 3600) / 60);
  return h ? `${h} h ${m} min` : `${m} min`;
}
function fmtAgo(ms) {
  if (!ms) return "–";
  const sec = Math.max(0, Math.round(Date.now() / 1000 - ms / 1000));
  return sec < 90 ? `${sec} s` : sec < 5400 ? `${Math.round(sec / 60)} min` : `${Math.round(sec / 3600)} h`;
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


function logo(parent, name, cx, cy, size) {
  el("image", { href: `icons/${name}.svg`, x: cx - size / 2, y: cy - size / 2, width: size, height: size }, parent);
}

function icon(parent, name, cx, cy, size) {
  if (!name.startsWith("M")) { logo(parent, name, cx, cy, size); return; }
  const scale = size / 16;
  const path = el("path", { d: name, class: "glyph", transform: `translate(${cx - size / 2} ${cy - size / 2}) scale(${scale})` }, parent);
  path.style.strokeWidth = 3 / scale;
}

function drawCard(g, x, y, w, h, name) {
  el("rect", { x, y, width: w, height: h, rx: 14, class: "card" }, g);
  // Short cards put the logo beside the text: stacked, the two lines would overlap.
  const short = h < 80;
  const tx = short ? x + w / 2 + 16 : x + w / 2;
  icon(g, name, short ? x + 28 : tx, short ? y + h / 2 : y + h * 0.3, short ? 30 : Math.min(44, h * 0.38));
  text(g, tx, y + h * (short ? 0.46 : 0.66), "name", "");
  text(g, tx, y + h * (short ? 0.74 : 0.86), "sub", "");
  return [x + w - 4, y + 4];
}

function drawCylinder(g, cx, top, w, h, name, labelY) {
  const rx = w / 2, ry = h * 0.14, bottom = top + h;
  el("path", { d: `M${cx - rx} ${top + ry}V${bottom - ry}A${rx} ${ry} 0 0 0 ${cx + rx} ${bottom - ry}V${top + ry}`, class: "cyl" }, g);
  el("ellipse", { cx, cy: top + ry, rx, ry, class: "cyl" }, g);
  if (name) {
    logo(g, name, cx, top + h * 0.58, w * 0.42);
  } else {
    for (const f of [0.42, 0.66]) {
      el("path", { d: `M${cx - rx} ${top + h * f}A${rx} ${ry} 0 0 0 ${cx + rx} ${top + h * f}`, class: "cyl" }, g);
    }
    for (const f of [0.31, 0.55, 0.79]) {
      for (const dx of [22, 36, 50]) el("circle", { cx: cx + rx - dx, cy: top + h * f + 10, r: 4, class: "cyl-dot" }, g);
    }
  }
  text(g, cx, labelY, "caption", "");
  text(g, cx, labelY + 20, "sub", "");
  return [cx + rx - 6, top + 6];
}

// Bronze, Silver and Gold share one database-and-folders glyph, each in its own colour.
function drawTier(g, x, y, color, role) {
  g.style.color = color;
  text(g, x + 70, y - 12, "tier-name", "");
  el("rect", { x, y, width: 140, height: 210, rx: 14, class: "tier-box" }, g);
  const gl = el("g", { class: "tier-glyph", transform: `translate(${x + 22} ${y + 38})` }, g);
  for (const dy of [0, 18, 36]) el("path", { d: `M0 ${dy + 8}a20 8 0 0 0 40 0` }, gl);
  el("ellipse", { cx: 20, cy: 8, rx: 20, ry: 8 }, gl);
  el("path", { d: "M0 8V52a20 8 0 0 0 40 0V8 M40 30H52 M52 6V76 M52 6H60 M52 41H60 M52 76H60" }, gl);
  for (const fy of [0, 35, 70]) el("path", { d: `M60 ${fy - 6}h10l3 4h24v24H60z` }, gl);
  text(g, x + 70, y + 160, "tier-role", role);
  text(g, x + 70, y + 184, "sub", "");
  return null;
}

function persona(parent, x, y, name, sub) {
  el("circle", { cx: x, cy: y - 16, r: 15, class: "persona" }, parent);
  el("path", { d: `M${x - 25} ${y + 22}a25 22 0 0 1 50 0z`, class: "persona" }, parent);
  text(parent, x, y + 44, "persona-name", name);
  text(parent, x, y + 61, "persona-sub", sub);
}

function drawStatic() {
  const back = $("back");
  el("rect", { x: 262, y: 50, width: 1496, height: 980, rx: 44, class: "frame" }, back);
  logo(back, "docker", 352, 128, 110);
  text(back, 352, 222, "docker-word", "docker");
  text(back, 450, 470, "heading", "Data Ingestion");
  text(back, 730, 462, "heading", "Stream Processing");
  text(back, 530, 850, "heading", "Monitoring");
  el("rect", { x: 850, y: 375, width: 520, height: 350, rx: 36, class: "zone" }, back);
  text(back, 1110, 706, "caption", "Apache Iceberg tables");
  persona(back, 1835, 175, "Discover Data", "table catalog");
  persona(back, 1835, 452, "BI Developer", "dashboards");
  persona(back, 1835, 652, "Data Analyst", "SQL queries");
  persona(back, 1835, 842, "Data Scientist", "fraud model");

  const edges = $("edges");
  for (const [d, live, label, at] of EDGES) {
    el("path", { d, class: live === "meta" ? "edge meta" : "edge" }, edges);
    if (label) text(edges, at[0], at[1], "edge-label", label).setAttribute("text-anchor", at[2] || "middle");
  }

  const nodes = $("nodes");
  for (const node of catalog) {
    const [kind, ...geo] = SHAPES[node.key];
    const g = el("g", { class: `node ${kind}`, "data-key": node.key, tabindex: 0, role: "button", "aria-label": node.label }, nodes);
    const draw = { card: drawCard, cyl: drawCylinder, tier: drawTier }[kind];
    const dot = draw(g, ...geo);
    g.querySelector(".name, .caption, .tier-name").textContent = kind === "tier" ? node.label.toUpperCase() : node.label;
    if (node.container && dot) el("circle", { cx: dot[0], cy: dot[1], r: 8, class: "status stopped" }, g);
    g.addEventListener("click", () => openPanel(node.key));
    g.addEventListener("keydown", (e) => { if (e.key === "Enter") openPanel(node.key); });
  }
}

function rowsOf(s, name) {
  const t = s.tables.find((x) => x.name === name);
  return t && t.rows != null ? `${fmtInt(t.rows)} rows` : "–";
}

const SUB = {
  csv: (s) => `${fmtInt(s.producer.total)} rows`,
  producer: (s) => s.producer.finished ? "whole CSV sent" : `${fmtRate(s.producer.rate)}/s · ${s.producer.known ? Math.floor(s.producer.percent) : "–"} %`,
  kafka: () => "3 partitions",
  "spark-streaming": (s) => s.streaming.batch_ms == null ? "micro-batches" : `batch ${(s.streaming.batch_ms / 1000).toFixed(1)} s`,
  airflow: () => "schedules dbt",
  dbt: () => "SQL models",
  bronze: (s) => rowsOf(s, "bronze.transactions_raw"),
  silver: (s) => rowsOf(s, "silver.transactions"),
  gold: (s) => `${s.tables.filter((t) => t.name.startsWith("gold.")).length} tables`,
  "spark-thrift": () => "JDBC :10000",
  metabase: (s) => `${s.dashboards.length} dashboards`,
  beeline: () => "make beeline",
  jupyter: () => "PySpark",
  "spark-master": () => "scheduler",
  "spark-worker": () => "executors",
  minio: () => "S3 object store",
  "hive-metastore": (s) => `${s.tables.length} tables`,
  postgres: (s) => `${s.databases.length} databases`,
  cadvisor: () => "per container",
  prometheus: () => "scrape 15 s",
  grafana: () => "alerts by email",
};

function subText(node, s) {
  const st = node.container ? status(s, node.key) : "running";
  if (st === "stopped") return node.key === "producer" && s.services.producer ? "paused" : "stopped";
  if (st !== "running") return st;
  return SUB[node.key](s);
}


function render() {
  if (!state) return;
  for (const node of catalog) {
    const g = document.querySelector(`.node[data-key="${node.key}"]`);
    g.querySelector(".sub").textContent = subText(node, state);
    if (!node.container) continue;
    const st = status(state, node.key);
    const dot = g.querySelector(".status");
    if (dot) dot.setAttribute("class", `status ${st}`);
    g.classList.toggle("down", st === "stopped");
  }
  const paths = document.querySelectorAll("#edges path");
  EDGES.forEach(([, live], i) => {
    if (live !== "meta") paths[i].classList.toggle("live", Boolean(live(state)));
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
  $("producer-replay").hidden = !p.silver_complete;
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

function grid(head, rows) {
  const cells = (r, tag) => r.map((c, i) => `<${tag}${i ? ' class="num"' : ""}>${esc(c)}</${tag}>`).join("");
  return `<table class="mini"><thead><tr>${cells(head, "th")}</tr></thead>
    <tbody>${rows.map((r) => `<tr>${cells(r, "td")}</tr>`).join("")}</tbody></table>`;
}

function tableGrid(list) {
  const rows = list.map((t) => [t.name, t.rows == null ? "view" : fmtInt(t.rows), fmtBytes(t.bytes), fmtAgo(t.committed_ms)]);
  return grid(["Table", "Rows", "Size", "Written"], rows);
}

function extra(key, s) {
  if (key === "hive-metastore") return `<h3>Tables it knows</h3>${tableGrid(s.tables)}`;
  if (key === "postgres") {
    return `<h3>Databases</h3>${grid(["Database", "Size"], s.databases.map((d) => [d.name, fmtBytes(d.bytes)]))}
      <ul>${s.databases.map((d) => `<li><b>${esc(d.name)}</b>: ${esc(d.holds)}</li>`).join("")}</ul>`;
  }
  if (["bronze", "silver", "gold"].includes(key)) {
    const list = s.tables.filter((t) => t.name.startsWith(`${key}.`));
    return list.length ? `<h3>Tables</h3>${tableGrid(list)}` : "";
  }
  if (key === "metabase" && s.producer.silver_complete) {
    return `<p class="warn">Live is empty: Silver already holds all ${fmtInt(s.producer.total)} rows, so this replay
      adds nothing (MERGE skips every known trans_num). To show Live filling, press Reset then Start all before presenting.</p>`;
  }
  return "";
}

function openPanel(key, refresh = false) {
  const node = catalog.find((n) => n.key === key);
  if (key !== selected) panelHtml = null;
  selected = key;
  document.querySelectorAll(".node").forEach((g) => g.classList.toggle("selected", g.dataset.key === key));
  const svc = state && state.services[key];
  const stats = state && state.stats[key];
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
    <div class="layer">${esc(STAGE_TITLE[node.layer])}</div>
    <h2>${esc(node.label)}</h2>
    <p class="role">${esc(node.role)}</p>
    ${rows.length ? `<dl>${rows.map(([k, v]) => `<dt>${k}</dt><dd>${k === "Ports" ? v : esc(v)}</dd>`).join("")}</dl>` : ""}
    ${node.details.length ? `<ul>${node.details.map((d) => `<li>${esc(d)}</li>`).join("")}</ul>` : ""}
    ${state ? extra(key, state) : ""}
    ${node.shell.length ? `<h3>In a terminal</h3><pre class="shell">${node.shell.map(esc).join("\n")}</pre>` : ""}
`;
  const dashboards = key === "metabase" && state ? state.dashboards : [];
  const links = [...dashboards, ...node.links].map((l) => `<a href="${l.url}" target="_blank" rel="noopener">${esc(l.label)}<span>${esc(l.url.replace("http://", "").split("#")[0])} ↗</span></a>`).join("");
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
$("producer-replay").addEventListener("click", () => openPanel("metabase"));
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
