"use strict";

const SVG = "http://www.w3.org/2000/svg";
const H = 56;

// x, y, width of each card in the 1352 × 590 diagram; rows are centred on y 178, 268, 358.
const LAYOUT = {
  csv: [32, 150, 148], producer: [208, 150, 148], kafka: [384, 150, 148],
  "spark-streaming": [580, 150, 160], airflow: [580, 240, 160], dbt: [580, 330, 160],
  bronze: [788, 150, 160], silver: [788, 240, 160], gold: [788, 330, 160],
  "spark-thrift": [996, 240, 160],
  metabase: [1184, 150, 140], beeline: [1184, 240, 140], jupyter: [1184, 330, 140],
  "spark-master": [376, 462, 156], "spark-worker": [580, 462, 160], minio: [788, 462, 160],
  "hive-metastore": [996, 462, 160], postgres: [1184, 462, 140],
  cadvisor: [384, 582, 148], prometheus: [580, 582, 160], grafana: [788, 582, 160],
};

// x, width, number, title, caption
const STAGES = [
  [16, 532, 1, "Ingest", "the CSV replayed as live events"],
  [564, 192, 2, "Process", "stream, then batch"],
  [772, 192, 3, "Store", "Iceberg tables on MinIO"],
  [980, 356, 4, "Serve", "SQL, dashboards, notebooks"],
];

const STAGE_TITLE = {
  ingest: "1 · Ingest", process: "2 · Process", store: "3 · Store", serve: "4 · Serve",
  platform: "Platform", observability: "Observability",
};

const LOGO = {
  producer: "python", kafka: "kafka", "spark-streaming": "spark", airflow: "airflow", dbt: "dbt",
  "spark-thrift": "spark", metabase: "metabase", jupyter: "jupyter", "spark-master": "spark",
  "spark-worker": "spark", minio: "minio", "hive-metastore": "hive", postgres: "postgresql",
  prometheus: "prometheus", grafana: "grafana",
};

// Drawn for what has no logo of its own, on a 16 × 16 grid.
const TABLE_GLYPH = "M2 2.5h12v11H2z M2 6h12 M2 9.5h12 M6.5 6v7.5";
const GLYPH = {
  csv: "M3 1.5h6.5L13 5v9.5H3z M9.5 1.5V5H13 M5.5 8.5h5 M5.5 11h5",
  bronze: TABLE_GLYPH, silver: TABLE_GLYPH, gold: TABLE_GLYPH,
  beeline: "M1.5 2.5h13v11h-13z M4 6l2.5 2L4 10 M8 10.5h4",
  cadvisor: "M2 4.5l6-3 6 3v7l-6 3-6-3z M2 4.5l6 3 6-3 M8 7.5v7",
};

const TIER = { bronze: "var(--bronze)", silver: "var(--silver)", gold: "var(--gold)" };

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

// [path, kind, live(state), label(state), [x, y, anchor]]; kind: data, control or meta.
const EDGES = [
  ["M180 178H208", "data", producing],
  ["M356 178H384", "data", producing],
  ["M532 178H580", "data", streamingIn, (s) => `${fmtRate(s.streaming.input_per_second)}/s`, [556, 170]],
  ["M740 178H788", "data", streamingIn, () => "append", [764, 170]],
  ["M868 206V240", "data", streamingIn, () => "MERGE", [876, 227, "start"]],
  ["M868 296V330", "data", goldRefreshing, () => "staging view", [876, 317, "start"]],
  ["M660 296V330", "control", goldRefreshing, () => "every 10 min", [668, 317, "start"]],
  ["M740 358H788", "data", goldRefreshing, () => "builds", [764, 350]],
  ["M948 268H996", "data", (s) => running(s, "metabase"), () => "SQL", [972, 260]],
  ["M1156 268H1184", "data", (s) => running(s, "metabase")],
  ["M1170 268V178H1184", "data", (s) => running(s, "metabase"), () => "JDBC", [1164, 222, "end"]],
  ["M948 358H1184", "data", (s) => running(s, "jupyter"), () => "PySpark", [1066, 350]],
  ["M532 490H580", "meta"],
  ["M868 386V462", "meta", null, () => "Parquet and metadata files", [876, 430, "start"]],
  ["M1076 296V462", "meta", null, () => "table names", [1084, 430, "start"]],
  ["M1156 490H1184", "meta"],
  ["M532 610H580", "data", (s) => running(s, "cadvisor") && running(s, "prometheus")],
  ["M740 610H788", "data", (s) => running(s, "prometheus") && running(s, "grafana")],
  ["M948 610H996", "data", (s) => running(s, "grafana"), () => "email alerts", [1004, 614, "start"]],
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

function drawStatic() {
  const back = $("stages");
  for (const [x, w, n, title, caption] of STAGES) {
    el("rect", { x, y: 70, width: w, height: 340, rx: 8, class: "stage" }, back);
    el("circle", { cx: x + 24, cy: 96, r: 10, class: "stage-num" }, back);
    text(back, x + 24, 100, "stage-num-text", n);
    text(back, x + 42, 100, "stage-title", title);
    text(back, x + 42, 118, "stage-caption", caption);
  }
  for (const [y, title] of [[436, "Platform"], [556, "Observability"]]) {
    el("line", { x1: 16, x2: 1336, y1: y, y2: y, class: "band-rule" }, back);
    text(back, 16, y + 54, "band-title", title);
  }
  const edges = $("edges");
  for (const [d, kind, , , pos] of EDGES) {
    const path = el("path", { d, class: `edge ${kind}` }, edges);
    if (kind !== "meta") path.setAttribute("marker-end", "url(#head)");
    if (pos) text(edges, pos[0], pos[1], "edge-label", "").setAttribute("text-anchor", pos[2] || "middle");
  }
  const nodes = $("nodes");
  for (const node of catalog) {
    const [x, y, w] = LAYOUT[node.key];
    const g = el("g", { class: node.container ? "node" : "node data", "data-key": node.key,
                        tabindex: 0, role: "button", "aria-label": node.label }, nodes);
    el("rect", { x, y, width: w, height: H, rx: 6, class: "card" }, g);
    if (LOGO[node.key]) {
      el("image", { href: `icons/${LOGO[node.key]}.svg`, x: x + 11, y: y + 18, width: 20, height: 20 }, g);
    } else {
      const icon = el("g", { transform: `translate(${x + 12} ${y + 19}) scale(1.15)` }, g);
      const glyph = el("path", { d: GLYPH[node.key], class: "glyph" }, icon);
      if (TIER[node.key]) glyph.style.stroke = TIER[node.key];
    }
    text(g, x + 40, y + 25, "name", node.label);
    text(g, x + 40, y + 42, "sub", "");
    if (node.container) el("circle", { cx: x + w - 11, cy: y + 11, r: 3.5, class: "status stopped" }, g);
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
    g.querySelector(".status").setAttribute("class", `status ${st}`);
    g.classList.toggle("down", st === "stopped");
  }
  const paths = document.querySelectorAll("#edges path");
  const labels = document.querySelectorAll("#edges text");
  let li = 0;
  EDGES.forEach(([, kind, live, label, pos], i) => {
    const on = Boolean(live && live(state));
    if (kind !== "meta") {
      paths[i].setAttribute("class", `edge ${kind}${on ? " live" : ""}`);
      paths[i].setAttribute("marker-end", on ? "url(#head-live)" : "url(#head)");
    }
    if (pos) {
      labels[li].textContent = label(state);
      labels[li].classList.toggle("live", on);
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
