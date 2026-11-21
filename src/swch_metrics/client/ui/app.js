/* Swarmchestrate k3s Metrics Client UI — dependency-free (works on air-gapped edge clusters).
 * Talks to the sidecar REST API (/api/*). If the API is unreachable (e.g. on GitHub Pages)
 * or ?demo is in the URL, it switches to an in-browser simulated backend. */
"use strict";

const COLORS = ["#0455bf", "#13a89e", "#f29f05", "#2ba6de", "#162d73", "#d6453d", "#8e5bd8", "#5aa02c"];
const $ = (s, el = document) => el.querySelector(s);
const $$ = (s, el = document) => [...el.querySelectorAll(s)];
const esc = (v) => String(v ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const clone = (o) => JSON.parse(JSON.stringify(o));
const fmt = (v, d = 1) => v == null || isNaN(v) ? "–" : Math.abs(v) >= 1000 ? Number(v).toLocaleString(undefined, { maximumFractionDigits: 0 }) : Number(v).toFixed(d).replace(/\.0+$/, "");
const ago = (iso) => { if (!iso) return "never"; const s = (Date.now() - Date.parse(iso)) / 1000; return s < 60 ? `${Math.max(0, s | 0)}s ago` : s < 3600 ? `${(s / 60) | 0}m ago` : `${(s / 3600) | 0}h ago`; };
const dur = (s) => s >= 3600 && s % 3600 === 0 ? `${s / 3600}h` : s >= 60 && s % 60 === 0 ? `${s / 60}m` : `${s}s`;

const S = { demo: false, status: null, config: null, ctypes: [], stypes: [], metric: null, hidden: new Set(), topology: null, topoGw: "" };
const multi = () => (S.status?.gateways || []).length > 1;

/* ------------------------------------------------------------------ API */
const api = {
  async call(method, path, body) {
    if (S.demo) return Demo.handle(method, path, body);
    const r = await fetch(path, { method, headers: body ? { "Content-Type": "application/json" } : {}, body: body ? JSON.stringify(body) : undefined });
    const ct = r.headers.get("content-type") || "";
    const data = ct.includes("json") ? await r.json() : await r.text();
    if (!r.ok) { const e = new Error(typeof data === "string" ? data : JSON.stringify(data.detail ?? data)); e.detail = data.detail; throw e; }
    return data;
  },
  get: (p) => api.call("GET", p), put: (p, b) => api.call("PUT", p, b), post: (p, b) => api.call("POST", p, b || {}),
};

/* ------------------------------------------------------------------ demo backend */
const Demo = {
  data: null, t0: Date.now(), series: new Map(), recent: [], states: {}, sinks: {}, total: 0,
  async init() {
    this.data = await (await fetch("ui/demo-data.json")).json();
    this.data.topology.updated_at = new Date(Date.now() - 40000).toISOString();
    this.config = clone(this.data.config);
    this.reset();
    for (let i = 60; i > 0; i--) this.tick(Date.now() - i * 5000, true);
    setInterval(() => this.tick(Date.now()), 5000);
  },
  clusters() { return (this.config.gateways || []).filter((g) => g.enabled !== false).map((g) => ({ ...g, ...(this.data.clusters.find((c) => c.id === g.id) || {}) })); },
  reset() {
    this.states = {};
    for (const c of this.config.collectors) this.states[c.id] = { id: c.id, type: c.type, enabled: c.enabled !== false, interval_s: this.secs(c.interval || this.config.default_interval), runs: 0, failures: 0, last_run: null, last_duration_ms: null, last_count: 0, last_error: null, sink_errors: {}, status: "pending" };
    for (const s of this.config.sinks) this.sinks[s.id] = this.sinks[s.id] || { sent: 0, errors: 0 };
  },
  env(v) { return typeof v === "string" ? v.replace(/\$\{[A-Za-z_][A-Za-z0-9_]*(?::-([^}]*))?\}/g, (_, d) => d || "") : v; },
  secs(v) { if (typeof v === "number") return v; const m = /^(\d+(?:\.\d+)?)\s*(ms|s|sec|m|min|h|d|w)?$/i.exec(String(v).trim()); if (!m) return 30; return +m[1] * ({ ms: .001, s: 1, sec: 1, m: 60, min: 60, h: 3600, d: 86400, w: 604800 }[(m[2] || "s").toLowerCase()]); },
  wave(seed, t, base, amp) { return Math.max(0, base + amp * Math.sin(t / 60000 + seed) + amp * 0.35 * Math.sin(t / 9000 + seed * 3) + (Math.random() - .5) * amp * .25); },
  gen(type, t) {
    const out = [];
    this.clusters().forEach((cl, k) => {
      const lbl = { cluster: cl.cluster, gateway: cl.id, swarm_id: cl.swarm_id || "swarm-demo-01" };
      for (const s of this.gen1(type, t, cl, k)) out.push({ ...s, labels: { ...lbl, ...s.labels } });
    });
    return out;
  },
  gen1(type, t, cl, k) {
    const pre = this.config.metric_prefix ?? "swch_k3s_", nodes = cl.nodes_list || this.data.topology.nodes, out = [];
    const pods = (cl.pods || [["default", "optimusdb-0"], ["default", "optimusdb-1"], ["default", "web-7d9c"], ["swarm-system", "emsserver-ems-server-6f8"], ["default", "resource-agent-5b2"], ["default", "lib-optimiser-77c"]]);
    const add = (m, v, inst, labels) => out.push({ metric: pre + m, value: +Math.max(0, v + k * 3).toFixed(2), instance: inst, labels, timestamp_ms: t });
    if (type === "node_usage") nodes.forEach((n, i) => { const c = this.wave(i, t, 35 + i * 12, 14); add("node_cpu_pct", c, n.name, { node: n.name }); add("node_memory_pct", this.wave(i + 9, t, 48 + i * 8, 6), n.name, { node: n.name }); add("node_cpu_millicores", c * 40, n.name, { node: n.name }); });
    if (type === "pod_usage") pods.forEach(([ns, p], i) => { add("pod_cpu_millicores", this.wave(i * 2, t, 80 + i * 45, 50), p, { namespace: ns, pod: p }); add("pod_memory_mib", this.wave(i * 5, t, 120 + i * 60, 12), p, { namespace: ns, pod: p }); });
    if (type === "pod_overview") pods.forEach(([ns, p], i) => { add("pod_cpu_pct_of_limit", this.wave(i, t, 30 + i * 9, 18), p, { namespace: ns, pod: p }); add("pod_memory_pct_of_limit", this.wave(i + 4, t, 40 + i * 6, 5), p, { namespace: ns, pod: p }); add("pod_restarts", i === 2 ? 3 : 0, p, { namespace: ns, pod: p }); });
    if (type === "topology") { add("cluster_nodes_total", nodes.length, "cluster", {}); add("cluster_nodes_ready", nodes.filter((n) => n.ready).length, "cluster", {}); }
    if (type === "events" && Math.random() < .15) add("k8s_event", 1, "web-7d9c", { namespace: "default", reason: "BackOff", kind: "Pod", object: "web-7d9c" });
    if (type === "apiserver_health") { add("apiserver_livez", 1, "apiserver", {}); add("apiserver_readyz", 1, "apiserver", {}); }
    if (type === "pod_stats") pods.forEach(([ns, p], i) => add("pod_network_rx_bytes_per_sec", this.wave(i * 7, t, 2000 + i * 900, 1500), p, { namespace: ns, pod: p }));
    if (type === "cadvisor") pods.forEach(([ns, p], i) => add("container_cpu_cfs_throttled_periods_per_sec", this.wave(i * 3, t, i === 3 ? 4 : .4, .5), p, { namespace: ns, pod: p }));
    return out;
  },
  tick(t, warm) {
    for (const c of this.config.collectors) {
      const st = this.states[c.id]; if (!st || !st.enabled) continue;
      if (!warm && st.last_run && t - Date.parse(st.last_run) < st.interval_s * 1000 - 100) continue;
      this.run(c, t);
    }
  },
  run(c, t = Date.now()) {
    const st = this.states[c.id], samples = this.gen(c.type, t).map((s) => ({ ...s, collector: c.id, source: (this.data.types.find((x) => x.type === c.type) || {}).endpoint, labels: { ...Object.fromEntries(Object.entries(this.data.labels).filter(([, v]) => v)), ...s.labels } }));
    st.runs++; st.last_run = new Date(t).toISOString(); st.last_count = samples.length; st.last_duration_ms = +(20 + Math.random() * 60).toFixed(1); st.status = "ok";
    for (const s of samples) {
      const k = s.metric + "|" + (this.clusters().length > 1 ? s.labels.cluster + "/" : "") + s.instance; if (!this.series.has(k)) this.series.set(k, []);
      const arr = this.series.get(k); arr.push([s.timestamp_ms, s.value]); if (arr.length > 360) arr.shift();
      this.recent.push(s);
    }
    if (this.recent.length > 2000) this.recent.splice(0, this.recent.length - 2000);
    this.total += samples.length;
    for (const sk of this.config.sinks) if (sk.enabled !== false && (!c.sinks || c.sinks.includes(sk.id))) this.sinks[sk.id].sent += samples.length;
    return samples;
  },
  yaml(o, ind = "") {
    const sc = (v) => typeof v === "string" ? (v === "" || /[:#{}\[\],&*?|<>=!%@`'"]|^\s|\s$|^(true|false|null|\d+)$/i.test(v) ? JSON.stringify(v) : v) : v === null ? "null" : String(v);
    if (Array.isArray(o)) return o.length ? o.map((v) => typeof v === "object" && v !== null ? `${ind}- ${this.yaml(v, ind + "  ").trimStart()}` : `${ind}- ${sc(v)}`).join("\n") : `${ind}[]`;
    return Object.entries(o).map(([k, v]) => v !== null && typeof v === "object" ? (Object.keys(v).length || Array.isArray(v) && v.length ? `${ind}${k}:\n${this.yaml(v, ind + "  ")}` : `${ind}${k}: ${Array.isArray(v) ? "[]" : "{}"}`) : `${ind}${k}: ${sc(v)}`).join("\n");
  },
  async handle(method, path, body) {
    const u = new URL(path, location.href), p = u.pathname.replace(/^.*?\/api\//, "/api/");
    await new Promise((r) => setTimeout(r, 60));
    if (p === "/api/status") {
      const now = Date.now();
      const cls = this.clusters(), gws = (this.config.gateways || []).map((g) => { const c = this.data.clusters.find((x) => x.id === g.id) || {}; const on = g.enabled !== false; return { id: g.id, url: this.env(g.url), enabled: on, source: g.source || "config", cluster: g.cluster || c.cluster || g.id, swarm_id: "swarm-demo-01", cluster_uid: c.cluster_uid, ok: on ? true : null, last_ok: on ? new Date(now - 4000).toISOString() : null, error: null, registered_at: g.registered_at || null, last_heartbeat: g.source === "registered" ? new Date(now - (c.hb_ago || 90) * 1000).toISOString() : null, stale: false, summary: c.summary, kubernetes_version: "v1.33.4+k3s1", collectors: on ? this.config.collectors.length : 0, samples_last_run: on ? 40 + 7 * (c.summary?.nodes || 1) : 0, errors: 0, info: g.info || {} }; });
      return { client: this.config.client_name, version: "demo", config_source: "demo (in-browser)", uptime_s: ((now - this.t0) / 1000 + 300) | 0,
        gateway: { url: gws[0]?.url || null, ok: true, count: gws.length, up: cls.length }, gateways: gws,
        clusters: { count: gws.length, nodes: cls.reduce((a, c) => a + (c.summary?.nodes || 0), 0), ready: cls.reduce((a, c) => a + (c.summary?.ready || 0), 0) },
        registration: { enabled: true, token_configured: true, auto_approve: true, stale_after: "15m", path: "api/register" }, total_samples: this.total,
        samples_last_minute: this.recent.filter((s) => s.timestamp_ms > now - 60000).length,
        collectors: Object.values(this.states).map((s) => ({ ...s, status: s.enabled ? s.status : "disabled" })),
        sinks: this.config.sinks.map((s) => ({ id: s.id, type: s.type, enabled: s.enabled !== false, sent: this.sinks[s.id].sent, errors: 0, last_error: null, connected: s.type === "ems_stomp" ? s.enabled !== false : null })),
        series: this.series.size, topology: { cluster_id: this.data.topology.cluster_id, swarm_id: this.data.topology.swarm_id, summary: this.data.topology.summary, hash: this.data.topology.hash } };
    }
    if (p === "/api/config" && method === "GET") return { config: this.config, yaml: this.yaml(this.config) + "\n", source: "demo" };
    if (p === "/api/config" && method === "PUT") {
      if (body.yaml) throw new Error("YAML editing is not available in the demo — use the forms.");
      this.config = clone(body.config); this.reset();
      return { saved: true, persisted: { file: "(demo) /data/client.yaml", gateway: "(demo) ConfigMap swch-metrics-client-config-saved" } };
    }
    if (p === "/api/config/validate") return { valid: true };
    if (p === "/api/collector-types") return this.data.types;
    if (p === "/api/sink-types") return this.data.sink_types;
    if (p === "/api/topology") { const g = u.searchParams.get("gateway") || this.clusters()[0]?.id, c = this.clusters().find((x) => x.id === g) || {}; return { ...this.data.topology, cluster_name: c.cluster, cluster_id: c.cluster_uid, summary: c.summary || this.data.topology.summary, nodes: c.nodes_list || this.data.topology.nodes, gateway_id: g }; }
    if (p === "/api/gateways" && method === "GET") return (await this.handle("GET", "api/status")).gateways;
    const gtest = /^\/api\/gateways\/([^/]+)\/test$/.exec(p);
    if (gtest) { const c = this.data.clusters.find((x) => x.id === gtest[1]) || {}; return { reachable: true, authorized: true, cluster_name: c.cluster, cluster_uid: c.cluster_uid, nodes: c.summary?.nodes }; }
    const gen = /^\/api\/gateways\/([^/]+)\/enabled$/.exec(p);
    if (gen) { const g = this.config.gateways.find((x) => x.id === gen[1]); g.enabled = !!body.enabled; this.reset(); return { id: g.id, enabled: g.enabled }; }
    const gdel = /^\/api\/gateways\/([^/]+)$/.exec(p);
    if (gdel && method === "DELETE") { this.config.gateways = this.config.gateways.filter((x) => x.id !== gdel[1]); this.reset(); return { deleted: gdel[1] }; }
    if (p === "/api/metrics") { const idx = {}; for (const k of this.series.keys()) { const [m, i] = k.split("|"); (idx[m] = idx[m] || []).push(i); } return Object.keys(idx).sort().map((m) => ({ metric: m, instances: idx[m] })); }
    if (p === "/api/series") { const m = u.searchParams.get("metric"); return [...this.series.entries()].filter(([k]) => k.startsWith(m + "|")).map(([k, pts]) => ({ instance: k.split("|")[1], points: pts })).sort((a, b) => b.points.at(-1)[1] - a.points.at(-1)[1]).slice(0, 6); }
    if (p === "/api/samples") { const c = u.searchParams.get("collector"), cl = u.searchParams.get("cluster"), q = (u.searchParams.get("q") || "").toLowerCase(); return this.recent.slice().reverse().filter((s) => (!c || s.collector === c) && (!cl || s.labels.cluster === cl) && (!q || JSON.stringify(s).toLowerCase().includes(q))).slice(0, 200); }
    const run = /^\/api\/collectors\/([^/?]+)\/run$/.exec(p);
    if (run) { const c = this.config.collectors.find((x) => x.id === run[1]); const s = this.run(c); return { collector: c.id, count: s.length, samples: s }; }
    throw new Error("demo: unsupported " + p);
  },
};

/* ------------------------------------------------------------------ helpers */
function toast(msg, err = false) { const t = $("#toast"); t.textContent = msg; t.className = "toast" + (err ? " err" : ""); t.hidden = false; clearTimeout(t._h); t._h = setTimeout(() => (t.hidden = true), 4500); }
function errText(e) { return Array.isArray(e.detail) ? e.detail.map((d) => d.loc ? `${d.loc}: ${d.msg}` : JSON.stringify(d)).join("\n") : e.message; }
async function saveConfig(cfg, label = "Configuration") {
  const r = await api.put("api/config", { config: cfg });
  S.config = cfg;
  const p = r.persisted || {};
  toast(`${label} saved & applied.` + (p.gateway && String(p.gateway).startsWith("error") ? ` (ConfigMap: ${p.gateway})` : ""));
  await refresh(); renderConfigViews();
}

/* ------------------------------------------------------------------ rendering */
function renderHeader(st) {
  const gw = $("#pill-gw"), ems = $("#pill-ems");
  const gws = st.gateways || [], on = gws.filter((g) => g.enabled), up = on.filter((g) => g.ok).length;
  gw.className = "pill " + (!on.length ? "" : up === on.length ? "ok" : up ? "warn" : "err");
  gw.title = gws.map((g) => `${g.cluster}: ${g.enabled ? (g.ok ? "up" : g.error || "down") : "disabled"}`).join("\n") || "no gateway registered yet";
  $("#pill-gw-text").textContent = gws.length > 1 ? `Gateways ${up}/${on.length}` : "Gateway";
  const e = st.sinks.find((s) => s.type === "ems_stomp" && s.enabled);
  ems.className = "pill " + (!e ? "" : e.connected ? "ok" : e.connected === false || e.last_error ? "err" : ""); ems.title = e ? (e.last_error || "connected") : "no EMS sink enabled";
  const t = st.topology || {}, sm = t.summary || {}, cl = st.clusters || {};
  if (gws.length > 1) {
    const swarms = [...new Set(gws.map((g) => g.swarm_id).filter(Boolean))];
    $("#hero-title").textContent = `${gws.length} k3s clusters`;
    $("#hero-eyebrow").textContent = swarms.length ? `Swarm ${swarms.join(", ")} · EMS sidecar` : "EMS sidecar";
    $("#hero-meta").innerHTML = gws.map((g) => `<span><i class="dot" style="background:${g.ok ? "#13a89e" : g.enabled ? "#d6453d" : "#aab3c2"}"></i> <b>${esc(g.cluster)}</b> ${g.summary ? `${g.summary.ready}/${g.summary.nodes} nodes` : ""}</span>`).join("");
  } else {
    $("#hero-title").textContent = t.cluster_name || t.swarm_id || t.cluster_id?.slice(0, 8) || (gws.length ? st.client : "Waiting for a gateway");
    $("#hero-eyebrow").textContent = t.swarm_id ? `Swarm ${t.swarm_id} · k3s cluster` : "k3s cluster";
    $("#hero-meta").innerHTML = [["Cluster ID", t.cluster_id ? t.cluster_id.slice(0, 13) + "…" : "–"], ["Zones", (sm.zones || []).join(", ") || "–"], ["Clouds", (sm.clouds || []).join(", ") || "–"], ["Gateway", st.gateway.url || "none yet: see Gateways"]].map(([k, v]) => `<span>${k}: <b>${esc(v)}</b></span>`).join("");
  }
  $("#hero-stats").innerHTML = [[`${cl.ready ?? sm.ready ?? "–"}/${cl.nodes ?? sm.nodes ?? "–"}`, "Nodes ready"], [gws.length, gws.length === 1 ? "Cluster" : "Clusters"], [st.collectors.filter((c) => c.enabled).length, "Collectors"], [fmt(st.samples_last_minute, 0), "Samples / min"]].map(([v, l]) => `<div class="hero-stat"><div class="v">${v}</div><div class="l">${l}</div></div>`).join("");
  $("#version").textContent = `v${st.version} · ${st.client} · up ${dur(Math.round(st.uptime_s / 60) * 60 || 60)}`;
}

function renderKpis(st) {
  const ems = st.sinks.filter((s) => s.type === "ems_stomp").reduce((a, s) => a + s.sent, 0);
  const errs = st.collectors.filter((c) => c.last_error).length + st.sinks.filter((s) => s.last_error).length;
  const gws = st.gateways || [], up = gws.filter((g) => g.ok).length;
  const k = [["Clusters", `${up}<small>/${gws.filter((g) => g.enabled).length}</small>`, "teal", `${st.clusters?.ready ?? "–"}/${st.clusters?.nodes ?? "–"} nodes ready`],
    ["Active collectors", st.collectors.filter((c) => c.enabled).length, "", `${st.collectors.length} configured`],
    ["Samples / minute", fmt(st.samples_last_minute, 0), "navy", `${fmt(st.total_samples, 0)} total`],
    ["Sent to EMS", fmt(ems, 0), "amber", "STOMP messages"],
    ["Errors", errs, errs ? "red" : "teal", errs ? "see collectors & sinks" : "all healthy"]];
  $("#kpis").innerHTML = k.map(([l, v, c, s]) => `<div class="kpi ${c}"><div class="l">${l}</div><div class="v">${v}</div><div class="s">${s}</div></div>`).join("");
}

const badge = (s) => `<span class="badge ${s}">${s}</span>`;

function renderDashTables(st) {
  $("#dash-collectors").innerHTML = `<tr><th>Collector</th><th>Interval</th><th>Last run</th><th class="num">Samples</th><th>Status</th></tr>` +
    st.collectors.map((c) => `<tr><td><b>${esc(c.id)}</b> <span class="badge type">${esc(c.type)}</span>${(c.per_gateway || []).length > 1 ? ` <span class="muted">× ${c.per_gateway.length} clusters</span>` : ""}</td><td>${dur(c.interval_s)}</td><td>${ago(c.last_run)}</td><td class="num">${c.last_count}</td><td>${badge(c.status)}${c.last_error ? `<div class="err-text">${esc(c.last_error)}</div>` : ""}</td></tr>`).join("");
  $("#dash-sinks").innerHTML = st.sinks.map((s) => `<div class="sink"><div><div class="n">${esc(s.id)} <span class="badge type">${esc(s.type)}</span></div><div class="m">${s.enabled ? (s.last_error ? `<span class="err-text">${esc(s.last_error)}</span>` : s.connected === false ? "disconnected" : "healthy") : "disabled"}</div></div><div><div class="c">${fmt(s.sent, 0)}</div><div class="m">sent · ${s.errors} errors</div></div></div>`).join("") || `<div class="empty">No sinks configured</div>`;
}

function renderCollectorsTable() {
  const st = Object.fromEntries((S.status?.collectors || []).map((c) => [c.id, c]));
  const rows = S.config.collectors.map((c, i) => { const s = st[c.id] || {}; return `<tr>
    <td><label class="switch"><input type="checkbox" data-toggle="${i}" ${c.enabled !== false ? "checked" : ""}><span></span></label></td>
    <td><b>${esc(c.id)}</b><div class="muted">${esc((S.ctypes.find((t) => t.type === c.type) || {}).title || "")}</div></td>
    <td><span class="badge type">${esc(c.type)}</span></td><td>${esc(c.interval || S.config.default_interval)}</td>
    <td>${esc((c.sinks || ["all"]).join(", "))}<div class="muted">gateways: ${esc((c.gateways || ["all"]).join(", "))}</div></td><td>${ago(s.last_run)} · ${s.last_count ?? 0}</td>
    <td>${badge(s.status || "pending")}${s.last_error ? `<div class="err-text">${esc(s.last_error)}</div>` : ""}</td>
    <td class="actions-cell"><button class="btn sm" data-run="${esc(c.id)}">Run now</button> <button class="btn sm" data-edit="${i}">Edit</button> <button class="btn sm danger" data-del="${i}">Delete</button></td></tr>`; }).join("");
  $("#collectors-table").innerHTML = `<tr><th></th><th>ID</th><th>Type</th><th>Interval</th><th>Sinks</th><th>Last run</th><th>Status</th><th></th></tr>` + (rows || `<tr><td colspan="8" class="empty">No collectors yet.</td></tr>`);
  $("#collector-types").innerHTML = S.ctypes.map((t) => `<div class="type-card"><h4>${esc(t.title)}</h4><span class="badge type">${esc(t.type)}</span> <span class="ep">${esc(t.endpoint)}</span><p>${esc(t.description)}</p><button class="btn sm" data-addtype="${esc(t.type)}">Add</button></div>`).join("");
}

function renderSinksTable() {
  const st = Object.fromEntries((S.status?.sinks || []).map((s) => [s.id, s]));
  $("#sinks-table").innerHTML = `<tr><th></th><th>ID</th><th>Type</th><th>Target</th><th class="num">Sent</th><th>Status</th><th></th></tr>` + S.config.sinks.map((s, i) => {
    const o = s.options || {}, x = st[s.id] || {}, target = s.type === "ems_stomp" ? `${o.host}:${o.port} ${o.destination || ""}` : s.type === "optimusdb_trust" ? `${o.url || ""}/${o.context || "swarmkb"}/command → ${o.dstype || "kbtrust"}` : o.url || o.path || "container log";
    return `<tr><td><label class="switch"><input type="checkbox" data-stoggle="${i}" ${s.enabled !== false ? "checked" : ""}><span></span></label></td><td><b>${esc(s.id)}</b></td><td><span class="badge type">${esc(s.type)}</span></td><td><code>${esc(target)}</code></td><td class="num">${fmt(x.sent ?? 0, 0)}</td><td>${x.last_error ? `<span class="err-text">${esc(x.last_error)}</span>` : s.enabled === false ? badge("disabled") : badge("ok")}</td><td class="actions-cell"><button class="btn sm" data-sedit="${i}">Edit</button> <button class="btn sm danger" data-sdel="${i}">Delete</button></td></tr>`;
  }).join("");
}

function renderGateways() {
  const st = S.status || {}, gws = st.gateways || [], reg = st.registration || {};
  const state = (g) => !g.enabled ? (g.source === "registered" && !g.ok ? "pending-approval" : "disabled") : g.stale ? "stale" : g.ok ? "ok" : g.ok === false ? "error" : "pending";
  $("#gateways-table").innerHTML = `<tr><th></th><th>Cluster</th><th>Gateway URL</th><th>Source</th><th>Nodes</th><th>Last heartbeat</th><th class="num">Samples</th><th>Status</th><th></th></tr>` + (gws.map((g) => `<tr>
    <td><label class="switch"><input type="checkbox" data-gtoggle="${esc(g.id)}" ${g.enabled ? "checked" : ""}><span></span></label></td>
    <td><b>${esc(g.cluster)}</b><div class="muted">id ${esc(g.id)}${g.swarm_id ? " · " + esc(g.swarm_id) : ""}</div></td>
    <td><span class="gw-url">${esc(g.url)}</span>${g.cluster_uid ? `<div class="muted">uid ${esc(String(g.cluster_uid).slice(0, 13))}…</div>` : ""}</td>
    <td>${badge(g.source)}${g.registered_at ? `<div class="muted">${ago(g.registered_at)}</div>` : ""}</td>
    <td>${g.summary ? `${g.summary.ready}/${g.summary.nodes} ready` : (g.info?.nodes ? `${g.info.nodes_ready ?? "?"}/${g.info.nodes}` : "–")}<div class="muted">${esc(g.kubernetes_version || g.info?.kubernetes_version || "")}</div></td>
    <td>${g.source === "registered" ? ago(g.last_heartbeat) : "–"}</td>
    <td class="num">${fmt(g.samples_last_run, 0)}</td>
    <td>${badge(state(g))}${g.error ? `<div class="err-text">${esc(g.error)}</div>` : ""}${g.errors ? `<div class="err-text">${g.errors} collector error(s)</div>` : ""}</td>
    <td class="actions-cell"><button class="btn sm" data-gtest="${esc(g.id)}">Test</button> <button class="btn sm" data-gedit="${esc(g.id)}">Edit</button> <button class="btn sm danger" data-gdel="${esc(g.id)}">Delete</button></td></tr>`).join("")
    || `<tr><td colspan="9" class="empty">No gateway yet. Gateways on new k3s hosts register themselves here (see below), or add one manually.</td></tr>`);
  const url = new URL(reg.path || "api/register", location.href).href;
  $("#reg-badge").innerHTML = badge(reg.enabled && reg.token_configured ? "ok" : "disabled");
  $("#reg-info").innerHTML = [["Endpoint", `<code>POST ${esc(url)}</code>`], ["Token", reg.token_configured ? "configured (REGISTRATION_TOKEN)" : "<span class='err-text'>not set: registration refused</span>"],
    ["New gateways", reg.auto_approve ? "enabled immediately" : "added disabled, waiting for approval"], ["Stale after", esc(reg.stale_after || "15m") + " without heartbeat"]].map(([k, v]) => `<dt>${k}</dt><dd>${v}</dd>`).join("");
  $("#onboard-cmd").textContent = `curl -fsSL https://raw.githubusercontent.com/georgeGeorgakakos/k3s-metrics-gateway/v${(st.version || "0.3.0").replace("demo", "0.3.0")}/deploy/onboard/onboard-k3s-host.sh -o onboard-k3s-host.sh\nsudo REGISTER_URL=${url} \\\n     REGISTER_TOKEN=<token> \\\n     CLUSTER_NAME=<unique-name> SWARM_ID=<swarm> \\\n     bash onboard-k3s-host.sh`;
}

function gatewayForm(existing) {
  const g = existing ? clone(existing) : { id: "", url: "http://<host>/swch-metrics", enabled: true };
  openModal(existing ? `Edit gateway · ${existing.id}` : "Add gateway", [
    field("id", { type: "string", required: true, description: "lowercase, digits, - or _" }, g.id, "ID"),
    field("url", { type: "string", required: true, description: "As reachable from this EMS host, e.g. http://10.0.0.7/swch-metrics" }, g.url, "Gateway URL", true),
    field("api_key", { type: "secret", description: "X-API-Key of that gateway (${ENV} allowed)" }, g.api_key, "API key"),
    field("cluster", { type: "string", description: "Name used in EMS headers and OptimusDB (empty = from the gateway's topology)" }, g.cluster, "Cluster name"),
    field("swarm_id", { type: "string" }, g.swarm_id, "Swarm ID"),
    field("enabled", { type: "boolean" }, g.enabled !== false, "Enabled"),
    field("labels", { type: "json", description: "Extra labels for this cluster's samples, e.g. {\"site\": \"lab\"}" }, g.labels || {}, "Labels", true),
  ].join(""), async (root) => {
    const out = { ...g, id: readField(root, "id", { type: "string" }), url: readField(root, "url", { type: "string" }), enabled: readField(root, "enabled", { type: "boolean" }), labels: readField(root, "labels", { type: "json" }) };
    for (const k of ["api_key", "cluster", "swarm_id"]) { const v = readField(root, k, { type: "string" }); if (v) out[k] = v; else delete out[k]; }
    const cfg = clone(S.config); cfg.gateways = cfg.gateways || [];
    const idx = existing ? cfg.gateways.findIndex((x) => x.id === existing.id) : -1;
    if (!existing && cfg.gateways.some((x) => x.id === out.id)) throw new Error(`gateway id '${out.id}' already exists`);
    if (idx >= 0) cfg.gateways[idx] = out; else cfg.gateways.push(out);
    await saveConfig(cfg, `Gateway ${out.id}`);
  });
}

function renderTopology(t) {
  if (!t) { $("#topo-diagram").innerHTML = `<div class="empty">Topology not available yet.</div>`; return; }
  $("#topo-hash").textContent = `${t.cluster_name ? t.cluster_name + " · " : ""}hash ${t.hash || "–"} · updated ${ago(t.updated_at)}`;
  const zones = {}; for (const n of t.nodes || []) (zones[n.zone || n.region || "default zone"] ||= []).push(n);
  const pct = (a, b) => (b ? Math.min(100, (100 * a) / b) : 0);
  $("#topo-diagram").innerHTML = Object.entries(zones).map(([z, ns]) => `<div class="zone"><div class="zt">${esc(z)}${ns[0].cloud ? " · " + esc(ns[0].cloud) : ""}</div>` + ns.map((n) => {
    const cpu = parseFloat(n.allocatable?.cpu) || 0, mem = (parseInt(n.allocatable?.memory) || 0) / 1048576;
    return `<div class="node ${n.ready ? "" : "down"}"><div class="nn"><span>${esc(n.name)}</span><span class="badge ${n.ready ? "ok" : "error"}">${n.ready ? "Ready" : "NotReady"}</span></div>
      <div class="nm">${esc((n.roles || []).join(", "))} · ${esc(n.internal_ip || "")} · ${esc(n.arch || "")}</div>
      <div class="nm">${cpu} vCPU · ${fmt(mem, 1)} GiB allocatable</div><div class="bar"><i style="width:${pct(cpu, 16)}%"></i></div></div>`;
  }).join("") + `</div>`).join("");
  $("#nodes-table").innerHTML = `<tr><th>Node</th><th>Roles</th><th>Zone / Region</th><th>Cloud</th><th>Internal IP</th><th>Kubelet</th><th class="num">CPU</th><th class="num">Memory</th><th>Ready</th></tr>` +
    (t.nodes || []).map((n) => `<tr><td><b>${esc(n.name)}</b></td><td>${esc((n.roles || []).join(", "))}</td><td>${esc(n.zone || "–")} / ${esc(n.region || "–")}</td><td>${esc(n.cloud || "–")}</td><td><code>${esc(n.internal_ip || "")}</code></td><td>${esc(n.kubelet_version || "")}</td><td class="num">${esc(n.allocatable?.cpu || "")}</td><td class="num">${esc(n.allocatable?.memory || "")}</td><td>${badge(n.ready ? "ok" : "error")}</td></tr>`).join("");
  $("#endpoints-table").innerHTML = `<tr><th>ID</th><th>Path</th><th>Source</th><th>Description</th><th>Available</th></tr>` +
    (t.endpoints || []).map((e) => `<tr><td><b>${esc(e.id)}</b></td><td><code>${esc(e.path)}</code></td><td>${esc(e.source)}</td><td>${esc(e.description)}</td><td>${e.available == null ? "–" : badge(e.available ? "ok" : "disabled")}</td></tr>`).join("");
}

async function renderSamples() {
  const c = $("#sample-collector").value, cl = $("#sample-cluster").value, q = $("#sample-filter").value;
  const rows = await api.get(`api/samples?limit=200${c ? "&collector=" + encodeURIComponent(c) : ""}${cl ? "&cluster=" + encodeURIComponent(cl) : ""}${q ? "&q=" + encodeURIComponent(q) : ""}`);
  $("#samples-table").innerHTML = `<tr><th>Time</th><th>Cluster</th><th>Metric</th><th>Instance</th><th class="num">Value</th><th>Labels</th><th>Collector</th></tr>` +
    (rows.map((s) => `<tr><td>${new Date(s.timestamp_ms).toLocaleTimeString()}</td><td><span class="cluster-tag">${esc(s.labels?.cluster || "–")}</span></td><td>${esc(s.metric)}</td><td>${esc(s.instance || "")}</td><td class="num">${fmt(s.value, 2)}</td><td>${esc(Object.entries(s.labels || {}).filter(([k]) => !["swarm_id", "cloud", "region", "zone", "cluster", "cluster_uid", "gateway"].includes(k)).map(([k, v]) => `${k}=${v}`).join(" "))}</td><td>${esc(s.collector)}</td></tr>`).join("") || `<tr><td colspan="7" class="empty">No samples yet.</td></tr>`);
}

function renderConfigViews() {
  renderCollectorsTable(); renderSinksTable();
  const c = S.config;
  $("#general-form").innerHTML = [
    field("client_name", { type: "string", description: "Shown in EMS headers as 'producer'" }, c.client_name, "Client name"),
    field("default_interval", { type: "string", description: "Used when a collector has no interval (e.g. 30s, 1m)" }, c.default_interval, "Default interval"),
    field("metric_prefix", { type: "string", description: "Prepended to every metric/topic name" }, c.metric_prefix, "Metric prefix"),
    field("labels", { type: "json", description: "Static labels added to every sample (swarm_id, cloud, region, zone…). ${ENV} placeholders allowed." }, c.labels || {}, "Static labels", true),
  ].join("");
  $("#persist-info").innerHTML = [["Mode", c.persist?.mode || "gateway"], ["File", c.persist?.path || "/data/client.yaml"], ["Loaded from", S.status?.config_source || "–"]].map(([k, v]) => `<dt>${k}</dt><dd>${esc(v)}</dd>`).join("");
  api.get("api/config").then((r) => { $("#yaml-editor").value = r.yaml; });
}

/* ------------------------------------------------------------------ forms */
function field(name, spec, value, label, full) {
  const id = "f_" + name, t = spec.type, v = value ?? spec.default;
  let input;
  if (spec.enum) input = `<select id="${id}" name="${name}">${spec.enum.map((o) => `<option value="${esc(o)}" ${o === (v ?? "") ? "selected" : ""}>${esc(o || "(any)")}</option>`).join("")}</select>`;
  else if (t === "boolean") input = `<label class="switch"><input type="checkbox" id="${id}" name="${name}" ${v ? "checked" : ""}><span></span></label>`;
  else if (t === "integer") input = `<input type="number" id="${id}" name="${name}" value="${esc(v ?? "")}">`;
  else if (t === "number") input = `<input type="number" step="any" id="${id}" name="${name}" value="${esc(v ?? "")}">`;
  else if (t === "json") input = `<textarea id="${id}" name="${name}" rows="3">${esc(typeof v === "string" ? v : JSON.stringify(v ?? {}, null, 0))}</textarea>`;
  else if (t === "list") input = `<input type="text" id="${id}" name="${name}" value="${esc((v || []).join(", "))}" placeholder="comma separated">`;
  else input = `<input type="${t === "secret" ? "password" : "text"}" id="${id}" name="${name}" value="${esc(v ?? "")}">`;
  return `<div class="field ${full || t === "json" ? "full" : ""}"><label for="${id}">${esc(label || name)}${spec.required ? " *" : ""}</label>${input}${spec.description ? `<div class="hint">${esc(spec.description)}</div>` : ""}</div>`;
}
function readField(root, name, spec) {
  const el = root.querySelector(`[name="${name}"]`); if (!el) return undefined;
  if (spec.type === "boolean") return el.checked;
  if (spec.type === "integer") return el.value === "" ? null : parseInt(el.value, 10);
  if (spec.type === "number") return el.value === "" ? null : parseFloat(el.value);
  if (spec.type === "list") return el.value.split(",").map((s) => s.trim()).filter(Boolean);
  if (spec.type === "json") { const s = el.value.trim(); if (!s) return {}; try { return JSON.parse(s); } catch { throw new Error(`${name}: invalid JSON`); } }
  return el.value === "" ? null : el.value;
}
const checks = (name, opts, selected) => `<div class="checks">${opts.map((o) => `<label><input type="checkbox" name="${name}" value="${esc(o)}" ${selected.includes(o) ? "checked" : ""}> ${esc(o)}</label>`).join("") || '<span class="muted">—</span>'}</div>`;
const checked = (root, name) => $$(`[name="${name}"]:checked`, root).map((e) => e.value);

function openModal(title, html, onOk) {
  const dlg = $("#modal"); $("#modal-title").textContent = title; $("#modal-body").innerHTML = html; $("#modal-msg").textContent = ""; $("#modal-msg").className = "msg";
  $("#modal-ok").onclick = async (ev) => { ev.preventDefault(); try { await onOk($("#modal-body")); dlg.close(); } catch (e) { $("#modal-msg").textContent = errText(e); $("#modal-msg").className = "msg err"; } };
  dlg.showModal(); return $("#modal-body");
}

function collectorForm(existing, presetType) {
  const c = existing ? clone(existing) : { id: "", type: presetType || S.ctypes[0].type, enabled: true, params: {} };
  const draw = (body) => {
    const t = S.ctypes.find((x) => x.type === c.type) || { params: {}, outputs: [] };
    body.innerHTML = [
      `<div class="field"><label>Type</label><select name="type" ${existing ? "disabled" : ""}>${S.ctypes.map((x) => `<option value="${x.type}" ${x.type === c.type ? "selected" : ""}>${esc(x.title)} (${x.type})</option>`).join("")}</select><div class="hint">${esc(t.description)}</div></div>`,
      field("id", { type: "string", required: true, description: "lowercase, digits, - or _" }, c.id || c.type.replace(/_/g, "-"), "ID"),
      field("interval", { type: "string", description: `e.g. 15s, 1m — type default ${t.default_interval}` }, c.interval || t.default_interval, "Interval"),
      field("enabled", { type: "boolean" }, c.enabled !== false, "Enabled"),
      `<div class="section-title">Parameters · <code>${esc(t.endpoint)}</code></div>`,
      ...Object.entries(t.params || {}).map(([k, sp]) => field("p_" + k, sp, (c.params || {})[k], k)),
      Object.keys(t.params || {}).length ? "" : `<div class="field full muted">This collector has no parameters.</div>`,
      `<div class="section-title">Routing &amp; shaping</div>`,
      `<div class="field full"><label>Sinks</label>${checks("sinks", S.config.sinks.map((s) => s.id), c.sinks || S.config.sinks.map((s) => s.id))}<div class="hint">Leave all checked to send to every enabled sink.</div></div>`,
      t.outputs?.length ? `<div class="field full"><label>Metrics to emit</label>${checks("metrics", t.outputs, c.metrics?.length ? c.metrics : t.outputs)}</div>` : "",
      field("include_namespaces", { type: "list", description: "Only these namespaces" }, c.include_namespaces || [], "Include namespaces"),
      field("exclude_namespaces", { type: "list", description: "Skip these namespaces" }, c.exclude_namespaces || [], "Exclude namespaces"),
      `<div class="field"><label>Cluster aggregates</label>${checks("aggregate", ["sum", "avg", "min", "max", "count"], c.aggregate || [])}<div class="hint">Adds &lt;metric&gt;_&lt;op&gt; with instance=cluster</div></div>`,
      field("metric_prefix", { type: "string", description: `Empty = global (${S.config.metric_prefix})` }, c.metric_prefix ?? "", "Metric prefix override"),
    ].join("");
    body.querySelector('[name="type"]').onchange = (e) => { c.type = e.target.value; c.params = {}; c.metrics = []; c.id = ""; c.interval = null; draw(body); };
  };
  const body = openModal(existing ? `Edit collector · ${existing.id}` : "Add collector", "", async (root) => {
    const t = S.ctypes.find((x) => x.type === c.type);
    const out = { id: readField(root, "id", { type: "string" }), type: c.type, enabled: readField(root, "enabled", { type: "boolean" }), interval: readField(root, "interval", { type: "string" }) };
    const params = {}; for (const [k, sp] of Object.entries(t.params || {})) { const v = readField(root, "p_" + k, sp); if (v !== null && v !== undefined && !(Array.isArray(v) && !v.length && !sp.default?.length)) params[k] = v; }
    if (Object.keys(params).length) out.params = params;
    const sinks = checked(root, "sinks"); if (sinks.length !== S.config.sinks.length) out.sinks = sinks;
    if (t.outputs?.length) { const m = checked(root, "metrics"); if (m.length !== t.outputs.length) out.metrics = m; }
    for (const k of ["include_namespaces", "exclude_namespaces"]) { const v = readField(root, k, { type: "list" }); if (v.length) out[k] = v; }
    const agg = checked(root, "aggregate"); if (agg.length) out.aggregate = agg;
    const pre = readField(root, "metric_prefix", { type: "string" }); if (pre !== null) out.metric_prefix = pre;
    const cfg = clone(S.config);
    const idx = existing ? cfg.collectors.findIndex((x) => x.id === existing.id) : -1;
    if (!existing && cfg.collectors.some((x) => x.id === out.id)) throw new Error(`collector id '${out.id}' already exists`);
    if (idx >= 0) cfg.collectors[idx] = out; else cfg.collectors.push(out);
    await saveConfig(cfg, `Collector ${out.id}`);
  });
  draw(body);
}

function sinkForm(existing) {
  const s = existing ? clone(existing) : { id: "", type: S.stypes[0].type, enabled: true, options: {} };
  const draw = (body) => {
    const t = S.stypes.find((x) => x.type === s.type) || { options: {} };
    body.innerHTML = [`<div class="field"><label>Type</label><select name="type" ${existing ? "disabled" : ""}>${S.stypes.map((x) => `<option value="${x.type}" ${x.type === s.type ? "selected" : ""}>${esc(x.title)}</option>`).join("")}</select><div class="hint">${esc(t.description)}</div></div>`,
      field("id", { type: "string", required: true }, s.id || s.type.replace(/_/g, "-"), "ID"), field("enabled", { type: "boolean" }, s.enabled !== false, "Enabled"),
      `<div class="section-title">Options</div>`, ...Object.entries(t.options).map(([k, sp]) => field("o_" + k, sp, (s.options || {})[k], k))].join("");
    body.querySelector('[name="type"]').onchange = (e) => { s.type = e.target.value; s.options = {}; draw(body); };
  };
  const body = openModal(existing ? `Edit sink · ${existing.id}` : "Add sink", "", async (root) => {
    const t = S.stypes.find((x) => x.type === s.type);
    const out = { id: readField(root, "id", { type: "string" }), type: s.type, enabled: readField(root, "enabled", { type: "boolean" }), options: {} };
    for (const [k, sp] of Object.entries(t.options)) { const v = readField(root, "o_" + k, sp); if (v !== null && v !== undefined) out.options[k] = v; }
    const cfg = clone(S.config), idx = existing ? cfg.sinks.findIndex((x) => x.id === existing.id) : -1;
    if (!existing && cfg.sinks.some((x) => x.id === out.id)) throw new Error(`sink id '${out.id}' already exists`);
    if (idx >= 0) { cfg.sinks[idx] = out; if (out.id !== existing.id) cfg.collectors.forEach((c) => { if (c.sinks) c.sinks = c.sinks.map((x) => (x === existing.id ? out.id : x)); }); } else cfg.sinks.push(out);
    await saveConfig(cfg, `Sink ${out.id}`);
  });
  draw(body);
}

/* ------------------------------------------------------------------ chart */
const Chart = {
  data: [], hover: null,
  draw() {
    const cv = $("#chart"), dpr = window.devicePixelRatio || 1, W = cv.clientWidth, H = 320;
    cv.width = W * dpr; cv.height = H * dpr; cv.style.height = H + "px";
    const g = cv.getContext("2d"); g.scale(dpr, dpr); g.clearRect(0, 0, W, H);
    const series = this.data.filter((s) => !S.hidden.has(s.instance));
    const pad = { l: 58, r: 16, t: 14, b: 28 }, pw = W - pad.l - pad.r, ph = H - pad.t - pad.b;
    const pts = series.flatMap((s) => s.points);
    g.font = "12px -apple-system, Segoe UI, sans-serif";
    if (!pts.length) { g.fillStyle = "#5d6b82"; g.textAlign = "center"; g.fillText("Waiting for samples…", W / 2, H / 2); return; }
    let x0 = Math.min(...pts.map((p) => p[0])), x1 = Math.max(...pts.map((p) => p[0])); if (x1 === x0) x1 = x0 + 1;
    let y0 = Math.min(0, ...pts.map((p) => p[1])), y1 = Math.max(...pts.map((p) => p[1])) * 1.1 || 1;
    const X = (x) => pad.l + ((x - x0) / (x1 - x0)) * pw, Y = (y) => pad.t + ph - ((y - y0) / (y1 - y0)) * ph;
    g.strokeStyle = "#e9edf5"; g.fillStyle = "#5d6b82"; g.lineWidth = 1; g.textAlign = "right"; g.textBaseline = "middle";
    for (let i = 0; i <= 4; i++) { const v = y0 + ((y1 - y0) * i) / 4, y = Y(v); g.beginPath(); g.moveTo(pad.l, y); g.lineTo(W - pad.r, y); g.stroke(); g.fillText(fmt(v, 1), pad.l - 8, y); }
    g.textAlign = "center"; g.textBaseline = "top";
    const tf = x1 - x0 < 600000 ? { hour: "2-digit", minute: "2-digit", second: "2-digit" } : { hour: "2-digit", minute: "2-digit" };
    for (let i = 0; i <= 5; i++) { const x = x0 + ((x1 - x0) * i) / 5; g.textAlign = i === 0 ? "left" : i === 5 ? "right" : "center"; g.fillText(new Date(x).toLocaleTimeString([], tf), X(x), H - pad.b + 8); }
    this.data.forEach((s, i) => {
      if (S.hidden.has(s.instance) || !s.points.length) return;
      const col = COLORS[i % COLORS.length];
      const grad = g.createLinearGradient(0, pad.t, 0, pad.t + ph); grad.addColorStop(0, col + "33"); grad.addColorStop(1, col + "00");
      g.beginPath(); s.points.forEach(([x, y], j) => (j ? g.lineTo(X(x), Y(y)) : g.moveTo(X(x), Y(y))));
      g.strokeStyle = col; g.lineWidth = 2; g.lineJoin = "round"; g.stroke();
      if (series.length <= 3) { g.lineTo(X(s.points.at(-1)[0]), Y(y0)); g.lineTo(X(s.points[0][0]), Y(y0)); g.closePath(); g.fillStyle = grad; g.fill(); }
    });
    if (this.hover != null) {
      const hx = this.hover; g.strokeStyle = "#162d73"; g.setLineDash([4, 4]); g.beginPath(); g.moveTo(X(hx), pad.t); g.lineTo(X(hx), pad.t + ph); g.stroke(); g.setLineDash([]);
      const rows = this.data.map((s, i) => { if (S.hidden.has(s.instance) || !s.points.length) return null; const p = s.points.reduce((a, b) => (Math.abs(b[0] - hx) < Math.abs(a[0] - hx) ? b : a)); g.fillStyle = COLORS[i % COLORS.length]; g.beginPath(); g.arc(X(p[0]), Y(p[1]), 4, 0, 7); g.fill(); return [s.instance, p[1], COLORS[i % COLORS.length]]; }).filter(Boolean).sort((a, b) => b[1] - a[1]);
      const tt = $("#tooltip"); tt.hidden = false;
      tt.innerHTML = `<div class="t">${new Date(hx).toLocaleTimeString()}</div>` + rows.map(([n, v, c]) => `<div class="r"><span><i class="dot" style="background:${c}"></i>${esc(n)}</span><b>${fmt(v, 2)}</b></div>`).join("");
      const left = X(hx) + 14; tt.style.left = (left + 200 > W ? X(hx) - 214 : left) + "px"; tt.style.top = pad.t + 8 + "px";
      this._X = X;
    }
    this._map = { x0, x1, pad, pw };
  },
  bind() {
    const cv = $("#chart");
    cv.addEventListener("mousemove", (e) => { if (!this._map) return; const r = cv.getBoundingClientRect(), { x0, x1, pad, pw } = this._map; const x = x0 + ((e.clientX - r.left - pad.l) / pw) * (x1 - x0); this.hover = x < x0 || x > x1 ? null : x; if (this.hover == null) $("#tooltip").hidden = true; this.draw(); });
    cv.addEventListener("mouseleave", () => { this.hover = null; $("#tooltip").hidden = true; this.draw(); });
    window.addEventListener("resize", () => this.draw());
  },
};

async function loadChart() {
  if ($("#chart-pause").checked || !S.metric) { Chart.draw(); return; }
  Chart.data = await api.get(`api/series?metric=${encodeURIComponent(S.metric)}&top=8`);
  $("#legend").innerHTML = Chart.data.map((s, i) => `<button data-inst="${esc(s.instance)}" class="${S.hidden.has(s.instance) ? "off" : ""}"><i class="dot" style="background:${COLORS[i % COLORS.length]}"></i>${esc(s.instance)} <b>${fmt(s.points.at(-1)?.[1], 2)}</b></button>`).join("");
  Chart.draw();
}

async function loadMetrics() {
  const m = await api.get("api/metrics"), sel = $("#metric-select");
  const names = m.map((x) => x.metric);
  if (!S.metric || !names.includes(S.metric)) S.metric = names.find((n) => n.endsWith("node_cpu_pct")) || names.find((n) => n.endsWith("cpu_millicores")) || names[0] || null;
  sel.innerHTML = m.map((x) => `<option ${x.metric === S.metric ? "selected" : ""}>${esc(x.metric)}</option>`).join("") || "<option>(no metrics yet)</option>";
}

/* ------------------------------------------------------------------ main loop */
async function refresh() {
  try {
    S.status = await api.get("api/status");
    renderHeader(S.status); renderKpis(S.status); renderDashTables(S.status);
    const tab = currentTab();
    if (tab === "collectors" || tab === "sinks") { renderCollectorsTable(); renderSinksTable(); }
    if (tab === "gateways") renderGateways();
    fillClusterSelects();
    if (tab === "dashboard") { await loadMetrics(); await loadChart(); }
    if (tab === "samples" && !$("#sample-filter").matches(":focus")) await renderSamples();
  } catch (e) { $("#pill-gw").className = "pill err"; console.error(e); }
}
const currentTab = () => (location.hash || "#dashboard").slice(1);
function showTab() {
  const t = currentTab(); $$(".view").forEach((v) => (v.hidden = v.id !== "view-" + t)); $$("#tabs a").forEach((a) => a.classList.toggle("active", a.dataset.tab === t));
  if (t === "topology") loadTopology();
  if (t === "settings") renderConfigViews();
  refresh();
}

function bind() {
  window.addEventListener("hashchange", showTab);
  $("#metric-select").onchange = (e) => { S.metric = e.target.value; S.hidden.clear(); loadChart(); };
  $("#legend").onclick = (e) => { const b = e.target.closest("button"); if (!b) return; const i = b.dataset.inst; S.hidden.has(i) ? S.hidden.delete(i) : S.hidden.add(i); b.classList.toggle("off"); Chart.draw(); };
  $("#add-collector").onclick = () => collectorForm(null);
  $("#add-sink").onclick = () => sinkForm(null);
  $("#add-gateway").onclick = () => gatewayForm(null);
  $("#topo-gateway").onchange = (e) => { S.topoGw = e.target.value; loadTopology(); };
  $("#sample-cluster").onchange = renderSamples;
  $("#gateways-table").onclick = async (e) => {
    const d = e.target.dataset;
    if (d.gedit) gatewayForm((S.config.gateways || []).find((g) => g.id === d.gedit));
    if (d.gdel && confirm(`Remove gateway '${d.gdel}'? (a registered gateway re-registers at its next heartbeat unless it is switched off on its host)`)) { try { await api.call("DELETE", `api/gateways/${encodeURIComponent(d.gdel)}`); S.config = (await api.get("api/config")).config; toast(`Gateway ${d.gdel} removed.`); refresh(); } catch (x) { toast(errText(x), true); } }
    if (d.gtest) { e.target.disabled = true; try { const r = await api.post(`api/gateways/${encodeURIComponent(d.gtest)}/test`); toast(r.reachable && r.authorized && !r.error ? `${d.gtest}: reachable · cluster ${r.cluster_name || "?"} · ${r.nodes ?? "?"} nodes` : `${d.gtest}: ${r.error || "unreachable"}`, !!r.error); } catch (x) { toast(errText(x), true); } e.target.disabled = false; }
  };
  $("#gateways-table").onchange = async (e) => { const id = e.target.dataset.gtoggle; if (!id) return; try { await api.post(`api/gateways/${encodeURIComponent(id)}/enabled`, { enabled: e.target.checked }); S.config = (await api.get("api/config")).config; toast(`Gateway ${id} ${e.target.checked ? "enabled" : "disabled"}.`); refresh(); } catch (x) { toast(errText(x), true); } };
  $("#collector-types").onclick = (e) => { const t = e.target.dataset.addtype; if (t) collectorForm(null, t); };
  $("#collectors-table").onclick = async (e) => {
    const d = e.target.dataset;
    if (d.edit) collectorForm(S.config.collectors[+d.edit]);
    if (d.del && confirm(`Delete collector '${S.config.collectors[+d.del].id}'?`)) { const c = clone(S.config); c.collectors.splice(+d.del, 1); saveConfig(c, "Collector deleted").catch((x) => toast(errText(x), true)); }
    if (d.run) { e.target.disabled = true; try { const r = await api.post(`api/collectors/${d.run}/run`); toast(`${d.run}: ${r.count} samples collected and published.`); refresh(); } catch (x) { toast(errText(x), true); } e.target.disabled = false; }
  };
  $("#collectors-table").onchange = (e) => { const i = e.target.dataset.toggle; if (i == null) return; const c = clone(S.config); c.collectors[+i].enabled = e.target.checked; saveConfig(c, `Collector ${c.collectors[+i].id} ${e.target.checked ? "enabled" : "disabled"}`).catch((x) => toast(errText(x), true)); };
  $("#sinks-table").onclick = (e) => {
    const d = e.target.dataset;
    if (d.sedit) sinkForm(S.config.sinks[+d.sedit]);
    if (d.sdel) { const c = clone(S.config), id = c.sinks[+d.sdel].id; if (c.collectors.some((x) => x.sinks?.includes(id))) return toast(`Sink '${id}' is referenced by collectors — remove it there first.`, true); if (confirm(`Delete sink '${id}'?`)) { c.sinks.splice(+d.sdel, 1); saveConfig(c, "Sink deleted").catch((x) => toast(errText(x), true)); } }
  };
  $("#sinks-table").onchange = (e) => { const i = e.target.dataset.stoggle; if (i == null) return; const c = clone(S.config); c.sinks[+i].enabled = e.target.checked; saveConfig(c, `Sink ${c.sinks[+i].id}`).catch((x) => toast(errText(x), true)); };
  $("#save-general").onclick = async () => {
    try {
      const f = $("#general-form"), c = clone(S.config);
      c.client_name = readField(f, "client_name", { type: "string" }) || c.client_name;
      c.default_interval = readField(f, "default_interval", { type: "string" }) || "30s";
      c.metric_prefix = readField(f, "metric_prefix", { type: "string" }) ?? "";
      c.labels = readField(f, "labels", { type: "json" });
      await saveConfig(c, "General settings");
    } catch (e) { toast(errText(e), true); }
  };
  const msg = (t, ok) => { const m = $("#yaml-msg"); m.textContent = t; m.className = "msg " + (ok ? "ok" : "err"); };
  $("#yaml-validate").onclick = async () => { try { await api.post("api/config/validate", { yaml: $("#yaml-editor").value }); msg("✓ Configuration is valid.", true); } catch (e) { msg(errText(e), false); } };
  $("#yaml-save").onclick = async () => { try { const r = await api.put("api/config", { yaml: $("#yaml-editor").value }); S.config = (await api.get("api/config")).config; msg(`✓ Saved & applied.\nFile: ${r.persisted?.file}\nConfigMap: ${r.persisted?.gateway}`, true); refresh(); } catch (e) { msg(errText(e), false); } };
  $("#sample-filter").oninput = () => { clearTimeout(window._sf); window._sf = setTimeout(renderSamples, 250); };
  $("#sample-collector").onchange = renderSamples;
  Chart.bind();
}

function fillClusterSelects() {
  const gws = (S.status?.gateways || []).filter((g) => g.enabled);
  const tsel = $("#topo-gateway"), ssel = $("#sample-cluster");
  const tv = S.topoGw || gws[0]?.id || "", sv = ssel.value;
  const topts = gws.map((g) => `<option value="${esc(g.id)}" ${g.id === tv ? "selected" : ""}>${esc(g.cluster)}</option>`).join("");
  if (tsel.innerHTML !== topts) tsel.innerHTML = topts;
  tsel.hidden = gws.length < 2;
  const sopts = `<option value="">All clusters</option>` + [...new Set(gws.map((g) => g.cluster))].map((c) => `<option ${c === sv ? "selected" : ""}>${esc(c)}</option>`).join("");
  if (ssel.innerHTML !== sopts) ssel.innerHTML = sopts;
  ssel.hidden = gws.length < 2;
}
function loadTopology() {
  const g = S.topoGw || "";
  api.get("api/topology" + (g ? "?gateway=" + encodeURIComponent(g) : "")).then((d) => renderTopology((S.topology = d))).catch(() => renderTopology(null));
}

async function boot() {
  const forceDemo = new URLSearchParams(location.search).has("demo");
  if (!forceDemo) { try { const r = await fetch("api/status"); if (!r.ok && r.status !== 401) throw 0; if (!(r.headers.get("content-type") || "").includes("json")) throw 0; } catch { S.demo = true; } }
  else S.demo = true;
  if (S.demo) { await Demo.init(); $("#pill-demo").hidden = false; $("#yaml-download").removeAttribute("href"); }
  [S.ctypes, S.stypes, S.config] = await Promise.all([api.get("api/collector-types"), api.get("api/sink-types"), api.get("api/config").then((r) => r.config)]);
  $("#sample-collector").innerHTML = `<option value="">All collectors</option>` + S.config.collectors.map((c) => `<option>${esc(c.id)}</option>`).join("");
  bind(); showTab();
  setInterval(refresh, 5000);
}
boot();
