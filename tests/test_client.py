import asyncio
import base64
import copy
import json

import httpx
import pytest
import yaml
from fastapi.testclient import TestClient

from swch_metrics.client import config as C
from swch_metrics.client.collectors import REGISTRY, build
from swch_metrics.client.engine import Engine
from swch_metrics.client.gateway import GatewayClient
from swch_metrics.client.model import Sample
from swch_metrics.client.server import create_app
from swch_metrics.client.sinks import build_sink
from swch_metrics.client.sinks.builtin import EmsStompSink

# ------------------------------------------------------------------ fake gateway
GW = {
    "/health": {"status": "ok"},
    "/pods/usage": [{"namespace": "default", "pod": "web-1", "cpu_millicores": 250.0, "memory_mib": 128.0,
                     "containers": [{"name": "web", "cpu_millicores": 250.0, "memory_mib": 128.0}]},
                    {"namespace": "kube-system", "pod": "coredns", "cpu_millicores": 5.0, "memory_mib": 20.0,
                     "containers": []}],
    "/nodes/usage": [{"node": "n1", "cpu_millicores": 2000.0, "cpu_pct": 50.0, "memory_mib": 4096.0, "memory_pct": 50.0},
                     {"node": "n2", "cpu_millicores": 1000.0, "cpu_pct": 25.0, "memory_mib": 2048.0, "memory_pct": 30.0}],
    "/pods/overview": [{"namespace": "default", "pod": "web-1", "node": "n1", "phase": "Running", "restarts": 3,
                        "cpu_pct_of_request": 50.0, "cpu_pct_of_limit": 25.0, "memory_pct_of_request": 50.0,
                        "memory_pct_of_limit": 25.0}],
    "/pods/stats": [{"namespace": "default", "pod": "web-1", "node": "n1", "cpu_millicores": 250.0,
                     "memory_working_set_mib": 128.0, "network_rx_bytes_total": 1000, "network_tx_bytes_total": 500,
                     "ephemeral_storage_used_mib": 3.0,
                     "volumes": [{"name": "data", "used_mib": 1.0, "capacity_mib": 4.0, "pvc": "data-pvc"}]}],
    "/events": [{"namespace": "default", "type": "Warning", "reason": "BackOff", "object": "Pod/web-1",
                 "message": "back-off", "count": 2, "last_seen": "2026-11-14T00:00:00Z"}],
    "/cadvisor": [{"name": "container_cpu_cfs_throttled_periods_total", "node": "n1", "value": 10,
                   "labels": {"namespace": "default", "pod": "web-1", "container": "web"}},
                  {"name": "container_cpu_cfs_throttled_periods_total", "node": "n1", "value": 5,
                   "labels": {"namespace": "default", "pod": "web-1", "container": "side"}}],
    "/prometheus/query": {"status": "success", "data": {"result": [
        {"metric": {"pod": "web-1", "namespace": "default"}, "value": [1, "0.42"]}]}},
    "/topology": {"cluster_id": "cid", "hash": "h1", "summary": {"nodes": 2, "ready": 1},
                  "nodes": [{"name": "n1", "ready": True, "zone": "a"}, {"name": "n2", "ready": False}]},
    "/apiserver/health": {"livez": ["[+]ping ok", "livez check passed"], "readyz": {"status": 500}},
    "/nodes": [{"node": "n1", "allocatable": {"pods": "110"}}],
}


def handler(request: httpx.Request) -> httpx.Response:
    path = request.url.path
    if path in GW:
        return httpx.Response(200, json=GW[path])
    return httpx.Response(404, json={"detail": "nope"})


@pytest.fixture(autouse=True)
def _mock_gateway(monkeypatch):
    monkeypatch.setattr(GatewayClient, "transport", httpx.MockTransport(handler))


def cfg(**over) -> C.ClientConfig:
    raw = copy.deepcopy(C.DEFAULT_CONFIG)
    raw["sinks"] = [{"id": "mem", "type": "log"}]
    raw.update(over)
    return C.validate(raw)


async def collect(ctype, **ccfg):
    cc = cfg()
    c = build(C.CollectorCfg(id="t", type=ctype, **ccfg), cc)
    gw = GatewayClient(cc.gateway)
    try:
        return c, await c.collect(gw)
    finally:
        await gw.aclose()


def run(coro):
    return asyncio.run(coro)


# ------------------------------------------------------------------ config
@pytest.mark.parametrize("v,s", [("7d", 604800), ("15s", 15), ("1m", 60), ("500ms", .5), ("2h", 7200), (30, 30), ("1.5 min", 90)])
def test_duration(v, s):
    assert C.parse_duration(v) == s


def test_env_expansion(monkeypatch):
    monkeypatch.setenv("SWARM_ID", "swarm-9")
    assert C.expand_env({"a": "${SWARM_ID}", "b": ["${MISSING:-dflt}"], "c": "${MISSING}"}) == \
        {"a": "swarm-9", "b": ["dflt"], "c": ""}


def test_default_config_valid():
    c = C.validate(C.DEFAULT_CONFIG)
    assert {x.type for x in c.collectors} <= set(REGISTRY)
    assert c.sinks[0].type == "ems_stomp"


@pytest.mark.parametrize("mut,err", [
    (lambda r: r["collectors"].append(dict(r["collectors"][0])), "duplicate collector"),
    (lambda r: r["collectors"][0].update(sinks=["nope"]), "unknown sink"),
    (lambda r: r["collectors"][0].update(interval="200ms"), "interval must be"),
    (lambda r: r["collectors"][0].update(id="Bad Id"), "id must be"),
    (lambda r: r.update(default_interval="soon"), "invalid duration"),
])
def test_config_errors(mut, err):
    raw = copy.deepcopy(C.DEFAULT_CONFIG)
    mut(raw)
    with pytest.raises(Exception, match=err):
        C.validate(raw)


def test_store_load_order_and_save(tmp_path, monkeypatch):
    base = tmp_path / "base.yaml"
    base.write_text(yaml.safe_dump({**C.DEFAULT_CONFIG, "client_name": "from-base"}))
    store = C.ConfigStore(str(base))
    assert store.load().client_name == "from-base"
    saved = tmp_path / "saved.yaml"
    saved.write_text(yaml.safe_dump({**C.DEFAULT_CONFIG, "client_name": "from-saved"}))
    assert C.ConfigStore(str(base)).load().client_name == "from-saved"
    raw = {**C.DEFAULT_CONFIG, "client_name": "from-ui", "persist": {"mode": "gateway", "path": str(tmp_path / "data.yaml")}}
    calls = []
    rep = store.save(raw, gateway_put=lambda ns, text: calls.append((ns, text)))
    assert rep["file"].endswith("data.yaml") and "swch-metrics-client-config-saved" in rep["gateway"]
    assert calls[0][0] == "swarm-system" and "from-ui" in calls[0][1]
    assert "${GATEWAY_API_KEY" in calls[0][1]   # secrets stay as placeholders
    assert C.ConfigStore(str(base)).load().client_name == "from-ui"


# ------------------------------------------------------------------ collectors
def test_pod_usage_prefix_labels_filters(monkeypatch):
    monkeypatch.setenv("SWARM_ID", "swarm-1")
    c, s = run(collect("pod_usage", exclude_namespaces=["kube-system"]))
    assert {x.metric for x in s} == {"swch_k3s_pod_cpu_millicores", "swch_k3s_pod_memory_mib"}
    assert all(x.labels["namespace"] == "default" and x.labels["swarm_id"] == "swarm-1" for x in s)
    assert s[0].instance == "web-1" and s[0].source == "/pods/usage" and s[0].collector == "t"


def test_pod_usage_include_and_whitelist():
    _, s = run(collect("pod_usage", include_namespaces=["kube-system"], metrics=["pod_memory_mib"]))
    assert [(x.metric, x.instance, x.value) for x in s] == [("swch_k3s_pod_memory_mib", "coredns", 20.0)]


def test_node_usage_aggregates():
    _, s = run(collect("node_usage", aggregate=["avg", "max"], metrics=["node_cpu_pct"]))
    by = {x.metric: x for x in s if x.instance == "cluster"}
    assert by["swch_k3s_node_cpu_pct_avg"].value == 37.5 and by["swch_k3s_node_cpu_pct_max"].value == 50.0
    assert by["swch_k3s_node_cpu_pct_avg"].labels["scope"] == "cluster"


def test_custom_prefix():
    _, s = run(collect("node_usage", metric_prefix=""))
    assert s[0].metric == "node_cpu_millicores"


def test_pod_overview_and_running():
    _, s = run(collect("pod_overview"))
    d = {x.metric: x.value for x in s}
    assert d["swch_k3s_pod_restarts"] == 3 and d["swch_k3s_pod_running"] == 1


def test_pod_stats_rate_needs_two_polls():
    c, s1 = run(collect("pod_stats"))
    assert not any("per_sec" in x.metric for x in s1)
    assert any(x.metric == "swch_k3s_pod_volume_used_pct" and x.value == 25.0 for x in s1)
    key = ("default", "web-1", "rx")
    t0, v0 = c.state["prev"][key]
    c.state["prev"][key] = (t0 - 10, v0 - 500)  # pretend 500 bytes over 10 s
    out = c.transform(GW["/pods/stats"])
    rx = [x for x in out if x.metric == "pod_network_rx_bytes_per_sec"][0]
    assert 49 < rx.value <= 50


def test_events_are_deduplicated():
    c, first = run(collect("events"))
    assert first == []                                   # priming poll: history is not replayed
    GW["/events"][0]["count"] = 3
    try:
        out = c.transform(GW["/events"])
        assert len(out) == 1 and out[0].labels["reason"] == "BackOff" and out[0].value == 3
        assert c.transform(GW["/events"]) == []           # unchanged -> nothing new
    finally:
        GW["/events"][0]["count"] = 2


def test_cadvisor_sum_by_pod_and_rate():
    c, s = run(collect("cadvisor", params={"rate": False}))
    assert len(s) == 1 and s[0].value == 15 and s[0].labels == {
        **{k: v for k, v in s[0].labels.items() if k in ("swarm_id",)}, "namespace": "default", "pod": "web-1", "node": "n1"}
    c2, s2 = run(collect("cadvisor"))
    assert s2 == []                                      # first rate poll primes the counter


def test_prometheus_query():
    _, s = run(collect("prometheus", params={"query": "up", "name": "p95"}))
    assert s[0].metric == "swch_k3s_p95" and s[0].value == 0.42 and s[0].instance == "web-1"


def test_topology_publishes_document_on_change_only():
    c, s = run(collect("topology"))
    d = {x.metric: x for x in s if x.instance == "cluster"}
    assert d["swch_k3s_cluster_nodes_ready"].value == 1 and d["swch_k3s_topology"].payload["hash"] == "h1"
    again = c.post_process(c.transform(GW["/topology"]))
    assert not any(x.payload for x in again)


def test_apiserver_health():
    _, s = run(collect("apiserver_health"))
    assert {x.metric: x.value for x in s} == {"swch_k3s_apiserver_livez": 1.0, "swch_k3s_apiserver_readyz": 0.0}


def test_http_json_generic():
    _, s = run(collect("http_json", params={"path": "/nodes", "value_field": "allocatable.pods",
                                             "metric_name": "pods_cap", "label_fields": ["node"],
                                             "instance_field": "node"}))
    assert (s[0].metric, s[0].value, s[0].labels["node"], s[0].instance) == ("swch_k3s_pods_cap", 110.0, "n1", "n1")


def test_required_param():
    with pytest.raises(ValueError, match="required"):
        build(C.CollectorCfg(id="x", type="prometheus", params={"query": ""}), cfg())


# ------------------------------------------------------------------ sinks
class FakeConn:
    def __init__(self):
        self.sent = []

    def is_connected(self):
        return True

    def send(self, destination, body, headers):
        self.sent.append((destination, body, headers))

    def disconnect(self):
        pass


def test_ems_sink_message_shape(monkeypatch):
    cc = cfg()
    sink = EmsStompSink(C.SinkCfg(id="ems", type="ems_stomp", options={"destination": "/topic/{metric}_SENSOR"}), cc)
    conn = FakeConn()
    monkeypatch.setattr(sink, "_connect_sync", lambda: conn)
    s = Sample(metric="swch_k3s_node_cpu_pct", value=50.0, labels={"node": "n1", "zone": "a", "swarm_id": "sw"},
               instance="n1", timestamp_ms=1700000000000, collector="nodes", source="/nodes/usage")
    topo = Sample(metric="swch_k3s_topology", value=2, payload={"hash": "h"}, collector="topology", source="/topology")
    assert run(sink.send([s, topo])) == 2
    dest, body, h = conn.sent[0]
    assert dest == "/topic/swch_k3s_node_cpu_pct_SENSOR"
    assert json.loads(body) == {"metricValue": 50.0, "level": 1, "timestamp": 1700000000000}
    assert h["metric"] == "swch_k3s_node_cpu_pct" and h["instance"] == "n1" and h["zone"] == "a"
    assert h["swarm-id"] == "sw" and h["label-node"] == "n1"
    assert h["source-endpoint"].endswith(":8000/nodes/usage")
    assert json.loads(conn.sent[1][1]) == {"hash": "h"}          # documents go as JSON body
    assert sink.status()["sent"] == 2 and sink.connected


def test_ems_sink_backoff_when_broker_down(monkeypatch):
    sink = EmsStompSink(C.SinkCfg(id="ems", type="ems_stomp"), cfg())

    def boom():
        raise OSError("connection refused")

    monkeypatch.setattr(sink, "_connect_sync", boom)
    with pytest.raises(ConnectionError):
        run(sink.send([Sample(metric="m", value=1)]))
    with pytest.raises(ConnectionError, match="refused"):   # no reconnect storm while backing off
        run(sink.send([Sample(metric="m", value=1)]))
    assert sink.status()["connected"] is False and sink.errors == 2


def test_jsonl_sink(tmp_path):
    p = tmp_path / "m.jsonl"
    sink = build_sink(C.SinkCfg(id="j", type="jsonl", options={"path": str(p)}), cfg())
    run(sink.send([Sample(metric="a", value=1), Sample(metric="b", value=2)]))
    assert [json.loads(line)["metric"] for line in p.read_text().splitlines()] == ["a", "b"]


# ------------------------------------------------------------------ engine + server
def test_engine_routes_to_selected_sinks(tmp_path):
    raw = copy.deepcopy(C.DEFAULT_CONFIG)
    raw["sinks"] = [{"id": "a", "type": "jsonl", "options": {"path": str(tmp_path / "a.jsonl")}},
                    {"id": "b", "type": "jsonl", "options": {"path": str(tmp_path / "b.jsonl")}}]
    raw["collectors"] = [{"id": "nodes", "type": "node_usage", "sinks": ["b"]},
                         {"id": "broken", "type": "http_json", "params": {"path": "/missing", "value_field": "x",
                                                                           "metric_name": "x"}}]

    async def go():
        e = Engine(C.validate(raw))
        await e.run_once("nodes")
        await e.run_once("broken")
        st = e.status()
        await e.stop()
        return e, st

    e, st = run(go())
    assert not (tmp_path / "a.jsonl").exists() and (tmp_path / "b.jsonl").exists()
    by = {c["id"]: c for c in st["collectors"]}
    assert by["nodes"]["status"] == "ok" and by["nodes"]["last_count"] == 8
    assert by["broken"]["status"] == "error" and "404" in by["broken"]["last_error"]
    assert e.series_points("swch_k3s_node_cpu_pct")[0]["instance"] == "n1"


@pytest.fixture
def ui(tmp_path, monkeypatch):
    raw = copy.deepcopy(C.DEFAULT_CONFIG)
    raw["sinks"] = [{"id": "log", "type": "log"}]
    raw["persist"] = {"mode": "file", "path": str(tmp_path / "data.yaml")}
    raw["collectors"] = [{"id": "nodes", "type": "node_usage", "interval": "1h"}]
    base = tmp_path / "base.yaml"
    base.write_text(yaml.safe_dump(raw))
    store = C.ConfigStore(str(base))
    store.load()
    with TestClient(create_app(store)) as c:
        yield c, store


def test_server_status_and_ui(ui):
    c, _ = ui
    assert c.get("/healthz").json()["status"] == "ok"
    assert "Swarmchestrate" in c.get("/").text
    assert c.get("/ui/app.js").status_code == 200
    st = c.get("/api/status").json()
    assert st["collectors"][0]["id"] == "nodes" and st["gateway"]["url"].endswith(":8000")
    assert {t["type"] for t in c.get("/api/collector-types").json()} == set(REGISTRY)


def test_server_run_now_series_samples(ui):
    c, _ = ui
    r = c.post("/api/collectors/nodes/run").json()
    assert r["count"] == 8
    assert any(m["metric"] == "swch_k3s_node_cpu_pct" for m in c.get("/api/metrics").json())
    pts = c.get("/api/series", params={"metric": "swch_k3s_node_cpu_pct"}).json()
    assert {p["instance"] for p in pts} == {"n1", "n2"}
    assert c.get("/api/samples", params={"q": "n2"}).json()[0]["instance"] == "n2"
    assert c.post("/api/collectors/nope/run").status_code == 404


def test_server_config_put_validate_reload(ui):
    c, store = ui
    cur = c.get("/api/config").json()["config"]
    bad = copy.deepcopy(cur)
    bad["collectors"].append({"id": "x", "type": "does_not_exist"})
    r = c.put("/api/config", json={"config": bad})
    assert r.status_code == 422 and "unknown collector type" in r.text
    assert c.post("/api/config/validate", json={"yaml": "collectors: [ {id: 'A B'} ]"}).status_code == 422
    good = copy.deepcopy(cur)
    good["collectors"].append({"id": "pods", "type": "pod_usage", "interval": "1h"})
    r = c.put("/api/config", json={"config": good})
    assert r.status_code == 200 and r.json()["persisted"]["file"].endswith("data.yaml")
    assert [x["id"] for x in c.get("/api/status").json()["collectors"]] == ["nodes", "pods"]
    assert "pods" in c.get("/api/config/download").text
    r = c.put("/api/config", json={"yaml": yaml.safe_dump(cur)})
    assert r.status_code == 200 and len(c.get("/api/status").json()["collectors"]) == 1


def test_server_basic_auth(ui, monkeypatch):
    c, store = ui
    raw = copy.deepcopy(store.raw)
    raw["ui"] = {"password": "pw"}
    store.raw = raw
    assert c.get("/api/status").status_code == 401
    tok = base64.b64encode(b"admin:pw").decode()
    assert c.get("/api/status", headers={"Authorization": f"Basic {tok}"}).status_code == 200
    assert c.get("/healthz").status_code == 200
