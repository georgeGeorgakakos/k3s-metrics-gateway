"""v0.3: one sidecar, many gateways; gateway self-registration."""
from __future__ import annotations

import asyncio
import copy
import json

import httpx
import pytest
import yaml
from fastapi.testclient import TestClient

from swch_metrics.client import config as C
from swch_metrics.client.engine import Engine
from swch_metrics.client.gateway import GatewayClient
from swch_metrics.client.model import Sample
from swch_metrics.client.server import create_app
from swch_metrics.client.sinks import build_sink
from swch_metrics.gateway import registrar as R
from tests.test_client import handler

TOKEN = "reg-token-123"
CLUSTERS = {"gw-a.test": ("epm-server", "uid-a"), "gw-b.test": ("edge-07", "uid-b"), "gw-c.test": ("edge-08", "uid-c")}


def multi(request: httpx.Request) -> httpx.Response:
    """Mock gateways: same data on every host, but each host is its own cluster."""
    r = handler(request)
    host = request.url.host
    if request.url.path.endswith("/topology") and host in CLUSTERS:
        doc = json.loads(r.content)
        doc["cluster_name"], doc["cluster_id"] = CLUSTERS[host]
        return httpx.Response(200, json=doc)
    if host == "down.test":
        return httpx.Response(503, text="unavailable")
    return r


@pytest.fixture(autouse=True)
def _transport(monkeypatch):
    monkeypatch.setattr(GatewayClient, "transport", httpx.MockTransport(multi))


def run(coro):
    return asyncio.run(coro)


def raw_cfg(tmp_path, gateways, **extra):
    raw = copy.deepcopy(C.DEFAULT_CONFIG)
    raw["gateways"] = gateways
    raw["persist"] = {"mode": "file", "path": str(tmp_path / "data.yaml")}
    raw["sinks"] = [{"id": "log", "type": "log", "options": {"max_lines": 0}}]
    raw["collectors"] = [{"id": "pods", "type": "pod_overview", "interval": "1h"},
                         {"id": "nodes", "type": "node_usage", "interval": "1h"},
                         {"id": "topology", "type": "topology", "interval": "1h"}]
    raw.update(extra)
    return raw


def app_client(tmp_path, monkeypatch, raw, password=None):
    if password:
        raw["ui"] = {"password": password}
    base = tmp_path / "base.yaml"
    base.write_text(yaml.safe_dump(raw))
    monkeypatch.setenv("CONFIG_DATA_PATH", str(tmp_path / "data.yaml"))
    monkeypatch.setenv("CONFIG_SAVED_PATH", str(tmp_path / "none.yaml"))
    store = C.ConfigStore(str(base))
    store.load()
    return TestClient(create_app(store)), store


# ------------------------------------------------------------------ configuration
def test_legacy_single_gateway_is_normalised():
    cfg = C.validate({"gateway": {"url": "http://old.test:8000", "api_key": "k"}})
    assert [g.id for g in cfg.gateways] == ["default"] and cfg.gateway.url == "http://old.test:8000"
    raw = {"gateway": {"url": "http://old.test"}}
    assert C.normalize_raw(raw) == {"gateways": [{"id": "default", "url": "http://old.test"}]}


def test_gateway_ids_unique_and_collector_references_checked():
    with pytest.raises(Exception, match="duplicate gateway"):
        C.validate({"gateways": [{"id": "a", "url": "http://x"}, {"id": "a", "url": "http://y"}]})
    with pytest.raises(Exception, match="unknown gateway"):
        C.validate({"gateways": [{"id": "a", "url": "http://x"}],
                    "collectors": [{"id": "n", "type": "node_usage", "gateways": ["b"]}]})


@pytest.mark.parametrize("v,out", [("EPM Server.01", "epm-server-01"), ("edge_7", "edge_7"), ("", "gateway"),
                                   ("---", "gateway")])
def test_slug(v, out):
    assert C.slug(v) == out


# ------------------------------------------------------------------ engine
def test_engine_polls_every_gateway_with_cluster_labels(tmp_path):
    cfg = C.validate(raw_cfg(tmp_path, [{"id": "a", "url": "http://gw-a.test"},
                                        {"id": "b", "url": "http://gw-b.test", "labels": {"site": "lab"}},
                                        {"id": "off", "url": "http://gw-c.test", "enabled": False}]))

    async def go():
        e = Engine(cfg)
        await asyncio.gather(*(e._learn_topology(g) for g in e.gws))
        out = await e.run_once("nodes")
        return e, out

    e, samples = run(go())
    assert set(e.gws) == {"a", "b"} and len(e.collectors) == 6          # 3 collectors x 2 enabled gateways
    by = {s.labels["cluster"] for s in samples}
    assert by == {"epm-server", "edge-07"}
    b = next(s for s in samples if s.labels["cluster"] == "edge-07")
    assert b.labels["gateway"] == "b" and b.labels["cluster_uid"] == "uid-b" and b.labels["site"] == "lab"
    assert any(k[1].startswith("edge-07/") for k in e.series)            # instances prefixed per cluster
    st = e.status()
    assert st["gateway"]["count"] == 3 and {g["cluster"] for g in st["gateways"]} >= {"epm-server", "edge-07"}
    row = next(c for c in st["collectors"] if c["id"] == "nodes")
    assert {p["gateway"] for p in row["per_gateway"]} == {"a", "b"} and row["status"] == "ok"


def test_collector_can_be_bound_to_one_gateway(tmp_path):
    raw = raw_cfg(tmp_path, [{"id": "a", "url": "http://gw-a.test"}, {"id": "b", "url": "http://gw-b.test"}])
    raw["collectors"][1]["gateways"] = ["b"]
    e = Engine(C.validate(raw))
    assert [k for k in e.collectors if k[0] == "nodes"] == [("nodes", "b")]


def test_one_gateway_down_does_not_stop_the_others(tmp_path):
    cfg = C.validate(raw_cfg(tmp_path, [{"id": "a", "url": "http://gw-a.test"}, {"id": "x", "url": "http://down.test"}]))
    e = Engine(cfg)
    samples = run(e.run_once("nodes"))
    assert samples and all(s.labels["gateway"] == "a" for s in samples)
    row = next(c for c in e.status()["collectors"] if c["id"] == "nodes")
    assert row["status"] == "degraded" and "x: " in row["last_error"]


def test_explicit_cluster_name_wins(tmp_path):
    cfg = C.validate(raw_cfg(tmp_path, [{"id": "a", "url": "http://gw-a.test", "cluster": "lab-a"}]))
    e = Engine(cfg)
    run(e._learn_topology("a"))
    assert e.labels_for("a")["cluster"] == "lab-a"


# ------------------------------------------------------------------ registration API
def reg(c, body, token=TOKEN, **headers):
    h = {"Authorization": f"Bearer {token}"} if token else {}
    return c.post("/api/register", json=body, headers={**h, **headers})


def test_registration_disabled_without_token(tmp_path, monkeypatch):
    monkeypatch.delenv("REGISTRATION_TOKEN", raising=False)
    c, _ = app_client(tmp_path, monkeypatch, raw_cfg(tmp_path, [], registration={"token": ""}))
    with c:
        assert reg(c, {"url": "http://gw-b.test"}).status_code == 403


def test_register_new_cluster_then_heartbeat_then_update(tmp_path, monkeypatch):
    monkeypatch.setenv("REGISTRATION_TOKEN", TOKEN)
    c, store = app_client(tmp_path, monkeypatch, raw_cfg(tmp_path, []), password="pw")
    with c:
        assert reg(c, {"url": "http://gw-b.test"}, token="wrong").status_code == 401
        assert reg(c, {"url": "ftp://x", "cluster_name": "x"}).status_code == 422
        r = reg(c, {"url": "http://gw-b.test/", "api_key": "kb", "cluster_name": "Edge 07", "cluster_uid": "uid-b",
                    "swarm_id": "swarm-01", "nodes": 3, "gateway_version": "0.3.0"})
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["id"] == "edge-07" and body["action"] == "registered" and body["enabled"] is True
        # persisted to the client's file and applied to the running engine
        saved = yaml.safe_load((tmp_path / "data.yaml").read_text())
        g = saved["gateways"][0]
        assert g["url"] == "http://gw-b.test" and g["api_key"] == "kb" and g["source"] == "registered"
        assert g["cluster"] == "Edge 07" and g["info"]["nodes"] == 3
        gws = c.get("/api/gateways", auth=("admin", "pw")).json()
        assert gws[0]["id"] == "edge-07" and gws[0]["last_heartbeat"] and not gws[0]["stale"]
        assert c.post("/api/collectors/nodes/run", auth=("admin", "pw")).json()["count"] > 0
        # same cluster again -> heartbeat only (no reload)
        assert reg(c, {"url": "http://gw-b.test", "cluster_uid": "uid-b", "nodes": 4})\
            .json()["action"] == "heartbeat"
        assert c.get("/api/gateways", auth=("admin", "pw")).json()[0]["info"]["nodes"] == 4
        # same cluster, new URL / key -> updated in place (matched by cluster UID)
        r = reg(c, {"url": "http://gw-b2.test", "api_key": "kb2", "cluster_uid": "uid-b"}).json()
        assert r["action"] == "updated" and r["id"] == "edge-07"
        assert [g["url"] for g in store.raw["gateways"]] == ["http://gw-b2.test"]
        # a second cluster with a clashing name gets its own id
        r = reg(c, {"url": "http://gw-c.test", "cluster_name": "edge-07", "cluster_uid": "uid-c"}).json()
        assert r["id"] == "edge-07-2"
        # the UI API stays protected; /api/register is not
        assert c.get("/api/gateways").status_code == 401


def test_registration_matches_existing_gateway_by_url(tmp_path, monkeypatch):
    """Upgrade path: host A was configured by hand (id 'default'); its first registration must not duplicate it."""
    monkeypatch.setenv("REGISTRATION_TOKEN", TOKEN)
    c, store = app_client(tmp_path, monkeypatch,
                          raw_cfg(tmp_path, [{"id": "default", "url": "http://gw-a.test", "api_key": "${GATEWAY_API_KEY:-}"}]))
    with c:
        r = reg(c, {"url": "http://gw-a.test", "cluster_name": "epm-server", "cluster_uid": "uid-a"}).json()
        assert r["id"] == "default" and r["action"] == "updated" and len(store.raw["gateways"]) == 1
        assert store.raw["gateways"][0]["cluster"] == "epm-server"
        assert store.raw["gateways"][0]["api_key"] == "${GATEWAY_API_KEY:-}"     # not sent -> kept


def test_pending_approval_and_enable_delete(tmp_path, monkeypatch):
    monkeypatch.setenv("REGISTRATION_TOKEN", TOKEN)
    c, store = app_client(tmp_path, monkeypatch, raw_cfg(tmp_path, [], registration={"auto_approve": False}))
    with c:
        r = reg(c, {"url": "http://gw-b.test", "cluster_name": "edge-07"}).json()
        assert r["action"] == "pending-approval" and r["enabled"] is False
        assert c.get("/api/status").json()["gateway"]["count"] == 1
        assert c.post("/api/gateways/edge-07/enabled", json={"enabled": True}).json()["enabled"] is True
        assert c.post("/api/gateways/edge-07/test").json()["cluster_name"] == "edge-07"
        assert c.delete("/api/gateways/edge-07").status_code == 200
        assert store.raw["gateways"] == []
        assert c.delete("/api/gateways/edge-07").status_code == 404


def test_topology_per_gateway(tmp_path, monkeypatch):
    c, _ = app_client(tmp_path, monkeypatch, raw_cfg(tmp_path, [{"id": "a", "url": "http://gw-a.test"},
                                                                {"id": "b", "url": "http://gw-b.test"}]))
    with c:
        assert c.get("/api/topology", params={"gateway": "b"}).json()["cluster_name"] == "edge-07"
        assert c.get("/api/topology").json()["gateway_id"] == "a"


def test_empty_sidecar_waits_for_registrations(tmp_path, monkeypatch):
    c, _ = app_client(tmp_path, monkeypatch, raw_cfg(tmp_path, []))
    with c:
        st = c.get("/api/status").json()
        assert st["gateways"] == [] and st["gateway"]["ok"] is None
        assert c.get("/api/topology").status_code == 404


# ------------------------------------------------------------------ sinks
def test_ems_headers_and_body_carry_the_cluster():
    cfg = C.validate({"gateways": [{"id": "b", "url": "http://gw-b.test/swch-metrics"}]})
    sk = build_sink(C.SinkCfg(id="ems", type="ems_stomp", options={"instance_template": "{cluster}/{instance}"}), cfg)
    s = Sample(metric="swch_k3s_node_cpu_pct", value=12.5, instance="master", source="/nodes/usage",
               labels={"cluster": "edge-07", "cluster_uid": "uid-b", "gateway": "b", "node": "master"})
    h = sk.headers(s)
    assert h["cluster"] == "edge-07" and h["cluster-uid"] == "uid-b" and h["instance"] == "edge-07/master"
    assert h["source-endpoint"] == "http://gw-b.test/swch-metrics/nodes/usage"
    assert json.loads(sk.body(s)) == {"metricValue": 12.5, "level": 1, "timestamp": s.timestamp_ms,
                                      "cluster": "edge-07"}
    plain = build_sink(C.SinkCfg(id="e", type="ems_stomp", options={"body_labels": []}), cfg)
    assert "cluster" not in json.loads(plain.body(s)) and plain.headers(s)["instance"] == "master"


def test_trust_sink_keeps_clusters_apart(tmp_path):
    cfg = C.validate({"gateways": [{"id": "a", "url": "http://a"}, {"id": "b", "url": "http://b"}]})
    sk = build_sink(C.SinkCfg(id="t", type="optimusdb_trust",
                              options={"state_path": str(tmp_path / "t.json"), "dry_run": True}), cfg)
    t = 1_794_000_000_000
    run(sk.send([Sample("swch_k3s_node_ready", 1, {"node": "master", "cluster": "epm-server", "cluster_uid": "uid-a"},
                        "master", t),
                 Sample("swch_k3s_node_ready", 0, {"node": "master", "cluster": "edge-07", "swarm_id": "swarm-01"},
                        "master", t)]))
    docs = {d["_id"]: d for d in sk.evaluate(t + 60_000) if d["type"] == "trust_evidence"}
    a1a, a1b = docs["trust:epm-server:node:master:A1"], docs["trust:edge-07:node:master:A1"]
    assert a1a["value"] == 1.0 and a1b["value"] == 0.0
    assert a1a["cluster_uid"] == "uid-a" and a1b["swarm_id"] == "swarm-01"
    assert a1b["subject_did"] == "did:swarm:edge-07:node:master"
    assert sk.status()["clusters"] == ["edge-07", "epm-server"]


def test_trust_state_from_v02_gets_the_fallback_cluster(tmp_path):
    p = tmp_path / "t.json"
    p.write_text(json.dumps({"cluster_id": "", "subjects": [{"kind": "node", "name": "epm-server", "namespace": "",
                                                              "node": "", "first_seen": 1, "last_seen": 1,
                                                              "avail": {"1": [1, 1]}}]}))
    cfg = C.validate({"gateways": []})
    sk = build_sink(C.SinkCfg(id="t", type="optimusdb_trust", options={"state_path": str(p), "cluster": "epm-server"}),
                    cfg)
    assert list(sk.subjects) == ["epm-server|node||epm-server"]


# ------------------------------------------------------------------ gateway side
def test_registration_payload(monkeypatch):
    monkeypatch.setenv("PUBLIC_URL", "http://10.0.0.7/swch-metrics/")
    monkeypatch.setenv("API_KEY", "k7")
    monkeypatch.setenv("CLUSTER_NAME", "edge-07")
    topo = {"cluster_id": "uid-7", "swarm_id": "swarm-01", "kubernetes_version": "v1.33.4+k3s1",
            "summary": {"nodes": 2, "ready": 2}, "nodes": [{"name": "n1"}, {"name": "n2"}], "hash": "h"}
    p = R.registration_payload(topo)
    assert p["url"] == "http://10.0.0.7/swch-metrics" and p["api_key"] == "k7" and p["cluster_uid"] == "uid-7"
    assert p["cluster_name"] == "edge-07" and p["nodes"] == 2 and p["node_names"] == ["n1", "n2"]


def test_registrar_posts_with_bearer_token(monkeypatch):
    monkeypatch.setenv("PUBLIC_URL", "http://10.0.0.7/swch-metrics")
    calls = []

    class Resp:
        status_code = 200
        headers = {"content-type": "application/json"}

        def json(self):
            return {"id": "edge-07", "action": "registered"}

    def fake_post(url, json, headers, timeout):
        calls.append((url, json, headers))
        return Resp()

    monkeypatch.setattr(R.requests, "post", fake_post)
    r = R.Registrar(lambda: {"cluster_id": "u"}, ["http://ems/swch-metrics-ui/api/register"], "tok")
    assert r.register_once() is True
    url, body, headers = calls[0]
    assert headers == {"Authorization": "Bearer tok"} and body["url"] == "http://10.0.0.7/swch-metrics"
    assert r.status()["targets"][url]["status"] == "registered"


def test_registrar_from_env(monkeypatch):
    monkeypatch.delenv("REGISTER_URL", raising=False)
    assert R.Registrar.from_env(lambda: None) is None
    monkeypatch.setenv("REGISTER_URL", "http://a/api/register, http://b/api/register")
    monkeypatch.setenv("REGISTER_INTERVAL", "120")
    r = R.Registrar.from_env(lambda: None)
    assert r.urls == ["http://a/api/register", "http://b/api/register"] and r.interval == 120


def test_gateway_registration_endpoint(monkeypatch, fake_k8s):
    from swch_metrics.gateway import app as A
    monkeypatch.setattr(A, "API_KEY", None)
    with TestClient(A.app) as c:
        assert c.get("/registration").json()["enabled"] is False
        assert c.post("/registration").status_code == 409
