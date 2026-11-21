"""OptimusDB trust sink: A1 availability / A3 operational stability evidence."""
from __future__ import annotations

import asyncio
import copy
import json

import httpx
import pytest

from swch_metrics.client import config as C
from swch_metrics.client.model import Sample
from swch_metrics.client.sinks import build_sink
from swch_metrics.client.sinks.optimusdb_trust import HOUR_MS, workload_of

T0 = 1_794_000_000_000 // HOUR_MS * HOUR_MS   # an hour boundary
P = "swch_k3s_"


def run(coro):
    return asyncio.run(coro)


def make_sink(tmp_path, **opts):
    raw = copy.deepcopy(C.DEFAULT_CONFIG)
    raw["labels"] = {"cluster": "ems-server"}
    o = {"url": "http://optimus.test/optimusdb1", "state_path": str(tmp_path / "trust.json"), **opts}
    raw["sinks"] = [{"id": "trust", "type": "optimusdb_trust", "options": o}]
    cc = C.validate(raw)
    return build_sink(cc.sinks[0], cc)


class Recorder:
    def __init__(self, status=200):
        self.calls: list[dict] = []
        self.status = status

    def __call__(self, req: httpx.Request):
        self.calls.append({"url": str(req.url), "body": json.loads(req.content)})
        return httpx.Response(self.status, json={"status": 200, "data": "ok"})

    @property
    def docs(self):
        return [d for c in self.calls for d in c["body"]["criteria"]]


def pod(metric, value, ts, name="optimusdb1-7f9c6d5b8-x2k9p", ns="optimusddc"):
    return Sample(metric=P + metric, value=value, timestamp_ms=ts, instance=name,
                  labels={"namespace": ns, "pod": name, "node": "epm-server", "cluster": "ems-server"})


def node(value, ts, name="epm-server"):
    return Sample(metric=P + "node_ready", value=value, timestamp_ms=ts, instance=name, labels={"node": name})


def by_id(docs, suffix):
    return next(d for d in docs if d["_id"].endswith(suffix))


def test_pod_availability_and_stability(tmp_path):
    s = make_sink(tmp_path, history=False)
    seq = [(1, 0), (1, 0), (0, 0), (1, 2), (1, 2)]   # (running, restart counter)
    for i, (up, rc) in enumerate(seq):
        ts = T0 + i * 60_000
        run(s.send([pod("pod_running", up, ts), pod("pod_restarts", rc, ts),
                    pod("pod_cpu_pct_of_limit", 12.0, ts)]))
    docs = s.evaluate(T0 + 10 * 60_000)
    a1 = by_id(docs, ":A1")
    assert a1["_id"] == "trust:ems-server:pod:optimusddc/optimusdb1-7f9c6d5b8-x2k9p:A1"
    assert a1["value"] == 0.8 and a1["evidence"]["checks_ok"] == 4 and a1["evidence"]["checks_total"] == 5
    assert a1["slo_met"] is False and a1["attribute"] == "Availability"
    assert a1["subject"]["workload"] == "optimusdb1" and a1["subject_kind"] == "pod"
    assert a1["evidence"]["source_metric"] == "swch_k3s_pod_running"
    a3 = by_id(docs, ":A3")
    assert a3["value"] == 2 and a3["slo_met"] is True and a3["score"] == pytest.approx(0.3333)
    assert a3["evidence"]["restart_counter"] == 2
    mon = next(d for d in docs if d["type"] == "monitoring_snapshot")
    assert mon["monitoring"] == {"uptime": 0.8, "unplanned_restarts": 2}
    assert mon["device_did"] == "did:swarm:ems-server:pod:optimusddc:optimusdb1-7f9c6d5b8-x2k9p"


def test_restarts_over_slo(tmp_path):
    s = make_sink(tmp_path, max_unplanned_restarts=2)
    for i, rc in enumerate([5, 6, 9]):            # baseline 5 -> +4 restarts in window
        run(s.send([pod("pod_restarts", rc, T0 + i * 60_000)]))
    a3 = by_id(s.evaluate(T0 + 5 * 60_000), ":A3")
    assert a3["value"] == 4 and a3["slo_met"] is False and a3["score"] == 0.0


def test_node_and_control_plane(tmp_path):
    s = make_sink(tmp_path, cluster="swarm-01", did_map={"epm-server": "did:swarm:capacity-vm-01"})
    for i, v in enumerate([1, 0, 1, 1]):
        run(s.send([node(v, T0 + i * 30_000),
                    Sample(metric=P + "apiserver_readyz", value=1, instance="apiserver", timestamp_ms=T0 + i * 30_000)]))
    docs = s.evaluate(T0 + 5 * 60_000)
    n1 = by_id(docs, "trust:swarm-01:node:epm-server:A1")
    assert n1["value"] == 0.75 and n1["subject_did"] == "did:swarm:capacity-vm-01"
    assert by_id(docs, "trust:swarm-01:node:epm-server:A3")["value"] == 1      # one ready->not-ready transition
    cp = by_id(docs, "trust:swarm-01:control-plane:kube-apiserver:A1")
    assert cp["value"] == 1.0 and cp["slo_met"] is True


def test_warning_events_as_evidence(tmp_path):
    s = make_sink(tmp_path)
    name = "optimusdb2-6b7d8c9f4-xz2q4"
    run(s.send([pod("pod_restarts", 1, T0, name=name),
                Sample(metric=P + "k8s_event", value=3, timestamp_ms=T0 + 1000, instance=name,
                       labels={"namespace": "optimusddc", "reason": "BackOff", "kind": "Pod", "object": name}),
                Sample(metric=P + "k8s_event", value=1, timestamp_ms=T0 + 1000, instance=name,
                       labels={"namespace": "optimusddc", "reason": "Scheduled", "kind": "Pod", "object": name})]))
    a3 = by_id(s.evaluate(T0 + 60_000), ":A3")
    assert a3["evidence"]["warning_events"] == 3 and a3["evidence"]["recent_warning_reasons"] == ["BackOff"]


def test_filters(tmp_path):
    s = make_sink(tmp_path, namespaces=["optimusddc"], exclude_pod_regex="^job-")
    run(s.send([pod("pod_running", 1, T0, ns="other"), pod("pod_running", 1, T0, name="job-abc12"),
                pod("pod_running", 1, T0)]))
    assert len(s.subjects) == 1


def test_flush_writes_crudput_and_hourly_history(tmp_path):
    s = make_sink(tmp_path, dstype="kbtrust", max_docs_per_request=2)
    rec = Recorder()
    s._http = httpx.AsyncClient(transport=httpx.MockTransport(rec))
    run(s.send([pod("pod_running", 1, T0), pod("pod_restarts", 0, T0), pod("pod_restarts", 0, T0 + 1)]))
    n = run(s.flush(T0 + 60_000))
    assert rec.calls[0]["url"] == "http://optimus.test/optimusdb1/swarmkb/command"
    body = rec.calls[0]["body"]
    assert body["method"] == {"cmd": "crudput", "argcnt": 1} and body["dstype"] == "kbtrust"
    assert len(body["criteria"]) <= 2 and n == len(rec.docs)
    types = sorted(d["type"] for d in rec.docs)
    assert types == ["monitoring_snapshot", "trust_evidence", "trust_evidence",
                     "trust_evidence_history", "trust_evidence_history"]
    hist = next(d for d in rec.docs if d["type"] == "trust_evidence_history")
    assert hist["_id"].startswith(hist["latest_id"] + ":") and len(hist["_id"].rsplit(":", 1)[1]) == 11
    rec.calls.clear()
    run(s.flush(T0 + 120_000))                  # same hour -> no new history copies
    assert "trust_evidence_history" not in {d["type"] for d in rec.docs}
    assert s.status()["sent"] == n + len(rec.docs) and s.status()["errors"] == 0


def test_flush_error_is_reported_and_history_retried(tmp_path):
    s = make_sink(tmp_path)
    s._http = httpx.AsyncClient(transport=httpx.MockTransport(Recorder(status=503)))
    run(s.send([pod("pod_running", 1, T0)]))
    assert run(s.flush(T0 + 60_000)) == 0
    st = s.status()
    assert st["errors"] == 1 and "503" in st["last_error"] and st["connected"] is False
    rec = Recorder()
    s._http = httpx.AsyncClient(transport=httpx.MockTransport(rec))
    run(s.flush(T0 + 120_000))
    assert any(d["type"] == "trust_evidence_history" for d in rec.docs)


def test_state_survives_restart(tmp_path):
    s = make_sink(tmp_path, dry_run=True)
    for i, up in enumerate([1, 1, 0, 1]):
        run(s.send([pod("pod_running", up, T0 + i * 60_000)]))
    run(s.flush(T0 + 5 * 60_000))
    s2 = make_sink(tmp_path, dry_run=True)
    assert by_id(s2.evaluate(T0 + 6 * 60_000), ":A1")["value"] == 0.75


def test_window_expiry(tmp_path):
    s = make_sink(tmp_path, availability_window="2h", stability_window="2h")
    run(s.send([pod("pod_running", 0, T0)]))
    run(s.send([pod("pod_running", 1, T0 + 2 * HOUR_MS)]))
    a1 = by_id(s.evaluate(T0 + 2 * HOUR_MS + 60_000), ":A1")
    assert a1["value"] == 1.0 and a1["evidence"]["checks_total"] == 1
    assert s.evaluate(T0 + 10 * HOUR_MS) == []           # gone for a whole window -> forgotten


@pytest.mark.parametrize("pod_name,wl", [("optimusdb1-7f9c6d5b8-x2k9p", "optimusdb1"), ("redis-0", "redis"),
                                         ("node-exporter-x7k2p", "node-exporter"), ("plain", "plain")])
def test_workload_of(pod_name, wl):
    assert workload_of(pod_name) == wl


# ------------------------------------------------------------------ end-to-end through the engine + UI API
def test_engine_to_trust_api(tmp_path, monkeypatch):
    import yaml
    from fastapi.testclient import TestClient

    from swch_metrics.client.gateway import GatewayClient
    from swch_metrics.client.server import create_app
    from tests.test_client import handler

    monkeypatch.setattr(GatewayClient, "transport", httpx.MockTransport(handler))
    raw = copy.deepcopy(C.DEFAULT_CONFIG)
    raw["persist"] = {"mode": "file", "path": str(tmp_path / "data.yaml")}
    raw["sinks"] = [{"id": "trust", "type": "optimusdb_trust",
                     "options": {"dry_run": True, "state_path": str(tmp_path / "t.json"), "flush_interval": "1h"}}]
    raw["collectors"] = [{"id": "pods", "type": "pod_overview", "interval": "1h", "metrics": ["pod_running", "pod_restarts"]},
                         {"id": "topology", "type": "topology", "interval": "1h"},
                         {"id": "api", "type": "apiserver_health", "interval": "1h"}]
    base = tmp_path / "base.yaml"
    base.write_text(yaml.safe_dump(raw))
    store = C.ConfigStore(str(base))
    store.load()
    with TestClient(create_app(store)) as c:
        for cid in ("topology", "pods", "api", "pods"):
            assert c.post(f"/api/collectors/{cid}/run").status_code == 200
        docs = c.get("/api/trust", params={"flush": True}).json()
        ids = {d["_id"] for d in docs}
        # the mock topology has no cluster_name -> the gateway id ("default") names the cluster
        assert "trust:default:pod:default/web-1:A1" in ids and "trust:default:pod:default/web-1:A3" in ids
        assert "trust:default:node:n2:A1" in ids and "trust:default:control-plane:kube-apiserver:A1" in ids
        assert all(d["cluster_uid"] == "cid" for d in docs)
        bad = c.get("/api/trust", params={"violations": True, "attribute": "a1"}).json()
        assert {d["subject_name"] for d in bad} == {"n2", "kube-apiserver"}     # n2 NotReady, readyz failing
        st = next(s for s in c.get("/api/status").json()["sinks"] if s["id"] == "trust")
        assert st["clusters"] == ["default"] and st["subjects"] == 4
