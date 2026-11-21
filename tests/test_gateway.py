import pytest
import yaml
from fastapi.testclient import TestClient

from swch_metrics.gateway import app as A
from swch_metrics.gateway import attacher, topology


@pytest.fixture
def client(monkeypatch, fake_k8s):
    monkeypatch.setenv("TOPOLOGY_ENABLED", "false")
    monkeypatch.setenv("ATTACH_EMS_SIDECAR", "false")
    monkeypatch.setattr(A, "k8s", lambda: fake_k8s)
    monkeypatch.setattr(A, "API_KEY", None)
    with TestClient(A.app) as c:
        yield c


@pytest.mark.parametrize("url", [
    "/health", "/capabilities", "/pods/usage", "/pods/usage?sort=memory", "/nodes/usage", "/pods/stats",
    "/nodes/n1/summary", "/nodes/n1/cadvisor?metric=container_cpu_cfs&namespace=default",
    "/cadvisor?metric=container_cpu", "/nodes/n1/resource", "/nodes/n1/probes", "/nodes/n1/kubelet",
    "/pods/status", "/pods/overview", "/events", "/nodes", "/apiserver/metrics?metric=container",
    "/apiserver/health", "/raw?path=/api/v1/pods", "/topology", "/endpoints", "/status", "/openapi.json",
])
def test_endpoints_ok(client, url):
    assert client.get(url).status_code == 200, url


def test_values(client):
    pod = client.get("/pods/usage").json()[0]
    assert pod["cpu_millicores"] == 250.0 and pod["memory_mib"] == 128.0
    node = client.get("/nodes/usage").json()[0]
    assert node["cpu_pct"] == 50.0 and node["memory_pct"] == 50.0
    ov = client.get("/pods/overview").json()[0]
    assert ov["cpu_pct_of_request"] == 50.0 and ov["memory_pct_of_limit"] == 25.0 and ov["restarts"] == 3
    st = client.get("/pods/stats").json()[0]
    assert st["volumes"][0]["used_mib"] == 1.0 and st["network_rx_bytes_total"] == 10


def test_raw_rejects_paths(client):
    assert client.get("/raw?path=/etc/passwd").status_code == 400


def test_prometheus_not_configured(client):
    assert client.get("/prometheus/pods/cpu").status_code == 503


def test_api_key(client, monkeypatch):
    monkeypatch.setattr(A, "API_KEY", "s3cret")
    assert client.get("/pods/usage").status_code == 401
    assert client.get("/pods/usage", headers={"X-API-Key": "s3cret"}).status_code == 200
    assert client.get("/health").status_code == 200  # always open for probes


def test_client_config_roundtrip(client, fake_k8s):
    base = "/api/v1/namespaces/swarm-system/configmaps"
    fake_k8s.objects[f"{base}/swch-metrics-client-config"] = {"data": {"client.yaml": "collectors: []\n"}}
    r = client.get("/client-config/swarm-system").json()
    assert r["source"] == "swch-metrics-client-config"
    assert client.put("/client-config/swarm-system", json={"client.yaml": "a: ["}).status_code == 422
    r = client.put("/client-config/swarm-system", json={"client.yaml": "default_interval: 10s\n"})
    assert r.status_code == 200 and r.json()["configmap"] == "swch-metrics-client-config-saved"
    assert fake_k8s.objects[f"{base}/swch-metrics-client-config-saved"]["data"]["client.yaml"].startswith("default_interval")
    # defaults ConfigMap (owned by the k3s manifest) untouched
    assert fake_k8s.objects[f"{base}/swch-metrics-client-config"]["data"]["client.yaml"] == "collectors: []\n"
    assert client.get("/client-config/swarm-system").json()["source"] == "swch-metrics-client-config-saved"


# ---------------------------------------------------------------- topology
def test_build_topology(fake_k8s, monkeypatch):
    monkeypatch.setenv("SWARM_ID", "swarm-42")
    doc = topology.build_topology(fake_k8s, {"metrics.k8s.io": True, "kubelet (stats/cadvisor via nodes/proxy)": True})
    assert doc["swarm_id"] == "swarm-42" and doc["cluster_id"] == "cluster-uid-1"
    assert doc["distribution"] == "k3s"
    n = doc["nodes"][0]
    assert n["zone"] == "eu-west-1a" and n["cloud"] == "aws" and n["ready"] and "control-plane" in n["roles"]
    assert doc["summary"] == {"nodes": 1, "ready": 1, "control_plane": 1, "zones": ["eu-west-1a"],
                              "regions": ["eu-west-1"], "clouds": ["aws"]}
    eps = {e["id"]: e for e in doc["endpoints"]}
    assert eps["pods_usage"]["available"] is True and eps["prometheus_query"]["available"] is False
    assert eps["pods_usage"]["url"].endswith("svc.cluster.local:8000/pods/usage")


def test_topology_hash_ignores_timestamp(fake_k8s):
    a = topology.build_topology(fake_k8s, {})
    b = topology.build_topology(fake_k8s, {})
    assert a["hash"] == b["hash"]
    fake_k8s.objects["/api/v1/nodes"]["items"][0]["status"]["conditions"][0]["status"] = "False"
    assert topology.build_topology(fake_k8s, {})["hash"] != a["hash"]


def test_publish_configmap_create_then_update(fake_k8s):
    doc = topology.build_topology(fake_k8s, {})
    assert topology.publish_configmap(fake_k8s, doc) == "created"
    assert topology.publish_configmap(fake_k8s, doc) == "updated"
    cm = fake_k8s.objects["/api/v1/namespaces/swarm-system/configmaps/swch-metrics-topology"]
    assert cm["data"]["CLUSTER_ID"] == "cluster-uid-1" and "topology.json" in cm["data"]


def test_publisher_only_writes_on_change(fake_k8s):
    p = topology.TopologyPublisher(lambda: fake_k8s, lambda: {})
    p.run_once()
    writes = [c for c in fake_k8s.calls if c[0] in ("PUT", "POST")]
    p.run_once()
    assert len([c for c in fake_k8s.calls if c[0] in ("PUT", "POST")]) == len(writes)


# ---------------------------------------------------------------- sidecar attacher
SIDECAR = {"container": {"name": "x", "image": "img", "command": ["swch-metrics-client"]},
           "volumes": [{"name": "swch-client-data", "emptyDir": {}}]}


def _ems(fake_k8s, ns="swarm-system", extra_containers=()):
    fake_k8s.objects[f"/apis/apps/v1/namespaces/{ns}/deployments/emsserver-ems-server"] = {
        "spec": {"template": {"spec": {"containers": [{"name": "ems-server"}, *extra_containers]}}}}
    fake_k8s.objects["/api/v1/namespaces/swarm-system/configmaps/swch-ems-sidecar-template"] = {
        "data": {"sidecar.yaml": yaml.safe_dump(SIDECAR)}}
    fake_k8s.objects["/api/v1/namespaces/swarm-system/configmaps/swch-metrics-client-config"] = {
        "metadata": {"labels": {}}, "data": {"client.yaml": "x: 1"}}


def test_attacher_noop_without_ems(fake_k8s):
    assert attacher.attach_once(fake_k8s) == []


def test_attacher_patches_once(fake_k8s):
    _ems(fake_k8s)
    r = attacher.attach_once(fake_k8s)
    assert r == [{"namespace": "swarm-system", "deployment": "emsserver-ems-server", "status": "attached"}]
    patch = [c for c in fake_k8s.calls if c[0] == "PATCH"][0]
    assert patch[3] == "application/strategic-merge-patch+json"
    assert patch[2]["spec"]["template"]["spec"]["containers"][0]["name"] == "swch-metrics-client"
    assert attacher.attach_once(fake_k8s)[0]["status"] == "present"
    assert len([c for c in fake_k8s.calls if c[0] == "PATCH"]) == 1


def test_attacher_copies_config_to_other_namespace(fake_k8s, monkeypatch):
    monkeypatch.setenv("EMS_NAMESPACES", "default")
    _ems(fake_k8s, ns="default")
    assert attacher.attach_once(fake_k8s)[0]["status"] == "attached"
    assert "/api/v1/namespaces/default/configmaps/swch-metrics-client-config" in fake_k8s.objects


def test_behind_path_prefix(monkeypatch, fake_k8s):
    """Traefik strips /swch-metrics; the app gets root_path so docs and auth still work."""
    monkeypatch.setenv("TOPOLOGY_ENABLED", "false")
    monkeypatch.setenv("ATTACH_EMS_SIDECAR", "false")
    monkeypatch.setattr(A, "k8s", lambda: fake_k8s)
    monkeypatch.setattr(A, "API_KEY", "s3cret")
    with TestClient(A.app, root_path="/swch-metrics") as c:
        assert c.get("/health").status_code == 200
        assert "/swch-metrics/openapi.json" in c.get("/docs").text
        assert c.get("/openapi.json").status_code == 200
        assert c.get("/pods/usage").status_code == 401
        assert c.get("/pods/usage", headers={"X-API-Key": "s3cret"}).status_code == 200
