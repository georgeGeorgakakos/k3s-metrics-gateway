"""Shared fakes: an in-memory Kubernetes API server and gateway responses."""
from __future__ import annotations

import json
from typing import Any

import pytest
from fastapi import HTTPException

CADVISOR = """# HELP container_cpu_cfs_throttled_periods_total x
# TYPE container_cpu_cfs_throttled_periods_total counter
container_cpu_cfs_throttled_periods_total{container="web",namespace="default",pod="web-1"} 42
container_cpu_cfs_throttled_periods_total{container="side",namespace="default",pod="web-1"} 8
container_cpu_cfs_throttled_periods_total{container="x",namespace="kube-system",pod="x-1"} 1
"""

NODE = {"metadata": {"name": "n1", "uid": "u-n1", "labels": {
            "node-role.kubernetes.io/control-plane": "true",
            "topology.kubernetes.io/zone": "eu-west-1a", "topology.kubernetes.io/region": "eu-west-1"}},
        "spec": {"providerID": "aws:///eu-west-1a/i-123", "podCIDR": "10.42.0.0/24"},
        "status": {"allocatable": {"cpu": "4", "memory": "8Gi"}, "capacity": {"cpu": "4", "memory": "8Gi"},
                   "nodeInfo": {"kubeletVersion": "v1.33.4+k3s1", "osImage": "Ubuntu", "architecture": "amd64",
                                "containerRuntimeVersion": "containerd://2"},
                   "conditions": [{"type": "Ready", "status": "True"}],
                   "addresses": [{"type": "InternalIP", "address": "10.0.0.5"}]}}

BASE: dict[str, Any] = {
    "/api/v1/nodes": {"items": [NODE]},
    "/api/v1/namespaces/kube-system": {"metadata": {"uid": "cluster-uid-1"}},
    "/version": {"gitVersion": "v1.33.4+k3s1"},
    "/apis/metrics.k8s.io/v1beta1": {"kind": "APIResourceList"},
    "/apis/metrics.k8s.io/v1beta1/pods": {"items": [{"metadata": {"name": "web-1", "namespace": "default"},
        "timestamp": "t", "window": "15s", "containers": [{"name": "web", "usage": {"cpu": "250000000n", "memory": "128Mi"}}]}]},
    "/apis/metrics.k8s.io/v1beta1/nodes": {"items": [{"metadata": {"name": "n1"}, "usage": {"cpu": "2", "memory": "4Gi"}}]},
    "/api/v1/pods": {"items": [{"metadata": {"name": "web-1", "namespace": "default"},
        "spec": {"nodeName": "n1", "containers": [{"name": "web", "image": "nginx", "resources": {
            "requests": {"cpu": "500m", "memory": "256Mi"}, "limits": {"cpu": "1", "memory": "512Mi"}}}]},
        "status": {"phase": "Running", "qosClass": "Burstable", "containerStatuses": [{"name": "web", "ready": True,
            "restartCount": 3, "state": {"running": {}}, "lastState": {"terminated": {"reason": "OOMKilled"}}}]}}]},
    "/api/v1/nodes/n1/proxy/stats/summary": {"pods": [{"podRef": {"name": "web-1", "namespace": "default", "uid": "u"},
        "cpu": {"usageNanoCores": 250000000, "usageCoreNanoSeconds": 5e9}, "memory": {"workingSetBytes": 134217728},
        "network": {"rxBytes": 10, "txBytes": 20},
        "volume": [{"name": "data", "usedBytes": 1048576, "capacityBytes": 2097152}],
        "containers": [{"name": "web", "cpu": {"usageNanoCores": 250000000}, "memory": {"workingSetBytes": 134217728}}]}]},
    "/api/v1/events": {"items": [{"metadata": {"namespace": "default"}, "type": "Warning", "reason": "BackOff",
        "involvedObject": {"kind": "Pod", "name": "web-1"}, "message": "m", "count": 2,
        "lastTimestamp": "2026-11-14T00:00:00Z"}]},
}


class Resp:
    def __init__(self, status: int, body: Any = None, text: str | None = None):
        self.status_code, self._body = status, body
        self.text = text if text is not None else json.dumps(body)

    def json(self):
        return self._body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class FakeK8s:
    """In-memory API server supporting GET/PUT/POST/PATCH on a path dict."""

    host = "https://10.0.0.5:6443"

    def __init__(self, extra: dict | None = None):
        self.objects: dict[str, Any] = {**BASE, **(extra or {})}
        self.calls: list[tuple[str, str, Any, str | None]] = []

    def _text(self, path: str) -> str | None:
        if "cadvisor" in path or path.endswith("/proxy/metrics") or path == "/metrics" \
                or "metrics/resource" in path or "metrics/probes" in path:
            return CADVISOR
        if path in ("/livez", "/readyz"):
            return "[+]ping ok\nlivez check passed"
        return None

    def request(self, method, path, *, params=None, json=None, content_type=None):
        self.calls.append((method, path, json, content_type))
        if method == "GET":
            t = self._text(path)
            if t is not None:
                return Resp(200, None, t)
            if path in self.objects:
                return Resp(200, self.objects[path])
            return Resp(404, {"reason": "NotFound"})
        if method == "PUT":
            if path not in self.objects:
                return Resp(404, {"reason": "NotFound"})
            self.objects[path] = json
            return Resp(200, json)
        if method == "POST":
            name = json["metadata"]["name"]
            self.objects[f"{path}/{name}"] = json
            return Resp(201, json)
        if method == "PATCH":
            if path not in self.objects:
                return Resp(404, {"reason": "NotFound"})
            obj = self.objects[path]
            if "data" in (json or {}):
                obj.setdefault("data", {}).update(json["data"])
            else:
                spec = json["spec"]["template"]["spec"]
                tpl = obj["spec"]["template"]["spec"]
                names = {c["name"] for c in tpl["containers"]}
                tpl["containers"] += [c for c in spec["containers"] if c["name"] not in names]
                tpl.setdefault("volumes", []).extend(spec.get("volumes", []))
            return Resp(200, obj)
        raise AssertionError(method)

    def get(self, path, params=None, text=False):
        r = self.request("GET", path, params=params)
        if r.status_code >= 400:
            raise HTTPException(r.status_code, detail=path)
        return r.text if text else r.json()


@pytest.fixture
def fake_k8s():
    return FakeK8s()


@pytest.fixture(autouse=True)
def _env(monkeypatch, tmp_path):
    monkeypatch.setenv("POD_NAMESPACE", "swarm-system")
    monkeypatch.setenv("CONFIG_DATA_PATH", str(tmp_path / "data.yaml"))
    monkeypatch.setenv("CONFIG_SAVED_PATH", str(tmp_path / "saved.yaml"))
    for v in ("API_KEY", "SWARM_ID", "GATEWAY_URL"):
        monkeypatch.delenv(v, raising=False)
