"""Cluster topology discovery and publication for EMS.

The gateway builds a topology document (cluster identity, nodes, zones,
allocatable resources and the catalog of metric endpoints with their
availability) and publishes it as the ConfigMap ``swch-metrics-topology`` so
that EMS and its sidecar know *what* the cluster looks like and *where* to
fetch metrics from.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
from typing import Any

from .k8s import K8s, own_namespace

log = logging.getLogger("swch.gateway.topology")

TOPOLOGY_CONFIGMAP = os.getenv("TOPOLOGY_CONFIGMAP", "swch-metrics-topology")
LABELS = {
    "app.kubernetes.io/part-of": "swarmchestrate",
    "app.kubernetes.io/managed-by": "swch-metrics-gateway",
    "swarmchestrate.eu/component": "metrics-topology",
}

# Catalog of every endpoint exposed by the gateway. ``source`` is matched
# against the capability probe so consumers know what really works here.
ENDPOINTS: list[dict[str, str]] = [
    {"id": "pods_usage", "path": "/pods/usage", "source": "metrics.k8s.io",
     "description": "CPU (millicores) and memory (MiB) per pod and container"},
    {"id": "pod_usage", "path": "/pods/{namespace}/{pod}/usage", "source": "metrics.k8s.io",
     "description": "CPU/memory for a single pod"},
    {"id": "nodes_usage", "path": "/nodes/usage", "source": "metrics.k8s.io",
     "description": "CPU/memory per node with % of allocatable"},
    {"id": "pods_stats", "path": "/pods/stats", "source": "kubelet",
     "description": "Per-pod CPU, memory, network, ephemeral storage and volumes"},
    {"id": "node_summary", "path": "/nodes/{node}/summary", "source": "kubelet",
     "description": "Raw kubelet /stats/summary"},
    {"id": "cadvisor", "path": "/cadvisor?metric={prefix}", "source": "kubelet",
     "description": "cAdvisor container metrics across all nodes (throttling, I/O, network)"},
    {"id": "node_cadvisor", "path": "/nodes/{node}/cadvisor", "source": "kubelet",
     "description": "cAdvisor metrics of one node"},
    {"id": "node_resource", "path": "/nodes/{node}/resource", "source": "kubelet",
     "description": "Kubelet /metrics/resource"},
    {"id": "node_probes", "path": "/nodes/{node}/probes", "source": "kubelet",
     "description": "Liveness/readiness/startup probe results"},
    {"id": "node_kubelet", "path": "/nodes/{node}/kubelet", "source": "kubelet",
     "description": "Kubelet operational metrics"},
    {"id": "pods_status", "path": "/pods/status", "source": "core",
     "description": "Phase, node, QoS, restarts, requests/limits"},
    {"id": "pods_overview", "path": "/pods/overview", "source": "core",
     "description": "Usage joined with requests/limits (% utilisation)"},
    {"id": "events", "path": "/events", "source": "core",
     "description": "Cluster events (OOMKilled, BackOff, FailedScheduling)"},
    {"id": "nodes", "path": "/nodes", "source": "core",
     "description": "Nodes with capacity, allocatable, conditions"},
    {"id": "apiserver_metrics", "path": "/apiserver/metrics", "source": "apiserver",
     "description": "API server Prometheus metrics"},
    {"id": "apiserver_health", "path": "/apiserver/health", "source": "apiserver",
     "description": "API server livez/readyz"},
    {"id": "custom_metrics", "path": "/custom-metrics/{path}", "source": "custom.metrics.k8s.io",
     "description": "Custom metrics adapter passthrough"},
    {"id": "external_metrics", "path": "/external-metrics/{path}", "source": "external.metrics.k8s.io",
     "description": "External metrics adapter passthrough"},
    {"id": "prometheus_query", "path": "/prometheus/query?q={promql}", "source": "prometheus",
     "description": "Instant PromQL query"},
    {"id": "prometheus_query_range", "path": "/prometheus/query_range", "source": "prometheus",
     "description": "Range PromQL query"},
    {"id": "prometheus_presets", "path": "/prometheus/pods/{preset}", "source": "prometheus",
     "description": "Ready-made pod PromQL presets"},
    {"id": "topology", "path": "/topology", "source": "core",
     "description": "This topology document"},
    {"id": "raw", "path": "/raw?path={api_path}", "source": "core",
     "description": "Any read-only API-server path"},
]


def gateway_url() -> str:
    if os.getenv("GATEWAY_URL"):
        return os.environ["GATEWAY_URL"].rstrip("/")
    ns = own_namespace()
    return f"http://swch-metrics-gateway.{ns}.svc.cluster.local:8000"


def _labels_subset(labels: dict[str, str]) -> dict[str, str]:
    keep = ("topology.kubernetes.io/", "node.kubernetes.io/instance-type", "kubernetes.io/arch",
            "swarmchestrate.eu/", "node-role.kubernetes.io/")
    return {k: v for k, v in labels.items() if k.startswith(keep)}


def _provider(node: dict) -> str | None:
    pid = node.get("spec", {}).get("providerID", "")
    if "://" in pid:
        return pid.split("://", 1)[0]
    lbl = node["metadata"].get("labels", {})
    return lbl.get("swarmchestrate.eu/cloud") or lbl.get("node.kubernetes.io/cloud")


def build_topology(k: K8s, capabilities: dict[str, Any] | None = None) -> dict[str, Any]:
    nodes_raw = k.get("/api/v1/nodes")["items"]
    try:
        cluster_id = k.get("/api/v1/namespaces/kube-system")["metadata"]["uid"]
    except Exception:  # noqa: BLE001
        cluster_id = None
    try:
        version = k.get("/version")
    except Exception:  # noqa: BLE001
        version = {}

    nodes = []
    for n in nodes_raw:
        md, st = n["metadata"], n.get("status", {})
        lbl = md.get("labels", {})
        addr = {a["type"]: a["address"] for a in st.get("addresses", [])}
        cond = {c["type"]: c["status"] for c in st.get("conditions", [])}
        nodes.append({
            "name": md["name"],
            "uid": md.get("uid"),
            "roles": sorted(k_.split("/")[-1] for k_ in lbl if k_.startswith("node-role.kubernetes.io/")) or ["worker"],
            "internal_ip": addr.get("InternalIP"),
            "external_ip": addr.get("ExternalIP") or lbl.get("k3s.io/external-ip"),
            "hostname": addr.get("Hostname"),
            "region": lbl.get("topology.kubernetes.io/region") or os.getenv("REGION") or None,
            "zone": lbl.get("topology.kubernetes.io/zone") or os.getenv("ZONE") or None,
            "cloud": _provider(n) or os.getenv("CLOUD") or None,
            "instance_type": lbl.get("node.kubernetes.io/instance-type"),
            "arch": st.get("nodeInfo", {}).get("architecture"),
            "os_image": st.get("nodeInfo", {}).get("osImage"),
            "kubelet_version": st.get("nodeInfo", {}).get("kubeletVersion"),
            "container_runtime": st.get("nodeInfo", {}).get("containerRuntimeVersion"),
            "pod_cidr": n.get("spec", {}).get("podCIDR"),
            "ready": cond.get("Ready") == "True",
            "conditions": cond,
            "unschedulable": bool(n.get("spec", {}).get("unschedulable")),
            "capacity": st.get("capacity", {}),
            "allocatable": st.get("allocatable", {}),
            "labels": _labels_subset(lbl),
            "metrics": {
                "kubelet_summary": f"/nodes/{md['name']}/summary",
                "cadvisor": f"/nodes/{md['name']}/cadvisor",
                "probes": f"/nodes/{md['name']}/probes",
            },
        })

    caps = capabilities or {}
    src_ok = {
        "metrics.k8s.io": caps.get("metrics.k8s.io") is True,
        "custom.metrics.k8s.io": caps.get("custom.metrics.k8s.io") is True,
        "external.metrics.k8s.io": caps.get("external.metrics.k8s.io") is True,
        "apiserver": caps.get("apiserver /metrics") is True,
        "kubelet": caps.get("kubelet (stats/cadvisor via nodes/proxy)") is True,
        "prometheus": caps.get("prometheus") is True,
        "core": True,
    }
    base = gateway_url()
    endpoints = [dict(e, url=base + e["path"], available=src_ok.get(e["source"], False) if caps else None)
                 for e in ENDPOINTS]

    doc = {
        "schema": "swarmchestrate.eu/metrics-topology/v1",
        "swarm_id": os.getenv("SWARM_ID") or None,
        "cluster_id": cluster_id,
        "cluster_name": os.getenv("CLUSTER_NAME") or None,
        "distribution": "k3s" if "k3s" in str(version.get("gitVersion", "")) else "kubernetes",
        "kubernetes_version": version.get("gitVersion"),
        "api_server": k.host,
        "gateway": {
            "url": base,
            "openapi": base + "/openapi.json",
            "auth": "X-API-Key" if os.getenv("API_KEY") else "none",
            "namespace": own_namespace(),
        },
        "summary": {
            "nodes": len(nodes),
            "ready": sum(1 for n in nodes if n["ready"]),
            "control_plane": sum(1 for n in nodes if "control-plane" in n["roles"] or "master" in n["roles"]),
            "zones": sorted({n["zone"] for n in nodes if n["zone"]}),
            "regions": sorted({n["region"] for n in nodes if n["region"]}),
            "clouds": sorted({n["cloud"] for n in nodes if n["cloud"]}),
        },
        "capabilities": caps,
        "nodes": nodes,
        "endpoints": endpoints,
    }
    doc["hash"] = topology_hash(doc)
    doc["updated_at"] = datetime.now(timezone.utc).isoformat()
    return doc


def topology_hash(doc: dict) -> str:
    stable = {k: v for k, v in doc.items() if k not in ("updated_at", "hash")}
    return hashlib.sha256(json.dumps(stable, sort_keys=True, default=str).encode()).hexdigest()[:16]


def publish_configmap(k: K8s, doc: dict, namespace: str | None = None) -> str:
    """Create or replace the topology ConfigMap. Returns 'created' | 'updated'."""
    ns = namespace or own_namespace()
    body = {
        "apiVersion": "v1", "kind": "ConfigMap",
        "metadata": {"name": TOPOLOGY_CONFIGMAP, "namespace": ns, "labels": LABELS,
                     "annotations": {"swarmchestrate.eu/topology-hash": doc["hash"]}},
        "data": {
            "topology.json": json.dumps(doc, indent=2, default=str),
            "endpoints.json": json.dumps(doc["endpoints"], indent=2),
            "GATEWAY_URL": doc["gateway"]["url"],
            "CLUSTER_ID": doc.get("cluster_id") or "",
            "SWARM_ID": doc.get("swarm_id") or "",
            "UPDATED_AT": doc["updated_at"],
        },
    }
    path = f"/api/v1/namespaces/{ns}/configmaps/{TOPOLOGY_CONFIGMAP}"
    r = k.request("PUT", path, json=body)
    if r.status_code == 404:
        r = k.request("POST", f"/api/v1/namespaces/{ns}/configmaps", json=body)
        r.raise_for_status()
        return "created"
    r.raise_for_status()
    return "updated"


class TopologyPublisher(threading.Thread):
    """Background loop: rebuild topology, publish the ConfigMap when it changes."""

    def __init__(self, k_factory, caps_fn, interval: float = 60.0) -> None:
        super().__init__(daemon=True, name="topology-publisher")
        self.k_factory, self.caps_fn, self.interval = k_factory, caps_fn, interval
        self.last: dict | None = None
        self.last_hash: str | None = None
        self.last_error: str | None = None
        self._stop = threading.Event()

    def run_once(self) -> dict:
        k = self.k_factory()
        doc = build_topology(k, self.caps_fn())
        if doc["hash"] != self.last_hash:
            action = publish_configmap(k, doc)
            log.info("topology %s (hash=%s, nodes=%d)", action, doc["hash"], doc["summary"]["nodes"])
            self.last_hash = doc["hash"]
        self.last = doc
        return doc

    def run(self) -> None:
        while not self._stop.is_set():
            try:
                self.run_once()
                self.last_error = None
            except Exception as e:  # noqa: BLE001
                self.last_error = f"{type(e).__name__}: {e}"
                log.warning("topology publish failed: %s", self.last_error)
            self._stop.wait(self.interval)

    def stop(self) -> None:
        self._stop.set()


