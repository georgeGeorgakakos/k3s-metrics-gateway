"""
Swarmchestrate k3s Metrics Gateway
==================================
Deployed once per Swarmchestrate-orchestrated k3s cluster. Exposes every
metrics source of the cluster through one REST API and publishes the cluster
topology (ConfigMap ``swch-metrics-topology``) so that EMS knows the topology
and where to fetch metrics from.

Interactive docs: http://<host>:8000/docs   (OpenAPI JSON: /openapi.json)

Environment:
  KUBECONFIG            kubeconfig when running outside the cluster
  API_KEY               if set, clients must send header  X-API-Key: <value>
  PROMETHEUS_URL        optional Prometheus for history/PromQL
  GATEWAY_URL           URL advertised in the topology (default: in-cluster service DNS)
  SWARM_ID, CLUSTER_NAME, CLOUD, REGION, ZONE   identity advertised to EMS
  TOPOLOGY_ENABLED      publish topology ConfigMap (default true)
  TOPOLOGY_INTERVAL     seconds between topology refreshes (default 60)
  ATTACH_EMS_SIDECAR    attach the metrics-client sidecar to EMS (default true)
  EMS_NAMESPACES        namespaces to look for EMS in (default swarm-system,default)
  EMS_DEPLOYMENT        EMS Deployment name (default emsserver-ems-server)
"""
from __future__ import annotations

import logging
import os
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from typing import Any, Optional

import requests
import yaml
from fastapi import Body, Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import PlainTextResponse
from prometheus_client.parser import text_string_to_metric_families

from .. import __version__
from . import attacher as _attacher
from . import topology as _topology
from .k8s import TIMEOUT, k8s, own_namespace

PROM_URL = os.getenv("PROMETHEUS_URL", "").rstrip("/")
API_KEY = os.getenv("API_KEY") or None
log = logging.getLogger("swch.gateway")

_bg: dict[str, Any] = {}


def _truthy(name: str, default: str = "true") -> bool:
    return os.getenv(name, default).lower() in ("1", "true", "yes", "on")


@asynccontextmanager
async def lifespan(_app: FastAPI):
    if _truthy("TOPOLOGY_ENABLED"):
        t = _topology.TopologyPublisher(lambda: k8s(), lambda: capabilities(),
                                        float(os.getenv("TOPOLOGY_INTERVAL", "60")))
        t.start()
        _bg["topology"] = t
    if _truthy("ATTACH_EMS_SIDECAR"):
        a = _attacher.SidecarAttacher(lambda: k8s(), float(os.getenv("ATTACH_INTERVAL", "30")))
        a.start()
        _bg["attacher"] = a
    yield
    for th in _bg.values():
        th.stop()


OPEN_PATHS = {"/health", "/docs", "/openapi.json", "/docs/oauth2-redirect", "/redoc"}


def require_key(request: Request, x_api_key: Optional[str] = Header(default=None)):
    if request.url.path in OPEN_PATHS:
        return
    if API_KEY and x_api_key != API_KEY:
        raise HTTPException(401, "invalid or missing X-API-Key")


app = FastAPI(
    title="Swarmchestrate k3s Metrics Gateway",
    version=__version__,
    description="Unified REST access to all pod/node/cluster metrics and topology of a Swarmchestrate k3s cluster.",
    dependencies=[Depends(require_key)],
    lifespan=lifespan,
)


# --------------------------------------------------------------------------- #
# Helpers                                                                      #
# --------------------------------------------------------------------------- #
def cpu_m(q: str | None) -> float | None:
    if q is None:
        return None
    q = str(q)
    for suf, div in (("n", 1e6), ("u", 1e3)):
        if q.endswith(suf):
            return round(float(q[:-1]) / div, 3)
    if q.endswith("m"):
        return float(q[:-1])
    return float(q) * 1000


_MEM = {"Ki": 2**10, "Mi": 2**20, "Gi": 2**30, "Ti": 2**40, "Pi": 2**50, "Ei": 2**60,
        "k": 1e3, "K": 1e3, "M": 1e6, "G": 1e9, "T": 1e12, "P": 1e15, "E": 1e18}


def mem_b(q: str | None) -> int | None:
    if q is None:
        return None
    q = str(q)
    for suf in sorted(_MEM, key=len, reverse=True):
        if q.endswith(suf):
            return int(float(q[: -len(suf)]) * _MEM[suf])
    return int(float(q))


def mib(b: int | float | None) -> float | None:
    return None if b is None else round(b / 2**20, 2)


def ns_path(base: str, namespace: str | None, kind: str) -> str:
    return f"{base}/namespaces/{namespace}/{kind}" if namespace else f"{base}/{kind}"


def parse_prom(text: str, metric: str | None, namespace: str | None, pod: str | None,
               limit: int) -> list[dict]:
    out: list[dict] = []
    for fam in text_string_to_metric_families(text):
        if metric and not fam.name.startswith(metric):
            continue
        for s in fam.samples:
            if metric and not s.name.startswith(metric):
                continue
            lbl = s.labels
            if namespace and lbl.get("namespace") != namespace:
                continue
            if pod and lbl.get("pod") != pod:
                continue
            out.append({"name": s.name, "labels": lbl, "value": s.value,
                        "timestamp": s.timestamp, "type": fam.type})
            if len(out) >= limit:
                return out
    return out


def node_names() -> list[str]:
    return [n["metadata"]["name"] for n in k8s().get("/api/v1/nodes")["items"]]


def node_metrics(node: str, kind: str, fmt: str, metric, namespace, pod, limit):
    text = k8s().get(f"/api/v1/nodes/{node}/proxy/{kind}", text=True)
    if fmt == "text":
        return PlainTextResponse(text)
    return {"node": node, "source": kind,
            "samples": parse_prom(text, metric, namespace, pod, limit)}


# --------------------------------------------------------------------------- #
# Meta                                                                         #
# --------------------------------------------------------------------------- #
@app.get("/health", tags=["meta"])
def health():
    return {"status": "ok", "version": __version__}


@app.get("/capabilities", tags=["meta"], summary="Which metric sources are reachable right now")
def capabilities():
    checks = {
        "metrics.k8s.io": "/apis/metrics.k8s.io/v1beta1",
        "custom.metrics.k8s.io": "/apis/custom.metrics.k8s.io/v1beta1",
        "external.metrics.k8s.io": "/apis/external.metrics.k8s.io/v1beta1",
        "apiserver /metrics": "/metrics",
    }
    res: dict[str, Any] = {}
    for name, path in checks.items():
        try:
            k8s().get(path, text=True)
            res[name] = True
        except HTTPException as e:
            res[name] = f"unavailable ({e.status_code})"
        except Exception as e:  # noqa: BLE001
            res[name] = f"unavailable ({type(e).__name__})"
    try:
        n = node_names()[0]
        k8s().get(f"/api/v1/nodes/{n}/proxy/stats/summary")
        res["kubelet (stats/cadvisor via nodes/proxy)"] = True
    except Exception as e:  # noqa: BLE001
        res["kubelet (stats/cadvisor via nodes/proxy)"] = f"unavailable ({getattr(e, 'status_code', type(e).__name__)})"
    if PROM_URL:
        try:
            requests.get(PROM_URL + "/-/ready", timeout=5).raise_for_status()
            res["prometheus"] = True
        except Exception as e:  # noqa: BLE001
            res["prometheus"] = f"unavailable ({type(e).__name__})"
    else:
        res["prometheus"] = "not configured (set PROMETHEUS_URL)"
    return res


# --------------------------------------------------------------------------- #
# metrics.k8s.io  (current CPU / memory, ~15s window)                          #
# --------------------------------------------------------------------------- #
def _shape_pod_usage(item: dict) -> dict:
    cs = [{"name": c["name"], "cpu_millicores": cpu_m(c["usage"]["cpu"]),
           "memory_mib": mib(mem_b(c["usage"]["memory"]))} for c in item["containers"]]
    return {"namespace": item["metadata"]["namespace"], "pod": item["metadata"]["name"],
            "timestamp": item.get("timestamp"), "window": item.get("window"),
            "cpu_millicores": round(sum(c["cpu_millicores"] for c in cs), 3),
            "memory_mib": round(sum(c["memory_mib"] for c in cs), 2), "containers": cs}


@app.get("/pods/usage", tags=["metrics.k8s.io"], summary="CPU/memory for all pods (kubectl top pods)")
def pods_usage(namespace: Optional[str] = None, sort: str = Query("cpu", pattern="^(cpu|memory|name)$"),
               raw: bool = False):
    data = k8s().get(ns_path("/apis/metrics.k8s.io/v1beta1", namespace, "pods"))
    if raw:
        return data
    rows = [_shape_pod_usage(i) for i in data["items"]]
    key = {"cpu": lambda r: -r["cpu_millicores"], "memory": lambda r: -r["memory_mib"],
           "name": lambda r: (r["namespace"], r["pod"])}[sort]
    return sorted(rows, key=key)


@app.get("/pods/{namespace}/{pod}/usage", tags=["metrics.k8s.io"], summary="CPU/memory for one pod")
def pod_usage(namespace: str, pod: str, raw: bool = False):
    d = k8s().get(f"/apis/metrics.k8s.io/v1beta1/namespaces/{namespace}/pods/{pod}")
    return d if raw else _shape_pod_usage(d)


@app.get("/nodes/usage", tags=["metrics.k8s.io"], summary="CPU/memory per node (kubectl top nodes)")
def nodes_usage(raw: bool = False):
    usage = k8s().get("/apis/metrics.k8s.io/v1beta1/nodes")
    if raw:
        return usage
    nodes = {n["metadata"]["name"]: n for n in k8s().get("/api/v1/nodes")["items"]}
    out = []
    for i in usage["items"]:
        name = i["metadata"]["name"]
        alloc = nodes.get(name, {}).get("status", {}).get("allocatable", {})
        c, m = cpu_m(i["usage"]["cpu"]), mem_b(i["usage"]["memory"])
        ca, ma = cpu_m(alloc.get("cpu")), mem_b(alloc.get("memory"))
        out.append({"node": name, "timestamp": i.get("timestamp"),
                    "cpu_millicores": c, "cpu_allocatable_m": ca,
                    "cpu_pct": round(100 * c / ca, 1) if ca else None,
                    "memory_mib": mib(m), "memory_allocatable_mib": mib(ma),
                    "memory_pct": round(100 * m / ma, 1) if ma else None})
    return out


# --------------------------------------------------------------------------- #
# Kubelet Summary API  (CPU, memory, network, filesystem, volumes per pod)     #
# --------------------------------------------------------------------------- #
@app.get("/nodes/{node}/summary", tags=["kubelet"], summary="Raw kubelet /stats/summary for a node")
def node_summary(node: str):
    return k8s().get(f"/api/v1/nodes/{node}/proxy/stats/summary")


def _shape_pod_stats(node: str, p: dict) -> dict:
    ref = p["podRef"]
    net = p.get("network") or {}
    eph = p.get("ephemeral-storage") or {}
    return {
        "node": node, "namespace": ref["namespace"], "pod": ref["name"], "uid": ref.get("uid"),
        "start_time": p.get("startTime"),
        "cpu_millicores": round((p.get("cpu") or {}).get("usageNanoCores", 0) / 1e6, 3),
        "cpu_core_seconds_total": round((p.get("cpu") or {}).get("usageCoreNanoSeconds", 0) / 1e9, 3),
        "memory_working_set_mib": mib((p.get("memory") or {}).get("workingSetBytes")),
        "memory_rss_mib": mib((p.get("memory") or {}).get("rssBytes")),
        "network_rx_bytes_total": net.get("rxBytes"), "network_tx_bytes_total": net.get("txBytes"),
        "network_rx_errors": net.get("rxErrors"), "network_tx_errors": net.get("txErrors"),
        "ephemeral_storage_used_mib": mib(eph.get("usedBytes")),
        "volumes": [{"name": v.get("name"), "used_mib": mib(v.get("usedBytes")),
                     "capacity_mib": mib(v.get("capacityBytes")),
                     "pvc": (v.get("pvcRef") or {}).get("name")} for v in p.get("volume") or []],
        "containers": [{"name": c["name"], "start_time": c.get("startTime"),
                        "cpu_millicores": round((c.get("cpu") or {}).get("usageNanoCores", 0) / 1e6, 3),
                        "memory_working_set_mib": mib((c.get("memory") or {}).get("workingSetBytes")),
                        "rootfs_used_mib": mib((c.get("rootfs") or {}).get("usedBytes")),
                        "logs_used_mib": mib((c.get("logs") or {}).get("usedBytes"))}
                       for c in p.get("containers") or []],
    }


@app.get("/pods/stats", tags=["kubelet"],
         summary="Rich per-pod stats across all nodes: CPU, memory, network, disk, volumes")
def pods_stats(namespace: Optional[str] = None, pod: Optional[str] = None):
    def one(n):
        s = k8s().get(f"/api/v1/nodes/{n}/proxy/stats/summary")
        return [_shape_pod_stats(n, p) for p in s.get("pods", [])]

    with ThreadPoolExecutor(max_workers=8) as ex:
        rows = [r for lst in ex.map(one, node_names()) for r in lst]
    if namespace:
        rows = [r for r in rows if r["namespace"] == namespace]
    if pod:
        rows = [r for r in rows if r["pod"] == pod]
    return sorted(rows, key=lambda r: -r["cpu_millicores"])


# --------------------------------------------------------------------------- #
# Kubelet Prometheus endpoints (cAdvisor, resource, probes, kubelet)           #
# --------------------------------------------------------------------------- #
_fmt = Query("json", pattern="^(json|text)$", description="json = parsed samples, text = Prometheus exposition")


@app.get("/nodes/{node}/cadvisor", tags=["kubelet"],
         summary="cAdvisor container metrics (throttling, I/O, network, OOM ...)")
def cadvisor(node: str, metric: Optional[str] = Query(None, description="prefix, e.g. container_cpu_cfs_throttled"),
             namespace: Optional[str] = None, pod: Optional[str] = None, limit: int = 5000, format: str = _fmt):
    return node_metrics(node, "metrics/cadvisor", format, metric, namespace, pod, limit)


@app.get("/nodes/{node}/resource", tags=["kubelet"], summary="Kubelet /metrics/resource (lightweight CPU/mem)")
def resource(node: str, metric: Optional[str] = None, namespace: Optional[str] = None,
             pod: Optional[str] = None, limit: int = 5000, format: str = _fmt):
    return node_metrics(node, "metrics/resource", format, metric, namespace, pod, limit)


@app.get("/nodes/{node}/probes", tags=["kubelet"], summary="Liveness/readiness/startup probe results")
def probes(node: str, namespace: Optional[str] = None, pod: Optional[str] = None,
           limit: int = 5000, format: str = _fmt):
    return node_metrics(node, "metrics/probes", format, None, namespace, pod, limit)


@app.get("/nodes/{node}/kubelet", tags=["kubelet"], summary="Kubelet's own operational metrics")
def kubelet_metrics(node: str, metric: Optional[str] = None, limit: int = 5000, format: str = _fmt):
    return node_metrics(node, "metrics", format, metric, None, None, limit)


@app.get("/cadvisor", tags=["kubelet"], summary="cAdvisor metrics from ALL nodes, filtered")
def cadvisor_all(metric: str = Query(..., description="required prefix, e.g. container_memory_working_set_bytes"),
                 namespace: Optional[str] = None, pod: Optional[str] = None, limit: int = 5000):
    def one(n):
        t = k8s().get(f"/api/v1/nodes/{n}/proxy/metrics/cadvisor", text=True)
        return [dict(s, node=n) for s in parse_prom(t, metric, namespace, pod, limit)]

    with ThreadPoolExecutor(max_workers=8) as ex:
        return [s for lst in ex.map(one, node_names()) for s in lst][:limit]


# --------------------------------------------------------------------------- #
# Core API: pod status / requests & limits / restarts / events / nodes         #
# --------------------------------------------------------------------------- #
def _pod_status(p: dict) -> dict:
    spec, st = p["spec"], p.get("status", {})
    cstat = {c["name"]: c for c in st.get("containerStatuses", [])}
    containers = []
    for c in spec["containers"]:
        r = c.get("resources", {})
        s = cstat.get(c["name"], {})
        state = next(iter(s.get("state", {}) or {}), None)
        last = s.get("lastState", {}).get("terminated", {})
        containers.append({
            "name": c["name"], "image": c.get("image"), "ready": s.get("ready"),
            "restarts": s.get("restartCount", 0), "state": state,
            "last_termination_reason": last.get("reason"),
            "cpu_request_m": cpu_m(r.get("requests", {}).get("cpu")),
            "cpu_limit_m": cpu_m(r.get("limits", {}).get("cpu")),
            "memory_request_mib": mib(mem_b(r.get("requests", {}).get("memory"))),
            "memory_limit_mib": mib(mem_b(r.get("limits", {}).get("memory"))),
        })
    return {"namespace": p["metadata"]["namespace"], "pod": p["metadata"]["name"],
            "node": spec.get("nodeName"), "phase": st.get("phase"), "qos": st.get("qosClass"),
            "pod_ip": st.get("podIP"), "start_time": st.get("startTime"),
            "labels": p["metadata"].get("labels", {}),
            "restarts": sum(c["restarts"] for c in containers), "containers": containers}


@app.get("/pods/status", tags=["core"], summary="Phase, node, restarts, requests/limits per pod")
def pods_status(namespace: Optional[str] = None, label_selector: Optional[str] = None,
                field_selector: Optional[str] = None):
    params = {k: v for k, v in {"labelSelector": label_selector, "fieldSelector": field_selector}.items() if v}
    data = k8s().get(ns_path("/api/v1", namespace, "pods"), params=params)
    return [_pod_status(p) for p in data["items"]]


@app.get("/pods/overview", tags=["core"],
         summary="Usage joined with requests/limits -> utilisation % per pod")
def pods_overview(namespace: Optional[str] = None, label_selector: Optional[str] = None):
    status = pods_status(namespace, label_selector, None)
    try:
        usage = {(u["namespace"], u["pod"]): u for u in pods_usage(namespace, "cpu", False)}
    except HTTPException:
        usage = {}

    def pct(a, b):
        return round(100 * a / b, 1) if a is not None and b else None

    out = []
    for s in status:
        u = usage.get((s["namespace"], s["pod"]), {})
        sumf = lambda k: (sum(c[k] for c in s["containers"])  # noqa: E731
                          if all(c[k] is not None for c in s["containers"]) else None)
        cr, cl, mr, ml = sumf("cpu_request_m"), sumf("cpu_limit_m"), sumf("memory_request_mib"), sumf("memory_limit_mib")
        cu, mu = u.get("cpu_millicores"), u.get("memory_mib")
        out.append({"namespace": s["namespace"], "pod": s["pod"], "node": s["node"],
                    "phase": s["phase"], "restarts": s["restarts"],
                    "cpu_millicores": cu, "cpu_request_m": cr, "cpu_limit_m": cl,
                    "cpu_pct_of_request": pct(cu, cr), "cpu_pct_of_limit": pct(cu, cl),
                    "memory_mib": mu, "memory_request_mib": mr, "memory_limit_mib": ml,
                    "memory_pct_of_request": pct(mu, mr), "memory_pct_of_limit": pct(mu, ml)})
    return out


@app.get("/events", tags=["core"], summary="Cluster events (OOMKilled, BackOff, FailedScheduling ...)")
def events(namespace: Optional[str] = None, pod: Optional[str] = None,
           type: Optional[str] = Query(None, pattern="^(Normal|Warning)$"), limit: int = 500):
    fs = []
    if pod:
        fs += [f"involvedObject.name={pod}", "involvedObject.kind=Pod"]
    if type:
        fs.append(f"type={type}")
    params = {"limit": limit, **({"fieldSelector": ",".join(fs)} if fs else {})}
    data = k8s().get(ns_path("/api/v1", namespace, "events"), params=params)
    rows = [{"namespace": e["metadata"]["namespace"], "type": e.get("type"), "reason": e.get("reason"),
             "object": f'{e["involvedObject"].get("kind")}/{e["involvedObject"].get("name")}',
             "message": e.get("message"), "count": e.get("count"),
             "last_seen": e.get("lastTimestamp") or e.get("eventTime")} for e in data["items"]]
    return sorted(rows, key=lambda r: r["last_seen"] or "", reverse=True)


@app.get("/nodes", tags=["core"], summary="Nodes with capacity, allocatable, conditions")
def nodes():
    out = []
    for n in k8s().get("/api/v1/nodes")["items"]:
        st = n["status"]
        out.append({"node": n["metadata"]["name"],
                    "roles": [k.split("/")[-1] for k in n["metadata"].get("labels", {})
                              if k.startswith("node-role.kubernetes.io/")],
                    "kubelet_version": st["nodeInfo"]["kubeletVersion"],
                    "os_image": st["nodeInfo"]["osImage"], "arch": st["nodeInfo"]["architecture"],
                    "capacity": st.get("capacity"), "allocatable": st.get("allocatable"),
                    "conditions": {c["type"]: c["status"] for c in st.get("conditions", [])},
                    "addresses": {a["type"]: a["address"] for a in st.get("addresses", [])}})
    return out


# --------------------------------------------------------------------------- #
# API server                                                                   #
# --------------------------------------------------------------------------- #
@app.get("/apiserver/metrics", tags=["apiserver"], summary="API server Prometheus metrics")
def apiserver_metrics(metric: Optional[str] = Query(None, description="prefix, e.g. apiserver_request_total"),
                      limit: int = 5000, format: str = _fmt):
    text = k8s().get("/metrics", text=True)
    if format == "text":
        return PlainTextResponse(text)
    return parse_prom(text, metric, None, None, limit)


@app.get("/apiserver/health", tags=["apiserver"], summary="livez / readyz checks (verbose)")
def apiserver_health():
    res = {}
    for ep in ("livez", "readyz"):
        try:
            res[ep] = k8s().get(f"/{ep}", params={"verbose": "true"}, text=True).splitlines()
        except HTTPException as e:
            res[ep] = {"status": e.status_code, "detail": e.detail}
    return res


# --------------------------------------------------------------------------- #
# Custom / external metrics (prometheus-adapter, KEDA ...)                     #
# --------------------------------------------------------------------------- #
@app.get("/custom-metrics/{path:path}", tags=["adapters"],
         summary="custom.metrics.k8s.io passthrough, e.g. namespaces/default/pods/*/http_requests")
def custom_metrics(path: str = "", metricLabelSelector: Optional[str] = None):
    params = {"metricLabelSelector": metricLabelSelector} if metricLabelSelector else None
    return k8s().get(f"/apis/custom.metrics.k8s.io/v1beta1/{path}".rstrip("/"), params=params)


@app.get("/external-metrics/{path:path}", tags=["adapters"], summary="external.metrics.k8s.io passthrough")
def external_metrics(path: str = "", labelSelector: Optional[str] = None):
    params = {"labelSelector": labelSelector} if labelSelector else None
    return k8s().get(f"/apis/external.metrics.k8s.io/v1beta1/{path}".rstrip("/"), params=params)


# --------------------------------------------------------------------------- #
# Prometheus (history, PromQL)                                                 #
# --------------------------------------------------------------------------- #
def _prom(path: str, params: dict):
    if not PROM_URL:
        raise HTTPException(503, "PROMETHEUS_URL not configured")
    r = requests.get(PROM_URL + path, params=params, timeout=TIMEOUT)
    if r.status_code >= 400:
        raise HTTPException(r.status_code, r.text[:2000])
    return r.json()


@app.get("/prometheus/query", tags=["prometheus"], summary="Instant PromQL query")
def prom_query(q: str, time: Optional[str] = None):
    return _prom("/api/v1/query", {"query": q, **({"time": time} if time else {})})


@app.get("/prometheus/query_range", tags=["prometheus"], summary="Range PromQL query (time series)")
def prom_query_range(q: str, start: str, end: str, step: str = "30s"):
    return _prom("/api/v1/query_range", {"query": q, "start": start, "end": end, "step": step})


@app.get("/prometheus/targets", tags=["prometheus"], summary="Scrape targets and their health")
def prom_targets(state: str = "active"):
    return _prom("/api/v1/targets", {"state": state})


PRESETS = {
    "cpu":      'sum by (namespace,pod) (rate(container_cpu_usage_seconds_total{container!="",NS}[5m]))',
    "memory":   'sum by (namespace,pod) (container_memory_working_set_bytes{container!="",NS})',
    "throttle": 'sum by (namespace,pod) (rate(container_cpu_cfs_throttled_periods_total{NS}[5m])) '
                '/ sum by (namespace,pod) (rate(container_cpu_cfs_periods_total{NS}[5m]))',
    "net_rx":   'sum by (namespace,pod) (rate(container_network_receive_bytes_total{NS}[5m]))',
    "net_tx":   'sum by (namespace,pod) (rate(container_network_transmit_bytes_total{NS}[5m]))',
    "fs_read":  'sum by (namespace,pod) (rate(container_fs_reads_bytes_total{NS}[5m]))',
    "fs_write": 'sum by (namespace,pod) (rate(container_fs_writes_bytes_total{NS}[5m]))',
    "restarts": 'sum by (namespace,pod) (kube_pod_container_status_restarts_total{NS})',
    "oomkills": 'sum by (namespace,pod) (kube_pod_container_status_last_terminated_reason{reason="OOMKilled",NS})',
}


@app.get("/prometheus/pods/{preset}", tags=["prometheus"],
         summary="Ready-made pod queries: " + ", ".join(PRESETS))
def prom_preset(preset: str, namespace: Optional[str] = None,
                start: Optional[str] = None, end: Optional[str] = None, step: str = "30s"):
    if preset not in PRESETS:
        raise HTTPException(404, f"unknown preset; choose from {list(PRESETS)}")
    q = PRESETS[preset].replace(",NS", f',namespace="{namespace}"' if namespace else "") \
                       .replace("{NS}", f'{{namespace="{namespace}"}}' if namespace else "")
    if start and end:
        return {"query": q, **prom_query_range(q, start, end, step)}
    return {"query": q, **prom_query(q, None)}


# --------------------------------------------------------------------------- #
# Escape hatch: any read-only API path                                         #
# --------------------------------------------------------------------------- #
@app.get("/raw", tags=["raw"], summary="GET any API-server path, e.g. /api/v1/namespaces")
def raw(path: str = Query(..., description="must start with /api, /apis, /metrics, /livez, /readyz, /version")):
    if not path.startswith(("/api/", "/apis", "/api?", "/metrics", "/livez", "/readyz", "/version")):
        raise HTTPException(400, "path not allowed")
    try:
        return k8s().get(path)
    except ValueError:
        return PlainTextResponse(k8s().get(path, text=True))


# --------------------------------------------------------------------------- #
# Topology & discovery (what EMS reads)                                        #
# --------------------------------------------------------------------------- #
@app.get("/topology", tags=["topology"],
         summary="Cluster identity, nodes, zones and the metric-endpoint catalog")
def topology(refresh: bool = False):
    t = _bg.get("topology")
    if t and t.last and not refresh:
        return t.last
    return _topology.build_topology(k8s(), capabilities())


@app.get("/endpoints", tags=["topology"], summary="Catalog of metric endpoints with availability")
def endpoints():
    return topology(False)["endpoints"]


@app.get("/status", tags=["meta"], summary="Background workers: topology publisher & EMS sidecar attacher")
def status():
    t, a = _bg.get("topology"), _bg.get("attacher")
    return {
        "version": __version__,
        "namespace": own_namespace(),
        "auth": bool(API_KEY),
        "topology": None if not t else {"hash": t.last_hash, "error": t.last_error,
                                        "configmap": _topology.TOPOLOGY_CONFIGMAP},
        "sidecar_attacher": None if not a else {"targets": a.last, "error": a.last_error},
    }


# --------------------------------------------------------------------------- #
# Sidecar client configuration persistence                                     #
# --------------------------------------------------------------------------- #
def _client_cm_path(namespace: str) -> str:
    return f"/api/v1/namespaces/{namespace}/configmaps/{_attacher.CLIENT_SAVED_CM}"


@app.get("/client-config/{namespace}", tags=["sidecar"], summary="Read the sidecar client config")
def get_client_config(namespace: str):
    r = k8s().request("GET", _client_cm_path(namespace))
    if r.status_code == 404:
        cm = k8s().get(f"/api/v1/namespaces/{namespace}/configmaps/{_attacher.CLIENT_CONFIG_CM}")
        return {"namespace": namespace, "source": _attacher.CLIENT_CONFIG_CM,
                "client.yaml": cm.get("data", {}).get("client.yaml", "")}
    if r.status_code >= 400:
        raise HTTPException(r.status_code, r.text[:1000])
    return {"namespace": namespace, "source": _attacher.CLIENT_SAVED_CM,
            "client.yaml": r.json().get("data", {}).get("client.yaml", "")}


@app.put("/client-config/{namespace}", tags=["sidecar"],
         summary="Persist the sidecar client config (used by the sidecar UI)")
def put_client_config(namespace: str, payload: dict = Body(..., examples=[{"client.yaml": "collectors: []"}])):
    text = payload.get("client.yaml")
    if not isinstance(text, str):
        raise HTTPException(422, "body must be {\"client.yaml\": \"...\"}")
    try:
        yaml.safe_load(text)
    except yaml.YAMLError as e:
        raise HTTPException(422, f"invalid YAML: {e}") from e
    patch = {"data": {"client.yaml": text}}
    r = k8s().request("PATCH", _client_cm_path(namespace), json=patch,
                      content_type="application/merge-patch+json")
    if r.status_code == 404:
        body = {"apiVersion": "v1", "kind": "ConfigMap",
                "metadata": {"name": _attacher.CLIENT_SAVED_CM, "namespace": namespace,
                             "labels": {"app.kubernetes.io/part-of": "swarmchestrate"}},
                "data": {"client.yaml": text}}
        r = k8s().request("POST", f"/api/v1/namespaces/{namespace}/configmaps", json=body)
    if r.status_code >= 400:
        raise HTTPException(r.status_code, r.text[:1000])
    return {"saved": True, "namespace": namespace, "configmap": _attacher.CLIENT_SAVED_CM}
