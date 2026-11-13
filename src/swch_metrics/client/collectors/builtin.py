"""Built-in collectors, one per gateway capability."""
from __future__ import annotations

import json
from typing import Any

from ..model import Sample
from .base import Collector, dig, param, register

NS = param("string", None, "Namespace (empty = all namespaces)")


def _num(v: Any) -> float | None:
    try:
        return None if v is None else float(v)
    except (TypeError, ValueError):
        return None


def _emit(out: list[Sample], metric: str, value: Any, labels: dict, instance: str | None) -> None:
    v = _num(value)
    if v is not None:
        out.append(Sample(metric=metric, value=v, labels=labels, instance=instance))


@register
class PodUsage(Collector):
    type = "pod_usage"
    title = "Pod CPU & memory"
    description = "Current CPU (millicores) and memory (MiB) per pod from metrics-server (kubectl top pods)."
    endpoint = "/pods/usage"
    default_interval = "30s"
    params_schema = {"namespace": NS, "per_container": param("boolean", False, "Also emit per-container values")}
    outputs = ["pod_cpu_millicores", "pod_memory_mib", "container_cpu_millicores", "container_memory_mib"]

    def query(self):
        return {"namespace": self.params.get("namespace")}

    def transform(self, data):
        out: list[Sample] = []
        for p in data:
            lbl = {"namespace": p["namespace"], "pod": p["pod"]}
            _emit(out, "pod_cpu_millicores", p["cpu_millicores"], lbl, p["pod"])
            _emit(out, "pod_memory_mib", p["memory_mib"], lbl, p["pod"])
            if self.params.get("per_container"):
                for c in p.get("containers", []):
                    cl = {**lbl, "container": c["name"]}
                    _emit(out, "container_cpu_millicores", c["cpu_millicores"], cl, p["pod"])
                    _emit(out, "container_memory_mib", c["memory_mib"], cl, p["pod"])
        return out


@register
class NodeUsage(Collector):
    type = "node_usage"
    title = "Node CPU & memory"
    description = "CPU/memory per node with percentage of allocatable (kubectl top nodes)."
    endpoint = "/nodes/usage"
    default_interval = "30s"
    outputs = ["node_cpu_millicores", "node_cpu_pct", "node_memory_mib", "node_memory_pct"]

    def transform(self, data):
        out: list[Sample] = []
        for n in data:
            lbl = {"node": n["node"]}
            for k in ("cpu_millicores", "cpu_pct", "memory_mib", "memory_pct"):
                _emit(out, f"node_{k}", n.get(k), lbl, n["node"])
        return out


@register
class PodStats(Collector):
    type = "pod_stats"
    title = "Pod network, disk & volumes"
    description = "Kubelet summary per pod: CPU, working-set memory, network rx/tx, ephemeral storage and volume usage."
    endpoint = "/pods/stats"
    default_interval = "60s"
    params_schema = {"namespace": NS,
                     "rate": param("boolean", True, "Convert network byte counters to bytes/second")}
    outputs = ["pod_cpu_millicores", "pod_memory_working_set_mib",
               "pod_network_rx_bytes_per_sec", "pod_network_tx_bytes_per_sec",
               "pod_network_rx_bytes_total", "pod_network_tx_bytes_total",
               "pod_ephemeral_storage_used_mib", "pod_volume_used_mib", "pod_volume_used_pct"]

    def query(self):
        return {"namespace": self.params.get("namespace")}

    def transform(self, data):
        out: list[Sample] = []
        prev = self.state.setdefault("prev", {})
        for p in data:
            lbl = {"namespace": p["namespace"], "pod": p["pod"], "node": p["node"]}
            inst = p["pod"]
            _emit(out, "pod_cpu_millicores", p.get("cpu_millicores"), lbl, inst)
            _emit(out, "pod_memory_working_set_mib", p.get("memory_working_set_mib"), lbl, inst)
            _emit(out, "pod_ephemeral_storage_used_mib", p.get("ephemeral_storage_used_mib"), lbl, inst)
            for d in ("rx", "tx"):
                total = p.get(f"network_{d}_bytes_total")
                if total is None:
                    continue
                name = f"pod_network_{d}_bytes"
                if self.params.get("rate"):
                    key = (p["namespace"], p["pod"], d)
                    now = self._now()
                    if key in prev:
                        t0, v0 = prev[key]
                        if now > t0 and total >= v0:
                            _emit(out, f"{name}_per_sec", (total - v0) / (now - t0), lbl, inst)
                    prev[key] = (now, total)
                else:
                    _emit(out, f"{name}_total", total, lbl, inst)
            for v in p.get("volumes", []):
                vl = {**lbl, "volume": v.get("name") or "", "pvc": v.get("pvc") or ""}
                _emit(out, "pod_volume_used_mib", v.get("used_mib"), vl, inst)
                if v.get("used_mib") is not None and v.get("capacity_mib"):
                    _emit(out, "pod_volume_used_pct", 100 * v["used_mib"] / v["capacity_mib"], vl, inst)
        return out

    @staticmethod
    def _now() -> float:
        import time
        return time.monotonic()


@register
class PodOverview(Collector):
    type = "pod_overview"
    title = "Pod utilisation vs requests/limits"
    description = "Usage joined with requests and limits: % of request/limit, restarts and running state per pod."
    endpoint = "/pods/overview"
    default_interval = "60s"
    params_schema = {"namespace": NS,
                     "label_selector": param("string", None, "Label selector, e.g. app=web")}
    outputs = ["pod_cpu_pct_of_request", "pod_cpu_pct_of_limit", "pod_memory_pct_of_request",
               "pod_memory_pct_of_limit", "pod_restarts", "pod_running"]

    def query(self):
        return {"namespace": self.params.get("namespace"), "label_selector": self.params.get("label_selector")}

    def transform(self, data):
        out: list[Sample] = []
        for p in data:
            lbl = {"namespace": p["namespace"], "pod": p["pod"], "node": p.get("node") or ""}
            for k in ("cpu_pct_of_request", "cpu_pct_of_limit", "memory_pct_of_request",
                      "memory_pct_of_limit", "restarts"):
                _emit(out, f"pod_{k}", p.get(k), lbl, p["pod"])
            _emit(out, "pod_running", 1 if p.get("phase") == "Running" else 0, lbl, p["pod"])
        return out


@register
class Events(Collector):
    type = "events"
    title = "Kubernetes events"
    description = "New cluster events since the last poll (e.g. OOMKilled, BackOff, FailedScheduling). Value = event count."
    endpoint = "/events"
    default_interval = "60s"
    params_schema = {"namespace": NS,
                     "type": param("string", "Warning", "Event type", enum=["Warning", "Normal", ""]),
                     "reasons": param("list", [], "Only these reasons (empty = all)")}
    outputs = ["k8s_event"]

    def query(self):
        return {"namespace": self.params.get("namespace"), "type": self.params.get("type") or None}

    def transform(self, data):
        seen: dict[str, str] = self.state.setdefault("seen", {})
        first = not self.state.get("primed")
        reasons = set(self.params.get("reasons") or [])
        out: list[Sample] = []
        for e in data:
            key = f'{e["namespace"]}/{e["object"]}/{e["reason"]}'
            stamp = f'{e.get("last_seen")}#{e.get("count")}'
            if seen.get(key) == stamp:
                continue
            seen[key] = stamp
            if first or (reasons and e.get("reason") not in reasons):
                continue
            kind, _, name = (e.get("object") or "/").partition("/")
            out.append(Sample(metric="k8s_event", value=float(e.get("count") or 1),
                              labels={"namespace": e["namespace"], "reason": e.get("reason") or "",
                                      "kind": kind, "object": name, "event_type": e.get("type") or "",
                                      "message": (e.get("message") or "")[:200]},
                              instance=name))
        self.state["primed"] = True
        return out


@register
class Cadvisor(Collector):
    type = "cadvisor"
    title = "cAdvisor container metrics"
    description = ("Any cAdvisor metric by name prefix across all nodes, e.g. container_cpu_cfs_throttled_periods_total. "
                   "Counters can be converted to per-second rates.")
    endpoint = "/cadvisor"
    default_interval = "60s"
    params_schema = {"metric": param("string", "container_cpu_cfs_throttled_periods_total",
                                     "Metric name prefix", required=True),
                     "namespace": NS,
                     "rate": param("boolean", True, "Emit per-second rate for counters (_total)"),
                     "sum_by_pod": param("boolean", True, "Sum container series per pod")}
    outputs = []  # dynamic

    def query(self):
        return {"metric": self.params["metric"], "namespace": self.params.get("namespace"), "limit": 20000}

    def transform(self, data):
        agg: dict[tuple, float] = {}
        meta: dict[tuple, dict] = {}
        for s in data:
            l = s.get("labels", {})
            if not l.get("pod") or l.get("container") in ("POD",):
                continue
            keep = {k: l[k] for k in ("namespace", "pod", "container", "interface", "device") if l.get(k)}
            keep["node"] = s.get("node", "")
            if self.params.get("sum_by_pod"):
                keep = {k: keep[k] for k in ("namespace", "pod", "node") if k in keep}
            key = (s["name"], tuple(sorted(keep.items())))
            agg[key] = agg.get(key, 0.0) + float(s["value"])
            meta[key] = keep
        out: list[Sample] = []
        prev = self.state.setdefault("prev", {})
        import time
        now = time.monotonic()
        for key, v in agg.items():
            name, lbl = key[0], meta[key]
            if self.params.get("rate") and name.endswith("_total"):
                if key in prev:
                    t0, v0 = prev[key]
                    if now > t0 and v >= v0:
                        out.append(Sample(metric=name[:-6] + "_per_sec", value=(v - v0) / (now - t0),
                                          labels=lbl, instance=lbl.get("pod")))
                prev[key] = (now, v)
            else:
                out.append(Sample(metric=name, value=v, labels=lbl, instance=lbl.get("pod")))
        return out


@register
class PrometheusQuery(Collector):
    type = "prometheus"
    title = "PromQL query"
    description = "Instant PromQL query through the gateway (requires PROMETHEUS_URL on the gateway)."
    endpoint = "/prometheus/query"
    default_interval = "60s"
    params_schema = {"query": param("string", 'sum by (namespace,pod) (rate(container_cpu_usage_seconds_total{container!=""}[5m]))',
                                    "PromQL expression", required=True),
                     "name": param("string", "promql_value", "Metric name to emit", required=True),
                     "instance_label": param("string", "pod", "Label used as EMS instance")}
    outputs = []

    def query(self):
        return {"q": self.params["query"]}

    def transform(self, data):
        out: list[Sample] = []
        for r in dig(data, "data.result") or []:
            lbl = {k: v for k, v in r.get("metric", {}).items() if k != "__name__"}
            val = r.get("value", [None, None])[1]
            _emit(out, self.params["name"], val, lbl, lbl.get(self.params.get("instance_label") or "pod"))
        return out


@register
class Topology(Collector):
    type = "topology"
    title = "Cluster topology"
    description = ("Cluster identity, nodes, zones and endpoint catalog. Emits node readiness numbers every poll and the "
                   "full topology document (topic <prefix>topology) whenever it changes.")
    endpoint = "/topology"
    default_interval = "2m"
    params_schema = {"publish_document": param("boolean", True, "Publish the full JSON document on change")}
    outputs = ["cluster_nodes_total", "cluster_nodes_ready", "node_ready", "topology"]

    def transform(self, data):
        self.state["last"] = data
        out: list[Sample] = []
        summ = data.get("summary", {})
        cl = {"cluster_id": data.get("cluster_id") or ""}
        _emit(out, "cluster_nodes_total", summ.get("nodes"), cl, "cluster")
        _emit(out, "cluster_nodes_ready", summ.get("ready"), cl, "cluster")
        for n in data.get("nodes", []):
            _emit(out, "node_ready", 1 if n.get("ready") else 0,
                  {"node": n["name"], "zone": n.get("zone") or "", "region": n.get("region") or ""}, n["name"])
        if self.params.get("publish_document") and data.get("hash") != self.state.get("hash"):
            self.state["hash"] = data.get("hash")
            out.append(Sample(metric="topology", value=float(summ.get("nodes") or 0), labels=cl,
                              instance="cluster", payload=data))
        return out


@register
class ApiServerHealth(Collector):
    type = "apiserver_health"
    title = "API server health"
    description = "1/0 for API server livez and readyz."
    endpoint = "/apiserver/health"
    default_interval = "30s"
    outputs = ["apiserver_livez", "apiserver_readyz"]

    def transform(self, data):
        out = []
        for k in ("livez", "readyz"):
            v = data.get(k)
            ok = isinstance(v, list) and any("passed" in line for line in v)
            out.append(Sample(metric=f"apiserver_{k}", value=1.0 if ok else 0.0, instance="apiserver"))
        return out


@register
class HttpJson(Collector):
    type = "http_json"
    title = "Generic gateway endpoint"
    description = ("Poll any gateway endpoint returning JSON and map fields to metrics: "
                   "items_path selects the list, value_field the value, label_fields the labels.")
    endpoint = "(custom)"
    default_interval = "60s"
    params_schema = {"path": param("string", "/nodes/usage", "Gateway path", required=True),
                     "query": param("json", {}, "Query parameters as JSON object"),
                     "items_path": param("string", "", "Dotted path to the list of items ('' = root)"),
                     "value_field": param("string", "cpu_pct", "Dotted path to the numeric value", required=True),
                     "metric_name": param("string", "custom_value", "Metric name to emit", required=True),
                     "label_fields": param("list", ["node"], "Fields copied to labels"),
                     "instance_field": param("string", "node", "Field used as EMS instance")}
    outputs = []

    def path(self):
        return self.params["path"]

    def query(self):
        q = self.params.get("query") or {}
        return json.loads(q) if isinstance(q, str) else q

    def transform(self, data):
        items = dig(data, self.params.get("items_path"))
        if isinstance(items, dict):
            items = [items]
        out: list[Sample] = []
        for it in items or []:
            lbl = {f.split(".")[-1]: str(dig(it, f)) for f in self.params.get("label_fields") or [] if dig(it, f) is not None}
            inst = dig(it, self.params.get("instance_field")) if self.params.get("instance_field") else None
            _emit(out, self.params["metric_name"], dig(it, self.params["value_field"]), lbl,
                  str(inst) if inst is not None else None)
        return out
