"""OptimusDB trust sink: turns k3s samples into trust-attribute evidence documents.

The sink listens to the samples that the sidecar already collects for EMS and
derives the two trust attributes that k3s supports out of the box (see the
Swarmchestrate Trust Attributes Catalogue):

* **A1 Availability**: the share of checks in which the subject was available.
  ``pod_running`` is used for pods, ``node_ready`` for nodes and
  ``apiserver_readyz`` for the control plane.
* **A3 Operational stability**: unexpected restarts and crashes in a window.
  For pods this is the increase of the container restart counter
  (``pod_restarts``). For nodes and the control plane it is the number of
  ready-to-not-ready transitions. Warning events (BackOff, OOMKilling,
  Rebooted ...) are attached as evidence.

At every ``flush_interval`` the sink upserts one ``trust_evidence`` document per
subject and attribute, plus one ``monitoring_snapshot`` per subject, into an
OptimusDB datastore (default ``kbtrust``) through the public command API::

    POST {url}/{context}/command
    {"method": {"cmd": "crudput", "argcnt": 1}, "dstype": "kbtrust", "criteria": [docs...]}

Documents are keyed by ``_id``, so each write replaces the previous version
(upsert). If ``history`` is on, an hourly copy is also written under a
time-stamped ``_id``, which gives an append-only trust history.

Hourly buckets are written to ``state_path`` (on the sidecar PVC), so the
rolling windows survive restarts of the sidecar.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx

from ... import __version__
from ..collectors.base import param
from ..config import parse_duration
from ..model import Sample
from .base import Sink, register_sink
from .builtin import CLIENT_NODE_ID

log = logging.getLogger("swch.client.sinks.trust")

HOUR_MS = 3_600_000

# base metric name -> (subject kind, attribute it feeds)
AVAILABILITY_INPUTS = {"pod_running": "pod", "node_ready": "node", "apiserver_readyz": "control-plane"}
STABILITY_COUNTERS = {"pod_restarts": "pod"}
INPUT_METRICS = sorted([*AVAILABILITY_INPUTS, *STABILITY_COUNTERS, "k8s_event", "topology"],
                       key=len, reverse=True)

ATTRIBUTES = {
    "A1": {"attribute": "Availability", "role_in_tms": "Conditional or calculable",
           "unit": "ratio", "catalogue_rule": "Availability >= 99.5% in a defined 30-day period"},
    "A3": {"attribute": "Operational stability", "role_in_tms": "Calculable",
           "unit": "count/window", "catalogue_rule": "No more than 2 unplanned restarts in 7 days"},
}

_SAFE = "[bcdfghjklmnpqrstvwxz2456789]"                        # k8s random-name alphabet (no vowels)
_RS_SUFFIX = re.compile(rf"-{_SAFE}{{6,10}}-{_SAFE}{{5}}$")     # Deployment pod: <name>-<rs-hash>-<pod>
_POD_SUFFIX = re.compile(rf"-{_SAFE}{{5}}$")                    # DaemonSet / Job pod
_STS_SUFFIX = re.compile(r"-\d+$")                         # StatefulSet pod


def workload_of(pod: str) -> str:
    """Best-effort workload name from a pod name (the pod name changes on every rollout)."""
    for rx in (_RS_SUFFIX, _STS_SUFFIX, _POD_SUFFIX):
        if rx.search(pod):
            return rx.sub("", pod)
    return pod


def iso(ms: float) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def base_metric(name: str) -> str | None:
    for m in INPUT_METRICS:
        if name == m or name.endswith("_" + m):
            return m
    return None


class _Subject:
    __slots__ = (
        "avail",
        "cluster",
        "cluster_uid",
        "first_seen",
        "kind",
        "last_counter",
        "last_reasons",
        "last_seen",
        "last_up",
        "name",
        "namespace",
        "node",
        "swarm_id",
        "unstable",
        "warnings",
    )

    def __init__(self, kind: str, name: str, namespace: str = "", node: str = "", cluster: str = ""):
        self.kind, self.name, self.namespace, self.node, self.cluster = kind, name, namespace, node, cluster
        self.cluster_uid = self.swarm_id = ""
        self.first_seen = self.last_seen = 0
        self.avail: dict[int, list[int]] = {}     # hour -> [ok, total]
        self.unstable: dict[int, int] = {}        # hour -> restarts / down transitions
        self.warnings: dict[int, int] = {}        # hour -> warning events
        self.last_counter: float | None = None    # last restart counter value
        self.last_up: int | None = None           # last availability value (for transitions)
        self.last_reasons: list[str] = []

    @property
    def key(self) -> str:
        return f"{self.cluster}|{self.kind}|{self.namespace}|{self.name}"

    def to_json(self) -> dict[str, Any]:
        return {k: getattr(self, k) for k in self.__slots__}

    @classmethod
    def from_json(cls, d: dict[str, Any]) -> _Subject:
        s = cls(d["kind"], d["name"], d.get("namespace", ""), d.get("node", ""), d.get("cluster") or "")
        for k in cls.__slots__:
            if k in d:
                setattr(s, k, d[k])
        s.avail = {int(h): v for h, v in (d.get("avail") or {}).items()}
        s.unstable = {int(h): v for h, v in (d.get("unstable") or {}).items()}
        s.warnings = {int(h): v for h, v in (d.get("warnings") or {}).items()}
        return s


@register_sink
class OptimusDBTrustSink(Sink):
    type = "optimusdb_trust"
    title = "OptimusDB trust evidence"
    description = ("Derive trust attributes A1 Availability and A3 Operational stability per pod, node and "
                   "control plane and upsert them into an OptimusDB datastore (default kbtrust).")
    options_schema = {
        "url": param("string", "http://193.225.250.240/optimusdb1", "OptimusDB agent base URL", required=True),
        "context": param("string", "swarmkb", "OptimusDB API context"),
        "dstype": param("string", "kbtrust", "Datastore (dstype) for the trust documents"),
        "cluster": param("string", "", "Cluster name in the documents (empty = topology cluster_id, then the 'cluster' label)"),
        "flush_interval": param("string", "60s", "How often documents are written"),
        "availability_window": param("string", "30d", "Rolling window for A1"),
        "availability_slo": param("number", 0.995, "A1 SLO: minimum availability ratio"),
        "stability_window": param("string", "7d", "Rolling window for A3"),
        "max_unplanned_restarts": param("integer", 2, "A3 SLO: maximum unplanned restarts in the window"),
        "namespaces": param("list", [], "Only pods in these namespaces (empty = all collected)"),
        "exclude_pod_regex": param("string", "", "Skip pods whose name matches (e.g. job pods)"),
        "warning_reasons": param("list", ["BackOff", "OOMKilling", "Unhealthy", "Failed", "Evicted", "Rebooted",
                                          "NodeNotReady"], "Warning events attached as A3 evidence"),
        "did_template": param("string", "did:swarm:{cluster}:{kind}:{id}",
                              "Subject DID; placeholders {cluster} {kind} {id} {name} {namespace}"),
        "did_map": param("json", {}, "Explicit DIDs by subject name, e.g. {\"epm-server\": \"did:swarm:capacity-vm-01\"}"),
        "history": param("boolean", True, "Also write an hourly append-only copy (trust_evidence_history)"),
        "monitoring_snapshot": param("boolean", True, "Also write a monitoring_snapshot per subject (optimusTrust shape)"),
        "state_path": param("string", "/data/trust-state.json", "Rolling-window state file (on the PVC)"),
        "headers": param("json", {}, "Extra HTTP headers (e.g. auth)"),
        "timeout": param("integer", 15, "HTTP timeout (s)"),
        "max_docs_per_request": param("integer", 50, "Batch size per crudput"),
        "dry_run": param("boolean", False, "Build the documents but do not write them"),
    }

    def __init__(self, cfg, client_cfg):
        super().__init__(cfg, client_cfg)
        self.subjects: dict[str, _Subject] = {}
        self.cluster_id = ""
        self.cluster_uids: dict[str, str] = {}
        self.ingested = 0
        self.flushes = 0
        self.last_flush: str | None = None
        self.last_docs: list[dict[str, Any]] = []
        self._last_history_hour: int | None = None
        self._task: asyncio.Task | None = None
        self._http: httpx.AsyncClient | None = None
        rx = self.opts.get("exclude_pod_regex") or ""
        self._exclude = re.compile(rx) if rx else None
        self._load_state()

    # ------------------------------------------------------------- lifecycle
    async def start(self):
        hdr = self.opts.get("headers") or {}
        if isinstance(hdr, str):
            hdr = json.loads(hdr or "{}")
        self._http = httpx.AsyncClient(timeout=float(self.opts.get("timeout") or 15),
                                       headers={"Content-Type": "application/json", **hdr})
        self._task = asyncio.create_task(self._loop(), name=f"sink-{self.cfg.id}")

    async def stop(self):
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
        try:
            await self.flush()
        except Exception:  # noqa: BLE001
            pass
        if self._http:
            await self._http.aclose()

    async def _loop(self):
        every = parse_duration(self.opts.get("flush_interval") or "60s")
        while True:
            await asyncio.sleep(every)
            try:
                await self.flush()
            except Exception as e:  # noqa: BLE001
                log.warning("trust flush failed: %s", e)

    # ------------------------------------------------------------- ingestion
    async def send(self, samples: list[Sample]) -> int:
        n = 0
        for s in samples:
            m = base_metric(s.metric)
            if m is None:
                continue
            n += 1
            self._ingest(m, s)
        self.ingested += n
        return len(samples)

    def _subject(self, kind: str, name: str, namespace: str = "", node: str = "",
                 lbl: dict | None = None) -> _Subject | None:
        if kind == "pod":
            ns_filter = self.opts.get("namespaces") or []
            if ns_filter and namespace not in ns_filter:
                return None
            if self._exclude and self._exclude.search(name):
                return None
        lbl = lbl or {}
        cluster = lbl.get("cluster") or self.cluster_name()     # one gateway = one cluster (label set by the engine)
        key = f"{cluster}|{kind}|{namespace}|{name}"
        sub = self.subjects.get(key)
        if sub is None:
            sub = self.subjects[key] = _Subject(kind, name, namespace, node, cluster)
        if node:
            sub.node = node
        sub.cluster_uid = lbl.get("cluster_uid") or sub.cluster_uid or self.cluster_uids.get(cluster, "")
        sub.swarm_id = lbl.get("swarm_id") or sub.swarm_id
        return sub

    def _ingest(self, m: str, s: Sample) -> None:
        ts = int(s.timestamp_ms)
        hour = ts // HOUR_MS
        lbl = s.labels or {}
        if m == "topology":
            uid = lbl.get("cluster_uid") or lbl.get("cluster_id") or (s.payload or {}).get("cluster_id")
            if uid:
                self.cluster_id = self.cluster_id or uid
                self.cluster_uids[lbl.get("cluster") or self.cluster_name()] = uid
            return
        if m == "k8s_event":
            if lbl.get("reason") not in (self.opts.get("warning_reasons") or []):
                return
            kind = {"Pod": "pod", "Node": "node"}.get(lbl.get("kind", ""))
            if not kind:
                return
            sub = self._subject(kind, lbl.get("object") or s.instance or "",
                                lbl.get("namespace", "") if kind == "pod" else "", lbl=lbl)
            if sub:
                sub.warnings[hour] = sub.warnings.get(hour, 0) + int(s.value or 1)
                sub.last_reasons = (sub.last_reasons + [lbl["reason"]])[-5:]
            return

        if m in AVAILABILITY_INPUTS:
            kind = AVAILABILITY_INPUTS[m]
            name = {"pod": lbl.get("pod") or s.instance, "node": lbl.get("node") or s.instance,
                    "control-plane": "kube-apiserver"}[kind] or ""
            sub = self._subject(kind, name, lbl.get("namespace", "") if kind == "pod" else "",
                                lbl.get("node", "") if kind == "pod" else "", lbl=lbl)
            if sub is None:
                return
            up = 1 if float(s.value) >= 1 else 0
            b = sub.avail.setdefault(hour, [0, 0])
            b[0] += up
            b[1] += 1
            if kind != "pod" and sub.last_up == 1 and up == 0:    # node / control-plane went down
                sub.unstable[hour] = sub.unstable.get(hour, 0) + 1
            sub.last_up = up
        elif m in STABILITY_COUNTERS:
            sub = self._subject("pod", lbl.get("pod") or s.instance or "", lbl.get("namespace", ""),
                                lbl.get("node", ""), lbl=lbl)
            if sub is None:
                return
            v = float(s.value)
            if sub.last_counter is not None and v > sub.last_counter:
                sub.unstable[hour] = sub.unstable.get(hour, 0) + int(v - sub.last_counter)
            sub.last_counter = v
        else:
            return
        sub.first_seen = sub.first_seen or ts
        sub.last_seen = max(sub.last_seen, ts)

    # ------------------------------------------------------------- evaluation
    def cluster_name(self) -> str:
        return (self.opts.get("cluster") or self.cluster_id
                or self.client_cfg.labels.get("cluster") or "k3s")

    def _did(self, sub: _Subject, cluster: str) -> str:
        dmap = self.opts.get("did_map") or {}
        if isinstance(dmap, str):
            dmap = json.loads(dmap or "{}")
        for k in (f"{cluster}/{sub.name}", sub.name):     # "cluster/name" wins over a bare name
            if k in dmap:
                return dmap[k]
        ident = f"{sub.namespace}:{sub.name}" if sub.namespace else sub.name
        return (self.opts.get("did_template") or "did:swarm:{cluster}:{kind}:{id}").format(
            cluster=cluster, kind=sub.kind, id=ident, name=sub.name, namespace=sub.namespace)

    def evaluate(self, now_ms: int | None = None) -> list[dict[str, Any]]:
        now_ms = now_ms or int(time.time() * 1000)
        now_h = now_ms // HOUR_MS
        a_win = parse_duration(self.opts.get("availability_window") or "30d")
        s_win = parse_duration(self.opts.get("stability_window") or "7d")
        a_hours, s_hours = max(1, int(a_win // 3600)), max(1, int(s_win // 3600))
        slo = float(self.opts.get("availability_slo") or 0.995)
        max_r = int(self.opts.get("max_unplanned_restarts") if self.opts.get("max_unplanned_restarts") is not None else 2)
        producer = {"client": self.client_cfg.client_name, "sink": self.cfg.id, "node_id": CLIENT_NODE_ID,
                    "version": __version__}
        docs: list[dict[str, Any]] = []
        keep = max(a_hours, s_hours)
        for key in list(self.subjects):
            sub = self.subjects[key]
            # forget subjects not seen for a whole window (deleted pods); their documents stay in OptimusDB
            if sub.last_seen and now_h - sub.last_seen // HOUR_MS > keep:
                del self.subjects[key]
                continue
            for d in (sub.avail, sub.unstable, sub.warnings):
                for h in [h for h in d if now_h - h >= keep]:
                    del d[h]
            if not sub.first_seen:
                continue
            cluster = sub.cluster or self.cluster_name()
            did = self._did(sub, cluster)
            subject = {"kind": sub.kind, "name": sub.name, "namespace": sub.namespace, "node": sub.node,
                       "cluster": cluster, "did": did,
                       "workload": workload_of(sub.name) if sub.kind == "pod" else sub.name}
            flat = {"subject_kind": sub.kind, "subject_name": sub.name, "subject_did": did, "cluster": cluster,
                    "cluster_uid": sub.cluster_uid or self.cluster_uids.get(cluster, ""), "swarm_id": sub.swarm_id,
                    "namespace": sub.namespace, "node": sub.node, "workload": subject["workload"]}
            base_id = f"trust:{cluster}:{sub.kind}:{(sub.namespace + '/') if sub.namespace else ''}{sub.name}"
            observed_from = max(sub.first_seen, now_ms - int(a_win * 1000))
            # ---- A1
            win_h = sorted(h for h in sub.avail if now_h - h < a_hours)
            win = [sub.avail[h] for h in win_h]
            ok, total = sum(v[0] for v in win), sum(v[1] for v in win)
            if total:
                ratio = round(ok / total, 6)
                hours_span = max(1, now_h - win_h[0] + (1 if win_h[-1] == now_h else 0))   # hours that had polls
                src = {"pod": "pod_running", "node": "node_ready", "control-plane": "apiserver_readyz"}[sub.kind]
                docs.append(self._doc(base_id, "A1", subject, flat, producer, now_ms, observed_from, a_win,
                                      value=ratio, score=ratio, slo_met=ratio >= slo,
                                      slo={"threshold": slo, "operator": ">=", "window": self.opts.get("availability_window")},
                                      evidence={"checks_ok": ok, "checks_total": total,
                                                "hours_with_data": len(win),
                                                "coverage": round(min(1.0, len(win) / hours_span), 4),
                                                "source_metric": self.client_cfg.metric_prefix + src,
                                                "method": "available checks / observed checks"}))
            # ---- A3
            events = sum(v for h, v in sub.unstable.items() if now_h - h < s_hours)
            warns = sum(v for h, v in sub.warnings.items() if now_h - h < s_hours)
            if sub.kind == "pod" and sub.last_counter is None and not warns:
                pass   # no restart counter seen yet
            else:
                score = round(max(0.0, 1 - events / (max_r + 1)), 4)
                src = "pod_restarts" if sub.kind == "pod" else {"node": "node_ready",
                                                                "control-plane": "apiserver_readyz"}[sub.kind]
                ev = {"unplanned_events": events, "warning_events": warns, "recent_warning_reasons": sub.last_reasons,
                      "source_metric": self.client_cfg.metric_prefix + src,
                      "method": ("increase of container restart counter" if sub.kind == "pod"
                                 else "ready -> not-ready transitions")}
                if sub.kind == "pod":
                    ev["restart_counter"] = sub.last_counter
                docs.append(self._doc(base_id, "A3", subject, flat, producer, now_ms,
                                      max(sub.first_seen, now_ms - int(s_win * 1000)), s_win,
                                      value=events, score=score, slo_met=events <= max_r,
                                      slo={"threshold": max_r, "operator": "<=", "window": self.opts.get("stability_window")},
                                      evidence=ev))
            if self.opts.get("monitoring_snapshot"):
                a1 = next((d for d in docs if d["_id"] == f"{base_id}:A1"), None)
                a3 = next((d for d in docs if d["_id"] == f"{base_id}:A3"), None)
                docs.append({"_id": f"monitoring:{cluster}:{sub.kind}:{(sub.namespace + '/') if sub.namespace else ''}{sub.name}",
                             "type": "monitoring_snapshot", "device_did": did, "device_name": sub.name,
                             "device_type": sub.kind, **flat,
                             "monitoring": {"uptime": a1["value"] if a1 else None,
                                            "unplanned_restarts": a3["value"] if a3 else None},
                             "attributes": {"A1": a1["score"] if a1 else None, "A3": a3["score"] if a3 else None},
                             "last_seen": iso(sub.last_seen), "updated_at": iso(now_ms), "producer": producer})
        return docs

    def _doc(self, base_id, attr, subject, flat, producer, now_ms, start_ms, win_s, **kw) -> dict[str, Any]:
        meta = ATTRIBUTES[attr]
        return {"_id": f"{base_id}:{attr}", "type": "trust_evidence", "attribute_id": attr,
                "attribute": meta["attribute"], "role_in_tms": meta["role_in_tms"], "unit": meta["unit"],
                "subject": subject, **flat, "value": kw["value"], "score": kw["score"],
                "slo": {**kw["slo"], "catalogue_rule": meta["catalogue_rule"]}, "slo_met": kw["slo_met"],
                "evidence": kw["evidence"],
                "window": {"start": iso(start_ms), "end": iso(now_ms), "seconds": int(win_s)},
                "updated_at": iso(now_ms), "producer": producer}

    # ------------------------------------------------------------- writing
    async def flush(self, now_ms: int | None = None) -> int:
        now_ms = now_ms or int(time.time() * 1000)
        docs = self.evaluate(now_ms)
        hour = now_ms // HOUR_MS
        if self.opts.get("history") and docs and hour != self._last_history_hour:
            stamp = datetime.fromtimestamp(hour * 3600, tz=UTC).strftime("%Y%m%dT%H")
            docs += [{**d, "_id": f"{d['_id']}:{stamp}", "type": d["type"] + "_history", "latest_id": d["_id"]}
                     for d in docs if d["type"] == "trust_evidence"]
            self._last_history_hour = hour
        self.last_docs = docs
        self._save_state()
        if not docs or self.opts.get("dry_run"):
            self.flushes += 1
            self.last_flush = iso(now_ms)
            return 0
        if self._http is None:
            self._http = httpx.AsyncClient(timeout=float(self.opts.get("timeout") or 15))
        url = f"{str(self.opts['url']).rstrip('/')}/{self.opts.get('context') or 'swarmkb'}/command"
        size = max(1, int(self.opts.get("max_docs_per_request") or 50))
        written = 0
        try:
            for i in range(0, len(docs), size):
                body = {"method": {"cmd": "crudput", "argcnt": 1}, "dstype": self.opts.get("dstype") or "kbtrust",
                        "criteria": docs[i:i + size]}
                r = await self._http.post(url, json=body)
                r.raise_for_status()
                written += len(body["criteria"])
            self.connected, self.last_error = True, None
        except httpx.HTTPError as e:
            self.connected = False
            self.errors += 1
            self.last_error = f"{type(e).__name__}: {e}"
            log.warning("optimusdb write failed: %s", self.last_error)
            if self._last_history_hour == hour:
                self._last_history_hour = None   # retry the history copy at the next flush
        finally:
            self.sent += written
            self.flushes += 1
            self.last_flush = iso(now_ms)
        return written

    # ------------------------------------------------------------- state
    def _load_state(self) -> None:
        p = self.opts.get("state_path")
        if not p or not os.path.exists(p):
            return
        try:
            d = json.loads(Path(p).read_text())
            self.cluster_id = d.get("cluster_id", "")
            self.cluster_uids = d.get("cluster_uids") or {}
            subs = [_Subject.from_json(x) for x in d.get("subjects", [])]
            for sub in subs:                     # state written by v0.2 (single cluster): no cluster field
                sub.cluster = sub.cluster or self.cluster_name()
            self.subjects = {sub.key: sub for sub in subs}
            log.info("trust state loaded: %d subjects", len(self.subjects))
        except Exception as e:  # noqa: BLE001
            log.warning("cannot read trust state %s: %s", p, e)

    def _save_state(self) -> None:
        p = self.opts.get("state_path")
        if not p:
            return
        try:
            Path(p).parent.mkdir(parents=True, exist_ok=True)
            tmp = f"{p}.tmp"
            Path(tmp).write_text(json.dumps({"cluster_id": self.cluster_id, "cluster_uids": self.cluster_uids,
                                             "subjects": [s.to_json() for s in self.subjects.values()]}))
            os.replace(tmp, p)
        except OSError as e:
            log.debug("cannot write trust state %s: %s", p, e)

    def status(self) -> dict[str, Any]:
        st = super().status()
        st.update({"endpoint": f"{self.opts.get('url')}/{self.opts.get('context')}/command",
                   "dstype": self.opts.get("dstype"), "cluster": self.cluster_name(),
                   "clusters": sorted({sub.cluster for sub in self.subjects.values()}),
                   "subjects": len(self.subjects), "samples_ingested": self.ingested,
                   "flushes": self.flushes, "last_flush": self.last_flush, "documents_last_flush": len(self.last_docs),
                   "slo_violations": sorted(d["_id"] for d in self.last_docs
                                            if d.get("type") == "trust_evidence" and d.get("slo_met") is False)})
        return st
