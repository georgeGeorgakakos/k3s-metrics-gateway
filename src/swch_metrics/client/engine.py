"""Scheduler: one asyncio task per (collector, gateway), fan-out to sinks, live state for the UI.

One client polls any number of k3s Metrics Gateways (one per k3s cluster). Every
sample carries the identity of the cluster it came from (labels ``cluster``,
``swarm_id``, ``gateway``), so EMS and OptimusDB can tell clusters apart.
"""
from __future__ import annotations

import asyncio
import importlib
import logging
import time
from collections import deque
from datetime import datetime, timezone
from typing import Any

from .collectors import build
from .collectors.base import Collector
from .config import ClientConfig, GatewayCfg, parse_duration
from .gateway import GatewayClient, GatewayError
from .model import Sample
from .sinks import build_sink

log = logging.getLogger("swch.client.engine")

MAX_SERIES = 2000
POINTS_PER_SERIES = 360
RECENT_SAMPLES = 3000


def _iso(ts: float | None) -> str | None:
    return datetime.fromtimestamp(ts, timezone.utc).isoformat() if ts else None


class CollectorState:
    def __init__(self, cid: str, ctype: str, interval: float, enabled: bool, gateway: str = "") -> None:
        self.id, self.type, self.interval, self.enabled, self.gateway = cid, ctype, interval, enabled, gateway
        self.runs = 0
        self.failures = 0
        self.last_run: float | None = None
        self.next_run: float | None = None
        self.last_duration_ms: float | None = None
        self.last_count = 0
        self.last_error: str | None = None
        self.sink_errors: dict[str, str] = {}

    @property
    def status(self) -> str:
        return "disabled" if not self.enabled else ("error" if self.last_error else ("ok" if self.runs else "pending"))

    def as_dict(self) -> dict[str, Any]:
        return {"id": self.id, "type": self.type, "gateway": self.gateway, "interval_s": self.interval,
                "enabled": self.enabled, "runs": self.runs, "failures": self.failures,
                "last_run": _iso(self.last_run), "next_run": _iso(self.next_run),
                "last_duration_ms": self.last_duration_ms, "last_count": self.last_count,
                "last_error": self.last_error, "sink_errors": self.sink_errors, "status": self.status}


def merge_states(cid: str, ctype: str, interval: float, enabled: bool, parts: list[CollectorState]) -> dict:
    """One row per collector for the UI, with the per-gateway breakdown."""
    if not parts:
        st = CollectorState(cid, ctype, interval, enabled)
        return {**st.as_dict(), "per_gateway": []}
    errs = [f"{p.gateway}: {p.last_error}" for p in parts if p.last_error]
    last = max((p.last_run or 0) for p in parts) or None
    nxt = [p.next_run for p in parts if p.next_run]
    status = ("disabled" if not enabled else "error" if errs and len(errs) == len(parts)
              else "degraded" if errs else "ok" if any(p.runs for p in parts) else "pending")
    durs = [p.last_duration_ms for p in parts if p.last_duration_ms is not None]
    return {"id": cid, "type": ctype, "interval_s": interval, "enabled": enabled,
            "runs": sum(p.runs for p in parts), "failures": sum(p.failures for p in parts),
            "last_run": _iso(last), "next_run": _iso(min(nxt)) if nxt else None,
            "last_duration_ms": max(durs) if durs else None,
            "last_count": sum(p.last_count for p in parts), "last_error": "; ".join(errs) or None,
            "sink_errors": {k: v for p in parts for k, v in p.sink_errors.items()}, "status": status,
            "per_gateway": [p.as_dict() for p in parts]}


class Engine:
    def __init__(self, cfg: ClientConfig) -> None:
        self.started_at = time.time()
        self.recent: deque[Sample] = deque(maxlen=RECENT_SAMPLES)
        self.series: dict[tuple[str, str], deque] = {}
        self.total_samples = 0
        self.gateway_state: dict[str, dict[str, Any]] = {}   # gid -> {ok, last_ok, error}
        self.topologies: dict[str, dict] = {}                 # gid -> last topology document
        self.heartbeats: dict[str, float] = {}                # gid -> last registration heartbeat (epoch)
        self._tasks: list[asyncio.Task] = []
        self._lock = asyncio.Lock()
        self._apply(cfg)

    # ---------------- lifecycle ----------------
    def _apply(self, cfg: ClientConfig) -> None:
        for mod in cfg.plugins:
            importlib.import_module(mod)  # plugin modules self-register collectors/sinks
        self.cfg = cfg
        self.gws: dict[str, GatewayClient] = {g.id: GatewayClient(g) for g in cfg.gateways if g.enabled}
        self.sinks = {s.id: build_sink(s, cfg) for s in cfg.sinks}
        self.collectors: dict[tuple[str, str], Collector] = {}
        self.states: dict[tuple[str, str], CollectorState] = {}
        for c in cfg.collectors:
            for gid in self.targets(c.id):
                self.collectors[(c.id, gid)] = build(c, cfg)
                self.states[(c.id, gid)] = CollectorState(c.id, c.type, cfg.interval_of(c), c.enabled, gid)
        for gid in list(self.gateway_state):
            if gid not in self.gws:
                self.gateway_state.pop(gid)
        for gid in self.gws:
            self.gateway_state.setdefault(gid, {"ok": None, "last_ok": None, "error": None})

    def targets(self, cid: str) -> list[str]:
        c = next(c for c in self.cfg.collectors if c.id == cid)
        return [g for g in self.gws if c.gateways is None or g in c.gateways]

    async def start(self) -> None:
        for s in self.sinks.values():
            if s.cfg.enabled:
                try:
                    await s.start()
                except Exception as e:  # noqa: BLE001
                    s.last_error = f"start failed: {e}"
        # learn each cluster's identity first, so the very first samples carry the right cluster label
        await asyncio.gather(*(self._learn_topology(gid) for gid in self.gws), return_exceptions=True)
        for i, (key, c) in enumerate(self.collectors.items()):
            if c.cfg.enabled:
                self._tasks.append(asyncio.create_task(self._loop(key, i), name=f"collector:{key[0]}@{key[1]}"))
        for gid in self.gws:
            self._tasks.append(asyncio.create_task(self._health_loop(gid), name=f"gateway-health:{gid}"))
        log.info("engine started: %d gateways, %d collectors (%d tasks), %d sinks", len(self.gws),
                 len(self.cfg.collectors), len(self.collectors), len(self.sinks))

    async def stop(self) -> None:
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        for s in self.sinks.values():
            try:
                await s.stop()
            except Exception:  # noqa: BLE001
                pass
        for gw in self.gws.values():
            await gw.aclose()

    async def reload(self, cfg: ClientConfig) -> None:
        async with self._lock:
            await self.stop()
            self._apply(cfg)
            await self.start()

    # ---------------- cluster identity ----------------
    def gw_cfg(self, gid: str) -> GatewayCfg | None:
        return next((g for g in self.cfg.gateways if g.id == gid), None)

    def cluster_of(self, gid: str) -> str:
        g, topo = self.gw_cfg(gid), self.topologies.get(gid) or {}
        return ((g.cluster if g else None) or topo.get("cluster_name")
                or (self.cfg.labels.get("cluster") if len(self.gws) <= 1 else None) or gid)

    def labels_for(self, gid: str) -> dict[str, str]:
        g, topo = self.gw_cfg(gid), self.topologies.get(gid) or {}
        lbl = {"cluster": self.cluster_of(gid), "gateway": gid,
               "swarm_id": (g.swarm_id if g else None) or topo.get("swarm_id") or self.cfg.labels.get("swarm_id") or ""}
        if topo.get("cluster_id"):
            lbl["cluster_uid"] = topo["cluster_id"]
        lbl.update((g.labels if g else {}) or {})
        return {k: v for k, v in lbl.items() if v}

    async def _learn_topology(self, gid: str) -> None:
        try:
            doc = await asyncio.wait_for(self.gws[gid].get("/topology"), timeout=8)
            if isinstance(doc, dict):
                self.topologies[gid] = doc
        except (GatewayError, asyncio.TimeoutError, Exception) as e:  # noqa: BLE001
            log.info("topology of gateway %s not available yet: %s", gid, e)

    # ---------------- work ----------------
    async def _loop(self, key: tuple[str, str], i: int) -> None:
        st = self.states[key]
        await asyncio.sleep(min(3.0, 0.1 * i))     # stagger so collectors don't hit gateways at once
        while True:
            await self._run(key)
            st.next_run = time.time() + st.interval
            await asyncio.sleep(st.interval)

    async def run_once(self, cid: str, gid: str | None = None) -> list[Sample]:
        keys = [k for k in self.collectors if k[0] == cid and (gid is None or k[1] == gid)]
        out: list[Sample] = []
        for k in keys:
            out += await self._run(k)
        return out

    async def _run(self, key: tuple[str, str]) -> list[Sample]:
        cid, gid = key
        c, st, gw = self.collectors[key], self.states[key], self.gws[gid]
        t0 = time.perf_counter()
        st.last_run = time.time()
        st.runs += 1
        try:
            samples = await c.collect(gw, self.labels_for(gid))
            st.last_error = None
            self._gateway_up(gid)          # a successful poll proves the gateway is reachable
        except Exception as e:  # noqa: BLE001
            st.failures += 1
            st.last_error = f"{type(e).__name__}: {e}"
            st.last_duration_ms = round((time.perf_counter() - t0) * 1000, 1)
            log.warning("collector %s@%s failed: %s", cid, gid, st.last_error)
            return []
        if c.cfg.type == "topology" and c.state.get("last"):
            self.topologies[gid] = c.state["last"]
        self._record(samples, gid)
        await self._dispatch(c.cfg.sinks, samples, st)
        st.last_count = len(samples)
        st.last_duration_ms = round((time.perf_counter() - t0) * 1000, 1)
        return samples

    async def _dispatch(self, sink_ids: list[str] | None, samples: list[Sample], st: CollectorState) -> None:
        if not samples:
            return
        targets = [s for sid, s in self.sinks.items() if s.cfg.enabled and (sink_ids is None or sid in sink_ids)]
        results = await asyncio.gather(*(s.send(samples) for s in targets), return_exceptions=True)
        st.sink_errors = {s.cfg.id: f"{type(r).__name__}: {r}" for s, r in zip(targets, results)
                          if isinstance(r, Exception)}

    def series_instance(self, s: Sample, gid: str) -> str:
        inst = s.instance or "cluster"
        return f"{s.labels.get('cluster', gid)}/{inst}" if len(self.gws) > 1 else inst

    def _record(self, samples: list[Sample], gid: str) -> None:
        self.total_samples += len(samples)
        for s in samples:
            self.recent.append(s)
            if s.payload is not None:
                continue
            key = (s.metric, self.series_instance(s, gid))
            dq = self.series.get(key)
            if dq is None:
                if len(self.series) >= MAX_SERIES:
                    continue
                dq = self.series[key] = deque(maxlen=POINTS_PER_SERIES)
            dq.append((s.timestamp_ms, s.value))

    def _gateway_up(self, gid: str) -> None:
        st = self.gateway_state.setdefault(gid, {})
        st.update(ok=True, last_ok=time.time(), error=None)

    async def _health_loop(self, gid: str) -> None:
        while True:
            ok = await self.gws[gid].health()
            st = self.gateway_state.setdefault(gid, {})
            if ok:
                self._gateway_up(gid)
                if gid not in self.topologies:
                    await self._learn_topology(gid)
            else:
                st.update(ok=False, error=f"{self.gws[gid].base_url}/health unreachable")
            await asyncio.sleep(30 if ok else 5)   # re-check quickly while it is down

    # ---------------- views for the UI ----------------
    @property
    def gateway_ok(self) -> bool | None:
        vals = [s.get("ok") for s in self.gateway_state.values()]
        return None if not vals or all(v is None for v in vals) else all(v for v in vals if v is not None)

    @property
    def topology(self) -> dict | None:
        return next(iter(self.topologies.values()), None)

    def gateways_status(self) -> list[dict[str, Any]]:
        stale_s = parse_duration(self.cfg.registration.stale_after)
        now = time.time()
        out = []
        for g in self.cfg.gateways:
            st, topo = self.gateway_state.get(g.id, {}), self.topologies.get(g.id) or {}
            hb = self.heartbeats.get(g.id)
            parts = [s for k, s in self.states.items() if k[1] == g.id]
            out.append({
                "id": g.id, "url": g.url, "enabled": g.enabled, "source": g.source,
                "cluster": self.cluster_of(g.id) if g.enabled else (g.cluster or g.id),
                "swarm_id": g.swarm_id or topo.get("swarm_id"), "cluster_uid": g.cluster_uid or topo.get("cluster_id"),
                "ok": st.get("ok"), "last_ok": _iso(st.get("last_ok")), "error": st.get("error"),
                "registered_at": g.registered_at, "last_heartbeat": _iso(hb),
                "stale": bool(g.source == "registered" and hb and now - hb > stale_s),
                "summary": topo.get("summary"), "kubernetes_version": topo.get("kubernetes_version"),
                "collectors": len(parts), "samples_last_run": sum(p.last_count for p in parts),
                "errors": sum(1 for p in parts if p.last_error), "info": g.info,
            })
        return out

    def status(self) -> dict[str, Any]:
        now = time.time()
        window = [s for s in self.recent if s.timestamp_ms >= (now - 60) * 1000]
        rows = []
        for c in self.cfg.collectors:
            parts = [st for (cid, _), st in self.states.items() if cid == c.id]
            rows.append(merge_states(c.id, c.type, self.cfg.interval_of(c), c.enabled, parts))
        gws = self.gateways_status()
        sums = [g["summary"] for g in gws if g.get("summary")]
        topo = self.topology
        return {
            "client": self.cfg.client_name,
            "uptime_s": round(now - self.started_at),
            "gateway": {"url": self.cfg.gateway.url if self.cfg.gateways else None, "ok": self.gateway_ok,
                        "count": len(gws), "up": sum(1 for g in gws if g["ok"])},
            "gateways": gws,
            "total_samples": self.total_samples,
            "samples_last_minute": len(window),
            "collectors": rows,
            "sinks": [s.status() for s in self.sinks.values()],
            "series": len(self.series),
            "clusters": {"count": len(gws), "nodes": sum(s.get("nodes") or 0 for s in sums),
                         "ready": sum(s.get("ready") or 0 for s in sums)},
            "topology": None if not topo else {
                "cluster_id": topo.get("cluster_id"), "swarm_id": topo.get("swarm_id"),
                "cluster_name": topo.get("cluster_name"), "summary": topo.get("summary"), "hash": topo.get("hash")},
        }

    def metrics_index(self) -> list[dict[str, Any]]:
        idx: dict[str, set] = {}
        for (m, inst) in self.series:
            idx.setdefault(m, set()).add(inst)
        return [{"metric": m, "instances": sorted(v)} for m, v in sorted(idx.items())]

    def series_points(self, metric: str, instances: list[str] | None = None, top: int = 6) -> list[dict]:
        rows = [(inst, list(dq)) for (m, inst), dq in self.series.items() if m == metric
                and (not instances or inst in instances)]
        rows.sort(key=lambda r: -(r[1][-1][1] if r[1] else 0))
        return [{"instance": inst, "points": pts} for inst, pts in rows[:top]]
