"""Scheduler: one asyncio task per collector, fan-out to sinks, live state for the UI."""
from __future__ import annotations

import asyncio
import importlib
import logging
import time
from collections import deque
from datetime import datetime, timezone
from typing import Any

from .collectors import build
from .config import ClientConfig
from .gateway import GatewayClient
from .model import Sample
from .sinks import build_sink

log = logging.getLogger("swch.client.engine")

MAX_SERIES = 1000
POINTS_PER_SERIES = 360
RECENT_SAMPLES = 2000


def _iso(ts: float | None) -> str | None:
    return datetime.fromtimestamp(ts, timezone.utc).isoformat() if ts else None


class CollectorState:
    def __init__(self, cid: str, ctype: str, interval: float, enabled: bool) -> None:
        self.id, self.type, self.interval, self.enabled = cid, ctype, interval, enabled
        self.runs = 0
        self.failures = 0
        self.last_run: float | None = None
        self.next_run: float | None = None
        self.last_duration_ms: float | None = None
        self.last_count = 0
        self.last_error: str | None = None
        self.sink_errors: dict[str, str] = {}

    def as_dict(self) -> dict[str, Any]:
        status = "disabled" if not self.enabled else ("error" if self.last_error else
                                                       ("ok" if self.runs else "pending"))
        return {"id": self.id, "type": self.type, "interval_s": self.interval, "enabled": self.enabled,
                "runs": self.runs, "failures": self.failures, "last_run": _iso(self.last_run),
                "next_run": _iso(self.next_run), "last_duration_ms": self.last_duration_ms,
                "last_count": self.last_count, "last_error": self.last_error,
                "sink_errors": self.sink_errors, "status": status}


class Engine:
    def __init__(self, cfg: ClientConfig) -> None:
        self.started_at = time.time()
        self.recent: deque[Sample] = deque(maxlen=RECENT_SAMPLES)
        self.series: dict[tuple[str, str], deque] = {}
        self.total_samples = 0
        self.gateway_ok: bool | None = None
        self.topology: dict | None = None
        self._tasks: list[asyncio.Task] = []
        self._lock = asyncio.Lock()
        self._apply(cfg)

    # ---------------- lifecycle ----------------
    def _apply(self, cfg: ClientConfig) -> None:
        for mod in cfg.plugins:
            importlib.import_module(mod)  # plugin modules self-register collectors/sinks
        self.cfg = cfg
        self.gw = GatewayClient(cfg.gateway)
        self.sinks = {s.id: build_sink(s, cfg) for s in cfg.sinks}
        self.collectors = {c.id: build(c, cfg) for c in cfg.collectors}
        self.states = {c.id: CollectorState(c.id, c.type, cfg.interval_of(c), c.enabled)
                       for c in cfg.collectors}

    async def start(self) -> None:
        for s in self.sinks.values():
            if s.cfg.enabled:
                try:
                    await s.start()
                except Exception as e:  # noqa: BLE001
                    s.last_error = f"start failed: {e}"
        for cid, c in self.collectors.items():
            if c.cfg.enabled:
                self._tasks.append(asyncio.create_task(self._loop(cid), name=f"collector:{cid}"))
        self._tasks.append(asyncio.create_task(self._health_loop(), name="gateway-health"))
        log.info("engine started: %d collectors, %d sinks", len(self.collectors), len(self.sinks))

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
        await self.gw.aclose()

    async def reload(self, cfg: ClientConfig) -> None:
        async with self._lock:
            await self.stop()
            self._apply(cfg)
            await self.start()

    # ---------------- work ----------------
    async def _loop(self, cid: str) -> None:
        st = self.states[cid]
        # small stagger so collectors don't all hit the gateway at once
        await asyncio.sleep(min(2.0, 0.1 * list(self.collectors).index(cid)))
        while True:
            await self.run_once(cid)
            st.next_run = time.time() + st.interval
            await asyncio.sleep(st.interval)

    async def run_once(self, cid: str) -> list[Sample]:
        c, st = self.collectors[cid], self.states[cid]
        t0 = time.perf_counter()
        st.last_run = time.time()
        st.runs += 1
        try:
            samples = await c.collect(self.gw)
            st.last_error = None
        except Exception as e:  # noqa: BLE001
            st.failures += 1
            st.last_error = f"{type(e).__name__}: {e}"
            st.last_duration_ms = round((time.perf_counter() - t0) * 1000, 1)
            log.warning("collector %s failed: %s", cid, st.last_error)
            return []
        if c.cfg.type == "topology" and c.state.get("last"):
            self.topology = c.state["last"]
        self._record(samples)
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

    def _record(self, samples: list[Sample]) -> None:
        self.total_samples += len(samples)
        for s in samples:
            self.recent.append(s)
            if s.payload is not None:
                continue
            key = (s.metric, s.instance or "cluster")
            dq = self.series.get(key)
            if dq is None:
                if len(self.series) >= MAX_SERIES:
                    continue
                dq = self.series[key] = deque(maxlen=POINTS_PER_SERIES)
            dq.append((s.timestamp_ms, s.value))

    async def _health_loop(self) -> None:
        while True:
            self.gateway_ok = await self.gw.health()
            await asyncio.sleep(30)

    # ---------------- views for the UI ----------------
    def status(self) -> dict[str, Any]:
        now = time.time()
        window = [s for s in self.recent if s.timestamp_ms >= (now - 60) * 1000]
        return {
            "client": self.cfg.client_name,
            "uptime_s": round(now - self.started_at),
            "gateway": {"url": self.cfg.gateway.url, "ok": self.gateway_ok},
            "total_samples": self.total_samples,
            "samples_last_minute": len(window),
            "collectors": [st.as_dict() for st in self.states.values()],
            "sinks": [s.status() for s in self.sinks.values()],
            "series": len(self.series),
            "topology": None if not self.topology else {
                "cluster_id": self.topology.get("cluster_id"), "swarm_id": self.topology.get("swarm_id"),
                "summary": self.topology.get("summary"), "hash": self.topology.get("hash")},
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
