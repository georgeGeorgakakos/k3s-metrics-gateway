"""Built-in sinks: EMS (STOMP), log, JSONL archive, HTTP webhook."""
from __future__ import annotations

import asyncio
import json
import logging
import os
import socket
import time
import uuid
from pathlib import Path

import httpx

from ..collectors.base import param
from ..model import Sample
from .base import Sink, register_sink

log = logging.getLogger("swch.client.sinks")

CLIENT_NODE_ID = os.getenv("CLIENT_NODE_ID") or str(uuid.uuid5(uuid.NAMESPACE_DNS, socket.gethostname()))


@register_sink
class EmsStompSink(Sink):
    """Publishes every sample to the EMS broker over STOMP.

    Running as a sidecar of the EMS server the broker is on ``localhost:61610``.
    Message shape follows the Swarmchestrate monitoring convention: routing and
    provenance in STOMP headers (metric, instance, node-id, source-endpoint,
    cloud/region/zone ...) and, by default, an EMS event body
    ``{"metricValue": v, "level": 1, "timestamp": ms}``.
    """

    type = "ems_stomp"
    title = "EMS broker (STOMP)"
    description = "Publish to the EMS broker topics so EMS/EPM can use the metrics in its metric model and SLOs."
    options_schema = {
        "host": param("string", "localhost", "Broker host (localhost when running as EMS sidecar)"),
        "port": param("integer", 61610, "STOMP port"),
        "username": param("string", "", "Broker username"),
        "password": param("secret", "", "Broker password"),
        "destination": param("string", "/topic/{metric}",
                             "Destination template; placeholders {metric} {collector} {instance} {cluster}"),
        "instance_template": param("string", "{instance}",
                                   "EMS 'instance' header; e.g. {cluster}/{instance} when node names repeat across clusters"),
        "body_labels": param("list", ["cluster"],
                             "Labels added to the ems_event body (lets OptimusDB ems_events tell clusters apart); empty = none"),
        "body_format": param("string", "ems_event", "Message body",
                             enum=["ems_event", "json", "headers_only"]),
        "level": param("integer", 1, "EMS event level"),
        "include_labels_as_headers": param("boolean", True, "Copy labels as 'label-<name>' headers"),
        "heartbeat_ms": param("integer", 10000, "STOMP heart-beat (0 = off)"),
        "max_messages_per_batch": param("integer", 5000, "Safety cap per collection cycle"),
    }

    def __init__(self, cfg, client_cfg):
        super().__init__(cfg, client_cfg)
        self._conn = None
        self._lock = asyncio.Lock()
        self._next_retry = 0.0

    # ---- connection (blocking stomp.py calls run in a worker thread) ----
    def _connect_sync(self):
        import stomp

        hb = int(self.opts.get("heartbeat_ms") or 0)
        conn = stomp.Connection([(self.opts["host"], int(self.opts["port"]))], heartbeats=(hb, hb),
                                reconnect_attempts_max=1, timeout=10)
        conn.connect(self.opts.get("username") or None, self.opts.get("password") or None, wait=True)
        return conn

    async def _ensure(self):
        if self._conn is not None and self._conn.is_connected():
            return self._conn
        if time.monotonic() < self._next_retry:
            raise ConnectionError(self.last_error or "broker unavailable (backing off)")
        try:
            self._conn = await asyncio.to_thread(self._connect_sync)
            self.connected = True
            log.info("connected to EMS broker %s:%s", self.opts["host"], self.opts["port"])
            return self._conn
        except Exception as e:
            self.connected = False
            self._next_retry = time.monotonic() + 10
            raise ConnectionError(f"{type(e).__name__}: {e}") from e

    def _gateway_url(self, s: Sample) -> str:
        gid = s.labels.get("gateway")
        g = next((g for g in self.client_cfg.gateways if g.id == gid), None) if gid else None
        return (g or self.client_cfg.gateway).url.rstrip("/")

    def instance(self, s: Sample) -> str:
        lbl = s.labels
        inst = s.instance or lbl.get("node") or lbl.get("pod") or "cluster"
        tpl = self.opts.get("instance_template") or "{instance}"
        return tpl.format(instance=inst, cluster=lbl.get("cluster", ""), gateway=lbl.get("gateway", ""))

    def headers(self, s: Sample) -> dict[str, str]:
        lbl = s.labels
        h = {
            "metric": s.metric,
            "instance": self.instance(s),
            "timestamp": str(s.timestamp_ms),
            "node-id": CLIENT_NODE_ID,
            "producer": self.client_cfg.client_name,
            "producer-host": os.getenv("POD_IP", socket.gethostname()),
            "source-node": lbl.get("node") or os.getenv("NODE_NAME", ""),
            "source-endpoint": f"{self._gateway_url(s)}{s.source}",
            "cluster": lbl.get("cluster", ""), "cluster-uid": lbl.get("cluster_uid", ""),
            "gateway": lbl.get("gateway", ""),
            "collector": s.collector,
            "cloud": lbl.get("cloud", ""), "region": lbl.get("region", ""), "zone": lbl.get("zone", ""),
            "swarm-id": lbl.get("swarm_id", ""),
            "content-type": "application/json",
        }
        if self.opts.get("include_labels_as_headers"):
            for k, v in lbl.items():
                h[f"label-{k.replace('_', '-')}"] = str(v)[:256]
        return {k: v for k, v in h.items() if v != ""}

    def body(self, s: Sample) -> str:
        if s.payload is not None:
            return json.dumps(s.payload, default=str)
        fmt = self.opts.get("body_format")
        if fmt == "headers_only":
            return ""
        if fmt == "json":
            return json.dumps(s.to_dict(), default=str)
        body = {"metricValue": s.value, "level": int(self.opts.get("level") or 1), "timestamp": s.timestamp_ms}
        for k in self.opts.get("body_labels") or []:
            if s.labels.get(k):
                body[k] = s.labels[k]
        return json.dumps(body)

    def destination(self, s: Sample) -> str:
        return self.opts["destination"].format(metric=s.metric, collector=s.collector,
                                               instance=s.instance or "cluster", cluster=s.labels.get("cluster", ""))

    async def send(self, samples):
        cap = int(self.opts.get("max_messages_per_batch") or 5000)
        batch = samples[:cap]
        async with self._lock:
            try:
                conn = await self._ensure()
            except ConnectionError as e:
                self.errors += 1
                self.last_error = str(e)
                raise

            def _send_all():
                for s in batch:
                    conn.send(destination=self.destination(s), body=self.body(s), headers=self.headers(s))

            try:
                await asyncio.to_thread(_send_all)
            except Exception as e:
                self.errors += 1
                self.connected = False
                self.last_error = f"{type(e).__name__}: {e}"
                try:
                    conn.disconnect()
                except Exception:  # noqa: BLE001
                    pass
                self._conn = None
                raise
        self.sent += len(batch)
        self.last_error = None
        return len(batch)

    async def stop(self):
        if self._conn is not None:
            try:
                await asyncio.to_thread(self._conn.disconnect)
            except Exception:  # noqa: BLE001
                pass
            self._conn = None


@register_sink
class LogSink(Sink):
    type = "log"
    title = "Container log"
    description = "Write samples to the container log (kubectl logs). Useful for debugging."
    options_schema = {"max_lines": param("integer", 20, "Max lines per batch (0 = all)")}

    async def send(self, samples):
        n = int(self.opts.get("max_lines") or 0)
        for s in samples[: n or None]:
            log.info("%s{%s} %s = %s", s.metric, ",".join(f"{k}={v}" for k, v in s.labels.items()),
                     s.instance or "", s.value)
        if n and len(samples) > n:
            log.info("... %d more samples from %s", len(samples) - n, samples[0].collector)
        self.sent += len(samples)
        return len(samples)


@register_sink
class JsonlSink(Sink):
    type = "jsonl"
    title = "JSONL archive"
    description = "Append samples to a JSON-lines file with size-based rotation."
    options_schema = {"path": param("string", "/data/metrics.jsonl", "File path"),
                      "max_mb": param("integer", 50, "Rotate when larger than this (MiB)")}

    async def send(self, samples):
        p = Path(self.opts["path"])

        def _write():
            p.parent.mkdir(parents=True, exist_ok=True)
            if p.exists() and p.stat().st_size > int(self.opts["max_mb"]) * 2**20:
                p.replace(p.with_suffix(p.suffix + ".1"))
            with p.open("a") as f:
                for s in samples:
                    f.write(json.dumps(s.to_dict(), default=str) + "\n")

        try:
            await asyncio.to_thread(_write)
        except OSError as e:
            self.errors += 1
            self.last_error = str(e)
            raise
        self.sent += len(samples)
        return len(samples)


@register_sink
class WebhookSink(Sink):
    type = "webhook"
    title = "HTTP webhook"
    description = "POST each batch as JSON to an HTTP endpoint (e.g. OptimusDB, a data lake ingest, n8n)."
    options_schema = {"url": param("string", "", "Target URL", required=True),
                      "headers": param("json", {}, "Extra HTTP headers"),
                      "timeout": param("integer", 10, "Timeout (s)")}

    async def start(self):
        hdr = self.opts.get("headers") or {}
        if isinstance(hdr, str):
            hdr = json.loads(hdr or "{}")
        self._http = httpx.AsyncClient(timeout=float(self.opts.get("timeout") or 10), headers=hdr)

    async def send(self, samples):
        if not self.opts.get("url"):
            raise ValueError("webhook url not set")
        body = {"client": self.client_cfg.client_name, "samples": [s.to_dict() for s in samples]}
        try:
            r = await self._http.post(self.opts["url"], json=body)
            r.raise_for_status()
        except httpx.HTTPError as e:
            self.errors += 1
            self.last_error = f"{type(e).__name__}: {e}"
            raise
        self.sent += len(samples)
        return len(samples)

    async def stop(self):
        if getattr(self, "_http", None):
            await self._http.aclose()
