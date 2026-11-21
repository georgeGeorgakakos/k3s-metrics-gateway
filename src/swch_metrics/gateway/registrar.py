"""Self-registration of this gateway with one or more EMS metrics sidecars.

When a new k3s host is deployed with the gateway, the gateway announces itself
to the EMS sidecar (``POST <REGISTER_URL>``). The sidecar adds it to its
``gateways:`` list and starts polling it, so no manual step is needed on the
EMS host. The announcement is repeated every ``REGISTER_INTERVAL`` seconds as a
heartbeat (idempotent: the sidecar only reloads when something changed).

Environment:
  REGISTER_URL        comma-separated sidecar registration URLs,
                      e.g. http://193.225.251.34/swch-metrics-ui/api/register
  REGISTER_TOKEN      shared registration token (must match the sidecar's REGISTRATION_TOKEN)
  PUBLIC_URL          gateway URL as reachable from the EMS host (default: GATEWAY_URL)
  REGISTER_INTERVAL   seconds between heartbeats (default 300)
"""
from __future__ import annotations

import logging
import os
import socket
import threading
from datetime import datetime, timezone
from typing import Any, Callable

import requests

from .. import __version__

log = logging.getLogger("swch.gateway.registrar")


def registration_payload(topo: dict | None) -> dict[str, Any]:
    topo = topo or {}
    gw = topo.get("gateway") or {}
    summ = topo.get("summary") or {}
    return {
        "schema": "swarmchestrate.eu/gateway-registration/v1",
        "url": (os.getenv("PUBLIC_URL") or gw.get("url") or os.getenv("GATEWAY_URL") or "").rstrip("/"),
        "api_key": os.getenv("API_KEY") or None,
        "cluster_name": os.getenv("CLUSTER_NAME") or topo.get("cluster_name"),
        "cluster_uid": topo.get("cluster_id"),
        "swarm_id": os.getenv("SWARM_ID") or topo.get("swarm_id"),
        "cloud": os.getenv("CLOUD") or None,
        "region": os.getenv("REGION") or None,
        "zone": os.getenv("ZONE") or None,
        "kubernetes_version": topo.get("kubernetes_version"),
        "distribution": topo.get("distribution"),
        "nodes": summ.get("nodes"),
        "nodes_ready": summ.get("ready"),
        "node_names": [n.get("name") for n in topo.get("nodes", [])][:50],
        "topology_hash": topo.get("hash"),
        "gateway_version": __version__,
        "hostname": socket.gethostname(),
    }


class Registrar(threading.Thread):
    """Background loop: POST the registration to every REGISTER_URL."""

    def __init__(self, topo_fn: Callable[[], dict | None], urls: list[str], token: str | None,
                 interval: float = 300.0, retry: float = 30.0) -> None:
        super().__init__(daemon=True, name="registrar")
        self.topo_fn, self.urls, self.token = topo_fn, urls, token
        self.interval, self.retry = interval, retry
        self.results: dict[str, dict[str, Any]] = {u: {"status": "pending"} for u in urls}
        self._stop = threading.Event()

    @classmethod
    def from_env(cls, topo_fn) -> "Registrar | None":
        urls = [u.strip() for u in os.getenv("REGISTER_URL", "").split(",") if u.strip()]
        if not urls:
            return None
        return cls(topo_fn, urls, os.getenv("REGISTER_TOKEN") or None,
                   float(os.getenv("REGISTER_INTERVAL", "300")))

    def register_once(self) -> bool:
        payload = registration_payload(self.topo_fn())
        if not payload["url"]:
            for u in self.urls:
                self.results[u] = {"status": "error", "error": "PUBLIC_URL / GATEWAY_URL not set",
                                   "at": _now()}
            return False
        headers = {"Authorization": f"Bearer {self.token}"} if self.token else {}
        ok_all = True
        for u in self.urls:
            try:
                r = requests.post(u, json=payload, headers=headers, timeout=15)
                body = r.json() if "json" in r.headers.get("content-type", "") else {"text": r.text[:300]}
                if r.status_code >= 400:
                    raise RuntimeError(f"HTTP {r.status_code}: {body}")
                self.results[u] = {"status": "registered", "at": _now(), "response": body}
                log.info("registered with %s as '%s' (%s)", u, body.get("id"), body.get("action"))
            except Exception as e:  # noqa: BLE001
                ok_all = False
                self.results[u] = {"status": "error", "at": _now(), "error": f"{type(e).__name__}: {e}"[:400]}
                log.warning("registration with %s failed: %s", u, self.results[u]["error"])
        return ok_all

    def run(self) -> None:
        self._stop.wait(5)      # let the topology publisher produce a first document
        while not self._stop.is_set():
            ok = self.register_once()
            self._stop.wait(self.interval if ok else self.retry)

    def stop(self) -> None:
        self._stop.set()

    def status(self) -> dict[str, Any]:
        return {"targets": self.results, "interval_s": self.interval}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")
