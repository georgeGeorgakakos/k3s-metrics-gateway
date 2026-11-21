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
  PUBLIC_URL          gateway URL as reachable from the EMS host. Default: GATEWAY_URL when it is not an
                      in-cluster address, otherwise derived (zero-touch): http://<PUBLIC_HOST><ROOT_PATH>
  PUBLIC_HOST         host[:port] the EMS host uses to reach this cluster's Traefik. Default: ExternalIP
                      (or k3s.io/external-ip) of the first control-plane node, then its InternalIP
  CLUSTER_NAME        cluster name. Default: the topology's cluster name, then the first control-plane node
  REGISTER_INTERVAL   seconds between heartbeats (default 300)

With none of PUBLIC_URL / CLUSTER_NAME set, one identical manifest can be dropped on every new k3s host
(/var/lib/rancher/k3s/server/manifests/): each gateway works out its own URL and name and registers itself.
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


def _internal(url: str | None) -> bool:
    """In-cluster addresses are useless to the EMS host."""
    return not url or ".svc" in url or "://localhost" in url or "://127." in url


def _first_control_plane(topo: dict) -> dict:
    nodes = topo.get("nodes") or []
    cp = [n for n in nodes if {"control-plane", "master"} & set(n.get("roles") or [])]
    return (cp or nodes or [{}])[0]


def public_url(topo: dict) -> str:
    """Gateway URL for the EMS host: PUBLIC_URL > external GATEWAY_URL > http://<node IP><ROOT_PATH>."""
    if os.getenv("PUBLIC_URL"):
        return os.environ["PUBLIC_URL"].rstrip("/")
    for u in (os.getenv("GATEWAY_URL"), (topo.get("gateway") or {}).get("url")):
        if not _internal(u):
            return u.rstrip("/")
    node = _first_control_plane(topo)
    host = os.getenv("PUBLIC_HOST") or node.get("external_ip") or node.get("internal_ip")
    if not host:
        return ""
    path = (os.getenv("ROOT_PATH") or "/swch-metrics").rstrip("/")
    return f"http://{host}{path}"


def cluster_name(topo: dict) -> str | None:
    return (os.getenv("CLUSTER_NAME") or topo.get("cluster_name")
            or _first_control_plane(topo).get("name") or None)


def registration_payload(topo: dict | None) -> dict[str, Any]:
    topo = topo or {}
    summ = topo.get("summary") or {}
    return {
        "schema": "swarmchestrate.eu/gateway-registration/v1",
        "url": public_url(topo),
        "api_key": os.getenv("API_KEY") or None,
        "cluster_name": cluster_name(topo),
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
                self.results[u] = {"status": "error",
                                   "error": "no public URL: set PUBLIC_URL or PUBLIC_HOST (node has no IP in the topology)",
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
