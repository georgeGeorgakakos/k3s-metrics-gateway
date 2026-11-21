"""Sidecar web server: Swarmchestrate-branded UI + REST API for configuration."""
from __future__ import annotations

import base64
import copy
import logging
import secrets
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import yaml
from fastapi import Body, Depends, FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from pydantic import ValidationError

from .. import __version__
from .collectors import describe
from .config import ConfigStore, normalize_raw, slug, validate
from .engine import Engine
from .sinks import describe_sinks

UI_DIR = Path(__file__).parent / "ui"
OPEN_PATHS = ("/healthz", "/api/register")      # /api/register has its own token check
log = logging.getLogger("swch.client.server")

# fields of a registration that are copied into the gateway entry
_REG_FIELDS = ("url", "api_key", "swarm_id", "cluster_uid")
_INFO_FIELDS = ("nodes", "nodes_ready", "node_names", "kubernetes_version", "distribution", "gateway_version",
                "hostname", "cloud", "region", "zone", "topology_hash")


def create_app(store: ConfigStore, engine: Engine | None = None) -> FastAPI:
    state: dict[str, Any] = {"engine": engine}

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        if state["engine"] is None:
            state["engine"] = Engine(validate(store.raw))
        await state["engine"].start()
        yield
        await state["engine"].stop()

    def eng() -> Engine:
        return state["engine"]

    def auth(request: Request):
        ui = validate(store.raw).ui
        if not ui.password or request.url.path in OPEN_PATHS:
            return
        h = request.headers.get("authorization", "")
        if h.startswith("Basic "):
            try:
                user, _, pw = base64.b64decode(h[6:]).decode().partition(":")
            except Exception:  # noqa: BLE001
                user, pw = "", ""
            if secrets.compare_digest(user, ui.username) and secrets.compare_digest(pw, ui.password):
                return
        raise HTTPException(401, "authentication required", headers={"WWW-Authenticate": 'Basic realm="swarmchestrate"'})

    app = FastAPI(title="Swarmchestrate Metrics Client", version=__version__, lifespan=lifespan,
                  dependencies=[Depends(auth)])

    def _validate_raw(raw: dict) -> None:
        try:
            cfg = validate(raw)
        except ValidationError as e:
            raise HTTPException(422, [{"loc": ".".join(map(str, err["loc"])), "msg": err["msg"]}
                                      for err in e.errors()]) from e
        from .collectors import build
        from .sinks import build_sink
        try:
            for c in cfg.collectors:
                build(c, cfg)
            for s in cfg.sinks:
                build_sink(s, cfg)
        except ValueError as e:
            raise HTTPException(422, [{"loc": "collectors/sinks", "msg": str(e)}]) from e

    def _parse(payload: dict) -> dict:
        if "yaml" in payload:
            try:
                raw = yaml.safe_load(payload["yaml"]) or {}
            except yaml.YAMLError as e:
                raise HTTPException(422, [{"loc": "yaml", "msg": str(e)}]) from e
        else:
            raw = payload.get("config")
        if not isinstance(raw, dict):
            raise HTTPException(422, [{"loc": "body", "msg": "expected {yaml: str} or {config: object}"}])
        return raw

    @app.get("/healthz")
    def healthz():
        return {"status": "ok", "version": __version__}

    @app.get("/api/status")
    def status():
        reg = validate(store.raw).registration
        return {**eng().status(), "version": __version__, "config_source": store.source,
                "registration": {"enabled": reg.enabled, "token_configured": bool(reg.effective_token()),
                                 "auto_approve": reg.auto_approve, "stale_after": reg.stale_after,
                                 "path": "api/register"}}

    @app.get("/api/config")
    def get_config():
        return {"config": store.raw, "yaml": store.to_yaml(), "source": store.source}

    @app.post("/api/config/validate")
    def validate_config(payload: dict = Body(...)):
        raw = _parse(payload)
        _validate_raw(raw)
        return {"valid": True}

    @app.put("/api/config")
    async def put_config(payload: dict = Body(...)):
        raw = _parse(payload)
        _validate_raw(raw)
        return {"saved": True, "persisted": await _apply(raw)}

    async def _apply(raw: dict) -> dict:
        gws = eng().gws
        put = next(iter(gws.values())).put_client_config_sync if gws else None
        report = store.save(raw, gateway_put=put)
        await eng().reload(validate(raw))
        return report

    # ------------------------------------------------------------ gateways (multi-cluster)
    @app.get("/api/gateways")
    def gateways():
        return eng().gateways_status()

    @app.delete("/api/gateways/{gid}")
    async def delete_gateway(gid: str):
        raw = normalize_raw(copy.deepcopy(store.raw))
        before = len(raw["gateways"])
        raw["gateways"] = [g for g in raw["gateways"] if g.get("id") != gid]
        if len(raw["gateways"]) == before:
            raise HTTPException(404, f"no gateway {gid}")
        for c in raw.get("collectors", []):
            if c.get("gateways"):
                c["gateways"] = [g for g in c["gateways"] if g != gid] or None
                if c["gateways"] is None:
                    c.pop("gateways")
        _validate_raw(raw)
        eng().heartbeats.pop(gid, None)
        return {"deleted": gid, "persisted": await _apply(raw)}

    @app.post("/api/gateways/{gid}/enabled")
    async def enable_gateway(gid: str, payload: dict = Body(...)):
        raw = normalize_raw(copy.deepcopy(store.raw))
        g = next((g for g in raw["gateways"] if g.get("id") == gid), None)
        if g is None:
            raise HTTPException(404, f"no gateway {gid}")
        g["enabled"] = bool(payload.get("enabled", True))
        _validate_raw(raw)
        return {"id": gid, "enabled": g["enabled"], "persisted": await _apply(raw)}

    @app.post("/api/gateways/{gid}/test")
    async def test_gateway(gid: str):
        g = next((g for g in validate(store.raw).gateways if g.id == gid), None)
        if g is None:
            raise HTTPException(404, f"no gateway {gid}")
        return await _probe(g.model_dump())

    async def _probe(g: dict) -> dict:
        from .config import GatewayCfg
        from .gateway import GatewayClient
        cli = GatewayClient(GatewayCfg(**{k: v for k, v in g.items() if k in GatewayCfg.model_fields}))
        try:
            topo = await cli.get("/topology")
            return {"reachable": True, "authorized": True, "cluster_name": topo.get("cluster_name"),
                    "cluster_uid": topo.get("cluster_id"), "nodes": (topo.get("summary") or {}).get("nodes")}
        except Exception as e:  # noqa: BLE001
            msg = str(e)
            return {"reachable": "HTTP " in msg, "authorized": "HTTP 401" not in msg, "error": msg[:300]}
        finally:
            await cli.aclose()

    @app.post("/api/register")
    async def register(request: Request, payload: dict = Body(...)):
        """Called by a k3s Metrics Gateway on a newly deployed host (self-registration + heartbeat)."""
        reg = validate(store.raw).registration
        token = reg.effective_token()
        if not reg.enabled or not token:
            raise HTTPException(403, "registration is disabled on this EMS sidecar (no REGISTRATION_TOKEN)")
        h = request.headers.get("authorization", "")
        given = h[7:] if h.lower().startswith("bearer ") else request.headers.get("x-registration-token", "")
        if not secrets.compare_digest(given or "", token):
            raise HTTPException(401, "invalid registration token")
        url = str(payload.get("url") or "").rstrip("/")
        if not url.startswith(("http://", "https://")):
            raise HTTPException(422, "url must be the gateway URL reachable from the EMS host (http[s]://...)")
        name = payload.get("cluster_name") or None
        uid = payload.get("cluster_uid") or None

        raw = normalize_raw(copy.deepcopy(store.raw))
        gws = raw["gateways"]
        # 1. same cluster (UID) -> 2. same URL -> 3. new entry
        g = next((x for x in gws if uid and x.get("cluster_uid") == uid), None) \
            or next((x for x in gws if str(x.get("url", "")).rstrip("/") == url), None)
        action = "updated"
        if g is None:
            gid = slug(name or (uid or "")[:8] or url.split("//", 1)[-1].split("/")[0], "cluster")
            base, n = gid, 2
            while any(x.get("id") == gid for x in gws):
                gid, n = f"{base}-{n}"[:63], n + 1
            g = {"id": gid, "enabled": bool(reg.auto_approve)}
            gws.append(g)
            action = "registered" if reg.auto_approve else "pending-approval"
        before = copy.deepcopy(g)
        for k in _REG_FIELDS:
            if payload.get(k) is not None:
                g[k] = str(payload[k]).rstrip("/") if k == "url" else payload[k]
        if name and not g.get("cluster"):
            g["cluster"] = name
        g["source"] = "registered"
        g.setdefault("registered_at", time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
        info = {k: payload.get(k) for k in _INFO_FIELDS if payload.get(k) is not None}
        relevant = {k: before.get(k) for k in (*_REG_FIELDS, "cluster", "enabled", "source")} != \
                   {k: g.get(k) for k in (*_REG_FIELDS, "cluster", "enabled", "source")}
        eng().heartbeats[g["id"]] = time.time()
        if relevant or action != "updated":
            g["info"] = info
            _validate_raw(raw)
            persisted = await _apply(raw)
            log.info("gateway %s %s from %s (cluster=%s)", g["id"], action, url, g.get("cluster"))
        else:
            action, persisted = "heartbeat", None
            live = next((x for x in eng().cfg.gateways if x.id == g["id"]), None)
            if live is not None:
                live.info = info            # refresh details without a reload
        return {"id": g["id"], "action": action, "enabled": g.get("enabled", True), "cluster": g.get("cluster"),
                "persisted": persisted, "poll_interval": validate(store.raw).default_interval}

    @app.get("/api/config/download")
    def download():
        return PlainTextResponse(store.to_yaml(), media_type="application/x-yaml",
                                 headers={"Content-Disposition": "attachment; filename=client.yaml"})

    @app.get("/api/collector-types")
    def collector_types():
        return describe()

    @app.get("/api/sink-types")
    def sink_types():
        return describe_sinks()

    @app.post("/api/collectors/{cid}/run")
    async def run_now(cid: str, gateway: str | None = None):
        if not any(k[0] == cid for k in eng().collectors):
            raise HTTPException(404, f"no collector {cid} (or no enabled gateway)")
        samples = await eng().run_once(cid, gateway)
        return {"collector": cid, "count": len(samples),
                "samples": [s.to_dict() for s in samples[:50] if s.payload is None]}

    @app.get("/api/samples")
    def samples(collector: str | None = None, q: str | None = None, limit: int = 200, cluster: str | None = None):
        items = list(eng().recent)[::-1]
        if collector:
            items = [s for s in items if s.collector == collector]
        if cluster:
            items = [s for s in items if s.labels.get("cluster") == cluster or s.labels.get("gateway") == cluster]
        if q:
            ql = q.lower()
            items = [s for s in items if ql in s.metric.lower() or ql in (s.instance or "").lower()
                     or any(ql in str(v).lower() for v in s.labels.values())]
        return [{k: v for k, v in s.to_dict().items() if k != "payload"} for s in items[:limit]]

    @app.get("/api/metrics")
    def metrics():
        return eng().metrics_index()

    @app.get("/api/series")
    def series(metric: str, instance: list[str] | None = None, top: int = 6):
        return eng().series_points(metric, instance, top)

    @app.get("/api/topology")
    async def topology(gateway: str | None = None):
        e = eng()
        gid = gateway or next(iter(e.gws), None)
        if gid is None:
            raise HTTPException(404, "no gateway configured yet (waiting for registrations)")
        if gid in e.topologies:
            return {**e.topologies[gid], "gateway_id": gid}
        if gid not in e.gws:
            raise HTTPException(404, f"no enabled gateway {gid}")
        try:
            return {**await e.gws[gid].get("/topology"), "gateway_id": gid}
        except Exception as ex:  # noqa: BLE001
            raise HTTPException(502, str(ex)) from ex

    @app.get("/api/trust")
    async def trust(attribute: str | None = None, kind: str | None = None, violations: bool = False,
                    flush: bool = False):
        """Latest trust-evidence documents built by the optimusdb_trust sink(s)."""
        out = []
        for sk in eng().sinks.values():
            if sk.type != "optimusdb_trust":
                continue
            if flush:
                await sk.flush()
            for d in getattr(sk, "last_docs", []):
                if d.get("type") != "trust_evidence":
                    continue
                if attribute and d["attribute_id"] != attribute.upper():
                    continue
                if kind and d["subject_kind"] != kind:
                    continue
                if violations and d.get("slo_met") is not False:
                    continue
                out.append(d)
        return sorted(out, key=lambda d: d["_id"])

    @app.get("/", include_in_schema=False)
    def index():
        return FileResponse(UI_DIR / "index.html")

    app.mount("/ui", StaticFiles(directory=UI_DIR), name="ui")
    return app
