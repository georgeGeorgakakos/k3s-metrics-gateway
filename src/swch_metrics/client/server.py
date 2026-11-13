"""Sidecar web server: Swarmchestrate-branded UI + REST API for configuration."""
from __future__ import annotations

import base64
import secrets
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
from .config import ConfigStore, validate
from .engine import Engine
from .sinks import describe_sinks

UI_DIR = Path(__file__).parent / "ui"


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
        if not ui.password or request.url.path in ("/healthz",):
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
        return {**eng().status(), "version": __version__, "config_source": store.source}

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
        report = store.save(raw, gateway_put=eng().gw.put_client_config_sync)
        await eng().reload(validate(raw))
        return {"saved": True, "persisted": report}

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
    async def run_now(cid: str):
        if cid not in eng().collectors:
            raise HTTPException(404, f"no collector {cid}")
        samples = await eng().run_once(cid)
        return {"collector": cid, "count": len(samples),
                "samples": [s.to_dict() for s in samples[:50] if s.payload is None]}

    @app.get("/api/samples")
    def samples(collector: str | None = None, q: str | None = None, limit: int = 200):
        items = list(eng().recent)[::-1]
        if collector:
            items = [s for s in items if s.collector == collector]
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
    async def topology():
        if eng().topology:
            return eng().topology
        try:
            return await eng().gw.get("/topology")
        except Exception as e:  # noqa: BLE001
            raise HTTPException(502, str(e)) from e

    @app.get("/", include_in_schema=False)
    def index():
        return FileResponse(UI_DIR / "index.html")

    app.mount("/ui", StaticFiles(directory=UI_DIR), name="ui")
    return app
