"""Entry point: ``swch-metrics-client`` (or ``python -m swch_metrics.client``)."""
from __future__ import annotations

import argparse
import asyncio
import logging
import os

import uvicorn

from .config import ConfigStore, validate
from .engine import Engine
from .server import create_app


def main() -> None:
    ap = argparse.ArgumentParser(description="Swarmchestrate metrics client (EMS sidecar)")
    ap.add_argument("-c", "--config", default=os.getenv("CONFIG_PATH", "/etc/swch/client.yaml"))
    ap.add_argument("--once", action="store_true", help="run every enabled collector once, print, exit")
    ap.add_argument("--print-default-config", action="store_true")
    a = ap.parse_args()
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    store = ConfigStore(a.config)
    if a.print_default_config:
        print(store.to_yaml())
        return
    cfg = store.load()
    logging.getLogger("swch.client").info("config loaded from %s", store.source)

    if a.once:
        async def _once():
            eng = Engine(cfg)
            for cid, c in eng.collectors.items():
                if c.cfg.enabled:
                    samples = await eng.run_once(cid)
                    st = eng.states[cid]
                    print(f"# {cid} ({c.cfg.type}): {len(samples)} samples {st.last_error or ''}")
                    for s in samples[:10]:
                        print(f"  {s.metric} {s.instance or ''} = {s.value} {s.labels}")
            await eng.stop()
        asyncio.run(_once())
        return

    if not cfg.ui.enabled:
        async def _headless():
            eng = Engine(cfg)
            await eng.start()
            await asyncio.Event().wait()
        asyncio.run(_headless())
        return

    app = create_app(store, Engine(validate(store.raw)))
    uvicorn.run(app, host=cfg.ui.host, port=cfg.ui.port, log_level=os.getenv("LOG_LEVEL", "info").lower())


if __name__ == "__main__":
    main()
