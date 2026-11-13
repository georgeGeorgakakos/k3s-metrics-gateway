"""Sink plugin framework: where collected samples are delivered."""
from __future__ import annotations

from typing import Any, ClassVar

from ..config import ClientConfig, SinkCfg
from ..model import Sample

SINKS: dict[str, type["Sink"]] = {}


def register_sink(cls: type["Sink"]) -> type["Sink"]:
    SINKS[cls.type] = cls
    return cls


class Sink:
    type: ClassVar[str] = ""
    title: ClassVar[str] = ""
    description: ClassVar[str] = ""
    options_schema: ClassVar[dict[str, dict[str, Any]]] = {}

    def __init__(self, cfg: SinkCfg, client_cfg: ClientConfig) -> None:
        self.cfg, self.client_cfg = cfg, client_cfg
        self.opts = {k: v.get("default") for k, v in self.options_schema.items()}
        self.opts.update(cfg.options or {})
        self.sent = 0
        self.errors = 0
        self.last_error: str | None = None
        self.connected: bool | None = None

    async def start(self) -> None:  # noqa: B027
        pass

    async def stop(self) -> None:  # noqa: B027
        pass

    async def send(self, samples: list[Sample]) -> int:
        raise NotImplementedError

    def status(self) -> dict[str, Any]:
        return {"id": self.cfg.id, "type": self.type, "enabled": self.cfg.enabled, "sent": self.sent,
                "errors": self.errors, "last_error": self.last_error, "connected": self.connected}


def describe_sinks() -> list[dict[str, Any]]:
    return [{"type": s.type, "title": s.title, "description": s.description, "options": s.options_schema}
            for s in sorted(SINKS.values(), key=lambda s: s.type)]


def build_sink(cfg: SinkCfg, client_cfg: ClientConfig) -> Sink:
    cls = SINKS.get(cfg.type)
    if not cls:
        raise ValueError(f"unknown sink type '{cfg.type}' (available: {sorted(SINKS)})")
    return cls(cfg, client_cfg)
