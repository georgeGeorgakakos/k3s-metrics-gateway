"""Collector plugin framework.

A collector polls one gateway endpoint on its own interval and turns the JSON
response into :class:`Sample` objects. Register new collectors with
``@register`` (built-ins live in ``builtin.py``; third-party modules can be
loaded through the ``plugins:`` list in the client config).
"""
from __future__ import annotations

from collections import defaultdict
from typing import Any, ClassVar

from ..config import ClientConfig, CollectorCfg
from ..gateway import GatewayClient
from ..model import Sample, now_ms

REGISTRY: dict[str, type["Collector"]] = {}


def register(cls: type["Collector"]) -> type["Collector"]:
    if not cls.type:
        raise ValueError(f"{cls.__name__} has no 'type'")
    REGISTRY[cls.type] = cls
    return cls


def param(kind: str = "string", default: Any = None, description: str = "",
          enum: list[str] | None = None, required: bool = False) -> dict[str, Any]:
    d = {"type": kind, "default": default, "description": description, "required": required}
    if enum:
        d["enum"] = enum
    return d


class Collector:
    # --- metadata (shown in the UI) ---
    type: ClassVar[str] = ""
    title: ClassVar[str] = ""
    description: ClassVar[str] = ""
    endpoint: ClassVar[str] = ""
    default_interval: ClassVar[str] = "30s"
    params_schema: ClassVar[dict[str, dict[str, Any]]] = {}
    outputs: ClassVar[list[str]] = []   # unprefixed metric names it can emit

    def __init__(self, cfg: CollectorCfg, client_cfg: ClientConfig) -> None:
        self.cfg = cfg
        self.client_cfg = client_cfg
        self.params = {k: v.get("default") for k, v in self.params_schema.items()}
        self.params.update(cfg.params or {})
        for k, spec in self.params_schema.items():
            if spec.get("required") and self.params.get(k) in (None, ""):
                raise ValueError(f"collector '{cfg.id}': parameter '{k}' is required")
        self.state: dict[str, Any] = {}

    # --- override points ---
    def path(self) -> str:
        return self.endpoint

    def query(self) -> dict[str, Any]:
        return {}

    def transform(self, data: Any) -> list[Sample]:
        raise NotImplementedError

    # --- pipeline ---
    async def collect(self, gw: GatewayClient) -> list[Sample]:
        path = self.path()
        data = await gw.get(path, self.query())
        ts = now_ms()
        samples = self.transform(data)
        for s in samples:
            s.collector, s.source = self.cfg.id, path
            s.timestamp_ms = s.timestamp_ms or ts
        return self.post_process(samples)

    def post_process(self, samples: list[Sample]) -> list[Sample]:
        c = self.cfg
        if c.include_namespaces:
            samples = [s for s in samples if "namespace" not in s.labels or s.labels["namespace"] in c.include_namespaces]
        if c.exclude_namespaces:
            samples = [s for s in samples if s.labels.get("namespace") not in c.exclude_namespaces]
        if c.metrics:
            samples = [s for s in samples if s.metric in c.metrics]
        if c.aggregate:
            samples += aggregate(samples, c.aggregate)
        prefix = c.metric_prefix if c.metric_prefix is not None else self.client_cfg.metric_prefix
        static = {k: v for k, v in self.client_cfg.labels.items() if v}
        for s in samples:
            s.metric = f"{prefix}{s.metric}"
            s.labels = {**static, **s.labels}
        return samples


def aggregate(samples: list[Sample], ops: list[str]) -> list[Sample]:
    groups: dict[str, list[float]] = defaultdict(list)
    for s in samples:
        if s.payload is None:
            groups[s.metric].append(float(s.value))
    out = []
    fns = {"sum": sum, "avg": lambda v: sum(v) / len(v), "min": min, "max": max, "count": len}
    for metric, vals in groups.items():
        if not vals:
            continue
        for op in ops:
            out.append(Sample(metric=f"{metric}_{op}", value=round(float(fns[op](vals)), 4),
                              labels={"scope": "cluster"}, instance="cluster",
                              collector=samples[0].collector, source=samples[0].source))
    return out


def dig(obj: Any, path: str | None) -> Any:
    """Dotted path lookup: 'data.result' / 'items.0.value'. Empty -> obj."""
    if not path:
        return obj
    for part in path.split("."):
        if isinstance(obj, list):
            obj = obj[int(part)]
        elif isinstance(obj, dict):
            obj = obj.get(part)
        else:
            return None
    return obj


def describe() -> list[dict[str, Any]]:
    return [{"type": c.type, "title": c.title, "description": c.description, "endpoint": c.endpoint,
             "default_interval": c.default_interval, "params": c.params_schema, "outputs": c.outputs}
            for c in sorted(REGISTRY.values(), key=lambda c: c.type)]


def build(cfg: CollectorCfg, client_cfg: ClientConfig) -> Collector:
    cls = REGISTRY.get(cfg.type)
    if not cls:
        raise ValueError(f"unknown collector type '{cfg.type}' (available: {sorted(REGISTRY)})")
    return cls(cfg, client_cfg)
