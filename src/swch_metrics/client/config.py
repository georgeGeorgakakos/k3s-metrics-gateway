"""Client configuration: schema, env expansion, loading and persistence."""
from __future__ import annotations

import copy
import os
import re
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator

_ENV = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")
_DUR = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*(ms|s|sec|m|min|h)?\s*$", re.I)

MIN_INTERVAL = 1.0


def parse_duration(v: Any) -> float:
    """'15s' | '1m' | '500ms' | '2h' | 30 -> seconds."""
    if isinstance(v, (int, float)):
        return float(v)
    m = _DUR.match(str(v))
    if not m:
        raise ValueError(f"invalid duration {v!r} (use e.g. 15s, 1m, 500ms)")
    n, unit = float(m.group(1)), (m.group(2) or "s").lower()
    return n * {"ms": 0.001, "s": 1, "sec": 1, "m": 60, "min": 60, "h": 3600}[unit]


def expand_env(obj: Any) -> Any:
    """Recursively expand ${VAR} and ${VAR:-default} in strings."""
    if isinstance(obj, str):
        return _ENV.sub(lambda m: os.getenv(m.group(1), m.group(2) or ""), obj)
    if isinstance(obj, list):
        return [expand_env(i) for i in obj]
    if isinstance(obj, dict):
        return {k: expand_env(v) for k, v in obj.items()}
    return obj


class GatewayCfg(BaseModel):
    url: str = "http://swch-metrics-gateway.swarm-system.svc.cluster.local:8000"
    api_key: str | None = None
    timeout: float = 10.0
    verify_tls: bool = True


class UiCfg(BaseModel):
    enabled: bool = True
    host: str = "0.0.0.0"
    port: int = 8090
    username: str = "admin"
    password: str | None = None  # empty -> no auth (cluster-internal access only)


class PersistCfg(BaseModel):
    mode: Literal["gateway", "file", "none"] = "gateway"
    path: str = "/data/client.yaml"
    namespace: str | None = None  # defaults to POD_NAMESPACE


class SinkCfg(BaseModel):
    id: str
    type: str
    enabled: bool = True
    options: dict[str, Any] = Field(default_factory=dict)


Agg = Literal["sum", "avg", "min", "max", "count"]


class CollectorCfg(BaseModel):
    id: str
    type: str
    enabled: bool = True
    interval: str | float | None = None
    params: dict[str, Any] = Field(default_factory=dict)
    sinks: list[str] | None = None            # None -> all enabled sinks
    metric_prefix: str | None = None          # None -> global prefix
    include_namespaces: list[str] = Field(default_factory=list)
    exclude_namespaces: list[str] = Field(default_factory=list)
    metrics: list[str] = Field(default_factory=list)  # whitelist of output names
    aggregate: list[Agg] = Field(default_factory=list)

    @field_validator("id")
    @classmethod
    def _id(cls, v: str) -> str:
        if not re.fullmatch(r"[a-z0-9][a-z0-9_\-]{0,62}", v):
            raise ValueError("id must be lowercase letters, digits, '-' or '_' (max 63)")
        return v

    @field_validator("interval")
    @classmethod
    def _interval(cls, v):
        if v is not None and parse_duration(v) < MIN_INTERVAL:
            raise ValueError(f"interval must be >= {MIN_INTERVAL}s")
        return v


class ClientConfig(BaseModel):
    client_name: str = "swch-metrics-client"
    default_interval: str | float = "30s"
    metric_prefix: str = "swch_k3s_"
    labels: dict[str, str] = Field(default_factory=dict)
    gateway: GatewayCfg = Field(default_factory=GatewayCfg)
    ui: UiCfg = Field(default_factory=UiCfg)
    persist: PersistCfg = Field(default_factory=PersistCfg)
    plugins: list[str] = Field(default_factory=list)
    sinks: list[SinkCfg] = Field(default_factory=list)
    collectors: list[CollectorCfg] = Field(default_factory=list)

    @model_validator(mode="after")
    def _unique(self):
        for kind, items in (("sink", self.sinks), ("collector", self.collectors)):
            ids = [i.id for i in items]
            dup = {i for i in ids if ids.count(i) > 1}
            if dup:
                raise ValueError(f"duplicate {kind} id(s): {sorted(dup)}")
        sink_ids = {s.id for s in self.sinks}
        for c in self.collectors:
            for s in c.sinks or []:
                if s not in sink_ids:
                    raise ValueError(f"collector '{c.id}' references unknown sink '{s}'")
        parse_duration(self.default_interval)
        return self

    def interval_of(self, c: CollectorCfg) -> float:
        return parse_duration(c.interval if c.interval is not None else self.default_interval)


DEFAULT_CONFIG: dict[str, Any] = {
    "client_name": "swch-metrics-client",
    "default_interval": "30s",
    "metric_prefix": "swch_k3s_",
    "labels": {"swarm_id": "${SWARM_ID:-}", "cluster": "${CLUSTER_NAME:-}",
               "cloud": "${CLOUD:-}", "region": "${REGION:-}", "zone": "${ZONE:-}"},
    "gateway": {"url": "${GATEWAY_URL:-http://swch-metrics-gateway.swarm-system.svc.cluster.local:8000}",
                "api_key": "${GATEWAY_API_KEY:-}", "timeout": 10},
    "ui": {"enabled": True, "port": 8090, "username": "admin", "password": "${UI_PASSWORD:-}"},
    "persist": {"mode": "gateway", "path": "/data/client.yaml"},
    "sinks": [
        {"id": "ems", "type": "ems_stomp", "enabled": True,
         "options": {"host": "${EMS_BROKER_HOST:-localhost}", "port": 61610,
                     "username": "${EMS_BROKER_USERNAME:-aaa}", "password": "${EMS_BROKER_PASSWORD:-111}",
                     "destination": "/topic/{metric}", "body_format": "ems_event"}},
        {"id": "log", "type": "log", "enabled": False, "options": {"max_lines": 20}},
    ],
    "collectors": [
        {"id": "nodes", "type": "node_usage", "interval": "30s"},
        {"id": "pods", "type": "pod_usage", "interval": "30s", "exclude_namespaces": ["kube-system"]},
        {"id": "pod-utilisation", "type": "pod_overview", "interval": "60s",
         "exclude_namespaces": ["kube-system"],
         "metrics": ["pod_cpu_pct_of_limit", "pod_memory_pct_of_limit", "pod_restarts"]},
        {"id": "warnings", "type": "events", "interval": "60s", "params": {"type": "Warning"}},
        {"id": "topology", "type": "topology", "interval": "2m"},
    ],
}


def validate(raw: dict[str, Any]) -> ClientConfig:
    return ClientConfig.model_validate(expand_env(copy.deepcopy(raw)))


class ConfigStore:
    """Holds the raw (unexpanded) config so secrets stay as ${VAR} when saved."""

    def __init__(self, base_path: str | None = None) -> None:
        self.base_path = base_path or os.getenv("CONFIG_PATH", "/etc/swch/client.yaml")
        self.source = "builtin"
        self.raw: dict[str, Any] = copy.deepcopy(DEFAULT_CONFIG)

    def load(self) -> ClientConfig:
        # 1. saved from the UI during this pod's lifetime
        # 2. saved from the UI earlier (ConfigMap swch-metrics-client-config-saved, mounted)
        # 3. defaults shipped with the deployment (ConfigMap swch-metrics-client-config)
        candidates = [os.getenv("CONFIG_DATA_PATH", "/data/client.yaml"),
                      os.getenv("CONFIG_SAVED_PATH", "/etc/swch-saved/client.yaml"),
                      self.base_path]
        for p in candidates:
            if p and Path(p).is_file():
                data = yaml.safe_load(Path(p).read_text()) or {}
                if isinstance(data, dict) and data:
                    self.raw, self.source = data, p
                    break
        return validate(self.raw)

    def to_yaml(self, raw: dict | None = None) -> str:
        return yaml.safe_dump(raw if raw is not None else self.raw, sort_keys=False, allow_unicode=True)

    def save(self, raw: dict[str, Any], gateway_put=None) -> dict[str, Any]:
        """Validate and persist. Returns a report of where it was written."""
        cfg = validate(raw)
        text = self.to_yaml(raw)
        report: dict[str, Any] = {"file": None, "gateway": None}
        try:
            p = Path(cfg.persist.path)
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(text)
            report["file"] = str(p)
        except OSError as e:
            report["file"] = f"error: {e}"
        if cfg.persist.mode == "gateway" and gateway_put is not None:
            try:
                ns = cfg.persist.namespace or os.getenv("POD_NAMESPACE", "swarm-system")
                gateway_put(ns, text)
                report["gateway"] = f"ConfigMap swch-metrics-client-config-saved in {ns}"
            except Exception as e:  # noqa: BLE001
                report["gateway"] = f"error: {e}"
        self.raw, self.source = raw, report["file"] or self.source
        return report
