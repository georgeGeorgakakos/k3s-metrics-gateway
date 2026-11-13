from __future__ import annotations

import time
from dataclasses import asdict, dataclass, field
from typing import Any


def now_ms() -> int:
    return int(time.time() * 1000)


@dataclass
class Sample:
    """One metric value produced by a collector and delivered to sinks."""

    metric: str
    value: float
    labels: dict[str, str] = field(default_factory=dict)
    instance: str | None = None
    timestamp_ms: int = field(default_factory=now_ms)
    collector: str = ""
    source: str = ""                  # gateway path the value came from
    payload: dict[str, Any] | None = None  # structured document (e.g. topology)

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        if d["payload"] is None:
            d.pop("payload")
        return d
