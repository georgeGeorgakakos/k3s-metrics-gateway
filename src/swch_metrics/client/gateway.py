"""Async HTTP client for the k3s Metrics Gateway."""
from __future__ import annotations

from typing import Any

import httpx

from .config import GatewayCfg


class GatewayError(RuntimeError):
    pass


class GatewayClient:
    transport: httpx.AsyncBaseTransport | None = None  # injectable for tests

    def __init__(self, cfg: GatewayCfg) -> None:
        self.cfg = cfg
        headers = {"X-API-Key": cfg.api_key} if cfg.api_key else {}
        self._http = httpx.AsyncClient(base_url=cfg.url.rstrip("/"), headers=headers,
                                       timeout=cfg.timeout, verify=cfg.verify_tls,
                                       transport=self.transport)

    @property
    def base_url(self) -> str:
        return self.cfg.url.rstrip("/")

    async def get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        params = {k: v for k, v in (params or {}).items() if v not in (None, "", [])}
        try:
            r = await self._http.get(path, params=params)
        except httpx.HTTPError as e:
            raise GatewayError(f"{type(e).__name__}: {e}") from e
        if r.status_code >= 400:
            raise GatewayError(f"HTTP {r.status_code} on {path}: {r.text[:300]}")
        ctype = r.headers.get("content-type", "")
        return r.json() if "json" in ctype else r.text

    async def health(self) -> bool:
        try:
            await self.get("/health")
            return True
        except GatewayError:
            return False

    def put_client_config_sync(self, namespace: str, text: str) -> None:
        headers = {"X-API-Key": self.cfg.api_key} if self.cfg.api_key else {}
        r = httpx.put(f"{self.base_url}/client-config/{namespace}", json={"client.yaml": text},
                      headers=headers, timeout=self.cfg.timeout, verify=self.cfg.verify_tls)
        if r.status_code >= 400:
            raise GatewayError(f"HTTP {r.status_code}: {r.text[:300]}")

    async def aclose(self) -> None:
        await self._http.aclose()
