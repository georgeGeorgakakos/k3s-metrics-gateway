"""Thin authenticated HTTP client for the Kubernetes API server.

Works in-cluster (projected ServiceAccount token, rotated automatically) and
outside the cluster through a kubeconfig (k3s.yaml uses client certificates).
"""
from __future__ import annotations

import os
from functools import lru_cache
from typing import Any

import requests
from fastapi import HTTPException

TIMEOUT = float(os.getenv("K8S_TIMEOUT", "15"))
SA_TOKEN = "/var/run/secrets/kubernetes.io/serviceaccount/token"
SA_NAMESPACE = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"


class K8s:
    def __init__(self) -> None:
        from kubernetes import client, config

        try:
            config.load_incluster_config()
            self.in_cluster = True
        except config.ConfigException:
            config.load_kube_config()
            self.in_cluster = False
        c = client.Configuration.get_default_copy()
        self.host = c.host.rstrip("/")
        self.s = requests.Session()
        self.s.verify = c.ssl_ca_cert if c.verify_ssl else False
        if c.cert_file and c.key_file:
            self.s.cert = (c.cert_file, c.key_file)
        auth = c.get_api_key_with_prefix("authorization")
        if auth:
            self.s.headers["Authorization"] = auth

    def _refresh_token(self) -> None:
        if os.path.exists(SA_TOKEN):
            with open(SA_TOKEN) as f:
                self.s.headers["Authorization"] = "Bearer " + f.read().strip()

    def request(self, method: str, path: str, *, params: dict | None = None,
                json: Any = None, content_type: str | None = None) -> requests.Response:
        self._refresh_token()
        headers = {"Content-Type": content_type} if content_type else None
        return self.s.request(method, self.host + path, params=params, json=json,
                              headers=headers, timeout=TIMEOUT)

    def get(self, path: str, params: dict | None = None, text: bool = False):
        r = self.request("GET", path, params=params)
        if r.status_code >= 400:
            raise HTTPException(r.status_code, detail={"k8s_path": path, "error": r.text[:2000]})
        return r.text if text else r.json()


@lru_cache(maxsize=1)
def k8s() -> K8s:
    return K8s()


def own_namespace(default: str = "swarm-system") -> str:
    if os.getenv("POD_NAMESPACE"):
        return os.environ["POD_NAMESPACE"]
    try:
        with open(SA_NAMESPACE) as f:
            return f.read().strip()
    except OSError:
        return default
