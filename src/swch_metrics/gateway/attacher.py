"""EMS sidecar attacher.

EMS is (re)deployed by ``swchmonclient`` at any time after the cluster is up,
so the sidecar cannot be baked into the k3s auto-deploy manifest. The gateway
therefore watches for the EMS Deployment and, when it appears without the
metrics-client container, patches it in (strategic merge, idempotent).

The container/volume spec comes from the ConfigMap ``swch-ems-sidecar-template``
(key ``sidecar.yaml``) so operators can tune it without rebuilding images.
"""
from __future__ import annotations

import logging
import os
import threading
from typing import Any

import yaml

from .k8s import K8s, own_namespace

log = logging.getLogger("swch.gateway.attacher")

SIDECAR_NAME = os.getenv("SIDECAR_NAME", "swch-metrics-client")
TEMPLATE_CM = os.getenv("SIDECAR_TEMPLATE_CONFIGMAP", "swch-ems-sidecar-template")
CLIENT_CONFIG_CM = os.getenv("CLIENT_CONFIG_CONFIGMAP", "swch-metrics-client-config")
# Saved from the UI. Kept separate so k3s re-applying the auto-deploy manifest never overwrites it.
CLIENT_SAVED_CM = os.getenv("CLIENT_SAVED_CONFIGMAP", "swch-metrics-client-config-saved")
ANNOTATION = "swarmchestrate.eu/metrics-sidecar"


def ems_targets() -> list[tuple[str, str]]:
    namespaces = [n.strip() for n in os.getenv("EMS_NAMESPACES", "swarm-system,default").split(",") if n.strip()]
    name = os.getenv("EMS_DEPLOYMENT", "emsserver-ems-server")
    return [(ns, name) for ns in namespaces]


def load_template(k: K8s) -> dict[str, Any]:
    cm = k.get(f"/api/v1/namespaces/{own_namespace()}/configmaps/{TEMPLATE_CM}")
    tpl = yaml.safe_load(cm["data"]["sidecar.yaml"])
    if "container" not in tpl:
        raise ValueError("sidecar.yaml must contain 'container' (and optionally 'volumes')")
    tpl["container"]["name"] = SIDECAR_NAME
    return tpl


def has_sidecar(deploy: dict) -> bool:
    cs = deploy["spec"]["template"]["spec"].get("containers", [])
    return any(c.get("name") == SIDECAR_NAME for c in cs)


def build_patch(template: dict[str, Any]) -> dict[str, Any]:
    spec: dict[str, Any] = {"containers": [template["container"]]}
    if template.get("volumes"):
        spec["volumes"] = template["volumes"]
    return {
        "metadata": {"annotations": {ANNOTATION: "attached"}},
        "spec": {"template": {
            "metadata": {"annotations": {ANNOTATION: "attached"}},
            "spec": spec,
        }},
    }


def ensure_client_config(k: K8s, target_ns: str) -> None:
    """Copy the client ConfigMap into the EMS namespace when it is not the gateway's."""
    src_ns = own_namespace()
    if target_ns == src_ns:
        return
    path = f"/api/v1/namespaces/{target_ns}/configmaps/{CLIENT_CONFIG_CM}"
    if k.request("GET", path).status_code == 200:
        return
    src = k.get(f"/api/v1/namespaces/{src_ns}/configmaps/{CLIENT_CONFIG_CM}")
    body = {"apiVersion": "v1", "kind": "ConfigMap",
            "metadata": {"name": CLIENT_CONFIG_CM, "namespace": target_ns,
                         "labels": src["metadata"].get("labels", {})},
            "data": src.get("data", {})}
    k.request("POST", f"/api/v1/namespaces/{target_ns}/configmaps", json=body).raise_for_status()


def attach_once(k: K8s) -> list[dict[str, str]]:
    results = []
    template = None
    for ns, name in ems_targets():
        path = f"/apis/apps/v1/namespaces/{ns}/deployments/{name}"
        r = k.request("GET", path)
        if r.status_code == 404:
            continue
        if r.status_code >= 400:
            results.append({"namespace": ns, "deployment": name, "status": f"error {r.status_code}"})
            continue
        deploy = r.json()
        if has_sidecar(deploy):
            results.append({"namespace": ns, "deployment": name, "status": "present"})
            continue
        template = template or load_template(k)
        ensure_client_config(k, ns)
        pr = k.request("PATCH", path, json=build_patch(template),
                       content_type="application/strategic-merge-patch+json")
        if pr.status_code >= 400:
            results.append({"namespace": ns, "deployment": name, "status": f"patch failed {pr.status_code}: {pr.text[:300]}"})
            log.warning("sidecar patch failed for %s/%s: %s", ns, name, pr.text[:300])
        else:
            results.append({"namespace": ns, "deployment": name, "status": "attached"})
            log.info("attached %s sidecar to %s/%s", SIDECAR_NAME, ns, name)
    return results


class SidecarAttacher(threading.Thread):
    def __init__(self, k_factory, interval: float = 30.0) -> None:
        super().__init__(daemon=True, name="ems-sidecar-attacher")
        self.k_factory, self.interval = k_factory, interval
        self.last: list[dict[str, str]] = []
        self.last_error: str | None = None
        self._stop = threading.Event()

    def run(self) -> None:
        while not self._stop.is_set():
            try:
                self.last = attach_once(self.k_factory())
                self.last_error = None
            except Exception as e:  # noqa: BLE001
                self.last_error = f"{type(e).__name__}: {e}"
                log.warning("sidecar attacher: %s", self.last_error)
            self._stop.wait(self.interval)

    def stop(self) -> None:
        self._stop.set()
