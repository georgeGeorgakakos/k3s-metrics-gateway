#!/usr/bin/env python3
"""Render the k3s auto-deploy manifest for one Swarmchestrate cluster.

The output is a single multi-document YAML meant to be dropped into
``/var/lib/rancher/k3s/server/manifests/`` (which is exactly what
``cluster-builder``'s ``deploy_manifests`` does), so k3s applies it on boot.

    python scripts/render_manifest.py                       # -> deploy/k3s/swch-metrics-gateway.yaml (defaults)
    python scripts/render_manifest.py --swarm-id swarm-42 --cloud aws --region eu-west-1 \
        --api-key auto --ui-password auto --expose-ui nodeport -o manifests/swch-metrics-gateway.yaml
"""
from __future__ import annotations

import argparse
import copy
import secrets
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from swch_metrics import __version__  # noqa: E402
from swch_metrics.client.config import DEFAULT_CONFIG  # noqa: E402

NS = "swarm-system"
PART_OF = {"app.kubernetes.io/part-of": "swarmchestrate"}


def lbl(name: str, component: str) -> dict:
    return {"app.kubernetes.io/name": name, "app.kubernetes.io/component": component,
            "app.kubernetes.io/version": __version__, **PART_OF}


def build(a: argparse.Namespace) -> list[dict]:
    image = a.image or f"ghcr.io/georgegeorgakakos/k3s-metrics-gateway:{__version__}"
    api_key = secrets.token_urlsafe(24) if a.api_key == "auto" else (a.api_key or "")
    ui_pw = secrets.token_urlsafe(12) if a.ui_password == "auto" else (a.ui_password or "")
    gw_url = f"http://swch-metrics-gateway.{NS}.svc.cluster.local:8000"
    ems_namespaces = a.ems_namespaces

    sec_ctx = {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True,
               "runAsNonRoot": True, "runAsUser": 10001, "capabilities": {"drop": ["ALL"]}}

    client_cfg = copy.deepcopy(DEFAULT_CONFIG)
    client_cfg["gateway"]["url"] = "${GATEWAY_URL:-%s}" % gw_url

    sidecar = {
        "container": {
            "name": "swch-metrics-client",
            "image": image,
            "imagePullPolicy": "IfNotPresent",
            "command": ["swch-metrics-client"],
            "ports": [{"name": "swch-ui", "containerPort": 8090, "protocol": "TCP"}],
            "envFrom": [{"configMapRef": {"name": "swch-cluster-identity", "optional": True}}],
            "env": [
                {"name": "CONFIG_PATH", "value": "/etc/swch/client.yaml"},
                {"name": "GATEWAY_URL", "value": gw_url},
                {"name": "EMS_BROKER_HOST", "value": "localhost"},
                {"name": "GATEWAY_API_KEY", "valueFrom": {"secretKeyRef": {
                    "name": "swch-metrics-gateway-auth", "key": "API_KEY", "optional": True}}},
                {"name": "UI_PASSWORD", "valueFrom": {"secretKeyRef": {
                    "name": "swch-metrics-gateway-auth", "key": "UI_PASSWORD", "optional": True}}},
                {"name": "POD_NAMESPACE", "valueFrom": {"fieldRef": {"fieldPath": "metadata.namespace"}}},
                {"name": "POD_IP", "valueFrom": {"fieldRef": {"fieldPath": "status.podIP"}}},
                {"name": "NODE_NAME", "valueFrom": {"fieldRef": {"fieldPath": "spec.nodeName"}}},
            ],
            "volumeMounts": [{"name": "swch-client-config", "mountPath": "/etc/swch", "readOnly": True},
                             {"name": "swch-client-config-saved", "mountPath": "/etc/swch-saved", "readOnly": True},
                             {"name": "swch-client-data", "mountPath": "/data"}],
            "readinessProbe": {"httpGet": {"path": "/healthz", "port": 8090}, "periodSeconds": 10},
            "livenessProbe": {"httpGet": {"path": "/healthz", "port": 8090}, "periodSeconds": 30,
                              "failureThreshold": 5},
            "resources": {"requests": {"cpu": "25m", "memory": "64Mi"},
                          "limits": {"cpu": "300m", "memory": "192Mi"}},
            "securityContext": sec_ctx,
        },
        "volumes": [
            {"name": "swch-client-config", "configMap": {"name": "swch-metrics-client-config", "optional": True}},
            {"name": "swch-client-config-saved",
             "configMap": {"name": "swch-metrics-client-config-saved", "optional": True}},
            {"name": "swch-client-data", "emptyDir": {"sizeLimit": "256Mi"}},
        ],
    }

    a._sidecar = sidecar
    docs: list[dict] = [
        {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": NS, "labels": PART_OF}},
        {"apiVersion": "v1", "kind": "ConfigMap",
         "metadata": {"name": "swch-cluster-identity", "namespace": NS, "labels": PART_OF,
                      "annotations": {"swarmchestrate.eu/description":
                                      "Identity advertised to EMS in the topology and in every metric header"}},
         "data": {"SWARM_ID": a.swarm_id or "", "CLUSTER_NAME": a.cluster_name or "",
                  "CLOUD": a.cloud or "", "REGION": a.region or "", "ZONE": a.zone or ""}},
        {"apiVersion": "v1", "kind": "Secret", "type": "Opaque",
         "metadata": {"name": "swch-metrics-gateway-auth", "namespace": NS, "labels": PART_OF},
         "stringData": {"API_KEY": api_key, "UI_PASSWORD": ui_pw}},
        {"apiVersion": "v1", "kind": "ServiceAccount",
         "metadata": {"name": "swch-metrics-gateway", "namespace": NS, "labels": lbl("swch-metrics-gateway", "gateway")}},
        {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRole",
         "metadata": {"name": "swch-metrics-gateway", "labels": PART_OF},
         "rules": [
             {"apiGroups": ["metrics.k8s.io"], "resources": ["pods", "nodes"], "verbs": ["get", "list"]},
             {"apiGroups": ["custom.metrics.k8s.io", "external.metrics.k8s.io"], "resources": ["*"],
              "verbs": ["get", "list"]},
             {"apiGroups": [""], "resources": ["pods", "nodes", "events", "namespaces", "services", "endpoints"],
              "verbs": ["get", "list", "watch"]},
             {"apiGroups": ["events.k8s.io"], "resources": ["events"], "verbs": ["get", "list"]},
             {"apiGroups": [""], "resources": ["nodes/proxy", "nodes/stats", "nodes/metrics"], "verbs": ["get"]},
             {"nonResourceURLs": ["/metrics", "/livez", "/livez/*", "/readyz", "/readyz/*", "/version"],
              "verbs": ["get"]},
             # topology ConfigMap, sidecar client config + template
             {"apiGroups": [""], "resources": ["configmaps"], "verbs": ["get", "list", "create"]},
             {"apiGroups": [""], "resources": ["configmaps"], "verbs": ["update", "patch"],
              "resourceNames": ["swch-metrics-topology", "swch-metrics-client-config-saved"]},
             # EMS sidecar attachment (only the EMS deployment)
             {"apiGroups": ["apps"], "resources": ["deployments"], "verbs": ["get", "patch"],
              "resourceNames": [a.ems_deployment]},
         ]},
        {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRoleBinding",
         "metadata": {"name": "swch-metrics-gateway", "labels": PART_OF},
         "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": "swch-metrics-gateway"},
         "subjects": [{"kind": "ServiceAccount", "name": "swch-metrics-gateway", "namespace": NS}]},
        {"apiVersion": "v1", "kind": "ConfigMap",
         "metadata": {"name": "swch-metrics-client-config", "namespace": NS, "labels": PART_OF},
         "data": {"client.yaml": yaml.safe_dump(client_cfg, sort_keys=False)}},
        {"apiVersion": "v1", "kind": "ConfigMap",
         "metadata": {"name": "swch-ems-sidecar-template", "namespace": NS, "labels": PART_OF,
                      "annotations": {"swarmchestrate.eu/description":
                                      "Container patched into the EMS Deployment by the gateway"}},
         "data": {"sidecar.yaml": yaml.safe_dump(sidecar, sort_keys=False)}},
        {"apiVersion": "apps/v1", "kind": "Deployment",
         "metadata": {"name": "swch-metrics-gateway", "namespace": NS, "labels": lbl("swch-metrics-gateway", "gateway")},
         "spec": {
             "replicas": 1,
             "strategy": {"type": "Recreate"},
             "selector": {"matchLabels": {"app.kubernetes.io/name": "swch-metrics-gateway"}},
             "template": {
                 "metadata": {"labels": lbl("swch-metrics-gateway", "gateway")},
                 "spec": {
                     "serviceAccountName": "swch-metrics-gateway",
                     "securityContext": {"seccompProfile": {"type": "RuntimeDefault"}},
                     "tolerations": [{"key": "node-role.kubernetes.io/control-plane", "operator": "Exists",
                                      "effect": "NoSchedule"}],
                     "containers": [{
                         "name": "gateway",
                         "image": image,
                         "imagePullPolicy": "IfNotPresent",
                         "command": ["swch-gateway"],
                         "ports": [{"name": "http", "containerPort": 8000}],
                         "envFrom": [{"configMapRef": {"name": "swch-cluster-identity", "optional": True}}],
                         "env": [
                             {"name": "API_KEY", "valueFrom": {"secretKeyRef": {
                                 "name": "swch-metrics-gateway-auth", "key": "API_KEY", "optional": True}}},
                             {"name": "GATEWAY_URL", "value": gw_url},
                             {"name": "POD_NAMESPACE", "valueFrom": {"fieldRef": {"fieldPath": "metadata.namespace"}}},
                             {"name": "TOPOLOGY_ENABLED", "value": "true"},
                             {"name": "TOPOLOGY_INTERVAL", "value": "60"},
                             {"name": "ATTACH_EMS_SIDECAR", "value": "true" if a.attach_sidecar else "false"},
                             {"name": "EMS_NAMESPACES", "value": ems_namespaces},
                             {"name": "EMS_DEPLOYMENT", "value": a.ems_deployment},
                             {"name": "PROMETHEUS_URL", "value": a.prometheus_url or ""},
                         ],
                         "readinessProbe": {"httpGet": {"path": "/health", "port": 8000}, "periodSeconds": 10},
                         "livenessProbe": {"httpGet": {"path": "/health", "port": 8000}, "periodSeconds": 20},
                         "resources": {"requests": {"cpu": "50m", "memory": "96Mi"},
                                       "limits": {"cpu": "500m", "memory": "256Mi"}},
                         "securityContext": sec_ctx,
                     }],
                 },
             },
         }},
        {"apiVersion": "v1", "kind": "Service",
         "metadata": {"name": "swch-metrics-gateway", "namespace": NS, "labels": lbl("swch-metrics-gateway", "gateway"),
                      "annotations": {"swarmchestrate.eu/metrics-api": "true",
                                      "swarmchestrate.eu/topology-configmap": "swch-metrics-topology",
                                      "swarmchestrate.eu/openapi": "/openapi.json"}},
         "spec": {"type": "NodePort" if a.expose_gateway == "nodeport" else "ClusterIP",
                  "selector": {"app.kubernetes.io/name": "swch-metrics-gateway"},
                  "ports": [{"name": "http", "port": 8000, "targetPort": "http",
                             **({"nodePort": 30800} if a.expose_gateway == "nodeport" else {})}]}},
        {"apiVersion": "v1", "kind": "Service",
         "metadata": {"name": "swch-metrics-client-ui", "namespace": NS, "labels": lbl("swch-metrics-client", "sidecar-ui")},
         "spec": {"type": "NodePort" if a.expose_ui == "nodeport" else "ClusterIP",
                  "selector": {"app.kubernetes.io/name": "ems-server"},
                  "ports": [{"name": "ui", "port": 8090, "targetPort": 8090,
                             **({"nodePort": 30890} if a.expose_ui == "nodeport" else {})}]}},
    ]
    return docs


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("-o", "--output", default=str(ROOT / "deploy/k3s/swch-metrics-gateway.yaml"))
    ap.add_argument("--patch-output", default=str(ROOT / "deploy/ems/ems-sidecar-patch.yaml"))
    ap.add_argument("--image")
    ap.add_argument("--swarm-id")
    ap.add_argument("--cluster-name")
    ap.add_argument("--cloud")
    ap.add_argument("--region")
    ap.add_argument("--zone")
    ap.add_argument("--api-key", help="literal key or 'auto' to generate one (empty = no auth)")
    ap.add_argument("--ui-password", help="literal password or 'auto' (empty = no UI auth)")
    ap.add_argument("--expose-gateway", choices=["clusterip", "nodeport"], default="clusterip")
    ap.add_argument("--expose-ui", choices=["clusterip", "nodeport"], default="clusterip")
    ap.add_argument("--ems-namespaces", default="swarm-system,default")
    ap.add_argument("--ems-deployment", default="emsserver-ems-server")
    ap.add_argument("--prometheus-url")
    ap.add_argument("--no-attach-sidecar", dest="attach_sidecar", action="store_false")
    a = ap.parse_args()
    if (a.expose_gateway == "nodeport" and not a.api_key) or (a.expose_ui == "nodeport" and not a.ui_password):
        print("WARNING: exposing via NodePort without --api-key/--ui-password", file=sys.stderr)
    docs = build(a)
    header = (f"# Swarmchestrate k3s Metrics Gateway v{__version__} - generated by scripts/render_manifest.py\n"
              "# Drop into /var/lib/rancher/k3s/server/manifests/ (cluster-builder deploy_manifests) or kubectl apply -f.\n")
    out = Path(a.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(header + yaml.safe_dump_all(docs, sort_keys=False))
    print(f"wrote {out}")
    patch = {"metadata": {"annotations": {"swarmchestrate.eu/metrics-sidecar": "attached"}},
             "spec": {"template": {"metadata": {"annotations": {"swarmchestrate.eu/metrics-sidecar": "attached"}},
                                   "spec": {"containers": [a._sidecar["container"]], "volumes": a._sidecar["volumes"]}}}}
    pp = Path(a.patch_output)
    pp.parent.mkdir(parents=True, exist_ok=True)
    pp.write_text("# Strategic-merge patch adding the metrics-client sidecar to the EMS Deployment.\n"
                  "# Only needed when the gateway's automatic attacher is disabled (--no-attach-sidecar):\n"
                  "#   kubectl -n swarm-system patch deployment emsserver-ems-server --patch-file " + pp.name + "\n"
                  + yaml.safe_dump(patch, sort_keys=False))
    print(f"wrote {pp}")
    if a.api_key == "auto" or a.ui_password == "auto":
        print("generated credentials are in the Secret swch-metrics-gateway-auth inside the file", file=sys.stderr)


if __name__ == "__main__":
    main()
