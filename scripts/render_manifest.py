#!/usr/bin/env python3
"""Render the k3s auto-deploy manifest for one Swarmchestrate cluster.

The output is a single multi-document YAML meant to be dropped into
``/var/lib/rancher/k3s/server/manifests/`` (which is exactly what
``cluster-builder``'s ``deploy_manifests`` does), so k3s applies it on boot.

    python scripts/render_manifest.py                       # -> deploy/k3s/swch-metrics-gateway.yaml (defaults)
    python scripts/render_manifest.py --swarm-id swarm-42 --cloud aws --region eu-west-1 \
        --api-key auto --ui-password auto --expose-ui nodeport -o manifests/swch-metrics-gateway.yaml

Multi-cluster (one EMS sidecar, many gateways):
    # EMS host: sidecar accepting registrations
    python scripts/render_manifest.py --role ems --namespace default --expose-ui ingress \
        --ui-password auto --register-token auto -o hostB-ems.yaml
    # every k3s host: gateway that registers itself with the EMS sidecar
    python scripts/render_manifest.py --role gateway --expose-gateway ingress --api-key auto \
        --cluster-name edge-07 --swarm-id swarm-01 --gateway-url http://<this-host>/swch-metrics \
        --register-url http://<ems-host>/swch-metrics-ui/api/register --register-token <token> -o host-gw.yaml
    # or: deploy/onboard/onboard-k3s-host.sh (kubectl + curl only, uses --onboard-template)
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


def ingress_docs(a: argparse.Namespace, ns: str) -> list[dict]:
    """Traefik (k3s default ingress) routes on the 'web' entrypoint (port 80)."""
    out: list[dict] = []

    def mw(name: str, spec: dict) -> None:
        out.append({"apiVersion": "traefik.io/v1alpha1", "kind": "Middleware",
                    "metadata": {"name": name, "namespace": ns, "labels": PART_OF}, "spec": spec})

    def route(name: str, path: str, service: str, port: int, mws: list[str]) -> None:
        out.append({"apiVersion": "traefik.io/v1alpha1", "kind": "IngressRoute",
                    "metadata": {"name": name, "namespace": ns, "labels": PART_OF},
                    "spec": {"entryPoints": ["web"],
                             "routes": [{"match": f"PathPrefix(`{path}`)", "kind": "Rule", "priority": 70,
                                         "middlewares": [{"name": m} for m in mws],
                                         "services": [{"name": service, "port": port}]}]}})

    allow = []
    if a.ingress_allow_ip:
        mw("swch-metrics-allowlist", {"ipAllowList": {"sourceRange": a.ingress_allow_ip}})
        allow = ["swch-metrics-allowlist"]
    if a.expose_gateway == "ingress" and a.role in ("all", "standalone", "gateway"):
        mw("swch-metrics-gateway-strip", {"stripPrefix": {"prefixes": [a.gateway_path]}})
        route("swch-metrics-gateway", a.gateway_path, "swch-metrics-gateway", 8000,
              allow + ["swch-metrics-gateway-strip"])
    if a.expose_ui == "ingress" and a.role in ("all", "standalone", "ems"):
        p = a.ui_path.rstrip("/")
        mw("swch-metrics-ui-slash", {"redirectRegex": {"regex": f"^(https?://[^/]+{p})$", "replacement": "${1}/",
                                                        "permanent": True}})
        mw("swch-metrics-ui-strip", {"stripPrefix": {"prefixes": [p]}})
        route("swch-metrics-client-ui", p, "swch-metrics-client-ui", 8090,
              allow + ["swch-metrics-ui-slash", "swch-metrics-ui-strip"])
    return out


def build(a: argparse.Namespace) -> list[dict]:
    image = a.image or f"ghcr.io/georgegeorgakakos/k3s-metrics-gateway:{__version__}"
    api_key = secrets.token_urlsafe(24) if a.api_key == "auto" else (a.api_key or "")
    reg_token = secrets.token_urlsafe(24) if a.register_token == "auto" else (a.register_token or "")
    a._reg_token = reg_token
    onboard = getattr(a, "onboard_template", False)
    if onboard:
        a.role, a.expose_gateway = "gateway", "ingress"
    ui_pw = secrets.token_urlsafe(12) if a.ui_password == "auto" else (a.ui_password or "")
    ns = a.namespace
    gw_url = (a.gateway_url or f"http://swch-metrics-gateway.{ns}.svc.cluster.local:8000").rstrip("/")
    ems_namespaces = a.ems_namespaces

    sec_ctx = {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True,
               "runAsNonRoot": True, "runAsUser": 10001, "capabilities": {"drop": ["ALL"]}}

    client_cfg = copy.deepcopy(DEFAULT_CONFIG)
    client_cfg["gateways"][0]["url"] = "${GATEWAY_URL:-%s}" % gw_url
    if a.role == "ems" and not a.gateway_url:
        # no fixed gateway: the sidecar starts empty and gateways register themselves (POST /api/register)
        client_cfg["gateways"] = []
    if a.role == "ems":
        # the gateway lives in another cluster: keep UI-saved config on a local PVC instead
        client_cfg["persist"] = {"mode": "file", "path": "/data/client.yaml"}
    if a.optimusdb_url:
        # A1 / A3 trust evidence -> OptimusDB (kbtrust); state lives on the client PVC in the ems role
        for sk in client_cfg["sinks"]:
            if sk["type"] == "optimusdb_trust":
                sk["enabled"] = True
                sk["options"]["url"] = "${OPTIMUSDB_URL:-%s}" % a.optimusdb_url.rstrip("/")
                if a.trust_cluster:
                    sk["options"]["cluster"] = "${TRUST_CLUSTER:-%s}" % a.trust_cluster
                if a.trust_namespaces:
                    sk["options"]["namespaces"] = [n for n in a.trust_namespaces.split(",") if n]

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
                {"name": "REGISTRATION_TOKEN", "valueFrom": {"secretKeyRef": {
                    "name": "swch-metrics-gateway-auth", "key": "REGISTRATION_TOKEN", "optional": True}}},
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
            {"name": "swch-client-data", "emptyDir": {"sizeLimit": "256Mi"}} if a.role != "ems" else
            {"name": "swch-client-data", "persistentVolumeClaim": {"claimName": "swch-metrics-client-data"}},
        ],
    }

    a._sidecar = sidecar
    docs: list[dict] = [
        {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": ns, "labels": PART_OF}},
        {"apiVersion": "v1", "kind": "ConfigMap",
         "metadata": {"name": "swch-cluster-identity", "namespace": ns, "labels": PART_OF,
                      "annotations": {"swarmchestrate.eu/description":
                                      "Identity advertised to EMS in the topology and in every metric header"}},
         "data": {"SWARM_ID": a.swarm_id or "", "CLUSTER_NAME": a.cluster_name or "",
                  "CLOUD": a.cloud or "", "REGION": a.region or "", "ZONE": a.zone or ""}},
        {"apiVersion": "v1", "kind": "Secret", "type": "Opaque",
         "metadata": {"name": "swch-metrics-gateway-auth", "namespace": ns, "labels": PART_OF},
         "stringData": {"API_KEY": api_key, "UI_PASSWORD": ui_pw, "REGISTRATION_TOKEN": reg_token}},
        {"apiVersion": "v1", "kind": "ServiceAccount",
         "metadata": {"name": "swch-metrics-gateway", "namespace": ns, "labels": lbl("swch-metrics-gateway", "gateway")}},
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
         "subjects": [{"kind": "ServiceAccount", "name": "swch-metrics-gateway", "namespace": ns}]},
        {"apiVersion": "v1", "kind": "ConfigMap",
         "metadata": {"name": "swch-metrics-client-config", "namespace": ns, "labels": PART_OF},
         "data": {"client.yaml": yaml.safe_dump(client_cfg, sort_keys=False)}},
        {"apiVersion": "v1", "kind": "ConfigMap",
         "metadata": {"name": "swch-ems-sidecar-template", "namespace": ns, "labels": PART_OF,
                      "annotations": {"swarmchestrate.eu/description":
                                      "Container patched into the EMS Deployment by the gateway"}},
         "data": {"sidecar.yaml": yaml.safe_dump(sidecar, sort_keys=False)}},
        {"apiVersion": "apps/v1", "kind": "Deployment",
         "metadata": {"name": "swch-metrics-gateway", "namespace": ns, "labels": lbl("swch-metrics-gateway", "gateway")},
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
                             *([] if onboard else [{"name": "GATEWAY_URL", "value": gw_url}]),
                             {"name": "REGISTER_TOKEN", "valueFrom": {"secretKeyRef": {
                                 "name": "swch-metrics-gateway-auth", "key": "REGISTRATION_TOKEN", "optional": True}}},
                             *([] if onboard or not a.register_url else
                               [{"name": "REGISTER_URL", "value": a.register_url},
                                {"name": "PUBLIC_URL", "value": (a.public_url or gw_url).rstrip("/")}]),
                             {"name": "REGISTER_INTERVAL", "value": str(a.register_interval)},
                             {"name": "POD_NAMESPACE", "valueFrom": {"fieldRef": {"fieldPath": "metadata.namespace"}}},
                             {"name": "TOPOLOGY_ENABLED", "value": "true"},
                             {"name": "TOPOLOGY_INTERVAL", "value": "60"},
                             {"name": "ATTACH_EMS_SIDECAR", "value": "true" if a.attach_sidecar and a.role == "all" else "false"},
                             {"name": "EMS_NAMESPACES", "value": ems_namespaces},
                             {"name": "EMS_DEPLOYMENT", "value": a.ems_deployment},
                             {"name": "PROMETHEUS_URL", "value": a.prometheus_url or ""},
                             {"name": "ROOT_PATH", "value": a.gateway_path if a.expose_gateway == "ingress" else ""},
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
         "metadata": {"name": "swch-metrics-gateway", "namespace": ns, "labels": lbl("swch-metrics-gateway", "gateway"),
                      "annotations": {"swarmchestrate.eu/metrics-api": "true",
                                      "swarmchestrate.eu/topology-configmap": "swch-metrics-topology",
                                      "swarmchestrate.eu/openapi": "/openapi.json"}},
         "spec": {"type": "NodePort" if a.expose_gateway == "nodeport" else "ClusterIP",
                  "selector": {"app.kubernetes.io/name": "swch-metrics-gateway"},
                  "ports": [{"name": "http", "port": 8000, "targetPort": "http",
                             **({"nodePort": 30800} if a.expose_gateway == "nodeport" else {})}]}},
        {"apiVersion": "v1", "kind": "Service",
         "metadata": {"name": "swch-metrics-client-ui", "namespace": ns, "labels": lbl("swch-metrics-client", "sidecar-ui")},
         "spec": {"type": "NodePort" if a.expose_ui == "nodeport" else "ClusterIP",
                  "selector": {"app.kubernetes.io/name": "ems-server"},
                  "ports": [{"name": "ui", "port": 8090, "targetPort": 8090,
                             **({"nodePort": 30890} if a.expose_ui == "nodeport" else {})}]}},
    ]
    docs += ingress_docs(a, ns)
    if ns == "default":
        docs = [d for d in docs if d["kind"] != "Namespace"]
    if a.role == "standalone":
        # gateway + client as its own Deployment in this cluster; the client publishes to a remote EMS
        # broker reachable on this node (e.g. an SSH tunnel bound on the host: status.hostIP:61610)
        docs = [d for d in docs if d["metadata"]["name"] != "swch-ems-sidecar-template"]
        for d in docs:
            if d["kind"] == "ClusterRole":
                d["rules"] = [r for r in d["rules"] if "deployments" not in r.get("resources", [])]
            if d["kind"] == "Service" and d["metadata"]["name"] == "swch-metrics-client-ui":
                d["spec"]["selector"] = {"app.kubernetes.io/name": "swch-metrics-client"}
        c = copy.deepcopy(sidecar["container"])
        c["env"] = [e for e in c["env"] if e["name"] != "EMS_BROKER_HOST"]
        if a.ems_broker_host in ("hostIP", "", None):
            c["env"].append({"name": "EMS_BROKER_HOST", "valueFrom": {"fieldRef": {"fieldPath": "status.hostIP"}}})
        else:
            c["env"].append({"name": "EMS_BROKER_HOST", "value": a.ems_broker_host})
        docs.append({"apiVersion": "apps/v1", "kind": "Deployment",
                     "metadata": {"name": "swch-metrics-client", "namespace": ns,
                                  "labels": lbl("swch-metrics-client", "client")},
                     "spec": {"replicas": 1, "strategy": {"type": "Recreate"},
                              "selector": {"matchLabels": {"app.kubernetes.io/name": "swch-metrics-client"}},
                              "template": {"metadata": {"labels": lbl("swch-metrics-client", "client")},
                                           "spec": {"securityContext": {"seccompProfile": {"type": "RuntimeDefault"}},
                                                    "containers": [c], "volumes": sidecar["volumes"]}}}})
    elif a.role == "gateway":
        keep = {"Namespace", "ServiceAccount", "ClusterRole", "ClusterRoleBinding", "Deployment",
                "Middleware", "IngressRoute"}
        names = {"swch-cluster-identity", "swch-metrics-gateway-auth", "swch-metrics-gateway"}
        docs = [d for d in docs if d["kind"] in keep or d["metadata"]["name"] in names]
        for d in docs:
            if d["kind"] == "ClusterRole":
                d["rules"] = [r for r in d["rules"] if "deployments" not in r.get("resources", [])]
        if onboard:
            # per-host values live in the Secret / identity ConfigMap created by onboard-k3s-host.sh
            docs = [d for d in docs if d["metadata"]["name"] not in ("swch-cluster-identity", "swch-metrics-gateway-auth")]
    elif a.role == "ems":
        names = {"swch-cluster-identity", "swch-metrics-gateway-auth", "swch-metrics-client-config",
                 "swch-metrics-client-ui"}
        docs = [d for d in docs if d["kind"] in ("Namespace", "Middleware", "IngressRoute")
                or d["metadata"]["name"] in names]
        docs.append({"apiVersion": "v1", "kind": "PersistentVolumeClaim",
                     "metadata": {"name": "swch-metrics-client-data", "namespace": ns, "labels": PART_OF},
                     "spec": {"accessModes": ["ReadWriteOnce"], "resources": {"requests": {"storage": "256Mi"}}}})
    return docs


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("-o", "--output", default=str(ROOT / "deploy/k3s/swch-metrics-gateway.yaml"))
    ap.add_argument("--role", choices=["all", "standalone", "gateway", "ems"], default="all",
                    help="all = gateway and EMS in the same cluster (default); "
                         "standalone = gateway + client Deployment here, publishing to a remote EMS "
                         "(e.g. through an SSH tunnel on the node, see --ems-broker-host); "
                         "gateway = only the gateway (host with k3s/workloads); "
                         "ems = only the sidecar client resources (host running EMS in another cluster)")
    ap.add_argument("--namespace", default=NS, help="namespace (for --role ems: the EMS namespace)")
    ap.add_argument("--gateway-url", help="URL of the gateway as reachable by the client, e.g. http://10.0.0.5:30800")
    ap.add_argument("--ems-broker-host", default="hostIP",
                    help="for --role standalone: EMS STOMP host; 'hostIP' = the node's IP (SSH tunnel on the host)")
    ap.add_argument("--patch-output", default=str(ROOT / "deploy/ems/ems-sidecar-patch.yaml"))
    ap.add_argument("--image")
    ap.add_argument("--swarm-id")
    ap.add_argument("--cluster-name")
    ap.add_argument("--cloud")
    ap.add_argument("--region")
    ap.add_argument("--zone")
    ap.add_argument("--api-key", help="literal key or 'auto' to generate one (empty = no auth)")
    ap.add_argument("--ui-password", help="literal password or 'auto' (empty = no UI auth)")
    ap.add_argument("--expose-gateway", choices=["clusterip", "nodeport", "ingress"], default="clusterip",
                    help="ingress = Traefik IngressRoute on port 80 under --gateway-path (k3s default ingress)")
    ap.add_argument("--expose-ui", choices=["clusterip", "nodeport", "ingress"], default="clusterip",
                    help="ingress = Traefik IngressRoute on port 80 under --ui-path")
    ap.add_argument("--gateway-path", default="/swch-metrics")
    ap.add_argument("--ui-path", default="/swch-metrics-ui")
    ap.add_argument("--ingress-allow-ip", action="append", default=[],
                    help="restrict the ingress routes to these source IPs/CIDRs (Traefik ipAllowList); repeatable")
    ap.add_argument("--ems-namespaces", default="swarm-system,default")
    ap.add_argument("--ems-deployment", default="emsserver-ems-server")
    ap.add_argument("--prometheus-url")
    ap.add_argument("--optimusdb-url", help="enable the trust sink, e.g. http://193.225.250.240/optimusdb1")
    ap.add_argument("--trust-cluster", help="cluster name in the trust documents (default: topology cluster_id)")
    ap.add_argument("--trust-namespaces", help="comma-separated namespaces evaluated for trust (default: all)")
    ap.add_argument("--no-attach-sidecar", dest="attach_sidecar", action="store_false")
    ap.add_argument("--register-url",
                    help="gateway: EMS sidecar registration URL(s), comma-separated, "
                         "e.g. http://193.225.251.34/swch-metrics-ui/api/register")
    ap.add_argument("--register-token",
                    help="shared registration token ('auto' = generate); gateway: sent, ems: accepted")
    ap.add_argument("--public-url", help="gateway URL as reachable from the EMS host (default --gateway-url)")
    ap.add_argument("--register-interval", type=int, default=300, help="seconds between registration heartbeats")
    ap.add_argument("--onboard-template", action="store_true",
                    help="gateway template for deploy/onboard/onboard-k3s-host.sh: no Secret / identity ConfigMap, "
                         "URLs taken from the swch-cluster-identity ConfigMap")
    a = ap.parse_args()
    if (a.expose_gateway != "clusterip" and not a.api_key) or (a.expose_ui != "clusterip" and not a.ui_password):
        print("WARNING: exposing outside the cluster without --api-key/--ui-password", file=sys.stderr)
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
    if a.role in ("gateway", "standalone"):
        print(f"role={a.role}: no EMS sidecar patch needed")
        return
    pp = Path(a.patch_output)
    pp.parent.mkdir(parents=True, exist_ok=True)
    pp.write_text("# Strategic-merge patch adding the metrics-client sidecar to the EMS Deployment.\n"
                  "# Apply when the attacher is disabled (--no-attach-sidecar) or EMS runs in another cluster (--role ems):\n"
                  f"#   kubectl -n {a.namespace} patch deployment {a.ems_deployment} --patch-file " + pp.name + "\n"
                  + yaml.safe_dump(patch, sort_keys=False))
    print(f"wrote {pp}")
    if a.api_key == "auto" or a.ui_password == "auto" or a.register_token == "auto":
        print("generated credentials are in the Secret swch-metrics-gateway-auth inside the file", file=sys.stderr)
    if a.register_token == "auto":
        print(f"registration token (give it to every k3s host): {a._reg_token}", file=sys.stderr)


if __name__ == "__main__":
    main()
