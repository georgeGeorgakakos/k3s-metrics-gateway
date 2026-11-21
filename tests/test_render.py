import subprocess
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def render(tmp_path, *args):
    out, patch = tmp_path / "m.yaml", tmp_path / "p.yaml"
    subprocess.run([sys.executable, str(ROOT / "scripts/render_manifest.py"), "-o", str(out),
                    "--patch-output", str(patch), *args], check=True, capture_output=True)
    docs = [d for d in yaml.safe_load_all(out.read_text()) if d]
    return {f'{d["kind"]}/{d["metadata"]["name"]}': d for d in docs}, patch


def test_role_all_default(tmp_path):
    d, patch = render(tmp_path)
    assert "Deployment/swch-metrics-gateway" in d and "ConfigMap/swch-ems-sidecar-template" in d
    assert patch.exists()


def test_role_gateway(tmp_path):
    d, patch = render(tmp_path, "--role", "gateway", "--gateway-url", "http://10.0.0.5:30800",
                      "--expose-gateway", "nodeport", "--api-key", "k")
    assert "ConfigMap/swch-ems-sidecar-template" not in d and "Service/swch-metrics-client-ui" not in d
    env = {e["name"]: e.get("value") for e in
           d["Deployment/swch-metrics-gateway"]["spec"]["template"]["spec"]["containers"][0]["env"]}
    assert env["ATTACH_EMS_SIDECAR"] == "false" and env["GATEWAY_URL"] == "http://10.0.0.5:30800"
    assert not any("deployments" in r.get("resources", []) for r in d["ClusterRole/swch-metrics-gateway"]["rules"])
    assert d["Service/swch-metrics-gateway"]["spec"]["ports"][0]["nodePort"] == 30800
    assert not patch.exists()


def test_role_ems(tmp_path):
    d, patch = render(tmp_path, "--role", "ems", "--namespace", "default",
                      "--gateway-url", "http://10.0.0.5:30800", "--api-key", "k")
    assert "Deployment/swch-metrics-gateway" not in d and "PersistentVolumeClaim/swch-metrics-client-data" in d
    assert all(x["metadata"].get("namespace", "default") == "default" for k, x in d.items() if not k.startswith("Namespace"))
    cfg = yaml.safe_load(d["ConfigMap/swch-metrics-client-config"]["data"]["client.yaml"])
    assert cfg["persist"]["mode"] == "file"
    p = yaml.safe_load(patch.read_text())
    c = p["spec"]["template"]["spec"]["containers"][0]
    assert {e["name"]: e.get("value") for e in c["env"]}["GATEWAY_URL"] == "http://10.0.0.5:30800"
    assert any(v.get("persistentVolumeClaim") for v in p["spec"]["template"]["spec"]["volumes"])


def test_role_standalone_uses_host_tunnel(tmp_path):
    d, patch = render(tmp_path, "--role", "standalone", "--api-key", "k")
    assert "ConfigMap/swch-ems-sidecar-template" not in d and not patch.exists()
    gw_env = {e["name"]: e.get("value") for e in
              d["Deployment/swch-metrics-gateway"]["spec"]["template"]["spec"]["containers"][0]["env"]}
    assert gw_env["ATTACH_EMS_SIDECAR"] == "false"
    c = d["Deployment/swch-metrics-client"]["spec"]["template"]["spec"]["containers"][0]
    broker = [e for e in c["env"] if e["name"] == "EMS_BROKER_HOST"]
    assert broker == [{"name": "EMS_BROKER_HOST", "valueFrom": {"fieldRef": {"fieldPath": "status.hostIP"}}}]
    assert d["Service/swch-metrics-client-ui"]["spec"]["selector"] == {"app.kubernetes.io/name": "swch-metrics-client"}
    assert d["Service/swch-metrics-gateway"]["spec"]["type"] == "ClusterIP"
    d2, _ = render(tmp_path, "--role", "standalone", "--ems-broker-host", "10.1.2.3")
    c2 = d2["Deployment/swch-metrics-client"]["spec"]["template"]["spec"]["containers"][0]
    assert {"name": "EMS_BROKER_HOST", "value": "10.1.2.3"} in c2["env"]


def test_ingress_exposure_two_hosts(tmp_path):
    a, _ = render(tmp_path, "--role", "gateway", "--expose-gateway", "ingress", "--api-key", "k",
                  "--gateway-url", "http://10.0.0.5/swch-metrics", "--ingress-allow-ip", "10.0.0.9/32")
    r = a["IngressRoute/swch-metrics-gateway"]["spec"]["routes"][0]
    assert r["match"] == "PathPrefix(`/swch-metrics`)" and r["services"] == [{"name": "swch-metrics-gateway", "port": 8000}]
    assert [m["name"] for m in r["middlewares"]] == ["swch-metrics-allowlist", "swch-metrics-gateway-strip"]
    assert a["Middleware/swch-metrics-allowlist"]["spec"]["ipAllowList"]["sourceRange"] == ["10.0.0.9/32"]
    env = {e["name"]: e.get("value") for e in
           a["Deployment/swch-metrics-gateway"]["spec"]["template"]["spec"]["containers"][0]["env"]}
    assert env["ROOT_PATH"] == "/swch-metrics"
    b, patch = render(tmp_path, "--role", "ems", "--namespace", "default", "--expose-ui", "ingress",
                      "--gateway-url", "http://10.0.0.5/swch-metrics", "--api-key", "k", "--ui-password", "p")
    assert "Namespace/default" not in b
    assert b["IngressRoute/swch-metrics-client-ui"]["metadata"]["namespace"] == "default"
    assert "IngressRoute/swch-metrics-gateway" not in b
    assert b["Middleware/swch-metrics-ui-slash"]["spec"]["redirectRegex"]["replacement"] == "${1}/"


def test_role_ems_with_trust_sink(tmp_path):
    d, _ = render(tmp_path, "--role", "ems", "--namespace", "default", "--gateway-url", "http://10.0.0.5/swch-metrics",
                  "--optimusdb-url", "http://10.0.0.5/optimusdb1/", "--trust-cluster", "swarm-01",
                  "--trust-namespaces", "optimusddc")
    cfg = yaml.safe_load(d["ConfigMap/swch-metrics-client-config"]["data"]["client.yaml"])
    t = next(s for s in cfg["sinks"] if s["type"] == "optimusdb_trust")
    assert t["enabled"] is True and t["options"]["url"] == "${OPTIMUSDB_URL:-http://10.0.0.5/optimusdb1}"
    assert t["options"]["cluster"] == "${TRUST_CLUSTER:-swarm-01}" and t["options"]["namespaces"] == ["optimusddc"]
    assert "pod_running" in next(c for c in cfg["collectors"] if c["type"] == "pod_overview")["metrics"]
    assert any(c["type"] == "apiserver_health" for c in cfg["collectors"])
    d2, _ = render(tmp_path, "--role", "ems", "--namespace", "default")
    cfg2 = yaml.safe_load(d2["ConfigMap/swch-metrics-client-config"]["data"]["client.yaml"])
    assert next(s for s in cfg2["sinks"] if s["type"] == "optimusdb_trust")["enabled"] is False


def _env(d, name):
    c = d["Deployment/swch-metrics-gateway"]["spec"]["template"]["spec"]["containers"][0]
    return {e["name"]: e for e in c["env"]}.get(name)


def test_gateway_registers_with_ems(tmp_path):
    d, _ = render(tmp_path, "--role", "gateway", "--expose-gateway", "ingress", "--api-key", "k",
                  "--gateway-url", "http://10.0.0.7/swch-metrics", "--cluster-name", "edge-07",
                  "--register-url", "http://193.225.251.34/swch-metrics-ui/api/register", "--register-token", "tok")
    assert _env(d, "REGISTER_URL")["value"] == "http://193.225.251.34/swch-metrics-ui/api/register"
    assert _env(d, "PUBLIC_URL")["value"] == "http://10.0.0.7/swch-metrics"
    assert _env(d, "REGISTER_TOKEN")["valueFrom"]["secretKeyRef"]["key"] == "REGISTRATION_TOKEN"
    assert d["Secret/swch-metrics-gateway-auth"]["stringData"]["REGISTRATION_TOKEN"] == "tok"


def test_gateway_without_registration_has_no_register_url(tmp_path):
    d, _ = render(tmp_path, "--role", "gateway")
    assert _env(d, "REGISTER_URL") is None


def test_ems_without_gateway_url_waits_for_registrations(tmp_path):
    d, _ = render(tmp_path, "--role", "ems", "--namespace", "default", "--register-token", "tok")
    cfg = yaml.safe_load(d["ConfigMap/swch-metrics-client-config"]["data"]["client.yaml"])
    assert cfg["gateways"] == [] and cfg["registration"]["token"] == "${REGISTRATION_TOKEN:-}"
    assert d["Secret/swch-metrics-gateway-auth"]["stringData"]["REGISTRATION_TOKEN"] == "tok"


def test_onboard_template(tmp_path):
    d, _ = render(tmp_path, "--onboard-template")
    assert "Secret/swch-metrics-gateway-auth" not in d and "ConfigMap/swch-cluster-identity" not in d
    assert "IngressRoute/swch-metrics-gateway" in d and _env(d, "GATEWAY_URL") is None
    assert _env(d, "REGISTER_TOKEN") is not None and _env(d, "ROOT_PATH")["value"] == "/swch-metrics"


def test_zero_touch_manifest_has_no_per_host_values(tmp_path):
    d, _ = render(tmp_path, "--role", "gateway", "--expose-gateway", "ingress", "--api-key", "fleet",
                  "--swarm-id", "swarm-01", "--register-url", "http://ems/swch-metrics-ui/api/register",
                  "--register-token", "tok")
    assert _env(d, "PUBLIC_URL") is None                         # derived by the gateway itself
    assert d["ConfigMap/swch-cluster-identity"]["data"]["CLUSTER_NAME"] == ""
    assert _env(d, "REGISTER_URL")["value"] == "http://ems/swch-metrics-ui/api/register"
