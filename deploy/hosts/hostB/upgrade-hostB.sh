#!/usr/bin/env bash
# Host B (EMS, 193.225.251.34): upgrade the EMS sidecar to v0.3.1+ (one sidecar, many gateways)
# and enable self-registration of gateways. Keeps the API key and UI password as they are.
# Also enables the OptimusDB trust sink if it is not on yet (TRUST_NS=optimusddc to limit it to OptimusDB pods).
# Run in /opt/iccs/Scripts/SetupK3sMetric:   bash upgrade-hostB.sh
# Prints the REGISTRATION TOKEN that every k3s host (incl. host A) needs.
set -uo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"; K="sudo k3s kubectl"; NS=default; EMS=emsserver-ems-server
VERSION="${VERSION:-0.3.1}"
IMG=ghcr.io/georgegeorgakakos/k3s-metrics-gateway:$VERSION
UI="${UI:-http://localhost/swch-metrics-ui}"
PW="${PW:-$(grep ' UI_PASSWORD:' "$DIR/hostB-ems.yaml" 2>/dev/null | awk '{print $2}')}"
die() { echo "  ✗ $1"; echo "STOPPED."; exit 1; }

echo "== 1/5 pre-checks"
grep -q "$IMG" "$DIR/hostB-ems-sidecar-patch.yaml" 2>/dev/null && echo "  ✓ v$VERSION patch in $DIR" || die "hostB-ems-sidecar-patch.yaml (v$VERSION) not in $DIR"
sudo k3s crictl pull "$IMG" >/dev/null 2>&1 && echo "  ✓ image $IMG" || die "cannot pull $IMG: push tag v$VERSION and wait for the Release workflow"
mkdir -p "$DIR/backup"; TS=$(date +%Y%m%d-%H%M%S)
$K -n $NS get deploy $EMS -o yaml > "$DIR/backup/ems-deploy-$TS.yaml" && echo "  ✓ backup/ems-deploy-$TS.yaml"
curl -s -u "admin:$PW" "$UI/api/config/download" > "$DIR/backup/client-config-$TS.yaml" && echo "  ✓ backup/client-config-$TS.yaml"

echo "== 2/5 registration token"
TOKEN="$($K -n $NS get secret swch-metrics-gateway-auth -o jsonpath='{.data.REGISTRATION_TOKEN}' 2>/dev/null | base64 -d 2>/dev/null)"
TOKEN="${REGISTRATION_TOKEN:-${TOKEN:-$(openssl rand -hex 24)}}"
$K -n $NS patch secret swch-metrics-gateway-auth --type merge -p "{\"stringData\":{\"REGISTRATION_TOKEN\":\"$TOKEN\"}}" >/dev/null || die "secret patch failed"
grep -q ' REGISTRATION_TOKEN:' "$DIR/hostB-ems.yaml" && sed -i "s/^\(  REGISTRATION_TOKEN: \).*/\1$TOKEN/" "$DIR/hostB-ems.yaml" \
  || sed -i "s/^\(  UI_PASSWORD: .*\)$/\1\n  REGISTRATION_TOKEN: $TOKEN/" "$DIR/hostB-ems.yaml"
echo "  ✓ REGISTRATION_TOKEN in secret default/swch-metrics-gateway-auth (and hostB-ems.yaml)"

echo "== 3/5 sidecar v$VERSION (EMS restarts once, ~3-4 min)"
$K -n $NS patch deployment $EMS --patch-file "$DIR/hostB-ems-sidecar-patch.yaml" >/dev/null || die "patch failed"
$K -n $NS rollout status deploy/$EMS --timeout=600s || die "rollout not finished"
for i in $(seq 1 30); do
  V=$(curl -s --max-time 5 -u "admin:$PW" "$UI/api/status" | python3 -c "import sys,json;print(json.load(sys.stdin)['version'])" 2>/dev/null)
  [ "$V" = "$VERSION" ] && break; sleep 5
done
[ "$V" = "$VERSION" ] && echo "  ✓ sidecar v$V" || die "sidecar not answering with v$VERSION (got '${V:-nothing}')"

echo "== 4/5 OptimusDB trust sink + collectors it needs (skipped if already enabled)"
UI="$UI" PW="$PW" TRUST_NS="${TRUST_NS:-}" python3 - <<'PY' || echo "  ✗ config update failed (enable it in the UI: Sinks -> optimusdb-trust)"
import base64, json, os, urllib.request
UI, PW = os.environ["UI"], os.environ["PW"]
H = {"Authorization": "Basic " + base64.b64encode(f"admin:{PW}".encode()).decode(), "Content-Type": "application/json"}
def call(m, p, b=None):
    return json.load(urllib.request.urlopen(urllib.request.Request(UI + p, method=m, headers=H,
                     data=json.dumps(b).encode() if b else None), timeout=30))
cfg = call("GET", "/api/config")["config"]
changed = False
sk = next((s for s in cfg.setdefault("sinks", []) if s.get("type") == "optimusdb_trust"), None)
if sk is None:
    sk = {"id": "optimusdb-trust", "type": "optimusdb_trust", "enabled": False, "options": {}}
    cfg["sinks"].append(sk)
if not sk.get("enabled"):
    sk["enabled"], changed = True, True
    o = sk.setdefault("options", {})
    o.setdefault("url", "${OPTIMUSDB_URL:-http://193.225.250.240/optimusdb1}")
    o.setdefault("context", "swarmkb"); o.setdefault("dstype", "kbtrust")
    o.setdefault("cluster", "${TRUST_CLUSTER:-epm-server}"); o.setdefault("flush_interval", "60s")
    o.setdefault("state_path", "/data/trust-state.json")
    if os.environ.get("TRUST_NS"):
        o["namespaces"] = [n for n in os.environ["TRUST_NS"].split(",") if n]
for c in cfg.setdefault("collectors", []):
    if c.get("type") == "pod_overview" and c.get("metrics") and "pod_running" not in c["metrics"]:
        c["metrics"].append("pod_running"); changed = True
if not any(c.get("type") == "apiserver_health" for c in cfg["collectors"]):
    cfg["collectors"].append({"id": "control-plane", "type": "apiserver_health", "interval": "30s"}); changed = True
if changed:
    call("PUT", "/api/config", {"config": cfg}); print("  ✓ trust sink enabled (OptimusDB kbtrust), pod_running + control-plane collectors")
else:
    print("  ✓ trust sink already enabled")
PY

echo "== 5/5 status"
curl -s -u "admin:$PW" "$UI/api/status" | python3 -c "
import sys,json
s=json.load(sys.stdin); r=s['registration']
print('  ✓' if r['token_configured'] else '  ✗', 'registration endpoint: POST http://193.225.251.34/swch-metrics-ui/api/register')
for g in s['gateways']:
    print('  ✓' if g['ok'] else '  ✗', 'gateway', g['id'], '->', g['url'], '| cluster', g['cluster'], '|', g['source'])"
echo
echo "REGISTRATION TOKEN (host A: bash upgrade-hostA.sh <token>; new hosts: bash make-autoregister-manifest.sh):"
echo "  $TOKEN"
