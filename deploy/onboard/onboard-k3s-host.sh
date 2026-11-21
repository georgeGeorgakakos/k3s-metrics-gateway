#!/usr/bin/env bash
# Onboard a new k3s host into Swarmchestrate monitoring.
#
# Installs the k3s Metrics Gateway on this host, exposes it through Traefik on port 80
# (/swch-metrics) and registers it with the EMS metrics sidecar. EMS starts receiving this
# cluster's metrics automatically: no step is needed on the EMS host.
#
# Needs: k3s (kubectl) with the default Traefik ingress, curl, openssl. No Python needed.
#
#   sudo REGISTER_URL=http://<ems-host>/swch-metrics-ui/api/register \
#        REGISTER_TOKEN=<registration token of the EMS sidecar> \
#        CLUSTER_NAME=<unique cluster name> SWARM_ID=<swarm id> \
#        bash onboard-k3s-host.sh
#
# Optional:
#   PUBLIC_URL   gateway URL as reachable from the EMS host (default http://<first host IP>/swch-metrics)
#   API_KEY      gateway API key (default: keep the existing one, else generate)
#   CLOUD REGION ZONE   identity labels
#   VERSION      release to install (default 0.3.0)   MANIFEST  local manifest file instead of downloading
#   NAMESPACE    (default swarm-system)
set -euo pipefail

VERSION="${VERSION:-0.3.0}"
NS="${NAMESPACE:-swarm-system}"
REPO_RAW="https://raw.githubusercontent.com/georgeGeorgakakos/k3s-metrics-gateway/v${VERSION}"
: "${REGISTER_URL:?set REGISTER_URL, e.g. http://193.225.251.34/swch-metrics-ui/api/register}"
: "${REGISTER_TOKEN:?set REGISTER_TOKEN (registration token of the EMS sidecar)}"
: "${CLUSTER_NAME:?set CLUSTER_NAME (unique name of this k3s cluster)}"

if command -v k3s >/dev/null 2>&1; then K="k3s kubectl"; else K="kubectl"; fi
ok()   { echo "  ✓ $*"; }
fail() { echo "  ✗ $*"; echo "STOPPED."; exit 1; }

echo "== 1/6 pre-checks"
$K get nodes >/dev/null 2>&1 || fail "kubectl cannot reach this k3s (run with sudo?)"
ok "k3s reachable: $($K get nodes --no-headers | wc -l) node(s)"
$K get crd ingressroutes.traefik.io >/dev/null 2>&1 && ok "Traefik CRDs present" \
  || fail "Traefik (k3s default ingress) not found: the gateway is exposed through Traefik on port 80"
HOST_IP="$(hostname -I 2>/dev/null | awk '{print $1}')"
PUBLIC_URL="${PUBLIC_URL:-http://${HOST_IP}/swch-metrics}"
PUBLIC_URL="${PUBLIC_URL%/}"
code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 "$REGISTER_URL" || true)
case "$code" in
  405|401|403|422) ok "EMS sidecar reachable: $REGISTER_URL (HTTP $code to GET)";;
  000) fail "cannot reach $REGISTER_URL from this host (port 80 to the EMS host)";;
  *)   echo "  ! $REGISTER_URL answered HTTP $code to GET (expected 405); continuing";;
esac

echo "== 2/6 identity and credentials"
$K create namespace "$NS" --dry-run=client -o yaml | $K apply -f - >/dev/null
EXISTING_KEY="$($K -n "$NS" get secret swch-metrics-gateway-auth -o jsonpath='{.data.API_KEY}' 2>/dev/null | base64 -d 2>/dev/null || true)"
API_KEY="${API_KEY:-${EXISTING_KEY:-$(openssl rand -hex 16)}}"
$K -n "$NS" create secret generic swch-metrics-gateway-auth \
  --from-literal=API_KEY="$API_KEY" --from-literal=REGISTRATION_TOKEN="$REGISTER_TOKEN" --from-literal=UI_PASSWORD="" \
  --dry-run=client -o yaml | $K apply -f - >/dev/null
$K -n "$NS" create configmap swch-cluster-identity \
  --from-literal=CLUSTER_NAME="$CLUSTER_NAME" --from-literal=SWARM_ID="${SWARM_ID:-}" \
  --from-literal=CLOUD="${CLOUD:-}" --from-literal=REGION="${REGION:-}" --from-literal=ZONE="${ZONE:-}" \
  --from-literal=GATEWAY_URL="$PUBLIC_URL" --from-literal=PUBLIC_URL="$PUBLIC_URL" \
  --from-literal=REGISTER_URL="$REGISTER_URL" \
  --dry-run=client -o yaml | $K apply -f - >/dev/null
ok "cluster $CLUSTER_NAME, gateway URL $PUBLIC_URL (Secret + ConfigMap in $NS)"

echo "== 3/6 gateway manifest (v$VERSION)"
TMP="$(mktemp)"; trap 'rm -f "$TMP"' EXIT
if [ -n "${MANIFEST:-}" ]; then cp "$MANIFEST" "$TMP"
else curl -fsSL "$REPO_RAW/deploy/k3s/swch-metrics-gateway-onboard.yaml" -o "$TMP" || fail "download of the manifest failed"; fi
[ "$NS" = swarm-system ] || sed -i "s/namespace: swarm-system/namespace: $NS/g" "$TMP"
$K apply -f "$TMP" >/dev/null && ok "applied (gateway, RBAC, Traefik route /swch-metrics)"

echo "== 4/6 (re)start"
$K -n "$NS" rollout restart deploy/swch-metrics-gateway >/dev/null
$K -n "$NS" rollout status deploy/swch-metrics-gateway --timeout=180s >/dev/null || fail "gateway not ready: $K -n $NS logs deploy/swch-metrics-gateway"
ok "gateway running"

echo "== 5/6 local check"
LOCAL="http://localhost/swch-metrics"
for _ in $(seq 1 20); do
  c=$(curl -s -o /dev/null -w '%{http_code}' -H "X-API-Key: $API_KEY" "$LOCAL/topology" || true); [ "$c" = 200 ] && break; sleep 3
done
[ "$c" = 200 ] && ok "$LOCAL answers (topology 200)" || fail "$LOCAL/topology answered $c"

echo "== 6/6 registration with EMS"
for _ in $(seq 1 20); do
  R="$(curl -s -H "X-API-Key: $API_KEY" "$LOCAL/registration" || true)"
  echo "$R" | grep -q '"status":"registered"' && break; sleep 3
done
if echo "$R" | grep -q '"status":"registered"'; then
  ok "registered with EMS: $(echo "$R" | grep -o '"action":"[a-z-]*"' | head -1) as $(echo "$R" | grep -o '"id":"[^"]*"' | head -1)"
else
  echo "  ✗ not registered yet: $(echo "$R" | grep -o '"error":"[^"]*"' | head -1)"
  echo "    check: curl -s -H 'X-API-Key: <key>' $LOCAL/registration"
fi
echo
echo "Done. Cluster '$CLUSTER_NAME' -> $PUBLIC_URL"
echo "API key is in: $K -n $NS get secret swch-metrics-gateway-auth -o jsonpath='{.data.API_KEY}' | base64 -d"
echo "EMS UI: ${REGISTER_URL%/api/register}/#gateways"
