#!/usr/bin/env bash
# Host A (k3s + OptimusDB, 193.225.250.240): upgrade the gateway to v0.3.1+ and let it register
# itself with the EMS sidecar on host B. Keeps the API key as it is.
# Run in /opt/iccs/Scripts/SetupK3sMetric:   bash upgrade-hostA.sh <registration-token-from-host-B>
set -uo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"; K="sudo k3s kubectl"; NS=swarm-system; D=swch-metrics-gateway
VERSION="${VERSION:-0.3.1}"
IMG=ghcr.io/georgegeorgakakos/k3s-metrics-gateway:$VERSION
TOKEN="${1:-${REGISTRATION_TOKEN:-}}"
REGISTER_URL="${REGISTER_URL:-http://193.225.251.34/swch-metrics-ui/api/register}"
PUBLIC_URL="${PUBLIC_URL:-http://193.225.250.240/swch-metrics}"
KEY=$(grep ' API_KEY:' "$DIR/hostA-gateway.yaml" 2>/dev/null | awk '{print $2}')
G=http://localhost/swch-metrics
die() { echo "  ✗ $1"; echo "STOPPED."; exit 1; }
[ -n "$TOKEN" ] || die "usage: bash upgrade-hostA.sh <registration token printed on host B>"

echo "== 1/4 pre-checks"
sudo k3s crictl pull "$IMG" >/dev/null 2>&1 && echo "  ✓ image $IMG" || die "cannot pull $IMG"
c=$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 "$REGISTER_URL"); [ "$c" = 405 ] \
  && echo "  ✓ EMS sidecar (v0.3+) reachable: $REGISTER_URL" || die "$REGISTER_URL answered $c (expected 405: run upgrade-hostB.sh first)"
mkdir -p "$DIR/backup"; $K -n $NS get deploy $D -o yaml > "$DIR/backup/gateway-deploy-$(date +%Y%m%d-%H%M%S).yaml"

echo "== 2/4 token + registration settings"
$K -n $NS patch secret swch-metrics-gateway-auth --type merge -p "{\"stringData\":{\"REGISTRATION_TOKEN\":\"$TOKEN\"}}" >/dev/null || die "secret patch failed"
$K -n $NS patch deployment $D --type strategic -p "{\"spec\":{\"template\":{\"spec\":{\"containers\":[{\"name\":\"gateway\",\"image\":\"$IMG\",\"env\":[
  {\"name\":\"REGISTER_URL\",\"value\":\"$REGISTER_URL\"},{\"name\":\"PUBLIC_URL\",\"value\":\"$PUBLIC_URL\"},
  {\"name\":\"REGISTER_INTERVAL\",\"value\":\"300\"},
  {\"name\":\"REGISTER_TOKEN\",\"valueFrom\":{\"secretKeyRef\":{\"name\":\"swch-metrics-gateway-auth\",\"key\":\"REGISTRATION_TOKEN\",\"optional\":true}}}]}]}}}}" >/dev/null \
  || die "deployment patch failed"
echo "  ✓ gateway -> v$VERSION, REGISTER_URL=$REGISTER_URL, PUBLIC_URL=$PUBLIC_URL"

echo "== 3/4 rollout"
$K -n $NS rollout status deploy/$D --timeout=180s || die "gateway not ready"

echo "== 4/4 registration"
for i in $(seq 1 20); do
  R=$(curl -s -H "X-API-Key: $KEY" "$G/registration"); echo "$R" | grep -q '"status":"registered"' && break; sleep 3
done
echo "$R" | python3 -c "
import sys,json
d=json.load(sys.stdin)
for u,t in d.get('targets',{}).items():
    r=t.get('response') or {}
    print('  ✓' if t['status']=='registered' else '  ✗', u, '->', t['status'], r.get('action',''), 'as', r.get('id',''), t.get('error',''))"
echo
echo "Note: re-applying the old hostA-gateway.yaml would remove the registration settings; re-run this script afterwards."
