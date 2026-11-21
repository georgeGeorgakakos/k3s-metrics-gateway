#!/usr/bin/env bash
# Run ONCE on the EMS host (host B). Builds one manifest that turns ANY new k3s host into a monitored
# cluster with no command on that host (zero-touch):
#
#   bash make-autoregister-manifest.sh            ->  swch-metrics-gateway-autoregister.yaml
#
# Put that file on every new k3s host (or bake it into the VM image / cloud-init / cluster-builder):
#   /var/lib/rancher/k3s/server/manifests/swch-metrics-gateway-autoregister.yaml
# k3s applies it on boot; the gateway works out its own URL and cluster name and registers itself with EMS.
#
# Env: VERSION (default 0.3.1), REGISTER_URL (default http://193.225.251.34/swch-metrics-ui/api/register),
#      SWARM_ID (default swarm-01), OUT (output file), TEMPLATE (local onboard template instead of download)
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
K="sudo k3s kubectl"
VERSION="${VERSION:-0.3.1}"
REGISTER_URL="${REGISTER_URL:-http://193.225.251.34/swch-metrics-ui/api/register}"
SWARM_ID="${SWARM_ID:-swarm-01}"
OUT="${OUT:-$DIR/swch-metrics-gateway-autoregister.yaml}"
REL="https://github.com/georgeGeorgakakos/k3s-metrics-gateway/releases/download/v${VERSION}/swch-metrics-gateway-onboard.yaml"
RAW="https://raw.githubusercontent.com/georgeGeorgakakos/k3s-metrics-gateway/v${VERSION}/deploy/k3s/swch-metrics-gateway-onboard.yaml"
die() { echo "  ✗ $*"; exit 1; }

TOKEN="$($K -n default get secret swch-metrics-gateway-auth -o jsonpath='{.data.REGISTRATION_TOKEN}' 2>/dev/null | base64 -d 2>/dev/null || true)"
[ -n "$TOKEN" ] || die "no REGISTRATION_TOKEN on this host: run upgrade-hostB first"
echo "  ✓ registration token read from secret default/swch-metrics-gateway-auth"

# one API key shared by the auto-registered gateways (kept next to this script, readable by you only)
KEYF="$DIR/fleet-api-key"
if [ ! -s "$KEYF" ]; then (umask 077; openssl rand -hex 16 > "$KEYF"); echo "  ✓ new fleet API key in $KEYF"
else echo "  ✓ fleet API key from $KEYF"; fi
KEY="$(cat "$KEYF")"

TPL="$(mktemp)"; trap 'rm -f "$TPL"' EXIT
if [ -n "${TEMPLATE:-}" ]; then cp "$TEMPLATE" "$TPL"
else curl -fsSL "$REL" -o "$TPL" 2>/dev/null || curl -fsSL "$RAW" -o "$TPL" || die "cannot download the v$VERSION gateway template"; fi
grep -q "k3s-metrics-gateway:${VERSION}" "$TPL" || echo "  ! template image is not ${VERSION}: $(grep -o 'k3s-metrics-gateway:[0-9.]*' "$TPL" | head -1)"
echo "  ✓ gateway template v$VERSION"

umask 077
{
  echo "# Swarmchestrate k3s Metrics Gateway v${VERSION}: zero-touch auto-registration manifest."
  echo "# Generated on the EMS host on $(date -u +%Y-%m-%dT%H:%M:%SZ). Contains the registration token and the fleet"
  echo "# API key: keep it private (mode 600). Drop it into /var/lib/rancher/k3s/server/manifests/ on a new k3s host."
  echo "# The gateway derives its URL (http://<control-plane node IP>/swch-metrics) and cluster name (node name);"
  echo "# override per host with PUBLIC_HOST / CLUSTER_NAME in the swch-cluster-identity ConfigMap if needed."
  cat <<YAML
apiVersion: v1
kind: Namespace
metadata:
  name: swarm-system
  labels: {app.kubernetes.io/part-of: swarmchestrate}
---
apiVersion: v1
kind: Secret
type: Opaque
metadata:
  name: swch-metrics-gateway-auth
  namespace: swarm-system
  labels: {app.kubernetes.io/part-of: swarmchestrate}
stringData:
  API_KEY: "${KEY}"
  REGISTRATION_TOKEN: "${TOKEN}"
  UI_PASSWORD: ""
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: swch-cluster-identity
  namespace: swarm-system
  labels: {app.kubernetes.io/part-of: swarmchestrate}
data:
  REGISTER_URL: "${REGISTER_URL}"
  SWARM_ID: "${SWARM_ID}"
---
YAML
  cat "$TPL"
} > "$OUT"
echo "  ✓ wrote $OUT ($(grep -c '^kind:' "$OUT") objects, mode 600)"
cat <<EOF

Use it on every new k3s host (any one of):
  a) existing host:  scp it there, then  sudo install -m 600 $(basename "$OUT") /var/lib/rancher/k3s/server/manifests/
  b) new VM:         cloud-init write_files to that path (deploy/onboard/cloud-init-k3s-swch.yaml)
  c) cluster-builder: add it to the manifests it deploys
Then watch it appear:  http://193.225.251.34/swch-metrics-ui/#gateways   and check:  bash e2e-check.sh <node-name>
EOF
