# Many k3s clusters, one EMS sidecar (v0.3)

One EMS metrics sidecar (`swch-metrics-client`) polls **any number of k3s clusters**. Every
cluster runs one k3s Metrics Gateway. When a new k3s host is deployed, its gateway **registers
itself** with the EMS sidecar and is polled straight away, with no step on the EMS host.

```
k3s host A (epm-server)  gateway ──┐  POST /swch-metrics-ui/api/register  (token, heartbeat every 5 min)
k3s host C (edge-07)     gateway ──┼──────────────────────────────────────▶ EMS host B: swch-metrics-client sidecar
k3s host D (edge-08)     gateway ──┘                                          │  polls every enabled gateway (HTTP :80 /swch-metrics)
                                                                              ├─▶ EMS broker  (headers: cluster, cluster-uid, gateway)
                                                                              └─▶ OptimusDB kbtrust (trust:<cluster>:...)
```

## How a host is added

| Step | Where | What happens |
|------|-------|--------------|
| 1 | new k3s host | `onboard-k3s-host.sh` creates the Secret (API key, registration token) and the identity ConfigMap (cluster name, swarm, public URL, register URL), applies the gateway manifest and the Traefik route `/swch-metrics` |
| 2 | new k3s host | the gateway starts, builds its topology and `POST`s a registration to every `REGISTER_URL` (Bearer token) |
| 3 | EMS host | the sidecar checks the token, adds `{id, url, api_key, cluster, cluster_uid, swarm_id}` to `gateways:`, saves it to `/data/client.yaml` (PVC) and reloads |
| 4 | EMS host | every collector now also runs against the new gateway; samples carry `cluster`, `cluster_uid`, `swarm_id`, `gateway` |
| 5 | both | every 5 min the gateway re-announces itself (heartbeat). Nothing reloads unless the URL, key or name changed; a gateway that stays silent is shown as **stale** after `stale_after` |

Matching on the EMS side: the **same cluster UID** (the kube-system namespace UID) or the **same URL** updates the
existing entry, so a re-deployed host or an IP change never duplicates a cluster. A name clash with another cluster
gets a suffix (`edge-07-2`).

## Onboard a new host

Step-by-step guide with diagrams, commands per host and the end-to-end test: [add-k3s-host.md](add-k3s-host.md).
For hosts created by automation (VM image, cloud-init, cluster-builder) use the **zero-touch** manifest from
`deploy/onboard/make-autoregister-manifest.sh`: no command on the new host at all.

On the new host (k3s with the default Traefik, port 80 reachable from the EMS host):

```bash
curl -fsSL https://raw.githubusercontent.com/georgeGeorgakakos/k3s-metrics-gateway/v0.3.1/deploy/onboard/onboard-k3s-host.sh -o onboard-k3s-host.sh
sudo REGISTER_URL=http://193.225.251.34/swch-metrics-ui/api/register \
     REGISTER_TOKEN=<registration token of the EMS sidecar> \
     CLUSTER_NAME=edge-07 SWARM_ID=swarm-01 \
     bash onboard-k3s-host.sh
```

Optional: `PUBLIC_URL` (default `http://<first host IP>/swch-metrics`), `API_KEY` (default: keep or generate),
`CLOUD` `REGION` `ZONE`, `VERSION`, `MANIFEST` (local file instead of downloading). Only `kubectl`, `curl` and
`openssl` are needed.

With `cluster-builder`, render the gateway once with the renderer and drop it into the k3s manifests folder:

```bash
python scripts/render_manifest.py --role gateway --expose-gateway ingress --api-key auto \
  --cluster-name edge-07 --swarm-id swarm-01 --gateway-url http://<host-ip>/swch-metrics \
  --register-url http://193.225.251.34/swch-metrics-ui/api/register --register-token <token> \
  -o /var/lib/rancher/k3s/server/manifests/swch-metrics-gateway.yaml
```

## EMS host settings

```yaml
registration:
  enabled: true
  token: ${REGISTRATION_TOKEN:-}   # Secret swch-metrics-gateway-auth, key REGISTRATION_TOKEN
  auto_approve: true               # false: new gateways are added disabled; enable them in the UI
  stale_after: 15m
gateways: []                       # filled by registrations (or add entries by hand / in the UI)
collectors:
  - id: nodes
    type: node_usage
    gateways: [epm-server]         # optional: bind a collector to some clusters (default: all)
```

The **Gateways** tab of the UI lists every cluster with its source (`registered` / `config`), last heartbeat,
nodes, samples and status. From there you can test, enable or disable, edit or delete a gateway. It also shows the
registration endpoint and the onboarding command.

API: `GET /api/gateways` · `POST /api/gateways/{id}/test` · `POST /api/gateways/{id}/enabled {"enabled": bool}` ·
`DELETE /api/gateways/{id}` · `POST /api/register` (token, used by gateways) · `GET /api/topology?gateway={id}`.
Gateway side: `GET /registration` (status and payload) · `POST /registration` (register now).

## Telling clusters apart

| Where | How |
|-------|-----|
| Sample labels | `cluster`, `cluster_uid`, `swarm_id`, `gateway` (+ the gateway's own `labels`) |
| EMS headers | `cluster`, `cluster-uid`, `gateway`, `label-*`, `source-endpoint` = that cluster's gateway URL |
| EMS body | `{"metricValue", "level", "timestamp", "cluster"}`: `body_labels` (default `[cluster]`, set `[]` for the plain body). OptimusDB `ems_events`, which stores only bodies, can now tell clusters apart |
| EMS instance | `instance_template` (default `{instance}`; use `{cluster}/{instance}` if node names repeat across clusters) |
| OptimusDB `kbtrust` | `_id = trust:<cluster>:<kind>:<ns>/<name>:<A1\|A3>`, fields `cluster`, `cluster_uid`, `swarm_id`, `subject_did = did:swarm:<cluster>:...` |
| UI | series named `<cluster>/<instance>`; a cluster filter in Samples and Topology |

The cluster name comes from: the gateway entry's `cluster` field, then the gateway's topology (`CLUSTER_NAME` on that
host), then the gateway id.

## Security

- `POST /api/register` is the only UI path without Basic auth; it requires the registration token (constant-time
  compare). Without a token, registration is refused (403).
- The registration carries the gateway's API key so the sidecar can poll it. It travels over plain HTTP on port 80,
  as the metrics already do: use `https://` URLs or `ingress-allow-ip` where that matters.
- `auto_approve: false` keeps unknown clusters disabled until someone approves them.

## Upgrading an existing single-cluster setup

Configs with the old single `gateway:` are converted automatically to `gateways: [{id: default, ...}]`. When that
host's gateway registers for the first time, it matches the existing entry by URL and updates it in place.
Trust state written by v0.2 keeps its cluster (`TRUST_CLUSTER`), so the OptimusDB `_id`s do not change.
