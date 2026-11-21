# Changelog

## 0.3.1: zero-touch onboarding
- The gateway works out its public URL (`http://<control-plane node ExternalIP/InternalIP><ROOT_PATH>`) and cluster
  name (control-plane node name) when `PUBLIC_URL` / `CLUSTER_NAME` are not set; overrides `PUBLIC_HOST`,
  `PUBLIC_URL`, `CLUSTER_NAME`. In-cluster `GATEWAY_URL`s are never sent to EMS.
- `deploy/onboard/make-autoregister-manifest.sh` (EMS host): one manifest (token, fleet API key, REGISTER_URL,
  gateway) for every new k3s host; `deploy/onboard/cloud-init-k3s-swch.yaml` example.
- `deploy/onboard/e2e-check.sh`: end-to-end check (sidecar UI, EMS broker, OptimusDB kbtrust and ems_events);
  `docs/add-k3s-host.md`: step-by-step guide with diagrams.

## 0.3.0: one sidecar, many k3s clusters; self-registration
- **Many gateways per client**: `gateways:` list (one per k3s cluster). Every collector runs against every
  enabled gateway (or the ones in its `gateways:`). Samples carry `cluster`, `cluster_uid`, `swarm_id`, `gateway`.
  One unreachable cluster does not affect the others (collector status `degraded`). Old single `gateway:` configs
  are converted automatically.
- **Self-registration**: gateways with `REGISTER_URL`/`REGISTER_TOKEN` announce themselves to the EMS sidecar
  (`POST /api/register`, Bearer token) and send a heartbeat every 5 min. The sidecar adds or updates the gateway
  (matched by cluster UID or URL), persists it and starts polling. `auto_approve`, `stale_after`. Gateway endpoints
  `GET/POST /registration`.
- **Onboarding**: `deploy/onboard/onboard-k3s-host.sh` (kubectl + curl only) and the release asset
  `swch-metrics-gateway-onboard.yaml`. Renderer: `--register-url`, `--register-token`, `--public-url`,
  `--register-interval`, `--onboard-template`. `--role ems` without `--gateway-url` starts empty and waits for
  registrations.
- **EMS sink**: headers `cluster`, `cluster-uid`, `gateway`; `source-endpoint` per cluster; `body_labels`
  (default `[cluster]`) adds the cluster to the event body; `instance_template`; `{cluster}` in `destination`.
- **Trust sink**: subjects per cluster (`trust:<cluster>:...`), `cluster_uid` and `swarm_id` in every document.
  v0.2 state is migrated.
- **UI**: Gateways tab (status, heartbeat, test, enable/disable, edit, delete, registration info, onboarding
  command); multi-cluster header and KPIs; cluster column and filter in Samples; cluster selector in Topology;
  per-gateway collector breakdown. The demo shows three clusters.
- API: `GET /api/gateways`, `POST /api/gateways/{id}/test|enabled`, `DELETE /api/gateways/{id}`,
  `GET /api/topology?gateway=`, `GET /api/samples?cluster=`.

## 0.2.0 — trust evidence to OptimusDB
- **New sink `optimusdb_trust`**: computes A1 Availability (pod_running / node_ready / apiserver_readyz) and
  A3 Operational stability (container restart increase, ready→not-ready transitions, warning events as evidence)
  over rolling windows. Upserts `trust_evidence`, hourly `trust_evidence_history` and `monitoring_snapshot`
  documents into OptimusDB (`POST /optimusdb1/swarmkb/command`, `crudput`, `dstype=kbtrust`). Window state is kept
  on the client PVC.
- Client API `GET /api/trust` (filters: `attribute`, `kind`, `violations`, `flush`).
- Default config: `pod_running` added to the pod-utilisation whitelist; new `control-plane` collector
  (`apiserver_health`); `optimusdb-trust` sink (disabled until enabled).
- `render_manifest.py`: `--optimusdb-url`, `--trust-cluster`, `--trust-namespaces`.
- Durations accept `d` and `w` (e.g. `7d`). UI supports `number` options.

## 0.1.0 — initial release
- **Gateway** (`swch-gateway`): one REST API over metrics-server, kubelet (summary, cAdvisor, resource, probes),
  the core API (pod status, utilisation, events, nodes), API-server metrics/health, custom/external metrics adapters
  and optional Prometheus.
- **Topology publisher**: ConfigMap `swch-metrics-topology` (identity, nodes, zones/clouds, capacity, endpoint
  catalog with availability), rewritten only when it changes.
- **EMS sidecar attacher**: patches `swch-metrics-client` into `emsserver-ems-server` whenever EMS is (re)deployed.
- **Metrics client** (`swch-metrics-client`): 10 collector types, 4 sinks (EMS STOMP, JSONL, webhook, log),
  per-collector intervals, filters, aggregates, plugin loading, hot reload.
- **Swarmchestrate-branded UI**: dashboard with live charts, collector/sink editors, topology view, samples, YAML editor.
- k3s auto-deploy manifest + render script for cluster-builder; GitHub Pages site with interactive figures and live demo.
