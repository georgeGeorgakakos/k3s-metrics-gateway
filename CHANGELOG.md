# Changelog

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
