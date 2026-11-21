# deploy/onboard: adding k3s hosts

Everything needed to add a k3s host (host C, D, E …) to Swarmchestrate monitoring. Full guide with diagrams:
[docs/add-k3s-host.md](../../docs/add-k3s-host.md).

| File | Runs on | Purpose |
|------|---------|---------|
| [`make-autoregister-manifest.sh`](make-autoregister-manifest.sh) | **EMS host (host B)**, once | builds `swch-metrics-gateway-autoregister.yaml`: one file for every new host (zero-touch) |
| [`cloud-init-k3s-swch.yaml`](cloud-init-k3s-swch.yaml) | new VM (user-data) | installs k3s with that file, so the VM joins on first boot |
| [`onboard-k3s-host.sh`](onboard-k3s-host.sh) | **new k3s host** | one-command onboarding with a chosen name / URL (alternative to zero-touch) |
| [`e2e-check.sh`](e2e-check.sh) | **EMS host (host B)** | end-to-end check of one cluster: sidecar UI → EMS broker → OptimusDB |
| [`kbtrust-query.sh`](kbtrust-query.sh) | any host reaching OptimusDB | reads trust records from `kbtrust` as a table / JSON / count, see [docs/query-kbtrust.md](../../docs/query-kbtrust.md) |
| [`../k3s/swch-metrics-gateway-onboard.yaml`](../k3s/swch-metrics-gateway-onboard.yaml) | (used by the scripts) | gateway template without per-host values |

All of them are also attached to every GitHub release (`v0.3.1` …).

## Get the tools onto the EMS host (host B)

```bash
cd /opt/iccs/Scripts/SetupK3sMetric
V=0.3.1; R=https://github.com/georgeGeorgakakos/k3s-metrics-gateway/releases/download/v$V
sudo curl -fsSLO $R/make-autoregister-manifest.sh -fsSLO $R/e2e-check.sh -fsSLO $R/cloud-init-k3s-swch.yaml
sudo chmod +x make-autoregister-manifest.sh e2e-check.sh
```

## A · Zero-touch (no command on the new host)

```bash
# host B, once (again only if the token changes)
bash make-autoregister-manifest.sh                     # -> swch-metrics-gateway-autoregister.yaml (mode 600)

# new host: copy the file there, then
sudo install -m 600 swch-metrics-gateway-autoregister.yaml /var/lib/rancher/k3s/server/manifests/
#   or put it in the VM user-data (cloud-init-k3s-swch.yaml) / the cluster-builder manifests

# host B
bash e2e-check.sh <hostname-of-the-new-host>
```

## B · One command on the new host

```bash
# host B: the registration token
sudo k3s kubectl -n default get secret swch-metrics-gateway-auth -o jsonpath='{.data.REGISTRATION_TOKEN}' | base64 -d; echo

# new host
curl -fsSL https://raw.githubusercontent.com/georgeGeorgakakos/k3s-metrics-gateway/v0.3.1/deploy/onboard/onboard-k3s-host.sh -o onboard-k3s-host.sh
sudo REGISTER_URL=http://193.225.251.34/swch-metrics-ui/api/register REGISTER_TOKEN=<token> \
     CLUSTER_NAME=<unique-name> SWARM_ID=swarm-01 bash onboard-k3s-host.sh

# host B
bash e2e-check.sh <unique-name>
```

## Requirements on the new host

See the checklist in [docs/add-k3s-host.md](../../docs/add-k3s-host.md#requirements-for-a-new-host).
