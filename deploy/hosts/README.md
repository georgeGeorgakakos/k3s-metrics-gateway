# deploy/hosts: the two existing hosts

One-time setup / upgrade scripts for the current Swarmchestrate test bed. They contain host addresses but **no
secrets**: API key, UI password and registration token are read from the per-host files and Kubernetes Secrets that
stay on the hosts (`hostA-gateway.yaml`, `hostB-ems.yaml`, Secret `swch-metrics-gateway-auth`).

| Host | Folder on the host | Files | Command |
|------|--------------------|-------|---------|
| **Host B** · EMS · `193.225.251.34` | `/opt/iccs/Scripts/SetupK3sMetric` (next to `hostB-ems.yaml`) | [`hostB/upgrade-hostB.sh`](hostB/upgrade-hostB.sh), [`hostB/hostB-ems-sidecar-patch.yaml`](hostB/hostB-ems-sidecar-patch.yaml), [`hostB/check-trust.sh`](hostB/check-trust.sh), plus [`../onboard/e2e-check.sh`](../onboard/e2e-check.sh) and [`../onboard/make-autoregister-manifest.sh`](../onboard/make-autoregister-manifest.sh) | `TRUST_NS=optimusddc bash upgrade-hostB.sh` → prints the **registration token** |
| **Host A** · k3s + OptimusDB · `193.225.250.240` | `/opt/iccs/Scripts/SetupK3sMetric` (next to `hostA-gateway.yaml`) | [`hostA/upgrade-hostA.sh`](hostA/upgrade-hostA.sh) | `bash upgrade-hostA.sh <token>` |

Order: **host B first** (it creates the token), then host A, then on host B `bash e2e-check.sh epm-server`.

What the scripts do:

| Script | Steps |
|--------|-------|
| `upgrade-hostB.sh` | pull the image · back up the EMS Deployment and the client config · create `REGISTRATION_TOKEN` · patch the EMS Deployment (sidecar `$VERSION`, Recreate, EMS liveness 180 s; EMS restarts once, ~3–4 min) · enable the OptimusDB trust sink · print the status and the token |
| `upgrade-hostA.sh` | pull the image · back up the gateway Deployment · store the token · set `REGISTER_URL`, `PUBLIC_URL`, image `$VERSION` · wait for the registration with host B |
| `check-trust.sh` | trust evidence computed by the sidecar and stored in OptimusDB `kbtrust` |

Both upgrade scripts take `VERSION=<x.y.z>` (default the current release) and can be run again safely.
Rollback of the sidecar:

```bash
sudo k3s kubectl -n default set image deploy/emsserver-ems-server swch-metrics-client=ghcr.io/georgegeorgakakos/k3s-metrics-gateway:<previous>
```

Copy from GitHub to the host:

```bash
cd /opt/iccs/Scripts/SetupK3sMetric
B=https://raw.githubusercontent.com/georgeGeorgakakos/k3s-metrics-gateway/main/deploy
# host B
for f in hosts/hostB/upgrade-hostB.sh hosts/hostB/hostB-ems-sidecar-patch.yaml hosts/hostB/check-trust.sh \
         onboard/e2e-check.sh onboard/make-autoregister-manifest.sh; do sudo curl -fsSL "$B/$f" -o "$(basename $f)"; done
# host A
sudo curl -fsSL "$B/hosts/hostA/upgrade-hostA.sh" -o upgrade-hostA.sh
sudo chmod +x *.sh
```
