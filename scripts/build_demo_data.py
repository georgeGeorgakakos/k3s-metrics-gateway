#!/usr/bin/env python3
"""Generate ui/demo-data.json for the in-browser demo (GitHub Pages).

Collector/sink catalogs and the default config come from the real code, and the
topology is produced by the real topology builder over a simulated 3-node
multi-cloud k3s cluster (AWS master, OpenStack worker, edge worker) - the same
shape cluster-builder's demo deploys.
"""
from __future__ import annotations

import copy
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

os.environ.setdefault("SWARM_ID", "swarm-demo-01")
os.environ.setdefault("CLUSTER_NAME", "swch-demo")
os.environ.setdefault("GATEWAY_URL", "http://swch-metrics-gateway.swarm-system.svc.cluster.local:8000")

from swch_metrics.client.collectors import describe  # noqa: E402
from swch_metrics.client.config import DEFAULT_CONFIG  # noqa: E402
from swch_metrics.client.sinks import describe_sinks  # noqa: E402
from swch_metrics.gateway.topology import build_topology  # noqa: E402

NODES = [
    ("swch-master-aws", ["control-plane", "master"], "10.0.1.10", "aws", "eu-west-1", "eu-west-1a", "4", "7834380Ki", "amd64"),
    ("swch-worker-openstack", [], "192.168.10.21", "openstack", "hu-budapest", "sztaki-cloud", "8", "16231716Ki", "amd64"),
    ("swch-worker-edge", [], "172.16.5.30", "edge", "gr-athens", "edge-athens", "4", "3922168Ki", "arm64"),
]


class FakeK8s:
    host = "https://10.0.1.10:6443"

    def get(self, path, params=None, text=False):
        if path == "/api/v1/nodes":
            items = []
            for name, roles, ip, cloud, region, zone, cpu, mem, arch in NODES:
                labels = {f"node-role.kubernetes.io/{r}": "true" for r in roles}
                labels.update({"topology.kubernetes.io/region": region, "topology.kubernetes.io/zone": zone,
                               "swarmchestrate.eu/cloud": cloud, "kubernetes.io/arch": arch})
                items.append({"metadata": {"name": name, "uid": f"uid-{name}", "labels": labels},
                              "spec": {"podCIDR": "10.42.0.0/24"},
                              "status": {"addresses": [{"type": "InternalIP", "address": ip}],
                                         "conditions": [{"type": "Ready", "status": "True"}],
                                         "capacity": {"cpu": cpu, "memory": mem, "pods": "110"},
                                         "allocatable": {"cpu": cpu, "memory": mem, "pods": "110"},
                                         "nodeInfo": {"architecture": arch, "osImage": "Ubuntu 24.04.1 LTS",
                                                      "kubeletVersion": "v1.33.4+k3s1",
                                                      "containerRuntimeVersion": "containerd://2.0.5-k3s2"}}})
            return {"items": items}
        if path == "/api/v1/namespaces/kube-system":
            return {"metadata": {"uid": "5f1c2a7e-3b9d-4c61-9a0e-7d2b8c4e1f90"}}
        if path == "/version":
            return {"gitVersion": "v1.33.4+k3s1"}
        raise KeyError(path)


def main() -> None:
    caps = {"metrics.k8s.io": True, "custom.metrics.k8s.io": "unavailable (404)",
            "external.metrics.k8s.io": "unavailable (404)", "apiserver /metrics": True,
            "kubelet (stats/cadvisor via nodes/proxy)": True, "prometheus": "not configured (set PROMETHEUS_URL)"}
    topo = build_topology(FakeK8s(), caps)
    topo["updated_at"] = "2026-01-01T00:00:00+00:00"  # deterministic file; the demo sets "now" at runtime
    cfg = copy.deepcopy(DEFAULT_CONFIG)
    data = {
        "types": describe(),
        "sink_types": describe_sinks(),
        "config": cfg,
        "labels": {"swarm_id": "swarm-demo-01", "cluster": "swch-demo"},
        "topology": topo,
    }
    out = ROOT / "src/swch_metrics/client/ui/demo-data.json"
    out.write_text(json.dumps(data, indent=1))
    print(f"wrote {out} ({out.stat().st_size} bytes)")


if __name__ == "__main__":
    main()
