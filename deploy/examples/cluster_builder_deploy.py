"""Deploy the metrics gateway with Swarmchestrate cluster-builder.

cluster-builder copies a manifest folder to the master node's
/var/lib/rancher/k3s/server/manifests/, where k3s applies it automatically.
Render a per-swarm manifest first, then hand the folder to deploy_manifests().
"""
import subprocess
import sys
from pathlib import Path

from cluster_builder import Swarmchestrate  # pip install cluster-builder

SWARM_ID = "swarm-42"
OUT = Path("manifests/swch-metrics-gateway.yaml")

subprocess.run([sys.executable, "scripts/render_manifest.py", "--swarm-id", SWARM_ID,
                "--cluster-name", "swch-demo", "--cloud", "aws", "--region", "eu-west-1",
                "--api-key", "auto", "-o", str(OUT),
                "--patch-output", "manifests-extra/ems-sidecar-patch.yaml"], check=True)

orchestrator = Swarmchestrate(template_dir="templates", output_dir="output")  # see cluster-builder README
orchestrator.deploy_manifests(
    manifest_folder=str(OUT.parent),
    master_ip="MASTER_NODE_IP",
    ssh_user="ubuntu",
    ssh_key="path/to/key.pem",
    ssh_auth_method="key",
)
