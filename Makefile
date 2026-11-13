IMAGE ?= ghcr.io/georgegeorgakakos/k3s-metrics-gateway
VERSION ?= $(shell python3 -c "import tomllib;print(tomllib.load(open('pyproject.toml','rb'))['project']['version'])")

.PHONY: install test manifest demo-data image import-k3s run-gateway run-client site

install:            ## editable install with dev deps
	pip install -e ".[dev]"

test:               ## unit tests
	pytest -q

manifest:           ## regenerate deploy/k3s/*.yaml and deploy/ems/*.yaml
	python3 scripts/render_manifest.py

demo-data:          ## regenerate the UI demo data (GitHub Pages)
	python3 scripts/build_demo_data.py

image:              ## build the container image
	docker build -t $(IMAGE):$(VERSION) .

import-k3s: image   ## load the image into the local k3s containerd
	docker save $(IMAGE):$(VERSION) | sudo k3s ctr images import -

run-gateway:        ## run the gateway locally against k3s
	KUBECONFIG=$${KUBECONFIG:-/etc/rancher/k3s/k3s.yaml} TOPOLOGY_ENABLED=false ATTACH_EMS_SIDECAR=false swch-gateway

run-client:         ## run the client UI locally (http://localhost:8090)
	CONFIG_PATH=deploy/examples/client.local.yaml CONFIG_DATA_PATH=/tmp/swch-client.yaml swch-metrics-client

site:               ## build the GitHub Pages site into _site/
	python3 scripts/build_site.py
