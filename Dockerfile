# One image, two entrypoints:
#   swch-gateway          per-cluster metrics & topology API   (Deployment swch-metrics-gateway)
#   swch-metrics-client   EMS sidecar with the configuration UI (patched into emsserver-ems-server)
FROM python:3.12-slim AS build
WORKDIR /src
COPY pyproject.toml README.md LICENSE ./
COPY src ./src
RUN pip install --no-cache-dir build && python -m build --wheel --outdir /dist

FROM python:3.12-slim
LABEL org.opencontainers.image.source="https://github.com/georgeGeorgakakos/k3s-metrics-gateway" \
      org.opencontainers.image.description="Swarmchestrate k3s Metrics Gateway and EMS sidecar metrics client" \
      org.opencontainers.image.licenses="Apache-2.0"
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
COPY --from=build /dist/*.whl /tmp/
RUN pip install --no-cache-dir /tmp/*.whl && rm /tmp/*.whl \
 && useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin swch
USER 10001
EXPOSE 8000 8090
CMD ["swch-gateway"]
