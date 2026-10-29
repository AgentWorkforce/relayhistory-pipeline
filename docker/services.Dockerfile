# 96bulls services image: one image, SERVICE= picks the entry point.
# Contains this repo and the vendored relayhistory-cloud sources (pinned submodule).
FROM oven/bun:1.4.2-slim AS base
WORKDIR /app
ENV NODE_ENV=production
# Vendored deps first (own lockfile), then ours.
COPY vendor/relayhistory-cloud/package.json vendor/relayhistory-cloud/package-lock.json vendor/relayhistory-cloud/
COPY vendor/relayhistory-cloud/packages/relayhistory/package.json vendor/relayhistory-cloud/packages/relayhistory/
COPY vendor/relayhistory-cloud/packages/object-store/package.json vendor/relayhistory-cloud/packages/object-store/
COPY vendor/relayhistory-cloud/packages/cloud-client/package.json vendor/relayhistory-cloud/packages/cloud-client/
RUN cd vendor/relayhistory-cloud && bun install --ignore-scripts --no-progress
COPY package.json bun.lock tsconfig.json ./
RUN bun install --ignore-scripts --no-progress
COPY vendor/relayhistory-cloud vendor/relayhistory-cloud
RUN cd vendor/relayhistory-cloud && bun run --cwd packages/object-store build && bun run --cwd packages/cloud-client build
COPY src src
COPY stacks stacks
COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh && mkdir -p /telemetry /concourse && chown -R bun:bun /app /telemetry /concourse
USER bun
ENV OTEL_EXPORTER_OTLP_ENDPOINT=http://96b-otel-collector:4318
ENTRYPOINT ["/entrypoint.sh"]
