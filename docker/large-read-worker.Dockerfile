# Separate Node24 image; the Bun services image does not select this runtime.
FROM oven/bun:1.4.2-slim AS build
WORKDIR /app
COPY package.json bun.lock tsconfig.json ./
COPY packages/pipeline/package.json packages/pipeline/
COPY packages/object-store/package.json packages/object-store/
COPY packages/session-read-worker/package.json packages/session-read-worker/
RUN bun install --frozen-lockfile --ignore-scripts --no-progress
COPY packages packages
COPY src src
RUN bun build src/session-pipeline/large-read-main.ts --target=node --outfile=/out/large-read-main.mjs --external pg
FROM node:24.21.0-bookworm-slim
ARG SOURCE_REVISION
LABEL org.opencontainers.image.revision=$SOURCE_REVISION
WORKDIR /app
COPY --from=build /app/node_modules ./node_modules
COPY --from=build /out/large-read-main.mjs ./large-read-main.mjs
ENV NODE_ENV=production
USER node
CMD ["node", "/app/large-read-main.mjs"]
