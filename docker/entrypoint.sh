#!/bin/sh
# SERVICE selects the process. Unknown values fail loudly.
# Ports, probes and routing per SERVICE: docs/selfhost-composition.md.
set -e
case "${SERVICE:-}" in
  hoopdb-migrate)   exec bun run /app/src/hoopdb/migrate.ts ;;
  locker-init)      exec bun run /app/src/locker/init.ts ;;
  alleyoop-ingest)  exec bun run /app/src/alleyoop/ingest.ts ;;
  alleyoop-courier) exec bun run /app/src/alleyoop/courier.ts ;;
  hoopdb-indexer)   exec bun run /app/src/hoopdb/indexer.ts ;;
  referee)          exec bun run /app/src/referee/main.ts ;;
  bob-costas)       exec bun run /app/src/bob-costas/main.ts ;;
  session-brief)    exec bun run /app/src/session-brief/main.ts ;;
  acquisition)      exec bun run /app/src/acquisition/main.ts ;;
  session-read-api) exec bun run /app/src/session-read-api/server.ts ;;
  session-pipeline-consumer) exec bun run /app/src/session-pipeline/consumer-service.ts ;;
  session-pipeline-consumer-bootstrap) exec bun run /app/src/session-pipeline/consumer-bootstrap.ts ;;
  session-pipeline-output-facts-bootstrap) exec bun run /app/src/session-pipeline/output-facts-bootstrap.ts ;;
  session-ingest)   exec bun run /app/src/session-ingest/selfhost-server.ts ;;
  session-publication) exec bun run /app/src/session-ingest/publication-service.ts --continuous ;;
  session-erase-bytes) exec bun run /app/src/session-erase/selfhost-eraser-main.ts ;;
  user-erase) exec bun run /app/src/session-erase/user-erase-main.ts ;;
  s3-ledger-rearm-sweeper) exec bun run /app/src/session-ingest/rearm-sweeper-main.ts --loop ;;
  *) echo "96bulls: unknown SERVICE='${SERVICE:-}'" >&2; exit 2 ;;
esac
