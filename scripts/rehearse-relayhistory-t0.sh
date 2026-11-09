#!/usr/bin/env bash
# Local rehearsal of the RelayHistory T0 data path on a Docker host. Nothing
# leaves the host: two throwaway PostgreSQL 17 + pgvector containers stand in
# for Neon (source) and Supabase (target).
#
#   1. source: relayhistory-cloud's runner builds the schema; seed rows go in
#      with live triggers (so derived rows exist, as on Neon)
#   2. target: 96Bulls supabase/migrations (with the test-only role shim), then
#      relayhistory-cloud's runner, as in runbook step A3
#   3. migration:verify --mode=schema, migration:copy mirror (copy set),
#      migration:copy backfill (every other table), migration:verify --mode=full
#
# Usage: scripts/rehearse-relayhistory-t0.sh [relayhistory-cloud ref] [sessions-pipeline ref]
#   BULLS_TARBALL=/path/to.tar  use a local sessions-pipeline tree (git archive) instead of cloning
set -euo pipefail
RH_REF=${1:-feat/t0-cutover-gate}
BULLS_REF=${2:-feat/supabase-production-migration}
NAME=rh-t0-$$
WORK=$(mktemp -d "/tmp/$NAME.XXXXXX")
PW=$(head -c 18 /dev/urandom | base64 | tr -dc A-Za-z0-9)

cleanup() {
  docker rm -f "$NAME-src" "$NAME-dst" >/dev/null 2>&1 || true
  docker network rm "$NAME" >/dev/null 2>&1 || true
  # Containers wrote node_modules as root.
  docker run --rm -v "$(dirname "$WORK")":/w node:24 rm -rf "/w/$(basename "$WORK")" >/dev/null 2>&1 || true
}
trap cleanup EXIT

step() { printf '\n== %s\n' "$*"; }

git clone -q --depth 1 -b "$RH_REF" git@github.com:AgentWorkforce/relayhistory-cloud.git "$WORK/rh"
if [ -n "${BULLS_TARBALL:-}" ]; then
  mkdir -p "$WORK/bulls" && tar -xf "$BULLS_TARBALL" -C "$WORK/bulls"
else
  git clone -q --depth 1 -b "$BULLS_REF" git@github.com:AgentWorkforce/sessions-pipeline.git "$WORK/bulls"
fi

docker network create "$NAME" >/dev/null
for db in src dst; do
  docker run -d --name "$NAME-$db" --network "$NAME" -e POSTGRES_PASSWORD="$PW" \
    pgvector/pgvector:pg17 >/dev/null
done
for db in src dst; do
  for _ in $(seq 1 60); do
    docker exec "$NAME-$db" pg_isready -U postgres >/dev/null 2>&1 && break
    sleep 1
  done
done
url() { printf 'postgres://postgres:%s@%s-%s:5432/postgres' "$PW" "$NAME" "$1"; }
psql_in() { docker exec -i "$NAME-$1" psql -v ON_ERROR_STOP=1 -q -U postgres -d postgres; }

step "target: 96Bulls supabase migrations (test-only role shim, then the role bootstrap as superuser)"
cat "$WORK/bulls/supabase/tests/supabase_shim.sql" "$WORK/bulls/supabase/bootstrap/session_directory_search_worker.sql" \
  "$WORK/bulls/supabase/bootstrap/session_token_verifier.sql" "$WORK/bulls/supabase/bootstrap/session_ingest_roles.sql" \
  "$WORK/bulls/supabase/bootstrap/session_read_api_roles.sql" \
  "$WORK/bulls/supabase/bootstrap/session_consumer_roles.sql" \
  "$WORK/bulls"/supabase/migrations/*.sql | psql_in dst

step "source and target: relayhistory-cloud runner over TCP"
docker run --rm --network "$NAME" -v "$WORK/rh":/rh -w /rh node:24 sh -c "
  npm ci --no-audit --no-fund >/dev/null 2>&1
  cd packages/relayhistory
  for target in '$(url src)' '$(url dst)'; do
    DATABASE_URL=\"\$target\" RELAYHISTORY_LEGACY_RETRACT_INDEX=1 node scripts/apply-neon-migration.mjs 2>&1 | grep -E 'Sessions schema ready|failed'
  done"

step "source: seed rows with live triggers"
psql_in src <<'SQL'
INSERT INTO sessions.auth_sessions (id, token_family_id, user_id, org_id, workspace_id,
  access_token_hash, access_token_expires_at, refresh_token_hash, refresh_token_expires_at)
SELECT 'tok-' || i, 'fam-' || i, 'user-1', 'org-1', 'ws-1', 'ah-' || i, now() + interval '1 day',
       'rh-' || i, now() + interval '30 days'
  FROM generate_series(1, 25) AS i;
INSERT INTO sessions.machines (org_id, workspace_id, machine_id, hostname, cursors_json)
SELECT 'org-1', 'ws-1', 'm-' || i, 'host-' || i, jsonb_build_object('claude', i)
  FROM generate_series(1, 5) AS i;
INSERT INTO sessions.convergence_events (org_id, workspace_id, machine_id, user_id, source,
  session_id, event_id, kind, type, ts, content, input_tokens, record)
SELECT 'org-1', 'ws-1', 'm-' || (i % 5 + 1), 'user-1', 'claude', 's-' || (i % 40),
       'e-' || i, 'history', 'message', timestamptz '2026-10-01' + i * interval '1 minute',
       'event ' || i || ' ' || repeat('x', i % 50), 9007199254740993 - i,
       jsonb_build_object('i', i, 'nested', jsonb_build_array(i, 'x'))
  FROM generate_series(1, 1200) AS i;
SQL
docker exec "$NAME-src" psql -At -U postgres -c \
  "SELECT 'source rows: ' || sum(n_live_tup) || ' across ' || count(*) FILTER (WHERE n_live_tup > 0) || ' tables'
     FROM pg_stat_user_tables WHERE schemaname = 'sessions'"

TABLES=$(docker exec "$NAME-src" psql -At -U postgres -c \
  "SELECT string_agg(tablename, ',' ORDER BY tablename) FROM pg_tables
    WHERE schemaname = 'sessions' AND tablename <> '__migrations' AND tablename NOT LIKE '%\\_rollout'")
COPY_SET=auth_sessions,deleted_upload_sessions,session_resets,upload_cleanup_origins,upload_cleanup_tombstones,machines,probe_connections,project_aliases,github_org_links,delivery_origins,delivery_receipts
REPROJECTED=session_catalog,session_relationships,session_markers,session_commit_links,session_catalog_agents
REST=$(printf '%s' "$TABLES" | tr ',' '\n' | grep -v -x -F -f <(printf '%s,%s' "$COPY_SET" "$REPROJECTED" | tr ',' '\n') | paste -sd, -)

step "96Bulls tools: verify schema, mirror copy set, backfill the rest, verify data"
docker run --rm --network "$NAME" -v "$WORK/bulls":/bulls -w /bulls \
  -e RELAYHISTORY_SOURCE_URL="$(url src)" -e BULLS_TARGET_URL="$(url dst)" \
  oven/bun:1 sh -c "
  bun install --frozen-lockfile >/dev/null 2>&1
  set -e
  bun run src/migration/relayhistory-verify.ts --mode=schema --source-role=postgres --target-role=postgres > /tmp/schema.json || true
  bun -e 'const r=JSON.parse(require(\"fs\").readFileSync(\"/tmp/schema.json\",\"utf8\"));console.log(\"schema passed:\",r.passed,\"| incompatible:\",r.tables.filter(t=>t.schema!==\"compatible\").map(t=>t.table).join(\" \")||\"none\",\"| objects checked:\",r.objects.checked,\"missing:\",r.objects.missing.length)'
  bun run src/migration/relayhistory-copy.ts --mode=mirror --tables=$COPY_SET --keep-target-only=machines --chunk-size=7 --execute --expect-target-host=$NAME-dst \
    | bun -e 'const r=JSON.parse(await Bun.stdin.text());console.log(\"mirror:\",r.tables.map(t=>t.table+\"=\"+t.read+\"/\"+t.written).join(\" \"))'
  bun run src/migration/relayhistory-sequence-fence.ts --apply --execute --expect-target-host=$NAME-dst | bun -e 'const r=JSON.parse(await Bun.stdin.text());console.log(\"sequence fence passed:\",r.passed,\"| sequences:\",r.sequences.length)'
  bun run src/migration/relayhistory-copy.ts --mode=backfill --tables=$REST --chunk-size=97 --checkpoint=/tmp/backfill.json --execute --expect-target-host=$NAME-dst \
    | bun -e 'const r=JSON.parse(await Bun.stdin.text());console.log(\"backfill:\",r.tables.filter(t=>t.read).map(t=>t.table+\"=\"+t.read+\"/\"+t.written).join(\" \"))'
  bun run src/migration/relayhistory-reproject.ts --batch=13 --execute --expect-target-host=$NAME-dst | bun -e 'const r=JSON.parse(await Bun.stdin.text());console.log(\"reproject keys:\",r.keys,\"batches:\",r.batches)'
  bun run src/migration/relayhistory-copy.ts --mode=check --tables=$REST,$COPY_SET --chunk-size=211 > /tmp/check.json || true
  bun -e 'const r=JSON.parse(require(\"fs\").readFileSync(\"/tmp/check.json\",\"utf8\"));console.log(\"check: missing rows\",r.tables.reduce((n,t)=>n+t.missing,0))'
  # Guard: without --execute nothing is written; a swapped target is refused.
  bun run src/migration/relayhistory-copy.ts --mode=backfill --tables=convergence_events > /tmp/dry.json && bun -e 'const r=JSON.parse(require(\"fs\").readFileSync(\"/tmp/dry.json\",\"utf8\"));console.log(\"dry run:\",r.dryRun,\"written\",r.tables[0].written)'
  RELAYHISTORY_SOURCE_URL=\"\$BULLS_TARGET_URL\" BULLS_TARGET_URL=\"\$RELAYHISTORY_SOURCE_URL\" bun run src/migration/relayhistory-copy.ts --mode=backfill --tables=convergence_events --execute --expect-target-host=$NAME-dst 2>&1 | grep -o \"refusing to write\" | sed \"s/^/swapped URLs: /\"
  bun run src/migration/relayhistory-sequence-fence.ts | bun -e 'const r=JSON.parse(await Bun.stdin.text());console.log(\"sequence fence re-check passed:\",r.passed)'
  bun run src/migration/relayhistory-verify.ts --mode=full --chunk-size=113 > /tmp/full.json || true
  bun -e 'const r=JSON.parse(require(\"fs\").readFileSync(\"/tmp/full.json\",\"utf8\"));const bad=r.tables.filter(t=>t.data&&t.data!==\"equal\"||t.schema!==\"compatible\");console.log(\"full verify passed:\",r.passed,\"| tables:\",r.tables.length,\"| not equal:\",bad.map(t=>t.table+\":\"+(t.data||t.schema)).join(\" \")||\"none\")'
  # After the faithful-copy check: rebuild intentionally recomputes derived state.
  bun run src/migration/relayhistory-rebuild.ts --since=2026-10-01T10:00:00Z --batch=7 --execute --expect-target-host=$NAME-dst | bun -e 'const r=JSON.parse(await Bun.stdin.text());console.log(\"rebuild:\",r.sessions,\"sessions\")'
"
step "target: no wakeups or revision events from history"
docker exec "$NAME-dst" psql -At -U postgres -c \
  "SELECT 'outbox=' || (SELECT count(*) FROM sessions.session_event_outbox) || ' revision_events=' || (SELECT count(*) FROM sessions.session_revision_events) || ' (source: outbox=' || 0 || ')'"
docker exec "$NAME-src" psql -At -U postgres -c \
  "SELECT 'source outbox=' || (SELECT count(*) FROM sessions.session_event_outbox) || ' revision_events=' || (SELECT count(*) FROM sessions.session_revision_events)"
