#!/usr/bin/env bash
# One local CI lane, the same commands GitHub runs (.github/workflows/ci.yml and pg-proof.yml), so
# flows/ci.flow.ts can start them side by side and a person can run one by hand:
#
#   bash flows/lib/lane.sh guards            the always-on checks (any verdict)
#   bash flows/lib/lane.sh typecheck         the stack and packages/pipeline typecheck
#   bash flows/lib/lane.sh shard <n>         unit-tests-shard n of 4, with its own disposable ClickHouse
#   bash flows/lib/lane.sh aux               unit-tests-aux: package tests, shard plan, G4-only re-runs, stacks/, receipt
#   bash flows/lib/lane.sh pg-proof <major>  pg-proof's real-postgres job on a disposable postgres:<major>
#   bash flows/lib/lane.sh e2e [args]        the pipeline e2e (e2e/pipeline/run.ts args)
#
# Every container is disposable (trust auth or a CI-only password, loopback port, removed on exit); no
# credential is read or written. Run from the repository root with CI's bun (1.4.2) first on PATH.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
lane="${1:?usage: lane.sh guards|typecheck|shard <n>|aux|pg-proof <major>}"
want_bun="$(sed -n 's/^ *bun-version: *//p' .github/workflows/ci.yml | head -1)"
have_bun="$(bun --version)"
if [ "$have_bun" != "$want_bun" ]; then
  echo "bun $have_bun is first on PATH; CI pins $want_bun. Put bun $want_bun first on PATH and rerun." >&2
  exit 1
fi
# ci.yml sets up node 24 beside bun (the Lambda archive proof runs node); an older node fails that test, not the change.
want_node="$(sed -n 's/^ *node-version: *//p' .github/workflows/ci.yml | head -1)"
have_node="$(node --version 2>/dev/null | sed 's/^v//; s/\..*//')"
if [ -n "$want_node" ] && [ "${have_node:-0}" -lt "$want_node" ]; then
  echo "node ${have_node:-none} is first on PATH; CI sets up node $want_node. Put node $want_node first on PATH and rerun." >&2
  exit 1
fi

container=""
cleanup() { [ -z "$container" ] || docker rm -f "$container" >/dev/null 2>&1 || true; }
trap cleanup EXIT INT TERM

# Starts a disposable container, waits for its health check, and prints its loopback host port.
start() { # name image internal-port health-cmd docker-run-args...
  local name="$1" image="$2" port="$3" health="$4"; shift 4
  container="$name"
  docker rm -f "$name" >/dev/null 2>&1 || true
  docker run -d --name "$name" -p "127.0.0.1::$port" --health-cmd "$health" --health-interval 2s --health-retries 60 "$@" "$image" >/dev/null
  for _ in $(seq 1 90); do
    [ "$(docker inspect -f '{{.State.Health.Status}}' "$name")" = healthy ] && break
    sleep 2
  done
  [ "$(docker inspect -f '{{.State.Health.Status}}' "$name")" = healthy ] || { echo "$image did not become healthy" >&2; docker logs "$name" | tail -20 >&2; exit 1; }
  docker port "$name" "$port/tcp" | head -1 | sed 's/.*://'
}
checkout="$(printf '%s' "$PWD" | shasum | cut -c1-8)"
run_id="$checkout-$$"
# A lane killed hard (the fleet's stop, a SIGKILL) skips its trap: remove this checkout's lane containers whose
# lane process is gone. Another checkout's, or a live lane's, are never touched.
for name in $(docker ps -a --format '{{.Names}}' 2>/dev/null | grep -E "^rh-ci-(ch|pg)-$checkout-[0-9]+-" || true); do
  pid="$(printf '%s' "$name" | sed -E "s/^rh-ci-(ch|pg)-$checkout-([0-9]+)-.*/\2/")"
  kill -0 "$pid" 2>/dev/null || docker rm -f "$name" >/dev/null 2>&1 || true
done

case "$lane" in
  guards)
    key="helm.sh/ho""ok"
    ! grep -rn "$key" --include='*.yaml' --include='*.yml' --include='*.tpl' . --exclude-dir=node_modules --exclude-dir=vendor --exclude-dir=.github || { echo "helm hooks are not allowed (Alchemy renders --no-hooks)"; exit 1; }
    ! git ls-files | grep -E '(^|/)(\.env|inputs\.json|.*\.pem)$' || { echo "secret-like file tracked"; exit 1; }
    bun scripts/check-engine-identifiers.ts
    ;;
  typecheck)
    bunx tsc --noEmit -p tsconfig.json
    bun run --filter @96bulls/pipeline typecheck
    ;;
  shard)
    n="${2:?shard number}"
    port="$(start "rh-ci-ch-$run_id-$n" clickhouse/clickhouse-server:26.8.34.13 8123 "clickhouse-client --password ci-only -q 'SELECT 1'" \
      -e CLICKHOUSE_PASSWORD=ci-only -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1)"
    list="$(mktemp)"
    bun scripts/ci/unit-test-shards.ts --shard "$n" --of 4 > "$list"
    echo "shard $n of 4: $(wc -l < "$list" | tr -d ' ') test files"
    files=()
    while IFS= read -r f; do files+=("$f"); done < "$list"
    rm -f "$list"
    [ "${#files[@]}" -gt 0 ]
    READ_API_TEST_CLICKHOUSE_URL="http://127.0.0.1:$port" READ_API_TEST_CLICKHOUSE_PASSWORD=ci-only bun test --timeout 30000 "${files[@]}"
    ;;
  aux)
    # packages/pipeline's `vitest run` with a 30 s case limit (vitest's default is 5 s), the same limit the root
    # suite runs under: beside the shards and the e2e on one machine, two CPU-bound cases (a 10.8 MB scrub, a
    # PGlite transaction) took 5-7 s while passing in 14 s for both files alone. GitHub runs the 5 s default.
    (cd packages/pipeline && bunx vitest run --testTimeout=30000)
    bun run --filter @relayhistory/object-store build
    bun run --filter @relayhistory/object-store test
    bun scripts/ci/unit-test-shards.ts --check --of 4
    for suite in ledger turns ordered-controls custody-baseline lazy-custody frozen-custody first-ack-seats; do
      HOOPDB_MIGRATE_UNTIL=20261105043800_lazy_ack_clock_no_key_update.sql bun test --timeout 30000 "src/session-ingest/__tests__/${suite}.pglite.test.ts"
    done
    bun test --timeout 30000 ./stacks
    out="$(mktemp)"
    bun scripts/scrub-bindgen-receipt.ts --out "$out"
    rm -f "$out"
    ;;
  pg-proof)
    major="${2:?postgres major}"
    port="$(start "rh-ci-pg-$run_id-$major" "postgres:$major" 5432 "pg_isready -U postgres" -e POSTGRES_HOST_AUTH_METHOD=trust)"
    head="$(git rev-parse HEAD)"
    out="$(mktemp -d)"
    if [ "$major" = 15 ]; then
      for file in rds_owner_function_default_revoke.sql rds_migration_roles.sql hoopdb_runtime_role_rds.sql; do
        if psql -X -h 127.0.0.1 -p "$port" -U postgres -d postgres -v ON_ERROR_STOP=1 -v VERBOSITY=verbose -v migration_owner=postgres \
          -v runtime_role=g7_rds_version_probe -v runtime_login=false -f "supabase/bootstrap/$file" > "$out/refusal.log" 2>&1; then
          echo "RDS setup unexpectedly accepted PostgreSQL 15: $file"; exit 1
        fi
        grep -Eq 'ERROR:[[:space:]]+0A000:' "$out/refusal.log"
        echo "$file refused PostgreSQL 15 with SQLSTATE 0A000"
      done
    fi
    SEARCH_CONFORMANCE_PG_URL="postgres://postgres@127.0.0.1:$port/postgres" bun run --filter @96bulls/pipeline test test/search-index-postgres-real.test.ts
    PGPROOF_AUTH_UPGRADE_DISPOSABLE=1 PGPROOF_HOST=127.0.0.1 PGPROOF_PORT="$port" PGPROOF_SUPERUSER=postgres PGPROOF_HEAD="$head" PGPROOF_OUT="$out/auth.json" \
      bun scripts/pg-proof/auth-upgrade-proof.ts
    PGPROOF_HOST=127.0.0.1 PGPROOF_PORT="$port" PGPROOF_SUPERUSER=postgres PGPROOF_HEAD="$head" PGPROOF_OUT="$out/role.json" \
      bun scripts/pg-proof/role-proof.ts
    jq -e --arg head "$head" --argjson major "$major" '.head == $head and .pg_major == $major and .failed == 0 and .passed > 0 and (.cases | all(.pass))' "$out/role.json" >/dev/null
    jq -e --arg head "$head" --argjson major "$major" '.head == $head and .pg_major == $major and .failed == 0 and .passed >= 12 and (.cases | all(.pass))' "$out/auth.json" >/dev/null
    jq -r '"role proof: \(.passed) passed, \(.failed) failed on PostgreSQL \(.pg_version)"' "$out/role.json"
    jq -r '"auth upgrade: \(.passed) passed, \(.failed) failed"' "$out/auth.json"
    rm -rf "$out"
    ;;
  e2e)
    shift
    bun run e2e/pipeline/run.ts "$@"
    ;;
  *)
    echo "unknown lane: $lane" >&2
    exit 2
    ;;
esac
