#!/usr/bin/env bash
# Run from the reviewed checkout with installed workspace dependencies.
# Accepts one local PG installation's bin directory. Never consumes a DB URL.
set -euo pipefail
bin=$(realpath "${1:?pass the local PostgreSQL bin directory}")
repo=$(git rev-parse --show-toplevel)
scratch=$(mktemp -d)
export LD_LIBRARY_PATH="$bin/../lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
trap 'if [[ -f "$scratch/data/postmaster.pid" ]]; then "$bin/pg_ctl" -D "$scratch/data" -m immediate -w stop; fi; rm -rf -- "$scratch"' EXIT
mkdir "$scratch/tree"
git -C "$repo" archive HEAD | tar -xf - -C "$scratch/tree"
# Bun resolves dependencies from each workspace package after a frozen install.
(cd "$scratch/tree" && bun install --frozen-lockfile >/dev/null)
"$bin/initdb" -D "$scratch/data" -U fixture_owner -E UTF8 --locale=C -A trust >/dev/null
"$bin/pg_ctl" -D "$scratch/data" -l "$scratch/server.log" -o "-c listen_addresses='' -k $scratch -p 5549" -w start
PUBLICATION_PROOF_SCRATCH="$scratch" bun "$scratch/tree/supabase/proposals/continuous-publication/qualify-ordered.ts"
