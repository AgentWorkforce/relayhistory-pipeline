# RelayHistory T0 runbook: Neon to Supabase

Owner: `96-no-bull-migration`. Target date: 2026-10-31. The design is in
`docs/relayhistory-supabase-migration.md` and `docs/relayhistory-t0-write-hold.md`.

**Every step marked `[A#]` needs Khaliq's explicit OK, in his own words, for
that exact action.** This document is not that approval (AGENTS.md). Steps
without a marker are read-only or local.

Never put a connection string on a command line, in a log, in chat or in a
file. Read each one into the operator's shell with `op read`, not echoed.

```sh
read -r -s RELAYHISTORY_SOURCE_URL < <(op read "op://<vault>/<neon read-only>/url")
read -r -s BULLS_TARGET_URL        < <(op read "op://96Bulls-prod/<supabase owner>/url")
export RELAYHISTORY_SOURCE_URL BULLS_TARGET_URL
```

## Approval checklist

| # | Action | Reversible? |
| --- | --- | --- |
| A1 | Create the Supabase credentials for `sessions` (`pyamkrhoyszgzirqqffq`): a schema-owner role for migrations and a runtime role for the Worker (owner of, or `BYPASSRLS` on, `sessions.machines`), both in 1Password `96Bulls-prod`; plus a read-only Neon role for copy and verify | yes (drop roles) |
|    | Read-only check on Supabase, required before A3: `SELECT has_parameter_privilege(current_user, 'session_replication_role', 'SET')` must be true for the copy role, or the copier refuses to run | — |
|    | Read-only check: `SELECT datcollate, datctype, version() FROM pg_database WHERE datname = current_database()` on Neon and Supabase. If the collation differs, add `COLLATE "C"` to the verifier's text-key ranges and re-check that prefix-search plans use their indexes | — |
| A2 | Size the Supabase compute and disk for the eventual ~486 GiB plus growth | yes |
| A3 | Apply the schema to Supabase: the 96Bulls migrations (`supabase db push`), then relayhistory-cloud's runner and rollouts with `DATABASE_MIGRATIONS_URL` set to Supabase | empty database: yes (drop schema) |
| A4 | Pre-copy the T0 copy set (mirror) and apply the sequence fence on Supabase | yes, while Supabase takes no traffic |
| A5 | Merge relayhistory-cloud #223, #224, the HoldLog and lane 4 (driver seam); set the Worker secrets `SUPABASE_DATABASE_URL` and `CUTOVER_ADMIN_SECRET`; set the repository variables `CUTOVER_GATE_ENABLED=true` and `SUPABASE_PROJECT_REF`; deploy. The phase stays `neon`, so behaviour is unchanged | yes (unset the variable, redeploy) |
| A6 | Deploy Cloud's change that moves its three direct `sessions` calls behind a flag, default off | yes |
| A7 | **T0:** move the gate to `holding`, make Neon read-only for the Worker and Cloud roles, catch up the copy set, then move the gate to `supabase` and flip Cloud's flag | before the flip: abort to `neon`; after: see Rollback |
| A8 | Start the history backfill | yes (stop it; rows are additive) |
| A9 | Remove the Neon read fallback once the verifier passes | yes |
| A10 | Retire Neon (much later) | no |

## Before T0

1. **Gates green (local):** in 96BullSessions,
   `packages/pipeline/test/relayhistory-schema-parity.test.ts` re-pinned at
   the relayhistory-cloud commit being deployed, and
   `relayhistory-copy.test.ts`. In relayhistory-cloud, the `cutover-*` tests
   and the lane 4 contract suite on PostgreSQL 17.
2. **[A3] Schema on Supabase.**
   ```sh
   scripts/sb db push            # 96Bulls supabase/migrations (wrapper; never a saved login)
   # relayhistory-cloud, with DATABASE_MIGRATIONS_URL/DATABASE_URL set to Supabase:
   SUPABASE_PROJECT_REF=pyamkrhoyszgzirqqffq node scripts/check-database-target.mjs
   (cd packages/relayhistory && RELAYHISTORY_LEGACY_RETRACT_INDEX=1 node scripts/apply-neon-migration.mjs)
   node packages/relayhistory/scripts/rollout-delivery-projection.mjs
   node packages/relayhistory/scripts/rollout-session-rollups.mjs
   node packages/relayhistory/scripts/rollout-onboarding-rollups.mjs
   node packages/relayhistory/scripts/rollout-upload-cleanup.mjs
   ```
   On an empty database each rollout completes immediately. Then run the live
   schema check:
   ```sh
   bun run migration:verify --mode=schema \
     --source-role=<neon runtime role> --target-role=<supabase runtime role> > /secure/t0/schema.json
   ```
   `passed` must be `true`. It covers columns (type, nullability, default,
   generated, identity, collation), every valid index, constraint, trigger,
   full function definition, row security and policies, and types. The Neon
   runtime role's table, sequence, function and schema privileges must all be
   held by the Supabase role, which must not be filtered by RLS where the Neon
   role was not. Only two differences are allowed, both on the shared
   `machines`: RLS on the target, and the `workspace_id` default. Applying the
   96Bulls migrations revokes PUBLIC's EXECUTE on `sessions` functions, so the
   Supabase runtime role needs explicit grants (the runner's runtime-role
   grants), or this gate fails.
3. **Measure the copy set (read-only on Neon):**
   `SELECT relname, pg_total_relation_size(oid), n_live_tup FROM pg_stat_user_tables WHERE schemaname = 'sessions'`.
   Decide whether `delivery_origins` and `delivery_receipts` stay in the copy
   set or move to the backfill, keeping the in-hold catch-up under about 60
   seconds.
4. **[A4] Pre-copy and sequence fence.**
   ```sh
   COPY_SET=auth_sessions,deleted_upload_sessions,session_resets,upload_cleanup_origins,upload_cleanup_tombstones,machines,probe_connections,project_aliases,github_org_links,delivery_origins,delivery_receipts
   # machines is shared with the 96Bulls pipeline: never delete its pipeline-only rows
   # dry run first (read-only), then the same command with the guard:
   bun run migration:copy --mode=mirror --tables=$COPY_SET --keep-target-only=machines --chunk-size=1000
   bun run migration:copy --mode=mirror --tables=$COPY_SET --keep-target-only=machines --chunk-size=1000 \
     --execute --expect-supabase-ref=pyamkrhoyszgzirqqffq > /secure/t0/precopy.json
   # every Supabase sequence >= Neon's + 1,000,000 (never-used ones from their start)
   bun run migration:sequence-fence --apply --execute --expect-supabase-ref=pyamkrhoyszgzirqqffq
   ```
5. **[A5] Deploy the gate and the driver.** The phase stays `neon`. Check that
   `GET /v1/internal/cutover/phase` returns `neon`, and that health, upload and
   read canaries are unchanged.
6. **[A6] Deploy Cloud** with its flag off.
7. **Benchmarks (read-only):** run `benchmark:relayhistory` against Neon and
   Supabase. Supabase is still nearly empty, so this checks plans and indexes,
   not absolute latency. Record the results.

## T0 (target: under 5 minutes of hold)

8. **[A7] Hold:**
   `curl -X POST .../v1/internal/cutover/phase -H "authorization: Bearer $CUTOVER_ADMIN_SECRET" -d '{"from":"neon","to":"holding"}'`.
   Buffered uploads queue up, other writes get 503, reads keep working, and
   crons and consumers pause.
9. **Drain.** Wait until Neon's `pg_stat_activity` shows no active non-idle
   session for the Worker or Cloud runtime roles, up to the 125 s statement
   ceiling. Then fence `sessions` on Neon. The Worker reuses Cloud's runtime
   role, so this is scoped to the schema, never role-wide read-only. Plan it
   ahead, review it, and apply the `fence` statements as the schema owner:
   ```sh
   bun run migration:neon-fence --roles=<runtime_role> > /secure/t0/neon-fence.json
   # apply .fence now; keep .restore for the abort path
   ```
   The fence revokes table DML and `EXECUTE` on every volatile `sessions`
   function, which catches the `SECURITY DEFINER` writers. Reads and stable
   functions keep working, and Cloud's `public` schema is untouched.
10. **Catch up:** run
    `migration:copy --mode=mirror --tables=$COPY_SET --keep-target-only=machines --execute --expect-supabase-ref=pyamkrhoyszgzirqqffq`
    again.
    Then re-apply and re-check the sequence fence against Neon's final values
    (Supabase has taken no writes yet, so raising is safe), and re-run the
    schema gate:
    `bun run migration:sequence-fence --apply --execute --expect-supabase-ref=pyamkrhoyszgzirqqffq`
    must report `passed: true`.
    **On any failure, abort:** apply `.restore` from `/secure/t0/neon-fence.json`
    on Neon, then move the phase `holding → neon`. The HoldLog replays to Neon.
11. **Flip:** move the phase `{"from":"holding","to":"supabase"}` and flip
    Cloud's flag. The HoldLogs replay in order against Supabase, and crons
    and consumers resume.
12. **Verify:** canaries for each route class (login, ingest, delivery batch,
    turns, session list, search, brief, a pre-T0 session through the Neon
    fallback). Hold logs are empty, 5xx rates are back to baseline, and
    `/health/ready` is green.

## After T0

13. **[A8] Backfill.** Copy every legacy table except the catalog-derived
    ones, newest first (by an indexed time column, else by key). The copier's session runs with triggers off
    (`session_replication_role = replica`), so history lands exactly as Neon
    has it. No revision feed, analysis wakeups, brief jobs or Cloud messages
    are generated for old sessions. `ON CONFLICT DO NOTHING` keeps every row
    written after T0.
    ```sh
    # every legacy table except per-database state (__migrations, *_rollout) and the
    # catalog-derived tables (the copier refuses them; step 13b rebuilds them)
    bun run migration:copy --mode=backfill --tables=<legacy tables> \
      --chunk-size=500 --checkpoint=/secure/t0/backfill.json \
      --execute --expect-supabase-ref=pyamkrhoyszgzirqqffq
    ```
    The backfill never brings back what users deleted on Supabase after T0. It
    skips rows matching a `deleted_upload_sessions` tombstone, and all history
    of users with a reset recorded after T0. Each page runs under the same
    per-user advisory locks as the deletion functions.
    Resume with the same checkpoint. Throttle by running it off-peak if
    Supabase p95 rises.
    **Straddling sessions** (rows on both sides of T0): their rollups and
    timing cache were built after T0 from partial data. After the backfill,
    rebuild them through the live triggers (a normal session, small
    transactions):
    ```sh
    bun run migration:rebuild --since=<T0 ISO timestamp> --batch=200 \
      --execute --expect-supabase-ref=pyamkrhoyszgzirqqffq
    ```
    It marks every session with an event at or after T0 in
    `session_rollup_dirty` and `onboarding_rollup_dirty`. Their deferred
    triggers rebuild the session from all of its rows, and the tool drops the
    session's `session_timing_cache` rows.
13b. **Catalog reprojection.** `session_catalog`, `session_relationships`,
    `session_markers`, `session_commit_links` and `session_catalog_agents` are
    never copied. A trigger-off copy would resurrect rows that a post-T0
    tombstone, or a post-T0 winner without an agent name, removed. Once
    `delivery_records` and `delivery_catalog_acceptance` are backfilled,
    reproject every key from all records:
    ```sh
    bun run migration:reproject --batch=500 \
      --execute --expect-supabase-ref=pyamkrhoyszgzirqqffq
    ```
    It covers every live upsert's catalog key plus the key of every existing
    derived row. Each page is one transaction, under the workspace's
    delivery-projection lock, through `project_delivery_catalog_key`. It is
    idempotent: a second run changes no row. It holds workspace locks while it
    runs, so schedule it off-peak.
    **Residual risk:** a client that re-uploads an *older* revision of a
    record after T0 is accepted as new and wins the record-key conflict over
    the backfill. Uploaders send forward deltas, and HoldLog replays are newer
    than Neon, so this needs an unusual client.
14. **[A9]** Remove the Neon read fallback only once all of these hold:
    - `migration:copy --mode=check --tables=<backfilled tables>` exits 0. It
      counts only rows the backfill should have brought: rows a post-T0
      tombstone or reset removed don't count as missing.
    - The straddling rebuild (`migration:rebuild`) and the reprojection
      (`migration:reproject`) have run.
    - A second `migration:reproject` dry run reports the same key count.

## Rehearsal evidence

`scripts/rehearse-relayhistory-t0.sh` runs the data path on two throwaway
PostgreSQL 17 + pgvector containers: both schemas, a seed with live triggers,
schema verify, copy-set mirror, backfill and full verify. On 2026-10-30 (linuyc,
relayhistory-cloud `feat/t0-cutover-gate`, this branch) the schema verify
passed, the mirror copied the copy set, and the backfill copied every remaining
table (including `session_rollups` and `session_timing_cache` through the
nullable-key path). Full verify passed on all 55 tables, with the five
per-database state tables skipped. The outbox and revision-event counts on the
target equalled the source's: copies, not trigger output.

## Rollback

- **Before step 11:** abort as in step 10. Nothing has reached Supabase except
  the copy set, which Neon still holds.
- **After step 11:** do not repoint to Neon. Supabase alone has the writes
  made since T0. Under a new hold, with its own approval:
  ```sh
  # dry run first, then with the guard
  SUPABASE_SOURCE_URL=... NEON_TARGET_URL=... bun run migration:reverse \
    --since=<T0 ISO> --tables=<legacy tables> --execute \
    --expect-supabase-ref=pyamkrhoyszgzirqqffq --expect-neon-host=<neon endpoint host>
  ```
  1. It replays on Neon the deletions Supabase recorded after T0.
  2. It upserts every row written since T0, Supabase winning.
  3. Then apply the Neon fence's `.restore`, and only then move the Worker
     back. That needs a reverse gate deploy.
