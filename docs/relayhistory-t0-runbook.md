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
   `bun run migration:verify --mode=schema --emit-sequence-fence > /secure/t0/schema.json`.
   `passed` must be `true`.
3. **Measure the copy set (read-only on Neon):**
   `SELECT relname, pg_total_relation_size(oid), n_live_tup FROM pg_stat_user_tables WHERE schemaname = 'sessions'`.
   Decide whether `delivery_origins` and `delivery_receipts` stay in the copy
   set or move to the backfill, keeping the in-hold catch-up under about 60
   seconds.
4. **[A4] Pre-copy and sequence fence.**
   ```sh
   COPY_SET=auth_sessions,deleted_upload_sessions,session_resets,upload_cleanup_origins,upload_cleanup_tombstones,machines,probe_connections,project_aliases,github_org_links,delivery_origins,delivery_receipts
   # machines is shared with the 96Bulls pipeline: never delete its pipeline-only rows
   bun run migration:copy --mode=mirror --tables=$COPY_SET --keep-target-only=machines --chunk-size=1000 > /secure/t0/precopy.json
   # apply the sequenceFenceSql statements from /secure/t0/schema.json on Supabase
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
   ceiling. Then revoke writes on Neon:
   `REVOKE INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA sessions FROM <worker_role>, <cloud_role>;`
10. **Catch up:** run
    `migration:copy --mode=mirror --tables=$COPY_SET --keep-target-only=machines`
    again.
    Then re-run the schema check and confirm every target sequence is above
    the source (the `sequences[].ready` field).
    **On any failure, abort:** move the phase `holding → neon`, re-grant
    writes on Neon, and the HoldLog replays to Neon.
11. **Flip:** move the phase `{"from":"holding","to":"supabase"}` and flip
    Cloud's flag. The HoldLogs replay in order against Supabase, and crons
    and consumers resume.
12. **Verify:** canaries for each route class (login, ingest, delivery batch,
    turns, session list, search, brief, a pre-T0 session through the Neon
    fallback). Hold logs are empty, 5xx rates are back to baseline, and
    `/health/ready` is green.

## After T0

13. **[A8] Backfill.** Copy every legacy table, base and derived, newest key
    first. The copier's session runs with triggers off
    (`session_replication_role = replica`), so history lands exactly as Neon
    has it. No revision feed, analysis wakeups, brief jobs or Cloud messages
    are generated for old sessions. `ON CONFLICT DO NOTHING` keeps every row
    written after T0.
    ```sh
    bun run migration:copy --mode=backfill --tables=<every legacy table, parents first> \
      --chunk-size=500 --checkpoint=/secure/t0/backfill.json
    ```
    Resume with the same checkpoint. Throttle by running it off-peak if
    Supabase p95 rises.
    **Straddling sessions** (rows on both sides of T0): their rollups, timing
    cache and catalog rows were built after T0 from partial data. After the
    backfill, mark them dirty through the live triggers, without the replica
    setting, so the database rebuilds them:
    insert their keys into `session_rollup_dirty` and `onboarding_rollup_dirty`,
    and delete their `session_timing_cache` rows. The straddling set is every
    session with a `convergence_events` row on both sides of T0.
14. **[A9]** Once the backfill verifier passes for every table, remove the
    Neon read fallback.

## Rollback

- **Before step 11:** abort as in step 10. Nothing has reached Supabase except
  the copy set, which Neon still holds.
- **After step 11:** do not repoint to Neon. Supabase alone has the writes
  made since T0. Run a new hold, mirror the rows above the sequence fence back
  to Neon, re-grant writes on Neon, and only then move the Worker back. That
  needs a reverse gate deploy and its own approval.
