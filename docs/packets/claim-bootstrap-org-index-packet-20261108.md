# Claim bootstrap `org_id` index packet (out-of-band, operator)

Status: informational. No production change is made by this document or the PR
that adds it. The table set, row counts and sizes below are the credentialed
operator's production measurements (receipt 37339295, snapshot
2026-11-08T23:59:39Z), as ruled by the pipeline lead (245340973846945792). Build
times are still estimates. The operator re-runs the read-only SQL in this packet
immediately before building, because the snapshot is a point in time.

## Purpose

The first claim for a new org runs `sessions.ingest_lazy_begin`, whose current
body is in `supabase/migrations/20261105041000_ingest_privacy_authority.sql`. When
the admission insert at :893 is new (`IF v_new IS TRUE`), the loop at :901-913
goes through every `sessions` table matching `ingest_%` / `upload_%` that has an
`org_id` column, minus the documented exclusions. For each one it runs
`SELECT EXISTS(SELECT 1 FROM sessions.%I WHERE org_id=$1)` (:911). On `main`
that is 43 tables; in production (snapshot 23:59:39Z) it is 37. On 8 of the
production tables, no valid, non-partial btree index or primary key has
`org_id` as its first column, so each probe is a sequential scan, done while the
admission row lock is held. The same lazy path's user loop
(`sessions.ingest_lazy_user`, same file, :776-789, 22 tables on `main`,
predicate `org_id=$1 AND workspace_id=$2 AND user_id=$3` at :787) seq-scans 5
of those 8 tables too.

This packet lists the out-of-band `CREATE INDEX CONCURRENTLY` statements the
operator can run to turn those probes into index probes. It also covers the
measurements to take first, the lock behaviour, the build budgets, cancellation,
the order of operations, the inverse statements and how to verify the result.
CONCURRENTLY cannot run inside a transaction block, so these indexes cannot go
through `src/hoopdb/migrate.ts`, which wraps every migration in BEGIN/COMMIT
(SQLSTATE 25001). The precedent is
`scripts/ops/c3-privacy-authority/ingest_privacy_authority_index.sql`. The last
section explains why a later catch-up migration must be designed to adopt these
indexes rather than naively recreating them.

## How the list was built (static from `main` at 38149b52, corrected to production)

- Org loop selection (041000:901-910): `pg_class` in `sessions`, `relkind='r'`,
  name `LIKE 'ingest_%' OR LIKE 'upload_%'`, has an `org_id` attribute. It
  excludes `ingest_buffer_admission`, the six frozen-source tables,
  `ingest_frozen_source_owned_sessions` and `ingest_source_control_heads`.
- User loop selection (041000:776-786): the same filter, excluding
  `ingest_fence_owners` and the same frozen/control tables, and requiring
  `org_id`, `workspace_id` and `user_id`. It runs only when the user has no
  seal and is new.
- "Covered" means a valid btree index or PK whose first column is `org_id` and
  that has no partial predicate. A partial index does not count, because the
  probe's `WHERE org_id=$1` does not imply its predicate.
- The loop is dynamic over the live catalog. If production has drifted, the
  catalog query in "Measurement SQL" (M0) is authoritative and this table is not.
- Production correction (receipt 37339295, snapshot 23:59:39Z): the static read
  of `main` found 10 uncovered tables. Two of them, `ingest_acquisition_operations`
  and `ingest_acquisition_revoked_grants`, are absent in production as of the
  23:59:39Z snapshot (their migration 043850 is not applied there, consistent
  with the schema-window freeze). They get their index from their own migration
  line, not from this packet. That leaves the 8 tables below.

## Uncovered tables (8)

Each statement is a single command run with autocommit, never inside BEGIN.
The numbering is the operator's build order. Rows and sizes are measured,
snapshot 23:59:39Z.

| # | Table | Loops | Index columns | Statement | Why these columns | Rows / size (measured) | Lock impact |
|---|-------|-------|---------------|-----------|-------------------|------------------------|-------------|
| 1 | `ingest_history_mask_pages` | org | `org_id` | `CREATE INDEX CONCURRENTLY IF NOT EXISTS ingest_history_mask_pages_org_id_idx ON sessions.ingest_history_mask_pages (org_id);` | Narrow: org probe only; other access is by PK `sha256` | empty | SHARE UPDATE EXCLUSIVE on this table only; DML continues; seconds |
| 2 | `ingest_history_plans` | org + user | `org_id, workspace_id, user_id` | `CREATE INDEX CONCURRENTLY IF NOT EXISTS ingest_history_plans_org_id_idx ON sessions.ingest_history_plans (org_id, workspace_id, user_id);` | Covering for both probes | empty | Same; seconds |
| 3 | `ingest_seed_captures` | org | `org_id` | `CREATE INDEX CONCURRENTLY IF NOT EXISTS ingest_seed_captures_org_id_idx ON sessions.ingest_seed_captures (org_id);` | Narrow: org probe only (table has no `user_id`) | empty | Same; seconds |
| 4 | `ingest_publication_attempts` | org + user | `org_id, workspace_id, user_id` | `CREATE INDEX CONCURRENTLY IF NOT EXISTS ingest_publication_attempts_org_id_idx ON sessions.ingest_publication_attempts (org_id, workspace_id, user_id);` | Covering for both probes | empty | Same; seconds |
| 5 | `ingest_publication_generations` | org + user | `org_id, workspace_id, user_id` | `CREATE INDEX CONCURRENTLY IF NOT EXISTS ingest_publication_generations_org_id_idx ON sessions.ingest_publication_generations (org_id, workspace_id, user_id);` | Covering for both probes | empty | Same; seconds |
| 6 | `ingest_stage_intents` | org + user | `org_id, workspace_id, user_id` | `CREATE INDEX CONCURRENTLY IF NOT EXISTS ingest_stage_intents_org_id_idx ON sessions.ingest_stage_intents (org_id, workspace_id, user_id);` | Covering for both probes; existing indexes lead with `stage_token`, `stage_key`, `claim_token` or `cleanup_state` | 356k rows, 0.9 GB | Same; first of the big three |
| 7 | `ingest_custody_records` | org + user | `org_id, workspace_id, user_id` | `CREATE INDEX CONCURRENTLY IF NOT EXISTS ingest_custody_records_org_id_idx ON sessions.ingest_custody_records (org_id, workspace_id, user_id);` | Covering for both probes; `(org_id)` alone would make the user probe for a new user in a big org scan the whole org's rows | 3.3M rows, 7 GB | Same; second of the big three |
| 8 | `ingest_lazy_native_slots` | org | `org_id` | `CREATE INDEX CONCURRENTLY IF NOT EXISTS ingest_lazy_native_slots_org_id_idx ON sessions.ingest_lazy_native_slots (org_id);` | Narrow; the existing `ingest_lazy_native_current_owner` is partial (`WHERE current_owner IS NOT NULL`) and unusable for the probe | 18.6M rows, 24 GB total, 21 GB heap | Same; largest, built last; can take tens of minutes |

The big three (6, 7 and 8) are what the claim bootstrap scans actually pay for
today. The five empty tables (1-5) build in seconds; indexing them now keeps the
probes cheap as they fill.

Naming: all names follow `<table>_org_id_idx`. On `main`, no index, constraint
or other relation with any of these names exists in `supabase/`, `src/`,
`scripts/` or `docs/`. The longest name is 41 characters, under the
63-character limit. The name is kept for the three-column indexes too, so one
scheme covers all eight. Production collisions are checked by M4 before any build.

Evidence of absence (CREATE TABLE line, PK and every existing index), in build order:

1. `ingest_history_mask_pages`:
   - CREATE TABLE at `20261105026000_canonical_history_lineage.sql:708`; PK `sha256` (:709).
   - No index.
2. `ingest_history_plans`:
   - CREATE TABLE at 026000:374; PK `plan_token` (:375).
   - No index.
3. `ingest_seed_captures`:
   - CREATE TABLE at `20261104040000_ingest_s3_proof_seed.sql:122`; PK `id` (:123).
   - No index.
4. `ingest_publication_attempts`:
   - CREATE TABLE at `20261104060000_ingest_canonical_publication.sql:42`; PK `attempt_token` (:43).
   - Index `(stage_token, lease_until)` (:53).
5. `ingest_publication_generations`:
   - CREATE TABLE at 060000:26; PK `generation` and `UNIQUE` prefix (:27).
   - No index.
6. `ingest_stage_intents`:
   - CREATE TABLE at `20261104050000_buffered_upload_receipts.sql:20`; PK `stage_token` (:21).
   - `UNIQUE stage_key` (:24); `UNIQUE (claim_token, stage_key)` (:35).
   - Index `(cleanup_state, created_at, stage_token)` (:38); partial expression index `ingest_stage_gc_scan_idx` (`20261104051000_ingest_buffer_gc.sql:17-18`).
7. `ingest_custody_records`:
   - CREATE TABLE at `20261104054000_ingest_custody_baseline.sql:38`; PK `key_sha256` (:39).
   - `UNIQUE (import_token, source_key_sha256)` (:47) and `ingest_custody_records_import (import_token, page)` (:49).
   - Altered at `20261104055000_lazy_current_custody.sql:76,:81`, with no index added.
8. `ingest_lazy_native_slots`:
   - CREATE TABLE at `20261104055000_lazy_current_custody.sql:43`; PK `key_sha256` (:44).
   - Only org-leading index is partial, at :57-58.

Not in scope, but noted: four user-loop tables have an `org_id`-leading key that
is not an `(org_id, workspace_id, user_id)` prefix:
- `ingest_batches` PK;
- `ingest_buffer_outbox` PK;
- `ingest_claim_attempts_batch_idx`;
- `ingest_tenant_slots` PK `(org_id, user_id, holder)`.

For a new user they visit the org's (or org+workspace's) rows through the index
instead of scanning the heap. That is bounded and acceptable for now; measure
before acting on it. The other 13 user-loop tables on `main` have an
`(org_id, workspace_id, user_id)` prefix. The other org-loop tables are
covered (33 on `main`; 29 of the 37 in production).

## Measurement SQL (read-only, counts and sizes only)

Run these from the operator's direct connection. None of them write anything.
Do not paste query text, client addresses or identifiers into tickets. Counts
and sizes are enough.

M0, the loop's table set and coverage from the live catalog. If this
disagrees with the table above, the live catalog wins.

```sql
WITH loop AS (
  SELECT t.oid, t.relname,
         (SELECT a.attnum FROM pg_attribute a
           WHERE a.attrelid=t.oid AND a.attname='org_id' AND a.attnum>0 AND NOT a.attisdropped) AS org_att
  FROM pg_class t JOIN pg_namespace n ON n.oid=t.relnamespace
  WHERE n.nspname='sessions' AND t.relkind='r' AND (t.relname LIKE 'ingest_%' OR t.relname LIKE 'upload_%')
    AND t.relname<>'ingest_buffer_admission'
    AND t.relname NOT IN ('ingest_frozen_source_records','ingest_frozen_source_deletions','ingest_frozen_source_resets',
      'ingest_frozen_source_org_seals','ingest_frozen_source_completeness','ingest_frozen_source_receipts',
      'ingest_frozen_source_owned_sessions') AND t.relname<>'ingest_source_control_heads'
)
SELECT l.relname,
       EXISTS (SELECT 1 FROM pg_index i JOIN pg_class ic ON ic.oid=i.indexrelid
                JOIN pg_am am ON am.oid=ic.relam
               WHERE i.indrelid=l.oid AND am.amname='btree' AND i.indpred IS NULL
                 AND i.indisvalid AND i.indkey[0]=l.org_att) AS org_leading_covered
FROM loop l WHERE l.org_att IS NOT NULL
ORDER BY org_leading_covered, l.relname;
```

The selection is copied from 041000:901-910, including the unescaped `LIKE`
patterns, so it returns exactly the tables the function probes. In production
at snapshot 23:59:39Z it returned 37 rows, 8 of them with
`org_leading_covered = false` (exactly the 8 tables above). On a database built
from `main` it returns 43 rows, 10 uncovered.

M1, size and planner row estimate per target table:

```sql
SELECT c.relname, c.reltuples::bigint AS est_rows,
       pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size,
       pg_size_pretty(pg_relation_size(c.oid)) AS heap_size
FROM pg_class c
WHERE c.relnamespace='sessions'::regnamespace
  AND c.relname IN ('ingest_history_mask_pages','ingest_history_plans','ingest_seed_captures',
    'ingest_publication_attempts','ingest_publication_generations',
    'ingest_stage_intents','ingest_custody_records','ingest_lazy_native_slots')
ORDER BY pg_total_relation_size(c.oid);
```

`reltuples = -1` means the table has never been analyzed. Treat its size as the
guide.

M2, sequential-scan pressure (to confirm that the probe is what is scanning):

```sql
SELECT relname, seq_scan, seq_tup_read, idx_scan, n_live_tup
FROM pg_stat_user_tables
WHERE schemaname='sessions'
  AND relname IN ('ingest_history_mask_pages','ingest_history_plans','ingest_seed_captures',
    'ingest_publication_attempts','ingest_publication_generations',
    'ingest_stage_intents','ingest_custody_records','ingest_lazy_native_slots')
ORDER BY seq_tup_read DESC;
```

M3, long transactions (each one delays a CIC build, see Lock impact):

```sql
SELECT pid, now()-xact_start AS xact_age, state
FROM pg_stat_activity
WHERE xact_start < now()-interval '1 minute'
ORDER BY xact_start;
```

M4, name collisions and leftover INVALID indexes (run before and after each build):

```sql
SELECT c.relname, c.relkind
FROM pg_class c
WHERE c.relnamespace='sessions'::regnamespace AND c.relname LIKE '%\_org\_id\_idx';

SELECT indexrelid::regclass FROM pg_index WHERE NOT indisvalid;
```

M5, ownership. CREATE INDEX requires the table owner, so run the builds as the
role this query returns (the migration owner role), not as an application role:

```sql
SELECT DISTINCT pg_get_userbyid(relowner) FROM pg_class
WHERE relnamespace='sessions'::regnamespace AND relname LIKE 'ingest\_%';
```

M6, IO pressure baseline (read-only; take it before the big three and repeat it
during each of their builds, see the stop rule):

```sql
SELECT count(*) AS io_waiters FROM pg_stat_activity WHERE wait_event_type = 'IO';

SELECT max(replay_lag) AS max_replay_lag FROM pg_stat_replication;
```

`max_replay_lag` is NULL when there is no streaming replica, or when the role
lacks `pg_monitor`; then rely on the other observables.

## Build sizes and budgets (measured, snapshot 23:59:39Z)

| # | Table | Rows | Size | Expected build time (estimate) | Proposed `statement_timeout` |
|---|-------|------|------|--------------------------------|------------------------------|
| 1 | `ingest_history_mask_pages` | empty | empty | seconds | `2min` |
| 2 | `ingest_history_plans` | empty | empty | seconds | `2min` |
| 3 | `ingest_seed_captures` | empty | empty | seconds | `2min` |
| 4 | `ingest_publication_attempts` | empty | empty | seconds | `2min` |
| 5 | `ingest_publication_generations` | empty | empty | seconds | `2min` |
| 6 | `ingest_stage_intents` | 356k | 0.9 GB | about a minute | `15min` |
| 7 | `ingest_custody_records` | 3.3M | 7 GB | minutes | `45min` |
| 8 | `ingest_lazy_native_slots` | 18.6M | 24 GB total, 21 GB heap | tens of minutes | `120min` |

The budgets are proposals. The operator may adjust any of them, and must record
the value actually used for each table with the build result.

CIC time is dominated by two full table scans (build, then validate) and by the
waits for transactions that are older than each phase. Under write load,
expect the upper end of each estimate. One long transaction from M3 can stretch
any build until it finishes, and that wait counts against the table's
`statement_timeout`.

## Lock impact and session rules

- The lock taken is SHARE UPDATE EXCLUSIVE on the one table. Normal SELECT,
  INSERT, UPDATE and DELETE are not blocked. It does conflict with autovacuum
  and ANALYZE (autovacuum yields, except anti-wraparound), with
  `VALIDATE CONSTRAINT`, with other DDL on the table and with another CIC on
  the same table. Build one table at a time.
- It cannot run inside a transaction block, so it cannot go through the
  migrator. Use `psql` with autocommit on (the default) and one statement per
  command. Never wrap in BEGIN, and do not use `psql -1` or `--single-transaction`.
- Use a direct connection, not Hyperdrive or the pgbouncer pooler. A
  transaction-mode pooler does not keep session `SET`s, and a pooled
  connection may be recycled mid-build.
- (a) Finite build budget. Session settings before each statement, with the
  table's budget from "Build sizes and budgets":
  `SET statement_timeout = '<budget>'; SET lock_timeout = '5s';`
  (for example `SET statement_timeout = '120min';` before table 8). Never use
  `statement_timeout = 0` for these builds. A build that exceeds its budget is
  cancelled with SQLSTATE 57014 and leaves an INVALID index; handle it with (c).
- `lock_timeout` applies to every lock wait inside CIC, not only the initial
  table lock. That includes the waits on older transactions' virtual xids
  between phases. So with `5s`, any transaction older than about 5 s that is
  still running at a phase boundary cancels the build (SQLSTATE 55P03) and
  leaves an INVALID index. Those waits do not block application DML. If 5 s
  keeps cancelling builds, raising it for the session (for example
  `SET lock_timeout = '60s'`) is safe. The trade-off is only that it waits
  longer behind a conflicting DDL or vacuum.
- (b) Cancellation and progress. Keep a second direct session open as the same
  owner role (or a role with `pg_signal_backend`). Find the build's backend by
  its query text, without selecting the text itself:

  ```sql
  SELECT pid, now()-query_start AS running_for, wait_event_type, wait_event
  FROM pg_stat_activity
  WHERE query ILIKE 'CREATE INDEX CONCURRENTLY%\_org\_id\_idx%'
    AND pid <> pg_backend_pid();
  ```

  Watch progress with:

  ```sql
  SELECT pid, relid::regclass AS tbl, phase,
         blocks_done, blocks_total, tuples_done, tuples_total
  FROM pg_stat_progress_create_index;
  ```

  To cancel, run `SELECT pg_cancel_backend(<pid>);` with the pid from the
  first query. Cancelling leaves an INVALID index; handle it with (c).
- (c) INVALID-index handling. A cancelled, timed-out or failed CONCURRENTLY
  build leaves an INVALID index behind. It costs write overhead and the planner
  never uses it. Before any retry, run
  `SELECT indexrelid::regclass FROM pg_index WHERE NOT indisvalid AND indrelid = 'sessions.<table>'::regclass;`.
  If it returns a row, run `DROP INDEX CONCURRENTLY sessions.<name>;` (same
  session rules, autocommit), confirm the check now returns nothing, and then
  retry the build once. If the retry also fails, stop and re-plan; do not loop.
  This step is mandatory: `IF NOT EXISTS` checks only the name, so re-running
  the CREATE statement while the INVALID index exists prints a NOTICE, does
  nothing and silently keeps the INVALID index.
- `IF NOT EXISTS` also does nothing when an index with that name exists with a
  different definition. That is why the exact-definition check in Verification
  is mandatory after every statement. (The precedent
  `scripts/ops/ingest-bookkeeping-cleanup/phase_a_index.sql` omits IF NOT
  EXISTS for this reason. Either style is fine as long as V1 runs.)
- (d) IO-pressure stop rule. Before and during each of the big three (tables 6,
  7 and 8), watch: `pg_stat_activity` rows with `wait_event_type = 'IO'` and
  replication lag (M6); the provider dashboard's IO utilisation; ingest p95
  claim latency, and the counts of 55P03 (lock timeout) and 57014 (statement
  timeout) errors, from the worker logs. Take the baseline before starting the
  build. If claim latency or the 55P03/57014 counts rise during a build, cancel
  the build (b), drop the INVALID index it leaves (c), and resume in a quieter
  window. The 21 GB build of table 8 can take tens of minutes, so keep watching
  for its whole duration.
- The DDL event trigger `hoopdb_sessions_function_boundary` fires only on the
  function, GRANT and REVOKE command tags, not on CREATE INDEX. The schema
  generation and migration ledger do not change.

## Order of operations

0. One production change at a time (pipeline lead). The claim timeout-relief trial that this step waited on ended and was rolled back at 2026-11-08 23:41Z (pipeline lead, 245334193840164864), so the only remaining gate is Khaliq's own word in the operator's session.
1. Measure: re-run M0-M6 and record counts and sizes only. Confirm that M0's
   uncovered set is exactly the eight tables above. Stop and re-plan if it is
   not.
2. Pick a low-traffic window. Confirm M3 shows no long transactions. Record
   the IO baseline for the stop rule (d).
3. Build one table at a time, in this order:
   1. `ingest_history_mask_pages` (empty, `2min`)
   2. `ingest_history_plans` (empty, `2min`)
   3. `ingest_seed_captures` (empty, `2min`)
   4. `ingest_publication_attempts` (empty, `2min`)
   5. `ingest_publication_generations` (empty, `2min`)
   6. `ingest_stage_intents` (356k rows, 0.9 GB, `15min`)
   7. `ingest_custody_records` (3.3M rows, 7 GB, `45min`)
   8. `ingest_lazy_native_slots` (18.6M rows, 24 GB total, 21 GB heap, `120min`)

   For each table:
   1. `SET statement_timeout = '<budget>'; SET lock_timeout = '5s';` (a), and
      record the budget used.
   2. For tables 6-8, re-check the IO baseline (d) first. Do not start in a
      busy period.
   3. Run that table's `CREATE INDEX CONCURRENTLY ...` statement. From the
      second session, watch progress (b) and, for tables 6-8, the stop rule (d).
      If the stop rule fires, cancel (b).
   4. Run V1 for that index and the table-scoped INVALID check from (c).
   5. If the build failed, timed out or was cancelled, or the index is INVALID
      or its definition differs, stop. Drop the INVALID index (c), find the
      cause (usually a long transaction, the lock timeout, the budget or IO
      pressure), and retry once only in a suitable window. A second failure
      stops the run for re-planning.
4. After all eight, run V1 (expect exactly 8 rows, all valid), then V2, then
   M2 again later to confirm `seq_scan` stops growing on these tables.

## Inverse block

Run these one at a time, with autocommit and as the same owner role. Each one
takes the same non-blocking SHARE UPDATE EXCLUSIVE lock. The order is the
reverse of the build order.

```sql
DROP INDEX CONCURRENTLY IF EXISTS sessions.ingest_lazy_native_slots_org_id_idx;
DROP INDEX CONCURRENTLY IF EXISTS sessions.ingest_custody_records_org_id_idx;
DROP INDEX CONCURRENTLY IF EXISTS sessions.ingest_stage_intents_org_id_idx;
DROP INDEX CONCURRENTLY IF EXISTS sessions.ingest_publication_generations_org_id_idx;
DROP INDEX CONCURRENTLY IF EXISTS sessions.ingest_publication_attempts_org_id_idx;
DROP INDEX CONCURRENTLY IF EXISTS sessions.ingest_seed_captures_org_id_idx;
DROP INDEX CONCURRENTLY IF EXISTS sessions.ingest_history_plans_org_id_idx;
DROP INDEX CONCURRENTLY IF EXISTS sessions.ingest_history_mask_pages_org_id_idx;
```

## Verification

V1, exact definition plus valid and ready. After the full run, expect exactly
8 rows, one per index above, all with `indisvalid` and `indisready` true:

```sql
SELECT i.indexrelid::regclass AS idx, i.indisvalid, i.indisready,
       pg_get_indexdef(i.indexrelid) AS def
FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid
WHERE c.relnamespace='sessions'::regnamespace AND c.relname LIKE '%\_org\_id\_idx'
ORDER BY 1;
```

Expected `def`: `CREATE INDEX <name> ON sessions.<table> USING btree (org_id)`
for tables 1, 3 and 8. For tables 2, 4, 5, 6 and 7 it is
`... USING btree (org_id, workspace_id, user_id)`. A row count other than 8
(after the full run), any other definition, or `indisvalid`/`indisready` false,
counts as a failure.

V2, the plan for the probe. Use a literal that cannot match a real org, so
nothing about real tenants is touched or printed:

```sql
EXPLAIN SELECT 1 FROM sessions.<table> WHERE org_id = 'packet-probe-nonexistent' LIMIT 1;
-- user-loop tables additionally:
EXPLAIN SELECT 1 FROM sessions.<table>
 WHERE org_id = 'packet-probe-nonexistent' AND workspace_id = 'packet-probe-nonexistent'
   AND user_id = 'packet-probe-nonexistent' LIMIT 1;
```

Expect `Index Only Scan` or `Index Scan` using `<table>_org_id_idx`. On a tiny
table the planner may still choose `Seq Scan`, which is correct at that size.
For the generic plan that the function's `EXECUTE ... USING $1` can reuse, run
`SET plan_cache_mode = force_generic_plan; PREPARE p(text) AS SELECT 1 FROM sessions.<table> WHERE org_id = $1 LIMIT 1; EXPLAIN EXECUTE p('packet-probe-nonexistent'); DEALLOCATE p;`.
(If `org_id` is not `text` on a table, use its declared type.)

## What this does NOT fix

- The losing concurrent claim still waits up to its `lock_timeout` on the admission row. The wait just gets shorter.
- The per-user fence row lock (`ingest_fence_owners`) still serialises retries for the same user.
- The SQL advisory gate in front of lazy bootstrap is still a separate follow-up.
- Async admission (taking bootstrap off the claim request path) is still a separate follow-up.

## Runtime safety: no live index-set comparison

Verdict, from source on this branch: no runtime path compares the live
PostgreSQL index set against an expected set. No component refuses work
because an index exists that no migration names. There are no class (c)
readers (compares at runtime and would trip). The only runtime index checks
require that one specific named index exists (expected is a subset of the
catalog), so an extra index cannot fail them.

| Path | What it reads and compares | When it runs | Verdict |
|---|---|---|---|
| Ingest Worker readiness and preflight | `schemaVerdict` (`src/shared/db.ts:192-237`) checks that ledger names are a subset of `sessions.__96bulls_migrations` (:206-210), reads the generation marker (MARKER_SQL :169), runs the role, token-verifier, ingest-seed and ingest-role verify SQL (:219-222), then `SESSIONS_FUNCTION_GATE_SQL` (:134-151, `pg_proc` only). `waitForSchema` :244. Services import only `expectedSchema()`/`generation()` (`src/shared/worker.ts:39`, `src/hoopdb/indexer.ts:19`, `src/session-pipeline/consumer-bootstrap.ts:23`, `consumer-service.ts:37`, `src/alleyoop/ingest.ts:46`). `generation()` (`src/hoopdb/migrate.ts:322-350`) hashes migration and verify file contents, not the catalog. | Every Worker start and claim | No index read. Safe. |
| Migrator guards | `migrationPreflight` (`src/hoopdb/migrate.ts:389-424`) calls the RDS-owner, runtime-role, function-boundary and CLI-history preflights, then `sequencePreflight` (:423). `sequencePreflight` (`src/hoopdb/migrate-guards.ts:990-1091`) never enumerates the catalog. It builds `seen` from the inventory of ledgered steps (:1046-1050). For each unledgered step, any inventoried object not in `seen` is probed by name with `exists()` (:928-984; an unqualified index is `pg_class` `relname=$1 AND relkind IN ('i','I')` at :945, any schema, fail closed on error) (:1051-1063). Every unledgered step needs a sentinel, and a sentinel that reports applied also counts (:1064-1083). Anything found raises `unledgered_schema` (:1084-1089). Direction: inventory(unledgered files) intersected with the catalog must be empty. `runtimeRoleViolations` `pg_class` reads (:528-752) are privilege checks on relkind r/p/v/m/f/S, which excludes indexes. | Only inside `runMigrations` (:488, under an advisory lock) and `inspectMigrations` (:430, read-only release planning), plus release and proof scripts. Not at Worker start, not on a schedule. | Class (b). Today every migration is ledgered, so nothing is probed. It trips only if a future unledgered migration literally names one of these indexes, or its sentinel reports applied. |
| Generation fences F1-F4, reader, G4 census, role-validation markers, bootstrap verify SQL | The F1-F4 labels do not appear in `src/session-ingest` or `src/hoopdb`. Privacy and claim fences are SQL RPCs (`admission-consumer.ts:114-116`, `app.ts:407,443`, `deliver.ts:356`, `babysitter-retention.ts:343-397`, `ledger-sql.ts:307`), and none of them reads `pg_index`. The census (`src/session-ingest/bookkeeping-cleanup.ts:23,29,62`) goes through `ingest_bookkeeping_cleanup_gate` (`20261105043880_ingest_bookkeeping_cleanup.sql:114-116`), which calls `ingest_bookkeeping_cleanup_index_ok` (:89-96): an exact `pg_get_indexdef`, valid and ready check for the single index `babysitter_evidence_index_claim_idx`. The privacy-authority apply (`20261105041000_ingest_privacy_authority.sql:218-222`) requires `ingest_frozen_source_records_user_session` to be valid and ready. The markers are `MARKER_SQL` plus the role-validation table. Every bootstrap verify SQL loop over `sessions` `pg_class` filters relkind r/p/v/m/f or S (privilege checks), for example `session_ingest_verify.sql:104-110`, `session_ingest_seeder_verify.sql:38-44`, `session_token_verifier_verify.sql:63-71`, `session_read_api_role_verify.sql:68-80`, `session_consumer_role_verify.sql:158-386`, `session_directory_search_worker_verify.sql:203-216` (`pg_shdepend` for the worker role; indexes have no ACL or owner entry of their own). | Runtime | Class (a) for the verify SQL. The two named-index RPC checks require a named index to exist, so extra indexes and other names cannot trip them. Safe. |
| Drift and schema-snapshot checks (CI, scripts, runbooks) | `scripts/pg-proof/role-proof.ts:355-395` hashes a catalog snapshot that includes indexes (:376-379), but only on a disposable PostgreSQL built from migrations (CI `.github/workflows/pg-proof.yml:93`). `src/hoopdb/__tests__/migration-profile.test.ts:1130-1176` checks that the fresh-DB catalog is a subset of the inventory. `src/hoopdb/search-index-contract.ts:727-783` (`bun run audit:search-indexes`, `search-index-audit.ts:24-58`) looks up the required `session_directory*` indexes by name. `src/migration/relayhistory-verify.ts:403-417` iterates source objects and requires the target to be a superset. Other script catalog reads are row-security, lock, ownership or privilege reads on named relations. | CI on fresh databases; operator CLIs on demand | None of them reports the new indexes on production. CI is blocking but never sees out-of-band indexes. The audit and verify CLIs are on-demand, exit non-zero only on missing or different required objects, and ignore extras. Caveat: if production were ever the source of `migration:verify` (for example a self-host move), a target without these indexes would report them missing. |

Every other `pg_index`/`pg_indexes`/`pg_class` reader under `src` is class
(a): `src/session-ingest/frontier-capture-source.ts:165-176` (relkind r/p/S),
`src/session-pipeline/consumer-service.ts:965-970` and
`src/search-features/source.ts:456` (row security of named relations),
`src/migration/relayhistory-copy.ts:85-153` (uses unique indexes and
time-leading btree indexes; the new ones are neither),
`src/migration/relayhistory-neon-fence.ts:47-85`,
`src/migration/neon-s3-erasure/authority.ts:146`, `src/proof/aws/probes.ts:71-84`
and `src/smoke/e2e.ts:143`. `src/hoopdb/migration-sentinels.ts` runs only
inside `sequencePreflight` (class (b)). Test-only readers run against
disposable databases.

## Drift guard analysis and the catch-up migration (not written here)

What these indexes do and do not trip today:

- The migrator's `sequencePreflight` (`src/hoopdb/migrate-guards.ts`, around
  :990-1080) checks only migrations that are not in the ledger yet. All
  current migrations are in the ledger, so the extra indexes trip nothing at
  runtime today.
- `src/migration/relayhistory-verify.ts` `schemaDifferences` requires the
  target to be a superset, so an extra index on the target passes.
- Role proof (`scripts/pg-proof/role-proof.ts`) and
  `packages/pipeline/test/relayhistory-schema-parity.test.ts` build fresh
  databases from migrations, so they never see out-of-band indexes. Fresh
  environments (self-host, CI, new stages) will therefore lack these indexes
  until a migration adds them. That is drift by construction.
- Schema-release inspection does not list indexes.

The trap for a later catch-up migration:

- `inventoryOf(sql)` in migrate-guards parses
  `CREATE [UNIQUE] INDEX [CONCURRENTLY] [IF NOT EXISTS] <name>`. For an
  unledgered step it fails `unledgered_schema` if any inventoried object
  already exists.
- Every step also needs a sentinel in `src/hoopdb/migration-sentinels.ts`
  (otherwise `sentinel_missing`). A sentinel that reports applied also fails
  `unledgered_schema`.
- So a naive migration such as
  `CREATE INDEX IF NOT EXISTS ingest_seed_captures_org_id_idx ON sessions.ingest_seed_captures (org_id);`
  would make the migrator refuse on production, where the index already
  exists, even though the SQL itself is idempotent.

What the catch-up should look like (design only, not written):

- File: `supabase/migrations/<future slot>_ingest_claim_bootstrap_org_indexes.sql`.
  The slot is assigned by the layered agent from the registry when the schema window reopens; it is not 049000. Do not add placeholders.
- Adopt or create, for each of the eight indexes in this packet (and only those):
  - if an index with that name exists and its `pg_get_indexdef` equals the
    expected definition and `indisvalid AND indisready` hold, adopt it
    (record it and do not rebuild);
  - if no index with that name exists, run
    `CREATE INDEX IF NOT EXISTS <name> ON sessions.<table> (...)`
    non-concurrently inside the migrator transaction (the migrator is
    transactional; this is fine on fresh databases, where the tables are small
    or empty);
  - if an index with the same name exists with a different definition, or it
    is INVALID or not ready, `RAISE` and refuse.
- Ordering, given how the guard actually behaves: `sequencePreflight` never
  lists the catalog, so the indexes are never "unledgered" by themselves. A
  later migration that does not name them is unaffected, whether or not the
  catch-up has landed. The only constraints are:
  - the catch-up itself, while unledgered on production, trips
    `unledgered_schema` if its text inventories these names or its sentinel
    reports applied;
  - any later migration that names these indexes must come after the catch-up
    in the sequence. Once the catch-up is ledgered, its inventory goes into
    `seen` (`migrate-guards.ts:1046-1050`) and later mentions are not probed.
- The guard and the test pull in opposite directions, so `format()` hiding is
  not enough:
  - `inventoryOf` keeps string literals and dollar-quoted bodies, so a literal
    name anywhere in the file, including inside `EXECUTE '...'`, is inventoried
    and probed. On production it exists, so the step is refused.
  - Building the name with `format('%I', ...)` skips the inventory (names
    containing `%` are dropped, `migrate-guards.ts:884-886`). But then
    `migration-profile.test.ts:1130-1176` (fresh-DB catalog must be a subset
    of the inventory) fails, because the indexes exist on a fresh database and
    no step inventories them.
  - So the catch-up needs a reviewed guard change. One option is an explicit
    adoptable-index allowance in `migrate-guards.ts`: an index named by an
    unledgered step is accepted only when its live `pg_get_indexdef` matches
    the step's definition exactly, and it is valid and ready. Anything else
    still fails closed.
- The two acquisition tables (`ingest_acquisition_operations`,
  `ingest_acquisition_revoked_grants`) are out of scope for the catch-up: if
  they are created later by 043850, their own migration carries their indexes.
- A sentinel keyed on something only the catch-up creates (not on the
  indexes), so that production reads "not applied" before the run.
- Tests: fresh DB (create path); adopt path where the index exists and matches;
  refusal where it is INVALID, not ready or a different definition; the
  inventory coverage test; and a guard test proving that a non-matching
  same-name index still raises `unledgered_schema`.

Approval: production index creation needs Khaliq's own word in the credentialed operator's session; this packet is informational.

Agent-Session: 313e9666-c9aa-4a01-bb32-613a8225481a
