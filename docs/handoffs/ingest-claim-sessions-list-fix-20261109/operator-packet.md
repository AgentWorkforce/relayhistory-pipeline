# List fix (044080 + 044090): production operator packet

Scope: the claim-sessions side table (slot 044080) and its reader (slot 044090), taken from
code-only to production. The SQL for each flag flip, the backfill and the verification is in
`docs/handoffs/ingest-claim-sessions-044080/operator-packet.md` (the "044080 packet"). This
packet adds the apply-path decision, the order, the bounds, the stop rules, a canary and a
rollback for each step, and what to report. Citations are `path:line` against main 368b32c0.

## 0. Status

* Code only. Nothing in this packet has been applied to any database. 044080 and 044090 are on
  main and are not applied anywhere in production.
* Every apply, flag flip (`write_enabled`, `read_enabled`) and backfill run needs Khaliq's own
  explicit OK, in his own words, given in Brian's session. A relayed or paraphrased OK does not
  count, and neither does an OK given for an earlier step.
* Brian is the only DB actor. No other agent or person connects to the production database for
  any step here.
* **Schema-window freeze (standing).** No production apply of 043850 or later until the 044030
  overlap is applied. 044080 and 044090 are both later than 043850, so they wait behind
  window 1 (section 1).
* Report counts, durations and pass/fail only (section 6). Never select, log or paste claim
  tokens, tenant ids, session ids, record contents, cursors, generation hashes, hosts or
  project refs.

## 1. Apply path: out of band or ledgered?

**Answer: ledgered only.** 044080 and 044090 cannot go out of band like 044040's Packet A.
They need the ledgered apply under the 044030 overlap, then the rebind, then the G2 parity
re-proof at the new PASS generation.

Why the no-generation-move path is closed:

* An out-of-band create would leave the generation unchanged. `generation()` hashes migration
  files, not catalog state (`src/hoopdb/migrate.ts:353-381`). But that does not help, because
  the next migrator run refuses.
* Before any write, `sequencePreflight` collects the objects of every unledgered step
  (`inventoryOf`, `src/hoopdb/migrate-guards.ts:1049`; loop `:1396-1420`) and runs each
  unledgered step's catalog sentinel (`:1421-1454`). Any hit refuses the whole run with
  `unledgered_schema` (`:1456-1461`).
  * 044080's sentinel is the relation `sessions.ingest_claim_sessions`
    (`src/hoopdb/migration-sentinels.ts:357-360`).
  * 044090's sentinel is the column `ingest_claim_sessions_policy.read_enabled` (`:353-356`).
  * Their tables, triggers and functions are also in the inventory.
* The only exemption is an adoptable index: `ADOPTABLE_INDEXES` / `ADOPTABLE_INDEX_STEPS`
  (`migration-sentinels.ts:68`, `:122`), with the sentinel override at
  `migrate-guards.ts:1442-1453`. It covers only 044020 and 044040
  (`migration-sentinels.ts:43`, `:45`). 044080 and 044090 are not listed, and they create
  tables, triggers and a column, not just indexes.
* Nothing can be marked in the ledger by hand: "Nothing is ever imported into, or marked in,
  the ledger" (`migrate-guards.ts:1338`).
* Both migrations also refuse to run on top of an out-of-band copy. Their creator blocks raise
  55000 if any of their objects already exist (`supabase/migrations/20261105044080_ingest_claim_sessions.sql:42-54`,
  `supabase/migrations/20261105044090_claim_sessions_reader.sql:51-63`). An out-of-band
  copy would have to be dropped again before any ledgered apply could proceed.
* Both files are already on main, so any image built from current main contains them in
  `generation()`, whether or not they are applied.

What the ledgered path needs, in order:

1. **Window 1 (the one accepted outage, `docs/design/zero-503-schema-windows.md:213-234`).**
   Owner apply pinned to a source commit that contains 044030 and not 044040. The pending
   union per the G4 ledger is 043850 to 044030, as listed in
   `docs/handoffs/buffered-stages-session-first-044040/operator-packet.md:166-178`.
   `migrate.ts` has no target-slot option (`zero-503-schema-windows.md:219-221`), so the pin
   is the only way to split windows. Then come the rebind and the G2 re-proof (step 4 below).
   Report the refusal interval, from the pending marker write to the rebind commit.
2. **Prerequisite for window 2: 044040's GIN, prebuilt CONCURRENTLY out of band.**
   This uses 044040 packet A2 (`.../buffered-stages-session-first-044040/operator-packet.md:72`).
   Without it the migrator's read-only preflight refuses the whole run with
   `index_prebuild_required` (`src/hoopdb/migrate-guards.ts:1233-1290`, called at
   `src/hoopdb/migrate.ts:479`).
3. **Window 2 (overlap-protected, `zero-503-schema-windows.md:236-256`).** Source = a main
   commit containing 044090. The lane must stay a contiguous prefix
   (`migrate-guards.ts:1373-1383`, `ledger_gap`), so the union is 044040, 044050, 044070,
   044080 and 044090 (there is no 044060 file). The steps:
   1. Deploy the Worker as {G, H}.
   2. `bun scripts/ingest-generation-overlap.ts open --from G --to H --ttl-seconds <apply+runtime+margin> --approval-sha <sha256>`.
      Use the default `admitDuringApply` off. The TTL can be at most 4 h.
   3. Run the normal owner apply. The marker goes `(H, pending)` → `(H, pass)`.
   4. `ingest-generation-rebind.ts --from G --to H --execute`.
   5. Clean up the Worker.

   With `admitDuringApply` off, uploads are refused only from the pending write to PASS
   (`zero-503-schema-windows.md:273-283`).

   *Alternative:* everything rides one §3.1 window (the source contains 044090 while 044030
   is pending). That is one longer outage. Report it the same way as window 1.
4. **G2 parity refresh, after each window's PASS and rebind and before any canary.**
   * Owner-buffered reads are closed from the pending marker write (PASS generation NULL)
     (`supabase/migrations/20261105043700_owner_buffered_read.sql:256-260`). They stay closed
     until each owner's `sessions.owner_buffered_parity` row names the new PASS generation
     (`:375`, `:383`).
   * The refresh needs these things, and nothing else:
     1. `bun run scripts/owner-buffered-parity-operator.ts bulk --expect-source-sha <merged-main-sha> --expect-target-generation <new PASS generation, hex64> --batch-size 25 --confirm-write owner-buffered-parity-bulk/v1`
        (`scripts/owner-buffered-parity-operator.ts:20-23`). It runs a fresh G1 census, then
        a G2 write per owner with a live buffered outbox row. Run it as Root, with credentials
        only on an approved RAM-only pipe (`:3-9`). The output is counts only (`:35-37`).
     2. The source SHA is the exact merged main commit that was applied, and the target
        generation is the marker's new PASS value. Read both in Brian's session. Do not paste
        either into the report.
     3. A stopped bulk run resumes by rerunning the same pinned command (`:25-29`).
     4. No relabelling, ever. Never UPDATE `target_generation` (or any other column) of a
        stored proof (044040 packet `:182-188`).
     5. Until an owner is re-proven, that owner's list and stages reads return empty, not
        stale data.
   * The write still needs Khaliq's direct OK; `--confirm-write` is only an accidental-execution
     guard (`scripts/owner-buffered-parity-operator.ts:37-39`).

After window 2, both migrations are **inert**. `write_enabled` and `read_enabled` start
false (`20261105044080_ingest_claim_sessions.sql:75-83`, `20261105044090_claim_sessions_reader.sql:66-69`).
While the gate is off, both readers run 044040's query (`20261105044090_claim_sessions_reader.sql:207-215`;
tested at `src/session-ingest/__tests__/claim-sessions-reader.pglite.test.ts:150`). The one
runtime change is a single policy-row read per claim insert (`20261105044080_ingest_claim_sessions.sql:138-144`)
and per batch commit (`20261105044090_claim_sessions_reader.sql:112-114`).

Canary for window 2 (counts only, read-only):

```sql
SELECT write_enabled, read_enabled, backfill_cursor IS NULL AS cursor_clear,
       backfill_complete_at IS NULL AS not_complete
  FROM sessions.ingest_claim_sessions_policy;           -- expect: false, false, true, true
SELECT count(*) FROM sessions.ingest_claim_sessions;    -- expect: 0
SELECT count(*) FROM sessions.__96bulls_migrations
 WHERE name IN ('upstream:20261105044080_ingest_claim_sessions.sql',
                'upstream:20261105044090_claim_sessions_reader.sql');   -- expect: 2
```

Also check that the ingest p95 claim latency and the 55P03/57014 counts are back at the
pre-window baseline (section 3, stop rules).

Rollback for window 2: **do not use the down files on a running image.**

* `supabase/rollbacks/20261105044090_claim_sessions_reader.down.sql` and
  `supabase/rollbacks/20261105044080_ingest_claim_sessions.down.sql` delete their ledger
  rows. Readiness then reports `schema_pending` for any image whose files include the step
  (`src/shared/db.ts:226-227`), which closes the gate.
* So a down file is valid only together with deploying an image built without those files.
  That is a new window, and it needs its own OK.
* The guards: each down refuses (55000) unless its own slot is applied with the exact
  checksum and no later upstream slot is applied. 044080's down refuses while 044090 is
  applied (`src/hoopdb/__tests__/migration-profile.test.ts:2374-2466`).
* Because the slots are inert, the normal rollback is to leave them applied with both flags
  false.

## 2. Writer enable (`write_enabled = true`)

Preconditions:

* Window 2 has passed its canary.
* The G2 re-proof has finished.
* A baseline of the section 3 stop-rule metrics has been taken.
* No claim retirement (`ingest_claim_attempt_records_retire`,
  `supabase/migrations/20261105038000_claim_attempt_records_retirement.sql:176`) is running.

The bounded transaction (044080 packet `:51-63`), with a shorter lock wait:

```sql
-- pre-check: transactions holding ROW EXCLUSIVE on the claims table, and the oldest one's age
SELECT count(*) AS holders, max(now() - a.xact_start) AS oldest
  FROM pg_locks l JOIN pg_stat_activity a USING (pid)
 WHERE l.relation = 'sessions.ingest_claim_attempts'::regclass
   AND l.mode = 'RowExclusiveLock' AND a.pid <> pg_backend_pid();
-- proceed only if oldest is under 1s

BEGIN;
SET LOCAL lock_timeout = '2s';
UPDATE sessions.ingest_claim_sessions_policy SET write_enabled = true;
COMMIT;
```

What the trigger does:

* **Enabling** takes `SHARE` on `sessions.ingest_claim_attempts`. The statement-level guard
  takes it before the policy row is locked (`20261105044080_ingest_claim_sessions.sql:115-119`),
  and the row-level guard takes it again as a fallback (`:122-125`).
  * `SHARE` conflicts with the claim insert's `ROW EXCLUSIVE`. The UPDATE first waits for
    every in-flight claim transaction. While it waits and until COMMIT, every new claim
    insert queues behind it (`:104-108`).
  * The stall is therefore bounded by `lock_timeout` (2 s) plus the commit. On 55P03 nothing
    changes; retry at most twice, at least 60 s apart, then stop and report.
  * The change also clears the cursor and the completion stamp (`:122-123`). 044090 then
    clears `read_enabled` as well (`20261105044090_claim_sessions_reader.sql:84`).
* **Once on:** every claim insert, and every `records` rewrite, runs the sync trigger
  (`20261105044080_ingest_claim_sessions.sql:133-164`). The trigger does the following:
  * It reads the policy row: plain under read committed, `FOR SHARE` otherwise (`:138-142`).
    The claim RPC runs at the default read committed (no isolation is set on the ingest
    path), so it takes no policy row lock.
  * On UPDATE, it deletes the side rows whose pair is gone (`:145-150`).
  * It inserts the claim's distinct valid `(source, session_id)` pairs with
    `ON CONFLICT DO NOTHING` (`:151-160`).
  * It never raises on record content (`:15-16`). Tested at
    `src/session-ingest/__tests__/ingest-claim-sessions.pglite.test.ts:91`, `:233`.
* **The batch-commit refresh (044090).** When a batch commits, it sets `accepted_at`
  (`20261105044090_claim_sessions_reader.sql:106-136`).
  * It works in a subtransaction with a transaction-local `lock_timeout` of 100 ms and
    `FOR UPDATE ... SKIP LOCKED`, and every error is swallowed (`:117-131`).
  * It cannot fail a commit except on 57014 (`:104-105`).

Latency impact on the claim insert path. The claim does one `INSERT` per claim
(`supabase/migrations/20261105043850_ingest_acquisition_owner.sql:1122-1123`). The added
cost is one policy PK read, plus, per distinct pair, one heap tuple, three btree entries
(PK, `session_idx`, `accepted_idx`; `20261105044080_ingest_claim_sessions.sql:68-73`), one
FK check and one `ingest_batches` lookup. The cost scales with the number of distinct
sessions per claim, not with records.

Measured with the full schema on a local PGlite (single-threaded WASM, no production data;
these are direction and scale only, not production numbers):

| claim shape | N | writer off p50 / mean | writer on p50 / mean | added |
| --- | --- | --- | --- | --- |
| 50 records, 3 sessions | 2,000 | 0.85-0.91 / 0.95-1.02 ms | 1.15 / 1.24-1.26 ms | about +0.25-0.3 ms |
| 200 records, 20 sessions | 1,000 | 2.02-2.10 / 2.39-2.41 ms | 2.93-2.96 / 3.29-3.36 ms | about +0.9 ms |

"Writer off" here already includes the inert trigger's policy read. The pre-044080 baseline
was not measured separately.

Canary:

* For 15 minutes after the commit, ingest p95 claim latency stays within 20% of baseline,
  and the 55P03/57014 counts do not rise.
* Side rows grow:
  `SELECT count(*) FROM sessions.ingest_claim_sessions;` increases with traffic.
* In production, measure the claim RPC's `pg_stat_statements` `mean_exec_time` before and
  after, and report the numbers only.

Rollback (instant, takes no table lock; the guard locks only when enabling, `:115-124`):

```sql
BEGIN; SET LOCAL lock_timeout = '2s';
UPDATE sessions.ingest_claim_sessions_policy SET write_enabled = false; COMMIT;
```

This keeps the rows and clears the cursor, the completion stamp and `read_enabled`.
Re-enabling restarts at section 2.

## 3. Resumable backfill

Command (044080 packet `:65-79`). The owner URL is piped from 1Password and never written
to disk (`scripts/backfill-ingest-claim-sessions.ts:3`, `:44-49`):

```sh
op read "op://<vault>/<schema owner>/url" | jq -Rc '{ownerUrl: .}' \
  | bun scripts/backfill-ingest-claim-sessions.ts --batch-size 500 --statement-timeout-ms 30000 --max-batches 60
```

**Batch size: 500.** This is the default (`scripts/backfill-ingest-claim-sessions.ts:40`); the
allowed range is 1-10,000 (`src/session-ingest/claim-sessions-backfill.ts:85`).

* Each batch is one transaction (`claim-sessions-backfill.ts:98-130`). In order, it:
  1. Sets a transaction-local `statement_timeout` (`:100`).
  2. Locks the policy row `FOR UPDATE` (`:101`).
  3. Takes the next 500 claims in `claim_token` order after the cursor, `FOR SHARE`
     (`:60-63`).
  4. Inserts the missing pairs and repairs `accepted_at` (`:65-71`).
  5. Deletes stale pairs (`:73-77`).
  6. Advances the cursor (`:126-128`).
  7. Commits.
* Halve the batch size to 250 if a batch takes over 5 s (see the stop rules).

**lock_timeout.** The backfill sets none, and there is no flag for one.

* It inherits the owner role's default (`claim-sessions-backfill.ts:100` sets only
  `statement_timeout`). It cannot be passed through the URL or PG* env either: the client
  takes discrete fields and sets its own `options` (`src/shared/owner-pg-config.ts:11`,
  `:38`).
* The effective bound on any lock wait is therefore the per-batch `statement_timeout`
  (30 s), plus the client backstop of +5 s (`scripts/backfill-ingest-claim-sessions.ts:48`).
* What it can wait on, and what can wait on it:
  * **The policy row (`FOR UPDATE`).** It blocks a flag flip for up to one batch. Do not flip
    flags while it runs. Read-committed claims read the policy without a lock and are not
    blocked.
  * **The 500 claim rows (`FOR SHARE`).** These block only a `FOR NO KEY UPDATE`, `UPDATE`
    or `DELETE` of those rows. The live claim path updates only the claim it just inserted
    (`20261105043850_ingest_acquisition_owner.sql:1155-1161`). Child-row FK checks take
    `FOR KEY SHARE`, which is compatible. That leaves the owner-only retirement: do not run
    it during the backfill.
  * **Side rows.** The 044090 refresh skips rows the backfill holds (`SKIP LOCKED`); the
    backfill writes `accepted_at` itself.
* If a role-level `lock_timeout` is wanted, it is a role setting change and needs its own OK.
  It is not part of this packet.

Cursor and resume semantics:

* The cursor is the last `claim_token` of the last committed batch, stored in the policy row
  (`claim-sessions-backfill.ts:126-128`).
* An interrupted run (Ctrl-C, `pg_cancel_backend`, a timeout or a `--max-batches` stop)
  loses at most the batch in flight, which rolls back. Rerunning the same command resumes
  (`resumed:true`, `:105`).
* An orphaned transaction from a dead client is ended by the server after 60 s
  (`idle_in_transaction_session_timeout`, `owner-pg-config.ts:38`).
* Completion is stamped only when a batch returns fewer than the batch size **and** the
  writer is on (`claim-sessions-backfill.ts:123`, `:126-128`, `:137-141`). Any
  `write_enabled` change clears the cursor and the completion stamp, so the walk restarts
  from the beginning (`20261105044080_ingest_claim_sessions.sql:122-123`).
* Tested: dry run, resume and completion at
  `src/session-ingest/__tests__/ingest-claim-sessions.pglite.test.ts:148`; reconciliation at
  `:197`; `accepted_at` repair at `:215`.

**The walk is global, not per tenant.** `claim_token` is a random uuid, so the heavy user's
claims are interleaved with everyone's. There is no per-tenant backfill and no per-tenant
completion. The reader gate needs the global completion stamp
(`20261105044090_claim_sessions_reader.sql:212-215`).

Duration: `ceil(total claims / 500) × per-batch time`. There is no sleep between batches.

* PGlite per-batch time (500 claims): 228 ms at 3 sessions per claim; 1.9 s at 20 sessions
  per claim.
* On real PostgreSQL the per-batch time depends on the records size and on cache state. Take
  it from the first chunk of 60 batches (see the dry run).

| claims | batches | at 0.25 s/batch | at 2 s/batch | at 5 s/batch |
| --- | --- | --- | --- | --- |
| about 167,000 (heavy user alone) | 334 | about 1.4 min | about 11 min | about 28 min |
| global total (sizing, below) | ceil(N/500) | N/500 × 0.25 s | N/500 × 2 s | N/500 × 5 s |

Because the walk is global, the heavy user's 167k claims are done only when the global walk
is. The 167k row is the floor of the global walk, not a separate run. The 167k figure is the
heavy user's backlog per the brief; it is the same scale as the 167,000-batch single-user
test in the 044040 packet (`:201`).

Sizing (read-only, counts only, before the dry run):

```sql
BEGIN READ ONLY; SET LOCAL statement_timeout = '60s';
SELECT reltuples::bigint AS est_claims FROM pg_class
 WHERE oid = 'sessions.ingest_claim_attempts'::regclass;
SELECT count(*) AS claims, count(*) FILTER (WHERE records IS NOT NULL) AS live_claims
  FROM sessions.ingest_claim_attempts;
SELECT max(n) AS heaviest_user_claims FROM (
  SELECT count(*) AS n FROM sessions.ingest_claim_attempts
   GROUP BY org_id, workspace_id, user_id) x;
COMMIT;
```

Dry run (read-only transactions, no locks, writes nothing; `claim-sessions-backfill.ts:98`,
`:113-115`):

* Run it with the writer off, after window 2, using the same command plus `--dry-run`.
* It reports `claimsScanned` and `rowsMissing`. With the writer off and an empty side table,
  `rowsMissing` is the total number of pairs the real run will insert.
* Time it. Dry-run time per batch is a lower bound for the real run (the real run also
  writes, `FOR SHARE`-locks and logs WAL).
* For a first real chunk, use `--max-batches 60` (30,000 claims), then rerun.

IO stop rules. These are Brian's harm-based rules from
`docs/packets/claim-bootstrap-org-index-packet-20261108.md:317-325`, applied here.

* **Baseline**, before the first batch (`:220-230`):

  ```sql
  SELECT count(*) AS io_waiters FROM pg_stat_activity WHERE wait_event_type = 'IO';
  SELECT max(replay_lag) AS max_replay_lag FROM pg_stat_replication;
  ```

  `max_replay_lag` is NULL with no replica or without `pg_monitor`. Also record:
  * provider IO utilisation;
  * ingest p95 claim latency;
  * the 55P03 and 57014 counts in the worker logs.
* **Watch** the same during every chunk.
* **Stop** as soon as any of these happens:
  * ingest p95 claim latency rises above baseline (Brian's rule: any sustained rise, no tolerance band);
  * the 55P03 or 57014 count rises above baseline;
  * `io_waiters` or replication lag stays above baseline for longer than one batch;
  * provider IO utilisation stays near saturation;
  * a single batch exceeds 10 s, or any batch hits the 30 s `statement_timeout`.

  How to stop:
  1. Ctrl-C, or cancel the backend. Find the pid without the query text:
     `SELECT pid, now()-xact_start AS running_for, wait_event_type, wait_event FROM pg_stat_activity WHERE application_name = '96bulls-claim-sessions-backfill';`
     (`scripts/backfill-ingest-claim-sessions.ts:48`).
  2. Then `SELECT pg_cancel_backend(<pid>);` (claim-bootstrap packet `:300`).
  3. The batch rolls back, and nothing is left to clean up.
  4. Resume in a quieter window with the same command. Use a smaller `--batch-size` if
     batch time was the trigger.
* Run between chunks of `--max-batches 60` and re-check the metrics at each boundary.
  This is the only throttle the CLI offers.

Canary (after `completionMarked:true`): run the 044080 packet verification SQL (`:81-104`).

* `expected_pairs = side_rows`
* `stale_rows = 0`
* `write_enabled = true` and `complete = true`

Rollback: there is nothing to roll back. The side table is derived data, and nothing reads it
until section 4. To stop, cancel as above. To discard the work, turn the writer off
(section 2 rollback). The rows stay, and a later re-enable plus backfill reconciles them
(`ingest-claim-sessions.pglite.test.ts:197`).

## 4. Completion sentinel and reader enable

* **The completion sentinel** is `ingest_claim_sessions_policy.backfill_complete_at IS NOT NULL`.
  * The backfill sets it only at the end of a walk with the writer on
    (`claim-sessions-backfill.ts:126-128`).
  * The CHECK allows it only while `write_enabled` is true
    (`20261105044080_ingest_claim_sessions.sql:81`).
  * Any `write_enabled` change clears it (`:122-123`).
  * A run after completion exits at once with `alreadyComplete:true`
    (`claim-sessions-backfill.ts:104`).
* **The reader CHECK** `ingest_claim_sessions_policy_read_check` refuses
  `read_enabled` (23514) unless `write_enabled` is true and the completion stamp is set
  (`20261105044090_claim_sessions_reader.sql:66-69`; tested at
  `claim-sessions-reader.pglite.test.ts:316`).
* **The gate** is read on every call by both readers: `read_enabled AND write_enabled AND
  backfill_complete_at IS NOT NULL` (`20261105044090_claim_sessions_reader.sql:212-215`,
  `:741`). It is checked after the G2 scope check (`:207-210`).
  * It is global. There is no per-tenant reader canary in code.
  * With the gate on, both readers find sessions through the side table. They return the same
    rows, order and cursor as 044040 while the table is complete
    (`claim-sessions-reader.pglite.test.ts:166`).
  * A missing side row drops that session from the list, silently and without an error
    (`:348`). Completeness is therefore the precondition.

Preconditions:

* Section 3 canary passed.
* A fresh pair check run immediately before the flip passes: the 044080 packet `:81-104`
  shows `expected_pairs = side_rows` and `stale_rows = 0`.
* No backfill is running.

Enable (a single-row update. With the writer already on, the guard takes no table lock;
`20261105044080_ingest_claim_sessions.sql:115-119`):

```sql
BEGIN; SET LOCAL lock_timeout = '2s';
UPDATE sessions.ingest_claim_sessions_policy SET read_enabled = true; COMMIT;
```

Canary:

* Run the 044080 packet `:191-214` checks:
  * `write_enabled`, `read_enabled` and `complete` are all true;
  * `null_accepted` is not growing;
  * `drifted = 0`;
  * the pair counts are still equal.
* The heavy user's list and stages calls succeed, and their p95 is reported. The
  owner-buffered read API p95 and error counts are compared with the pre-flip baseline.
* The session count shown for the heavy user equals the pre-flip count.
* For Dev, the desktop app's list loads end to end.

Rollback (instant; both readers go back to 044040's query on their next call;
`claim-sessions-reader.pglite.test.ts:150`):

```sql
UPDATE sessions.ingest_claim_sessions_policy SET read_enabled = false;
```

* Turning the writer off also turns the reader off (`20261105044090_claim_sessions_reader.sql:84`).
* Do the reader rollback first if `expected_pairs` and `side_rows` ever diverge.

## 5. Canary and rollback per step (summary)

| step | canary (pass = all true) | rollback | rollback cost |
| --- | --- | --- | --- |
| Window 1 (§3.1) | PASS; rebind committed; refusal interval reported; G2 bulk counts with zero failures | none in place (a forward fix in a new window) | outage already taken |
| GIN prebuild (044040 A2) | index valid, exact definition (044040 packet) | `DROP INDEX CONCURRENTLY` of an INVALID build only | none |
| Window 2 (§3.2) | section 1 canary SQL; claim p95 and 55P03/57014 back at baseline; G2 bulk re-proof done | leave the slots applied and inert; down files only with an image without the files (section 1) | none if left inert |
| Writer on | claim p95 within 20%; no 55P03/57014 rise; side rows growing | `write_enabled = false` | cursor, completion and reader cleared |
| Backfill | `completionMarked:true`; `expected_pairs = side_rows`; `stale_rows = 0` | cancel; resume later | at most one batch rolled back |
| Reader on | section 4 canary | `read_enabled = false` | none; the 044040 path returns |

## 6. Reporting back

Report one line per step. Use counts, durations and pass/fail only. Never report identifiers,
hashes, hosts, cursors or tenant data.

* Windows 1 and 2:
  * apply duration;
  * pending-to-PASS duration and pending-to-rebind duration;
  * the G2 bulk counts (owners proven, unchanged, failed);
  * pass/fail.
* Writer on:
  * lock wait duration and attempts;
  * claim p95 and the 55P03/57014 counts before and after;
  * the claim RPC `mean_exec_time` before and after;
  * pass/fail.
* Sizing: `est_claims`, `claims`, `live_claims`, `heaviest_user_claims`.
* Dry run: `claimsScanned`, `rowsMissing`, `batches`, duration.
* Backfill, per chunk:
  * `batches`, `claimsScanned`, `rowsInserted`, `rowsRefreshed`, `rowsDeleted`;
  * duration and the slowest batch;
  * any stop-rule trigger and the metric that fired;
  * the final `completionMarked`.
* Verification: `expected_pairs`, `side_rows`, `stale_rows`, `null_accepted`, `drifted`,
  pass/fail.
* Reader on:
  * list and stages p95 before and after;
  * the heavy user's session count before and after;
  * pass/fail.

Not answered by the code (measure, do not assume):

* The real PostgreSQL per-batch backfill time and the global claim count. The durations above
  are a formula until the sizing and the dry run are done.
* The claim-path latency added on real PostgreSQL. Only PGlite was measured.
* The batch-commit cost of the 044090 refresh once the writer is on. It was not measured.
* The real PostgreSQL latency of the gate-on list for the heavy user. The 044040 packet's
  real-PG numbers are also still unmeasured (`:203-206`).
