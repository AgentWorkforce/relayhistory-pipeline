# History 620 operations: kill switch, rollback and reapply

Covers `supabase/migrations/20261105026000_canonical_history_lineage.sql` (620) and its generated
rollback `supabase/rollbacks/20261105026000_canonical_history_lineage.down.sql`. Every step here is an
owner (migration role) operation. None of it is run by the migration itself, and none of it is approval
to run anything against a real database: applying, rolling back or switching a production database
still needs Khaliq's explicit OK.

## What "managed" means

A session is managed by 620 only when it has an **installed** current authority
(`sessions.ingest_history_scope_current`) and the kill switch is live. That single gate
(`sessions.ingest_history_scope_managed` / `ingest_history_scope_required`) controls every effect 620
has on legacy state: invalidation, `ingest_history_required_scopes`, directory summary/provenance and
work-state clears, replay-epoch advances, `session_publication_is_managed`, `session_brief`,
`claim_session_large_read`, `session_directory_history_summary` and the replay snapshot's `history`
member. A build in flight, an abandoned or expired build, or a stray `required_scopes` row never gates
anything. A live ACK or control that touches a build in flight only fences it: the build row moves to
`abandoned`, its install refuses with `history: scope build lease lost`, and the builder starts again.

Lock order on every live path: user fences, then the receipt seat clock
(`sessions.ingest_receipt_seat_clock`), then the switch row, then build rows (sorted), then per-scope
advisory locks (sorted). The install only try-locks the scope, and begin and install take the switch row
before any build row.

Since 043500 the lazy batch claim reads the seat clock `FOR KEY SHARE` (claims no longer exclude each
other there); writers that must exclude claims (`set_live`, control changes, `lazy_clear`, seat
initialization) still take it `FOR UPDATE`. After the fence lock and the clock read, and
before any write, each claim takes exactly one transaction advisory lock keyed
`'96bulls:lazy-native-origin-v1'` per (org, workspace, origin).

Since 043800 the staged ACK (`ingest_lazy_ack_batch`) takes the clock `FOR NO KEY UPDATE`: it writes
`last_seat` only, so staged ACKs still serialize with each other and with every `FOR SHARE` /
`FOR UPDATE` holder, but a claim's `KEY SHARE` no longer queues behind an in-flight ACK. Right after
the clock, and before any stage-row or native-slot lock, the staged ACK takes the same
`'96bulls:lazy-native-origin-v1'` advisory lock as the claim for its one (org, workspace, origin).
The ACK lock order is therefore fence -> clock -> origin -> slot, the same as the claim's, so an
ACK and a same-origin claim of another user cannot deadlock on native slots. A same-origin claim
that runs while a staged ACK is in flight waits for that ACK's transaction on the origin lock.

## Kill switch (usable at any time, including after installs)

```sql
SET lock_timeout='5s';                           -- see the note below: the function's own 5s wins
SELECT sessions.ingest_history_set_live(false);  -- off
SELECT sessions.ingest_history_set_live(true);   -- on again
```

Both calls fail with 55P03 (lock_not_available) when contended; nothing has changed, so retry.
`set_live(true)` takes the seat clock first and then `session_index` SHARE with **NOWAIT**. Main
takes those two locks in both orders (the lazy batch claim: seat clock `FOR KEY SHARE`, which still conflicts
with `set_live`'s `FOR UPDATE`, then a `session_index` write;
a lazy control: `session_index`, then the seat clock), so resume never waits for `session_index`. It
fails at once with `history: session_index has an in-flight writer; nothing changed, retry
ingest_history_set_live(true)` while any indexer write is uncommitted. Retry in a loop with a short
pause until it succeeds. A call that asks for the state already in place returns `changed:false`
without taking any lock.

`lock_timeout` is a `SET` clause on `set_live` and `set_pointer_tracking`, so inside them it is always
5s. It overrides a shorter (or longer) timeout set by the caller. The caller's own setting applies only
to statements outside the function.

If `set_live(true)` keeps returning 55P03, something is holding a lock on `session_index` that
conflicts with SHARE. Look in `pg_stat_activity` and `pg_locks` for:
- autovacuum or `ANALYZE` on `sessions.session_index`, or a `CREATE INDEX CONCURRENTLY` on it (each
  holds SHARE UPDATE EXCLUSIVE);
- a long-running indexer transaction.

NOWAIT never cancels autovacuum: wait for it to finish, or pause the indexer briefly, then retry.

```sql
SELECT a.pid, a.state, a.backend_type, l.mode, l.granted, left(a.query, 120) AS query
  FROM pg_locks l JOIN pg_stat_activity a USING (pid)
 WHERE l.relation = 'sessions.session_index'::regclass;
```

There is also a known, pre-existing three-way 40P01 between `set_live(false)`, a running
`ingest_history_pointer_ref_backfill` page and a lazy selection/reset control. PostgreSQL's deadlock
detector resolves it by aborting one of them. It only involves operator calls, and nothing is left
half-done, so retry the aborted call (the switch or the backfill page).

- **Off:** every in-flight build is abandoned; every 620 trigger (ACK, control, directory,
  session_index) returns immediately; every read gate behaves as main; `ingest_history_scope_begin`
  and `ingest_history_scope_install` refuse with `history: 620 is switched off`. Pointer tracking stops
  too, so `refs_complete` is cleared and the inventory is no longer attested. All ledgers stay:
  scope versions, current rows, derived objects, the feature journal, outboxes and builds.
- **On:** takes the seat clock, then SHARE on `session_index` with NOWAIT. If a writer from the off
  period is still uncommitted, it refuses with 55P03 and you retry. It never restores
  `refs_complete`, so rerun the full pointer backfill before any inventory work. Every in-flight build is abandoned again (one that raced the switch saw untracked changes),
  and every installed scope is invalidated in the same transaction (ACKs and controls while off
  were not tracked), so its directory row is cleared and its next build re-derives it. This is five
  set-based statements over all installed scopes (`ingest_history_invalidate_installed`), not a
  per-scope loop; indexer writes wait for it behind the SHARE lock.

## Lock-wait alert (55P03 / 57014)

The ingest Worker writes one extra log line when a buffered lazy claim or a staged ACK fails with
SQLSTATE 55P03 (`lock_not_available`, e.g. the 2s `lock_timeout`) or 57014 (query cancelled;
usually the statement timeout, but not provably so): `route` = `ingest.lock_alert`, `stage` =
`lazy_claim` (`ingest_claim_buffered_batch`) or `staged_ack` (`ingest_commit_buffered_batch`),
and `sqlstate`. It carries no tenant data and no ids. The request's own 503 line is unchanged
(`route` `delivery.batches` / `turns.batches` / `delivery.recovery`, `outcome`
`delivery_unavailable`, `stage` `claim` or `buffer_receipt`, `error_code`, `db_error_code`).
Since 043800 it also fires with `stage` = `staged_intent` for the buffered stage-intent call
(`ingest_prepare_buffer_stage`, deliver stage `buffer_intent`), which takes no clock lock but waits
on the fence and batch rows a staged ACK holds. This line covers only those three calls. Control changes and `lazy_clear` run nested inside the
ordered selection/reset SQL (`ingest_apply_selection` / reset and the history-control triggers);
their 55P03/57014 currently reach the Worker's selection/reset routes as a generic unavailable
error without the SQLSTATE, so this alert line does NOT cover them. A follow-up PR will preserve
the closed SQLSTATE through that conversion and emit `ingest.lock_alert` with stage tags
`control_changed`, `lazy_clear`, `set_live`, plus an exact attempt/success counter at the commit
call site. Until then those paths are observable only in the database/operator log.

Alert when any of these is true:

- commit lock-alert request rate above 1% over 5 minutes (numerator: `ingest.lock_alert` lines
  with `stage=staged_ack`; denominator: buffered requests that reached `buffer_receipt`, i.e.
  successful batch/recovery lines plus 503 lines with `stage=buffer_receipt`). This is a request
  rate, not an exact RPC failure rate: the denominator includes replay/suppressed successes that
  never called `ingest_commit_buffered_batch`;
- any 55P03 on `set_live`, `control_changed` or `lazy_clear`, read from the database/operator log
  (not from the Worker line) until the follow-up lands;
- more than 1% of claim-path requests return 503 over 5 minutes (`outcome=delivery_unavailable`
  with `stage=claim`, divided by all batch, turns and recovery requests).

Per-stage latency (default off): with `INGEST_STAGE_LATENCY_LOG=on` the Worker writes one
`route` = `ingest.stage_latency` line per deliver stage with `stage`, `outcome` (`ok`, `refused`,
`unavailable`), `duration_ms` and, for a database failure, `sqlstate`; no tenant data, no ids.

Claim lock-wait busy (default off): with `INGEST_CLAIM_LOCK_BUSY=on` a claim (any acceptance
mode) whose statement loses its lock wait (55P03 or 40P01, `error_code` `ledger_lock_timeout`)
answers 429 `delivery_busy` with `Retry-After` 5-10 s instead of the 503. The lower bound is the
role `statement_timeout` (5 s), so the retry lands after the holding claim has committed or been
cancelled. The statement rolled back, so nothing was written; the retry is the same idempotent claim.
The flag changes only the HTTP answer: the async admission consumer still retries a claim lock wait
with the normal admission backoff (15, 30, 60 ... 300 s by attempt), with the same `error_code`
`ledger_lock_timeout`, `stage` `claim` and `db_*` fields on its `ingest.admission` retry line. The
route line keeps `stage` `claim`, `error_code` and `db_error_code`, with `outcome` `delivery_busy`.
The `ingest.lock_alert` `lazy_claim` line still fires for 55P03 (buffered mode) only: the lock
alert set is 55P03 and 57014, so a 40P01 (deadlock) turned into 429 emits no lock alert and shows
only as `outcome=delivery_busy` with `stage=claim` and `db_error_code` `40P01`. The
`ingest.stage_latency` line (if enabled) still records the converted claim as `outcome`
`unavailable`, because it is observed before the conversion. 57014 (`ledger_timeout`) and every
other cause stay 503. With the flag on, the 503 claim-path alert above no longer counts 55P03 or
40P01. Watch `outcome=delivery_busy` with `stage=claim` (all claim lock waits), or the `lazy_claim`
lock alert (55P03 only). Unset or `off`: unchanged. The Cloudflare stack renders the var from the
stack input `sessionIngest.claimLockBusy: "on"` (canonical or buffered acceptance; omit for `off`,
which renders no var). Operator note: if the deploy actor previously set `INGEST_CLAIM_LOCK_BUSY`
out of band as a Worker version secret of the same name, it must remove that secret before the
stack-rendered var applies, or the apply conflicts.

Claim statement-timeout busy (default off, independent of `INGEST_CLAIM_LOCK_BUSY`): with
`INGEST_CLAIM_TIMEOUT_BUSY=on` (parsed like `INGEST_CLAIM_LOCK_BUSY`: only `on`, any case, trimmed)
a claim whose statement hit the role `statement_timeout` or was cancelled (57014, `error_code`
`ledger_timeout`) answers the same 429 `delivery_busy` with `Retry-After` 5-10 s instead of the 503.
Why: the role `lock_timeout` (2 s) applies to each lock acquisition separately, so a claim queued
behind a chain of same-user claims on the per-user fence row waits several times (each under 2 s)
and reaches the 5 s `statement_timeout` before any single wait reaches `lock_timeout`; that is the
dominant claim-stage 503. A 57014 (timeout or `pg_cancel_backend`) rolls back the single autocommit
claim statement just like 55P03, so nothing was written and the retry is the same idempotent claim;
57P01/57P02 (terminate, shutdown) and 08xxx are other codes and stay 503. Only `stage=claim`
converts: 57014 at `buffer_intent`, `buffer_receipt`, `slot`, `commit` and every other stage stays
503. The route line keeps `error_code` `ledger_timeout` and `db_error_code` `57014` with `outcome`
`delivery_busy`, so it stays separable from the 55P03 lock wait (`ledger_lock_timeout`); the
`ingest.lock_alert` `lazy_claim` line still fires for 57014 (buffered mode); the admission consumer
is unchanged (normal admission backoff). Caveat: a claim that is slow without contention also
yields 57014 and would retry as 429 instead of 503; watch `outcome=delivery_busy` with
`error_code=ledger_timeout` per tenant. Unset or `off`: unchanged. Not yet rendered by the
Cloudflare stack (`stacks/cloudflare/session-ingest.ts` has no input for it) and not read by the
self-host server; both are follow-ups.

No Workers analytics or Logpush observer is named in these runbooks, so the alert is not wired
yet. It is to be wired by the release owner in the G3 window, using the fields `route`, `stage`,
`sqlstate`, `outcome` and `db_error_code`. A sustained `lazy_claim` 55P03 with a single origin
lock holder is expected same-origin serialization (see the lock order above): check the
uploader for overlapping same-origin uploads before treating it as an incident.

## Pointer-reference tracking

The `session_index` pointer-reference trigger is dormant until armed, so the legacy indexer pays
nothing before History needs the inventory.

1. `SET lock_timeout='5s'; SELECT sessions.ingest_history_set_pointer_tracking(true);` Arming takes a
   brief SHARE lock on `session_index`, which waits out in-flight indexer writes, so no write that began
   before the arm can commit an untracked row after the backfill has passed it.
2. Page the backfill until it returns NULL. Each page locks its `session_index` rows (`FOR UPDATE`),
   so keep pages small (for example 500) in production:
   `SELECT sessions.ingest_history_pointer_ref_backfill(<cursor or NULL>, 500);`
3. Each page also prunes references in its key range whose `session_index` row is gone or lost its
   pointer (an untracked delete). The final page sets `refs_complete`. `ingest_history_inventory_claim`
   refuses until then, and it also refuses a carrier that still has a dangling reference.

Normal indexer writes resume immediately after arming or resume, before any backfill: a tracked
insert, update or delete replaces whatever stale reference is left at its key from an untracked period
(no unique violation) and queues every carrier involved for a rescan. The backfill is still what sets
`refs_complete`.

Disarming (`set_pointer_tracking(false)`) clears `refs_complete`. Re-arming requires a fresh full
backfill pass, which rewrites any reference left stale while disarmed.

## Constraint validation left for an operator (022000)

`20261105022000_session_desktop_catalog.sql` adds its four CHECKs `NOT VALID`, so the migration holds
no validation scan under ACCESS EXCLUSIVE. Validate later; this takes SHARE UPDATE EXCLUSIVE, and
readers and writers keep running:

```sql
SET lock_timeout='5s';
ALTER TABLE sessions.session_directory VALIDATE CONSTRAINT session_directory_catalog_read_version_check;
ALTER TABLE sessions.session_directory_relationships VALIDATE CONSTRAINT session_directory_relationships_child_model_check;
ALTER TABLE sessions.session_directory_relationships VALIDATE CONSTRAINT session_directory_relationships_child_agent_name_check;
ALTER TABLE sessions.session_directory_relationships VALIDATE CONSTRAINT session_directory_relationships_child_agent_type_check;
```

The 022000 partial index is still built inside the migration transaction, after the ALTER, so it is
built under that ALTER's lock. Building it `CONCURRENTLY` needs a non-transactional step kind in
`src/hoopdb/migrate.ts`, which this change does not add. Every `2026110502xxxx` file starts with
`SET LOCAL lock_timeout='5s'`, so a blocked DDL lock fails the file (it rolls back whole) instead of
queueing live traffic behind it. A lock_timeout failure during apply rolls that file back and the
migrator stops: the files before it stay applied, and the role-validation marker stays `pending` (the
Worker gate stays closed) until a rerun of the migrator gets through and passes validation. Rerun it at
a quieter moment; nothing needs to be undone.

## Schema rollback (down) and reapply

Use the down only when the kill switch is not enough, that is, before any authority was installed.

Once any later upstream migration has landed—including 027000 project events,
028000 semantic state, 029000 P3b reads or 030000 consumer progress—the 026000
down refuses until every later file has been rolled back in reverse order and
its ledger row removed. The 030000 named down removes its own row after safely
dropping the restartable producer-progress state.

The down **refuses** (nothing is dropped) when:
- any plan, scope version, derived object or model-work function exists
  (`history: retain authority/object ledgers; use sessions.ingest_history_set_live(false)`); or
- any upstream migration that sorts after 026000 is in `sessions.__96bulls_migrations`
  (`history: a later migration is applied; ...`). Deleting 026000's ledger row would leave a gap, and
  `sequencePreflight` would refuse every later migrate with `ledger_gap`.

Procedure:

1. Stop the 620/630 workers (scope builders, inventory, reconcile and privacy consumers).
2. Run the down file in one transaction as the owner. It drops 620's triggers, functions and tables,
   restores the eight replaced functions from their pre-620 definitions, and deletes the row
   `upstream:20261105026000_canonical_history_lineage.sql` from `sessions.__96bulls_migrations`.
   Re-derivable queues (reconcile and privacy outboxes, required scopes, in-flight builds) go with
   the schema.
3. To reapply, run the normal migrator. Because the ledger row is gone, it applies 026000 again, writes
   the ledger row, and the post-validation function-ACL check matches the committed manifest
   (`function_acl_drift` does not fire). Then repeat the pointer-tracking steps above.

`src/session-ingest/__tests__/history-ack-safety.pglite.test.ts` covers: down, then reapply, then a
live ACK, both dormant and after queued in-flight work; the later-migration refusal; the
installed-authority refusal; and the kill switch after install.
