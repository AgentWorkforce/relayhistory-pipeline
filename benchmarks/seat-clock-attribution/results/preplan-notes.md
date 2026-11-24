# Mode p (20261105062250): notes for review

Synthetic evidence only (this directory). Nothing here was applied to, or read from, a real database.

## What changes and what does not

- One slot, 20261105062250: it replaces `sessions.ingest_lazy_ack_batch(jsonb)` and `sessions.lazy_ack_t2_set_mode(text)`
  (062210's, same signatures and ACLs) and widens the mode CHECK of 062210's owner-only row `sessions.lazy_ack_t2_mode`
  to `IN('main','t2','p')`. No new function, table, grant or bootstrap edit.
- Installed state: gate `off`, mode `main`. Gate off runs 062210's main body statements plus its one singleton read.
- **Generation change.** The slot changes the ingest generation: base (48f61828)
  `778af6866b9b28b64ff5af46bc59b1d6c7de4b43e24302bf64321b222cdf23df`, branch head
  `880ac8b266f82f5ee8d9893eea9b6e24962915c8d2d338275f102be00f5c6263` (`bun src/hoopdb/migrate.ts --generation`). Shipping
  it costs one more full overlap release cycle. The measured cells are in `phase3.md`; whether that cost is worth them is
  not decided here.

## Apply and rollback conditions (proved on PostgreSQL 15/16/17, `lazy-ack-preplan.postgres.test.ts`)

- The 062250 file try-locks (961996,50000) EXCLUSIVE, then takes `sessions.lazy_ack_t2_mode` ACCESS EXCLUSIVE NOWAIT,
  then swaps the CHECK. While any ingest or erase transaction is open (they hold 50000 shared) or anything holds the gate
  table, it raises 55P03 at once; nothing of 062250 is applied, nothing is queued.
- **Through the real migrator** the order is different: the migrator first upserts the readiness marker to `pending`.
  That statement waits on the marker row that every live ACK holds FOR SHARE (`ingest_lazy_guard`), so the migrator waits
  for in-flight ACKs before it reaches 062250 (existing migrator behaviour). Once the marker is pending, new staged
  commits are refused (55000 `generation not ready`, a 503 at the Worker) until a run completes.
- **Apply condition: a direct reader of the gate table.** If a transaction holds the gate table's AccessShareLock without
  holding 50000 (a direct `SELECT` on the table, e.g. the flip helper's read below, or an operator query), the migrator
  passes its marker step, then 062250 refuses with 55P03. Result: nothing of 062250 applied, ledger without 062250, the
  marker left **pending**. In between, every staged commit fails 55000 (`generation not ready`; the Worker answers 503 and
  the client keeps the batch); claims fail the same way. A rerun needs only the reader gone: the next migrator run applies
  062250 and marks the generation pass (proved: "quiet rerun applies, marker pass"). A second refusal is safe in the same
  way: each refusal rolls back the 062250 transaction completely and only leaves the marker pending.
- The down (generated) refuses unless the row says gate `off` and mode `main`, try-locks 50000 exclusive, takes the
  marker row NOWAIT, the gate table ACCESS EXCLUSIVE NOWAIT and the row NOWAIT (55P03 when busy), restores 062210's two
  bodies byte for byte and the CHECK `IN('main','t2')`, removes its ledger row. Afterwards readiness is pending as for
  062210's own down; the three restoration steps are the same at the target tree's generation (runbook).

### Readers of the gate table outside the ACK body, on the base (48f61828)

- `supabase/migrations/20261105062210_lazy_ack_t2.sql:158` (`lazy_ack_t2_set_mode`) and `:181` (`lazy_ack_t2_set_gate`):
  owner-only setters; each reads the row FOR UPDATE NOWAIT only after its own TRY-lock of 50000 exclusive succeeded, so
  they never run beside an apply or down that holds 50000.
- `src/session-ingest/lazy-ack-t2-flip.ts:71` (`MODE_READBACK`): the operator flip helper's read-back after a lost reply
  (one autocommit statement, no barrier). On this branch the helper also runs `MODE_PRECHECK` before its first attempt.
  Nothing in the product calls the helper.
- `src/hoopdb/migration-sentinels.ts:545`: reads catalogs (`to_regclass`, `pg_attribute`, `pg_proc`), not the table.
- No other product code (src, stacks, scripts, bootstrap) reads the table. Tests do.
So outside operator actions, the only reader that can meet an apply is the flip helper's own read, which is an
operator-run tool; a refusal from it is the case described above.

## Mode changes

- `sessions.lazy_ack_t2_set_mode('p')`: owner-only, READ COMMITTED, try-lock 50000, row NOWAIT, needs the gate fenced;
  refuses a direct `t2 <-> p` switch (55000, through main only). Leaving p: the owner runs
  `SELECT sessions.lazy_ack_t2_set_mode('main');` then, to turn the gate off, `SELECT sessions.lazy_ack_t2_set_gate('off');`,
  each as one autocommit statement. No privilege is granted for it.
- `src/session-ingest/lazy-ack-t2-flip.ts` switches only main/t2: before any attempt it reads the row and stops with
  `LazyAckT2FlipUnknownMode` (code `lazy_ack_t2_flip_unknown_mode`) unless the row says main or t2.

## The pre-plan (S0)

- One statement, after the gate read and before the clock: it captures, in one snapshot, the outbox -> stage row, the
  attempt row of that claim and the clock identity, and builds T2's candidate list (A) from those captured values only.
  It takes no row lock and writes nothing. Its relations (outbox, stage intents, claim attempts, seat clock) are already
  locked by the same transaction before the ACK (the commit function reads and writes them), so it adds no relation lock.
- It holds the fence row and (961996,50000) shared while it runs (the ACK already holds both from `ingest_lock_fences`),
  and its time counts against the 5 s statement timeout: see `phase3.md` for its measured time per cell.
- Exception classes it may swallow (meaning "no pre-plan"): `data_exception` (class 22), `no_data_found`, `too_many_rows`,
  `cardinality_violation`. Every other class propagates, including lock_not_available, serialization_failure,
  deadlock_detected, query_canceled, resource and internal errors. It cannot hide a refusal main would raise at that point:
  main raises nothing between the gate read and the clock statement, and every check main makes runs unchanged after it.
- Its exception block assigns no transaction id (read-only; proved: the ACK holds exactly one transactionid lock while
  waiting for the clock).
- A first version took about 0.9 s per ACK: the planner estimated 225 rows for the captured row and multiplied it through
  each `CROSS JOIN cap`, the estimated cost crossed the JIT thresholds, and the statement was JIT-compiled on every call.
  `LIMIT 2` on the capture bounds the estimate.

## Dependency map (summary; the full map was reviewed by relay)

| captured value | used for | class | serialized through COMMIT by | on mismatch |
|---|---|---|---|---|
| outbox row presence, active stage token | picks the stage row | hint only | the locked stage-row query (FOR UPDATE OF the stage row) | recompute (A) |
| stage claim token, group ordinals | candidate source, ordinal set | decides a write | stage row FOR UPDATE to COMMIT | recompute (A) |
| attempt org, workspace, user, origin, records, epochs, reset epoch, processing ranks | key material, digests, epoch filter (an absent or duplicate session epoch drops the record and flips validity), rank order | decides a write | same-user same-origin claims: the wrapper's origin-claim key; retirement: fence row FOR UPDATE vs this ACK's FOR SHARE; user erase: 50000 exclusive vs shared | recompute (A) |
| clock control epoch, source identity | key material | decides a write | writers take the clock FOR UPDATE, excluded by this ACK's NO KEY UPDATE from the clock statement to COMMIT | recompute (A) |
| validity flag | whether (A) may be used | hint only | - | recompute (A), which raises the real function's first error |

Comparisons are `IS NOT DISTINCT FROM` (row presence and NULL included; jsonb arrays keep their order). The recompute is
T2's own (A) statement at T2's position; it takes no lock, so it cannot reorder locks. Every vacant-slot, seat and decision
lookup is in T2's (C1), under the clock.

## What stays unverified

- Real-PostgreSQL proofs ran on single-node containers on one shared host; no replica, connection proxy or production
  storage.
- The benchmark is PostgreSQL 16 only (two runs); PostgreSQL 15/17 ran the lifecycle and race suites, not the benchmark.
- No real race of a captured input between the pre-plan and the clock was produced: each writer is blocked by a lock the
  ACK holds (proved by lock probes), and the mismatch path is exercised by fixture injection (PGlite) instead.
- The 044030 apply-window admission path (off by default) was not exercised.
