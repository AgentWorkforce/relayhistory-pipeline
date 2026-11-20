# Staged-ACK seat-clock contention bench (evidence, not for landing)

Measurements behind the receipt seat-clock investigation: `sessions.ingest_lazy_ack_batch` holds the singleton
`sessions.ingest_receipt_seat_clock` row `FOR NO KEY UPDATE` until the transaction ends, so every staged ACK serializes.
Local synthetic data only, on a throwaway loopback PostgreSQL 16.15 running on one laptop, with the client on the same
host. Nothing here describes production.

**Safety:** every script refuses a non-loopback URL or a URL with a password. The scripts create and drop their own
database. Each script uses the owner connection to change the function body, gate, triggers or indexes, but only in
that scratch database.

**Variants:**
- `loop` is the 043800 body.
- `set` is the 062155 set body with the gate on.
- `H` is the 062155 set body patched in the scratch database to always use its round path (one slot `UPDATE` per
  round).

`set` and `H` need a checkout that contains `20261105062155_lazy_ack_set_based.sql` (branch
`review/receipt-clock-fix-set-based`); on main the scripts stop with `patch anchors` / `H anchor`.

**Running:** set `SESSION_INGEST_TEST_PG_URL=postgresql://postgres@127.0.0.1:<port>/postgres` (an owned fixture).
Files named `*.bench.ts` run with `bun test <file>`. The `.ts`/`.mjs` scripts run with `bun <file>`. Nothing here is
a `*.test.ts`, and nothing is imported by `src/`.

## Scripts

| File | What it measures |
|---|---|
| `ack-phase-timing.bench.ts` | Single connection: per-phase time of one staged commit (ACK, seat_records, tail, COMMIT). Env `RCF_GATE`, `RCF_LIVE`, `RCF_EXPLAIN`. |
| `ack-breakdown.bench.ts` | Single connection: ACK time split by history state, trigger and its helpers, optional trigger/index removal, amplifier fixtures (`RCF_BIG_SLOTS`, `RCF_BIG_OUTBOX`, `RCF_LIVE_BUILDS`, `RCF_BIG_BUILDS`, `RCF_DERIVED`), `RCF_H=1`. |
| `h-diff.bench.ts` | PGlite: loop vs H-rounds on the same input; reports every table, column and sequence that differs. |
| `contention.ts` | C workers replaying one prepared batch each (`ROLLBACK`). Clock wait is measured from NOTICE timestamps around the clock lock line. |
| `contention-commit.ts` | C workers committing fresh, unique batches (`COMMIT`); durable rows are counted after the drain. |
| `contention-hx.ts` | As `contention-commit.ts`, for the 062156 candidate (gate on vs off), with distinct-scope states, per-worker `application_name` for `log_lock_waits`, and optional clock takers. Needs a checkout carrying 062156 (`review/receipt-clock-fix-h-exact`). |
| `locktimeout.mjs` | Scratch table: `lock_timeout` applies per lock acquisition, not to the whole wait. |
| `upgrade.mjs` | Scratch table: a `FOR KEY SHARE` holder's upgrade to `FOR NO KEY UPDATE` with a queued `FOR UPDATE` waiter. |

## Results (`results/`)

| File | Transaction end | Batches | Status |
|---|---|---|---|
| `run1-table1-rollback-replay.json` | ROLLBACK | replay | **Withdrawn rates**: the run window started before the warm-up calls. Latency, wait and error columns stand. |
| `run2-grid-rollback-replay-prefix.json` | ROLLBACK | replay | **Withdrawn rates**: same window bug. Latency, wait and error columns stand. |
| `run3-grid-rollback-replay-fixed.json` | ROLLBACK | replay | Window fixed. **H rows withdrawn**: replays bloat the same slot rows, and H ran last. |
| `run4a-real-commit-fresh.json` | COMMIT | fresh | Primary evidence (C=4/8). |
| `run4b-real-commit-fresh.json` | COMMIT | fresh | Primary evidence (C=8/16). Absolute numbers differ 1.5-2.5x from run 4a: compare only within one run. |
| `run5-h-exact-real-commit-fresh.json` | COMMIT | fresh | 062156 candidate vs gate off, two rotations per cell; lock timeouts by stage from `log_lock_waits`. Hold p50 is the stable signal; throughput and error counts swing between rotations. |
| `breakdown-single-connection.json` | ROLLBACK | single batch per cell | Single-connection breakdown by history state. |
| `amplifiers-single-connection.json` | ROLLBACK | single batch per cell | Amplifier fixtures and the H variant, single connection. |
| `h-equality-matrix.json` | ROLLBACK (PGlite) | n/a | Loop vs H-rounds stored differences. `authority_id` differences are fixture UUIDs. |

## What these results do not establish

- Production hold times, network round-trips or fsync.
- Which stage each `55P03` hit in runs 1-4 (`log_lock_waits` was off; run 5 has it).
- Stable absolute numbers on this host.
- An absence of `40P01` beyond the schedules that were run.
