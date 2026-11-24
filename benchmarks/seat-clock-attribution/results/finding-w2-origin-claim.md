# Finding: on a hot origin, the origin-claim key queues behind the seat clock wait

Synthetic cells only (`phase1-pg16`, PostgreSQL 16, one run per cell). No claim or prepare code was changed.

## What the code does

- Every origin-scoped call takes the transaction advisory lock
  `hashtextextended(['96bulls:ingest-origin-claim-v1', org, workspace, user, origin], 0)` before any other lock
  (`supabase/migrations/20261105062010_ingest_origin_claim_fence.sql:85-88`, `ingest_origin_claim_enter`).
- The claim, prepare and commit wrappers take it first (`062010:109`, `:119`, `:129`). It is a transaction lock, so it is
  held to COMMIT; `ingest_origin_claim_leave` (`062010:98-103`) only deletes the mark row.
- Serializing calls within one origin is the intended design (`062010:9-10`: ordering within one origin stream is
  required, across one user's origins it is not).
- The commit takes the seat clock later, inside the same transaction, through the `ingest_receipt_seat_ack` trigger
  -> `ingest_lazy_ack_batch` (`062210:228` on the base). So a commit holds its origin-claim key during its own clock wait
  and its whole clock hold.

## Consequence measured here

A same-origin call that queues on the origin-claim key waits for the holder's clock wait plus the holder's hold. The
clock queue is copied onto the origin key. W1 (one worker per origin) never shows this. W2 (half the batches on one
user and origin, served by C/2 workers) does:

| body | n | C | origin-claim enter, commit stage, p50 / p95 / max (ms) | stage failures |
|---|---|---|---|---|
| main | 250 | 4 | 0.24 / 0.45 / 118 | none |
| main | 250 | 16 | 0.58 / 1143 / 1960 | claim 55P03 x2, commit 55P03 x2 |
| main | 500 | 4 | 0.25 / 0.39 / 0.53 | none |
| main | 500 | 16 | 5.4 / 1181 / 1925 | claim 55P03 x14, prepare 55P03 x6, commit 55P03 x4 |
| t2 | 250 | 16 | 7.9 / 794 / 1845 | none |
| t2 | 500 | 16 | 45 / 1030 / 1466 | claim 55P03 x7, commit 55P03 x7, prepare 55P03 x1 |
| W1 (any) | any | any | p95 <= 1.3 | (clock only) |

- In the W2 C16 cells the origin-claim wait is of the same order as the clock wait at the tail (main n500: origin-claim
  p95 1181 ms, clock wait p95 1578 ms; main n250: 1143 vs 755). A large part of the hot origin's commit queueing sits on
  the origin-claim key, not on the clock row.
- Claims and prepares of the hot origin wait on the same key behind an in-flight commit (and its clock wait). The
  lock-timeout failures of the claim and prepare stages in W2 C16 come from that.
- The origin-claim wait is measured only for commits (the probed wrapper). For claims and prepares there are only the
  stage failures by SQLSTATE.

## What a shorter clock hold changes

Any reduction of the clock wait or hold shortens the origin-claim wait by the same amount per queued predecessor, but
the per-origin serialization stays. That is the intended order, and it is outside this unit.

## Evidence

- `results/phase1-pg16/summary.json`: per cell, `split.originClaimEnter`, `split.wait`, `stageFailures`.
- `results/phase1-pg16/txns-*-W2-*.jsonl.gz`: per transaction, `wrap.w0` / `wrap.w1` (server stamps, microseconds)
  around `ingest_origin_claim_enter`.
