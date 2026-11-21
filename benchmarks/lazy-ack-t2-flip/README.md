# 062210 try-lock flip under ingest load (evidence, not for landing as-is)

`flip-bench.ts` measures whether the 062210 operator flip (`sessions.lazy_ack_t2_set_mode`, try-lock only) finds a
free instant on the (961996,50000) barrier while uploads run. It also measures what uploads see meanwhile.

**Safety:**
- Throwaway loopback PostgreSQL only. The script refuses a non-loopback URL or a URL with a password.
- It creates and drops its own database. Synthetic data only.
- Flipping the mode, changing the gate or applying the migration on any real database is Khaliq's decision. This
  script does none of that.

**Load:** C workers. Each is a distinct user and origin, and runs `deliverBatch` back to back with fresh N-record
batches through the real upload path:
- claim, prepare, then commit+ACK through `ingest_origin_commit_buffered_batch`;
- each ledger call is its own autocommit transaction, as the runtime role, with `lock_timeout` 2 s and
  `statement_timeout` 5 s.

Every one of those transactions holds 50000 shared, so the barrier is free only between them.

**Flipper:**
- The gate is `fenced`.
- Every `gapMs` the operator helper (`src/session-ingest/lazy-ack-t2-flip.ts`, outcome form) tries to toggle the
  mode within `budgetMs`, with jittered backoff.
- Each attempt is one autocommit statement.
- Control cells (`budgetMs: null`) run the same load with no flipper.

**Running:**

```
SESSION_INGEST_TEST_PG_URL=postgresql://postgres@127.0.0.1:<port>/postgres RCF_OUT=<file> bun benchmarks/lazy-ack-t2-flip/flip-bench.ts '<plan json>'
```

`results/run1-plan.json` is the plan used for run 1.

**What it does not show:**
- Production transaction lengths. Network, Hyperdrive and fsync all lengthen every shared hold. Local macOS fsync does
  not force stable media.
- Production concurrency or tenant mix.
- Client retry behaviour.
- Publisher or controls traffic. The real publisher under back-to-back flips is checked separately in
  `src/session-ingest/__tests__/publication.postgres.test.ts`.

The barrier-free fraction falls as concurrency and hold time grow, so treat the numbers as an upper bound on
production success.

## Results (`results/`): local loopback PostgreSQL 16.15, one laptop

| File | Cells | Status |
|---|---|---|
| `run1-partial.jsonl` (plan `run1-plan.json`) | C=8, 30 s cells, budgets 250 ms / 1 s / 5 s and one fast backoff | **The n=100 cells stand.** Throughput fell across cells, flip and no-flip alike (cells ran in sequence on a growing database). **The n=500 cells are withdrawn:** the host disk filled (`No space left on device`), hence the `delivery_unavailable` and `57P03` failures. |
| `run2-c8.json`, `run2-c4.json`, `run2-c2.json` | C=8/4/2, 20 s cells: a no-flip control, then a flip cell (budget 5 s), then another control, for n=100 and for n=500 | Primary evidence. Compare each flip cell with the controls on either side of it. |

**Run 2, flips with a 5 s budget and 1 s between tries:**
- C=8: 1 of 4 tries switched at n=100 and 1 of 4 at n=500. Each success took about 20 attempts and 1.3-2.4 s.
- C=4: 1 of 4 at both sizes.
- C=2: 12 of 12 at n=100 (median 0.55 s, max 2.2 s) and 4 of 6 at n=500.
- Run 1 at C=8, n=100: 0 of 59 tries over 30 s per budget (250 ms, 1 s, 5 s, and a 1 s budget with fast backoff).

**Uploads:** no failures in any valid cell. Each flip cell's throughput falls between its two controls or above both (C=4, n=500), so no stall can be attributed to failed tries.

**Reading:** under sustained back-to-back load from 4 or more uploaders, a 50000 try-lock rarely finds an instant with no shared holder. Production transactions are longer, so that instant is rarer there.
