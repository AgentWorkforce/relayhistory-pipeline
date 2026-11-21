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
