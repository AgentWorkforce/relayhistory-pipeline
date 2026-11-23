# Seat clock hold attribution

Where the time goes between taking the receipt seat clock in an exclusive mode and COMMIT, inside
`sessions.ingest_lazy_ack_batch`, on a throwaway loopback PostgreSQL. Synthetic data only; CI does not run it.

Adapted from `benchmarks/seat-clock-contention/` at 1befee7a (the baseline; not changed): same URL refusals, schema build,
upload path (`deliverBatch -> sqlLedger`, buffered, origin claim fence on), W1/W2 streams and 2 s / 5 s limits as
`bulls96_session_ingest`. `load-worker.ts` is that harness's worker plus one post-COMMIT read.

## What differs from the baseline

- No lock sampler. Instead `probe.ts` reads the installed bodies of `sessions.ingest_lazy_ack_batch` and
  `sessions.ingest_origin_commit_buffered_batch` (`pg_get_functiondef`), inserts timing probes at fixed anchors (each must
  match exactly once) and installs the probed copies into the throwaway database. Nothing under `supabase/` or `src/`
  changes. A probe is three PL/pgSQL assignments; the trace goes out through a session-level `set_config`, read by the
  worker after COMMIT on the same connection (outside the hold). Failed transactions carry no trace.
- `cell.body`: `main` (the base body, T2 gate off), `t2` (gate fenced, mode t2, set with the shipped owner setters on the
  template), `main-plain` / `t2-plain` (unprobed, for the probe overhead check).
- Controls: none by default; `controls: "c2"` (plan or cell) runs the baseline's C2 ordered control loop unchanged.

## Measured span

- HOLD: from the server stamp right after the statement that took the clock exclusively returned (main: the clock
  `SELECT ... FOR NO KEY UPDATE`; t2: the upgrade `SELECT ... FOR NO KEY UPDATE`) to the client's COMMIT completion.
  Server stamps are put on the client clock per transaction (one server clock reading after COMMIT, bracketed by
  client stamps).
- CLOCK WAIT: the duration of that clock statement.
- The commit statement as the caller sees it is split into: origin-claim advisory enter, commit body before the ACK, ACK
  before the clock, clock wait, ACK after the lock (per-step), after the ACK (rest of the commit function, later AFTER
  triggers, origin-claim leave), result return; then the COMMIT round trip.
- Per-step figures are sums over the per-record loop per ACK.

## Running

```sh
docker run -d --name <name> -e POSTGRES_HOST_AUTH_METHOD=trust -p 127.0.0.1:<port>:5432 --cpus 8 --memory 8g --shm-size 1g \
  postgres:16 -c max_connections=300 -c shared_buffers=1GB
SESSION_INGEST_TEST_PG_URL=postgresql://postgres@127.0.0.1:<port>/postgres \
  bun benchmarks/seat-clock-attribution/attribution.ts "$(cat benchmarks/seat-clock-attribution/plans/phase1.json)"
```

Output: `results/<runId>/summary.json` and `txns-*.jsonl.gz` (every ledger transaction with its trace). `results/phase1.md`
renders the phase 1 runs.

## Limits

- Shared host; run-to-run noise is large (see `results/phase1.md`). One run per cell.
- The probes add work inside the hold; the overhead check at C=1 showed no difference beyond noise.
- PostgreSQL 16 only for phase 1.
