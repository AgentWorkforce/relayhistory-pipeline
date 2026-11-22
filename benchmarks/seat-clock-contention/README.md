# Receipt seat clock contention benchmark

A synthetic benchmark of lock contention on `sessions.ingest_receipt_seat_clock` along the buffered upload path. It runs
the current migrations and functions with their current lock order. Nothing in `supabase/` or `src/` is changed. It runs only
against a disposable loopback PostgreSQL that you create and destroy. CI does not run it.

## What it drives

- **Schema.** The repository migrator (`runMigrations`, local profile) runs once into a template database, as the
  `src/session-ingest/__tests__/*.postgres.test.ts` files do. That template also gets the lazy policy row
  (`ingest_lazy_policy_set`), the seat clock (`ingest_receipt_seat_initialize`) and the `pgrowlocks` extension (for the
  sampler only). Each run gets a fresh `CREATE DATABASE ... TEMPLATE` copy. All databases are dropped at exit.
- **Load.** The real upload path is `deliverBatch -> sqlLedger` with `acceptanceMode: "buffered"` and
  `originClaimFence: true`: claim, prepare, then commit with the staged ACK. The commit is
  `ingest_origin_commit_buffered_batch`, which fires `ingest_receipt_seat_ack` and then `ingest_lazy_ack_batch`.
  - Every ledger call is its own transaction, run as `bulls96_session_ingest`, with `SET LOCAL lock_timeout='2s'` and
    `statement_timeout='5s'`. These are that role's settings in `supabase/bootstrap/session_ingest_role.sql`; `SET ROLE`
    does not apply role settings, so the harness sets them itself.
  - Isolation is READ COMMITTED (the server default).
  - Every transaction is tagged `SET LOCAL application_name='scb:<stage>'`.
- **Workloads.** Each run has the same total number of records (`totalRecords`, default 64,000), built as a backlog before
  timing starts.
  - `W1`, distinct origins: C workers, each with its own user and origin (orgs rotate over 4). All of them compete only
    on the seat clock.
  - `W2`, hot origin: half the batches go to one user and origin, served by C/2 concurrent workers. The other half go
    to C/2 tail workers with one user and origin each. Hot-origin claims, prepares and commits queue on the per-origin
    advisory key, and commits also queue behind commits on the clock.
  - A failed delivery goes back on its worker's queue and is re-uploaded after a 100-300 ms pause, as a client would.
  - Warm-up deliveries (first-seen user and org adoption) run before timing and are excluded.
- **Ordered control.** At a fixed rate (`controlEveryMs`, default 1000 ms, one at a time), an owner connection runs
  `sessions.ingest_lazy_clear(<reset for a dedicated control user>, 'reset')` with the same 2 s / 5 s limits. That is the
  step of the lazy ordered control (`ingest_apply_ordered_control` -> `ingest_lazy_clear`) that takes the clock row
  `FOR UPDATE`, which conflicts with every other clock lock mode. The harness reports attempts, completions, failures by
  SQLSTATE, latency and missed slots.
- **Client.** Load runs in up to 8 Bun worker threads (`load-worker.ts`). The lock sampler runs in its own thread
  (`sampler-worker.ts`), so client CPU cannot delay it. The client's event-loop lag and the sampler period are reported
  for every run.

## How hold and wait are measured

Nothing fires between the clock's `SELECT ... FOR NO KEY UPDATE` and the per-record loop, and the harness does not change
the functions. So one superuser connection samples back to back (period reported, about 1 ms):

- `pgrowlocks('sessions.ingest_receipt_seat_clock')`: which transaction holds the clock row, and in which mode;
- the not-granted rows of `pg_locks` with `pg_blocking_pids`, plus the granted tuple locks: who waits, on what, and
  behind whom;
- the `application_name` of every active backend, which gives the stage.

Samples are matched to client-side transactions by backend pid and time window.

- **HOLD** applies to a transaction holding the clock in an exclusive mode (staged ACK: NO KEY UPDATE; control: FOR
  UPDATE). It runs from the first sample that shows the transaction as the clock locker to the end of its COMMIT. That
  end is the client's COMMIT completion, or one sample period after the last locker sample if the client saw it later.
  The start has one sample period of resolution.
- **WAIT for the clock** runs from the first sample that shows the transaction waiting on the clock row to the first
  sample that shows it holding the row. A waiter on the row waits either on the clock's tuple lock or on the holder's
  transaction id while holding that tuple lock. A transaction never seen waiting counts as 0.
- **Lock waits by class** is the number of samples showing a transaction waiting on a class, times the sample period.
  The classes are clock, row:<table>, advisory:native-origin, advisory:origin-claim, the erase gate (961996,50000),
  and xid (a row lock or unique-index insert conflict on another transaction).

## Running

```sh
docker run -d --name <name> -e POSTGRES_HOST_AUTH_METHOD=trust -p 127.0.0.1:<port>:5432 --cpus 8 --memory 8g --shm-size 1g \
  postgres:16 -c max_connections=300 -c shared_buffers=1GB -c track_functions=all
SESSION_INGEST_TEST_PG_URL=postgresql://postgres@127.0.0.1:<port>/postgres \
  bun benchmarks/seat-clock-contention/bench.ts "$(cat benchmarks/seat-clock-contention/plans/matrix.json)"
bun benchmarks/seat-clock-contention/report.ts <runId>
docker rm -f <name>
```

The plan is `{ runId?, totalRecords?, maxSeconds?, controlEveryMs?, originClaimFence?, historyLive?, threads?, cells: [{ workload, n, c }] }`.
`historyLive` left unset keeps the installed `ingest_history_switch.live` (true).

The script refuses any URL that is not a loopback IP literal, or that has a password. Output goes to
`results/<runId>/`: `summary.json` (every metric), `report.md` (tables), `txns-*.jsonl.gz` (every transaction: stage,
pid, times, SQLSTATE, first and last clock-holder sample, wait samples by class) and `graph-*.json` (the full wait graph
counts).

## What the data cannot show

- **Production transaction lengths.** Production adds network round trips through the connection proxy, its own storage
  and fsync, and a different CPU. The COMMIT round trip sits inside the hold. Here it is one loopback round trip
  (reported as `rtt`).
- **Production concurrency, batch mix and client retry behaviour.** The retry pause here is fixed at 100-300 ms; the
  Worker's `bufferLockRetry` / `bufferLockBusy` flags stay off, which is their default.
- **Short holds.** Sampling resolution is about 1 ms, so holds shorter than that can go unobserved (counted as
  `unobservedHolds`).
- **Time inside the hold by phase.** The sampler sees who holds the clock, not which statement inside
  `ingest_lazy_ack_batch` is running.
- **Server capacity.** The PostgreSQL container shares the host with the client threads and is capped at the CPUs and
  memory given above.
