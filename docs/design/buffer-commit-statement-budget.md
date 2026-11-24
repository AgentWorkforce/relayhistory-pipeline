# Buffered commit statement budget and the buffer-stage timeout class

Two default-off options of the private ingest Worker. They build on the buffered commit lock wait
(`docs/design/buffer-commit-lock-wait.md`). With both unset, nothing changes.

| Option | Worker variable | Stack input | Protected-environment variable |
|---|---|---|---|
| Statement budget | `INGEST_BUFFER_COMMIT_STATEMENT_BUDGET_MS` | `bufferCommitBudgetMs` | `PRIVATE_INGEST_BUFFER_COMMIT_STATEMENT_BUDGET_MS` |
| Timeout class | `INGEST_BUFFER_TIMEOUT_BUSY` | `bufferTimeoutBusy` | `PRIVATE_INGEST_BUFFER_TIMEOUT_BUSY` |

## Enable conditions

- **Prove the proxy first.** Before any enable of the statement budget, a dev proof through the
  connection proxy (Hyperdrive) is needed. Two things are unproven: the multi-statement opening message,
  and transaction pinning across the three flights. If the proxy refuses the message, every buffered
  commit answers 503 once the budget is set.
- **Turn on the timeout class first.** Do not enable `INGEST_BUFFER_COMMIT_LOCK_WAIT_MS` before
  `INGEST_BUFFER_TIMEOUT_BUSY` is on. A longer lock wait can move a commit refusal from 55P03, which is
  retried and answered 429, to 57014, which is otherwise one attempt and a 503 with no `Retry-After`.
  This order is advice: nothing in validation enforces it.
- **What the timeout class does to alerting.** With `INGEST_BUFFER_TIMEOUT_BUSY` on together with
  `INGEST_BUFFER_LOCK_BUSY`, a commit or stage-intent statement timeout is answered 429. It then no
  longer feeds the DB-error alert, which counts 503s only (`src/session-ingest/ingest-error-alert.ts`).
  It stays visible only through the `ingest.lock_alert` line (`staged_ack` / `staged_intent`, `sqlstate`
  57014) and the request line (`error_code: ledger_timeout`, `db_error_code: 57014`). This is the same
  trade the claim's `INGEST_CLAIM_TIMEOUT_BUSY` makes.

## Part 1: the statement budget

### Why an explicit transaction

A setting made inside a statement cannot move that statement's timer. Measured: a statement that
sets `statement_timeout` to 9 s still ends 57014 at 5 s. The only Worker-side way to give the commit a
budget other than the role's 5 s, without changing the role or a function, is therefore a transaction
of its own. Role settings, function-level settings and migrations are out of scope.

### Shape (`src/session-ingest/ledger-sql.ts` `inBufferCommitBudget`)

1. **One flight with no parameters.** The driver sends it as a single simple-protocol message:
   `BEGIN; SET LOCAL statement_timeout = '<budget>ms'; SET LOCAL idle_in_transaction_session_timeout = '1000ms'`,
   plus `SET LOCAL lock_timeout = '<ms>ms'` when `INGEST_BUFFER_COMMIT_LOCK_WAIT_MS` is set. The values
   are validated integers.
2. **The unchanged one-parameter call.** The per-user or origin entry point is used, and each
   origin-fence fallback attempt runs as its own transaction.
3. **`COMMIT`.**

Any failure before `COMMIT` is followed by `ROLLBACK` and the first error is kept. Only the
`buffer_receipt` commit changes; the claim and the stage intent are untouched. With the option unset,
the commit is unit A's statement, or the base statement, byte for byte.

The driver as used here (`pg`, one `Client` per request) cannot pipeline. The three flights each wait
for the previous answer: three round trips instead of one.

### Range: 5001..7000 ms, buffered only

- **Lower bound**: above the role's 5 s, because a shorter budget is not this option's purpose.
- **Upper bound**: 7000 ms. The commit stage's statement time with its one `INGEST_BUFFER_LOCK_RETRY`
  retry, 2 × 7 s + 250 ms, stays under 15 s, the shortest sender request timeout we were told of. That
  timeout is not in this repository.
  - The bound covers commit-stage statement time only.
  - Round trips add to it: about 6 with a retry.
  - Earlier stages add to it. With `INGEST_BUFFER_TIMEOUT_BUSY` and `INGEST_BUFFER_LOCK_RETRY` on,
    `buffer_intent` can itself retry a 57014 (up to 5 s + 250 ms) before the commit stage starts.
  - The whole request can exceed 15 s on the base already.
- **Lock wait**: the commit's lock wait (`INGEST_BUFFER_COMMIT_LOCK_WAIT_MS`, or the role's 2 s) must
  end at least 1 s before the budget. Validation refuses the combination otherwise. Within the two
  ranges it always holds (4000 ≤ 5001 − 1000).
- **Unset or empty**: means unset at every layer, the same rule as the lock wait.
- **Refused values**: anything else, at deploy-plan time (`private_ingest_profile_buffer_commit_budget`)
  and at Worker start, where every request is answered 503 `not_configured`.
- **Worker request time limit**: none is configured in this repository.

### Outcomes

| Failure point | Batch | Worker answer | The client's repeat |
|---|---|---|---|
| Before the call (opening flight fails) | Nothing written; `ROLLBACK` | 503 (mapped SQLSTATE) | The same commit, from scratch |
| During the call (55P03 / 57014 / other) | Rolled back | As today for that cause | The same commit |
| At `COMMIT` with a SQLSTATE | Rolled back | 503 (mapped) | The same commit |
| `COMMIT` sent, answer lost | **Unknown to the Worker; in the proof it had committed** | 503 `ledger_connection` (an 08xxx or E* code) or `ledger_error` (no code, e.g. pg's "Connection terminated unexpectedly"); never a refusal. The proof injects a synthetic ECONNRESET. | The function answers the repeat with the same result and does not move the seat (proved) |
| More than 1 s between flights while the Worker is alive | Rolled back for certain: the server ends the session at the 1 s idle-in-transaction bound | 503 `ledger_error`. Reproduced on 15/16/17 with a 1.2 s pause before the call: the server sent 25P03 and "Connection terminated unexpectedly", seen only on the client's error event; the next flight failed "not queryable" with no SQLSTATE; ROLLBACK was attempted; nothing was written. Not `ledger_timeout`, so part 2 never retries it. | The same commit on a new connection. On the admission consumer one client serves the whole queue batch, so the later messages of that batch fail too and go to queue backoff |
| Worker dies between flights, socket closed | Rolled back by the server at once | (none) | The same commit |
| Worker stops but the connection stays open | Rolled back by the 1 s idle-in-transaction bound | (none) | The same commit |

A repeat that arrives while the first transaction is still open waits for it, then answers the same
result (proved).

## Part 2: the timeout class (`src/session-ingest/deliver.ts` `bufferLedgerStage`)

**When it applies.** At `buffer_intent` and `buffer_receipt` only, a failure with cause
`ledger_timeout` and SQLSTATE 57014 by code joins the lock class. A name-only client timeout never
qualifies.

**What it does.** Under `INGEST_BUFFER_LOCK_RETRY` the call is retried once after 50–250 ms. Under
`INGEST_BUFFER_LOCK_BUSY` the answer is 429 `delivery_busy` with the lock-busy `Retry-After` of
5–10 s (`CLAIM_LOCK_RETRY_AFTER`). Without either of those flags it changes nothing.

**Why both stages are safe to retry.** Each call is one statement or one transaction that rolled back
whole. `ingest_prepare_buffer_stage` is idempotent per claim and stage key. The commit answers a
repeat with the same receipt.

**The slow-call case.** A retry of a call that is simply slow, rather than queued, is that call once
more. The extra time is at most one more budget plus 250 ms per stage, and the retry happens only once.

**Keeping the classes apart.** The request line keeps `error_code: ledger_timeout` and
`db_error_code: 57014`. The retry line keeps `db_error_code` of the first failure. No new log key.

**Alerting.** The lock-wait alert is a runbook definition (`runbooks/history-620-operations.md`,
"Lock-wait alert"); no alert query is defined in code. Its commit numerator counts every
`ingest.lock_alert` `staged_ack` line, which includes sqlstate 57014 today. A test pins that a
converted 57014 still emits that line through the real `logLine`. Its denominator ("successful lines
plus 503 lines with `stage=buffer_receipt`") does not count a 429 at `buffer_receipt`, so with the flag
on the rate reads higher, not lower. `INGEST_BUFFER_LOCK_BUSY` already has the same property.

## Measured (synthetic, PostgreSQL 15/16/17 containers, `buffer-commit-statement-budget.postgres.test.ts`)

**Two sequential waits on a 1-record batch.** The seat clock was held 3 s, then the origin advisory
lock to 6 s, with a 4 s lock wait:

- as a single statement: 57014 at 5.0 s;
- with a 7000 ms budget: committed at 6.0 s;
- past a 6000 ms budget: 57014 at 6.0 s.

**Hygiene.** After success, 55P03, 57014, a refused opening flight and a `COMMIT` that was never sent,
the next statement saw the role's settings (2 s, 5 s, 10 s) and the backend was idle with no
transaction id.

**Extra seat-clock hold.** This is the time from the call's answer to `COMMIT` completing; the single
statement releases before it answers. Median of 5 runs, loopback:

| Records | Budget off | Budget on |
|---|---|---|
| 250 | 0 ms | about 2.5 ms |
| 500 | 0 ms | about 2.4 ms |

Over a real network the extra hold is at least one round trip from the Worker to the database through
the pooled proxy; that was not measured.

**Failure path.** When the call fails inside the explicit transaction (55P03 or 57014), the aborted
transaction keeps the locks it took before the failure until the `ROLLBACK` round trip arrives. Before
this option, the autocommit statement released them when it failed. With the retry flag, the retry's
pause starts after that `ROLLBACK`. Commit-stage wall time was within noise off and on (250
records about 50 ms, 500 records about 105 ms, loopback).

## Not proven

- The pooled proxy in front of the database (Hyperdrive): transaction pooling across the three
  flights, a simple-protocol multi-statement message through it, what it does with a client that
  disappears mid-transaction, and its round-trip time.
- Real concurrency, and how much the extra hold per commit lengthens a queue on the seat clock.
- The sender's own timeouts, which are not in this repository.
