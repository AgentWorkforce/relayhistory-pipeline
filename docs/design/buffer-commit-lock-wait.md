# Buffered commit lock wait (`INGEST_BUFFER_COMMIT_LOCK_WAIT_MS`)

A default-off option of the private ingest Worker that lets the buffered commit, stage
`buffer_receipt`, wait longer for a lock than the ingest role's 2 s `lock_timeout`. With the option
unset, nothing changes.

## Alternatives not taken

- **Role setting** (`ALTER ROLE ... SET lock_timeout`): changes every statement of the role,
  including the claim and the stage intent. A role change is outside this option's scope.
- **Function-level `SET lock_timeout` through a migration**: needs a new slot and a schema release,
  and applies to every caller of the function. This option is Worker code only.
- **Explicit transaction** (`BEGIN; SET LOCAL ...; SELECT fn(...); COMMIT`): three or four round trips
  instead of one. The commit holds the seat clock row until it commits, so every extra round trip
  lengthens every hold.

## Mechanism

Option unset: the commit sends exactly the base statement, one parameter, one autocommit statement
(`src/session-ingest/ledger-sql.ts` `call`):

```sql
SELECT sessions.ingest_commit_buffered_batch($1::jsonb) AS result
```

Option set (`ledger-sql.ts` `bufferCommitLockWaitSql`, both the per-user and the origin entry point):

```sql
SELECT sessions.<fn>(CASE WHEN set_config('lock_timeout', $2, true) IS NOT NULL THEN $1::jsonb END) AS result
```

`$2` is `<ms>ms`. It is still one autocommit statement with no extra round trip. The transaction-local
`set_config` is the condition that produces the function's only argument, so it is evaluated before the
function runs. That is a data dependency, which the planner can neither drop nor reorder.
`set_config` is volatile, so it is never folded. `set_config(..., true)` is `SET LOCAL`: it ends with
the statement's implicit transaction, whether that commits or rolls back. The commit chain
(`ingest_commit_buffered_batch`, `ingest_origin_commit_buffered_batch`, `ingest_lazy_ack_batch`) has no
function-level `lock_timeout` that would override it.

The claim (`buffer` claim) and the stage intent (`buffer_intent`) keep the role settings.
`statement_timeout` is left alone. No role setting, migration, bootstrap file or slot changes.

## Range: 2001..4000 ms, buffered acceptance only

- **Lower bound 2001**: strictly longer than the role's `lock_timeout` (2 s). A shorter wait is a
  different control (see `INGEST_CLAIM_LOCK_WAIT_MS` for the claim).
- **Upper bound 4000**: one second below the role's 5 s `statement_timeout`. That second covers
  parse, plan and the commit's own work before and after the wait, so a single wait that starts early
  in the statement still ends with `55P03`, not `57014`.
- **Accepted forms**: an integer stack input (`sessionIngest.bufferCommitLockWaitMs`), a
  protected-environment variable `PRIVATE_INGEST_BUFFER_COMMIT_LOCK_WAIT_MS` of exactly four digits
  (empty means unset, because an unset GitHub variable renders empty), and a Worker variable
  `INGEST_BUFFER_COMMIT_LOCK_WAIT_MS` of exactly four digits.
- **Refused values**: anything else, including at the Worker an empty value, a space, a sign, a unit,
  an exponent, a fraction or a value outside the range. A refusal happens at deploy-plan time
  (`private_ingest_profile_buffer_commit_lock_wait`) and at Worker start. At Worker start the settings
  fail, so every request answers 503 `not_configured` and admission messages are retried. The Worker
  never runs with an unvalidated wait.
- **Canonical acceptance**: refused, because canonical acceptance has no buffered commit.

## Limits (measured on real PostgreSQL, synthetic cells)

- **The statement budget caps the wait.** `lock_timeout` applies to each lock acquisition, and
  `statement_timeout` (5 s) bounds the whole statement. A setting made inside the statement does not
  move that statement's timer: a statement that sets `statement_timeout` to 9 s and then runs for
  6 s still ends `57014` at 5 s.
- **A wait that reaches the budget ends as a statement timeout.** With the seat clock held for 7 s,
  lock waits of 4500 and 4900 ms ended `55P03` at the configured value, and 5000 and 5100 ms ended
  `57014` at 5 s. Two waits in one statement, or one wait that starts late, can also reach the budget
  inside the range.
- **The commit stage handles the two classes differently.** `55P03` maps to `ledger_lock_timeout`
  (`ledger-sql.ts` `ledgerErrorCodeFrom`). At `buffer_receipt` it is retried once under
  `INGEST_BUFFER_LOCK_RETRY` and answered 429 under `INGEST_BUFFER_LOCK_BUSY` (`deliver.ts`
  `bufferLedgerStage`). `57014` maps to `ledger_timeout`, is never retried or converted, and stays a
  503 that the DB-error alert counts. Both raise the `staged_ack` lock alert (`app.ts`
  `lockAlertFields`). The option can therefore move a refusal from the lock class to the timeout
  class; it does not remove refusals. It gains only when the holder releases within the configured
  wait and the statement still has budget.
- **A queue restarts the wait.** Commits queued on the seat clock behind each other wait again each time
  the lock passes to the next commit, and each wait has its own `lock_timeout`. In synthetic queue cells
  (8 commits of 500 records behind a 1.5 s holder), queued commits finished after more than 4 s in total
  without any 55P03. Queue depth times hold reaches the statement budget, not one long wait, and the
  option does not change that. In the same cells with the option at 4000, some runs ended commits 57014
  where the base wait ended them 55P03 or let them finish. The cells ran on a loaded shared host and
  varied between repetitions, so they show the direction only, not a rate (opt-in test `cells`,
  `BUFFER_COMMIT_WAIT_CELLS=1`).
- **The commit's own work after the wait counts against the budget.** A successful commit through the
  origin entry point once finished about 0.8 s after the holder released. The one second of headroom
  under 4000 is a margin, not a guarantee.
- **Each failed attempt holds its connection for up to the configured wait**, not 2 s. A retried
  stage still has the same worst case, about 10.25 s, because the statement budget bounds each
  attempt.

## Interactions

| With | Behaviour |
|---|---|
| `INGEST_BUFFER_LOCK_BUSY` | Unchanged. A commit that still ends `55P03` is the same 429 `BufferLockBusy`, and `57014` stays 503. |
| `INGEST_BUFFER_LOCK_RETRY` | Unchanged. The retry re-sends the same statement with the same setting. The worst case per stage stays about 10.25 s, because each attempt is bounded by the statement budget. A typical failed retry is now about 2 × value + 250 ms instead of about 4.25 s. Not refused. |
| `INGEST_CLAIM_LOCK_WAIT_MS` | Independent. That option covers the claim, this one the commit. |
| `INGEST_ORIGIN_CLAIM_FENCE` | The origin entry point carries the same setting, and so does the per-user fallback after a 42883. |
| In-flight lease (`inflight-lease.ts`) | Its 90 s holder TTL is far above one stage's worst case. Unchanged. |
| Slot lease (90 s TTL, `worker.ts` `deliverDepsFor`) | Same as the in-flight lease. |
| Worker request time | No request deadline is configured in this tree. The statement budget still bounds each attempt. |
| Connection pool | One pg client per request and per admission batch (`worker.ts`). A waiting commit holds its pooled connection up to the value instead of 2 s, so under a queue on the seat clock, concurrent connection hold time grows by up to the value / 2 s. The pool size is not visible in this tree. |
| Admission consumer | Uses the same deliver deps, so its commits carry the setting too. |

No combination is refused other than canonical acceptance. Every combination above stays inside the
statement budget per attempt.

## Prepared statements and caching

The Worker's `pg` client sends `client.query(text, params)` without a statement name
(`worker.ts`), which is the unnamed extended-protocol statement: parse, bind and execute every time,
with no result cache in the driver. The base statement has the same shape, and the new text is
protected exactly as the base is. That means the Hyperdrive configuration's caching is disabled
(`docs/session-ingest-design.md`, Supabase connection). This tree does not check that setting in
code, and it is not re-proved here.

## Release, turn-on and turn-off

`bufferCommitLockWaitMs` is a private-ingest cutover control (`private-ingest-release.ts`): it is
never tracked in the public profile, is supplied from `PRIVATE_INGEST_BUFFER_COMMIT_LOCK_WAIT_MS` by
`.github/workflows/deploy-private-ingest-worker.yml`, and is checked in the profile, in the live
version's bindings and in the readback.

- **Turn on**: set the variable to a value in range, then plan and deploy. The plan adds one
  plain-text binding.
- **Change**: set another value in range, then plan and deploy. The plan updates the binding, and the
  version readback must show exactly the new value. This is also the live replacement path when
  backing out a value without turning the option off.
- **Turn off**: unset the variable (or set it empty), then plan and deploy. The plan shows one
  `unbind` of `INGEST_BUFFER_COMMIT_LOCK_WAIT_MS`. The operator admits it
  (`bufferCommitLockWaitDeletionAllowed`) only when the candidate omits the value, the proposed
  environment has no value, the live value is exactly four digits within 2001..4000, and the deleted
  binding is the plain-text binding with that exact text. The operator admits at most one unbind per
  plan, so this unbind cannot share a plan with another unbind. Plan each from the live state the
  previous deploy produced.

Every one of these steps is a production change that needs its own approval. This document does not
approve any of them.

## What was proven, and what was not

Proven on PostgreSQL 16 and 17 in disposable containers
(`src/session-ingest/__tests__/buffer-commit-lock-wait.postgres.test.ts`). The setup was a login role
carrying the bootstrap file's role settings, a holder of the seat clock row, and the real
`sqlLedger().commitBuffered` over the input the real `deliverBatch` builds, through both entry
points.

- Unset, with the clock held 3 s: `55P03` at about 2 s.
- 4000, with the clock held 3 s: committed after about 3 s.
- 3000, with the clock held 4.5 s: `55P03` at about 3 s.
- 4000, with the clock held 7 s: `55P03` at about 4 s.
- A value past the range, sent directly: `57014` at 5 s.
- `lock_timeout` is back to 2s on the next statement after every outcome.
- The order holds for unnamed, named-prepared and forced-generic-plan executions.

Unit tests (`buffer-commit-lock-wait.test.ts`) pin the off-path text, the stage scope, validation, the
Worker wiring and the class handling through the real deliver path.

Not proven:

- The pooled proxy in front of the database (transaction pooling, statement handling).
- Real concurrency and queue depth.
- Waits on locks other than the seat clock row inside the commit.
- Any effect on a real database.
