# Backport flags on main

The hosted session-pipeline Lambda ran a backport line. It had three default-off features that main did not
have. They are ported here as behaviour, not as patches, because main's search stage and consumer have changed.

Each flag keeps the backport's variable name and the backport's accepted values. So an environment that
turned a feature on for the backport turns on the same feature here. Every flag is off by default. With a
flag unset, `""` or `false` (for `SESSION_PIPELINE_RETRY_VISIBILITY_S`: unset, `""` or `0`), the code runs
the same statements, reads, writes and log lines as the base commit 9d1adf84. The golden test
`src/session-pipeline/__tests__/search-index-forward-port-off-golden.pglite.test.ts` checks this against a
transcript recorded at 9d1adf84.

This change contains code, tests and docs only. It deploys nothing, and nothing in it turns a flag on.

## SEARCH_INDEX_SKIP_ACCEPTANCE_MISSING_ENABLED

The backport commits are 1c931eff, 0c4fe2be and 247d9994.

### Grammar

| Value | Result |
| --- | --- |
| unset, `""`, `false` | Off. |
| `true` | On. Needs `SEARCH_INDEX_ENABLED=1`. |
| anything else | Init fails and names the variable. |

### What it does

A pass can end incomplete with exactly one reason, `relayhistory_acceptance_missing`, from the whole read
or the streamed read. It must also have read every pointer as listed: the same revision, content sha256 and
etag as the pointer row. Such a pass writes a stamp `{v: 1, reason, digest}` into `search_ingest_state`
under `acceptance_skip/<sha256 of the scope>`.

A later pass skips when it finds a matching stamp. A stamp matches when it is the latest row, it has the
same digest, and it was written less than six hours away from the pass's time, in either direction. The
skip happens before any manifest read. It returns `complete: false`, `skippedBeforeRead: true` and the
reasons `relayhistory_acceptance_missing, skipped_before_read`. It writes nothing.

Any other outcome writes a tombstone, but only if the pass saw a stamp or could not read it. Erasing the
scope also writes one. When the pass saw a stamp, the tombstone's time is the later of the pass's time and
the stamp's time plus 1 ms.

If the stamp read fails, the pass runs in full (fail open). If a stamp write fails, the pass logs one warn
line, `search index acceptance stamp not written`, and its result is unchanged.

### How main differs from the backport

- **Read set.** Main reads the canonical sibling union of a scope (`readCanonicalScope` over every live row
  with the same canonical session id), so the digest is built from that set. Each row contributes its key,
  session, machine, revision, content sha256, etag and scrubbed flag. The digest also covers:
  - the canonical id and the directory epoch, which the pointer trigger moves on every pointer change;
  - the window marker;
  - the read limits;
  - the stream budgets;
  - `READER_MAX_RULESET`.
- **Salt.** The salt is `acceptance-skip-v2`, so a stamp written by the backport line never matches here.
  The first pass after a deploy reads in full and writes a v2 stamp.
- **Facts listing.** With the flag on, the sibling listing selects each row's facts in the same statement,
  with the same filter and order, so it is still one statement. With the flag off, it is the original
  statement.
- **Read observation.** The pass observes what each manifest read returned through a wrapper around the
  reader. `readCanonicalScope` is unchanged.
- **What a skip leaves out.** On main, an incomplete whole read inserts no rows. It returns before the
  index scan, after `invalidateDirectoryEvents` and a `needs_full = 1` state row. So a skip leaves out only
  those two writes, which the stamped pass already made. The backport's caveat that "a skip does not
  re-insert lost rows" has nothing to apply to on main.
- **Where the skip applies.** Only on the S3-event pass on the hosted legacy ClickHouse binding. The
  function that decides this is `inlineSkipsApply` in `search-index.ts`. The skip does not apply to these
  passes:
  - **The managed search-jobs (M2) pass.** `runSearchJob` sets `committedGuard`, so the M2 pass never
    reads, writes or honours a stamp. Its pickup, lease, `held` (design §4.5), backoff, exhaustion and
    `ready` decisions are byte-identical with the flag on or off. When the M2 flags are off there is no M2
    pass at all. `search-jobs.ts` is not changed.
  - **The staged reconcile, a portable search adapter, and either output-facts store** (with
    `SEARCH_INDEX_OUTPUT_FACTS=1`, the flag does nothing).
- **Pre-read.** With the flag on, `SEARCH_INDEX_PREREAD_ENABLED` passes take the locked base path. Without
  that, the pre-read would read the manifests before the skip could decide.

### Conditions carried over from the backport

- **INSERT grant.** The pipeline's ClickHouse user needs INSERT on `search_ingest_state`. Without it,
  every stamp write logs a warn line, nothing is ever stamped, and every pass reads in full.
- **Bump the salt with code changes.** Bump `ACCEPTANCE_SKIP_VERSION` with any change to the fold, the
  transform or the projection. Otherwise purge the `acceptance_skip/` rows after every instance runs the
  new code.
- **Writers must raise the revision.** Every manifest writer must raise the revision when a manifest's
  content changes. The pointer upsert keeps a row at the same revision.
- **Readers outside this repository.** Any reader that lists `search_ingest_state` by prefix must ignore
  `acceptance_skip/` names.

### Rollback

Unset the flag, or set it to `false`. Leftover stamps are then inert.

## SEARCH_INDEX_SKIP_OVER_BOUND_ENABLED

The backport commits are 14e9c00a and 95776f5b.

### Grammar

The grammar is the same as for the acceptance flag:

| Value | Result |
| --- | --- |
| unset, `""`, `false` | Off. |
| `true` | On. Needs `SEARCH_INDEX_ENABLED=1`. |
| anything else | Init fails and names the variable. |

The two skip flags are independent.

### What it does

The pass checks three conditions before any manifest read. If all three hold, it returns the size refusal
again without reading or writing anything. The result is `partial: true`, `skippedBeforeRead: true` and the
reasons `scope_too_large, skipped_before_read`. If any condition fails, the normal pass runs.

1. **Pointer sizes.** The summed `raw_bytes` of the live, scrubbed sibling pointers is over
   `maxScopeBytes`. The bound is strict.
2. **Stream guard.** With stream budgets (`SEARCH_INDEX_STREAMING_CH=1`), four times that sum is also over
   the stream byte budget, so the streamed path could not take the scope either.
3. **State row.** The scope's latest `search_ingest_session_state` row has `needs_full = 1`,
   `scanned_rows = 0` and the highest live sibling revision. It was written less than six hours away from
   the pass's time. This is one bounded read, made only after 1 and 2 hold. If the read fails, the normal
   pass runs.

The consumer handles the result like a size refusal. It logs one `search index partial` warn line with the
reason `scope_too_large,skipped_before_read` and one `search index reconciled` line. It does not requeue
the message and does not report a batch item failure.

### How main differs from the backport

- **Pointer set.** The sizes, revisions and count come from the same sibling-facts listing the read uses
  (the canonical union). The backport ran a separate aggregate query, so its "live count changed between
  listing and facts" check is not needed here.
- **Order of checks.** The skip runs after skip-current and before the acceptance stamp.
- **Where the skip applies.** The same `inlineSkipsApply` rule applies as for the acceptance flag. The
  managed search-jobs pass never skips. Its over-bound pass stays `held`, exactly as with the flag off.
- **Reasons.** On main, a pass refused by the stream budgets reports
  `scope_too_large, stream_read_bytes_budget`. Its skip reports `scope_too_large, skipped_before_read`, as on
  the backport.
- **Known gap.** A live scrubbed row that the read would still refuse is counted in the sum. Two such
  refusals are a key mismatch and a scrub ruleset above `READER_MAX_RULESET`. The normal pass for such a
  scope is incomplete as well, and it indexes nothing either.

## SESSION_PIPELINE_RETRY_VISIBILITY_S

The backport commits are ae153e62 and 35cf5404, on `review/pipeline-handoff-sfmini-pmp-fast-retry`.

### Grammar

| Value | Result |
| --- | --- |
| unset, `""`, `0` | Off. |
| an integer from 1 to 43200 | On. 60 is the intended value. |
| anything else | Init fails and names the variable. Examples: `60s`, ` 60`, `1.5`, `43201`, a value of more than five digits. |

### What it does

A message can fail in the pointer stage with exactly `pointer_move_pending`. If that message fed no other
failure of the batch, it gets one SQS `ChangeMessageVisibility` call with the configured seconds. The
message is still returned in `batchItemFailures`. A message that also fed any other failure keeps the
queue's full visibility timeout. The other failures are:

- another key's pointer error;
- an invalid record;
- a buffered-stage feed;
- the large-read route;
- the search pass;
- the replay publication.

On main, the search pass skips a scope whose every message already failed. A pmp message that only shares
such a scope is therefore still pmp-only.

The receipt handle reaches the consumer only while the flag is armed (`queueRecords`). It is used only as
the call's argument and is never logged.

Each call is best effort:

- **Bound.** Each call is bounded by `min(2 s, remaining - 1 s)`. The same bound goes to the SQS request
  timeout. The calls run concurrently.
- **Failure.** A rejected call logs the warn line `retry visibility not shortened` with the reason
  `change_visibility_failed`. A call that times out logs it with the reason `change_visibility_timeout`. A
  missing receipt handle logs it with the reason `no_receipt_handle`. In every case the message still fails
  with the normal timeout.
- **Deadline.** `invocationDeps` passes the invocation's remaining time whenever the flag is armed and a
  context is present, with or without streaming. Right before the calls, if less than 3 s is left, the
  consumer makes no call. It logs one warn line with the reason `retry_visibility_deadline` and the count,
  and returns the response at once.
- **No context.** Without a context, each call has the fixed 2 s bound.

### How main differs from the backport

- **Failure sites.** Main's consumer has more failure sites than the backport's: buffered stages, the
  large-read route and replay. Every failure site goes through one `fail(id, fast)` helper, so each new
  site keeps the full timeout. With the flag off, the helper only adds the message to the failed set, as
  before.
- **When the calls run.** The calls run last, after the search-jobs follow-on (`SEARCH_JOBS_ENABLED`),
  which never fails a message. So a 60 s visibility cannot lapse while the invocation is still running. A
  follow-on that runs close to the deadline is covered by the deadline check. With the M2 flags off, there
  is no follow-on.
- **Batch records.** The batch records are `{ messageId, body }` plus `receiptHandle` only when the flag is
  armed. Main has no `sentTimestamp`.
- **Tests.** The tests drive the real exported handler through `createPipelineLambdaHandler({ loadRuntime })`.
  The backport used `setRuntimeForTests`, which is not ported.

### IAM

The Lambda role needs `sqs:ChangeMessageVisibility` on the source queue. The in-repo stack policy
(`stacks/aws/session-pipeline.ts`) lists it. Whether the deployed role has it is not checked here. Without
it, each call fails safely with a warn line.

## Pool checkout timeout (not a flag)

The pipeline pool now sets `connectionTimeoutMillis: HOOPDB_CONNECT_TIMEOUT_MS` (10 000 ms), the same
constant and value as the deployed backport (6d88b638). Before this, main's `supabasePool` had no checkout
bound. A checkout can fail in two ways:

- **No connection.** The pooler accepts TCP but never answers.
- **No free client.** Every client is in use (`HOOPDB_POOL_MAX`).

In either case the checkout now fails after about 10 s. The stage that asked for it fails its messages, and
the batch reports them in `batchItemFailures` for SQS to redeliver. Without the bound, the invocation would
run to the 300 s Lambda timeout. `lambda-pool-timeout.test.ts` tests both cases through the real batch
path.

## SEARCH_INDEX_STREAMING_CH=1: guards carried over from the backport

The backport commits are 59b74b18, 6264d119 and 51f876b6. This flag is live in production, so each item
below changes real behaviour on main to match the deployed backport. With the flag unset, nothing here runs.

- **Guarded search transaction.** `transaction(db, work, guarded)` is guarded exactly when the flag is
  armed (`searchTransaction`). Guarded, a per-checkout `'error'` listener keeps a dropped connection's
  error, so it cannot become an uncaught exception that crashes the invocation and loses the batch
  response. The next statement rejects with that error, and the pass fails as a normal rejection. The
  ROLLBACK attempt is skipped because the connection is gone. The listener is removed, and the client is
  released with the error, which destroys it instead of returning it to the pool. Unguarded, the statements
  are the same as before.
  - **S3-event path.** The scope's messages fail and SQS redelivers them. The redelivered batch completes.
  - **M2 job paths.** The job's result is `{ error }`. It is settled on M2's own backoff (`retried`),
    unchanged.
  - **Claim and settle.** These use `searchJobsSql`, which is unguarded. Only the search pass inside a job
    shares the guarded transaction.
- **Start minimum.** A stream does not start with less than `STREAM_MIN_START_MS` (120 s) left before the
  invocation's stream deadline. The deadline is the remaining time less `SEARCH_INDEX_STREAM_DEADLINE_MARGIN_MS`,
  60 s by default. So a stream needs at least 180 s of remaining time to start. With less, it returns
  `deferred`, like a second streamed scope:
  - **S3-event batch.** The scope is requeued with `stream_deferred`.
  - **M2 job.** No job is claimed when the work could only be deferred (see "M2: no claim a deferral would
    waste" below). A job that is deferred after its claim settles `busy`, which gives the attempt back.
  - Only a pass that has a deadline checks the start minimum. The self-host consumer never sets one.
- **One stream per invocation, even without a context.** `invocationSearchDeps` gives every invocation its
  own `streamSlot` whenever streaming is armed. That includes the case with no Lambda context or a
  non-finite remaining time. 9d1adf84 set no slot there, so such an invocation could stream every scope it
  held, with no limit. Without a context there is still no deadline, as on the backport.
- **Scheduled `{searchJobs: true}` path.** This path now uses `invocationSearchDeps(rt, context)`, so it
  gets the same slot, deadline, start minimum and guarded transaction as an SQS batch and its follow-on.
  It used to pass `rt.deps.search` directly, with no slot and no deadline. The claim SQL, the lease and the
  epoch boundaries are unchanged.

Tests in `stream-guards.pglite.test.ts`, through the real exported handler:

- remaining time 1 ms below and exactly at the threshold;
- no context;
- a connection dropped mid-stream, on the SQS path and on the scheduled path;
- the scheduled path's slot, start minimum and no-context cases;
- `transaction()` and `searchTransaction` statement parity and guard wiring.

### The rest of the backport's streamed-reconcile protections

The backport commits are e79df11b, 6264d119, 51f876b6 and c9a0c68e. They apply only to a hosted streamed pass,
meaning a pass that carries the invocation's `streamSlot`. The self-host consumer's streamed pass is unchanged.

- **Batch deadline requeue (`stream_batch_deadline`).** Once the invocation's stream deadline has passed, no
  further scope of an SQS batch starts, whole read or streamed. Each remaining scope is requeued with the
  reason `stream_batch_deadline`, carrying the original upload time.
  - If the requeue itself fails, that scope's messages fail and SQS redelivers them. Scopes already done
    are acknowledged.
  - The M2 analogue is the existing claim deadline: no job is claimed once the remaining time reaches the
    margin.
- **Tail reserve (`STREAM_TAIL_RESERVE_MS`, 30 s).** The pass does not start its tail with less than 30 s
  left before the deadline. The tail is the delete statements, then the directory and catalog writes, the
  state row and COMMIT.
  - The check runs before the first delete statement, even with nothing to delete, and before every later
    delete statement.
  - Deletes go in chunks, each of which `deleteRows` sends as one statement: at most 500 keys, and within
    its byte budget by an upper-bound estimate (`deleteStatementChunks`). An unthrottled lock check comes
    before every chunk.
  - A stop between chunks keeps the deletes already done and leaves the scope owing a pass
    (`scope_too_large, stream_deadline`, `needs_full = 1`).
- **Lock-checking keepalive.** The keepalive re-takes the scope lock the transaction already holds, which
  is re-entrant. Any answer but `ok = true` throws `stream_keepalive_failed`, which is retryable. After
  that no row is inserted or deleted, no directory or catalog write happens and no final state row is
  written. The message fails and is redelivered.
- **`needs_full = 1` before reading.** The pass records `needs_full = 1` one millisecond before its own
  state time, before anything is read. A pass interrupted on the way, even one whose best-effort final
  state write is lost, therefore leaves the scope owing a full pass. A previously failed or partial state
  is never overwritten as complete. The next pass reads in full.
- **Strict keyset pages.** Every keyset page of the ClickHouse scan must meet three conditions before
  anything is projected or written:
  - it holds at most the rows asked for;
  - its keys are in strictly increasing order of (ts, session_id, machine_id, kind, event_id), with strings
    compared as UTF-8 bytes;
  - it comes after the previous page's last key.

  Otherwise the pass stops with `clickhouse_page_invalid`. Nothing is written and the scope is owed a pass.
  This check applies to every user of `legacyProjectionIo.scanPages`.

### M2: no claim a deferral would waste

Migration 062300 charges an attempt for a `retry` settlement and parks a job at attempt 12. Settling
`stream_deferred` as a retry could therefore park a healthy job: on cc2ab4bb, twelve low-time invocations
produced two dead letters.

The search-jobs loop now checks, before every ordinary claim (`claim_search_jobs_v1`) and before the probe
claim (`claim_search_probe_v1`), whether the invocation's stream slot is used or less than
`STREAM_MIN_START_MS` is left before its stream deadline (`searchJobsStreamExhausted`). If either holds, it
stops claiming. The run logs `state: stream_exhausted`.

- **Jobs not claimed.** They keep their attempts, lease and contention state exactly as they were. The SQL
  and its lease and epoch boundaries are unchanged.
- **Jobs deferred after their claim.** This happens when the start minimum is crossed between the check and
  the pass. Such a job settles `busy`, which 062300 settles without spending an attempt (`searchSettlementFor`
  maps `stream_deferred` with the scope-lock codes).
- **Throughput cost.** Once the slot is used, the rest of that invocation's jobs wait for the next
  invocation.

### Init check

`SEARCH_INDEX_STREAM_DEADLINE_MARGIN_MS` plus the 120 s start minimum must fit in the 300 s function
timeout (`LAMBDA_FUNCTION_TIMEOUT_MS`, pinned equal to the stack's `FUNCTION_TIMEOUT_SECONDS`). A larger
margin is refused at init, and the error names the variable. The accepted range is 10 000 to 180 000.

### Telemetry carried over

The backport commits are ecb5d981 and 9279264f.

- **Latency fields.** `event_age_ms`, `s3_event_age_ms` and `queue_origin` appear on "pointer indexed",
  "search index reconciled" and "search index partial".
- **Requeue bodies.** These are now `{"reconcile":{"key","requeued":true,"oldest_event_time"?}}`, so
  `s3_event_age_ms` stays end to end across busy, deferred and batch-deadline requeues. The carried time is
  stripped before any stage sees the event.
- **`contender_seats`.** This is one of `all_unseated`, `seated_unseated`, `both` or `catalog_key`. It is set
  on a whole-read pass refused with `relayhistory_acceptance_missing`.
  - **Gap.** Main's streamed reader (`canonical-stream-projection.ts`) does not classify its acceptance
    refusals, so a streamed pass carries no `contender_seats`.
- **Log rules.** The allowlist keeps the ages only as non-negative integers and the words only as their
  closed sets.
- **Goldens.** The off-goldens drop these additive keys, as the backport's golden capture did.

## Acceptance-stamp cold start (salt acceptance-skip-v2)

The cold start is estimated from the code paths only, with no production access.

- **Why it happens.** A stamp written by the backport (salt `acceptance-skip-v1`) never matches on main. So
  after the deploy, the first acceptance-only pass of each stamped scope reads in full and writes a v2
  stamp. Repeats of that scope skip again from then on.
- **How many scopes it can touch.** A scope is affected only if it meets both conditions:
  1. it held a live v1 stamp in the six hours before the deploy (`SKIP_STATE_MAX_AGE_MS`; an older stamp
     already no longer skips on the backport);
  2. it gets another S3 event, or a scheduled reconcile re-enqueue, after the deploy.

  Each such scope costs one full read: the manifests, then a size or fold refusal. That is the same cost
  the backport pays at least every six hours while a scope keeps receiving events. A scope with no new
  events costs nothing.
- **Counting the bound.** Stamp names hash the scope, so they do not reveal orgs. An operator can count
  from logs: distinct (`org_id`, `session_id`) on `search index reconciled` lines whose `reason` is
  `relayhistory_acceptance_missing,skipped_before_read`, over the last six hours before the deploy. Or,
  without org detail:
  `SELECT count() FROM search_ingest_state FINAL WHERE startsWith(name, 'acceptance_skip/') AND value != '' AND updated_at > now() - INTERVAL 6 HOUR`.
- **Scopes whose digest changes anyway.** On main the digest also covers canonical sibling unions and the
  directory epoch. For a scope whose pointers changed at deploy, the digest would differ regardless of the
  salt.
- **The over-bound skip has no cold start.** It reads `search_ingest_session_state`, whose format is the
  same on both lines. One exception: for a scope with canonical siblings, main records the highest revision
  across the siblings. The first pass on main may then run once normally before it skips.

## ClickHouse permission for the acceptance stamp

The stamp needs INSERT on `<database>.search_ingest_state`. Reading the stamp, and `window_start`, needs
SELECT on the same table.

- **Who the Lambda connects as.** It uses the user in the pipeline secret's `clickhouse` block
  (`clickhouse.user`, with `url`, `password` and `database`). It reads that secret from
  `SESSION_PIPELINE_SECRET_ARN` (`parseSecret` and `createClickHouseClient` in `lambda.ts`; the
  `X-ClickHouse-User` header in `vendor/search-index/clickhouse.ts`).
- **Where the secret comes from.** The stack creates the secret container only
  (`stacks/aws/session-pipeline.ts`, "session-pipeline-secret", "Supabase HoopDb URL and ClickHouse ingest
  credentials"). Its value is set by the lead from 1Password.
- **No grant in this repository.** No ClickHouse user, role or `GRANT` for `search_ingest_state` is defined
  anywhere in this repository. The grant is an operator-side ClickHouse admin step. The checks to run first
  (`SHOW GRANTS` and `system.grants`) are listed under the acceptance flag above.
- **Production status.** The backport runs with this flag on, so the production user very likely already
  has the grant. This is not verified here.
- **Without the grant.** Each stamp write logs `acceptance_stamp_write_failed`, nothing is stamped, and
  every pass reads in full. Results are unaffected.
- **Other writers on main.** The buffered search feed (default off) also writes `search_ingest_state` with
  the same client: its refusal ledger `buffered-tie:<token>`, and the buffered erasure cursor.
- **Access is not changed by this work.**
