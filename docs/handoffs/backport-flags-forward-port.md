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

## SEARCH_INDEX_ACCEPTANCE_WATERMARK_ENABLED

Not a backport flag: it is new here, and it extends the acceptance skip above. Default off.

### Grammar

| Value | Result |
| --- | --- |
| unset, `""`, `false` | Off. The acceptance skip runs exactly as described above. |
| `true` | On. Needs `SEARCH_INDEX_SKIP_ACCEPTANCE_MISSING_ENABLED=true`. |
| anything else | Init fails and names the variable. |

### Why

The acceptance skip matches only an unchanged pointer set. A live session gets a new manifest revision with
every upload, so each revision of a session that is still refused for acceptance reads the whole scope again,
only to refuse again.

### What it does

A stamped full read also stores a watermark in the stamp (`w`). It holds hashes and counts only:

- the record ids of the refused groups, hashed (at most 32). Each one is proved by the fold itself: the
  records carrying that id, alone, make `normalizeRelayhistoryRecords` refuse;
- for every live sibling manifest (its key hashed): how many chunks were read, the manifest's content chain
  over them, and the chain over all but the last;
- a digest of the settings (salt, scope, canonical id, window, limits, stream budgets, reader ruleset);
- the time of the full read.

A later pass whose pointer set differs runs a probe before its transaction, with no lock held. The probe
reads the stamp, the live pointer rows (one short read-only transaction), the stored window, each sibling
manifest, and only the chunks added since the watermark. The refusal is held when all of these are true:

1. The sibling manifest set is the watermark's.
2. Every manifest was read as its pointer row lists it (revision, content sha256, etag) and is scrubbed.
3. Every chunk the refused read covered is still listed unchanged: the content chain over the first `n`
   chunks is the stored one. If only the last of them changed (a writer that re-cuts its tail chunk), the
   read starts at that chunk instead.
4. The scope is still within the whole-read bounds, and the added chunks are within the probe's bounds
   (64 chunks, 32 MiB).
5. At least one refused record id has no record in the added chunks, in any sibling manifest, from any origin.
6. Less than `SKIP_STATE_MAX_AGE_MS` (six hours) has passed since the full read. A hold never moves that
   time, so a scope that stays refused is still read in full every six hours.

The locked pass then runs every branch it always runs first (canonical mismatch, alias still live, erase,
skip-current, over-bound). It returns the held outcome only if the pointer rows, the epoch and the canonical
id it reads under the locks are exactly the probe's. This check uses SQL facts only. The result is the
acceptance skip's (`complete: false`, `skippedBeforeRead`, reasons `relayhistory_acceptance_missing,
skipped_before_read`) plus `acceptanceHold`; the log line carries `acceptance_hold: "watermark"` and
`delta_chunks`. After the transaction commits, the stamp is advanced to the new pointer set, written at the
probe's time. A repeat at that revision is then the plain skip, and the next revision's probe reads only what
was added after it.

Anything else runs the normal pass: no watermark, an error of any kind (fail open), or any condition above
not met.

### A manifest replaced under an unmoved pointer row

The pointer upsert takes only a higher revision. A manifest whose content is replaced at the same revision
leaves its pointer row as it was, so the pointer set, and with it the stamp's digest, still match.

- **The acceptance skip alone (unchanged, and what the backport does):** it returns before any manifest read.
  The new content is not indexed until a pointer row moves or the stamp is six hours old. This is the
  "writers must raise the revision" condition above. It is a known gap of that flag.
- **With the watermark flag on:** when the digest matches a stamp that carries a watermark, the probe first
  reads each sibling manifest (no chunk) and compares its chunk count and content chain with the stamped ones.
  If any differs, the stamp licenses nothing for that pass and it reads in full. A stamp without a watermark
  (a catalog-key refusal, or more than 64 siblings) keeps the acceptance skip's behaviour.

The test "a manifest replaced with accepted content at the SAME revision, pointer rows unmoved" runs the three
configurations from the same state and compares each index with a fresh evaluation.

### Why a hold is correct

The pass indexes nothing while the cross-origin fold refuses one record id group, and a group is built only
from the scope's records that carry its record id. The scope's records are the chunks of its live sibling
manifests. Conditions 1, 3 and 5 together mean the group's input is identical, so the fold refuses again.

A seat (`accepted_seq`), an acceptance repair, a new revision, a tombstone and a new contender can each
change a group only as a record with that record id. Such a record is either in an added chunk (condition 5
ends the hold) or it replaced an earlier chunk (condition 3 fails) or it came with a new manifest (condition 1
fails). This does not depend on how any writer publishes: no writer path is assumed.

### Locks

The probe's object and ClickHouse reads run before the search transaction. The stamp write runs after it
commits. The transaction itself gains no read: one in-memory digest comparison. The acceptance skip's own
stamp and window reads stay where they were.

### Costs with the flag on

- Every pass: one more stamp read, outside the lock. When the stamp carries a watermark: one short read-only
  transaction and one window read as well.
- A pass the plain skip takes (same pointer set): one manifest read per sibling as well, no chunk.
- A held pass: one manifest read per sibling, the added chunks, one stamp insert.
- A probe that does not hold costs what it read, and the normal pass follows.

### Known limits

- A refusal that comes only from the catalog-key fold has no record id group and is stamped without a
  watermark.
- A scope with more than 64 sibling manifests is stamped without a watermark.
- When every kept record id is touched by the added chunks (for example the contended record is rewritten
  on every upload), the pass reads in full each time, as without the flag.
- Stamp writes are ordered by time. A stamp advanced after the lock is released is written at the probe's
  time, so a pass that ran in between stays the latest row unless the two runtimes' clocks disagree by more
  than the gap. A stale stamp cannot cause a wrong skip or hold: both re-check the current pointer rows and
  manifest content. It is hashes and counts only.

### Rollout: stamps written before the flag

A stamp written before the flag was on has no watermark. Turning the flag on does not read anything by itself.

- While such a session's pointer set is unchanged, its deliveries keep taking the acceptance skip on the plain
  stamp. Nothing is read, no watermark is written, and the manifest check above does not apply.
- It gets a watermark on its next eligible delivery after a pointer change or after the plain stamp expires: that
  pass reads in full, and writes a watermark if it ends with the acceptance refusal alone, read as listed, with
  a record id group. Holds can start from the revision after that.
- Six hours bounds how long a stamp is valid. Expiry does not schedule a read: a session that gets no delivery
  is never converted, and costs nothing.

To measure uptake, count three things separately on the `search index reconciled` line:

| What | How it logs |
| --- | --- |
| Full-read refusal | `reason: relayhistory_acceptance_missing`, no `skipped_before_read` |
| Same-pointer skip | reason ends `skipped_before_read`, with `acceptance_stamp: plain` or `acceptance_stamp: watermark` |
| Hold across a new revision | `acceptance_hold: watermark`, with `delta_chunks` |

`acceptance_stamp` is logged only with the watermark flag on.

### Rollback

Unset the flag, or set it to `false`. The acceptance skip ignores the `w` field, so stamps written with the
flag on keep working as plain stamps.

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
  - The reserve is also rechecked after each pre-statement lock check, immediately before the statement runs.
  - After the last delete statement and before the directory and catalog writes, the reserve is checked,
    then the lock, then the reserve again. The lock check can itself take time, so the second reserve check
    closes that gap.
  - A single key whose literal could exceed the statement's byte budget is refused, by the same upper-bound
    estimate. Nothing is deleted and the pass is owed (`stream_delete_key_too_large`).
- **Lock-checking keepalive.** The keepalive checks lock continuity, not re-acquisition. The scope's
  transaction advisory lock must already be granted to this backend (`pg_locks`: `locktype = 'advisory'`,
  `granted`, `pid = pg_backend_pid()`, the key in `classid`/`objid` with `objsubid = 1`;
  `STREAM_LOCK_HELD_SQL`). A try-lock could take a lock that had become free, which would prove nothing.
  Any answer but `ok = true` throws `stream_keepalive_failed`, which is retryable. After
  that no row is inserted or deleted, no directory or catalog write happens and no final state row is
  written. The message fails and is redelivered.
- **`needs_full = 1` before any read, cleared only after COMMIT, with per-scope monotonic ownership
  (`HostedState`).** This covers every key `recordState` writes: the scope's session id and its canonical id.
  - **Marker.** Under the pass's scope and canonical locks, before any source read (including manifest
    admission), the pass writes a marker on every key. It is written at M = max(pass time - 1 ms, every key's
    latest `synced_at` + 2 ms), with `needs_full = 1` and a random per-pass token in `scanned_rows`. The
    pass's other incomplete rows go at M + 1, and its completing rows at M + 1.
  - **Ordering does not depend on wall time across invocations.** A later marker is computed from the rows
    already there, so it is at least M + 2 and always sorts above this pass's rows. That holds for a later
    pass in the same millisecond, under clock skew, and for one injected between this pass's fence read and
    its write.
  - **Completing rows wait for COMMIT.** `finalize` runs in a transaction of its own. It takes the scope and
    canonical try-locks, the same keys a pass takes first, reads every key's latest row, and clears only if
    each row is exactly this pass's marker: same time, `needs_full = 1`, same token, same revision.
  - **Serialized.** A later pass needs those locks to write its marker, so the fence read and the completing
    write cannot interleave with it.
  - **Validation.** Every key's latest row is read and validated before any decision (`validStateRow`). The
    row must have exactly four columns (`revision`, `needs_full`, `scanned_rows`, `synced_ms`), each in its
    domain:
    - `needs_full` is 0 or 1;
    - `synced_ms`, `scanned_rows` and `revision` are non-null, non-negative integers.
  - **Outcomes other than a clear:**

    | Case | Effect |
    | --- | --- |
    | Every key shows the same valid successor row, later than this pass's marker | That successor owns the outcome. Logged as `completion_superseded`; the message is acknowledged. |
    | Either lock held by another session | Logged as `completion_lock_contended`. The holder may roll back without writing, so contention proves nothing. |
    | A missing, malformed or foreign row on any key, or successors that differ between keys | Logged as `completion_marker_missing`, `_malformed`, `_foreign` or `completion_successor_divergent`. |
    | An unreadable fence | Logged as `completion_fence_unreadable`. |
    | A rollback, or a COMMIT error with an unknown outcome | Nothing is written. |

    Every case except a proven successor and a rollback fails the pass retryably with
    `search_completion_unrecorded`, so SQS redelivers the message and M2 retries the job. In every case the
    scope stays owed, and the next pass is full.
  - **Erase.** An erase pass is marked and fenced the same way.
- **Strict keyset pages.** Every keyset page of the ClickHouse scan must meet three conditions before
  anything is projected or written:
  - it holds at most the rows asked for;
  - its keys are in strictly increasing order of (ts, session_id, machine_id, kind, event_id), with strings
    compared as UTF-8 bytes;
  - it comes after the previous page's last key.

  Otherwise the pass stops with `clickhouse_page_invalid`. No projection row is inserted or deleted.
  Intentionally, the pass still writes the owed marker (`needs_full = 1`) and invalidates the directory
  (`event_projection_ready = false`), so the next pass is a full one.
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
  - **Gap, made explicit.** Main's streamed reader (`canonical-stream-projection.ts`) does not classify its
    acceptance refusals. A streamed pass refused for acceptance therefore carries `contender_seats: "unknown"`
    rather than omitting the field.
- **Log rules.** The allowlist keeps the ages only as non-negative integers and the words only as their
  closed sets.
- **Goldens.** The off-goldens strip exactly these keys, as the backport's golden capture did:
  - log lines: `event_age_ms`, `s3_event_age_ms`, `queue_origin` and `contender_seats`;
  - results: `contenderSeats`.

  `ts`, `duration_ms` and the search phase timers were already stripped at 9d1adf84.
  `search-index-forward-port-off-golden` asserts, with flags off, that these keys are the only difference
  from the 9d1adf84 golden. In its scenarios only `queue_origin` and `contender_seats` appear: reconcile
  bodies carry no event or send times, so the two age fields are absent there.

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
