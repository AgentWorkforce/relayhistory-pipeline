# Indexer batch deadline

Fixes for the session-pipeline Lambda (`src/session-pipeline/lambda.ts`), each opt-in. Code,
tests and docs only. Nothing here deploys anything, sets any variable or stack input, or changes
anything under `supabase/`. With every knob unset the function's env object, consumer deps, pool
options and rendered stack variables equal those of the commit before these changes (comparator
tests in `src/session-pipeline/__tests__/batch-deadline.test.ts` and
`stacks/aws/__tests__/session-pipeline.test.ts`).

## Knob grammar

Every millisecond knob below is read once at Lambda init by `msKnobFromEnv`
(`src/session-pipeline/lambda.ts`):

| value | effect |
| --- | --- |
| unset, `""` | off (the default) |
| a decimal integer from `1000` to `299000` (milliseconds) | on |
| anything else, including `0`, `-1`, `1.5`, `1e5`, `090000`, ` 90000`, `+90000`, `300000` | init fails |

A refused value fails Lambda init for the whole function, every stage, as the neighbouring
knobs do. The error names the variable, never the value. The upper bound is one second under
the function timeout (`FUNCTION_TIMEOUT_SECONDS` in `stacks/aws/session-pipeline.ts`). The stack
inputs take the same bounds (`MS_INPUT_MIN`, `MS_INPUT_MAX`) and refuse unknown keys. A variable
is rendered only when its input is present.

## Batch deadline (search loop)

`SEARCH_INDEX_BATCH_DEADLINE_MARGIN_MS`. Setting it needs `SEARCH_INDEX_ENABLED=1`; set without
it, init fails. The stack renders it only from
`transcriptStore.sessionPipeline.searchBatchDeadline: { marginMs }`, which needs
`searchIndex: true`.

Each SQS invocation with a Lambda context and a finite remaining time gets a batch deadline at
the remaining time less the margin (`invocationDeps`). In the search loop (`processBatch` in
`src/session-pipeline/consumer.ts`), before each scope's pass starts, a scope whose deadline has
passed is not started. It is requeued (log reason `batch_deadline`) and its message ids are not
failures. A pass already running is never interrupted, so **the margin must exceed the longest
single search pass**. Otherwise a pass started just before the deadline can still run into the
function timeout. Scopes finished before the deadline are reported as before. If the requeue is
missing or throws, that scope's message ids go to the batch failures. The pointer loop is not
changed.

The margin must also leave room for the pointer stage. The deadline is fixed when the invocation
starts, and the pointer loop runs before the search loop. If the remaining time less the margin
has already passed when the search loop starts, no pass runs in that invocation, and a scope at
the deferral cap (below) is failed without a pass. In particular, a margin near the upper bound
(`299000`) leaves no start window at all: every scope is deferred, then failed at the cap, and
reaches the dead-letter queue without ever being indexed.

### Requeue body and deferral rule

Main's requeue body is `{"reconcile":{"key":"…"}}` (`requeueBody` in
`src/session-pipeline/s3-events.ts`, sent by `sqsRequeue` in `lambda.ts` with the existing 60 s
delay). A batch-deadline requeue adds one field, `batch_deadline_deferrals`: the scope's count
plus one, the count being the largest over the scope's messages. Every other requeue writes the
body exactly as before.

A scope whose count has reached 3 (`BATCH_DEADLINE_MAX_DEFERRALS`) runs before the other scopes
of its batch (a stable sort, done only while the deadline is armed). If such a scope still
cannot start before the deadline, it is not requeued but failed (log reason
`batch_deadline_deferrals_exhausted`, level error), so it falls back to SQS redelivery and,
eventually, the dead-letter queue.

The count is read as 0 when absent, negative, fractional, not a number or not a safe integer. The
consumer strips it from the event before the pointer stage sees it. The parser picks named fields
only, so:

- a build without this change ignores the field (it reads the key, etag and version);
- this build reads a body without the field as count 0;
- a body carrying fields this build does not read (for example `oldest_event_time` and
  `requeued`, written by another lineage) parses as its key, etag and version, plus its count.

The self-host consumer (`consumer-service.ts`) never sets a batch deadline. Its requeue takes
the key alone and so ignores the count argument.

### What the cap bounds

- Only batch-deadline requeues carry the count. A requeue for any other reason carries none, so
  the next delivery reads 0 and the count starts again. Those requeues are: the scope busy, a
  stream deferred (`streamSlot`), the buffered stage pending, and pointer liveness after a
  pre-read. The cap therefore bounds only consecutive batch-deadline deferrals, not how often a
  scope is requeued in total.
- A failed scope at the cap goes back to SQS redelivery. SQS then counts receives toward the
  queue's `maxReceiveCount` as for any other failure.

### With streaming armed

On main the stream deadline (`SEARCH_INDEX_STREAMING_CH=1`, margin
`SEARCH_INDEX_STREAM_DEADLINE_MARGIN_MS`, default 60000, configurable from 10000 to 240000) is a
different kind of deadline. It never stops the loop or requeues anything. A pass that streams
checks it between chunks, between index scan pages, before the regenerate and before any delete.
Once it is crossed, that scope stops there and is recorded partial (`scope_too_large`,
`stream_deadline`). A pass that does not stream never reads it. `streamSlot` lets one scope per
invocation stream; a later scope that would stream returns `deferred` and is requeued
(`stream_deferred`).

The two deadlines are independent. `invocationDeps` computes both from the same remaining time,
each less its own margin, and leaves the stream deadline, the stream slot and their log reasons
exactly as without the batch deadline. The batch deadline is checked before a pass starts; the
stream deadline is checked inside a streamed pass. So:

- batch margin larger than the stream margin: the loop stops starting passes before streamed
  reads would be cut short;
- batch margin smaller or equal: a streamed pass may stop at its stream deadline (partial) while
  passes that do not stream still start until the batch deadline.

New log field: `batch_deadline_deferrals`, a non-negative integer bounded at 1000
(`BOUNDED_COUNT_FIELDS` in `src/shared/log.ts`). Any other value is dropped.

## Object read deadline

`SESSION_PIPELINE_OBJECT_READ_TIMEOUT_MS`. It does not need the search stage. The stack renders
it only from `transcriptStore.sessionPipeline.objectReadTimeout: { ms }`.

When set, the function's bucket clients (`pipelineStore` in `lambda.ts`, which passes
`readTimeoutMs` to `createS3Store`) give every HEAD and GET one deadline that covers the headers
and the whole body (`send` in `packages/object-store/src/core.ts`). A read that crosses it fails
as a transient `ObjectStoreError` with `transportCause: "timeout"`. A caller's own abort signal
still works (both are combined). Responses without a body (204, 205, 304) are returned as they
are. PUT, list and delete get no deadline. Unset, the client sends no signal and returns the
fetch's own body stream, as before.

Which clients carry it (all built by `pipelineStore`):

- the runtime store (`runtimeFromSecret`). On main this one client is shared by the pointer
  stage (manifest reader and `exists` head), the search pass, the buffered feed, replay, the
  brief tick, the analysis tick and the analysis cache, so the deadline applies to the reads of
  every one of those stages, not only the search pass;
- the scheduled reconcile's store (`runReconcile`; list and head).

Not covered: the S3 eraser of the analysis deps (deletes only), the SQS / Secrets Manager /
invoke calls, and the bucket clients of the other functions (read API, ingest, analysis runner,
self-host Locker).

How a timed-out read reaches the stages: the manifest reader (`objectStoreManifestReader`)
returns null only for not found, so a timeout is rethrown, not read as a missing manifest. A raw
chunk read (`readManifestRecords` / `streamManifestRecords` in
`src/session-read-api/canonical-projection.ts`) turns it into the retryable `chunk_unavailable`,
which the search pass rethrows; the message then goes back to SQS like any other retryable
failure. The deadline bounds one request; it does not bound a pass that makes many reads, which
is what the batch deadline is for.

## Main-pool timeouts

`HOOPDB_STATEMENT_TIMEOUT_MS`, `HOOPDB_LOCK_TIMEOUT_MS`, `HOOPDB_ACQUIRE_TIMEOUT_MS`, each
independent. The stack renders only the keys set in
`transcriptStore.sessionPipeline.dbTimeouts: { statementTimeoutMs?, lockTimeoutMs?, acquireTimeoutMs? }`
(at least one key; unknown keys refused).

They apply to the function's main pool only (`runtimeFromSecret` passes them to `supabasePool`,
whose options come from `supabasePoolOptions` in `lambda.ts`). TLS, the connection string, the
other options and the other callers of `supabasePool` are unchanged, and `refuseEnvOptions` still
runs first: the pool never sets node-pg's `options`, so a set `PGOPTIONS` is still refused.

- `statementTimeoutMs`, `lockTimeoutMs` become node-pg's `statement_timeout` and `lock_timeout`
  client options, which it sends as startup parameters when each connection opens. They are the
  connection's defaults; a `SET LOCAL` inside a transaction wins for that transaction:

  | where | statement_timeout | lock_timeout |
  | --- | --- | --- |
  | `pool.query` outside a transaction (pointer reads and writes, fan-out, reconcile) | the default | the default |
  | `transaction()` (search transaction, every other caller) | 60 s (its own `SET LOCAL`) | the default |
  | scope lock (`lock` in `runtimeFromSecret`) | 60 s | 30 s (its own) |
  | publication transaction (`publication.transact`) | 5 s (its own) | 1 s (its own) |

  So inside a transaction the statement default never applies (every `transaction()` sets 60 s),
  and the lock default applies only where no `SET LOCAL lock_timeout` is sent.
- `acquireTimeoutMs` is pg-pool's `connectionTimeoutMillis`, enforced in this process: a
  checkout waiting for a free client, or for a new connection to open, fails after it with
  `timeout exceeded when trying to connect`.

**Poolers that drop startup parameters.** The code elsewhere notes that Supavisor drops startup
options (`refuseEnvOptions`, `analysis-snapshot.ts`). If the main pool's URL goes through such a
pooler, the statement and lock defaults are silently not applied; only `acquireTimeoutMs` (local
to the process) still works. Before relying on them, read `SHOW statement_timeout` and
`SHOW lock_timeout` on a connection from the real pool, through the real endpoint.

Not covered (their own pools or clients, unchanged): the buffered-feed pool (its own login
timeout profile), the analysis-cache read-only pool (`analysis-snapshot.ts`, its own `SET LOCAL`),
the self-host consumer and its bootstrap, the output-facts bootstrap, the large-read main, the
repair and restamp jobs, and the analysis runner.

## Guarded transactions

No knob: this one is always on for two transactions of the Lambda, and changes behaviour only
when a connection drops.

pg-pool removes a client's idle `error` listener when the client is checked out. If the
connection then ends between two statements (no query in flight), pg emits `error` on the
client with no listener, which crashes the process: every message of the batch is redelivered,
not only the one whose connection dropped. The connection's advisory transaction locks go with
it, so work that continues after the drop no longer holds the lock it was started under.

`transaction()` (`src/session-pipeline/lambda.ts`) takes a third argument, `guarded`. Guarded,
it adds an `error` listener to the checked-out client for that checkout only, and records the
error. `ensureLive()`, which it hands to its work, throws the recorded error; so does every later
statement on the transaction (no COMMIT or ROLLBACK is sent on a broken client). The client is
then destroyed (`release(err)`), not returned to the pool, and the listener is removed before
release, so the next checkout of a reused client starts with one listener, not two. The error
fails that key or scope through the consumer like any other failure. With no `error` emitted,
a guarded transaction sends the same statements and releases the client the same way
(`release()`). Unguarded (the default), no listener is added and `ensureLive` never throws.

### What is guarded

Every `transaction()` caller and every holder of a pooled client, on main:

| caller | guarded |
| --- | --- |
| scope lock (`scopeLock` in `lambda.ts`, the pointer stage's `lock`) | yes |
| search transaction (`searchTransaction` in `lambda.ts`, the search stage's `transact`) | yes |
| publication transaction (`publication.transact` in `runtimeFromSecret`) | no (runs inside the scope lock; its own `SET LOCAL` 5 s statement / 1 s lock timeouts, unchanged) |
| analysis deps `transact` (`runtimeFromSecret`) | no |
| self-host consumer (`consumer-service.ts`, its lock and search `transact`) | no |
| large-read main (`large-read-main.ts`) | no |
| analysis runner (`src/session-analysis/runner.ts`) | no |
| move transaction (`moveTransaction` in `src/hoopdb/index.ts`) | no own listener; checked (below). Unreachable from the Lambda, which always sets `publication` |
| other `pool.connect()` holders (buffered-feed login, analysis snapshot, brief claim, bootstraps, repair and restamp jobs, read API, migrate, eraser, smoke) | no |

The unguarded callers that pass a work function now wrap it (`(tx) => work(tx)`), so their work
receives exactly the argument it received before.

### Checks under the lock

`ScopeLock` (`src/session-pipeline/pointer.ts`) passes `ensureLive` to its work as an optional
argument; a lock that passes none (the in-memory test lock, the self-host consumer's lock) runs
the work exactly as before. The pointer stage threads it, per call (never stored on the shared
index object), into `upsertRawPointer` (`UpsertOptions.ensureLive` in `src/hoopdb/index.ts`). With
it, a check runs:

- before each main-pool statement the pointer work sends: the pre-read stamp re-read and the
  stored-row read (the pointer stage's own, through `checkedDb` in `src/hoopdb/index.ts`), and
  the upsert's own reads and writes, the tombstone and the rekey (each `upsertRawPointer` call
  wraps its executor, its transactions and its queue for that call only);
- before the publication transaction starts, before each statement the upsert sends in it and
  before its COMMIT (its own BEGIN and `SET LOCAL` lines: see below);
- before the move transaction's BEGIN, each of its statements and its COMMIT;
- before each fan-out queue send and before the brief wakeup is armed.

Nothing else on the pointer path is checked; the list below says what is not.

The search pass (`indexSearchScopeBase` in `src/session-pipeline/search-index.ts`) wraps its index
calls (`io`: scope reads and scan pages, upserts, removes, erase, record) and its ClickHouse facts
calls (`chFacts`) with `checkedCalls`: the check runs before each call. Its statements on the
transaction itself are checked by `transaction()`. On main the in-transaction streamed reconcile
(`streamedReconcile`) receives the same checked `io`, so its scans, insert batches, removes and
its `owing()` record go through a check per call; its `owing()` directory invalidation runs on the
transaction. It writes no facts state (it never runs with output facts).

### Not covered

- A statement already in flight when the connection drops: it completes or fails on its own.
- A drop pg has not reported (no `error` emitted yet): the check passes.
- Work inside one checked call: the pages of one index scan call, and the delete batches inside
  the vendored `deleteRows` (one `remove` call), run without a check between them.
- Object-store reads on the pointer path are not checked or interrupted: the manifest read, the
  message enrichment reads, the moved-sibling HEAD probe and, on a move, the read of the other
  key. With `SESSION_PIPELINE_OBJECT_READ_TIMEOUT_MS` unset they have no deadline at all.
- Unchecked ClickHouse reads in the transaction: the skip check's output-facts completeness read
  (`alreadyCurrent` with output facts, `outputFactsComplete` on the raw client).
- The streamed reconcile's own source reads (raw chunks from the object store, between its
  checked calls) and its keepalive between them (a transaction statement, so `transaction()`
  checks it). An insert batch already sent runs to its end; a drop that pg has not reported yet
  is noticed at the next keepalive or checked call.
- The pre-read capture's index scan (`captureSearchPreRead`, outside the guarded transaction) and
  the staged run (outside the transaction, in its own batches).
- The move transaction's own ROLLBACK after a failed check is still sent (cleanup on its own
  connection).
- Inside the publication transaction, its own BEGIN and its two `SET LOCAL` timeouts run right
  after the check that precedes the transaction, without a check of their own.
- The manifest reads and the moved-sibling HEAD requests are object-store calls, not statements.
- Timeouts: the checks neither shorten nor extend any timeout (the lock's 30 s `lock_timeout`,
  the 60 s statement timeout, the publication transaction's 5 s / 1 s). A pass still running when
  the function times out is cut by the timeout, not by a check. A batch-deadline margin near the
  upper bound (`299000`) still leaves no start window (above); a margin that is too small still
  lets a pass run into the function timeout.
- Every unguarded caller in the table above.
