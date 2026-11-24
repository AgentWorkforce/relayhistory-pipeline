# Indexer batch deadline

Branch `review/layered-sessions-pipeline-indexer-batch-deadline-246a`. Code, tests and
docs only. Nothing here deploys anything, and nothing enables the knob.

## The knob

`SEARCH_INDEX_BATCH_DEADLINE_MARGIN_MS`, read once at Lambda init (`lambdaEnv` in
`src/session-pipeline/lambda.ts`):

| value | effect |
| --- | --- |
| unset, `""` | off (the default): the env object and the consumer deps are exactly as without the knob |
| a decimal integer from `1000` to `299000` (milliseconds) | on; needs `SEARCH_INDEX_ENABLED=1` |
| anything else, including `0`, `-1`, `1.5`, `1e5`, `090000`, ` 90000`, `300000` | init fails |

A refused value (or a valid one without `SEARCH_INDEX_ENABLED=1`) fails Lambda init for
the whole function, every stage, as the neighbouring knobs do; the error names the
variable, never the value. The upper bound is one second under the function timeout
(`FUNCTION_TIMEOUT_SECONDS` in `stacks/aws/session-pipeline.ts`).

The stack renders it only from the optional input
`transcriptStore.sessionPipeline.searchBatchDeadline: { marginMs }` (same bounds, unknown
keys refused, needs `searchIndex: true`). Absent, nothing new is rendered.

## What it does

Each SQS invocation with a Lambda context gets a batch deadline at the remaining time less
the margin (`invocationDeps`). In the search loop (`processBatch` in
`src/session-pipeline/consumer.ts`), before each scope's pass starts, a scope whose
deadline has passed is not started: it is requeued with its carried upload time (log
reason `batch_deadline`) and its message ids are not failures. A pass already running is
never interrupted, so **the margin must exceed the longest single search pass**, or a
pass started just before the deadline can still run into the function timeout. Scopes
finished before the deadline are reported as before. If the requeue is missing or
throws, that scope's message ids go to the batch failures.

Deferral rule: each batch-deadline requeue carries `batch_deadline_deferrals` (the
scope's count plus one, the count being the largest over the scope's messages) in the
reconcile body, and a scope whose count has reached 3 runs before the other scopes of its
batch. If such a scope still cannot start before the deadline it is not requeued but
failed (log reason `batch_deadline_deferrals_exhausted`, level error), so it falls back to
SQS redelivery and, eventually, the dead-letter queue.

The count is read as 0 when absent, negative, fractional, not a number or not a safe
integer. Only batch-deadline requeues write it; busy, deferred and stream requeues keep
their body. A build that predates the field reads only the named fields of a reconcile
body and ignores it. The pointer loop is not changed. With
`SEARCH_INDEX_STREAMING_CH=1` as well, each deadline applies, the earlier is crossed first,
and the stream deadline's path and reason (`stream_batch_deadline`) are unchanged.

New log field: `batch_deadline_deferrals` (non-negative integer only).

## Object read deadline

`SESSION_PIPELINE_OBJECT_READ_TIMEOUT_MS`, read once at Lambda init (`objectReadTimeoutFromEnv`
in `src/session-pipeline/lambda.ts`):

| value | effect |
| --- | --- |
| unset, `""` | off (the default): no abort signal, responses returned as before |
| a decimal integer from `1000` to `299000` (milliseconds) | on, for every stage (not tied to `SEARCH_INDEX_ENABLED`) |
| anything else | init fails, naming the variable, never the value |

The stack renders it only from `transcriptStore.sessionPipeline.objectReadTimeout: { ms }`
(same bounds, unknown keys refused).

When set, the function's bucket client (`pipelineStore`, used by init and by reconcile)
gives each object GET and HEAD one deadline from the call to the end of the response body
(`send` in `packages/object-store/src/core.ts`, option `readTimeoutMs` on the S3 store).
Before the headers arrive, crossing it aborts the request; during the body, a pending read
errors the body stream. Either way the error is an `ObjectStoreError` with code
`transient` (retryable). Request signing before the call is not covered. Put, delete, list
and presign are unchanged.

A timed-out manifest read is rethrown by the manifest reader (only `not_found` reads as
missing), so the key or scope fails and its messages are redelivered. A timed-out raw chunk
read becomes the retryable `chunk_unavailable` (`readChunkRecords` in
`src/session-pipeline/search-index.ts`), which the search pass rethrows, so the scope fails
the same way rather than being recorded as incomplete.

## Pool timeouts

Three variables, read once at Lambda init (`dbTimeoutsFromEnv` in
`src/session-pipeline/lambda.ts`), each independent:

| variable | pool option | effect when set |
| --- | --- | --- |
| `HOOPDB_STATEMENT_TIMEOUT_MS` | `statement_timeout` | connection default for every statement |
| `HOOPDB_LOCK_TIMEOUT_MS` | `lock_timeout` | connection default for every lock wait |
| `HOOPDB_ACQUIRE_TIMEOUT_MS` | `connectionTimeoutMillis` | how long a checkout waits for a client (queued on a full pool, or connecting) |

Each takes the same grammar: unset or `""` is off (the default; the pool options are exactly
as before), a decimal integer from `1000` to `299000` (milliseconds) is on, anything else fails
init naming the variable, never the value. The stack renders them only from
`transcriptStore.sessionPipeline.dbTimeouts: { statementTimeoutMs?, lockTimeoutMs?,
acquireTimeoutMs? }` (at least one key, same bounds, unknown keys refused).

The two server settings go in the pool options (`supabasePoolOptions`), which pg sends as
startup parameters on every new connection. There is no extra statement per checkout, it
covers `pool.query` as well as `transaction()`, and both settings are ones any role may set,
so nothing needs a new privilege. Inside a transaction the existing `SET LOCAL` still wins:
`transaction()` keeps its own 60 s `statement_timeout`, and the scope lock keeps its 30 s
`lock_timeout`. A transaction without its own `SET LOCAL lock_timeout` (the search
transaction) gets the connection default. A transaction-mode connection pooler in front of
the database may refuse unknown startup parameters; check the connection path before turning
these on.

An acquire timeout rejects the checkout with pg-pool's "timeout exceeded when trying to
connect"; the key or scope fails and its messages are redelivered.

## Guarded transactions

No knob. The scope lock (`scopeLock` in `src/session-pipeline/lambda.ts`, the pointer stage) and
the search transaction (`searchTransaction`, streamed or not) both run `transaction()` in its
guarded mode; before this only the streamed search transaction did.

pg-pool removes its idle `'error'` listener when a client is checked out, and pg emits
`'error'` on the client when the connection ends while no query is in flight. The scope lock
holds its client idle while the pointer work runs on other connections, and an unstreamed search
pass holds its client idle while it reads the bucket and ClickHouse, so such an emit had no
listener and ended the invocation with an uncaught exception. Guarded, a listener added at
checkout keeps the error: the next statement on that client (at the latest `COMMIT`) rejects
with it, the key or scope fails and its messages are redelivered, and the client is released
with the error (destroyed, not returned to the pool). The listener is removed before release.

With no `'error'` emitted the guarded mode sends the same statements and releases the client
the same way (`release()`); the only difference is one listener on the client while it is
checked out.

Writes made while the lock may be gone. The pointer stage takes its lock because the revision
fence alone is not enough: a put whose read raced an erasure could upsert after the delete's
tombstone and bring the row back (`upsert_session_index` lets a newer revision undelete; see
the header of `src/session-pipeline/pointer.ts`). An advisory lock lives on its connection, so
when that connection drops the lock is freed, and another invocation can take it while this one
still runs. The pointer work writes on other connections and the unstreamed search pass writes
to ClickHouse, so without a check the drop would be noticed only at the next statement on the
lock's own client, after those writes. `transaction()` therefore hands its work a synchronous check (`ensureLive`)
that throws the recorded error once one was emitted and sends no statement. The pointer work
calls it immediately before each write it makes under the lock (each pointer upsert and the
re-key delete of a moved session), and the unstreamed pass calls it before each ClickHouse
insert batch, before `deleteRows` and before writing its state row (complete or owing a pass).
Once an error was emitted, that write is not made and the key or scope fails with the error.
The streamed pass is unchanged: its keepalive and forced lock check are statements on the lock's
client.

What the check does not cover:

- a write already in flight when the error is emitted, and the statements inside one pointer
  upsert (`upsertRawPointer` sends several; the check runs once before the first);
- a drop that pg has not reported yet: the client emits nothing until it next uses the
  connection. The streamed pass forces a round trip with its keepalive; the unstreamed pass has
  no such statement between its ClickHouse writes, so such a drop is seen only at its next
  statement on the lock's client (the catalog write of a complete pass, or `COMMIT`);
- the delete batches inside one `deleteRows` call, which are not checked one by one.

In these cases the key or scope still fails (at the latest at `COMMIT`) and its messages are
redelivered, and the next delivery re-reads S3 under a fresh lock and writes from that state.
That repair does not happen if the message reaches the dead-letter queue first, and it does not
undo what a reader already took from the intermediate row.
