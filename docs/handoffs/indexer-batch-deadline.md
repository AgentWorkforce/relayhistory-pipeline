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
