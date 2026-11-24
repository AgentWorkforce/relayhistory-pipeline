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
