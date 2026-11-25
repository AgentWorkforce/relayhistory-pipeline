# Search-queue split: pointer work never waits behind search

Default off. With `SESSION_PIPELINE_SEARCH_QUEUE_URL` unset (or empty) the pipeline
behaves and logs exactly as before. Nothing here deploys anything.

## What it does

Today one queue and one event source mapping carry both stages: a batch runs the
pointer stage, then the search stage inline (`src/session-pipeline/consumer.ts`
processBatch). Long search passes hold mapping slots, so new sessions wait for them.

With the split armed:

- **Pointer queue** (the existing events queue): the pointer stage runs as before. Then,
  instead of the search stage, each scope gets one SendMessage to the search queue, with the
  same body and signing as the busy requeue (`requeueBody`, `sqsRequeue`) and no delay. One
  info line per handoff: `search index handed off`, `stage: search-index`,
  `reason: search_handoff`, `org_id`, `session_id`. A failed send logs an error and fails
  that scope's messages, so SQS redelivers them (the pointer stage reruns; it is idempotent).
- **Search queue**: records are recognised by their `eventSourceARN` (same owner and exact
  queue name as the configured URL). The ARN is set by the event source mapping, so no
  message body can route itself past the pointer stage. These records skip the pointer stage:
  the scope is rebuilt read-only from the manifest key (`pointer.ts resolveScope`: key parse,
  org and prefix checks; no lock, no S3 read, no write), then the search stage runs exactly as
  the inline loop did. Busy, deferred and past-the-stream-deadline passes requeue to the
  **search** queue (60 s delay). Failures fail only the messages behind that scope.
- The `pointer_move_pending` fast retry (`SESSION_PIPELINE_RETRY_VISIBILITY_S`) and its
  deadline recheck are unchanged; they only ever apply to pointer-stage failures, which only
  happen on the pointer queue.
- Telemetry: a handed-off pass carries the oldest upload time (`s3_event_age_ms` stays end
  to end) and reports `queue_origin: requeue`.

## Flags

Function env:

| variable | value | effect |
| --- | --- | --- |
| `SESSION_PIPELINE_SEARCH_QUEUE_URL` | unset or empty | off (as before) |
| | `https://<host>/<owner>/<queue-name>` | armed; needs `SEARCH_INDEX_ENABLED=1`. Anything else fails init naming the variable, never the value. |

Stage inputs (`transcriptStore.sessionPipeline.searchQueue`, `stacks/aws/session-pipeline.ts`),
all off by default:

```json
"sessionPipeline": {
  "searchIndex": true,
  "searchQueue": { "enabled": true, "mappingEnabled": false, "handoff": false, "maxConcurrency": 2 }
}
```

- `enabled`: create the search queue `<prefix>-session-search`, its DLQ
  `<prefix>-session-search-dlq` (14 days), and a second event source mapping on the same
  function. The queue keeps the events queue's visibility timeout, retention, long poll,
  SSE and redrive (`maxReceiveCount` 8), with no delivery delay. Its policy allows only the
  function role ReceiveMessage, DeleteMessage, ChangeMessageVisibility, GetQueueAttributes and
  SendMessage, TLS only (the role has no identity policy; grants are resource-based).
- `mappingEnabled`: the search mapping polls (default false: created disabled).
- `handoff`: render `SESSION_PIPELINE_SEARCH_QUEUE_URL` (needs `enabled` and `searchIndex`).
- `maxConcurrency`: the search mapping's MaximumConcurrency (2-10, default 2).

The deploy plan contract judges the three new resources with the pipeline's own phases:
creating them is a `session pipeline rollout`, updating them a `session pipeline update`.
Deleting or replacing them is refused, as for every pipeline resource.

## Activation order

1. Create the queue, DLQ and mapping disabled: `searchQueue: { enabled: true }`.
2. Deploy this code with the env still unset (behaviour unchanged).
3. Set the env: `handoff: true`. The pointer path now hands off; passes wait in the search queue.
4. Enable the search mapping: `mappingEnabled: true`.
5. Then lower the pointer mapping's MaximumConcurrency, keeping the pool budget below.

## Rollback

Unset the env (`handoff: false`, or remove the variable). Search runs inline on the pointer
path again at once. Leave the search mapping enabled: whatever is still in the search queue
drains through it. With the env unset those records are no longer recognised, so they take
the ordinary path (pointer stage, which is idempotent, then search inline). Disable the search
mapping only once the queue is empty.

## Pool budget

Every concurrent invocation opens up to `HOOPDB_POOL_MAX` Supabase connections. Keep

    (pointer MaximumConcurrency + search MaximumConcurrency) x HOOPDB_POOL_MAX <= the pooler limit

including any other client of the same pooler. Raise one mapping only after lowering the other.

## Not covered

- No DLQ or age alarm is created for the search queue yet.
- A role replacement while the search queue exists updates its policy, which the role-ARN
  cascade check does not list, so such a plan is refused and needs its own review.
