# Search-queue split: pointer work never waits behind search

Default off at every step. With `SESSION_PIPELINE_SEARCH_QUEUE_URL` unset (or empty) the pipeline
behaves and logs exactly as on 6d88b638 (a golden recorded on 6d88b638 is asserted in
`src/session-pipeline/__tests__/search-queue-split.test.ts`). Nothing here deploys anything.
There is no fast retry on this base (`SESSION_PIPELINE_RETRY_VISIBILITY_S` does not exist here).

## What it does

Today one queue and one event source mapping carry both stages: a batch runs the pointer stage,
then the search stage inline (`src/session-pipeline/consumer.ts` processBatch). Long search passes
hold mapping slots, so new sessions wait behind them.

With the split armed:

- **Pointer queue** (the existing events queue): the pointer stage runs as before. Then, instead
  of the search stage, each scope gets one SendMessage to the search queue (the requeue body,
  `requeueBody`, no delay). The sends run concurrently, each bounded by
  `min(5 s, remaining - 2 s)` of the invocation (`SEARCH_HANDOFF_*` in consumer.ts). With less
  than 1 s left after the 2 s margin no send starts: those scopes' messages are returned as
  failures at once, the rest of the batch is acknowledged, and one warn line says
  `reason: handoff_deadline` with `count`. A failed or timed-out send fails exactly that scope's
  messages; SQS redelivers them and the pointer stage, which is idempotent, reruns.
- **Search queue**: a record runs search-only when its `eventSourceARN` equals, in full, the ARN
  derived from `SESSION_PIPELINE_SEARCH_QUEUE_URL` (`arn:<partition>:sqs:<region>:<account>:<name>`,
  lambda.ts `sqsQueueArn`). Another region, partition, account or name is not the search queue.
  The ARN is set by the event source mapping, so no message body can route itself past the pointer
  stage. These records skip the pointer stage (no lock, no S3 read, no pointer write): the scope is
  rebuilt read-only from the key (`pointer.ts resolveScope`), then the search stage runs as the
  inline loop did. Busy, deferred and past-the-stream-deadline passes requeue to the **search**
  queue (60 s delay).
- **Startup** (lambda.ts `searchQueueUrlFromEnv`) refuses, before anything is consumed and naming
  only the variable: a URL that is not a regional SQS endpoint in `AWS_REGION` with that region's
  partition suffix, a pointer URL that is not one, a search queue in another account than the
  pointer queue, a search URL naming the pointer queue, and the variable without
  `SEARCH_INDEX_ENABLED=1`. Init failing returns every message of the batch.
- **Record source** (`QueueRecord.queue`, lambda.ts `queueRecords`) is derived from the pointer
  queue's ARN whenever `SESSION_PIPELINE_QUEUE_URL` parses, armed or not: `pointer`, `search`
  (only while armed) or `other`. After a rollback, a leftover search-queue record is therefore
  `other`, never `pointer`: anything that would act on a receipt handle against the pointer queue
  must require `queue === "pointer"`. On this base no receipt handle is passed to the consumer at
  all.

## Flags and inputs

Function env (rendered by the stack only with `searchQueue.handoff`):

| variable | value | effect |
| --- | --- | --- |
| `SESSION_PIPELINE_SEARCH_QUEUE_URL` | unset or empty | off (as before) |
| | `https://sqs.<region>.<suffix>/<account>/<name>` | armed (see Startup) |
| `HOOPDB_POOL_MAX` | `hoopdbPoolMax` (required) | Supabase connections per invocation |

Stage inputs (`transcriptStore.sessionPipeline`, `stacks/aws/session-pipeline.ts`):

```json
"sessionPipeline": {
  "searchIndex": true,
  "maxConcurrency": 10,
  "hoopdbPoolMax": 3,
  "searchQueue": { "enabled": true, "mappingEnabled": true, "handoff": true, "maxConcurrency": 16 }
}
```

- `maxConcurrency`: the events (pointer) mapping, 2-20, default 4. 20 is representable so the
  live value renders unchanged.
- `hoopdbPoolMax`: 1-4, **required whenever `sessionPipeline` is declared** (the render is refused
  without it). The live function's `HOOPDB_POOL_MAX` is 3, set by hand; before this input the stack
  hard-coded 4, so any render that omitted it would put 4 back over the live 3. Requiring it (rather
  than defaulting) is the option chosen; stages that declare no `sessionPipeline` render exactly as
  before. The production stage input must carry `"hoopdbPoolMax": 3`. The rendered env and the
  budget check read this one value.
- `searchQueue.enabled`: creates `<prefix>-session-search`, `<prefix>-session-search-dlq`
  (14 days), the search mapping (created disabled), and two alarms (below). The queue keeps the
  events queue's visibility timeout, retention, long poll, SSE and redrive (`maxReceiveCount` 8),
  with no delivery delay. Its policy allows only the function role ReceiveMessage, DeleteMessage,
  ChangeMessageVisibility, GetQueueAttributes and SendMessage, TLS only.
- `searchQueue.mappingEnabled`: the search mapping polls. Needs `enabled`.
- `searchQueue.handoff`: renders the URL. Needs `mappingEnabled` and `searchIndex: true`.
- `searchQueue.maxConcurrency`: the search mapping's MaximumConcurrency, 2-20, default 2.

## Pool budget

    (pointer MaximumConcurrency + search MaximumConcurrency while polling) x HOOPDB_POOL_MAX <= 80

The budget (`POOLER_BUDGET`) is fixed; there is no input to raise it. The render is refused above
it. Live today: 20 x 3 = 60. Production split values: (10 + 16) x 3 = 78. The same caps at a pool
of 4 are 104 (refused), so the pool is never raised back to 4 while the split is on; enabling
search before lowering the pointer cap is (20 + 16) x 3 = 108 (refused).

An apply moves resources one at a time, and warm instances keep the old pool until replaced, so
any mix of old and new values can coexist: the worst moment is
(max pointer + max search) x max pool (`sessionPoolTransitionDemand`). It stays within the budget
only when each apply changes one capacity setting; the deploy guard enforces that (below).

## Staged rollout and rollback

The full staged render, from the stack's own props with placeholders, is
`docs/handoffs/search-queue-split.staged-render.txt` (regenerate with
`bun scripts/search-split-staged-render.ts > docs/handoffs/search-queue-split.staged-render.txt`;
a stack test fails when it is stale). One capture and apply per stage, each verified before the
next:

| stage | change | guard phase | demand | worst during apply |
| --- | --- | --- | --- | --- |
| live | pointer 20, pool 3 (hand-set) | post-apply | 60 | |
| A | `searchQueue: { enabled: true, maxConcurrency: 16 }`: queue, DLQ, policy, alarms, mapping DISABLED | session search queue rollout | 60 | 60 |
| B | `maxConcurrency: 10` (events mapping) | session pipeline update | 30 | 60 |
| C1 | `mappingEnabled: true` (search mapping) | session pipeline update | 78 | 78 |
| C2 | `handoff: true` (function env: URL) | session pipeline update | 78 | 78 |

C is two captures because the guard refuses a function update together with any mapping change
(the role-ARN cascade rule), and the two mappings together (one capacity setting per apply).
Lowering the pointer cap and enabling search in one apply could reach 108 (from A). Apply C1 only
after B's mapping update has finished and the live mapping reads 10.

Rollback (from C2), each step within the budget; the pool stays 3 throughout:

| stage | change | demand | worst during apply |
| --- | --- | --- | --- |
| R1 | `handoff: false` (URL removed). Search runs inline on the pointer path again at once. | 78 | 78 |
| R2 | `mappingEnabled: false`, once the search queue's visible and in-flight counts are 0 | 30 | 78 |
| R3 | `maxConcurrency: 20` | 60 | 60 |

R1 is the fast rollback. Leave the search mapping enabled after it: records still in the search
queue drain through it. With the URL unset they are `queue: "other"`, take the ordinary path
(pointer stage, idempotent, then search inline; a busy pass is a new pointer-queue message) and
nothing acts on their receipt handles. The search resources stay (disabled) after R3; deleting
them is refused by the guard, as for every pipeline resource.

## Hand-set function variables: the stack renders the full env

This stack renders the pipeline function's **whole** environment. Alchemy's Lambda provider
(`@alchemy.run/aws` `Lambda/Function.ts`) sends `Environment.Variables` as exactly the rendered
env on every create and every `UpdateFunctionConfiguration`, so any function update replaces the
live env and **drops every variable set by hand** that `sessionPipelineEnv` does not render. The
plan does not show this: it diffs against Alchemy state (the last applied props), not the live
function, so a hand-set variable is invisible in the capture.

What `sessionPipelineEnv` renders (and so survives): the fixed ids, `SEARCH_INDEX_ENABLED`,
`SEARCH_INDEX_STREAMING_CH` (`streamReconcile`), `HOOPDB_POOL_MAX` (`hoopdbPoolMax`, now required),
`SESSION_PIPELINE_SEARCH_QUEUE_URL` (`searchQueue.handoff`), and the brief settings. What it does not
render, so a function update would drop it if set by hand: `SEARCH_INDEX_SKIP_OVER_BOUND_ENABLED`,
`SEARCH_INDEX_SKIP_ACCEPTANCE_MISSING_ENABLED`, `SESSION_POINTER_SKIP_ORIGIN_BUCKET_ENABLED`, and any
other variable added by hand.

Consequences for this rollout:

- A, B, C1, R2 and R3 change only queues, mappings and alarms: the function's env is not touched.
- C2 and R1 are function env updates. Before either is captured, read the live env back and
  compare it with the rendered env; every hand-set variable must first become a stack input (or
  the capture is not applied). This branch does not add inputs for the `SEARCH_INDEX_SKIP_*` or
  `SESSION_POINTER_SKIP_*` flags.
- Alchemy state still holds `HOOPDB_POOL_MAX: "4"` from the last apply if the 3 was set only by
  hand. Then the first capture with `hoopdbPoolMax: 3` shows `session-pipeline` updating
  `HOOPDB_POOL_MAX` "4" -> "3", and stage A's provisioning-only guard refuses it. That state
  reconcile is its own capture before A (a session pipeline update: env only, live stays 3,
  60 connections throughout), and it is a full env write, so the previous point applies to it too.

## Deploy guard (stacks/aws/transcript-store.ts)

- **session search queue rollout** (stage A): the search resources are created (all, or the rest
  after a partial apply) and every other resource is noop. Each created resource's detailed block
  must be exactly its rendered props (tags aside): the DLQ; the queue, whose policy names only the
  pipeline role (the state snapshot's role, else one role in the production account) and whose
  DLQ is the search DLQ; the mapping on the search queue for the pipeline function, with batch 10,
  partial batch responses, MaximumConcurrency 2-20 and `enabled: false`; the two alarms with
  `ActionsEnabled: false`.
- In place: the DLQ and the alarms never update. The queue updates only by the role-ARN cascade of
  a pipeline update (it is checked line by line with the other grants). The search mapping may
  change only `enabled` and `MaximumConcurrency`, and never in the same plan as the events
  mapping; `HOOPDB_POOL_MAX` never changes in the same plan as either mapping. A pipeline update that sets `SESSION_PIPELINE_SEARCH_QUEUE_URL` must set this stage's
  search queue URL; one that sets `HOOPDB_POOL_MAX` must keep it within 1-4.
- A plan that lists only some search resources is refused; delete and replace are refused.

## Per-queue guards

Every log line about messages from the search queue carries `queue: "search"`; while armed the
pointer queue's lines carry `queue: "pointer"`; a leftover after rollback carries `queue: "other"`.
Unarmed, a pointer-queue line has no `queue` field (as before). Log group:
`/aws/lambda/96bulls-<stage>-session-pipeline`.

CloudWatch Logs Insights, errors per queue (5 min bins):

    fields @timestamp, queue, message, error_code
    | filter level in ["error", "warn"]
    | stats count(*) as n by coalesce(queue, "pointer") as q, message, error_code, bin(5m)

Search-queue failures only (the search side's error guard):

    filter queue = "search" and level in ["error", "warn"]
    | stats count(*) as n by message, error_code, bin(5m)

Handoff failures and deadline skips on the pointer side:

    filter reason in ["search_handoff", "handoff_deadline"] and level != "info"
    | stats count(*) as n by message, error_code, bin(5m)

Latency per queue (upload to pointer, upload to search):

    filter message = "pointer indexed"
    | stats pct(s3_event_age_ms, 50) as p50, pct(s3_event_age_ms, 95) as p95 by bin(5m)

    filter message = "search index reconciled" and queue = "search"
    | stats pct(s3_event_age_ms, 95) as upload_p95, pct(event_age_ms, 95) as queue_wait_p95 by bin(5m)

Leftovers after a rollback (should fall to 0 before R2):

    filter queue = "other" | stats count(*) as n by message, bin(5m)

Queue metrics (namespace `AWS/SQS`, dimension `QueueName`):

| guard | queue | metric | alarm |
| --- | --- | --- | --- |
| search DLQ depth | `96bulls-<stage>-session-search-dlq` | `ApproximateNumberOfMessagesVisible` max > 0 over 5 min | `session-search-dlq-alarm` |
| search age | `96bulls-<stage>-session-search` | `ApproximateAgeOfOldestMessage` max > 900 s, 3 x 5 min | `session-search-age-alarm` |
| pointer DLQ depth | `96bulls-<stage>-session-events-dlq` | as above | `session-events-dlq-alarm` (existing) |
| pointer age | `96bulls-<stage>-session-events` | as above | `session-events-age-alarm` (existing) |
| drain before R2 | `96bulls-<stage>-session-search` | `ApproximateNumberOfMessagesVisible` and `ApproximateNumberOfMessagesNotVisible` both 0 | none |

Like the existing alarms, the two new ones are created with `ActionsEnabled: false`. The Lambda
`Errors` metric and alarm stay function-wide: an invocation error is not attributable to a queue
by metric, only by the log queries above.

## Not covered

- The detailed-plan pin for stage A was checked against Alchemy's printed format as it appears in
  the checked-in production plan (`planProperties` on the brief schedule's create block), not
  against a real capture of these five resources. A format difference refuses the plan; it never
  accepts more.
- A first rollout of the whole pipeline together with the search resources is refused (stage A is
  provisioning only); production already has the pipeline.
