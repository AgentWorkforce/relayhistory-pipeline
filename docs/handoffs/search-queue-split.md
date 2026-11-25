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

These totals (and the "demand" and "worst" columns below) are **configured** demand: the caps the
mappings and the env allow, multiplied out. They are an upper bound, not measured in-flight demand.
Measure the actual numbers during the rollout, after each apply and before the next capture:

- Concurrent executions of the function, all sources (both mappings, the reconcile chain, the
  brief tick): `AWS/Lambda` `ConcurrentExecutions`, dimension `FunctionName =
  96bulls-<stage>-session-pipeline`, statistic Maximum, period 1 min. Times `HOOPDB_POOL_MAX` this
  bounds the connections the function can actually hold.
- Per mapping, execution environments active per minute, by queue (each environment runs one
  invocation at a time, so this bounds that queue's concurrency in the minute):

      filter ispresent(queue)
      | stats count_distinct(@logStream) as environments by queue, bin(1m)

  While the split is armed every pointer-queue line carries `queue: "pointer"`, so both mappings
  are covered; before C2 the pointer side has no `queue` field: use `filter stage =
  "session-indexed" | stats count_distinct(@logStream) by bin(1m)` for it.
- Messages in flight per queue: `AWS/SQS` `ApproximateNumberOfMessagesNotVisible` (Maximum, 1 min)
  for `96bulls-<stage>-session-events` and `96bulls-<stage>-session-search`; with batch 10, the
  invocations behind them are at least that number / 10.
- Connections actually open at the database, if the pooler exposes them:
  `select count(*) from pg_stat_activity where application_name = '96bulls-session-pipeline'`.

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
| R2 | `mappingEnabled: false`, only after the drain below | 30 | 78 |
| R3 | `maxConcurrency: 20` (the agreed rollback state) | 60 | 60 |

Each stage rolls back to its agreed state, never to whatever ran before: the pointer cap's
rollback is 20. A trial above the staged caps (for example an owner-approved 26) is not restored
by this rollout; its own agreed rollback is 20, and the guard refuses any planned cap above 20. While the live pointer is at such a trial value, B is refused (B starts only from the settled
Enabled/20); apply the trial's own rollback to 20 first.

R1 is the fast rollback. Leave the search mapping enabled after it: records still in the search
queue drain through it. With the URL unset they are `queue: "other"`, take the ordinary path
(pointer stage, idempotent, then search inline; a busy pass is a new pointer-queue message) and
nothing acts on their receipt handles. The search resources stay (disabled) after R3; deleting
them is refused by the guard, as for every pipeline resource.

### R2 drain: before disabling the search mapping

1. **Producers quiesced.** R1 is applied (the function's live env has no
   `SESSION_PIPELINE_SEARCH_QUEUE_URL`), and for 5 minutes no line shows a handoff or a search-path
   requeue (after R1 the search path no longer exists; this waits out invocations still running the
   armed config, up to the 300 s timeout):

       filter reason in ["search_handoff", "handoff_deadline"] or queue = "search"
       | stats count(*) as n by bin(1m)

   must return 0 in every minute of the last 5.
2. **Queue empty, sustained.** Three polls, 60 s apart, each with all three counts 0 (delayed
   covers the 60 s busy, deferred and deadline requeues still waiting):

       aws sqs get-queue-attributes --queue-url https://sqs.<region>.amazonaws.com/<account>/96bulls-<stage>-session-search \
         --attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible ApproximateNumberOfMessagesDelayed

   Any non-zero count restarts the three polls.
3. **Disable** the mapping: capture and apply R2.
4. **Confirm no polling and no invocations** from the search mapping before R3 raises the pointer
   cap: `aws lambda list-event-source-mappings --function-name 96bulls-<stage>-session-pipeline`
   shows the search mapping `State: Disabled` (not `Disabling`); `AWS/SQS` `NumberOfEmptyReceives`
   (Sum, 5 min) on the search queue is 0 (an enabled poller long-polls and records empty receives);
   and `filter queue = "other" | stats count(*) by bin(1m)` is 0 for 5 minutes.

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
- C2 and R1 are function env updates. The capture and the deploy now read the live env back and
  the guard refuses a plan that would drop or overwrite a live variable it does not show (Deploy
  guard, below). So a hand-set variable blocks C2 and R1 until it becomes a stack input; this
  branch does not add inputs for the `SEARCH_INDEX_SKIP_*` or `SESSION_POINTER_SKIP_*` flags.
- Alchemy state still holds `HOOPDB_POOL_MAX: "4"` from the last apply if the 3 was set only by
  hand. Then the first capture with `hoopdbPoolMax: 3` shows `session-pipeline` updating
  `HOOPDB_POOL_MAX` "4" -> "3", and stage A's provisioning-only guard refuses it. That state
  reconcile is its own capture before A (a session pipeline update: env only, live stays 3,
  60 connections throughout), and it is a full env write, so the previous point applies to it too.

## Deploy guard (stacks/aws/transcript-store.ts)

- **Approved values, exactly** (`APPROVED` in `stacks/aws/transcript-store.ts`): live pointer 20,
  split pointer 10, search 16, pool 3. Every stage is pinned to them from the plan's old -> new values
  and the verified live state; budget compliance alone is not enough (pointer 12 / search 8, pool 2,
  or a pointer of 26 are all refused even where they fit 80).
- **session search queue rollout** (stage A): the search resources are created (all, or the rest
  after a partial apply) and every other resource is noop. Live baseline required, at capture and
  at deploy: events mapping Enabled at exactly 20, `HOOPDB_POOL_MAX` exactly 3, no
  `SESSION_PIPELINE_SEARCH_QUEUE_URL`, and no search mapping (after a partial apply only this stage's,
  Disabled at 16). Each created resource's detailed block must be exactly its rendered props,
  **tags included** (exactly `96bulls:component=session-pipeline`,
  `96bulls:repo=AgentWorkforce/sessions-pipeline`, `96bulls:stack=aws-sessions-prod`; a foreign tag is
  refused): the DLQ; the queue, whose policy names exactly the function's **live** role (from the
  readback, so the deploy, which has no state snapshot, refuses any other role even in the same
  account; at capture the role in state must equal it) and whose DLQ is the search DLQ; the
  mapping on the search queue for the pipeline function, batch 10, partial batch responses,
  MaximumConcurrency exactly 16, `enabled: false`; the two alarms with `ActionsEnabled: false`.
- **In place, while the search resources exist**: a plan that touches a search resource, the events
  mapping or the function must be exactly one approved stage, recognised from its old -> new values
  and the live state (old values must equal the live ones: a stale plan is refused):

  | stage | plan | live state required |
  | --- | --- | --- |
  | B | events mapping MaximumConcurrency 20 -> 10, nothing else | events Enabled 20, search Disabled 16, pool 3, no URL |
  | C1 | search mapping `enabled` false -> true at 16, nothing else | events Enabled 10, search Disabled 16, pool 3, no URL |
  | C2 | function env adds exactly `SESSION_PIPELINE_SEARCH_QUEUE_URL` (this stage's), pool exactly 3 | events Enabled 10, search Enabled 16 on this queue and function, pool 3, no URL |
  | R1 | function env removes exactly that URL | events Enabled 10, search Enabled 16, pool 3, the URL |
  | R2 | search mapping `enabled` true -> false at 16 | events Enabled 10, search Enabled 16, pool 3, no URL (R1 applied) |
  | R3 | events mapping MaximumConcurrency 10 -> 20 | events Enabled 10, search Disabled 16, pool 3, no URL |

  For C2 and R1 the full intended post-apply env (the planned new env, which Alchemy writes whole)
  is compared with the live env: the URL is the only permitted delta, in the stage's direction; a
  key the live env lacks and the apply would add (a stale `SESSION_PIPELINE_RETRY_VISIBILITY_S`, for
  example), or the reverse, is refused. A code-only function update is not a split stage. Anything
  else is refused: a DLQ or alarm update, two changes in one plan, a function update with a mapping
  change, any other resource beside them, a planned cap outside the table (the trial value 26 is
  never restored: R3's rollback state is 20). A plan whose search resources are all noop and that
  touches neither the events mapping nor the function is judged by its own phase as before. The
  budget over every enabled mapping is checked on top (26 + 16 at pool 3 is 126: refused).
- **Verified live source state** (`verifiedLive`): exactly this stage's function (name and full
  FunctionArn), `State: Active`, `LastUpdateStatus: Successful`, its role in the production account;
  every mapping stable (`Enabled` or `Disabled`), targeting the unqualified function (a mapping on an
  alias or a version is refused), one of the two this stack declares (unknown or duplicate refused),
  with the rendered settings and a MaximumConcurrency read within the account limit.
- **Bound to the capture.** The capture records the live function's FunctionArn, role, RevisionId,
  aliases, versions, an env digest (never the values) and every mapping's UUID, state and settings in
  the provenance (`pipelineLive`). The deploy reads the live function again and refuses any difference.
- **No readback, no change**: stage A and every in-place change are refused without the live readback,
  at capture and at deploy.
- **Every pipeline plan**: an events mapping update is pinned as in B; and a function update's full
  intended post-apply env is compared with the live env: the planned new env when the plan declares
  an env change, the env in state when it does not (a code-only update still writes the stored env
  whole). Every key that differs between them, in either direction, must be one the plan visibly
  changes; with no declared change none may differ. At deploy, without state, a code-only update is
  accepted only for the exact live function the capture compared (the fingerprint).
- **The readback** (`stacks/aws/pipeline-live.ts`): `lambda get-function-configuration`,
  `list-aliases`, `list-versions-by-function`, and `list-event-source-mappings` for the function, every
  alias and every published version (a name-filtered listing need not show an alias's mapping), each
  listing followed through `NextMarker` to its end (a page that fails or repeats refuses: never a
  partial list), mappings deduplicated by UUID (one reported differently on two targets refuses).
  External fields are checked against allow-lists (UUID format, state enum, ARN, role, alias,
  version, env-key and response-type patterns) before any use, so no refusal can carry a malformed
  value. Failures have fixed codes (`live_cli_failed`, `live_output_empty`, `live_parse_failed`,
  `live_incomplete`, `live_env_error`, `live_wrong_function`, `live_not_settled`); no CLI output,
  exception text or env value is ever put in a message; guard refusals name env keys, never values.
- A plan that lists only some search resources is refused; delete and replace are refused.

## Per-queue guards

Every log line about messages from the search queue carries `queue: "search"`; while armed the
pointer queue's lines carry `queue: "pointer"`; a leftover after rollback carries `queue: "other"`.
Unarmed, a pointer-queue line has no `queue` field (as before). An init failure (the runtime could
not start, so every record of the batch was returned, including a refused
`SESSION_PIPELINE_SEARCH_QUEUE_URL`) is logged before any queue is known: `session pipeline init
failed` with `queue: "unattributed"` and `count` (the batch's records). It is reported on its own,
never counted as either queue. Log group: `/aws/lambda/96bulls-<stage>-session-pipeline`.

CloudWatch Logs Insights, errors per queue (5 min bins):

    fields @timestamp, queue, message, error_code
    | filter level in ["error", "warn"] and (not ispresent(queue) or queue != "unattributed")
    | stats count(*) as n by coalesce(queue, "pointer") as q, message, error_code, bin(5m)

Init failures, separately (whole batches returned before any queue was known):

    filter queue = "unattributed"
    | stats count(*) as failures, sum(count) as records_returned by error_code, bin(5m)

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
| drain before R2 | `96bulls-<stage>-session-search` | `ApproximateNumberOfMessagesVisible`, `ApproximateNumberOfMessagesNotVisible` and `ApproximateNumberOfMessagesDelayed` all 0, three polls 60 s apart | none |

Like the existing alarms, the two new ones are created with `ActionsEnabled: false`. The Lambda
`Errors` metric and alarm stay function-wide: an invocation error is not attributable to a queue
by metric, only by the log queries above.

## Rights granted in stage A, and the rights the operator needs

What stage A grants (resource policies; the role has no identity policy):

| resource | grantee | rights |
| --- | --- | --- |
| search queue policy | the pipeline function's execution role | `sqs:ReceiveMessage`, `sqs:DeleteMessage`, `sqs:ChangeMessageVisibility`, `sqs:GetQueueAttributes` (consume, used by the mapping's poller) and `sqs:SendMessage` (handoff, requeue), on the search queue only; everything denied without TLS |
| search DLQ policy | nobody | only the TLS deny; SQS itself moves messages there by the redrive policy |
| the two alarms | nobody | `ActionsEnabled: false`, no alarm actions, so no SNS or other target |
| the search mapping | (created disabled) | Lambda polls with the execution role above once enabled; no new permission on the function |

What the executor (the operator's own IAM scope for the capture and the deploy) needs, by
stage, from the calls `@alchemy.run/aws` makes (`SQS/Queue.ts`, `Lambda/EventSourceMapping.ts`,
`CloudWatch/Alarm.ts`) and the live readback. Compare these with the operator's policy before A;
this branch cannot check them.

| stage | actions |
| --- | --- |
| capture and deploy (every stage) | `lambda:GetFunctionConfiguration`, `lambda:ListAliases`, `lambda:ListVersionsByFunction`, `lambda:ListEventSourceMappings` (live readback), plus the existing state-bucket reads |
| A: queues | `sqs:CreateQueue`, `sqs:GetQueueUrl`, `sqs:GetQueueAttributes`, `sqs:SetQueueAttributes` (policy, redrive, timeouts, SSE), `sqs:TagQueue`, `sqs:UntagQueue`, `sqs:ListQueueTags`, `sqs:ListQueues` |
| A: mapping | `lambda:CreateEventSourceMapping`, `lambda:GetEventSourceMapping`, `lambda:ListEventSourceMappings`, `lambda:TagResource`, `lambda:UntagResource`, `lambda:ListTags` (no `iam:PassRole`: the mapping uses the function's existing role) |
| A: alarms | `cloudwatch:PutMetricAlarm`, `cloudwatch:DescribeAlarms`, `cloudwatch:TagResource`, `cloudwatch:UntagResource`, `cloudwatch:ListTagsForResource` |
| B, C1, R2, R3 | `lambda:UpdateEventSourceMapping`, `lambda:GetEventSourceMapping` |
| C2, R1 | the existing pipeline update rights (`lambda:UpdateFunctionConfiguration`, `lambda:UpdateFunctionCode`, `lambda:GetFunction*`) and the grants' cascade (`sqs:SetQueueAttributes` on both queues, the secret, KMS and bucket policy updates as today) |
| drain checks | `sqs:GetQueueAttributes` on the search queue, `cloudwatch:GetMetricData`, `logs:StartQuery`, `logs:GetQueryResults` |

Not needed and not to be granted for this rollout: `sqs:DeleteQueue`, `sqs:PurgeQueue`,
`lambda:DeleteEventSourceMapping`, `cloudwatch:DeleteAlarms` (the guard refuses deletes). Alchemy
would call the delete actions only to undo a failed create; without them such a failure is left
for an operator.

## Not covered

- The detailed-plan pin for stage A was checked against Alchemy's printed format as it appears in
  the checked-in production plan (`planProperties` on the brief schedule's create block), not
  against a real capture of these five resources. A format difference refuses the plan; it never
  accepts more.
- A first rollout of the whole pipeline together with the search resources is refused (stage A is
  provisioning only); production already has the pipeline.
- The guard's in-place pins were exercised with update blocks generated in Alchemy's format (and the
  parser on the checked-in production plan), not with a real capture of B, C1 or C2.
- The live readback is read again at deploy and compared with the capture's, but a change made by
  hand between that readback and Alchemy's own API calls in the same run is not seen.
