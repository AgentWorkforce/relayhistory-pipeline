# Session pipeline in sessions-prod (`aws-sessions-prod`)

A manifest that lands in the transcript bucket upserts its Supabase pointer
row and indexes its events into ClickHouse `transcript_events`. Code:
`stacks/aws/session-pipeline.ts` (infrastructure, inside the transcript-store
stage) and `src/session-pipeline/` (the Lambda). Tests:
`stacks/aws/__tests__/session-pipeline.test.ts`,
`src/session-pipeline/__tests__/`.

```text
transcripts bucket --S3 event (96bulls/**/manifest.json, put + delete)--> SQS 96bulls-aws-sessions-prod-session-events
  --event source mapping (batch 10, 20 s window, max concurrency 4, partial batch failures)--> Lambda 96bulls-aws-sessions-prod-session-pipeline
       1. object-landed + session-indexed (the pipeline's own handlers): sessions.upsert_session_index,
          session_index_ext, session_directory. Revision-fenced in SQL.
       2. ClickHouse: reconcile the session scope (org, workspace, harness, session) against every live
          manifest of that session, read from S3 (search-index.ts). Off until searchIndex is true.
  --after 8 receives--> SQS ...-session-events-dlq (14 days, alarm on any message)
EventBridge rate(30 minutes) --> Lambda {"reconcile":true}:
  1. HEAD the manifest of every live pointer indexed in the last 60 minutes; enqueue the gone ones (lost or raced deletes).
  2. List manifests modified in the last 60 minutes; enqueue those whose pointer row is missing, deleted or names another ETag.
  At 240 s (under the 300 s timeout) it invokes itself asynchronously with the listing cursor (at most 24 links).
```

## Guarantees

- **Idempotent, at least once.** Every message re-reads the manifest. The
  pointer upsert is fenced by the manifest revision, so a duplicate, late or
  replayed event never rolls a row back. The ClickHouse step diffs identities
  and fingerprints, inserts what changed and deletes what S3 no longer has.
- **Deletes and erasure.** A manifest delete (`ObjectRemoved:*`, including the
  erasure job's `DeleteObjectVersion`) is re-checked against S3. If no current
  manifest is left, the pointer is tombstoned and the directory row
  re-projected. When a session has no live pointer, every ClickHouse row of
  that session is deleted, inside the index window or not. If an old version
  was deleted but a current manifest remains, the current one is indexed.
- **Never deletes on a partial read.** A missing manifest, an unreadable chunk
  or a truncated ClickHouse read makes the pass incomplete: inserts happen,
  deletes do not, and the next event or the reconcile finishes the job.
- **One session at a time.** The pointer stage (manifest read and upsert) holds
  the session's pointer lock, a transaction-scoped advisory lock in Supabase. A put whose read raced an erasure therefore cannot upsert after
  the delete's tombstone: `upsert_session_index` lets a newer revision
  undelete, so without the lock it would bring the row back for good
  (#121 review H1, `__tests__/pointer-race.test.ts` on the real SQL).
- **A busy session is requeued, not waited on.** The ClickHouse pass takes a
  separate search lock with `pg_try_advisory_xact_lock`. If another pass holds
  it, the event goes back on the queue 60 s later and is acked, so a hot live
  session never times events out into the DLQ.
- **Date moves follow the session.** The exporter re-keys a session whose first
  record moved earlier (a new `d=`, revision 1 at the new key), then erases the
  old prefix. The pointer row's identity has no date, so:
  - a manifest delete first looks for the same session (same machine) under
    another date, up to 45 days either side; if one is live, the row follows
    it instead of being tombstoned;
  - a put the revision fence kept out, because the row names another key, re-keys
    the row when that key is gone, and retries while both are live;
  - a tombstone applies only to a row that still names the deleted key.
  (#121 review 2 N1; `__tests__/pointer-race.test.ts`, real SQL.)
- **Large sessions are marked partial, never indexed from a subset.** A session
  over 64 MiB raw or 100,000 events (summed from its manifests, before any chunk
  is read) is not read. Its `search_ingest_session_state` row gets
  `needs_full = 1`, and nothing else is written or deleted. Every other
  incomplete pass also writes `needs_full = 1`; only a complete pass writes 0.
  Readers must not treat a `needs_full = 1` session's rows as current. Indexing
  those sessions completely (cursored over their chunks) is open work.
- **Erasure is one predicate delete.** With no live pointer, the ClickHouse
  rows of the session are deleted by `(org, workspace, source, session)`,
  without listing, so no session size can truncate it. A failure throws and
  the message retries.
- **Unreadable is not empty.** A quarantined (unscrubbed) manifest, or one whose
  body names another session than its key, makes the pass incomplete: it
  deletes nothing.
- **Backpressure.** The mapping caps concurrency at `maxConcurrency` (default
  4). Each invocation holds at most 4 Supabase connections. A slow downstream
  backs messages up in SQS (4-day retention). The backlog-age alarm fires
  after 15 minutes.
- **Scrubbed raw only.** A manifest without `raw.scrub` gets a quarantined
  pointer, and its chunks are never read.
- **Logs** carry ids, counts and codes only, through `src/shared/log.ts`.

## Credentials

There is one Secrets Manager secret, `96bulls/aws-sessions-prod/session-pipeline`.
The stack creates it empty, so Alchemy state never holds the value, and its
resource policy lets only the pipeline's execution role read it. The value is
JSON:

```json
{ "hoopdb_url": "postgresql://<hoopdb runtime role>:<password>@<pooler host>:5432/postgres",
  "clickhouse": { "url": "https://<host>:8443", "user": "search_ingest", "password": "…", "database": "relay_search" },
  "openai_api_key": "…" }
```

- `hoopdb_url` is the same HoopDb runtime role the hoopdb-indexer uses (it
  needs `upsert_session_index` and writes to `session_index_ext` and
  `session_directory`). Use the session pooler or a direct connection. Any
  `sslmode` in the URL is ignored: the Lambda always verifies against the
  pinned Supabase root (`certs/supabase-prod-ca-2021.crt`).
- `clickhouse` is the `clickhouse relay-search-prod ingest` item, the same
  credentials the Neon tail uses. Omit the block while `searchIndex` is off.
- `openai_api_key` is the session brief model key (Session briefs, below).
  Omit it while briefs are off. Without it a brief tick claims nothing.

Set the value from 1Password on kjg-lap. The value is never echoed, written to
a file or put in argv. First check the item's field labels with
`op item get … --format json | jq '[.fields[].label]'`, then run this in a
kjg-lap zsh (the process substitutions expose only `/dev/fd` paths in argv):

```sh
jq -n \
  --rawfile db <(op read "op://96Bulls-prod/<hoopdb runtime item>/<url field>") \
  --slurpfile ch <(op item get "clickhouse relay-search-prod ingest" --vault 96Bulls-prod --format json \
    | jq '[.fields[] | {(.label): .value}] | add | {url: ("https://" + .host + ":" + (.port // "8443")), user, password, database}') \
  '{hoopdb_url: ($db | rtrimstr("\n")), clickhouse: $ch[0]}' \
| AWS_PROFILE=sessions-prod aws secretsmanager put-secret-value --region us-east-1 \
    --secret-id 96bulls/aws-sessions-prod/session-pipeline --secret-string file:///dev/stdin --query VersionId
```

A warm Lambda keeps the value it read until its sandbox is recycled. After a
rotation, force fresh reads with
`aws lambda update-function-configuration --function-name 96bulls-aws-sessions-prod-session-pipeline --description "rotated <date>"`.

## Rollout (each step is the lead's, in order)

1. **Merge** this PR (code and stack).
2. **Inputs.** In `.96bulls/aws-sessions-prod/aws-inputs.json`, add
   `"sessionPipeline": { "searchIndex": false }` under `transcriptStore`, and
   write the approval record.
3. **Capture and apply.** Run `bin/96bulls plan aws-sessions-prod --capture`.
   The contract accepts it only as **session pipeline rollout**: the 11
   pipeline resources are `create`, `transcripts` (bucket policy grant) and
   `transcripts-key` (key policy grant) are `update`, and the other 11 store
   resources are `noop`. Then commit the capture, write `approval.capture` and
   run `NINETY_SIX_BULLS_CONFIRM_STAGE=aws-sessions-prod bin/96bulls up aws-sessions-prod`.
   If KMS or S3 refuses the new role as an invalid principal (IAM
   propagation, as on 2026-10-31), wait a minute, re-capture (it is still a
   rollout) and apply again.
4. **Secret value** (above).
5. **Notification.** Run
   `SESSIONS_PROD_ACCOUNT_ID=<prod-account-id> NINETY_SIX_BULLS_CONFIRM_STAGE=aws-sessions-prod scripts/sessions-prod-bucket-notification.sh --apply`,
   then `--check` (the script refuses without `SESSIONS_PROD_ACCOUNT_ID`). S3 sends one `s3:TestEvent`, which the Lambda ignores.
6. **Verify the pointer stage.** In CloudWatch Logs
   `/aws/lambda/96bulls-aws-sessions-prod-session-pipeline`, look for
   `pointer indexed` lines and for no `pointer index failed`. The DLQ should
   stay empty. Run one reconcile now:
   `aws lambda invoke --function-name 96bulls-aws-sessions-prod-session-pipeline --payload '{"reconcile":true}' --cli-binary-format raw-in-base64-out /dev/stdout`.
   Confirm the function is not reachable from the internet:
   `aws lambda get-function-url-config --function-name 96bulls-aws-sessions-prod-session-pipeline`
   must fail with `ResourceNotFoundException`, and
   `aws lambda get-policy --function-name 96bulls-aws-sessions-prod-session-pipeline`
   must show exactly three `lambda:InvokeFunction` statements: EventBridge
   (the reconcile rule, this account), EventBridge (the brief rule, this
   account) and the function's own role.
7. **ClickHouse stage.** Two gates, both required:
   - **Live writes reach S3:** relayhistory-cloud #241 `WRITE_PATH=s3_first`.
     Until then S3 trails Neon by the exporter's 30-minute debounce, and
     indexing from S3 would make search staler.
   - **Every exported session carries every kind the index projects.** Sessions
     exported by `neon-s3-sync` before 5e498e0 have no `trajectory` and
     `commit_link` records in S3. The whole-session diff would delete the
     trajectory rows the Neon backfill put in ClickHouse. Re-export those
     sessions first (#101 review R1), and confirm there are none left before
     setting `searchIndex`.
   - Add the `clickhouse` block to the secret.
   - Turn the Neon tail off (`SEARCH_INGEST_ENABLED` unset on the
     relayhistory Worker), so the two writers do not keep rewriting each
     other's rows.
   - Set `searchIndex: true`, then capture (**session pipeline update**,
     below) and apply.
   - The first pass over each session rewrites its rows once (`s3`
     fingerprints). This is expected.

## Updating the pipeline (code or settings)

A change to the function's code or settings is captured and applied like any
other change. The contract accepts it only as **session pipeline update**:

- Pipeline resources are `update` or `noop`, and at least one is `update`.
  The store resources other than `transcripts` and `transcripts-key` stay
  `noop`. Nothing is created, replaced or deleted.
- When `session-pipeline` updates, the plan re-computes the function's
  `roleArn` as `(known after apply)`: Alchemy keeps the role's name stable,
  not its ARN. The five resources whose policies name the role then show an
  `update`: `session-events` (queue policy), `session-pipeline-secret`
  (resource policy), `session-reconcile-continue` (permission principal),
  `transcripts` (bucket policy) and `transcripts-key` (key policy). The role
  and its ARN do not change.
- Each of those five may differ only by that cascade. Every removed line must
  equal the added line at the same position, except an `AWS:` or `principal:`
  line where the removed value is the function's role ARN (read from the
  capture's state snapshot) and the added value is `(known after apply)`.
  Any other difference refuses the capture: an added or removed statement or
  line, a changed `Action`, `Effect`, `Resource`, `Sid` or condition, or a
  different ARN.
- With the cascade, only `session-pipeline` and those five may change. A
  cascade together with any other change, or without `session-pipeline`
  updating, is refused.

## Session briefs

Briefs (src/session-brief) run inside the pipeline function, so they need no
new principal on the bucket, key or secret. `transcriptStore.sessionPipeline.briefs`
controls them:

```json
"briefs": { "enabled": true, "backgroundDailyBudget": 5000, "globalDailyCap": 20000 }
```

- `enabled: true` sets `SESSION_BRIEF_ENABLED=true` on the function, so indexing
  arms a brief wakeup per session. It also sets the `session-brief-schedule`
  rule (`96bulls-aws-sessions-prod-session-brief`, every minute, input
  `{"brief":true}`) to `ENABLED`. Absent or `false`: the env says `false`, the
  rule is `DISABLED`, and a stray `{"brief":true}` returns before init.
- Each tick claims up to `SESSION_BRIEF_BATCH` (5) due wakeups under a lease
  and briefs them one by one. Each model call has a 20 s timeout, which fits
  well inside the function's 300 s. Ticks may overlap: claims use `SKIP LOCKED`.
- The caps default to src/session-brief/config.ts (5,000 sessions per org per
  UTC day, 20,000 model attempts across all orgs per UTC day, 30 per session).
  Set them here so the budget sits in the reviewed inputs.
- The model key is `openai_api_key` in the pipeline secret, never env or
  inputs. It goes in with the rest of the value (Credentials). Generate it into
  1Password first, then add
  `--rawfile key <(op read "op://96Bulls-prod/<brief model item>/credential")`
  to the `jq -n` command and
  `openai_api_key: ($key | rtrimstr("\n"))` to its output object. Do not use
  `--arg key "$(op read …)"`; command substitution would expose the key in
  `jq`'s argv.
- Historical sessions are not enqueued unless `briefs.sweepSeconds` is set: it emits
  `SESSION_BRIEF_SWEEP_SECONDS` (an integer 0-86400; absent = not emitted, so the worker
  default 0, off). Each sweep queues up to 100 indexed sessions that have no brief row at
  all, under the same budgets. Setting it is a budgeted backfill and needs its own
  approval. Sessions holding a Neon-backfilled title (docs/brief-titles.md) have a brief
  row, so the sweep never re-briefs them; new evidence does.

**Turning them on (each step needs Khaliq's OK):**

1. **Retire the catch-up loop** (STOP, no restart). Its `b47c1e6` projector
   overwrites brief titles in `session_directory`
   (recorded in the eff8b3be-to-5ed11913 upgrade, U6a; that operator record is not
   kept in the repository).
2. **Ship the code off.** Capture and apply this code with `briefs` absent. The
   contract accepts it only as **session brief rollout**: the two brief
   resources are `create`, and the rest is all `noop` or exactly a session
   pipeline update (above, role-ARN cascade included). The rule is created
   `DISABLED`.
3. **Secret.** Put the value with `openai_api_key` (above), then force fresh
   reads (Credentials).
4. **Enable.** Set `briefs.enabled: true` with the approved caps, then capture
   and apply. It is accepted only as **session brief update**: the rule
   updates, plus the function's env update and its cascade.
5. **Verify.** Look for `session brief tick` log lines with `failed: 0`, and
   for rows in `sessions.session_briefs`.

**Off again:** set `briefs.enabled: false`, then capture and apply (a brief
update). For an immediate stop, run
`aws events disable-rule --name 96bulls-aws-sessions-prod-session-brief`.
Then reconcile the inputs, since the next capture would show the drift.

## Buffered search feed

The stack defines a one-minute EventBridge rule for the buffered search feed. It stays DISABLED, and neither
feed variable is set, until `transcriptStore.sessionPipeline.bufferedSearchFeed` is
`{ enabled: true, gateDeclared: "buffered-search-gate/v1" }` with `searchIndex: true`. `gateDeclared` is an
operator's assertion that the read Worker is already deployed and verified with the gate on; the deployment
does not prove it. No credential is wired here: the feeder reads `buffered_search_feed_database_url` and
`buffered_search_feed_database_ca_pem` from the pipeline secret and stays off without a valid login.

Optional feed tuning under `bufferedSearchFeed`; each is set on the ingest runtime only when provided, otherwise the feeder's own default applies:

- `pageSize` sets `INGEST_BUFFERED_SEARCH_FEED_PAGE` (integer 1 to 200; feeder default 50).
- `stagesPerSecond` sets `INGEST_BUFFERED_SEARCH_FEED_RATE` (integer 1 to 50; feeder default 5).
- `budgetMs` sets `INGEST_BUFFERED_SEARCH_FEED_BUDGET_MS` (integer 5000 to 240000; feeder default 50000).
- `secondVersion: true` sets `INGEST_BUFFERED_SEARCH_FEED_SECOND_VERSION=on` (the S version encoding; the feeder refuses `enabled` without it). Absent or `false`: not set (feeder default off).
- `settleSeconds` sets `INGEST_BUFFERED_SEARCH_FEED_SETTLE_S` (integer 180 to 3600; feeder default 300).

Rolling out this rule alone creates only the DISABLED schedule and its permission. Arming it is a production
step that needs Khaliq's explicit OK and follows the buffered search activation order: ClickHouse columns,
read gate verified, then feeder. Do not arm it before the later-erasure delete lane for ClickHouse exists.

### Buffered erasure delete

A second one-minute rule sends `{"bufferedErasureDelete":true}` to the later-erasure delete lane
(`src/session-pipeline/buffered-erasure-delete.ts`). It stays DISABLED, and `INGEST_BUFFERED_ERASURE_DELETE` is not
set, until `transcriptStore.sessionPipeline.bufferedErasureDelete` is `{ enabled: true }`. That needs
`bufferedSearchFeed.gateDeclared: "buffered-search-gate/v1"` and `searchIndex: true`; it then also sets
`BUFFERED_SEARCH_FEED_GATE_DECLARED`, even with the feed itself off. It uses the feeder's login from the pipeline
secret. Optional, each set only when provided: `pageSize` (`INGEST_BUFFERED_ERASURE_PAGE`, 1 to 200, default 25),
`stagesPerSecond` (`INGEST_BUFFERED_ERASURE_RATE`, 1 to 20, default 2), `budgetMs`
(`INGEST_BUFFERED_ERASURE_BUDGET_MS`, 5000 to 240000, default 40000). Its rule and permission roll out as their own
reviewed plan phase ("session buffered erasure delete rollout"), never together with the feed's rule. Arming it is
a production step that needs Khaliq's explicit OK.

## Repo tag rename (one time, before the read API rollout)

3f8494c renamed the repository, so the stack-wide tag `96bulls:repo` changes
from `AgentWorkforce/96BullSessions` to `AgentWorkforce/sessions-pipeline` on
every tagged resource. The contract accepts that plan only as **repo tag
rename**, and only on its own:

- Capture **without** `transcriptStore.sessionReadApi` in the inputs, so the
  plan has no read API creates. A plan with any create (the read API or
  anything else), replacement or deletion is refused.
- Every updated resource must differ by exactly one pair,
  `96bulls:repo: AgentWorkforce/96BullSessions` →
  `96bulls:repo: AgentWorkforce/sessions-pipeline`, at the same position and
  indentation. `session-pipeline` may update only with "no declared property
  changes". The five role grants (above) may also show the role-ARN cascade,
  checked as in a pipeline update; `session-reconcile-continue` has no tags and
  shows only the cascade. Any other changed, added or removed line is refused,
  including another tag key or another repo value. The two repo values are
  hard-coded, so a later tag change needs its own contract change.
- Apply it, then add `sessionReadApi` back, capture again and apply: that plan
  is the separate **session read api rollout** (creates only, everything else
  `noop`).

## Rollback

| What | How |
| --- | --- |
| Stop everything, keep messages | `aws lambda update-event-source-mapping --uuid <uuid> --no-enabled` (messages wait in SQS for 4 days) |
| Stop new events | `SESSIONS_PROD_ACCOUNT_ID=<prod-account-id> NINETY_SIX_BULLS_CONFIRM_STAGE=aws-sessions-prod scripts/sessions-prod-bucket-notification.sh --remove` (the reconcile still runs: disable the rule with `aws events disable-rule --name 96bulls-aws-sessions-prod-session-reconcile`) |
| ClickHouse back to the Neon tail | `searchIndex: false` (capture and apply, an update), then `SEARCH_INGEST_ENABLED=1` on the Worker |
| Remove the pipeline | Revert, then do a reviewed capture. The contract refuses deletes, so removing the pipeline needs its own contract change and approval. |

Re-drive the DLQ after a fix with
`aws sqs start-message-move-task --source-arn <dlq arn> --destination-arn <queue arn>`.

## Known limits

- The reconcile lists the whole `96bulls/` prefix, chunks included, on every
  chain (S3 cannot filter by date). The chain covers any size, but each run
  costs one LIST per 1,000 objects. Listing per org and day, or S3 Inventory,
  is the follow-up.
- A session's ClickHouse pass reads every chunk of every live manifest. Above
  64 MiB raw or 100,000 events it is marked partial (above), logged as
  `search index partial`, and not indexed from S3. Cursored, chunk-by-chunk
  indexing is the open work that would cover them.
- A date move further than 45 days from the old key is not followed: the old
  key's delete then tombstones the row, and the new key's put re-keys the
  `session_index` row, but the `session_directory` row stays tombstoned
  (decision 0002: a tombstoned session does not come back on an older
  revision).
- The role's `s3:ListBucket` is unconditional (as for the reader user): S3
  answers 404 instead of 403 for a missing manifest only with it, and deletes
  are detected that way. It exposes key names, never content.
- The event source mapping relies on the queue policy alone (Alchemy attaches
  no custom policy to the function's role). This is not yet proven against
  AWS; if `CreateEventSourceMapping` refuses it, the fallback is an Alchemy
  Effect function with `consumeQueueMessages`.
- Fan-out to summaries and state (`summary-*`, `status-classify`) is off
  (`PIPELINE_FANOUT=off`). Nothing consumes those queues in production yet.
  Bob Costas's sweep rebuilds missing summaries once it runs.
- A pointer row's `user_id` prefers the authenticated HoopDb identity, retains
  an already-recorded real identity, then accepts a validated export/Courier
  manifest binding. `unknown` is never projected as a directory membership.
  ClickHouse rows use the same export grant or admitted manifest identity.
