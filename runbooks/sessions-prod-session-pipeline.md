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
EventBridge rate(30 minutes) --> Lambda {"reconcile":true}: list manifests modified in the last 60 minutes,
  enqueue those whose pointer row is missing or names another ETag.
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
- **One session at a time.** The ClickHouse pass holds a transaction-scoped
  advisory lock per session in Supabase, taken after the event's pointer
  commit.
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
  "clickhouse": { "url": "https://<host>:8443", "user": "search_ingest", "password": "…", "database": "relay_search" } }
```

- `hoopdb_url` is the same HoopDb runtime role the hoopdb-indexer uses (it
  needs `upsert_session_index` and writes to `session_index_ext` and
  `session_directory`). Use the session pooler or a direct connection. Any
  `sslmode` in the URL is ignored: the Lambda always verifies against the
  pinned Supabase root (`certs/supabase-prod-ca-2021.crt`).
- `clickhouse` is the `clickhouse relay-search-prod ingest` item, the same
  credentials the Neon tail uses. Omit the block while `searchIndex` is off.

Set the value from 1Password with one prompt on kjg-lap. The value is never
echoed, written to a file or put in argv. First check the item's field labels
with `op item get … --format json | jq '[.fields[].label]'`:

```sh
ssh kjg-lap "zsh -lc 'jq -n \
  --arg db \"\$(op read \"op://96Bulls-prod/<hoopdb runtime item>/<url field>\")\" \
  --argjson ch \"\$(op item get \"clickhouse relay-search-prod ingest\" --vault 96Bulls-prod --format json \
      | jq \"[.fields[] | {(.label): .value}] | add | {url: (\\\"https://\\\" + .host + \\\":\\\" + (.port // \\\"8443\\\")), user, password, database}\")\" \
  \"{hoopdb_url: \\\$db, clickhouse: \\\$ch}\"'" \
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
   The contract accepts it only as **session pipeline rollout**: the 10
   pipeline resources are `create`, `transcripts` (bucket policy grant) and
   `transcripts-key` (key policy grant) are `update`, and the other 10 store
   resources are `noop`. Then commit the capture, write `approval.capture` and
   run `NINETY_SIX_BULLS_CONFIRM_STAGE=aws-sessions-prod bin/96bulls up aws-sessions-prod`.
   If KMS or S3 refuses the new role as an invalid principal (IAM
   propagation, as on 2026-10-31), wait a minute, re-capture (it is still a
   rollout) and apply again.
4. **Secret value** (above).
5. **Notification.** Run
   `NINETY_SIX_BULLS_CONFIRM_STAGE=aws-sessions-prod scripts/sessions-prod-bucket-notification.sh --apply`,
   then `--check`. S3 sends one `s3:TestEvent`, which the Lambda ignores.
6. **Verify the pointer stage.** In CloudWatch Logs
   `/aws/lambda/96bulls-aws-sessions-prod-session-pipeline`, look for
   `pointer indexed` lines and for no `pointer index failed`. The DLQ should
   stay empty. Run one reconcile now:
   `aws lambda invoke --function-name 96bulls-aws-sessions-prod-session-pipeline --payload '{"reconcile":true}' --cli-binary-format raw-in-base64-out /dev/stdout`.
7. **ClickHouse stage.** Do this only once live writes reach S3: relayhistory-cloud
   #241 `WRITE_PATH=s3_first`. Until then S3 trails Neon by the exporter's
   30-minute debounce, and indexing from S3 would make search staler.
   - Add the `clickhouse` block to the secret.
   - Turn the Neon tail off (`SEARCH_INGEST_ENABLED` unset on the
     relayhistory Worker), so the two writers do not keep rewriting each
     other's rows.
   - Set `searchIndex: true`, then capture (**session pipeline update**:
     `session-pipeline` update only) and apply.
   - The first pass over each session rewrites its rows once (`s3`
     fingerprints). This is expected.

## Rollback

| What | How |
| --- | --- |
| Stop everything, keep messages | `aws lambda update-event-source-mapping --uuid <uuid> --no-enabled` (messages wait in SQS for 4 days) |
| Stop new events | `scripts/sessions-prod-bucket-notification.sh --remove` (the reconcile still runs: disable the rule with `aws events disable-rule --name 96bulls-aws-sessions-prod-session-reconcile`) |
| ClickHouse back to the Neon tail | `searchIndex: false` (capture and apply, an update), then `SEARCH_INGEST_ENABLED=1` on the Worker |
| Remove the pipeline | Revert, then do a reviewed capture. The contract refuses deletes, so removing the pipeline needs its own contract change and approval. |

Re-drive the DLQ after a fix with
`aws sqs start-message-move-task --source-arn <dlq arn> --destination-arn <queue arn>`.

## Known limits

- The reconcile lists the whole `96bulls/` prefix on every run (S3 cannot
  filter by date). This is acceptable at today's object count. A persisted
  cursor (S3 Inventory, or a `last_modified` watermark) is the follow-up.
- A session's ClickHouse pass reads every chunk of every live manifest, capped
  at 256 MiB per session. Larger sessions are indexed without deletes and
  logged with `scope_too_large`. Incremental, chunk-only indexing is a
  follow-up for very long live sessions.
- Fan-out to summaries and state (`summary-*`, `status-classify`) is off
  (`PIPELINE_FANOUT=off`). Nothing consumes those queues in production yet.
  Bob Costas's sweep rebuilds missing summaries once it runs.
- A pointer row's `user_id` follows the HoopDb index (`unknown` without an
  authenticated upload). ClickHouse rows take the user from the export grant
  or the manifest's `96bulls.user_id`.
