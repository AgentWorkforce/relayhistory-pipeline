# Streamed reconcile backport: build and operate

Branch `review/layered-sessions-pipeline-stream-backport-246a`. It starts from
`246ae498` (the search-index base) plus the 2026-11-15 projection change, then
ports the hosted streamed ClickHouse reconcile behind `SEARCH_INDEX_STREAMING_CH`
(default off). Nothing here deploys anything.

## Build the Lambda ZIP

Bun 1.4.2, from the repository root of a clean checkout. No AWS, registry,
credential or network access is needed:

```sh
git checkout <SHA>
bun install --frozen-lockfile
bun scripts/build-lambda-artifact.ts --source-sha "$(git rev-parse HEAD)"
```

The script refuses to build unless HEAD equals `--source-sha` and the tree is clean.
It calls the locked Alchemy bundler itself (`makeFunctionBundler` and `Bundle.build`
from alchemy 2.0.0-beta.84, rolldown 1.2.13) with the session-pipeline function's
props (`src/session-pipeline/lambda.ts`, `build.tsconfig = tsconfig.json`, arm64,
`isExternal: true`), then zips the result with Alchemy's `zipCode`.

Output, in `dist/lambda/session-pipeline/` (`--out dist/<a>/<b>` changes the
directory; it must be three levels deep, like `.alchemy/bundles/<name>`, so the
source map's relative paths come out the same):

| file | what |
| --- | --- |
| `index.mjs` | the bundle, the ZIP's handler entry |
| `index.js.map` | its source map, the ZIP's second entry |
| `session-pipeline.zip` | the deployable archive (sorted entries, fixed 1980-01-01 mtime, mode 0644) |
| `build.json` | source SHA, Bun version, sha256 and size of the three files above |

The script prints `sha256  bytes  name` for each of the three files.

## Compare two builds

Two runs, two clones at different paths, and two machines all produce
byte-identical files. No absolute path ends up in the output.

```sh
diff a/build.json b/build.json          # all three hashes at once
cmp a/index.mjs b/index.mjs
diff -u a/index.mjs b/index.mjs         # rolldown output is not minified: diffs read as source
unzip -p session-pipeline.zip index.mjs | sha256sum   # an archive against a build
```

To check what the port changes in the bundle, build the baseline commit (the
2026-11-15 projection state) and the head, then compare the SQL they contain and the
`sessions.` functions they name:

```sh
grep -oE 'sessions\.[a-z_]+' index.mjs | sort -u
grep -oE '\b[0-9a-f]{64}\b' index.mjs | sort -u
```

## The flag and its constants

`SEARCH_INDEX_STREAMING_CH=1` on the function arms it. It needs
`SEARCH_INDEX_ENABLED=1` and a `clickhouse` block in the pipeline secret; without
them, and for any value other than unset, `""`, `0` or `1`, init refuses. The stack
renders it only when `transcriptStore.sessionPipeline.streamReconcile.enabled` is
`true`, and that setting needs `searchIndex: true`.

On main the settings below are env vars. This base has constants instead, and
sets no new env var:

| here (constant) | value | main's env var |
| --- | --- | --- |
| `SEARCH_INDEX_STREAM_CH_BUDGET.maxReadBytes` (search-index.ts) | 1 GiB | `SEARCH_INDEX_STREAM_CH_MAX_READ_BYTES` |
| `SEARCH_INDEX_STREAM_CH_BUDGET.maxReadEvents` (search-index.ts) | 1,000,000 | `SEARCH_INDEX_STREAM_CH_MAX_READ_EVENTS` |
| `SEARCH_INDEX_STREAM_DEADLINE_MARGIN_MS` (search-index.ts) | 60,000 ms | `SEARCH_INDEX_STREAM_DEADLINE_MARGIN_MS` |
| `STREAM_PASSES` (search-index-stream.ts) | 4 | (same constant) |
| `STREAM_KEEPALIVE_MS` (search-index.ts) | 20,000 ms | (same constant) |
| `STREAM_MIN_START_MS` (search-index.ts) | 120,000 ms | (none: main has no start floor) |
| `STREAM_TAIL_RESERVE_MS` (search-index.ts) | 30,000 ms | (none: main has no tail reserve) |
| keyset page size: the whole read's `insertBatch` | 5,000 | (same) |

A scope streams only when the whole read refuses it with `scope_too_large` alone,
and four passes fit the budgets. That allows at most 256 MiB or 250,000 records
per scope. A scope above that limit stays partial (`needs_full = 1`) exactly as
before the port. Each invocation streams at most one scope. A second one in the
same batch, or one with less than `STREAM_MIN_START_MS` (120 s) left before the
deadline, is requeued with the existing 60 s delayed SQS message (log reason
`stream_deferred`). Once the invocation's stream deadline has passed, no further scope of the batch is
started, whether a whole read or a stream; the rest are requeued the same way
(reason `stream_batch_deadline`). A requeued scope comes back as a new SQS
message: its receive count starts again, so a scope requeued this way never
reaches the DLQ.

A scope that is already running when the deadline passes is not interrupted. A
whole read normally ends inside the 60 s margin, but nothing guarantees it: each
ClickHouse statement may take up to 60 s and the state-row insert up to 300 s. A
streamed pass stops at its next check. It does not start its tail (a batch of
deletes, then the catalog write, the state row and COMMIT) with less than
`STREAM_TAIL_RESERVE_MS` (30 s) left before the deadline, which is about 90 s
before the function timeout. The tail check runs even when there is nothing to
delete. So a streamed pass has about 210 s (the deadline less the 30 s reserve)
for its reads, scan and inserts, not about 240 s. A pass that finishes those inside
the reserve is cut with `scope_too_large,stream_deadline` and stays partial.

A stopped streamed pass (deadline, a read budget) returns success: the message is
deleted from the queue and the scope stays `needs_full = 1` until its next upload
or a new reconcile message (below). Its log line carries the pass's own reasons
(for example `scope_too_large,stream_deadline`).

## Reconcile one session

Each SQS message on the session-events queue with this body makes the Lambda
re-check one manifest. That covers its pointer row and then its session's search
scope, including the streamed reconcile when it applies (`s3-events.ts`
`RECONCILE_BODY_KEY`, consumer.ts `processBatch`):

```json
{"reconcile":{"key":"96bulls/<org_id>/v1/ws=<workspace_id>/m=<machine_id>/h=<harness>/d=<YYYY>/<MM>/<DD>/s=<session_id>/manifest.json"}}
```

`key` is the manifest's object key as stored: not URL-encoded, starting with the
function's `PIPELINE_KEY_PREFIX`. Any one live manifest of the session works,
because the search stage reconciles the whole scope (every live pointer of the
session). `etag` and `version` are optional and are left out here. The pass is
idempotent, so sending it twice is harmless.

Sending this message to the production queue is an operator action. It needs its
own approval, separate from building or reviewing this branch.

## Open operator check: the database's idle-in-transaction limit

A streamed scope keeps its Postgres transaction open (it holds the scope's advisory
lock) while it reads S3 and ClickHouse, for up to the invocation's deadline. That is
about 240 s inside the 300 s function timeout. The pg pool's `idleTimeoutMillis`
(60 s) only closes clients idle in the pool, never a checked-out client inside a
transaction. The limit that matters is the server's
`idle_in_transaction_session_timeout` for the pipeline's database role, and nobody
working on this branch can see it.

The pass re-sends the statement it already sends,
`SELECT pg_try_advisory_xact_lock(hashtextextended($1, 0)) AS ok` with the scope's
key: at most every 20 s (`STREAM_KEEPALIVE_MS`) while it reads, and unthrottled
before and after the ClickHouse scan and before every batch of deletes. Main sends
`SELECT 1 AS stream_keepalive` instead; this base reuses its own statement so the
bundle's Postgres statement set stays the same as the baseline's.

If the connection ends while the pass holds it, pg emits `error` on the client.
With the flag on, the search transaction holds a per-checkout listener that keeps
that error. The pass's next statement (its keepalive, the check before any delete,
or COMMIT) rejects with it, the transaction fails normally, and the broken client
is destroyed. If the keepalive statement fails, or returns anything but
`ok = true`, the pass also stops. Either way nothing more is deleted,
`needs_full = 1` is recorded (best effort, before the error is rethrown), and the
message fails and is redelivered. With the flag off there is no listener, exactly
as before; there the transaction stays open only briefly.

Before arming the flag, an operator should confirm that the role's
`idle_in_transaction_session_timeout` is either 0 or at least 300 s, on the server
and on any connection pooler in between. The pass inserts its state rows (the
pre-mark, a stop and the final row) with the ClickHouse client's default insert
timeout of 300 s, and the Postgres transaction is idle while each insert runs.

## Known behaviour while a scope streams

- From the start of a streamed pass until its end, the session's state row reads
  `needs_full = 1` (the pre-mark, 1 ms before the pass's own state row). Main has
  the same in its pending row. A complete pass then records `needs_full = 0`.
- A scope that needs more time than one invocation allows (about 210 s for all four
  passes, the scan and the inserts, see the tail reserve above) is cut on every attempt. Nothing is
  lost, but it stays `needs_full = 1`. The pass emits no new signal for this; the
  existing warn line is enough to find it. Search the pipeline's logs for
  `"message":"search index partial"` with `"reason":"scope_too_large,stream_deadline"`.
  The same `org_id` and `session_id` turning up on each attempt is such a scope.
  `"reason":"scope_too_large,stream_read_events_budget"` is a scope whose index holds
  more identities than the events budget. A plain `"reason":"scope_too_large"` only
  says the scope was partial and did not stream: the consumer writes that literal for
  every partial result of the whole read, whatever its reasons (above the streamed
  limit, a missing manifest, the flag off).
- Known limit: after the reserve check the tail is not bounded in time. One delete
  batch may take up to 60 s and the state-row insert up to 300 s. A timeout for the
  stream's own statements is a possible follow-up; it is not built here.

## Upload-to-searchable latency: `s3_event_age_ms`, `event_age_ms`, `queue_origin`

Three log fields measure latency. They appear on the `pointer indexed` line (one line
per key, covering the messages for that key) and on the `search index reconciled`
line and the `search index partial` warn line (one pass per session scope, covering
the messages for that scope). No flag gates them. They change nothing that is
indexed, deleted or acknowledged. The requeue decision, key, delay and count are
unchanged; the requeue body gains two telemetry fields (below). The log allowlist
keeps each field only if its value passes its rule.

- `s3_event_age_ms` is `floor(log time - oldest upload event time covered)`. It is
  a non-negative integer. A negative age, from clock skew, is logged as 0.
  - **Upload time.** The upload time is the S3 notification record's `eventTime`
    for an `ObjectCreated:*` event. When several messages cover the line, the oldest
    time wins. This includes repeated puts of one key, where the later sequencer
    still wins the key. Deletes carry no upload and are not counted.
  - **Requeued passes.** A pass can be requeued because the scope is busy, the
    stream is deferred, or the batch deadline has passed. A requeued pass carries
    the time forward in its requeue body. The later pass then reports the age since
    the original upload, not since the requeue.
  - **Redelivered messages.** A message redelivered after a failure or a visibility
    timeout is the original body, so it keeps its `eventTime`.
  - **When the field is omitted.** It is omitted, not guessed, when any covered
    message that is not a delete has no usable time. That covers:
    - a scheduled reconcile sweep body;
    - a requeue body without a time;
    - a missing or unparseable `eventTime`.
- `event_age_ms` is `floor(log time - oldest SQS SentTimestamp)` over the messages
  covered by the line. It is a non-negative integer, clamped at 0.
  - **What it measures.** `SentTimestamp` is the time the message was put on the
    queue. A requeue and a sweep message are new messages, so for them the field is
    the wait since that enqueue, not since the upload.
  - **Redelivered messages.** An `s3` message that SQS redelivers after a failure
    or a visibility timeout keeps its original `SentTimestamp` and is still tagged
    `s3`. Its `event_age_ms` therefore includes the failed attempt and the timeout,
    and redeliveries inflate the tail. They cannot be told apart on these lines:
    `ApproximateReceiveCount` is not logged. The `search index failed` and
    `pointer index failed` error lines are a rough indicator of how many there are.
  - **When the field is omitted.** It is omitted when any covered message lacks a
    `SentTimestamp` attribute that is a positive integer.
- `queue_origin` is one of `s3`, `requeue` or `sweep`. It is always present on these
  lines.
  - `s3` means every covered message is an S3 notification.
  - Otherwise it is `requeue` if any covered message is a search pass that this
    consumer requeued.
  - Otherwise it is `sweep`: a reconcile body from the scheduled sweep, or from any
    producer that marks neither.

Requeue body shapes:

- **Before this change:** `{"reconcile":{"key":...}}`.
- **Now:** `{"reconcile":{"key":...,"requeued":true,"oldest_event_time":"<ISO 8601>"}}`.
  `oldest_event_time` is present only when every covered upload time is known.
- **Sweep:** `{"reconcile":{"key":...,"etag":...,"version":...}}`, unchanged.

A build without this change reads only `key`, `etag` and `version`. It ignores the
two extra fields. A body without `requeued` still parses:

- it counts as `requeue` if it carries `oldest_event_time`;
- otherwise it counts as `sweep`.

No field holds an id, key or time string. Only the integers and the three origin
words are logged.

### Computing p50/p90 of upload-to-searchable latency

1. Take `s3_event_age_ms` on `search index reconciled` lines with `"ok":true` that
   carry the field, and compute p50 and p90 over the window. Use a percentile
   aggregation over the parsed JSON field in the log store.
   - These lines are end to end across requeues, so keep every `queue_origin`.
   - Count the `"ok":true` lines without the field alongside, so you can see the
     share the percentiles leave out (passes driven by a sweep, requeues without a
     time).
   - Exclude lines with `"ok":false`, which are partial or incomplete passes and not
     fully searchable. Such a scope completes only in a later pass, triggered by a
     new upload or a sweep. That pass reports the age of the events it covers, not
     of the earlier ones.
2. For queue wait, take `event_age_ms` on the same lines, and on `pointer indexed`
   lines for the pointer stage, restricted to `"queue_origin":"s3"`. A requeue or
   sweep message has its own `SentTimestamp`, so exclude every other origin.
   - An `s3` message redelivered after a failure or a visibility timeout keeps its
     `SentTimestamp`, so redeliveries inflate the tail (p90) and cannot be filtered
     out; `ApproximateReceiveCount` is not logged. Read the tail next to the count
     of `search index failed` and `pointer index failed` lines in the same window,
     a rough indicator of redeliveries.
3. For the delay from S3 to the queue, compare the
   two fields on the same `s3` lines. `s3_event_age_ms - event_age_ms` is roughly
   the time from the S3 event to the SQS enqueue. The two fields use different
   clocks, so treat small differences as noise.

## Acceptance refusals: `contender_seats`

`contender_seats` says what a pass's acceptance refusal looked like. It appears on the
`search index reconciled` line, and on the `search index partial` warn line when that line's
pass carries it, only when the pass read its records and its reasons include
`relayhistory_acceptance_missing`. Every such pass carries it. No other pass does, and no
flag gates it. It is computed only on that failure path, from records the pass already
holds, with no extra S3, ClickHouse or SQL read. It changes nothing that is indexed,
deleted, stamped or acknowledged.

The fold refuses a group of two or more contending records with one record id when at
least one of them has no `accepted_seq` (`lastAccepted`). A member is seated when it has
one. A group is the fold's own view: per (origin, record id) the newest revision, with
`accepted_seq` merged across copies of that revision, tombstones included. Convergence
events and legacy kinds never join a group.

- `all_unseated`: no refused record id group has a seated member.
- `seated_unseated`: every refused record id group has a seated member.
- `both`: refused record id groups of both kinds.
- `catalog_key`: no record id group was refused; the refusal came from the catalog-key
  fold in `projectRelayhistorySession` (the whole read's projection, or the streamed
  read's pass-2 catalog projection).

The first three words describe the record id groups only. With one of them, the
catalog-key fold may also have refused; the word does not say.

A pass skipped before read (`SEARCH_INDEX_SKIP_ACCEPTANCE_MISSING_ENABLED=true`) read
nothing and carries no `contender_seats`. The log allowlist keeps only the four words;
any other value is dropped. No id, content or count is logged.

## Legacy kinds and the record id fold (backport of 9fd94a57)

What changes: the record id fold leaves the four legacy kinds (`legacy_conversation_turn`,
`legacy_session_link`, `legacy_session_outcome`, `legacy_scope`, see
`isLegacyRelayhistoryRecord`) out, as it already left convergence events out. A legacy
record also keeps its newest revision per (origin, record id, kind), so it never replaces
or hides a delivery record, or a legacy record of another kind, that shares its
(origin, record id). Before the backport, two `legacy_*` records with one record id from
two origins formed a group, and a group with an unseated member refused the scope.

This is a behaviour change with no flag. It brings the lineage to main's rule: a scope
whose only unseated cross-origin contenders are legacy kinds stops being refused with
`relayhistory_acceptance_missing`. Legacy records still project no rows (the catalog-key
fold gives them no key), so a scope that now passes indexes only its delivery records.
Groups of delivery records are refused exactly as before.

The streamed read's payload-free seat fold (`search-index-stream.ts`) and the
`contender_seats` mirror (`contender-seats.ts`) follow the same rule, so the streamed read
and the whole read still end with the same outcome and reasons. A convergence line's owner
is still the first manifest carrying its (origin, record id) in any kind, legacy included.

The backport is its own commit, so it can be deployed alone or left out.
