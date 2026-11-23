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
