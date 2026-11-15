# Epoch event time repair

> **Self-host only. Do not `--apply` on the hosted AWS Lambda stack.** The Lambda consumer has
> no freshness sweep (`reconcileRecent` looks back only minutes, and
> `reconcileStaleProjections` runs only in `consumer-service.ts`). A session marked there stays
> not ready, and its workspace's directory event reads are refused
> (`session_directory_events_require_ready`, SQLSTATE `0A000`) until that session gets a new
> upload. The tool refuses `--apply` (exit 2) unless `--self-host-consumer` is also passed.

`src/session-pipeline/epoch-event-time-repair.ts` is a one-shot operator tool. It finds
directory sessions whose event times hold 1970-01-01 (`event_ts = 'epoch'`) and marks them
so the consumer re-projects them with the fixed projection. The module header documents the
selection, the mark, the env and the exit codes.

Nothing here runs automatically. Running it against a real database is an operator action
and needs the database owner's explicit OK (on the 96Bulls stacks, Khaliq's own words).

## Why

The relayhistory Claude parser writes `ts_ms = 0` for a record that carries no time. The
export projection (`packages/pipeline/src/lib/pipeline/relayhistory-projection.ts`) took 0
as 1970-01-01, and its tool-call settling gave every timeless tool call under such an event
the same instant. The index pass then wrote an `event_ts = 'epoch'` row to
`sessions.session_directory_event_times` (and `event_first_ts`), and the read API showed
those events in 1970.

The fixed projection treats a `*_ms` of 0 as "not reported": the event takes its delivery
time (`received_at`) and its tool calls take their parent's time. A session whose manifests
do not change is never re-projected, so its epoch rows stay until it is marked.

## When to run

On a self-host install, after the consumer with the fix is deployed, in a quiet window: until
the consumer re-projects them, marked sessions make their workspaces' directory event reads
refuse. A consumer still on the old code writes the
epoch rows back on its next pass; a rerun then marks those sessions again.

## What it does

- **Plan (default).** Counts only. Writes nothing.
- **`--apply`.** For each live directory session that is ready and has an epoch row, one
  autocommit `UPDATE` sets `event_projection_ready = false` and
  `event_projection_epoch = event_projection_epoch + 1`. That is the directory state the
  pointer trigger (`sessions.session_directory_events_changed`) sets when a session owes a
  pass. The epoch bump also fences a pass that read the old epoch. A session that is already
  not ready is left alone (`already_pending`). It deletes no rows: the consumer's next
  complete pass replaces the session's event times.

Who re-projects a marked session:

- **Self-host consumer** (`SERVICE=session-pipeline-consumer`). Its freshness check treats a
  directory row that is not ready, or whose epoch moved, as stale, and re-sends every live
  manifest of the session on its next tick. Nothing else is needed.
- **AWS Lambda consumer.** It has no freshness sweep. A marked session is re-projected on
  its next S3 event or reconcile body for one of its manifests. Until then it stays not
  ready.

While a session is not ready, the directory event readers treat its workspace as not ready
(`sessions.session_directory_events_require_ready`), as they do after any incomplete pass.

## How to run

Run it in the services image as a Job or exec:

```sh
bun run /app/src/session-pipeline/epoch-event-time-repair.ts            # plan
bun run /app/src/session-pipeline/epoch-event-time-repair.ts --apply --self-host-consumer    # mark
```

| Variable / flag | Value |
| --- | --- |
| `EPOCH_EVENT_TIME_REPAIR_DATABASE_URL` or `EPOCH_EVENT_TIME_REPAIR_DATABASE_URL_FILE` | exactly one: the schema owner login (it reads `session_directory_event_times` and updates `session_directory`). The file form takes an absolute path to a mounted secret |
| `SESSION_POSTGRES_TLS_PROFILE` (+ CA vars) | as for the other self-host workloads |
| `--page N` | scopes per page within one workspace, 1..1000, default 200 |
| `--self-host-consumer` | required with `--apply`: states that the consumer is the self-host `session-pipeline-consumer`, whose freshness check re-projects marked sessions |
| `--max-pages N` | stop after N pages that returned scopes (exit 3). Empty workspaces do not count. The output then carries `next_after` |
| `--after CURSOR` | resume strictly after the scope a stopped run handled last: pass its `next_after` unchanged |

Load the secret from 1Password into the Job's Secret. Never write it to disk. The output is
one JSON line of counts (`workspaces`, `affected`, `markable`, `marked`, `already_pending`,
`not_live`, `raced`, `refused`, `refused_codes`, `complete`), plus `next_after` when it stopped
early. `next_after` is an opaque cursor that encodes the last scope's org, workspace, harness and
session id (base64url): treat it like an id and keep it out of tickets and chat.

## Exit codes

| Code | Meaning | Action |
| --- | --- | --- |
| 0 | done | verify (below) |
| 2 | refused: bad flags (including `--apply` without `--self-host-consumer`, or a malformed `--after`), or the database URL is missing, ambiguous or unreadable | fix the flags or env. Nothing was read or written |
| 3 | incomplete: `--max-pages` reached, or a scope's `UPDATE` was refused (counted by SQLSTATE, for example `55P03` lock timeout) | after `--max-pages`: rerun with `--after <next_after>` (and the same `--max-pages`) until exit 0. After a refusal: rerun |
| 4 | any other error | read the `error` code, fix, rerun |

## Verification

1. Plan before apply: `affected` is the number of sessions with an epoch row, and
   `markable` is what apply marks.
2. After apply, once the consumer has caught up, plan again. Expect `affected` to count only
   tombstoned sessions (`not_live`) or none.
3. If a session stays `already_pending` across runs, its re-projection is not happening.
   Check the consumer's logs and dead-lettered keys.

## Rerun semantics

- Idempotent. A marked session is not ready until the consumer re-projects it, so a rerun
  counts it as `already_pending` and changes nothing.
- A session the consumer re-projects between selection and `UPDATE` is counted `raced` and
  left as the consumer wrote it. The `UPDATE` re-checks that the session is live, is ready,
  and still has an epoch row.
- Only the exact instant 1970-01-01 00:00:00 is selected. A later 1970 time (1 ms or more)
  is a time the client reported and is left alone.

## Related

- The SQL twin of the projection, `sessions.delivery_activity_ms` and
  `sessions.delivery_catalog_time` (relayhistory-cloud migrations `0023` and `0022`,
  vendored in `vendor/relayhistory-cloud`), still accepts 0 upstream. The proposed upstream
  fix is
  `integration/relayhistory-cloud/packages/relayhistory/db/migrations/0068_delivery_zero_ms_not_reported.sql`.
  `packages/pipeline/test/relayhistory-zero-ms.test.ts` proves it agrees with the
  TypeScript projection.
