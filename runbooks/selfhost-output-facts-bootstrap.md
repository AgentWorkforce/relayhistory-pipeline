# Self-host output facts bootstrap

`SERVICE=session-pipeline-output-facts-bootstrap` is a one-shot Job. It backfills output
facts (migration `045040`) for sessions ingested before the consumer's facts flag was on,
then writes the per-workspace watermark (`sessions.output_facts_workspace_state.complete_since`)
that readers use to tell "no facts yet" apart from "no activity".
Source: `src/session-pipeline/output-facts-bootstrap.ts`. Composition: section 7 of
`docs/selfhost-composition.md`.

Nothing here runs automatically. Running it against a real database is an operator action
and needs the database owner's explicit OK (on the 96Bulls dev stack, Khaliq's own words).

## When to run

- Once, after `RELAYHISTORY_SELFHOST_OUTPUT_FACTS=true` is live on the running
  `session-pipeline-consumer`, to cover sessions ingested before the flag.
- Again (it is safe) after a run that exited 3, after fixing dead-lettered keys, or after
  importing older history into a workspace.

Prerequisites, in order:

1. `hoopdb-migrate` has applied `20261105045040_selfhost_output_facts.sql` (the ledger row is
   checked against this image's checksum).
2. `session-pipeline-consumer-bootstrap` has run (it creates `sessions.search_consumer_pending`).
3. `session-pipeline-consumer` is running from the same image with
   `RELAYHISTORY_SELFHOST_OUTPUT_FACTS=true`. The Job only queues work. The consumer does
   the reprojection. If the consumer is down, the run waits until the deadline and then exits 3.

## How to run

Same image, same env as `session-pipeline-consumer-bootstrap`:

| Variable | Value |
| --- | --- |
| `SERVICE` | `session-pipeline-output-facts-bootstrap` |
| `RELAYHISTORY_SELFHOST_PIPELINE_CONSUMER` | `true` |
| `RELAYHISTORY_SELFHOST_OUTPUT_FACTS` | exactly `true` (anything else: exit 2) |
| `RELAYHISTORY_PIPELINE_CONSUMER_SECRET` | the consumer secret JSON whose `hoopdb_url` is the **owner** URL (the consumer-bootstrap identity), never the runtime or reader URL |
| `SEARCH_INDEX_BACKEND`, `SEARCH_INDEX_POSTGRES_BACKEND_ID`, `PIPELINE_KEY_PREFIX`, `SESSION_POSTGRES_TLS_PROFILE` (+ CA vars) | the same values as the running consumer; the backend id selects its queue |
| `RELAYHISTORY_SELFHOST_OUTPUT_FACTS_BOOTSTRAP_DEADLINE_SECONDS` | optional, digits only, 1..86400, default 7200: how long Phase A waits for the consumer |
| `RELAYHISTORY_SELFHOST_OUTPUT_FACTS_BOOTSTRAP_ALLOW_PARTIAL` | optional, `true` or `false` (default `false`): exit 0 instead of 3 when workspaces stay incomplete |

Load the secret from 1Password into the Job's Secret. Never write it to disk.

Why it needs the owner identity: the `045040` tables have RLS on with no policy or grant, so
only their owner (without FORCE RLS) or a superuser can read the session state or write the
watermark. The Job checks this before anything else and refuses with exit 2 otherwise.

What it does:

1. **Phase A, reprojection (enqueue, not inline).** For each live session (directory and
   pointer both not deleted, key under `PIPELINE_KEY_PREFIX`) whose facts are missing,
   incomplete, or older than the directory's `event_projection_epoch`, it inserts the
   session's manifest keys into `sessions.search_consumer_pending` for the backend
   (`attempts 0`, due now, marker `output_facts_bootstrap`, `ON CONFLICT DO NOTHING`). It
   never touches existing rows, including dead ones. The consumer loads due rows on every
   tick and reprojects them through the normal search-index pass, which writes facts only
   through the `045040` writers. The Job polls every 15 s until no session owes facts, the
   backend's live queue is empty, or the deadline passes. It logs counts only.
2. **Phase B, watermark.** In one transaction holding
   `pg_advisory_xact_lock(hashtextextended('output-facts-bootstrap', 0))`, it inserts,
   for every workspace in `session_directory` whose live sessions all have complete,
   current facts, `complete_since` = the earliest `min_ts` of those sessions (or `now()`
   when the workspace has no live session) and `marked_by = 'bootstrap'`. It uses
   `ON CONFLICT DO NOTHING`.

## Exit codes

| Code | Meaning | Action |
| --- | --- | --- |
| 0 | every workspace is marked, or was already marked (or partial was allowed) | verify, done |
| 1 | failed (connection, SQL, unexpected error); the Phase B transaction rolled back | read the `error_code` in the log, fix, rerun |
| 2 | refused: flag not exactly `true`, consumer config invalid or dormant, deadline or partial value invalid, `045040` not in the ledger or checksum mismatch, required table missing, or not the owner identity | fix the env, identity or migrate order; nothing was written |
| 3 | some workspaces still owe facts; they got no watermark (the complete ones did) | check the consumer and the dead rows (below), then rerun |

## Verification

As the read API role (`bulls96_read_api`) inside a tenant transaction:

```sql
BEGIN;
SELECT set_config('bulls96_read.org_id', '<org>', true),
       set_config('bulls96_read.workspace_id', '<workspace>', true);
SELECT * FROM sessions.output_facts_coverage_for('<workspace>', '<from>', '<to>');
COMMIT;
```

`complete_since` is non-null once the workspace is marked, and `incomplete` should be 0.
The read API serves facts for windows at or after `complete_since`. The Job's final log
lines report `workspaces`, `marked`, `already marked` and `incomplete` counts.

To see why a workspace stays incomplete (as the owner):

```sql
SELECT count(*) FILTER (WHERE dead) AS dead, count(*) FILTER (WHERE NOT dead) AS pending
  FROM sessions.search_consumer_pending WHERE backend_id = '<backend id>';
```

## Rerun semantics

- Idempotent. Phase A queues only sessions that still owe facts and never resets an
  existing queue row. Phase B never updates an existing watermark, so `complete_since` is
  never lowered or moved, even when older sessions complete later.
- A workspace that becomes incomplete after it was marked keeps its mark. The Job counts it
  as incomplete (and also as "incomplete but marked earlier"), so the run still exits 3
  until the consumer catches up.
- Concurrent runs: Phase B is serialized by the advisory lock (its single statement
  reads after the lock is held). Phase A takes no lock and is safe to overlap
  (`ON CONFLICT DO NOTHING`).

## Known limitations

- Sessions whose only live pointers the consumer does not project (upload-generation keys,
  managed `srv_` publications) never get facts, so their workspace stays incomplete and the
  run exits 3. Use `..._ALLOW_PARTIAL=true` only once that is understood.
- Dead-lettered keys are not revived. The consumer retries a dead key only when a new
  revision arrives.
- Keys outside `PIPELINE_KEY_PREFIX` are not queued. Their workspaces stay unmarked
  (Phase B considers every workspace in `session_directory`).
- `complete_since` comes from the live sessions' state rows. Facts outside them (for example
  of tombstoned sessions not yet erased) do not lower it.
