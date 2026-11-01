# Continuous Neon to S3 transcript export (`neon-s3-sync`)

Khaliq's direction (2026-10-31): load all RelayHistory content from Neon into
the sessions-prod transcript bucket continuously, newest sessions first, so the
Supabase pointers (later, once #96/#99 settle) point at real data. Neon stays
the source of truth; the exporter only reads it.

Code: `src/migration/neon-s3-sync/`. Tests: `src/migration/__tests__/neon-s3-sync.test.ts`.

## What it writes

`96bulls/<org>/v1/ws=…/m=…/h=…/d=…/s=…/manifest.json` and `raw/*.ndjson.zst`,
the pipeline's org-prefix layout (`PIPELINE_KEY_PREFIX=96bulls`,
`PIPELINE_ORG_FROM_PREFIX=true`). Chunks are sealed by the pipeline's
`sealRawChunk` (scrub + zstd + create-only put, the metadata AlleyOop's
`chunksAreServerSealed` checks). Manifests are #40/#55's cold-export shape and
pass `parseManifest`, `relayhistoryExportProvenance` and
`readRelayhistoryExport`. One manifest per `(org, workspace, harness, session,
machine)`; the machine of a delivered record is Neon's own
`delivery:md5([workspace, origin])`.

Provenance is self-issued, not a RelayAuth grant: `grant_id=neon-s3-sync`,
`plan_digest=sha256("96bulls-neon-s3-sync/0.1.0\n<org>")`, owner
`migration:neon-s3-sync`. The bucket's IAM is the trust boundary.

A JSON string longer than the scrub bound (262,144 chars; the sealer refuses
it rather than truncating) is cut with an in-band note and counted in the
catalog's `truncated_strings`; the full text stays in Neon. Khaliq accepted
this temporarily (2026-10-31): every cut manifest is in the state file's
`truncations` ledger (and its manifest's catalog in S3), the progress log
reports `truncation_ledger`, and T0 is gated on zero outstanding truncations
(follow-up: raise or chunk the sealer's limit, then re-export those sessions).

## Neon load

- Role `relayhistory_readonly`, direct (unpooled) connection, every session
  `default_transaction_read_only=on`, `statement_timeout=20s`,
  `lock_timeout=2s`, verified at start.
- Keyset pages over existing indexes only; a session under ~2,000 events is one
  statement, larger ones are paged per kind.
- Token bucket on rows and payload bytes (`S3X_ROWS_PER_SEC`,
  `S3X_BYTES_PER_SEC`), multiplied by `S3X_SLOW_FACTOR` inside
  `S3X_SLOW_WINDOWS` (hackathon hours). Pauses 2 min while Neon has more than
  3 non-advisory lock waiters or 25 in total.

## Change detection and erasure (48 h SLA)

Two processes, two keys (Khaliq, 2026-10-31). The exporter is write-only:
it holds the writer key, never the eraser key, and a change becomes a new
object version (the bucket expires noncurrent versions after 1 day).
`neon-s3-erasure` (`src/migration/neon-s3-erasure/`) is the only deleter and
holds only the eraser key ("sessions-prod transcript eraser").

- Exporter: sweeps `session_revisions` every 2 min and the rollups every 30
  min. A changed session is re-read and rewritten after a debounce of 30 min
  plus 0.6 s per event, at most 24 h. Unchanged chunks are reused. Objects the
  new manifest no longer references, and session prefixes the group no longer
  produces, go to the `erasures` queue in the shared state file. Anything it
  references again is cancelled under a per-session lease.
- Erasure job, every 10 min:
  - It sweeps the rollups itself, and erases every exported group Neon no
    longer has. A sweep that reads nothing erases nothing.
  - It erases every exported group a deletion fence covers.
  - It drains the queue, deleting **every version by id**
    (ListObjectVersions + DeleteObject?versionId), only under a session prefix
    of the item's own org, with the session leased.
  - It logs `erasure SLA at risk` once the oldest queued item is older than 24 h.
- Accepted gap (#105): record-level changes that do not bump
  `session_revisions` reach S3 only through the exporter's idle 14-day re-read.

## Run (linuyc, for now)

State: `~/.agent-tmp/s3x/state.sqlite` (ids, revisions, keys; no content).
Logs: `~/.agent-tmp/s3x/sync.log` (counts and codes only).

```sh
nohup ~/.agent-tmp/s3x/run.sh >/dev/null 2>&1 &   # waits on ~/.agent-tmp/s3x/creds.fifo
# then, one approval prompt on kjg-lap, values never echoed:
ssh kjg-lap "zsh -lc 'jq -n --argjson w \"\$(op item get <exporter item> --vault 96Bulls-prod --format json)\" \
  --argjson e \"\$(op item get <eraser item> --vault 96Bulls-prod --format json)\" ...'" > ~/.agent-tmp/s3x/creds.fifo
touch ~/.agent-tmp/s3x/STOP   # stop after the current run

# The erasure job (its own key, its own process; once the eraser item exists):
NEON_URL_FILE=<(...) S3X_ERASER_CREDS_FILE=<(...) S3X_BUCKET=... S3X_EXPECT_BUCKET=... \
  bun run migration:neon-s3-erasure --execute
```

Dry run (reads Neon, builds everything, stores nothing): run `main.ts` without
`--execute`.

## Later

Move to a scheduled Worker or a 96Bulls service in us-east-1 (the transcript
store runbook asks for an in-region host): the loop is already
bounded per group and resumable from its watermark; the watermark moves from
SQLite to a Postgres table.
