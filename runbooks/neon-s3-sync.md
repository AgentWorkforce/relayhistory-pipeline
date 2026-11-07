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
- **Strict inventory: fail closed** (lead ruling C3 on #294; shipped unflagged as
  destructive-operation safety hardening, NOT gated by any Babysitter flag). The
  erasure job's existing S3 calls (`eraseObject`, `erasePrefix`,
  `eraseSessionAnyDate`, `eraseObjectBefore`, `listKeys`, via
  `src/migration/neon-s3-erasure/eraser.ts`) now accept only a strictly valid
  ListObjectVersions inventory. Two kinds of refusal, with different meaning:
  - **Initial inventory refusal (no deletes were made from that inventory).**
    The whole listing is read and validated before its first delete, and is
    refused when it is not strictly valid:
    - `erasure_inventory`: malformed, truncated or non-UTF-8 XML; an entry
      without Key/VersionId/IsLatest; unproven pagination (no IsTruncated,
      missing or mismatched next markers, a repeated cursor); or a bound:
      more than 10,000 versions or 100 pages in one whole-prefix listing, or
      a single listing response body over 16 MiB;
    - `erasure_foreign_key`: a listed key outside the requested prefix.
  - **Post-delete refusal (deletes MAY already have happened).**
    `erasure_inventory_changed`, or any failure of the post-delete
    verification listing (including an `erasure_inventory` /
    `erasure_foreign_key` refusal of that second listing), or a DeleteObject
    error partway through the deletes: the deletes of the first inventory were
    issued, and some or all may have succeeded. Pending
    here is NOT an atomic rollback: the deleted versions are gone, and the item
    stays pending only because zero is not yet proven.
  - **Semantics (both kinds):** a refusal is an error for that item or group
    only, and the item is NOT marked done: the queued erasure, tombstone or
    fence stays pending and is retried. A fenced tombstone is found again by
    the next sweep (every 10 min). A queued item records the code in its
    `error` column and backs off 1, 2, 4 ... minutes, capped at one hour
    (`erasureFailed` in `src/migration/neon-s3-sync/state.ts`), and is retried after that.
    Other items still run. A retry lists afresh; after a post-delete
    refusal it sees only what survived.
  - **Where the S3 refusal codes appear** (`src/migration/neon-s3-erasure/job.ts`;
    the log strings and counter names are the stable anchors): as `error_code`
    on the job's structured log lines `live listing failed` and `live erasure
    failed`, counted by `liveFenceFailed`, and `erasure failed` with `kind` for
    a queued item, counted by `failed`. The erasure backlog's `failing` count
    is the queued items whose `error` is set. `live erasure failed` can also
    carry an authority error: the per-group `stillFenced` re-check runs inside
    the same step, before that group's deletes.
  - **Authority failures** are the lines `sweep refused: privacy authority not
    ready` and `fence sweep failed`, counted by `fenceSweepFailed`, and `live
    fence sweep failed`, counted by `liveFenceFailed`. The failing step made
    no S3 call. Earlier steps of the same sweep may still have deleted: live
    erasure runs first, and a later page follows earlier pages' deletes. Fix
    the authority, never the inventory checks.
  - **Either kind:** a refusal never stops the job. The oldest queued item's
    age drives `erasure SLA at risk` after 24 h.
  - **Operator action** (the lead's ruling, verbatim): "retain the
    obligation/tombstone, investigate the inventory shape, or move to a
    separately reviewed resumable consumer. NO manual done, NO reservation
    clearing, NO disabling the strict checks, NO cap escalation as recovery."
    The codes say which rule refused; a whole-inventory workload above 10k
    versions needs the separately reviewed resumable consumer (`eraseStep`,
    the paged eraser with checkpoints).
  - **Regression evidence** on the existing E3 path:
    `src/migration/__tests__/neon-s3-erasure-live.test.ts` ("E3 on the strict
    shared eraser": a small valid inventory erases as before; a malformed and a
    >10k inventory are each refused before any delete and the same tombstone is
    erased by a later healthy sweep; a queued prefix erasure over a malformed
    inventory stays queued, backed off, and a later healthy drain erases it);
    the eraser-level refusals are in
    `strict-inventory-erasure.test.ts` and `buffered-version-erasure.test.ts`.
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

# The erasure job (its own key, its own process). Credentials ONLY on stdin, held in RAM:
# one JSON object {"databaseUrl": ..., "eraser": {accessKeyId, secretAccessKey[, sessionToken]}}.
# databaseUrl must carry user, non-empty password and host; only ?sslmode= is allowed (never ~/.pgpass).
# NEON_URL_FILE / NEON_URL / S3X_ERASER_CREDS_FILE / AWS_* credential variables refuse to start, and so
# does a terminal or a regular file on stdin (a pipe from `op` is the intended source).
ssh kjg-lap "zsh -lc 'jq -cn --arg u \"\$(op read <authority db url ref>)\" \
  --argjson e \"\$(op item get <eraser item> --vault 96Bulls-prod --format json | jq -c <reshape to the eraser object>)\" \
  \"{databaseUrl:\$u,eraser:\$e}\"'" | \
  S3X_BUCKET=... S3X_EXPECT_BUCKET=... bun run migration:neon-s3-erasure --execute
```

`S3X_PRIVACY_AUTHORITY` selects where deletions are read: `neon` (default;
`databaseUrl` is a direct Neon URL, `deleted_upload_sessions` plus the rollup
presence sweep) or `supabase` (zero-Neon checklist C3/E3; `databaseUrl` is a
Supabase direct or session-pooler URL; reads `ingest_frozen_source_deletions`,
has no presence sweep, and erases nothing unless
`sessions.ingest_privacy_authority` is `active`). Switch it only after the C3
activation (migration 20261105041000_ingest_privacy_authority) and the RHC
`supabase-authority` flip; restarting in `supabase` before activation only
logs `sweep refused: privacy authority not ready`.

Dry run (reads the authority, builds everything, stores nothing): run `main.ts`
without `--execute`; it needs only `databaseUrl` on stdin.

## Later

Move to a scheduled Worker or a 96Bulls service in us-east-1 (the transcript
store runbook asks for an in-region host): the loop is already
bounded per group and resumable from its watermark; the watermark moves from
SQLite to a Postgres table.
