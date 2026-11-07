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
  destructive-operation safety hardening). Since #309 the erasure job erases ONLY
  through the shared strict paged eraser (`eraseStep` / `verifyErased`,
  `src/migration/neon-s3-erasure/eraser.ts`), in both `S3X_OBLIGATIONS` modes:
  - **Bounded traversal, not a refusal.** A run reads at most 10,000 versions /
    100 pages (and its step deadline), then saves a durable checkpoint and stops
    `incomplete`; the next sweep resumes. A large prefix is therefore erased as
    PARTIAL progress across sweeps (no single-listing "> 10k refused" any more).
  - **Initial inventory refusal (no deletes from that page).** A page that is not
    strictly valid is refused before its deletes: `erasure_inventory` (malformed,
    truncated or non-UTF-8 XML; an entry without Key/VersionId/IsLatest; unproven
    pagination: no IsTruncated, missing or mismatched next markers, a repeated
    cursor; a listing body over 16 MiB) or `erasure_foreign_key` (a key outside
    the requested prefix).
  - **Post-delete refusal (deletes MAY already have happened).** A later page's
    refusal, a failed fresh verification, or a DeleteObject error partway: the
    earlier pages' deletes were issued and some or all may have succeeded.
    Pending here is NOT an atomic rollback: deleted versions are gone; the item
    stays pending only because zero is not yet proven.
  - **Semantics:** a refusal is an error for that item or group only. In EITHER
    mode, a refused (e.g. malformed) listing NEVER marks queued work done or
    removes it; after a later-page or proof refusal the item stays incomplete.
    The queued item (backed off), tombstone or fence stays pending and is
    retried. OFF marks an item done after a healthy fresh strict zero inventory;
    ON never marks an exporter item done under its unattested genesis.
  - **Where the codes appear:** `error_code` on the job's log lines (`erasure
    failed` with `kind` for a queued item; `fence sweep failed`, `live fence sweep
    failed`, `live listing failed`, `live erasure failed`, `live erasure
    unverified` for a sweep) and its counters (`failed`, `unverified`,
    `fenceSweepFailed`, `liveFenceFailed`). The full table is in "E3 erasure:
    strict eraser (both modes)" below.
  - **Operator action** (the lead's ruling, verbatim): "retain the
    obligation/tombstone, investigate the inventory shape, or move to a
    separately reviewed resumable consumer. NO manual done, NO reservation
    clearing, NO disabling the strict checks, NO cap escalation as recovery."
  - **Regression evidence:** `neon-s3-erasure-live.test.ts` ("E3 on the strict
    shared eraser", run in both modes): a small valid inventory erases as before;
    a malformed inventory is refused before any delete and is never marked done; a
    > 10k machine prefix is erased in bounded resumable sweeps (partial progress)
    with its pointer only after a strict proof (`erasure_verify_pending` until
    then); a queued prefix over a malformed inventory stays queued and backed off.
    Eraser-level refusals: `paged-erasure.test.ts`, `strict-inventory-erasure.test.ts`,
    `buffered-version-erasure.test.ts`.
- Accepted gap (#105): record-level changes that do not bump
  `session_revisions` reach S3 only through the exporter's idle 14-day re-read.

## Run (linuyc, for now)

State: `~/.agent-tmp/s3x/state.sqlite` (ids, revisions, keys; no content).
- `S3X_OBLIGATIONS` OFF (default): that SQLite file, as before.
- ON: that path is a small pointer, and the database is the sibling
  `state.sqlite.obligations.sqlite`. It is NOT disposable: it holds the
  unresolved-write obligations (every exporter PUT recorded before it is sent),
  ownership claims, erasure reservations and the protected terminal / erased
  history. Never delete, replace or restore the pointer, the sibling or its
  `-wal` / `-shm`. Its genesis is always UNATTESTED (no dead process, absent lock,
  cancellation or wait proves an earlier PUT cannot still commit), so the erasure
  job never marks an exporter prefix or object done on it
  (`erasure_obligations_unattested`). It still erases. There is no unlock: clearing
  it needs a separately reviewed recovery contract.

UNQUALIFIED dependency (remote writers): relayhistory-api's live writer
(`m=srv_` session directories and pointers) and the session ingest's staged
batches (`live-batches/`) are NOT in this local ledger. Their PUTs have no local
invocation, claim or reservation, so the erasure job cannot prove their unknown
outcomes are settled. It only re-sweeps their tombstones on every pass and never
retires or reports them complete. Their publication/obligation interlock belongs to
their own lanes and must be qualified there before activation.
Logs: `~/.agent-tmp/s3x/sync.log` (counts and codes only).

```sh
nohup ~/.agent-tmp/s3x/run.sh >/dev/null 2>&1 &   # waits on ~/.agent-tmp/s3x/creds.fifo
# then, one approval prompt on kjg-lap, values never echoed:
ssh kjg-lap "zsh -lc 'jq -n --argjson w \"\$(op item get <exporter item> --vault 96Bulls-prod --format json)\" \
  --argjson e \"\$(op item get <eraser item> --vault 96Bulls-prod --format json)\" ...'" > ~/.agent-tmp/s3x/creds.fifo
touch ~/.agent-tmp/s3x/STOP   # stop after the current run

# The erasure job (its own key, its own process). Credentials ONLY on stdin, held in RAM:
# one JSON object {"databaseUrl": ..., "eraser": {accessKeyId, secretAccessKey[, sessionToken]}}.
# databaseUrl must carry user, non-empty password, host and ?sslmode=verify-full (never ~/.pgpass or PGSSL*).
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

Dry run (reads the authority, builds everything, erases nothing): run `main.ts`
without `--execute`; it needs only `databaseUrl` on stdin. It does open the state
file at `S3X_STATE` (creating it if absent, and with `S3X_OBLIGATIONS=on`
initializing the obligation schema and an unattested genesis there); point
`S3X_STATE` at a scratch copy for a dry run that must leave the real file untouched.

`S3X_OBLIGATIONS` (default OFF; set in BOTH the exporter's and the eraser's
environment) gates the NEW exporter-obligation protocol: its schema and genesis on
open, the exporter's invocation recording and claims, reservations and the
obligation-gated completion. The strict canonical paged eraser (bounded, resumable,
done only after a strict zero inventory) runs in BOTH modes as safety hardening.
A state file whose obligation state was used refuses to open with the flag off
(`obligations_state_in_use`): turning it off never drops tracked unknowns.

Completion: in BOTH modes a session prefix is erased in bounded, resumable steps
(checkpoint in the state file), done only after a fresh strict zero inventory.
With ON, every doomed version is also reserved first, and an exporter item would
additionally need no unresolved exporter PUT under it; under the always-unattested
ON genesis it is never marked done. With OFF it is marked done after that zero proof
(no obligation protocol). A single object (an orphan, a staged batch, a pointer)
is erased and verified in the shared primitive's exact-key mode, so it completes
however many other versions share its directory, except a live pointer, which stays
until a strict proof also succeeds (> 10k kept versions in the machine prefix keep it
pending: `erasure_verify_pending`; nothing outside the session prefix is deleted).
With ON, a PUT whose outcome is unknown (timeout,
crash, lost response) keeps the prefix queued indefinitely, re-swept on the
bounded backoff: `erasure_write_unresolved` in the queue is expected, not a
failure to clear by hand.

## E3 erasure: strict eraser (both modes)

The strict canonical paged eraser (`src/migration/neon-s3-erasure/eraser.ts`,
shared primitive 9fef0acd) runs with `S3X_OBLIGATIONS` OFF and ON. Every
destructive route uses it, with no permissive fallback:
- queued session prefixes and queued single objects (exact-key mode);
- live-fenced session directories (every date);
- live pointers and staged batches (exact-key);
- machine prefixes with more than 10k versions (bounded per run, resumed from
  `erase_checkpoints`).

Each item is done only after a fresh strict zero inventory. ON additionally
requires the obligation protocol. Everything below about invocations, claims,
reservations, the retirement cursor and genesis applies to ON only.

| code (queue `error` / log) | meaning | queue | operator action |
|---|---|---|---|
| `erasure_refused_shape` | key outside a session prefix of its own org | retried, never deleted | investigate the enqueuer |
| `erasure_lease_lost` / `erasure_item_cancelled` | the lease was taken, or the exporter cancelled the item | stays / removed | none |
| `erasure_inventory` (initial) | a strict listing page failed before THIS page's deletes (earlier pages of a bounded run may already be deleted) | stays, retried | none (provider/listing issue) |
| `erasure_inventory` / pending verify (post-delete) | deletes MAY have happened, then verification failed or the inventory changed; pending is NOT a rollback | stays, retried | none; the next run re-proves |
| `erasure_verify_pending` (log `live erasure unverified`, with the group ids and `reason` = the primitive's own pending reason: `budget` or `remaining`) | the live session is NOT complete: its pointer waits for a fresh strict proof that did not succeed this pass (`budget`: e.g. > 10k kept versions share the machine prefix; `remaining`: selected versions are still listed). Observability only, not a waiver | tombstone re-swept every pass; never claimed | none; it can complete only once a later strict proof succeeds |
| `erasure_reservation_refused` (ON) | a writer changed the prefix, or a version is owned | stays, retried | none |
| `erasure_write_unresolved` (ON) | an exporter PUT has an unknown outcome | stays indefinitely (backoff) | none |
| `erasure_obligations_unattested` (ON) | genesis is unattested: completion is never claimed | stays indefinitely | none until a reviewed recovery contract |
| `erasure_done_refused` (ON) | the done claim's revision / lease / queue identity re-check failed | stays, retried | none |
| `obligations_state_in_use` | an OFF handle met used obligation state, or a pointer | process refuses | keep `S3X_OBLIGATIONS=on` for that state |
| `obligations_requires_fresh_state` | ON was given an existing SQLite state | process refuses | use a FRESH `S3X_STATE`; converting existing state is a reviewed offline contract |
| `obligations_state_conflict` / `obligations_pointer_invalid` | pointer/sibling collision, a leftover sibling / its `-wal` / `-shm` / pointer temp, `S3X_STATE` naming a sibling or temp file, permission, symlink or identity mismatch | process refuses; state quarantined | none without a reviewed, evidence-backed recovery |

**Mismatched modes (symptom).** The flag must match across the exporter and the eraser:
- **Eraser ON, exporter OFF:** they are on different state paths. The exporter's OFF file is refused by the ON eraser (`obligations_requires_fresh_state`), or the ON eraser bootstraps its own fresh pointer state that the exporter never writes. The exporter records no invocations, so an ON eraser never sees the exporter's PUTs. The symptom is queue items that never reach "done" and an exporter that refuses the pointer (`obligations_state_in_use`). Set the flag the same in both environments.
- **Exporter ON, eraser OFF:** the OFF eraser refuses the pointer at start.

**Prohibited recoveries:** do these never:
- no manual "done";
- no reservation clearing;
- no disabling strict checks;
- no cap escalation;
- no deleting, adopting or resetting a quarantined sibling or pointer;
- no downgrade of a used ON state to older code.

**ON state layout (fresh state only).**
- **Layout:** `S3X_STATE` holds a small 0600 JSON pointer, and the database is the fixed sibling `<S3X_STATE>.obligations.sqlite` (0600, same directory).
- **Bootstrap:** it happens only when `S3X_STATE` is absent, before any external effect. The commit is checkpointed and the connection closed before the fsync. The pointer is published with a no-replace link, then the directory is fsynced.
- **Pre-obligation code** (main 341dd5a9) fails at open on the pointer. Its eraser makes no external call first. Its exporter makes only its read-only preflight (S3 list + head, with `--execute`), never a PUT or DELETE.
- **Downgrade:** it is unsupported for used state, and enforced by that open failure.
- **Dry run:** an ON dry run needs its own fresh scratch `S3X_STATE`.
- **Scope:** these guarantees are stated for the tested local storage, not network filesystems.

## Later

Move to a scheduled Worker or a 96Bulls service in us-east-1 (the transcript
store runbook asks for an in-region host): the loop is already
bounded per group and resumable from its watermark; the watermark moves from
SQLite to a Postgres table.
