# Operator session erase: self-host byte eraser

Lane C of the operator per-session hard delete. This is the piece that removes
a session's **bytes** from the self-host object store. The orchestrator (lane D)
decides when it runs and supplies its input. The SQL side (tombstones and
fences) is lane B.

- Library: `src/session-erase/selfhost-eraser.ts` (`eraseSessionBytes`, `planTargets`)
- Entry: `src/session-erase/selfhost-eraser-main.ts` (`runSessionEraseBytes`),
  dispatched as `SERVICE=session-erase-bytes` (a one-shot Job, see
  `docs/selfhost-composition.md`)
- Tests: `src/session-erase/__tests__/selfhost-eraser.test.ts`

The automatic orchestrator can use `eraseDiscoveredSessionBytes(eraser, key, options)`.
This scans strict version inventories for all machines and dates in the requested
workspace and the organization's upload generations, selecting only that exact
source and session. It finds uploads whose manifest event has not been indexed,
old date directories and publication generations without reading transcript
objects. The S3 role still needs only version listing and version deletion.

The caller must commit the session fence and drain outstanding writes first.
Discovery refuses identifiers changed by the legacy, lossy layout encoder
(`discovery_key_ambiguous`); it must never erase a neighbour that maps to the same
directory. An incomplete discovery or verification scan remains pending. Large
inventories use bounded continuations of the shared strict paged primitive,
including a fresh pass with deletion disabled to prove zero matching versions.
The existing location-based CLI remains available with its original scope and
staged-object age rules. Neither library entry enables itself; the automatic
caller must enforce `SESSION_ERASE_BYTES_ENABLED=1`.

Default off. `SESSION_ERASE_BYTES_ENABLED` must be exactly `1`. Unset, empty or
`0` returns `{"enabled":false}` before any config, credential or request is
read and before the store is touched. Any other value (including `true`) fails
closed with `session_erase_flag_invalid`. Nothing is deployed.

## Completion means bytes gone

A successful run reports `verified: true` only when a fresh, strict listing of
every target finds **zero versions and zero delete markers**. A traversal, a
checkpoint or a delete count is never proof. The orchestrator should mark an
erase complete only on `verified: true`. Anything else (a late write, staged
versions still inside the redrive window, a run budget exhausted) means "run
again later", not "done".

The SQL selection erase (`enabled:false` in `docs/selfhost-composition.md`
section 3) makes a session unreadable. It does not remove bytes. This eraser
does.

## What it deletes

Every version and every delete marker, by version id, of:

| Target | Key | Mode |
| --- | --- | --- |
| Transcript directory, per location and date | `{keyRoot}/{org}/v1/ws={ws}/m={machine}/h={harness}/d=yyyy/mm/dd/s={sid}/` (manifest, `raw/`, anything else under it) | prefix |
| Live location pointer, per location | `{keyRoot}/{org}/live-sessions/v1/ws={ws}/m={machine}/h={harness}/s={sid}.json` | exact key, erased **last** |
| Staged batch objects supplied by the caller | `{keyRoot}/{org}/live-batches/v1/ws={ws}/m=srv_…/b=….staged.ndjson.zst` | exact key |

`ws`, `harness` and `sid` are `layoutComponent` of the erase key's ids
(`src/shared/layout-component.ts`). Through `layoutComponent` two raw ids can
share one physical directory (for example `a:b` and `a_b` both map to `a_b`).
Erasing one of them erases that shared directory. This is inherent to the
storage layout.

Order: staged objects and transcript directories first, then the pointers, then
the verification listing. Keeping the pointer until last does **not** stop a
writer from re-creating bytes: a writer that finds its pointer writes into the
`d=` directory the pointer names. The benefit is where a re-creation lands. With
the pointer still present, it lands in a planned directory, so verify catches it
(`verified: false`) and a rerun erases it. Without the pointer, a writer would
create a fresh pointer and a directory at a new `d=` that may be outside the
plan. Pointers are erased only when every other target was fully traversed in
the same run **and** no staged version is still pending inside the redrive
window. Otherwise they stay for the rerun and are counted in
`deferredPointerTargets`.

It never deletes anything else:

- `planTargets` computes the allow-list from the request and refuses (with a
  code and no value) any location that is not this key's session, a
  `manifestKey` that is not that location's directory, a staged key that is not
  a `live-batches` staged object of one of the request's `srv_` machines, and
  any `buffered-batches` key.
- Before every delete, every doomed key is checked against the allow-list
  (`assertAllowed`): it must lie inside one of the planned transcript
  directories, or be one of the planned exact keys. This is in addition to the
  shared primitive's own per-page prefix check (`erasure_foreign_key`).
- Directory prefixes end in `/`, so `s=abc/` never matches `s=abcd/`, and
  exact-key mode never matches `s=abc.json.bak`.

## Inputs

```ts
eraseSessionBytes(eraser: PagedEraser, request: SessionEraseBytesRequest, options)

SessionEraseBytesRequest = {
  key: { orgId, workspaceId, source, sessionId },   // canonical (pre-encoding) ids;
                                                    // workspaceId as stored on publication rows
  locations: SessionEraseLocation[],                // 1..64, one per session_index row
  stagedKeys?: string[],                            // 0..1000, attributed by the caller
}
SessionEraseLocation = {                            // a session_index row's columns
  workspaceId, machineId, harness, sessionId,       // the row's values; each must equal
                                                    // layoutComponent of the key's id
  manifestKey?: string | null,                      // row.manifest_key: fixes d= (required
                                                    // unless the row has none)
  dates?: string[],                                 // yyyy-mm-dd, extra dates to cover
  storageProvider?: string | null,                  // row.storage_provider: s3 | r2 only
  bucket?: string | null,                           // row.bucket: must equal the eraser's
}
options = { keyRoot, bucket, dryRun?, concurrency? (1..16, default 4),
            maxRunsPerTarget? (default 50), stagedMinAgeMs? (default 48 h),
            authorize?, now?, log? }
```

Rows store the canonical workspace id (`tenant.workspaceId`). For a
layout-safe id (`[A-Za-z0-9._-]`, not starting with `.`) the canonical and
physical values are identical. Any other id does not match its layout-encoded
form and is refused (`location_not_this_session`), which fails closed.

**Lane D must have fenced the session (lane B) before it starts the Job.** The
process entry does not re-check lane B's tombstone or fence before each page
(`authorize` is not wired), so a write that is not fenced can land after the
verification listing.

Pass every `session_index` row of the session, for every machine and every
user, including tombstoned ones (`deleted_at` set), **except** rows in the
upload-generation layout (below). A row not passed is not erased. Each location
needs at least one date source. `manifestKey` is required unless the row has
none: it is the row's own key, and it binds the key root and org (a wrong
`SESSION_ERASE_KEY_ROOT` or org is refused as `manifest_key_mismatch`). A
location with only `dates` skips that cross-check, so a wrong key root, org or
date finds nothing and still reports `verified: true`.

**Publication-generation rows are excluded.** A `session_index` row written by
the self-host `session-publication` service has a `manifest_key` under the
upload-generation layout:
`{keyRoot}/{org}/upload-generations/g=<uuid>/v1/ws=…/m=…/h=…/d=…/s={sid}/publications/<uuid>/manifest.json`.
This eraser does not erase publication generations, which are a separate step.
Such a row is refused with `location_publication_generation`, and the whole
request is refused with it. Lane D must exclude these rows from this eraser's
request and track that session's publication-generation bytes as **not
erased**. The result's `verified: true` covers only the targets planned from
the request. It says nothing about rows that were not passed, and that includes
publication generations.

**Staged keys come from the orchestrator.** A staged key carries no session id.
Attributing a staged object to a session needs either the DB ledger or a read
of the object's header line (`groups: [source, sessionId][]`). The eraser key
holds no `GetObject` and the eraser does not read the DB, so lane D must
resolve and pass the list.

## Output

```ts
{ dryRun, transcriptObjects, pointerObjects, stagedObjects, versionsDeleted,
  stagedPendingVersions, incompleteTargets, deferredPointerTargets, verified }
```

Object counts are distinct keys, and `versionsDeleted` counts versions plus
delete markers. `deferredPointerTargets` counts the live pointers left in place
because a target was incomplete or a staged version is still pending. Any
deferred pointer makes `verified` false. In a dry run the same fields count
what would be deleted, `deferredPointerTargets` is 0, and `verified` is always
false. `verified: true` covers only the targets planned from this request (see
Inputs). The process entry prints this as one JSON line (`enabled: true`
added).

Exit codes (`exitCodeForResult` / `exitCodeForError` in the entry):

| Code | Meaning | Orchestrator action |
| --- | --- | --- |
| 0 | verified, disabled, or a dry run | done (verified only on `verified: true`) |
| 3 | ran to the end, not verified | rerun later |
| 2 | refused or misconfigured (`SessionEraseRefused`; stderr `error` is `session_erase_<code>`) | do not retry unchanged; fix the request or config |
| 4 | failed with any other error: S3 5xx or 403 (`s3_<status>`, e.g. object lock or legal hold), network, `erasure_*` inventory errors | the run may have deleted part of the session; rerun it |

Planning refusals happen before any listing or delete. The one refusal that can
follow deletes is the eraser's own guard (`key_outside_allowed_prefixes`),
which means a listing returned a key outside the plan. It needs a human, not a
blind retry.

## Verification

After the deletes, `verifyErased` runs on every target: a fresh, strict, full
listing from the start, which passes only when zero versions remain. Any
remaining version (a late write between delete and verify, a staged version
inside the redrive window), an exhausted verify budget, or a target whose
bounded runs ended early gives `verified: false`. The run is idempotent: a
second run deletes 0 and verifies true.

## The 48 h staged window

A staged object is the fan-out input of one upload batch, and **it can hold
many sessions' lines**. Until the fan-out has run, it is the only copy of the
other sessions' lines. The cloud eraser (`neon-s3-erasure/job.ts`,
`STAGED_ERASE_AFTER_MS`) therefore erases a staged object only once it is
older than the 48 h redrive window, and this eraser does the same. Versions
younger than `stagedMinAgeMs` (default 48 h, pinned to the cloud constant by a
test) are kept, counted in `stagedPendingVersions`, and make `verified` false.
Rerun after the window. Erasing a staged object erases the whole object,
including other sessions' lines, which by then have been fanned out into their
own directories.

`buffered-batches` objects are never deleted here. The buffered-GC redaction
protocol owns them (`docs/buffered-upload-gc.md`), and a direct delete would
break its barrier.

## Limits

- **Versioned S3-compatible stores only** (AWS S3, MinIO, R2). It uses the
  shared strict paged primitive (`createS3Eraser` in
  `src/migration/neon-s3-erasure/eraser.ts`): ListObjectVersions plus
  DeleteObject by versionId, signed with SigV4. GCS, Azure and `fs` locations
  are refused (`store_not_versioned_s3`). `storeEraser` exists for tests and is
  not wired.
- **Bounded runs:** each target gets at most `maxRunsPerTarget` runs of at most
  10 000 versions or 100 pages each. A larger target ends with
  `incompleteTargets > 0` and `verified: false`. A rerun relists from the start,
  which is idempotent.
- **Concurrency:** at most `concurrency` targets in flight (1..16).
- **Credentials:** a separate eraser key (ListBucketVersions and
  DeleteObjectVersion on the key root), never the ingest writer's key, read
  from env or a mounted `_FILE`. Creating that key or policy is an IAM change
  and needs explicit owner approval. Nothing here creates it.
- **Not covered:** a row not passed (another user's copy, an exporter machine
  whose row is missing); a date not given by `manifestKey` or `dates`;
  publication and derived copies (search index, publication generations,
  whose rows are refused with `location_publication_generation`);
  replica batch descriptors under `live-batches/v1/ws=…/o=…/`, which are a
  cloud transition artifact; anything written after the erase and not fenced
  by lane B.
- **Logs:** one line per run with counts and codes only. It never contains a
  key, an org, a workspace, a machine or a session id.
