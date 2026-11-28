# T139: authority-issued incarnation binding (amendment proposal to decision 0002)

Status: PROPOSAL, revision 2 (answers Pipeline's design HOLD at 8aba8f82; see "Revision 2" below, which supersedes sections 1 and 3 where they differ). For review by layered-sessions-pipeline. Code only, offline. No migration is applied and no slot
is reserved (`20261105062360` belongs to erasure). Decision 0002 stays enforced in the existing code until this
amendment is accepted.

## Problem

A session whose upload is deleted by an ordinary (permitted) delete, then re-created, can reuse its revision
numbers. Revision numbers restart in the writer when the manifest no longer exists
(`src/session-ingest/writer.ts nextManifest`).

Two revision-only fences then keep the re-created session invisible to an authorized caller:

1. **Pointer row.** `sessions.upsert_session_index` applies a row only `WHERE excluded.revision > s.revision`
   (20261025000300). A tombstone keeps its revision, so a re-create at revision <= N is `unchanged`.
2. **Directory resurrection fence**, decision 0002 (20261031001000, `session_directory_resurrection_fences`).
   It records each machine's highest revision at tombstone time. `projectSessionDirectory` resurrects only for
   a live pointer whose revision is strictly above it. The search pass then cannot write directory events
   (`directory_event_sources_changed`), and the authorized read stays empty.

Required acceptance (Skip, Pipeline):
- After an ordinary delete, the re-create event itself makes the new marker searchable to an AUTHORIZED caller,
  at the reused revision 1, on both the standard and FIFO queue paths.
- That still holds after delayed predecessor (pre-delete) work resumes.
- A permanent operator erase still refuses the same sequence.

## Why not the earlier candidates

- **Relaxing the revision fence on an ETag difference** (b8b70e27 WIP) is insufficient. An identical-byte
  re-create can reuse the ETag. A delete of the current S3 version can expose older bytes with their own ETag. And
  an ETag says nothing about which incarnation is authoritative.
- **A monotonic writer** (re-created manifests continue above the tombstoned revision) would change the external
  contract: acceptance requires revision 1. It needs its own contract decision and is not proposed here.

## Proposal

### 1. Incarnation is issued by the authority that permits re-creation

The ordinary-delete and re-enable authority is the ingest's selection path:
- `sessions.upload_deletions`, written on deselection or delete;
- re-enablement through `sessions.ingest_set_session_uploads` / `ingest_apply_selection`.

Add a per-logical-session incarnation counter, owned by the same authority:

    sessions.session_incarnations(org_id, workspace_id, source, session_id,
                                  incarnation bigint NOT NULL,          -- current live incarnation
                                  predecessor_deleted_at timestamptz,   -- receipt of the predecessor's deletion
                                  PRIMARY KEY (org_id, workspace_id, source, session_id))

Rules:
- **Default 0.** A session with no row is incarnation 0, which covers every existing session and every legacy
  writer.
- **Deletion receipt.** An ordinary delete of incarnation N records `predecessor_deleted_at` for N, in the same
  transaction as its `upload_deletions` write.
- **Issue N+1.** The authority issues N+1 only when re-creation is permitted: a recorded predecessor deletion
  exists, and there is no permanent erasure fence (`session_erasure_fences`). It does this in the ingest claim
  that admits the first batch of the re-created session. The writer stamps it on the new manifest
  (`extensions["96bulls"].incarnation`) when it creates the manifest (`ifNoneMatch`).
- **Never lowered or reused.** Only the authority moves it, and only upward.

### 2. Pointer rows and resurrection fences compare `(incarnation, revision)`

- `session_index` and `session_directory_resurrection_fences` gain `incarnation bigint NOT NULL DEFAULT 0`.
- `upsert_session_index` applies when `(excluded.incarnation, excluded.revision) > (s.incarnation, s.revision)`.
  This is lexicographic, so a higher incarnation wins at ANY revision, including a reused revision 1.
- `projectSessionDirectory` resurrects for a live pointer whose `(incarnation, revision)` is above its
  machine's fence.
- **Decision 0002, amended:** "a deleted upload never comes back" holds per incarnation. The deleted
  incarnation N never returns, whether through a re-sent event, a stale read or an exposed predecessor version.
  A new incarnation is a new upload, admitted only by the authority.

### 3. Guarded commit-time currency check

Inside the publication transaction, after the upsert statement and before commit, the pointer stage verifies
two things, and refuses with `pointer_not_current` (retryable, rolled back) if either fails:
- **(a)** the object it read is still the live object: a HEAD of versionId, else ETag, as in b8b70e27's
  `IndexDeps.reincarnation.stamp`;
- **(b)** the manifest's incarnation equals the authority's CURRENT incarnation for the session, read
  `FOR SHARE` in the same transaction.

So delayed predecessor work, which read incarnation N before the delete, cannot publish, even when its row
comparison would pass in a race.

### 4. What stays exactly as today

- **Permanent operator erase.** `session_erasure_fences` with the `ingest_erase_fence_index_guard` trigger, and
  the operator runtime upsert. The authority never issues a new incarnation for a fenced session, and the guard
  suppresses any live row whatever its incarnation.
- **Incarnation-0 sessions** (all existing data and legacy writers) behave byte-for-byte as now, since equal
  incarnations fall back to the revision comparison.
- **Delete of the current S3 version exposing older bytes.** The older manifest carries an older or no
  incarnation, so it stays refused, and the semantics case is explicit.

## Revision 2: one serialization point, active authority at commit, single-use receipts

Pipeline's HOLD at 8aba8f82 asked for three things. This revision answers each one.

### R1. One serialization point shared by deletion, reissue and publication, even with no legacy row

A row lock cannot serialize a session that has no row yet. That covers every incarnation-0 session, and a
publication racing the FIRST deletion. So the serialization point is a transaction-scoped advisory lock on the
LOGICAL session, independent of any row:

    k = hashtextextended('96bulls:incarnation' || E'\x1f' || org_id || E'\x1f' || workspace_id
                         || E'\x1f' || source || E'\x1f' || session_id, 0)

- **Ordinary deletion** (`ingest_apply_selection` with p_enabled = false, `ingest_apply_reset`): takes
  `pg_advisory_xact_lock(k)` (EXCLUSIVE) for each affected session, in a stable (sorted) order to avoid deadlock,
  before writing `upload_deletions`.
- **Reissue** (the ingest claim that admits the first batch after re-enable): takes `pg_advisory_xact_lock(k)`
  (EXCLUSIVE).
- **Publication** (the pointer upsert transaction, `upsert_session_index` and the move path): takes
  `pg_advisory_xact_lock_shared(k)` (SHARED). Publications of one session still serialize on the existing pointer
  lock (`scopeLockKey`, `96bulls:pointer`); the shared lock only excludes deletion and reissue.

Every party takes `k` inside its own transaction and holds it to commit. A deletion therefore commits either
entirely before a publication's check or entirely after its commit, never in between.

Lock order, fixed for every path: `96bulls:pointer` scope lock, then `k`, then any row lock.
Deletion and reissue never take the pointer lock, so they cannot deadlock with publication.

### R2. Active authority checked at commit

`sessions.session_incarnations(org_id, workspace_id, source, session_id, incarnation bigint NOT NULL,
PRIMARY KEY (...))`. A missing row means incarnation 0.

An incarnation N is **active** when no deletion receipt for N exists, and none was consumed by a reissue to N+1.
Under `k` (shared), immediately before commit, publication requires all of the following, or it refuses with
`pointer_not_current`, which is retryable and rolled back:

1. the manifest's incarnation (`extensions["96bulls"].incarnation`, absent means 0) equals the CURRENT
   incarnation (missing row means 0);
2. that incarnation is active: there is no `session_deletion_receipts` row for (session, N);
3. no `session_erasure_fences` row (permanent erase; unchanged, and also enforced by
   `ingest_erase_fence_index_guard`);
4. the object read is still the live object (HEAD by versionId, else ETag): the existing currency check.

A pass that read incarnation N before the delete therefore refuses at commit. Its check runs after the delete
committed (the delete held `k` exclusively), and it sees N's receipt. Delayed predecessor work that resumes after
the reissue sees current = N+1 and refuses on check 1.

### R3. Single-use consumption of the ordinary-deletion receipt

`sessions.session_deletion_receipts(org_id, workspace_id, source, session_id, incarnation bigint, receipt_id uuid,
deleted_at, consumed_by bigint NULL, PRIMARY KEY (org_id, workspace_id, source, session_id, incarnation))`.

- Deletion of incarnation N (under `k`, EXCLUSIVE) inserts the receipt for (session, N) with consumed_by NULL,
  ON CONFLICT DO NOTHING. A second deletion of the same N is idempotent: one receipt.
- Reissue (under `k`, EXCLUSIVE) does `UPDATE ... SET consumed_by = N+1 WHERE incarnation = N AND consumed_by IS
  NULL RETURNING`, and sets `session_incarnations.incarnation = N+1` only if exactly one row returned. A
  concurrent or replayed reissue finds the receipt consumed and issues nothing. One receipt gives at most one new
  incarnation.
- Reissue also refuses when an erasure fence exists. Permanent erase never yields a receipt that can be consumed.
- Re-enable (`ingest_apply_selection` with p_enabled = true) deletes the `upload_deletions` row as today. It does
  NOT touch receipts. The receipt is consumed only by the reissue that admits the first re-created batch.

### R4. Fixtures added by revision 2 (offline, inline schema delta in PGlite, both queue paths)

8. **Stale publication while the replacement's delivery is withheld:** a pass reads incarnation N and pauses at
   commit; delete N; reissue N+1; B is written, but B's notification is held back. The paused pass resumes and
   refuses. An authorized read is empty (the tombstone holds). Then B's notification is released and the authorized
   read returns B's marker at revision 1.
9. **Authorized revision-1 reads before and after old work resumes:** the parsed read-API response after B
   publishes, then again after the paused incarnation-N work resumes and refuses. Both return B, never A. Both
   queue paths.
10. **Permanent-erase denial:** the same sequence with an erasure fence. Reissue refuses, so there is no N+1; the
    pointer guard suppresses any row; the authorized read is empty before and after old work resumes.
11. **No-row race:** an incarnation-0 session (no session_incarnations row) has its first deletion race a
    publication. Either the publication commits first and then the delete tombstones it, or the delete commits
    first and the publication refuses. Never a live row after the delete.
12. **Receipt single use:** two concurrent reissues for one receipt give exactly one N+1, and the other issues
    nothing.

Open for review in revision 2:
- The key for `k`: (org, workspace, source, session) versus upload_deletions' (org, workspace, user, origin, source,
  session). Can one logical session span two users or origins? If it can, deletion locks the logical key, and the
  receipt records the deleting user and origin.
- Reset (`ingest_apply_reset`) deletes many sessions in one statement. Sorted lock acquisition keeps it
  deadlock-free; the cost is one advisory lock per session.

## Measurement (T139 sizing)

- **Refused attempts:** a structured `pointer fence refused live write` line carrying both `(incarnation,
  revision)` pairs, `stored_deleted`, and `incarnation_sha256` = a scoped digest over namespace, key and
  versionId, JSON-encoded. Raw keys never appear in logs; the digest is never a metric dimension. When the
  authoritative version is absent, the record stays unmatched.
- **Distinct candidates:** deduplicated by logical session plus that digest.
- **Confirmed invisible re-uploads:** require a post-delete incarnation that the authority confirmed, plus a
  timestamped missing pointer or an authorized-read miss for the expected marker. Pointer absence and caller
  absence are recorded separately.

## Offline fixture plan (no migration slot)

The test applies the schema and function deltas inline in PGlite over every existing migration. Each scenario is
run on the standard path (processBatch) and on the FIFO ordered path (on the T126 branch), with the authorized
read through the read API over the registered Postgres projection, as in pointer-reincarnation.pglite.test.ts:

1. **Ordinary re-create.** Ordinary delete, then the authority issues incarnation 1, then a re-create at
   revision 1. The re-create event alone makes the new marker searchable; `deleted_at` is null and the old
   marker is absent.
2. **Paused predecessor work.** A pass reads incarnation 0, pauses inside the lock across the delete and the
   re-create, then resumes. Its commit is refused by check (b); the new marker stays visible before and after.
3. **Stale read at the same incarnation.** Paused after its read while the object is replaced: refused by
   check (a); the retry publishes.
4. **Permanent operator erase.** The same sequence after an erasure fence: the authority refuses issuance, the
   guard suppresses the row, and the authorized read is empty.
5. **Predecessor version exposed** by deleting the current version: refused, and the marker is not revived.
6. **Identical-byte re-create:** the new incarnation wins regardless of the ETag.
7. **Incarnation-0 control:** unchanged behaviour (the existing fence goldens).

Open for review:
- the exact claim RPC that issues the incarnation (`ingest_claim_batch_mode` returns it);
- whether relayhistory-api's legacy writer path needs the stamp (it does not: it stays at incarnation 0, as
  today);
- the migration slot (Pipeline coordinates, once accepted).
