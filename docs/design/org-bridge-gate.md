# Org bridge gate: the S3 writer fence through a committed bridge_complete

Status: milestone 1 (code only, nothing wired to a workflow mode). Branch `review/s3-admission-fence-bridge`.
Owner: sessions-s3-admission-fence. Review: layered-sessions-pipeline. Qualification belongs to sessions-hold-test-harness.

## The defect

The synthetic cutover finding `s3_admits_before_bridge_complete` showed the problem. The S3 consumer
(`ledger-s3.ts` `requireFenceAuthority`) admits an org as soon as its per-org fence-authority marker exists. That
happens before the drain, before the erasure replay and before the 062320 hold's `bridge_complete`. The SQL hold does
not bind the S3 consumer, so the direct writer and the fallback writer could both land for one org.

## Writers of one cut-over org O (hold epoch E)

| Class | Writer | Fenced by |
|---|---|---|
| direct | S3 consumer: ledger-s3 `claim`, then `commit` (direct-to-S3 inbox uploads) | this gate: the bridge-complete record |
| fallback | Worker HTTP admission into SQL | the 062320 hold (install to release), then `workerHttpAdmission: "off"` (config, see open item B) |
| fallback | buffered drain (`buffered-drain.ts`, the backlog into the canonical layout) | this gate (refuses once the record exists) plus the drain's run lease |

## Invariant (milestones 1 and 2A)

- An org O is **enrolled** at generation G by a CAS-versioned S3 object, `.ledger/fence-authority/.enrolled/<org>.json`.
  It is bound to sha256 of the hold install's request id and written by `enrollOrg` BEFORE the SQL install. The
  install tool calls `enrollmentFor(org, request)` right before `_install`.
- While O is enrolled, no direct claim admits and no direct commit reaches a re-arm marker, an index merge or
  pending -> committed, unless both of these hold:
  - O's per-org marker is present;
  - the create-only bridge-complete record `.ledger/fence-authority/.bridge/<org>/g<G>.json` binds O, G, the
    enrolled request and the marker's exact bytes.
- The deployment-wide marker and a warm positive cache do not count. Enrollment and the bridge are read uncached on
  every claim and commit. The STOP still refuses new claims first, and an org that is not enrolled is unchanged.
- The record exists only after slot 062320's status shows the hold of G's request released with `bridge_complete`,
  and only once the marker has landed.
- Once the record exists, the drain writes no new canonical byte for O and merges no index entry for O.

Both objects sit under a dot directory of `fence-authority/`, so no tenant segment can collide with them, and the
consumer's read grant already covers them. Records carry hashes only.

**Trusted enrollment source.** Operators write the `fence-authority/` prefix and the consumers only read it, the
same trust as the marker. Enrollment is effective for new claims from its PUT, because every claim reads it
uncached. It does NOT retire a writer that was admitted or in flight before the PUT. A claim that read "not
enrolled" and then paused can still write canonical bytes. Its commit then refuses (see the commit-resume test), but
those bytes are not retracted. That is item C, so **A alone does not qualify install**.

**Recovery** (`withdrawEnrollment`, CAS to `withdrawn`; the markers decide again):
- *Enrollment succeeded, SQL install failed.* Allowed only while the status shows no hold of that request. The next
  attempt enrolls G+1 with a NEW request id. If the old install commits late, it creates a hold no enrollment covers:
  `completeOrgBridge` refuses it (`bridge_request_mismatch`), and its only exit is `verified_rollback`. Marker
  publication must also require `enrollmentFor` (sessions-wave-tooling).
- *Verified rollback.* Allowed only when the latest hold is that request's, released with `verified_rollback`, which
  means no marker was ever attempted. The order is: SQL release first, then withdraw. In between, SQL is open and S3
  is fenced, so there is a single writer.
- *Install before enrollment.* `enrollOrg` refuses (`enrollment_after_install`). Recovery is `verified_rollback` and
  a new request.
- *A later cutover of O.* It takes G+1, allowed from `withdrawn` or once G's record exists. G's bytes sit under G's
  key and bind G's request, so they never satisfy G+1 (tested). An org enrolled at G+1 is fenced again at once.

## Pieces

- `ledger-s3.ts` `orgBridge: "required"`. The hosted Lambda (`upload-admission-hosted.ts`) always sets it, and the
  self-host/ingest-edge server sets it with `INGEST_ORG_BRIDGE=required`. `enrolledGate` runs before the positive
  cache on every claim, and at commit before any effect and again before pending -> committed. While the marker or
  the record is missing the answer is `ledger_fence_authority_bridge_pending`. When something does not bind it is
  `ledger_fence_authority_bridge_invalid` or `ledger_fence_authority_enrollment_invalid`. Under the STOP, a claim
  answers `ledger_fence_authority_stopped`.
- `org-bridge-gate.ts`:
  - `enrollOrg`, `enrollmentFor` and `withdrawEnrollment`.
  - `completeOrgBridge`: checks the enrollment, the committed release of its request, and the marker's bytes. It then
    adopts an identical record or treats any other as a conflict, takes the drain's own run lease, and makes a
    create-only PUT with a read-back.
- `buffered-drain.ts` `assertBridgeOpen`: uncached. It reads the enrollment and HEADs G's record before a batch's
  first canonical byte and again before its index merge. The outcomes are `bridge_complete_drain_refused` and
  `bridge_state_unreadable`.

## Proven (focused suites; regression tests fail with the gate disabled)

- `org-bridge-gate.test.ts` (19 tests; 9 fail with `enrolledGate` disabled):
  - an enrolled org with no marker refuses with a valid global marker and a warm cache, while the unrelated org stays
    open;
  - the STOP keeps precedence;
  - records and enrollments that do not bind;
  - a claim acquired before install, committed while held: stays pending with no index entry, then commits after
    the record;
  - self-host unchanged; withdrawn or unenrolled unchanged;
  - enrollment idempotence, request binding, install-before-enrollment, install-failed withdrawal plus a late old
    install, the verified-rollback withdrawal, a later generation against old bytes;
  - both commit orders, a crash between R and P, an ambiguous PUT, the hold refusals, a conflict, drain and export
    leases, store errors.
- `buffered-drain.test.ts` (appended block):
  - a record before the run;
  - the switch after the cached authority read;
  - a drain paused across the switch after its bytes landed (no merge, not marked);
  - an unreadable enrollment;
  - an old generation's record leaving G+1 open;
  - control.
- `upload-admission-s3-ledger.test.ts`: the hosted Lambda, enrolled, retries at the marker and admits after the record.

**Readable bytes today (input to C).** A direct claim admitted before enrollment, or a drain batch paused across P,
can leave canonical chunk bytes and manifest entries that no index entry covers. The tests pin that the index merge,
the committed state and the drained marker are withheld. The bytes themselves are not retracted, and whether the
reader serves them unindexed is exactly what C must settle.

## B. SQL fallback after bridge_complete: caller inventory (design only, no slot)

Today slot 062320's `_check` returns as soon as O has no live hold, so after `bridge_complete` every class reopens.
The Worker's HTTP path stays closed only through `INGEST_HTTP_ADMISSION_STOPPED_ORGS`, which is configuration, not
enforcement. The table below covers the runtime-granted functions (`src/hoopdb/org-admission-hold-classes.ts`, 85),
for an org whose hold of the enrolled request released with `bridge_complete`:

| 062320 class | Functions | After bridge_complete | Why |
|---|---|---|---|
| admission | claim_batch (via claim_batch_mode), claim_buffered_batch, commit_batch, commit_buffered_batch, commit_revisions, fanout_fence, prepare_buffer_stage, the three origin_* wrappers, seed_proven_records | **close** | SQL is no longer the ledger for O. An admission here is content the S3 index and owner stubs never see, which is the dual-writer case. Retries count too: a Worker request that got RH042 while held, and retries after the release, must keep failing. commit_revisions takes a NULL batch id (no pending batch required), so closing the claims alone is not enough. The seeder imports revisions, and S3 seeding (seed-s3.ts) replaces it for O. |
| publication | publication_claim, publication_publish, history_plan_claim | **close** | They publish O's leftover SQL stages into canonical. The drain has already moved that backlog, so this is a second fallback writer beside the drain. |
| control | set_session_uploads, reset_uploaded_sessions, turn_register, selfhost_set_session_uploads | **keep, mirror-only** | Removal state must stay consistent: erasers still read SQL `upload_deletions`, and the mirror writes S3 first. Keep the sealed-state rule (only members of `bulls96_session_ingest_mirror`) permanently for O, rather than reopening to any caller. |
| erasure | operator_runtime_begin, operator_user_erase_begin | **keep** | Never refused. |
| unaffected | GC (buffer_gc_*), consent, privacy and source control, history, publication bookkeeping, seed_fence_baseline, seed_scope_* | **keep** | They write no admission content. GC redaction and erasure runtime steps must keep working. |

**bridge_complete vs verified_rollback.** After `verified_rollback`, everything reopens as today: the org is back on
SQL, and its enrollment is withdrawn after the release. After `bridge_complete`, admission and publication stay
closed for O for good. There is no rollback after a marker attempt (design 062320 section 6.1). A newer hold of O
does not lift it: the rule keys on "any hold of O ever released with `bridge_complete`". Cutting an org back from S3
is unmodelled and needs its own design.

**The release-to-projection gap, modelled.** Let R be the commit of `_release(bridge_complete)` and P the create-only
PUT of G's record.

| Interval | SQL writers of O | S3 direct writer of O | Drain |
|---|---|---|---|
| install to R | refused (hold) | refused (enrolled, no record) | runs (the bridge) |
| R to P, today | **open** (062320 reopens; only config closes HTTP) | refused | refused only once P exists; still open in this interval |
| R to P, with the proposed rule in R's transaction | refused (permanently) | refused | completeOrgBridge holds the drain lease |
| after P, today | **open**, so dual writer with S3 | open | refused |
| after P, with the rule | refused | open | refused |

With the rule, **the first point where exclusion and safe recovery both hold is R itself**. R closes SQL admission
and publication atomically with the release, and S3 stays fenced until P. A crash anywhere between R and P therefore
leaves zero writers for O (availability only), and the recovery is to rerun `completeOrgBridge`, which is idempotent.
No SQL action is needed. Without the rule there is no such point: from R on, SQL can admit beside the soon-to-open
S3 consumer.

**Placement (open, the lander's call).** Two options:
- a new slot that replaces `_check` with the "ever released `bridge_complete`" refusal, as a new non-retryable code
  so the Worker can answer "use direct upload";
- the same rule folded into 062320's successor body.

Either way the refusal must be read under the org key in `_check`'s second statement, as the hold is today. Then a
release (EXCLUSIVE) serializes with every in-flight admission, and no admission that read "no hold" can commit after R.

## C. Exclusion at the authoritative write (PROPOSAL; held by the lander, nothing built)

The SigV4 time-window proposal is withdrawn. A fresh signature bounds only when a request may start, not when its
body or effect lands, and presigned-URL expiry gives no commit-time guarantee for a header-signed PUT.

**Where readability is decided today.** The read API (`src/session-read-api/canonical-projection.ts`) serves session
manifests (`<org>/v1/.../s=…/manifest.json`, writer.ts) and the chunks they list. It never consults the ledger's
batch state, the revision index or a drained marker. So the authoritative write is each writer's manifest CAS
(create-only for a new session or date, If-Match otherwise). A stale writer's chunk becomes readable the moment its
manifest append lands, whatever happens to its index merge or commit afterwards. Milestones 1 and 2A withhold only
the index merge, the committed state and the drained marker: **those bytes stay readable**.

No per-org object is CASed by every write. A manifest key is per session and date, and the location pointer is per
session, so a stale writer creating a never-seen session or date cannot be stopped by stamping the objects that
exist. Two protocols satisfy the requirement:

**W2 (recommended): readers serve only committed effects; the switch supersedes atomically.**
1. Every manifest entry already names its batch digest (`relayhistory_live.batches`). Readers of an ENROLLED org
   serve an entry only when its batch is committed: for direct writes the ledger batch record is `committed`, and for
   drain writes the staged object's `drained` marker exists. Unenrolled orgs read as today. The authoritative write
   is then one single-object CAS per effect: pending -> committed on the batch record, or the create-only drained
   marker.
2. The switch (enrollment effective, or the bridge record) proceeds only after it has resolved every unresolved
   effect of O, durably:
   - each `pending` batch record of O is CASed to `superseded_by_switch`, so the stale writer's If-Match
     pending -> committed fails atomically (`ledger_attempt_superseded`);
   - each undrained staged object of O gets a create-only `drained` marker with outcome `switch_abandoned`, so the
     stale drain's create-only marker fails.
   A lost ACK is reconciled by re-reading the object: committed means it counts, superseded or abandoned means it
   never counts. A crash leaves the switch unwritten, so it fails closed and a rerun resumes the sweep.
3. Superseded and abandoned bytes are unreadable by rule 1, and the re-arm sweeper erases them (a re-arm marker is
   written for each one).
   - Cost: a reader change in canonical-projection, owned by the read-API lane, plus one batch-record or marker read
     per entry for enrolled orgs (cacheable once committed, since committed is terminal).
   - The sweep lists O's batch records and staged objects. That is bounded per org and fine for wave 1.

**W1 (alternative): a durable writer registry with a switch barrier.** Each writer registers a create-only intent
under `.ledger/writers/<org>/<id>.json` BEFORE its first canonical byte, re-reads the switch after registering, and
deletes the intent only after its final durable effect. The switch writes itself first, then waits until every
intent of O is resolved. A crashed writer's intent never resolves by itself, so an operator or the sweeper must
supersede it. Without reader gating, that means retracting its manifest entries, and a manifest retraction races
the next appender. That is why W1 still needs W2's rule 1 to be correct, and why W2 is recommended.

**The installer (lander finding on a0308776).** An installer can pass `enrollmentFor`, pause, and commit `_install`
after an `install_failed` withdrawal has restored global-marker admission. Fix: the withdrawal consumes the request
id in SQL before touching S3.
- It calls `_install(org, request)` itself, which is idempotent: it returns the epoch if the paused installer already
  committed, and creates the hold otherwise.
- It then calls `_release(verified_rollback)` for that epoch, and only then CASes the enrollment to `withdrawn`.
- 062320 refuses any later `_install` of a released request ("request reused"), and request_id is UNIQUE. So the
  paused installer has irrevocably lost the ability to commit before admission can reopen, and `install_failed`
  collapses into `verified_rollback`.
Notes from sessions-wave-tooling:
- The flow leaves an installed -> released epoch with no seal, which hold-dry reads as installable.
- It takes the org key EXCLUSIVE briefly, so the org's concurrent admissions get RH042 meanwhile.
- Its `_install` skips the tool's post-key checks (pending batches, canonical history, active seed captures). If the
  flow stops between the two calls, the org is left held with no seal; a retried withdrawal or rollback-execute
  recovers it.
To prove: on a warm global marker, an installer paused between `enrollmentFor` and `_install` against a concurrent
withdrawal, asserting the readable bytes (W2 rule 1) and that no direct write of O lands between the withdrawal's
SQL step and its S3 CAS.

**B additions the lander asked for (inventory, no slot).**
- The 062390 lease versus preview: the seed scope capture is unaffected-class, and 062390 adds the admission check
  under the key. Its lease (a write) must close after `bridge_complete`, while its preview (read-only) stays open.
- Seed import (`seed_proven_records`): closes, as the table says.
- `verified_rollback` after an earlier `bridge_complete`: the closing rule keys on "any hold of O ever released with
  `bridge_complete`", so a later epoch's rollback reopens nothing in SQL. On S3, that epoch's enrollment G+1 is
  withdrawn and the consumer falls back to O's create-only per-org marker from G, so S3 stays the writer.
- Plan a new forward migration after the applied chain and slots are reconciled.
- R-to-P exclusion stays conditional on W2's sweep accounting for every outstanding effect.

**W2 read-side constraints (sessions-partial-canonical, reader lane T89 8a043736; no blocker claimed).**
1. Generation fence. The canonical cursor and the post-IO recheck bind `canonicalSourceGeneration`, a hash over
   pointer rows only. Committed-ness must either be folded into that generation or refuse with
   `source_generation_changed` (409) when it moves between pages. It must never silently continue.
2. No subset folds. An uncommitted entry makes the scope incomplete (partial, so the read falls back). It is never
   dropped, because dropping could serve an older revision of a record whose newer revision is uncommitted.
3. Grants and SQL. The reader reads as `bulls96_read_api` through `session_index` and `ingest_operator_runtime_erased`
   only. A commit check needs a new tenant-bound read and grant, a migration, and a role-verify update. An S3-only
   read of the ledger (batch records and drained markers) avoids the grant but costs IO.
4. Budget. 3000 ms per request (`READ_API_PARTIAL_CANONICAL_DEADLINE_MS`, max 3500; Cloud's timeout is 4 s). The
   check must be one batched lookup per scope, never one per chunk.
5. Operator fence ordering. The authoritative fence verdict comes before the read and after the IO, around any new
   ledger read, failed reads included. An uncommitted-entry refusal must never become a fallback-eligible 501 for a
   fenced session.
6. Unenrolled orgs: add a fixture proving the reader is byte-identical (compatible with the dormant flag).
7. Required tests (lander relay of the T89 owner, 252330578929815552): a commit between two pages while the pointer
   rows stay identical, which must 409 or be bound into the cursor and never continue silently; an uncommitted newer
   revision, where the scope is incomplete and the older revision is never served. The operator fence takes
   precedence on success and failure: 404 `session_fenced` and the terminal 503 `operator_fence_unavailable` win over
   any commit-check answer. Any reader diff goes to sessions-partial-canonical for review. This is design input, not
   a W2 verdict or a grant approval.

**W2 and erasure (agreed with sessions-buffered-drain-materialization; B3 list-then-CAS guard withdrawn).** For
enrolled orgs the drain's create-only `drained` marker is the visibility point, and the drain writes it strictly
last, after verified materialization. An erase of an enrolled org pre-creates create-only markers with outcome
`erased` for the undrained stages in its scope, before its SQL commit. When a drain's marker put fails with
already_exists, or its ACK was lost, the drain re-reads the stored marker. It counts the stage drained only when that
marker is its own: outcome `drained` with the same `staged_sha256`. An `erased` marker, or any other outcome or
digest, means the stage is not drained, and the drain reports the erase's outcome. Its bytes are re-armed, never
served. B1 and B2 (the binding and the witness) are diagnosable refusals only.

## W2 revision (design and isolated synthetic proof only; no migration, grant or production enable)

Split agreed with sessions-buffered-drain-materialization (cc lander):
- (1) revocation authority and (2) registration and exclusion of late writers: sessions-s3-admission-fence;
- (3) granularity: sessions-buffered-drain-materialization;
- the serving-path inventory: with the T89 reader owner, sessions-partial-canonical.
Direction: for enrolled orgs during the hold, the authoritative visibility and revocation decision is a SQL
visibility row per effect, committed under the same lock as the erasure-history capture. Readers check its verdict
before and after IO, and the S3 drained marker is only a projection of it.

### (3) Granularity: per (stage, session) effect, never per stage (by sessions-buffered-drain-materialization)

- **Unit.** A drained stage's canonical effect is one write per (source, session) group: writeGroups appends exactly
  one chunk per group to that session's manifest. The visibility unit is the effect e = (stage_sha256, session_key),
  with session_key = sessionKeyHash(source, session). A direct committed batch is e = (batch digest, session_key).
  There is one row per effect, keyed (org, effect kind, effect digest, session_key): hashes only.
- **Provenance.** Before the first canonical byte, the drain records its intent for each group: session_key, the
  chunk source sha256 (embedded in the chunk key) and the line count. After the write it records the manifest seq
  and stored_sha256. Every canonical chunk maps to exactly one effect, and the mapping survives a lost ACK. A replay
  is the same effect (the batch-digest dedupe).
- **Mixed stage.** Stage X has groups s1 and s2, and s2 is erased.
  - Erase first: its SQL commit, under the history lock, refuses the row for (X, s2) only. (X, s1) can still publish.
  - Publish first: the erase revokes (X, s2) in the same commit that records the erasure, and (X, s1) stays served.
  - No subset fold: a chunk is served only when its effect row is visible, and is never re-folded partially. No
    chunk spans two sessions, and none of the 15 sources erases below a session, so (stage, session) is sufficient. A
    record-level source would be a new effect kind, never a sub-chunk mask.
- **Cross-page generation.** The row carries the fence generation (epoch, resets) it was written under, as
  recordGeneration stamps. A reset or an epoch bump makes it stale by rule, as entryForgotten treats an index entry,
  so a later page never revives an older effect.
- **Crash and lost ACK.** With an intent and possibly bytes but no row, nothing is served. The rerun re-derives the
  same effect, reads the bytes back (materialization_unverified otherwise) and only then requests the row. If an
  erase won meanwhile, the request refuses and the orphan chunk is re-armed (marker_foreign).
- **S3 outage.** The erase's SQL commit, which revokes rows, needs no S3, so erasure is never blocked by the store.
  Byte deletion follows through the re-arm sweeper once S3 is back, and reads stay excluded meanwhile because the
  row is revoked.
- **Synthetic proof (theirs, isolated: fs store and PGlite, then PG17/MinIO with put-gate):**
  - a mixed stage [s1, s2];
  - park the s2 manifest PUT after the last check, commit a SQL-only s2 erase, resume;
  - read both sessions through the canonical reader plus the verdict, before any repair;
  - the reverse order;
  - a crash after the intent, after the bytes and after the row;
  - a lost ACK on each put, then a retry.
  Driven by an in-memory verdict function until the visibility-row stand-in exists.

### Inventory: the mutation entry points behind the 15 history sources (source read at 6d60dc4f)

There are about 25 SQL writer entry points, all owner-run functions. No runtime role has DML on any of the 15
tables. The only direct TypeScript DML is in scripts/frozen-source-copy.ts and the owner-buffered-parity CI fixtures.

| Family | Entry points (latest body) | 062320 class | S3 effect and order |
|---|---|---|---|
| Selection | `ingest_set_session_uploads` (via `_internal`, `ingest_apply_ordered_control`, `ingest_apply_selection`) | control | Disable: S3 first. Enable: SQL first, then S3. Ordered: S3 begin, then SQL, then S3 finish (app.ts:1075-1087) |
| Selection | `ingest_selfhost_set_session_uploads` | control | **None** |
| Reset | `ingest_reset_uploaded_sessions` (via `ingest_apply_reset`) | control | S3 first; `withSqlControlMirror` reconciles (ledger-s3) |
| Turns | `ingest_turn_register` (via `ingest_turn_materialize_fence`) | control | **None** (SQL-only fence deletions) |
| Admission side effects | `ingest_commit_batch` (rearm, `ingest_erase_fence_extend`), `ingest_commit_buffered_batch`, `ingest_claim_batch_mode`, `ingest_commit_revisions`, `ingest_claim_buffered_batch` (lazy begin, user, records) | admission | S3 objects before the commit, or SQL first at the claim |
| Seeding | `ingest_seed_fence_baseline` | unaffected | **None** |
| Session erase | `ingest_operator_runtime_begin` (scopes, runs, receipts, events, fences, deletions); `_step` and `_purge`; `_write_begin`, `_write_finish` and `_write_failed` (writes); `_next`, `_authorize` and `_release` (runs, lease only) | erasure / unaffected | SQL begin first. Bytes are erased later by the worker, S3 first and then the step (runtime.ts:156-177). Self-host s3 writes a tombstone after the begin |
| User erase | `operator_user_erase_begin`, `_step` (purges deletions, resets, rearm), `_complete`; `relayfacts_user_erase_v1` (relayfacts fences, before the begin) | erasure (begin) / owner | SQL first, then the S3 byte erase, then complete. Relayfacts: **none** |
| Privacy authority | `ingest_privacy_authority_begin` and `ingest_privacy_authority_activate` (frozen-source deletions and resets; `session_analysis_purge_grant` / `purge_session_analysis_for_owned` through dynamic EXECUTE: purge authority and fences) | unaffected / owner | **None** on these paths |

**Capture.** 062340 puts AFTER INSERT/UPDATE/DELETE row triggers (`ingest_org_erasure_history_record`) on all 15
tables. Each one inserts a row into the insert-only `sessions.ingest_org_erasure_history` and **takes the org hold key
SHARED until commit**. BEFORE TRUNCATE triggers refuse a truncate, and `recording()` detects disarmed triggers. This
is the one serialization point every one of the ~25 writers passes through, the SQL-only ones included: no eraser
mirror is needed.

### (1) Revocation authority, its ordering with the SQL commit, and in-flight reads

- **Authority.** SQL, for an enrolled org during and after its hold: a visibility row per effect (unit and keys
  from section 3) and the erasure history. Proposed table (needs a future slot): `cutover_effects`. It has no
  `org_id` column and no `ingest_`/`upload_` prefix, so lazy-begin bootstrap scans ignore it.
- **Publish.** One READ COMMITTED transaction per effect, or per stage's effects:
  1. `pg_advisory_xact_lock` on the org hold key, **EXCLUSIVE**. It waits for every in-flight write to any of the 15
     tables, because their capture triggers hold the key SHARED until commit.
  2. In a later statement (a fresh snapshot after the grant):
     - the hold is the enrolled request's epoch and not released;
     - the writer's fencing token equals the org's current SQL drain token (see 2);
     - no erasure-history row covers the effect's scope (org, workspace, user, session) after the bound export
       snapshot position;
     - the generation (epoch, resets) still matches.
  3. Insert the row as visible, and commit.
- **Revocation.** It is the erasure itself. The history row commits atomically with the erase's own SQL write, in
  the same transaction. The verdict for an effect is visible when its row exists AND no history row covering its
  scope is positioned after the row's publish position. A publish holds the key EXCLUSIVE and every history insert
  holds it SHARED, so each history row is strictly before (seen by the publish check, which then refuses) or after
  (it revokes) a publish. Read exclusion is immediate at the erase commit, with no replay. Byte deletion follows
  later (replay, re-arm sweeper) and does not affect visibility.
  - Open check: "position" must be a history ordering assigned after the trigger's lock, such as an identity taken in
    the trigger's INSERT. Verify it against 062340's columns before the slot.
- **Reads in progress.** The reader calls one batched, tenant-bound verdict function per scope (`bulls96_read_api`:
  a grant, migration and role-verify dependency) before IO and again after IO, on success and on failure, inside
  the operator-fence bracket. An erase that commits during the IO fails the post-IO verdict, which is answered as the
  operator fence answers. 404 `session_fenced` and the terminal 503 `operator_fence_unavailable` take precedence, and
  a refusal is never fallback-eligible. A response completed before the erase commit is linearized before it.
- **S3 outage.** An erase never needs S3 for its SQL commit, and revocation is that commit. Reads stay excluded, and
  byte deletion resumes with S3.
- **After bridge_complete.** New publishes refuse (the hold is released). Rows and history remain, so a later erase
  still revokes every SQL-tracked effect at its commit, until its bytes are gone.

### (2) Registration and exclusion of late stages and direct claims, without enumeration

A prefix listing cannot close the writer set, so exclusion is not by enumeration. For an enrolled org, a canonical
manifest entry is served only when one of three things holds:
- (a) it is within the enrollment baseline;
- (b) it maps to a visible effect row;
- (c) it is a direct entry whose batch record was committed under bridge stamp G.

The baseline is the per-session manifest version captured BEFORE the enrollment PUT and stored beside the
enrollment. Every entry appended after the capture, legitimate or stale, needs (b) or (c).
- **Late or retried drain stage.** It can be read only through a publish, which checks the SQL drain token. The
  bridge step bumps that token under the key, so a drain that lost its lease, or a delayed retry, is refused
  atomically at the decision. Bytes it already wrote are never served and get re-armed.
- **Direct claims.** Claims after enrollment are refused by `enrolledGate` until the bridge record exists. A claim
  then stamps its batch record with G. A commit of an enrolled org's batch requires the stamp. A claim made before
  enrollment that commits later, even after P, finds no stamp and is refused (`ledger_attempt_superseded`). The
  client's retry re-claims, and the claim CAS-replaces the unstamped pending record with a stamped one and a new
  token, so the old writer can never commit. That decision is made at the batch record, the authoritative object, with
  no listing.
- **Direct batches committed before enrollment but appended after the baseline capture.** They are legitimate
  content and are adopted. The bridge tool, and any rerun, publishes their effect rows through the same EXCLUSIVE and
  history-checked publish. A commit that raced the enrollment and missed one adoption pass is invisible (fail closed)
  until a later idempotent adoption pass publishes it, never without the history check.
- **Already-issued writes.** Bytes landing at any time after losing authority never satisfy (a), (b) or (c), so they
  are never readable.

### Stand-in interface for the synthetic proofs (sessions-buffered-drain-materialization)

```ts
interface CutoverVisibility {
  /** The EXCLUSIVE-key publish. A refusal is a terminal code; the effect stays unserved. */
  publish(e: { org: string; kind: "drain" | "direct"; digest: string; sessionKey: string; generation: [epoch: number, resets: number]; token?: number }):
    Promise<"visible" | "refused_erased" | "refused_released" | "refused_token" | "refused_generation">;
  /** Batched verdict for one read scope (called before and after IO). */
  verdict(org: string, effects: { kind: "drain" | "direct"; digest: string; sessionKey: string }[]): Promise<Map<string, "visible" | "revoked" | "absent">>;
  /** Test hook: one erasure-history row for a scope (stands in for any of the ~25 writers through the capture trigger). */
  erase(org: string, scope: { workspaceId?: string; userId?: string; sessionKey?: string }): Promise<void>;
  /** The bridge step's token bump (drain fencing). */
  bumpToken(org: string): Promise<number>;
}
```

### Serving paths (inventory owed with sessions-partial-canonical)

Candidates that read canonical manifests or chunks, found by grep and still to be confirmed per path:
- the session read API (`canonical-projection.ts`, `canonical-stream-projection.ts`);
- `packages/session-read-worker`;
- the pipeline package (`relayhistory-transform`, `reconcile`, `adapter`, `manifest-readiness`);
- the HoopDb search indexer (`src/hoopdb/indexer.ts`);
- the session-analysis runner;
- the neon-s3 sync and erasure tools.
Every confirmed path must enforce the verdict, or be shown never to read an enrolled org's manifests.
