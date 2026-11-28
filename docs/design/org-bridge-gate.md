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

**SUPERSEDED by the W2 revision below (lander verdict 252339171875885056, design at 79431c4b): SQL decides
visibility per effect, and the drained marker only records terminal per-session outcomes. Kept for history.**
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
  - **Correction after reading 062340 (columns: erased_org, source, op, at, xid).** There is no ordering column
    taken after the lock: `xid` is assigned at the erase transaction's first write, before its trigger takes the
    key, and `at` is a clock. Position is therefore defined by snapshot. The publish captures `pg_current_snapshot()`
    in the statement after its EXCLUSIVE grant, and stores it on the row. Every erase that held the key SHARED
    earlier has committed and is visible in that snapshot. A history row revokes the effect exactly when
    `NOT pg_visible_in_snapshot(xid, published_snapshot)`, that is, when it committed after the publish.
  - **Scope.** The history row has no workspace, user or session, so on its own it can revoke only org-wide. Two
    options:
    - (i) Coarse plus a reconciler. Any post-publish history row for the org marks the org's effects revoked pending
      reconciliation, and readers see an incomplete scope and fall back. A SQL reconciler reads the post-snapshot
      rows of the 15 source tables, which carry scope, and re-publishes the unaffected effects with a new snapshot.
      It is correct and fail-closed, at an availability cost while erasures are active.
    - (ii) Recommended, in B's forward migration: the capture trigger also records the row's scope (workspace,
      user, session key where the source has them), so the verdict revokes exactly the covered effects. Sources
      without a session (user erase, reset, privacy authority) revoke at the user or org scope they name.
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

### Serving paths (by sessions-partial-canonical, static grep at 8a043736; deploy wiring and raw getObject readers not verified)

**Choke points.**
- C1: `canonical-projection.ts` readManifestRecords and streamManifestRecords, through which every chunk GET of the
  projection readers passes. Mark entries before boundedPrefetch, re-check after the scope loop, and make the scope
  incomplete on a non-visible entry, never dropping it.
- C2: `objectStoreManifestReader` (pipeline adapter). It reads manifests only, with no SQL or tenant, so the
  verdict goes at its callers.
- C3: `canonical.ts` snapshotWithLimits. It already reads SQL pointers before IO and re-reads them after (the
  generation fence). This is the place for one batched visibility read per scope, folded into
  `canonicalSourceGeneration` so cursors answer 409 when visibility moves.
- C4: `canonical-transcript.ts` has its own chunk loop, bypasses C1, and needs its own verdict.

**Request-time serving paths.**
1. The hosted partial fallback (`/v1/sessions/:id/events`): C3 and C1, with pointers and the operator fence pre and
   post.
2. The self-host service-canonical routes (timing, turns, metadata, catalog, events, includeMessageId): C3 and C1.
   The transcriptSource path goes through C4. The operator fence applies only with `SESSION_ERASE_FENCE_ENFORCED=1`.
3. The Read Worker canonical routes: C3 and C1, with **no operator fence** (index.ts:655).
4. Read Worker pages mode: serves published replay artifacts. They must publish only visible effects, or re-check
   at serve time against the artifact's source generation.
5. The session-analysis reader: as 4.
6. The canonical history adapter: covered if 3 and 4 are.

**Producers (derived stores outlive a serve-time verdict, so they need gating plus invalidation on a verdict change).**
- P1: the search index (search-index, consumer, search-jobs, large-read, staged-reconcile, canonical-stream-projection).
- P2: output and digest facts.
- P3: relayfacts backfill.
- P4: session analysis (evidence and worker; results served by /analysis).
- P5: replay publication (which feeds 4 and 5).
- P6: the session brief (via C3, C1 and C4; served by /summary).
- P7: search features.
- P8: pipeline reconcile and adapter (pointer reconcile).
- P9: the ingest, seed and courier tails (writer side).
- Not canonical: buffered-search-feed and owner-buffered.

**Readers that build chunk keys themselves** (sessions-buffered-drain-materialization, grep at 6d60dc4f). Each needs
a verdict, or an argument that it cannot serve an enrolled org's drain effect:
- the Bob Costas session input (src/bob-costas/session-input.ts:227);
- the session-brief input (src/session-brief/input.ts:231);
- alleyoop manifests (:670);
- the pipeline relayhistory-transform (:514) and parked (:66);
- publication-image (:147), which copies base chunks forward;
- session-pipeline pointer (:258).

**Deltas on the readers above** (sessions-buffered-drain-materialization, static call sites, lander checks at 6d60dc4f):
- **Bob Costas, the brief input and relayhistory-transform** consume chunk content. Their derived outputs, and the
  publish and read gates on those outputs, still need tracing.
- **publication-image `baseCopies`.** A copied hidden chunk must keep its effect identity (chunk key to effect row),
  or be refused before publication.
- **Corrections:**
  - alleyoop/manifests.ts:670 is a HEAD that validates metadata; what that validation authorizes needs tracing.
  - parked.ts:66 is a DELETE in deleteParkedSession, which is cleanup. Its key isolation from live chunks needs
    checking.
  - pointer.ts:258 probes whether manifests exist across dates; the downstream pointer selection needs tracing.
- **Open:** whether an enrolled org can reach these paths at all, and caller-visible closure. Nobody is assigned to
  trace them yet.

**Not traced:** raw `getObject` readers over `raw/` outside these helpers, and src/migration (legacy-export,
neon-s3-sync, neon-s3-erasure). A targeted grep is owed before W2 is ruled.

**Consequence for scope (for the lander).** W2 as a full reader contract touches 4 choke points, 6 serving paths
and 7 derived producers. A narrower option for wave 1 would apply that contract only to the paths that can serve an
enrolled org during the hold, and keep every derived producer from consuming an enrolled org's canonical entries
beyond its baseline until the bridge sweep has settled visibility. Whether that is sound depends on the producers'
triggers (fan-out or queue), which is not yet traced.

### Evidence and ruling state

- **SQL-only erase finding, pinned** (review/buffered-drain-materialization @ f6cfa827, "race with a SQL-only
  erase"). A drain manifest PUT is parked after the last check, a SQL-only erasure source commits, and the PUT
  resumes. Nothing on S3 moves, and the stage is marked drained with no re-arm. The bytes are readable at once, and
  the S3-only verifier passes. No S3-side check can exclude a SQL-only erase, so the SQL visibility decision is
  necessary.
- **Lander ruling 8c1c6f7eb8f4.**
  - W2 is cleared for a revised design and an isolated synthetic proof only.
  - B goes in a new forward migration, slot unassigned (062400 is T51's). It must close admission AND publication,
    and distinguish verified_rollback from a completed cutover.
  - R is only a candidate safe point until pending direct and drain effects are accounted for.
  - A's late-install versus withdrawal race still needs proof at the readable-byte boundary.
  - Wave 1 stays held.

### Isolated synthetic proof (PGlite, every migration, real 062340 triggers; stand-in schema only)

`src/session-ingest/__tests__/cutover-visibility.pglite.test.ts` (8 tests) pins:
- the capture trigger records a real source-table write with an xid and no session scope;
- erase first, after the bound: the publish refuses;
- an erase before the bound: the publish is allowed;
- publish first: the erase's own commit revokes, so pre-IO says visible and post-IO says revoked, with no replay;
- scope is org-wide with 062340 as it is (option i), and another org is unaffected;
- snapshot ordering;
- publish refuses after the release;
- the installer race: the withdrawal's SQL `_install` plus `verified_rollback` leaves a paused installer unable to
  commit its request (55000).

**Owed, needing the reserved PG17 lane (claim-ci):**
- the two-connection orderings: a publish blocked by an in-flight erase that holds the key SHARED, and the reverse;
- the interleaving where the xid is taken before commit.

**Owed, needing reader stand-ins (C1/C3):**
- the readable-byte boundary for the late-install race and the T97 parked direct writer.

### Drain-side synthetic proof and its two findings (sessions-buffered-drain-materialization, 585770d2)

`cutover-visibility.synthetic.test.ts` on review/buffered-drain-materialization (8 pass, test-only) runs the stand-in
at the drain's marker PUT, with a test reader that checks the verdict before and after IO. Each read happens right
after resumption and before any repair. The cases:
- erase first, with a mixed stage (s1 served, s2 never served);
- publish first, revoked at once;
- a read in flight across the erase commit fails post-IO;
- a user-scope erase;
- a crash before the publish;
- a lost publish ACK;
- a late drain after the token bump;
- a pre-export erase.

- **F1, agreed: a terminal per-session outcome.** A stage with one refused effect never ends today, because
  marker_failed is not terminal. Fix: the stage's create-only marker carries per-session results, for example
  `{s1: visible, s2: refused_erased}`. A stage whose every effect is decided (visible or refused) is terminal, the
  checkpoint passes it, and the refused sessions' orphan bytes are re-armed. The marker remains a projection: the SQL
  rows stay authoritative, and the marker is written only after every effect's publish has returned a terminal
  answer.
- **F2, agreed: derive the effect from the chunk, not from the bounded manifest index.** The `relayhistory_live`
  batch index keeps only the last 64 batches (writer.ts REMEMBERED_BATCHES), so an older chunk has no mapping. That
  fails closed but loses availability on long sessions. Fix: the effect row records the chunk key (its embedded
  source sha256) at publish, and the reader's batched verdict is keyed by the chunk keys it is about to GET. That
  needs no manifest index and survives lost ACKs, because the chunk key is deterministic.

### Installer withdrawal and paused direct writer: synthetic proof (lander: synthetic proof only)

`withdrawEnrollment(install_failed)` now consumes the request in SQL before its S3 CAS: `_install` (idempotent),
then `_release(verified_rollback, detail 'enrollment_withdrawal')`, then the CAS. Any failure is
`enrollment_consume_failed` and withdraws nothing, so the org stays fenced. The proof is
`cutover-withdrawal.pglite.test.ts` (3 tests), on real 062320/062340 SQL, a real ledger-s3 with `orgBridge`, and a toy
layout with two labelled toy readers:
- **The paused installer resumes after the withdrawal.** Its `_install` refuses (55000), no hold is opened, readers
  see only the pre-cutover bytes, and the org is back to its single pre-cutover writer.
- **A crash between the withdrawal's `_install` and its rollback.** The org is held in SQL and enrolled in S3, so
  both writers are fenced and nothing new is readable. The rerun finds the live hold of the request, releases it and
  withdraws.
- **A paused direct writer** that claimed before enrollment and whose bytes land after the baseline capture. Its
  commit is refused while held. **Today's reader serves its bytes (the gap); the W2 rule (2) does not.** An entry
  that landed before the baseline capture is pre-cutover state: it is erased through its owner stub like any other.

### Ruling state (latest)

- **W2 verdict 252339171875885056.** The revised design and the isolated synthetic proof may proceed.
  - The seven reader constraints stand, including the identical-pointer cursor regression and terminal fence
    verdicts on failed reads.
  - Erase ordering follows 79431c4b: SQL decides visibility per effect, and the drained marker records terminal
    per-session outcomes.
- Production reader wiring, grants, the migration and release remain held. B needs an unassigned forward slot
  (062400 belongs to T51). The PG17 two-connection orderings wait for the reserved lane. Wave 1 is held.
