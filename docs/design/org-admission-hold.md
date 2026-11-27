# Per-org admission hold: an enforced writer fence for the fence-authority marker

Status: revision 4, for joint review by b1 and layered-sessions-pipeline. Revisions 1 to 3 (8e0463d9, 6d7e1b06/dacfc1d9,
507caea3) were held; every disposition is folded in (section 9). layered-sessions-pipeline decides the code-only start. No migration code until both agree. Slot
`20261105062320_ingest_org_admission_fences` is reserved for this design (registry receipt 251934773687459840).
This track is code-only. Applying the migration to a real database is a separate, gated action. backlog-ops
authority-execute stays dormant until the slot lands, is applied, and the marker step binds to it.

## 1. Why

Writing an org's fence-authority marker (`ledger-s3.ts` `validOrgFenceAuthority`) asserts that nothing admits that org
through SQL any more. Read-only evidence (`src/session-ingest/authority-evidence.ts`, now corroboration only) can show
that only at its sample times. Across four review rounds, a claim landing between the last read and the marker write
always remained possible. The guarantee has to be enforced by the database from a known instant, the install of
epoch E, and every later step has to be bound to E.

The Worker's `INGEST_HTTP_ADMISSION_STOPPED_ORGS` stop stays in place to stop HTTP traffic early. The hold is the
SQL-side enforcement, and it does not depend on which caller reaches the database.

## 2. Caller classification

The full matrix (latest bodies, file:line, locks, security, isolation) is in
[org-admission-hold-caller-matrix.md](org-admission-hold-caller-matrix.md). Line numbers are against 2bfa8c32 and are
re-verified against the tree the slot lands on. The rule: **every function a runtime role (bulls96_session_ingest, the
seeder) can call that writes admission content checks the hold at entry, before any side effect.** Admission content
means batches, stage intents, outbox, native slots, revisions, ownership, claim attempts and admission rows created
by a bootstrap. Each function in the ACL manifest (`src/hoopdb/sessions-function-acl.json`) is classified once, in the
slot's header, as one of the classes below. The test suite asserts that the classification covers every
runtime-granted function, so a new function is unclassified and fails the build.

| Class | Functions (latest body) | Under a hold |
|---|---|---|
| **Admission entry**: checks the hold first | `ingest_origin_claim_buffered_batch` 062010:105; `ingest_claim_buffered_batch` 043850:1130; `ingest_claim_batch` 050000:306; `ingest_claim_batch_mode` 050010:117 (inner; checked again, harmlessly re-entrant); `ingest_origin_commit_buffered_batch` 062010:125; `ingest_commit_buffered_batch` 050010:373; the origin and plain `ingest_prepare_buffer_stage` 043850:1172; **`ingest_commit_batch` 050010:441** (accepts committed rows and can reach revision writes; disposition 1); **`ingest_commit_revisions` 060000:907** (`ingest_attempt_epochs`, 030000:60, validates a token and digest with a NULL batch id and never requires pending; disposition 1); `ingest_lazy_attempt` / `ingest_lazy_ack` / `ingest_lazy_ack_batch` / `ingest_lazy_stage_seats` where runtime-reachable; **`ingest_seed_proven_records` 040000:280** (refused; disposition 2. It seeds ONE capture's org across several users and accepts a `capture_id` even with zero records. The check therefore resolves the org from the trusted capture row, not from the payload, takes that org's key S, re-reads and validates the capture under the lock, and only then refuses or proceeds.) | **refused**, RH042, before any write. Replays, resumes and fast paths of `claim_mode` (050010:117ff) are refused too, uniformly. A committed-batch replay would be side-effect free, but the buffered wrappers run `ingest_lazy_begin` and `ingest_lazy_records` (a bootstrap admission row, slots) before `claim_mode` decides it is a replay, so the check has to come before them. Replays sit behind the Worker's HTTP stop anyway, and a refusal is retryable. |
| **Control**: proceeds | selection and reset (incl. their `ingest_lazy_begin` and `ingest_lazy_clear` use), self-host erase, consent, privacy authority, source control, custody, buffered GC | **proceed in every state**. Removal and consent state only. A control's `ingest_lazy_begin` bootstrap of an admission row admits nothing by itself. Selection and reset must be S3-mirrored while `sealed` or later (section 6.3). |
| **Erasure**: always proceeds | operator session erase, operator user erase | **proceed in every state**, never refused by the hold. Erasures recorded after FULL started are replayed to the S3 ledger before `bridge_complete` (section 6.2). |
| **Catch-up / bridge**: proceeds until the seal | publication claim, publish and heartbeat; history plans; `buffered-drain.ts` (no SQL); the fence backfill and accepted-stage export (read-only SQL) | **proceed while `held`**. Publication writes revisions of previously accepted content, so its entry points take the org key shared and are **refused once `sealed`** (section 5). History plans write no revisions or ownership; the read-only and S3-only paths are unaffected. |
| **Admission-row operators**: untouched | `ingest_lazy_begin` bootstrap (inside a claim it rolls back with the refused claim), renewal, repair, rebind, cutover ceremony | **untouched**. Renewal and rebind are all-or-nothing bulk statements. The hold never reads or writes admission rows. |

Backstop: a `BEFORE INSERT FOR EACH ROW` trigger on `sessions.ingest_batches` runs the same check. It catches a future
inserter that bypasses classification. It is not the primary enforcement.

## 3. Objects (slot 062320)

- `sessions.ingest_org_admission_holds`: `epoch uuid PRIMARY KEY`, `held_org text`, `request_id uuid UNIQUE`,
  `state` (`held` | `sealed` | `marker_attempted` | `released`), `installed_at`, `sealed_at`, `marker_attempted_at`, `released_at`,
  `release_reason` (`bridge_complete` | `verified_rollback`), `release_evidence_sha256`. A partial unique index
  allows at most one row per org with `state <> 'released'`. The column is deliberately **not** `org_id`, and there is
  no `workspace_id` or `user_id`. `ingest_lazy_begin`'s bootstrap scan (041000:901) treats any `ingest_%`/`upload_%`
  table with `org_id` as prior authority, `ingest_lazy_user` (041000:776) scans `workspace_id`/`user_id`, and the
  repair operator hashes that scan.
- `sessions.ingest_org_admission_hold_events`: `epoch`, `kind` (`installed` | `sealed` | `marker_attempted` | `released`), `at`,
  `detail` (for example `abandoned_before_marker`), `evidence_sha256`. Insert-only. Exactly one event per transition;
  an idempotent repeat writes none.
- Immutability triggers on both tables:
  - No DELETE, and no TRUNCATE of these two tables.
  - The only UPDATEs allowed are the transitions of the state machine in section 6.1, each with every identifying
    column unchanged.
  - Both tables are owner-only, with RLS on and REVOKE ALL from every runtime role.
- Owner-only functions under the lane prefix `sessions.ingest_org_admission_hold_`: `_lock`, `_check`, `_install`,
  `_seal`, `_marker_attempt`, `_release`, `_mirror_gate`, and the trigger functions.

## 4. Locks, visibility, lock order

**Key**: `pg_advisory_xact_lock[_shared](hashtextextended('96bulls:ingest-org-hold-v1:' || org, 0))`. This is a new
single-bigint namespace that does not collide with any key in matrix G. It exists whether or not a hold, admission or
fence row exists, so the missing-row bootstrap serializes on it too. A hash collision only over-serializes.

**`_check(org)`** (admission entry, the backstop trigger, seeding):
1. `transaction_isolation` must be `read committed`; otherwise 40001, retryable. Precedents: 043500:39 and the privacy
   lock at 041000:85.
2. Take the key **shared**.
3. Then, in a later statement, raise RH042 if the org has a row with `state <> 'released'`. In a `VOLATILE` plpgsql
   function under READ COMMITTED, that statement takes a fresh snapshot after the lock was granted. REPEATABLE READ and
   SERIALIZABLE are refused in step 1, before they could read a stale snapshot. Every admission path already runs
   READ COMMITTED (`ingest_lazy_records` requires it).

**Namespace.** The key string is the single literal `'96bulls:ingest-org-hold-v1:' || org`. Every existing
`hashtextextended` key uses a different literal prefix: `96bulls:bookkeeping-*`, `-canonical-feature-journal`,
`-relayfacts-user-erase-v1`, `repo-catalog:`, `session-analysis-request:`/`-scope:`/`-schedule:global`, the per-scope
completion keys among them. Others use a different encoding: `jsonb_build_array('96bulls:lazy-native-origin-v1', …)`,
`jsonb_build_array('upload_consent', …)`, and `concat_ws(chr(31), '96bulls:directory-events' | 'operator-write' |
'publication-pointer', …)`. Two-int keys live in a different lock space (objsubid 2). So the key can coincide with
another only through a 64-bit hash collision, and that would only over-serialize the two holders. It never affects
correctness.

**Retryable end to end.** Every hold refusal uses one SQLSTATE, `RH042`, and one fixed message, `session ingest: org
admission held`. RH042 is not used elsewhere today.
- **SQL to edge.** `src/session-ingest/ledger-sql.ts`'s `ledgerErrorCodeFrom` gains `RH042 -> ledger_org_admission_held`
  (a `LedgerUnavailableError`). The HTTP paths answer 503 `delivery_unavailable` with Retry-After
  (`HTTP_ADMISSION_STOPPED_RETRY_AFTER_SECONDS`, 300 s), as the Worker's HTTP stop does. The desktop keeps the batch
  and retries; it is never a permanent reject and never client-visible data loss.
- **Queue consumer.** The admission queue consumer treats RH042 as a deferral (retry with delay). The queue is drained
  before install (protocol), so receiving RH042 there means the protocol was violated. It also raises an alert, because
  repeated retries would eventually dead-letter, and a dead-lettered message for a held org must not be replayed.
- **Tests.**
  - Each refusing function, at the SQL level.
  - Through the TypeScript callers: a held org's claim through the hosted upload-admission and buffered path
    (`deliverBatch` via `sqlLedger`) round-trips as a retryable 503 with Retry-After, and nothing is written.
  - A matching consumer test for the deferral.

**`_install(org, request_id)`**:
1. READ COMMITTED; key **exclusive**. This waits for every transaction that holds it shared, i.e. every in-flight
   admission of the org, until it ends.
2. If a non-released row exists with the same `request_id`, return its epoch. This is the idempotent case for a lost
   response, and no event is written.
3. Refuse if any other non-released hold exists for the org.
4. Refuse (RH042) if the org has any `pending` batch, read in a fresh statement.
5. Insert the hold with a fresh epoch and the `installed` event, then return the epoch.

**Lock order.** Every admission entry takes, first: the org key S (its own `_check`). Then, unchanged from today:
1. The origin advisory (origin wrappers).
2. Marker, policy and admission row FOR SHARE (`lazy_begin`).
3. 50000S.
4. The fence row.
5. The clock.
6. The native-origin advisory.
7. The inner `claim_mode`/`commit` `_check`. This re-acquires the same shared key, a no-op re-grant in the same
   transaction.

Every entry point is per batch, so it covers one org. `ingest_seed_proven_records` may span several pointer users:
it takes the keys of all its orgs in sorted org order before anything else. `_install`, `_marker_attempt` and
`_release` take exactly one key X and nothing else that an admission holds. An exclusive waiter blocks only that org's
new admissions, never another org's. No cycle exists.

**Both orderings.**
- **Admission first.** It holds S until it ends; install waits. If the admission left a pending batch, install refuses
  on it. If it committed a batch, install proceeds; that batch is committed SQL state, which the backfill reads
  (section 5).
- **Install first.** The admission's entry `_check` waits for the key. When install commits, the next statement sees
  the hold and the admission raises RH042 before any write. Because the check runs at entry, nothing (no bootstrap
  row, slot or stage) is written even transiently.

## 5. Backfill, the seal and the snapshot contract

The fence backfill opens a separate REPEATABLE READ transaction per page (`src/session-ingest/upload-fence-backfill.ts`,
:34, :618, :647), so no single snapshot covers it. A publication allowed under the hold can also commit after the
final catch-up page. 060000 stamps `updated_at = now()`, the transaction's start, so a long transaction can land
behind any time cursor. Start and end checks of E bind nothing about such rows. Revision 3 adds a terminal handoff.

**Seal.** `_seal(org, E)` takes the org key **exclusive**. That waits for every in-flight transaction holding it
shared: admissions, which are refused anyway, and publications, whose entry points now take it shared. It then
commits `held -> sealed` and its event. After the seal, no revision or ownership writer of the org can run except the
classes in the safety table below.

**Order.** install(E) -> FULL backfill-execute -> `_seal(E)` -> final catch-up -> marker attempt. The catch-up must
start after the seal committed.

**Catch-up predicate.** FULL's receipt records `full_snapshot_xmin`, the `pg_snapshot_xmin` of its first page's
snapshot. The final catch-up re-applies every revision row of the org with
`updated_at >= FULL.created_at - margin` **or** a full xid (computed as in authority-evidence's `xid8`) `>=
full_snapshot_xmin`. The second clause catches an old-start transaction regardless of its `updated_at`, because
its xid was in progress, or later, when FULL took its first snapshot. Fences and ownership are re-run in full, as the
refresh does. Its receipt carries the hash of E. The gate refuses it unless the hold was `sealed` with E when it
started and when it ended.

**Per-class safety table** (after the seal):

| Class | Can it still change the org's SQL state? | Why the S3 state stays correct |
|---|---|---|
| admission entries | no (RH042) | nothing is written |
| publication | no (RH042 once sealed) | waited out by the seal's X |
| controls: selection, reset | yes: deletes revisions and ownership | only through the S3 mirror while `sealed` or later (section 6.3, enforced by `_mirror_gate`). The Worker writes the S3 fence before SQL for a disable or reset and after SQL for an enable (`app.ts` 1046-1063, 1104-1118). The S3 claim honours the fence's deleted epochs over any index entry or owner stub. **Proof obligation:** S3 claims after the final catch-up for a session deleted, reset, or re-enabled through the mirrored path are refused or admitted as the fence says, despite stale stubs and index entries. A non-mirrored SQL caller is refused (RH042, retryable). |
| operator session and user erase | yes: deletes SQL rows; the user erase also S3 bytes | **proceed** (erasure is never blocked). Every erasure recorded after FULL started is replayed to the S3 ledger before `bridge_complete` (section 6.2). |
| buffered GC | stage intents and outbox only | does not touch revisions or ownership |
| renewal, rebind, repair | admission rows only | the hold never reads them |
## 6. State machine, marker writes, erasure, controls

### 6.1 One state machine

| From | To | Function (key X) | Allowed when | Admission | Publication | Controls (selection/reset) | Erasure |
|---|---|---|---|---|---|---|---|
| (none) | `held` | `_install(org, request_id)` | no live hold, zero pending | refused | proceeds | proceed | proceeds |
| `held` | `sealed` | `_seal(org, E)` | FULL backfill-execute bound to E finished | refused | **refused** | mirrored only | proceeds |
| `sealed` | `marker_attempted` | `_marker_attempt(org, E)` | the final catch-up bound to E finished after the seal | refused | refused | mirrored only | proceeds |
| `marker_attempted` | `marker_attempted` | `_marker_attempt(org, E)` | same E (idempotent, before every PUT retry; no event) | refused | refused | mirrored only | proceeds |
| `held` or `sealed` | `released` | `_release(E, 'verified_rollback', ev)` | no `marker_attempted` event for E | open again | open again | normal | proceeds |
| `marker_attempted` | `released` | `_release(E, 'bridge_complete', ev)` | bridge done, every post-FULL erasure replayed (6.2) | (S3 authority) | (S3 authority) | normal | proceeds |

There is no other transition. In particular, **there is no rollback after an attempt**. The first PUT attempt is the
point of no return: commit does not check S3 authority (ledger-s3.ts:312), the reader caches positive authority, and
no enforced bound covers in-flight S3 claims. So once attempted, E stays held until `bridge_complete`, with erasure
preserved throughout. Any recovery from an attempted-but-abandoned cutover is a separate, operator-owned procedure,
out of scope. Revision 3's create-only barrier is dropped. The marker PUT itself is always create-only
(`If-None-Match: *`, as ledger-s3.ts:555), never deleted "to make it absent".

`_marker_attempt` refuses unless the gate shows that the final catch-up's receipt is bound to E and ran entirely after
the seal (section 5). So an attempt cannot bypass the final catch-up. Every bound step and the bridge's terminal
receipt record the effective authority source the reader resolves (`per_org` | `deployment` | `stopped` | `missing`),
read after the configured reader-cache TTL (`authorityTtl`, read and recorded from the deployment, not assumed).

`_release(org, E, reason, evidence_sha256)`:
- **Evidence:** the hash of an authenticated terminal receipt (issuer, run, sha, scope, org, E).
- **Idempotency:** a repeat with the same E, reason and evidence returns ok and writes no event.
- **Refusals:** a stale epoch or any other repeat.

### 6.2 Erasure: always proceeds, replayed durably

Erasure is never refused by the hold, in any state. Its SQL effects after FULL started are absent from the S3 state
that FULL and the catch-up wrote, so the bridge replays them before `bridge_complete`:
- **Source.** Every erasure of the org is recorded durably with timestamps: `sessions.operator_user_erasures` (058010:104,
  `created_at`/`updated_at`) and the operator session erase records (050000).
- **Replay.** For every erasure of the org with `created_at >= FULL.created_at - margin`, read after the hold is
  `marker_attempted` and again until none is new, the bridge applies the S3-ledger equivalent:
  - for a user erase, the S3 fence reset of that (org, ws, user), which supersedes its index entries and owner stubs
    (the reset path `resetUploadedSessions` the mirror already uses), and `eraseLogPayloads` once it is wired
    (ledger-log-erase.ts, "not wired to a route yet");
  - for a session erase, the S3 fence deletion of that session.
- **Bounded and durable.**
  - Each replay is idempotent, receipted, and keyed by the erasure's request id.
  - An erasure arriving after `bridge_complete` runs against S3 authority as any post-cutover erase does.
  - `bridge_complete` requires a receipt for every erasure in the set, re-read at release under the key, so none is
    lost between the last replay and the release.
- **Owner.** The replay is the bridge job's (backlog-ops). The erase lane owner must confirm that the S3 fence reset
  and the session fence deletion are the complete S3 equivalent of each SQL erase: a named dependency, not assumed.

### 6.3 Controls while sealed: the mirror gate

The S3 mirror of selection and reset is Worker application code (`app.ts` 1046-1063, 1104-1118). A SQL-only caller
of the control functions never runs it.
- **Gate.** While the org's hold is `sealed` or `marker_attempted`, the control SQL entry points
  (`ingest_set_session_uploads` and its internal and self-host variants, `ingest_reset_uploaded_sessions`, and
  `ingest_apply_ordered_control`) call `_mirror_gate(org)`. It refuses (RH042, retryable) unless the transaction
  carries `set_config('bulls96.s3_fence_mirror', 'on', true)`.
- **Who sets it.** The Worker sets it in the control's transaction, only when its `s3FenceMirror` is configured, as
  `ledger-sql.ts` already sets transaction-local settings (`CLAIM_LOCK_WAIT_SET`).
- **What it is.** A code-path gate, not an access control: it stops every caller that does not run the mirror,
  not a caller that deliberately forges the setting.
- **Tests.**
  - Stale and pending ordered controls, a re-enable (the S3 lift follows SQL), and a reset.
  - Direct authorized SQL callers, refused while sealed.
  - Each case asserts the actual S3 admission state afterwards.

**Schema down** (generated by the lane), serialized with every lifecycle function through the migrator's lock.
- **The lock.** Generated downs already take `pg_advisory_xact_lock(961996,1)` exclusive. `_install`, `_seal`,
  `_marker_attempt` and `_release` take `pg_advisory_xact_lock_shared(961996,1)` **first**.
- **The fresh check.** Under that shared lock, in a fresh statement, each lifecycle function re-checks that its slot
  is applied: the ledger row `upstream:20261105062320_ingest_org_admission_fences.sql` exists in
  `sessions.__96bulls_migrations`. An install paused before its INSERT therefore either finishes before the down
  starts, or wakes after the down and refuses. A table lock alone would only schedule its INSERT after the down.
- **Both orderings are tested.**
- **Then:**
- **Refuses** (fail closed) while any hold is not `released`, and while a later slot is applied.
- **Locks and resets** role validation to `pending`, as 062210 does, so readiness has to be re-proven after the
  restored generation and privileges are verified.
- **Removes the enforcement:** the `_check` calls (by restoring the prior bodies exactly), the backstop trigger and the
  functions.
- **Retains** `ingest_org_admission_holds` and `_events` as inert history, so the audit is preserved. Their
  immutability triggers and trigger function are retained too, so immutability persists. A reapply reuses them. The
  down ends by verifying the exact post-down schema and ACL: that exact retained set and nothing else, compared with the
  ACL manifest. This is the reviewer's choice between "preserve" and "verified export before drop". I recommend preserve,
  because it needs no external artifact. The cost is that the post-down schema differs from pre-062320 by two inert
  tables, which the release target must account for.

## 7. Maintenance and owner boundary

The hold binds every caller that goes through the sessions functions, owner-run operators included. It does not bind
the schema owner or a superuser acting directly on tables: DISABLE TRIGGER, `session_replication_role`, direct DML on
`ingest_batches` or the hold tables, running this slot's down, or TRUNCATE.

There is no global TRUNCATE trigger: one org's hold must not block maintenance for every org. The runbook forbids
those operations on `sessions` while any hold is not `released`. Every bound step re-verifies, at its own read, that
the hold is in the state it expects with E.

## 8. Acceptance tests

1. **Two real PG connections (PostgreSQL 15, 16, 17), blocking observed both ways.**
   - An admission blocks install; install then refuses on pending or proceeds on committed.
   - Install blocks an admission; the admission then gets RH042 with no write.
   - Blocking is observed via `pg_locks` and `pg_stat_activity` wait events, not just timing.
2. **Snapshots.** The fresh post-wait statement snapshot under READ COMMITTED is shown; REPEATABLE READ and
   SERIALIZABLE claimers get 40001 before any read.
3. **Entry refusal.** Every class-1 function refuses under a hold with no side effect (row counts and xmin of every
   touched table unchanged; the whole transaction rolled back). This includes the committed-token replays of
   `commit_batch` and `commit_revisions`, claim replay, resume and fast paths, and seeding.
4. **Hold lifecycle.** Concurrent installs for one org: exactly one wins, and a same-`request_id` retry returns the
   same epoch. Release and reinstall; stale-epoch refusal; exactly one event per transition.
5. **What proceeds.** Unrelated orgs make progress during an install and under a hold. Controls, GC, publication,
   history plans, and bulk renewal, rebind and repair with one org held all succeed.
6. **Classification.** A test fails on any runtime-granted function that is not classified.
7. **Owner boundary.** The documented owner operations are shown to bypass, and the bound steps detect the state.
8. **Disposable DB plus object store (MinIO) interleavings.**
   - Marker attempt, then crash.
   - Delayed PUT after a timeout.
   - Ambiguous result, then retry.
   - Rollback before an attempt (released; later attempts refused).
   - Rollback after an attempt (refused until reconciled).
   - A stale replay of an old epoch.
9. **Schema down** refuses with an active hold and with a later slot applied, resets readiness to pending, restores
   the prior bodies exactly, and retains the audit (b1 agrees: retain as inert history).
11. **Seal and catch-up** (section 5):
    - A late publication committing after the final catch-up page is impossible once sealed.
    - An old-start publication transaction begun before FULL and committed after it is caught by the xmin clause.
    - A control or erase behind the final cursor.
    - Each case asserts the effective S3 target state (claims refused or admitted as they should be), not receipt
      chronology.
12. **No rollback after an attempt** (section 6.1), DB + MinIO with a warm reader cache:
    - `verified_rollback` is refused from `marker_attempted`.
    - A paused marker PUT delivered late leaves the hold `marker_attempted`, and the effective authority source is
      recorded.
    - An attempt before the final catch-up is refused.
    - A same-E retry is idempotent; a stale E is refused.
    - Old cached claims that commit after the TTL wait are counted in the bridge's receipt.
14. **Erasure** (section 6.2):
    - A user erase and a session erase in each state proceed.
    - Their S3 replay appears in the bridge receipt.
    - `bridge_complete` refuses while any erasure lacks a replay receipt.
    - An erasure racing the release is caught by the re-read under the key.
15. **Mirror gate** (section 6.3): as listed there.
16. **Down vs lifecycle:**
    - An install paused before its INSERT, then the down: the install refuses on the fresh slot check.
    - The down waits for an in-flight install.
    - The same for seal, attempt and release.
13. **Stale snapshots and wrappers:**
    - Stale snapshots for installer, sealer and writer.
    - Every runtime-granted wrapper and every nested call.
    - No bootstrap write on a refused claim.
    - Refusal and retry through the actual Worker mapping.
    - Down/install serialization, the post-down exact schema and ACL check, persisted immutability, and a reapply.
10. **Retryable end to end** (section 4): the TypeScript-level round trip of a held org's claim as a retryable 503
    with no write, and the consumer deferral.

## 9. Resolved items (from revisions 1 and 2)

1. **Canonical commits** (`ingest_commit_batch`, `ingest_commit_revisions`) are guarded at entry. Disposition 1.
2. **Seeding** is refused under the hold. A seed under a hold would need a separately designed epoch-bound operation.
   Disposition 2.
3. **Down:** fail closed, readiness reset to pending, audit preserved. Per-org rollback reconciles the real S3 marker
   versions and the global fallback before release. Disposition 3.
4. **Release:** only `bridge_complete` and `verified_rollback`, with authenticated terminal evidence, idempotent install
   and release, and the marker-attempt point of no return. Disposition 4 and the joint disposition;
   `abandoned_before_marker` is an audit detail, not a release path.
5. **Revision 2, gap 1** (superseded by item 8): after a marker attempt, rollback only through the create-only barrier at the org key,
   effective after the configured reader-cache TTL plus in-flight claims; the effective authority source is recorded.
6. **Revision 2, gap 2:** a seal under key X waits out in-flight publications, then the final catch-up takes rows by
   xmin against FULL's first snapshot as well as by `updated_at`; a per-class safety table covers what may still
   change.
7. **Matrix:** seeding resolves the trusted capture org, takes S, re-reads the capture; matrix G and the risks are
   updated.
8. **Revision 3, major 1:** no rollback after a marker attempt (E stays held until `bridge_complete`), so the barrier
   is dropped and no quiescence bound is needed. The first PUT attempt is the point of no return.
9. **Revision 3, major 2:** one state machine including `sealed` (6.1); an attempt only after the final catch-up; a
   same-E attempt is idempotent; publication is refused from `sealed` on; erasure proceeds in every state with a
   durable, bounded S3 replay before `bridge_complete` (6.2).
10. **Revision 3, major 3:** the mirror gate refuses non-mirrored control callers while sealed (6.3).
11. **Down:** lifecycle functions serialize with the down through (961996,1) shared plus a fresh slot check.
