# Per-org admission hold: an enforced writer fence for the fence-authority marker

Status: revision 2, for joint review by b1 and layered-sessions-pipeline. Revision 1 (8e0463d9) was held; its four
dispositions and the joint disposition are folded in (section 7). No migration code until both agree. Slot
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
| **Admission entry**: checks the hold first | `ingest_origin_claim_buffered_batch` 062010:105; `ingest_claim_buffered_batch` 043850:1130; `ingest_claim_batch` 050000:306; `ingest_claim_batch_mode` 050010:117 (inner; checked again, harmlessly re-entrant); `ingest_origin_commit_buffered_batch` 062010:125; `ingest_commit_buffered_batch` 050010:373; the origin and plain `ingest_prepare_buffer_stage` 043850:1172; **`ingest_commit_batch` 050010:441** (accepts committed rows and can reach revision writes; disposition 1); **`ingest_commit_revisions` 060000:907** (`ingest_attempt_epochs`, 030000:60, validates a token and digest with a NULL batch id and never requires pending; disposition 1); `ingest_lazy_attempt` / `ingest_lazy_ack` / `ingest_lazy_ack_batch` / `ingest_lazy_stage_seats` where runtime-reachable; **`ingest_seed_proven_records` 040000:280** (refused; disposition 2) | **refused**, RH042, before any write. Replays, resumes and fast paths of `claim_mode` (050010:117ff) are refused too, uniformly. A committed-batch replay would be side-effect free, but the buffered wrappers run `ingest_lazy_begin` and `ingest_lazy_records` (a bootstrap admission row, slots) before `claim_mode` decides it is a replay, so the check has to come before them. Replays sit behind the Worker's HTTP stop anyway, and a refusal is retryable. |
| **Control**: proceeds | selection and reset (incl. their `ingest_lazy_begin` and `ingest_lazy_clear` use), self-host erase, consent, privacy authority, source control, custody, operator session and user erase, buffered GC | **proceed**. Removal and consent state only. A control's `ingest_lazy_begin` bootstrap of an admission row admits nothing by itself. |
| **Catch-up / bridge**: proceeds | publication claim, publish and heartbeat; history plans; `buffered-drain.ts` (no SQL); the fence backfill and accepted-stage export (read-only SQL) | **proceed**. Publication writes revisions of previously accepted content (section 5). |
| **Admission-row operators**: untouched | `ingest_lazy_begin` bootstrap (inside a claim it rolls back with the refused claim), renewal, repair, rebind, cutover ceremony | **untouched**. Renewal and rebind are all-or-nothing bulk statements. The hold never reads or writes admission rows. |

Backstop: a `BEFORE INSERT FOR EACH ROW` trigger on `sessions.ingest_batches` runs the same check. It catches a future
inserter that bypasses classification. It is not the primary enforcement.

## 3. Objects (slot 062320)

- `sessions.ingest_org_admission_holds`: `epoch uuid PRIMARY KEY`, `held_org text`, `request_id uuid UNIQUE`,
  `state` (`held` | `marker_attempted` | `released`), `installed_at`, `marker_attempted_at`, `released_at`,
  `release_reason` (`bridge_complete` | `verified_rollback`), `release_evidence_sha256`. A partial unique index
  allows at most one row per org with `state <> 'released'`. The column is deliberately **not** `org_id`, and there is
  no `workspace_id` or `user_id`. `ingest_lazy_begin`'s bootstrap scan (041000:901) treats any `ingest_%`/`upload_%`
  table with `org_id` as prior authority, `ingest_lazy_user` (041000:776) scans `workspace_id`/`user_id`, and the
  repair operator hashes that scan.
- `sessions.ingest_org_admission_hold_events`: `epoch`, `kind` (`installed` | `marker_attempted` | `released`), `at`,
  `detail` (for example `abandoned_before_marker`), `evidence_sha256`. Insert-only. Exactly one event per transition;
  an idempotent repeat writes none.
- Immutability triggers on both tables:
  - No DELETE, and no TRUNCATE of these two tables.
  - The only UPDATEs allowed are `held -> marker_attempted`, `held -> released` and `marker_attempted -> released`,
    each with every identifying column unchanged.
  - Both tables are owner-only, with RLS on and REVOKE ALL from every runtime role.
- Owner-only functions under the lane prefix `sessions.ingest_org_admission_hold_`: `_lock`, `_check`, `_install`,
  `_marker_attempt`, `_release`, and the trigger functions.

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

## 5. Backfill and the snapshot contract

The fence backfill opens a separate REPEATABLE READ transaction per page (`src/session-ingest/upload-fence-backfill.ts`,
:34, :618, :647). A single baseline snapshot therefore cannot attest every page. What can change for a held org during
the backfill:
- **Controls and erase:** they delete revisions and ownership, and the S3 fences carry them.
- **Publication of previously accepted content:** it writes revisions with `updated_at`.
- **GC:** stage and outbox only.

The contract reuses backlog-ops' existing FULL + catch-up reconciliation (docs/backlog-ops-workflow.md):
1. The FULL backfill-execute starts after `_install` returned E.
2. The catch-up runs after FULL with `revisions_since_ms` = FULL's created_at minus the catch-up margin. It re-applies
   every revision changed since then, and it re-runs fences and ownership in full.
3. Both receipts carry the hash of E. The gate refuses them unless E is still non-released when each run starts and
   when it ends.

Admission content cannot change after E (section 4). The only remaining changes are the controls, publication and GC
above, and the catch-up re-reads exactly what they can change. The bound evidence run stays corroboration on top.

## 6. Marker writes, release, rollback

**Point of no return: the first marker PUT attempt** (joint disposition). A timeout or an absent HEAD can coexist
with a delayed write.
- **Before any PUT:** the marker writer calls `_marker_attempt(org, E)` (key X, idempotent for the same E). It
  succeeds only while the hold is `held` or already `marker_attempted` with E. It commits the
  `held -> marker_attempted` transition and its event **before** the first PUT attempt. Every retry of the PUT calls
  it again first.
- **`verified_rollback` from `held`:** allowed only while no attempt has been recorded. `_release` takes key X, so it
  serializes with `_marker_attempt`. A writer that never recorded an attempt never PUT. After the release, every
  `_marker_attempt` for E refuses, so no retried writer can start one. The durable, epoch-bound evidence is the absence
  of any `marker_attempted` event for E, read under the lock. The audit `detail` records `abandoned_before_marker`.
- **After an attempt (`marker_attempted`):** no automatic release. E is retained, and an operator reconciles:
  - outstanding writes, i.e. no writer lease is live;
  - the actual object versions of the org's marker key and of the deployment-wide marker;
  - the effective authority, including the global fallback (`buffered-drain.ts`: a missing per-org marker falls back
    to the deployment-wide one).

  Release then needs either `bridge_complete` or `verified_rollback` with evidence of a verified safe restoration.
- **`_release(org, E, reason, evidence_sha256)`:**
  - **Reasons:** `bridge_complete` or `verified_rollback`, nothing else.
  - **Evidence:** the hash is of a terminal receipt that the gated job authenticates (issuer, run, sha, scope, org and
    E), as backlog-ops binds every other receipt. SQL records it but cannot authenticate it.
  - **Idempotency:** a repeat with the same E, reason and evidence after a lost response returns ok and writes no event.
  - **Refusals:** a stale or other epoch refuses, and so does any other repeat.

**Schema down** (generated by the lane):
- **Refuses** (fail closed) while any hold is not `released`, and while a later slot is applied.
- **Locks and resets** role validation to `pending`, as 062210 does, so readiness has to be re-proven after the
  restored generation and privileges are verified.
- **Removes the enforcement:** the `_check` calls (by restoring the prior bodies exactly), the backstop trigger and the
  functions.
- **Retains** `ingest_org_admission_holds` and `_events` as inert history, so the audit is preserved. A reapply reuses
  them. This is the reviewer's choice between "preserve" and "verified export before drop". I recommend preserve,
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
   the prior bodies exactly, and retains the audit.

## 9. Resolved items (from revision 1)

1. **Canonical commits** (`ingest_commit_batch`, `ingest_commit_revisions`) are guarded at entry. Disposition 1.
2. **Seeding** is refused under the hold. A seed under a hold would need a separately designed epoch-bound operation.
   Disposition 2.
3. **Down:** fail closed, readiness reset to pending, audit preserved. Per-org rollback reconciles the real S3 marker
   versions and the global fallback before release. Disposition 3.
4. **Release:** only `bridge_complete` and `verified_rollback`, with authenticated terminal evidence, idempotent install
   and release, and the marker-attempt point of no return. Disposition 4 and the joint disposition;
   `abandoned_before_marker` is an audit detail, not a release path.
