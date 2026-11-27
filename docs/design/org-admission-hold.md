# Per-org admission hold: an enforced writer fence for the fence-authority marker

Status: design for joint review (b1 and layered-sessions-pipeline). No migration until both agree. Slot
`20261105062320_ingest_org_admission_fences` is reserved (registry receipt 251934773687459840). Code only: applying
it to any real database needs Khaliq's explicit OK in his own words. backlog-ops authority-execute stays dormant until
this lands, is applied, and the marker step binds to it.

## 1. Why

Writing an org's fence-authority marker (`ledger-s3.ts` `validOrgFenceAuthority`) asserts that nothing admits that
org through SQL any more. Read-only evidence (`src/session-ingest/authority-evidence.ts`) can only corroborate that
at its sample times: across four review rounds, a claim landing between the last read and the marker write was always
possible (receipts 251926796863643648, 251930556919701504, 251934548453982208). The guarantee has to be *enforced* by
the database from a known instant on, and every later step bound to that instant.

The Worker's `INGEST_HTTP_ADMISSION_STOPPED_ORGS` stop stays (it stops HTTP traffic early). The hold is the SQL-side
enforcement that does not depend on which caller reaches the database.

## 2. Caller matrix (summary)

Latest function bodies, by migration filename order. Full matrix with every line reference: appendix A.

| Path | Latest body | Creates new admitted content? | Under the hold |
|---|---|---|---|
| Claim, buffered: `ingest_claim_buffered_batch` (and the origin wrapper `ingest_origin_claim_buffered_batch`) | 043850:1130, 062010:105 | yes (new batch row via `ingest_claim_batch_mode`) | **refused** |
| Claim, canonical: `ingest_claim_batch` | 050000:306 | yes (same INSERT); calls no admission guard | **refused** |
| The single INSERT point: `ingest_claim_batch_mode` | 050010:117 (INSERT 303) | every claim goes through it | enforcement point (trigger, section 3) |
| Commit of an existing claim: `ingest_commit_buffered_batch` (+ origin wrapper), seat-ACK trigger -> `ingest_lazy_ack_batch` | 050010:373, 062210:190 | yes, for an already claimed batch | impossible: install requires zero pending batches, and a pending -> committed transition is refused |
| Stage for an existing claim: `ingest_prepare_buffer_stage` | 043850:1172 | stages bytes of an existing claim | impossible (no pending claim) |
| Canonical commit: `ingest_commit_batch`, `ingest_commit_revisions` | 050010:441, 060000:907 | yes, given a valid canonical claim token; **no admission guard** | **open item 1**: hold check added to both, or proof they cannot write without a pending canonical batch |
| Seeding: `ingest_seed_proven_records` (seeder role) | 040000:280 | imports proven S3 content; no admission check | **open item 2**: refuse (default) or allow as catch-up |
| Admission-row writers: `ingest_lazy_begin` bootstrap, renewal, repair, rebind, cutover ceremony | 041000:893; renewal :503; repair :514; rebind :65 | no (rows gate admission; they admit nothing) | **untouched**: renewal and rebind are all-or-nothing bulk statements (risk 1); a bootstrapped row inside a refused claim rolls back with it |
| Gate readers: `ingest_lazy_guard`, `ingest_require_buffer_admission` | 044030:312, 055000:514 | no | **untouched**: shared with publication, history plans and ordered controls (risk 2) |
| Controls: selection, reset, self-host erase, consent, privacy authority, custody, operator session/user erase, buffered GC | appendix A.E | no (removal, consent, custody state) | **proceed** |
| Catch-up and bridge: publication claim/publish/heartbeat, history plans, `buffered-drain.ts`, the fence backfill, accepted-stage export | appendix A.C/F | no new content (publish already accepted content; drain/backfill/export write S3 only, read SQL only) | **proceed** |

## 3. Design

### Objects (slot 062320)

- `sessions.ingest_org_admission_holds(held_org text, epoch uuid PRIMARY KEY, state text CHECK (state IN
  ('held','released')), installed_at timestamptz, released_at timestamptz, release_reason text)`, with a partial
  unique index on `(held_org) WHERE state='held'` (at most one live hold per org). The column is deliberately
  **not** named `org_id` and the table has no `workspace_id`/`user_id`: `ingest_lazy_begin`'s bootstrap scan
  (041000:901) treats any `ingest_%`/`upload_%` table with `org_id` as prior authority, and `ingest_lazy_user`
  (041000:776) scans `workspace_id`/`user_id`; a hold row would otherwise break claims *and* controls with 55000, and
  the repair operator's scan hash would refuse (same reason 062010's `ingest_origin_claim_marks` avoids them).
- `sessions.ingest_org_admission_hold_events(epoch uuid, kind text CHECK (kind IN ('installed','released')), at
  timestamptz, reason text)`: insert-only audit.
- Immutability triggers on both tables: no DELETE, no TRUNCATE of these two tables, and the only UPDATE is a hold's
  single `held -> released` transition with every other column unchanged. Owner-only, RLS on, REVOKE ALL from every
  runtime role (as `operator_user_erasures`, 058010:104-128).
- Owner-only functions (lane prefix `sessions.ingest_org_admission_hold_`): `_lock(org, exclusive)`, `_check(org)`,
  `_install(org) RETURNS uuid`, `_release(org, epoch, reason)`, the trigger functions.

### The stable per-org key

`pg_advisory_xact_lock[_shared](hashtextextended('96bulls:ingest-org-hold-v1:' || org, 0))`, a new namespace in the
single-bigint form (no collision with the keys in appendix A.G). It exists whether or not any hold, admission or fence
row exists, so the bootstrap/missing-row case serializes on it too. Collisions only over-serialize.

### Enforcement point

`_check(org)`: requires `transaction_isolation = 'read committed'` (else 40001, retryable, as 043500:39 and the
privacy authority lock 041000:85), takes the org key **shared**, then, in a later statement (so a fresh snapshot,
taken after the lock was granted), raises `RH042` if a `held` row exists for the org.

It runs from two triggers on `sessions.ingest_batches`:
- `BEFORE INSERT FOR EACH ROW`: every claim, buffered or canonical, inserts through `ingest_claim_batch_mode`
  (050010:303), so one trigger covers every claim entry without replacing any claim function.
- `BEFORE UPDATE OF state FOR EACH ROW WHEN (OLD.state = 'pending' AND NEW.state = 'committed')`: no claim can
  complete under a hold (belt and braces; install already requires none pending).

The check is **not** placed in `ingest_lazy_guard`, `ingest_require_buffer_admission`, the admission row or
`ingest_lock_fences` (risks 1 and 2): those are shared with publication, catch-up and controls.

### Install, release

- `_install(org)`: read committed required; takes the org key **exclusive**: waits for every in-flight claim of the
  org (they hold it shared until their transaction ends). Then refuses if a live hold exists, refuses (`RH042`) if the
  org has any `pending` batch (fresh statement after the lock), inserts the hold with a fresh epoch and the
  `installed` audit row, returns the epoch.
- `_release(org, epoch, reason)`: `reason IN ('bridge_complete','verified_rollback')`; exclusive key; requires the
  live hold to carry exactly that epoch; flips it to `released` and writes the audit row. Nothing else releases.

### Lock order and why it is sound

A claim takes, in order: origin advisory (origin wrapper only) -> marker/policy/admission row (lazy begin) -> 50000S ->
fence row -> clock -> native-origin advisory -> **org key S at INSERT** (inside the trigger). Install takes only the
org key X, then reads `ingest_batches` and writes the two hold tables: it never waits on a lock a claim holds while
holding one the claim needs, so no deadlock cycle exists.

Both orderings:
- **Claim first.** The claim holds the key shared (granted at its INSERT) until it ends; install waits; once the claim
  commits, install's fresh statement sees its batch as `pending` (or committed, if its commit was in the same
  transaction) and refuses on pending, or proceeds when it was a committed batch. Either way nothing is admitted after
  the hold.
- **Install first.** The claim's trigger waits for the key; when install commits, the trigger's next statement (fresh
  snapshot, read committed) sees the hold and raises `RH042`; the claim rolls back with everything it did (bootstrap
  row, slots) in the same transaction.
- **Older snapshot.** A REPEATABLE READ or SERIALIZABLE caller is refused by `_check` (40001) before it could read a
  stale snapshot; every admission path already runs read committed (`ingest_lazy_records` requires it, 043500:39).

### Epoch binding and the protocol

stop (Worker, verified) -> queue drained -> **install (epoch E)** -> baseline **under the hold** (`authorityBaseline`
in the backfill's REPEATABLE READ snapshot, which must show the hold `held` with epoch E visible and zero pending) ->
backfills in that snapshot -> evidence run bound to the baseline (re-verifies E held and both triggers enabled) ->
marker write (its receipt records the hash of E; the marker document's shape is fixed) -> bridge -> `_release(E,
'bridge_complete')`. A marker write whose outcome is ambiguous keeps the hold; a verified rollback releases with
`'verified_rollback'`. A retry or replay binds to the same E and refuses if E is no longer `held`.

Because the baseline is taken *under* the hold and the install required zero pending batches, everything admitted
before the hold is committed in SQL and read by the backfill; the pre-hold interval needs no observational proof.

### Maintenance and owner boundary

The hold binds every caller that admits through the sessions functions, owner-run operators included. It does not
bind the schema owner or a superuser acting on the tables directly: DISABLE TRIGGER, `session_replication_role`,
direct DML on `ingest_batches` or the hold tables, running this slot's rollback, or TRUNCATE. There is **no** global
BEFORE TRUNCATE trigger (it would block maintenance for every org for one org's hold). Instead the runbook forbids
those operations on `sessions` while any hold is `held`, and every bound step re-verifies, at its own read, that both
triggers are enabled and the hold is `held` with its epoch.

### Rollback

A generated `.down.sql` (lane `extraOutputs`, as 044080): refused while any hold is `held` (a held org would silently
reopen), and while a later slot is applied; then drops the triggers, functions and both tables. Readiness marker
handling as 062210 if review asks for it.

## 4. Tests

pglite (unit) and real PostgreSQL 15/16/17 (`*.postgres.test.ts`, pg-proof), two independent sessions each:
1. claim-first: install waits for an in-flight claim (observed blocked on the key), then refuses on pending /
   proceeds on committed.
2. install-first: a claim started after install began waits, then gets RH042 and rolls back fully (no batch, slot,
   bootstrap admission row).
3. older snapshot: REPEATABLE READ and SERIALIZABLE claimers get 40001 before reading the hold.
4. pending -> committed refused under a hold.
5. proceed under a hold: selection, reset, self-host erase, consent, privacy authority, custody, operator erases,
   buffered GC, publication claim/publish/heartbeat, history plans; bulk renewal, rebind and repair (all orgs, one
   held).
6. canonical commit paths and seeding per open items 1 and 2.
7. release: wrong epoch, wrong state, bad reason refused; audit immutable; DELETE/TRUNCATE/other UPDATE of the hold
   tables refused; rollback refused while held.
8. the bootstrap scan and repair scan hash unchanged by the new tables.

## 5. Open items for the review

1. `ingest_commit_batch` / `ingest_commit_revisions` (canonical, no admission guard): add `_check` to both (a
   CREATE OR REPLACE of their latest bodies, rollback restores them), or prove they cannot write for an org with no
   pending canonical batch.
2. `ingest_seed_proven_records`: refuse under a hold (default) or allow as catch-up.
3. Whether the readiness marker goes back to pending on rollback (as 062210).
4. Release reasons: are `bridge_complete` and `verified_rollback` the full set.

## Appendix A

The full caller matrix (functions, security, isolation, lock order, file:line, risks):
[org-admission-hold-caller-matrix.md](org-admission-hold-caller-matrix.md).
