# Operator per-user erase (buffered desktop uploads)

Status: **code only, default off.** Applying migrations 20261105058000/058010 to any real database, prebuilding the
indexes, running the job against production or turning the flag on each needs Khaliq's explicit OK (AGENTS.md).

## Why

A data-subject deletion request ("delete everything I uploaded") has to remove one user's entire upload footprint.
The per-session operator erase (`docs/operator-session-erase-*.md`) cannot do it:

- it is keyed by session, and refuses lazy/buffered orgs (`RH011`);
- a desktop upload that was accepted but never published has **no `session_index` row**: its records live only in
  the buffered ingest tables (batches, claims, stage intents, outbox, lazy slots, receipt seats, ack decisions) and
  as `buffered-batches/` objects in the transcript bucket.

This was once done by hand for one user. This lane encodes that procedure, including the two failures hit on the
way:

1. **Unindexed foreign keys.** Deleting a lazy slot or receipt seat made PostgreSQL check `ingest_lazy_ack_decisions`
   and `ingest_lazy_native_slots` (both large) by sequential scan, once per deleted row. The erase never finished. 058000 adds the missing indexes: ten foreign-key support indexes (slots, seats, ack decisions, receipt
   rows, stage intents, outbox, history plans), one for the step's un-owned-slot lookup, four `key_material`
   expression indexes on `ingest_receipt_seats` and `ingest_receipt_record_heads` (org, workspace, user and org,
   workspace, origin) for the seat and record-head lookups, and one on `ingest_receipt_ord_seats(candidate_key_sha256)`
   for the step's ord-seat delete: sixteen in all, every lookup that runs under the exclusive erase lock.
2. **One long transaction.** A ~35-minute transaction lost its connection before COMMIT. The slot phase commits in
   bounded steps (`stepLimit` slots per transaction) and the entry uses TCP keepalives. The **final purge is NOT
   bounded by `stepLimit`**: it deletes every other per-user row (batches, claims, stage intents, outbox, history
   plans, ownership, receipts, consent, ...) in one transaction, so its size and duration scale with the user's
   batches, claims and stages. Every call runs with `SET LOCAL lock_timeout = 30s` and
   `SET LOCAL statement_timeout = 15min` (`USER_ERASE_LOCK_TIMEOUT_MS`, `USER_ERASE_STATEMENT_TIMEOUT_MS` in
   `src/session-erase/user-erase.ts`): a final purge that runs past 15 minutes fails with `57014` and rolls back
   whole, the request stays `purging`, and the job exits `4` (rerun; a user that large needs the owner to raise the
   bound deliberately, not a retry loop). A lock wait past 30 s fails with `55P03`, also exit `4`.

## Scope

- One `(org_id, workspace_id, user_id)`.
- **Tombstoned**: `ingest_lazy_native_slots` (owned by the user, or un-owned under a captured origin). The slot row
  stays (20261104055000: the global native-key initialization persists so a stale source row can never reseed the
  slot) but holds nothing of the user: `key_material` becomes `["operator-user-erase-tombstone-v1"]`, `origin_id`
  and `record_id` become `''`, every `current_*` column `NULL`; `key_sha256`, org, workspace, lineage and the hex
  source/epoch/witness hashes stay. A later delivery of the same record id from the same device origin computes the
  same key, meets the tombstone and is refused before it writes anything (`55000` 'native custody identity changed').
  So **those record ids can never be uploaded again from that origin**; new records or a new origin are unaffected.
- **Deleted** (every row of the user; `operator_user_erase_step` deletes them in this order and the final probes
  assert none is left): the rows hanging off the tombstoned slots (`ingest_lazy_ack_decisions`,
  `ingest_history_reconcile_outbox`, `ingest_receipt_record_heads`, `ingest_receipt_ord_seats`) and **every** receipt
  seat of the user, not only each slot's current one (a new seat is created per accepted revision): seats from the
  user's ACK decisions and ordinal seats, and seats whose `key_material` names (org, workspace, user) or (org,
  workspace, captured origin). A seat another user's slot still points to survives, and blocks completion (see
  "Seat ambiguity" below). The `key_material` arm and its
  probe run while holding the exclusive erase lock, which every publication and fence-row write takes shared; they
  use 058000's `key_material` expression indexes on `ingest_receipt_seats` and `ingest_receipt_record_heads`
  (one per OR arm: user, origin; both share the org/workspace prefix) rather than scanning both tables. Then
  `ingest_publication_deferrals`,
  `ingest_publication_images`, `ingest_publication_attempts`, `ingest_history_plan_refs`,
  `ingest_history_plan_carriers`, `ingest_history_plans`, `ingest_buffer_outbox`, `ingest_stage_intents`,
  `ingest_publication_frontiers`, `ingest_claim_sessions`, `ingest_claim_attempts`, `ingest_batches`,
  `ingest_session_ownership`, `ingest_fence_versions`, `upload_deletions`, `upload_resets`, `upload_erasure_rearm`,
  `ingest_control_receipts`, `ingest_source_control_heads`, `ingest_lazy_user_seals`, `ingest_seed_fence_baselines`,
  `ingest_tenant_slots`, `owner_buffered_parity`, `upload_consent_sessions`, `upload_consent_origins`,
  `upload_consent_policies`, `read_api_tokens` (begin and again at the end), and `history.probe_connections` when
  present and deletable by the owner login. **Inventory rule**: every table the purge or a step deletes from is also
  probed by `sessions.operator_user_erase_remaining` (through an index); `user-erase.pglite.test.ts` diffs the two
  function bodies, so a new purge target without a probe fails the test. The user's `ingest_fence_owners` row is the lock anchor of the ingest
  fence (below): it stays while the request is active and `complete` deletes it in the transaction that records
  `bytes_verified`, so a returning user is adopted as a new one.
- **Revoked, rows kept**: the user's `sessions.auth_sessions` in this database (`revoked_reason='operator_user_erase'`
  at begin; the final purge revokes any row that arrived since and asserts none is live). The rows are kept on
  purpose: `auth_sessions_mirror` (20261104020000) keeps a revocation only while the row exists, so deleting it would
  let a replayed Neon mirror row re-insert the token live.
- **Objects**: every version and delete marker under the user's machine prefixes
  `{root}/{org}/{buffered-batches|live-batches|live-sessions}/v1/ws={ws}/m=srv_…/` and `{root}/{org}/v1/ws={ws}/m=srv_…/`,
  for each device origin captured at begin. The machine id is `sha256(ws \x1f origin \x1f user)`, per user, so a
  prefix never holds another user's objects.
- **Refused (`RH040`)**: a user with published or canonical state: `session_index`, `ingest_publication_heads`,
  `ingest_publication_generations`, `ingest_custody_records`, `ingest_frozen_source_owned_sessions`,
  `ingest_frozen_source_records`, `ingest_record_revisions`, `ingest_history_scope_owners`,
  `ingest_history_privacy_outbox`, or an `ingest_history_plans` row in state `published`. The probe runs in `begin`,
  at the start of every `step`, and again at the end of the final purge, so a publication that commits after `begin`
  is caught: that step fails with `RH040`, rolls back whole, and the request stays `purging` (job exit `2`).
  **Operator action on `RH040`**: hand the user's published sessions to the per-session operator erase, then rerun the
  same request (it resumes from `purging`). Do not delete the published rows by hand.
- **Known gap: published AND buffered users.** The per-session erase refuses lazy/buffered orgs (`RH011`), and this
  lane refuses users with published state (`RH040`). A user with both has no automated path today: hold the request
  and hand it to the operator session-erase owner explicitly; do not work around either refusal.
- **Not covered here** (each with the reason; report them in the request):
  - `upload_consent_holds`: operator containment records (`hold_id`, `''` widens a hold); deleting one would release
    a hold. They carry no session or origin, only org/workspace/user and a reason code.
  - `ingest_frozen_source_deletions`, `ingest_frozen_source_resets`: immutable freeze evidence and privacy tombstones
    (they record that sessions were deleted); removing them would weaken the privacy fences.
  - `ingest_acquisition_owners`, `ingest_acquisition_grants`, `ingest_acquisition_grant_origins`: append-only
    ownership fence of the acquisition lane, ids only.
  - `session_directory_users`, `canonical_project_events`, `session_event_facts` and other published/derived read
    models: they exist only for published sessions, which this lane refuses (`RH040`); the per-session erase owns them.
  - `babysitter_evidence_index`, `babysitter_retention_objects`, `migration_*` audit tables, `session_analysis_*`:
    separate lanes with their own retention and erase paths.
  - relayhistory-cloud's Neon `sessions.auth_sessions` and `sessions.probe_connections` copies: a separate database
    this function cannot reach. Revoke them on Neon **before** `begin` (runbook step 0 below).
  - database backups, WAL archives, S3 access logs and CloudTrail: they expire on their retention; report them.
- **Ordering dependency**: 058010 deletes from 20261105057010's `upload_consent_*` tables, so 057010 must be applied
  first. The migrator's contiguity rule enforces that; note that 057010 is landed on main but **not yet applied in
  production**, so 058010 cannot be applied there before it.

## Ingest fence (upload policy)

Uploads for the user are **refused from `begin` until `complete` records `bytes_verified`**, from every device
origin. Deleting rows does not stop a device from uploading again: without the fence a delivery from an origin
`begin` never captured could land after the purge and the request would still be reported verified.

- 058010 replaces 20261104030000's `sessions.ingest_lock_fences`: after it holds the user's fence row `FOR UPDATE`
  it raises SQLSTATE **`RH041`** ('operator user erase: uploads for this user are refused until the erase is
  verified') while an `operator_user_erasures` row for the user has `state<>'bytes_verified'`. Every ingest admission
  path calls it (claim, buffered stage prepare, buffered and direct commit, revisions, lazy records/ACKs/attempts,
  controls, resets, selections, fan-out, publication and history-plan claims, buffered-GC finish, custody, seed).
- `begin` writes the ledger row in the same transaction that holds the fence row, so the refusal starts at begin's
  commit; a writer that waited for begin re-reads in a fresh read-committed snapshot and sees it.
- At the ingest edge the refusal is a **503 `delivery_unavailable`** (stage `claim`, `db_error_code` `RH041`, cause
  `ledger_sqlstate_RH041`); the desktop keeps the batch and retries. The buffered GC treats `sql_RH041` from its
  finish as a deferral. The buffered GC claim (20261105050010's body, replaced here) never leases a stage of a user
  with an active erase.
- Other users are unaffected, including users of the same workspace.
- **Source controls.** `ingest_source_control_begin` and `ingest_privacy_authority_begin` (20261105041000) write the
  user's `ingest_source_control_heads` row without `ingest_lock_fences`. 058010 re-creates both with their exact
  bodies plus one call, `sessions.operator_user_erase_control_gate`, before their privacy-authority lock: it takes the
  erase lock `(961996,50000)` shared and refuses `55000` 'operator user erase: source controls for this user are
  refused until the erase is verified' while the user has an active erase. Nothing is written before the gate. At the
  control routes (`custody.control.begin` and the privacy-authority begin) it surfaces like any other ledger refusal
  there: 503 `delivery_unavailable` (stage `fence`); the client retries the control after the erase. A row trigger on `ingest_source_control_heads` (`operator_user_erase_guard`) refuses any other
  writer of the user's head with the same message; it takes no lock (see "Locking").
- **Privacy authority.** While `ingest_privacy_authority` is `active`, 20261105041000's guard refuses every write to
  `ingest_source_control_heads` unless `bulls96.privacy_authority_writer` is `authority`. The final purge sets that
  marker for its one heads `DELETE` and restores the previous value right after: the erase is an authority-class
  writer of this user's head (it holds the erase lock exclusive and the authority lock shared, and the user's control
  begins are gated). Without it an active authority would leave the request stuck in `purging` with the user's
  uploads refused. **Residual**: authority activation rewrites heads; while a user's erase is active the trigger
  refuses activation writing that user's head (activation fails whole and can be retried after `bytes_verified`), and
  activation after the erase may write a fresh head for the user from authority state; erase the user again after it.
- `complete` re-checks: after the byte phase it takes the same locks and the fence row `FOR UPDATE`, then:
  1. refuses without changing anything if an **upload root** came back (a lazy slot, stage intent, batch, claim
     attempt or claim session, outbox, publication attempt or history plan of the user: bytes may have landed after
     the listing);
  2. **re-runs the final purge** (every metadata table except control heads) idempotently, so a late consent row,
     seal, token, receipt or frontier is deleted rather than stranding the request; the ledger's `deleted` counts
     add up across both runs;
  3. re-runs the full "rows of the user remain" probe. A row still there (a control head, which `complete` never
     drops silently, or an upload root) refuses with `55000` 'operator user erase: rows of the user remain at
     completion'; the request stays `metadata_purged`, the job exits `2` with `user_rows_remain_at_completion` and
     never reports `verified`. Operator action: find and remove that row through its owning lane (for a head: the
     erase guard refuses every control write for this user until the erase is verified, so a head here means a
     writer bypassed the gate; cancel that control through the control lane rather than waiting for it to finish;
     do not edit the ledger), then rerun the same request;
  4. runs the seat-ambiguity check (below).

## Seat ambiguity

A receipt seat whose `key_material` names the user (or a captured origin) but which **another user's** slot still
references is kept: deleting it would break that user's slot. Identity and reference disagree, so the erase fails
closed:

- the final purge counts such seats in the ledger's `retained` (`user_seats_referenced_by_other_slots`); they are
  never counted as deleted;
- `complete` refuses with `55000` 'operator user erase: seats naming the user remain referenced by other slots'; the
  request stays `metadata_purged` and the job exits `2` with `user_seat_ambiguity`. It never certifies
  `bytes_verified` while such a seat exists.

**Manual repair** (the owning ingest lane, never this job, and never by editing the ledger): establish which user the
seat really belongs to. If it is the user under erase, move the other slot off it (clear or re-point that slot's
`current_seat_key_sha256` through the slot's owning lane, and remove that slot's ACK decisions and ordinal seats on
the shared seat); if it is the other user's, record why the `key_material` names the erased user and get the owner's
decision. Then rerun the same request: `complete` deletes the now-unreferenced seat and certifies.

## Phases

| Phase | Where | Effect |
|---|---|---|
| `begin` | `sessions.operator_user_erase_begin(request, org, ws, user)` | Creates (if absent) and locks the user's fence row, deletes the user's `ingest_publication_frontiers` (publication and history-plan claims need them), refuses published state (`RH040`), captures the device origins, revokes this database's `auth_sessions` of the user (rows kept) and deletes the user's `read_api_tokens`, writes the ledger row (`purging`) in that same transaction, which turns on the ingest fence. Idempotent per `request_id`; a request id reused for another user is `22023`; a second active request for the same user is refused (`55000`) until the first reaches `bytes_verified`. |
| `step` × N | `sessions.operator_user_erase_step(request, limit)` | One transaction per call; every call first re-runs the `RH040` probe and the shared-origin refusal (`55000` when another user's batches or slots appeared under a captured origin; nothing is changed). Phase `slots`: up to `limit` lazy slots tombstoned, plus their acks, record heads, ord seats and reconcile rows, and every seat of theirs no other slot references. Last call (`purged`, not bounded by `limit`): the user's remaining seats, every remaining per-user row in foreign-key order, the `RH040` probe once more, then probes prove none is left (`55000` otherwise), ledger → `metadata_purged`. Answers `gc_lease_active` + `retry_after_ms` while a buffered-GC lease is live on the user's stage. |
| bytes | `src/session-erase/user-erase.ts` | Every version/delete marker under each machine prefix (strict paged eraser), settle wait, then a fresh read-only full listing. Anything found (a late in-flight PUT) is erased and re-verified, up to 3 rounds. |
| `complete` | `sessions.operator_user_erase_complete(request)` | Only after the listing found nothing. Same locks as `step`; refuses on a late upload root, re-runs the final purge (heads excepted: late auth sessions revoked, late tokens, consent, seals, frontiers deleted), re-runs the full "rows remain" probe (`55000` otherwise) and the seat-ambiguity check (`55000`), deletes the fence row, ledger → `bytes_verified`, origins cleared (the ledger keeps counts only); the ingest fence lifts. A rerun of a `metadata_purged` request always runs the byte phase and this recheck; only `bytes_verified` short-circuits. |

Locking: `begin`, `step` and `complete` take `pg_advisory_xact_lock(961996,50000)` exclusively; it serializes erases
with each other. Ingest writers take it shared through the 050010 statement trigger on `ingest_fence_owners`, and the
buffered GC claim takes it shared at its start. What excludes a user's writers is that user's `ingest_fence_owners`
row, which `begin`, `step` and `complete` lock `FOR UPDATE` (and, while the request is active, the `RH041` fence): the GC takes it before claiming a stage, and
publication's pointer guard takes it `FOR UPDATE NOWAIT` before writing `session_index`, so a publication racing an
erase step fails fast instead of waiting. `begin` deletes the frontier rows after the fence row (a publication claim
holding a frontier `FOR SHARE` makes it wait, then the `RH040` probe sees what that claim committed); the final purge
deletes frontiers only after the stage rows, the claims' own order (stage row, then frontier). No lock-order cycle.

The privacy-authority lock `(1919249529,96)` joins in one order, always erase lock first: `step` and `complete` take
it shared right after the erase lock; both control begins take the erase lock shared (the gate) and then it shared;
authority activation takes it exclusive and never takes the erase lock; control completes take it shared only. The
heads trigger takes no lock: activation writes heads while holding the authority lock exclusive, so an erase-lock
wait there could deadlock with a step holding the erase lock and waiting for the authority lock. With `step` taking
the authority lock shared, activation and an erase transaction simply wait for each other in one direction.
The bytes phase starts only after the stage rows are gone, so the GC can no longer claim the user's objects.

## Running it

**Step 0, before `begin`: revoke the user on relayhistory-cloud's Neon database** (`sessions.auth_sessions` there, and
delete its `sessions.probe_connections` rows). `begin` revokes only the tokens this Supabase database holds. Until the
Neon side is revoked, a Neon-held token still reads and uploads through relayhistory-cloud, and its mirror can add rows
here. Origins are captured **once**, at `begin`; the ingest fence refuses (`RH041`) every upload of the user from
`begin` until `bytes_verified`, so a device first used after `begin` cannot add rows or objects through ingest. A
second active request for the same user is refused (`55000`) until the first reaches `bytes_verified`; after that a
new request (new `request_id`) erases anything uploaded since.

One-shot job, `SERVICE=user-erase` (`src/session-erase/user-erase-main.ts`):

```bash
OPERATOR_USER_ERASE_ENABLED=1 \
OPERATOR_USER_ERASE_DATABASE_URL_FILE=/run/secrets/sessions-owner-url \
SESSION_ERASE_S3_BUCKET=… SESSION_ERASE_S3_REGION=us-east-1 \
SESSION_ERASE_S3_ACCESS_KEY_ID_FILE=… SESSION_ERASE_S3_SECRET_ACCESS_KEY_FILE=… \
bun run src/session-erase/user-erase-main.ts --request /abs/path/request.json
```

```json
{ "request_id": "dsr-2026-11-11-001", "org_id": "…", "workspace_id": "…", "user_id": "…",
  "confirmation": "ERASE ALL UPLOADS OF THIS USER" }
```

- The database login is the `sessions` schema owner (the functions have no EXECUTE grant).
- The S3 key is the transcript bucket's **eraser** principal: the production bucket policy denies deletes to
  everyone else, including administrator SSO roles.
- Exit codes: `0` verified (or disabled), `3` not verified yet (rerun the same request; every phase resumes),
  `2` refused or misconfigured (do not retry unchanged; `RH040` means hand the published sessions to the
  per-session erase first; `user_rows_remain_at_completion` means an upload root or a control head of the user
  landed after the purge, see "Ingest fence"; `user_seat_ambiguity` means a seat naming the user is still referenced
  by another user's slot, see "Seat ambiguity"), `4` other failure, including a `lock_timeout` (`55P03`) or `statement_timeout`
  (`57014`) (rerun).
- Output is one JSON line of counts; nothing logged carries an id or a key.

Rerunning the same `request_id` is always safe: a finished erase answers from its ledger row.

## Before the first production run

1. Prebuild CONCURRENTLY every 058000 index whose table is populated; an empty table's index may be left to 058000
   (`docs/packets/operator-user-erase-index-packet.md`). Apply 057010 first, then 058000 (adopts the
   prebuilt indexes) and 058010, through the normal reviewed migration path.
2. Store the eraser key and the owner URL in the secret store; never on disk.
3. Run once against a non-production copy with a seeded user and confirm `verified: true`.

## Admin account resets

The admin dashboard submits a durable Cloud Neon job and links to its status. The
services image runs it with `SERVICE=account-reset`, the user-erase configuration
above, `RELAYCAST_URL` set to the HTTPS gateway origin and
`ACCOUNT_RESET_CLOUD_DATABASE_URL` set to a direct/session-mode Cloud Neon URL
with TLS verification. Run one supervised dispatcher. It holds a session advisory
lock and replays external operations using their recorded targets.

Apply `scripts/ops/account-reset/cloud-neon.sql` on Cloud Neon after reviewing the
migration in a disposable environment. It installs the queue, three submission/resume
functions, and guards against rebinding retired Relaycast IDs. Grant the dashboard
EXECUTE on the three public admin enqueue/resume functions and SELECT on the status
columns of `admin_account_resets` (user_id, phase, legacy_relay_only, stalled,
deferred_error, error_code and history_targets). The dashboard needs no queue DML.

The dispatcher needs SELECT/UPDATE on the job table and SELECT/INSERT on
`admin_reset_retired_workspaces`. Its Cloud login reads organizations, users,
memberships, workspaces and the Relaycast registry, including provider ID and key.
It updates users, memberships, API tokens, workspace bindings and registry timestamps,
and deletes the target's Cloud identity. The brief explicit table locks require
the corresponding PostgreSQL LOCK privileges (including UPDATE on organizations).
Use a dedicated role with the privileges needed by the installed invoker-rights
legacy History reset function. It also reads legacy ownership and digest tables,
revokes History auth sessions and deletes only the user's own probe connections.

Submission captures the target's own private organizations and its owned Relaycast
IDs. The dispatcher intersects that scope with current ownership and excludes
organizations with another current or former member on record. It rechecks the
confirmation and commits Cloud revocation, profile scrub and exact cleanup targets
together. Legacy uploads use `reset_uploaded_sessions(org, workspace, user, request)`.
A digest with another contributor or unattributed turns requires operator action.
Modern uploads use the per-user eraser. For RH040, follow the published-session
procedure above. Mixed published/buffered cases still require the erase owner.

Before a Relaycast request the dispatcher checks the current registry owner, provider
ID and every binding, records retirement and detaches the old Cloud bindings in
one short transaction. It calls the engine's explicit-ID endpoint after commit.
A 204 acknowledges retirement; the engine can finish physical removal in its reaper.
A 401 or 404 cannot prove deletion: the job retains its target for investigation.
Rate limits and outages retry. One failed cleanup can be deferred while the other
finishes. The job remains incomplete until the pending cleanup is acknowledged.
After repairing the cause, use the status page to resume the stored job.
