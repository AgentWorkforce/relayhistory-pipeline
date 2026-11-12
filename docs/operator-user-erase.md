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
   rows, stage intents, outbox, history plans), one for the step's un-owned-slot lookup, and four `key_material`
   expression indexes on `ingest_receipt_seats` and `ingest_receipt_record_heads` (org, workspace, user and org,
   workspace, origin) for the seat and record-head lookups that run under the exclusive erase lock.
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
  workspace, captured origin). A seat another user's slot still points to survives. The `key_material` arm and its
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
  present and deletable by the owner login. The user's `ingest_fence_owners` row is the lock anchor of the ingest
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
- `complete` re-checks: after the byte phase it takes the same locks, revokes or deletes tokens that arrived since,
  and re-runs the full "rows of the user remain" probe. A row that landed after `metadata_purged` through a writer
  the fence does not cover (e.g. consent rows written by the control plane) refuses the completion with `55000`
  'operator user erase: rows of the user remain at completion'; the request stays `metadata_purged`, the job exits
  `2` with `user_rows_remain_at_completion` and never reports `verified`. Operator action: find and remove that row
  through its owning lane (do not edit the ledger), then rerun the same request.

## Phases

| Phase | Where | Effect |
|---|---|---|
| `begin` | `sessions.operator_user_erase_begin(request, org, ws, user)` | Creates (if absent) and locks the user's fence row, deletes the user's `ingest_publication_frontiers` (publication and history-plan claims need them), refuses published state (`RH040`), captures the device origins, revokes this database's `auth_sessions` of the user (rows kept) and deletes the user's `read_api_tokens`, writes the ledger row (`purging`) in that same transaction, which turns on the ingest fence. Idempotent per `request_id`; a request id reused for another user is `22023`; a second active request for the same user is refused (`55000`) until the first reaches `bytes_verified`. |
| `step` × N | `sessions.operator_user_erase_step(request, limit)` | One transaction per call; every call first re-runs the `RH040` probe and the shared-origin refusal (`55000` when another user's batches or slots appeared under a captured origin; nothing is changed). Phase `slots`: up to `limit` lazy slots tombstoned, plus their acks, record heads, ord seats and reconcile rows, and every seat of theirs no other slot references. Last call (`purged`, not bounded by `limit`): the user's remaining seats, every remaining per-user row in foreign-key order, the `RH040` probe once more, then probes prove none is left (`55000` otherwise), ledger → `metadata_purged`. Answers `gc_lease_active` + `retry_after_ms` while a buffered-GC lease is live on the user's stage. |
| bytes | `src/session-erase/user-erase.ts` | Every version/delete marker under each machine prefix (strict paged eraser), settle wait, then a fresh read-only full listing. Anything found (a late in-flight PUT) is erased and re-verified, up to 3 rounds. |
| `complete` | `sessions.operator_user_erase_complete(request)` | Only after the listing found nothing. Same locks as `step`; revokes late auth sessions, deletes late read-API tokens and frontiers, re-runs the full "rows remain" probe (`55000` otherwise), deletes the fence row, ledger → `bytes_verified`, origins cleared (the ledger keeps counts only); the ingest fence lifts. A rerun of a `metadata_purged` request always runs the byte phase and this recheck; only `bytes_verified` short-circuits. |

Locking: `begin`, `step` and `complete` take `pg_advisory_xact_lock(961996,50000)` exclusively; it serializes erases
with each other. Ingest writers take it shared through the 050010 statement trigger on `ingest_fence_owners`, and the
buffered GC claim takes it shared at its start. What excludes a user's writers is that user's `ingest_fence_owners`
row, which `begin`, `step` and `complete` lock `FOR UPDATE` (and, while the request is active, the `RH041` fence): the GC takes it before claiming a stage, and
publication's pointer guard takes it `FOR UPDATE NOWAIT` before writing `session_index`, so a publication racing an
erase step fails fast instead of waiting. `begin` deletes the frontier rows after the fence row (a publication claim
holding a frontier `FOR SHARE` makes it wait, then the `RH040` probe sees what that claim committed); the final purge
deletes frontiers only after the stage rows, the claims' own order (stage row, then frontier). No lock-order cycle.
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
  per-session erase first; `user_rows_remain_at_completion` means a row of the user landed after the purge, see
  "Ingest fence"), `4` other failure, including a `lock_timeout` (`55P03`) or `statement_timeout`
  (`57014`) (rerun).
- Output is one JSON line of counts; nothing logged carries an id or a key.

Rerunning the same `request_id` is always safe: a finished erase answers from its ledger row.

## Before the first production run

1. Prebuild CONCURRENTLY every 058000 index whose table is populated; an empty table's index may be left to 058000
   (`docs/packets/operator-user-erase-index-packet.md`). Apply 057010 first, then 058000 (adopts the
   prebuilt indexes) and 058010, through the normal reviewed migration path.
2. Store the eraser key and the owner URL in the secret store; never on disk.
3. Run once against a non-production copy with a seeded user and confirm `verified: true`.
