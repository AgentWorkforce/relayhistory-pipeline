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

On 2026-11-11 this was done by hand for one user (101,872 records, 256 batches). This lane encodes that procedure,
including the two failures hit on the way:

1. **Unindexed foreign keys.** Deleting a lazy slot or receipt seat made PostgreSQL check `ingest_lazy_ack_decisions`
   (~74M rows) and `ingest_lazy_native_slots` (~28M) by sequential scan, once per deleted row. The erase never
   finished. 058000 adds the five missing indexes.
2. **One long transaction.** A ~35-minute transaction lost its connection before COMMIT. The lane commits in bounded
   steps (`stepLimit` slots per transaction) and the entry uses TCP keepalives.

## Scope

- One `(org_id, workspace_id, user_id)`.
- Covered: every ingest-ledger row of the user (`operator_user_erase_step` lists the tables), the user's auth
  sessions (revoked at begin, deleted at the end), `history.probe_connections` when present and deletable, and every
  object version and delete marker under the user's machine prefixes:
  `{root}/{org}/{buffered-batches|live-batches|live-sessions}/v1/ws={ws}/m=srv_…/` and `{root}/{org}/v1/ws={ws}/m=srv_…/`,
  for each device origin captured at begin. The machine id is `sha256(ws \x1f origin \x1f user)`, per user, so a
  prefix never holds another user's objects.
- **Refused (`RH040`)**: a user with published or canonical state (`session_index`, publication heads/generations,
  custody records, frozen-source sessions, record revisions). Erase those sessions with the per-session operator
  erase first; it owns search documents and canonical transcript bytes.
- **Known gap: published AND buffered users.** The per-session erase refuses lazy/buffered orgs (`RH011`), and this
  lane refuses users with published state (`RH040`). A user with both has no automated path today: hold the request
  and hand it to the operator session-erase owner explicitly; do not work around either refusal.
- **Not covered here** (separate services; do them in the same request):
  - relayhistory-cloud's Neon `sessions.auth_sessions` and `sessions.probe_connections` copies (the Supabase mirror
    never clears a revocation, so revoke/delete them on Neon too);
  - database backups, WAL archives, S3 access logs and CloudTrail (they expire on their retention; report them).

## Phases

| Phase | Where | Effect |
|---|---|---|
| `begin` | `sessions.operator_user_erase_begin(request, org, ws, user)` | Ledger row (`purging`), every auth session of the user revoked (`revoked_reason='operator_user_erase'`), device origins captured. Idempotent per `request_id`; a request id reused for another user is `22023`; a second active request for the user is `55000`. |
| `step` × N | `sessions.operator_user_erase_step(request, limit)` | One transaction per call. Phase `slots`: up to `limit` lazy slots plus their seats, acks, record heads, ord seats and reconcile rows. Last call (`purged`): every remaining per-user row in foreign-key order, then index-backed probes prove none is left, ledger → `metadata_purged`. Answers `gc_lease_active` + `retry_after_ms` while a buffered-GC lease is live on the user's stage. |
| bytes | `src/session-erase/user-erase.ts` | Every version/delete marker under each machine prefix (strict paged eraser), settle wait, then a fresh read-only full listing. Anything found (a late in-flight PUT) is erased and re-verified, up to 3 rounds. |
| `complete` | `sessions.operator_user_erase_complete(request)` | Only after the listing found nothing: ledger → `bytes_verified`, origins cleared (the ledger keeps counts only). |

Locking: `begin` and `step` take `pg_advisory_xact_lock(961996,50000)` exclusively (the erase gate every writer
shares), then `step` locks the user's `ingest_fence_owners` row, the buffered GC's own lock order. The bytes phase
starts only after the stage rows are gone, so the GC can no longer claim the user's objects.

## Running it

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
  `2` refused or misconfigured (do not retry unchanged), `4` other failure (rerun).
- Output is one JSON line of counts; nothing logged carries an id or a key.

Rerunning the same `request_id` is always safe: a finished erase answers from its ledger row.

## Before the first production run

1. Prebuild the three large-table indexes CONCURRENTLY (`docs/packets/operator-user-erase-index-packet.md`), then
   apply 058000 (adopts them) and 058010 through the normal reviewed migration path.
2. Store the eraser key and the owner URL in the secret store; never on disk.
3. Run once against a non-production copy with a seeded user and confirm `verified: true`.
