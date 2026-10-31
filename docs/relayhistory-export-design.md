# P1: Neon-to-S3 session export, design

Lane P1 of `docs/relayhistory-pipeline-migration.md`, implementing decision
0002 (S3-first). Design only: running it against production needs Khaliq's
explicit OK (A6), and the per-workspace KMS keys (P9) must exist first.

## What Neon actually holds

Read-only, structure-only samples from Neon production on 2026-10-30
(`TABLESAMPLE`; no content read):

- `sessions.delivery_records` (161 GB, 54M rows) holds the RelayHistory
  client's own history rows, uploaded as records with revisions. Kinds include
  `session`, `session_event`, `tool_call`, `file_edit`, `history`,
  `session_marker`, `relationship`, `presence`, `source_observation`, with
  upserts and deletes. `session` payloads carry the catalog metadata: title,
  `agent_name`, `models_json`, git branch, repo, commit, first and last
  activity, cwd, and so on.
- `sessions.convergence_events` (205 GB) is almost entirely derived: in the
  sample, 99.8% of rows reference a `delivery_record_id`.
- Neon does **not** hold the harnesses' original transcripts byte for byte.
  The raw stream we can export is the client-derived records.

## Unit and layout

- **Delivery path:** the export is a courier whose source is Neon instead of a
  local `history.db`. It uses the same protocol as `src/alleyoop/courier.ts`:
  raw ranges go through the ingest API's server-side scrubbing chunk hop
  (`sealRawChunk`), then the v1 manifest is written, then the pipeline is
  notified. Scrubbing, chunk keys, manifest validation and indexing are
  therefore exactly the live path's, with no second implementation.
- **One exported session** is one `(workspace_id, machine_id, harness,
  session_id)` in the v1 layout:
  `v1/ws=…/m=…/h=…/d=…/s=…/manifest.json` plus
  `raw/{seq:010}-{offset}-{sha8}.ndjson.zst`.
- **Harness** is the record `source`. **Machine** comes from the record's
  origin (resolve `origin_id` to a machine through `delivery_origins` and
  `machines`; open item O1).
- **`d=`** is the session's first activity date (UTC).

## Raw stream

- **Format `ndjson`:** one line per live record, its latest revision only.
  Each line is `{kind, record_id, origin_id, revision, received_at, payload}`.
  Records whose latest revision is a delete are omitted, so deleted content is
  never exported.
- **Order:** by event time (payload `ts_ms`, `timestamp_ms` or `created_ms`),
  then `record_id`. The stream is deterministic, so a re-export produces the
  same chunk hashes.
- **Chunks:** sealed at a record boundary at 4 MiB uncompressed and
  compressed with zstd. Each has `records`, `first_ts` and `last_ts`. A cold
  export creates epoch 0. When live delivery created the manifest after T0,
  the export appends one epoch whose reason is `backfill`; it never rewrites
  the live epochs.
- **Scrubbing:** done by the ingest hop (`sealRawChunk`, rule set
  `relayhistory-scrub`), as for live uploads. It sets
  `raw.scrub = {ruleset, version}`. Unscrubbed manifests are quarantined by
  every reader, so the export never writes chunks directly.
- **Marker:** `extensions["client.relayhistory_export"] = {version: 1, source:
  "neon.delivery_records", exported_at}`. Its presence tells the transform this
  is the RelayHistory record shape, not a harness log.

## Metadata for `session_index` and V1 search

The index stage currently leaves title, model, git and agent null, and it
takes identity only from the authenticated request (`ctx`), never from the
manifest body.

- **Exporter:** writes the latest `session` record's descriptive fields into
  `extensions["client.relayhistory_export"].catalog`: title, `agent_name`,
  models, git branch, commit and repo, first and last activity, plus Neon
  rollup totals (events, tokens, cost).
- **Index stage (code change):** fills `title`, `model`, `git_branch`,
  `git_commit` and the usage columns from that block, and also from the
  equivalent block once live clients send it. The agent name goes where P5
  decides. Summaries come later from S3 (P8a), so the exporter doesn't supply
  them.
- **Identity (open item O2):** `user_id` must come from an authenticated
  path. The export delivers each manifest through `POST
  /v1/pipeline/manifests` with an org-scoped migration credential allowed to
  assert the session's `user_id` from Neon. That needs an explicit, audited
  migration grant in the ingest API. Without it, rows land as `unknown`.

## Safety (reused from #28)

- **Source:** Neon is read with `relayhistory_readonly` only.
- **Order:** newest sessions first, by last activity.
- **Deletion fence:** skip any session with a `deleted_upload_sessions`
  tombstone or a reset covering its user, re-checked per page.
- **Checkpoints:** v2, bound to the endpoints (Neon and bucket) and the plan.
- **Write guard:** `--execute`, the expected bucket and AWS account, and the
  KMS key alias pattern. Without it the run is a dry run: it reports counts
  and sizes and writes nothing.
- **Idempotent:** an existing manifest with an equal `raw.content_sha256` is
  skipped. A different one is a conflict that needs a human, never an
  overwrite.
- **Encryption:** every object is put with SSE-KMS under the workspace's key
  (P9).
- **No new traffic to Supabase:** the pipeline's index stage does the Supabase
  writes, as for any upload.

## Open items

- **O1 (resolved):** `delivery_origins` has no machine column. The machine
  comes from the session's `convergence_events.machine_id` (99.8% of events
  link to a delivery record). Records of a session with no event fall back to
  machine `unknown-origin-<origin_id>`, so they never merge into another
  machine's stream.
- **O2:** identity. The ingest API takes `user_id` from the bearer token, as
  for every courier. The export needs a RelayAuth assertion per Neon user: a
  migration service principal with an audited, org-scoped "act as this user
  for ingest" grant, time-boxed to the export. This is an auth decision for
  the RelayAuth and ingest owners, and Khaliq.
  Ingest side (E3, #43 with relayhistory-cloud #228; nothing applied or
  enabled). The contract is
  [docs/contracts/relayhistory-export-plan-entry-v1.md](contracts/relayhistory-export-plan-entry-v1.md):
  - **Switches:** `RELAYHISTORY_ACCEPT_MIGRATION_ASSERTIONS=true` (default
    off) admits only migration-class tokens and never a generic RelayAuth
    bearer. `RELAYHISTORY_MIGRATION_GRANTS` lists the accepted
    `grantId=planDigest` pairs. The broad `ALLOW_INTERIM_RELAYAUTH_BEARER` is
    off unless explicitly set to `true` (the local Docker stack sets it).
  - **Routes:** only `POST /v1/pipeline/chunks`, `POST /v1/pipeline/manifests`
    and `GET /v1/pipeline/manifests/status` (anything else is 403
    `migration_assertion_route`).
  - **Membership:** the ingest does not look up live workspace membership.
    The grant operator checks it when creating the grant, and relayauth
    #111 (@2adf414a) mints only for the grant's stored pairs.
  - **Identity:** the assertion carries an issuer-derived `meta.upload` (relayauth #111 @2adf414a). The
    ingest recomputes it from the actual request (key, machine, session,
    content, entry digest) and compares (else 403
    `migration_upload_forbidden`). The signed pair must also be the grant
    pair (else 403 `migration_workspace_forbidden`).
  - **Expiry:** 60 s plus 5 s skew. An expired token gets 401
    `migration_assertion_expired`; mint again.
  - **Single use:** 409 `migration_assertion_replayed` (mint again); 503
    `migration_ledger_unavailable` fails closed. The ledger row and the
    enriched `pipeline_jobs` row commit in one transaction.
  - **Chunks:** each request carries its canonical plan entry in front of
    the bytes. The chunk must be one of the signed entry's chunks, checked
    against the actual bytes before anything is stored. Also one chunk per
    `(jti, seq)` (409 `migration_chunk_conflict`) and at most 256 per
    assertion (403 `migration_chunk_limit`).
  - **Audit:** one durable, append-only `sessions.migration_ingest_audit` row
    per commit, in the same transaction as the job.
  - **Retention:** no cron. Each ledger insert deletes at most 100 rows
    expired for more than 5 minutes. An expired token is refused before the
    ledger is read.
- **O3 (resolved in E4):** the transform recognizes
  `client.relayhistory_export`, reads the canonical record envelopes from all
  live and backfill epochs, keeps the highest numeric revision of each
  `(origin_id, record_id)`, applies tombstones and orders the survivors by
  event time. It writes those live records to
  `normalized/v1/relayhistory-{raw.content_sha256}.ndjson`. The digest in the
  key makes the object create-only, so a late transform cannot overwrite
  output for another manifest revision. Post-T0 live chunks use the same
  record envelope; `operation` is optional for historical upserts and required
  to carry a live tombstone.
- **O4 (resolved in E4):** sessions active across T0 are serialized at the
  manifest CAS boundary. A grant-bound migration assertion may append exactly
  one `backfill` epoch while preserving `raw.owner`, the delivery writer,
  session identity and every prior epoch/chunk. Its `opened_by` is
  `migration:<grantId>`. A live write and export based on the same object
  version cannot both commit; the loser rereads and rebuilds. Normal live
  assertions cannot open a `backfill` epoch.
  The epoch is sealed, and the client continues in a `resumed` epoch at its
  own offset (`manifest-v1.md`, "Backfill"). The courier edits the stored
  manifest, adopts it after a lost race, and with `COURIER_FORMAT=ndjson` sends
  its change feed's rows, whose `origin_id`, `record_id` and `revision` equal the
  Neon records'. A `jsonl` session still cannot take a
  backfill. A backfill stays refused (`activation_required`) until the session's
  writer and every reader in `MANIFEST_READER_CAPABILITIES` advertise
  `manifest-epochs-v2`, so an old courier keeps delivering and its backfill
  waits. Still open: a session cold-exported first and resumed later stays
  fenced on its client.
- **O5:** volume. About 54M records in 161 GB of payload; estimate the
  exported zstd size and the PUT count per workspace (KMS request cost, P9).

## Gate

A fixture session from a Neon-shaped database must export to MinIO, pass the
manifest validator, and come through the pipeline as a `session_index` row
whose title, model, git, times and usage equal the Neon rollup. The deletion
fence, checkpoint resume, dry run and the guard's negative tests must all pass.
The rehearsal script then runs it end to end on local containers.
