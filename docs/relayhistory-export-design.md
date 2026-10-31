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

- **O4 dedupe watermark:** the marker carries
  `origin_revisions: {<origin_id>: <max exported revision>}`, and each chunk
  carries `source_origin` and `source_end`, which are what #51's takeover
  resume reads from the tail chunk. All of them are derived from the exact
  chunk bytes (the ndjson record lines), so each chunk's signed sha256 binds
  them, and an ingest that holds the source bytes can recompute them. The
  courier checks them before sending, and the fake ingest recomputes them.
  The shape was confirmed with mig-o4:
  - values are safe integers of 0 or more;
  - an empty `{}` is kept;
  - `machine:` origins are left out;
  - an absent field is read as `{}`.
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

## Upload courier (code only, not enabled)

`src/migration/relayhistory-export-upload.ts` delivers a plan manifest by
manifest: chunks through `POST /v1/pipeline/chunks`, then `POST
/v1/pipeline/manifests`, each manifest under its own fresh migration
assertion. One pure function (`derivePlanContract`) gives the canonical,
grant-bound plan entries, the plan digest and one per-upload identity per
manifest, exactly as `docs/contracts/relayhistory-export-plan-entry-v1.md`
(#43) defines them; a test reproduces the contract's vectors byte for byte.
RelayAuth derives the signed `meta.upload` (grant, plan digest, manifest key,
org, workspace, source user, session, content and entry digests) from its
stored plan; a mint names only the manifest key, and the courier refuses an
assertion whose signed plan or identity differs from what it is about to
send. Ids have one canonical form, without the `relay_` prefix.

Retries: one re-mint on 409 `migration_assertion_replayed` (a second fails
that manifest) and on exactly 401 `migration_assertion_expired` (a second
stops the run); at most 256 chunks per assertion, then a fresh mint, and one
re-mint on 403 `migration_chunk_limit`; bounded backoff on 5xx and lost
responses, then stop. Any other 401 or 403 (including
`migration_upload_forbidden`) stops the run; 409 `migration_chunk_conflict`
fails that manifest. 503 `migration_storage_read_required` (an ingest that
cannot read the commit back from storage) stops the run without backoff.
409 `activation_required` (the O4 gate, #51) is neither a lost race nor
terminal: the courier waits a bounded number of times with fresh assertions,
then defers the manifest (re-queued and reported) and moves on.
Each chunk request carries `canonical(plan entry)` in front of its source
bytes, with the entry's length in `X-Relay-Migration-Plan-Entry-Length`.
The commit's work key must be exactly `upload.key + "@" + upload.entry_sha256`
(signed fields only), the same under any fresh assertion, and the manifest
carries the export marker's `grant_id`, `plan_digest` and `source_user_id` and
`96bulls.user_id`, each equal to the signed values.
A per-manifest checkpoint (ids, digests and work keys,
never tokens) keeps a resumed run from re-uploading committed manifests. It is
tested only against an in-process fake ingest, and the CLI still refuses
`--execute`.

## Cold-export ownership and straddling sessions

- A cold export's `raw.owner`, `writer.delivery_instance_id` and initial
  `opened_by` are all `migration:<grantId>`, as merged #43's owner fence
  (`manifestOwnerIssue`) requires. An end-to-end test runs the courier
  against main's real ingest routes.
- Before sending chunks, the courier reads the manifest status. If a live
  session already has a manifest at the key, the export is deferred as
  `straddling_session`: contract v1 cannot sign a backfill entry yet. So the
  exporter never sends a backfill, and #51's `activation_required` gate is
  not reachable from it.

## One set of Cloud ids

`src/migration/relayhistory-export-ids.ts` holds the shared id rules: only
canonical UUIDs are Cloud ids, and a legacy alias counts only through a
reviewed, collision-free mapping ledger. Both sides apply the same rewrite:
- the grant lifecycle, with `canonicalizeUploads`;
- the courier, with `canonicalizeExportPlan`, which `runExportUpload` applies
  with `idMapping` (shard selection does the same).

So the grant's plan, every create request and every manifest the courier
writes carry the same Cloud ids and the same plan digest.

## Grant shards

One grant holds what relayauth #111 admits: at most 64 distinct source users,
1,000,000 bytes of canonical plan plus allowed pairs, 1,000 manifests, 4,096
chunks per manifest and 50,000 chunks per plan.
`src/migration/relayhistory-export-shards.ts` splits the plan into grants and
asks #111's own `deriveMigrationGrantPlan` (vendored unchanged) to admit every
candidate, so whichever limit binds first decides. Sessions never straddle
grants; a session no grant can hold is reported as `oversized_session` and
the plan is refused. Shard ids hash the planner version, contract, org,
`exported_at` and the shard's canonical entries. On a synthetic plan sized
like the export (116,180 sessions, 122,868 chunks) the 1,000-manifest cap
binds: 117 grants, each at most about 0.6 MB and 1,200 chunks. A 4,096-chunk
manifest alone is about 0.6 MB, so the byte cap binds before the 50,000-chunk
cap ever can.

## Grant lifecycle (code only, not wired)

`src/migration/relayhistory-export-grants.ts` has four commands: `dry-run`,
`plan`, `apply` and `revoke`, with one grant per shard.

- **`plan`:** produces an aggregate digest over the grant parameters, the
  pinned membership snapshot id, every shard's id and plan-body digest, and
  the exception list. A human approves that digest plus the snapshot id.
- **`apply`:** fails closed on a missing, changed or stale snapshot, on any
  pair exception, on digest drift, on partial shard state, or on an
  unrecognized origin. It is idempotent per shard id. If it fails partway, it
  revokes the grants created in that run.
- **`revoke`:** with no shard ids, this is the rollback, and it writes
  revocation evidence for each shard.

**Membership:** in Cloud, a user may act on a workspace when they have an
active `organization_memberships` row for the organization that owns it,
joined to `workspaces`. There is no per-workspace table. Whether the export's
ids are Cloud's ids is unverified, so the real reader stays unwired.

- **Ids:** only canonical UUIDs are Cloud ids, and `relay_` is never stripped.
  A legacy alias counts only through a reviewed, collision-free mapping
  ledger, which is empty by default.
- **One read:** a single `REPEATABLE READ READ ONLY` transaction must find all
  of: `workspaces.id` exact, `workspaces.organization_id` equal to the org,
  `users.id` exact, and an active membership row. The snapshot id hashes the
  rows read, together with `read_at`.
- **Identity preflight:** every user must be an active human RelayAuth
  identity in the same org.
- **Blockers:** any invalid, alias, default, inactive, cross-org, unknown or
  ambiguous id blocks its shard and the run.
- **Idempotency (relayauth #112):** each grant create sends a per-shard
  `Idempotency-Key`: base64url of a sha256 over the shard id, the approved
  aggregate digest and the versions. It is never stored or logged. A lost
  response is retried with the same key, and RelayAuth replays the original
  grant. A 409 `idempotency_key_conflict`, any other 4xx, a replayed grant
  that is already revoked, or retries running out halts the run.

- **Ledger:** before `apply` or `revoke` trusts the ledger, it checks the
  exact version and shape, the approved aggregate digest, the source
  snapshot id (on apply), and that every shard is in this plan. For every
  recorded grant, the plan digest must be what relayauth's own derivation
  gives for that grant id and that shard. Anything else is refused.
- **Aliases:** reviewed aliases are rewritten to their Cloud ids in the
  shards, the plan and every create request, including the key's `ws=`
  segment. The courier must build its export plan from the same canonical
  uploads (`canonicalizeUploads`).
- **Revoke:** it converges against relayauth #112. A lost response is
  retried. When the retry gets 409 `migration_grant_revoked`, `revoke` reads
  `revokedAt` back by replaying the shard's create with the same
  Idempotency-Key, which returns the stored grant and creates nothing.

**Not active:** the CLI refuses `apply` and `revoke`. Activation waits for the
authoritative membership reader, the E3 HOLDs, and Khaliq's OK.

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
- **O5 (first measurement):** a read-only dry run over the 50 newest Neon
  sessions (2026-10-30) gave 50 machine streams, 280,144 records, 560 MB of
  raw ndjson and 169 chunks, with none fenced. The newest sessions are the
  largest, so this doesn't extrapolate linearly. A random sample of 120 of
  the 116,180 sessions (read-only) gave:
  - mean 0.83 MB per session; p50 175 KB, p90 1.7 MB, p99 11.5 MB, max
    12 MB; none fenced or empty;
  - extrapolated across all sessions: about 96.5 GB of raw ndjson in about
    124k chunks;
  - after zstd, roughly 12–20 GB stored, and about 240k PUTs including the
    manifests. That PUT count sizes the KMS request cost (P9).
- **O6 (resolved): ingest-API sessions.** About 0.3% of events have no
  delivery record. They come from server-side harnesses (`chatgpt`,
  `grok-bot`) that upload through the ingest API, and 4 of the 50 newest
  sessions consist only of such events. The export adds them to the session's
  stream as `convergence_event` lines (the event row, without tenant
  columns), on the event's own machine. A delivery-records-only export would
  have dropped these sessions entirely.

## Gate

A fixture session from a Neon-shaped database must export to MinIO, pass the
manifest validator, and come through the pipeline as a `session_index` row
whose title, model, git, times and usage equal the Neon rollup. The deletion
fence, checkpoint resume, dry run and the guard's negative tests must all pass.
The rehearsal script then runs it end to end on local containers.
