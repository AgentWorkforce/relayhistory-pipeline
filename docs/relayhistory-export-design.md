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
  compressed with zstd. Each has `records`, `first_ts` and `last_ts`. There is
  one epoch, and `raw.state` is `closed` unless the session saw activity after
  T0 started (then `open`, so live delivery can continue it).
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
Each chunk request carries `canonical(plan entry)` in front of its source
bytes, with the entry's length in `X-Relay-Migration-Plan-Entry-Length`.
A per-manifest checkpoint (ids, digests and work keys,
never tokens) keeps a resumed run from re-uploading committed manifests. It is
tested only against an in-process fake ingest, and the CLI still refuses
`--execute`.

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
- **O3:** the transform's normalizer for `client.relayhistory_export`
  sessions, so replay, briefs and embeddings read them like live sessions.
- **O4:** sessions active across T0. Live delivery after T0 continues the
  session's stream: the export writes epoch 0, and the client opens epoch 1.
  Confirm with the delivery coordinator's owner-fencing rules.
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
