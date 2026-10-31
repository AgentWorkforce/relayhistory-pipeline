# The local stack

`bin/96bulls up local` stands the whole sessions pipeline up as Docker
containers on one machine, from `alchemy.run.ts` (Alchemy v2, Docker
provider). It is the proving ground for the hosted and self-hosted stacks: same
services, same env contract, same migrations.

## What runs

| Container | Image | Role |
| --- | --- | --- |
| `96b-hoopdb` | pgvector/pgvector:pg17 | HoopDb (port 15432 on the host) |
| `96b-locker` | Chainguard MinIO (digest-pinned) | Locker, S3 API (port 19100) |
| `96b-locker-init` | 96bulls/services | one-shot: creates the bucket (SigV4, no SDK), enables versioning |
| `96b-hoopdb-migrate` | 96bulls/services | one-shot (`HOOPDB_DB_MODE=local`, `HOOPDB_SUPABASE_SHIM=true`, `HOOPDB_RUNTIME_ROLE=hoop`): shim roles + search Worker role bootstrap (`supabase/bootstrap`) + `sessions` migrations (supabase/migrations) + 96bulls migrations, ledger `sessions.__96bulls_migrations` |
| `96b-alleyoop-ingest` | 96bulls/services | the pipeline routes from `packages/pipeline` on Node/Bun with Postgres deps; Technical Foul runs in the chunk hop (port 18080) |
| `96b-hoopdb-indexer` | 96bulls/services | stages `object-landed`, `session-indexed` (our index, fans out), 5-minute manifest reconcile |
| `96b-referee` | 96bulls/services | stage `status-classify` |
| `96b-bob-costas` | 96bulls/services | stages `summary-short`, `summary-long` |
| `96b-alleyoop-courier` | 96bulls/services | watches the seeded Concourse and uploads chunks plus validated manifests through AlleyOop |
| `96b-telemetry-init` | alpine | one-shot: opens the telemetry volume for the collector's uid |
| `96b-otel-collector` | otel/opentelemetry-collector-contrib | OTLP in; spans to `/telemetry/spans.jsonl` (port 14318) |

Every worker blocks at start until HoopDb answers and the migration ledger is
complete, so apply order does not matter (the same rule the Kubernetes chart
uses via `/readyz`).

## Flow of one session

1. `96bulls seed` writes fixture transcripts (plus one with shape-only fake
   secrets) into a fake HOME and runs the real `ai-hist sync` to build
   `history.db` (Concourse).
2. The courier reads `sessions` from `history.db`, mints `session_uuid` (UUIDv7)
   and a pipeline `trace_id` once per session, and POSTs each new byte range to
   `/v1/pipeline/chunks` with `traceparent`. The ingest scrubs record by
   record, zstd-compresses and stores the chunk create-only in the Locker with
   scrub metadata. Unscrubbed bytes never touch the Locker.
3. The courier builds `manifest.json` (validated with the vendored validator,
   hash chain and all; `extensions["96bulls"]` carries the session uuid) and
   POSTs it to `/v1/pipeline/manifests`. AlleyOop validates it again, writes it
   create-only (or by compare-and-swap for the same writer's next revision)
   after verifying every referenced chunk carries the server's scrub metadata,
   restores identity and keyed telemetry object metadata, re-reads the
   authoritative copy, and enqueues `object-landed` into
   `sessions.pipeline_jobs` with the ids in `ctx`. The host courier never
   receives Locker credentials.
4. The indexer handles `object-landed` (vendored), then `session-indexed`
   (ours): `sessions.upsert_session_index`, `session_index_ext` (session uuid),
   then fans out `summary-short`, `summary-long`, `status-classify`.
5. Referee writes a verdict (`set_session_state` + `session_verdicts`). Bob
   Costas reads the scrubbed chunks and writes each summary as an encrypted Locker object
   (`summary/{short,long}.json.enc`; the master key is a file in the runtime tmpfs,
   `$XDG_RUNTIME_DIR/96bulls/<stage>/summary-master-keys`, never in `.96bulls/`), with only the
   pointer in `session_summaries`. Both are
   idempotent by revision / content hash. Model calls are off by default.
6. Every hop records `sessions.pipeline_events` (SQL sink) and a span whose
   trace id is the pipeline trace id; `GET /v1/pipeline/traces/:id` answers.

## Auth, locally

The vendored middleware accepts an RS256 JWT verified against a JWKS URL. `up`
mints a key pair; the ingest serves `/.well-known/jwks.json`; the local courier
mints short-lived tokens in memory. Hosted and customer installs point
`RELAYAUTH_JWKS_URL` at RelayAuth instead. A durable host courier sets
`RELAYAUTH_URL`, the exact `RELAYAUTH_HISTORY_ORIGIN`, and
`RELAYAUTH_API_KEY_COMMAND_JSON`, a JSON argv array that reads the dedicated
assertion key from a central broker or secret manager. The command is never run
through a shell, and neither that key nor minted assertions are saved in the
courier ledger. `96bulls token local` remains available for manual calls.

## Ports and credentials

Host ports are bound to `127.0.0.1` only; the ingest API is the boundary, and
HoopDb and the Locker are never reachable from other machines. The courier
uses short-lived in-memory auth and sends all storage writes through AlleyOop.
Its upload ledger (`.alleyoop-courier.json` beside the seeded database)
survives `down`: ids stay stable, and on the next process start the courier
checks its manifests through the authenticated ingest API and re-uploads
missing ones with the same session uuids and trace ids. Host enrollment defaults
to `COURIER_START_MODE=new`, leaving existing sessions local; `all` is the
explicit backfill mode and is set explicitly by the fixture stack. Fixed bearer
tokens require `COURIER_ALLOW_FIXED_TOKEN=1`. `JWT_PRIVATE_KEY_PEM` is local
fixture auth only and is refused in `new` mode unless
`COURIER_ALLOW_LOCAL_SIGNING_KEY=1` is explicitly set.

## Known limits

- Alchemy's Docker provider has no health gating; the CLI waits.
- No epoch handling in the courier yet (a transcript that shrinks is skipped with a warning).
- `hoopdb-indexer` is one replica (reconcile has no leader election yet).
- Chunk objects carry the upstream scrub metadata but not `session-uuid`; adding it to the chunk hop is an upstream change we will propose.
- A forced removal of running containers under a busy Docker daemon was seen to hang for 20+ minutes on one machine; `down` now stops containers first and waits.
