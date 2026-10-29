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
| `96b-hoopdb-migrate` | 96bulls/services | one-shot: shim roles + vendored `sessions` migrations + 96bulls migrations, ledger `sessions.__96bulls_migrations` |
| `96b-alleyoop-ingest` | 96bulls/services | the vendored Hono pipeline routes on Node/Bun with Postgres deps; Technical Foul runs in the chunk hop (port 18080) |
| `96b-hoopdb-indexer` | 96bulls/services | stages `object-landed`, `session-indexed` (our index, fans out), 5-minute manifest reconcile |
| `96b-referee` | 96bulls/services | stage `status-classify` |
| `96b-bob-costas` | 96bulls/services | stages `summary-short`, `summary-long` |
| `96b-alleyoop-courier` | 96bulls/services | watches the seeded Concourse, uploads via the chunk hop, writes manifests |
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
3. The courier writes `manifest.json` (validated with the vendored validator,
   hash chain and all; `extensions["96bulls"]` carries the session uuid) with
   `x-amz-meta-session-uuid` and `-trace-id`, then POSTs
   `/v1/pipeline/manifests`. The ingest re-reads the manifest from storage and
   enqueues `object-landed` into `sessions.pipeline_jobs` with the ids in `ctx`.
4. The indexer handles `object-landed` (vendored), then `session-indexed`
   (ours): `sessions.upsert_session_index`, `session_index_ext` (session uuid),
   then fans out `summary-short`, `summary-long`, `status-classify`.
5. Referee writes a verdict (`set_session_state` + `session_verdicts`). Bob
   Costas reads the scrubbed chunks and writes `session_summaries`. Both are
   idempotent by revision / content hash. Model calls are off by default.
6. Every hop records `sessions.pipeline_events` (SQL sink) and a span whose
   trace id is the pipeline trace id; `GET /v1/pipeline/traces/:id` answers.

## Auth, locally

The vendored middleware accepts an RS256 JWT verified against a JWKS URL. `up`
mints a key pair; the ingest serves `/.well-known/jwks.json`; the courier token
is in `.96bulls/local/inputs.json`. Hosted and customer installs point
`RELAYAUTH_JWKS_URL` at RelayAuth instead. `96bulls token local` mints one for
manual calls.

## Known limits

- Alchemy's Docker provider has no health gating; the CLI waits.
- No epoch handling in the courier yet (a transcript that shrinks is skipped with a warning).
- `hoopdb-indexer` is one replica (reconcile has no leader election yet).
- Chunk objects carry the upstream scrub metadata but not `session-uuid`; adding it to the chunk hop is an upstream change we will propose.
- A forced removal of running containers under a busy Docker daemon was seen to hang for 20+ minutes on one machine; `down` now stops containers first and waits.
