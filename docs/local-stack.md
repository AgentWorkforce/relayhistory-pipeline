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

The services image also carries `SERVICE=session-read-api` (the session read
API over HTTP, `src/session-read-api/server.ts`). The local stack does not run
it, and it is default-off (`RELAYHISTORY_SELFHOST_READ_API`). Dormant, it
answers `/healthz` only. Enabled, it needs no ClickHouse: search is selected
by `SEARCH_INDEX_BACKEND` (default `postgres`; `clickhouse` keeps the hosted
binding; anything else fails startup), and it also requires
`SEARCH_INDEX_POSTGRES_BACKEND_ID` (with `postgres`) and an explicit
`SESSION_POSTGRES_TLS_PROFILE` (env table below). With `postgres`, search answers
503 `backend_unavailable` until the search schema owner bootstrap and the read
role's grants have run. The read login must be a real login as exactly
`bulls96_read_api`, shaped as `supabase/bootstrap/session_read_api_role.sql`
provisions it (both search tables have forced tenant row-level security). The
service checks the session identity (no `SET ROLE` or `SET SESSION
AUTHORIZATION`: the backend's `pg_stat_activity.usename`; `system_user` on PG16+),
the role's attributes, its memberships in both directions and its pinned
settings at startup, and refuses otherwise (`reader_role_<property>`). See "Postgres search prerequisites" under
"Self-host SERVICE" in `docs/session-read-api-design.md`.

A self-host runs this service with no RelayAuth and no relayhistory-cloud.
With `LOCKER_BUCKET` set, it serves `GET /v1/sessions/:id/timing`, `/turns`,
`/metadata` and `/catalog` from the session's canonical objects in the Locker.
It reads the same `LOCKER_*` names the other services read; MinIO is
`LOCKER_ENDPOINT=http://minio:9000`. With `LOCKER_BUCKET` unset, those routes
keep their 501 `*_not_configured`. `READ_API_AUTH_MODE=relaycontrol_only`
accepts relaycontrol `sh_at_` tokens only. In that mode the RelayAuth verifier
is never built, no JWKS is fetched, and a `RELAYAUTH_JWKS_URL` or
`RELAYAUTH_ISSUER` setting refuses startup. Env table below; details in
"Self-host canonical reads and `relaycontrol_only`" in
`docs/session-read-api-design.md`.

The services image also carries `SERVICE=session-publication` (the canonical publisher,
`src/session-ingest/publication-service.ts --continuous`, default-off on
`INGEST_PUBLISH_ENABLED`, probes on `WORKER_HEALTH_PORT`). The local stack does
not run it either. The per-SERVICE ports, probes, single-origin routing and
role order are in `docs/selfhost-composition.md`.

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
mints short-lived tokens in memory (the local stack sets the dev-only
`ALLOW_INTERIM_RELAYAUTH_BEARER=true`). Hosted and customer installs point
`RELAYAUTH_JWKS_URL` at RelayAuth instead, set `AUTH_ISSUER` to RelayAuth's
issuer, and set `RELAYHISTORY_ORIGIN`, with the interim bearer off: the ingest
then accepts only RelayAuth's History assertions minted for that exact origin
(RS256, sole audience, the dedicated token class). A durable host courier sets
`RELAYAUTH_URL`, the exact `RELAYAUTH_HISTORY_ORIGIN`, and
`RELAYAUTH_API_KEY_COMMAND_JSON`, a JSON argv array that reads the dedicated
assertion key from a central broker or secret manager. The command is never run
through a shell, and neither that key nor minted assertions are saved in the
courier ledger. `96bulls token local` remains available for manual calls.

### Relaycontrol user access tokens (dual-accept, default off)

Relaycontrol (owned by layered-control) issues opaque user access tokens with
the prefix `sh_at_`. The ingest and the read API can also accept them, verified
by relaycontrol's introspection endpoint (contract v1.2, pinned to
relay-control PR #20: `POST <RELAYCONTROL_INTROSPECT_URL>/internal/v1/auth/introspect`
with body `{token, aud, workspace?, machine?}`). Off by default; RelayAuth JWTs
keep their path unchanged either way.

| Env | Where | Meaning |
| --- | --- | --- |
| `RELAYHISTORY_ACCEPT_RELAYCONTROL_TOKENS` | ingest, read API (Lambda, Worker and the self-host SERVICE) | Exactly `true` enables; anything else is off. Off, a `sh_at_` bearer is refused exactly as before. |
| `RELAYCONTROL_INTROSPECT_URL` | same | Relaycontrol's base URL, configuration only. Same transport rule as every destination: https, loopback, or `http` to an in-cluster Service name with `ALLOW_IN_CLUSTER_HTTP=true` (the Worker: https or loopback only). |
| `RELAYCONTROL_INTROSPECT_SECRET` | same | The dedicated service secret, sent as `Authorization: Bearer`. From the secret store only, never a file. |
| `READ_API_INTROSPECT_POSITIVE_TTL_MS` | read API (Lambda and the self-host SERVICE; the Worker ignores it) | How long (ms) an admitted `sh_at_` token is reused before it is introspected again: the read path's revocation lag. Unset: `60000` (unchanged hosted behaviour). An integer `0`-`60000`; `0` introspects every request (strict revocation, recommended for self-host). Anything else, `""` included, fails startup. See "Cache and revocation lag" below. |
| `RELAYHISTORY_SELFHOST_READ_API` | read API | Exactly `true` adds `POST /v1/cli/login` (pass-through `sh_at_` exchange through the introspection above, no refresh) and `GET /v1/sessions/:id/catalog`; unset/`""`/`false` off; anything else fails startup. See docs/session-read-api-design.md §2a. |
| `SEARCH_INDEX_BACKEND` | session-read-api (enabled) | `postgres` (default; no ClickHouse, any known `PIPELINE_RUNTIME`) or `clickhouse` (hosted binding; needs the secret's `clickhouse` block). Anything else, including a name with no implementation in this build (`opensearch`), fails startup; the error names the variable, never the value. |
| `SEARCH_INDEX_POSTGRES_BACKEND_ID` | session-read-api (enabled, `postgres`) | Required, `[a-zA-Z0-9._-]{1,128}`: the projection-state backend id the search writer records under. Coverage is `READ_API_COMPLETE_SINCE`. Not a credential. |
| `SESSION_POSTGRES_TLS_PROFILE` | session-read-api (enabled) | Required explicitly (the Lambda's implicit `supabase` default does not apply): `supabase`, or `custom-ca` with `SESSION_POSTGRES_CA_FILE` and `SESSION_POSTGRES_CA_SHA256` (docs/portable-lambda-runtime.md). The pool gets discrete host/port/database/user/password from the secret's URL; URL `sslmode`/`options` never reach pg, and a URL without user, password or database fails startup. |
| `READ_API_AUTH_MODE` | session-read-api (and the Lambda, which nothing hosted sets it on; the Worker ignores it) | Unset or `""`: the default (RelayAuth, plus relaycontrol when enabled above). Exactly `relaycontrol_only`: `sh_at_` tokens only, and the RelayAuth verifier is never built, so no JWKS fetch happens and any other bearer is 401 `invalid_token` with no outbound call. In this mode startup also requires `RELAYHISTORY_ACCEPT_RELAYCONTROL_TOKENS=true`, `READ_API_ORIGIN`, `RELAYCONTROL_INTROSPECT_URL` and `RELAYCONTROL_INTROSPECT_SECRET`, and refuses a non-blank `RELAYAUTH_JWKS_URL` or `RELAYAUTH_ISSUER`. Any other value fails startup. |
| `LOCKER_BUCKET`, `LOCKER_PROVIDER`, `LOCKER_ENDPOINT`, `LOCKER_REGION`, `LOCKER_ACCESS_KEY_ID`, `LOCKER_SECRET_ACCESS_KEY`, `LOCKER_GCS_TOKEN_URI` | session-read-api (enabled) | The canonical store behind `/timing`, `/turns`, `/metadata` and `/catalog`. The names mean the same as for the other services, and the store is built by the same `buildLockerStore`: static keys from the secret store (MinIO), or, with no keys, EKS Pod Identity (`AWS_CONTAINER_CREDENTIALS_FULL_URI` and `AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE`, refreshed before expiry). For `gcs`, `LOCKER_ENDPOINT` (the official API or a loopback emulator) and `LOCKER_GCS_TOKEN_URI` (the GKE metadata server by default, or loopback) are both honoured. `LOCKER_BUCKET` unset: those routes answer 501 `*_not_configured`. Set without a usable store: startup fails. |
| `PIPELINE_ORG_FROM_PREFIX`, `PIPELINE_KEY_PREFIX` | session-read-api (with `LOCKER_BUCKET`) | `PIPELINE_ORG_FROM_PREFIX` must be exactly `true`, or startup fails. The org-prefix key layout is the only thing that binds an object to its org, so a static layout is refused, as in the session pipeline Lambda. `PIPELINE_KEY_PREFIX` must match the ingest's key layout. |
| `READ_API_DIRECTORY_WORKSPACES` | read API | The canary allowlist for directory, timing, turns, catalog and login: comma-separated workspace ids or `*`. Unset means none (501 `*_workspace_not_enabled`). |
| `READ_API_TIMING_ASSUME_NO_TURNS` | read API (Worker, self-host SERVICE) | Exactly `true`: time a session that has no legacy turn evidence as having no turns. Otherwise such a session is 501 `timing_turns_unknown`. |
| `READ_API_TURNS_ASSUME_NO_LEGACY` | read API (self-host SERVICE only) | Exactly `true`: this deployment never had a Neon source, so `/turns` and `/metadata` serve a session's canonical turns without the legacy coverage marker. Otherwise such a session is 501 `turns_unknown`. Never set it on a deployment with legacy history. |
| `READ_API_CANONICAL_PIN_LAGGING_POINTER` | read API (self-host SERVICE only) | Exactly `true` (default off; the self-host compose sets it): when the session pointer lags an append-only manifest (async admission answers before the consumer re-indexes), `/timing`, `/turns`, `/metadata`, `/catalog` and message-id reads serve the indexed generation instead of 501 `*_manifest_mismatch`, but only when the pointer digest is an exact prefix of the manifest chain and its revision is older. Newer chunks appear after the next reconcile. Any other value is off. Never read by the hosted Lambda. |

- **Dual accept.** A bearer starting `sh_at_` goes to relaycontrol and never to
  the RelayAuth verifiers (it is never JWT-parsed); every other bearer goes to
  RelayAuth and never to relaycontrol.
- **Binding.** `aud` is `RELAYHISTORY_ORIGIN` at the ingest and
  `READ_API_ORIGIN` at the read API, and relaycontrol must echo the same
  origin, compared normalized on both sides as relaycontrol matches it
  (scheme and host lowercased, a trailing slash or path dropped, an explicit
  non-default port kept): `https://History.example/` matches
  `https://history.example`, a different scheme, host or port does not. A workspace the request names (the ingest's
  `x-relayhistory-workspace-id` header, the read API's single `workspace=`
  query) is sent along and relaycontrol must return exactly that workspace;
  with none, `workspace` is omitted. Either way the org and workspace come only
  from relaycontrol's answer. A malformed workspace is refused without a call.
- **Principal.** Only `principal_type: "desktop-user"` is admitted; any other
  principal (relaycontrol's `agent` tokens, a new type) or none is 401, reason
  `wrong-principal` in logs only.
- **Scopes.** Only `rth:read` and `rth:sync` are granted; `cli:auth` and
  anything else grant nothing.
- **Fail closed.** Inactive (any reason, including the machine reasons
  below), wrong principal, aud, workspace or machine mismatch, or expired:
  401 `unauthorized`, with the reason in logs only. Relaycontrol answering
  400, 401 or 404 is a 503, never a token refusal: 400 means our request was
  refused, which a caller cannot cause because History validates token,
  workspace and machine first, so it is contract drift or a History bug
  (logged `introspect_bad_request`); 401 means our introspection secret is
  wrong, a deployment fault (logged `introspect_unauthorized`); 404 means
  introspection is disabled on relaycontrol. Likewise a timeout (5 s, of the
  call or the body read), network error, 5xx, a redirect (never followed: the fetch uses
  `redirect: "manual"`, the mode workerd supports), a response body over
  16 KiB (`response_too_large`, refused unparsed), or malformed JSON: 503
  `relaycontrol_unavailable` with `Retry-After: 1`. The flag on with the URL, secret or origin missing: 401
  `auth_not_configured` (read API: `not_configured`). With the flag on and both
  values set, the ingest starts without any RelayAuth key source.
- **Cache and revocation lag.** Answers are cached in memory per
  sha256(token), aud, workspace sent and machine claimed: an admit for min(token exp, 60 s), a refusal
  for at most 5 s, at most 1000 entries, single-flight per key. A revoked token
  can therefore keep working for up to 60 s per process. Unavailability is never
  cached, an expired entry is never reused, and a result is never reused for a
  different claimed machine. Per path:
  - **Read API** (Lambda and the self-host SERVICE, reads and
    `POST /v1/cli/login`): the admit window is
    `READ_API_INTROSPECT_POSITIVE_TTL_MS`, always passed to the introspector
    explicitly. Unset is `60000`, the hosted behaviour. Otherwise it must be a
    plain decimal integer from `0` to `60000` (no sign, spaces, exponent or
    leading zero). Any other value, `""` included, fails startup, with the flag
    on or off. `0` introspects every request, so a revoked token or a removed
    scope is refused on the next read. **Self-host operators who want strict
    revocation set `READ_API_INTROSPECT_POSITIVE_TTL_MS=0`.** The refusal
    window stays the client's own (at most 5 s); this setting never lengthens
    it. The Worker does not read the variable and keeps 60 s.
  - **Self-host ingest edge** (`src/session-ingest/selfhost-auth.ts`): no
    positive cache at all (every request introspects); not configurable.
  - **Hosted ingest middleware**: the client default of 60 s.
- **Machine verification (AlleyOop courier surface only).** At the ingest the
  request's `X-Machine-Id` (which the courier sends on chunk and manifest
  writes) is forwarded verbatim as `machine`, a claim for relaycontrol to
  verify against the token's user and workspace. It must match
  `^[A-Za-z0-9._:-]{1,200}$` or it is refused (401) without a call. The machine
  claim the unchanged live-write machine gate reads is set only from
  relaycontrol's `machine_id`, and only when it equals the claimed id. No
  `X-Machine-Id`, or `machine_id: null`, means no machine claim: every write
  is refused (403 `machine_claim_refused`) and reads pass. An unverified claim
  (`machine_not_owned`, `machine_unclaimed`, `machine_workspace_mismatch`) is
  401. A verified machine still has to match the chunk's `machine` query, the
  event's `machineId` and the manifest's `session.machine_id`, and writes need
  `rth:sync`. Desktop uploads are authenticated by the edge that signs
  `x-relay-ingest-identity` and are out of scope here.
- **Logs and egress.** Logs carry relaycontrol's `token_id`, never the token or
  the secret, and go to the console only. The only new call is to the
  configured introspection URL (in-cluster for a self-host); nothing new leaves
  the cluster.

## End-to-end acceptance test

One command, from a clean checkout, with only Docker and Bun installed:

```sh
bun run e2e:selfhost
```

It builds the services image, starts the stack in `e2e/selfhost/compose.yaml`,
runs the checks, and removes everything (containers and volumes) when done.
`E2E_KEEP=1` leaves the stack up; `E2E_SKIP_BUILD=1` reuses a built image. CI runs
the same command on every pull request (`ci.yml`, job `selfhost-e2e`, part of
the required `checks` result).

The stack is what `docs/selfhost-composition.md` describes: Postgres, MinIO, the
migrations, the operator role step (`e2e/selfhost/roles.sh`), `session-ingest`,
the `session-pipeline-consumer` (the only processing worker on the canonical
self-host path; `session-publication` is only for buffered acceptance and stays
off), and `session-read-api`. Credentials and the Postgres CA are generated per
run and never written outside the run's own `.96bulls/e2e-*` directory.

The only stub is `control-stub`, at the real boundary: Control's token
introspection endpoint. The ingest and read API run their real introspection
client against it with `READ_API_AUTH_MODE=relaycontrol_only`.

Checks, each reported PASS, FAIL or BLOCKED: authenticated upload (no token 401,
read-only token 403, cross-tenant refused), processing by the consumer, reads
through the read API (list, turns, search, activity), tenant isolation (org B's
token gets nothing from org A), durability across a restart of Postgres, MinIO
and all services, and erase through the session selection route (BLOCKED, not
FAIL, on a build where that route still answers 501).

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

## Search backend

The local stack keeps the default search backend, Postgres (`SEARCH_INDEX_BACKEND`
unset or `postgres`). The `opensearch` backend (OpenSearch or Elasticsearch) is
configured with `SEARCH_INDEX_BACKEND=opensearch`, `SEARCH_INDEX_OPENSEARCH_URL`
(required), `SEARCH_INDEX_OPENSEARCH_DIALECT` (`opensearch` or `elasticsearch`),
`SEARCH_INDEX_OPENSEARCH_INDEX_PREFIX`, `SEARCH_INDEX_OPENSEARCH_AUTH` (`none`,
`basic`, or `sigv4` with EKS Pod Identity and `SEARCH_INDEX_OPENSEARCH_AWS_REGION`), `SEARCH_INDEX_OPENSEARCH_USERNAME`, the
`SEARCH_INDEX_OPENSEARCH_PASSWORD` secret, `SEARCH_INDEX_OPENSEARCH_CA_FILE` and
`SEARCH_INDEX_OPENSEARCH_REQUEST_TIMEOUT_MS` (see
`docs/search-and-runtime-adapters.md`). `basic` is accepted only when
`SEARCH_INDEX_OPENSEARCH_URL` is https or a loopback host; a plain-http service
hostname (for example a compose service name) fails at startup with
`basic_auth_needs_https`. No local container runs it yet, and the
worker entry points still allow only `postgres`; the backend's proof runs against
disposable containers (`docs/session-search-proof.md`).

## Known limits

- Alchemy's Docker provider has no health gating; the CLI waits.
- No epoch handling in the courier yet (a transcript that shrinks is skipped with a warning).
- `hoopdb-indexer` is one replica (reconcile has no leader election yet).
- Chunk objects carry the upstream scrub metadata but not `session-uuid`; adding it to the chunk hop is an upstream change we will propose.
- A forced removal of running containers under a busy Docker daemon was seen to hang for 20+ minutes on one machine; `down` now stops containers first and waits.
