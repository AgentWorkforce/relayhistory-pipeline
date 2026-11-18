# The local stack

`bin/96bulls up local` stands the whole sessions pipeline up as Docker
containers on one machine, from `alchemy.run.ts` (Alchemy v2, Docker
provider). It is the proving ground for the hosted and self-hosted stacks: same
services, same env contract, same migrations.

To run the self-host services for yourself on one machine (fs Locker, single-user control, TLS edge), see [selfhost-local.md](selfhost-local.md).

## What runs

| Container | Image | Role |
| --- | --- | --- |
| `96b-hoopdb` | postgres:17 | HoopDb (port 15432 on the host) |
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
| `RELAYHISTORY_SELFHOST_INTROSPECT_POSITIVE_TTL_MS` | self-host session-ingest SERVICE only (the Worker and hosted middleware ignore it) | How long (ms) an admitted `sh_at_` upload token is reused before it is introspected again: the ingest edge's revocation lag. Unset, `""` or `0`: off, every upload request is introspected (unchanged default). Otherwise a plain decimal integer `1`-`60000`; `30000`-`60000` recommended when Control load matters. Anything else (negative, fractional, over `60000`, garbage) fails startup and is never clamped. See "Cache and revocation lag" below. |
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
| `READ_API_SESSION_QUERY` | read API (Lambda and self-host SERVICE; the Worker ignores it) | Exactly `true` (default off): `GET /v1/sessions` and `/v1/sessions/activity` accept the Dashboard session query `q` (with optional `search=metadata`), evaluated on the session directory (needs migration 046010; before it, 501 `directory_not_migrated`). Free words match the directory search text only (title, task title, agent name, project id, git repo/branch, model, scrubbed short summary; never transcript content); 1- or 2-character free words (`Fix a bug`) need an included free word of 3 or more characters in the same query, else 400. `path:`, `upload:` and `is:on-relay`/`is:off-relay` are explicit 501s; see `docs/contracts/session-read-api-v1.openapi.yaml`. Unset, `""` or `false` is off, and off is unchanged (501 `param_q`); any other value fails startup. |
| `READ_API_MINT_READER_KIND` | read API (Lambda and self-host SERVICE; the Worker ignores it) | `1` or `true` (default off): the self-issued mint (`POST /v1/tokens/self-issued`, `src/session-read-api/self-issued.ts`) accepts the optional `reader_kind` field (`browser`, `device` or `mcp`) and stores it on `sessions.read_api_tokens.reader_kind` through `sessions.read_api_token_issue_v2`; needs migration 20261105062050 applied first. Unset, `""` or `0`/`false` is off, and off is unchanged (the 044000 `sessions.read_api_token_issue` call, `reader_kind` not accepted).
| `READ_API_CANONICAL_SCOPE_BYTES` | read API (self-host SERVICE only) | Raw-byte budget of one canonical snapshot (`/timing`, `/turns`, `/metadata`, `/catalog`, message-id reads): the sum of each chunk's raw size. Bytes as a positive integer, or with a `MiB` / `GiB` suffix. Default 32 MiB (sized for the chart's read API pod memory limits; raise it only together with the pod limit); accepted 16 MiB to 2 GiB, so an operator can lower it to the hosted Worker's 16 MiB. A larger session is 501 `*_canonical_canonical_projection_incomplete` (reason `scope_too_large`). The service holds up to this many raw bytes per in-flight read, so size its memory to match. Unset or empty is the default; malformed, below 16 MiB or above 2 GiB fails startup. Never read by the hosted Lambda, whose budget stays 16 MiB. |
| `READ_API_CANONICAL_SCOPE_EVENTS` | read API (self-host SERVICE only) | Record budget of the same snapshot, a positive integer. Default 100000; accepted 100000 to 4000000 (the 500000 projected-row bound is unchanged). Unset or empty is the default; malformed or out of range fails startup. Never read by the hosted Lambda. |
| `READ_API_CANONICAL_DEADLINE_MS` | read API (self-host SERVICE only) | Per-request deadline in milliseconds of every canonical read (`/timing`, `/turns`, `/metadata`, `/catalog`, events and message-id reads), a positive integer. Default 3000; accepted 3000 to 60000. A read past it is 501 `*_canonical_deadline`. Chunks of a session are read one at a time unless `READ_API_CANONICAL_FETCH_CONCURRENCY` is raised, so the first read of a ~70 MiB session takes on the order of 10-25 s sequentially; raise this together with `READ_API_CANONICAL_SCOPE_BYTES`, and keep it below the ingress/proxy read timeout (stock ingress-nginx: 60 s). Unset or empty is the default; malformed or out of range fails startup. Never read by the hosted Worker/Lambda, which keep 3000. |
| `READ_API_CANONICAL_FETCH_CONCURRENCY` | read API (self-host SERVICE only) | How many chunk objects of one session a canonical read (`/timing`, `/turns`, `/metadata`, `/catalog`, events and message-id reads, and the session-brief transcript source) fetches from the object store at once, a whole number. Default 1 (sequential, the previous behaviour); accepted 1 to 16. Chunks are still processed and checked strictly in manifest order; at most this many chunk bodies (each capped at 16 MiB stored) are held at once, so size the pod memory limit accordingly. Recommended 8 on GCS or any object store with more than about 20 ms per-object latency, where a session over about 100 chunks otherwise hits the 3000 ms deadline. Unset or empty is the default; malformed or out of range fails startup. The session-brief worker's transcript source reads the same variable through `serviceCanonicalDeps` (`src/session-brief/canonical-input.ts`), where a bad value fails the first brief rather than startup, like the other `READ_API_CANONICAL_*` settings. Never read by the hosted Worker/Lambda, which stay sequential. |
| `READ_API_SEARCH_PARTIAL_SESSIONS` | read API (self-host SERVICE only) | What a workspace search (`GET /v1/events?q=` with no `session`) does while a session of the workspace is partial in the search index. Unset, `""` or `refuse` (default, unchanged): the first page is 501 `session_partial` / `session_partial_unbounded`. `skip`: every page (cursor pages too) is checked again, answers 200 from the index without the listed sessions' rows, and adds `incomplete` (`{"reason":"session_partial","sessions":[...]}`, or `{"reason":"session_partial_unbounded"}` with no list when more are partial than one check lists) and the `X-Sessions-Read-Incomplete: <reason>` header. `session_partial_unbounded` lists nothing and excludes nothing, so its pages may still hold stale rows of a partial session. A search backend that cannot exclude sessions refuses `skip` at startup. On the Postgres or OpenSearch search path (both keep their index state in Postgres) a session is partial while that state is not complete and current: its first index pass has not run yet, it is too large for the limits (`scope_too_large`), or a new upload moved its directory epoch and the re-index has not run yet. So with `skip` a session that is being written to drops out of search results until its next index pass. A session-scoped read of a partial session is still 501 `session_partial`. Any other value fails startup; the hosted Lambda refuses `skip`. |
| `SEARCH_INDEX_MAX_SCOPE_BYTES` | session-pipeline consumer (self-host SERVICE only, enabled) | Raw bytes of one search index scope; above it the session is marked partial (`scope_too_large`). Bytes as a positive integer, or with a `MiB` / `GiB` suffix. Default 64 MiB; accepted 64 MiB to 256 MiB (the replay bound). The indexer holds the scope several times over in memory, so raise the pod limit with it. Needs no manual re-index: every reconcile tick (`CONSUMER_RECONCILE_EVERY_MINUTES`) the consumer re-sends each session the search state lists as partial, `scope_too_large` ones included, so after a restart with a higher limit those sessions are indexed again under it (a large backlog over several ticks; a key in retry backoff waits for it). Unset or empty is the default; malformed, below 64 MiB or above 256 MiB fails startup before any connection opens, naming the variable only. Never read by the hosted Lambda. |
| `SEARCH_INDEX_MAX_SCOPE_EVENTS` | session-pipeline consumer (self-host SERVICE only, enabled) | Records of one search index scope, a positive integer. Default 100000; accepted 100000 to 1000000. Same partial, re-index and startup rules as `SEARCH_INDEX_MAX_SCOPE_BYTES`. Never read by the hosted Lambda. |
| `SEARCH_INDEX_MAX_ROWS` | session-pipeline consumer (self-host SERVICE only, enabled) | Index rows compared for one scope, a positive integer. Default 500000; accepted 500000 to 1000000. Same startup rules. Never read by the hosted Lambda. |
| `SEARCH_INDEX_STREAMING_ENABLED` | session-pipeline consumer (self-host SERVICE only, enabled) | Default on: streams a scope that is too large for one index pass (`scope_too_large` alone) instead of indexing it partial; see "Streaming reconcile (Phase 2)" below. Unset or `""` is the default, which is on, except off (logged at startup with `reason=output_facts`, no failure) when `RELAYHISTORY_SELFHOST_OUTPUT_FACTS=true` and the staged reconcile is off. `false` is off, and neither budget is read; set it explicitly to turn streaming off. `true` is an explicit on and, with `RELAYHISTORY_SELFHOST_OUTPUT_FACTS=true` and the staged reconcile off, fails startup. With `SEARCH_INDEX_STAGED_RECONCILE_ENABLED=true`, output facts do not change any of this: the staged run stages them. Any other value (`1` and `0` included) fails startup. Never read by the hosted Lambda or the large-read worker. |
| `SEARCH_INDEX_STREAM_MAX_READ_BYTES` | session-pipeline consumer (streaming on only) | Bytes read for one streamed scope, summed over every pass. Bytes as a positive integer, or with a `MiB` / `GiB` suffix. Default 2 GiB (scopes up to 512 MiB stream); accepted from four times the effective `SEARCH_INDEX_MAX_SCOPE_BYTES` (capped at 2 GiB) to 2 GiB. A scope streams only when four times its size fits both budgets; with both budgets at that floor no scope streams (startup warns). Past it the scope stays partial with `stream_read_bytes_budget`. Out of range or malformed fails startup, naming the variable only. |
| `SEARCH_INDEX_STREAM_MAX_READ_EVENTS` | session-pipeline consumer (streaming on only) | Records read for one streamed scope, summed over every pass, a positive integer. Default 2000000 (scopes up to 500000 records stream; size the pod for about 2 GB on top of its usual limit, about 3 KB per record, measured); accepted from four times the effective `SEARCH_INDEX_MAX_SCOPE_EVENTS` (capped at 2000000) to 2000000. Past it the scope stays partial with `stream_read_events_budget`. Same startup rules. |
| `SEARCH_INDEX_STAGED_RECONCILE_ENABLED` | session-pipeline consumer (self-host SERVICE only, enabled) | Default off (unset, `""` or `false`). `true` turns on the staged reconcile, Phase 3 slice 1: paged index scans (past the cap the reason is `stream_keyed_records_cap`, not `search_index_truncated`; at the defaults no more rows are read than before), streaming budgets per pass instead of summed, and an interim cap of 500000 records read per pass or index rows per scan (never below `SEARCH_INDEX_MAX_ROWS`), so sessions above 500000 records stay incomplete until a later slice; see "Staged reconcile (Phase 3), slice 1" below. A scope that would stream runs as a staged run instead (migration 20261105062110 required; startup refuses without it; see "Staged reconcile (Phase 3), staged runs"). `true` fails startup unless streaming resolves on; with `RELAYHISTORY_SELFHOST_OUTPUT_FACTS=true` it starts and stages the facts of a too-large scope ("Staged output facts" below; migration `20261105062140` required); any other value fails startup. Never read by the hosted Lambda or the large-read worker. |

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
    positive cache by default (every upload request introspects, one
    relaycontrol call and so one Control SQL query per request).
    `RELAYHISTORY_SELFHOST_INTROSPECT_POSITIVE_TTL_MS` (default off; an integer `0`-`60000`,
    anything else fails startup, never clamped; `30000`-`60000` recommended
    under Desktop upload bursts) turns the positive cache on: an admit is reused
    for min(TTL, the token's remaining `exp`) per process, keyed by
    sha256(token) and the edge's one `aud` (it sends no workspace or machine).
    The introspection answer carries no revocation epoch, so a revoked token or
    a removed `rth:sync` keeps uploading for at most the TTL. Refusals stay
    cached at most 5 s, outages are never cached, and the cache is bounded at
    1000 entries (LRU, the client default; not configurable here).
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

`bun run e2e:pipeline` runs this stack extended with the S3-ledger ingest and the
courier pipeline (AlleyOop, indexer, referee, Bob Costas), and asserts every piece
of the pipeline on its own line: [pipeline-e2e.md](pipeline-e2e.md).

## Local Postgres TLS in two commands

The services refuse plaintext Postgres and trust only an explicitly pinned CA
(`src/shared/postgres-tls.ts`). For a single-user local stack, mint a throwaway
CA and server certificate (needs `openssl`; the CA key is deleted on exit, so
rerun into a new directory to rotate):

```sh
bun scripts/selfhost-local-ca.ts --out .96bulls/pg-tls   # default --host db --host localhost --host 127.0.0.1 --days 825
```

It writes `ca.crt`, `server.crt` and `server.key` (0600), refuses a non-empty
`--out`, and prints one line, `SESSION_POSTGRES_CA_SHA256=<hex>`. Give every
service `SESSION_POSTGRES_TLS_PROFILE=custom-ca`,
`SESSION_POSTGRES_CA_FILE=<path to ca.crt as the service sees it>` and that
printed `SESSION_POSTGRES_CA_SHA256`. Then start Postgres with TLS on, copying
the key in as the `postgres` user first, because Postgres refuses a key it does
not own (as in `e2e/selfhost/compose.yaml`):

```sh
docker run -d --name rh-db -p 127.0.0.1:5432:5432 -e POSTGRES_PASSWORD=... \
  -v "$PWD/.96bulls/pg-tls:/certs:ro" --entrypoint sh postgres:17 -c '
  install -o postgres -g postgres -m 0600 /certs/server.key /var/lib/postgresql/server.key &&
  install -o postgres -g postgres -m 0644 /certs/server.crt /var/lib/postgresql/server.crt &&
  exec docker-entrypoint.sh postgres -c ssl=on \
    -c ssl_cert_file=/var/lib/postgresql/server.crt -c ssl_key_file=/var/lib/postgresql/server.key'
psql "host=localhost user=postgres sslmode=verify-full sslrootcert=.96bulls/pg-tls/ca.crt"
```

Use `sslmode=verify-full` so `psql` checks the chain and host name, too. The
services strip URL `sslmode` and always verify. `.96bulls/` is gitignored.

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
`SEARCH_INDEX_OPENSEARCH_PASSWORD` secret, `SEARCH_INDEX_OPENSEARCH_CA_FILE`,
`SEARCH_INDEX_OPENSEARCH_REQUEST_TIMEOUT_MS` and
`SEARCH_INDEX_OPENSEARCH_BULK_MAX_BYTES` (`_bulk` body bound, default 4 MiB) (see
`docs/search-and-runtime-adapters.md`). `basic` is accepted only when
`SEARCH_INDEX_OPENSEARCH_URL` is https or a loopback host; a plain-http service
hostname (for example a compose service name) fails at startup with
`basic_auth_needs_https`. No local container runs it yet, and the
worker entry points still allow only `postgres`; the backend's proof runs against
disposable containers (`docs/session-search-proof.md`).

### Streaming reconcile (Phase 2)

`SEARCH_INDEX_STREAMING_ENABLED` (`session-pipeline-consumer`, self-host service only, enabled consumer only,
**default on**) indexes a session that is too large for one search index pass instead of marking it partial.
Unset or `""` is the code default: on, except off when `RELAYHISTORY_SELFHOST_OUTPUT_FACTS=true` (the facts are
built from a whole read), where startup does not fail and the consumer logs `pipeline consumer streaming reconcile
off` with `reason=output_facts` (info, once per start). `false` turns it off, with or without output facts, and the
consumer then reads neither budget below. `true` is an explicit on and still fails startup together with
`RELAYHISTORY_SELFHOST_OUTPUT_FACTS=true`. Any other value (`1`, `0`, `TRUE`, padded values included) fails startup.
To turn streaming off, set `SEARCH_INDEX_STREAMING_ENABLED=false` explicitly: leaving the variable out of the
environment no longer means off. It applies only to a scope whose whole read stopped on `scope_too_large` alone
(above `SEARCH_INDEX_MAX_SCOPE_BYTES` / `_EVENTS`); every other scope takes the whole read as before. The hosted
pipeline Lambda and the large-read worker never read these settings, so the default changes nothing there.

Helm chart (AgentWorkforce/infra PR 414, chart `sessions-pipeline` 0.8.42): the values are
`sessions-pipeline.searchIndexer.streaming.enabled`, `sessions-pipeline.searchIndexer.streaming.maxReadBytes` and
`sessions-pipeline.searchIndexer.streaming.maxReadEvents`. `enabled` is tri-state: `null` (the chart default) leaves
`SEARCH_INDEX_STREAMING_ENABLED` unset so the image's code default applies, `true` renders `"true"`, and `false`
renders `SEARCH_INDEX_STREAMING_ENABLED="false"` explicitly. The chart never drops the variable to mean off, because
an unset variable now means on. `enabled: true` together with `searchIndexer.outputFacts.enabled: true` fails the
chart render (the image would fail startup the same way); `enabled: null` with output facts on renders, and the
image turns streaming off itself (the `reason=output_facts` info line above). `maxReadBytes` / `maxReadEvents`
render `SEARCH_INDEX_STREAM_MAX_READ_BYTES` / `_EVENTS` when set and are subject to the startup checks below.

The streaming reader reads the scope's chunks again in several passes rather than holding them: roughly four object
reads per chunk for flagged sessions, memory bounded by one chunk plus per-record keys. It produces the same index
rows, deletes, directory events, catalog and search state as the whole read would with unlimited limits, and
regenerates rows only for the identities the index lacks or holds stale, so an up-to-date session costs three
passes. `SEARCH_INDEX_MAX_ROWS` still bounds the index scan (`search_index_truncated`). The stream runs inside the
scope's index transaction under its advisory lock, so that lock is held for the whole read; between reads, and
between delete batches, the stream sends `SELECT 1` on that transaction at most every 20 seconds, and always just
before and after the index scan, so a long pass does not trip the consumer pool's 60-second idle-in-transaction
timeout.

Two budgets bound it, each summed over every pass of one scope: `SEARCH_INDEX_STREAM_MAX_READ_BYTES` (a whole
number of bytes, optional `MiB` / `GiB` suffix) and `SEARCH_INDEX_STREAM_MAX_READ_EVENTS` (records). Unset, each
is the large-read profile's bound, 2 GiB / 2000000. A scope streams only when its size times four fits both
budgets, so at the defaults scopes up to 512 MiB and 500000 records stream. Set, a budget must lie between its
floor, four times the effective whole-read bound capped at 2 GiB / 2000000, and that cap, or startup fails naming
the variable and its range, never the value; lower a budget to shrink the largest streamed scope and the memory it
needs. A scope is refused before any chunk is read when its declared size times four exceeds a budget, again
after the first pass from the sizes actually read and declared, and on any chunk or record past it. A scope whose
chunks do not declare their record counts is refused only after a full first pass, read under the scope's lock.
A scope streams only when it is strictly larger than the whole-read bound, so the smallest one needs four times
(bound + 1) of a budget: with neither budget at or above four times (bound + 1), the floors included, every scope
past the whole-read bound is refused and the consumer logs a warning at startup (a floor itself is still accepted,
so values set before stay valid); from `SEARCH_INDEX_MAX_SCOPE_EVENTS=500000` up the events floor is already the
2000000 cap, so a scope past the events bound never streams there, while one past only the bytes bound still can.

Minimum consumer memory with streaming on: the non-streaming sizing plus about 2 GB at the default budgets. The
streaming reader never holds the scope (up to 512 MiB at the default budgets) at once; it holds one decompressed
chunk (at most 16 MiB) plus about 3 KB per record of the scope for its keys and per-record state (measured on
synthetic scopes with `scripts/bench/staged-stream-rss.ts`: peak RSS +0.6 GB at 100000 records, +3.3 GB at 1000000),
about 2 GB for the largest scope the default budgets stream (500000 records). That part scales with
`SEARCH_INDEX_STREAM_MAX_READ_EVENTS`: lowering it lowers the largest streamed scope and this memory in proportion.
`SEARCH_INDEX_STAGED_RECONCILE_ENABLED=true` needs the same sizing (its keyed-records cap is the same 500000). An image upgrade
turns this on for an install that never set the variable, so check the pod limit or set `false` before upgrading
(see "Upgrading to the default-on image" below). On the first reconcile tick after the upgrade, every session
already partial with `scope_too_large` alone is streamed (or refused by the budgets) in turn. A refused scope stays
partial with `scope_too_large` first and then the budget's code, `stream_read_bytes_budget` or
`stream_read_events_budget`, and is retried every reconcile tick like any partial session; the consumer's
`search index partial` warning names those codes and, for a streamed scope, the passes, chunks, bytes and records
it read. Such a scope, still truncated after a streamed pass because it is beyond `SEARCH_INDEX_STREAM_MAX_READ_*`,
is read again, up to its budget, on every reconcile tick. That is the accepted Phase 2 scope: no wrong data (rows
are written only after a complete verifying pass, nothing is deleted, the session stays partial), only the read
work repeated each tick. To
avoid that work, raise the budget it exceeds, or opt out with `enabled: false` in the chart
(`SEARCH_INDEX_STREAMING_ENABLED="false"`).

Streaming stops at the first read or fold error and reports that one code (for example `chunk_decompress_failed`),
while the whole read may list more codes for the same scope. A chunk or record whose content changed between passes
stops it with `stream_source_changed`; `stream_duplicate_event` is a defensive check that two records never produce
one index identity, not reachable from well-formed manifests. Rows are written only in the last pass, after a
complete verifying pass, and each insert batch commits on its own. Nothing is deleted and the directory is not marked
current unless the streamed read was complete. A last pass that fails (a store error, or a source change seen while
regenerating) may leave rows already written; they are rows the verify pass checked, nothing is deleted, the
session stays partial and keeps owing a pass, and the next reconcile repairs it. The hosted pipeline Lambda never
reads these settings. With streaming and `MESSAGE_ORIGIN_LABELS_ENABLED=true` both on, self-host streamed output
carries origin labels exactly as the whole read does; this is intended.

#### Upgrading to the default-on image

Besides turning streaming on, the default-on image validates the streaming budgets at startup whenever streaming
resolves on, which now includes an unset `SEARCH_INDEX_STREAMING_ENABLED`. An install that set
`SEARCH_INDEX_STREAM_MAX_READ_BYTES` / `_EVENTS` earlier (for example while trying streaming out, then left them in
place with the flag unset), or raised `SEARCH_INDEX_MAX_SCOPE_BYTES` / `_EVENTS` so that a set budget is now below
its floor (four times the effective bound), gets those values checked on the next start. A stale, out-of-range or
malformed value that the old image ignored now fails startup, naming the variable and its range. Before upgrading,
remove or correct those values, or set `SEARCH_INDEX_STREAMING_ENABLED="false"` (chart `enabled: false`), under
which neither budget is read.

### Staged reconcile (Phase 3), slice 1

`SEARCH_INDEX_STAGED_RECONCILE_ENABLED` (`session-pipeline-consumer`, self-host service only, enabled consumer only)
is **default off**: unset, `""` and `false` are off, and off changes nothing (no log line, no reason code, the same
SQL and index calls as before). `true` turns it on and needs the streaming reconcile above to resolve on: with
`SEARCH_INDEX_STREAMING_ENABLED=false`, or with `RELAYHISTORY_SELFHOST_OUTPUT_FACTS=true` (on or off by default),
startup fails naming the variable. Any other value (`1`, `TRUE`, padded values included) fails startup. On, the
consumer logs `pipeline consumer staged reconcile on` once at startup, and refuses to start on a search index without
paged scans (the Postgres and OpenSearch adapters both have them). The hosted pipeline Lambda and the large-read
worker never read it.

With it on:

- **Paged index scans.** The index scan of every pass, whole read or streamed, walks the adapter's keyset pages
  (`scanFingerprintsPage`, one insert batch per page) instead of one read bounded by `SEARCH_INDEX_MAX_ROWS`, so a
  scope with more index rows than that is no longer `search_index_truncated`: up to the cap below it reconciles,
  deletes included.
- **Per-pass read budget.** `SEARCH_INDEX_STREAM_MAX_READ_BYTES` / `_EVENTS` bound each pass of a streamed scope,
  not the sum of its passes, so a scope is refused only when its own size exceeds a budget, never four times its
  size: at the defaults a scope up to 2 GiB streams (records are capped below).
- **Interim keyed-records cap.** Every record a pass reads is still keyed in memory (about 3 KB each, measured), so one pass
  may read at most 500000 records, and one index scan may hold at most 500000 rows (or `SEARCH_INDEX_MAX_ROWS` when
  that is higher). The cap counts records read, not unique keys: a record carried by two sibling manifests counts
  twice. Past it, nothing is deleted and the scope is retried every reconcile tick, with one of two outcomes:
  - a **streamed** scope (past the whole-read bound) stays partial with `scope_too_large` and
    `stream_keyed_records_cap`, like a spent budget;
  - a **whole-read** scope whose index scan passes the cap is marked incomplete (not partial) with
    `stream_keyed_records_cap` alone, in place of `search_index_truncated`. Its missing rows are still written.

  That is the same key memory as the default streaming budgets already need (about 2 GB at the cap), so the pod
  sizing above holds. The cap is a constant, not a setting; a later slice replaces the in-memory keys and removes it.
- **Between scan pages** the consumer checks the scope's deadline (if any) and keeps its transaction busy (`SELECT
  1` at most every 20 seconds) on the whole-read path. A streamed scope no longer runs in that transaction: it runs
  as a staged run (migration 20261105062110, required at startup), see "Staged reconcile (Phase 3), staged runs"
  below, with its own reason codes (`staged_run_leased`, `staged_run_interrupted`,
  `staged_lease_lost`, `output_facts_stage_incomplete`).

What it does not change yet: at the default settings (`SEARCH_INDEX_MAX_ROWS` = the cap = 500000) the paged scan
reads no more index rows than the bounded scan did; only the reason code past that point changes. A
`SEARCH_INDEX_MAX_ROWS` set between 500000 and 1000000 keeps its higher bound. The flag never lowers it (the cap is
a floor, so a `SEARCH_INDEX_MAX_ROWS` below 500000, as tests set it, is raised to the cap). A session above 500000
records read per pass stays incomplete until a later slice removes the cap; what slice 1 adds today is the per-pass
byte budget (scopes up to 2 GiB under 500000 records) and the paged scan itself.

Turning the default on is a separate, gated step, not part of this slice.

### Staged reconcile (Phase 3), staged runs

Migration `20261105062110_search_reconcile_staging.sql` and `src/session-pipeline/staged-reconcile.ts` let the
streaming reconcile above keep its diff state in Postgres instead of in memory, commit it in batches, and resume after
a crash. They sit behind `SEARCH_INDEX_STAGED_RECONCILE_ENABLED` (default off), and with it on the consumer uses them
(`search-index.ts` `stagedSearchScope`).

**Which scopes take the staged path.** With the flag on, exactly the scopes that stream today: a scope whose whole read
stopped on `scope_too_large` alone, on the self-host consumer (the Postgres or OpenSearch adapter binding). Those scopes
already take a flag-specific path since slice 1 (paged scans, per-pass budgets), so the staged run changes no other
scope. Every scope the whole read completes, and every scope partial for another reason, stays on the whole read
exactly as with the flag on in slice 1. With the flag off nothing here runs (no query, log line or reason code).

How a staged run differs from the in-transaction stream it replaces:

- **No long transaction.** The index transaction that found the scope too large commits without a write and hands the
  scope to the staged run. Passes 1-3 and pass 4 read the source outside any transaction; the wanted keys and the index
  scan are staged in committed batches. Every pass 4 write page and the final step take the scope advisory lock and
  re-read the source stamp before writing to the index, so they are exclusive with a whole read and the erase pass of
  the scope; the final step also takes the canonical lock. During a long source read the run renews its lease on a
  timer, so another consumer's tick does not take it over mid-read.
- **The session reads as incomplete for the whole run.** The claim records the scope owing a pass (partial, 0 rows)
  before any work, so readers treat it as not current until the final step records it complete.
- **Resume on the reconcile tick.** A successful claim records the scope as owing a pass (search state incomplete) in
  the claim's own transaction, so an interrupted run is listed by the next tick and resumes at its committed cursor
  once its lease expires (a crash) or at once (an error: the lease is released). Every outcome after a successful claim
  that is not done leaves the scope owing (directory events invalidated, state incomplete), so the next tick always
  takes it again; except `staged_lease_lost`, which writes nothing (the consumer that took the run over, or the
  erase, owns the state). A failed lease renewal during a read is logged (`search index staged lease renewal
  failed`, error class and code) and retried at the next renewal; only RH062 or 42501 stop renewing. No staged outcome answers `busy`: the queue's requeue, backoff and dead-letter path is never used
  for a staged scope.
- **Outcome reasons** (all incomplete, none `busy`): `staged_run_leased` (another consumer holds the run's live
  lease; nothing written), `scope_busy` / `canonical_scope_busy` (a lock was held at the final step; the lease is
  released), `stream_source_changed` (the source stamp moved; the run was abandoned and restarts on the tick),
  `staged_lease_lost` (the run was taken over or erased under this consumer), `staged_run_interrupted` (a step failed
  after the claim; logged as `search index staged run interrupted` with the error class and code, never its text;
  the lease is released; partial, resumed on the tick; a run that keeps failing hits the attempts cap below),
  `output_facts_stage_incomplete` (an output-facts hook sent the run round again), `staged_run_failed` (past the
  attempts cap, see "A stuck or failed staged run" below). A spent read budget or the keyed-records cap stays partial with
  `scope_too_large` and its code, as before.
- **A second consumer** that reaches the same scope while a run holds a live lease gets `staged_run_leased` at the
  claim, before any work. The whole-read path itself does not look at the run table (the runtime role has no grant on
  it and 062110 has no reader for it): for a scope with a live run it either refuses the scope as too large and reaches
  the claim (the same source), or completes because the source shrank, which moves the stamp, so the run abandons at
  its final check.
- **Erasure.** The erase pass (no live pointer left) drops the scope's staged runs and stage rows
  (`search_reconcile_erase_scope_v1`) before it erases the index, in the erase transaction under the scope lock; a
  write page that would follow finds its run gone (`staged_lease_lost`) and writes nothing.
- **Deletes on OpenSearch.** As on the in-transaction streamed path, index deletes on a non-Postgres index commit on
  their own: if the final transaction then rolls back, the deletes stay, and the next attempt deletes nothing more
  (idempotent).
- **The facts pass (output facts hooks only).** With output-facts hooks, the end of the want phase makes one more
  source pass that hands the whole read's facts input to the hook in chunks, under the same read budgets and deadline
  as passes 1-3. It is not resumable: every attempt that is in the want phase pays passes 1-3 plus the facts pass.
  With output facts off there is no extra pass and no extra query.
- **Startup.** With the flag on the consumer refuses to start unless every 062110 writer exists, naming the migration.

- **Tables.** `sessions.search_reconcile_runs_v1` holds one run per scope: the source stamp it started from
  (canonical id, directory epoch, admitted-manifest fingerprint), its phase (`want`, `have`, `write`, `final`), the
  cursor reached in that phase, and its lease. `sessions.search_reconcile_stage_v1` holds the staged keys: the wanted
  identities from passes 1-3 and the index scan. Both tables have RLS on and no table grants.
- **Functions.** The seven `sessions.search_reconcile_*_v1` writers are SECURITY DEFINER, and only
  `search_consumer_runtime` may execute them. Each call checks the run's lease nonce and renews the lease. Once a lease
  has expired, the next claim takes the run over and resumes at its committed cursor. The old holder then gets
  SQLSTATE `RH062`.
- **Final step.** The final step is one transaction. It takes the scope and canonical advisory locks that
  `indexSearchScope` takes and re-reads the source stamp; if the stamp changed, the run is abandoned and the next tick
  restarts it. Otherwise it deletes, writes the directory events, catalog and state, and drops the run.
- **Rollback.** `supabase/rollbacks/20261105062110_search_reconcile_staging.down.sql` drops only in-progress runs,
  which then start over. It refuses while any run holds a live lease.

**One summary line per pass.** Every staged pass logs `search index staged run` once, fields only (never message
text): `stage`, `org_id`, `workspace_id`, `harness`, `session_id`, `outcome` (`done`, `busy`, `restart`, `failed`,
`incomplete`), `reason` (the first reason code; absent on a clean `done`), `attempt` (the run's claim count; absent
when nothing was claimed), `count` (rows in the window), `inserted`, `deleted`, `duration_ms`. Level `warn` for
`failed` and `incomplete`, `info` otherwise. With the flag off it is never logged.

#### A stuck or failed staged run

**The attempts cap.** A run claimed more than 24 times since it last finished or was reset (source changes and
restarts included; the run row's `attempts`) is failed: each later claim abandons it at once with `last_error =
'staged_run_failed'`, before any source read and without writing an index row, records the scope owing, and logs the
summary line at `warn` with `outcome=failed`, `reason=staged_run_failed`. Every 72nd claim past the cap (about every 6
hours at the default 5-minute tick) still runs in full, so a scope that failed through an outage (an object store
down for two hours, say) heals on its own; a success removes the run as usual. A failed run whose source changes is
dropped and claimed afresh (attempt 1). That is the only automatic reset of the count, and only past the cap: below it
every claim counts whatever its cause, so the cap counts claims, not failures. A scope that needs more than 24 claims to
finish because the consumer keeps being restarted or killed under it is failed too, and a scope at claim 24 that is
interrupted once is failed until the next slow retry or an operator reset. The slow retry heals only a scope that
finishes within that one claim; a failed scope that needs more than one claim to finish needs the operator reset.

The slow retry has a cost: a scope whose read kills the pod (out of memory) does so again on every retry, about every
6 hours, instead of never. To stop it, lower `SEARCH_INDEX_STREAM_MAX_READ_BYTES` / `_EVENTS` below that scope's size,
or turn `SEARCH_INDEX_STAGED_RECONCILE_ENABLED` off. Under a lowered budget the scope is read up to the budget on each
claim that runs (or refused from its declared size before any read) and reports `scope_too_large` with the budget's
code; after 24 such claims it reports `staged_run_failed`, and the budget's code again on each slow-retry claim.

**Symptoms.** A session stays partial for many ticks; the summary line for it shows the same `reason` tick after
tick (`staged_run_interrupted`, then `staged_run_failed` past the cap); `search index staged run interrupted` lines
name the error class and code.

**Checks** (owner SQL, read only):

```sql
-- Failed runs. A failed run whose slow-retry claim just ended on another code (a budget, a source change) shows that
-- code in last_error until its next failed claim; in the second query, any run with attempts past 24 is failed too.
SELECT org_id, workspace_id, harness, session_id, attempts, phase, updated_at
  FROM sessions.search_reconcile_runs_v1 WHERE last_error = 'staged_run_failed' ORDER BY updated_at;
-- Every run, with its lease.
SELECT org_id, workspace_id, harness, session_id, phase, attempts, last_error,
       lease_expires_at > clock_timestamp() AS leased, updated_at
  FROM sessions.search_reconcile_runs_v1 ORDER BY updated_at;
```

**Reset one scope** (owner SQL; the runtime role cannot): the next tick claims a fresh run (attempt 1). Its stage rows,
and any staged output facts keyed to the run (migration 20261105062140), go with it. In psql:

```sql
\set org 'org-id'
\set ws 'workspace-id'
\set harness 'claude'
\set session 'session-id'
DELETE FROM sessions.search_reconcile_runs_v1
 WHERE org_id = :'org' AND workspace_id = :'ws' AND harness = :'harness' AND session_id = :'session';
```

Deleting a run under a live lease is safe: its holder's next step gets RH062, it stops with `staged_lease_lost` and
writes no state, and the next tick starts over. The same holds when a failed run's source change drops a sibling
scope's run of the same canonical session (the reset deletes by session or canonical id).

**Cleanup** of old idle runs (abandoned, failed, or left by sessions that never come back), never one under a live
lease:

```sql
DELETE FROM sessions.search_reconcile_runs_v1
 WHERE (lease_nonce IS NULL OR lease_expires_at < clock_timestamp())
   AND updated_at < clock_timestamp() - interval '7 days';
```

A failed scope that is still ticking is never selected (each tick's claim moves its `updated_at`); that is intended:
reset it explicitly, or let the slow retry heal it.

Preconditions before the flag may be turned on anywhere (1-3, 5 and 6 open, tracked for slice 2b):

1. **Erasure.** The consumer's erase pass now calls `search_reconcile_erase_scope_v1` (`eraseStagedReconcileRuns`)
   under the scope lock. Other erase paths (the operator session and user erase) do not yet, so the staged rows of a
   session erased that way outlive the erase. Those rows hold index identities (machine, session
   and event ids, timestamps, fingerprints) but no content. Calling it is not enough on its own: a tick that read its
   stamp before the erase can claim afterwards, re-creating the run and staging that session's identities again. Its
   final step then abandons the run, which drops the stage rows but keeps the `search_reconcile_runs_v1` row (org,
   workspace, session and canonical ids). So the erase must run under the scope lock or a fence the claim checks, and
   a sweep must delete abandoned runs and runs of sessions that never come back.
2. **Final transaction.** The final transaction is not bounded and has no keepalive. Its deletes, the directory write
   and (for a run resumed at that step) the source read all run in one transaction under the advisory locks.
3. **Fast-changing sessions.** A session whose source stamp changes faster than one run completes never finishes.
   This needs a policy and metrics on `attempts` and `last_error`.
4. **Consumer registration.** Done with the migration: the seven writers are in `CONSUMER_RUNTIME_FUNCTIONS` (the live
   sessions-function gate accepts them for the consumer login only) and in the `session_consumer_role_verify.sql`
   allowlist; the boundary lane is `search-reconcile-staging`. Keep both lists in step with any new writer.
5. **Passes 1-3 are not staged** (the stage table reserves the `fold` and `directory_seen` sides). A resumed run reads
   the source again whenever it still needs the read. When slice 2b stages them, note that those sides share the run's
   single `chunk_cursor` with `want`: they need their own cursor or a strict order within the `want` phase.
6. **Lease during the read.** The lease is not renewed while `readSource` runs. With one consumer that is harmless (an
   expired lease nobody takes over stays the holder's). With a second tick arriving after `leaseMs` but before a read
   longer than `leaseMs` ends, the second tick takes the run over, the first gets `RH062` at its next call, and the
   second re-reads and can be taken over the same way: repeated full reads, never wrong data. Before the flag is on
   with more than one consumer, add a heartbeat during the read or size `leaseMs` from the measured read time.

A claim never waits on a run row another transaction holds (a pass 4 write, the final step, a finish or erase in
flight): it answers busy, so redelivered ticks of a hot scope do not each hold a pool connection. Against another
claim's uncommitted first insert of the same scope it waits at most 2 s (`lock_timeout` on the function), then
answers busy. `staged-reconcile-claim.postgres.test.ts` proves both on real PostgreSQL in pg-proof.

#### Staged output facts (Phase 3 slice 2c)

Migration `20261105062140_staged_output_facts.sql` and the staged writer in `output-facts-store.ts` let a staged run
carry the self-host output facts (`RELAYHISTORY_SELFHOST_OUTPUT_FACTS=true`) without holding the session's whole fact
set: `sessions.output_facts_reconcile` takes it as one jsonb argument, which is why output facts turn streaming off
today.

- **Staging.** The wiring delivers the read's facts input once per attempt, at the end of the want phase, as
  numbered chunks (every post-fold record and projection row, in source order). Each chunk stages its event and
  message facts, its commit links, and what needs the whole session (tool calls and results for the commit check,
  legacy outcome and turn records, the session ids) in `sessions.staged_facts_v1`. The final transaction decides
  those, stages the result, and runs `sessions.staged_facts_reconcile_v1`: `output_facts_reconcile` over the staged
  rows, with the same version rule, upserts and complete-only deletes. It must run before `search_reconcile_finish_v1`,
  which drops the stage.
- **Lifecycle.** The facts stage is two tables of its own: `sessions.staged_facts_runs_v1` (one row per run: the
  chain state) references `search_reconcile_runs_v1` with a cascading foreign key, and `sessions.staged_facts_v1`
  references that. `search_reconcile_finish_v1` and `search_reconcile_erase_scope_v1` delete the run row, so the facts
  stage cascades; `search_reconcile_abandon_v1` and a claim restart on a changed source keep the run row and reset it,
  and the AFTER UPDATE trigger `staged_facts_reset` on the run table (its WHEN clause matches only that reset, one call
  per run update) deletes the facts stage. Nothing references `search_reconcile_stage_v1`, so 062110's bulk deletes
  of millions of stage rows cost what they cost without this step. No 062110 function is replaced.
- **Wholeness.** `staged_facts_runs_v1` records whether the stage holds the attempt's whole facts pass: chunk 0 drops
  whatever the run had staged and starts the chain, each later chunk must be the next number, and the want -> have
  advance seals it, whole only if the last chunk was seen. A stage that is not whole (facts turned on mid-pass or
  after it, a gap, no seal) is never reconciled. The final step throws `StagedOutputFactsIncompleteError` (a
  `StagedRunRestart`), the run is abandoned with `output_facts_stage_incomplete` and goes round again, and the facts
  tables keep what they had.
- **Wiring.** With `SEARCH_INDEX_STAGED_RECONCILE_ENABLED=true` and `RELAYHISTORY_SELFHOST_OUTPUT_FACTS=true` the
  consumer starts (it checks the 062140 writers at startup, naming the migration), streaming resolves on, and a scope
  the whole read refuses as too large runs as a staged run whose hooks stage, seal and reconcile its facts (in the
  final transaction, after the directory write, before the run finishes). A scope the whole read completes still
  takes the whole-set path. With a facts store and the staged reconcile off, a too-large scope stays incomplete
  (`scope_too_large`) as before; the in-transaction streamed reconcile never runs with facts. The ClickHouse facts
  flag (`SEARCH_INDEX_OUTPUT_FACTS`) is refused together with a staged run.
- **Chunk bounds.** The facts pass closes a chunk at 5000 inputs (never more than 25000, what one staged call takes
  at 4 items per input) or at 32 MiB of the inputs' JSON text, catalog rows included; one larger input is a chunk
  of its own. The writer checks the serialized call before sending it (128 MiB, half the database's jsonb limit). A
  chunk that still cannot be staged (over that, or refused by the database for its size or memory) abandons the run
  as `output_facts_chunk_unstageable` and writes no facts; any other staging error releases the lease and the next
  tick retries.
- **Exactness.** Over a whole read the staged facts equal the whole-set path's (`output-facts-parity.test.ts`). One
  difference: on a malformed event or commit link, the whole path stops at the first one and the staged path skips
  only that row. Both are incomplete with the same reason and delete nothing.
- **Memory.** Event and fact rows never leave the database. The final step holds in memory the scope's legacy
  records (kinds `legacy_conversation_turn`, `legacy_session_outcome`, `legacy_session_link`, `legacy_scope`,
  `conversation_turn`: each one parsed record body, as `outputFactsFromScope` holds them for the whole-set path), the
  session ids, and for each tool result with commit output the bodies of the tool calls with its tool use id and of
  their parent session events, tool arguments included. The streaming sizing above (about 3 KB per record, about
  2 GB at the default budgets) covers a record's keys and state, not its body: the parsed legacy record bodies come on
  top of it and are bounded only by the scope's bytes. Sending a facts chunk holds the chunk's entries plus their JSON
  text (several times the chunk's bytes), and the database holds the jsonb parameter. A facts chunk is bounded by inputs
  and by bytes ("Chunk bounds" above). Measured with
  `scripts/bench/staged-facts-rss.ts` (Bun, synthetic, the consumer process only; the stage lives in the database):
  the facts pass on top of passes 1-3 adds about 0.13 GB at 100k records (0.73 against 0.60 GB) and 0.23 GB at 1M
  (3.36 against 3.12 GB), at most one 5000-input chunk in flight (about 5 MB sent); the final step holding a
  session's legacy turns peaks at about 0.33 GB for 100k turns and 2.7 GB for 1M (about 2.7 KB per turn of about
  300 characters). The legacy part is the same fold the whole-set path makes over the same records.
- **Rollback.** `supabase/rollbacks/20261105062140_staged_output_facts.down.sql` removes the trigger, both tables and
  the functions. It refuses while any staged run (062110) holds a live lease, facts or not.
- **Operator resets.** The reset trigger's function runs as the role that updates `search_reconcile_runs_v1`. Every
  062110 writer is a SECURITY DEFINER function owned by the table owner, so an operator reset of a run must also go
  through one of them (or run as the owner), never as another role.

### Hosted Lambda: streamed ClickHouse reconcile (issue 610 L3 D1)

The hosted session pipeline Lambda has its own flags, separate from the self-host ones above (it keeps ignoring
`SEARCH_INDEX_STREAMING_ENABLED`). All are default off: with them unset the Lambda behaves as before and never
streams. `SEARCH_INDEX_STREAMING_CH` must be exactly `1` (unset, `""` or `0` is off; any other value fails
startup) and needs `SEARCH_INDEX_ENABLED=1` and the `clickhouse` search backend from the pipeline secret; it is
refused together with `SEARCH_INDEX_OUTPUT_FACTS=1`. Only with it on, `SEARCH_INDEX_STREAM_CH_MAX_READ_BYTES`
(default 1 GiB, accepted 256 MiB to 2 GiB, optional `MiB` / `GiB` suffix) and `SEARCH_INDEX_STREAM_CH_MAX_READ_EVENTS`
(default 1000000, accepted 400000 to 1000000) bound the read as above, and `SEARCH_INDEX_STREAM_DEADLINE_MARGIN_MS`
(default 60000, accepted 10000 to 240000) sets a deadline of the invocation's remaining time minus the margin. The
index scan reads the ClickHouse scope in keyset pages under the events budget; going past it leaves the scope
partial with `stream_read_events_budget`. The deadline is checked between chunk reads, between scan pages, before
the regenerating pass and before deletes; once passed, the scope stays partial with `scope_too_large` and
`stream_deadline`, keeps owing a pass, and nothing is deleted (rows already written stay). The deadline bounds the
streamed scope only, so at most one scope streams per invocation: a later scope in the same SQS batch that would
also stream is not started and is requeued (with the same delay as a busy scope, `stream_deferred` in the log) for
a later invocation, and the margin needs to cover only the rest of the batch's whole reads. The events cap is set by
memory: the index scan holds about 0.8 KB of keys per record (measured on Node 24: about 0.75 GiB of heap at
1000000 records), against a default heap limit of about 1.65 GiB at the stack's 3008 MiB `memorySize`, which also
has to hold one decompressed chunk and the whole reads; raise the cap only together with `memorySize`. The stack renders these
only from `transcriptStore.sessionPipeline.streamReconcile` with `enabled: true` (and `searchIndex: true`); its
`maxReadBytes`, `maxReadEvents` and `deadlineMarginMs` take the same ranges in plain numbers. Streamed output facts
(D2) and the large-scope worker (D3) are not in this cut.

## Known limits

- Alchemy's Docker provider has no health gating; the CLI waits.
- No epoch handling in the courier yet (a transcript that shrinks is skipped with a warning).
- `hoopdb-indexer` is one replica (reconcile has no leader election yet).
- Chunk objects carry the upstream scrub metadata but not `session-uuid`; adding it to the chunk hop is an upstream change we will propose.
- A forced removal of running containers under a busy Docker daemon was seen to hang for 20+ minutes on one machine; `down` now stops containers first and waits.
