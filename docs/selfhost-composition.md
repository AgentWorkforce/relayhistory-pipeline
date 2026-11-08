# Self-host composition: one image, one public origin

Status (2026-11-08): contract and dispatcher only. Nothing here is deployed, and
no chart, stack or migration changes come with it. Every service named below is
default-off. This is the handoff contract for whoever builds the stack
(infra-kind-stack or another orchestrator).

A self-host runs without RelayAuth and without the legacy relayhistory-cloud
Worker. History is served by the `sessions-*` services in this repository. The
user's relaycontrol (Control) is the only identity authority: it issues the
`sh_at_` user tokens and answers introspection.

## 1. The image and its SERVICEs

There is one image (`docker/services.Dockerfile`), and `SERVICE=` picks the
process (`docker/entrypoint.sh`). A value that is unset, empty or unknown exits
2 and runs nothing. `src/shared/__tests__/docker-entrypoint.test.ts` pins the
whole table.

| SERVICE | Kind | Entry | Port | Probes |
| --- | --- | --- | --- | --- |
| `hoopdb-migrate` | Job (one-shot, idempotent) | `src/hoopdb/migrate.ts` | none | exit code |
| `locker-init` | Job (one-shot) | `src/locker/init.ts` | none | exit code |
| `alleyoop-ingest` | Deployment | `src/alleyoop/ingest.ts` | pipeline routes (local stack: 18080) | see `docs/local-stack.md` |
| `alleyoop-courier`, `hoopdb-indexer`, `referee`, `bob-costas`, `session-brief` | Deployment (worker) | `src/<name>/...` | `WORKER_HEALTH_PORT`, default 8081, probes only | `GET /healthz`, `GET /readyz` (`src/shared/worker.ts` `serveHealth`) |
| `session-read-api` | Deployment (HTTP) | `src/session-read-api/server.ts` | `RELAYHISTORY_READ_API_PORT`, default 8080 | `GET /healthz` (200 `{"ok":true,"enabled":false\|true}`) |
| `session-publication` | Deployment (worker) | `src/session-ingest/publication-service.ts --continuous` | `WORKER_HEALTH_PORT`, default 8081, probes only | `GET /healthz`, `GET /readyz` (below) |
| `session-ingest` | Deployment (HTTP) | `src/session-ingest/selfhost-server.ts` | `RELAYHISTORY_INGEST_PORT`, default 8081 | `GET /healthz` (200 `{"ok":true,"enabled":false\|true}`), `GET /readyz` (section 3) |

The worker port serves probes only. It must never be reachable from outside
the cluster.

### `SERVICE=session-publication`

This is the canonical publisher (`publication-main.ts`, `--continuous`)
wrapped as a long-running container with the worker probe shape. It is off
unless `INGEST_PUBLISH_ENABLED` is exactly `true`. Unset or `false` is
dormant; any other value fails startup.

- **Dormant.** It does no config, credential, database or store work. The
  process stays up, both probes answer 200 `{"ok":true,"enabled":false}`, and
  SIGTERM exits 0. Rollouts complete, and enabling it later is an env change.
- **Enabled.** `publicationRuntimeConfig` is validated before the probe port
  opens, so a bad or missing target is a startup failure (exit 1, with a JSON
  error line carrying only a code).
  - `/healthz` answers 200 `{"ok":true,"enabled":true,"reason":"starting"|"ok"}`
    while the last cycle ended (or the service started) within
    `INGEST_PUBLISH_BUDGET_MS` (default 1 200 000) + 5 s idle wait + 60 s.
    Past that it answers 503 `loop_stalled`.
  - `/readyz` answers 503 `starting` until the first healthy cycle, then 200.
  - A failed cycle exits 1, and the orchestrator restarts the container.
    SIGTERM/SIGINT cancel owned IO and exit 0.
- Publication GC (`publication-main.ts --gc`, `INGEST_PUBLICATION_GC_ENABLED`)
  is separately armed and is **not** dispatched here.
- **Target selection. Read this before enabling.** `INGEST_PUBLICATION_TARGET`
  picks the runtime. Unset or `aws` is the existing host runtime, which requires
  `AWS_REGION=us-east-1`, `HOOPDB_DB_MODE=supabase`, IMDSv2 instance
  credentials, a Secrets Manager secret and the `INGEST_PUBLISH_*` target
  identity (`publication-runtime.ts`). On a plain kind or CNPG stack that fails
  startup with `publication_target_required`. Set `portable` instead
  (`publication-portable.ts`): an S3-compatible endpoint (MinIO) with static
  keys, a `bulls96_session_ingest` Postgres URL and an explicit
  `SESSION_POSTGRES_TLS_PROFILE`. The variables, Secret keys, role and grants,
  the owner-written `sessions.ingest_publication_frontiers` rows and the
  versioned-bucket requirement are listed in that file's header. Until the
  frontier rows exist, an enabled portable publisher runs but finds nothing to
  publish.

## 2. One public origin

A single public origin (for example `https://sessions.example.test`) fronts
everything. The router is the cluster's ingress or reverse proxy (an Ingress,
Gateway API HTTPRoute, Caddy, nginx, or anything else). No new proxy service is
needed. Rules are evaluated in order, first match wins, on the exact path with
no rewriting.

| # | Match | Upstream | Notes |
| - | --- | --- | --- |
| 1 | `POST /v1/cli/login` (exact) | session-read-api | the only read-api POST; 8 KiB body cap |
| 2 | `GET /v1/delivery/records` (exact) | session-read-api | babysitter evidence. It must win over rule 4. Default 501 `delivery_records_not_configured` |
| 3 | `GET /v1/sessions`, `/v1/sessions/*`, `GET /v1/events`, `GET /v1/digest/*` | session-read-api | read routes; read-api answers non-GET with 405 |
| 4 | `/v1/delivery/*`, `/v1/turns/*` | session-ingest | `POST /v1/delivery/batches`, `GET /v1/delivery/limits`, `POST /v1/delivery/recovery`, `POST /v1/delivery/sessions/{selection,reset}`, `POST /v1/turns/batches` |
| 5 | `/v1/onboarding/*` | session-ingest | `POST /v1/onboarding/heartbeat`, `GET /v1/onboarding/connection` |
| 5a | `POST /v1/auth/token/refresh` (exact) | session-ingest | always 401 `refresh_not_supported`. Required: without it the Desktop's refresh gets this table's 404, which it treats as an outage, and uploads stop when the token expires |
| 6 | anything else, including `/healthz`, `/readyz`, `/health`, `/v1/custody/*` | **none (404 at the edge)** | probes and custody are internal |

Requirements on the router:

- **Strip client-supplied trust headers.** Before forwarding, the router must
  remove `x-relay-ingest-identity` and `x-relay-ingest-signature` from every
  request. Today's ingest app takes tenancy from those headers, which only the
  relayhistory Worker sets in hosted mode. A forwarded client copy would be a
  tenant assertion.
- Send the original `Host` or `X-Forwarded-Host`. Do not rewrite paths: the
  ingest signature covers the path, and read-api matches `/v1/cli/login` exactly
  (a trailing slash is 405).
- Pass request bodies through unbuffered, or allow at least the ingest's
  `MAX_DELIVERY_BYTES` (2 MiB on the wire). Keep `Content-Encoding` (gzip and zstd are decoded by
  the ingest).
- Terminate TLS at the edge. Upstream hops may be plain http to Service DNS
  names only when the service sets `ALLOW_IN_CLUSTER_HTTP=true`.
- Internal-only, never routed publicly: every probe port (8081, plus read-api
  `/healthz`), `/v1/custody/*`, and `hoopdb-migrate`.

### Control audience

`READ_API_ORIGIN` is the public origin exactly: scheme and host, no path, no
trailing slash. Control must be configured with that same string as this
History origin. A read bearer or login token is introspected with
`aud = READ_API_ORIGIN`, and introspection must return
`historyOrigin === READ_API_ORIGIN`. Otherwise the request gets 401
`wrong_audience` or `unauthorized`. One origin means one audience, and the
session-ingest service must check the same value.

Read-api auth env:

- `RELAYHISTORY_ACCEPT_RELAYCONTROL_TOKENS=true`
- `RELAYCONTROL_INTROSPECT_URL`, which passes the transport rule
  (`ALLOW_IN_CLUSTER_HTTP`)
- `RELAYCONTROL_INTROSPECT_SECRET`, injected from the secret store

There is no switch to turn RelayAuth verification off. With `RELAYAUTH_JWKS_URL`
and `RELAYAUTH_ISSUER` unset, the verifier falls back to the hosted RelayAuth
defaults (`lambda.ts` `readApiAuthConfig`). Those tokens are still bound to
`aud = READ_API_ORIGIN`, so the hosted issuer would have to mint one for this
self-host origin. This is a residual trust in hosted RelayAuth. A self-host
that wants Control as its only authority needs a read-api change. This PR does
not make it.

**Known gap.** The `/v1/cli/login` response advertises only `rth:read`
(`docs/session-read-api-design.md` section 2a), so relaycontrol refuses
`mode=sync` against this origin. A single origin that also syncs needs the
login to advertise `rth:sync` when an ingest is configured. That is a
read-api change, not made here.

## 3. session-ingest contract

`src/session-ingest/app.ts` serves the delivery contract as a Cloudflare
Worker behind relayhistory (`worker.ts`). `SERVICE=session-ingest`
(`src/session-ingest/selfhost-server.ts`, default off with
`RELAYHISTORY_SELFHOST_INGEST`) serves the same app over node:http. Its exact
behaviour is in `docs/session-ingest-design.md` ("Self-host"). It differs from
the original sketch below in four ways:

- Port: it listens on `RELAYHISTORY_INGEST_PORT`, default 8081, not 8090.
- Audience: introspection uses `aud = RELAYHISTORY_INGEST_ORIGIN`.
- Served routes: only `POST /v1/delivery/batches`, `POST /v1/turns/batches`
  and `GET /v1/delivery/limits`.
- Not served yet: recovery, reset and onboarding answer 501
  `unsupported_by_backend`. Selection answers 501 too, unless
  `RELAYHISTORY_SELFHOST_SESSION_SELECTION=true` (default off; needs migration
  043910): then `enabled:false` is the per-session erase, one transaction that
  fences every origin the caller uploaded the session from, and the relay-turns
  origin, and tombstones those pointers and the directory row. `enabled:true`
  answers 400. Unreadable through the read API, not bytes gone. It covers the
  session-ingest upload path only; another user's copy is untouched, an origin
  first used after the erase is not fenced, and a session not yet indexed has
  no pointer to tombstone.

At startup it admits only a real login of `bulls96_session_ingest` with the
bootstrap's role shape. The original sketch:

1. Listen on its own port. 8090 is suggested, and the port is the router's
   upstream. It answers `GET /healthz`, default-off like read-api.
2. Authenticate the client's own bearer itself, since there is no relayhistory
   in front. It should take a relaycontrol `sh_at_` token through the same
   introspection, with `aud = READ_API_ORIGIN`, and require `rth:sync` for
   writes (`rth:read` for `GET /v1/onboarding/connection`). It must never
   accept `x-relay-ingest-identity` from the network.
3. Connect as `bulls96_session_ingest` and write the canonical S3 layout that
   `session-publication` reads.

## 4. Postgres roles and order

Every service gets its own login and its own Secret. No service connects as
the migration owner.

| Consumer | Role | Where it is set |
| --- | --- | --- |
| `hoopdb-migrate` | the database superuser / migration owner | `HOOPDB_URL` (Job only) |
| workers (`hoopdb-indexer`, `referee`, `bob-costas`, `session-brief`, `alleyoop-*`) | `HOOPDB_RUNTIME_ROLE` (exact app ACL from `migrate-guards.ts`) | `HOOPDB_URL` |
| `session-read-api` | `bulls96_read_api`: `LOGIN NOINHERIT NOSUPERUSER NOBYPASSRLS`, read-only, and not owner of the search schema (checked at startup) | `supabase_url` inside `RELAYHISTORY_READ_API_SECRET` (JSON) |
| session-ingest, `session-publication` | `bulls96_session_ingest` (the publisher refuses any other `current_user`) | ingest: its Secret; publisher: `hoopdb_url` in the Secrets Manager secret `INGEST_PUBLISH_SECRET_ARN` |

Order (plain Postgres or CNPG, `HOOPDB_DB_MODE=selfhosted`):

1. **Postgres up**, with `pg_trgm` and `btree_gin` available.
2. **`SERVICE=hoopdb-migrate`** (Job), as the superuser:
   - `HOOPDB_DB_MODE=selfhosted`
   - `HOOPDB_SUPABASE_SHIM=true|false` (explicit)
   - `HOOPDB_ROLE_BOOTSTRAP` (defaults to the shim choice)
   - `HOOPDB_RUNTIME_ROLE`, `HOOPDB_RUNTIME_ROLE_LOGIN`
   - `HOOPDB_FUNCTION_BOUNDARY_MODE=global-default-revoke`

   With the role bootstrap on, the migrator itself creates the bare NOLOGIN
   roles first: the search worker, the token verifier,
   `session_ingest_roles.sql`, `session_read_api_roles.sql` and
   `session_consumer_roles.sql` (single-sourced in
   `src/hoopdb/session-bootstrap-chain.ts`). Then it applies
   the migrations, and then the read-only verifies. With the bootstrap off, an
   operator runs those five `supabase/bootstrap/*.sql` files as the superuser
   **before** this Job. Migrations verify the roles and fail closed (42704) if
   they are absent.
3. **Bootstrap roles (post-migrate, operator, superuser).** This step makes
   the logins usable:
   - `session_ingest_role.sql`, then set the password, then
     `session_ingest_role_verify.sql`.
   - `session_read_api_role.sql` (LOGIN, timeouts, tenant RLS policies, and the
     id and epoch columns of `sessions.session_directory` that Postgres search's
     completeness check needs), then the password, then
     `session_read_api_role_verify.sql`.
   - The search schema owner runs `ensureSchema` for `search_index` and
     `projection_state`, and grants the read role `USAGE`/`SELECT` (read-api
     design, "Postgres search prerequisites").
   - The search worker marker
     (`runbooks/session-directory-search-worker-bootstrap.md`).

   Passwords go straight from the secret store into the Secrets. They are
   never written to disk or into a file in this repository.
4. **Services.** Workers block on `/readyz` until the ledger is complete, so
   their own order does not matter. Start `session-read-api` and
   session-ingest after step 3: read-api admits its login at startup (a real
   `bulls96_read_api` login: session identity, attributes, memberships both
   ways, pinned settings) and refuses otherwise with `reader_role_<property>`.
   `session-publication` comes last (dormant by default).

## 5. Env, Secret and CA names

Every flag is strict: exactly `true` enables, unset or `false` is off, and
anything else fails startup.

| SERVICE | Flag | Required when enabled | Secret material (from the secret store, never on disk) |
| --- | --- | --- | --- |
| `session-read-api` | `RELAYHISTORY_SELFHOST_READ_API` | `READ_API_ORIGIN`, `READ_API_COMPLETE_SINCE` (ISO or `epoch`), `READ_API_DIRECTORY_WORKSPACES` (canary allowlist; login answers 501 for an unlisted workspace), `SEARCH_INDEX_BACKEND` (`postgres`), `SEARCH_INDEX_POSTGRES_BACKEND_ID`, `SESSION_POSTGRES_TLS_PROFILE`, the relaycontrol trio in section 2, optional `ALLOW_IN_CLUSTER_HTTP`, `RELAYHISTORY_READ_API_PORT` | `RELAYHISTORY_READ_API_SECRET` (JSON `{supabase_url}`; no `clickhouse` with `postgres`), `RELAYCONTROL_INTROSPECT_SECRET` |
| `session-publication` | `INGEST_PUBLISH_ENABLED` | `INGEST_PUBLISH_{BUCKET,EXPECT_GENERATION,AWS_ACCOUNT_ID,SUPABASE_PROJECT_REF,INSTANCE_ID,SECRET_ARN,KMS_KEY_ARN,INSTANCE_PROFILE_ARN,ROLE_NAME}`, `AWS_REGION=us-east-1`, `HOOPDB_DB_MODE=supabase`, optional `INGEST_PUBLISH_BUDGET_MS`, `INGEST_PUBLISH_BATCH_LIMIT`, `WORKER_HEALTH_PORT` | none in env: IMDSv2 plus Secrets Manager (see the caveat in section 1) |
| `session-ingest` | `RELAYHISTORY_SELFHOST_INGEST` | `RELAYHISTORY_INGEST_ORIGIN`, `RELAYCONTROL_INTROSPECT_URL`, `RELAYHISTORY_INGEST_DATABASE_URL`, `SESSION_POSTGRES_TLS_PROFILE`, `LOCKER_BUCKET` (+ Locker endpoint/region or Pod Identity/GCS), optional `RELAYHISTORY_INGEST_PORT`, `INGEST_KEY_ROOT`, `INGEST_MAX_PER_USER`, `INGEST_MAX_PER_ORG`, `RELAYHISTORY_INGEST_PG_POOL_MAX`, `ALLOW_IN_CLUSTER_HTTP` | `RELAYCONTROL_INTROSPECT_SECRET`, `RELAYHISTORY_INGEST_DATABASE_URL` (password), Locker keys |
| workers | per service (`docs/local-stack.md`) | `HOOPDB_URL`, `WORKER_HEALTH_PORT` | `HOOPDB_URL` password |

CA: `SESSION_POSTGRES_TLS_PROFILE=custom-ca` with `SESSION_POSTGRES_CA_FILE`
(a mounted file, at most 128 KiB) and `SESSION_POSTGRES_CA_SHA256` (64 hex
characters, pinned). A mismatch fails startup. `supabase` selects the pinned
Supabase CA. There is no implicit default for the service.

## 6. Summaries and the model transport (Bob Costas, session-brief)

Both model users are off by default. With no change, Bob Costas writes
extractive summaries (no model call) and session-brief claims nothing.

### Enabling the Bob Costas model summariser

`BOB_COSTAS_MODEL` picks the summariser (`src/bob-costas/model-summarizer.ts`,
whose header is the reference for every variable):

| Variable | Default | Meaning |
| --- | --- | --- |
| `BOB_COSTAS_MODEL` | unset (`off`) | unset, empty, `off` or `extractive`: extractive only. Any other value is the model id (session-brief's `MODEL_PATTERN`); an invalid id fails startup |
| `OPENAI_API_KEY` | unset | the model credential, shared with session-brief, from the secret store. Unset, blank or `*-disabled`: every summary is extractive (reason `no_api_key`) |
| `OPENAI_BASE_URL` | `https://api.openai.com/v1` | OpenAI-compatible Responses API base, shared with session-brief; https, loopback, or an in-cluster Service with `ALLOW_IN_CLUSTER_HTTP=true`. A bad value fails startup |
| `BOB_COSTAS_BACKGROUND_DAILY_BUDGET` | 5000 | distinct sessions per org per UTC day that may use the model |
| `BOB_COSTAS_SESSION_DAILY_CAP` | 30 | model attempts per session per UTC day |
| `BOB_COSTAS_GLOBAL_DAILY_CAP` | 20000 | model attempts per UTC day across every org (0 disables model calls) |
| `BOB_COSTAS_MAX_ATTEMPTS` | 2 | a retryable failure (429, 5xx, timeout) is retried once, after `BOB_COSTAS_RETRY_DELAY_MS` (1000) |
| `BOB_COSTAS_MODEL_TIMEOUT_MS` | 20000 | per attempt, as session-brief |
| `BOB_COSTAS_MAX_OUTPUT_TOKENS_SHORT` / `_LONG` | 256 / 1024 | `max_output_tokens` per kind |
| `BOB_COSTAS_REASONING_EFFORT` | `none` | as session-brief sends; `minimal`/`low`/`medium`/`high`, or `omit` for a gateway that rejects the field |

The budget and caps are counted in memory by each Bob Costas process and reset
at the UTC day boundary, so with N replicas the effective ceiling is N times
the configured value. Size the values for the replica count.

The summariser never fails a stage because of the model. With no key, with the
budget or a cap exhausted, or when the call still fails after its retries, it
writes the extractive summary and logs a reason code (`no_api_key`, `budget`,
`session_cap`, `global_cap`, `model_failed` plus the provider error code). The
prompt is built from the session digest only (harness, counts, the first user
turns, the tool-call histogram, models, time span), every string scrubbed
first; the model's output is scrubbed before it is stored. Logs carry the model
id, token counts from the Responses usage block, attempt count and codes only:
never prompts, outputs, request or response bodies, provider error text or the
key.

### One transport for both services

Bob Costas reuses session-brief's Responses transport unchanged, so one
`OPENAI_*` configuration drives both: `OPENAI_API_KEY` and `OPENAI_BASE_URL`
are read by both services, while `SESSION_BRIEF_MODEL` and `BOB_COSTAS_MODEL`
pick each one's model. The Helm chart will expose this as:

| Helm value | Env |
| --- | --- |
| `intelligence.mode` | whether the model env is rendered at all (off by default) |
| `intelligence.baseUrl` | `OPENAI_BASE_URL` |
| `intelligence.briefModel` | `SESSION_BRIEF_MODEL` |
| `intelligence.bobCostasModel` | `BOB_COSTAS_MODEL` |
| `intelligence.existingSecret` | the Secret holding `OPENAI_API_KEY` |

(The chart values are a contract for the stack builder; this repository ships
no chart change.)

### Bedrock on self-host today

There is no native Bedrock provider. A self-host on AWS runs an in-cluster
OpenAI-compatible gateway (any gateway that serves the Responses API and calls
Bedrock with a pod IAM role), and points both services at it:

```
OPENAI_BASE_URL=http://<gateway>.<namespace>.svc:<port>/v1
ALLOW_IN_CLUSTER_HTTP=true
OPENAI_API_KEY=<the gateway's key, from the secret store>
BOB_COSTAS_MODEL=<the gateway's model id>
```

The IAM role belongs to the gateway, not to the 96bulls workers. Plain http is
accepted only for a Kubernetes Service name with `ALLOW_IN_CLUSTER_HTTP=true`;
anything else needs https.

### Follow-up: native Bedrock provider (scope only, no code now)

A native provider would need: SigV4 request signing from the pod's role
credentials (no `OPENAI_API_KEY`); a mapping from the Responses request
(instructions, input, JSON-schema output, `max_output_tokens`) to the Bedrock
Converse API and back (output text, `usage.inputTokens`/`outputTokens`, stop
reasons onto the existing provider error codes and their retry classes); and
region plus Bedrock model-id configuration, validated at startup like
`OPENAI_BASE_URL`. It would sit behind the same transport interface so both
services gain it together.

### Hosted activation

The hosted stack keeps the extractive summariser until a person flips it. The
change is one env value on the hosted Bob Costas worker:

- set `BOB_COSTAS_MODEL` to the chosen model id;
- confirm `OPENAI_API_KEY` and `OPENAI_BASE_URL` (if overridden) are already
  present there for session-brief; Bob Costas reads the same names;
- optionally set the `BOB_COSTAS_*` budgets above.

Budget sizing for the hosted flip (Khaliq, 2026-11-08, relayed by his desktop
lead; the pipeline lead's concern that hosted isolates multiply an in-memory
cap is covered by the sizing rule): per-process counters are acceptable for
the first flip, with a conservative global cap divided by the replica or
isolate count. Set `BOB_COSTAS_GLOBAL_DAILY_CAP` and
`BOB_COSTAS_BACKGROUND_DAILY_BUDGET` to the intended total divided by the
expected isolate count, keep the per-session cap
(`BOB_COSTAS_SESSION_DAILY_CAP`), and prefer a provider-side spend cap on the
API key or project as the backstop. A database-backed shared counter (a new
table, so a migration-registry slot) follows after the schema freeze lifts.
Until the flip, `BOB_COSTAS_MODEL` stays `off` on the hosted worker.

That flip and its deploy are a separate production change and need Khaliq's
own explicit approval. Nothing in this repository changes the hosted or local
value; the local stack (`alchemy.run.ts`) stays at `BOB_COSTAS_MODEL: "off"`.
