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
| `session-erase-bytes` | Job (one-shot, operator-run, default off) | `src/session-erase/selfhost-eraser-main.ts` | none | exit code (0 verified / disabled / dry run, 3 not verified (rerun), 2 refused or misconfigured (do not retry unchanged), 4 failed, may be partial (rerun)) |

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

### `SERVICE=session-erase-bytes`

The self-host per-session byte eraser
(`docs/operator-session-erase-selfhost-eraser.md`). It is a one-shot Job, never
a Deployment, and is off unless `SESSION_ERASE_BYTES_ENABLED` is exactly `1`
(not `true`: see the note in section 5). Unset, empty or `0` prints
`{"enabled":false}` and exits 0 without reading config, credentials or the
request, and without touching the store. Any other value exits 2.

When enabled it reads one erase request (JSON) from
`SESSION_ERASE_REQUEST_FILE` (a mounted file), deletes every version and delete
marker of that session's transcript directories, live pointer and supplied
staged objects in the versioned S3-compatible store, lists again, and prints
one count-only result line. It is internal: no port, no route, and it never
runs on a schedule. The operator orchestrator decides when to run it.

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
| 5 | `/v1/onboarding/*` | session-ingest | `POST /v1/onboarding/heartbeat`, `GET /v1/onboarding/connection`. Self-host: the heartbeat answers 204 (body discarded, `rth:sync`) only with `RELAYHISTORY_SELFHOST_HEARTBEAT_ACCEPT=true`, otherwise 501; `connection` is always 501 |
| 5a | `POST /v1/auth/token/refresh` (exact) | session-ingest | always 401 `refresh_not_supported`. Required: without it the Desktop's refresh gets this table's 404, which it treats as an outage, and uploads stop when the token expires |
| 6 | anything else, including `/healthz`, `/readyz`, `/health`, `/v1/custody/*`, `/internal/*` | **none (404 at the edge)** | probes, custody and operator routes are internal |

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
  `/healthz`), `/v1/custody/*`, `/internal/*` (the operator session-erase route,
  `docs/operator-session-erase-contract.md`), and `hoopdb-migrate`.

### Control audience

`READ_API_ORIGIN` is the public origin exactly: scheme and host, no path, no
trailing slash. Control must be configured with that same string as this
History origin. A read bearer or login token is introspected with
`aud = READ_API_ORIGIN`, and introspection must return
`historyOrigin === READ_API_ORIGIN`. Otherwise the request gets 401
`wrong_audience` or `unauthorized`. One origin means one audience, and the
session-ingest service must check the same value.

Read-api auth env, one of three `READ_API_AUTH_MODE` settings
(`src/session-read-api/lambda.ts` `readApiAuthMode`; errors name variables,
never values):

- **Native (Control issues read sessions): `READ_API_AUTH_MODE=self_issued`.**
  The read API serves only its own opaque `rth_at_read_` tokens
  (`src/session-read-api/self-issued.ts`, hash rows from migration 044000,
  `aud = READ_API_ORIGIN`). relaycontrol's `POST /api/v1/workspaces/{ws}/relayhistory/session`
  in `RELAYHISTORY_BACKEND=native` mode decides membership and calls
  `POST /internal/v1/session-read/sessions` (and `/refresh`) over the in-cluster
  Service URL with `READ_API_INTERNAL_SECRET` (relaycontrol's
  `SESSION_READ_API_SECRET`). This mode refuses `RELAYAUTH_JWKS_URL`,
  `RELAYAUTH_ISSUER`, `RELAYCONTROL_INTROSPECT_URL`,
  `RELAYCONTROL_INTROSPECT_SECRET`, `RELAYHISTORY_ACCEPT_RELAYCONTROL_TOKENS`
  and `RELAYHISTORY_SELFHOST_LOGIN_ADVERTISES_SYNC` at startup: the read API
  never calls Control or RelayAuth, and `POST /v1/cli/login` is not registered
  (`RELAYHISTORY_SELFHOST_READ_API=true` then adds only the catalog route). No
  RelayAuth anywhere. This is the self-host target (lead-desktop, 2026-11-09).
- **Control-proxied reads: `READ_API_AUTH_MODE=relaycontrol_only`.** Only
  relaycontrol `sh_at_` bearers, introspected at `RELAYCONTROL_INTROSPECT_URL`
  (transport rule `ALLOW_IN_CLUSTER_HTTP`) with `RELAYCONTROL_INTROSPECT_SECRET`
  and `aud = READ_API_ORIGIN`; needs `RELAYHISTORY_ACCEPT_RELAYCONTROL_TOKENS=true`
  and refuses the `RELAYAUTH_*` variables. No JWKS is fetched and every other
  bearer is 401 without an outbound call. With `RELAYHISTORY_SELFHOST_READ_API=true`
  this mode also registers `POST /v1/cli/login` (pass-through exchange of the
  `sh_at_` token, `docs/session-read-api-design.md` section 2a).
- **Default (unset): the hosted dual path.** `sh_at_` tokens through
  relaycontrol when `RELAYHISTORY_ACCEPT_RELAYCONTROL_TOKENS=true`, every other
  bearer through the RelayAuth JWKS verifier (hosted defaults when
  `RELAYAUTH_JWKS_URL` and `RELAYAUTH_ISSUER` are unset). This is the only mode
  that still trusts RelayAuth; a self-host install uses one of the two above.

**`rth:sync` on one origin.** In native mode relaycontrol's `mode=sync` answer
hands the Desktop its own `sh_at_` token as the upload bearer with `baseUrl` =
this public origin; the session-ingest service (section 3) introspects it on
every request and requires the granted `rth:sync` scope. No read-api setting is
involved. Only the `relaycontrol_only` + `/v1/cli/login` path has the login
reply advertise scopes: there, `RELAYHISTORY_SELFHOST_LOGIN_ADVERTISES_SYNC=true`
(Pattern A, default off, `docs/session-ingest-design.md`) adds `rth:sync` to the
reply when introspection granted it; without it relaycontrol refuses
`mode=sync` against this origin.

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
  and `GET /v1/delivery/limits`. The limits answer adds `sessionDeletion: false`
  and `sessionReset: false` at the top level (next to `maxRecords`,
  `maxRequestBytes` and `contentEncodings`), so the Desktop hides its delete and
  reset controls; the hosted answer is unchanged.
- Onboarding heartbeat: `POST /v1/onboarding/heartbeat` (the Desktop's
  best-effort upload progress) answers 501 by default. With
  `RELAYHISTORY_SELFHOST_HEARTBEAT_ACCEPT=true` (default off) it answers 204
  with no body after the same introspection and `rth:sync` check; the body is
  discarded (a body over 8 KiB is not read and the connection closes), never
  stored or logged.
- Not served yet: recovery, reset and the rest of onboarding answer 501
  `unsupported_by_backend`. Selection answers 501 too, unless
  `RELAYHISTORY_SELFHOST_SESSION_SELECTION=true` (default off; needs migration
  043910): then `enabled:false` is the per-session erase, one transaction that
  fences every origin the caller uploaded the session from, and the relay-turns
  origin, and tombstones those pointers and the directory row. `enabled:true`
  answers 400. Unreadable through the read API, not bytes gone. It covers the
  session-ingest upload path only; another user's copy is untouched, an origin
  first used after the erase is not fenced, and a session not yet indexed has
  no pointer to tombstone.
- Operator session erase: `/internal/v1/session-erase/...`
  (`docs/operator-session-erase-contract.md`) is reachable only from inside the
  cluster, for relay-control. The wrapper forwards GET and POST (other methods: 405) to the app without
  relaycontrol introspection; the app checks its own bearer,
  `SESSION_ERASE_API_SECRET`. It is default off (`SESSION_ERASE_API_ENABLED`),
  and today it always answers 501 `not_implemented`, even when enabled.

At startup it admits only a real login of `bulls96_session_ingest` with the
bootstrap's role shape.

`SELFHOST_INGEST_ADMISSION=async` (default `sync`) moves the database off the
upload request path: one create-only inbox PUT, then the hosted queued receipt,
with an in-process consumer that admits the inbox. It also serves
`GET /v1/delivery/admission` (rule 4 already routes it here), and `/readyz`
follows the consumer instead of the database. Every upload still makes one
relaycontrol introspection call. See `docs/selfhost-async-admission.md`. The original sketch:

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
| `session-read-api` digest narrative writer (optional, off by default; section 6) | `bulls96_digest_narrative`: `LOGIN NOINHERIT`, EXECUTE on `sessions.digest_narrative_upsert` only (checked at startup) | `READ_API_DIGEST_NARRATIVE_DB_URL` |
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
   - Only for the optional digest narrative writer (section 6):
     `digest_narrative_role.sql`, then the password. Needs Khaliq's OK.

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
anything else fails startup. One exception: `SESSION_ERASE_API_ENABLED` shares
its value with the hosted Worker, so it is exactly `1` (unset, empty or `0` is
off; anything else, `true` included, fails startup).

| SERVICE | Flag | Required when enabled | Secret material (from the secret store, never on disk) |
| --- | --- | --- | --- |
| `session-read-api` | `RELAYHISTORY_SELFHOST_READ_API` | `READ_API_ORIGIN`, `READ_API_COMPLETE_SINCE` (ISO or `epoch`), `READ_API_DIRECTORY_WORKSPACES` (canary allowlist; login answers 501 for an unlisted workspace), `SEARCH_INDEX_BACKEND` (`postgres`), `SEARCH_INDEX_POSTGRES_BACKEND_ID`, `SESSION_POSTGRES_TLS_PROFILE`, the relaycontrol trio in section 2, optional `ALLOW_IN_CLUSTER_HTTP`, `RELAYHISTORY_READ_API_PORT`, `READ_API_CANONICAL_PIN_LAGGING_POINTER` (recommended `true` with `SELFHOST_INGEST_ADMISSION=async`; default off; any value other than `true` is off, see `docs/selfhost-async-admission.md`), `READ_API_CANONICAL_SCOPE_BYTES` / `READ_API_CANONICAL_SCOPE_EVENTS` (canonical snapshot budget; defaults 32 MiB / 100000 (raise bytes only with the pod memory limit), bytes accepted 16 MiB..2 GiB, events up to 4000000; out of range fails startup, see `docs/local-stack.md`) | `RELAYHISTORY_READ_API_SECRET` (JSON `{supabase_url}`; no `clickhouse` with `postgres`), `RELAYCONTROL_INTROSPECT_SECRET` |
| `session-publication` | `INGEST_PUBLISH_ENABLED` | `INGEST_PUBLISH_{BUCKET,EXPECT_GENERATION,AWS_ACCOUNT_ID,SUPABASE_PROJECT_REF,INSTANCE_ID,SECRET_ARN,KMS_KEY_ARN,INSTANCE_PROFILE_ARN,ROLE_NAME}`, `AWS_REGION=us-east-1`, `HOOPDB_DB_MODE=supabase`, optional `INGEST_PUBLISH_BUDGET_MS`, `INGEST_PUBLISH_BATCH_LIMIT`, `WORKER_HEALTH_PORT` | none in env: IMDSv2 plus Secrets Manager (see the caveat in section 1) |
| `session-ingest` | `RELAYHISTORY_SELFHOST_INGEST` | `RELAYHISTORY_INGEST_ORIGIN`, `RELAYCONTROL_INTROSPECT_URL`, `RELAYHISTORY_INGEST_DATABASE_URL`, `SESSION_POSTGRES_TLS_PROFILE`, `LOCKER_BUCKET` (+ Locker endpoint/region or Pod Identity/GCS), optional `RELAYHISTORY_INGEST_PORT`, `INGEST_KEY_ROOT`, `INGEST_MAX_PER_USER`, `INGEST_MAX_PER_ORG`, `RELAYHISTORY_INGEST_PG_POOL_MAX`, `ALLOW_IN_CLUSTER_HTTP`, `SELFHOST_INGEST_ADMISSION` (`sync` default, `async`, `drain`) with `SELFHOST_ADMISSION_CONCURRENCY` / `_SCAN_MS` / `_MAX_ATTEMPTS`, `SESSION_ERASE_API_ENABLED` (`1`; default off), `RELAYHISTORY_SELFHOST_SESSION_SELECTION`, `RELAYHISTORY_SELFHOST_HEARTBEAT_ACCEPT` (both `true`; default off) | `RELAYCONTROL_INTROSPECT_SECRET`, `RELAYHISTORY_INGEST_DATABASE_URL` (password), Locker keys, `SESSION_ERASE_API_SECRET` (+ `_PREVIOUS` during a rotation; at least 32 characters; only read when the erase flag is on) |
| `session-erase-bytes` (Job) | `SESSION_ERASE_BYTES_ENABLED` (exactly `1`; see below) | `SESSION_ERASE_S3_BUCKET`, `SESSION_ERASE_S3_REGION`, `SESSION_ERASE_REQUEST_FILE`, optional `SESSION_ERASE_S3_ENDPOINT` (bare origin; https, loopback, or a Service name with `ALLOW_IN_CLUSTER_HTTP`), `SESSION_ERASE_KEY_ROOT` (default `96bulls`, must equal `INGEST_KEY_ROOT`), `SESSION_ERASE_DRY_RUN` (`1` = list and count only), `SESSION_ERASE_CONCURRENCY` (1..16, default 4) | `SESSION_ERASE_S3_ACCESS_KEY_ID[_FILE]`, `SESSION_ERASE_S3_SECRET_ACCESS_KEY[_FILE]`: a separate eraser key (ListBucketVersions + DeleteObjectVersion on the key root), not the ingest writer's key |
| workers | per service (`docs/local-stack.md`) | `HOOPDB_URL`, `WORKER_HEALTH_PORT` | `HOOPDB_URL` password |

Installs with bigger sessions must raise the read API container memory limit and
`READ_API_CANONICAL_SCOPE_BYTES` together, because a raw scope is held several times over per request. The shipped
pairing is the 32 MiB default against the charts' 512Mi read API limit (umbrella chart 1Gi), so keep roughly that
ratio when raising the budget (for example 64 MiB needs at least 1Gi, and a 1.5Gi limit is the pairing used for a
64 MiB dev budget). The hosted Worker budget stays 16 MiB and is not affected.

`SESSION_ERASE_BYTES_ENABLED` is the one exception to the `true` rule above:
the operator-erase contract fixes the literal `1`. `true` is not accepted; it
fails startup like any other unknown value, so it is never silently off.

### session-ingest admission mode (config contract)

`SELFHOST_INGEST_ADMISSION` is `sync` (the default; unset or empty is the same),
`async` or `drain`. Any other value fails startup. The knobs
`SELFHOST_ADMISSION_CONCURRENCY` (1..32, default 4), `SELFHOST_ADMISSION_SCAN_MS`
(1000..600000, default 15000) and `SELFHOST_ADMISSION_MAX_ATTEMPTS` (1..100,
default 20) are read only in `async` and `drain`. The mode is never taken from a
request. Semantics: `docs/selfhost-async-admission.md`.

Once enabled, both `GET /healthz` and `GET /readyz` pin the effective mode:

```json
{ "ok": true, "enabled": true, "admission": "sync" }
```

In `async` and `drain` they also report the consumer:

```json
{ "ok": true, "enabled": true, "admission": "async",
  "consumer": { "ok": true, "state": "ok", "backlog": 0, "oldestAgeMs": null,
                "deadLettered": 0, "inFlight": 0, "lastScanAgeMs": 1200 } }
```

- `admission`: exactly `sync`, `async` or `drain`. The chart compares it with
  the value it set and refuses the rollout on a mismatch. An image older than
  this contract reports no `admission` field.
- `consumer.state`: `starting` (no scan has completed yet), `ok`, `stale` (no
  successful scan for max(3 × scan interval, 60 s)) or `stopped`. `consumer.ok`
  is true only for `ok`. `oldestAgeMs` is `null` when the backlog is empty.
- `/healthz` stays 200 whatever the consumer says (liveness).
- `/readyz` status:
  - `sync`: 200 iff the ledger answers `SELECT 1` (unchanged);
  - `async`: 200 iff `consumer.ok` (the database is not on the upload path);
  - `drain`: 200 iff both.
- A dormant edge (`RELAYHISTORY_SELFHOST_INGEST` unset) reports neither field.

**`drain` semantics.** New uploads take the synchronous path (SQL in the
request; nothing new enters the inbox). The consumer keeps running and
admits the backlog already in the inbox. It is not "consumer only": the
upload routes are served. Roll back `async` -> `drain` -> wait for
`consumer.backlog` 0 and `consumer.deadLettered` 0 -> `sync`.

**Object-store permissions (async and drain).** `<root>` is `INGEST_KEY_ROOT`
(default `96bulls`). Every key below sits in the srv-writer shape
`<root>/*/v1/ws=*/m=srv_*`. The three dot-prefixed roots are disjoint from every
tenant prefix, because no org id may start with a dot.

Today the request path and the consumer are one process with ONE Locker
credential, so that credential needs the union of the two role tables below.
They are split so that a future separate consumer SERVICE can take a narrower
grant. `J` is the privacy journal,
`<root>/.admission-inbox/<org>/v1/ws=<ws>/m=srv_<sha256(ws,user)[0:32]>/privacy-journal.json`.
It is the ONLY privacy object in the store: the deletion fences, reset counts
and control heads live in SQL (read by the claim), and nothing else in S3 is
consulted during admission.

*Request-path role (`async` uploads; `drain` and `sync` uploads use the synchronous tenant-data set instead):*

| Prefix / key | Operations |
| --- | --- |
| `J` (the upload's witness, read before the body) | GetObject. A missing object must answer 404 (needs `s3:ListBucket`, below). |
| `<root>/.admission-inbox/<org>/v1/ws=<ws>/m=srv_<machine>/r=<receipt>/a=<ms>-<nonce>.ndjson.zst` | PutObject create-only (`If-None-Match: *`). On a 412 the adapter runs HeadObject, plus GetObject when the digest metadata is missing, to accept identical bytes. |
| `…/r=<receipt>/outcome-admitted.json`, `outcome-refused.json` | GetObject (the `GET /v1/delivery/admission` status read) |

*Consumer role (`async` and `drain`):*

| Prefix / key | Operations |
| --- | --- |
| `<root>/.admission-inbox` | ListBucket with prefix `<root>/.admission-inbox` |
| the inbox object | GetObject; PutObject **unconditional overwrite** with a small tombstone after the terminal outcome. Never DeleteObject. |
| `…/r=<receipt>/outcome-admitted.json`, `outcome-refused.json` | PutObject overwrite |
| `J` (the post-claim privacy gate) | GetObject. PutObject with `If-Match: <etag>` (or `If-None-Match: *` on a missing journal) only to complete a control left pending for more than 120 s. Self-host serves no journaled control today, so this write does not happen in practice, but grant it. |
| `<root>/.admission-dlq/<inbox path minus .ndjson.zst>.json` | ListBucket with prefix `<root>/.admission-dlq`; PutObject overwrite (the dead-letter marker); DeleteObject (best effort after a later admission; also the operator's redrive) |
| tenant data `<root>/<org>/…` | exactly the synchronous path's set (chunks, manifests, pointers: GetObject, HeadObject, conditional PutObject). Canonical acceptance stages nothing, so there is no `buffered-batches` stage prefix. |

*Startup gate (whichever role starts with `async` or `drain`; once per process start):*

| Key | Operations |
| --- | --- |
| `<root>/.admission-probe/v1/ws=probe/m=srv_<random>/probe.json` | PutObject create-only; a second create that must be refused (with the HeadObject reconcile above); PutObject `If-Match` twice (the stale one must be refused); GetObject; DeleteObject best effort. If DeleteObject is denied, one ~60-byte object per start remains and startup still succeeds. Any other failure refuses startup. |

*Operator (not a service role):* DeleteObject on `<root>/.admission-dlq/…` to redrive.

Backend notes:

- **S3 / MinIO.** Grant `s3:ListBucket` on the bucket (prefix-conditioned to the
  two listings above, plus whatever the sync path already needs). Without it S3
  answers a missing key with 403 instead of 404, and a missing privacy journal
  (the normal case) would read as an error. Every upload would then be a 503.
  MinIO must honour `If-None-Match` and `If-Match` on PUT; the startup gate
  checks this.
- **GCS.** Creates use `x-goog-if-generation-match: 0` and swaps use
  `x-goog-if-generation-match: <generation>`. Overwriting an existing object
  (the tombstone, outcome and marker writes) needs `storage.objects.delete` as
  well as `storage.objects.create`. Listing needs `storage.objects.list`.
- `sync` touches none of the dot-prefixed roots and runs no probe.

CA: `SESSION_POSTGRES_TLS_PROFILE=custom-ca` with `SESSION_POSTGRES_CA_FILE`
(a mounted file, at most 128 KiB) and `SESSION_POSTGRES_CA_SHA256` (64 hex
characters, pinned). A mismatch fails startup. `supabase` selects the pinned
Supabase CA. There is no implicit default for the service.

## 6. Summaries and the model transport (Bob Costas, session-brief)

Every model user is off by default. With no change, Bob Costas writes
extractive summaries (no model call) and session-brief claims nothing.

### Native canonical session briefs

For native ingest, run `SERVICE=session-brief` with
`SESSION_BRIEF_ENABLED=true`, `SESSION_BRIEF_CANONICAL_INPUT=true` and an
explicit comma-separated `SESSION_BRIEF_WORKSPACE_IDS` allowlist. Reuse the
strict-TLS `HOOPDB_URL` for the dedicated application runtime role and the
existing read-only Locker bindings. `PIPELINE_ORG_FROM_PREFIX` must be `true`
and `PIPELINE_KEY_PREFIX` must be the bare ingest root. The model credential
is `OPENAI_API_KEY`, supplied through the operator's secret store; without a
usable key the worker claims nothing.

This input path reads complete current canonical objects, including sessions
older than the workspace search window. It does not move the search floor or
use the search projection as complete session content. It supports one live
physical source per logical session, with consistent canonical identity across
its machines; ambiguous aliases refuse generation and reads. Publication keeps
the existing per-machine revision fence, leased wakeup and model budget checks.

Set `SESSION_BRIEF_SWEEP_SECONDS` to a positive interval to discover both
missing briefs and changed canonical revisions without the legacy indexer.
`SESSION_BRIEF_BATCH`, `SESSION_BRIEF_GLOBAL_DAILY_CAP` and
`SESSION_BRIEF_SESSION_DAILY_CAP` bound work and spend. Keep one worker for
initial qualification; do not enable the legacy indexer to produce wakeups.

Apply the complete migration plan, including
`20261105044100_session_brief_summary_read.sql`, before starting services with
the new schema generation. The Node read service then serves
`GET /v1/sessions/:id/summary?workspace=<id>&source=<source>&kind=short` for its
existing directory-workspace allowlist. It returns only a fresh ready native
model description with its stored model and generation timestamp; missing or
pending output is explicitly empty. State is `null` because the brief's state
enum is not the dashboard state contract. The reader receives only a scoped
function grant. The consumer receives no new summary-table privileges; its
existing SELECT is retained for native title precedence.

The same migration stores bounded recorded catalog titles separately. Native model
titles win over pointer titles; a current complete recorded title is the final list
fallback and does not require a model credential or model call. Source invalidation
and deletion clear that fallback. Existing sessions need a normal complete canonical
reprojection to populate the new slot; no direct title backfill is implied.

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

### Daily digest narrative on the read API service

The read API service (`SERVICE=session-read-api`, the `service` entry of
`src/session-read-api/lambda.ts`) can generate the daily digest narrative
in-process (`src/session-read-api/digest-narrative-selfhost.ts`, whose header is
the reference). It reuses the hosted narrative's prompt, its 10 s model timeout,
its 3 s read deadline and the migration `20261105045000` cache. No new
migration is needed. Everything is off by default:

| Variable | Default | Meaning |
| --- | --- | --- |
| `READ_API_DIGEST_ENABLED` | unset (off) | exactly `1`: build the narrative deps. Anything else: no narrative variable is read and the service is unchanged |
| `READ_API_DIGEST_NARRATIVE_MODEL` | unset | unset or blank: `provider_not_configured` (no model call, no writer pool). Otherwise the chat-completions model id (`MODEL_PATTERN`); an invalid id fails startup |
| `OPENAI_API_KEY` | unset | shared with session-brief and Bob Costas. Unset, blank or `*-disabled`: no generation |
| `OPENAI_BASE_URL` | `https://api.openai.com/v1` | calls go to `<base>/chat/completions` (not the Responses API), under the same destination rule (`ALLOW_IN_CLUSTER_HTTP=true` for an in-cluster Service). A Bedrock gateway must serve chat completions too |
| `READ_API_DIGEST_NARRATIVE_DB_URL` | unset | the narrative writer login. Unset: the narrative is read-only from the cache (hits served, a miss is `provider_not_configured`, no model call) |

The writer is a separate pool, never the read pool. It has 1 connection, a 5 s
`statement_timeout` (the role's own pin), the `SESSION_POSTGRES_TLS_PROFILE`
trust, and discrete URL fields (nothing in the query string reaches pg). It
runs only `sessions.digest_narrative_upsert`. Migration `20261105045000` grants
EXECUTE on that function to `bulls96_digest_narrative` alone, and the function
refuses any other caller. So the URL must be that role's login (provisioned by
`supabase/bootstrap/digest_narrative_role.sql`). Startup refuses a writer login
that is not exactly that role, or that is superuser or BYPASSRLS. The read pool
stays read-only as `bulls96_read_api` and reads the cache through
`digest_narrative_read`.

Trust note: the writer role can upsert a narrative for any tenant. On the hosted
stack, that credential lives only in the separate narrative Worker. Here it
lives in the read API process, and the verified caller's tenant is the only
guard. Provisioning the role's password and the URL secret, through the
operator's secret store and never on disk, needs Khaliq's explicit OK, as do
deploys and applying `20261105045000`.

Generation runs detached from the request. A slow miss answers
`provider_error` at the 3 s deadline, then the model call finishes within its
10 s timeout and is cached for the next read. Concurrent reads of one key share
one call. At most 4 generations run per process. The prompt is the digest's
aggregates plus up to 15 session summaries, project ids and branches. It is
scrubbed before it is sent, and the narrative is scrubbed before it is cached.
Logs carry outcome codes only.

This entry does not serve `GET /v1/digest/daily` yet: the route needs a digest
backend on this service. Until that wiring passes these deps to the route, it
answers 501 `digest_not_configured` whatever these variables say.

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
