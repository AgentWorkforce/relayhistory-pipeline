# Self-host composition: one image, one public origin

Status (2026-11-08): contract and dispatcher only. Nothing here is deployed, and
no chart, stack or migration changes come with it. Every service named below is
default-off. This is the handoff contract for whoever builds the stack
(infra-kind-stack or another orchestrator).

A self-host runs without RelayAuth and without the legacy relayhistory-cloud
Worker. History is served by the `sessions-*` services in this repository. The
user's relaycontrol (Control) is the only identity authority: it issues the
`sh_at_` user tokens and answers introspection.

For a local-only, single-user install of this composition (docker compose behind an nginx TLS edge), see [selfhost-local.md](selfhost-local.md).

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
| `session-analysis` | Deployment (worker, default off; `SESSION_ANALYSIS_ENABLED=true`) | `src/session-analysis/runner.ts` | `WORKER_HEALTH_PORT`, default 8081, probes only | `GET /healthz`, `GET /readyz`; exits 2 on a refused config ([session-analysis-runner.md](session-analysis-runner.md)) |
| `session-ingest` | Deployment (HTTP) | `src/session-ingest/selfhost-server.ts` | `RELAYHISTORY_INGEST_PORT`, default 8081 | `GET /healthz` (200 `{"ok":true,"enabled":false\|true}`), `GET /readyz` (section 3) |
| `session-erase-bytes` | Job (one-shot, operator-run, default off) | `src/session-erase/selfhost-eraser-main.ts` | none | exit code (0 verified / disabled / dry run, 3 not verified (rerun), 2 refused or misconfigured (do not retry unchanged), 4 failed, may be partial (rerun)) |
| `user-erase` | Job (one-shot, operator-run, default off; `OPERATOR_USER_ERASE_ENABLED=1`) | `src/session-erase/user-erase-main.ts` | none | exit code (0 verified / disabled, 3 not verified (rerun), 2 refused or misconfigured (do not retry unchanged), 4 failed, may be partial (rerun)); see `docs/operator-user-erase.md` |
| `s3-ledger-rearm-sweeper` | Deployment (long-running `--loop`, default off; `INGEST_REARM_SWEEPER_ENABLED=true`) | `src/session-ingest/rearm-sweeper-main.ts` | none | one JSON line per pass (counts and codes only); exits 2 on a refused config |
| `selfhost-control` | Deployment (HTTP, local single-user only, default off; `RELAYHISTORY_SELFHOST_CONTROL=true`) | `src/selfhost-control/server.ts` | `SELFHOST_CONTROL_LISTEN`, default `0.0.0.0:8090` | `GET /healthz` (200 `{"ok":true}`); exits 2 on a refused config (below) |

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
  On a GCS install set `LOCKER_PROVIDER=gcs` on the publisher as well as on the
  read API: the publisher stamps that provider on every published row, the read
  API refuses rows stamped with another provider, and the SQL admits `s3` and
  `gcs` from migration `20261105059000` onward. Unset means `s3`.
  The pipeline consumer (`SERVICE=session-pipeline-consumer`) stamps its
  Locker's provider on the session_index rows it writes, so it needs
  `LOCKER_PROVIDER=gcs` too. Rows an older consumer stamped `s3` on a GCS
  install stay unreadable (501 `unsupported_by_backend`) until
  `src/session-pipeline/index-provider-restamp.ts` re-stamps them: plan by
  default, `--apply` to write, refused unless the Locker is GCS, and it
  re-stamps only rows in `LOCKER_BUCKET` whose manifest the GCS Locker holds.
  Its header lists the env, the owner login it needs and the exit codes.
  Local-only installs may instead name `LOCKER_PROVIDER=fs` or `azure` (never
  selected when unset; `src/shared/locker-selection.ts`). `fs` stores objects
  under `LOCKER_FS_ROOT` (an absolute, normalized path other than `/`;
  `LOCKER_BUCKET` is still required as the name stamped on rows). It is refused
  unless the process sets one of `RELAYHISTORY_SELFHOST_INGEST`, `_READ_API` or
  `_PIPELINE_CONSUMER` to `true`, and on Lambda. Writes are serialized per
  process only, so run one session-ingest replica (the only Locker writer), and
  the ledger stays in `LOCKER_FS_ROOT`: `INGEST_LEDGER_BUCKET` is refused.
  `azure` takes `LOCKER_AZURE_ACCOUNT`, `LOCKER_AZURE_ACCOUNT_KEY` (secret
  store), the container in `LOCKER_BUCKET` and an optional `LOCKER_ENDPOINT`
  (https, or http to loopback only). With either, drop the `locker-init` Job
  (`src/locker/init.ts` signs S3 only). Session ingest, the read API and the
  pipeline consumer work; the publisher, operator erase, the restamp tool, the
  vendored pipeline routes and stage workers, the hoopdb indexer and alleyoop
  refuse them at startup.
  Sessions an older consumer projected with 1970-01-01 event times (a Claude
  record with `ts_ms` 0) are marked for re-projection by
  `src/session-pipeline/epoch-event-time-repair.ts` (plan by default,
  `--apply --self-host-consumer` to mark); the consumer's freshness check then
  re-projects them. **Self-host only: do not `--apply` on the hosted AWS Lambda
  stack**, whose consumer has no freshness sweep, so marked sessions would keep
  their workspaces' directory event reads refused until a new upload. See
  `runbooks/epoch-event-time-repair.md`.

### `SERVICE=session-analysis`

The session-analysis job processor (lane F1) as a pod, for EKS next to ingest-edge, in place of the
session-pipeline Lambda's `{"sessionAnalysis": true}` schedule. It runs the Lambda's exact path:
both build their deps with `src/session-analysis/runtime-deps.ts`. It is off unless
`SESSION_ANALYSIS_ENABLED` is exactly `true`. Dormant, it reads only `WORKER_HEALTH_PORT` and its probes answer 200
`{"ok":true,"enabled":false}`. The env contract, the database role, the S3 and KMS grants (with
the us-east-1 bucket and us-west-2 cluster note), the chart values infra needs and the activation
checklist are in [session-analysis-runner.md](session-analysis-runner.md).

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

### Local single-user control (selfhost-control)

For one developer running the pipeline locally with no relaycontrol.
`SERVICE=selfhost-control` (`src/selfhost-control/server.ts`) answers only
relaycontrol's introspection contract (`POST /internal/v1/auth/introspect`,
`packages/pipeline/src/auth/relaycontrol.ts`) for exactly one user, plus
`GET /healthz` (200 `{"ok":true}`). It never starts unless `SERVICE=selfhost-control`
AND `RELAYHISTORY_SELFHOST_CONTROL` is exactly `true`; otherwise, or on any bad
setting, it prints one stderr line naming the variable (never a value) and exits 2.

- `SELFHOST_CONTROL_INTROSPECT_SECRET`: the bearer the read API presents (its
  `RELAYCONTROL_INTROSPECT_SECRET`); at least 32 printable ASCII characters, no spaces.
- `SELFHOST_CONTROL_TOKENS_FILE`: absolute path to a JSON file, read once at startup:
  `{"sub": "...", "org": "...", "workspace": "...", "tokens": [{"token": "sh_at_...", "token_id": "...", "scopes": ["rth:read", "rth:sync"]}]}`.
  Each token is at least 32 characters and starts with `sh_at_` (the only
  bearers the read API and session-ingest introspect); unknown fields refuse.
  At most 64 KiB, owned by the uid the container runs as (the services image
  runs as uid 1000, `stacks/local/topology.ts`), mode `0600`: any group or other
  permission refuses startup. On Kubernetes a Secret volume defaults to `0644`
  (group-readable too with `fsGroup`), so mount it with `defaultMode: 0400` or
  `0600`, or copy it into place in an init step.
- `SELFHOST_CONTROL_LISTEN`: `host:port`, default `0.0.0.0:8090` (it runs in a
  container; publish it to the read API's network only, never publicly).

Replies. Active token: the stub's fields (`principal_type` `desktop-user`,
`email` and `machine_id` null, `aud` echoed, `iat` = now - 10 s, `exp` = now + 1 h
computed per request, because the client refuses an expired `exp`). `aud` is
echoed unchecked, so on this single-user install a token is valid for any
audience. The tokens never expire here: remove one from the file and restart to
revoke it. Unknown token: 200 `{"active":false,"reason":"unknown"}`; a
`workspace` other than the user's: 200
`{"active":false,"reason":"workspace_mismatch"}`; wrong or missing bearer: 401
`{}`; body over 16 KiB or not the contract shape: 400.

What works. Read API reads and session-ingest sync/upload work. AlleyOop's
mutating routes (POST/PUT/PATCH/DELETE under `/v1/*`, for example
`/v1/pipeline/chunks`, `/v1/pipeline/manifests`, `/v1/pipeline/events`) are
refused with 403 `machine_claim_refused` (the gate at `src/alleyoop/routes.ts:43`),
because `machine_id` is always null and no machine claim is ever set.

Wiring. The URL is `http://<name>.<ns>.svc:8090` (a network alias such as the
e2e's `control.e2e.svc`, with `ALLOW_IN_CLUSTER_HTTP=true`; plain http otherwise
only to loopback), and the secret is `SELFHOST_CONTROL_INTROSPECT_SECRET`.

- Read API: `READ_API_AUTH_MODE=relaycontrol_only`,
  `RELAYHISTORY_ACCEPT_RELAYCONTROL_TOKENS=true`, `RELAYCONTROL_INTROSPECT_URL`,
  `RELAYCONTROL_INTROSPECT_SECRET` and `READ_API_ORIGIN` (sent as `aud`; in
  `relaycontrol_only` startup refuses without it, and in the default mode every
  relaycontrol token is refused 401 `not_configured`).
- Session-ingest (`RELAYHISTORY_SELFHOST_INGEST=true`): `RELAYCONTROL_INTROSPECT_URL`,
  `RELAYCONTROL_INTROSPECT_SECRET` and `RELAYHISTORY_INGEST_ORIGIN` (sent as
  `aud`), all required at startup (`src/session-ingest/selfhost-config.ts`).

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
every request (unless `RELAYHISTORY_SELFHOST_INTROSPECT_POSITIVE_TTL_MS`, default off, reuses an
admit for up to that many ms, at most 60 s and never past the token's `exp`; see
`docs/local-stack.md` "Cache and revocation lag") and requires the granted
`rth:sync` scope. No read-api setting is
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
- Capture policy (relay-control `docs/capture-policy.md`): relaycontrol
  introspection (v1.5) answers the organization's capture policy and the
  caller's live role with every verified bearer; the edge keeps no copy
  (`src/session-ingest/capture-policy.ts`). With `user_retroactive_delete:
  false`, a member's own delete (selection `enabled:false`; reset, where served)
  is refused 403 `policy_forbids_user_delete` before any fence is written.
  Organization owners and admins still delete, and the operator session erase
  below is unaffected. A relaycontrol that predates v1.5 sends no policy, and
  deletes stay as before.
  Uploads (`/v1/delivery/batches`, `/v1/turns/batches`): Agent Relay Desktop
  names the policy revision it applies in `X-Agent-Relay-Capture-Policy-Revision`.
  When the policy sets any key, a missing or older revision, or (under
  `sharing_mode: new`) a `session` record whose `first_activity_ms` is before
  the policy's `sharing_mode_since`, is logged with
  `metric: capture_policy_upload_violation`.
  An All-only policy, optionally accompanied by `user_retroactive_delete`, does
  not require a revision header: it places no restriction on upload content,
  and older clients omit that header. Additional upload settings (including
  an oops window) still require the current revision. Delete authorization is
  unchanged. This compatibility rule is not a Selected allowlist or proof of
  per-user upload consent: with `INGEST_UPLOAD_CONSENT=enforce`, an upload
  without the person's confirmed choice still holds before storage.
  `RELAYHISTORY_SELFHOST_CAPTURE_POLICY_UPLOADS=report` (default) accepts it;
  `enforce` refuses it 409 `capture_policy_stale` (Desktop retries and sends
  the new revision after its next policy read) or 422
  `capture_policy_session_predates_policy`. Roll out in report, watch the log
  for violations from current Desktops, then switch to enforce. Direct-to-S3
  uploads are gated at Control's credentials route
  (`SELFHOST_CAPTURE_POLICY_UPLOADS`), not here.
- Operator session erase: `/internal/v1/session-erase/...`
  (`docs/operator-session-erase-contract.md`) is reachable only from inside the
  cluster, for relay-control. The wrapper forwards GET and POST (other methods: 405) to the app without
  relaycontrol introspection; the app checks its own bearer,
  `SESSION_ERASE_API_SECRET`. It is default off (`SESSION_ERASE_API_ENABLED`),
  and today it always answers 501 `not_implemented`, even when enabled.

At startup it admits only a real login of `bulls96_session_ingest` with the
bootstrap's role shape.

`INGEST_CLAIM_LOCK_BUSY` (default off; parsed as on the Worker: only `on`, any
case, trimmed, enables it, and any other value is off) makes a claim that loses
its lock wait (55P03/40P01) a 429 `delivery_busy` with `Retry-After` 5-10 s
instead of a 503, as on the hosted Worker. 57014 and every other cause stay 503.
The chart turns it on with the managed value `ingestEdge.claimLockBusy: true`
(AgentWorkforce/infra#187), which emits exactly one quoted
`INGEST_CLAIM_LOCK_BUSY="on"` and omits the variable when false; the chart
rejects `ingestEdge.extraEnv`.

`SELFHOST_INGEST_ADMISSION=async` (default `sync`) moves the database off the
upload request path: one create-only inbox PUT, then the hosted queued receipt,
with an in-process consumer that admits the inbox. It also serves
`GET /v1/delivery/admission` (rule 4 already routes it here), and `/readyz`
follows the consumer instead of the database. Every upload still makes one
relaycontrol introspection call (unless `RELAYHISTORY_SELFHOST_INTROSPECT_POSITIVE_TTL_MS` is set). See `docs/selfhost-async-admission.md`. The original sketch:

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
anything else fails startup. `SEARCH_INDEX_STREAMING_ENABLED` keeps that grammar but
defaults on: unset or empty is on (off with `RELAYHISTORY_SELFHOST_OUTPUT_FACTS=true`), and only
`false` turns it off (section "Streaming reconcile (Phase 2)"). One exception: `SESSION_ERASE_API_ENABLED` shares
its value with the hosted Worker, so it is exactly `1` (unset, empty or `0` is
off; anything else, `true` included, fails startup).

| SERVICE | Flag | Required when enabled | Secret material (from the secret store, never on disk) |
| --- | --- | --- | --- |
| `session-read-api` | `RELAYHISTORY_SELFHOST_READ_API` | `READ_API_ORIGIN`, `READ_API_COMPLETE_SINCE` (ISO or `epoch`), `READ_API_DIRECTORY_WORKSPACES` (canary allowlist; login answers 501 for an unlisted workspace), `SEARCH_INDEX_BACKEND` (`postgres`), `SEARCH_INDEX_POSTGRES_BACKEND_ID`, `SESSION_POSTGRES_TLS_PROFILE`, `READ_API_COORDINATION_OVERLAPS_ENABLED` (exactly `true`) with `READ_API_COORDINATION_OVERLAPS_BACKEND=postgres`, optional `READ_API_LIST_SHORT_SUMMARY_ENABLED` (exactly `true` serves the opt-in `includeShortSummary=recorded` on `GET /v1/sessions`; default off, see section 6), the relaycontrol trio in section 2, optional `ALLOW_IN_CLUSTER_HTTP`, `RELAYHISTORY_READ_API_PORT`, `READ_API_CANONICAL_PIN_LAGGING_POINTER` (recommended `true` with `SELFHOST_INGEST_ADMISSION=async`; default off; any value other than `true` is off, see `docs/selfhost-async-admission.md`), `READ_API_SESSION_QUERY` (exactly `true` enables the Dashboard session query `q` on the list and activity routes over the directory, migration 046010; default off, off is unchanged 501 `param_q`; `""`/`false` are off, any other value fails startup; `path:`, `upload:` and `is:on-relay` stay explicit 501s, see `docs/local-stack.md`), `READ_API_CANONICAL_SCOPE_BYTES` / `READ_API_CANONICAL_SCOPE_EVENTS` (canonical snapshot budget; defaults 32 MiB / 100000 (raise bytes only with the pod memory limit), bytes accepted 16 MiB..2 GiB, events up to 4000000; out of range fails startup, see `docs/local-stack.md`), `READ_API_CANONICAL_DEADLINE_MS` (canonical read deadline in ms; default 3000, accepted 3000..60000; out of range fails startup), `READ_API_CANONICAL_FETCH_CONCURRENCY` (chunk objects fetched at once per canonical read, results still processed in manifest order; default 1 = sequential, accepted 1..16, recommended 8 on GCS or any store with more than about 20 ms per-object latency; malformed or out of range fails startup; never read by the hosted Worker/Lambda, see `docs/local-stack.md`), `READ_API_SEARCH_PARTIAL_SESSIONS` (`refuse` default, `skip`; see below), `READ_API_COORDINATION_ADVISORIES_ENABLED` (exactly `true` serves `GET /v1/coordination/advisories` and its ack, migration `062000`; default off, off is 501 `unsupported_by_backend` with reason `advisories_disabled`; see `docs/coordination-live-advisories.md`) | `RELAYHISTORY_READ_API_SECRET` (JSON `{supabase_url}`; no `clickhouse` with `postgres`), `RELAYCONTROL_INTROSPECT_SECRET` |
| `coordination-advisor` | `COORDINATION_ADVISOR_ENABLED` (exactly `true`; default off) | `READ_API_DIRECTORY_WORKSPACES` (the same canary allowlist as the read API; unset or empty scans no workspace), `COORDINATION_ADVISOR_FACTS_BACKEND` (`clickhouse` default, `postgres` needs migration `062070`), optional `COORDINATION_ADVISOR_INTERVAL_SECONDS` (default `120`), `COORDINATION_ADVISOR_PAIR_COOLDOWN_SECONDS` (default `21600`, per urgency), `COORDINATION_ADVISOR_SESSION_HOURLY_CAP` (default `3`), `COORDINATION_ADVISOR_RELAY_DM_ENABLED` (exactly `true`; default off) with `COORDINATION_ADVISOR_RELAY_URL` (https, http only on loopback), `WORKER_HEALTH_PORT`; cooldown and cap survive a restart only with `062070` applied (see `docs/coordination-live-advisories.md`) | `SESSION_PIPELINE_SECRET_JSON` (with `clickhouse`, only for the ClickHouse backend), `COORDINATION_ADVISOR_RELAY_TOKEN` (only read when the DM flag is on) |
| `session-publication` | `INGEST_PUBLISH_ENABLED` | `INGEST_PUBLISH_{BUCKET,EXPECT_GENERATION,AWS_ACCOUNT_ID,SUPABASE_PROJECT_REF,INSTANCE_ID,SECRET_ARN,KMS_KEY_ARN,INSTANCE_PROFILE_ARN,ROLE_NAME}`, `AWS_REGION=us-east-1`, `HOOPDB_DB_MODE=supabase`, optional `INGEST_PUBLISH_BUDGET_MS`, `INGEST_PUBLISH_BATCH_LIMIT`, `WORKER_HEALTH_PORT` | none in env: IMDSv2 plus Secrets Manager (see the caveat in section 1) |
| `session-ingest` | `RELAYHISTORY_SELFHOST_INGEST` | `RELAYHISTORY_INGEST_ORIGIN`, `RELAYCONTROL_INTROSPECT_URL`, `RELAYHISTORY_INGEST_DATABASE_URL`, `SESSION_POSTGRES_TLS_PROFILE`, `LOCKER_BUCKET` (+ Locker endpoint/region or Pod Identity/GCS), optional `RELAYHISTORY_INGEST_PORT`, `INGEST_KEY_ROOT`, `INGEST_MAX_PER_USER`, `INGEST_MAX_PER_ORG`, `RELAYHISTORY_INGEST_PG_POOL_MAX`, `ALLOW_IN_CLUSTER_HTTP`, `SELFHOST_INGEST_ADMISSION` (`sync` default, `async`, `drain`) with `SELFHOST_ADMISSION_CONCURRENCY` / `_SCAN_MS` / `_MAX_ATTEMPTS` / `_AWAIT_AUTHORITY` (`on`; default off), `SESSION_ERASE_API_ENABLED` (`1`; default off), `RELAYHISTORY_SELFHOST_SESSION_SELECTION`, `RELAYHISTORY_SELFHOST_HEARTBEAT_ACCEPT` (both `true`; default off), `RELAYHISTORY_SELFHOST_CAPTURE_POLICY_UPLOADS` (`report` default, `enforce`; the capture policy upload check, section 2), `RELAYHISTORY_SELFHOST_INTROSPECT_POSITIVE_TTL_MS` (positive introspection cache in ms, `0`-`60000`; default `0` = off; revocation lag = the TTL), `INGEST_CLAIM_LOCK_BUSY` (`on`; default off; a claim lock wait is a 429 with Retry-After 5-10 s, section 3), `INGEST_LEDGER` (`sql` default, `s3`; requires the fence-authority marker, section 5 "session-ingest upload ledger"), `INGEST_S3_TENANT_LIMIT` (`on`; default off; needs `s3`) with `INGEST_ASYNC_MAX_PER_ORG` (section 5), `INGEST_LEDGER_BUCKET` (+ `INGEST_LEDGER_REGION`, default `LOCKER_REGION`, and `INGEST_LEDGER_ENDPOINT`, default `LOCKER_ENDPOINT` only in the same region; default unset = the ledger stays in `LOCKER_BUCKET`; needs `s3`), `INGEST_LEDGER_HOME_GUARD` (`on`; default off; needs `s3`; [ledger-relocation.md](ledger-relocation.md)), `INGEST_S3_ERASE_FENCE` (`on`; default off; needs `s3`; section 5 "session-ingest upload ledger"), `INGEST_LEDGER_LOG_COMMIT` (`on`; default off; needs `s3`; non-production only, section 5), `INGEST_LEDGER_LOG_ERASE` (`on`; default off; needs `s3` and `INGEST_LEDGER_LOG_COMMIT=on`; section 5), `INGEST_SEAL_WORKERS` (`0`-`32`; default `0` = off; the raw-chunk seal (scrub, zstd, sha256) on that many worker threads, capped at the CPUs available; byte-identical output, refused with `SCRUB_ENGINE=wasm-bindgen`; `src/session-ingest/seal-pool.ts`) | `RELAYCONTROL_INTROSPECT_SECRET`, `RELAYHISTORY_INGEST_DATABASE_URL` (password), Locker keys, `SESSION_ERASE_API_SECRET` (+ `_PREVIOUS` during a rotation; at least 32 characters; only read when the erase flag is on) |
| `session-erase-bytes` (Job) | `SESSION_ERASE_BYTES_ENABLED` (exactly `1`; see below) | `SESSION_ERASE_S3_BUCKET`, `SESSION_ERASE_S3_REGION`, `SESSION_ERASE_REQUEST_FILE`, optional `SESSION_ERASE_S3_ENDPOINT` (bare origin; https, loopback, or a Service name with `ALLOW_IN_CLUSTER_HTTP`), `SESSION_ERASE_KEY_ROOT` (default `96bulls`, must equal `INGEST_KEY_ROOT`), `SESSION_ERASE_DRY_RUN` (`1` = list and count only), `SESSION_ERASE_CONCURRENCY` (1..16, default 4) | `SESSION_ERASE_S3_ACCESS_KEY_ID[_FILE]`, `SESSION_ERASE_S3_SECRET_ACCESS_KEY[_FILE]`: a separate eraser key (ListBucketVersions + DeleteObjectVersion on the key root), not the ingest writer's key |
| `s3-ledger-rearm-sweeper` (Deployment) | `INGEST_REARM_SWEEPER_ENABLED` (exactly `true`) | `INGEST_REARM_BUCKET`, `INGEST_REARM_REGION`, optional `INGEST_REARM_ENDPOINT` (bare origin), `INGEST_REARM_KEY_ROOT` (default `96bulls`, must equal `INGEST_KEY_ROOT`), `INGEST_REARM_ORGS` (comma list; unset = every org), `INGEST_REARM_DRY_RUN` (`1` = classify and count only), `INGEST_REARM_CONCURRENCY` (1..16, default 4), `INGEST_REARM_MAX_MARKERS` (default 500 per pass), `INGEST_REARM_INTERVAL_MS` (default 600000), `INGEST_REARM_CREDENTIALS=keys`, `INGEST_REARM_LEDGER_BUCKET` (+ `INGEST_REARM_LEDGER_REGION`; must equal the edge's `INGEST_LEDGER_BUCKET`: markers, completion records and stubs are read there, fences stay in `INGEST_REARM_BUCKET`; [ledger-relocation.md](ledger-relocation.md)) | `INGEST_REARM_S3_ACCESS_KEY_ID[_FILE]`, `INGEST_REARM_S3_SECRET_ACCESS_KEY[_FILE]`: ListBucket on the bucket (unconditioned: a conditioned one turns a missing-object HEAD into a 403, which aborts the pass `access_denied`), Get on `.admission-inbox/*/ledger/rearm/*`, `*/ledger/owners/*`, `*/upload-fences.json`, `.ledger/rearm-sweeper/*` and the canonical `v1/ws=*/m=srv_*/h=*/d=*/s=*/manifest.json` (deriving an unstamped stub's machine), Put on `ledger/rearm/*.done/*`, `ledger/rearm/*.corrupt/*` and `.ledger/rearm-sweeper/*` (its durable cursor), ListBucketVersions + DeleteObjectVersion on the canonical layout. See `src/session-ingest/rearm-sweeper.ts` |
| workers | per service (`docs/local-stack.md`) | `HOOPDB_URL`, `WORKER_HEALTH_PORT`, optional `HOOPDB_POOL_MAX` (shared pool size per replica; default `8`, `1`-`64`, anything else is refused at startup; lower it when a pooler caps clients per role) | `HOOPDB_URL` password |

Installs with bigger sessions must raise the read API container memory limit and
`READ_API_CANONICAL_SCOPE_BYTES` together, because a raw scope is held several times over per request. The shipped
pairing is the 32 MiB default against the charts' 512Mi read API limit (umbrella chart 1Gi), so keep roughly that
ratio when raising the budget (for example 64 MiB needs at least 1Gi, and a 1.5Gi limit is the pairing used for a
64 MiB dev budget). The hosted Worker budget stays 16 MiB and is not affected.

A bigger budget also needs a longer read deadline: by default the read API fetches a session's chunks one at a
time, in manifest order, so the first read of a ~70 MiB session takes on the order of 10-25 s and is otherwise 501
`*_canonical_deadline` at the 3000 ms default. Set `READ_API_CANONICAL_DEADLINE_MS` (3000..60000, self-host service
only) and keep it below the ingress/proxy read timeout (stock ingress-nginx is 60 s), or the proxy cuts the request
first. On an object store with per-object latency (GCS, remote S3), also set `READ_API_CANONICAL_FETCH_CONCURRENCY`
(1..16, default 1; 8 is recommended) so up to that many chunks are fetched at once while they are still processed
in manifest order; it holds up to that many chunk bodies (each at most 16 MiB stored) at once, so keep the pod
memory limit in step. The session-brief worker's transcript source reads the same variable through
`serviceCanonicalDeps` (`src/session-brief/canonical-input.ts`), where a bad value fails the first brief rather than
startup, like the other `READ_API_CANONICAL_*` settings. The hosted Worker/Lambda deadline stays 3000 ms and their chunk reads stay sequential.

A session too large for one search index pass is marked partial (`scope_too_large`), and while any session of a
workspace is partial its workspace search answers 501 `session_partial`; a self-host install has no relayhistory to
fall back to. Two settings, both default off and both self-host service only, address that. On the
`session-pipeline-consumer`, `SEARCH_INDEX_MAX_SCOPE_BYTES` (64 MiB..256 MiB, `MiB`/`GiB` suffix accepted),
`SEARCH_INDEX_MAX_SCOPE_EVENTS` (100000..1000000) and `SEARCH_INDEX_MAX_ROWS` (500000..1000000) raise the per-scope
index limits from their defaults (the lower bound) up to the replay bound. Raise the consumer's memory limit with
them, because the scope is held several times over. A raised limit needs no manual re-index: every reconcile
tick the consumer re-sends each session the search state lists as partial, `scope_too_large` ones included, so after
a restart with a higher limit those sessions are indexed again under it. Out of
range or malformed fails startup before any connection opens, naming the variable only, and a dormant consumer reads
none of them. On `session-read-api`, `READ_API_SEARCH_PARTIAL_SESSIONS=skip` lets every page of a workspace
search answer 200 anyway, with `incomplete` (`session_partial` plus the partial `sessions`, or
`session_partial_unbounded` with no list) and the `X-Sessions-Read-Incomplete` header. Each page, cursor pages
included, checks again and leaves out the listed sessions' rows, so a session indexed while small and partial
since never serves stale rows. `session_partial_unbounded` lists nothing and excludes nothing, so its pages may
still hold stale rows. A search backend that cannot exclude sessions refuses `skip` at startup. On the Postgres or
OpenSearch search path a session is partial while its index state is not complete and current: its first index pass
has not run yet, it is too large for the limits (`scope_too_large`), or a new upload moved its directory epoch and the
re-index has not run yet. So with `skip` a session that is being written to drops out of search results until its
next index pass. A session-scoped read of a partial session still answers 501
`session_partial`. The hosted pipeline Lambda never reads the limits, and the hosted read Lambda refuses `skip`.
`SEARCH_INDEX_STREAMING_ENABLED` (default on, except with `RELAYHISTORY_SELFHOST_OUTPUT_FACTS=true`) indexes such a
session instead (see "Streaming reconcile (Phase 2)" below). `SEARCH_INDEX_STAGED_RECONCILE_ENABLED` (default
off; exactly `true` turns it on) pages the index scan and bounds each streamed pass on its own (see "Staged
reconcile (Phase 3), slice 1" below).

`SESSION_ERASE_BYTES_ENABLED` is the one exception to the `true` rule above:
the operator-erase contract fixes the literal `1`. `true` is not accepted; it
fails startup like any other unknown value, so it is never silently off.

`RELAYHISTORY_SELFHOST_OUTPUT_FACTS` (pipeline consumer,
`SERVICE=session-pipeline-consumer`, next to `SEARCH_INDEX_BACKEND` and
`RELAYHISTORY_SELFHOST_PIPELINE_CONSUMER`; documented in
`src/session-pipeline/consumer-config.ts`) is an optional behaviour flag like
`READ_API_CANONICAL_PIN_LAGGING_POINTER`: exactly `true` enables it; unset or
any other value is off (default off), never a startup error. When on, every
search-index pass also builds the session's output facts with the shared
builder and writes them through the Postgres facts store
(`sessions.session_event_facts`, `sessions.session_output_facts` and
`sessions.output_facts_session_state`, migration `045040`) inside the same
transaction as the directory events; a pass older than the stored source
version is a no-op, and the erase path deletes the session's facts. Default
off; sessions ingested before the flag have no facts (bootstrap is a later PR).
Sessions erased while the flag is off keep any facts written earlier.
Migration `045040` grants the four writers to `search_consumer_runtime` whatever
the flag says, so turning the flag on needs no extra grants. The consumer's live
sessions-function gate (`SESSIONS_FUNCTION_GATE_SQL`) accepts those writers only
when it runs as `search_consumer_runtime` (`CONSUMER_RUNTIME_FUNCTIONS` in
`src/hoopdb/migrate-guards.ts`); any other role that can execute them, or
PUBLIC, still closes the gate.

`READ_API_OUTPUT_BACKEND`, `READ_API_DIGEST_BACKEND`, and
`READ_API_COORDINATION_OVERLAPS_BACKEND` (`session-read-api`)
select where `GET /v1/sessions/output`, `GET /v1/digest/daily`, and
`GET /v1/coordination/overlaps` read from:
unset, empty or `clickhouse` (the default; the routes stay exactly as before,
which on the service means not configured, since it has no ClickHouse) or
`postgres`. Any other value fails startup, naming the flag and never echoing
the value. `postgres` is accepted only by the self-host service; the hosted
Lambda refuses it at startup, and the hosted read Worker does not read these
flags. Each route is still served only with its own `READ_API_OUTPUT_ENABLED`
/ `READ_API_DIGEST_ENABLED` set to exactly `1` (unchanged semantics: anything
else answers 501 `output_not_configured` / `digest_not_configured`), and only
for workspaces in `READ_API_DIRECTORY_WORKSPACES` (unchanged allowlist).
`GET /v1/coordination/overlaps` is separately exposed only when
`READ_API_COORDINATION_OVERLAPS_ENABLED` is exactly `true`; every other value
returns `404 {"error":"coordination_overlaps_disabled"}`. Self-hosted installs
select the existing Postgres facts with
`READ_API_COORDINATION_OVERLAPS_BACKEND=postgres`; this adds no worker, model,
or secret. It requires migration `20261105047000_coordination_fact_readers.sql`,
which adds two filtered SECURITY DEFINER readers and two partial indexes over
the existing facts tables. On a populated database, prebuild those exact
indexes with `CREATE INDEX CONCURRENTLY` using the packet in
`docs/coordination-overlap-detection.md`; the transactional migrator refuses a
blocking build unless `HOOPDB_ALLOW_BLOCKING_INDEX_BUILD=1` (or `true`) is set.
Its default `outcome-facts` detector ranks duplicate
migrations, cross-lane reverted commits, and rare different-branch shared
writes using only those stored facts; the pure detector package retains
`shared-write-60m` as a named comparison detector. With `postgres` the routes read the Postgres output facts on the read pool, as
`bulls96_read_api`, through the migration `045040` readiness/usage readers and
the `047000` sparse coordination readers, in one
READ ONLY tenant transaction, with erased sessions dropped through the session
liveness check before any aggregation. No ClickHouse block or client is needed.

Serving from `postgres` needs, in order:

1. migration `045040` applied (otherwise 501 `output_schema_missing` /
   `digest_schema_missing`) and, for coordination overlaps, migration `047000`
   applied (otherwise the overlap route returns 200 with unknown coverage and a
   schema-unavailable note);
2. `RELAYHISTORY_SELFHOST_OUTPUT_FACTS=true` on the pipeline consumer, so new
   passes write facts;
3. the output facts bootstrap (a later PR), which backfills the sessions
   ingested before the flag and only then writes the workspace watermark
   (`sessions.output_facts_workspace_state.complete_since`). Until the
   watermark exists every window answers 501 `output_backfill_incomplete` /
   `digest_backfill_incomplete`; a window starting before it answers
   `*_before_watermark`.

Readiness keeps the ClickHouse codes and order (`backfill_incomplete`,
`before_watermark` with a microsecond compare, `session_partial`,
`session_unindexed`, then ready). `session_unindexed` is narrower than on
ClickHouse: it fires when the window's event facts name more sessions than
the state rows overlapping the window. ClickHouse compares against raw
transcript events; `045040` exposes no per-session state to the read role,
and a session never processed with the facts flag on has no facts row at all.
That gap is what the bootstrap closes before it writes the watermark. The
count also treats every state row as one session, so a session recorded under
alias ids can mask another session's missing state row; a later reader (for
example `sessions.output_facts_unindexed_for`) can make the check exact.
Output and the digest read every output fact of the workspace (commit
candidates are unwindowed, as on ClickHouse) and aggregate in the service, so
the read cost grows with the workspace's history; the deadline bounds it. A read pages the facts (at most 10000 rows a page) under a 3 second
deadline checked before each page; past it the route answers 503
`backend_unavailable`, never a partial answer. A Postgres permission error is
also a 503. The digest narrative on the postgres digest backend comes from the
self-host narrative deps (section 6, "Daily digest narrative on the read API
service"): the cache (`sessions.digest_narrative_read` through the read role)
always, and in-process generation only when the model, key and writer URL are
all set. Without them a cache hit whose stats hash matches is served and a miss
answers `narrative.unavailable = provider_not_configured`; a cache error is
`provider_error`, always within a 200.
Defaults are unchanged: with neither flag set nothing new is wired.

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
reads these settings.

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
  in docs/local-stack.md, with its own reason codes (`staged_run_leased`, `staged_run_interrupted`,
  `staged_lease_lost`, `output_facts_stage_incomplete`).

What it does not change yet: at the default settings (`SEARCH_INDEX_MAX_ROWS` = the cap = 500000) the paged scan
reads no more index rows than the bounded scan did; only the reason code past that point changes. A
`SEARCH_INDEX_MAX_ROWS` set between 500000 and 1000000 keeps its higher bound. The flag never lowers it (the cap is
a floor, so a `SEARCH_INDEX_MAX_ROWS` below 500000, as tests set it, is raised to the cap). A session above 500000
records read per pass stays incomplete until a later slice removes the cap; what slice 1 adds today is the per-pass
byte budget (scopes up to 2 GiB under 500000 records) and the paged scan itself.

Turning the default on is a separate, gated step, not part of this slice.

### session-ingest admission mode (config contract)

`SELFHOST_INGEST_ADMISSION` is `sync` (the default; unset or empty is the same),
`async` or `drain`. Any other value fails startup. The knobs
`SELFHOST_ADMISSION_CONCURRENCY` (1..32, default 4), `SELFHOST_ADMISSION_SCAN_MS`
(1000..600000, default 15000), `SELFHOST_ADMISSION_MAX_ATTEMPTS` (1..100,
default 20) and `SELFHOST_ADMISSION_AWAIT_AUTHORITY` (`on`/`off`, default off; a
missing fence-authority marker or the deployment-wide stop then defers instead of
counting toward the dead letter, `INGEST_LEDGER=s3` only) and `SELFHOST_ADMISSION_SPREAD`
(`on`/`off`, default off; with more than one replica, each orders work younger than
`SELFHOST_ADMISSION_SPREAD_GRACE_MS` by a per-process salted hash of its machine directory so replicas
stop racing on the same oldest entries) and `SELFHOST_ADMISSION_LEARN_USERS` (`on`/`off`,
default off; with the tenant limit, scan-found inbox keys are counted per user, learned from each machine
directory's first admission, instead of per machine directory) and `SELFHOST_ADMISSION_ONE_PER_RECEIPT`
(`on`/`off`, default off; never two attempts of one batch in flight at once, so a re-send never supersedes the
attempt already writing) are read only in `async` and `drain`. The mode is never taken from a
request. Semantics: `docs/selfhost-async-admission.md`.

Sizing with `INGEST_LEDGER=s3`: one admission of a 100-record batch holds a few MB (the batch, its
chunk, and ~100 revision-index shards of ~10 KB each) and makes ~300 small S3 requests, 32 at a time
(`SERVER_LEDGER_PARALLEL`). Concurrency 16 is safe on a 512Mi ingest-edge (~5 MB per admission
above the process baseline). 1536Mi has room for 32, but for one user's uploads throughput stops
rising at about 16: their batches race on the same revision-index shards (CAS retries), so more only
helps when many users upload at once. Each
admission logs its per-phase wall time (`admission_read_ms`, `admission_claim_ms`,
`admission_gate_ms`, `admission_objects_ms`, `admission_commit_ms`, `admission_finish_ms`).

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
| `<root>/.admission-inbox` | Lists prefix `<root>/.admission-inbox` (covered by the unconditioned ListBucket, below) |
| the inbox object | GetObject; PutObject **unconditional overwrite** with a small tombstone after the terminal outcome. Never DeleteObject. |
| `…/r=<receipt>/outcome-admitted.json`, `outcome-refused.json` | PutObject overwrite |
| `J` (the post-claim privacy gate) | GetObject. PutObject with `If-Match: <etag>` (or `If-None-Match: *` on a missing journal) only to complete a control left pending for more than 120 s. Self-host serves no journaled control today, so this write does not happen in practice, but grant it. |
| `<root>/.admission-dlq/<inbox path minus .ndjson.zst>.json` | Lists prefix `<root>/.admission-dlq` (covered by the unconditioned ListBucket, below); PutObject overwrite (the dead-letter marker); DeleteObject (best effort after a later admission; also the operator's redrive) |
| tenant data `<root>/<org>/…` | exactly the synchronous path's set (chunks, manifests, pointers: GetObject, HeadObject, conditional PutObject). Canonical acceptance stages nothing, so there is no `buffered-batches` stage prefix. |

*Startup gate (whichever role starts with `async` or `drain`; once per process start):*

| Key | Operations |
| --- | --- |
| `<root>/.admission-probe/v1/ws=probe/m=srv_<random>/probe.json` | PutObject create-only; a second create that must be refused (with the HeadObject reconcile above); PutObject `If-Match` twice (the stale one must be refused); GetObject; DeleteObject best effort. If DeleteObject is denied, one ~60-byte object per start remains and startup still succeeds. Any other failure refuses startup. |

*Operator (not a service role):* DeleteObject on `<root>/.admission-dlq/…` to redrive.

Backend notes:

- **S3 / MinIO.** Grant `s3:ListBucket` on the bucket with **no condition**. S3
  answers a GET or HEAD of a missing key with 404 only when the caller may
  `s3:ListBucket` for that very request, and such a request carries no
  `s3:prefix`, so a prefix-conditioned List never applies there: a missing key
  reads as 403. A missing privacy journal (the normal case) would then read as
  an error, and every upload would be a 503. (The same List also covers the two
  listings above.)
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

### session-ingest upload ledger (`INGEST_LEDGER`)

`INGEST_LEDGER` chooses where upload accounting lives (decision 0005,
`src/session-ingest/ledger-s3.ts`):

- **Unset, empty or `sql` (the default).** Today's SQL ledger (`ledger-sql.ts`), unchanged.
- **`s3`.** Every admission path (the synchronous HTTP path, the async consumer and direct-upload promotion) claims and commits through the S3 upload ledger under the per-user admission directory, with no SQL. Any other value fails startup.

What `s3` requires and changes:

- **It refuses at startup** the features whose state is SQL-only:
  - `RELAYHISTORY_SELFHOST_SESSION_SELECTION`;
  - the operator session erase runtime (`SESSION_ERASE_API_ENABLED` and the erase fence and bytes settings),
    except as `INGEST_S3_ERASE_FENCE=on` allows below;
  - `INGEST_UPLOAD_CONSENT` other than `off`.
- **`INGEST_S3_ERASE_FENCE`** (`on`; default off; needs `s3`; issue #613, `src/session-ingest/s3-erase-fence.ts`) lifts
  only the operator **per-session** erase refusal (`SESSION_ERASE_API_ENABLED` with `SESSION_ERASE_FENCE_ENFORCED=1`).
  The erase route writes create-only tombstones under `<root>/.ledger/session-erase/v1/` (the canonical session and
  its layout pair, in `LOCKER_BUCKET`) after the SQL fence and before it answers; a store failure is a 503 that
  Control retries. Every admission path (synchronous, async consumer, direct-upload promotion) HEADs the tombstones
  after the claim and before any write and refuses 409 `operator_session_erased`; a store error fails closed (503).
  An erase refused `too_many_owners` (the owner cap; the SQL fence is kept) writes the tombstones too.
  Set it on **every** ingest process on the same store, the async consumers and promotion too: they have no SQL erase
  fence, and a process started without the flag does not read the tombstones, so it fails open. Each `s3` process
  without the flag logs `S3 erase fence off: operator erase tombstones are not checked by this process` at startup;
  enforcing the flag across processes is a follow-up. Still refused with `s3`: `SESSION_ERASE_BYTES_ENABLED`,
  session selection, upload consent and whole-user erase (follow-ups); `on` also refuses
  `INGEST_LEDGER_LOG_COMMIT=on` (log appends bypass the check) and `INGEST_LEDGER=sql`. Not covered: the hosted
  upload-admission Lambda (`upload-admission-hosted.ts`). Tombstones are never deleted (no
  un-erase). Residual: a claim in another process that passed its tombstone check just before the write can still
  land that batch, the same kind of cross-process obligation as the SQL fence's; alias session ids only SQL knows
  are fenced by the SQL guard in the erase process only. Off: no guard, no write, bytes unchanged.
- **`INGEST_LEDGER_LOG_COMMIT`** (`on`; default off; needs `s3`;
  [design/s3-ledger-log-commit.md](design/s3-ledger-log-commit.md), `src/session-ingest/ledger-log.ts`) admits a
  stream that an operator switched to log mode (`scripts/ledger-log-stream.ts switch`, non-production buckets only)
  through its append-only log, and runs the log materializer; every other stream stays on the legacy claim and
  commit. The switch and each process's first write to a stream also create the stream's index object under
  `…/m=srv_<user>/ledger/log-index/` (create-if-absent, no id inside), which the reset erase below lists. So the
  ingest process and the operator need PutObject (and GetObject) under `…/ledger/log-index/` as well as `…/ledger/log/`,
  with the bucket key's GenerateDataKey for that prefix; a grant on `…/ledger/log/*` alone refuses the first write.
  Exactly `on` or `off`; anything else refuses startup. Refuses `INGEST_S3_ERASE_FENCE=on`. Not cleared for
  production: the hosted runtime and the stack input refuse it.
- **`INGEST_LEDGER_LOG_ERASE`** (`on`; default off; needs `s3` and `INGEST_LEDGER_LOG_COMMIT=on`, refused at startup
  otherwise; `src/session-ingest/ledger-log-erase.ts`) makes every reset through the S3 ledger delete the user's
  unapplied log payloads that reset superseded before it completes or answers. A failure fails the reset (503,
  retryable, the reset stays in flight) and logs `ledger_log_erase_failed`; the design doc's slice 8 has the
  runbook. Cost per reset: a LIST of the user's stream index, a LIST of each stream's remaining payloads, and only
  for a stream that still holds some, O(log n) marker HEADs, its unapplied entries, and a DELETE plus a HEAD per
  superseded payload. Lists prefix `…/m=srv_<user>/ledger/log/` and `…/ledger/log-index/` (both covered by the unconditioned
  ListBucket of the S3 / MinIO note above; it only lists the index, whose writes are LOG_COMMIT's, above), and needs GetObject and
  DeleteObject under `…/m=srv_<user>/ledger/log/`. Before turning it
  on, run `bun scripts/ledger-log-stream.ts backfill-index --bucket <b> --apply --kms-key-arn <arn>
  --i-know-this-is-not-prod` once (dry run first, without `--apply`): a stream switched before the index existed and
  not taken since is otherwise invisible to the reset's erase (the design doc's runbook "before turning the erase on",
  after slice 8; startup does not check it). Set it on every ingest process of the deployment: the materializer answers a batch whose
  payload a reset erased as `delivery_privacy_superseded` only where the flag is on (`payload_erased` elsewhere).
  Off: a reset touches nothing in the log, bytes unchanged.
- **No admission for an org until a fence-authority marker covers it.** Until then, every claim for that org is a retry (in the async consumer with `SELFHOST_ADMISSION_AWAIT_AUTHORITY=on`, a deferral that never dead-letters; see `docs/selfhost-async-admission.md`). On the synchronous path that means its uploads answer 503; nothing of that org is admitted on any path. There are two forms:
  - **Per org (use this first):** `<root>/.ledger/fence-authority/<org>.json` covers that one org. Orgs are turned on one at a time, each once its own users' state is backfilled. It holds the fields below plus `"orgId":"<org>"`, which must equal the org in the key. A per-org marker that is present but invalid, or that names another org, refuses that org even if the deployment-wide marker exists.
  - **Deployment-wide:** `<root>/.ledger/fence-authority.json` covers every org at once. It is for **after the full backfill** of every org, and it holds exactly the fields below, with no `orgId`.
  - The fields are exactly `{"v":1,"fenceBackfill":"complete","revisionBackfill":"complete","ownershipBackfill":"complete","controlMirror":"on","rearmSweeper":"live","revisionLayout":"4096","workerHttpAdmission":"off","writtenAtMs":<ms>}`.
  - An operator writes a marker only once all of these hold, for its org or for every org:
    1. Nothing else admits these users through a SQL ledger any more (`workerHttpAdmission: "off"`).
    2. The one-time backfills have copied SQL's deletions and resets into each user's fence (`fenceBackfill`), its stored revisions into the index (`revisionBackfill`), and its session ownership into owner stubs (`ownershipBackfill`). They must run after step 1, so no SQL-only state appears after them.
    3. Controls write the S3 fence before SQL (`controlMirror`; on self-host the ledger does this itself).
    4. The eraser sweeps the S3 re-arm markers (`rearmSweeper`).
    5. The revision index is in the 4096-shard layout (`revisionLayout: "4096"`): `scripts/ledger-reshard-revisions.ts --org <org>` ran (dry run, then `--execute --kms-key-arn <arn>`), and then `--verify` reported `verify_passed: true`, or the listing found no 64-shard (`r=<2 hex>.json`) shard. A marker written before this field existed is now invalid, so that org's admissions retry until the operator rewrites it with the field after the verify. `INGEST_LEDGER_PARALLEL` (1 to 64, default 32) sets how many index reads and merges one admission runs at once; anything else refuses startup.
  - **Stopping the Worker's HTTP admission for an org** (hosted; the `workerHttpAdmission: "off"` precondition). The Cloudflare Worker admits through the SQL ledger, and the desktop falls back to its POST on any direct-upload credential error, so moving an org to direct uploads does not stop it on its own. Set `INGEST_HTTP_ADMISSION_STOPPED_ORGS` (Cloudflare stack input `sessionIngest.httpAdmissionStoppedOrgs`, a list of org ids; omitted = none, unchanged):
    - A listed org's `POST /v1/delivery/batches`, `/v1/turns/batches` and `/v1/delivery/recovery` answer 503 `delivery_unavailable` with `Retry-After: 300` before the body is parsed or any ledger call. For delivery batches that is a retry: the desktop keeps the batch and retries, later through the direct path. The route logs `stage: http_admission`, `error_code: http_admission_stopped`; the answer never names the org.
    - **Turns and recovery have no direct path.** `/v1/turns/batches` is relayhistory-cloud's server-side forward (`docs/relay-session-turns.md`), and `/v1/delivery/recovery` is an operator replay. For a listed org they are refused for as long as it stays listed, which is permanent once its marker exists. With `TURNS_WRITE_PATH=dual` the ingest copy of its turns is silently missing; with `forward` every turn write for the org fails (503 `turns_unavailable`). **List an org only while relayhistory's `TURNS_WRITE_PATH` is `neon` (unset) for it**, and do not move it to `dual` or `forward` while it is listed, until turns have an S3-native path.
    - Controls (selection, reset, consent, custody), reads and onboarding are unchanged, so `controlMirror` keeps working for the org.
    - The Worker's `INGEST_ADMISSION` consumer is **not** stopped: a batch it holds was already ACKed to the desktop and must still be admitted. No new batch reaches it for a stopped org, because the HTTP entry refuses before the enqueue.
    - A malformed list (an empty entry, a character outside `[A-Za-z0-9._-]`, a leading dot, over 128 characters) refuses Worker startup: every request is 503 `not_configured`, and the `INGEST_ADMISSION` consumer stops too (its messages retry and, after about 100 minutes, reach the dead-letter queue, which must not be replayed for a stopped org). It is never read as empty. Set the list through the stack, which validates it before deploy.
    - Ids are matched **exactly and case-sensitively** against the org id in the signed identity. A well-formed wrong id (a typo, other casing, a workspace id) passes every validator and stops nothing, which is why step (2b) below checks the effect.
    - **Order, per org:**
      1. Confirm the org's turns write path is `neon` (above), then deploy the Worker with the org listed.
      2. Wait until the `INGEST_ADMISSION` queue is empty and no Worker version without the list still serves traffic (gradual deployments finished). Do not replay the admission dead-letter queue for this org from then on: a replay would admit through SQL.
      2b. **Verify the stop took effect.** The Worker's logs show `error_code: http_admission_stopped` lines for the org's traffic, and SQL shows no new admission for the org since the deploy: no `sessions.ingest_batches` row (and no `ingest_record_revisions` or `ingest_session_ownership` change) for its users created after the deploy time. Also confirm nothing else writes its SQL revisions: the buffered publisher (`ingest_publication_*`) and the canonical fan-out are not running for it (prod is buffered, and 0005's drain replaces the publisher). If anything is still landing, stop here and fix the id.
      3. Run the org's fence, revision and ownership backfills.
      4. Write its marker.

      Removing the org from the list before its marker is deleted would let SQL admit it again and break the marker's guarantee. The stop covers this Worker only: the Node ingest edge (`ingest-edge.ts`) does not take it and is not mounted today; if it is ever mounted over the SQL ledger, it must take the stop too.
  - Deleting a marker stops **new** claims for the orgs it covered within 60 s (the cache is per org). A batch already claimed still commits. Errors carry codes only (`ledger_fence_authority_missing`, `_invalid`, `_org_mismatch`), never an org id.
  - **Stopping admission (kill switch).**
    - **To stop every org in one step,** write `<keyRoot>/.ledger/fence-authority/.stop.json`, for example `{"v":1,"stop":"all","writtenAtMs":<ms>}`.
      - While it is present (valid or not), every claim of every org is refused (`ledger_fence_authority_stopped`), whatever the per-org and deployment-wide markers say.
      - It is checked before them. Delete it to re-admit.
      - The buffered drain (`buffered-drain.ts`) honours it too: while it is present, `--execute` writes nothing of any org (`fence_authority_stopped`). It re-reads it with the markers, at most every 60 s, and once more before each batch's first canonical byte. The dry run still classifies.
      - Its name starts with a dot, which no org id may, so it can never collide with an org's marker.
    - **To stop one org,** write an *invalid* marker at `<keyRoot>/.ledger/fence-authority/<org>.json` (for example `{}`). An invalid per-org marker refuses that org even while the deployment-wide marker is valid. Deleting the org's marker is **not** enough while a valid deployment-wide marker exists: a missing per-org marker falls back to it.
    - **The async consumer and these states.** An invalid per-org marker (the one-org stop) is an ordinary retry: its queued objects dead-letter after `SELFHOST_ADMISSION_MAX_ATTEMPTS` (about 80 min) and are re-attempted after 14 days. With `SELFHOST_ADMISSION_AWAIT_AUTHORITY=on`, a missing marker and the deployment-wide stop are deferrals that never dead-letter (`docs/selfhost-async-admission.md`); an invalid marker is deliberately not one.
    - Either way, already-claimed batches still commit, and each warm instance stops within its 60 s cache.
- **Controls also write SQL.** Selection, reset and ordered controls write the S3 fence first, then reconcile SQL `upload_deletions` (and write SQL's reset record) to the fence's final state. The read API and the eraser still read SQL. A failure on either side fails the control (503), and its retry is idempotent. This is deletion state for readers, never upload accounting: claims and commits never call SQL.
- **A reset is completed only by its own retry** on self-host: a claim never completes a reset left running, so the SQL half is never skipped. A client that abandons a reset leaves it running until a retry with the same `request_id`.
- **The conditional-write startup probe** (the `.admission-probe` row above) runs in every admission mode, including `sync`.
- **Object store.** Besides the rows above, the Locker credential needs GetObject and conditional PutObject (`If-None-Match: *` and `If-Match`) under `…/m=srv_<user>/ledger/` (`batches/`, `revisions/`, `owners/`, `rearm/`) and on `…/upload-fences.json`. A reset also lists `…/ledger/owners/`. Grant ListBucket on the bucket with no condition: it covers that listing, and a GET of a missing ledger key (a revision shard or owner stub not written yet) reads as 404 only with an unconditioned List (a GET carries no `s3:prefix`), never with a prefix-conditioned one. The markers need GetObject on `<root>/.ledger/fence-authority.json` and `<root>/.ledger/fence-authority/*`. Nothing in the S3 ledger is ever deleted.
- **A separate ledger bucket (`INGEST_LEDGER_BUCKET`, default unset).** The ledger's own objects (each user's `ledger/`: batch records, revision shards, ownership stubs, re-arm markers, log streams; and the ledger-home marker `<root>/.ledger/fence-authority/.ledger-home.json`) can live in a bucket in the edge's own region while everything else, the fences (`upload-fences.json`, which the hosted Worker's fence mirror writes) included, stays in `LOCKER_BUCKET` (`src/session-ingest/ledger-store.ts`). Unset changes nothing. `INGEST_LEDGER_HOME_GUARD=on` (default off) adds one GET, cached with the fence-authority marker (60 s per org), under the prefix the edge already reads: the ledger-home marker, which, when it names another bucket, refuses claims (`ledger_home_moved`). Moving an existing ledger is an offline, scripted step: [ledger-relocation.md](ledger-relocation.md).
- **Tenant limits (`INGEST_S3_TENANT_LIMIT`, default off).** There is no slot table and no SQL on the upload path. With the flag unset, empty or `off`, `INGEST_MAX_PER_USER` and `INGEST_MAX_PER_ORG` bound nothing while the ledger is `s3`, exactly as before. Exactly `on` turns on an in-memory limit (`tenant-limiter.ts`), and any other value refuses startup. `on` needs `INGEST_LEDGER=s3`.
  - **The caps are per replica.** Each ingest-edge process counts only its own in-flight admissions, so with N replicas one tenant can have up to N times each cap in flight.
  - **Proving two replicas.** `scripts/proof-ingest-edge-multireplica.ts` drives a retry storm, two origins per user and a mid-burst reset against a dev or stage edge scaled to two replicas with halved caps, then asserts exactly-once admission from the S3 ledger. `scripts/proof-ingest-edge-multireplica-local.ts` runs the same proof against two local ingest-edge processes on one MinIO, with no cloud and no credentials: [runbooks/ingest-edge-multireplica-proof.md](../runbooks/ingest-edge-multireplica-proof.md).
  - **Synchronous path** (`POST /v1/delivery/batches` in `sync` and `drain`): per user `INGEST_MAX_PER_USER` (default 4) and per org `INGEST_MAX_PER_ORG` (default 32), the same variables and defaults as the SQL lease. At the cap the answer is the SQL slot refusal: 429 `delivery_busy` with a jittered `Retry-After` of 2 to 10 s. The batch's claim stays `pending`, the desktop keeps the batch and retries, and the retry resumes the same claim.
  - **Async consumer** (HTTP-queued uploads, presigned direct uploads and the inbox bridge): its own pool, separate from the synchronous one. The per-user cap is `INGEST_MAX_PER_USER`. The per-org cap is `INGEST_ASYNC_MAX_PER_ORG`, an integer from 1 to `SELFHOST_ADMISSION_CONCURRENCY`. Unset, it defaults to `max(1, floor(concurrency / 2))`; any other value refuses startup, and so does setting it without the flag or without `async` / `drain` admission. One org therefore never holds more than half the consumer, and another org always gets slots. At the cap the object is deferred, never refused or dropped. It stays in the inbox and is retried after the same `Retry-After`. A deferral does not count toward `SELFHOST_ADMISSION_MAX_ATTEMPTS`, so a busy tenant is never dead-lettered. The log line is `ingest.admission` with `outcome: "deferred"`, `error_code: "tenant_limit"`.
  - **Recommended hosted values:** `SELFHOST_ADMISSION_CONCURRENCY=8` with `INGEST_ASYNC_MAX_PER_ORG=4` (the default for 8), and the default `INGEST_MAX_PER_USER=4`.
  - **Release and TTL.** A slot is released when its admission finishes, on every path, including errors. Each slot also expires 90 s after it was taken (the SQL lease's TTL). That frees a slot whose admission never finishes, but it also frees one whose admission is still running after 90 s, so a very slow admission can briefly let the tenant exceed its cap.

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

A session refused as `rate_limited` waits for the next UTC midnight, but in
this mode the worker re-checks parked sessions once a minute. It re-arms a
parked wakeup when the session gets new canonical revisions. It also makes a
parked wakeup due as soon as the admission checks would pass, for example after
a cap is raised mid-day; the global daily cap is checked first. The sweep treats
a `rate_limited` brief as unbriefed, so a session whose parked wakeup expired
after `SESSION_WAKE_MAX_AGE_HOURS` is found again. None of this changes a cap
or a default, and claim still enforces every limit.

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

### Summary flags on the read API (both default off)

Two read API flags expose stored summaries. Each is on only when its value is
exactly `true`; unset or any other value is off and leaves responses unchanged
for requests that do not send the opt-in.
Neither changes SQL, needs a new migration, or adds a credential. Turning
either on for the hosted stack is a deploy decision and needs the owner's
explicit approval; nothing in this repository sets them.

- `READ_API_BRIEF_SUMMARY_ENABLED`: the Node self-host service always wires the
  brief-summary read above. The hosted Lambda entry wires it only with this
  flag (`GET /v1/sessions/:id/summary`, and `GET /v1/sessions/summaries/activity`,
  which reads the same dependency). Off, both answer 501
  `summary_not_configured`, as before. It reads
  `sessions.session_brief_summary_for` from
  `20261105044100_session_brief_summary_read.sql`, which must be applied before
  the flag is turned on; the activity route still answers 501
  `summary_activity_not_migrated` until `20261105056000` is applied.
- `READ_API_LIST_SHORT_SUMMARY_ENABLED` (either entry): `GET /v1/sessions`
  accepts the opt-in `includeShortSummary=recorded`. With it, each directory
  row gains `shortSummary` (string or `null`, at most 1000 characters, the
  producer's `char_length` bound) and `shortSummaryGeneratedAt` (ISO 8601 or
  `null`). The value is the recorded directory short summary as stored: no
  title substitution and no fallback. A stored value over the bound is a
  row out of contract, handled like an over-long title (a first page answers
  501 `directory_row_out_of_contract`, a cursor page 503). Any other param
  value is a 400. With the flag off the param answers 501
  `param_includeShortSummary`. The hosted read Worker
  (`packages/session-read-worker`) does not read either flag, so there the
  summary routes and the param keep their 501s.

Without the param the list rows are byte-identical to today's, so Cloud's
strict `@relayhistory/cloud-client` 0.2.0 `sessionSummary` parse is
unaffected: the two keys appear only when a client asks for them.

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
turns (with sampling on, a spread to the last), the tool-call histogram,
models, time span), every string scrubbed
first; the model's output is scrubbed before it is stored. Logs carry the model
id, token counts from the Responses usage block, attempt count and codes only:
never prompts, outputs, request or response bodies, provider error text or the
key.

### Bob Costas input caps

Bob Costas streams a session's chunks into the digest one at a time and folds
delivery envelopes as they arrive (`src/bob-costas/session-input.ts`, whose
header is the reference), so it never holds more than one chunk of the
session at a time. With `BOB_COSTAS_INPUT_SAMPLING` off (the default) every
chunk is read and the digest, and so every summary and prompt, is exactly what
the whole-session read produced; memory then still grows with the session's
user-turn text and its number of envelope records, though not with its bytes
or chunk count. With sampling on it is bounded whatever the session's size.

With sampling on, a transcript session past the caps is summarised from a
sample of its chunks (the first and the last, then an evenly spaced selection
between), user turns are sampled and clipped, and tool names, models and
envelope text are capped. A session of delivery envelopes is always read
whole, so every revision and tombstone is applied.

When chunks are skipped or names dropped, the counts are lower bounds: the
summary object carries `input_truncated: true`, the long summary says so, and
the `bob costas summary` log line adds `reason: input_truncated` with
`chunks_read`, `chunks_total`, `bytes_read` and `names_dropped`. When only the
user turns were sampled the counts are exact: the object carries
`turns_sampled: true` and the log line `turns_dropped`. A capped read that
finds no record, or an envelope session with an unreadable chunk, never
publishes `empty` or keeps an older summary: the summary is cleared
(`quarantined`, so nothing deleted or revised since stays visible) and the
delivery fails (non-retryable). The row is recorded as not current, so a
replay of the same revision reads the session again.

Lowering a cap with sampling on (`BOB_COSTAS_MAX_CHUNK_BYTES`,
`BOB_COSTAS_MAX_INPUT_BYTES`) can therefore clear summaries that were ready:
the next summary of a session whose chunks no longer fit is cleared and
dead-lettered. Nothing retries it on its own (the failure is non-retryable, and
a new summary is only queued by a new revision), so recovery is manual: raise
the cap again, then replay those deliveries from the summary dead-letter queue.

| Variable | Default | Meaning |
| --- | --- | --- |
| `BOB_COSTAS_INPUT_SAMPLING` | `off` | `on` turns on chunk, turn and name sampling as above |
| `BOB_COSTAS_MAX_CHUNKS` | 512 | chunks read per summary (sampling on) |
| `BOB_COSTAS_MAX_INPUT_BYTES` | 268435456 (256 MiB) | decompressed chunk bytes read per summary (sampling on); never below the per-chunk cap |
| `BOB_COSTAS_MAX_CHUNK_BYTES` | 33554432 (32 MiB) | per chunk, decompressed, always; a bigger chunk is never inflated. With sampling off, never below 24 MiB (the ingest writes at most 16 MiB) |

The defaults fit a 4 GiB container: the worker handles one message at a time,
and a summary's peak is one inflated chunk plus the digest's state. A value
that is not a whole number in the cap's range keeps the default.

### Summaries regeneration flags (all default off)

Read by Bob Costas and the session-brief worker (`src/summary-regen/settings.ts`).
A flag is on only when its value is exactly `true`. The brief columns and the
audit table come from `20261105062040_summary_regeneration`, the Bob Costas
columns from hoopdb `0006_summary_regeneration`:
with a flag on and the columns or the audit table missing, the worker logs
`summary regeneration schema missing; flags behave as off` once and keeps
today's behaviour. On AWS they come from `transcriptStore.sessionPipeline.summaryRegen`
(`turns`, `retry`, `audit`), rendered only when set.

| Variable | Default | Effect |
| --- | --- | --- |
| `SUMMARY_REGEN_TURNS_ENABLED` | off | regenerate a current summary or brief when `SUMMARY_REGEN_TURN_THRESHOLD` (10) more user turns arrived, or when the session is idle for `SUMMARY_REGEN_IDLE_SECONDS` (600) or ended with any uncovered turn; a brief below the threshold on an active session is deferred to the idle window |
| `SUMMARY_RETRY_BACKOFF_ENABLED` | off | a model fallback (Bob Costas: still extractive, marked degraded) or a final brief failure is retried after `SUMMARY_RETRY_BACKOFF_SECONDS` (`300,3600,21600`); attempts stop at the list length |
| `SUMMARY_RETRY_CONFIG_RESET_ENABLED` | off | a degraded or failed row written under a different model configuration (model, base URL, effort, prompt version, key fingerprint) is due again once, attempts restart at 1. Turning it on revives failed rows written before the migration once, at most 100 per sweep (per stage for Bob Costas) and within the usual budgets |
| `SUMMARY_REGEN_AUDIT_ENABLED` | off | one `summary_regeneration_audit` row and one `summary regeneration queued` log line (session id hashed) per enqueue; rows older than `SUMMARY_REGEN_AUDIT_RETENTION_DAYS` (90) are purged hourly |

For Bob Costas the turn threshold only labels `regeneration_reason`
(`turns_gained`): its sweep queues a summary that is behind on revision and
whose session is idle or ended, and a new revision always rebuilds the summary,
as with the flag off. `covered_turns` counts every user turn read; when an input
cap fired (`input_truncated`) it is a lower bound.

Out-of-bounds tuning values fall back to the defaults in code (the AWS stack
refuses them). The config hash never contains the key: only a 16-hex
HMAC-SHA256 fingerprint of it.

Rolling out `20261105062040_summary_regeneration`:

- **Re-apply the function boundary bootstrap first.** Run
  `supabase/bootstrap/hoopdb_sessions_function_boundary_supabase.sql` again
  **before** this migration, the same pattern as
  `20261105062000_coordination_advisories`. Its embedded manifest and function
  lists name the migration's four functions, so the installed event-trigger
  function's source (and its sha) changes; with the old bootstrap installed the
  function boundary refuses the step.
- **The partial index briefly blocks writes.** The `next_retry_at` index on
  `sessions.session_briefs` starts empty but is built non-concurrently (the
  migrator is transactional): `CREATE INDEX` holds a SHARE lock and blocks
  writes to `session_briefs` while it scans the table. Apply it in a quiet
  window on a large table.
- **The first turns-flag-on rollout stamps, it does not regenerate.** With
  `SUMMARY_REGEN_TURNS_ENABLED` first turned on, a Bob Costas summary written
  before the flag (covered turns unknown) that is current by revision is
  stamped with its present turn count, without a model call, audit row or
  `summary regeneration queued` line (the sweep logs
  `summary coverage stamp queued` with a count). Only turns that arrive after
  the stamp count towards the threshold; a summary behind on revision
  regenerates. A legacy brief is only regenerated by a new upload's wakeup, as
  with the flag off, and that write stamps it.
- **A stamp job can dead-letter.** With the turns flag on, a current legacy Bob
  Costas row runs the scrub-marker check and a capped read to be stamped, so a
  scrub ruleset newer than the reader or a persistent store error dead-letters
  its `:regen-stamp` job, and the DLQ guard then stops the missing-summary and
  regeneration sweeps for that session and stage until the dead letter is
  resolved. A stamp read that comes back quarantined stamps 0, leaves the
  summary in place and logs (warn)
  `summary coverage stamp read found no digest; stamping 0, summary left in place`.
- **A refused or gone regeneration is held.** When the brief sweep arms a
  `config_changed` or `retry_backoff` row it sets `next_retry_at` to the
  longest backoff step (at least 300 s). An attempt that runs replaces it, so the
  hold only bounds a session that was gone or wrote nothing: it is re-armed and
  re-audited at most once per hold. A config change waits for a pending hold or
  backoff to pass.

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

These deps reach `GET /v1/digest/daily` only through the Postgres digest
backend (`READ_API_DIGEST_BACKEND=postgres`, section 5). With the default
(`clickhouse`) the service has no digest backend and the route answers 501
`digest_not_configured` whatever these variables say.

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

## 7. Output facts bootstrap

`SERVICE=session-pipeline-output-facts-bootstrap`
(`src/session-pipeline/output-facts-bootstrap.ts`) is a one-shot Job that backfills
output facts for sessions ingested before `RELAYHISTORY_SELFHOST_OUTPUT_FACTS` was
turned on, then records each fully covered workspace in
`sessions.output_facts_workspace_state`. The operator steps are in
`runbooks/selfhost-output-facts-bootstrap.md`.

- **Gate:** the consumer's env (`RELAYHISTORY_SELFHOST_PIPELINE_CONSUMER=true`,
  `RELAYHISTORY_PIPELINE_CONSUMER_SECRET`, `SEARCH_INDEX_BACKEND`,
  `SEARCH_INDEX_POSTGRES_BACKEND_ID`, `PIPELINE_KEY_PREFIX`,
  `SESSION_POSTGRES_TLS_PROFILE`), with `RELAYHISTORY_SELFHOST_OUTPUT_FACTS`
  exactly `true`. The secret's `hoopdb_url` must be the **owner** identity, the
  same one `session-pipeline-consumer-bootstrap` uses, not the consumer's runtime
  role. The ledger must record migration `045040` with this image's checksum.
- **Phase A (reprojection):** this phase writes no facts itself. It queues the manifest
  of every live session that owes facts into `sessions.search_consumer_pending`, the
  running consumer's retry queue. The consumer (`SERVICE=session-pipeline-consumer`,
  with the facts flag on) then reprojects each one through the normal search-index
  pass and the `045040` writers. The job polls until nothing is owed, the queue for
  its backend is empty, or
  `RELAYHISTORY_SELFHOST_OUTPUT_FACTS_BOOTSTRAP_DEADLINE_SECONDS` runs out
  (default 7200, range 1..86400).
- **Phase B (watermark):** in one transaction under an advisory lock, every
  workspace whose live sessions are all complete and current gets
  `complete_since` = the earliest `min_ts` of those sessions, or `now()` when it has
  none, with `marked_by = 'bootstrap'`. An existing row is never changed, so a
  watermark is never lowered. An incomplete workspace is never marked.
- **Exit codes:** 0 done; 1 failed; 2 refused (gate, ledger or identity); 3 some
  workspaces are still incomplete. Set
  `RELAYHISTORY_SELFHOST_OUTPUT_FACTS_BOOTSTRAP_ALLOW_PARTIAL=true` to turn 3 into
  0. The marks are the same either way.

Nothing runs the Job by default. The e2e compose does not include it.
