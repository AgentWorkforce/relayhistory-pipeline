# 96Bulls session read API: design (2026-11-01)

Inputs: 96bulls-read-api-step1.md, search-dimension-coverage.md and neon-readers-inventory.md. Code read:
- 96BullSessions `origin/main` 2213dde and PR #121 at 93008de.
- relayhistory-cloud main c9026d5 and PR #242 at 82088b2.
- Cloud trunk e3a0995ef.

## 0. Scope and phasing

The `/v1` read contract is wide. Several routes need data that no 96Bulls store holds yet. So the API ships in phases, and each route is switched on its own Cloud flag.

| phase | routes (relayhistory contract) | Cloud callers | stores | blocked on |
|---|---|---|---|---|
| **A (this PR)** | `GET /v1/sessions/:id/events`; `GET /v1/events` with `session` (in-session, `q` / `since` / `until` / `kind`); `GET /v1/events` with `q` (workspace search) | MCP `search_shared_sessions`, `get_shared_session`, `get_shared_session_context`, `list_team_activity` / canvas agents, Skip session events | ClickHouse `transcript_events`; Supabase `session_index` (liveness, erasure, history floor) | #121 applied + backfill complete; read role applied |
| B | dashboard replay, session overview, reflex events proxy (`includeMessageId`, model, usage, toolCalls, durationMs) | dashboard, desktop, iPad (through Cloud) | ClickHouse `event_json` column/table written by the pipeline (the #242 `transcript_session_events` shape) | pipeline schema change + backfill |
| C | `GET /v1/sessions` (plain list + filters), `/sessions/activity`, `/:id/catalog`, `/:id/timing` | dashboard list/activity, canvas, Skip list, MCP `list_session_timings` | Supabase `list_session_directory` / `session_directory_activity` (#124), timing projection (does not exist) | #124 merged + applied; claim-bound role binding (section 3) |
| D | `search=hybrid`, `q` on lists/activity | dashboard search, public API | ClickHouse session-grouped search + Supabase directory keyword | gap 1 and gap 3 in the coverage audit. **Self-host `q` (2026-11-09, #473):** behind `READ_API_SESSION_QUERY=true` (default off; off is unchanged, 501 `param_q`), `GET /v1/sessions` and `/v1/sessions/activity` accept the Dashboard's local session query, evaluated on the directory by `sessions.query_session_directory_for` / `query_session_directory_activity_for` (migration 046010). Free words match `search_text` only (title, task title, agent name, project id, git repo/branch, model, scrubbed short summary; never transcript content); a free word of 1 or 2 meaningful characters (`Fix a bug`) is an extra AND / NOT predicate in the same WHERE, accepted only beside an included free word of 3 or more (the trigram index's anchor), else 400, enforced by the parser and by `sessions.session_directory_query_check`; `title:`, `project:`, `harness:`/`source:`, `agent:`/`@`, `id:`, `is:active`/`is:inactive` and last-activity dates map to directory columns. `path:`, `upload:`, `is:on-relay`/`is:off-relay`, negated dates and `q` with owner-buffered reads are explicit 501s (`q_filter_*`, `q_with_buffered`); only `search=metadata` is accepted, with `q`. Pages use a `b96q1:` cursor bound to a fingerprint of the query and filters. Grammar: `src/session-read-api/list-query.ts`. `search=hybrid` stays 501. |

Phase A gives agents (MCP) and Skip a Neon-free path first. It is the hottest content read, and its fields fit `transcript_events` exactly: Cloud's `EventSchema` (`lib/mcp/shared-sessions.ts:113`) is non-strict and reads only eventId, sessionId, source, kind, type, ts, userId, actor*, subagentId, projectId, machineId, taskTitle, content, contentTruncated, toolName and toolStatus.

Anything Phase A cannot answer exactly gets **`501 unsupported_by_backend`** with a `reason`, and Cloud falls back to relayhistory. The service never sends a best-effort answer.

## 1. Hosting

> **Superseded (2026-11-01): the API moves to a Cloudflare Worker,** `packages/session-read-worker`, so that no Lambda is on the request path. The account's Lambda concurrency limit is 10, and the Lambda runs without reserved concurrency. The Worker runs the same `src/session-read-api` code. Supabase is reached through Hyperdrive; the reasons are in `sessions-pipeline-cloud:hosted-ops/preserved/runbooks/sessions-prod-session-read-worker.md`. The Lambda below stays deployed only until the Worker is `on` for every route.

> **Self-host SERVICE (2026-11-07).** The API is no longer Lambda/Worker only: the self-host services image runs the same `src/session-read-api` app as `SERVICE=session-read-api` (`src/session-read-api/server.ts`, config in `service-config.ts`). It is default-off. `RELAYHISTORY_SELFHOST_READ_API` unset, empty or `false` is dormant: the process listens, `GET /healthz` answers 200 `{"ok":true,"enabled":false}` and every other route answers 503 `not_configured`. Exactly `true` enables; any other value fails startup. `RELAYHISTORY_READ_API_PORT` (default 8080) is parsed even when dormant. Enabled, the app is assembled once at startup by the Lambda's own `readAppFromSecret` under the service entry's rules (`entry: "service"`), from `RELAYHISTORY_READ_API_SECRET`. That variable holds the same JSON the Lambda reads from Secrets Manager, injected from the secret store. The rest of the env is the Lambda's: `READ_API_COMPLETE_SINCE`, `READ_API_ORIGIN`, `RELAYAUTH_*`, the relaycontrol switch, `SESSION_POSTGRES_TLS_PROFILE` and `ALLOW_IN_CLUSTER_HTTP` (the transport rule). A bad value fails startup. Search is selected by `SEARCH_INDEX_BACKEND` through the adapter registry, never hard-wired: `postgres` (the service default) composes the registry's Postgres `SearchIndexAdapter` with epoch-fenced Postgres completeness state (`createPostgresSearchComposition`, backend id `SEARCH_INDEX_POSTGRES_BACKEND_ID`, coverage from `READ_API_COMPLETE_SINCE`), so the secret needs no `clickhouse` block and no ClickHouse client is created; `clickhouse` keeps the hosted binding. Any other name, including one with no implementation in this build (`opensearch` today), fails startup with an error naming the variable, never the value. `PIPELINE_RUNTIME` is not this process's concern (any known value). Unlike the Lambda, the service requires `SESSION_POSTGRES_TLS_PROFILE` explicitly (no implicit Supabase pin) and hands pg discrete host/port/database/user/password fields from the sanitized URL, so nothing in its query string reaches pg; a URL without user, password or database fails startup. Directory and liveness stay on the Postgres read role, as on the Lambda. **Postgres search prerequisites.** On a fresh self-host neither search relation exists, and the read service creates no DDL and issues no GRANT. The consumer's one-shot schema bootstrap (`consumer-bootstrap.ts` `runBootstrap`) performs steps 2 and 3; the remaining manual prerequisites are step 1 and provisioning `bulls96_read_api` (`session_read_api_roles.sql`/`session_read_api_role.sql`) before that bootstrap runs. Until the steps below are done, every workspace search answers 503 `backend_unavailable` (a missing relation, 42P01, or a missing grant, 42501; the response names neither): (1) the HoopDb bootstrap has installed `pg_trgm`; (2) a schema owner runs the Postgres index's `ensureSchema` (creates `"<SEARCH_INDEX_POSTGRES_SCHEMA, default search_index>".transcript_events` and its trigram indexes, with forced row-level security and a `tenant_only` policy) and `postgresSearchState(...).ensureSchema(owner)` (creates `projection_state` with the same forced `tenant_only` policy). Both policies admit a row only where `org_id`/`workspace_id` equal `current_setting('bulls96_read.org_id'/'bulls96_read.workspace_id', true)`, for reads and writes, and each `ensureSchema` re-verifies its policy exactly (an extra, rewritten or narrowed policy refuses: `search index: schema_missing (tenant_policy_mismatch)` / `search state tenant policy mismatch`). FORCE binds the owner too: a search writer must run its index statements inside its scope transaction, where the adapter binds each statement's tenant (the writer runtime's `writerTransactionSql`); with no tenant bound every index call throws `unavailable (tenant_unbound)` rather than quietly reading, deleting or erasing nothing, and an erase confirms no scope row is left (`delete_incomplete` otherwise). Under RLS the trigram GIN indexes are not used for search (`LIKE` is not leakproof, so it cannot run before the policy), so search cost is linear in one tenant's size: accepted at self-host scale for now, with a follow-up; (3) the read role (the `supabase_url` user) gets `USAGE` on that schema and `SELECT` on `transcript_events` and `projection_state`, beside its existing `sessions.session_index`/`session_directory` reads. `postgresSearchState(...).ensureSchema(owner)` issues these grants itself when `bulls96_read_api` exists: `USAGE` on the schema (never `CREATE`), `SELECT` on exactly the nine `projection_state` columns its completeness check reads (`backend_id`, `org_id`, `workspace_id`, `source`, `session_id`, `canonical_session_id`, `source_epoch`, `coverage_from_ms`, `complete`), and `SELECT` on `transcript_events`, with no write privilege. Rows stay scoped by the forced `tenant_only` policies. If the role does not exist yet when the bootstrap runs, nothing is granted; re-run the bootstrap after provisioning the role. Without these grants every events read answers 503 `backend_unavailable` (42501). That login must be exactly `bulls96_read_api` (the role the RLS policies name, as `session_read_api_role.sql` provisions it) and must not own the search schema or tables: tenant isolation is the policies, which a superuser, `BYPASSRLS` or otherwise different login skips or changes silently. So the service admits the read pool once at startup (after opening it, before any search, state or directory SQL; `lambda.ts` `assertReaderRole`): `session_user` and `current_user` are `bulls96_read_api` and `current_setting('role')` is `none` (no `SET ROLE` from another login); this backend's `pg_stat_activity.usename` is `bulls96_read_api` on every version (a superuser's `SET SESSION AUTHORIZATION` leaves the three above looking genuine, and on PostgreSQL 15 only `usename` shows the real login); on PostgreSQL 16+ `system_user` is `<method>:bulls96_read_api` (never trust auth); the role is `LOGIN NOINHERIT NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE NOREPLICATION`; it is a member of no role, and no role is a member of it except, on PG16+ with `SESSION_POSTGRES_TLS_PROFILE=supabase` (the Lambda's default), Supabase's creator edge (member `postgres`, grantor superuser `supabase_admin`, `ADMIN` without `INHERIT` or `SET`); `rolconfig` holds `statement_timeout=2s` and `default_transaction_read_only=on` (the verifier's rule; other entries allowed) and the live session shows exactly those values. Any mismatch refuses startup with `session read api: reader_role_<property>` (`unknown`, `session_user`, `current_user`, `set_role`, `backend_user`, `system_user`, `superuser`, `bypassrls`, `attributes`, `membership`, `members`, `settings`), naming the property only, never an observed role or the URL. The hosted ClickHouse Lambda and the Worker do not run the check. Startup therefore needs the database reachable. The service binds the caller's tenant in a read-only transaction for each index query, as it does for completeness, directory and liveness reads; (4) a search writer records projection state under the same `SEARCH_INDEX_POSTGRES_BACKEND_ID` with `coverage_from_ms` equal to `READ_API_COMPLETE_SINCE`. Before step 4, a workspace with indexed sessions but no matching state rows answers 501 `session_partial` (or `session_partial_unbounded`) rather than incomplete results. `/healthz` answers `{"ok":true,"enabled":true}`, and the read API routes are GET only, as on the Lambda and the Worker, with one exception: `POST /v1/cli/login` (exact path; a trailing slash or longer path is 405). Its JSON body is the only request body the service reads. The entry buffers it with an 8 KiB cap that mirrors the app's login `bodyLimit`, and checks both `Content-Length` and the bytes received. Over the cap it answers 413 `body_too_large` without calling the app and closes the connection. Any other non-GET request answers 405 `method_not_allowed` with `Allow: GET`, or `Allow: GET, POST` on the login path. Dormant, `POST /v1/cli/login` is 503 like every other route and its body is never read. SIGTERM stops accepting connections, drains in-flight requests (bounded at 10 s), then closes the read pool. Nothing is deployed by this: no chart, stack or migration changes.

> **Self-host canonical reads and `relaycontrol_only` (2026-11-08).** A self-host runs with no RelayAuth and no relayhistory-cloud; history is served by this SERVICE alone.
>
> - **Canonical routes.** The service entry (`entry: "service"`, never the hosted Lambda) wires `GET /v1/sessions/:id/timing`, `/turns`, `/metadata` and, with `RELAYHISTORY_SELFHOST_READ_API=true`, `/catalog` to the session's canonical S3 objects (`src/session-read-api/service-canonical.ts`). This is the same source the Worker's `timingDeps`/`turnsDeps` use: pointer rows read AS the read role inside the request's tenant-bound READ ONLY transaction, then `canonicalSnapshot`, which rechecks tenant, bucket, provider, digests and the org key prefix. The store is the self-host Locker, under the names every other self-host service reads: `LOCKER_BUCKET`, `LOCKER_PROVIDER` (`s3` default or `gcs`), `LOCKER_ENDPOINT` (s3: MinIO, path-style; gcs: the official API or a loopback emulator), `LOCKER_REGION`, `LOCKER_ACCESS_KEY_ID` / `LOCKER_SECRET_ACCESS_KEY` (s3 static keys, e.g. MinIO, from the secret store; unset: EKS Pod Identity through `AWS_CONTAINER_CREDENTIALS_FULL_URI` and `AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE`, the agent's token going only to the trusted agent hosts, loopback or https), `LOCKER_GCS_TOKEN_URI` (gcs only: the GKE metadata server by default, or loopback), plus `PIPELINE_KEY_PREFIX`, and `READ_API_TIMING_ASSUME_NO_TURNS` as on the Worker. For gcs, both `LOCKER_ENDPOINT` and `LOCKER_GCS_TOKEN_URI` are honoured, so an emulator setup never reaches `storage.googleapis.com` or `169.254.169.254`. The store is built by `src/shared/locker.ts` `buildLockerStore`, the same builder as every other service. One credentials provider (or GCS token source) serves the whole process, cached and refreshed before expiry, and each request's object reads are aborted at the route deadline.
>   - **The org-prefix layout is required.** Manifests carry no org. The only thing that binds an object to the caller's org is the key prefix `<PIPELINE_KEY_PREFIX>/<org>/...`. With a static layout, the prefix check would cover only the shared base, and another org's pointer row naming this org's manifest would be served. So with `LOCKER_BUCKET` set, startup refuses unless `PIPELINE_ORG_FROM_PREFIX` is exactly `true`, as `src/session-pipeline/lambda.ts` does.
>   - `LOCKER_BUCKET` unset or blank: nothing is wired, and the routes keep their 501 `timing_not_configured` / `turns_not_configured` / `catalog_not_configured`.
>   - `LOCKER_BUCKET` set without a usable store (s3 without keys, GCS behind the S3 client): startup fails, naming the variable.
>   - Every 501/404 reason is the Worker's (`*_no_pointers`, `*_canonical_<reason>`, the canary allowlist `READ_API_DIRECTORY_WORKSPACES`).
>   - `LOCKER_ENDPOINT` is not put through `ALLOW_IN_CLUSTER_HTTP`, the same as for the other services that read it.
> - **`READ_API_AUTH_MODE=relaycontrol_only`** (unset or `""` keeps the default; any other value fails startup). Only relaycontrol `sh_at_` tokens are accepted, through the introspection in §2. `readApiVerifier` never constructs the RelayAuth verifier, so no JWKS is fetched, and any other bearer is 401 `invalid_token` before any network call. Startup refuses when `RELAYAUTH_JWKS_URL` or `RELAYAUTH_ISSUER` is set to a non-blank value. It also refuses without `RELAYHISTORY_ACCEPT_RELAYCONTROL_TOKENS=true`, `READ_API_ORIGIN`, `RELAYCONTROL_INTROSPECT_URL` (same transport rule) and `RELAYCONTROL_INTROSPECT_SECRET`. All of these are env checks, made before the read pool opens. The audience stays `READ_API_ORIGIN`. Outages fail closed (503 `relaycontrol_unavailable`, `Retry-After: 1`), and refusals are 401 (403 without `rth:read`). The Lambda reads the same variable through `readApiAuthConfig`, but nothing hosted sets it. The Worker does not read it.
> - **Health.** `GET /healthz` is unauthenticated and unchanged: `{"ok":true,"enabled":true|false}`. It reports liveness and the flag only; it checks neither the Locker nor relaycontrol.
> - Tests: `src/session-read-api/__tests__/server-selfhost-reads.pglite.test.ts` runs real node:http through `server.ts` with the real assembly, against PGlite (every migration plus the read role), an S3-API stub and a relaycontrol introspection stub.

- **Choice: one Lambda (`AWS.Lambda.Function`, `functionUrl: { authType: "NONE" }`) in sessions-prod us-east-1.** It runs nodejs24.x on arm64 with no VPC, a 1024 MB memory size and a 10 s timeout.
  - Auth is handled by the application (section 2). The Function URL's IAM auth can't be used because the caller is a Cloudflare Worker with no AWS identity.
- **Why not API Gateway or ECS:**
  - The sessions-prod stack has no compute, and `assertTranscriptStoreInputs` forbids `eks`.
  - A Function URL is a single resource and needs no gateway, certificate or DNS. Phase A traffic is tens of requests per minute.
  - ClickHouse Cloud and Supabase are public TLS endpoints, so no VPC or NAT is needed.
  - Cold start with a tiny bundle (no AWS SDK; reuses PR #121's SigV4 helper) is about 300 ms. Warm requests take one ClickHouse round trip plus one Supabase round trip, so p95 should stay under 1 s. Shadow mode measures it before any switch.
- **Code layout:**
  - `src/session-read-api/`: a Hono app, with `lambda.ts` as the Function URL adapter and `app.ts` for the routes.
  - `stacks/aws/session-read-api.ts`: the resources.
- **Plan contract:** new phases `session read api rollout` and `session read api update` are added in the PR #121 pattern. That means a resource list, `expectedResources()`, tests and mutation cases.
  - Resources: the function, its secret container (`AWS.SecretsManager.Secret`, value set from 1Password: ClickHouse read user and Supabase read role URL), and an error-rate alarm.
- **Ordering:** this PR stacks on #121, since both change `transcript-store.ts` phases. The lead applies #121 first.

## 2. Auth: the same callers, no Neon

- **Request:** Cloud sends `Authorization: Bearer <RelayAuth assertion JWT>`. Cloud already mints this token for History (`lib/relayhistory-session.ts` `mintRelayhistoryAssertion`), with:
  - `historyOrigin` set to **the read API's own origin**. RelayAuth accepts any HTTPS origin (`relayauth/packages/server/src/routes/tokens.ts:2112`).
  - scope `rth:read`, a 60 s TTL, and the claims `org`, `wks` and `sponsorId`.
- **Verification:** the service verifies the token with 96Bulls' `verifyAgentRelayToken` (`packages/pipeline/src/auth/relayauth.ts`: RS256, JWKS with refetch on an unknown kid, `iss` check).
  - `RELAYHISTORY_ORIGIN` is set to the read API origin, so `aud` must be exactly this service. A token minted for History, or for any other origin, is refused.
  - The legacy shared audience is refused (`RELAYHISTORY_ACCEPT_LEGACY_ASSERTIONS` is unset), and migration assertions are refused.
  - It requires `rth:read` in `scopes`.
- **Tenancy:** org and workspace come **only** from the verified claims. A `workspace` query parameter must equal the `wks` claim, else 403 (the relayhistory rule). Every response carries `X-Relayhistory-Workspace-Id`, which Cloud's `rejectHistoryResponse` already checks.
- **What this avoids:** Cloud's path needs no `/v1/cli/login` exchange (the self-host one in §2a is pass-through and flag-gated), no `rth_at_` token and no `auth_sessions` lookup. So there is no dependency on relayhistory-cloud #245 (auth to Supabase); #245 is still needed for desktop uploads.
  - Coordination note for the Neon-retirement lead: reads stop using `rth_` tokens entirely once their routes are switched.
- **Relaycontrol tokens (dual accept, default off):** with `RELAYHISTORY_ACCEPT_RELAYCONTROL_TOKENS=true`, a `sh_at_` bearer is introspected by relaycontrol (`src/session-read-api/auth.ts` `readApiVerifier`) with `aud` = this origin and the request's single `workspace=` when given (relaycontrol must echo it exactly; without one the response's workspace is authoritative, and the routes still require `workspace=` to equal it); reads send no machine claim; only `rth:read`/`rth:sync` are granted. Every other bearer takes the RelayAuth path above unchanged. Only `principal_type: "desktop-user"` is admitted and `aud` is compared as a normalized origin. Unreachable or misconfigured relaycontrol (timeout, its 400 for a request it refuses, which a caller cannot cause, its 401 for a wrong introspection secret, its 404, 5xx, a redirect, a response over 16 KiB) is a 503 `relaycontrol_unavailable`. Details, cache lag and limits: docs/local-stack.md "Relaycontrol user access tokens".
  - **Revocation lag (`READ_API_INTROSPECT_POSITIVE_TTL_MS`).** An admit is reused for min(token `exp`, this TTL) per process, so a revoked token, or one whose `rth:read` was removed, can keep reading for up to the TTL. Unset is `60000` (60 s, the hosted behaviour, unchanged). Otherwise it must be a plain decimal integer from `0` to `60000`; any other value, `""` included, fails startup (`readApiIntrospectPositiveTtlMs` in `src/session-read-api/lambda.ts`, checked whether or not the flag is on). The value is passed to the introspector explicitly for reads and for `POST /v1/cli/login`, and the process-wide introspector is keyed by it, so it is never inherited from another caller. `0` introspects every request (concurrent identical requests still share one in-flight call), matching the self-host ingest edge's default (no positive cache unless `RELAYHISTORY_SELFHOST_INTROSPECT_POSITIVE_TTL_MS` sets one). **Self-host operators who want strict revocation set `READ_API_INTROSPECT_POSITIVE_TTL_MS=0`.** Refusals keep the client's own negative cache (at most 5 s); this setting never lengthens it. The Lambda and the self-host SERVICE read the variable; the Worker does not and keeps 60 s.
- **Relaycontrol only (self-host, `READ_API_AUTH_MODE=relaycontrol_only`):** the RelayAuth path above is not built at all. A non-`sh_at_` bearer is 401 `invalid_token` with no outbound call, and RelayAuth settings refuse startup; see "Self-host canonical reads and `relaycontrol_only`" in §1.
- **Cloud side:**
  - The token is minted per (user, org, workspace) and cached in the isolate for up to 45 s.
  - Cloud keeps its existing access check (`resolveRelayhistoryWorkspaceAccess` / `assertCloudMayReadHistory`) **before** minting. A customer-hosted workspace is never routed to 96Bulls.

## 2b. Self-issued tokens (`READ_API_AUTH_MODE=self_issued`, default off)

Contract: `docs/contracts/session-read-api-v1.openapi.yaml`. Self-host service only; the Lambda refuses the mode at startup.

- **Tokens** are opaque (`rth_at_read_`, `rth_rt_read_`, `rth_st_read_`, 256 bits of CSPRNG entropy) and stored only as SHA-256 in `sessions.read_api_tokens` (migration `20261105044000`), reached only through SECURITY DEFINER functions guarded for the genuine `bulls96_read_api` login. Every read looks the hash up, so deleting a row is immediate revocation and there is no signing key. A lookup failure or timeout denies with 503.
- **Identity** (user, org, workspace) comes only from the row, and the row's audience must be `READ_API_ORIGIN`. Other token families are refused before any lookup.
- **Issuance** is Control's: after its membership check, Control calls `POST /internal/v1/session-read/sessions` and `.../sessions/refresh` with `READ_API_INTERNAL_SECRET` (`READ_API_INTERNAL_SECRET_PREVIOUS` overlaps during rotation). This service never calls Control. Refresh is one compare-and-set on the refresh hash and the exact identity.
- **Access lifetime** defaults to 5 minutes (`READ_API_ACCESS_TOKEN_TTL_SECONDS`, 60 to 300); refresh tokens live 90 days.
- **Offboarding and erasure:** `POST /internal/v1/session-read/revoke` deletes by org and optionally user, workspace or id. Holders end their own session with `POST /v1/auth/token/revoke`. Issue and refresh sweep up to 100 fully expired rows per call.
- **Standalone:** an operator mints an `rth_st_read_` token with `POST /internal/v1/session-read/service-tokens`. `/internal/*` must stay inside the cluster or operator network.
- **Refuses to start** without `READ_API_ORIGIN` or a 32+ character secret, with any RelayAuth or relaycontrol setting beside it, or with a bad TTL. `POST /v1/cli/login` is not registered in this mode.
- **Unchanged:** the R2 control-head privacy gate and `PIPELINE_ORG_FROM_PREFIX` / `PIPELINE_KEY_PREFIX` semantics run per request as before; a token replaces neither.
- **Reader kind on the mint** (`READ_API_MINT_READER_KIND`, default off; `1` or `true` in any case is on, anything else is off):
  - **Body contract.** With the flag on, `POST /internal/v1/session-read/sessions` and `.../sessions/refresh` accept an optional `reader_kind` of exactly `"browser"`, `"device"` or `"mcp"`. Any other value or type, including `""`, is 400 `bad_request`, and the message never echoes the value. With the flag off, nothing changes: a body carrying `reader_kind` is 400 `bad_request` "body has an unknown field". The service-token route never accepts it.
  - **Storage.** Issue records the value in the nullable `sessions.read_api_tokens.reader_kind` (migration `20261105062050`, CHECK on the same three values) through `read_api_token_issue_v2`, which only `bulls96_read_api` may execute. A mint without the field stores NULL through the unchanged 044000 `read_api_token_issue`, so the flag-off path does not need 062050. Apply 062050 before turning the flag on. Its rollback drops the column and the recorded values, so turn the flag off first. The issue log line carries `reader_kind` as a structured field when one was recorded.
  - **Refresh semantics.** Refresh validates `reader_kind` when it is present but does not store it. The rotated row keeps the value recorded at issue, so a refresh cannot set, change or clear it, and a different reader kind needs a new mint.
  - **Gate-only installs.** relaygate strips `reader_kind` before proxying, so this service never sees it and the flag has no effect there. On non-gate installs, Control sends `reader_kind` only once this flag ships, so turn the flag on before Control starts sending the field. Otherwise those mints are refused as unknown fields.
- **Message ids** (`READ_API_EVENT_MESSAGE_IDS=true`, default off, service entry with the canonical store): `includeMessageId=true` on session events. Off, the parameter stays a 501.
- **CORS** (`READ_API_CORS_ALLOWED_ORIGINS`, any mode, service entry only): exact https origins (http only on loopback), GET read routes only, never credentials or token routes.

## 2a. Self-host login and catalog (`RELAYHISTORY_SELFHOST_READ_API`, default off)

A self-host's relaycontrol proxies History reads with a user's `sh_at_` token. Two routes serve it, registered only when `RELAYHISTORY_SELFHOST_READ_API` is exactly `true` (unset, `""` or `false`: neither exists and every other route is unchanged; any other value fails startup). Code: `src/session-read-api/selfhost.ts`, `app.ts`, `auth.ts` `relaycontrolLogin`.

- **`POST /v1/cli/login`**, body `{agentRelayToken, label?}` (nothing else; 8 KiB max, 413 `body_too_large`). Bad shapes are 400 `invalid_body` / `invalid_agent_relay_token` / `invalid_label`. The token is introspected exactly as a relaycontrol read bearer (§2, `aud` = `READ_API_ORIGIN`, no workspace), so it needs `RELAYHISTORY_ACCEPT_RELAYCONTROL_TOKENS=true` with the URL and secret:
  - off: 503 `relaycontrol_not_enabled`; URL, secret or origin missing: 503 `relaycontrol_not_configured`; unreachable or malformed: 503 `relaycontrol_unavailable` (all with `Retry-After: 1`);
  - not an `sh_at_` token, wrong principal or no workspace: 401 `invalid_token`; inactive: 401 `token_inactive`; expired: 401 `token_expired`; other audience: 401 `wrong_audience`; no `rth:read`: 403 `forbidden`.
  - workspace not on the reads' canary allowlist (`READ_API_DIRECTORY_WORKSPACES`, checked after introspection): 501 `unsupported_by_backend` reason `login_workspace_not_enabled`, no workspace header;
  - 200: `{accessToken, accessTokenExpiresAt, refreshToken, refreshTokenExpiresAt, tokenType: "Bearer", scopes}` with `X-Relayhistory-Workspace-Id`.
  - `scopes` is the token's scopes intersected with the scopes this read-only backend serves (`rth:read`), never `rth:sync`, so relaycontrol refuses `mode=sync` against it; a grant left with no served scope is 403 `forbidden` (`missing required scope: rth:read`) from the handler, checked after the canary allowlist and expiry (with `relaycontrolLogin`, a token without `rth:read` already gets the same 403 during introspection, before the allowlist).
  - Pattern A (`RELAYHISTORY_SELFHOST_LOGIN_ADVERTISES_SYNC=true`, default off, needs `RELAYHISTORY_SELFHOST_READ_API=true`): when the self-host ingest edge (`SERVICE=session-ingest`, docs/session-ingest-design.md section 8) serves sync for the same origin, `scopes` may also include `rth:sync`, but only when introspection granted it. The edge introspects every request again and never trusts this reply.
- **Pass-through, no minting.** `accessToken` is the presented token. Nothing is signed, stored or cached beyond the introspector's positive cache (`READ_API_INTROSPECT_POSITIVE_TTL_MS`, ≤ 60 s, 0 for none; §2), so there is no new credential or long-lived secret, and every later read re-authenticates the token through the same introspection (revocation is seen within that cache window). `accessTokenExpiresAt` = min(relaycontrol `exp`, now + 15 min). The cap is advisory to the client: the server keeps accepting the token until relaycontrol's own expiry or revocation.
- **No refresh.** There is no refresh route. `refreshToken` repeats the access token with the same expiry only to fill the shape; a client logs in again (relaycontrol's proxy re-logs-in on 401). The `label` is validated and otherwise unused. Token values are never logged or echoed.
- **`GET /v1/sessions/:sessionId/catalog?workspace=&source=`**: authenticated and workspace-scoped like `/timing` (same canary allowlist, `workspace=` must equal the token's, else 403; `source=` exactly once). Body `{workspaceId, catalog, parents, children, commitLinks, truncated}`, read from the session's canonical S3 projection (`session_catalog`, `session_catalog_agents`, `session_relationships` with this session as parent, `session_commit_links`). Items carry Cloud's camelCase column keys; delivery provenance (record and origin ids) is dropped; the newest delivery wins; each array is capped at 500 (`truncated`). The catalog response exposes the session's `cwd`, `firstPrompt` and `workspaceRoots` to any `rth:read` holder of the workspace (the same scope that can read the session's events).
  - `parents` is always `[]`: a parent edge is delivered with the parent session's records, and the Postgres relationship directory is service-role only (a read path would need a migration).
  - No pointer for the session: 404 `not_found` (reason `catalog_no_pointers`), which relaycontrol maps to an empty catalog; erased: the empty catalog. The 404 is in-workspace only: it is answered strictly after the workspace attestation (`workspace=` must match the token, else 403; absent, 501 `org_scope`) and the canary allowlist (else 501 `catalog_workspace_not_enabled`) pass, so it is never a cross-workspace existence oracle. `/timing` and `/turns` keep their 501 `*_no_pointers`. An entry without a canonical source answers 501 `catalog_not_configured`: the Lambda always, and the self-host SERVICE when `LOCKER_BUCKET` is unset (§1, "Self-host canonical reads"). The Lambda Function URL handler is GET-only, so login there is 405; it is meant for the self-host server entry.

## 3. Data access and isolation

- **ClickHouse:** a read-only ClickHouse user (`SELECT` on `relay_search.transcript_events` only; created by the lead), reached through the vendored fetch client (`vendor/search-index/clickhouse.ts`).
  - Every query binds `org_id = {org}` and `workspace_id = {ws}` from auth as typed parameters, never interpolated.
  - `max_execution_time = 2` and `max_result_bytes` keep queries bounded.
  - Every returned row is rechecked in code (org, workspace, session, source). A mismatch raises `TenantMismatchError` and a 500, logged as `crossTenantRows: true` with no row data.
- **Supabase (phase A):** a new LOGIN role `bulls96_read_api` with `SELECT` on `sessions.session_index` columns `(org_id, workspace_id, harness, session_id, created_at, deleted_at)` only. (P3c, timing: also the object-pointer columns `canonical_session_id, machine_id, canonical_machine_id, user_id, storage_provider, bucket, manifest_key, content_sha256, raw_format, revision`, so the read API can find a session's canonical S3 objects; still no content.) No content, no writes, `statement_timeout = 2s`. It ships as a bootstrap SQL file plus a verify check in this PR and is applied by the lead.
  - Every query binds org and workspace from auth.
  - Why not the claim-bound RPCs now: the search-worker role admits only `authenticator` as a member (`session_directory_search_worker_verify.sql:101-140`). Adding another member is a HoopDb security-model change. **Phase C** routes lists through those RPCs, either through PostgREST with a short-lived JWT or through an approved second member edge, and that decision goes to the HoopDb owner.
  - Phase A needs only liveness, which carries no tenant content.

## 4. ClickHouse query rules

`transcript_events` is `ReplacingMergeTree(version)`, `ORDER BY (org_id, workspace_id, ts, session_id, source, machine_id, kind, event_id)`. Content is complete; timestamps are millisecond precision.

1. **Dedupe to the newest version before any value filter** (#242 H1). The inner query applies only sort-key predicates: org, workspace, ts bounds, cursor bounds, session_id (bloom), source and kind. It then takes `ORDER BY <sort key>, version DESC LIMIT 1 BY <sort key>`.
   - Value predicates are applied **outside**, on the deduped row: `project_id`, and the `q` match `positionCaseInsensitiveUTF8(substringUTF8(content,1,4096), q) > 0 OR positionCaseInsensitiveUTF8(task_title, q) > 0`, the legacy semantics.
   - So a redacted or rewritten newer version can never be filtered out while its stale predecessor is still served.
2. **Read in order** (#242 H2): the inner `ORDER BY` follows the table order (asc or desc on the full key) plus `version DESC`, with a plain `ts` bound alongside the tuple cursor (#242 H3).
3. **Erasure:**
   - The pipeline deletes an erased session's rows with a synchronous lightweight DELETE (#121 M2). Masked rows are invisible to SELECT.
   - The read API also refuses any session whose `session_index` rows all carry `deleted_at` (404 for session reads), and drops such sessions from search pages. This closes the gap between the S3 delete and the pipeline running.
   - A session with no pointer row at all is not treated as erased, because backfilled history may predate pointers.
4. **No silent truncation** (#251 lesson):
   - Pages fetch `limit+1` rows to decide `nextCursor`.
   - The 2 MiB byte budget (relayhistory's rule) cuts the page early only **with** a `nextCursor`.
   - `maxContent` adds relayhistory's marker `…[truncated by maxContent]` and sets `contentTruncated`.
   - **History floor:** `READ_API_COMPLETE_SINCE` is the timestamp from which ClickHouse is known complete (backfill start).
     - A session read for a session whose `created_at` is older returns 501 `incomplete_history`.
     - So does a session read with no pointer row while the floor is set above the epoch.
     - A first search page that would need rows older than the floor returns 501, because it reached the floor without filling.
     - A search **cursor** page that reaches the floor is a terminal 200 with `continuation: { backend: "relayhistory", reason: "history_floor", until }` (`until` = floor − 1 µs). Cloud appends relayhistory's page for the same parameters with that `until` and no cursor, and returns relayhistory's cursor, so a below-floor search paged to its end never ends in an error.
     - When backfill is proven complete, the floor is set to the epoch.
   - **Partial sessions:** a session whose latest `search_ingest_session_state` row has `needs_full = 1` (a partial S3 pass writes only that row; a capped reconcile writes some rows and that row) is not current in ClickHouse.
     - A session read's first page returns 501 `session_partial` (on the self-host SERVICE, optionally a retryable 503, see `READ_API_SESSION_PARTIAL_RETRYABLE` below).
     - A workspace search's first page returns 501 `session_partial` while a partial session belongs to the token's workspace. The state table has no workspace column, so a partial session is attributed to the workspace when it has any row (any version) in that workspace's `transcript_events`, or a live `session_index` pointer there. The pointer covers a session whose every pass was partial and so wrote no row. A session with neither belongs to another workspace, or is erased here (its rows are dropped anyway). Partial sessions of other workspaces of the org no longer stop the search.
     - Scoping to the candidate page alone would not be sound: the events a partial session is missing from ClickHouse are never candidates, so their matches would vanish silently.
     - The org's partial state is keyset-paged and attributed to the requested workspace. The first page has no synthetic empty-string lower bound; later pages advance by ClickHouse's UTF-8 byte order. An empty, malformed or non-advancing id makes the scan undecidable. Default `refuse` mode needs only an existence answer and returns on the first attributed session. It maps an undecidable id, the 32-page scan bound or the existing 3 s deadline to 501 `session_partial_unbounded`, so the caller still falls back instead of seeing a transient 503. Sibling-workspace sessions do not count toward the 1000-session result bound, but enough of them can exhaust the attribution scan's page or time bound. `skip` cannot safely exclude an unknown set and returns 503 for any of these undecided cases.
     - The check runs only when pagination starts. A cursor continues the read it admitted: a 501 on a cursor page would end the caller's pagination in an error or restart it with duplicates.
     - The ClickHouse read user also needs `SELECT` on `search_ingest_session_state`.
     - **Self-host SERVICE only, default off:** with `READ_API_SEARCH_PARTIAL_SESSIONS=skip` every page of a workspace search answers 200 from the index instead of the first page's 501. Its body adds `incomplete: { reason: "session_partial", sessions: [...] }`, naming the workspace's partial sessions (at most 1000, sorted by UTF-8 bytes, including both event- and pointer-attributed sessions), or `incomplete: { reason: "session_partial_unbounded" }` with no `sessions` when more are partial than one check lists. The response also carries `X-Sessions-Read-Incomplete: <reason>` (exposed to CORS callers only with the setting on). Every page, cursor pages included, runs the check again and reports what it found then, and the listed sessions' rows are left out of that page (`SearchQuery.excludeSessionIds`: ClickHouse `session_id NOT IN [...]` on the candidate keys, an array literal inlined in the query body with every id quoted and escaped, like the newest-version query's key list, because the client sends parameters in the request URL and up to 1000 ids could exceed a proxy's request-line limit; Postgres a bound `session_id <> ALL($n::text[])`; OpenSearch a `must_not` `terms` clause), so a session indexed while small that later grew past the bound never serves its stale rows. The set is recomputed per page, not carried in the cursor, so a session that completes between pages is searched again from the next page on, but only for rows after the cursor: its rows before the cursor were skipped and the caller never sees them in this pagination. `session_partial_unbounded` lists nothing and so excludes nothing: its pages may still hold stale rows of a partial session. Skip mode must collect the exclusion list, so its 32-page or 3 s bound is a 503 rather than an incomplete answer that could serve stale rows. A portable search backend that does not declare the `exclude_session` filter refuses `skip` at startup. An error after the check (history floor, backend) carries neither. A session-scoped read (`/v1/sessions/:id/events`, `/v1/events?session=`) of a partial session still answers 501 `session_partial` (or the retryable 503 below). On the Postgres or OpenSearch search path (both keep their index state in Postgres) "partial" is any live pointer whose index state is not complete and current: its first index pass has not run yet, it is too large for the limits (`scope_too_large`), or a new upload moved its directory epoch and the re-index has not run yet. So with `skip` a session that is being written to drops out of search results until its next index pass. Unset, `""` or `refuse` is unchanged; any other value fails startup, and the hosted Lambda refuses `skip`. The indexer side is `SEARCH_INDEX_MAX_SCOPE_BYTES` / `_EVENTS` / `_ROWS` (docs/local-stack.md), which keep a large session from being marked partial at all.
     - **Self-host SERVICE only, default off:** the 501 on a session read is deliberate: it tells the caller to read the session elsewhere, and on a cursor page it would end pagination. An install with nowhere else to read can set `READ_API_SESSION_PARTIAL_RETRYABLE=true`. Then the FIRST page of a session-scoped read of a partial session (`/v1/sessions/:id/events`, its canonical first page included, before or after the object read, and `/v1/events?session=`) answers 503 `backend_unavailable`, reason `session_partial`, with `Retry-After: 60` (exposed to CORS callers only with the setting on), so the caller retries the same first page later. Everything else is unchanged and keeps the 501: workspace search (`session_partial` and `session_partial_unbounded`), the canonical cursor page's recheck, and the hosted Lambda and Worker. The change is in the app's single error mapping (`fail()`), which each covered route asks for only on a first page, so the 501 call sites are untouched. There is no staleness bound: a session can stay partial indefinitely (too large for the index limits, or no index pass running), and the read role cannot see when its index state last changed. Such a session answers 503 on every try, so clients MUST cap their retries and then treat the read as the 501, and an operator's 5xx counts include these answers. Bounding it on the server would need a later, separate column grant to the read role (a timestamp of the index state); this setting adds no migration, grant or SQL. Unset, `""` or `false` is off and the response is byte-for-byte unchanged; any other value fails startup, naming the variable and never the value. The hosted Lambda refuses to start with `true`, and the read Worker answers every request 503 (`missing_READ_API_SESSION_PARTIAL_RETRYABLE`).
5. **Cursors** are opaque and namespaced: `b96e1:<base64url(JSON [tsMs, sessionId, source, machineId, kind, eventId])>`.
   - A relayhistory cursor (`events-v2:`) sent to 96Bulls gets 501 `foreign_cursor`.
   - Cloud keeps pagination sticky by prefix: a `b96e1:` cursor is never sent to relayhistory, which would silently restart at page one. The service answers a cursor page with a 200 (a terminal page carries `continuation` when relayhistory must finish it, see the history floor above). On a transport error or 5xx with such a cursor, Cloud returns the error and the caller retries the same cursor.
6. **Search** (`/v1/events?q=`) is workspace-scoped only (org-wide gets 501), newest first, `limit` ≤ 50.
   - `q` must be at least 1 character. Literal `%`, `_` and `\` are fine, because `positionCaseInsensitiveUTF8` is literal.
   - `asc` with `q` gets 501.
   - Filters `taskRef`, `tag`, `agent`, `projectContains` and `noProject` get 501 in phase A (no caller in phase A sends them).

### Hosted canonical events beyond the floor (`READ_API_CANONICAL_EVENTS_HOSTED`, default off)

Code only, default off, read Worker only (`ReadApiDeps.canonicalEventsHosted` in `src/session-read-api/app.ts`, wired in `packages/session-read-worker/src/index.ts`). The Lambda never builds the dependency, and the self-host service keeps its own canonical events path, which ignores this one.

- **Flags.** `READ_API_CANONICAL_EVENTS_HOSTED` is on only when exactly `true`. Unset or empty is off. Any other value, including `TRUE` or `false`, is a configuration error: every request that reaches the Worker's configuration step answers 503, and the log names `missing_READ_API_CANONICAL_EVENTS_HOSTED`. The `/v1/canonical/*` routes and the 405 method check answer before that step and are not affected. The flag also needs the raw-store secrets (`PIPELINE_RAW_S3_*`); without them the path stays off, and no log line says so. `READ_API_CANONICAL_EVENTS_WORKSPACES` is the workspace allowlist (`*` or a list). Unset or empty allows no workspace. Neither variable may be declared in `wrangler.jsonc` or any deploy template; the deploy-config render and the profile resolver refuse a template that declares either.
- **When it serves.** `GET /v1/sessions/:sessionId/events` is answered from the session's canonical objects only when all of these hold: the token's workspace is on the allowlist; the session has a live pointer; and its `created_at` is older than `READ_API_COMPLETE_SINCE`. Without `source`, the one live source of the session id is found through the `/sources` reader (`session-sources.ts`). Where an operator erase fence is bound, a source is dropped first when any form of its key is fenced, as `/sources` drops it. If there is no live source, the route answers as without the flag; if every source was dropped as fenced, it answers 404. If more than one is left, it answers 501 `events_canonical_source_ambiguous`; sources are never merged. The resolved source must itself be older than the floor.
- **Checks on every page.** These are the self-host path's checks:
  - The partial-session check runs before and after the object read (501 `session_partial`).
  - A tombstone found after the object read answers an empty page.
  - Where an operator erase fence is bound, a fenced source is dropped before the source is resolved, and the fence is rechecked after the object read (404, no content). The read Worker binds no operator fence today, so there an erase shows through the tombstone and partial checks.
  - A row of another tenant, session or source is 501 `events_canonical_tenant_mismatch`, and is never served.
  - A source row of another tenant is a 500. A source row that names the requested id in neither its physical nor its source ids is outside the reader's contract and answers 503. Neither is served.
  - A scope too large to read stays a 501.
- **Contract changes, with the dependency present only:**
  - 409 `cursor_backend_transition` for a `b96e1:` cursor on a session served from objects.
  - The same 409 for a `b96c1:` object cursor on a session not served from objects, for allowlisted workspaces only. Off the allowlist, and after a workspace is removed from the allowlist or the flag is turned off, a `b96c1:` cursor answers 501 `canonical_cursor_scope` as today.
  - The same 409 for a `b96c1:` cursor whose canonical source generation changed.
  - The 501 `events_canonical_source_ambiguous` above.
  - Object pages carry `b96c1:` cursors.
  - Known limit: an id that is also an alias of another source's session counts as two sources (501 `events_canonical_source_ambiguous`), as `/sources` lists both. The caller can pass `source`.
  - Every other answer, including sessions inside the floor and workspaces off the allowlist, is byte-identical to the flag-off route: 501 `history_floor`, the ClickHouse page, and 501 `canonical_cursor_scope`.
- **Origin labels.** Object pages carry message origin labels only when `MESSAGE_ORIGIN_LABELS_ENABLED` is exactly `true` on the Worker, as on the other paths.
- **Enabling.** Enabling is an operator decision: setting both variables on the deployed Worker. The deploy workflow passes its variables explicitly and carries neither of these, so a plain variable set by hand is dropped by the next workflow deploy. A durable enable needs a later reviewed change to the deploy workflow. With the flag on but the raw-store secrets missing, the path stays off without a log line. The caller's pagination stickiness must keep a `b96c1:` cursor on 96Bulls, as it does a `b96e1:` cursor, and must treat the 409 as "restart without a cursor".

## 5. Rollout

- **Cloud flags** are one Worker var per route group, `SESSIONS_READ_96BULLS_<GROUP>` = `off|shadow|on`. Phase A groups: `EVENTS_SEARCH`, `SESSION_EVENTS` and `EVENTS_IN_SESSION`. `SESSIONS_READ_96BULLS_URL` holds the origin.
  - Wiring follows the existing var pattern: `infra/web-worker.ts`, both deploy workflows and `fast-path-mirrored-vars.json`.
  - All default to `off`.
- **`shadow`:** relayhistory serves the request. The same request goes to 96Bulls in `waitUntil`, with a sample-rate var.
  - A structured log line `[sessions-read-shadow]` records route, status of both sides, latency of both sides, event-id overlap, first-mismatch index, a `crossTenantRows` check and the 501 reasons. It never logs content.
- **`on`:** 96Bulls serves the request.
  - **Automatic fallback** to relayhistory on network error, timeout (2.5 s), 5xx or 501, but never with a `b96e1:` cursor.
  - A 401 or 403 from 96Bulls is not retried against relayhistory. It is logged and Cloud remints once.
- **Switch gates per route:**
  - ≥ 30 min of shadow traffic;
  - zero `crossTenantRows`;
  - 96Bulls error rate ≤ 1%;
  - p95 ≤ 1 s;
  - event-id overlap ≥ 99% on the first page, with explained diffs only (tail lag);
  - erasure test: delete a test session, then confirm 404 and no search hit within the pipeline SLA.
- **Rollback:** set the var back to `off`. No deploy of 96Bulls is needed.

## 6. Out of scope here

- Brief, analysis, turns, digest and onboarding (the Worker now serves analysis behind a flag: section 9).
- Hybrid search.
- Cloud's direct `sessions.*` SQL readers.
- Delivery and auth for uploads.

These are tracked in the neon-readers inventory lanes.

## 7. Open decisions for the lead

1. **Supabase read role:** approve `bulls96_read_api` (SELECT on six `session_index` columns) for phase A.
2. **Phase C binding:** PostgREST JWT or a second member edge for the claim-bound RPCs. This goes to the HoopDb owner.
3. **Phase B schema:** pipeline writes an `event_json` column (the #242 shape) into a `transcript_session_events` table ordered by `(org_id, session_id, ts…)`. This goes to the #121 owner.
4. **`READ_API_COMPLETE_SINCE`:** the production value, from the backfill state.

## 8. Owner-only buffered read (Option C)

Code only, default off. `RELAYHISTORY_READ_OWNER_BUFFERED` enables it only when it is exactly `1` or `true`; unset or any other value leaves every route exactly as without the feature (no gate call, no SQL, no header). It is wired in the Worker only (`packages/session-read-worker`); the Lambda entry never builds the dependency. Code: `src/session-read-api/owner-buffered.ts`.

**Who sees what.** A buffered row is served only to its owner: the token must carry a verified user, `workspace=` must equal the token's workspace, and the workspace must be on both `READ_API_DIRECTORY_WORKSPACES` and the owner-buffered canary allowlist `READ_API_OWNER_BUFFERED_WORKSPACES` (each `*` or a list; unset means none). The SQL readers bind the user from the verified identity (`bulls96_read.user_id`); any returned row or stage header for another org, workspace or user is a 500 (`cross_tenant`), never a page.

**Authority gate (control-head contract v5; wire v5.1 (lead 245001538553413632), frozen at relayhistory-cloud #328 head a06a58f9 (lead 245002086845415424)).** The numbers are relay message ids of pipeline-lead's rulings, not account or resource ids. Every buffered response passes the privacy authority twice: before the SQL read and again after every RPC, S3 fetch and the list's live-pointer lookup, immediately before emit (nothing awaits between that check and the response). The authority is the `HISTORY_CONTROL_HEAD` service binding to relayhistory-cloud's named entrypoint `SessionReadPrivacyControls`: `GET /v1/session-read/control-head`, no query string, no body, identity only in the `x-relayhistory-org-id`, `x-relayhistory-workspace-id` and `x-relayhistory-user-id` request headers set from the verified token (never from caller input), a 2 s total deadline, and no caching (`cache-control: no-store` on the request, a 200 must carry a `cache-control` with the `no-store` token, nothing memoized). An identifier the producer would refuse with 403 (empty, over 512 UTF-8 bytes, not equal to its own trim, or containing U+0000-001F or U+007F) is fenced locally without the call.

The wire is whatever #328 at a06a58f9 emits, and the parser's key set and constants live in one table (`CONTROL_HEAD_WIRE`). A 200 body must have exactly these 12 top-level keys, none missing and none extra: `protocol` (`"relay-session-read-control-head-v1"`), `version` (the JSON number `1`), `issuer` (`"relayhistory-cloud"`), `tenant` (exactly `{orgId, workspaceId, userId}`, an exact echo of the caller, otherwise `gate_echo`), `controlEpoch` (hex64), `cutover` (exactly `{epoch, status, mode}` with `epoch == controlEpoch`, `status "active"`, `mode "ingest"`), `privacyBaselineSha256` (hex64), `sourceControlEpoch`, `sourceControlVersion` (decimal string `0..2^63-1`, never a Number), `sourceControlDigest`, `sourceControlKind` and `pending` (boolean). Version `"0"` means no head: `sourceControlEpoch`, `sourceControlDigest` and `sourceControlKind` all null and `pending` false. A version above `"0"` needs `sourceControlEpoch == controlEpoch` (a foreign or null epoch is fenced, never read as "no head"), a hex64 digest and a kind of `selection` or `reset`. `pending` true is fenced. Any unknown or missing key, top level or nested, fences, so **any key the producer adds fences every read until this parser moves with the producer**. The two observations must agree on controlEpoch, privacyBaselineSha256 and the head tuple. Every other key is a constant or the caller's identity, so this is deep equality of the two bodies. Any non-200 (403, 404, 405, 503, ...) means not visible, and its error body (flat, or the nested 503 `{version, error:{code}}`) is never parsed. Unbound, timeout, a 200 without no-store, or a malformed body also mean fenced.

**Visibility rule.** A buffered row or stage is visible only if the final observation is not pending and the row's original admission satisfies `admission epoch == controlEpoch == sourceControlEpoch` and `admission version == sourceControlVersion` (BigInt on decimal strings; NULL or absent admission is hidden). With no head, the admission must be version `"0"` at `controlEpoch`. The per-user G2 parity row (rule 4) is mandatory: PR A's readers gate through `sessions.owner_buffered_read_scope` on `sessions.owner_buffered_parity`, and the observed `privacyBaselineSha256` and head kind are explicit RPC parameters. The RPC authority parameters are `p_cutover_epoch = controlEpoch` and `p_privacy_baseline_sha256` (hex64, else not visible) always; with a head `p_head_epoch = controlEpoch`, `p_head_version` (exact decimal string), `p_head_digest`, `p_head_kind` (`selection` or `reset`) and `p_head_pending = false`; with no head `p_head_epoch`, `p_head_version`, `p_head_digest` and `p_head_kind` are NULL (never `"0"`) and `p_head_pending` is FALSE. Any reader error is fenced (not visible, retryable), never a 500: SQLSTATE 55006, 57014 (the 2 s statement timeout) and any other RPC or connection error. The SQL-side hides (frozen deletions, `upload_deletions`, `upload_resets`) belong to PR A's readers. PR A delta needed: `p_privacy_baseline_sha256` and `p_head_kind` on `buffered_sessions_for`, `buffered_session_stages_for` and `owner_buffered_read_scope`, the matching parity columns, and `group_ordinals` in the stage reader's result.

**Routes.**

- `GET /v1/sessions`: canonical rows UNION the caller's visible buffered rows, deduped by `(source, sessionId)` with canonical winning (also when a live pointer exists), newest first, one opaque composite cursor (accepted only when the feature is on for that request; with it off, or with a filter that skips buffered rows, a composite cursor is a 400 exactly as before, so a client paging across a flag flip restarts). `source`, `notSource`, `user` (only the caller) and the time bounds apply; `agent` or `projectContains` skip buffered rows for that request. Body unchanged; buffered rows fill every `sessionSummary` key. Header `X-Relayhistory-Buffered-Ids: <source>:<sessionId>,...` when the page holds buffered rows; fenced means buffered rows are dropped, the canonical page is still served and `X-Relayhistory-Buffered: fenced` is set.
- `GET /v1/sessions/:id/turns` and `/metadata` (with `source`): only when no canonical pointer exists. Stages are fetched from S3 (`buffered-batches/`, sha256 and tenant checked), the keep predicate is rechecked after the fetch and must list exactly the fetched stages, unchanged (all gone: erased, empty turns, metadata 404; any other difference hides the whole session, never a partial one; an S3 404 is retried once), and the result carries `X-Relayhistory-Buffered: 1` and `Cache-Control: private, no-store`. Not owned, hidden or absent: 501 `turns_no_pointers` as today. Authority unavailable before the SQL read (no candidate yet): the same 501, logged `buffered_unverified`. Fenced after a buffered candidate was found: 503 `buffered_fenced` with `Retry-After: 1`.
- Byte bounds on detail reads: each stage at most 16 MiB compressed (checked before GetObject) and 16 MiB decompressed (the decompressor stops at the bound), and at most 32 MiB per session (compressed and decompressed, across stages). Every zstd frame header is checked before decoding, so a declared content or window size over the bound is refused before the decoder allocates. Peak memory per read has not been measured. Over a bound: the session is hidden (501, logged `buffered_too_large`), never partial.
- `GET /v1/sessions/:id/events` (with `source`) only with `READ_API_OWNER_BUFFERED_EVENTS=on` as well; see **Owner-buffered events** below. `GET /v1/events` (workspace search) never.
- Never on activity, timing, catalog, output, digest or delivery routes.

**Read-time scrub.** Buffered stage objects carry no trustworthy scrub version, so every text field is scrubbed at read time with `scrubText` plus the v4 quoted-value rule: a quoted value under a secret key that is over 4096 units or unclosed is redacted in full (the remainder of an unclosed value is dropped). A ruleset version claimed in an object header never grants eligibility. `scrub.ts` is unchanged; the wrapper is marked TODO until v4 lands there.

**Env and bindings (Worker).** `RELAYHISTORY_READ_OWNER_BUFFERED`, `READ_API_OWNER_BUFFERED_WORKSPACES`, `INGEST_S3_BUCKET`, `INGEST_S3_REGION`, the existing raw-store key pair, and the `HISTORY_CONTROL_HEAD` binding (`service: relayhistory-api`, `entrypoint: SessionReadPrivacyControls`). All unset in `wrangler.jsonc`; only the deploy workflow's `owner_buffered=on` dispatch renders them (`packages/session-read-worker/scripts/render-deploy-config.ts`; sessions-pipeline-cloud:hosted-ops/preserved/runbooks/sessions-prod-session-read-worker.md). With the flag on and the binding absent, every request answers 503 `backend_unavailable` (logged `missing_HISTORY_CONTROL_HEAD`). Enabling needs the PR A migration applied (its `buffered_session_stages_for` must also return `group_ordinals`, which maps `kept_ordinals` to object lines), the S3 GetObject and KMS grant, and the binding; each is a separate approval.

**All-workspaces mode.** `READ_API_OWNER_BUFFERED_WORKSPACES=*`, rendered by a deploy dispatch with `owner_buffered=on` and `owner_buffered_workspaces=*`. The render accepts it only when `SESSION_READ_DIRECTORY_WORKSPACES` (the Worker's `READ_API_DIRECTORY_WORKSPACES`) is itself exactly `*`. It refuses `*` mixed with ids, `*` with an explicit or unset directory list, and `*` with the feature off. Default unchanged: off, and an explicit canary list validates as before. The privacy invariant does not change. The mode widens only the workspace allowlist in `ownerBufferedFor` (`src/session-read-api/app.ts`). A buffered row is still served only to its verified owner:
- the token must carry a user;
- the readers bind that user and the token's workspace per call, and gate on the caller's own G2 parity row (rule 4, `sessions.owner_buffered_read_scope` on `sessions.owner_buffered_parity`), so a user without a G2 row stays hidden;
- the control head is observed for exactly that (org, workspace, user).

Tested in `src/session-read-api/__tests__/owner-buffered.routes.test.ts`.

**Owner-buffered events (`READ_API_OWNER_BUFFERED_EVENTS`, default off).** The caller's own buffered events of one session, on `GET /v1/sessions/:sessionId/events` with `source` only. `GET /v1/events` stays canonical-only.
- Flag: unset or empty is off; exactly `on` enables; any other value makes every request answer 503 `backend_unavailable` (the Worker refuses the configuration, as it does owner-buffered on without its binding). On has no effect unless `RELAYHISTORY_READ_OWNER_BUFFERED` is on and the workspace is on both allowlists (`ownerBufferedFor`). With the flag off every events response is byte-identical to a deploy without it.
- Precedence and gating order: decided before `sessionGate`, on `sessionStart` alone. Any canonical pointer (live or tombstone) and canonical serves the whole response; the buffered gate, RPC and S3 are never called. With no pointer the gated read runs (`readOwnerBufferedEvents`: control head observed, stage RPC, S3 fetch with sha256 and tenant checks, recheck, control head observed again as the last I/O, admission against the final head). Visible rows are served with `X-Relayhistory-Buffered: 1`; `sessionGate` (history floor, partial-session 501) does not run for them, because those checks describe canonical completeness.
- Projection: the rows come from `canonical-projection.ts` `projectEventRows`, the projection `readCanonicalScope` runs, over the stages' records folded across stages (revisions and tombstones, `normalizeRelayhistoryRecords`), with the stages' admission version as the index-row version (not emitted). Each record keeps its own origin's machine (`deliveryMachine`) and the caller as owner; org, workspace, user, session, source and machine are checked per row (else 500 `internal`, logged `cross_tenant_rows`). Text fields are scrubbed (`scrubBufferedText`; `task_ref` through `scrubJson`) before the page is built; rows carry no stage key, ordinal or raw line. A buffered `convergence_event` line is refused and the session answers as hidden (logged `buffered_projection_failed`).
- Outcomes: hidden, not owned, too large, unverified before a candidate, or a refused projection answer exactly as without the flag. Fenced after a candidate: 503 `buffered_fenced`. Erased at the recheck: an empty page with the header.
- Paging: `page.ts` `readBufferedEventPage`, the canonical sort key (`comparePositions`: ts ms, session, source, machine, kind, event id by code point), the same limit and the same byte-budget cut as `readAdapterPage` (`budgetCut`). The cursor is the same `b96e1:` wire with the same six-field position plus a seventh element `"ob1"` (`encodeBufferedCursor`); `decodeCursor` still refuses seven elements (400 as today).
- Backend transition: **409 `cursor_backend_transition`**. A buffered cursor presented once the session has any canonical pointer (or is no longer visible as buffered), or a canonical cursor presented on a buffered page, is refused; the client restarts without a cursor. Continuing is not honest: identity and order are shared, but a continuing canonical cursor skips the partial-session (`needs_full = 1`) check, and nothing can tell whether the canonical transcript is yet complete against the buffered rows already served. A canonical tombstone answers the canonical empty page and never resurrects buffered rows; with a buffered cursor it is the same 409. With the events flag off a buffered cursor stays a 400.
- Wiring: Worker env `READ_API_OWNER_BUFFERED_EVENTS`, rendered only by a deploy dispatch with `owner_buffered_events=on` together with `owner_buffered=on` (the render refuses it otherwise). Out of scope here: buffered events on `GET /v1/events`, activity, timing or digests; served buffered `convergence_event` lines. Turning it on is a production deploy and needs Khaliq's explicit OK, as do the owner-buffered prerequisites.

Tested in `src/session-read-api/__tests__/owner-buffered-events.test.ts`, `owner-buffered.routes.test.ts` and `buffered-privacy-fixtures.test.ts`.

**Owner-buffered events latency flags (both default off).** Rendered only together with `owner_buffered_events=on` (sessions-pipeline-cloud:hosted-ops/preserved/runbooks/sessions-prod-session-read-worker.md, "Owner-buffered events latency flags").
- `READ_API_OWNER_BUFFERED_STAGE_CONCURRENCY`: unset, empty, `0` or `1` keeps the serial stage loop unchanged; `2`..`32` lets the events read (`readOwnerBufferedEvents` only; turns stay serial) keep at most N S3 GetObject+decompress calls in flight (`fetchStagesBounded`). Order and placement of authority checks are unchanged: control head, stages SQL, all fetches, recheck, final control head as the last I/O, admission, fold in stage order. A failing stage fails with the error the serial loop would throw (lowest failing index). An abort of the request signal stops new fetches and fails closed with `BufferedFenced("request_aborted")`. Any other value is a configuration error (503 `backend_unavailable`).
- `READ_API_OWNER_BUFFERED_PHASE_LOG`: exactly `1` adds `session_start_ms`, `control_head_1_ms`, `stages_sql_ms`, `s3_fetch_ms`, `s3_stage_count`, `recheck_ms`, `control_head_2_ms`, `fold_ms` (whole ms; phases not reached are absent) to the events request line, plus `aborted_phase` on failure; a canceled request logs them once on a `request_aborted` line (`aborted_phase` = phase in progress or `none`). Durations and a count only. Workerd timers do not advance during CPU-only work, so `fold_ms` can read 0; the Worker sees cancellation only with the `enable_request_signal` compatibility flag (not set today).

**Owner-buffered index verify (`READ_API_OWNER_BUFFERED_INDEX_VERIFY`, default off).** The buffered readers depend on the partial index `ingest_buffer_outbox_user_buffered_idx` on `sessions.ingest_buffer_outbox`, which migration `20261105043700_owner_buffered_read.sql` leaves to an operator's out-of-band `CREATE INDEX CONCURRENTLY`. Without it the buffered RPCs scan the outbox and hit the statement timeout. This flag makes the read API prove the index first instead of finding out by timeout.
- Flag: unset or empty is off (no probe, no new log field, byte-identical responses); exactly `on` enables; any other value is a configuration error (503 `backend_unavailable`, logged `missing_READ_API_OWNER_BUFFERED_INDEX_VERIFY`). No effect unless owner-buffered reads are on for the request (`ownerBufferedFor`).
- Probe: `sqlBufferedIndexProbe` (`src/session-read-api/owner-buffered.ts`) runs one READ ONLY transaction through the same `TenantRunner` with the same per-transaction `statement_timeout` as the buffered readers, and reads `pg_index` joined to `pg_class` and `pg_namespace` (catalogs readable by every role: no migration, function or grant). The index counts as valid only when `indisvalid`, `indisready` and `indislive` are all true; an invalid index (a failed or unfinished CONCURRENTLY build, which `IF NOT EXISTS` would skip) counts as not present.
- Cache (`BufferedIndexVerifier`, one per Worker isolate): a valid answer is remembered for the isolate's lifetime and no query runs again; a failure is remembered for 60 s (requests are fenced without a query), then probed again, so an operator's index build takes effect without a redeploy. Concurrent requests share one in-flight probe; each waits at most 5 s for it, after which that request is fenced `index_unverified` and the probe is forgotten.
- Where it sits: the first step of every exported gated read (`readOwnerBufferedList`, and `readGatedSession` under turns, metadata and events), before the first control-head observation. Not proven throws `BufferedFenced` with reason `index_missing`, `index_invalid` or `index_unverified` (a probe error or timeout; never a crash). Detail routes treat it as unverified before a candidate: the same 501 as without the flag, logged `buffered_unverified` with that `error_code`. The list serves the canonical page with `X-Relayhistory-Buffered: fenced`, logged `buffered_fenced` with that `error_code`. Only the reason name is logged; never SQL, values or identifiers.
- Wiring: Worker env `READ_API_OWNER_BUFFERED_INDEX_VERIFY`, unset in `wrangler.jsonc`, rendered only by a deploy dispatch with `owner_buffered_index_verify=on` together with `owner_buffered=on`. Turning it on is a production deploy and needs Khaliq's explicit OK.

Tested in `src/session-read-api/__tests__/owner-buffered.test.ts`, `owner-buffered.routes.test.ts` and `owner-buffered-events.test.ts`.

**Buffered search gate (`READ_API_BUFFERED_SEARCH_GATE`, default off).** With the flag exactly `on`, every buffered-origin hit in a workspace search passes `src/session-read-api/buffered-search-gate.ts` before it is emitted. The Worker role (`bulls96_read_api`) never reads the gate tables directly; it has no grants on them. The only SQL the gate sends is one call per owner to the SECURITY DEFINER function `sessions.buffered_search_gate_for` (migration `20261105043860_buffered_search_gate.sql`, replaced by `20261105043890_buffered_search_gate_retired_witness.sql` so a claim retired by the 043880 bookkeeping cleanup drops as `retired`; EXECUTE granted to `bulls96_read_api` only). The function checks six things. First, admission at the observed head. Second, that the claim's live (not retired) records cover the hit's (source, session_id). Third, the same gate-closed and G2 parity rules as 043700, recomputed through `owner_buffered_parity_expected`. Fourth, deletions under any origin. Fifth, 043700's batch predicate and rule 5(c): the claim's `ingest_batches` row must exist, be the owner's with the claim's digest, and be committed, buffered and accepted; the claim and its per-session epochs entry must be at the owner's current reset count; and no reset may be at or after the batch's `committed_at`. Sixth, latest admission: the claim's epochs entry for the session must be unambiguous and still equal the session's `ingest_fence_versions` epoch on the claim's origin, otherwise the hit drops as `superseded`. The function returns one verdict per hit, and a missing verdict or any error drops the hit. The Worker runs each owner's calls (chunks of up to 500 hits) in one read-only transaction; its isolation is set by `READ_API_REPEATABLE_READ` (below). Migration 043860 must be applied before the flag is set to `on`; without it every gated call fails and every buffered hit is dropped, which fails closed but empties the buffered results.

The gate mirrors the admission conditions 043700 applies at read time. The migration header carries the same table with line numbers.

| 043700 condition | Gate verdict |
| --- | --- |
| Caller is `bulls96_read_api`; org/workspace equal the transaction pins | 42501 refusal |
| User is the pinned reader | Different by design: the owner is a parameter and the claim must belong to (pins, owner); see the threat model below |
| Head, baseline and hit shapes | Malformed input returns no rows |
| Authority not fenced; head not pending; head epoch is the cutover; policy unexpired with this epoch and baseline | `gate_closed` |
| G2 parity row present; full tuple, receipts, generation and digests match | `parity_missing` / `parity_stale` |
| Admission epoch (policy epoch via the claim's identity) and version (head version, 0 without a head); ACK decisions agree | `admission_epoch_mismatch` / `admission_version_mismatch` |
| No `ingest_bookkeeping_retired_claims` tombstone for the claim (043890; the 043880 cleanup deleted its ACK decisions, so they can no longer witness the version). Checked after the epoch, before the version | `retired` |
| Claim records are an array, not retired, and hold (source, session_id) | `admission_session_mismatch` |
| Batch row: owner, digest, `committed`, `buffered`, `committed_at` set | `batch_not_buffered` (a missing row also drops) |
| Claim reset epoch and the entry's reset epoch equal the reset count; no reset at or after `committed_at` (rule 5(c)) | `reset` |
| Epochs entry present and unambiguous, and equal to the current fence version | `superseded` |
| `upload_deletions` (same or any origin) and frozen deletions (any origin) | `deleted` |
| Outbox active stage, stage acknowledged and not redacted, stage identity rows, `group_ordinals` shape, GC ancestor chain | Not mirrored. These are per-stage conditions and the hit names a claim, not a stage (residual divergence) |
| Group and ordinal shape, single-session group, live-ordinal count, lazy ACK native-slot owner | Not mirrored. These are per-group or per-ordinal conditions and the hit carries no ordinal (residual divergence) |
| Published exclusion (`ingest_publication_heads`) | Inapplicable. It is list dedupe so the canonical copy wins; the detail reader does not apply it |

Threat model. The owner is a parameter, not a pin, because a reader in a shared workspace sees hits owned by other users. Only `bulls96_read_api` may call the function, and only inside the verified (org, workspace) pins. Within that tenant a caller may name any user. For each supplied (claim_token, source, session_id), the caller learns whether the token is a claim of that owner (a row or no row). Only for such a claim does it also get one drop reason, which can reveal:

- the owner's gate and parity state;
- whether the session is deleted or reset;
- the batch state;
- whether the claim was superseded.

The function returns no content, no timestamps and no other claim. The claim token is what bounds this. Tokens are random v4 UUIDs held only in the ledger and in the owner's `transcript_events` rows (never in API responses). A caller without a token gets no row, so it cannot enumerate users, sessions or deletions. The Worker only passes tokens and owners taken from rows the reader's own tenant-scoped search returned. It logs only aggregate drop counts and never returns per-hit reasons to the client.

**Buffered search gate deploy input.** `READ_API_BUFFERED_SEARCH_GATE` (#338, `src/session-read-api/buffered-search-gate.ts`; only `on` enables) reads the same `HISTORY_CONTROL_HEAD` binding. Both are unset in `wrangler.jsonc`. Only a deploy dispatch with `buffered_search_gate=on` renders them: the flag `on` plus the pinned binding, rendered once when `owner_buffered=on` too. The gate needs no workspace list and no S3 vars. `off` (the default) renders byte-identically to a deploy without the input. Deploy the gate before the buffered search feeder (#339) is enabled. The dispatch is Khaliq's call (sessions-pipeline-cloud:hosted-ops/preserved/runbooks/sessions-prod-session-read-worker.md, "Buffered search gate").

**Brief summary deploy inputs (hosted Lambda parity, default off).** `brief_summary=on` adds `--var READ_API_BRIEF_SUMMARY_ENABLED:true` to the Worker deploy command, wiring `GET /v1/sessions/:id/summary` (`src/session-read-api/brief-summary.ts`, migration `20261105044100`); off (the default) renders no variable and the route answers 501 `summary_not_configured`. `list_short_summary=on` adds `--var READ_API_LIST_SHORT_SUMMARY_ENABLED:true`, serving `includeShortSummary=recorded` on `GET /v1/sessions`; off answers 501 `param_includeShortSummary`. The Worker enables each only on the exact string `true`, as the Lambda does. Neither is preserved from live: once on, a later `profile`/`preserve` dispatch must repeat it, or the live-settings guard refuses the dispatch. The dispatch is Khaliq's call.

### `GET /v1/sessions/:sessionId/summary` contract (with the Bob Costas fallback)

Query: `workspace` (required, the token's), `source` (required harness), `kind` (`short`
default). Any other parameter is a 501 `param_<name>`. Canary-gated (`summary`), brief SQL
`sessions.session_brief_summary_for` (migration `20261105044100`) required
(`READ_API_BRIEF_SUMMARY_ENABLED=true`).

**Fallback off (`READ_API_SUMMARY_FALLBACK_ENABLED` anything but exactly `true`, the
default): byte-identical to before.** `kind=long` (or any kind but `short`) is 501
`summary_kind`; the body is exactly one of

```json
{"summary":{"status":"ready","kind":"short","summary":"<1..320 code points>","model":"<1..128 chars>","builtAt":"<ISO>","state":null}}
{"summary":{"status":"empty","kind":"short","state":null}}
```

**Fallback on (`READ_API_SUMMARY_FALLBACK_ENABLED=true`, Worker only).** The inner `summary`
object never gains a key in any mode (live consumers parse it strictly); provenance is a
sibling `provenance` key, present on every 200 while the flag is on. `kind` is `short` or
`long`; anything else stays 501 `summary_kind`.

- `kind=short` with a native brief: the brief exactly as above, plus
  `"provenance":{"source":"session_brief","kind":"short","generatedAt":"<builtAt>","revision":null,"truncated":false}`.
- `kind=short` without a brief, and every `kind=long`: Bob Costas's summary object for the
  session, when one is servable:

```json
{"summary":{"status":"ready","kind":"short|long","summary":"<text>","model":"<recorded model id>","builtAt":"<ISO>","state":null},
 "provenance":{"source":"session_summary","kind":"short|long","generatedAt":"<ISO>","revision":4,"truncated":false}}
```

- Nothing servable: `{"summary":{"status":"empty","kind":"short|long","state":null},"provenance":null}`.

Field meaning for `source:"session_summary"`: `summary` is the scrubbed text Bob Costas
stored, cut on a word boundary (ending in `…`) to at most **320** code points for `short`
(the brief ready variant's bound) and **16000** for `long`; `provenance.truncated` says
whether the *returned* text was cut. `model` is the model id Bob Costas recorded inside the
encrypted summary record with that text (e.g. `extractive/v1` or the summariser model); a
record without a usable recorded model (non-empty, at most 128 chars) is not served, never
labelled with a default. `builtAt` = `provenance.generatedAt` = the stored record's
`built_at`. `provenance.kind` and `provenance.revision` are the *stored* summary object's
(the revision always equals the session's live pointer revision: a summary built for an
older revision is not served). For `source:"session_brief"`, `revision` is `null` (briefs
carry per-machine source revisions, not one number) and `truncated` is `false`.

What is servable (`src/session-read-api/summary-fallback.ts`), every step fail closed:
`session_brief_summary_for` runs first for both kinds, so its tenant scope, alias,
canonical-mapping and managed-history refusals hold (400 / 501 `summary_unqualified` / 503;
the fallback is not read). Then the session's live `sessions.session_index` pointer is read
as `bulls96_read_api` in a second tenant-bound READ ONLY transaction on the request's client
(the Worker's `requestTenantRunner` opens one transaction per check, so the brief check and
the pointer read are two transactions with their own snapshots, whatever
`READ_API_REPEATABLE_READ` says; a change committed between them, such as an erasure, is seen
by the pointer read, which itself requires `deleted_at IS NULL`). These are the same rows the
timing and turns routes read the transcript through: no live pointer (unknown, tombstoned,
erased) = empty; more than one live physical row (several machines) = empty, since each
machine's summary covers only its own raw. The pointer must be this tenant's, in this
Worker's raw bucket and provider, with a manifest key whose location matches the row and
whose prefix is allowed for the org (`prefixAllowed`); the object key
`<session prefix>/summary/<kind>.json.enc` is derived from it, never from the request. The
object opens only with AES-256-GCM under the caller's workspace key (HKDF from
`SUMMARY_MASTER_KEYS`, `src/shared/keyring.ts`) with associated data binding the key, kind
and revision, and its recorded `source_content_sha256` must equal the pointer's
`content_sha256` (revisions restart on a re-upload, so a leftover object from erased or
replaced content is never served); a text-free record (opt-out, quarantine, empty) is empty.
An object that does not open is empty only while it is stale: when its plaintext object
metadata (written with it) names exactly the pointer's revision, the requested kind and the
envelope's key id, and it still fails the AES-GCM tag, the cause is key material (a key id
this Worker holds with other bytes than the writer's), tampering or an object written under
another workspace's key, and the route answers 503 `summary_decrypt_failed`. (A wrong key
cannot be told from staleness on an object whose metadata names an older revision: that stays
empty until Bob Costas rewrites the object at the current revision, which then fails loudly.)
An object with no or partial `summary-*` metadata (written before it existed) keeps the old
behaviour: a failed open is empty. **Key ids in `SUMMARY_MASTER_KEYS` are immutable**: new key
bytes need a new id. Rebinding an id to other bytes makes every object written under it a lasting
503 for its tenant until the next revision rewrites it (the writer keeps an object whose
metadata matches). No migration and no
grant: the read role never reads `sessions.session_summaries`.

Errors with the flag on: a row outside the tenant 500 (`TenantMismatchError`); missing raw
store or `SUMMARY_MASTER_KEYS` (or a malformed value) 501 `summary_fallback_not_configured`;
a pointer that fails the checks, a malformed record, a store/deadline failure or a key id
this Worker does not hold 503 `backend_unavailable`; an object at the pointer's revision that
does not open under the key id it names 503 `backend_unavailable` with
`"reason":"summary_decrypt_failed"`. Never an empty 200 for a failure. Each 503 logs one line
with its reason code (`summary_fallback_pointer_invalid`, `summary_fallback_parse`,
`summary_fallback_unavailable` or `summary_decrypt_failed`) beside the route's usual request
line; no text, key, path or id.

Deploy (Khaliq's call): `summary_fallback=on` (only with `brief_summary=on`) adds
`--var READ_API_SUMMARY_FALLBACK_ENABLED:true`; the Worker also needs the secret
`SUMMARY_MASTER_KEYS` (the same value Bob Costas encrypts under) and the `PIPELINE_RAW_S3_*`
secrets with GetObject on the summary objects. Not preserved from live, like the other
summary inputs. The self-host service and the hosted Lambda do not wire the fallback.

The raw store itself is plain config the deploy renders (`scripts/render-deploy-config.ts`,
whenever `summary_fallback=on` or `analysis=on`): `PIPELINE_RAW_S3_BUCKET` and
`PIPELINE_RAW_S3_REGION` from the GitHub environment variables `SESSION_READ_RAW_S3_BUCKET` /
`SESSION_READ_RAW_S3_REGION` when set, else `SESSION_READ_INGEST_S3_BUCKET` /
`SESSION_READ_INGEST_S3_REGION`, plus `PIPELINE_KEY_PREFIX` (`TENANT_KEY_BASE`,
`stacks/aws/tenants.ts`) and `PIPELINE_ORG_FROM_PREFIX=true`, the hosted pipeline's org-derived
key layout (`stacks/aws/session-pipeline.ts`); without those two, `prefixAllowed` refuses every
hosted pointer row. The provider stays unset (`s3`, the provider the pipeline records). Before
this, a deploy rendered none of them and the route answered 501
`summary_fallback_not_configured` even with the secrets set. An on dispatch fails before
`wrangler deploy` while the bucket or region is unset or malformed (names only). A wrong bucket
fails closed: every pointer row whose bucket differs is refused (503 `backend_unavailable`, logged
`summary_fallback_pointer_invalid`), never an empty 200. The fallback to the ingest variables is
correct only when the pipeline's raw store is the same bucket as the ingest bucket; when it is not,
`SESSION_READ_RAW_S3_BUCKET` / `_REGION` must be set, and a wrong bucket fails closed as above (503s,
no other store's data served). With `summary_fallback` and `analysis` both off the rendered config
is byte-identical to before. A profile/preserve dispatch refuses to drop or change live raw-store
vars; `analysis=disable` without `summary_fallback=on` may drop them, under the rule below.

**The render does NOT turn on hosted timing, turns or metadata reads.** Those routes (and
owner-buffered turns, hosted canonical events, babysitter evidence and `/v1/canonical/*`) read the
same raw store without a flag of their own, so binding it would otherwise switch them on. The Worker
variable `READ_API_RAW_STORE_OTHER_READS` keeps them as they are: unset or empty = as before;
exactly `off` = they answer as without a raw store (timing 501 `timing_not_configured`, turns and
metadata 501 `turns_not_configured`, `/v1/canonical/*` `not_configured`); any other value fails
closed (503, logged `missing_READ_API_RAW_STORE_OTHER_READS`; `/v1/canonical/*` withholds the
store). The summary fallback and analysis are not gated by it. `resolve-deploy-profile.ts`
(`rawStoreOtherReads`) decides it from the live Worker, fail-closed: when an on dispatch introduces
the raw store (no live `PIPELINE_RAW_S3_BUCKET`), the render adds `READ_API_RAW_STORE_OTHER_READS=off`;
when the raw store is already live as plain vars without the switch, it adds none (those reads stay
on as they were); a live switch is kept (only plain `off` is accepted). Any raw-store var stored as
a secret (checked before a live switch is accepted), a partial live raw store, an invalid live
switch, or (with no live switch) any raw-store value other than the live one is refused, explicit
`owner_buffered=on/off` included. So is any other live `PIPELINE_RAW_*` binding while no
`PIPELINE_RAW_S3_BUCKET` is live (a provider, endpoint or SSE mode set up by hand that the render
would combine with its own values); the two credential secrets `PIPELINE_RAW_S3_ACCESS_KEY_ID` /
`_SECRET_ACCESS_KEY` without a bucket are the expected state and still count as a fresh add. The
removal (`analysis=disable` without `summary_fallback=on`) drops the vars together with the switch,
and is refused when the raw store is live without the switch (those reads were served through it
and would silently go off) or when the bucket stays a secret (dropping the switch would turn them
on). An explicit `owner_buffered=on/off` dispatch with both inputs off would drop them the same
way, so the same two refusals apply there (profile and preserve mode already refuse dropping any
live setting); with no raw-store var and no switch live, the ordinary redeploy, nothing changes. Break-glass reads
no live state, so it refuses `summary_fallback=on`. Turning timing, turns or metadata on is a
separate decision with its own step (removing the switch from the Worker), never part of a
`summary_fallback` or `analysis` dispatch.

### `GET /v1/sessions/:sessionId/sources` contract (`READ_API_SESSION_SOURCES_ENABLED`, default off)

For a caller that holds a session id without its harness (Cloud resolving an id the iPad sends
without a `source`): the distinct harnesses that hold a **live** pointer row for the id in the
token's org and the attested workspace. Query: `workspace` only (required, the token's); any other
parameter is a 501 `param_<name>`. No paging, one indexed statement
(`src/session-read-api/session-sources.ts`).

```json
{"sessionId":"<the id>","sources":[]}
{"sessionId":"<the id>","sources":["claude"]}
{"sessionId":"<the id>","sources":["claude","codex"]}
```

- `sources` is deduplicated and sorted by name, at most 16 entries (more is never expected; an id
  under more harnesses lists the first 16 and is ambiguous either way).
- `[]` = **unknown here**, a 200 like an empty directory page, not a 404: the id has no live row in
  this workspace (never published, erased, or owner-buffered only). Exactly one entry = the source
  is proven. Two or more = ambiguous. The caller decides only on exactly one.
- Live = `deleted_at IS NULL` on `sessions.session_index`, the same liveness the directory, timing,
  turns and summary reads use. An erased session (every pointer tombstoned) has none, so it
  answers `[]`. The id matches the source RelayHistory id (`canonical_session_id`, NULL meaning
  `session_id`) or the physical `session_id`, as the timing and turns pointer reads match it.
- With the self-host operator erase fence configured (`SESSION_ERASE_FENCE_ENFORCED`, service entry
  only), a source is dropped when its key is fenced under **any** form: the requested id and every
  physical and source id the harness's live rows hold (`sessions.ingest_operator_runtime_erased`,
  already granted to the read role). The checks run one after another on the request's runner.
  The shared `/v1/sessions/:sessionId/*` fence middleware skips this path (as it skips the summary
  search), so the flag check comes first there too and a flag-off request makes no database
  statement. That middleware keys the fence on the requested id only; it is left as is.
- Owner-buffered (unpublished) sessions have no pointer row and are never listed, even for their
  owner; a buffered-only id answers `[]` (unknown). This is close to, not identical with, the
  list: the list hides a buffered session whose **physical** id has a live pointer under any
  harness (`LIVE_POINTER_SESSIONS_SQL`), while `/sources` also matches a pointer by its source id
  (`canonical_session_id`). So `sources` can name a harness for an id whose buffered copy the list
  would still show beside it; it never names a harness without a live pointer row.

**Privacy fences (what `deleted_at IS NULL` does and does not cover).** `/sources` excludes exactly
what the directory list, timing, turns and summary reads exclude, and no more: none of them has an
extra fence predicate (the list filters `sessions.session_directory.deleted_at IS NULL` in
`sessions.session_directory_list_sql`, 20261101050000; the pointer reads and
`session_brief_summary_for`, 20261105044100, filter `session_index.deleted_at IS NULL`). Per fence:

- Operator per-session erase (`ingest_operator_erase_session`, 20261105050000): the fence row
  (`session_erasure_fences`, every alias) and the pointer tombstone are one transaction
  (`ingest_operator_erase_fence`: `UPDATE sessions.session_index ... SET deleted_at ...`). Two
  windows leave a live row behind the fence: a fence failure (RH010) commits the fence and a
  `failed` receipt without the tombstone, and a pointer guarded by a live managed publication head
  is skipped (`pointerRows` stays pending). On the self-host service with the fence enforced the
  route drops such sources (above); the hosted entries do not wire the fence check for any route.
- Operator per-user erase (20261105058010): buffered uploads only; it refuses while the user has
  any `session_index` row, so it never leaves a live pointer.
- Self-host per-session selection erase (`ingest_selfhost_set_session_uploads`, latest 20261105062060):
  selection and tombstone (revision raised to 2^53-1) in one transaction, for the caller's origins
  known at erase time; a later first upload from a new origin, or a pointer indexed after the
  erase, is not fenced (documented gaps of that function).
- Hosted selection (stop-sync, `ingest_apply_selection`: `ingest_fence_versions` bump +
  `upload_deletions`) and privacy reset (`ingest_apply_reset`: the same + `upload_resets`),
  20261104030000: the AFTER triggers `ingest_publication_retire_version` / `_retire_reset`
  (`ingest_publication_retire_fence`, 20261104060000, latest body 20261105044070) tombstone, in the
  same transaction, only pointers of **managed publication heads**. A non-managed pointer (written
  by `upsert_session_index` from a manifest event) stays live until its deletion manifest or the
  eraser tombstones it. During that window the list, timing, turns, summary and `/sources` all
  still see it. The read role holds no grant on `upload_deletions`, `upload_resets` or
  `session_erasure_fences` (only `bulls96_legacy_export_privacy` reads them), so closing that
  window needs a new granted function and a migration (separate approval), for every read route,
  not only this one.

Order of refusals, so nothing about another workspace is revealed: no or bad bearer 401 (and 403
without `rth:read`); flag off 501 `sources_not_configured`; `workspace=` absent 501 `org_scope`,
repeated 400, not the token's 403; off the directory canary allowlist
(`READ_API_DIRECTORY_WORKSPACES`) 501 `directory_workspace_not_enabled`; an unknown parameter 501
`param_<name>`; an id over 512 characters 400 `bad_request`. Then the read: a row outside the
tenant 500, a database failure or fence-check failure 503 `backend_unavailable`. Every 501 uses the
standard `{"error":{"code":"unsupported_by_backend","message":"...","reason":"<reason>"},"correlationId":"..."}`
envelope. Before this route existed the path answered the not-found 501 with reason `route`; with
the flag off it now answers reason `sources_not_configured` (same status and code).

Data path: the `bulls96_read_api` role's existing column SELECT on `sessions.session_index`
(`org_id, workspace_id, harness, session_id, deleted_at, canonical_session_id`;
`supabase/bootstrap/session_read_api_role.sql`) under its tenant RLS policy, in the request's
tenant-bound READ ONLY transaction: one row per harness (with the physical and source ids its
live rows hold), ordered by `harness COLLATE "C"`, `LIMIT 16`. The physical-id branch is an index
lookup (`si_session`); the source-id branch scans the workspace's live aliased rows in the partial
index `session_index_canonical_live`. No new function, grant or migration.

Wiring: every entry enables it only on the exact string `true`. The Worker and the self-host
service read it from their environment. The hosted Lambda reads it too, but its stack
(`stacks/aws/session-read-api.ts`) does not pass the variable yet, so enabling it there needs a
stack change (not made). Worker deploy (Khaliq's call): `session_sources=on` adds
`--var READ_API_SESSION_SOURCES_ENABLED:true`; off (the default) renders no variable. Not
preserved from live, like the summary inputs. In `profile`/`preserve` mode (`owner_buffered`
unset, `profile` or `preserve`) the live-settings guard refuses a dispatch that would drop a live
one; with an explicit `owner_buffered=on|off` the guard does not run, and `session_sources=off`
then drops a live variable silently (true of every summary input).

**Read transaction isolation (`READ_API_REPEATABLE_READ`, default off).** Every Worker read (timing, turns, babysitter evidence, owner-buffered, liveness, directory, brief titles and the buffered search gate) runs in a tenant-bound read-only transaction opened by `requestTenantRunner`. Unset, `0` or any value other than exactly `1` keeps `BEGIN READ ONLY` at READ COMMITTED, the behaviour before this flag. `1` opens `BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY`. Read-only REPEATABLE READ transactions never fail serialization. The flag exists because Hyperdrive's handling of it is not yet proven. With the flag off, the two chunks of up to 500 hits in one owner's gate call each see their own snapshot. Each chunk is still one statement judged against one consistent state and still fails closed on that state, so no hit passes a check it failed. But a commit between the chunks (a deletion, reset or fence bump, say) can leave hits of the same owner on one page judged against different states: an earlier chunk may admit a hit that a later chunk would have dropped. With the flag on, both chunks share the transaction's one snapshot, so the page is judged against a single state.

### `GET /v1/sessions` list readiness (`READ_API_LIST_READINESS_ENABLED`, default off)

Cloud lists sessions from this API and needs to tell, or filter, the sessions whose search projection is
complete. Migration `20261105062200_session_directory_list_readiness.sql` adds two new functions beside the list
functions the route calls, `sessions.list_session_directory_readiness_for` (beside 034000's
`list_session_directory_for`) and `sessions.query_session_directory_readiness_for` (beside 046010's
`query_session_directory_for`). They return the same rows plus `events_ready` (`event_projection_ready AND
event_projection_version = 1`) and `event_projected_at`, and take `p_ready_only`. SECURITY DEFINER, pinned
`search_path`, EXECUTE for `bulls96_read_api` only, as their neighbours. No existing function, table or index
changes.

Two caller opt-ins, served only with the server flag exactly `true` (`exactTrueFlag`; the Lambda, the self-host
service and the Worker read it alike):

- `includeReadiness=true`: each session also carries `eventsReady` (boolean) and `eventsProjectedAt` (ISO 8601
  or `null`, the last completed projection as stored, so it can be set while `eventsReady` is false). Rows, order
  and cursor are unchanged.
- `readyOnly=true`: only the ready sessions. The predicate is in the SQL, before `ORDER BY`, the keyset and
  `LIMIT p_limit + 1`, so a page is full whenever it has a `nextCursor`. Across a walk, no row repeats and none
  is skipped as long as its readiness and activity time do not change during the walk; a row that becomes ready,
  becomes not ready again, or moves in activity time while a client pages can appear, vanish or move, as with any
  keyset list. It applies to `q` reads too; with owner-buffered reads configured, `q` + `readyOnly` answers 501
  `q_with_buffered`, as `q` alone does on main. Each 200 carries `X-Relayhistory-Ready-Only: applied`. **A client must check this
  header: readers older than this change ignore the param and answer 200 unfiltered, without it.**

Order of checks: the workspace checks and the `includeAgentName` / `includeShortSummary` checks answer before
the readiness parameter check, so a request that fails one of those gets that answer, not a readiness one.

Grammar: only the exact `true` turns either on. With the flag on, any other value (`false`, `1`, empty, `TRUE`)
or a repeated param is 400 (`<name> must be true`), as `includeShortSummary` answers 400 for anything but
`recorded`: an opt-in has one spelling, so a typo never silently reads as off. With the flag off the param, with
any value, is 501 `param_includeReadiness` / `param_readyOnly`. That check comes before the value check, unlike
`includeShortSummary`, so a flag-off reader gives one answer for every value. Without either param the list is the
pre-readiness one whatever the flag says: same functions, same SQL text, same rows, cursors and bytes
(`src/session-read-api/__tests__/fixtures/list-readiness-flag-off.golden.json`, recorded on the base commit).

Cursors: a `readyOnly` plain page's cursor is the `b96s1:` wire with a fourth element, `"ready1"`. The
pre-readiness decoders refuse it as they are, and a `readyOnly` read accepts only that form, so replaying a
cursor across a `readyOnly` change, in either direction, is 400 `cursor is not valid`, never a silent
continuation. A `q` page's fingerprint gains one element only when `readyOnly` is true, so every other `b96q1:`
fingerprint is unchanged. `includeReadiness` never changes a cursor.

Owner-buffered rows have no directory row. `includeReadiness` keeps the union and reports them `eventsReady:
false`, `eventsProjectedAt: null`. `readyOnly` serves the canonical page only: no buffered read and no
`X-Relayhistory-Buffered*` headers. **A `readyOnly` page never shows a just-uploaded buffered session.**

Readiness is search-projection state (the directory row's `event_projection_ready` with projection version 1). It does not prove that the session's exact or context reads work: those use pointer liveness. `readyOnly` therefore hides, for good, sessions too large to index, scopes over budget, sessions with persistent canonical metadata errors and every session on an install with no indexer; and, briefly, every active session after each upload (a pointer write makes the row not ready until the next index pass).

Counts: `GET /v1/sessions` returns no count or total. `GET /v1/sessions/activity`, the users aggregate and the
summary counts are unchanged and count every session, ready or not, so they can be larger than what a
`readyOnly` walk lists.

Rollout: apply `20261105062200` first. With the flag on and the migration absent (or rolled back) either param
is 501 `directory_not_migrated` and every other list request is unchanged. Worker deploy input: `list_readiness=on`
adds `--var READ_API_LIST_READINESS_ENABLED:true`; off (the default) renders no variable; not preserved from live.
Hosted Lambda: `transcriptStore.sessionReadApi.listReadiness: true` renders the variable; absent, the env is
unchanged.

## 9. Session analysis on the Worker (lane F1, `READ_API_ANALYSIS_ENABLED`, default off)

The analysis contract (`GET` / `POST /v1/sessions/:sessionId/analysis`, `src/session-read-api/app.ts`, migration `20261105046000`) was wired only on the hosted Lambda (`lambda.ts`, `sqlAnalysisSource`). The Worker now wires the same `AnalysisDeps` (`packages/session-read-worker/src/index.ts` `analysisDeps`), behind one flag. This is a different lane from `/v1/canonical/analyses/` (`analysis-cache.ts`), which is unchanged.

**Flag off (`READ_API_ANALYSIS_ENABLED` anything but exactly `true`, the default): byte-identical to before.** `GET` answers 501 `unsupported_by_backend` with reason `analysis_not_configured`; `POST` (and every other non-GET) answers 405 `method_not_allowed` with `Allow: GET`. No analysis SQL runs, no admission connection opens and no result object is read.

**Flag on.**

| request | answer |
|---|---|
| `GET /v1/sessions/:id/analysis?workspace=` | 200 `{sessionId, requests, analyses, correlationId}` (relayhistory's v1 read; `analyses[].result` from the verified result object, `resultRef`/`origin`/`harness` never on the wire); 404 `not_found`; 400 `source_mismatch`; 503 `backend_unavailable` for a missing, resized or altered result object |
| `POST /v1/sessions/:id/analysis?workspace=` | 202 `{request}`, 404 `not_found`, 429 `rate_limited` (`Retry-After: 86400`), 400 `invalid_request`; 403 for another workspace or without `rth:sync`, both checked before any SQL; 501 `analysis_admission_not_configured` without the `ANALYSIS_ADMIT` binding |
| either, without the `PIPELINE_RAW_S3_*` raw store | 501 `analysis_not_configured` |
| any other non-GET (another path, a trailing slash or longer path, `PUT`/`PATCH`/`DELETE`) | 405 `method_not_allowed`, `Allow: GET` |

Both methods keep the app's gates: `attestWorkspace` (the `workspace` must be the token's), the canary allowlist (`READ_API_DIRECTORY_WORKSPACES`, route `analysis`) and the live-harness uniqueness check (`source_mismatch`).

**Wiring.**

- *Read side:* `sessions.analysis_read_v1` and the live-harness query run on the request's existing tenant runner (`requestTenantRunner`: `bulls96_read_api` through `SUPABASE`, `BEGIN READ ONLY`, `TENANT_SETTINGS_SQL` bound from the verified identity). The result object is read from the pipeline raw bucket (the canonical store the analysis worker publishes to) with the Worker's `PIPELINE_RAW_S3_*` secrets (`rawStoreFromEnv`, the store the summary fallback and timing reads use), bounded at 10 s per store call. An object whose response `content-length` exceeds the size the job row recorded is refused before its body is read (as is a response with no size when the row recorded one; the body is also cut off past the recorded size), and one GET reads at most 16 MiB of result objects in total (`ANALYSIS_RESULT_REQUEST_BUDGET_BYTES`); past either it is a 503, never a partial page. The key, SHA-256 and size come from the job row the read function returned (server data); the key must match `…/normalized/session-analysis-v2/<uuid>/<sha256>.json` (`session-analysis/read.ts`), and `readAnalysisResult` refuses an object whose size or digest differs. Nothing from the request goes into the key.
- *Admission side:* a second, optional Hyperdrive binding `ANALYSIS_ADMIT` connects as `bulls96_analysis_admit` (its own Hyperdrive config, caching disabled). `perRequestAdmitPool` (`src/session-read-api/analysis-source.ts`) gives `sqlAnalysisAdmit` the `AdmitPool` `connect()`/`release()` it expects over one fresh client per admission: `BEGIN` (read-write), `statement_timeout` 2 s and `lock_timeout` 1.5 s set LOCAL (`ANALYSIS_ADMIT_BOUNDS_SQL`; the function takes a per-org advisory lock before its throttle check, and Hyperdrive clients carry no startup options), `TENANT_SETTINGS_SQL`, `sessions.analysis_request_v1`, `COMMIT`, or `ROLLBACK` on any failure; the client is closed on release and, in `waitUntil`, any client never released. It never shares the read role's client. Without the binding, `POST` is 501 `analysis_admission_not_configured` and `GET` still serves.

**Privacy and fences (the DB functions; no new fence work here, #614 is separate).** In `supabase/migrations/20261105046000_analysis_jobs.sql`:

- `analysis_read_v1` takes its org from `sessions.session_directory_read_api_scope(p_workspace_id)` (line 748), which pins the read role's bound tenant, and filters every request and job row by that org and workspace. It answers `not_found` for a session with no live pointer and no request (lines 755-762) and for a session behind a purge fence that has not been re-uploaded since (lines 763-770). EXECUTE is granted to `bulls96_read_api` only (line 826).
- `analysis_request_v1` first calls `sessions.session_analysis_admit_scope` (line 635; defined at 393-409), which refuses (42501) unless the caller is `bulls96_analysis_admit` and the org and workspace equal the transaction's bound `bulls96_read.*` settings. It takes the per-org and per-scope advisory locks the purge takes (637-640), answers `not_found` for a session with no live pointer (641-646) or behind a purge fence (648-655), and applies relayhistory's rate limits. EXECUTE is granted to `bulls96_analysis_admit` only (line 827).
- Erasure: the forget triggers (lines 435-448) delete a tombstoned or deleted session's analysis rows and ticket its result objects for the sweeper.
- Known window (pre-existing for every Worker session route): the Worker wires no `operatorErased` middleware, so the read query and the S3 fetch are not one snapshot. A purge or erasure that commits between `analysis_read_v1` and the result `GetObject` can still serve that result's bytes on that one request, until the sweeper deletes the object; the next read answers `not_found`.

**Deploy (Khaliq's call).** The workflow input `analysis` (`off`/`on`/`disable`, default `off`): `on` adds `--var READ_API_ANALYSIS_ENABLED:true` to the deploy command. Like the summary inputs it is not preserved from live, but its guard (`resolve-deploy-profile.ts` `unchangedAnalysisSettings`) runs in every mode, including explicit `owner_buffered=on/off` (which reads live for it): it refuses a dispatch that would drop a live `READ_API_ANALYSIS_ENABLED=true` or change or drop a live `ANALYSIS_ADMIT` binding. `analysis=disable` is the explicit removal. (The other summary inputs keep their profile/preserve-only guard.) An `on` dispatch fails before `wrangler deploy` unless the `PIPELINE_RAW_S3_*` secrets exist (names only). The binding is rendered by `scripts/render-deploy-config.ts` whenever the GitHub environment variable `SESSION_READ_ANALYSIS_HYPERDRIVE_ID` is set (32 hex, not the read role's id), independent of the `analysis` input (a flag-off Worker ignores it). The render is byte-identical to before only with that variable unset, not merely with the flag off. When it is set, the workflow first checks the live config with `check-hyperdrive-config.ts` `HYPERDRIVE_ROLE=analysis_admit`: caching disabled, `verify-full` against the uploaded Supabase CA, the same pooler host, port, scheme and database as the read role, and the admission role's pooler user.

**Raw store.** An `analysis=on` dispatch also renders the same plain raw-store vars as `summary_fallback=on`, under the same `READ_API_RAW_STORE_OTHER_READS` rule (the summary-fallback deploy note above: bucket and region required, from the same variables; the render does NOT turn on hosted timing, turns or metadata reads, and turning those on is a separate decision with its own step). The fallback to the ingest bucket is correct only when it is the same bucket as the pipeline's raw store. A wrong bucket fails closed: the result-object read fails and GET serves an error, 503 `backend_unavailable` (logged reason, for example `analysis_result_missing`; the body carries only the code), never another object. This wiring is additional scope: it gives no authority to enable or deploy analysis.

**Break-glass and pinning.** Explicit `owner_buffered=on/off` with a directory (input or `SESSION_READ_DIRECTORY_WORKSPACES`) and `analysis=disable` contacts no live metadata at all, so it still deploys when the live Worker is split, absent or its API read fails. It cannot compare live, so a set `SESSION_READ_ANALYSIS_HYPERDRIVE_ID` renders as given there. Everywhere else `analysis=disable` requires that variable to be unset (drop the binding) or to name the live binding's config (keep it), never another one. Explicit mode with a directory reads live only for the analysis guard and does not pin that version (no `PRESERVED_READER_VERSION`, so the pre-deploy live-version check does not run); only a directory taken from live is pinned.

**Shared code on the Lambda (readback).** The route and `sqlAnalysisAdmit` changes are in shared code, so the hosted Lambda (and the self-host service) behave the same way: a POST without `rth:sync`, or for another workspace, now answers 403 before any 501 (`analysis_not_configured`, `analysis_admission_not_configured`, canary) or 400 (`source_mismatch`); every admission runs with `statement_timeout` 2 s and `lock_timeout` 1.5 s set LOCAL; and a POST in an org whose analysis scope lock is held (e.g. a purge running) fails after 1.5 s as a 503 `backend_unavailable` without `Retry-After`.

**Operator steps, in order (no values here; they live in 1Password and the private ops repository):**

1. Database (approved operator step): make `bulls96_analysis_admit` `LOGIN` with a new password generated into 1Password, with role defaults `statement_timeout=2s` and `default_transaction_read_only=off` (the RDS activation profile's values). Verify it is still a member of no role and has no members.
2. Hyperdrive: create a config whose origin is the admission role's pooler URL (that password is stored only as the Hyperdrive origin), with caching disabled and mTLS `verify-full` against the same uploaded Supabase CA as the read role's config.
3. GitHub: set `SESSION_READ_ANALYSIS_HYPERDRIVE_ID` on the `session-read-worker` environment to that config's id.
4. IAM: make sure the Worker's `PIPELINE_RAW_S3_*` principal can `GetObject` (and `kms:Decrypt`) the `normalized/session-analysis-v2/*` objects in the raw bucket.
5. Deploy with `analysis=on` (repeat it on every later dispatch). Canary through `READ_API_DIRECTORY_WORKSPACES`.

Out of scope here: the analysis job processor and its evidence reads (canonical S3, not Neon), and the census-seal gate for S3-only sessions.
