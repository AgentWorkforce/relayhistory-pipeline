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
| D | `search=hybrid`, `q` on lists/activity | dashboard search, public API | ClickHouse session-grouped search + Supabase directory keyword | gap 1 and gap 3 in the coverage audit |

Phase A gives agents (MCP) and Skip a Neon-free path first. It is the hottest content read, and its fields fit `transcript_events` exactly: Cloud's `EventSchema` (`lib/mcp/shared-sessions.ts:113`) is non-strict and reads only eventId, sessionId, source, kind, type, ts, userId, actor*, subagentId, projectId, machineId, taskTitle, content, contentTruncated, toolName and toolStatus.

Anything Phase A cannot answer exactly gets **`501 unsupported_by_backend`** with a `reason`, and Cloud falls back to relayhistory. The service never sends a best-effort answer.

## 1. Hosting

> **Superseded (2026-11-01): the API moves to a Cloudflare Worker,** `packages/session-read-worker`, so that no Lambda is on the request path. The account's Lambda concurrency limit is 10, and the Lambda runs without reserved concurrency. The Worker runs the same `src/session-read-api` code. Supabase is reached through Hyperdrive; the reasons are in `runbooks/sessions-prod-session-read-worker.md`. The Lambda below stays deployed only until the Worker is `on` for every route.

> **Self-host SERVICE (2026-11-07).** The API is no longer Lambda/Worker only: the self-host services image runs the same `src/session-read-api` app as `SERVICE=session-read-api` (`src/session-read-api/server.ts`, config in `service-config.ts`). It is default-off. `RELAYHISTORY_SELFHOST_READ_API` unset, empty or `false` is dormant: the process listens, `GET /healthz` answers 200 `{"ok":true,"enabled":false}` and every other route answers 503 `not_configured`. Exactly `true` enables; any other value fails startup. `RELAYHISTORY_READ_API_PORT` (default 8080) is parsed even when dormant. Enabled, the app is assembled once at startup by the Lambda's own `readAppFromSecret` under the service entry's rules (`entry: "service"`), from `RELAYHISTORY_READ_API_SECRET`. That variable holds the same JSON the Lambda reads from Secrets Manager, injected from the secret store. The rest of the env is the Lambda's: `READ_API_COMPLETE_SINCE`, `READ_API_ORIGIN`, `RELAYAUTH_*`, the relaycontrol switch, `SESSION_POSTGRES_TLS_PROFILE` and `ALLOW_IN_CLUSTER_HTTP` (the transport rule). A bad value fails startup. Search is selected by `SEARCH_INDEX_BACKEND` through the adapter registry, never hard-wired: `postgres` (the service default) composes the registry's Postgres `SearchIndexAdapter` with epoch-fenced Postgres completeness state (`createPostgresSearchComposition`, backend id `SEARCH_INDEX_POSTGRES_BACKEND_ID`, coverage from `READ_API_COMPLETE_SINCE`), so the secret needs no `clickhouse` block and no ClickHouse client is created; `clickhouse` keeps the hosted binding. Any other name, including one with no implementation in this build (`opensearch` today), fails startup with an error naming the variable, never the value. `PIPELINE_RUNTIME` is not this process's concern (any known value). Unlike the Lambda, the service requires `SESSION_POSTGRES_TLS_PROFILE` explicitly (no implicit Supabase pin) and hands pg discrete host/port/database/user/password fields from the sanitized URL, so nothing in its query string reaches pg; a URL without user, password or database fails startup. Directory and liveness stay on the Postgres read role, as on the Lambda. Readiness of the Postgres search state (its writer recording under the same backend id and coverage) is an operator prerequisite; until then search answers partial-session refusals rather than incomplete results. `/healthz` answers `{"ok":true,"enabled":true}`, and the read API routes are GET only, as on the Lambda and the Worker, with one exception: `POST /v1/cli/login` (exact path; a trailing slash or longer path is 405). Its JSON body is the only request body the service reads. The entry buffers it with an 8 KiB cap that mirrors the app's login `bodyLimit`, and checks both `Content-Length` and the bytes received. Over the cap it answers 413 `body_too_large` without calling the app and closes the connection. Any other non-GET request answers 405 `method_not_allowed` with `Allow: GET`, or `Allow: GET, POST` on the login path. Dormant, `POST /v1/cli/login` is 503 like every other route and its body is never read. SIGTERM stops accepting connections, drains in-flight requests (bounded at 10 s), then closes the read pool. Nothing is deployed by this: no chart, stack or migration changes.

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
- **Cloud side:**
  - The token is minted per (user, org, workspace) and cached in the isolate for up to 45 s.
  - Cloud keeps its existing access check (`resolveRelayhistoryWorkspaceAccess` / `assertCloudMayReadHistory`) **before** minting. A customer-hosted workspace is never routed to 96Bulls.

## 2a. Self-host login and catalog (`RELAYHISTORY_SELFHOST_READ_API`, default off)

A self-host's relaycontrol proxies History reads with a user's `sh_at_` token. Two routes serve it, registered only when `RELAYHISTORY_SELFHOST_READ_API` is exactly `true` (unset, `""` or `false`: neither exists and every other route is unchanged; any other value fails startup). Code: `src/session-read-api/selfhost.ts`, `app.ts`, `auth.ts` `relaycontrolLogin`.

- **`POST /v1/cli/login`**, body `{agentRelayToken, label?}` (nothing else; 8 KiB max, 413 `body_too_large`). Bad shapes are 400 `invalid_body` / `invalid_agent_relay_token` / `invalid_label`. The token is introspected exactly as a relaycontrol read bearer (§2, `aud` = `READ_API_ORIGIN`, no workspace), so it needs `RELAYHISTORY_ACCEPT_RELAYCONTROL_TOKENS=true` with the URL and secret:
  - off: 503 `relaycontrol_not_enabled`; URL, secret or origin missing: 503 `relaycontrol_not_configured`; unreachable or malformed: 503 `relaycontrol_unavailable` (all with `Retry-After: 1`);
  - not an `sh_at_` token, wrong principal or no workspace: 401 `invalid_token`; inactive: 401 `token_inactive`; expired: 401 `token_expired`; other audience: 401 `wrong_audience`; no `rth:read`: 403 `forbidden`.
  - workspace not on the reads' canary allowlist (`READ_API_DIRECTORY_WORKSPACES`, checked after introspection): 501 `unsupported_by_backend` reason `login_workspace_not_enabled`, no workspace header;
  - 200: `{accessToken, accessTokenExpiresAt, refreshToken, refreshTokenExpiresAt, tokenType: "Bearer", scopes}` with `X-Relayhistory-Workspace-Id`.
  - `scopes` is the token's scopes intersected with the scopes this read-only backend serves (`rth:read`), never `rth:sync`, so relaycontrol refuses `mode=sync` against it; a grant left with no served scope is 403 `forbidden` (`missing required scope: rth:read`) from the handler, checked after the canary allowlist and expiry (with `relaycontrolLogin`, a token without `rth:read` already gets the same 403 during introspection, before the allowlist).
- **Pass-through, no minting.** `accessToken` is the presented token. Nothing is signed, stored or cached beyond the introspector's existing ≤ 60 s positive cache, so there is no new credential or long-lived secret, and every later read re-authenticates the token through the same introspection (revocation is seen within that cache window). `accessTokenExpiresAt` = min(relaycontrol `exp`, now + 15 min). The cap is advisory to the client: the server keeps accepting the token until relaycontrol's own expiry or revocation.
- **No refresh.** There is no refresh route. `refreshToken` repeats the access token with the same expiry only to fill the shape; a client logs in again (relaycontrol's proxy re-logs-in on 401). The `label` is validated and otherwise unused. Token values are never logged or echoed.
- **`GET /v1/sessions/:sessionId/catalog?workspace=&source=`**: authenticated and workspace-scoped like `/timing` (same canary allowlist, `workspace=` must equal the token's, else 403; `source=` exactly once). Body `{workspaceId, catalog, parents, children, commitLinks, truncated}`, read from the session's canonical S3 projection (`session_catalog`, `session_catalog_agents`, `session_relationships` with this session as parent, `session_commit_links`). Items carry Cloud's camelCase column keys; delivery provenance (record and origin ids) is dropped; the newest delivery wins; each array is capped at 500 (`truncated`). The catalog response exposes the session's `cwd`, `firstPrompt` and `workspaceRoots` to any `rth:read` holder of the workspace (the same scope that can read the session's events).
  - `parents` is always `[]`: a parent edge is delivered with the parent session's records, and the Postgres relationship directory is service-role only (a read path would need a migration).
  - No pointer for the session: 404 `not_found` (reason `catalog_no_pointers`), which relaycontrol maps to an empty catalog; erased: the empty catalog. The 404 is in-workspace only: it is answered strictly after the workspace attestation (`workspace=` must match the token, else 403; absent, 501 `org_scope`) and the canary allowlist (else 501 `catalog_workspace_not_enabled`) pass, so it is never a cross-workspace existence oracle. `/timing` and `/turns` keep their 501 `*_no_pointers`. An entry without a canonical source (the Lambda today) answers 501 `catalog_not_configured`. The Lambda Function URL handler is GET-only, so login there is 405; it is meant for the self-host server entry.

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
     - A session read's first page returns 501 `session_partial`.
     - A workspace search's first page returns 501 `session_partial` while a partial session belongs to the token's workspace. The state table has no workspace column, so a partial session is attributed to the workspace when it has any row (any version) in that workspace's `transcript_events`, or a live `session_index` pointer there. The pointer covers a session whose every pass was partial and so wrote no row. A session with neither belongs to another workspace, or is erased here (its rows are dropped anyway). Partial sessions of other workspaces of the org no longer stop the search.
     - Scoping to the candidate page alone would not be sound: the events a partial session is missing from ClickHouse are never candidates, so their matches would vanish silently.
     - The org's partial sessions are listed up to 1000. More than that is not decided, and the search returns 501 `session_partial_unbounded`.
     - The check runs only when pagination starts. A cursor continues the read it admitted: a 501 on a cursor page would end the caller's pagination in an error or restart it with duplicates.
     - The ClickHouse read user also needs `SELECT` on `search_ingest_session_state`.
5. **Cursors** are opaque and namespaced: `b96e1:<base64url(JSON [tsMs, sessionId, source, machineId, kind, eventId])>`.
   - A relayhistory cursor (`events-v2:`) sent to 96Bulls gets 501 `foreign_cursor`.
   - Cloud keeps pagination sticky by prefix: a `b96e1:` cursor is never sent to relayhistory, which would silently restart at page one. The service answers a cursor page with a 200 (a terminal page carries `continuation` when relayhistory must finish it, see the history floor above). On a transport error or 5xx with such a cursor, Cloud returns the error and the caller retries the same cursor.
6. **Search** (`/v1/events?q=`) is workspace-scoped only (org-wide gets 501), newest first, `limit` ≤ 50.
   - `q` must be at least 1 character. Literal `%`, `_` and `\` are fine, because `positionCaseInsensitiveUTF8` is literal.
   - `asc` with `q` gets 501.
   - Filters `taskRef`, `tag`, `agent`, `projectContains` and `noProject` get 501 in phase A (no caller in phase A sends them).

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

- Brief, analysis, turns, digest and onboarding.
- Hybrid search.
- Cloud's direct `sessions.*` SQL readers.
- Delivery and auth for uploads.

These are tracked in the neon-readers inventory lanes.

## 7. Open decisions for the lead

1. **Supabase read role:** approve `bulls96_read_api` (SELECT on six `session_index` columns) for phase A.
2. **Phase C binding:** PostgREST JWT or a second member edge for the claim-bound RPCs. This goes to the HoopDb owner.
3. **Phase B schema:** pipeline writes an `event_json` column (the #242 shape) into a `transcript_session_events` table ordered by `(org_id, session_id, ts…)`. This goes to the #121 owner.
4. **`READ_API_COMPLETE_SINCE`:** the production value, from the backfill state.
