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
- **What this avoids:** there is no `/v1/cli/login` exchange, no `rth_at_` token and no `auth_sessions` lookup. So there is no dependency on relayhistory-cloud #245 (auth to Supabase); #245 is still needed for desktop uploads.
  - Coordination note for the Neon-retirement lead: reads stop using `rth_` tokens entirely once their routes are switched.
- **Cloud side:**
  - The token is minted per (user, org, workspace) and cached in the isolate for up to 45 s.
  - Cloud keeps its existing access check (`resolveRelayhistoryWorkspaceAccess` / `assertCloudMayReadHistory`) **before** minting. A customer-hosted workspace is never routed to 96Bulls.

## 3. Data access and isolation

- **ClickHouse:** a read-only ClickHouse user (`SELECT` on `relay_search.transcript_events` only; created by the lead), reached through the vendored fetch client (`vendor/search-index/clickhouse.ts`).
  - Every query binds `org_id = {org}` and `workspace_id = {ws}` from auth as typed parameters, never interpolated.
  - `max_execution_time = 2` and `max_result_bytes` keep queries bounded.
  - Every returned row is rechecked in code (org, workspace, session, source). A mismatch raises `TenantMismatchError` and a 500, logged as `crossTenantRows: true` with no row data.
- **Supabase (phase A):** a new LOGIN role `bulls96_read_api` with `SELECT` on `sessions.session_index` columns `(org_id, workspace_id, harness, session_id, created_at, deleted_at)` only. No content, no writes, `statement_timeout = 2s`. It ships as a bootstrap SQL file plus a verify check in this PR and is applied by the lead.
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
     - A workspace search's first page returns 501 `session_partial` while any session of the org is partial. The state table has no workspace column, and a never-indexed partial session has no row to attribute it with.
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

## 6a. Internal directory search (company brain)

`GET /v1/directory/search?workspace=<wks>&q=<text>[&limit=1-50][&cursor=b96d1:…]` serves one internal consumer, the `company` Cloudflare Worker (AgentWorkforce/company-brain), for the Agent Relay workspace only. It is not part of the relayhistory contract, so it never answers 501: there is no backend to fall back to.

- **Auth:** the same RelayAuth assertion as every `/v1` route (RS256, `aud` = `READ_API_ORIGIN`, `rth:read`). `workspace` must equal the token's `wks` (403 otherwise). The tenant is never taken from a parameter, and any parameter other than `workspace`, `q`, `limit` and `cursor` is a 400.
- **Allowlist:** the route is closed (403 `directory_not_enabled`) unless the token's workspace is in `READ_API_DIRECTORY_WORKSPACES`, which the stack sets only from `transcriptStore.sessionReadApi.directorySearchWorkspaces`. Unset, empty or malformed means closed for everyone. A malformed value closes only this route; the event routes keep serving.
- **Match:** case-insensitive literal substring (`ILIKE … ESCAPE '\'`, with `%`, `_` and `\` escaped) over `title`, `task_title`, `git_repo` and `search_text`. `q` is trimmed and whitespace-collapsed, and must be 3 to 200 characters, the same bound as `search_session_directory`.
- **Response:** pointers only, `{ sessions: [{ session_id, workspace_id, title, git_repo, last_activity_at, state }], nextCursor, correlationId }`. Rows are mapped field by field, so no other column can reach a response. `last_activity_at` is a microsecond ISO UTC timestamp.
- **Paging:** newest first. The keyset is `(last_activity_at, session_id, harness) DESC`, the workspace recency index's order. `limit + 1` rows are fetched; a `nextCursor` means more rows exist.
- **Visibility, in one statement, run as `bulls96_read_api` in a READ ONLY transaction with the tenant settings bound:**
  - tenant: `org_id` and `workspace_id` are bound as parameters **and** fenced by the role's RLS policies on `session_directory` and `session_index`. Every returned row is rechecked in code (500 `cross_tenant_rows` on a mismatch).
  - tombstone: `session_directory.deleted_at IS NULL`.
  - erasure: at least one live `session_index` pointer (`deleted_at IS NULL`). This is stricter than the event routes, which treat "no pointer at all" as live because ClickHouse history can predate pointers. A directory row is always projected from live pointers, so this only closes the window between a pointer tombstone (or erasure) and the projector running.
  - sharing mode: see below.
- **Why not `sessions.search_session_directory`:** the RPC refuses every caller except the role `session_directory_search_worker`, and takes the tenant from `request.jwt.claims`. Only `authenticator` may be a member of that role, because any member could forge those claims with `set_config`. Using it would mean adding a second member edge (a HoopDb security-model change, section 3), granting a new EXECUTE (the frozen `sessions-function-acl.json` manifest and the function-boundary event trigger), or adding a new function. It also returns `short_summary`, token counts and user and machine ids, and it does not encode sharing-mode rules either. A column grant plus an RLS policy is the smaller change: no function, no EXECUTE, no membership.

### Sharing mode

Sharing mode is **not modelled** in `session_directory`, `session_index` or any other server-side table. It is Relay Desktop's upload consent (`relay-desktop/docs/agent-relay-probe.md`, "sharing set … --mode all|new|selected"): `all` shares every session that is not excluded, `new` withholds the sessions its baseline excluded, and `selected` shares only member sessions. What a mode withholds is never uploaded, so it never reaches the directory. Everything the directory holds was shared with its workspace, which is exactly what the existing `session_directory_member_read` policy gives every workspace member.

The strictest reading available server-side is what the route enforces: one workspace (the token's), never org-wide; live pointers only; tombstoned and erased rows excluded; pointers only.

**Open question for the lead:** `sessions exclude` on the desktop "does not erase already uploaded history". So a session that was uploaded and later excluded stays in the directory and is served here, as it already is to every workspace member through Cloud. If a later exclusion must hide a session from the company brain, the desktop has to send a server-side signal (an erasure, or a new per-session flag), and this route has to read it. That is not built yet.

### Grant

`supabase/bootstrap/session_read_api_directory.sql`, applied after `session_read_api_role.sql`: column SELECT on exactly `org_id, workspace_id, harness, session_id, title, task_title, git_repo, search_text, last_activity_at, state, deleted_at`, and the policy `session_directory_read_api` (FOR SELECT TO `bulls96_read_api`, the same tenant binding as `session_index_read_api`). `search_text` is used only in `WHERE`, but PostgreSQL has no filter-only privilege, so the role can read it. It is the metadata search document (title, task title, agent, project, repository, branch, model and the scrubbed `short_summary`), never transcript content. `short_summary` itself, counters, `user_ids`, `machine_ids` and every catalog column stay unreadable. `session_read_api_role_verify.sql` checks this exact shape. It also now refuses:
- any role that is a member of `bulls96_read_api`, except the creator edge to `postgres`, because a member such as `authenticator` could act as the role;
- a permissive policy for PUBLIC on either table;
- a policy expression other than exactly the tenant binding. The check used to be a substring match, which an `OR true` passed.

## 7. Open decisions for the lead

1. **Supabase read role:** approve `bulls96_read_api` (SELECT on six `session_index` columns) for phase A.
2. **Phase C binding:** PostgREST JWT or a second member edge for the claim-bound RPCs. This goes to the HoopDb owner.
3. **Phase B schema:** pipeline writes an `event_json` column (the #242 shape) into a `transcript_session_events` table ordered by `(org_id, session_id, ts…)`. This goes to the #121 owner.
4. **`READ_API_COMPLETE_SINCE`:** the production value, from the backfill state.
5. **Directory search and sharing mode:** whether a session that was excluded after upload must disappear from directory search (section 6a, "Sharing mode").
