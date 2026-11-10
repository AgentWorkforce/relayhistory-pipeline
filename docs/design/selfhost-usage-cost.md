# Self-host usage and cost endpoints (design)

## 1. Status and gates

- **Design only, default-off.** No usage or cost endpoint exists in this repo today. Nothing here is implemented,
  applied, deployed or enabled. A migration apply, flag flip or deploy needs Khaliq's separate, explicit OK
  (AGENTS.md).
- **Scope.** This covers the self-host read API (`SERVICE=session-read-api`, `src/session-read-api/server.ts`) serving
  usage (tokens) and cost (`cost_usd_micros`) for one workspace. Callers are relaycontrol-authenticated (`sh_at_`).
  relayhistory-cloud and RelayAuth are not involved. The same routes could later serve hosted, but that is out of
  scope.
- **Base:** `main` at `c048238c`.

## 2. What exists today

### 2.1 Where cost and usage are computed

- **Per request (event).** `normalizedUsage` in `packages/pipeline/src/lib/pipeline/relayhistory-projection.ts`
  (l.301-420):
  - It reads each harness's token blob and computes `input`, `output`, `reasoning`, `cacheRead`, `cacheCreate`,
    `cacheCreate5m` and `cacheCreate1h`.
  - It also computes `costUsdMicros`: `cost_usd` / `costUSD` / `total_cost_usd` scaled to 6 decimals by exact decimal
    rounding (`scaledRound(cost, 6)`). It is `null` when no cost was reported, and negative or non-numeric values
    reject the whole usage.
  - Every counter is bounded by `MAX_COUNT`.
  - `settleRequests` (l.601-641) puts one request's usage on exactly one carrier event (the least
    `delivery_record_id`), so a request is never double-counted across its events.
- **Per session (directory).** `sessions.session_directory` holds the session totals:
  - `input_tokens`, `output_tokens` and `cost_usd_micros` (cost is nullable: "NULL when no event reported a cost",
    `supabase/migrations/20261025000000_sessions_index.sql` l.66-69, the same rule on `session_directory`);
  - `reasoning_tokens`, `cache_read_tokens`, `cache_create_tokens`, `cache_create_5m_tokens` and
    `cache_create_1h_tokens` (`20261104021000_session_directory_event_metadata.sql` l.19-23).
  - These are written by `writeDirectoryEvents` (`src/hoopdb/session-directory-events.ts` l.295-380) from the
    session's projected events (`totals(...)`, l.240 and l.275-288; cost stays `null` unless some event
    reported one). The flag `event_projection_ready` marks them current. The base directory projector
    (`src/hoopdb/session-directory-projector.ts` l.96-125) copies `session_index` totals only while
    `event_projection_version = 0`.
- **Not in search.** The Postgres search table `search_index.transcript_events`
  (`packages/pipeline/src/search-index/postgres.ts` l.295-308) has **no** usage columns, and neither does the vendored
  ClickHouse `transcript_events`. A per-event usage table exists only as the **dormant** ClickHouse
  `transcript_event_facts` DDL (`src/session-pipeline/output-facts-schema.ts`, `docs/output-digest-clickhouse-design.md`
  section 2.1). Nothing writes it.

### 2.2 Where they are exposed

- **`GET /v1/sessions`** (directory list) returns per session `totalCostUsdMicros`, `totalInputTokens` and
  `totalOutputTokens` (`src/session-read-api/directory.ts` l.351-353). It also returns `totalReasoningTokens: 0`,
  `totalCacheReadTokens: 0`, `totalCacheCreateTokens: 0` and the 5m/1h fields as `null`, **hard-coded** (l.354-358).
  The list SELECT (`DIRECTORY_LIST_SQL`, l.108-115) does not read those columns. Values go
  through `num()`, which makes them JS numbers.
- **Column contract.** `SESSION_DIRECTORY_LIST_COLUMNS` (`src/hoopdb/search-index-contract.ts` l.140-165) includes
  `input_tokens`, `output_tokens` and `cost_usd_micros`.
- **Not migrated.** `/v1/sessions/output` and `/v1/digest/daily` answer 501 (`app.ts` l.911, l.1005). The hosted daily
  digest's `totalCostUsdMicros` and token sums are therefore not served by the pipeline anywhere.
- **No aggregate endpoint.** There is no usage or cost aggregate endpoint.

### 2.3 Self-host dependency

In self-host the directory's event totals are written only when a scope-reconcile writer runs. That is the PG consumer
in **PR #336 (open)**, the ledgered `session-pipeline-consumer`. Without it, the totals stay at whatever
`session_index` carried (0 for AlleyOop-uploaded sessions), `event_projection_ready` stays false, and usage must say
so (section 4.4) rather than report zeros as fact.

## 3. API shape

Two routes, both GET, both behind one flag `RELAYHISTORY_READ_USAGE`. Only exactly `true` enables them, and any other
non-empty value fails startup. Off, neither route is registered, so the catch-all keeps its current answer. The routes
are also gated by the existing directory canary allowlist `READ_API_DIRECTORY_WORKSPACES` (`directoryFor`), with the
same 501 off the allowlist as `/v1/sessions/activity`.

### 3.1 `GET /v1/usage/summary`

Query (each parameter at most once unless noted; unknown parameters give 400):

| Param | Required | Meaning |
|---|---|---|
| `workspace` | yes | must equal the token's workspace (`attestWorkspace`, as every directory route); else 403 |
| `since`, `until` | yes | ISO-8601 instants, inclusive; `until >= since`; span at most `MAX_SESSION_ACTIVITY_DAYS` (92) UTC days, the existing activity bound (`app.ts` l.223) |
| `groupBy` | no | repeatable, at most 2 of `day`, `user`, `source`, `model`, `project`. Default: none (one total row) |
| `user` | no | repeatable, at most 20 (`MAX_LIST_USERS`); filter to sessions whose `user_ids` overlaps |
| `source` | no | repeatable, at most `MAX_LIST_SOURCES` (16); harness filter, `HARNESS_NAME` format |
| `tz` | no | only `UTC` in v1; anything else gives 501 `unsupported_by_backend` reason `usage_tz` |

200:

```json
{
  "workspaceId": "ws_…",
  "since": "2026-11-01T00:00:00.000Z",
  "until": "2026-11-07T23:59:59.999Z",
  "attribution": "session_last_activity",
  "groupBy": ["day"],
  "totals": {
    "sessions": 41,
    "inputTokens": "1203345",
    "outputTokens": "88123",
    "reasoningTokens": "0",
    "cacheReadTokens": "5512000",
    "cacheCreateTokens": "230111",
    "costUsdMicros": "18234567",
    "costReportedSessions": 39,
    "unprojectedSessions": 2
  },
  "groups": [
    { "key": { "day": "2026-11-01" }, "sessions": 6, "inputTokens": "…", "outputTokens": "…",
      "reasoningTokens": "…", "cacheReadTokens": "…", "cacheCreateTokens": "…",
      "costUsdMicros": "…" | null, "costReportedSessions": 5, "unprojectedSessions": 0 }
  ],
  "truncated": false,
  "correlationId": "…"
}
```

Rules:

- **Counters are decimal strings.** Every token and cost value is a decimal string (`^(0|[1-9][0-9]*)$`), never a JS
  number. A workspace sum of `cost_usd_micros` can pass 2^53 at scale. This is the repo's wire convention for bigint
  values (`sourceControlVersion`, `ingest_*` versions). `sessions` and the `*Sessions` counts stay JSON integers
  (session counts, far below 2^53).
- **Cost semantics.** `costUsdMicros` is the sum over sessions with a non-null cost. It is `null` when **no** session
  in the group reported one, matching Neon `session_rollups` and the directory (`sum()` skips NULL, then no
  COALESCE). `costReportedSessions` says how many sessions contributed, so a partial cost is never mistaken for a
  complete one.
- **`unprojectedSessions`** counts sessions with `event_projection_ready = false`. Their counters are included as
  stored, which may be the pointer-level values. The client can then mark the figures provisional. See Q3 for whether
  to exclude them instead.
- **Attribution (v1).** Session-granular: each session counts once, in the `day` of `last_activity_at` (UTC). It
  counts toward the window when `last_activity_at` falls in `[since, until]`. This is exact per session and **not**
  exact per day for sessions that span days. The response says so in `attribution`. Section 4.3 gives the exact-per-day
  v2.
- **`user` grouping.** A session's `user_ids` can hold more than one user (`session_directory.user_ids`, one per
  pointer row's `user_id`, `'unknown'` excluded). v1 counts such a session **once per listed user** under
  `groupBy=user`. It sets `"multiUserSessions": n` on each group it touches, so totals across user groups can exceed
  the workspace total, and the response says when they do. Q4 covers the alternative.
- **`model` grouping.** v1 uses the session's `model` column (the most recent model). Like day attribution, this is
  approximate for sessions that switched models. It is exact in v2.
- **`project` grouping.** `project_id`, with `null` as its own bucket (the digest rule).
- **Bounds.** At most 1000 groups. Beyond that the response sets `truncated: true` and returns the first 1000 in key
  order, and the totals are still complete.
- **Erased and buffered sessions.** Erased sessions (`deleted_at IS NOT NULL`) are excluded. Buffered,
  not-yet-published sessions are **not** counted, because they have no directory row. The response always carries
  `"includesBuffered": false` so nobody reads it as complete for in-flight uploads.

### 3.2 `GET /v1/usage/sessions`

This is a keyset page of per-session usage for drill-down, with the same filters and window as the summary plus
`limit` (default 50, max 200) and `cursor` (the directory list cursor format). Items:

```json
{ "sessionId": "…", "source": "claude", "userIds": ["…"], "projectId": null, "model": "…",
  "firstTs": "…", "lastTs": "…", "inputTokens": "…", "outputTokens": "…", "reasoningTokens": "…",
  "cacheReadTokens": "…", "cacheCreateTokens": "…", "cacheCreate5mTokens": "…" | null,
  "cacheCreate1hTokens": "…" | null, "costUsdMicros": "…" | null, "projected": true }
```

It differs from `GET /v1/sessions` in two ways: every counter column is real (the list's hard-coded zeros are not
reused), and every value is a decimal string. Fixing the list's hard-coded fields is a separate, Cloud-contract-bound
change (`directory.ts` l.333-336 notes the cloud-client 0.2.0 contract) and is out of scope here.

### 3.3 Errors (existing body shape `{ error: { code, message }, correlationId }`)

| Status | Code | When |
|---|---|---|
| 400 | `invalid_request` | missing or invalid `since`/`until`, span over 92 days, a bad `groupBy`, too many values, an unknown param |
| 401 | `unauthorized` / `invalid_token` | relaycontrol refusal (existing mapping) |
| 403 | `forbidden` | `workspace=` differs from the token's, or no `rth:read` |
| 501 | `unsupported_by_backend` | off the canary allowlist (`directory_not_migrated`), or `tz` other than UTC |
| 503 | `relaycontrol_unavailable` | introspection unavailable (existing, `Retry-After: 1`) |
| 503 | `backend_unavailable` | the RPC is missing (42883), a grant is missing (42501), or `statement_timeout` (57014). The response never names which |

## 4. Data source

### 4.1 v1: `session_directory`, through a new definer RPC

```sql
sessions.session_directory_usage_for(
  p_workspace_id text, p_since timestamptz, p_until timestamptz,
  p_group text[], p_users text[], p_sources text[]
) RETURNS TABLE (
  g_day date, g_user text, g_source text, g_model text, g_project text, g_project_null boolean,
  sessions bigint, input_tokens numeric, output_tokens numeric, reasoning_tokens numeric,
  cache_read_tokens numeric, cache_create_tokens numeric, cost_usd_micros numeric,
  cost_reported_sessions bigint, unprojected_sessions bigint, multi_user_sessions bigint
)
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path = pg_catalog, sessions, pg_temp
```

- **Scope.** It starts with `sessions.session_directory_read_api_scope(p_workspace_id)` (034000 item 1). That refuses
  42501 unless the caller is `bulls96_read_api` with both tenant settings bound and the workspace equal to the bound
  one. Org and workspace then come only from the scope, as in `session_directory_activity_for` (034000 l.258-316).
- **Validation.** It validates the same way as `session_directory_activity`: the window is required, at most 366 days
  in SQL (the route enforces 92), `p_group` is a subset of the five names with at most 2 entries, and there are at
  most 20 users and 16 sources (the route's `MAX_LIST_USERS` / `MAX_LIST_SOURCES`).
- **Overflow.** Sums are `numeric`, so a bigint overflow never aborts the query. The route converts them to decimal
  strings and refuses (`backend_unavailable`) any value that is not a non-negative integer.
- **Grouping is fixed SQL.** Each of the five keys has a fixed `CASE WHEN 'day' = ANY(p_group) THEN … END` expression.
  The SQL is static, with no dynamic statement built from input.
- **Index use.** It reads `deleted_at IS NULL` rows by `(org_id, workspace_id, last_activity_at)` through the existing
  `si_ws_recent`-style directory index. If the plan shows a sequential scan at scale, an index proposal goes out of
  band, CONCURRENTLY.
- **Grants.** EXECUTE goes to `bulls96_read_api` alone (and the entry is added to `sessions-function-acl.json`), with
  REVOKE from PUBLIC, anon, authenticated and service_role. No new table privilege.
- **Isolation.** No locks and no writes. It runs in the read API's `BEGIN READ ONLY` tenant transaction with the
  per-transaction `statement_timeout` (2 s).
- **Recheck in code.** Grouped rows carry no tenant columns, so the scope pin is the only tenant control for the
  summary, exactly as for activity. The per-session route (3.2) returns rows with
  `org_id`/`workspace_id`, and those are rechecked (`directoryPage` pattern, `TenantMismatchError` gives a 500 with no
  row data).

`GET /v1/usage/sessions` uses a second definer RPC, `sessions.session_directory_usage_sessions_for(...)`. It has the
same scope and filters, returns the counter columns plus the list's identity columns, and uses the list's keyset
cursor (`last_activity_at DESC, session_id DESC, harness DESC`).

### 4.2 Why not compute it in the read API from `/v1/sessions` pages

Paging every session of a 92-day window through the list RPC to sum it in JS would cost one round trip per 200
sessions. It would also need the read API to hold every row. The definer aggregate keeps the cost in one bounded
statement under the same 2 s budget, and returns only grouped rows.

### 4.3 v2: exact per-day and per-model (new table; later, separately approved)

Exact daily buckets and model splits need per-request usage. Proposal:

- A Postgres sibling of the dormant ClickHouse `transcript_event_facts`, named `search_index.usage_event_facts`. It is
  keyed like `transcript_events`, with forced RLS `tenant_only` (the search tables' policy shape,
  `docs/session-read-api-design.md` "Postgres search prerequisites").
- Columns: `ts`, `user_id`, `source`, `session_id`, `model`, `provider`, `project_id`, the seven counters and
  `cost_usd_micros` (nullable). There is **one row per carrier event** that `settleRequests` chose, so the sum equals
  the directory's.
- Written by the same scope reconcile (#336's writer) in the same transaction as `transcript_events`, and erased with
  the scope.
- With it, `attribution` becomes `"request_time"` and `day` and `model` become exact. The response shape does not
  change, so v1 clients keep working.

v2 is listed for completeness. It needs a migration slot, a writer change and its own review.

### 4.4 Freshness and completeness

- **Freshness.** Directory totals change when the scope reconcile re-projects a session (each new upload). There is
  no extra cache. Reads see committed state.
- **Unprojected sessions.** Without the self-host writer (#336), every session is unprojected. In that state the
  summary still answers, with `unprojectedSessions == sessions`, so a dashboard can show "usage not yet available"
  instead of zeros. A route-level guard is proposed (Q3): when every session in the window is unprojected, return 501
  `unsupported_by_backend` reason `usage_not_projected` rather than a 200 full of provisional zeros.
- **History floor.** `READ_API_COMPLETE_SINCE` applies as for activity. A window starting before the floor answers 501
  `incomplete_history`, because sessions older than the floor may be missing from the directory.

## 5. Auth

- **Token.** `Authorization: Bearer sh_at_…`, accepted only with `RELAYHISTORY_ACCEPT_RELAYCONTROL_TOKENS=true`,
  `RELAYCONTROL_INTROSPECT_URL` and `RELAYCONTROL_INTROSPECT_SECRET`, through the existing
  `src/session-read-api/auth.ts` `readApiVerifier`. This is relaycontrol contract v1.2
  (`packages/pipeline/src/auth/relaycontrol.ts` header).
- **Binding.**
  - `aud` must equal `READ_API_ORIGIN` (normalized origin compare).
  - The request's single `workspace=` is sent to introspection, which must echo it exactly.
  - The org and workspace come **only** from the introspection response.
  - `principal_type` must be `desktop-user`. Any other principal, including relaycontrol `agent` tokens, gets 401.
- **Scope.** `rth:read`, the same as `/v1/sessions` and `/v1/sessions/activity`. No new scope in v1. The only granted
  relaycontrol scopes are `rth:read` and `rth:sync` (`RELAYCONTROL_SCOPE_ALLOWLIST`).
- **No RelayAuth path in self-host.** A self-host runs with only the relaycontrol family configured, and the RelayAuth
  verifiers are unconfigured. The routes are not special-cased per token family: whatever `readApiVerifier` admits is
  admitted.
- **Caching.** This is the introspector's existing cache: an admit is cached at most min(exp, 60 s) and a refusal 5 s,
  so revocation lag is up to 60 s per process (`docs/local-stack.md` "Cache and revocation lag").
- **Who may see whose cost (Q1).** Today any `rth:read` holder of a workspace already sees every session's
  `totalCostUsdMicros` and `userIds` in `GET /v1/sessions`, so workspace and user totals add no new raw exposure. But
  `groupBy=user` turns that into a per-person spend report. The introspection response carries no role or admin claim
  (contract v1.2 fields: `sub, email, principal_type, machine_id, org, workspace, scopes, aud, iat, exp, token_id`). So
  the pipeline cannot restrict per-user grouping to admins without a new relaycontrol scope (for example
  `rth:usage:admin`) added to `RELAYCONTROL_SCOPE_ALLOWLIST`. The proposed v1 default:
  - `groupBy=user` and a `user=` other than the caller's own id are refused with 403 `forbidden` (reason
    `usage_user_scope`) unless that scope is present;
  - totals and non-user groupings stay `rth:read`.

## 6. Tenancy

- **One workspace per request.** It is the token's workspace, attested by `workspace=`. There are no cross-workspace
  or org-wide totals in v1. An org-wide view would have to aggregate across workspaces the caller can read. That needs
  a relaycontrol claim listing them, which does not exist (Q2).
- **DB enforcement, twice:**
  1. The definer RPC pins org and workspace from the transaction settings, and the workspace argument must match.
  2. The read API binds those settings only from the verified identity (`TENANT_SETTINGS_SQL`), in a READ ONLY
     transaction that ends with the request, so a pooled connection never carries them over.
- **Read role.** `bulls96_read_api` must be NOSUPERUSER NOBYPASSRLS. The service checks this at startup
  (`assertReaderRole`).
- **No new data leaves the tenant.** Grouped rows hold only counters and the group key. The user key is a user id
  already present in the list, and the model and project keys are values already present in the list.
- **Logs.** Logs carry the route, outcome, group names, row count and duration. They never carry user ids, project
  ids, model names or counter values.

## 7. Placement in code (for the implementation PR)

- `src/session-read-api/usage.ts`:
  - parses the query (`parseUsageQuery`);
  - calls the RPC through `sqlDirectory`'s runner (the same `TenantRunner` as activity);
  - maps rows to the wire with BigInt-safe decimal strings.
- `app.ts`: two `app.get` routes registered only when `deps.usage?.enabled`, after the directory routes, using
  `attestWorkspace`, `directoryFor` and `directoryFail`.
- `lambda.ts` `readAppFromSecret`: reads `RELAYHISTORY_READ_USAGE` (strict parse) for both entries. It is meaningful
  only where the directory is wired.
- Migration: one new file for the two definer RPCs and their ACL entries, with a rollback in `supabase/rollbacks/` and
  a slot from the registry keeper.

## 8. Test plan

- **PGlite / real PG (synthetic tenants):**
  - Sums equal hand-computed values. NULL cost sessions are skipped, and an all-NULL group gives `null`.
  - Bigint-scale sums do not overflow (numeric).
  - `deleted_at` rows are excluded.
  - A tenant mismatch between the setting and the argument gives 42501, and a non-`bulls96_read_api` caller gives
    42501.
  - Another workspace's rows never appear, even with equal session ids.
  - The multi-user attribution counts are right, and the window edges are inclusive.
- **Unit (route):**
  - Strict params; the 92-day bound; at most 2 `groupBy`; `tz` other than UTC gives 501.
  - `workspace=` attestation; `user=` / `groupBy=user` scope rule (Q1).
  - Counters are always strings, and a JSON number never appears.
  - Off the canary allowlist gives 501, and with the flag off neither route is registered.
  - 42883, 42501 and 57014 give 503 `backend_unavailable` with no relation name in the body.
  - Logs carry no ids or values.
- **Contract:**
  - The per-session route's sum over all pages equals the summary's totals for the same window and filters.

## 9. Open questions

1. **Per-user cost visibility.** Should `groupBy=user` (and `user=` for others) require a new relaycontrol scope (for
   example `rth:usage:admin`)? Who owns adding it to relaycontrol, and to `RELAYCONTROL_SCOPE_ALLOWLIST` here?
   Proposed default: refuse without it.
2. **Org-wide usage.** Org-wide usage across workspaces is out of v1. Is it needed for self-host? If so, what claim
   would list a caller's readable workspaces?
3. **Unprojected sessions.** Should they be included with `unprojectedSessions`, excluded, or should an
   all-unprojected window answer 501 `usage_not_projected`? Proposed: include and count, plus the 501 when all are
   unprojected.
4. **Multi-user sessions.** Under `groupBy=user`, count once per listed user (proposed, flagged with
   `multiUserSessions`), or attribute only to the pointer whose `user_id` matches (per-machine attribution through
   `session_index`)? The second option is exact but needs `session_index` totals, which are pointer-level.
5. **v2.** Is v2 (`usage_event_facts`, exact per-day and per-model) wanted for the first self-host release, or is
   session attribution acceptable? v2 depends on #336.
6. **Path.** Is `/v1/usage/*` the right path, given what relaycontrol's proxy forwards to History? This repo does not
   show the proxy's route allowlist, and the hosted Cloud client calls no such route today.
