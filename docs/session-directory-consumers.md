# session_directory consumer inventory (planning artifact)

Status: **HOLD, planning only.** No access is revoked and no code path is activated by
this document. It is the first artifact of the surface-closure follow-up
(helm-chart-chief's ruling on the direct `project_id` ILIKE finding).

## Sources reviewed

| Source                                                  | Ref            | State                                                                               |
| ------------------------------------------------------- | -------------- | ----------------------------------------------------------------------------------- |
| 96BullSessions #38 `feat/session-directory-contract`    | `9688d63e0f5b` | MERGED to `main` at `9688d63e` (2026-10-31T06:42Z); migrations not applied anywhere |
| 96BullSessions #59 `feat/search-benchmark`              | `4102156f2e6f` | open, not draft                                                                     |
| 96BullSessions #72 `feat/session-directory-token-pins`  | `f3141b6601d1` | open, draft                                                                         |
| 96BullSessions #73 `test/session-directory-postgrest`   | `2a75466c3293` | open, draft                                                                         |
| 96BullSessions #74 `feat/aws-proof-harness`             | `9b4fc6be5b71` | open, draft                                                                         |
| 96BullSessions #75 `feat/aws-proof-hardening`           | `5c1ee5e941bf` | open, draft                                                                         |
| relayhistory-cloud #226 `codex/p3-phase-attested-reads` | `c4b6cf7aad74` | open, draft                                                                         |
| relayhistory-cloud `main`                               | `45d53b18c670` | default branch                                                                      |
| cloud `main`                                            | `ec7e0ecb2e9c` | default branch                                                                      |
| relay `main`                                            | `e759d2ead565` | default branch                                                                      |
| relay-desktop `main`                                    | `1b2856ce7572` | default branch                                                                      |

Method: read-only `git grep` of every 96BullSessions remote branch and of #226; read-only
GitHub code search of AgentWorkforce. Code search indexes default branches only, so
unmerged branches outside 96BullSessions and #226 are not covered. Line numbers are from
the refs above.

**Deploy state.** None of the reviewed refs records a deployment that reads
`session_directory`. #226 states that its adapter "remains inert before the admitted
terminal phase" (`docs/96bulls-terminal-reads.md`). Every directory reader below is
therefore an intended reader in reviewed draft source, not an observed production reader.

## Gate result: FAILS

- **Token-dependent rows** (H1, H3, H4, H5, and the indirect routes that reach them) are
  classified **unknown pending provenance**: `SESSION_READS_TOKEN`'s PostgreSQL role is
  not recorded in code or config.
- **Unowned consumer**: Relay Desktop has no owning lane (helm-chart-chief: it stays
  UNOWNED).

## Routes

Columns: **Path** = direct table read, RPC, indirect (through #226) or write. **Token**
= the credential class the read runs under.

### relayhistory-cloud #226 (`packages/relayhistory/src/lib/read-plane/`), owner: own_rhc_226_fix lane

| Id  | Route                                                                        | Path                                                                 | Token                                                                                                                                         | Query, filters, projection                                                                                                                                                                                          | Fields needed                                                    | Order, pagination                                                                                                            | SLO                              | Flags                           |
| --- | ---------------------------------------------------------------------------- | -------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------- | -------------------------------- | ------------------------------- |
| H1  | `PostgrestDirectoryStore.list`, no `q` and no agent (`postgrest.ts:103-155`) | direct table (`session_directory`, or `_users`/`_machines` `!inner`) | `SESSION_READS_TOKEN`, **unknown pending provenance**                                                                                         | org; workspace optional; harness; project eq; `projectContains` as `project_id=ilike.*v*`; project null; state; `last_activity_at` gte/lte; user or machine; keyset `or`. 22 explicit columns (`postgrest.ts:9-32`) | 22 listing columns incl. `project_id`; no `short_summary`        | `last_activity_at, session_id, harness [, workspace_id]` desc; `limit + 1`, up to `MAX_SESSION_LIMIT = 500` (`recall.ts:60`) | none recorded                    | inert before the terminal phase |
| H2  | `list` with `q` or agent (`#search`, `postgrest.ts:198-272`)                 | RPC `search_session_directory`                                       | per-request RelayAuth worker token (`/v1/tokens/session-directory-search`, `postgrest.ts:52,275`), plus the static token as `apikey` (`:250`) | every `p_*` filter and the cursor; `p_limit = limit - 1`; refuses org-wide                                                                                                                                          | all RPC columns incl. `short_summary` (dropped later, see below) | RPC keyset, `limit` 2..51                                                                                                    | RPC role `statement_timeout` 2 s | same                            |
| H3  | `get` (detail) (`postgrest.ts:158-176`)                                      | direct table                                                         | `SESSION_READS_TOKEN`, **unknown**                                                                                                            | org + session id [+ workspace] [+ harness], `limit=2`, null if ambiguous                                                                                                                                            | 22 columns                                                       | at most 2 rows                                                                                                               | none recorded                    | same                            |
| H4  | `pointers` (`postgrest.ts:178-196`)                                          | direct table (`session_index`)                                       | `SESSION_READS_TOKEN`, **unknown**                                                                                                            | full key, `deleted_at=is.null`; 13 `POINTER_COLUMNS`                                                                                                                                                                | pointer columns                                                  | `machine_id` asc                                                                                                             | none recorded                    | same                            |
| H5  | `rolloutComplete` (`postgrest.ts:94-100`)                                    | direct table (`session_directory_rollout`)                           | `SESSION_READS_TOKEN`, **unknown**                                                                                                            | `completed_at`                                                                                                                                                                                                      | `completed_at`                                                   | none                                                                                                                         | none recorded                    | same                            |

The events routes (`routes.ts` `eventsRoute`) also call `store.get`, so they depend on H3.

### cloud (`packages/web`, `ec7e0ecb2e9c`), indirect through #226

Every cloud read goes through `lib/relayhistory-reflex.ts` and `lib/relayhistory-private.ts`
(the History read binding), which call relayhistory-cloud's `/v1/*` routes. None of them
touches Supabase. Owners: cloud.

| Id  | Route                                                                                                                        | Calls                                                | Params                                                                    | Lands on                                               | SLO           |
| --- | ---------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------- | ------------------------------------------------------------------------- | ------------------------------------------------------ | ------------- |
| C1  | Dashboard list `app/dashboard/sessions/session-list.ts:74-93`                                                                | `GET /v1/sessions`                                   | `limit=50`, `workspace`, `cursor`; `q` ≤500                               | H1; with `q` refused (`transcript_search_unavailable`) | none recorded |
| C2  | Dashboard hybrid search (same file)                                                                                          | `GET /v1/sessions?search=hybrid`                     | `q` ≥2, `limit` 1..25                                                     | refused                                                | 30 s timeout  |
| C3  | MCP `list_session_timings` `lib/mcp/session-timings.ts:41-56`                                                                | `GET /v1/sessions`                                   | `since` (required), `until`, `source`, `project`, `cursor`, `limit` 1..20 | H1                                                     | none recorded |
| C4  | Skip `list_sessions` `lib/skip/history.ts:56-67`                                                                             | `GET /v1/sessions`                                   | `workspace`, `limit=50`, `q`, `project`, `source`, `cursor`               | H1; with `q` refused                                   | none recorded |
| C5  | Skip projects `lib/skip/projects.ts:529-534`                                                                                 | `GET /v1/sessions`                                   | `workspace`, `limit=50`, `since`, `cursor`                                | H1                                                     | none recorded |
| C6  | Canvas presets `lib/mcp-events/canvas-presets-data.ts:31-34`                                                                 | `GET /v1/sessions`                                   | `limit=50`, `workspace`, `cursor`, up to 6 pages                          | H1                                                     | none recorded |
| C7  | MCP `search_shared_sessions` `lib/mcp/shared-sessions.ts:732-790`                                                            | `GET /v1/events?q=`                                  | `q`, `limit` 1..20, `source`, `project`, `since`, `until`, `cursor`       | refused (transcript search is V2)                      | 30 s timeout  |
| C8  | MCP `get_shared_session`, `get_shared_session_context`, `list_team_activity` (`shared-sessions.ts`)                          | `GET /v1/sessions/:id/events`, `/v1/events?session=` | session, source, cursor, `maxContent`                                     | H3 through `eventsRoute`                               | 30 s timeout  |
| C9  | Session overview and pull request (`app/dashboard/sessions/[sessionId]/session-overview-data.ts`, `session-pull-request.ts`) | `/v1/events?session=`                                | session, source, `kind`; `taskRef` refused                                | H3                                                     | none recorded |

### Relay Desktop (`relay-desktop`, `1b2856ce7572`), owner: UNOWNED

| Id  | Route                                                                                                      | Path                                                                    | Notes                      |
| --- | ---------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------- | -------------------------- |
| D1  | macOS and Linux search (`probe/src/linux_app/models/session_query.rs`, `probe/src/relay/session_query.rs`) | local SQLite only                                                       | never calls the directory  |
| D2  | iPad summary search (`ipad/native/Sources/SummarySearch.swift:81-111`)                                     | indirect: cloud `/api/v1/workspaces/:id/sessions` with `ranking=hybrid` | lands on C2, refused today |

### relay (`e759d2ead565`), owner: relay

| Id  | Route                                                          | Path                   |
| --- | -------------------------------------------------------------- | ---------------------- |
| R1  | MCP proxy `packages/cli/src/cli/mcp/shared-sessions-client.ts` | proxies C7 and C8 only |

### 96BullSessions

| Id  | Consumer                                                                                  | Ref and state          | Path                    | Token               | Notes                                                                                                                                                                    | Owner                 |
| --- | ----------------------------------------------------------------------------------------- | ---------------------- | ----------------------- | ------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------- |
| B1  | Bob Costas `summary-short` (`src/bob-costas/handler.ts`)                                  | #38 (merged `9688d63`) | write                   | HoopDb owner        | `writeShortSummary` / `clearShortSummary` by full key                                                                                                                    | 96BS (G6)             |
| B2  | HoopDb indexer projector (`src/hoopdb/index.ts`, `session-directory-projector.ts`)        | #38                    | write                   | HoopDb owner        | INSERT/UPDATE of directory and memberships                                                                                                                               | 96BS (G6)             |
| B3  | Search index audit (`src/hoopdb/search-index-audit.ts`)                                   | #38                    | catalog, rollout        | operator connection | read-only CLI                                                                                                                                                            | 96BS (G6)             |
| B4  | `SESSION_DIRECTORY_QUERIES` (`src/hoopdb/search-index-contract.ts`)                       | #38                    | direct table, as tested | `authenticated`     | imported only by `session-directory-plan`, `-rls-prefix` and `-search-parity` tests; exact agent only, no prefix query (prefix is RPC-only, `directAgentPrefix` refuses) | 96BS (G6)             |
| B5  | Benchmark Lane C (`benchmarks/session-search/queries.ts` `listQueries`, `run.ts` `laneC`) | #59 `4102156`, open    | direct table            | `authenticated`     | Lane A uses the RPC and `SESSION_DIRECTORY_SEARCH_SQL` as owner                                                                                                          | #59 (miyaontherelay)  |
| B6  | AWS proof probes (`src/proof/aws/probes.ts:66-100`)                                       | #74 `9b4fc6b`, draft   | owner read + RPC        | operator; Worker    | **broken:** `:68` checks `to_regprocedure('sessions.search_session_directory(text,text)')`, a signature the 19-argument RPC replaced                                     | self-hosted-lead      |
| B7  | AWS hardening service account (`stacks/aws/hardening.ts:141`)                             | #75 `5c1ee5e`, draft   | RPC (declaration)       | Worker              | —                                                                                                                                                                        | self-hosted-lead      |
| B8  | Real-PostgREST isolation tests                                                            | #73 `2a75466`, draft   | RPC                     | Worker              | self-hosted profile: the authenticator can assume only the Worker; direct table reads refused                                                                            | #73 (miyaontherelay)  |
| B9  | Token pins                                                                                | #72 `f3141b6`, draft   | RPC                     | Worker              | issuer, audience, lifetime                                                                                                                                               | sp-w6-supabase-schema |

## Semantic gaps between today's direct reads and the RPC

| Behavior                    | Direct (#226 H1/H3)                                                                | RPC                                                                                                                                                                              | Consequence for migration                                                                                                                                                               |
| --------------------------- | ---------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `until`                     | `last_activity_at <= until` (`postgrest.ts` `lte`)                                 | `last_activity_at <= until`, inclusive, `timestamptz` (#38 `7816a07`)                                                                                                            | no gap: the RPC now matches the legacy direct filter exactly                                                                                                                            |
| Short project substrings    | `projectContains` of any length                                                    | `p_project_contains` needs 3+ meaningful characters (`22023`)                                                                                                                    | 1- and 2-character `projectContains` become errors                                                                                                                                      |
| Project filters combined    | `project`/`projectContains`/`noProject` overwrite one `project_id` key (last wins) | ANDed; `no_project` with either is `22023`                                                                                                                                       | silent drops become explicit                                                                                                                                                            |
| Org-wide vs workspace       | org-wide with any filter                                                           | org-wide over the claimed workspaces only; with agent, project, no_project, user or machine is `0A000`; #72 tokens claim exactly one workspace                                   | org-wide pages need one call per workspace, or a broader token design                                                                                                                   |
| Detail lookup ambiguity     | `limit=2`, null when ambiguous; can omit workspace and harness                     | `p_session_id` returns up to `p_limit + 1` rows, only within claimed workspaces                                                                                                  | #226 keeps its "exactly one" rule; an org-scoped lookup without a workspace needs the workspace in the token                                                                            |
| Lost bearer                 | static token, works without a caller                                               | needs a per-request RelayAuth subject token; local History tokens fail closed (`postgrest.ts:275-290`)                                                                           | callers without a caller bearer (background or service paths) lose directory reads                                                                                                      |
| `short_summary`             | not projected                                                                      | returned                                                                                                                                                                         | #226 still drops it (next section)                                                                                                                                                      |
| Agent filter                | `agentPrefix` only (prefix)                                                        | `exact` by default; prefix only through the RPC (`p_agent_match => 'prefix'`), at most 5,000 candidates, else `54000 agent_prefix_too_broad`; non-ASCII handled by SQL `lower()` | #226 must pass `p_agent_match` explicitly; the direct surface refuses every prefix (`agent_prefix_requires_rpc`), so #226's `agentPrefix` must go through `#search`, as it already does |
| Limit                       | up to 500                                                                          | 1..50                                                                                                                                                                            | #226 clamps to 50                                                                                                                                                                       |
| Users and machines together | machine dropped (users table wins)                                                 | ANDed                                                                                                                                                                            | silent drop becomes correct                                                                                                                                                             |

## `SESSION_READS_TOKEN` role: unknown pending provenance

The token was not read, decoded or fetched. Code and config only:

| Question             | Finding                                                                                                                            | Evidence (#226 `c4b6cf7`)                                                                                                                                                                                                                              |
| -------------------- | ---------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Mint path            | none in either repository                                                                                                          | `git grep SESSION_READS` matches only `src/env.ts:63-65`, `src/lib/cutover/gate.ts:820-823`, `src/lib/read-plane/routes.ts:61-63,89`, tests and docs; nothing in `scripts/deploy-cloudflare.mjs`, `wrangler.jsonc` or `.github/workflows/deploy-*.yml` |
| Issuer and key class | not recorded; sent as both `apikey` and bearer on table reads, and as `apikey` beside the worker bearer on RPC calls               | `postgrest.ts:336-337`, `:250`                                                                                                                                                                                                                         |
| Role-claim template  | none                                                                                                                               | the only minted token is the RelayAuth search worker token (`postgrest.ts:52,275`)                                                                                                                                                                     |
| Secret wiring        | Worker secret supplied out of band                                                                                                 | as above                                                                                                                                                                                                                                               |
| PostgREST mapping    | hosted Supabase maps the bearer's `role` claim; #73's self-hosted profile refuses direct table reads for every role but the Worker | #73 `integration/session-directory-postgrest/session-directory-postgrest.test.ts:381-396`                                                                                                                                                              |
| Tests                | synthetic strings only                                                                                                             | `test/cutover-gate.test.ts:54`, `test/phase-read-plane.test.ts:977`                                                                                                                                                                                    |
| #226's statement     | "The static metadata-reader's deployed role is not inferred"                                                                       | `docs/96bulls-terminal-reads.md:232-234`                                                                                                                                                                                                               |

Observation, not a classification: `session_directory_rollout` grants nothing to
`authenticated` or `anon` (#38 `20261030010000`), so H5 would fail under either. The
role stays **unknown pending provenance** until it is recorded.

Proposed live check (not run; needs Khaliq's OK). In a process that already holds the
token, print only the role:

- for a JWT, base64url-decode the payload and print `payload.role`;
- for a new-style key, print only its class prefix (`sb_publishable` or `sb_secret`).

The output is one word, and no credential material leaves the process.

## #226 `short_summary` end to end

1. `#search` casts the RPC rows as `DirectoryRow[]` (`postgrest.ts:272`). `short_summary`
   is present at runtime but absent from `DirectoryRow` (`contracts.ts`), and the direct
   projection (`postgrest.ts:9-32`) does not select it.
2. `sessionSummary` (`routes.ts:511-550`) sets `summary: row.task_title ?? row.title` and
   `summarySource: row.task_title ? "task_title" : null`. It never reads `short_summary`.
3. So no #226 response carries the scrubbed short summary.
4. **Ruling (helm-chart-chief): an additive, nullable `shortSummary`, end to end.**
   - `summarySource` and its provenance are unchanged. It stays
     `z.enum(["task_title", "first_user_turn"])` (relayhistory-cloud
     `packages/cloud-client/src/contract/recall.ts:75,127`), and `summary` keeps its
     current derivation.
   - #226's API row: `DirectoryRow` gains `short_summary: string | null`. The direct
     projection, while it exists, selects it where the grant allows; the RPC already
     returns it.
   - #226's adapter response: `sessionSummary` adds `shortSummary: row.short_summary ?? null`.
   - cloud-client contract: the session schema adds `shortSummary: z.string().nullable()`
     as an additive field.
   - Consumers may prefer `shortSummary` for display and fall back to `task_title` /
     `first_user_turn` (the existing `summary`).
   - Tests: a response with `shortSummary` present, with `shortSummary: null`, and a
     legacy response without the field, each accepted by the contract and rendered
     with the right fallback. An RPC-row-to-response parity test covers the field.

   Owner: own_rhc_226_fix lane (#226 and the cloud-client contract).

## Agent prefix: RPC only (#38 `fa3272d`)

- The direct list surface serves exact agent names only. Every prefix is a typed
  `agent_prefix_requires_rpc` refusal before any SQL, and `SESSION_DIRECTORY_QUERIES`
  has no prefix query.
- The RPC counts a prefix's candidates with an ordered walk of
  `session_directory_agent_recent_idx`, stopping at 5,001. A broader prefix is
  `54000 agent_prefix_too_broad` (use a longer prefix or an exact agent), never a
  tenant scan.
- PG15 and PG17 evidence: #38 `docs/evidence/session-directory-plans.md`.
- #226's H2 (`#search`) already routes `agentPrefix` to the RPC. It must map `54000` to
  a 4xx telling the caller to refine the prefix.

## Expand then contract

**Expand, on `main` via #38 (merged at `9688d63e`; additive only, not applied to any database)**

- Migration `20261031001000_session_directory_reads_expand.sql` adds
  `sessions.session_directory_pointers` and `sessions.session_directory_readiness`, with the
  shape below.
- Project substring was already in the RPC (`p_project_contains`, tenant project trigram
  index inside the RPC identity).
- Nothing is revoked. There is no index, no `short_summary` change and no `service_role`
  fallback.
- Retained PG15/PG17 plans: #38 `docs/evidence/session-directory-plans.md`.

**Contract, later, as its own separate forward migration (not in #38, not written, not applied)**

- Revoke direct SELECT on `session_directory`, `_users`, `_machines`, `session_index` and
  `session_directory_rollout` from `authenticated` and from the static reader's role.
- Gated behind #226's flagged cutover with shadow parity, the migration of the remaining
  consumers to the pointer and readiness functions, and direct-use proof (the rollout steps
  below). Merging #38 does not start this phase.

## Compatibility plan (proposed; nothing applied or activated)

**Principles**

- No static `service_role` key anywhere in the read path.
- Every directory, pointer and readiness read runs under the per-request, claim-bound
  Worker identity.

**New functions** (each `SECURITY DEFINER`, `search_path = pg_catalog, sessions, pg_temp`,
fixed SQL with binds, EXECUTE only to `session_directory_search_worker`)

| Function                                                                                      | Replaces | Shape                                                                                                                                                    |
| --------------------------------------------------------------------------------------------- | -------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `sessions.session_directory_pointers(p_workspace_id text, p_harness text, p_session_id text)` | H4       | Tenant from the verified claims (`42501` otherwise). Exactly #226's 13 pointer columns for live pointers, ordered by `machine_id`, with a fixed row cap. |
| `sessions.session_directory_readiness()`                                                      | H5       | Role-guarded and claims-checked like the search RPC. Returns only `completed_at`.                                                                        |
| existing `sessions.search_session_directory`                                                  | H1, H3   | Unchanged contract.                                                                                                                                      |

**Rollout, consumer first**

1. #226 adds the RPC paths for H1, H3, H4 and H5 behind a flag. The flag is off by
   default, so the direct paths stay in place.
2. Shadow parity: with the flag in shadow mode, each request issues both reads and compares
   fields, order and cursors, excluding the semantic gaps above, which are reviewed one by
   one. Mismatches are recorded as counts and keys, never values.
3. Flip the flag per environment once shadow parity is clean over an agreed window.
4. **Direct-use proof before any revoke:** PostgREST and database logs (`pg_stat_statements`
   or request logs) show zero direct `session_directory`, `_users`, `_machines`,
   `session_index` and `session_directory_rollout` reads by any consumer role over the agreed
   window. Fix the B6 probe and move B5 Lane C to the RPC first.
5. Revoke direct `SELECT` on those tables from `authenticated` and from whatever role the
   static token turns out to be, in a forward migration.

**Rollback** (ordered, so least privilege is restored before the flag flips back)

1. Re-grant only the column-limited, RLS-bound privileges from `20261030020000` (no
   `service_role`, no table-wide SELECT).
2. Then flip #226's flag back to the direct paths.
3. The RPCs stay in place throughout, so either step can be repeated.

## Owners

- **own_rhc_226_fix lane:** relayhistory-cloud #226 (H1 to H5) and the cloud-client
  `shortSummary` contract change.
- **self-hosted-lead:** AWS proof harness and hardening (#74, #75; B6, B7), including the
  B6 presence-probe fix.
- **96BullSessions database-contract lane:** the new `session_directory_pointers` and
  `session_directory_readiness` functions.
- **UNOWNED:** Relay Desktop (D1, D2). Nothing changes there today, but iPad search lands on
  a refused route.
