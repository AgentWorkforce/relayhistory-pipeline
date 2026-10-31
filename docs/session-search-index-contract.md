# Logical session directory and search contract

Production uses S3 for canonical session content and Supabase for a rebuildable
metadata index (decision 0002). `sessions.session_index` remains one row per
physical machine manifest and holds the object pointer. The additive
`sessions.session_directory` is one row per logical session:

`(org_id, workspace_id, harness, session_id)`.

That split is required for stable pagination. A session resumed on three machines
has three manifest pointers but must occur once on a session list. The projector
(`src/hoopdb/session-directory-projector.ts`, called by the HoopDb indexer) merges
the physical rows into the directory with the provisional precedence described in the
G6 section below. It never sets the rollout marker: backfill and the precedence
decision ("Projector decision still required") remain open, so the audit fails closed.

Cloud must treat agent lookup as a future RelayHistory-advertised metadata
capability. It must not bind directly to this table or assume an `agent_name` API
field until that capability is implemented. The scalar stored here is the delivered
root agent identity; child-agent relationship labels are not session identities.

User and machine filters use `session_directory_users` and
`session_directory_machines`. The arrays on `session_directory` are response
conveniences, not indexed filter inputs. This avoids GIN-plus-sort plans for
recent-session pages.

## Indexed remote query shapes

| Filter or lookup | Index strategy |
| --- | --- |
| workspace or org recent | partial ordered btree with a total logical-session cursor |
| harness/source | workspace + harness + activity cursor |
| exact project or no project | separate partial activity indexes |
| agent exact or prefix | normalized name with `text_pattern_ops`, then activity cursor; served through the RPC so the prefix is an index condition (under RLS `LIKE` is not LEAKPROOF and was only a filter: 1409x row amplification at benchmark size M) |
| created range | workspace + `created_at` cursor |
| state | workspace + state + activity cursor |
| user or machine | normalized membership table + activity cursor |
| session ID without workspace/harness | org + session ID lookup |
| project substring | `sessions.search_session_directory` RPC only, over tenant-scoped trigram GIN `(org_id, workspace_id, project_id)` (replaces #38's `session_directory_project_trgm_idx`) |
| metadata + short summary substring | `sessions.search_session_directory` RPC only, over tenant-scoped trigram GIN `(org_id, workspace_id, search_text)` (btree_gin) |

The executable definitions live in
`src/hoopdb/search-index-contract.ts`. `bun run audit:search-indexes` is
read-only: it checks catalog structure and fails until the singleton
`session_directory_rollout.completed_at` is set by the future projector/backfill
lane. It never creates indexes or marks rollout complete.

## Metadata-only search boundary

`search_text` may contain only title, task title, agent name, project, repository,
branch, and model metadata, plus the scrubbed `short_summary` (below), and is searched
only through the tenant-bound RPC. It must never
contain transcript text, event content, tool payloads, prompts, or responses.
Canonical content remains in S3.

### Short summary (G6)

Provenance of scope: the helm-chart-chief coordinator recorded the user's direct
approval to implement bounded short_summary work in its controlling session. Draft
only; applying or merging needs the user's OK on the exact head. Migration:
`20261030020000_session_directory_short_summary.sql`.

- **Bound:** `char_length(short_summary) <= 1000`, a CHECK in the database and the
  same bound (in code points) in the writer. Over-length text is refused, not cut.
- **Scrubbed only:** `src/hoopdb/session-directory-summary.ts` is the only write path.
  It runs the shared `lib/scrub.ts` scrubber, refuses if the scrub throws or is not
  stable on its own output, and records `short_summary_scrub_ruleset` and
  `short_summary_scrub_version` (the raw-chunk `SCRUB_RULESET` pair) plus
  `short_summary_source` and `short_summary_generated_at`. The database requires those
  labels (all or none) but cannot prove the text was scrubbed: only the writer enforces
  that. An older `generated_at` never replaces a newer summary. A refused summary (`ShortSummaryRejected`) clears any older stored summary and its share of `search_text`, under the same fence, so an older summary never stays searchable after a newer one was refused; a refused older summary leaves a newer one in place.
- **Search document:** `sessions.session_directory_search_text(...)` is the one
  definition of `search_text`; the summary is its last field. The writer rebuilds
  `search_text` with it, and the future projector must use the same function.
- **Projector:** `src/hoopdb/session-directory-projector.ts` (`projectSessionDirectory`) is the create/update path. The HoopDb indexer calls it after every pointer upsert. Precedence is PROVISIONAL until the projector decision below is recorded: created_at is the earliest live pointer, last_activity_at the latest, scalar metadata and counters come from the most recently active live pointer (no summing), memberships are the union, and task_title, agent_name and short_summary are kept. It never resurrects a tombstone and never sets the rollout marker.
- **Summary producer:** Bob Costas's `summary-short` handler calls `writeShortSummary` after publishing the encrypted object (and `clearShortSummary` for any non-ready status). A refused summary is logged by reason only. If the directory write fails after the pointer committed, every retry that finds the summary current authenticates the published object and republishes it, even when the row already holds a summary (it may be an older revision's text). The `generated_at` fence makes this idempotent. If the pointer has no object, or the object is missing or cannot be authenticated, the directory summary is cleared and the stage fails with the non-retryable `summary_object_integrity`, which the stage queue dead-letters at once. If the store or keyring read throws, the directory summary is cleared first and the (retryable) error is rethrown. Each current-summary replay costs one object-store read plus one decrypt.
- **Deletion:** the HoopDb indexer's pointer-tombstone branch (`src/hoopdb/index.ts`)
  calls `propagatePointerTombstone`, which clears the summary and its share of
  `search_text` at once, then `projectSessionDirectory`. With machines still live the
  projector rebuilds metadata and memberships. With none left it tombstones the row,
  empties `machine_ids`/`user_ids` and deletes the membership rows. A trigger plus CHECK is the backstop for any other writer that
  sets `deleted_at`. Erasure (`eraseSessionDirectory`) deletes rows. PostgreSQL WAL,
  replicas and backups keep their own copies until retention ages them out; that is
  accepted for this bounded, scrubbed field and is why nothing larger goes here.
- **Rollback:** `supabase/rollbacks/20261030020000_session_directory_short_summary.down.sql`
  rebuilds `search_text` without the summary, drops the columns, trigger, functions,
  RPC, Worker role and tenant index, and restores #38's index and grants (tested up,
  down, up).

### The search RPC (pinned contract)

`search_text`, and therefore `short_summary`, is searchable only through this RPC.
#38's non-tenant-scoped `session_directory_metadata_trgm_idx` is dropped and replaced
by `session_directory_tenant_search_trgm_idx` on `(org_id, workspace_id, search_text
gin_trgm_ops)`. `authenticated` keeps RLS-scoped SELECT on the listing columns but not
on `search_text` or `short_summary`, so a PostgREST `search_text=ilike.*q*` table
filter is refused. (Under RLS, ILIKE is not LEAKPROOF and could never use a trigram
index anyway.) The executable copy is `SESSION_DIRECTORY_SEARCH_RPC` and
`SESSION_DIRECTORY_SEARCH_SQL` in `src/hoopdb/search-index-contract.ts`.

| Item | Contract |
| --- | --- |
| Function | `sessions.search_session_directory(p_workspace_id, p_query, p_harness, p_project_id, p_project_contains, p_no_project, p_agent, p_agent_match, p_state, p_user_id, p_machine_id, p_since, p_until, p_session_id, p_cursor_last_activity_at, p_cursor_session_id, p_cursor_harness, p_cursor_workspace_id, p_limit)`, every argument defaulted (the first two keep their positions). PostgREST `POST /rest/v1/rpc/search_session_directory`, `Content-Profile: sessions`, a JSON body of named `p_*` arguments. `searchRpcRequest` in `src/hoopdb/search-query.ts` builds it under the same rules |
| Security | `SECURITY DEFINER`, `search_path = pg_catalog, sessions, pg_temp`. The statement is assembled only from fixed fragments chosen by which arguments are set; every value is bound with `EXECUTE ... USING` (one custom plan per call). Runs as the migration owner (table owner, RLS not forced); no BYPASSRLS role is created |
| Caller | only `session_directory_search_worker`: `NOLOGIN NOINHERIT NOBYPASSRLS`, `statement_timeout = 2s`, `USAGE` on `sessions`, `EXECUTE` on this function, no table or column privileges. `EXECUTE` is revoked from PUBLIC, `anon`, `authenticated`, `service_role`, `relayhistory_app` and any grantee present at creation (default privileges). A grant added later (a runner's `GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA sessions`) is still refused: the function itself requires the caller's role to be the Worker (`42501`). The revoke also strips `EXECUTE` from `postgres` or `supabase_admin` if default privileges granted it to them, so ops tooling must not expect to call the RPC; only the Worker, through PostgREST, calls it. Binding it to `authenticator` and issuing its credential are a separate, reviewed deployment step. That binding (`GRANT session_directory_search_worker TO authenticator`) must keep `authenticator` NOINHERIT; with INHERIT, `has_function_privilege` becomes true for `authenticator` through membership. The Worker must stay NOLOGIN: the function's `session_user` fallback relies on it. **Role collision:** the migration's first statement creates the role, or, if it already exists, reuses it only when it is exactly what the migration would create: no LOGIN, INHERIT, BYPASSRLS, SUPERUSER, CREATEROLE, CREATEDB or REPLICATION; a member of no role; no member except a NOINHERIT `authenticator`; no owned objects; no explicit table, column, function, schema, database or default privileges; and no role setting other than `statement_timeout`. Otherwise the migration fails closed with `42710`, naming every violation, before any other DDL runs. The down path drops the role it owns |
| Tenant | from the signed JWT claims only (`request.jwt.claims`, as the RLS helpers read them): `role` must equal `session_directory_search_worker`, `app_metadata.org_id` (string) is the org, and `app_metadata.workspace_ids` (array of strings) the workspaces. `p_workspace_id` must be one of them; `NULL` means organization-wide over exactly the claimed workspaces. The caller cannot name an org. `workspace_ids` must hold 1 to 64 unique strings of 1 to 128 non-space, non-control characters; one bad entry refuses the whole claim. `iss` and `aud` must equal the row in `sessions.session_directory_search_token_policy`, which no migration seeds, so search is refused until an environment adds one. `aud` must be a string, `exp - iat` must be 1 to 60 seconds, and `exp` at most 90 seconds away. Signature verification is PostgREST's |
| Query | One rule, implemented identically in `src/hoopdb/search-query.ts` (`normalizeSearchQuery`, `searchRpcRequest`) and `sessions.session_directory_search_query`: NFC; remove every format character (Cf: ZWSP, ZWNJ, ZWJ, BOM, bidi controls, soft hyphen); collapse runs of separators and controls (Z, Cc: NBSP, U+2000-U+200A, U+3000, tabs, newlines) to one space and trim; NFC again; then at least 3 meaningful code points (not a space, not a combining mark M) and at most 200 in total. The character classes are pinned Unicode 17.0 range tables, rendered into the migration from the TypeScript data (a test pins them). Refused input raises `22023` before the search statement runs. `\`, `%`, `_` are escaped so the term is literal; matched as `search_text ILIKE '%term%'`. PostgreSQL `normalize()` uses the server's own Unicode tables, which may be older than 17.0; for code points the server does not know, NFC may differ from the TypeScript result. The category classes are fixed data and do not depend on the server version, so the 3-character floor holds either way |
| Order and size | `ORDER BY last_activity_at DESC, session_id DESC, harness DESC` (organization-wide: then `workspace_id DESC`); membership filters order by the membership table's copy of the same key, which the projector keeps equal. `p_limit` 1..50 (default 50); at most `p_limit + 1` rows (51), the extra row meaning "more". Cursor: the last returned row's `(last_activity_at, session_id, harness[, workspace_id])`, all required together; `workspace_id` is required organization-wide and must match `p_workspace_id` otherwise |
| Returns | `org_id, workspace_id, harness, session_id, created_at, last_activity_at, title, task_title, agent_name, project_id, git_repo, git_branch, model, state, event_count, input_tokens, output_tokens, cost_usd_micros, machine_ids, user_ids, short_summary, projected_at`; live rows only |
| Errors | `42501` caller is not the Worker role, or tenant claims missing, malformed, for another role, or not covering the workspace; `22023` an invalid value (text under the meaningful-character rule for `p_query`, `p_project_contains` and a prefix `p_agent`; `p_limit` outside 1..50; unknown state, agent match or harness format; an incomplete or mismatched cursor; `p_since` after `p_until`; `p_no_project` with `p_project_id` or `p_project_contains`); `0A000` an intentionally unsupported combination (below); `57014` the 2 s statement timeout |

#### Filters and their semantics

Inventory of today's callers (cloud MCP `search_shared_sessions` and `list_session_timings`,
the dashboard list and hybrid search, Skip history and projects, canvas presets, the iPad
summary search, relay's MCP proxy, and RelayHistory #226's `GET /v1/sessions`) is in the
G6 PR. Every list filter they send maps to an argument; text composes with every filter
and with the cursor, and filters AND together.

| Read API filter | RPC argument | Semantics |
| --- | --- | --- |
| `workspace` / org scope | `p_workspace_id` / `NULL` | workspace equality, or the claimed workspaces |
| `q` (`search=metadata`) | `p_query` | case-insensitive literal substring of `search_text` (metadata + scrubbed `short_summary`) |
| `source` | `p_harness` | exact |
| `project` | `p_project_id` | exact |
| `projectContains` | `p_project_contains` | case-insensitive literal substring; ANDed with `project` when both are given |
| `noProject` | `p_no_project` | `project_id IS NULL` |
| `agent` | `p_agent` + `p_agent_match` | `exact` (default, as legacy RelayHistory) or `prefix` (3+ meaningful characters), on the normalized name |
| `state` | `p_state` | exact |
| `user`, `machine` | `p_user_id`, `p_machine_id` | membership; ANDed when both are given |
| `since` | `p_since` | `last_activity_at >= since` |
| `until` | `p_until` | `created_at <= until`: the session's activity interval overlaps the window, a superset of legacy "has an event at or before until" (#226's `last_activity_at <= until` is narrower and is replaced) |
| session id | `p_session_id` | exact, workspace-scoped or organization-wide |
| `cursor`, `limit` | `p_cursor_*`, `p_limit` | keyset as above |

Intentionally unsupported, returned as `0A000` (never as empty results): an
organization-wide page (`p_workspace_id` `NULL`) with `p_agent`, `p_project_id`,
`p_project_contains`, `p_no_project`, `p_user_id` or `p_machine_id`, because those
indexes lead with the workspace. Not filters here at all: `kind`, `taskRef`,
`noTaskRef`, `tag`, transcript text (`/v1/events?q=`, hybrid ranking) and ordering
other than recency; #226 refuses those with a capability error before the RPC.

Plan evidence (PGlite, 30,000 rows, tenant of 22,500, captured with auto_explain
ANALYZE/BUFFERS/JSON inside the RPC as called by the Worker role): a selective term is
a Bitmap Index Scan on `session_directory_tenant_search_trgm_idx` with no recheck
losses; a common term walks `session_directory_workspace_recent_idx` and stops at 51
rows. Neither plan has a Seq Scan. Buffer and time gates in the test are indicative
for PGlite, not production SLOs; the production benchmark gate above still applies.

### Not in V1 Supabase search

FTS/tsvector, embeddings/pgvector, long summaries, transcripts, and model or reasoning
search belong to the V2 search store (ClickHouse/ObsessionDB), as already decided. G6
adds none of them. Project filters (exact, substring and none) are in V1 because the
current read surface exposes them; the substring is served only through the RPC over
the tenant-scoped project trigram index.

Consequently `search_shared_sessions`, `/v1/events?q=`, and dashboard transcript
search cannot be implemented by silently querying `session_directory`. They remain
unavailable on this data plane until ClickHouse/ObsessionDB is ready. A product may
label a separate metadata search, but it must not claim transcript-search parity.
Do not add a temporary Supabase transcript/tsvector projection based on a claim that
deleting an S3 or per-workspace KMS key crypto-shreds it: PostgreSQL heap pages,
indexes, WAL, replicas, and backups have independent persistence. Transcript storage
requires a recorded, technically viable encryption, deletion, RLS, and backup design
before it can enter this contract.

The legacy relayhistory-cloud `0067_session_catalog_agent_lookup.sql` projection
and its `session_catalog_agents` index are not the production contract. Its
normalization and ordered-prefix lessons remain valid; its event-triggered catalog
projection and legacy rollout marker do not.

## Agent prefix and benchmark gate

Exact lookup accepts a complete normalized name. Prefix lookup requires at least
three Unicode characters after trim/lowercase normalization. Shorter prefixes are
rejected rather than allowed to scan a large tenant directory.

Before a rollout can be marked complete, run exact and prefix queries against a
production-shaped population and record p50/p95 latency plus `EXPLAIN (ANALYZE,
BUFFERS, FORMAT JSON)`. The deterministic gate is:

- page size: 50;
- exact and representative selective prefixes must use
  `session_directory_agent_recent_idx` to constrain the name;
- at least exact, representative three-character, and deliberately broad
  three-character prefixes are measured;
- for a deliberately broad prefix PostgreSQL may instead walk
  `session_directory_workspace_recent_idx` or `session_directory_org_recent_idx`
  and filter in recency order; actual index rows plus `Rows Removed by Filter`,
  divided by returned rows, must be at most 100.

If a real three-character prefix fails that gate, do not relax the limit or hide it
with a faster test host. Raise the minimum prefix, add a proven projection, or move
agent discovery to a dedicated names table. Timing thresholds are environment
specific and belong in the release evidence; the row-amplification bound is the
portable correctness/performance gate.

## Projector decision still required

Before the projector/backfill lane can mark rollout complete, Khaliq must approve
the precedence rules for a logical session whose machine manifests disagree on
title, task title, agent name, project, model, or state. The schema can hold the
result, but it intentionally does not guess whether newest activity, a designated
owner machine, or another source wins. Usage counters also need an explicit
sum-versus-deduplicate rule. Until those rules are recorded, the rollout marker
stays incomplete and the audit fails closed.

## Dimensions that remain local

Relay Desktop also filters local path/directory, upload state, relay presence,
reasoning, and live active/inactive state. Those are local catalog/runtime
dimensions and do not belong in Supabase. Model is searchable metadata, but there
is no remote ordered model-filter contract yet. Adding an API filter requires a
query-plan test before adding another write-amplifying index.
