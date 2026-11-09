# Output and the daily digest off Neon (design, P4)

Status (lane F4, 2026-11-06):
- **Output is implemented and default-off.** The producer is `src/session-pipeline/output-facts.ts`, which `indexSearchScope` calls only with Lambda `SEARCH_INDEX_OUTPUT_FACTS=1`. The query module is `src/session-read-api/output-query.ts`. The route `GET /v1/sessions/output` is served by the read Worker only with `READ_API_OUTPUT_ENABLED=1`.
- The DDL (`output-facts-schema.ts`, schema version 2) is **not applied**. Applying it is an owner step that needs Khaliq's OK.
- Parity: `src/session-pipeline/__tests__/output-facts-parity.test.ts` runs relayhistory's own `getSessionOutput` SQL on Postgres against the facts on ClickHouse, for every window and filter shape.
- **The digest is implemented and default-off (lane F4 PR B).**
  - `GET /v1/digest/daily` is served by the read Worker with `READ_API_DIGEST_ENABLED=1`.
  - The deterministic half is in `src/session-read-api/digest-query.ts`. The window is `day-window.ts`, ported verbatim with relayhistory's DST tests.
  - The narrative is in `digest-narrative.ts`. Its cache is Supabase `sessions.daily_digest_narratives` (migration `20261105045000`, not applied). Generation runs in the service-bound narrative Worker (`packages/session-read-worker/wrangler.digest-narrative.jsonc`, not deployed).
  - `stats.outcomes.reverted` is **omitted**, per Khaliq: "drop and rebuild". It is sent as 0, because the strict 0.2.0 contract requires the key, and declared in `X-Sessions-Digest-Omitted: outcomes.reverted`, which Cloud hides. `committed`/`unknown` keep relayhistory's rules over the frozen legacy outcomes.
  - Follow-up F4-R: a sessions-pipeline session-outcomes producer, to rebuild `reverted`.

Sources read: relayhistory-cloud `c7de2163` (`lib/session-output.ts`, `lib/digest.ts`, `lib/narrative.ts`, `lib/rollups.ts`, `lib/ingest.ts`, migrations 0003/0018/0021/0073); sessions-pipeline main (`vendor/search-index/schema.ts`, `session-pipeline/search-index.ts`, `packages/pipeline/.../relayhistory-projection.ts`, `session-read-api`); Cloud trunk (`packages/web/lib/sessions-read-96bulls.ts`, `app/api/v1/workspaces/[workspaceId]/sessions/output/route.ts`, `dashboard/_components/session-output.tsx`, `lib/skip/history.ts`).

## 1. What Neon does today

### 1.1 `GET /v1/sessions/output` (`getSessionOutput`)
Scope: `org_id` plus an explicit workspace list (the route passes the one attested workspace), optional `source`/`sessionId`, and `since <= t <= until` (both inclusive; the range may span at most 366 UTC days). Response: `{since, until, output: {agentMessages, commits, linesAdded, linesRemoved, commitsWithoutLineCounts}, correlationId}`.

- **events** = `convergence_events` in scope with `ts` inside the window.
- **messages** = `DISTINCT (source, session_id, message_id)` over events where:
  - `actor_role = 'assistant'`, content is not blank;
  - `kind IN ('session_event','assistant','text')`;
  - and either `record.payload.kind = 'text'`, or `payload.kind IS NULL AND type IN ('assistant','text')`.

  `message_id = COALESCE(NULLIF(request_key,''), NULLIF(payload.message_id,''), delivery_record_id, machine_id||':'||event_id)`. The window filter applies before dedup: one message id with events both inside and outside the window still counts.
- **legacy_messages**: `conversation_turns` rows in scope and window, with `role = 'assistant'` and non-blank content, whose `(source, session_id)` has no message in the window.
- **commit_candidates** (a UNION ALL of three sources):
  1. `session_commit_links`: `lower(commit_sha)`, `COALESCE(linked_at, received_at)`, `numstat`. **Not** filtered by the window.
  2. `session_outcomes`: `lower(commit_sha)`, `COALESCE(shipped_at, created_at)`, `numstat_json`. **Not** filtered by the window.
  3. Tool results in the window whose content matches Git's success header plus shortstat (`commitOutput` regex). The match needs exactly one logical matching `tool_call`, all of whose shell commands run `git commit`. That call is matched by `tool_use_id`, by parent/message id, and by `turn_id` through a parent `session_event`.
- **Prefix resolution**: an abbreviated sha (1–39 chars) maps to the one 40-char sha in the candidate set with that prefix, if it's unique. Otherwise it stays as is. Resolution runs over **all** candidates in scope, so out-of-window commits can disambiguate in-window abbreviations.
- **Stats normalization**: a stats value can be an array of entries, a single entry object, or an object of entries. Additions and deletions are each summed separately. A side becomes NULL if any entry on it isn't digits, `-`, or a binary entry (path set, both counts JSON null).
- **commits** = group by resolved sha: `min(ts)`, `max(added)`, `max(removed)`. Then the window is applied to `min(ts)`.
- **Result**:
  - `commits` = count of commits;
  - `linesAdded` / `linesRemoved` = sum of `added` / `removed`;
  - `commitsWithoutLineCounts` = commits with either side NULL;
  - `agentMessages` = messages + legacy_messages.

The same function feeds `session-analysis/request-processor.ts` (brief inputs), so a port should be one shared query module.

### 1.2 `GET /v1/digest/daily` (`buildDailyDigest` + `resolveNarrative`)
- **Window**: one civil day in an IANA `tz`, half-open `[since, until)`, computed by `resolveDayWindow`. It handles DST gaps and ambiguities and is pure, so it can be ported verbatim. Scope: org, optional token workspace, optional exact `project_id`.
- **stats** over `convergence_events` in the window:
  - `sessionCount` = distinct (source, session); `eventCount` = `count(*)`;
  - `projectCount` = distinct project, NULL counted as its own bucket;
  - `epicCount` = distinct (project, branch). `branch` is the text after the first `@` of `task_ref.id`. It's NULL for `{}` or when there's no `@`;
  - `totalCostUsdMicros` = sum of cost, NULL skipped, then COALESCE 0;
  - input/output token sums;
  - `models` = distinct non-null `model`;
  - `filesTouchedCount` and `topFiles` (10, by distinct sessions desc then path) over `files_touched` arrays; a non-array value counts as `[]`;
  - `outcomes`: each of the day's (source, session) is `reverted` if any of its `session_outcomes` rows is reverted, `committed` if it has rows and none reverted, and `unknown` otherwise. This is org-bound, and workspace-bound when the digest is workspace-scoped.
- **projects** and **epics**: grouped rollups (≤50 each), ordered by `max(ts) desc`, with `firstTs`/`lastTs`. The epic `epicKey` is the newest non-null `task_ref.id` in the group.
- **sessions**: `listSessions` for the window (until − 1 ms), i.e. the session list. In 96Bulls that's the P3b Supabase directory (`session_directory_list`).
- **narrative**:
  - `statsHash` = sha256 of `{workspaceId?, stats, projects, epics, sessions}`;
  - the cache is `sessions.daily_digests` (Neon), keyed `(org, day, tz, scope_key)`;
  - on a cache miss (or `refresh=true`) the route calls the extraction model and **writes** the cache. A quiet day is `no_activity` with no model call. So a non-refresh read is a read plus a possible write.

### 1.3 Who writes the inputs
| Input | Neon writer today | After the upload cutover |
|---|---|---|
| events (model, tokens, cost, project, task_ref, files) | delivery projection triggers | S3 canonical streams. The pipeline projection (`relayhistory-projection.ts`) computes every one of these fields, but `toIndexRow` drops them because `transcript_events` has no such columns. |
| `session_commit_links` | delivery projection of `commit_link` records | `commit_link` delivery records in S3; the projection emits `session_commit_links` rows |
| `session_outcomes` | legacy `POST /v1/ingest` only (`upsertSessionOutcome`: strongest match method wins, newest write for facts) | frozen in Neon. The PR 2 legacy export writes them to S3 as `legacy_session_outcome` records. There is no live writer unless `/v1/ingest` still has traffic (port-out plan R5). |
| `conversation_turns` | `POST /v1/sessions/:id/turns` | PR 1 forwards new turns to the ingest; PR 2 exports history as `legacy_conversation_turn` |
| `daily_digests` | digest route (cache) | needs a new home (see 6) |

## 2. Proposed ClickHouse tables

DDL: `src/session-pipeline/output-facts-schema.ts` (`outputFactsSchemaStatements()`). These are two **sibling** tables. `transcript_events` stays unchanged: its DDL is vendored verbatim from relayhistory-cloud, and its reconcile fingerprint covers exactly its columns, so adding columns would fork the vendored schema and mark every row as changed.

### 2.1 `transcript_event_facts`
- **Rows**: one per projected event, with the same identity and `ORDER BY` as `transcript_events`: `(org_id, workspace_id, ts, session_id, source, machine_id, kind, event_id)`.
- **Columns**: `project_id`, `task_ref_id`, `branch` (all Nullable: NULL is a real bucket), `model`, `provider`, the five token counters (Int64), the two cache-write splits and `cost_usd_micros` (Nullable, so "not reported" is not zero), and `files_touched Array(String)`.
- **Serves**: every digest aggregate except outcomes and sessions.
- **Engine**: `ReplacingMergeTree(version)` (`version` = epoch ms of the S3 read, as in transcript_events), `PARTITION BY toYYYYMM(ts)`. Readers use `FINAL` (or `LIMIT 1 BY` identity ordered by version).
- **Time**: `DateTime64(6)`, because Neon is microsecond and both routes compare against caller instants. transcript_events uses (3), and that's fine for search but not for exact counts at a boundary.

### 2.2 `session_output_facts`
- **Rows**: one per fact. `fact_kind` is one of:
  - `message`: one row per qualifying event, `fact_key` = message id;
  - `legacy_turn_message`: one per qualifying assistant turn, `fact_key` = turn id;
  - `commit`: one per candidate, with `origin` ∈ `commit_link`, `outcome`, `tool_result`, and `fact_key` = `lower(sha)` (possibly abbreviated), plus normalized `added`/`removed`;
  - `outcome`: one per `session_outcomes` row, with `reverted`.
- **Why dedup and prefix resolution happen at read time**: both depend on the query scope (the set of candidates and the window), so they can't be precomputed per session without changing results.
- **Why the per-row filters happen at write time**: the qualification filters, the tool-call uniqueness check and stats normalization all depend only on the session scope. The indexer has the whole scope in memory when it projects.
- **Key**: `ORDER BY (org_id, workspace_id, fact_kind, ts, source, session_id, fact_key, origin, row_key)`, `PARTITION BY toYYYYMM(ts)`, with bloom filters on `session_id` and `fact_key`.
- **Scan cost**: commit candidates from links and outcomes must be read for the whole tenant, as in Neon. They're small: one row per commit.

### 2.3 Retention-ready
Both tables are tenant-first and month-partitioned, and carry `retention_days UInt16 DEFAULT 0`, where 0 means keep. No TTL is active. Enabling a per-plan TTL later is one reviewed `ALTER ... MODIFY TTL toDateTime(ts) + toIntervalDay(retention_days) WHERE retention_days > 0`, plus the producer stamping the plan's value. Erasure is the same scope-wise lightweight DELETE the indexer already issues for transcript_events.

### 2.4 Read queries (sketch, exact semantics of 1.1)
```sql
-- messages + legacy messages (window inclusive both ends)
WITH m AS (SELECT DISTINCT source, session_id, fact_key FROM session_output_facts FINAL
           WHERE org_id={org} AND workspace_id IN {ws} AND fact_kind='message' AND ts BETWEEN {since} AND {until}),
     l AS (SELECT fact_key FROM session_output_facts FINAL
           WHERE org_id={org} AND workspace_id IN {ws} AND fact_kind='legacy_turn_message' AND ts BETWEEN {since} AND {until}
             AND (source, session_id) NOT IN (SELECT source, session_id FROM m))
SELECT (SELECT count() FROM m) + (SELECT count() FROM l);

-- commits: all link/outcome candidates + windowed tool results, prefix-resolved, grouped, then windowed on min(ts)
WITH c AS (SELECT fact_key AS sha, ts, added, removed FROM session_output_facts FINAL
           WHERE org_id={org} AND workspace_id IN {ws} AND fact_kind='commit'
             AND (origin <> 'tool_result' OR ts BETWEEN {since} AND {until})),
     full_ids AS (SELECT DISTINCT sha FROM c WHERE length(sha)=40),
     lens AS (SELECT DISTINCT length(sha) AS n FROM c WHERE length(sha) BETWEEN 1 AND 39),
     uniq AS (SELECT left(f.sha, lens.n) AS prefix, any(f.sha) AS sha FROM full_ids f CROSS JOIN lens
              GROUP BY prefix HAVING count() = 1),
     g AS (SELECT if(u.sha != '', u.sha, c.sha) AS sha, min(c.ts) AS ts, max(c.added) AS added, max(c.removed) AS removed
           FROM c LEFT JOIN uniq u ON u.prefix = c.sha GROUP BY sha)
SELECT count(), sum(added), sum(removed), countIf(added IS NULL OR removed IS NULL) FROM g WHERE ts BETWEEN {since} AND {until};
```
- `DISTINCT sha` in Neon is over all candidates; the `full_ids` set above is the 40-char subset, which is the only one `unique_prefixes` reads, so the two are equivalent.
- `LEFT JOIN` in ClickHouse yields `''` rather than NULL for a missing String unless `join_use_nulls=1`. The port must set `join_use_nulls=1` or test `u.sha != ''` as shown.
- The digest aggregates are straight `GROUP BY`s over `transcript_event_facts FINAL` with the day's half-open window. `count(distinct coalesce(project_id, chr(1)))` becomes `uniqExact(ifNull(project_id, '\x01'))`, and likewise for the `\x02` pair.
- Outcomes: the day's (source, session) set from event facts, then `outcome` facts grouped by session with `max(reverted)`.

## 3. Producer (implemented: `output-facts.ts`)
`session-pipeline/search-index.ts` already reconciles one session scope from S3 into `transcript_events` under the Supabase advisory lock. The plan is to extend that same pass, so the three tables can never disagree about a scope:
1. From the projected events, emit one `transcript_event_facts` row per event. The fields come from `projectRelayhistorySession` (model, provider, tokens, cost, project, task_ref, files_touched), and `branch` is derived exactly as `branchSql`.
2. Emit `message` facts with the qualification and the COALESCE id. Emit `commit` facts from the projection's `session_commit_links` rows and from tool results: port the regex and the call-uniqueness check from `session-output.ts`, using the scope's tool calls.
3. From the scope's `legacy_session_outcome` records (PR 2 export), emit `outcome` facts and `origin='outcome'` commit facts. From `legacy_conversation_turn` records and PR 1 live turn records, emit `legacy_turn_message` facts.
4. Reconcile by fingerprint as for transcript_events: insert what's new or changed, delete what S3 no longer has, and on erasure delete the whole scope. An incomplete read never deletes.

As built:
- Identity deletes go by `row_id`, the sha256 of the identity.
- `session_output_facts.scope_source` carries the harness, so that a legacy turn whose stored `source` differs is still erased with its scope.
- Facts cover the whole scope, not the transcript_events window.
- **Facts state.** Every pass with the flag on records the scope in `output_facts_session_state`, complete or not, with the facts' time span. The table is separate from the search `needs_full`, so a facts problem never degrades transcript search.
  - The state is written **pending (complete=0) before** the ClickHouse insert/delete, and final after. A pass that dies mid-reconcile therefore leaves the scope incomplete, never complete over torn facts. Version = 2 × pass ms + phase.
  - A malformed legacy record or timestamp marks the facts inexact; it never throws out of the pass.
- **Route readiness** (`outputWindowReadiness`) is positive and window-scoped:
  - `output_backfill_incomplete`: no watermark row.
  - `output_before_watermark`: `since` is earlier than `complete_since`.
  - `output_session_partial`: an inexact session whose span overlaps the window. An unknown span overlaps every window.
  - `output_session_unindexed`: a session with transcript events in the window but no complete facts-state row. A missing row is never read as complete.
  - Sessions entirely outside the window never block it.
- **Erasure** purges the fact tables whenever they exist, **independent of the flag**, so turning the producer off never leaves an erased session's facts behind. The S3 tombstone → `indexSearchScope` erasure pass covers the upload-erase path too: C3/#289 tombstones the pointers, and this pass purges ClickHouse.
- **One key.** Every fact kind (event, message, tool-result commit, commit link, outcome, legacy turn) is stored under the scope's **canonical session id + harness**. The event's own ids stay in the identity for dedup; a legacy turn keeps its stored `source` for relayhistory's NOT EXISTS. The facts scope lists every session id the pass saw (the scope's, the canonical, and any owner/alias id on a projected event). That one set is used by the state rows, the existing-row read, the delete and erase filters, and the route's readiness join.
- **Versions** are strictly increasing per scope: base = max(2 × ms, previous + 1), rounded up to even; pending = base, final = base + 1. Fingerprints are the full sha256.
- **DDL upgrades.** `outputFactsUpgradeStatements()` (`ALTER … ADD COLUMN IF NOT EXISTS`) upgrades a table created by an older copy; apply it after `outputFactsSchemaStatements()`. A missing table or column on the read path is 501 `output_schema_missing` / `digest_schema_missing`.
- **Parity oracle.** It stays relayhistory's SQL transcribed into the test. `getSessionOutput` is drizzle TypeScript, and no vendored Neon function exists to run. `scripts/output-digest-parity.ts` is the live dual-run once both URLs are set.
- **Schema guard.** With the flag on but the DDL absent, or an older skeleton (which `CREATE TABLE IF NOT EXISTS` never alters), nothing is written. The pass reports `output_facts_schema_missing`, and search is unaffected.
- **Residual.** A session that has no transcript rows in the window (e.g. older than the transcript window) and no facts at all is invisible to the readiness check. The backfill watermark covers it: the backfill marks a workspace only after every live scope has a state row.

`session_outcomes` dedup across exported copies: the Neon key is `(org, workspace, source, session, commit_sha)`, and the export carries the row as it stands (strongest method already applied), so the indexer needs no merge rule. `row_key` = the sha.

## 4. Read Worker routes
The dormant stubs keep their 501s until each condition holds. Both routes stay behind `READ_API_DIRECTORY_WORKSPACES` (501 `*_workspace_not_enabled` off the allowlist) with the P3b workspace attestation and strict-contract response.

- **`/v1/sessions/output`** (implemented):
  - 501 `output_workspace_not_enabled` off `READ_API_DIRECTORY_WORKSPACES` (`*` = all);
  - 501 `output_not_configured` without `READ_API_OUTPUT_ENABLED=1`;
  - 501 `output_backfill_incomplete` until the workspace has a row in `output_facts_workspace_state`. The operator backfill writes that row once every scope of the workspace has been re-read with the flag on; the read path never writes it;
  - 501 `output_session_partial` if any in-range session is `needs_full`;
  - then the two queries in 2.4;
  - same 400s as relayhistory: `workspace is required`, `since and until are required exactly once`, `Invalid output date range`.
- **`/v1/digest/daily`** (implemented; the bullets below were the original plan, superseded where noted):
  - As built:
    - Gates: 501 `digest_workspace_not_enabled`, `digest_not_configured`, `digest_backfill_incomplete` and `digest_session_partial`, as for Output.
    - `sessions`: window aggregates from `transcript_event_facts` (new v3 columns `task_ref_json`, `task_title`, `user_id`). Title, workState and catalog come from the directory row (`list_session_directory_for` by session key). `summary` has no first-user-turn fallback yet.
    - Narrative: a cache hit is served. On a miss, a stale hash or `refresh=true`, the narrative Worker is called with a 3 s deadline, below Cloud's 4 s read timeout. Every narrative failure is a 200 with `narrative.unavailable`, never a 501 (no fallback after GA).
  - deterministic parts from facts plus the P3b directory list;
  - 400s copied from `resolveDayWindow` (port it verbatim, with its DST tests);
  - the narrative is served only from a cache keyed like `daily_digests` whose `stats_hash` equals the recomputed hash. A miss is 501 `digest_narrative_uncached`, so Cloud falls back to relayhistory, which generates and caches in Neon. That stays until the cache and the model call move (see 6). `no_activity` needs no cache and is served directly;
  - `refresh=true` never reaches 96Bulls (Cloud's classifier already keeps it on relayhistory). *Superseded:* the read API serves `refresh=true` by regenerating through the narrative Worker. Cloud routes it only with `SESSIONS_READ_96BULLS_DIGEST_REFRESH=on`.
- **Cloud**: no code change. `sessions-read-96bulls.ts` already classifies `sessions_output` and `digest_daily`, parses `sessions_output` with the dashboard's `sessionOutputSchema` and the digest with cloud-client 0.2.0, and supports `shadow` then `on`, with every 501 falling back.

## 5. Backfill and parity
1. Apply the DDL (owner, ClickHouse prod) in a reviewed window.
2. Deploy the producer extension (default-off flag on the S3 indexer, `SEARCH_INDEX_OUTPUT_FACTS=on`).
3. Re-run the existing per-scope backfill that re-reads S3 for every live pointer, for the canary workspace first and then all, throttled like the 2026-11-05 ClickHouse ingest re-enable. This needs PR 2's export of `session_outcomes` and `conversation_turns`. Without it, Output undercounts commits that only `session_outcomes` knew, and legacy-only turn messages.
4. Run the read-only parity checks in 7, then Cloud `sessions_output=shadow`, then `digest_daily=shadow` for the canary workspace. Shadow logs one content-free `equal` flag per read. Go to `on` only after a sustained all-equal shadow.

Expected shadow differences are mostly explainable:
- S3 is more complete than Neon after the freeze (sessions active since the cutover);
- `/v1/ingest`-only events in Neon that S3 lacks (export coverage);
- the narrative (501 until cached).

Any other inequality blocks.

## 6. Open questions
1. **Live `session_outcomes`**: after the freeze, does anything still post outcome envelopes to `/v1/ingest`? If so, they need a delivery kind or an ingest route, as PR 1 did for turns. Reverts (`reverted`, `reverted_by_sha`) have no other producer, so the digest's `reverted` count would stop moving.
2. **Narrative cache home**: a Supabase table (migration ≥ 20261105030000; owner apply), plus the extraction-model secret on an SP Worker. Alternatively, drop the cache write from the read path and accept an uncached narrative as `provider_not_configured`. That second option is a contract-visible change, so it needs Khaliq's call.
3. **Source of truth for `session_commit_links` in Output**: the projection's rows from S3, or the Supabase catalog (`writeDirectoryCatalog`)? Using the same projection pass (proposed) avoids a cross-store race.
4. **Sharing with session analysis**: Output also feeds brief generation (`request-processor.ts`). When analysis moves to SP it should call the same CH query module.
5. **ClickHouse version features**: confirm `uniqExact` over tuples and `join_use_nulls` on the production version (CI runs 26.8).
6. **`eventCount` scope**: Neon counts every convergence_events row, including legacy `/v1/ingest` kinds. Confirm that the neon-s3-sync export's `convergence_event` direct rows cover them, or parity will differ on old days.

## 7. Operator steps (Khaliq; none are run by merging)
1. **Review and apply the DDL** to ClickHouse prod as the owner, then verify it read-only:
   ```sql
   SELECT name, engine, partition_key, sorting_key FROM system.tables
    WHERE database = currentDatabase() AND name IN ('transcript_event_facts','session_output_facts');
   SHOW CREATE TABLE transcript_event_facts;   -- expect no TTL clause
   ```
2. **Neon read-only baselines** for the canary workspace and day (role `relayhistory_readonly`):
   ```sql
   -- output inputs
   SELECT count(*) FROM sessions.session_outcomes WHERE org_id=$1 AND workspace_id=$2;
   SELECT count(*) FROM sessions.session_commit_links WHERE org_id=$1 AND workspace_id=$2;
   SELECT count(*) FROM sessions.conversation_turns WHERE org_id=$1 AND workspace_id=$2 AND role='assistant' AND btrim(content)<>'';
   -- digest stats for one day [since, until)
   SELECT count(*) AS events, count(DISTINCT (source, session_id)) AS sessions,
          coalesce(sum(cost_usd_micros),0) AS cost, coalesce(sum(input_tokens),0) AS input, coalesce(sum(output_tokens),0) AS output
     FROM sessions.convergence_events WHERE org_id=$1 AND workspace_id=$2 AND ts >= $3 AND ts < $4;
   ```
3. **After the backfill, the matching ClickHouse reads**:
   ```sql
   SELECT count() AS events, uniqExact(source, session_id) AS sessions, coalesce(sum(cost_usd_micros),0) AS cost,
          sum(input_tokens) AS input, sum(output_tokens) AS output
     FROM transcript_event_facts FINAL WHERE org_id={o:String} AND workspace_id={w:String} AND ts >= {s:DateTime64(6)} AND ts < {u:DateTime64(6)};
   SELECT fact_kind, origin, count() FROM session_output_facts FINAL WHERE org_id={o:String} AND workspace_id={w:String} GROUP BY fact_kind, origin;
   ```
   The `outcome` count must equal the Neon `session_outcomes` count, `commit/commit_link` the `session_commit_links` count, and the day's events/sessions/cost/tokens must match exactly. Then compare `GET /v1/sessions/output` and `/v1/digest/daily` bodies from both backends for the same token.
4. **Cloud flags**: `SESSIONS_READ_96BULLS_ROUTES=…,sessions_output=shadow,digest_daily=shadow` with the canary on `SESSIONS_READ_96BULLS_WORKSPACES`. Watch `[sessions-read-shadow]` `equal=true`, then switch to `on`.
5. **Rollback** at any point: remove the route from `SESSIONS_READ_96BULLS_ROUTES`. The tables are derived and can be dropped and rebuilt.
