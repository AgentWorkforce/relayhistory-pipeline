# The session list from the session directory

Migration: `supabase/migrations/20261101050000_session_directory_list.sql`.
Rollback: `supabase/rollbacks/20261101050000_session_directory_list.down.sql`.
Tests: `src/hoopdb/__tests__/session-directory-list.test.ts`.

Consumer: relayhistory-cloud `LIST_BACKEND=supabase`
(`docs/list-backend.md` there). It serves `GET /v1/sessions` and
`GET /v1/sessions/activity` without a text query. Text search belongs to
ClickHouse; this list never reads transcript content.

## Dimensions

| dimension | where it lives | written by |
| --- | --- | --- |
| source (harness), latest model, counts, tokens, cost, first/last activity, users, machines | `session_directory` (20261030010000) | projector, from `session_index` |
| project id, repository, branch | `session_directory` | `writeDirectoryCatalog` from the complete raw-record projection; pointer values are a fallback |
| title | `session_directory.title` | native model title, latest pointer title, then current complete recorded catalog title (`recorded_catalog_title`) |
| short summary | `session_directory.short_summary` (20261030020000) | `writeShortSummary` |
| **agent names (several per session)** | **`session_directory.agent_names` + `session_directory_agents`** (new) | **`writeDirectoryCatalog`** |
| **catalog models** | **`session_directory.catalog_models`** (new); the RPC returns them united with the pointer model | `writeDirectoryCatalog` |
| **project key and method, initial commit, originator, agent version** | **`session_directory`** (new columns) | `writeDirectoryCatalog` |
| **parent and child sessions** | **`session_directory_relationships`** (new, parent to child edges); parents and child count are read from the edges | `writeDirectoryCatalog` |

`writeDirectoryCatalog` (`src/hoopdb/session-directory-catalog.ts`) takes a
`DirectoryCatalog`. `directoryCatalogFromProjection` builds one from the rows
that `projectRelayhistorySession` (`packages/pipeline`) already makes out of
the delivered `session` and `relationship` records. That is the same parse that
relayhistory's `project_delivery_catalog_row` performs.

A project key that is a local path is never stored. This covers method `path`,
absolute paths, `~`, drive letters, UNC paths and `file:` paths.

The S3 search-index pass calls `writeDirectoryCatalog` after every complete
logical-session read. A partial, missing, unscrubbed, conflicting, over-limit,
or ClickHouse-truncated pass never clears or updates catalog fields. Historical
sessions still need the controlled re-projection described in "Backfill"
below. Until that backfill runs and is verified, old catalog fields and
parent/child links may be empty, and the list **refuses `p_agent`** with
`0A000 agent_filter_not_ready` instead of answering an empty list.
relayhistory-cloud falls back to Neon on that error. An operator unlocks it
with
`UPDATE sessions.session_directory_catalog_rollout SET completed_at = now()`
only after the "Backfill" verification passes.

`writeDirectoryCatalog` is one statement. It locks the live directory row
(`FOR UPDATE`) and joins every write to that lock, so a tombstone either
commits first (nothing is written) or waits and then removes what was written.

## RPCs

Both RPCs resolve the tenant through `sessions.session_directory_worker_scope`.
That means the same role guard and the same signed-claims pins (issuer,
audience, lifetime, workspace bound) as `search_session_directory`. EXECUTE is
granted to `session_directory_search_worker` only.

`sessions.list_session_directory` takes:

- `p_workspace_id`
- `p_sources text[]` (OR), `p_not_sources text[]`
- `p_project_contains`: a literal substring of the project id, project key or
  repository
- `p_user_ids text[]` (OR), `p_agent` (lower, exact)
- `p_last_ts_after` (>=), `p_last_ts_before` (<), `p_first_ts_after` (>=),
  `p_first_ts_before` (<)
- `p_session_harnesses text[]` + `p_session_ids text[]` (a batch lookup by key)
- `p_cursor_last_activity_at`, `p_cursor_session_id`, `p_cursor_harness`
- `p_limit` (1 to 200)

It returns up to `p_limit + 1` rows, newest activity first.

`sessions.session_directory_activity(p_workspace_id, p_since, p_until)` uses
span semantics: a session counts on every UTC day from its first to its last
activity. The window is at most 366 days.

**Ordering.** Every page is ordered, and every cursor compared, on the
directory row's `(last_activity_at, session_id, harness)`. The agent and user
companions are membership filters only. A trigger also keeps every companion's
`last_activity_at` equal to the row's in the row's own transaction, so a stale
companion can neither repeat nor skip a session.

**Erasure.** Triggers on tombstone and on delete remove the session's agent
rows, the edges it delivered as a parent, and the edges live parents hold
naming it as a child.

Plans by filter shape, each pinned by an EXPLAIN test (20,000 tenant rows plus
60,000 other-tenant rows) with a selective value:

| shape | index |
| --- | --- |
| no filter | `session_directory_workspace_recent_idx` |
| one source | `session_directory_harness_recent_idx` (org, workspace, harness, last_activity_at DESC) |
| project substring | `session_directory_tenant_project_text_trgm_idx` (new; trigram over id, key and repo) |
| one or several users | `session_directory_users_recent_idx` (membership probe), then the directory primary key and a sort |
| agent | `session_directory_agents_recent_idx` (new; membership probe), then a sort |
| batch keys | a key index (`session_directory_session_lookup_idx` or the primary key) |

A tenant-prefixed GIN over `user_ids` was tried and dropped. Its
`org_id`/`workspace_id` posting lists cover the whole tenant, and the planner
never picks it.

## Backfill

After the migration, existing rows have empty catalog columns and no agent or
relationship rows. The backfill is a controlled re-projection of those derived
fields. It is an operator step that needs Khaliq's OK, like any write to a real
database; this section describes the procedure and does not authorize it. Keep
relayhistory-cloud on Neon, or in `shadow` mode for the agent and catalog
fields, until the activation step below.

The canonical raw objects in object storage are the content source of truth.
Neon is a read-only fallback, used only for sessions that have no complete
history in object storage.

**Invariants.** The run must:

- never write `unknown` as a directory user; leave the user set empty when no
  trusted user source exists;
- never store a local path as a project key or project id
  (`writeDirectoryCatalog` re-derives its input through
  `directoryCatalogFromProjection`, which enforces this);
- never write from a partial, missing, unscrubbed, conflicting, unreadable or
  over-limit read: record the reason and leave the row unchanged;
- never leave two live directory rows for the same logical session;
- never roll back tombstone, erasure, reset, fence or epoch state;
- never mark the catalog rollout complete before the verification below passes;
- never write Neon.

**Before.**

1. Confirm the target database has this migration and the later migrations
   `writeDirectoryCatalog` requires (20261105022000 and 20261105031000)
   applied, the migration ledger is green, and function grants match
   `src/hoopdb/sessions-function-acl.json`.
2. Record, as evidence outside the database, the row counts of
   `sessions.session_directory`, its companion tables
   (`session_directory_agents`, `session_directory_users`,
   `session_directory_machines`, `session_directory_relationships`,
   `session_directory_resurrection_fences`) and both rollout tables
   (`session_directory_rollout`, `session_directory_catalog_rollout`),
   together with the current `event_projection_epoch` values. These tables are
   never restored from a snapshot by this procedure: tombstones and
   resurrection fences are monotonic history (migration 20261031001000: a
   later tombstone only raises a fence, `record_fences` keeps the greatest
   revision, and only erasure removes them with the row), so overwriting them
   with earlier rows could bring a deleted session back.
3. Hold the directory projector queue for the rebuild window so its work can be
   replayed afterwards. Do not pause canonical object-storage writes.
4. Dry run, read only: for every live session key, build the catalog as in
   path 1 below without writing, and report how many would be written and how
   many are incomplete, by reason. Stop on any identity conflict; resolve those
   rows explicitly rather than choosing a winner.

**Re-projection.** Process stable `(org_id, workspace_id, harness, session_id)`
keyset batches. Write each logical session in its own transaction and keep an
external checkpoint with counts.

1. Default path: read every live manifest and verified raw chunk of the
   session, project the complete record set with `projectRelayhistorySession`
   (`packages/pipeline`), build the catalog with
   `directoryCatalogFromProjection`, and call
   `writeDirectoryCatalog(tx, key, catalog, { expectedEpoch })` with the epoch
   read for that key. This is the same call the S3 search-index pass makes
   (`src/session-pipeline/search-index.ts`). A `{ written: false }` result (a
   stale epoch) is left for a later batch, never forced.
2. For sessions that only Neon knows, copy from Neon `sessions.session_catalog`,
   `session_catalog_agents` and `session_relationships` (read-only role) into
   `writeDirectoryCatalog` inputs. Neon keeps one agent per session (0058
   primary key).
3. Desktop-catalog parents that migration 20261105031000 marks pending are
   drained with the bounded, resumable driver in
   `src/hoopdb/session-desktop-catalog-reproject.ts` (schema owner connection
   only).

Rate-limit batches by database latency, lock wait, queue age and error rate.
Stop on any collision, unexpected deletion, dead-letter growth, sustained lock
pressure or rising mismatch rate, and resume only from the last committed
checkpoint.

**Verify.** After replaying the held projector work, produce tenant- and
harness-sliced reports and require:

- no duplicate logical sessions and no `unknown` directory user;
- every directory machine maps to exactly one live pointer;
- project id, project key, repository, branch, agent and user filters match
  Neon for at least 95% of page-1 sessions, with every remaining mismatch
  classified;
- `src/hoopdb/__tests__/session-directory-list.test.ts` passes for the deployed
  code;
- pagination has no repeats, skips or cursor loops, and queue, dead-letter and
  query-plan gates stay green;
- at least 48 hours of list traffic in shadow, comparing keys, ordering,
  filters and hydrated fields with Neon.

**Activate.** Only after every check passes, and with approval, set
`session_directory_catalog_rollout.completed_at` (above) and then change
`LIST_BACKEND`.

**Recovery.** This procedure has no snapshot restore. Before activation, a
wrong or incomplete re-projection is corrected by re-running step 1 for the
affected keys: `writeDirectoryCatalog` rewrites only derived catalog fields,
under the row lock and the expected epoch, and writes nothing for a row that a
tombstone has claimed. After activation, first return `LIST_BACKEND` to Neon
and keep the mismatch evidence, then correct derived fields the same way. Any
recovery that would change tombstone, erasure, reset, fence or epoch state is
outside this document and needs a schema-owner-reviewed operator procedure,
backed by isolated tests that delete or reset after a snapshot and replay late
pointer events, proving that nothing deleted comes back. Never mutate Neon
during recovery.

A hosted operator copy of this plan, with deployment-specific details, is kept
privately. It is optional context and is not needed to follow this section.

## Native self-host activation

The Neon shadow procedure above remains the hosted procedure. Native installs
use their verified canonical object-storage manifests and the normal consumer's
complete-source projection instead. Do not invent a Neon fallback or mark only
one workspace complete: `session_directory_catalog_rollout` is global.

After the normal consumer has projected **every live logical session**, run
`bun scripts/native-agent-catalog-activation.ts --plan` with
`HOOPDB_DB_MODE=selfhosted`, an owner `HOOPDB_URL` using `sslmode=verify-full`,
and the installation's normal CA trust. Supply credentials through the existing
secret mechanism; never save them in a plan or command transcript. The command
first runs the source image's checksum/role schema verifier. It refuses a role
whose RLS visibility could hide another tenant, and limits the audit to 10,000
directory rows; a larger install needs a separately reviewed bounded driver.

The plan covers all tenants and harnesses, complete current event/catalog
projection, live pointer coverage, agent companion membership and ordering,
unknown users, machine pointers, and parent-edge liveness. Its report contains
counts and a fingerprint, not names, content or credentials. A missing or unreadable
legacy source must be repaired through the normal complete-source writer. It is
not an exclusion, and cannot be waived by clearing a flag or deleting the row.
An empty recorded agent array is legitimate for an unnamed session. It does not
prove that Desktop's registered name was exported correctly.

Before activation, retain canonical-read/reprojection receipts, schema and role
proofs, queue/dead-letter health, pagination checks, and the intended registered
agents' actual SDK-to-catalog evidence. Review the plan. With authorization for
the real database write, run the same command with `--apply PLAN_FINGERPRINT`.
It takes short bounded locks, blocks concurrent migration admission and reruns
the complete audit before changing only the existing completion marker. A changed
plan, stale projection or failed invariant refuses the write. It never repairs
data, resets epochs, changes tombstones/fences, or turns on the newer Desktop
relationship/model catalog (`catalog_read_version` / forget tracking).

Then prove the actual authenticated `agent=` list filters, tenant isolation,
pagination and Dashboard chips against the known sessions. `q=@name` alone is
not this proof. The list's `agent=` parameter is an exact normalized name; a
short prefix must not be reported as matching every longer agent name. Missing
registered names require the SDK/export repair, not names inferred from text.
If any acceptance fails, treat activation as incomplete and retain the evidence;
do not claim success because the audit or marker write passed. Native recovery
uses the same complete-source re-projection without restoring deletion history.

## Gaps, not in this migration

- **`task_refs` / PR link and `kinds`.** Neither has a projection, so the list
  returns `[]` for both. Cloud's PR badge reads `taskRefs`.
- **Reasoning and cache token totals, cache 5m/1h split.** Not on
  `session_index`.
- **Session catalog detail.** Relationship detail (kind, identity status,
  child agent type/name/model, spawn depth), markers, commit links and the
  first prompt are not carried. relayhistory `GET /v1/sessions/:id/catalog`
  stays on Neon.
- **Exact `project_id` and `no_project`.** Not list RPC parameters yet; the
  search RPC has them.
- **Hybrid search v2 asks (relayhistory-cloud #246).** A title-plus-summary-only
  match mode, separate from the metadata `search_text`, belongs in
  `search_session_directory` and is not here. The brief `generated_at`
  lives in `session_briefs`, owned by feat/model-session-brief. This RPC
  returns `short_summary_generated_at`.


### Recorded title fallback (044100)

The complete canonical catalog supplies its existing scrubbed `title` metadata to
`recorded_catalog_title`; the list still serves `title` through its existing RPC.
Both catalog writer paths bind the physical source to the logical session, current
event epoch, live row and ready projection. Display priority is the native model
brief, latest live pointer, then the recorded title. Replays and model quarantine
recompute that priority instead of retaining a previous generated value.

The recorded title is at most 90 UTF-16 code units, trimmed, and contains no control
characters. Invalid titles are not inferred from prompts or transcript text. Source
invalidation (including an epoch transition left marked ready) and tombstones clear
the recorded slot; a new complete projection must
rebuild it. If an authoritative title equals the invalidated recorded value, the
trigger conservatively clears the displayed copy until the normal priority projector
recomputes it; equality never decides normal ready publication priority.
This adds no model call, project-ID inference, object read per list row,
or database privilege. Local paths remain excluded from project IDs/keys.

The storage and invoker-only invalidation trigger are part of the summary read
migration, `20261105044100_session_brief_summary_read.sql`. Complete canonical
replays also clear a removed or invalid recorded title at the same source epoch.
A denied schema-metadata read fails closed; pre-044100 no-title metadata callers
retain their prior behavior. Deployments require this migration's exact generation,
rollback and boundary manifest qualification.
