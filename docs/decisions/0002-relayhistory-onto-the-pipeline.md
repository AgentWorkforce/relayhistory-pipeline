# 0002: RelayHistory moves onto the 96Bulls pipeline (content in S3, index in Supabase)

**Status:** decided (Khaliq, 2026-10-30). The plan below is being written.
**Refines:** 0001 (Supabase plus S3 only).
**Supersedes:** the content-copy design in `docs/relayhistory-supabase-migration.md`,
which copied Neon's `sessions` schema, content included, into Supabase Postgres.

## Decision

- **Session content lives in S3**, in the 96Bulls object layout: scrubbed raw
  chunks, a manifest, and normalized objects (`packages/pipeline`,
  `docs/pipeline-storage-adapter.md`). Neon's content tables don't move into
  Supabase. Those tables are `convergence_events` (205 GB),
  `delivery_records` (161 GB) and their projections; `sessions` totals 484 GB
  on Neon today.
- **Supabase holds the index only:** `sessions.session_index` and its
  companions (repos, machines, pipeline events). Each row is a session's
  metadata with proper indexes (title, state, user, machine, project, repo,
  branch, model, usage, activity times) plus the pointer to its S3 objects.
- **The RelayHistory Worker is re-pointed, not rewritten.** Metadata reads use
  `session_index`. Reads of one session's content fetch its objects from S3
  through the pointer.
- **Search (V1, Khaliq, 2026-10-30, relayed by sessionspipelinegrok and
  confirmed in session):** Supabase holds the pointers plus a search
  projection. Agent name, short summary and long summary are searchable there,
  and Postgres full-text search is a temporary stopgap. **V2** moves full-text
  search to ClickHouse or ObsessionDB, loaded from the normalized S3 objects.
- **Security, defense in depth:**
  - Raw session content stays in S3 under KMS, with a key per workspace.
    Deleting that key crypto-shreds the workspace's objects.
  - The Supabase search projection has row-level security and disk
    encryption.
  - No embeddings in V1, and no pgvector in Supabase. ClickHouse or
    ObsessionDB native text, token and n-gram search owns transcripts. If a
    semantic or hybrid layer is adopted later, its embeddings live with that
    search projection, built from S3, not duplicated into Supabase.
- **Sequencing is switch-first (Khaliq, 2026-10-30, superseding S3-first).**
  New writes move to the pipeline at T0. History is exported to S3 afterwards,
  and until a session is exported the Worker reads it from Neon, read-only.
  No state may be broken: every intermediate state serves complete reads, and
  the switch can be aborted before the flip. relayhistory-cloud #224's
  data-loss fix still lands before T0 and before any bulk export.
- **Neon must not be put at risk.** Today it shows a 91% cache hit rate,
  1.35 TB of temp spill, millions of dead tuples and 56 deadlocks
  (read-only check, 2026-10-30). The export therefore reads from a Neon child
  branch taken at T0, with its own compute, never from the production
  compute. The read fallback is bounded and drops once a session is exported.
  After T0, Neon production takes no writes.

## How each existing query surface is served

The surfaces come from `docs/session-search-index-contract.md`.

| Surface | Today (Neon) | Pointer model |
| --- | --- | --- |
| Session lists, filters, catalog, rollups, projects, repos, usage | `session_rollups`, `session_catalog`, ... | `session_index` and its indexes |
| `get_shared_session` (ordered replay), session events | `convergence_events` keyset | That session's normalized objects in S3, by `manifest_key` |
| `get_shared_session_context` (window in one session) | trigram + replay keyset | Read the session's objects and filter in the service (one session at a time) |
| Briefs and analysis, memory pack, digest | Event rows | The session's objects in S3 |
| `list_session_timings`, `list_team_activity` | Events, markers, timing cache | `session_index` activity times, plus the manifest for per-session detail |
| `search_shared_sessions`, `/v1/events?q=`, dashboard transcript search | `ILIKE` over `left(content, 4096)` and `task_title` with trigram GIN | V1: the Supabase search projection (agent name, short and long summary, plus a full-text stopgap), with RLS. V2: ClickHouse or ObsessionDB |

## Search before V2, and its open erasure question

V1 search is served from the Supabase projection. Before V2:
- Check ObsessionDB supports ClickHouse's text, token and n-gram indexes.
- Check it can load from S3.
- Check it isolates each org.
- Check pipeline tombstones propagate into it.

**Open (decision deferred to sessionspipelinegrok):** deleting a KMS key
shreds only the S3 objects encrypted with it. A Postgres full-text projection
needs plaintext tokens, and Supabase's disk encryption uses Supabase-managed
keys. Erasing a workspace's search data on Supabase therefore means deleting
its projection rows (tombstone-driven), with backups and PITR ageing out on
Supabase's retention. Unless the searchable text moves to a store with
per-workspace encryption (for example ClickHouse in V2), key deletion alone
does not erase it.

## Consequences for the migration

- **The backfill becomes an export, not a table copy.** Each historical Neon
  session is written to S3 in the pipeline layout (scrubbed chunks plus a
  manifest), and the existing pipeline builds its `session_index` row. Supabase
  stays small, so the Large compute (A2) is ample, and the Neon `C` versus
  Supabase `en_US` collation difference no longer affects content queries.
- **Carried over:**
  - the T0 cutover gate and durable hold (relayhistory-cloud #224);
  - the read-only Neon role and the Neon write fence;
  - the write guards and their negative tests;
  - deletion-tombstone handling (deleted uploads and resets never come back);
  - plan-bound checkpoints;
  - the rehearsal harness and the benchmark gates.
- **Obsolete for production:** copying the legacy content tables into
  Supabase, the catalog reprojection, the rollup rebuild, and the
  legacy-schema parity on Supabase. They stay in the repo as self-host and
  history tooling; the runbook no longer uses them.
- **New work:**
  1. a Neon-to-S3 session exporter;
  2. the Worker's reads re-pointed to `session_index` and S3;
  3. Cloud's consumers (Sessions board, shared-session MCP) moved to those
     reads;
  4. the search store, later.
- **Still needs Khaliq's explicit OK, each separately:** writing exported
  objects to the production bucket; deploys; the cutover; and retiring Neon.

## Amendment 1: a bounded recent transcript window in Supabase (DRAFT)

**Status:** draft in 96BullSessions #90. It takes effect when that PR
merges. Source: Khaliq, 2026-10-31, lead session, K-39 ("ObsessionDB off
the hot path").
**Amends:** "Supabase holds the index only" and the V1 search bullet
above. **Does not change:** S3 as the source of truth; no embeddings or
pgvector in Supabase.

### Decision

- Supabase may hold a **48-hour window** (K-47) of complete scrubbed session
  transcript text in the `sessions` schema, with trigram and FTS indexes.
  It serves the Postgres adapter of the search interface on hosted as a
  **safety net**, for indexer lag and short history-store outages. The
  full-history store is **primary** for all transcript search (K-47).
  - Full history is served by the store-agnostic history store (0001
    Amendment 1).
  - Self-host may run the window unbounded.
- **Window length: 48 hours** (Khaliq, 2026-10-31, K-47; it supersedes
  K-42's 14 days). Measured by K-38
  (`~/selfhost-proof/notes/prod-shape-2026-10-31.md`): ≈ 2.4 GB of text,
  and ≈ 3-4 GB to ≈ 8-10 GB with indexes depending on the overhead. The
  disk proposal is 32 GB now, 64 GB planned (K-32), excluding the open
  ~90 GB bookkeeping question.
- **Content rules:**
  - The text is scrubbed before storage, through the same scrub hop as S3
    raw.
  - **Complete event text**, with no per-event cap (K-42).
  - No embeddings.
  - RLS on every row, and every index leads with `org_id`.

### Retention and erasure

- **Retention:**
  - An hourly job *(proposed)* deletes every row older than 48 h.
  - The retention job's success is alarmed: it must have run within 2 h.
  - Rows are rebuildable from S3, so the window can be shrunk or regrown.
- **Erasure SLA (K-45, Khaliq, 2026-10-31):** a **logical delete
  immediately**, and **physical purge within 48 h**, in every store that
  holds transcript text (this window, the history store including
  ObsessionDB's 24 h time travel, and S3 noncurrent versions).
  - Tombstones, session resets and workspace erasure delete or hide
    matching rows immediately, through the same tombstone path as the V1
    projection. Vacuum and the S3 lifecycle complete physical removal
    within 48 h.
  - KMS crypto-shredding covers only S3. In Supabase, erasure is by row
    deletion.
  - Supabase backups and PITR age out within Supabase's retention, which
    is typically at least 7 days. **Whether backups fall under the 48 h SLA
    is open (plan Q27).** Until it is decided, the backup retention period
    is documented as the backup erasure tail.
- **Disk:** the Supabase disk sizing for the window is part of K-32.

### Still needs Khaliq's explicit OK for each exact action

- the window length is decided (K-47: 48 h, complete text);
- applying the migration and enabling the writer and retention job (K-43);
- resizing the disk (K-32).

This amendment is not that approval.
