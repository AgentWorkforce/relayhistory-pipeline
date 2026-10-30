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
  - Embeddings are backfilled from S3 after the raw content is in place, not
    at ingest.
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
