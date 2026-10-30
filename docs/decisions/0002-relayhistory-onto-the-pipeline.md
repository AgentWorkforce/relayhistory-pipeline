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
- **Full-text search over event content moves to ClickHouse or ObsessionDB**
  (ClickHouse-compatible, on object storage), loaded from the normalized S3
  objects. Neither is ready yet. Until one is, transcript search is not
  served (see Interim below).

## How each existing query surface is served

The surfaces come from `docs/session-search-index-contract.md`.

| Surface | Today (Neon) | Pointer model |
| --- | --- | --- |
| Session lists, filters, catalog, rollups, projects, repos, usage | `session_rollups`, `session_catalog`, ... | `session_index` and its indexes |
| `get_shared_session` (ordered replay), session events | `convergence_events` keyset | That session's normalized objects in S3, by `manifest_key` |
| `get_shared_session_context` (window in one session) | trigram + replay keyset | Read the session's objects and filter in the service (one session at a time) |
| Briefs and analysis, memory pack, digest | Event rows | The session's objects in S3 |
| `list_session_timings`, `list_team_activity` | Events, markers, timing cache | `session_index` activity times, plus the manifest for per-session detail |
| `search_shared_sessions`, `/v1/events?q=`, dashboard transcript search | `ILIKE` over `left(content, 4096)` and `task_title` with trigram GIN | **ClickHouse/ObsessionDB** (deferred). Interim: title and metadata search over `session_index` only |

## Interim (until ClickHouse or ObsessionDB is ready)

Transcript and full-text event search is unavailable on the new data plane.
Search falls back to the indexed metadata (title, project, repo, agent). This
is a user-visible regression: a cutover before the search store exists ships
it, and Khaliq accepts or defers that at cutover time. For the search store
itself, check before adopting ObsessionDB:
- that it supports ClickHouse's text, token and n-gram indexes;
- loading from S3;
- per-org isolation;
- that pipeline tombstones propagate to it.

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
