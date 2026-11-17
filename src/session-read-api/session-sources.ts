/**
 * GET /v1/sessions/:sessionId/sources (READ_API_SESSION_SOURCES_ENABLED=true): the distinct
 * harnesses (`source`) holding a live pointer row for one session id in the caller's tenant.
 *
 * One statement over `sessions.session_index` as `bulls96_read_api`, inside the read API's
 * tenant-bound READ ONLY transaction (liveness.ts TENANT_SETTINGS_SQL). It reads only columns the
 * role already holds (supabase/bootstrap/session_read_api_role.sql: org_id, workspace_id, harness,
 * session_id, deleted_at, canonical_session_id) through the existing tenant RLS policy
 * `session_index_read_api`: no new function, grant or migration. It is the read the turns route
 * makes (session-turns-source.ts SESSION_HARNESSES_SQL), restricted to live rows:
 *
 * - live = `deleted_at IS NULL`; an erased session (every pointer tombstoned) and an unknown id
 *   both have no live row, so both answer no source;
 * - the id matches the source RelayHistory id (`canonical_session_id`, NULL meaning
 *   `session_id`) or the physical `session_id`, as the timing and turns pointer reads match it.
 *   `session_id = $3 OR canonical_session_id = $3` equals
 *   `coalesce(canonical_session_id, session_id) = $3 OR session_id = $3`. The physical branch is an
 *   index lookup (si_session); the canonical branch is a scan of the workspace's live canonical
 *   rows in the partial index session_index_canonical_live (org, workspace leading), so it is
 *   bounded by that workspace's aliased sessions, not by the table.
 *
 * Each source row also carries the physical ids and source ids its live rows hold, so the caller
 * can check a per-key fence (the self-host operator erase fence) under every form of the key, not
 * only the form the caller asked with.
 *
 * Privacy fences. Live means `deleted_at IS NULL` and nothing else, exactly as the directory list,
 * timing, turns and summary reads. Fences that do not tombstone synchronously (a hosted selection
 * or reset of a non-managed pointer, a failed operator erase) are not visible to the read role
 * here; see docs/session-read-api-design.md, the /sources contract.
 *
 * Owner-buffered (unpublished) sessions have no pointer row and are never a source here.
 */
import type { TenantRunner } from "./liveness.ts";

/** At most this many sources are listed (sorted by name); more is never expected. */
export const SESSION_SOURCES_MAX = 16;

/**
 * One row per harness, sorted by harness under the "C" collation (byte order; harness names are
 * ASCII, so it equals the route's JS sort and the LIMIT cut is the first SESSION_SOURCES_MAX of
 * that order).
 */
export const SESSION_SOURCES_SQL = `SELECT p.org_id, p.workspace_id, p.harness,
       array_agg(DISTINCT p.session_id) AS session_ids,
       coalesce(array_agg(DISTINCT p.canonical_session_id) FILTER (WHERE p.canonical_session_id IS NOT NULL), '{}') AS canonical_ids
  FROM sessions.session_index AS p
 WHERE p.org_id = $1 AND p.workspace_id = $2 AND p.deleted_at IS NULL
   AND (p.session_id = $3 OR p.canonical_session_id = $3)
 GROUP BY p.org_id, p.workspace_id, p.harness
 ORDER BY p.harness COLLATE "C"
 LIMIT ${SESSION_SOURCES_MAX}`;

export interface SessionSourceRow {
  org_id: string;
  workspace_id: string;
  harness: string;
  /** The physical session ids of the harness's live rows for this key. */
  session_ids: string[];
  /** Their source RelayHistory ids, where one is recorded. */
  canonical_ids: string[];
}

/** The live sources of one session id in the bound tenant; rows are rechecked by the route. */
export type SessionSources = (orgId: string, workspaceId: string, sessionId: string) => Promise<SessionSourceRow[]>;

export function sqlSessionSources(run: TenantRunner): SessionSources {
  return async (orgId, workspaceId, sessionId) => {
    const { rows } = await run(orgId, workspaceId, (q) => q.query<SessionSourceRow>(SESSION_SOURCES_SQL, [orgId, workspaceId, sessionId]));
    return rows;
  };
}
