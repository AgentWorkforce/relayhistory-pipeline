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
 *   `canonical_session_id = $3 OR session_id = $3` is that predicate written so each branch has
 *   an index (si_session; session_index_canonical_live), and equals
 *   `coalesce(canonical_session_id, session_id) = $3 OR session_id = $3`.
 *
 * Owner-buffered (unpublished) sessions have no pointer row and are never a source here.
 */
import type { TenantRunner } from "./liveness.ts";

/** At most this many sources are listed (sorted by name); more is never expected. */
export const SESSION_SOURCES_MAX = 16;

export const SESSION_SOURCES_SQL = `SELECT DISTINCT p.org_id, p.workspace_id, p.harness
  FROM sessions.session_index AS p
 WHERE p.org_id = $1 AND p.workspace_id = $2 AND p.deleted_at IS NULL
   AND (p.session_id = $3 OR p.canonical_session_id = $3)
 ORDER BY p.harness
 LIMIT ${SESSION_SOURCES_MAX}`;

export interface SessionSourceRow {
  org_id: string;
  workspace_id: string;
  harness: string;
}

/** The live sources of one session id in the bound tenant; rows are rechecked by the route. */
export type SessionSources = (orgId: string, workspaceId: string, sessionId: string) => Promise<SessionSourceRow[]>;

export function sqlSessionSources(run: TenantRunner): SessionSources {
  return async (orgId, workspaceId, sessionId) => {
    const { rows } = await run(orgId, workspaceId, (q) => q.query<SessionSourceRow>(SESSION_SOURCES_SQL, [orgId, workspaceId, sessionId]));
    return rows;
  };
}
