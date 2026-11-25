import type { Queryable } from "../../relayhistory-export.ts";
import type { LegacyScope } from "../../neon-s3-sync/legacy-source.ts";

/**
 * A point-in-time observation, never an export certificate. One statement
 * sees the same snapshot for ownership, ordinary inventory and legacy data.
 * Conservatively refuse any turn in this workspace/session: an untagged
 * turn can be returned for a source without sharing its stored source label.
 */
export async function inspectEmptyLegacyScope(db: Queryable, s: LegacyScope) {
  const { rows } = await db.query<{ reason: string }>(
    `WITH candidates AS (
       SELECT user_id FROM sessions.convergence_events
        WHERE org_id=$1 AND workspace_id=$2 AND source=$3 AND session_id=$4
     ) SELECT CASE
       WHEN EXISTS (SELECT 1 FROM unnest(ARRAY[
         'session_catalog','session_rollups','convergence_events','conversation_turns',
         'session_links','session_outcomes','deleted_upload_sessions','conversation_turn_fences'
       ]) t WHERE row_security_active(('sessions.' || t)::regclass)) THEN 'filtered_inventory'
       WHEN EXISTS (SELECT 1 FROM sessions.deleted_upload_sessions WHERE org_id=$1
         AND (workspace_id=$2 OR (workspace_id IN ('','default') AND $2 IN ('','default')))
         AND source=$3 AND session_id=$4)
         OR EXISTS (SELECT 1 FROM sessions.conversation_turn_fences WHERE org_id=$1
         AND (workspace_id=$2 OR (workspace_id IN ('','default') AND $2 IN ('','default')))
         AND session_id=$4) THEN 'fenced'
       WHEN NOT EXISTS (SELECT 1 FROM sessions.session_rollups WHERE org_id=$1 AND workspace_id=$2 AND source=$3 AND session_id=$4)
        AND NOT EXISTS (SELECT 1 FROM sessions.session_catalog WHERE org_id=$1 AND workspace_id=$2 AND source=$3 AND session_id=$4)
         THEN 'missing_inventory'
       WHEN (SELECT count(DISTINCT user_id) FROM candidates) > 1 THEN 'ambiguous_owner'
       WHEN NOT EXISTS (SELECT 1 FROM candidates WHERE user_id IS NOT NULL AND btrim(user_id)<>'')
         OR EXISTS (SELECT 1 FROM candidates WHERE user_id IS NULL OR btrim(user_id)='') THEN 'unbound_owner'
       WHEN EXISTS (SELECT 1 FROM sessions.conversation_turns WHERE org_id=$1 AND workspace_id=$2 AND session_id=$4)
         THEN 'turns_present_or_unattributable'
       WHEN EXISTS (SELECT 1 FROM sessions.session_links WHERE org_id=$1 AND workspace_id=$2 AND source=$3 AND session_id=$4)
         OR EXISTS (SELECT 1 FROM sessions.session_outcomes WHERE org_id=$1 AND workspace_id=$2 AND source=$3 AND session_id=$4)
         THEN 'legacy_rows_present'
       ELSE 'empty_in_snapshot' END AS reason`,
    [s.orgId, s.workspaceId, s.source, s.sessionId],
  );
  if (rows.length !== 1 || !rows[0]?.reason) throw new Error("empty scope observation incomplete");
  return { reason: rows[0].reason, exportable: false };
}
