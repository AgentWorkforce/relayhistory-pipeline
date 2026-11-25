import type { Queryable } from "../relayhistory-export.ts";
import type { LegacyScope } from "../neon-s3-sync/legacy-source.ts";

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
       WHEN $2 IN ('','default') AND (${["session_rollups","session_catalog","convergence_events","conversation_turns","session_links","session_outcomes"].map((table) =>
         `EXISTS(SELECT 1 FROM sessions.${table} WHERE org_id=$1 AND workspace_id IN ('','default') AND workspace_id<>$2 AND session_id=$4)`).join(" OR ")}) THEN 'workspace_alias_collision'
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
  return rows[0].reason;
}

export const INVENTORY_TABLES = ["session_rollups", "session_catalog"] as const;
type InventoryTable = (typeof INVENTORY_TABLES)[number];
export type InventoryCursor = { workspace_id: string; stored_source: string; session_id: string };

const bounds = {
  session_rollups: ["first_ts", "last_ts"],
  session_catalog: ["coalesce(first_activity_at,received_at)", "coalesce(last_activity_at,first_activity_at,received_at)"],
} as const;

export async function assertInventoryKeys(db: Queryable, table: InventoryTable, org: string) {
  const [first,last] = bounds[table];
  const { rows: malformed } = await db.query(`SELECT 1 FROM sessions.${table} WHERE org_id=$1
    AND (workspace_id IS NULL OR source IS NULL OR source='' OR session_id IS NULL OR session_id=''
      OR ${first} IS NULL OR ${last} IS NULL OR ${first} > ${last}) LIMIT 1`, [org]);
  if (malformed.length) throw new Error("incomplete ordinary source inventory");
}

/** Enumeration alone never grants coverage. Each candidate is rechecked inside its own snapshot. */
export async function inventoryPage(db: Queryable, table: InventoryTable, org: string, after: InventoryCursor | null,
  window: { sinceMs?: number; untilMs?: number }, limit: number) {
  const [first, last] = bounds[table];
  const params = [org, limit, after?.workspace_id ?? null, after?.stored_source ?? null, after?.session_id ?? null,
    window.sinceMs === undefined ? null : new Date(window.sinceMs).toISOString(),
    window.untilMs === undefined ? null : new Date(window.untilMs).toISOString()];
  const { rows } = await db.query<InventoryCursor>(`SELECT DISTINCT workspace_id,source AS stored_source,session_id
    FROM sessions.${table} WHERE org_id=$1
    AND ($3::text IS NULL OR (workspace_id,source,session_id)>($3::text,$4::text,$5::text))
    AND ($6::timestamptz IS NULL OR ${last}>=$6) AND ($7::timestamptz IS NULL OR ${first}<$7)
    ORDER BY workspace_id,stored_source,session_id LIMIT $2`, params);
  return rows;
}

export async function ordinaryUnitForEmptyScope(db: Queryable, scope: LegacyScope) {
  // Rollups take precedence over the catalog, matching the ordinary writer.
  for (const table of INVENTORY_TABLES) {
    const [first, last] = bounds[table];
    const { rows } = await db.query<{ first_ms: string | null; last_ms: string | null; damaged: boolean | null }>(
      `SELECT floor(extract(epoch FROM min(${first}))*1000)::bigint::text AS first_ms,
       floor(extract(epoch FROM max(${last}))*1000)::bigint::text AS last_ms,
       bool_or(${first} IS NULL OR ${last} IS NULL OR ${last}<${first}) AS damaged
       FROM sessions.${table} WHERE org_id=$1 AND workspace_id=$2 AND source=$3 AND session_id=$4`,
      [scope.orgId, scope.workspaceId, scope.source, scope.sessionId]);
    const r = rows[0];
    if (!r || r.damaged) throw new Error("ordinary inventory clock missing or inconsistent");
    if (r.first_ms !== null && r.last_ms !== null) {
      const firstMs = Number(r.first_ms), lastMs = Number(r.last_ms);
      if (!Number.isSafeInteger(firstMs) || !Number.isSafeInteger(lastMs)) throw new Error("ordinary inventory clock invalid");
      return { ...scope, firstMs, lastMs, machineIds: [], totals: {} };
    }
  }
  throw new Error("ordinary source inventory missing");
}
