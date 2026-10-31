/**
 * One logical session under two machine ids: a cold export filed under
 * `unknown-origin-*`, and the live client under its own. They never share a
 * manifest, so no takeover fixes them and each looks complete on its own. The
 * export must not count as finished while one exists.
 */
import type { SqlExecutor } from "./queue.js";

export const SESSION_MACHINE_SPLIT = "session_machine_split" as const;

export interface SessionMachineSplit {
  type: typeof SESSION_MACHINE_SPLIT;
  orgId: string;
  workspaceId: string;
  harness: string;
  sessionId: string;
  machineIds: string[];
}

/** Every session id filed under more than one machine, in one scope. */
export async function detectMachineSplits(
  db: SqlExecutor,
  scope: { orgId: string; workspaceId?: string },
): Promise<SessionMachineSplit[]> {
  const { rows } = await db.query<{
    workspace_id: string;
    harness: string;
    session_id: string;
    machine_ids: string[];
  }>(
    `SELECT workspace_id, harness, session_id,
            array_agg(machine_id ORDER BY machine_id) AS machine_ids
       FROM sessions.session_index
      WHERE org_id = $1 AND ($2::text IS NULL OR workspace_id = $2)
      GROUP BY workspace_id, harness, session_id
     HAVING count(*) > 1
        AND bool_or(machine_id LIKE 'unknown-origin-%')`,
    [scope.orgId, scope.workspaceId ?? null],
  );
  return rows.map((row) => ({
    type: SESSION_MACHINE_SPLIT,
    orgId: scope.orgId,
    workspaceId: row.workspace_id,
    harness: row.harness,
    sessionId: row.session_id,
    machineIds: row.machine_ids,
  }));
}

/**
 * The completion check. Throws while a split exists: "every session exported"
 * is not authoritative for a session whose records sit in two places.
 */
export async function assertNoMachineSplits(
  db: SqlExecutor,
  scope: { orgId: string; workspaceId?: string },
): Promise<void> {
  const splits = await detectMachineSplits(db, scope);
  if (splits.length > 0)
    throw Object.assign(
      new Error(`${splits.length} sessions are split across machines`),
      { code: SESSION_MACHINE_SPLIT, splits },
    );
}
