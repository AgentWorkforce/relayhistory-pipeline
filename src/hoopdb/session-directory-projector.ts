/**
 * Projects one logical session (org, workspace, harness, session) from its physical
 * `sessions.session_index` pointer rows into `sessions.session_directory`. Called by
 * the HoopDb indexer after every pointer upsert and pointer tombstone.
 *
 * PROVISIONAL precedence (the projector decision in
 * docs/session-search-index-contract.md is still open, so the rollout marker is never
 * set here and the index audit keeps failing closed):
 *   - created_at = earliest live pointer, last_activity_at = latest live pointer;
 *   - title, project, repository, branch, model, state and the usage counters come
 *     from the most recently active live pointer (no summing across machines);
 *   - machine_ids / user_ids and the membership tables are the union of live pointers;
 *   - task_title, agent_name and short_summary are not in session_index and are kept
 *     as already projected (short_summary is written only by writeShortSummary).
 *
 * A tombstoned directory row is never resurrected: deleted sessions do not come back.
 * search_text is always rebuilt with sessions.session_directory_search_text, so the
 * scrubbed short_summary stays part of it.
 */
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import type { SessionDirectoryKey } from "./session-directory-summary.ts";

export async function projectSessionDirectory(
  db: SqlExecutor,
  key: SessionDirectoryKey,
): Promise<{ projected: boolean }> {
  const params = [key.orgId, key.workspaceId, key.harness, key.sessionId];
  const { rows } = await db.query<{ projected: boolean }>(
    `WITH live AS (
       SELECT * FROM sessions.session_index
        WHERE org_id = $1 AND workspace_id = $2 AND harness = $3 AND session_id = $4
          AND deleted_at IS NULL
     ), latest AS (
       SELECT * FROM live ORDER BY last_activity_at DESC, revision DESC, machine_id DESC LIMIT 1
     ), agg AS (
       SELECT min(created_at) AS created_at, max(last_activity_at) AS last_activity_at,
              array_agg(DISTINCT machine_id ORDER BY machine_id) AS machine_ids,
              array_agg(DISTINCT user_id ORDER BY user_id) AS user_ids
         FROM live
     )
     INSERT INTO sessions.session_directory AS d
       (org_id, workspace_id, harness, session_id, created_at, last_activity_at,
        title, project_id, git_repo, git_branch, model, state,
        event_count, input_tokens, output_tokens, cost_usd_micros,
        machine_ids, user_ids, search_text, projected_at)
     SELECT $1, $2, $3, $4, agg.created_at, agg.last_activity_at,
            l.title, l.project_id, l.git_repo, l.git_branch, l.model, l.state,
            l.event_count, l.input_tokens, l.output_tokens, l.cost_usd_micros,
            agg.machine_ids, agg.user_ids,
            sessions.session_directory_search_text(
              l.title, NULL, NULL, l.project_id, l.git_repo, l.git_branch, l.model, NULL),
            now()
       FROM latest AS l CROSS JOIN agg
     ON CONFLICT (org_id, workspace_id, harness, session_id) DO UPDATE SET
       created_at = excluded.created_at,
       last_activity_at = excluded.last_activity_at,
       title = excluded.title,
       project_id = excluded.project_id,
       git_repo = excluded.git_repo,
       git_branch = excluded.git_branch,
       model = excluded.model,
       state = excluded.state,
       event_count = excluded.event_count,
       input_tokens = excluded.input_tokens,
       output_tokens = excluded.output_tokens,
       cost_usd_micros = excluded.cost_usd_micros,
       machine_ids = excluded.machine_ids,
       user_ids = excluded.user_ids,
       search_text = sessions.session_directory_search_text(
         excluded.title, d.task_title, d.agent_name, excluded.project_id,
         excluded.git_repo, excluded.git_branch, excluded.model, d.short_summary),
       projected_at = now()
     WHERE d.deleted_at IS NULL
     RETURNING true AS projected`,
    params,
  );
  if (rows.length === 0) return { projected: false };
  // Membership companions: exactly the live pointers' users and machines.
  for (const [table, column] of [
    ["session_directory_users", "user_id"],
    ["session_directory_machines", "machine_id"],
  ] as const) {
    await db.query(
      `DELETE FROM sessions.${table} AS m
        WHERE m.org_id = $1 AND m.workspace_id = $2 AND m.harness = $3 AND m.session_id = $4
          AND NOT EXISTS (
            SELECT 1 FROM sessions.session_index AS p
             WHERE p.org_id = m.org_id AND p.workspace_id = m.workspace_id AND p.harness = m.harness
               AND p.session_id = m.session_id AND p.${column} = m.${column} AND p.deleted_at IS NULL)`,
      params,
    );
    await db.query(
      `INSERT INTO sessions.${table} (org_id, workspace_id, harness, session_id, ${column}, last_activity_at)
       SELECT org_id, workspace_id, harness, session_id, ${column}, max(last_activity_at)
         FROM sessions.session_index
        WHERE org_id = $1 AND workspace_id = $2 AND harness = $3 AND session_id = $4 AND deleted_at IS NULL
        GROUP BY org_id, workspace_id, harness, session_id, ${column}
       ON CONFLICT (org_id, workspace_id, harness, session_id, ${column})
       DO UPDATE SET last_activity_at = excluded.last_activity_at`,
      params,
    );
  }
  return { projected: true };
}
