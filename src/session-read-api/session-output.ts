/**
 * GET /v1/sessions/output — deterministic summary metrics over the Supabase
 * lane. One claim-bound SECURITY DEFINER call; the SQL mirrors relayhistory's
 * lib/session-output.ts oracle exactly (no transcript reads).
 */
import type { SqlQuery, TenantRunner } from "./liveness.ts";

export interface SessionOutput {
  agentMessages: number;
  commits: number;
  linesAdded: number;
  linesRemoved: number;
  commitsWithoutLineCounts: number;
}

interface OutputRow {
  agent_messages: string | number;
  commits: string | number;
  lines_added: string | number;
  lines_removed: string | number;
  commits_without_line_counts: string | number;
}

export async function sessionOutput(
  run: TenantRunner,
  orgId: string,
  workspaceId: string,
  since: Date,
  until: Date,
): Promise<SessionOutput> {
  const { rows } = await run(orgId, workspaceId, (q: SqlQuery) =>
    q.query<OutputRow>(
      "SELECT agent_messages, commits, lines_added, lines_removed, commits_without_line_counts FROM sessions.session_output($1, $2, $3)",
      [workspaceId, since.toISOString(), until.toISOString()],
    ));
  const r = rows[0]!;
  return {
    agentMessages: Number(r.agent_messages),
    commits: Number(r.commits),
    linesAdded: Number(r.lines_added),
    linesRemoved: Number(r.lines_removed),
    commitsWithoutLineCounts: Number(r.commits_without_line_counts),
  };
}
