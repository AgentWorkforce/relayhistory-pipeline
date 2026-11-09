/**
 * Runtime-neutral configuration parsing shared by the Lambda (lambda.ts) and
 * the Worker (packages/session-read-worker): no AWS or Node imports here.
 */

/** "epoch" → 0 (complete history); otherwise an ISO timestamp. Missing or junk refuses to start. */
export function parseCompleteSince(raw: string | undefined): number {
  if (raw === "epoch") return 0;
  const t = raw ? Date.parse(raw) : Number.NaN;
  if (Number.isNaN(t)) throw new Error('session read api: READ_API_COMPLETE_SINCE must be an ISO timestamp or "epoch"');
  return t;
}

export const READ_API_AGENT_MATCH_VAR = "READ_API_AGENT_MATCH";

/**
 * READ_API_AGENT_MATCH (default off): exactly "true" serves agent-name prefix matching on GET /v1/sessions
 * (`agentMatch`, a trailing `*`; migration 20261105050020). Unset, "" or "false" leaves the list as without it;
 * anything else refuses startup. Shared by the Lambda, the service and the Worker.
 */
export function agentMatchEnabled(raw: string | undefined): boolean {
  if (raw === undefined || raw === "" || raw === "false") return false;
  if (raw !== "true") throw new Error(`session read api: ${READ_API_AGENT_MATCH_VAR} must be unset, "", "true" or "false"`);
  return true;
}

export function agentMatchDeps(env: { READ_API_AGENT_MATCH?: string }): { agentMatch?: true } {
  return agentMatchEnabled(env.READ_API_AGENT_MATCH) ? { agentMatch: true } : {};
}
