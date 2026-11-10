/** Coordination facts over migration 045040's existing SECURITY DEFINER readers. */
import type { Directory } from "../directory.ts";
import type { CoordinationBackend, CoordinationEventRow, CoordinationOutputRow, CoordinationParams } from "../coordination-backend.ts";
import { COORDINATION_FACTS_ROW_CAP, normalizeCoordinationFacts } from "../coordination-backend.ts";
import { deadlineOf, dropErased, FactsReadDeadline, isPostgresFactsSchemaMissing, microsText, dateMicros, postgresWindowReadiness, type PostgresFactsDeps } from "./output-query.ts";

const ts = (column: string) => `to_char(${column} AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.US')`;
export const COORDINATION_EVENTS_SQL = `SELECT ${ts("e.ts")} AS ts, e.session_id, e.source, e.event_id, e.kind, e.project_id,
       e.task_ref_id, e.task_title, e.user_id, e.branch, e.input_tokens, e.output_tokens, e.reasoning_tokens,
       e.cost_usd_micros, e.files_touched, e.row_id
  FROM sessions.output_facts_events_for($1, $2::timestamptz, $3::timestamptz, $4::text, $5::integer) AS e
 ORDER BY e.row_id`;
export const COORDINATION_OUTPUTS_SQL = `SELECT f.fact_kind, ${ts("f.ts")} AS ts, f.scope_source, f.source, f.session_id,
       f.fact_key, f.origin, f.reverted, f.row_id
  FROM sessions.output_facts_outputs_for($1, $2::timestamptz, $3::timestamptz, $4::text, $5::integer) AS f
 ORDER BY f.row_id`;

async function capped<R extends { row_id: string }>(
  q: { query<T>(sql: string, args?: unknown[]): Promise<{ rows: T[] }> }, sql: string, args: unknown[], cap: number,
  deadline: number, now: () => number,
) {
  const rows: R[] = [];
  let after: string | null = null;
  while (rows.length <= cap) {
    if (now() > deadline) throw new FactsReadDeadline();
    const limit = Math.min(10_000, cap + 1 - rows.length);
    const page: R[] = (await q.query<R>(sql, [...args, after, limit])).rows;
    rows.push(...page);
    if (page.length < limit) break;
    after = page.at(-1)!.row_id;
  }
  return rows;
}

export function postgresCoordinationBackend(deps: PostgresFactsDeps): CoordinationBackend {
  return {
    kind: "postgres",
    windowReadiness: (orgId, workspaceId, since, until, opts = {}) => postgresWindowReadiness(deps, orgId, workspaceId, since, until, opts),
    isSchemaMissing: isPostgresFactsSchemaMissing,
    async readFacts(directory: Directory | undefined, p: CoordinationParams, opts = {}) {
      const cap = Math.max(1, Math.min(COORDINATION_FACTS_ROW_CAP, Math.trunc(p.rowCap ?? COORDINATION_FACTS_ROW_CAP)));
      const args = [p.workspaceId, microsText(dateMicros(p.since)), microsText(dateMicros(p.until))];
      const now = deps.now ?? Date.now;
      const deadline = deadlineOf(deps, opts.timeoutMs);
      const raw = await deps.run(p.orgId, p.workspaceId, async (q) => ({
        events: await capped<CoordinationEventRow>(q, COORDINATION_EVENTS_SQL, args, cap, deadline, now),
        outputs: await capped<CoordinationOutputRow>(q, COORDINATION_OUTPUTS_SQL, args, cap, deadline, now),
      }));
      const truncated = raw.events.length > cap || raw.outputs.length > cap;
      const events = await dropErased(deps.liveness, p.orgId, p.workspaceId, raw.events.slice(0, cap), (r) => ({ source: r.source, sessionId: r.session_id }));
      const outputs = await dropErased(deps.liveness, p.orgId, p.workspaceId, raw.outputs.slice(0, cap), (r) => ({ source: r.scope_source, sessionId: r.session_id }));
      return normalizeCoordinationFacts(directory, p, events, outputs, truncated);
    },
  };
}
