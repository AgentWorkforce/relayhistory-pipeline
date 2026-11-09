/**
 * Output (`GET /v1/sessions/output`) over the ClickHouse facts: relayhistory's
 * `getSessionOutput` (lib/session-output.ts @ c7de2163) with the same result for
 * the same scope. The per-row decisions were made by the producer
 * (session-pipeline/output-facts.ts); what depends on the query scope happens here:
 *
 *   messages        DISTINCT (source, session, message id) of `message` facts in the window
 *   legacy_messages `legacy_turn_message` facts in the window whose (source, session) has
 *                   no message in the window
 *   commits         every `commit_link`/`outcome` candidate (unwindowed, as in Neon) plus
 *                   the window's `tool_result` candidates; an abbreviated sha resolves to
 *                   the one 40-char candidate it prefixes, if unique; grouped by sha with
 *                   min(ts)/max(added)/max(removed); then the window applies to min(ts)
 *
 * The window is inclusive at both ends and optional on either side (Neon's `time()`).
 *
 * This module is the contract lane F1 (session analysis) calls for its brief
 * inputs, per session: `{ source, sessionId }` and no window. Read-only: SELECTs only.
 */
import type { ClickHouseClient, QueryParams } from "../../session-pipeline/vendor/search-index/clickhouse.ts";
import { OUTPUT_FACTS_SESSION_STATE_TABLE, OUTPUT_FACTS_STATE_TABLE, OUTPUT_FACTS_TABLE } from "../../session-pipeline/output-facts-schema.ts";
import { EVENTS_TABLE } from "../../session-pipeline/vendor/search-index/schema.ts";

export interface SessionOutput {
  agentMessages: number;
  commits: number;
  linesAdded: number;
  linesRemoved: number;
  commitsWithoutLineCounts: number;
}

export interface SessionOutputScope {
  orgId: string;
  /** Explicit, non-empty: relayhistory throws without one, so does this. */
  workspaceIds: string[];
}

export interface SessionOutputFilter {
  since?: Date;
  until?: Date;
  source?: string;
  sessionId?: string;
}

export class OutputScopeError extends Error {
  constructor() {
    super("An explicit evidence workspace scope is required");
    this.name = "OutputScopeError";
  }
}

/** ClickHouse DateTime64 text ("YYYY-MM-DD HH:MM:SS[.ffffff]", UTC) as epoch microseconds. */
export function chMicros(text: string): bigint {
  const [whole, fraction = ""] = text.split(".");
  const ms = Date.parse(`${whole!.replace(" ", "T")}Z`);
  if (!Number.isFinite(ms)) throw new Error("invalid ClickHouse time");
  return BigInt(ms) * 1000n + BigInt(fraction.slice(0, 6).padEnd(6, "0"));
}

/** A JS instant as DateTime64(6) text (UTC). JS Dates are millisecond, as relayhistory's are. */
export function chInstant(date: Date): string {
  return date.toISOString().replace("T", " ").replace("Z", "000");
}

const num = (v: unknown) => Number(v ?? 0);

function where(scope: SessionOutputScope, filter: SessionOutputFilter, sourceColumn = "source") {
  const workspaceIds = [...new Set(scope.workspaceIds)];
  if (!workspaceIds.length || workspaceIds.some((id) => !id)) throw new OutputScopeError();
  const params: QueryParams = { org: scope.orgId, ws: workspaceIds };
  let sql = "org_id = {org:String} AND workspace_id IN {ws:Array(String)}";
  if (filter.source) {
    sql += ` AND ${sourceColumn} = {source:String}`;
    params.source = filter.source;
  }
  if (filter.sessionId) {
    sql += " AND session_id = {session:String}";
    params.session = filter.sessionId;
  }
  const window = (column: string) => {
    const parts: string[] = [];
    if (filter.since) parts.push(`${column} >= toDateTime64({since:String}, 6, 'UTC')`);
    if (filter.until) parts.push(`${column} <= toDateTime64({until:String}, 6, 'UTC')`);
    return parts.length ? parts.join(" AND ") : "1";
  };
  if (filter.since) params.since = chInstant(filter.since);
  if (filter.until) params.until = chInstant(filter.until);
  return { sql, params, window };
}

/** Exactly the SQL of `getSessionOutput`, on the facts. */
export function buildSessionOutputQueries(scope: SessionOutputScope, filter: SessionOutputFilter = {}) {
  const w = where(scope, filter);
  const messages = `
    WITH m AS (
      SELECT DISTINCT source, session_id, fact_key FROM ${OUTPUT_FACTS_TABLE} FINAL
       WHERE ${w.sql} AND fact_kind = 'message' AND ${w.window("ts")}
    )
    SELECT
      (SELECT count() FROM m) +
      (SELECT count() FROM ${OUTPUT_FACTS_TABLE} FINAL
        WHERE ${w.sql} AND fact_kind = 'legacy_turn_message' AND ${w.window("ts")}
          AND (source, session_id) NOT IN (SELECT source, session_id FROM m)) AS messages`;
  const commits = `
    WITH c AS (
      SELECT fact_key AS sha, ts AS at, added, removed FROM ${OUTPUT_FACTS_TABLE} FINAL
       WHERE ${w.sql} AND fact_kind = 'commit' AND (origin != 'tool_result' OR (${w.window("ts")}))
    ),
    full_ids AS (SELECT DISTINCT sha FROM c WHERE length(sha) = 40),
    lens AS (SELECT DISTINCT length(sha) AS n FROM c WHERE length(sha) BETWEEN 1 AND 39),
    uniq AS (
      SELECT substring(f.sha, 1, l.n) AS prefix, min(f.sha) AS resolved
        FROM full_ids AS f CROSS JOIN lens AS l
       GROUP BY prefix HAVING count() = 1
    ),
    g AS (
      SELECT ifNull(u.resolved, c.sha) AS commit_sha, min(c.at) AS first_at, max(c.added) AS a, max(c.removed) AS r
        FROM c LEFT JOIN uniq AS u ON u.prefix = c.sha
       GROUP BY commit_sha
    )
    SELECT count() AS commits, ifNull(sum(a), 0) AS added, ifNull(sum(r), 0) AS removed,
           countIf(a IS NULL OR r IS NULL) AS missing
      FROM g WHERE ${w.window("first_at")}
    SETTINGS join_use_nulls = 1`;
  return { messages, commits, params: w.params };
}

export async function readSessionOutput(
  ch: ClickHouseClient,
  scope: SessionOutputScope,
  filter: SessionOutputFilter = {},
  opts: { timeoutMs?: number; settings?: Record<string, string | number> } = {},
): Promise<SessionOutput> {
  const q = buildSessionOutputQueries(scope, filter);
  const options = { params: q.params, timeoutMs: opts.timeoutMs ?? 3_000, ...(opts.settings ? { settings: opts.settings } : {}) };
  const [[m], [c]] = await Promise.all([
    ch.select<{ messages: string | number }>(q.messages, options),
    ch.select<{ commits: string | number; added: string | number; removed: string | number; missing: string | number }>(q.commits, options),
  ]);
  if (!m || !c) throw new Error("output aggregate returned no row");
  return {
    agentMessages: num(m.messages),
    commits: num(c.commits),
    linesAdded: num(c.added),
    linesRemoved: num(c.removed),
    commitsWithoutLineCounts: num(c.missing),
  };
}

/** The backfill's per-workspace watermark (output-facts-schema.ts): null = not complete. */
export async function outputCompleteSince(
  ch: ClickHouseClient,
  orgId: string,
  workspaceId: string,
  opts: { timeoutMs?: number; settings?: Record<string, string | number> } = {},
): Promise<string | null> {
  const rows = await ch.select<{ complete_since: string }>(
    `SELECT toString(complete_since) AS complete_since FROM ${OUTPUT_FACTS_STATE_TABLE} FINAL
      WHERE org_id = {org:String} AND workspace_id = {workspace:String} LIMIT 1`,
    { params: { org: orgId, workspace: workspaceId }, timeoutMs: opts.timeoutMs ?? 3_000, ...(opts.settings ? { settings: opts.settings } : {}) },
  );
  return rows[0]?.complete_since ?? null;
}

export type OutputReadiness = "ready" | "backfill_incomplete" | "before_watermark" | "session_partial" | "session_unindexed";

/**
 * Whether the facts answer [since, until] exactly for this workspace. Positive, window-scoped:
 *   backfill_incomplete  no backfill watermark row for the workspace
 *   before_watermark     `since` is earlier than the watermark's complete_since
 *   session_partial      a session whose last facts pass was inexact, and whose facts span
 *                        overlaps the window (an unknown span overlaps every window)
 *   session_unindexed    a session with transcript events in the window but NO complete facts
 *                        state row: a missing row is never read as complete
 * Sessions entirely outside the window never block it.
 */
export async function outputWindowReadiness(
  ch: ClickHouseClient,
  orgId: string,
  workspaceId: string,
  since: Date,
  until: Date,
  opts: { timeoutMs?: number; settings?: Record<string, string | number>; untilExclusive?: boolean } = {},
): Promise<OutputReadiness> {
  // The readiness window is exactly the caller's data window: Output is [since, until],
  // the digest [since, until) (untilExclusive).
  const lt = opts.untilExclusive ? "<" : "<=";
  const base = { org: orgId, workspace: workspaceId, since: chInstant(since), until: chInstant(until) };
  const o = { params: base, timeoutMs: opts.timeoutMs ?? 3_000, ...(opts.settings ? { settings: opts.settings } : {}) };
  const watermark = await outputCompleteSince(ch, orgId, workspaceId, { ...(opts.timeoutMs ? { timeoutMs: opts.timeoutMs } : {}), ...(opts.settings ? { settings: opts.settings } : {}) });
  if (!watermark) return "backfill_incomplete";
  // Microsecond compare: Date.parse would truncate the watermark to milliseconds.
  if (chMicros(watermark) > BigInt(since.getTime()) * 1000n) return "before_watermark";
  const [[partial], [unindexed]] = await Promise.all([
    ch.select<{ n: string | number }>(
      `SELECT count() AS n FROM ${OUTPUT_FACTS_SESSION_STATE_TABLE} FINAL
        WHERE org_id = {org:String} AND workspace_id = {workspace:String} AND complete = 0
          AND (isNull(min_ts) OR (max_ts >= toDateTime64({since:String}, 6, 'UTC') AND min_ts ${lt} toDateTime64({until:String}, 6, 'UTC')))`,
      o,
    ),
    ch.select<{ n: string | number }>(
      `SELECT count() AS n FROM (
         SELECT DISTINCT session_id, source FROM ${EVENTS_TABLE} FINAL
          WHERE org_id = {org:String} AND workspace_id = {workspace:String}
            AND ts >= toDateTime64({since:String}, 6, 'UTC') AND ts ${lt} toDateTime64({until:String}, 6, 'UTC'))
        WHERE (session_id, source) NOT IN (
         SELECT session_id, source FROM ${OUTPUT_FACTS_SESSION_STATE_TABLE} FINAL
          WHERE org_id = {org:String} AND workspace_id = {workspace:String} AND complete = 1)`,
      o,
    ),
  ]);
  if (Number(partial?.n ?? 0) > 0) return "session_partial";
  if (Number(unindexed?.n ?? 0) > 0) return "session_unindexed";
  return "ready";
}

/** ClickHouse codes for a table or column the query names but the database lacks (DDL not applied / older skeleton). */
const SCHEMA_SHORT_CODES = new Set(["16", "47", "60", "81"]);
export function isOutputSchemaShort(error: unknown): boolean {
  const code = (error as { code?: unknown } | null)?.code;
  return error instanceof Error && error.name === "ClickHouseError" && code !== undefined && SCHEMA_SHORT_CODES.has(String(code));
}
