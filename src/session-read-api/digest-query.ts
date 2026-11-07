/**
 * The daily digest's deterministic half (`GET /v1/digest/daily` stats, projects, epics,
 * sessions) over the ClickHouse output facts: relayhistory `buildDailyDigest`
 * (lib/digest.ts @ c7de2163) with the same window and grouping rules. Read-only.
 *
 * Window: [since, until) for stats/projects/epics (digestWhere); the sessions list is
 * relayhistory's `listSessions` for `since <= ts <= until - 1 ms`, exactly as it calls it.
 * Optional exact `project_id`. Workspace-scoped only (the read API serves no org-wide digest).
 *
 * outcomes.reverted is OMITTED (Khaliq 2026-11-06, "drop and rebuild"): session outcomes have
 * no producer after the Neon freeze, so a reverted count would be stale. The strict 0.2.0
 * contract requires the key, so it is 0 and the route declares it with
 * `X-Sessions-Digest-Omitted: outcomes.reverted` (consumers hide it). `committed` and
 * `unknown` keep relayhistory's exact rules over the (frozen) legacy outcome facts:
 * committed = has outcome rows and none reverted; unknown = no outcome rows. A session
 * whose rows include a revert is therefore in neither bucket, as in relayhistory, and the
 * three no longer sum to sessionCount while reverted is omitted.
 *
 * sessions: the window aggregates come from the facts; the session-level display fields
 * (title: lane F2's brief titles; workState; catalog) come from the P3b directory row.
 * `summary` is the window taskTitle condensed; relayhistory's opening-user-turn fallback
 * (first_user_turn) has no SP source yet, so such a session's summary is null (a known,
 * parity-visible difference; see the F4 map).
 */
import type { ClickHouseClient, QueryParams } from "../session-pipeline/vendor/search-index/clickhouse.ts";
import { EVENT_FACTS_TABLE, OUTPUT_FACTS_TABLE } from "../session-pipeline/output-facts-schema.ts";
import { chInstant } from "./output-query.ts";
import { condenseTurn, sessionFromDirectoryRow, type Directory } from "./directory.ts";

export const MAX_ROLLUP_PROJECTS = 50;
export const MAX_ROLLUP_EPICS = 50;
export const MAX_TOP_FILES = 10;
export const DIGEST_SESSION_LIMIT = 50;
/** The fields this backend declares omitted on every digest (X-Sessions-Digest-Omitted). */
export const DIGEST_OMITTED_FIELDS = ["outcomes.reverted"] as const;

export interface DigestStats {
  sessionCount: number;
  eventCount: number;
  projectCount: number;
  epicCount: number;
  totalCostUsdMicros: number;
  totalInputTokens: number;
  totalOutputTokens: number;
  models: string[];
  filesTouchedCount: number;
  topFiles: Array<{ path: string; sessionCount: number }>;
  outcomes: { committed: number; reverted: number; unknown: number };
}
export interface DigestProjectRollup {
  projectId: string | null; sessionCount: number; eventCount: number; epicCount: number; costUsdMicros: number; firstTs: string; lastTs: string;
}
export interface DigestEpicRollup {
  projectId: string | null; epicKey: string | null; branch: string | null; sessionCount: number; eventCount: number; costUsdMicros: number; firstTs: string; lastTs: string;
}
export type DigestSession = Record<string, unknown>;
export interface DailyDigest {
  stats: DigestStats;
  projects: DigestProjectRollup[];
  epics: DigestEpicRollup[];
  sessions: DigestSession[];
}

export interface DigestParams {
  orgId: string;
  workspaceId: string;
  since: Date;
  until: Date;
  projectId?: string | null;
}

export class DigestDirectoryUnavailable extends Error {
  constructor() {
    super("digest sessions need the session directory");
    this.name = "DigestDirectoryUnavailable";
  }
}

const num = (v: unknown) => Number(v ?? 0);
const nnum = (v: unknown) => (v === null || v === undefined ? null : Number(v));
/** relayhistory toIso of a timestamptz: a JS Date's millisecond ISO. */
const iso = (chText: string) => new Date(`${chText.replace(" ", "T")}Z`).toISOString();
const strings = (v: unknown) => (Array.isArray(v) ? v.filter((x): x is string => typeof x === "string" && x !== "") : []);

function where(p: DigestParams, untilOp: "<" | "<=", until: Date) {
  const params: QueryParams = { org: p.orgId, ws: p.workspaceId, since: chInstant(p.since), until: chInstant(until) };
  let sql = `org_id = {org:String} AND workspace_id = {ws:String}
    AND ts >= toDateTime64({since:String}, 6, 'UTC') AND ts ${untilOp} toDateTime64({until:String}, 6, 'UTC')`;
  if (p.projectId) {
    sql += " AND project_id = {project:String}";
    params.project = p.projectId;
  }
  return { sql, params };
}

/**
 * No SELECT alias may equal a column name the query also reads: ClickHouse would resolve the
 * column reference (in WHERE or inside another aggregate) to the alias (code 184).
 */
export function buildDigestQueries(p: DigestParams) {
  const day = where(p, "<", p.until);
  const list = where(p, "<=", new Date(p.until.getTime() - 1));
  const F = `${EVENT_FACTS_TABLE} FINAL`;
  return {
    params: day.params,
    listParams: list.params,
    stats: `SELECT uniqExact(source, session_id) AS sessions, count() AS events,
        uniqExact(ifNull(project_id, '\\x01')) AS projects,
        uniqExact(concat(ifNull(project_id, '\\x01'), '\\x02', ifNull(branch, '\\x01'))) AS epics,
        ifNull(sum(cost_usd_micros), 0) AS cost, sum(input_tokens) AS input, sum(output_tokens) AS output,
        arraySort(groupUniqArrayIf(assumeNotNull(model), isNotNull(model))) AS models
      FROM ${F} WHERE ${day.sql}`,
    files: `WITH by_path AS (
        SELECT path, uniqExact(source, session_id) AS session_count
          FROM ${F} ARRAY JOIN files_touched AS path WHERE ${day.sql} GROUP BY path)
      SELECT (SELECT count() FROM by_path) AS total, path, session_count
        FROM by_path WHERE isNotNull(path) ORDER BY session_count DESC, path ASC LIMIT ${MAX_TOP_FILES}`,
    filesCount: `SELECT count() AS total FROM (SELECT path FROM ${F} ARRAY JOIN files_touched AS path WHERE ${day.sql} GROUP BY path)`,
    outcomes: `WITH day_sessions AS (SELECT DISTINCT source, session_id FROM ${F} WHERE ${day.sql}),
      o AS (SELECT source, session_id, max(reverted) AS any_reverted FROM ${OUTPUT_FACTS_TABLE} FINAL
             WHERE org_id = {org:String} AND workspace_id = {ws:String} AND fact_kind = 'outcome'
               AND (source, session_id) IN (SELECT source, session_id FROM day_sessions)
             GROUP BY source, session_id)
      SELECT
        (SELECT count() FROM o WHERE any_reverted = 0) AS committed,
        (SELECT count() FROM day_sessions WHERE (source, session_id) NOT IN (SELECT source, session_id FROM o)) AS unknown`,
    projects: `SELECT project_id, uniqExact(source, session_id) AS sessions, count() AS events,
        uniqExact(ifNull(branch, '\\x01')) AS epics, ifNull(sum(cost_usd_micros), 0) AS cost,
        toString(min(ts)) AS first_ts, toString(max(ts)) AS last_ts
      FROM ${F} WHERE ${day.sql} GROUP BY project_id ORDER BY max(ts) DESC LIMIT ${MAX_ROLLUP_PROJECTS}`,
    epics: `SELECT project_id, branch,
        if(countIf(isNotNull(task_ref_id)) = 0, NULL, argMaxIf(task_ref_id, (ts, session_id), isNotNull(task_ref_id))) AS epic_key,
        uniqExact(source, session_id) AS sessions, count() AS events, ifNull(sum(cost_usd_micros), 0) AS cost,
        toString(min(ts)) AS first_ts, toString(max(ts)) AS last_ts
      FROM ${F} WHERE ${day.sql} GROUP BY project_id, branch ORDER BY max(ts) DESC LIMIT ${MAX_ROLLUP_EPICS}`,
    sessions: `SELECT session_id, source,
        arraySort(groupUniqArrayIf(assumeNotNull(user_id), isNotNull(user_id))) AS user_ids,
        max(project_id) AS max_project, arraySort(groupUniqArray(machine_id)) AS machine_ids,
        toString(min(ts)) AS first_ts, toString(max(ts)) AS last_ts, count() AS events,
        arraySort(groupUniqArray(kind)) AS kinds,
        arraySort(groupUniqArrayIf(assumeNotNull(model), isNotNull(model))) AS models,
        if(countIf(isNotNull(task_title)) = 0, NULL, argMaxIf(task_title, ts, isNotNull(task_title))) AS latest_title,
        arraySort(groupUniqArrayIf(task_ref_json, task_ref_empty = 0)) AS task_refs,
        sum(cost_usd_micros) AS cost, sum(input_tokens) AS input, sum(output_tokens) AS output,
        sum(reasoning_tokens) AS reasoning, sum(cache_read_tokens) AS cache_read, sum(cache_create_tokens) AS cache_create,
        min(cache_create_tokens = 0 OR (ifNull(cache_create_5m_tokens, 0) + ifNull(cache_create_1h_tokens, 0) = cache_create_tokens
            AND (isNotNull(cache_create_5m_tokens) OR isNotNull(cache_create_1h_tokens)))) AS split_complete,
        ifNull(sum(cache_create_5m_tokens), 0) AS cache_5m, ifNull(sum(cache_create_1h_tokens), 0) AS cache_1h
      FROM ${F} WHERE ${list.sql}
      GROUP BY session_id, source ORDER BY max(ts) DESC, session_id DESC, source DESC LIMIT ${DIGEST_SESSION_LIMIT}`,
  };
}

type Opts = { timeoutMs?: number; settings?: Record<string, string | number> };

export async function buildDailyDigest(ch: ClickHouseClient, directory: Directory | undefined, p: DigestParams, opts: Opts = {}): Promise<DailyDigest> {
  const q = buildDigestQueries(p);
  const o = (params: QueryParams) => ({ params, timeoutMs: opts.timeoutMs ?? 3_000, ...(opts.settings ? { settings: opts.settings } : {}) });
  const [[s], files, [fc], [oc], projects, epics, sessions] = await Promise.all([
    ch.select<Record<string, any>>(q.stats, o(q.params)),
    ch.select<Record<string, any>>(q.files, o(q.params)),
    ch.select<Record<string, any>>(q.filesCount, o(q.params)),
    ch.select<Record<string, any>>(q.outcomes, o(q.params)),
    ch.select<Record<string, any>>(q.projects, o(q.params)),
    ch.select<Record<string, any>>(q.epics, o(q.params)),
    ch.select<Record<string, any>>(q.sessions, o(q.listParams)),
  ]);
  if (!s || !fc || !oc) throw new Error("digest aggregate returned no row");
  const stats: DigestStats = {
    sessionCount: num(s.sessions),
    eventCount: num(s.events),
    projectCount: num(s.projects),
    epicCount: num(s.epics),
    totalCostUsdMicros: num(s.cost),
    totalInputTokens: num(s.input),
    totalOutputTokens: num(s.output),
    models: strings(s.models),
    filesTouchedCount: num(fc.total),
    topFiles: files.map((f) => ({ path: String(f.path), sessionCount: num(f.session_count) })),
    outcomes: { committed: num(oc.committed), reverted: 0, unknown: num(oc.unknown) },
  };
  return {
    stats,
    projects: projects.map((r) => ({
      projectId: r.project_id ?? null, sessionCount: num(r.sessions), eventCount: num(r.events), epicCount: num(r.epics),
      costUsdMicros: num(r.cost), firstTs: iso(r.first_ts), lastTs: iso(r.last_ts),
    })),
    epics: epics.map((r) => ({
      projectId: r.project_id ?? null, epicKey: r.epic_key ?? null, branch: r.branch ?? null, sessionCount: num(r.sessions),
      eventCount: num(r.events), costUsdMicros: num(r.cost), firstTs: iso(r.first_ts), lastTs: iso(r.last_ts),
    })),
    sessions: await digestSessions(directory, p, sessions),
  };
}

/** Window aggregates in relayhistory's completeSessionPage key order, display fields from the directory. */
async function digestSessions(directory: Directory | undefined, p: DigestParams, rows: Array<Record<string, any>>): Promise<DigestSession[]> {
  if (!rows.length) return [];
  if (!directory) throw new DigestDirectoryUnavailable();
  const keys = rows.map((r) => ({ harness: String(r.source), sessionId: String(r.session_id) }));
  const dir = await directory.list(p.orgId, p.workspaceId, { limit: 200, sessionKeys: keys });
  if (dir === null) throw new DigestDirectoryUnavailable();
  const byKey = new Map<string, ReturnType<typeof sessionFromDirectoryRow>>();
  for (const row of dir) {
    if (row.org_id !== p.orgId || row.workspace_id !== p.workspaceId) throw new Error("digest directory tenant mismatch");
    byKey.set(JSON.stringify([row.harness, row.session_id]), sessionFromDirectoryRow(row));
  }
  return rows.map((r) => {
    const d = byKey.get(JSON.stringify([String(r.source), String(r.session_id)]));
    const taskTitle: string | null = r.latest_title ?? null;
    const titled = typeof taskTitle === "string" && taskTitle.trim() !== "";
    const split = Number(r.split_complete) === 1;
    return {
      title: d?.title ?? null,
      sessionId: String(r.session_id),
      userIds: strings(r.user_ids).sort(),
      source: String(r.source),
      projectId: r.max_project ?? null,
      machineIds: strings(r.machine_ids),
      firstTs: iso(r.first_ts),
      lastTs: iso(r.last_ts),
      workState: d?.workState ?? null,
      eventCount: num(r.events),
      kinds: strings(r.kinds),
      models: strings(r.models),
      taskTitle,
      summary: titled ? condenseTurn(taskTitle) : null,
      summarySource: titled ? "task_title" : null,
      taskRefs: strings(r.task_refs).map((j) => JSON.parse(j) as unknown),
      totalCostUsdMicros: nnum(r.cost),
      totalInputTokens: num(r.input),
      totalOutputTokens: num(r.output),
      totalReasoningTokens: num(r.reasoning),
      totalCacheReadTokens: num(r.cache_read),
      totalCacheCreateTokens: num(r.cache_create),
      totalCacheCreate5mTokens: split ? num(r.cache_5m) : null,
      totalCacheCreate1hTokens: split ? num(r.cache_1h) : null,
      ...(d?.catalog ? { catalog: d.catalog } : {}),
    };
  });
}

/** relayhistory computeStatsHash: sha256 hex over the same JSON (key order included). */
export async function computeStatsHash(digest: DailyDigest, workspaceId?: string): Promise<string> {
  const text = JSON.stringify({ ...(workspaceId ? { workspaceId } : {}), stats: digest.stats, projects: digest.projects, epics: digest.epics, sessions: digest.sessions });
  const bytes = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(text));
  return [...new Uint8Array(bytes)].map((b) => b.toString(16).padStart(2, "0")).join("");
}
