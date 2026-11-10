/** Facts-store abstraction for GET /v1/coordination/overlaps. */
import type { ClickHouseClient, QueryParams } from "../session-pipeline/vendor/search-index/clickhouse.ts";
import { EVENT_FACTS_TABLE, OUTPUT_FACTS_TABLE } from "../session-pipeline/output-facts-schema.ts";
import type { LaneFacts } from "../../packages/coordination-detect/src/index.ts";
import type { FactsBackendKind } from "./config.ts";
import type { Directory, DirectoryListRow } from "./directory.ts";
import type { FactsReadOptions, OutputReadiness } from "./output-backend.ts";
import { clickhouseOutputBackend } from "./output-backend.ts";
import type { Liveness } from "./page.ts";
import { keyOf } from "./page.ts";

export const COORDINATION_FACTS_ROW_CAP = 10_000;

export interface CoordinationParams {
  orgId: string;
  workspaceId: string;
  since: Date;
  until: Date;
  projectId?: string | null;
  rowCap?: number;
}

export interface CoordinationFacts {
  lanes: LaneFacts[];
  sessionsTruncated: number;
  notes: string[];
}

export interface CoordinationBackend {
  readonly kind: FactsBackendKind;
  windowReadiness(orgId: string, workspaceId: string, since: Date, until: Date, opts?: FactsReadOptions & { untilExclusive?: boolean }): Promise<OutputReadiness>;
  readFacts(directory: Directory | undefined, params: CoordinationParams, opts?: FactsReadOptions): Promise<CoordinationFacts>;
  isSchemaMissing(error: unknown): boolean;
}

export interface CoordinationEventRow {
  ts: string; session_id: string; source: string; event_id: string; kind: string; project_id: string | null;
  task_ref_id: string | null; task_title: string | null; user_id: string | null; branch: string | null;
  input_tokens: string | number | bigint; output_tokens: string | number | bigint; reasoning_tokens: string | number | bigint;
  cost_usd_micros: string | number | bigint | null; files_touched: Array<string | null> | null; row_id: string;
}
export interface CoordinationOutputRow {
  fact_kind: string; ts: string; scope_source: string; source: string; session_id: string; fact_key: string;
  origin: string; reverted: string | number; row_id: string;
}

const compare = (a: string, b: string) => a < b ? -1 : a > b ? 1 : 0;
const key = (source: string, sessionId: string) => `${source}\u0000${sessionId}`;
const iso = (value: Date | string) => value instanceof Date ? value.toISOString() : new Date(value.includes("T") ? value : `${value.replace(" ", "T")}Z`).toISOString();
const number = (value: string | number | bigint) => Number(value);

async function directoryRows(directory: Directory | undefined, p: CoordinationParams, keys: Array<{ harness: string; sessionId: string }>) {
  if (!directory || !keys.length) return { rows: [] as DirectoryListRow[], unavailable: keys.length > 0 };
  const rows: DirectoryListRow[] = [];
  for (let i = 0; i < keys.length; i += 200) {
    const part = keys.slice(i, i + 200);
    const page = await directory.list(p.orgId, p.workspaceId, { limit: 200, sessionKeys: part });
    if (page === null) return { rows: [] as DirectoryListRow[], unavailable: true };
    for (const row of page) {
      if (row.org_id !== p.orgId || row.workspace_id !== p.workspaceId) throw new Error("coordination directory tenant mismatch");
      rows.push(row);
    }
  }
  const found = new Set(rows.map((row) => key(row.harness, row.session_id)));
  return { rows, unavailable: keys.some(({ harness, sessionId }) => !found.has(key(harness, sessionId))) };
}

/** Normalize both facts stores to the dependency-free detector input. */
export async function normalizeCoordinationFacts(
  directory: Directory | undefined,
  p: CoordinationParams,
  events: CoordinationEventRow[],
  outputs: CoordinationOutputRow[],
  truncated: boolean,
): Promise<CoordinationFacts> {
  const filtered = events.filter((event) => !p.projectId || event.project_id === p.projectId);
  const groups = new Map<string, CoordinationEventRow[]>();
  const identities = new Map<string, { harness: string; sessionId: string }>();
  for (const event of filtered) {
    const k = key(event.source, event.session_id);
    const rows = groups.get(k) ?? [];
    rows.push(event);
    groups.set(k, rows);
    identities.set(k, { harness: event.source, sessionId: event.session_id });
  }
  // Output-only lanes are material for cross-lane reverts. Under a project filter, however,
  // output facts have no project column, so retain only lanes corroborated by a filtered event.
  if (!p.projectId) for (const output of outputs) {
    const k = key(output.scope_source, output.session_id);
    identities.set(k, { harness: output.scope_source, sessionId: output.session_id });
  }
  const keys = [...identities].map(([k, identity]) => ({ k, ...identity }));
  const directoryResult = await directoryRows(directory, p, keys.map(({ harness, sessionId }) => ({ harness, sessionId })));
  const byDirectoryKey = new Map(directoryResult.rows.map((row) => [key(row.harness, row.session_id), row]));
  const outputsByKey = new Map<string, CoordinationOutputRow[]>();
  for (const output of outputs) {
    // Output facts are owned by the canonical session scope. `source` is retained for
    // fact provenance (and can differ for legacy turns); `scope_source` is the harness
    // used by event facts, liveness, and every other output-facts aggregate.
    const k = key(output.scope_source, output.session_id);
    if (!identities.has(k)) continue;
    const rows = outputsByKey.get(k) ?? [];
    rows.push(output);
    outputsByKey.set(k, rows);
  }
  const lanes: LaneFacts[] = [];
  for (const { k, harness, sessionId } of keys) {
    const rows = (groups.get(k) ?? []).sort((a, b) => compare(a.ts, b.ts) || compare(a.event_id, b.event_id));
    const out = (outputsByKey.get(k) ?? []).sort((a, b) => compare(a.ts, b.ts) || compare(a.row_id, b.row_id));
    const d = byDirectoryKey.get(k);
    const latest = <K extends "user_id" | "project_id" | "branch" | "task_ref_id" | "task_title">(field: K) => {
      for (let i = rows.length - 1; i >= 0; i--) if (rows[i]![field] !== null) return rows[i]![field] as string;
      return null;
    };
    const factTimes = [...rows.map((r) => iso(r.ts)), ...out.map((r) => iso(r.ts))].sort(compare);
    const lastAt = factTimes.at(-1)!;
    const parents = d?.parent_session_ids?.filter(Boolean) ?? [];
    const directoryLastAt = d ? iso(d.last_activity_at) : null;
    const usage: LaneFacts["usage"] = rows.map((event) => ({
      ts: iso(event.ts), eventId: event.event_id,
      tokens: number(event.input_tokens) + number(event.output_tokens) + number(event.reasoning_tokens),
      costUsdMicros: event.cost_usd_micros === null ? null : number(event.cost_usd_micros),
    }));
    // Directory activity after the requested facts window means the requested rows cannot price
    // through the lane's true last action. A null sentinel makes the detector say unknown.
    if (directoryLastAt && directoryLastAt > lastAt)
      usage.push({ ts: directoryLastAt, eventId: "coverage:directory-last-action", tokens: null, costUsdMicros: null });
    lanes.push({
      sessionId,
      source: harness,
      userId: latest("user_id"),
      projectId: latest("project_id"),
      repo: d?.git_repo ?? null,
      branch: latest("branch") ?? d?.git_branch ?? null,
      taskRef: latest("task_ref_id"),
      title: d?.title ?? latest("task_title"),
      parentSessionId: new Set(parents).size === 1 ? parents[0]! : null,
      startedAt: d ? iso(d.created_at) : factTimes[0]!,
      lastActionAt: directoryLastAt ? [directoryLastAt, lastAt].sort(compare).at(-1)! : lastAt,
      writes: rows.flatMap((event) => (event.files_touched ?? []).filter((path): path is string => typeof path === "string" && path !== "")
        .map((path) => ({ path, ts: iso(event.ts), eventId: event.event_id, kind: event.kind, branch: event.branch }))),
      commits: out.filter((fact) => fact.fact_kind === "commit").map((fact) => ({
        sha: fact.fact_key, ts: iso(fact.ts), eventId: fact.row_id, reverted: null,
      })),
      outcomes: out.filter((fact) => fact.fact_kind === "outcome").map((fact) => ({
        key: fact.fact_key, ts: iso(fact.ts), eventId: fact.row_id, reverted: Number(fact.reverted) !== 0,
      })),
      usage,
    });
  }
  lanes.sort((a, b) => compare(a.source, b.source) || compare(a.sessionId, b.sessionId));
  return {
    lanes,
    sessionsTruncated: truncated ? 1 : 0,
    notes: [
      ...(truncated ? [`facts row cap ${p.rowCap ?? COORDINATION_FACTS_ROW_CAP} reached; at least one session may be omitted`] : []),
      ...(directoryResult.unavailable ? ["session directory unavailable; titles and parent relationships may be incomplete"] : []),
      "agent-to-agent communication evidence is unavailable in the facts store",
    ],
  };
}

const chTime = (d: Date) => d.toISOString().replace("T", " ").replace("Z", "");

export function clickhouseCoordinationBackend(ch: ClickHouseClient, liveness: Liveness): CoordinationBackend {
  const output = clickhouseOutputBackend(ch);
  return {
    kind: "clickhouse",
    windowReadiness: output.windowReadiness,
    isSchemaMissing: output.isSchemaMissing,
    async readFacts(directory, p, opts = {}) {
      const cap = Math.max(1, Math.min(COORDINATION_FACTS_ROW_CAP, Math.trunc(p.rowCap ?? COORDINATION_FACTS_ROW_CAP)));
      const params: QueryParams = { org: p.orgId, ws: p.workspaceId, since: chTime(p.since), until: chTime(p.until) };
      const o = { params, timeoutMs: opts.timeoutMs ?? 3_000, ...(opts.settings ? { settings: opts.settings } : {}) };
      const [eventRows, outputRows] = await Promise.all([
        ch.select<CoordinationEventRow>(`SELECT toString(ts) AS ts, session_id, source, event_id, kind, project_id,
            task_ref_id, task_title, user_id, branch, input_tokens, output_tokens, reasoning_tokens,
            cost_usd_micros, files_touched, row_id
          FROM ${EVENT_FACTS_TABLE} FINAL
         WHERE org_id = {org:String} AND workspace_id = {ws:String}
           AND ts >= toDateTime64({since:String}, 6, 'UTC') AND ts < toDateTime64({until:String}, 6, 'UTC')
         ORDER BY row_id LIMIT ${cap + 1}`, o),
        ch.select<CoordinationOutputRow>(`SELECT fact_kind, toString(ts) AS ts, scope_source, source, session_id, fact_key, origin, reverted, row_id
          FROM ${OUTPUT_FACTS_TABLE} FINAL
         WHERE org_id = {org:String} AND workspace_id = {ws:String}
           AND ts >= toDateTime64({since:String}, 6, 'UTC') AND ts < toDateTime64({until:String}, 6, 'UTC')
         ORDER BY row_id LIMIT ${cap + 1}`, o),
      ]);
      const truncated = eventRows.length > cap || outputRows.length > cap;
      const events = eventRows.slice(0, cap);
      const outputs = outputRows.slice(0, cap);
      const candidates = [...new Map([
        ...events.map((r) => [key(r.source, r.session_id), { source: r.source, sessionId: r.session_id }] as const),
        ...outputs.map((r) => [key(r.scope_source, r.session_id), { source: r.scope_source, sessionId: r.session_id }] as const),
      ]).values()];
      const erased = new Set(await liveness.erased(p.orgId, p.workspaceId, candidates));
      return normalizeCoordinationFacts(directory, p,
        events.filter((r) => !erased.has(keyOf({ source: r.source, sessionId: r.session_id }))),
        outputs.filter((r) => !erased.has(keyOf({ source: r.scope_source, sessionId: r.session_id }))), truncated);
    },
  };
}
