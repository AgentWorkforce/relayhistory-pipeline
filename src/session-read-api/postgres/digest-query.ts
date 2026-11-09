/**
 * The daily digest's deterministic half over the self-host Postgres facts (migration
 * 20261105045040): clickhouse/digest-query.ts buildDailyDigest, the same DailyDigest for the
 * same facts, so computeStatsHash (and the narrative cache key) agree across backends.
 *
 * Reads only through 045040's readers (postgres/output-query.ts), in one READ ONLY tenant
 * transaction on the read pool, under the request deadline:
 *   the day's event facts [since, until), and every output fact of the workspace (outcome
 *   facts are unwindowed, as in ClickHouse; the reader has no kind filter).
 * Erased sessions are dropped first (liveness), then each ClickHouse aggregate is ported:
 *   uniqExact(a, b)          a Set of the (a, b) pair
 *   groupUniqArray + sort    unique values in code-point (= UTF-8 byte) order; NULLs skipped by
 *                            the *If variants, as there
 *   sum over Nullable        NULL when every value is NULL (sessions cost), else wrapped in ifNull
 *   max(project_id)          byte-order max, NULLs skipped
 *   argMaxIf(x, (ts, sid))   the row with the greatest (ts, session_id) among non-NULL x
 *   NOT IN (subquery)        a Set lookup
 * ClickHouse leaves three orders open, which this fixes (and documents) deterministically:
 *   projects / epics         ORDER BY max(ts) DESC, then project_id (then branch) ascending,
 *                            NULL last
 *   epic_key ties            equal (ts, session_id): the greatest task_ref_id
 *   latest_title ties        equal ts: the greatest task_title
 * The sessions list window is `since <= ts <= until - 1 ms`, as in ClickHouse (and relayhistory).
 */
import { DIGEST_SESSION_LIMIT, DigestDirectoryUnavailable, MAX_ROLLUP_EPICS, MAX_ROLLUP_PROJECTS, MAX_TOP_FILES, type DailyDigest, type DigestParams, type DigestSession, type DigestStats } from "../clickhouse/digest-query.ts";
import { chInstant } from "../clickhouse/output-query.ts";
import { condenseTurn, sessionFromDirectoryRow, type Directory } from "../directory.ts";
import type { DigestBackend } from "../digest-backend.ts";
import {
  compareText, dateMicros, deadlineOf, dropErased, EVENT_FACTS_SQL, isPostgresFactsSchemaMissing, microsText, OUTPUT_FACTS_SQL,
  postgresWindowReadiness, readPages, UNBOUNDED, type EventFact, type OutputFact, type PostgresFactsDeps,
} from "./output-query.ts";

const num = (v: unknown) => Number(v ?? 0);
const nnum = (v: unknown) => (v === null || v === undefined ? null : Number(v));
/** relayhistory toIso of a timestamptz: a JS Date's millisecond ISO. */
const iso = (chText: string) => new Date(`${chText.replace(" ", "T")}Z`).toISOString();
const strings = (v: unknown) => (Array.isArray(v) ? v.filter((x): x is string => typeof x === "string" && x !== "") : []);

const pair = (a: string | null, b: string | null) => JSON.stringify([a, b]);
const sortedUnique = (values: Iterable<string>) => [...new Set(values)].sort(compareText);
const nonNull = <T>(v: T | null | undefined): v is T => v !== null && v !== undefined;
const big = (v: unknown) => (v === null || v === undefined ? null : BigInt(typeof v === "number" ? Math.trunc(v) : (v as string | bigint)));
/** sum() over a Nullable column: NULL when every value is NULL. */
function sumNullable(values: unknown[]): bigint | null {
  let total: bigint | null = null;
  for (const v of values) { const b = big(v); if (b !== null) total = (total ?? 0n) + b; }
  return total;
}
const sum = (values: unknown[]) => sumNullable(values) ?? 0n;
/** Byte-order compare, NULL after every string (the documented project/branch tie-break). */
const nullLast = (a: string | null, b: string | null) => (a === b ? 0 : a === null ? 1 : b === null ? -1 : compareText(a, b));
const maxTs = (rows: EventFact[]) => rows.reduce((m, r) => (r.ts > m ? r.ts : m), rows[0]!.ts);
const minTs = (rows: EventFact[]) => rows.reduce((m, r) => (r.ts < m ? r.ts : m), rows[0]!.ts);
function groupBy<K, R>(rows: R[], key: (r: R) => K): Map<K, R[]> {
  const out = new Map<K, R[]>();
  for (const r of rows) { const k = key(r); const g = out.get(k); if (g) g.push(r); else out.set(k, [r]); }
  return out;
}
const sessionsOf = (rows: EventFact[]) => new Set(rows.map((r) => pair(r.source, r.session_id))).size;

/** The digest aggregates over already-fetched, liveness-filtered facts (exported for the parity test). */
export function digestAggregates(p: DigestParams, events: EventFact[], outputs: OutputFact[]) {
  const sinceText = chInstant(p.since);
  const untilText = chInstant(p.until);
  const listUntilText = chInstant(new Date(p.until.getTime() - 1));
  const scoped = events.filter((e) => e.ts >= sinceText && (!p.projectId || e.project_id === p.projectId));
  const day = scoped.filter((e) => e.ts < untilText);
  const list = scoped.filter((e) => e.ts <= listUntilText);

  // stats
  const s = {
    sessions: sessionsOf(day),
    events: day.length,
    projects: new Set(day.map((e) => e.project_id ?? "\x01")).size,
    epics: new Set(day.map((e) => `${e.project_id ?? "\x01"}\x02${e.branch ?? "\x01"}`)).size,
    cost: sum(day.map((e) => e.cost_usd_micros)),
    input: sum(day.map((e) => e.input_tokens)),
    output: sum(day.map((e) => e.output_tokens)),
    models: sortedUnique(day.map((e) => e.model).filter(nonNull)),
  };

  // files: ARRAY JOIN files_touched (an empty array yields no row); the NULL path is a group too.
  const byPath = new Map<string | null, Set<string>>();
  for (const e of day)
    for (const path of e.files_touched ?? []) {
      const k = path ?? null;
      const g = byPath.get(k);
      if (g) g.add(pair(e.source, e.session_id)); else byPath.set(k, new Set([pair(e.source, e.session_id)]));
    }
  const files = [...byPath].filter(([path]) => path !== null)
    .map(([path, set]) => ({ path: path!, session_count: set.size }))
    .sort((a, b) => b.session_count - a.session_count || compareText(a.path, b.path))
    .slice(0, MAX_TOP_FILES);

  // outcomes: unwindowed outcome facts of the day's (source, session_id).
  const daySessions = new Set(day.map((e) => pair(e.source, e.session_id)));
  const anyReverted = new Map<string, number>();
  for (const f of outputs) {
    if (f.fact_kind !== "outcome") continue;
    const k = pair(f.source, f.session_id);
    if (!daySessions.has(k)) continue;
    anyReverted.set(k, Math.max(anyReverted.get(k) ?? 0, Number(f.reverted)));
  }
  const committed = [...anyReverted.values()].filter((v) => v === 0).length;
  const unknown = [...daySessions].filter((k) => !anyReverted.has(k)).length;

  // projects
  const projects = [...groupBy(day, (e) => e.project_id ?? null)]
    .map(([projectId, rows]) => ({ projectId, rows, last: maxTs(rows) }))
    .sort((a, b) => (a.last === b.last ? nullLast(a.projectId, b.projectId) : a.last < b.last ? 1 : -1))
    .slice(0, MAX_ROLLUP_PROJECTS)
    .map(({ projectId, rows, last }) => ({
      project_id: projectId, sessions: sessionsOf(rows), events: rows.length,
      epics: new Set(rows.map((e) => e.branch ?? "\x01")).size, cost: sum(rows.map((e) => e.cost_usd_micros)),
      first_ts: minTs(rows), last_ts: last,
    }));

  // epics: argMaxIf(task_ref_id, (ts, session_id), isNotNull(task_ref_id))
  const epics = [...groupBy(day, (e) => pair(e.project_id ?? null, e.branch ?? null))]
    .map(([, rows]) => ({ rows, last: maxTs(rows), projectId: rows[0]!.project_id ?? null, branch: rows[0]!.branch ?? null }))
    .sort((a, b) => (a.last === b.last ? nullLast(a.projectId, b.projectId) || nullLast(a.branch, b.branch) : a.last < b.last ? 1 : -1))
    .slice(0, MAX_ROLLUP_EPICS)
    .map(({ rows, last, projectId, branch }) => {
      let best: EventFact | null = null;
      for (const r of rows) {
        if (r.task_ref_id === null || r.task_ref_id === undefined) continue;
        if (!best || r.ts > best.ts || (r.ts === best.ts && (compareText(r.session_id, best.session_id) > 0
          || (r.session_id === best.session_id && compareText(r.task_ref_id, best.task_ref_id!) > 0)))) best = r;
      }
      return {
        project_id: projectId, branch, epic_key: best?.task_ref_id ?? null, sessions: sessionsOf(rows), events: rows.length,
        cost: sum(rows.map((e) => e.cost_usd_micros)), first_ts: minTs(rows), last_ts: last,
      };
    });

  // sessions (the list window)
  const sessions = [...groupBy(list, (e) => pair(e.session_id, e.source))]
    .map(([, rows]) => ({ rows, last: maxTs(rows), session_id: rows[0]!.session_id, source: rows[0]!.source }))
    .sort((a, b) => (a.last !== b.last ? (a.last < b.last ? 1 : -1) : -compareText(a.session_id, b.session_id) || -compareText(a.source, b.source)))
    .slice(0, DIGEST_SESSION_LIMIT)
    .map(({ rows, last, session_id, source }) => {
      let title: EventFact | null = null;
      for (const r of rows) {
        if (r.task_title === null || r.task_title === undefined) continue;
        if (!title || r.ts > title.ts || (r.ts === title.ts && compareText(r.task_title, title.task_title!) > 0)) title = r;
      }
      const projectIds = rows.map((r) => r.project_id).filter(nonNull).sort(compareText);
      const splitComplete = rows.every((r) => {
        const cc = big(r.cache_create_tokens)!, m5 = big(r.cache_create_5m_tokens), h1 = big(r.cache_create_1h_tokens);
        return cc === 0n || ((m5 ?? 0n) + (h1 ?? 0n) === cc && (m5 !== null || h1 !== null));
      });
      return {
        session_id, source,
        user_ids: sortedUnique(rows.map((r) => r.user_id).filter(nonNull)),
        max_project: projectIds.length ? projectIds[projectIds.length - 1]! : null,
        machine_ids: sortedUnique(rows.map((r) => r.machine_id)),
        first_ts: minTs(rows), last_ts: last, events: rows.length,
        kinds: sortedUnique(rows.map((r) => r.kind)),
        models: sortedUnique(rows.map((r) => r.model).filter(nonNull)),
        latest_title: title?.task_title ?? null,
        task_refs: sortedUnique(rows.filter((r) => Number(r.task_ref_empty) === 0).map((r) => r.task_ref_json)),
        cost: sumNullable(rows.map((r) => r.cost_usd_micros)),
        input: sum(rows.map((r) => r.input_tokens)), output: sum(rows.map((r) => r.output_tokens)),
        reasoning: sum(rows.map((r) => r.reasoning_tokens)), cache_read: sum(rows.map((r) => r.cache_read_tokens)),
        cache_create: sum(rows.map((r) => r.cache_create_tokens)),
        split_complete: splitComplete ? 1 : 0,
        cache_5m: sum(rows.map((r) => r.cache_create_5m_tokens)), cache_1h: sum(rows.map((r) => r.cache_create_1h_tokens)),
      };
    });

  return { s, files, filesTotal: byPath.size, outcomes: { committed, unknown }, projects, epics, sessions };
}

/** The ClickHouse buildDailyDigest result mapping, over digestAggregates. */
export async function dailyDigestFromFacts(directory: Directory | undefined, p: DigestParams, events: EventFact[], outputs: OutputFact[]): Promise<DailyDigest> {
  const a = digestAggregates(p, events, outputs);
  const stats: DigestStats = {
    sessionCount: num(a.s.sessions),
    eventCount: num(a.s.events),
    projectCount: num(a.s.projects),
    epicCount: num(a.s.epics),
    totalCostUsdMicros: num(a.s.cost),
    totalInputTokens: num(a.s.input),
    totalOutputTokens: num(a.s.output),
    models: strings(a.s.models),
    filesTouchedCount: num(a.filesTotal),
    topFiles: a.files.map((f) => ({ path: String(f.path), sessionCount: num(f.session_count) })),
    outcomes: { committed: num(a.outcomes.committed), reverted: 0, unknown: num(a.outcomes.unknown) },
  };
  return {
    stats,
    projects: a.projects.map((r) => ({
      projectId: r.project_id ?? null, sessionCount: num(r.sessions), eventCount: num(r.events), epicCount: num(r.epics),
      costUsdMicros: num(r.cost), firstTs: iso(r.first_ts), lastTs: iso(r.last_ts),
    })),
    epics: a.epics.map((r) => ({
      projectId: r.project_id ?? null, epicKey: r.epic_key ?? null, branch: r.branch ?? null, sessionCount: num(r.sessions),
      eventCount: num(r.events), costUsdMicros: num(r.cost), firstTs: iso(r.first_ts), lastTs: iso(r.last_ts),
    })),
    sessions: await digestSessions(directory, p, a.sessions),
  };
}

/** clickhouse/digest-query.ts digestSessions, verbatim: window aggregates, display fields from the directory. */
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

/** buildDailyDigest over the Postgres facts: one tenant transaction, then liveness, then the aggregates. */
export async function buildDailyDigestPostgres(deps: PostgresFactsDeps, directory: Directory | undefined, p: DigestParams, opts: { timeoutMs?: number } = {}): Promise<DailyDigest> {
  const now = deps.now ?? Date.now;
  const deadline = deadlineOf(deps, opts.timeoutMs);
  const from = microsText(dateMicros(p.since));
  const to = microsText(dateMicros(p.until));
  const { events, outputs } = await deps.run(p.orgId, p.workspaceId, async (q) => ({
    events: await readPages<EventFact>(q, EVENT_FACTS_SQL, [p.workspaceId, from, to], deadline, now, deps.pageLimit),
    outputs: await readPages<OutputFact>(q, OUTPUT_FACTS_SQL, [p.workspaceId, ...UNBOUNDED], deadline, now, deps.pageLimit),
  }));
  const liveEvents = await dropErased(deps.liveness, p.orgId, p.workspaceId, events, (e) => ({ source: e.source, sessionId: e.session_id }));
  const liveOutputs = await dropErased(deps.liveness, p.orgId, p.workspaceId, outputs, (f) => ({ source: f.scope_source, sessionId: f.session_id }));
  return dailyDigestFromFacts(directory, p, liveEvents, liveOutputs);
}

/** The digest backend over the Postgres facts (READ_API_DIGEST_BACKEND=postgres). */
export function postgresDigestBackend(deps: PostgresFactsDeps): DigestBackend {
  return {
    kind: "postgres",
    windowReadiness: (orgId, workspaceId, since, until, opts = {}) => postgresWindowReadiness(deps, orgId, workspaceId, since, until, opts),
    buildDailyDigest: (directory, params, opts = {}) => buildDailyDigestPostgres(deps, directory, params, opts),
    isSchemaMissing: isPostgresFactsSchemaMissing,
  };
}
