/**
 * Output (`GET /v1/sessions/output`) and its window readiness over the self-host Postgres facts
 * (migration 20261105045040): the same result as clickhouse/output-query.ts for the same facts.
 *
 * Access is only through 045040's SECURITY DEFINER readers, EXECUTE to bulls96_read_api alone,
 * on the read pool inside one READ ONLY tenant transaction (liveness.ts poolTenantRunner):
 *   output_facts_events_for / output_facts_outputs_for(ws, from, to, after_row_id, limit)
 *       keyset pages (row_id order, at most 10000 rows) of `from <= ts < to`
 *   output_facts_coverage_for(ws, from, to)
 *       complete_since, and the count of state rows (and incomplete ones) overlapping the window
 * The readers return rows, not aggregates, so the ClickHouse SQL's aggregation runs here, over
 * every page, in one transaction (one snapshot per statement, READ COMMITTED as every tenant
 * read) under the request deadline. Timestamps travel as DateTime64-shaped UTC text
 * (`YYYY-MM-DD HH:MM:SS.ffffff`), so comparisons are microsecond-exact, as in ClickHouse.
 *
 * Erased sessions (every session_index pointer tombstoned, liveness.ts) are dropped before any
 * aggregation: events by (source, session_id), output facts by (scope_source, session_id).
 * Facts of an erased session are deleted by the consumer's erase; this closes the window until
 * it runs (a 043910 tombstone not yet processed).
 *
 * Window mapping: Output's inclusive [since, until] reads `to = until + 1 µs`; the digest's
 * [since, until) reads `to = until`. The commit candidates of every origin but tool_result are
 * unwindowed (as in Neon and ClickHouse): read with an infinite window.
 *
 * Readiness, in the ClickHouse order:
 *   backfill_incomplete  coverage.complete_since is null (no workspace watermark row)
 *   before_watermark     complete_since (µs) > since (µs)
 *   session_partial      coverage.incomplete > 0 (an inexact state row overlapping the window;
 *                        a null span overlaps every window)
 *   session_unindexed    more distinct (source, session_id) in the window's event facts than
 *                        state rows overlapping it. A narrower signal than ClickHouse's, which
 *                        reads raw transcript_events: the Postgres facts have no row for a
 *                        session never processed with the facts flag on, and 045040 exposes no
 *                        per-session state to the read role. The pre-flag sessions are what the
 *                        PR4 bootstrap backfills, and the watermark (complete_since) is only
 *                        written after it; see docs/selfhost-composition.md.
 */
import { chInstant, chMicros, OutputScopeError, type OutputReadiness, type SessionOutput, type SessionOutputFilter, type SessionOutputScope } from "../clickhouse/output-query.ts";
import type { SqlQuery, TenantRunner } from "../liveness.ts";
import { keyOf, type Liveness } from "../page.ts";
import type { OutputBackend } from "../output-backend.ts";

/** The reader's own page cap (045040: limit 1..10000). */
export const FACTS_PAGE_LIMIT = 10_000;
const LIVENESS_BATCH = 1_000;
const DEFAULT_TIMEOUT_MS = 3_000;

export interface PostgresFactsDeps {
  /** A READ ONLY tenant transaction on the read pool (bulls96_read_api). */
  run: TenantRunner;
  liveness: Liveness;
  /** Test seam for the deadline clock. */
  now?: () => number;
  /** Test seam: the reader page size (default and maximum FACTS_PAGE_LIMIT). */
  pageLimit?: number;
}

/** The request deadline passed while paging the facts: a 503, never a partial answer. */
export class FactsReadDeadline extends Error {
  constructor() {
    super("output facts read exceeded its deadline");
    this.name = "FactsReadDeadline";
  }
}

/** undefined_function, undefined_table, invalid_schema_name, undefined_column: 045040 not applied. */
const PG_SCHEMA_MISSING = new Set(["42883", "42P01", "3F000", "42703"]);
export function isPostgresFactsSchemaMissing(error: unknown): boolean {
  const code = (error as { code?: unknown } | null)?.code;
  return typeof code === "string" && PG_SCHEMA_MISSING.has(code);
}

/** A timestamptz as ClickHouse DateTime64(6) text, UTC, microsecond-exact. */
const ts = (column: string) => `to_char(${column} AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.US')`;

export const EVENT_FACTS_SQL = `SELECT ${ts("e.ts")} AS ts, e.session_id, e.source, e.machine_id, e.kind, e.project_id,
       e.task_ref_id, e.task_ref_empty, e.task_ref_json, e.task_title, e.user_id, e.branch, e.model,
       e.input_tokens, e.output_tokens, e.reasoning_tokens, e.cache_read_tokens, e.cache_create_tokens,
       e.cache_create_5m_tokens, e.cache_create_1h_tokens, e.cost_usd_micros, e.files_touched, e.row_id
  FROM sessions.output_facts_events_for($1, $2::timestamptz, $3::timestamptz, $4::text, $5::integer) AS e
 ORDER BY e.row_id`;
export const EVENT_KEYS_SQL = `SELECT e.session_id, e.source, e.row_id
  FROM sessions.output_facts_events_for($1, $2::timestamptz, $3::timestamptz, $4::text, $5::integer) AS e
 ORDER BY e.row_id`;
export const OUTPUT_FACTS_SQL = `SELECT f.fact_kind, ${ts("f.ts")} AS ts, f.scope_source, f.source, f.session_id, f.fact_key,
       f.origin, f.added, f.removed, f.reverted, f.row_id
  FROM sessions.output_facts_outputs_for($1, $2::timestamptz, $3::timestamptz, $4::text, $5::integer) AS f
 ORDER BY f.row_id`;
export const COVERAGE_SQL = `SELECT ${ts("c.complete_since")} AS complete_since, c.sessions, c.incomplete
  FROM sessions.output_facts_coverage_for($1, $2::timestamptz, $3::timestamptz) AS c`;

export interface EventFact {
  ts: string; session_id: string; source: string; machine_id: string; kind: string; project_id: string | null;
  task_ref_id: string | null; task_ref_empty: number | string; task_ref_json: string; task_title: string | null;
  user_id: string | null; branch: string | null; model: string | null;
  input_tokens: Int; output_tokens: Int; reasoning_tokens: Int; cache_read_tokens: Int; cache_create_tokens: Int;
  cache_create_5m_tokens: Int | null; cache_create_1h_tokens: Int | null; cost_usd_micros: Int | null;
  files_touched: Array<string | null> | null; row_id: string;
}
export interface OutputFact {
  fact_kind: string; ts: string; scope_source: string; source: string; session_id: string; fact_key: string;
  origin: string; added: Int | null; removed: Int | null; reverted: number | string; row_id: string;
}
/** int8 arrives as a string (node-postgres) or a number/bigint (PGlite). */
type Int = string | number | bigint;

/** Epoch microseconds as timestamptz text the readers accept ("…T…ffffffZ"). */
export function microsText(us: bigint): string {
  const ms = us / 1000n - (us % 1000n < 0n ? 1n : 0n);
  const sub = us - ms * 1000n;
  return new Date(Number(ms)).toISOString().replace("Z", `${String(sub).padStart(3, "0")}Z`);
}
export const dateMicros = (d: Date) => BigInt(d.getTime()) * 1000n;
export const UNBOUNDED = ["-infinity", "infinity"] as const;

/** Every page of one reader window, in row_id order, checking the deadline before each page. */
export async function readPages<R extends { row_id: string }>(
  q: SqlQuery, sql: string, args: unknown[], deadline: number, now: () => number, pageLimit = FACTS_PAGE_LIMIT,
): Promise<R[]> {
  const limit = Math.max(1, Math.min(FACTS_PAGE_LIMIT, Math.trunc(pageLimit)));
  const out: R[] = [];
  let after: string | null = null;
  for (;;) {
    if (now() > deadline) throw new FactsReadDeadline();
    const { rows }: { rows: R[] } = await q.query<R>(sql, [...args, after, limit]);
    for (const r of rows) out.push(r);
    if (rows.length < limit) return out;
    after = rows[rows.length - 1]!.row_id;
  }
}

/** The keys of `keys` liveness reports erased, asked in batches. */
export async function erasedKeys(liveness: Liveness, orgId: string, workspaceId: string, keys: Array<{ source: string; sessionId: string }>): Promise<Set<string>> {
  const unique = [...new Map(keys.map((k) => [keyOf(k), k])).values()];
  const erased = new Set<string>();
  for (let i = 0; i < unique.length; i += LIVENESS_BATCH)
    for (const k of await liveness.erased(orgId, workspaceId, unique.slice(i, i + LIVENESS_BATCH))) erased.add(k);
  return erased;
}

/** Drops the rows of erased sessions; `key` names the row's (harness, session id). */
export async function dropErased<R>(liveness: Liveness, orgId: string, workspaceId: string, rows: R[], key: (r: R) => { source: string; sessionId: string }): Promise<R[]> {
  if (!rows.length) return rows;
  const erased = await erasedKeys(liveness, orgId, workspaceId, rows.map(key));
  return erased.size ? rows.filter((r) => !erased.has(keyOf(key(r)))) : rows;
}

export const deadlineOf = (deps: PostgresFactsDeps, timeoutMs: number | undefined) => (deps.now ?? Date.now)() + (timeoutMs ?? DEFAULT_TIMEOUT_MS);

/** clickhouse/output-query.ts outputWindowReadiness over the Postgres facts (see the header). */
export async function postgresWindowReadiness(
  deps: PostgresFactsDeps, orgId: string, workspaceId: string, since: Date, until: Date,
  opts: { timeoutMs?: number; untilExclusive?: boolean } = {},
): Promise<OutputReadiness> {
  const now = deps.now ?? Date.now;
  const deadline = deadlineOf(deps, opts.timeoutMs);
  const sinceUs = dateMicros(since);
  const from = microsText(sinceUs);
  const to = microsText(dateMicros(until) + (opts.untilExclusive ? 0n : 1n));
  return deps.run(orgId, workspaceId, async (q) => {
    const { rows: [cov] } = await q.query<{ complete_since: string | null; sessions: Int; incomplete: Int }>(COVERAGE_SQL, [workspaceId, from, to]);
    if (!cov?.complete_since) return "backfill_incomplete";
    if (chMicros(cov.complete_since) > sinceUs) return "before_watermark";
    if (Number(cov.incomplete) > 0) return "session_partial";
    const keys = await readPages<{ session_id: string; source: string; row_id: string }>(q, EVENT_KEYS_SQL, [workspaceId, from, to], deadline, now, deps.pageLimit);
    const sessions = new Set(keys.map((k) => JSON.stringify([k.source, k.session_id])));
    if (sessions.size > Number(cov.sessions)) return "session_unindexed";
    return "ready";
  });
}

/** Code point order: ClickHouse compares strings as UTF-8 bytes, which orders exactly as code points. */
export function compareText(a: string, b: string): number {
  if (a === b) return 0;
  const x = [...a], y = [...b];
  for (let i = 0; i < Math.min(x.length, y.length); i++) {
    const d = x[i]!.codePointAt(0)! - y[i]!.codePointAt(0)!;
    if (d) return d;
  }
  return x.length - y.length;
}
/** UTF-8 bytes as a one-char-per-byte string: ClickHouse length()/substring() are byte-wise. */
const utf8 = new TextEncoder();
const bytes = (s: string) => String.fromCharCode(...utf8.encode(s));

const big = (v: Int | null | undefined) => (v === null || v === undefined ? null : BigInt(typeof v === "number" ? Math.trunc(v) : v));

/**
 * readSessionOutput's two aggregates over already-fetched output facts (erased ones dropped):
 * the messages CTE and the commit dedup, ported literally from clickhouse/output-query.ts.
 */
export function outputFromFacts(facts: OutputFact[], filter: SessionOutputFilter = {}): SessionOutput {
  const sinceText = filter.since ? chInstant(filter.since) : null;
  const untilText = filter.until ? chInstant(filter.until) : null;
  const inWindow = (t: string) => (sinceText === null || t >= sinceText) && (untilText === null || t <= untilText);
  const scoped = facts.filter((f) => (!filter.source || f.source === filter.source) && (!filter.sessionId || f.session_id === filter.sessionId));

  // messages: DISTINCT (source, session_id, fact_key) of `message` facts in the window, plus every
  // windowed `legacy_turn_message` row whose (source, session_id) has no message in the window.
  const messages = new Set<string>();
  const messageSessions = new Set<string>();
  for (const f of scoped) {
    if (f.fact_kind !== "message" || !inWindow(f.ts)) continue;
    messages.add(JSON.stringify([f.source, f.session_id, f.fact_key]));
    messageSessions.add(JSON.stringify([f.source, f.session_id]));
  }
  let legacy = 0;
  for (const f of scoped)
    if (f.fact_kind === "legacy_turn_message" && inWindow(f.ts) && !messageSessions.has(JSON.stringify([f.source, f.session_id]))) legacy++;

  // c: every non-tool_result commit candidate, plus the window's tool_result ones.
  const c = scoped.filter((f) => f.fact_kind === "commit" && (f.origin !== "tool_result" || inWindow(f.ts)))
    .map((f) => ({ sha: bytes(f.fact_key), at: f.ts, added: big(f.added), removed: big(f.removed) }));
  // full_ids (40 bytes), lens (1..39 bytes), uniq: prefix -> the one full id it abbreviates.
  const fullIds = new Set(c.filter((x) => x.sha.length === 40).map((x) => x.sha));
  const lens = new Set(c.filter((x) => x.sha.length >= 1 && x.sha.length <= 39).map((x) => x.sha.length));
  const prefixes = new Map<string, { n: number; resolved: string }>();
  for (const full of fullIds)
    for (const n of lens) {
      const prefix = full.slice(0, n);
      const seen = prefixes.get(prefix);
      if (seen) { seen.n++; if (full < seen.resolved) seen.resolved = full; }
      else prefixes.set(prefix, { n: 1, resolved: full });
    }
  // g: grouped by the resolved sha, min(at) / max(added) / max(removed) (max skips NULL).
  const g = new Map<string, { firstAt: string; a: bigint | null; r: bigint | null }>();
  for (const x of c) {
    const u = prefixes.get(x.sha);
    const key = u && u.n === 1 ? u.resolved : x.sha;
    const row = g.get(key);
    if (!row) { g.set(key, { firstAt: x.at, a: x.added, r: x.removed }); continue; }
    if (x.at < row.firstAt) row.firstAt = x.at;
    if (x.added !== null && (row.a === null || x.added > row.a)) row.a = x.added;
    if (x.removed !== null && (row.r === null || x.removed > row.r)) row.r = x.removed;
  }
  let commits = 0, missing = 0, added = 0n, removed = 0n;
  for (const row of g.values()) {
    if (!inWindow(row.firstAt)) continue;
    commits++;
    if (row.a !== null) added += row.a;
    if (row.r !== null) removed += row.r;
    if (row.a === null || row.r === null) missing++;
  }
  return { agentMessages: messages.size + legacy, commits, linesAdded: Number(added), linesRemoved: Number(removed), commitsWithoutLineCounts: missing };
}

/** Every output fact of the workspace (unwindowed: commit candidates are), erased sessions dropped. */
export async function liveOutputFacts(deps: PostgresFactsDeps, orgId: string, workspaceId: string, deadline: number): Promise<OutputFact[]> {
  const now = deps.now ?? Date.now;
  const rows = await deps.run(orgId, workspaceId, (q) => readPages<OutputFact>(q, OUTPUT_FACTS_SQL, [workspaceId, ...UNBOUNDED], deadline, now, deps.pageLimit));
  return dropErased(deps.liveness, orgId, workspaceId, rows, (f) => ({ source: f.scope_source, sessionId: f.session_id }));
}

/** clickhouse/output-query.ts readSessionOutput over the Postgres facts. One tenant transaction per workspace. */
export async function readSessionOutputPostgres(
  deps: PostgresFactsDeps, scope: SessionOutputScope, filter: SessionOutputFilter = {}, opts: { timeoutMs?: number } = {},
): Promise<SessionOutput> {
  const workspaceIds = [...new Set(scope.workspaceIds)];
  if (!workspaceIds.length || workspaceIds.some((id) => !id)) throw new OutputScopeError();
  const deadline = deadlineOf(deps, opts.timeoutMs);
  const facts: OutputFact[] = [];
  for (const ws of workspaceIds) for (const f of await liveOutputFacts(deps, scope.orgId, ws, deadline)) facts.push(f);
  return outputFromFacts(facts, filter);
}

/** The Output backend over the Postgres facts (READ_API_OUTPUT_BACKEND=postgres). */
export function postgresOutputBackend(deps: PostgresFactsDeps): OutputBackend {
  return {
    kind: "postgres",
    windowReadiness: (orgId, workspaceId, since, until, opts = {}) => postgresWindowReadiness(deps, orgId, workspaceId, since, until, opts),
    readOutput: (scope, filter, opts = {}) => readSessionOutputPostgres(deps, scope, filter, opts),
    isSchemaMissing: isPostgresFactsSchemaMissing,
  };
}
