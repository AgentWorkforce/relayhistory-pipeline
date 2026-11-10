/**
 * One event page: candidates, newest versions, tenant recheck, byte budget,
 * erasure filter, then the relayhistory event shape.
 *
 * Pagination never truncates silently (relayhistory-cloud #251): a page that
 * the byte budget cuts carries a cursor at its last kept row, and a page whose
 * candidate scan filled `limit` carries a cursor at the last candidate even when
 * fewer rows survived the newest-version recheck. A page can therefore be short
 * (or empty) with a cursor; it is never short without one while rows remain.
 */
import { ClickHouseError, type ClickHouseClient } from "../session-pipeline/vendor/search-index/clickhouse.ts";
import { SearchIndexError, assertPosition, comparePositions, type SearchHit, type SearchPosition } from "@rhc/search-index/contracts.ts";
import type { SearchServiceBinding } from "../session-pipeline/search-adapter.ts";
import { encodeBufferedCursor, encodeCursor, type EventPosition } from "./cursor.ts";
import type { BufferedSearchGate, DropReason } from "./buffered-search-gate.ts";
import {
  READ_SETTINGS,
  buildCandidateQuery,
  buildNewestQuery,
  buildOriginColumnProbe,
  buildPartialSessionsQuery,
  MISSING_COLUMN_CODES,
  type OriginMode,
  buildWorkspacePartialSessionsQuery,
  PARTIAL_SESSIONS_CAP,
  PAGE_BYTE_BUDGET,
  ROW_OVERHEAD_BYTES,
  candidatePosition,
  type CandidateRow,
  type EventQuery,
  type EventRow,
} from "./queries.ts";

/** relayhistory's maxContent marker (lib/recall.ts CONTENT_TRUNCATION_NOTE). */
export const CONTENT_TRUNCATION_NOTE = "…[truncated by maxContent]";

export class TenantMismatchError extends Error {
  constructor() {
    super("a row outside the authorized tenant reached the read path");
    this.name = "TenantMismatchError";
  }
}

export interface SessionKey {
  source: string;
  sessionId: string;
}

/** Supabase-side checks. Only ids cross this boundary. */
export interface Liveness {
  /** The subset of `keys` whose every pointer row is tombstoned (erased). */
  erased(orgId: string, workspaceId: string, keys: SessionKey[]): Promise<Set<string>>;
  /**
   * The session's first activity across its live pointer rows (all harnesses when
   * `source` is omitted), whether any pointer row exists, and whether all are erased.
   */
  sessionStart(orgId: string, workspaceId: string, sessionId: string, source?: string): Promise<{ pointers: number; live: number; createdAtMs: number | null }>;
  /** The subset of `sessionIds` with at least one live pointer row in this workspace (any harness). */
  livePointerSessions(orgId: string, workspaceId: string, sessionIds: string[]): Promise<Set<string>>;
}

export const keyOf = (k: SessionKey) => `${k.source}\u0000${k.sessionId}`;

export interface ShapedEvent {
  eventId: string;
  userId: string | null;
  sessionId: string;
  source: string;
  kind: string;
  type: string;
  ts: string;
  actorName: string | null;
  actorRole: string | null;
  subagentId: string | null;
  machineId: string;
  projectId: string | null;
  trajectoryId: null;
  taskRef: Record<string, unknown>;
  taskTitle: string | null;
  content: string | null;
  contentTruncated: boolean;
  tags: string[];
  toolName: string | null;
  toolStatus: string | null;
  filesTouched: string[];
}

export interface EventPage {
  events: ShapedEvent[];
  nextCursor: string | null;
  /** Rows dropped because their session is erased (telemetry only). */
  erasedDropped: number;
  /** Buffered-origin rows checked and dropped by the buffered search gate (telemetry only; absent when off). */
  buffered?: { checked: number; dropped: Partial<Record<DropReason, number>> };
}

const nullable = (v: string | null | undefined) => (v === undefined || v === null || v === "" ? null : v);

function parseTaskRef(raw: string): Record<string, unknown> {
  if (!raw) return {};
  try {
    const v = JSON.parse(raw) as unknown;
    return v && typeof v === "object" && !Array.isArray(v) ? (v as Record<string, unknown>) : {};
  } catch {
    return {};
  }
}

export function shapeRow(row: EventRow, maxContent?: number): ShapedEvent {
  let content: string | null = row.content;
  let contentTruncated = false;
  if (maxContent !== undefined && maxContent > 0 && typeof content === "string" && content.length > maxContent) {
    content = `${content.slice(0, maxContent)}${CONTENT_TRUNCATION_NOTE}`;
    contentTruncated = true;
  }
  return {
    eventId: row.event_id,
    userId: nullable(row.user_id),
    sessionId: row.session_id,
    source: row.source,
    kind: row.kind,
    type: row.type || row.kind,
    ts: new Date(Number(row.ts_ms)).toISOString(),
    actorName: nullable(row.actor_name),
    actorRole: nullable(row.actor_role),
    subagentId: nullable(row.subagent_id),
    machineId: row.machine_id,
    projectId: nullable(row.project_id),
    trajectoryId: null,
    taskRef: parseTaskRef(row.task_ref),
    taskTitle: nullable(row.task_title),
    content,
    contentTruncated,
    tags: Array.isArray(row.tags) ? row.tags : [],
    toolName: nullable(row.tool_name),
    toolStatus: nullable(row.tool_status),
    filesTouched: Array.isArray(row.files_touched) ? row.files_touched : [],
  };
}

const rowPosition = (r: EventRow): EventPosition => ({
  tsMs: Number(r.ts_ms),
  sessionId: r.session_id,
  source: r.source,
  machineId: r.machine_id,
  kind: r.kind,
  eventId: r.event_id,
});

export interface PageDeps {
  ch?: ClickHouseClient;
  adapter?: SearchServiceBinding;
  completeSinceMs?: number;
  liveness: Liveness;
  timeoutMs?: number;
  /** READ_API_BUFFERED_SEARCH_GATE on: every buffered-origin row passes the R2 privacy gate (ClickHouse binding only). */
  bufferedGate?: BufferedSearchGate;
}

export async function readEventPage(deps: PageDeps, input: EventQuery): Promise<EventPage> {
  if (deps.adapter) {
    // The portable hit carries no origin marker, so buffered rows cannot be told apart.
    if (deps.bufferedGate) throw new Error("buffered search gate requires the ClickHouse binding");
    return readAdapterPage(deps, input);
  }
  if (!deps.ch) throw new Error("read search binding missing");
  const ch = deps.ch;
  const options: ReadOptions = { settings: { ...READ_SETTINGS }, timeoutMs: deps.timeoutMs ?? 3_000 };
  if (deps.bufferedGate) return finishPage(deps, input, await selectPage(ch, input, "gate", options));
  // No gate in this runtime (the Lambda, or the gate off): canonical rows only. A table
  // without the origin column fails that query; the read is then answered unfiltered only
  // if the column is STILL absent after the unfiltered read (columns are only ever added,
  // and the feeder writes no row before they exist), so a buffered row is never served.
  let selected: Selected;
  try {
    selected = await selectPage(ch, input, "canonical", options);
  } catch (error) {
    if (!(error instanceof ClickHouseError) || !error.code || !MISSING_COLUMN_CODES.has(error.code)) throw error;
    selected = await selectPage(ch, input, "legacy", options);
    const probe = buildOriginColumnProbe(ch.database);
    const [found] = await ch.select<{ n: number | string }>(probe.query, { ...options, params: probe.params });
    if (found === undefined || Number(found.n) !== 0) throw new ClickHouseError(503, null, "origin_column_changed");
  }
  return finishPage(deps, input, selected);
}

interface Selected {
  candidates: EventPosition[];
  rows: EventRow[];
}

type ReadOptions = { settings: Record<string, string | number>; timeoutMs: number };

async function selectPage(ch: ClickHouseClient, input: EventQuery, origin: OriginMode, options: ReadOptions): Promise<Selected> {
  const c = buildCandidateQuery(ch.database, input, origin);
  const candidateRows = await ch.select<CandidateRow>(c.query, { ...options, params: c.params });
  const candidates = candidateRows.map(candidatePosition);
  if (candidates.length === 0) return { candidates, rows: [] };
  const n = buildNewestQuery(ch.database, input, candidates, origin);
  const rows = await ch.select<EventRow>(n.query, { ...options, params: n.params });
  // Defense in depth, as the tenant recheck: canonical mode never passes on a row the SQL
  // should have excluded, nor one without the marker.
  return { candidates, rows: origin === "canonical" ? rows.filter((r) => r.origin === "canonical") : rows };
}

async function finishPage(deps: PageDeps, input: EventQuery, { candidates, rows }: Selected): Promise<EventPage> {
  if (candidates.length === 0) return { events: [], nextCursor: null, erasedDropped: 0 };

  // Defense in depth: every row must belong to the verified tenant and the
  // requested session/source, whatever the SQL said.
  for (const r of rows) {
    if (r.org_id !== input.orgId || r.workspace_id !== input.workspaceId) throw new TenantMismatchError();
    if (input.sessionId !== undefined && r.session_id !== input.sessionId) throw new TenantMismatchError();
    if (input.source !== undefined && r.source !== input.source) throw new TenantMismatchError();
  }

  let kept = rows;
  let nextCursor: string | null = null;
  const cut = rows.findIndex((r) => Number(r.in_budget) !== 1);
  if (cut > 0) {
    kept = rows.slice(0, cut);
    nextCursor = encodeCursor(rowPosition(kept[kept.length - 1]!));
  } else if (candidates.length >= input.limit) {
    nextCursor = encodeCursor(candidates[candidates.length - 1]!);
  }

  const distinct = new Map<string, SessionKey>();
  for (const r of kept) distinct.set(keyOf({ source: r.source, sessionId: r.session_id }), { source: r.source, sessionId: r.session_id });
  const erased = distinct.size ? await deps.liveness.erased(input.orgId, input.workspaceId, [...distinct.values()]) : new Set<string>();
  const live = kept.filter((r) => !erased.has(keyOf({ source: r.source, sessionId: r.session_id })));
  // The buffered gate only removes rows; nextCursor above is the unfiltered page's.
  if (!deps.bufferedGate) {
    return { events: live.map((r) => shapeRow(r, input.maxContent)), nextCursor, erasedDropped: kept.length - live.length };
  }
  const gated = await deps.bufferedGate.filter(input.orgId, input.workspaceId, live);
  return {
    events: gated.kept.map((r) => shapeRow(r, input.maxContent)),
    nextCursor,
    erasedDropped: kept.length - live.length,
    buffered: { checked: gated.buffered, dropped: gated.dropped },
  };
}

/**
 * Partial sessions (`needs_full = 1`, queries.ts buildPartialSessionsQuery):
 * the one session when `sessionId` is given, else any of the org's, capped.
 */
export async function partialSessions(deps: PageDeps, orgId: string, sessionId?: string, workspaceId?: string): Promise<string[]> {
  if (deps.adapter) {
    if (!workspaceId) throw new Error("verified workspace required");
    const result = await adapterPartials(deps, orgId, workspaceId, sessionId);
    // A truncated check can never admit a session as complete.
    if (result.overflow) throw new SearchIndexError("unavailable", true);
    return [...result.sessions];
  }
  if (!deps.ch) throw new Error("read search binding missing");
  const q = buildPartialSessionsQuery(deps.ch.database, orgId, sessionId);
  const rows = await deps.ch.select<{ session_id: string }>(q.query, {
    settings: { ...READ_SETTINGS },
    timeoutMs: deps.timeoutMs ?? 3_000,
    params: q.params,
  });
  return rows.map((r) => r.session_id);
}

export interface WorkspacePartials {
  /** Partial sessions that may hold this workspace's rows. */
  sessions: string[];
  /** The org has more partial sessions than one check lists: not decided. */
  overflow: boolean;
}

/**
 * The partial sessions a workspace search must not answer without: those of
 * the token's org that belong to the token's workspace. The state table has
 * no workspace, so a partial session belongs to this workspace when either
 *
 *   - it has a row (any version) in this workspace's `transcript_events`, or
 *   - it has a live pointer row in this workspace's `session_index`.
 *
 * The second covers a session whose passes were all partial and so wrote no
 * row (search-index.ts indexSearchScope writes only the state row then). The
 * pipeline indexes a scope only from its live pointers, and every Neon session
 * reaches S3 through the exporter's pointers, so a session holding content of
 * this workspace has a row here or a live pointer here. A session with neither
 * belongs to another workspace (whose search it still blocks), or is erased
 * here, where the erasure filter drops its rows anyway. Candidate rows alone
 * would not do: the events a partial session is missing are not candidates.
 *
 * The check stops at the first finding: an in-workspace row decides without
 * asking Supabase.
 */
export async function partialSessionsInWorkspace(deps: PageDeps, orgId: string, workspaceId: string): Promise<WorkspacePartials> {
  if (deps.adapter) {
    const result = await adapterPartials(deps, orgId, workspaceId);
    return { sessions: [...result.sessions], overflow: result.overflow };
  }
  if (!deps.ch) throw new Error("read search binding missing");
  const q = buildWorkspacePartialSessionsQuery(deps.ch.database, orgId, workspaceId);
  const rows = await deps.ch.select<{ session_id: string; in_workspace: number | string }>(q.query, {
    settings: { ...READ_SETTINGS },
    timeoutMs: deps.timeoutMs ?? 3_000,
    params: q.params,
  });
  if (rows.length > PARTIAL_SESSIONS_CAP) return { sessions: [], overflow: true };
  const inWorkspace = rows.filter((r) => Number(r.in_workspace) === 1).map((r) => r.session_id);
  if (inWorkspace.length > 0) return { sessions: inWorkspace, overflow: false };
  if (rows.length === 0) return { sessions: [], overflow: false };
  const pointed = await deps.liveness.livePointerSessions(orgId, workspaceId, rows.map((r) => r.session_id));
  return { sessions: rows.map((r) => r.session_id).filter((id) => pointed.has(id)), overflow: false };
}

async function deadline<T>(deps: PageDeps, work: (signal: AbortSignal) => Promise<T>): Promise<T> {
  const controller = new AbortController();
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([work(controller.signal), new Promise<never>((_, reject) => {
      timer = setTimeout(() => { controller.abort(); reject(new SearchIndexError("unavailable", true, "timeout")); }, deps.timeoutMs ?? 3000);
    })]);
  } finally { clearTimeout(timer); }
}

async function adapterPartials(deps: PageDeps, orgId: string, workspaceId: string, sessionId?: string) {
  await assertCoverage(deps);
  const result = await deadline(deps, () => deps.adapter!.state.partials({ orgId, workspaceId }, { sessionId, limit: PARTIAL_SESSIONS_CAP }));
  if (!result || !Array.isArray(result.sessions) || typeof result.overflow !== "boolean" || result.sessions.some(s => typeof s !== "string" || !s) || result.sessions.length > PARTIAL_SESSIONS_CAP && !result.overflow) throw new SearchIndexError("unavailable", true);
  return result;
}

function eventRow(hit: SearchHit): EventRow {
  return {
    org_id: hit.orgId, workspace_id: hit.workspaceId, ts_ms: hit.tsMs, session_id: hit.sessionId,
    source: hit.source, machine_id: hit.machineId, kind: hit.kind, event_id: hit.eventId,
    type: hit.type, user_id: hit.userId, actor_name: hit.actorName, actor_role: hit.actorRole,
    subagent_id: hit.subagentId, project_id: hit.projectId, task_title: hit.taskTitle,
    task_ref: hit.taskRef, tags: [...hit.tags], files_touched: [...hit.filesTouched],
    tool_name: hit.toolName, tool_status: hit.toolStatus, content: hit.content, in_budget: 1,
  };
}

/** Portable query results use the same public shaping, byte-budget, erasure
 * and cursor rules. No SQL emulation and no fallback to another backend. */
async function readAdapterPage(deps: PageDeps, input: EventQuery): Promise<EventPage> {
  await assertCoverage(deps);
  const page = await deadline(deps, signal => deps.adapter!.index.query({
    tenant: { orgId: input.orgId, workspaceId: input.workspaceId }, sessionId: input.sessionId,
    source: input.source, kind: input.kind, projectId: input.projectId, q: input.q,
    sinceMs: input.sinceMs, untilMs: input.untilMs, order: input.order, cursor: input.cursor,
    limit: input.limit, ...(input.maxContent === undefined ? {} : { maxContentChars: input.maxContent + 1 }),
  }, { signal }));
  if (!page || !Array.isArray(page.hits) || page.hits.length > input.limit) throw new SearchIndexError("unavailable", true);
  const advances = (a: SearchPosition, b: SearchPosition) => input.order === "asc" ? comparePositions(a, b) > 0 : comparePositions(a, b) < 0;
  let previous: SearchPosition | undefined = input.cursor;
  for (const hit of page.hits) {
    assertPosition(hit);
    if (hit.orgId !== input.orgId || hit.workspaceId !== input.workspaceId || input.sessionId !== undefined && hit.sessionId !== input.sessionId || input.source !== undefined && hit.source !== input.source) throw new TenantMismatchError();
    if (input.kind !== undefined && hit.kind !== input.kind || input.projectId !== undefined && hit.projectId !== input.projectId || input.sinceMs !== undefined && hit.tsMs < input.sinceMs || input.untilMs !== undefined && hit.tsMs > input.untilMs || previous && !advances(hit, previous)) throw new SearchIndexError("unavailable", true);
    previous = hit;
  }
  if (page.next) {
    assertPosition(page.next);
    if (input.cursor && !advances(page.next, input.cursor) || previous && comparePositions(page.next, previous) !== 0 && !advances(page.next, previous)) throw new SearchIndexError("unavailable", true);
  }
  const rows = page.hits.map(eventRow);
  const cut = budgetCut(rows);
  const kept = rows.slice(0, cut);
  const nextCursor = cut < rows.length ? encodeCursor(rowPosition(kept[kept.length - 1]!)) : page.next ? encodeCursor(page.next) : null;
  const distinct = new Map<string, SessionKey>();
  for (const row of kept) distinct.set(keyOf({ source: row.source, sessionId: row.session_id }), { source: row.source, sessionId: row.session_id });
  const erased = distinct.size ? await deps.liveness.erased(input.orgId, input.workspaceId, [...distinct.values()]) : new Set<string>();
  const live = kept.filter(row => !erased.has(keyOf({ source: row.source, sessionId: row.session_id })));
  return { events: live.map(row => shapeRow(row, input.maxContent)), nextCursor, erasedDropped: kept.length - live.length };
}

async function assertCoverage(deps: PageDeps): Promise<void> {
  const fromMs = await deadline(deps, () => deps.adapter!.state.fromMs(new Date()));
  if (!Number.isSafeInteger(fromMs) || fromMs < 0 || fromMs > (deps.completeSinceMs ?? 0)) throw new SearchIndexError("unavailable", true, "coverage_changed");
}

/**
 * The page byte budget's cut (readAdapterPage and readBufferedEventPage): the index of the first
 * row not kept. The first row is always kept; a later row is cut once the kept rows' content
 * bytes plus ROW_OVERHEAD_BYTES each exceed PAGE_BYTE_BUDGET.
 */
export function budgetCut(rows: readonly EventRow[]): number {
  let bytes = 0;
  for (let i = 0; i < rows.length; i++) {
    if (i > 0 && bytes > PAGE_BYTE_BUDGET) return i;
    bytes += Buffer.byteLength(rows[i]!.content, "utf8") + ROW_OVERHEAD_BYTES;
  }
  return rows.length;
}

/**
 * One page of an owner-buffered session's projected rows (owner-buffered.ts
 * readOwnerBufferedEvents; every row is already gated, scrubbed and tenant-checked). The same
 * full sort key and order as the canonical page (comparePositions: ts in ms, then session,
 * source, machine, kind, event id by code point, the ClickHouse/portable index order), rows
 * strictly after the cursor, `limit` rows, then the same byte-budget cut as readAdapterPage.
 * A page that the limit or the budget cuts carries a buffered cursor (encodeBufferedCursor) at
 * its last kept row; the last page carries none. Erasure is the buffered gate's, not Supabase
 * liveness: nothing here asks liveness.
 */
export function readBufferedEventPage(rows: readonly EventRow[], input: Pick<EventQuery, "order" | "limit" | "maxContent" | "cursor">): EventPage {
  const sign = input.order === "desc" ? -1 : 1;
  const cursor = input.cursor;
  const sorted = rows
    .filter((row) => !cursor || sign * comparePositions(rowPosition(row), cursor) > 0)
    .sort((a, b) => sign * comparePositions(rowPosition(a), rowPosition(b)));
  const limited = sorted.slice(0, input.limit);
  const cut = budgetCut(limited);
  const kept = limited.slice(0, cut);
  const more = cut < limited.length || sorted.length > limited.length;
  return {
    events: kept.map((row) => shapeRow(row, input.maxContent)),
    nextCursor: more && kept.length > 0 ? encodeBufferedCursor(rowPosition(kept[kept.length - 1]!)) : null,
    erasedDropped: 0,
  };
}
