/**
 * The harvest itself (docs/session-intel-harvest.md): page the read API's own session directory over the
 * window to exhaustion (the frame), then read each session's events for the facts the snapshot needs, and
 * record a coverage receipt. Progress goes to an append-only state log in --out, so a crash or the request
 * cap resumes where it stopped. Only ids, timestamps, repo paths and counts are kept; no title, summary,
 * message or event content is ever read into the state, the snapshot or the mapping.
 */
import { createReadStream } from "node:fs";
import { open, type FileHandle } from "node:fs/promises";
import { createInterface } from "node:readline";
import { HarvestError, RequestCapReached, type ClientStats, type ReadApiClient } from "./client.ts";
import { IMPORT_BOUNDS, projectKey, repoRelativePath } from "../import/normalize.ts";
import { isoTime } from "../replay/util.ts";

export const STATE_FILE = "harvest-state.jsonl";
export const STATE_FORMAT = "session-intel-harvest-state/1";
export const RECEIPT_FILE = "coverage.json";
export const RECEIPT_FORMAT = "session-intel-harvest-receipt/1";
/** DIRECTORY_LIST_MAX_LIMIT (src/session-read-api/directory.ts): a larger page is 501 `limit`. */
export const FRAME_PAGE_LIMIT = 200;
/** EVENTS_MAX_LIMIT (src/session-read-api/app.ts). */
export const EVENTS_PAGE_LIMIT = 1_000;
/** Event content is truncated to one character by the server and never read here. */
export const EVENT_MAX_CONTENT = 1;

const HARNESS = /^[a-z0-9][a-z0-9._-]{0,63}$/u;
const KIND = /^[A-Za-z0-9_.:-]{1,64}$/u;
const MAX_ID = IMPORT_BOUNDS.id;

export interface HarvestParams { readApi: string; workspace: string; since: string; until: string; deleteBy: string }

/** One directory row, reduced to what the snapshot needs. Raw ids: this lives only in the 0600 state and the mapping. */
export interface FrameRow {
  source: string;
  sessionId: string;
  firstTs: string;
  lastTs: string;
  projectId: string | null;
  /** The repository name only (the last segment of catalog.repoUrl, the import's projectKey): a URL may carry credentials. */
  repo: string | null;
  gitBranch: string | null;
  parentSessionIds: string[];
  catalog: boolean;
  eventCount: number | null;
}
export interface RawWrite { path: string; ts: string; eventId: string; kind: string }
export interface FactsRecord {
  source: string;
  sessionId: string;
  writes: RawWrite[];
  /** The latest projectId an event carried (the directory row's is preferred). */
  eventProjectId: string | null;
  events: number;
  eventsWithFiles: number;
  eventsOutsideWindow: number;
  droppedPaths: number;
  truncatedWrites: number;
  pages: number;
  /** Why no events could be read: a 501 reason or `not_found`. */
  failure: string | null;
  /**
   * True when the session's writes were cut at the per-session bound (IMPORT_BOUNDS.facts): the snapshot holds
   * fewer writes than the read API has. `censorReason` says how; `droppedWrites` is the count dropped, or null
   * when it is unknown (the cursor was not exhausted, so later pages were never read).
   */
  writesCensored: boolean;
  censorReason: string | null;
  droppedWrites: number | null;
}
export type OutputCounts = { counts: Record<string, number> } | { unavailable: string };

export interface HarvestState {
  params: HarvestParams;
  startedAt: string;
  runs: number;
  requests: number;
  /** The client's cumulative counts as last recorded, restored on resume; null on a fresh harvest. */
  stats: ClientStats | null;
  /** Set when a run ended without recording its final counts (a hard crash): the counts are lower bounds. */
  statsPartial: string | null;
  frame: { pages: number; cursor: string | null; done: boolean; rows: FrameRow[]; duplicates: number; outsideWindow: number };
  facts: Map<string, FactsRecord>;
  output: OutputCounts | null;
}

const key = (source: string, sessionId: string) => `${source}\u0000${sessionId}`;
export const rowKey = (r: { source: string; sessionId: string }) => key(r.source, r.sessionId);

/** The append-only state log: one JSON line per completed unit (start, a run, a frame page, a session's facts). */
export class StateLog {
  private chain: Promise<void> = Promise.resolve();
  private constructor(private readonly handle: FileHandle) {}
  static async create(path: string): Promise<StateLog> {
    // O_EXCL: never replace a state log; 0600: it holds raw session ids and repo paths.
    return new StateLog(await open(path, "wx", 0o600));
  }
  static async append(path: string): Promise<StateLog> {
    return new StateLog(await open(path, "a", 0o600));
  }
  write(line: Record<string, unknown>): Promise<void> {
    const text = `${JSON.stringify(line)}\n`;
    this.chain = this.chain.then(async () => { await this.handle.write(text); });
    return this.chain;
  }
  async close() { await this.chain.catch(() => {}); await this.handle.close(); }
}

/**
 * Reads a state log. A torn last line (a crash mid-write) is ignored, and `goodBytes` is where it starts, so
 * the caller truncates it before appending; any other bad line refuses the resume.
 */
export async function readState(path: string): Promise<HarvestState & { goodBytes: number; torn: boolean }> {
  const lines: Record<string, unknown>[] = [];
  const rl = createInterface({ input: createReadStream(path, { encoding: "utf8" }), crlfDelay: Infinity });
  let torn = false;
  let goodBytes = 0;
  for await (const text of rl) {
    if (torn) throw new HarvestError(`${STATE_FILE} has a malformed line; delete --out and start again`);
    if (text.trim()) {
      try { lines.push(JSON.parse(text) as Record<string, unknown>); } catch { torn = true; continue; }
    }
    // The log is written by this module: one JSON line and "\n" each.
    goodBytes += Buffer.byteLength(text, "utf8") + 1;
  }
  const start = lines[0];
  if (!start || start.t !== "start" || start.format !== STATE_FORMAT) throw new HarvestError(`${STATE_FILE} is not a harvest state log`);
  const state = newState(start.params as HarvestParams, String(start.startedAt));
  const seen = new Set<string>();
  let open = false;
  let abrupt = 0;
  for (const l of lines) {
    if (typeof l.requests === "number") state.requests = Math.max(state.requests, l.requests);
    // Cumulative and written in order, so the last line's counts are the latest.
    if (isRecord(l.stats)) state.stats = l.stats as unknown as ClientStats;
    if (l.t === "run") {
      if (open) abrupt++;
      open = true;
      state.runs++;
    } else if (l.t === "runEnd") open = false;
    else if (l.t === "frame") {
      state.frame.pages++;
      state.frame.cursor = (l.cursor as string | null) ?? null;
      state.frame.done = l.cursor === null;
      state.frame.duplicates += Number(l.duplicates ?? 0);
      state.frame.outsideWindow += Number(l.outsideWindow ?? 0);
      for (const r of l.rows as FrameRow[]) if (!seen.has(rowKey(r))) { seen.add(rowKey(r)); state.frame.rows.push(r); }
    } else if (l.t === "facts") {
      const f = l.record as FactsRecord;
      state.facts.set(rowKey(f), { ...f, writesCensored: f.writesCensored === true, censorReason: f.censorReason ?? null, droppedWrites: f.droppedWrites ?? null });
    } else if (l.t === "output") state.output = l.output as OutputCounts;
  }
  if (open) abrupt++;
  if (abrupt) state.statsPartial = `${abrupt} run(s) ended without recording final request counts (a hard crash); requests after their last state line are not counted, so every request count is a lower bound`;
  // Stats from before they were persisted cannot be rebuilt: the total survives, nothing else does.
  if (!state.stats && state.requests > 0) state.statsPartial ??= "the state log holds no request breakdown; only the total is known";
  return { ...state, goodBytes, torn };
}

/** What every progress line records: the request total and the client's cumulative counts. */
export const progress = (client: ReadApiClient) => ({ requests: client.stats.requests, stats: client.stats });

export const newState = (params: HarvestParams, startedAt: string): HarvestState => ({
  params, startedAt, runs: 0, requests: 0, stats: null, statsPartial: null,
  frame: { pages: 0, cursor: null, done: false, rows: [], duplicates: 0, outsideWindow: 0 }, facts: new Map(), output: null,
});

const isRecord = (v: unknown): v is Record<string, unknown> => typeof v === "object" && v !== null && !Array.isArray(v);
const str = (v: unknown, max: number = MAX_ID): string | null =>
  // eslint-disable-next-line no-control-regex
  typeof v === "string" && v.length > 0 && v.length <= max && !/[\u0000-\u001f\u007f]/u.test(v) ? v : null;
const ts = (v: unknown): string | null => { try { return isoTime(v, "ts"); } catch { return null; } };

/**
 * One directory row as the frame keeps it. Title, task title, short summary, user, machine, model and agent
 * fields are never copied. A row naming another workspace in its catalog refuses the harvest.
 */
export function frameRow(raw: unknown, workspace: string): FrameRow {
  if (!isRecord(raw)) throw new HarvestError("/v1/sessions: a row is not an object");
  const source = str(raw.source, 64), sessionId = str(raw.sessionId), firstTs = ts(raw.firstTs), lastTs = ts(raw.lastTs);
  if (!source || !HARNESS.test(source) || !sessionId || !firstTs || !lastTs || lastTs < firstTs)
    throw new HarvestError("/v1/sessions: a row is outside the contract (source, sessionId, firstTs <= lastTs)");
  const catalog = isRecord(raw.catalog) ? raw.catalog : null;
  if (catalog && catalog.workspaceId !== undefined && catalog.workspaceId !== workspace)
    throw new HarvestError("/v1/sessions: a row's catalog names another workspace than --workspace; refused");
  const parents = catalog && Array.isArray(catalog.parentSessionIds)
    ? [...new Set(catalog.parentSessionIds.map((p) => str(p)).filter((p): p is string => p !== null && p !== sessionId))].slice(0, IMPORT_BOUNDS.parents)
    : [];
  return {
    source, sessionId, firstTs, lastTs,
    projectId: str(raw.projectId),
    repo: catalog && str(catalog.repoUrl, 2_048) ? projectKey(catalog.repoUrl as string, null) : null,
    gitBranch: catalog ? str(catalog.gitBranch) : null,
    parentSessionIds: parents,
    catalog: catalog !== null,
    eventCount: typeof raw.eventCount === "number" && Number.isSafeInteger(raw.eventCount) && raw.eventCount >= 0 ? raw.eventCount : null,
  };
}

export interface RunOptions {
  client: ReadApiClient;
  state: HarvestState;
  log: StateLog;
  concurrency: number;
}

const overlaps = (r: FrameRow, p: HarvestParams) => r.lastTs >= p.since && r.firstTs < p.until;

/** Pages GET /v1/sessions to exhaustion: lastTsAfter=since and firstTsBefore=until, newest first, by cursor. */
export async function runFrame({ client, state, log }: RunOptions): Promise<void> {
  const p = state.params;
  const seen = new Set(state.frame.rows.map(rowKey));
  while (!state.frame.done) {
    const params: [string, string][] = [["limit", String(FRAME_PAGE_LIMIT)], ["lastTsAfter", p.since], ["firstTsBefore", p.until]];
    if (state.frame.cursor) params.push(["cursor", state.frame.cursor]);
    const a = await client.get("/v1/sessions", params);
    if (a.kind !== "ok") throw new HarvestError(`/v1/sessions is not served by this read API (${a.kind === "unsupported" ? a.reason : "not_found"}); there is no frame to harvest`);
    const body = a.body;
    if (!isRecord(body) || !Array.isArray(body.sessions) || body.sessions.length > FRAME_PAGE_LIMIT ||
        !(body.nextCursor === null || (typeof body.nextCursor === "string" && body.nextCursor.length > 0 && body.nextCursor.length <= 8_192)))
      throw new HarvestError("/v1/sessions: a page is outside the contract");
    if (body.nextCursor !== null && body.nextCursor === state.frame.cursor) throw new HarvestError("/v1/sessions: the cursor did not advance; refused to loop");
    const rows: FrameRow[] = [];
    let duplicates = 0, outsideWindow = 0;
    for (const raw of body.sessions) {
      const r = frameRow(raw, p.workspace);
      if (!overlaps(r, p)) { outsideWindow++; continue; }
      if (seen.has(rowKey(r))) { duplicates++; continue; }
      if (seen.size >= IMPORT_BOUNDS.sessions) throw new HarvestError(`the window holds more than ${IMPORT_BOUNDS.sessions} sessions; harvest a shorter window`);
      seen.add(rowKey(r));
      rows.push(r);
    }
    state.frame.rows.push(...rows);
    state.frame.pages++;
    state.frame.cursor = body.nextCursor as string | null;
    state.frame.done = body.nextCursor === null;
    state.frame.duplicates += duplicates;
    state.frame.outsideWindow += outsideWindow;
    await log.write({ t: "frame", cursor: state.frame.cursor, rows, duplicates, outsideWindow, ...progress(client) });
  }
}

/** GET /v1/sessions/output for the window: workspace counts only (no session, no sha), for the receipt. */
export async function runOutput({ client, state, log }: RunOptions): Promise<void> {
  if (state.output) return;
  const a = await client.get("/v1/sessions/output", [["since", state.params.since], ["until", state.params.until]]);
  let output: OutputCounts;
  if (a.kind === "ok" && isRecord(a.body) && isRecord(a.body.output)) {
    const counts: Record<string, number> = {};
    for (const k of ["agentMessages", "commits", "linesAdded", "linesRemoved", "commitsWithoutLineCounts"]) {
      const v = a.body.output[k];
      if (typeof v === "number" && Number.isSafeInteger(v) && v >= 0) counts[k] = v;
    }
    output = { counts };
  } else output = { unavailable: a.kind === "unsupported" ? a.reason : a.kind === "not_found" ? "not_found" : "outside_contract" };
  state.output = output;
  await log.write({ t: "output", output, ...progress(client) });
}

/**
 * One session's write facts from GET /v1/sessions/:sessionId/events (ascending, content truncated to one
 * character and never read). As the production facts backend (src/session-read-api/coordination-backend.ts),
 * every event with a non-empty filesTouched is a write of each path, with the event's kind. Pages are read
 * until the cursor is exhausted; events outside the window are counted and skipped, never taken as a reason
 * to stop (the order is not relied on to bound the window). The one early stop is the per-session write bound
 * with the cursor not exhausted: the record is then censored with an unknown drop count, never silently short.
 */
export async function sessionFacts(client: ReadApiClient, row: FrameRow, p: HarvestParams, heldWrites: { n: number }): Promise<FactsRecord> {
  const rec: FactsRecord = {
    source: row.source, sessionId: row.sessionId, writes: [], eventProjectId: null, events: 0, eventsWithFiles: 0,
    eventsOutsideWindow: 0, droppedPaths: 0, truncatedWrites: 0, pages: 0, failure: null,
    writesCensored: false, censorReason: null, droppedWrites: null,
  };
  let cursor: string | null = null;
  for (;;) {
    const params: [string, string][] = [["source", row.source], ["order", "asc"], ["limit", String(EVENTS_PAGE_LIMIT)], ["maxContent", String(EVENT_MAX_CONTENT)]];
    if (cursor) params.push(["cursor", cursor]);
    const a = await client.get("/v1/sessions/:sessionId/events", params, row.sessionId);
    if (a.kind !== "ok") { rec.failure = a.kind === "unsupported" ? a.reason : "not_found"; rec.writes = []; return rec; }
    const body = a.body;
    rec.pages++;
    if (!isRecord(body) || !Array.isArray(body.events) || body.events.length > EVENTS_PAGE_LIMIT ||
        !(body.nextCursor === null || (typeof body.nextCursor === "string" && body.nextCursor.length > 0 && body.nextCursor.length <= 8_192)) ||
        (body.sessionId !== undefined && body.sessionId !== row.sessionId))
      throw new HarvestError("/v1/sessions/:sessionId/events: a page is outside the contract");
    if (body.nextCursor !== null && body.nextCursor === cursor) throw new HarvestError("/v1/sessions/:sessionId/events: the cursor did not advance; refused to loop");
    for (const e of body.events) {
      if (!isRecord(e)) throw new HarvestError("/v1/sessions/:sessionId/events: an event is not an object");
      // An event of another session or harness is a server fault, never silently mixed in.
      if (e.sessionId !== row.sessionId || (e.source !== undefined && e.source !== row.source))
        throw new HarvestError("/v1/sessions/:sessionId/events: an event names another session; refused");
      rec.events++;
      const t = ts(e.ts);
      const eventId = str(e.eventId);
      if (!t || !eventId) throw new HarvestError("/v1/sessions/:sessionId/events: an event is outside the contract (eventId, ts)");
      if (t >= p.until || t < p.since) { rec.eventsOutsideWindow++; continue; }
      const project = str(e.projectId);
      if (project) rec.eventProjectId = project;
      const files = Array.isArray(e.filesTouched) ? e.filesTouched : [];
      const paths = files.filter((f): f is string => typeof f === "string" && f !== "");
      if (!paths.length) continue;
      rec.eventsWithFiles++;
      const kind = typeof e.kind === "string" && KIND.test(e.kind) ? e.kind : "event";
      for (const raw of paths.slice(0, IMPORT_BOUNDS.eventFiles)) {
        // Made repo-relative here (the import's rule; no cwd is exposed), so no home directory or user
        // name reaches even the state log. A path that cannot be made one safely is dropped and counted.
        const path = repoRelativePath(raw, null);
        if (!path) { rec.droppedPaths++; continue; }
        if (rec.writes.length >= IMPORT_BOUNDS.facts) { rec.truncatedWrites++; continue; }
        if (++heldWrites.n > IMPORT_BOUNDS.totalFacts) throw new HarvestError(`more than ${IMPORT_BOUNDS.totalFacts} writes; harvest a shorter window`);
        rec.writes.push({ path, ts: t, eventId, kind });
      }
      rec.droppedPaths += Math.max(0, paths.length - IMPORT_BOUNDS.eventFiles);
    }
    cursor = body.nextCursor as string | null;
    if (!cursor) {
      // Exhausted: any drop at the bound is known exactly.
      if (rec.truncatedWrites > 0) Object.assign(rec, { writesCensored: true, censorReason: "writes_bound_exceeded", droppedWrites: rec.truncatedWrites });
      return rec;
    }
    if (rec.writes.length >= IMPORT_BOUNDS.facts) {
      // At or over the bound with pages left: later pages are never read, so the drop count is unknown.
      Object.assign(rec, {
        writesCensored: true,
        censorReason: rec.truncatedWrites > 0 ? "writes_bound_exceeded_cursor_remaining" : "writes_bound_reached_cursor_remaining",
        droppedWrites: null,
      });
      return rec;
    }
  }
}

/** Reads the facts of every frame session without a facts record yet, `concurrency` at a time. */
export async function runFacts({ client, state, log, concurrency }: RunOptions): Promise<void> {
  const pending = state.frame.rows.filter((r) => !state.facts.has(rowKey(r)));
  const held = { n: [...state.facts.values()].reduce((n, f) => n + f.writes.length, 0) };
  let next = 0;
  let stop: unknown = null;
  const worker = async () => {
    while (stop === null && next < pending.length) {
      const row = pending[next++]!;
      try {
        const record = await sessionFacts(client, row, state.params, held);
        state.facts.set(rowKey(row), record);
        await log.write({ t: "facts", record, ...progress(client) });
      } catch (err) {
        stop ??= err;
      }
    }
  };
  await Promise.all(Array.from({ length: Math.max(1, concurrency) }, worker));
  if (stop !== null) throw stop;
}

export { RequestCapReached };

/** The fields no allowed route exposes, or that policy withholds: recorded in the receipt and the doc. */
export const UNAVAILABLE_FIELDS: { field: string; reason: string }[] = [
  { field: "commits", reason: "no read route exposes per-session commit shas: /v1/sessions/output gives workspace counts only, and per-session output facts are reachable only through /v1/coordination/overlaps, which the harvest must not call" },
  { field: "outcomes", reason: "no read route exposes per-session outcomes or revert flags (same as commits)" },
  { field: "usage", reason: "events carry no token or cost counts; the directory has session totals only, with no time, so no usage fact is written" },
  { field: "writes.branch", reason: "events carry no branch; the session catalog's gitBranch is used for every write" },
  { field: "cwd", reason: "no route exposes the session's working directory, so an absolute filesTouched path is kept only when it is anchored at a source root" },
  { field: "orgId", reason: "no read route returns the org id; --org labels the tenant (hashed)" },
  { field: "contractVersion", reason: "the read routes carry no version or readiness field; X-Sessions-Read-Backend and the buffered and incomplete markers are recorded instead" },
];
export const WITHHELD_FIELDS = ["title", "taskTitle", "shortSummary", "userIds", "machineIds", "models", "agentName", "event content", "summaries", "turns"];

export interface ReceiptInput {
  state: HarvestState;
  stats: ClientStats;
  finishedAt: string;
  workspaceHash: string;
  complete: boolean;
  notes: string[];
  /** Per session: the clamped first activity (for the per-day counts). */
  createdAt: Map<string, string>;
  snapshot: { sessions: number; writes: number; droppedPaths: number; oversizeSessions: number };
}

/** The coverage receipt: what the frame saw, per source and per day, and what the facts could and could not hold. */
export function buildReceipt(r: ReceiptInput): Record<string, unknown> {
  const { state, stats } = r;
  const bySource: Record<string, number> = {};
  const byDay: Record<string, Record<string, number>> = {};
  let withoutCatalog = 0;
  for (const row of state.frame.rows) {
    bySource[row.source] = (bySource[row.source] ?? 0) + 1;
    const day = (r.createdAt.get(rowKey(row)) ?? row.firstTs).slice(0, 10);
    const d = (byDay[day] ??= {});
    d[row.source] = (d[row.source] ?? 0) + 1;
    if (!row.catalog) withoutCatalog++;
  }
  const facts = [...state.facts.values()];
  const failures: Record<string, number> = {};
  for (const f of facts) if (f.failure) failures[f.failure] = (failures[f.failure] ?? 0) + 1;
  const censored = facts.filter((f) => f.writesCensored);
  const censoredBy: Record<string, number> = {};
  for (const f of censored) censoredBy[f.censorReason ?? "unknown"] = (censoredBy[f.censorReason ?? "unknown"] ?? 0) + 1;
  const sum = (k: keyof FactsRecord) => facts.reduce((n, f) => n + (typeof f[k] === "number" ? f[k] as number : 0), 0);
  const sortObj = <T>(o: Record<string, T>) => Object.fromEntries(Object.entries(o).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)));
  return {
    format: RECEIPT_FORMAT,
    complete: r.complete,
    workspaceId: r.workspaceHash,
    window: { since: state.params.since, until: state.params.until },
    startedAt: state.startedAt,
    finishedAt: r.finishedAt,
    runs: state.runs,
    requests: {
      total: stats.requests, byRoute: sortObj(stats.byRoute), retries: stats.retries, retryAfterHonoured: stats.retryAfterHonoured, waitedMs: stats.waitedMs,
      // Restored across runs from the state log; partial (lower bounds) only when a run ended without its counts.
      partial: state.statsPartial !== null,
      ...(state.statsPartial ? { partialReason: state.statsPartial } : {}),
    },
    frame: {
      route: "/v1/sessions",
      params: { limit: FRAME_PAGE_LIMIT, lastTsAfter: state.params.since, firstTsBefore: state.params.until },
      pages: state.frame.pages,
      exhausted: state.frame.done,
      sessions: state.frame.rows.length,
      bySource: sortObj(bySource),
      byDay: sortObj(Object.fromEntries(Object.entries(byDay).map(([d, v]) => [d, sortObj(v)]))),
      duplicateRows: state.frame.duplicates,
      rowsOutsideWindow: state.frame.outsideWindow,
      sessionsWithoutCatalog: withoutCatalog,
    },
    facts: {
      route: "/v1/sessions/:sessionId/events",
      sessionsRead: facts.filter((f) => !f.failure).length,
      sessionsNotRead: state.frame.rows.length - facts.length,
      sessionsWithoutEvents: sortObj(failures),
      pages: sum("pages"),
      events: sum("events"),
      eventsWithFiles: sum("eventsWithFiles"),
      eventsOutsideWindow: sum("eventsOutsideWindow"),
      writesRead: facts.reduce((n, f) => n + f.writes.length, 0),
      truncatedWrites: sum("truncatedWrites"),
      // Sessions whose writes were cut at the per-session bound; their drop count is known only when exhausted.
      sessionsCensored: censored.length,
      censoredByReason: sortObj(censoredBy),
      droppedWritesKnown: censored.reduce((n, f) => n + (f.droppedWrites ?? 0), 0),
      sessionsWithUnknownDrops: censored.filter((f) => f.droppedWrites === null).length,
      droppedPaths: sum("droppedPaths") + r.snapshot.droppedPaths,
    },
    snapshot: { sessions: r.snapshot.sessions, writes: r.snapshot.writes, oversizeSessions: r.snapshot.oversizeSessions },
    output: state.output ?? { unavailable: "not_read" },
    server: { headers: Object.fromEntries(Object.entries(stats.headers).map(([h, v]) => [h, sortObj(v)])) },
    unavailable: UNAVAILABLE_FIELDS,
    withheld: WITHHELD_FIELDS,
    notes: r.notes,
  };
}
