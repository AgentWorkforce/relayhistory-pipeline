/**
 * Cloud harvest (docs/session-intel-cloud-harvest.md): the session directory over a window, then each coding
 * session's events reduced to derived facts. Output is append-only so a rerun resumes; nothing is ever rewritten
 * except the coverage receipt.
 */
import { appendFileSync, closeSync, existsSync, ftruncateSync, openSync, readFileSync, readSync, statSync } from "node:fs";
import { assertPrivateFile, readPrivate, writePrivate } from "./private-fs.ts";
import { eventNanos, instantOf } from "./instant.ts";
import { join } from "node:path";
import { type CloudClient, HarvestCap, HttpError } from "./client.ts";
import { directoryProjection, eventFacts, informative, type EventFacts, type RawEvent } from "./features.ts";
import { harvestMcpFrame, harvestMcpSession } from "./mcp-source.ts";

/** Harnesses whose events carry tool calls and file edits. Others are kept in the directory only. */
export const CODING_SOURCES = new Set(["claude", "codex", "cursor", "opencode", "grok", "devin", "muse", "copilot", "gemini", "antigravity", "amp", "capy", "openclaw", "replicas"]);
const EVENTS_PAGE = 1000;
export const MAX_EVENT_PAGES = 300;

export interface DirectoryRow { sessionId: string; source: string; lastTs: string; [k: string]: unknown }
export interface Gap { from: string; to: string; reason: string }
/** `outOfWindow` counts events read but dropped, unreduced, for falling outside the approved window. */
export interface SessionFacts { source: string; sessionId: string; eventsRead: number; censored: boolean; error: string | null; events: EventFacts[]; outOfWindow?: number }
export interface Window { since: string; until: string }

/**
 * An interrupted append can leave a final record without its newline. Under the harvest lock, that partial tail
 * (and only it: everything after the last newline) is cut away, so the session is read again and the next append
 * starts on a fresh line. Returns the bytes removed.
 */
export function recoverTornTail(file: string): number {
  if (!existsSync(file)) return 0;
  const size = statSync(file).size;
  if (size === 0) return 0;
  const fd = openSync(file, "r+");
  try {
    const last = Buffer.alloc(1);
    readSync(fd, last, 0, 1, size - 1);
    if (last[0] === 0x0a) return 0;
    const text = readFileSync(file);
    const keep = text.lastIndexOf(0x0a) + 1;
    ftruncateSync(fd, keep);
    return size - keep;
  } finally { closeSync(fd); }
}

/** Every record of a facts file; a malformed line anywhere is corruption and stops the run (no line is skipped). */
export function readFactsStrict(file: string): SessionFacts[] {
  if (!existsSync(file)) return [];
  const text = readPrivate(file);
  if (text && !text.endsWith("\n")) throw new Error(`${file} ends in a partial record; resume the harvest to recover it`);
  return text.split("\n").slice(0, -1).map((line, i) => {
    try { return JSON.parse(line) as SessionFacts; } catch { throw new Error(`${file} is corrupt at line ${i + 1}`); }
  });
}

/**
 * What each reader can enforce at the read boundary. Neither current reader can bound event times on the server:
 * Cloud's events route and the shared-sessions MCP page a session's whole history, so out-of-window events are
 * received and then dropped before derivation. A bulk reader must filter by event time on the server.
 */
export const READERS = {
  cloud: { serverSideEventWindow: false, workspaceInPath: true, boundedListPages: false },
  "reader-mcp": { serverSideEventWindow: false, workspaceInPath: false, boundedListPages: true },
} as const;

/**
 * -1 before the window, 0 inside it, 1 after it, comparing the event's time at its full precision against the
 * admitted bounds (millisecond-exact, cloud-harvest/instant.ts). A time that does not parse counts as outside.
 */
export function inWindow(ts: unknown, w: Window): -1 | 0 | 1 {
  const t = eventNanos(ts), since = BigInt(instantOf(w.since, "window since")) * 1_000_000n, until = BigInt(instantOf(w.until, "window until")) * 1_000_000n;
  if (t === null || t < since) return -1;
  return t > until ? 1 : 0;
}

interface ListPage { items?: Array<Record<string, unknown>>; nextCursor?: string | null; error?: boolean }
/** A keyset cursor just below `ts`; the backend writes microsecond times (`…07.209000Z`). */
export const cursorAt = (ts: string) =>
  `sessions-v2:${JSON.stringify({ ts: new Date(ts).toISOString().replace(/\.(\d{3})Z$/, ".$1000Z"), source: "~", sessionId: "~" })}`;
const minus = (ts: string, ms: number) => new Date(Date.parse(ts) - ms).toISOString();

const failed = (p: ListPage) => p.error === true && (p.items ?? []).length === 0;

/**
 * The backend fails a whole page when one row in it cannot be served, so a failing cursor at `t` means a bad row
 * among the next page's rows, not at `t`. Finds the nearest cursor below `t` that pages again: grow the step until
 * a page succeeds, then binary-search down to `resolutionMs`. Rows between that cursor and `t` are unreachable
 * through the list and are returned as the gap.
 */
export async function stepPastBadRow(client: CloudClient, t: string, floor: string, resolutionMs = 250) {
  let bad = 0, good = 1_000, page: ListPage | null = null;
  for (;;) {
    if (minus(t, good) <= floor) return { gapFrom: floor, page: null, probes: 0 };
    const p = await client.get("list", `/sessions?cursor=${encodeURIComponent(cursorAt(minus(t, good)))}`) as ListPage;
    if (!failed(p)) { page = p; break; }
    bad = good; good *= 4;
  }
  while (good - bad > resolutionMs) {
    const mid = Math.floor((bad + good) / 2);
    const p = await client.get("list", `/sessions?cursor=${encodeURIComponent(cursorAt(minus(t, mid)))}`) as ListPage;
    if (failed(p)) bad = mid; else { good = mid; page = p; }
  }
  return { gapFrom: minus(t, good), page };
}

/**
 * Pages the directory newest first until `since`. A failing page is stepped over with `stepPastBadRow`, and the
 * skipped span is recorded as a gap, never silently dropped.
 */
/** Thrown when a directory listing would exceed the approved row ceiling: nothing beyond it is read. */
/** Thrown when the directory row ceiling is reached, or a page brings more rows than it was granted. */
export class RowCeiling extends Error {}
/**
 * The directory row allowance, reserved before each list request. `reserve(max)` grants at most `max` rows from what
 * is left (throwing RowCeiling when nothing is) and counts them as spent at once; `settle` returns the unused part
 * and refuses a page that brought more than its grant. A request is therefore never sent for rows beyond the
 * ceiling, and an interruption between the two can only over-count.
 */
export interface RowBudget { reserve(max: number): number; settle(granted: number, received: number): void }


/**
 * The Cloud directory. Its list route takes no page size, so a row ceiling cannot be enforced before a request; a
 * harvest under a row ceiling refuses this reader (READERS.cloud.boundedListPages).
 */
export async function harvestFrame(client: CloudClient, window: { since: string; until: string }, log: (m: string) => void = () => {}) {
  const rows = new Map<string, DirectoryRow>();
  const gaps: Gap[] = [];
  let cursor: string | null = cursorAt(window.until);
  let pages = 0, lastTs = window.until;
  let pending: ListPage | null = null;
  while (cursor || pending) {
    const page: ListPage = pending ?? await client.get("list", `/sessions?cursor=${encodeURIComponent(cursor!)}`) as ListPage;
    pending = null;
    pages++;
    const items = page.items ?? [];
    if (failed(page)) {
      const step = await stepPastBadRow(client, lastTs, window.since);
      gaps.push({ from: step.gapFrom, to: lastTs, reason: "directory_page_error" });
      log(`directory error page below ${lastTs}; resumed at ${step.gapFrom}`);
      lastTs = step.gapFrom;
      if (!step.page) break;
      pending = step.page;
      continue;
    }
    let reachedSince = false;
    for (const it of items) {
      const ts = String(it.lastTs ?? "");
      if (ts && ts < window.since) { reachedSince = true; continue; }
      const row = directoryProjection(it, "cloud") as DirectoryRow | null;
      if (row) rows.set(`${row.source}\u0000${row.sessionId}`, row);
      if (ts) lastTs = ts;
    }
    if (pages % 25 === 0) log(`directory pages ${pages}, sessions ${rows.size}, at ${lastTs}`);
    cursor = reachedSince ? null : page.nextCursor ?? null;
    if (!cursor && !reachedSince && lastTs > window.since) gaps.push({ from: window.since, to: lastTs, reason: "directory_ended_early" });
  }
  return { rows: [...rows.values()], gaps, pages };
}

interface EventsPage { events?: RawEvent[]; nextCursor?: string | null }

export async function harvestSession(client: CloudClient, row: DirectoryRow, window: Window): Promise<SessionFacts> {
  const out: SessionFacts = { source: row.source, sessionId: row.sessionId, eventsRead: 0, censored: false, error: null, events: [], outOfWindow: 0 };
  let cursor: string | null = null, seq = 0;
  for (let page = 0; ; page++) {
    if (page >= MAX_EVENT_PAGES) { out.censored = true; break; }
    const q = new URLSearchParams({ limit: String(EVENTS_PAGE), order: "asc" });
    if (cursor) q.set("cursor", cursor);
    let body: EventsPage;
    try {
      body = await client.get("events", `/reflex/sessions/${encodeURIComponent(row.sessionId)}/events?${q}`) as EventsPage;
    } catch (e) {
      if (e instanceof HttpError) { out.error = `http_${e.status}`; break; }
      throw e;
    }
    let past = false;
    for (const raw of body.events ?? []) {
      out.eventsRead++;
      // The window is enforced before an event is reduced: nothing outside it is derived or kept.
      const w = inWindow(raw.ts, window);
      if (w !== 0) { out.outOfWindow!++; if (w > 0) past = true; continue; }
      const f = { ...eventFacts(raw), seq: seq++ };
      if (informative(f)) out.events.push(f);
    }
    cursor = body.nextCursor ?? null;
    // Events come oldest first, so once one is past the window the rest are too.
    if (!cursor || !(body.events ?? []).length || past) break;
  }
  return out;
}

/** Pages `slices` equal sub-windows concurrently and merges them; each slice records its own gaps. */
export async function harvestFrameSliced(client: CloudClient, window: { since: string; until: string }, slices: number, log: (m: string) => void = () => {}) {
  const a = Date.parse(window.since), b = Date.parse(window.until), w = (b - a) / slices;
  const parts = await Promise.all(Array.from({ length: slices }, (_, i) => harvestFrame(client, {
    since: new Date(a + i * w).toISOString(), until: new Date(i === slices - 1 ? b : a + (i + 1) * w).toISOString(),
  }, (m) => log(`[slice ${i + 1}/${slices}] ${m}`))));
  const rows = new Map<string, DirectoryRow>();
  for (const p of parts) for (const r of p.rows) rows.set(`${r.source}\u0000${r.sessionId}`, r);
  return { rows: [...rows.values()], gaps: parts.flatMap((p) => p.gaps), pages: parts.reduce((n, p) => n + p.pages, 0) };
}

export interface HarvestOptions {
  out: string; window: { since: string; until: string }; concurrency: number; frameSlices?: number; log?: (m: string) => void;
  /** Sessions whose project name is one of these are read first (newest first within each group). */
  priorityProjects?: string[];
  /** The approved directory row allowance (see RowBudget). Only a reader with bounded list pages can honour it. */
  rowBudget?: RowBudget;
  /** Sessions whose project name is one of these are not read at all, and are counted as skipped. */
  skipProjects?: string[];
  /**
   * Where sessions are read from: Cloud's list and events routes (relayhistory; sessions before the 2026-11-05
   * storage move), or the hosted shared-sessions MCP (the 96Bulls Reader; sessions after it). Default "cloud".
   */
  via?: "cloud" | "reader-mcp";
}
const projectNameOf = (r: DirectoryRow) => { const p = r.project as { name?: unknown } | null; return typeof p?.name === "string" ? p.name.toLowerCase() : ""; };

export async function runHarvest(client: CloudClient, opts: HarvestOptions) {
  const log = opts.log ?? (() => {});
  const dirFile = join(opts.out, "directory.jsonl"), factsFile = join(opts.out, "facts.jsonl"), frameFile = join(opts.out, "frame.json");
  // The Reader's list tool reads the credential's own workspace, so the credential is attested first.
  if (opts.via === "reader-mcp") await client.attestWorkspace();
  let frame: { rows: DirectoryRow[]; gaps: Gap[]; pages: number };
  // Every file the harvest resumes from or writes must be private (private-fs.ts); the caller holds the lock.
  for (const f of [dirFile, factsFile, frameFile, join(opts.out, "coverage.json")]) assertPrivateFile(f);
  recoverTornTail(factsFile);
  if (opts.rowBudget && !READERS[opts.via ?? "cloud"].boundedListPages) throw new RowCeiling(`--via ${opts.via ?? "cloud"} cannot bound its list pages, so the row ceiling cannot be enforced`);
  // A finished listing was counted when it was received; a resume does not list again.
  if (existsSync(frameFile)) frame = JSON.parse(readPrivate(frameFile));
  else {
    frame = opts.via === "reader-mcp"
      ? await harvestMcpFrame(client, opts.window, log, opts.rowBudget)
      : await harvestFrameSliced(client, opts.window, Math.max(1, opts.frameSlices ?? 1), log);
    writePrivate(dirFile, frame.rows.map((r) => JSON.stringify(r)).join("\n") + "\n");
    writePrivate(frameFile, JSON.stringify(frame));
  }
  const done = new Set<string>();
  for (const s of readFactsStrict(factsFile)) done.add(`${s.source}\u0000${s.sessionId}`);
  const skip = new Set((opts.skipProjects ?? []).map((x) => x.toLowerCase()));
  const prio = new Set((opts.priorityProjects ?? []).map((x) => x.toLowerCase()));
  const rank = (r: DirectoryRow) => (prio.has(projectNameOf(r)) ? 0 : 1);
  const queue = frame.rows.filter((r) => CODING_SOURCES.has(r.source) && !skip.has(projectNameOf(r)) && !done.has(`${r.source}\u0000${r.sessionId}`))
    .sort((a, b) => rank(a) - rank(b) || (a.lastTs < b.lastTs ? 1 : -1));
  log(`sessions ${frame.rows.length}; coding to read ${queue.length}; already read ${done.size}`);
  let read = 0, capped = false;
  const worker = async () => {
    for (let row = queue.shift(); row && !capped; row = queue.shift()) {
      try {
        const s = opts.via === "reader-mcp" ? await harvestMcpSession(client, row, opts.window) : await harvestSession(client, row, opts.window);
        appendFileSync(factsFile, JSON.stringify(s) + "\n", { mode: 0o600 });
        if (++read % 50 === 0) log(`sessions read ${read}, requests ${client.requests.total}`);
      } catch (e) {
        if (e instanceof HarvestCap) { capped = true; return; }
        throw e;
      }
    }
  };
  await Promise.all(Array.from({ length: opts.concurrency }, worker));
  return writeCoverage(opts, frame, factsFile, client, capped);
}

function writeCoverage(opts: HarvestOptions, frame: { rows: DirectoryRow[]; gaps: Gap[]; pages: number }, factsFile: string, client: CloudClient, capped: boolean) {
  const bySource: Record<string, { sessions: number; read: number; events: number; informative: number; withFiles: number; censored: number; errors: number; noTranscript: number; truncated: number; outOfWindow: number }> = {};
  for (const r of frame.rows) { bySource[r.source] ??= { sessions: 0, read: 0, events: 0, informative: 0, withFiles: 0, censored: 0, errors: 0, noTranscript: 0, truncated: 0, outOfWindow: 0 }; bySource[r.source]!.sessions++; }
  for (const s of readFactsStrict(factsFile) as Array<SessionFacts & { truncated?: number }>) {
    const b = bySource[s.source];
    if (!b) continue;
    b.read++; b.events += s.eventsRead; b.informative += s.events.length;
    if (s.events.some((e) => e.files.length || e.touched.length)) b.withFiles++;
    if (s.censored) b.censored++;
    // A directory row with no published transcript has nothing to read; it is counted, not failed.
    if (s.error === "no_transcript") b.noTranscript++;
    else if (s.error) b.errors++;
    b.truncated += s.truncated ?? 0;
    b.outOfWindow += s.outOfWindow ?? 0;
  }
  const coding = Object.entries(bySource).filter(([k]) => CODING_SOURCES.has(k));
  const complete = !capped && frame.gaps.length === 0 && coding.every(([, b]) => b.read === b.sessions && b.censored === 0 && b.errors === 0);
  const coverage = {
    format: "session-intel-cloud-harvest-receipt/1", via: opts.via ?? "cloud", window: opts.window, complete, capped,
    frame: { pages: frame.pages, sessions: frame.rows.length, gaps: frame.gaps }, bySource, requests: client.requests,
    withheld: ["content", "summary", "shortSummary", "title", "taskTitle"],
  };
  writePrivate(join(opts.out, "coverage.json"), JSON.stringify(coverage, null, 2));
  return coverage;
}
