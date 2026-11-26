/**
 * The Reader source for the cloud harvest (docs/session-intel-cloud-harvest.md, "Sessions after the storage move").
 * Since 2026-11-05 coding sessions are stored by the 96Bulls Reader (Supabase directory, ClickHouse events), and
 * Cloud's `/sessions` list and `/reflex/.../events` routes still read relayhistory, so they never list those
 * sessions. The hosted shared-sessions MCP reads the Reader; this module pages its two read tools and maps their
 * rows onto the shapes the rest of the harvest already uses, so the same `eventFacts` reduction (and the same
 * privacy boundary) applies.
 *
 * What the MCP does not carry, compared with the Cloud routes: `createdAt`, `parentSessionIds` and
 * `machineIds` on directory rows, and `filesTouched` on events; event content is capped at `MAX_CONTENT`
 * characters. Each harvested session records how many of its events were truncated.
 */
import { type CloudClient, HarvestCap, HttpError, McpToolError } from "./client.ts";
import { directoryProjection, eventFacts, informative, type RawEvent } from "./features.ts";
import { type DirectoryRow, type Gap, inWindow, RowCeiling, type RowBudget, type SessionFacts, type Window } from "./harvest.ts";

const LIST_PAGE = 50;
const EVENTS_PAGE = 50;
export const MAX_CONTENT = 4000;
/** Same event cap as the Cloud route (300 pages of 1,000). */
export const MAX_MCP_EVENT_PAGES = 6000;
const NOT_FOUND = /^No shared session was found/;

interface SummaryRow {
  session_id: string; source: string; owner?: Array<{ user_id?: string | null }> | null; project?: string | null;
  repo?: string | null; branch?: string | null; last_active?: string | null;
}
interface McpEvent {
  event_id: string; timestamp: string; kind?: string | null; type?: string | null; actor_role?: string | null;
  tool_name?: string | null; tool_status?: string | null; subagent_id?: string | null; user_id?: string | null;
  project_id?: string | null; content?: string | null; content_truncated?: boolean | null;
}

/** A Reader directory row through the same closed projection as Cloud's (features.ts `directoryProjection`). */
export function directoryRowOf(r: SummaryRow): DirectoryRow | null {
  return directoryProjection({
    sessionId: r.session_id, source: r.source, lastTs: r.last_active, project: { name: r.project },
    userIds: (r.owner ?? []).map((o) => o.user_id), branch: r.branch,
  }, "reader-mcp") as DirectoryRow | null;
}

export const rawEventOf = (e: McpEvent): RawEvent => ({
  eventId: e.event_id, ts: e.timestamp, kind: e.kind ?? null, type: e.type ?? null, actorRole: e.actor_role ?? null,
  toolName: e.tool_name ?? null, toolStatus: e.tool_status ?? null, subagentId: e.subagent_id ?? null,
  userId: e.user_id ?? null, projectId: e.project_id ?? null, filesTouched: null, content: e.content ?? null,
});

/**
 * Pages `list_session_summaries` newest first for sessions last active since `window.since`. `until` is not
 * applied: `last_active` is a session's latest activity, so a session that worked in the window and again later
 * would be lost (event times bound the analysis instead). Pages may be short or empty and still continue.
 */
/** Every response names its workspace; one that names another (a misrouted answer) is refused before projection. */
function sameWorkspace(client: CloudClient, body: Record<string, unknown>) {
  if (body.workspace_id !== client.workspaceId) throw new Error("a shared-sessions response named a different workspace (or none)");
}

export async function harvestMcpFrame(client: CloudClient, window: { since: string; until: string }, log: (m: string) => void = () => {}, budget?: RowBudget) {
  const rows = new Map<string, DirectoryRow>();
  const gaps: Gap[] = [];
  let cursor: string | undefined, pages = 0;
  do {
    // The page size is the allowance left (reserved before sending), so no request asks for rows beyond the ceiling.
    const limit = budget ? budget.reserve(LIST_PAGE) : LIST_PAGE;
    const page = await client.callTool("list_session_summaries", { since: window.since, limit, ...(cursor ? { cursor } : {}) });
    pages++;
    sameWorkspace(client, page);
    const received = Array.isArray(page.sessions) ? page.sessions.length : 0;
    if (budget) budget.settle(limit, received);
    else if (received > limit) throw new RowCeiling("a directory page brought more rows than it asked for");
    for (const r of (page.sessions ?? []) as SummaryRow[]) {
      if (!r.session_id || !r.source || (r.last_active && r.last_active < window.since)) continue;
      const row = directoryRowOf(r);
      if (row) rows.set(`${row.source}\u0000${row.sessionId}`, row);
    }
    cursor = typeof page.next_cursor === "string" && page.next_cursor ? page.next_cursor : undefined;
    if (pages % 25 === 0) log(`reader directory pages ${pages}, sessions ${rows.size}`);
  } while (cursor);
  return { rows: [...rows.values()], gaps, pages };
}

export async function harvestMcpSession(client: CloudClient, row: DirectoryRow, window: Window): Promise<SessionFacts & { truncated: number; outOfWindow: number }> {
  const out: SessionFacts & { truncated: number; outOfWindow: number } = { source: row.source, sessionId: row.sessionId, eventsRead: 0, censored: false, error: null, events: [], truncated: 0, outOfWindow: 0 };
  let cursor: string | undefined, seq = 0;
  for (let page = 0; ; page++) {
    if (page >= MAX_MCP_EVENT_PAGES) { out.censored = true; break; }
    let body: Record<string, unknown>;
    try {
      body = await client.callTool("get_shared_session", { session_id: row.sessionId, source: row.source, workspace_id: client.workspaceId, limit: EVENTS_PAGE, order: "asc", max_content: MAX_CONTENT, ...(cursor ? { cursor } : {}) });
    } catch (e) {
      if (e instanceof HarvestCap) throw e;
      // A directory row whose transcript was never published is not a read failure.
      if (e instanceof McpToolError) { out.error = NOT_FOUND.test(e.message) ? "no_transcript" : "tool_error"; break; }
      if (e instanceof HttpError) { out.error = `http_${e.status}`; break; }
      throw e;
    }
    sameWorkspace(client, body);
    const events = (body.events ?? []) as McpEvent[];
    let past = false;
    for (const e of events) {
      out.eventsRead++;
      // The window is enforced before an event is reduced: nothing outside it is derived or kept.
      const w = inWindow(e.timestamp, window);
      if (w !== 0) { out.outOfWindow++; if (w > 0) past = true; continue; }
      if (e.content_truncated) out.truncated++;
      const f = { ...eventFacts(rawEventOf(e)), seq: seq++ };
      if (informative(f)) out.events.push(f);
    }
    cursor = typeof body.next_cursor === "string" && body.next_cursor ? body.next_cursor : undefined;
    // Events come oldest first, so once one is past the window the rest are too.
    if (!cursor || !events.length || past) break;
  }
  return out;
}
