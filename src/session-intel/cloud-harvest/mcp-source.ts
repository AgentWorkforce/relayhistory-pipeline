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
import { eventFacts, informative, type RawEvent } from "./features.ts";
import type { DirectoryRow, Gap, SessionFacts } from "./harvest.ts";

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

/** A Reader directory row in the harvest's directory shape. Fields the MCP does not serve are null or empty. */
export function directoryRowOf(r: SummaryRow): DirectoryRow {
  return {
    sessionId: r.session_id, source: r.source, lastTs: r.last_active ?? "", createdAt: null, projectId: null,
    project: r.project ? { name: r.project } : null, parentSessionIds: [],
    userIds: (r.owner ?? []).map((o) => o.user_id).filter((u): u is string => typeof u === "string"),
    machineIds: [], eventCount: null, pullRequest: null, workState: null, model: null, branch: r.branch ?? null,
    via: "reader-mcp",
  };
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
export async function harvestMcpFrame(client: CloudClient, window: { since: string; until: string }, log: (m: string) => void = () => {}) {
  const rows = new Map<string, DirectoryRow>();
  const gaps: Gap[] = [];
  let cursor: string | undefined, pages = 0;
  do {
    const page = await client.callTool("list_session_summaries", { since: window.since, limit: LIST_PAGE, ...(cursor ? { cursor } : {}) });
    pages++;
    for (const r of (page.sessions ?? []) as SummaryRow[]) {
      if (!r.session_id || !r.source || (r.last_active && r.last_active < window.since)) continue;
      rows.set(`${r.source}\u0000${r.session_id}`, directoryRowOf(r));
    }
    cursor = typeof page.next_cursor === "string" && page.next_cursor ? page.next_cursor : undefined;
    if (pages % 25 === 0) log(`reader directory pages ${pages}, sessions ${rows.size}`);
  } while (cursor);
  return { rows: [...rows.values()], gaps, pages };
}

export async function harvestMcpSession(client: CloudClient, row: DirectoryRow): Promise<SessionFacts & { truncated: number }> {
  const out: SessionFacts & { truncated: number } = { source: row.source, sessionId: row.sessionId, eventsRead: 0, censored: false, error: null, events: [], truncated: 0 };
  let cursor: string | undefined;
  for (let page = 0; ; page++) {
    if (page >= MAX_MCP_EVENT_PAGES) { out.censored = true; break; }
    let body: Record<string, unknown>;
    try {
      body = await client.callTool("get_shared_session", { session_id: row.sessionId, source: row.source, limit: EVENTS_PAGE, order: "asc", max_content: MAX_CONTENT, ...(cursor ? { cursor } : {}) });
    } catch (e) {
      if (e instanceof HarvestCap) throw e;
      // A directory row whose transcript was never published is not a read failure.
      if (e instanceof McpToolError) { out.error = NOT_FOUND.test(e.message) ? "no_transcript" : "tool_error"; break; }
      if (e instanceof HttpError) { out.error = `http_${e.status}`; break; }
      throw e;
    }
    const events = (body.events ?? []) as McpEvent[];
    for (const e of events) {
      out.eventsRead++;
      if (e.content_truncated) out.truncated++;
      const f = eventFacts(rawEventOf(e));
      if (informative(f)) out.events.push(f);
    }
    cursor = typeof body.next_cursor === "string" && body.next_cursor ? body.next_cursor : undefined;
    if (!cursor || !events.length) break;
  }
  return out;
}
