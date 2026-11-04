/**
 * Compact aggregate candidates derived from the canonical S3 fold — the only
 * facts session_output/session_activity need, so no transcript text reaches
 * Supabase. Mirrors the oracle semantics in the old lib/session-output.ts:
 * message dedup keys, legacy-turn exclusion, normalized line counts, tool-call
 * commit correlation within the requested window.
 */
export type CandidateType =
  | "assistant_message" | "legacy_turn" | "explicit_commit" | "tool_commit";

export interface FoldEvent {
  orgId: string; workspaceId: string; source: string; sessionId: string;
  kind: string; type: string; ts: string;
  actorRole?: string; content?: string;
  requestKey?: string; deliveryRecordId?: string;
  record?: { payload?: {
    message_id?: string; kind?: string; args_json?: string;
    tool_use_id?: string; parent_id?: string; turn_id?: string;
  } };
  toolName?: string; machineId?: string; eventId?: string;
}
export interface LegacyTurn { id: string; orgId: string; workspaceId: string; source: string; sessionId: string; ts: string }
export interface CommitRecord {
  sha: string; orgId: string; workspaceId: string; source: string; sessionId: string;
  ts: string; stats?: unknown;
  provenance: string;
}
export interface CandidateRow {
  orgId: string; workspaceId: string; source: string; sessionId: string;
  type: CandidateType; dedupKey: string; ts: string;
  stats?: unknown; provenance?: Record<string, string>;
}

const GIT_COMMIT = /\bgit\b[^\n]*\bcommit\b/;
const RESULT_SHA = /(?:^|\n)\[[^\]\n]+ ([0-9a-f]{7,40})\][^\n]*\n[ \t]*[0-9]+ files? changed(?:, ([0-9]+) insertions?\(\+\))?(?:, ([0-9]+) deletions?\(-\))?/g;
const TOOLS = new Set(["exec_command","exec","Bash","bash","shell","run_shell_command"]);

export function deriveCandidates(
  events: FoldEvent[], turns: LegacyTurn[], links: CommitRecord[], outcomes: CommitRecord[],
): CandidateRow[] {
  // Publication is ALL-TIME per scope: turn suppression is applied at read
  // time in session_output (window-scoped), so every legacy turn is kept here.
  const out: CandidateRow[] = [];
  const messageIds = new Map<string, string>();
  for (const e of events) {
    if (e.actorRole !== "assistant" || !(e.content ?? "").trim()) continue;
    if (!["session_event", "assistant", "text"].includes(e.kind)) continue;
    const pk = e.record?.payload;
    if (!(pk?.kind === "text" || (pk?.kind === undefined && ["assistant","text"].includes(e.type)))) continue;
    const key = e.requestKey || pk?.message_id || e.deliveryRecordId || `${e.machineId}:${e.eventId}`;
    const msgKey = `${e.source}:${e.sessionId}:${key}`;
    if (messageIds.has(msgKey)) continue;
    messageIds.set(msgKey, key);
    out.push({ orgId: e.orgId, workspaceId: e.workspaceId, source: e.source, sessionId: e.sessionId,
      type: "assistant_message", dedupKey: key, ts: e.ts });
  }
  for (const t of turns)
    out.push({ orgId: t.orgId, workspaceId: t.workspaceId, source: t.source, sessionId: t.sessionId,
      type: "legacy_turn", dedupKey: t.id, ts: t.ts });
  for (const c of [...links, ...outcomes])
    out.push({ orgId: c.orgId, workspaceId: c.workspaceId, source: c.source, sessionId: c.sessionId,
      type: "explicit_commit", dedupKey: c.sha.toLowerCase(), ts: c.ts,
      stats: c.stats, provenance: { method: c.provenance } });
  // Tool-derived commits: oracle correlation — a tool_result matches when a
  // tool_call exists in the same scope+session, not after it, sharing
  // tool_use_id, parent/turn-compatible, and the call set collapses to exactly
  // one logical call that is a git-commit shell tool (ambiguity or a competing
  // non-commit call disqualifies the candidate entirely).
  for (const r of events.filter(e => e.actorRole === "tool_result")) {
    const rp = r.record?.payload ?? {};
    const calls = events.filter(c =>
      c.orgId === r.orgId && c.workspaceId === r.workspaceId &&
      c.source === r.source && c.sessionId === r.sessionId &&
      c.kind === "tool_call" && c.ts <= r.ts &&
      (c.record?.payload?.tool_use_id ?? "") !== "" &&
      c.record?.payload?.tool_use_id === rp.tool_use_id &&
      (!rp.parent_id || rp.parent_id === c.record?.payload?.message_id ||
       rp.message_id === c.record?.payload?.message_id) &&
      (!rp.turn_id || events.some(par =>
        par.orgId === c.orgId && par.workspaceId === c.workspaceId &&
        par.source === c.source && par.sessionId === c.sessionId &&
        par.kind === "session_event" &&
        (par.record?.payload?.message_id ?? "") !== "" &&
        par.record?.payload?.message_id === c.record?.payload?.message_id &&
        par.record?.payload?.turn_id === rp.turn_id)));
    const logical = new Set(calls.map(c => JSON.stringify([
      c.record?.payload?.message_id ?? null, c.toolName ?? null,
      c.record?.payload?.args_json ?? null,
      c.record?.payload?.message_id ? null : c.ts])));
    if (logical.size !== 1) continue;
    const call = calls[0]!;
    if (!(call.toolName && TOOLS.has(call.toolName) &&
          GIT_COMMIT.test(call.record?.payload?.args_json ?? ""))) continue;
    for (const m of (r.content ?? "").matchAll(RESULT_SHA) as unknown as RegExpExecArray[])
      out.push({ orgId: r.orgId, workspaceId: r.workspaceId, source: r.source, sessionId: r.sessionId,
        type: "tool_commit", dedupKey: m[1]!.toLowerCase(), ts: r.ts,
        stats: { added: m[2] ?? "0", deleted: m[3] ?? "0" },
        provenance: { call: call.record?.payload?.tool_use_id ?? "" } });
  }
  return out;
}
