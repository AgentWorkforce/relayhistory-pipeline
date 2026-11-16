/**
 * Delivery envelopes as the ingest writer stores them, one per raw chunk line
 * (`session-ingest/writer.ts` liveLine): `{ kind, origin_id, record_id, revision
 * (decimal string), operation, received_at, payload }`, payload `null` on a delete.
 * Payloads carry the relayhistory columns the desktop uploads (`session_events`,
 * `tool_calls`, `history`, `session_markers`); record ids are the feed's
 * `sha256(["kind", source, session_id, event_uid | tool_use_id])`
 * (`alleyoop/feed-records.ts` feedRecordId). Text is synthetic.
 */
import { createHash } from "node:crypto";

export const ORIGIN = "origin-desktop-1";
const SESSION = "sess-envelope-1";
export const T0 = Date.parse("2026-11-14T09:00:00.000Z");
const RECEIVED = new Date(T0 + 600_000).toISOString();

export const recordId = (kind: string, uid: string) =>
  createHash("sha256").update(JSON.stringify([kind, "claude", SESSION, uid])).digest("hex");

export interface EnvelopeOptions { revision?: number; operation?: "upsert" | "delete"; receivedAt?: string; origin?: string }

export function envelope(kind: string, uid: string, payload: Record<string, unknown> | null, o: EnvelopeOptions = {}): string {
  const operation = o.operation ?? (payload ? "upsert" : "delete");
  return JSON.stringify({
    kind, origin_id: o.origin ?? ORIGIN, record_id: recordId(kind, uid), revision: String(o.revision ?? 1),
    operation, received_at: o.receivedAt ?? RECEIVED, payload: operation === "upsert" ? payload : null,
  });
}

/** One `session_events` row (the message-origin search fixture's column set). */
export function sessionEvent(n: number, role: string, text: string, over: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    session_id: SESSION, source: "claude", id: n, event_index: n, message_id: `m-${n}`, event_uid: `m-${n}:0`,
    role, kind: "text", raw_kind: role, text, ts_ms: T0 + n * 1000, is_meta: 0, is_sidechain: 0,
    control_kind: null, agent_id: null, subagent_session_id: null, provider: "anthropic",
    model: role === "assistant" ? "claude-sonnet-4-6" : null, ...over,
  };
}

export function toolCall(n: number, name: string, args: Record<string, unknown>, over: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    session_id: SESSION, source: "claude", message_id: `m-${n}`, tool_use_id: `toolu_${n}`, name,
    args_json: JSON.stringify(args), is_error: 0, ts_ms: T0 + n * 1000, ...over,
  };
}

export const FIRST_PROMPT = "Fix the flaky upload retry in the ingest worker";
export const SECOND_PROMPT = "Now run the whole suite and push";

/** A Claude session as the desktop delivers it. */
export const deliveredSession: string[] = [
  envelope("history", "h-1", { session_id: SESSION, source: "claude", id: 1, prompt: FIRST_PROMPT, timestamp_ms: T0 + 1000, project: "/work/repo" }),
  envelope("session_event", "m-1:0", sessionEvent(1, "user", FIRST_PROMPT, { origin_kind: "human" })),
  envelope("session_event", "m-2:0", sessionEvent(2, "assistant", "I'll read the worker first.")),
  envelope("tool_call", "toolu_3", toolCall(3, "Read", { file_path: "src/worker.ts" })),
  envelope("tool_call", "toolu_4", toolCall(4, "Bash", { command: "bun test worker" })),
  // A control row (a task notification relayhistory classified) and a meta row are not turns.
  envelope("session_event", "m-5:0", sessionEvent(5, "user", "<task-notification>done</task-notification>", { control_kind: "task_notification" })),
  envelope("session_event", "m-6:0", sessionEvent(6, "user", "Caveat: local command output", { is_meta: 1 })),
  // A subagent's prompt rides in the parent session as a sidechain user row.
  envelope("session_event", "m-7:0", sessionEvent(7, "user", "Subagent brief: grep the retries", { is_sidechain: 1, agent_id: "agent-7" })),
  envelope("session_event", "m-8:0", sessionEvent(8, "assistant", "Fixed: the retry awaits the lease.", { model: "claude-opus-4-6" })),
  envelope("session_event", "m-9:0", sessionEvent(9, "user", SECOND_PROMPT, { origin_kind: "human" })),
  envelope("tool_call", "toolu_10", toolCall(10, "Bash", { command: "bun test" })),
  envelope("session_event", "m-11:0", sessionEvent(11, "assistant", "All green; pushed.")),
  envelope("session_marker", "mk-1", { session_id: SESSION, source: "claude", marker_uid: "mk-1", text: "release", ts_ms: T0 + 11_500 }),
];
