import { expect, test } from "bun:test";
import { projectEventRows } from "../canonical-projection.ts";
import { canonicalEventPage } from "../canonical-events.ts";
import { parseRelayhistoryRecord, type RelayhistoryStreamRecord } from "../../../packages/pipeline/src/lib/pipeline/relayhistory-transform.ts";
import type { Liveness } from "../page.ts";

const scope = { orgId: "org-a", workspaceId: "workspace-a", harness: "codex", sessionId: "session-a" };
const identity = { sessionId: scope.sessionId, machineId: "machine-a", userId: "user-a" };
const event = (id: string, kind: string, payload: Record<string, unknown>, origin = "origin-a"): RelayhistoryStreamRecord =>
  parseRelayhistoryRecord(JSON.stringify({ kind, origin_id: origin, record_id: id, revision: "1", operation: "upsert", received_at: "2026-11-09T00:00:00Z", payload: { source: "codex", session_id: "physical-a", ts_ms: 1794182400000, ...payload } }));
const capture = (text = "[REDACTED]") => event("capture", "session_event", { role: "assistant", kind: "tool_use", message_id: "recorded-message", text });
const call = (patch: Record<string, unknown> = {}, origin = "origin-a") => event("call", "tool_call", { message_id: "recorded-message", tool_use_id: "recorded-call", name: "recorded_tool", args_json: "{}", ...patch }, origin);
function project(records: RelayhistoryStreamRecord[], harness = "codex") {
  const result = projectEventRows(records, { ...scope, harness }, identity, 1);
  if ("error" in result) throw result.error;
  return result;
}
function captured(result: ReturnType<typeof project>) { return result.events.find(e => e.row.delivery_record_id === "capture")!; }

test("explicit paired Codex tool use stays activity even when the scrubbed input is only a placeholder", () => {
  for (const text of ["[REDACTED]", "ordinary recorded tool input"]) {
    // Message identity, not an equal timestamp or content, binds the witness.
    const result = project([capture(text), call({ ts_ms: 1794182405000 })]);
    const selected = captured(result);
    expect(selected.row.type).toBe("tool_use"); expect(selected.row.tool_name).toBe("recorded_tool");
    expect(selected.indexRow.type).toBe("tool_use"); expect(selected.indexRow.tool_name).toBe("recorded_tool");
    expect(selected.row.content).toBe(text); expect(selected.indexRow.content).toBe(text);
    expect(result.events).toHaveLength(2);
    expect(selected.row.delivery_record_id).toBe("capture");
  }
});

test("a real assistant redaction is never hidden or reclassified by a neighboring tool", () => {
  for (const kind of ["message", "text", "reasoning", undefined]) {
    const row = event("capture", "session_event", { role: "assistant", kind, message_id: "recorded-message", text: "[REDACTED]" });
    const selected = captured(project([row, call()]));
    expect(selected.row.type).toBe("assistant"); expect(selected.row.content).toBe("[REDACTED]");
  }
});

test("missing, ambiguous, foreign-origin or foreign-physical-session witnesses cannot reclassify chat", () => {
  for (const records of [
    [capture()], [capture(), call({ message_id: "other-message" })],
    [capture(), call({}, "other-origin")], [capture(), call({ session_id: "other-physical" })],
    [capture(), call({ source: "other-source" })], [capture(), call({ name: "" })],
    [capture(), call({ name: " " })],
    [event("capture", "session_event", { role: "assistant", kind: "tool_use", message_id: " ", text: "[REDACTED]" }), call({ message_id: " " })],
    [capture(), call(), event("second-call", "tool_call", { message_id: "recorded-message", name: "another_tool" })],
  ]) expect(captured(project(records)).row.type).toBe("assistant");
  expect(captured(project([capture(), call()], "claude")).row.type).toBe("assistant");
});

test("canonical page preserves identities, content and count while publishing the recorded non-chat classification", async () => {
  const original = project([capture(), call(), event("answer", "session_event", { role: "assistant", kind: "message", message_id: "answer-message", text: "[REDACTED]" })]);
  const page = await canonicalEventPage({ source: async () => ({ exists: true, generation: "a".repeat(64), legacyCoverage: null, rows: original.events.map(({ row }) => ({ table: "convergence_events", row })) }) }, {
    erased: async () => new Set(),
  } as Liveness, { ...scope, sessionId: scope.sessionId, source: "codex", order: "asc", limit: 100, maxContent: 100000 });
  expect(page.events).toHaveLength(3);
  const c = page.events.find(e => e.eventId.endsWith(":capture"))!;
  const a = page.events.find(e => e.eventId.endsWith(":answer"))!;
  expect(c.type).toBe("tool_use"); expect(c.toolName).toBe("recorded_tool"); expect(c.content).toBe("[REDACTED]");
  expect(a.type).toBe("assistant"); expect(a.content).toBe("[REDACTED]");
  expect(page.events.map(e => e.eventId).sort()).toEqual(original.events.map(e => String(e.row.event_id)).sort());
});
