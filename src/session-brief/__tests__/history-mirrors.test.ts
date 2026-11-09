import { expect, test } from "bun:test";
import { canonicalHistoryWithoutMirrors } from "../history-mirrors.ts";
import { briefInputFromCanonical } from "../canonical-input.ts";
import { SESSION_SUMMARY_MAX_MESSAGES } from "../engine/summary-contract.ts";

const scope = { orgId: "org", workspaceId: "workspace", harness: "codex", sessionId: "session" };
const row = (id: string, extra: Record<string, unknown> = {}) => ({
  org_id: scope.orgId, workspace_id: scope.workspaceId, source: scope.harness,
  session_id: scope.sessionId, event_id: id, machine_id: "machine", ts: "2026-11-09T19:00:00Z",
  kind: "session_event", type: "user", actor_role: "user", content: "A synthetic greeting", ...extra,
});
const history = (id = "history", extra = {}) => row(id, { kind: "history", ...extra });
const ids = (rows: Record<string, unknown>[]) => rows.map(r => r.event_id);

test("canonical model sees one actual greeting, not its history mirror", () => {
  const rows = [history(), row("native", { ts: "2026-11-09T19:00:00.500Z" }), row("answer", { type: "assistant", actor_role: "assistant", content: "Synthetic acknowledgement", ts: "2026-11-09T19:00:01Z" })];
  const result = briefInputFromCanonical(scope, [{ machineId: "machine", manifestKey: "opaque", revision: 1 }], { exists: true, generation: "a".repeat(64), legacyCoverage: null, rows: rows.map(row => ({ table: "convergence_events", row })) });
  expect(result.kind).toBe("ready");
  if (result.kind === "ready") expect(result.input.messages.map(m => m.role)).toEqual(["user", "assistant"]);
});

test("one projected capture removes only one history copy; nearest match wins", () => {
  const rows = [history("first"), history("second"), row("native")];
  expect(ids(canonicalHistoryWithoutMirrors(rows))).toEqual(["second", "native"]);
  expect(ids(canonicalHistoryWithoutMirrors([history(), row("later", { ts: "2026-11-09T19:00:02Z" }), row("nearest", { ts: "2026-11-09T19:00:00.001Z" }), history("second", { ts: "2026-11-09T19:00:04Z" })]))).toEqual(["later", "nearest"]);
});

test("assistant mirrors reconcile too and matching never mutates the canonical rows", () => {
  const h = Object.freeze(history("h", { actor_role: "assistant", type: "assistant" }));
  const n = Object.freeze(row("n", { actor_role: "assistant", type: "assistant" }));
  const rows = Object.freeze([h, n]);expect(canonicalHistoryWithoutMirrors(rows)).toEqual([n]);expect(rows).toEqual([h, n]);
});

test("real repeated projected prompts and unmatched historical prompts remain", () => {
  const rows = [row("first"), row("second"), history("earlier", { ts: "2026-11-09T18:00:00Z" })];
  expect(canonicalHistoryWithoutMirrors(rows)).toEqual(rows);
});

test("time window is inclusive and preserves unknown or distant times", () => {
  expect(ids(canonicalHistoryWithoutMirrors([history(), row("native", { ts: "2026-11-09T19:00:02Z" })]))).toEqual(["native"]);
  for (const ts of ["2026-11-09T19:00:02.001Z", "invalid", "1970-01-01T00:00:00Z"]) {
    const rows = [history(), row("native", { ts })]; expect(canonicalHistoryWithoutMirrors(rows)).toEqual(rows);
  }
  const undated = [history("h", { ts: "1970-01-01T00:00:00Z" }), row("n", { ts: "1970-01-01T00:00:00Z" })];expect(canonicalHistoryWithoutMirrors(undated)).toEqual(undated);
});

test.each(["message_id", "user_id", "tool_use_id"])("conflicting raw %s prevents collapse before normalization", field => {
  const a = history("history", { record: { payload: { [field]: "first" } } });
  const b = row("native", { record: { payload: { [field]: "second" } } });
  expect(canonicalHistoryWithoutMirrors([a, b])).toEqual([a, b]);
});

test("missing IDs can match, exact IDs match, contradictory aliases preserve both", () => {
  expect(ids(canonicalHistoryWithoutMirrors([history(), row("native", { record: { payload: { message_id: "same" } } })]))).toEqual(["native"]);
  expect(ids(canonicalHistoryWithoutMirrors([history("h", { message_id: "same" }), row("n", { record: { payload: { message_id: "same" } } })]))).toEqual(["n"]);
  const rows = [history("h", { message_id: "first", record: { payload: { message_id: "second" } } }), row("n")];expect(canonicalHistoryWithoutMirrors(rows)).toEqual(rows);
});

test("raw tool-call aliases cannot evade a conflicting capture identity", () => {
  const rows = [history("h", { toolUseId: "first" }), row("n", { record: { payload: { call_id: "second" } } })];
  expect(canonicalHistoryWithoutMirrors(rows)).toEqual(rows);
});

test.each(["source", "session_id", "org_id", "workspace_id", "subagent_id"])("scope field %s never crosses the mirror match", field => {
  const rows = [history(), row("native", { [field]: "other" })];expect(canonicalHistoryWithoutMirrors(rows)).toEqual(rows);
});

test("different speaker or text and tool activity remain untouched", () => {
  for (const extra of [{ actor_role: "assistant", type: "assistant" }, { content: "Other synthetic prompt" }, { kind: "tool_result", actor_role: "tool_result" }]) {
    const rows = [history(), row("native", extra)];expect(canonicalHistoryWithoutMirrors(rows)).toEqual(rows);
  }
});

test("normalization of embedded images cannot turn different recorded content into mirrors", () => {
  const rows = [history("h", { content: "data:image/png;base64,AAAA" }), row("n", { content: "data:image/png;base64,BBBB" })];
  expect(canonicalHistoryWithoutMirrors(rows)).toEqual(rows);
});

test("foreign raw row refuses scope before it could be collapsed", () => {
  const rows = [history(), row("foreign", { workspace_id: "foreign" })];
  expect(() => briefInputFromCanonical(scope, [], { exists: true, generation: "a".repeat(64), legacyCoverage: null, rows: rows.map(row => ({ table: "convergence_events", row })) })).toThrow("canonical input scope refused");
});

test("already-admitted optional tenant fields do not hide a genuine mirror", () => {
  const h: Record<string, unknown> = history();delete h.org_id;delete h.workspace_id;
  const result = briefInputFromCanonical(scope, [], { exists: true, generation: "a".repeat(64), legacyCoverage: null, rows: [h, row("native")].map(row => ({ table: "convergence_events", row })) });
  expect(result.kind).toBe("ready");if (result.kind === "ready") expect(result.input.messages).toHaveLength(1);
});

test("oversized sources retain the existing ineligible outcome without pair matching", () => {
  const rows = Array.from({ length: SESSION_SUMMARY_MAX_MESSAGES + 1 }, (_, i) => i % 2 ? history(String(i)) : row(String(i)));
  expect(canonicalHistoryWithoutMirrors(rows)).toEqual(rows);
  const result = briefInputFromCanonical(scope, [], { exists: true, generation: "a".repeat(64), legacyCoverage: null, rows: rows.map(row => ({ table: "convergence_events", row })) });
  expect(result.kind).toBe("ineligible");
});
