import { expect, test } from "bun:test";
import { normalizeRelayhistoryRecords, parseRelayhistoryRecord, type RelayhistoryStreamRecord } from "@rhc/lib/pipeline/relayhistory-transform.ts";
import { deliveryMachine, projectRelayhistorySession } from "@rhc/lib/pipeline/relayhistory-projection.ts";
import { maskedProjection, maskRecords, type HistoryMask } from "../mask.ts";
import { extractPullRequestEvidence, type PrEvidenceProjectionRow } from "../extract.ts";

const scope = { workspaceId: "ws-1", source: "claude", sessionId: "s-1" };
const T = 1_793_000_000_000;
let n = 0;
const rec = (kind: string, payload: Record<string, unknown>, o: Partial<RelayhistoryStreamRecord> = {}): RelayhistoryStreamRecord =>
  parseRelayhistoryRecord(JSON.stringify({ kind, origin_id: "o1", record_id: `r${++n}`, revision: "1",
    received_at: "2026-10-30 12:00:00.000000+00", payload, ...o }));
const ghComment = (id: string, number: number) => [
  rec("tool_call", { name: "Bash", tool_use_id: id, args_json: JSON.stringify({ command: `gh pr comment ${number} -b hi` }), is_error: 0, ts_ms: T + 10 }),
  rec("session_event", { role: "tool_result", kind: "tool_result", tool_use_id: id, text: "commented", ts_ms: T + 11 }),
];
const all = (records: RelayhistoryStreamRecord[]): HistoryMask => new Map(records.map((r) => [r.record_id, null]));
/** The reader's shape: normalized records plus its own (unmasked) projection. */
const readOf = (records: RelayhistoryStreamRecord[]) => {
  const normalized = normalizeRelayhistoryRecords(records);
  return { records: normalized, projectionRows: projectRelayhistorySession(normalized, { ...scope, machineId: "m", userId: "u" }).rows as PrEvidenceProjectionRow[] };
};
const numbers = (rows: PrEvidenceProjectionRow[]) => extractPullRequestEvidence(rows).map((r) => r.pull_request_url);

test("an out-of-mask session record that would win the catalog collapse cannot suppress the in-mask repo_url", () => {
  const inMask = rec("session", { repo_url: "https://github.com/acme/repo.git" }, { accepted_seq: "1" });
  const moved = rec("session", { repo_url: "https://github.com/evil/elsewhere.git" }, { received_at: "2026-10-30 12:00:05.000000+00", accepted_seq: "2" });
  const calls = ghComment("t1", 7);
  const read = readOf([inMask, moved, ...calls]);
  const mask = all([inMask, ...calls]);
  // The unmasked projection collapses to the LATER (out-of-mask) record: the bug the old post-projection
  // filter had, because dropping that collapsed row also dropped the in-mask repo.
  expect(numbers(read.projectionRows)).toEqual(["https://github.com/evil/elsewhere/pull/7"]);
  expect(numbers(read.projectionRows.filter((r) => r.table !== "session_catalog"))).toEqual([]);
  expect(numbers(maskedProjection(read, scope, mask))).toEqual(["https://github.com/acme/repo/pull/7"]);
});

test("colliding commit-link catalog keys: the in-mask link survives an out-of-mask later one", () => {
  const link = (id: string, at: string, seq: string) => rec("commit_link", { commit_sha: "deadbeef", match_method: "trailer", created_at_ms: T,
    evidence_json: JSON.stringify({ github_pr: { id } }) }, { received_at: at, accepted_seq: seq });
  const keep = link("acme/repo#18", "2026-10-30 12:00:00.000000+00", "1");
  const moved = link("evil/elsewhere#99", "2026-10-30 12:00:09.000000+00", "2");
  const read = readOf([keep, moved]);
  expect(numbers(read.projectionRows)).toEqual(["https://github.com/evil/elsewhere/pull/99"]);
  expect(numbers(maskedProjection(read, scope, all([keep])))).toEqual(["https://github.com/acme/repo/pull/18"]);
});

test("moved tool calls never contribute; delete winners and other-origin or other-revision copies are dropped raw", () => {
  const mine = ghComment("t1", 1), moved = ghComment("t2", 2);
  const catalog = rec("session", { repo_url: "https://github.com/acme/repo" });
  const read = readOf([catalog, ...mine, ...moved]);
  expect(numbers(read.projectionRows).sort()).toEqual(["https://github.com/acme/repo/pull/1", "https://github.com/acme/repo/pull/2"]);
  expect(numbers(maskedProjection(read, scope, all([catalog, ...mine])))).toEqual(["https://github.com/acme/repo/pull/1"]);
  const r = mine[1]!;
  expect(maskRecords([r], new Map([[r.record_id, { originId: "o1", operation: "delete" as const, revision: "1" }]]))).toEqual([]);
  expect(maskRecords([r], new Map([[r.record_id, { originId: "o2", operation: "upsert" as const, revision: "1" }]]))).toEqual([]);
  expect(maskRecords([r], new Map([[r.record_id, { originId: "o1", operation: "upsert" as const, revision: "2" }]]))).toEqual([]);
  expect(maskRecords([r], new Map([[r.record_id, { originId: "o1", operation: "upsert" as const, revision: "1" }]]))).toEqual([r]);
});

/** A raw direct `convergence_event` line: the row it carries is its payload (Neon export shape). */
const directLine = (machine: string, eventId: string, extra: Record<string, unknown>): RelayhistoryStreamRecord => ({
  kind: "convergence_event", origin_id: `machine:${machine}`, record_id: eventId, revision: "1", operation: "upsert",
  received_at: "2026-10-30 12:00:00.000000+00",
  payload: { org_id: "org", workspace_id: scope.workspaceId, machine_id: machine, source: scope.source, session_id: scope.sessionId,
    user_id: "u", event_id: eventId, delivery_record_id: eventId, ts: "2026-11-01 00:00:00+00", ...extra },
});
const directCall = (machine: string, eventId: string, tool: string) => directLine(machine, eventId, { kind: "tool_call", tool_name: "Bash",
  tool_calls: [{ toolUseId: tool, args: { command: "gh pr create" } }], record: { payload: { tool_use_id: tool } } });
const directResult = (machine: string, eventId: string, tool: string, n: number) => directLine(machine, eventId, { kind: "session_event",
  type: "tool_result", content: `https://github.com/acme/repo/pull/${n}`, record: { payload: { tool_use_id: tool, kind: "tool_result" } } });

test("two manifests with the same direct event ids: only the winning machine's copy contributes", () => {
  const records = [directCall("m1", "e1", "t1"), directResult("m1", "e2", "t1", 5), directCall("m2", "e1", "t2"), directResult("m2", "e2", "t2", 9)];
  const mask: HistoryMask = new Map([
    ["e1", { originId: "machine:m1", operation: "upsert" as const, revision: "1" }],
    ["e2", { originId: "machine:m1", operation: "upsert" as const, revision: "1" }],
  ]);
  expect(maskRecords(records, mask).map((r) => r.origin_id)).toEqual(["machine:m1", "machine:m1"]);
  expect(numbers(maskedProjection({ records }, scope, mask))).toEqual(["https://github.com/acme/repo/pull/5"]);
});

test("direct/delivery projected-key collision (machine_id = deliveryMachine(W, O)) cannot re-admit masked-out delivery evidence", () => {
  const O = "origin-o", M = deliveryMachine(scope.workspaceId, O);
  // Delivery records r1/r2 from O carry PR #9; the real projection gives them (machine_id=M, event_id=M:r1/M:r2).
  const r1 = rec("tool_call", { name: "Bash", tool_use_id: "t9", args_json: JSON.stringify({ command: "gh pr create" }), is_error: 0, ts_ms: T }, { origin_id: O, record_id: "r1" });
  const r2 = rec("session_event", { role: "tool_result", kind: "tool_result", tool_use_id: "t9", text: "https://github.com/acme/repo/pull/9", ts_ms: T + 1 }, { origin_id: O, record_id: "r2" });
  const readerRows = projectRelayhistorySession([r1, r2], { ...scope, machineId: "m", userId: "u" }).rows;
  expect(readerRows.map((r) => [r.row.machine_id, r.row.event_id])).toEqual([[M, `${M}:r1`], [M, `${M}:r2`]]);
  // Winning DIRECT lines from machine:M with record ids M:r1/M:r2 (a different, PR-free tool call).
  const d1 = directCall(M, `${M}:r1`, "tA"), d2 = directLine(M, `${M}:r2`, { kind: "session_event", type: "tool_result",
    content: "no pull request here", record: { payload: { tool_use_id: "tA", kind: "tool_result" } } });
  const records = normalizeRelayhistoryRecords([r1, r2]).concat([d1, d2]);
  const mask: HistoryMask = new Map([
    [`${M}:r1`, { originId: `machine:${M}`, operation: "upsert" as const, revision: "1" }],
    [`${M}:r2`, { originId: `machine:${M}`, operation: "upsert" as const, revision: "1" }],
  ]);
  // The reader's rows collide on (machine:M, event id) with the admitted direct copies; reconstructing
  // admission from those keys (f1672c6b) admitted the delivery rows and leaked PR #9.
  const leakedByKey = readerRows.filter((r) => mask.has(String(r.row.event_id)) && `machine:${r.row.machine_id}` === `machine:${M}`);
  expect(numbers(leakedByKey as PrEvidenceProjectionRow[])).toEqual(["https://github.com/acme/repo/pull/9"]);
  expect(numbers(maskedProjection({ records }, scope, mask))).toEqual([]);
});

test("all four admitted with matching canonical identities: the reader's winner rule keeps only the delivery rows (PR #9)", () => {
  const O = "origin-o", M = deliveryMachine(scope.workspaceId, O);
  const r1 = rec("tool_call", { name: "Bash", tool_use_id: "t9", args_json: JSON.stringify({ command: "gh pr create" }), is_error: 0, ts_ms: T }, { origin_id: O, record_id: "r1" });
  const r2 = rec("session_event", { role: "tool_result", kind: "tool_result", tool_use_id: "t9", text: "https://github.com/acme/repo/pull/9", ts_ms: T + 1 }, { origin_id: O, record_id: "r2" });
  // Direct lines with the SAME (machine_id, source, kind, event_id) as the delivery rows, carrying PR #5.
  const d1 = directCall(M, `${M}:r1`, "t5"), d2 = directResult(M, `${M}:r2`, "t5", 5);
  const delivery = normalizeRelayhistoryRecords([r1, r2]);
  const projected = projectRelayhistorySession(delivery, { ...scope, machineId: "m", userId: "u" }).rows;
  expect(projected.map((r) => [r.row.kind, r.row.event_id])).toEqual([["tool_call", `${M}:r1`], ["session_event", `${M}:r2`]]);
  const records = [...delivery, d1, d2];
  const mask: HistoryMask = new Map([["r1", null], ["r2", null], [`${M}:r1`, null], [`${M}:r2`, null]]);
  expect(maskRecords(records, mask)).toHaveLength(4);
  // cf225368 concatenated both classes and emitted #9 AND #5; the reader (and Neon's one-row PK) yields only #9.
  expect(numbers(maskedProjection({ records }, scope, mask))).toEqual(["https://github.com/acme/repo/pull/9"]);
});
