import { expect, test } from "bun:test";
import { normalizeRelayhistoryRecords, parseRelayhistoryRecord, type RelayhistoryStreamRecord } from "@rhc/lib/pipeline/relayhistory-transform.ts";
import { projectRelayhistorySession } from "@rhc/lib/pipeline/relayhistory-projection.ts";
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
