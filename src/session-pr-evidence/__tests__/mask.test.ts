import { expect, test } from "bun:test";
import { deliveryMachine } from "@rhc/lib/pipeline/relayhistory-projection.ts";
import { applyHistoryMask } from "../mask.ts";
import { extractPullRequestEvidence } from "../extract.ts";

const ws = "ws-1";
const call = (id: string, origin: string, record: string) => ({ table: "convergence_events", row: {
  kind: "tool_call", delivery_record_id: record, machine_id: deliveryMachine(ws, origin), event_id: "e" + record, ts: "2026-11-01T00:00:00.000000Z",
  tool_name: "Bash", tool_calls: [{ toolUseId: id, args: { command: "gh pr create" } }], record: { payload: { tool_use_id: id } } } });
const result = (id: string, origin: string, record: string, n: number) => ({ table: "convergence_events", row: {
  kind: "session_event", type: "tool_result", delivery_record_id: record, machine_id: deliveryMachine(ws, origin), event_id: "e" + record,
  ts: "2026-11-01T00:00:01.000000Z", content: `https://github.com/acme/repo/pull/${n}`, record: { payload: { tool_use_id: id, kind: "tool_result" } } } });
const rows = [call("t1", "o1", "r1"), result("t1", "o1", "r2", 1), call("t2", "o1", "moved-1"), result("t2", "o1", "moved-2", 2)];

test("records outside the mask (moved to another scope) never contribute evidence", () => {
  expect(extractPullRequestEvidence(rows).map(r => r.number)).toEqual([1, 2]);
  const mask = new Map([["r1", null], ["r2", null]]);
  expect(extractPullRequestEvidence(applyHistoryMask(rows, ws, mask)).map(r => r.number)).toEqual([1]);
});
test("a delete winner removes the record; a winner from another origin drops this copy", () => {
  expect(applyHistoryMask(rows, ws, new Map([["r1", null], ["r2", { originId: "o1", operation: "delete" as const }]]))).toHaveLength(1);
  expect(applyHistoryMask(rows, ws, new Map([["r1", { originId: "o2", operation: "upsert" as const }], ["r2", null]]))).toHaveLength(1);
  expect(applyHistoryMask(rows, ws, new Map([["r1", { originId: "o1", operation: "upsert" as const }], ["r2", null]]))).toHaveLength(2);
});
test("catalog and commit-link rows are masked by their own record and origin", () => {
  const link = { table: "session_commit_links", row: { record_id: "c1", origin_id: "o1", evidence: { github_pr: { id: "acme/repo#9" } }, received_at: "2026-11-01T00:00:00.000000Z" } };
  expect(extractPullRequestEvidence(applyHistoryMask([link], ws, new Map()))).toEqual([]);
  expect(extractPullRequestEvidence(applyHistoryMask([link], ws, new Map([["c1", null]]))).map(r => r.number)).toEqual([9]);
});
