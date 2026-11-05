import { describe, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { HISTORY_APPLY, historyApplyArgs, validateHistoryApproval } from "../../../scripts/history-owner-apply.ts";
import { OWNER_APPLY } from "../../hoopdb/owner-operator-apply.ts";
import { APPROVER } from "../../../stacks/aws/deploy-guard.ts";

const SHA = "a".repeat(40), GEN = "b".repeat(64), LEDGER = "c".repeat(64);
const now = Date.parse("2026-11-05T12:00:00Z");
const digest = (t: string) => createHash("sha256").update(t).digest("hex");
const planText = (over: Record<string, unknown> = {}) => JSON.stringify({ version: 1, kind: HISTORY_APPLY.protocol, status: "planned_readonly_not_apply_approval",
  at: "2026-11-05T11:50:00Z", project: OWNER_APPLY.project, sourceSha: SHA, generation: GEN, ledgerSha256: LEDGER, inventory: [], ...over });
const approvalFor = (plan: string, over: Record<string, unknown> = {}) => JSON.stringify({ version: 1, decision: "approved", by: APPROVER,
  execution: HISTORY_APPLY.execution, quote: "everything is pre approved", project: OWNER_APPLY.project, sourceSha: SHA, generation: GEN,
  ledgerSha256: LEDGER, planSha256: digest(plan), at: "2026-11-05T11:55:00Z", ...over });

describe("history owner apply guards", () => {
  test("a fresh approval bound to the exact plan passes", () => {
    const plan = planText();
    expect(() => validateHistoryApproval(plan, approvalFor(plan), SHA, now)).not.toThrow();
  });
  test("refuses: other plan bytes, other source, stale plan, missing quote, wrong approver, approval before plan", () => {
    const plan = planText();
    const cases: [string, string, string][] = [
      [plan, approvalFor(planText({ at: "2026-11-05T11:51:00Z" })), "history_approval_required"],
      [plan, approvalFor(plan), "history_plan_binding"],
      [planText({ at: "2026-11-05T10:00:00Z" }), approvalFor(planText({ at: "2026-11-05T10:00:00Z" })), "history_artifact_stale"],
      [plan, approvalFor(plan, { quote: "" }), "history_approval_required"],
      [plan, approvalFor(plan, { by: "someone else" }), "history_approval_required"],
      [plan, approvalFor(plan, { at: "2026-11-05T11:40:00Z" }), "history_approval_predates_plan"],
    ];
    for (const [i, [p, a, code]] of cases.entries()) {
      const source = i === 1 ? "d".repeat(40) : SHA;
      expect(() => validateHistoryApproval(p, a, source, now)).toThrow(code);
    }
  });
  test("arguments are exact", () => {
    expect(historyApplyArgs(["plan", "--source-sha", SHA])).toEqual({ mode: "plan", sourceSha: SHA });
    expect(() => historyApplyArgs(["plan", "--source-sha", "nope"])).toThrow("history_apply_args");
    expect(() => historyApplyArgs(["apply", "--source-sha", SHA])).toThrow("history_apply_args");
  });
});
