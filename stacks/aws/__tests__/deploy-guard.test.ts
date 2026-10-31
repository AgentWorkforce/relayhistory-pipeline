import { describe, expect, it } from "bun:test";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { assertApproval, assertAwsDeploymentTarget, assertClearance, awsMode, findPlaceholders, parseCalendarDate, type Approval } from "../deploy-guard.ts";

const NOW = new Date("2026-10-30T18:00:00Z");
const approval: Approval = {
  decision: "approved",
  by: "Khaliq Gant",
  date: "2026-10-30",
  operations: ["deploy"],
  stage: "aws-proof",
  accountId: "500268966557",
  region: "us-west-2",
  quote: "approved applying stage A (aws-proof, create-only)",
};
const inputs = { accountId: "500268966557", region: "us-west-2", approval };
const env = { NINETY_SIX_BULLS_CONFIRM_ACCOUNT: "500268966557", NINETY_SIX_BULLS_CONFIRM_STAGE: "aws-proof" };
const deploy = (a: Partial<Approval>, e: Record<string, string> = env) => () => assertClearance("deploy", "aws-proof", { ...inputs, approval: { ...approval, ...a } }, e, NOW);

describe("AWS deploy guards", () => {
  it("refuses a caller in another account or a provider in another region", () => {
    expect(() => assertAwsDeploymentTarget({ accountId: "500268966557", region: "us-west-2" }, { accountId: "500268966557", region: "us-west-2" })).not.toThrow();
    expect(() => assertAwsDeploymentTarget({ accountId: "500268966557", region: "us-west-2" }, { accountId: "111122223333", region: "us-west-2" })).toThrow(/STS caller is account 111122223333/);
    expect(() => assertAwsDeploymentTarget({ accountId: "500268966557", region: "us-west-2" }, { accountId: "500268966557", region: "us-east-1" })).toThrow(/region is us-east-1/);
  });

  it("binds the live STS account and region before the first resource", () => {
    const stack = readFileSync(fileURLToPath(new URL("../alchemy.aws.run.ts", import.meta.url)), "utf8");
    const guard = stack.indexOf("assertAwsDeploymentTarget(");
    expect(guard).toBeGreaterThan(0);
    expect(guard).toBeLessThan(stack.indexOf("yield* AWS.KMS.Key("));
  });

  it("finds placeholders anywhere, $comment included", () => {
    expect(findPlaceholders({ $comment: "<x>", a: "<x>", b: ["ok", "TODO"], c: { d: "REPLACE_ME" } })).toEqual(["$.$comment", "$.a", "$.b[1]", "$.c.d"]);
  });

  it("accepts only real calendar dates", () => {
    expect(parseCalendarDate("2026-10-30")?.toISOString()).toBe("2026-10-30T00:00:00.000Z");
    for (const bad of ["2026-99-99", "2026-02-30", "2026-13-01", "2026-00-10", "2026-1-1", "30/10/2026", ""]) expect(parseCalendarDate(bad)).toBeUndefined();
  });

  it("accepts an exact, current approval record for this operation and target", () => {
    expect(deploy({})).not.toThrow();
  });

  it("refuses unless decision is exactly \"approved\", whatever the quote says", () => {
    for (const [decision, quote] of [
      ["refused", "I refuse to approve this deployment"],
      ["denied", "deployment is denied"],
      ["unauthorized", "deployment is unauthorized"],
      ["no", "absolutely no deployment"],
      ["Approved", "go ahead"],
      ["approved ", "go ahead"],
      [undefined, "go ahead and deploy"],
    ] as const)
      expect(deploy({ decision: decision as string, quote })).toThrow(/approval.decision is .*not "approved"/);
    // The quote is never parsed: approval comes only from the decision field.
    expect(deploy({ quote: "I refuse to approve this deployment" })).not.toThrow();
    expect(deploy({ date: "2026-99-99" })).toThrow(/not a real YYYY-MM-DD date/);
  });

  it("refuses every mismatch in the record", () => {
    expect(deploy({ by: "Someone Else" })).toThrow(/approval.by/);
    expect(deploy({ date: "2026-11-01" })).toThrow(/in the future/);
    expect(deploy({ date: "2026-08-01" })).toThrow(/older than 30 days/);
    expect(deploy({ operations: ["destroy"] })).toThrow(/does not include "deploy"/);
    expect(deploy({ stage: "aws-sessions-dev" })).toThrow(/approval.stage/);
    expect(deploy({ accountId: "111122223333" })).toThrow(/accountId\/region/);
    expect(deploy({ region: "us-east-1" })).toThrow(/accountId\/region/);
    expect(deploy({ quote: "" })).toThrow(/verbatim/);
    expect(() => assertApproval("deploy", "aws-proof", { accountId: "500268966557", region: "us-west-2" }, NOW)).toThrow(/no `approval` record/);
    expect(deploy({ date: "<YYYY-MM-DD of the approval>" })).toThrow(/placeholder/);
  });

  it("needs the typed account and stage; plan needs neither the record nor the stage", () => {
    expect(deploy({}, { NINETY_SIX_BULLS_CONFIRM_ACCOUNT: "500268966557" })).toThrow(/CONFIRM_STAGE=aws-proof/);
    expect(deploy({}, { NINETY_SIX_BULLS_CONFIRM_STAGE: "aws-proof" })).toThrow(/CONFIRM_ACCOUNT/);
    expect(() => assertClearance("plan", "aws-proof", { accountId: "500268966557", region: "us-west-2" }, { NINETY_SIX_BULLS_CONFIRM_ACCOUNT: "500268966557" }, NOW)).not.toThrow();
  });

  it("refuses destroy of sessions-dev and production inside the stack, and needs the typed stage and a destroy approval", () => {
    const destroyEnv = { NINETY_SIX_BULLS_CONFIRM_ACCOUNT: "500268966557", NINETY_SIX_BULLS_CONFIRM_DESTROY: "aws-sessions-dev" };
    expect(() => assertClearance("destroy", "aws-sessions-dev", { ...inputs, approval: { ...approval, stage: "aws-sessions-dev", operations: ["destroy"] } }, destroyEnv, NOW)).toThrow(/never destroyed/);
    expect(() => assertClearance("destroy", "aws-prod", { ...inputs, approval: { ...approval, stage: "aws-prod", operations: ["destroy"] } }, { ...destroyEnv, NINETY_SIX_BULLS_CONFIRM_DESTROY: "aws-prod" }, NOW)).toThrow(/never destroyed/);
    const proof = { ...inputs, approval: { ...approval, operations: ["destroy" as const] } };
    expect(() => assertClearance("destroy", "aws-proof", proof, { NINETY_SIX_BULLS_CONFIRM_ACCOUNT: "500268966557" }, NOW)).toThrow(/CONFIRM_DESTROY=aws-proof/);
    expect(() => assertClearance("destroy", "aws-proof", proof, { NINETY_SIX_BULLS_CONFIRM_ACCOUNT: "500268966557", NINETY_SIX_BULLS_CONFIRM_DESTROY: "aws-proof" }, NOW)).not.toThrow();
    expect(() => assertClearance("destroy", "aws-proof", inputs, { NINETY_SIX_BULLS_CONFIRM_ACCOUNT: "500268966557", NINETY_SIX_BULLS_CONFIRM_DESTROY: "aws-proof" }, NOW)).toThrow(/does not include "destroy"/);
  });

  it("takes the operation from Alchemy's argv, so a direct `alchemy destroy` cannot pass as a deploy", () => {
    const argv = (sub: string) => ["/usr/bin/bun", "/x/node_modules/.bin/alchemy", sub, "--config", "stacks/aws/alchemy.aws.run.ts", "--stage", "aws-proof"];
    expect(() => awsMode({}, argv("destroy"))).toThrow(/needs NINETY_SIX_BULLS_AWS_MODE=destroy/);
    expect(() => awsMode({ NINETY_SIX_BULLS_AWS_MODE: "deploy" }, argv("destroy"))).toThrow(/needs NINETY_SIX_BULLS_AWS_MODE=destroy/);
    expect(awsMode({ NINETY_SIX_BULLS_AWS_MODE: "destroy" }, argv("destroy"))).toBe("destroy");
    expect(awsMode({}, argv("deploy"))).toBe("deploy");
    expect(awsMode({ NINETY_SIX_BULLS_AWS_MODE: "plan" }, argv("plan"))).toBe("plan");
    expect(() => awsMode({ NINETY_SIX_BULLS_AWS_MODE: "plan" }, argv("deploy"))).toThrow(/does not match/);
    expect(() => awsMode({ NINETY_SIX_BULLS_AWS_MODE: "apply" }, argv("deploy"))).toThrow(/must be plan, deploy or destroy/);
    const stack = readFileSync(fileURLToPath(new URL("../alchemy.aws.run.ts", import.meta.url)), "utf8");
    expect(stack).toContain("const mode = awsMode(process.env, process.argv);");
    expect(stack).toContain("assertClearance(mode, stage, inputs, process.env);");
  });
});

describe("a deploy approval is bound to the reviewed capture (B1)", () => {
  const { assertApprovalBinding, inputsDigest } = require("../deploy-guard.ts") as typeof import("../deploy-guard.ts");
  const { createHash } = require("node:crypto") as typeof import("node:crypto");
  const h = (s: string) => createHash("sha256").update(s).digest("hex");
  const inputsText = JSON.stringify({ accountId: "500268966557", region: "us-west-2", eks: { imageDigest: "d" }, approval: { decision: "approved" } });
  const planText = "Plan: 3 to update\n[96bulls] update\n";
  const provenance = (o: { commit?: string; generatedAt?: string; inputs?: string; plan?: string } = {}) =>
    JSON.stringify({ generatedAt: o.generatedAt ?? "2026-10-31T06:00:00Z", source: { commit: o.commit ?? "c".repeat(40) }, inputs: { sha256: o.inputs ?? inputsDigest(inputsText) }, outputs: { plan: { sha256: o.plan ?? h(planText) } } });
  const provenanceText = provenance();
  const capture = { provenanceSha256: h(provenanceText), planSha256: h(planText), inputsSha256: inputsDigest(inputsText), sourceCommit: "c".repeat(40) };
  const ok = { provenanceText, planText, inputsText };

  it("passes only for the exact capture it names", () => {
    expect(() => assertApprovalBinding(capture, ok)).not.toThrow();
    // Editing only the approval inside the inputs does not change what was captured.
    expect(() => assertApprovalBinding(capture, { ...ok, inputsText: JSON.stringify({ ...JSON.parse(inputsText), approval: { decision: "approved", quote: "x" } }) })).not.toThrow();
  });

  it("refuses a recapture with the same quote (new provenance, same plan)", () => {
    expect(() => assertApprovalBinding(capture, { ...ok, provenanceText: provenance({ generatedAt: "2026-10-31T07:00:00Z" }) })).toThrow(/provenanceSha256/);
  });

  it("refuses a changed plan, changed inputs, another commit, or no binding at all", () => {
    expect(() => assertApprovalBinding(capture, { ...ok, planText: planText + "[worker-role] update\n" })).toThrow(/planSha256/);
    const otherInputs = JSON.stringify({ ...JSON.parse(inputsText), eks: { imageDigest: "e" } });
    expect(() => assertApprovalBinding(capture, { ...ok, inputsText: otherInputs })).toThrow(/inputsSha256/);
    const other = provenance({ commit: "d".repeat(40) });
    expect(() => assertApprovalBinding({ ...capture, provenanceSha256: h(other) }, { ...ok, provenanceText: other })).toThrow(/sourceCommit/);
    expect(() => assertApprovalBinding(undefined, ok)).toThrow(/approval.capture is missing/);
  });

  it("refuses a provenance record that describes other inputs or another plan file", () => {
    const stale = provenance({ inputs: "0".repeat(64) });
    expect(() => assertApprovalBinding({ ...capture, provenanceSha256: h(stale) }, { ...ok, provenanceText: stale })).toThrow(/other inputs/);
  });

  it("the stack checks the binding on every deploy, before the account check", () => {
    const stack = readFileSync(fileURLToPath(new URL("../alchemy.aws.run.ts", import.meta.url)), "utf8");
    expect(stack).toContain("assertApprovalBinding(inputs.approval?.capture, { provenanceText, planText, inputsText });");
    expect(stack.indexOf("assertApprovalBinding(inputs.approval")).toBeLessThan(stack.indexOf("assertAwsDeploymentTarget("));
  });
});
