import { describe, expect, it } from "bun:test";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { assertApproval, assertAwsDeploymentTarget, assertClearance, awsMode, findPlaceholders, parseCalendarDate, type Approval } from "../deploy-guard.ts";

const NOW = new Date("2026-10-30T18:00:00Z");
const approval: Approval = {
  by: "Khaliq Gant",
  date: "2026-10-30",
  operations: ["deploy"],
  stage: "aws-proof",
  accountId: "500268966557",
  region: "us-west-2",
  words: "approved applying stage A (aws-proof, create-only)",
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

  it("refuses the review's repro: negative wording on an impossible date", () => {
    expect(deploy({ words: "Khaliq Gant said DO NOT deploy this stage on 2026-99-99" })).toThrow(/countermand/);
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
    expect(deploy({ words: "" })).toThrow(/verbatim/);
    for (const w of ["hold off on this", "wait until Monday", "not yet, sorry", "please don't apply", "never deploy", "approval revoked"]) expect(deploy({ words: w })).toThrow(/countermand/);
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
