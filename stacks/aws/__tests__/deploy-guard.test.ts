import { describe, expect, it } from "bun:test";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { assertAwsDeploymentTarget, assertClearance, awsMode, findPlaceholders } from "../deploy-guard.ts";

const ok = { accountId: "500268966557", clearedBy: "Khaliq Gant, 2026-10-30: approved applying stage A (aws-proof, create-only)" };
const env = { NINETY_SIX_BULLS_CONFIRM_ACCOUNT: "500268966557" };

describe("AWS deploy guards", () => {
  it("refuses a caller in another account or a provider in another region", () => {
    expect(() => assertAwsDeploymentTarget({ accountId: "500268966557", region: "us-west-2" }, { accountId: "500268966557", region: "us-west-2" })).not.toThrow();
    expect(() => assertAwsDeploymentTarget({ accountId: "500268966557", region: "us-west-2" }, { accountId: "111122223333", region: "us-west-2" })).toThrow(/STS caller is account 111122223333/);
    expect(() => assertAwsDeploymentTarget({ accountId: "500268966557", region: "us-west-2" }, { accountId: "500268966557", region: "us-east-1" })).toThrow(/region is us-east-1/);
  });

  it("binds the live STS account and region before the first resource", () => {
    const stack = readFileSync(fileURLToPath(new URL("../alchemy.aws.run.ts", import.meta.url)), "utf8");
    const guard = stack.indexOf("assertAwsDeploymentTarget(");
    const firstResource = stack.indexOf("yield* AWS.KMS.Key(");
    expect(guard).toBeGreaterThan(0);
    expect(guard).toBeLessThan(firstResource);
  });

  it("finds placeholders anywhere except $comment", () => {
    expect(findPlaceholders({ $comment: "<ignored>", a: "<x>", b: ["ok", "TODO"], c: { d: "REPLACE_ME" } })).toEqual(["$.a", "$.b[1]", "$.c.d"]);
  });

  it("deploy and destroy need Khaliq's dated words; plan does not; all need the typed account and no placeholders", () => {
    expect(() => assertClearance("deploy", ok, env)).not.toThrow();
    expect(() => assertClearance("destroy", ok, env)).not.toThrow();
    expect(() => assertClearance("deploy", { ...ok, clearedBy: "go ahead" }, env)).toThrow(/Khaliq's words and the date/);
    expect(() => assertClearance("deploy", { ...ok, clearedBy: "<Khaliq's words, verbatim, with the date>" }, env)).toThrow(/placeholder/);
    expect(() => assertClearance("plan", { accountId: ok.accountId }, env)).not.toThrow();
    expect(() => assertClearance("plan", ok, {})).toThrow(/CONFIRM_ACCOUNT/);
  });

  it("treats a direct `alchemy deploy` (no mode set) as deploy", () => {
    expect(awsMode({})).toBe("deploy");
    expect(awsMode({ NINETY_SIX_BULLS_AWS_MODE: "plan" })).toBe("plan");
    expect(() => awsMode({ NINETY_SIX_BULLS_AWS_MODE: "apply" })).toThrow();
  });
});
