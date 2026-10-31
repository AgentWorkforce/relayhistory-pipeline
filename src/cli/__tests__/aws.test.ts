import { describe, expect, test } from "bun:test";
import { awsInvocation } from "../aws.ts";

const base = { NINETY_SIX_BULLS_CONFIRM_ACCOUNT: "500268966557", AWS_REGION: "us-west-2", AWS_PROFILE: "sessions-dev" };

describe("96bulls plan|up|down aws-*", () => {
  test("plan is read-only Alchemy plan in plan mode", () => {
    expect(awsInvocation("plan", "aws-proof", base)).toEqual({
      args: ["alchemy", "plan", "--config", "stacks/aws/alchemy.aws.run.ts", "--stage", "aws-proof", "--no-input"],
      env: { ALCHEMY_PLAIN: "1", NINETY_SIX_BULLS_AWS_MODE: "plan" },
    });
  });

  test("up deploys only with the typed stage", () => {
    expect(() => awsInvocation("up", "aws-proof", base)).toThrow(/CONFIRM_STAGE=aws-proof/);
    expect(() => awsInvocation("up", "aws-proof", { ...base, NINETY_SIX_BULLS_CONFIRM_STAGE: "aws-sessions-dev" })).toThrow(/CONFIRM_STAGE/);
    expect(awsInvocation("up", "aws-proof", { ...base, NINETY_SIX_BULLS_CONFIRM_STAGE: "aws-proof" }).args).toEqual(["alchemy", "deploy", "--config", "stacks/aws/alchemy.aws.run.ts", "--stage", "aws-proof", "--no-input", "--yes"]);
  });

  test("down destroys only throwaway stages, with the typed stage", () => {
    expect(() => awsInvocation("down", "aws-sessions-dev", { ...base, NINETY_SIX_BULLS_CONFIRM_DESTROY: "aws-sessions-dev" })).toThrow(/not destroyed/);
    expect(() => awsInvocation("down", "aws-prod", { ...base, NINETY_SIX_BULLS_CONFIRM_DESTROY: "aws-prod" })).toThrow(/not destroyed/);
    expect(() => awsInvocation("down", "aws-proof", base)).toThrow(/CONFIRM_DESTROY=aws-proof/);
    expect(awsInvocation("down", "aws-proof", { ...base, NINETY_SIX_BULLS_CONFIRM_DESTROY: "aws-proof" })).toEqual({
      args: ["alchemy", "destroy", "--config", "stacks/aws/alchemy.aws.run.ts", "--stage", "aws-proof", "--no-input", "--yes"],
      env: { ALCHEMY_PLAIN: "1", NINETY_SIX_BULLS_AWS_MODE: "destroy" },
    });
  });

  test("refuses without the account confirmation or an explicit region", () => {
    expect(() => awsInvocation("plan", "aws-proof", { AWS_REGION: "us-west-2" })).toThrow(/CONFIRM_ACCOUNT/);
    expect(() => awsInvocation("plan", "aws-proof", { NINETY_SIX_BULLS_CONFIRM_ACCOUNT: "500268966557" })).toThrow(/AWS_REGION/);
  });
});

describe("plan --detailed", () => {
  test("is forwarded to alchemy plan (the documented detailed command really runs)", () => {
    expect(awsInvocation("plan", "aws-proof", base, { detailed: true }).args).toEqual(["alchemy", "plan", "--config", "stacks/aws/alchemy.aws.run.ts", "--stage", "aws-proof", "--no-input", "--detailed"]);
  });
  test("up verifies the captured plan before deploying", () => {
    const cli = require("node:fs").readFileSync(new URL("../main.ts", import.meta.url), "utf8") as string;
    const runner = cli.slice(cli.indexOf("async function aws("), cli.indexOf("async function main()"));
    expect(runner.indexOf("verifyCapturedPlan(ROOT, stage)")).toBeGreaterThan(0);
    expect(runner.indexOf("verifyCapturedPlan(ROOT, stage)")).toBeLessThan(runner.indexOf('sh("bunx", inv.args'));
  });
});
