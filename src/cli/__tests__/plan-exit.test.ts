import { readFileSync } from "node:fs";
import { describe, expect, it } from "bun:test";
import { planChildExitCode, planChildMustFailClosed } from "../plan-exit.ts";

describe("gcp plan exit", () => {
  it("exits non-zero when the child fails and allowFail is not set", () => {
    expect(planChildMustFailClosed(false, 2)).toBe(true);
    expect(planChildMustFailClosed(undefined, 2)).toBe(true);
    expect(planChildMustFailClosed(false, null)).toBe(true);
    expect(planChildExitCode(2)).toBe(2);
    expect(planChildExitCode(null)).toBe(1);
    expect(planChildMustFailClosed(false, 0)).toBe(false);
    expect(planChildMustFailClosed(true, 2)).toBe(false);
    expect(planChildMustFailClosed(true, null)).toBe(false);
  });

  it("keeps the gcp plan call fail-closed and leaves the aws plan swallow in place", () => {
    const cli = readFileSync(new URL("../main.ts", import.meta.url), "utf8");
    const plan = cli.slice(cli.indexOf('case "plan"'), cli.indexOf('case "seed"'));
    const gcpAt = plan.indexOf('startsWith("gcp-")');
    const awsAt = plan.indexOf('startsWith("aws-")');
    const localAt = plan.indexOf("requireLocalStage(stageArg)");
    expect(gcpAt).toBeGreaterThan(awsAt);
    expect(localAt).toBeGreaterThan(gcpAt);
    const gcp = plan.slice(gcpAt, localAt);
    const aws = plan.slice(awsAt, gcpAt);
    expect(gcp).toContain("stacks/gcp/alchemy.gcp.run.ts");
    expect(gcp).toContain("allowFail: false");
    expect(gcp).not.toContain("allowFail: true");
    expect(aws).toContain("allowFail: true");
    expect(aws).toContain("stacks/aws/alchemy.aws.run.ts");
  });
});
