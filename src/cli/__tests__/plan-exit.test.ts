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

  it("keeps the gcp plan call fail-closed, and routes aws plan/up/down through the fail-closed aws runner", () => {
    const cli = readFileSync(new URL("../main.ts", import.meta.url), "utf8");
    const plan = cli.slice(cli.indexOf('case "plan"'), cli.indexOf('case "seed"'));
    const gcpAt = plan.indexOf('startsWith("gcp-")');
    const localAt = plan.indexOf("requireLocalStage(stageArg)");
    expect(localAt).toBeGreaterThan(gcpAt);
    const gcp = plan.slice(gcpAt, localAt);
    expect(gcp).toContain("stacks/gcp/alchemy.gcp.run.ts");
    expect(gcp).toContain("allowFail: false");
    expect(gcp).not.toContain("allowFail: true");
    // AWS: dispatched before the switch; the runner never sets allowFail.
    expect(cli).toContain('if (stageArg?.startsWith("aws-") && (command === "plan" || command === "up" || command === "down")) return await aws(command, stageArg);');
    const runner = cli.slice(cli.indexOf("function aws("), cli.indexOf("async function main()"));
    expect(runner).toContain("sh(\"bunx\", inv.args, { env: inv.env });");
    expect(runner).not.toContain("allowFail");
    expect(plan).not.toContain("stacks/aws/alchemy.aws.run.ts");
  });
});
