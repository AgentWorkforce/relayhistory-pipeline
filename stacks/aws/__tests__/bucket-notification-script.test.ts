import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";

// The --apply/--remove guard in scripts/sessions-prod-bucket-notification.sh counts notification
// targets the script does not own (put replaces the whole configuration). Run the script's own jq
// expression, extracted from the file, against the shapes `live()` produces.
const script = readFileSync(join(import.meta.dir, "../../../scripts/sessions-prod-bucket-notification.sh"), "utf8");
const foreignExpr = (() => {
  const m = script.match(/foreign="\$\(live \| jq '([^']+)'\)"/);
  if (!m) throw new Error("foreign-target jq expression not found in the script");
  return m[1];
})();

const foreign = (live: object): string => {
  const r = Bun.spawnSync(["jq", foreignExpr], { stdin: new TextEncoder().encode(JSON.stringify(live)) });
  if (r.exitCode !== 0) throw new Error(`jq failed: ${r.stderr.toString()}`);
  return r.stdout.toString().trim();
};

const empty = { QueueConfigurations: [], TopicConfigurations: [], LambdaFunctionConfigurations: [], EventBridgeConfiguration: null };

describe("sessions-prod-bucket-notification.sh foreign-target guard", () => {
  test("an unconfigured bucket has no foreign targets", () => {
    expect(foreign(empty)).toBe("0");
  });

  test("the script's own queue target is not foreign", () => {
    expect(foreign({ ...empty, QueueConfigurations: [{ Id: "96bulls-session-pipeline-manifests", QueueArn: "arn:q" }] })).toBe("0");
  });

  test("another queue, topic or Lambda target is foreign", () => {
    expect(foreign({ ...empty, QueueConfigurations: [{ Id: "other", QueueArn: "arn:q" }] })).toBe("1");
    expect(foreign({ ...empty, TopicConfigurations: [{ Id: "t" }] })).toBe("1");
    expect(foreign({ ...empty, LambdaFunctionConfigurations: [{ Id: "l" }] })).toBe("1");
  });

  test("EventBridge delivery counts as a foreign target", () => {
    expect(foreign({ ...empty, EventBridgeConfiguration: {} })).toBe("1");
    expect(foreign({ ...empty, TopicConfigurations: [{ Id: "t" }], EventBridgeConfiguration: {} })).toBe("2");
  });
});
