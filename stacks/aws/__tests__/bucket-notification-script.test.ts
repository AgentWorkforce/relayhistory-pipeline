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

  test("the upload-inbox rule is owned too (both Ids go in one put)", () => {
    expect(foreign({ ...empty, QueueConfigurations: [{ Id: "96bulls-upload-inbox", QueueArn: "arn:u" }] })).toBe("0");
    expect(foreign({ ...empty, QueueConfigurations: [{ Id: "96bulls-session-pipeline-manifests", QueueArn: "arn:q" }, { Id: "96bulls-upload-inbox", QueueArn: "arn:u" }, { Id: "other", QueueArn: "arn:o" }] })).toBe("1");
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

describe("sessions-prod-bucket-notification.sh upload-inbox flag", () => {
  const dropExpr = (() => {
    const m = script.match(/upload_live="\$\(live \| jq '([^']+)'\)"/);
    if (!m) throw new Error("upload-inbox drop-guard jq expression not found in the script");
    return m[1]!;
  })();
  const live = (q: object[]) => {
    const r = Bun.spawnSync(["jq", dropExpr], { stdin: new TextEncoder().encode(JSON.stringify({ ...empty, QueueConfigurations: q })) });
    if (r.exitCode !== 0) throw new Error(`jq failed: ${r.stderr.toString()}`);
    return r.stdout.toString().trim();
  };

  test("the drop guard sees a live upload-inbox rule, and only that", () => {
    expect(live([])).toBe("0");
    expect(live([{ Id: "96bulls-session-pipeline-manifests" }])).toBe("0");
    expect(live([{ Id: "96bulls-session-pipeline-manifests" }, { Id: "96bulls-upload-inbox" }])).toBe("1");
  });

  test("the drop guard runs only for --apply with the flag off, and has an explicit override", () => {
    expect(script).toContain(`if [ "$mode" = "--apply" ] && [ "$UPLOAD_DIRECT" = "off" ] && [ "\${NINETY_SIX_BULLS_CONFIRM_DROP_UPLOAD_INBOX:-}" != "yes" ]; then`);
  });

  test("print renders one rule off and both rules on, and refuses a bad flag value (no AWS call)", () => {
    const run = (flag?: string) => Bun.spawnSync(["bash", join(import.meta.dir, "../../../scripts/sessions-prod-bucket-notification.sh"), "--print"], {
      env: { PATH: process.env.PATH!, SESSIONS_PROD_ACCOUNT_ID: "111111111111", ...(flag !== undefined ? { HISTORY_UPLOAD_DIRECT_INFRA: flag } : {}) },
    });
    const ids = (flag?: string) => JSON.parse(run(flag).stdout.toString()).QueueConfigurations.map((q: { Id: string }) => q.Id);
    expect(ids()).toEqual(["96bulls-session-pipeline-manifests"]);
    expect(ids("off")).toEqual(["96bulls-session-pipeline-manifests"]);
    expect(ids("on")).toEqual(["96bulls-session-pipeline-manifests", "96bulls-upload-inbox"]);
    const bad = run("yes");
    expect(bad.exitCode).toBe(2);
    expect(bad.stderr.toString()).toContain("HISTORY_UPLOAD_DIRECT_INFRA must be on or off");
  });
});
