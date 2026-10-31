import { describe, expect, it } from "bun:test";
import { mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { snapshotContents, snapshotStateLayer, type StateSnapshot } from "../state-snapshot.ts";

const snap: StateSnapshot = {
  schema: 1,
  stack: "96bulls-aws",
  stage: "aws-sessions-dev",
  bucket: "b",
  prefix: "96bulls/96bulls-aws/aws-sessions-dev/",
  objects: [
    { key: "96bulls/96bulls-aws/aws-sessions-dev/locker.json", versionId: "v1", text: JSON.stringify({ status: "created", fqn: "locker", logicalId: "locker" }) },
    { key: "96bulls/96bulls-aws/aws-sessions-dev/network__Vpc.json", versionId: "v2", text: JSON.stringify({ status: "created", fqn: "network/Vpc", logicalId: "Vpc" }) },
    { key: "96bulls/96bulls-aws/aws-sessions-dev/__stack_output__.json", versionId: "v3", text: JSON.stringify({ stage: "aws-sessions-dev" }) },
  ],
};

describe("plans run against a read-only state snapshot", () => {
  it("decodes each object by its fqn, and the stack output separately", () => {
    const [resources, outputs] = snapshotContents(snap) as unknown as [Record<string, Record<string, Record<string, { fqn: string }>>>, Record<string, Record<string, unknown>>];
    expect(Object.keys(resources["96bulls-aws"]!["aws-sessions-dev"]!).sort()).toEqual(["locker", "network/Vpc"]);
    expect(outputs["96bulls-aws"]!["aws-sessions-dev"]).toEqual({ stage: "aws-sessions-dev" });
  });

  it("refuses to load for anything but `alchemy plan`", () => {
    const f = join(mkdtempSync(join(process.env.TMPDIR ?? tmpdir(), "snap-")), "s.json");
    writeFileSync(f, JSON.stringify(snap));
    expect(() => snapshotStateLayer(f, ["bun", "alchemy", "deploy"])).toThrow(/is for `alchemy plan` only/);
    expect(() => snapshotStateLayer(f, ["bun", "alchemy", "destroy"])).toThrow(/is for `alchemy plan` only/);
    expect(() => snapshotStateLayer(f, ["bun", "alchemy", "plan"])).not.toThrow();
  });

  it("the stack uses the snapshot only when asked, and refuses it for deploy or destroy", () => {
    const stack = readFileSync(fileURLToPath(new URL("../alchemy.aws.run.ts", import.meta.url)), "utf8");
    expect(stack).toContain("? snapshotStateLayer(process.env.NINETY_SIX_BULLS_STATE_SNAPSHOT)\n      : AWS.state({ prefix: \"96bulls\" }),");
    expect(stack).toContain('if (process.env.NINETY_SIX_BULLS_STATE_SNAPSHOT && mode !== "plan")');
    const io = readFileSync(fileURLToPath(new URL("../../../src/cli/aws-provenance-io.ts", import.meta.url)), "utf8");
    expect(io).toContain("NINETY_SIX_BULLS_STATE_SNAPSHOT: snapPath");
  });
});
