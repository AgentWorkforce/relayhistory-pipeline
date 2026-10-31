import { describe, expect, it } from "bun:test";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { verifyDeployCapture, type CaptureIO } from "../capture-verify.ts";
import { assertApprovalBinding, inputsDigest } from "../deploy-guard.ts";
import { versionsDigest } from "../../../src/cli/plan-provenance.ts";

/**
 * B1: a deploy, including a direct `alchemy deploy` (the stack runs this before
 * any resource), applies exactly the reviewed capture. Each regression keeps
 * the SAME approval quote and capture object, so the approval binding alone
 * passes; only the live checks catch the difference.
 */
const h = (s: string) => createHash("sha256").update(s).digest("hex");
const ROOT = "/repo";
const STAGE = "aws-proof";
const C = "c".repeat(40);
const LS_TREE = "100644 blob aaa\tstacks/aws/eks.ts\n";
const LISTING = { Versions: [{ Key: "96bulls/96bulls-aws/aws-proof/cluster.json", VersionId: "v1", IsLatest: true, ETag: "e1" }], DeleteMarkers: [] };
const inputsText = JSON.stringify({ accountId: "500268966557", region: "us-west-2", eks: { imageDigest: "d" }, approval: { decision: "approved", quote: "approved applying stage A" } });
const planText = "Plan: 3 to update\n";
const detailedText = "Plan: 3 to update\n  [96bulls] update\n";
const provenanceText = JSON.stringify({
  schema: 1,
  stage: STAGE,
  target: { accountId: "500268966557", region: "us-west-2", callerArn: "arn" },
  source: { commit: C, stackSourceSha256: h(LS_TREE), clean: true },
  inputs: { sha256: inputsDigest(inputsText) },
  state: { versioning: "Enabled", versionsDigest: versionsDigest(LISTING).digest, versionEntries: versionsDigest(LISTING).entries },
  outputs: { plan: { sha256: h(planText) }, detailed: { sha256: h(detailedText) } },
});
const capture = { provenanceSha256: h(provenanceText), planSha256: h(planText), inputsSha256: inputsDigest(inputsText), sourceCommit: C };

interface World { head: string; descends: boolean; changed: string[]; lsTree: string; listing: typeof LISTING | Record<string, unknown>; files: Record<string, string>; dirty?: boolean }
const world = (w: Partial<World> = {}): World => ({
  head: C,
  descends: true,
  changed: [],
  lsTree: LS_TREE,
  listing: LISTING,
  files: { "docs/plans/aws-proof.provenance.json": provenanceText, "docs/plans/aws-proof.plan.txt": planText, "docs/plans/aws-proof.plan.detailed.txt": detailedText },
  ...w,
});
const io = (w: World): CaptureIO => ({
  root: ROOT,
  readFile: (p) => w.files[p.slice(ROOT.length + 1)],
  run: (cmd, args) => {
    const a = args.join(" ");
    if (cmd === "git") {
      if (a.startsWith("status --porcelain")) return w.dirty ? " M stacks/aws/eks.ts\n" : "";
      if (a === "rev-parse HEAD") return `${w.head}\n`;
      if (a.startsWith("merge-base --is-ancestor")) { if (!w.descends) throw new Error("exit 1"); return ""; }
      if (a.startsWith("diff --name-only")) return w.changed.join("\n");
      if (a.startsWith("ls-tree -r HEAD")) return w.lsTree;
    }
    if (cmd === "aws") {
      if (a.startsWith("sts get-caller-identity")) return JSON.stringify({ Account: "500268966557" });
      if (a.startsWith("s3api get-bucket-versioning")) return JSON.stringify({ Status: "Enabled" });
      if (a.startsWith("s3api list-object-versions")) return JSON.stringify(w.listing);
    }
    throw new Error(`unexpected ${cmd} ${a}`);
  },
});
const deploy = (w: World) => () => verifyDeployCapture(io(w), STAGE, inputsText);
const bindingStillPasses = (w: World) => {
  // The approval (same quote, same capture object) matches the committed files in every case.
  expect(() => assertApprovalBinding(capture, { provenanceText: w.files["docs/plans/aws-proof.provenance.json"]!, planText: w.files["docs/plans/aws-proof.plan.txt"]!, inputsText })).not.toThrow();
};

describe("every deploy applies the reviewed capture (B1)", () => {
  it("passes at the capture commit, and at a descendant that only commits the capture files", () => {
    expect(deploy(world())).not.toThrow();
    expect(deploy(world({ head: "e".repeat(40), changed: ["docs/plans/aws-proof.provenance.json", "docs/plans/aws-proof.plan.txt"] }))).not.toThrow();
  });

  it("regression 1: changed source / current commit with unchanged capture files is refused", () => {
    const changedSource = world({ head: "e".repeat(40), changed: ["stacks/aws/eks.ts"], lsTree: "100644 blob bbb\tstacks/aws/eks.ts\n" });
    bindingStillPasses(changedSource);
    expect(deploy(changedSource)).toThrow(/source commit: e{40} changes 1 file\(s\)[\s\S]*stack source/);
    // Same tree, other commit content outside the capture files: still refused on the commit.
    expect(deploy(world({ head: "e".repeat(40), changed: ["README.md"] }))).toThrow(/source commit/);
    expect(deploy(world({ head: "f".repeat(40), descends: false }))).toThrow(/does not descend/);
    expect(deploy(world({ dirty: true }))).toThrow(/uncommitted changes/);
  });

  it("regression 2: changed live state digest with unchanged capture files is refused", () => {
    const moved = world({ listing: { Versions: [...LISTING.Versions, { Key: "96bulls/96bulls-aws/aws-proof/worker-role.json", VersionId: "v2", IsLatest: true, ETag: "e2" }], DeleteMarkers: [] } });
    bindingStillPasses(moved);
    expect(deploy(moved)).toThrow(/state digest/);
  });

  it("regression 3: a changed detailed-plan file is refused", () => {
    const w = world();
    w.files["docs/plans/aws-proof.plan.detailed.txt"] = detailedText + "  [worker-role] update\n";
    bindingStillPasses(w);
    expect(deploy(w)).toThrow(/detailed plan file/);
  });

  it("regression 4: changed inputs are refused, while the lease taken by the deploy itself is not state", () => {
    expect(() => verifyDeployCapture(io(world()), STAGE, JSON.stringify({ ...JSON.parse(inputsText), eks: { imageDigest: "x" } }))).toThrow(/inputs/);
    const withLease = world({ listing: { Versions: [...LISTING.Versions, { Key: "96bulls/96bulls-aws/aws-proof/__lease__.json", VersionId: "l1", IsLatest: true, ETag: "x" }], DeleteMarkers: [] } });
    expect(deploy(withLease)).not.toThrow();
  });

  it("the stack runs it on every deploy, before any resource, not only bin/96bulls", () => {
    const stack = readFileSync(fileURLToPath(new URL("../alchemy.aws.run.ts", import.meta.url)), "utf8");
    const at = stack.indexOf("verifyDeployCapture(\n");
    expect(at).toBeGreaterThan(0);
    expect(stack.slice(stack.lastIndexOf('if (mode === "deploy") {', at), at)).toContain("assertApprovalBinding(");
    expect(at).toBeLessThan(stack.indexOf("yield* AWS.KMS.Key("));
  });
});
