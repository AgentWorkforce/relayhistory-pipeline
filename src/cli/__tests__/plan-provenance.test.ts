import { describe, expect, test } from "bun:test";
import { provenanceMismatches, sanitizeInputs, sanitizePlan, stateDigest, type Observed, type Provenance } from "../plan-provenance.ts";

const rec: Provenance = {
  schema: 1,
  stage: "aws-proof",
  generatedAt: "2026-10-31T00:00:00Z",
  target: { accountId: "500268966557", region: "us-west-2", callerArn: "arn:aws:sts::500268966557:assumed-role/x/y" },
  source: { commit: "a".repeat(40), stackSourceSha256: "s", clean: true },
  inputs: { sha256: "i", sanitizedPath: "p", sanitizedSha256: "q" },
  state: { bucket: "b", prefix: "x/", bucketExisted: true, objects: 0, digest: "d" },
  tools: {},
  outputs: { plan: { path: "p", sha256: "p1" }, detailed: { path: "d", sha256: "d1" } },
  summary: "Plan: 78 to create",
  note: "",
};
const ok: Observed = { stage: "aws-proof", accountId: "500268966557", region: "us-west-2", commit: "b".repeat(40), stackSourceSha256: "s", clean: true, inputsSha256: "i", state: { objects: 0, digest: "d" }, planSha256: "p1", detailedSha256: "d1" };

describe("plan provenance binds up to the captured plan", () => {
  test("a later commit that leaves the stack source unchanged still verifies", () => {
    expect(provenanceMismatches(rec, ok)).toEqual([]);
  });
  test("any change to source, inputs, state, target or plan files refuses", () => {
    const cases: [Partial<Observed>, RegExp][] = [
      [{ stackSourceSha256: "s2" }, /stack source/],
      [{ clean: false }, /uncommitted/],
      [{ inputsSha256: "i2" }, /inputs/],
      [{ state: { objects: 1, digest: "d" } }, /state objects/],
      [{ state: { objects: 0, digest: "d2" } }, /state digest/],
      [{ accountId: "111122223333" }, /account/],
      [{ region: "us-east-1" }, /region/],
      [{ planSha256: "x" }, /plan file/],
      [{ detailedSha256: "x" }, /detailed plan file/],
      [{ stage: "aws-sessions-dev" }, /stage/],
    ];
    for (const [patch, re] of cases) expect(provenanceMismatches(rec, { ...ok, ...patch }).join("; ")).toMatch(re);
  });
  test("state digest covers every key and ETag, order-independent", () => {
    const a = stateDigest([{ Key: "k1", ETag: "e1" }, { Key: "k2", ETag: "e2" }]);
    expect(stateDigest([{ Key: "k2", ETag: "e2" }, { Key: "k1", ETag: "e1" }])).toEqual(a);
    expect(stateDigest([{ Key: "k1", ETag: "e1" }, { Key: "k2", ETag: "e3" }]).digest).not.toBe(a.digest);
  });
  test("sanitizing removes the operator address, timestamps and the cluster CA", () => {
    expect(JSON.stringify(sanitizeInputs({ eks: { apiAccess: { publicAccessCidrs: ["73.162.135.254/32"] } } }))).not.toContain("73.162");
    const out = sanitizePlan("[17:51:29.318] INFO (#1): x 73.162.135.254/32\n[1:2:3.4] INFO (#1):   certificateAuthorityData: LS0t", ["73.162.135.254/32"]);
    expect(out).toBe("x operator-address-redacted/32\n  certificateAuthorityData: <public cluster CA omitted>");
  });
});
