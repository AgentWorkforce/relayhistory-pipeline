import { describe, expect, test } from "bun:test";
import { currentObjects, provenanceMismatches, sanitizeInputs, sanitizePlan, versionsDigest, type Observed, type Provenance } from "../plan-provenance.ts";

const rec: Provenance = {
  schema: 1,
  stage: "aws-proof",
  generatedAt: "2026-10-31T00:00:00Z",
  target: { accountId: "500268966557", region: "us-west-2", callerArn: "arn:aws:sts::500268966557:assumed-role/x/y" },
  source: { commit: "a".repeat(40), stackSourceSha256: "s", clean: true },
  inputs: { sha256: "i", sanitizedPath: "p", sanitizedSha256: "q" },
  state: { bucket: "b", prefix: "x/", versioning: "Enabled", method: "snapshot", objects: 0, snapshotSha256: "z", versionsDigest: "d", versionEntries: 0 },
  tools: {},
  outputs: { plan: { path: "p", sha256: "p1" }, detailed: { path: "d", sha256: "d1" } },
  summary: "Plan: 78 to create",
  note: "",
};
const ok: Observed = { stage: "aws-proof", accountId: "500268966557", region: "us-west-2", commit: "a".repeat(40), commitChangedFiles: [], stackSourceSha256: "s", clean: true, inputsSha256: "i", state: { versioning: "Enabled", versionsDigest: "d", versionEntries: 0 }, planSha256: "p1", detailedSha256: "d1" };

describe("plan provenance binds up to the captured plan", () => {
  test("the capture commit, or a descendant changing only the capture files, verifies", () => {
    expect(provenanceMismatches(rec, ok)).toEqual([]);
    expect(provenanceMismatches(rec, { ...ok, commit: "b".repeat(40), commitChangedFiles: ["docs/plans/aws-proof.provenance.json"] })).toEqual([]);
  });
  test("compares the source commit (B1 review): another commit, or a non-descendant, refuses", () => {
    expect(provenanceMismatches(rec, { ...ok, commit: "b".repeat(40), commitChangedFiles: ["stacks/aws/eks.ts"] }).join(";")).toMatch(/source commit/);
    expect(provenanceMismatches(rec, { ...ok, commit: "b".repeat(40), commitChangedFiles: null }).join(";")).toMatch(/does not descend/);
  });
  test("any change to source, inputs, state, target or plan files refuses", () => {
    const cases: [Partial<Observed>, RegExp][] = [
      [{ stackSourceSha256: "s2" }, /stack source/],
      [{ clean: false }, /uncommitted/],
      [{ inputsSha256: "i2" }, /inputs/],
      [{ state: { versioning: "Enabled", versionsDigest: "d", versionEntries: 1 } }, /state versions/],
      [{ state: { versioning: "Enabled", versionsDigest: "d2", versionEntries: 0 } }, /state digest/],
      [{ state: { versioning: "Suspended", versionsDigest: "d", versionEntries: 0 } }, /versioning/],
      [{ accountId: "111122223333" }, /account/],
      [{ region: "us-east-1" }, /region/],
      [{ planSha256: "x" }, /plan file/],
      [{ detailedSha256: "x" }, /detailed plan file/],
      [{ stage: "aws-sessions-dev" }, /stage/],
    ];
    for (const [patch, re] of cases) expect(provenanceMismatches(rec, { ...ok, ...patch }).join("; ")).toMatch(re);
  });
  test("a transient write that was undone (state object PUT then DELETE) still changes the state digest; the lease does not", () => {
    const before = { Versions: [{ Key: "p/cluster.json", VersionId: "v1", IsLatest: true, ETag: "e1" }], DeleteMarkers: [] };
    const after = {
      Versions: [...before.Versions, { Key: "p/worker-role.json", VersionId: "w1", IsLatest: false, ETag: "e9" }],
      DeleteMarkers: [{ Key: "p/worker-role.json", VersionId: "w2", IsLatest: true }],
    };
    // Same current objects (what key+ETag compared), different version history.
    expect(currentObjects(after)).toEqual(currentObjects(before));
    expect(versionsDigest(after).digest).not.toBe(versionsDigest(before).digest);
    expect(versionsDigest({ Versions: [...before.Versions].reverse(), DeleteMarkers: [] })).toEqual(versionsDigest(before));
    // Alchemy's lease (taken by every deploy before the stack runs) is not state.
    const lease = { ...before, Versions: [...before.Versions, { Key: "p/__lease__.json", VersionId: "l1", IsLatest: false, ETag: "x" }], DeleteMarkers: [{ Key: "p/__lease__.json", VersionId: "l2", IsLatest: true }] };
    expect(versionsDigest(lease)).toEqual(versionsDigest(before));
    // A delete alone (marker, no new version) is a write too.
    expect(versionsDigest({ ...before, DeleteMarkers: [{ Key: "p/cluster.json", VersionId: "d1", IsLatest: false }] }).digest).not.toBe(versionsDigest(before).digest);
  });

  test("the snapshot reads only the latest, undeleted version of each key", () => {
    expect(currentObjects({
      Versions: [
        { Key: "a", VersionId: "a2", IsLatest: true, ETag: "x" },
        { Key: "a", VersionId: "a1", IsLatest: false, ETag: "y" },
        { Key: "gone", VersionId: "g1", IsLatest: false, ETag: "z" },
      ],
      DeleteMarkers: [{ Key: "gone", VersionId: "g2", IsLatest: true }],
    })).toEqual([{ key: "a", versionId: "a2" }]);
  });

  test("sanitizing removes the operator address, timestamps and the cluster CA", () => {
    expect(JSON.stringify(sanitizeInputs({ eks: { apiAccess: { publicAccessCidrs: ["73.162.135.254/32"] } } }))).not.toContain("73.162");
    const out = sanitizePlan("[17:51:29.318] INFO (#1): x 73.162.135.254/32\n[1:2:3.4] INFO (#1):   certificateAuthorityData: LS0t", ["73.162.135.254/32"]);
    expect(out).toBe("x operator-address-redacted/32\n  certificateAuthorityData: <public cluster CA omitted>");
  });
});
