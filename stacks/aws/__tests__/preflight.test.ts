import { describe, expect, it } from "bun:test";
import { networkPolicies } from "../proof-addons.ts";
import { policyLogicalId, policySetSha256, verifyAccountOwner, verifyNetworkPolicyPhase, verifyServicesImage, type AwsCli } from "../preflight.ts";

const DIGEST = "sha256:c13b879a668984f905e30e38b05764ec712532e8395b256d416ba1602f6c6726";
const REV = "ffb7ef0f684a92c129bbf14c2da2066fbe6fe6b2";
const REPO = "500268966557.dkr.ecr.us-west-2.amazonaws.com/96bulls-services";
const fake = (json: (args: string[]) => unknown, s3: (key: string) => unknown = () => { throw new Error("NoSuchKey"); }): AwsCli => ({ json, s3Json: (_b, key) => s3(key) });

describe("services image provenance against ECR", () => {
  it("passes when the digest exists and is tagged sha-<sourceRevision>", () => {
    const aws = fake((a) => {
      expect(a).toContain(`imageDigest=${DIGEST}`);
      return { imageDetails: [{ imageDigest: DIGEST, imageTags: [`sha-${REV}`] }] };
    });
    expect(() => verifyServicesImage(aws, { repositoryUri: REPO, imageDigest: DIGEST, imageSourceRevision: REV })).not.toThrow();
  });
  it("refuses a digest built from another commit, an untagged digest, or a missing one", () => {
    const tagged = (tags: string[]) => fake(() => ({ imageDetails: [{ imageDigest: DIGEST, imageTags: tags }] }));
    expect(() => verifyServicesImage(tagged([`sha-${"a".repeat(40)}`]), { repositoryUri: REPO, imageDigest: DIGEST, imageSourceRevision: REV })).toThrow(/not built from imageSourceRevision/);
    expect(() => verifyServicesImage(tagged([]), { repositoryUri: REPO, imageDigest: DIGEST, imageSourceRevision: REV })).toThrow(/not built from imageSourceRevision/);
    expect(() => verifyServicesImage(fake(() => { throw new Error("ImageNotFoundException"); }), { repositoryUri: REPO, imageDigest: DIGEST, imageSourceRevision: REV })).toThrow(/is not in/);
  });
});

describe("accountShared ownership", () => {
  const aws = (owner: string) =>
    fake((a) => (a[1] === "describe-repositories" ? { repositories: [{ repositoryArn: "arn:aws:ecr:us-west-2:500268966557:repository/96bulls-services" }] } : { tags: [{ Key: "alchemy::stage", Value: owner }] }));
  it("passes only when the owner stage's Alchemy tag is on the repository", () => {
    expect(() => verifyAccountOwner(aws("aws-sessions-dev"), { accountId: "500268966557", region: "us-west-2", ownerStage: "aws-sessions-dev" })).not.toThrow();
    expect(() => verifyAccountOwner(aws("aws-other"), { accountId: "500268966557", region: "us-west-2", ownerStage: "aws-sessions-dev" })).toThrow(/owned by stage "aws-other"/);
    expect(() => verifyAccountOwner(fake(() => { throw new Error("RepositoryNotFound"); }), { accountId: "500268966557", region: "us-west-2", ownerStage: "aws-sessions-dev" })).toThrow(/no 96bulls-services repository/);
  });
});

describe("NetworkPolicy phase 2 is state-enforced", () => {
  const policies = networkPolicies({ hoopdb: "cnpg", relayauth: true, relaycast: true });
  const sha = policySetSha256(policies);
  const record = { policySetSha256: sha, verifiedBy: "operator", verifiedOn: "2026-10-31" };
  const applied = (key: string) => {
    const p = policies.find((x) => key.endsWith(`/${policyLogicalId(x)}.json`));
    if (!p) throw new Error("NoSuchKey");
    return { status: "created", props: { manifest: { ...p, metadata: { ...p.metadata, annotations: { a: "b" } } } } };
  };
  const base = { stateBucket: "b", statePrefix: "96bulls/96bulls-aws/aws-proof", region: "us-west-2", policies };

  it("refuses on a fresh stage (nothing in state), even with a correct record", () => {
    expect(() => verifyNetworkPolicyPhase(fake(() => ({})), { ...base, record })).toThrow(/not in Alchemy state: apply phase 1 first/);
  });
  it("refuses a record for another policy set, or without who and when", () => {
    expect(() => verifyNetworkPolicyPhase(fake(() => ({}), applied), { ...base, record: { ...record, policySetSha256: "0".repeat(64) } })).toThrow(new RegExp(`must be ${sha}`));
    expect(() => verifyNetworkPolicyPhase(fake(() => ({}), applied), { ...base, record: { policySetSha256: sha } })).toThrow(/verifiedBy and verifiedOn/);
  });
  it("refuses when state has a policy that differs from the current one", () => {
    const drifted = (key: string) => {
      const s = applied(key) as { props: { manifest: { spec: unknown } } };
      return key.endsWith("netpol-96bulls-default-deny.json") ? { ...s, props: { manifest: { spec: { podSelector: {}, policyTypes: ["Ingress"] } } } } : s;
    };
    expect(() => verifyNetworkPolicyPhase(fake(() => ({}), drifted), { ...base, record })).toThrow(/netpol-96bulls-default-deny in state differs/);
  });
  it("passes when every current policy was applied in an earlier apply and the record names this set", () => {
    expect(() => verifyNetworkPolicyPhase(fake(() => ({}), applied), { ...base, record })).not.toThrow();
  });
});
