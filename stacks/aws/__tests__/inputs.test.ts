import { describe, expect, it } from "bun:test";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { sharedServicesRepository, validateAwsInputs, type AwsInputs } from "../inputs.ts";

const example = JSON.parse(readFileSync(fileURLToPath(new URL("../examples/aws-proof.inputs.example.json", import.meta.url)), "utf8")) as AwsInputs;
const filled = (patch: (i: AwsInputs) => void = () => {}) => {
  const i = structuredClone(example);
  i.eks!.imageDigest = `sha256:${"c".repeat(64)}`;
  i.eks!.imageSourceRevision = "d".repeat(40);
  i.eks!.apiAccess = { publicAccessCidrs: ["203.0.113.7/32"] };
  patch(i);
  return i;
};

describe("aws-proof: a fresh, fully hardened stage beside sessions-dev", () => {
  it("validates once the placeholders are filled, with every hardening default on", () => {
    const r = validateAwsInputs("aws-proof", filled());
    expect(r.hoopdb).toBe("cnpg");
    expect(r.endpointAccess).toEqual({ endpointPrivateAccess: true, endpointPublicAccess: true, publicAccessCidrs: ["203.0.113.7/32"] });
    expect(example.eks!.relayauth).toBeDefined(); // RelayAuth with in-cluster TLS
    expect(example.eks!.ingress!.hostname).toBe("proof-sessions.agentrelay.com");
    expect(example.eks!.ingress!.scheme).toBe("internet-facing");
  });

  it("refuses the example as committed: placeholders are not digests or commits", () => {
    expect(() => validateAwsInputs("aws-proof", { ...example, eks: { ...example.eks!, apiAccess: {} } })).toThrow(/Nothing was created/);
  });

  it("shares the account's ECR repository, OIDC provider and publisher with the owner stage", () => {
    expect(example.accountShared).toEqual({ ownerStage: "aws-sessions-dev" });
    expect(sharedServicesRepository("500268966557", "us-west-2")).toBe("500268966557.dkr.ecr.us-west-2.amazonaws.com/96bulls-services");
    expect(() => validateAwsInputs("aws-proof", filled((i) => (i.accountShared = { ownerStage: "aws-proof" })))).toThrow(/another aws-\* stage/);
  });

  it("is refused if it would expose the API to the internet or run CNPG on production", () => {
    expect(() => validateAwsInputs("aws-proof", filled((i) => (i.eks!.apiAccess = { publicAccessCidrs: ["0.0.0.0/0"] })))).toThrow(/broader than/);
    expect(() => validateAwsInputs("aws-prod", filled())).toThrow(/Supabase/);
    expect(() => validateAwsInputs("aws-proof", filled((i) => (i.eks!.relayauthIssuer = "https://relayauth.dev")))).toThrow(/both set/);
  });

  it("names a cluster the #74 harness guard expects", () => {
    // eks.ts: clusterName = `96bulls-${stage}`
    expect(readFileSync(fileURLToPath(new URL("../eks.ts", import.meta.url)), "utf8")).toContain("const clusterName = `96bulls-${i.stage}`;");
    expect(`96bulls-${"aws-proof"}`).toBe("96bulls-aws-proof");
  });
});
