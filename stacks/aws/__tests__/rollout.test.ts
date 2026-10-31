import { describe, expect, it } from "bun:test";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { legacyPodsRoleProps } from "../eks.ts";

const eks = readFileSync(fileURLToPath(new URL("../eks.ts", import.meta.url)), "utf8");

describe("staged, reversible rollout", () => {
  it("applies every NetworkPolicy first and enforces only in a later, separate apply", () => {
    const enforce = eks.indexOf('Kubernetes.Manifest("network-policy-controller"');
    const policies = eks.indexOf("for (const policy of networkPolicies(");
    expect(policies).toBeGreaterThan(0);
    expect(enforce).toBeGreaterThan(policies);
    expect(eks.slice(enforce - 120, enforce)).toContain("if (i.networkPolicyEnforcement)");
    expect(eks.match(/network-policy-controller/g)).toHaveLength(1);
  });

  it("keeps the old shared pods role, byte for byte and unattached, until a later apply retires it", () => {
    expect(eks).toContain('if (i.legacyPodsRole === "keep") yield* AWS.IAM.Role("pods-role", legacyPodsRoleProps(i));');
    const p = legacyPodsRoleProps({ stage: "aws-sessions-dev", lockerBucketArn: "arn:aws:s3:::l", lockerKmsKeyArn: "k", tags: {} });
    expect(p.roleName).toBe("96bulls-aws-sessions-dev-pods");
    expect(p.description).toBe("96bulls aws-sessions-dev: worker and ingest pods (EKS Pod Identity)");
    expect(p.inlinePolicies.locker.Statement[1]!.Action).toEqual(["s3:GetObject", "s3:GetObjectVersion", "s3:PutObject", "s3:DeleteObject"]);
    // No association points at it.
    expect(eks).not.toMatch(/roleArn: .*pods-role|legacyPodsRole[^\n]*roleArn/);
  });
});
