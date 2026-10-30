import { describe, expect, test } from "bun:test";
import { ALCHEMY_GCP_BETA_84_SQL, cloudSqlPlan } from "../cloud-sql.ts";

describe("Cloud SQL private attachment", () => {
  test("does not declare an instance on the pinned provider", () => {
    const plan = cloudSqlPlan(
      { network: "awf-dev-vpc", allocatedIpRange: "awf-dev-psa" },
      ALCHEMY_GCP_BETA_84_SQL,
    );
    expect(ALCHEMY_GCP_BETA_84_SQL.privateNetwork).toBe(false);
    expect(plan.declare).toBe(false);
    if (!plan.declare) expect(plan.reason).toContain("Nothing was created.");
  });

  test("a provider that can attach a private network disables public IPv4 and sets the proxy to private IP", () => {
    const plan = cloudSqlPlan(
      { network: "awf-dev-vpc", allocatedIpRange: "awf-dev-psa" },
      { privateNetwork: true },
    );
    expect(plan.declare).toBe(true);
    if (plan.declare) {
      expect(plan.attachment).toEqual({
        ipv4Enabled: false,
        privateNetwork: "awf-dev-vpc",
        allocatedIpRange: "awf-dev-psa",
        proxyArgs: ["--private-ip", "--address=0.0.0.0", "--port=5432"],
      });
    }
  });

  test("refuses a capable provider when the VPC or the peering range is missing", () => {
    expect(() => cloudSqlPlan({ network: "awf-dev-vpc" }, { privateNetwork: true })).toThrow(/Nothing was created/);
    expect(() => cloudSqlPlan({ allocatedIpRange: "awf-dev-psa" }, { privateNetwork: true })).toThrow(/Nothing was created/);
    expect(() => cloudSqlPlan({ network: { name: "awf-dev-vpc" }, allocatedIpRange: "awf-dev-psa" }, { privateNetwork: true })).toThrow(
      /Nothing was created/,
    );
  });
});
