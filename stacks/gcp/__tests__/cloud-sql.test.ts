import { describe, expect, test } from "bun:test";
import { ALCHEMY_GCP_BETA_84_SQL, CLOUD_SQL_PROXY_IMAGE, cloudSqlProxyApplies, cloudSqlProxyWorkload, privateCloudSqlSpec } from "../cloud-sql.ts";

const input = {
  projectId: "agentworkforce-dev1",
  region: "us-central1",
  instanceName: "awf-dev-postgres",
  networkName: "awf-dev-vpc",
  allocatedIpRange: "awf-dev-psa",
};

describe("Cloud SQL private attachment", () => {
  test("the pinned provider still cannot post privateNetwork", () => {
    expect(ALCHEMY_GCP_BETA_84_SQL.privateNetwork).toBe(false);
  });

  test("builds a private-only insert body and routes the proxy with --private-ip", () => {
    const spec = privateCloudSqlSpec(input);
    expect(spec.posted).toBe(false);
    expect(cloudSqlProxyApplies(spec)).toBe(false);
    expect(spec.ipv4Enabled).toBe(false);
    expect(spec.connectionName).toBe("agentworkforce-dev1:us-central1:awf-dev-postgres");
    expect(spec.privateNetwork).toBe("/projects/agentworkforce-dev1/global/networks/awf-dev-vpc");
    expect(spec.insertBody.settings.ipConfiguration).toEqual({
      ipv4Enabled: false,
      privateNetwork: "/projects/agentworkforce-dev1/global/networks/awf-dev-vpc",
      allocatedIpRange: "awf-dev-psa",
    });
    expect(spec.insertBody.settings.availabilityType).toBe("ZONAL");
    expect(spec.proxyArgs).toEqual([
      "--private-ip",
      "--address=0.0.0.0",
      "--port=5432",
      "agentworkforce-dev1:us-central1:awf-dev-postgres",
    ]);
    const rendered = JSON.stringify(spec.insertBody);
    expect(rendered).not.toContain("authorizedNetworks");
    expect(rendered).not.toContain("rootPassword");
    expect(rendered).not.toContain('"ipv4Enabled":true');
  });

  test("uses a regional availability type only when HA is requested", () => {
    expect(privateCloudSqlSpec({ ...input, highAvailability: true }).insertBody.settings.availabilityType).toBe("REGIONAL");
  });

  test("refuses a public network, a missing range, or a non-name network", () => {
    expect(() => privateCloudSqlSpec({ ...input, networkName: "" })).toThrow(/Nothing was created/);
    expect(() => privateCloudSqlSpec({ ...input, allocatedIpRange: "" })).toThrow(/Nothing was created/);
    expect(() => privateCloudSqlSpec({ ...input, networkName: "/projects/agentworkforce-dev1/global/networks/awf-dev-vpc" })).toThrow(
      /Nothing was created/,
    );
    expect(() => privateCloudSqlSpec({ ...input, projectId: "other-gcp-dev1/../" })).toThrow(/Nothing was created/);
  });

  test("renders the proxy with the private args and no secret material", () => {
    const spec = privateCloudSqlSpec(input);
    const proxy = cloudSqlProxyWorkload(spec, "awf-dev-runtime@agentworkforce-dev1.iam.gserviceaccount.com");
    expect(proxy.deployment.spec.template.spec.containers[0]!.image).toBe(CLOUD_SQL_PROXY_IMAGE);
    expect(proxy.deployment.spec.template.spec.containers[0]!.args[0]).toBe("--private-ip");
    expect(proxy.deployment.spec.template.spec.containers[0]!.args.at(-1)).toBe(spec.connectionName);
    expect(proxy.serviceAccount.metadata.annotations["iam.gke.io/gcp-service-account"]).toBe(
      "awf-dev-runtime@agentworkforce-dev1.iam.gserviceaccount.com",
    );
    expect(JSON.stringify(proxy)).not.toMatch(/password|token|BEGIN /i);
    expect(() => cloudSqlProxyWorkload(spec, "not-an-email")).toThrow(/Nothing was created/);
  });
});
