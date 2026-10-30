import { readFileSync } from "node:fs";
import { describe, expect, test } from "bun:test";
import {
  ALCHEMY_GCP_BETA_84_SQL,
  CLOUD_SQL_PROXY_IMAGE,
  cloudSqlProxyApplies,
  cloudSqlProxyWorkload,
  postPrivateCloudSqlInsert,
  privateCloudSqlSpec,
  sqlAdminInsertRequest,
  type PrivateCloudSqlSpec,
} from "../cloud-sql.ts";

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

  test("builds a SQL Admin insert without sending it", () => {
    const spec = privateCloudSqlSpec(input);
    const request = sqlAdminInsertRequest(spec);
    expect(request.sent).toBe(false);
    expect(request.method).toBe("POST");
    expect(request.url).toBe("https://sqladmin.googleapis.com/v1/projects/agentworkforce-dev1/instances");
    expect(request.headers).toEqual({ "content-type": "application/json" });
    expect(JSON.stringify(request)).not.toMatch(/authorization|bearer|token/i);
    expect(request.body).toEqual(spec.insertBody);
    expect(spec.posted).toBe(false);
    expect(JSON.stringify(request)).not.toContain("authorizedNetworks");
    expect(JSON.stringify(request)).not.toContain("rootPassword");
  });

  test("refuses a public or secret insert before the transport is called", async () => {
    const token = "ya29.secret-boundary";
    async function posts(mutate: (spec: PrivateCloudSqlSpec) => void) {
      const spec = privateCloudSqlSpec(input);
      mutate(spec);
      let calls = 0;
      await expect(
        postPrivateCloudSqlInsert(spec, {
          post: async () => {
            calls += 1;
            return { name: "op-1" };
          },
        }),
      ).rejects.toThrow(/Nothing was created/);
      expect(calls).toBe(0);
    }
    await posts((spec) => {
      (spec.insertBody.settings.ipConfiguration as { ipv4Enabled: boolean }).ipv4Enabled = true;
    });
    await posts((spec) => {
      (spec.insertBody.settings.ipConfiguration as { authorizedNetworks?: unknown }).authorizedNetworks = [{ value: "0.0.0.0/0" }];
    });
    await posts((spec) => {
      (spec.insertBody as { rootPassword?: string }).rootPassword = token;
    });
    await posts((spec) => {
      spec.insertBody.settings.ipConfiguration.privateNetwork = "/projects/other-gcp-dev1/global/networks/awf-dev-vpc";
    });
    await posts((spec) => {
      (spec as { posted: boolean }).posted = true;
    });
  });

  test("posts only the private body through an injected transport and does not echo a credential", async () => {
    const spec = privateCloudSqlSpec(input);
    const token = "ya29.secret-boundary";
    let seen = "";
    const result = await postPrivateCloudSqlInsert(spec, {
      post: async (request) => {
        seen = JSON.stringify(request);
        return { name: "op-1" };
      },
    });
    expect(result).toEqual({ operation: "op-1", sent: true });
    expect(spec.posted).toBe(false);
    expect(cloudSqlProxyApplies(spec)).toBe(false);
    expect(seen).toContain("https://sqladmin.googleapis.com/v1/projects/agentworkforce-dev1/instances");
    expect(seen).not.toContain(token);
    await expect(
      postPrivateCloudSqlInsert(spec, {
        post: async () => {
          throw new Error(`${token} was refused`);
        },
      }),
    ).rejects.toThrow(/outcome is uncertain for agentworkforce-dev1\/awf-dev-postgres; reconcile.*before retry or teardown/);
    await postPrivateCloudSqlInsert(spec, {
      post: async () => {
        throw new Error(`${token} was refused`);
      },
    }).catch((error: Error) => {
      expect(error.message).not.toContain(token);
      expect(error.message).not.toContain("Nothing was created");
    });
    await expect(
      postPrivateCloudSqlInsert(spec, {
        post: async () => ({ name: "https://evil.example/operations/op-1" }),
      }),
    ).rejects.toThrow(/outcome is uncertain/);
  });

  test("the Alchemy stack can prepare the insert and cannot post it or call GCP.SQL.Instance", () => {
    const stack = readFileSync(new URL("../alchemy.gcp.run.ts", import.meta.url), "utf8");
    const seam = readFileSync(new URL("../cloud-sql.ts", import.meta.url), "utf8");
    expect(stack).toContain("sqlAdminInsertRequest");
    expect(stack).not.toContain("postPrivateCloudSqlInsert");
    expect(stack).not.toContain("GCP.SQL.Instance(");
    expect(seam).not.toContain("fetch(");
  });
});
