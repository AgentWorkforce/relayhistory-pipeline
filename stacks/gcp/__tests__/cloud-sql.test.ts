import { readFileSync } from "node:fs";
import { describe, expect, test } from "bun:test";
import {
  ALCHEMY_GCP_BETA_84_SQL,
  CLOUD_SQL_PROXY_IMAGE,
  cloudSqlProxyApplies,
  cloudSqlProxyWorkload,
  ensurePrivateCloudSql,
  postPrivateCloudSqlInsert,
  privateCloudSqlSpec,
  sqlAdminInsertRequest,
  type PrivateCloudSqlSpec,
  type SqlAdminCredential,
  type SqlAdminEnsureRequest,
  type SqlAdminEnsureResponse,
  type SqlAdminPollControl,
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
    expect(stack).not.toContain("ensurePrivateCloudSql");
    expect(stack).not.toContain("GCP.SQL.Instance(");
    expect(seam).not.toContain("fetch(");
    expect(seam).not.toContain("console.");
  });
});

const token = "ya29.secret-boundary";
const credential: SqlAdminCredential = { bearer: token };

function fastPoll(partial?: Partial<SqlAdminPollControl>): SqlAdminPollControl {
  let nowMs = 0;
  return {
    now: () => nowMs,
    sleep: async (ms: number) => {
      nowMs += ms;
    },
    deadlineMs: 1_000,
    intervalMs: 10,
    maxPolls: 10,
    ...partial,
  };
}

function matchingInstance(spec: PrivateCloudSqlSpec, ipPatch: Record<string, unknown> = {}, omitNetworks = false) {
  const ipConfiguration: Record<string, unknown> = {
    ipv4Enabled: false,
    privateNetwork: spec.privateNetwork,
    allocatedIpRange: spec.allocatedIpRange,
    ...ipPatch,
  };
  if (!omitNetworks && !("authorizedNetworks" in ipPatch)) ipConfiguration.authorizedNetworks = [];
  return {
    name: spec.instanceName,
    region: spec.insertBody.region,
    databaseVersion: spec.databaseVersion,
    settings: {
      tier: spec.insertBody.settings.tier,
      availabilityType: spec.insertBody.settings.availabilityType,
      ipConfiguration,
    },
    ipAddresses: [{ type: "PRIVATE", ipAddress: "10.52.0.5" }],
    state: "RUNNABLE",
  };
}

function scripted(steps: SqlAdminEnsureResponse[]) {
  const calls: SqlAdminEnsureRequest[] = [];
  const seenBearers: string[] = [];
  return {
    calls,
    seenBearers,
    transport: {
      request: async (call: SqlAdminEnsureRequest, presented: SqlAdminCredential) => {
        calls.push({ method: call.method, url: call.url, body: call.body });
        seenBearers.push(presented.bearer);
        const next = steps.shift();
        if (!next) throw new Error(`no scripted response for ${call.method} ${call.url}`);
        return next;
      },
    },
  };
}

describe("SQL Admin idempotent poster", () => {
  test("returns a matching private instance without inserting", async () => {
    const spec = privateCloudSqlSpec(input);
    const omitted = scripted([{ status: 200, body: matchingInstance(spec, {}, true) }]);
    const empty = scripted([{ status: 200, body: matchingInstance(spec) }]);
    await expect(ensurePrivateCloudSql(spec, credential, omitted.transport, fastPoll())).resolves.toEqual({
      created: false,
      operation: null,
    });
    await expect(ensurePrivateCloudSql(spec, credential, empty.transport, fastPoll())).resolves.toEqual({
      created: false,
      operation: null,
    });
    expect(omitted.calls.map((call) => call.method)).toEqual(["GET"]);
    expect(empty.calls.map((call) => call.method)).toEqual(["GET"]);
    expect(spec.posted).toBe(false);
    expect(JSON.stringify(omitted.calls)).not.toContain(token);
    expect(omitted.seenBearers).toEqual([token]);
  });

  test("inserts on 404 and polls until the operation is done", async () => {
    const spec = privateCloudSqlSpec(input);
    const poster = scripted([
      { status: 404, body: { error: { message: token } } },
      {
        status: 200,
        body: { name: "op-1", status: "PENDING", selfLink: `https://evil.example/steal?t=${token}` },
      },
      { status: 200, body: { status: "RUNNING" } },
      { status: 200, body: { status: "DONE" } },
      { status: 200, body: matchingInstance(spec) },
    ]);
    await expect(ensurePrivateCloudSql(spec, credential, poster.transport, fastPoll())).resolves.toEqual({
      created: true,
      operation: "op-1",
    });
    expect(poster.calls.map((call) => `${call.method} ${call.url}`)).toEqual([
      "GET https://sqladmin.googleapis.com/v1/projects/agentworkforce-dev1/instances/awf-dev-postgres",
      "POST https://sqladmin.googleapis.com/v1/projects/agentworkforce-dev1/instances",
      "GET https://sqladmin.googleapis.com/v1/projects/agentworkforce-dev1/operations/op-1",
      "GET https://sqladmin.googleapis.com/v1/projects/agentworkforce-dev1/operations/op-1",
      "GET https://sqladmin.googleapis.com/v1/projects/agentworkforce-dev1/instances/awf-dev-postgres",
    ]);
    expect(poster.calls[1]!.body?.settings.ipConfiguration).toEqual(spec.insertBody.settings.ipConfiguration);
    expect(JSON.stringify(poster.calls)).not.toContain(token);
    expect(JSON.stringify(poster.calls)).not.toContain("evil.example");
    expect(poster.seenBearers.every((value) => value === token)).toBe(true);
    expect(spec.posted).toBe(false);
    expect(cloudSqlProxyApplies(spec)).toBe(false);
  });

  test("treats a 409 race as idempotent when the instance matches", async () => {
    const spec = privateCloudSqlSpec(input);
    const poster = scripted([
      { status: 404 },
      { status: 409, body: { error: { message: token } } },
      { status: 200, body: matchingInstance(spec) },
    ]);
    await expect(ensurePrivateCloudSql(spec, credential, poster.transport, fastPoll())).resolves.toEqual({
      created: false,
      operation: null,
    });
    expect(poster.calls.map((call) => call.method)).toEqual(["GET", "POST", "GET"]);
  });

  test("refuses public fallback and a mismatched instance before any insert", async () => {
    const tokenBody = token;
    async function once(body: unknown, pattern: RegExp) {
      const spec = privateCloudSqlSpec(input);
      const poster = scripted([{ status: 200, body }]);
      let caught: Error | undefined;
      try {
        await ensurePrivateCloudSql(spec, credential, poster.transport, fastPoll());
      } catch (error) {
        caught = error as Error;
      }
      expect(caught?.message).toMatch(pattern);
      expect(caught?.message).not.toContain(tokenBody);
      expect(poster.calls.map((call) => call.method)).toEqual(["GET"]);
      expect(spec.posted).toBe(false);
    }
    const spec = privateCloudSqlSpec(input);
    await once(matchingInstance(spec, { ipv4Enabled: true }), /is public/);
    await once(
      matchingInstance(spec, { authorizedNetworks: [{ value: "0.0.0.0/0" }] }),
      /is public/,
    );
    await once(
      { ...matchingInstance(spec), ipAddresses: [{ type: "PRIMARY", ipAddress: "203.0.113.10" }] },
      /is public/,
    );
    await once(matchingInstance(spec, { allocatedIpRange: "other-psa" }), /does not match/);
    await once(matchingInstance(spec, { privateNetwork: "/projects/other-gcp-dev1/global/networks/awf-dev-vpc" }), /does not match/);
    await once({ ...matchingInstance(spec), state: "FAILED" }, /not runnable/);
    await once({ ...matchingInstance(spec), state: "PENDING_CREATE" }, /not runnable/);

    const mutated = privateCloudSqlSpec(input);
    (mutated.insertBody.settings.ipConfiguration as { ipv4Enabled: boolean }).ipv4Enabled = true;
    const blocked = scripted([]);
    await expect(ensurePrivateCloudSql(mutated, credential, blocked.transport, fastPoll())).rejects.toThrow(/Nothing was created/);
    expect(blocked.calls).toHaveLength(0);
  });

  test("refuses to adopt a different instance after 409", async () => {
    const spec = privateCloudSqlSpec(input);
    const poster = scripted([
      { status: 404 },
      { status: 409, body: { error: { message: token } } },
      { status: 200, body: matchingInstance(spec, { privateNetwork: "/projects/other-gcp-dev1/global/networks/awf-dev-vpc" }) },
    ]);
    let caught: Error | undefined;
    try {
      await ensurePrivateCloudSql(spec, credential, poster.transport, fastPoll());
    } catch (error) {
      caught = error as Error;
    }
    expect(caught?.message).toMatch(/refused to adopt/);
    expect(caught?.message).not.toContain(token);
    expect(caught?.message).not.toContain("Nothing was created");
    expect(poster.calls.map((call) => call.method)).toEqual(["GET", "POST", "GET"]);
    expect(spec.posted).toBe(false);
  });

  test("redacts failed operations, timeouts, and transport errors", async () => {
    async function capture(steps: Array<SqlAdminEnsureResponse | "throw">, pattern: RegExp) {
      const spec = privateCloudSqlSpec(input);
      const calls: string[] = [];
      let caught: Error | undefined;
      try {
        await ensurePrivateCloudSql(
          spec,
          credential,
          {
            request: async (call) => {
              calls.push(`${call.method} ${call.url}`);
              const next = steps.shift();
              if (next === "throw") throw new Error(`transport failed ${token}`);
              if (!next) throw new Error("missing step");
              return next;
            },
          },
          fastPoll({ deadlineMs: 25, intervalMs: 10, maxPolls: 10 }),
        );
      } catch (error) {
        caught = error as Error;
      }
      expect(caught?.message).toMatch(pattern);
      expect(caught?.message).not.toContain(token);
      expect(JSON.stringify(caught)).not.toContain(token);
      if (!/Nothing was created/.test(pattern.source)) expect(caught?.message).not.toContain("Nothing was created");
      return calls;
    }

    const instanceUrl = "https://sqladmin.googleapis.com/v1/projects/agentworkforce-dev1/instances/awf-dev-postgres";
    const operationUrl = "https://sqladmin.googleapis.com/v1/projects/agentworkforce-dev1/operations/op-1";
    const insertUrl = "https://sqladmin.googleapis.com/v1/projects/agentworkforce-dev1/instances";
    const failed = await capture(
      [
        { status: 404 },
        { status: 200, body: { name: "op-1", status: "PENDING" } },
        { status: 200, body: { status: "DONE", error: { errors: [{ message: token }] } } },
      ],
      /operation failed/,
    );
    expect(failed).toEqual([`GET ${instanceUrl}`, `POST ${insertUrl}`, `GET ${operationUrl}`]);

    const immediate = await capture(
      [{ status: 404 }, { status: 200, body: { name: "op-1", status: "DONE", error: { message: token } } }],
      /operation failed/,
    );
    expect(immediate).toEqual([`GET ${instanceUrl}`, `POST ${insertUrl}`]);

    const createdPublic = await capture(
      [
        { status: 404 },
        { status: 200, body: { name: "op-1", status: "DONE" } },
        { status: 200, body: matchingInstance(privateCloudSqlSpec(input), { ipv4Enabled: true }) },
      ],
      /does not match the private body/,
    );
    expect(createdPublic).toEqual([`GET ${instanceUrl}`, `POST ${insertUrl}`, `GET ${instanceUrl}`]);

    const running = { status: 200, body: { status: "RUNNING" } };
    const timed = await capture(
      [{ status: 404 }, { status: 200, body: { name: "op-1", status: "RUNNING" } }, running, running, running],
      /timed out/,
    );
    expect(timed[0]).toBe(`GET ${instanceUrl}`);
    expect(timed[1]).toBe(`POST ${insertUrl}`);
    expect(timed.slice(2).every((call) => call === `GET ${operationUrl}`)).toBe(true);

    const lookup = await capture(["throw"], /Nothing was created/);
    expect(lookup).toEqual([`GET ${instanceUrl}`]);

    const insert = await capture([{ status: 404 }, "throw"], /outcome is uncertain/);
    expect(insert).toEqual([`GET ${instanceUrl}`, `POST ${insertUrl}`]);
    expect(insert.join(" ")).not.toContain(token);

    await expect(ensurePrivateCloudSql(privateCloudSqlSpec(input), { bearer: "" }, scripted([]).transport, fastPoll())).rejects.toThrow(
      /credential is missing/,
    );
  });
});
