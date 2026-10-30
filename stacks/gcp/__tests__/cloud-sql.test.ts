import { readFileSync } from "node:fs";
import { describe, expect, test } from "bun:test";
import {
  ALCHEMY_GCP_BETA_84_SQL,
  CLOUD_SQL_PROXY_IMAGE,
  cloudSqlProxyWorkload,
  ensurePrivateCloudSql,
  privateCloudSqlSpec,
  sqlAdminFetchTransport,
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
  allocatedIpCidr: "10.80.0.0/20",
};

describe("Cloud SQL private attachment", () => {
  test("the pinned provider still cannot post privateNetwork", () => {
    expect(ALCHEMY_GCP_BETA_84_SQL.privateNetwork).toBe(false);
  });

  test("builds a private-only insert body and routes the proxy with --private-ip", () => {
    const spec = privateCloudSqlSpec(input);
    expect(spec.posted).toBe(false);
    expect(spec.ipv4Enabled).toBe(false);
    expect(spec.connectionName).toBe("agentworkforce-dev1:us-central1:awf-dev-postgres");
    expect(spec.privateNetwork).toBe("/projects/agentworkforce-dev1/global/networks/awf-dev-vpc");
    expect(spec.insertBody.settings.ipConfiguration).toEqual({
      ipv4Enabled: false,
      privateNetwork: "/projects/agentworkforce-dev1/global/networks/awf-dev-vpc",
      allocatedIpRange: "awf-dev-psa",
    });
    expect(spec.insertBody.settings.edition).toBe("ENTERPRISE");
    expect(spec.insertBody.settings.availabilityType).toBe("ZONAL");
    expect(spec.insertBody.settings.backupConfiguration).toEqual({
      enabled: true,
      pointInTimeRecoveryEnabled: true,
    });
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
    const settings = privateCloudSqlSpec({ ...input, highAvailability: true }).insertBody.settings;
    expect(settings.availabilityType).toBe("REGIONAL");
    expect(settings.backupConfiguration).toEqual({ enabled: true, pointInTimeRecoveryEnabled: true });
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
    const proxy = cloudSqlProxyWorkload(
      spec,
      "awf-dev-runtime@agentworkforce-dev1.iam.gserviceaccount.com",
      spec.connectionName,
    );
    expect(proxy.deployment.spec.template.spec.containers[0]!.image).toBe(CLOUD_SQL_PROXY_IMAGE);
    expect(proxy.deployment.spec.template.spec.containers[0]!.args[0]).toBe("--private-ip");
    expect(proxy.deployment.spec.template.spec.containers[0]!.args.at(-1)).toBe(spec.connectionName);
    expect(proxy.serviceAccount.metadata.annotations["iam.gke.io/gcp-service-account"]).toBe(
      "awf-dev-runtime@agentworkforce-dev1.iam.gserviceaccount.com",
    );
    expect(JSON.stringify(proxy)).not.toMatch(/password|token|BEGIN /i);
    expect(() => cloudSqlProxyWorkload(spec, "not-an-email", spec.connectionName)).toThrow(/Nothing was created/);
  });

  test("builds a SQL Admin insert without sending it", () => {
    const spec = privateCloudSqlSpec(input);
    const request = sqlAdminInsertRequest(spec);
    expect(request.sent).toBe(false);
    expect(request.method).toBe("POST");
    expect(request.url).toBe("https://sqladmin.googleapis.com/v1/projects/agentworkforce-dev1/instances");
    expect(request.headers).toEqual({ "content-type": "application/json" });
    expect(request.body.settings.edition).toBe("ENTERPRISE");
    expect(JSON.stringify(request)).not.toMatch(/authorization|bearer|token/i);
    expect(request.body).toEqual(spec.insertBody);
    expect(spec.posted).toBe(false);
    expect(JSON.stringify(request)).not.toContain("authorizedNetworks");
    expect(JSON.stringify(request)).not.toContain("rootPassword");

    const wrongEdition = privateCloudSqlSpec(input);
    (wrongEdition.insertBody.settings as { edition: string }).edition = "ENTERPRISE_PLUS";
    expect(() => sqlAdminInsertRequest(wrongEdition)).toThrow(/Nothing was created/);

    const backupsDisabled = privateCloudSqlSpec({ ...input, highAvailability: true });
    (backupsDisabled.insertBody.settings.backupConfiguration as { enabled: boolean }).enabled = false;
    expect(() => sqlAdminInsertRequest(backupsDisabled)).toThrow(/Nothing was created/);
  });

  test("the Alchemy stack posts only through the apply-time action and never calls GCP.SQL.Instance", () => {
    const stack = readFileSync(new URL("../alchemy.gcp.run.ts", import.meta.url), "utf8");
    const seam = readFileSync(new URL("../cloud-sql.ts", import.meta.url), "utf8");
    expect(stack).toContain("Alchemy.Action");
    expect(stack).toContain("ensurePrivateCloudSql");
    expect(stack).toContain("sqlAdminFetchTransport");
    expect(stack).toContain("dataPlane.sqlPoster.connectionName");
    expect(stack).not.toContain("GCP.SQL.Instance(");
    expect(seam).not.toContain("console.");
  });

  test("the live transport attaches the bearer only to the approved SQL Admin origin", async () => {
    const calls: Array<{ url: string; init: RequestInit }> = [];
    const transport = sqlAdminFetchTransport(async (url, init) => {
      calls.push({ url: String(url), init: init ?? {} });
      return new Response(JSON.stringify({ state: "RUNNABLE" }), { status: 200 });
    });
    await transport.request(
      {
        method: "GET",
        url: "https://sqladmin.googleapis.com/v1/projects/agentworkforce-dev1/instances/awf-dev-postgres",
      },
      credential,
    );
    expect(calls).toHaveLength(1);
    expect(calls[0]!.url).toBe(
      "https://sqladmin.googleapis.com/v1/projects/agentworkforce-dev1/instances/awf-dev-postgres",
    );
    expect(new Headers(calls[0]!.init.headers).get("authorization")).toBe(`Bearer ${token}`);

    await expect(
      transport.request(
        { method: "GET", url: "https://attacker.example/v1/projects/agentworkforce-dev1/instances/awf-dev-postgres" },
        credential,
      ),
    ).rejects.toThrow(/target is not approved.*Nothing was created/);
    await expect(
      transport.request(
        {
          method: "POST",
          url: "https://sqladmin.googleapis.com/v1/projects/agentworkforce-dev1/instances/awf-dev-postgres",
          body: privateCloudSqlSpec(input).insertBody,
        },
        credential,
      ),
    ).rejects.toThrow(/target is not approved.*Nothing was created/);
    expect(calls).toHaveLength(1);
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
    requestTimeoutMs: 5_000,
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
      edition: spec.insertBody.settings.edition,
      availabilityType: spec.insertBody.settings.availabilityType,
      backupConfiguration: spec.insertBody.settings.backupConfiguration,
      ipConfiguration,
    },
    ipAddresses: [{ type: "PRIVATE", ipAddress: "10.80.0.5" }],
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
    const apiPath = scripted([
      {
        status: 200,
        body: matchingInstance(spec, { privateNetwork: spec.privateNetwork.slice(1) }),
      },
    ]);
    await expect(ensurePrivateCloudSql(spec, credential, omitted.transport, fastPoll())).resolves.toEqual({
      created: false,
      operation: null,
    });
    await expect(ensurePrivateCloudSql(spec, credential, empty.transport, fastPoll())).resolves.toEqual({
      created: false,
      operation: null,
    });
    await expect(ensurePrivateCloudSql(spec, credential, apiPath.transport, fastPoll())).resolves.toEqual({
      created: false,
      operation: null,
    });
    expect(omitted.calls.map((call) => call.method)).toEqual(["GET"]);
    expect(empty.calls.map((call) => call.method)).toEqual(["GET"]);
    expect(apiPath.calls.map((call) => call.method)).toEqual(["GET"]);
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
  });

  test("treats a 409 race as idempotent when the instance matches", async () => {
    const spec = privateCloudSqlSpec(input);
    const poster = scripted([
      { status: 404 },
      { status: 409, body: { error: { message: token } } },
      { status: 404 },
      { status: 200, body: { ...matchingInstance(spec), state: "PENDING_CREATE" } },
      {
        status: 200,
        body: matchingInstance(spec, { privateNetwork: spec.privateNetwork.slice(1) }),
      },
    ]);
    await expect(ensurePrivateCloudSql(spec, credential, poster.transport, fastPoll())).resolves.toEqual({
      created: false,
      operation: null,
    });
    expect(poster.calls.map((call) => call.method)).toEqual(["GET", "POST", "GET", "GET", "GET"]);
  });

  test("bounds a 409 race that never becomes runnable", async () => {
    const spec = privateCloudSqlSpec(input);
    const poster = scripted([
      { status: 404 },
      { status: 409 },
      { status: 200, body: { ...matchingInstance(spec), state: "PENDING_CREATE" } },
      { status: 200, body: { ...matchingInstance(spec), state: "PENDING_CREATE" } },
    ]);
    await expect(
      ensurePrivateCloudSql(spec, credential, poster.transport, fastPoll({ maxPolls: 2 })),
    ).rejects.toThrow(/timed out.*reconcile.*before retry or teardown/);
    expect(poster.calls.map((call) => call.method)).toEqual(["GET", "POST", "GET", "GET"]);
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
    await once({ ...matchingInstance(spec), name: "other-postgres" }, /does not match/);
    await once({ ...matchingInstance(spec), region: "us-east1" }, /does not match/);
    await once({ ...matchingInstance(spec), databaseVersion: "POSTGRES_16" }, /does not match/);
    await once(
      {
        ...matchingInstance(spec),
        settings: { ...matchingInstance(spec).settings, tier: "db-custom-2-7680" },
      },
      /does not match/,
    );
    await once(
      {
        ...matchingInstance(spec),
        settings: { ...matchingInstance(spec).settings, edition: "ENTERPRISE_PLUS" },
      },
      /does not match/,
    );
    await once(
      {
        ...matchingInstance(spec),
        settings: {
          ...matchingInstance(spec).settings,
          backupConfiguration: { enabled: true, pointInTimeRecoveryEnabled: false },
        },
      },
      /does not match/,
    );
    await once(
      {
        ...matchingInstance(spec),
        settings: { ...matchingInstance(spec).settings, availabilityType: "REGIONAL" },
      },
      /does not match/,
    );
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

  test("rejects an unsafe 409 follow-up without polling", async () => {
    const spec = privateCloudSqlSpec(input);
    const failed = scripted([
      { status: 404 },
      { status: 409 },
      { status: 200, body: { ...matchingInstance(spec), state: "FAILED" } },
    ]);
    await expect(ensurePrivateCloudSql(spec, credential, failed.transport, fastPoll())).rejects.toThrow(/not runnable/);
    expect(failed.calls.map((call) => call.method)).toEqual(["GET", "POST", "GET"]);

    const publicInstance = scripted([
      { status: 404 },
      { status: 409, body: { error: { message: token } } },
      { status: 200, body: matchingInstance(spec, { ipv4Enabled: true }) },
    ]);
    let caught: Error | undefined;
    try {
      await ensurePrivateCloudSql(spec, credential, publicInstance.transport, fastPoll());
    } catch (error) {
      caught = error as Error;
    }
    expect(caught?.message).toMatch(/refused to adopt/);
    expect(caught?.message).not.toContain(token);
    expect(caught?.message).not.toMatch(/timed out/);
    expect(publicInstance.calls.map((call) => call.method)).toEqual(["GET", "POST", "GET"]);
  });

  test("rejects a runnable instance that has no private address", async () => {
    const spec = privateCloudSqlSpec(input);
    const missing = { ...matchingInstance(spec) };
    delete (missing as { ipAddresses?: unknown }).ipAddresses;
    const empty = scripted([{ status: 200, body: { ...matchingInstance(spec), ipAddresses: [] } }]);
    const absent = scripted([{ status: 200, body: missing }]);
    await expect(ensurePrivateCloudSql(spec, credential, empty.transport, fastPoll())).rejects.toThrow(/does not match/);
    await expect(ensurePrivateCloudSql(spec, credential, absent.transport, fastPoll())).rejects.toThrow(/does not match/);
    expect(empty.calls.map((call) => call.method)).toEqual(["GET"]);
    expect(absent.calls.map((call) => call.method)).toEqual(["GET"]);
  });

  test("rejects PRIVATE labels with malformed or whitespace address values", async () => {
    const spec = privateCloudSqlSpec(input);
    for (const ipAddress of ["", "10.80.0.5 ", "not-an-ip", "999.52.0.5"]) {
      const poster = scripted([
        { status: 200, body: { ...matchingInstance(spec), ipAddresses: [{ type: "PRIVATE", ipAddress }] } },
      ]);
      await expect(ensurePrivateCloudSql(spec, credential, poster.transport, fastPoll())).rejects.toThrow(
        /does not match the private body/,
      );
      expect(poster.calls.map((call) => call.method)).toEqual(["GET"]);
    }
  });

  test("rejects public, mislabeled, or out-of-PSA addresses", async () => {
    const spec = privateCloudSqlSpec(input);
    for (const ipAddress of ["203.0.113.7", "10.52.0.5", "192.168.1.4", "2001:db8::1"]) {
      const poster = scripted([
        { status: 200, body: { ...matchingInstance(spec), ipAddresses: [{ type: "PRIVATE", ipAddress }] } },
      ]);
      await expect(ensurePrivateCloudSql(spec, credential, poster.transport, fastPoll())).rejects.toThrow(
        /does not match the private body/,
      );
    }
    expect(() => privateCloudSqlSpec({ ...input, allocatedIpCidr: "203.0.113.0/24" })).toThrow(/RFC1918/);
    expect(() => privateCloudSqlSpec({ ...input, allocatedIpCidr: "10.80.0.1/20" })).toThrow(/RFC1918/);
    expect(() => privateCloudSqlSpec({ ...input, allocatedIpCidr: "172.16.0.0/8" })).toThrow(/RFC1918/);
    expect(() => privateCloudSqlSpec({ ...input, allocatedIpCidr: "192.168.0.0/8" })).toThrow(/RFC1918/);
  });

  test("polls a pending create that has not received an address yet", async () => {
    const spec = privateCloudSqlSpec(input);
    const pending = { ...matchingInstance(spec), state: "PENDING_CREATE" };
    delete (pending as { ipAddresses?: unknown }).ipAddresses;
    const poster = scripted([
      { status: 404 },
      { status: 409 },
      { status: 200, body: pending },
      { status: 200, body: matchingInstance(spec) },
    ]);
    await expect(ensurePrivateCloudSql(spec, credential, poster.transport, fastPoll())).resolves.toEqual({
      created: false,
      operation: null,
    });
    expect(poster.calls.map((call) => call.method)).toEqual(["GET", "POST", "GET", "GET"]);
  });

  test("aborts a hung lookup before the poll deadline", async () => {
    let aborted = false;
    const transport = {
      request: (_call: SqlAdminEnsureRequest, _credential: SqlAdminCredential, signal?: AbortSignal) =>
        new Promise<SqlAdminEnsureResponse>((_resolve, reject) => {
          signal?.addEventListener("abort", () => {
            aborted = true;
            reject(new Error("aborted"));
          });
        }),
    };
    await expect(
      ensurePrivateCloudSql(privateCloudSqlSpec(input), credential, transport, fastPoll({ requestTimeoutMs: 25 })),
    ).rejects.toThrow(/lookup timed out/);
    expect(aborted).toBe(true);
  });

  test("hard-stops when a transport ignores AbortSignal", async () => {
    let aborted = false;
    const started = Date.now();
    await expect(
      ensurePrivateCloudSql(
        privateCloudSqlSpec(input),
        credential,
        {
          request: (_call, _credential, signal) =>
            new Promise<SqlAdminEnsureResponse>(() => {
              signal?.addEventListener("abort", () => {
                aborted = true;
              });
            }),
        },
        { deadlineMs: 30, requestTimeoutMs: 5000, intervalMs: 5, maxPolls: 10, now: Date.now, sleep: Bun.sleep },
      ),
    ).rejects.toThrow(/lookup timed out/);
    expect(aborted).toBe(true);
    expect(Date.now() - started).toBeLessThan(500);
  });

  test("post-request deadline check catches synchronous event-loop blocking", async () => {
    const blocker = new Int32Array(new SharedArrayBuffer(4));
    const started = performance.now();
    await expect(
      ensurePrivateCloudSql(
        privateCloudSqlSpec(input),
        credential,
        {
          request: async () => {
            // A timer and AbortSignal cannot run while JS is blocked. The
            // monotonic post-request check must still refuse this response.
            Atomics.wait(blocker, 0, 0, 25);
            return { status: 200, body: matchingInstance(privateCloudSqlSpec(input)) };
          },
        },
        { deadlineMs: 10, requestTimeoutMs: 100, intervalMs: 1, maxPolls: 2, now: performance.now, sleep: Bun.sleep },
      ),
    ).rejects.toThrow(/lookup timed out/);
    expect(performance.now() - started).toBeLessThan(500);
  });

  test("uses one outer deadline through lookup, insert, operation, and final confirmation", async () => {
    const spec = privateCloudSqlSpec(input);
    let nowMs = 0;
    const calls: SqlAdminEnsureRequest[] = [];
    const steps: SqlAdminEnsureResponse[] = [
      { status: 404 },
      { status: 200, body: { name: "op-1", status: "PENDING" } },
      { status: 200, body: { status: "DONE" } },
      { status: 200, body: matchingInstance(spec) },
    ];
    await expect(
      ensurePrivateCloudSql(
        spec,
        credential,
        {
          request: async (call) => {
            calls.push(call);
            nowMs += 4;
            return steps.shift()!;
          },
        },
        {
          now: () => nowMs,
          sleep: async (ms) => {
            nowMs += ms;
          },
          deadlineMs: 10,
          requestTimeoutMs: 100,
          intervalMs: 1,
          maxPolls: 10,
        },
      ),
    ).rejects.toThrow(/outcome is uncertain/);
    expect(calls.map((call) => call.method)).toEqual(["GET", "POST", "GET"]);
  });

  test("hard-bounds a 409 adoption sleep that never resolves", async () => {
    const spec = privateCloudSqlSpec(input);
    const poster = scripted([
      { status: 404 },
      { status: 409 },
      { status: 200, body: { ...matchingInstance(spec), state: "PENDING_CREATE" } },
    ]);
    const started = Date.now();
    await expect(
      ensurePrivateCloudSql(spec, credential, poster.transport, {
        now: Date.now,
        sleep: () => new Promise<void>(() => {}),
        deadlineMs: 30,
        intervalMs: 5000,
        maxPolls: 10,
        requestTimeoutMs: 5000,
      }),
    ).rejects.toThrow(/operation timed out/);
    expect(Date.now() - started).toBeLessThan(500);
    expect(poster.calls.map((call) => call.method)).toEqual(["GET", "POST", "GET"]);
  });

  test("rejects invalid poll controls before any request", async () => {
    const spec = privateCloudSqlSpec(input);
    for (const poll of [
      fastPoll({ deadlineMs: 0 }),
      fastPoll({ intervalMs: 0 }),
      fastPoll({ maxPolls: 0 }),
      fastPoll({ requestTimeoutMs: 0 }),
      fastPoll({ deadlineMs: Number.NaN }),
    ]) {
      const poster = scripted([]);
      await expect(ensurePrivateCloudSql(spec, credential, poster.transport, poll)).rejects.toThrow(/Nothing was created/);
      expect(poster.calls).toHaveLength(0);
    }
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

    const missingStatus = await capture(
      [{ status: 404 }, { status: 200, body: { name: "op-1" } }],
      /operation is malformed/,
    );
    expect(missingStatus).toEqual([`GET ${instanceUrl}`, `POST ${insertUrl}`]);

    const invalidOperationName = await capture(
      [{ status: 404 }, { status: 200, body: { name: `https://evil.example/operations/op-1?t=${token}`, status: "PENDING" } }],
      /outcome is uncertain/,
    );
    expect(invalidOperationName).toEqual([`GET ${instanceUrl}`, `POST ${insertUrl}`]);

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

    const refusedLookup = await capture([{ status: 403, body: { error: token } }], /Nothing was created/);
    expect(refusedLookup).toEqual([`GET ${instanceUrl}`]);

    const insert = await capture([{ status: 404 }, "throw"], /outcome is uncertain/);
    expect(insert).toEqual([`GET ${instanceUrl}`, `POST ${insertUrl}`]);
    expect(insert.join(" ")).not.toContain(token);

    const refusedInsert = await capture(
      [{ status: 404 }, { status: 403, body: { error: token } }],
      /outcome is uncertain/,
    );
    expect(refusedInsert).toEqual([`GET ${instanceUrl}`, `POST ${insertUrl}`]);

    const refusedPoll = await capture(
      [
        { status: 404 },
        { status: 200, body: { name: "op-1", status: "PENDING" } },
        { status: 503, body: { error: token } },
      ],
      /outcome is uncertain/,
    );
    expect(refusedPoll).toEqual([`GET ${instanceUrl}`, `POST ${insertUrl}`, `GET ${operationUrl}`]);

    const malformedPoll = await capture(
      [
        { status: 404 },
        { status: 200, body: { name: "op-1", status: "PENDING" } },
        { status: 200, body: {} },
      ],
      /operation is malformed/,
    );
    expect(malformedPoll).toEqual([`GET ${instanceUrl}`, `POST ${insertUrl}`, `GET ${operationUrl}`]);

    const refusedConfirmation = await capture(
      [
        { status: 404 },
        { status: 200, body: { name: "op-1", status: "DONE" } },
        { status: 404, body: { error: token } },
      ],
      /outcome is uncertain/,
    );
    expect(refusedConfirmation).toEqual([`GET ${instanceUrl}`, `POST ${insertUrl}`, `GET ${instanceUrl}`]);

    await expect(ensurePrivateCloudSql(privateCloudSqlSpec(input), { bearer: "" }, scripted([]).transport, fastPoll())).rejects.toThrow(
      /credential is missing/,
    );
  });
});
