import { describe, expect, test } from "bun:test";
import { fetchRedisServerCa, redisAdminFetchTransport, type RedisAdminTransport } from "../redis-ca.ts";

const input = {
  projectId: "agentrelay-96bulls-soc2-2610",
  region: "us-central1",
  instanceId: "awf-conformance-redis",
};
const cert = "-----BEGIN CERTIFICATE-----\nQUJDRA==\n-----END CERTIFICATE-----\n";
const ready = {
  state: "READY",
  transitEncryptionMode: "SERVER_AUTHENTICATION",
  authEnabled: false,
  serverCaCerts: [{ cert }],
};

describe("Memorystore service CA", () => {
  test("returns only validated public certificates from a ready passwordless TLS instance", async () => {
    let requested = "";
    const transport: RedisAdminTransport = {
      async get(url) {
        requested = url;
        return { status: 200, body: ready };
      },
    };
    expect(await fetchRedisServerCa(input, { bearer: "boundary-token-at-least-sixteen" }, transport))
      .toEqual({ caPem: cert, certificateCount: 1 });
    expect(requested).toBe("https://redis.googleapis.com/v1/projects/agentrelay-96bulls-soc2-2610/locations/us-central1/instances/awf-conformance-redis");
    expect(await fetchRedisServerCa(input, { bearer: "boundary-token-at-least-sixteen" }, {
      async get() { return { status: 200, body: { ...ready, authEnabled: undefined } }; },
    })).toEqual({ caPem: cert, certificateCount: 1 });
  });

  test("fails closed for wrong state, AUTH, TLS mode, private keys, or duplicates", async () => {
    for (const body of [
      { ...ready, state: "CREATING" },
      { ...ready, authEnabled: true },
      { ...ready, authString: "must-not-leak" },
      { ...ready, transitEncryptionMode: "DISABLED" },
      { ...ready, serverCaCerts: [{ cert: "-----BEGIN PRIVATE KEY-----\nnope" }] },
      { ...ready, serverCaCerts: [{ cert }, { cert }] },
    ]) {
      await expect(fetchRedisServerCa(input, { bearer: "boundary-token-at-least-sixteen" }, {
        async get() { return { status: 200, body }; },
      })).rejects.toThrow(/Nothing was created or changed/);
    }
  });

  test("attaches bearer credentials only after exact host and path validation", async () => {
    let authorization = "";
    const transport = redisAdminFetchTransport(async (_url, init) => {
      authorization = new Headers(init?.headers).get("authorization") ?? "";
      return new Response(JSON.stringify(ready), { status: 200 });
    });
    expect(await fetchRedisServerCa(input, { bearer: "boundary-token-at-least-sixteen" }, transport))
      .toEqual({ caPem: cert, certificateCount: 1 });
    expect(authorization).toBe("Bearer boundary-token-at-least-sixteen");
    await expect(transport.get("https://attacker.example/v1/projects/x", { bearer: "boundary-token-at-least-sixteen" }))
      .rejects.toThrow(/not approved/);
  });

  test("bounds and sanitizes a hung or failed lookup", async () => {
    await expect(fetchRedisServerCa(input, { bearer: "boundary-token-at-least-sixteen" }, {
      async get() { return new Promise(() => {}); },
    }, 5)).rejects.toThrow("Redis service CA lookup failed");
    await expect(fetchRedisServerCa(input, { bearer: "boundary-token-at-least-sixteen" }, {
      async get() { throw new Error("sensitive upstream body"); },
    })).rejects.toThrow("Redis service CA lookup failed");
  });
});
