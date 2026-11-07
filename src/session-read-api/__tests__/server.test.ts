/**
 * The self-host SERVICE entry (server.ts) over real loopback HTTP on a random
 * port: dormant answers /healthz only; enabled serves the read API, both with
 * the test adapters and with the Lambda's own assembly (no database is touched:
 * pg and ClickHouse connect lazily and the routes hit here never query).
 */
import { afterEach, describe, expect, test } from "bun:test";
import type { ClickHouseClient } from "../../session-pipeline/vendor/search-index/clickhouse.ts";
import { AuthError, createReadApi, type VerifiedIdentity } from "../app.ts";
import { startReadApiService, type ReadApiService } from "../server.ts";
import { memoryLiveness } from "./support.ts";

const ID: VerifiedIdentity = { userId: "u-1", orgId: "org-a", workspaceId: "ws-a", scopes: ["rth:read"] };
const LOOPBACK = { host: "127.0.0.1", port: 0 };

const running: ReadApiService[] = [];
afterEach(async () => {
  for (const s of running.splice(0)) await s.close();
});

async function start(env: NodeJS.ProcessEnv, build?: Parameters<typeof startReadApiService>[1]["build"]) {
  const service = await startReadApiService(env, { ...LOOPBACK, build });
  running.push(service);
  const url = (path: string) => `http://127.0.0.1:${service.port}${path}`;
  return { service, url };
}

function stubCh(): ClickHouseClient {
  return {
    database: "relay_search",
    async command() {},
    async insert() {},
    async select<T>() {
      return [] as T[];
    },
  };
}

describe("dormant (flag unset, empty or false)", () => {
  test.each([undefined, "", "false"])("%p: /healthz 200 enabled:false, every other route 503 not_configured", async (flag) => {
    let built = false;
    const { service, url } = await start({ RELAYHISTORY_SELFHOST_READ_API: flag }, async () => {
      built = true;
      throw new Error("must not build");
    });
    expect(service.enabled).toBe(false);
    expect(built).toBe(false);
    const health = await fetch(url("/healthz"));
    expect(health.status).toBe(200);
    expect(await health.json()).toEqual({ ok: true, enabled: false });
    for (const [path, method] of [["/v1/events?q=x", "GET"], ["/health", "GET"], ["/", "GET"], ["/v1/sessions", "POST"]] as const) {
      const r = await fetch(url(path), { method, headers: { authorization: "Bearer good" } });
      expect(r.status).toBe(503);
      expect(r.headers.get("retry-after")).toBe("60");
      expect(((await r.json()) as { error: { code: string } }).error.code).toBe("not_configured");
    }
  });

  test("a bad flag value refuses to start", async () => {
    await expect(startReadApiService({ RELAYHISTORY_SELFHOST_READ_API: "yes" }, LOOPBACK)).rejects.toThrow(
      "RELAYHISTORY_SELFHOST_READ_API must be true or false",
    );
  });
});

describe("enabled", () => {
  const env = { RELAYHISTORY_SELFHOST_READ_API: "true", RELAYHISTORY_READ_API_SECRET: "{}" };

  test("serves the read API through the server entry with the test adapters", async () => {
    let closed = false;
    const { service, url } = await start(env, async (secret) => {
      expect(secret).toBe("{}");
      const app = createReadApi({
        verify: async (t) => {
          if (t !== "good") throw new AuthError(401, "invalid_token", "token is not valid");
          return ID;
        },
        ch: stubCh(),
        liveness: memoryLiveness({ createdAt: { "codex\u0000s-1": Date.parse("2026-10-01T00:00:00Z") } }).liveness,
        completeSinceMs: 0,
      });
      return { app, close: async () => void (closed = true) };
    });
    expect(service.enabled).toBe(true);

    const health = await fetch(url("/healthz"));
    expect(health.status).toBe(200);
    expect(await health.json()).toEqual({ ok: true, enabled: true });

    const ok = await fetch(url("/v1/sessions/s-1/events?workspace=ws-a"), { headers: { authorization: "Bearer good" } });
    expect(ok.status).toBe(200);
    expect(ok.headers.get("x-sessions-read-backend")).toBe("96bulls");
    expect(ok.headers.get("x-relayhistory-workspace-id")).toBe("ws-a");
    expect(((await ok.json()) as { events: unknown[] }).events).toEqual([]);

    const denied = await fetch(url("/v1/sessions/s-1/events?workspace=ws-a"), { headers: { authorization: "Bearer bad" } });
    expect(denied.status).toBe(401);
    expect(((await denied.json()) as { error: { code: string } }).error.code).toBe("invalid_token");

    // GET only, like the Lambda and the Worker.
    const post = await fetch(url("/v1/sessions/s-1/events?workspace=ws-a"), { method: "POST", body: "x", headers: { authorization: "Bearer good" } });
    expect(post.status).toBe(405);
    expect(post.headers.get("allow")).toBe("GET");

    await service.close();
    running.splice(0);
    expect(closed).toBe(true);
  });

  test("the Lambda's own assembly starts without a database and authenticates first", async () => {
    const { url } = await start({
      ...env,
      RELAYHISTORY_READ_API_SECRET: JSON.stringify({
        supabase_url: "postgresql://reader@db.example.invalid:5432/postgres",
        clickhouse: { url: "https://ch.example.invalid:8443", user: "reader", password: "p", database: "relay_search" },
      }),
      READ_API_COMPLETE_SINCE: "epoch",
    });
    expect(await (await fetch(url("/healthz"))).json()).toEqual({ ok: true, enabled: true });
    expect((await fetch(url("/health"))).status).toBe(200);
    const r = await fetch(url("/v1/events?q=x"));
    expect(r.status).toBe(401);
    expect(((await r.json()) as { error: { code: string } }).error.code).toBe("missing_authorization");
  });

  test("startup fails on the Lambda's own checks (bad secret, missing history floor)", async () => {
    await expect(startReadApiService({ ...env, READ_API_COMPLETE_SINCE: "epoch", RELAYHISTORY_READ_API_SECRET: "not json" }, LOOPBACK)).rejects.toThrow(
      "session read api secret is not JSON",
    );
    await expect(
      startReadApiService({ ...env, RELAYHISTORY_READ_API_SECRET: JSON.stringify({ supabase_url: "postgresql://r@h/p" }) }, LOOPBACK),
    ).rejects.toThrow("READ_API_COMPLETE_SINCE");
  });
});
