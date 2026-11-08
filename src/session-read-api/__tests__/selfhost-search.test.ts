/**
 * Self-host SERVICE search selection (server.ts selfhostReadApp): with
 * SEARCH_INDEX_BACKEND=postgres (the service default) the app starts from a
 * Postgres-only secret, with no ClickHouse block, no Lambda env and any known
 * PIPELINE_RUNTIME, and its search route queries the registry's Postgres
 * SearchIndexAdapter. Unknown or unrunnable names refuse startup without
 * echoing the value. No database is touched: pg.Pool is replaced by a recorder.
 */
import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import pg from "pg";
import { createReadApi } from "../app.ts";
import { readApiDepsFromSecret } from "../lambda.ts";
import { selfhostSearchComposition, startReadApiService, type ReadApiService } from "../server.ts";

const LOOPBACK = { host: "127.0.0.1", port: 0 };
const PG_URL = "postgresql://reader:pw@db.selfhost.internal:6543/history?sslmode=disable&application_name=x&statement_timeout=0&options=-c%20statement_timeout%3D0";
const ENV = {
  RELAYHISTORY_SELFHOST_READ_API: "true",
  RELAYHISTORY_READ_API_SECRET: JSON.stringify({ supabase_url: PG_URL }),
  READ_API_COMPLETE_SINCE: "epoch",
  SESSION_POSTGRES_TLS_PROFILE: "supabase",
  SEARCH_INDEX_POSTGRES_BACKEND_ID: "selfhost-pg",
} as const;

type Recorded = { text: string; params?: unknown[] };
let configs: pg.PoolConfig[] = [];
let sql: Recorded[] = [];
const RealPool = pg.Pool;

/** Records the pool config and every statement; answers every query with no rows. */
class RecordingPool {
  constructor(config: pg.PoolConfig) { configs.push(config); }
  on() { return this; }
  async query(text: string, params?: unknown[]) { sql.push({ text, params }); return { rows: [] }; }
  async connect() {
    return { query: async (text: string, params?: unknown[]) => { sql.push({ text, params }); return { rows: [] }; }, release() {} };
  }
  async end() {}
}

const running: ReadApiService[] = [];
beforeEach(() => {
  configs = [];
  sql = [];
  pg.Pool = RecordingPool as unknown as typeof pg.Pool;
});
afterEach(async () => {
  for (const s of running.splice(0)) await s.close();
  pg.Pool = RealPool;
});

async function start(env: NodeJS.ProcessEnv) {
  const service = await startReadApiService(env, LOOPBACK);
  running.push(service);
  return (path: string) => fetch(`http://127.0.0.1:${service.port}${path}`);
}

describe("SEARCH_INDEX_BACKEND=postgres (the service default)", () => {
  test("starts from a Postgres-only secret: no ClickHouse block, no Lambda env, any known PIPELINE_RUNTIME", async () => {
    for (const extra of [{}, { SEARCH_INDEX_BACKEND: "postgres" }, { PIPELINE_RUNTIME: "workers" }, { SEARCH_INDEX_BACKEND: "" }]) {
      configs = [];
      const get = await start({ ...ENV, ...extra });
      expect(await (await get("/healthz")).json()).toEqual({ ok: true, enabled: true });
      const r = await get("/v1/events?q=x");
      expect(r.status).toBe(401);
      expect(configs).toHaveLength(1);
    }
    expect(sql).toEqual([]); // authentication comes first; nothing queried
  });

  test("the read pool gets discrete fields and explicit verified TLS; nothing from the URL query reaches pg", async () => {
    await start(ENV);
    const [config] = configs;
    expect(config).toMatchObject({ host: "db.selfhost.internal", port: 6543, user: "reader", password: "pw", database: "history", statement_timeout: 2000, options: "-c statement_timeout=2000" });
    expect(config!.connectionString).toBeUndefined();
    expect(config!.ssl).toMatchObject({ rejectUnauthorized: true });
    expect(typeof (config!.ssl as { ca: string }).ca).toBe("string");
  });

  test("refuses startup without an explicit TLS profile, backend id, or URL credentials; never echoes values", async () => {
    const { SESSION_POSTGRES_TLS_PROFILE: _t, ...noTls } = ENV;
    await expect(startReadApiService(noTls, LOOPBACK)).rejects.toThrow(/SESSION_POSTGRES_TLS_PROFILE/);
    await expect(startReadApiService({ ...ENV, SESSION_POSTGRES_TLS_PROFILE: "none" }, LOOPBACK)).rejects.toThrow("PostgreSQL TLS configuration is invalid");
    const { SEARCH_INDEX_POSTGRES_BACKEND_ID: _b, ...noId } = ENV;
    await expect(startReadApiService(noId, LOOPBACK)).rejects.toThrow(/SEARCH_INDEX_POSTGRES_BACKEND_ID/);
    await expect(startReadApiService({ ...ENV, SEARCH_INDEX_POSTGRES_BACKEND_ID: "bad id;--" }, LOOPBACK)).rejects.toThrow("search state configuration invalid");
    configs = []; // that check runs in the composition after the lazy pool is built (and ended); the rest refuse before it
    for (const url of ["postgresql://reader@db.selfhost.internal/history", "postgresql://reader:pw@db.selfhost.internal/", "postgresql://:pw@db.selfhost.internal/history"]) {
      const error = await startReadApiService({ ...ENV, RELAYHISTORY_READ_API_SECRET: JSON.stringify({ supabase_url: url }) }, LOOPBACK).then(() => null, (e: Error) => e);
      expect(error?.message).toBe("session read api secret supabase_url needs a user, password and database");
    }
    expect(configs).toEqual([]);
  });

  test("the search route queries the registry's Postgres adapter, tenant-scoped", async () => {
    const composition = selfhostSearchComposition(ENV);
    const built = await readApiDepsFromSecret(ENV.RELAYHISTORY_READ_API_SECRET, ENV, composition, { entry: "service" });
    expect(built.deps.ch).toBeUndefined();
    expect(built.deps.adapter).toBeDefined();
    const app = createReadApi({ ...built.deps, verify: async () => ({ userId: "u-1", orgId: "org-a", workspaceId: "ws-a", scopes: ["rth:read"] }) });
    const r = await app.request("/v1/events?workspace=ws-a&q=hello&limit=10", { headers: { authorization: "Bearer synthetic" } });
    expect(r.status).toBe(200);
    expect(((await r.json()) as { events: unknown[] }).events).toEqual([]);
    // Completeness from the Postgres projection state under the configured backend id, in the caller's tenant only.
    const partials = sql.find(s => s.text.includes('"search_index".projection_state'));
    expect(partials?.params?.slice(0, 3)).toEqual(["org-a", "ws-a", "selfhost-pg"]);
    // The query went to the Postgres search index, bound to the same tenant.
    const search = sql.filter(s => s.text.includes('FROM "search_index"."transcript_events"'));
    expect(search).toHaveLength(1);
    expect(search[0]!.params?.slice(0, 3)).toEqual(["org-a", "ws-a", "%hello%"]);
    // A caller's ?workspace= can never widen the tenant: a different workspace is refused before any SQL.
    sql = [];
    const other = await app.request("/v1/events?workspace=ws-b&q=hello&limit=10", { headers: { authorization: "Bearer synthetic" } });
    expect(other.status).toBeGreaterThanOrEqual(400);
    expect(JSON.stringify(sql)).not.toContain("ws-b");
    await built.close();
  });
});

describe("SEARCH_INDEX_BACKEND selection", () => {
  test("an unknown or unrunnable name refuses startup naming the variable, never the value", async () => {
    for (const value of ["nope-synthetic-value", "opensearch", "elasticsearch", "Postgres"]) {
      const error = await startReadApiService({ ...ENV, SEARCH_INDEX_BACKEND: value }, LOOPBACK).then(() => null, (e: Error) => e);
      expect(error?.message).toMatch(/^SEARCH_INDEX_BACKEND: /);
      expect(error!.message).not.toContain(value);
    }
    await expect(startReadApiService({ ...ENV, PIPELINE_RUNTIME: "nope" }, LOOPBACK)).rejects.toThrow(/^PIPELINE_RUNTIME: unknown value/);
    expect(configs).toEqual([]);
  });

  test("the hosted Lambda entry is unchanged: ClickHouse default, lambda runtime only, connectionString pool", async () => {
    const lambdaEnv = { READ_API_COMPLETE_SINCE: "epoch" };
    await expect(readApiDepsFromSecret(ENV.RELAYHISTORY_READ_API_SECRET, lambdaEnv)).rejects.toThrow("session read api secret clickhouse.url is missing");
    await expect(readApiDepsFromSecret(ENV.RELAYHISTORY_READ_API_SECRET, { ...lambdaEnv, PIPELINE_RUNTIME: "workers" })).rejects.toThrow(/^PIPELINE_RUNTIME: not allowed/);
    await expect(readApiDepsFromSecret(ENV.RELAYHISTORY_READ_API_SECRET, { ...lambdaEnv, SEARCH_INDEX_BACKEND: "postgres" })).rejects.toThrow(/^SEARCH_INDEX_BACKEND: not allowed/);
    const built = await readApiDepsFromSecret(JSON.stringify({
      supabase_url: PG_URL,
      clickhouse: { url: "https://ch.example.invalid:8443", user: "reader", password: "p", database: "relay_search" },
    }), lambdaEnv);
    expect(built.deps.adapter).toBeUndefined();
    expect(built.deps.ch).toBeDefined();
    expect(configs.at(-1)!.connectionString).toStartWith("postgresql://reader:pw@db.selfhost.internal:6543/history");
    await built.close();
  });

  test("clickhouse keeps the hosted binding and needs the secret's clickhouse block", async () => {
    const env = { ...ENV, SEARCH_INDEX_BACKEND: "clickhouse" };
    expect(selfhostSearchComposition(env)).toBeUndefined();
    await expect(startReadApiService(env, LOOPBACK)).rejects.toThrow("session read api secret clickhouse.url is missing");
    const get = await start({
      ...env,
      RELAYHISTORY_READ_API_SECRET: JSON.stringify({
        supabase_url: PG_URL,
        clickhouse: { url: "https://ch.example.invalid:8443", user: "reader", password: "p", database: "relay_search" },
      }),
    });
    expect((await get("/v1/events?q=x")).status).toBe(401);
  });
});
