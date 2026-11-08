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
import { TENANT_SETTINGS_SQL } from "../liveness.ts";
import { assertReaderRole, readApiDepsFromSecret, READER_LOGIN_SQL, READER_PG16_SQL } from "../lambda.ts";
import { READER_LOGIN_ROW, readerLoginAnswer } from "./reader-login-support.ts";
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
type RoleRow = Record<string, unknown>;
const READER: RoleRow = { ...READER_LOGIN_ROW };
let roleRows: RoleRow[] = [READER];
let roleChecks = 0;
const isAdmission = (text: string) => text === READER_LOGIN_SQL || text === READER_PG16_SQL;

/** The startup login check reads `roleRows` (and the PG16 row); every other query answers no rows. */
const answer = async (text: string) => ({ rows: text === READER_LOGIN_SQL ? roleRows : readerLoginAnswer(text) ?? [] });

/** Records the pool config and every statement other than the startup role check. */
class RecordingPool {
  constructor(config: pg.PoolConfig) { configs.push(config); }
  on() { return this; }
  async query(text: string, params?: unknown[]) { if (text === READER_LOGIN_SQL) roleChecks++; else if (!isAdmission(text)) sql.push({ text, params }); return answer(text); }
  async connect() {
    return { query: async (text: string, params?: unknown[]) => { sql.push({ text, params }); return answer(text); }, release() {} };
  }
  async end() {}
}

const running: ReadApiService[] = [];
beforeEach(() => {
  configs = [];
  sql = [];
  roleRows = [READER];
  roleChecks = 0;
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
    // The query went to the Postgres search index, bound to the same tenant,
    // inside a tenant transaction whose settings name that tenant (forced RLS).
    const at = sql.findIndex(s => s.text.includes('FROM "search_index"."transcript_events"'));
    expect(sql.filter(s => s.text.includes('FROM "search_index"."transcript_events"'))).toHaveLength(1);
    expect(sql[at]!.params?.slice(0, 3)).toEqual(["org-a", "ws-a", "%hello%"]);
    expect(sql.slice(at - 2, at).map(s => [s.text, s.params])).toEqual([["BEGIN READ ONLY", undefined], [TENANT_SETTINGS_SQL, ["org-a", "ws-a"]]]);
    // A caller's ?workspace= can never widen the tenant: a different workspace is refused before any SQL.
    sql = [];
    const other = await app.request("/v1/events?workspace=ws-b&q=hello&limit=10", { headers: { authorization: "Bearer synthetic" } });
    expect(other.status).toBeGreaterThanOrEqual(400);
    expect(JSON.stringify(sql)).not.toContain("ws-b");
    await built.close();
  });
});

describe("a fresh self-host before the search owner bootstrap and reader grants", () => {
  test("a missing relation (42P01) or missing SELECT grant (42501) answers 503 backend_unavailable, naming nothing", async () => {
    for (const code of ["42P01", "42501"]) {
      sql = [];
      const fail = async (text: string, params?: unknown[]) => {
        sql.push({ text, params });
        if (text.includes("search_index")) throw Object.assign(new Error(`relation "search_index".x: ${code}`), { code });
        return answer(text);
      };
      pg.Pool = class extends RecordingPool {
        override query = fail;
        override async connect() { return { query: fail, release() {} }; }
      } as unknown as typeof pg.Pool;
      const built = await readApiDepsFromSecret(ENV.RELAYHISTORY_READ_API_SECRET, ENV, selfhostSearchComposition(ENV), { entry: "service" });
      const app = createReadApi({ ...built.deps, verify: async () => ({ userId: "u-1", orgId: "org-a", workspaceId: "ws-a", scopes: ["rth:read"] }) });
      const r = await app.request("/v1/events?workspace=ws-a&q=hello&limit=10", { headers: { authorization: "Bearer synthetic" } });
      expect(r.status).toBe(503);
      const text = await r.text();
      expect(JSON.parse(text).error.code).toBe("backend_unavailable");
      expect(text).not.toContain("search_index");
      expect(text).not.toContain(code);
      await built.close();
    }
  });
});

describe("reader admission: the read login must be bound by row-level security", () => {
  test("a superuser or BYPASSRLS login refuses startup naming the property, never the URL or an observed role", async () => {
    for (const [row, code] of [
      [{ ...READER, rolsuper: true }, "reader_role_superuser"],
      [{ ...READER, rolbypassrls: true }, "reader_role_bypassrls"],
      [{ ...READER, rolsuper: true, rolbypassrls: true }, "reader_role_superuser"],
    ] as const) {
      roleRows = [row];
      const error = await startReadApiService(ENV, LOOPBACK).then(() => null, (e: Error) => e);
      expect(error).toMatchObject({ name: "ReaderRoleError", code });
      expect(error!.message).toBe(`session read api: ${code} (the read login must be bulls96_read_api)`);
      for (const secret of ["pw", "db.selfhost.internal", "postgresql:"]) expect(error!.message).not.toContain(secret);
    }
    expect(sql).toEqual([]); // refused before any search, state or directory SQL
  });

  test("an unreadable role (no row, unexpected shape, query failure) refuses rather than admits", async () => {
    for (const rows of [[], [{ role: "search_reader" }], [{ ...READER, rolbypassrls: null }], [READER, READER], [{ ...READER, server_version: "170004" }]] as unknown as RoleRow[][]) {
      roleRows = rows;
      const error = await startReadApiService(ENV, LOOPBACK).then(() => null, (e: Error) => e);
      expect(error?.name).toBe("ReaderRoleError");
    }
    const boom = Object.assign(new Error("permission denied for table pg_authid"), { code: "42501" });
    await expect(assertReaderRole({ query: async () => { throw boom; } })).rejects.toBe(boom);
  });

  test("a NOSUPERUSER NOBYPASSRLS login is admitted; the hosted ClickHouse Lambda does not run the check, a Postgres-bound one does", async () => {
    const get = await start(ENV);
    expect(await (await get("/healthz")).json()).toEqual({ ok: true, enabled: true });
    expect(roleChecks).toBe(1); // once, at startup, on the read pool
    roleRows = [{ ...READER, rolbypassrls: true }];
    const built = await readApiDepsFromSecret(JSON.stringify({
      supabase_url: PG_URL,
      clickhouse: { url: "https://ch.example.invalid:8443", user: "reader", password: "p", database: "relay_search" },
    }), { READ_API_COMPLETE_SINCE: "epoch" });
    expect(roleChecks).toBe(1); // the hosted ClickHouse binding: unchanged
    await built.close();
    // A Lambda composed with the portable Postgres binding is checked as well.
    const lambdaPg = { READ_API_COMPLETE_SINCE: "epoch", SEARCH_INDEX_BACKEND: "postgres", SEARCH_INDEX_POSTGRES_BACKEND_ID: "selfhost-pg" };
    await expect(readApiDepsFromSecret(ENV.RELAYHISTORY_READ_API_SECRET, lambdaPg, selfhostSearchComposition(lambdaPg))).rejects.toMatchObject({ code: "reader_role_bypassrls" });
    expect(roleChecks).toBe(2);
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
