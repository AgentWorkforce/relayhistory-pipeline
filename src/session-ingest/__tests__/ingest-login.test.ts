/** Genuine-login identity assertion (ingest-login.ts).
 *
 * Pure decisions over rows recorded from real PostgreSQL 15.19, 16.15 and 17.11 (official
 * postgres images, scram-sha-256, loopback, 2026-11-08) always run. The live suite runs the
 * same probes, and the portable runtime itself, against real servers when
 * INGEST_LOGIN_TEST_PG_URLS lists disposable owner URLs (comma-separated, loopback only).
 * It creates bulls96_session_ingest with a fixture-only password if absent.
 */
import { describe, expect, test } from "bun:test";
import pg from "pg";
import { ingestLoginIdentity, ingestLoginIdentityRefusal, INGEST_LOGIN_IDENTITY_SQL } from "../ingest-login.ts";
import { runPortablePublicationRuntime } from "../publication-portable.ts";

const R = "bulls96_session_ingest";
// Recorded rows: [login row, PG16+ row].
const RECORDED: Record<string, Record<string, [Record<string, unknown>, Record<string, unknown> | undefined]>> = {
  "15": {
    genuine: [{ session_user: R, current_user: R, role_setting: "none", server_version: 150019, backend_user: R }, undefined],
    ssa: [{ session_user: R, current_user: R, role_setting: "none", server_version: 150019, backend_user: "postgres" }, undefined],
    setrole: [{ session_user: "postgres", current_user: R, role_setting: R, server_version: 150019, backend_user: "postgres" }, undefined],
    ssaSetrole: [{ session_user: R, current_user: R, role_setting: R, server_version: 150019, backend_user: "postgres" }, undefined],
  },
  "16": {
    genuine: [{ session_user: R, current_user: R, role_setting: "none", server_version: 160015, backend_user: R }, { system_user: `scram-sha-256:${R}` }],
    ssa: [{ session_user: R, current_user: R, role_setting: "none", server_version: 160015, backend_user: "postgres" }, { system_user: "scram-sha-256:postgres" }],
    setrole: [{ session_user: "postgres", current_user: R, role_setting: R, server_version: 160015, backend_user: "postgres" }, { system_user: "scram-sha-256:postgres" }],
    ssaSetrole: [{ session_user: R, current_user: R, role_setting: R, server_version: 160015, backend_user: "postgres" }, { system_user: "scram-sha-256:postgres" }],
  },
  "17": {
    genuine: [{ session_user: R, current_user: R, role_setting: "none", server_version: 170011, backend_user: R }, { system_user: `scram-sha-256:${R}` }],
    ssa: [{ session_user: R, current_user: R, role_setting: "none", server_version: 170011, backend_user: "postgres" }, { system_user: "scram-sha-256:postgres" }],
    setrole: [{ session_user: "postgres", current_user: R, role_setting: R, server_version: 170011, backend_user: "postgres" }, { system_user: "scram-sha-256:postgres" }],
    ssaSetrole: [{ session_user: R, current_user: R, role_setting: R, server_version: 170011, backend_user: "postgres" }, { system_user: "scram-sha-256:postgres" }],
  },
};

test("recorded real-server rows: only a genuine login admits; SET SESSION AUTHORIZATION and SET ROLE refuse on 15, 16 and 17", () => {
  for (const [v, rows] of Object.entries(RECORDED)) {
    expect([v, ingestLoginIdentityRefusal(...rows.genuine!)]).toEqual([v, null]);
    // PG15 has no system_user and SET SESSION AUTHORIZATION leaves role='none': only the backend's usename shows it.
    expect([v, ingestLoginIdentityRefusal(...rows.ssa!)]).toEqual([v, "backend_user"]);
    expect([v, ingestLoginIdentityRefusal(...rows.setrole!)]).toEqual([v, "session_user"]);
    expect([v, ingestLoginIdentityRefusal(...rows.ssaSetrole!)]).toEqual([v, "set_role"]);
  }
});

test("PG16+ system_user: required, non-null, <method>:<role> exactly; checked even when the backend row agrees", () => {
  const [login] = RECORDED["16"]!.genuine!;
  for (const [su, want] of [
    [`scram-sha-256:${R}`, null], [`cert:${R}`, null], [`md5:${R}`, null],
    [null, "system_user"], [R, "system_user"], [`:${R}`, "system_user"], [`scram-sha-256:${R}x`, "system_user"],
    ["scram-sha-256:postgres", "system_user"], [`scram-sha-256:${R.toUpperCase()}`, "system_user"],
  ] as const) expect([su, ingestLoginIdentityRefusal(login, { system_user: su })]).toEqual([su, want]);
  expect(ingestLoginIdentityRefusal(login, undefined)).toBe("login_unreadable");
});

test("unreadable or drifted rows refuse", () => {
  const [login] = RECORDED["15"]!.genuine!;
  expect(ingestLoginIdentityRefusal(undefined, undefined)).toBe("login_unreadable");
  for (const v of [undefined, "150019", 90000, 1.5]) expect(ingestLoginIdentityRefusal({ ...login, server_version: v }, undefined)).toBe("login_unreadable");
  expect(ingestLoginIdentityRefusal({ ...login, backend_user: null }, undefined)).toBe("backend_user");
  expect(ingestLoginIdentityRefusal({ ...login, role_setting: undefined }, undefined)).toBe("set_role");
});

test("probe runner: one row each, failures are unreadable, aborts rethrow, PG16 row only on 16+", async () => {
  const seen: string[] = [];
  const runner = (rows: Record<string, unknown>[][]) => async (sql: string) => { seen.push(sql); return { rows: rows.shift() ?? [] }; };
  expect(await ingestLoginIdentity(runner([[RECORDED["15"]!.genuine![0]]]))).toBeNull();
  expect(seen).toEqual([INGEST_LOGIN_IDENTITY_SQL]);
  expect(await ingestLoginIdentity(runner([[RECORDED["17"]!.genuine![0]], [RECORDED["17"]!.genuine![1]!]]))).toBeNull();
  expect(await ingestLoginIdentity(runner([[RECORDED["17"]!.genuine![0]], []]))).toBe("login_unreadable");
  expect(await ingestLoginIdentity(runner([[], []]))).toBe("login_unreadable");
  expect(await ingestLoginIdentity(async () => { throw Object.assign(new Error("x"), { code: "42501" }); })).toBe("login_unreadable");
  const abort = Object.assign(new Error("aborted"), { name: "AbortError" });
  await expect(ingestLoginIdentity(async () => { throw abort; })).rejects.toBe(abort);
});

const URLS = (process.env.INGEST_LOGIN_TEST_PG_URLS ?? "").split(",").filter(Boolean);
describe.skipIf(!URLS.length)("genuine login against real PostgreSQL", () => {
  const PW = "fixture-ingest-only";
  const target = (url: string) => { const u = new URL(url); if (!["127.0.0.1", "localhost"].includes(u.hostname)) throw new Error("loopback only"); return u; };
  const ensureRole = async (url: string) => {
    const c = new pg.Client({ connectionString: url }); await c.connect();
    await c.query(`DO $$BEGIN IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='${R}') THEN CREATE ROLE ${R} LOGIN NOINHERIT PASSWORD '${PW}'; END IF; END$$`);
    await c.end();
  };
  const probe = async (client: pg.Client) => ingestLoginIdentity(async (sql) => client.query(sql) as never);
  for (const url of URLS) {
    test(`${target(url).port}: genuine login admits; superuser SET SESSION AUTHORIZATION and SET ROLE refuse`, async () => {
      await ensureRole(url);
      const u = target(url);
      const ingest = new pg.Client({ host: u.hostname, port: Number(u.port), user: R, password: PW, database: u.pathname.slice(1) || "postgres" });
      await ingest.connect();
      expect(await probe(ingest)).toBeNull();
      await ingest.end();
      const owner = new pg.Client({ connectionString: url }); await owner.connect();
      await owner.query(`SET SESSION AUTHORIZATION ${R}`);
      expect((await owner.query("SELECT session_user::text s, current_user::text c, current_setting('role') r")).rows[0]).toEqual({ s: R, c: R, r: "none" });
      expect(await probe(owner)).toBe("backend_user");
      await owner.query(`RESET SESSION AUTHORIZATION; SET ROLE ${R}`);
      expect(await probe(owner)).toBe("session_user");
      await owner.end();
    });

    test(`${target(url).port}: the portable runtime refuses a superuser SET SESSION AUTHORIZATION pool before verifiers, store or RPC; a genuine login passes identity`, async () => {
      await ensureRole(url);
      const u = target(url), database = u.pathname.slice(1) || "postgres";
      const env = {
        INGEST_PUBLISH_ENABLED: "true", INGEST_PUBLICATION_TARGET: "portable", INGEST_PUBLISH_BUCKET: "selfhost-sessions", INGEST_PUBLISH_EXPECT_GENERATION: "a".repeat(64),
        INGEST_PUBLISH_S3_ENDPOINT: "http://127.0.0.1:9", INGEST_PUBLISH_S3_REGION: "us-east-1", INGEST_PUBLISH_S3_ACCESS_KEY_ID: "publisher-fixture",
        INGEST_PUBLISH_S3_SECRET_ACCESS_KEY: "publisher-fixture-secret-only", INGEST_PUBLISH_DATABASE_URL: `postgresql://${R}:${PW}@${u.host}/${database}`,
        HOOPDB_DB_MODE: "selfhosted", SESSION_POSTGRES_TLS_PROFILE: "supabase",
      };
      const queries: string[] = [];
      const assets = async () => ({ expected: { names: ["x"], generation: "a".repeat(64) }, verifiers: { searchWorker: "SELECT 'verifier-1';", tokenVerifier: "SELECT 2;", ingestSeeder: "SELECT 3;", ingest: "SELECT 4;" } }) as any;
      let stores = 0;
      const store = () => { stores++; throw Object.assign(Error("stop"), { code: "stop_at_store" }); };
      // The loopback fixture server has no TLS: the factory keeps every validated field but ssl.
      const plain = (c: pg.PoolConfig, as?: { user: string; password: string }) => {
        const pool = new pg.Pool({ ...c, ssl: false, ...(as ?? {}) });
        if (as) pool.on("connect", (client) => { client.query(`SET SESSION AUTHORIZATION ${R}`); });
        const original = pool.connect.bind(pool) as any;
        (pool as any).connect = (cb: any) => original((e: any, client: any, done: any) => {
          if (client && !client.__traced) { const q = client.query.bind(client); client.query = (x: any, ...r: any[]) => { queries.push(typeof x === "string" ? x : x.text); return q(x, ...r); }; client.__traced = true; }
          cb(e, client, done);
        });
        return pool;
      };
      const owner = new URL(url);
      await expect(runPortablePublicationRuntime(env, "publish", { assets, store, pool: (c) => plain(c, { user: decodeURIComponent(owner.username), password: decodeURIComponent(owner.password) }) })).rejects.toMatchObject({ code: "publication_database_role" });
      expect(stores).toBe(0);
      expect(queries.some((q) => q.includes("verifier-1"))).toBe(false);
      queries.length = 0;
      // Genuine login: identity passes, the (stub) verifiers run, and the run stops at the store factory.
      await expect(runPortablePublicationRuntime(env, "publish", { assets, store, pool: (c) => plain(c) })).rejects.toMatchObject({ code: "stop_at_store" });
      expect(queries.some((q) => q.includes("verifier-1"))).toBe(true);
      expect(stores).toBe(1);
    });
  }
});
