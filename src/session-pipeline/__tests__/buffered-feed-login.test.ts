import { afterEach, expect, test } from "bun:test";
import { LOGIN_SQL, MEMBERS_SQL, SYSTEM_USER_SQL, type LoginQuery } from "../../acquisition/db-login.ts";
import {
  BUFFERED_FEED_APPLICATION_NAME, BUFFERED_FEED_OPTIONS, BUFFERED_SEARCH_FEED_RUNTIME_ROLE as ROLE, BufferedFeedLoginError,
  assertBufferedFeedLogin, bufferedFeedClientConfig,
} from "../buffered-feed-login.ts";
import { SUPABASE_PROD_CA_2021 } from "../supabase-ca.ts";

const PASSWORD = "pw-test-only-9c2e";
const good = `postgresql://${ROLE}:${PASSWORD}@db.example.test:5432/history`;
const CA = SUPABASE_PROD_CA_2021;
const saved = { ...process.env };
afterEach(() => { for (const k of Object.keys(process.env)) if (!(k in saved)) delete process.env[k]; Object.assign(process.env, saved); });

function refusal(f: () => unknown): string {
  try { f(); } catch (e) { return String(e); }
  return "accepted";
}

test("role name comes from the one TS constant", () => {
  expect(ROLE).toBe("buffered_search_feed_runtime");
});

test("positive: discrete pinned fields, verify-full against the configured CA, fixed name and options, no connectionString", () => {
  const c = bufferedFeedClientConfig(good, CA);
  expect(c).toEqual({
    host: "db.example.test", port: 5432, user: ROLE, password: PASSWORD, database: "history",
    application_name: BUFFERED_FEED_APPLICATION_NAME, ssl: { rejectUnauthorized: true, ca: CA, servername: "db.example.test" },
    options: "-c statement_timeout=5s -c lock_timeout=2s -c idle_in_transaction_session_timeout=10s",
  });
  expect(c.options).toBe(BUFFERED_FEED_OPTIONS);
  expect("connectionString" in c).toBe(false);
  expect(bufferedFeedClientConfig(good + "?sslmode=verify-full", CA)).toEqual(c);
  expect(bufferedFeedClientConfig(good.replace(`${ROLE}:`, `${ROLE}.abcdefghij:`), CA).user).toBe(`${ROLE}.abcdefghij`);
});

test("negatives: TLS downgrade, role override, missing password, wrong login and every other parameter refuse without echoing values", () => {
  Object.assign(process.env, { PGPASSWORD: "env-pw", PGUSER: ROLE, PGSSLMODE: "disable", PGOPTIONS: "-c role=postgres" });
  const cases: Array<[unknown, string]> = [
    [good + "?sslmode=disable", "query_params"],
    [good + "?sslmode=no-verify", "query_params"],
    [good + "?sslmode=require", "query_params"],
    [good + "?options=-c%20role%3Dpostgres", "query_params"],
    [good + "?sslmode=verify-full&options=-c%20role%3Dpostgres", "query_params"],
    [good + "?application_name=other", "query_params"],
    [good + "?statement_timeout=0", "query_params"],
    [`postgresql://${ROLE}@db.example.test:5432/history`, "password"],
    [`postgresql://${ROLE}:@db.example.test:5432/history`, "password"],
    [`postgresql://:${PASSWORD}@db.example.test/history`, "user"],
    [`postgresql://${ROLE}:${PASSWORD}@db.example.test/`, "database"],
    [`https://${ROLE}:${PASSWORD}@db.example.test/history`, "protocol"],
    ["not a url", "url"],
    [42, "url"],
    [undefined, "url"],
  ];
  for (const [url, code] of cases) {
    const msg = refusal(() => bufferedFeedClientConfig(url, CA));
    expect({ url, msg }).toEqual({ url, msg: `Error: 96bulls: buffered_search_feed_database_url refused (${code})` });
    expect(msg.includes(PASSWORD)).toBe(false);
  }
  for (const user of ["postgres", "service_role", "bulls96_acquisition_installer", `${ROLE}x`, `${ROLE}.Bad`]) {
    const msg = refusal(() => bufferedFeedClientConfig(good.replace(`${ROLE}:`, `${user}:`), CA));
    expect([user, msg]).toEqual([user, "Error: 96bulls: buffered_search_feed_database_url user must be the buffered_search_feed_runtime login"]);
  }
  for (const ca of [undefined, 1, "", "x", "-----BEGIN CERTIFICATE-----\nnope\n-----END CERTIFICATE-----", "-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----\n" + CA]) {
    const msg = refusal(() => bufferedFeedClientConfig(good, ca));
    expect(msg).toBe("Error: 96bulls: buffered_search_feed_database_ca_pem must be a PEM CA certificate");
  }
});

// ---------------------------------------------------------------- live-session identity check (mocked)

const healthy = {
  session_user: ROLE, current_user: ROLE, role_setting: "none", server_version: 160004,
  statement_timeout: "5s", lock_timeout: "2s", idle_in_transaction_session_timeout: "10s",
  rolsuper: false, rolbypassrls: false, rolcreaterole: false, rolcreatedb: false, rolreplication: false, rolinherit: false, memberships: 0, members: 0,
  backend_user: ROLE,
};

function fakeDb(row: Record<string, unknown> | null, systemUser: unknown = `scram-sha-256:${ROLE}`, memberRows: Record<string, unknown>[] = []) {
  const seen: string[] = [];
  const db: LoginQuery = {
    async query<T>(text: string) {
      seen.push(text);
      if (text === LOGIN_SQL) return { rows: (row ? [row] : []) as T[] };
      if (text === SYSTEM_USER_SQL) return { rows: [{ system_user: systemUser }] as T[] };
      if (text === MEMBERS_SQL) return { rows: memberRows as T[] };
      throw new Error("unexpected statement");
    },
  };
  return { db, seen };
}

async function loginRefusal(row: Record<string, unknown> | null, systemUser?: unknown, memberRows?: Record<string, unknown>[]): Promise<string> {
  const { db } = fakeDb(row, systemUser === undefined ? `scram-sha-256:${ROLE}` : systemUser, memberRows);
  try { await assertBufferedFeedLogin(db); } catch (e) { return e instanceof BufferedFeedLoginError ? e.code : `other: ${String(e)}`; }
  return "accepted";
}

test("live login: the exact feeder identity with the expected shape is accepted (PG16+ and PG15)", async () => {
  const pg16 = fakeDb(healthy);
  await assertBufferedFeedLogin(pg16.db);
  expect(pg16.seen).toEqual([LOGIN_SQL, SYSTEM_USER_SQL, MEMBERS_SQL]);
  const pg15 = fakeDb({ ...healthy, server_version: 150008 });
  await assertBufferedFeedLogin(pg15.db);
  expect(pg15.seen).toEqual([LOGIN_SQL]);
});

test("live login negatives: wrong identity, SET ROLE, privileged role shape or timeout profile refuse", async () => {
  const cases: Array<[string, Record<string, unknown> | null, string, unknown?]> = [
    ["no row", null, "login_unreadable"],
    ["wrong login (session_user)", { ...healthy, session_user: "postgres", current_user: "postgres" }, "session_user"],
    ["service_role login", { ...healthy, session_user: "service_role", current_user: "service_role" }, "session_user"],
    ["other login that SET ROLE to the feeder", { ...healthy, session_user: "bulls96_session_ingest" }, "session_user"],
    ["feeder login switched role", { ...healthy, current_user: "postgres" }, "current_user"],
    ["role setting present", { ...healthy, role_setting: ROLE }, "set_role"],
    ["superuser", { ...healthy, rolsuper: true }, "superuser"],
    ["bypassrls", { ...healthy, rolbypassrls: true }, "bypassrls"],
    ["createrole", { ...healthy, rolcreaterole: true }, "role_attributes"],
    ["createdb", { ...healthy, rolcreatedb: true }, "role_attributes"],
    ["replication", { ...healthy, rolreplication: true }, "role_attributes"],
    ["inherit", { ...healthy, rolinherit: true }, "role_attributes"],
    ["member of another role", { ...healthy, memberships: 1 }, "role_membership"],
    ["statement_timeout off", { ...healthy, statement_timeout: "0" }, "timeouts"],
    ["lock_timeout differs", { ...healthy, lock_timeout: "0" }, "timeouts"],
    ["idle timeout differs", { ...healthy, idle_in_transaction_session_timeout: "0" }, "timeouts"],
    ["unreadable version", { ...healthy, server_version: "16" }, "login_unreadable"],
    ["PG16 system_user is another identity", healthy, "system_user", "scram-sha-256:postgres"],
    ["PG16 system_user NULL (trust auth)", healthy, "system_user", null],
    ["PG16 system_user without method", healthy, "system_user", ROLE],
    ["backend usename NULL", { ...healthy, backend_user: null }, "backend_user"],
    ["backend usename absent", { ...healthy, backend_user: undefined }, "backend_user"],
    ["backend usename is another login (SET SESSION AUTHORIZATION)", { ...healthy, backend_user: "postgres" }, "backend_user"],
    ["backend usename pooler form", { ...healthy, backend_user: `${ROLE}.abcdefghij` }, "backend_user"],
  ];
  for (const [label, row, code, systemUser] of cases) expect([label, await loginRefusal(row, systemUser)]).toEqual([label, code]);
  const broken: LoginQuery = { async query() { throw new Error("connect ECONNREFUSED"); } };
  let err: unknown;
  try { await assertBufferedFeedLogin(broken); } catch (e) { err = e; }
  expect(String(err)).toBe("BufferedFeedLoginError: 96bulls: buffered search feed login refused: login_unreadable");
  expect(String(new BufferedFeedLoginError("session_user"))).not.toContain("postgres");
});

test("SET SESSION AUTHORIZATION: refused by the backend usename on PG15 and PG16+, before system_user is read", async () => {
  const SSA = { ...healthy, backend_user: "postgres" };
  for (const server_version of [150019, 160015, 170011]) {
    const f = fakeDb({ ...SSA, server_version }, `scram-sha-256:${ROLE}`);
    let err: unknown;
    try { await assertBufferedFeedLogin(f.db); } catch (e) { err = e; }
    expect([server_version, err instanceof BufferedFeedLoginError && err.code]).toEqual([server_version, "backend_user"]);
    expect(f.seen).toEqual([LOGIN_SQL]);
  }
});

const CREATOR_EDGE = Object.freeze({
  member: "postgres", grantor: "supabase_admin", grantor_super: true, admin_option: true, inherit_option: false, set_option: false,
});

test("members: only the exact Supabase PG16+ creator edge is tolerated; any other member refuses", async () => {
  expect(await loginRefusal({ ...healthy, members: 1 }, undefined, [{ ...CREATOR_EDGE }])).toBe("accepted");
  expect(await loginRefusal({ ...healthy, server_version: 150008, members: 1 }, undefined, [{ ...CREATOR_EDGE }])).toBe("role_members");
  const cases: Array<[string, Record<string, unknown>[], number?]> = [
    ["attacker member", [{ member: "feed_attacker", grantor: "postgres", grantor_super: true, admin_option: false, inherit_option: true, set_option: true }]],
    ["SET option true", [{ ...CREATOR_EDGE, set_option: true }]],
    ["INHERIT option true", [{ ...CREATOR_EDGE, inherit_option: true }]],
    ["no ADMIN option", [{ ...CREATOR_EDGE, admin_option: false }]],
    ["grantor not superuser", [{ ...CREATOR_EDGE, grantor_super: false }]],
    ["other grantor", [{ ...CREATOR_EDGE, grantor: "postgres" }]],
    ["other member", [{ ...CREATOR_EDGE, member: "service_role" }]],
    ["creator edge plus another member", [{ ...CREATOR_EDGE }, { ...CREATOR_EDGE, member: "feed_attacker", admin_option: false, set_option: true }]],
    ["count and rows disagree", [{ ...CREATOR_EDGE }], 2],
    ["count says member, rows empty", [], 1],
  ];
  for (const [label, rows, count] of cases)
    expect([label, await loginRefusal({ ...healthy, members: count ?? rows.length }, undefined, rows)]).toEqual([label, "role_members"]);
  expect(await loginRefusal({ ...healthy, members: "0" }, undefined, [])).toBe("login_unreadable");
});

// ---------------------------------------------------------------- the feeder's checked executor

import { loginCheckedFeedExecutor, type FeedPool, type FeedPoolClient } from "../buffered-feed-login.ts";
import { BufferedFeedError, sqlBufferedFeedSource } from "../buffered-search-feed.ts";

function fakePool(identity: () => Record<string, unknown>) {
  const log: string[] = [];
  let n = 0;
  const pool: FeedPool = {
    async connect() {
      const id = ++n;
      const client: FeedPoolClient = {
        async query<T>(text: string) {
          log.push(text === LOGIN_SQL ? `${id}:login` : text === SYSTEM_USER_SQL ? `${id}:system_user` : text === MEMBERS_SQL ? `${id}:members` : `${id}:rpc`);
          if (text === LOGIN_SQL) return { rows: [identity()] as T[] };
          if (text === SYSTEM_USER_SQL) return { rows: [{ system_user: `scram-sha-256:${identity().backend_user}` }] as T[] };
          if (text === MEMBERS_SQL) return { rows: [] as T[] };
          return { rows: [{ r: { advanced: true } }] as T[] };
        },
        release(destroy) { log.push(`${id}:release${destroy ? ":destroy" : ""}`); },
      };
      return client;
    },
  };
  return { pool, log };
}

test("checked executor: every backend passes the login check before its first feed RPC; a refusal runs no RPC and is not cached", async () => {
  // Genuine login: checked once per backend, then the RPC runs.
  const ok = fakePool(() => healthy);
  await loginCheckedFeedExecutor(ok.pool).query("SELECT sessions.buffered_search_feed_advance($1::jsonb) AS r", ["{}"]);
  expect(ok.log).toEqual(["1:login", "1:system_user", "1:members", "1:rpc", "1:release"]);

  // A superuser's SET SESSION AUTHORIZATION, a SET ROLE and a service_role login are each refused: no RPC, backend destroyed.
  for (const [label, row, code] of [
    ["set session authorization", { ...healthy, backend_user: "postgres" }, "login_backend_user"],
    ["set role", { ...healthy, role_setting: ROLE }, "login_set_role"],
    ["service_role", { ...healthy, session_user: "service_role", current_user: "service_role", backend_user: "service_role" }, "login_session_user"],
  ] as const) {
    const bad = fakePool(() => row);
    const source = sqlBufferedFeedSource(loginCheckedFeedExecutor(bad.pool));
    let err: unknown;
    try { await source.advance("c", { createdAt: "2026-11-06T00:00:00.000000Z", stageToken: "00000000-0000-4000-8000-000000000001" }, { indexed: 0, skipped: 0 }); } catch (e) { err = e; }
    expect([label, err instanceof BufferedFeedError && err.code, (err as BufferedFeedError).retryable]).toEqual([label, code, false]);
    expect(bad.log.some((l) => l.endsWith(":rpc"))).toBe(false);
    expect(bad.log.at(-1)).toBe("1:release:destroy");
  }

  // Not cached on failure: the next call checks again (and is accepted once the login is genuine).
  let current: Record<string, unknown> = { ...healthy, backend_user: null };
  const flip = fakePool(() => current);
  const exec = loginCheckedFeedExecutor(flip.pool);
  await expect(exec.query("SELECT 1")).rejects.toMatchObject({ code: "login_backend_user" });
  current = healthy;
  await exec.query("SELECT 1");
  expect(flip.log.filter((l) => l.endsWith(":login"))).toEqual(["1:login", "2:login"]);
  expect(flip.log.filter((l) => l.endsWith(":rpc"))).toEqual(["2:rpc"]);
});
