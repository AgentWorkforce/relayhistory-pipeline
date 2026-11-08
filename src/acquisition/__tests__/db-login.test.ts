import { afterEach, expect, test } from "bun:test";
import { SUPABASE_PROD_CA_2021 } from "../../session-pipeline/supabase-ca.ts";
import { acquisitionConfig } from "../config.ts";
import {
  ACQUISITION_APPLICATION_NAME, ACQUISITION_INSTALLER_ROLE, INSTALLER_OPTIONS, InstallerLoginError, LOGIN_SQL, MEMBERS_SQL, SYSTEM_USER_SQL,
  assertInstallerLogin, installerClientConfig, type LoginQuery,
} from "../db-login.ts";
import { pinnedKeysJson } from "./helpers.ts";

const PASSWORD = "pw-test-only-7f3a";
const good = `postgresql://${ACQUISITION_INSTALLER_ROLE}:${PASSWORD}@db.example.test:5432/history`;
const CA = SUPABASE_PROD_CA_2021;
const saved = { ...process.env };
afterEach(() => { for (const k of Object.keys(process.env)) if (!(k in saved)) delete process.env[k]; Object.assign(process.env, saved); });

function refusal(f: () => unknown): string {
  try { f(); } catch (e) { return String(e); }
  return "accepted";
}

test("positive: discrete pinned fields, verify-full against the configured CA, fixed name and options, no connectionString", () => {
  const c = installerClientConfig(good, CA);
  expect(c).toEqual({
    host: "db.example.test", port: 5432, user: ACQUISITION_INSTALLER_ROLE, password: PASSWORD, database: "history",
    application_name: ACQUISITION_APPLICATION_NAME, ssl: { rejectUnauthorized: true, ca: CA, servername: "db.example.test" },
    options: "-c statement_timeout=5s -c lock_timeout=2s -c idle_in_transaction_session_timeout=10s",
  });
  expect(c.options).toBe(INSTALLER_OPTIONS);
  expect("connectionString" in c).toBe(false);
  expect(installerClientConfig(good + "?sslmode=verify-full", CA)).toEqual(c);
  // Supabase pooler login form <role>.<tenant>.
  expect(installerClientConfig(good.replace(`${ACQUISITION_INSTALLER_ROLE}:`, `${ACQUISITION_INSTALLER_ROLE}.abcdefghij:`), CA).user).toBe(`${ACQUISITION_INSTALLER_ROLE}.abcdefghij`);
});

test("negatives: TLS downgrade, role override, missing password, wrong login and every other parameter refuse startup without echoing values", () => {
  // Ambient fallbacks must not rescue a refused URL.
  Object.assign(process.env, { PGPASSWORD: "env-pw", PGUSER: ACQUISITION_INSTALLER_ROLE, PGSSLMODE: "disable", PGOPTIONS: "-c role=postgres" });
  const cases: Array<[string, string]> = [
    [good + "?sslmode=disable", "query_params"],
    [good + "?sslmode=no-verify", "query_params"],
    [good + "?sslmode=require", "query_params"],
    [good + "?sslmode=prefer", "query_params"],
    [good + "?options=-c%20role%3Dpostgres", "query_params"],
    [good + "?sslmode=verify-full&options=-c%20role%3Dpostgres", "query_params"],
    [good + "?application_name=other", "query_params"],
    [good + "?statement_timeout=0", "query_params"],
    [good + "?sslrootcert=system", "query_params"],
    [`postgresql://${ACQUISITION_INSTALLER_ROLE}@db.example.test:5432/history`, "password"],
    [`postgresql://${ACQUISITION_INSTALLER_ROLE}:@db.example.test:5432/history`, "password"],
    [`postgresql://:${PASSWORD}@db.example.test/history`, "user"],
    [`postgresql://${ACQUISITION_INSTALLER_ROLE}:${PASSWORD}@db.example.test/`, "database"],
    [`https://${ACQUISITION_INSTALLER_ROLE}:${PASSWORD}@db.example.test/history`, "protocol"],
    ["not a url", "url"],
  ];
  for (const [url, code] of cases) {
    const msg = refusal(() => installerClientConfig(url, CA));
    expect({ url, msg }).toEqual({ url, msg: `Error: 96bulls: ACQUISITION_DATABASE_URL refused (${code})` });
    expect(msg.includes(PASSWORD)).toBe(false);
  }
  // Another login (e.g. a superuser or an ingest role) is refused before any connection.
  for (const user of ["postgres", "bulls96_session_ingest", `${ACQUISITION_INSTALLER_ROLE}x`, `${ACQUISITION_INSTALLER_ROLE}.Bad`]) {
    const msg = refusal(() => installerClientConfig(good.replace(`${ACQUISITION_INSTALLER_ROLE}:`, `${user}:`), CA));
    expect([user, msg]).toEqual([user, "Error: 96bulls: ACQUISITION_DATABASE_URL user must be the bulls96_acquisition_installer login"]);
  }
  // The CA must be a parseable PEM certificate, never a private key or junk.
  for (const ca of ["", "x", "-----BEGIN CERTIFICATE-----\nnope\n-----END CERTIFICATE-----", "-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----\n" + CA]) {
    const msg = refusal(() => installerClientConfig(good, ca));
    expect(msg).toBe("Error: 96bulls: ACQUISITION_DATABASE_CA_PEM must be a PEM CA certificate");
  }
});

test("startup config: the installer login is required and pinned through acquisitionConfig", () => {
  const live = {
    ACQUISITION_ENABLED: "true", ACQUISITION_ORIGIN: "https://history.example.test", RELAYCONTROL_INTROSPECT_URL: "https://relaycontrol.example.test/v1/introspect",
    RELAYCONTROL_INTROSPECT_SECRET: "x", ACQUISITION_PINNED_KEYS: pinnedKeysJson, ACQUISITION_DATABASE_URL: good, ACQUISITION_DATABASE_CA_PEM: CA,
  };
  const c = acquisitionConfig(live);
  expect(c.enabled && c.database).toEqual(installerClientConfig(good, CA.trim())); // required() trims
  for (const bad of [good + "?sslmode=disable", good + "?sslmode=no-verify", good + "?options=-c%20role%3Dpostgres", good.replace(`:${PASSWORD}`, "")]) {
    const msg = refusal(() => acquisitionConfig({ ...live, ACQUISITION_DATABASE_URL: bad }));
    expect(msg).toMatch(/^Error: 96bulls: ACQUISITION_DATABASE_URL refused \(/);
    expect(msg.includes(PASSWORD)).toBe(false);
  }
  expect(refusal(() => acquisitionConfig({ ...live, ACQUISITION_DATABASE_CA_PEM: undefined }))).toContain("needs ACQUISITION_DATABASE_CA_PEM");
  // Disabled: neither variable is read.
  expect(acquisitionConfig({ ACQUISITION_DATABASE_URL: "postgres://x?sslmode=disable", ACQUISITION_DATABASE_CA_PEM: "x" }).enabled).toBe(false);
});

// ---------------------------------------------------------------- live-session identity check (mocked)

const healthy = {
  session_user: ACQUISITION_INSTALLER_ROLE, current_user: ACQUISITION_INSTALLER_ROLE, role_setting: "none", server_version: 160004,
  statement_timeout: "5s", lock_timeout: "2s", idle_in_transaction_session_timeout: "10s",
  rolsuper: false, rolbypassrls: false, rolcreaterole: false, rolcreatedb: false, rolreplication: false, rolinherit: false, memberships: 0, members: 0,
  backend_user: ACQUISITION_INSTALLER_ROLE,
};

function fakeDb(
  row: Record<string, unknown> | null,
  systemUser: unknown = `scram-sha-256:${ACQUISITION_INSTALLER_ROLE}`,
  memberRows: Record<string, unknown>[] = [],
) {
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

async function loginRefusal(row: Record<string, unknown> | null, systemUser?: unknown): Promise<string> {
  const { db } = fakeDb(row, ...(systemUser === undefined ? [] : [systemUser]));
  try { await assertInstallerLogin(db); } catch (e) { return e instanceof InstallerLoginError ? e.code : `other: ${String(e)}`; }
  return "accepted";
}

test("live login: the exact installer identity with the expected shape is accepted (PG16+ and PG15)", async () => {
  const pg16 = fakeDb(healthy);
  await assertInstallerLogin(pg16.db);
  expect(pg16.seen).toEqual([LOGIN_SQL, SYSTEM_USER_SQL, MEMBERS_SQL]);
  const pg15 = fakeDb({ ...healthy, server_version: 150008 });
  await assertInstallerLogin(pg15.db);
  expect(pg15.seen).toEqual([LOGIN_SQL]); // system_user and the PG16 member columns do not exist before PG16
});

test("live login negatives: wrong identity, SET ROLE, privileged role shape or timeout profile refuse startup", async () => {
  const cases: Array<[string, Record<string, unknown> | null, string, unknown?]> = [
    ["no row", null, "login_unreadable"],
    ["wrong login (session_user)", { ...healthy, session_user: "postgres", current_user: "postgres" }, "session_user"],
    ["other login that SET ROLE to the installer", { ...healthy, session_user: "bulls96_session_ingest" }, "session_user"],
    ["installer login switched role", { ...healthy, current_user: "postgres" }, "current_user"],
    ["role setting present", { ...healthy, role_setting: ACQUISITION_INSTALLER_ROLE }, "set_role"],
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
    ["PG16 system_user without method", healthy, "system_user", ACQUISITION_INSTALLER_ROLE],
    ["backend usename NULL", { ...healthy, backend_user: null }, "backend_user"],
    ["backend usename absent (no pg_stat_activity row)", { ...healthy, backend_user: undefined }, "backend_user"],
    ["backend usename is another login", { ...healthy, backend_user: "postgres" }, "backend_user"],
    ["backend usename case differs", { ...healthy, backend_user: ACQUISITION_INSTALLER_ROLE.toUpperCase() }, "backend_user"],
    ["backend usename pooler form", { ...healthy, backend_user: `${ACQUISITION_INSTALLER_ROLE}.abcdefghij` }, "backend_user"],
  ];
  for (const [label, row, code, systemUser] of cases) expect([label, await loginRefusal(row, systemUser)]).toEqual([label, code]);
  // A failing query refuses too, and the error never carries a role name.
  const broken: LoginQuery = { async query() { throw new Error("connect ECONNREFUSED"); } };
  let err: unknown;
  try { await assertInstallerLogin(broken); } catch (e) { err = e; }
  expect(String(err)).toBe("InstallerLoginError: 96bulls: acquisition installer login refused: login_unreadable");
  expect(String(new InstallerLoginError("session_user"))).not.toContain("postgres");
});

// ---------------------------------------------------------------- inbound memberships (dogpatch P1)
// A LOGIN granted the installer role could SET ROLE to it and call the SECURITY DEFINER RPCs,
// so the installer must have no member. The only tolerated edge is the Supabase PG16+ creator
// edge (postgres, granted by the superuser supabase_admin, ADMIN only, no SET, no INHERIT).

const CREATOR_EDGE = Object.freeze({
  member: "postgres", grantor: "supabase_admin", grantor_super: true, admin_option: true, inherit_option: false, set_option: false,
});

async function membersRefusal(row: Record<string, unknown>, memberRows: Record<string, unknown>[]): Promise<string> {
  const { db } = fakeDb(row, `scram-sha-256:${ACQUISITION_INSTALLER_ROLE}`, memberRows);
  try { await assertInstallerLogin(db); } catch (e) { return e instanceof InstallerLoginError ? e.code : `other: ${String(e)}`; }
  return "accepted";
}

test("inbound only: a member of the installer role refuses startup even with no outgoing membership", async () => {
  const attacker = { member: "acq_attacker", grantor: "postgres", grantor_super: true, admin_option: false, inherit_option: true, set_option: true };
  // PG16+: the row is read and refused.
  expect(await membersRefusal({ ...healthy, memberships: 0, members: 1 }, [attacker])).toBe("role_members");
  // Before PG16 any member refuses (no inherit/set columns to tell the creator edge apart).
  expect(await membersRefusal({ ...healthy, server_version: 150008, memberships: 0, members: 1 }, [])).toBe("role_members");
});

test("outbound only: an installer that is a member of another role refuses startup with no inbound row", async () => {
  expect(await membersRefusal({ ...healthy, memberships: 1, members: 0 }, [])).toBe("role_membership");
  expect(await membersRefusal({ ...healthy, server_version: 150008, memberships: 1, members: 0 }, [])).toBe("role_membership");
});

test("the exact Supabase PG16+ creator edge is accepted; before PG16 it is refused", async () => {
  const pg16 = fakeDb({ ...healthy, members: 1 }, `scram-sha-256:${ACQUISITION_INSTALLER_ROLE}`, [{ ...CREATOR_EDGE }]);
  await assertInstallerLogin(pg16.db);
  expect(pg16.seen).toEqual([LOGIN_SQL, SYSTEM_USER_SQL, MEMBERS_SQL]);
  expect(await membersRefusal({ ...healthy, server_version: 150008, members: 1 }, [{ ...CREATOR_EDGE }])).toBe("role_members");
});

test("creator-edge look-alikes refuse startup", async () => {
  const cases: Array<[string, Record<string, unknown>[], number?]> = [
    ["SET option true", [{ ...CREATOR_EDGE, set_option: true }]],
    ["INHERIT option true", [{ ...CREATOR_EDGE, inherit_option: true }]],
    ["no ADMIN option", [{ ...CREATOR_EDGE, admin_option: false }]],
    ["grantor not superuser", [{ ...CREATOR_EDGE, grantor_super: false }]],
    ["other grantor", [{ ...CREATOR_EDGE, grantor: "postgres" }]],
    ["other member", [{ ...CREATOR_EDGE, member: "supabase_admin" }]],
    ["creator edge plus another member", [{ ...CREATOR_EDGE }, { ...CREATOR_EDGE, member: "acq_attacker", admin_option: false, set_option: true }], 2],
    ["option read as a string", [{ ...CREATOR_EDGE, set_option: "false" }]],
    ["count and rows disagree", [{ ...CREATOR_EDGE }], 2],
    ["count says member, rows empty", [], 1],
  ];
  for (const [label, rows, count] of cases)
    expect([label, await membersRefusal({ ...healthy, members: count ?? rows.length }, rows)]).toEqual([label, "role_members"]);
  // An unreadable count or a failing member query refuses as unreadable.
  expect(await membersRefusal({ ...healthy, members: "0" }, [])).toBe("login_unreadable");
  const failing: LoginQuery = {
    async query<T>(text: string) {
      if (text === LOGIN_SQL) return { rows: [healthy] as T[] };
      if (text === SYSTEM_USER_SQL) return { rows: [{ system_user: `scram-sha-256:${ACQUISITION_INSTALLER_ROLE}` }] as T[] };
      throw new Error("permission denied");
    },
  };
  let err: unknown;
  try { await assertInstallerLogin(failing); } catch (e) { err = e; }
  expect(err instanceof InstallerLoginError && err.code).toBe("login_unreadable");
});

// ---------------------------------------------------------------- SET SESSION AUTHORIZATION (usename)
// After a superuser runs SET SESSION AUTHORIZATION <installer>, session_user and current_user are the
// installer and current_setting('role') stays 'none' (recorded on real PostgreSQL 15.19, 16.15 and 17.11,
// as in #366). Only this backend's pg_stat_activity usename, and on PG16+ system_user, still name the
// authenticated superuser. The usename check runs on every version, before the PG16 system_user query.

const SSA = { ...healthy, backend_user: "postgres" };

test("SET SESSION AUTHORIZATION on PG15: refused by the backend usename, with no PG16 query", async () => {
  const pg15 = fakeDb({ ...SSA, server_version: 150000 });
  let err: unknown;
  try { await assertInstallerLogin(pg15.db); } catch (e) { err = e; }
  expect(err instanceof InstallerLoginError && err.code).toBe("backend_user");
  expect(String(err)).toBe("InstallerLoginError: 96bulls: acquisition installer login refused: backend_user");
  expect(pg15.seen).toEqual([LOGIN_SQL]);
  // Before this check the same row was accepted on PG15: every other property looks genuine.
  expect(await loginRefusal({ ...SSA, server_version: 150019, backend_user: ACQUISITION_INSTALLER_ROLE })).toBe("accepted");
});

test("SET SESSION AUTHORIZATION on PG16+: the usename check refuses it too, independently of system_user", async () => {
  for (const server_version of [160000, 160015, 170011]) {
    // system_user would also catch it; the usename check refuses first, before system_user is read.
    const both = fakeDb({ ...SSA, server_version }, "scram-sha-256:postgres");
    let err: unknown;
    try { await assertInstallerLogin(both.db); } catch (e) { err = e; }
    expect([server_version, err instanceof InstallerLoginError && err.code]).toEqual([server_version, "backend_user"]);
    expect(both.seen).toEqual([LOGIN_SQL]);
    // Even if system_user agreed, usename alone refuses.
    expect([server_version, await loginRefusal({ ...SSA, server_version }, `scram-sha-256:${ACQUISITION_INSTALLER_ROLE}`)]).toEqual([server_version, "backend_user"]);
  }
  // And system_user still refuses on its own when usename agrees (existing check kept).
  expect(await loginRefusal({ ...healthy, server_version: 160015 }, "scram-sha-256:postgres")).toBe("system_user");
});

test("positive: backend usename equal to the installer role is accepted on PG15, PG16 and PG17", async () => {
  for (const server_version of [150000, 150019, 160000, 160015, 170011]) {
    expect([server_version, await loginRefusal({ ...healthy, server_version, backend_user: ACQUISITION_INSTALLER_ROLE })]).toEqual([server_version, "accepted"]);
  }
  // The query reads this backend's row, schema-qualified so a search_path object cannot shadow it.
  expect(LOGIN_SQL).toContain("(SELECT a.usename::text FROM pg_catalog.pg_stat_activity a WHERE a.pid = pg_catalog.pg_backend_pid()) AS backend_user");
});
