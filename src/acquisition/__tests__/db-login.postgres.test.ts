/**
 * Opt-in OWNED PostgreSQL (ACQUISITION_LOGIN_TEST_PG_URL, disposable trust-auth fixture only; the shared
 * owned_loopback_fixture_required guard admits only postgres://postgres@<loopback>[:port]/postgres and
 * throws at import, before any connection):
 * assertInstallerLogin against a real server. A superuser that runs SET SESSION AUTHORIZATION to the
 * installer role leaves session_user, current_user and current_setting('role') looking genuine; this
 * proves the backend's pg_stat_activity usename refuses it on PG15, 16 and 17 (backend_user), and that
 * a genuine installer login passes that check. The pg-proof workflow runs it on each matrix version.
 *
 * Trust auth: a genuine login has NULL system_user on PG16+, which assertInstallerLogin refuses
 * (system_user, checked after backend_user); PG15 has no system_user, so the genuine login is accepted.
 * Creates (or reshapes) bulls96_acquisition_installer with no password; never run against a shared server.
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import pg from "pg";
import { ownedLoopbackFixture } from "../../shared/__tests__/owned-loopback-fixture.ts";
import { ACQUISITION_INSTALLER_ROLE as R, INSTALLER_TIMEOUTS, InstallerLoginError, assertInstallerLogin, type LoginQuery } from "../db-login.ts";

const url = process.env.ACQUISITION_LOGIN_TEST_PG_URL;
// Refuse credentials, query options, remote endpoints and nonfixture identities at import, before connecting.
const fixtureUrl = url ? ownedLoopbackFixture(url) : undefined;

/** Every client is built from discrete fields of the guarded URL (never a raw connectionString). */
function fixtureClient(raw: string, user: string, extra: Partial<pg.ClientConfig> = {}, make = (c: pg.ClientConfig) => new pg.Client(c)): pg.Client {
  const u = ownedLoopbackFixture(raw);
  return make({ host: u.hostname.replace(/^\[(.*)\]$/, "$1"), port: Number(u.port || 5432), user, database: "postgres", ...extra });
}

test("fixture guard: only postgres://postgres@<loopback>/postgres; refuses before building a client", () => {
  let made = 0;
  const make = (c: pg.ClientConfig) => { made++; return c as unknown as pg.Client; };
  for (const ok of ["postgres://postgres@127.0.0.1:5432/postgres", "postgresql://postgres@localhost/postgres", "postgres://postgres@[::1]:5433/postgres"])
    expect(fixtureClient(ok, "postgres", {}, make)).toMatchObject({ user: "postgres", database: "postgres" });
  expect((fixtureClient("postgres://postgres@[::1]:5433/postgres", "postgres", {}, make) as unknown as pg.ClientConfig).host).toBe("::1");
  expect(made).toBe(4);
  made = 0;
  for (const bad of [
    "postgres://postgres@127.0.0.1:5432/postgres?host=db.example.test", // query option override
    "postgres://postgres@127.0.0.1:5432/postgres?user=other&password=x",
    "postgres://postgres@127.0.0.1:5432/postgres?options=-c%20role%3Dother",
    "postgres://postgres@127.0.0.1:5432/postgres#x", // hash
    "postgres://postgres@db.example.test:5432/postgres", // non-loopback
    "postgres://postgres@10.0.0.1:5432/postgres",
    "postgres://other@127.0.0.1:5432/postgres", // other user
    "postgres://127.0.0.1:5432/postgres", // omitted user (would fall back to PGUSER)
    "postgres://postgres:pw@127.0.0.1:5432/postgres", // password present
    "postgres://postgres@127.0.0.1:5432/other", // other database
    "postgres://postgres@127.0.0.1:5432/", // omitted database
    "https://postgres@127.0.0.1:5432/postgres",
    "not a url",
  ]) expect([bad, (() => { try { fixtureClient(bad, "postgres", {}, make); return "built"; } catch (e) { return String(e); } })()])
    .toEqual([bad, "Error: owned_loopback_fixture_required"]);
  expect(made).toBe(0);
});

async function refusal(client: pg.Client): Promise<string> {
  try { await assertInstallerLogin(client as unknown as LoginQuery); } catch (e) { return e instanceof InstallerLoginError ? e.code : `other: ${String(e)}`; }
  return "accepted";
}

const SET_TIMEOUTS = Object.entries(INSTALLER_TIMEOUTS).map(([k, v]) => `SET ${k} = '${v}'`).join("; ");

describe.skipIf(!fixtureUrl)("assertInstallerLogin against real PostgreSQL", () => {
  let owner: pg.Client;
  let version = 0;
  beforeAll(async () => {
    owner = fixtureClient(url!, "postgres");
    await owner.connect();
    version = Number((await owner.query("SELECT current_setting('server_version_num') AS v")).rows[0].v);
    await owner.query(`DO $$BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${R}') THEN CREATE ROLE ${R} LOGIN; END IF; END$$`);
    await owner.query(`ALTER ROLE ${R} LOGIN NOSUPERUSER NOBYPASSRLS NOCREATEROLE NOCREATEDB NOREPLICATION NOINHERIT`);
  });
  afterAll(async () => { await owner?.end().catch(() => undefined); });

  test("superuser SET SESSION AUTHORIZATION to the installer is refused by the backend usename (all versions)", async () => {
    const su = fixtureClient(url!, "postgres");
    await su.connect();
    try {
      await su.query(SET_TIMEOUTS);
      await su.query(`SET SESSION AUTHORIZATION ${R}`);
      // The finding: every pre-#331-fix signal looks genuine on every version.
      expect((await su.query("SELECT session_user::text AS s, current_user::text AS c, current_setting('role') AS r")).rows[0]).toEqual({ s: R, c: R, r: "none" });
      expect((await su.query("SELECT usename::text AS u FROM pg_catalog.pg_stat_activity WHERE pid = pg_catalog.pg_backend_pid()")).rows[0].u).not.toBe(R);
      expect([version, await refusal(su)]).toEqual([version, "backend_user"]);
      // Without it, SET ROLE is caught earlier by session_user (existing check kept).
      await su.query(`RESET SESSION AUTHORIZATION; SET ROLE ${R}`);
      expect([version, await refusal(su)]).toEqual([version, "session_user"]);
    } finally {
      await su.end().catch(() => undefined);
    }
  });

  test("a genuine installer login passes the usename check", async () => {
    const login = fixtureClient(url!, R, { options: Object.entries(INSTALLER_TIMEOUTS).map(([k, v]) => `-c ${k}=${v}`).join(" ") });
    await login.connect();
    try {
      expect((await login.query("SELECT usename::text AS u FROM pg_catalog.pg_stat_activity WHERE pid = pg_catalog.pg_backend_pid()")).rows[0].u).toBe(R);
      // PG15: accepted. PG16+: trust auth leaves system_user NULL, refused after backend_user passed.
      expect([version, await refusal(login)]).toEqual([version, version >= 160000 ? "system_user" : "accepted"]);
    } finally {
      await login.end().catch(() => undefined);
    }
  });
});
