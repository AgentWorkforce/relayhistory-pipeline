/**
 * Opt-in OWNED PostgreSQL (ACQUISITION_LOGIN_TEST_PG_URL, loopback, disposable trust-auth fixture only):
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
import { ACQUISITION_INSTALLER_ROLE as R, INSTALLER_TIMEOUTS, InstallerLoginError, assertInstallerLogin, type LoginQuery } from "../db-login.ts";

const url = process.env.ACQUISITION_LOGIN_TEST_PG_URL;
// Refuse credentials and remote endpoints before connecting.
function fixture(raw: string): URL {
  const u = new URL(raw);
  if (!["127.0.0.1", "localhost"].includes(u.hostname)) throw new Error("ACQUISITION_LOGIN_TEST_PG_URL must be loopback");
  if (u.password) throw new Error("ACQUISITION_LOGIN_TEST_PG_URL must carry no password (disposable trust-auth fixture)");
  return u;
}

async function refusal(client: pg.Client): Promise<string> {
  try { await assertInstallerLogin(client as unknown as LoginQuery); } catch (e) { return e instanceof InstallerLoginError ? e.code : `other: ${String(e)}`; }
  return "accepted";
}

const SET_TIMEOUTS = Object.entries(INSTALLER_TIMEOUTS).map(([k, v]) => `SET ${k} = '${v}'`).join("; ");

describe.skipIf(!url)("assertInstallerLogin against real PostgreSQL", () => {
  let u: URL;
  let owner: pg.Client;
  let version = 0;
  beforeAll(async () => {
    u = fixture(url!);
    owner = new pg.Client({ connectionString: url });
    await owner.connect();
    version = Number((await owner.query("SELECT current_setting('server_version_num') AS v")).rows[0].v);
    await owner.query(`DO $$BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${R}') THEN CREATE ROLE ${R} LOGIN; END IF; END$$`);
    await owner.query(`ALTER ROLE ${R} LOGIN NOSUPERUSER NOBYPASSRLS NOCREATEROLE NOCREATEDB NOREPLICATION NOINHERIT`);
  });
  afterAll(async () => { await owner?.end().catch(() => undefined); });

  test("superuser SET SESSION AUTHORIZATION to the installer is refused by the backend usename (all versions)", async () => {
    const su = new pg.Client({ connectionString: url });
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
    const login = new pg.Client({
      host: u.hostname, port: Number(u.port || 5432), user: R, database: u.pathname.slice(1) || "postgres",
      options: Object.entries(INSTALLER_TIMEOUTS).map(([k, v]) => `-c ${k}=${v}`).join(" "),
    });
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
