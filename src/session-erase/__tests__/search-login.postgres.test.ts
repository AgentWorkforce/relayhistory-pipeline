/** Genuine logins on an owned loopback fixture, including the PG16+ creator edge. */
import { describe, expect, test } from "bun:test";
import pg from "pg";
import { assertEraseSearchIdentity, assertEraseSearchPrivileges } from "../selfhost-runtime.ts";
const url = process.env.SESSION_INGEST_TEST_PG_URL;
if (url) {
 const u = new URL(url);
 if (!["127.0.0.1","localhost","[::1]"].includes(u.hostname) || u.username !== "postgres" || u.password || u.pathname !== "/postgres" || u.search || u.hash)
  throw Error("owned_loopback_fixture_required");
}
describe.skipIf(!url)("operator erase RDS login identity", () => {
 test("only the schema owner's non-inheriting ADMIN edge is tolerated", async () => {
  const admin = new pg.Client({connectionString:url}); await admin.connect();
  const id = crypto.randomUUID().replaceAll("-", "");
  const database = `erase_login_${id}`, ownerName = `erase_owner_${id}`, otherName = `erase_other_${id}`;
  const role = "bulls96_session_erase_search";
  let owner: pg.Client | undefined, login: pg.Pool | undefined, created = false;
  try {
   if ((await admin.query("SELECT 1 FROM pg_roles WHERE rolname=$1",[role])).rows.length) throw Error("fixture_role_already_exists");
   await admin.query(`CREATE ROLE ${ownerName} LOGIN CREATEROLE; CREATE ROLE ${otherName} NOLOGIN`);
   await admin.query(`CREATE DATABASE ${database} OWNER ${ownerName}`);
   const target = new URL(url!); target.pathname = `/${database}`; target.username = ownerName;
   owner = new pg.Client({connectionString:target.href}); await owner.connect();
   await owner.query(`CREATE SCHEMA search_index; CREATE ROLE ${role} LOGIN NOINHERIT`); created = true;
   target.username = role; login = new pg.Pool({connectionString:target.href,max:1});
   await expect(assertEraseSearchIdentity(login)).resolves.toBeUndefined();
   await owner.query(`CREATE TABLE search_index.transcript_events(org_id text,workspace_id text,source text,session_id text,content text);
    CREATE TABLE search_index.projection_state(org_id text,workspace_id text,source text,session_id text,canonical_session_id text,content text);
    ALTER TABLE search_index.transcript_events ENABLE ROW LEVEL SECURITY;
    ALTER TABLE search_index.transcript_events FORCE ROW LEVEL SECURITY;
    ALTER TABLE search_index.projection_state ENABLE ROW LEVEL SECURITY;
    ALTER TABLE search_index.projection_state FORCE ROW LEVEL SECURITY;
    GRANT USAGE ON SCHEMA search_index TO ${role};
    GRANT DELETE ON search_index.transcript_events,search_index.projection_state TO ${role};
    GRANT SELECT(org_id,workspace_id,source,session_id) ON search_index.transcript_events TO ${role};
    GRANT SELECT(org_id,workspace_id,source,session_id,canonical_session_id) ON search_index.projection_state TO ${role}`);
   await expect(assertEraseSearchPrivileges(login)).resolves.toBeUndefined();
   await owner.query(`GRANT CREATE ON SCHEMA search_index TO ${role}`);
   await expect(assertEraseSearchPrivileges(login)).rejects.toThrow("privileges refused");
   await owner.query(`REVOKE CREATE ON SCHEMA search_index FROM ${role}; GRANT SELECT(content) ON search_index.transcript_events TO ${role}`);
   await expect(assertEraseSearchPrivileges(login)).rejects.toThrow("privileges refused");
   await owner.query(`REVOKE SELECT(content) ON search_index.transcript_events FROM ${role}; GRANT INSERT ON search_index.transcript_events TO ${role}`);
   await expect(assertEraseSearchPrivileges(login)).rejects.toThrow("privileges refused");
   await owner.query(`REVOKE INSERT ON search_index.transcript_events FROM ${role}`);
   await expect(assertEraseSearchPrivileges(login)).resolves.toBeUndefined();
   const version = Number((await admin.query("SHOW server_version_num")).rows[0].server_version_num);
   if (version >= 160000) {
    await admin.query(`GRANT ${role} TO ${ownerName} WITH INHERIT TRUE`);
    await expect(assertEraseSearchIdentity(login)).rejects.toThrow("login refused");
    await admin.query(`GRANT ${role} TO ${ownerName} WITH INHERIT FALSE; GRANT ${role} TO ${ownerName} WITH SET TRUE`);
    await expect(assertEraseSearchIdentity(login)).rejects.toThrow("login refused");
    await admin.query(`GRANT ${role} TO ${ownerName} WITH SET FALSE`);
   }
   await admin.query(`GRANT ${role} TO ${otherName} WITH ADMIN OPTION`);
   await expect(assertEraseSearchIdentity(login)).rejects.toThrow("login refused");
   await admin.query(`REVOKE ${role} FROM ${otherName}`);
   await admin.query(`GRANT ${otherName} TO ${role}`);
   await expect(assertEraseSearchIdentity(login)).rejects.toThrow("login refused");
   await admin.query(`REVOKE ${otherName} FROM ${role}`);
   await admin.query(`ALTER ROLE ${role} INHERIT`);
   await expect(assertEraseSearchIdentity(login)).rejects.toThrow("login refused");
   await admin.query(`ALTER ROLE ${role} NOINHERIT`);
   await expect(assertEraseSearchIdentity(login)).resolves.toBeUndefined();
  } finally {
   await login?.end(); await owner?.end();
   await admin.query(`DROP DATABASE IF EXISTS ${database}`);
   if (created) await admin.query(`DROP ROLE ${role}`);
   await admin.query(`DROP ROLE IF EXISTS ${otherName}; DROP ROLE IF EXISTS ${ownerName}`); await admin.end();
  }
 });
});
