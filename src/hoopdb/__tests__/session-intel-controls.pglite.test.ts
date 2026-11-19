import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { createHash } from "node:crypto";
import { PGlite } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/index.js";

const migration = readFileSync(new URL("../../../supabase/migrations/20261105062160_session_intel_controls.sql", import.meta.url), "utf8");
const fn = "sessions.session_intel_controls_v1(text,text)";

describe("session intelligence controls schema", () => {
  let db: PGlite;
  beforeAll(async () => {
    db = new PGlite();
    await db.exec("CREATE SCHEMA sessions; CREATE ROLE anon; CREATE ROLE authenticated; CREATE ROLE service_role; CREATE ROLE bulls96_read_api; GRANT USAGE ON SCHEMA sessions TO service_role,anon,authenticated,bulls96_read_api;");
    await db.exec(migration);
  });
  afterAll(async () => { await db?.close(); });
  const read = async (org = "org", ws = "ws") => db.transaction(async (tx) => {
    await tx.exec("SET LOCAL ROLE service_role");
    return (await tx.query<{ feature: string; enabled: boolean }>("SELECT * FROM sessions.session_intel_controls_v1($1,$2)", [org, ws])).rows;
  });
  const enabled = async (org = "org", ws = "ws") => (await read(org, ws)).filter((r) => r.enabled).map((r) => r.feature);
  const code = async (p: Promise<unknown>) => p.then(() => "ok", (e: { code: string }) => e.code);

  test("no rows means off; both global and exact workspace must opt in; other tenants remain off", async () => {
    expect(await read()).toHaveLength(5);
    expect(await enabled()).toEqual([]);
    await db.exec("INSERT INTO sessions.session_intel_controls(scope,feature,enabled,updated_by) VALUES ('global','advisor',true,'fixture');");
    expect(await enabled()).toEqual([]);
    await db.exec("INSERT INTO sessions.session_intel_controls(scope,org_id,workspace_id,feature,enabled,updated_by) VALUES ('workspace','org','ws','advisor',true,'fixture'),('workspace','org','ws','dm',true,'fixture');");
    expect(await enabled()).toEqual(["advisor"]);
    expect(await enabled("org", "other")).toEqual([]);
    expect(await enabled("other", "ws")).toEqual([]);
    await db.exec("UPDATE sessions.session_intel_controls SET enabled=false WHERE scope='global';");
    expect(await enabled()).toEqual([]);
    await db.exec("UPDATE sessions.session_intel_controls SET enabled=true WHERE scope='global'; UPDATE sessions.session_intel_controls SET enabled=false WHERE scope='workspace' AND feature='advisor';");
    expect(await enabled()).toEqual([]);
  });

  test("global null scope is unique, invalid tenant/scope and unknown features are refused", async () => {
    expect(await code(db.exec("INSERT INTO sessions.session_intel_controls(scope,feature,enabled,updated_by) VALUES ('global','advisor',false,'fixture')"))).toBe("23505");
    for (const stmt of [
      "INSERT INTO sessions.session_intel_controls(scope,feature,updated_by) VALUES ('workspace','regen','fixture')",
      "INSERT INTO sessions.session_intel_controls(scope,org_id,feature,updated_by) VALUES ('global','org','regen','fixture')",
      "INSERT INTO sessions.session_intel_controls(scope,feature,updated_by) VALUES ('global','unknown','fixture')",
    ]) expect(await code(db.exec(stmt))).toBe("23514");
    expect(await code(read("", "ws"))).toBe("22023");
  });

  test("RLS on; pinned definer getter is service-only; no runtime role can read or write the table", async () => {
    const metadata = (await db.query<{ rls: boolean; definer: boolean; config: string[]; public_exec: boolean }>(
      `SELECT c.relrowsecurity rls,p.prosecdef definer,p.proconfig config,
        EXISTS(SELECT 1 FROM aclexplode(p.proacl) a WHERE a.grantee=0) public_exec
        FROM pg_class c,pg_proc p WHERE c.oid='sessions.session_intel_controls'::regclass AND p.oid=$1::regprocedure`, [fn])).rows[0]!;
    expect(metadata).toEqual({ rls: true, definer: true, config: ["search_path=pg_catalog, sessions, pg_temp"], public_exec: false });
    for (const role of ["service_role", "bulls96_read_api", "anon", "authenticated"]) {
      const permissions = (await db.query<{ select: boolean; insert: boolean; execute: boolean }>(
        "SELECT has_table_privilege($1,'sessions.session_intel_controls','SELECT') AS select, has_table_privilege($1,'sessions.session_intel_controls','INSERT') AS insert, has_function_privilege($1,$2,'EXECUTE') AS execute", [role, fn])).rows[0]!;
      expect(permissions).toEqual({ select: false, insert: false, execute: role === "service_role" });
      expect(await code(db.transaction(async (tx) => { await tx.exec(`SET LOCAL ROLE ${role}`); await tx.exec("INSERT INTO sessions.session_intel_controls(scope,feature,updated_by) VALUES ('global','regen','fixture')"); }))).toBe("42501");
    }
  });

  test("rollback requires its checksum and newest ledger slot; reapply enables nothing", async () => {
    const name = "upstream:20261105062160_session_intel_controls.sql";
    const down = readFileSync(new URL("../../../supabase/rollbacks/20261105062160_session_intel_controls.down.sql", import.meta.url), "utf8");
    const rollback = async () => {
      try { await db.exec(down); }
      catch (e) { await db.exec("ROLLBACK"); throw e; }
    };
    await db.exec("CREATE TABLE sessions.__96bulls_migrations(name text PRIMARY KEY,sha256 text NOT NULL)");
    await db.query("INSERT INTO sessions.__96bulls_migrations VALUES ($1,'wrong')", [name]);
    expect(await code(rollback())).toBe("55000");
    await db.query("UPDATE sessions.__96bulls_migrations SET sha256=$1 WHERE name=$2", [createHash("sha256").update(migration).digest("hex"), name]);
    await db.exec("INSERT INTO sessions.__96bulls_migrations VALUES ('upstream:20261105099000_probe.sql','probe')");
    expect(await code(rollback())).toBe("55000");
    await db.exec("DELETE FROM sessions.__96bulls_migrations WHERE name='upstream:20261105099000_probe.sql'");
    await rollback();
    expect((await db.query<{ gone: boolean }>("SELECT to_regclass('sessions.session_intel_controls') IS NULL AS gone")).rows[0]?.gone).toBe(true);
    expect(await code(rollback())).toBe("55000");
    await db.exec(migration);
    expect(await enabled()).toEqual([]);
  });
});
