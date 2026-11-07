/**
 * Ledgered portable search state (20261105047000) on actual PostgreSQL, through the
 * real migrator. Opt in with an owned, synthetic, disposable admin URL in
 * PORTABLE_SEARCH_TEST_PG_URL; never a deployed database. Each test creates its own
 * database and uniquely named runtime/foreign roles, and drops them afterwards.
 * Fixed bootstrap roles are cluster-wide and idempotent (same pattern as the C3
 * privacy-authority suite); nothing here alters them.
 */
import { afterEach, describe, expect, test } from "bun:test";
import pg from "pg";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { runMigrations, type MigrationProfile } from "../../hoopdb/migrate.ts";
import { runtimeRoleViolations } from "../../hoopdb/migrate-guards.ts";
import { ledgeredSearchState } from "../postgres-search-state.ts";
import { admitPortableConnection } from "../../shared/portable-connection-identity.ts";

const url = process.env.PORTABLE_SEARCH_TEST_PG_URL;
const root = join(import.meta.dir, "../../..");
const VERIFY = readFileSync(join(root, "supabase/bootstrap/portable_search_storage_verify.sql"), "utf8");
const cleanup: (() => Promise<void>)[] = [];
afterEach(async () => { for (const f of cleanup.splice(0).reverse()) await f(); }, 120_000);

async function database() {
  const id = crypto.randomUUID().replaceAll("-", "").slice(0, 16);
  const name = `ps_state_${id}`;
  const rt = `ps_rt_${id}`;
  const other = `ps_other_${id}`;
  const admin = new pg.Client({ connectionString: url });
  await admin.connect();
  await admin.query(`CREATE DATABASE ${name}`);
  await admin.query(`CREATE ROLE ${rt} NOLOGIN NOINHERIT NOSUPERUSER NOBYPASSRLS; CREATE ROLE ${other} NOLOGIN NOSUPERUSER NOBYPASSRLS`);
  const target = new URL(url!);
  target.pathname = `/${name}`;
  const owner = new pg.Client({ connectionString: target.href });
  await owner.connect();
  cleanup.push(async () => {
    await owner.end().catch(() => undefined);
    await admin.query(`DROP DATABASE ${name} WITH (FORCE)`);
    await admin.query(`DROP ROLE IF EXISTS ${rt}; DROP ROLE IF EXISTS ${other}`);
    await admin.end();
  });
  // What the selfhosted bootstraps do to this database (cf. migration-profile.test.ts).
  await owner.query(`DO $t$ BEGIN EXECUTE format('REVOKE TEMPORARY ON DATABASE %I FROM PUBLIC', current_database()); END $t$`);
  await owner.query(`ALTER DEFAULT PRIVILEGES FOR ROLE ${(await owner.query<{ u: string }>("SELECT current_user AS u")).rows[0]!.u} REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC`);
  const profile: MigrationProfile = { mode: "local", shim: true, bootstrap: true, verify: true, runtimeRole: rt, runtimeLogin: false };
  const client = { query: <R>(text: string, params?: unknown[]) => owner.query(text, params) as unknown as Promise<{ rows: R[] }> };
  return { name, rt, other, owner, profile, migrate: () => runMigrations(client as never, { profile }) };
}

const verifyAs = (c: pg.Client, role: string) => c.query("BEGIN").then(async () => {
  try {
    await c.query("SELECT set_config('hoopdb.runtime_role', $1, true), set_config('hoopdb.db_mode', 'local', true)", [role]);
    await c.query(VERIFY);
  } finally { await c.query("ROLLBACK"); }
});

describe.skipIf(!url)("ledgered portable search state on PostgreSQL", () => {
  test("real migrator PASS with the storage verifier in phase 2; the non-owner NOBYPASSRLS writer records without owner credentials", async () => {
    const t = await database();
    await t.migrate();
    const marker = (await t.owner.query<{ status: string }>("SELECT status FROM sessions.__96bulls_role_validation")).rows;
    expect(marker.map(r => r.status)).toEqual(["pass"]);
    expect(await runtimeRoleViolations(t.owner as never, { mode: "local", role: t.rt, login: false }, { ledger: "sessions.__96bulls_migrations", marker: "sessions.__96bulls_role_validation" }, "post")).toEqual([]);
    await verifyAs(t.owner, t.rt);
    // The writer is the runtime role, not the owner: SET ROLE drops every owner/superuser right.
    await t.owner.query(`SET ROLE ${t.rt}`);
    try {
      await admitPortableConnection(t.owner as never, "writer");
      const state = ledgeredSearchState({ backendId: "opensearch-v1", fromMs: 0 });
      await t.owner.query("BEGIN");
      await state.record(t.owner as never, { orgId: "org-a", workspaceId: "ws-a", harness: "codex", sessionId: "s1" }, { revision: 1, rows: 2, complete: true, at: new Date(0), canonicalSessionId: "s1" });
      await t.owner.query("COMMIT");
      const rows = (await t.owner.query<{ org_id: string; source_epoch: string }>("SELECT org_id, source_epoch FROM sessions.search_projection_state")).rows;
      expect(rows).toEqual([{ org_id: "org-a", source_epoch: "-1" }]);
      // TRUNCATE is not granted (RLS does not cover it).
      await expect(t.owner.query("TRUNCATE sessions.search_projection_state")).rejects.toThrow(/permission denied/);
    } finally { await t.owner.query("RESET ROLE"); }
    // FORCE RLS: the owner role itself (not superuser) would see nothing; the read role is refused.
    await t.owner.query("SET ROLE bulls96_read_api");
    try { await expect(t.owner.query("SELECT 1 FROM sessions.search_projection_state")).rejects.toThrow(/permission denied/); }
    finally { await t.owner.query("RESET ROLE"); }
    // Privileged connections are refused at admission (the admin is a superuser).
    await expect(admitPortableConnection(t.owner as never, "writer")).rejects.toThrow("superuser");
  }, 300_000);

  test("drift after PASS: a PUBLIC grant or a moved policy+grant makes the next migrate refuse and leaves the marker pending", async () => {
    const t = await database();
    await t.migrate();
    await t.owner.query("GRANT SELECT ON sessions.search_projection_state TO PUBLIC");
    await expect(t.migrate()).rejects.toThrow();
    expect((await t.owner.query<{ status: string }>("SELECT status FROM sessions.__96bulls_role_validation")).rows.map(r => r.status)).toEqual(["pending"]);
    await t.owner.query("REVOKE SELECT ON sessions.search_projection_state FROM PUBLIC");
    await t.migrate(); // restored: PASS again
    // Standalone verifier: moving both policy and grant to another ordinary role is refused for the bound role.
    await t.owner.query(`REVOKE ALL ON sessions.search_projection_state FROM ${t.rt}; DROP POLICY bulls96_trusted_backend ON sessions.search_projection_state;
      GRANT SELECT, INSERT, UPDATE, DELETE ON sessions.search_projection_state TO ${t.other};
      CREATE POLICY bulls96_trusted_backend ON sessions.search_projection_state AS PERMISSIVE FOR ALL TO ${t.other} USING (true) WITH CHECK (true)`);
    await expect(verifyAs(t.owner, t.rt)).rejects.toThrow(/unapproved policy/);
  }, 300_000);
});
