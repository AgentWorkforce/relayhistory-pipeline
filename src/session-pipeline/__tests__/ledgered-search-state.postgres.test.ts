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
import { spawnSync } from "node:child_process";
import { join } from "node:path";
import { runMigrations, type MigrationProfile } from "../../hoopdb/migrate.ts";
import { runtimeRoleViolations } from "../../hoopdb/migrate-guards.ts";
import { ledgeredSearchState } from "../postgres-search-state.ts";
import { admitPortableConnection } from "../../shared/portable-connection-identity.ts";

const url = process.env.PORTABLE_SEARCH_TEST_PG_URL;
/** psql for the external-PASS route (mark_pass.sql as operators run it). */
const PSQL = process.env.PORTABLE_SEARCH_TEST_PSQL ?? "psql";
const root = join(import.meta.dir, "../../..");
const VERIFY = readFileSync(join(root, "supabase/bootstrap/portable_search_storage_verify.sql"), "utf8");
const cleanup: (() => Promise<void>)[] = [];
afterEach(async () => { for (const f of cleanup.splice(0).reverse()) await f(); }, 120_000);

async function database() {
  const id = crypto.randomUUID().replaceAll("-", "").slice(0, 16);
  const name = `ps_state_${id}`;
  const rt = `ps_rt_${id}`;
  const other = `ps_other_${id}`;
  // Cleanup is registered BEFORE any mutation and drops only what this run created
  // (tracked flags), so an early setup failure cannot leak or touch anything else.
  const created = { db: false, rt: false, other: false };
  const clients: pg.Client[] = [];
  const admin = new pg.Client({ connectionString: url });
  clients.push(admin);
  cleanup.push(async () => {
    for (const c of clients.slice(1).reverse()) await c.end().catch(() => undefined);
    try {
      if (created.db) await admin.query(`DROP DATABASE IF EXISTS ${name} WITH (FORCE)`);
      if (created.rt) await admin.query(`DROP ROLE IF EXISTS ${rt}`);
      if (created.other) await admin.query(`DROP ROLE IF EXISTS ${other}`);
    } finally { await admin.end().catch(() => undefined); }
  });
  await admin.connect();
  await admin.query(`CREATE DATABASE ${name}`); created.db = true;
  // The runtime role logs in itself (the fixture's loopback test-only auth); no password.
  await admin.query(`CREATE ROLE ${rt} LOGIN NOINHERIT NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE`); created.rt = true;
  await admin.query(`CREATE ROLE ${other} NOLOGIN NOSUPERUSER NOBYPASSRLS`); created.other = true;
  const at = (user?: string) => { const u = new URL(url!); u.pathname = `/${name}`; if (user) u.username = user; return u.href; };
  const owner = new pg.Client({ connectionString: at() });
  clients.push(owner);
  await owner.connect();
  // What the selfhosted bootstraps do to this database (cf. migration-profile.test.ts).
  await owner.query(`DO $t$ BEGIN EXECUTE format('REVOKE TEMPORARY ON DATABASE %I FROM PUBLIC', current_database()); END $t$`);
  await owner.query(`ALTER DEFAULT PRIVILEGES FOR ROLE ${(await owner.query<{ u: string }>("SELECT current_user AS u")).rows[0]!.u} REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC`);
  const profile: MigrationProfile = { mode: "local", shim: true, bootstrap: true, verify: true, runtimeRole: rt, runtimeLogin: true };
  const client = { query: <R>(text: string, params?: unknown[]) => owner.query(text, params) as unknown as Promise<{ rows: R[] }> };
  /** A separate authenticated connection as the runtime role itself. */
  const runtime = async () => { const c = new pg.Client({ connectionString: at(rt) }); clients.push(c); await c.connect(); return c; };
  const markPass = (vars: Record<string, string>) => {
    const u = new URL(at());
    const args = ["-X", "-v", "ON_ERROR_STOP=1", "-h", u.hostname, "-p", u.port, "-U", decodeURIComponent(u.username), "-d", name];
    for (const [k, v] of Object.entries(vars)) args.push("-v", `${k}=${v}`);
    args.push("-f", join(root, "supabase/bootstrap/session_directory_search_worker_mark_pass.sql"));
    const r = spawnSync(PSQL, args, { encoding: "utf8", env: { PATH: process.env.PATH ?? "" } });
    return { status: r.status, stderr: r.stderr };
  };
  return { name, rt, other, owner, profile, runtime, markPass,
    migrate: (over: Partial<MigrationProfile> = {}) => runMigrations(client as never, { profile: { ...profile, ...over } }) };
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
    expect(await runtimeRoleViolations(t.owner as never, { mode: "local", role: t.rt, login: true }, { ledger: "sessions.__96bulls_migrations", marker: "sessions.__96bulls_role_validation" }, "post")).toEqual([]);
    await verifyAs(t.owner, t.rt);
    // The writer is its own LOGIN connection: no owner, superuser or SET ROLE.
    const w = await t.runtime();
    expect((await w.query<{ u: string; s: string }>("SELECT current_user AS u, session_user AS s")).rows[0]).toEqual({ u: t.rt, s: t.rt });
    await admitPortableConnection(w as never, "writer");
    const state = ledgeredSearchState({ backendId: "opensearch-v1", fromMs: 0 });
    await w.query("BEGIN");
    await state.record(w as never, { orgId: "org-a", workspaceId: "ws-a", harness: "codex", sessionId: "s1" }, { revision: 1, rows: 2, complete: true, at: new Date(0), canonicalSessionId: "s1" });
    await w.query("COMMIT");
    const rows = (await w.query<{ org_id: string; source_epoch: string }>("SELECT org_id, source_epoch FROM sessions.search_projection_state")).rows;
    expect(rows).toEqual([{ org_id: "org-a", source_epoch: "-1" }]);
    // TRUNCATE is not granted (RLS does not cover it), nor DDL.
    await expect(w.query("TRUNCATE sessions.search_projection_state")).rejects.toThrow(/permission denied|must be owner/);
    await expect(w.query("ALTER TABLE sessions.search_projection_state DISABLE ROW LEVEL SECURITY")).rejects.toThrow(/must be owner/);
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
    // A drift only the portable storage verifier detects: its own refusal blocks PASS.
    await t.owner.query("ALTER TABLE sessions.search_projection_state NO FORCE ROW LEVEL SECURITY");
    await expect(t.migrate()).rejects.toThrow(/portable search storage is not in the approved shape: .*RLS is not FORCEd/);
    expect((await t.owner.query<{ status: string }>("SELECT status FROM sessions.__96bulls_role_validation")).rows.map(r => r.status)).toEqual(["pending"]);
    await t.owner.query("ALTER TABLE sessions.search_projection_state FORCE ROW LEVEL SECURITY");
    await t.migrate();
    // Standalone verifier: moving both policy and grant to another ordinary role is refused for the bound role.
    await t.owner.query(`REVOKE ALL ON sessions.search_projection_state FROM ${t.rt}; DROP POLICY bulls96_trusted_backend ON sessions.search_projection_state;
      GRANT SELECT, INSERT, UPDATE, DELETE ON sessions.search_projection_state TO ${t.other};
      CREATE POLICY bulls96_trusted_backend ON sessions.search_projection_state AS PERMISSIVE FOR ALL TO ${t.other} USING (true) WITH CHECK (true)`);
    await expect(verifyAs(t.owner, t.rt)).rejects.toThrow(/unapproved policy/);
  }, 300_000);

  test("causal M1: a same-name weakened CHECK passes every earlier guard and is refused by the portable verifier on migrate AND external mark_pass; mark_pass needs the exact runtime_role", async () => {
    const t = await database();
    await t.migrate();
    const marker = async () => (await t.owner.query<{ status: string; generation: string }>("SELECT status, generation FROM sessions.__96bulls_role_validation")).rows;
    // Same name, weaker definition; privileges and policies unchanged.
    await t.owner.query(`ALTER TABLE sessions.search_projection_state DROP CONSTRAINT search_projection_state_counts,
      ADD CONSTRAINT search_projection_state_counts CHECK (revision >= 0)`);
    const refused = await t.migrate().catch((e: { code?: string; message?: string }) => e);
    expect((refused as { code?: string }).code).toBe("55000");
    expect((refused as { message?: string }).message).toMatch(/^portable search storage is not in the approved shape: .*constraint search_projection_state_counts definition differs/);
    expect((await marker()).map(r => r.status)).toEqual(["pending"]);
    // External route: the migrator with its verifier off leaves the marker pending ...
    await t.migrate({ verify: false });
    const gen = (await marker())[0]!.generation;
    expect((await marker()).map(r => r.status)).toEqual(["pending"]);
    // ... and mark_pass refuses on the same drift, with the portable verifier's own message.
    const drift = t.markPass({ generation: gen, db_mode: "local", runtime_role: t.rt });
    expect(drift.status).not.toBe(0);
    expect(drift.stderr).toMatch(/portable search storage is not in the approved shape: .*constraint search_projection_state_counts definition differs/);
    expect((await marker()).map(r => r.status)).toEqual(["pending"]);
    // Restore the exact definition.
    await t.owner.query(`ALTER TABLE sessions.search_projection_state DROP CONSTRAINT search_projection_state_counts,
      ADD CONSTRAINT search_projection_state_counts CHECK (revision >= 0 AND row_count >= 0 AND coverage_from_ms >= 0)`);
    // Missing runtime_role: psql stops before PASS.
    const missing = t.markPass({ generation: gen, db_mode: "local" });
    expect(missing.status).not.toBe(0);
    expect((await marker()).map(r => r.status)).toEqual(["pending"]);
    // Wrong (unapproved) runtime_role: refused by the bound verifier.
    const wrong = t.markPass({ generation: gen, db_mode: "local", runtime_role: t.other });
    expect(wrong.status).not.toBe(0);
    expect(wrong.stderr).toMatch(/portable search storage is not in the approved shape/);
    expect((await marker()).map(r => r.status)).toEqual(["pending"]);
    // Exact configured runtime_role: PASS for exactly this generation.
    const ok = t.markPass({ generation: gen, db_mode: "local", runtime_role: t.rt });
    expect(ok.status).toBe(0);
    expect(await marker()).toEqual([{ status: "pass", generation: gen }]);
  }, 300_000);
});
