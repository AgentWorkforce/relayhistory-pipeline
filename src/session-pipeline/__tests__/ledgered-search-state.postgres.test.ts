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
import { expectedMigrationNames, generation, runMigrations, type MigrationProfile } from "../../hoopdb/migrate.ts";
import { gatedSearchTransact, portableWorkGate } from "../lambda.ts";
import { createLedgeredSearchComposition } from "../../shared/search-composition.ts";
import { createSearchIndexRegistry } from "@rhc/adapters/defaults.ts";
import { admitPortableReader, gatedReaderRunner, readCommittedTenantRunner, type ReaderAdmission } from "../../shared/portable-reader-admission.ts";
import { poolTenantRunner } from "../../session-read-api/liveness.ts";
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
  /** A pg Pool authenticated as the runtime role, shaped as HoopDb for gatedSearchTransact. */
  const runtimePool = () => {
    const pool = new pg.Pool({ connectionString: at(rt), max: 2 });
    cleanup.push(() => pool.end()); // runs before the database cleanup (reverse order)
    return { pool, query: (q: string, p?: unknown[]) => pool.query(q, p), end: () => pool.end() };
  };
  /** A pool that logs in AS a given role (its own startup identity), like the read API's. */
  const loginPool = (user: string) => {
    const pool = new pg.Pool({ connectionString: at(user), max: 2, statement_timeout: 2_000, options: "-c statement_timeout=2000" });
    cleanup.push(() => pool.end());
    return pool;
  };
  return { name, rt, other, owner, profile, runtime, runtimePool, loginPool, markPass,
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
    // The post-migrate read-role activation, as production runs it (USAGE on sessions,
    // session_index columns/policy, LOGIN). Owned disposable cluster only.
    await t.owner.query(readFileSync(join(root, "supabase/bootstrap/session_read_api_role.sql"), "utf8").split("\n").filter(l => !l.startsWith("\\")).join("\n"));
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
    // 048000 reader contract (applied by the real migrator on this branch): the fixed read
    // role sees exactly its tenant's state rows and contracted columns, never another
    // tenant's, never a non-contract column, and cannot write.
    await w.query("BEGIN");
    await state.record(w as never, { orgId: "org-b", workspaceId: "ws-a", harness: "codex", sessionId: "s1" }, { revision: 1, rows: 1, complete: true, at: new Date(0), canonicalSessionId: "s1" });
    await w.query("COMMIT");
    const asReader = async <R>(org: string, sql: string) => {
      await t.owner.query("BEGIN");
      try {
        await t.owner.query("SET LOCAL ROLE bulls96_read_api");
        await t.owner.query("SELECT set_config('bulls96_read.org_id', $1, true), set_config('bulls96_read.workspace_id', 'ws-a', true)", [org]);
        return (await t.owner.query<R>(sql)).rows;
      } finally { await t.owner.query("ROLLBACK"); }
    };
    expect(await asReader<{ org_id: string }>("org-a", "SELECT org_id FROM sessions.search_projection_state")).toEqual([{ org_id: "org-a" }]);
    expect(await asReader<{ org_id: string }>("org-b", "SELECT org_id FROM sessions.search_projection_state")).toEqual([{ org_id: "org-b" }]);
    expect(await asReader<{ org_id: string }>("org-c", "SELECT org_id FROM sessions.search_projection_state")).toEqual([]);
    await expect(asReader("org-a", "SELECT revision FROM sessions.search_projection_state")).rejects.toThrow(/permission denied/);
    await expect(asReader("org-a", "DELETE FROM sessions.search_projection_state")).rejects.toThrow(/permission denied|read-only/);
    await expect(asReader("org-a", "SELECT search_text FROM sessions.session_directory")).rejects.toThrow(/permission denied/);
    // The read API's admission accepts the fixed read role (not privileged, cannot write).
    await t.owner.query("BEGIN");
    try { await t.owner.query("SET LOCAL ROLE bulls96_read_api"); await admitPortableConnection(t.owner as never, "reader"); }
    finally { await t.owner.query("ROLLBACK"); }
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

  test("transition window on real PG: a marker flip committed by a SECOND connection during verification is refused by the gate's final read (RC pinned, database default REPEATABLE READ); a missing ledger row with old PASS refuses before work", async () => {
    const t = await database();
    await t.migrate();
    // Make the pin causal: without it, every gated transaction would be REPEATABLE READ.
    await t.owner.query(`ALTER DATABASE ${t.name} SET default_transaction_isolation = 'repeatable read'`);
    const gen = await generation();
    const names = await expectedMigrationNames();
    const db = t.runtimePool();
    const real = createLedgeredSearchComposition({ backendId: "opensearch-v1", fromMs: 0, registry: createSearchIndexRegistry() });
    let flipDuringVerify = false;
    const comp = { ...real, admit: async (sql: never, role: never, options: never) => {
      await real.admit!(sql, role, options);
      if (flipDuringVerify) await t.owner.query("UPDATE sessions.__96bulls_role_validation SET status = 'pending'"); // committed on another connection
    } };
    const admission = { storageVerifySql: VERIFY, dbMode: "local", expectedNames: names };
    let worked = 0;
    const pass = () => gatedSearchTransact(db as never, (tx) => portableWorkGate(tx, gen, comp as never, admission), async () => { worked++; });
    await pass(); // warm success
    expect(worked).toBe(1);
    flipDuringVerify = true;
    const refused = await pass().catch((e: { code?: string }) => e);
    expect((refused as { code?: string }).code).toBe("work_gate_marker_pending");
    expect(worked).toBe(1);
    flipDuringVerify = false;
    // Control: the same sequence without the RC pin reads the stale snapshot (proves causality).
    await t.owner.query("UPDATE sessions.__96bulls_role_validation SET status = 'pass'");
    const c = await db.pool.connect();
    try {
      await c.query("BEGIN");
      expect((await c.query("SELECT current_setting('transaction_isolation') AS i")).rows[0].i).toBe("repeatable read");
      expect((await c.query("SELECT status FROM sessions.__96bulls_role_validation")).rows[0].status).toBe("pass");
      await t.owner.query("UPDATE sessions.__96bulls_role_validation SET status = 'pending'");
      expect((await c.query("SELECT status FROM sessions.__96bulls_role_validation")).rows[0].status).toBe("pass"); // stale without the pin
      await c.query("ROLLBACK");
    } finally { c.release(); }
    // Missing expected ledger row, old PASS and same generation, valid storage: refused before work.
    await t.owner.query("UPDATE sessions.__96bulls_role_validation SET status = 'pass'");
    const victim = names.find(n => n.startsWith("upstream:"))!;
    const saved = (await t.owner.query<{ sha256: string }>("SELECT sha256 FROM sessions.__96bulls_migrations WHERE name = $1", [victim])).rows[0]!.sha256;
    await t.owner.query("DELETE FROM sessions.__96bulls_migrations WHERE name = $1", [victim]);
    const pending = await pass().catch((e: { code?: string }) => e);
    expect((pending as { code?: string }).code).toBe("work_gate_schema_pending");
    expect(worked).toBe(1);
    await t.owner.query("INSERT INTO sessions.__96bulls_migrations(name, sha256) VALUES ($1, $2)", [victim, saved]);
    await pass();
    expect(worked).toBe(2);
  }, 300_000);

  test("full reader admission on a real reader startup LOGIN (bulls96_read_api, local trust): assembly + gated completeness reads under default REPEATABLE READ; concurrent flip, identity substitution and writer-role misconfiguration refused", async () => {
    const t = await database();
    await t.migrate();
    await t.owner.query(readFileSync(join(root, "supabase/bootstrap/session_read_api_role.sql"), "utf8").split("\n").filter(l => !l.startsWith("\\")).join("\n"));
    await t.owner.query(`ALTER DATABASE ${t.name} SET default_transaction_isolation = 'repeatable read'`);
    // The writer records state for org-a as itself.
    const w = await t.runtime();
    const writer = ledgeredSearchState({ backendId: "opensearch-v1", fromMs: 0 });
    await w.query("BEGIN");
    await writer.record(w as never, { orgId: "org-a", workspaceId: "ws-a", harness: "codex", sessionId: "s1" }, { revision: 1, rows: 1, complete: true, at: new Date(0), canonicalSessionId: "s1" });
    await w.query("COMMIT");
    const readerPool = t.loginPool("bulls96_read_api");
    const startup = (await readerPool.query("SELECT current_user AS cu, session_user AS su, current_setting('default_transaction_read_only') AS ro, current_setting('statement_timeout') AS st")).rows[0];
    expect(startup).toEqual({ cu: "bulls96_read_api", su: "bulls96_read_api", ro: "on", st: "2s" });
    const a: ReaderAdmission = {
      generation: await generation(), expectedNames: await expectedMigrationNames(),
      storageVerifySql: VERIFY,
      readerVerifySql: readFileSync(join(root, "supabase/bootstrap/session_read_api_role_verify.sql"), "utf8").split("\n").filter(l => !l.startsWith("\\")).join("\n"),
      writerRole: t.rt, dbMode: "local",
    };
    await admitPortableReader(readerPool as never, a); // assembly: PASS
    // Inject a second-connection flip when the gate reaches the storage verifier.
    let flip = false;
    const flipping = { connect: async () => {
      const c = await readerPool.connect();
      return { release: () => c.release(), query: async (text: string, params?: unknown[]) => {
        if (flip && text === a.storageVerifySql) await t.owner.query("UPDATE sessions.__96bulls_role_validation SET status = 'pending'");
        return c.query(text, params);
      } };
    } };
    const reader = ledgeredSearchState({ backendId: "opensearch-v1", fromMs: 0, reader: gatedReaderRunner(readCommittedTenantRunner(flipping as never), a) });
    expect(await reader.partials({ orgId: "org-a", workspaceId: "ws-a" }, { limit: 10 })).toEqual({ sessions: [], overflow: false });
    flip = true;
    await expect(reader.partials({ orgId: "org-a", workspaceId: "ws-a" }, { limit: 10 })).rejects.toThrow("reader_validation_pending");
    flip = false;
    await t.owner.query("UPDATE sessions.__96bulls_role_validation SET status = 'pass'");
    // Control: the shared (ungated) runner inherits the database default REPEATABLE READ.
    const iso = await poolTenantRunner(readerPool as never)("org-a", "ws-a", q => q.query<{ i: string }>("SELECT current_setting('transaction_isolation') AS i"));
    expect(iso.rows[0]!.i).toBe("repeatable read");
    const pinned = await readCommittedTenantRunner(readerPool as never)("org-a", "ws-a", q => q.query<{ i: string }>("SELECT current_setting('transaction_isolation') AS i"));
    expect(pinned.rows[0]!.i).toBe("read committed");
    // Identity substitution refused: the writer's own login, and SET ROLE from another login.
    await expect(admitPortableReader(t.loginPool(t.rt) as never, a)).rejects.toThrow("reader_identity");
    const substitute = { connect: async () => {
      const c = await t.loginPool("rdsadmin").connect();
      await c.query("SET ROLE bulls96_read_api");
      return c;
    } };
    await expect(admitPortableReader(substitute as never, a)).rejects.toThrow("reader_identity");
    // Writer role misconfigured: the storage verifier bound to the wrong writer refuses.
    await expect(admitPortableReader(readerPool as never, { ...a, writerRole: t.other })).rejects.toThrow(/reader_storage_unverified/);
    // Still valid afterwards.
    await admitPortableReader(readerPool as never, a);
  }, 300_000);

  test("readiness RPC natively: each status from real state, input validation, and the fixed-session guard; seeded complete/incomplete/missing/cross-tenant completeness through the gated reader", async () => {
    const t = await database();
    await t.migrate();
    await t.owner.query(readFileSync(join(root, "supabase/bootstrap/session_read_api_role.sql"), "utf8").split("\n").filter(l => !l.startsWith("\\")).join("\n"));
    const gen = await generation();
    const names = (await expectedMigrationNames()).filter(n => !/^(shim|bootstrap|verify):/.test(n));
    const readerPool = t.loginPool("bulls96_read_api");
    const rpc = async (pool: pg.Pool, n: unknown, g: unknown) =>
      (await pool.query<{ v: string }>("SELECT sessions.portable_reader_readiness($1::text[], $2::text) AS v", [n, g])).rows[0]!.v;
    const errCode = async (p: Promise<unknown>) => ((await p.then(() => undefined, (e: { code?: string }) => e)) as { code?: string } | undefined)?.code;
    // Status codes from real state.
    expect(await rpc(readerPool, names, gen)).toBe("pass");
    expect(await rpc(readerPool, names, "f".repeat(64))).toBe("generation_mismatch");
    expect(await rpc(readerPool, [...names, "upstream:99999999999999_not_applied.sql"], gen)).toBe("schema_pending");
    await t.owner.query("UPDATE sessions.__96bulls_role_validation SET status = 'pending'");
    expect(await rpc(readerPool, names, gen)).toBe("validation_pending");
    await t.owner.query("UPDATE sessions.__96bulls_role_validation SET status = 'pass'");
    // Input validation (22023): NULL generation/array/member, empty array, bad member, oversize, bad hex.
    for (const [n, g] of [[names, null], [null, gen], [[...names, null], gen], [[], gen], [["drop table x"], gen], [["upstream:" + "a".repeat(201)], gen], [Array.from({ length: 513 }, (_, i) => `upstream:f${i}.sql`), gen], [names, "XYZ"]] as [unknown, unknown][])
      expect(await errCode(rpc(readerPool, n, g))).toBe("22023");
    // Guard (42501): the owner/superuser, SET ROLE substitution, and the writer's own login.
    expect(await errCode(t.owner.query("SELECT sessions.portable_reader_readiness($1::text[], $2::text)", [names, gen]))).toBe("42501");
    await t.owner.query("BEGIN; SET LOCAL ROLE bulls96_read_api");
    expect(await errCode(t.owner.query("SELECT sessions.portable_reader_readiness($1::text[], $2::text)", [names, gen]))).toBe("42501");
    await t.owner.query("ROLLBACK");
    expect(await errCode(rpc(t.loginPool(t.rt), names, gen))).toMatch(/^42501$/);
    // The reader still may NOT read the ledger or marker directly.
    expect(await errCode(readerPool.query("SELECT 1 FROM sessions.__96bulls_migrations"))).toBe("42501");
    expect(await errCode(readerPool.query("SELECT 1 FROM sessions.__96bulls_role_validation"))).toBe("42501");

    // Seeded completeness fixtures through the production schema (owner inserts pointers and
    // directory rows; the writer records state as itself).
    const seed = async (org: string, sid: string, epoch: number, ready: boolean) => {
      await t.owner.query(`INSERT INTO sessions.session_index(org_id, workspace_id, harness, session_id, machine_id, user_id, created_at, last_activity_at, storage_provider, bucket, manifest_key, trace_id)
        VALUES ($1, 'ws-a', 'codex', $2, 'm1', 'u1', now(), now(), 's3', 'b', 'k/' || $1 || '/' || $2, gen_random_uuid())`, [org, sid]);
      // A ready projection must carry its version/time/sources (session_directory_events_ready).
      await t.owner.query(`INSERT INTO sessions.session_directory(org_id, workspace_id, harness, session_id, created_at, last_activity_at,
          event_projection_epoch, event_projection_ready, event_projection_version, event_projected_at, event_projection_sources)
        VALUES ($1, 'ws-a', 'codex', $2, now(), now(), $3, $4, CASE WHEN $4 THEN 1 ELSE 0 END, CASE WHEN $4 THEN now() END,
                CASE WHEN $4 THEN jsonb_build_object('k/' || $1 || '/' || $2, 1) ELSE '{}'::jsonb END)`, [org, sid, epoch, ready]);
    };
    const w = await t.runtime();
    const writer = ledgeredSearchState({ backendId: "opensearch-v1", fromMs: 0 });
    const record = async (org: string, sid: string, complete: boolean) => {
      await w.query("BEGIN");
      await writer.record(w as never, { orgId: org, workspaceId: "ws-a", harness: "codex", sessionId: sid }, { revision: 1, rows: 1, complete, at: new Date(0), canonicalSessionId: sid });
      await w.query("COMMIT");
    };
    await seed("org-a", "s-complete", 3, true); await record("org-a", "s-complete", true);
    await seed("org-a", "s-partial", 3, true); await record("org-a", "s-partial", false);
    await seed("org-a", "s-stale", 3, true); await record("org-a", "s-stale", true);
    await t.owner.query("UPDATE sessions.session_directory SET event_projection_epoch = 4 WHERE org_id = 'org-a' AND session_id = 's-stale'");
    await seed("org-a", "s-unready", 3, false); await record("org-a", "s-unready", true);
    await seed("org-a", "s-nostate", 3, true);
    await seed("org-b", "s-complete", 3, true); await record("org-b", "s-complete", true);
    await seed("org-b", "s-only-b", 3, true);
    const a: ReaderAdmission = {
      generation: gen, expectedNames: await expectedMigrationNames(), storageVerifySql: VERIFY,
      readerVerifySql: readFileSync(join(root, "supabase/bootstrap/session_read_api_role_verify.sql"), "utf8").split("\n").filter(l => !l.startsWith("\\")).join("\n"),
      writerRole: t.rt, dbMode: "local",
    };
    await admitPortableReader(readerPool as never, a);
    const reader = ledgeredSearchState({ backendId: "opensearch-v1", fromMs: 0, reader: gatedReaderRunner(readCommittedTenantRunner(readerPool as never), a) });
    expect(await reader.partials({ orgId: "org-a", workspaceId: "ws-a" }, { limit: 50 })).toEqual({ sessions: ["s-nostate", "s-partial", "s-stale", "s-unready"], overflow: false });
    expect(await reader.partials({ orgId: "org-b", workspaceId: "ws-a" }, { limit: 50 })).toEqual({ sessions: ["s-only-b"], overflow: false });
    expect((await reader.partials({ orgId: "org-a", workspaceId: "ws-a" }, { limit: 50, sessionId: "s-complete" })).sessions).toEqual([]);
    // Readiness flips refuse the completeness read before any result.
    await t.owner.query("UPDATE sessions.__96bulls_role_validation SET status = 'pending'");
    await expect(reader.partials({ orgId: "org-a", workspaceId: "ws-a" }, { limit: 50 })).rejects.toThrow("reader_validation_pending");
    await t.owner.query("UPDATE sessions.__96bulls_role_validation SET status = 'pass'");
  }, 300_000);
});
