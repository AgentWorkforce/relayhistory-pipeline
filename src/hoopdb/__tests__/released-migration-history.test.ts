import { afterEach, expect, setDefaultTimeout, test } from "bun:test";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { PGlite } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/index.js";
import { btree_gin } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/contrib/btree_gin.js";
import { pg_trgm } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/contrib/pg_trgm.js";
import { effectiveMigrationLedger, RELEASED_MIGRATION_NAME_ALIASES } from "../released-migration-history.ts";
import { sequencePreflight, sessionsFunctionAclDrift } from "../migrate-guards.ts";
import { expectedSchema, migrationLedgerSha256, plan, runMigrations, type MigrationClient, type MigrationProfile } from "../migrate.ts";
import { MIGRATION_SENTINELS } from "../migration-sentinels.ts";
import { schemaVerdict, type Transact } from "../../shared/db.ts";

setDefaultTimeout(60_000);
const root = join(import.meta.dir, "../../..");
const fixtures = join(import.meta.dir, "fixtures/released-migration-history");
const physicalRows = JSON.parse(readFileSync(join(fixtures, "ledger.json"), "utf8")) as { name: string; sha256: string }[];
const released = () => new Map(physicalRows.map((row) => [row.name, row.sha256]));
const LOCAL: MigrationProfile = { mode: "local", shim: true, bootstrap: true, verify: false, runtimeRole: "postgres" };
const SELFHOSTED: MigrationProfile = { mode: "selfhosted", shim: true, bootstrap: true, verify: true, runtimeRole: "history_rt", runtimeLogin: false, functionBoundary: "global-default-revoke" };
const open: PGlite[] = [];
afterEach(async () => { await Promise.all(open.splice(0).map((db) => db.close())); });
const digest = (text: string) => createHash("sha256").update(text).digest("hex");
const failure = async (operation: Promise<unknown>) => operation.then(() => null, (e: unknown) => e as { code?: string });
function client(db: PGlite): MigrationClient {
  return { async query<R>(text: string, params?: unknown[]) {
    if (params?.length) return await db.query<R>(text, params) as { rows: R[] };
    const results = await db.exec(text);
    return { rows: (results.at(-1)?.rows ?? []) as R[] };
  } };
}
const transact = (db: PGlite): Transact => (work) => db.transaction((tx) => work(tx as never)) as never;
const stored = async (db: PGlite) => (await db.query<{ name: string; sha256: string }>("SELECT name, sha256 FROM sessions.__96bulls_migrations ORDER BY name")).rows;

test("only the exact released names and SQL hashes have an effective view; physical plan binding is unchanged", () => {
  const physical = released();
  const binding = migrationLedgerSha256(physical);
  const effective = effectiveMigrationLedger(physical);
  for (const alias of RELEASED_MIGRATION_NAME_ALIASES) {
    expect(physical.get(alias.original)).toBe(alias.originalSha256);
    expect(effective.has(alias.original)).toBe(false);
    expect(effective.get(alias.current)).toBe(alias.currentSha256);
    expect(digest(readFileSync(join(fixtures, alias.original.slice("upstream:".length)), "utf8"))).toBe(alias.originalSha256);
    expect(digest(readFileSync(join(root, "supabase/migrations", alias.current.slice("upstream:".length)), "utf8"))).toBe(alias.currentSha256);
  }
  expect(migrationLedgerSha256(physical)).toBe(binding);
  expect(migrationLedgerSha256(effective)).not.toBe(binding);
});

test("wrong hash and old/new collision refuse, including when hashes otherwise match", () => {
  for (const alias of RELEASED_MIGRATION_NAME_ALIASES) {
    expect(() => effectiveMigrationLedger(new Map([[alias.original, "0".repeat(64)]]))).toThrow("unrecognized checksum");
    expect(() => effectiveMigrationLedger(new Map([[alias.original, alias.originalSha256], [alias.current, alias.currentSha256]]))).toThrow("both released and renamed");
  }
});

test("current histories are identical and arbitrary unknown names still refuse before any query", async () => {
  const current = new Map(RELEASED_MIGRATION_NAME_ALIASES.map((a) => [a.current, a.currentSha256]));
  expect(effectiveMigrationLedger(current)).toEqual(current);
  let queries = 0;
  const sql: MigrationClient = { query: async () => { queries++; return { rows: [] }; } };
  expect((await failure(sequencePreflight(sql, [], new Map([["upstream:unrecognized.sql", "a".repeat(64)]]))))?.code).toBe("ledger_unknown_step");
  expect(queries).toBe(0);
});

test("a released ingest history cannot be admitted without its exact forward security repair", async () => {
  const alias = RELEASED_MIGRATION_NAME_ALIASES[1];
  const sql: MigrationClient = { query: async () => { throw new Error("unexpected query"); } };
  expect((await failure(sequencePreflight(sql, [], new Map([[alias.original, alias.originalSha256]]))))?.code).toBe("ledger_alias_repair_missing");
});

test("the exact aliases do not admit a gap, changed hash, collision or unknown additional step", async () => {
  const steps = (await plan(SELFHOSTED)).filter((s) => s.name.startsWith("upstream:") || s.name.startsWith("96bulls:"));
  const cases: { ledger: Map<string, string>; code: string }[] = [];
  const gap = released(); gap.delete("upstream:20261025000600_pipeline_queue.sql");
  cases.push({ ledger: gap, code: "ledger_gap" });
  const changed = released(); changed.set(RELEASED_MIGRATION_NAME_ALIASES[0].original, "0".repeat(64));
  cases.push({ ledger: changed, code: "migration_changed" });
  const collision = released(); collision.set(RELEASED_MIGRATION_NAME_ALIASES[0].current, RELEASED_MIGRATION_NAME_ALIASES[0].currentSha256);
  cases.push({ ledger: collision, code: "ledger_alias_collision" });
  const unknown = released(); unknown.set("upstream:never-shipped.sql", "1".repeat(64));
  cases.push({ ledger: unknown, code: "ledger_unknown_step" });
  for (const item of cases) {
    let queries = 0;
    const sql: MigrationClient = { query: async () => { queries++; throw new Error("unexpected query"); } };
    const before = [...item.ledger];
    expect((await failure(sequencePreflight(sql, steps, item.ledger)))?.code).toBe(item.code);
    expect([...item.ledger]).toEqual(before);
    expect(queries).toBe(0);
  }
});

async function oldDatabase() {
  const db = new PGlite({ extensions: { btree_gin, pg_trgm } }); open.push(db);
  const current = await plan(LOCAL);
  const originals = new Map(RELEASED_MIGRATION_NAME_ALIASES.map((a) => [a.current, a]));
  const wanted = new Set(physicalRows.map((r) => r.name));
  const prerequisites = current.filter((step) => step.name.startsWith("shim:") || step.name.startsWith("bootstrap:"));
  const schema = current.filter((step) => step.name.startsWith("upstream:") || step.name.startsWith("96bulls:"))
    .map((step) => {
      const alias = originals.get(step.name);
      return alias ? { name: alias.original, path: join(fixtures, alias.original.slice("upstream:".length)), sentinel: MIGRATION_SENTINELS[alias.current] } : step;
    }).filter((step) => wanted.has(step.name));
  schema.sort((a, b) => a.name.startsWith("upstream:") !== b.name.startsWith("upstream:") ? (a.name.startsWith("upstream:") ? -1 : 1) : a.name.localeCompare(b.name));
  await runMigrations(client(db), { profile: LOCAL, steps: [...prerequisites, ...schema] });
  expect(await stored(db)).toEqual(physicalRows);
  await db.exec(`INSERT INTO sessions.session_index(org_id,workspace_id,harness,session_id,machine_id,user_id,title,created_at,last_activity_at,storage_provider,bucket,manifest_key,trace_id)
    VALUES('org_history','ws_history','codex','released-session','machine_history','user_history','retained history',now(),now(),'s3','history-fixture','96bulls/org_history/v1/ws=ws_history/m=machine_history/h=codex/d=2026/11/08/s=released-session/manifest.json','01984624-2222-7000-8000-111111111111');`);
  await db.exec(`CREATE ROLE history_rt NOLOGIN NOINHERIT; ALTER DEFAULT PRIVILEGES FOR ROLE postgres REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;
    DO $$ BEGIN EXECUTE format('REVOKE TEMPORARY ON DATABASE %I FROM PUBLIC',current_database()); END $$;`);
  return db;
}

test("CLI history under either released or current version refuses without changing history, data or marker", async () => {
  const db = await oldDatabase();
  await db.exec("CREATE SCHEMA supabase_migrations; CREATE TABLE supabase_migrations.schema_migrations(version text PRIMARY KEY)");
  const physicalBefore = await stored(db);
  const dataBefore = (await db.query("SELECT * FROM sessions.session_index")).rows;
  const markerBefore = (await db.query("SELECT * FROM sessions.__96bulls_role_validation")).rows;
  const versions = [...new Set(RELEASED_MIGRATION_NAME_ALIASES.flatMap(alias =>
    [alias.original, alias.current].map(name => name.slice("upstream:".length).split("_")[0]!)))];
  expect(versions.sort()).toEqual(["20261102010000", "20261103010000", "20261104021000", "20261104022000"]);
  expect(markerBefore).toHaveLength(1);
  expect(markerBefore[0]?.status).toBe("pending");
  const originals = new Map<string, string>(RELEASED_MIGRATION_NAME_ALIASES.map(alias => [alias.current, alias.original]));
  const releasedSteps = (await plan(SELFHOSTED))
    .map(step => ({ ...step, name: originals.get(step.name) ?? step.name }))
    .filter(step => physicalBefore.some(row => row.name === step.name));
  for (const steps of [undefined, releasedSteps]) for (const version of versions) {
    await db.query("INSERT INTO supabase_migrations.schema_migrations(version) VALUES($1)", [version]);
    expect((await failure(runMigrations(client(db), { profile: SELFHOSTED, steps })))?.code).toBe("mixed_migration_authority");
    expect(await stored(db)).toEqual(physicalBefore);
    expect((await db.query("SELECT * FROM sessions.session_index")).rows).toEqual(dataBefore);
    expect((await db.query("SELECT * FROM sessions.__96bulls_role_validation")).rows).toEqual(markerBefore);
    await db.query("DELETE FROM supabase_migrations.schema_migrations WHERE version=$1", [version]);
  }
});

test("real released history upgrades through every repair, preserves rows/owners/physical ledger, and reruns cleanly", async () => {
  const db = await oldDatabase();
  const physical = await stored(db);
  const before = (await db.query("SELECT * FROM sessions.session_index")).rows;
  const ownerBefore = (await db.query("SELECT nspowner FROM pg_namespace WHERE nspname='sessions'")).rows;
  expect((await db.query<{ allowed: boolean }>("SELECT has_function_privilege('bulls96_session_ingest_seeder','sessions.ingest_seed_records(jsonb)','EXECUTE') AS allowed")).rows[0]?.allowed).toBe(true);
  const result = await runMigrations(client(db), { profile: SELFHOSTED });
  expect(result.applied).toBeGreaterThan(0);
  const after = await stored(db);
  for (const row of physical) expect(after.find((r) => r.name === row.name)).toEqual(row);
  for (const alias of RELEASED_MIGRATION_NAME_ALIASES) expect(after.some((r) => r.name === alias.current)).toBe(false);
  expect((await db.query("SELECT * FROM sessions.session_index")).rows).toEqual(before);
  expect((await db.query("SELECT nspowner FROM pg_namespace WHERE nspname='sessions'")).rows).toEqual(ownerBefore);
  expect((await db.query<{ allowed: boolean }>("SELECT has_function_privilege('bulls96_session_ingest_seeder','sessions.ingest_seed_records(jsonb)','EXECUTE') AS allowed")).rows[0]?.allowed).toBe(false);
  expect(await sessionsFunctionAclDrift(client(db), "history_rt")).toEqual([]);
  await db.exec("SET ROLE history_rt");
  expect(await schemaVerdict({ ...await expectedSchema(), dbMode: "selfhosted" }, transact(db))).toEqual({ ok: true });
  await db.exec("RESET ROLE");
  expect((await runMigrations(client(db), { profile: SELFHOSTED })).applied).toBe(0);
  expect(await stored(db)).toEqual(after);
});

test("failure at the security repair leaves pending, and a normal retry finishes without rewriting history", async () => {
  const db = await oldDatabase(); const sql = client(db);
  const failing: MigrationClient = { query: async (text, params) => {
    if (text.startsWith("-- S3-proven ingest replay bounds.")) throw Object.assign(new Error("injected repair failure"), { code: "XX000" });
    return sql.query(text, params);
  } };
  expect((await failure(runMigrations(failing, { profile: SELFHOSTED })))?.code).toBe("XX000");
  expect((await db.query<{ status: string }>("SELECT status FROM sessions.__96bulls_role_validation")).rows[0]?.status).toBe("pending");
  expect((await schemaVerdict({ ...await expectedSchema(), dbMode: "selfhosted" }, transact(db))).ok).toBe(false);
  await runMigrations(sql, { profile: SELFHOSTED });
  expect(await schemaVerdict({ ...await expectedSchema(), dbMode: "selfhosted" }, transact(db))).toEqual({ ok: true });
  for (const row of physicalRows) expect((await stored(db)).find((r) => r.name === row.name)).toEqual(row);
});
