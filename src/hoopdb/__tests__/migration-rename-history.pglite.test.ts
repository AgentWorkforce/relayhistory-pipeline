/**
 * In-process PGlite proofs on the real released history (fixtures from #396): the
 * database an image with the original migration names built, then this image.
 * - Supabase CLI history under either the original or the release version of a rename
 *   alias refuses as mixed_migration_authority before any write (ported from #396
 *   42bc3488).
 * - Readiness (schemaVerdict) and the migrator reach the same verdict on every alias
 *   ledger state, through the one shared resolveRenamedLedger.
 */
import { afterEach, expect, setDefaultTimeout, test } from "bun:test";
import { mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { PGlite } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/index.js";
import { btree_gin } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/contrib/btree_gin.js";
import { pg_trgm } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/contrib/pg_trgm.js";
import { MIGRATION_RENAME_ALIASES } from "../migrate-guards.ts";
import { expectedSchema, plan, runMigrations, type MigrationClient, type MigrationProfile } from "../migrate.ts";
import { MIGRATION_SENTINELS } from "../migration-sentinels.ts";
import { schemaVerdict, type ExpectedSchema, type Transact } from "../../shared/db.ts";

setDefaultTimeout(240_000);
const root = join(import.meta.dir, "../../..");
const fixtures = join(import.meta.dir, "fixtures/released-migration-history");
const physicalRows = JSON.parse(readFileSync(join(fixtures, "ledger.json"), "utf8")) as { name: string; sha256: string }[];
const LOCAL: MigrationProfile = { mode: "local", shim: true, bootstrap: true, verify: false, runtimeRole: "postgres" };
const SELFHOSTED: MigrationProfile = { mode: "selfhosted", shim: true, bootstrap: true, verify: true, runtimeRole: "history_rt", runtimeLogin: false, functionBoundary: "global-default-revoke" };
const open: PGlite[] = [];
afterEach(async () => { await Promise.all(open.splice(0).map((db) => db.close())); });
const failure = async (operation: Promise<unknown>) => operation.then(() => null, (e: unknown) => e as { code?: string; message?: string });
function client(db: PGlite): MigrationClient {
  return { async query<R>(text: string, params?: unknown[]) {
    if (params?.length) return await db.query<R>(text, params) as { rows: R[] };
    const results = await db.exec(text);
    return { rows: (results.at(-1)?.rows ?? []) as R[] };
  } };
}
const transact = (db: PGlite): Transact => (work) => db.transaction((tx) => work(tx as never)) as never;
const stored = async (db: PGlite) => (await db.query<{ name: string; sha256: string }>("SELECT name, sha256 FROM sessions.__96bulls_migrations ORDER BY name")).rows;
const marker = async (db: PGlite) => (await db.query("SELECT * FROM sessions.__96bulls_role_validation")).rows;
const data = async (db: PGlite) => (await db.query("SELECT * FROM sessions.session_index")).rows;
type Step = Awaited<ReturnType<typeof plan>>[number];
const version = (name: string) => name.slice("upstream:".length).split("_")[0]!;

/** A database migrated by the released image: original names, original file bytes. */
async function oldDatabase() {
  const db = new PGlite({ extensions: { btree_gin, pg_trgm } }); open.push(db);
  const current = await plan(LOCAL);
  const originals = new Map(MIGRATION_RENAME_ALIASES.map((a) => [a.migration as string, a]));
  const wanted = new Set(physicalRows.map((r) => r.name));
  const prerequisites = current.filter((step) => step.name.startsWith("shim:") || step.name.startsWith("bootstrap:"));
  const schema = current.filter((step) => step.name.startsWith("upstream:") || step.name.startsWith("96bulls:"))
    .map((step): Step => {
      const a = originals.get(step.name);
      return a ? { name: a.ledger_name, path: join(fixtures, a.ledger_name.slice("upstream:".length)), sentinel: MIGRATION_SENTINELS[a.migration] } : step;
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

test("CLI history under either the original or the release version refuses without changing history, data or marker", async () => {
  const db = await oldDatabase();
  await db.exec("CREATE SCHEMA supabase_migrations; CREATE TABLE supabase_migrations.schema_migrations(version text PRIMARY KEY)");
  const physicalBefore = await stored(db);
  const dataBefore = await data(db);
  const markerBefore = await marker(db);
  const versions = [...new Set(MIGRATION_RENAME_ALIASES.flatMap((a) => [a.ledger_name, a.migration].map(version)))];
  expect(versions.sort()).toEqual(["20261102010000", "20261103010000", "20261104021000", "20261104022000"]);
  expect(markerBefore).toHaveLength(1);
  expect((markerBefore[0] as { status?: string }).status).toBe("pending");
  const originals = new Map<string, string>(MIGRATION_RENAME_ALIASES.map((a) => [a.migration, a.ledger_name]));
  const releasedSteps = (await plan(SELFHOSTED))
    .map((step) => ({ ...step, name: originals.get(step.name) ?? step.name }))
    .filter((step) => physicalBefore.some((row) => row.name === step.name));
  for (const steps of [undefined, releasedSteps]) for (const v of versions) {
    await db.query("INSERT INTO supabase_migrations.schema_migrations(version) VALUES($1)", [v]);
    expect((await failure(runMigrations(client(db), { profile: SELFHOSTED, steps })))?.code).toBe("mixed_migration_authority");
    expect(await stored(db)).toEqual(physicalBefore);
    expect(await data(db)).toEqual(dataBefore);
    expect(await marker(db)).toEqual(markerBefore);
    await db.query("DELETE FROM supabase_migrations.schema_migrations WHERE version=$1", [v]);
  }
});

test("readiness and the migrator agree on every rename-alias ledger state (one shared verdict)", async () => {
  const db = await oldDatabase();
  expect((await runMigrations(client(db), { profile: SELFHOSTED })).applied).toBeGreaterThan(0);
  const baseline = await stored(db);
  const expected: ExpectedSchema = { ...await expectedSchema(), dbMode: "selfhosted" };
  const [event, ingest] = MIGRATION_RENAME_ALIASES;
  // A planned forward repair with other bytes (the ingest alias's repair-missing case).
  const dir = mkdtempSync(join(tmpdir(), "alias-parity-"));
  const changedRepair = join(dir, ingest.repair.slice("upstream:".length));
  writeFileSync(changedRepair, `${readFileSync(join(root, "supabase/migrations", ingest.repair.slice("upstream:".length)), "utf8")}\n-- edited\n`);
  const repairSteps = (await plan(SELFHOSTED)).map((st) => (st.name === ingest.repair ? { ...st, path: changedRepair } : st));
  const repairExpected: ExpectedSchema = { ...expected, shas: { ...expected.shas, [ingest.repair]: "f".repeat(64) } };

  type Case = { name: string; sql: [string, unknown[]][]; code: string | null; steps?: Step[]; readiness?: ExpectedSchema };
  const current: [string, unknown[]][] = MIGRATION_RENAME_ALIASES.flatMap((a): [string, unknown[]][] => [
    ["DELETE FROM sessions.__96bulls_migrations WHERE name=$1", [a.ledger_name]],
    ["INSERT INTO sessions.__96bulls_migrations(name, sha256) VALUES($1, $2)", [a.migration, a.accepted_as_alias_of]],
  ]);
  const cases: Case[] = [
    { name: "current-only", sql: current, code: null },
    { name: "original-only genuine (adopted)", sql: [], code: null },
  ];
  for (const a of [event, ingest]) {
    const insertRelease = (sha: string): [string, unknown[]] => ["INSERT INTO sessions.__96bulls_migrations(name, sha256) VALUES($1, $2)", [a.migration, sha]];
    const setOriginal = (sha: string): [string, unknown[]] => ["UPDATE sessions.__96bulls_migrations SET sha256=$1 WHERE name=$2", [sha, a.ledger_name]];
    cases.push(
      { name: `${a.migration} collision-good`, sql: [insertRelease(a.accepted_as_alias_of)], code: "ledger_alias_collision" },
      { name: `${a.migration} collision-bad`, sql: [setOriginal("0".repeat(64)), insertRelease(a.accepted_as_alias_of)], code: "ledger_alias_collision" },
      { name: `${a.migration} original-only wrong-sha`, sql: [setOriginal("0".repeat(64))], code: "ledger_unknown_step" },
      {
        name: `${a.migration} new-name-with-original-sha`,
        sql: [["DELETE FROM sessions.__96bulls_migrations WHERE name=$1", [a.ledger_name]], insertRelease(a.ledger_sha)],
        code: "migration_changed",
      },
      {
        name: `${a.migration} new-name-with-unknown-sha`,
        sql: [["DELETE FROM sessions.__96bulls_migrations WHERE name=$1", [a.ledger_name]], insertRelease("0".repeat(64))],
        code: "migration_changed",
      },
    );
  }
  cases.push({ name: "repair-missing", sql: [], code: "ledger_alias_repair_missing", steps: repairSteps, readiness: repairExpected });

  const results: Record<string, { migrator: string; readiness: string }> = {};
  for (const c of cases) {
    for (const [text, params] of c.sql) await db.query(text, params);
    const physical = await stored(db);
    const dataBefore = await data(db);
    const markerBefore = await marker(db);
    await db.exec("SET ROLE history_rt");
    let verdict;
    try {
      verdict = await schemaVerdict(c.readiness ?? expected, transact(db));
    } finally {
      await db.exec("RESET ROLE");
    }
    const error = await failure(runMigrations(client(db), { profile: SELFHOSTED, steps: c.steps }));
    results[c.name] = { migrator: error ? String(error.code) : "ok", readiness: verdict.ok ? "ok" : verdict.reason };
    expect(error?.code ?? null).toBe(c.code);
    // The same verdict: ready exactly when the migrator accepts the ledger.
    expect(verdict.ok).toBe(c.code === null);
    if (!verdict.ok) expect(verdict).toEqual({ ok: false, reason: "schema_pending", count: 1 });
    expect(await stored(db)).toEqual(physical);
    expect(await data(db)).toEqual(dataBefore);
    if (c.code !== null) expect(await marker(db)).toEqual(markerBefore);
    // Back to the released baseline for the next case.
    await db.exec("DELETE FROM sessions.__96bulls_migrations");
    for (const row of baseline)
      await db.query("INSERT INTO sessions.__96bulls_migrations(name, sha256) VALUES($1, $2)", [row.name, row.sha256]);
  }
  console.log(JSON.stringify(results));
  expect(Object.keys(results)).toHaveLength(cases.length);
});
