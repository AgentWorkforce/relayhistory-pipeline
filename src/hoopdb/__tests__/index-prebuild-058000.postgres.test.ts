/**
 * The 058000 CONCURRENTLY index prebuild (src/hoopdb/index-prebuild-058000.ts) on a REAL PostgreSQL server: real
 * concurrent builds, a genuinely in-progress build visible in pg_stat_progress_create_index (held in
 * "waiting for writers" by an open writer transaction), a build backend terminated mid-build (57P01) read back on a
 * fresh session, and the 058000 migration adopting the prebuilt indexes.
 * PGlite cannot run two sessions, so these cases live here.
 *
 * Opt-in OWNED PostgreSQL (SESSION_INGEST_TEST_PG_URL, a superuser URL of a DISPOSABLE server: pg-proof's brief-postgres
 * service container in CI, trust auth, no credential). It creates and drops its own database; skipped otherwise.
 */
import { afterAll, beforeAll, expect, setDefaultTimeout, test } from "bun:test";
import pg from "pg";
import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { SESSION_BOOTSTRAP_FILES } from "../session-bootstrap-chain.ts";
import { expectedMigrationNames, schemaStepDigests } from "../migrate.ts";
import { ADOPTABLE_INDEXES } from "../migration-sentinels.ts";
import {
  INDEX_PREBUILD_STEP,
  concurrentStatement,
  indexPrebuildConfirmation,
  runIndexPrebuild,
  type IndexPrebuildCensusArtifact,
  type PrebuildDb,
} from "../index-prebuild-058000.ts";

setDefaultTimeout(300_000);
const URL_ENV = process.env.SESSION_INGEST_TEST_PG_URL;
const root = join(import.meta.dir, "../../..");
const read = (p: string) => readFileSync(join(root, p), "utf8");
const SPEC = ADOPTABLE_INDEXES[INDEX_PREBUILD_STEP]!;
const SHA = "1".repeat(40);
const DB = `index_prebuild_058000_${process.pid}`;
const clients: pg.Client[] = [];

function urlFor(database: string): string {
  const u = new URL(URL_ENV!);
  u.pathname = "/" + database;
  return u.toString();
}
async function connect(database = DB): Promise<pg.Client> {
  const c = new pg.Client({ connectionString: urlFor(database) });
  c.on("error", () => {});
  await c.connect();
  clients.push(c);
  return c;
}
const adapter = (c: pg.Client, log: string[] = []): PrebuildDb => ({
  query: async <R>(sql: string, params?: unknown[]) => { log.push(sql); return (await c.query(sql, params)) as unknown as { rows: R[] }; },
});

async function populate(c: pg.Client, table: string) {
  const cols = (await c.query<{ column_name: string; data_type: string }>(
    `SELECT column_name, data_type FROM information_schema.columns
      WHERE table_schema = 'sessions' AND table_name = $1 AND is_nullable = 'NO' AND column_default IS NULL`, [table])).rows;
  const extra: Record<string, string> = table === "ingest_buffer_outbox" ? { active_stage_token: "gen_random_uuid()" } : {};
  const value = (type: string) => type === "uuid" ? "gen_random_uuid()" : /int/u.test(type) ? "1" : type === "boolean" ? "false"
    : type === "jsonb" ? "'[]'::jsonb" : type.startsWith("timestamp") ? "now()" : "md5(random()::text) || md5(random()::text)";
  const names = [...cols.map((x) => x.column_name), ...Object.keys(extra).filter((k) => !cols.some((x) => x.column_name === k))];
  const values = names.map((n) => extra[n] ?? value(cols.find((x) => x.column_name === n)!.data_type));
  await c.query("SET session_replication_role = replica");
  await c.query(`INSERT INTO sessions.${table}(${names.join(",")}) VALUES (${values.join(",")})`);
  await c.query("RESET session_replication_role");
}
const POPULATED = ["ingest_lazy_native_slots", "ingest_buffer_outbox", "ingest_receipt_seats"];
const REQUIRED = SPEC.filter((s) => POPULATED.includes(s.table.slice("sessions.".length))).map((s) => s.name);

beforeAll(async () => {
  if (!URL_ENV) return;
  const admin = await connect("postgres");
  await admin.query(`DROP DATABASE IF EXISTS ${DB}`);
  await admin.query(`CREATE DATABASE ${DB}`);
  const c = await connect();
  await c.query(read("supabase/tests/supabase_shim.sql"));
  for (const f of SESSION_BOOTSTRAP_FILES) await c.query(read(f));
  for (const f of readdirSync(join(root, "supabase/migrations")).filter((f) => f.endsWith(".sql") && f < "20261105058000").sort())
    await c.query(read("supabase/migrations/" + f));
  const held = (await expectedMigrationNames()).filter((n) => n.startsWith("upstream:") && n >= INDEX_PREBUILD_STEP);
  await c.query(`CREATE TABLE sessions.__96bulls_migrations(name text PRIMARY KEY, sha256 text NOT NULL, applied_at timestamptz NOT NULL DEFAULT now())`);
  for (const [name, sha] of await schemaStepDigests(held))
    await c.query("INSERT INTO sessions.__96bulls_migrations(name, sha256) VALUES($1, $2)", [name, sha]);
  await c.query(`CREATE TABLE sessions.__96bulls_role_validation(name text PRIMARY KEY, generation text, status text)`);
  await c.query(`INSERT INTO sessions.__96bulls_role_validation VALUES('session_directory_search_worker', $1, 'pass')`, ["e".repeat(64)]);
  for (const t of POPULATED) await populate(c, t);
});
afterAll(async () => {
  if (!URL_ENV) return;
  for (const c of clients.splice(0).reverse()) await c.end().catch(() => {});
  const admin = new pg.Client({ connectionString: urlFor("postgres") });
  await admin.connect();
  await admin.query(`DROP DATABASE IF EXISTS ${DB} WITH (FORCE)`).catch(() => {});
  await admin.end();
});

const census = async (c: pg.Client, runId = "424242") =>
  runIndexPrebuild({ PREBUILD_MODE: "census", PREBUILD_SHA: SHA }, { db: adapter(c) }, { runId });
const prebuild = async (c: pg.Client, artifact: IndexPrebuildCensusArtifact, log: string[] = []) =>
  runIndexPrebuild({ PREBUILD_MODE: "prebuild", PREBUILD_SHA: SHA, PREBUILD_CENSUS_RUN: artifact.censusRun,
    PREBUILD_CONFIRMATION: indexPrebuildConfirmation(artifact) },
  { db: adapter(c, log), fresh: async () => { const f = await connect(); return { db: adapter(f), close: () => f.end() }; } },
  { runId: "434343", census: artifact, enabled: true });

test.skipIf(!URL_ENV)("a build in progress is reported from pg_stat_progress_create_index and refused; prebuild never joins it", async () => {
  const c = await connect();
  const before = (await census(c)).census!;
  expect(before.verdict).toBe("prebuild_required");
  expect(before.required).toEqual(REQUIRED);
  const target = SPEC.find((s) => s.name === REQUIRED[0])!;
  // An open writer on the table holds the concurrent build in "waiting for writers before build".
  const writer = await connect();
  await writer.query("BEGIN");
  await populate(writer, target.table.slice("sessions.".length));
  const builder = await connect();
  const build = builder.query(concurrentStatement(target)).then(() => null, (e) => e);
  for (let i = 0; i < 800; i++) {
    const rows = (await c.query("SELECT count(*)::int n FROM pg_stat_progress_create_index")).rows[0];
    if (rows.n > 0) break;
    await Bun.sleep(25);
  }
  const during = (await census(c, "424243"));
  expect(during.exitCode).toBe(3);
  const entry = during.census!.indexes.find((i) => i.name === target.name)!;
  expect(entry).toMatchObject({ action: "refused", reason: "in_progress", presence: "exact", valid: false, ready: false,
    progress: { command: "CREATE INDEX CONCURRENTLY" } });
  expect(entry.progress!.phase).toMatch(/waiting/u);
  expect(during.census!.progressRows).toBeGreaterThan(0);
  expect(during.census!.verdict).toBe("blocked");
  // A prebuild with the earlier census refuses: nothing runs alongside a live build.
  const log: string[] = [];
  const refused = await prebuild(c, before, log);
  expect(refused.receipt.status).toBe("refused");
  expect(refused.receipt.code).toBe("index_prebuild_in_progress");
  expect(log.some((s) => s.startsWith("CREATE INDEX"))).toBe(false);
  const readback = await runIndexPrebuild({ PREBUILD_MODE: "readback", PREBUILD_SHA: SHA }, { db: adapter(c) }, { runId: "1" });
  expect(readback.receipt.status).toBe("incomplete");
  await writer.query("COMMIT");
  expect(await build).toBeNull();
  const after = (await census(c, "424244")).census!;
  expect(after.indexes.find((i) => i.name === target.name)).toMatchObject({ action: "present", valid: true, ready: true, live: true, progress: null });
});

test.skipIf(!URL_ENV)("a build backend terminated mid-build (57P01, FATAL) is read back on a NEW session, refused, never retried", async () => {
  const c = await connect();
  const artifact = (await census(c, "626262")).census!;
  expect(artifact.required).toEqual(REQUIRED.slice(1));
  const target = SPEC.find((s) => s.name === REQUIRED[1])!;
  const writer = await connect();
  await writer.query("BEGIN");
  await populate(writer, target.table.slice("sessions.".length));
  const b = await connect();
  const log: string[] = [];
  let freshOpened = 0;
  const run = runIndexPrebuild({ PREBUILD_MODE: "prebuild", PREBUILD_SHA: SHA, PREBUILD_CENSUS_RUN: artifact.censusRun,
    PREBUILD_CONFIRMATION: indexPrebuildConfirmation(artifact) },
  { db: adapter(b, log), fresh: async () => { freshOpened++; const f = await connect(); return { db: adapter(f), close: () => f.end() }; } },
  { runId: "636363", census: artifact, enabled: true });
  let pid: number | null = null;
  for (let i = 0; i < 800 && pid === null; i++) {
    pid = (await c.query<{ pid: number }>(`SELECT pid FROM pg_stat_progress_create_index
      WHERE command = 'CREATE INDEX CONCURRENTLY' AND index_relid = to_regclass($1)`, ["sessions." + target.name])).rows[0]?.pid ?? null;
    if (pid === null) await Bun.sleep(25);
  }
  expect(pid).not.toBeNull();
  expect((await c.query("SELECT pg_terminate_backend($1) AS ok", [pid])).rows[0].ok).toBe(true);
  const out = await run;
  await writer.query("COMMIT");
  expect(out.receipt.status).toBe("refused");
  expect(out.receipt.code).toBe("index_prebuild_unknown_commit_invalid");
  expect(freshOpened).toBe(1);
  // Nothing ran on the terminated session after the build statement, and the build was not retried.
  expect(log.slice(log.indexOf(concurrentStatement(target)) + 1)).toEqual([]);
  expect(log.filter((s) => s.startsWith("CREATE INDEX"))).toEqual([concurrentStatement(target)]);
  expect(out.receipt.indexes.find((e) => e.name === target.name)).toMatchObject({ status: "refused", error: "57P01", reason: "invalid",
    readback: { presence: "exact", valid: false } });
  // The INVALID index was left in place; this test (not the operator) removes it so the next case starts clean.
  expect((await c.query("SELECT indisvalid FROM pg_index WHERE indexrelid = to_regclass($1)", ["sessions." + target.name])).rows)
    .toEqual([{ indisvalid: false }]);
  await c.query(`DROP INDEX sessions.${target.name}`);
});

test.skipIf(!URL_ENV)("real concurrent prebuild, then the 058000 migration adopts every prebuilt index and readback is complete", async () => {
  const c = await connect();
  const artifact = (await census(c, "525252")).census!;
  // The first required index was built by the previous case: it resumes; the rest build concurrently.
  expect(artifact.required).toEqual(REQUIRED.slice(1));
  const log: string[] = [];
  const out = await prebuild(c, artifact, log);
  expect(out.receipt.code).toBeUndefined();
  expect(out.exitCode).toBe(0);
  expect(log.filter((s) => s.startsWith("CREATE INDEX"))).toEqual(REQUIRED.slice(1).map((n) => concurrentStatement(SPEC.find((s) => s.name === n)!)));
  await c.query("BEGIN");
  await c.query(read("supabase/migrations/20261105058000_operator_user_erase_indexes.sql"));
  await c.query("COMMIT");
  const done = await runIndexPrebuild({ PREBUILD_MODE: "readback", PREBUILD_SHA: SHA }, { db: adapter(c) }, { runId: "2" });
  expect(done.receipt.status).toBe("complete");
  expect(done.receipt.counts.present).toBe(16);
});
