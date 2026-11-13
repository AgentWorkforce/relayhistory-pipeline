/**
 * The 058000 CONCURRENTLY index prebuild operator (src/hoopdb/index-prebuild-058000.ts) on PGlite over every
 * migration before 058000, with a production-shaped ledger (058000 and every later upstream step unrecorded) and an
 * opaque readiness marker. Census on empty and populated tables, the prebuild gating (dormant, stale census, wrong
 * confirmation, drifted, invalid, census drift, binding drift), resume, an unknown commit read back and never
 * retried, and readback; then the real 058000 migration adopts the prebuilt indexes on populated tables. An INVALID
 * index (what a failed CONCURRENTLY build leaves) is simulated with a superuser UPDATE of pg_index, as in
 * claim-bootstrap-org-indexes.pglite.test.ts. The GitHub gate and workflow shape are in index-prebuild-058000-gate.test.ts.
 */
import { afterEach, expect, setDefaultTimeout, test } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { SESSION_BOOTSTRAP_FILES } from "../session-bootstrap-chain.ts";
import { expectedMigrationNames, schemaStepDigests } from "../migrate.ts";
import { ADOPTABLE_INDEXES } from "../migration-sentinels.ts";
import {
  INDEX_PREBUILD_COUNT,
  INDEX_PREBUILD_ENABLED,
  INDEX_PREBUILD_STEP,
  concurrentStatement,
  indexPrebuildConfirmation,
  indexPrebuildDescriptor,
  indexPrebuildDispatch,
  indexPrebuildSpec,
  parseIndexPrebuildCensus,
  runIndexPrebuild,
  sessionFatalError,
  verifyIndexPrebuildInputs,
  type IndexPrebuildCensusArtifact,
  type IndexPrebuildOutcome,
  type IndexPrebuildReceipt,
  type PrebuildDb,
} from "../index-prebuild-058000.ts";

setDefaultTimeout(300_000);
const root = join(import.meta.dir, "../../..");
const read = (p: string) => readFileSync(join(root, p), "utf8");
const MIGRATION_FILE = "supabase/migrations/20261105058000_operator_user_erase_indexes.sql";
const H = "a".repeat(64);
const GENERATION = "e".repeat(64);
const SHA = "1".repeat(40);
const CENSUS_RUN = "424242";
const SPEC = ADOPTABLE_INDEXES[INDEX_PREBUILD_STEP]!;
const upstream = (await expectedMigrationNames()).filter((n) => n.startsWith("upstream:"));
const HELD = upstream.filter((n) => n >= INDEX_PREBUILD_STEP);
const LEDGER = await schemaStepDigests(HELD);

let cached: File | Blob | undefined;
async function template(): Promise<File | Blob> {
  if (cached) return cached;
  const db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  await db.exec(read("supabase/tests/supabase_shim.sql"));
  for (const f of SESSION_BOOTSTRAP_FILES) await db.exec(read(f));
  for (const f of readdirSync(join(root, "supabase/migrations")).filter((f) => f.endsWith(".sql") && f < "20261105058000").sort())
    await db.exec(read("supabase/migrations/" + f));
  await db.exec(`CREATE TABLE sessions.__96bulls_migrations(name text PRIMARY KEY, sha256 text NOT NULL,
    applied_at timestamptz NOT NULL DEFAULT now())`);
  for (const [name, sha] of LEDGER) await db.query("INSERT INTO sessions.__96bulls_migrations(name, sha256) VALUES($1, $2)", [name, sha]);
  await db.exec(`CREATE TABLE sessions.__96bulls_role_validation(name text PRIMARY KEY,generation text,status text);
    INSERT INTO sessions.__96bulls_role_validation VALUES('session_directory_search_worker','${GENERATION}','pass')`);
  cached = await db.dumpDataDir("none");
  await db.close();
  return cached;
}

const open: PGlite[] = [];
afterEach(async () => { await Promise.all(open.splice(0).map((db) => db.close())); });
async function fresh(): Promise<PGlite> {
  const db = new PGlite({ extensions: { btree_gin, pg_trgm }, loadDataDir: await template() });
  open.push(db);
  return db;
}
type Hook = (sql: string, params: unknown[] | undefined, next: () => Promise<{ rows: any[] }>) => Promise<{ rows: any[] }>;
function adapter(db: PGlite, log: string[] = [], hook?: Hook): PrebuildDb {
  return {
    query: async <R>(sql: string, params?: unknown[]) => {
      log.push(sql);
      const next = () => (params?.length ? db.query(sql, params as never) : db.query(sql)) as Promise<{ rows: any[] }>;
      return (hook ? hook(sql, params, next) : next()) as Promise<{ rows: R[] }>;
    },
  };
}

/** One row in `table`, every NOT NULL column filled by type; FKs and triggers off (replica), checks still enforced. */
async function populate(db: PGlite, table: string): Promise<void> {
  const cols = (await db.query<{ column_name: string; data_type: string }>(
    `SELECT column_name, data_type FROM information_schema.columns
      WHERE table_schema = 'sessions' AND table_name = $1 AND is_nullable = 'NO' AND column_default IS NULL`, [table])).rows;
  const overrides: Record<string, Record<string, string>> = {
    ingest_buffer_outbox: { active_stage_token: "gen_random_uuid()" },
    ingest_history_plans: { state: "'admitted'" },
    ingest_receipt_record_heads: { winner_key_sha256: `'${H}'` },
  };
  const value = (type: string) => type === "uuid" ? "gen_random_uuid()" : /int/u.test(type) ? "1" : type === "boolean" ? "false"
    : type === "jsonb" ? "'[]'::jsonb" : type.startsWith("timestamp") ? "now()" : `'${H}'`;
  const extra = overrides[table] ?? {};
  const names = [...cols.map((c) => c.column_name), ...Object.keys(extra).filter((k) => !cols.some((c) => c.column_name === k))];
  const values = names.map((n) => extra[n] ?? value(cols.find((c) => c.column_name === n)!.data_type));
  await db.exec(`SET session_replication_role = replica;
    INSERT INTO sessions.${table}(${names.join(",")}) VALUES (${values.join(",")});
    RESET session_replication_role;`);
}
/** The three tables populated in these tests (production-shaped: some populated, some empty). */
const POPULATED = ["ingest_lazy_native_slots", "ingest_buffer_outbox", "ingest_receipt_seats"];
const REQUIRED = SPEC.filter((s) => POPULATED.includes(s.table.slice("sessions.".length))).map((s) => s.name);

const env = (mode: string, extra: Record<string, string> = {}) => ({ PREBUILD_MODE: mode, PREBUILD_SHA: SHA, ...extra });
const T0 = Date.now();
async function census(db: PGlite, now = T0): Promise<IndexPrebuildOutcome> {
  return runIndexPrebuild(env("census"), { db: adapter(db) }, { runId: CENSUS_RUN, now: () => now });
}
const prebuildEnv = (c: IndexPrebuildCensusArtifact, confirmation = indexPrebuildConfirmation(c)) =>
  env("prebuild", { PREBUILD_CENSUS_RUN: c.censusRun, PREBUILD_CONFIRMATION: confirmation });
interface RunOpts { now?: number; log?: string[]; hook?: Hook; enabled?: boolean; confirmation?: string; persisted?: IndexPrebuildReceipt[]; fresh?: boolean }
function prebuild(db: PGlite, c: unknown, o: RunOpts = {}): Promise<IndexPrebuildOutcome> {
  const artifact = c as IndexPrebuildCensusArtifact;
  return runIndexPrebuild(prebuildEnv(artifact, o.confirmation), {
    db: adapter(db, o.log, o.hook),
    ...(o.fresh === false ? {} : { fresh: async () => ({ db: adapter(db), close: async () => {} }) }),
  }, { runId: "434343", census: c, now: () => o.now ?? T0 + 3600_000, enabled: o.enabled ?? true,
    persist: (r) => o.persisted?.push(JSON.parse(JSON.stringify(r))) });
}
const indexRows = async (db: PGlite) => (await db.query<{ name: string; valid: boolean; def: string }>(
  `SELECT c.relname AS name, i.indisvalid AS valid, pg_get_indexdef(c.oid) AS def FROM pg_class c JOIN pg_index i ON i.indexrelid = c.oid
    WHERE c.relnamespace = 'sessions'::regnamespace AND c.relname = ANY($1::text[]) ORDER BY c.relname`, [SPEC.map((s) => s.name)])).rows;
const ddl = (log: string[]) => log.filter((s) => /^\s*(CREATE|DROP|ALTER|REINDEX)\b/iu.test(s));
const expectRefused = (o: IndexPrebuildOutcome, code: string) => {
  expect(o.receipt.status).toBe("refused");
  expect(o.receipt.code).toBe(code);
  expect(o.exitCode).toBe(1);
};

test("the sixteen come from ADOPTABLE_INDEXES in declared order, byte-identical to the migration; the only DDL is CONCURRENTLY", () => {
  const spec = indexPrebuildSpec();
  expect(spec).toBe(SPEC);
  expect(spec.length).toBe(INDEX_PREBUILD_COUNT);
  const literals = [...read(MIGRATION_FILE).matchAll(/'(CREATE INDEX [^']+)'/gu)].map((m) => m[1]);
  expect(literals).toEqual(spec.map((s) => s.def));
  for (const s of spec) {
    const stmt = concurrentStatement(s);
    expect(stmt).toBe(s.def.replace(/^CREATE INDEX /u, "CREATE INDEX CONCURRENTLY "));
    expect(stmt.startsWith(`CREATE INDEX CONCURRENTLY ${s.name} ON ${s.table} USING btree (`)).toBe(true);
  }
  // Anything but a declared single btree statement refuses.
  for (const bad of [
    { ...SPEC[0]!, def: SPEC[0]!.def + "; DROP TABLE sessions.x" },
    { ...SPEC[0]!, def: SPEC[0]!.def.replace("CREATE INDEX", "CREATE UNIQUE INDEX") },
    { ...SPEC[0]!, name: "Bad-Name" },
    { ...SPEC[0]!, table: "public.ingest_receipt_ord_seats" },
    { ...SPEC[0]!, def: SPEC[0]!.def.replace("btree", "gin") },
  ]) expect(() => concurrentStatement(bad)).toThrow("index_prebuild_spec");
  // Source: no blocking-build opt-in, no migrator runner, no migration file read, no DROP.
  for (const file of ["src/hoopdb/index-prebuild-058000.ts", "scripts/index-prebuild-058000.ts", "scripts/index-prebuild-058000-github.ts"]) {
    const src = read(file).replace(/^\s*(\*|\/\/).*$/gmu, "");
    for (const forbidden of ["HOOPDB_ALLOW_BLOCKING_INDEX_BUILD", "allow_blocking_index_build", "runMigrations", "migrate()",
      "supabase/migrations/", "DROP INDEX", "pendingMigrationSql"])
      expect(src).not.toContain(forbidden);
  }
});

test("census on empty tables: all sixteen absent and deferred to the migration, nothing to prebuild, read-only, binding recorded", async () => {
  const db = await fresh();
  const log: string[] = [];
  const out = await runIndexPrebuild(env("census"), { db: adapter(db, log) }, { runId: CENSUS_RUN, now: () => T0 });
  expect(out.exitCode).toBe(0);
  const c = out.census!;
  expect(c.verdict).toBe("nothing_to_prebuild");
  expect(c.required).toEqual([]);
  expect(c.indexes.map((i) => i.name)).toEqual(SPEC.map((s) => s.name));
  for (const i of c.indexes)
    expect(i).toMatchObject({ tableExists: true, tableNonEmpty: false, presence: "absent", valid: null, ready: null, live: null,
      action: "deferred", reason: null, progress: null, tableBuilds: 0, defSha256: null, owner: null, tableOwner: "postgres" });
  expect(c).toMatchObject({ generation: GENERATION, markerStatus: "pass", ledgerTail: upstream[upstream.indexOf(INDEX_PREBUILD_STEP) - 1],
    stepCount: LEDGER.length, stepLedgered: false, owner: "postgres", progressRows: 0, unattributableProgressRows: 0,
    sourceSha: SHA, censusRun: CENSUS_RUN });
  expect(parseIndexPrebuildCensus(JSON.parse(JSON.stringify(c))).descriptorSha256).toBe(c.descriptorSha256);
  expect(ddl(log)).toEqual([]);
  expect(log[0]).toBe("BEGIN ISOLATION LEVEL READ COMMITTED READ ONLY");
  expect(log.some((s) => s.includes("pg_try_advisory_xact_lock_shared(961996, 1)"))).toBe(true);
  expect(await indexRows(db)).toEqual([]);
});

test("census on populated tables: exactly the populated-table absent indexes are required, in declared order", async () => {
  const db = await fresh();
  for (const t of POPULATED) await populate(db, t);
  const out = await census(db);
  const c = out.census!;
  expect(out.exitCode).toBe(0);
  expect(c.verdict).toBe("prebuild_required");
  expect(c.required).toEqual(REQUIRED);
  expect(REQUIRED.length).toBe(5);
  for (const i of c.indexes) {
    const populated = POPULATED.includes(i.table.slice("sessions.".length));
    expect(i.tableNonEmpty).toBe(populated);
    expect(i.action).toBe(populated ? "prebuild" : "deferred");
  }
  expect(indexPrebuildConfirmation(c)).toBe(`PREBUILD-058000 ${SHA} GENERATION ${GENERATION} TAIL ${c.ledgerTail} LEDGER ` +
    `${c.ledgerPrefixDigest} DESCRIPTOR ${c.descriptorSha256} CENSUS ${CENSUS_RUN} INDEXES ${REQUIRED.join(",")}`);
});

test("census reports drifted, invalid and wrong-schema objects and blocks; nothing is dropped", async () => {
  const db = await fresh();
  for (const t of POPULATED) await populate(db, t);
  const [a, b] = REQUIRED.map((n) => SPEC.find((s) => s.name === n)!);
  // Drifted: right name, wrong definition.
  await db.exec(`CREATE INDEX ${a!.name} ON ${a!.table} USING btree (org_id)`);
  // Invalid: exact definition, failed CONCURRENTLY build.
  await db.exec(b!.def);
  await db.exec(`UPDATE pg_index SET indisvalid = false WHERE indexrelid = 'sessions.${b!.name}'::regclass`);
  const out = await census(db);
  expect(out.exitCode).toBe(3);
  expect(out.receipt.status).toBe("blocked");
  const c = out.census!;
  expect(c.verdict).toBe("blocked");
  expect(c.indexes.find((i) => i.name === a!.name)).toMatchObject({ presence: "drifted", action: "refused", reason: "drifted", valid: true });
  expect(c.indexes.find((i) => i.name === b!.name)).toMatchObject({ presence: "exact", action: "refused", reason: "invalid", valid: false,
    ready: true, live: true, owner: "postgres", schema: "sessions" });
  expect(c.indexes.find((i) => i.name === b!.name)!.defSha256).toBe(c.indexes.find((i) => i.name === b!.name)!.expectedDefSha256);
  // A blocked census can never feed a prebuild.
  expectRefused(await prebuild(db, c), "index_prebuild_census_blocked");
  expect((await indexRows(db)).map((r) => r.name).sort()).toEqual([a!.name, b!.name].sort());
});

test("prebuild gating: dormant, stale census, wrong confirmation, other sha, forged artifact and nothing-required refuse before any statement", async () => {
  expect(INDEX_PREBUILD_ENABLED).toBe(false);
  const db = await fresh();
  for (const t of POPULATED) await populate(db, t);
  const c = (await census(db)).census!;
  const phrase = indexPrebuildConfirmation(c);
  const cases: Array<[RunOpts & { census?: unknown; envOver?: Record<string, string> }, string]> = [
    [{ enabled: false }, "index_prebuild_dormant"],
    [{ now: Date.parse(c.capturedAt) + 24 * 3600_000 }, "index_prebuild_census_stale"],
    [{ now: Date.parse(c.capturedAt) - 6 * 60_000 }, "index_prebuild_census_stale"],
    [{ confirmation: phrase + " " }, "index_prebuild_confirmation"],
    [{ confirmation: phrase.replace(REQUIRED.join(","), [...REQUIRED].reverse().join(",")) }, "index_prebuild_confirmation"],
    [{ confirmation: phrase.replace(REQUIRED.join(","), REQUIRED.slice(1).join(",")) }, "index_prebuild_confirmation"],
    [{ confirmation: phrase.replace(`GENERATION ${GENERATION}`, `GENERATION ${"f".repeat(64)}`) }, "index_prebuild_confirmation"],
    [{ confirmation: phrase.replace(`TAIL ${c.ledgerTail}`, `TAIL ${upstream[0]}`) }, "index_prebuild_confirmation"],
    [{ census: { ...c, generation: "f".repeat(64) } }, "index_prebuild_census_descriptor"],
    [{ census: { ...c, required: REQUIRED.slice(1) } }, "index_prebuild_census_invalid"],
    [{ census: { ...c, extra: 1 } }, "index_prebuild_census_invalid"],
    [{ census: { ...c, indexes: c.indexes.slice(1) } }, "index_prebuild_census_invalid"],
  ];
  for (const [o, code] of cases) {
    const log: string[] = [];
    const got = await prebuild(db, o.census ?? c, { ...o, log });
    expect(`${code} ${got.receipt.status} ${got.receipt.code}`).toBe(`${code} refused ${code}`);
    expect(log).toEqual([]);
  }
  // Another sha's census, and a census run that is not the dispatched one.
  const log: string[] = [];
  const other = await runIndexPrebuild(env("prebuild", { PREBUILD_SHA: "2".repeat(40), PREBUILD_CENSUS_RUN: CENSUS_RUN, PREBUILD_CONFIRMATION: phrase }),
    { db: adapter(db, log) }, { runId: "1", census: c, enabled: true, now: () => T0 });
  expectRefused(other, "index_prebuild_census_sha_mismatch");
  const run = await runIndexPrebuild(env("prebuild", { PREBUILD_CENSUS_RUN: "7", PREBUILD_CONFIRMATION: phrase }),
    { db: adapter(db, log) }, { runId: "1", census: c, enabled: true, now: () => T0 });
  expectRefused(run, "index_prebuild_census_run_mismatch");
  expect(log).toEqual([]);
  // A census with nothing to prebuild cannot be used either.
  const empty = (await census(await fresh())).census!;
  expectRefused(await prebuild(db, empty), "index_prebuild_nothing_required");
  expect(await indexRows(db)).toEqual([]);
  // Dispatch shape alone.
  expect(() => indexPrebuildDispatch(env("apply"), true)).toThrow("index_prebuild_inputs");
  expect(() => indexPrebuildDispatch(env("census", { PREBUILD_CONFIRMATION: phrase }), true)).toThrow("index_prebuild_confirmation");
  expect(() => indexPrebuildDispatch(env("readback", { PREBUILD_CENSUS_RUN: "1" }), true)).toThrow("index_prebuild_census_run");
});

test("prebuild: one CONCURRENTLY build per required index, readback after each, durable receipts; 058000 then adopts them", async () => {
  const db = await fresh();
  for (const t of POPULATED) await populate(db, t);
  const c = (await census(db)).census!;
  const log: string[] = [];
  const persisted: IndexPrebuildReceipt[] = [];
  const out = await prebuild(db, c, { log, persisted });
  expect(out.receipt.code).toBeUndefined();
  expect(out.exitCode).toBe(0);
  expect(out.receipt.status).toBe("ok");
  expect(ddl(log)).toEqual(REQUIRED.map((n) => concurrentStatement(SPEC.find((s) => s.name === n)!)));
  expect(out.receipt.statements).toBe(REQUIRED.length);
  expect(out.receipt.counts).toMatchObject({ required: 5, built: 5, resumed: 0, refused: 0, deferred: 11 });
  for (const e of out.receipt.indexes)
    expect(e.status).toBe(REQUIRED.includes(e.name) ? "built" : "deferred");
  for (const e of out.receipt.indexes.filter((x) => x.status === "built"))
    expect(e.readback).toMatchObject({ presence: "exact", valid: true, ready: true, live: true, progress: null });
  // Each build: statement then an immediate readback of that index; never inside a transaction.
  for (const n of REQUIRED) {
    const at = log.indexOf(concurrentStatement(SPEC.find((s) => s.name === n)!));
    expect(log.slice(at + 1, at + 6)).toEqual(["RESET statement_timeout", "RESET lock_timeout",
      "BEGIN ISOLATION LEVEL READ COMMITTED READ ONLY", "SET LOCAL row_security = off", expect.stringContaining("pg_stat_progress_create_index")]);
    expect(log.slice(at - 2, at)).toEqual(["SET statement_timeout = 3600000", "SET lock_timeout = 600000"]);
  }
  expect(log.some((s) => s.includes("pg_try_advisory_lock(961996, 58000)"))).toBe(true);
  expect(log.at(-1)).toContain("pg_advisory_unlock(961996, 58000)");
  // A receipt was persisted before and after every build.
  for (const n of REQUIRED) {
    expect(persisted.some((r) => r.indexes.find((e) => e.name === n)?.status === "building")).toBe(true);
    expect(persisted.some((r) => r.indexes.find((e) => e.name === n)?.status === "built")).toBe(true);
  }
  expect(JSON.stringify(persisted)).not.toMatch(/postgres(ql)?:\/\//u);
  // Readback before the migration: the empty-table indexes are still absent, so it is not complete.
  const early = await runIndexPrebuild(env("readback"), { db: adapter(db) }, { runId: "5" });
  expect(early.exitCode).toBe(3);
  expect(early.receipt.status).toBe("incomplete");
  // The real migration adopts the five on populated tables and creates the eleven on empty ones.
  await db.exec("BEGIN");
  await db.exec(read(MIGRATION_FILE));
  await db.exec("COMMIT");
  const rows = await indexRows(db);
  expect(rows.length).toBe(16);
  expect(rows.every((r) => r.valid && SPEC.find((s) => s.name === r.name)!.def === r.def)).toBe(true);
  const done = await runIndexPrebuild(env("readback"), { db: adapter(db) }, { runId: "6" });
  expect(done.exitCode).toBe(0);
  expect(done.receipt.status).toBe("complete");
  expect(done.receipt.counts.present).toBe(16);
});

test("without the prebuild the 058000 migration refuses a populated table (the contract this workflow unblocks)", async () => {
  const db = await fresh();
  await populate(db, "ingest_lazy_native_slots");
  await db.exec("BEGIN");
  const error = await db.exec(read(MIGRATION_FILE)).then(() => null, (e) => e as { code?: string });
  await db.exec("ROLLBACK");
  expect(error?.code).toBe("55000");
});

test("resume: an already exact valid index is accepted; nothing else of a required name is", async () => {
  const db = await fresh();
  for (const t of POPULATED) await populate(db, t);
  const c = (await census(db)).census!;
  const first = SPEC.find((s) => s.name === REQUIRED[0])!;
  await db.exec(concurrentStatement(first));
  const log: string[] = [];
  const out = await prebuild(db, c, { log });
  expect(out.exitCode).toBe(0);
  expect(out.receipt.indexes.find((e) => e.name === first.name)!.status).toBe("resumed_exact");
  expect(out.receipt.counts).toMatchObject({ resumed: 1, built: 4 });
  expect(ddl(log)).toEqual(REQUIRED.slice(1).map((n) => concurrentStatement(SPEC.find((s) => s.name === n)!)));
  // Re-running the same census once everything is built: all five resume, no statement runs.
  const again: string[] = [];
  const rerun = await prebuild(db, c, { log: again });
  expect(rerun.exitCode).toBe(0);
  expect(rerun.receipt.counts).toMatchObject({ resumed: 5, built: 0 });
  expect(ddl(again)).toEqual([]);
});

test("resume refuses an invalid or drifted object of a required name and never drops it", async () => {
  for (const kind of ["invalid", "drifted"] as const) {
    const db = await fresh();
    for (const t of POPULATED) await populate(db, t);
    const c = (await census(db)).census!;
    const target = SPEC.find((s) => s.name === REQUIRED[1])!;
    if (kind === "invalid") {
      await db.exec(target.def);
      await db.exec(`UPDATE pg_index SET indisvalid = false WHERE indexrelid = 'sessions.${target.name}'::regclass`);
    } else await db.exec(`CREATE INDEX ${target.name} ON ${target.table} USING hash (org_id)`);
    const log: string[] = [];
    const out = await prebuild(db, c, { log });
    expectRefused(out, `index_prebuild_refused_${kind}`);
    expect(ddl(log)).toEqual([]);
    expect((await indexRows(db)).map((r) => r.name)).toEqual([target.name]);
  }
});

test("prebuild refuses census drift: a deferred table that became populated, or a moved marker or ledger", async () => {
  const db = await fresh();
  for (const t of POPULATED) await populate(db, t);
  const c = (await census(db)).census!;
  await populate(db, "ingest_history_plans");
  const log: string[] = [];
  expectRefused(await prebuild(db, c, { log }), "index_prebuild_census_drift");
  expect(ddl(log)).toEqual([]);

  const db2 = await fresh();
  for (const t of POPULATED) await populate(db2, t);
  const c2 = (await census(db2)).census!;
  await db2.query("UPDATE sessions.__96bulls_role_validation SET generation = $1", ["f".repeat(64)]);
  expectRefused(await prebuild(db2, c2), "index_prebuild_binding_moved");
  await db2.query("UPDATE sessions.__96bulls_role_validation SET generation = $1, status = 'pending'", [GENERATION]);
  expectRefused(await prebuild(db2, c2), "index_prebuild_binding_moved");
  await db2.query("UPDATE sessions.__96bulls_role_validation SET status = 'pass'");
  await db2.query("INSERT INTO sessions.__96bulls_migrations(name, sha256) VALUES($1, $2)", [INDEX_PREBUILD_STEP, H]);
  const moved = await prebuild(db2, c2);
  expect(moved.receipt.status).toBe("refused");
  expect(moved.receipt.code).not.toBeUndefined();
  expect(await indexRows(db2)).toEqual([]);
});

test("locks: a busy migrator lock or a held prebuild lock refuses without waiting and without any build", async () => {
  const db = await fresh();
  for (const t of POPULATED) await populate(db, t);
  const c = (await census(db)).census!;
  const deny = (needle: string): Hook => (sql, _p, next) => sql.includes(needle) ? Promise.resolve({ rows: [{ ok: false }] }) : next();
  const log: string[] = [];
  expectRefused(await prebuild(db, c, { log, hook: deny("pg_try_advisory_lock(961996, 58000)") }), "index_prebuild_busy");
  expectRefused(await prebuild(db, c, { log, hook: deny("pg_try_advisory_xact_lock_shared(961996, 1)") }), "index_prebuild_migrator_busy");
  const censusLog: string[] = [];
  const busy = await runIndexPrebuild(env("census"), { db: adapter(db, censusLog, deny("pg_try_advisory_xact_lock_shared")) }, { runId: CENSUS_RUN });
  expectRefused(busy, "index_prebuild_migrator_busy");
  expect(ddl(log)).toEqual([]);
  expect([...log, ...censusLog].some((s) => /pg_advisory(_xact)?_lock(_shared)?\(/u.test(s))).toBe(false);
});

test("unknown commit: the outcome is read back on a fresh session and never retried", async () => {
  // Committed, then the connection dropped before the reply.
  const db = await fresh();
  for (const t of POPULATED) await populate(db, t);
  const c = (await census(db)).census!;
  const log: string[] = [];
  const committed: Hook = async (sql, _p, next) => {
    if (sql.startsWith("CREATE INDEX CONCURRENTLY")) { await next(); throw new Error("Connection terminated unexpectedly"); }
    return next();
  };
  const out = await prebuild(db, c, { log, hook: committed });
  expectRefused(out, "index_prebuild_unknown_commit_verified_stop");
  expect(ddl(log).length).toBe(1);
  expect(out.receipt.indexes.find((e) => e.name === REQUIRED[0])).toMatchObject({ status: "committed_after_error", error: "unknown" });
  // The next dispatch resumes it.
  const next = await prebuild(db, c);
  expect(next.exitCode).toBe(0);
  expect(next.receipt.counts).toMatchObject({ resumed: 1, built: 4 });

  // Lost before it ran: read back absent, refused, not retried.
  const db2 = await fresh();
  for (const t of POPULATED) await populate(db2, t);
  const c2 = (await census(db2)).census!;
  const log2: string[] = [];
  const lost: Hook = async (sql, _p, next) => {
    if (sql.startsWith("CREATE INDEX CONCURRENTLY")) throw new Error("Connection terminated unexpectedly");
    return next();
  };
  const out2 = await prebuild(db2, c2, { log: log2, hook: lost });
  expectRefused(out2, "index_prebuild_unknown_commit_absent_after_build");
  expect(ddl(log2).length).toBe(1);
  expect(await indexRows(db2)).toEqual([]);
  // With no way to open a fresh session the outcome stays unverified (refused, not retried), durably recorded.
  const persisted3: IndexPrebuildReceipt[] = [];
  const out3 = await prebuild(db2, c2, { hook: lost, fresh: false, persisted: persisted3 });
  expectRefused(out3, "index_prebuild_unknown_commit_unverified");
  expect(out3.receipt.indexes.find((e) => e.name === REQUIRED[0])).toMatchObject({ status: "outcome_unknown", error: "unknown" });
  expect(persisted3.at(-1)!.indexes.find((e) => e.name === REQUIRED[0])!.status).toBe("outcome_unknown");
});

test("session-fatal classification: no SQLSTATE, FATAL/PANIC, 08, 57P01-57P05, 25P03/25P04, 58, XX are fatal; statement errors are not", () => {
  for (const e of [new Error("Connection terminated unexpectedly"), { code: "ECONNRESET" }, { code: "08006" }, { code: "08003" },
    { code: "57P01" }, { code: "57P02" }, { code: "57P03" }, { code: "57P04" }, { code: "57P05" }, { code: "25P03" },
    { code: "25P04" }, { code: "58030" }, { code: "XX000" }, { code: "23505", severity: "FATAL" }, { code: "42P07", severity: "PANIC" }, null])
    expect(`${JSON.stringify(e)} ${sessionFatalError(e)}`).toBe(`${JSON.stringify(e)} true`);
  for (const e of [{ code: "57014" }, { code: "55P03" }, { code: "23505" }, { code: "42P07" }, { code: "40P01" }, { code: "57014", severity: "ERROR" }])
    expect(`${JSON.stringify(e)} ${sessionFatalError(e)}`).toBe(`${JSON.stringify(e)} false`);
});

test("a connection-fatal SQLSTATE (57P01) is read back on a NEW session only, never on the dead one, and never retried", async () => {
  const setup = async () => {
    const db = await fresh();
    for (const t of POPULATED) await populate(db, t);
    return { db, c: (await census(db)).census! };
  };
  // The backend is terminated after the build committed: the dead session is never queried again.
  const { db, c } = await setup();
  let dead = false;
  const after: string[] = [];
  const terminated: Hook = async (sql, _p, next) => {
    if (dead) { after.push(sql); throw new Error("Connection terminated"); }
    if (sql.startsWith("CREATE INDEX CONCURRENTLY")) {
      await next();
      dead = true;
      throw Object.assign(new Error("terminating connection due to administrator command"), { code: "57P01" });
    }
    return next();
  };
  let freshOpened = 0;
  const persisted: IndexPrebuildReceipt[] = [];
  const log: string[] = [];
  const out = await runIndexPrebuild(prebuildEnv(c), {
    db: adapter(db, log, terminated),
    fresh: async () => { freshOpened++; return { db: adapter(db), close: async () => {} }; },
  }, { runId: "1", census: c, now: () => T0 + 3600_000, enabled: true, persist: (r) => persisted.push(JSON.parse(JSON.stringify(r))) });
  expectRefused(out, "index_prebuild_unknown_commit_verified_stop");
  expect(after).toEqual([]);
  expect(freshOpened).toBe(1);
  expect(ddl(log).length).toBe(1);
  expect(out.receipt.indexes.find((e) => e.name === REQUIRED[0])).toMatchObject({ status: "committed_after_error", error: "57P01",
    readback: { presence: "exact", valid: true } });
  expect(persisted.at(-1)!.indexes.some((e) => e.status === "building")).toBe(false);

  // Terminated mid-build (INVALID left behind): fresh readback refuses it; no retry, nothing dropped.
  const s2 = await setup();
  const target = SPEC.find((s) => s.name === REQUIRED[0])!;
  const midBuild: Hook = async (sql, _p, next) => {
    if (sql.startsWith("CREATE INDEX CONCURRENTLY")) {
      await next();
      await s2.db.exec(`UPDATE pg_index SET indisvalid = false WHERE indexrelid = 'sessions.${target.name}'::regclass`);
      throw Object.assign(new Error("terminating connection"), { code: "57P01", severity: "FATAL" });
    }
    return next();
  };
  const log2: string[] = [];
  const out2 = await prebuild(s2.db, s2.c, { log: log2, hook: midBuild });
  expectRefused(out2, "index_prebuild_unknown_commit_invalid");
  expect(log2.slice(log2.indexOf(concurrentStatement(target)) + 1)).toEqual([]);
  expect(ddl(log2).length).toBe(1);
  expect(await indexRows(s2.db)).toEqual([{ name: target.name, valid: false, def: target.def }]);

  // No fresh session, or the fresh readback itself fails: a durable outcome_unknown receipt, never "building".
  for (const variant of ["no_fresh", "fresh_fails"] as const) {
    const s3 = await setup();
    let gone = false;
    const fatal: Hook = async (sql, _p, next) => {
      if (gone) throw new Error("Connection terminated");
      if (sql.startsWith("CREATE INDEX CONCURRENTLY")) { gone = true; throw Object.assign(new Error("x"), { code: "08006" }); }
      return next();
    };
    const persisted3: IndexPrebuildReceipt[] = [];
    const out3 = await runIndexPrebuild(prebuildEnv(s3.c), {
      db: adapter(s3.db, [], fatal),
      ...(variant === "no_fresh" ? {} : { fresh: async () => ({ db: { query: async () => { throw new Error("refused"); } }, close: async () => {} }) }),
    }, { runId: "1", census: s3.c, now: () => T0 + 3600_000, enabled: true, persist: (r) => persisted3.push(JSON.parse(JSON.stringify(r))) });
    expectRefused(out3, "index_prebuild_unknown_commit_unverified");
    expect(out3.receipt.indexes.find((e) => e.name === REQUIRED[0])).toMatchObject({ status: "outcome_unknown", error: "08006" });
    expect(persisted3.at(-1)!.indexes.find((e) => e.name === REQUIRED[0])!.status).toBe("outcome_unknown");
    expect(out3.receipt.statements).toBe(1);
  }

  // A definite SQLSTATE whose session then fails on RESET is treated as broken: fresh readback, nothing more on it.
  const s4 = await setup();
  let broke = false;
  const lateBreak: Hook = async (sql, _p, next) => {
    if (broke) throw new Error("Connection terminated");
    if (sql.startsWith("CREATE INDEX CONCURRENTLY")) {
      await next();
      broke = true;
      throw Object.assign(new Error("canceling statement due to statement timeout"), { code: "57014" });
    }
    return next();
  };
  const log4: string[] = [];
  const out4 = await prebuild(s4.db, s4.c, { log: log4, hook: lateBreak });
  expectRefused(out4, "index_prebuild_unknown_commit_verified_stop");
  expect(log4.slice(log4.indexOf(concurrentStatement(target)) + 1)).toEqual(["RESET statement_timeout"]);
  expect(out4.receipt.indexes.find((e) => e.name === REQUIRED[0])).toMatchObject({ status: "committed_after_error", error: "57014" });
});

test("a pending readiness marker: census reports it but blocks; no artifact from it can prebuild; zero DDL", async () => {
  const db = await fresh();
  for (const t of POPULATED) await populate(db, t);
  await db.query("UPDATE sessions.__96bulls_role_validation SET status = 'pending'");
  const out = await census(db);
  expect(out.exitCode).toBe(3);
  expect(out.receipt.status).toBe("blocked");
  const c = out.census!;
  expect(c).toMatchObject({ markerStatus: "pending", verdict: "blocked", generation: GENERATION });
  expect(c.indexes.filter((i) => i.action === "prebuild").map((i) => i.name)).toEqual(REQUIRED);
  expect(c.indexes.every((i) => i.action !== "refused")).toBe(true);
  // Even after the marker returns to pass, the pending census is refused before any statement.
  await db.query("UPDATE sessions.__96bulls_role_validation SET status = 'pass'");
  const log: string[] = [];
  expectRefused(await prebuild(db, c, { log }), "index_prebuild_marker_not_pass");
  // A forged prebuild_required verdict with a recomputed descriptor fails the recomputed verdict.
  const { descriptorSha256: _d, ...body } = c;
  const forgedBody = { ...body, verdict: "prebuild_required" as const };
  const forged = { ...forgedBody, descriptorSha256: indexPrebuildDescriptor(forgedBody) };
  expectRefused(await prebuild(db, forged, { log }), "index_prebuild_census_invalid");
  expect(log).toEqual([]);
  // The input check refuses a non-pass marker on its own, whatever the verdict says.
  const dispatch = indexPrebuildDispatch(prebuildEnv(forged), true);
  expect(() => verifyIndexPrebuildInputs(dispatch, forged, T0 + 60_000)).toThrow("index_prebuild_marker_not_pass");
  expect(await indexRows(db)).toEqual([]);

  // A pass census, but the marker turns pending before the run: the re-census refuses, zero DDL.
  const pass = (await census(db)).census!;
  expect(pass.verdict).toBe("prebuild_required");
  await db.query("UPDATE sessions.__96bulls_role_validation SET status = 'pending'");
  const log2: string[] = [];
  const out2 = await prebuild(db, pass, { log: log2 });
  expect(out2.receipt.status).toBe("refused");
  expect(["index_prebuild_binding_moved", "index_prebuild_marker_not_pass"]).toContain(out2.receipt.code!);
  expect(ddl(log2)).toEqual([]);

  // The marker turns pending between builds: the next build is refused before its statement.
  await db.query("UPDATE sessions.__96bulls_role_validation SET status = 'pass'");
  const flip: Hook = async (sql, _p, next) => {
    const r = await next();
    if (sql.startsWith("CREATE INDEX CONCURRENTLY")) await db.query("UPDATE sessions.__96bulls_role_validation SET status = 'pending'");
    return r;
  };
  const log3: string[] = [];
  const out3 = await prebuild(db, pass, { log: log3, hook: flip });
  expectRefused(out3, "index_prebuild_marker_not_pass");
  expect(ddl(log3).length).toBe(1);

  // Readback still reports with a pending marker (read-only), as before.
  const rb = await runIndexPrebuild(env("readback"), { db: adapter(db) }, { runId: "7" });
  expect(rb.receipt.status).toBe("incomplete");
});

test("a same-named index in another schema (any owner or definition) is refused in census and before every build; zero DDL", async () => {
  const db = await fresh();
  for (const t of POPULATED) await populate(db, t);
  const req = SPEC.find((s) => s.name === REQUIRED[0])!;
  const deferred = SPEC.find((s) => !POPULATED.includes(s.table.slice("sessions.".length)))!;
  await db.exec(`CREATE SCHEMA archive; CREATE TABLE archive.t(org_id text, other text);
    CREATE INDEX ${req.name} ON archive.t USING btree (org_id); CREATE INDEX ${deferred.name} ON archive.t USING hash (other)`);
  const out = await census(db);
  expect(out.exitCode).toBe(3);
  const c = out.census!;
  expect(c.verdict).toBe("blocked");
  for (const n of [req.name, deferred.name])
    expect(c.indexes.find((i) => i.name === n)).toMatchObject({ presence: "absent", sameNameCount: 1, action: "refused", reason: "name_collision" });
  expect(parseIndexPrebuildCensus(JSON.parse(JSON.stringify(c))).descriptorSha256).toBe(c.descriptorSha256);
  const log: string[] = [];
  expectRefused(await prebuild(db, c, { log }), "index_prebuild_census_blocked");
  expect(ddl(log)).toEqual([]);

  // A clean census, then a collision appears: the immediate re-inspection refuses before any build.
  const db2 = await fresh();
  for (const t of POPULATED) await populate(db2, t);
  const c2 = (await census(db2)).census!;
  await db2.exec(`CREATE SCHEMA archive; CREATE TABLE archive.t(org_id text); CREATE INDEX ${req.name} ON archive.t USING btree (org_id)`);
  const log2: string[] = [];
  expectRefused(await prebuild(db2, c2, { log: log2 }), "index_prebuild_refused_name_collision");
  expect(ddl(log2)).toEqual([]);

  // A collision that appears between builds: the next index's pre-build inspection refuses it, no statement for it.
  const db3 = await fresh();
  for (const t of POPULATED) await populate(db3, t);
  const c3 = (await census(db3)).census!;
  const second = SPEC.find((s) => s.name === REQUIRED[1])!;
  const collide: Hook = async (sql, _p, next) => {
    const r = await next();
    if (sql.startsWith("CREATE INDEX CONCURRENTLY"))
      await db3.exec(`CREATE SCHEMA IF NOT EXISTS archive; CREATE TABLE IF NOT EXISTS archive.t(org_id text);
        CREATE INDEX IF NOT EXISTS ${second.name} ON archive.t USING btree (org_id)`);
    return r;
  };
  const log3: string[] = [];
  const out3 = await prebuild(db3, c3, { log: log3, hook: collide });
  expectRefused(out3, "index_prebuild_refused_name_collision");
  expect(ddl(log3)).toEqual([concurrentStatement(req)]);
  expect(out3.receipt.indexes.find((e) => e.name === second.name)).toMatchObject({ status: "refused", reason: "name_collision" });
  expect((await indexRows(db3)).map((r) => r.name)).toEqual([req.name]);
});

test("an exact index of a required name with a different owner is refused (wrong_owner); zero DDL", async () => {
  const db = await fresh();
  for (const t of POPULATED) await populate(db, t);
  const req = SPEC.find((s) => s.name === REQUIRED[0])!;
  await db.exec(req.def);
  await db.exec(`CREATE ROLE prebuild_other_owner;
    UPDATE pg_class SET relowner = 'prebuild_other_owner'::regrole WHERE oid = 'sessions.${req.name}'::regclass`);
  const c = (await census(db)).census!;
  expect(c.verdict).toBe("blocked");
  expect(c.indexes.find((i) => i.name === req.name)).toMatchObject({ presence: "exact", owner: "prebuild_other_owner", action: "refused",
    reason: "wrong_owner" });
  const log: string[] = [];
  expectRefused(await prebuild(db, c, { log }), "index_prebuild_census_blocked");
  expect(ddl(log)).toEqual([]);
});

test("a failed build (SQLSTATE) is read back on the same session, refused and its INVALID index left in place", async () => {
  const db = await fresh();
  for (const t of POPULATED) await populate(db, t);
  const c = (await census(db)).census!;
  const target = SPEC.find((s) => s.name === REQUIRED[0])!;
  const cancelled: Hook = async (sql, _p, next) => {
    if (sql.startsWith("CREATE INDEX CONCURRENTLY")) {
      await next();
      await db.exec(`UPDATE pg_index SET indisvalid = false WHERE indexrelid = 'sessions.${target.name}'::regclass`);
      throw Object.assign(new Error("canceling statement due to statement timeout"), { code: "57014" });
    }
    return next();
  };
  const log: string[] = [];
  const out = await prebuild(db, c, { log, hook: cancelled });
  expectRefused(out, "index_prebuild_build_failed");
  expect(out.receipt.indexes.find((e) => e.name === target.name)).toMatchObject({ status: "refused", error: "57014", reason: "invalid",
    readback: { presence: "exact", valid: false } });
  expect(ddl(log).length).toBe(1);
  expect(await indexRows(db)).toEqual([{ name: target.name, valid: false, def: target.def }]);
  // A later prebuild refuses the INVALID index rather than dropping or rebuilding it.
  expectRefused(await prebuild(db, c), "index_prebuild_refused_invalid");
});

test("run budget: no build starts that could not finish inside it; the receipt says how far it got", async () => {
  const db = await fresh();
  for (const t of POPULATED) await populate(db, t);
  const c = (await census(db)).census!;
  let calls = 0;
  const base = T0 + 3600_000;
  const out = await runIndexPrebuild(prebuildEnv(c), { db: adapter(db) }, { runId: "9", census: c, enabled: true,
    // The first calls (preflight, start) are early; then time jumps past the budget.
    now: () => (++calls > 6 ? base + 250 * 60_000 : base) });
  expect(out.exitCode).toBe(4);
  expect(out.receipt.status).toBe("partial_budget");
  expect(out.receipt.indexes.filter((e) => e.reason === "budget").length).toBeGreaterThan(0);
});

test("readback refuses to call a state complete while a build is invalid or a name is drifted", async () => {
  const db = await fresh();
  await db.exec("BEGIN");
  await db.exec(read(MIGRATION_FILE));
  await db.exec("COMMIT");
  expect((await runIndexPrebuild(env("readback"), { db: adapter(db) }, { runId: "1" })).receipt.status).toBe("complete");
  await db.exec(`UPDATE pg_index SET indisready = false WHERE indexrelid = 'sessions.${SPEC[3]!.name}'::regclass`);
  const out = await runIndexPrebuild(env("readback"), { db: adapter(db) }, { runId: "2" });
  expect(out.exitCode).toBe(3);
  expect(out.receipt.indexes.find((e) => e.name === SPEC[3]!.name)).toMatchObject({ status: "refused", reason: "invalid" });
});
