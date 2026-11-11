/**
 * The release target on the real migrator (runMigrations) in in-process PGlite:
 * steps after the target are held (never applied, never ledgered); phase 2 runs on the
 * prefix and marks PASS for the target (prefix) generation only, never the full-tree
 * one; a pending set or target generation other than the approved one, an
 * applied/unknown target or a plan without a target refuse before any write. The
 * real-tree tests hold the current upstream tail on a full SELFHOSTED migrate and
 * check readiness (schemaVerdict) and the 043700 G2 parity generation.
 */
import { afterEach, expect, setDefaultTimeout, test } from "bun:test";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { PGlite } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/index.js";
import { btree_gin } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/contrib/btree_gin.js";
import { pg_trgm } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/contrib/pg_trgm.js";
import {
  expectedSchema,
  generation,
  generationOfSteps,
  migrationTargetSplit,
  plan,
  runMigrations,
  type MigrationClient,
  type MigrationProfile,
} from "../migrate.ts";
import { sessionsFunctionAclDrift } from "../migrate-guards.ts";
import { schemaVerdict, type ExpectedSchema, type Transact } from "../../shared/db.ts";

setDefaultTimeout(120_000);
const LOCAL: MigrationProfile = { mode: "local", shim: false, bootstrap: false, verify: true, runtimeRole: "postgres" };
const open: PGlite[] = [];
afterEach(async () => {
  await Promise.all(open.splice(0).map((db) => db.close()));
});
function client(db: PGlite, writes?: string[]): MigrationClient {
  return {
    async query<R>(text: string, params?: unknown[]) {
      if (writes && /^\s*(CREATE|ALTER|GRANT|REVOKE|INSERT|UPDATE|DELETE|DROP)\b/i.test(text)) writes.push(text);
      if (params?.length) return (await db.query<R>(text, params)) as { rows: R[] };
      const results = await db.exec(text);
      return { rows: (results.at(-1)?.rows ?? []) as R[] };
    },
  };
}
const dir = mkdtempSync(join(tmpdir(), "migration-target-"));
const step = (name: string, sql: string) => {
  const path = join(dir, name.replace(/[:/]/g, "_"));
  writeFileSync(path, sql);
  const table = /sessions\.(\w+)/.exec(sql)![1]!;
  // A real catalog sentinel: the step's table exists.
  const sentinel = { evidence: table, sql: `SELECT to_regclass('sessions.${table}') IS NOT NULL AS applied` };
  return { name, path, sentinel };
};
const u = (n: string) => `upstream:2099010100000${n}_t${n}.sql`;
const steps = [
  step(u("1"), "CREATE TABLE sessions.t1 (x int);"),
  step(u("2"), "CREATE TABLE sessions.t2 (x int);"),
  step(u("3"), "CREATE TABLE sessions.t3 (x int);"),
  step(u("4"), "CREATE TABLE sessions.t4 (x int);"),
  step("verify:probe", "CREATE TABLE IF NOT EXISTS sessions.verify_ran (x int);"),
];
const fresh = async () => {
  const db = new PGlite();
  open.push(db);
  return db;
};
const ledger = async (db: PGlite) =>
  (await db.query<{ name: string }>("SELECT name FROM sessions.__96bulls_migrations ORDER BY name")).rows.map((r) => r.name);
const marker = async (db: PGlite) =>
  (await db.query<{ generation: string; status: string }>("SELECT generation, status FROM sessions.__96bulls_role_validation")).rows;
const exists = async (db: PGlite, rel: string) =>
  (await db.query<{ r: string | null }>("SELECT to_regclass($1)::text AS r", [rel])).rows[0]!.r !== null;
const failure = (p: Promise<unknown>) => p.then(() => null, (e: { code?: string }) => e.code ?? "no_code");

// The fixture tree's target generation: the same algorithm over its schema steps
// without the held ones (runMigrations computes it from the steps it runs).
const fixtureGeneration = (held: string[]) =>
  generationOfSteps(steps.filter((s) => s.name.startsWith("upstream:") && !held.includes(s.name)));

test("steps after the target are held: never applied or ledgered; phase 2 runs and PASS binds the target generation only", async () => {
  const db = await fresh();
  const gen = await generation();
  const prefix = await fixtureGeneration([u("3"), u("4")]);
  expect(prefix).toMatch(/^[0-9a-f]{64}$/);
  expect(prefix).not.toBe(gen);
  const first = await runMigrations(client(db), { profile: LOCAL, steps, target: u("2"), expectedPending: [u("1"), u("2")], targetGeneration: prefix });
  expect(first).toEqual({ applied: 2, skipped: 0, held: [u("3"), u("4")] });
  expect(await ledger(db)).toEqual([u("1"), u("2")]);
  expect(await exists(db, "sessions.t3")).toBe(false);
  expect(await exists(db, "sessions.t4")).toBe(false);
  expect(await exists(db, "sessions.verify_ran")).toBe(true);
  expect(await marker(db)).toEqual([{ generation: prefix, status: "pass" }]);
  // The next release targets the tail: the held steps apply, phase 2 runs and passes the
  // full generation (nothing held: generation(), unchanged).
  const second = await runMigrations(client(db), { profile: LOCAL, steps, target: u("4"), expectedPending: [u("3"), u("4")], targetGeneration: gen });
  expect(second).toEqual({ applied: 2, skipped: 2, held: [] });
  expect(await ledger(db)).toEqual([u("1"), u("2"), u("3"), u("4")]);
  expect(await marker(db)).toEqual([{ generation: gen, status: "pass" }]);
});

test("a target generation other than the computed one, or one without a target, refuses before any write", async () => {
  const db = await fresh();
  await runMigrations(client(db), { profile: LOCAL, steps, target: u("1"), expectedPending: [u("1")] });
  const ledgerBefore = await ledger(db);
  const markerBefore = await marker(db);
  const full = await generation();
  const right = await fixtureGeneration([u("4")]);
  for (const [opts, code] of [
    // The full-tree generation while steps are held: never accepted as the target.
    [{ target: u("3"), expectedPending: [u("2"), u("3")], targetGeneration: full }, "release_target_generation_changed"],
    // The prefix of a different target.
    [{ target: u("3"), expectedPending: [u("2"), u("3")], targetGeneration: await fixtureGeneration([u("3"), u("4")]) }, "release_target_generation_changed"],
    [{ target: u("3"), expectedPending: [u("2"), u("3")], targetGeneration: right.toUpperCase() }, "release_target_generation_changed"],
    [{ target: u("3"), expectedPending: [u("2"), u("3")], targetGeneration: "" }, "release_target_generation_changed"],
    [{ targetGeneration: right }, "release_target_required"],
  ] as const) {
    const writes: string[] = [];
    let gate = false;
    expect(
      await failure(runMigrations(client(db, writes), { profile: LOCAL, steps, ...opts, beforeWrite: async () => { gate = true; } })),
    ).toBe(code);
    expect(writes).toEqual([]);
    expect(gate).toBe(false);
  }
  expect(await ledger(db)).toEqual(ledgerBefore);
  expect(await marker(db)).toEqual(markerBefore);
  // The computed one is accepted.
  await runMigrations(client(db), { profile: LOCAL, steps, target: u("3"), expectedPending: [u("2"), u("3")], targetGeneration: right });
  expect(await marker(db)).toEqual([{ generation: right, status: "pass" }]);
});

test("a pending set other than the approved one refuses under the lock before any write", async () => {
  const db = await fresh();
  await runMigrations(client(db), { profile: LOCAL, steps, target: u("1"), expectedPending: [u("1")] });
  const ledgerBefore = await ledger(db);
  const markerBefore = await marker(db);
  for (const expectedPending of [[u("2")], [u("1"), u("2")], [u("2"), u("3"), u("4")], [u("3"), u("2")], [u("3")], []]) {
    const writes: string[] = [];
    let gate = false;
    expect(
      await failure(
        runMigrations(client(db, writes), {
          profile: LOCAL,
          steps,
          target: u("3"),
          expectedPending,
          beforeWrite: async () => {
            gate = true;
          },
        }),
      ),
    ).toBe("release_target_pending_changed");
    expect(writes).toEqual([]);
    expect(gate).toBe(false);
  }
  expect(await ledger(db)).toEqual(ledgerBefore);
  expect(await marker(db)).toEqual(markerBefore);
});

test("unknown, applied or missing targets refuse before any write; no target keeps the full run", async () => {
  const db = await fresh();
  await runMigrations(client(db), { profile: LOCAL, steps, target: u("2"), expectedPending: [u("1"), u("2")] });
  for (const [opts, code] of [
    [{ target: "upstream:20990101000009_t9.sql" }, "release_target_unknown"],
    [{ target: "verify:probe" }, "release_target_unknown"],
    [{ target: u("2") }, "release_target_not_pending"],
    [{ target: u("1"), expectedPending: [] }, "release_target_not_pending"],
    [{ expectedPending: [u("3")] }, "release_target_required"],
  ] as const) {
    const writes: string[] = [];
    expect(await failure(runMigrations(client(db, writes), { profile: LOCAL, steps, ...opts }))).toBe(code);
    expect(writes).toEqual([]);
  }
  expect(await ledger(db)).toEqual([u("1"), u("2")]);
  // Without a target the migrator is unchanged: every pending step and phase 2.
  expect(await runMigrations(client(db), { profile: LOCAL, steps })).toEqual({ applied: 2, skipped: 2, held: [] });
  expect((await marker(db))[0]!.status).toBe("pass");
});

// ---- Real tree: hold the current upstream tail on a full SELFHOSTED migrate ----------

const SELFHOSTED: MigrationProfile = { mode: "selfhosted", shim: true, bootstrap: true, verify: true, runtimeRole: "history_rt", runtimeLogin: false, functionBoundary: "global-default-revoke" };
const TARGET = "upstream:20261105047000_coordination_fact_readers.sql";
const ERASE = "upstream:20261105050000_operator_session_erase.sql";
const ERASE_FENCE = "upstream:20261105050010_operator_session_erase_fence_check.sql";
const transact = (db: PGlite): Transact => (work) => db.transaction((tx) => work(tx as never)) as never;
const realDb = async () => {
  const db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  open.push(db);
  await db.exec(`CREATE ROLE history_rt NOLOGIN NOINHERIT; ALTER DEFAULT PRIVILEGES FOR ROLE postgres REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;
    DO $$ BEGIN EXECUTE format('REVOKE TEMPORARY ON DATABASE %I FROM PUBLIC',current_database()); END $$;`);
  return db;
};
const schemaNames = async () => (await plan(SELFHOSTED)).map((s) => s.name).filter((n) => n.startsWith("upstream:") || n.startsWith("96bulls:"));
/**
 * The production shape: every step except TARGET and the tail after it (both lanes'
 * earlier steps) is ledgered. Setup applies them with the verifier off; its closing
 * full-manifest ACL check refuses (the 047000+ functions are missing, nothing held)
 * after every step committed, and the marker stays pending.
 */
async function prefixDatabase() {
  const db = await realDb();
  const names = await schemaNames();
  const tail = names.slice(names.indexOf(TARGET)).filter((n) => n.startsWith("upstream:"));
  const setup = (await plan(SELFHOSTED)).filter((s) => !tail.includes(s.name) && !s.name.startsWith("verify:"));
  expect(await failure(runMigrations(client(db), { profile: { ...SELFHOSTED, verify: false }, steps: setup }))).toBe("function_acl_drift");
  const have = new Set(await ledger(db));
  const split = migrationTargetSplit(names.map((name) => ({ name, pending: !have.has(name) })), TARGET);
  return { db, names, split, targetGeneration: await generation(split.held) };
}
const verdict = async (db: PGlite, expected: ExpectedSchema) => {
  await db.exec("SET ROLE history_rt");
  try {
    return await schemaVerdict(expected, transact(db));
  } finally {
    await db.exec("RESET ROLE");
  }
};
const parityGeneration = async (db: PGlite) =>
  (await db.query<{ g: string | null }>(
    "SELECT target_generation AS g FROM sessions.owner_buffered_parity_expected('org_t','ws_t','user_t','epoch_t',NULL,NULL,NULL)",
  )).rows[0]!.g;
const runTarget = (db: PGlite, s: { split: { expectedPending: string[] }; targetGeneration: string }, extra: object = {}) =>
  runMigrations(client(db), { profile: SELFHOSTED, target: TARGET, expectedPending: s.split.expectedPending, targetGeneration: s.targetGeneration, ...extra });

test("real tree, target 047000: the prefix generation is the full algorithm without the held steps and differs from the full one", async () => {
  const names = await schemaNames();
  const split = migrationTargetSplit(names.map((name) => ({ name, pending: true })), TARGET);
  expect(split.held).toContain(ERASE);
  expect(split.held).toContain(ERASE_FENCE);
  const all = (await plan(SELFHOSTED)).filter((s) => s.name.startsWith("upstream:") || s.name.startsWith("96bulls:"));
  // Byte for byte what a tree without the held files computes; nothing held = generation().
  expect(await generation(split.held)).toBe(await generationOfSteps(all.filter((s) => !split.held.includes(s.name))));
  expect(await generation([])).toBe(await generation());
  expect(await generationOfSteps(all)).toBe(await generation());
  expect(await generation(split.held)).not.toBe(await generation());
  expect(await generation([ERASE])).not.toBe(await generation([ERASE, ERASE_FENCE]));
  expect(await failure(generation(["upstream:20990101000000_not_in_tree.sql"]))).toBe("release_target_unknown");
});

test("real tree (1)-(3): steps held, readiness is OK for the prefix image, schema_pending for the full image, G2 parity binds the prefix generation", async () => {
  const s = await prefixDatabase();
  const full: ExpectedSchema = { ...(await expectedSchema()), dbMode: "selfhosted" };
  const prefix: ExpectedSchema = { ...full, names: full.names.filter((n) => !s.split.held.includes(n)), generation: s.targetGeneration };
  expect(s.split.held).toEqual([ERASE, ERASE_FENCE]);
  expect(s.targetGeneration).not.toBe(full.generation);
  // Before the run: fenced (TARGET pending).
  expect(await verdict(s.db, prefix)).toEqual({ ok: false, reason: "schema_pending", count: 1 });
  expect(await parityGeneration(s.db)).toBeNull();
  const result = await runTarget(s.db, s);
  expect(result.held).toEqual([ERASE, ERASE_FENCE]);
  expect(result.applied).toBe(1);
  // (1) PASS for exactly the prefix generation, after phase 2 on the prefix; the prefix
  //     image is ready.
  expect(await marker(s.db)).toEqual([{ generation: s.targetGeneration, status: "pass" }]);
  expect(await verdict(s.db, prefix)).toEqual({ ok: true });
  // (2) the full-tree image stays fenced: its held steps are not in the ledger.
  expect(await verdict(s.db, full)).toEqual({ ok: false, reason: "schema_pending", count: 2 });
  // ... and never matches the prefix PASS even if its names were present.
  expect(await verdict(s.db, { ...prefix, generation: full.generation })).toEqual({ ok: false, reason: "validation_pending" });
  // (3) 043700 G2: owner_buffered_parity_expected reads the PASS generation, i.e. the prefix
  //     one an image of the prefix tree is bound to, never the full-tree one.
  expect(await parityGeneration(s.db)).toBe(s.targetGeneration);
  // Held steps: no ledger row, no catalog object.
  expect(await ledger(s.db)).not.toContain(ERASE);
  expect(await ledger(s.db)).not.toContain(ERASE_FENCE);
  expect(await exists(s.db, "sessions.session_erasure_fences")).toBe(false);
  expect((await s.db.query<{ n: number }>("SELECT count(*)::int n FROM pg_proc WHERE proname LIKE 'ingest_operator_erase%' OR proname LIKE 'ingest_erase_fence%'")).rows[0]!.n).toBe(0);
});

test("real tree (4): the next release applies the held steps and PASS moves to the full generation", async () => {
  const s = await prefixDatabase();
  await runTarget(s.db, s);
  const full: ExpectedSchema = { ...(await expectedSchema()), dbMode: "selfhosted" };
  const prefix: ExpectedSchema = { ...full, names: full.names.filter((n) => !s.split.held.includes(n)), generation: s.targetGeneration };
  const names = await schemaNames();
  const tailSplit = migrationTargetSplit(names.map((name) => ({ name, pending: s.split.held.includes(name) })), ERASE_FENCE);
  expect(tailSplit).toEqual({ expectedPending: [ERASE, ERASE_FENCE], held: [] });
  // The prefix generation is not the full one: a stale binding refuses before any write.
  expect(await failure(runMigrations(client(s.db), { profile: SELFHOSTED, target: ERASE_FENCE, expectedPending: tailSplit.expectedPending, targetGeneration: s.targetGeneration }))).toBe("release_target_generation_changed");
  expect(await marker(s.db)).toEqual([{ generation: s.targetGeneration, status: "pass" }]);
  const result = await runMigrations(client(s.db), { profile: SELFHOSTED, target: ERASE_FENCE, expectedPending: tailSplit.expectedPending, targetGeneration: full.generation });
  expect(result).toMatchObject({ applied: 2, held: [] });
  expect(await marker(s.db)).toEqual([{ generation: full.generation, status: "pass" }]);
  expect(await verdict(s.db, full)).toEqual({ ok: true });
  expect(await parityGeneration(s.db)).toBe(full.generation);
  // The prefix image is no longer ready once the full tree passed (a generation change).
  expect(await verdict(s.db, prefix)).toEqual({ ok: false, reason: "validation_pending" });
});

test("real tree (5): a failing phase 2 on the prefix leaves the marker pending for the prefix generation and everything fenced", async () => {
  const s = await prefixDatabase();
  const full: ExpectedSchema = { ...(await expectedSchema()), dbMode: "selfhosted" };
  const prefix: ExpectedSchema = { ...full, names: full.names.filter((n) => !s.split.held.includes(n)), generation: s.targetGeneration };
  const boom = Object.assign(new Error("verifier down"), { code: "verify_failed_test" });
  expect(await failure(runTarget(s.db, s, { beforeVerify: async () => { throw boom; } }))).toBe("verify_failed_test");
  // TARGET committed (its own transaction); held steps absent; never PASS.
  expect(await ledger(s.db)).toContain(TARGET);
  expect(await ledger(s.db)).not.toContain(ERASE);
  expect(await marker(s.db)).toEqual([{ generation: s.targetGeneration, status: "pending" }]);
  expect(await verdict(s.db, prefix)).toEqual({ ok: false, reason: "validation_pending" });
  expect(await verdict(s.db, full)).toEqual({ ok: false, reason: "schema_pending", count: 2 });
  expect(await parityGeneration(s.db)).toBeNull();
});

test("real tree: a prefix the phase 2 verifiers cannot validate (later-lane steps held) fails closed, marker pending", async () => {
  const db = await realDb();
  const names = await schemaNames();
  const split = migrationTargetSplit(names.map((name) => ({ name, pending: true })), TARGET);
  // From empty, an upstream target also holds the whole 96bulls lane, whose objects
  // the verifiers require.
  expect(split.held.some((n) => n.startsWith("96bulls:"))).toBe(true);
  const targetGeneration = await generation(split.held);
  expect(await failure(runMigrations(client(db), { profile: SELFHOSTED, target: TARGET, expectedPending: split.expectedPending, targetGeneration }))).not.toBeNull();
  expect(await marker(db)).toEqual([{ generation: targetGeneration, status: "pending" }]);
  expect(await parityGeneration(db)).toBeNull();
});

test("real tree: the prefix-scoped ACL check excuses only absent functions a held step creates", async () => {
  const s = await prefixDatabase();
  await runTarget(s.db, s);
  const heldSql = await Promise.all(
    (await plan(SELFHOSTED)).filter((st) => s.split.held.includes(st.name)).map((st) => Bun.file(st.path).text()),
  );
  const c = client(s.db);
  expect(await sessionsFunctionAclDrift(c, "history_rt", heldSql)).toEqual([]);
  // Unscoped, the full manifest names every held function as missing.
  const unscoped = await sessionsFunctionAclDrift(c, "history_rt");
  expect(unscoped.length).toBeGreaterThan(0);
  expect(unscoped.every((d) => /^sessions\.(ingest_operator_erase_|ingest_erase_fence_)\w+\(.*\): missing/.test(d))).toBe(true);
  // A function a held step only replaces (it exists) is still compared exactly.
  await s.db.exec("GRANT EXECUTE ON FUNCTION sessions.ingest_commit_batch(jsonb) TO PUBLIC");
  expect((await sessionsFunctionAclDrift(c, "history_rt", heldSql)).join(";")).toContain("sessions.ingest_commit_batch(jsonb)");
  await s.db.exec("REVOKE EXECUTE ON FUNCTION sessions.ingest_commit_batch(jsonb) FROM PUBLIC");
  expect(await sessionsFunctionAclDrift(c, "history_rt", heldSql)).toEqual([]);
  // A held step's function that already exists (created early) is compared exactly too.
  await s.db.exec("CREATE FUNCTION sessions.ingest_erase_fence_index_lock() RETURNS void LANGUAGE sql AS 'SELECT'; GRANT EXECUTE ON FUNCTION sessions.ingest_erase_fence_index_lock() TO history_rt");
  expect((await sessionsFunctionAclDrift(c, "history_rt", heldSql)).join(";")).toContain("sessions.ingest_erase_fence_index_lock()");
});
