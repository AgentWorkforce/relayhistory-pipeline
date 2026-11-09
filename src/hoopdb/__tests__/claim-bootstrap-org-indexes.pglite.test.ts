/**
 * Migration 20261105044020 (claim-bootstrap org_id indexes) on PGlite: adopt-or-create, the refusals, the
 * migrator's adoptable-index allowance and the guarded rollback. An INVALID / not-ready index (what a cancelled
 * CREATE INDEX CONCURRENTLY leaves) is simulated by a superuser UPDATE of pg_index.indisvalid / indisready, which
 * PGlite permits; the catalog state the migration and the guard read is the same as after a real failed build.
 */
import { afterEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import { createHash } from "node:crypto";
import { mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { PGlite } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/index.js";
import { btree_gin } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/contrib/btree_gin.js";
import { pg_trgm } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/contrib/pg_trgm.js";
import { inventoryOf, sequencePreflight } from "../migrate-guards.ts";
import { ADOPTABLE_INDEXES, MIGRATION_SENTINELS } from "../migration-sentinels.ts";
import { migrationProfile, plan, runMigrations, type MigrationClient, type MigrationProfile } from "../migrate.ts";

setDefaultTimeout(300_000);
const root = join(import.meta.dir, "../../..");
const FILE = "20261105044020_claim_bootstrap_org_indexes.sql";
const STEP = `upstream:${FILE}`;
const MIGRATION = join(root, "supabase/migrations", FILE);
const DOWN = join(root, "supabase/rollbacks/20261105044020_claim_bootstrap_org_indexes.down.sql");
const SQL = readFileSync(MIGRATION, "utf8");
const IDX = ADOPTABLE_INDEXES[STEP]!;
const LOCAL: MigrationProfile = { mode: "local", shim: true, bootstrap: true, verify: true, runtimeRole: "postgres" };

/** The packet's eight definitions, in its build order (1..8). */
const PACKET: Array<[string, string]> = [
  ["ingest_history_mask_pages", "org_id"],
  ["ingest_history_plans", "org_id, workspace_id, user_id"],
  ["ingest_seed_captures", "org_id"],
  ["ingest_publication_attempts", "org_id, workspace_id, user_id"],
  ["ingest_publication_generations", "org_id, workspace_id, user_id"],
  ["ingest_stage_intents", "org_id, workspace_id, user_id"],
  ["ingest_custody_records", "org_id, workspace_id, user_id"],
  ["ingest_lazy_native_slots", "org_id"],
];
const def = (t: string, cols: string) => `CREATE INDEX ${t}_org_id_idx ON sessions.${t} USING btree (${cols})`;

const open: PGlite[] = [];
afterEach(async () => { await Promise.all(open.splice(0).map((db) => db.close())); });
function client(db: PGlite): MigrationClient {
  return {
    async query<R>(text: string, params?: unknown[]) {
      if (params?.length) return (await db.query<R>(text, params)) as { rows: R[] };
      const results = await db.exec(text);
      return { rows: (results.at(-1)?.rows ?? []) as R[] };
    },
  };
}
const failure = async (run: Promise<unknown>) =>
  (await run.then(() => null, (caught: unknown) => caught)) as { code?: string; message?: string } | null;

/** Every migration strictly before 044020 applied and ledgered through the real migrator. Later slots
 * must stay out entirely (not just unapplied): the migrator ledgers one contiguous prefix, so a
 * recorded later slot with 044020 missing is a ledger gap, and 044020's own down-guard refuses
 * while a later migration is applied. */
async function before044020(): Promise<PGlite> {
  const db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  open.push(db);
  const names = (await plan(LOCAL)).map((s) => s.name).filter((n) => !n.startsWith("verify:"));
  expect(names).toContain(STEP);
  const steps = (await plan(LOCAL)).filter((s) => s.name < STEP && !s.name.startsWith("verify:"));
  expect(steps.every((s) => s.name < STEP)).toBe(true);
  await runMigrations(client(db), { profile: { ...LOCAL, verify: false }, steps });
  return db;
}
/** The 044020 step plus its strict prefix: 044020 applied and latest, nothing later in the tree. */
async function through044020() {
  const db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  open.push(db);
  const steps = (await plan(LOCAL)).filter((s) => s.name <= STEP && !s.name.startsWith("verify:"));
  expect(steps.some((s) => s.name === STEP)).toBe(true);
  await runMigrations(client(db), { profile: { ...LOCAL, verify: false }, steps });
  return { db, steps };
}
const orgIndexes = async (db: PGlite) =>
  (await db.query<{ name: string; def: string; oid: number; filenode: number; valid: boolean; ready: boolean }>(
    `SELECT c.relname AS name, pg_get_indexdef(c.oid) AS def, c.oid::int AS oid, c.relfilenode::int AS filenode,
            i.indisvalid AS valid, i.indisready AS ready
       FROM pg_class c JOIN pg_index i ON i.indexrelid = c.oid
      WHERE c.relnamespace = 'sessions'::regnamespace AND c.relname LIKE '%\\_org\\_id\\_idx' ORDER BY c.relname`,
  )).rows;
const ledgered = async (db: PGlite) =>
  (await db.query<{ n: number }>("SELECT count(*)::int n FROM sessions.__96bulls_migrations WHERE name = $1", [STEP])).rows[0]!.n === 1;
const applyFile = async (db: PGlite, sql: string) => {
  await db.exec("BEGIN");
  try { await db.exec(sql); await db.exec("COMMIT"); }
  catch (error) { await db.exec("ROLLBACK"); throw error; }
};
const H = "a".repeat(64);
/** One row in sessions.ingest_lazy_native_slots: the table production still lacks the index on. */
const populateLazySlots = (db: PGlite) =>
  db.query(
    `INSERT INTO sessions.ingest_lazy_native_slots(key_sha256,key_material,org_id,workspace_id,origin_id,record_id,
       source_identity_sha256,control_epoch,initialized_witness_sha256) VALUES ($1,'{}','org-probe','ws-probe','o','r',$1,$1,$1)`,
    [H],
  );
const LAZY = "ingest_lazy_native_slots_org_id_idx";
const expectedRows = PACKET.map(([t, c]) => ({ name: `${t}_org_id_idx`, def: def(t, c) })).sort((a, b) => (a.name < b.name ? -1 : 1));

describe("20261105044020 claim-bootstrap org_id indexes", () => {
  test("the declaration, the migration text and the inventory are exactly the packet's eight, in build order", () => {
    expect(IDX.map((x) => [x.name, x.table, x.def])).toEqual(PACKET.map(([t, c]) => [`${t}_org_id_idx`, `sessions.${t}`, def(t, c)]));
    // The migration executes and compares exactly the declared definitions, in the declared order.
    const literals = [...SQL.matchAll(/'(CREATE INDEX [^']+)'/g)].map((m) => m[1]);
    expect(literals).toEqual(IDX.map((x) => x.def));
    // The migrator's inventory sees the eight literal names and nothing else.
    expect(inventoryOf(SQL).map((o) => `${o.kind}:${o.name}`).sort()).toEqual(IDX.map((x) => `index:${x.name}`).sort());
    // Outside comments and messages: no CONCURRENTLY, no IF NOT EXISTS, no DROP; the only EXECUTE runs a declared def.
    const code = SQL.replace(/--[^\n]*/g, "").replace(/(MESSAGE|HINT)=format\('[^']*'/g, "$1");
    expect(code).not.toMatch(/CONCURRENTLY|IF NOT EXISTS|\bDROP\b/i);
    // The only EXECUTEs: the emptiness probe (twice: unlocked, then under the SHARE lock), the lock, a declared def.
    expect([...code.matchAll(/\bEXECUTE\s+([^;]+);/g)].map((m) => m[1]!.trim())).toEqual([
      "format('SELECT EXISTS(SELECT 1 FROM %s LIMIT 1)',v_tbl) INTO v_rows",
      "format('LOCK TABLE %s IN SHARE MODE',v_tbl)",
      "format('SELECT EXISTS(SELECT 1 FROM %s LIMIT 1)',v_tbl) INTO v_rows",
      "v_spec[3]",
    ]);
    expect(MIGRATION_SENTINELS[STEP]).toBeDefined();
  });

  test("(a) fresh database: the migrator creates all eight with the expected definitions, valid and ready", async () => {
    const db = new PGlite({ extensions: { btree_gin, pg_trgm } });
    open.push(db);
    expect(await failure(runMigrations(client(db), { profile: LOCAL }))).toBeNull();
    expect(await ledgered(db)).toBe(true);
    const rows = await orgIndexes(db);
    expect(rows.map(({ name, def }) => ({ name, def }))).toEqual(expectedRows);
    expect(rows.every((r) => r.valid && r.ready)).toBe(true);
    const viaPgIndexes = (await db.query<{ indexname: string; indexdef: string }>(
      "SELECT indexname, indexdef FROM pg_indexes WHERE schemaname = 'sessions' AND indexname = ANY($1) ORDER BY indexname",
      [IDX.map((x) => x.name)],
    )).rows;
    expect(viaPgIndexes.map((r) => ({ name: r.indexname, def: r.indexdef }))).toEqual(expectedRows);
    // Btree, non-unique, non-partial, no expressions, no INCLUDE.
    const shape = (await db.query<{ n: number }>(
      `SELECT count(*)::int n FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid JOIN pg_am a ON a.oid = c.relam
        WHERE c.relnamespace = 'sessions'::regnamespace AND c.relname = ANY($1)
          AND a.amname = 'btree' AND NOT i.indisunique AND i.indpred IS NULL AND i.indexprs IS NULL AND i.indnatts = i.indnkeyatts`,
      [IDX.map((x) => x.name)],
    )).rows[0]!.n;
    expect(shape).toBe(8);
    // A second run is a no-op.
    expect(await failure(runMigrations(client(db), { profile: LOCAL }))).toBeNull();
  });

  test("the definitions do not depend on search_path (sessions visible or not)", async () => {
    const db = await before044020();
    await db.exec("SET search_path = sessions, public");
    await applyFile(db, SQL);
    expect((await orgIndexes(db)).map(({ name, def }) => ({ name, def }))).toEqual(expectedRows);
  });

  test("(b) pre-existing matching valid indexes (the operator's build): adopted, not rebuilt, no table lock; the migrator accepts and ledgers", async () => {
    const db = await before044020();
    // All eight built out of band, plus partial state is covered below.
    for (const x of IDX) await db.exec(x.def);
    const before = await orgIndexes(db);
    expect(before).toHaveLength(8);
    // The migration's own transaction takes no lock on any of the eight tables.
    await db.exec("BEGIN");
    await db.exec(SQL);
    const locks = (await db.query<{ n: number }>(
      "SELECT count(*)::int n FROM pg_locks WHERE locktype = 'relation' AND relation = ANY($1::regclass[]::oid[])",
      [IDX.map((x) => x.table)],
    )).rows[0]!.n;
    await db.exec("ROLLBACK");
    expect(locks).toBe(0);
    // Through the real migrator: preflight accepts the adoptable indexes, the step is ledgered, nothing rebuilt.
    expect(await failure(runMigrations(client(db), { profile: LOCAL }))).toBeNull();
    expect(await ledgered(db)).toBe(true);
    expect(await orgIndexes(db)).toEqual(before);
  });

  test("(b) positive control: the create path does lock the table, so the lock probe can see one", async () => {
    const db = await before044020();
    await db.exec("BEGIN");
    await db.exec(SQL);
    const locks = (await db.query<{ n: number }>(
      "SELECT count(*)::int n FROM pg_locks WHERE locktype = 'relation' AND mode = 'ShareLock' AND relation = ANY($1::regclass[]::oid[])",
      [IDX.map((x) => x.table)],
    )).rows[0]!.n;
    await db.exec("ROLLBACK");
    expect(locks).toBe(8);
  });

  test("(b) partial out-of-band build: the present ones are adopted untouched, the missing ones are created", async () => {
    const db = await before044020();
    const built = [IDX[0]!, IDX[5]!, IDX[7]!];
    for (const x of built) await db.exec(x.def);
    const before = await orgIndexes(db);
    expect(await failure(runMigrations(client(db), { profile: LOCAL }))).toBeNull();
    const after = await orgIndexes(db);
    expect(after.map(({ name, def }) => ({ name, def }))).toEqual(expectedRows);
    for (const b of before) expect(after.find((a) => a.name === b.name)).toEqual(b);
  });

  describe("absent index: never a silent blocking build on a populated table", () => {
    test("absent + empty table (fresh, kind, CI): created, no opt-in needed", async () => {
      const db = await before044020();
      expect((await db.query<{ n: number }>("SELECT count(*)::int n FROM sessions.ingest_lazy_native_slots")).rows[0]!.n).toBe(0);
      expect(await failure(runMigrations(client(db), { profile: LOCAL }))).toBeNull();
      expect((await orgIndexes(db)).map(({ name, def }) => ({ name, def }))).toEqual(expectedRows);
    });

    test("production shape (7 prebuilt valid, lazy_native_slots absent, populated), no opt-in: refused naming index, table and packet; nothing changes", async () => {
      const db = await before044020();
      for (const x of IDX) if (x.name !== LAZY) await db.exec(x.def);
      await populateLazySlots(db);
      const before = await orgIndexes(db);
      expect(before).toHaveLength(7);
      const error = (await failure(runMigrations(client(db), { profile: LOCAL }))) as { code?: string; message?: string; hint?: string } | null;
      expect(error?.code).toBe("55000");
      expect(error?.message).toContain(`index sessions.${LAZY} is absent and table sessions.ingest_lazy_native_slots is not empty`);
      expect(error?.message).toContain("refusing a blocking CREATE INDEX on a populated table");
      expect(error?.hint).toContain("prebuild it out of band, CONCURRENTLY, with the claim-bootstrap index packet (docs/packets/claim-bootstrap-org-index-packet-20261108.md, PR #407)");
      expect(error?.hint).toContain("HOOPDB_ALLOW_BLOCKING_INDEX_BUILD=true");
      expect(await orgIndexes(db)).toEqual(before);
      expect(await ledgered(db)).toBe(false);
      // The opt-in GUC set to anything but exactly 'on' is not an opt-in.
      await db.exec("BEGIN");
      await db.exec("SELECT set_config('hoopdb.allow_blocking_index_build', 'true', true)");
      const raised = await failure(db.exec(SQL));
      await db.exec("ROLLBACK");
      expect(raised?.code).toBe("55000");
    });

    test("absent + populated + opt-in (HOOPDB_ALLOW_BLOCKING_INDEX_BUILD): created; the seven prebuilt ones are adopted untouched", async () => {
      const db = await before044020();
      for (const x of IDX) if (x.name !== LAZY) await db.exec(x.def);
      await populateLazySlots(db);
      const before = await orgIndexes(db);
      expect(await failure(runMigrations(client(db), { profile: { ...LOCAL, allowBlockingIndexBuild: true } }))).toBeNull();
      expect(await ledgered(db)).toBe(true);
      const after = await orgIndexes(db);
      expect(after.map(({ name, def }) => ({ name, def }))).toEqual(expectedRows);
      for (const b of before) expect(after.find((a) => a.name === b.name)).toEqual(b);
      expect(after.find((a) => a.name === LAZY)).toMatchObject({ valid: true, ready: true });
      // The opt-in is per migration transaction: it is not left set on the session.
      expect((await db.query<{ v: string | null }>("SELECT current_setting('hoopdb.allow_blocking_index_build', true) v")).rows[0]!.v ?? "").not.toBe("on");
    });

    test("the populated probe cannot be fooled by row-level security hiding rows", async () => {
      const db = await before044020();
      await populateLazySlots(db);
      await db.exec(`ALTER TABLE sessions.ingest_lazy_native_slots ENABLE ROW LEVEL SECURITY;
        ALTER TABLE sessions.ingest_lazy_native_slots FORCE ROW LEVEL SECURITY`);
      const raised = await failure(applyFile(db, SQL));
      // Superuser bypasses RLS: the row is seen and the build refused (a non-bypass owner would get 42501 instead).
      expect(raised?.code).toBe("55000");
      expect(raised?.message).toContain(LAZY);
    });

    test("HOOPDB_ALLOW_BLOCKING_INDEX_BUILD: strict true/false, default off", () => {
      const env = { HOOPDB_DB_MODE: "local", HOOPDB_SUPABASE_SHIM: "true", HOOPDB_RUNTIME_ROLE: "hoop" };
      expect(migrationProfile(env).allowBlockingIndexBuild).toBeUndefined();
      expect(migrationProfile({ ...env, HOOPDB_ALLOW_BLOCKING_INDEX_BUILD: "false" }).allowBlockingIndexBuild).toBeUndefined();
      expect(migrationProfile({ ...env, HOOPDB_ALLOW_BLOCKING_INDEX_BUILD: "true" }).allowBlockingIndexBuild).toBe(true);
      expect(() => migrationProfile({ ...env, HOOPDB_ALLOW_BLOCKING_INDEX_BUILD: "yes" })).toThrow(/HOOPDB_ALLOW_BLOCKING_INDEX_BUILD must be/);
    });
  });

  const MISMATCH: Array<[string, string]> = [
    ["a different column", "CREATE INDEX ingest_seed_captures_org_id_idx ON sessions.ingest_seed_captures (id)"],
    ["a different column order", "CREATE INDEX ingest_custody_records_org_id_idx ON sessions.ingest_custody_records (org_id, user_id, workspace_id)"],
    ["a narrower definition", "CREATE INDEX ingest_stage_intents_org_id_idx ON sessions.ingest_stage_intents (org_id)"],
    ["UNIQUE", "CREATE UNIQUE INDEX ingest_history_mask_pages_org_id_idx ON sessions.ingest_history_mask_pages (org_id, sha256)"],
    ["a partial predicate", "CREATE INDEX ingest_lazy_native_slots_org_id_idx ON sessions.ingest_lazy_native_slots (org_id) WHERE org_id IS NOT NULL"],
    ["INCLUDE", "CREATE INDEX ingest_seed_captures_org_id_idx ON sessions.ingest_seed_captures (org_id) INCLUDE (id)"],
    ["DESC", "CREATE INDEX ingest_seed_captures_org_id_idx ON sessions.ingest_seed_captures (org_id DESC)"],
    ["the wrong table", "CREATE INDEX ingest_seed_captures_org_id_idx ON sessions.ingest_history_mask_pages (org_id)"],
  ];
  for (const [what, ddl] of MISMATCH)
    test(`(c) a same-name index with ${what}: the migrator refuses before any write, and the migration itself raises`, async () => {
      const db = await before044020();
      await db.exec(ddl);
      const name = /INDEX (\w+)/.exec(ddl)![1]!;
      const before = await orgIndexes(db);
      const error = await failure(runMigrations(client(db), { profile: LOCAL }));
      expect(error?.code).toBe("unledgered_schema");
      expect(error?.message).toContain(`index ${name} (${STEP})`);
      expect(await ledgered(db)).toBe(false);
      // Bypassing the guard: the migration raises, names the index and the fix, and changes nothing.
      const raised = await failure(applyFile(db, SQL));
      expect(raised?.code).toBe("42P07");
      expect(raised?.message).toContain(`index sessions.${name} exists with a different definition; reconcile it`);
      expect(await orgIndexes(db)).toEqual(before);
    });

  test("(c) a non-index relation of a declared name: the migration raises", async () => {
    const db = await before044020();
    await db.exec("CREATE TABLE sessions.ingest_seed_captures_org_id_idx (x int)");
    const raised = await failure(applyFile(db, SQL));
    expect(raised?.code).toBe("42P07");
    expect(raised?.message).toContain("sessions.ingest_seed_captures_org_id_idx exists but is not a plain index");
  });

  for (const flag of ["indisvalid", "indisready"] as const)
    test(`(d) a pre-existing exact index with ${flag}=false (simulated by a superuser catalog update): refused and raised, never dropped`, async () => {
      const db = await before044020();
      for (const x of IDX) await db.exec(x.def);
      await db.exec(`UPDATE pg_index SET ${flag} = false WHERE indexrelid = 'sessions.ingest_custody_records_org_id_idx'::regclass`);
      const before = await orgIndexes(db);
      const error = await failure(runMigrations(client(db), { profile: LOCAL }));
      expect(error?.code).toBe("unledgered_schema");
      expect(error?.message).toContain(`index ingest_custody_records_org_id_idx (${STEP})`);
      // The all-eight sentinel is false too, but it is not the reason: the one invalid index is.
      expect(error?.message).not.toContain("index ingest_seed_captures_org_id_idx");
      const raised = await failure(applyFile(db, SQL));
      expect(raised?.code).toBe("55000");
      expect(raised?.message).toContain("index sessions.ingest_custody_records_org_id_idx is INVALID or not ready");
      expect(raised?.message).toContain("DROP INDEX CONCURRENTLY sessions.ingest_custody_records_org_id_idx");
      expect(await orgIndexes(db)).toEqual(before);
      expect(await ledgered(db)).toBe(false);
    });

  describe("(e) the preflight allowance is exactly the declared names, tables and definitions", () => {
    const tmp = mkdtempSync(join(tmpdir(), "idx-044020-"));
    const step = (name: string, sql: string) => {
      const path = join(tmp, `${createHash("sha256").update(name + sql).digest("hex").slice(0, 12)}.sql`);
      writeFileSync(path, sql);
      return { name, path, sentinel: { evidence: "probe", sql: "SELECT false AS applied" } };
    };
    const preflight = (db: PGlite, steps: ReturnType<typeof step>[]) => failure(sequencePreflight(client(db), steps, new Map()));

    test("an unledgered declared index, exact, valid and ready: accepted (with the real sentinel reporting applied)", async () => {
      const db = await before044020();
      for (const x of IDX) await db.exec(x.def);
      expect((await db.query<{ applied: boolean }>(MIGRATION_SENTINELS[STEP]!.sql)).rows[0]!.applied).toBe(true);
      expect(await preflight(db, [{ name: STEP, path: MIGRATION } as never])).toBeNull();
    });

    test("an unledgered index that is not declared is still refused, even on the same table with the same definition shape", async () => {
      const db = await before044020();
      await db.exec("CREATE INDEX probe_unlisted_org_idx ON sessions.ingest_seed_captures (org_id)");
      const error = await preflight(db, [step("upstream:20261105099000_probe.sql", "CREATE INDEX probe_unlisted_org_idx ON sessions.ingest_seed_captures (org_id);")]);
      expect(error?.code).toBe("unledgered_schema");
      expect(error?.message).toContain("index probe_unlisted_org_idx");
    });

    test("a declared name inventoried by any other step is not adoptable there", async () => {
      const db = await before044020();
      await db.exec(IDX[2]!.def);
      const error = await preflight(db, [step("upstream:20261105099000_probe.sql", `${IDX[2]!.def};`)]);
      expect(error?.code).toBe("unledgered_schema");
      expect(error?.message).toContain(`index ${IDX[2]!.name}`);
    });

    test("the allowance switches off when the adoptable step also creates anything else", async () => {
      const db = await before044020();
      for (const x of IDX) await db.exec(x.def);
      const error = await preflight(db, [{ ...step(STEP, `${SQL}\nCREATE TABLE sessions.probe_extra(x int);`), sentinel: undefined } as never]);
      expect(error?.code).toBe("unledgered_schema");
      expect(error?.message).toContain(`index ${IDX[0]!.name} (${STEP})`);
    });

    test("a second index of a declared name in another schema: refused", async () => {
      const db = await before044020();
      for (const x of IDX) await db.exec(x.def);
      await db.exec("CREATE TABLE public.probe_t (org_id text); CREATE INDEX ingest_seed_captures_org_id_idx ON public.probe_t (org_id)");
      const error = await preflight(db, [{ name: STEP, path: MIGRATION } as never]);
      expect(error?.code).toBe("unledgered_schema");
      expect(error?.message).toContain(`index ingest_seed_captures_org_id_idx (${STEP})`);
      // The sentinel (all eight exact in sessions) is true, but not every declared index is adoptable: refused too.
      expect(error?.message).toContain(`sentinel ${MIGRATION_SENTINELS[STEP]!.evidence} (${STEP})`);
    });
  });

  test("(f) the rollback drops exactly the eight indexes and the ledger row, nothing else; it refuses without its ledger row", async () => {
    // 044020 applied and latest (nothing later): the down-guard refuses while a later migration is applied.
    const { db, steps } = await through044020();
    const rerun = () => runMigrations(client(db), { profile: LOCAL, steps });
    expect(await failure(rerun())).toBeNull();
    const catalog = async () =>
      (await db.query<{ k: string }>(
        `SELECT c.relkind::text || ':' || c.relname AS k FROM pg_class c WHERE c.relnamespace = 'sessions'::regnamespace
         UNION ALL SELECT 'fn:' || p.oid::regprocedure::text FROM pg_proc p WHERE p.pronamespace = 'sessions'::regnamespace ORDER BY 1`,
      )).rows.map((r) => r.k);
    const ledger = async () => (await db.query<{ name: string }>("SELECT name FROM sessions.__96bulls_migrations ORDER BY name")).rows.map((r) => r.name);
    const before = await catalog(), ledgerBefore = await ledger();
    const down = readFileSync(DOWN, "utf8");
    // A later upstream migration applied: refused, nothing changes.
    await db.query("INSERT INTO sessions.__96bulls_migrations (name, sha256) VALUES ('upstream:20261105099000_later.sql', 'x')");
    expect((await failure(db.exec(down).catch(async (e) => { await db.exec("ROLLBACK"); throw e; })))?.code).toBe("55000");
    await db.query("DELETE FROM sessions.__96bulls_migrations WHERE name = 'upstream:20261105099000_later.sql'");
    expect(await catalog()).toEqual(before);
    await db.exec(down);
    const after = await catalog();
    expect(before.filter((k) => !after.includes(k)).sort()).toEqual(IDX.map((x) => `i:${x.name}`).sort());
    expect(after.filter((k) => !before.includes(k))).toEqual([]);
    expect(await ledger()).toEqual(ledgerBefore.filter((n) => n !== STEP));
    // Again: its ledger row is gone, so it refuses and changes nothing.
    expect((await failure(db.exec(down).catch(async (e) => { await db.exec("ROLLBACK"); throw e; })))?.code).toBe("55000");
    expect(await catalog()).toEqual(after);
    // The next migrator run recreates them.
    expect(await failure(rerun())).toBeNull();
    expect(await catalog()).toEqual(before);
  });

  test("(g) sequence: 044020 sits after 044000; later slots are exactly the known stacked lanes", async () => {
    const names = (await plan(LOCAL)).map((s) => s.name).filter((n) => n.startsWith("upstream:"));
    const i = names.indexOf(STEP);
    expect(i).toBeGreaterThan(0);
    expect(names[i - 1]! < STEP).toBe(true);
    expect(names.indexOf("upstream:20261105044000_read_api_tokens.sql")).toBeLessThan(i);
    // Later slots known in this tree (045000 digest slots between on merge; keep slot order here).
    expect(names.filter((n) => n > STEP)).toEqual(["upstream:20261105046000_analysis_jobs.sql"]);
  });
});
