/** 047030 frozen-copy progress: owner-only table shape, no runtime privileges, receipts guard trigger, rollback. */
import { afterEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { PGlite } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/index.js";
import { btree_gin } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/contrib/btree_gin.js";
import { pg_trgm } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/contrib/pg_trgm.js";
import { inventoryOf } from "../migrate-guards.ts";
import { MIGRATION_SENTINELS } from "../migration-sentinels.ts";
import { plan, runMigrations, type MigrationClient, type MigrationProfile } from "../migrate.ts";

setDefaultTimeout(300_000);
const root = join(import.meta.dir, "../../..");
const FILE = "20261105047030_frozen_copy_progress.sql";
const STEP = `upstream:${FILE}`;
const SQL = readFileSync(join(root, "supabase/migrations", FILE), "utf8");
const DOWN = readFileSync(join(root, "supabase/rollbacks/20261105047030_frozen_copy_progress.down.sql"), "utf8");
const TABLE = "sessions.__96bulls_frozen_copy_progress";
const RUNTIME_ROLES = ["anon", "authenticated", "service_role", "bulls96_session_ingest", "bulls96_session_ingest_seeder"];
const LOCAL: MigrationProfile = { mode: "local", shim: true, bootstrap: true, verify: false, runtimeRole: "postgres" };
const open: PGlite[] = [];

afterEach(async () => { await Promise.all(open.splice(0).map((db) => db.close())); });
const failure = async (run: Promise<unknown>) =>
  (await run.then(() => null, (caught: unknown) => caught)) as { code?: string; message?: string } | null;
function client(db: PGlite): MigrationClient {
  return { async query<R>(text: string, params?: unknown[]) {
    if (params?.length) return (await db.query<R>(text, params)) as { rows: R[] };
    const results = await db.exec(text);
    return { rows: (results.at(-1)?.rows ?? []) as R[] };
  } };
}
const chain = async () => (await plan(LOCAL)).filter((step) => !step.name.startsWith("verify:"));
async function upTo(include: (name: string) => boolean) {
  const db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  open.push(db);
  const steps = (await chain()).filter((step) => step.name.startsWith("upstream:") ? include(step.name) : true);
  await runMigrations(client(db), { profile: LOCAL, steps });
  return db;
}
const before = () => upTo((name) => name < STEP);
const through = () => upTo((name) => name <= STEP);
async function down(db: PGlite) {
  try { await db.exec(DOWN); } catch (caught) { await db.exec("ROLLBACK"); throw caught; }
}
const one = async <R>(db: PGlite, sql: string) => (await db.query<R>(sql)).rows[0]!;
const receiptsGuards = async (db: PGlite) => (await one<{ n: number }>(db,
  "SELECT count(*)::int n FROM pg_trigger WHERE tgrelid='sessions.ingest_frozen_source_receipts'::regclass AND tgname='ingest_privacy_authority_guard' AND NOT tgisinternal")).n;
// Catalog state the rollback must restore: sessions relations, triggers on sessions tables, and the ledger.
async function snapshot(db: PGlite) {
  const q = async (sql: string) => (await db.query<{ v: string }>(sql)).rows.map((row) => row.v);
  return {
    relations: await q("SELECT relname||':'||relkind::text v FROM pg_class WHERE relnamespace='sessions'::regnamespace ORDER BY 1"),
    triggers: await q("SELECT tgrelid::regclass::text||':'||tgname v FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid WHERE c.relnamespace='sessions'::regnamespace AND NOT tgisinternal ORDER BY 1"),
    ledger: await q("SELECT name||':'||sha256 v FROM sessions.__96bulls_migrations ORDER BY name"),
  };
}
const hex = (c: string) => c.repeat(64);
const progressRow = (overrides: Record<string, string> = {}) => {
  const v: Record<string, string> = {
    org_id: "'o'", dataset: "'records'", binding_sha256: `'${hex("a")}'`, generation: `'${hex("b")}'`,
    source_identity_sha256: `'${hex("c")}'`, control_epoch: `'${hex("d")}'`, privacy_baseline_sha256: `'${hex("e")}'`,
    operator_sha256: `'${hex("f")}'`, cursor: "'[]'", page_seq: "1", rows_copied: "0", status: "'in_progress'", ...overrides,
  };
  return `INSERT INTO ${TABLE}(${Object.keys(v).join(",")}) VALUES(${Object.values(v).join(",")})`;
};

describe("20261105047030 frozen-copy progress", () => {
  test("declares exactly the table and the receipts trigger; sentinel covers both", () => {
    expect(inventoryOf(SQL)).toEqual([
      { kind: "relation", name: "sessions.__96bulls_frozen_copy_progress" },
      { kind: "trigger", name: "ingest_privacy_authority_guard", on: "sessions.ingest_frozen_source_receipts" },
    ]);
    expect(SQL).not.toMatch(/\bGRANT\b/);
    expect(SQL).not.toMatch(/CREATE\s+(OR\s+REPLACE\s+)?FUNCTION/i);
    expect(DOWN).toContain(createHash("sha256").update(SQL, "utf8").digest("hex"));
    expect(MIGRATION_SENTINELS[STEP]?.evidence).toContain("__96bulls_frozen_copy_progress");
    expect(MIGRATION_SENTINELS[STEP]?.evidence).toContain("ingest_frozen_source_receipts");
  });

  test("table shape, constraints, RLS with no policies, and no runtime-role privilege", async () => {
    const db = await before();
    const sentinel = MIGRATION_SENTINELS[STEP]!.sql;
    expect((await one<{ applied: boolean }>(db, sentinel)).applied).toBe(false);
    await runMigrations(client(db), { profile: LOCAL, steps: (await chain()).filter((s) => !s.name.startsWith("upstream:") || s.name <= STEP) });
    expect((await one<{ applied: boolean }>(db, sentinel)).applied).toBe(true);
    const sha = createHash("sha256").update(SQL, "utf8").digest("hex");
    expect((await db.query("SELECT sha256 FROM sessions.__96bulls_migrations WHERE name=$1", [STEP])).rows).toEqual([{ sha256: sha }]);

    expect((await db.query<{ c: string; t: string; n: boolean; d: string | null }>(
      `SELECT attname c, format_type(atttypid,atttypmod) t, attnotnull n, pg_get_expr(d.adbin,d.adrelid) d
         FROM pg_attribute a LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
        WHERE attrelid='${TABLE}'::regclass AND attnum>0 AND NOT attisdropped ORDER BY attnum`)).rows).toEqual([
      ["org_id", "text"], ["dataset", "text"], ["binding_sha256", "text"], ["generation", "text"],
      ["source_identity_sha256", "text"], ["control_epoch", "text"], ["privacy_baseline_sha256", "text"],
      ["operator_sha256", "text"], ["cursor", "jsonb"], ["page_seq", "bigint"], ["rows_copied", "bigint"],
      ["status", "text"], ["updated_at", "timestamp with time zone"],
    ].map(([c, t]) => ({ c, t, n: true, d: c === "updated_at" ? "clock_timestamp()" : null })));
    const cons = (await db.query<{ k: string; def: string }>(
      `SELECT contype::text k, pg_get_constraintdef(oid) def FROM pg_constraint WHERE conrelid='${TABLE}'::regclass ORDER BY 1,2`)).rows;
    expect(cons.filter((c) => c.k === "p").map((c) => c.def)).toEqual(["PRIMARY KEY (org_id, dataset)"]);
    expect(cons.filter((c) => c.k === "c")).toHaveLength(11); // dataset, six sha256 hex, cursor, page_seq, rows_copied, status
    // PGlite (PG 18) also catalogs NOT NULL as contype n; nothing else (no FK, unique or exclusion).
    expect(cons.filter((c) => !["p", "c", "n"].includes(c.k))).toEqual([]);

    const rel = await one<{ rls: boolean; owner: string; acl: string | null }>(db,
      `SELECT relrowsecurity rls, pg_get_userbyid(relowner) owner, relacl::text acl FROM pg_class WHERE oid='${TABLE}'::regclass`);
    expect(rel.rls).toBe(true);
    expect(rel.owner).toBe((await one<{ u: string }>(db, "SELECT current_user u")).u);
    expect((await one<{ n: number }>(db, `SELECT count(*)::int n FROM pg_policy WHERE polrelid='${TABLE}'::regclass`)).n).toBe(0);
    expect((await one<{ n: number }>(db,
      `SELECT count(*)::int n FROM pg_class c, aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))) a
        WHERE c.oid='${TABLE}'::regclass AND a.grantee<>c.relowner`)).n).toBe(0);
    for (const role of RUNTIME_ROLES) {
      expect((await one<{ e: boolean }>(db, `SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='${role}') e`)).e).toBe(true);
      for (const priv of ["SELECT", "INSERT", "UPDATE", "DELETE"])
        expect([role, priv, (await one<{ p: boolean }>(db, `SELECT has_table_privilege('${role}','${TABLE}','${priv}') p`)).p]).toEqual([role, priv, false]);
    }

    // The owner writes it; the checks hold.
    await db.exec(progressRow());
    for (const bad of [{ dataset: "'x'" }, { binding_sha256: "'A'" }, { cursor: "'{}'" }, { page_seq: "0" }, { rows_copied: "-1" }, { status: "'done'" }, { dataset: "'receipts'", org_id: "NULL" }])
      expect((await failure(db.exec(progressRow({ dataset: "'receipts'", ...bad }))))?.code).toMatch(/^23/);
    expect((await failure(db.exec(progressRow())))?.code).toBe("23505");
  });

  test("receipts guard trigger exists, is dormant while fenced, and refuses a legacy write once authority is active", async () => {
    const db = await through();
    expect(await receiptsGuards(db)).toBe(1);
    const t = await one<{ def: string }>(db,
      "SELECT pg_get_triggerdef(oid) def FROM pg_trigger WHERE tgrelid='sessions.ingest_frozen_source_receipts'::regclass AND tgname='ingest_privacy_authority_guard'");
    expect(t.def).toBe("CREATE TRIGGER ingest_privacy_authority_guard BEFORE INSERT OR DELETE OR UPDATE OR TRUNCATE ON sessions.ingest_frozen_source_receipts FOR EACH STATEMENT EXECUTE FUNCTION sessions.ingest_privacy_authority_guard()");
    const insert = "INSERT INTO sessions.ingest_frozen_source_receipts VALUES('o','w','or','b',repeat('a',64),'{}')";
    // No authority row: the guard is dormant and a write passes.
    expect(await failure(db.exec(insert))).toBeNull();
    await db.exec("DELETE FROM sessions.ingest_frozen_source_receipts");
    await db.exec("INSERT INTO sessions.ingest_privacy_authority(state,control_epoch,neon_fingerprint_sha256,activated_at) VALUES('active',repeat('3',64),repeat('a',64),now())");
    await db.exec("RESET bulls96.privacy_authority_writer");
    for (const sql of [insert, "UPDATE sessions.ingest_frozen_source_receipts SET org_id=org_id", "DELETE FROM sessions.ingest_frozen_source_receipts", "TRUNCATE sessions.ingest_frozen_source_receipts"]) {
      const caught = await failure(db.exec(sql));
      expect([sql, caught?.code]).toEqual([sql, "55000"]);
      expect(caught?.message).toContain("legacy writer refused");
    }
    // The authority writer's session setting is what lets a write through.
    await db.exec("SET bulls96.privacy_authority_writer='authority'");
    expect(await failure(db.exec(insert))).toBeNull();
    await db.exec("RESET bulls96.privacy_authority_writer");
  });

  test("rollback restores the pre-state and refuses a second run", async () => {
    const pre = await snapshot(await before());
    const db = await through();
    expect(await snapshot(db)).not.toEqual(pre);
    await db.exec(progressRow());
    await down(db);
    expect(await snapshot(db)).toEqual(pre);
    expect(await receiptsGuards(db)).toBe(0);
    expect((await one<{ r: string | null }>(db, `SELECT to_regclass('${TABLE}')::text r`)).r).toBeNull();
    expect((await one<{ applied: boolean }>(db, MIGRATION_SENTINELS[STEP]!.sql)).applied).toBe(false);
    const again = await failure(down(db));
    expect(again?.code).toBe("55000");
    expect(await snapshot(db)).toEqual(pre);
    // Re-applying after the rollback is clean.
    await runMigrations(client(db), { profile: LOCAL, steps: (await chain()).filter((s) => !s.name.startsWith("upstream:") || s.name <= STEP) });
    expect(await receiptsGuards(db)).toBe(1);
  });

  test("rollback refuses while a newer migration is applied, or when its own checksum differs", async () => {
    const db = await upTo(() => true);
    const newer = (await db.query<{ name: string }>(`SELECT name FROM sessions.__96bulls_migrations WHERE name LIKE 'upstream:%' AND name COLLATE "C" > $1 ORDER BY name`, [STEP])).rows;
    expect(newer.length).toBeGreaterThan(0);
    const pre = await snapshot(db);
    expect((await failure(down(db)))?.code).toBe("55000");
    expect(await snapshot(db)).toEqual(pre);
    expect(await receiptsGuards(db)).toBe(1);

    const own = await through();
    await own.exec(`UPDATE sessions.__96bulls_migrations SET sha256=repeat('0',64) WHERE name='${STEP}'`);
    const tampered = await snapshot(own);
    expect((await failure(down(own)))?.code).toBe("55000");
    expect(await snapshot(own)).toEqual(tampered);
  });
});
