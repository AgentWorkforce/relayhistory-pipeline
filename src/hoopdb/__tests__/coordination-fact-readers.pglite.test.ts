/** 047000 coordination fact readers: exact partial indexes, adoption, blocking-build refusal, and opt-in. */
import { afterEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { PGlite } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/index.js";
import { btree_gin } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/contrib/btree_gin.js";
import { pg_trgm } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/contrib/pg_trgm.js";
import { inventoryOf } from "../migrate-guards.ts";
import { ADOPTABLE_INDEXES, MIGRATION_SENTINELS } from "../migration-sentinels.ts";
import { plan, runMigrations, type MigrationClient, type MigrationProfile } from "../migrate.ts";

setDefaultTimeout(300_000);
const root = join(import.meta.dir, "../../..");
const FILE = "20261105047000_coordination_fact_readers.sql";
const STEP = `upstream:${FILE}`;
const SQL = readFileSync(join(root, "supabase/migrations", FILE), "utf8");
const DOWN = readFileSync(join(root, "supabase/rollbacks/20261105047000_coordination_fact_readers.down.sql"), "utf8");
const IDX = ADOPTABLE_INDEXES[STEP]!;
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
async function before047000() {
  const db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  open.push(db);
  const all = (await plan(LOCAL)).filter((step) => !step.name.startsWith("verify:"));
  const steps = all.filter((step) => step.name.startsWith("upstream:") ? step.name < STEP : true);
  await runMigrations(client(db), { profile: LOCAL, steps });
  return db;
}
const indexRows = (db: PGlite) => db.query<{ name: string; def: string; predicate: string }>(
  `SELECT c.relname name, pg_get_indexdef(c.oid) def, pg_get_expr(i.indpred,i.indrelid) predicate
     FROM pg_class c JOIN pg_index i ON i.indexrelid=c.oid
    WHERE c.relnamespace='sessions'::regnamespace AND c.relname=ANY($1) ORDER BY c.relname`,
  [IDX.map((item) => item.name)],
);
async function populate(db: PGlite) {
  await db.exec(`INSERT INTO sessions.session_event_facts
    (org_id,workspace_id,ts,session_id,source,machine_id,kind,event_id,input_tokens,output_tokens,reasoning_tokens,
     cache_read_tokens,cache_create_tokens,row_id,fingerprint,source_version)
    VALUES ('o','w',now(),'s','codex','m','tool','e',0,0,0,0,0,repeat('a',64),'f',1);
    INSERT INTO sessions.session_output_facts
    (org_id,workspace_id,fact_kind,ts,scope_source,source,session_id,fact_key,origin,row_key,row_id,fingerprint,source_version)
    VALUES ('o','w','message',now(),'codex','codex','s','k','o','r',repeat('b',64),'f',1);`);
}

describe("20261105047000 coordination fact readers", () => {
  test("migration, guard declarations, partial predicates, and function sentinel are byte-exact", async () => {
    expect(inventoryOf(SQL).map((item) => `${item.kind}:${item.name}`).sort()).toEqual([
      "function:sessions.coordination_output_facts_for", "function:sessions.coordination_write_facts_for",
      ...IDX.map((item) => `index:${item.name}`),
    ].sort());
    expect([...SQL.matchAll(/'(CREATE INDEX [^']+(?:''[^']*)*)'/g)].map((match) => match[1]!.replaceAll("''", "'")))
      .toEqual(IDX.map((item) => item.def));
    expect(MIGRATION_SENTINELS[STEP]?.evidence).toContain("coordination_write_facts_for");
    const db = await before047000();
    await runMigrations(client(db), { profile: LOCAL });
    const rows = (await indexRows(db)).rows;
    expect(rows.map(({ name, def }) => ({ name, def }))).toEqual([...IDX].sort((a, b) => a.name.localeCompare(b.name)).map(({ name, def }) => ({ name, def })));
    expect(rows.map((row) => row.predicate)).toEqual([
      "(cardinality(files_touched) > 0)",
      "(fact_kind = ANY (ARRAY['commit'::text, 'outcome'::text]))",
    ]);
  });

  test("exact prebuilt indexes are adopted and the two readers are still created", async () => {
    const db = await before047000();
    for (const item of IDX) await db.exec(item.def);
    const before = (await indexRows(db)).rows;
    expect(await failure(runMigrations(client(db), { profile: LOCAL }))).toBeNull();
    expect((await indexRows(db)).rows).toEqual(before);
    expect((await db.query<{ applied: boolean }>(MIGRATION_SENTINELS[STEP]!.sql)).rows[0]!.applied).toBe(true);
  });

  test("an absent index on a populated table is refused with the packet and exact target", async () => {
    const db = await before047000();
    await populate(db);
    const error = await failure(runMigrations(client(db), { profile: LOCAL }));
    expect(error?.code).toBe("index_prebuild_required");
    expect(error?.message).toContain("session_event_facts_coordination_writes_idx");
    expect(error?.message).toContain("docs/coordination-overlap-detection.md");
    expect(error?.message).toContain("HOOPDB_ALLOW_BLOCKING_INDEX_BUILD=true");
  });

  test("the explicit blocking-build opt-in creates both indexes on populated tables", async () => {
    const db = await before047000();
    await populate(db);
    expect(await failure(runMigrations(client(db), { profile: { ...LOCAL, allowBlockingIndexBuild: true } }))).toBeNull();
    expect((await indexRows(db)).rows).toHaveLength(2);
  });

  test("the guarded rollback pins this migration and removes only its readers and indexes", async () => {
    expect(DOWN).toContain(createHash("sha256").update(SQL).digest("hex"));
    const db = await before047000();
    await runMigrations(client(db), { profile: LOCAL });
    await db.exec(DOWN);
    expect((await indexRows(db)).rows).toEqual([]);
    expect((await db.query<{ n: number }>(
      "SELECT count(*)::int n FROM pg_proc WHERE oid IN (to_regprocedure('sessions.coordination_write_facts_for(text,timestamptz,timestamptz,text,integer)'),to_regprocedure('sessions.coordination_output_facts_for(text,timestamptz,timestamptz,text,integer)'))",
    )).rows[0]!.n).toBe(0);
  });
});
