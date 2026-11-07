/**
 * Migration 20261105045000 (the daily digest narrative cache, lane F4) on PGlite with every
 * migration: the fail-closed role check, the read RPC as bulls96_read_api under the read
 * Worker's tenant binding, the upsert RPC as bulls96_digest_narrative only, no table access
 * for either role, the erasure purge, and up/down/up.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { TENANT_SETTINGS_SQL } from "../../session-read-api/liveness.ts";
import { UPSERT_SQL, narrativeScopeKey } from "../../session-read-api/digest-narrative.ts";

const root = join(import.meta.dir, "../../..");
const read = (path: string) => readFileSync(join(root, path), "utf8");
const sql = (path: string) => read(path).split("\n").filter((l) => !l.startsWith("\\")).join("\n");
const MIGRATION = "supabase/migrations/20261105045000_digest_narrative_cache.sql";
const DOWN = "supabase/rollbacks/20261105045000_digest_narrative_cache.down.sql";
const HASH = "a".repeat(64);
const KEY = narrativeScopeKey("ws-a", null);

const open: PGlite[] = [];
afterEach(async () => {
  await Promise.all(open.splice(0).map((db) => db.close()));
});

async function fresh(withWriter = true): Promise<PGlite> {
  const db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  open.push(db);
  await db.exec(read("supabase/tests/supabase_shim.sql"));
  for (const f of ["session_directory_search_worker.sql", "session_token_verifier.sql", "session_ingest_roles.sql", "session_read_api_roles.sql"])
    await db.exec(read(`supabase/bootstrap/${f}`));
  // A cluster bootstrapped by an older copy of the file (before the writer role existed).
  if (!withWriter) await db.exec("DROP ROLE bulls96_digest_narrative");
  return db;
}

async function migrate(db: PGlite): Promise<{ code?: string } | null> {
  for (const file of readdirSync(join(root, "supabase/migrations")).sort()) {
    const path = `supabase/migrations/${file}`;
    if (path === MIGRATION) return db.exec(read(MIGRATION)).then(() => null, (e: { code?: string }) => e);
    await db.exec(read(path));
  }
  throw new Error("migration not found");
}

async function database(): Promise<PGlite> {
  const db = await fresh();
  const error = await migrate(db);
  if (error) throw error;
  await db.exec(sql("supabase/bootstrap/session_read_api_role.sql"));
  await db.exec(sql("supabase/bootstrap/digest_narrative_role.sql"));
  return db;
}

const code = async (p: Promise<unknown>) => ((await p.then(() => null, (e: unknown) => e)) as { code?: string } | null)?.code;

async function as<T>(db: PGlite, role: string, tenant: [string, string] | null, work: () => Promise<T>): Promise<T> {
  await db.exec(`SET ROLE ${role}`);
  try {
    await db.exec("BEGIN");
    try {
      if (tenant) await db.query(TENANT_SETTINGS_SQL, tenant);
      const out = await work();
      await db.exec("COMMIT");
      return out;
    } catch (e) {
      await db.exec("ROLLBACK");
      throw e;
    }
  } finally {
    await db.exec("RESET ROLE");
  }
}

const upsert = (db: PGlite, over: Partial<Record<"org" | "ws" | "key" | "text" | "hash", string>> = {}) =>
  db.query(UPSERT_SQL, [over.org ?? "org-a", over.ws ?? "ws-a", "2026-11-05", "UTC", over.key ?? KEY, over.hash ?? HASH, over.text ?? "Shipped.", "gpt-4o-mini", "2026-11-06T00:00:00Z"]);
const readFor = (db: PGlite, ws = "ws-a", key = KEY) =>
  db.query<{ stats_hash: string; narrative_text: string }>("SELECT * FROM sessions.digest_narrative_read($1, $2::date, $3, $4)", [ws, "2026-11-05", "UTC", key]);

describe("20261105045000 digest narrative cache", () => {
  test("fails closed (42704) when the writer role was not bootstrapped", async () => {
    const db = await fresh(false);
    expect((await migrate(db))?.code).toBe("42704");
  }, 120_000);

  test("writer upserts; read role reads only its bound tenant; nobody touches the table directly", async () => {
    const db = await database();
    await as(db, "bulls96_digest_narrative", null, () => upsert(db));
    await as(db, "bulls96_digest_narrative", null, () => upsert(db, { text: "Shipped twice.", hash: "b".repeat(64) }));
    const rows = await as(db, "bulls96_read_api", ["org-a", "ws-a"], () => readFor(db));
    expect(rows.rows).toEqual([expect.objectContaining({ stats_hash: "b".repeat(64), narrative_text: "Shipped twice." })]);
    // Another org bound to the same workspace id sees nothing.
    expect((await as(db, "bulls96_read_api", ["org-b", "ws-a"], () => readFor(db))).rows).toEqual([]);
    // A scope key naming another workspace, an unbound tenant, or another workspace parameter: 42501.
    expect(await code(as(db, "bulls96_read_api", ["org-a", "ws-a"], () => readFor(db, "ws-a", narrativeScopeKey("ws-b", null))))).toBe("42501");
    expect(await code(as(db, "bulls96_read_api", null, () => readFor(db)))).toBe("42501");
    expect(await code(as(db, "bulls96_read_api", ["org-a", "ws-a"], () => readFor(db, "ws-b", narrativeScopeKey("ws-b", null))))).toBe("42501");
    // Cross-role: the read role cannot write, the writer cannot read; neither reaches the table.
    expect(await code(as(db, "bulls96_read_api", ["org-a", "ws-a"], () => upsert(db)))).toBe("42501");
    expect(await code(as(db, "bulls96_digest_narrative", ["org-a", "ws-a"], () => readFor(db)))).toBe("42501");
    for (const role of ["bulls96_read_api", "bulls96_digest_narrative"])
      expect(await code(as(db, role, ["org-a", "ws-a"], () => db.query("SELECT * FROM sessions.daily_digest_narratives")))).toBe("42501");
  }, 120_000);

  test("the upsert refuses a scope key that disagrees with the tenant, and bounds the text", async () => {
    const db = await database();
    expect(await code(as(db, "bulls96_digest_narrative", null, () => upsert(db, { key: narrativeScopeKey("ws-b", null) })))).toBe("22023");
    expect(await code(as(db, "bulls96_digest_narrative", null, () => upsert(db, { key: "" })))).toBe("22023");
    expect(await code(as(db, "bulls96_digest_narrative", null, () => upsert(db, { text: "x".repeat(16_385) })))).toBe("23514");
    expect(await code(as(db, "bulls96_digest_narrative", null, () => upsert(db, { hash: "nothex" })))).toBe("23514");
  }, 120_000);

  test("the purge (upload erasure) removes the workspace's rows only, and is owner-only", async () => {
    const db = await database();
    await as(db, "bulls96_digest_narrative", null, () => upsert(db));
    await as(db, "bulls96_digest_narrative", null, () => upsert(db, { ws: "ws-b", key: narrativeScopeKey("ws-b", "p") }));
    for (const role of ["bulls96_read_api", "bulls96_digest_narrative"])
      expect(await code(as(db, role, null, () => db.query("SELECT sessions.purge_daily_digest_narratives('org-a', 'ws-a')")))).toBe("42501");
    expect(Number((await db.query<{ n: string | number }>("SELECT sessions.purge_daily_digest_narratives('org-a', 'ws-a') AS n")).rows[0]!.n)).toBe(1);
    expect(Number((await db.query<{ n: string | number }>("SELECT count(*) AS n FROM sessions.daily_digest_narratives")).rows[0]!.n)).toBe(1);
  }, 120_000);

  test("down then up again", async () => {
    const db = await database();
    await db.exec(read(DOWN));
    expect((await db.query<{ t: string | null }>("SELECT to_regclass('sessions.daily_digest_narratives')::text AS t")).rows[0]!.t).toBeNull();
    await db.exec(read(MIGRATION));
    await as(db, "bulls96_digest_narrative", null, () => upsert(db));
  }, 120_000);
});
