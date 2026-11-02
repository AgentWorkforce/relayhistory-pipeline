/**
 * seedOrg: Neon delivery_records (read only, keyset paged, one org) into the
 * ingest's committed-revision index as the seeder role, and then an old
 * Desktop's re-sent history takes the replay fast path.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { sqlLedger } from "../ledger-sql.ts";
import { seedOrg, type SeedSql } from "../seed.ts";

const root = join(import.meta.dir, "../../..");
const read = (path: string) => readFileSync(join(root, path), "utf8");
const open: PGlite[] = [];
afterEach(async () => {
  await Promise.all(open.splice(0).map((db) => db.close()));
});

async function supabase(): Promise<PGlite> {
  const db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  open.push(db);
  await db.exec(read("supabase/tests/supabase_shim.sql"));
  await db.exec(read("supabase/bootstrap/session_directory_search_worker.sql"));
  for (const file of readdirSync(join(root, "supabase/migrations")).sort()) await db.exec(read(`supabase/migrations/${file}`));
  await db.exec(read("supabase/bootstrap/session_ingest_role.sql").split("\n").filter((l) => !l.startsWith("\\")).join("\n"));
  return db;
}

async function neon(rows: [ws: string, origin: string, record: string, revision: number, digest: string][], org = "org-a") {
  const db = new PGlite();
  open.push(db);
  await db.exec(`CREATE SCHEMA sessions;
    CREATE TABLE sessions.delivery_records (
      org_id text NOT NULL, workspace_id text NOT NULL, origin_id text NOT NULL,
      record_id text NOT NULL, revision_id text NOT NULL, revision bigint NOT NULL,
      digest text NOT NULL, PRIMARY KEY (org_id, workspace_id, origin_id, record_id));`);
  for (const [ws, origin, record, revision, digest] of rows)
    await db.query("INSERT INTO sessions.delivery_records VALUES ($1,$2,$3,$4,$5,$6,$7)", [org, ws, origin, record, `${record}:r${revision}`, revision, digest]);
  const readOnly = async <T>(work: (sql: SeedSql) => Promise<T>) => {
    await db.exec("BEGIN READ ONLY");
    try {
      return await work({ query: (text, params) => db.query(text, params as unknown[]) as never });
    } finally {
      await db.exec("COMMIT");
    }
  };
  return { db, readOnly };
}

const as = (db: PGlite, role: string): SeedSql => ({
  async query(text, params) {
    await db.exec(`SET ROLE ${role}`);
    try {
      return (await db.query(text, params as unknown[])) as never;
    } finally {
      await db.exec("RESET ROLE");
    }
  },
});

const hex = (c: string) => c.repeat(64);

describe("seedOrg", () => {
  test("pages through one org's records and makes a re-sent history a replay_fast claim", async () => {
    const supa = await supabase();
    const rows = Array.from({ length: 25 }, (_, i) => ["ws-a", "origin-1", `r${String(i).padStart(2, "0")}`, 3, hex("a")] as [string, string, string, number, string]);
    const src = await neon([...rows, ["ws-b", "origin-2", "x", 1, hex("b")]]);
    // Another org's rows never leave Neon.
    await src.db.query("INSERT INTO sessions.delivery_records VALUES ('org-b','ws-a','origin-1','secret',  'secret:r1', 1, $1)", [hex("c")]);
    const result = await seedOrg(src, as(supa, "bulls96_session_ingest_seeder"), "org-a", { page: 10 });
    expect(result).toEqual({ pages: 3, read: 26, upserted: 26 });
    const stored = await supa.query<{ org_id: string; n: string }>("SELECT org_id, count(*)::text AS n FROM sessions.ingest_record_revisions GROUP BY org_id");
    expect(stored.rows).toEqual([{ org_id: "org-a", n: "26" }]);
    // Re-running is a no-op; it never lowers what the ingest committed since.
    await supa.query("UPDATE sessions.ingest_record_revisions SET revision = 9 WHERE record_id = 'r00'");
    expect((await seedOrg(src, as(supa, "bulls96_session_ingest_seeder"), "org-a", { page: 10 })).upserted).toBe(0);
    expect((await supa.query<{ revision: string }>("SELECT revision::text FROM sessions.ingest_record_revisions WHERE record_id = 'r00'")).rows[0]!.revision).toBe("9");

    // The old Desktop regenerates and re-sends at the same revisions, other bytes.
    const ledger = sqlLedger(as(supa, "bulls96_session_ingest").query as never);
    const claim = await ledger.claim({
      tenant: { orgId: "org-a", workspaceId: "ws-a", userId: "user-1" },
      originId: "origin-1",
      batchId: "gen-2",
      batchDigest: hex("d"),
      receiptId: `rhr_${hex("e")}`,
      sessions: [["claude", "s"]],
      records: rows.slice(1).map(([, , record]) => ({ source: "claude", sessionId: "s", recordId: record, revisionId: `${record}:again`, revision: 3, digest: hex("f") })),
    });
    expect(claim.kind).toBe("replay_fast");
  });

  test("the ingest role cannot seed", async () => {
    const supa = await supabase();
    const src = await neon([["ws-a", "origin-1", "r1", 1, hex("a")]]);
    await expect(seedOrg(src, as(supa, "bulls96_session_ingest"), "org-a")).rejects.toThrow();
  });
});
