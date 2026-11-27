/** AUTHORITY_SQL against a real Postgres engine (pglite), over the three ledger tables' released column shapes. */
import { expect, test } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { AUTHORITY_SQL } from "../authority-evidence.ts";

const ORG = "11111111-2222-4333-8444-555555555555", OTHER = "66666666-7777-4888-9999-aaaaaaaaaaaa";
const STOP = Date.parse("2026-11-26T10:00:00Z");
const ts = (ms: number) => new Date(ms).toISOString();

async function db() {
  const pg = new PGlite();
  await pg.exec(`
    CREATE SCHEMA sessions;
    CREATE TABLE sessions.ingest_batches (org_id text NOT NULL, workspace_id text NOT NULL, origin_id text NOT NULL, batch_id text NOT NULL,
      user_id text NOT NULL, state text NOT NULL CHECK (state IN ('pending', 'committed')), claimed_at timestamptz NOT NULL DEFAULT now(),
      committed_at timestamptz, PRIMARY KEY (org_id, workspace_id, origin_id, batch_id));
    CREATE TABLE sessions.ingest_record_revisions (org_id text NOT NULL, workspace_id text NOT NULL, origin_id text NOT NULL, record_id text NOT NULL,
      revision bigint NOT NULL, revision_id text NOT NULL, digest text NOT NULL, user_id text NOT NULL, source text NOT NULL, session_id text,
      updated_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY (org_id, workspace_id, origin_id, record_id));
    CREATE TABLE sessions.ingest_session_ownership (org_id text NOT NULL, workspace_id text NOT NULL, user_id text NOT NULL, origin_id text NOT NULL,
      source text NOT NULL, session_id text NOT NULL, PRIMARY KEY (org_id, workspace_id, user_id, origin_id, source, session_id));
  `);
  return pg;
}
const read = async (pg: PGlite, org = ORG, at = STOP) =>
  (await pg.query<Record<string, unknown>>(AUTHORITY_SQL, [org, String(at)])).rows;

test("empty org: one row of zeros", async () => {
  const pg = await db();
  const rows = await read(pg);
  expect(rows).toHaveLength(1);
  expect(Object.fromEntries(Object.entries(rows[0]!).map(([k, v]) => [k, Number(v)]))).toEqual({
    batches_claimed_since: 0, batches_committed_since: 0, batches_pending: 0, revisions_updated_since: 0,
    revisions_count: 0, revisions_hash: 0, ownership_count: 0, ownership_hash: 0,
  });
});

test("counts since the anchor (inclusive), the org only; hashes move on a rewrite and stay put otherwise", async () => {
  const pg = await db();
  await pg.query(`INSERT INTO sessions.ingest_batches VALUES
    ($1,'w','o','b1','u','committed',$3,$3), ($1,'w','o','b2','u','committed',$3,$4), ($1,'w','o','b3','u','pending',$4,NULL),
    ($1,'w','o','b4','u','committed',$5,$5), ($2,'w','o','b5','u','pending',$4,NULL)`,
    [ORG, OTHER, ts(STOP - 60_000), ts(STOP), ts(STOP + 1)]);
  await pg.query(`INSERT INTO sessions.ingest_record_revisions VALUES
    ($1,'w','o','r1',1,'v1','${"a".repeat(64)}','u','s','sess',$3), ($1,'w','o','r2',1,'v1','${"b".repeat(64)}','u','s',NULL,$4),
    ($2,'w','o','r3',1,'v1','${"c".repeat(64)}','u','s','sess',$4)`, [ORG, OTHER, ts(STOP - 1), ts(STOP)]);
  await pg.query(`INSERT INTO sessions.ingest_session_ownership VALUES ($1,'w','u','o','s','sess'), ($2,'w','u','o','s','sess')`, [ORG, OTHER]);
  const [r] = await read(pg);
  expect(Object.fromEntries(Object.entries(r!).map(([k, v]) => [k, typeof v]))).toMatchObject({ batches_claimed_since: "number" });
  expect(r).toMatchObject({
    batches_claimed_since: 2, // b3 (at STOP) and b4 (after)
    batches_committed_since: 2, // b2 (at STOP) and b4
    batches_pending: 1,
    revisions_updated_since: 1, // r2 at STOP; r1 one ms before
    revisions_count: 2,
    ownership_count: 1,
  });
  const h0 = (await read(pg))[0]!;
  expect(h0.revisions_hash).toBe(r!.revisions_hash);
  expect(Number(h0.revisions_hash)).toBeGreaterThanOrEqual(0);
  expect(Number(h0.revisions_hash)).toBeLessThan(2 ** 40);
  // Another org's change moves nothing; this org's rewrite moves its hash.
  await pg.query(`UPDATE sessions.ingest_record_revisions SET revision = 9 WHERE org_id = $1`, [OTHER]);
  expect((await read(pg))[0]!.revisions_hash).toBe(r!.revisions_hash);
  await pg.query(`UPDATE sessions.ingest_record_revisions SET revision = 2 WHERE org_id = $1 AND record_id = 'r1'`, [ORG]);
  expect((await read(pg))[0]!.revisions_hash).not.toBe(r!.revisions_hash);
  const own0 = (await read(pg))[0]!.ownership_hash;
  await pg.query(`UPDATE sessions.ingest_session_ownership SET session_id = 'other' WHERE org_id = $1`, [ORG]);
  const own1 = (await read(pg))[0]!;
  expect(own1.ownership_count).toBe(1);
  expect(own1.ownership_hash).not.toBe(own0);
});

test("read-only: runs inside a READ ONLY transaction", async () => {
  const pg = await db();
  await pg.exec("BEGIN TRANSACTION READ ONLY");
  expect(await read(pg)).toHaveLength(1);
  await pg.exec("ROLLBACK");
});
