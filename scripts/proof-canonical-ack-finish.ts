// Real-PG negative matrix for finish_canonical_copy + the ack store.
// Run: bun scripts/proof-canonical-ack-finish.ts <pgPort>
import { Pool } from "pg";
import { readFileSync } from "node:fs";
const port = Number(process.argv[2] ?? 32770);
const db = new Pool({ host: "127.0.0.1", port, user: "postgres",
  password: "synthetic-feature-only", database: "postgres" });
await db.query("DROP SCHEMA sessions CASCADE; CREATE SCHEMA sessions");
await db.query(`CREATE TABLE sessions.canonical_feature_journal(revision bigint PRIMARY KEY,kind text,org_id text,workspace_id text,scope jsonb,schema_generation text,image_key text,source_hash text,prior_keyset_hash text,source_authority jsonb,prior_source_authority jsonb,pointer_set jsonb);
CREATE TABLE sessions.canonical_feature_page_ack(entry_revision bigint,kind text,pool text,cursor text,next_cursor text,page_sha256 text,PRIMARY KEY(entry_revision,pool,cursor));
CREATE TABLE sessions.canonical_feature_copy_state(entry_revision bigint PRIMARY KEY,kind text,image_key text,source_authority jsonb,pointer_set jsonb,profile_binding text,target_binding text DEFAULT '',dirty boolean NOT NULL DEFAULT true);`);
const sql = readFileSync(
  "docs/proposals/20261104061000_canonical_feature_journal.sql","utf8");
const rpc = sql.match(
  /CREATE OR REPLACE FUNCTION sessions\.finish_canonical_copy[\s\S]*?\$finish\$;/)?.[0];
await db.query(rpc!);
const A = "a".repeat(64);
const J = JSON.stringify({ authorityId: "u", epoch: "1", fingerprint: A });
await db.query(`INSERT INTO sessions.canonical_feature_journal(revision,kind,org_id,workspace_id,scope,schema_generation,image_key,source_authority,pointer_set) VALUES(10,'publish','o','w','{}','g','img',$1,$2)`, [J, J]);
const seed = () => db.query(
  `INSERT INTO sessions.canonical_feature_copy_state(entry_revision,kind,image_key,source_authority,pointer_set,profile_binding) VALUES(10,'publish','img',$1,$2,'pb') ON CONFLICT (entry_revision) DO UPDATE SET dirty=true,source_authority=EXCLUDED.source_authority,profile_binding=EXCLUDED.profile_binding`,
  [J, J]);
const page = (p: string, c: string, n: string | null) => db.query(
  `INSERT INTO sessions.canonical_feature_page_ack(entry_revision,kind,pool,cursor,next_cursor,page_sha256) VALUES(10,'publish',$1,$2,$3,'h')`,
  [p, c, n]);
const fin = (a = J, pb = "pb", te = "h", tb = "h") => db.query(
  "SELECT sessions.finish_canonical_copy($1,$2::jsonb,$3,$4,$5,$6) AS ok",
  [10, a, pb, te, tb, ""]).then((r) => r.rows[0].ok);
const reset = async () => {
  await db.query("DELETE FROM sessions.canonical_feature_page_ack");
  await seed();
};
const t = async (n: string, f: () => Promise<boolean>) =>
  console.log(n, await f());
await reset(); await t("zero_pages", fin);
await reset();
await page("event", "start", "c1"); await page("event", "c1", null);
await page("brief", "start", null);
await t("contiguous", fin);
await t("second_finish", fin);
await reset();
await page("event", "start", "c1"); await page("event", "c2", null);
await page("brief", "start", null);
await t("gap_dangling", fin);
await reset();
await page("event", "start", null); await page("event", "c1", null);
await page("brief", "start", null);
await t("two_terminal", fin);
await reset();
await page("event", "start", "c1"); await page("event", "c1", "start");
await page("event", "c2", null); await page("brief", "start", null);
await t("cycle", fin);
await reset();
await page("event", "start", null); await page("event", "x", null);
await page("brief", "start", null);
await t("disconnected", fin);
await reset();
await page("event", "start", null); await page("brief", "start", null);
await t("binding_drift", () =>
  fin(JSON.stringify({ authorityId: "u", epoch: "9", fingerprint: A }), "pb"));
await t("journal_drift", () =>
  db.query("DELETE FROM sessions.canonical_feature_journal").then(fin));
await db.end();
