/**
 * Real PostgreSQL proof for the parts PGlite cannot show: a concurrent operator session holding the
 * advisory lock, a real frozen-copy connection visible in pg_stat_activity, and the session-level
 * READ ONLY characteristic the operator sets on Neon. Skipped unless FROZEN_PARITY_PG_URL names a
 * DISPOSABLE superuser database (e.g. a local postgres:17 container); it creates and drops two databases.
 */
import { test, expect, beforeAll, afterAll } from "bun:test";
import pg from "pg";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { applySeedRepair, census } from "../frozen-control-parity.ts";

const URL_ = process.env.FROZEN_PARITY_PG_URL;
const t = URL_ ? test : test.skip;
const root = join(import.meta.dir, "../../..");
const EPOCH = "3".repeat(64), BASE = "a".repeat(64), d = (c: string) => c.repeat(64);
const dbUrl = (name: string) => { const u = new URL(URL_!); u.pathname = "/" + name; return u.toString(); };
const clients: pg.Client[] = [];
const connect = async (name: string, app = "parity-test") => { const c = new pg.Client({ connectionString: dbUrl(name), application_name: app }); await c.connect(); clients.push(c); return c; };
let admin: pg.Client;

beforeAll(async () => {
  if (!URL_) return;
  admin = new pg.Client({ connectionString: URL_ }); await admin.connect();
  for (const n of ["parity_neon", "parity_supa"]) { await admin.query(`DROP DATABASE IF EXISTS ${n} WITH (FORCE)`); await admin.query(`CREATE DATABASE ${n}`); }
  const neon = await connect("parity_neon");
  await neon.query(`CREATE SCHEMA sessions;
    CREATE TABLE sessions.deleted_upload_sessions(org_id text NOT NULL,workspace_id text NOT NULL,user_id text NOT NULL,origin_id text NOT NULL,source text NOT NULL,session_id text NOT NULL,PRIMARY KEY(org_id,workspace_id,user_id,origin_id,source,session_id));
    CREATE TABLE sessions.session_resets(org_id text NOT NULL,workspace_id text NOT NULL,user_id text NOT NULL,request_id text NOT NULL,deleted bigint NOT NULL,PRIMARY KEY(org_id,workspace_id,user_id,request_id));
    CREATE TABLE sessions.delivery_cutover_control_heads(org_id text NOT NULL,workspace_id text NOT NULL,user_id text NOT NULL,epoch text NOT NULL,version bigint NOT NULL,digest text NOT NULL,kind text NOT NULL,payload jsonb NOT NULL,pending boolean NOT NULL,deleted bigint,before_privacy jsonb,privacy_version bigint NOT NULL DEFAULT 0,updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),PRIMARY KEY(org_id,workspace_id,user_id));
    CREATE FUNCTION sessions.delivery_cutover_status() RETURNS jsonb LANGUAGE sql VOLATILE AS $$ SELECT jsonb_build_object('version',1,'phase','active','epoch','${EPOCH}','privacyBaselineSha256','${BASE}') $$;
    INSERT INTO sessions.delivery_cutover_control_heads(org_id,workspace_id,user_id,epoch,version,digest,kind,payload,pending,privacy_version) VALUES
      ('o','','seed','${EPOCH}',2,'${d("2")}','reset','{"request_id":"r","n":123456789012345678901}',false,1),
      ('o','w','stale','${EPOCH}',5,'${d("5")}','selection','{"enabled":true}',false,4);
    INSERT INTO sessions.deleted_upload_sessions VALUES('o','','seed','or','claude','s');
    INSERT INTO sessions.session_resets VALUES('o','','seed','r',9007199254740993);`);
  const mig = readFileSync(join(root, "supabase/migrations/20261105010000_frozen_source_custody.sql"), "utf8");
  const ddl = (x: string) => mig.match(new RegExp(`CREATE TABLE sessions\\.${x}\\([\\s\\S]*?\\n\\);`))![0];
  const supa = await connect("parity_supa");
  await supa.query("CREATE SCHEMA sessions;" + ["ingest_source_control_heads", "ingest_frozen_source_deletions", "ingest_frozen_source_resets", "ingest_frozen_source_completeness"].map(ddl).join("\n")
    + `CREATE TABLE sessions.ingest_lazy_policy(singleton boolean PRIMARY KEY DEFAULT true,source_identity_sha256 text,control_epoch text,privacy_baseline_sha256 text);
       INSERT INTO sessions.ingest_lazy_policy VALUES(true,'${"c".repeat(64)}','${EPOCH}','${BASE}');
       INSERT INTO sessions.ingest_source_control_heads(org_id,workspace_id,user_id,epoch,version,digest,kind,payload,pending,privacy_version) VALUES('o','w','stale','${EPOCH}',3,'${d("3")}','selection','{"enabled":false}',false,2);`);
});
afterAll(async () => {
  if (!URL_) return;
  for (const c of clients.splice(0)) await c.end().catch(() => {});
  for (const n of ["parity_neon", "parity_supa"]) await admin.query(`DROP DATABASE IF EXISTS ${n} WITH (FORCE)`);
  await admin.end();
});
const code = async (p: Promise<unknown>) => { try { await p; return null; } catch (e) { return (e as any).code; } };

t("real PG17: read-only session, concurrent operator lock, live copy session, then seed + repair to parity", async () => {
  const neon = await connect("parity_neon", "p1-frozen-control-parity-readonly");
  await neon.query("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY");
  expect(await code(neon.query("INSERT INTO sessions.session_resets VALUES('x','','x','x',1)"))).toBe("25006");
  const supaRo = await connect("parity_supa", "p1-frozen-control-parity");
  await supaRo.query("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY");
  const c = await census(neon, supaRo);
  expect(c.plan).toMatchObject({ seedDeletions: 1, seedResets: 1, seedHeads: 1, repairHeads: 1, blocked: [] });
  expect(c.heads).toMatchObject({ missingSeedable: 1, staleQualified: 1 });
  // census really is read-only: the same session cannot write
  expect(await code(supaRo.query("DELETE FROM sessions.ingest_source_control_heads"))).toBe("25006");

  const supa = await connect("parity_supa", "p1-frozen-control-parity");
  // another operator in flight holds the advisory lock
  const other = await connect("parity_supa", "p1-frozen-control-parity");
  await other.query("BEGIN"); await other.query("SELECT pg_advisory_xact_lock(hashtextextended('p1-frozen-control-parity',0))");
  expect(await code(applySeedRepair(neon, supa, c.plan.planSha256))).toBe("parity_operator_busy");
  await other.query("ROLLBACK");
  // a frozen-source-copy connection is live
  const copy = await connect("parity_supa", "p1-frozen-copy");
  expect(await code(applySeedRepair(neon, supa, c.plan.planSha256))).toBe("frozen_copy_session_active");
  await copy.end(); clients.splice(clients.indexOf(copy), 1);
  for (let i = 0; i < 50 && (await supa.query("SELECT count(*)::int n FROM pg_stat_activity WHERE application_name='p1-frozen-copy'")).rows[0].n; i++) await Bun.sleep(100);
  expect((await supa.query("SELECT count(*)::int n FROM sessions.ingest_source_control_heads")).rows[0].n).toBe(1);

  const r = await applySeedRepair(neon, supa, c.plan.planSha256);
  expect(r.seeded).toEqual({ deletions: 1, resets: 1, heads: 1 });
  expect(r.repairs).toEqual({ repaired: 1, already: 0, refused: {} });
  expect(r.after.parity).toEqual({ sealHeadParity: true, privacyParity: true, held: 0 });
  expect(r.after.heads.equal).toBe(2);
  const q = "SELECT payload::text p,version::text v,digest,pending,privacy_version::text pv FROM sessions.%s ORDER BY user_id";
  expect((await supa.query(q.replace("%s", "ingest_source_control_heads"))).rows).toEqual((await neon.query(q.replace("%s", "delivery_cutover_control_heads"))).rows);
  expect((await supa.query("SELECT deleted::text d FROM sessions.ingest_frozen_source_resets")).rows[0].d).toBe("9007199254740993");
  const again = await census(neon, supaRo);
  expect(again.plan.writes).toBe(0);
});
