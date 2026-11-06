// Independent review of #260: GAP-5 inverted into a permanent real-PostgreSQL negative test (FAILS if the gap returns).
import { test, expect, beforeAll, afterAll } from "bun:test";
import pg from "pg";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { applySeedRepair, census } from "../frozen-control-parity.ts";
const URL_ = process.env.FROZEN_PARITY_PG_URL; const t = URL_ ? test : test.skip;
const root = join(import.meta.dir, "../../..");
const EPOCH = "3".repeat(64), BASE = "a".repeat(64), IDENT = "c".repeat(64), d = (c: string) => c.repeat(64);
const dbUrl = (n: string) => { const u = new URL(URL_!); u.pathname = "/" + n; return u.toString(); };
const clients: pg.Client[] = []; let admin: pg.Client;
const connect = async (n: string, app = "gap") => { const c = new pg.Client({ connectionString: dbUrl(n), application_name: app }); await c.connect(); clients.push(c); return c; };
beforeAll(async () => {
  if (!URL_) return;
  admin = new pg.Client({ connectionString: URL_ }); await admin.connect();
  for (const n of ["gap_neon", "gap_supa"]) { await admin.query(`DROP DATABASE IF EXISTS ${n} WITH (FORCE)`); await admin.query(`CREATE DATABASE ${n}`); }
  const neon = await connect("gap_neon");
  await neon.query(`CREATE SCHEMA sessions;
    CREATE TABLE sessions.deleted_upload_sessions(org_id text NOT NULL,workspace_id text NOT NULL,user_id text NOT NULL,origin_id text NOT NULL,source text NOT NULL,session_id text NOT NULL,PRIMARY KEY(org_id,workspace_id,user_id,origin_id,source,session_id));
    CREATE TABLE sessions.session_resets(org_id text NOT NULL,workspace_id text NOT NULL,user_id text NOT NULL,request_id text NOT NULL,deleted bigint NOT NULL,PRIMARY KEY(org_id,workspace_id,user_id,request_id));
    CREATE TABLE sessions.delivery_cutover_control_heads(org_id text NOT NULL,workspace_id text NOT NULL,user_id text NOT NULL,epoch text NOT NULL,version bigint NOT NULL,digest text NOT NULL,kind text NOT NULL,payload jsonb NOT NULL,pending boolean NOT NULL,deleted bigint,before_privacy jsonb,privacy_version bigint NOT NULL DEFAULT 0,updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),PRIMARY KEY(org_id,workspace_id,user_id));
    CREATE FUNCTION sessions.delivery_cutover_status() RETURNS jsonb LANGUAGE sql VOLATILE AS $$ SELECT jsonb_build_object('version',1,'phase','active','epoch','${EPOCH}','privacyBaselineSha256','${BASE}') $$;
    INSERT INTO sessions.deleted_upload_sessions VALUES('o','','u','or','claude','s');`);
  const mig = readFileSync(join(root, "supabase/migrations/20261105010000_frozen_source_custody.sql"), "utf8");
  const ddl = (x: string) => mig.match(new RegExp(`CREATE TABLE sessions\\.${x}\\([\\s\\S]*?\\n\\);`))![0];
  const supa = await connect("gap_supa");
  await supa.query("CREATE SCHEMA sessions;" + ["ingest_source_control_heads", "ingest_frozen_source_deletions", "ingest_frozen_source_resets", "ingest_frozen_source_completeness"].map(ddl).join("\n")
    + `CREATE TABLE sessions.ingest_lazy_policy(singleton boolean PRIMARY KEY DEFAULT true,source_identity_sha256 text,control_epoch text,privacy_baseline_sha256 text);
       INSERT INTO sessions.ingest_lazy_policy VALUES(true,'${IDENT}','${EPOCH}','${BASE}');`);
});
afterAll(async () => { if (!URL_) return; for (const c of clients.splice(0)) await c.end().catch(() => {}); for (const n of ["gap_neon", "gap_supa"]) await admin.query(`DROP DATABASE IF EXISTS ${n} WITH (FORCE)`); await admin.end(); });

t("GAP-5 closed, real PG17: a concurrent seal waits for the apply's SHARE lock; a seal committed first blocks the privacy seed", async () => {
  const neon = await connect("gap_neon"); await neon.query("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY");
  const supa = await connect("gap_supa", "p1-frozen-control-parity"), sealer = await connect("gap_supa", "not-p1-frozen-copy-name");
  const sealSql = "INSERT INTO sessions.ingest_frozen_source_completeness(source_identity_sha256,control_epoch,privacy_baseline_sha256,org_count,record_rows,manifest_sha256,neon_snapshot) VALUES($1,$2,$3,0,0,$4,'{}')";
  const c = await census(neon, supa);
  expect(c.plan).toMatchObject({ seedDeletions: 1, blocked: [] });
  let sealing: Promise<unknown> | undefined, sealedAt = 0, committedAt = 0;
  const hooked = { query: async (s: string, p?: unknown[]) => {
    if (!sealing && s.includes("INSERT INTO sessions.ingest_frozen_source_deletions"))
      sealing = sealer.query(sealSql, [IDENT, EPOCH, BASE, d("e")]).then(() => { sealedAt = performance.now(); });
    return supa.query(s, p);
  } };
  const r = await applySeedRepair(neon, hooked as any, c.plan.planSha256, {}, { onCommitted: () => { committedAt = performance.now(); } });
  await sealing;
  expect(r.seeded.deletions).toBe(1);
  expect(committedAt).toBeGreaterThan(0); expect(sealedAt).toBeGreaterThanOrEqual(committedAt); // the seal waited for the apply
  // now sealed: a fresh privacy gap cannot be seeded any more
  await supa.query("DELETE FROM sessions.ingest_frozen_source_deletions");
  const after = await census(neon, supa);
  expect(after.plan.blocked).toContain("privacy_seed_after_seal");
  expect(await (async () => { try { await applySeedRepair(neon, supa, after.plan.planSha256); return null; } catch (e) { return (e as any).code; } })()).toBe("privacy_seed_after_seal");
});
