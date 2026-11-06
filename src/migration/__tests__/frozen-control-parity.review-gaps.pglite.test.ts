// Independent review of #260 at 3d6edb0 (pr260-independent-review-20261106.md): each gap probe, inverted into a permanent
// negative test. Every test here FAILS if the gap it names comes back.
import { test, expect, afterEach } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { applySeedRepair, census } from "../frozen-control-parity.ts";
const err = async (p: Promise<unknown>) => { try { await p; return null; } catch (e) { return (e as any).code ?? String(e); } };
// Counts the apply's Neon snapshots: 1 = plan read, 2 = fresh read under the Supabase lock, 3 = final pre-COMMIT read.
const onNeonRead = (db: PGlite, n: number, fn: () => Promise<unknown>): Queryable => { let k = 0; return { query: async (s: string, p?: unknown[]) => {
  if (s.startsWith("BEGIN ISOLATION LEVEL REPEATABLE READ") && ++k === n) await fn(); return db.query(s, p as any) as any; } }; };
import type { Queryable } from "../../hoopdb/frozen-control-head-repair.ts";

const root = join(import.meta.dir, "../../..");
const open: PGlite[] = [];
afterEach(async () => { for (const d of open.splice(0)) await d.close(); });
const EPOCH = "3".repeat(64), BASE = "a".repeat(64), IDENT = "c".repeat(64);
const d = (c: string) => c.repeat(64);
const ORG = "org", USER = "user", WS = "";

async function fixtures() {
  const neon = new PGlite(), supa = new PGlite(); open.push(neon, supa);
  await neon.exec(`CREATE SCHEMA sessions;
    CREATE TABLE sessions.deleted_upload_sessions(org_id text NOT NULL,workspace_id text NOT NULL,user_id text NOT NULL,origin_id text NOT NULL,source text NOT NULL,session_id text NOT NULL,
      PRIMARY KEY(org_id,workspace_id,user_id,origin_id,source,session_id));
    CREATE TABLE sessions.session_resets(org_id text NOT NULL,workspace_id text NOT NULL,user_id text NOT NULL,request_id text NOT NULL,deleted bigint NOT NULL,
      PRIMARY KEY(org_id,workspace_id,user_id,request_id));
    CREATE TABLE sessions.delivery_cutover_control_heads(org_id text NOT NULL,workspace_id text NOT NULL,user_id text NOT NULL,epoch text NOT NULL,version bigint NOT NULL CHECK(version>0),
      digest text NOT NULL,kind text NOT NULL,payload jsonb NOT NULL,pending boolean NOT NULL,deleted bigint,
      before_privacy jsonb,privacy_version bigint NOT NULL DEFAULT 0,updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),PRIMARY KEY(org_id,workspace_id,user_id));
    CREATE FUNCTION sessions.delivery_cutover_status() RETURNS jsonb LANGUAGE sql AS $$
      SELECT jsonb_build_object('version',1,'phase','active','epoch','${EPOCH}','privacyBaselineSha256','${BASE}') $$;`);
  const mig = readFileSync(join(root, "supabase/migrations/20261105010000_frozen_source_custody.sql"), "utf8");
  const ddl = (t: string) => mig.match(new RegExp(`CREATE TABLE sessions\\.${t}\\([\\s\\S]*?\\n\\);`))![0];
  await supa.exec("CREATE SCHEMA sessions;" + ["ingest_source_control_heads", "ingest_frozen_source_deletions", "ingest_frozen_source_resets", "ingest_frozen_source_completeness"].map(ddl).join("\n")
    + `CREATE TABLE sessions.ingest_lazy_policy(singleton boolean PRIMARY KEY DEFAULT true,source_identity_sha256 text,control_epoch text,privacy_baseline_sha256 text);
       INSERT INTO sessions.ingest_lazy_policy VALUES(true,'${IDENT}','${EPOCH}','${BASE}');`);
  const head = (db: PGlite, t: string, v: string, dig: string, payload = '{"origin_id":"o","sessions":[],"enabled":false}', pending = false) =>
    db.query(`INSERT INTO sessions.${t}(org_id,workspace_id,user_id,epoch,version,digest,kind,payload,pending,before_privacy,privacy_version)
      VALUES($1,$2,$3,$4,$5::bigint,$6,'selection',$7::jsonb,$8,NULL,$9::bigint)
      ON CONFLICT (org_id,workspace_id,user_id) DO UPDATE SET version=EXCLUDED.version,digest=EXCLUDED.digest,pending=EXCLUDED.pending,payload=EXCLUDED.payload,privacy_version=EXCLUDED.privacy_version`,
      [ORG, WS, USER, EPOCH, v, dig, payload, pending, String(Number(v) - 1)]);
  const seal = () => supa.query("INSERT INTO sessions.ingest_frozen_source_completeness(source_identity_sha256,control_epoch,privacy_baseline_sha256,org_count,record_rows,manifest_sha256,neon_snapshot) VALUES($1,$2,$3,0,0,$4,'{}')", [IDENT, EPOCH, BASE, d("e")]);
  return { neon, supa, nhead: (v: string, g: string, p?: string, pend?: boolean) => head(neon, "delivery_cutover_control_heads", v, g, p, pend),
    mhead: (v: string, g: string, p?: string) => head(supa, "ingest_source_control_heads", v, g, p), seal };
}

test("GAP-1 closed: post-seal repair is bound to the PLANNED Neon head; a Neon change after the plan refuses, nothing written", async () => {
  for (const at of [2, 3]) {
    const f = await fixtures();
    await f.mhead("1", d("1")); await f.nhead("3", d("3"));
    await f.seal();
    const c = await census(f.neon, f.supa);
    expect(c.plan).toMatchObject({ repairHeads: 1, writes: 1, blocked: [] });
    const neon = onNeonRead(f.neon, at, async () => {
      await f.nhead("4", d("4"), '{"origin_id":"o","sessions":[{"source":"c","session_id":"s9"}],"enabled":false}');
      await f.neon.query("INSERT INTO sessions.deleted_upload_sessions VALUES($1,$2,$3,'o','c','s9')", [ORG, WS, USER]);
    });
    expect(await err(applySeedRepair(neon, f.supa, c.plan.planSha256))).toBe(at === 2 ? "neon_changed_since_plan" : "neon_changed_during_apply");
    expect((await f.supa.query<any>("SELECT version::text v FROM sessions.ingest_source_control_heads")).rows[0].v).toBe("1");
    expect((await f.supa.query<any>("SELECT count(*)::int n FROM sessions.ingest_frozen_source_deletions")).rows[0].n).toBe(0);
  }
});

test("GAP-1 control: with Neon unchanged, the post-seal repair lands on exactly the planned version", async () => {
  const f = await fixtures();
  await f.mhead("1", d("1")); await f.nhead("3", d("3"));
  await f.seal();
  const c = await census(f.neon, f.supa);
  const r = await applySeedRepair(f.neon, f.supa, c.plan.planSha256);
  expect(r.repairs.repaired).toBe(1);
  expect((await f.supa.query<any>("SELECT version::text v,digest FROM sessions.ingest_source_control_heads")).rows[0]).toEqual({ v: "3", digest: d("3") });
  expect(r.after.parity).toEqual({ sealHeadParity: true, privacyParity: true, held: 0 });
});

test("GAP-2 closed: a HELD item appearing after the census refuses the apply", async () => {
  const f = await fixtures();
  await f.neon.query("INSERT INTO sessions.deleted_upload_sessions VALUES($1,$2,$3,'o','c','s1')", [ORG, WS, USER]);
  const c = await census(f.neon, f.supa);
  expect(c.parity.held).toBe(0);
  await f.supa.query("INSERT INTO sessions.ingest_frozen_source_deletions VALUES($1,$2,$3,'o','c','old')", [ORG, WS, USER]);
  expect(await err(applySeedRepair(f.neon, f.supa, c.plan.planSha256))).toBe("held_items_present");
  expect((await f.supa.query<any>("SELECT count(*)::int n FROM sessions.ingest_frozen_source_deletions")).rows[0].n).toBe(1);
});

test("GAP-2b closed: planSha256 binds HELD counts and set hashes, not only the write set", async () => {
  const f = await fixtures();
  await f.nhead("1", d("1"));
  const a = await census(f.neon, f.supa);
  await f.supa.query("INSERT INTO sessions.ingest_frozen_source_deletions VALUES('other','','x','o','c','z')");
  const b = await census(f.neon, f.supa);
  expect(b.plan.seedHeads).toBe(a.plan.seedHeads);
  expect(b.plan.planSha256).not.toBe(a.plan.planSha256);
});

test("GAP-3 closed: Neon advancing between the plan read and the lock refuses; nothing committed", async () => {
  const f = await fixtures();
  await f.nhead("1", d("1"));
  const c = await census(f.neon, f.supa);
  const supa: Queryable = { query: async (s, p) => { if (s.startsWith("LOCK TABLE sessions.ingest_source_control_heads")) await f.nhead("2", d("2")); return f.supa.query(s, p); } };
  expect(await err(applySeedRepair(f.neon, supa, c.plan.planSha256))).toBe("neon_changed_since_plan");
  expect((await f.supa.query<any>("SELECT count(*)::int n FROM sessions.ingest_source_control_heads")).rows[0].n).toBe(0);
});

test("GAP-4 closed: the repair path is byte-exact for jsonb (numbers beyond 2^53 survive)", async () => {
  const f = await fixtures();
  const big = '{"n": 123456789012345678901, "origin_id": "o", "sessions": [], "enabled": false}';
  await f.mhead("1", d("1")); await f.nhead("3", d("3"), big);
  const c = await census(f.neon, f.supa);
  const r = await applySeedRepair(f.neon, f.supa, c.plan.planSha256);
  expect(r.repairs.repaired).toBe(1);
  expect((await f.supa.query<any>("SELECT payload::text p FROM sessions.ingest_source_control_heads")).rows[0].p).toContain("123456789012345678901");
  expect(r.after.heads.equal).toBe(1);
});

test("L-5 closed: a failure after COMMIT still reports that the writes landed", async () => {
  const f = await fixtures();
  await f.nhead("1", d("1"));
  const c = await census(f.neon, f.supa);
  let committed: unknown = null, done = false;
  const supa: Queryable = { query: async (s, p) => { if (s === "COMMIT") { const r = await f.supa.query(s, p); done = true; return r; }
    if (done && s.startsWith("BEGIN ISOLATION LEVEL")) throw Object.assign(new Error("x"), { code: "08006" }); return f.supa.query(s, p); } };
  expect(await err(applySeedRepair(f.neon, supa, c.plan.planSha256, {}, { onCommitted: (r) => { committed = r; } }))).toBe("08006");
  expect(committed).toEqual({ seeded: { deletions: 0, resets: 0, heads: 1 }, repaired: 0 });
  expect((await f.supa.query<any>("SELECT count(*)::int n FROM sessions.ingest_source_control_heads")).rows[0].n).toBe(1);
});

test("M-3 (documented): a Neon re-enable leaves an extra mirror fence that #260 HOLDS and never deletes", async () => {
  const f = await fixtures();
  await f.supa.query("INSERT INTO sessions.ingest_frozen_source_deletions VALUES($1,$2,$3,'o','c','reenabled')", [ORG, WS, USER]); // Neon DELETEd it
  const c = await census(f.neon, f.supa);
  expect(c.deletions.extra).toBe(1); expect(c.plan.blocked).toEqual(["held_items_present"]);
  expect(await err(applySeedRepair(f.neon, f.supa, c.plan.planSha256))).toBe("held_items_present");
  expect((await f.supa.query<any>("SELECT count(*)::int n FROM sessions.ingest_frozen_source_deletions")).rows[0].n).toBe(1);
});
