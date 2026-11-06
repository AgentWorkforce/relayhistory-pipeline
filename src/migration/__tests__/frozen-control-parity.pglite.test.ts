import { test, expect, afterEach } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { applySeedRepair, census, COPY_ACTIVE_SQL } from "../frozen-control-parity.ts";
import type { Queryable } from "../../hoopdb/frozen-control-head-repair.ts";

const root = join(import.meta.dir, "../../..");
const open: PGlite[] = [];
afterEach(async () => { for (const d of open.splice(0)) await d.close(); });
const EPOCH = "3".repeat(64), OTHER = "4".repeat(64), BASE = "a".repeat(64), IDENT = "c".repeat(64);
const d = (c: string) => c.repeat(64);
// Identifiers that must never appear in output; one has a JSON-hostile character and one is empty (workspace).
const ORG = "org-SECRET-7f3a", USER = "user-SECRET-91bc", WS = "";

async function fixtures(phase = "active") {
  const neon = new PGlite(), supa = new PGlite(); open.push(neon, supa);
  // Neon: relayhistory DDL (0038/0039/0078) for the three privacy/control tables plus the status function.
  await neon.exec(`CREATE SCHEMA sessions;
    CREATE TABLE sessions.deleted_upload_sessions(org_id text NOT NULL,workspace_id text NOT NULL,user_id text NOT NULL,origin_id text NOT NULL,source text NOT NULL,session_id text NOT NULL,
      PRIMARY KEY(org_id,workspace_id,user_id,origin_id,source,session_id));
    CREATE TABLE sessions.session_resets(org_id text NOT NULL,workspace_id text NOT NULL,user_id text NOT NULL,request_id text NOT NULL,deleted bigint NOT NULL,
      PRIMARY KEY(org_id,workspace_id,user_id,request_id));
    CREATE TABLE sessions.delivery_cutover_control_heads(org_id text NOT NULL,workspace_id text NOT NULL,user_id text NOT NULL,epoch text NOT NULL,version bigint NOT NULL CHECK(version>0),
      digest text NOT NULL CHECK(digest ~ '^[0-9a-f]{64}$'),kind text NOT NULL CHECK(kind IN ('selection','reset')),payload jsonb NOT NULL,pending boolean NOT NULL,deleted bigint,
      before_privacy jsonb,privacy_version bigint NOT NULL DEFAULT 0,updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),PRIMARY KEY(org_id,workspace_id,user_id));
    CREATE TABLE sessions.delivery_cutover_state(phase text,epoch text,privacy_baseline_sha256 text);
    INSERT INTO sessions.delivery_cutover_state VALUES('${phase}','${EPOCH}','${BASE}');
    CREATE FUNCTION sessions.delivery_cutover_status() RETURNS jsonb LANGUAGE sql AS $$
      SELECT jsonb_build_object('version',1,'phase',phase,'epoch',epoch,'privacyBaselineSha256',privacy_baseline_sha256) FROM sessions.delivery_cutover_state $$;`);
  // Supabase: the exact reviewed migration DDL for the mirror tables; policy reduced to the columns read.
  const mig = readFileSync(join(root, "supabase/migrations/20261105010000_frozen_source_custody.sql"), "utf8");
  const ddl = (t: string) => mig.match(new RegExp(`CREATE TABLE sessions\\.${t}\\([\\s\\S]*?\\n\\);`))![0];
  await supa.exec("CREATE SCHEMA sessions;" + ["ingest_source_control_heads", "ingest_frozen_source_deletions", "ingest_frozen_source_resets", "ingest_frozen_source_completeness"].map(ddl).join("\n")
    + `CREATE TABLE sessions.ingest_lazy_policy(singleton boolean PRIMARY KEY DEFAULT true,source_identity_sha256 text,control_epoch text,privacy_baseline_sha256 text);
       INSERT INTO sessions.ingest_lazy_policy VALUES(true,'${IDENT}','${EPOCH}','${BASE}');`);
  const head = (db: PGlite, t: string, user: string, v: string, dig: string, o: { pending?: boolean; epoch?: string; payload?: string } = {}) =>
    db.query(`INSERT INTO sessions.${t}(org_id,workspace_id,user_id,epoch,version,digest,kind,payload,pending,before_privacy,privacy_version)
      VALUES($1,$2,$3,$4,$5::bigint,$6,'selection',$7::jsonb,$8,NULL,$9::bigint)
      ON CONFLICT (org_id,workspace_id,user_id) DO UPDATE SET epoch=EXCLUDED.epoch,version=EXCLUDED.version,digest=EXCLUDED.digest,pending=EXCLUDED.pending,payload=EXCLUDED.payload,privacy_version=EXCLUDED.privacy_version`,
      [ORG, WS, user, o.epoch ?? EPOCH, v, dig, o.payload ?? '{"origin_id":"o","sessions":[],"enabled":false}', !!o.pending, String(Number(v) - 1)]);
  const nhead = (u: string, v: string, g: string, o?: any) => head(neon, "delivery_cutover_control_heads", u, v, g, o);
  const mhead = (u: string, v: string, g: string, o?: any) => head(supa, "ingest_source_control_heads", u, v, g, o);
  const snapshot = async () => JSON.stringify(await Promise.all(["ingest_source_control_heads", "ingest_frozen_source_deletions", "ingest_frozen_source_resets"].map(async (t) =>
    (await supa.query(`SELECT to_jsonb(x)-'updated_at' AS r FROM sessions.${t} x ORDER BY 1`)).rows)));
  return { neon, supa, nhead, mhead, snapshot };
}
const err = async (p: Promise<unknown>) => { try { await p; return null; } catch (e) { return (e as any).code ?? String(e); } };

test("parity: census is all-equal with an empty plan and no identifiers in its output", async () => {
  const f = await fixtures();
  await f.nhead(USER, "2", d("1")); await f.mhead(USER, "2", d("1"));
  await f.neon.query("INSERT INTO sessions.session_resets VALUES($1,$2,$3,'req',7)", [ORG, WS, USER]);
  await f.supa.query("INSERT INTO sessions.ingest_frozen_source_resets VALUES($1,$2,$3,'req',7)", [ORG, WS, USER]);
  const c = await census(f.neon, f.supa);
  expect(c.heads).toMatchObject({ neon: 1, mirror: 1, equal: 1, missingSeedable: 0, extra: 0 });
  expect(c.heads.neonCoreSetSha256).toBe(c.heads.mirrorCoreSetSha256);
  expect(c.resets.neonSetSha256).toBe(c.resets.mirrorSetSha256);
  expect(c.parity).toEqual({ sealHeadParity: true, privacyParity: true, held: 0 });
  expect(c.plan.writes).toBe(0);
  const out = JSON.stringify(c);
  for (const secret of [ORG, USER, "req", "origin_id"]) expect(out).not.toContain(secret);
});

test("census issues only SELECTs inside READ ONLY transactions on both sides", async () => {
  const f = await fixtures();
  await f.nhead(USER, "1", d("1"));
  const seen: string[] = [];
  const spy = (q: Queryable): Queryable => ({ query: (s: string, p?: unknown[]) => { seen.push(s); return q.query(s, p); } });
  await census(spy(f.neon), spy(f.supa));
  const begins = seen.filter((s) => s.startsWith("BEGIN"));
  expect(begins.length).toBe(2);
  expect(begins.every((s) => /READ ONLY/.test(s))).toBe(true);
  expect(seen.every((s) => /^(BEGIN|ROLLBACK|SELECT)/.test(s.trim()))).toBe(true);
});

test("seed-only: missing privacy rows and a completed missing head land exactly; rerun is a no-op", async () => {
  const f = await fixtures();
  // A payload number beyond 2^53 must survive byte-exact (it would not through JSON.parse/stringify).
  const payload = '{"n": 123456789012345678901, "origin_id": "o", "sessions": [{"source": "c", "session_id": "S\u00e9x"}]}';
  await f.nhead(USER, "5", d("5"), { payload });
  await f.neon.query("INSERT INTO sessions.deleted_upload_sessions VALUES($1,$2,$3,'o','claude','s1'),($1,$2,$3,'o','claude','s2')", [ORG, WS, USER]);
  await f.neon.query("INSERT INTO sessions.session_resets VALUES($1,$2,$3,'r1',9007199254740993)", [ORG, WS, USER]);
  const dry = await census(f.neon, f.supa);
  expect(dry.plan).toMatchObject({ seedDeletions: 2, seedResets: 1, seedHeads: 1, repairHeads: 0, writes: 4, blocked: [] });
  expect(dry.heads.missingSeedable).toBe(1);
  const r = await applySeedRepair(f.neon, f.supa, dry.plan.planSha256);
  expect(r.seeded).toEqual({ deletions: 2, resets: 1, heads: 1 });
  expect(r.after.parity).toEqual({ sealHeadParity: true, privacyParity: true, held: 0 });
  const n = (await f.neon.query<any>("SELECT payload::text p,version::text v,privacy_version::text pv FROM sessions.delivery_cutover_control_heads")).rows[0];
  const m = (await f.supa.query<any>("SELECT payload::text p,version::text v,privacy_version::text pv FROM sessions.ingest_source_control_heads")).rows[0];
  expect(m).toEqual(n);
  expect(m.p).toContain("123456789012345678901");
  expect((await f.supa.query<any>("SELECT deleted::text d FROM sessions.ingest_frozen_source_resets")).rows[0].d).toBe("9007199254740993");
  const again = await census(f.neon, f.supa);
  expect(again.plan.writes).toBe(0);
  const before = await f.snapshot();
  const r2 = await applySeedRepair(f.neon, f.supa, again.plan.planSha256);
  expect(r2.seeded).toEqual({ deletions: 0, resets: 0, heads: 0 });
  expect(await f.snapshot()).toBe(before);
});

test("qualified repair: same-epoch completed stale head is replaced; dry-run census binds it", async () => {
  const f = await fixtures();
  await f.mhead(USER, "1", d("1")); await f.nhead(USER, "3", d("3"));
  const c = await census(f.neon, f.supa);
  expect(c.heads.staleQualified).toBe(1); expect(c.plan.repairHeads).toBe(1);
  const r = await applySeedRepair(f.neon, f.supa, c.plan.planSha256);
  expect(r.repairs).toEqual({ repaired: 1, already: 0, refused: {} });
  expect(r.after.heads.equal).toBe(1);
  expect((await f.supa.query<any>("SELECT version::text v,digest FROM sessions.ingest_source_control_heads")).rows[0]).toEqual({ v: "3", digest: d("3") });
});

test("held categories are counted and never written", async () => {
  const f = await fixtures();
  const users = ["mp", "spm", "spn", "ep", "mn", "sv", "ex", "mo", "ps"];
  await f.nhead("mp", "1", d("1"), { pending: true });                                  // missingPending
  await f.nhead("mo", "1", d("1"), { epoch: OTHER });                                   // missingOtherEpoch
  await f.mhead("spm", "1", d("1"), { pending: true }); await f.nhead("spm", "2", d("2")); // staleNeonNewerPending (mirror pending)
  await f.mhead("spn", "1", d("1")); await f.nhead("spn", "2", d("2"), { pending: true }); // staleNeonNewerPending (neon pending)
  await f.mhead("ep", "1", d("1"), { epoch: OTHER }); await f.nhead("ep", "2", d("2"));   // epochMismatch
  await f.mhead("mn", "4", d("4")); await f.nhead("mn", "2", d("2"));                     // mirrorNewer
  await f.mhead("sv", "2", d("5")); await f.nhead("sv", "2", d("2"));                     // sameVersionDigestDiffers
  await f.mhead("ex", "1", d("1"));                                                      // extra
  await f.mhead("ps", "2", d("2"), { pending: true }); await f.nhead("ps", "2", d("2"));  // pendingStateDiffers
  await f.neon.query("INSERT INTO sessions.session_resets VALUES($1,$2,$3,'r',1)", [ORG, WS, USER]);
  await f.supa.query("INSERT INTO sessions.ingest_frozen_source_resets VALUES($1,$2,$3,'r',2)", [ORG, WS, USER]); // conflicting
  await f.supa.query("INSERT INTO sessions.ingest_frozen_source_deletions VALUES($1,$2,$3,'o','c','x')", [ORG, WS, USER]); // extra
  const c = await census(f.neon, f.supa);
  expect(c.heads).toMatchObject({ missingPending: 1, missingOtherEpoch: 1, staleNeonNewerPending: 2, epochMismatch: 1, mirrorNewer: 1,
    sameVersionDigestDiffers: 1, extra: 1, pendingStateDiffers: 1, staleQualified: 0, missingSeedable: 0 });
  expect(c.resets.conflicting).toBe(1); expect(c.deletions.extra).toBe(1);
  expect(c.parity.held).toBe(11); expect(c.parity.sealHeadParity).toBe(false);
  expect(c.plan.writes).toBe(0);
  const before = await f.snapshot();
  await applySeedRepair(f.neon, f.supa, c.plan.planSha256);
  expect(await f.snapshot()).toBe(before);
  const out = JSON.stringify(c);
  for (const u of users) expect(out).not.toContain(`"${u}"`);
});

test("copy-mode pending seeding only with the explicit flag, and it changes the plan hash", async () => {
  const f = await fixtures();
  await f.nhead(USER, "1", d("1"), { pending: true });
  const plain = await census(f.neon, f.supa), flagged = await census(f.neon, f.supa, { seedPendingMissing: true });
  expect(plain.plan.seedHeads).toBe(0); expect(flagged.plan).toMatchObject({ seedHeads: 1, seedHeadsPending: 1 });
  expect(plain.plan.planSha256).not.toBe(flagged.plan.planSha256);
  expect(await err(applySeedRepair(f.neon, f.supa, flagged.plan.planSha256))).toBe("plan_changed_rerun_census");
  await applySeedRepair(f.neon, f.supa, flagged.plan.planSha256, { seedPendingMissing: true });
  expect((await f.supa.query<any>("SELECT pending FROM sessions.ingest_source_control_heads")).rows[0].pending).toBe(true);
});

test("refuses without writing: drift after dry run, bad hash, policy mismatch, inactive phase, seal + privacy seed, copy session", async () => {
  const f = await fixtures();
  await f.nhead(USER, "1", d("1"));
  await f.neon.query("INSERT INTO sessions.deleted_upload_sessions VALUES($1,$2,$3,'o','claude','s1')", [ORG, WS, USER]);
  const c = await census(f.neon, f.supa), before = await f.snapshot();
  expect(await err(applySeedRepair(f.neon, f.supa, "nothex"))).toBe("expect_plan_invalid");
  await f.neon.query("INSERT INTO sessions.session_resets VALUES($1,$2,$3,'late',1)", [ORG, WS, USER]);
  expect(await err(applySeedRepair(f.neon, f.supa, c.plan.planSha256))).toBe("plan_changed_rerun_census");
  await f.neon.query("DELETE FROM sessions.session_resets");
  // copy session connected: intercept the activity probe
  const busy: Queryable = { query: (s: string, p?: unknown[]) => s === COPY_ACTIVE_SQL ? Promise.resolve({ rows: [{ n: 1 }] as any }) : f.supa.query(s, p) };
  expect(await err(applySeedRepair(f.neon, busy, c.plan.planSha256))).toBe("frozen_copy_session_active");
  await f.supa.query("UPDATE sessions.ingest_lazy_policy SET control_epoch=$1", [OTHER]);
  expect((await census(f.neon, f.supa)).plan.blocked).toEqual(["policy_mismatch"]);
  expect(await err(applySeedRepair(f.neon, f.supa, c.plan.planSha256))).toBe("policy_mismatch");
  await f.supa.query("UPDATE sessions.ingest_lazy_policy SET control_epoch=$1", [EPOCH]);
  await f.supa.query("INSERT INTO sessions.ingest_frozen_source_completeness(source_identity_sha256,control_epoch,privacy_baseline_sha256,org_count,record_rows,manifest_sha256,neon_snapshot) VALUES($1,$2,$3,0,0,$4,'{}')", [IDENT, EPOCH, BASE, d("e")]);
  const sealed = await census(f.neon, f.supa);
  expect(sealed.plan.blocked).toEqual(["privacy_seed_after_seal"]);
  expect(await err(applySeedRepair(f.neon, f.supa, sealed.plan.planSha256))).toBe("privacy_seed_after_seal");
  await f.supa.query("DELETE FROM sessions.ingest_frozen_source_completeness");
  await f.neon.query("UPDATE sessions.delivery_cutover_state SET phase='halted'");
  expect(await err(census(f.neon, f.supa))).toBe("neon_phase_not_active");
  expect(await err(applySeedRepair(f.neon, f.supa, c.plan.planSha256))).toBe("neon_phase_not_active");
  expect(await f.snapshot()).toBe(before);
});

test("a head that appears between plan and insert rolls the whole seed back", async () => {
  const f = await fixtures();
  await f.nhead(USER, "1", d("1"));
  await f.neon.query("INSERT INTO sessions.deleted_upload_sessions VALUES($1,$2,$3,'o','claude','s1')", [ORG, WS, USER]);
  const c = await census(f.neon, f.supa), before = await f.snapshot();
  const racing: Queryable = { query: async (s: string, p?: unknown[]) => {
    if (s.includes("INSERT INTO sessions.ingest_source_control_heads")) await f.mhead(USER, "1", d("9"), { pending: true });
    return f.supa.query(s, p);
  } };
  expect(await err(applySeedRepair(f.neon, racing, c.plan.planSha256))).toBe("seed_postcondition_heads");
  expect(await f.snapshot()).toBe(before);
});

test("a stale head that moves after the plan is refused by the reviewed repair, not overwritten", async () => {
  const f = await fixtures();
  await f.mhead(USER, "1", d("1")); await f.nhead(USER, "3", d("3"));
  const c = await census(f.neon, f.supa);
  // Neon's head goes pending after the dry run but before repair (a new control began on Neon).
  let flipped = false;
  const neon: Queryable = { query: async (s: string, p?: unknown[]) => {
    if (!flipped && s === "BEGIN READ ONLY") { flipped = true; await f.nhead(USER, "4", d("4"), { pending: true }); }
    return f.neon.query(s, p);
  } };
  const r = await applySeedRepair(neon, f.supa, c.plan.planSha256);
  expect(r.repairs).toEqual({ repaired: 0, already: 0, refused: { neon_head_pending: 1 } });
  expect((await f.supa.query<any>("SELECT version::text v FROM sessions.ingest_source_control_heads")).rows[0].v).toBe("1");
});

test("row bound refuses instead of reading an unbounded table", async () => {
  const f = await fixtures();
  await f.nhead("a", "1", d("1")); await f.nhead("b", "1", d("1"));
  expect(await err(census(f.neon, f.supa, { maxRows: 1 }))).toBe("row_bound_exceeded");
});

test("CLI refuses bad usage and a non-production stdin before any credential or network use", async () => {
  const run = (args: string[], stdin = "") => {
    const p = Bun.spawnSync([process.execPath, join(root, "scripts/frozen-control-parity.ts"), ...args], { stdin: new TextEncoder().encode(stdin), env: { ...process.env, SUPABASE_CLI: "/nonexistent" } });
    return { code: p.exitCode, out: p.stdout.toString() + p.stderr.toString() };
  };
  for (const a of [[], ["--apply"], ["--apply", "--expect-plan", "abc"], ["--census", "--apply"], ["--census", "--expect-plan", "a".repeat(64)], ["--census", "--execute"]])
    expect(run(a).code).toBe(64);
  const r = run(["--census"], JSON.stringify({ neonOwnerUrl: "postgresql://someone:pw@elsewhere/db" }) + "\n");
  expect(r.code).toBe(64); expect(r.out).not.toContain("pw@");
});
