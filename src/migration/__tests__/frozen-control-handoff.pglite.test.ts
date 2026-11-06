/**
 * Option B handoff: a control mutates Neon in EACH race window, and nothing is lost or silently diverges.
 * Supabase = the real migration stack (real ingest_source_control_begin/complete and the seal-gated policy).
 * Neon = relayhistory 0078 control semantics: ONE transaction issues the permit, inserts or bumps the head, and
 *        applies the privacy writes. RHC = the c7de216 route order: mirror begin (mirror mode only) -> Neon
 *        begin+apply -> requireMirrorMatch -> Neon complete -> mirror complete.
 */
import { test, expect, afterEach, setDefaultTimeout } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { handoffCensus, handoffSync } from "../frozen-control-handoff.ts";
import { census } from "../frozen-control-parity.ts";
setDefaultTimeout(60000);

const root = join(import.meta.dir, "../../..");
const G = "a".repeat(64), EPOCH = "3".repeat(64), IDENT = "c".repeat(64), BASE = "a".repeat(64);
const open: PGlite[] = [];
afterEach(async () => { for (const d of open.splice(0)) await d.close(); });
const T = (user: string, org = "org-1") => ({ orgId: org, workspaceId: "w1", userId: user });
const sel = (sid: string, enabled: boolean) => ({ origin_id: "o1", sessions: [{ source: "claude", session_id: sid }], enabled });

async function fixtures() {
  const supa = new PGlite({ extensions: { btree_gin, pg_trgm } }), neon = new PGlite(); open.push(supa, neon);
  const read = (p: string) => readFileSync(join(root, p), "utf8");
  await supa.exec(read("supabase/tests/supabase_shim.sql"));
  for (const f of ["session_directory_search_worker", "session_token_verifier", "session_ingest_roles"]) await supa.exec(read("supabase/bootstrap/" + f + ".sql"));
  for (const f of readdirSync(join(root, "supabase/migrations")).filter((f) => f.endsWith(".sql")).sort()) await supa.exec(read("supabase/migrations/" + f));
  await supa.exec(`CREATE TABLE sessions.__96bulls_role_validation(name text PRIMARY KEY,generation text,status text);INSERT INTO sessions.__96bulls_role_validation VALUES('session_directory_search_worker','${G}','pass')`);
  await supa.query("SELECT sessions.ingest_lazy_policy_set($1::jsonb)", [JSON.stringify({ baselineMode: "lazy-current", generation: G, sourceIdentitySha256: IDENT, controlEpoch: EPOCH,
    privacyBaselineSha256: BASE, retentionContractSha256: "b".repeat(64), sourceProofSha256: "d".repeat(64), validUntil: new Date(Date.now() + 3600000).toISOString(), issuer: "relayhistory-cloud" })]);
  await neon.exec(`CREATE SCHEMA sessions;
    CREATE TABLE sessions.deleted_upload_sessions(org_id text NOT NULL,workspace_id text NOT NULL,user_id text NOT NULL,origin_id text NOT NULL,source text NOT NULL,session_id text NOT NULL,PRIMARY KEY(org_id,workspace_id,user_id,origin_id,source,session_id));
    CREATE TABLE sessions.session_resets(org_id text NOT NULL,workspace_id text NOT NULL,user_id text NOT NULL,request_id text NOT NULL,deleted bigint NOT NULL,PRIMARY KEY(org_id,workspace_id,user_id,request_id));
    CREATE TABLE sessions.delivery_cutover_control_heads(org_id text NOT NULL,workspace_id text NOT NULL,user_id text NOT NULL,epoch text NOT NULL,version bigint NOT NULL,digest text NOT NULL,kind text NOT NULL,payload jsonb NOT NULL,pending boolean NOT NULL,deleted bigint,before_privacy jsonb,privacy_version bigint NOT NULL DEFAULT 0,updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),PRIMARY KEY(org_id,workspace_id,user_id));
    CREATE TABLE sessions.delivery_cutover_state(phase text,epoch text,baseline text);
    INSERT INTO sessions.delivery_cutover_state VALUES('active','${EPOCH}','${BASE}');
    CREATE FUNCTION sessions.delivery_cutover_status() RETURNS jsonb LANGUAGE sql AS $$ SELECT jsonb_build_object('version',1,'phase',phase,'epoch',epoch,'privacyBaselineSha256',baseline) FROM sessions.delivery_cutover_state $$;
    -- Same shape as Supabase ingest_frozen_source_privacy / Neon snapshot_delivery_cutover_privacy_v1.
    CREATE FUNCTION sessions.snap(o text,w text,u text) RETURNS jsonb LANGUAGE sql AS $$ SELECT jsonb_build_object('complete',true,
      'deletions',(SELECT coalesce(jsonb_agg(jsonb_build_object('origin_id',origin_id,'source',source,'session_id',session_id) ORDER BY origin_id COLLATE "C",source COLLATE "C",session_id COLLATE "C"),'[]') FROM sessions.deleted_upload_sessions WHERE org_id=o AND workspace_id=w AND user_id=u),
      'resets',(SELECT coalesce(jsonb_agg(jsonb_build_object('request_id',request_id,'deleted',deleted::text) ORDER BY request_id COLLATE "C"),'[]') FROM sessions.session_resets WHERE org_id=o AND workspace_id=w AND user_id=u)) $$;
    -- 0078 begin_delivery_cutover_control_v1 + set_session_uploads/reset in ONE statement/transaction.
    CREATE FUNCTION sessions.control(e text,o text,w text,u text,k text,p jsonb) RETURNS jsonb LANGUAGE plpgsql AS $$
    DECLARE h sessions.delivery_cutover_control_heads%ROWTYPE; d text; fresh boolean; s record;
    BEGIN
      SELECT * INTO s FROM sessions.delivery_cutover_state; IF s.phase<>'active' OR s.epoch<>e THEN RAISE EXCEPTION 'delivery_cutover_held'; END IF;
      d:=encode(sha256(convert_to(jsonb_build_array('delivery-cutover-control-v1',e,o,w,u,k,p)::text,'UTF8')),'hex');
      INSERT INTO sessions.delivery_cutover_control_heads(org_id,workspace_id,user_id,epoch,version,digest,kind,payload,pending) VALUES(o,w,u,e,1,d,k,p,true) ON CONFLICT DO NOTHING RETURNING true INTO fresh;
      SELECT * INTO STRICT h FROM sessions.delivery_cutover_control_heads WHERE org_id=o AND workspace_id=w AND user_id=u FOR UPDATE;
      IF coalesce(fresh,false) THEN UPDATE sessions.delivery_cutover_control_heads SET before_privacy=sessions.snap(o,w,u),privacy_version=0 WHERE org_id=o AND workspace_id=w AND user_id=u RETURNING * INTO h;
      ELSE
        IF h.pending THEN IF h.digest<>d THEN RAISE EXCEPTION 'delivery_control_pending'; END IF;
          RETURN jsonb_build_object('epoch',h.epoch,'version',h.version::text,'digest',h.digest,'kind',h.kind,'apply',false); END IF;
        UPDATE sessions.delivery_cutover_control_heads SET version=h.version+1,digest=d,kind=k,payload=p,pending=true,before_privacy=NULL,privacy_version=h.version,updated_at=clock_timestamp()
          WHERE org_id=o AND workspace_id=w AND user_id=u RETURNING * INTO h;
      END IF;
      IF k='selection' AND NOT (p->>'enabled')::boolean THEN
        INSERT INTO sessions.deleted_upload_sessions SELECT o,w,u,p->>'origin_id',x->>'source',x->>'session_id' FROM jsonb_array_elements(p->'sessions') x ON CONFLICT DO NOTHING;
      ELSIF k='selection' THEN
        DELETE FROM sessions.deleted_upload_sessions t USING jsonb_array_elements(p->'sessions') x WHERE t.org_id=o AND t.workspace_id=w AND t.user_id=u AND t.origin_id=p->>'origin_id' AND t.source=x->>'source' AND t.session_id=x->>'session_id';
      ELSE INSERT INTO sessions.session_resets VALUES(o,w,u,p->>'request_id',(SELECT count(*) FROM sessions.deleted_upload_sessions WHERE org_id=o AND workspace_id=w AND user_id=u)); END IF;
      RETURN jsonb_build_object('epoch',h.epoch,'version',h.version::text,'digest',h.digest,'kind',h.kind,'apply',true);
    END $$;
    CREATE FUNCTION sessions.complete(e text,o text,w text,u text,v bigint,d text) RETURNS boolean LANGUAGE sql AS $$
      UPDATE sessions.delivery_cutover_control_heads SET pending=false,updated_at=clock_timestamp() WHERE org_id=o AND workspace_id=w AND user_id=u AND epoch=e AND version=v AND digest=d RETURNING true $$;`);

  const sfn = async (fn: string, p: unknown) => (await supa.query<any>(`SELECT sessions.${fn}($1::jsonb) AS r`, [JSON.stringify(p)])).rows[0].r;
  /** One RHC control in the given mode. Throws "503:<why>" where the deployed route returns 503. */
  async function control(mode: "relayhistory" | "mirror", t: ReturnType<typeof T>, kind: "selection" | "reset", payload: any, o: { crashAfterNeonBegin?: boolean } = {}) {
    let head: any;
    if (mode === "mirror") { try { head = await sfn("ingest_source_control_begin", { epoch: EPOCH, tenant: t, kind, payload }); } catch (e) { throw new Error("503:mirror_begin:" + (e as Error).message); } }
    const intent = (await neon.query<any>("SELECT sessions.control($1,$2,$3,$4,$5,$6::jsonb) AS r", [EPOCH, t.orgId, t.workspaceId, t.userId, kind, JSON.stringify(payload)])).rows[0].r;
    if (head && (head.version !== intent.version || head.digest !== intent.digest)) throw new Error("503:mirror_mismatch");
    if (o.crashAfterNeonBegin) return intent;
    await neon.query("SELECT sessions.complete($1,$2,$3,$4,$5::bigint,$6)", [EPOCH, t.orgId, t.workspaceId, t.userId, intent.version, intent.digest]);
    if (head) await sfn("ingest_source_control_complete", { epoch: EPOCH, tenant: t, version: intent.version, digest: intent.digest, kind, outcome: intent.apply ? "applied" : "replay" });
    return intent;
  }
  const seal = () => supa.query("INSERT INTO sessions.ingest_frozen_source_completeness(source_identity_sha256,control_epoch,privacy_baseline_sha256,org_count,record_rows,manifest_sha256,neon_snapshot) VALUES($1,$2,$3,0,0,$4,'{}')", [IDENT, EPOCH, BASE, "e".repeat(64)]);
  const mirrorState = async () => JSON.stringify((await supa.query("SELECT to_jsonb(h)-'updated_at' r FROM sessions.ingest_source_control_heads h ORDER BY 1")).rows)
    + JSON.stringify((await supa.query("SELECT to_jsonb(d) r FROM sessions.ingest_frozen_source_deletions d ORDER BY 1")).rows);
  const neonState = async () => JSON.stringify((await neon.query("SELECT to_jsonb(h)-'updated_at' r FROM sessions.delivery_cutover_control_heads h ORDER BY 1")).rows);
  // Frozen state at freeze time, already seeded into the mirror (the lead's 02:30Z census: deletions 8/8, heads 0/0).
  await neon.query("INSERT INTO sessions.deleted_upload_sessions VALUES('org-1','w1','frozen','o1','claude','old')");
  await supa.query("INSERT INTO sessions.ingest_frozen_source_deletions VALUES('org-1','w1','frozen','o1','claude','old')");
  return { supa, neon, control, seal, mirrorState, neonState };
}
const err = async (p: Promise<unknown>) => { try { await p; return null; } catch (e) { return (e as any).code ?? String((e as Error).message); } };
const now = { settleMs: 0 };

test("baseline: today's state is exact parity, the plan writes nothing, and mirror mode is fenced while unsealed", async () => {
  const f = await fixtures();
  const c = await handoffCensus(f.neon, f.supa, now);
  expect(c).toMatchObject({ stable: true, exactParity: true, handoff: { writes: 0, blocked: [], affectedOrgs: 0 } });
  // fence: a mirror-mode control 503s BEFORE it touches Neon or the mirror
  const n0 = await f.neonState(), m0 = await f.mirrorState();
  expect(await err(f.control("mirror", T("u1"), "selection", sel("s1", false)))).toContain("503:mirror_begin:frozen source: copy not sealed");
  expect(await f.neonState()).toBe(n0); expect(await f.mirrorState()).toBe(m0);
});

test("window 1, before the handoff census (mirror off during copy): selection, re-enable and reset are all captured exactly", async () => {
  const f = await fixtures();
  await f.control("relayhistory", T("u1"), "selection", sel("s1", false));
  await f.control("relayhistory", T("u1"), "selection", sel("s1", true));            // re-enable removes the deletion again
  await f.control("relayhistory", T("frozen"), "selection", sel("old", true));        // re-enables a FROZEN deletion: mirror must drop it
  await f.control("relayhistory", T("u2", "org-2"), "reset", { request_id: "r1" });
  await f.control("relayhistory", T("u3"), "selection", sel("s9", false), { crashAfterNeonBegin: true }); // left pending on Neon
  const c = await handoffCensus(f.neon, f.supa, now);
  expect(c.exactParity).toBe(false);
  expect(c.handoff).toMatchObject({ insertHeads: 4, pendingHeads: 1, removeDeletions: 1, insertResets: 1, insertDeletions: 1, affectedOrgs: 2, affectedTenants: 4, blocked: [] });
  const r = await handoffSync(f.neon, f.supa, c.handoff.planSha256, now);
  expect(r).toMatchObject({ exactParity: true, handoff: { writes: 0 } });
  // the pending head was copied exactly, so the mirror gate blocks that user's batches like Neon's does
  expect((await f.supa.query<any>("SELECT pending,version::text v FROM sessions.ingest_source_control_heads WHERE user_id='u3'")).rows[0]).toEqual({ pending: true, v: "1" });
});

test("window 2, between handoff census and sync: a straggler control changes the plan and the sync refuses without writing", async () => {
  const f = await fixtures();
  await f.control("relayhistory", T("u1"), "selection", sel("s1", false));
  const c = await handoffCensus(f.neon, f.supa, now), m0 = await f.mirrorState();
  await f.control("relayhistory", T("u1"), "selection", sel("s2", false));
  expect(await err(handoffSync(f.neon, f.supa, c.handoff.planSha256, now))).toBe("plan_changed_rerun_handoff_census");
  expect(await f.mirrorState()).toBe(m0);
  const c2 = await handoffCensus(f.neon, f.supa, now);
  expect((await handoffSync(f.neon, f.supa, c2.handoff.planSha256, now)).exactParity).toBe(true);
});

test("window 2b, a straggler commits during the settle interval or inside the sync transaction: refused, nothing written", async () => {
  const f = await fixtures();
  await f.control("relayhistory", T("u1"), "selection", sel("s1", false));
  const during = { settleMs: 1, sleep: async () => { await f.control("relayhistory", T("u2"), "reset", { request_id: "late" }); } };
  const c = await handoffCensus(f.neon, f.supa, during);
  expect(c.stable).toBe(false); expect(c.handoff.blocked[0]).toBe("neon_not_stable");
  const good = await handoffCensus(f.neon, f.supa, now), m0 = await f.mirrorState();
  expect(await err(handoffSync(f.neon, f.supa, good.handoff.planSha256, { settleMs: 1, sleep: async () => { await f.control("relayhistory", T("u2"), "reset", { request_id: "later" }); } }))).toBe("neon_not_stable");
  expect(await f.mirrorState()).toBe(m0);
  // straggler between the in-transaction verify and COMMIT: the final Neon re-read catches it and rolls back
  const g = await handoffCensus(f.neon, f.supa, now);
  let reads = 0;
  const neon = { query: async (s: string, p?: unknown[]) => {
    if (s.startsWith("BEGIN ISOLATION LEVEL REPEATABLE READ") && ++reads === 3) await f.control("relayhistory", T("u4"), "selection", sel("x", false));
    return f.neon.query(s, p as any) as any; } };
  expect(await err(handoffSync(neon, f.supa, g.handoff.planSha256, now))).toBe("neon_changed_during_sync");
  expect(await f.mirrorState()).toBe(m0);
});

test("window 3, between sync and mirror enable (unsafe order): detected by the fenced re-census; without it the user would be stuck", async () => {
  const f = await fixtures();
  await f.control("relayhistory", T("u1"), "selection", sel("s1", false));
  const c = await handoffCensus(f.neon, f.supa, now);
  await handoffSync(f.neon, f.supa, c.handoff.planSha256, now);           // synced, but the mirror is still OFF...
  await f.control("relayhistory", T("u1"), "selection", sel("s2", false)); // ...so this commits on Neon unmirrored
  // mirror enable now; the REQUIRED fenced re-census before the seal sees it and the sync captures it
  const again = await handoffCensus(f.neon, f.supa, now);
  expect(again.exactParity).toBe(false); expect(again.handoff.overwriteHeads).toBe(1);
  expect((await handoffSync(f.neon, f.supa, again.handoff.planSha256, now)).exactParity).toBe(true);

  // Counterfactual proof of why that gate is mandatory: same gap, but seal WITHOUT the re-census.
  const g = await fixtures();
  await g.control("relayhistory", T("u1"), "selection", sel("s1", false));
  const gc = await handoffCensus(g.neon, g.supa, now); await handoffSync(g.neon, g.supa, gc.handoff.planSha256, now);
  await g.control("relayhistory", T("u1"), "selection", sel("s2", false));
  await g.seal();
  expect(await err(g.control("mirror", T("u1"), "selection", sel("s3", false)))).toBe("503:mirror_mismatch"); // fails closed, but...
  const stuck = await census(g.neon, g.supa);
  expect(stuck.heads.staleNeonNewerPending).toBe(1); // ...both sides are now pending at different versions: not qualified-repairable
});

test("window 4, during mirror enable (mixed isolates): mirror-mode calls are fenced, an old-isolate commit is caught by the settle check", async () => {
  const f = await fixtures();
  const n0 = await f.neonState();
  expect(await err(f.control("mirror", T("u1"), "reset", { request_id: "new" }))).toContain("copy not sealed");
  expect(await f.neonState()).toBe(n0);                                     // new isolate: nothing reached Neon
  const c = await handoffCensus(f.neon, f.supa, { settleMs: 1, sleep: async () => { await f.control("relayhistory", T("u2"), "reset", { request_id: "old-isolate" }); } });
  expect(c.handoff.blocked).toContain("neon_not_stable");
  const settled = await handoffCensus(f.neon, f.supa, { settleMs: 1, sleep: async () => {} });
  expect(settled.stable).toBe(true);
  expect((await handoffSync(f.neon, f.supa, settled.handoff.planSha256, now)).exactParity).toBe(true);
});

test("window 5, during the seal: controls stay fenced; a straggler before the seal's head compare is refused by the gate", async () => {
  const f = await fixtures();
  await f.control("relayhistory", T("u1"), "selection", sel("s1", false));
  const c = await handoffCensus(f.neon, f.supa, now); await handoffSync(f.neon, f.supa, c.handoff.planSha256, now);
  // seal running (unsealed until it commits): a mirror-mode control is fenced and leaves both sides untouched
  const n0 = await f.neonState(), m0 = await f.mirrorState();
  expect(await err(f.control("mirror", T("u1"), "selection", sel("s2", false)))).toContain("copy not sealed");
  expect(await f.neonState()).toBe(n0); expect(await f.mirrorState()).toBe(m0);
  // an old-isolate straggler inside the seal window: --seal's final head compare (== sealHeadParity) refuses
  await f.control("relayhistory", T("u1"), "selection", sel("s3", false));
  expect((await census(f.neon, f.supa)).parity.sealHeadParity).toBe(false);
  // ...and the handoff sync refuses once the seal row exists, because the fence is then open
  const g = await handoffCensus(f.neon, f.supa, now);
  await f.seal();
  expect(await err(handoffSync(f.neon, f.supa, g.handoff.planSha256, now))).toBe("sealed_fence_unavailable");
});

test("window 6, after the seal: the fence opens atomically, mirrored controls move both sides in lockstep, a pending retry completes both", async () => {
  const f = await fixtures();
  await f.control("relayhistory", T("u3"), "selection", sel("p", false), { crashAfterNeonBegin: true }); // pending before the handoff
  await f.control("relayhistory", T("u1"), "selection", sel("s1", false));
  const c = await handoffCensus(f.neon, f.supa, now); await handoffSync(f.neon, f.supa, c.handoff.planSha256, now);
  expect((await census(f.neon, f.supa)).parity.sealHeadParity).toBe(true);
  await f.seal();
  await f.control("mirror", T("u1"), "selection", sel("s2", false));        // version 2 on both
  await f.control("mirror", T("u5", "org-3"), "reset", { request_id: "fresh" }); // first-ever control, created on both
  expect(await err(f.control("mirror", T("u3"), "selection", sel("other", false)))).toContain("control pending"); // same refusal as Neon
  const retry = await f.control("mirror", T("u3"), "selection", sel("p", false)); // same digest: replay, then both complete
  expect(retry.apply).toBe(false);
  const after = await census(f.neon, f.supa);
  expect(after.parity.sealHeadParity).toBe(true); expect(after.heads.equal).toBe(3);
});

test("source epoch, policy and unexplained divergence refuse", async () => {
  const f = await fixtures();
  // a mirror-only privacy row for a tenant with NO Neon control cannot be explained by a control
  await f.supa.query("INSERT INTO sessions.ingest_frozen_source_deletions VALUES('org-9','w1','nobody','o','c','x')");
  expect((await handoffCensus(f.neon, f.supa, now)).handoff.blocked).toEqual(["unexplained_privacy_divergence"]);
  await f.supa.query("DELETE FROM sessions.ingest_frozen_source_deletions WHERE org_id='org-9'");
  // epoch bump on Neon mid-handoff
  const ok = await handoffCensus(f.neon, f.supa, now);
  await f.neon.query("UPDATE sessions.delivery_cutover_state SET epoch=$1", ["4".repeat(64)]);
  expect((await handoffCensus(f.neon, f.supa, now)).handoff.blocked).toContain("policy_mismatch");
  expect(await err(handoffSync(f.neon, f.supa, ok.handoff.planSha256, now))).toBe("policy_mismatch");
  await f.neon.query("UPDATE sessions.delivery_cutover_state SET epoch=$1,phase='halted'", [EPOCH]);
  expect(await err(handoffCensus(f.neon, f.supa, now))).toBe("neon_phase_not_active");
  await f.neon.query("UPDATE sessions.delivery_cutover_state SET phase='active'");
  // a mirror head Neon does not have, and a mirror head newer than Neon's
  await f.supa.query(`INSERT INTO sessions.ingest_source_control_heads(org_id,workspace_id,user_id,epoch,version,digest,kind,payload,pending) VALUES('org-1','w1','ghost',$1,1,$2,'reset','{}',false)`, [EPOCH, "1".repeat(64)]);
  expect((await handoffCensus(f.neon, f.supa, now)).handoff.blocked).toContain("mirror_head_without_neon_head");
  await f.control("relayhistory", T("ghost"), "reset", { request_id: "g" });
  await f.supa.query("UPDATE sessions.ingest_source_control_heads SET version=7 WHERE user_id='ghost'");
  expect((await handoffCensus(f.neon, f.supa, now)).handoff.blocked).toContain("mirror_head_newer");
  const out = JSON.stringify(await handoffCensus(f.neon, f.supa, now));
  for (const s of ["org-1", "ghost", "frozen", "w1"]) expect(out).not.toContain(s);
});

test("CLI: handoff usage refusals happen before any credential read", () => {
  const run = (args: string[]) => Bun.spawnSync([process.execPath, join(root, "scripts/frozen-control-parity.ts"), ...args], { stdin: new TextEncoder().encode(""), env: { ...process.env, SUPABASE_CLI: "/nonexistent" } }).exitCode;
  for (const a of [["--handoff-sync"], ["--handoff-sync", "--expect-plan", "x"], ["--handoff-census", "--settle-seconds", "30"], ["--handoff-census", "--settle-seconds", "1000"],
    ["--census", "--handoff-census"], ["--census", "--settle-seconds", "120"], ["--handoff-census", "--seed-pending-missing"], ["--handoff-census", "--expect-plan", "a".repeat(64)]])
    expect(run(a)).toBe(64);
  expect(run(["--handoff-census", "--settle-seconds", "120"])).toBe(64); // valid usage, but no production stdin
});
