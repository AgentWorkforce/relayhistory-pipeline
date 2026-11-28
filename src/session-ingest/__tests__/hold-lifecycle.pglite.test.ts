/**
 * hold-lifecycle.ts against the REAL slot 062320 and 062340 functions (PGlite with every migration), through
 * scripts/backlog-hold.ts pgHoldSession (the tool's own transaction shapes), with real uploads through the TS ledger
 * (deliverBatch) as the ingest role. A synthetic two-org wave-1 run, each org with its own outcome:
 *   A: hold-dry -> install -> (FULL dispatched after the install) -> seal+bind -> verified rollback -> admission back.
 *   B: install -> seal+bind -> marker attempt -> rollback REFUSED -> bridge_complete (empty erase set) -> admission back.
 * Measured: admission refused (RH042) from install to release, as downtime per org (installed_at to released_at, plus
 * the refused attempts); uploads accepted again after release; old and new records readable afterwards.
 * Also:
 *   - a lost-response retry of the install (same epoch) and of seal+bind (same binding);
 *   - a seal+bind interrupted at the bind rolls back whole (still `held`) and its retry succeeds;
 *   - an erase arriving during the hold after an empty initial read makes the empty-set release refuse.
 * Single connection: the two-connection orderings are hold-lifecycle.postgres.test.ts.
 */
import { afterEach, expect, setDefaultTimeout, test } from "bun:test";
import { deliverBatch } from "../deliver.ts";
import { sqlLedger } from "../ledger-sql.ts";
import { deliveryAccount, prepareDelivery } from "../vendor/delivery/prepare.ts";
import { batch, record } from "./support.ts";
import { authorityFixture, epoch as controlEpoch, generation, identity } from "./privacy-authority-support.ts";
import { evidenceHash } from "../authority-evidence.ts";
import { epochHash } from "../erasure-window.ts";
import { HOLD_BIND_FUNCTION, holdRun, type HoldRunDeps, type HoldSqlSession } from "../hold-lifecycle.ts";
import { pgHoldSession } from "../../../scripts/backlog-hold.ts";

setDefaultTimeout(240_000);
const open: (() => unknown)[] = [];
afterEach(async () => { for (const c of open.splice(0).reverse()) await c(); });
const SHA = "1".repeat(40), DRY = "801";
const A = "5e5e5e5e-0000-4000-8000-00000000000a", B = "5e5e5e5e-0000-4000-8000-00000000000b";
const FULL_EVIDENCE = "f".repeat(64), MARKER_EVIDENCE = "c".repeat(64), ROLLBACK_EVIDENCE = "d".repeat(64);
const tenant = (orgId: string) => ({ orgId, workspaceId: "ws-a", userId: "user-1" });

async function fixture() {
  const f = await authorityFixture();
  open.push(f.close);
  await f.seal();
  await f.db.exec("CREATE TABLE IF NOT EXISTS sessions.__96bulls_migrations(name text PRIMARY KEY, sha256 text)");
  for (const slot of ["upstream:20261105062320_ingest_org_admission_fences.sql", "upstream:20261105062340_ingest_org_erasure_history.sql"])
    await f.db.query("INSERT INTO sessions.__96bulls_migrations(name) VALUES($1) ON CONFLICT DO NOTHING", [slot]);
  const session = pgHoldSession(f.db as never);
  const run = (step: HoldRunDeps["step"], over: Partial<HoldRunDeps> = {}, s: HoldSqlSession = session) =>
    holdRun({ step, orgs: [A, B], sha: SHA, withSession: (fn) => fn(s), now: () => Date.now(), concurrency: 1, ...over });
  const one = async (sql: string, p: unknown[] = []) => (await f.db.query<Record<string, any>>(sql, p)).rows[0]!;
  const status = async (org: string) => (await one("SELECT sessions.ingest_org_admission_hold_status($1) s", [org])).s as any;
  // Real uploads through the TS ledger as the ingest role; "accepted" or the SQLSTATE behind the refusal.
  let n = 0;
  const upload = async (org: string): Promise<string> => {
    n++;
    const t = tenant(org), id = `b-${org.slice(-1)}-${n}`, rid = `r-${org.slice(-1)}-${n}`;
    const raw = await batch([record({ record_id: rid, revision: 2, session_id: rid + "-s", origin_id: "machine-a" })],
      { batch_id: id, account_id: await deliveryAccount(t), origin_id: "machine-a" });
    const p = await prepareDelivery(t, raw);
    const w = { version: 1, issuer: "relayhistory-cloud", sourceIdentitySha256: identity, controlEpoch, privacyBaselineSha256: "a".repeat(64), tenant: t,
      originId: p.originId, batchId: p.batchId, batchDigest: p.batchDigest, sourceControlVersion: "0", pending: false, pendingControl: null, privacyVersion: "0",
      originalRecordCount: raw.records.length, candidateCount: p.records.length,
      records: p.records.map((r, i) => ({ preparedOrd: i, processingRank: i, candidateDigest: r.digest, recordId: r.record_id, revision: String(r.revision),
        revisionId: r.revision_id, candidateFenced: false, currentFenced: false, current: null })),
      privacy: { complete: true, deletions: [], resets: [] } };
    const d = (await one("SELECT encode(sha256(convert_to($1::jsonb::text,'UTF8')),'hex') AS d", [JSON.stringify(w)])).d;
    const errs: string[] = [];
    const ledger = sqlLedger(async (sql: string, params: unknown) => { try { return await f.run(sql, params as unknown[]); } catch (e) { errs.push(String((e as any).code)); throw e; } },
      { acceptanceMode: "buffered", expectedGeneration: generation });
    try { await deliverBatch({ ...f.deps, ledger, custody: { witness: w, sourceDigest: d } } as never, t, raw); return "accepted"; }
    catch (e) { const c = String((e as any).code ?? ""); return c.startsWith("ledger_sqlstate_") ? c.slice(16) : errs.at(-1) ?? c; }
  };
  const batches = async (org: string) => (await one("SELECT count(*)::int n FROM sessions.ingest_batches WHERE org_id=$1", [org])).n as number;
  const publicationOpen = async (org: string) => {
    await f.db.exec("BEGIN ISOLATION LEVEL READ COMMITTED");
    try { await f.db.query("SELECT sessions.ingest_org_admission_hold_check($1,'publication')", [org]); return true; }
    catch (e) { return (e as any).code; } finally { await f.db.exec("ROLLBACK"); }
  };
  return { f, session, run, status, upload, batches, publicationOpen, one };
}
const by = (s: { per_org: { org_hash: string }[] }) => Object.fromEntries(s.per_org.map((p) => [p.org_hash, p])) as Record<string, any>;
const H = (o: string) => evidenceHash("org", o);

test("synthetic wave 1: two orgs, measured admission downtime, independent outcomes, resumed uploads and publication, old and new records readable", async () => {
  const x = await fixture();
  // Before the hold: both orgs upload.
  expect([await x.upload(A), await x.upload(B)]).toEqual(["accepted", "accepted"]);
  const before = { A: await x.batches(A), B: await x.batches(B) };
  // hold-dry: both ready to install (no hold, nothing pending, no open canonical claim).
  const d = await x.run("dry");
  expect(d.per_org.every((p: any) => p.verdict === "ready" && p.hold === "absent" && p.pending_batches === 0 && p.canonical_open === 0)).toBe(true);
  // Install (and a lost-response retry: the same request id gives the same epoch, one event).
  const approved = d.per_org.map((p: any) => ({ orgHash: p.org_hash, epochHash: null }));
  const i1 = await x.run("install", { dryRun: DRY, approved });
  const i2 = await x.run("install", { dryRun: DRY, approved });
  expect(i1.complete && i2.complete).toBe(true);
  expect(i2.per_org.map((p: any) => p.epoch_hash)).toEqual(i1.per_org.map((p: any) => p.epoch_hash));
  for (const o of [A, B]) {
    const st = await x.status(o);
    expect(st.hold.state).toBe("held");
    expect(epochHash(st.hold.epoch)).toBe(by(i1)[H(o)].epoch_hash);
    expect((await x.one("SELECT count(*)::int n FROM sessions.ingest_org_admission_hold_events WHERE epoch=$1::uuid AND kind='installed'", [st.hold.epoch])).n).toBe(1);
  }
  // Admission downtime: every upload refused (RH042) while held; nothing written.
  let refused = 0;
  for (const o of [A, B, A]) { const r = await x.upload(o); expect(r).toBe("RH042"); refused++; }
  expect([await x.batches(A), await x.batches(B)]).toEqual([before.A, before.B]);
  // The FULL is dispatched AFTER the install (062340: installed_at < start <= sealed_at).
  await x.f.db.query("SELECT pg_sleep(0.01)");
  const fullStartedMs = Date.now();
  await x.f.db.query("SELECT pg_sleep(0.01)");
  const sealApproved = i1.per_org.map((p: any) => ({ orgHash: p.org_hash, epochHash: p.epoch_hash }));
  const full = { run: "802", startedMs: fullStartedMs, evidence: FULL_EVIDENCE };
  const s1 = await x.run("seal", { dryRun: "803", approved: sealApproved, full });
  expect(s1.complete).toBe(true);
  const s2 = await x.run("seal", { dryRun: "803", approved: sealApproved, full }); // lost-response retry: same binding, no error
  expect(s2.complete).toBe(true);
  for (const o of [A, B]) {
    const st = await x.status(o);
    expect(st.hold.state).toBe("sealed");
    expect(st.selected_full).toMatchObject({ run: "802", started_ms: fullStartedMs, evidence_sha256: FULL_EVIDENCE });
    expect(await x.publicationOpen(o)).toBe("RH042"); // publication refused while sealed
  }
  // A: verified rollback (no attempt). B: a marker attempt, then a rollback is REFUSED.
  const epochB = (await x.status(B)).hold.epoch;
  await x.f.db.query("SELECT sessions.ingest_org_admission_hold_marker_attempt($1,$2::uuid,$3)", [B, epochB, MARKER_EVIDENCE]);
  const rb = await x.run("rollback", { dryRun: "804", approved: sealApproved, evidence: ROLLBACK_EVIDENCE });
  expect(by(rb)[H(A)]).toMatchObject({ verdict: "rolled_back", hold: "released" });
  expect(by(rb)[H(B)]).toMatchObject({ verdict: "refused", refused: "hold_marker_attempted" });
  expect(rb.complete).toBe(false);
  // The SQL refuses it too, on its own.
  await expect(x.f.db.query("SELECT sessions.ingest_org_admission_hold_release($1,$2::uuid,'verified_rollback',$3,NULL,0)", [B, epochB, ROLLBACK_EVIDENCE]))
    .rejects.toMatchObject({ code: "55000" });
  // A is open again; B is still held (independent outcomes).
  expect(await x.upload(A)).toBe("accepted");
  expect(await x.publicationOpen(A)).toBe(true);
  expect(await x.upload(B)).toBe("RH042");
  refused++;
  // B: bridge_complete with an EMPTY erase set (the only release this tree can prove), then B is open again.
  const stB = await x.status(B);
  expect(stB.erasures).toBe(0);
  await x.f.db.query("SELECT sessions.ingest_org_admission_hold_release($1,$2::uuid,'bridge_complete',$3,NULL,$4)", [B, epochB, "e".repeat(64), stB.erasures]);
  expect(await x.upload(B)).toBe("accepted");
  expect(await x.publicationOpen(B)).toBe(true);
  // Downtime per org: installed_at to released_at (both from the hold history), plus the refused attempts counted.
  const downtime = async (o: string) => (await x.one(`SELECT floor(extract(epoch FROM released_at - installed_at) * 1000)::bigint ms
    FROM sessions.ingest_org_admission_holds WHERE held_org=$1 AND state='released'`, [o])).ms;
  const msA = Number(await downtime(A)), msB = Number(await downtime(B));
  expect(msA).toBeGreaterThan(0);
  expect(msB).toBeGreaterThan(msA); // B was held through its attempt to the bridge
  expect(refused).toBe(4);
  // Old and new records are both there and readable.
  expect(await x.batches(A)).toBe(before.A + 1);
  expect(await x.batches(B)).toBe(before.B + 1);
  // Buffered uploads: each accepted batch left its stage intent (the record before the hold and the one after).
  for (const o of [A, B]) expect((await x.one("SELECT count(*)::int n FROM sessions.ingest_stage_intents WHERE org_id=$1", [o])).n).toBe(2);
});

test("a seal+bind interrupted at the bind rolls back whole (still held); the retry seals and binds", async () => {
  const x = await fixture();
  const d = await x.run("dry", { orgs: [A] });
  const i = await x.run("install", { orgs: [A], dryRun: DRY, approved: d.per_org.map((p: any) => ({ orgHash: p.org_hash, epochHash: null })) });
  await x.f.db.query("SELECT pg_sleep(0.01)");
  const full = { run: "802", startedMs: Date.now(), evidence: FULL_EVIDENCE };
  await x.f.db.query("SELECT pg_sleep(0.01)");
  const approved = i.per_org.map((p: any) => ({ orgHash: p.org_hash, epochHash: p.epoch_hash }));
  // A session whose bind statement dies (a lost connection mid-transaction).
  const broken: HoldSqlSession = { ...x.session, tx: (fn) => x.session.tx((q) => fn(async (t, p) => {
    if (t.includes(HOLD_BIND_FUNCTION)) throw Object.assign(new Error("connection lost"), { code: "08006" });
    return q(t, p);
  })) };
  const s1 = await x.run("seal", { orgs: [A], dryRun: "803", approved, full }, broken);
  expect(s1.per_org[0]).toMatchObject({ verdict: "refused", refused: "hold_seal_failed" });
  const st = await x.status(A);
  expect(st.hold.state).toBe("held"); // the seal was rolled back with the bind
  expect(st.selected_full).toBeNull();
  const s2 = await x.run("seal", { orgs: [A], dryRun: "803", approved, full });
  expect(s2.complete).toBe(true);
  expect((await x.status(A)).hold.state).toBe("sealed");
  // A FULL started before the install never seals (the tool refuses early; 062340 would too).
  const x2 = await fixture();
  const early = Date.now() - 60_000;
  const d2 = await x2.run("dry", { orgs: [B] });
  const i2 = await x2.run("install", { orgs: [B], dryRun: DRY, approved: d2.per_org.map((p: any) => ({ orgHash: p.org_hash, epochHash: null })) });
  const s3 = await x2.run("seal", { orgs: [B], dryRun: "803", approved: i2.per_org.map((p: any) => ({ orgHash: p.org_hash, epochHash: p.epoch_hash })),
    full: { ...full, startedMs: early } });
  expect(s3.per_org[0]).toMatchObject({ verdict: "refused", refused: "hold_full_order" });
  expect((await x2.status(B)).hold.state).toBe("held");
});

test("an erase arriving during the hold after an empty initial read makes the empty-set release refuse", async () => {
  const x = await fixture();
  const d = await x.run("dry", { orgs: [A] });
  const i = await x.run("install", { orgs: [A], dryRun: DRY, approved: d.per_org.map((p: any) => ({ orgHash: p.org_hash, epochHash: null })) });
  await x.f.db.query("SELECT pg_sleep(0.01)");
  const full = { run: "802", startedMs: Date.now(), evidence: FULL_EVIDENCE };
  await x.f.db.query("SELECT pg_sleep(0.01)");
  await x.run("seal", { orgs: [A], dryRun: "803", approved: i.per_org.map((p: any) => ({ orgHash: p.org_hash, epochHash: p.epoch_hash })), full });
  const epoch = (await x.status(A)).hold.epoch;
  await x.f.db.query("SELECT sessions.ingest_org_admission_hold_marker_attempt($1,$2::uuid,$3)", [A, epoch, MARKER_EVIDENCE]);
  const initial = (await x.status(A)).erasures;
  expect(initial).toBe(0); // the initial read: empty
  // A session erase lands during the hold (an erasure source write; 062340 records it in the history).
  await x.f.db.query("INSERT INTO sessions.session_erasure_fences(org_id, workspace_id, source, session_id) VALUES ($1,'ws-a','claude','sess-erased')", [A]);
  expect((await x.status(A)).erasures).toBe(1);
  // A release against the empty initial read refuses: an empty initial set never guarantees release later.
  await expect(x.f.db.query("SELECT sessions.ingest_org_admission_hold_release($1,$2::uuid,'bridge_complete',$3,NULL,$4)", [A, epoch, "e".repeat(64), initial]))
    .rejects.toMatchObject({ code: "55000" });
  expect((await x.status(A)).hold.state).toBe("marker_attempted"); // still held (fail safe)
});

test("an open canonical claim: hold-dry is not ready, and the install rolls back whole (no hold row); a pending batch refuses too", async () => {
  const x = await fixture();
  await x.f.db.query(`INSERT INTO sessions.ingest_batches(org_id,workspace_id,origin_id,batch_id,user_id,digest,receipt_id,state,committed_at)
    VALUES($1,'ws','o','b-canon','u',$2,$3,'committed',clock_timestamp())`, [A, "d".repeat(64), "rhr_" + "f".repeat(64)]);
  await x.f.db.query(`INSERT INTO sessions.ingest_claim_attempts(claim_token,org_id,workspace_id,user_id,origin_id,batch_id,digest,epochs)
    VALUES(gen_random_uuid(),$1,'ws','u','o','b-canon',$2,'[]'::jsonb)`, [A, "d".repeat(64)]);
  await x.f.db.query(`INSERT INTO sessions.ingest_batches(org_id,workspace_id,origin_id,batch_id,user_id,digest,receipt_id,state)
    VALUES($1,'ws','o','b-pend','u',$2,$3,'pending')`, [B, "d".repeat(64), "rhr_" + "e".repeat(64)]);
  const d = await x.run("dry");
  expect(by(d)[H(A)]).toMatchObject({ verdict: "refused", refused: "hold_canonical_open", canonical_open: 1 });
  expect(by(d)[H(B)]).toMatchObject({ verdict: "refused", refused: "hold_pending_batch", pending_batches: 1 });
  // Even approved anyway, the execute re-checks in its own transaction: A rolls back after _install, B is refused by it.
  const i = await x.run("install", { dryRun: DRY, approved: [A, B].map((o) => ({ orgHash: H(o), epochHash: null })) });
  expect(by(i)[H(A)]).toMatchObject({ verdict: "refused", refused: "hold_canonical_open" });
  expect(by(i)[H(B)]).toMatchObject({ verdict: "refused", refused: "hold_pending_batch" });
  expect((await x.status(A)).hold).toBeNull();
  expect((await x.status(B)).hold).toBeNull();
});
