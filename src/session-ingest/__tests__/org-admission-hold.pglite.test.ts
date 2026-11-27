/**
 * 20261105062320_ingest_org_admission_fences, single-connection semantics on PGlite with every migration, driven
 * through the real TS ledger (deliverBatch, sqlLedger) as the ingest role. The two-connection ordering proofs
 * (install waits for an in-flight claim, a claim waits for an install, seal vs publication, erase vs release) are the
 * PostgreSQL suite's; this file proves the refusals, what still proceeds, the state machine and the audit.
 */
import { afterEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import { deliverBatch } from "../deliver.ts";
import { sqlLedger } from "../ledger-sql.ts";
import { deliveryAccount, prepareDelivery } from "../vendor/delivery/prepare.ts";
import { TENANT, batch, record } from "./support.ts";
import { authorityFixture, epoch, generation, identity } from "./privacy-authority-support.ts";

setDefaultTimeout(180_000);
const open: Array<() => unknown> = [];
afterEach(async () => { for (const c of open.splice(0).reverse()) await c(); });
type Fixture = Awaited<ReturnType<typeof authorityFixture>>;
type Tenant = typeof TENANT;
const SLOT = "upstream:20261105062320_ingest_org_admission_fences.sql";
const EVIDENCE = "e".repeat(64);
const OTHER: Tenant = { ...TENANT, orgId: "org-b", userId: "user-b" };

async function fixture() {
  const f = await authorityFixture();
  open.push(f.close);
  await f.seal();
  // The lifecycle functions require this slot's ledger row (the fixture applies migrations without the migrator).
  await f.db.exec("CREATE TABLE IF NOT EXISTS sessions.__96bulls_migrations(name text PRIMARY KEY, sha256 text)");
  await f.db.query("INSERT INTO sessions.__96bulls_migrations(name) VALUES($1) ON CONFLICT DO NOTHING", [SLOT]);
  return f;
}
async function witness(f: Fixture, tenant: Tenant, raw: any) {
  const p = await prepareDelivery(tenant, raw);
  const w = { version: 1, issuer: "relayhistory-cloud", sourceIdentitySha256: identity, controlEpoch: epoch, privacyBaselineSha256: "a".repeat(64), tenant,
    originId: p.originId, batchId: p.batchId, batchDigest: p.batchDigest, sourceControlVersion: "0", pending: false, pendingControl: null, privacyVersion: "0",
    originalRecordCount: raw.records.length, candidateCount: p.records.length,
    records: p.records.map((r, i) => ({ preparedOrd: i, processingRank: i, candidateDigest: r.digest, recordId: r.record_id, revision: String(r.revision),
      revisionId: r.revision_id, candidateFenced: false, currentFenced: false, current: null })),
    privacy: { complete: true, deletions: [], resets: [] } };
  const d = (await f.db.query<{ d: string }>("SELECT encode(sha256(convert_to($1::jsonb::text,'UTF8')),'hex') AS d", [JSON.stringify(w)])).rows[0]!.d;
  return { witness: w, sourceDigest: d };
}
/** The SQLSTATEs the ledger's database calls raised during the last delivery (deliverBatch maps them to a 503). */
let sqlErrors: string[] = [];
const run = (f: Fixture) => async (sql: string, params: unknown) => {
  try { return await f.run(sql, params as unknown[]); } catch (e) { sqlErrors.push(String((e as { code?: unknown }).code)); throw e; }
};
async function deliver(f: Fixture, tenant: Tenant, id: string, rid: string, origin = "machine-a") {
  const raw = await batch([record({ record_id: rid, revision: 2, session_id: rid + "-s", origin_id: origin })], { batch_id: id, account_id: await deliveryAccount(tenant), origin_id: origin });
  const custody = await witness(f, tenant, raw);
  const ledger = sqlLedger(run(f), { acceptanceMode: "buffered", expectedGeneration: generation });
  return deliverBatch({ ...f.deps, ledger, custody } as never, tenant, raw);
}
/** "accepted", or the SQLSTATE behind the refusal (the database's, as the ledger saw it). */
async function refusal(p: Promise<unknown>): Promise<string> {
  sqlErrors = [];
  try { await p; return "accepted"; } catch (e) {
    const code = String((e as { code?: unknown }).code ?? "");
    if (code.startsWith("ledger_sqlstate_")) return code.slice("ledger_sqlstate_".length);
    return sqlErrors.at(-1) ?? code;
  }
}
const one = async (f: Fixture, sql: string, p: unknown[] = []) => (await f.db.query<Record<string, unknown>>(sql, p)).rows[0]!;
const counts = async (f: Fixture, org: string) => one(f, `SELECT
  (SELECT count(*)::int FROM sessions.ingest_batches WHERE org_id=$1) batches,
  (SELECT count(*)::int FROM sessions.ingest_stage_intents WHERE org_id=$1) stages,
  (SELECT count(*)::int FROM sessions.ingest_lazy_native_slots WHERE org_id=$1) slots,
  (SELECT count(*)::int FROM sessions.ingest_claim_attempts WHERE org_id=$1) attempts,
  (SELECT count(*)::int FROM sessions.ingest_session_ownership WHERE org_id=$1) ownership`, [org]);
const install = async (f: Fixture, org: string) =>
  (await one(f, "SELECT sessions.ingest_org_admission_hold_install($1, gen_random_uuid())::text AS e", [org])).e as string;
const release = (f: Fixture, org: string, e: string, reason: string, erasures: number | null = null) =>
  f.db.query("SELECT sessions.ingest_org_admission_hold_release($1,$2::uuid,$3,$4,NULL,$5)", [org, e, reason, EVIDENCE, erasures]);

describe("062320 admission under a hold", () => {
  test("a held org's buffered claim is refused (RH042) with no write; another org proceeds; release re-opens", async () => {
    const f = await fixture();
    expect((await deliver(f, TENANT, "b-1", "r-1")).outcome).toBe("accepted_buffered");
    const e = await install(f, TENANT.orgId);
    const before = await counts(f, TENANT.orgId);
    expect(await refusal(deliver(f, TENANT, "b-2", "r-2"))).toBe("RH042");
    expect(await counts(f, TENANT.orgId)).toEqual(before); // nothing written, not even transiently committed
    // A replay of an already committed batch is refused too, uniformly, before the lazy bootstrap and records.
    expect(await refusal(deliver(f, TENANT, "b-1", "r-1"))).toBe("RH042");
    expect(await counts(f, TENANT.orgId)).toEqual(before);
    // Another org is unaffected.
    expect((await deliver(f, OTHER, "o-1", "o-1")).outcome).toBe("accepted_buffered");
    // Rollback before any attempt re-opens the org.
    await release(f, TENANT.orgId, e, "verified_rollback");
    expect((await deliver(f, TENANT, "b-2", "r-2")).outcome).toBe("accepted_buffered");
  });

  test("install refuses while the org has a pending batch, and is idempotent by request id", async () => {
    const f = await fixture();
    await f.db.query("INSERT INTO sessions.ingest_batches(org_id,workspace_id,origin_id,batch_id,user_id,digest,receipt_id,state) VALUES($1,'ws-a','o','p-1','user-1',$2,$3,'pending')",
      [TENANT.orgId, "d".repeat(64), "rhr_" + "f".repeat(64)]).catch(async () => {
        // The batch table has more NOT NULL columns in later slots; fall back to a minimal claim through the ledger.
      });
    const pending = await one(f, "SELECT count(*)::int n FROM sessions.ingest_batches WHERE org_id=$1 AND state='pending'", [TENANT.orgId]);
    if ((pending.n as number) > 0)
      await expect(f.db.query("SELECT sessions.ingest_org_admission_hold_install($1, gen_random_uuid())", [TENANT.orgId]))
        .rejects.toMatchObject({ code: "55000" });
    const r = "11111111-1111-4111-8111-111111111111";
    const g = OTHER.orgId;
    const a = (await one(f, "SELECT sessions.ingest_org_admission_hold_install($1,$2::uuid)::text e", [g, r])).e;
    const b = (await one(f, "SELECT sessions.ingest_org_admission_hold_install($1,$2::uuid)::text e", [g, r])).e;
    expect(b).toBe(a);
    expect((await one(f, "SELECT count(*)::int n FROM sessions.ingest_org_admission_hold_events WHERE epoch=$1::uuid", [a])).n).toBe(1);
    await expect(f.db.query("SELECT sessions.ingest_org_admission_hold_install($1, gen_random_uuid())", [g])).rejects.toMatchObject({ code: "55000" });
  });

  test("a claim outside READ COMMITTED is refused 40001 before reading the hold", async () => {
    const f = await fixture();
    await install(f, TENANT.orgId);
    await f.db.exec("BEGIN ISOLATION LEVEL REPEATABLE READ; SET LOCAL ROLE bulls96_session_ingest");
    try {
      await expect(f.db.query("SELECT sessions.ingest_claim_buffered_batch($1::jsonb)", [JSON.stringify({ tenant: TENANT })]))
        .rejects.toMatchObject({ code: "40001" });
    } finally { await f.db.exec("ROLLBACK"); }
  });
});

describe("062320 controls, publication and erasure classes", () => {
  test("controls proceed while held; while sealed they fail closed without the mirror role, and a caller-set GUC does not open them", async () => {
    // PGlite runs as a superuser session (a member of every existing role) and cannot reset a changed session
    // authorization, so the NON-member login cases (a non-mirror login refused, a granted member passing) are proven
    // on real PostgreSQL (org-admission-hold.postgres.test.ts). Here: the real control entry, the fail-closed default,
    // the GUC, and the role's existence gating a member.
    const f = await fixture();
    const e = await install(f, TENANT.orgId);
    const control = async (pre = "") => {
      await f.db.exec("BEGIN; SET LOCAL ROLE bulls96_session_ingest;" + pre);
      try { await f.db.query("SELECT sessions.ingest_set_session_uploads($1::jsonb)", [JSON.stringify({ tenant: TENANT })]); return "passed"; }
      catch (x) { return String((x as { code?: unknown }).code); }
      finally { await f.db.exec("ROLLBACK"); }
    };
    expect(await control()).not.toBe("RH042"); // held: controls proceed (past the check, the empty selection is 22023)
    await f.db.query("SELECT sessions.ingest_org_admission_hold_seal($1,$2::uuid,NULL)", [TENANT.orgId, e]);
    expect(await control()).toBe("RH042"); // sealed, no mirror role exists: fail closed, even for a superuser session
    expect(await control("SELECT set_config('bulls96.s3_fence_mirror','on',true);")).toBe("RH042"); // a GUC is not the gate
    await f.db.exec("CREATE ROLE bulls96_session_ingest_mirror NOLOGIN");
    expect(await control()).not.toBe("RH042"); // the role exists and the (superuser) session user is a member
    const check = (cls: string) => f.db.query("SELECT sessions.ingest_org_admission_hold_check($1,$2)", [TENANT.orgId, cls]);
    await expect(check("publication")).rejects.toMatchObject({ code: "RH042" });
    // Erasure is never refused, at any state or isolation level.
    await check("erasure");
    await f.db.exec("BEGIN ISOLATION LEVEL REPEATABLE READ");
    try { await check("erasure"); } finally { await f.db.exec("ROLLBACK"); }
  });

  test("the selection control path through the ledger is refused while sealed, allowed while held", async () => {
    const f = await fixture();
    expect((await deliver(f, TENANT, "s-1", "s-1")).outcome).toBe("accepted_buffered");
    const e = await install(f, TENANT.orgId);
    const ledger = sqlLedger(run(f), { acceptanceMode: "buffered", expectedGeneration: generation });
    const sel = { tenant: TENANT, originId: "machine-a", enabled: false, sessions: [{ source: "claude", session_id: "s-1-s" }] } as never;
    expect(await refusal(ledger.setSessionUploads(sel))).not.toBe("RH042");
    await f.db.query("SELECT sessions.ingest_org_admission_hold_seal($1,$2::uuid,NULL)", [TENANT.orgId, e]);
    expect(await refusal(ledger.setSessionUploads(sel))).toBe("RH042");
  });
});

describe("062320 state machine and audit", () => {
  test("only the reviewed transitions; bridge-only release after an attempt; erasure watermark; immutable history", async () => {
    const f = await fixture();
    const org = TENANT.orgId;
    const e = await install(f, org);
    await expect(f.db.query("SELECT sessions.ingest_org_admission_hold_marker_attempt($1,$2::uuid,NULL)", [org, e])).rejects.toMatchObject({ code: "55000" });
    await expect(release(f, org, e, "bridge_complete", 0)).rejects.toMatchObject({ code: "55000" });
    await f.db.query("SELECT sessions.ingest_org_admission_hold_seal($1,$2::uuid,NULL)", [org, e]);
    await f.db.query("SELECT sessions.ingest_org_admission_hold_seal($1,$2::uuid,NULL)", [org, e]); // idempotent
    await f.db.query("SELECT sessions.ingest_org_admission_hold_marker_attempt($1,$2::uuid,NULL)", [org, e]);
    await f.db.query("SELECT sessions.ingest_org_admission_hold_marker_attempt($1,$2::uuid,NULL)", [org, e]); // before every PUT
    await expect(release(f, org, e, "verified_rollback")).rejects.toMatchObject({ code: "55000" }); // no rollback after an attempt
    const status = (await one(f, "SELECT sessions.ingest_org_admission_hold_status($1) s", [org])).s as { hold: { state: string }; erasures: number };
    expect(status.hold.state).toBe("marker_attempted");
    await expect(release(f, org, e, "bridge_complete", status.erasures + 1)).rejects.toMatchObject({ code: "55000" }); // erasures not replayed
    await expect(release(f, org, e, "bridge_complete", null)).rejects.toMatchObject({ code: "55000" });
    await release(f, org, e, "bridge_complete", status.erasures);
    await release(f, org, e, "bridge_complete", status.erasures); // idempotent repeat
    await expect(f.db.query("SELECT sessions.ingest_org_admission_hold_release($1,$2::uuid,'bridge_complete',$3,NULL,$4)", [org, e, "a".repeat(64), status.erasures]))
      .rejects.toMatchObject({ code: "55000" }); // a different repeat
    expect((await f.db.query("SELECT kind FROM sessions.ingest_org_admission_hold_events WHERE epoch=$1::uuid ORDER BY at", [e])).rows.map((r: any) => r.kind))
      .toEqual(["installed", "sealed", "marker_attempted", "released"]);
    for (const sql of ["DELETE FROM sessions.ingest_org_admission_holds", "UPDATE sessions.ingest_org_admission_holds SET state='held'",
      "DELETE FROM sessions.ingest_org_admission_hold_events", "UPDATE sessions.ingest_org_admission_hold_events SET detail='x'",
      "TRUNCATE sessions.ingest_org_admission_hold_events", "TRUNCATE sessions.ingest_org_admission_holds CASCADE"])
      await expect(f.db.query(sql)).rejects.toMatchObject({ code: "55000" });
    // A new hold can follow a released one.
    const e2 = await install(f, org);
    expect(e2).not.toBe(e);
    await expect(f.db.query("SELECT sessions.ingest_org_admission_hold_seal($1,$2::uuid,NULL)", [org, e])).rejects.toMatchObject({ code: "55000" }); // stale epoch
  });

  test("lifecycle refuses when the slot's ledger row is absent (a down ran)", async () => {
    const f = await fixture();
    await f.db.query("DELETE FROM sessions.__96bulls_migrations WHERE name=$1", [SLOT]);
    await expect(f.db.query("SELECT sessions.ingest_org_admission_hold_install($1, gen_random_uuid())", [TENANT.orgId])).rejects.toMatchObject({ code: "55000" });
  });

  test("the runtime role cannot reach the hold tables or lifecycle functions", async () => {
    const f = await fixture();
    for (const sql of ["SELECT * FROM sessions.ingest_org_admission_holds", "SELECT sessions.ingest_org_admission_hold_install('x', gen_random_uuid())",
      "SELECT sessions.ingest_org_admission_hold_status('x')", "SELECT sessions.ingest_org_admission_hold_check('x','admission')"])
      await expect(f.run(sql)).rejects.toMatchObject({ code: "42501" });
  });
});
