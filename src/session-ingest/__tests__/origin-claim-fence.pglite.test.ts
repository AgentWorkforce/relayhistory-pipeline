/**
 * 20261105062010_ingest_origin_claim_fence, single-connection semantics (PGlite; every migration) and the Worker-side
 * routing (ledger-sql.ts originClaimFence). The multi-connection lock proofs (two origins concurrent, one origin
 * serialized, controls and the operator erase excluded) are in origin-claim-fence.postgres.test.ts.
 */
import { afterEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import { deliverBatch } from "../deliver.ts";
import { CLAIM_LOCK_WAIT_SET, resetOriginFenceHint, sqlLedger } from "../ledger-sql.ts";
import { LedgerUnavailableError } from "../ledger.ts";
import { deliveryAccount, prepareDelivery } from "../vendor/delivery/prepare.ts";
import { TENANT, batch, record } from "./support.ts";
import { authorityFixture, epoch, generation, identity } from "./privacy-authority-support.ts";

setDefaultTimeout(120_000);
const open: Array<() => unknown> = [];
afterEach(async () => {
  resetOriginFenceHint();
  for (const c of open.splice(0).reverse()) await c();
});
type Fixture = Awaited<ReturnType<typeof authorityFixture>>;
type Tenant = typeof TENANT;

async function fixture() {
  const f = await authorityFixture();
  open.push(f.close);
  await f.seal();
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
async function deliver(f: Fixture, tenant: Tenant, id: string, rid: string, origin: string, originClaimFence: boolean, seen?: string[]) {
  const raw = await batch([record({ record_id: rid, revision: 2, session_id: rid + "-s", origin_id: origin })], { batch_id: id, account_id: await deliveryAccount(tenant), origin_id: origin });
  const custody = await witness(f, tenant, raw);
  const ledger = sqlLedger(async (sql, params) => { seen?.push(sql); return f.run(sql, params as unknown[]); },
    { acceptanceMode: "buffered", expectedGeneration: generation, originClaimFence });
  return deliverBatch({ ...f.deps, ledger, custody } as never, tenant, raw);
}
const marks = async (f: Fixture) => (await f.db.query<{ n: number }>("SELECT count(*)::int n FROM sessions.ingest_origin_claim_marks")).rows[0]!.n;

describe("062010 schema", () => {
  test("three SECURITY DEFINER entry points for the ingest role only; helpers, ingest_lock_fences and the mark table stay owner-only", async () => {
    const f = await fixture();
    const fn = async (sig: string) => (await f.db.query<{ d: boolean; ingest: boolean; seeder: boolean; pub: boolean }>(
      `SELECT prosecdef d, has_function_privilege('bulls96_session_ingest',oid,'EXECUTE') ingest,
        has_function_privilege('bulls96_session_ingest_seeder',oid,'EXECUTE') seeder, has_function_privilege('anon',oid,'EXECUTE') pub
       FROM pg_proc WHERE oid=$1::regprocedure`, [sig])).rows[0]!;
    for (const sig of ["sessions.ingest_origin_claim_buffered_batch(jsonb)", "sessions.ingest_origin_prepare_buffer_stage(jsonb)", "sessions.ingest_origin_commit_buffered_batch(jsonb)"])
      expect(await fn(sig)).toEqual({ d: true, ingest: true, seeder: false, pub: false });
    for (const sig of ["sessions.ingest_origin_claim_enter(jsonb)", "sessions.ingest_origin_claim_leave(xid8)", "sessions.ingest_lock_fences(text,text,text)"])
      expect(await fn(sig)).toEqual({ d: false, ingest: false, seeder: false, pub: false });
    const t = (await f.db.query<{ s: boolean; i: boolean; rls: boolean }>(
      `SELECT has_table_privilege('bulls96_session_ingest','sessions.ingest_origin_claim_marks','SELECT') s,
        has_table_privilege('bulls96_session_ingest','sessions.ingest_origin_claim_marks','INSERT') i,
        (SELECT relrowsecurity FROM pg_class WHERE oid='sessions.ingest_origin_claim_marks'::regclass) rls`)).rows[0]!;
    expect(t).toEqual({ s: false, i: false, rls: true });
    // The runtime role cannot forge a mark (and so cannot downgrade any control's FOR UPDATE).
    await expect(f.run("INSERT INTO sessions.ingest_origin_claim_marks VALUES(pg_current_xact_id(),'x')")).rejects.toMatchObject({ code: "42501" });
    await expect(f.run("SELECT sessions.ingest_origin_claim_enter('{}'::jsonb)")).rejects.toMatchObject({ code: "42501" });
    // No org_id/workspace_id/user_id columns: ingest_lazy_user/begin never treat a mark as prior authority.
    const cols = (await f.db.query<{ a: string }>("SELECT attname a FROM pg_attribute WHERE attrelid='sessions.ingest_origin_claim_marks'::regclass AND attnum>0 AND NOT attisdropped ORDER BY attnum")).rows.map((r) => r.a);
    expect(cols).toEqual(["xact", "tenant"]);
    const src = (await f.db.query<{ s: string }>("SELECT prosrc s FROM pg_proc WHERE oid='sessions.ingest_lock_fences(text,text,text)'::regprocedure")).rows[0]!.s;
    expect(src).toContain("FOR SHARE;");
    expect(src).toContain("FOR UPDATE;");
    expect(src).toContain("pg_current_xact_id_if_assigned()");
    expect(src).toContain("operator_user_erasures");
  });

  test("malformed input is refused before any lock or mark (22023); another role is refused (42501)", async () => {
    const f = await fixture();
    for (const p of [[], "x", { tenant: TENANT }, { tenant: TENANT, originId: "o", batchId: "b", batchDigest: "nothex" }])
      await expect(f.run("SELECT sessions.ingest_origin_claim_buffered_batch($1::jsonb)", [JSON.stringify(p)])).rejects.toMatchObject({ code: "22023" });
    expect(await marks(f)).toBe(0);
  });
});

describe("flag-on deliveries", () => {
  test("claim, stage and commit go through the origin-scoped entry points; a flag-off retry replays the same batch; no mark remains", async () => {
    const f = await fixture();
    const seen: string[] = [];
    const u: Tenant = { ...TENANT, userId: "origin-user" };
    expect((await deliver(f, u, "b-a", "r-a", "machine-a", true, seen)).outcome).toBe("accepted_buffered");
    expect((await deliver(f, u, "b-b", "r-b", "machine-b", true, seen)).outcome).toBe("accepted_buffered");
    const calls = seen.map((s) => /sessions\.(\w+)\(/.exec(s)?.[1]).filter(Boolean);
    expect(calls).toContain("ingest_origin_claim_buffered_batch");
    expect(calls).toContain("ingest_origin_prepare_buffer_stage");
    expect(calls).toContain("ingest_origin_commit_buffered_batch");
    expect(calls).not.toContain("ingest_claim_buffered_batch");
    expect(calls).not.toContain("ingest_prepare_buffer_stage");
    expect(calls).not.toContain("ingest_commit_buffered_batch");
    expect((await deliver(f, u, "b-a", "r-a", "machine-a", false)).outcome).toBe("replay");
    expect((await deliver(f, u, "b-b", "r-b", "machine-b", true)).outcome).toBe("replay");
    expect(await marks(f)).toBe(0);
    const batches = (await f.db.query<{ n: number }>("SELECT count(*)::int n FROM sessions.ingest_batches WHERE user_id=$1 AND state='committed'", [u.userId])).rows[0]!.n;
    expect(batches).toBe(2);
  });

  test("an operator erase of the user refuses the origin-scoped claim with RH041 (the fence check is unchanged)", async () => {
    const f = await fixture();
    const u: Tenant = { ...TENANT, userId: "origin-erased" };
    expect((await deliver(f, u, "e-a", "r-e", "machine-a", true)).outcome).toBe("accepted_buffered");
    await f.db.query("SELECT sessions.operator_user_erase_begin($1,$2,$3,$4)", ["req-1", u.orgId, u.workspaceId, u.userId]);
    const refused = await deliver(f, u, "e-b", "r-e2", "machine-b", true).then(() => null, (e) => e);
    expect(refused).toMatchObject({ status: 503, stage: "claim", cause_code: "ledger_sqlstate_RH041" });
    expect(await marks(f)).toBe(0);
  });
});

describe("ledger-sql routing (INGEST_ORIGIN_CLAIM_FENCE)", () => {
  const claimInput = { tenant: TENANT, originId: "o", batchId: "b", batchDigest: "d".repeat(64), receiptId: "rhr_" + "1".repeat(64), sessions: [], records: [] } as never;
  const claimed = { result: { kind: "claimed", claimToken: "00000000-0000-4000-8000-000000000000", deleted: [], stale: [], resets: 0 } };
  function fake(fail: (fn: string) => string | null) {
    const calls: string[] = [];
    const run = async (sql: string) => {
      if (sql === "BEGIN" || sql === "COMMIT" || sql === "ROLLBACK" || sql === CLAIM_LOCK_WAIT_SET) { calls.push(sql); return { rows: [] }; }
      const fn = /sessions\.(\w+)\(/.exec(sql)![1]!; calls.push(fn);
      const code = fail(fn);
      if (code) throw Object.assign(new Error("x"), { code });
      return { rows: [claimed] };
    };
    return { run, calls };
  }
  test("off (default): the per-user functions only; on in canonical mode is refused", async () => {
    const { run, calls } = fake(() => null);
    await sqlLedger(run, { acceptanceMode: "buffered", expectedGeneration: generation }).claim(claimInput);
    expect(calls).toEqual(["ingest_claim_buffered_batch"]);
    expect(() => sqlLedger(run, { acceptanceMode: "canonical", originClaimFence: true })).toThrow(LedgerUnavailableError);
  });
  test("on: the origin-scoped function; 42883 (062010 absent) falls back to the per-user one and keeps using it for the isolate", async () => {
    const { run, calls } = fake((fn) => (fn.startsWith("ingest_origin_") ? "42883" : null));
    const ledger = sqlLedger(run, { acceptanceMode: "buffered", expectedGeneration: generation, originClaimFence: true });
    expect((await ledger.claim(claimInput)).kind).toBe("claimed");
    expect(calls).toEqual(["ingest_origin_claim_buffered_batch", "ingest_claim_buffered_batch"]);
    calls.length = 0;
    await ledger.claim(claimInput);
    expect(calls).toEqual(["ingest_claim_buffered_batch"]);
  });
  test("on, inside the claim lock-wait transaction: the failed origin attempt is rolled back before the per-user retry", async () => {
    const { run, calls } = fake((fn) => (fn.startsWith("ingest_origin_") ? "42883" : null));
    const ledger = sqlLedger(run, { acceptanceMode: "buffered", expectedGeneration: generation, originClaimFence: true, claimLockWaitMs: 250 });
    await ledger.claim(claimInput);
    expect(calls).toEqual(["BEGIN", CLAIM_LOCK_WAIT_SET, "ingest_origin_claim_buffered_batch", "ROLLBACK", "BEGIN", CLAIM_LOCK_WAIT_SET, "ingest_claim_buffered_batch", "COMMIT"]);
  });
  test("worker: INGEST_ORIGIN_CLAIM_FENCE=on (any case) routes the deliver ledger; unset/garbage leave it off; canonical acceptance refuses it", async () => {
    const { ingestSettings, deliverDepsFor } = await import("../worker.ts");
    const full = { HYPERDRIVE: { connectionString: "postgres://x" }, INGEST_FANOUT: {} as never, S3_ACCESS_KEY_ID: "k", S3_SECRET_ACCESS_KEY: "s", INGEST_SIGNING_KEY: "sign",
      INGEST_S3_BUCKET: "b", INGEST_KMS_KEY_ARN: "arn", INGEST_ACCEPTANCE_MODE: "buffered", INGEST_EXPECT_GENERATION: generation, INGEST_INLINE_MAX_SESSIONS: "0" };
    const target = { store: {} as never, keyRoot: "96bulls" };
    const routed = async (s: Awaited<ReturnType<typeof ingestSettings>>) => {
      const { run, calls } = fake(() => null);
      await deliverDepsFor(s!, run as never, target as never).ledger.claim(claimInput);
      return calls;
    };
    const unset = (await ingestSettings(full))!;
    expect("originClaimFence" in unset).toBe(false);
    expect(await routed(unset)).toEqual(["ingest_claim_buffered_batch"]);
    for (const v of ["on", "ON", " On "]) {
      const on = (await ingestSettings({ ...full, INGEST_ORIGIN_CLAIM_FENCE: v }))!;
      expect(on.originClaimFence).toBe(true);
      expect(await routed(on)).toEqual(["ingest_origin_claim_buffered_batch"]);
    }
    for (const v of ["", "off", "1", "true", "garbage"]) expect("originClaimFence" in (await ingestSettings({ ...full, INGEST_ORIGIN_CLAIM_FENCE: v }))!).toBe(false);
    const canonical = { ...full, INGEST_ACCEPTANCE_MODE: "canonical", INGEST_EXPECT_GENERATION: undefined, INGEST_INLINE_MAX_SESSIONS: undefined, INGEST_ORIGIN_CLAIM_FENCE: "on" };
    await expect(ingestSettings(canonical as never)).rejects.toThrow("INGEST_ORIGIN_CLAIM_FENCE needs buffered acceptance");
  });
  test("on: every other error passes through unchanged and never retries the per-user function", async () => {
    for (const code of ["55P03", "57014", "40001", "55000"]) {
      resetOriginFenceHint();
      const { run, calls } = fake((fn) => (fn.startsWith("ingest_origin_") ? code : null));
      const ledger = sqlLedger(run, { acceptanceMode: "buffered", expectedGeneration: generation, originClaimFence: true });
      const e = await ledger.claim(claimInput).then(() => null, (x) => x);
      expect(e).toBeInstanceOf(LedgerUnavailableError);
      expect(calls.filter((c) => c === "ingest_claim_buffered_batch")).toEqual([]);
    }
  });
});
