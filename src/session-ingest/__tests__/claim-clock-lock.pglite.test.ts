/**
 * 20261105043500_lazy_claim_clock_narrowing, single-connection semantics (PGlite; every migration incl. 043500).
 * The two-connection lock behaviour (A1-A4, B1, B2) is proven on real PG17 in claim-clock-lock.postgres.test.ts;
 * this file pins what one connection can: the READ COMMITTED fail-closed check, U4, the 2c no-write proof and the
 * same-key ownership paths (P0W01 for an owned key; an initialized empty slot is shared, never a 23505).
 */
import { afterEach, expect, setDefaultTimeout, test } from "bun:test";
import { deliverBatch } from "../deliver.ts";
import { sqlLedger } from "../ledger-sql.ts";
import { deliveryAccount, prepareDelivery } from "../vendor/delivery/prepare.ts";
import type { SourceCurrentRecord } from "../custody-protocol.ts";
import { TENANT, batch, record } from "./support.ts";
import { authorityFixture, epoch, generation, identity } from "./privacy-authority-support.ts";

setDefaultTimeout(120_000);
const open: Array<() => unknown> = [];
afterEach(async () => {
  for (const c of open.splice(0).reverse()) await c();
});
type Fixture = Awaited<ReturnType<typeof authorityFixture>>;
type Tenant = typeof TENANT;
const X: Tenant = { ...TENANT, userId: "claim-x" }, Y: Tenant = { ...TENANT, userId: "claim-y" };

async function fixture() {
  const f = await authorityFixture();
  open.push(f.close);
  await f.seal();
  return f;
}
async function witness(f: Fixture, tenant: Tenant, raw: any, opts: { current?: SourceCurrentRecord | null; fenced?: boolean } = {}) {
  const p = await prepareDelivery(tenant, raw);
  const w = { version: 1, issuer: "relayhistory-cloud", sourceIdentitySha256: identity, controlEpoch: epoch, privacyBaselineSha256: "a".repeat(64), tenant,
    originId: p.originId, batchId: p.batchId, batchDigest: p.batchDigest, sourceControlVersion: "0", pending: false, pendingControl: null, privacyVersion: "0",
    originalRecordCount: raw.records.length, candidateCount: p.records.length,
    records: p.records.map((r, i) => ({ preparedOrd: i, processingRank: i, candidateDigest: r.digest, recordId: r.record_id, revision: String(r.revision),
      revisionId: r.revision_id, candidateFenced: opts.fenced ?? false, currentFenced: false, current: opts.current ?? null })),
    privacy: { complete: true, deletions: [], resets: [] } };
  const d = (await f.db.query<{ d: string }>("SELECT encode(sha256(convert_to($1::jsonb::text,'UTF8')),'hex') AS d", [JSON.stringify(w)])).rows[0]!.d;
  return { witness: w, sourceDigest: d };
}
/** Source current custody owned by `owner` (identical for every claimant: never contradictory). */
async function current(owner: Tenant, id: string): Promise<SourceCurrentRecord> {
  const r = (await prepareDelivery(owner, await batch([record({ record_id: id, revision: 1, session_id: id + "-s" })], { account_id: await deliveryAccount(owner) }))).records[0]!;
  return { userId: owner.userId, revision: String(r.revision), digest: r.digest, revisionId: r.revision_id, kind: r.kind, source: r.source, sessionId: r.session_id,
    operation: r.operation, catalogAcceptedSeq: null, projectionAcceptedSeq: "17" };
}
/** Delivers through the real ledger; `onClaim` runs right after the claim transaction committed. */
async function deliver(f: Fixture, tenant: Tenant, id: string, rid: string, opts: { current?: SourceCurrentRecord | null; fenced?: boolean; revision?: number; beforeReceipt?: boolean; onClaim?: () => Promise<void> } = {}) {
  const raw = await batch([record({ record_id: rid, revision: opts.revision ?? 2, session_id: rid + "-s" })], { batch_id: id, account_id: await deliveryAccount(tenant) });
  const custody = await witness(f, tenant, raw, opts);
  const ledger = sqlLedger(async (sql, params) => {
    const r = await f.run(sql, params as unknown[]);
    if (sql.includes("ingest_claim_buffered_batch") && opts.onClaim) await opts.onClaim();
    return r;
  }, { acceptanceMode: "buffered", expectedGeneration: generation });
  if (opts.beforeReceipt) ledger.commitBuffered = async () => { throw Error("synthetic pause before durable receipt"); };
  return deliverBatch({ ...f.deps, ledger, custody } as never, tenant, raw);
}
const batchRow = async (f: Fixture, id: string) =>
  (await f.db.query<{ state: string; kind: string | null }>("SELECT state,buffer_receipt_kind kind FROM sessions.ingest_batches WHERE batch_id=$1", [id])).rows[0];
const count = async (f: Fixture, table: string, rid: string) =>
  (await f.db.query<{ n: number }>(`SELECT count(*)::int n FROM sessions.${table} WHERE record_id=$1`, [rid])).rows[0]!.n;

test("043500 is applied: lazy_records holds KEY SHARE + the origin advisory namespace; ack_batch returns suppressed above the exclusive clock lock", async () => {
  const f = await fixture();
  const src = async (sig: string) => (await f.db.query<{ s: string; d: boolean }>(`SELECT prosrc s,prosecdef d FROM pg_proc WHERE oid='sessions.${sig}'::regprocedure`)).rows[0]!;
  const records = await src("ingest_lazy_records(jsonb)"), ack = await src("ingest_lazy_ack_batch(jsonb)");
  expect(records.s).toContain("lazy-native-origin-v1");
  expect(records.s).toContain("WHERE singleton FOR KEY SHARE;");
  expect(records.s).not.toContain("WHERE singleton FOR UPDATE;");
  expect(records.s.indexOf("pg_advisory_xact_lock")).toBeLessThan(records.s.indexOf("INSERT INTO sessions.ingest_fence_versions"));
  expect(records.s).toContain("ON CONFLICT(key_sha256) DO NOTHING");
  expect([records.d, ack.d]).toEqual([false, false]);
  const suppressed = ack.s.indexOf("IF p->>'buffer_receipt_kind'='suppressed' THEN");
  expect(suppressed).toBeGreaterThan(ack.s.indexOf("real user seal required before any receipt"));
  expect(suppressed).toBeLessThan(ack.s.search(/WHERE singleton FOR (NO KEY )?UPDATE;/));
});

test("lazy_records fails closed (40001) outside READ COMMITTED; the same claim succeeds under READ COMMITTED", async () => {
  const f = await fixture();
  let payload: unknown;
  const raw = await batch([record({ record_id: "rr", session_id: "rr-s" })], { batch_id: "rr", account_id: await deliveryAccount(X) });
  const custody = await witness(f, X, raw);
  const capture = sqlLedger(async (sql, params) => {
    if (sql.includes("ingest_claim_buffered_batch")) { payload = params[0]; throw Object.assign(Error("captured"), { code: "XXCAP" }); }
    return f.run(sql, params as unknown[]);
  }, { acceptanceMode: "buffered", expectedGeneration: generation });
  await expect(deliverBatch({ ...f.deps, ledger: capture, custody } as never, X, raw)).rejects.toBeDefined();
  expect(typeof payload).toBe("string");
  for (const level of ["REPEATABLE READ", "SERIALIZABLE"]) {
    await f.db.exec(`BEGIN ISOLATION LEVEL ${level};SET LOCAL ROLE bulls96_session_ingest`);
    const e = await f.db.query("SELECT sessions.ingest_claim_buffered_batch($1::jsonb)", [payload]).then(() => null, (x: any) => x);
    await f.db.exec("ROLLBACK");
    expect(e?.code).toBe("40001");
    expect(String(e?.message)).toContain("lazy records require read committed");
  }
  expect((await f.run("SELECT sessions.ingest_claim_buffered_batch($1::jsonb) AS r", [payload]) as any).rows[0].r.kind).toBeDefined();
});

test("lock footprint: a 500-record claim holds exactly one origin lock and one fixed shared erase gate", async () => {
  const f = await fixture();
  let payload: unknown;
  const ids = Array.from({ length: 500 }, (_, i) => `fp-${i}`);
  const raw = await batch(ids.map((rid) => record({ record_id: rid, revision: 2, session_id: rid + "-s" })), { batch_id: "fp", account_id: await deliveryAccount(X) });
  const custody = await witness(f, X, raw);
  const capture = sqlLedger(async (sql, params) => {
    if (sql.includes("ingest_claim_buffered_batch")) { payload = params[0]; throw Object.assign(Error("captured"), { code: "XXCAP" }); }
    return f.run(sql, params as unknown[]);
  }, { acceptanceMode: "buffered", expectedGeneration: generation });
  await expect(deliverBatch({ ...f.deps, ledger: capture, custody } as never, X, raw)).rejects.toBeDefined();
  expect(typeof payload).toBe("string");
  const locks = "SELECT classid::text::bigint::text classid,objid::text::bigint::text objid,objsubid,mode,granted FROM pg_locks WHERE locktype='advisory' AND pid=pg_backend_pid()";
  const want = (await f.db.query<{ classid: string; objid: string }>("SELECT ((k>>32)&4294967295)::text classid,(k&4294967295)::text objid FROM(SELECT hashtextextended(jsonb_build_array('96bulls:lazy-native-origin-v1',$1::text,$2::text,$3::text)::text,0) k)s",
    [X.orgId, X.workspaceId, raw.origin_id])).rows[0]!;
  await f.db.exec("BEGIN;SET LOCAL ROLE bulls96_session_ingest");
  try {
    expect((await f.db.query(locks)).rows).toEqual([]);
    expect(((await f.db.query<{ r: any }>("SELECT sessions.ingest_claim_buffered_batch($1::jsonb) AS r", [payload])).rows[0]!.r).kind).toBe("claimed");
    const held = (await f.db.query(locks)).rows;
    expect(held).toHaveLength(2); // constant per transaction, never per record
    expect(held).toEqual(expect.arrayContaining([
      { ...want, objsubid: 1, mode: "ExclusiveLock", granted: true },
      { classid:"961996",objid:"50000",objsubid:2,mode:"ShareLock",granted:true },
    ]));
  } finally { await f.db.exec("ROLLBACK"); }
});

test("U4: a buffered claim never commits a non-suppressed receipt inside the claim transaction", async () => {
  const f = await fixture();
  // Staged path: at claim commit the batch is pending; only the separate ingest_commit_buffered_batch commits it (staged).
  const seen: unknown[] = [];
  expect((await deliver(f, X, "u4-staged", "u4a", { onClaim: async () => { seen.push(await batchRow(f, "u4-staged")); } })).outcome).toBe("accepted_buffered");
  expect(seen).toEqual([{ state: "pending", kind: null }]);
  expect(await batchRow(f, "u4-staged")).toEqual({ state: "committed", kind: "staged" });
  // Suppressed path (every candidate fenced -> nothing live): committed inside the claim, and only as 'suppressed'.
  seen.length = 0;
  await deliver(f, X, "u4-suppressed", "u4b", { fenced: true, onClaim: async () => { seen.push(await batchRow(f, "u4-suppressed")); } });
  expect(seen).toEqual([{ state: "committed", kind: "suppressed" }]);
  // Exhaustive over the fixture: no batch committed in a claim transaction carries a non-suppressed kind.
  expect((await f.db.query("SELECT count(*)::int n FROM sessions.ingest_batches WHERE publication_state='buffered' AND state='committed' AND buffer_receipt_kind NOT IN('suppressed','staged')")).rows[0]).toEqual({ n: 0 });
});

test("2c: the suppressed ACK path writes no tuple and allocates no seat (nothing for history tracking or retire-fence triggers to observe)", async () => {
  const f = await fixture();
  await deliver(f, X, "2c", "c2", { fenced: true });
  expect(await batchRow(f, "2c")).toEqual({ state: "committed", kind: "suppressed" });
  const row = (await f.db.query<{ r: unknown }>("SELECT to_jsonb(b) r FROM sessions.ingest_batches b WHERE batch_id='2c'")).rows[0]!.r;
  const writes = "SELECT coalesce(sum(n_tup_ins+n_tup_upd+n_tup_del),0)::int n FROM pg_stat_xact_user_tables";
  const seat = "SELECT last_seat::text s FROM sessions.ingest_receipt_seat_clock WHERE singleton";
  const before = (await f.db.query<{ s: string }>(seat)).rows[0]!.s;
  await f.db.exec("BEGIN");
  try {
    const w0 = (await f.db.query<{ n: number }>(writes)).rows[0]!.n;
    await f.db.query("SELECT sessions.ingest_lazy_ack_batch($1::jsonb)", [JSON.stringify(row)]);
    expect((await f.db.query<{ n: number }>(writes)).rows[0]!.n).toBe(w0);
    // Control: the same probe does observe a write inside the transaction.
    await f.db.exec("CREATE TEMP TABLE __probe(x int);INSERT INTO __probe VALUES(1)");
    expect((await f.db.query<{ n: number }>(writes)).rows[0]!.n).toBeGreaterThan(w0);
  } finally {
    await f.db.exec("ROLLBACK");
  }
  expect((await f.db.query<{ s: string }>(seat)).rows[0]!.s).toBe(before);
});

test("same key, owned: the second user gets P0W01 (403), never 23505/40001; exactly one slot and one custody row", async () => {
  const f = await fixture();
  const cur = await current(X, "owned");
  expect((await deliver(f, X, "own-x", "owned", { current: cur })).outcome).toBe("accepted_buffered");
  const y = await deliver(f, Y, "own-y", "owned", { current: cur }).then(() => null, (e: any) => e);
  expect(y?.status).toBe(403);
  expect(await count(f, "ingest_lazy_native_slots", "owned")).toBe(1);
  expect(await count(f, "ingest_custody_records", "owned")).toBe(1);
  expect((await f.db.query("SELECT current_owner FROM sessions.ingest_lazy_native_slots WHERE record_id='owned'")).rows[0]).toEqual({ current_owner: "claim-x" });
});

test("same key, source-absent: an initialized empty slot stays NULL-owned until a non-suppressed ACK; a second user CONTINUEs past it", async () => {
  const f = await fixture();
  // X's claim commits and initializes the slot with no owner; X's delivery stops before its staged ACK.
  const x = await deliver(f, X, "abs-x", "absent", { beforeReceipt: true }).then(() => null, (e: any) => e);
  expect(x?.stage).toBe("buffer_receipt");
  expect((await f.db.query("SELECT current_owner FROM sessions.ingest_lazy_native_slots WHERE record_id='absent'")).rows).toEqual([{ current_owner: null }]);
  // Y finds the NULL-owned slot and CONTINUEs (pre-043500 a concurrent Y could raise 23505 on the plain INSERT); its staged ACK owns it.
  expect((await deliver(f, Y, "abs-y", "absent")).outcome).toBe("accepted_buffered");
  expect(await count(f, "ingest_lazy_native_slots", "absent")).toBe(1);
  expect((await f.db.query("SELECT current_owner FROM sessions.ingest_lazy_native_slots WHERE record_id='absent'")).rows).toEqual([{ current_owner: "claim-y" }]);
});
