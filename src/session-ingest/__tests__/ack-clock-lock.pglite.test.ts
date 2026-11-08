/**
 * 20261105043800_lazy_ack_clock_no_key_update, single-connection semantics (PGlite; every migration incl. 043800).
 * The staged ACK (sessions.ingest_lazy_ack_batch) now locks the singleton seat clock FOR NO KEY UPDATE instead of
 * FOR UPDATE, so a claim's FOR KEY SHARE read no longer queues behind it. The two-connection behaviour is proven on
 * real PostgreSQL in ack-clock-lock.postgres.test.ts; this file pins the installed body, the unchanged claim path and
 * that a full staged delivery still allocates a seat through the narrowed lock. Since the reviewer P1 the ACK also takes
 * the claim's bounded origin advisory lock after the clock and before any slot (fence -> clock -> origin -> slot); the
 * reversed-two-slot schedule is proven on real PostgreSQL (D1 in claim-clock-lock.postgres.test.ts), this file pins the
 * body order and that a staged commit holds exactly that one advisory lock.
 */
import { afterEach, expect, setDefaultTimeout, test } from "bun:test";
import { deliverBatch } from "../deliver.ts";
import { sqlLedger } from "../ledger-sql.ts";
import { deliveryAccount, prepareDelivery } from "../vendor/delivery/prepare.ts";
import { TENANT, batch, record } from "./support.ts";
import { authorityFixture, epoch, generation, identity } from "./privacy-authority-support.ts";

setDefaultTimeout(120_000);
const open: Array<() => unknown> = [];
afterEach(async () => {
  for (const c of open.splice(0).reverse()) await c();
});
type Fixture = Awaited<ReturnType<typeof authorityFixture>>;

async function fixture(opts: Parameters<typeof authorityFixture>[0] = {}) {
  const f = await authorityFixture(opts);
  open.push(f.close);
  await f.seal();
  return f;
}
const def = async (f: Fixture, sig: string) =>
  (await f.db.query<{ d: string }>("SELECT pg_get_functiondef($1::regprocedure) d", [sig])).rows[0]!.d;
const secdef = async (f: Fixture, sig: string) =>
  (await f.db.query<{ s: boolean }>("SELECT prosecdef s FROM pg_proc WHERE oid=$1::regprocedure", [sig])).rows[0]!.s;
const ACK = "sessions.ingest_lazy_ack_batch(jsonb)", CLAIM = "sessions.ingest_lazy_records(jsonb)";

test("043800 installs the staged ACK with FOR NO KEY UPDATE on the seat clock and leaves the claim path on FOR KEY SHARE", async () => {
  const f = await fixture();
  const ack = await def(f, ACK);
  expect(ack).toContain("FROM sessions.ingest_receipt_seat_clock WHERE singleton FOR NO KEY UPDATE;v_last:=c.last_seat;");
  expect(ack).not.toMatch(/ingest_receipt_seat_clock WHERE singleton FOR UPDATE/);
  // The suppressed (no-seat) path was already KEY SHARE in 043500 and is unchanged.
  expect(ack).toContain("FOR KEY SHARE");
  expect(await secdef(f, ACK)).toBe(false);
  const claim = await def(f, CLAIM);
  expect(claim).toContain("WHERE singleton FOR KEY SHARE;");
  expect(claim).not.toMatch(/ingest_receipt_seat_clock WHERE singleton FOR (NO KEY )?UPDATE/);
});

test("043800 also applies on the pending:false profile (043700 skipped) because it depends only on 043500", async () => {
  const f = await fixture({ pending: false });
  expect(await def(f, ACK)).toContain("WHERE singleton FOR NO KEY UPDATE;");
});

test("a full staged delivery still allocates exactly one seat through the narrowed clock lock", async () => {
  const f = await fixture();
  const lastSeat = async () => (await f.db.query<{ s: string }>("SELECT last_seat::text s FROM sessions.ingest_receipt_seat_clock WHERE singleton")).rows[0]!.s;
  const before = await lastSeat();
  const raw = await batch([record({ record_id: "ack-r1", revision: 2, session_id: "ack-r1-s" })], { batch_id: "ack-b1", account_id: await deliveryAccount(TENANT) });
  const p = await prepareDelivery(TENANT, raw);
  const w = { version: 1, issuer: "relayhistory-cloud", sourceIdentitySha256: identity, controlEpoch: epoch, privacyBaselineSha256: "a".repeat(64), tenant: TENANT,
    originId: p.originId, batchId: p.batchId, batchDigest: p.batchDigest, sourceControlVersion: "0", pending: false, pendingControl: null, privacyVersion: "0",
    originalRecordCount: raw.records.length, candidateCount: p.records.length,
    records: p.records.map((r, i) => ({ preparedOrd: i, processingRank: i, candidateDigest: r.digest, recordId: r.record_id, revision: String(r.revision),
      revisionId: r.revision_id, candidateFenced: false, currentFenced: false, current: null })),
    privacy: { complete: true, deletions: [], resets: [] } };
  const sourceDigest = (await f.db.query<{ d: string }>("SELECT encode(sha256(convert_to($1::jsonb::text,'UTF8')),'hex') AS d", [JSON.stringify(w)])).rows[0]!.d;
  const ledger = sqlLedger(f.run, { acceptanceMode: "buffered", expectedGeneration: generation });
  const out = await deliverBatch({ ...f.deps, ledger, custody: { witness: w, sourceDigest } } as never, TENANT, raw);
  expect(out.outcome).toBe("accepted_buffered");
  expect(BigInt(await lastSeat())).toBe(BigInt(before) + 1n);
});

const ORIGIN_LINE = "PERFORM pg_advisory_xact_lock(hashtextextended(jsonb_build_array('96bulls:lazy-native-origin-v1',p->>'org_id',p->>'workspace_id',p->>'origin_id')::text,0));";

test("043800 orders the staged ACK fence -> clock -> origin advisory lock -> stage row -> native slot", async () => {
  const f = await fixture();
  const ack = (await f.db.query<{ s: string }>("SELECT prosrc s FROM pg_proc WHERE oid=$1::regprocedure", [ACK])).rows[0]!.s;
  const at = (needle: string) => { const i = ack.indexOf(needle); expect(i).toBeGreaterThan(-1); return i; };
  expect(ack.split("pg_advisory_xact_lock(").length - 1).toBe(1); // exactly one advisory lock, single origin per call
  expect(at("ingest_lock_fences(")).toBeLessThan(at("WHERE singleton FOR NO KEY UPDATE;"));
  expect(at("WHERE singleton FOR NO KEY UPDATE;")).toBeLessThan(at(ORIGIN_LINE));
  expect(at(ORIGIN_LINE)).toBeLessThan(at("FOR UPDATE OF i;"));
  expect(at(ORIGIN_LINE)).toBeLessThan(at("FROM sessions.ingest_lazy_native_slots WHERE key_sha256=v_key FOR UPDATE;"));
  // The suppressed path returns above the clock lock, so it never takes the origin lock (the claim already holds it).
  expect(at("FOR KEY SHARE;RETURN;")).toBeLessThan(at(ORIGIN_LINE));
  // Same key construction as the claim: same version string, same argument order, same seed.
  const claim = (await f.db.query<{ s: string }>("SELECT prosrc s FROM pg_proc WHERE oid=$1::regprocedure", [CLAIM])).rows[0]!.s;
  expect(claim).toContain("PERFORM pg_advisory_xact_lock(hashtextextended(jsonb_build_array('96bulls:lazy-native-origin-v1',v_org,v_ws,v_origin)::text,0));");
});

test("a staged commit holds exactly ONE advisory lock, the claim's (org, ws, origin) key", async () => {
  const f = await fixture();
  const raw = await batch(["ack-o1", "ack-o2"].map((rid) => record({ record_id: rid, revision: 2, session_id: rid + "-s" })), { batch_id: "ack-o", account_id: await deliveryAccount(TENANT) });
  const p = await prepareDelivery(TENANT, raw);
  const w = { version: 1, issuer: "relayhistory-cloud", sourceIdentitySha256: identity, controlEpoch: epoch, privacyBaselineSha256: "a".repeat(64), tenant: TENANT,
    originId: p.originId, batchId: p.batchId, batchDigest: p.batchDigest, sourceControlVersion: "0", pending: false, pendingControl: null, privacyVersion: "0",
    originalRecordCount: raw.records.length, candidateCount: p.records.length,
    records: p.records.map((r, i) => ({ preparedOrd: i, processingRank: i, candidateDigest: r.digest, recordId: r.record_id, revision: String(r.revision),
      revisionId: r.revision_id, candidateFenced: false, currentFenced: false, current: null })),
    privacy: { complete: true, deletions: [], resets: [] } };
  const sourceDigest = (await f.db.query<{ d: string }>("SELECT encode(sha256(convert_to($1::jsonb::text,'UTF8')),'hex') AS d", [JSON.stringify(w)])).rows[0]!.d;
  let payload: unknown;
  const capture = sqlLedger(async (sql, params) => {
    if (sql.includes("ingest_commit_buffered_batch")) { payload = params[0]; throw Object.assign(Error("captured"), { code: "XXCAP" }); }
    return f.run(sql, params as unknown[]);
  }, { acceptanceMode: "buffered", expectedGeneration: generation });
  await expect(deliverBatch({ ...f.deps, ledger: capture, custody: { witness: w, sourceDigest } } as never, TENANT, raw)).rejects.toBeDefined();
  expect(typeof payload).toBe("string");
  const locks = "SELECT classid::text::bigint::text classid,objid::text::bigint::text objid,objsubid,mode,granted FROM pg_locks WHERE locktype='advisory' AND pid=pg_backend_pid()";
  const want = (await f.db.query<{ classid: string; objid: string }>("SELECT ((k>>32)&4294967295)::text classid,(k&4294967295)::text objid FROM(SELECT hashtextextended(jsonb_build_array('96bulls:lazy-native-origin-v1',$1::text,$2::text,$3::text)::text,0) k)s",
    [TENANT.orgId, TENANT.workspaceId, raw.origin_id])).rows[0]!;
  await f.db.exec("BEGIN;SET LOCAL ROLE bulls96_session_ingest");
  try {
    expect((await f.db.query(locks)).rows).toEqual([]);
    expect(((await f.db.query<{ r: any }>("SELECT sessions.ingest_commit_buffered_batch($1::jsonb) AS r", [payload])).rows[0]!.r).kind).toBe("committed");
    expect((await f.db.query(locks)).rows).toEqual([{ ...want, objsubid: 1, mode: "ExclusiveLock", granted: true }]);
  } finally { await f.db.exec("ROLLBACK"); }
});
