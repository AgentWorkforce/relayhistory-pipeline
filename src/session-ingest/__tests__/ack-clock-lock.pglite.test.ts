/**
 * 20261105053000_lazy_ack_clock_no_key_update, single-connection semantics (PGlite; every migration incl. 053000).
 * The staged ACK (sessions.ingest_lazy_ack_batch) now locks the singleton seat clock FOR NO KEY UPDATE instead of
 * FOR UPDATE, so a claim's FOR KEY SHARE read no longer queues behind it. The two-connection behaviour is proven on
 * real PostgreSQL in ack-clock-lock.postgres.test.ts; this file pins the installed body, the unchanged claim path and
 * that a full staged delivery still allocates a seat through the narrowed lock.
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

test("053000 installs the staged ACK with FOR NO KEY UPDATE on the seat clock and leaves the claim path on FOR KEY SHARE", async () => {
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

test("053000 also applies on the pending:false profile (043700 skipped) because it depends only on 043500", async () => {
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
