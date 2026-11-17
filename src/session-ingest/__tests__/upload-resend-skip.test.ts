/**
 * INGEST_ADMISSION_RESEND_SKIP (upload-admission.ts skipIfCurrent, ledger-s3.ts claimOrCurrent): a desktop re-sending
 * history it already uploaded, under new batch ids, is finished at promotion with zero canonical writes and the same
 * client-visible answer the full path's replay_fast gives; anything newer, deleted, reset or ambiguous falls through
 * to the unchanged hand-off and admission.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { zstdCompressSync, zstdDecompressSync } from "node:zlib";
import { objectKey, readAll, type ObjectStore } from "../../shared/vendor.ts";
import { admitMessage } from "../admission-consumer.ts";
import { journalKey, outcomeKey, receiptDir } from "../async-admission.ts";
import { ledgerClaimInput, type DeliverDeps } from "../deliver.ts";
import type { ClaimInput, Ledger } from "../ledger.ts";
import { fenceAuthorityKey, s3Ledger, s3LedgerKeys } from "../ledger-s3.ts";
import { promoteUpload, RESEND_SKIP_CODE, uploadAdmissionStatus, uploadDirectConfig, uploadStatusKey, type PromoteResult } from "../upload-admission.ts";
import { encodeUploadWitness, parseUploadKey, parseUploadWitnessKey, uploadKeyPrefix, uploadObjectKey, uploadObjectSha256Hex } from "../upload-inbox-contract.ts";
import type { HistoryExportRecord } from "../vendor/delivery/delivery-contracts.ts";
import { TENANT, batch, record, scratchStore } from "./support.ts";

const V = JSON.parse(readFileSync(join(import.meta.dir, "../../../docs/contracts/relay-upload-inbox-v1.vectors.json"), "utf8"));
const WITNESS_KEY = V.witness.testOnlyKey as string;
const KEY = parseUploadWitnessKey(WITNESS_KEY);
const ROOT = "96bulls";
const T0 = 1_800_000_000_000;
const CONFIG = uploadDirectConfig({ HISTORY_UPLOAD_DIRECT: "on" });

const cleanups: (() => void)[] = [];
afterEach(() => cleanups.splice(0).forEach((f) => f()));

const text = (b: Uint8Array) => new TextDecoder().decode(b);
const json = async (store: ObjectStore, key: string) => JSON.parse(text(await readAll((await store.getObject(objectKey(key))).body)));
async function listAll(store: ObjectStore, prefix: string): Promise<string[]> {
  const out: string[] = [];
  let cursor: string | undefined;
  do { const page = await store.list(objectKey(prefix), cursor); out.push(...page.items.map((i) => String(i.key))); cursor = page.nextCursor; } while (cursor);
  return out.sort();
}
/** Every canonical object (the per-org v1 tree) with its content hash: what "zero canonical writes" compares. */
async function canonical(store: ObjectStore): Promise<Record<string, string>> {
  const out: Record<string, string> = {};
  for (const k of await listAll(store, `${ROOT}/${TENANT.orgId}/v1`))
    out[k] = createHash("sha256").update(await readAll((await store.getObject(objectKey(k))).body)).digest("hex");
  return out;
}

function harness() {
  const s = scratchStore();
  cleanups.push(s.done);
  const store = s.store;
  const ledger = s3Ledger(store, ROOT, { acceptanceMode: "canonical", fenceAuthority: "required" });
  const probes: ClaimInput[] = [];
  const spied: Ledger = { ...ledger, claimIfCurrent: async (i) => { probes.push(i); return ledger.claimIfCurrent!(i); } };
  const deliver: DeliverDeps = { ledger: spied, acceptanceMode: "canonical", target: { store, keyRoot: ROOT } };
  const queued: string[] = [];
  const ready = Promise.all([
    store.put(objectKey(fenceAuthorityKey(ROOT)), new TextEncoder().encode(JSON.stringify({ v: 1, fenceBackfill: "complete", revisionBackfill: "complete", ownershipBackfill: "complete",
      controlMirror: "on", rearmSweeper: "live", revisionLayout: "4096", workerHttpAdmission: "off", writtenAtMs: T0 })), { contentType: "application/json" }),
    store.put(objectKey(journalKey(ROOT, TENANT)), new TextEncoder().encode(JSON.stringify({ v: 3, seq: 0, pending: [], ambiguous: [] })), { contentType: "application/json" }),
  ]);
  /** A desktop upload object (zstd JSON at its content-addressed key, the witness in its metadata). */
  async function put(batchId: string, records: HistoryExportRecord[]) {
    await ready;
    const b = await batch(records, { batch_id: batchId });
    const bytes = new Uint8Array(zstdCompressSync(Buffer.from(JSON.stringify({ protocolVersion: 1, batch: b }))));
    const key = uploadObjectKey(uploadKeyPrefix({ keyRoot: ROOT, ...TENANT }), { originId: "origin-1", batchId, objectSha256Hex: uploadObjectSha256Hex(bytes), witnessSeq: 0 });
    const witness = encodeUploadWitness(KEY, { v: 1, org: TENANT.orgId, ws: TENANT.workspaceId, user: TENANT.userId, seq: 0, exp: Math.floor(T0 / 1000) + 3600 });
    await store.put(objectKey(key), bytes, { metadata: { "relay-witness": witness } });
    return key;
  }
  /** Promotion (skip on or off), then the unchanged admission consumer for whatever it handed off. */
  async function run(key: string, skip: boolean): Promise<{ promoted: PromoteResult; admitted: { outcome: string; code: string }[] }> {
    const promoted = await promoteUpload(key, { config: CONFIG, deliver, witnessKey: WITNESS_KEY, enqueue: async (m) => void queued.push(m.key), nowMs: () => T0,
      ...(skip ? { resendSkip: (t, p) => spied.claimIfCurrent!(ledgerClaimInput(t, p)) } : {}) });
    const admitted: { outcome: string; code: string }[] = [];
    for (const k of queued.splice(0)) {
      const r = await admitMessage({ mode: "on", deliver, custodySource: "relayhistory", decompress: (b) => new Uint8Array(zstdDecompressSync(b)), nowMs: () => T0 },
        { v: 1, kind: "ingest-admission", key: k }, 1);
      if (r.action !== "ack") throw new Error(`admission ${r.action} ${r.code}`);
      admitted.push({ outcome: r.outcome, code: r.code });
    }
    return { promoted, admitted };
  }
  const status = async (key: string) => uploadAdmissionStatus({ store, keyRoot: ROOT }, CONFIG, TENANT, key);
  const outcomeOf = async (key: string) => {
    const st = await json(store, uploadStatusKey(parseUploadKey(key) as never));
    return json(store, outcomeKey(receiptDir(ROOT, TENANT, "origin-1", st.receiptId), "admitted"));
  };
  return { store, ledger, probes, run, put, status, outcomeOf };
}

const HISTORY = () => [record({ record_id: "r1", revision: 3 }), record({ record_id: "r2", session_id: "sess-2", revision: 2 })];
/** The client-visible answer, minus the per-upload key and receipt (they differ per batch id by construction). */
const visible = (s: Awaited<ReturnType<ReturnType<typeof harness>["status"]>>) => {
  if (!s.ok) return s;
  const { uploadKey: _k, receiptId, ...rest } = s.body;
  return { ...rest, receiptId: typeof receiptId === "string" && /^rhr_[0-9a-f]{64}$/.test(receiptId) };
};

describe("INGEST_ADMISSION_RESEND_SKIP", () => {
  test("a re-sent batch at equal or older revisions is finished at promotion: zero canonical writes, the full path's client-visible answer", async () => {
    const h = harness();
    const first = await h.run(await h.put("b-orig", HISTORY()), true);
    // New data is never skipped: the probe answered false and the full path stored it.
    expect(first.promoted).toEqual({ action: "ack", outcome: "handed_off" });
    expect(first.admitted).toEqual([{ outcome: "admitted", code: "accepted" }]);
    const stored = await canonical(h.store);
    expect(Object.keys(stored).length).toBeGreaterThan(0);

    // The full path for the same re-send (flag off): replay_fast, nothing canonical.
    const fullKey = await h.put("b-resend-full", HISTORY());
    const full = await h.run(fullKey, false);
    expect(full).toEqual({ promoted: { action: "ack", outcome: "handed_off" }, admitted: [{ outcome: "admitted", code: "replay_fast" }] });
    expect(await canonical(h.store)).toEqual(stored);

    // Skip on: equal revisions (r1 at 3) and older ones (r2 at 1 < 2), a new batch id.
    const probesBefore = h.probes.length;
    const skipKey = await h.put("b-resend-skip", [record({ record_id: "r1", revision: 3 }), record({ record_id: "r2", session_id: "sess-2", revision: 1 })]);
    const skipped = await h.run(skipKey, true);
    expect(skipped).toEqual({ promoted: { action: "ack", outcome: RESEND_SKIP_CODE }, admitted: [] });
    expect(h.probes.length).toBe(probesBefore + 1);
    expect(await canonical(h.store)).toEqual(stored);
    // Same client-visible answer as the full path; the code is server-side only.
    expect(visible(await h.status(skipKey))).toEqual(visible(await h.status(fullKey)));
    expect(visible(await h.status(skipKey))).toMatchObject({ state: "admitted", receiptId: true });
    expect(await h.outcomeOf(skipKey)).toMatchObject({ state: "admitted", code: RESEND_SKIP_CODE });
    expect(await h.outcomeOf(fullKey)).toMatchObject({ state: "admitted", code: "replay_fast" });
    // The ledger state replay_fast leaves: a committed batch record with no claim token, for both.
    const keys = s3LedgerKeys(ROOT, TENANT);
    const a = await json(h.store, keys.batch("origin-1", "b-resend-full"));
    const b = await json(h.store, keys.batch("origin-1", "b-resend-skip"));
    expect(b).toMatchObject({ state: "committed", claimToken: null });
    expect({ ...b, batchId: "x", digest: "x", receiptId: "x", epochs: Object.keys(b.epochs).sort() }).toEqual({ ...a, batchId: "x", digest: "x", receiptId: "x", epochs: Object.keys(a.epochs).sort() });
    // No admission-inbox content is left for the consumer, and the upload object is gone.
    expect(await h.store.head(objectKey(skipKey))).toBeUndefined();
  });

  test("a redelivery after a skip acks (already_promoted) and writes nothing; a replayed batch id falls through to the full path's replay", async () => {
    const h = harness();
    await h.run(await h.put("b-orig", HISTORY()), true);
    const stored = await canonical(h.store);
    const key = await h.put("b-resend", HISTORY());
    expect((await h.run(key, true)).promoted).toEqual({ action: "ack", outcome: RESEND_SKIP_CODE });
    // The same object written again (a desktop retry): the status + tombstone + outcome are found, nothing re-runs.
    expect(await h.put("b-resend", HISTORY())).toBe(key);
    expect(await h.run(key, true)).toEqual({ promoted: { action: "ack", outcome: "already_promoted" }, admitted: [] });
    expect(await canonical(h.store)).toEqual(stored);
    // A crash before the status write: the batch record exists, so the probe declines and the full path answers replay.
    await h.store.delete(objectKey(uploadStatusKey(parseUploadKey(key) as never)));
    await h.put("b-resend", HISTORY());
    expect(await h.run(key, true)).toEqual({ promoted: { action: "ack", outcome: "handed_off" }, admitted: [{ outcome: "admitted", code: "replay" }] });
    expect(visible(await h.status(key))).toMatchObject({ state: "admitted", receiptId: true });
    expect(await canonical(h.store)).toEqual(stored);
  });

  test("one newer record (a mixed batch) falls through and is stored; the probe wrote nothing", async () => {
    const h = harness();
    await h.run(await h.put("b-orig", HISTORY()), true);
    const stored = await canonical(h.store);
    const ledgerBefore = await listAll(h.store, s3LedgerKeys(ROOT, TENANT).dir);
    // Direct probe of the mixed batch: false, and not one object written (no batch record, owner stub or index).
    const { prepareDelivery } = await import("../vendor/delivery/prepare.ts");
    const mixed = [record({ record_id: "r1", revision: 3 }), record({ record_id: "r2", session_id: "sess-2", revision: 3 })];
    const prepared = await prepareDelivery(TENANT, await batch(mixed, { batch_id: "b-mixed" }));
    expect(await h.ledger.claimIfCurrent!(ledgerClaimInput(TENANT, prepared))).toBe(false);
    expect(await listAll(h.store, s3LedgerKeys(ROOT, TENANT).dir)).toEqual(ledgerBefore);
    // Through promotion: handed off, admitted by the full path, and r2's new revision is stored.
    const r = await h.run(await h.put("b-mixed", mixed), true);
    expect(r).toEqual({ promoted: { action: "ack", outcome: "handed_off" }, admitted: [{ outcome: "admitted", code: "accepted" }] });
    expect(await canonical(h.store)).not.toEqual(stored);
    expect((await json(h.store, s3LedgerKeys(ROOT, TENANT).revisions("origin-1", "r2"))).records.r2[0]).toBe(3);
    // A brand-new record id is new data too.
    const r3 = await h.run(await h.put("b-new", [record({ record_id: "r1", revision: 3 }), record({ record_id: "r3" })]), true);
    expect(r3.promoted).toEqual({ action: "ack", outcome: "handed_off" });
  });

  test("a session the user disabled falls through: the full path decides (acknowledged, not stored)", async () => {
    const h = harness();
    await h.run(await h.put("b-orig", HISTORY()), true);
    await h.ledger.setSessionUploads({ tenant: TENANT, originId: "origin-1", sessions: [["claude", "sess-2"]], enabled: false });
    const stored = await canonical(h.store);
    const r = await h.run(await h.put("b-resend", HISTORY()), true);
    expect(r.promoted).toEqual({ action: "ack", outcome: "handed_off" });
    expect(r.admitted).toEqual([{ outcome: "admitted", code: "replay_fast" }]);
    expect(await canonical(h.store)).toEqual(stored);
  });

  test("after a reset the reset sessions fall through: the full path decides (acknowledged, never stored again)", async () => {
    const h = harness();
    await h.run(await h.put("b-orig", HISTORY()), true);
    await h.ledger.resetUploadedSessions(TENANT, "reset-1");
    const r = await h.run(await h.put("b-resend", HISTORY()), true);
    expect(r.promoted).toEqual({ action: "ack", outcome: "handed_off" });
    expect(r.admitted).toEqual([{ outcome: "admitted", code: "replay_fast" }]);
  });

  test("a pending control or an unfinished reset falls through without writing", async () => {
    const h = harness();
    await h.run(await h.put("b-orig", HISTORY()), true);
    const keys = s3LedgerKeys(ROOT, TENANT);
    // A fence object to edit: a control on a session outside the batch (the batch stays current).
    await h.ledger.setSessionUploads({ tenant: TENANT, originId: "origin-1", sessions: [["claude", "sess-9"]], enabled: false });
    const fence = await json(h.store, keys.fence);
    const { prepareDelivery } = await import("../vendor/delivery/prepare.ts");
    const probe = async (batchId: string) => h.ledger.claimIfCurrent!(ledgerClaimInput(TENANT, await prepareDelivery(TENANT, await batch(HISTORY(), { batch_id: batchId }))));
    // Control: current with the fence as it is.
    expect(await probe("b-control")).toBe(true);
    for (const edit of [
      { ...fence, v: 2, pending: { "e:1:d": { kind: "selection", originId: "origin-1", sessions: [["claude", "sess-9"]] } } },
      { ...fence, resets: fence.resets + 1 },
    ]) {
      await h.store.put(objectKey(keys.fence), new TextEncoder().encode(JSON.stringify(edit)), { contentType: "application/json" });
      const before = await listAll(h.store, keys.dir);
      expect(await probe(`b-${before.length}`)).toBe(false);
      expect(await listAll(h.store, keys.dir)).toEqual(before);
    }
  });

  test("flag off: the probe is never called and promotion hands off exactly as before", async () => {
    const h = harness();
    await h.run(await h.put("b-orig", HISTORY()), false);
    const r = await h.run(await h.put("b-resend", HISTORY()), false);
    expect(r).toEqual({ promoted: { action: "ack", outcome: "handed_off" }, admitted: [{ outcome: "admitted", code: "replay_fast" }] });
    expect(h.probes).toEqual([]);
  });
});

describe("INGEST_ADMISSION_RESEND_SKIP config", () => {
  test("off by default; on only with the S3 ledger and direct upload; anything else refuses startup", async () => {
    const { selfhostAdmissionResendSkip } = await import("../selfhost-config.ts");
    const upload = { config: CONFIG, witnessKey: undefined, scanBatch: 1, promoteConcurrency: 1 };
    for (const raw of [undefined, "", "off"]) expect(selfhostAdmissionResendSkip({ INGEST_ADMISSION_RESEND_SKIP: raw }, "s3", upload)).toBeUndefined();
    expect(selfhostAdmissionResendSkip({ INGEST_ADMISSION_RESEND_SKIP: "on" }, "s3", upload)).toBe(true);
    for (const raw of ["ON", "true", "1", " on"]) expect(() => selfhostAdmissionResendSkip({ INGEST_ADMISSION_RESEND_SKIP: raw }, "s3", upload)).toThrow("must be off or on");
    expect(() => selfhostAdmissionResendSkip({ INGEST_ADMISSION_RESEND_SKIP: "on" }, undefined, upload)).toThrow("needs INGEST_LEDGER=s3");
    expect(() => selfhostAdmissionResendSkip({ INGEST_ADMISSION_RESEND_SKIP: "on" }, "s3", undefined)).toThrow("needs HISTORY_UPLOAD_DIRECT");
  });
});
