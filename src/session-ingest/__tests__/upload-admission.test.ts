/**
 * HISTORY_UPLOAD_DIRECT consumer side (upload-admission.ts, upload-admission-lambda.ts): promoteUpload over
 * the contract vectors and every §6 negative, idempotent redelivery (the tombstoned-attempt case included),
 * the hand-off into the unchanged admission consumer, the Lambda batch contract, the §5 status read
 * (GET /v1/delivery/admission?uploadKey=) and the flag parsing. No database: MemoryLedger + scratch fs store.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { zstdCompressSync, zstdDecompressSync } from "node:zlib";
import { ObjectStoreError, objectKey, readAll, type ObjectStore } from "../../shared/vendor.ts";
import { bindingIdentity, createIngestApp, encodeIdentity, IDENTITY_HEADER, SYNC_SCOPE } from "../app.ts";
import { admitMessage } from "../admission-consumer.ts";
import { prepareForAdmission, TenantNotASegment } from "../admission-core.ts";
import { INBOX_MIN_BYTES, journalKey, outcomeKey, receiptDir, type AdmissionMessage } from "../async-admission.ts";
import { DeliveryError } from "../vendor/delivery/delivery-contracts.ts";
import { createUploadAdmissionHandler, notificationKeys, type UploadAdmissionRuntimeFactory } from "../upload-admission-lambda.ts";
import {
  contractCode, promoteUpload, uploadAdmissionStatus, uploadDirectConfig, uploadStatusKey, UPLOAD_DIRECT_OFF, UPLOAD_PENDING_RETRY_AFTER_SECONDS,
  type PromoteUploadDeps, type UploadDirectConfig,
} from "../upload-admission.ts";
import {
  encodeUploadWitness, parseUploadAdmissionStatus, parseUploadKey, parseUploadWitnessKey, UPLOAD_MAX_OBJECT_BYTES, UPLOAD_REFUSAL_CODES, uploadKeyPrefix,
  uploadObjectKey, uploadObjectSha256Hex, type UploadKeyParts,
} from "../upload-inbox-contract.ts";
import { MemoryLedger, TENANT, batch, record, scratchStore } from "./support.ts";

const V = JSON.parse(readFileSync(join(import.meta.dir, "../../../docs/contracts/relay-upload-inbox-v1.vectors.json"), "utf8"));
const WITNESS_KEY = V.witness.testOnlyKey as string;
const KEY = parseUploadWitnessKey(WITNESS_KEY);
const VNOW = (V.witness.verifiedAtSeconds as number) * 1000;
const VTENANT = { orgId: V.inputs.orgId as string, workspaceId: V.inputs.workspaceId as string, userId: V.inputs.userId as string };
const ROOT = "96bulls";
const ON: UploadDirectConfig = uploadDirectConfig({ HISTORY_UPLOAD_DIRECT: "on" });
const T0 = 1_800_000_000_000;
const EXP = Math.floor(T0 / 1000) + 3600;
const zstd = (b: Uint8Array) => new Uint8Array(zstdDecompressSync(b));
const pack = (v: unknown) => new Uint8Array(zstdCompressSync(Buffer.from(typeof v === "string" ? v : JSON.stringify(v))));

const cleanups: (() => void)[] = [];
afterEach(() => cleanups.splice(0).forEach((f) => f()));

/** The scratch store, counting reads and with injectable faults (a transient store error). */
function counted() {
  const s = scratchStore();
  cleanups.push(s.done);
  const inner = s.store;
  const reads: string[] = [];
  const faults: { put?: (k: string) => boolean; get?: (k: string) => boolean; head?: (k: string) => boolean; del?: (k: string) => boolean } = {};
  const fail = (k: string) => { throw new ObjectStoreError("transient", "fs", `injected ${k.length}`); };
  const store: ObjectStore = {
    backend: inner.backend,
    head: async (k) => { reads.push(String(k)); return faults.head?.(String(k)) ? fail(String(k)) : inner.head(k); },
    get: (k, r) => { reads.push(String(k)); return inner.get(k, r); },
    getObject: async (k, r) => { reads.push(String(k)); return faults.get?.(String(k)) ? fail(String(k)) : inner.getObject(k, r); },
    list: async (p, c) => { reads.push(String(p)); return inner.list(p, c); },
    presignGet: (k, t) => inner.presignGet(k, t),
    put: async (k, b, o) => (faults.put?.(String(k)) ? fail(String(k)) : inner.put(k, b, o)),
    delete: async (k) => (faults.del?.(String(k)) ? fail(String(k)) : inner.delete(k)),
  };
  return { store, reads, faults };
}

const exists = async (store: ObjectStore, key: string) => (await store.head(objectKey(key))) !== undefined;
async function json(store: ObjectStore, key: string) {
  return JSON.parse(new TextDecoder().decode(await readAll((await store.getObject(objectKey(key))).body)));
}
async function setJournal(store: ObjectStore, keyRoot: string, tenant: typeof TENANT, seq: number) {
  await store.put(objectKey(journalKey(keyRoot, tenant)), new TextEncoder().encode(JSON.stringify({ v: 3, seq, pending: [], ambiguous: [] })), { contentType: "application/json" });
}

interface UploadOpts {
  tenant?: typeof TENANT; keyRoot?: string; seq?: number; exp?: number; witness?: string | null; bytes?: Uint8Array; body?: unknown;
  originId?: string; batchId?: string; keyOrigin?: string; keyBatch?: string; digestOf?: Uint8Array; witnessTenant?: typeof TENANT;
}
/** A desktop upload: the object (zstd JSON delivery request) at its content-addressed key, witness in metadata. */
async function upload(store: ObjectStore, o: UploadOpts = {}) {
  const tenant = o.tenant ?? TENANT, keyRoot = o.keyRoot ?? ROOT, seq = o.seq ?? 0;
  const originId = o.originId ?? "origin-1", batchId = o.batchId ?? "b-1";
  const b = await batch([record({ record_id: "r1", origin_id: originId })], { origin_id: originId, batch_id: batchId });
  const bytes = o.bytes ?? pack(o.body ?? { protocolVersion: 1, batch: b });
  const key = uploadObjectKey(uploadKeyPrefix({ keyRoot, ...tenant }), {
    originId: o.keyOrigin ?? originId, batchId: o.keyBatch ?? batchId, objectSha256Hex: uploadObjectSha256Hex(o.digestOf ?? bytes), witnessSeq: seq,
  });
  const wt = o.witnessTenant ?? tenant;
  const witness = o.witness === undefined ? encodeUploadWitness(KEY, { v: 1, org: wt.orgId, ws: wt.workspaceId, user: wt.userId, seq, exp: o.exp ?? EXP }) : o.witness;
  await store.put(objectKey(key), bytes, witness === null ? {} : { metadata: { "relay-witness": witness } });
  return { key, bytes, batch: b };
}

function deps(store: ObjectStore, over: Partial<PromoteUploadDeps> = {}, keyRoot = ROOT) {
  const sent: AdmissionMessage[] = [];
  const d: PromoteUploadDeps = {
    config: ON, witnessKey: WITNESS_KEY, nowMs: () => T0, nonce: () => "0123456789abcdef",
    deliver: { acceptanceMode: "buffered", target: { store, keyRoot } },
    enqueue: async (m) => { sent.push(m); },
    ...over,
  };
  return { d, sent };
}
const statusOf = async (store: ObjectStore, key: string) => json(store, uploadStatusKey(parseUploadKey(key) as UploadKeyParts));

describe("promoteUpload: the contract vectors", () => {
  test("the vector object round-trips: key parse, witness, digest, zstd, account, origin, b=; handed off at the witnessed seq", async () => {
    const { store } = counted();
    const key = V.objectKey as string;
    expect(parseUploadKey(key)).toEqual(V.parsedKey);
    await store.put(objectKey(key), new Uint8Array(Buffer.from(V.object.objectBytesBase64, "base64")), { metadata: { "relay-witness": V.witness.token } });
    // The witness seq (7) must not be ahead of the user's journal.
    expect(journalKey(V.inputs.keyRoot, VTENANT)).toBe(V.userSegment.journalKey);
    await setJournal(store, V.inputs.keyRoot, VTENANT, 7);
    const { d, sent } = deps(store, { nowMs: () => VNOW }, V.inputs.keyRoot);
    expect(await promoteUpload(key, d)).toEqual({ action: "ack", outcome: "handed_off" });
    expect(sent).toHaveLength(1);
    expect(sent[0]!.key).toMatch(/\/r=[0-9a-f]{64}\/a=\d{15}-0123456789abcdef\.ndjson\.zst$/);
    expect((await store.head(objectKey(sent[0]!.key)))!.size).toBeGreaterThanOrEqual(INBOX_MIN_BYTES);
    expect(await exists(store, key)).toBe(false);
    const status = await statusOf(store, key);
    expect(status).toMatchObject({ v: 1, kind: "upload-admission-status", state: "handed_off", originId: V.inputs.originId, inboxKey: sent[0]!.key, receivedAtMs: VNOW });
    // The status object sits in the journal's user dir, never under the desktop-writable prefix.
    expect(uploadStatusKey(parseUploadKey(key) as UploadKeyParts).startsWith(V.userSegment.journalKey.replace(/privacy-journal\.json$/, "u="))).toBe(true);
  });

  test("the same vector a second later than exp is upload_witness_expired, recorded and removed", async () => {
    const { store } = counted();
    const key = V.objectKey as string;
    await store.put(objectKey(key), new Uint8Array(Buffer.from(V.object.objectBytesBase64, "base64")), { metadata: { "relay-witness": V.witness.token } });
    await setJournal(store, V.inputs.keyRoot, VTENANT, 7);
    const { d, sent } = deps(store, { nowMs: () => V.witness.payload.exp * 1000 }, V.inputs.keyRoot);
    expect(await promoteUpload(key, d)).toEqual({ action: "ack", outcome: "refused", code: "upload_witness_expired" });
    expect(sent).toHaveLength(0);
    expect(await exists(store, key)).toBe(false);
    expect(await statusOf(store, key)).toMatchObject({ state: "refused", code: "upload_witness_expired" });
  });
});

describe("promoteUpload: retryable conditions are never refusals", () => {
  test("flag off: nothing is read, nothing written", async () => {
    const { store, reads } = counted();
    const { key } = await upload(store);
    reads.length = 0;
    const { d, sent } = deps(store, { config: UPLOAD_DIRECT_OFF });
    expect(await promoteUpload(key, d)).toEqual({ action: "retry", code: "history_upload_direct_off" });
    expect(reads).toEqual([]);
    expect(sent).toEqual([]);
    expect(await exists(store, key)).toBe(true);
  });

  test("a tenant the allowlist excludes waits (retry), unread; an allowlisted org or workspace is promoted", async () => {
    const { store, reads } = counted();
    const { key } = await upload(store);
    reads.length = 0;
    const other = uploadDirectConfig({ HISTORY_UPLOAD_DIRECT: "allowlist", HISTORY_UPLOAD_DIRECT_ALLOWLIST: "org:org-b,ws:ws-b" });
    expect(await promoteUpload(key, deps(store, { config: other }).d)).toEqual({ action: "retry", code: "history_upload_direct_excluded" });
    expect(reads).toEqual([]);
    const byWs = uploadDirectConfig({ HISTORY_UPLOAD_DIRECT: "allowlist", HISTORY_UPLOAD_DIRECT_ALLOWLIST: "org:org-b, ws:ws-a" });
    expect(await promoteUpload(key, deps(store, { config: byWs }).d)).toMatchObject({ action: "ack", outcome: "handed_off" });
  });

  test("missing or malformed UPLOAD_WITNESS_KEY: retry, never a refusal, never a default key", async () => {
    const { store } = counted();
    const { key } = await upload(store);
    for (const [witnessKey, code] of [[undefined, "upload_witness_key_missing"], ["", "upload_witness_key_missing"], ["  ", "upload_witness_key_missing"], ["short", "upload_witness_key_invalid"], [WITNESS_KEY + "=", "upload_witness_key_invalid"]] as const) {
      const { d, sent } = deps(store, { witnessKey });
      expect(await promoteUpload(key, d)).toEqual({ action: "retry", code });
      expect(sent).toEqual([]);
    }
    expect(await exists(store, key)).toBe(true);
    expect(await exists(store, uploadStatusKey(parseUploadKey(key) as UploadKeyParts))).toBe(false);
  });

  test("a key that is not an upload key of this key root is invalid: nothing read or written", async () => {
    const { store, reads } = counted();
    const { key } = await upload(store);
    reads.length = 0;
    for (const bad of ["", "96bulls/.admission-inbox/x", key.replace(/\.json\.zst$/, ".json"), key.replace("/v1/", "/v2/")])
      expect(await promoteUpload(bad, deps(store).d)).toEqual({ action: "invalid", code: "upload_key_invalid" });
    expect(await promoteUpload(key, deps(store, {}, "otherroot").d)).toEqual({ action: "invalid", code: "upload_key_invalid" });
    expect(reads).toEqual([]);
  });

  test("store, journal and queue faults are retries; the upload stays", async () => {
    const c = counted();
    const { key } = await upload(c.store);
    c.faults.get = (k) => k === key;
    expect(await promoteUpload(key, deps(c.store).d)).toMatchObject({ action: "retry", code: "upload_object_unavailable" });
    c.faults.get = (k) => k.endsWith("privacy-journal.json");
    expect(await promoteUpload(key, deps(c.store).d)).toMatchObject({ action: "retry", code: "privacy_journal_unavailable" });
    c.faults.get = (k) => k.includes("/u=") && k.endsWith(".json") && k.includes(".admission-inbox");
    expect(await promoteUpload(key, deps(c.store).d)).toMatchObject({ action: "retry", code: "upload_status_unavailable" });
    c.faults.get = undefined;
    // A malformed status object is a store fault, never a verdict.
    await c.store.put(objectKey(uploadStatusKey(parseUploadKey(key) as UploadKeyParts)), new TextEncoder().encode('{"v":1}'), { contentType: "application/json" });
    expect(await promoteUpload(key, deps(c.store).d)).toMatchObject({ action: "retry", code: "upload_status_unavailable" });
    expect(await exists(c.store, key)).toBe(true);
  });

  test("a journal with a pending-but-unreadable shape is a retry; a seq ahead of the journal is upload_witness_seq_mismatch", async () => {
    const { store } = counted();
    await store.put(objectKey(journalKey(ROOT, TENANT)), new TextEncoder().encode("{}"), { contentType: "application/json" });
    const { key } = await upload(store, { seq: 1 });
    expect(await promoteUpload(key, deps(store).d)).toMatchObject({ action: "retry", code: "privacy_journal_unavailable" });
    await setJournal(store, ROOT, TENANT, 0);
    expect(await promoteUpload(key, deps(store).d)).toEqual({ action: "ack", outcome: "refused", code: "upload_witness_seq_mismatch" });
  });
});

describe("promoteUpload: §6 refusals are recorded, the upload removed, nothing handed off", () => {
  const cases: [string, (s: ObjectStore) => Promise<{ key: string }>, string][] = [
    ["wrong tenant (witness user differs from the key's u=)", (s) => upload(s, { witnessTenant: { ...TENANT, userId: "user-2" } }), "upload_tenant_mismatch"],
    ["wrong tenant (witness org differs)", (s) => upload(s, { witnessTenant: { ...TENANT, orgId: "org-b" } }), "upload_tenant_mismatch"],
    ["no witness metadata", (s) => upload(s, { witness: null }), "upload_witness_invalid"],
    ["a witness under another key", (s) => upload(s, { witness: encodeUploadWitness(new Uint8Array(32).fill(9), { v: 1, org: "org-a", ws: "ws-a", user: "user-1", seq: 0, exp: EXP }) }), "upload_witness_invalid"],
    ["a tampered witness", async (s) => { const w = encodeUploadWitness(KEY, { v: 1, org: "org-a", ws: "ws-a", user: "user-1", seq: 0, exp: EXP }); return upload(s, { witness: w.slice(0, -2) + (w.endsWith("A") ? "BB" : "AA") }); }, "upload_witness_invalid"],
    ["an expired witness", (s) => upload(s, { exp: Math.floor(T0 / 1000) }), "upload_witness_expired"],
    ["checksum mismatch (d= is not the stored bytes' sha256)", (s) => upload(s, { digestOf: new Uint8Array([1, 2, 3]) }), "upload_digest_mismatch"],
    ["wrong encoding (plain JSON, not zstd)", (s) => upload(s, { bytes: new TextEncoder().encode('{"protocolVersion":1}') }), "unsupported_encoding"],
    ["wrong encoding (zstd magic, broken frame)", (s) => upload(s, { bytes: new Uint8Array([0x28, 0xb5, 0x2f, 0xfd, 1, 2, 3, 4, 5]) }), "unsupported_encoding"],
    ["decoded over MAX_DELIVERY_BYTES (zstd bomb)", (s) => upload(s, { bytes: new Uint8Array(zstdCompressSync(Buffer.alloc(UPLOAD_MAX_OBJECT_BYTES + 1, 0x20))) }), "delivery_too_large"],
    ["not JSON", (s) => upload(s, { bytes: pack("not json") }), "invalid_delivery"],
    ["not a delivery request", (s) => upload(s, { body: { hello: 1 } }), "invalid_delivery"],
    ["protocolVersion 2", async (s) => upload(s, { body: { protocolVersion: 2, batch: await batch([record({ record_id: "r1" })], { batch_id: "b-1" }) } }), "unsupported_protocol"],
    ["account of another workspace", async (s) => upload(s, { body: { protocolVersion: 1, batch: { ...(await batch([record({ record_id: "r1" })], { batch_id: "b-1" })), account_id: "relayhistory:" + "0".repeat(64) } } }), "delivery_account_mismatch"],
    ["batch origin differs from o=", (s) => upload(s, { keyOrigin: "origin-2" }), "upload_origin_mismatch"],
    ["batch id differs from b=", (s) => upload(s, { keyBatch: "b-other" }), "upload_key_invalid"],
  ];
  for (const [name, make, code] of cases) {
    test(name, async () => {
      const { store } = counted();
      const { key } = await make(store);
      const { d, sent } = deps(store);
      expect(await promoteUpload(key, d)).toEqual({ action: "ack", outcome: "refused", code: code as never });
      expect(sent).toEqual([]);
      expect(await exists(store, key)).toBe(false);
      const status = await statusOf(store, key);
      expect(status).toEqual({ v: 1, kind: "upload-admission-status", state: "refused", code, receivedAtMs: T0, decidedAtMs: T0 });
      expect(UPLOAD_REFUSAL_CODES).toContain(status.code);
      // Nothing reached the admission inbox beyond the status object.
      const listed = await store.list(objectKey(`${ROOT}/.admission-inbox`));
      expect(listed.items.filter((i) => String(i.key).endsWith(".ndjson.zst"))).toEqual([]);
    });
  }

  test("oversize: a stored object over UPLOAD_MAX_OBJECT_BYTES is refused by size before its body is read", async () => {
    const { store } = counted();
    const big = new Uint8Array(UPLOAD_MAX_OBJECT_BYTES + 1);
    big.set([0x28, 0xb5, 0x2f, 0xfd]);
    const { key } = await upload(store, { bytes: big });
    expect(await promoteUpload(key, deps(store).d)).toEqual({ action: "ack", outcome: "refused", code: "delivery_too_large" });
    expect(await exists(store, key)).toBe(false);
  });

  test("an unknown key (no object, no status) acks as gone and writes nothing", async () => {
    const { store } = counted();
    const { key } = await upload(store);
    await store.delete(objectKey(key));
    expect(await promoteUpload(key, deps(store).d)).toEqual({ action: "ack", outcome: "gone" });
    expect(await exists(store, uploadStatusKey(parseUploadKey(key) as UploadKeyParts))).toBe(false);
  });

  test("a refusal whose status write fails is a retry and the upload stays", async () => {
    const c = counted();
    const { key } = await upload(c.store, { witness: null });
    c.faults.put = (k) => k.includes("/u=") && k.includes(".admission-inbox");
    expect(await promoteUpload(key, deps(c.store).d)).toMatchObject({ action: "retry", code: "upload_status_unavailable" });
    expect(await exists(c.store, key)).toBe(true);
  });
});

describe("promoteUpload: idempotent redelivery", () => {
  test("after a hand-off: the inbox is re-notified once more, never written twice; after deletion: already_promoted", async () => {
    const c = counted();
    const { key } = await upload(c.store);
    // The delete fails the first time: the hand-off landed, so a redelivery must only finish the removal.
    c.faults.del = (k) => k === key;
    const first = deps(c.store);
    expect(await promoteUpload(key, first.d)).toMatchObject({ action: "retry", code: "upload_delete_unavailable" });
    expect(first.sent).toHaveLength(1);
    c.faults.del = undefined;
    const again = deps(c.store, { nonce: () => "fedcba9876543210" });
    expect(await promoteUpload(key, again.d)).toEqual({ action: "ack", outcome: "already_promoted" });
    expect(again.sent).toEqual([first.sent[0]!]);
    expect(await exists(c.store, key)).toBe(false);
    const inboxes = (await c.store.list(objectKey(`${ROOT}/.admission-inbox`))).items.filter((i) => String(i.key).endsWith(".ndjson.zst"));
    expect(inboxes).toHaveLength(1);
    // A third delivery (S3 at-least-once) after the object is gone.
    expect(await promoteUpload(key, deps(c.store).d)).toEqual({ action: "ack", outcome: "already_promoted" });
  });

  test("after a refusal: already_promoted with the recorded code, no second decision", async () => {
    const c = counted();
    const { key } = await upload(c.store, { witness: null });
    c.faults.del = (k) => k === key;
    expect(await promoteUpload(key, deps(c.store).d)).toMatchObject({ action: "retry", code: "upload_delete_unavailable" });
    c.faults.del = undefined;
    expect(await promoteUpload(key, deps(c.store, { nowMs: () => T0 + 1 }).d)).toEqual({ action: "ack", outcome: "already_promoted", code: "upload_witness_invalid" });
    expect((await statusOf(c.store, key)).decidedAtMs).toBe(T0);
  });

  test("a queue send failure tombstones the attempt; the redelivery hands off again (no data loss)", async () => {
    const { store } = counted();
    const { key } = await upload(store);
    const down = deps(store, { enqueue: async () => { throw new Error("queue down"); } });
    expect(await promoteUpload(key, down.d)).toMatchObject({ action: "retry", code: "admission_enqueue_failed" });
    expect(await exists(store, key)).toBe(true);
    const first = (await statusOf(store, key)).inboxKey as string;
    expect((await store.head(objectKey(first)))!.size).toBeLessThan(INBOX_MIN_BYTES);
    const up = deps(store, { nonce: () => "fedcba9876543210" });
    expect(await promoteUpload(key, up.d)).toEqual({ action: "ack", outcome: "handed_off" });
    expect(up.sent).toHaveLength(1);
    expect(up.sent[0]!.key).not.toBe(first);
    expect((await store.head(objectKey(up.sent[0]!.key)))!.size).toBeGreaterThanOrEqual(INBOX_MIN_BYTES);
    expect((await statusOf(store, key)).inboxKey).toBe(up.sent[0]!.key);
  });

  test("a consumed (tombstoned + outcome) hand-off is terminal: redelivery only removes the upload", async () => {
    const { store } = counted();
    const { key } = await upload(store);
    const first = deps(store);
    // Interpose: the hand-off lands, then the consumer admits it, then the delete is lost (object re-put to simulate).
    expect(await promoteUpload(key, first.d)).toEqual({ action: "ack", outcome: "handed_off" });
    const ledger = new MemoryLedger();
    const consumer = { mode: "on" as const, deliver: { ledger, target: { store, keyRoot: ROOT } }, custodySource: "relayhistory" as const, decompress: zstd, nowMs: () => T0 + 3000 };
    expect(await admitMessage(consumer, first.sent[0]!, 1)).toMatchObject({ action: "ack", outcome: "admitted" });
    expect((await store.head(objectKey(first.sent[0]!.key)))!.size).toBeLessThan(INBOX_MIN_BYTES);
    const again = await upload(store);
    expect(again.key).toBe(key);
    const redo = deps(store, { nonce: () => "fedcba9876543210" });
    expect(await promoteUpload(key, redo.d)).toEqual({ action: "ack", outcome: "already_promoted" });
    expect(redo.sent).toEqual([]);
    expect(await exists(store, key)).toBe(false);
    expect(ledger.claims).toHaveLength(1);
  });
});

describe("hand-off into the unchanged admission consumer", () => {
  test("the inbox the upload writes is admitted by admitMessage exactly as a POST's; the privacy gate uses the witnessed seq", async () => {
    const { store } = counted();
    const { key, batch: b } = await upload(store);
    const { d, sent } = deps(store);
    expect(await promoteUpload(key, d)).toMatchObject({ outcome: "handed_off" });
    const ledger = new MemoryLedger();
    const consumer = { mode: "on" as const, deliver: { ledger, target: { store, keyRoot: ROOT } }, custodySource: "relayhistory" as const, decompress: zstd, nowMs: () => T0 + 3000 };
    expect(await admitMessage(consumer, sent[0]!, 1)).toMatchObject({ action: "ack", outcome: "admitted" });
    expect(ledger.claims[0]).toMatchObject({ tenant: TENANT, originId: "origin-1", batchId: b.batch_id });
  });

  test("a reset completed after the witness: the consumer refuses delivery_privacy_superseded and the status says so", async () => {
    const { store } = counted();
    const { key } = await upload(store);
    const { d, sent } = deps(store);
    await promoteUpload(key, d);
    await setJournal(store, ROOT, TENANT, 1);
    const consumer = { mode: "on" as const, deliver: { ledger: new MemoryLedger(), target: { store, keyRoot: ROOT } }, custodySource: "relayhistory" as const, decompress: zstd, nowMs: () => T0 + 3000 };
    expect(await admitMessage(consumer, sent[0]!, 1)).toMatchObject({ action: "ack", outcome: "refused", code: "delivery_privacy_superseded" });
    const answer = await uploadAdmissionStatus({ store, keyRoot: ROOT }, ON, TENANT, key);
    expect(answer).toMatchObject({ ok: true, body: { state: "refused", code: "delivery_privacy_superseded", receivedAtMs: T0, decidedAtMs: T0 + 3000 } });
    expect(parseUploadAdmissionStatus((answer as { body: unknown }).body)).not.toBeNull();
  });
});

describe("admission core", () => {
  test("a tenant that is not a key segment is 400 invalid_delivery (was 503), with the precise reason kept for the upload status", async () => {
    const b = await batch([record({ record_id: "r1" })]);
    const err = await prepareForAdmission({ target: { store: counted().store, keyRoot: "Not_A_Root" } }, TENANT, b, T0).catch((e) => e);
    expect(err).toBeInstanceOf(TenantNotASegment);
    expect(err).toBeInstanceOf(DeliveryError);
    expect([err.status, err.code, err.reason]).toEqual([400, "invalid_delivery", "tenant_not_a_segment"]);
  });

  test("consumer codes outside §6 map to admission_failed; listed codes pass through", () => {
    expect(contractCode("delivery_privacy_superseded")).toBe("delivery_privacy_superseded");
    expect(contractCode("delivery_conflict")).toBe("admission_failed");
  });
});

describe("GET /v1/delivery/admission?uploadKey=", () => {
  const identity = { ...TENANT, scopes: [SYNC_SCOPE] };
  function app(store: ObjectStore, uploadDirect?: UploadDirectConfig) {
    return createIngestApp({ identify: (req) => bindingIdentity(req), log: () => {}, deliver: { ledger: new MemoryLedger(), acceptanceMode: "buffered", target: { store, keyRoot: ROOT } }, ...(uploadDirect ? { uploadDirect } : {}) });
  }
  const get = (a: ReturnType<typeof app>, q: string, who: object = identity) =>
    a.request(`/v1/delivery/admission?${q}`, { headers: { [IDENTITY_HEADER]: encodeIdentity(who as never) } });
  const err = async (r: Response) => ((await r.json()) as { error: { code: string } }).error.code;

  test("pending while the object waits: Retry-After and retryAfterSeconds, a §5 body", async () => {
    const { store } = counted();
    const { key } = await upload(store);
    const res = await get(app(store, ON), `uploadKey=${encodeURIComponent(key)}`);
    expect(res.status).toBe(200);
    expect(res.headers.get("retry-after")).toBe(String(UPLOAD_PENDING_RETRY_AFTER_SECONDS));
    const body = await res.json();
    expect(parseUploadAdmissionStatus(body)).toMatchObject({ contract: "relay-upload-inbox-v1", uploadKey: key, state: "pending", retryAfterSeconds: UPLOAD_PENDING_RETRY_AFTER_SECONDS });
  });

  test("handed off then admitted: pending, then admitted with the receipt; refused statuses carry their code", async () => {
    const { store } = counted();
    const { key } = await upload(store);
    const { d, sent } = deps(store);
    await promoteUpload(key, d);
    const a = app(store, ON);
    const pending = await (await get(a, `uploadKey=${encodeURIComponent(key)}`)).json();
    expect(parseUploadAdmissionStatus(pending)).toMatchObject({ state: "pending", receivedAtMs: T0 });
    const consumer = { mode: "on" as const, deliver: { ledger: new MemoryLedger(), target: { store, keyRoot: ROOT } }, custodySource: "relayhistory" as const, decompress: zstd, nowMs: () => T0 + 3000 };
    await admitMessage(consumer, sent[0]!, 1);
    const res = await get(a, `uploadKey=${encodeURIComponent(key)}`);
    expect(res.headers.get("retry-after")).toBeNull();
    const admitted = parseUploadAdmissionStatus(await res.json());
    expect(admitted).toMatchObject({ state: "admitted", receivedAtMs: T0, decidedAtMs: T0 + 3000 });
    expect(admitted!.receiptId).toBe((await statusOf(store, key)).receiptId);

    const bad = await upload(store, { originId: "origin-9", batchId: "b-9", witness: null });
    await promoteUpload(bad.key, deps(store).d);
    expect(parseUploadAdmissionStatus(await (await get(a, `uploadKey=${encodeURIComponent(bad.key)}`)).json())).toMatchObject({ state: "refused", code: "upload_witness_invalid", decidedAtMs: T0 });
  });

  test("400 invalid_delivery for a key that does not parse or names another key root", async () => {
    const { store } = counted();
    const { key } = await upload(store, { keyRoot: "otherroot" });
    for (const q of ["uploadKey=", "uploadKey=nope", `uploadKey=${encodeURIComponent(key)}`]) {
      const res = await get(app(store, ON), q);
      expect([res.status, await err(res)]).toEqual([400, "invalid_delivery"]);
    }
  });

  test("403 upload_tenant_mismatch for another user's, workspace's or org's key; nothing read", async () => {
    const { store, reads } = counted();
    const { key } = await upload(store);
    reads.length = 0;
    for (const who of [{ ...identity, userId: "user-2" }, { ...identity, workspaceId: "ws-b" }, { ...identity, orgId: "org-b" }]) {
      const res = await get(app(store, ON), `uploadKey=${encodeURIComponent(key)}`, who);
      expect([res.status, await err(res)]).toEqual([403, "upload_tenant_mismatch"]);
    }
    expect(reads).toEqual([]);
  });

  test("404 upload_unknown for no object and no status, and for a caller the allowlist excludes", async () => {
    const { store } = counted();
    const { key } = await upload(store);
    const excluded = uploadDirectConfig({ HISTORY_UPLOAD_DIRECT: "allowlist", HISTORY_UPLOAD_DIRECT_ALLOWLIST: "org:org-b" });
    let res = await get(app(store, excluded), `uploadKey=${encodeURIComponent(key)}`);
    expect([res.status, await err(res)]).toEqual([404, "upload_unknown"]);
    await store.delete(objectKey(key));
    res = await get(app(store, ON), `uploadKey=${encodeURIComponent(key)}`);
    expect([res.status, await err(res)]).toEqual([404, "upload_unknown"]);
  });

  test("503 delivery_unavailable with Retry-After on a store fault", async () => {
    const c = counted();
    const { key } = await upload(c.store);
    c.faults.get = () => true;
    const res = await get(app(c.store, ON), `uploadKey=${encodeURIComponent(key)}`);
    expect([res.status, await err(res)]).toEqual([503, "delivery_unavailable"]);
    expect(res.headers.get("retry-after")).toBe("5");
  });

  test("flag off: ?uploadKey= is ignored and the originId + receiptId form answers exactly as before", async () => {
    const { store } = counted();
    const { key } = await upload(store);
    for (const config of [undefined, UPLOAD_DIRECT_OFF]) {
      const res = await get(app(store, config), `uploadKey=${encodeURIComponent(key)}`);
      expect([res.status, await err(res)]).toEqual([400, "invalid_delivery"]);
      const ok = await get(app(store, config), `originId=origin-1&receiptId=rhr_${"a".repeat(64)}`);
      expect(await ok.json()).toEqual({ receiptId: `rhr_${"a".repeat(64)}`, state: "pending" });
    }
    // With the flag on, the existing form is unchanged too.
    const ok = await get(app(store, ON), `originId=origin-1&receiptId=rhr_${"a".repeat(64)}`);
    expect(await ok.json()).toEqual({ receiptId: `rhr_${"a".repeat(64)}`, state: "pending" });
  });
});

describe("upload-admission Lambda", () => {
  const saved = { ...process.env };
  afterEach(() => {
    for (const k of ["HISTORY_UPLOAD_DIRECT", "HISTORY_UPLOAD_DIRECT_ALLOWLIST", "UPLOAD_WITNESS_KEY"]) {
      if (saved[k] === undefined) delete process.env[k];
      else process.env[k] = saved[k];
    }
  });
  const s3Body = (...keys: string[]) => JSON.stringify({ Records: keys.map((k) => ({ eventName: "ObjectCreated:Put", s3: { object: { key: encodeURIComponent(k).replace(/%2F/g, "/") } } })) });

  test("notification parsing: URL-decoded keys, TestEvent acks, anything else is malformed", () => {
    expect(notificationKeys(JSON.stringify({ Records: [{ eventName: "ObjectCreated:Put", s3: { object: { key: "a/b+c%3Dd" } } }] }))).toEqual(["a/b c=d"]);
    expect(notificationKeys(JSON.stringify({ Event: "s3:TestEvent" }))).toEqual([]);
    for (const bad of [undefined, "", "{", "[]", "{}", JSON.stringify({ Records: [] }), JSON.stringify({ Records: [{ eventName: "ObjectRemoved:Delete", s3: { object: { key: "k" } } }] }),
      JSON.stringify({ Records: [{ eventName: "ObjectCreated:Put", s3: { object: {} } }] }), JSON.stringify({ Records: [{ eventName: "ObjectCreated:Put", s3: { object: { key: "%E0%A4%A" } } }] })])
      expect(notificationKeys(bad)).toBeNull();
  });

  test("flag off (and unset, and invalid): every record fails, the runtime is never built, storage never read", async () => {
    let built = 0;
    const runtime: UploadAdmissionRuntimeFactory = async () => { built++; throw new Error("must not run"); };
    for (const flag of [undefined, "off", "ON", "yes"]) {
      if (flag === undefined) delete process.env.HISTORY_UPLOAD_DIRECT;
      else process.env.HISTORY_UPLOAD_DIRECT = flag;
      const lines: Record<string, unknown>[] = [];
      const handler = createUploadAdmissionHandler(runtime, (l) => void lines.push(l));
      const res = await handler({ Records: [{ messageId: "m1", body: s3Body("k") }, { messageId: "m2", body: "{" }] });
      expect(res).toEqual({ batchItemFailures: [{ itemIdentifier: "m1" }, { itemIdentifier: "m2" }] });
      expect(JSON.stringify(lines)).not.toContain('"k"');
    }
    expect(built).toBe(0);
  });

  test("flag on: handed off and refused records ack; malformed, invalid-key and retry records fail; logs carry no key", async () => {
    process.env.HISTORY_UPLOAD_DIRECT = "on";
    process.env.UPLOAD_WITNESS_KEY = WITNESS_KEY;
    const { store } = counted();
    const good = await upload(store);
    const refused = await upload(store, { originId: "origin-2", batchId: "b-2", witness: null });
    const sent: AdmissionMessage[] = [];
    const runtime: UploadAdmissionRuntimeFactory = async () => ({ deliver: { acceptanceMode: "buffered", target: { store, keyRoot: ROOT } }, enqueue: async (m) => { sent.push(m); }, nowMs: () => T0 });
    const lines: Record<string, unknown>[] = [];
    const handler = createUploadAdmissionHandler(runtime, (l) => void lines.push(l));
    const res = await handler({ Records: [
      { messageId: "ok", body: s3Body(good.key) },
      { messageId: "refused", body: s3Body(refused.key) },
      { messageId: "test", body: JSON.stringify({ Event: "s3:TestEvent" }) },
      { messageId: "malformed", body: "not json" },
      { messageId: "foreign", body: s3Body("elsewhere/x.json.zst") },
      { body: s3Body(good.key) },
    ] });
    expect(res.batchItemFailures).toEqual([{ itemIdentifier: "malformed" }, { itemIdentifier: "foreign" }, { itemIdentifier: "" }]);
    expect(sent).toHaveLength(1);
    expect(JSON.stringify(lines)).not.toContain(good.key);
    expect(JSON.stringify(lines)).not.toContain("org-a");
    expect(lines.at(-1)).toMatchObject({ outcome: "batch", records: 6, failures: 3, handed_off: 1, upload_witness_invalid: 1, malformed_record: 1, upload_key_invalid: 1 });
  });

  test("flag on without UPLOAD_WITNESS_KEY: every upload record fails (retry), nothing refused or removed", async () => {
    process.env.HISTORY_UPLOAD_DIRECT = "on";
    delete process.env.UPLOAD_WITNESS_KEY;
    const { store } = counted();
    const good = await upload(store);
    const handler = createUploadAdmissionHandler(async () => ({ deliver: { acceptanceMode: "buffered", target: { store, keyRoot: ROOT } }, enqueue: async () => {}, nowMs: () => T0 }), () => {});
    expect(await handler({ Records: [{ messageId: "m1", body: s3Body(good.key) }] })).toEqual({ batchItemFailures: [{ itemIdentifier: "m1" }] });
    expect(await exists(store, good.key)).toBe(true);
  });

  test("the deployed handler has no runtime: flag on fails every record (runtime_unavailable)", async () => {
    process.env.HISTORY_UPLOAD_DIRECT = "on";
    const lines: Record<string, unknown>[] = [];
    const res = await createUploadAdmissionHandler(undefined, (l) => void lines.push(l))({ Records: [{ messageId: "m1", body: s3Body("k") }] });
    expect(res).toEqual({ batchItemFailures: [{ itemIdentifier: "m1" }] });
    expect(lines[0]).toMatchObject({ outcome: "runtime_unavailable" });
  });
});

describe("HISTORY_UPLOAD_DIRECT parsing", () => {
  test("off by default; exact on / allowlist; anything else refuses without echoing the value", () => {
    for (const v of [undefined, "", "off"]) expect(uploadDirectConfig({ HISTORY_UPLOAD_DIRECT: v }).mode).toBe("off");
    expect(uploadDirectConfig({ HISTORY_UPLOAD_DIRECT: "on" }).mode).toBe("on");
    for (const v of ["ON", "true", "1", " on"]) expect(() => uploadDirectConfig({ HISTORY_UPLOAD_DIRECT: v })).toThrow("HISTORY_UPLOAD_DIRECT must be off, allowlist or on");
    expect(() => uploadDirectConfig({ HISTORY_UPLOAD_DIRECT: "allowlist" })).toThrow(/needs HISTORY_UPLOAD_DIRECT_ALLOWLIST/);
    expect(() => uploadDirectConfig({ HISTORY_UPLOAD_DIRECT: "allowlist", HISTORY_UPLOAD_DIRECT_ALLOWLIST: "org-a" })).toThrow(/org:<id> or ws:<id>/);
    expect(() => uploadDirectConfig({ HISTORY_UPLOAD_DIRECT: "allowlist", HISTORY_UPLOAD_DIRECT_ALLOWLIST: "org:.hidden" })).toThrow();
    const a = uploadDirectConfig({ HISTORY_UPLOAD_DIRECT: "allowlist", HISTORY_UPLOAD_DIRECT_ALLOWLIST: "org:org-a, ws:ws-z" });
    expect([a.mode, [...a.orgs], [...a.workspaces]]).toEqual(["allowlist", ["org-a"], ["ws-z"]]);
  });

  test("Worker: default off; on reaches the app; an invalid value is logged and treated as off", async () => {
    const { ingestSettings } = await import("../worker.ts");
    const full = { HYPERDRIVE: { connectionString: "postgres://x" }, INGEST_FANOUT: {} as never, S3_ACCESS_KEY_ID: "k", S3_SECRET_ACCESS_KEY: "s", INGEST_SIGNING_KEY: "sign", INGEST_S3_BUCKET: "b", INGEST_KMS_KEY_ARN: "arn" };
    expect((await ingestSettings(full))?.uploadDirect).toBeUndefined();
    expect((await ingestSettings({ ...full, HISTORY_UPLOAD_DIRECT: "on" }))?.uploadDirect?.mode).toBe("on");
    const original = console.log, errors = console.error, warns = console.warn;
    const out: string[] = [];
    console.log = console.error = console.warn = (...a: unknown[]) => void out.push(a.map(String).join(" "));
    try {
      expect((await ingestSettings({ ...full, HISTORY_UPLOAD_DIRECT: "maybe" }))?.uploadDirect?.mode ?? "off").toBe("off");
    } finally {
      console.log = original; console.error = errors; console.warn = warns;
    }
    expect(out.join("\n")).not.toContain("maybe");
  });

  test("self-host: off unless set; on needs async admission and a valid key root; the scan batch is bounded", async () => {
    const { selfhostUploadDirectSettings } = await import("../selfhost-config.ts");
    const admission = {} as never;
    expect(selfhostUploadDirectSettings({}, ROOT, admission)).toBeUndefined();
    expect(() => selfhostUploadDirectSettings({ HISTORY_UPLOAD_DIRECT: "on" }, ROOT, undefined)).toThrow(/SELFHOST_INGEST_ADMISSION/);
    const on = selfhostUploadDirectSettings({ HISTORY_UPLOAD_DIRECT: "on", UPLOAD_WITNESS_KEY: WITNESS_KEY }, ROOT, admission)!;
    expect([on.config.mode, on.witnessKey, on.scanBatch]).toEqual(["on", WITNESS_KEY, 16]);
    expect(selfhostUploadDirectSettings({ HISTORY_UPLOAD_DIRECT: "on", SELFHOST_UPLOAD_SCAN_BATCH: "100" }, ROOT, admission)!.scanBatch).toBe(100);
    for (const n of ["0", "101", "x"]) expect(() => selfhostUploadDirectSettings({ HISTORY_UPLOAD_DIRECT: "on", SELFHOST_UPLOAD_SCAN_BATCH: n }, ROOT, admission)).toThrow();
    expect(() => selfhostUploadDirectSettings({ HISTORY_UPLOAD_DIRECT: "on" }, "Bad_Root", admission)).toThrow();
  });
});

// Keep the status-key helper honest: a refused status read back by the status endpoint is the same object.
test("receiptDir/outcomeKey are the admission consumer's (status resolution reads the consumer's own outcome)", () => {
  expect(outcomeKey(receiptDir(ROOT, TENANT, "origin-1", `rhr_${"a".repeat(64)}`), "admitted")).toMatch(/\/r=a{64}\/outcome-admitted\.json$/);
});
