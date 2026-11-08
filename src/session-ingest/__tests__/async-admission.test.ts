/**
 * S3-first async admission without a database: settings, the request path's gating and
 * failure answers, the inbox's at-rest scrubbing and tenant binding, the privacy journal's
 * monotonic CAS, the consumer's outcome classification (frozen-supabase legacy receipt and
 * witness included) and the Worker queue dispatch (buffered mode admits, never fans out).
 */
import { afterEach, describe, expect, test } from "bun:test";
import { zstdDecompressSync } from "node:zlib";
import { objectKey, readAll } from "../../shared/vendor.ts";
import { createIngestApp, SYNC_SCOPE } from "../app.ts";
import { admitMessage, backoffSeconds } from "../admission-consumer.ts";
import {
  ADMISSION_ACCEPT_HEADER, ADMISSION_ACCEPT_VALUE, ADMISSION_HEADER, assertNoLaterPrivacyControl, asyncAdmissionMode, decodeInbox, encodeInbox,
  enqueueAdmission, inboxKey, journalKey, raisePrivacyWatermark, type AdmissionMessage,
} from "../async-admission.ts";
import { LedgerUnavailableError, type Ledger } from "../ledger.ts";
import { prepareDelivery } from "../vendor/delivery/prepare.ts";
import { CURRENT_CUSTODY_CONTENT_TYPE } from "../custody-protocol.ts";
import { MemoryLedger, TENANT, batch, record, scratchStore } from "./support.ts";

const cleanups: (() => void)[] = [];
afterEach(() => cleanups.splice(0).forEach((f) => f()));
const zstd = (b: Uint8Array) => new Uint8Array(zstdDecompressSync(b));
const identity = { ...TENANT, scopes: [SYNC_SCOPE] };
const T0 = 1_800_000_000_000;

function store() {
  const s = scratchStore();
  cleanups.push(s.done);
  return s.store;
}
async function text(s: ReturnType<typeof store>, key: string) {
  return new TextDecoder().decode(zstd(await readAll((await s.getObject(objectKey(key))).body)));
}

describe("INGEST_ASYNC_ADMISSION", () => {
  test("parses off/on/drain only; anything else refuses", () => {
    expect(asyncAdmissionMode(undefined)).toBe("off");
    expect(asyncAdmissionMode(" ON ")).toBe("on");
    expect(asyncAdmissionMode("drain")).toBe("drain");
    expect(() => asyncAdmissionMode("yes")).toThrow();
  });

  test("Worker settings: default off; on needs buffered acceptance and the INGEST_ADMISSION queue", async () => {
    const { ingestSettings } = await import("../worker.ts");
    const full = { HYPERDRIVE: { connectionString: "postgres://x" }, INGEST_FANOUT: {} as never, S3_ACCESS_KEY_ID: "k", S3_SECRET_ACCESS_KEY: "s", INGEST_SIGNING_KEY: "sign", INGEST_S3_BUCKET: "b", INGEST_KMS_KEY_ARN: "arn" };
    const buffered = { ...full, INGEST_ACCEPTANCE_MODE: "buffered", INGEST_EXPECT_GENERATION: "a".repeat(64), INGEST_INLINE_MAX_SESSIONS: "0" };
    expect((await ingestSettings(buffered))?.asyncAdmission).toBeUndefined();
    await expect(ingestSettings({ ...full, INGEST_ASYNC_ADMISSION: "on", INGEST_ADMISSION: {} as never })).rejects.toThrow(/buffered/);
    expect(await ingestSettings({ ...buffered, INGEST_ASYNC_ADMISSION: "on" })).toBeNull();
    expect((await ingestSettings({ ...buffered, INGEST_ASYNC_ADMISSION: "drain", INGEST_ADMISSION: {} as never }))?.asyncAdmission).toBe("drain");
  });

  test("createIngestApp refuses async admission outside buffered acceptance", () => {
    expect(() => createIngestApp({ identify: () => identity, deliver: { ledger: new MemoryLedger(), target: { store: store(), keyRoot: "96bulls" } }, asyncAdmission: { mode: "on", enqueue: async () => {} } })).toThrow(/buffered/);
  });
});

describe("request path", () => {
  const app = (s = store(), enqueue: (m: AdmissionMessage) => Promise<void> = async () => {}, ledger: Ledger = new MemoryLedger()) =>
    createIngestApp({ identify: () => identity, log: () => {}, deliver: { ledger, acceptanceMode: "buffered", target: { store: s, keyRoot: "96bulls" } }, asyncAdmission: { mode: "on", enqueue, nowMs: () => T0 } });
  const post = async (a: ReturnType<typeof app>, b: unknown, headers: Record<string, string> = { [ADMISSION_ACCEPT_HEADER]: ADMISSION_ACCEPT_VALUE }) =>
    a.request("/v1/delivery/batches", { method: "POST", headers: { "content-type": "application/json", ...headers }, body: JSON.stringify({ protocolVersion: 1, batch: b }) });

  test("the receipt body is byte-identical to the synchronous receipt (Desktop compatibility)", async () => {
    const b = await batch([record({ record_id: "r1" })]);
    const queued = await post(app(), b);
    const prepared = await prepareDelivery(TENANT, b);
    const body = await queued.json() as Record<string, unknown>;
    const { limits, ...receipt } = body;
    expect(receipt).toEqual(prepared.receipt as never);
    expect((limits as { contentEncodings: string[] }).contentEncodings.length).toBeGreaterThan(0);
    expect(queued.headers.get(ADMISSION_HEADER)).toBe('{"version":1,"state":"queued"}');
  });

  test("a queue send failure is a retryable 503 and the unacknowledged inbox content is tombstoned", async () => {
    const s = store();
    const res = await post(app(s, async () => { throw new Error("queue down"); }), await batch([record({ record_id: "r1", payload: { text: "private words" } })]));
    expect(res.status).toBe(503);
    expect(((await res.json()) as { error: { code: string } }).error.code).toBe("delivery_unavailable");
    const keys = (await s.list(objectKey("96bulls/.admission-inbox/org-a"))).items.map((i) => String(i.key));
    expect(keys).toHaveLength(1);
    expect(await text(s, keys[0]!)).not.toContain("private words");
  });

  test("validation errors keep their synchronous codes (400/403) and write nothing", async () => {
    const s = store();
    const res = await post(app(s), await batch([record({ record_id: "r1" })], { account_id: "acct_wrong" }));
    expect(res.status).toBe(403);
    expect((await s.list(objectKey("96bulls/.admission-inbox"))).items).toHaveLength(0);
  });

  test("a private custody envelope never takes the async path", async () => {
    const sent: AdmissionMessage[] = [];
    const res = await post(app(store(), async (m) => { sent.push(m); }), await batch([record({ record_id: "r1" })]), { [ADMISSION_ACCEPT_HEADER]: ADMISSION_ACCEPT_VALUE, "content-type": CURRENT_CUSTODY_CONTENT_TYPE });
    expect(res.status).not.toBe(200);
    expect(sent).toHaveLength(0);
  });

  test("a journal failure refuses the reset (fail closed) before the ledger sees it", async () => {
    const s = store();
    const ledger = new MemoryLedger();
    let resets = 0;
    ledger.resetUploadedSessions = async () => { resets++; return 0; };
    const broken = { ...s, getObject: async () => { throw Object.assign(new Error("s3 down"), { code: "transient" }); } } as typeof s;
    const a = app(broken, async () => {}, ledger);
    const { deliveryAccount } = await import("../vendor/delivery/prepare.ts");
    const res = await a.request("/v1/delivery/sessions/reset", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ account_id: await deliveryAccount(TENANT), request_id: "r", confirmation: "DELETE ALL SESSIONS" }) });
    expect(res.status).toBe(503);
    expect(resets).toBe(0);
  });
});

describe("inbox object", () => {
  test("is scrubbed at rest, bound to its key, and refuses a re-pointed or tampered object", async () => {
    const s = store();
    const b = await batch([record({ record_id: "r1", payload: { text: "export AWS_SECRET_ACCESS_KEY=abcdEFGHijklMNOPqrstUVWXyz0123456789abcd", ts_ms: 1_761_955_200_000 } })]);
    const prepared = await prepareDelivery(TENANT, b);
    const key = await enqueueAdmission({ store: s, keyRoot: "96bulls", enqueue: async () => {}, nonce: () => "0123456789abcdef" }, TENANT, prepared, T0);
    expect(key).toBe(inboxKey("96bulls", TENANT, "origin-1", prepared.receipt.receiptId, T0, "0123456789abcdef"));
    expect(key.startsWith("96bulls/.admission-inbox/org-a/v1/ws=ws-a/m=srv_")).toBe(true);
    const raw = await text(s, key);
    expect(raw).not.toContain("abcdEFGHijklMNOPqrstUVWXyz0123456789abcd");
    const bytes = new TextEncoder().encode(raw);
    const decoded = await decodeInbox("96bulls", key, bytes);
    expect(decoded?.tenant).toEqual(TENANT);
    expect(decoded?.receivedAtMs).toBe(T0);
    expect(decoded?.prepared.records.map((r) => [r.record_id, r.digest])).toEqual(prepared.records.map((r) => [r.record_id, r.digest]));
    // Another user's key for the same bytes, a changed upload time, or another batch's header: refused.
    await expect(decodeInbox("96bulls", inboxKey("96bulls", { ...TENANT, userId: "user-2" }, "origin-1", prepared.receipt.receiptId, T0, "0123456789abcdef"), bytes)).rejects.toThrow();
    await expect(decodeInbox("96bulls", inboxKey("96bulls", TENANT, "origin-1", prepared.receipt.receiptId, T0 + 1, "0123456789abcdef"), bytes)).rejects.toThrow();
    await expect(decodeInbox("96bulls", key, new TextEncoder().encode(raw.replace(`"batchId":"batch-1"`, `"batchId":"batch-2"`)))).rejects.toThrow();
    const extra = raw.trimEnd() + "\n" + raw.split("\n")[1] + "\n";
    await expect(decodeInbox("96bulls", key, new TextEncoder().encode(extra))).rejects.toThrow();
  });

  test("record identities are never scrubbed: a secret-shaped client id keeps the claim and receipt exact", async () => {
    const id = "ghp_" + "A1b2C3d4E5f6G7h8I9j0K1l2M3n4O5p6Q7r8";
    const { scrubRecords } = await import("@rhc/lib/pipeline/raw-chunks.ts");
    // The scrubber would rewrite this id inside a record line (so the test is meaningful).
    expect(new TextDecoder().decode(scrubRecords(new TextEncoder().encode(JSON.stringify({ record_id: id }) + "\n")).scrubbed)).not.toContain(id);
    const prepared = await prepareDelivery(TENANT, await batch([record({ record_id: id })]));
    const key = inboxKey("96bulls", TENANT, "origin-1", prepared.receipt.receiptId, T0, "0123456789abcdef");
    const decoded = await decodeInbox("96bulls", key, zstd(encodeInbox(TENANT, prepared, T0)));
    expect(decoded?.prepared.records.map((r) => [r.record_id, r.revision_id, r.revision, r.digest, r.source, r.session_id, r.kind, r.operation]))
      .toEqual(prepared.records.map((r) => [r.record_id, r.revision_id, r.revision, r.digest, r.source, r.session_id, r.kind, r.operation]));
  });

  test("the inbox root can never be a tenant prefix (no org id may start with a dot)", async () => {
    const { assertTenantSegment } = await import("../writer.ts");
    expect(() => assertTenantSegment("96bulls", ".admission-inbox")).toThrow();
    const key = inboxKey("96bulls", TENANT, "origin-1", "rhr_" + "a".repeat(64), T0, "0123456789abcdef");
    expect(key.startsWith("96bulls/.admission-inbox/org-a/")).toBe(true);
    expect(objectKey(key)).toBe(key as never);
  });

  test("an empty prepared batch round-trips", async () => {
    const prepared = await prepareDelivery(TENANT, await batch([record({ record_id: "t1", kind: "trajectory" as never })]));
    expect(prepared.records).toHaveLength(0);
    const key = inboxKey("96bulls", TENANT, "origin-1", prepared.receipt.receiptId, T0, "0123456789abcdef");
    expect((await decodeInbox("96bulls", key, zstd(encodeInbox(TENANT, prepared, T0))))?.prepared.records).toHaveLength(0);
  });
});

describe("privacy journal", () => {
  test("is a monotonic max under concurrent raises, and refuses exactly the uploads that started before it", async () => {
    const s = store();
    await Promise.all([5, 3, 9, 1, 7].map((n) => raisePrivacyWatermark(s, "96bulls", TENANT, T0 + n)));
    const j = JSON.parse(new TextDecoder().decode(await readAll((await s.getObject(objectKey(journalKey("96bulls", TENANT)))).body)));
    expect(j).toEqual({ v: 1, watermarkMs: T0 + 9 });
    await raisePrivacyWatermark(s, "96bulls", TENANT, T0 + 2);
    await expect(assertNoLaterPrivacyControl(s, "96bulls", TENANT, T0 + 9)).rejects.toMatchObject({ code: "delivery_privacy_superseded" });
    await expect(assertNoLaterPrivacyControl(s, "96bulls", TENANT, T0 + 10)).resolves.toBeUndefined();
    // Per user: another user of the same workspace is unaffected.
    await expect(assertNoLaterPrivacyControl(s, "96bulls", { ...TENANT, userId: "user-2" }, T0)).resolves.toBeUndefined();
  });
});

describe("consumer", () => {
  async function queued(s = store(), b?: Awaited<ReturnType<typeof batch>>) {
    const prepared = await prepareDelivery(TENANT, b ?? await batch([record({ record_id: "r1" })]));
    const key = await enqueueAdmission({ store: s, keyRoot: "96bulls", enqueue: async () => {} }, TENANT, prepared, T0);
    return { s, prepared, message: { v: 1, kind: "ingest-admission", key } as AdmissionMessage };
  }
  const deps = (s: ReturnType<typeof store>, ledger: Ledger, custodySource: "relayhistory" | "frozen-supabase" = "relayhistory") =>
    ({ mode: "on" as const, deliver: { ledger, acceptanceMode: "buffered" as const, target: { store: s, keyRoot: "96bulls" } }, custodySource, decompress: zstd, nowMs: () => T0 + 5000 });

  test("a batch id conflict is a terminal refusal with a visible outcome, and the inbox is retired", async () => {
    const q = await queued();
    const ledger = new MemoryLedger();
    ledger.script.push({ kind: "batch_conflict", currentDigest: "f".repeat(64) });
    expect(await admitMessage(deps(q.s, ledger), q.message, 1)).toMatchObject({ action: "ack", outcome: "refused", code: "delivery_conflict", lagMs: 5000 });
    expect(await admitMessage(deps(q.s, ledger), q.message, 2)).toMatchObject({ action: "ack", outcome: "already_terminal" });
  });

  test("a lock timeout is retried with backoff, never refused, and writes no outcome", async () => {
    const q = await queued();
    const ledger = new MemoryLedger();
    ledger.claim = async () => { throw new LedgerUnavailableError("ledger_lock_timeout", { db_error_class: "lock_timeout", db_error_code: "55P03", db_error_basis: "sqlstate" } as never); };
    const r = await admitMessage(deps(q.s, ledger), q.message, 3);
    expect(r).toMatchObject({ action: "retry", delaySeconds: 60, code: "ledger_lock_timeout", stage: "claim" });
    expect((await q.s.list(objectKey("96bulls/.admission-inbox"))).items.filter((i) => String(i.key).includes("outcome-"))).toHaveLength(0);
    expect([1, 2, 3, 6, 20].map(backoffSeconds)).toEqual([15, 30, 60, 300, 300]);
  });

  test("a slot-busy refusal waits its Retry-After", async () => {
    const q = await queued();
    const ledger = new MemoryLedger();
    ledger.acquireSlot = async () => ({ ok: false, retryAfterSeconds: 7 });
    const r = await admitMessage({ ...deps(q.s, ledger), deliver: { ...deps(q.s, ledger).deliver, acceptanceMode: undefined as never, slots: { maxPerUser: 1, maxPerOrg: 1, ttlSeconds: 90 } } }, q.message, 1);
    expect(r).toMatchObject({ action: "retry", delaySeconds: 7 });
  });

  test("frozen-supabase: the legacy receipt and the witness move from relayhistory's request path into the consumer", async () => {
    const q = await queued();
    const ledger = new MemoryLedger() as MemoryLedger & Ledger;
    const calls: string[] = [];
    ledger.frozenReceipt = async () => { calls.push("receipt"); return { digest: q.prepared.batchDigest, receipt: {} }; };
    expect(await admitMessage(deps(q.s, ledger, "frozen-supabase"), q.message, 1)).toMatchObject({ action: "ack", outcome: "admitted", code: "legacy_replay" });
    const other = await queued(q.s, await batch([record({ record_id: "r2" })], { batch_id: "b2" }));
    ledger.frozenReceipt = async () => ({ digest: "0".repeat(64), receipt: {} });
    expect(await admitMessage(deps(q.s, ledger, "frozen-supabase"), other.message, 1)).toMatchObject({ action: "ack", outcome: "refused", code: "delivery_conflict" });
    const third = await queued(q.s, await batch([record({ record_id: "r3" })], { batch_id: "b3" }));
    ledger.frozenReceipt = async () => null;
    ledger.frozenWitness = async () => { calls.push("witness"); throw new LedgerUnavailableError("ledger_sqlstate_55000"); };
    expect(await admitMessage(deps(q.s, ledger, "frozen-supabase"), third.message, 1)).toMatchObject({ action: "retry" });
    expect(calls).toEqual(["receipt", "witness"]);
  });

  test("off waits; a malformed message, a foreign key or a missing inbox walk to the DLQ (retry), never ack", async () => {
    const q = await queued();
    const ledger = new MemoryLedger();
    expect(await admitMessage({ ...deps(q.s, ledger), mode: "off" }, q.message, 1)).toMatchObject({ action: "retry", code: "async_admission_off" });
    expect(await admitMessage(deps(q.s, ledger), { v: 1, kind: "ingest-admission", key: "96bulls/org-a/v1/x" }, 1)).toMatchObject({ action: "retry", code: "admission_message_invalid" });
    expect(await admitMessage(deps(q.s, ledger), { ...q.message, key: q.message.key.replace(".ndjson.zst", "0.ndjson.zst") }, 1)).toMatchObject({ action: "retry", code: "inbox_missing" });
    expect(ledger.claims).toHaveLength(0);
  });

  test("an inbox past the admission window is refused, not admitted", async () => {
    const q = await queued();
    const ledger = new MemoryLedger();
    expect(await admitMessage({ ...deps(q.s, ledger), nowMs: () => T0 + 7 * 24 * 3600 * 1000 }, q.message, 1)).toMatchObject({ action: "ack", outcome: "refused", code: "admission_expired" });
    expect(ledger.claims).toHaveLength(0);
  });
});

describe("Worker queue dispatch", () => {
  test("buffered mode admits admission messages and still refuses canonical fanout", async () => {
    const { admitQueueBatch } = await import("../worker.ts");
    const s = store();
    const prepared = await prepareDelivery(TENANT, await batch([record({ record_id: "r1" })]));
    const key = await enqueueAdmission({ store: s, keyRoot: "96bulls", enqueue: async () => {} }, TENANT, prepared, Date.now());
    const results: string[] = [];
    const msg = (body: unknown) => ({ body, attempts: 1, ack: () => results.push("ack"), retry: (o?: { delaySeconds?: number }) => results.push(`retry:${o?.delaySeconds}`) });
    const settings = { bucket: "b", region: "us-east-1", keyRoot: "96bulls", kmsKeyArn: "a", accessKeyId: "k", secretAccessKey: "s", signingKey: "x", maxPerUser: 4, maxPerOrg: 32, inlineMaxSessions: 0, replicaMode: "off" as const, acceptanceMode: "buffered" as const, expectedGeneration: "a".repeat(64), asyncAdmission: "on" as const };
    const sql: string[] = [];
    const connect = () => ({ run: async (t: string) => { sql.push(t); throw Object.assign(new Error("lock"), { code: "55P03" }); }, end: async () => {} });
    await admitQueueBatch([msg({ v: 1, kind: "ingest-admission", key })], { INGEST_ACCEPTANCE_MODE: "buffered" }, settings, connect, { store: s, keyRoot: "96bulls" });
    expect(results).toEqual(["retry:15"]);
    expect(sql.some((t) => t.includes("ingest_claim_buffered_batch"))).toBe(true);
  });
});
