/**
 * S3-first async admission without a database: settings, the request path's gating and
 * failure answers, the inbox's at-rest scrubbing and tenant binding, the privacy journal's
 * monotonic CAS, the consumer's outcome classification (frozen-supabase legacy receipt and
 * witness included) and the Worker queue dispatch (buffered mode admits, never fans out).
 */
import { afterEach, describe, expect, test } from "bun:test";
import { zstdDecompressSync } from "node:zlib";
import { objectKey, readAll } from "../../shared/vendor.ts";
import { bindingIdentity, createIngestApp, encodeIdentity, IDENTITY_HEADER, SYNC_SCOPE } from "../app.ts";
import { admitMessage, backoffSeconds } from "../admission-consumer.ts";
import {
  ADMISSION_ACCEPT_VALUE, ADMISSION_HEADER, PENDING_TTL_MS, abortPrivacyControl, assertNoLaterPrivacyControl, asyncAdmissionMode, beginPrivacyControl,
  decodeInbox, encodeInbox, enqueueAdmission, inboxKey, journalKey, settlePrivacyControl, witnessPrivacyJournal, type AdmissionMessage,
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
    createIngestApp({ identify: (req) => bindingIdentity(req), log: () => {}, deliver: { ledger, acceptanceMode: "buffered", target: { store: s, keyRoot: "96bulls" } }, asyncAdmission: { mode: "on", enqueue, nowMs: () => T0 } });
  const optIn = { [IDENTITY_HEADER]: encodeIdentity({ ...identity, admission: ADMISSION_ACCEPT_VALUE }) };
  const post = async (a: ReturnType<typeof app>, b: unknown, headers: Record<string, string> = optIn) =>
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
    const res = await post(app(store(), async (m) => { sent.push(m); }), await batch([record({ record_id: "r1" })]), { ...optIn, "content-type": CURRENT_CUSTODY_CONTENT_TYPE });
    expect(res.status).not.toBe(200);
    expect(sent).toHaveLength(0);
  });

  test("the opt-in outside the signed identity (a loose header) is ignored: synchronous path", async () => {
    const sent: AdmissionMessage[] = [];
    const ledger = new MemoryLedger();
    await post(app(store(), async (m) => { sent.push(m); }, ledger), await batch([record({ record_id: "r1" })]), { [IDENTITY_HEADER]: encodeIdentity(identity), "x-relay-ingest-admission": ADMISSION_ACCEPT_VALUE });
    expect(sent).toHaveLength(0);
    expect(ledger.claims).toHaveLength(1);
  });

  test("a non-ordered reset or re-enable is refused before SQL while async admission is on (buffered SQL refuses them anyway)", async () => {
    const s = store();
    const ledger = new MemoryLedger();
    let calls = 0;
    ledger.resetUploadedSessions = async () => { calls++; return 0; };
    ledger.setSessionUploads = async () => { calls++; };
    const a = app(s, async () => {}, ledger);
    const { deliveryAccount } = await import("../vendor/delivery/prepare.ts");
    const account = await deliveryAccount(TENANT);
    const h = { "content-type": "application/json", [IDENTITY_HEADER]: encodeIdentity(identity) };
    expect((await a.request("/v1/delivery/sessions/reset", { method: "POST", headers: h, body: JSON.stringify({ account_id: account, request_id: "r", confirmation: "DELETE ALL SESSIONS" }) })).status).toBe(503);
    expect((await a.request("/v1/delivery/sessions/selection", { method: "POST", headers: h, body: JSON.stringify({ account_id: account, origin_id: "o", sessions: [{ source: "claude", session_id: "s" }], enabled: true }) })).status).toBe(503);
    expect(calls).toBe(0);
    // A disable is not journaled and still reaches the ledger exactly as with async off.
    expect((await a.request("/v1/delivery/sessions/selection", { method: "POST", headers: h, body: JSON.stringify({ account_id: account, origin_id: "o", sessions: [{ source: "claude", session_id: "s" }], enabled: false }) })).status).toBe(200);
    expect(calls).toBe(1);
  });

  test("a journal witness failure falls back to the synchronous path, never a refusal", async () => {
    const s = store();
    const broken = { ...s, getObject: async () => { throw Object.assign(new Error("s3 down"), { code: "transient" }); } } as typeof s;
    const sent: AdmissionMessage[] = [];
    const ledger = new MemoryLedger();
    await post(app(broken, async (m) => { sent.push(m); }, ledger), await batch([record({ record_id: "r1" })]));
    expect(sent).toHaveLength(0);
    expect(ledger.claims).toHaveLength(1);
  });

  test("a journal failure refuses the reset (fail closed) before the ledger sees it", async () => {
    const s = store();
    const ledger = new MemoryLedger();
    let resets = 0;
    ledger.resetUploadedSessions = async () => { resets++; return 0; };
    (ledger as MemoryLedger & Ledger).applyOrderedControl = async () => { resets++; throw new Error("must not be reached"); };
    const broken = { ...s, getObject: async () => { throw Object.assign(new Error("s3 down"), { code: "transient" }); } } as typeof s;
    const a = app(broken, async () => {}, ledger);
    const { deliveryAccount } = await import("../vendor/delivery/prepare.ts");
    const res = await a.request("/v1/delivery/sessions/reset", { method: "POST", headers: { "content-type": "application/json", [IDENTITY_HEADER]: encodeIdentity({ ...identity, control: { epoch: "e".repeat(64), version: "1", digest: "d".repeat(64), kind: "reset" } } as never) }, body: JSON.stringify({ account_id: await deliveryAccount(TENANT), request_id: "r", confirmation: "DELETE ALL SESSIONS" }) });
    expect(res.status).toBe(503);
    expect(resets).toBe(0);
  });
});

describe("inbox object", () => {
  test("is scrubbed at rest, bound to its key, and refuses a re-pointed or tampered object", async () => {
    const s = store();
    const b = await batch([record({ record_id: "r1", payload: { text: "export AWS_SECRET_ACCESS_KEY=abcdEFGHijklMNOPqrstUVWXyz0123456789abcd", ts_ms: 1_761_955_200_000 } })]);
    const prepared = await prepareDelivery(TENANT, b);
    const key = await enqueueAdmission({ store: s, keyRoot: "96bulls", enqueue: async () => {}, nonce: () => "0123456789abcdef" }, TENANT, prepared, T0, 3);
    expect(key).toBe(inboxKey("96bulls", TENANT, "origin-1", prepared.receipt.receiptId, T0, "0123456789abcdef"));
    expect(key.startsWith("96bulls/.admission-inbox/org-a/v1/ws=ws-a/m=srv_")).toBe(true);
    const raw = await text(s, key);
    expect(raw).not.toContain("abcdEFGHijklMNOPqrstUVWXyz0123456789abcd");
    const bytes = new TextEncoder().encode(raw);
    const decoded = await decodeInbox("96bulls", key, bytes);
    expect(decoded?.tenant).toEqual(TENANT);
    expect(decoded?.receivedAtMs).toBe(T0);
    expect(decoded?.journalSeq).toBe(3);
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
    const decoded = await decodeInbox("96bulls", key, zstd(encodeInbox(TENANT, prepared, T0, 0)));
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
    expect((await decodeInbox("96bulls", key, zstd(encodeInbox(TENANT, prepared, T0, 0))))?.prepared.records).toHaveLength(0);
  });
});

describe("privacy journal", () => {
  const j = async (s: ReturnType<typeof store>) => JSON.parse(new TextDecoder().decode(await readAll((await s.getObject(objectKey(journalKey("96bulls", TENANT)))).body)));
  const E = "e".repeat(64);
  const o = (version: number | string) => ({ epoch: E, version: String(version) });
  const run = async (s: ReturnType<typeof store>, c: ReturnType<typeof o>, outcome: "applied" | "replay" | "stale", now = T0) => {
    const begun = await beginPrivacyControl(s, "96bulls", TENANT, c, now);
    await settlePrivacyControl(s, "96bulls", TENANT, c, begun, outcome);
    return begun;
  };

  test("seq advances once per applied control; a replay or stale answer, an abort and a re-run change nothing; per user", async () => {
    const s = store();
    expect(await witnessPrivacyJournal(s, "96bulls", TENANT)).toBe(0);
    expect(await beginPrivacyControl(s, "96bulls", TENANT, o(1), T0)).toBe("new");
    expect(await witnessPrivacyJournal(s, "96bulls", TENANT)).toBe(0);
    expect(await beginPrivacyControl(s, "96bulls", TENANT, o(1), T0 + 1)).toBe("unfinished");
    await settlePrivacyControl(s, "96bulls", TENANT, o(1), "unfinished", "applied");
    expect(await witnessPrivacyJournal(s, "96bulls", TENANT)).toBe(1);
    for (const v of [1, 0]) expect(await run(s, o(v), "replay")).toBe("done");
    expect(await witnessPrivacyJournal(s, "96bulls", TENANT)).toBe(1);
    expect(await run(s, o(2), "stale")).toBe("new"); // superseded: never applies
    expect(await beginPrivacyControl(s, "96bulls", TENANT, o(3), T0)).toBe("new");
    await abortPrivacyControl(s, "96bulls", TENANT, o(3));
    expect((await j(s)).pending).toEqual([]);
    expect(await witnessPrivacyJournal(s, "96bulls", TENANT)).toBe(1);
    expect(await witnessPrivacyJournal(s, "96bulls", { ...TENANT, userId: "user-2" })).toBe(0);
  });

  test("concurrent settlements each count (CAS), and the gate refuses exactly witnesses taken before them", async () => {
    const s = store();
    for (const v of [1, 2, 3, 4, 5]) await beginPrivacyControl(s, "96bulls", TENANT, o(v), T0);
    await Promise.all([1, 2, 3, 4, 5].map((v) => settlePrivacyControl(s, "96bulls", TENANT, o(v), "new", "applied")));
    expect((await j(s)).seq).toBe(5);
    await expect(assertNoLaterPrivacyControl(s, "96bulls", TENANT, 4, T0)).rejects.toMatchObject({ code: "delivery_privacy_superseded" });
    await expect(assertNoLaterPrivacyControl(s, "96bulls", TENANT, 5, T0)).resolves.toBeUndefined();
  });

  test("the gate waits on a fresh pending control and counts a stale one conservatively, keeping it ambiguous", async () => {
    const s = store();
    await beginPrivacyControl(s, "96bulls", TENANT, o(1), T0);
    await expect(assertNoLaterPrivacyControl(s, "96bulls", TENANT, 0, T0 + 1000)).rejects.toMatchObject({ code: "privacy_control_pending" });
    await expect(assertNoLaterPrivacyControl(s, "96bulls", TENANT, 0, T0 + PENDING_TTL_MS + 1)).rejects.toMatchObject({ code: "delivery_privacy_superseded" });
    expect(await j(s)).toMatchObject({ seq: 1, pending: [], ambiguous: [`o:${E}:1`] });
  });

  // Dogpatch round 3, P1-a: R applied in SQL, its Worker died before settling; a consumer counted it.
  const strandedApplied = async (s: ReturnType<typeof store>) => {
    expect(await beginPrivacyControl(s, "96bulls", TENANT, o(1), T0)).toBe("new"); // R: SQL applies, then the Worker dies
    await expect(assertNoLaterPrivacyControl(s, "96bulls", TENANT, 0, T0 + PENDING_TTL_MS + 1)).rejects.toThrow(); // the consumer counts R
    expect((await j(s)).seq).toBe(1);
  };

  test("P1-a causal: an applied-then-stranded R, then 256 later controls, then U, then R replayed: U is admitted", async () => {
    const s = store();
    await strandedApplied(s);
    for (let v = 2; v <= 257; v++) expect(await run(s, o(v), "applied")).toBe("new");
    const witness = await witnessPrivacyJournal(s, "96bulls", TENANT); // U
    expect(witness).toBe(257);
    expect(await run(s, o(1), "stale")).toBe("done"); // R replayed: below the maximum, SQL can only answer stale/replay
    expect(await witnessPrivacyJournal(s, "96bulls", TENANT)).toBe(257);
    await expect(assertNoLaterPrivacyControl(s, "96bulls", TENANT, witness, T0 + PENDING_TTL_MS + 2)).resolves.toBeUndefined();
  });

  test("P1-a causal: an applied-then-stranded R replayed with no later control: SQL says replay, seq does not move, U admitted", async () => {
    const s = store();
    await strandedApplied(s);
    const witness = await witnessPrivacyJournal(s, "96bulls", TENANT);
    expect(await run(s, o(1), "replay")).toBe("ambiguous");
    expect(await j(s)).toMatchObject({ seq: 1, ambiguous: [] });
    await expect(assertNoLaterPrivacyControl(s, "96bulls", TENANT, witness, T0 + PENDING_TTL_MS + 2)).resolves.toBeUndefined();
  });

  test("P1-a inverse: a stranded R that NEVER applied, replayed later, applies then: seq advances and U (witnessed before) is refused", async () => {
    const s = store();
    expect(await beginPrivacyControl(s, "96bulls", TENANT, o(1), T0)).toBe("new"); // R: the Worker dies BEFORE its SQL ran
    await expect(assertNoLaterPrivacyControl(s, "96bulls", TENANT, 0, T0 + PENDING_TTL_MS + 1)).rejects.toThrow();
    const witness = await witnessPrivacyJournal(s, "96bulls", TENANT); // U, after the conservative count, before the real apply
    expect(await run(s, o(1), "applied")).toBe("ambiguous"); // the replay is R's first real application
    expect((await j(s)).seq).toBe(witness + 1);
    await expect(assertNoLaterPrivacyControl(s, "96bulls", TENANT, witness, T0 + PENDING_TTL_MS + 2)).rejects.toMatchObject({ code: "delivery_privacy_superseded" });
  });

  test("a control SQL refused with a SQLSTATE is aborted (no pending left to count later); an ambiguous failure stays pending", async () => {
    const s = store();
    const ledger = new MemoryLedger() as MemoryLedger & Ledger;
    const a = createIngestApp({ identify: (req) => bindingIdentity(req), log: () => {}, deliver: { ledger, acceptanceMode: "buffered", target: { store: s, keyRoot: "96bulls" } }, asyncAdmission: { mode: "on", enqueue: async () => {}, nowMs: () => T0 } });
    const { deliveryAccount } = await import("../vendor/delivery/prepare.ts");
    const account = await deliveryAccount(TENANT);
    const send = (version: string) => a.request("/v1/delivery/sessions/selection", { method: "POST", headers: { "content-type": "application/json",
      [IDENTITY_HEADER]: encodeIdentity({ ...identity, control: { epoch: E, version, digest: "d".repeat(64), kind: "selection" } } as never) },
      body: JSON.stringify({ account_id: account, origin_id: "origin-1", sessions: [{ source: "claude", session_id: "s" }], enabled: true }) });
    ledger.applyOrderedControl = async () => { throw new LedgerUnavailableError("ledger_sqlstate_55000", { db_error_class: "other", db_error_code: "55000", db_error_basis: "code" } as never); };
    expect((await send("1")).status).toBe(503);
    expect((await j(s)).pending).toEqual([]);
    await expect(assertNoLaterPrivacyControl(s, "96bulls", TENANT, 0, T0 + PENDING_TTL_MS + 1)).resolves.toBeUndefined();
    ledger.applyOrderedControl = async () => { throw new LedgerUnavailableError("ledger_connection", { db_error_class: "transport", db_error_code: "ECONNRESET", db_error_basis: "code" } as never); };
    expect((await send("2")).status).toBe(503);
    expect((await j(s)).pending).toHaveLength(1);
  });

  test("an unreadable journal fails closed (never read as seq 0)", async () => {
    const s = store();
    await s.put(objectKey(journalKey("96bulls", TENANT)), new TextEncoder().encode("{}"), { contentType: "application/json" });
    await expect(witnessPrivacyJournal(s, "96bulls", TENANT)).rejects.toThrow("privacy_journal_invalid");
    await expect(assertNoLaterPrivacyControl(s, "96bulls", TENANT, 0, T0)).rejects.toThrow("privacy_journal_invalid");
  });
});

describe("consumer", () => {
  async function queued(s = store(), b?: Awaited<ReturnType<typeof batch>>) {
    const prepared = await prepareDelivery(TENANT, b ?? await batch([record({ record_id: "r1" })]));
    const key = await enqueueAdmission({ store: s, keyRoot: "96bulls", enqueue: async () => {} }, TENANT, prepared, T0, 0);
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

  test("a pending privacy control is a retry with a short delay, never a refusal or an admission", async () => {
    const q = await queued();
    const ledger = new MemoryLedger();
    await beginPrivacyControl(q.s, "96bulls", TENANT, { epoch: "e".repeat(64), version: "1" }, T0 + 5000);
    ledger.prepareBufferStage = async () => { throw new Error("must not be reached"); };
    expect(await admitMessage(deps(q.s, ledger), q.message, 1)).toMatchObject({ action: "retry", code: "privacy_control_pending", delaySeconds: 15 });
  });
});

describe("durable recovery (the queue is not the authority)", () => {
  test("the S3 scan rebuilds exactly the pending, old-enough inbox messages; tombstoned and fresh ones are skipped", async () => {
    const { recoverLines } = await import("../../../scripts/admission-inbox-recover.ts");
    const { tombstoneInbox } = await import("../async-admission.ts");
    const s = store();
    const enqueue = async (id: string, tenant = TENANT) => {
      const prepared = await prepareDelivery(tenant, await batch([record({ record_id: id })], { batch_id: id, ...(tenant !== TENANT ? { account_id: await (await import("../vendor/delivery/prepare.ts")).deliveryAccount(tenant) } : {}) }));
      return enqueueAdmission({ store: s, keyRoot: "96bulls", enqueue: async () => {} }, tenant, prepared, T0, 0);
    };
    const pending = await enqueue("pending");
    const done = await enqueue("done");
    await tombstoneInbox(s, done);
    const other = await enqueue("other-org", { orgId: "org-b", workspaceId: "ws-b", userId: "user-9" });
    const prefixTwin = await enqueue("prefix-twin", { orgId: "org-ab", workspaceId: "ws-a", userId: "user-1" });
    // Journals and outcome markers live under the same prefix and are never listed as work.
    await beginPrivacyControl(s, "96bulls", TENANT, { epoch: "e".repeat(64), version: "1" }, T0);
    const now = Date.now();
    expect((await recoverLines(s, { keyRoot: "96bulls", nowMs: now + 3_600_000, olderThanMs: 60_000 })).map((l) => JSON.parse(l).key).sort()).toEqual([other, pending, prefixTwin].sort());
    // --org org-a is slash-bounded: org-ab's objects are not org-a's.
    expect(await recoverLines(s, { keyRoot: "96bulls", nowMs: now + 3_600_000, olderThanMs: 60_000, orgId: "org-a" })).toEqual([JSON.stringify({ v: 1, kind: "ingest-admission", key: pending })]);
    expect(await recoverLines(s, { keyRoot: "96bulls", nowMs: now + 3_600_000, olderThanMs: 60_000, orgId: "org-ab" })).toEqual([JSON.stringify({ v: 1, kind: "ingest-admission", key: prefixTwin })]);
    // Too fresh: an upload still in its normal queue window is left to the queue.
    expect(await recoverLines(s, { keyRoot: "96bulls", nowMs: now, olderThanMs: 60_000 })).toEqual([]);
  });
});

describe("recovery CLI: nothing reaches a host it should not", () => {
  test("strict --region / --bucket / --org, and the S3 transport itself refuses a host-altering region or bucket with zero fetches", async () => {
    const { parseRecoverArgs } = await import("../../../scripts/admission-inbox-recover.ts");
    const ok = parseRecoverArgs(["--bucket", "synthetic-transcripts-111122223333", "--region", "us-east-1", "--org", "org-a"]);
    expect(ok).toMatchObject({ bucket: "synthetic-transcripts-111122223333", region: "us-east-1", orgId: "org-a", olderThanMs: 3_600_000 });
    for (const region of ["us-east-1@evil.example/x", "us-east-1.evil.example", "127.0.0.1", "localhost", "us-east-1:443", "US-EAST-1", ""])
      expect(() => parseRecoverArgs(["--bucket", "b-bucket", "--region", region])).toThrow("--region");
    for (const bucket of ["evil.example/x", "a@b", "127.0.0.1", "x..y", "UPPER", "a", ""])
      expect(() => parseRecoverArgs(["--bucket", bucket])).toThrow("--bucket");
    expect(() => parseRecoverArgs(["--bucket", "b-bucket", "--org", "org/a"])).toThrow("--org");
    const { createS3Store } = await import("../../shared/vendor.ts");
    let fetches = 0;
    const spy = (async () => { fetches++; return new Response("", { status: 500 }); }) as unknown as typeof fetch;
    for (const [bucket, region] of [["b-bucket", "us-east-1@evil.example/x"], ["b-bucket", "127.0.0.1:8080"], ["evil.example/x", "us-east-1"], ["a@127.0.0.1", "us-east-1"]])
      expect(() => createS3Store({ bucket, region, credentials: { accessKeyId: "k", secretAccessKey: "s" }, fetch: spy } as never)).toThrow();
    expect(fetches).toBe(0);
  });
});

describe("Worker queue dispatch", () => {
  test("buffered mode admits admission messages and still refuses canonical fanout", async () => {
    const { admitQueueBatch } = await import("../worker.ts");
    const s = store();
    const prepared = await prepareDelivery(TENANT, await batch([record({ record_id: "r1" })]));
    const key = await enqueueAdmission({ store: s, keyRoot: "96bulls", enqueue: async () => {} }, TENANT, prepared, Date.now(), 0);
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
