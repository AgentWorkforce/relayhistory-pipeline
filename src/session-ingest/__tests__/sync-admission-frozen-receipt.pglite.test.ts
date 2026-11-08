/**
 * INGEST_SYNC_ADMISSION_FROZEN_RECEIPT (default off): a queued-v1 caller that lands on the synchronous
 * path under frozen-supabase custody gets the admission consumer's frozen legacy-receipt read BEFORE any
 * witness, claim or delivery. relayhistory skips its own pre-forward read for such a caller, so without
 * this a pre-cutover replay is re-claimed and a conflicting digest is delivered instead of refused.
 * Real buffered SQL (PGlite), so the "absent" case proves the unchanged claim and its custody ACK header.
 */
import { afterEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { sqlLedger } from "../ledger-sql.ts";
import { SESSION_BOOTSTRAP_FILES } from "../../hoopdb/session-bootstrap-chain.ts";
import type { Ledger } from "../ledger.ts";
import { bindingIdentity, createIngestApp, encodeIdentity, IDENTITY_HEADER, SIGNATURE_HEADER, signRequest, SYNC_SCOPE, type IngestIdentity } from "../app.ts";
import { ADMISSION_ACCEPT_VALUE, ADMISSION_HEADER, ADMISSION_LEGACY_REPLAY, ADMISSION_QUEUED, syncAdmissionFrozenReceiptMode, type AdmissionMessage, type AsyncAdmissionMode } from "../async-admission.ts";
import { CURRENT_CUSTODY_ACK_HEADER } from "../custody-protocol.ts";
import { prepareDelivery } from "../vendor/delivery/prepare.ts";
import { DELIVERY_LIMITS, type HistoryExportBatch } from "../vendor/delivery/delivery-contracts.ts";
import type { ObjectStore } from "../../shared/vendor.ts";
import { TENANT, batch, record, scratchStore } from "./support.ts";

setDefaultTimeout(60000);
const root = join(import.meta.dir, "../../.."), generation = "a".repeat(64), epoch = "3".repeat(64), sourceIdentity = "c".repeat(64), baseline = "a".repeat(64);
const open: Array<() => Promise<unknown> | void> = [];
afterEach(async () => { for (const close of open.splice(0).reverse()) await close(); });

/** The frozen-custody.pglite.test.ts fixture: every migration, the lazy policy, a sealed frozen copy. */
async function fixture() {
  const db = new PGlite({ extensions: { btree_gin, pg_trgm } }); open.push(() => db.close());
  const read = (p: string) => readFileSync(join(root, p), "utf8");
  await db.exec(read("supabase/tests/supabase_shim.sql"));
  for (const file of SESSION_BOOTSTRAP_FILES) await db.exec(read(file));
  for (const f of readdirSync(join(root, "supabase/migrations")).filter((f) => f.endsWith(".sql")).sort()) await db.exec(read("supabase/migrations/" + f));
  await db.exec("CREATE TABLE sessions.__96bulls_role_validation(name text PRIMARY KEY,generation text,status text);INSERT INTO sessions.__96bulls_role_validation VALUES('session_directory_search_worker','" + generation + "','pass')");
  const owner = async (name: string, p: unknown) => (await db.query("SELECT sessions." + name + "($1::jsonb) AS r", [JSON.stringify(p)])).rows[0];
  await owner("ingest_lazy_policy_set", { baselineMode: "lazy-current", generation, sourceIdentitySha256: sourceIdentity, controlEpoch: epoch, privacyBaselineSha256: baseline, retentionContractSha256: "b".repeat(64), sourceProofSha256: "d".repeat(64), validUntil: new Date(Date.now() + 3600000).toISOString(), issuer: "relayhistory-cloud" });
  await owner("ingest_receipt_seat_initialize", { protocol: "delivery-acceptance-floor-v1", controlEpoch: epoch, capturedAt: "2026-11-04T00:00:00.000000Z", sourceSequences: ["sessions.delivery_catalog_acceptance_seq", "sessions.delivery_projection_acceptance_seq"].map((sequenceRegclass, i) => ({ sequenceRegclass, sequenceOid: String(i + 1), allocatedHighWater: "9007199254740993", isCalled: true, incrementBy: "1", cycle: false })), allocatedHighWater: "9007199254740993", generation, sourceIdentitySha256: sourceIdentity, sourceProofSha256: "d".repeat(64) });
  await db.query("INSERT INTO sessions.ingest_frozen_source_completeness(source_identity_sha256,control_epoch,privacy_baseline_sha256,org_count,record_rows,manifest_sha256,neon_snapshot) VALUES($1,$2,$3,0,0,$4,'{}')", [sourceIdentity, epoch, baseline, "e".repeat(64)]);
  const run = async (s: string, p: unknown[] = []) => { await db.exec("BEGIN;SET LOCAL ROLE bulls96_session_ingest"); try { const r = await db.query(s, p as never); await db.exec("COMMIT"); return r as never; } catch (e) { await db.exec("ROLLBACK"); throw e; } };
  const fs = scratchStore(); open.push(fs.done);
  /** A legacy (pre-cutover) receipt for this batch, stored with `digest`. */
  const legacy = async (b: HistoryExportBatch, digest: string, withReceiptId = true) => {
    const receipt = { protocolVersion: 1, batchId: b.batch_id, acceptanceLevel: "durable", acceptedRevisionIds: b.records.map((r) => r.revision_id), unsupportedRevisionIds: [], ...(withReceiptId ? { receiptId: "rhr_legacy" } : {}) };
    await db.query("INSERT INTO sessions.ingest_frozen_source_receipts VALUES($1,$2,$3,$4,$5,$6)", [TENANT.orgId, TENANT.workspaceId, b.origin_id, b.batch_id, digest, JSON.stringify(receipt)]);
    return receipt;
  };
  return { db, run, store: fs.store, ledger: sqlLedger(run, { acceptanceMode: "buffered", expectedGeneration: generation }), legacy };
}

/** Every ledger method call, by name, in order; `drop` hides one method (a ledger without it). */
function spied(ledger: Ledger, drop?: keyof Ledger) {
  const calls: string[] = [];
  const proxy = new Proxy(ledger, {
    get(t, k, r) {
      if (k === drop) return undefined;
      const v = Reflect.get(t, k, r);
      return typeof v === "function" ? (...a: unknown[]) => { calls.push(String(k)); return v.apply(t, a); } : v;
    },
  });
  return { ledger: proxy, calls };
}

/** The pre-body journal witness fails (S3 read error on the privacy journal): the request falls back to the synchronous path. */
function journalBroken(store: ObjectStore): ObjectStore {
  return new Proxy(store, {
    get(t, k, r) {
      const v = Reflect.get(t, k, r);
      if (k === "getObject") return (key: unknown, ...rest: unknown[]) => String(key).endsWith("privacy-journal.json")
        ? Promise.reject(Object.assign(new Error("s3 down"), { code: "transient" })) : (v as (...a: unknown[]) => unknown).apply(t, [key, ...rest]);
      return typeof v === "function" ? v.bind(t) : v;
    },
  });
}

type Mode = "off" | "on" | "on-witness-failed" | "drain";
interface Setup { mode?: Mode; flag?: "off" | "on"; admission?: boolean; drop?: keyof Ledger }
async function setup({ mode = "off", flag = "on", admission = true, drop }: Setup) {
  const f = await fixture();
  const s = spied(f.ledger, drop);
  const logs: Record<string, unknown>[] = [];
  const sent: AdmissionMessage[] = [];
  const asyncMode: AsyncAdmissionMode = mode === "on-witness-failed" ? "on" : mode;
  const app = createIngestApp({
    identify: (req) => bindingIdentity(req),
    log: (x) => logs.push(x),
    custodySource: "frozen-supabase",
    deliver: { ledger: s.ledger, acceptanceMode: "buffered", target: { store: mode === "on-witness-failed" ? journalBroken(f.store) : f.store, keyRoot: "96bulls" } },
    ...(asyncMode !== "off" ? { asyncAdmission: { mode: asyncMode, enqueue: async (m: AdmissionMessage) => { sent.push(m); } } } : {}),
    ...(flag === "on" ? { syncAdmissionFrozenReceipt: "on" as const } : {}),
  });
  const identity: IngestIdentity = { ...TENANT, scopes: [SYNC_SCOPE], ...(admission ? { admission: ADMISSION_ACCEPT_VALUE } : {}) };
  const post = (b: HistoryExportBatch) => app.request("/v1/delivery/batches", { method: "POST", headers: { "content-type": "application/json", [IDENTITY_HEADER]: encodeIdentity(identity) }, body: JSON.stringify({ protocolVersion: 1, batch: b }) });
  const limits = async () => (await app.request("/v1/delivery/limits", { headers: { [IDENTITY_HEADER]: encodeIdentity(identity) } })).json();
  return { ...f, ...s, logs, sent, post, limits };
}
const digestOf = async (b: HistoryExportBatch) => (await prepareDelivery(TENANT, b)).batchDigest;
const route = (logs: Record<string, unknown>[]) => logs.filter((l) => l.route === "delivery.batches");

describe.each(["off", "on-witness-failed", "drain"] as const)("flag on, queued-v1 identity, INGEST_ASYNC_ADMISSION %s: synchronous path", (mode) => {
  test("(a) stored legacy receipt, same digest: 200 with that receipt; no witness, claim, delivery or custody ACK", async () => {
    const t = await setup({ mode });
    const b = await batch([record({ record_id: "r1" })], { batch_id: "legacy-same" });
    const receipt = await t.legacy(b, await digestOf(b));
    const res = await t.post(b);
    expect(res.status).toBe(200);
    const { limits, ...body } = (await res.json()) as Record<string, unknown>;
    expect(body).toEqual(receipt);
    expect(body.receiptId).toBe("rhr_legacy");
    expect(limits).toEqual(await t.limits());
    expect(limits).toMatchObject({ ...DELIVERY_LIMITS });
    expect(t.calls).toEqual(["frozenReceipt"]);
    expect(res.headers.get(CURRENT_CUSTODY_ACK_HEADER)).toBeNull();
    expect(res.headers.get(ADMISSION_HEADER)).toBe('{"version":1,"state":"legacy-replay"}');
    expect(res.headers.get(ADMISSION_HEADER)).toBe(ADMISSION_LEGACY_REPLAY);
    expect(t.sent).toHaveLength(0);
    expect(route(t.logs).map((l) => l.outcome)).toEqual(["legacy_replay"]);
  });

  test("(a2) stored legacy receipt without receiptId, same digest: 200 whose receiptId is the prepared receipt's; no witness, claim, delivery or custody ACK", async () => {
    const t = await setup({ mode });
    const b = await batch([record({ record_id: "r1" })], { batch_id: "legacy-no-receipt-id" });
    const stored = await t.legacy(b, await digestOf(b), false);
    expect("receiptId" in stored).toBe(false);
    const expected = (await prepareDelivery(TENANT, b)).receipt.receiptId;
    expect(typeof expected).toBe("string");
    const res = await t.post(b);
    expect(res.status).toBe(200);
    const { limits, ...body } = (await res.json()) as Record<string, unknown>;
    expect(body).toEqual({ ...stored, receiptId: expected });
    expect(limits).toEqual(await t.limits());
    expect(t.calls).toEqual(["frozenReceipt"]);
    expect(res.headers.get(CURRENT_CUSTODY_ACK_HEADER)).toBeNull();
    expect(res.headers.get(ADMISSION_HEADER)).toBe('{"version":1,"state":"legacy-replay"}');
    expect(t.sent).toHaveLength(0);
    expect(route(t.logs).map((l) => l.outcome)).toEqual(["legacy_replay"]);
  });

  test("(b) stored legacy receipt, other digest: 409 delivery_conflict (batch_id); no witness, claim or delivery", async () => {
    const t = await setup({ mode });
    const b = await batch([record({ record_id: "r1" })], { batch_id: "legacy-other" });
    await t.legacy(b, "9".repeat(64));
    const res = await t.post(b);
    expect(res.status).toBe(409);
    const body = (await res.json()) as { error: { code: string; conflict: Record<string, unknown> } };
    expect(body.error.code).toBe("delivery_conflict");
    expect(body.error.conflict).toEqual({ type: "batch_id", originId: b.origin_id, batchId: b.batch_id, submittedDigest: await digestOf(b), currentDigest: "9".repeat(64) });
    expect(t.calls).toEqual(["frozenReceipt"]);
    expect(res.headers.get(CURRENT_CUSTODY_ACK_HEADER)).toBeNull();
    expect(res.headers.get(ADMISSION_HEADER)).toBeNull();
    expect(t.sent).toHaveLength(0);
    expect(route(t.logs).map((l) => l.outcome)).toEqual(["delivery_conflict"]);
  });

  test("(c) no stored receipt: the unchanged path (frozen witness, claim, delivery, custody ACK header)", async () => {
    const t = await setup({ mode });
    const b = await batch([record({ record_id: "r1" })], { batch_id: "legacy-absent" });
    const res = await t.post(b);
    expect(res.status).toBe(200);
    expect(t.calls[0]).toBe("frozenReceipt");
    expect(t.calls).toContain("frozenWitness");
    expect(t.calls.length).toBeGreaterThan(2);
    expect(JSON.parse(res.headers.get(CURRENT_CUSTODY_ACK_HEADER)!)).toMatchObject({ version: 1, sourceControlVersion: "0" });
    expect(res.headers.get(ADMISSION_HEADER)).toBeNull();
    const { limits: _l, ...body } = (await res.json()) as Record<string, unknown>;
    expect(body).toEqual((await prepareDelivery(TENANT, b)).receipt as never);
    expect(t.sent).toHaveLength(0);
  });
});

test("flag on, INGEST_ASYNC_ADMISSION on with a journal witness: the queued path is unchanged (queued header, no ledger call, even with a stored receipt)", async () => {
  const t = await setup({ mode: "on" });
  const b = await batch([record({ record_id: "r1" })], { batch_id: "queued" });
  await t.legacy(b, await digestOf(b));
  const res = await t.post(b);
  expect(res.status).toBe(200);
  expect(res.headers.get(ADMISSION_HEADER)).toBe(ADMISSION_QUEUED);
  expect(res.headers.get(ADMISSION_HEADER)).toBe('{"version":1,"state":"queued"}');
  expect(res.headers.get(CURRENT_CUSTODY_ACK_HEADER)).toBeNull();
  expect(t.calls).toEqual([]);
  expect(t.sent).toHaveLength(1);
});

test("flag on, identity WITHOUT admission: frozenReceipt is never read; the path is unchanged", async () => {
  const t = await setup({ admission: false });
  const b = await batch([record({ record_id: "r1" })], { batch_id: "plain" });
  await t.legacy(b, "9".repeat(64));
  const res = await t.post(b);
  expect(res.status).toBe(200);
  expect(t.calls).not.toContain("frozenReceipt");
  expect(t.calls[0]).toBe("frozenWitness");
  expect(res.headers.get(CURRENT_CUSTODY_ACK_HEADER)).not.toBeNull();
  expect(res.headers.get(ADMISSION_HEADER)).toBeNull();
});

test("flag off (default), queued-v1 identity, other stored digest: today's behaviour (no receipt read; the batch is claimed and delivered)", async () => {
  const t = await setup({ flag: "off" });
  const b = await batch([record({ record_id: "r1" })], { batch_id: "flag-off" });
  await t.legacy(b, "9".repeat(64));
  const res = await t.post(b);
  expect(res.status).toBe(200);
  expect(t.calls).not.toContain("frozenReceipt");
  expect(t.calls[0]).toBe("frozenWitness");
  expect(res.headers.get(CURRENT_CUSTODY_ACK_HEADER)).not.toBeNull();
  expect(res.headers.get(ADMISSION_HEADER)).toBeNull();
});

test("flag on, ledger without frozenReceipt, queued-v1 identity: 503 custody_source_unavailable, no witness or claim", async () => {
  const t = await setup({ drop: "frozenReceipt" });
  const res = await t.post(await batch([record({ record_id: "r1" })], { batch_id: "no-receipt" }));
  expect(res.status).toBe(503);
  expect(((await res.json()) as { error: { code: string } }).error.code).toBe("delivery_unavailable");
  expect(t.calls).toEqual([]);
  expect(route(t.logs)[0]).toMatchObject({ outcome: "delivery_unavailable", stage: "fence", error_code: "custody_source_unavailable" });
});

describe("INGEST_SYNC_ADMISSION_FROZEN_RECEIPT settings", () => {
  const full = { HYPERDRIVE: { connectionString: "postgres://x" }, INGEST_FANOUT: {} as never, S3_ACCESS_KEY_ID: "k", S3_SECRET_ACCESS_KEY: "s", INGEST_SIGNING_KEY: "sign", INGEST_S3_BUCKET: "b", INGEST_KMS_KEY_ARN: "arn" };
  const buffered = { ...full, INGEST_ACCEPTANCE_MODE: "buffered", INGEST_EXPECT_GENERATION: generation, INGEST_INLINE_MAX_SESSIONS: "0" };

  test("parses off/on only (default off); anything else refuses", () => {
    expect(syncAdmissionFrozenReceiptMode(undefined)).toBe("off");
    expect(syncAdmissionFrozenReceiptMode("")).toBe("off");
    expect(syncAdmissionFrozenReceiptMode(" OFF ")).toBe("off");
    expect(syncAdmissionFrozenReceiptMode(" On ")).toBe("on");
    for (const bad of ["yes", "true", "1", "drain"]) expect(() => syncAdmissionFrozenReceiptMode(bad)).toThrow(/INGEST_SYNC_ADMISSION_FROZEN_RECEIPT/);
  });

  test("Worker settings: absent by default, present only for on, an invalid value throws at startup", async () => {
    const { ingestSettings } = await import("../worker.ts");
    expect((await ingestSettings(buffered))?.syncAdmissionFrozenReceipt).toBeUndefined();
    expect((await ingestSettings({ ...buffered, INGEST_SYNC_ADMISSION_FROZEN_RECEIPT: "off" }))?.syncAdmissionFrozenReceipt).toBeUndefined();
    expect((await ingestSettings({ ...buffered, INGEST_SYNC_ADMISSION_FROZEN_RECEIPT: "on" }))?.syncAdmissionFrozenReceipt).toBe(true);
    await expect(ingestSettings({ ...buffered, INGEST_SYNC_ADMISSION_FROZEN_RECEIPT: "yes" })).rejects.toThrow(/INGEST_SYNC_ADMISSION_FROZEN_RECEIPT/);
  });

  test.each(["on", undefined] as const)("Worker wiring (signed request through ingestApp), flag %s", async (flag) => {
    const { ingestApp, ingestSettings } = await import("../worker.ts");
    const f = await fixture();
    const env = { ...buffered, INGEST_CUSTODY_SOURCE: "frozen-supabase", ...(flag ? { INGEST_SYNC_ADMISSION_FROZEN_RECEIPT: flag } : {}) };
    const settings = (await ingestSettings(env))!;
    const app = ingestApp(env, settings, f.run, { store: f.store, keyRoot: "96bulls" });
    const b = await batch([record({ record_id: "r1" })], { batch_id: "wired" });
    await f.legacy(b, "9".repeat(64));
    const identity = encodeIdentity({ ...TENANT, scopes: [SYNC_SCOPE], admission: ADMISSION_ACCEPT_VALUE });
    const body = new TextEncoder().encode(JSON.stringify({ protocolVersion: 1, batch: b }));
    const res = await app.request("/v1/delivery/batches", { method: "POST", body, headers: { "content-type": "application/json", [IDENTITY_HEADER]: identity,
      [SIGNATURE_HEADER]: signRequest("sign", Math.floor(Date.now() / 1000), "POST", "/v1/delivery/batches", identity, body) } });
    // on: the stored conflicting legacy receipt refuses; off: today's path claims and delivers.
    expect(res.status).toBe(flag === "on" ? 409 : 200);
  });
});
