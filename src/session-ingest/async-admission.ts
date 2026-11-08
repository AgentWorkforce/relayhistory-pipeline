/**
 * S3-first async admission (docs/design/s3-first-async-ingest.md), behind
 * INGEST_ASYNC_ADMISSION (default off).
 *
 *   request  (no Supabase): authenticate + parse + prepare (CPU) -> one create-only,
 *            scrubbed inbox object in S3 -> one INGEST_ADMISSION queue send -> 200 receipt
 *   consumer (queue):       read the inbox -> frozen custody / legacy receipt -> the
 *            unchanged buffered claim -> upload-time privacy gate -> stage intent -> PUT ->
 *            receipt ACK (deliverPrepared) -> outcome marker -> inbox content tombstoned
 *
 * A 2xx from the async path means DURABLY STORED, NOT YET ADMITTED. The batch is later
 * admitted (the same buffered SQL, idempotent by batch digest) or explicitly refused, with
 * the outcome readable at GET /v1/delivery/admission. Nothing in the inbox is readable by
 * any session read path: no SQL pointer names it, and every reader resolves objects only
 * through SQL (buffered outbox) or canonical manifests.
 *
 * The privacy journal closes the one gap the claim alone cannot: a reset, or a re-enable of
 * a disabled session, that STARTED after an upload but is applied before its admission.
 * Every such control first raises the user's journal watermark (S3 CAS, before its SQL
 * call); the consumer reads it AFTER its claim and refuses an upload that started before
 * the watermark. A control applied after the claim is caught by the existing ACK recheck
 * (original epochs and reset count), and one applied after the ACK erases as today.
 */
import { createHash } from "node:crypto";
import { rawZstdFrame, scrubRecords } from "@rhc/lib/pipeline/raw-chunks.ts";
import { isObjectStoreError, objectKey, readAll, type ObjectStore } from "../shared/vendor.ts";
import { layoutComponent } from "../shared/layout-component.ts";
import { fitForScrub } from "./oversize.ts";
import { assertTenantSegment, liveMachineId } from "./writer.ts";
import type { LedgerTenant } from "./ledger.ts";
import { DELIVERY_KINDS, DeliveryError, MAX_DELIVERY_RECORDS } from "./vendor/delivery/delivery-contracts.ts";
import { digest, type PreparedDelivery, type PreparedDeliveryRecord } from "./vendor/delivery/prepare.ts";

/** INGEST_ASYNC_ADMISSION. off: today's synchronous path only, and admission messages wait.
 * on: async ACK for callers that opt in; journal written; consumer admits.
 * drain: no new async ACK; journal still written and the consumer still admits (the rollback step). */
export type AsyncAdmissionMode = "off" | "on" | "drain";

export function asyncAdmissionMode(raw: string | undefined): AsyncAdmissionMode {
  const v = raw?.trim().toLowerCase() ?? "";
  if (v === "" || v === "off") return "off";
  if (v === "on" || v === "drain") return v;
  throw new Error("session ingest: INGEST_ASYNC_ADMISSION must be off, on or drain");
}

/** relayhistory opts in per request: it then accepts ADMISSION_HEADER instead of a custody ACK. */
export const ADMISSION_ACCEPT_HEADER = "x-relay-ingest-admission";
export const ADMISSION_ACCEPT_VALUE = "queued-v1";
/** On an async 2xx: exactly this JSON, and never a custody ACK header. */
export const ADMISSION_HEADER = "x-relay-admission";
export const ADMISSION_QUEUED = JSON.stringify({ version: 1, state: "queued" });

export const INBOX_NAMESPACE = "admission-inbox";
/** An inbox older than this is refused (`admission_expired`) rather than admitted: the
 * prefix lifecycle (7 days) must never expire a journal an unadmitted upload still needs. */
export const INBOX_MAX_AGE_MS = 6 * 24 * 3600 * 1000;
const INBOX_KIND = "ingest-admission-inbox";
/** Same framing as an inbox (one zstd frame), so a re-read decodes it as terminal. */
const TOMBSTONE = rawZstdFrame(new TextEncoder().encode(JSON.stringify({ v: 1, kind: "ingest-admission-tombstone" }) + "\n"));

export interface AdmissionMessage {
  v: 1;
  kind: "ingest-admission";
  key: string;
}

const encoder = new TextEncoder();
const decoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: false });
const sha256Hex = (value: string) => createHash("sha256").update(value).digest("hex");
const HEX64 = /^[0-9a-f]{64}$/;

/** A consumer-side terminal refusal: written as the batch's refused outcome, never retried. */
export class AdmissionRefused extends DeliveryError {
  constructor(code: "delivery_privacy_superseded" | "admission_expired" | "admission_invalid", message: string) {
    super(code, 409, message);
  }
}

function assertInboxTenant(keyRoot: string, orgId: string) {
  assertTenantSegment(keyRoot, orgId);
  // The lifecycle rule expires `<keyRoot>/admission-inbox/`; an org of that name would share it.
  if (orgId === INBOX_NAMESPACE) throw new DeliveryError("delivery_unavailable", 503, "Durable delivery is temporarily unavailable");
}

/** `<keyRoot>/admission-inbox/<org>/v1/ws=<ws>/m=srv_<…>/` — inside the srv-writer `…/v1/ws=*\/m=srv_*` grant. */
function inboxDir(keyRoot: string, tenant: LedgerTenant, machine: string): string {
  return `${keyRoot}/${INBOX_NAMESPACE}/${tenant.orgId}/v1/ws=${layoutComponent(tenant.workspaceId)}/m=${machine}`;
}
const receiptHex = (receiptId: string) => {
  if (!/^rhr_[0-9a-f]{64}$/.test(receiptId)) throw new AdmissionRefused("admission_invalid", "Invalid receipt id");
  return receiptId.slice(4);
};
export function receiptDir(keyRoot: string, tenant: LedgerTenant, originId: string, receiptId: string): string {
  return `${inboxDir(keyRoot, tenant, liveMachineId(tenant.workspaceId, originId, tenant.userId))}/r=${receiptHex(receiptId)}`;
}
/** One object per request attempt: idempotency is the claim's (batch id + digest), never the key's. */
export function inboxKey(keyRoot: string, tenant: LedgerTenant, originId: string, receiptId: string, receivedAtMs: number, nonce: string): string {
  if (!/^[0-9a-f]{16}$/.test(nonce) || !Number.isSafeInteger(receivedAtMs) || receivedAtMs < 0) throw new Error("admission_key_invalid");
  return `${receiptDir(keyRoot, tenant, originId, receiptId)}/a=${String(receivedAtMs).padStart(15, "0")}-${nonce}.ndjson.zst`;
}
export const outcomeKey = (dir: string, outcome: "admitted" | "refused") => `${dir}/outcome-${outcome}.json`;
/** Per (org, workspace, user): the privacy journal every reset / re-enable raises first. */
export function journalKey(keyRoot: string, tenant: LedgerTenant): string {
  const machine = `srv_${sha256Hex(JSON.stringify(["admission-user-v1", tenant.workspaceId, tenant.userId])).slice(0, 32)}`;
  return `${inboxDir(keyRoot, tenant, machine)}/privacy-journal.json`;
}

// ---- inbox object ----

interface InboxHeader {
  v: 1;
  kind: typeof INBOX_KIND;
  receivedAtMs: number;
  tenant: LedgerTenant;
  originId: string;
  batchId: string;
  batchDigest: string;
  receipt: PreparedDelivery["receipt"];
  /**
   * Each record's identity, never scrubbed: the claim, the receipt and the custody witness are keyed
   * by these exact values (as SQL holds them), and a scrubber rule matching a client id must never
   * re-key an acknowledged record. Payloads travel separately, scrubbed.
   */
  records: [recordId: string, revisionId: string, revision: number, digest: string, kind: string, source: string, sessionId: string | null, operation: "upsert" | "delete"][];
}

/** Header line (ids only) + one scrubbed payload line per prepared record, as one zstd frame. */
export function encodeInbox(tenant: LedgerTenant, prepared: PreparedDelivery, receivedAtMs: number): Uint8Array {
  const header: InboxHeader = { v: 1, kind: INBOX_KIND, receivedAtMs, tenant: { orgId: tenant.orgId, workspaceId: tenant.workspaceId, userId: tenant.userId },
    originId: prepared.originId, batchId: prepared.batchId, batchDigest: prepared.batchDigest, receipt: prepared.receipt,
    records: prepared.records.map((r) => [r.record_id, r.revision_id, r.revision, r.digest, r.kind, r.source, r.session_id ?? null, r.operation]) };
  // Payloads are already the service form (servicePayload); the pipeline scrubber runs once more so
  // nothing unscrubbed is at rest, exactly as a staged batch (staging.ts stageGroups).
  const body = prepared.records.length ? prepared.records.map((r) => JSON.stringify({ p: fitForScrub(r).payload })).join("\n") + "\n" : "";
  const scrubbed = scrubRecords(encoder.encode(body)).scrubbed;
  const prefix = encoder.encode(JSON.stringify(header) + "\n");
  const raw = new Uint8Array(prefix.byteLength + scrubbed.byteLength);
  raw.set(prefix);
  raw.set(scrubbed, prefix.byteLength);
  return rawZstdFrame(raw);
}

const isObject = (v: unknown): v is Record<string, unknown> => v !== null && typeof v === "object" && !Array.isArray(v);
const ident = (v: unknown): v is string => typeof v === "string" && v.length > 0 && encoder.encode(v).length <= 512 && !/[\x00-\x1f\x7f]/.test(v);
const bad = (): never => { throw new AdmissionRefused("admission_invalid", "Admission inbox object is malformed"); };

function inboxRecord(id: unknown, line: unknown, originId: string): PreparedDeliveryRecord {
  if (!Array.isArray(id) || id.length !== 8 || !isObject(line) || Object.keys(line).length !== 1 || !Object.hasOwn(line, "p")) bad();
  const [record_id, revision_id, revision, digest, kind, source, session_id, operation] = id as unknown[];
  const payload = (line as Record<string, unknown>).p;
  if (!ident(record_id) || !ident(revision_id) || typeof revision !== "number" || !Number.isSafeInteger(revision) || revision < 1 ||
      typeof digest !== "string" || !HEX64.test(digest) || !DELIVERY_KINDS.includes(kind as never) || !ident(source) ||
      !(session_id === null || ident(session_id)) ||
      !((operation === "delete" && payload === null) || (operation === "upsert" && isObject(payload)))) bad();
  const record = { schema_version: 1, origin_id: originId, record_id, revision_id, revision, kind, source, session_id, operation, payload, digest } as unknown as PreparedDeliveryRecord;
  // Already scrubbed once at rest: fit again for the stage's second scrub (oversize.ts fitLine).
  return fitForScrub(record);
}

export interface DecodedInbox { tenant: LedgerTenant; prepared: PreparedDelivery; receivedAtMs: number }

/** Decode and bind an inbox object to the key it was read from. `null`: tombstoned (already terminal). */
export async function decodeInbox(keyRoot: string, key: string, raw: Uint8Array): Promise<DecodedInbox | null> {
  let text: string;
  try { text = decoder.decode(raw); } catch { return bad(); }
  const newline = text.indexOf("\n");
  if (newline < 0) bad();
  let header: InboxHeader;
  try { header = JSON.parse(text.slice(0, newline)); } catch { return bad(); }
  if (isObject(header) && (header as unknown as Record<string, unknown>).kind === "ingest-admission-tombstone") return null;
  const h = header as unknown as Record<string, unknown>;
  if (!isObject(h) || h.v !== 1 || h.kind !== INBOX_KIND || !Number.isSafeInteger(h.receivedAtMs) || !isObject(h.tenant) || !isObject(h.receipt) ||
      !ident(h.originId) || !ident(h.batchId) || typeof h.batchDigest !== "string" || !HEX64.test(h.batchDigest) ||
      !Array.isArray(h.records) || (h.records as unknown[]).length > MAX_DELIVERY_RECORDS) bad();
  const t = h.tenant as Record<string, unknown>;
  if (!ident(t.orgId) || typeof t.workspaceId !== "string" || !ident(t.userId)) bad();
  try { assertInboxTenant(keyRoot, t.orgId as string); } catch { bad(); }
  const tenant = { orgId: t.orgId as string, workspaceId: t.workspaceId as string, userId: t.userId as string };
  const receipt = h.receipt as Record<string, unknown>;
  // The receipt is content-addressed: recompute it, so a header can never re-bind content to another batch or tenant.
  const expected = "rhr_" + await digest(["relayhistory-receipt-v1", tenant.orgId, tenant.workspaceId, h.originId, h.batchId, h.batchDigest]);
  if (receipt.receiptId !== expected || receipt.batchId !== h.batchId || receipt.protocolVersion !== 1 || receipt.acceptanceLevel !== "durable" ||
      !Array.isArray(receipt.acceptedRevisionIds) || !Array.isArray(receipt.unsupportedRevisionIds)) bad();
  // The key names this tenant, origin, receipt and upload time: a message cannot point one tenant's object at another.
  const dir = receiptDir(keyRoot, tenant, h.originId as string, expected);
  const m = key.startsWith(dir + "/a=") ? /^(\d{15})-[0-9a-f]{16}\.ndjson\.zst$/.exec(key.slice(dir.length + 3)) : null;
  if (!m || Number(m[1]) !== h.receivedAtMs) bad();
  const lines = text.slice(newline + 1).split("\n").filter((l) => l.length > 0);
  const ids = h.records as unknown[];
  if (lines.length !== ids.length) bad();
  const records = lines.map((l, i) => { try { return inboxRecord(ids[i], JSON.parse(l), h.originId as string); } catch (e) { if (e instanceof AdmissionRefused) throw e; return bad(); } });
  if (new Set(records.map((r) => r.revision_id)).size !== records.length) bad();
  const accepted = new Set(receipt.acceptedRevisionIds as unknown[]);
  if (records.some((r) => !accepted.has(r.revision_id))) bad();
  return { tenant, receivedAtMs: h.receivedAtMs as number,
    prepared: { originId: h.originId as string, batchId: h.batchId as string, batchDigest: h.batchDigest as string, receipt: receipt as unknown as PreparedDelivery["receipt"], records } };
}

// ---- request path ----

export interface AsyncAdmissionTarget {
  store: ObjectStore;
  keyRoot: string;
  enqueue: (message: AdmissionMessage) => Promise<void>;
  nonce?: () => string;
}

/**
 * The request path: durable inbox PUT, then the queue send. Either failing is a retryable 503
 * (nothing was acknowledged; a retry writes a new attempt object and the claim dedupes).
 */
export async function enqueueAdmission(target: AsyncAdmissionTarget, tenant: LedgerTenant, prepared: PreparedDelivery, receivedAtMs: number): Promise<string> {
  assertInboxTenant(target.keyRoot, tenant.orgId);
  const nonce = (target.nonce ?? (() => crypto.randomUUID().replace(/-/g, "").slice(0, 16)))();
  const key = inboxKey(target.keyRoot, tenant, prepared.originId, prepared.receipt.receiptId, receivedAtMs, nonce);
  await target.store.put(objectKey(key), encodeInbox(tenant, prepared, receivedAtMs), { ifNoneMatch: true, contentType: "application/zstd" });
  try {
    await target.enqueue({ v: 1, kind: "ingest-admission", key });
  } catch (error) {
    // Unacknowledged: retire the content now (best effort; the prefix lifecycle is the backstop).
    await target.store.put(objectKey(key), TOMBSTONE, { contentType: "application/zstd" }).catch(() => {});
    throw error;
  }
  return key;
}

// ---- privacy journal ----

interface Journal { v: 1; watermarkMs: number }
const JOURNAL_ATTEMPTS = 6;

async function readJournal(store: ObjectStore, key: string): Promise<{ journal: Journal | null; version?: string }> {
  let got;
  try {
    got = await store.getObject(objectKey(key));
  } catch (error) {
    if (isObjectStoreError(error, "not_found")) return { journal: null };
    throw error;
  }
  const value = JSON.parse(decoder.decode(await readAll(got.body))) as Journal;
  if (!isObject(value) || value.v !== 1 || !Number.isSafeInteger(value.watermarkMs)) throw new Error("privacy_journal_invalid");
  return { journal: value, version: got.meta.version as unknown as string };
}

/**
 * Raise the user's watermark to `startedAtMs` (monotonic max, compare-and-swap). Called BEFORE a
 * reset or a re-enable reaches SQL; a failure must refuse the control (fail closed).
 */
export async function raisePrivacyWatermark(store: ObjectStore, keyRoot: string, tenant: LedgerTenant, startedAtMs: number): Promise<void> {
  assertInboxTenant(keyRoot, tenant.orgId);
  const key = journalKey(keyRoot, tenant);
  for (let attempt = 0; attempt < JOURNAL_ATTEMPTS; attempt++) {
    const { journal, version } = await readJournal(store, key);
    if (journal && journal.watermarkMs >= startedAtMs) return;
    const body = encoder.encode(JSON.stringify({ v: 1, watermarkMs: startedAtMs } satisfies Journal));
    try {
      await store.put(objectKey(key), body, version ? { ifMatch: version as never, contentType: "application/json" } : { ifNoneMatch: true, contentType: "application/json" });
      return;
    } catch (error) {
      if (!isObjectStoreError(error, "precondition_failed") && !isObjectStoreError(error, "already_exists")) throw error;
    }
  }
  throw new Error("privacy_journal_contended");
}

/** The consumer's post-claim gate: refuse an upload that started before the user's latest reset / re-enable began. */
export async function assertNoLaterPrivacyControl(store: ObjectStore, keyRoot: string, tenant: LedgerTenant, receivedAtMs: number): Promise<void> {
  const { journal } = await readJournal(store, journalKey(keyRoot, tenant));
  if (journal && journal.watermarkMs >= receivedAtMs)
    throw new AdmissionRefused("delivery_privacy_superseded", "A session reset or re-enable started after this upload; it was not admitted");
}

// ---- outcomes ----

export interface AdmissionOutcome { v: 1; state: "admitted" | "refused"; code: string; atMs: number }

export async function writeOutcome(store: ObjectStore, dir: string, outcome: AdmissionOutcome): Promise<void> {
  await store.put(objectKey(outcomeKey(dir, outcome.state)), encoder.encode(JSON.stringify(outcome)), { contentType: "application/json" });
}

async function readOutcome(store: ObjectStore, dir: string, state: "admitted" | "refused"): Promise<AdmissionOutcome | null> {
  try {
    const v = JSON.parse(decoder.decode(await readAll((await store.getObject(objectKey(outcomeKey(dir, state)))).body))) as AdmissionOutcome;
    return isObject(v) && v.v === 1 && v.state === state && typeof v.code === "string" ? v : null;
  } catch (error) {
    if (isObjectStoreError(error, "not_found")) return null;
    throw error;
  }
}

/** GET /v1/delivery/admission: admitted wins over refused (a later attempt may admit what an earlier one could not). */
export async function admissionStatus(store: ObjectStore, keyRoot: string, tenant: LedgerTenant, originId: string, receiptId: string) {
  assertInboxTenant(keyRoot, tenant.orgId);
  const dir = receiptDir(keyRoot, tenant, originId, receiptId);
  const admitted = await readOutcome(store, dir, "admitted");
  if (admitted) return { receiptId, state: "admitted" as const };
  const refused = await readOutcome(store, dir, "refused");
  if (refused) return { receiptId, state: "refused" as const, code: refused.code };
  return { receiptId, state: "pending" as const };
}

/** Retire the inbox content after a terminal outcome: the old version becomes noncurrent (the bucket's 1-day purge). */
export async function tombstoneInbox(store: ObjectStore, key: string): Promise<void> {
  await store.put(objectKey(key), TOMBSTONE, { contentType: "application/zstd" });
}

export async function readInbox(store: ObjectStore, key: string, decompress: (b: Uint8Array) => Uint8Array): Promise<Uint8Array | null> {
  try {
    return decompress(await readAll((await store.getObject(objectKey(key))).body));
  } catch (error) {
    if (isObjectStoreError(error, "not_found")) return null;
    throw error;
  }
}
