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
 * The privacy journal closes the one gap the claim alone cannot: a reset, or a re-enable of a
 * disabled session, that completes after an upload began but before its admission. The upload
 * witnesses the user's journal seq BEFORE its body is read; every such control marks itself
 * pending before its SQL and advances seq after it; the consumer, AFTER its claim, waits while a
 * control is pending and refuses when seq grew past the witness. A control applied after the
 * claim is caught by the existing ACK recheck (original epochs and reset count), and one applied
 * after the ACK erases as today.
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

/**
 * relayhistory opts in per request INSIDE the signed identity (`admission`, app.ts bindingIdentity),
 * never as a loose header; it then accepts ADMISSION_HEADER instead of a custody ACK, and only for a
 * request it opted in itself.
 */
export const ADMISSION_ACCEPT_VALUE = "queued-v1";
/** On an async 2xx: exactly this JSON, and never a custody ACK header. */
export const ADMISSION_HEADER = "x-relay-admission";
export const ADMISSION_QUEUED = JSON.stringify({ version: 1, state: "queued" });

/**
 * A leading `.` keeps the inbox disjoint from every tenant prefix `<keyRoot>/<org>/`: no org id
 * may start with `.` (writer.ts assertTenantSegment, stacks/aws/tenants.ts ORG_RE), so the
 * inbox namespace can never overlap (or be confused with) any tenant data.
 */
export const INBOX_NAMESPACE = ".admission-inbox";
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
  constructor(code: "delivery_privacy_superseded" | "admission_invalid", message: string) {
    super(code, 409, message);
  }
}

function assertInboxTenant(keyRoot: string, orgId: string) {
  assertTenantSegment(keyRoot, orgId);
}

/** `<keyRoot>/.admission-inbox/<org>/v1/ws=<ws>/m=srv_<…>/` — inside the srv-writer `…/v1/ws=*\/m=srv_*` grant. */
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
const userDir = (keyRoot: string, tenant: LedgerTenant) =>
  inboxDir(keyRoot, tenant, `srv_${sha256Hex(JSON.stringify(["admission-user-v1", tenant.workspaceId, tenant.userId])).slice(0, 32)}`);
/** Per (org, workspace, user): the privacy journal every reset / re-enable brackets. */
export function journalKey(keyRoot: string, tenant: LedgerTenant): string {
  return `${userDir(keyRoot, tenant)}/privacy-journal.json`;
}
// ---- inbox object ----

interface InboxHeader {
  v: 1;
  kind: typeof INBOX_KIND;
  receivedAtMs: number;
  /** The privacy journal seq witnessed before the request body was read (the admission fence). */
  journalSeq: number;
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
export function encodeInbox(tenant: LedgerTenant, prepared: PreparedDelivery, receivedAtMs: number, journalSeq: number): Uint8Array {
  if (!Number.isSafeInteger(journalSeq) || journalSeq < 0) throw new Error("admission_witness_invalid");
  const header: InboxHeader = { v: 1, kind: INBOX_KIND, receivedAtMs, journalSeq, tenant: { orgId: tenant.orgId, workspaceId: tenant.workspaceId, userId: tenant.userId },
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
  const framed = rawZstdFrame(raw);
  // The recovery scan tells content from tombstones by size: keep that true by construction.
  if (framed.byteLength < INBOX_MIN_BYTES || TOMBSTONE.byteLength >= INBOX_MIN_BYTES) throw new Error("admission_inbox_size_invariant");
  return framed;
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

export interface DecodedInbox { tenant: LedgerTenant; prepared: PreparedDelivery; receivedAtMs: number; journalSeq: number }

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
  if (!isObject(h) || h.v !== 1 || h.kind !== INBOX_KIND || !Number.isSafeInteger(h.receivedAtMs) || !Number.isSafeInteger(h.journalSeq) || (h.journalSeq as number) < 0 || !isObject(h.tenant) || !isObject(h.receipt) ||
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
  return { tenant, receivedAtMs: h.receivedAtMs as number, journalSeq: h.journalSeq as number,
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
export async function enqueueAdmission(target: AsyncAdmissionTarget, tenant: LedgerTenant, prepared: PreparedDelivery, receivedAtMs: number, journalSeq: number): Promise<string> {
  assertInboxTenant(target.keyRoot, tenant.orgId);
  const nonce = (target.nonce ?? (() => crypto.randomUUID().replace(/-/g, "").slice(0, 16)))();
  const key = inboxKey(target.keyRoot, tenant, prepared.originId, prepared.receipt.receiptId, receivedAtMs, nonce);
  await target.store.put(objectKey(key), encodeInbox(tenant, prepared, receivedAtMs, journalSeq), { ifNoneMatch: true, contentType: "application/zstd" });
  try {
    await target.enqueue({ v: 1, kind: "ingest-admission", key });
  } catch (error) {
    // Unacknowledged: retire the content now (best effort; a 503 means the client retries with a new attempt).
    await target.store.put(objectKey(key), TOMBSTONE, { contentType: "application/zstd" }).catch(() => {});
    throw error;
  }
  return key;
}

// ---- privacy journal ----
//
// One object per (org, workspace, user). `seq` is a monotonic privacy-transition watermark: it only
// ever grows, and it grows after every reset / re-enable that applied (or may have applied), but it is
// NOT a count of them: settlements of the same or contiguous versions can coalesce into one step
// (a completed version subsumes lower ones). The gate needs only "seq moved since the witness". A
// control first records itself `pending` (before
// its SQL); after the SQL it completes (pending removed, seq + 1) or aborts (it definitely did not
// apply). An upload witnesses `seq` BEFORE its body is read; the consumer, after its claim, waits
// while a control is pending and refuses when `seq` has grown since.
//
// Only ORDERED controls are journaled: buffered acceptance (the only mode async admission runs in)
// routes every reset/selection to `ingest_apply_ordered_control`, which refuses a non-ordered one,
// so app.ts refuses those before SQL. An ordered control's identity is (epoch, version); SQL's own
// answer (applied | replay | stale) is the replay knowledge, so nothing here is evictable state:
//   - version <= the completed maximum (same epoch): already applied or superseded -> "done";
//   - a pending mark the consumer had to complete after its Worker died is kept as `ambiguous`
//     (seq already counted it); its next attempt asks SQL: "applied" (it had NOT applied before)
//     advances seq again, "replay" / "stale" do not.

export const PENDING_TTL_MS = 120_000;
interface PendingControl { id: string; startMs: number }
interface Journal { v: 3; seq: number; pending: PendingControl[]; ambiguous: string[]; ordered?: { epoch: string; maxVersion: string } }
const JOURNAL_ATTEMPTS = 8;
const emptyJournal = (): Journal => ({ v: 3, seq: 0, pending: [], ambiguous: [] });
const DECIMAL = /^[0-9]{1,19}$/;

function validJournal(v: unknown): v is Journal {
  if (!isObject(v) || v.v !== 3 || !Number.isSafeInteger(v.seq) || (v.seq as number) < 0 || !Array.isArray(v.pending) || !Array.isArray(v.ambiguous)) return false;
  if (!(v.pending as unknown[]).every((p) => isObject(p) && typeof p.id === "string" && Number.isSafeInteger(p.startMs))) return false;
  if (!(v.ambiguous as unknown[]).every((d) => typeof d === "string")) return false;
  const o = v.ordered as Record<string, unknown> | undefined;
  return o === undefined || (isObject(o) && typeof o.epoch === "string" && typeof o.maxVersion === "string" && DECIMAL.test(o.maxVersion));
}

async function readJournal(store: ObjectStore, key: string): Promise<{ journal: Journal; version?: string }> {
  let got;
  try {
    got = await store.getObject(objectKey(key));
  } catch (error) {
    if (isObjectStoreError(error, "not_found")) return { journal: emptyJournal() };
    throw error;
  }
  const value = JSON.parse(decoder.decode(await readAll(got.body)));
  // Fail closed: an unreadable journal stops controls (503) and admissions (retry), never resets seq.
  if (!validJournal(value)) throw new Error("privacy_journal_invalid");
  return { journal: value, version: got.meta.version as unknown as string };
}

/** Compare-and-swap edit; `edit` returns null for "no change". */
async function editJournal<T>(store: ObjectStore, keyRoot: string, tenant: LedgerTenant, edit: (j: Journal) => { next: Journal | null; result: T }): Promise<T> {
  assertInboxTenant(keyRoot, tenant.orgId);
  const key = journalKey(keyRoot, tenant);
  for (let attempt = 0; attempt < JOURNAL_ATTEMPTS; attempt++) {
    const { journal, version } = await readJournal(store, key);
    const { next, result } = edit(structuredClone(journal));
    if (!next) return result;
    if (next.seq < journal.seq) throw new Error("privacy_journal_regression");
    try {
      await store.put(objectKey(key), encoder.encode(JSON.stringify(next)), version ? { ifMatch: version as never, contentType: "application/json" } : { ifNoneMatch: true, contentType: "application/json" });
      return result;
    } catch (error) {
      if (!isObjectStoreError(error, "precondition_failed") && !isObjectStoreError(error, "already_exists")) throw error;
    }
  }
  throw new Error("privacy_journal_contended");
}

/** The upload's witness: the journal seq BEFORE its body is read. */
export async function witnessPrivacyJournal(store: ObjectStore, keyRoot: string, tenant: LedgerTenant): Promise<number> {
  assertInboxTenant(keyRoot, tenant.orgId);
  return (await readJournal(store, journalKey(keyRoot, tenant))).journal.seq;
}

/** An ordered privacy control's logical identity (control-protocol.ts): its epoch and version. */
export interface PrivacyControlId { epoch: string; version: string }
const idOf = (c: PrivacyControlId) => `o:${c.epoch}:${c.version}`;
const assertId = (c: PrivacyControlId) => {
  if (typeof c.epoch !== "string" || !c.epoch || typeof c.version !== "string" || !DECIMAL.test(c.version)) throw new Error("privacy_control_id_invalid");
};
const atOrBelowMax = (j: Journal, c: PrivacyControlId) => j.ordered?.epoch === c.epoch && BigInt(c.version) <= BigInt(j.ordered.maxVersion);
const raiseMax = (j: Journal, c: PrivacyControlId) => {
  if (j.ordered?.epoch !== c.epoch || BigInt(c.version) > BigInt(j.ordered.maxVersion)) j.ordered = { epoch: c.epoch, maxVersion: c.version };
  // SQL applies only version last+1 (ingest_apply_ordered_control), so a completed version proves every
  // lower one of its epoch already applied before it, and is covered by the count this version made:
  // their pending marks and ambiguity are subsumed (a delayed lower settler then no-ops).
  const below = (id: string) => { const [, e, v] = id.split(":"); return e === j.ordered!.epoch && BigInt(v!) <= BigInt(j.ordered!.maxVersion); };
  j.ambiguous = j.ambiguous.filter((a) => !below(a));
  j.pending = j.pending.filter((p) => !below(p.id));
};

/**
 * begin, BEFORE the control's SQL (fail closed: a failure must refuse the control):
 *   "unfinished" an earlier attempt is still pending (its completion may not have landed);
 *   "done"       at or below the completed maximum: SQL can only replay or answer stale;
 *   "ambiguous"  completed conservatively by a consumer; SQL's answer resolves it;
 *   "new"        now pending.
 */
export type ControlBegin = "done" | "unfinished" | "ambiguous" | "new";
export async function beginPrivacyControl(store: ObjectStore, keyRoot: string, tenant: LedgerTenant, control: PrivacyControlId, nowMs: number): Promise<ControlBegin> {
  assertId(control);
  const id = idOf(control);
  return editJournal<ControlBegin>(store, keyRoot, tenant, (j) => {
    if (j.pending.some((p) => p.id === id)) return { next: null, result: "unfinished" };
    if (atOrBelowMax(j, control)) return { next: null, result: "done" };
    if (j.ambiguous.includes(id)) return { next: null, result: "ambiguous" };
    j.pending.push({ id, startMs: nowMs });
    return { next: j, result: "new" };
  });
}

/**
 * After the control's SQL answered. `outcome` is SQL's: "applied" (it changed the fence now),
 * "replay" (it had already applied), "stale" (superseded; it never applies).
 *   new / unfinished: applied or replay -> seq + 1 (this journal had not counted it); stale -> abort.
 *   ambiguous:        applied -> seq + 1 (the conservative count was for an attempt that never
 *                     applied); replay / stale -> resolved, seq unchanged (already counted, or never applies).
 */
export async function settlePrivacyControl(store: ObjectStore, keyRoot: string, tenant: LedgerTenant, control: PrivacyControlId, begun: ControlBegin, outcome: "applied" | "replay" | "stale"): Promise<void> {
  assertId(control);
  if (begun === "done") return;
  const id = idOf(control);
  await editJournal(store, keyRoot, tenant, (j) => {
    // Decided on the CURRENT journal, not on what this request saw at begin: several requests can
    // carry the same (epoch, version) (a retry while the first is still in flight), and the watermark
    // must move at most once for it, whichever settles first.
    const wasPending = j.pending.some((p) => p.id === id), wasAmbiguous = j.ambiguous.includes(id);
    // At or below the completed maximum: already counted, by this control's own earlier settler or by a
    // higher version (which proves this one applied before it). Never count it again; just drop any
    // leftover mark. Neither pending nor ambiguous and stale: it never applies; nothing to do.
    if (atOrBelowMax(j, control)) {
      if (!wasPending && !wasAmbiguous) return { next: null, result: undefined };
      j.pending = j.pending.filter((p) => p.id !== id);
      j.ambiguous = j.ambiguous.filter((a) => a !== id);
      return { next: j, result: undefined };
    }
    if (!wasPending && !wasAmbiguous && outcome === "stale") return { next: null, result: undefined };
    j.pending = j.pending.filter((p) => p.id !== id);
    j.ambiguous = j.ambiguous.filter((a) => a !== id);
    // Ambiguous now (counted conservatively by a consumer, whatever this request saw at begin): only a
    // fresh application counts again. Otherwise an applied or replayed control this journal has not
    // counted yet counts once.
    const counted = begun === "ambiguous" || (!wasPending && wasAmbiguous) ? outcome === "applied" : outcome !== "stale";
    if (counted) j.seq += 1;
    if (outcome !== "stale") raiseMax(j, control);
    return { next: j, result: undefined };
  });
}

/** The control definitely did not apply (a typed refusal, or a SQL error that rolled back): drop its pending mark, seq unchanged. */
export async function abortPrivacyControl(store: ObjectStore, keyRoot: string, tenant: LedgerTenant, control: PrivacyControlId): Promise<void> {
  assertId(control);
  const id = idOf(control);
  await editJournal(store, keyRoot, tenant, (j) => {
    if (!j.pending.some((p) => p.id === id)) return { next: null, result: undefined };
    j.pending = j.pending.filter((p) => p.id !== id);
    return { next: j, result: undefined };
  });
}

/**
 * The consumer's post-claim gate. A fresh pending control: wait (retry). A pending control older
 * than PENDING_TTL_MS (its Worker died between SQL and settlement): counted conservatively here
 * (seq + 1) and kept `ambiguous`, so its next attempt lets SQL say whether it had applied. Then
 * refuse iff seq grew since the upload's witness.
 */
export async function assertNoLaterPrivacyControl(store: ObjectStore, keyRoot: string, tenant: LedgerTenant, witnessSeq: number, nowMs: number): Promise<void> {
  const seq = await editJournal<number | "pending">(store, keyRoot, tenant, (j) => {
    const stale = j.pending.filter((p) => nowMs - p.startMs > PENDING_TTL_MS);
    if (j.pending.length > stale.length) return { next: null, result: "pending" };
    if (stale.length === 0) return { next: null, result: j.seq };
    j.pending = [];
    j.seq += 1;
    j.ambiguous = [...new Set([...j.ambiguous, ...stale.map((p) => p.id)])];
    return { next: j, result: j.seq };
  });
  if (seq === "pending") throw new PrivacyControlPending();
  if (seq > witnessSeq)
    throw new AdmissionRefused("delivery_privacy_superseded", "A session reset or re-enable completed after this upload began; it was not admitted");
}

/** Retryable: a reset / re-enable is in flight for this user. */
export class PrivacyControlPending extends Error {
  readonly code = "privacy_control_pending";
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

// ---- durable recovery: the queue is not the authority ----

/** A pending inbox is never smaller than this (its header alone is larger); tombstones and markers always are. */
export const INBOX_MIN_BYTES = 256;
const INBOX_ATTEMPT = /\/r=[0-9a-f]{64}\/a=\d{15}-[0-9a-f]{16}\.ndjson\.zst$/;

/**
 * Every acknowledged inbox object that still holds content (not tombstoned) and is older than
 * `olderThanMs`, as the admission message that re-drives it. Built from S3 alone, so an expired or
 * lost queue message never strands an acknowledged upload: the operator re-sends these (a message
 * for an already-terminal object just acks, and a re-run admission replays the claim). `orgId`
 * narrows the scan to one tenant.
 */
export async function* pendingAdmissions(store: ObjectStore, keyRoot: string, opts: { nowMs: number; olderThanMs: number; orgId?: string }): AsyncGenerator<AdmissionMessage> {
  if (opts.orgId !== undefined) assertInboxTenant(keyRoot, opts.orgId);
  const prefix = `${keyRoot}/${INBOX_NAMESPACE}${opts.orgId !== undefined ? `/${opts.orgId}` : ""}`;
  // Slash-bounded: org-a must never match org-ab (a store lists by string prefix).
  const bounded = prefix + "/";
  let cursor: string | undefined;
  do {
    const page = await store.list(objectKey(prefix), cursor);
    for (const item of page.items) {
      const key = String(item.key);
      if (!key.startsWith(bounded) || !INBOX_ATTEMPT.test(key) || item.size < INBOX_MIN_BYTES) continue;
      if (opts.nowMs - item.lastModified.getTime() < opts.olderThanMs) continue;
      yield { v: 1, kind: "ingest-admission", key };
    }
    cursor = page.nextCursor;
  } while (cursor);
}

export async function readInbox(store: ObjectStore, key: string, decompress: (b: Uint8Array) => Uint8Array): Promise<Uint8Array | null> {
  try {
    return decompress(await readAll((await store.getObject(objectKey(key))).body));
  } catch (error) {
    if (isObjectStoreError(error, "not_found")) return null;
    throw error;
  }
}
