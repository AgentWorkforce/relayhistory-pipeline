/**
 * The upload-inbox consumer side of relay-upload-inbox-v1 (docs/contracts/relay-upload-inbox-v1.md,
 * decision docs/decisions/0004-direct-upload-inbox.md). Everything here sits behind HISTORY_UPLOAD_DIRECT
 * (off | allowlist | on, default off); with it off nothing reads the upload prefix.
 *
 * `promoteUpload(key)` turns one desktop-written object under `<keyRoot>/.upload-inbox/` into the SAME
 * acknowledged admission-inbox object the HTTP path writes (async-admission.ts enqueueAdmission), after the
 * §6 checks in contract order, through the request path's admission core (admission-core.ts). From there the
 * unchanged admission consumer (admission-consumer.ts) claims, gates on the privacy journal at the witnessed
 * seq, writes, and records the outcome. A refusal before the hand-off is recorded here.
 *
 *   key -> keyRoot -> flag scope -> witness key (config) -> status (idempotency) -> object GET
 *   -> witness (MAC, exp) -> binding (tenant, seq) -> journal (seq not ahead) -> bytes (size, d=)
 *   -> zstd -> JSON + parseDeliveryRequest -> account -> origin (+ b=) -> prepareForAdmission
 *   -> status "handed_off" -> enqueueAdmission -> delete the upload object
 *
 * Retryable conditions are never refusals (§6): store or queue failures, a missing or malformed
 * UPLOAD_WITNESS_KEY, the flag excluding the tenant on this side. Those return `retry`, so the queue
 * (or the self-host scan) re-drives the object and a DLQ / operator sees it.
 *
 * The per-upload status object lives with the admission inbox of the same user, never under the
 * desktop-writable upload prefix: `<keyRoot>/.admission-inbox/<org>/v1/ws=<ws>/m=srv_<uh>/u=<sha256(key)>.json`.
 * pendingAdmissions and the self-host dead-letter scan match only `/r=<hex>/a=...ndjson.zst`, so they skip it.
 * Logs and errors never carry a key, body, witness or tenant id.
 */
import { createHash, randomUUID } from "node:crypto";
import { zstdDecompressSync } from "node:zlib";
import { isObjectStoreError, objectKey, readAll, type ObjectStore } from "../shared/vendor.ts";
import { layoutComponent } from "../shared/layout-component.ts";
import { prepareForAdmission, TenantNotASegment } from "./admission-core.ts";
import { enqueueAdmission, inboxKey, INBOX_MIN_BYTES, INBOX_NAMESPACE, outcomeKey, receiptDir, witnessPrivacyJournal, type AdmissionMessage } from "./async-admission.ts";
import type { DeliverDeps } from "./deliver.ts";
import type { LedgerTenant } from "./ledger.ts";
import { DeliveryError, MAX_DELIVERY_BYTES, type HistoryExportBatch } from "./vendor/delivery/delivery-contracts.ts";
import { parseDeliveryRequest, requireDeliveryAccount, type PreparedDelivery } from "./vendor/delivery/prepare.ts";
import {
  batchSegment,
  bindUploadToWitness,
  checkUploadObjectBytes,
  INBOX_UPLOAD_NAMESPACE,
  isUploadRefusal,
  parseUploadKey,
  parseUploadWitnessKey,
  UPLOAD_CONTRACT,
  UPLOAD_MAX_OBJECT_BYTES,
  UPLOAD_REFUSAL_CODES,
  userSegment,
  verifyUploadWitness,
  type UploadAdmissionStatus,
  type UploadKeyParts,
  type UploadRefusalCode,
  type UploadWitnessPayload,
} from "./upload-inbox-contract.ts";

// ---- HISTORY_UPLOAD_DIRECT ----

export const UPLOAD_DIRECT_FLAG = "HISTORY_UPLOAD_DIRECT";
/** The allowlist for HISTORY_UPLOAD_DIRECT=allowlist: comma-separated `org:<orgId>` / `ws:<workspaceId>`. */
export const UPLOAD_DIRECT_ALLOWLIST_VAR = "HISTORY_UPLOAD_DIRECT_ALLOWLIST";
/** The shared witness MAC key (32 bytes, unpadded base64url). No default, ever. */
export const UPLOAD_WITNESS_KEY_VAR = "UPLOAD_WITNESS_KEY";

export type UploadDirectMode = "off" | "allowlist" | "on";
export interface UploadDirectConfig {
  mode: UploadDirectMode;
  /** allowlist only: the admitted orgs and workspaces (a tenant matches when either matches). */
  orgs: ReadonlySet<string>;
  workspaces: ReadonlySet<string>;
}
export const UPLOAD_DIRECT_OFF: UploadDirectConfig = Object.freeze({ mode: "off", orgs: new Set<string>(), workspaces: new Set<string>() });

const SEGMENT = /^(?!\.)[A-Za-z0-9._-]{1,128}$/;

/**
 * Unset, "" or "off": off. Exactly "allowlist" or "on" otherwise; anything else throws (a typo never means
 * either). allowlist needs at least one valid entry. Errors name the variable, never the value.
 */
export function uploadDirectConfig(env: Record<string, string | undefined>): UploadDirectConfig {
  const raw = env[UPLOAD_DIRECT_FLAG];
  if (raw === undefined || raw === "" || raw === "off") return UPLOAD_DIRECT_OFF;
  if (raw === "on") return { mode: "on", orgs: new Set(), workspaces: new Set() };
  if (raw !== "allowlist") throw new Error(`96bulls: ${UPLOAD_DIRECT_FLAG} must be off, allowlist or on`);
  const orgs = new Set<string>(), workspaces = new Set<string>();
  for (const entry of (env[UPLOAD_DIRECT_ALLOWLIST_VAR] ?? "").split(",").map((e) => e.trim()).filter(Boolean)) {
    const m = /^(org|ws):(.+)$/.exec(entry);
    if (!m || !SEGMENT.test(m[2]!)) throw new Error(`96bulls: ${UPLOAD_DIRECT_ALLOWLIST_VAR} entries must be org:<id> or ws:<id>`);
    (m[1] === "org" ? orgs : workspaces).add(m[2]!);
  }
  if (orgs.size + workspaces.size === 0) throw new Error(`96bulls: ${UPLOAD_DIRECT_FLAG}=allowlist needs ${UPLOAD_DIRECT_ALLOWLIST_VAR}`);
  return { mode: "allowlist", orgs, workspaces };
}

export function uploadDirectAllows(config: UploadDirectConfig, tenant: { orgId: string; workspaceId: string }): boolean {
  if (config.mode === "on") return true;
  if (config.mode === "off") return false;
  return config.orgs.has(tenant.orgId) || config.workspaces.has(tenant.workspaceId);
}

// ---- status object ----

const STATUS_KIND = "upload-admission-status";
type StoredStatus =
  | { v: 1; kind: typeof STATUS_KIND; state: "refused"; code: UploadRefusalCode; receivedAtMs: number; decidedAtMs: number; receiptId?: string }
  | { v: 1; kind: typeof STATUS_KIND; state: "handed_off"; originId: string; receiptId: string; inboxKey: string; receivedAtMs: number };

const encoder = new TextEncoder();
const decoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: false });
const sha256Hex = (v: string) => createHash("sha256").update(v).digest("hex");
const msOk = (v: unknown): v is number => typeof v === "number" && Number.isSafeInteger(v) && v >= 0;
const RECEIPT = /^rhr_[0-9a-f]{64}$/;

/** `<keyRoot>/.admission-inbox/<org>/v1/ws=<ws>/m=srv_<uh>/u=<sha256(uploadKey)>.json` (the journal's user dir). */
export function uploadStatusKey(parts: UploadKeyParts): string {
  return `${parts.keyRoot}/${INBOX_NAMESPACE}/${parts.orgId}/v1/ws=${layoutComponent(parts.workspaceId)}/m=srv_${parts.userSegment}/u=${sha256Hex(parts.key)}.json`;
}

function validStatus(v: unknown): v is StoredStatus {
  if (!v || typeof v !== "object" || Array.isArray(v)) return false;
  const s = v as Record<string, unknown>;
  if (s.v !== 1 || s.kind !== STATUS_KIND || !msOk(s.receivedAtMs)) return false;
  if (s.state === "refused") return UPLOAD_REFUSAL_CODES.includes(s.code as never) && msOk(s.decidedAtMs) && (s.receiptId === undefined || (typeof s.receiptId === "string" && RECEIPT.test(s.receiptId)));
  return s.state === "handed_off" && typeof s.originId === "string" && typeof s.receiptId === "string" && RECEIPT.test(s.receiptId) && typeof s.inboxKey === "string";
}

/** The stored status, or null when absent. A malformed status object is a store fault (throws, retry). */
async function readStatus(store: ObjectStore, key: string): Promise<StoredStatus | null> {
  let raw: Uint8Array;
  try {
    raw = await readAll((await store.getObject(objectKey(key))).body);
  } catch (error) {
    if (isObjectStoreError(error, "not_found")) return null;
    throw error;
  }
  const v: unknown = JSON.parse(decoder.decode(raw));
  if (!validStatus(v)) throw new Error("upload_status_invalid");
  return v;
}

const writeStatus = (store: ObjectStore, key: string, status: StoredStatus) =>
  store.put(objectKey(key), encoder.encode(JSON.stringify(status)), { contentType: "application/json" });

/** The outcome marker next to an inbox attempt (`<receiptDir>/a=...` -> `<receiptDir>/outcome-*.json`), if any. */
async function handedOffOutcome(store: ObjectStore, inbox: string): Promise<boolean> {
  const dir = inbox.slice(0, inbox.lastIndexOf("/"));
  for (const state of ["admitted", "refused"] as const) if (await store.head(objectKey(outcomeKey(dir, state)))) return true;
  return false;
}

// ---- promotion ----

export interface PromoteUploadDeps {
  config: UploadDirectConfig;
  /** The request path's deliver deps: the target (store, keyRoot) and the budget inputs prepareForAdmission reads. */
  deliver: Pick<DeliverDeps, "target" | "babysitterRetention" | "replica" | "subrequestCap" | "subrequestRedirects" | "acceptanceMode" | "inlineMaxSessions" | "retentionGeneration">;
  /** UPLOAD_WITNESS_KEY as configured (raw). Missing or malformed: every object is a `retry`, never a refusal. */
  witnessKey: string | undefined;
  /** The admission queue send (hosted) or the self-host consumer's notify. */
  enqueue: (message: AdmissionMessage) => Promise<void>;
  nowMs?: () => number;
  nonce?: () => string;
}

export type PromoteResult =
  /** handed_off: an admission-inbox object now holds the batch; refused: a terminal §6 refusal is recorded. */
  | { action: "ack"; outcome: "handed_off" | "refused" | "already_promoted" | "gone"; code?: UploadRefusalCode }
  /** Transient or configuration: leave the object, re-drive later. `code` is an operational code, never a refusal. */
  | { action: "retry"; code: string; error?: unknown }
  /** Not an upload key of this key root: nothing is read or written; the caller fails (DLQ) or skips it. */
  | { action: "invalid"; code: "upload_key_invalid" };

const retry = (code: string, error?: unknown): PromoteResult => ({ action: "retry", code, ...(error !== undefined ? { error } : {}) });
const randomNonce = () => randomUUID().replace(/-/g, "").slice(0, 16);

/** A refusal code from an error, or null when it is not terminal (retry). Codes outside §6 map to the nearest listed one. */
export function uploadRefusalOf(error: unknown): UploadRefusalCode | null {
  if (error instanceof TenantNotASegment) return "tenant_not_a_segment";
  if (!(error instanceof DeliveryError) || error.status === 503 || error.status === 429) return null;
  return contractCode(error.code);
}

/** A consumer / delivery code as a §6 code: listed codes pass; anything else is `admission_failed` (a recorded gap). */
export function contractCode(code: string): UploadRefusalCode {
  return UPLOAD_REFUSAL_CODES.includes(code as never) ? (code as UploadRefusalCode) : "admission_failed";
}

async function readBounded(body: ReadableStream<Uint8Array>, max: number): Promise<Uint8Array | null> {
  const reader = body.getReader();
  const chunks: Uint8Array[] = [];
  let size = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    size += value.byteLength;
    if (size > max) {
      await reader.cancel().catch(() => {});
      return null;
    }
    chunks.push(value);
  }
  const out = new Uint8Array(size);
  let at = 0;
  for (const c of chunks) { out.set(c, at); at += c.byteLength; }
  return out;
}

const ZSTD_MAGIC = [0x28, 0xb5, 0x2f, 0xfd];

/** §6 encoding + decode: one zstd frame (magic checked; see the report for the single-frame gap), decoded <= MAX_DELIVERY_BYTES, UTF-8 JSON. */
function decodeObject(bytes: Uint8Array): { value: unknown } | { code: UploadRefusalCode } {
  if (bytes.byteLength < 4 || ZSTD_MAGIC.some((b, i) => bytes[i] !== b)) return { code: "unsupported_encoding" };
  let decoded: Uint8Array;
  try {
    decoded = new Uint8Array(zstdDecompressSync(bytes, { maxOutputLength: MAX_DELIVERY_BYTES }));
  } catch (error) {
    return { code: error instanceof RangeError ? "delivery_too_large" : "unsupported_encoding" };
  }
  try {
    return { value: JSON.parse(decoder.decode(decoded)) };
  } catch {
    return { code: "invalid_delivery" };
  }
}

/**
 * Promote one upload object. Idempotent: a redelivery after a terminal status (or after the hand-off
 * landed) only finishes the upload object's removal and acks. Never throws for a store or queue fault
 * (those are `retry`).
 */
export async function promoteUpload(key: string, deps: PromoteUploadDeps): Promise<PromoteResult> {
  // Flag off: nothing is read.
  if (deps.config.mode === "off") return retry("history_upload_direct_off");
  const parts = parseUploadKey(key);
  const { store, keyRoot } = deps.deliver.target;
  if (isUploadRefusal(parts) || parts.keyRoot !== keyRoot) return { action: "invalid", code: "upload_key_invalid" };
  // A tenant the flag excludes on this side: the issuer and the consumer disagree. Wait (DLQ), never refuse.
  if (!uploadDirectAllows(deps.config, parts)) return retry("history_upload_direct_excluded");
  let witnessKey: Uint8Array;
  try {
    witnessKey = parseUploadWitnessKey(deps.witnessKey ?? "");
  } catch {
    return retry(deps.witnessKey === undefined || deps.witnessKey.trim() === "" ? "upload_witness_key_missing" : "upload_witness_key_invalid");
  }
  const now = (deps.nowMs ?? Date.now)();
  const statusKey = uploadStatusKey(parts);
  const removeUpload = async (result: PromoteResult): Promise<PromoteResult> => {
    try {
      await store.delete(objectKey(key));
    } catch (error) {
      return retry("upload_delete_unavailable", error);
    }
    return result;
  };

  let status: StoredStatus | null;
  try {
    status = await readStatus(store, statusKey);
  } catch (error) {
    return retry("upload_status_unavailable", error);
  }
  if (status?.state === "refused") return removeUpload({ action: "ack", outcome: "already_promoted", code: status.code });
  if (status?.state === "handed_off") {
    try {
      const inbox = await store.head(objectKey(status.inboxKey));
      if (inbox && inbox.size >= INBOX_MIN_BYTES) {
        // Content: the inbox PUT landed; the queue send may not have. Re-send (a message for an inbox
        // the consumer already finished just acks), then finish.
        await deps.enqueue({ v: 1, kind: "ingest-admission", key: status.inboxKey });
        return removeUpload({ action: "ack", outcome: "already_promoted" });
      }
      // A tombstone is terminal only with an outcome next to it; enqueueAdmission also tombstones an
      // attempt whose queue send failed (unacknowledged), and that one must run again.
      if (inbox && (await handedOffOutcome(store, status.inboxKey))) return removeUpload({ action: "ack", outcome: "already_promoted" });
    } catch (error) {
      return retry("upload_status_unavailable", error);
    }
    // The status was written but the inbox PUT or the queue send was not: run the hand-off again.
  }

  let object;
  try {
    object = await store.getObject(objectKey(key));
  } catch (error) {
    if (isObjectStoreError(error, "not_found")) return { action: "ack", outcome: status ? "already_promoted" : "gone" };
    return retry("upload_object_unavailable", error);
  }
  const cancel = () => object.body.cancel().catch(() => {});
  const refuse = async (code: UploadRefusalCode, receiptId?: string): Promise<PromoteResult> => {
    await cancel();
    try {
      await writeStatus(store, statusKey, { v: 1, kind: STATUS_KIND, state: "refused", code, receivedAtMs: now, decidedAtMs: now, ...(receiptId ? { receiptId } : {}) });
    } catch (error) {
      return retry("upload_status_unavailable", error);
    }
    return removeUpload({ action: "ack", outcome: "refused", code });
  };

  // Witness (§3): from the object's own metadata, the same response as the body.
  const witness = verifyUploadWitness(witnessKey, object.meta.metadata?.["relay-witness"], Math.floor(now / 1000));
  if (isUploadRefusal(witness)) return refuse(witness.code);
  const bound = bindUploadToWitness(parts, witness);
  if (isUploadRefusal(bound)) return refuse(bound.code);
  const tenant: LedgerTenant = { orgId: witness.org, workspaceId: witness.ws, userId: witness.user };
  // A seq ahead of the user's journal was never issued against it.
  try {
    if (witness.seq > await witnessPrivacyJournal(store, keyRoot, tenant)) return refuse("upload_witness_seq_mismatch");
  } catch (error) {
    await cancel();
    return retry("privacy_journal_unavailable", error);
  }

  // Bytes: size before reading, then the digest.
  if (object.meta.size > UPLOAD_MAX_OBJECT_BYTES) return refuse("delivery_too_large");
  let bytes: Uint8Array | null;
  try {
    bytes = await readBounded(object.body, UPLOAD_MAX_OBJECT_BYTES);
  } catch (error) {
    return retry("upload_object_unavailable", error);
  }
  if (!bytes) return refuse("delivery_too_large");
  const checked = checkUploadObjectBytes(parts, bytes);
  if (isUploadRefusal(checked)) return refuse(checked.code);

  const decoded = decodeObject(bytes);
  if ("code" in decoded) return refuse(decoded.code);
  let batch: HistoryExportBatch;
  try {
    batch = parseDeliveryRequest(decoded.value);
  } catch (error) {
    return refuse(error instanceof DeliveryError ? contractCode(error.code) === "admission_failed" ? "invalid_delivery" : contractCode(error.code) : "invalid_delivery");
  }
  try {
    await requireDeliveryAccount({ orgId: tenant.orgId, workspaceId: tenant.workspaceId }, batch.account_id);
  } catch {
    return refuse("delivery_account_mismatch");
  }
  if (batch.origin_id !== parts.originId) return refuse("upload_origin_mismatch");
  // `b=` names the batch: a key whose batch segment is not this batch's does not describe the object.
  if (batchSegment(batch.batch_id) !== parts.batchSegment) return refuse("upload_key_invalid");

  let prepared: PreparedDelivery;
  try {
    prepared = await prepareForAdmission(deps.deliver, tenant, batch, now);
  } catch (error) {
    const code = uploadRefusalOf(error);
    return code ? refuse(code) : retry("admission_prepare_failed", error);
  }
  return handOff(deps, statusKey, tenant, prepared, witness, now, removeUpload);
}

async function handOff(deps: PromoteUploadDeps, statusKey: string, tenant: LedgerTenant, prepared: PreparedDelivery, witness: UploadWitnessPayload,
  now: number, removeUpload: (r: PromoteResult) => Promise<PromoteResult>): Promise<PromoteResult> {
  const { store, keyRoot } = deps.deliver.target;
  const nonce = (deps.nonce ?? randomNonce)();
  let inbox: string;
  try {
    inbox = inboxKey(keyRoot, tenant, prepared.originId, prepared.receipt.receiptId, now, nonce);
    // Status first: once the inbox exists, a redelivery finds the pointer and never writes a second attempt.
    await writeStatus(store, statusKey, { v: 1, kind: STATUS_KIND, state: "handed_off", originId: prepared.originId, receiptId: prepared.receipt.receiptId, inboxKey: inbox, receivedAtMs: now });
  } catch (error) {
    return retry("upload_status_unavailable", error);
  }
  try {
    // The witnessed seq is the inbox's journal seq: the consumer's post-claim gate refuses
    // delivery_privacy_superseded when a reset / re-enable completed after the witness was issued.
    await enqueueAdmission({ store, keyRoot, enqueue: deps.enqueue, nonce: () => nonce }, tenant, prepared, now, witness.seq);
  } catch (error) {
    return retry("admission_enqueue_failed", error);
  }
  return removeUpload({ action: "ack", outcome: "handed_off" });
}

// ---- status (GET /v1/delivery/admission?uploadKey=) ----

/** Pending answers: the desktop's poll interval (and Retry-After). */
export const UPLOAD_PENDING_RETRY_AFTER_SECONDS = 15;
/** 503 answers' Retry-After. */
export const UPLOAD_UNAVAILABLE_RETRY_AFTER_SECONDS = 5;

export type UploadStatusAnswer =
  | { ok: true; body: UploadAdmissionStatus; retryAfterSeconds?: number }
  | { ok: false; status: 400 | 403 | 404 | 503; code: "invalid_delivery" | "upload_tenant_mismatch" | "upload_unknown" | "delivery_unavailable"; message: string; retryAfterSeconds?: number };

const unknown = (): UploadStatusAnswer => ({ ok: false, status: 404, code: "upload_unknown", message: "No upload, outcome or status exists for this key" });

interface Outcome { state: "admitted" | "refused"; code: string; atMs: number }
async function readOutcome(store: ObjectStore, dir: string, state: "admitted" | "refused"): Promise<Outcome | null> {
  try {
    const v = JSON.parse(decoder.decode(await readAll((await store.getObject(objectKey(outcomeKey(dir, state)))).body))) as Record<string, unknown>;
    return v && v.v === 1 && v.state === state && typeof v.code === "string" ? { state, code: v.code, atMs: msOk(v.atMs) ? v.atMs : 0 } : null;
  } catch (error) {
    if (isObjectStoreError(error, "not_found")) return null;
    throw error;
  }
}

/**
 * The status of one upload for the VERIFIED identity (never a header or query tenant). The key must parse, sit
 * under this key root and belong to the identity's (org, ws, user); then: the recorded status (resolved through
 * the admission outcome after a hand-off), else pending while the object exists, else 404 upload_unknown.
 */
export async function uploadAdmissionStatus(target: { store: ObjectStore; keyRoot: string }, config: UploadDirectConfig, identity: LedgerTenant, uploadKey: string): Promise<UploadStatusAnswer> {
  const parts = parseUploadKey(uploadKey);
  if (isUploadRefusal(parts) || parts.keyRoot !== target.keyRoot) return { ok: false, status: 400, code: "invalid_delivery", message: "Invalid upload key" };
  if (parts.orgId !== identity.orgId || parts.workspaceId !== identity.workspaceId || parts.userSegment !== userSegment(identity.workspaceId, identity.userId))
    return { ok: false, status: 403, code: "upload_tenant_mismatch", message: "The upload key is not the caller's" };
  // The flag excludes this caller: the same answer as an unknown upload (decision 0004).
  if (!uploadDirectAllows(config, parts)) return unknown();
  const pending = (receivedAtMs?: number): UploadStatusAnswer => ({
    ok: true,
    retryAfterSeconds: UPLOAD_PENDING_RETRY_AFTER_SECONDS,
    body: { contract: UPLOAD_CONTRACT, uploadKey, state: "pending", ...(receivedAtMs !== undefined ? { receivedAtMs } : {}), retryAfterSeconds: UPLOAD_PENDING_RETRY_AFTER_SECONDS },
  });
  try {
    const status = await readStatus(target.store, uploadStatusKey(parts));
    if (status?.state === "refused")
      return { ok: true, body: { contract: UPLOAD_CONTRACT, uploadKey, state: "refused", code: status.code, ...(status.receiptId ? { receiptId: status.receiptId } : {}), receivedAtMs: status.receivedAtMs, decidedAtMs: status.decidedAtMs } };
    if (status?.state === "handed_off") {
      const dir = receiptDir(target.keyRoot, identity, status.originId, status.receiptId);
      // Admitted wins over refused (async-admission.ts admissionStatus).
      const outcome = (await readOutcome(target.store, dir, "admitted")) ?? (await readOutcome(target.store, dir, "refused"));
      if (!outcome) return pending(status.receivedAtMs);
      const decidedAtMs = Math.max(outcome.atMs, status.receivedAtMs);
      return outcome.state === "admitted"
        ? { ok: true, body: { contract: UPLOAD_CONTRACT, uploadKey, state: "admitted", receiptId: status.receiptId, receivedAtMs: status.receivedAtMs, decidedAtMs } }
        : { ok: true, body: { contract: UPLOAD_CONTRACT, uploadKey, state: "refused", code: contractCode(outcome.code), receiptId: status.receiptId, receivedAtMs: status.receivedAtMs, decidedAtMs } };
    }
    const meta = await target.store.head(objectKey(uploadKey));
    return meta ? pending(meta.lastModified.getTime()) : unknown();
  } catch {
    return { ok: false, status: 503, code: "delivery_unavailable", message: "Durable delivery is temporarily unavailable", retryAfterSeconds: UPLOAD_UNAVAILABLE_RETRY_AFTER_SECONDS };
  }
}

/** `<keyRoot>/.upload-inbox/`: the self-host scan root. */
export const uploadInboxRoot = (keyRoot: string) => `${keyRoot}/${INBOX_UPLOAD_NAMESPACE}/`;
