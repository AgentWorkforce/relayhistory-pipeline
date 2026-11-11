import type {FrozenWitnessInput,LedgerTenant} from "./ledger.ts";
import {decodeCustodyEnvelope} from "./custody-wire.ts";
import {validatedCustodyAck} from "./custody-ack.ts";
import {CURRENT_CUSTODY_CONTENT_TYPE,CURRENT_CUSTODY_ACK_HEADER,PRIVATE_CUSTODY_BYTES,type CurrentCustodyPacket} from "./custody-protocol.ts";
import {orderedControl,orderedControlResult,type OrderedControl} from "./control-protocol.ts";
import { registerSessionEraseRoutes, SESSION_ERASE_DISABLED, type SessionEraseConfig, type SessionEraseService } from "./session-erase.ts";
import { checkUploadConsent, CONSENT_SCOPE, UploadConsentRequired, validateConsentWrite, type ConsentWrite, type UploadConsentConfig } from "./upload-consent.ts";
import { operatorSharingCap } from "./capture-policy.ts";
import { assertUserDeleteAllowed, CAPTURE_POLICY_REVISION_HEADER, uploadPolicyError, uploadPolicyViolation, type CapturePolicySubject, type CapturePolicyUploadsMode } from "./capture-policy.ts";
/**
 * The 96Bulls session ingest: relayhistory's delivery write contract
 * (relayhistory-cloud packages/relayhistory/src/routes/delivery.ts), served
 * from AWS with S3 as the record and Supabase as the ledger. No Neon.
 *
 *   POST /v1/delivery/batches   rth:sync   one batch -> receipt (same shapes, same codes)
 *   GET  /v1/delivery/limits    rth:sync   DELIVERY_LIMITS + contentEncodings
 *   POST /v1/turns/batches      rth:sync   relay session turns as records (turns.ts), same receipt
 *   GET  /healthz                          liveness only
 *   POST|GET /internal/v1/session-erase/... operator per-session erase (session-erase.ts; default
 *                                          off: 501 not_implemented, and still 501 when enabled)
 *
 * Every other route is `501 unsupported_by_backend`. Selection, reset and
 * reads stay on relayhistory until their own cutover.
 *
 * Who calls it: only relayhistory's Worker, over a Cloudflare service binding.
 * This Worker has no route, no custom domain and no workers.dev URL (the stack
 * enforces it), so it is unreachable from the internet. relayhistory has
 * already authenticated the client's own bearer (`requireAuth`, `rth:sync`),
 * and passes the verified identity in `x-relay-ingest-identity`. Tenancy comes
 * only from that header, never from the request body.
 *
 * Wire compatibility, line by line with relayhistory: the request body is read
 * bounded by MAX_DELIVERY_BYTES on the wire and decoded (identity, gzip, zstd;
 * 413 / 415 / 400 as `boundedJson`), parsed by the vendored
 * `parseDeliveryRequest`, and answered with the receipt plus `limits`
 * (`contentEncodings` included). Errors are `{ error: { code, message,
 * conflict?, conflicts?, conflictCount? }, correlationId }`, and
 * `X-Correlation-Id` is echoed as relayhistory does.
 */
import { zstdDecompressSync } from "node:zlib";
import { createHash, createHmac, timingSafeEqual } from "node:crypto";
import { Hono, type Context } from "hono";
import { log } from "../shared/log.ts";
import { captureSafely, ingestOutcome, type AnalyticsPort } from "../shared/analytics.ts";
import { AuthError, type VerifiedIdentity } from "../session-read-api/app.ts";
import { batchIdConflict, ClaimLockBusy, deliverBatch, deliverPrepared, IngestUnavailable, type DeliverDeps } from "./deliver.ts";
import { prepareForAdmission } from "./admission-core.ts";
import { UPLOAD_DIRECT_OFF, uploadAdmissionStatus, type UploadDirectConfig } from "./upload-admission.ts";
import { ADMISSION_ACCEPT_VALUE, ADMISSION_HEADER, ADMISSION_LEGACY_REPLAY, ADMISSION_QUEUED, abortPrivacyControl, admissionStatus, beginPrivacyControl, enqueueAdmission, settlePrivacyControl, witnessPrivacyJournal, type AdmissionMessage, type AsyncAdmissionMode, type SyncAdmissionFrozenReceiptMode } from "./async-admission.ts";
import { LedgerUnavailableError } from "./ledger.ts";
import type { DbErrorDiagnosis } from "./db-error-class.ts";
import { DELIVERY_LIMITS, DeliveryBusyError, DeliveryError, MAX_DELIVERY_BYTES, type HistoryExportBatch } from "./vendor/delivery/delivery-contracts.ts";
import { AcquisitionRefusedError } from "./acquisition-refusal.ts";
import { parseDeliveryRequest, requireDeliveryAccount, prepareDelivery } from "./vendor/delivery/prepare.ts";
import { decodeRecoveryEnvelope, RECOVERY_CONTENT_TYPE } from "./recovery-protocol.ts";
import { parseTurnBatch } from "./turns.ts";
import { heartbeatProgress, ProgressError, readConnection, readSessionReceipt, recordHeartbeat, type Run as OnboardingRun } from "../history-db/onboarding.ts";

/**
 * `admission`: relayhistory's SIGNED opt-in to the queued ACK (async-admission.ts); absent: synchronous.
 * `role` and `capturePolicy`: self-host only, from relaycontrol introspection (selfhost-auth.ts); the hosted
 * signed identity never carries them (capture-policy.ts).
 */
export type IngestIdentity=VerifiedIdentity & {control?:OrderedControl;admission?:typeof ADMISSION_ACCEPT_VALUE} & CapturePolicySubject;

export const SYNC_SCOPE = "rth:sync";
export type ContentEncoding = "zstd" | "gzip";

/** relayhistory-cloud's read scope; only the onboarding status reads accept it here. */
export const READ_SCOPE = "rth:read";
export const UPLOAD_CONSENT_PATH = "/v1/upload-consent";
const READ_PATHS = new Set(["/v1/onboarding/connection", "/v1/onboarding/receipt"]);
export const IDENTITY_HEADER = "x-relay-ingest-identity";
export const SIGNATURE_HEADER = "x-relay-ingest-signature";
/** How old (or how far ahead) a signature may be. */
export const SIGNATURE_WINDOW_SECONDS = 60;

/**
 * What relayhistory signs (HMAC-SHA256, the shared Secrets Store key
 * `96bulls-session-ingest-signing-key`): the version, the time, the method, the
 * path, the identity header exactly as sent, and the SHA-256 of the body. A
 * sibling Worker that binds this one without the key cannot assert a tenant;
 * one that replays a captured request gets at most the same idempotent write
 * within the window.
 */
export function signingInput(t: number, method: string, path: string, identity: string, body: Uint8Array): string {
  return `v1\n${t}\n${method.toUpperCase()}\n${path}\n${identity}\n${createHash("sha256").update(body).digest("hex")}`;
}

export function signRequest(key: string, t: number, method: string, path: string, identity: string, body: Uint8Array): string {
  return `t=${t},v1=${createHmac("sha256", key).update(signingInput(t, method, path, identity, body)).digest("hex")}`;
}

/** The identity relayhistory verified and signed (`signRequest`). Refuses an unsigned, stale or altered request. */
export function signedBindingIdentity(key: string | undefined, nowSeconds: () => number = () => Math.floor(Date.now() / 1000)) {
  return (request: Request, body: Uint8Array): IngestIdentity => {
    if (!key) throw new AuthError(401, "not_configured", "the session ingest has no signing key");
    const identity = request.headers.get(IDENTITY_HEADER) ?? "";
    const m = /^t=(\d{1,12}),v1=([0-9a-f]{64})$/.exec(request.headers.get(SIGNATURE_HEADER) ?? "");
    if (!identity || !m) throw new AuthError(401, "missing_authorization", "missing Authorization header");
    const t = Number(m[1]);
    if (Math.abs(nowSeconds() - t) > SIGNATURE_WINDOW_SECONDS) throw new AuthError(401, "invalid_token", "invalid or expired token");
    const expected = Buffer.from(signRequest(key, t, request.method, new URL(request.url).pathname, identity, body).slice(-64), "hex");
    const given = Buffer.from(m[2]!, "hex");
    if (expected.length !== given.length || !timingSafeEqual(expected, given)) throw new AuthError(401, "invalid_token", "invalid or expired token");
    return bindingIdentity(request);
  };
}

/** The identity relayhistory verified, as it sends it: base64url JSON { userId, orgId, workspaceId, scopes }. */
export function bindingIdentity(request: Request): IngestIdentity {
  const raw = request.headers.get(IDENTITY_HEADER);
  if (!raw) throw new AuthError(401, "missing_authorization", "missing Authorization header");
  let v: Partial<IngestIdentity> | null = null;
  try {
    v = JSON.parse(Buffer.from(raw, "base64url").toString("utf8")) as Partial<IngestIdentity>;
  } catch {
    // fall through
  }
  const id = (x: unknown) => typeof x === "string" && x.length > 0 && x.length <= 512;
  if (!v || !id(v.userId) || !id(v.orgId) || typeof v.workspaceId !== "string" || !Array.isArray(v.scopes) || !v.scopes.every((s) => typeof s === "string"))
    throw new AuthError(401, "invalid_token", "invalid or expired token");
  let control:OrderedControl|undefined;
  if("control" in v){try{control=orderedControl(v.control)}catch{throw new AuthError(401,"invalid_token","invalid or expired token")}}
  // The async opt-in travels inside the signed identity, never as a loose header.
  if("admission" in v&&v.admission!==ADMISSION_ACCEPT_VALUE)throw new AuthError(401,"invalid_token","invalid or expired token");
  return { userId: v.userId!, orgId: v.orgId!, workspaceId: v.workspaceId, scopes: v.scopes,...(control?{control}:{}),...(v.admission?{admission:v.admission}:{}) };
}

export function encodeIdentity(identity: IngestIdentity): string {
  return Buffer.from(JSON.stringify(identity), "utf8").toString("base64url");
}

export interface IngestAppDeps {
  /** Default: `signedBindingIdentity(signingKey)`. */
  identify?: (request: Request, body: Uint8Array) => IngestIdentity | Promise<IngestIdentity>;
  /** The shared HMAC key (Secrets Store). Unset with no `identify`: every request is 401 not_configured. */
  signingKey?: string;
  deliver: DeliverDeps;
  /** P5b G2: the onboarding heartbeat on history.probe_connections. Unset: those routes answer 503. */
  onboarding?: { run: OnboardingRun };
  log?: (fields: Record<string, unknown>) => void;
  /**
   * Product analytics (src/shared/analytics.ts): one `session_ingest.batch_completed` per batch on
   * POST /v1/delivery/batches and /v1/turns/batches (route, outcome class, record count, latency bucket,
   * the verified org/workspace). Absent = none. A sink can never change or fail the answer.
   */
  analytics?: AnalyticsPort;
  /**
   * RELAYHISTORY_SELFHOST_CAPTURE_POLICY_UPLOADS (capture-policy.ts): report (default) logs an upload that breaks the
   * caller's capture policy and accepts it; enforce refuses it. Only identities carrying a policy (self-host) are checked.
   */
  capturePolicyUploads?: CapturePolicyUploadsMode;
  /**
   * INGEST_UPLOAD_CONSENT (upload-consent.ts, docs/upload-consent.md). Absent or enforcement "off": nothing
   * is read. Otherwise every batch is checked against the verified principal's server-owned sharing choice
   * before any enqueue, claim or write, again after the claim, and once more before the receipt commit.
   */
  uploadConsent?: UploadConsentConfig;
  /**
   * P1 custody source (INGEST_CUSTODY_SOURCE). "relayhistory" (default): only the signed Neon witness
   * relayhistory sends. "shadow": additionally build the Supabase frozen-source witness and log
   * match/mismatch, never changing the answer. "frozen-supabase": when relayhistory sends no witness,
   * build it from the sealed Supabase copy (refuses, never guesses, when unsealed).
   */
  custodySource?: "relayhistory" | "shadow" | "frozen-supabase";
  /**
   * C3 privacy authority (INGEST_PRIVACY_AUTHORITY). "neon" (default): today's Neon-first controls, the
   * Supabase mirror routes, and relayhistory's Neon witness accepted. "supabase" (needs frozen-supabase):
   * only the Supabase-authority control routes; every relayhistory-built (Neon) custody envelope and the
   * legacy mirror routes are refused, because Neon's heads are no longer the authority.
   */
  privacyAuthority?: "neon" | "supabase";
  /**
   * S3-first async admission (INGEST_ASYNC_ADMISSION; async-admission.ts). Absent or mode "off":
   * nothing changes. "on": a batch whose caller sends `x-relay-ingest-admission: queued-v1` (and
   * no private custody envelope) is ACKed after the inbox PUT + queue send, with no Supabase call;
   * every reset / re-enable raises the privacy journal first. "drain": no new async ACK, the
   * journal is still raised (the consumer is still admitting). Buffered acceptance only.
   */
  asyncAdmission?: { mode: AsyncAdmissionMode; enqueue: (message: AdmissionMessage) => Promise<void>; nowMs?: () => number; selfhost?: SelfhostAdmissionHooks };
  /**
   * INGEST_SYNC_ADMISSION_FROZEN_RECEIPT (async-admission.ts). Absent or "off": nothing changes. "on":
   * under frozen-supabase custody, a batch whose SIGNED identity carries `admission: queued-v1` but did
   * not take the queued path first reads the frozen legacy receipt, exactly as the admission consumer
   * does: a stored receipt answers legacy_replay (same digest; ADMISSION_LEGACY_REPLAY header) or
   * delivery_conflict (other digest) with no witness, claim or delivery. relayhistory skips its own
   * pre-forward read for such a caller.
   */
  syncAdmissionFrozenReceipt?: SyncAdmissionFrozenReceiptMode;
  /**
   * HISTORY_UPLOAD_DIRECT (upload-admission.ts, relay-upload-inbox-v1). Absent or mode "off": nothing
   * changes. Otherwise GET /v1/delivery/admission?uploadKey= answers the upload's §5 status for the
   * verified identity; a tenant the allowlist excludes gets 404 upload_unknown. Promotion itself runs in
   * the upload-admission Lambda (hosted) or the self-host scan, never in this app.
   */
  uploadDirect?: UploadDirectConfig;
  /**
   * Operator per-session erase (session-erase.ts, docs/operator-session-erase-contract.md). Absent: the
   * disabled config, so /internal/v1/session-erase/* answers 501 not_implemented and reads nothing.
   */
  sessionErase?: SessionEraseConfig;
  /** Self-host only: hosted has no reviewed ingress and keeps the validated 501 stub. */
  sessionEraseService?: SessionEraseService;
}

/**
 * Self-host only (selfhost-server.ts, SELFHOST_INGEST_ADMISSION); the hosted Worker never sets it, and
 * without it every line below behaves exactly as before. With it:
 *   - canonical acceptance is allowed (the self-host ingest is canonical). Control routes stay
 *     fail-closed: `journaled` refuses every non-ordered reset / re-enable before SQL (503), and the
 *     self-host wrapper does not route them at all;
 *   - the pre-body journal witness is taken for the identity the wrapper ALREADY verified (relaycontrol
 *     introspection runs before any body byte), never from a request header;
 *   - no synchronous fallback: with mode "on", a batch that cannot take the queued path (its witness
 *     failed, or anything else) is a 503, so the request path never reaches SQL.
 */
export interface SelfhostAdmissionHooks {
  /** The verified identity bound to exactly this Request, or undefined. */
  identityOf: (request: Request) => IngestIdentity | undefined;
  /**
   * The self-host erase ledger (RELAYHISTORY_SELFHOST_SESSION_SELECTION, ledger-sql.ts selfhostSelectionErase)
   * refuses every re-enable with a 400 before any SQL, so a non-ordered re-enable goes to it unjournaled
   * (it can never apply) instead of the journal's 503. Never set with a ledger that can apply a re-enable.
   */
  reEnableRefusedByLedger?: boolean;
}

type Env = { Variables: { correlationId: string; identity: IngestIdentity; body: Uint8Array; custody:CurrentCustodyPacket|undefined; journalWitness: { tenant: string; seq: number } | undefined } };

/** Node 24 always has zstd; published most preferred first, as relayhistory. */
export function contentEncodings(): ContentEncoding[] {
  return typeof zstdDecompressSync === "function" ? ["zstd", "gzip"] : ["gzip"];
}

const tooLarge = () => new DeliveryError("delivery_too_large", 413, `Delivery request exceeds ${MAX_DELIVERY_BYTES} bytes`);
const unsupported = () => new DeliveryError("unsupported_encoding", 415, "Delivery request Content-Encoding is unsupported");

export interface BodyRead {
  bytes: number;
  wireBytes: number;
  encoding: ContentEncoding | "identity";
}

async function readBounded(stream: ReadableStream<Uint8Array>, limit: number): Promise<Uint8Array> {
  const reader = stream.getReader();
  const chunks: Uint8Array[] = [];
  let size = 0;
  try {
    for (;;) {
      const chunk = await reader.read();
      if (chunk.done) break;
      size += chunk.value.byteLength;
      if (size > limit) {
        await reader.cancel();
        throw tooLarge();
      }
      chunks.push(chunk.value);
    }
  } finally {
    reader.releaseLock();
  }
  const out = new Uint8Array(size);
  let offset = 0;
  for (const c of chunks) {
    out.set(c, offset);
    offset += c.byteLength;
  }
  return out;
}

/** relayhistory's `boundedJson`, in Node: the cap bounds wire AND decoded bytes; Content-Length is never trusted. */
export async function boundedJson(request: Request, read: BodyRead): Promise<unknown> {
  const header = request.headers.get("content-encoding")?.trim().toLowerCase() ?? "";
  const encoding: BodyRead["encoding"] = header === "" || header === "identity" ? "identity" : (contentEncodings() as string[]).includes(header) ? (header as ContentEncoding) : (() => { throw unsupported(); })();
  read.encoding = encoding;
  if (!request.body) throw new DeliveryError("invalid_delivery", 400, "Delivery request body is required");
  const wire = await readBounded(request.body, MAX_DELIVERY_BYTES);
  read.wireBytes = wire.byteLength;
  let decoded = wire;
  if (encoding === "gzip") {
    try {
      decoded = await readBounded(new Blob([wire]).stream().pipeThrough(new DecompressionStream("gzip")), MAX_DELIVERY_BYTES);
    } catch (error) {
      if (error instanceof DeliveryError) throw error;
      throw new DeliveryError("invalid_delivery", 400, "Delivery request body is not valid gzip");
    }
  } else if (encoding === "zstd") {
    try {
      decoded = new Uint8Array(zstdDecompressSync(wire, { maxOutputLength: MAX_DELIVERY_BYTES }));
    } catch (error) {
      if (error instanceof RangeError) throw tooLarge();
      throw new DeliveryError("invalid_delivery", 400, "Delivery request body is not valid zstd");
    }
  }
  read.bytes = decoded.byteLength;
  try {
    return JSON.parse(new TextDecoder("utf-8", { fatal: true, ignoreBOM: false }).decode(decoded));
  } catch {
    throw new DeliveryError("invalid_delivery", 400, "Delivery request must be valid UTF-8 JSON");
  }
}

function errorBody(c: Context<Env>, code: string, message: string, extra: Record<string, unknown> = {}) {
  return { error: { code, message, ...extra }, correlationId: c.get("correlationId") ?? "" };
}

/** The 503 diagnosis on the route's log line: stage, cause code and, for a database cause,
 * the closed diagnosis (heuristic class, safe code, basis; never a message). A raw ledger
 * error that reached the route outside a delivery stage is reported the same way.
 * Diagnostic only. */
function unavailableFields(error: unknown): Record<string, unknown> {
  if (error instanceof IngestUnavailable || error instanceof ClaimLockBusy) return { stage: error.stage, error_code: error.cause_code, ...dbFields(error.db) };
  if (error instanceof LedgerUnavailableError) return { error_code: error.code, ...dbFields(error.db) };
  return {};
}

/** Lock/timeout alert stages (runbooks/history-620-operations.md, "Lock-wait alert"). The two
 * Worker-side seat-clock callers: `lazy_claim` (stage "claim" in buffered mode =
 * ingest_claim_buffered_batch) and `staged_ack` (stage "buffer_receipt" = ingest_commit_buffered_batch).
 * Plus `staged_intent` (stage "buffer_intent" = ingest_prepare_buffer_stage): it takes no seat-clock lock,
 * but it waits on the same fence, batch and stage-intent rows the staged ACK holds FOR UPDATE, so a
 * long ACK surfaces there first. set_live / control_changed / lazy_clear have no Worker call site; they
 * alert from the operator/DB logs. */
export type LockAlertStage = "lazy_claim" | "staged_ack" | "staged_intent" | "set_live" | "control_changed" | "lazy_clear";
const LOCK_ALERT_SQLSTATES = new Set(["55P03", "57014"]);

/** The one alert line for a 55P03 (lock_not_available) / 57014 (query_canceled) ledger failure on a
 * lock-alert stage: `{ route, stage, sqlstate }` and nothing else (no tenant data, no ids). Diagnostic only.
 * A claim lock wait refused as 429 (ClaimLockBusy, INGEST_CLAIM_LOCK_BUSY=on) still alerts for 55P03
 * (a 40P01 alerts in neither flag state); a claim statement timeout refused as 429
 * (INGEST_CLAIM_TIMEOUT_BUSY=on) still alerts for 57014. */
export function lockAlertFields(error: unknown, acceptanceMode: string | undefined): { route: "ingest.lock_alert"; stage: LockAlertStage; sqlstate: string } | undefined {
  if (!(error instanceof IngestUnavailable || error instanceof ClaimLockBusy)) return undefined;
  const sqlstate = error.db?.db_error_code;
  if (sqlstate === undefined || !LOCK_ALERT_SQLSTATES.has(sqlstate)) return undefined;
  const stage: LockAlertStage | undefined =
    error.stage === "claim" && acceptanceMode === "buffered" ? "lazy_claim"
      : error.stage === "buffer_receipt" ? "staged_ack"
      : error.stage === "buffer_intent" ? "staged_intent"
      : undefined;
  return stage ? { route: "ingest.lock_alert", stage, sqlstate } : undefined;
}

function dbFields(db: DbErrorDiagnosis | undefined): Record<string, unknown> {
  return db ? { db_error_class: db.db_error_class, db_error_code: db.db_error_code, db_error_basis: db.db_error_basis } : {};
}

function failure(c: Context<Env>, error: unknown) {
  const safe = error instanceof DeliveryError ? error : new DeliveryError("delivery_unavailable", 503, "Durable delivery is temporarily unavailable");
  // A busy refusal is retryable and wrote nothing durable; tell the client when (relayhistory's header).
  if (safe instanceof DeliveryBusyError) c.header("Retry-After", String(safe.retryAfterSeconds));
  // Held for consent: a long wait, and the 503 keeps every released Desktop's batch on disk for the retry.
  if (safe instanceof UploadConsentRequired) c.header("Retry-After", String(safe.retryAfterSeconds));
  // Acquisition refusal (design §5.6): the refused sessions sit beside `error`, not inside it.
  if (safe instanceof AcquisitionRefusedError)
    return c.json({ ...errorBody(c, safe.code, safe.message), refused: safe.refused.map((r) => ({ source: r.source, sessionId: r.sessionId, code: r.code })) }, 409);
  return c.json(
    errorBody(c, safe.code, safe.message, {
      ...(safe.conflict ? { conflict: safe.conflict } : {}),
      ...(safe.conflicts ? { conflicts: safe.conflicts } : {}),
      ...(safe.conflictCount ? { conflictCount: safe.conflictCount } : {}),
    }),
    safe.status,
  );
}

/**
 * Whether a failed privacy-control call certainly changed nothing: a typed refusal, or a database error
 * carrying a real SQLSTATE (a single autocommit statement that errored rolled back). Connection-class
 * (08xxx), server-shutdown (57P0x) and code-less transport failures are ambiguous.
 */
export function definitelyNotApplied(error: unknown): boolean {
  if (error instanceof DeliveryError && !(error instanceof IngestUnavailable)) return error.status < 500;
  const db = error instanceof LedgerUnavailableError || error instanceof IngestUnavailable ? error.db : undefined;
  const code = db?.db_error_code ?? "";
  return /^[0-9A-Z]{5}$/.test(code) && !code.startsWith("08") && !code.startsWith("57P0");
}

/** The buffered body as a Request again, for `boundedJson` (decoding per Content-Encoding). */
function bufferedRequest(c: Context<Env>): Request {
  return new Request(c.req.raw.url, { method: c.req.raw.method, headers: c.req.raw.headers, body: c.get("body") });
}

async function jsonOf(c: Context<Env>,read:BodyRead={bytes:0,wireBytes:0,encoding:"identity"}): Promise<unknown> {
  if(c.req.header("content-type")?.split(";")[0]?.trim()!==CURRENT_CUSTODY_CONTENT_TYPE)return boundedJson(bufferedRequest(c),read);
  const decoded=decodeCustodyEnvelope(c.get("body"));c.set("custody",decoded.packet);
  const headers=new Headers(c.req.raw.headers);headers.set("content-encoding",decoded.encoding);headers.delete("content-length");
  return boundedJson(new Request(c.req.raw.url,{method:c.req.raw.method,headers,body:decoded.original}),read);
}
function custodyHeader(c:Context<Env>,ack:unknown){
 const packet=c.get("custody");if(!packet)return;
 const value=JSON.stringify(validatedCustodyAck(ack,packet));
 if(Buffer.byteLength(value)>512)throw new IngestUnavailable("fence","custody_ack_invalid");
 c.header(CURRENT_CUSTODY_ACK_HEADER,value);
}

const invalid = (message: string): never => {
  throw new DeliveryError("invalid_delivery", 400, message);
};
const isObject = (v: unknown): v is Record<string, unknown> => !!v && typeof v === "object" && !Array.isArray(v);
function exactKeys(v: Record<string, unknown>, keys: string[]) {
  const got = Object.keys(v).sort();
  if (got.length !== keys.length || got.some((k, i) => k !== [...keys].sort()[i])) invalid("Unexpected or missing field");
}
/** relayhistory's `identifier`: 1..512 UTF-8 bytes, no control characters, well-formed. */
function identifier(v: unknown): v is string {
  return typeof v === "string" && v.length > 0 && new TextEncoder().encode(v).length <= 512 && !/[\x00-\x1f\x7f]/.test(v) && !/[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/.test(v);
}

export function parseSelection(body: unknown): { account_id: string; expected_user_id?: string; origin_id: string; sessions: { source: string; session_id: string }[]; enabled: boolean } {
  if (!isObject(body)) invalid("Expected upload selection");
  const b = body as Record<string, unknown>;
  const expectedUser = Object.hasOwn(b, "expected_user_id");
  exactKeys(b, ["account_id", "origin_id", "sessions", "enabled", ...(expectedUser ? ["expected_user_id"] : [])]);
  if (expectedUser && !identifier(b.expected_user_id)) invalid("Invalid expected upload user");
  if (!identifier(b.origin_id) || typeof b.enabled !== "boolean" || !Array.isArray(b.sessions) || b.sessions.length < 1 || b.sessions.length > 2048)
    invalid("Invalid upload selection");
  for (const x of b.sessions as unknown[]) {
    if (!isObject(x)) invalid("Invalid session identity");
    exactKeys(x as Record<string, unknown>, ["source", "session_id"]);
    if (!identifier((x as Record<string, unknown>).source) || !identifier((x as Record<string, unknown>).session_id)) invalid("Invalid session identity");
  }
  return b as never;
}

/** POST /v1/upload-consent body. Known keys only (each optional); at most 2048 sessions per list. */
export function parseConsentWrite(body: unknown): ConsentWrite {
  if (!isObject(body)) invalid("Expected upload consent");
  const b = body as Record<string, unknown>;
  const allowed = new Set(["mode", "allow", "deny", "confirm_origins", "release_hold", "expected_revision"]);
  if (Object.keys(b).some((k) => !allowed.has(k))) invalid("Unexpected or missing field");
  const sessions = (v: unknown): { source: string; sessionId: string }[] | undefined => {
    if (v === undefined) return undefined;
    if (!Array.isArray(v) || v.length > 2048) invalid("Invalid upload consent sessions");
    return (v as unknown[]).map((x) => {
      if (!isObject(x)) invalid("Invalid upload consent session");
      const r = x as Record<string, unknown>;
      exactKeys(r, ["source", "session_id"]);
      if (!identifier(r.source) || !identifier(r.session_id)) invalid("Invalid upload consent session");
      return { source: r.source as string, sessionId: r.session_id as string };
    });
  };
  const write: ConsentWrite = {};
  if (b.mode !== undefined) {
    if (b.mode !== "selected" && b.mode !== "new" && b.mode !== "all") invalid("Invalid upload consent mode");
    write.mode = b.mode as ConsentWrite["mode"];
  }
  const allow = sessions(b.allow), deny = sessions(b.deny);
  if (allow) write.allow = allow;
  if (deny) write.deny = deny;
  if (b.confirm_origins !== undefined) {
    if (!Array.isArray(b.confirm_origins) || b.confirm_origins.length > 64 || !b.confirm_origins.every(identifier)) invalid("Invalid upload consent origins");
    write.confirmOrigins = b.confirm_origins as string[];
  }
  if (b.release_hold !== undefined) {
    if (typeof b.release_hold !== "boolean") invalid("Invalid upload consent hold release");
    if (b.release_hold) write.releaseHold = true;
  }
  if (b.expected_revision !== undefined) {
    if (!Number.isSafeInteger(b.expected_revision) || (b.expected_revision as number) < 0) invalid("Invalid upload consent revision");
    write.expectedRevision = b.expected_revision as number;
  }
  validateConsentWrite(write);
  return write;
}

export function parseReset(body: unknown): { account_id: string; request_id: string } {
  if (!isObject(body)) invalid("Expected session reset");
  const b = body as Record<string, unknown>;
  exactKeys(b, ["account_id", "request_id", "confirmation"]);
  if (b.confirmation !== "DELETE ALL SESSIONS" || !identifier(b.request_id)) invalid("Type DELETE ALL SESSIONS to confirm the reset");
  return b as never;
}

export function createIngestApp(deps: IngestAppDeps) {
  const app = new Hono<Env>();
  const emit = deps.log ?? ((fields) => log.info("session ingest", fields));
  const custodyMode = deps.custodySource ?? "relayhistory";
  const capturePolicyUploads: CapturePolicyUploadsMode = deps.capturePolicyUploads ?? "report";
  const asyncMode: AsyncAdmissionMode = deps.asyncAdmission?.mode ?? "off";
  const selfhostAdmission = deps.asyncAdmission?.selfhost;
  const uploadDirect = deps.uploadDirect ?? UPLOAD_DIRECT_OFF;
  if (asyncMode !== "off" && deps.deliver.acceptanceMode !== "buffered" && !selfhostAdmission) throw new Error("session ingest: async admission requires buffered acceptance");
  const nowMs = deps.asyncAdmission?.nowMs ?? Date.now;
  const syncFrozenReceipt = deps.syncAdmissionFrozenReceipt === "on";
  /**
   * A reset or re-enable, bracketed by the privacy journal (async-admission.ts) while async admission
   * may have uploads in flight (on / drain). Only ORDERED controls: buffered SQL refuses any other
   * (ingest_apply_ordered_control requires the control), so one is refused here before SQL with the
   * same 503. begin BEFORE the SQL (fail closed: no journal, no control); after it, settle with
   * SQL's own outcome (applied | replay | stale), or abort when it definitely did not apply.
   */
  const journaled = async <T>(tenant: { orgId: string; workspaceId: string; userId: string }, control: OrderedControl | undefined, run: () => Promise<T>, outcomeOf: (r: T) => "applied" | "replay" | "stale"): Promise<T> => {
    if (asyncMode === "off") return run();
    if (!control) throw new IngestUnavailable("privacy_journal", "control_unordered");
    const { store, keyRoot } = deps.deliver.target;
    const id = { epoch: control.epoch, version: control.version };
    let begun;
    try { begun = await beginPrivacyControl(store, keyRoot, tenant, id, nowMs()); } catch { throw new IngestUnavailable("privacy_journal", "privacy_journal_unavailable"); }
    let result: T;
    try {
      result = await run();
    } catch (error) {
      // Definitely not applied (a typed 4xx, or a SQL error with a SQLSTATE: the statement rolled back):
      // drop the pending mark. Anything ambiguous (a dropped connection) stays pending: TTL, then the
      // consumer counts it conservatively and keeps it ambiguous for SQL to resolve on replay.
      if (begun === "new" && definitelyNotApplied(error)) await abortPrivacyControl(store, keyRoot, tenant, id).catch(() => {});
      throw error;
    }
    try {
      await settlePrivacyControl(store, keyRoot, tenant, id, begun, outcomeOf(result));
    } catch {
      // Applied but not settled: answer 503 so the caller retries (SQL replays; settlement runs again).
      throw new IngestUnavailable("privacy_journal", "privacy_journal_completion");
    }
    return result;
  };
  const privacyAuthority = deps.privacyAuthority ?? "neon";
  if (privacyAuthority !== "neon" && (privacyAuthority !== "supabase" || custodyMode !== "frozen-supabase"))
    throw new Error("session ingest: privacyAuthority=supabase requires custodySource=frozen-supabase");
  const frozenWitness = async (input: FrozenWitnessInput): Promise<CurrentCustodyPacket> => {
    if (!deps.deliver.ledger.frozenWitness) throw new IngestUnavailable("fence", "custody_source_unavailable");
    return deps.deliver.ledger.frozenWitness(input);
  };
  const batchWitnessInput = async (tenant: LedgerTenant, batch: Parameters<typeof prepareDelivery>[1]): Promise<FrozenWitnessInput> => {
    const p = await prepareDelivery(tenant, batch);
    return { kind: "batch", tenant, originId: p.originId, batchId: p.batchId, batchDigest: p.batchDigest, originalRecordCount: (batch as { records: unknown[] }).records.length,
      candidates: p.records.map((r, i) => ({ preparedOrd: i, recordId: r.record_id, revision: String(r.revision), revisionId: r.revision_id, candidateDigest: r.digest, source: r.source, sessionId: r.session_id ?? null })) };
  };
  const controlWitnessInput = (kind: "selection" | "reset", tenant: LedgerTenant, originId: string | null): FrozenWitnessInput =>
    ({ kind, tenant, originId, batchId: null, batchDigest: null, originalRecordCount: 0, candidates: [] });
  /** Shadow compare: never throws, never changes the response. Logs no identifiers or content. */
  const shadow = async (route: string, received: CurrentCustodyPacket, input: () => Promise<FrozenWitnessInput> | FrozenWitnessInput) => {
    if (custodyMode !== "shadow") return;
    try {
      const frozen = await frozenWitness(await input());
      const differing = frozen.sourceDigest === received.sourceDigest ? [] : Object.keys({ ...received.witness, ...frozen.witness }).filter((k) =>
        JSON.stringify((received.witness as unknown as Record<string, unknown>)[k]) !== JSON.stringify((frozen.witness as unknown as Record<string, unknown>)[k])).sort();
      emit({ route: "custody.shadow", source_route: route, outcome: differing.length ? "mismatch" : "match", differing_fields: differing.join(",") || undefined });
    } catch (error) {
      emit({ route: "custody.shadow", source_route: route, outcome: "frozen_unavailable", error_code: (error as { code?: string; cause_code?: string })?.cause_code ?? (error as { code?: string })?.code ?? "error" });
    }
  };

  app.use("*", async (c, next) => {
    const requestId = crypto.randomUUID();
    c.set("correlationId", c.req.header("X-Correlation-Id")?.trim().slice(0, 128) || requestId);
    await next();
    c.header("X-Request-Id", requestId);
    c.header("X-Correlation-Id", c.get("correlationId"));
  });

  app.get("/healthz", (c) => c.json({ ok: true }));

  // Outside /v1, so neither the identity middleware below nor the /v1/* fallback sees it.
  registerSessionEraseRoutes(app, deps.sessionErase ?? SESSION_ERASE_DISABLED, emit, deps.sessionEraseService);

  // Identity for /v1, relayhistory's codes: 401 missing_authorization / invalid_token, 403 forbidden.
  const identify = deps.identify ?? signedBindingIdentity(deps.signingKey);
  app.use("/v1/*", async (c, next) => {
    // Async admission fence: witness the claimed user's privacy journal BEFORE any body IO, so a reset /
    // re-enable that completes while this upload is still streaming is after the witness. The identity is
    // not verified yet; the witness is kept only if the verified identity names the same tenant.
    c.set("journalWitness", undefined);
    const path = new URL(c.req.url).pathname;
    if (asyncMode === "on" && c.req.method === "POST" && (path === "/v1/delivery/batches" || path === "/v1/turns/batches")) {
      try {
        const claimed = selfhostAdmission ? selfhostAdmission.identityOf(c.req.raw) : bindingIdentity(c.req.raw);
        if (!claimed) throw new Error("no_identity");
        if (claimed.admission === ADMISSION_ACCEPT_VALUE) {
          const tenant = { orgId: claimed.orgId, workspaceId: claimed.workspaceId, userId: claimed.userId };
          c.set("journalWitness", { tenant: JSON.stringify(tenant), seq: await witnessPrivacyJournal(deps.deliver.target.store, deps.deliver.target.keyRoot, tenant) });
        }
      } catch {
        // No witness: this request takes the synchronous path.
      }
    }
    // The body is read once, bounded by the wire cap, so the signature covers exactly what is parsed.
    let body: Uint8Array;
    try {
      const contentType=c.req.header("content-type")?.split(";")[0]?.trim();
      const privateWire=contentType===CURRENT_CUSTODY_CONTENT_TYPE||contentType===RECOVERY_CONTENT_TYPE;
      if(privateWire&&(deps.deliver.acceptanceMode!=="buffered"||!["","identity"].includes(c.req.header("content-encoding")?.trim().toLowerCase()??"")))throw new IngestUnavailable("fence","custody_wire_unavailable");
      // C3: under Supabase privacy authority a relayhistory-built (Neon) witness is stale by definition.
      if(privateWire&&privacyAuthority==="supabase")throw new IngestUnavailable("fence","privacy_authority_moved");
      body = c.req.raw.body ? await readBounded(c.req.raw.body, privateWire?PRIVATE_CUSTODY_BYTES:MAX_DELIVERY_BYTES) : new Uint8Array();
    } catch (error) {
      return failure(c, error);
    }
    c.set("body", body);
    try {
      c.set("identity", await identify(c.req.raw, body));
      const id = c.get("identity"), witness = c.get("journalWitness");
      if (witness && witness.tenant !== JSON.stringify({ orgId: id.orgId, workspaceId: id.workspaceId, userId: id.userId })) c.set("journalWitness", undefined);
    } catch (error) {
      if (error instanceof AuthError) return c.json(errorBody(c, error.code, error.message), error.status);
      emit({ route: "auth", outcome: "error", error_code: "identity_failed" });
      return c.json(errorBody(c, "invalid_token", "invalid or expired token"), 401);
    }
    // The onboarding status reads are the only rth:read routes here; everything else needs rth:sync.
    const pathname = new URL(c.req.url).pathname;
    // The consent route takes a sync token (narrowing only) or a consent token (upload-consent.ts).
    // Hono's decoded path, the one routing matches (an encoded `/v1/upload-%63onsent` is this route too).
    if (c.req.path === UPLOAD_CONSENT_PATH) {
      if (!c.get("identity").scopes.some((s) => s === SYNC_SCOPE || s === CONSENT_SCOPE)) return c.json(errorBody(c, "forbidden", `missing required scope: ${SYNC_SCOPE}`), 403);
      await next();
      return;
    }
    const scope = c.req.method === "GET" && READ_PATHS.has(pathname) ? READ_SCOPE : SYNC_SCOPE;
    if (!c.get("identity").scopes.includes(scope)) return c.json(errorBody(c, "forbidden", `missing required scope: ${scope}`), 403);
    await next();
  });

  // P5b G2: relayhistory-cloud's onboarding heartbeat (same validation and response), stored
  // in history.probe_connections. relayhistory forwards here when ONBOARDING_BACKEND=ingest.
  app.post("/v1/onboarding/heartbeat", async (c) => {
    const identity = c.get("identity");
    if (!deps.onboarding) return c.json(errorBody(c, "not_configured", "onboarding is not configured"), 503);
    if (!identity.workspaceId) return c.json({ error: "Workspace required" }, 403);
    let parsed;
    try {
      parsed = heartbeatProgress(c.get("body"));
    } catch (error) {
      if (error instanceof ProgressError || error instanceof SyntaxError) return c.json({ error: "Invalid progress" }, 400);
      throw error;
    }
    if ("tooLarge" in parsed) return c.json({ error: "Progress too large" }, 413);
    try {
      const lastSeenAt = await recordHeartbeat(deps.onboarding.run, identity, parsed.progress);
      emit({ route: "onboarding.heartbeat", outcome: "ok", org_id: identity.orgId });
      return c.json({ connected: true, lastSeenAt });
    } catch (error) {
      emit({ route: "onboarding.heartbeat", outcome: "error", error_code: (error as { code?: string }).code ?? "onboarding_failed" });
      return c.json(errorBody(c, "onboarding_unavailable", "onboarding is temporarily unavailable"), 503);
    }
  });

  app.get("/v1/onboarding/connection", async (c) => {
    const identity = c.get("identity");
    if (!deps.onboarding) return c.json(errorBody(c, "not_configured", "onboarding is not configured"), 503);
    if (!identity.workspaceId) return c.json({ error: "Workspace required" }, 403);
    try {
      return c.json(await readConnection(deps.onboarding.run, identity));
    } catch (error) {
      emit({ route: "onboarding.connection", outcome: "error", error_code: (error as { code?: string }).code ?? "onboarding_failed" });
      return c.json(errorBody(c, "onboarding_unavailable", "onboarding is temporarily unavailable"), 503);
    }
  });

  // Zero-Neon lane D / W4: the session counters of relayhistory's onboarding status
  // (sessionCount, lastReceivedAt, sources), from the ingest ledger plus the imported
  // pre-cutover rollups. relayhistory reads this when ONBOARDING_STATUS_BACKEND=ingest.
  app.get("/v1/onboarding/receipt", async (c) => {
    const identity = c.get("identity");
    if (!deps.onboarding) return c.json(errorBody(c, "not_configured", "onboarding is not configured"), 503);
    if (!identity.workspaceId) return c.json({ error: "Workspace required" }, 403);
    try {
      return c.json(await readSessionReceipt(deps.onboarding.run, identity));
    } catch (error) {
      emit({ route: "onboarding.receipt", outcome: "error", error_code: (error as { code?: string }).code ?? "onboarding_failed" });
      return c.json(errorBody(c, "onboarding_unavailable", "onboarding is temporarily unavailable"), 503);
    }
  });

  app.get("/v1/delivery/limits", (c) => c.json({ ...DELIVERY_LIMITS, contentEncodings: contentEncodings() }));

  // One acceptance path for the public delivery shape and the private turns shape: the
  // same custody (relayhistory witness, shadow, or frozen-supabase), claim, buffer and receipt.
  const acceptBatch = async (c: Context<Env>, route: string, parse: (value: unknown, identity: IngestIdentity) => HistoryExportBatch) => {
    const started = Date.now();
    // The upload's start, in the same clock the privacy journal uses: taken before any IO.
    const receivedAtMs = nowMs();
    const identity = c.get("identity");
    const read: BodyRead = { bytes: 0, wireBytes: 0, encoding: "identity" };
    const fields: Record<string, unknown> = { route, org_id: identity.orgId, workspace_id: identity.workspaceId, correlation_id: c.get("correlationId") };
    // Counts and the outcome class only: never a batch, record or session id, and never content.
    const completed = (native: { outcome?: string; status?: number }, records: number) => captureSafely(deps.analytics, {
      kind: "session_ingest.batch_completed", traceId: c.get("correlationId"), orgId: identity.orgId, workspaceId: identity.workspaceId,
      route: route === "turns.batches" ? "turns" : "delivery", outcome: ingestOutcome(native), records, durationMs: Date.now() - started,
    });
    try {
      const batch = parse(await jsonOf(c, read), identity);
      // Capture policy (capture-policy.ts): self-host identities carry the org's policy. Report logs, enforce refuses.
      const violation = uploadPolicyViolation(identity, c.req.header(CAPTURE_POLICY_REVISION_HEADER), batch);
      if (violation) {
        emit({ ...fields, metric: "capture_policy_upload_violation", outcome: violation, mode: capturePolicyUploads, revision: identity.capturePolicy!.revision });
        if (capturePolicyUploads === "enforce") throw uploadPolicyError(violation, identity.capturePolicy!.revision);
      }
      const tenant = { orgId: identity.orgId, workspaceId: identity.workspaceId, userId: identity.userId };
      // Upload consent (upload-consent.ts): before any enqueue, claim or write. Explicit denies wait for the
      // post-claim check, which knows which sessions the deletion fence will drop anyway.
      // The operator's enforced sharing mode caps the person's choice (capture-policy.ts operatorSharingCap).
      // Only with INGEST_UPLOAD_CONSENT on: with it off (the default) capture enforce behaves exactly as on main.
      const operator = deps.uploadConsent && deps.uploadConsent.enforcement !== "off" ? operatorSharingCap(identity, capturePolicyUploads) : undefined;
      await checkUploadConsent(deps.uploadConsent, tenant, batch.origin_id, batch.records, { deferDenied: true, phase: "precheck", route, ...(operator ? { operator } : {}) }, (f) => emit({ ...fields, ...f }));
      // Async admission: no Supabase call. Only for a caller whose SIGNED identity accepts the queued ACK,
      // with a pre-body journal witness, and never for a relayhistory-built custody envelope (whose ACK
      // header must come from the claim).
      const witness = c.get("journalWitness");
      if (asyncMode === "on" && witness && identity.admission === ADMISSION_ACCEPT_VALUE && !c.get("custody")) {
        const prepared = await prepareForAdmission(deps.deliver, tenant, batch, receivedAtMs);
        try {
          await enqueueAdmission({ store: deps.deliver.target.store, keyRoot: deps.deliver.target.keyRoot, enqueue: deps.asyncAdmission!.enqueue }, tenant, prepared, receivedAtMs, witness.seq);
        } catch (error) {
          if (error instanceof DeliveryError) throw error;
          throw new IngestUnavailable("admission_enqueue", "admission_enqueue_failed");
        }
        emit({ ...fields, outcome: "accepted_queued", count: prepared.records.length, duration_ms: Date.now() - started, bytes_raw: read.bytes });
        completed({ outcome: "accepted_queued" }, prepared.records.length);
        c.header(ADMISSION_HEADER, ADMISSION_QUEUED);
        return c.json({ ...prepared.receipt, limits: { ...DELIVERY_LIMITS, contentEncodings: contentEncodings() } });
      }
      // Self-host "on": never a synchronous fallback (that would put SQL back on the request path).
      if (asyncMode === "on" && selfhostAdmission) throw new IngestUnavailable("admission_witness", witness ? "admission_not_queueable" : "privacy_journal_unavailable");
      // INGEST_SYNC_ADMISSION_FROZEN_RECEIPT: a queued-v1 caller's relayhistory skipped its pre-forward legacy-receipt
      // read, so a batch that fell back here gets the consumer's read first (admission-consumer.ts), before any claim.
      if (syncFrozenReceipt && identity.admission === ADMISSION_ACCEPT_VALUE && custodyMode === "frozen-supabase") {
        if (!deps.deliver.ledger.frozenReceipt) throw new IngestUnavailable("fence", "custody_source_unavailable");
        const prepared = await prepareDelivery({ orgId: tenant.orgId, workspaceId: tenant.workspaceId }, batch);
        const stored = await deps.deliver.ledger.frozenReceipt({ tenant, originId: prepared.originId, batchId: prepared.batchId });
        if (stored) {
          if (stored.digest !== prepared.batchDigest) throw batchIdConflict(prepared, stored.digest);
          // No claim, so no custody ACK header; the admission header names the legacy replay instead.
          emit({ ...fields, outcome: "legacy_replay", count: prepared.records.length, duration_ms: Date.now() - started, bytes_raw: read.bytes });
          completed({ outcome: "legacy_replay" }, prepared.records.length);
          c.header(ADMISSION_HEADER, ADMISSION_LEGACY_REPLAY);
          // receiptId is supplied explicitly: a stored receipt without a string one falls back to the prepared receipt's id.
          return c.json({ ...stored.receipt, receiptId: typeof stored.receipt.receiptId === "string" ? stored.receipt.receiptId : prepared.receipt.receiptId, limits: { ...DELIVERY_LIMITS, contentEncodings: contentEncodings() } });
        }
      }
      if (!c.get("custody") && custodyMode === "frozen-supabase") c.set("custody", await frozenWitness(await batchWitnessInput(tenant, batch)));
      else if (c.get("custody")) await shadow(route, c.get("custody")!, () => batchWitnessInput(tenant, batch));
      const custody=c.get("custody");
      if(custody&&custody.witness.originalRecordCount!==batch.records.length)throw new IngestUnavailable("fence","custody_record_count");
      const consentGate = (deps.uploadConsent && deps.uploadConsent.enforcement !== "off") || operator
        ? async (dropped: ReadonlySet<string>, phase: "admit" | "commit") => { await checkUploadConsent(deps.uploadConsent, tenant, batch.origin_id, batch.records, { dropped, phase, route, ...(operator ? { operator } : {}) }, (f) => emit({ ...fields, ...f })); }
        : undefined;
      const result = await deliverBatch({...deps.deliver,...(custody?{custody}:{}),...(consentGate?{consentGate}:{})}, { orgId: identity.orgId, workspaceId: identity.workspaceId, userId: identity.userId }, batch);
      emit({ ...fields, outcome: result.outcome, count: result.stats.records, manifests: result.stats.manifests_written, deleted: result.stats.deleted_sessions, state: result.stats.replica, duration_ms: Date.now() - started, bytes_raw: read.bytes });
      completed({ outcome: result.outcome }, result.stats.records);
      custodyHeader(c,result.custodyAck);
      return c.json({ ...result.receipt, limits: { ...result.receipt.limits, contentEncodings: contentEncodings() } });
    } catch (error) {
      const code = error instanceof DeliveryError ? error.code : "delivery_unavailable";
      emit({
        ...fields,
        outcome: code,
        ...unavailableFields(error),
        ...(error instanceof DeliveryError && error.conflictCount ? { count: error.conflictCount } : {}),
        duration_ms: Date.now() - started,
        bytes_raw: read.bytes,
      });
      // failure() answers 409 for an acquisition refusal and the error's own status otherwise (503 when untyped).
      completed({ status: error instanceof AcquisitionRefusedError ? 409 : error instanceof DeliveryError ? error.status : 503 }, 0);
      const lockAlert = lockAlertFields(error, deps.deliver.acceptanceMode);
      if (lockAlert) emit(lockAlert);
      return failure(c, error);
    }
  };

  app.post("/v1/delivery/batches", (c) => acceptBatch(c, "delivery.batches", (value) => parseDeliveryRequest(value)));

  // Relay session journal turns (relayhistory TURNS_WRITE_PATH=dual|forward): turns.ts.
  // Same signed binding, rth:sync and custody as a batch; only the turn record shape.
  app.post("/v1/turns/batches", (c) => acceptBatch(c, "turns.batches", parseTurnBatch));

  // Private archive replay. Unlike the public batch route, this accepts the
  // exact prepared values retained by the old Neon replica archive. It is
  // available only while the receiver is in buffered mode and requires both
  // the normal exact-body HMAC and fresh current-source custody.
  app.post("/v1/delivery/recovery", async (c) => {
    const started = Date.now();
    const identity = c.get("identity");
    const fields: Record<string, unknown> = { route: "delivery.recovery", org_id: identity.orgId, workspace_id: identity.workspaceId, correlation_id: c.get("correlationId") };
    try {
      if (deps.deliver.acceptanceMode !== "buffered" || c.req.header("content-type")?.split(";")[0]?.trim() !== RECOVERY_CONTENT_TYPE)
        throw new IngestUnavailable("recovery", "recovery_wire_unavailable");
      const tenant = { orgId: identity.orgId, workspaceId: identity.workspaceId, userId: identity.userId };
      const { prepared, custody } = await decodeRecoveryEnvelope(c.get("body"), tenant);
      c.set("custody", custody);
      // Replaying an archive re-publishes it: the same consent check as a live batch.
      await checkUploadConsent(deps.uploadConsent, tenant, prepared.originId, prepared.records, { deferDenied: true, phase: "precheck", route: "delivery.recovery" }, (f) => emit({ ...fields, ...f }));
      const consentGate = deps.uploadConsent && deps.uploadConsent.enforcement !== "off"
        ? async (dropped: ReadonlySet<string>, phase: "admit" | "commit") => { await checkUploadConsent(deps.uploadConsent, tenant, prepared.originId, prepared.records, { dropped, phase, route: "delivery.recovery" }, (f) => emit({ ...fields, ...f })); }
        : undefined;
      const result = await deliverPrepared({ ...deps.deliver, custody, ...(consentGate ? { consentGate } : {}) }, tenant, prepared);
      emit({ ...fields, outcome: result.outcome, count: result.stats.records, manifests: result.stats.manifests_written, deleted: result.stats.deleted_sessions, state: result.stats.replica, duration_ms: Date.now() - started, bytes_raw: c.get("body").byteLength });
      custodyHeader(c, result.custodyAck);
      return c.json({ ...result.receipt, limits: { ...result.receipt.limits, contentEncodings: contentEncodings() } });
    } catch (error) {
      const code = error instanceof DeliveryError ? error.code : "delivery_unavailable";
      emit({ ...fields, outcome: code, ...unavailableFields(error), duration_ms: Date.now() - started, bytes_raw: c.get("body").byteLength });
      const lockAlert = lockAlertFields(error, deps.deliver.acceptanceMode);
      if (lockAlert) emit(lockAlert);
      return failure(c, error);
    }
  });

  // Deletion fences (relayhistory's selection and reset, written here right after Neon's own, by
  // relayhistory-cloud #265). Same bodies and answers as relayhistory (lib/delivery.ts
  // setSessionUploads / resetUploadedSessions); tenancy only from the signed identity.
  // Upload consent writes (upload-consent.ts). Tenancy only from the signed identity. A sync token may
  // only narrow; widening needs the consent scope, which only a person's confirmed action mints.
  app.post(UPLOAD_CONSENT_PATH, async (c) => {
    const identity = c.get("identity");
    try {
      if (!deps.uploadConsent || deps.uploadConsent.enforcement === "off") throw new IngestUnavailable("consent", "upload_consent_off");
      const write = parseConsentWrite(await jsonOf(c));
      const trusted = identity.scopes.includes(CONSENT_SCOPE);
      const tenant = { orgId: identity.orgId, workspaceId: identity.workspaceId, userId: identity.userId };
      const { revision } = await deps.uploadConsent.store.apply(tenant, write, { trusted, nowMs: (deps.uploadConsent.nowMs ?? Date.now)() });
      emit({ route: "upload_consent.write", outcome: "applied", org_id: identity.orgId, state: trusted ? "trusted" : "narrowing", revision });
      return c.json({ revision });
    } catch (error) {
      emit({ route: "upload_consent.write", outcome: error instanceof DeliveryError ? error.code : "upload_consent_unavailable", org_id: identity.orgId });
      return failure(c, error instanceof DeliveryError ? error : new IngestUnavailable("consent", "upload_consent_store"));
    }
  });

  app.post("/v1/delivery/sessions/selection", async (c) => {
    const identity = c.get("identity");
    try {
      const input = parseSelection(await jsonOf(c));
      await requireDeliveryAccount(identity, input.account_id);
      if (input.expected_user_id !== undefined && input.expected_user_id !== identity.userId)
        throw new DeliveryError("delivery_user_mismatch", 403, "Upload selection does not match authenticated user");
      // A disable is the caller deleting their own uploaded session: the capture policy may withdraw it.
      if (!input.enabled) assertUserDeleteAllowed(identity);
      const selection={
        tenant: { orgId: identity.orgId, workspaceId: identity.workspaceId, userId: identity.userId },
        originId: input.origin_id,
        sessions: input.sessions.map((x):[string,string] => [x.source, x.session_id]),
        enabled: input.enabled,
      };
      let controlAck;
      // A re-enable lifts a tombstone: an upload that began before it must not be admitted after it.
      // (A disable needs no journal: the consumer's claim already filters a deleted session.)
      const journal = <T>(run: () => Promise<T>, outcomeOf: (r: T) => "applied" | "replay" | "stale") => !input.enabled || (selfhostAdmission?.reEnableRefusedByLedger === true && !identity.control) ? run()
        : journaled(selection.tenant, identity.control, run, outcomeOf);
      {const t={ orgId: identity.orgId, workspaceId: identity.workspaceId, userId: identity.userId };
       if(identity.control&&!c.get("custody")&&custodyMode==="frozen-supabase")c.set("custody",await frozenWitness(controlWitnessInput("selection",t,input.origin_id)));
       else if(identity.control&&c.get("custody"))await shadow("delivery.sessions.selection",c.get("custody")!,()=>controlWitnessInput("selection",t,input.origin_id));}
      const custody=c.get("custody");if(custody&&!identity.control)throw new IngestUnavailable("fence","control_unavailable");
      if(identity.control){
        if(identity.control.kind!=="selection"||!deps.deliver.ledger.applyOrderedControl)throw new IngestUnavailable("fence","control_unavailable");
        const control=identity.control;
        const result=await journal(async()=>orderedControlResult(await deps.deliver.ledger.applyOrderedControl!({...selection,control:{...control,kind:"selection"},...(custody?{custody}:{})}),control),(r)=>r.control.outcome);
        controlAck=result.control;custodyHeader(c,result.custodyAck);
      }else await journal(()=>deps.deliver.ledger.setSessionUploads(selection),()=>"applied");
      // A "Don't upload" is also the principal's explicit deny (every origin, every mode, kept when the mode
      // becomes "all"). Narrowing only. A re-enable never broadens consent: that needs a confirmed write.
      if (!input.enabled && deps.uploadConsent && deps.uploadConsent.enforcement !== "off")
        await deps.uploadConsent.store.apply(selection.tenant, { deny: input.sessions.map((x) => ({ source: x.source, sessionId: x.session_id })) }, { trusted: false, nowMs: (deps.uploadConsent.nowMs ?? Date.now)() });
      emit({ route: "delivery.sessions.selection", outcome: controlAck?"control_"+controlAck.outcome:input.enabled ? "enabled" : "disabled", org_id: identity.orgId, count: input.sessions.length });
      return c.json({ updated: true,...(controlAck?{control:controlAck}:{}) });
    } catch (error) {
      emit({ route: "delivery.sessions.selection", outcome: error instanceof DeliveryError ? error.code : "delivery_unavailable", org_id: identity.orgId });
      return failure(c, error instanceof DeliveryError ? error : new IngestUnavailable("fence", "ledger"));
    }
  });
  app.post("/v1/delivery/sessions/reset", async (c) => {
    const identity = c.get("identity");
    try {
      const input = parseReset(await jsonOf(c));
      await requireDeliveryAccount(identity, input.account_id);
      // A reset deletes every session the caller uploaded: the capture policy may withdraw it.
      assertUserDeleteAllowed(identity);
      const tenant={ orgId: identity.orgId, workspaceId: identity.workspaceId, userId: identity.userId };
      let deleted:number,controlAck;
      const journal = <T>(run: () => Promise<T>, outcomeOf: (r: T) => "applied" | "replay" | "stale") =>
        journaled(tenant, identity.control, run, outcomeOf);
      {const t={ orgId: identity.orgId, workspaceId: identity.workspaceId, userId: identity.userId };
       if(identity.control&&!c.get("custody")&&custodyMode==="frozen-supabase")c.set("custody",await frozenWitness(controlWitnessInput("reset",t,null)));
       else if(identity.control&&c.get("custody"))await shadow("delivery.sessions.reset",c.get("custody")!,()=>controlWitnessInput("reset",t,null));}
      const custody=c.get("custody");if(custody&&!identity.control)throw new IngestUnavailable("fence","control_unavailable");
      if(identity.control){
        if(identity.control.kind!=="reset"||!deps.deliver.ledger.applyOrderedControl)throw new IngestUnavailable("fence","control_unavailable");
        const control=identity.control;
        const result=await journal(async()=>orderedControlResult(await deps.deliver.ledger.applyOrderedControl!({tenant,requestId:input.request_id,control:{...control,kind:"reset"},...(custody?{custody}:{})}),control),(r)=>r.control.outcome);
        deleted=result.deleted!;controlAck=result.control;custodyHeader(c,result.custodyAck);
      }else deleted=await journal(()=>deps.deliver.ledger.resetUploadedSessions(tenant,input.request_id),()=>"applied");
      emit({ route: "delivery.sessions.reset", outcome: controlAck?"control_"+controlAck.outcome:"reset", org_id: identity.orgId, deleted });
      return c.json({ reset: true, deleted,...(controlAck?{control:controlAck}:{}) });
    } catch (error) {
      emit({ route: "delivery.sessions.reset", outcome: error instanceof DeliveryError ? error.code : "delivery_unavailable", org_id: identity.orgId });
      return failure(c, error instanceof DeliveryError ? error : new IngestUnavailable("fence", "ledger"));
    }
  });

  // Supabase-first mirror of Neon's privacy-control head (P1 option a). relayhistory: begin here,
  // then Neon begin (must return the same {version,digest}), apply, Neon complete, then complete here.
  const controlKind = (v: unknown): "selection" | "reset" => (v === "selection" || v === "reset" ? v : invalid("Invalid control kind"));
  // relayhistory frozen-supabase mode: the two remaining Neon reads on POST /v1/delivery/batches.
  app.post("/v1/custody/legacy-receipt", async (c) => {
    const identity = c.get("identity");
    try {
      if (!deps.deliver.ledger.frozenReceipt) throw new IngestUnavailable("fence", "custody_source_unavailable");
      const b = await jsonOf(c);if (!isObject(b)) invalid("Expected batch identity");
      const v = b as Record<string, unknown>;exactKeys(v, ["originId", "batchId"]);
      if (!identifier(v.originId) || !identifier(v.batchId)) invalid("Invalid batch identity");
      const stored = await deps.deliver.ledger.frozenReceipt({ tenant: { orgId: identity.orgId, workspaceId: identity.workspaceId, userId: identity.userId }, originId: v.originId as string, batchId: v.batchId as string });
      return c.json({ stored });
    } catch (error) {
      emit({ route: "custody.legacy_receipt", outcome: error instanceof DeliveryError ? error.code : "delivery_unavailable", org_id: identity.orgId });
      return failure(c, error instanceof DeliveryError ? error : new IngestUnavailable("fence", "ledger"));
    }
  });
  // 200 {pending:false} when no privacy control is pending for this user, else 503 (fail closed).
  app.post("/v1/custody/pending", async (c) => {
    const identity = c.get("identity");
    try {
      const b = await jsonOf(c);if (!isObject(b)) invalid("Expected origin");
      const v = b as Record<string, unknown>;exactKeys(v, ["originId"]);
      if (!identifier(v.originId)) invalid("Invalid origin");
      await frozenWitness({ kind: "batch", tenant: { orgId: identity.orgId, workspaceId: identity.workspaceId, userId: identity.userId }, originId: v.originId as string,
        batchId: "pending-check", batchDigest: "0".repeat(64), originalRecordCount: 0, candidates: [] });
      return c.json({ pending: false });
    } catch (error) {
      emit({ route: "custody.pending", outcome: error instanceof DeliveryError ? error.code : "delivery_unavailable", org_id: identity.orgId });
      return failure(c, error instanceof DeliveryError ? error : new IngestUnavailable("fence", "ledger"));
    }
  });
  app.post("/v1/custody/control/begin", async (c) => {
    const identity = c.get("identity");
    try {
      if (privacyAuthority !== "neon") throw new IngestUnavailable("fence", "privacy_authority_moved");
      if (!deps.deliver.ledger.sourceControlBegin) throw new IngestUnavailable("fence", "control_unavailable");
      const b = await jsonOf(c);if (!isObject(b)) invalid("Expected control");
      exactKeys(b as Record<string, unknown>, ["epoch", "kind", "payload"]);
      const head = await deps.deliver.ledger.sourceControlBegin({ epoch: String((b as Record<string, unknown>).epoch), kind: controlKind((b as Record<string, unknown>).kind), payload: (b as Record<string, unknown>).payload,
        tenant: { orgId: identity.orgId, workspaceId: identity.workspaceId, userId: identity.userId } });
      emit({ route: "custody.control.begin", outcome: head.apply ? "begun" : "pending_replay", org_id: identity.orgId });
      return c.json(head);
    } catch (error) {
      emit({ route: "custody.control.begin", outcome: error instanceof DeliveryError ? error.code : "delivery_unavailable", org_id: identity.orgId });
      return failure(c, error instanceof DeliveryError ? error : new IngestUnavailable("fence", "ledger"));
    }
  });
  app.post("/v1/custody/control/complete", async (c) => {
    const identity = c.get("identity");
    try {
      if (privacyAuthority !== "neon") throw new IngestUnavailable("fence", "privacy_authority_moved");
      if (!deps.deliver.ledger.sourceControlComplete) throw new IngestUnavailable("fence", "control_unavailable");
      const b = await jsonOf(c);if (!isObject(b)) invalid("Expected control completion");
      const v = b as Record<string, unknown>;exactKeys(v, ["epoch", "version", "digest", "kind", "outcome"]);
      if (v.outcome !== "applied" && v.outcome !== "replay" && v.outcome !== "stale") invalid("Invalid control outcome");
      const r = await deps.deliver.ledger.sourceControlComplete({ epoch: String(v.epoch), version: String(v.version), digest: String(v.digest), kind: controlKind(v.kind), outcome: v.outcome as "applied" | "replay" | "stale",
        tenant: { orgId: identity.orgId, workspaceId: identity.workspaceId, userId: identity.userId } });
      emit({ route: "custody.control.complete", outcome: r.superseded ? "superseded" : "completed", org_id: identity.orgId });
      return c.json(r);
    } catch (error) {
      emit({ route: "custody.control.complete", outcome: error instanceof DeliveryError ? error.code : "delivery_unavailable", org_id: identity.orgId });
      return failure(c, error instanceof DeliveryError ? error : new IngestUnavailable("fence", "ledger"));
    }
  });

  // C3 Supabase-first privacy authority: relayhistory (DELIVERY_CUSTODY_SOURCE_MODE=supabase-authority) begins
  // here (head + frozen privacy written, Neon never called), writes the unchanged ordered fence, then completes here.
  app.post("/v1/custody/authority/begin", async (c) => {
    const identity = c.get("identity");
    try {
      if (privacyAuthority !== "supabase" || !deps.deliver.ledger.privacyAuthorityBegin) throw new IngestUnavailable("fence", "privacy_authority_unavailable");
      const b = await jsonOf(c);if (!isObject(b)) invalid("Expected control");
      exactKeys(b as Record<string, unknown>, ["epoch", "kind", "payload"]);
      const v = b as Record<string, unknown>;
      if (typeof v.epoch !== "string") invalid("Invalid control epoch");
      const head = await deps.deliver.ledger.privacyAuthorityBegin({ epoch: v.epoch as string, kind: controlKind(v.kind), payload: v.payload,
        tenant: { orgId: identity.orgId, workspaceId: identity.workspaceId, userId: identity.userId } });
      emit({ route: "custody.authority.begin", outcome: head.apply ? "begun" : "pending_replay", org_id: identity.orgId });
      return c.json(head);
    } catch (error) {
      emit({ route: "custody.authority.begin", outcome: error instanceof DeliveryError ? error.code : "delivery_unavailable", org_id: identity.orgId });
      return failure(c, error instanceof DeliveryError ? error : new IngestUnavailable("fence", "ledger"));
    }
  });
  app.post("/v1/custody/authority/complete", async (c) => {
    const identity = c.get("identity");
    try {
      if (privacyAuthority !== "supabase" || !deps.deliver.ledger.privacyAuthorityComplete) throw new IngestUnavailable("fence", "privacy_authority_unavailable");
      const b = await jsonOf(c);if (!isObject(b)) invalid("Expected control completion");
      const v = b as Record<string, unknown>;exactKeys(v, ["epoch", "version", "digest", "kind", "outcome"]);
      if (v.outcome !== "applied" && v.outcome !== "replay" && v.outcome !== "stale") invalid("Invalid control outcome");
      if (typeof v.epoch !== "string" || typeof v.version !== "string" || typeof v.digest !== "string") invalid("Invalid control");
      const r = await deps.deliver.ledger.privacyAuthorityComplete({ epoch: v.epoch as string, version: v.version as string, digest: v.digest as string, kind: controlKind(v.kind),
        outcome: v.outcome as "applied" | "replay" | "stale", tenant: { orgId: identity.orgId, workspaceId: identity.workspaceId, userId: identity.userId } });
      emit({ route: "custody.authority.complete", outcome: r.superseded ? "superseded" : "completed", org_id: identity.orgId });
      return c.json(r);
    } catch (error) {
      emit({ route: "custody.authority.complete", outcome: error instanceof DeliveryError ? error.code : "delivery_unavailable", org_id: identity.orgId });
      return failure(c, error instanceof DeliveryError ? error : new IngestUnavailable("fence", "ledger"));
    }
  });

  // Async admission status (S3 only): admitted | refused (with its code) | pending.
  app.get("/v1/delivery/admission", async (c) => {
    const identity = c.get("identity");
    // HISTORY_UPLOAD_DIRECT (upload-admission.ts): `?uploadKey=` answers relay-upload-inbox-v1 §5. Flag off:
    // the query is ignored and the originId + receiptId form below answers, unchanged.
    const uploadKey = c.req.query("uploadKey");
    if (uploadKey !== undefined && uploadDirect.mode !== "off") {
      const answer = await uploadAdmissionStatus(deps.deliver.target, uploadDirect, { orgId: identity.orgId, workspaceId: identity.workspaceId, userId: identity.userId }, uploadKey);
      if (answer.retryAfterSeconds !== undefined) c.header("Retry-After", String(answer.retryAfterSeconds));
      if (answer.ok) return c.json(answer.body);
      emit({ route: "delivery.admission.upload", outcome: answer.code, org_id: identity.orgId });
      return c.json(errorBody(c, answer.code, answer.message), answer.status);
    }
    try {
      const originId = c.req.query("originId") ?? "", receiptId = c.req.query("receiptId") ?? "";
      if (!identifier(originId) || !/^rhr_[0-9a-f]{64}$/.test(receiptId)) invalid("Invalid admission query");
      return c.json(await admissionStatus(deps.deliver.target.store, deps.deliver.target.keyRoot, { orgId: identity.orgId, workspaceId: identity.workspaceId, userId: identity.userId }, originId, receiptId));
    } catch (error) {
      emit({ route: "delivery.admission", outcome: error instanceof DeliveryError ? error.code : "delivery_unavailable", org_id: identity.orgId });
      return failure(c, error instanceof DeliveryError ? error : new IngestUnavailable("admission_status", "store"));
    }
  });

  app.all("/v1/*", (c) => c.json(errorBody(c, "unsupported_by_backend", `${c.req.method} ${new URL(c.req.url).pathname} is not served by the session ingest`, { reason: "route_not_migrated" }), 501));
  app.notFound((c) => c.json(errorBody(c, "not_found", `No route for ${c.req.method} ${new URL(c.req.url).pathname}`), 404));
  app.onError((_err, c) => c.json(errorBody(c, "internal_error", "Internal server error"), 500));
  return app;
}
