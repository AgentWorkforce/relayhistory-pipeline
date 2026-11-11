/**
 * relay-upload-inbox-v1: the frozen contract for direct-to-S3 desktop uploads
 * (docs/decisions/0004-direct-upload-inbox.md, docs/contracts/relay-upload-inbox-v1.md).
 *
 * One definition for three parties: relay-control mints the credentials and the witness,
 * relay-desktop PUTs one object per delivery batch under the tenant's upload-inbox prefix,
 * and this pipeline's consumer re-validates, re-scrubs and admits it through the existing
 * async-admission path. Nothing here performs IO: no S3, no HTTP, no env reads (the key comes
 * in as a string through parseUploadWitnessKey). Nothing in the ingest path imports it yet.
 *
 *   key prefix   <keyRoot>/.upload-inbox/<org>/v1/ws=<ws>/u=<uh>/
 *   object key   <prefix>o=<originId>/b=<sha256(batchId)[:32]>/d=<sha256(objectBytes)[:32]>-w<witnessSeq>.json.zst
 *   witness      v1.<payloadB64url>.<macB64url>, mac = HMAC-SHA256(key, "relay-upload-witness-v1\n" + payloadB64url)
 *
 * `uh` is the same derivation as the privacy journal's `srv_` machine segment
 * (async-admission.ts `userDir`, used by `journalKey`), without the `srv_` prefix.
 *
 * Refusals are values (`{ code }`), never exceptions: the consumer writes them as the upload's
 * refused outcome. Exceptions are reserved for programming and configuration errors (a malformed
 * builder input, a witness key of the wrong length): those must never become a terminal refusal.
 */
import { createHash, createHmac, timingSafeEqual } from "node:crypto";
import { MAX_DELIVERY_BYTES } from "./vendor/delivery/delivery-contracts.ts";

export const UPLOAD_CONTRACT = "relay-upload-inbox-v1";
/**
 * A leading `.` keeps the upload inbox disjoint from every tenant prefix `<keyRoot>/<org>/` (no org id may
 * start with `.`, writer.ts assertTenantSegment), exactly as async-admission.ts INBOX_NAMESPACE. Not to be
 * confused with the unrelated `96bulls/upload-inbox/v1` candidate root (no dot) in stacks/aws.
 */
export const INBOX_UPLOAD_NAMESPACE = ".upload-inbox";
export const UPLOAD_LAYOUT_VERSION = "v1";
export const UPLOAD_OBJECT_SUFFIX = ".json.zst";
export const UPLOAD_WITNESS_PREFIX = "v1.";
/** The MAC input is this domain string followed by the payload's base64url text. */
export const UPLOAD_WITNESS_DOMAIN = "relay-upload-witness-v1\n";
/** The label of the `uh` hash; the same label async-admission.ts hashes for the privacy journal. */
export const UPLOAD_USER_LABEL = "admission-user-v1";
export const UPLOAD_WITNESS_MAX_BYTES = 1024;
export const UPLOAD_WITNESS_KEY_BYTES = 32;
/** Stored (zstd) bytes, before decoding; after decoding the usual delivery limits apply. */
export const UPLOAD_MAX_OBJECT_BYTES = MAX_DELIVERY_BYTES;
export const UPLOAD_MAX_PRESIGNED_URLS = 100;
export const UPLOAD_WITNESS_METADATA_HEADER = "x-amz-meta-relay-witness";
export const UPLOAD_CONTENT_TYPE = "application/json";
export const UPLOAD_CONTENT_ENCODING = "zstd";

/** The closed refusal-code set of an upload, in check order. A refused status carries exactly one of these. */
export const UPLOAD_REFUSAL_CODES = [
  "upload_key_invalid", //            the key does not parse under the upload-inbox grammar
  "upload_witness_invalid", //        witness missing, malformed, not exact-keys, or its MAC does not verify
  "upload_witness_expired", //        witness `exp` is at or before the consumer's clock
  "upload_tenant_mismatch", //        key (org, ws, uh) differs from the witness (org, ws, hash(user))
  "upload_witness_seq_mismatch", //   key `w<seq>` differs from the witness seq, or the seq is ahead of the journal
  "delivery_too_large", //            object over MAX_DELIVERY_BYTES, or decoded/prepared delivery over its limits
  "upload_digest_mismatch", //        `d=` is not the sha256 of the stored bytes
  "unsupported_encoding", //          the object is not one zstd frame
  "invalid_delivery", //              not UTF-8 JSON, or not a valid delivery request (parseDeliveryRequest)
  "unsupported_protocol", //          parseDeliveryRequest: protocolVersion is not 1
  "unsupported_schema", //            parseDeliveryRequest: batch or record schema_version unsupported
  "unsupported_mapping", //           parseDeliveryRequest: mapping_version unsupported
  "unsupported_evidence", //          parseDeliveryRequest: a record's evidence kind is unsupported
  "delivery_account_mismatch", //     batch.account_id is not deliveryAccount(org, ws)
  "upload_origin_mismatch", //        batch.origin_id differs from the key's `o=`
  "delivery_budget_exceeded", //      the batch exceeds the destination's per-request budget (deliver.ts)
  "delivery_privacy_superseded", //   a reset or re-enable completed after the witnessed journal seq
  "admission_invalid", //             the admission inbox object or receipt failed its own checks
  "admission_failed", //              admission failed for a reason with no narrower code
  "tenant_not_a_segment", //          the org is not a valid key segment for this key root (writer.ts)
] as const;
export type UploadRefusalCode = (typeof UPLOAD_REFUSAL_CODES)[number];
export interface UploadRefusal { code: UploadRefusalCode }

export const UPLOAD_ADMISSION_STATES = ["pending", "admitted", "refused"] as const;
export type UploadAdmissionState = (typeof UPLOAD_ADMISSION_STATES)[number];

/** GET /v1/delivery/admission?uploadKey=… error answers (body `{ error: <code> }`); Retry-After on 503. */
export const UPLOAD_STATUS_HTTP_ERRORS = { 400: "invalid_delivery", 403: "upload_tenant_mismatch", 404: "upload_unknown", 503: "delivery_unavailable" } as const;

/** A programming or configuration error: never written as an upload's refusal. */
export class UploadContractError extends Error {
  constructor(readonly code: "upload_key_input_invalid" | "upload_witness_input_invalid" | "upload_witness_key_invalid") {
    super(`relay-upload-inbox-v1: ${code}`);
    this.name = "UploadContractError";
  }
}

const refuse = (code: UploadRefusalCode): UploadRefusal => ({ code });
export const isUploadRefusal = (v: unknown): v is UploadRefusal =>
  v !== null && typeof v === "object" && Object.keys(v).length === 1 && UPLOAD_REFUSAL_CODES.includes((v as { code?: unknown }).code as never);

// ---- segments and hashes ----

const encoder = new TextEncoder();
const decoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: false });
const sha256Hex = (value: string | Uint8Array) => createHash("sha256").update(value).digest("hex");
/** writer.ts assertTenantSegment: `[A-Za-z0-9._-]{1,128}`, never starting with `.`. */
const SEGMENT_SRC = "(?!\\.)[A-Za-z0-9._-]{1,128}";
const SEGMENT = new RegExp(`^${SEGMENT_SRC}$`);
/** writer.ts assertTenantSegment's key-root rule (self-host INGEST_KEY_ROOT must satisfy it too). */
const KEY_ROOT_SRC = "[a-z0-9][a-z0-9-]{0,62}";
const KEY_ROOT = new RegExp(`^${KEY_ROOT_SRC}$`);
const HEX32 = /^[0-9a-f]{32}$/;
const HEX64 = /^[0-9a-f]{64}$/;
const SEQ_SRC = "0|[1-9][0-9]{0,15}";

export const isUploadSegment = (v: unknown): v is string => typeof v === "string" && SEGMENT.test(v);
/** A user id or batch id: non-empty, at most 512 UTF-8 bytes, no control characters (async-admission.ts `ident`). */
const ident = (v: unknown): v is string => typeof v === "string" && v.length > 0 && encoder.encode(v).length <= 512 && !/[\x00-\x1f\x7f]/.test(v);
const seqOk = (v: unknown): v is number => typeof v === "number" && Number.isSafeInteger(v) && v >= 0;

/** `uh`: sha256 hex of JSON ["admission-user-v1", ws, user], first 32 chars (async-admission.ts userDir, minus `srv_`). */
export function userSegment(workspaceId: string, userId: string): string {
  return sha256Hex(JSON.stringify([UPLOAD_USER_LABEL, workspaceId, userId])).slice(0, 32);
}

/** `b=`: sha256 hex of the batch id's UTF-8 bytes, first 32 chars. */
export const batchSegment = (batchId: string) => sha256Hex(batchId).slice(0, 32);
/** Lowercase hex sha256 of the exact stored bytes (the zstd frame); `d=` is its first 32 chars. */
export const uploadObjectSha256Hex = (objectBytes: Uint8Array) => sha256Hex(objectBytes);
/** The `x-amz-checksum-sha256` header value: base64 (standard, padded) of the raw sha256 of the stored bytes. */
export const uploadChecksumSha256 = (objectBytes: Uint8Array) => createHash("sha256").update(objectBytes).digest("base64");

// ---- keys ----

export interface UploadKeyPrefixInput { keyRoot: string; orgId: string; workspaceId: string; userId: string }

/** `<keyRoot>/.upload-inbox/<org>/v1/ws=<ws>/u=<uh>/`. Throws on an input that is not a valid segment. */
export function uploadKeyPrefix(i: UploadKeyPrefixInput): string {
  if (!KEY_ROOT.test(i.keyRoot) || !isUploadSegment(i.orgId) || !isUploadSegment(i.workspaceId) || !ident(i.userId)) throw new UploadContractError("upload_key_input_invalid");
  return `${i.keyRoot}/${INBOX_UPLOAD_NAMESPACE}/${i.orgId}/${UPLOAD_LAYOUT_VERSION}/ws=${i.workspaceId}/u=${userSegment(i.workspaceId, i.userId)}/`;
}

const PREFIX_SRC = `(${KEY_ROOT_SRC})/\\.upload-inbox/(${SEGMENT_SRC})/v1/ws=(${SEGMENT_SRC})/u=([0-9a-f]{32})/`;
const PREFIX_RE = new RegExp(`^${PREFIX_SRC}$`);
const KEY_RE = new RegExp(`^${PREFIX_SRC}o=(${SEGMENT_SRC})/b=([0-9a-f]{32})/d=([0-9a-f]{32})-w(${SEQ_SRC})\\.json\\.zst$`);

export interface UploadKeyPrefixParts { keyRoot: string; orgId: string; workspaceId: string; userSegment: string; prefix: string }
export interface UploadKeyParts extends UploadKeyPrefixParts { key: string; originId: string; batchSegment: string; digestSegment: string; witnessSeq: number }

/** A key prefix as the credentials response carries it, or null. */
export function parseUploadKeyPrefix(prefix: unknown): UploadKeyPrefixParts | null {
  const m = typeof prefix === "string" ? PREFIX_RE.exec(prefix) : null;
  return m ? { keyRoot: m[1]!, orgId: m[2]!, workspaceId: m[3]!, userSegment: m[4]!, prefix: prefix as string } : null;
}

export interface UploadObjectKeyInput { originId: string; batchId: string; objectSha256Hex: string; witnessSeq: number }

/** `<prefix>o=<originId>/b=<sha256(batchId)[:32]>/d=<sha256(objectBytes)[:32]>-w<witnessSeq>.json.zst`. */
export function uploadObjectKey(prefix: string, i: UploadObjectKeyInput): string {
  if (!parseUploadKeyPrefix(prefix) || !isUploadSegment(i.originId) || !ident(i.batchId) || typeof i.objectSha256Hex !== "string" || !HEX64.test(i.objectSha256Hex) || !seqOk(i.witnessSeq))
    throw new UploadContractError("upload_key_input_invalid");
  return `${prefix}o=${i.originId}/b=${batchSegment(i.batchId)}/d=${i.objectSha256Hex.slice(0, 32)}-w${i.witnessSeq}${UPLOAD_OBJECT_SUFFIX}`;
}

/** Parse an object key, or refuse with `upload_key_invalid`. The caller still compares `keyRoot` with its own. */
export function parseUploadKey(key: unknown): UploadKeyParts | UploadRefusal {
  const m = typeof key === "string" ? KEY_RE.exec(key) : null;
  const seq = m ? Number(m[8]) : NaN;
  if (!m || !Number.isSafeInteger(seq)) return refuse("upload_key_invalid");
  const k = key as string;
  return { key: k, keyRoot: m[1]!, orgId: m[2]!, workspaceId: m[3]!, userSegment: m[4]!, prefix: k.slice(0, k.indexOf("/o=") + 1),
    originId: m[5]!, batchSegment: m[6]!, digestSegment: m[7]!, witnessSeq: seq };
}

/** Checks 6 and 7 of the binding order: stored size <= MAX_DELIVERY_BYTES, then `d=` is the sha256 of the stored bytes. */
export function checkUploadObjectBytes(key: UploadKeyParts, objectBytes: Uint8Array): { ok: true } | UploadRefusal {
  if (objectBytes.byteLength > UPLOAD_MAX_OBJECT_BYTES) return refuse("delivery_too_large");
  if (!HEX32.test(key.digestSegment) || uploadObjectSha256Hex(objectBytes).slice(0, 32) !== key.digestSegment) return refuse("upload_digest_mismatch");
  return { ok: true };
}

// ---- witness ----

export interface UploadWitnessPayload { v: 1; org: string; ws: string; user: string; seq: number; exp: number }
const WITNESS_KEYS = ["v", "org", "ws", "user", "seq", "exp"] as const;
const B64URL = /^[A-Za-z0-9_-]+$/;

const b64url = (bytes: Uint8Array | Buffer) => Buffer.from(bytes).toString("base64url");
/** Canonical unpadded base64url only: a text that does not re-encode to itself (padding, stray bits) is null. */
function fromB64url(text: string): Buffer | null {
  if (!B64URL.test(text)) return null;
  const bytes = Buffer.from(text, "base64url");
  return bytes.toString("base64url") === text ? bytes : null;
}

/** UPLOAD_WITNESS_KEY: 32 random bytes as unpadded base64url. Throws (configuration error) on anything else. */
export function parseUploadWitnessKey(value: string): Uint8Array {
  const bytes = typeof value === "string" ? fromB64url(value.trim()) : null;
  if (!bytes || bytes.byteLength !== UPLOAD_WITNESS_KEY_BYTES) throw new UploadContractError("upload_witness_key_invalid");
  return new Uint8Array(bytes);
}

function assertKey(key: Uint8Array) {
  if (!(key instanceof Uint8Array) || key.byteLength !== UPLOAD_WITNESS_KEY_BYTES) throw new UploadContractError("upload_witness_key_invalid");
}

const witnessMac = (key: Uint8Array, payloadB64: string) => createHmac("sha256", key).update(UPLOAD_WITNESS_DOMAIN + payloadB64).digest();

function payloadOk(p: unknown): p is UploadWitnessPayload {
  if (p === null || typeof p !== "object" || Array.isArray(p)) return false;
  const keys = Object.keys(p);
  if (keys.length !== WITNESS_KEYS.length || keys.some((k, i) => k !== WITNESS_KEYS[i])) return false;
  const w = p as Record<string, unknown>;
  return w.v === 1 && isUploadSegment(w.org) && isUploadSegment(w.ws) && ident(w.user) && seqOk(w.seq) && typeof w.exp === "number" && Number.isSafeInteger(w.exp) && w.exp > 0;
}
/** The one serialization of a payload: keys in WITNESS_KEYS order, nothing else. */
const payloadJson = (p: UploadWitnessPayload) => JSON.stringify({ v: p.v, org: p.org, ws: p.ws, user: p.user, seq: p.seq, exp: p.exp });

/** Control's side: sign a payload. Throws on a bad key or payload, or a token over 1 KiB. */
export function encodeUploadWitness(key: Uint8Array, payload: UploadWitnessPayload): string {
  assertKey(key);
  if (!payloadOk(payload)) throw new UploadContractError("upload_witness_input_invalid");
  const body = b64url(encoder.encode(payloadJson(payload)));
  const token = `${UPLOAD_WITNESS_PREFIX}${body}.${b64url(witnessMac(key, body))}`;
  if (token.length > UPLOAD_WITNESS_MAX_BYTES) throw new UploadContractError("upload_witness_input_invalid");
  return token;
}

/**
 * The consumer's side: the signed payload, or `upload_witness_invalid` (absent, malformed, bad MAC, not the
 * exact payload form) / `upload_witness_expired` (exp <= nowSeconds). The MAC is compared in constant time
 * before the payload is parsed. A key of the wrong length throws: a misconfigured consumer must retry, not refuse.
 */
export function verifyUploadWitness(key: Uint8Array, token: unknown, nowSeconds: number): UploadWitnessPayload | UploadRefusal {
  assertKey(key);
  if (!Number.isSafeInteger(nowSeconds)) throw new UploadContractError("upload_witness_input_invalid");
  if (typeof token !== "string" || token.length > UPLOAD_WITNESS_MAX_BYTES || !token.startsWith(UPLOAD_WITNESS_PREFIX)) return refuse("upload_witness_invalid");
  const parts = token.slice(UPLOAD_WITNESS_PREFIX.length).split(".");
  if (parts.length !== 2) return refuse("upload_witness_invalid");
  const [body, macText] = parts as [string, string];
  const payloadBytes = fromB64url(body), mac = fromB64url(macText);
  if (!payloadBytes || !mac || mac.byteLength !== 32) return refuse("upload_witness_invalid");
  if (!timingSafeEqual(mac, witnessMac(key, body))) return refuse("upload_witness_invalid");
  let parsed: unknown, text: string;
  try { text = decoder.decode(payloadBytes); parsed = JSON.parse(text); } catch { return refuse("upload_witness_invalid"); }
  if (!payloadOk(parsed) || payloadJson(parsed) !== text) return refuse("upload_witness_invalid");
  if (parsed.exp <= nowSeconds) return refuse("upload_witness_expired");
  return { v: 1, org: parsed.org, ws: parsed.ws, user: parsed.user, seq: parsed.seq, exp: parsed.exp };
}

/**
 * The payload WITHOUT verifying the MAC, for the desktop's self-check of a credentials response (it holds no
 * key). Never a basis for admission. Null when the token is not in the exact form.
 */
export function readUploadWitnessUnverified(token: unknown): UploadWitnessPayload | null {
  if (typeof token !== "string" || token.length > UPLOAD_WITNESS_MAX_BYTES || !token.startsWith(UPLOAD_WITNESS_PREFIX)) return null;
  const parts = token.slice(UPLOAD_WITNESS_PREFIX.length).split(".");
  if (parts.length !== 2) return null;
  const payloadBytes = fromB64url(parts[0]!), mac = fromB64url(parts[1]!);
  if (!payloadBytes || !mac || mac.byteLength !== 32) return null;
  try {
    const text = decoder.decode(payloadBytes), parsed: unknown = JSON.parse(text);
    return payloadOk(parsed) && payloadJson(parsed) === text ? parsed : null;
  } catch { return null; }
}

/** Key tenant (org, ws, uh) must equal the witness (org, ws, hash(user)); then the key's `w<seq>` must equal its seq. */
export function bindUploadToWitness(key: UploadKeyParts, witness: UploadWitnessPayload): { ok: true } | UploadRefusal {
  if (key.orgId !== witness.org || key.workspaceId !== witness.ws || key.userSegment !== userSegment(witness.ws, witness.user)) return refuse("upload_tenant_mismatch");
  if (key.witnessSeq !== witness.seq) return refuse("upload_witness_seq_mismatch");
  return { ok: true };
}

// ---- credentials response (POST /api/v1/workspaces/{ws}/relayhistory/upload-credentials, relay-control) ----

interface UploadCredentialsCommon {
  contract: typeof UPLOAD_CONTRACT;
  bucket: string;
  region: string;
  /** null: the provider's default endpoint for `region` (hosted AWS). Self-host: its S3-compatible endpoint URL. */
  endpoint: string | null;
  keyPrefix: string;
  kmsKeyId: string;
  witness: string;
  witnessSeq: number;
  maxObjectBytes: typeof MAX_DELIVERY_BYTES;
  refreshAfter: string;
}
export interface UploadStsCredentials extends UploadCredentialsCommon {
  kind: "sts";
  credentials: { accessKeyId: string; secretAccessKey: string; sessionToken: string; expiresAt: string };
}
export interface UploadPresignedCredentials extends UploadCredentialsCommon {
  kind: "presigned";
  /** `key` is relative to keyPrefix; keyPrefix + key is a full object key. */
  urls: { key: string; url: string; expiresAt: string }[];
}
export type UploadCredentialsResponse = UploadStsCredentials | UploadPresignedCredentials;

const COMMON_KEYS = ["contract", "kind", "bucket", "region", "endpoint", "keyPrefix", "kmsKeyId", "witness", "witnessSeq", "maxObjectBytes", "refreshAfter"];
const STS_KEYS = [...COMMON_KEYS, "credentials"];
const PRESIGNED_KEYS = [...COMMON_KEYS, "urls"];

const isObj = (v: unknown): v is Record<string, unknown> => v !== null && typeof v === "object" && !Array.isArray(v);
const exactKeys = (v: Record<string, unknown>, keys: readonly string[]) => { const k = Object.keys(v); return k.length === keys.length && keys.every((x) => Object.hasOwn(v, x)); };
/** Printable ASCII, no spaces. */
const printable = (v: unknown, max: number): v is string => typeof v === "string" && v.length > 0 && v.length <= max && /^[\x21-\x7e]+$/.test(v);
const RFC3339_UTC = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(\.\d{1,9})?Z$/;
/** An RFC 3339 UTC timestamp (`Z` only) that names a real instant. Returns epoch ms or null. */
export function rfc3339UtcMs(v: unknown): number | null {
  const m = typeof v === "string" ? RFC3339_UTC.exec(v) : null;
  if (!m) return null;
  const ms = Date.UTC(+m[1]!, +m[2]! - 1, +m[3]!, +m[4]!, +m[5]!, +m[6]!);
  const d = new Date(ms);
  return d.getUTCFullYear() === +m[1]! && d.getUTCMonth() === +m[2]! - 1 && d.getUTCDate() === +m[3]! && d.getUTCHours() === +m[4]! && d.getUTCMinutes() === +m[5]! && d.getUTCSeconds() === +m[6]! ? ms + (m[7] ? Math.floor(Number(`0${m[7]}`) * 1000) : 0) : null;
}
const BUCKET = /^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/;
const REGION = /^[a-z0-9-]{1,64}$/;
function urlOk(v: unknown, max: number): v is string {
  if (typeof v !== "string" || v.length > max || !/^[\x21-\x7e]+$/.test(v)) return false;
  try {
    const u = new URL(v);
    return (u.protocol === "https:" || u.protocol === "http:") && u.username === "" && u.password === "" && u.hash === "";
  } catch { return false; }
}
const endpointOk = (v: unknown) => v === null || (urlOk(v, 2048) && new URL(v).search === "" && new URL(v).pathname === "/");

/**
 * Exact-keys validator for a credentials response, or null. Beyond shapes it checks what the desktop can check
 * without the witness key: the witness payload's (org, ws, user) is the keyPrefix tenant and its seq is
 * `witnessSeq`, every presigned key lands under keyPrefix with `w<witnessSeq>`, and `refreshAfter` is not later
 * than the earliest expiry.
 */
export function parseUploadCredentialsResponse(v: unknown): UploadCredentialsResponse | null {
  if (!isObj(v) || v.contract !== UPLOAD_CONTRACT || (v.kind !== "sts" && v.kind !== "presigned")) return null;
  if (!exactKeys(v, v.kind === "sts" ? STS_KEYS : PRESIGNED_KEYS)) return null;
  const prefix = parseUploadKeyPrefix(v.keyPrefix);
  const witness = readUploadWitnessUnverified(v.witness);
  const refreshAfter = rfc3339UtcMs(v.refreshAfter);
  if (typeof v.bucket !== "string" || !BUCKET.test(v.bucket) || typeof v.region !== "string" || !REGION.test(v.region) || !endpointOk(v.endpoint) ||
      !prefix || !printable(v.kmsKeyId, 2048) || !witness || !seqOk(v.witnessSeq) || v.maxObjectBytes !== MAX_DELIVERY_BYTES || refreshAfter === null) return null;
  if (witness.seq !== v.witnessSeq || witness.org !== prefix.orgId || witness.ws !== prefix.workspaceId || userSegment(witness.ws, witness.user) !== prefix.userSegment) return null;
  if (v.kind === "sts") {
    const c = v.credentials;
    if (!isObj(c) || !exactKeys(c, ["accessKeyId", "secretAccessKey", "sessionToken", "expiresAt"]) || !printable(c.accessKeyId, 128) || !printable(c.secretAccessKey, 256) || !printable(c.sessionToken, 8192)) return null;
    const expires = rfc3339UtcMs(c.expiresAt);
    if (expires === null || refreshAfter > expires) return null;
  } else {
    const urls = v.urls;
    if (!Array.isArray(urls) || urls.length < 1 || urls.length > UPLOAD_MAX_PRESIGNED_URLS) return null;
    const seen = new Set<string>();
    for (const u of urls) {
      if (!isObj(u) || !exactKeys(u, ["key", "url", "expiresAt"]) || typeof u.key !== "string" || seen.has(u.key) || !urlOk(u.url, 8192)) return null;
      seen.add(u.key);
      const parsed = parseUploadKey(prefix.prefix + u.key);
      const expires = rfc3339UtcMs(u.expiresAt);
      if (isUploadRefusal(parsed) || parsed.witnessSeq !== v.witnessSeq || expires === null || refreshAfter > expires) return null;
    }
  }
  return v as unknown as UploadCredentialsResponse;
}

// ---- admission status (GET /v1/delivery/admission?uploadKey=…) ----

export interface UploadAdmissionStatus {
  contract: typeof UPLOAD_CONTRACT;
  uploadKey: string;
  state: UploadAdmissionState;
  /** refused only, and always present when refused. */
  code?: UploadRefusalCode;
  /** admitted or refused, when known; never on pending. */
  receiptId?: string;
  receivedAtMs?: number;
  /** terminal only. */
  decidedAtMs?: number;
  /** pending only; the server also sends Retry-After. */
  retryAfterSeconds?: number;
}
const STATUS_KEYS = new Set(["contract", "uploadKey", "state", "code", "receiptId", "receivedAtMs", "decidedAtMs", "retryAfterSeconds"]);
const msOk = (v: unknown) => typeof v === "number" && Number.isSafeInteger(v) && v >= 0;

/** Exact-keys validator for a 200 admission-status body, or null. */
export function parseUploadAdmissionStatus(v: unknown): UploadAdmissionStatus | null {
  if (!isObj(v) || Object.keys(v).some((k) => !STATUS_KEYS.has(k)) || v.contract !== UPLOAD_CONTRACT || isUploadRefusal(parseUploadKey(v.uploadKey))) return null;
  const state = v.state;
  if (!UPLOAD_ADMISSION_STATES.includes(state as never)) return null;
  const terminal = state !== "pending";
  if (state === "refused" ? !UPLOAD_REFUSAL_CODES.includes(v.code as never) : Object.hasOwn(v, "code")) return null;
  if (Object.hasOwn(v, "receiptId") && (!terminal || typeof v.receiptId !== "string" || !/^rhr_[0-9a-f]{64}$/.test(v.receiptId))) return null;
  if (Object.hasOwn(v, "receivedAtMs") && !msOk(v.receivedAtMs)) return null;
  if (Object.hasOwn(v, "decidedAtMs") && (!terminal || !msOk(v.decidedAtMs))) return null;
  if (Object.hasOwn(v, "retryAfterSeconds") && (terminal || typeof v.retryAfterSeconds !== "number" || !Number.isSafeInteger(v.retryAfterSeconds) || v.retryAfterSeconds < 1 || v.retryAfterSeconds > 3600)) return null;
  return v as unknown as UploadAdmissionStatus;
}
