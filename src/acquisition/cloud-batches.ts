/**
 * Q3 private Cloud publication receiver (design §9, PR #314):
 * POST /v1/acquisition/cloud/batches on the acquisition service's PRIVATE (ops)
 * listener. HELD and default off (ACQUISITION_CLOUD_PUBLICATION_ENABLED).
 *
 * app.ts owns the flag/config gate, Content-Type, the declared and streamed
 * outer-body bound (2,100,000 bytes) and operator authentication (the
 * acquisition verifier: sh_wk_, principal_type service, scope
 * acquisition:operate, aud = ACQUISITION_ORIGIN). This module then, in order:
 *
 *   1. parses the exact three-key envelope {v:1, install:{payload,signature}, batchBodyBase64url};
 *   2. requires the install assertion's org/workspace to equal the introspected scope (403
 *      target_mismatch) and verifies the install against the server-pinned CLOUD issuer
 *      registry only (#322 verifyAcquisitionAssertion: rebuilds A, verifies S and both digests);
 *      nothing in the body names a key, class, identity or header that is trusted;
 *   3. only then decodes batchBodyBase64url (canonical unpadded base64url, <= 1,500,000 bytes,
 *      strict UTF-8, JSON) and checks the §9.2 shape and binding: exactly protocolVersion:1,
 *      batch, and one acquisition entry bound to the signed A (source droid, SID, grantId =
 *      operationUuid, ownerEpoch = expectedOwnerEpoch + 1 as BigInt, §3 grant hash of G =
 *      A.grantSha256), records <= 500 all droid/SID/origin, batch origin/instance = A.ownerOriginId,
 *      account_id = the ordinary deliveryAccount derivation, unique revision ids. Everything else
 *      about the records is the ordinary delivery validator's job upstream (no second parser);
 *   4. signs the internal hop with the existing session-ingest edge signer (encodeIdentity +
 *      signRequest) from the verified A and the operator scope only, and forwards the EXACT decoded
 *      bytes to <ACQUISITION_CLOUD_INGEST_ORIGIN>/v1/delivery/batches (no redirects, bounded);
 *   5. passes through a valid 200 DeliveryReceipt byte for byte, forwards ordinary ingest errors
 *      (including 409 acquisition_refused and Retry-After), and answers 503 for a transport failure,
 *      a redirect, an internal-hop auth failure or an invalid receipt (unknown outcome).
 *
 * Errors are static codes. No error, return value or log field carries G, the signed envelope,
 * the bearer or any body byte.
 */
import { timingSafeEqual } from "node:crypto";
import { acquisitionGrantDigest, verifyAcquisitionAssertion } from "../session-ingest/acquisition-assertion.ts";
import { encodeIdentity, IDENTITY_HEADER, SIGNATURE_HEADER, signRequest, SYNC_SCOPE } from "../session-ingest/app.ts";
import { deliveryAccount } from "../session-ingest/vendor/delivery/prepare.ts";
import type { IngestSigningKey } from "./config.ts";
import { isUuid, type IssuerRegistry } from "./validation.ts";

export const CLOUD_BATCHES_PATH = "/v1/acquisition/cloud/batches";
export const INGEST_DELIVERY_PATH = "/v1/delivery/batches";
/** §9.1 outer uncompressed body bound (declared and streamed). */
export const MAX_CLOUD_ENVELOPE_BYTES = 2_100_000;
/** §9.1 decoded delivery-request bound. */
export const MAX_CLOUD_BATCH_BYTES = 1_500_000;
/** Longest canonical unpadded base64url of MAX_CLOUD_BATCH_BYTES. */
export const MAX_CLOUD_BATCH_BASE64URL = Math.ceil((MAX_CLOUD_BATCH_BYTES * 4) / 3);
export const MAX_CLOUD_RECORDS = 500;
/** The v1 route's only source (§9.2). */
export const CLOUD_SOURCE = "droid";
/** Upstream answers larger than this are not a receipt or an ordinary error: 503. */
export const MAX_UPSTREAM_RESPONSE_BYTES = 1024 * 1024;
export const DEFAULT_UPSTREAM_TIMEOUT_MS = 30_000;

const MAX_BIGINT = 9223372036854775807n;
const POSITIVE_DECIMAL = /^[1-9][0-9]*$/;
const GRANT = /^rhg_[0-9a-f]{64}$/;
const BASE64URL = /^[A-Za-z0-9_-]*$/;
const RETRY_AFTER = /^[0-9]{1,10}$/;

/** A static refusal: the code is the whole public answer. */
export class CloudBatchRefusal extends Error {
  constructor(readonly status: 400 | 403 | 413 | 503, readonly code: string) {
    super(code);
    this.name = "CloudBatchRefusal";
  }
}

const invalid = (): never => { throw new CloudBatchRefusal(400, "invalid_request"); };
const tooLarge = (): never => { throw new CloudBatchRefusal(413, "payload_too_large"); };
const mismatch = (): never => { throw new CloudBatchRefusal(403, "target_mismatch"); };
const unauthorized = (): never => { throw new CloudBatchRefusal(403, "not_authorized"); };

/** The operator scope, from introspection only. */
export interface CloudOperator {
  readonly orgId: string;
  readonly workspaceId: string;
}

/** What the receiver hands back to app.ts: the bytes to answer with, never re-serialized. */
export interface CloudAnswer {
  readonly status: number;
  readonly body: Uint8Array;
  readonly retryAfter?: string;
  /** Allowlisted log fields (no body bytes). */
  readonly upstreamStatus?: number;
  readonly recordCount?: number;
}

export interface CloudBatchReceiverOptions {
  /** The cloud-class subset of ACQUISITION_PINNED_KEYS (registryOfClass). */
  readonly registry: IssuerRegistry;
  /** ACQUISITION_CLOUD_INGEST_ORIGIN: bare origin, already destination-checked. */
  readonly ingestOrigin: string;
  readonly signingKey: IngestSigningKey;
  readonly fetch?: typeof fetch;
  /** Epoch seconds for the ingest signature. */
  readonly nowSeconds?: () => number;
  readonly timeoutMs?: number;
}

export interface CloudBatchReceiver {
  publish(envelope: Uint8Array, operator: CloudOperator, correlationId: string): Promise<CloudAnswer>;
}

function plainObject(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function exactKeys(value: Record<string, unknown>, keys: readonly string[]): boolean {
  const own = Object.keys(value).sort();
  return own.length === keys.length && own.join("\u0000") === [...keys].sort().join("\u0000");
}

/** Strict UTF-8 (no replacement, BOM kept so JSON.parse refuses it) then JSON. */
function strictJson(bytes: Uint8Array): unknown {
  let text: string;
  try {
    text = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(bytes);
  } catch {
    return invalid();
  }
  try {
    return JSON.parse(text);
  } catch {
    return invalid();
  }
}

/** Canonical unpadded base64url, bounded before decoding. */
export function decodeCanonicalBase64url(value: string): Buffer {
  if (value.length > MAX_CLOUD_BATCH_BASE64URL) tooLarge();
  if (!BASE64URL.test(value) || value.length % 4 === 1) invalid();
  const bytes = Buffer.from(value, "base64url");
  // Rejects non-zero trailing bits (two spellings of one byte string) and anything Node tolerated.
  if (bytes.toString("base64url") !== value) invalid();
  return bytes;
}

function hexEqual(a: string, b: string): boolean {
  const x = Buffer.from(a, "hex");
  const y = Buffer.from(b, "hex");
  return x.byteLength === 32 && y.byteLength === 32 && timingSafeEqual(x, y);
}

interface Bound {
  readonly bytes: Buffer;
  readonly ownerUserId: string;
  readonly batchId: string;
  readonly revisionIds: ReadonlySet<string>;
  readonly recordCount: number;
}

/**
 * Steps 1-3: everything before the upstream call. Throws CloudBatchRefusal; returns the exact
 * decoded bytes plus the values the receipt check needs.
 */
export async function bindCloudBatch(raw: Uint8Array, operator: CloudOperator, registry: IssuerRegistry): Promise<Bound> {
  // 1. The outer envelope: exactly {v, install, batchBodyBase64url}.
  const outer = strictJson(raw);
  if (!plainObject(outer) || !exactKeys(outer, ["v", "install", "batchBodyBase64url"])) return invalid();
  if (outer.v !== 1 || typeof outer.batchBodyBase64url !== "string" || !plainObject(outer.install)) return invalid();
  const install = outer.install;

  // 2. Authenticate the install against the server-pinned Cloud registry and the operator scope.
  const claimed = install.payload;
  if (plainObject(claimed) && (claimed.orgId !== operator.orgId || claimed.workspaceId !== operator.workspaceId)) mismatch();
  let A: Readonly<Record<string, string>>;
  try {
    A = verifyAcquisitionAssertion("install", install, registry,
      (scope) => scope.orgId === operator.orgId && scope.workspaceId === operator.workspaceId).payload;
  } catch (error) {
    // AcquisitionAssertionError: invalid_request (shape) or not_authorized (key/class/signature/digest/scope).
    return (error as { code?: unknown }).code === "invalid_request" ? invalid() : unauthorized();
  }
  if (A.ownerKind !== "cloud") unauthorized();

  // 3. Only now decode the batch content.
  const bytes = decodeCanonicalBase64url(outer.batchBodyBase64url);
  if (bytes.byteLength === 0) invalid();
  if (bytes.byteLength > MAX_CLOUD_BATCH_BYTES) tooLarge();
  const request = strictJson(bytes);
  if (!plainObject(request) || !exactKeys(request, ["protocolVersion", "batch", "acquisition"]) || request.protocolVersion !== 1) return invalid();
  const { batch, acquisition } = request;
  if (!plainObject(batch) || !Array.isArray(batch.records) || typeof batch.batch_id !== "string" || batch.batch_id === "") return invalid();
  const records: unknown[] = batch.records;
  if (records.length === 0) invalid();
  if (records.length > MAX_CLOUD_RECORDS) tooLarge();
  if (!Array.isArray(acquisition) || acquisition.length !== 1) return invalid();
  const entry: unknown = acquisition[0];
  if (!plainObject(entry) || !exactKeys(entry, ["source", "sessionId", "grantId", "ownerEpoch", "grant"])) return invalid();
  const { source, sessionId, grantId, ownerEpoch, grant } = entry;
  if (typeof source !== "string" || typeof sessionId !== "string" || typeof grantId !== "string" || !isUuid(grantId) ||
      typeof ownerEpoch !== "string" || !POSITIVE_DECIMAL.test(ownerEpoch) || BigInt(ownerEpoch) > MAX_BIGINT ||
      typeof grant !== "string" || !GRANT.test(grant)) return invalid();

  // Binding to the signed A (403, no upstream call).
  if (A.source !== CLOUD_SOURCE || source !== A.source || sessionId !== A.sessionId || grantId !== A.operationUuid) mismatch();
  // Exact: BigInt arithmetic, compared as canonical decimal strings; never Number.
  if (ownerEpoch !== (BigInt(A.expectedOwnerEpoch!) + 1n).toString()) mismatch();
  let grantSha256: string;
  try {
    grantSha256 = acquisitionGrantDigest({ orgId: A.orgId!, workspaceId: A.workspaceId!, source: A.source!, sessionId: A.sessionId! }, A.operationUuid!, grant);
  } catch {
    return invalid();
  }
  if (!hexEqual(grantSha256, A.grantSha256!)) unauthorized();
  if (batch.origin_id !== A.ownerOriginId || batch.instance_id !== A.ownerOriginId) mismatch();
  if (batch.account_id !== (await deliveryAccount({ orgId: operator.orgId, workspaceId: operator.workspaceId }))) mismatch();
  const revisionIds = new Set<string>();
  for (const record of records) {
    if (!plainObject(record) || typeof record.revision_id !== "string") return invalid();
    if (record.source !== CLOUD_SOURCE || record.session_id !== A.sessionId || record.origin_id !== A.ownerOriginId) mismatch();
    if (revisionIds.has(record.revision_id)) invalid();
    revisionIds.add(record.revision_id);
  }
  return { bytes, ownerUserId: A.ownerUserId!, batchId: batch.batch_id, revisionIds, recordCount: records.length };
}

/** At most `limit` bytes, or null when larger (the rest is cancelled, never buffered). */
async function readUpstream(response: Response, limit: number): Promise<Uint8Array | null> {
  const declared = response.headers.get("content-length");
  if (declared !== null && (!/^[0-9]+$/.test(declared) || Number(declared) > limit)) {
    await response.body?.cancel().catch(() => {});
    return null;
  }
  if (!response.body) return new Uint8Array();
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    total += value.byteLength;
    if (total > limit) {
      await reader.cancel().catch(() => {});
      return null;
    }
    chunks.push(value);
  }
  return Buffer.concat(chunks);
}

/** The ordinary DeliveryReceipt for exactly this batch: batchId and a unique subset of its revision ids. */
function validReceipt(body: Uint8Array, bound: Bound): boolean {
  let value: unknown;
  try {
    value = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(body));
  } catch {
    return false;
  }
  if (!plainObject(value) || value.batchId !== bound.batchId || !Array.isArray(value.acceptedRevisionIds)) return false;
  const seen = new Set<string>();
  for (const id of value.acceptedRevisionIds) {
    if (typeof id !== "string" || !bound.revisionIds.has(id) || seen.has(id)) return false;
    seen.add(id);
  }
  return true;
}

const staticBody = (code: string) => new TextEncoder().encode(JSON.stringify({ error: { code } }));
const unavailable = (code: string, upstreamStatus?: number, recordCount?: number): CloudAnswer =>
  ({ status: 503, body: staticBody(code), ...(upstreamStatus === undefined ? {} : { upstreamStatus }), recordCount });

export function createCloudBatchReceiver(opts: CloudBatchReceiverOptions): CloudBatchReceiver {
  const endpoint = new URL(INGEST_DELIVERY_PATH, opts.ingestOrigin);
  if (endpoint.origin !== opts.ingestOrigin) throw new Error("96bulls: ACQUISITION_CLOUD_INGEST_ORIGIN must be a bare origin");
  const doFetch = opts.fetch ?? ((...args: Parameters<typeof fetch>) => fetch(...args));
  const nowSeconds = opts.nowSeconds ?? (() => Math.floor(Date.now() / 1000));
  const timeoutMs = opts.timeoutMs ?? DEFAULT_UPSTREAM_TIMEOUT_MS;

  return {
    async publish(envelope, operator, correlationId) {
      const bound = await bindCloudBatch(envelope, operator, opts.registry);
      // 4. Internal hop: identity only from the verified A + operator scope, signed by the existing edge signer.
      const identity = encodeIdentity({ userId: bound.ownerUserId, orgId: operator.orgId, workspaceId: operator.workspaceId, scopes: [SYNC_SCOPE] });
      const signature = signRequest(opts.signingKey.reveal(), nowSeconds(), "POST", endpoint.pathname, identity, bound.bytes);
      let response: Response;
      let body: Uint8Array | null;
      try {
        response = await doFetch(endpoint.href, {
          method: "POST",
          headers: {
            "content-type": "application/json",
            accept: "application/json",
            [IDENTITY_HEADER]: identity,
            [SIGNATURE_HEADER]: signature,
            "x-correlation-id": correlationId,
          },
          body: bound.bytes,
          redirect: "manual",
          signal: AbortSignal.timeout(timeoutMs),
        });
        body = await readUpstream(response, MAX_UPSTREAM_RESPONSE_BYTES);
      } catch {
        // Unknown outcome: Cloud retries the exact saved envelope.
        return unavailable("ingest_unavailable", undefined, bound.recordCount);
      }
      const status = response.status;
      if (body === null) return unavailable("ingest_unavailable", status, bound.recordCount);
      if (status === 200)
        return validReceipt(body, bound)
          ? { status: 200, body, upstreamStatus: status, recordCount: bound.recordCount }
          : unavailable("invalid_upstream_receipt", status, bound.recordCount);
      // Any other 1xx/2xx/3xx is not a receipt (redirects are never followed): unknown outcome.
      if (status < 400) return unavailable("invalid_upstream_receipt", status, bound.recordCount);
      // The internal hop's own HMAC was refused: a receiver configuration fault, never the caller's 401.
      if (status === 401) return unavailable("ingest_unavailable", status, bound.recordCount);
      // 5. An ordinary ingest error (400/403/409 acquisition_refused/413/422/429/5xx): forwarded verbatim.
      const retryAfter = response.headers.get("retry-after")?.trim();
      return {
        status,
        body,
        ...(retryAfter && RETRY_AFTER.test(retryAfter) ? { retryAfter } : {}),
        upstreamStatus: status,
        recordCount: bound.recordCount,
      };
    },
  };
}
