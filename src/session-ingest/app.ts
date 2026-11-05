import {decodeCustodyEnvelope} from "./custody-wire.ts";
import {validatedCustodyAck} from "./custody-ack.ts";
import {CURRENT_CUSTODY_CONTENT_TYPE,CURRENT_CUSTODY_ACK_HEADER,PRIVATE_CUSTODY_BYTES,type CurrentCustodyPacket} from "./custody-protocol.ts";
import {orderedControl,orderedControlResult,type OrderedControl} from "./control-protocol.ts";
/**
 * The 96Bulls session ingest: relayhistory's delivery write contract
 * (relayhistory-cloud packages/relayhistory/src/routes/delivery.ts), served
 * from AWS with S3 as the record and Supabase as the ledger. No Neon.
 *
 *   POST /v1/delivery/batches   rth:sync   one batch -> receipt (same shapes, same codes)
 *   GET  /v1/delivery/limits    rth:sync   DELIVERY_LIMITS + contentEncodings
 *   GET  /healthz                          liveness only
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
import { AuthError, type VerifiedIdentity } from "../session-read-api/app.ts";
import { deliverBatch, deliverPrepared, IngestUnavailable, type DeliverDeps } from "./deliver.ts";
import { DELIVERY_LIMITS, DeliveryBusyError, DeliveryError, MAX_DELIVERY_BYTES } from "./vendor/delivery/delivery-contracts.ts";
import { parseDeliveryRequest, requireDeliveryAccount } from "./vendor/delivery/prepare.ts";
import { decodeRecoveryEnvelope, RECOVERY_CONTENT_TYPE } from "./recovery-protocol.ts";

export type IngestIdentity=VerifiedIdentity & {control?:OrderedControl};

export const SYNC_SCOPE = "rth:sync";
export type ContentEncoding = "zstd" | "gzip";

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
  return { userId: v.userId!, orgId: v.orgId!, workspaceId: v.workspaceId, scopes: v.scopes,...(control?{control}:{}) };
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
  log?: (fields: Record<string, unknown>) => void;
}

type Env = { Variables: { correlationId: string; identity: IngestIdentity; body: Uint8Array; custody:CurrentCustodyPacket|undefined } };

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

function failure(c: Context<Env>, error: unknown) {
  const safe = error instanceof DeliveryError ? error : new DeliveryError("delivery_unavailable", 503, "Durable delivery is temporarily unavailable");
  // A busy refusal is retryable and wrote nothing durable; tell the client when (relayhistory's header).
  if (safe instanceof DeliveryBusyError) c.header("Retry-After", String(safe.retryAfterSeconds));
  return c.json(
    errorBody(c, safe.code, safe.message, {
      ...(safe.conflict ? { conflict: safe.conflict } : {}),
      ...(safe.conflicts ? { conflicts: safe.conflicts } : {}),
      ...(safe.conflictCount ? { conflictCount: safe.conflictCount } : {}),
    }),
    safe.status,
  );
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

export function parseSelection(body: unknown): { account_id: string; origin_id: string; sessions: { source: string; session_id: string }[]; enabled: boolean } {
  if (!isObject(body)) invalid("Expected upload selection");
  const b = body as Record<string, unknown>;
  exactKeys(b, ["account_id", "origin_id", "sessions", "enabled"]);
  if (!identifier(b.origin_id) || typeof b.enabled !== "boolean" || !Array.isArray(b.sessions) || b.sessions.length < 1 || b.sessions.length > 2048)
    invalid("Invalid upload selection");
  for (const x of b.sessions as unknown[]) {
    if (!isObject(x)) invalid("Invalid session identity");
    exactKeys(x as Record<string, unknown>, ["source", "session_id"]);
    if (!identifier((x as Record<string, unknown>).source) || !identifier((x as Record<string, unknown>).session_id)) invalid("Invalid session identity");
  }
  return b as never;
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

  app.use("*", async (c, next) => {
    const requestId = crypto.randomUUID();
    c.set("correlationId", c.req.header("X-Correlation-Id")?.trim().slice(0, 128) || requestId);
    await next();
    c.header("X-Request-Id", requestId);
    c.header("X-Correlation-Id", c.get("correlationId"));
  });

  app.get("/healthz", (c) => c.json({ ok: true }));

  // Identity for /v1, relayhistory's codes: 401 missing_authorization / invalid_token, 403 forbidden.
  const identify = deps.identify ?? signedBindingIdentity(deps.signingKey);
  app.use("/v1/*", async (c, next) => {
    // The body is read once, bounded by the wire cap, so the signature covers exactly what is parsed.
    let body: Uint8Array;
    try {
      const contentType=c.req.header("content-type")?.split(";")[0]?.trim();
      const privateWire=contentType===CURRENT_CUSTODY_CONTENT_TYPE||contentType===RECOVERY_CONTENT_TYPE;
      if(privateWire&&(deps.deliver.acceptanceMode!=="buffered"||!["","identity"].includes(c.req.header("content-encoding")?.trim().toLowerCase()??"")))throw new IngestUnavailable("fence","custody_wire_unavailable");
      body = c.req.raw.body ? await readBounded(c.req.raw.body, privateWire?PRIVATE_CUSTODY_BYTES:MAX_DELIVERY_BYTES) : new Uint8Array();
    } catch (error) {
      return failure(c, error);
    }
    c.set("body", body);
    try {
      c.set("identity", await identify(c.req.raw, body));
    } catch (error) {
      if (error instanceof AuthError) return c.json(errorBody(c, error.code, error.message), error.status);
      emit({ route: "auth", outcome: "error", error_code: "identity_failed" });
      return c.json(errorBody(c, "invalid_token", "invalid or expired token"), 401);
    }
    if (!c.get("identity").scopes.includes(SYNC_SCOPE)) return c.json(errorBody(c, "forbidden", `missing required scope: ${SYNC_SCOPE}`), 403);
    await next();
  });

  app.get("/v1/delivery/limits", (c) => c.json({ ...DELIVERY_LIMITS, contentEncodings: contentEncodings() }));

  app.post("/v1/delivery/batches", async (c) => {
    const started = Date.now();
    const identity = c.get("identity");
    const read: BodyRead = { bytes: 0, wireBytes: 0, encoding: "identity" };
    const fields: Record<string, unknown> = { route: "delivery.batches", org_id: identity.orgId, workspace_id: identity.workspaceId, correlation_id: c.get("correlationId") };
    try {
      const batch = parseDeliveryRequest(await jsonOf(c, read));
      const custody=c.get("custody");
      if(custody&&custody.witness.originalRecordCount!==batch.records.length)throw new IngestUnavailable("fence","custody_record_count");
      const result = await deliverBatch({...deps.deliver,...(custody?{custody}:{})}, { orgId: identity.orgId, workspaceId: identity.workspaceId, userId: identity.userId }, batch);
      emit({ ...fields, outcome: result.outcome, count: result.stats.records, manifests: result.stats.manifests_written, deleted: result.stats.deleted_sessions, state: result.stats.replica, duration_ms: Date.now() - started, bytes_raw: read.bytes });
      custodyHeader(c,result.custodyAck);
      return c.json({ ...result.receipt, limits: { ...result.receipt.limits, contentEncodings: contentEncodings() } });
    } catch (error) {
      const code = error instanceof DeliveryError ? error.code : "delivery_unavailable";
      emit({
        ...fields,
        outcome: code,
        ...(error instanceof IngestUnavailable ? { stage: error.stage, error_code: error.cause_code } : {}),
        ...(error instanceof DeliveryError && error.conflictCount ? { count: error.conflictCount } : {}),
        duration_ms: Date.now() - started,
        bytes_raw: read.bytes,
      });
      return failure(c, error);
    }
  });

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
      const result = await deliverPrepared({ ...deps.deliver, custody }, tenant, prepared);
      emit({ ...fields, outcome: result.outcome, count: result.stats.records, manifests: result.stats.manifests_written, deleted: result.stats.deleted_sessions, state: result.stats.replica, duration_ms: Date.now() - started, bytes_raw: c.get("body").byteLength });
      custodyHeader(c, result.custodyAck);
      return c.json({ ...result.receipt, limits: { ...result.receipt.limits, contentEncodings: contentEncodings() } });
    } catch (error) {
      const code = error instanceof DeliveryError ? error.code : "delivery_unavailable";
      emit({ ...fields, outcome: code, ...(error instanceof IngestUnavailable ? { stage: error.stage, error_code: error.cause_code } : {}), duration_ms: Date.now() - started, bytes_raw: c.get("body").byteLength });
      return failure(c, error);
    }
  });

  // Deletion fences (relayhistory's selection and reset, written here right after Neon's own, by
  // relayhistory-cloud #265). Same bodies and answers as relayhistory (lib/delivery.ts
  // setSessionUploads / resetUploadedSessions); tenancy only from the signed identity.
  app.post("/v1/delivery/sessions/selection", async (c) => {
    const identity = c.get("identity");
    try {
      const input = parseSelection(await jsonOf(c));
      await requireDeliveryAccount(identity, input.account_id);
      const selection={
        tenant: { orgId: identity.orgId, workspaceId: identity.workspaceId, userId: identity.userId },
        originId: input.origin_id,
        sessions: input.sessions.map((x):[string,string] => [x.source, x.session_id]),
        enabled: input.enabled,
      };
      let controlAck;
      const custody=c.get("custody");if(custody&&!identity.control)throw new IngestUnavailable("fence","control_unavailable");
      if(identity.control){
        if(identity.control.kind!=="selection"||!deps.deliver.ledger.applyOrderedControl)throw new IngestUnavailable("fence","control_unavailable");
        const result=orderedControlResult(await deps.deliver.ledger.applyOrderedControl({...selection,control:{...identity.control,kind:"selection"},...(custody?{custody}:{})}),identity.control);
        controlAck=result.control;custodyHeader(c,result.custodyAck);
      }else await deps.deliver.ledger.setSessionUploads(selection);
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
      const tenant={ orgId: identity.orgId, workspaceId: identity.workspaceId, userId: identity.userId };
      let deleted:number,controlAck;
      const custody=c.get("custody");if(custody&&!identity.control)throw new IngestUnavailable("fence","control_unavailable");
      if(identity.control){
        if(identity.control.kind!=="reset"||!deps.deliver.ledger.applyOrderedControl)throw new IngestUnavailable("fence","control_unavailable");
        const result=orderedControlResult(await deps.deliver.ledger.applyOrderedControl({tenant,requestId:input.request_id,control:{...identity.control,kind:"reset"},...(custody?{custody}:{})}),identity.control);
        deleted=result.deleted!;controlAck=result.control;custodyHeader(c,result.custodyAck);
      }else deleted=await deps.deliver.ledger.resetUploadedSessions(tenant,input.request_id);
      emit({ route: "delivery.sessions.reset", outcome: controlAck?"control_"+controlAck.outcome:"reset", org_id: identity.orgId, deleted });
      return c.json({ reset: true, deleted,...(controlAck?{control:controlAck}:{}) });
    } catch (error) {
      emit({ route: "delivery.sessions.reset", outcome: error instanceof DeliveryError ? error.code : "delivery_unavailable", org_id: identity.orgId });
      return failure(c, error instanceof DeliveryError ? error : new IngestUnavailable("fence", "ledger"));
    }
  });

  app.all("/v1/*", (c) => c.json(errorBody(c, "unsupported_by_backend", `${c.req.method} ${new URL(c.req.url).pathname} is not served by the session ingest`, { reason: "route_not_migrated" }), 501));
  app.notFound((c) => c.json(errorBody(c, "not_found", `No route for ${c.req.method} ${new URL(c.req.url).pathname}`), 404));
  app.onError((_err, c) => c.json(errorBody(c, "internal_error", "Internal server error"), 500));
  return app;
}
