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
import { Hono, type Context } from "hono";
import { log } from "../shared/log.ts";
import { AuthError, type VerifiedIdentity } from "../session-read-api/app.ts";
import { deliverBatch, IngestUnavailable, type DeliverDeps } from "./deliver.ts";
import { DELIVERY_LIMITS, DeliveryBusyError, DeliveryError, MAX_DELIVERY_BYTES } from "./vendor/delivery/delivery-contracts.ts";
import { parseDeliveryRequest } from "./vendor/delivery/prepare.ts";

export const SYNC_SCOPE = "rth:sync";
export type ContentEncoding = "zstd" | "gzip";

export const IDENTITY_HEADER = "x-relay-ingest-identity";

/** The identity relayhistory verified, as it sends it: base64url JSON { userId, orgId, workspaceId, scopes }. */
export function bindingIdentity(request: Request): VerifiedIdentity {
  const raw = request.headers.get(IDENTITY_HEADER);
  if (!raw) throw new AuthError(401, "missing_authorization", "missing Authorization header");
  let v: Partial<VerifiedIdentity> | null = null;
  try {
    v = JSON.parse(Buffer.from(raw, "base64url").toString("utf8")) as Partial<VerifiedIdentity>;
  } catch {
    // fall through
  }
  const id = (x: unknown) => typeof x === "string" && x.length > 0 && x.length <= 512;
  if (!v || !id(v.userId) || !id(v.orgId) || typeof v.workspaceId !== "string" || !Array.isArray(v.scopes) || !v.scopes.every((s) => typeof s === "string"))
    throw new AuthError(401, "invalid_token", "invalid or expired token");
  return { userId: v.userId!, orgId: v.orgId!, workspaceId: v.workspaceId, scopes: v.scopes };
}

export function encodeIdentity(identity: VerifiedIdentity): string {
  return Buffer.from(JSON.stringify(identity), "utf8").toString("base64url");
}

export interface IngestAppDeps {
  identify?: (request: Request) => VerifiedIdentity | Promise<VerifiedIdentity>;
  deliver: DeliverDeps;
  log?: (fields: Record<string, unknown>) => void;
}

type Env = { Variables: { correlationId: string; identity: VerifiedIdentity } };

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
  const identify = deps.identify ?? bindingIdentity;
  app.use("/v1/*", async (c, next) => {
    try {
      c.set("identity", await identify(c.req.raw));
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
      const batch = parseDeliveryRequest(await boundedJson(c.req.raw, read));
      const result = await deliverBatch(deps.deliver, { orgId: identity.orgId, workspaceId: identity.workspaceId, userId: identity.userId }, batch);
      emit({ ...fields, outcome: result.outcome, count: result.stats.records, manifests: result.stats.manifests_written, deleted: result.stats.deleted_sessions, state: result.stats.replica, duration_ms: Date.now() - started, bytes_raw: read.bytes });
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

  app.all("/v1/*", (c) => c.json(errorBody(c, "unsupported_by_backend", `${c.req.method} ${new URL(c.req.url).pathname} is not served by the session ingest`, { reason: "route_not_migrated" }), 501));
  app.notFound((c) => c.json(errorBody(c, "not_found", `No route for ${c.req.method} ${new URL(c.req.url).pathname}`), 404));
  app.onError((_err, c) => c.json(errorBody(c, "internal_error", "Internal server error"), 500));
  return app;
}
