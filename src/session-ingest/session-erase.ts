/**
 * Operator per-session erase: the internal route relay-control (Control) calls, PR 1 of 6 (contract only).
 * Contract: docs/operator-session-erase-contract.md. Runtime-neutral (Worker and node:http); no DB access.
 *
 *   POST /internal/v1/session-erase/organizations/{orgId}/workspaces/{workspaceId}/sources/{source}/sessions/{sessionId}
 *   GET  (same path)   the receipt
 *
 * Default off. SESSION_ERASE_API_ENABLED must be exactly "1"; unset, "" or "0" is off and anything else
 * refuses startup. While it is off, or SESSION_ERASE_API_SECRET is unset, every request under the prefix
 * answers 501 NOT_IMPLEMENTED_BODY and nothing is read. A hosted Worker whose erase settings are invalid keeps
 * serving ingest and answers 503 UNAVAILABLE_BODY here instead (reason "config_invalid"). With both set, the request is authenticated
 * (Bearer SESSION_ERASE_API_SECRET, or SESSION_ERASE_API_SECRET_PREVIOUS during a rotation, compared in
 * constant time), the path and body are validated, and the answer is still 501: the erase itself ships in
 * a later PR. Control decides membership before calling; this service never calls Control. The secret,
 * ids and the body are never logged or echoed.
 */
import type { Context, Hono } from "hono";
import { secretMatches } from "../session-read-api/self-issued.ts";

export const SESSION_ERASE_PREFIX = "/internal/v1/session-erase";
export const SESSION_ERASE_FLAG_VAR = "SESSION_ERASE_API_ENABLED";
export const SESSION_ERASE_SECRET_VAR = "SESSION_ERASE_API_SECRET";
export const SESSION_ERASE_SECRET_PREVIOUS_VAR = "SESSION_ERASE_API_SECRET_PREVIOUS";
export const SESSION_ERASE_SECRET_MIN_LENGTH = 32;
export const SESSION_ERASE_MAX_BODY_BYTES = 8 * 1024;
export const SESSION_ERASE_FEATURE = "session_erase";
/** The exact 501 body Control maps (contract section 4). */
export const NOT_IMPLEMENTED_BODY = Object.freeze({ error: "not_implemented", feature: SESSION_ERASE_FEATURE });
/** The 503 body for "config_invalid": the route fails closed and Control retries (contract sections 2 and 5). */
export const UNAVAILABLE_BODY = Object.freeze({ error: "unavailable", feature: SESSION_ERASE_FEATURE });
export const SESSION_ERASE_RETRY_AFTER_SECONDS = "60";
const REQUEST_ID_MAX_BYTES = 128;

export type SessionEraseConfig =
  | { enabled: false; reason: "flag_off" | "secret_unset" | "config_invalid" }
  | { enabled: true; secrets: readonly string[] };

export const SESSION_ERASE_DISABLED: SessionEraseConfig = Object.freeze({ enabled: false, reason: "flag_off" });
/** The Worker's isolation of an invalid flag or secret: erase fails closed, the rest of ingest keeps working. */
export const SESSION_ERASE_CONFIG_INVALID: SessionEraseConfig = Object.freeze({ enabled: false, reason: "config_invalid" });

function eraseSecret(env: Record<string, string | undefined>, name: string): string | undefined {
  const raw = env[name];
  if (raw === undefined || raw === "") return undefined;
  // The bearer is one token (\S+) compared exactly, so a secret holding any whitespace could never match.
  if (/\s/u.test(raw)) throw new Error(`session ingest: ${name} must not contain whitespace`);
  if (raw.length < SESSION_ERASE_SECRET_MIN_LENGTH) throw new Error(`session ingest: ${name} must be at least ${SESSION_ERASE_SECRET_MIN_LENGTH} characters`);
  return raw;
}

/** Exactly "1" enables; unset, "" or "0" is off; anything else throws. Never echoes the value. */
export function sessionEraseEnabled(raw: string | undefined): boolean {
  if (raw === undefined || raw === "" || raw === "0") return false;
  if (raw === "1") return true;
  throw new Error(`session ingest: ${SESSION_ERASE_FLAG_VAR} must be unset, "0" or "1"`);
}

/**
 * The route's config from plain values (the Worker resolves its Secrets Store bindings first). Secrets are
 * validated only while the flag is on; errors name the variable, never the value.
 */
export function sessionEraseConfig(env: Record<string, string | undefined>): SessionEraseConfig {
  if (!sessionEraseEnabled(env[SESSION_ERASE_FLAG_VAR])) return SESSION_ERASE_DISABLED;
  const current = eraseSecret(env, SESSION_ERASE_SECRET_VAR);
  const previous = eraseSecret(env, SESSION_ERASE_SECRET_PREVIOUS_VAR);
  if (!current) {
    if (previous) throw new Error(`session ingest: ${SESSION_ERASE_SECRET_PREVIOUS_VAR} needs ${SESSION_ERASE_SECRET_VAR}`);
    return { enabled: false, reason: "secret_unset" };
  }
  if (previous === current) throw new Error(`session ingest: ${SESSION_ERASE_SECRET_PREVIOUS_VAR} must differ from ${SESSION_ERASE_SECRET_VAR}`);
  return { enabled: true, secrets: Object.freeze(previous ? [current, previous] : [current]) };
}

export function isSessionErasePath(pathname: string): boolean {
  return pathname === SESSION_ERASE_PREFIX || pathname.startsWith(`${SESSION_ERASE_PREFIX}/`);
}

/** relayhistory's `identifier` (app.ts): 1..max UTF-8 bytes, no control characters, well-formed. */
function identifier(v: unknown, maxBytes = 512): v is string {
  return typeof v === "string" && v.length > 0 && new TextEncoder().encode(v).length <= maxBytes && !/[\x00-\x1f\x7f]/u.test(v)
    && !/[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/u.test(v);
}

export interface SessionEraseKey { orgId: string; workspaceId: string; source: string; sessionId: string }

class EraseError extends Error {
  constructor(readonly status: 400 | 401 | 404 | 405 | 413, readonly code: string) {
    super(code);
  }
}

/** The erase key from the raw (still percent-encoded) path; null when the path is not the route's shape. */
export function parseSessionErasePath(pathname: string): SessionEraseKey | null {
  const parts = pathname.slice(SESSION_ERASE_PREFIX.length).split("/");
  // "", organizations, {org}, workspaces, {ws}, sources, {source}, sessions, {session}
  if (parts.length !== 9 || parts[0] !== "" || parts[1] !== "organizations" || parts[3] !== "workspaces" || parts[5] !== "sources" || parts[7] !== "sessions")
    return null;
  const decoded: string[] = [];
  for (const i of [2, 4, 6, 8]) {
    let value: string;
    try {
      value = decodeURIComponent(parts[i]!);
    } catch {
      throw new EraseError(400, "invalid_request");
    }
    if (!identifier(value)) throw new EraseError(400, "invalid_request");
    decoded.push(value);
  }
  const [orgId, workspaceId, source, sessionId] = decoded as [string, string, string, string];
  return { orgId, workspaceId, source, sessionId };
}

export interface SessionEraseRequest { requestId: string; actor: { kind: "user" | "bootstrap"; id: string } }

function exactKeys(v: Record<string, unknown>, keys: readonly string[]): boolean {
  const got = Object.keys(v);
  return got.length === keys.length && keys.every((k) => Object.hasOwn(v, k));
}
const isObject = (v: unknown): v is Record<string, unknown> => !!v && typeof v === "object" && !Array.isArray(v);

/** The POST body, strict: exactly {requestId, actor:{kind, id}}. */
export function parseSessionEraseBody(value: unknown): SessionEraseRequest {
  if (!isObject(value) || !exactKeys(value, ["requestId", "actor"])) throw new EraseError(400, "invalid_request");
  const actor = value.actor;
  if (!identifier(value.requestId, REQUEST_ID_MAX_BYTES) || !isObject(actor) || !exactKeys(actor, ["kind", "id"])
    || (actor.kind !== "user" && actor.kind !== "bootstrap") || !identifier(actor.id, REQUEST_ID_MAX_BYTES))
    throw new EraseError(400, "invalid_request");
  return value as unknown as SessionEraseRequest;
}

/** Reads at most the cap: a declared or streamed body past it is 413 before anything is parsed. */
async function boundedBody(request: Request): Promise<Uint8Array> {
  const declared = request.headers.get("content-length");
  if (declared !== null && !(Number(declared) <= SESSION_ERASE_MAX_BODY_BYTES)) throw new EraseError(413, "body_too_large");
  if (!request.body) return new Uint8Array();
  const reader = request.body.getReader();
  const chunks: Uint8Array[] = [];
  let size = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    size += value.byteLength;
    if (size > SESSION_ERASE_MAX_BODY_BYTES) {
      await reader.cancel().catch(() => {});
      throw new EraseError(413, "body_too_large");
    }
    chunks.push(value);
  }
  const out = new Uint8Array(size);
  let at = 0;
  for (const chunk of chunks) {
    out.set(chunk, at);
    at += chunk.byteLength;
  }
  return out;
}

function bearer(c: Context): string | null {
  return /^Bearer\s+(\S+)$/iu.exec(c.req.header("authorization") ?? "")?.[1] ?? null;
}

const errorJson = (c: Context, status: number, code: string) => {
  if (status === 401) c.header("WWW-Authenticate", "Bearer");
  return c.json({ error: code, feature: SESSION_ERASE_FEATURE }, status as never);
};

/**
 * Registers the route on the ingest app (before its /v1/* fallbacks; the prefix is outside /v1, so no
 * existing route or middleware sees it). `log` gets one line per request: route, kind (the HTTP method), status, outcome
 * and the correlation id only.
 */
export function registerSessionEraseRoutes(app: Hono<any>, config: SessionEraseConfig, log: (fields: Record<string, unknown>) => void): void {
  const handler = async (c: Context) => {
    const line = (status: number, outcome: string) =>
      log({ route: "operator.session_erase", kind: c.req.method, status, outcome, correlation_id: c.get("correlationId") });
    if (!config.enabled) {
      if (config.reason === "config_invalid") {
        // Fail closed and retryable: the operator fixes the setting; nothing is authenticated or read.
        line(503, config.reason);
        c.header("Retry-After", SESSION_ERASE_RETRY_AFTER_SECONDS);
        return c.json(UNAVAILABLE_BODY, 503);
      }
      line(501, config.reason);
      return c.json(NOT_IMPLEMENTED_BODY, 501);
    }
    try {
      const presented = bearer(c);
      if (!presented || !(await secretMatches(presented, config.secrets))) throw new EraseError(401, "invalid_token");
      const key = parseSessionErasePath(new URL(c.req.url).pathname);
      if (!key) throw new EraseError(404, "not_found");
      if (c.req.method === "POST") {
        const bytes = await boundedBody(c.req.raw);
        let value: unknown;
        try {
          value = JSON.parse(new TextDecoder("utf-8", { fatal: true, ignoreBOM: false }).decode(bytes));
        } catch {
          throw new EraseError(400, "invalid_request");
        }
        parseSessionEraseBody(value);
      } else if (c.req.method !== "GET") {
        c.header("Allow", "GET, POST");
        throw new EraseError(405, "method_not_allowed");
      }
      // PR 1: authenticated and validated, but the erase itself is a later PR.
      line(501, "not_implemented");
      return c.json(NOT_IMPLEMENTED_BODY, 501);
    } catch (error) {
      if (!(error instanceof EraseError)) throw error;
      line(error.status, error.code);
      return errorJson(c, error.status, error.code);
    }
  };
  // A path whose decoded form holds a control character (%0A) matches no Hono route at all, this one
  // included: the app's generic 404 answers it, before any auth. Every other id is checked by the parser.
  app.all(SESSION_ERASE_PREFIX, handler);
  app.all(`${SESSION_ERASE_PREFIX}/*`, handler);
}
