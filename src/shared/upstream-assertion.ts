/**
 * The verifier side of the relaygate v1 upstream assertion (Ed25519), for the two self-host services: the read
 * API service (audience "session-read-api") and the ingest edge (audience "ingest-edge"). Default off; see
 * docs/selfhost-composition.md ("Upstream assertion").
 *
 *   X-Relaygate-Assertion: v1.<base64url JSON payload>.<base64url Ed25519 signature>
 *   signed bytes: ASCII "relaygate-upstream-v1\n" + the payload segment exactly as received
 *
 * Phase 1 (before the body is read): token shape, strict claim parse, key by kid (current, previous), signature,
 * iss / alg / v, aud (fixed per service), time, method, the exact request target (pathname + query as received),
 * credential (sha256 of the exact Authorization value), and the route table: access class plus every scope that is
 * known before the body. Phase 2 (after the service's own bounded read, before any side effect): body_sha256 over
 * the exact wire bytes, and the body-derived scope for the body-scoped ingest routes.
 *
 * No I/O and no logging here: every check returns a closed reason, and the services log that reason only. The
 * claims grant nothing: `principal` is shape-checked and never used for a decision.
 */
import { createHash, createPublicKey, verify as verifySignature, type KeyObject } from "node:crypto";

export const UPSTREAM_ASSERTION_HEADER = "x-relaygate-assertion";
export const READ_API_UPSTREAM_ASSERTION_PREFIX = "READ_API_UPSTREAM_ASSERTION_";
export const INGEST_UPSTREAM_ASSERTION_PREFIX = "INGEST_UPSTREAM_ASSERTION_";

export type UpstreamAssertionMode = "off" | "log" | "enforce";
export type UpstreamAssertionAudience = "session-read-api" | "ingest-edge";
export type UpstreamAssertionAccess = "read" | "write" | "filter" | "aggregate" | "passthrough";
const ACCESS: readonly string[] = ["read", "write", "filter", "aggregate", "passthrough"];

/** The closed set of refusal reasons; the only assertion detail a service ever logs. */
export type UpstreamAssertionReason =
  | "missing" | "bad_format" | "unknown_kid" | "bad_signature" | "claims" | "aud" | "lifetime" | "not_yet_valid" | "expired"
  | "method" | "path" | "credential" | "access" | "scope" | "body";

export interface UpstreamAssertionConfig {
  mode: "log" | "enforce";
  /** Current and (optionally) previous public key, by kid. Read once at startup. */
  keys: ReadonlyMap<string, KeyObject>;
}

export interface UpstreamAssertionClaims {
  v: number;
  alg: string;
  iss: string;
  aud: string;
  kid: string;
  iat: number;
  exp: number;
  jti: string;
  method: string;
  path: string;
  body_sha256: string;
  credential_sha256: string;
  principal: { agent_id: string; gid: number; groups: number[] };
  access: UpstreamAssertionAccess;
  session_ids: string[];
  session_count: number;
  session_ids_sha256: string;
}

const SIGNING_PREFIX = "relaygate-upstream-v1\n";
const TOKEN_SHAPE = /^v1\.([A-Za-z0-9_-]{1,16384})\.([A-Za-z0-9_-]{86})$/;
const HEX64 = /^[0-9a-f]{64}$/;
const KID_SHAPE = /^[A-Za-z0-9._-]{1,64}$/;
const MIN_LIFETIME_S = 5;
const MAX_LIFETIME_S = 60;
const FUTURE_SKEW_S = 5;
const INLINE_MAX_IDS = 16;
const INLINE_MAX_BYTES = 2048;

export const sha256Hex = (data: string | Uint8Array): string => createHash("sha256").update(data).digest("hex");

// ---------------------------------------------------------------------------------------------------------------
// Configuration

const MODE_VAR = "MODE";
const KEY_VAR = "PUBLIC_KEY";
const KID_VAR = "KID";
const PREVIOUS_KEY_VAR = "PUBLIC_KEY_PREVIOUS";
const PREVIOUS_KID_VAR = "KID_PREVIOUS";

/** `<prefix>MODE`: unset, "" or "off" is off; "log" and "enforce" are the only other values. Errors name the variable only. */
export function upstreamAssertionMode(env: NodeJS.ProcessEnv, prefix: string): UpstreamAssertionMode {
  const raw = env[prefix + MODE_VAR];
  if (raw === undefined || raw === "" || raw === "off") return "off";
  if (raw === "log" || raw === "enforce") return raw;
  throw new Error(`96bulls: ${prefix}${MODE_VAR} must be off, log or enforce`);
}

/** Canonical standard base64 of an Ed25519 SPKI DER public key, or a throw naming `name` (never the value). */
function publicKey(value: string | undefined, name: string): KeyObject {
  const refuse = () => new Error(`96bulls: ${name} must be the canonical standard base64 of an Ed25519 SPKI DER public key`);
  if (value === undefined || value === "") throw new Error(`96bulls: ${name} is required when the upstream assertion is on`);
  if (value.length % 4 !== 0 || !/^[A-Za-z0-9+/]+={0,2}$/.test(value)) throw refuse();
  const der = Buffer.from(value, "base64");
  if (der.toString("base64") !== value) throw refuse();
  let key: KeyObject;
  try {
    key = createPublicKey({ key: der, format: "der", type: "spki" });
  } catch {
    throw refuse();
  }
  if (key.asymmetricKeyType !== "ed25519") throw refuse();
  return key;
}

function kid(value: string | undefined, name: string): string {
  if (value === undefined || value === "") throw new Error(`96bulls: ${name} is required when the upstream assertion is on`);
  if (!KID_SHAPE.test(value)) throw new Error(`96bulls: ${name} must match ^[A-Za-z0-9._-]{1,64}$`);
  return value;
}

/**
 * The verifier config for one service (`prefix` = READ_API_UPSTREAM_ASSERTION_ or INGEST_UPSTREAM_ASSERTION_).
 * Off: null, and no key variable is read. log / enforce: the current key + kid are required, the previous pair is
 * optional but all-or-nothing and must use a different kid. Every refusal names the variable, never its value.
 */
export function upstreamAssertionConfig(env: NodeJS.ProcessEnv, prefix: string): UpstreamAssertionConfig | null {
  const mode = upstreamAssertionMode(env, prefix);
  if (mode === "off") return null;
  const keys = new Map<string, KeyObject>();
  const currentKid = kid(env[prefix + KID_VAR], prefix + KID_VAR);
  keys.set(currentKid, publicKey(env[prefix + KEY_VAR], prefix + KEY_VAR));
  const previousKey = env[prefix + PREVIOUS_KEY_VAR];
  const previousKid = env[prefix + PREVIOUS_KID_VAR];
  const hasKey = previousKey !== undefined && previousKey !== "";
  const hasKid = previousKid !== undefined && previousKid !== "";
  if (hasKey !== hasKid) throw new Error(`96bulls: ${prefix}${PREVIOUS_KEY_VAR} and ${prefix}${PREVIOUS_KID_VAR} must be set together`);
  if (hasKey) {
    const old = kid(previousKid, prefix + PREVIOUS_KID_VAR);
    if (old === currentKid) throw new Error(`96bulls: ${prefix}${PREVIOUS_KID_VAR} must differ from ${prefix}${KID_VAR}`);
    keys.set(old, publicKey(previousKey, prefix + PREVIOUS_KEY_VAR));
  }
  return { mode, keys };
}

/** Hosted entries (Lambda, Workers) serve no upstream assertion: any mode but off is refused, naming the variable. */
export function assertUpstreamAssertionOff(env: Record<string, string | undefined>, prefix: string): void {
  if (upstreamAssertionMode(env, prefix) !== "off") throw new Error(`96bulls: ${prefix}${MODE_VAR} is served only by the self-host service`);
}

// ---------------------------------------------------------------------------------------------------------------
// Session scope

export interface SessionScope {
  count: number;
  digest: string;
  /** The sorted list when count <= 16 and its UTF-8 JSON <= 2048 bytes; otherwise []. */
  inline: string[];
}

/** De-duplicate, plain code-unit sort, canonical JSON = JSON.stringify(sorted); digest = sha256 hex of that JSON. */
export function sessionScope(ids: readonly string[]): SessionScope {
  const sorted = [...new Set(ids)].sort();
  const canonical = JSON.stringify(sorted);
  const inline = sorted.length <= INLINE_MAX_IDS && Buffer.byteLength(canonical, "utf8") <= INLINE_MAX_BYTES ? sorted : [];
  return { count: sorted.length, digest: sha256Hex(canonical), inline };
}

function scopeMatches(claims: UpstreamAssertionClaims, ids: readonly string[]): boolean {
  const want = sessionScope(ids);
  return claims.session_count === want.count && claims.session_ids_sha256 === want.digest && JSON.stringify(claims.session_ids) === JSON.stringify(want.inline);
}

// ---------------------------------------------------------------------------------------------------------------
// Route tables (data)

/** Where a route's session scope comes from. */
type ScopeSource =
  | { kind: "none" }
  | { kind: "path"; segment: number }
  | { kind: "query_session" }
  | { kind: "body"; extract: (value: unknown) => string[] | null };

interface RouteRule {
  method: "GET" | "POST";
  /** Decoded pathname segments; ":" matches any one segment, "**" one or more trailing segments. */
  segments: readonly string[];
  access: UpstreamAssertionAccess;
  scope: ScopeSource;
  /** Optional query condition, for the two GET /v1/events forms. */
  query?: (query: URLSearchParams) => boolean;
}

const NONE: ScopeSource = { kind: "none" };
const rule = (method: "GET" | "POST", path: string, access: UpstreamAssertionAccess, scope: ScopeSource = NONE, query?: RouteRule["query"]): RouteRule =>
  ({ method, segments: path.split("/").slice(1), access, scope, ...(query ? { query } : {}) });

const isObject = (v: unknown): v is Record<string, unknown> => typeof v === "object" && v !== null && !Array.isArray(v);
/** `value[outer...][list][].field` as strings, or null when the body does not have that shape. */
function idsAt(value: unknown, outer: readonly string[], list: string, field: string): string[] | null {
  let at = value;
  for (const key of outer) {
    if (!isObject(at)) return null;
    at = at[key];
  }
  if (!isObject(at) || !Array.isArray(at[list])) return null;
  const out: string[] = [];
  for (const item of at[list] as unknown[]) {
    if (!isObject(item) || typeof item[field] !== "string") return null;
    out.push(item[field] as string);
  }
  return out;
}

/** The body-derived scopes of the ingest table, over the decoded JSON value the app already parses. */
export const ingestRecordsScope = (value: unknown) => idsAt(value, [], "records", "sessionId");
export const deliveryBatchScope = (value: unknown) => idsAt(value, ["batch"], "records", "session_id");
export const sessionSelectionScope = (value: unknown) => idsAt(value, [], "sessions", "session_id");

/** The one session= value of a query, or null when there is not exactly one (empty string when it is given empty). */
function singleSessionValue(query: URLSearchParams): string | null {
  const values = query.getAll("session");
  return values.length === 1 ? values[0]! : null;
}

const SESSION_READS = ["events", "turns", "metadata", "thread", "catalog", "timing", "analysis", "brief", "summary"];

export const UPSTREAM_ASSERTION_ROUTES: Readonly<Record<UpstreamAssertionAudience, readonly RouteRule[]>> = Object.freeze({
  "session-read-api": Object.freeze([
    ...["users", "activity", "output", "titles"].map((p) => rule("GET", `/v1/sessions/${p}`, "aggregate")),
    rule("GET", "/v1/sessions/summaries/activity", "aggregate"),
    ...SESSION_READS.map((p) => rule("GET", `/v1/sessions/:/${p}`, "read", { kind: "path", segment: 2 })),
    rule("GET", "/v1/events", "read", { kind: "query_session" }, (q) => (singleSessionValue(q) ?? "") !== ""),
    // The app treats an empty session= as absent: with q= it is the workspace search.
    rule("GET", "/v1/events", "filter", NONE, (q) => q.has("q") && (!q.has("session") || singleSessionValue(q) === "")),
    rule("GET", "/v1/sessions", "filter"),
    rule("GET", "/v1/delivery/records", "aggregate"),
    rule("GET", "/v1/digest/**", "aggregate"),
    rule("GET", "/v1/relayfacts", "passthrough"),
    rule("GET", "/v1/relayfacts/:", "passthrough"),
    rule("GET", "/v1/coordination/overlaps", "passthrough"),
    rule("GET", "/v1/coordination/advisories", "passthrough"),
    rule("POST", "/v1/coordination/advisories/:/ack", "passthrough"),
  ]),
  "ingest-edge": Object.freeze([
    rule("POST", "/v1/sessions/:/turns", "write", { kind: "path", segment: 2 }),
    rule("POST", "/v1/ingest", "write", { kind: "body", extract: ingestRecordsScope }),
    rule("POST", "/v1/delivery/batches", "write", { kind: "body", extract: deliveryBatchScope }),
    rule("POST", "/v1/delivery/sessions/selection", "write", { kind: "body", extract: sessionSelectionScope }),
    rule("GET", "/v1/delivery/admission", "passthrough"),
    rule("POST", "/v1/upload-consent", "passthrough"),
    rule("GET", "/v1/delivery/limits", "passthrough"),
    rule("POST", "/v1/auth/token/refresh", "passthrough"),
    rule("POST", "/v1/onboarding/heartbeat", "passthrough"),
  ]),
});

/** Routes where a MISSING assertion is allowed in enforce (a presented one is still validated). Exact method + raw pathname. */
const EXEMPT: Readonly<Record<UpstreamAssertionAudience, { method: string; path: string }>> = Object.freeze({
  "session-read-api": { method: "POST", path: "/v1/auth/token/revoke" },
  "ingest-edge": { method: "POST", path: "/v1/auth/token/refresh" },
});

const BYPASS_PATHS: ReadonlySet<string> = new Set(["/healthz", "/readyz", "/health"]);

const rawPathname = (target: string) => target.split("?", 1)[0]!;

/** Probes and CORS preflight never see assertion logic. */
export function upstreamAssertionBypass(method: string | undefined, target: string): boolean {
  return method === "OPTIONS" || BYPASS_PATHS.has(rawPathname(target));
}

/**
 * The single rule for a request that matches no table route but carries an assertion: every binding is verified
 * (signature, time, aud, method, path, credential, body), the access must be "passthrough" (else `access`) and the
 * claimed scope must be empty (else `scope`); the service's own auth decides as before.
 */
function unknownRouteRefusal(claims: UpstreamAssertionClaims): UpstreamAssertionReason | null {
  if (claims.access !== "passthrough") return "access";
  return scopeMatches(claims, []) ? null : "scope";
}

// ---------------------------------------------------------------------------------------------------------------
// Target hygiene and matching

/** The pathname's decoded segments, or null when it is ambiguous (dot segment, "//", encoded slash or backslash). */
function pathSegments(pathname: string): string[] | null {
  if (!pathname.startsWith("/") || pathname.includes("//") || pathname.includes("\\") || /%2f|%5c/i.test(pathname)) return null;
  const out: string[] = [];
  for (const raw of pathname.slice(1).split("/")) {
    let segment: string;
    try {
      segment = decodeURIComponent(raw);
    } catch {
      return null;
    }
    if (segment === "." || segment === "..") return null;
    out.push(segment);
  }
  return out;
}

function matchRoute(audience: UpstreamAssertionAudience, method: string, segments: readonly string[], query: URLSearchParams): RouteRule | undefined {
  return UPSTREAM_ASSERTION_ROUTES[audience].find((r) => {
    if (r.method !== method) return false;
    const tail = r.segments[r.segments.length - 1] === "**";
    const fixed = tail ? r.segments.length - 1 : r.segments.length;
    if (tail ? segments.length <= fixed : segments.length !== fixed) return false;
    for (let i = 0; i < fixed; i++) if (r.segments[i] !== ":" && r.segments[i] !== segments[i]) return false;
    return r.query ? r.query(query) : true;
  });
}

// ---------------------------------------------------------------------------------------------------------------
// Token parse

function canonicalBase64url(segment: string): Buffer | null {
  const bytes = Buffer.from(segment, "base64url");
  return bytes.toString("base64url") === segment ? bytes : null;
}

const isInt = (v: unknown): v is number => typeof v === "number" && Number.isInteger(v);
const isString = (v: unknown): v is string => typeof v === "string";

/** Strict claim types (values are checked by the later steps, each with its own reason). Unknown extra claims are ignored. */
function claimsOf(payload: Buffer): UpstreamAssertionClaims | null {
  let value: unknown;
  try {
    value = JSON.parse(new TextDecoder("utf-8", { fatal: true, ignoreBOM: false }).decode(payload));
  } catch {
    return null;
  }
  if (!isObject(value)) return null;
  const c = value;
  const p = c.principal;
  const ok =
    isInt(c.v) && isString(c.alg) && isString(c.iss) && isString(c.aud) && isString(c.kid) && isInt(c.iat) && isInt(c.exp) && isString(c.jti)
    && isString(c.method) && /^[A-Z]+$/.test(c.method) && isString(c.path)
    && isString(c.body_sha256) && HEX64.test(c.body_sha256) && isString(c.credential_sha256) && HEX64.test(c.credential_sha256)
    && isObject(p) && isString(p.agent_id) && isInt(p.gid) && Array.isArray(p.groups) && p.groups.every((g) => isInt(g) && g >= 0)
    && isString(c.access) && ACCESS.includes(c.access)
    && Array.isArray(c.session_ids) && c.session_ids.every(isString)
    && isInt(c.session_count) && c.session_count >= 0 && isString(c.session_ids_sha256) && HEX64.test(c.session_ids_sha256);
  return ok ? (c as unknown as UpstreamAssertionClaims) : null;
}

// ---------------------------------------------------------------------------------------------------------------
// Phase 1 and phase 2

export interface UpstreamAssertionRequest {
  method: string;
  /** The request target exactly as received (node `req.url`): pathname + query. */
  target: string;
  /** The exact Authorization value received, if any. */
  authorization: string | undefined;
  /** The X-Relaygate-Assertion value received, if any. */
  assertion: string | string[] | undefined;
}

/** Phase 1 passed: what phase 2 still has to bind. */
export interface PendingUpstreamAssertion {
  claims: UpstreamAssertionClaims;
  /** The body-derived scope still to check (body-scoped table routes only). */
  bodyScope: ((value: unknown) => string[] | null) | null;
}

export type UpstreamAssertionPhase1 =
  | { ok: true; pending: PendingUpstreamAssertion }
  /** `exempt`: no assertion was presented on this audience's exempt route (allowed, nothing to bind). */
  | { ok: true; pending: null; exempt: true }
  | { ok: false; reason: UpstreamAssertionReason };

/**
 * Phase 1, before any body byte is read. The caller has already handled bypass and authenticated-internal routes.
 * `nowMs` is the injectable clock; time is compared in whole seconds.
 */
export function verifyUpstreamAssertionPhase1(
  config: UpstreamAssertionConfig,
  audience: UpstreamAssertionAudience,
  request: UpstreamAssertionRequest,
  nowMs: number = Date.now(),
): UpstreamAssertionPhase1 {
  const pathname = rawPathname(request.target);
  const token = request.assertion;
  if (token === undefined || token === "") {
    const exempt = EXEMPT[audience];
    return request.method === exempt.method && pathname === exempt.path ? { ok: true, pending: null, exempt: true } : { ok: false, reason: "missing" };
  }
  if (typeof token !== "string") return { ok: false, reason: "bad_format" };
  // 1. shape
  const match = TOKEN_SHAPE.exec(token);
  if (!match) return { ok: false, reason: "bad_format" };
  const payloadSegment = match[1]!;
  const payload = canonicalBase64url(payloadSegment);
  const signature = canonicalBase64url(match[2]!);
  if (!payload || !signature || signature.length !== 64) return { ok: false, reason: "bad_format" };
  // 2. strict parse
  const claims = claimsOf(payload);
  if (!claims) return { ok: false, reason: "bad_format" };
  // 3. key by kid (current or previous)
  const key = config.keys.get(claims.kid);
  if (!key) return { ok: false, reason: "unknown_kid" };
  // 4. Ed25519 over the prefix + the payload segment as received
  if (!verifySignature(null, Buffer.from(SIGNING_PREFIX + payloadSegment, "utf8"), key, signature)) return { ok: false, reason: "bad_signature" };
  // 5. iss, alg (selects nothing), v
  if (claims.iss !== "relaygate" || claims.alg !== "Ed25519" || claims.v !== 1) return { ok: false, reason: "claims" };
  // 6. aud, fixed per service
  if (claims.aud !== audience) return { ok: false, reason: "aud" };
  // 7. time, whole seconds
  const now = Math.floor(nowMs / 1000);
  const lifetime = claims.exp - claims.iat;
  if (lifetime < MIN_LIFETIME_S || lifetime > MAX_LIFETIME_S) return { ok: false, reason: "lifetime" };
  if (claims.iat > now + FUTURE_SKEW_S) return { ok: false, reason: "not_yet_valid" };
  if (claims.exp <= now) return { ok: false, reason: "expired" };
  // 8. method
  if (claims.method !== request.method) return { ok: false, reason: "method" };
  // 9. the exact target, plus pathname hygiene
  if (claims.path !== request.target) return { ok: false, reason: "path" };
  const segments = pathSegments(pathname);
  if (!segments) return { ok: false, reason: "path" };
  // 10. credential
  if (request.authorization === undefined || claims.credential_sha256 !== sha256Hex(request.authorization)) return { ok: false, reason: "credential" };
  // 11. route table: access and the scope known before the body
  const query = new URLSearchParams(request.target.slice(pathname.length + 1));
  const route = matchRoute(audience, request.method, segments, query);
  if (!route) {
    const refusal = unknownRouteRefusal(claims);
    return refusal === null ? { ok: true, pending: { claims, bodyScope: null } } : { ok: false, reason: refusal };
  }
  if (claims.access !== route.access) return { ok: false, reason: "access" };
  const scope = route.scope;
  if (scope.kind === "body") return { ok: true, pending: { claims, bodyScope: scope.extract } };
  const ids = scope.kind === "path" ? [segments[scope.segment]!] : scope.kind === "query_session" ? query.getAll("session") : [];
  return scopeMatches(claims, ids) ? { ok: true, pending: { claims, bodyScope: null } } : { ok: false, reason: "scope" };
}

/** Phase 2a: body_sha256 over the exact wire bytes the service's bounded read returned (empty = sha256 of zero bytes). */
export function verifyUpstreamAssertionBody(pending: PendingUpstreamAssertion, wire: Uint8Array): UpstreamAssertionReason | null {
  return pending.claims.body_sha256 === sha256Hex(wire) ? null : "body";
}

/** Phase 2b: the body-derived scope, over the decoded value the service parses anyway. No-op for other routes. */
export function verifyUpstreamAssertionScope(pending: PendingUpstreamAssertion, value: unknown): UpstreamAssertionReason | null {
  if (!pending.bodyScope) return null;
  const ids = pending.bodyScope(value);
  return ids !== null && scopeMatches(pending.claims, ids) ? null : "scope";
}

// ---------------------------------------------------------------------------------------------------------------
// The per-request phase-2 hand-off (ingest: the wrapper binds, the app's bounded-read seam calls)

/** Phase 2 for one request. Each call returns true when the request must be refused now (enforce and failed). */
export interface UpstreamAssertionGate {
  body(wire: Uint8Array): boolean;
  scope(value: unknown): boolean;
}

const NO_PHASE_2: UpstreamAssertionGate = Object.freeze({ body: () => false, scope: () => false });

/**
 * Phase 1 plus the phase-2 gate, with the mode applied: `refuse` (enforce, phase 1 failed: answer the generic 401
 * now) or `gate` (continue; phase 2 still runs where there is something to bind). `report` receives every failed
 * check's closed reason, in log and enforce alike; the caller logs it with upstreamAssertionLogFields.
 */
export function checkUpstreamAssertion(
  config: UpstreamAssertionConfig,
  audience: UpstreamAssertionAudience,
  request: UpstreamAssertionRequest,
  report: (reason: UpstreamAssertionReason) => void,
  nowMs: number = Date.now(),
): { refuse: UpstreamAssertionReason } | { gate: UpstreamAssertionGate } {
  const result = verifyUpstreamAssertionPhase1(config, audience, request, nowMs);
  if (!result.ok) {
    report(result.reason);
    return config.mode === "enforce" ? { refuse: result.reason } : { gate: NO_PHASE_2 };
  }
  const pending = result.pending;
  if (!pending) return { gate: NO_PHASE_2 };
  const failed = (reason: UpstreamAssertionReason | null): boolean => {
    if (reason === null) return false;
    report(reason);
    return config.mode === "enforce";
  };
  return { gate: { body: (wire) => failed(verifyUpstreamAssertionBody(pending, wire)), scope: (value) => failed(verifyUpstreamAssertionScope(pending, value)) } };
}

/**
 * The fields of the one log line per failed check, existing allowlisted keys only (src/shared/log.ts): closed
 * values, never the assertion, a segment or hash of it, the Authorization value, the jti, the principal or a
 * session id.
 */
export function upstreamAssertionLogFields(mode: "log" | "enforce", reason: UpstreamAssertionReason, correlationId?: string): Record<string, string> {
  return { route: "upstream_assertion", outcome: mode === "enforce" ? "refused" : "would_refuse", reason, state: mode, ...(correlationId ? { correlation_id: correlationId } : {}) };
}
export const UPSTREAM_ASSERTION_LOG_MESSAGE = "upstream assertion check failed";

const gates = new WeakMap<Request, UpstreamAssertionGate>();
/** Binds phase 2 to exactly this Request object (the one handed to app.fetch), like the verified identity. */
export function bindUpstreamAssertionGate(request: Request, gate: UpstreamAssertionGate): void {
  gates.set(request, gate);
}
export function upstreamAssertionGate(request: Request): UpstreamAssertionGate | undefined {
  return gates.get(request);
}

export const MISSING_UPSTREAM_ASSERTION = "missing_upstream_assertion";
export const INVALID_UPSTREAM_ASSERTION = "invalid_upstream_assertion";
/** The generic refusal for a reason: never says which check failed. */
export function upstreamAssertionRefusal(reason: UpstreamAssertionReason): { code: string; message: string } {
  return reason === "missing"
    ? { code: MISSING_UPSTREAM_ASSERTION, message: "upstream assertion required" }
    : { code: INVALID_UPSTREAM_ASSERTION, message: "invalid upstream assertion" };
}
