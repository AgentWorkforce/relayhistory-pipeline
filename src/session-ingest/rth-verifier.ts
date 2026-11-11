/**
 * The ingest edge's verifier for opaque relayhistory access tokens (`rth_at_`): step 7 of the
 * direct-to-S3 / RHC-off plan, scoping option (d) "verifier client + bounded default-off cache".
 * It checks a bearer against relayhistory's own session table through the existing SQL function
 * `sessions.auth_session_verify(p_access_token_hash text, p_now timestamptz)`
 * (supabase/migrations/20261104020100_auth_session_verify.sql), called over PostgREST as the NOLOGIN
 * role `session_token_verifier`. Nothing here is deployed or routed; it is only reachable through
 * ingest-edge.ts, which is itself default-off.
 *
 * Default off: INGEST_EDGE_RTH_VERIFIER (unset, "" or "off" = off; exactly "supabase" = on; anything
 * else refuses startup, naming the variable, never the value). Off, no other INGEST_EDGE_RTH_VERIFIER_*
 * variable is read and the edge keeps step 6 behaviour exactly: every `rth_*` bearer is 401
 * unsupported_token and never sent anywhere. On, only `rth_at_` comes here; `rth_st_` and `rth_rt_`
 * stay 401 unsupported_token and are never sent anywhere.
 *
 * Environment (on only; each missing or malformed value refuses startup, naming the variable; no
 * defaults point at any host):
 *   INGEST_EDGE_RTH_VERIFIER_URL         the PostgREST base URL (e.g. https://example.invalid/rest/v1,
 *                                        or a self-hosted PostgREST root); the call goes to
 *                                        `<base>/rpc/auth_session_verify`. Same destination policy as
 *                                        the edge's introspection URL (https, loopback, or an in-cluster
 *                                        Service name with ALLOW_IN_CLUSTER_HTTP=true).
 *   INGEST_EDGE_RTH_VERIFIER_JWT_SECRET  the verifier credential: an HS256 key (>= 32 characters) that
 *                                        PostgREST trusts. It should be a key dedicated to this verifier
 *                                        in PostgREST's JWKS, never a project-wide secret that can mint
 *                                        other roles. Injected, never on disk.
 *   INGEST_EDGE_RTH_VERIFIER_ISSUER      the `iss` and `aud` the function's
 *   INGEST_EDGE_RTH_VERIFIER_AUDIENCE    sessions.auth_token_verifier_policy row pins.
 *   INGEST_EDGE_RTH_VERIFIER_POSITIVE_TTL_MS  default 0 (no positive cache); an integer 0..60000.
 *   INGEST_EDGE_RTH_VERIFIER_NEGATIVE_TTL_MS  default 0 (no negative cache); an integer 0..5000.
 *
 * Why a signing key and not a static bearer: the function refuses any verifier JWT whose exp - iat is
 * over the policy's max_lifetime_seconds (at most 60), so a fixed token stops working within a minute.
 * Each call therefore carries a freshly minted JWT { role: session_token_verifier, iss, aud, iat,
 * exp = iat + RTH_VERIFIER_JWT_LIFETIME_S }.
 *
 * Per `rth_at_` bearer: p_access_token_hash = SHA-256 of the raw bearer as 64 lowercase hex characters
 * (the function's own check: `^[0-9a-f]{64}$`, "hashToken output"), p_now = the edge's clock. One call
 * bounded at RTH_VERIFIER_TIMEOUT_MS (request and body read), no retry.
 *   one row with user, org, workspace and scopes  identity { userId, orgId, workspaceId, scopes }
 *   ...whose scopes lack rth:sync                 403 forbidden
 *   no row (unknown, revoked or expired)          401 invalid_token
 *   a row already expired by the edge's clock     401 token_expired (never cached)
 *   PostgREST unreachable, timeout, any non-200 (5xx, our JWT refused, the function's 42501 when no
 *   policy row exists, function missing), oversized or malformed answer
 *                                                 503 rth_verifier_unavailable; the edge adds its
 *                                                 jittered 5..10 s Retry-After. Never a 401: a hosted
 *                                                 outage must not read as a bad token.
 *
 * Cache ("no auth/cache waiver" ruling): keyed by the token hash only (one verifier configuration per
 * edge), at most RTH_VERIFIER_CACHE_MAX_ENTRIES entries, LRU. A positive entry lives
 * min(POSITIVE_TTL_MS, access_token_expires_at - now) and so never past the token's expiry; revocation
 * lag is bounded by POSITIVE_TTL_MS (default 0 = none). A negative entry is stored only for a definite
 * no-row answer, for at most 5 s. Outages are never cached. Concurrent identical lookups share one
 * in-flight call. With both TTLs 0 (the default) nothing is cached.
 *
 * The bearer, its hash, the credential and the minted JWT are never logged, echoed or put in an error.
 */
import { createHash, createHmac } from "node:crypto";
import { assertDestinationUrl, destinationPolicy } from "../shared/destination-url.ts";
import { log } from "../shared/log.ts";
import { AuthError } from "../session-read-api/app.ts";
import { SYNC_SCOPE, type IngestIdentity } from "./app.ts";

export const RTH_VERIFIER_FLAG = "INGEST_EDGE_RTH_VERIFIER";
export const RTH_VERIFIER_URL_VAR = "INGEST_EDGE_RTH_VERIFIER_URL";
export const RTH_VERIFIER_SECRET_VAR = "INGEST_EDGE_RTH_VERIFIER_JWT_SECRET";
export const RTH_VERIFIER_ISSUER_VAR = "INGEST_EDGE_RTH_VERIFIER_ISSUER";
export const RTH_VERIFIER_AUDIENCE_VAR = "INGEST_EDGE_RTH_VERIFIER_AUDIENCE";
export const RTH_VERIFIER_POSITIVE_TTL_VAR = "INGEST_EDGE_RTH_VERIFIER_POSITIVE_TTL_MS";
export const RTH_VERIFIER_NEGATIVE_TTL_VAR = "INGEST_EDGE_RTH_VERIFIER_NEGATIVE_TTL_MS";

/** The only `INGEST_EDGE_RTH_VERIFIER` value that turns the verifier on. */
export const RTH_VERIFIER_BACKEND = "supabase";
/** The only token class sent to the verifier. */
export const RTH_ACCESS_TOKEN_PREFIX = "rth_at_";
/**
 * The scope an `rth_at_` row must carry for the edge's routes. relayhistory's own delivery routes use
 * the same vocabulary (packages/pipeline/src/routes/pipeline.ts requireScope("rth:sync")), and it is
 * the scope the `sh_at_` path requires (selfhost-auth.ts).
 */
export const RTH_VERIFIER_REQUIRED_SCOPE = SYNC_SCOPE;

export const RTH_VERIFIER_RPC_PATH = "/rpc/auth_session_verify";
export const RTH_VERIFIER_PROFILE = "sessions";
export const RTH_VERIFIER_ROLE = "session_token_verifier";
/** Minted verifier JWT lifetime; must not exceed the policy row's max_lifetime_seconds (default 60). */
export const RTH_VERIFIER_JWT_LIFETIME_S = 30;
export const RTH_VERIFIER_TIMEOUT_MS = 2_000;
export const RTH_VERIFIER_MAX_RESPONSE_BYTES = 16 * 1024;
export const RTH_VERIFIER_POSITIVE_TTL_MAX_MS = 60_000;
export const RTH_VERIFIER_NEGATIVE_TTL_MAX_MS = 5_000;
export const RTH_VERIFIER_CACHE_MAX_ENTRIES = 1_000;
export const RTH_VERIFIER_MIN_SECRET_CHARS = 32;
export const RTH_VERIFIER_UNAVAILABLE_CODE = "rth_verifier_unavailable";

export interface RthVerifierSettings {
  url: string;
  jwtSecret: string;
  issuer: string;
  audience: string;
  positiveTtlMs: number;
  negativeTtlMs: number;
}

export type RthVerifierConfig = { enabled: false } | ({ enabled: true } & RthVerifierSettings);

/** Exactly "supabase" enables; unset, empty or "off" is off; anything else throws, naming only the variable. */
export function rthVerifierEnabled(env: NodeJS.ProcessEnv = process.env): boolean {
  const raw = env[RTH_VERIFIER_FLAG];
  if (raw === undefined || raw === "" || raw === "off") return false;
  if (raw === RTH_VERIFIER_BACKEND) return true;
  throw new Error(`96bulls: ${RTH_VERIFIER_FLAG} must be ${RTH_VERIFIER_BACKEND} or off`);
}

function need(env: NodeJS.ProcessEnv, name: string): string {
  const value = env[name]?.trim();
  if (!value) throw new Error(`96bulls: ${RTH_VERIFIER_FLAG}=${RTH_VERIFIER_BACKEND} needs ${name}`);
  return value;
}

function ttl(env: NodeJS.ProcessEnv, name: string, max: number): number {
  const raw = env[name];
  if (raw === undefined || raw === "") return 0;
  if (!/^\d{1,6}$/.test(raw) || Number(raw) > max) throw new Error(`96bulls: ${name} must be an integer 0..${max}`);
  return Number(raw);
}

/** Off: `{ enabled: false }` and nothing else is read. On: every value is checked, or startup is refused. */
export function rthVerifierConfig(env: NodeJS.ProcessEnv = process.env): RthVerifierConfig {
  if (!rthVerifierEnabled(env)) return { enabled: false };
  const url = need(env, RTH_VERIFIER_URL_VAR).replace(/\/+$/, "");
  assertDestinationUrl(RTH_VERIFIER_URL_VAR, url, destinationPolicy(env));
  const jwtSecret = need(env, RTH_VERIFIER_SECRET_VAR);
  if (jwtSecret.length < RTH_VERIFIER_MIN_SECRET_CHARS)
    throw new Error(`96bulls: ${RTH_VERIFIER_SECRET_VAR} must be at least ${RTH_VERIFIER_MIN_SECRET_CHARS} characters`);
  const issuer = need(env, RTH_VERIFIER_ISSUER_VAR);
  // The policy table's own CHECK (issuer ~ '^https?://'): anything else can never match.
  if (!/^https?:\/\//.test(issuer)) throw new Error(`96bulls: ${RTH_VERIFIER_ISSUER_VAR} must start with http:// or https://`);
  const audience = need(env, RTH_VERIFIER_AUDIENCE_VAR);
  const positiveTtlMs = ttl(env, RTH_VERIFIER_POSITIVE_TTL_VAR, RTH_VERIFIER_POSITIVE_TTL_MAX_MS);
  const negativeTtlMs = ttl(env, RTH_VERIFIER_NEGATIVE_TTL_VAR, RTH_VERIFIER_NEGATIVE_TTL_MAX_MS);
  return { enabled: true, url, jwtSecret, issuer, audience, positiveTtlMs, negativeTtlMs };
}

export interface RthVerifierOptions {
  /** Test seam: the PostgREST transport. Production: the global fetch. */
  fetch?: typeof fetch;
  /** Test seam: the call bound. Default RTH_VERIFIER_TIMEOUT_MS. */
  timeoutMs?: number;
  /** Test seam: the edge's clock (ms since epoch), used for p_now, the JWT and the cache. */
  now?: () => number;
}

/** Verifies one raw `rth_at_` bearer; throws AuthError (401 / 403 / 503) to refuse. */
export type RthVerify = (bearer: string) => Promise<IngestIdentity>;

/** The lookup key and p_access_token_hash: SHA-256 of the raw bearer, 64 lowercase hex characters. */
export function rthTokenHash(bearer: string): string {
  return createHash("sha256").update(bearer, "utf8").digest("hex");
}

const b64url = (value: string | Buffer) => Buffer.from(value).toString("base64url");

/** A short-lived HS256 JWT for the verifier role, as the policy row requires. */
export function mintVerifierJwt(settings: Pick<RthVerifierSettings, "jwtSecret" | "issuer" | "audience">, nowMs: number): string {
  const iat = Math.floor(nowMs / 1000);
  const head = b64url(JSON.stringify({ alg: "HS256", typ: "JWT" }));
  const body = b64url(JSON.stringify({ role: RTH_VERIFIER_ROLE, iss: settings.issuer, aud: settings.audience, iat, exp: iat + RTH_VERIFIER_JWT_LIFETIME_S }));
  const sig = createHmac("sha256", settings.jwtSecret).update(`${head}.${body}`).digest("base64url");
  return `${head}.${body}.${sig}`;
}

/** An outage: never a 401, never cached. `reason` is a closed value for the log. */
class Unavailable extends Error {
  constructor(readonly reason: string, readonly status?: number) {
    super("rth verifier unavailable");
  }
}

type Outcome =
  | { kind: "row"; identity: IngestIdentity; expiresAtMs: number }
  | { kind: "no_row" }
  | { kind: "expired" };

const ID_MAX = 256;
const okId = (v: unknown): v is string => typeof v === "string" && v.length > 0 && v.length <= ID_MAX;

/** Parses PostgREST's answer: [] (no row) or exactly one well-formed row. Anything else is an outage. */
function parseAnswer(text: string, nowMs: number): Outcome {
  let rows: unknown;
  try {
    rows = JSON.parse(text);
  } catch {
    throw new Unavailable("malformed");
  }
  if (!Array.isArray(rows) || rows.length > 1) throw new Unavailable("malformed");
  if (rows.length === 0) return { kind: "no_row" };
  const row = rows[0] as Record<string, unknown> | null;
  if (!row || typeof row !== "object") throw new Unavailable("malformed");
  const { subject_type, user_id, org_id, workspace_id, scopes, access_token_expires_at } = row;
  if (!okId(row.id) || !okId(subject_type) || !okId(user_id) || !okId(org_id) || !okId(workspace_id)) throw new Unavailable("malformed");
  if (!Array.isArray(scopes) || scopes.length > 64 || !scopes.every((s) => typeof s === "string" && s.length > 0 && s.length <= 128))
    throw new Unavailable("malformed");
  const expiresAtMs = typeof access_token_expires_at === "string" ? Date.parse(access_token_expires_at) : Number.NaN;
  if (!Number.isFinite(expiresAtMs)) throw new Unavailable("malformed");
  if (expiresAtMs <= nowMs) return { kind: "expired" };
  return { kind: "row", identity: { userId: user_id, orgId: org_id, workspaceId: workspace_id, scopes: [...(scopes as string[])] }, expiresAtMs };
}

/** At most `limit` bytes of the body, or null when larger (the rest is cancelled, never buffered). */
async function readCapped(response: Response, limit: number): Promise<string | null> {
  const declared = Number(response.headers.get("content-length"));
  if (Number.isFinite(declared) && declared > limit) {
    await response.body?.cancel().catch(() => {});
    return null;
  }
  if (!response.body) return "";
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
  return Buffer.concat(chunks).toString("utf8");
}

const fetchFailure = (error: unknown) => {
  const name = (error as { name?: string } | null)?.name;
  return name === "TimeoutError" || name === "AbortError" ? "timeout" : "network_error";
};

/**
 * The verifier. `settings` comes from `rthVerifierConfig` (enabled); the URL is checked against the
 * destination policy again here so a caller cannot bypass it.
 */
export function createRthVerifier(settings: RthVerifierSettings, env: NodeJS.ProcessEnv = process.env, options: RthVerifierOptions = {}): RthVerify {
  const base = settings.url.replace(/\/+$/, "");
  assertDestinationUrl(RTH_VERIFIER_URL_VAR, base, destinationPolicy(env));
  const endpoint = `${base}${RTH_VERIFIER_RPC_PATH}`;
  const doFetch = options.fetch ?? ((...args: Parameters<typeof fetch>) => fetch(...args));
  const now = options.now ?? (() => Date.now());
  const timeoutMs = options.timeoutMs ?? RTH_VERIFIER_TIMEOUT_MS;
  const positiveTtlMs = Math.max(0, Math.min(settings.positiveTtlMs, RTH_VERIFIER_POSITIVE_TTL_MAX_MS));
  const negativeTtlMs = Math.max(0, Math.min(settings.negativeTtlMs, RTH_VERIFIER_NEGATIVE_TTL_MAX_MS));
  const cache = new Map<string, { outcome: Outcome; expiresAt: number }>();
  const inflight = new Map<string, Promise<Outcome>>();

  const remember = (key: string, outcome: Outcome, at: number) => {
    let expiresAt: number;
    if (outcome.kind === "row") expiresAt = Math.min(at + positiveTtlMs, outcome.expiresAtMs);
    else if (outcome.kind === "no_row") expiresAt = at + negativeTtlMs;
    else return; // "expired" is never cached
    if (expiresAt <= at) return; // TTL 0: never cached
    cache.delete(key);
    while (cache.size >= RTH_VERIFIER_CACHE_MAX_ENTRIES) {
      const oldest = cache.keys().next().value;
      if (oldest === undefined) break;
      cache.delete(oldest);
    }
    cache.set(key, { outcome, expiresAt });
  };

  async function lookup(hash: string): Promise<Outcome> {
    const at = now();
    let response: Response;
    const signal = AbortSignal.timeout(timeoutMs);
    try {
      response = await doFetch(endpoint, {
        method: "POST",
        headers: {
          authorization: `Bearer ${mintVerifierJwt(settings, at)}`,
          "content-profile": RTH_VERIFIER_PROFILE,
          "content-type": "application/json",
          accept: "application/json",
        },
        body: JSON.stringify({ p_access_token_hash: hash, p_now: new Date(at).toISOString() }),
        redirect: "error",
        signal,
      });
    } catch (error) {
      throw new Unavailable(fetchFailure(error));
    }
    if (response.status !== 200) {
      // 401/403: our JWT refused, or the function's 42501 (no policy row, wrong iss/aud/lifetime).
      // 404: the function is not exposed. 5xx: PostgREST or the database. All outages, never a 401.
      await response.body?.cancel().catch(() => {});
      throw new Unavailable(`status_${response.status}`, response.status);
    }
    let text: string | null;
    try {
      text = await readCapped(response, RTH_VERIFIER_MAX_RESPONSE_BYTES);
    } catch (error) {
      throw new Unavailable(fetchFailure(error));
    }
    if (text === null) throw new Unavailable("too_large");
    return parseAnswer(text, now());
  }

  return async (bearer) => {
    if (!bearer.startsWith(RTH_ACCESS_TOKEN_PREFIX)) throw new AuthError(401, "unsupported_token", "opaque relayhistory tokens are not supported by this deployment");
    const key = rthTokenHash(bearer);
    let outcome: Outcome;
    const hit = cache.get(key);
    if (hit && hit.expiresAt > now() && (hit.outcome.kind !== "row" || hit.outcome.expiresAtMs > now())) {
      cache.delete(key); // LRU: a hit moves to the back
      cache.set(key, hit);
      outcome = hit.outcome;
    } else {
      if (hit) cache.delete(key); // an expired entry is never reused, not even during an outage
      let pending = inflight.get(key);
      if (!pending) {
        // The TTL runs from when the lookup started, so a slow answer never extends it.
        const startedAt = now();
        const p: Promise<Outcome> = lookup(key)
          .then((fresh) => {
            remember(key, fresh, startedAt);
            return fresh;
          })
          .finally(() => {
            if (inflight.get(key) === p) inflight.delete(key);
          });
        pending = p;
        inflight.set(key, p);
      }
      try {
        outcome = await pending;
      } catch (error) {
        const reason = error instanceof Unavailable ? error.reason : "internal";
        const status = error instanceof Unavailable ? error.status : undefined;
        log.warn("rth verifier unavailable", { route: "ingest_edge.rth_verifier", reason, ...(status ? { status } : {}) });
        throw new AuthError(503, RTH_VERIFIER_UNAVAILABLE_CODE, "token verification is unavailable");
      }
    }
    if (outcome.kind === "no_row") throw new AuthError(401, "invalid_token", "invalid or expired token");
    if (outcome.kind === "expired") throw new AuthError(401, "token_expired", "token has expired");
    if (!outcome.identity.scopes.includes(RTH_VERIFIER_REQUIRED_SCOPE))
      throw new AuthError(403, "forbidden", `missing required scope: ${RTH_VERIFIER_REQUIRED_SCOPE}`);
    const { userId, orgId, workspaceId, scopes } = outcome.identity;
    return { userId, orgId, workspaceId, scopes: [...scopes] };
  };
}
