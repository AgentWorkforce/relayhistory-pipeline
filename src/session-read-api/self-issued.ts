/**
 * Self-issued read tokens (READ_API_AUTH_MODE=self_issued, default off; contract:
 * docs/contracts/session-read-api-v1.openapi.yaml). Runtime-neutral: no AWS or Node imports.
 *
 * Tokens are opaque: a distinct prefix plus 256 bits of CSPRNG entropy, stored only as the SHA-256
 * hash of the whole string (migration 20261105054000). Every read looks the hash up, so deleting
 * the row is immediate revocation and there is no signing key to rotate. Identity (user, org,
 * workspace) comes only from the row; the row's audience must be this service's origin.
 *
 * Trusted callers (Control's session handoff, an operator) authenticate to /internal/v1/session-read/*
 * with READ_API_INTERNAL_SECRET, which may overlap with READ_API_INTERNAL_SECRET_PREVIOUS while it
 * rotates. Control decides membership before calling; this service never calls Control. Token
 * values and the secret are never logged or echoed in an error body.
 */
import { bodyLimit } from "hono/body-limit";
import type { Context, Hono } from "hono";

export const ACCESS_PREFIX = "rth_at_read_";
export const REFRESH_PREFIX = "rth_rt_read_";
export const SERVICE_PREFIX = "rth_st_read_";
export const READ_SCOPE = "rth:read";
export const READ_SCOPES: readonly string[] = Object.freeze([READ_SCOPE]);

export const ACCESS_TTL_SECONDS_DEFAULT = 300;
export const ACCESS_TTL_SECONDS_MIN = 60;
export const ACCESS_TTL_SECONDS_MAX = 3600;
export const REFRESH_TTL_SECONDS = 60 * 60 * 24 * 90;
export const SERVICE_TTL_DAYS_DEFAULT = 90;
export const SERVICE_TTL_DAYS_MAX = 365;
export const INTERNAL_SECRET_MIN_LENGTH = 32;
export const TOKEN_MAX_CHARS = 4096;
export const INTERNAL_MAX_BODY_BYTES = 8 * 1024;
const LABEL_MAX_CHARS = 128;
const ID_SHAPE = /^[A-Za-z0-9][A-Za-z0-9_.:-]{0,255}$/u;

export const SESSIONS_PATH = "/internal/v1/session-read/sessions";
export const REFRESH_PATH = "/internal/v1/session-read/sessions/refresh";
export const SERVICE_TOKENS_PATH = "/internal/v1/session-read/service-tokens";
export const INTERNAL_REVOKE_PATH = "/internal/v1/session-read/revoke";
export const CLIENT_REVOKE_PATH = "/v1/auth/token/revoke";
/** Every POST route the self-issued mode serves, for the HTTP entry's body handling. */
export const TOKEN_ROUTE_PATHS: readonly string[] = Object.freeze([SESSIONS_PATH, REFRESH_PATH, SERVICE_TOKENS_PATH, INTERNAL_REVOKE_PATH, CLIENT_REVOKE_PATH]);

export interface TokenRow {
  id: string;
  subjectType: "cli" | "service";
  audience: string;
  userId: string;
  orgId: string;
  workspaceId: string;
  scopes: string[];
}

export interface NewTokenRow {
  id: string;
  family: string;
  subjectType: "cli" | "service";
  audience: string;
  userId: string;
  orgId: string;
  workspaceId: string;
  accessHash: string;
  accessExpires: Date;
  refreshHash: string;
  refreshExpires: Date;
  label: string | null;
  now: Date;
}

export interface RefreshInput {
  refreshHash: string;
  userId: string;
  orgId: string;
  workspaceId: string;
  audience: string;
  nextAccessHash: string;
  accessExpires: Date;
  nextRefreshHash: string;
  refreshExpires: Date;
  now: Date;
}

/** The token table's functions. A rejected promise is an outage: every caller fails closed. */
export interface TokenStore {
  verify(accessHash: string, now: Date): Promise<TokenRow | null>;
  issue(row: NewTokenRow): Promise<void>;
  /** Atomic compare-and-set on the refresh hash and the exact identity; null when nothing matched. */
  refresh(input: RefreshInput): Promise<TokenRow | null>;
  deleteByHash(kind: "access" | "refresh", hash: string): Promise<boolean>;
  deleteFor(orgId: string, filter: { id?: string; userId?: string; workspaceId?: string }): Promise<number>;
}

export interface SelfIssuedDeps {
  store: TokenStore;
  /** This service's normalized origin; every issued token is bound to it. */
  audience: string;
  /** The current internal secret, then the previous one during an overlap. */
  internalSecrets: readonly string[];
  accessTtlSeconds: number;
  now?: () => number;
}

export async function hashToken(token: string): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(token));
  return Array.from(new Uint8Array(digest), (b) => b.toString(16).padStart(2, "0")).join("");
}

/** A prefix plus 32 CSPRNG bytes: 43 base64url characters, 256 bits. */
export function generateToken(prefix: string): string {
  const bytes = crypto.getRandomValues(new Uint8Array(32));
  let bin = "";
  for (const b of bytes) bin += String.fromCharCode(b);
  return `${prefix}${btoa(bin).replace(/\+/gu, "-").replace(/\//gu, "_").replace(/=+$/u, "")}`;
}

export const isAccessBearer = (token: string): boolean => token.startsWith(ACCESS_PREFIX) || token.startsWith(SERVICE_PREFIX);
const isRefreshToken = (token: string): boolean => token.startsWith(REFRESH_PREFIX);

/** Compares digests, so the time taken depends on neither value's length or content. */
export async function secretMatches(presented: string, secrets: readonly string[]): Promise<boolean> {
  const want = await Promise.all(secrets.map(hashToken));
  const got = await hashToken(presented);
  let matched = false;
  for (const candidate of want) {
    let diff = 0;
    for (let i = 0; i < 64; i += 1) diff |= candidate.charCodeAt(i) ^ got.charCodeAt(i);
    matched = matched || diff === 0;
  }
  return matched;
}

/** scheme://host[:port], lowercased, default port dropped; null for anything else. */
export function normalizedOrigin(value: string): string | null {
  try {
    const url = new URL(value.trim());
    if ((url.protocol !== "http:" && url.protocol !== "https:") || url.username || url.password) return null;
    return url.origin;
  } catch {
    return null;
  }
}

class RouteError extends Error {
  constructor(readonly status: 400 | 401 | 403 | 503, readonly code: string, message: string) {
    super(message);
  }
}
const bad = (message: string) => new RouteError(400, "bad_request", message);

function bearer(c: Context): string | null {
  return /^Bearer\s+(\S+)$/iu.exec(c.req.header("authorization") ?? "")?.[1] ?? null;
}

async function jsonBody(c: Context): Promise<Record<string, unknown>> {
  let value: unknown;
  try {
    value = JSON.parse(await c.req.text());
  } catch {
    throw bad("body must be a JSON object");
  }
  if (!value || typeof value !== "object" || Array.isArray(value)) throw bad("body must be a JSON object");
  return value as Record<string, unknown>;
}

function onlyFields(body: Record<string, unknown>, allowed: readonly string[]): void {
  for (const key of Object.keys(body)) if (!allowed.includes(key)) throw bad("body has an unknown field");
}

function idField(body: Record<string, unknown>, name: string): string {
  const value = body[name];
  if (typeof value !== "string" || !ID_SHAPE.test(value)) throw bad(`${name} must be a valid id`);
  return value;
}

function labelField(body: Record<string, unknown>): string | null {
  const label = body.label;
  if (label === undefined) return null;
  // eslint-disable-next-line no-control-regex
  if (typeof label !== "string" || label.length < 1 || label.length > LABEL_MAX_CHARS || /[\u0000-\u001f\u007f]/u.test(label))
    throw bad(`label must be 1 to ${LABEL_MAX_CHARS} printable characters`);
  return label;
}

/** Scope is fixed: exactly ["rth:read"], never widened or narrowed by a caller. */
function requireReadScope(body: Record<string, unknown>): void {
  const scopes = body.scopes;
  if (!Array.isArray(scopes) || scopes.length !== 1 || scopes[0] !== READ_SCOPE) throw bad(`scopes must be exactly ["${READ_SCOPE}"]`);
}

function requireAudience(body: Record<string, unknown>, deps: SelfIssuedDeps): string {
  const audience = typeof body.audience === "string" ? normalizedOrigin(body.audience) : null;
  if (!audience) throw bad("audience must be an origin");
  if (audience !== deps.audience) throw new RouteError(403, "audience_mismatch", "audience is not this service");
  return audience;
}

/** Registers the token routes; call before the /v1/* bearer middleware. */
export function registerSelfIssuedRoutes(app: Hono<any>, deps: SelfIssuedDeps, log: (fields: Record<string, unknown>) => void): void {
  const nowDate = () => new Date((deps.now ?? Date.now)());
  const limit = bodyLimit({ maxSize: INTERNAL_MAX_BODY_BYTES, onError: (c) => c.json({ error: { code: "body_too_large", message: "body is too large" }, correlationId: c.get("correlationId") }, 413) });

  const answer = async (c: Context, work: () => Promise<Response>): Promise<Response> => {
    try {
      return await work();
    } catch (error) {
      const e = error instanceof RouteError ? error : new RouteError(503, "token_store_unavailable", "the token store is unavailable");
      if (e.status === 503) c.header("Retry-After", "1");
      log({ route: c.req.routePath, reason: "token_route_refused", code: e.code, status: e.status, correlation_id: c.get("correlationId") });
      return c.json({ error: { code: e.code, message: e.message }, correlationId: c.get("correlationId") }, e.status);
    }
  };

  const internal = (handler: (c: Context, body: Record<string, unknown>) => Promise<Response>) => async (c: Context) =>
    answer(c, async () => {
      const presented = bearer(c);
      if (!presented || !(await secretMatches(presented, deps.internalSecrets)))
        throw new RouteError(401, "invalid_token", "token is not valid");
      return await handler(c, await jsonBody(c));
    });

  const pairReply = (
    row: Pick<TokenRow, "id" | "userId" | "orgId" | "workspaceId" | "audience">,
    tokens: { accessToken: string; accessExpires: Date; refreshToken: string; refreshExpires: Date },
  ) => ({
    sessionId: row.id,
    userId: row.userId,
    orgId: row.orgId,
    workspaceId: row.workspaceId,
    audience: row.audience,
    scopes: [...READ_SCOPES],
    accessToken: tokens.accessToken,
    accessTokenExpiresAt: tokens.accessExpires.toISOString(),
    refreshToken: tokens.refreshToken,
    refreshTokenExpiresAt: tokens.refreshExpires.toISOString(),
    tokenType: "Bearer" as const,
  });

  const lifetimes = (now: Date) => ({
    accessExpires: new Date(now.getTime() + deps.accessTtlSeconds * 1000),
    refreshExpires: new Date(now.getTime() + REFRESH_TTL_SECONDS * 1000),
  });

  app.post(SESSIONS_PATH, limit, internal(async (c, body) => {
    onlyFields(body, ["userId", "orgId", "workspaceId", "audience", "scopes", "label"]);
    const identity = { userId: idField(body, "userId"), orgId: idField(body, "orgId"), workspaceId: idField(body, "workspaceId") };
    const audience = requireAudience(body, deps);
    requireReadScope(body);
    const label = labelField(body);
    const now = nowDate();
    const id = crypto.randomUUID();
    const accessToken = generateToken(ACCESS_PREFIX);
    const refreshToken = generateToken(REFRESH_PREFIX);
    const { accessExpires, refreshExpires } = lifetimes(now);
    await deps.store.issue({
      id, family: crypto.randomUUID(), subjectType: "cli", audience, ...identity,
      accessHash: await hashToken(accessToken), accessExpires, refreshHash: await hashToken(refreshToken), refreshExpires, label, now,
    });
    log({ route: SESSIONS_PATH, reason: "session_issued", session_id: id, workspace_id: identity.workspaceId, correlation_id: c.get("correlationId") });
    return c.json(pairReply({ id, audience, ...identity }, { accessToken, accessExpires, refreshToken, refreshExpires }), 201);
  }));

  app.post(REFRESH_PATH, limit, internal(async (c, body) => {
    onlyFields(body, ["refreshToken", "userId", "orgId", "workspaceId", "audience", "scopes", "label"]);
    const identity = { userId: idField(body, "userId"), orgId: idField(body, "orgId"), workspaceId: idField(body, "workspaceId") };
    const audience = requireAudience(body, deps);
    requireReadScope(body);
    labelField(body);
    const presented = body.refreshToken;
    if (typeof presented !== "string" || presented.length > TOKEN_MAX_CHARS) throw bad("refreshToken must be a string");
    const refused = new RouteError(401, "invalid_token", "refresh token is not valid");
    if (!isRefreshToken(presented)) throw refused;
    const now = nowDate();
    const accessToken = generateToken(ACCESS_PREFIX);
    const refreshToken = generateToken(REFRESH_PREFIX);
    const { accessExpires, refreshExpires } = lifetimes(now);
    const row = await deps.store.refresh({
      refreshHash: await hashToken(presented), ...identity, audience,
      nextAccessHash: await hashToken(accessToken), accessExpires, nextRefreshHash: await hashToken(refreshToken), refreshExpires, now,
    });
    if (!row) throw refused;
    log({ route: REFRESH_PATH, reason: "session_refreshed", session_id: row.id, workspace_id: row.workspaceId, correlation_id: c.get("correlationId") });
    return c.json(pairReply(row, { accessToken, accessExpires, refreshToken, refreshExpires }));
  }));

  app.post(SERVICE_TOKENS_PATH, limit, internal(async (c, body) => {
    onlyFields(body, ["orgId", "workspaceId", "userId", "audience", "scopes", "label", "expiresInDays"]);
    const orgId = idField(body, "orgId");
    const workspaceId = idField(body, "workspaceId");
    const userId = body.userId === undefined ? "read-api-service" : idField(body, "userId");
    const audience = requireAudience(body, deps);
    requireReadScope(body);
    const label = labelField(body);
    if (label === null) throw bad("label is required");
    const days = body.expiresInDays === undefined ? SERVICE_TTL_DAYS_DEFAULT : body.expiresInDays;
    if (typeof days !== "number" || !Number.isInteger(days) || days < 1 || days > SERVICE_TTL_DAYS_MAX)
      throw bad(`expiresInDays must be an integer from 1 to ${SERVICE_TTL_DAYS_MAX}`);
    const now = nowDate();
    const id = crypto.randomUUID();
    const token = generateToken(SERVICE_PREFIX);
    const expires = new Date(now.getTime() + days * 86_400_000);
    await deps.store.issue({
      id, family: crypto.randomUUID(), subjectType: "service", audience, userId, orgId, workspaceId,
      accessHash: await hashToken(token), accessExpires: expires,
      // A value nobody holds: a service token has no refresh path.
      refreshHash: await hashToken(generateToken(REFRESH_PREFIX)), refreshExpires: expires, label, now,
    });
    log({ route: SERVICE_TOKENS_PATH, reason: "service_token_issued", session_id: id, workspace_id: workspaceId, correlation_id: c.get("correlationId") });
    return c.json({ id, token, label, userId, orgId, workspaceId, audience, scopes: [...READ_SCOPES], expiresAt: expires.toISOString(), tokenType: "Bearer" }, 201);
  }));

  app.post(INTERNAL_REVOKE_PATH, limit, internal(async (c, body) => {
    onlyFields(body, ["orgId", "id", "userId", "workspaceId"]);
    const orgId = idField(body, "orgId");
    const optional = (name: string) => (body[name] === undefined ? undefined : idField(body, name));
    const filter = { id: optional("id"), userId: optional("userId"), workspaceId: optional("workspaceId") };
    const revoked = await deps.store.deleteFor(orgId, filter);
    log({ route: INTERNAL_REVOKE_PATH, reason: "tokens_revoked", revoked, correlation_id: c.get("correlationId") });
    return c.json({ revoked });
  }));

  // A token holder ends its own session. Possession is the authorization.
  app.post(CLIENT_REVOKE_PATH, limit, (c) =>
    answer(c, async () => {
      const text = await c.req.text();
      let token = bearer(c);
      if (text.trim()) {
        const body = await (async () => {
          let value: unknown;
          try {
            value = JSON.parse(text);
          } catch {
            throw bad("body must be a JSON object");
          }
          if (!value || typeof value !== "object" || Array.isArray(value)) throw bad("body must be a JSON object");
          return value as Record<string, unknown>;
        })();
        onlyFields(body, ["token"]);
        if (body.token !== undefined) {
          if (typeof body.token !== "string" || body.token.length > TOKEN_MAX_CHARS) throw bad("token must be a string");
          token = body.token;
        }
      }
      if (!token) throw bad("token is required");
      const kind = isRefreshToken(token) ? "refresh" : isAccessBearer(token) ? "access" : null;
      // A token of another family was never stored here.
      if (!kind) return c.json({ revoked: false });
      return c.json({ revoked: await deps.store.deleteByHash(kind, await hashToken(token)) });
    }));
}

export interface TokenQuery {
  query(text: string, params?: unknown[]): Promise<{ rows: unknown[] }>;
}

export interface TokenPool extends TokenQuery {
  connect(): Promise<TokenQuery & { release(): void }>;
}

const ROW_COLUMNS = "id, subject_type, audience, user_id, org_id, workspace_id, scopes";

function tokenRow(raw: unknown): TokenRow {
  const r = raw as Record<string, unknown>;
  return {
    id: String(r.id),
    subjectType: r.subject_type === "service" ? "service" : "cli",
    audience: String(r.audience),
    userId: String(r.user_id),
    orgId: String(r.org_id),
    workspaceId: String(r.workspace_id),
    scopes: Array.isArray(r.scopes) ? r.scopes.map(String) : [],
  };
}

/**
 * The store over the read role's pool. Reads run as one statement; the writes run in an explicit
 * READ WRITE transaction, because the role's default is read-only.
 */
export function sqlTokenStore(pool: TokenPool): TokenStore {
  async function write<T>(work: (q: TokenQuery) => Promise<T>): Promise<T> {
    const client = await pool.connect();
    try {
      await client.query("BEGIN READ WRITE");
      const result = await work(client);
      await client.query("COMMIT");
      return result;
    } catch (error) {
      await client.query("ROLLBACK").catch(() => {});
      throw error;
    } finally {
      client.release();
    }
  }
  return {
    async verify(accessHash, now) {
      const { rows } = await pool.query(`SELECT ${ROW_COLUMNS} FROM sessions.read_api_token_verify($1, $2)`, [accessHash, now]);
      return rows.length === 1 ? tokenRow(rows[0]) : null;
    },
    issue: (row) =>
      write(async (q) => {
        await q.query("SELECT sessions.read_api_token_issue($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)", [
          row.id, row.family, row.subjectType, row.audience, row.userId, row.orgId, row.workspaceId,
          row.accessHash, row.accessExpires, row.refreshHash, row.refreshExpires, row.label, row.now,
        ]);
      }),
    refresh: (input) =>
      write(async (q) => {
        const { rows } = await q.query(`SELECT ${ROW_COLUMNS} FROM sessions.read_api_token_refresh($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`, [
          input.refreshHash, input.userId, input.orgId, input.workspaceId, input.audience,
          input.nextAccessHash, input.accessExpires, input.nextRefreshHash, input.refreshExpires, input.now,
        ]);
        return rows.length === 1 ? tokenRow(rows[0]) : null;
      }),
    deleteByHash: (kind, hash) =>
      write(async (q) => {
        const { rows } = await q.query("SELECT sessions.read_api_token_delete_by_hash($1, $2) AS deleted", [kind, hash]);
        return (rows[0] as { deleted?: boolean } | undefined)?.deleted === true;
      }),
    deleteFor: (orgId, filter) =>
      write(async (q) => {
        const { rows } = await q.query("SELECT sessions.read_api_token_delete_for($1, $2, $3, $4) AS deleted", [orgId, filter.id ?? null, filter.userId ?? null, filter.workspaceId ?? null]);
        return Number((rows[0] as { deleted?: number } | undefined)?.deleted ?? 0);
      }),
  };
}
