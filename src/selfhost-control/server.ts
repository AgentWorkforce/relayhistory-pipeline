/**
 * SERVICE=selfhost-control: a single-user stand-in for relaycontrol's token
 * introspection (POST /internal/v1/auth/introspect, contract v1.2,
 * packages/pipeline/src/auth/relaycontrol.ts), for one developer running the
 * self-host pipeline without relaycontrol. It knows exactly one user (sub, org,
 * workspace) and that user's opaque `sh_at_` tokens, read once at startup from
 * SELFHOST_CONTROL_TOKENS_FILE. Nothing else about relaycontrol is served.
 * Default off: it refuses to start (exit 2) unless RELAYHISTORY_SELFHOST_CONTROL
 * is exactly "true". docs/selfhost-composition.md "Local single-user control".
 *
 * Replies: an active token gets the same fields relaycontrol (and the e2e stub)
 * send, with iat = now - 10 s and exp = now + 1 h computed per request (the
 * client refuses an expired `exp`; the tokens themselves never expire here, so
 * removing one from the file and restarting is how it is revoked). An unknown
 * token is 200 {active:false, reason:"unknown"}; a workspace the caller names
 * that is not the user's is 200 {active:false, reason:"workspace_mismatch"}.
 * A wrong or missing introspection bearer is 401 {}; a body over 16 KiB, not
 * JSON or not the contract shape is 400; other paths 404, other methods 405.
 * The bearer and tokens are compared as sha256 digests with timingSafeEqual.
 * No token, secret or request body is ever logged or echoed (aud excepted, as
 * the contract requires); one log line per request carries method, a fixed
 * path label and the status.
 */
import { createHash, timingSafeEqual } from "node:crypto";
import { closeSync, fstatSync, openSync, readFileSync } from "node:fs";
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";
import { isAbsolute } from "node:path";
import { initScrubEngine } from "@rhc/lib/scrub-engine/index.ts";
import { log } from "../shared/log.ts";

export const SELFHOST_CONTROL_FLAG = "RELAYHISTORY_SELFHOST_CONTROL";
export const INTROSPECT_SECRET_VAR = "SELFHOST_CONTROL_INTROSPECT_SECRET";
export const TOKENS_FILE_VAR = "SELFHOST_CONTROL_TOKENS_FILE";
export const LISTEN_VAR = "SELFHOST_CONTROL_LISTEN";
export const DEFAULT_LISTEN = "0.0.0.0:8090";
export const INTROSPECT_PATH = "/internal/v1/auth/introspect";
export const HEALTH_PATH = "/healthz";
/**
 * Request bodies larger than this are 400, never buffered past it. The client
 * sends bearers of up to 4096 characters, which as UTF-8 JSON (a non-ASCII
 * Latin-1 character is two bytes) is up to about 8.2 KB; the cap leaves room.
 */
export const MAX_BODY_BYTES = 16 * 1024;
export const MIN_SECRET_LENGTH = 32;
export const MIN_TOKEN_LENGTH = 32;
/** The read API and session-ingest only introspect bearers with this prefix. */
export const TOKEN_PREFIX = "sh_at_";
const MAX_TOKENS_FILE_BYTES = 64 * 1024;
const ACCESS_TTL_SECONDS = 3600;
const IAT_SKEW_SECONDS = 10;
/** relaycontrol.ts isWorkspaceId: the workspace the read API may name in a request. */
const WORKSPACE_SHAPE = /^[A-Za-z0-9][A-Za-z0-9_.:-]{0,255}$/;

/** A refused configuration: the message names a variable, never a value. */
export class SelfhostControlConfigError extends Error {
  constructor(message: string) {
    super(`96bulls: selfhost-control: ${message}`);
    this.name = "SelfhostControlConfigError";
  }
}

export interface SelfhostControlToken {
  token: string;
  token_id: string;
  scopes: string[];
}

export interface SelfhostControlUser {
  sub: string;
  org: string;
  workspace: string;
  tokens: SelfhostControlToken[];
}

export interface SelfhostControlConfig {
  /** The bearer the read API presents (its RELAYCONTROL_INTROSPECT_SECRET). */
  secret: string;
  user: SelfhostControlUser;
  host: string;
  port: number;
  /** One call per request; defaults to one log.ts line (route and status only). */
  logRequest?: (route: string, status: number) => void;
  /** Epoch ms. */
  now?: () => number;
}

export interface SelfhostControlServer {
  host: string;
  port: number;
  close(): Promise<void>;
}

const boundedId = (v: unknown): v is string => typeof v === "string" && v.length > 0 && v.length <= 256;

/** Validates the tokens file's parsed JSON; errors name the variable and the field, never a value. */
export function parseSelfhostControlUser(parsed: unknown): SelfhostControlUser {
  const bad = (what: string) => new SelfhostControlConfigError(`${TOKENS_FILE_VAR}: ${what}`);
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) throw bad("must be a JSON object");
  const o = parsed as Record<string, unknown>;
  for (const key of Object.keys(o))
    if (!["sub", "org", "workspace", "tokens"].includes(key)) throw bad("has an unknown top-level field");
  if (!boundedId(o.sub)) throw bad("sub must be a string of 1 to 256 characters");
  if (!boundedId(o.org)) throw bad("org must be a string of 1 to 256 characters");
  if (typeof o.workspace !== "string" || !WORKSPACE_SHAPE.test(o.workspace))
    throw bad("workspace must be a workspace id (letters, digits, _ . : -, at most 256)");
  if (!Array.isArray(o.tokens) || o.tokens.length === 0) throw bad("tokens must be a non-empty array");
  const seenTokens = new Set<string>();
  const seenIds = new Set<string>();
  const tokens = o.tokens.map((entry, i): SelfhostControlToken => {
    const at = `tokens[${i}]`;
    if (!entry || typeof entry !== "object" || Array.isArray(entry)) throw bad(`${at} must be an object`);
    const t = entry as Record<string, unknown>;
    for (const key of Object.keys(t))
      if (!["token", "token_id", "scopes"].includes(key)) throw bad(`${at} has an unknown field`);
    if (typeof t.token !== "string" || t.token.length < MIN_TOKEN_LENGTH || t.token.length > 1024)
      throw bad(`${at}.token must be ${MIN_TOKEN_LENGTH} to 1024 characters`);
    if (!t.token.startsWith(TOKEN_PREFIX) || /\s/.test(t.token))
      throw bad(`${at}.token must start with ${TOKEN_PREFIX} and contain no whitespace`);
    if (!boundedId(t.token_id)) throw bad(`${at}.token_id must be a string of 1 to 256 characters`);
    if (!Array.isArray(t.scopes) || !t.scopes.every((s) => typeof s === "string" && s.length > 0 && s.length <= 64))
      throw bad(`${at}.scopes must be an array of non-empty strings`);
    if (seenTokens.has(t.token)) throw bad(`${at}.token repeats an earlier token`);
    if (seenIds.has(t.token_id)) throw bad(`${at}.token_id repeats an earlier token_id`);
    seenTokens.add(t.token);
    seenIds.add(t.token_id);
    return { token: t.token, token_id: t.token_id, scopes: [...(t.scopes as string[])] };
  });
  return { sub: o.sub, org: o.org, workspace: o.workspace, tokens };
}

/** Trimmed as the read API's client trims it; one printable-ASCII word, since it travels as `Bearer <secret>`. */
function checkedSecret(raw: string | undefined): string {
  const secret = raw?.trim() ?? "";
  if (secret.length < MIN_SECRET_LENGTH)
    throw new SelfhostControlConfigError(`${INTROSPECT_SECRET_VAR} is required, at least ${MIN_SECRET_LENGTH} characters`);
  if (!/^[\x21-\x7e]+$/.test(secret))
    throw new SelfhostControlConfigError(`${INTROSPECT_SECRET_VAR} must be printable ASCII with no spaces`);
  return secret;
}

function parseListen(raw: string): { host: string; port: number } {
  const bad = () => new SelfhostControlConfigError(`${LISTEN_VAR} must be host:port with a port of 1 to 65535`);
  const m = /^(\[[0-9A-Fa-f:.]+\]|[A-Za-z0-9.-]+):([0-9]{1,5})$/.exec(raw);
  if (!m) throw bad();
  const port = Number(m[2]);
  if (!Number.isInteger(port) || port < 1 || port > 65535) throw bad();
  return { host: m[1]!.replace(/^\[(.*)\]$/, "$1"), port };
}

/**
 * The service configuration from the environment. Refuses (throws
 * SelfhostControlConfigError) unless the flag is exactly "true" and every
 * setting is valid; the tokens file is read once, here.
 */
export function loadSelfhostControlConfig(env: NodeJS.ProcessEnv): SelfhostControlConfig {
  if (env[SELFHOST_CONTROL_FLAG] !== "true")
    throw new SelfhostControlConfigError(`disabled: ${SELFHOST_CONTROL_FLAG} must be exactly "true" to start`);
  const secret = checkedSecret(env[INTROSPECT_SECRET_VAR]);
  const file = env[TOKENS_FILE_VAR] ?? "";
  if (!file || !isAbsolute(file)) throw new SelfhostControlConfigError(`${TOKENS_FILE_VAR} must be an absolute path`);
  // One descriptor for the checks and the read, so the file checked is the file read.
  let fd: number;
  try {
    fd = openSync(file, "r");
  } catch {
    throw new SelfhostControlConfigError(`${TOKENS_FILE_VAR} cannot be read`);
  }
  let text: string;
  try {
    const st = fstatSync(fd);
    if (!st.isFile()) throw new SelfhostControlConfigError(`${TOKENS_FILE_VAR} must be a regular file`);
    if ((st.mode & 0o077) !== 0)
      throw new SelfhostControlConfigError(`${TOKENS_FILE_VAR} must not be accessible by group or others (chmod 600)`);
    if (st.size > MAX_TOKENS_FILE_BYTES)
      throw new SelfhostControlConfigError(`${TOKENS_FILE_VAR} is larger than ${MAX_TOKENS_FILE_BYTES} bytes`);
    try {
      text = readFileSync(fd, "utf8");
    } catch {
      throw new SelfhostControlConfigError(`${TOKENS_FILE_VAR} cannot be read`);
    }
  } finally {
    closeSync(fd);
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    throw new SelfhostControlConfigError(`${TOKENS_FILE_VAR} cannot be read as JSON`);
  }
  const user = parseSelfhostControlUser(parsed);
  const listen = env[LISTEN_VAR] === undefined || env[LISTEN_VAR] === "" ? DEFAULT_LISTEN : env[LISTEN_VAR]!;
  return { secret, user, ...parseListen(listen) };
}

const digest = (value: string) => createHash("sha256").update(value, "utf8").digest();
/** Constant time in the compared value: both sides are 32-byte sha256 digests. */
const sameDigest = (a: Buffer, b: Buffer) => timingSafeEqual(a, b);

const KNOWN_METHODS = new Set(["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"]);
const routeLabel = (req: IncomingMessage, path: string) =>
  `${KNOWN_METHODS.has(req.method ?? "") ? req.method : "OTHER"} ${
    path === INTROSPECT_PATH || path === HEALTH_PATH ? path : "other"
  }`;

/** Starts the HTTP server; resolves once it is listening. */
export async function startSelfhostControl(config: SelfhostControlConfig): Promise<SelfhostControlServer> {
  const user = parseSelfhostControlUser(config.user);
  const secret = checkedSecret(config.secret);
  const secretDigest = digest(secret);
  const tokenDigests = user.tokens.map((t) => ({ digest: digest(t.token), record: t }));
  const now = config.now ?? (() => Date.now());
  const logRequest =
    config.logRequest ?? ((route: string, status: number) => log.info("selfhost-control request", { route, status }));

  const handle = (req: IncomingMessage, res: ServerResponse) => {
    const path = (req.url ?? "").split("?")[0]!;
    let logged = false;
    const send = (status: number, body: unknown, close = false, headers: Record<string, string> = {}) => {
      if (res.headersSent) return;
      const text = JSON.stringify(body);
      res.writeHead(status, {
        "content-type": "application/json",
        "cache-control": "no-store",
        "content-length": String(Buffer.byteLength(text)),
        ...(close ? { connection: "close" } : {}),
        ...headers,
      });
      res.end(text);
      if (close) res.once("finish", () => req.socket.destroy());
      if (!logged) {
        logged = true;
        logRequest(routeLabel(req, path), status);
      }
    };

    if (path === HEALTH_PATH) {
      if (req.method !== "GET" && req.method !== "HEAD") return send(405, { error: "method_not_allowed" }, true, { allow: "GET, HEAD" });
      return send(200, { ok: true }, true);
    }
    if (path !== INTROSPECT_PATH) return send(404, { error: "not_found" }, true);
    if (req.method !== "POST") return send(405, { error: "method_not_allowed" }, true, { allow: "POST" });

    // The introspection secret before any body is read.
    const auth = req.headers.authorization;
    const m = typeof auth === "string" ? /^Bearer ([\x21-\x7e]+)$/.exec(auth) : null;
    if (!m || !sameDigest(digest(m[1]!), secretDigest)) return send(401, {}, true);

    const declared = Number(req.headers["content-length"]);
    if (Number.isFinite(declared) && declared > MAX_BODY_BYTES) return send(400, { error: "bad_request" }, true);

    const chunks: Buffer[] = [];
    let total = 0;
    let done = false;
    req.on("data", (chunk: Buffer) => {
      if (done) return;
      total += chunk.byteLength;
      if (total > MAX_BODY_BYTES) {
        done = true;
        chunks.length = 0;
        send(400, { error: "bad_request" }, true);
        return;
      }
      chunks.push(chunk);
    });
    req.on("error", () => {
      done = true;
      req.socket.destroy();
    });
    req.on("end", () => {
      if (done) return;
      done = true;
      let input: Record<string, unknown>;
      try {
        const parsed: unknown = JSON.parse(Buffer.concat(chunks).toString("utf8"));
        if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) throw new Error("not an object");
        input = parsed as Record<string, unknown>;
      } catch {
        return send(400, { error: "bad_request" });
      }
      const { token, aud, workspace, machine } = input;
      if (
        typeof token !== "string" || token.length === 0 || token.length > MAX_BODY_BYTES ||
        !boundedId(aud) ||
        !(workspace === undefined || typeof workspace === "string") ||
        !(machine === undefined || typeof machine === "string")
      )
        return send(400, { error: "bad_request" });

      // Every configured token is compared, so the time taken does not depend on which matched.
      const presented = digest(token);
      let record: SelfhostControlToken | undefined;
      for (const t of tokenDigests) if (sameDigest(presented, t.digest) && !record) record = t.record;
      if (!record) return send(200, { active: false, reason: "unknown" });
      if (workspace !== undefined && workspace !== user.workspace)
        return send(200, { active: false, reason: "workspace_mismatch" });
      const nowSeconds = Math.floor(now() / 1000);
      send(200, {
        active: true,
        sub: user.sub,
        email: null,
        principal_type: "desktop-user",
        machine_id: null,
        org: user.org,
        workspace: user.workspace,
        scopes: [...record.scopes],
        aud,
        iat: nowSeconds - IAT_SKEW_SECONDS,
        exp: nowSeconds + ACCESS_TTL_SECONDS,
        token_id: record.token_id,
      });
    });
  };

  const server = createServer(handle);
  server.requestTimeout = 10_000;
  server.headersTimeout = 10_000;
  server.on("clientError", (_err, socket) => socket.destroy());
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(config.port, config.host, () => {
      server.off("error", reject);
      resolve();
    });
  });
  const address = server.address() as AddressInfo;
  return {
    host: config.host,
    port: address.port,
    close: () =>
      new Promise<void>((resolve) => {
        server.closeAllConnections?.();
        server.close(() => resolve());
      }),
  };
}

if (import.meta.main) {
  process.env.OTEL_SERVICE_NAME ||= "selfhost-control";
  let config: SelfhostControlConfig;
  try {
    config = loadSelfhostControlConfig(process.env);
  } catch (error) {
    const message = error instanceof SelfhostControlConfigError ? error.message : "96bulls: selfhost-control: refused to start";
    process.stderr.write(`${message}\n`);
    process.exit(2);
  }
  // SCRUB_ENGINE (docs/scrub-engine.md): every service selects (and for wasm verifies) at startup.
  await initScrubEngine();
  let server: SelfhostControlServer;
  try {
    server = await startSelfhostControl(config);
  } catch (error) {
    // e.g. EADDRINUSE: the code only, never a message that could carry configuration.
    const code = (error as { code?: unknown } | null)?.code;
    process.stderr.write(`96bulls: selfhost-control: cannot listen on ${LISTEN_VAR} (${typeof code === "string" ? code : "error"})\n`);
    process.exit(1);
  }
  log.info("selfhost-control listening", { port: server.port });
  const stop = () => void server.close().then(() => process.exit(0));
  process.on("SIGTERM", stop);
  process.on("SIGINT", stop);
}
