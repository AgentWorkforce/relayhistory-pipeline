/**
 * The read-API client for the Session Intelligence harvest (docs/session-intel-harvest.md). It reads only
 * the routes in HARVEST_ROUTES, never a coordination route (overlaps and advisories are detector output,
 * which the evaluation must not see). The bearer token comes only from SESSION_INTEL_READ_TOKEN in the
 * environment; it is sent in the Authorization header and nowhere else, and every message this module
 * builds is redacted against it. Redirects are never followed. Every answered response must attest the
 * workspace given by --workspace (X-Relayhistory-Workspace-Id), as the read API's other clients check it
 * (src/session-read-api/canonical-history-adapter.ts).
 */

export const TOKEN_ENV = "SESSION_INTEL_READ_TOKEN";
export const WORKSPACE_HEADER = "x-relayhistory-workspace-id";

/** An error whose message is safe to print: flags, bounds, statuses and reason codes, never a token, id, path or text. */
export class HarvestError extends Error {
  constructor(message: string) { super(`harvest: ${message}`); this.name = "HarvestError"; }
}
/** The request cap was reached. The harvest's state is kept, so a rerun resumes. */
export class RequestCapReached extends HarvestError {
  constructor(cap: number) { super(`the request cap (--max-requests ${cap}) was reached`); this.name = "RequestCapReached"; }
}

/** The only routes the harvest calls. A coordination route is refused even if a caller asks for it. */
export const HARVEST_ROUTES = ["/v1/sessions", "/v1/sessions/:sessionId/events", "/v1/sessions/output"] as const;
export type HarvestRoute = (typeof HARVEST_ROUTES)[number];

const LOOPBACK = new Set(["127.0.0.1", "localhost", "[::1]", "::1"]);

/**
 * The read API's base URL: https, or http for a loopback host only. No credentials, query or fragment in
 * it, so nothing secret can ride in the URL and every route is built from a fixed path. A path prefix is kept.
 */
export function parseReadApi(raw: string | undefined): URL {
  if (!raw) throw new HarvestError("--read-api is required (an https URL)");
  let url: URL;
  try { url = new URL(raw); } catch { throw new HarvestError("--read-api must be an absolute URL"); }
  if (url.username || url.password) throw new HarvestError("--read-api must not carry credentials; the token comes from the environment only");
  if (url.search || url.hash || raw.includes("?") || raw.includes("#")) throw new HarvestError("--read-api must have no query or fragment");
  if (url.protocol === "http:") {
    if (!LOOPBACK.has(url.hostname.toLowerCase())) throw new HarvestError("--read-api must be https (http is allowed for a loopback host only)");
  } else if (url.protocol !== "https:") throw new HarvestError("--read-api must be https (http is allowed for a loopback host only)");
  url.pathname = url.pathname.replace(/\/+$/u, "");
  return url;
}

/** The bearer token from the environment: 16 to 8192 visible characters, no whitespace. Never from argv or a file. */
export function readToken(env: Record<string, string | undefined>): string {
  const token = env[TOKEN_ENV];
  if (token === undefined || token === "") throw new HarvestError(`${TOKEN_ENV} is not set; inject it at runtime (for example with \`op run\`)`);
  if (token.length < 16 || token.length > 8192 || /[\s\u0000-\u001f\u007f]/u.test(token)) throw new HarvestError(`${TOKEN_ENV} is not a valid bearer token`);
  return token;
}

/** Replaces every occurrence of the token (and of its first half, in case a message cut it) with a marker. */
export function redact(text: string, token: string | undefined): string {
  if (!token) return text;
  let out = text.split(token).join("[redacted]");
  const half = token.slice(0, Math.ceil(token.length / 2));
  if (half.length >= 8) out = out.split(half).join("[redacted]");
  return out;
}

const SAFE_CODE = /^[A-Za-z0-9_.:-]{1,96}$/u;
/** A server-supplied code or header value, kept only when it is a short code, and redacted. */
const safeCode = (v: unknown, token: string): string => typeof v === "string" && SAFE_CODE.test(v) ? redact(v, token) : "other";

/** Seconds or an HTTP date, as milliseconds from now; null when absent or unreadable. */
export function retryAfterMs(value: string | null, now: number): number | null {
  if (value === null) return null;
  const v = value.trim();
  if (/^\d{1,9}$/u.test(v)) return Number(v) * 1_000;
  const at = Date.parse(v);
  return Number.isFinite(at) ? Math.max(0, at - now) : null;
}

export interface ClientOptions {
  base: URL;
  token: string;
  workspace: string;
  maxRequests: number;
  /** Requests already spent by earlier runs of the same harvest (the cap covers the whole harvest). */
  spent?: number;
  concurrency: number;
  fetch?: typeof fetch;
  sleep?: (ms: number) => Promise<void>;
  clock?: () => number;
  maxResponseBytes?: number;
  maxRetries?: number;
  /** A Retry-After longer than this fails the harvest rather than being cut short. */
  maxRetryAfterMs?: number;
  timeoutMs?: number;
}

export type Answer =
  | { kind: "ok"; body: unknown }
  | { kind: "unsupported"; reason: string }
  | { kind: "not_found" };

export interface ClientStats {
  requests: number;
  byRoute: Record<string, number>;
  retries: number;
  retryAfterHonoured: number;
  waitedMs: number;
  /** Header values seen on answered responses, counted: the backend, buffered and incomplete markers. */
  headers: Record<string, Record<string, number>>;
}

const RECORDED_HEADERS = ["x-sessions-read-backend", "x-relayhistory-buffered", "x-sessions-read-incomplete", "x-sessions-read-continuation"];
const RETRYABLE = new Set([429, 502, 503, 504]);

export class ReadApiClient {
  readonly stats: ClientStats;
  private active = 0;
  private readonly waiting: (() => void)[] = [];
  private readonly f: typeof fetch;
  private readonly sleep: (ms: number) => Promise<void>;
  private readonly clock: () => number;

  constructor(private readonly o: ClientOptions) {
    this.stats = { requests: o.spent ?? 0, byRoute: {}, retries: 0, retryAfterHonoured: 0, waitedMs: 0, headers: {} };
    this.f = o.fetch ?? fetch;
    this.sleep = o.sleep ?? ((ms) => new Promise((r) => setTimeout(r, ms)));
    this.clock = o.clock ?? Date.now;
  }

  private async slot<T>(fn: () => Promise<T>): Promise<T> {
    while (this.active >= this.o.concurrency) await new Promise<void>((r) => this.waiting.push(r));
    this.active++;
    try { return await fn(); } finally {
      this.active--;
      this.waiting.shift()?.();
    }
  }

  /** The URL for a route: the base path plus the route with its one path parameter encoded. */
  private url(route: HarvestRoute, sessionId: string | undefined, params: [string, string][]): URL {
    if (!(HARVEST_ROUTES as readonly string[]).includes(route)) throw new HarvestError("refused a route outside the harvest's list");
    const path = route === "/v1/sessions/:sessionId/events" ? `/v1/sessions/${encodeURIComponent(sessionId ?? "")}/events` : route;
    const url = new URL(this.o.base.href);
    const want = `${this.o.base.pathname}${path}`;
    url.pathname = want;
    // The URL parser resolves dot segments (a session id of `..` or `%2e%2e` would climb to another
    // route): the path must come out exactly as built. And whatever built it, never a coordination route.
    if (url.pathname !== want) throw new HarvestError("refused a session id that changes the request path");
    if (/\/v1\/coordination(?:\/|$)/u.test(url.pathname)) throw new HarvestError("refused a coordination route");
    url.searchParams.set("workspace", this.o.workspace);
    for (const [k, v] of params) url.searchParams.append(k, v);
    return url;
  }

  /** GET one route; retries 429/502/503/504 and transport errors, honouring Retry-After. Every attempt counts. */
  async get(route: HarvestRoute, params: [string, string][], sessionId?: string): Promise<Answer> {
    const url = this.url(route, sessionId, params);
    const maxRetries = this.o.maxRetries ?? 5;
    const maxWait = this.o.maxRetryAfterMs ?? 120_000;
    for (let attempt = 0; ; attempt++) {
      if (this.stats.requests >= this.o.maxRequests) throw new RequestCapReached(this.o.maxRequests);
      this.stats.requests++;
      this.stats.byRoute[route] = (this.stats.byRoute[route] ?? 0) + 1;
      let res: Response;
      try {
        res = await this.slot(() => this.f(url, {
          method: "GET",
          redirect: "manual",
          headers: { authorization: `Bearer ${this.o.token}`, accept: "application/json" },
          signal: AbortSignal.timeout(this.o.timeoutMs ?? 60_000),
        }));
      } catch {
        // Transport errors carry the URL and sometimes more; none of it is printed.
        if (attempt >= maxRetries) throw new HarvestError(`${route}: the read API could not be reached after ${attempt + 1} attempts`);
        await this.wait(Math.min(maxWait, 1_000 * 2 ** attempt), false);
        continue;
      }
      if (res.status >= 300 && res.status < 400) {
        await res.body?.cancel().catch(() => {});
        throw new HarvestError(`${route}: the read API answered a redirect (${res.status}); redirects are never followed`);
      }
      if (RETRYABLE.has(res.status)) {
        const after = retryAfterMs(res.headers.get("retry-after"), this.clock());
        await res.body?.cancel().catch(() => {});
        if (attempt >= maxRetries) throw new HarvestError(`${route}: the read API stayed unavailable (${res.status}) after ${attempt + 1} attempts`);
        if (after !== null && after > maxWait) throw new HarvestError(`${route}: the read API asked to wait ${Math.ceil(after / 1_000)}s (Retry-After), longer than ${maxWait / 1_000}s; rerun later to resume`);
        await this.wait(after ?? Math.min(maxWait, 1_000 * 2 ** attempt), after !== null);
        continue;
      }
      if (res.ok) {
        if (res.headers.get(WORKSPACE_HEADER) !== this.o.workspace) {
          await res.body?.cancel().catch(() => {});
          throw new HarvestError(`${route}: a response did not attest --workspace (${WORKSPACE_HEADER} differs or is missing); refused`);
        }
        for (const h of RECORDED_HEADERS) {
          const v = res.headers.get(h);
          if (v === null) continue;
          const seen = (this.stats.headers[h] ??= {});
          const k = safeCode(v, this.o.token);
          seen[k] = (seen[k] ?? 0) + 1;
        }
      }
      const body = await this.body(res, route);
      if (res.ok) return { kind: "ok", body };
      const err = (body && typeof body === "object" ? (body as { error?: { code?: unknown; reason?: unknown } }).error : undefined) ?? {};
      const code = safeCode(err.code, this.o.token);
      if (res.status === 501) return { kind: "unsupported", reason: safeCode(err.reason ?? err.code, this.o.token) };
      if (res.status === 404) return { kind: "not_found" };
      if (res.status === 401 || res.status === 403) throw new HarvestError(`${route}: the read API refused the credential (${res.status} ${code})`);
      throw new HarvestError(`${route}: the read API answered ${res.status} ${code}`);
    }
  }

  private async wait(ms: number, honoured: boolean) {
    this.stats.retries++;
    if (honoured) this.stats.retryAfterHonoured++;
    this.stats.waitedMs += ms;
    await this.sleep(ms);
  }

  /** The JSON body, read up to maxResponseBytes (default 32 MiB) without trusting Content-Length. */
  private async body(res: Response, route: string): Promise<unknown> {
    const max = this.o.maxResponseBytes ?? 32 * 1024 * 1024;
    const declared = Number(res.headers.get("content-length"));
    if (Number.isFinite(declared) && declared > max) {
      await res.body?.cancel().catch(() => {});
      throw new HarvestError(`${route}: a response exceeds ${max} bytes`);
    }
    if (!res.body) return null;
    const reader = res.body.getReader();
    const parts: Uint8Array[] = [];
    let size = 0;
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      size += value.byteLength;
      if (size > max) { await reader.cancel().catch(() => {}); throw new HarvestError(`${route}: a response exceeds ${max} bytes`); }
      parts.push(value);
    }
    if (size === 0) return null;
    const text = Buffer.concat(parts).toString("utf8");
    try { return JSON.parse(text); } catch {
      if (res.ok) throw new HarvestError(`${route}: a response is not JSON`);
      return null;
    }
  }
}
