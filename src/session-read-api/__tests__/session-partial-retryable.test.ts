/**
 * READ_API_SESSION_PARTIAL_RETRYABLE (default off, self-host service only): the first page of a
 * session-scoped read of a partial session answers a retryable 503 `backend_unavailable`, reason
 * `session_partial`, `Retry-After: 60`, instead of the 501. Off: the 501, byte for byte. Workspace
 * search, `session_partial_unbounded` and the canonical cursor-page recheck keep the 501 either way.
 * The parse is strict and never echoes the value; the hosted Lambda refuses to start with it on and
 * the read Worker refuses every request. No database is touched: ClickHouse is a stub, pg.Pool a recorder.
 */
import { afterEach, beforeEach, describe, expect, spyOn, test } from "bun:test";
import pg from "pg";
import type { ClickHouseClient } from "../../session-pipeline/vendor/search-index/clickhouse.ts";
import type { SearchServiceBinding } from "../../session-pipeline/search-adapter.ts";
import type { SearchIndexAdapter } from "@rhc/search-index/contracts.ts";
import { AuthError, createReadApi, type ReadApiDeps, type VerifiedIdentity } from "../app.ts";
import { CANONICAL_CURSOR_PREFIX } from "../canonical-events.ts";
import { READ_API_SESSION_PARTIAL_RETRYABLE, sessionPartialRetryableFlag } from "../config.ts";
import { createReadApiLambdaHandler, type FunctionUrlEvent, READER_LOGIN_SQL, readApiDepsFromSecret, sessionPartialRetryable } from "../lambda.ts";
import type { Liveness } from "../page.ts";
import { PARTIAL_SESSIONS_CAP } from "../queries.ts";
import { selfhostSearchComposition } from "../server.ts";
import { handle, type Env } from "../../../packages/session-read-worker/src/index.ts";
import { READER_LOGIN_ROW, readerLoginAnswer } from "./reader-login-support.ts";
import { memoryLiveness } from "./support.ts";

const INVALID = `session read api: ${READ_API_SESSION_PARTIAL_RETRYABLE} must be unset, "", "true" or "false"`;
const SERVICE_ONLY = `session read api: ${READ_API_SESSION_PARTIAL_RETRYABLE} is served only by the self-host service`;
const BAD_VALUES = ["1", "TRUE", "True", "true ", " true", "yes", "on", "s3cret-value"];
const env = (value?: string) => (value === undefined ? {} : { [READ_API_SESSION_PARTIAL_RETRYABLE]: value }) as NodeJS.ProcessEnv;
const body = async (r: Response) => (await r.json()) as Record<string, any>;

const ID: VerifiedIdentity = { userId: "u-1", orgId: "org-a", workspaceId: "ws-a", scopes: ["rth:read"] };

/** A ClickHouse stub that answers the partial-session checks only (as app.test.ts stubCh). */
function stubCh(partial: string[], partialInWorkspace: string[] = []) {
  const ch: ClickHouseClient = {
    database: "relay_search",
    async command() {},
    async insert() {},
    async select<T>(query: string, options?: { params?: Record<string, unknown> }) {
      if (query.includes("in_workspace")) {
        const inWs = new Set(partialInWorkspace);
        const page = Number(options?.params?.page ?? PARTIAL_SESSIONS_CAP);
        return [...partial].sort().slice(0, page).map((session_id) => ({ session_id, in_workspace: inWs.has(session_id) ? 1 : 0 })) as T[];
      }
      if (query.includes("search_ingest_session_state")) {
        const session = options?.params?.session;
        return partial.filter((id) => session === undefined || id === session).map((session_id) => ({ session_id })) as T[];
      }
      return [] as T[];
    },
  };
  return ch;
}

function api(
  partial: string[], on: boolean, partialInWorkspace: string[] = [], cors?: ReadonlySet<string>,
  history: { completeSinceMs: number; createdAt: Record<string, number> } = { completeSinceMs: 0, createdAt: { "codex\u0000s-1": Date.parse("2026-10-01T00:00:00Z") } },
) {
  const logs: Record<string, unknown>[] = [];
  const app = createReadApi({
    verify: async (t) => {
      if (t !== "good") throw new AuthError(401, "invalid_token", "token is not valid");
      return ID;
    },
    ch: stubCh(partial, partialInWorkspace),
    liveness: memoryLiveness({ createdAt: history.createdAt }).liveness,
    completeSinceMs: history.completeSinceMs,
    ...(on ? { sessionPartialRetryable: true } : {}),
    ...(cors ? { corsAllowedOrigins: cors } : {}),
    log: (f) => logs.push(f),
  });
  const get = (path: string, origin?: string) => app.request(path, { headers: { authorization: "Bearer good", ...(origin ? { origin } : {}) } });
  return { get, logs };
}

/** The canonical (Locker) events fixture of canonical-events.test.ts, trimmed. */
const tenant = { orgId: "org-a", workspaceId: "ws-a" };
const floor = Date.parse("2026-11-08T22:02:00Z");
const row = (n: number) => ({ table: "convergence_events", row: { session_id: "old", source: "codex", event_id: `original-${n}`, machine_id: "machine", kind: "history", type: n === 1 ? "user" : "assistant", ts: `2026-11-07T10:00:0${n}.000Z`, content: `actual-old-message-${n}`, user_id: "u" } });
function canonical(on: boolean) {
  const state = { async fromMs() { return floor; }, async record() {}, async partials(): Promise<{ sessions: string[]; overflow: boolean }> { return { sessions: [], overflow: false }; } };
  const adapter: SearchServiceBinding = { index: { backend: "postgres", async query() { return { hits: [] }; } } as unknown as SearchIndexAdapter, state };
  const liveness: Liveness = { async erased() { return new Set(); }, async sessionStart() { return { pointers: 1, live: 1, createdAtMs: floor - 86400000 }; }, async livePointerSessions(_o, _w, ids) { return new Set(ids); } };
  const deps: ReadApiDeps = {
    verify: async () => ({ ...tenant, userId: "u", scopes: ["rth:read"] }), adapter, liveness, completeSinceMs: floor,
    selfhost: { login: async () => { throw Error("not used"); } }, directoryWorkspaces: new Set(["ws-a"]),
    canonicalEvents: { source: async () => ({ rows: [row(2), row(1)], legacyCoverage: null, exists: true, generation: "a".repeat(64) }) },
    ...(on ? { sessionPartialRetryable: true } : {}),
  };
  const get = (suffix = "") => createReadApi(deps).request(`/v1/sessions/old/events?workspace=ws-a&source=codex${suffix}`, { headers: { authorization: "Bearer synthetic" } });
  return { state, get };
}

const COVERED = ["/v1/sessions/s-1/events?workspace=ws-a", "/v1/events?workspace=ws-a&session=s-1"];

const expect501 = async (r: Response, reason = "session_partial") => {
  expect(r.status).toBe(501);
  expect(r.headers.get("Retry-After")).toBeNull();
  const b = await body(r);
  expect(b.error).toEqual({ code: "unsupported_by_backend", message: `not served by this backend: ${reason}`, reason });
};
const expect503 = async (r: Response) => {
  expect(r.status).toBe(503);
  expect(r.headers.get("Retry-After")).toBe("60");
  const b = await body(r);
  expect(b.error).toEqual({ code: "backend_unavailable", message: "session is not fully readable yet; retry later", reason: "session_partial" });
  expect(typeof b.correlationId).toBe("string");
  expect(JSON.stringify(b)).not.toContain("actual-old-message");
};

describe("sessionPartialRetryableFlag / sessionPartialRetryable (strict parse, service entry only)", () => {
  test("unset, empty and false are off on either entry", () => {
    for (const value of [undefined, "", "false"]) {
      expect(sessionPartialRetryableFlag(value)).toBe(false);
      for (const entry of ["service", "lambda"] as const) expect(sessionPartialRetryable(env(value), entry)).toBe(false);
    }
  });

  test("exactly true is on for the self-host service", () => {
    expect(sessionPartialRetryableFlag("true")).toBe(true);
    expect(sessionPartialRetryable(env("true"), "service")).toBe(true);
  });

  test("true refuses the hosted Lambda rather than being ignored", () => {
    expect(() => sessionPartialRetryable(env("true"), "lambda")).toThrow(SERVICE_ONLY);
  });

  test("a bad value refuses on either entry, naming the variable and never echoing the value", () => {
    for (const value of BAD_VALUES) {
      for (const entry of ["service", "lambda"] as const) {
        let message = "";
        try { sessionPartialRetryable(env(value), entry); } catch (error) { message = (error as Error).message; }
        // Exactly the fixed message: the value is never part of it.
        expect(message).toBe(INVALID);
        expect(message).not.toContain("s3cret");
      }
    }
  });
});

describe("startup wiring", () => {
  const RealPool = pg.Pool;
  const PG_URL = "postgresql://reader:pw@db.selfhost.internal:6543/history";
  const SERVICE_ENV = {
    RELAYHISTORY_SELFHOST_READ_API: "true",
    READ_API_COMPLETE_SINCE: "epoch",
    SESSION_POSTGRES_TLS_PROFILE: "supabase",
    SEARCH_INDEX_POSTGRES_BACKEND_ID: "selfhost-pg",
  } as NodeJS.ProcessEnv;
  let pools = 0;
  const answer = async (text: string) => ({ rows: text === READER_LOGIN_SQL ? [{ ...READER_LOGIN_ROW }] : readerLoginAnswer(text) ?? [] });
  class RecordingPool {
    constructor() { pools++; }
    on() { return this; }
    async query(text: string) { return answer(text); }
    async connect() { return { query: async (text: string) => answer(text), release() {} }; }
    async end() {}
  }
  beforeEach(() => { pools = 0; pg.Pool = RecordingPool as unknown as typeof pg.Pool; });
  afterEach(() => { pg.Pool = RealPool; });

  test("the hosted Lambda refuses true before any pool is built; unset and false build without the dep", async () => {
    const secret = JSON.stringify({ supabase_url: "postgresql://reader:pw@db.example.internal:5432/history", clickhouse: { url: "https://ch.example.internal:8443", user: "u", password: "p", database: "relay_search" } });
    const base = { READ_API_COMPLETE_SINCE: "epoch" } as NodeJS.ProcessEnv;
    const error = await readApiDepsFromSecret(secret, { ...base, ...env("true") }).then(() => null, (e: Error) => e);
    expect(error?.message).toBe(SERVICE_ONLY);
    expect(pools).toBe(0);
    for (const value of [undefined, "false"]) {
      const built = await readApiDepsFromSecret(secret, { ...base, ...env(value) });
      expect("sessionPartialRetryable" in built.deps).toBe(false);
      await built.close();
    }
  });

  test("the self-host service spreads the dep only when on, and refuses a bad value without echoing it", async () => {
    const secret = JSON.stringify({ supabase_url: PG_URL });
    const build = (e: NodeJS.ProcessEnv) => readApiDepsFromSecret(secret, e, selfhostSearchComposition(e), { entry: "service" });
    const on = await build({ ...SERVICE_ENV, ...env("true") });
    expect(on.deps.sessionPartialRetryable).toBe(true);
    await on.close();
    for (const value of [undefined, "", "false"]) {
      const off = await build({ ...SERVICE_ENV, ...env(value) });
      expect("sessionPartialRetryable" in off.deps).toBe(false);
      await off.close();
    }
    const error = await build({ ...SERVICE_ENV, ...env("s3cret-value") }).then(() => null, (e: Error) => e);
    expect(error?.message).toBe(INVALID);
    expect(error?.message).not.toContain("s3cret-value");
  });
});

describe("the hosted Lambda handler refuses true before the secret lookup", () => {
  // buildApp (reached through the exported handler factory) checks the flag before it asks Secrets Manager.
  // fetch is stubbed for the whole test, so nothing leaves the process; the unset run proves the stub sees the
  // lookup, so zero calls with "true" means the refusal came first. Synthetic values only.
  const VARS: Record<string, string> = {
    AWS_REGION: "us-east-1", SESSION_READ_API_SECRET_ARN: "arn:aws:secretsmanager:us-east-1:000000000000:secret:synthetic",
    AWS_ACCESS_KEY_ID: "synthetic-access-key", AWS_SECRET_ACCESS_KEY: "synthetic-secret-key", READ_API_COMPLETE_SINCE: "epoch",
  };
  const touched = [...Object.keys(VARS), "AWS_SESSION_TOKEN", READ_API_SESSION_PARTIAL_RETRYABLE];
  let saved: Record<string, string | undefined> = {};
  beforeEach(() => {
    saved = Object.fromEntries(touched.map((k) => [k, process.env[k]]));
    for (const k of touched) delete process.env[k];
    Object.assign(process.env, VARS);
  });
  afterEach(() => {
    for (const k of touched) {
      if (saved[k] === undefined) delete process.env[k];
      else process.env[k] = saved[k];
    }
    (globalThis.fetch as { mockRestore?: () => void }).mockRestore?.();
  });

  test("true: 503 init failure with no secret lookup; unset: the lookup is attempted", async () => {
    const urls: string[] = [];
    spyOn(globalThis, "fetch").mockImplementation((async (input: Parameters<typeof fetch>[0]) => {
      urls.push(String(input instanceof Request ? input.url : input));
      throw new Error("no network in this test");
    }) as typeof fetch);
    const event = {} as FunctionUrlEvent;
    process.env[READ_API_SESSION_PARTIAL_RETRYABLE] = "true";
    expect((await createReadApiLambdaHandler()(event)).statusCode).toBe(503);
    expect(urls).toEqual([]);
    delete process.env[READ_API_SESSION_PARTIAL_RETRYABLE];
    expect((await createReadApiLambdaHandler()(event)).statusCode).toBe(503);
    expect(urls).toHaveLength(1);
    expect(urls[0]).toContain("secretsmanager");
  });
});

describe("the read Worker refuses READ_API_SESSION_PARTIAL_RETRYABLE", () => {
  afterEach(() => { (console.error as { mockRestore?: () => void }).mockRestore?.(); });
  const base = {
    CLICKHOUSE_URL: "https://ch.test.example", CLICKHOUSE_USER: "u", CLICKHOUSE_PASSWORD: "p", CLICKHOUSE_DATABASE: "d",
    READ_API_ORIGIN: "https://read.test.example", READ_API_COMPLETE_SINCE: "epoch",
    RELAYAUTH_JWKS_URL: "https://auth.test.example/jwks", RELAYAUTH_ISSUER: "https://relayauth.dev",
  };
  const ctx = { waitUntil: () => {}, passThroughOnException: () => {} } as never;
  const overrides = { connect: async () => { throw new Error("no database in this test"); }, fetcher: (async () => new Response("{}", { status: 500 })) as never };
  const status = async (value?: string) =>
    (await handle(new Request("https://read.test.example/v1/sessions/s-1/events?workspace=ws"), { ...base, ...env(value) } as unknown as Env, ctx, overrides as never)).status;

  test("true or a bad value: every request 503, logged as missing_READ_API_SESSION_PARTIAL_RETRYABLE, value never logged", async () => {
    const errors = spyOn(console, "error").mockImplementation(() => {});
    for (const value of ["true", "s3cret-value"]) {
      errors.mockClear();
      expect(await status(value)).toBe(503);
      const lines = errors.mock.calls.map((c) => c.map(String).join(" "));
      expect(lines.some((l) => l.includes("missing_READ_API_SESSION_PARTIAL_RETRYABLE"))).toBe(true);
      expect(lines.some((l) => l.includes("s3cret-value"))).toBe(false);
    }
  });

  test("unset, empty and false: served as before (401 without a token)", async () => {
    for (const value of [undefined, "", "false"]) expect(await status(value)).toBe(401);
  });
});

describe("session_partial on the covered first pages", () => {
  test("flag off: each covered route stays 501 session_partial, no Retry-After", async () => {
    const { get, logs } = api(["s-1"], false);
    for (const path of COVERED) await expect501(await get(path));
    expect(logs.filter((l) => l.reason === "session_partial").map((l) => l.status)).toEqual([501, 501]);
  });

  test("flag on: each covered route is 503 backend_unavailable, reason session_partial, Retry-After 60", async () => {
    const { get, logs } = api(["s-1"], true);
    for (const path of COVERED) await expect503(await get(path));
    expect(logs.filter((l) => l.reason === "session_partial").map((l) => l.status)).toEqual([503, 503]);
    // A complete session is served as before.
    expect((await api(["s-2"], true).get(COVERED[0]!)).status).toBe(200);
    expect((await api(["s-2"], true).get(COVERED[1]!)).status).toBe(200);
  });

  test("CORS exposes Retry-After only with the flag on; the off header is unchanged", async () => {
    const origin = "https://app.example.test";
    const base = "X-Correlation-Id, X-Relayhistory-Workspace-Id, X-Sessions-Read-Continuation";
    const off = await api(["s-1"], false, [], new Set([origin])).get(COVERED[0], origin);
    expect(off.headers.get("access-control-expose-headers")).toBe(base);
    const on = await api(["s-1"], true, [], new Set([origin])).get(COVERED[0], origin);
    expect(on.status).toBe(503);
    expect(on.headers.get("access-control-expose-headers")).toBe(`${base}, Retry-After`);
  });

  test("canonical first page: off 501; on 503, both before and after the object read", async () => {
    const off = canonical(false);
    off.state.partials = async () => ({ sessions: ["old"], overflow: false });
    await expect501(await off.get());
    const on = canonical(true);
    on.state.partials = async () => ({ sessions: ["old"], overflow: false });
    await expect503(await on.get());
    // Complete at the first check, partial at the post-read recheck: still the first page.
    const changed = canonical(true);
    let checks = 0;
    changed.state.partials = async () => ({ sessions: ++checks > 1 ? ["old"] : [], overflow: false });
    await expect503(await changed.get());
  });
});

describe("excluded routes keep the 501 with the flag on", () => {
  test("workspace search: session_partial and session_partial_unbounded stay 501", async () => {
    const r = await api(["s-row"], true, ["s-row"]).get("/v1/events?workspace=ws-a&q=needle&limit=20");
    await expect501(r);
    // An empty partial id cannot be paged past, so the bounded refuse check cannot decide (as app.test.ts).
    const unbounded = await api([""], true).get("/v1/events?workspace=ws-a&q=needle&limit=20");
    expect(unbounded.status).toBe(501);
    expect(unbounded.headers.get("Retry-After")).toBeNull();
    expect((await body(unbounded)).error.reason).toBe("session_partial_unbounded");
  });

  test("another 501 on a covered first page (history_floor, history_floor_unknown_session) keeps its own reason, no Retry-After", async () => {
    // Floor after the session's start: history_floor. No live pointer, so no start time: history_floor_unknown_session.
    // Both are thrown on the same first-page path as session_partial and never clear by retrying, so the flag must
    // leave them alone. The session is also partial, so only the reason tells them apart.
    const floorMs = Date.parse("2026-11-01T00:00:00Z");
    const cases = [
      { reason: "history_floor", createdAt: { "codex\u0000s-1": Date.parse("2026-10-01T00:00:00Z") } },
      { reason: "history_floor_unknown_session", createdAt: {} },
    ];
    for (const { reason, createdAt } of cases) {
      const { get, logs } = api(["s-1"], true, [], undefined, { completeSinceMs: floorMs, createdAt });
      for (const path of COVERED) await expect501(await get(path), reason);
      expect(logs.filter((l) => l.status === 503)).toEqual([]);
    }
  });

  test("canonical cursor page: the recheck stays 501", async () => {
    const f = canonical(true);
    const first = await f.get("&limit=1");
    expect(first.status).toBe(200);
    const { nextCursor } = (await first.json()) as { nextCursor: string };
    expect(nextCursor.startsWith(CANONICAL_CURSOR_PREFIX)).toBe(true);
    f.state.partials = async () => ({ sessions: ["old"], overflow: false });
    await expect501(await f.get(`&cursor=${encodeURIComponent(nextCursor)}`));
  });
});
