/**
 * Relaycontrol user access tokens (`sh_at_`, opaque) through requireAuth and
 * the introspector itself: default off, fail closed, cached briefly, never
 * JWT-parsed, never logged. RelayAuth JWTs keep their own path. Synthetic
 * ids, a throwaway key and a synthetic secret only.
 */
import { createSign, generateKeyPairSync } from "node:crypto";
import { Hono, type Context } from "hono";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  createRelaycontrolIntrospector,
  introspectEndpoint,
  RelaycontrolRejectedError,
  RelaycontrolUnavailableError,
  resetSharedRelaycontrolIntrospectors,
} from "../src/auth/relaycontrol.js";
import type { AppEnv, Bindings } from "../src/env.js";
import { requireAuth, requireScope } from "../src/middleware/auth.js";

const ORIGIN = "https://history.example.test";
const ISSUER = "https://relayauth.example.test";
const JWKS_URL = "https://relayauth.test/relaycontrol-auth/jwks.json";
const INTROSPECT_BASE = "http://relaycontrol.test";
const INTROSPECT_URL = `${INTROSPECT_BASE}/internal/v1/auth/introspect`;
const SECRET = "synthetic-introspect-secret";
const KID = "relaycontrol-auth-test-key";
const ORG = "org-synthetic";
const WORKSPACE = "ws_1";
const USER = "user-synthetic-a";
const TOKEN = "sh_at_synthetic-token-value-0001";

const { privateKey, publicKey } = generateKeyPairSync("rsa", { modulusLength: 2048 });
const JWKS = { keys: [{ ...publicKey.export({ format: "jwk" }), kid: KID, alg: "RS256" }] };
const b64 = (value: unknown) => Buffer.from(JSON.stringify(value)).toString("base64url");
function signJwt(claims: Record<string, unknown>) {
  const header = b64({ alg: "RS256", kid: KID, typ: "JWT" });
  const body = b64(claims);
  return `${header}.${body}.${createSign("RSA-SHA256").update(`${header}.${body}`).end().sign(privateKey, "base64url")}`;
}
function assertionJwt() {
  const now = Math.floor(Date.now() / 1000);
  return signJwt({
    sub: "agent_relayhistory_assertion", org: ORG, wks: WORKSPACE, scopes: ["rth:sync", "rth:read"],
    sponsorId: USER, sponsorChain: [USER, "agent_relayhistory_assertion"], token_type: "access",
    iss: ISSUER, aud: [ORIGIN], iat: now, exp: now + 60, jti: "tok_synthetic",
    meta: { tokenClass: "relayhistory_assertion", historyOrigin: ORIGIN, machineId: "mach-1" },
  });
}

function active(overrides: Record<string, unknown> = {}) {
  const now = Math.floor(Date.now() / 1000);
  return {
    active: true, sub: USER, email: "a@example.test", org: ORG, workspace: WORKSPACE,
    scopes: ["cli:auth", "rth:read", "rth:sync"], machine_id: null, aud: ORIGIN,
    iat: now, exp: now + 3600, token_id: "tokid_synthetic_1", ...overrides,
  };
}

type Handler = (body: { token: string; aud: string; workspace?: string; machine?: string }, init: RequestInit) => Response | Promise<Response>;
let introspect: Handler;
let fetchMock: ReturnType<typeof vi.fn>;
let logged: string[];

beforeEach(() => {
  resetSharedRelaycontrolIntrospectors();
  introspect = () => Response.json(active());
  logged = [];
  for (const method of ["log", "warn", "error", "info"] as const)
    vi.spyOn(console, method).mockImplementation((...args: unknown[]) => { logged.push(args.map(String).join(" ")); });
  fetchMock = vi.fn(async (input: RequestInfo | URL, init: RequestInit = {}) => {
    const url = String(input);
    if (url === JWKS_URL) return Response.json(JWKS);
    if (url === INTROSPECT_URL) return introspect(JSON.parse(String(init.body)), init);
    throw new Error(`unexpected fetch ${url}`);
  });
  vi.stubGlobal("fetch", fetchMock);
});
afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

const introspectCalls = () => fetchMock.mock.calls.filter(([u]) => String(u) === INTROSPECT_URL);
const jwksCalls = () => fetchMock.mock.calls.filter(([u]) => String(u) === JWKS_URL);

function bindings(overrides: Partial<Bindings> = {}): Bindings {
  return {
    RELAYAUTH_JWKS_URL: JWKS_URL, RELAYAUTH_ISSUER: ISSUER, RELAYHISTORY_ORIGIN: ORIGIN,
    RELAYHISTORY_ACCEPT_RELAYCONTROL_TOKENS: "true", RELAYCONTROL_INTROSPECT_URL: INTROSPECT_BASE,
    RELAYCONTROL_INTROSPECT_SECRET: SECRET, ...overrides,
  } as Bindings;
}

function app() {
  const v1 = new Hono<AppEnv>();
  v1.use("*", requireAuth);
  const echo = (c: Context<AppEnv>) => {
    const auth = c.get("auth");
    return c.json({ userId: auth?.userId, orgId: auth?.orgId, workspaceId: auth?.workspaceId, historyOrigin: auth?.historyOrigin ?? null, scopes: auth?.scopes, machine: (auth?.claims as { meta?: unknown })?.meta ?? null });
  };
  v1.post("/pipeline/manifests", echo);
  v1.get("/pipeline/read", requireScope("rth:read"), echo);
  const root = new Hono<AppEnv>();
  root.route("/v1", v1);
  return (token: string | null, env: Partial<Bindings> = {}, opts: { workspace?: string | null; path?: string; method?: string; headers?: Record<string, string> } = {}) => {
    const headers: Record<string, string> = { ...(opts.headers ?? {}) };
    if (token !== null) headers.Authorization = `Bearer ${token}`;
    if (opts.workspace !== null) headers["x-relayhistory-workspace-id"] = opts.workspace ?? WORKSPACE;
    return root.request(opts.path ?? "/v1/pipeline/manifests", { method: opts.method ?? "POST", headers }, bindings(env));
  };
}
const code = async (response: Response) => ((await response.json()) as { error?: { code?: string } }).error?.code;

describe("flag off: sh_at_ is refused exactly as before", () => {
  it("is not introspected with the flag unset or not exactly true", async () => {
    for (const flag of [undefined, "", "1", "TRUE", "yes", "true "]) {
      const response = await app()(TOKEN, { RELAYHISTORY_ACCEPT_RELAYCONTROL_TOKENS: flag });
      expect(`${flag}: ${response.status}`).toBe(`${flag}: 401`);
      expect(await code(response)).toBe("invalid_token");
    }
    // No origin (bearer auth unconfigured): today's auth_not_configured, still no introspection.
    const unconfigured = await app()(TOKEN, { RELAYHISTORY_ACCEPT_RELAYCONTROL_TOKENS: undefined, RELAYHISTORY_ORIGIN: undefined });
    expect(unconfigured.status).toBe(401);
    expect(await code(unconfigured)).toBe("auth_not_configured");
    expect(introspectCalls()).toHaveLength(0);
  });
});

describe("flag on", () => {
  it("admits an active token as its user, org, workspace and allowlisted scopes, with no machine claim", async () => {
    const response = await app()(TOKEN);
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({
      userId: USER, orgId: ORG, workspaceId: WORKSPACE, historyOrigin: ORIGIN, scopes: ["rth:read", "rth:sync"], machine: null,
    });
    expect(jwksCalls()).toHaveLength(0);
  });

  it("sends the configured secret, the token, aud and workspace, with redirect error and a timeout signal", async () => {
    await app()(TOKEN);
    const [[url, init]] = introspectCalls() as [[string, RequestInit]];
    expect(url).toBe(INTROSPECT_URL);
    expect(init.method).toBe("POST");
    expect(init.redirect).toBe("error");
    expect(init.signal).toBeInstanceOf(AbortSignal);
    expect(new Headers(init.headers).get("authorization")).toBe(`Bearer ${SECRET}`);
    expect(JSON.parse(String(init.body))).toEqual({ token: TOKEN, aud: ORIGIN, workspace: WORKSPACE });
  });

  it("forwards X-Machine-Id verbatim as machine and takes the claim only from the verified machine_id", async () => {
    introspect = (body) => Response.json(active({ machine_id: body.machine ?? null }));
    const response = await app()(TOKEN, {}, { headers: { "X-Machine-Id": "mach-1" } });
    expect(response.status).toBe(200);
    expect(((await response.json()) as { machine: unknown }).machine).toEqual({ machineId: "mach-1" });
    expect(JSON.parse(String(introspectCalls()[0]![1].body))).toEqual({ token: TOKEN, aud: ORIGIN, workspace: WORKSPACE, machine: "mach-1" });
    // relaycontrol verified no machine: the header alone never becomes a claim.
    resetSharedRelaycontrolIntrospectors();
    introspect = () => Response.json(active({ machine_id: null }));
    const unverified = await app()(TOKEN, {}, { headers: { "X-Machine-Id": "mach-1" } });
    expect(((await unverified.json()) as { machine: unknown }).machine).toBeNull();
    // A malformed claim is refused here, without a call.
    resetSharedRelaycontrolIntrospectors();
    fetchMock.mockClear();
    const malformed = await app()(TOKEN, {}, { headers: { "X-Machine-Id": "mach 1/../x" } });
    expect(malformed.status).toBe(401);
    expect(await code(malformed)).toBe("unauthorized");
    expect(introspectCalls()).toHaveLength(0);
  });

  it("grants nothing for cli:auth or unknown scopes; requireScope then refuses (403)", async () => {
    introspect = () => Response.json(active({ scopes: ["cli:auth", "rth:admin", "constructor", "__proto__"] }));
    const response = await app()(TOKEN, {}, { method: "GET", path: "/v1/pipeline/read" });
    expect(response.status).toBe(403);
    expect(await code(response)).toBe("forbidden");
  });

  it("accepts and ignores principal_type, known or not", async () => {
    for (const principal_type of ["desktop-user", "something-new", undefined]) {
      resetSharedRelaycontrolIntrospectors();
      introspect = () => Response.json(active({ principal_type }));
      expect((await app()(TOKEN)).status).toBe(200);
    }
  });

  it("refuses inactive, wrong aud, workspace mismatch, expired, and relaycontrol 401/400 (401 unauthorized)", async () => {
    const cases: Record<string, Handler> = {
      ...Object.fromEntries(
        ["expired", "revoked", "unknown", "wrong-kind", "audience", "workspace", "machine_not_owned", "machine_unclaimed", "machine_workspace_mismatch", undefined]
          .map((reason) => [`inactive:${reason}`, () => Response.json({ active: false, ...(reason ? { reason } : {}) })]),
      ),
      audOther: () => Response.json(active({ aud: "https://history.other.test" })),
      audSlash: () => Response.json(active({ aud: `${ORIGIN}/` })),
      workspace: () => Response.json(active({ workspace: "ws_other" })),
      expired: () => Response.json(active({ exp: Math.floor(Date.now() / 1000) - 1 })),
      status401: () => new Response("no", { status: 401 }),
      status400: () => new Response("bad", { status: 400 }),
    };
    for (const [name, handler] of Object.entries(cases)) {
      resetSharedRelaycontrolIntrospectors();
      introspect = handler;
      const response = await app()(TOKEN);
      expect(`${name}: ${response.status}`).toBe(`${name}: 401`);
      expect(await code(response)).toBe("unauthorized");
    }
    // The reason is for the audit log only, never the response.
    resetSharedRelaycontrolIntrospectors();
    introspect = () => Response.json({ active: false, reason: "machine_not_owned" });
    const response = await app()(TOKEN, {}, { headers: { "X-Machine-Id": "mach-1" } });
    expect(await response.text()).not.toContain("machine_not_owned");
    expect(logged.join("\n")).toContain("machine_not_owned");
  });

  it("refuses a malformed workspace header without calling relaycontrol", async () => {
    for (const workspace of ["../x", "a b"]) {
      const response = await app()(TOKEN, {}, { workspace });
      expect(response.status).toBe(401);
    }
    expect(introspectCalls()).toHaveLength(0);
  });

  it("omits workspace when no header names one, and takes the workspace from the response", async () => {
    for (const workspace of [null, ""]) {
      resetSharedRelaycontrolIntrospectors();
      fetchMock.mockClear();
      introspect = (body) => { expect(body).not.toHaveProperty("workspace"); return Response.json(active({ workspace: "ws_from_response" })); };
      const response = await app()(TOKEN, {}, { workspace });
      expect(response.status).toBe(200);
      expect(((await response.json()) as { workspaceId: string }).workspaceId).toBe("ws_from_response");
      expect(Object.keys(JSON.parse(String(introspectCalls()[0]![1].body))).sort()).toEqual(["aud", "token"]);
    }
  });

  it("fails closed with 503 relaycontrol_unavailable on timeout, network error, 5xx, malformed JSON or shape", async () => {
    const timeout = Object.assign(new Error("timed out"), { name: "TimeoutError" });
    const cases: Record<string, Handler> = {
      timeout: () => { throw timeout; },
      network: () => { throw new TypeError("fetch failed"); },
      status404Disabled: () => new Response("not found", { status: 404 }),
      status500: () => new Response("down", { status: 500 }),
      status503: () => new Response("down", { status: 503 }),
      status302: () => new Response(null, { status: 302 }),
      notJson: () => new Response("<html>", { status: 200 }),
      jsonArray: () => Response.json([]),
      noActive: () => Response.json({ sub: USER }),
      badScopes: () => Response.json(active({ scopes: "rth:read" })),
      noTokenId: () => Response.json(active({ token_id: undefined })),
    };
    for (const [name, handler] of Object.entries(cases)) {
      resetSharedRelaycontrolIntrospectors();
      introspect = handler;
      const response = await app()(TOKEN);
      expect(`${name}: ${response.status}`).toBe(`${name}: 503`);
      expect(response.headers.get("retry-after")).toBe("1");
      expect(await code(response)).toBe("relaycontrol_unavailable");
    }
  });

  it("answers auth_not_configured (401) when the flag is on but the URL, secret or origin is missing", async () => {
    for (const missing of ["RELAYCONTROL_INTROSPECT_URL", "RELAYCONTROL_INTROSPECT_SECRET", "RELAYHISTORY_ORIGIN"] as const) {
      const response = await app()(TOKEN, { [missing]: undefined });
      expect(`${missing}: ${response.status}`).toBe(`${missing}: 401`);
      expect(await code(response)).toBe("auth_not_configured");
    }
    const badUrl = await app()(TOKEN, { RELAYCONTROL_INTROSPECT_URL: "ftp://relaycontrol.test" });
    expect(await code(badUrl)).toBe("auth_not_configured");
    expect(introspectCalls()).toHaveLength(0);
  });

  it("still verifies a RelayAuth JWT unchanged, and never sends it to relaycontrol", async () => {
    const response = await app()(assertionJwt());
    expect(response.status).toBe(200);
    expect((await response.json()) as object).toMatchObject({ userId: USER, workspaceId: WORKSPACE, machine: { machineId: "mach-1" } });
    expect(introspectCalls()).toHaveLength(0);
    // A junk non-sh_at_ bearer is a RelayAuth failure, not an introspection.
    expect((await app()("rc_at_whatever")).status).toBe(401);
    expect(introspectCalls()).toHaveLength(0);
  });

  it("never lets the request choose the introspection URL", async () => {
    await app()(TOKEN, {}, { path: "/v1/pipeline/manifests?RELAYCONTROL_INTROSPECT_URL=http://evil.test", headers: { "x-relaycontrol-introspect-url": "http://evil.test", host: "evil.test" } });
    expect(fetchMock.mock.calls.map(([u]) => String(u))).toEqual([INTROSPECT_URL]);
  });

  it("never logs the token, only its token_id", async () => {
    introspect = () => Response.json({ active: false, reason: "revoked" });
    await app()(TOKEN);
    resetSharedRelaycontrolIntrospectors();
    introspect = () => { throw new TypeError("fetch failed"); };
    await app()(TOKEN);
    resetSharedRelaycontrolIntrospectors();
    introspect = () => Response.json(active({ workspace: "ws_other" }));
    await app()(TOKEN);
    expect(logged.length).toBeGreaterThan(0);
    for (const line of logged) {
      expect(line).not.toContain(TOKEN);
      expect(line).not.toContain(SECRET);
    }
    expect(logged.join("\n")).toContain("tokid_synthetic_1");
  });
});

describe("cache", () => {
  it("serves a positive hit without a call, re-introspects after the TTL and sees revocation", async () => {
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(new Date("2026-11-07T00:00:00Z"));
    const run = app();
    expect((await run(TOKEN)).status).toBe(200);
    expect((await run(TOKEN)).status).toBe(200);
    expect(introspectCalls()).toHaveLength(1);
    introspect = () => Response.json({ active: false, reason: "revoked" });
    vi.setSystemTime(new Date("2026-11-07T00:00:59Z"));
    expect((await run(TOKEN)).status).toBe(200); // within the 60 s lag
    vi.setSystemTime(new Date("2026-11-07T00:01:01Z"));
    expect((await run(TOKEN)).status).toBe(401);
    expect(introspectCalls()).toHaveLength(2);
  });

  it("never reuses an expired entry when relaycontrol is down", async () => {
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(new Date("2026-11-07T00:00:00Z"));
    const run = app();
    expect((await run(TOKEN)).status).toBe(200);
    introspect = () => new Response("down", { status: 503 });
    vi.setSystemTime(new Date("2026-11-07T00:01:01Z"));
    expect((await run(TOKEN)).status).toBe(503);
  });

  it("caps the positive TTL at the token's own exp", async () => {
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(new Date("2026-11-07T00:00:00Z"));
    introspect = () => Response.json(active({ exp: Math.floor(Date.now() / 1000) + 10 }));
    const run = app();
    expect((await run(TOKEN)).status).toBe(200);
    vi.setSystemTime(new Date("2026-11-07T00:00:11Z"));
    introspect = () => Response.json({ active: false, reason: "expired" });
    expect((await run(TOKEN)).status).toBe(401);
    expect(introspectCalls()).toHaveLength(2);
  });

  it("keys on workspace and aud: a different one re-introspects", async () => {
    const run = app();
    await run(TOKEN);
    introspect = (body) => Response.json(active({ workspace: body.workspace, aud: body.aud }));
    expect((await run(TOKEN, {}, { workspace: "ws_2" })).status).toBe(200);
    expect((await run(TOKEN, { RELAYHISTORY_ORIGIN: "https://history2.example.test" })).status).toBe(200);
    expect(introspectCalls()).toHaveLength(3);
  });
});

describe("the introspector directly", () => {
  const make = (handler: Handler, extra: Partial<Parameters<typeof createRelaycontrolIntrospector>[0]> = {}) => {
    let clock = Date.parse("2026-11-07T00:00:00Z");
    const calls: unknown[] = [];
    const lines: string[] = [];
    const introspector = createRelaycontrolIntrospector({
      url: INTROSPECT_BASE, secret: SECRET, now: () => clock,
      fetch: (async (_u: RequestInfo | URL, init: RequestInit = {}) => { calls.push(init); return handler(JSON.parse(String(init.body)), init); }) as typeof fetch,
      log: (level, message, fields) => lines.push(JSON.stringify({ level, message, ...fields })),
      ...extra,
    });
    return { introspector, calls, lines, tick: (ms: number) => { clock += ms; } };
  };
  const req = { aud: ORIGIN, workspace: WORKSPACE };
  const at = (seconds: number) => Math.floor(Date.parse("2026-11-07T00:00:00Z") / 1000) + seconds;

  it("single-flights concurrent lookups for one key", async () => {
    let release!: () => void;
    const gate = new Promise<void>((r) => { release = r; });
    const { introspector, calls } = make(async () => { await gate; return Response.json(active({ iat: at(0), exp: at(3600) })); });
    const pending = [1, 2, 3].map(() => introspector.verifyRelaycontrolToken(TOKEN, req));
    release();
    const results = await Promise.all(pending);
    expect(calls).toHaveLength(1);
    expect(results.map((r) => r.userId)).toEqual([USER, USER, USER]);
    results[0].scopes.push("rth:admin");
    expect((await introspector.verifyRelaycontrolToken(TOKEN, req)).scopes).toEqual(["rth:read", "rth:sync"]);
  });

  it("caches a refusal for at most 5 s and never a relaycontrol 401", async () => {
    let body: unknown = { active: false };
    const { introspector, calls, tick } = make(() => Response.json(body));
    await expect(introspector.verifyRelaycontrolToken(TOKEN, req)).rejects.toBeInstanceOf(RelaycontrolRejectedError);
    body = active({ iat: at(0), exp: at(3600) });
    tick(4_000);
    await expect(introspector.verifyRelaycontrolToken(TOKEN, req)).rejects.toBeInstanceOf(RelaycontrolRejectedError);
    tick(1_001);
    await expect(introspector.verifyRelaycontrolToken(TOKEN, req)).resolves.toMatchObject({ userId: USER });
    expect(calls).toHaveLength(2);

    const second = make(() => new Response("", { status: 401 }));
    await expect(second.introspector.verifyRelaycontrolToken(TOKEN, req)).rejects.toBeInstanceOf(RelaycontrolRejectedError);
    await expect(second.introspector.verifyRelaycontrolToken(TOKEN, req)).rejects.toBeInstanceOf(RelaycontrolRejectedError);
    expect(second.calls).toHaveLength(2);
  });

  it("does not cache unavailability", async () => {
    const { introspector, calls } = make(() => new Response("", { status: 502 }));
    await expect(introspector.verifyRelaycontrolToken(TOKEN, req)).rejects.toBeInstanceOf(RelaycontrolUnavailableError);
    await expect(introspector.verifyRelaycontrolToken(TOKEN, req)).rejects.toBeInstanceOf(RelaycontrolUnavailableError);
    expect(calls).toHaveLength(2);
  });

  it("bounds the cache, evicting the oldest", async () => {
    const { introspector, calls } = make(() => Response.json({ active: false }), { maxEntries: 2 });
    for (const t of ["sh_at_a", "sh_at_b", "sh_at_c", "sh_at_a"])
      await expect(introspector.verifyRelaycontrolToken(t, req)).rejects.toBeInstanceOf(RelaycontrolRejectedError);
    expect(calls).toHaveLength(4);
  });

  it("refuses non-sh_at_ tokens, oversize tokens and bad requests without a call", async () => {
    const { introspector, calls } = make(() => Response.json(active()));
    const jwt = assertionJwt();
    for (const [token, request] of [
      [jwt, req], ["sh_at_" + "x".repeat(5000), req], ["sh_at_ x", req],
      [TOKEN, { aud: "", workspace: WORKSPACE }], [TOKEN, { aud: ORIGIN, workspace: "" }],
    ] as const) await expect(introspector.verifyRelaycontrolToken(token, request)).rejects.toBeInstanceOf(RelaycontrolRejectedError);
    expect(calls).toHaveLength(0);
  });

  it("sends workspace and machine only when given, and the response workspace is authoritative", async () => {
    const bodies: Record<string, unknown>[] = [];
    const { introspector } = make((body) => { bodies.push(body); return Response.json(active({ workspace: "ws_resp", iat: at(0), exp: at(3600) })); });
    const auth = await introspector.verifyRelaycontrolToken(TOKEN, { aud: ORIGIN });
    expect(auth.workspaceId).toBe("ws_resp");
    expect((auth.claims as { wks?: unknown }).wks).toBe("ws_resp");
    expect(bodies[0]).toEqual({ token: TOKEN, aud: ORIGIN });
    await introspector.verifyRelaycontrolToken(TOKEN, { aud: ORIGIN, workspace: "ws_resp", machine: "mach-1" }).catch(() => {});
    expect(bodies[1]).toEqual({ token: TOKEN, aud: ORIGIN, workspace: "ws_resp", machine: "mach-1" });
    // A workspace that was sent must come back exactly.
    await expect(introspector.verifyRelaycontrolToken(TOKEN, { aud: ORIGIN, workspace: WORKSPACE })).rejects.toBeInstanceOf(RelaycontrolRejectedError);
  });

  it("sets the machine claim only from a verified machine_id equal to the claimed one", async () => {
    let machineId: unknown = "mach-1";
    const { introspector } = make(() => Response.json(active({ machine_id: machineId, iat: at(0), exp: at(3600) })), { positiveTtlMs: 0, negativeTtlMs: 0 });
    const meta = async (machine?: string) =>
      (await introspector.verifyRelaycontrolToken(TOKEN, { ...req, ...(machine ? { machine } : {}) })).claims.meta;
    expect(await meta("mach-1")).toEqual({ machineId: "mach-1" });
    // machine_id null (no verified machine): no claim.
    machineId = null;
    expect(await meta("mach-1")).toBeUndefined();
    // No claim sent: a machine_id in the answer never becomes a claim.
    machineId = "mach-1";
    expect(await meta()).toBeUndefined();
    // Verified id differs from the claim: refused.
    machineId = "mach-2";
    await expect(meta("mach-1")).rejects.toBeInstanceOf(RelaycontrolRejectedError);
    // Not a string: malformed answer.
    machineId = 7;
    await expect(meta("mach-1")).rejects.toBeInstanceOf(RelaycontrolUnavailableError);
  });

  it("never reuses a cached result across claimed machines", async () => {
    const { introspector, calls } = make((body) => body.machine === "mach-a"
      ? Response.json(active({ machine_id: "mach-a", iat: at(0), exp: at(3600) }))
      : Response.json({ active: false, reason: "machine_not_owned" }));
    expect((await introspector.verifyRelaycontrolToken(TOKEN, { ...req, machine: "mach-a" })).claims.meta).toEqual({ machineId: "mach-a" });
    expect((await introspector.verifyRelaycontrolToken(TOKEN, { ...req, machine: "mach-a" })).claims.meta).toEqual({ machineId: "mach-a" });
    expect(calls).toHaveLength(1);
    await expect(introspector.verifyRelaycontrolToken(TOKEN, { ...req, machine: "mach-b" })).rejects.toBeInstanceOf(RelaycontrolRejectedError);
    const noMachine = await introspector.verifyRelaycontrolToken(TOKEN, req).catch((e) => e);
    expect(calls).toHaveLength(3);
    expect(noMachine).toBeInstanceOf(RelaycontrolRejectedError);
  });

  it("refuses a claimed machine outside NODE_ID_PATTERN without a call", async () => {
    const { introspector, calls } = make(() => Response.json(active()));
    for (const machine of ["", "a b", "mach/1", "m".repeat(201), "mach-1\n", "ünï"])
      await expect(introspector.verifyRelaycontrolToken(TOKEN, { ...req, machine })).rejects.toBeInstanceOf(RelaycontrolRejectedError);
    expect(calls).toHaveLength(0);
    const ok = make(() => Response.json(active({ machine_id: "A.b_c:d-9", iat: at(0), exp: at(3600) })));
    expect((await ok.introspector.verifyRelaycontrolToken(TOKEN, { ...req, machine: "A.b_c:d-9" })).claims.meta).toEqual({ machineId: "A.b_c:d-9" });
    expect((await ok.introspector.verifyRelaycontrolToken(TOKEN, { ...req, machine: "m".repeat(200) }).catch((e) => e))).toBeInstanceOf(RelaycontrolRejectedError);
  });

  it("validates the URL shape and runs the composition's policy, without echoing it", () => {
    expect(introspectEndpoint("http://relaycontrol.test/base/")).toBe("http://relaycontrol.test/base/internal/v1/auth/introspect");
    for (const bad of ["ftp://relaycontrol.test", "http://u:p@relaycontrol.test", "http://relaycontrol.test/?a=1", "http://relaycontrol.test/#x", "not a url"]) {
      let message = "";
      try { introspectEndpoint(bad); } catch (error) { message = String(error); }
      expect(message).not.toBe("");
      expect(message).not.toContain("relaycontrol.test");
    }
    expect(() => createRelaycontrolIntrospector({ url: INTROSPECT_BASE, secret: " " })).toThrow();
    expect(() => createRelaycontrolIntrospector({ url: INTROSPECT_BASE, secret: SECRET, validateUrl: () => { throw new Error("refused"); } })).toThrow("refused");
  });
});
