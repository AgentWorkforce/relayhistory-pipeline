/**
 * The ingest edge's `rth_at_` verifier (rth-verifier.ts) against an in-process fake PostgREST
 * (node:http on 127.0.0.1, no network): the flag (off = step 6 byte-for-byte), startup refusals, the
 * pinned RPC contract, identity mapping, 401/403/503 mapping, the bounded default-off cache and
 * revocation, outages never cached, `rth_st_`/`rth_rt_` never sent, and no secret in any output.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { createHash, createHmac } from "node:crypto";
import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { IDENTITY_HEADER, type IngestIdentity } from "../app.ts";
import {
  INGEST_EDGE_FLAG, createIngestEdge, edgeIdentify, ingestEdgeConfig, mountIngestEdge,
  type EdgeHandlerIdentify, type IngestEdgeAppDeps, type IngestEdgeOptions,
} from "../ingest-edge.ts";
import {
  RTH_VERIFIER_CACHE_MAX_ENTRIES, RTH_VERIFIER_FLAG, RTH_VERIFIER_JWT_LIFETIME_S, RTH_VERIFIER_REQUIRED_SCOPE,
  createRthVerifier, rthVerifierConfig, rthVerifierEnabled,
} from "../rth-verifier.ts";
import { TENANT } from "./support.ts";

const ORIGIN = "https://history.edge.test";
const INTROSPECT_SECRET = "introspect-secret-test";
const JWT_SECRET = "verifier-jwt-secret-for-tests-only-0123456789";
const ISSUER = "https://issuer.example.invalid";
const AUDIENCE = "session-token-verifier";
const TOKEN = "rth_at_desktop-token-AAAAbbbbCCCCdddd";
const HASH = createHash("sha256").update(TOKEN).digest("hex");
const T0 = Date.parse("2026-11-11T12:00:00.000Z");

const cleanups: (() => unknown)[] = [];
afterEach(async () => {
  for (const f of cleanups.splice(0).reverse()) await f();
});

type Answer = { status: number; body?: unknown; raw?: string } | "hang";
type Call = { path: string; method: string; headers: Record<string, string | string[] | undefined>; body: Record<string, unknown> };

/** A loopback HTTP server, scripted per call; records each call. */
async function fakeServer(answer: (call: Call) => Answer | Promise<Answer>) {
  const calls: Call[] = [];
  const server: Server = createServer((req, res) => {
    const chunks: Buffer[] = [];
    req.on("data", (c) => chunks.push(c));
    req.on("end", async () => {
      let body: Record<string, unknown> = {};
      try {
        body = JSON.parse(Buffer.concat(chunks).toString("utf8")) as Record<string, unknown>;
      } catch {}
      const call = { path: req.url ?? "", method: req.method ?? "", headers: req.headers, body };
      calls.push(call);
      const a = await answer(call);
      if (a === "hang") return;
      res.writeHead(a.status, { "content-type": "application/json" });
      res.end(a.raw ?? (a.body === undefined ? "" : JSON.stringify(a.body)));
    });
  });
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
  let closed = false;
  const close = () => (closed ? undefined : ((closed = true), new Promise<void>((r) => { server.closeAllConnections?.(); server.close(() => r()); })));
  cleanups.push(close);
  return { url: `http://127.0.0.1:${(server.address() as AddressInfo).port}`, calls, close };
}

const row = (scopes: string[], over: Record<string, unknown> = {}) => ({
  id: "sess-1", token_family_id: "fam-1", subject_type: "cli", user_id: TENANT.userId, org_id: TENANT.orgId,
  workspace_id: TENANT.workspaceId, scopes, access_token_expires_at: new Date(T0 + 3_600_000).toISOString(), label: "desktop", ...over,
});
const rows = (...r: unknown[]): Answer => ({ status: 200, body: r });

/** The fake relay-control introspection (only ever reached by `sh_at_`). */
async function relaycontrol() {
  return fakeServer(() => ({
    status: 200,
    body: { active: true, sub: TENANT.userId, email: null, principal_type: "desktop-user", machine_id: null, org: TENANT.orgId, workspace: TENANT.workspaceId, scopes: ["rth:sync"], aud: ORIGIN, iat: Math.floor(Date.now() / 1000) - 10, exp: Math.floor(Date.now() / 1000) + 600, token_id: "tok-1" },
  }));
}

const env = (introspectUrl: string, postgrest: string | undefined, over: NodeJS.ProcessEnv = {}): NodeJS.ProcessEnv => ({
  [INGEST_EDGE_FLAG]: "true",
  RELAYHISTORY_INGEST_ORIGIN: ORIGIN,
  RELAYCONTROL_INTROSPECT_URL: introspectUrl,
  RELAYCONTROL_INTROSPECT_SECRET: INTROSPECT_SECRET,
  ...(postgrest === undefined ? {} : {
    [RTH_VERIFIER_FLAG]: "supabase",
    INGEST_EDGE_RTH_VERIFIER_URL: `${postgrest}/rest/v1`,
    INGEST_EDGE_RTH_VERIFIER_JWT_SECRET: JWT_SECRET,
    INGEST_EDGE_RTH_VERIFIER_ISSUER: ISSUER,
    INGEST_EDGE_RTH_VERIFIER_AUDIENCE: AUDIENCE,
  }),
  ...over,
});

const DEPS = { deliver: {} as IngestEdgeAppDeps["deliver"], log: () => {} } as IngestEdgeAppDeps;

function recorder() {
  const seen: { headers: Record<string, string>; identity: IngestIdentity | Error }[] = [];
  const make = (identify: EdgeHandlerIdentify) => ({
    async fetch(request: Request) {
      let identity: IngestIdentity | Error;
      try {
        identity = identify(request);
      } catch (e) {
        identity = e as Error;
      }
      seen.push({ headers: Object.fromEntries(request.headers), identity });
      return new Response("ok", { status: 202 });
    },
  });
  return { seen, make };
}

const clock = (start = T0) => {
  const c = { t: start, now: () => c.t };
  return c;
};

/** An edge with the verifier on, over a recorder, with a controllable clock. */
async function edgeFor(answer: (call: Call) => Answer | Promise<Answer>, over: NodeJS.ProcessEnv = {}, timeoutMs = 300) {
  const pg = await fakeServer(answer);
  const rc = await relaycontrol();
  const e = env(rc.url, pg.url, over);
  const settings = ingestEdgeConfig(e);
  if (!settings.enabled) throw new Error("expected enabled");
  const c = clock();
  const rec = recorder();
  const options: IngestEdgeOptions = { random: () => 0.5, rthVerifier: { now: c.now, timeoutMs } };
  const edge = createIngestEdge(edgeIdentify(settings, e, options), rec.make, options);
  return { pg, rc, rec, edge, clock: c };
}

const req = (token: string, init: { method?: string; headers?: Record<string, string> } = {}) =>
  new Request(`${ORIGIN}/v1/delivery/limits`, { method: init.method ?? "GET", headers: { authorization: `Bearer ${token}`, "x-correlation-id": "corr-1", ...(init.headers ?? {}) } });

async function bodyOf(res: Response) {
  return (await res.json()) as { error?: { code: string; message: string } };
}

function decodeJwt(jwt: string) {
  const [h, p, s] = jwt.split(".");
  return {
    header: JSON.parse(Buffer.from(h!, "base64url").toString("utf8")) as Record<string, unknown>,
    payload: JSON.parse(Buffer.from(p!, "base64url").toString("utf8")) as Record<string, unknown>,
    signatureOk: createHmac("sha256", JWT_SECRET).update(`${h}.${p}`).digest("base64url") === s,
  };
}

describe("flag and startup", () => {
  test("unset, empty and off are off; exactly supabase is on; anything else refuses without echoing the value", () => {
    expect(rthVerifierEnabled({})).toBe(false);
    expect(rthVerifierEnabled({ [RTH_VERIFIER_FLAG]: "" })).toBe(false);
    expect(rthVerifierEnabled({ [RTH_VERIFIER_FLAG]: "off" })).toBe(false);
    expect(rthVerifierEnabled({ [RTH_VERIFIER_FLAG]: "supabase" })).toBe(true);
    for (const bad of ["true", "on", "SUPABASE", " supabase", "neon", "false", "1", "zz-secret-looking-value"]) {
      let message = "";
      try {
        rthVerifierEnabled({ [RTH_VERIFIER_FLAG]: bad });
      } catch (e) {
        message = (e as Error).message;
      }
      expect(message).toContain(RTH_VERIFIER_FLAG);
      if (bad.trim() !== "supabase" && bad !== "off" && bad !== "true") expect(message).not.toContain(bad);
    }
  });

  test("edge off: no verifier variable is read, even a malformed one", () => {
    const e: NodeJS.ProcessEnv = { [RTH_VERIFIER_FLAG]: "garbage", INGEST_EDGE_RTH_VERIFIER_POSITIVE_TTL_MS: "-1", INGEST_EDGE_RTH_VERIFIER_URL: "nope" };
    expect(ingestEdgeConfig(e)).toEqual({ enabled: false });
    expect(mountIngestEdge(e, DEPS)).toBeNull();
  });

  test("edge on, verifier off: the config is exactly step 6's (no rthVerifier key), and its variables are not read", () => {
    for (const flag of [undefined, "", "off"]) {
      const e = env("http://127.0.0.1:9", undefined, { INGEST_EDGE_RTH_VERIFIER_URL: "not a url", INGEST_EDGE_RTH_VERIFIER_POSITIVE_TTL_MS: "999999" });
      if (flag !== undefined) e[RTH_VERIFIER_FLAG] = flag;
      const config = ingestEdgeConfig(e);
      expect(config).toEqual({ enabled: true, origin: ORIGIN, introspectUrl: "http://127.0.0.1:9", introspectSecret: INTROSPECT_SECRET });
      expect(Object.keys(config)).not.toContain("rthVerifier");
    }
    expect(() => ingestEdgeConfig(env("http://127.0.0.1:9", undefined, { [RTH_VERIFIER_FLAG]: "yes" }))).toThrow(RTH_VERIFIER_FLAG);
  });

  test("verifier on: every variable required and checked, naming the variable, never the value", () => {
    const base = env("http://127.0.0.1:9", "http://127.0.0.1:8");
    const config = ingestEdgeConfig(base);
    expect(config).toMatchObject({ rthVerifier: { url: "http://127.0.0.1:8/rest/v1", jwtSecret: JWT_SECRET, issuer: ISSUER, audience: AUDIENCE, positiveTtlMs: 0, negativeTtlMs: 0 } });
    for (const name of ["INGEST_EDGE_RTH_VERIFIER_URL", "INGEST_EDGE_RTH_VERIFIER_JWT_SECRET", "INGEST_EDGE_RTH_VERIFIER_ISSUER", "INGEST_EDGE_RTH_VERIFIER_AUDIENCE"]) {
      expect(() => ingestEdgeConfig({ ...base, [name]: undefined })).toThrow(name);
      expect(() => ingestEdgeConfig({ ...base, [name]: "  " })).toThrow(name);
    }
    const refuse = (over: NodeJS.ProcessEnv, name: string, leaked?: string) => {
      let message = "";
      try {
        ingestEdgeConfig({ ...base, ...over });
      } catch (e) {
        message = (e as Error).message;
      }
      expect(message).toContain(name);
      if (leaked) expect(message).not.toContain(leaked);
    };
    refuse({ INGEST_EDGE_RTH_VERIFIER_URL: "http://postgrest.example.invalid" }, "INGEST_EDGE_RTH_VERIFIER_URL");
    refuse({ INGEST_EDGE_RTH_VERIFIER_URL: "https://u:pw-leak@postgrest.example.invalid" }, "INGEST_EDGE_RTH_VERIFIER_URL", "pw-leak");
    refuse({ INGEST_EDGE_RTH_VERIFIER_URL: "https://postgrest.example.invalid/rest/v1?apikey=leak" }, "INGEST_EDGE_RTH_VERIFIER_URL", "leak");
    refuse({ INGEST_EDGE_RTH_VERIFIER_JWT_SECRET: "short-secret-leak" }, "INGEST_EDGE_RTH_VERIFIER_JWT_SECRET", "short-secret-leak");
    refuse({ INGEST_EDGE_RTH_VERIFIER_ISSUER: "issuer-without-scheme" }, "INGEST_EDGE_RTH_VERIFIER_ISSUER", "issuer-without-scheme");
    // https to a real host passes the destination policy.
    expect(ingestEdgeConfig({ ...base, INGEST_EDGE_RTH_VERIFIER_URL: "https://postgrest.example.invalid/rest/v1/" })).toMatchObject({ rthVerifier: { url: "https://postgrest.example.invalid/rest/v1" } });
  });

  test("cache TTLs: positive 0..60000, negative 0..5000, integers only; anything else refuses startup", () => {
    const base = env("http://127.0.0.1:9", "http://127.0.0.1:8");
    for (const v of ["0", "1", "60000"]) expect(ingestEdgeConfig({ ...base, INGEST_EDGE_RTH_VERIFIER_POSITIVE_TTL_MS: v })).toMatchObject({ rthVerifier: { positiveTtlMs: Number(v) } });
    expect(ingestEdgeConfig({ ...base, INGEST_EDGE_RTH_VERIFIER_POSITIVE_TTL_MS: "" })).toMatchObject({ rthVerifier: { positiveTtlMs: 0 } });
    for (const v of ["60001", "-1", "1.5", "1e3", " 10", "abc", "0x10", "1000000"])
      expect(() => ingestEdgeConfig({ ...base, INGEST_EDGE_RTH_VERIFIER_POSITIVE_TTL_MS: v })).toThrow("INGEST_EDGE_RTH_VERIFIER_POSITIVE_TTL_MS");
    for (const v of ["0", "5000"]) expect(ingestEdgeConfig({ ...base, INGEST_EDGE_RTH_VERIFIER_NEGATIVE_TTL_MS: v })).toMatchObject({ rthVerifier: { negativeTtlMs: Number(v) } });
    for (const v of ["5001", "60000", "-5", "x"])
      expect(() => ingestEdgeConfig({ ...base, INGEST_EDGE_RTH_VERIFIER_NEGATIVE_TTL_MS: v })).toThrow("INGEST_EDGE_RTH_VERIFIER_NEGATIVE_TTL_MS");
    expect(() => rthVerifierConfig({ [RTH_VERIFIER_FLAG]: "supabase" })).toThrow("INGEST_EDGE_RTH_VERIFIER_URL");
  });
});

describe("flag off: step 6 behaviour byte-for-byte", () => {
  test("rth_at_, rth_st_ and rth_rt_ are 401 unsupported_token with step 6's exact body and headers, nothing sent", async () => {
    const pg = await fakeServer(() => rows(row(["rth:sync"])));
    const rc = await relaycontrol();
    const expected = '{"error":{"code":"unsupported_token","message":"opaque relayhistory tokens are not supported by this deployment"},"correlationId":"corr-1"}';
    for (const flag of [undefined, "", "off"]) {
      // The verifier's variables are present (pointing at the fake) but the flag is off.
      const e = env(rc.url, pg.url);
      if (flag === undefined) delete e[RTH_VERIFIER_FLAG];
      else e[RTH_VERIFIER_FLAG] = flag;
      const edge = mountIngestEdge(e, DEPS)!;
      for (const token of [TOKEN, "rth_st_abc", "rth_rt_abc"]) {
        const res = await edge.fetch(req(token));
        expect(res.status).toBe(401);
        expect([...res.headers.entries()].sort()).toEqual([
          ["cache-control", "private, no-store"],
          ["content-type", "application/json"],
          ["www-authenticate", "Bearer"],
          ["x-correlation-id", "corr-1"],
        ]);
        expect(await res.text()).toBe(expected);
      }
    }
    expect(pg.calls.length).toBe(0);
    expect(rc.calls.length).toBe(0);
  });
});

describe("verifier on", () => {
  test("valid row: identity mapped, the handler sees the edge's identity header, and the RPC contract is pinned", async () => {
    const { pg, rc, rec, edge } = await edgeFor(() => rows(row(["rth:sync", "rth:read"])));
    const res = await edge.fetch(req(TOKEN, { headers: { [IDENTITY_HEADER]: "spoofed" } }));
    expect(res.status).toBe(202);
    const want = { ...TENANT, scopes: ["rth:sync", "rth:read"] };
    expect(rec.seen.length).toBe(1);
    expect(rec.seen[0]!.identity).toEqual(want);
    expect(JSON.parse(Buffer.from(rec.seen[0]!.headers[IDENTITY_HEADER]!, "base64url").toString("utf8"))).toEqual(want);
    expect(rec.seen[0]!.headers.authorization).toBeUndefined();
    expect(rc.calls.length).toBe(0);

    expect(pg.calls.length).toBe(1);
    const call = pg.calls[0]!;
    expect(call.method).toBe("POST");
    expect(call.path).toBe("/rest/v1/rpc/auth_session_verify");
    expect(call.headers["content-profile"]).toBe("sessions");
    expect(call.headers["content-type"]).toBe("application/json");
    expect(call.headers.accept).toBe("application/json");
    expect(call.headers.apikey).toBeUndefined();
    expect(Object.keys(call.body).sort()).toEqual(["p_access_token_hash", "p_now"]);
    expect(call.body.p_access_token_hash).toBe(HASH);
    expect(call.body.p_access_token_hash).toMatch(/^[0-9a-f]{64}$/);
    expect(call.body.p_now).toBe(new Date(T0).toISOString());
    const auth = String(call.headers.authorization);
    expect(auth.startsWith("Bearer ")).toBe(true);
    const jwt = decodeJwt(auth.slice(7));
    expect(jwt.header).toEqual({ alg: "HS256", typ: "JWT" });
    expect(jwt.signatureOk).toBe(true);
    expect(jwt.payload).toEqual({ role: "session_token_verifier", iss: ISSUER, aud: AUDIENCE, iat: T0 / 1000, exp: T0 / 1000 + RTH_VERIFIER_JWT_LIFETIME_S });
    expect(RTH_VERIFIER_JWT_LIFETIME_S).toBeLessThanOrEqual(60);
    expect(RTH_VERIFIER_REQUIRED_SCOPE).toBe("rth:sync");
    expect(auth).not.toContain(TOKEN);
  });

  test("through the real ingest app (mountIngestEdge): limits answered for a verified rth_at_", async () => {
    // The real app runs on the wall clock, not the frozen T0, so the row's expiry must too.
    const pg = await fakeServer(() => rows(row(["rth:sync"], { access_token_expires_at: new Date(Date.now() + 3_600_000).toISOString() })));
    const rc = await relaycontrol();
    const edge = mountIngestEdge(env(rc.url, pg.url), DEPS)!;
    const res = await edge.fetch(new Request(`${ORIGIN}/v1/delivery/limits`, { headers: { authorization: `Bearer ${TOKEN}` } }));
    expect(res.status).toBe(200);
    expect(pg.calls.length).toBe(1);
    expect(rc.calls.length).toBe(0);
  });

  test("sh_at_ still goes to relay-control, never to the verifier", async () => {
    const { pg, rc, edge } = await edgeFor(() => rows(row(["rth:sync"])));
    const res = await edge.fetch(req("sh_at_desktop-token-1"));
    expect(res.status).toBe(202);
    expect(rc.calls.length).toBe(1);
    expect(pg.calls.length).toBe(0);
  });

  test("missing rth:sync: 403 forbidden, handler not reached", async () => {
    const { rec, edge } = await edgeFor(() => rows(row(["rth:read"])));
    const res = await edge.fetch(req(TOKEN));
    expect(res.status).toBe(403);
    expect((await bodyOf(res)).error?.code).toBe("forbidden");
    expect(rec.seen.length).toBe(0);
  });

  test("no row: 401 invalid_token; a row already expired by the edge clock: 401 token_expired", async () => {
    const { rec, edge } = await edgeFor(() => rows());
    const res = await edge.fetch(req(TOKEN));
    expect(res.status).toBe(401);
    expect(res.headers.get("www-authenticate")).toBe("Bearer");
    expect((await bodyOf(res)).error?.code).toBe("invalid_token");
    expect(rec.seen.length).toBe(0);
    const late = await edgeFor(() => rows(row(["rth:sync"], { access_token_expires_at: new Date(T0 - 1).toISOString() })), { INGEST_EDGE_RTH_VERIFIER_POSITIVE_TTL_MS: "60000" });
    for (let i = 0; i < 2; i++) {
      const r = await late.edge.fetch(req(TOKEN));
      expect(r.status).toBe(401);
      expect((await bodyOf(r)).error?.code).toBe("token_expired");
    }
    expect(late.pg.calls.length).toBe(2); // never cached
  });

  test("rth_st_ and rth_rt_ stay 401 unsupported_token and never reach PostgREST or relay-control", async () => {
    const { pg, rc, rec, edge } = await edgeFor(() => rows(row(["rth:sync"])));
    for (const token of ["rth_st_abc", "rth_rt_abc", "rth_xx_abc"]) {
      const res = await edge.fetch(req(token));
      expect(res.status).toBe(401);
      expect((await bodyOf(res)).error?.code).toBe("unsupported_token");
    }
    expect(pg.calls.length).toBe(0);
    expect(rc.calls.length).toBe(0);
    expect(rec.seen.length).toBe(0);
  });
});

describe("cache", () => {
  test("default (both TTLs 0): nothing cached, every request is one RPC call", async () => {
    let answer: Answer = rows(row(["rth:sync"]));
    const { pg, edge } = await edgeFor(() => answer);
    expect((await edge.fetch(req(TOKEN))).status).toBe(202);
    expect((await edge.fetch(req(TOKEN))).status).toBe(202);
    answer = rows();
    expect((await edge.fetch(req(TOKEN))).status).toBe(401);
    expect((await edge.fetch(req(TOKEN))).status).toBe(401);
    expect(pg.calls.length).toBe(4);
  });

  test("revocation with an opt-in positive TTL: honoured until the TTL, refused after it", async () => {
    let answer: Answer = rows(row(["rth:sync"]));
    const { pg, edge, clock } = await edgeFor(() => answer, { INGEST_EDGE_RTH_VERIFIER_POSITIVE_TTL_MS: "10000" });
    expect((await edge.fetch(req(TOKEN))).status).toBe(202);
    answer = rows(); // revoked: the function returns no row
    clock.t = T0 + 9_999;
    expect((await edge.fetch(req(TOKEN))).status).toBe(202); // revocation lag, bounded by the TTL
    expect(pg.calls.length).toBe(1);
    clock.t = T0 + 10_000;
    const res = await edge.fetch(req(TOKEN));
    expect(res.status).toBe(401);
    expect((await bodyOf(res)).error?.code).toBe("invalid_token");
    expect(pg.calls.length).toBe(2);
  });

  test("a positive entry never outlives access_token_expires_at, even with the maximum TTL", async () => {
    let answer: Answer = rows(row(["rth:sync"], { access_token_expires_at: new Date(T0 + 3_000).toISOString() }));
    const { pg, edge, clock } = await edgeFor(() => answer, { INGEST_EDGE_RTH_VERIFIER_POSITIVE_TTL_MS: "60000" });
    expect((await edge.fetch(req(TOKEN))).status).toBe(202);
    clock.t = T0 + 2_999;
    expect((await edge.fetch(req(TOKEN))).status).toBe(202);
    expect(pg.calls.length).toBe(1);
    answer = rows(); // expired: the function excludes the row
    clock.t = T0 + 3_000;
    expect((await edge.fetch(req(TOKEN))).status).toBe(401);
    expect(pg.calls.length).toBe(2);
  });

  test("a removed scope is honoured only until the positive TTL", async () => {
    let answer: Answer = rows(row(["rth:sync"]));
    const { edge, clock } = await edgeFor(() => answer, { INGEST_EDGE_RTH_VERIFIER_POSITIVE_TTL_MS: "1000" });
    expect((await edge.fetch(req(TOKEN))).status).toBe(202);
    answer = rows(row(["rth:read"]));
    clock.t = T0 + 1_000;
    expect((await edge.fetch(req(TOKEN))).status).toBe(403);
  });

  test("negative entries: only with the opt-in TTL, at most 5 s", async () => {
    let answer: Answer = rows();
    const { pg, edge, clock } = await edgeFor(() => answer, { INGEST_EDGE_RTH_VERIFIER_NEGATIVE_TTL_MS: "5000" });
    expect((await edge.fetch(req(TOKEN))).status).toBe(401);
    answer = rows(row(["rth:sync"]));
    clock.t = T0 + 4_999;
    expect((await edge.fetch(req(TOKEN))).status).toBe(401);
    expect(pg.calls.length).toBe(1);
    clock.t = T0 + 5_000;
    expect((await edge.fetch(req(TOKEN))).status).toBe(202);
    expect(pg.calls.length).toBe(2);
    // The bound holds even when a caller bypasses config parsing.
    const direct = await fakeServer(() => rows());
    const c = clock;
    c.t = T0;
    const verify = createRthVerifier({ url: direct.url, jwtSecret: JWT_SECRET, issuer: ISSUER, audience: AUDIENCE, positiveTtlMs: 0, negativeTtlMs: 600_000 }, {}, { now: c.now });
    await expect(verify(TOKEN)).rejects.toMatchObject({ status: 401 });
    c.t = T0 + 5_000;
    await expect(verify(TOKEN)).rejects.toMatchObject({ status: 401 });
    expect(direct.calls.length).toBe(2);
  });

  test("concurrent identical lookups share one in-flight call", async () => {
    const { pg, edge } = await edgeFor(async () => {
      await new Promise((r) => setTimeout(r, 100));
      return rows(row(["rth:sync"]));
    });
    const results = await Promise.all([1, 2, 3].map(() => edge.fetch(req(TOKEN))));
    expect(results.map((r) => r.status)).toEqual([202, 202, 202]);
    expect(pg.calls.length).toBe(1);
  });

  test("LRU bounded at 1000 entries", async () => {
    let calls = 0;
    const fakeFetch = (async (_url: string, init: RequestInit) => {
      calls++;
      void init;
      return new Response(JSON.stringify([row(["rth:sync"])]), { status: 200 });
    }) as unknown as typeof fetch;
    const verify = createRthVerifier({ url: "https://postgrest.example.invalid", jwtSecret: JWT_SECRET, issuer: ISSUER, audience: AUDIENCE, positiveTtlMs: 60_000, negativeTtlMs: 0 }, {}, { fetch: fakeFetch, now: () => T0 });
    for (let i = 0; i <= RTH_VERIFIER_CACHE_MAX_ENTRIES; i++) await verify(`rth_at_tok-${i}`);
    expect(calls).toBe(RTH_VERIFIER_CACHE_MAX_ENTRIES + 1);
    await verify(`rth_at_tok-${RTH_VERIFIER_CACHE_MAX_ENTRIES}`); // newest: cached
    expect(calls).toBe(RTH_VERIFIER_CACHE_MAX_ENTRIES + 1);
    await verify("rth_at_tok-0"); // oldest: evicted
    expect(calls).toBe(RTH_VERIFIER_CACHE_MAX_ENTRIES + 2);
  });
});

describe("outages: 503, never 401, never cached", () => {
  test("5xx, timeout, refused JWT, no policy row, missing function, malformed, oversized: 503 + Retry-After, and the next call reaches PostgREST again", async () => {
    const echo = (call: Call) => JSON.stringify({ code: "XX000", message: `echo ${String(call.headers.authorization)} ${JSON.stringify(call.body)}` });
    const cases: [string, (call: Call) => Answer][] = [
      ["500 echoing the request", (call) => ({ status: 500, raw: echo(call) })],
      ["502", () => ({ status: 502, body: { message: "bad gateway" } })],
      ["503", () => ({ status: 503, body: {} })],
      ["timeout", () => "hang"],
      ["JWT refused", (call) => ({ status: 401, raw: echo(call) })],
      ["no policy row (42501)", () => ({ status: 403, body: { code: "42501", message: "auth verify: verifier claims missing or invalid" } })],
      ["function missing", () => ({ status: 404, body: { code: "PGRST202" } })],
      ["not an array", () => ({ status: 200, body: { hello: "world" } })],
      ["not json", () => ({ status: 200, raw: "<html>" })],
      ["two rows", () => rows(row(["rth:sync"]), row(["rth:sync"]))],
      ["row without workspace", () => rows(row(["rth:sync"], { workspace_id: null }))],
      ["scopes not an array", () => rows(row(["rth:sync"], { scopes: "rth:sync" }))],
      ["bad expiry", () => rows(row(["rth:sync"], { access_token_expires_at: "soon" }))],
      ["oversized", () => ({ status: 200, raw: JSON.stringify([row(["rth:sync"], { label: "x".repeat(20_000) })]) })],
    ];
    const written: string[] = [];
    const out = process.stdout.write.bind(process.stdout);
    const err = process.stderr.write.bind(process.stderr);
    process.stdout.write = ((chunk: unknown, ...rest: unknown[]) => (written.push(String(chunk)), (out as (...a: unknown[]) => boolean)(chunk, ...rest))) as typeof process.stdout.write;
    process.stderr.write = ((chunk: unknown, ...rest: unknown[]) => (written.push(String(chunk)), (err as (...a: unknown[]) => boolean)(chunk, ...rest))) as typeof process.stderr.write;
    const responses: string[] = [];
    let jwts: string[] = [];
    try {
      for (const [name, answer] of cases) {
        const { pg, rec, edge } = await edgeFor(answer, { INGEST_EDGE_RTH_VERIFIER_POSITIVE_TTL_MS: "60000", INGEST_EDGE_RTH_VERIFIER_NEGATIVE_TTL_MS: "5000" }, 150);
        for (let i = 1; i <= 2; i++) {
          const res = await edge.fetch(req(TOKEN));
          const text = await res.text();
          responses.push(text, JSON.stringify([...res.headers.entries()]));
          expect({ name, status: res.status }).toEqual({ name, status: 503 });
          expect(JSON.parse(text).error.code).toBe("rth_verifier_unavailable");
          expect(res.headers.get("retry-after")).toBe("8");
          expect(res.headers.get("www-authenticate")).toBeNull();
          expect({ name, calls: pg.calls.length }).toEqual({ name, calls: i }); // not cached
        }
        expect(rec.seen.length).toBe(0);
        jwts = jwts.concat(pg.calls.map((c) => String(c.headers.authorization).slice(7)));
      }
      // Down: nothing listening.
      const { pg, edge } = await edgeFor(() => rows(row(["rth:sync"])));
      await pg.close();
      const res = await edge.fetch(req(TOKEN));
      expect(res.status).toBe(503);
      const ra = Number(res.headers.get("retry-after"));
      expect(ra).toBeGreaterThanOrEqual(5);
      expect(ra).toBeLessThanOrEqual(10);
      responses.push(await res.text());
    } finally {
      process.stdout.write = out;
      process.stderr.write = err;
    }
    // Nothing secret in any response, header or log line.
    const all = responses.join("\n") + written.join("\n");
    expect(written.some((l) => l.includes("rth verifier unavailable"))).toBe(true);
    for (const secret of [TOKEN, HASH, JWT_SECRET, ...jwts]) expect(all).not.toContain(secret);
  });

  test("an expired positive entry is not reused while PostgREST is down", async () => {
    let answer: Answer = rows(row(["rth:sync"]));
    const { pg, edge, clock } = await edgeFor(() => answer, { INGEST_EDGE_RTH_VERIFIER_POSITIVE_TTL_MS: "1000" });
    expect((await edge.fetch(req(TOKEN))).status).toBe(202);
    answer = { status: 503, body: {} };
    clock.t = T0 + 1_000;
    expect((await edge.fetch(req(TOKEN))).status).toBe(503);
    expect(pg.calls.length).toBe(2);
  });
});

describe("no secret in responses", () => {
  test("refusal bodies never carry the token, its hash or the credential", async () => {
    for (const answer of [rows(), rows(row(["rth:read"])), rows(row(["rth:sync"], { access_token_expires_at: new Date(T0 - 1).toISOString() }))]) {
      const { edge } = await edgeFor(() => answer);
      const res = await edge.fetch(req(TOKEN));
      const text = (await res.text()) + JSON.stringify([...res.headers.entries()]);
      for (const secret of [TOKEN, HASH, JWT_SECRET]) expect(text).not.toContain(secret);
    }
  });
});
