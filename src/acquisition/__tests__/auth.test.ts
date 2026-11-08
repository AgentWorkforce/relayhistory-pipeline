// Auth refusal matrix: fake authenticator for the route rules, and the real
// acquisition verifier (verifier.ts, fake fetch) for every cell of
// prefix x principal_type x scope on the ops route, audience, timeout and
// outages. The service is verify-only: there is no native route group.
import { expect, test } from "bun:test";
import { createAcquisitionApp } from "../app.ts";
import { createAcquisitionVerifier } from "../verifier.ts";
import { FakeAuthenticator, ORIGIN, call, captureLog, leaks, opsHarness, opsPrincipal, principal, secrets } from "./helpers.ts";

const NATIVE_STATUS = "/v1/acquisition/native/status?installationId=00000000-0000-4000-8000-0000000000aa&source=droid&sessionIds=s1";
const FORMER_NATIVE = [["POST", "/v1/acquisition/native/installations"], ["POST", "/v1/acquisition/native/claims"], ["POST", "/v1/acquisition/native/revocations"], ["POST", "/v1/acquisition/native/rebinds"], ["GET", NATIVE_STATUS]] as const;
const OPS_STATUS = "/v1/acquisition/ops/status";
const opsBody = { orgId: "org_test", workspaceId: "ws_test", source: "droid", sessionIds: ["s1"] };

test("ACQUISITION_ENABLED=false: every route 503 acquisition_disabled, healthz 200", async () => {
  for (const group of ["native", "ops"] as const) {
    const app = createAcquisitionApp({ enabled: false, group, log: () => {} });
    for (const path of ["/v1/acquisition/native/claims", "/v1/acquisition/ops/install", "/anything"]) {
      const r = await call(app, "POST", path, { token: "sh_at_x", body: {} });
      expect(r.status).toBe(503);
      expect(r.json).toEqual({ error: { code: "acquisition_disabled" } });
      expect(r.headers.get("Retry-After")).toBe("60");
    }
    expect((await call(app, "GET", "/healthz")).json).toEqual({ ok: true, enabled: false, group });
  }
});

test("group flag off: 503 acquisition_disabled", async () => {
  const auth = new FakeAuthenticator();
  const app = createAcquisitionApp({ enabled: true, group: "ops", groupEnabled: false, authenticator: auth, installer: null, log: () => {} });
  expect((await call(app, "POST", OPS_STATUS, { token: "sh_wk_ops", body: opsBody })).status).toBe(503);
});

test("verify-only: no native routes; the dormant listener is 503, the ops listener 404, neither authenticates", async () => {
  const dormant = createAcquisitionApp({ enabled: false, group: "native", log: () => {} });
  const h = opsHarness();
  let introspected = 0;
  const counting = { authenticate: async (t: string) => { introspected++; return h.auth.authenticate(t); } };
  const ops = createAcquisitionApp({ enabled: true, group: "ops", groupEnabled: true, authenticator: counting, installer: h.installer, log: () => {} });
  h.auth.tokens.set("sh_at_alice", principal());
  for (const [method, path] of FORMER_NATIVE)
    for (const token of ["sh_at_alice", "sh_wk_ops"]) {
      const body = method === "POST" ? {} : undefined;
      const d = await call(dormant, method, path, { token, body });
      expect([path, d.status, d.json]).toEqual([path, 503, { error: { code: "acquisition_disabled" } }]);
      const o = await call(ops, method, path, { token, body });
      expect([path, o.status, o.json]).toEqual([path, 404, { error: { code: "not_found" } }]);
    }
  expect(introspected).toBe(0);
  expect(h.sql.calls.length).toBe(0);
  // A valid-looking native claim body against the ops listener is still 404: nothing reads, verifies or signs it.
  const claim = await call(ops, "POST", "/v1/acquisition/native/claims", { token: "sh_at_alice", body: { installationId: "00000000-0000-4000-8000-0000000000aa", source: "droid", sessionId: "s1" }, headers: { "X-Acquisition-Installation-Proof": "p" } });
  expect([claim.status, claim.json, claim.text.includes("signature"), claim.text.includes("assertion")]).toEqual([404, { error: { code: "not_found" } }, false, false]);
  expect([introspected, h.sql.calls.length]).toEqual([0, 0]);
  expect((await call(dormant, "GET", "/healthz")).json).toEqual({ ok: true, enabled: false, group: "native" });
});

test("ops refusal matrix: only a service principal with acquisition:operate", async () => {
  const h = opsHarness();
  h.auth.tokens.set("sh_wk_desktop", principal({ scopes: ["acquisition:operate"] })); // desktop-user
  h.auth.tokens.set("sh_wk_wrongscope", opsPrincipal({ scopes: ["rth:acquire"] }));
  h.auth.tokens.set("sh_at_ops", opsPrincipal()); // a valid ops principal behind the wrong token class
  // Every ops refusal is 403, a missing or non-Bearer credential included.
  expect((await call(h.app, "POST", OPS_STATUS, { body: opsBody })).status).toBe(403);
  expect((await call(h.app, "POST", OPS_STATUS, { body: opsBody, headers: { Authorization: "Basic abc" } })).status).toBe(403);
  for (const t of ["sh_wk_unknown", "sh_wk_desktop", "sh_wk_wrongscope", "sh_at_ops", "bogus"]) {
    const r = await call(h.app, "POST", OPS_STATUS, { token: t, body: opsBody });
    expect([t, r.status, r.json]).toEqual([t, 403, { error: { code: "not_authorized" } }]);
  }
  expect((await call(h.app, "POST", OPS_STATUS, { token: "sh_wk_down", body: opsBody })).status).toBe(503);
  expect(h.sql.calls.length).toBe(0);
  // Unknown route, wrong media type, oversized body.
  expect((await call(h.app, "POST", "/v1/acquisition/ops/nope", { token: "sh_wk_ops", body: {} })).status).toBe(404);
  expect((await call(h.app, "POST", "/v1/acquisition/native/claims", { token: "sh_wk_ops", body: {} })).status).toBe(404);
  expect((await call(h.app, "POST", OPS_STATUS, { token: "sh_wk_ops", body: "x", headers: { "Content-Type": "text/plain" } })).status).toBe(415);
  expect((await call(h.app, "POST", OPS_STATUS, { token: "sh_wk_ops", body: "x".repeat(70 * 1024) })).status).toBe(413);
  expect((await call(h.app, "POST", OPS_STATUS, { token: "sh_wk_ops", body: "{not json" })).status).toBe(400);
});

// ------------------------------------------------------- real acquisition verifier

const INTROSPECT_SECRET = "introspect-secret-for-tests-only";
secrets.add(INTROSPECT_SECRET);
const TOKENS = { sh_at_: "sh_at_realtoken", sh_wk_: "sh_wk_realtoken" } as const;
for (const t of Object.values(TOKENS)) secrets.add(t);

type Sent = { url: string; body: Record<string, unknown>; authorization: string | null };
function realApp(answer: (body: Record<string, unknown>) => Response | Promise<Response>, timeoutMs = 50) {
  const captured = captureLog();
  const sent: Sent[] = [];
  const authenticator = createAcquisitionVerifier({
    url: "https://relaycontrol.example.test/base/",
    secret: INTROSPECT_SECRET,
    origin: ORIGIN,
    timeoutMs,
    fetch: (async (url: string, init: RequestInit) => {
      const body = JSON.parse(String(init.body)) as Record<string, unknown>;
      sent.push({ url, body, authorization: new Headers(init.headers).get("authorization") });
      const signal = init.signal;
      return Promise.race([
        answer(body),
        new Promise<Response>((_, reject) => signal?.addEventListener("abort", () => reject(signal.reason ?? new Error("aborted")))),
      ]);
    }) as unknown as typeof fetch,
  });
  const h = opsHarness();
  const app = createAcquisitionApp({ enabled: true, group: "ops", groupEnabled: true, authenticator, installer: h.installer, log: captured.log });
  return { app, captured, sent, touched: () => h.sql.calls.length };
}
/** An active introspection answer for the ops route's service principal (no sub). */
const active = (over: Record<string, unknown> = {}) => Response.json({
  active: true, email: null, principal_type: "service", machine_id: null, org: "org_test", workspace: "ws_test",
  scopes: ["acquisition:operate"], aud: ORIGIN + "/", iat: Math.floor(Date.now() / 1000), exp: Math.floor(Date.now() / 1000) + 600, token_id: "tok_1", ...over,
});
const request = (app: Parameters<typeof call>[0], token: string) => call(app, "POST", OPS_STATUS, { token, body: opsBody });

test("verifier matrix: prefix x principal_type x scope (ops route)", async () => {
  const prefixes = ["sh_at_", "sh_wk_"] as const;
  const principals = ["desktop-user", "service", "agent", "", undefined];
  const scopeSets = [["rth:acquire"], ["acquisition:operate"], ["rth:acquire", "acquisition:operate"], ["rth:read"], []];
  let cells = 0;
  for (const prefix of prefixes)
    for (const principalType of principals)
      for (const scopes of scopeSets) {
        const answer = { principal_type: principalType, scopes, ...(principalType === "desktop-user" ? { sub: "user_test" } : {}) };
        const r = realApp(() => active(answer));
        const res = await request(r.app, TOKENS[prefix]);
        // Every refusal is 403 (contract P2): wrong token class (before introspection), unknown
        // principal_type (fail closed), a desktop-user principal, or a missing acquisition:operate scope.
        const expected = prefix === "sh_wk_" && principalType === "service" && scopes.includes("acquisition:operate") ? 200 : 403;
        const label = `${prefix} ${String(principalType)} [${scopes.join(",")}]`;
        expect([label, res.status]).toEqual([label, expected]);
        if (expected !== 200) expect([label, r.touched()]).toEqual([label, 0]);
        expect([label, r.sent.length]).toEqual([label, prefix === "sh_wk_" ? 1 : 0]);
        cells++;
      }
  expect(cells).toBe(2 * 5 * 5);
});

test("verifier: #313 wire contract (path, secret header, body has only token and aud = ACQUISITION_ORIGIN)", async () => {
  const r = realApp(() => active());
  await request(r.app, TOKENS.sh_wk_);
  expect(r.sent).toEqual([{ url: "https://relaycontrol.example.test/base/internal/v1/auth/introspect", body: { token: TOKENS.sh_wk_, aud: ORIGIN }, authorization: `Bearer ${INTROSPECT_SECRET}` }]);
});

test("verifier: echoed aud must equal ACQUISITION_ORIGIN by normalized origin", async () => {
  for (const [aud, status] of [[ORIGIN, 200], [ORIGIN.toUpperCase() + "/x", 200], ["https://other.example.test", 403], ["https://history.example.test:8443", 403], ["not a url", 403]] as const) {
    const r = realApp(() => active({ aud }));
    const res = await request(r.app, TOKENS.sh_wk_);
    expect([aud, res.status]).toEqual([aud, status]);
  }
});

test("verifier: inactive, expired, desktop-user without sub; outages and malformed answers are 503", async () => {
  const now = Math.floor(Date.now() / 1000);
  for (const [answer, status] of [
    [() => Response.json({ active: false }), 403],
    [() => active({ exp: now - 1 }), 403],
    [() => active({ principal_type: "desktop-user", scopes: ["acquisition:operate"] }), 503],
    [() => active({ workspace: undefined }), 503],
    [() => active({ scopes: "acquisition:operate" }), 503],
    [() => new Response("x".repeat(17 * 1024), { status: 200 }), 503],
    [() => new Response("{not json", { status: 200 }), 503],
    [() => new Response(null, { status: 401 }), 503],
    [() => new Response(null, { status: 404 }), 503],
    [() => new Response(null, { status: 302, headers: { location: "https://evil.example.test" } }), 503],
    [() => { throw new TypeError("network"); }, 503],
    [() => new Promise<Response>(() => {}), 503],
  ] as const) {
    const r = realApp(answer as () => Response, 30);
    const res = await request(r.app, TOKENS.sh_wk_);
    expect(res.status).toBe(status);
    if (status === 503) expect(res.json).toEqual({ error: { code: "introspection_unavailable" } });
    expect(r.touched()).toBe(0);
    expect(leaks(r.captured.lines.map((l) => JSON.stringify(l)))).toEqual([]);
  }
});

test("verifier: config refuses an empty secret and a non-http origin", () => {
  expect(() => createAcquisitionVerifier({ url: "https://relaycontrol.example.test", secret: " ", origin: ORIGIN })).toThrow();
  expect(() => createAcquisitionVerifier({ url: "https://relaycontrol.example.test", secret: "s", origin: "ftp://x" })).toThrow();
  expect(() => createAcquisitionVerifier({ url: "https://u:p@relaycontrol.example.test", secret: "s", origin: ORIGIN })).toThrow();
});
