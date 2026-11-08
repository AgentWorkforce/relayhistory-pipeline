// Auth refusal matrix: fake authenticator for the route rules, and the real
// acquisition verifier (verifier.ts, fake fetch) for every cell of
// prefix x principal_type x scope x route, audience, timeout and outages.
import { expect, test } from "bun:test";
import { createAcquisitionApp } from "../app.ts";
import { createAcquisitionVerifier, type AcquisitionGroup } from "../verifier.ts";
import { FakeAuthenticator, ORIGIN, call, captureLog, leaks, nativeHarness, opsHarness, opsPrincipal, principal, secrets } from "./helpers.ts";

const NATIVE_STATUS = "/v1/acquisition/native/status?installationId=00000000-0000-4000-8000-0000000000aa&source=droid&sessionIds=s1";
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

test("native refusal matrix", async () => {
  const h = nativeHarness();
  h.auth.tokens.set("sh_at_noscope", principal({ scopes: ["rth:sync"] }));
  h.auth.tokens.set("sh_at_service", principal({ principalType: "service" }));
  h.auth.tokens.set("sh_at_nows", principal({ workspaceId: undefined }));
  h.auth.tokens.set("sh_wk_alice", principal()); // valid desktop principal behind the ops token class
  expect((await call(h.app, "GET", NATIVE_STATUS)).status).toBe(401); // no token
  expect((await call(h.app, "GET", NATIVE_STATUS, { headers: { Authorization: "Basic abc" } })).status).toBe(401);
  expect((await call(h.app, "GET", NATIVE_STATUS, { token: "not-a-relaycontrol-token" })).status).toBe(401);
  expect((await call(h.app, "GET", NATIVE_STATUS, { token: "sh_at_unknown" })).status).toBe(401); // rejected (bad aud etc.)
  expect((await call(h.app, "GET", NATIVE_STATUS, { token: "sh_wk_alice" })).status).toBe(401); // sh_wk_ never reaches introspection on native
  for (const t of ["sh_at_noscope", "sh_at_service", "sh_at_nows"]) {
    const r = await call(h.app, "GET", NATIVE_STATUS, { token: t });
    expect([t, r.status, r.json]).toEqual([t, 403, { error: { code: "not_authorized" } }]);
  }
  const down = await call(h.app, "GET", NATIVE_STATUS, { token: "sh_at_down" });
  expect([down.status, down.json]).toEqual([503, { error: { code: "introspection_unavailable" } }]);
  expect(down.headers.get("Retry-After")).toBe("5");
  expect(h.sql.calls.length).toBe(0);
  expect(h.store.calls.length).toBe(0);
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
function realApp(group: AcquisitionGroup, answer: (body: Record<string, unknown>) => Response | Promise<Response>, timeoutMs = 50) {
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
  if (group === "native") {
    const h = nativeHarness();
    const app = createAcquisitionApp({ enabled: true, group, groupEnabled: true, authenticator, issuer: h.issuer, log: captured.log });
    return { app, captured, sent, touched: () => h.sql.calls.length + h.store.calls.length };
  }
  const h = opsHarness();
  const app = createAcquisitionApp({ enabled: true, group, groupEnabled: true, authenticator, installer: h.installer, log: captured.log });
  return { app, captured, sent, touched: () => h.sql.calls.length };
}
const active = (over: Record<string, unknown> = {}) => Response.json({
  active: true, sub: "user_test", email: null, principal_type: "desktop-user", machine_id: null, org: "org_test", workspace: "ws_test",
  scopes: ["rth:acquire", "rth:sync"], aud: ORIGIN + "/", iat: Math.floor(Date.now() / 1000), exp: Math.floor(Date.now() / 1000) + 600, token_id: "tok_1", ...over,
});
const request = (group: AcquisitionGroup, app: Parameters<typeof call>[0], token: string) =>
  group === "native" ? call(app, "GET", NATIVE_STATUS, { token }) : call(app, "POST", OPS_STATUS, { token, body: opsBody });

const passed = (group: AcquisitionGroup, res: { status: number; json: unknown }) =>
  group === "native"
    ? res.status === 403 && JSON.stringify(res.json) === JSON.stringify({ error: { code: "installation_not_proven" } })
    : res.status === 200;

test("verifier matrix: prefix x principal_type x scope x route", async () => {
  const prefixes = ["sh_at_", "sh_wk_"] as const;
  const principals = ["desktop-user", "service", "agent", "", undefined];
  const scopeSets = [["rth:acquire"], ["acquisition:operate"], ["rth:acquire", "acquisition:operate"], ["rth:read"], []];
  let cells = 0;
  for (const group of ["native", "ops"] as const)
    for (const prefix of prefixes)
      for (const principalType of principals)
        for (const scopes of scopeSets) {
          const want = group === "native"
            ? { prefix: "sh_at_", principalType: "desktop-user", scope: "rth:acquire", refused: 401 }
            : { prefix: "sh_wk_", principalType: "service", scope: "acquisition:operate", refused: 403 };
          const answer = { principal_type: principalType, scopes, ...(principalType === "service" ? { sub: undefined } : {}) };
          const r = realApp(group, () => active(answer));
          const res = await request(group, r.app, TOKENS[prefix]);
          let expected: number;
          if (prefix !== want.prefix) expected = want.refused; // wrong token class: refused before introspection
          else if (principalType === undefined || principalType === "" || principalType === "agent") expected = want.refused; // unknown: fail closed
          else if (principalType !== want.principalType || !scopes.includes(want.scope)) expected = 403;
          else expected = 200;
          const label = `${group} ${prefix} ${String(principalType)} [${scopes.join(",")}]`;
          // A pass reaches the route: native status then asks for the installation proof
          // (403 installation_not_proven), ops status answers 200. Every auth refusal touches no SQL.
          if (expected === 200) expect([label, passed(group, res)]).toEqual([label, true]);
          else {
            expect([label, res.status]).toEqual([label, expected]);
            expect([label, r.touched()]).toEqual([label, 0]);
          }
          expect([label, r.sent.length]).toEqual([label, prefix === want.prefix ? 1 : 0]);
          cells++;
        }
  expect(cells).toBe(2 * 2 * 5 * 5);
});

test("verifier: #313 wire contract (path, secret header, body has only token and aud = ACQUISITION_ORIGIN)", async () => {
  const r = realApp("ops", () => active({ principal_type: "service", scopes: ["acquisition:operate"], sub: undefined }));
  await request("ops", r.app, TOKENS.sh_wk_);
  expect(r.sent).toEqual([{ url: "https://relaycontrol.example.test/base/internal/v1/auth/introspect", body: { token: TOKENS.sh_wk_, aud: ORIGIN }, authorization: `Bearer ${INTROSPECT_SECRET}` }]);
});

test("verifier: echoed aud must equal ACQUISITION_ORIGIN by normalized origin", async () => {
  for (const [aud, status] of [[ORIGIN, 200], [ORIGIN.toUpperCase() + "/x", 200], ["https://other.example.test", 401], ["https://history.example.test:8443", 401], ["not a url", 401]] as const) {
    const r = realApp("native", () => active({ aud }));
    const res = await request("native", r.app, TOKENS.sh_at_);
    expect([aud, passed("native", res) ? 200 : res.status]).toEqual([aud, status]);
  }
});

test("verifier: inactive, expired, desktop-user without sub; outages and malformed answers are 503", async () => {
  const now = Math.floor(Date.now() / 1000);
  for (const [answer, status] of [
    [() => Response.json({ active: false }), 401],
    [() => active({ exp: now - 1 }), 401],
    [() => active({ sub: undefined }), 503],
    [() => active({ workspace: undefined }), 503],
    [() => active({ scopes: "rth:acquire" }), 503],
    [() => new Response("x".repeat(17 * 1024), { status: 200 }), 503],
    [() => new Response("{not json", { status: 200 }), 503],
    [() => new Response(null, { status: 401 }), 503],
    [() => new Response(null, { status: 404 }), 503],
    [() => new Response(null, { status: 302, headers: { location: "https://evil.example.test" } }), 503],
    [() => { throw new TypeError("network"); }, 503],
    [() => new Promise<Response>(() => {}), 503],
  ] as const) {
    const r = realApp("native", answer as () => Response, 30);
    const res = await request("native", r.app, TOKENS.sh_at_);
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
