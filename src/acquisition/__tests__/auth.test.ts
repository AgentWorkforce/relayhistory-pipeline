// Auth refusal matrix: fake authenticator for the principal/scope rules, and the
// real #313 client (fake fetch) for audience, timeout and the allowlist blocker.
import { expect, test } from "bun:test";
import { createRelaycontrolIntrospector } from "@rhc/auth/relaycontrol.ts";
import { createAcquisitionApp, relaycontrolAuthenticator } from "../app.ts";
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
  expect((await call(app, "POST", OPS_STATUS, { token: "sh_at_ops", body: opsBody })).status).toBe(503);
});

test("native refusal matrix", async () => {
  const h = nativeHarness();
  h.auth.tokens.set("sh_at_noscope", principal({ scopes: ["rth:sync"] }));
  h.auth.tokens.set("sh_at_service", principal({ principalType: "service" }));
  h.auth.tokens.set("sh_at_nows", principal({ workspaceId: undefined }));
  expect((await call(h.app, "GET", NATIVE_STATUS)).status).toBe(401); // no token
  expect((await call(h.app, "GET", NATIVE_STATUS, { headers: { Authorization: "Basic abc" } })).status).toBe(401);
  expect((await call(h.app, "GET", NATIVE_STATUS, { token: "not-a-relaycontrol-token" })).status).toBe(401);
  expect((await call(h.app, "GET", NATIVE_STATUS, { token: "sh_at_unknown" })).status).toBe(401); // rejected (bad aud etc.)
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
  h.auth.tokens.set("sh_at_desktop", principal({ scopes: ["acquisition:operate"] })); // desktop-user
  h.auth.tokens.set("sh_at_wrongscope", opsPrincipal({ scopes: ["rth:acquire"] }));
  expect((await call(h.app, "POST", OPS_STATUS, { body: opsBody })).status).toBe(401);
  for (const t of ["sh_at_unknown", "sh_at_desktop", "sh_at_wrongscope", "bogus"]) {
    const r = await call(h.app, "POST", OPS_STATUS, { token: t, body: opsBody });
    expect([t, r.status, r.json]).toEqual([t, 403, { error: { code: "not_authorized" } }]);
  }
  expect((await call(h.app, "POST", OPS_STATUS, { token: "sh_at_down", body: opsBody })).status).toBe(503);
  expect(h.sql.calls.length).toBe(0);
  // Unknown route, wrong media type, oversized body.
  expect((await call(h.app, "POST", "/v1/acquisition/ops/nope", { token: "sh_at_ops", body: {} })).status).toBe(404);
  expect((await call(h.app, "POST", "/v1/acquisition/native/claims", { token: "sh_at_ops", body: {} })).status).toBe(404);
  expect((await call(h.app, "POST", OPS_STATUS, { token: "sh_at_ops", body: "x", headers: { "Content-Type": "text/plain" } })).status).toBe(415);
  expect((await call(h.app, "POST", OPS_STATUS, { token: "sh_at_ops", body: "x".repeat(70 * 1024) })).status).toBe(413);
  expect((await call(h.app, "POST", OPS_STATUS, { token: "sh_at_ops", body: "{not json" })).status).toBe(400);
});

// ------------------------------------------------------------ real #313 client

const INTROSPECT_SECRET = "introspect-secret-for-tests-only";
secrets.add(INTROSPECT_SECRET);
secrets.add("sh_at_realtoken");

function realApp(answer: (body: Record<string, unknown>) => Response | Promise<Response>, timeoutMs = 50) {
  const captured = captureLog();
  const introspector = createRelaycontrolIntrospector({
    url: "https://relaycontrol.example.test/v1/introspect",
    secret: INTROSPECT_SECRET,
    timeoutMs,
    fetch: (async (_url: string, init: RequestInit) => {
      const signal = init.signal;
      return Promise.race([
        answer(JSON.parse(String(init.body))),
        new Promise<Response>((_, reject) => signal?.addEventListener("abort", () => reject(signal.reason ?? new Error("aborted")))),
      ]);
    }) as unknown as typeof fetch,
    log: (level, message, fields) => captured.lines.push({ level, message, fields }),
  });
  const h = nativeHarness();
  const app = createAcquisitionApp({ enabled: true, group: "native", groupEnabled: true, authenticator: relaycontrolAuthenticator(introspector, ORIGIN), issuer: h.issuer, log: captured.log });
  return { app, captured, h };
}
const active = (over: Record<string, unknown> = {}) => Response.json({
  active: true, sub: "user_test", email: null, principal_type: "desktop-user", machine_id: null, org: "org_test", workspace: "ws_test",
  scopes: ["rth:acquire", "rth:sync"], aud: ORIGIN + "/", iat: Math.floor(Date.now() / 1000), exp: Math.floor(Date.now() / 1000) + 600, token_id: "tok_1", ...over,
});

test("real #313 client: aud is sent as ACQUISITION_ORIGIN; a different echoed origin is 401", async () => {
  let sentAud: unknown;
  const { app } = realApp((b) => { sentAud = b.aud; return active({ aud: "https://other.example.test" }); });
  expect((await call(app, "GET", NATIVE_STATUS, { token: "sh_at_realtoken" })).status).toBe(401);
  expect(sentAud).toBe(ORIGIN);
});

test("real #313 client: introspection timeout fails closed with 503", async () => {
  const { app, h } = realApp(() => new Promise<Response>(() => {}), 30);
  const r = await call(app, "GET", NATIVE_STATUS, { token: "sh_at_realtoken" });
  expect([r.status, r.json]).toEqual([503, { error: { code: "introspection_unavailable" } }]);
  expect(h.sql.calls.length + h.store.calls.length).toBe(0);
});

test("real #313 client today: rth:acquire is filtered by its allowlist (403), service principals are rejected (401)", async () => {
  const one = realApp(() => active());
  expect((await call(one.app, "GET", NATIVE_STATUS, { token: "sh_at_realtoken" })).status).toBe(403);
  const two = realApp(() => active({ principal_type: "service", scopes: ["acquisition:operate"] }));
  expect((await call(two.app, "GET", NATIVE_STATUS, { token: "sh_at_realtoken" })).status).toBe(401);
  const lines = [...one.captured.lines, ...two.captured.lines].map((l) => JSON.stringify(l));
  expect(lines.length).toBeGreaterThan(0);
  expect(leaks(lines)).toEqual([]);
});
