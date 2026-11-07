/**
 * The read API's relaycontrol path (RELAYHISTORY_ACCEPT_RELAYCONTROL_TOKENS):
 * `sh_at_` bearers are introspected with aud = READ_API_ORIGIN and the
 * request's single `workspace=` when given (omitted otherwise; the response's
 * workspace is authoritative); RelayAuth bearers keep relayAuthVerifier.
 * Synthetic ids and secret only; relaycontrol is a stub fetch.
 */
import { describe, expect, test } from "bun:test";
import { createRelaycontrolIntrospector } from "@rhc/auth/relaycontrol.ts";
import { AuthError, createReadApi, type Verifier } from "../app.ts";
import { readApiVerifier, type AuthConfig } from "../auth.ts";
import { readApiAuthConfig } from "../lambda.ts";
import { memoryLiveness } from "./support.ts";

const ORIGIN = "https://read.example.test";
const TOKEN = "sh_at_synthetic-read-token";
const config: AuthConfig = { origin: ORIGIN, relaycontrol: { url: "http://relaycontrol.test", secret: "synthetic-secret" } };

function active(over: Record<string, unknown> = {}) {
  const now = Math.floor(Date.now() / 1000);
  return { active: true, sub: "u-1", email: "a@example.test", principal_type: "desktop-user", org: "org-a", workspace: "ws-a", scopes: ["cli:auth", "rth:read"], machine_id: null, aud: ORIGIN, iat: now, exp: now + 600, token_id: "tokid_1", ...over };
}

function stub(answer: (body: { aud: string; workspace?: string }) => Response | Promise<Response>) {
  const calls: { aud: string; workspace?: string }[] = [];
  const introspector = async (opts: { url: string; secret: string }) =>
    createRelaycontrolIntrospector({
      ...opts,
      log: () => {},
      fetch: (async (_url: RequestInfo | URL, init: RequestInit = {}) => {
        const body = JSON.parse(String(init.body));
        calls.push(body);
        return answer(body);
      }) as typeof fetch,
    });
  return { introspector, calls };
}

const relayAuthCalls: string[] = [];
const fakeRelayAuth = (async (token: string) => {
  relayAuthCalls.push(token);
  if (token !== "relayauth-jwt") throw new Error("bad");
  return { userId: "u-1", orgId: "org-a", workspaceId: "ws-a", scopes: ["rth:read"], historyOrigin: ORIGIN, tokenSubject: "s", claims: { org: "org-a", wks: "ws-a", sponsorId: "u-1" } };
}) as never;

async function refusal(verify: Verifier, token: string, workspace?: string) {
  try {
    await verify(token, workspace === undefined ? {} : { workspace });
  } catch (error) {
    return error instanceof AuthError ? `${error.status} ${error.code}` : "other";
  }
  return "admitted";
}

describe("readApiVerifier with relaycontrol", () => {
  test("admits an active token bound to READ_API_ORIGIN and the request workspace, rth scopes only", async () => {
    const { introspector, calls } = stub(() => Response.json(active()));
    const verify = readApiVerifier(config, { introspector });
    expect(await verify(TOKEN, { workspace: "ws-a" })).toEqual({ userId: "u-1", orgId: "org-a", workspaceId: "ws-a", scopes: ["rth:read"] });
    expect(calls).toEqual([{ token: TOKEN, aud: ORIGIN, workspace: "ws-a" } as never]);
  });

  test("refuses mismatches and unavailability, fail closed", async () => {
    const cases: Record<string, [() => Response | never, string]> = {
      inactive: [() => Response.json({ active: false }), "401 unauthorized"],
      aud: [() => Response.json(active({ aud: "https://history.example.test" })), "401 unauthorized"],
      audEquivalent: [() => Response.json(active({ aud: "HTTPS://READ.example.test/" })), "admitted"],
      workspace: [() => Response.json(active({ workspace: "ws-b" })), "401 unauthorized"],
      agentPrincipal: [() => Response.json(active({ principal_type: "agent" })), "401 unauthorized"],
      noPrincipal: [() => Response.json(active({ principal_type: undefined })), "401 unauthorized"],
      rc400: [() => new Response("", { status: 400 }), "401 unauthorized"],
      // Our introspection secret is wrong: an outage, never a token refusal.
      rc401: [() => new Response("", { status: 401 }), "503 relaycontrol_unavailable"],
      rc302: [() => new Response(null, { status: 302, headers: { location: "http://elsewhere.test/" } }), "503 relaycontrol_unavailable"],
      oversized: [() => Response.json({ active: false, reason: "x".repeat(17 * 1024) }), "503 relaycontrol_unavailable"],
      rc500: [() => new Response("", { status: 500 }), "503 relaycontrol_unavailable"],
      malformed: [() => new Response("nope"), "503 relaycontrol_unavailable"],
      network: [() => { throw new TypeError("fetch failed"); }, "503 relaycontrol_unavailable"],
    };
    for (const [name, [answer, expected]] of Object.entries(cases)) {
      const verify = readApiVerifier(config, { introspector: stub(answer).introspector });
      expect(`${name}: ${await refusal(verify, TOKEN, "ws-a")}`).toBe(`${name}: ${expected}`);
    }
  });

  test("without a workspace query, omits workspace and takes the response's", async () => {
    const { introspector, calls } = stub(() => Response.json(active({ workspace: "ws-resp" })));
    expect((await readApiVerifier(config, { introspector })(TOKEN, {})).workspaceId).toBe("ws-resp");
    expect(calls).toEqual([{ token: TOKEN, aud: ORIGIN } as never]);
  });

  test("needs the config before any call", async () => {
    const { introspector, calls } = stub(() => Response.json(active()));
    expect(await refusal(readApiVerifier({ ...config, relaycontrol: { url: "http://relaycontrol.test" } }, { introspector }), TOKEN, "ws-a")).toBe("401 not_configured");
    expect(await refusal(readApiVerifier({ ...config, origin: undefined }, { introspector }), TOKEN, "ws-a")).toBe("401 not_configured");
    expect(calls).toHaveLength(0);
  });

  test("RelayAuth bearers keep their verifier; sh_at_ never reaches it, and is refused there with the flag off", async () => {
    const { introspector, calls } = stub(() => Response.json(active()));
    relayAuthCalls.length = 0;
    const on = readApiVerifier(config, { introspector, verify: fakeRelayAuth });
    expect((await on("relayauth-jwt", {})).workspaceId).toBe("ws-a");
    await on(TOKEN, { workspace: "ws-a" });
    expect(relayAuthCalls).toEqual(["relayauth-jwt"]);
    expect(calls).toHaveLength(1);
    // Flag off: the real RelayAuth verifier refuses the opaque token as before.
    const off = readApiVerifier({ origin: ORIGIN, jwksUrl: "http://127.0.0.1:9/jwks.json" }, { introspector });
    expect(await refusal(off, TOKEN, "ws-a")).toBe("401 invalid_token");
    expect(calls).toHaveLength(1);
  });
});

describe("the read app passes the workspace query and answers 503 with Retry-After", () => {
  const app = (verify: Verifier) =>
    createReadApi({ verify, ch: { database: "x", async command() {}, async insert() {}, async select() { return []; } } as never, liveness: memoryLiveness({}).liveness, completeSinceMs: 0, log: () => {} });

  test("single workspace= is passed; repeated or absent is not", async () => {
    const seen: unknown[] = [];
    const a = app(async (_t, request) => { seen.push(request); throw new AuthError(401, "unauthorized", "no"); });
    const get = (q: string) => a.request(`/v1/events?${q}`, { headers: { authorization: `Bearer ${TOKEN}` } });
    await get("workspace=ws-a&q=x");
    await get("workspace=ws-a&workspace=ws-b&q=x");
    await get("q=x");
    expect(seen).toEqual([{ workspace: "ws-a" }, {}, {}]);
  });

  test("an unavailable introspection is a 503 relaycontrol_unavailable", async () => {
    const a = app(async () => { throw new AuthError(503, "relaycontrol_unavailable", "relaycontrol is unavailable"); });
    const r = await a.request("/v1/events?workspace=ws-a&q=x", { headers: { authorization: `Bearer ${TOKEN}` } });
    expect(r.status).toBe(503);
    expect(r.headers.get("retry-after")).toBe("1");
    expect(((await r.json()) as { error: { code: string } }).error.code).toBe("relaycontrol_unavailable");
  });
});

describe("readApiAuthConfig with the relaycontrol flag", () => {
  test("off (unset or not exactly true): no relaycontrol key, as before", () => {
    for (const flag of [undefined, "1", "TRUE"])
      expect(readApiAuthConfig({ READ_API_ORIGIN: ORIGIN, RELAYHISTORY_ACCEPT_RELAYCONTROL_TOKENS: flag, RELAYCONTROL_INTROSPECT_URL: "https://rc.example.test", RELAYCONTROL_INTROSPECT_SECRET: "s" })).toEqual({ origin: ORIGIN, jwksUrl: undefined, issuer: undefined });
  });

  test("on: URL and secret carried; plain http needs the in-cluster flag and a Service name", () => {
    const on = { READ_API_ORIGIN: ORIGIN, RELAYHISTORY_ACCEPT_RELAYCONTROL_TOKENS: "true", RELAYCONTROL_INTROSPECT_SECRET: "s" };
    expect(readApiAuthConfig({ ...on, RELAYCONTROL_INTROSPECT_URL: "https://rc.example.test" }).relaycontrol).toEqual({ url: "https://rc.example.test", secret: "s" });
    const svc = "http://relaycontrol.relaycontrol.svc:8080";
    expect(() => readApiAuthConfig({ ...on, RELAYCONTROL_INTROSPECT_URL: svc })).toThrow(/RELAYCONTROL_INTROSPECT_URL must use https/);
    expect(readApiAuthConfig({ ...on, RELAYCONTROL_INTROSPECT_URL: svc, ALLOW_IN_CLUSTER_HTTP: "true" }).relaycontrol?.url).toBe(svc);
    expect(readApiAuthConfig({ ...on }).relaycontrol).toEqual({ url: undefined, secret: "s" });
  });
});
