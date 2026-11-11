/**
 * The Node ingest edge skeleton (ingest-edge.ts) against an in-process fake relay-control
 * introspection server (node:http on loopback): flag parsing, the not-mounted default, the happy path
 * through the real ingest app, every refusal, `rth_*` never sent, no cache, and the header strip.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { IDENTITY_HEADER, SIGNATURE_HEADER, contentEncodings, encodeIdentity, type IngestIdentity } from "../app.ts";
import { DELIVERY_LIMITS } from "../vendor/delivery/delivery-contracts.ts";
import {
  INGEST_EDGE_FLAG, createIngestEdge, edgeIdentify, ingestEdgeConfig, ingestEdgeEnabled, mountIngestEdge,
  type EdgeHandlerIdentify, type IngestEdgeAppDeps,
} from "../ingest-edge.ts";
import { TENANT } from "./support.ts";

const ORIGIN = "https://history.edge.test";
const SECRET = "introspect-secret-test";
const TOKEN = "sh_at_desktop-token-1";

const cleanups: (() => unknown)[] = [];
afterEach(async () => {
  for (const f of cleanups.splice(0).reverse()) await f();
});

type Answer = { status: number; body?: unknown } | "hang";
/** The fake introspection endpoint, scripted per call; records each call. */
async function relaycontrol(answer: (body: Record<string, unknown>) => Answer) {
  const calls: { body: Record<string, unknown>; authorization: string | undefined; path: string }[] = [];
  const server: Server = createServer((req, res) => {
    const chunks: Buffer[] = [];
    req.on("data", (c) => chunks.push(c));
    req.on("end", () => {
      const body = JSON.parse(Buffer.concat(chunks).toString("utf8")) as Record<string, unknown>;
      calls.push({ body, authorization: req.headers.authorization, path: req.url ?? "" });
      const a = answer(body);
      if (a === "hang") return;
      res.writeHead(a.status, { "content-type": "application/json" });
      res.end(a.body === undefined ? "" : JSON.stringify(a.body));
    });
  });
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
  let closed = false;
  const close = () => (closed ? undefined : ((closed = true), new Promise<void>((r) => { server.closeAllConnections?.(); server.close(() => r()); })));
  cleanups.push(close);
  return { url: `http://127.0.0.1:${(server.address() as AddressInfo).port}`, calls, close };
}

const now = () => Math.floor(Date.now() / 1000);
const active = (scopes: string[], over: Record<string, unknown> = {}) => ({
  status: 200,
  body: { active: true, sub: TENANT.userId, email: null, principal_type: "desktop-user", machine_id: null, org: TENANT.orgId, workspace: TENANT.workspaceId, scopes, aud: ORIGIN, iat: now() - 10, exp: now() + 600, token_id: "tok-1", ...over },
});

const env = (introspectUrl: string, over: NodeJS.ProcessEnv = {}): NodeJS.ProcessEnv => ({
  [INGEST_EDGE_FLAG]: "true",
  RELAYHISTORY_INGEST_ORIGIN: ORIGIN,
  RELAYCONTROL_INTROSPECT_URL: introspectUrl,
  RELAYCONTROL_INTROSPECT_SECRET: SECRET,
  ...over,
});

/** The ingest app's deps: the limits route never touches the ledger or store. */
const DEPS = { deliver: {} as IngestEdgeAppDeps["deliver"], log: () => {} } as IngestEdgeAppDeps;

const req = (path: string, init: RequestInit & { headers?: Record<string, string> } = {}) =>
  new Request(`${ORIGIN}${path}`, { ...init, headers: { authorization: `Bearer ${TOKEN}`, ...(init.headers ?? {}) } });

/** An in-process handler that records what it saw, resolving identity only through the edge's seam. */
function recorder() {
  const seen: { headers: Record<string, string>; identity: IngestIdentity | Error; body: string; method: string; url: string }[] = [];
  let identifyRef: EdgeHandlerIdentify | undefined;
  const make = (identify: EdgeHandlerIdentify) => {
    identifyRef = identify;
    return {
      async fetch(request: Request) {
        let identity: IngestIdentity | Error;
        try {
          identity = identify(request);
        } catch (e) {
          identity = e as Error;
        }
        seen.push({ headers: Object.fromEntries(request.headers), identity, body: await request.text(), method: request.method, url: request.url });
        return new Response("ok", { status: 202, headers: { "x-from-handler": "1" } });
      },
    };
  };
  return { seen, make, identify: () => identifyRef! };
}

async function json(res: Response) {
  return (await res.json()) as { error?: { code: string; message: string }; correlationId?: string } & Record<string, unknown>;
}

describe("flag", () => {
  test("unset, empty and false are off; exactly true is on; anything else refuses, without echoing the value", () => {
    expect(ingestEdgeEnabled({})).toBe(false);
    expect(ingestEdgeEnabled({ [INGEST_EDGE_FLAG]: "" })).toBe(false);
    expect(ingestEdgeEnabled({ [INGEST_EDGE_FLAG]: "false" })).toBe(false);
    expect(ingestEdgeEnabled({ [INGEST_EDGE_FLAG]: "true" })).toBe(true);
    for (const bad of ["1", "TRUE", "yes", " true", "on"]) {
      expect(() => ingestEdgeEnabled({ [INGEST_EDGE_FLAG]: bad })).toThrow(INGEST_EDGE_FLAG);
      try {
        ingestEdgeEnabled({ [INGEST_EDGE_FLAG]: bad });
      } catch (e) {
        expect((e as Error).message).not.toContain(`"${bad}"`);
      }
    }
  });

  test("off: not mounted, and no other variable is read (even malformed ones)", () => {
    for (const flag of [undefined, "", "false"]) {
      const e: NodeJS.ProcessEnv = { RELAYCONTROL_INTROSPECT_URL: "not a url", RELAYHISTORY_INGEST_ORIGIN: "nope" };
      if (flag !== undefined) e[INGEST_EDGE_FLAG] = flag;
      expect(ingestEdgeConfig(e)).toEqual({ enabled: false });
      expect(mountIngestEdge(e, DEPS)).toBeNull();
    }
    expect(() => mountIngestEdge({ [INGEST_EDGE_FLAG]: "on" }, DEPS)).toThrow(INGEST_EDGE_FLAG);
  });

  test("on: every variable is required and checked, naming the variable, never the value", () => {
    const base = env("http://127.0.0.1:9");
    expect(ingestEdgeConfig(base)).toEqual({ enabled: true, origin: ORIGIN, introspectUrl: "http://127.0.0.1:9", introspectSecret: SECRET });
    for (const name of ["RELAYHISTORY_INGEST_ORIGIN", "RELAYCONTROL_INTROSPECT_URL", "RELAYCONTROL_INTROSPECT_SECRET"]) {
      expect(() => ingestEdgeConfig({ ...base, [name]: undefined })).toThrow(name);
      expect(() => ingestEdgeConfig({ ...base, [name]: "  " })).toThrow(name);
    }
    expect(() => ingestEdgeConfig({ ...base, RELAYHISTORY_INGEST_ORIGIN: "not-an-origin" })).toThrow("RELAYHISTORY_INGEST_ORIGIN");
    // Destination policy: no plaintext http to a non-loopback host.
    expect(() => ingestEdgeConfig({ ...base, RELAYCONTROL_INTROSPECT_URL: "http://introspect.example.test" })).toThrow("RELAYCONTROL_INTROSPECT_URL");
    expect(() => ingestEdgeConfig({ ...base, RELAYCONTROL_INTROSPECT_URL: "https://u:p@introspect.example.test" })).toThrow("RELAYCONTROL_INTROSPECT_URL");
    try {
      ingestEdgeConfig({ ...base, RELAYCONTROL_INTROSPECT_URL: "https://u:p@introspect.example.test" });
    } catch (e) {
      expect((e as Error).message).not.toContain("u:p");
    }
  });
});

describe("mounted edge over the real ingest app", () => {
  test("happy path: sh_at_ introspected once (no cache), identity mapped, limits answered unchanged", async () => {
    const rc = await relaycontrol(() => active(["rth:sync"]));
    const edge = mountIngestEdge(env(rc.url), DEPS)!;
    expect(edge).not.toBeNull();
    for (let i = 0; i < 2; i++) {
      const res = await edge.fetch(req("/v1/delivery/limits", { headers: { "x-correlation-id": "corr-1" } }));
      expect(res.status).toBe(200);
      expect(res.headers.get("x-correlation-id")).toBe("corr-1");
      expect(await res.json()).toEqual(JSON.parse(JSON.stringify({ ...DELIVERY_LIMITS, contentEncodings: contentEncodings() })));
    }
    // No cache: each request is its own introspection call.
    expect(rc.calls.length).toBe(2);
    expect(rc.calls[0]!.path).toBe("/internal/v1/auth/introspect");
    expect(rc.calls[0]!.authorization).toBe(`Bearer ${SECRET}`);
    expect(rc.calls[0]!.body).toMatchObject({ token: TOKEN, aud: ORIGIN });
  });

  test("a request that bypasses the edge, even with a well-formed identity header, never reaches the handler's tenant", async () => {
    // The handler's identify trusts only requests the edge bound; prove with a recorder sharing the seam.
    const rec = recorder();
    const rc = await relaycontrol(() => active(["rth:sync"]));
    createIngestEdge(edgeIdentify(ingestEdgeConfig(env(rc.url)) as never, env(rc.url)), rec.make);
    const forged = new Request(`${ORIGIN}/v1/delivery/limits`, { headers: { [IDENTITY_HEADER]: encodeIdentity({ ...TENANT, scopes: ["rth:sync"] }) } });
    expect(() => rec.identify()(forged)).toThrow();
    expect(rc.calls.length).toBe(0);
  });
});

describe("refusals", () => {
  async function edgeFor(answer: (body: Record<string, unknown>) => Answer, timeoutMs = 500) {
    const rc = await relaycontrol(answer);
    const rec = recorder();
    const settings = ingestEdgeConfig(env(rc.url));
    if (!settings.enabled) throw new Error("expected enabled");
    const edge = createIngestEdge(edgeIdentify(settings, env(rc.url), { timeoutMs }), rec.make, { random: () => 0.5 });
    return { rc, rec, edge };
  }

  test("missing or malformed Authorization: 401 missing_authorization, nothing introspected", async () => {
    const { rc, rec, edge } = await edgeFor(() => active(["rth:sync"]));
    for (const headers of [{}, { authorization: "Basic abc" }, { authorization: "Bearer" }]) {
      const res = await edge.fetch(new Request(`${ORIGIN}/v1/delivery/limits`, { headers }));
      expect(res.status).toBe(401);
      expect(res.headers.get("www-authenticate")).toBe("Bearer");
      expect((await json(res)).error?.code).toBe("missing_authorization");
    }
    expect(rc.calls.length).toBe(0);
    expect(rec.seen.length).toBe(0);
  });

  test("rth_at_ (and every rth_ class) is refused 401 unsupported_token and never sent to introspection", async () => {
    const { rc, rec, edge } = await edgeFor(() => active(["rth:sync"]));
    for (const token of ["rth_at_abc", "rth_st_abc", "rth_rt_abc"]) {
      const res = await edge.fetch(new Request(`${ORIGIN}/v1/delivery/batches`, { method: "POST", headers: { authorization: `Bearer ${token}` }, body: "{}" }));
      expect(res.status).toBe(401);
      const b = await json(res);
      expect(b.error?.code).toBe("unsupported_token");
      expect(JSON.stringify(b)).not.toContain(token);
    }
    expect(rc.calls.length).toBe(0);
    expect(rec.seen.length).toBe(0);
  });

  test("another token class: 401 invalid_token, never sent", async () => {
    const { rc, edge } = await edgeFor(() => active(["rth:sync"]));
    const res = await edge.fetch(new Request(`${ORIGIN}/v1/delivery/limits`, { headers: { authorization: "Bearer eyJhbGciOi.jwt.x" } }));
    expect(res.status).toBe(401);
    expect((await json(res)).error?.code).toBe("invalid_token");
    expect(rc.calls.length).toBe(0);
  });

  test("inactive: 401 invalid_token", async () => {
    const { rec, edge } = await edgeFor(() => ({ status: 200, body: { active: false, reason: "revoked" } }));
    const res = await edge.fetch(req("/v1/delivery/limits"));
    expect(res.status).toBe(401);
    expect((await json(res)).error?.code).toBe("invalid_token");
    expect(rec.seen.length).toBe(0);
  });

  test("expired: 401 token_expired", async () => {
    const { edge } = await edgeFor(() => ({ status: 200, body: { active: false, reason: "expired" } }));
    const res = await edge.fetch(req("/v1/delivery/limits"));
    expect(res.status).toBe(401);
    expect((await json(res)).error?.code).toBe("token_expired");
  });

  test("wrong audience: 401 invalid_token", async () => {
    const { edge } = await edgeFor(() => active(["rth:sync"], { aud: "https://other.edge.test" }));
    const res = await edge.fetch(req("/v1/delivery/limits"));
    expect(res.status).toBe(401);
    expect((await json(res)).error?.code).toBe("invalid_token");
  });

  test("no rth:sync: 403 forbidden", async () => {
    const { rec, edge } = await edgeFor(() => active(["rth:read"]));
    const res = await edge.fetch(req("/v1/delivery/limits"));
    expect(res.status).toBe(403);
    expect((await json(res)).error?.code).toBe("forbidden");
    expect(rec.seen.length).toBe(0);
  });

  test("unreachable, timeout, 5xx and a refused service secret: 503 relaycontrol_unavailable, jittered Retry-After, never 401, no retry", async () => {
    const cases: [string, (body: Record<string, unknown>) => Answer][] = [
      ["5xx", () => ({ status: 502, body: { error: "bad gateway" } })],
      ["timeout", () => "hang"],
      ["service secret refused", () => ({ status: 401, body: { error: "unauthorized" } })],
      ["malformed answer", () => ({ status: 200, body: { hello: "world" } })],
    ];
    for (const [, answer] of cases) {
      const { rc, rec, edge } = await edgeFor(answer, 200);
      const res = await edge.fetch(req("/v1/delivery/limits"));
      expect(res.status).toBe(503);
      expect((await json(res)).error?.code).toBe("relaycontrol_unavailable");
      expect(res.headers.get("retry-after")).toBe("8"); // 5 + floor(0.5 * 6)
      expect(res.headers.get("www-authenticate")).toBeNull();
      expect(rc.calls.length).toBe(1);
      expect(rec.seen.length).toBe(0);
    }
    // Down: nothing listening at all.
    const { rc, edge } = await edgeFor(() => active(["rth:sync"]));
    await rc.close();
    const res = await edge.fetch(req("/v1/delivery/limits"));
    expect(res.status).toBe(503);
    expect((await json(res)).error?.code).toBe("relaycontrol_unavailable");
    const ra = Number(res.headers.get("retry-after"));
    expect(ra).toBeGreaterThanOrEqual(5);
    expect(ra).toBeLessThanOrEqual(10);
  });

  test("Retry-After jitter stays within 5..10 at both ends of the random source", async () => {
    for (const [r, want] of [[0, "5"], [0.999999, "10"]] as const) {
      const rec = recorder();
      const edge = createIngestEdge(async () => { throw new Error("boom"); }, rec.make, { random: () => r });
      const res = await edge.fetch(req("/v1/delivery/limits"));
      expect(res.status).toBe(503);
      expect(res.headers.get("retry-after")).toBe(want);
    }
  });

  test("routes: /internal and unknown paths 404, other /v1 501, wrong method 405, none introspected", async () => {
    const { rc, rec, edge } = await edgeFor(() => active(["rth:sync"]));
    for (const [path, method, status, code] of [
      ["/internal/v1/session-erase", "POST", 404, "not_found"],
      ["/healthz", "GET", 404, "not_found"],
      ["/v1/custody/x", "POST", 501, "unsupported_by_backend"],
      ["/v1/delivery/recovery", "POST", 501, "unsupported_by_backend"],
      ["/v1/delivery/batches", "GET", 405, "method_not_allowed"],
      ["/v1/delivery/limits", "POST", 405, "method_not_allowed"],
    ] as const) {
      const res = await edge.fetch(req(path, { method, ...(method === "POST" ? { body: "{}" } : {}) }));
      expect(res.status).toBe(status);
      expect((await json(res)).error?.code).toBe(code);
    }
    expect(rc.calls.length).toBe(0);
    expect(rec.seen.length).toBe(0);
  });
});

describe("header strip and pass-through", () => {
  test("spoofed trust headers never reach the handler; identity comes from introspection only; body and encoding unchanged", async () => {
    const rc = await relaycontrol(() => active(["rth:sync"]));
    const rec = recorder();
    const settings = ingestEdgeConfig(env(rc.url));
    if (!settings.enabled) throw new Error("expected enabled");
    const edge = createIngestEdge(edgeIdentify(settings, env(rc.url)), rec.make);
    const spoofed = encodeIdentity({ orgId: "org-evil", workspaceId: "ws-evil", userId: "user-evil", scopes: ["rth:sync", "rth:admin"] });
    const bytes = new Uint8Array([0x1f, 0x8b, 0x08, 0x00, 0xff, 0x00, 0x7f, 0x80]);
    const res = await edge.fetch(new Request(`${ORIGIN}/v1/delivery/batches?x=1`, {
      method: "POST",
      body: bytes,
      headers: {
        authorization: `Bearer ${TOKEN}`,
        "proxy-authorization": "Basic Zm9vOmJhcg==",
        cookie: "session=abc",
        [IDENTITY_HEADER]: spoofed,
        [SIGNATURE_HEADER]: "deadbeef",
        "X-Relay-Ingest-Admission": "queued-v1",
        "x-relay-ingest-anything": "spoof",
        "content-encoding": "gzip",
        "content-type": "application/json",
        "x-correlation-id": "corr-strip",
      },
    }));
    expect(res.status).toBe(202);
    expect(res.headers.get("x-from-handler")).toBe("1");
    expect(rec.seen.length).toBe(1);
    const seen = rec.seen[0]!;
    // The handler's identity is the introspected one, never the spoofed one.
    expect(seen.identity).toEqual({ ...TENANT, scopes: ["rth:sync"] });
    expect(JSON.parse(Buffer.from(seen.headers[IDENTITY_HEADER]!, "base64url").toString("utf8"))).toEqual({ ...TENANT, scopes: ["rth:sync"] });
    expect(seen.headers[IDENTITY_HEADER]).not.toBe(spoofed);
    for (const k of Object.keys(seen.headers)) if (k.startsWith("x-relay-ingest-")) expect(k).toBe(IDENTITY_HEADER);
    for (const k of ["authorization", "proxy-authorization", "cookie", SIGNATURE_HEADER]) expect(seen.headers[k]).toBeUndefined();
    // Pass-through: method, path, query, encoding, type, correlation and the exact bytes.
    expect(seen.method).toBe("POST");
    expect(seen.url).toBe(`${ORIGIN}/v1/delivery/batches?x=1`);
    expect(seen.headers["content-encoding"]).toBe("gzip");
    expect(seen.headers["content-type"]).toBe("application/json");
    expect(seen.headers["x-correlation-id"]).toBe("corr-strip");
  });

  test("exact body bytes reach the handler", async () => {
    const rc = await relaycontrol(() => active(["rth:sync"]));
    const got: Uint8Array[] = [];
    const settings = ingestEdgeConfig(env(rc.url));
    if (!settings.enabled) throw new Error("expected enabled");
    const edge = createIngestEdge(edgeIdentify(settings, env(rc.url)), (identify) => ({
      async fetch(request) {
        identify(request);
        got.push(new Uint8Array(await request.arrayBuffer()));
        return new Response(null, { status: 204 });
      },
    }));
    const bytes = new Uint8Array(70_000).map((_, i) => (i * 31) & 0xff);
    const res = await edge.fetch(req("/v1/turns/batches", { method: "POST", body: bytes }));
    expect(res.status).toBe(204);
    expect(got[0]).toEqual(bytes);
  });

  test("the handler refuses a request whose identity header was altered after the edge set it", async () => {
    const rc = await relaycontrol(() => active(["rth:sync"]));
    const settings = ingestEdgeConfig(env(rc.url));
    if (!settings.enabled) throw new Error("expected enabled");
    let outcome: unknown;
    const edge = createIngestEdge(edgeIdentify(settings, env(rc.url)), (identify) => ({
      async fetch(request) {
        request.headers.set(IDENTITY_HEADER, encodeIdentity({ orgId: "org-evil", workspaceId: "ws-evil", userId: "u", scopes: ["rth:sync"] }));
        try {
          outcome = identify(request);
        } catch (e) {
          outcome = e;
        }
        return new Response(null, { status: 204 });
      },
    }));
    await edge.fetch(req("/v1/delivery/limits"));
    expect(outcome).toBeInstanceOf(Error);
  });
});
