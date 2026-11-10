import { describe, expect, test } from "bun:test";
import { Hono } from "hono";
import { createPipelineRoutes, manifestKey, type AppEnv } from "../../shared/vendor.ts";
import { createCourierManifestRoutes } from "../manifests.ts";
import { PUBLIC_INGEST_PATHS, mountIngestApi } from "../routes.ts";
import { resetSharedRelaycontrolIntrospectors } from "@rhc/auth/relaycontrol.ts";

/**
 * The public ALB contract (docs/aws-stage.md "Ingest exposure"): only health
 * checks and the public JWKS answer without credentials. Every other route the
 * ingest mounts must refuse a request with no token, or a token that is not a
 * RelayAuth JWT, before touching any dependency (the deps below throw).
 */
const boom = () => {
  throw new Error("dependency reached before authentication");
};
const deps = new Proxy({}, { get: () => boom }) as never;

function app() {
  const a = new Hono<AppEnv>();
  for (const path of PUBLIC_INGEST_PATHS) a.get(path, (c) => c.json({ ok: true }));
  mountIngestApi(a, { courierManifests: createCourierManifestRoutes(deps), pipeline: createPipelineRoutes(deps) }, new Hono<AppEnv>(), { ampTransportToken: () => "fixture-platform-amp-transport-token" });
  return a;
}

const env = { ALLOW_INTERIM_RELAYAUTH_BEARER: "true", RELAYAUTH_JWKS_URL: "http://127.0.0.1:9/.well-known/jwks.json", RELAYAUTH_ISSUER: "https://relayauth.example", PIPELINE_OPS_SECRET: "ops-secret-for-test" };
const concrete = (path: string) => path.replace(/:([A-Za-z]+)(\{[^}]*\})?/g, "00000000-0000-7000-8000-000000000000").replace(/\*/g, "x");

describe("ingest authentication", () => {
  const a = app();
  const routes = a.routes.filter((r) => r.method !== "ALL" && !(PUBLIC_INGEST_PATHS as readonly string[]).includes(r.path));

  test("mounts the ingest, manifest and ops routes (the check covers a real surface)", () => {
    const paths = routes.map((r) => `${r.method} ${r.path}`);
    expect(paths.some((p) => p.includes("/v1/pipeline/chunks"))).toBe(true);
    expect(paths.some((p) => p.includes("/v1/pipeline/manifests"))).toBe(true);
    expect(paths.some((p) => p.includes("/v1/internal/pipeline/"))).toBe(true);
    expect(routes.length).toBeGreaterThan(5);
  });

  test("every non-public route refuses a request without credentials", async () => {
    for (const r of routes) {
      const res = await a.request(concrete(r.path), { method: r.method, headers: { "content-type": "application/json" }, body: r.method === "GET" || r.method === "HEAD" ? undefined : "{}" }, env);
      expect({ route: `${r.method} ${r.path}`, status: res.status }).toEqual({ route: `${r.method} ${r.path}`, status: 401 });
    }
  });

  test("every non-public route refuses a bearer that is not a valid RelayAuth JWT", async () => {
    for (const r of routes) {
      const res = await a.request(concrete(r.path), { method: r.method, headers: { authorization: "Bearer not-a-jwt", "content-type": "application/json" }, body: r.method === "GET" || r.method === "HEAD" ? undefined : "{}" }, env);
      expect({ route: `${r.method} ${r.path}`, status: res.status }).toEqual({ route: `${r.method} ${r.path}`, status: 401 });
    }
  });

  test("only health checks and the JWKS are public", () => {
    expect([...PUBLIC_INGEST_PATHS].sort()).toEqual(["/.well-known/jwks.json", "/health", "/healthz", "/readyz"]);
  });
});

describe("tenant org allowlist (TENANT_ORG_IDS)", () => {
  const { orgAllowlist, allowedOrgIdsFromEnv } = require("../routes.ts") as typeof import("../routes.ts");
  const withAuth = (orgId?: string) => {
    const a = new Hono<AppEnv>();
    a.use("*", async (c, next) => {
      if (orgId) c.set("auth" as never, { orgId } as never);
      await next();
    });
    a.use("*", orgAllowlist(["org_aws_proof_1", "org_aws_proof_2"]));
    a.get("/v1/x", (c) => c.json({ ok: true }));
    return a;
  };
  test("admits listed orgs and refuses any other authenticated org with 403", async () => {
    expect((await withAuth("org_aws_proof_1").request("/v1/x")).status).toBe(200);
    expect((await withAuth("org_aws_proof_2").request("/v1/x")).status).toBe(200);
    const r = await withAuth("org_other").request("/v1/x");
    expect(r.status).toBe(403);
    expect(((await r.json()) as { error: { code: string } }).error.code).toBe("org_not_allowed");
    expect((await withAuth(undefined).request("/v1/x")).status).toBe(403);
  });
  test("parses TENANT_ORG_IDS; unset means single-tenant (no allowlist)", () => {
    expect(allowedOrgIdsFromEnv("org_a, org_b,")).toEqual(["org_a", "org_b"]);
    expect(allowedOrgIdsFromEnv(undefined)).toEqual([]);
  });
});

describe("multi-tenant ingest", () => {
  test("mounts the allowlist only for multi-tenant deployments, after authentication", async () => {
    const make = (allowedOrgIds?: string[]) => {
      const a = new Hono<AppEnv>();
      mountIngestApi(a, { courierManifests: createCourierManifestRoutes(deps), pipeline: createPipelineRoutes(deps) }, new Hono<AppEnv>(), { allowedOrgIds });
      return a;
    };
    // Unauthenticated: 401 either way (auth runs first).
    expect((await make(["org_a"]).request("/v1/pipeline/events", { method: "POST", body: "{}" }, env)).status).toBe(401);
    const src = require("node:fs").readFileSync(new URL("../routes.ts", import.meta.url), "utf8") as string;
    const auth = src.indexOf('v1.use("*", async (c, next) => requireAuth(c as never, next));');
    const allow = src.indexOf('if (opts.allowedOrgIds?.length) v1.use("*", orgAllowlist(opts.allowedOrgIds));');
    expect(auth).toBeGreaterThan(0);
    expect(allow).toBeGreaterThan(auth);
    const ingest = require("node:fs").readFileSync(new URL("../ingest.ts", import.meta.url), "utf8") as string;
    expect(ingest).toContain("allowedOrgIds: allowedOrgIdsFromEnv(process.env.TENANT_ORG_IDS)");
  });
});

/**
 * Relaycontrol user access tokens (`sh_at_`, RELAYHISTORY_ACCEPT_RELAYCONTROL_TOKENS=true),
 * contract v1.1 machine verification. The caller's X-Machine-Id is forwarded
 * to relaycontrol as `machine`; the machine claim the unchanged live-write
 * machine gate reads comes only from relaycontrol's verified `machine_id`.
 * Relaycontrol is a stub fetch; synthetic values only.
 */
describe("relaycontrol tokens at the ingest", () => {
  const ORIGIN = "https://history.example.test";
  const INTROSPECT = "http://relaycontrol.test/internal/v1/auth/introspect";
  const rcEnv = {
    RELAYHISTORY_ORIGIN: ORIGIN,
    RELAYAUTH_JWKS_URL: "http://127.0.0.1:9/.well-known/jwks.json",
    RELAYHISTORY_ACCEPT_RELAYCONTROL_TOKENS: "true",
    RELAYCONTROL_INTROSPECT_URL: "http://relaycontrol.test",
    RELAYCONTROL_INTROSPECT_SECRET: "synthetic-secret",
    PIPELINE_OPS_SECRET: "ops-secret-for-test",
  };
  const a = new Hono<AppEnv>();
  mountIngestApi(a, { courierManifests: createCourierManifestRoutes(deps), pipeline: createPipelineRoutes(deps) }, new Hono<AppEnv>(), { relayhistoryOrigin: ORIGIN });
  const v1Routes = a.routes.filter((r) => r.method !== "ALL" && r.path.startsWith("/v1/") && !r.path.startsWith("/v1/internal/"));
  const writes = v1Routes.filter((r) => r.method !== "GET" && r.method !== "HEAD");
  const reads = v1Routes.filter((r) => r.method === "GET");
  type Body = { token: string; aud: string; workspace?: string; machine?: string };
  const verified = (over: Record<string, unknown> = {}) => (body: Body) => {
    const now = Math.floor(Date.now() / 1000);
    return Response.json({ active: true, sub: "u-1", email: "a@example.test", principal_type: "desktop-user", org: "org-a", workspace: "ws-a", scopes: ["rth:read", "rth:sync"], machine_id: body.machine ?? null, aud: ORIGIN, iat: now, exp: now + 600, token_id: "tokid_1", ...over });
  };
  const call = async (
    r: { method: string; path: string },
    opts: { relaycontrol?: (body: Body) => Response; machine?: string; query?: string; body?: unknown } = {},
  ) => {
    resetSharedRelaycontrolIntrospectors();
    const original = globalThis.fetch;
    const bodies: Body[] = [];
    globalThis.fetch = (async (input: RequestInfo | URL, init: RequestInit = {}) => {
      if (String(input) !== INTROSPECT) throw new Error(`unexpected fetch ${String(input)}`);
      const body = JSON.parse(String(init.body)) as Body;
      bodies.push(body);
      return (opts.relaycontrol ?? verified())(body);
    }) as typeof fetch;
    try {
      const write = r.method !== "GET" && r.method !== "HEAD";
      const headers: Record<string, string> = { authorization: "Bearer sh_at_synthetic", "x-relayhistory-workspace-id": "ws-a", "content-type": "application/json" };
      if (opts.machine !== undefined) headers["x-machine-id"] = opts.machine;
      const res = await a.request(`${concrete(r.path)}${opts.query ?? ""}`, { method: r.method, headers, body: write ? JSON.stringify(opts.body ?? { machineId: opts.machine ?? "mach-1" }) : undefined }, rcEnv);
      const text = await res.text();
      let code: string | undefined;
      try { code = (JSON.parse(text) as { error?: { code?: string } }).error?.code; } catch { code = undefined; }
      return { text, status: res.status, code, bodies };
    } finally {
      globalThis.fetch = original;
    }
  };
  const route = (method: string, path: string) => {
    const found = v1Routes.find((r) => r.method === method && r.path === path);
    if (!found) throw new Error(`${method} ${path} is not mounted`);
    return found;
  };
  const authCodes = ["missing_authorization", "unauthorized", "auth_not_configured", "invalid_token", "relaycontrol_unavailable", "forbidden", "machine_claim_refused", "origin_bound_assertion_required"];

  test("the chunk, event and manifest writes are all mounted", () => {
    for (const path of ["/v1/pipeline/chunks", "/v1/pipeline/events", "/v1/pipeline/manifests"]) expect(writes.some((r) => r.path === path)).toBe(true);
  });

  test("no X-Machine-Id: every write (chunk, event, manifest) is refused 403 machine_claim_refused, and no machine is sent", async () => {
    for (const r of writes) {
      const { status, code, bodies } = await call(r, { body: { machineId: "mach-1" }, query: "?machine=mach-1" });
      expect({ route: `${r.method} ${r.path}`, status, code }).toEqual({ route: `${r.method} ${r.path}`, status: 403, code: "machine_claim_refused" });
      expect(bodies.length).toBeLessThanOrEqual(1);
      for (const b of bodies) expect(b).not.toHaveProperty("machine");
    }
  });

  test("X-Machine-Id present but relaycontrol answers machine_id null: every write is refused", async () => {
    for (const r of writes) {
      const { status, code, bodies } = await call(r, { machine: "mach-1", query: "?machine=mach-1", relaycontrol: verified({ machine_id: null }) });
      expect({ route: `${r.method} ${r.path}`, status, code }).toEqual({ route: `${r.method} ${r.path}`, status: 403, code: "machine_claim_refused" });
      for (const b of bodies) expect(b.machine).toBe("mach-1");
    }
  });

  test("machine owned by another user, unclaimed, or in another workspace: 401 with a generic code", async () => {
    for (const reason of ["machine_not_owned", "machine_unclaimed", "machine_workspace_mismatch"]) {
      for (const r of writes) {
        const { text, status, code } = await call(r, { machine: "mach-1", query: "?machine=mach-1", relaycontrol: () => Response.json({ active: false, reason }) });
        expect({ reason, route: `${r.method} ${r.path}`, status, code }).toEqual({ reason, route: `${r.method} ${r.path}`, status: 401, code: "unauthorized" });
        expect(text).not.toContain(reason);
      }
    }
  });

  test("a verified machine passes the gate only where the request names that machine", async () => {
    const chunks = route("POST", "/v1/pipeline/chunks");
    const events = route("POST", "/v1/pipeline/events");
    // Verified and matching: past the gate and the scope (the throwing deps answer, never an auth refusal).
    for (const [r, opts] of [[chunks, { query: "?machine=mach-1" }], [events, { body: { machineId: "mach-1" } }]] as const) {
      const { status, code, bodies } = await call(r, { machine: "mach-1", ...opts });
      expect({ route: r.path, status, authRefused: authCodes.includes(code ?? "") }).toMatchObject({ route: r.path, authRefused: false });
      expect(status).not.toBe(401);
      expect(bodies).toEqual([{ token: "sh_at_synthetic", aud: ORIGIN, workspace: "ws-a", machine: "mach-1" }]);
    }
    // Verified, but the chunk or event names another machine: refused by the unchanged gate.
    expect((await call(chunks, { machine: "mach-1", query: "?machine=mach-2" })).code).toBe("machine_claim_refused");
    expect((await call(events, { machine: "mach-1", body: { machineId: "mach-2" } })).code).toBe("machine_claim_refused");
  });

  test("a verified machine with rth:read only cannot write", async () => {
    // A manifest body the courier route reads as far as its scope check.
    const key = manifestKey({ workspaceId: "ws-a", machineId: "mach-1", harness: "claude", sessionId: "s-1", startDate: "2026-11-07" });
    for (const r of writes) {
      const body = r.path === "/v1/pipeline/manifests" ? { key, manifest: {} } : { machineId: "mach-1" };
      const { status, code } = await call(r, { machine: "mach-1", query: "?machine=mach-1", body, relaycontrol: verified({ scopes: ["rth:read"] }) });
      expect({ route: `${r.method} ${r.path}`, status, code }).toEqual({ route: `${r.method} ${r.path}`, status: 403, code: "forbidden" });
    }
  });

  test("reads with no machine pass authentication with rth:read only (they reach the dependencies)", async () => {
    expect(reads.length).toBeGreaterThan(0);
    for (const r of reads) {
      const { status, code, bodies } = await call(r, { relaycontrol: verified({ scopes: ["rth:read"] }) });
      // Past auth the throwing dependency (or the route's own default-off gate)
      // answers; never an authentication, scope or machine refusal.
      expect({ route: `${r.method} ${r.path}`, status: status === 401 ? 401 : "not 401", authRefused: authCodes.includes(code ?? "") }).toEqual({ route: `${r.method} ${r.path}`, status: "not 401", authRefused: false });
      for (const b of bodies) expect(b).not.toHaveProperty("machine");
    }
  });
});
