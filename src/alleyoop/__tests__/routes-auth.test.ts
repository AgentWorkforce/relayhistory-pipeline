import { describe, expect, test } from "bun:test";
import { Hono } from "hono";
import { createPipelineRoutes, type AppEnv } from "../../shared/vendor.ts";
import { createCourierManifestRoutes } from "../manifests.ts";
import { PUBLIC_INGEST_PATHS, mountIngestApi } from "../routes.ts";

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
  mountIngestApi(a, { courierManifests: createCourierManifestRoutes(deps), pipeline: createPipelineRoutes(deps) }, new Hono<AppEnv>());
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
