/**
 * RELAYHISTORY_SELFHOST_READ_API (selfhost.ts): POST /v1/cli/login (pass-through
 * exchange of a relaycontrol `sh_at_` token through the #313 introspection) and
 * GET /v1/sessions/:id/catalog (canonical S3 rows). Off, neither route exists and
 * the app answers exactly as an unknown route. Synthetic ids, token and secret
 * only; relaycontrol is a stub fetch.
 */
import { describe, expect, test } from "bun:test";
import { createRelaycontrolIntrospector } from "@rhc/auth/relaycontrol.ts";
import { AuthError, createReadApi, type VerifiedIdentity } from "../app.ts";
import { relaycontrolLogin, type AuthConfig } from "../auth.ts";
import type { CanonicalIdentity } from "../canonical.ts";
import type { CanonicalRows } from "../session-timing-source.ts";
import { CATALOG_MAX_ITEMS, catalogFromCanonical, parseSelfhostLoginSync, parseSelfhostReadApi, type CatalogDeps, type SelfhostDeps } from "../selfhost.ts";
import { readAppFromSecret } from "../lambda.ts";
import { memoryLiveness } from "./support.ts";

const ORIGIN = "https://read.example.test";
const TOKEN = "sh_at_synthetic-login-token";
// The introspector checks exp against the real clock, so the app clock is pinned to it.
const NOW = Math.floor(Date.now() / 1000) * 1000;
const config: AuthConfig = { origin: ORIGIN, relaycontrol: { url: "http://relaycontrol.test", secret: "synthetic-secret" } };

function active(over: Record<string, unknown> = {}) {
  const now = Math.floor(NOW / 1000);
  return { active: true, sub: "u-1", email: "a@example.test", principal_type: "desktop-user", org: "org-a", workspace: "ws-a", scopes: ["cli:auth", "rth:read"], machine_id: null, aud: ORIGIN, iat: now, exp: now + 3600, token_id: "tokid_1", ...over };
}

function stub(answer: () => Response | Promise<Response>) {
  const calls: Record<string, unknown>[] = [];
  const introspector = async (opts: { url: string; secret: string }) =>
    createRelaycontrolIntrospector({
      ...opts,
      log: () => {},
      fetch: (async (_url: RequestInfo | URL, init: RequestInit = {}) => {
        calls.push(JSON.parse(String(init.body)));
        return answer();
      }) as typeof fetch,
    });
  return { introspector, calls };
}

const identity: VerifiedIdentity = { userId: "u-1", orgId: "org-a", workspaceId: "ws-a", scopes: ["rth:read"] };

const ROW_BASE = { source: "codex", record_id: "r-1", origin_id: "o-1", received_at: "2026-01-01T00:00:00Z" };
const ROWS: CanonicalRows = {
  legacyCoverage: null,
  rows: [
    { table: "session_catalog", row: { ...ROW_BASE, session_id: "s-1", title: "old", received_at: "2025-12-31T00:00:00Z" } },
    { table: "session_catalog", row: { ...ROW_BASE, session_id: "s-1", title: "Synthetic title", cwd: "/w", models: ["m-1"] } },
    { table: "session_catalog_agents", row: { ...ROW_BASE, session_id: "s-1", agent_name: "agent-a" } },
    { table: "session_relationships", row: { ...ROW_BASE, parent_session_id: "s-1", relationship_uid: "rel-2", child_session_id: "s-3", spawned_at: "2026-01-01T00:02:00Z" } },
    { table: "session_relationships", row: { ...ROW_BASE, parent_session_id: "s-1", relationship_uid: "rel-1", child_session_id: "s-2", spawned_at: "2026-01-01T00:01:00Z" } },
    { table: "session_commit_links", row: { ...ROW_BASE, session_id: "s-1", commit_sha: "abc123", match_method: "note", confidence_basis_points: 10000, linked_at: "2026-01-01T00:03:00Z" } },
    { table: "conversation_events", row: { ...ROW_BASE, session_id: "s-1" } },
  ],
};

function api(over: { selfhost?: Partial<SelfhostDeps> | null; catalog?: Partial<CatalogDeps> | null; workspaces?: ReadonlySet<string> | "*"; rows?: CanonicalRows; state?: { pointers: number; live: number } } = {}) {
  const logs: Record<string, unknown>[] = [];
  const calls: CanonicalIdentity[] = [];
  const catalog: CatalogDeps | undefined =
    over.catalog === null
      ? undefined
      : {
          state: async () => over.state ?? { pointers: 1, live: 1 },
          source: async (id) => {
            calls.push(id);
            return over.rows ?? ROWS;
          },
          ...over.catalog,
        };
  const selfhost: SelfhostDeps | undefined =
    over.selfhost === null
      ? undefined
      : { login: async () => ({ identity, expiresAtMs: NOW + 3600_000 }), catalog, now: () => NOW, ...over.selfhost };
  const app = createReadApi({
    verify: async (t) => {
      if (t !== "good") throw new AuthError(401, "invalid_token", "token is not valid");
      return identity;
    },
    ch: { database: "x", async command() {}, async insert() {}, async select() { return []; } },
    liveness: memoryLiveness(),
    completeSinceMs: 0,
    directoryWorkspaces: "workspaces" in over ? over.workspaces : new Set(["ws-a"]),
    ...(selfhost ? { selfhost } : {}),
    log: (f) => logs.push(f),
  });
  const get = (path: string, token: string | null = "good") => app.request(path, { headers: token ? { authorization: `Bearer ${token}` } : {} });
  const login = (body: unknown, headers: Record<string, string> = {}) =>
    app.request("/v1/cli/login", { method: "POST", headers: { "content-type": "application/json", ...headers }, body: typeof body === "string" ? body : JSON.stringify(body) });
  return { app, get, login, logs, calls };
}
const json = async (r: Response) => (await r.json()) as Record<string, any>;
const CATALOG = "/v1/sessions/s-1/catalog?workspace=ws-a&source=codex";

describe("parseSelfhostReadApi", () => {
  test("only exactly true enables; unset, empty and false are off; anything else throws without echoing", () => {
    expect(parseSelfhostReadApi({})).toBe(false);
    expect(parseSelfhostReadApi({ RELAYHISTORY_SELFHOST_READ_API: "" })).toBe(false);
    expect(parseSelfhostReadApi({ RELAYHISTORY_SELFHOST_READ_API: "false" })).toBe(false);
    expect(parseSelfhostReadApi({ RELAYHISTORY_SELFHOST_READ_API: "true" })).toBe(true);
    for (const bad of ["TRUE", "1", "yes", " true", "true ", "secret-ish"]) {
      expect(() => parseSelfhostReadApi({ RELAYHISTORY_SELFHOST_READ_API: bad })).toThrow(/must be unset, "", "true" or "false"/);
      try {
        parseSelfhostReadApi({ RELAYHISTORY_SELFHOST_READ_API: bad });
      } catch (error) {
        expect(String(error)).not.toContain("secret-ish");
      }
    }
  });
});

describe("lambda assembly", () => {
  test("an invalid flag value fails startup before any connection is made", async () => {
    await expect(readAppFromSecret("{}", { READ_API_COMPLETE_SINCE: "epoch", RELAYHISTORY_SELFHOST_READ_API: "yes" })).rejects.toThrow(/RELAYHISTORY_SELFHOST_READ_API must be unset/);
  });
});

describe("flag off", () => {
  test("both routes answer exactly as an unknown route does today", async () => {
    const off = api({ selfhost: null });
    const unknownPost = await off.app.request("/v1/cli/nope", { method: "POST", body: "{}" });
    const loginOff = await off.login({ agentRelayToken: TOKEN });
    expect(loginOff.status).toBe(unknownPost.status);
    expect(loginOff.status).toBe(401);
    expect((await json(loginOff)).error.code).toBe("missing_authorization");
    const authed = await off.app.request("/v1/cli/login", { method: "POST", headers: { authorization: "Bearer good" }, body: "{}" });
    expect(authed.status).toBe(501);
    expect((await json(authed)).error.reason).toBe("route");

    const catalogOff = await off.get(CATALOG);
    expect(catalogOff.status).toBe(501);
    expect((await json(catalogOff)).error).toEqual({ code: "unsupported_by_backend", message: "route not served by this backend", reason: "route" });
    expect((await off.get(CATALOG, null)).status).toBe(401);
  });
});

describe("POST /v1/cli/login", () => {
  test("bad bodies are 400 with a short code, never echoing the token", async () => {
    const { login, logs } = api();
    const cases: [unknown, string][] = [
      ["not json", "invalid_body"],
      ["[]", "invalid_body"],
      ["null", "invalid_body"],
      [{}, "invalid_agent_relay_token"],
      [{ agentRelayToken: "" }, "invalid_agent_relay_token"],
      [{ agentRelayToken: 7 }, "invalid_agent_relay_token"],
      [{ agentRelayToken: "x".repeat(4097) }, "invalid_agent_relay_token"],
      [{ agentRelayToken: TOKEN, label: "" }, "invalid_label"],
      [{ agentRelayToken: TOKEN, label: 3 }, "invalid_label"],
      [{ agentRelayToken: TOKEN, label: "a\nb" }, "invalid_label"],
      [{ agentRelayToken: TOKEN, label: "l".repeat(129) }, "invalid_label"],
      [{ agentRelayToken: TOKEN, extra: 1 }, "invalid_body"],
    ];
    for (const [body, code] of cases) {
      const r = await login(body);
      const text = await r.text();
      expect(`${JSON.stringify(body).slice(0, 40)} ${r.status} ${JSON.parse(text).error.code}`).toBe(`${JSON.stringify(body).slice(0, 40)} 400 ${code}`);
      expect(text).not.toContain(TOKEN);
    }
    const big = await login({ agentRelayToken: TOKEN, label: "x".repeat(9000) });
    expect(big.status).toBe(413);
    expect((await json(big)).error.code).toBe("body_too_large");
    expect(JSON.stringify(logs)).not.toContain(TOKEN);
  });

  test("503 when introspection is not enabled, not configured, unreachable or malformed", async () => {
    const cases: [AuthConfig, () => Response, string][] = [
      [{ origin: ORIGIN }, () => Response.json(active()), "relaycontrol_not_enabled"],
      [{ origin: ORIGIN, relaycontrol: { url: "http://relaycontrol.test" } }, () => Response.json(active()), "relaycontrol_not_configured"],
      [{ origin: ORIGIN, relaycontrol: { secret: "synthetic-secret" } }, () => Response.json(active()), "relaycontrol_not_configured"],
      [{ origin: undefined, relaycontrol: config.relaycontrol }, () => Response.json(active()), "relaycontrol_not_configured"],
      [config, () => { throw new TypeError("fetch failed"); }, "relaycontrol_unavailable"],
      [config, () => new Response("", { status: 500 }), "relaycontrol_unavailable"],
      [config, () => new Response("", { status: 401 }), "relaycontrol_unavailable"],
      [config, () => new Response("nope"), "relaycontrol_unavailable"],
      [config, () => Response.json(active({ exp: "soon" })), "relaycontrol_unavailable"],
    ];
    for (const [cfg, answer, code] of cases) {
      const { login, logs } = api({ selfhost: { login: relaycontrolLogin(cfg, stub(answer).introspector) } });
      const r = await login({ agentRelayToken: TOKEN });
      const text = await r.text();
      expect(`${r.status} ${JSON.parse(text).error.code}`).toBe(`503 ${code}`);
      expect(r.headers.get("retry-after")).toBe("1");
      expect(r.headers.get("x-relayhistory-workspace-id")).toBeNull();
      expect(text).not.toContain(TOKEN);
      expect(JSON.stringify(logs)).not.toContain(TOKEN);
    }
  });

  test("401 for inactive, expired, invalid format and wrong audience; 403 without rth:read", async () => {
    const exp = Math.floor(NOW / 1000);
    const cases: [string, () => Response, string][] = [
      [TOKEN, () => Response.json({ active: false }), "401 token_inactive"],
      [TOKEN, () => Response.json({ active: false, reason: "revoked" }), "401 token_inactive"],
      [TOKEN, () => Response.json({ active: false, reason: "expired" }), "401 token_expired"],
      [TOKEN, () => Response.json(active({ exp: exp - 10 })), "401 token_expired"],
      ["not-a-relaycontrol-token", () => Response.json(active()), "401 invalid_token"],
      ["sh_rt_synthetic-refresh", () => Response.json(active()), "401 invalid_token"],
      [TOKEN, () => Response.json(active({ aud: "https://history.example.test" })), "401 wrong_audience"],
      [TOKEN, () => Response.json(active({ principal_type: "agent" })), "401 invalid_token"],
      [TOKEN, () => Response.json(active({ scopes: ["cli:auth"] })), "403 forbidden"],
    ];
    for (const [token, answer, expected] of cases) {
      const { introspector, calls } = stub(answer);
      const { login, logs } = api({ selfhost: { login: relaycontrolLogin(config, introspector) } });
      const r = await login({ agentRelayToken: token });
      const text = await r.text();
      expect(`${answer.toString().slice(0, 60)} ${r.status} ${JSON.parse(text).error.code}`).toBe(`${answer.toString().slice(0, 60)} ${expected}`);
      expect(text).not.toContain(token);
      expect(JSON.stringify(logs)).not.toContain(token);
      if (!token.startsWith("sh_at_")) expect(calls).toEqual([]);
    }
  });

  test("200: exact shape, introspected with aud only, workspace header, no token in logs", async () => {
    const { introspector, calls } = stub(() => Response.json(active({ exp: Math.floor(NOW / 1000) + 600 })));
    const { login, logs } = api({ selfhost: { login: relaycontrolLogin(config, introspector) } });
    const r = await login({ agentRelayToken: TOKEN, label: "relay-dashboard" });
    expect(r.status).toBe(200);
    expect(r.headers.get("x-relayhistory-workspace-id")).toBe("ws-a");
    expect(r.headers.get("cache-control")).toBe("private, no-store");
    const b = await json(r);
    expect(b).toEqual({
      accessToken: TOKEN,
      accessTokenExpiresAt: new Date(NOW + 600_000).toISOString(),
      refreshToken: TOKEN,
      refreshTokenExpiresAt: new Date(NOW + 600_000).toISOString(),
      tokenType: "Bearer",
      scopes: ["rth:read"],
    });
    expect(calls).toEqual([{ token: TOKEN, aud: ORIGIN }]);
    expect(JSON.stringify(logs)).not.toContain(TOKEN);
    expect(logs.some((l) => l.reason === "cli_login" && l.workspace_id === "ws-a")).toBe(true);
  });

  test("expiry is capped at now + 15 minutes", async () => {
    const { introspector } = stub(() => Response.json(active({ exp: Math.floor(NOW / 1000) + 86_400 })));
    const { login } = api({ selfhost: { login: relaycontrolLogin(config, introspector) } });
    const b = await json(await login({ agentRelayToken: TOKEN }));
    expect(b.accessTokenExpiresAt).toBe(new Date(NOW + 15 * 60_000).toISOString());
    expect(b.refreshTokenExpiresAt).toBe(b.accessTokenExpiresAt);
  });

  test("a grant already expired at answer time is 401, and an unexpected login failure fails closed as 503", async () => {
    const expired = api({ selfhost: { login: async () => ({ identity, expiresAtMs: NOW }) } });
    const r = await expired.login({ agentRelayToken: TOKEN });
    expect(r.status).toBe(401);
    expect((await json(r)).error.code).toBe("token_expired");
    const broken = api({ selfhost: { login: async () => { throw new Error(`boom ${TOKEN}`); } } });
    const r2 = await broken.login({ agentRelayToken: TOKEN });
    const text = await r2.text();
    expect(r2.status).toBe(503);
    expect(text).not.toContain(TOKEN);
    expect(JSON.stringify(broken.logs)).not.toContain(TOKEN);
  });

  test("the reads' workspace allowlist applies after introspection, fail closed", async () => {
    const answer = () => Response.json(active());
    const allowed = api({ selfhost: { login: relaycontrolLogin(config, stub(answer).introspector) } });
    const ok = await allowed.login({ agentRelayToken: TOKEN });
    expect(ok.status).toBe(200);
    expect(ok.headers.get("x-relayhistory-workspace-id")).toBe("ws-a");

    for (const workspaces of [new Set(["ws-z"]), new Set<string>(), undefined] as const) {
      const { introspector, calls } = stub(answer);
      const off = api({ workspaces, selfhost: { login: relaycontrolLogin(config, introspector) } });
      const r = await off.login({ agentRelayToken: TOKEN });
      const text = await r.text();
      expect(r.status).toBe(501);
      expect(JSON.parse(text).error).toEqual({ code: "unsupported_by_backend", message: expect.any(String), reason: "login_workspace_not_enabled" });
      expect(r.headers.get("x-relayhistory-workspace-id")).toBeNull();
      expect(text).not.toContain(TOKEN);
      expect(JSON.stringify(off.logs)).not.toContain(TOKEN);
      expect(calls).toHaveLength(1);
    }

    const star = api({ workspaces: "*", selfhost: { login: relaycontrolLogin(config, stub(() => Response.json(active({ workspace: "ws-any" }))).introspector) } });
    const s = await star.login({ agentRelayToken: TOKEN });
    expect(s.status).toBe(200);
    expect(s.headers.get("x-relayhistory-workspace-id")).toBe("ws-any");
  });

  test("scopes: only served scopes are advertised; no served scope is 403 after canary and expiry", async () => {
    // (i) rth:sync is never advertised by this read-only backend.
    const both = api({ selfhost: { login: relaycontrolLogin(config, stub(() => Response.json(active({ scopes: ["rth:read", "rth:sync"] }))).introspector) } });
    const ok = await both.login({ agentRelayToken: TOKEN });
    expect(ok.status).toBe(200);
    expect((await json(ok)).scopes).toEqual(["rth:read"]);
    expect(JSON.stringify(both.logs)).not.toContain("rth:");

    // (ii) a grant with no served scope reaching the handler is refused (its own check, not only the exchange's).
    const syncOnly = { ...identity, scopes: ["rth:sync"] };
    const refused = api({ selfhost: { login: async () => ({ identity: syncOnly, expiresAtMs: NOW + 3600_000 }) } });
    const r = await refused.login({ agentRelayToken: TOKEN });
    const text = await r.text();
    expect(r.status).toBe(403);
    expect(JSON.parse(text).error).toEqual({ code: "forbidden", message: "missing required scope: rth:read" });
    expect(r.headers.get("x-relayhistory-workspace-id")).toBeNull();
    expect(text).not.toContain(TOKEN);
    expect(refused.logs.some((l) => l.reason === "cli_login_refused" && l.code === "forbidden" && l.status === 403)).toBe(true);
    expect(refused.logs.some((l) => l.reason === "cli_login")).toBe(false);
    expect(JSON.stringify(refused.logs)).not.toContain(TOKEN);
    expect(JSON.stringify(refused.logs)).not.toContain("rth:");

    // Ordering: off the canary allowlist the answer stays 501, and an expired grant stays 401.
    const offCanary = api({ workspaces: new Set(["ws-z"]), selfhost: { login: async () => ({ identity: syncOnly, expiresAtMs: NOW + 3600_000 }) } });
    const r501 = await offCanary.login({ agentRelayToken: TOKEN });
    expect(r501.status).toBe(501);
    expect((await json(r501)).error.reason).toBe("login_workspace_not_enabled");
    const expired = api({ selfhost: { login: async () => ({ identity: syncOnly, expiresAtMs: NOW }) } });
    const r401 = await expired.login({ agentRelayToken: TOKEN });
    expect(r401.status).toBe(401);
    expect((await json(r401)).error.code).toBe("token_expired");
  });

  test("Pattern A (syncServed): rth:sync is advertised only when introspection granted it", async () => {
    const granted = (scopes: string[]) => relaycontrolLogin(config, stub(() => Response.json(active({ scopes }))).introspector);
    const both = await api({ selfhost: { syncServed: true, login: granted(["rth:read", "rth:sync"]) } }).login({ agentRelayToken: TOKEN });
    expect(both.status).toBe(200);
    expect((await json(both)).scopes).toEqual(["rth:read", "rth:sync"]);
    // Not granted: not advertised, even with the edge serving sync.
    const readOnly = await api({ selfhost: { syncServed: true, login: granted(["rth:read"]) } }).login({ agentRelayToken: TOKEN });
    expect((await json(readOnly)).scopes).toEqual(["rth:read"]);
    // Unknown spellings never become rth:sync (the introspector's allowlist, then this handler's).
    const odd = await api({ selfhost: { syncServed: true, login: granted(["rth:read", "RTH:SYNC", "rth:sync:all"]) } }).login({ agentRelayToken: TOKEN });
    expect((await json(odd)).scopes).toEqual(["rth:read"]);
    // A sync-only grant still cannot log in: login is the read surface and requires rth:read.
    const syncOnly = await api({ selfhost: { syncServed: true, login: granted(["rth:sync"]) } }).login({ agentRelayToken: TOKEN });
    expect([syncOnly.status, (await json(syncOnly)).error.code]).toEqual([403, "forbidden"]);
  });

  test("parseSelfhostLoginSync: strict, needs the self-host read API, never echoes", () => {
    expect(parseSelfhostLoginSync({})).toBe(false);
    expect(parseSelfhostLoginSync({ RELAYHISTORY_SELFHOST_LOGIN_ADVERTISES_SYNC: "false" })).toBe(false);
    expect(parseSelfhostLoginSync({ RELAYHISTORY_SELFHOST_READ_API: "true", RELAYHISTORY_SELFHOST_LOGIN_ADVERTISES_SYNC: "true" })).toBe(true);
    expect(() => parseSelfhostLoginSync({ RELAYHISTORY_SELFHOST_LOGIN_ADVERTISES_SYNC: "true" })).toThrow(/needs RELAYHISTORY_SELFHOST_READ_API=true/);
    for (const bad of ["TRUE", "1", "secret-ish"]) {
      let message = "";
      try {
        parseSelfhostLoginSync({ RELAYHISTORY_SELFHOST_READ_API: "true", RELAYHISTORY_SELFHOST_LOGIN_ADVERTISES_SYNC: bad });
      } catch (error) {
        message = String(error);
      }
      expect(message).toContain("must be unset");
      expect(message).not.toContain("secret-ish");
    }
  });

  test("GET on the login path is not a login", async () => {
    const r = await api().get("/v1/cli/login");
    expect(r.status).toBe(501);
  });
});

describe("GET /v1/sessions/:id/catalog", () => {
  test("200: shape, newest catalog wins, children and commit links, provenance dropped", async () => {
    const { get, calls } = api();
    const r = await get(CATALOG);
    expect(r.status).toBe(200);
    expect(r.headers.get("x-relayhistory-workspace-id")).toBe("ws-a");
    const b = await json(r);
    expect(Object.keys(b).sort()).toEqual(["catalog", "children", "commitLinks", "parents", "truncated", "workspaceId"]);
    expect(b.workspaceId).toBe("ws-a");
    expect(b.catalog.title).toBe("Synthetic title");
    expect(b.catalog.sessionId).toBe("s-1");
    expect(b.catalog.agentName).toBe("agent-a");
    expect(b.catalog.models).toEqual(["m-1"]);
    expect(b.catalog.recordId).toBeUndefined();
    expect(b.catalog.originId).toBeUndefined();
    expect(b.parents).toEqual([]);
    expect(b.children.map((c: any) => c.childSessionId)).toEqual(["s-2", "s-3"]);
    expect(b.commitLinks).toHaveLength(1);
    expect(b.commitLinks[0].commitSha).toBe("abc123");
    expect(b.commitLinks[0].confidence).toBe(10000);
    expect(b.truncated).toBe(false);
    expect(calls).toEqual([{ orgId: "org-a", workspaceId: "ws-a", source: "codex", sessionId: "s-1" }]);
  });

  test("no rows is the empty catalog; erased is the empty catalog without a read; no pointer is an in-workspace 404", async () => {
    const empty = await json(await api({ rows: { rows: [], legacyCoverage: null } }).get(CATALOG));
    expect(empty).toEqual({ workspaceId: "ws-a", catalog: null, parents: [], children: [], commitLinks: [], truncated: false });
    const erased = api({ state: { pointers: 1, live: 0 } });
    expect(await json(await erased.get(CATALOG))).toEqual(empty);
    expect(erased.calls).toEqual([]);
    const missing = api({ state: { pointers: 0, live: 0 } });
    const unknown = await missing.get(CATALOG);
    expect(unknown.status).toBe(404);
    expect(unknown.headers.get("x-relayhistory-workspace-id")).toBe("ws-a");
    expect((await json(unknown)).error).toEqual({ code: "not_found", message: expect.any(String), reason: "catalog_no_pointers" });
    expect(missing.calls).toEqual([]);
    expect(missing.logs).toContainEqual(expect.objectContaining({ reason: "catalog_no_pointers", status: 404 }));
  });

  test("an unknown session is never a 404 unless the caller is attested and canaried for the workspace", async () => {
    let probes = 0;
    const unknown = (over: Parameters<typeof api>[0] = {}) =>
      api({
        state: { pointers: 0, live: 0 },
        catalog: {
          state: async () => {
            probes++;
            return { pointers: 0, live: 0 };
          },
        },
        ...over,
      });
    const status = async (r: Response) => `${r.status} ${(await json(r)).error.reason}`;
    const { get } = unknown();
    // Wrong workspace: the same 403 as a known session, no state probe.
    const mismatch = await get("/v1/sessions/s-unknown/catalog?workspace=ws-b&source=codex");
    expect(mismatch.status).toBe(403);
    expect((await json(mismatch)).error.code).toBe("forbidden");
    // No workspace: the org-scope 501.
    expect(await status(await get("/v1/sessions/s-unknown/catalog?source=codex"))).toBe("501 org_scope");
    // Unauthenticated or bad token: 401.
    expect((await get("/v1/sessions/s-unknown/catalog?workspace=ws-a&source=codex", null)).status).toBe(401);
    expect((await get("/v1/sessions/s-unknown/catalog?workspace=ws-a&source=codex", "bad")).status).toBe(401);
    // Workspace not on the canary allowlist (or allowlist absent): the canary 501.
    expect(await status(await unknown({ workspaces: new Set(["ws-z"]) }).get("/v1/sessions/s-unknown/catalog?workspace=ws-a&source=codex"))).toBe(
      "501 catalog_workspace_not_enabled",
    );
    expect(await status(await unknown({ workspaces: undefined }).get("/v1/sessions/s-unknown/catalog?workspace=ws-a&source=codex"))).toBe(
      "501 catalog_workspace_not_enabled",
    );
    expect(probes).toBe(0);
    // Attested and canaried: now, and only now, the 404.
    expect((await get("/v1/sessions/s-unknown/catalog?workspace=ws-a&source=codex")).status).toBe(404);
    expect(probes).toBe(1);
  });

  test("workspace mismatch, missing workspace and unauthenticated are refused like the sibling routes", async () => {
    const { get } = api();
    const mismatch = await get("/v1/sessions/s-1/catalog?workspace=ws-b&source=codex");
    expect(mismatch.status).toBe(403);
    expect((await json(mismatch)).error.code).toBe("forbidden");
    const timingMismatch = await get("/v1/sessions/s-1/timing?workspace=ws-b&source=codex");
    expect(timingMismatch.status).toBe(403);
    expect((await get("/v1/sessions/s-1/catalog?source=codex")).status).toBe(501);
    expect((await get(CATALOG, null)).status).toBe(401);
    expect((await get(CATALOG, "bad")).status).toBe(401);
    expect((await get("/v1/sessions/s-1/catalog?workspace=ws-a")).status).toBe(400);
    expect((await get("/v1/sessions/s-1/catalog?workspace=ws-a&source=a&source=b")).status).toBe(400);
  });

  test("canary allowlist, unconfigured source, foreign rows and the deadline are 501s", async () => {
    const reason = async (r: Response) => `${r.status} ${(await json(r)).error.reason}`;
    expect(await reason(await api({ workspaces: new Set(["ws-z"]) }).get(CATALOG))).toBe("501 catalog_workspace_not_enabled");
    expect(await reason(await api({ catalog: null }).get(CATALOG))).toBe("501 catalog_not_configured");
    const foreign: CanonicalRows = { legacyCoverage: null, rows: [{ table: "session_catalog", row: { ...ROW_BASE, session_id: "s-other" } }] };
    expect(await reason(await api({ rows: foreign }).get(CATALOG))).toBe("501 catalog_row_scope");
    const slow = api({ catalog: { deadlineMs: 5, source: (_id, signal) => new Promise((_, reject) => signal.addEventListener("abort", () => reject(new Error("aborted")))) } });
    expect(await reason(await slow.get(CATALOG))).toBe("501 catalog_canonical_deadline");
  });

  test("caps each array and reports truncation", () => {
    const rows: CanonicalRows["rows"] = [];
    for (let i = 0; i < CATALOG_MAX_ITEMS + 3; i++)
      rows.push({ table: "session_relationships", row: { ...ROW_BASE, parent_session_id: "s-1", relationship_uid: `rel-${String(i).padStart(4, "0")}`, spawned_at: "2026-01-01T00:00:00Z" } });
    const out = catalogFromCanonical({ orgId: "org-a", workspaceId: "ws-a", source: "codex", sessionId: "s-1" }, { rows, legacyCoverage: null });
    expect(out.children).toHaveLength(CATALOG_MAX_ITEMS);
    expect(out.truncated).toBe(true);
  });
});
