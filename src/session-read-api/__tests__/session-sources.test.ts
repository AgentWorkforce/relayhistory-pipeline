/**
 * GET /v1/sessions/:sessionId/sources (READ_API_SESSION_SOURCES_ENABLED, session-sources.ts) over a
 * recording reader fake: the refusal order (401, flag 501, 403, canary 501, params 501, id 400)
 * reaches no read; 0/1/2 sources answer sorted and deduplicated; rows are rechecked against the
 * tenant. The SQL itself runs as the read role in liveness.pglite.test.ts.
 */
import { describe, expect, test } from "bun:test";
import { AuthError, createReadApi, type ReadApiDeps, type VerifiedIdentity } from "../app.ts";
import type { ClickHouseClient } from "../../session-pipeline/vendor/search-index/clickhouse.ts";
import { SESSION_SOURCES_MAX, type SessionSourceRow } from "../session-sources.ts";
import { memoryLiveness } from "./support.ts";

const ID: VerifiedIdentity = { userId: "user-1", orgId: "org-a", workspaceId: "ws-a", scopes: ["rth:read"] };

const ch: ClickHouseClient = {
  database: "relay_search",
  async command() { throw new Error("never"); },
  async insert() { throw new Error("never"); },
  async select() { throw new Error("the sources route never reads ClickHouse"); },
};

function reader(answer: SessionSourceRow[] | (() => Promise<SessionSourceRow[]>)) {
  const calls: Array<{ orgId: string; workspaceId: string; sessionId: string }> = [];
  const read: NonNullable<ReadApiDeps["sessionSources"]> = async (orgId, workspaceId, sessionId) => {
    calls.push({ orgId, workspaceId, sessionId });
    return typeof answer === "function" ? answer() : answer;
  };
  return { read, calls };
}

const src = (harness: string, over: Partial<SessionSourceRow> = {}): SessionSourceRow => ({ org_id: "org-a", workspace_id: "ws-a", harness, session_ids: ["s-1"], canonical_ids: [], ...over });

function api(over: { sessionSources?: ReadApiDeps["sessionSources"]; workspaces?: ReadonlySet<string> | "*"; operatorErased?: ReadApiDeps["operatorErased"] } = {}) {
  const logs: Array<Record<string, unknown>> = [];
  const app = createReadApi({
    verify: async (t) => {
      if (t !== "good") throw new AuthError(401, "invalid_token", "token is not valid");
      return ID;
    },
    ch,
    liveness: memoryLiveness().liveness,
    completeSinceMs: 0,
    log: (f) => logs.push(f),
    directoryWorkspaces: over.workspaces ?? new Set(["ws-a"]),
    ...(over.sessionSources ? { sessionSources: over.sessionSources } : {}),
    ...(over.operatorErased ? { operatorErased: over.operatorErased } : {}),
  });
  const get = (path: string, bearer: string | null = "good") => app.request(path, { headers: bearer ? { authorization: `Bearer ${bearer}` } : {} });
  return { get, logs };
}
const json = async (r: Response) => (await r.json()) as Record<string, any>;
const path = (id = "s-1", extra = "") => `/v1/sessions/${encodeURIComponent(id)}/sources?workspace=ws-a${extra}`;

describe("GET /v1/sessions/:sessionId/sources", () => {
  test("0, 1 and 2 sources: sorted, deduplicated, keyed by the token's tenant and the path id", async () => {
    for (const [rows, expected] of [
      [[], []],
      [[src("claude")], ["claude"]],
      [[src("codex"), src("claude"), src("codex")], ["claude", "codex"]],
    ] as const) {
      const r = reader([...rows]);
      const res = await api({ sessionSources: r.read }).get(path("s-1"));
      expect(res.status).toBe(200);
      expect(await json(res)).toEqual({ sessionId: "s-1", sources: [...expected] });
      expect(res.headers.get("x-relayhistory-workspace-id")).toBe("ws-a");
      expect(r.calls).toEqual([{ orgId: "org-a", workspaceId: "ws-a", sessionId: "s-1" }]);
    }
  });

  test("the id is passed through decoded and exactly; a 512-character id is served", async () => {
    const r = reader([src("claude")]);
    const id = "x".repeat(512);
    const res = await api({ sessionSources: r.read }).get(path(id));
    expect(res.status).toBe(200);
    expect((await json(res)).sessionId).toBe(id);
    const odd = reader([]);
    expect((await api({ sessionSources: odd.read }).get(path("a b:c"))).status).toBe(200);
    expect(odd.calls[0]?.sessionId).toBe("a b:c");
  });

  test("unauthenticated is 401 before anything, even with the flag off", async () => {
    for (const sessionSources of [undefined, reader([src("claude")]).read]) {
      const a = api(sessionSources ? { sessionSources } : {});
      for (const bearer of [null, "bad"]) {
        const res = await a.get(path(), bearer);
        expect(res.status).toBe(401);
      }
    }
  });

  test("flag off: 501 sources_not_configured in the standard envelope, whatever the request says", async () => {
    const a = api();
    for (const p of [path(), "/v1/sessions/s-1/sources?workspace=ws-other", "/v1/sessions/s-1/sources", path("x".repeat(600)), path("s-1", "&source=claude")]) {
      const res = await a.get(p);
      expect(res.status).toBe(501);
      const b = await json(res);
      expect(b.error).toEqual({ code: "unsupported_by_backend", message: "not served by this backend: sources_not_configured", reason: "sources_not_configured" });
      expect(typeof b.correlationId).toBe("string");
    }
  });

  test("workspace mismatch is a 403 and absent is 501 org_scope, both before any read", async () => {
    const r = reader([src("claude")]);
    const a = api({ sessionSources: r.read });
    const other = await a.get("/v1/sessions/s-1/sources?workspace=ws-other");
    expect(other.status).toBe(403);
    expect((await json(other)).error.code).toBe("forbidden");
    const absent = await a.get("/v1/sessions/s-1/sources");
    expect(absent.status).toBe(501);
    expect((await json(absent)).error.reason).toBe("org_scope");
    expect((await a.get("/v1/sessions/s-1/sources?workspace=ws-a&workspace=ws-a")).status).toBe(400);
    expect(r.calls).toHaveLength(0);
  });

  test("off the directory canary allowlist: 501 directory_workspace_not_enabled, no read", async () => {
    for (const workspaces of [new Set<string>(), new Set(["ws-other"])]) {
      const r = reader([src("claude")]);
      const res = await api({ sessionSources: r.read, workspaces }).get(path());
      expect(res.status).toBe(501);
      expect((await json(res)).error.reason).toBe("directory_workspace_not_enabled");
      expect(r.calls).toHaveLength(0);
    }
    const all = reader([src("claude")]);
    expect((await api({ sessionSources: all.read, workspaces: "*" }).get(path())).status).toBe(200);
  });

  test("an id over 512 characters is 400; any parameter but workspace is 501 param_<name>; neither reads", async () => {
    const r = reader([src("claude")]);
    const a = api({ sessionSources: r.read });
    const long = await a.get(path("x".repeat(513)));
    expect(long.status).toBe(400);
    expect((await json(long)).error.code).toBe("bad_request");
    for (const extra of ["&source=claude", "&limit=5", "&cursor=x"]) {
      const res = await a.get(path("s-1", extra));
      expect(res.status).toBe(501);
      expect((await json(res)).error.reason).toBe(`param_${extra.slice(1).split("=")[0]}`);
    }
    expect(r.calls).toHaveLength(0);
  });

  test("a row of another tenant is a 500; a malformed row or a read failure is a 503; never a 200", async () => {
    const cross = await api({ sessionSources: reader([src("claude", { workspace_id: "ws-b" })]).read }).get(path());
    expect(cross.status).toBe(500);
    const crossOrg = await api({ sessionSources: reader([src("claude", { org_id: "org-b" })]).read }).get(path());
    expect(crossOrg.status).toBe(500);
    for (const bad of [[src("Not A Harness")], [src("")], Array.from({ length: SESSION_SOURCES_MAX + 1 }, (_, i) => src(`h${i}`))]) {
      const res = await api({ sessionSources: reader(bad).read }).get(path());
      expect(res.status).toBe(503);
    }
    const down = await api({ sessionSources: reader(async () => { throw new Error("pg down"); }).read }).get(path());
    expect(down.status).toBe(503);
    expect((await json(down)).error.code).toBe("backend_unavailable");
  });

  test("with the operator erase fence configured, a source fenced under any form of its key is dropped; checks run one at a time", async () => {
    const r = reader([src("claude"), src("codex")]);
    const fenced = await api({ sessionSources: r.read, operatorErased: async (k) => k.source === "codex" }).get(path());
    expect(fenced.status).toBe(200);
    expect(await json(fenced)).toEqual({ sessionId: "s-1", sources: ["claude"] });
    // The caller asks the source id X; the fence was written under the physical id P of claude's row.
    const aliased = reader([src("claude", { session_ids: ["P"], canonical_ids: ["X"] }), src("codex", { session_ids: ["X"] })]);
    const seen: string[] = [];
    let inFlight = 0, maxInFlight = 0;
    const fence: NonNullable<ReadApiDeps["operatorErased"]> = async (k) => {
      inFlight++; maxInFlight = Math.max(maxInFlight, inFlight);
      seen.push(`${k.source}:${k.sessionId}`);
      await new Promise((resolve) => setTimeout(resolve, 1));
      inFlight--;
      return k.source === "claude" && k.sessionId === "P";
    };
    const res = await api({ sessionSources: aliased.read, operatorErased: fence }).get(path("X"));
    expect(res.status).toBe(200);
    expect(await json(res)).toEqual({ sessionId: "X", sources: ["codex"] });
    expect(seen).toEqual(["claude:X", "claude:P", "codex:X"]);
    expect(maxInFlight).toBe(1);
    const broken = await api({ sessionSources: r.read, operatorErased: async () => { throw new Error("operator_fence_unavailable"); } }).get(path());
    expect(broken.status).toBe(503);
  });

  test("with the fence configured and the flag off: 501 before any fence statement, even for another workspace", async () => {
    let calls = 0;
    const a = api({ operatorErased: async () => { calls++; return false; } });
    for (const p of [path(), "/v1/sessions/s-1/sources?workspace=ws-other", "/v1/sessions/s-1/sources?workspace=ws-a&source=claude"]) {
      const res = await a.get(p);
      expect(res.status, p).toBe(501);
      expect((await json(res)).error.reason, p).toBe("sources_not_configured");
    }
    expect(calls).toBe(0);
  });

  test("the served log line carries a count only, never the id or a source", async () => {
    const a = api({ sessionSources: reader([src("claude")]).read });
    await a.get(path("secret-session-id"));
    const line = a.logs.find((l) => l.reason === "session_sources_served");
    expect(line).toMatchObject({ count: 1, status: 200 });
    expect(JSON.stringify(a.logs)).not.toContain("secret-session-id");
    expect(JSON.stringify(line)).not.toContain("claude");
  });
});
