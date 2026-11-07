/**
 * GET /v1/sessions/titles (feature F2): the allowlist, the request contract, the response
 * contract (only titled keys; non-2xx on any failure), tenant rechecks, and the not-migrated
 * 501. The SQL itself is covered on PGlite in src/session-brief/__tests__/brief-titles.test.ts.
 */
import { describe, expect, test } from "bun:test";
import { AuthError, createReadApi, type VerifiedIdentity } from "../app.ts";
import { BriefTitlesError, type BriefTitleKey, type BriefTitleRow, type BriefTitles } from "../brief-titles.ts";
import { memoryLiveness } from "./support.ts";

const ID: VerifiedIdentity = { userId: "u-1", orgId: "org-a", workspaceId: "ws-a", scopes: ["rth:read"] };
const row = (over: Partial<BriefTitleRow> = {}): BriefTitleRow => ({
  org_id: "org-a", workspace_id: "ws-a", harness: "claude", session_id: "s-1",
  title: "A brief title", origin: "native", generated_at: new Date("2026-11-01T00:00:00Z"), ...over,
});

function stub(init: { rows?: BriefTitleRow[] | null; fail?: unknown } = {}) {
  const calls: Array<{ orgId: string; workspaceId: string; keys: readonly BriefTitleKey[] }> = [];
  const titles: BriefTitles = {
    async titles(orgId, workspaceId, keys) {
      calls.push({ orgId, workspaceId, keys });
      if (init.fail) throw init.fail;
      return init.rows === undefined ? [row()] : init.rows;
    },
  };
  return { titles, calls };
}

function api(over: { identity?: VerifiedIdentity; titles?: BriefTitles; workspaces?: ReadonlySet<string> | "*" | undefined } = {}) {
  const logs: Record<string, unknown>[] = [];
  const app = createReadApi({
    verify: async (t) => {
      if (t !== "good") throw new AuthError(401, "invalid_token", "token is not valid");
      return over.identity ?? ID;
    },
    ch: { database: "x", async command() {}, async insert() {}, async select() { throw new Error("never ClickHouse"); } } as never,
    liveness: memoryLiveness().liveness,
    completeSinceMs: 0,
    log: (f) => logs.push(f),
    briefTitles: over.titles,
    briefTitleWorkspaces: "workspaces" in over ? over.workspaces : new Set(["ws-a"]),
  });
  const get = (path: string, token: string | null = "good", method = "GET") =>
    app.request(path, { method, headers: token ? { authorization: `Bearer ${token}` } : {} });
  return { get, logs };
}
const body = async (r: Response) => (await r.json()) as Record<string, any>;
const q = (...keys: string[]) => `/v1/sessions/titles?workspace=ws-a&${keys.map((k) => `key=${encodeURIComponent(k)}`).join("&")}`;

describe("GET /v1/sessions/titles", () => {
  test("serves only titled keys, tenant from the token, attested workspace", async () => {
    const { titles, calls } = stub({ rows: [row(), row({ harness: "codex", session_id: "a:b", title: "Colon id" })] });
    const r = await api({ titles }).get(q("claude:s-1", "codex:a:b", "claude:none", "claude:s-1"));
    expect(r.status).toBe(200);
    expect(r.headers.get("x-relayhistory-workspace-id")).toBe("ws-a");
    expect(r.headers.get("cache-control")).toBe("private, no-store");
    const b = await body(r);
    expect(b.titles).toEqual([
      { source: "claude", sessionId: "s-1", title: "A brief title" },
      { source: "codex", sessionId: "a:b", title: "Colon id" },
    ]);
    // Duplicates dropped; the first colon splits; org/workspace only from the token.
    expect(calls).toEqual([{ orgId: "org-a", workspaceId: "ws-a", keys: [
      { source: "claude", sessionId: "s-1" }, { source: "codex", sessionId: "a:b" }, { source: "claude", sessionId: "none" },
    ] }]);
  });

  test("is off unless the workspace is allowlisted ('*' = all); never reads when off", async () => {
    for (const workspaces of [undefined, new Set<string>(), new Set(["ws-other"])]) {
      const { titles, calls } = stub();
      const r = await api({ titles, workspaces }).get(q("claude:s-1"));
      expect(r.status).toBe(501);
      expect((await body(r)).error.reason).toBe("brief_titles_workspace_not_enabled");
      expect(calls).toHaveLength(0);
    }
    expect((await api({ titles: stub().titles, workspaces: "*" }).get(q("claude:s-1"))).status).toBe(200);
    const r = await api({ titles: undefined }).get(q("claude:s-1"));
    expect((await body(r)).error.reason).toBe("brief_titles_not_configured");
    const m = await api({ titles: stub({ rows: null }).titles }).get(q("claude:s-1"));
    expect(m.status).toBe(501);
    expect((await body(m)).error.reason).toBe("brief_titles_not_migrated");
  });

  test("auth, scope and workspace are checked before anything is read", async () => {
    const { titles, calls } = stub();
    expect((await api({ titles }).get(q("claude:s-1"), null)).status).toBe(401);
    expect((await api({ titles }).get(q("claude:s-1"), "bad")).status).toBe(401);
    expect((await api({ titles, identity: { ...ID, scopes: ["rth:write"] } }).get(q("claude:s-1"))).status).toBe(403);
    expect((await api({ titles }).get("/v1/sessions/titles?workspace=ws-b&key=claude:s-1")).status).toBe(403);
    expect((await api({ titles }).get("/v1/sessions/titles?key=claude:s-1")).status).toBe(501);
    expect((await api({ titles }).get("/v1/sessions/titles?workspace=ws-a&workspace=ws-a&key=claude:s-1")).status).toBe(400);
    expect(calls).toHaveLength(0);
  });

  test("malformed keys are a 400 and read nothing", async () => {
    const { titles, calls } = stub();
    const many = Array.from({ length: 101 }, (_, i) => `claude:s-${i}`);
    for (const path of [
      "/v1/sessions/titles?workspace=ws-a",
      q(...many),
      q("no-colon"),
      q(":s-1"),
      q("Claude:s-1"),
      q("claude:"),
      q(`claude:${"x".repeat(256)}`),
    ]) {
      const r = await api({ titles }).get(path);
      expect(r.status).toBe(400);
    }
    expect((await api({ titles }).get(q(...many.slice(0, 100)))).status).toBe(200);
    expect(calls).toHaveLength(1);
  });

  test("a row outside the request, the tenant or the title bound is never served", async () => {
    for (const [rows, status] of [
      [[row({ org_id: "org-b" })], 500],
      [[row({ workspace_id: "ws-b" })], 500],
      [[row({ session_id: "s-other" })], 503],
      [[row(), row()], 503],
      [[row({ title: "x".repeat(91) })], 503],
      [[row({ title: "two\nlines" })], 503],
      [[row({ title: "" })], 503],
    ] as Array<[BriefTitleRow[], number]>) {
      const r = await api({ titles: stub({ rows }).titles }).get(q("claude:s-1"));
      expect(r.status).toBe(status);
      expect(JSON.stringify(await body(r))).not.toContain("x".repeat(91));
    }
  });

  test("database refusals and failures are non-2xx, never an empty 200", async () => {
    expect((await api({ titles: stub({ fail: new BriefTitlesError("refused") }).titles }).get(q("claude:s-1"))).status).toBe(503);
    expect((await api({ titles: stub({ fail: new BriefTitlesError("invalid_argument") }).titles }).get(q("claude:s-1"))).status).toBe(400);
    expect((await api({ titles: stub({ fail: new Error("connection reset") }).titles }).get(q("claude:s-1"))).status).toBe(503);
    expect((await api({ titles: stub().titles }).get(q("claude:s-1"), "good", "POST")).status).toBe(501);
  });
});
