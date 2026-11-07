/**
 * GET /v1/sessions/:id/turns without the legacy export's coverage marker: a session that was
 * never in Neon (src/shared/never-in-neon.ts, the same proof as the session analysis) is
 * complete from S3 and served; a Neon-era session without coverage is still refused 501
 * turns_unknown; with coverage it is served as before. No proof wired = today's behavior.
 */
import { describe, expect, test } from "bun:test";
import { createReadApi, type TurnsDeps, type VerifiedIdentity } from "../app.ts";
import type { CanonicalRows } from "../session-timing-source.ts";

const identity: VerifiedIdentity = { userId: "u-1", orgId: "org-a", workspaceId: "ws-a", scopes: ["rth:read"] };
const TURN = {
  table: "conversation_turns",
  row: {
    id: "t-1", org_id: "org-a", workspace_id: "ws-a", session_id: "s-1", source: "claude", session_owner: "owner",
    turn_index: 0, role: "user", content: "hello", actor_name: "a", actor_role: "user", metadata: {},
    ts: "2026-11-07T00:00:00.000Z", live_revision: "1",
  },
};
const COVERED = { version: 1 as const, tables: ["conversation_turns", "session_links", "session_outcomes"] };

function api(rows: CanonicalRows, neonFree?: TurnsDeps["neonFree"]) {
  const asked: unknown[] = [];
  const turns: TurnsDeps = {
    harnesses: async () => [{ harness: "claude", pointers: 1, live: 1 }],
    source: async () => rows,
    ...(neonFree ? { neonFree: async (scope) => (asked.push(scope), neonFree(scope)) } : {}),
  };
  const app = createReadApi({
    verify: async () => identity,
    ch: { database: "x", async command() {}, async insert() {}, async select() { throw new Error("no ClickHouse"); } },
    liveness: { async erased() { return new Set(); }, async livePointerSessions() { return new Set(); }, async sessionStart() { return { pointers: 0, live: 0, createdAtMs: null }; } },
    completeSinceMs: 0,
    directoryWorkspaces: new Set(["ws-a"]),
    turns,
  });
  const get = () => app.request("/v1/sessions/s-1/turns?workspace=ws-a", { headers: { authorization: "Bearer t" } });
  return { get, asked };
}
const body = async (r: Response) => (await r.json()) as Record<string, any>;

describe("turns: the never-in-Neon proof (shared with the session analysis)", () => {
  test("born after the cutover (no marker, proven never in Neon): served", async () => {
    const { get, asked } = api({ rows: [TURN], legacyCoverage: null, neonDerived: false }, async () => true);
    const r = await get();
    expect(r.status).toBe(200);
    expect((await body(r)).turns.map((t: any) => t.content)).toEqual(["hello"]);
    expect(asked).toEqual([{ orgId: "org-a", workspaceId: "ws-a", sessionId: "s-1" }]);
  });

  test("a Neon-era session WITH the export's coverage: served, no proof consulted", async () => {
    const { get, asked } = api({ rows: [TURN], legacyCoverage: COVERED, neonDerived: true }, async () => false);
    expect((await get()).status).toBe(200);
    expect(asked).toEqual([]);
  });

  test("a Neon-era session WITHOUT coverage: refused 501 turns_unknown", async () => {
    // The database proof fails (in the frozen copy / born before the seal / no seal).
    const db = await api({ rows: [TURN], legacyCoverage: null, neonDerived: false }, async () => false).get();
    expect([db.status, (await body(db)).error.reason]).toEqual([501, "turns_unknown"]);
    // The objects half fails (a Neon export/backfill manifest), even if the database says free.
    const obj = await api({ rows: [TURN], legacyCoverage: null, neonDerived: true }, async () => true).get();
    expect([obj.status, (await body(obj)).error.reason]).toEqual([501, "turns_unknown"]);
  });

  test("default-preserving: with no proof wired, a coverage-less scope is refused exactly as before", async () => {
    const r = await api({ rows: [TURN], legacyCoverage: null }).get();
    expect([r.status, (await body(r)).error.reason]).toEqual([501, "turns_unknown"]);
  });
});
