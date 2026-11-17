/**
 * The advisor tick against migrated Postgres (pglite), as service_role: candidates from the session
 * directory, rows written only through 062000's functions, no downgrade, acked never resent.
 * Facts come from a fake CoordinationBackend: the facts read is the backend's own tested concern.
 */
import { afterAll, beforeAll, beforeEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import { PGlite } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/index.js";
import { btree_gin } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/contrib/btree_gin.js";
import { pg_trgm } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/contrib/pg_trgm.js";
import type { LaneFacts } from "../../../packages/coordination-detect/src/index.ts";
import { runMigrations, type MigrationClient, type MigrationProfile } from "../../hoopdb/migrate.ts";
import type { CoordinationBackend, CoordinationFacts } from "../../session-read-api/coordination-backend.ts";
import type { SqlQuery } from "../../session-read-api/liveness.ts";
import { InMemoryRelaySender } from "../relay-sender.ts";
import { AdvisorLimits } from "../rubric.ts";
import { advisorTick, scanOnce, sqlAdvisorStore } from "../scan.ts";

setDefaultTimeout(300_000);
const LOCAL: MigrationProfile = { mode: "local", shim: true, bootstrap: true, verify: false, runtimeRole: "postgres" };
const NOW = new Date(Date.UTC(2026, 10, 17, 12));
const at = (min: number) => new Date(NOW.getTime() + min * 60_000).toISOString();

function client(db: PGlite): MigrationClient {
  return { async query<R>(text: string, params?: unknown[]) {
    if (params?.length) return (await db.query<R>(text, params)) as { rows: R[] };
    const results = await db.exec(text);
    return { rows: (results.at(-1)?.rows ?? []) as R[] };
  } };
}
/** Every worker statement runs as service_role, like the deployed worker. */
const asService = (db: PGlite): SqlQuery => ({
  query: async <R,>(text: string, params?: unknown[]) => db.transaction(async (tx) => {
    await tx.exec("SET LOCAL ROLE service_role");
    return { rows: (await tx.query<R>(text, params ?? [])).rows };
  }),
}) as SqlQuery;

const lane = (sessionId: string, writes: Array<[string, number]>): LaneFacts => ({
  sessionId, source: "claude", userId: null, projectId: "proj", repo: "o/r", branch: `b/${sessionId}`, taskRef: null, title: null,
  parentSessionId: null, startedAt: at(-90), lastActionAt: at(-5), commits: [], outcomes: [], usage: [],
  writes: writes.map(([path, min]) => ({ path, ts: at(min), eventId: `${sessionId}-${min}`, kind: "tool", branch: `b/${sessionId}` })),
});
const backend = (byWs: Record<string, LaneFacts[]>): CoordinationBackend & { reads: string[] } => {
  const reads: string[] = [];
  return {
    reads, kind: "clickhouse",
    windowReadiness: async () => { throw new Error("unused"); },
    readFacts: async (_d, p) => { reads.push(`${p.orgId}/${p.workspaceId}`); return { lanes: byWs[p.workspaceId] ?? [], sessionsTruncated: false, notes: [] } as unknown as CoordinationFacts; },
    isSchemaMissing: () => false,
  };
};

describe("coordination advisor tick (pglite)", () => {
  let db: PGlite;
  const dir = (org: string, ws: string, session: string, project: string | null, lastMin: number) => db.query(
    `INSERT INTO sessions.session_directory (org_id, workspace_id, harness, session_id, created_at, last_activity_at, project_id)
     VALUES ($1,$2,'claude',$3,$4,$5,$6)`, [org, ws, session, at(-120), at(lastMin), project]);
  const rows = async () => (await db.query<{ workspace_id: string; session_id: string; urgency: string; status: string; kind: string }>(
    "SELECT workspace_id, session_id, urgency, status, kind FROM sessions.coordination_advisories ORDER BY workspace_id, session_id, kind")).rows;
  const shared = [lane("a", [["src/x/shared.ts", -40]]), lane("b", [["src/x/shared.ts", -20]])];

  beforeAll(async () => {
    db = new PGlite({ extensions: { btree_gin, pg_trgm } });
    await runMigrations(client(db), { profile: LOCAL });
  });
  afterAll(async () => { await db?.close(); });
  beforeEach(async () => {
    await db.exec("DELETE FROM sessions.coordination_advisories; DELETE FROM sessions.session_directory_relationships; DELETE FROM sessions.session_directory");
  });

  test("a tick reads candidates as service_role, writes HIGH rows through upsert_v1, DMs and marks them delivered", async () => {
    await dir("org", "ws1", "a", "proj", -10);
    await dir("org", "ws1", "b", "proj", -10);
    await dir("org", "ws2", "a", "proj", -10);           // alone in its workspace: no read at all
    await dir("org", "ws1", "old", "proj", -180);         // outside the 2h window
    const be = backend({ ws1: shared });
    const sender = new InMemoryRelaySender();
    const limits = new AdvisorLimits({ pairCooldownMs: 0, sessionHourlyCap: 3 });
    const tick = advisorTick((q) => ({ store: sqlAdvisorStore(q), backend: be, limits, sender }), { intervalMs: 1 });
    await tick(asService(db), NOW);
    expect(be.reads).toEqual(["org/ws1"]);
    expect(await rows()).toEqual([
      { workspace_id: "ws1", session_id: "a", urgency: "high", status: "delivered", kind: "concurrent_edit" },
      { workspace_id: "ws1", session_id: "b", urgency: "high", status: "delivered", kind: "concurrent_edit" },
    ]);
    expect(sender.sent).toHaveLength(2);
    // Next tick: the rows exist, nothing is resent.
    await tick(asService(db), new Date(NOW.getTime() + 1_000));
    expect(sender.sent).toHaveLength(2);
  });

  test("acked rows are never resent; a planned fleet gets no row", async () => {
    await dir("org", "ws1", "a", "proj", -10);
    await dir("org", "ws1", "b", "proj", -10);
    const store = sqlAdvisorStore(asService(db));
    const sender = new InMemoryRelaySender();
    const limits = () => new AdvisorLimits({ pairCooldownMs: 0, sessionHourlyCap: 3 });
    await scanOnce({ store, backend: backend({ ws1: shared }), limits: limits(), sender }, NOW);
    await db.exec("UPDATE sessions.coordination_advisories SET status='acked', acked_at=now()");
    sender.sent.length = 0;
    const again = await scanOnce({ store, backend: backend({ ws1: shared }), limits: limits(), sender }, NOW);
    expect(again).toMatchObject({ written: 0, existing: 2, dmSent: 0 });
    expect((await rows()).every((r) => r.status === "acked")).toBe(true);

    await db.exec("DELETE FROM sessions.coordination_advisories");
    await dir("org", "ws1", "p", "proj", -200);
    for (const child of ["a", "b"]) await db.query(
      "INSERT INTO sessions.session_directory_relationships (org_id, workspace_id, harness, parent_session_id, relationship_uid, child_session_id) VALUES ('org','ws1','claude','p',$1,$1)", [child]);
    expect(await scanOnce({ store, backend: backend({ ws1: shared }), limits: limits(), sender }, NOW)).toMatchObject({ written: 0 });
    expect(await rows()).toEqual([]);
  });

  test("no downgrade: a LOW upsert over an existing HIGH row leaves it high", async () => {
    const store = sqlAdvisorStore(asService(db));
    const row = { advisory_id: "00000000000000aa", harness: "claude", session_id: "a", peer_harness: "claude", peer_session_id: "b",
      urgency: "high" as const, kind: "concurrent_edit", artifact: "src/x/shared.ts", evidence: [] };
    expect(await store.upsert("org", "ws1", row)).toEqual({ inserted: 1, upgraded: 0, skipped: 0 });
    expect(await store.upsert("org", "ws1", { ...row, urgency: "low" })).toEqual({ inserted: 0, upgraded: 0, skipped: 1 });
    expect((await rows())[0]!.urgency).toBe("high");
    expect(await store.markDelivered("org", "ws1", row.advisory_id, { harness: "claude", sessionId: "a" })).toBe(true);
    expect(await store.markDelivered("org", "ws2", row.advisory_id, { harness: "claude", sessionId: "a" })).toBe(false);
    expect(await store.sweep()).toEqual({ expired: 0, deleted: 0 });
  });
});
