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
import { advisorTick, CANDIDATES_PER_WORKSPACE, scanOnce, sqlAdvisorStore } from "../scan.ts";
import { postgresAdvisorBackend, productionIo } from "../main.ts";
import { advisorConfig } from "../config.ts";

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
    const tick = advisorTick((q) => ({ workspaces: "*", store: sqlAdvisorStore(q), backend: be, limits, sender }), { intervalMs: 1 });
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
    await scanOnce({ workspaces: "*", store, backend: backend({ ws1: shared }), limits: limits(), sender }, NOW);
    await db.exec("UPDATE sessions.coordination_advisories SET status='acked', acked_at=now()");
    sender.sent.length = 0;
    const again = await scanOnce({ workspaces: "*", store, backend: backend({ ws1: shared }), limits: limits(), sender }, NOW);
    expect(again).toMatchObject({ written: 0, existing: 2, dmSent: 0 });
    expect((await rows()).every((r) => r.status === "acked")).toBe(true);

    await db.exec("DELETE FROM sessions.coordination_advisories");
    await dir("org", "ws1", "p", "proj", -200);
    for (const child of ["a", "b"]) await db.query(
      "INSERT INTO sessions.session_directory_relationships (org_id, workspace_id, harness, parent_session_id, relationship_uid, child_session_id) VALUES ('org','ws1','claude','p',$1,$1)", [child]);
    expect(await scanOnce({ workspaces: "*", store, backend: backend({ ws1: shared }), limits: limits(), sender }, NOW)).toMatchObject({ written: 0 });
    expect(await rows()).toEqual([]);
  });

  test("a restart keeps the cooldown and the cap: fresh limits are seeded from recent_v1 (062070)", async () => {
    // recent_v1 counts against the database clock, so this test runs on the real clock.
    const now = new Date();
    const rel = (min: number) => new Date(now.getTime() + min * 60_000).toISOString();
    for (const s of ["a", "b"]) await db.query(
      `INSERT INTO sessions.session_directory (org_id, workspace_id, harness, session_id, created_at, last_activity_at, project_id)
       VALUES ('org','ws1','claude',$1,$2,$3,'proj')`, [s, rel(-120), rel(-10)]);
    const live = ["a", "b"].map((id, i) => ({ ...lane(id, []), startedAt: rel(-90), lastActionAt: rel(-5),
      writes: [{ path: "src/x/shared.ts", ts: rel(-40 + i * 20), eventId: `${id}-w`, kind: "tool", branch: `b/${id}` }] }));
    const store = sqlAdvisorStore(asService(db));
    const first = await scanOnce({ workspaces: "*", store, backend: backend({ ws1: live }), limits: new AdvisorLimits({ pairCooldownMs: 6 * 3_600_000, sessionHourlyCap: 3 }) }, now);
    expect(first).toMatchObject({ written: 2 });
    // "Restart": new in-memory limits, same database.
    const later = new Date(now.getTime() + 1_000);
    expect(await scanOnce({ workspaces: "*", store, backend: backend({ ws1: live }), limits: new AdvisorLimits({ pairCooldownMs: 6 * 3_600_000, sessionHourlyCap: 3 }) }, later))
      .toMatchObject({ cooldown: 1, written: 0, existing: 0 });
    expect(await scanOnce({ workspaces: "*", store, backend: backend({ ws1: live }), limits: new AdvisorLimits({ pairCooldownMs: 0, sessionHourlyCap: 1 }) }, later))
      .toMatchObject({ capped: 2, written: 0, existing: 0 });
    // Another tenant's rows never seed this one.
    expect(await store.recent("org", "ws2", new Date(now.getTime() - 86_400_000))).toEqual([]);
  });

  test("postgres facts backend: a tick reads facts through the 062070 readers as service_role, tenant-scoped", async () => {
    const now = new Date();
    const rel = (min: number) => new Date(now.getTime() + min * 60_000).toISOString();
    await db.exec("DELETE FROM sessions.session_event_facts");
    for (const s of ["a", "b"]) await db.query(
      `INSERT INTO sessions.session_directory (org_id, workspace_id, harness, session_id, created_at, last_activity_at, project_id)
       VALUES ('org','ws1','claude',$1,$2,$3,'proj')`, [s, rel(-120), rel(-10)]);
    const fact = (org: string, session: string, min: number, row: string) => db.query(
      `INSERT INTO sessions.session_event_facts (org_id,workspace_id,ts,session_id,source,machine_id,kind,event_id,project_id,branch,
         input_tokens,output_tokens,reasoning_tokens,cache_read_tokens,cache_create_tokens,files_touched,row_id,fingerprint,source_version)
       VALUES ($1,'ws1',$2,$3,'claude','m','tool',$4,'proj','b/' || $3,0,0,0,0,0,ARRAY['src/x/shared.ts'],$5,'f',1)`,
      [org, rel(min), session, `${session}-${min}`, row.repeat(64)]);
    await fact("org", "a", -40, "1");
    await fact("org", "b", -20, "2");
    await fact("other-org", "a", -30, "3");
    const be = postgresAdvisorBackend(asService(db));
    expect(be.kind).toBe("postgres");
    const facts = await be.readFacts(sqlAdvisorStore(asService(db)).directory(),
      { orgId: "org", workspaceId: "ws1", since: new Date(now.getTime() - 2 * 3_600_000), until: now });
    expect(facts.lanes.map((l) => [l.sessionId, l.writes.map((w) => w.eventId)]).sort()).toEqual([["a", ["a--40"]], ["b", ["b--20"]]]);
    const sender = new InMemoryRelaySender();
    const stats = await scanOnce({ workspaces: "*", store: sqlAdvisorStore(asService(db)), backend: be, limits: new AdvisorLimits({ pairCooldownMs: 0, sessionHourlyCap: 3 }), sender }, now);
    expect(stats).toMatchObject({ written: 2, failed: 0 });
    expect((await rows()).map((r) => [r.session_id, r.urgency, r.status])).toEqual([["a", "high", "delivered"], ["b", "high", "delivered"]]);
    await expect(be.windowReadiness("org", "ws1", now, now)).rejects.toThrow(/not used by the worker/);
    // productionIo selects it for COORDINATION_ADVISOR_FACTS_BACKEND=postgres without ClickHouse configuration.
    const cfg = advisorConfig({ COORDINATION_ADVISOR_ENABLED: "true", COORDINATION_ADVISOR_FACTS_BACKEND: "postgres" });
    expect(productionIo.backend(cfg, {})(asService(db)).kind).toBe("postgres");
  });

  test("candidates: the workspace allowlist and the per-workspace cap apply in SQL", async () => {
    await db.query(
      `INSERT INTO sessions.session_directory (org_id, workspace_id, harness, session_id, created_at, last_activity_at, project_id)
       SELECT 'org', 'ws1', 'claude', 's' || g, $1, $2::timestamptz - make_interval(secs => g), 'proj' FROM generate_series(1, $3::int) g`,
      [at(-120), at(-1), CANDIDATES_PER_WORKSPACE + 5]);
    await dir("org", "ws2", "only", "proj", -3);
    await dir("org", "ws3", "other", "proj", -2);
    const store = sqlAdvisorStore(asService(db));
    const since = new Date(NOW.getTime() - 2 * 3_600_000);
    const all = await store.candidates(since, 10_000, null);
    expect(all.filter((r) => r.workspaceId === "ws1")).toHaveLength(CANDIDATES_PER_WORKSPACE);
    expect(all.filter((r) => r.workspaceId === "ws1").at(-1)!.sessionId).toBe(`s${CANDIDATES_PER_WORKSPACE}`);
    expect(all.filter((r) => r.workspaceId !== "ws1").map((r) => r.sessionId).sort()).toEqual(["only", "other"]);
    expect((await store.candidates(since, 10_000, ["ws2"])).map((r) => r.sessionId)).toEqual(["only"]);
    expect(await store.candidates(since, 10_000, [])).toEqual([]);
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
