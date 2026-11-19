/**
 * M2-4 against migrated Postgres (pglite) with 062150 (claims) and 062160 (controls) applied, as service_role:
 * claims written through relayfacts_claims_insert_v1 and decisions through relayfacts_insert_v1, read back by the
 * scan through relayfacts_claims_v1, and the advisory rows for each conflict fixture case. Facts come from a fake
 * backend; DMs go to the contract fake (delivered = {delivered, consumed, replied} or accepted_at).
 */
import { afterAll, beforeAll, beforeEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import { PGlite } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/index.js";
import { btree_gin } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/contrib/btree_gin.js";
import { pg_trgm } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/contrib/pg_trgm.js";
import type { LaneFacts } from "../../../packages/coordination-detect/src/index.ts";
import { runMigrations, type MigrationClient, type MigrationProfile } from "../../hoopdb/migrate.ts";
import type { CoordinationBackend, CoordinationFacts } from "../../session-read-api/coordination-backend.ts";
import type { SqlQuery } from "../../session-read-api/liveness.ts";
import { CLAIMS_LIMIT } from "../conflicts.ts";
import { AdvisorLimits } from "../rubric.ts";
import { scanOnce, sqlAdvisorStore, type AdvisorStore } from "../scan.ts";
import { ContractRelayFake } from "./contract-relay-fake.ts";

setDefaultTimeout(300_000);
const LOCAL: MigrationProfile = { mode: "local", shim: true, bootstrap: true, verify: false, runtimeRole: "postgres" };
const CLOCK_MARGIN_MS = 10 * 60_000;
let NOW = new Date(Date.now() + CLOCK_MARGIN_MS);
const at = (min: number) => new Date(NOW.getTime() + min * 60_000).toISOString();
const WS = "ws-c";

function client(db: PGlite): MigrationClient {
  return { async query<R>(text: string, params?: unknown[]) {
    if (params?.length) return (await db.query<R>(text, params)) as { rows: R[] };
    const results = await db.exec(text);
    return { rows: (results.at(-1)?.rows ?? []) as R[] };
  } };
}
const asService = (db: PGlite): SqlQuery => ({
  query: async <R,>(text: string, params?: unknown[]) => db.transaction(async (tx) => {
    await tx.exec("SET LOCAL ROLE service_role");
    return { rows: (await tx.query<R>(text, params ?? [])).rows };
  }),
}) as SqlQuery;

type W = [string, number];
const lane = (sessionId: string, project: string, writes: W[] = []): LaneFacts => ({
  sessionId, source: "claude", userId: null, projectId: project, repo: "o/r", branch: `b/${sessionId}`, taskRef: null, title: null,
  parentSessionId: null, startedAt: at(-90), lastActionAt: at(-5), commits: [], outcomes: [], usage: [],
  writes: writes.map(([path, min]) => ({ path, ts: at(min), eventId: `${sessionId}-${min}`, kind: "tool_result", branch: `b/${sessionId}` })),
});
const backend = (lanes: () => LaneFacts[]): CoordinationBackend => ({
  kind: "clickhouse",
  windowReadiness: async () => { throw new Error("unused"); },
  readFacts: async () => ({ lanes: lanes(), sessionsTruncated: false, notes: [] }) as unknown as CoordinationFacts,
  isSchemaMissing: () => false,
});

/** The fixture cases (src/session-intel/__tests__/fixtures/conflict, by letter), as live directory rows, claims and facts. */
const CV = "No new migrations under supabase/migrations until the 062 renumber lands.";
const SESSIONS: Array<[string, string]> = [
  ["cv-owner", "proj-cv"], ["cv-actor", "proj-cv"], ["ss-owner", "proj-ss"], ["ss-actor", "proj-ss"], ["ic-a", "proj-ic"], ["ic-b", "proj-ic"],
  ["id-a", "proj-id"], ["id-b", "proj-id"], ["sup-a", "proj-sup"], ["sup-b", "proj-sup"], ["fl-lead", "proj-fleet"], ["fl-1", "proj-fleet"],
  ["fl-2", "proj-fleet"], ["cat-a", "proj-cat"], ["cat-b", "proj-cat"], ["col-a", "proj-col"], ["col-b", "proj-col"],
];
const facts = () => [
  lane("cv-owner", "proj-cv"), lane("cv-actor", "proj-cv", [["supabase/migrations/062140_add_claims.sql", -20]]),
  lane("ss-owner", "proj-ss"), lane("ss-actor", "proj-ss", [["supabase/migrations/062141_add_index.sql", -20]]),
  lane("ic-a", "proj-ic"), lane("ic-b", "proj-ic", [["src/advisories/route.ts", -15]]),
  lane("id-a", "proj-id"), lane("id-b", "proj-id"),
  lane("sup-a", "proj-sup"), lane("sup-b", "proj-sup", [["src/readpool/pool.ts", -15]]),
  lane("fl-lead", "proj-fleet"), lane("fl-1", "proj-fleet", [["src/retry/queue.ts", -40]]), lane("fl-2", "proj-fleet", [["src/retry/worker.ts", -15]]),
  lane("cat-a", "proj-cat", [["src/catalog/list.ts", -30]]), lane("cat-b", "proj-cat"),
  // A collision pair with a conflict on top: the collision (HIGH, concurrent_edit) stays.
  lane("col-a", "proj-col", [["src/shared/x.ts", -40]]), lane("col-b", "proj-col", [["src/shared/x.ts", -20]]),
];

describe("conflict rows in the advisor tick (pglite, 062150 and 062160 applied)", () => {
  let db: PGlite;
  const ev = (session: string, n = 0) => ({ source: "claude", session_id: session, turn_start: n, turn_end: n + 1, claim: "c" });
  const svc = async (sql: string, params: unknown[]) => db.transaction(async (tx) => {
    await tx.exec("SET LOCAL ROLE service_role");
    return (await tx.query<Record<string, unknown>>(sql, params)).rows;
  });
  const claim = (list: unknown[]) => svc("SELECT * FROM sessions.relayfacts_claims_insert_v1('org',$1,$2::jsonb)", [WS, JSON.stringify(list)]);
  const decide = (list: unknown[]) => svc("SELECT * FROM sessions.relayfacts_insert_v1('org',$1,$2::jsonb)", [WS, JSON.stringify(list)]);
  const rows = async () => (await db.query<{ session_id: string; peer_session_id: string; urgency: string; status: string; kind: string; artifact: string; evidence: unknown }>(
    "SELECT session_id, peer_session_id, urgency, status, kind, artifact, evidence FROM sessions.coordination_advisories ORDER BY session_id, kind, urgency")).rows;
  const controls = async (features: string[]) => {
    await db.exec("DELETE FROM sessions.session_intel_controls");
    await db.query(`INSERT INTO sessions.session_intel_controls(scope,feature,enabled,updated_by)
      SELECT 'global', f, true, 'fixture' FROM unnest($1::text[]) AS f`, [features]);
    await db.query(`INSERT INTO sessions.session_intel_controls(scope,org_id,workspace_id,feature,enabled,updated_by)
      SELECT 'workspace','org',$2,f,true,'fixture' FROM unnest($1::text[]) AS f`, [features, WS]);
  };
  const scan = (over: { store?: AdvisorStore; sender?: ContractRelayFake; conflicts?: boolean; limits?: AdvisorLimits; now?: Date } = {}) =>
    scanOnce({
      workspaces: "*", store: over.store ?? sqlAdvisorStore(asService(db)), backend: backend(facts),
      limits: over.limits ?? new AdvisorLimits({ pairCooldownMs: 6 * 3_600_000, sessionHourlyCap: 10 }), sender: over.sender ?? new ContractRelayFake(),
      ...(over.conflicts === false ? {} : { conflicts: true }),
    }, over.now ?? NOW);

  beforeAll(async () => {
    db = new PGlite({ extensions: { btree_gin, pg_trgm } });
    await runMigrations(client(db), { profile: LOCAL });
  });
  afterAll(async () => { await db?.close(); });
  beforeEach(async () => {
    NOW = new Date(Date.now() + CLOCK_MARGIN_MS);
    await db.exec(`DELETE FROM sessions.coordination_advisories; DELETE FROM sessions.session_directory_relationships; DELETE FROM sessions.session_directory;
      DELETE FROM sessions.relayfacts_claims_v1; DELETE FROM sessions.relayfacts_v1;`);
    await controls(["advisor", "dm", "conflicts"]);
    for (const [s, p] of SESSIONS) await db.query(
      `INSERT INTO sessions.session_directory (org_id, workspace_id, harness, session_id, created_at, last_activity_at, project_id)
       VALUES ('org',$1,'claude',$2,$3,$4,$5)`, [WS, s, at(-120), at(-5), p]);
    for (const child of ["fl-1", "fl-2"]) await db.query(
      "INSERT INTO sessions.session_directory_relationships (org_id, workspace_id, harness, parent_session_id, relationship_uid, child_session_id) VALUES ('org',$1,'claude','fl-lead',$2,$2)", [WS, child]);
    const c = (kind: string, scope: string, topic: string, title: string, statement: string, session: string, observed: number) =>
      ({ kind, scope, topic, title, statement, observed_at: at(observed), evidence: [ev(session)] });
    await claim([
      c("constraint", "project", "supabase-migrations", "No new migrations", CV, "cv-owner", -50),
      c("constraint", "session", "supabase-migrations", "No new migrations here", CV, "ss-owner", -50),
      c("intent", "session", "src-advisories", "Return 404", "Return 404 when the flag is off in src/advisories routes.", "ic-b", -16),
      c("intent", "session", "src-flags", "Return 404 flags", "Return 404 when the flag is off in src/flags.", "id-b", -16),
      c("intent", "session", "src-retry", "Disable retry", "Disable the retry queue in src/retry.", "fl-2", -16),
      c("intent", "session", "src-catalog", "Pagination", "Add pagination to the list route in src/catalog.", "cat-a", -40),
      c("intent", "session", "src-shared", "Disable x", "Disable the shared x helper in src/shared.", "col-b", -25),
    ]);
    const d = (topic: string, title: string, statement: string, session: string) => ({ topic, title, statement, evidence: [ev(session)] });
    await decide([
      d("src-advisories", "501 when off", "Advisories return 501 when the flag is off in src/advisories.", "ic-a"),
      d("src-flags", "501 when off flags", "Advisories return 501 when the flag is off in src/flags.", "id-a"),
      d("src-retry", "Enable retry", "Enable the retry queue in src/retry.", "fl-1"),
      d("src-catalog", "Trigram search", "Search in src/catalog uses a trigram index.", "cat-b"),
      d("src-shared", "Enable x", "Enable the shared x helper in src/shared.", "col-a"),
    ]);
    // (e): a superseded decision is never read: sup-a's intent is replaced, and the reader returns current claims only.
    await claim([c("intent", "project", "src-readpool", "Use pgbouncer", "Use pgbouncer for the read pool in src/readpool.", "sup-a", -60)]);
    const ids = await db.query<{ id: string; title: string }>("SELECT id, title FROM sessions.relayfacts_claims_v1 WHERE title IN ('Use pgbouncer')");
    await claim([c("intent", "project", "src-readpool", "Pool directly", "Keep the read pool in src/readpool simple.", "sup-a", -30)]);
    const fresh = await db.query<{ id: string }>("SELECT id FROM sessions.relayfacts_claims_v1 WHERE title='Pool directly'");
    await svc("SELECT sessions.relayfacts_claims_supersede_v1('org',$1,$2::uuid,$3::uuid)", [WS, ids.rows[0]!.id, fresh.rows[0]!.id]);
    await claim([c("intent", "session", "src-readpool", "No pgbouncer", "Don't use pgbouncer in src/readpool; connect directly.", "sup-b", -16)]);
  });

  test("each fixture case gives its rows", async () => {
    const sender = new ContractRelayFake();
    const stats = await scan({ sender });
    expect(stats).toMatchObject({ failed: 0, rejected: 0, conflictsSkipped: 0 });
    const all = await rows();
    const by = (s: string) => all.filter((r) => r.session_id === s).map((r) => `${r.kind}/${r.urgency}/${r.status}/${r.peer_session_id}`);
    // (a) HIGH constraint_violation to the violator and the constraint's holder, DMed and delivered.
    expect(by("cv-actor")).toEqual(["constraint_violation/high/delivered/cv-owner"]);
    expect(by("cv-owner")).toEqual(["constraint_violation/high/delivered/cv-actor"]);
    // (b) a session-scope constraint never propagates.
    expect([...by("ss-owner"), ...by("ss-actor")]).toEqual([]);
    // (c) HIGH intent_conflict, both DMed.
    expect(by("ic-a")).toEqual(["intent_conflict/high/delivered/ic-b"]);
    expect(by("ic-b")).toEqual(["intent_conflict/high/delivered/ic-a"]);
    // (d) LOW intent_conflict, pull only.
    expect(by("id-a")).toEqual(["intent_conflict/low/pending/id-b"]);
    expect(by("id-b")).toEqual(["intent_conflict/low/pending/id-a"]);
    // (e) the superseded claim is never used: no conflict for the pair (a compatible current claim is an overlap at most).
    expect([...by("sup-a"), ...by("sup-b")].filter((r) => !r.startsWith("overlap/low"))).toEqual([]);
    // (f) the fleet conflict: one LOW pull row to the orchestrator, none to the workers.
    expect(by("fl-lead")).toEqual(["intent_conflict/low/pending/fl-2"]);
    expect([...by("fl-1"), ...by("fl-2")]).toEqual([]);
    // (g) compatible claims, one side writing: overlap LOW, never a DM.
    expect(by("cat-a")).toEqual(["overlap/low/pending/cat-b"]);
    // A conflict never suppresses a collision: the pair gets both rows in one tick.
    expect(by("col-a")).toEqual(["concurrent_edit/high/delivered/col-b", "intent_conflict/high/delivered/col-b"]);
    expect(by("col-b")).toEqual(["concurrent_edit/high/delivered/col-a", "intent_conflict/high/delivered/col-a"]);
    // The DMs: HIGH only, to sessions; every one delivered under the contract's set rule.
    expect(sender.sent.map((d) => d.to.sessionId).sort()).toEqual(["col-a", "col-a", "col-b", "col-b", "cv-actor", "cv-owner", "ic-a", "ic-b"].sort());
    // Evidence: cited claim ids and the action event id, as 062000 strings.
    const cv = all.find((r) => r.session_id === "cv-actor")!;
    expect((cv.evidence as Array<{ kind: string; eventId: string }>).map((e) => e.kind)).toEqual(["claim_constraint", "write"]);
    expect(cv.artifact).toBe("supabase-migrations");
    const text = sender.sent.find((d) => d.to.sessionId === "cv-actor")!.text;
    expect(text).toContain("possible constraint violation");
    expect(text).toContain(`Quoted constraint claim: "${CV}"`);
    expect(text.length).toBeLessThanOrEqual(600);
  });

  test("(h) a claim observed after now is not visible: no row until a later scan", async () => {
    await claim([{ kind: "constraint", scope: "project", topic: "src-ledger", title: "Ledger freeze", statement: "Don't edit src/ledger until the freeze ends.",
      observed_at: at(5), evidence: [ev("lk-owner")] }]);
    for (const s of ["lk-owner", "lk-actor"]) await db.query(
      `INSERT INTO sessions.session_directory (org_id, workspace_id, harness, session_id, created_at, last_activity_at, project_id)
       VALUES ('org',$1,'claude',$2,$3,$4,'proj-leak')`, [WS, s, at(-120), at(-5)]);
    const withLeak = () => [...facts(), lane("lk-owner", "proj-leak"), lane("lk-actor", "proj-leak", [["src/ledger/post.ts", 7]])];
    const store = sqlAdvisorStore(asService(db));
    const run = (now: Date) => scanOnce({ workspaces: "*", store, backend: backend(withLeak), conflicts: true,
      limits: new AdvisorLimits({ pairCooldownMs: 0, sessionHourlyCap: 10 }) }, now);
    await run(NOW);
    expect((await rows()).filter((r) => r.session_id.startsWith("lk-"))).toEqual([]);
    await run(new Date(NOW.getTime() + 10 * 60_000));
    expect((await rows()).filter((r) => r.session_id.startsWith("lk-")).map((r) => `${r.session_id}/${r.kind}/${r.urgency}`).sort())
      .toEqual(["lk-actor/constraint_violation/high", "lk-owner/constraint_violation/high"]);
  });

  test("the flag off or the conflicts control off: no claims read, collision and overlap rows only", async () => {
    let reads = 0;
    const base = sqlAdvisorStore(asService(db));
    const spy: AdvisorStore = { ...base, claims: async (...a) => { reads++; return base.claims!(...a); } };
    await scan({ store: spy, conflicts: false });
    expect(reads).toBe(0);
    expect((await rows()).map((r) => r.kind)).toEqual(["concurrent_edit", "concurrent_edit"]);
    await db.exec("DELETE FROM sessions.coordination_advisories");
    await controls(["advisor", "dm"]);
    await scan({ store: spy });
    expect(reads).toBe(0);
    expect((await rows()).map((r) => r.kind)).toEqual(["concurrent_edit", "concurrent_edit"]);
  });

  test("a claims read error, a missing 062150 (42883) or a full read skips conflicts and keeps the collision rows", async () => {
    const base = sqlAdvisorStore(asService(db));
    for (const claims of [
      async () => { throw new Error("boom"); },
      async () => { throw Object.assign(new Error("function does not exist"), { code: "42883" }); },
      async () => Array.from({ length: CLAIMS_LIMIT }, () => (({}) as never)),
    ]) {
      await db.exec("DELETE FROM sessions.coordination_advisories");
      const stats = await scan({ store: { ...base, claims } });
      expect(stats).toMatchObject({ failed: 0, conflictsSkipped: 1, conflicts: 0 });
      expect((await rows()).map((r) => r.kind)).toEqual(["concurrent_edit", "concurrent_edit"]);
    }
  });

  test("a duplicate_work and a constraint_violation for one pair in one tick: both rows; the next tick is cooled down", async () => {
    for (const s of ["dup-a", "dup-b"]) await db.query(
      `INSERT INTO sessions.session_directory (org_id, workspace_id, harness, session_id, created_at, last_activity_at, project_id)
       VALUES ('org',$1,'claude',$2,$3,$4,'proj-dup')`, [WS, s, at(-120), at(-5)]);
    await claim([{ kind: "constraint", scope: "project", topic: "supabase-migrations", title: "Hold migrations", statement: CV,
      observed_at: at(-50), evidence: [ev("dup-a")] }]);
    const withDup = () => [...facts(), lane("dup-a", "proj-dup", [["supabase/migrations/062170_a.sql", -40]]),
      lane("dup-b", "proj-dup", [["supabase/migrations/062170_b.sql", -20]])];
    const limits = new AdvisorLimits({ pairCooldownMs: 6 * 3_600_000, sessionHourlyCap: 10 });
    const run = (now: Date) => scanOnce({ workspaces: "*", store: sqlAdvisorStore(asService(db)), backend: backend(withDup), conflicts: true, limits,
      sender: new ContractRelayFake() }, now);
    await run(NOW);
    const dup = async () => (await rows()).filter((r) => r.session_id.startsWith("dup-")).map((r) => `${r.session_id}/${r.kind}/${r.urgency}`);
    expect(await dup()).toEqual([
      "dup-a/constraint_violation/high", "dup-a/duplicate_work/high", "dup-b/constraint_violation/high", "dup-b/duplicate_work/high",
    ]);
    const next = await run(new Date(NOW.getTime() + 2 * 60_000));
    expect(next.written).toBe(0);
    expect(next.cooldown).toBeGreaterThanOrEqual(2);
    expect(await dup()).toHaveLength(4);
  });

  test("the cooldown and the per-recipient cap hold for conflict rows", async () => {
    const limits = new AdvisorLimits({ pairCooldownMs: 6 * 3_600_000, sessionHourlyCap: 10 });
    await scan({ limits });
    const first = (await rows()).length;
    const again = await scan({ limits, now: new Date(NOW.getTime() + 60_000) });
    expect(again.written).toBe(0);
    expect(again.cooldown).toBeGreaterThan(0);
    expect((await rows()).length).toBe(first);
    await db.exec("DELETE FROM sessions.coordination_advisories");
    // Cap 0: no recipient may get a row this hour, conflict or not.
    const capped = await scan({ limits: new AdvisorLimits({ pairCooldownMs: 0, sessionHourlyCap: 0 }) });
    expect(capped.written).toBe(0);
    expect(capped.capped).toBeGreaterThan(0);
    expect(await rows()).toEqual([]);
    // Cap 1: a recipient in two conflicts this hour gets one row (cv-owner holds the constraint for two violators).
    await db.query(`INSERT INTO sessions.session_directory (org_id, workspace_id, harness, session_id, created_at, last_activity_at, project_id)
      VALUES ('org',$1,'claude','cv-actor2',$2,$3,'proj-cv')`, [WS, at(-120), at(-5)]);
    const two = () => [...facts(), lane("cv-actor2", "proj-cv", [["supabase/migrations/062150_more.sql", -10]])];
    await scanOnce({ workspaces: "*", store: sqlAdvisorStore(asService(db)), backend: backend(two), conflicts: true,
      limits: new AdvisorLimits({ pairCooldownMs: 0, sessionHourlyCap: 1 }) }, NOW);
    expect((await rows()).filter((r) => r.session_id === "cv-owner")).toHaveLength(1);
    expect((await rows()).filter((r) => r.session_id.startsWith("cv-actor"))).toHaveLength(2);
  });

  test("a DM the contract does not count as delivered leaves the row pending for the pull path", async () => {
    for (const outcome of [{ state: "queued" as const, acceptedAt: null }, { state: "failed" as const, acceptedAt: null }, { state: null, acceptedAt: null }]) {
      await db.exec("DELETE FROM sessions.coordination_advisories");
      const sender = new ContractRelayFake();
      sender.outcome = outcome;
      const stats = await scan({ sender });
      expect(stats.dmSent).toBe(0);
      expect((await rows()).filter((r) => r.urgency === "high").every((r) => r.status === "pending")).toBe(true);
    }
    // failed with accepted_at kept counts as delivered (the set rule, not an ordered chain).
    await db.exec("DELETE FROM sessions.coordination_advisories");
    const sender = new ContractRelayFake();
    sender.outcome = { state: "failed", acceptedAt: at(0) };
    await scan({ sender });
    expect((await rows()).filter((r) => r.urgency === "high").every((r) => r.status === "delivered")).toBe(true);
  });
});
