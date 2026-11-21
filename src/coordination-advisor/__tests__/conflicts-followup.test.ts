/**
 * M2-4 follow-up (the lander's review): the cooldown per source class across ticks and through durable state (M1),
 * the live conflict path's CPU bounds (M3), and one advisory whose DM text cannot be built (L1). Fake store and
 * backend; the store's `recent` mirrors 062070 from the rows it holds.
 */
import { describe, expect, test } from "bun:test";
import type { LaneFacts } from "../../../packages/coordination-detect/src/index.ts";
import type { CoordinationBackend, CoordinationFacts } from "../../session-read-api/coordination-backend.ts";
import type { Directory } from "../../session-read-api/directory.ts";
import { SESSION_INTEL_FEATURES, type SessionIntelControls } from "../../shared/session-intel-controls.ts";
import {
  CONFLICT_MAX_INPUTS, CONFLICT_STEP_BUDGET, liveConflictAdvisories, liveSessions, parseClaimRows, type ClaimRow, type ConflictAdvisory,
} from "../conflicts.ts";
import { InMemoryRelaySender } from "../relay-sender.ts";
import { AdvisorLimits, pairKey, type Advisory, type CandidateLane, type RecentAdvisory } from "../rubric.ts";
import { deliver, scanOnce, type AdvisorStore, type AdvisoryRowInput, type ScanStats, type UpsertResult } from "../scan.ts";

const NOW = new Date(Date.UTC(2026, 10, 17, 12));
const at = (min: number, base = NOW) => new Date(base.getTime() + min * 60_000).toISOString();
const CV = "No new migrations under supabase/migrations until the 062 renumber lands.";
const cand = (sessionId: string, over: Partial<CandidateLane> = {}): CandidateLane => ({
  orgId: "org", workspaceId: "ws", harness: "claude", sessionId, projectId: "proj", repo: "o/r",
  parentSessionIds: [], shortSummary: null, lastActivityAt: at(-5), ...over,
});
const lane = (sessionId: string, writes: Array<[string, string]>): LaneFacts => ({
  sessionId, source: "claude", userId: null, projectId: "proj", repo: "o/r", branch: `b/${sessionId}`, taskRef: null, title: null,
  parentSessionId: null, startedAt: at(-90), lastActionAt: at(-5), commits: [], outcomes: [], usage: [],
  writes: writes.map(([path, ts], i) => ({ path, ts, eventId: `${sessionId}-${i}`, kind: "tool", branch: `b/${sessionId}` })),
});
const constraintRow = (holder: string, workspaceId = "ws", id = `c-${holder}`): Record<string, unknown> => ({
  id, kind: "constraint", scope: "project", topic: "supabase-migrations", statement: CV, observed_at: at(-50), created_at: at(-50),
  evidence: [{ id: 1, source: "claude", session_id: holder, created_at: at(-50) }], workspaceId,
});

/** A store mirroring 062000's upsert and 062070's recent rows (pair: latest per recipient, peer, urgency). */
class DurableStore implements AdvisorStore {
  featureControls: SessionIntelControls = new Map(SESSION_INTEL_FEATURES.map((f) => [f, true]));
  rows = new Map<string, AdvisoryRowInput & { createdAt: number }>();
  clock = NOW.getTime();
  claimRows = new Map<string, ClaimRow[]>();
  constructor(private lanes: CandidateLane[]) {}
  async controls() { return this.featureControls; }
  async candidates() { return this.lanes; }
  directory(): Directory { return {} as Directory; }
  async upsert(_o: string, _w: string, row: AdvisoryRowInput): Promise<UpsertResult> {
    const k = `${row.advisory_id}|${row.harness}|${row.session_id}`;
    if (this.rows.has(k)) return { inserted: 0, upgraded: 0, skipped: 1 };
    this.rows.set(k, { ...row, createdAt: this.clock });
    return { inserted: 1, upgraded: 0, skipped: 0 };
  }
  async markDelivered() { return true; }
  async sweep() { return { expired: 0, deleted: 0 }; }
  async recent(_o: string, _w: string, pairSince: Date): Promise<RecentAdvisory[]> {
    const out = new Map<string, RecentAdvisory>();
    for (const r of this.rows.values()) {
      if (r.createdAt < pairSince.getTime()) continue;
      const recipient = { harness: r.harness, sessionId: r.session_id }, peer = { harness: r.peer_harness, sessionId: r.peer_session_id };
      const k = `${r.harness}|${r.session_id}|${r.peer_harness}|${r.peer_session_id}|${r.urgency}`;
      const cur = out.get(k);
      if (!cur || (cur.kind === "pair" && r.createdAt > cur.latestAt)) out.set(k, { kind: "pair", recipient, peer, urgency: r.urgency, latestAt: r.createdAt });
    }
    return [...out.values()];
  }
  async claims(_o: string, workspaceId: string) { return this.claimRows.get(workspaceId) ?? []; }
  kinds = () => [...this.rows.values()].map((r) => `${r.session_id}/${r.kind}/${r.urgency}`).sort();
}
const backend = (lanes: () => LaneFacts[]): CoordinationBackend => ({
  kind: "clickhouse",
  windowReadiness: async () => { throw new Error("unused"); },
  readFacts: async () => ({ lanes: lanes(), sessionsTruncated: false, notes: [] }) as unknown as CoordinationFacts,
  isSchemaMissing: () => false,
});
const limits = () => new AdvisorLimits({ pairCooldownMs: 6 * 3_600_000, sessionHourlyCap: 10 });

describe("M1: the pair cooldown is per source class", () => {
  const A = { harness: "claude", sessionId: "a" }, B = { harness: "claude", sessionId: "b" };
  const T0 = NOW.getTime();
  test("a conflict write cools down only conflicts; an advisor write only the advisor's rows; the default is the advisor", () => {
    const l = limits();
    l.touchPair(pairKey(A, B), "high", T0, "conflict");
    expect(l.inCooldown(pairKey(A, B), "high", T0 + 1, "conflict")).toBe(true);
    expect(l.inCooldown(pairKey(A, B), "high", T0 + 1, "advisor")).toBe(false);
    expect(l.inCooldown(pairKey(A, B), "low", T0 + 1)).toBe(false);
    l.touchPair(pairKey(A, B), "low", T0);
    expect(l.inCooldown(pairKey(A, B), "low", T0 + 1)).toBe(true);
    expect(l.inCooldown(pairKey(A, B), "high", T0 + 1)).toBe(false);
  });

  test("durable rows: attributed to the class this process holds; unattributable ones (a restart) hold back both; the cap is kept", () => {
    const durable: RecentAdvisory[] = [
      { kind: "pair", recipient: A, peer: B, urgency: "high", latestAt: T0 + 5 },
      { kind: "recipient", recipient: A, latestAt: T0, sentLastHour: 10 },
    ];
    const live = limits();
    live.touchPair(pairKey(A, B), "high", T0, "conflict");
    live.seed(durable, T0 + 10);
    expect(live.inCooldown(pairKey(A, B), "high", T0 + 10, "conflict")).toBe(true);
    expect(live.inCooldown(pairKey(A, B), "high", T0 + 10, "advisor")).toBe(false);
    const restarted = limits();
    restarted.seed(durable, T0 + 10);
    expect(restarted.inCooldown(pairKey(A, B), "high", T0 + 10, "conflict")).toBe(true);
    expect(restarted.inCooldown(pairKey(A, B), "high", T0 + 10, "advisor")).toBe(true);
    expect(restarted.inCooldown(pairKey(A, B), "high", T0 + 6 * 3_600_000 + 5, "advisor")).toBe(false);
    for (const l of [live, restarted]) expect(l.underCap(A, T0 + 10)).toBe(false);
    // Seeding again after the restart does not drop or re-attribute the source-blind state.
    restarted.seed(durable, T0 + 20);
    expect(restarted.inCooldown(pairKey(A, B), "high", T0 + 20, "advisor")).toBe(true);
  });

  test("durable rows with both classes held: neither class's cooldown is extended by the other's write", () => {
    const COOL = 6 * 3_600_000, later = T0 + 30 * 60_000;
    const l = limits();
    l.touchPair(pairKey(A, B), "high", T0, "advisor");
    l.touchPair(pairKey(A, B), "high", later, "conflict");
    // The durable latest row for the pair is the conflict's write (a few ms after this process's clock).
    l.seed([{ kind: "pair", recipient: A, peer: B, urgency: "high", latestAt: later + 5 }], later + 10);
    expect(l.inCooldown(pairKey(A, B), "high", T0 + COOL, "advisor")).toBe(false);
    expect(l.inCooldown(pairKey(A, B), "high", T0 + COOL, "conflict")).toBe(true);
    expect(l.inCooldown(pairKey(A, B), "high", later + COOL, "conflict")).toBe(false);
    // One class held: the durable row is that class's, as before (the advisor's, with conflicts off).
    const one = limits();
    one.touchPair(pairKey(A, B), "high", T0);
    one.seed([{ kind: "pair", recipient: A, peer: B, urgency: "high", latestAt: T0 + 60_000 }], T0 + 60_000);
    expect(one.inCooldown(pairKey(A, B), "high", T0 + COOL + 30_000)).toBe(true);
    expect(one.inCooldown(pairKey(A, B), "high", T0 + COOL + 30_000, "conflict")).toBe(false);
  });

  test("a tick-N conflict does not suppress a collision that first appears in tick N+1 on the same pair", async () => {
    const store = new DurableStore([cand("a"), cand("b")]);
    store.claimRows.set("ws", parseClaimRows([constraintRow("a")]));
    let tick = 0;
    // Tick N: b writes a migration against a's constraint (a conflict, no collision). Tick N+1: a writes the same file.
    const facts = () => tick === 0
      ? [lane("a", []), lane("b", [["supabase/migrations/062140_x.sql", at(-20)]])]
      : [lane("a", [["supabase/migrations/062140_x.sql", at(-1)]]), lane("b", [["supabase/migrations/062140_x.sql", at(-20)]])];
    const l = limits();
    const sender = new InMemoryRelaySender();
    const run = (now: Date) => { store.clock = now.getTime(); return scanOnce({ workspaces: "*", store, backend: backend(facts), limits: l, sender, conflicts: true }, now); };
    expect(await run(NOW)).toMatchObject({ written: 2, conflicts: 1, failed: 0 });
    expect(store.kinds()).toEqual(["a/constraint_violation/high", "b/constraint_violation/high"]);
    tick = 1;
    const next = await run(new Date(NOW.getTime() + 2 * 60_000));
    expect(next).toMatchObject({ written: 2, cooldown: 1, failed: 0 });
    expect(store.kinds()).toEqual(["a/constraint_violation/high", "a/duplicate_work/high", "b/constraint_violation/high", "b/duplicate_work/high"]);
    // Within each class the cooldown is unchanged: tick N+2 writes nothing.
    expect(await run(new Date(NOW.getTime() + 4 * 60_000))).toMatchObject({ written: 0, cooldown: 2 });
    // A restart (fresh in-memory limits) seeded from the durable rows holds back both classes: nothing resent.
    const restarted = await scanOnce({ workspaces: "*", store, backend: backend(facts), limits: limits(), sender, conflicts: true }, new Date(NOW.getTime() + 6 * 60_000));
    expect(restarted).toMatchObject({ written: 0, cooldown: 2, failed: 0 });
  });

  test("conflicts off: the cooldown is exactly the advisor's, as before", async () => {
    const store = new DurableStore([cand("a"), cand("b")]);
    store.claimRows.set("ws", parseClaimRows([constraintRow("a")]));
    const facts = () => [lane("a", [["src/x/s.ts", at(-40)]]), lane("b", [["src/x/s.ts", at(-20)]])];
    const l = limits();
    const first = await scanOnce({ workspaces: "*", store, backend: backend(facts), limits: l }, NOW);
    expect(first).toEqual({ candidates: 2, workspaces: 1, findings: 1, written: 2, existing: 0, cooldown: 0, capped: 0, dmSent: 0, dmFailed: 0, truncated: 0, failed: 0, rejected: 0 });
    expect(await scanOnce({ workspaces: "*", store, backend: backend(facts), limits: l }, new Date(NOW.getTime() + 60_000))).toMatchObject({ written: 0, cooldown: 1 });
    expect(await scanOnce({ workspaces: "*", store, backend: backend(facts), limits: limits() }, new Date(NOW.getTime() + 120_000))).toMatchObject({ written: 0, cooldown: 1 });
  });
});

describe("M3: the live conflict path is bounded per workspace", () => {
  const many = (sessionId: string, n: number, base = NOW) => lane(sessionId, Array.from({ length: n }, (_, i) => [`supabase/migrations/${i}.sql`, at(-60 + (i % 50), base)] as [string, string]));
  test("over the input cap or the step budget: no conflicts, counted; under them: unchanged", () => {
    const holders = Array.from({ length: 10 }, (_, i) => `h${i}`);
    const claims = parseClaimRows(holders.map((h) => constraintRow(h)));
    const sessions = (writes: number) => liveSessions([cand("actor"), ...holders.map((h) => cand(h))], [many("actor", writes)], claims, NOW);
    const t0 = performance.now();
    const huge = liveConflictAdvisories("ws", sessions(CONFLICT_MAX_INPUTS), NOW);
    expect(huge.overBudget).toBe("inputs");
    expect(huge.advisories).toEqual([]);
    expect(performance.now() - t0).toBeLessThan(1_000);
    // Under the input cap but over the step budget.
    const steps = liveConflictAdvisories("ws", sessions(2_000), NOW, { stepBudget: 10_000 });
    expect(steps).toMatchObject({ overBudget: "steps", advisories: [] });
    expect(steps.stats.steps).toBeGreaterThan(10_000);
    expect(steps.stats.steps).toBeLessThan(10_000 + 2 * 2_011);
    // A normal workspace: well inside both bounds, every violation found.
    const normal = liveConflictAdvisories("ws", sessions(20), NOW);
    expect(normal.overBudget).toBeUndefined();
    expect(normal.stats.steps).toBeLessThan(CONFLICT_STEP_BUDGET / 100);
    expect(normal.advisories.filter((a) => a.kind === "constraint_violation" && a.urgency === "high")).toHaveLength(10);
  });

  test("in the scan: the huge workspace's conflicts are skipped and counted; its collisions and the other workspace go on", async () => {
    const lanes = [cand("a"), cand("b"), cand("x", { workspaceId: "ws2" }), cand("y", { workspaceId: "ws2" })];
    const store = new DurableStore(lanes);
    store.claimRows.set("ws", parseClaimRows([constraintRow("a")]));
    store.claimRows.set("ws2", parseClaimRows([constraintRow("x", "ws2")]));
    const huge = { ...many("b", 300), writes: [...many("b", 300).writes, { path: "src/x/s.ts", ts: at(-20), eventId: "b-s", kind: "tool", branch: "b/b" }] };
    const be: CoordinationBackend = {
      kind: "clickhouse",
      windowReadiness: async () => { throw new Error("unused"); },
      readFacts: async (_d, scope) => ({ lanes: scope.workspaceId === "ws" ? [lane("a", [["src/x/s.ts", at(-40)]]), huge] : [lane("x", []), lane("y", [["supabase/migrations/1.sql", at(-20)]])],
        sessionsTruncated: false, notes: [] }) as unknown as CoordinationFacts,
      isSchemaMissing: () => false,
    };
    const stats = await scanOnce({ workspaces: "*", store, backend: be, limits: limits(), conflicts: true, conflictBounds: { stepBudget: 500 } }, NOW);
    expect(stats).toMatchObject({ failed: 0, conflictsSkipped: 1, conflicts: 1 });
    expect(store.kinds()).toEqual(["a/concurrent_edit/high", "b/concurrent_edit/high", "x/constraint_violation/high", "y/constraint_violation/high"]);
  });
});

describe("L1: an advisory whose DM text cannot be built", () => {
  test("is skipped and counted, with no row; the workspace's other advisories are written and DMed", async () => {
    const a = { harness: "claude", sessionId: "a" }, b = { harness: "claude", sessionId: "b" }, c = { harness: "claude", sessionId: "c" };
    const collision: Advisory = { advisoryId: "1".repeat(32), urgency: "high", kind: "concurrent_edit", artifact: "src/x/s.ts", lanes: [a, b], evidence: [], alertAt: at(-1) };
    const broken = {
      advisoryId: "2".repeat(32), urgency: "high", kind: "constraint_violation", artifact: "supabase-migrations", lanes: [a, c], evidence: [], alertAt: at(-2),
      rows: [{ recipient: a, peer: c }, { recipient: c, peer: a }], claimIds: ["c1"],
      get claimTexts(): ConflictAdvisory["claimTexts"] { throw new Error("excerpt failed"); },
    } as ConflictAdvisory;
    const store = new DurableStore([]);
    const sender = new InMemoryRelaySender();
    const l = limits();
    const stats = { written: 0, existing: 0, cooldown: 0, capped: 0, dmSent: 0, dmFailed: 0, rejected: 0 } as ScanStats;
    await deliver({ store, backend: backend(() => []), limits: l, workspaces: "*", sender, conflicts: true }, "org", "ws", [broken, collision], NOW.getTime(), stats);
    expect(stats).toMatchObject({ messageFailed: 1, written: 2, dmSent: 2 });
    expect(store.kinds()).toEqual(["a/concurrent_edit/high", "b/concurrent_edit/high"]);
    // The skipped advisory did not start a cooldown: once its text builds, the same limits let it through.
    const fixed = { ...collision, advisoryId: broken.advisoryId, kind: broken.kind, lanes: broken.lanes, rows: broken.rows, claimIds: ["c1"], claimTexts: [] } as ConflictAdvisory;
    const again = { written: 0, existing: 0, cooldown: 0, capped: 0, dmSent: 0, dmFailed: 0, rejected: 0 } as ScanStats;
    await deliver({ store, backend: backend(() => []), limits: l, workspaces: "*", sender, conflicts: true }, "org", "ws", [fixed], NOW.getTime(), again);
    expect(again).toMatchObject({ written: 2, cooldown: 0 });
    expect(again.messageFailed).toBeUndefined();
  });
});
