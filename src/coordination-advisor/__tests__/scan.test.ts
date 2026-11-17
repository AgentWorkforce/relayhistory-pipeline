/** The scan with a fake store, backend and sender; and the disabled worker doing nothing. */
import { describe, expect, test } from "bun:test";
import type { LaneFacts } from "../../../packages/coordination-detect/src/index.ts";
import type { CoordinationBackend, CoordinationFacts } from "../../session-read-api/coordination-backend.ts";
import type { Directory } from "../../session-read-api/directory.ts";
import { advisorMain, type AdvisorIo } from "../main.ts";
import { InMemoryRelaySender } from "../relay-sender.ts";
import { AdvisorLimits, type CandidateLane, type SessionRef } from "../rubric.ts";
import { advisorTick, scanOnce, type AdvisorStore, type AdvisoryRowInput, type UpsertResult } from "../scan.ts";

const NOW = new Date(Date.UTC(2026, 10, 17, 12));
const at = (min: number) => new Date(NOW.getTime() + min * 60_000).toISOString();
const cand = (sessionId: string, over: Partial<CandidateLane> = {}): CandidateLane => ({
  orgId: "org", workspaceId: "ws", harness: "claude", sessionId, projectId: "proj", repo: "o/r",
  parentSessionIds: [], shortSummary: null, lastActivityAt: at(-5), ...over,
});
const lane = (sessionId: string, writes: Array<[string, number]>): LaneFacts => ({
  sessionId, source: "claude", userId: null, projectId: "proj", repo: "o/r", branch: `b/${sessionId}`, taskRef: null, title: null,
  parentSessionId: null, startedAt: at(-90), lastActionAt: at(-5), commits: [], outcomes: [], usage: [],
  writes: writes.map(([path, min]) => ({ path, ts: at(min), eventId: `${sessionId}-${min}`, kind: "tool", branch: `b/${sessionId}` })),
});

/** Mirrors 062000's upsert: insert new; upgrade low->high on pending/delivered; skip acked/expired or same. */
class FakeStore implements AdvisorStore {
  rows = new Map<string, AdvisoryRowInput & { status: string }>();
  calls: string[] = [];
  constructor(private lanes: CandidateLane[]) {}
  key = (id: string, r: SessionRef) => `${id}|${r.harness}|${r.sessionId}`;
  async candidates() { this.calls.push("candidates"); return this.lanes; }
  directory(): Directory { return {} as Directory; }
  async upsert(_o: string, _w: string, row: AdvisoryRowInput): Promise<UpsertResult> {
    this.calls.push("upsert");
    const k = this.key(row.advisory_id, { harness: row.harness, sessionId: row.session_id });
    const cur = this.rows.get(k);
    if (!cur) { this.rows.set(k, { ...row, status: "pending" }); return { inserted: 1, upgraded: 0, skipped: 0 }; }
    if (cur.urgency === "low" && row.urgency === "high" && (cur.status === "pending" || cur.status === "delivered")) {
      cur.urgency = "high"; return { inserted: 0, upgraded: 1, skipped: 0 };
    }
    return { inserted: 0, upgraded: 0, skipped: 1 };
  }
  async markDelivered(_o: string, _w: string, id: string, r: SessionRef) {
    this.calls.push("mark");
    const cur = this.rows.get(this.key(id, r));
    if (cur?.status === "pending") { cur.status = "delivered"; return true; }
    return false;
  }
  async sweep() { this.calls.push("sweep"); return { expired: 0, deleted: 0 }; }
}
const backend = (lanes: LaneFacts[], over: Partial<CoordinationFacts> = {}): CoordinationBackend => ({
  kind: "clickhouse",
  windowReadiness: async () => { throw new Error("unused"); },
  readFacts: async () => ({ lanes, sessionsTruncated: false, notes: [], ...over }) as CoordinationFacts,
  isSchemaMissing: () => false,
});
const limits = (over: Partial<{ pairCooldownMs: number; sessionHourlyCap: number }> = {}) =>
  new AdvisorLimits({ pairCooldownMs: 6 * 3_600_000, sessionHourlyCap: 3, ...over });
const shared = [lane("a", [["src/x/shared.ts", -40]]), lane("b", [["src/x/shared.ts", -20]])];

describe("scanOnce", () => {
  test("a HIGH pair writes one row per recipient, DMs both, marks delivered only after the send", async () => {
    const store = new FakeStore([cand("a"), cand("b")]);
    const sender = new InMemoryRelaySender();
    const stats = await scanOnce({ store, backend: backend(shared), limits: limits(), sender }, NOW);
    expect(stats).toMatchObject({ findings: 1, written: 2, dmSent: 2, dmFailed: 0 });
    expect(store.calls.filter((c) => c === "mark")).toHaveLength(2);
    expect([...store.rows.values()].map((r) => r.status)).toEqual(["delivered", "delivered"]);
    expect(sender.sent.map((d) => d.to.sessionId).sort()).toEqual(["a", "b"]);
    for (const d of sender.sent) expect(d.text.length).toBeLessThanOrEqual(600);
  });

  test("a failed DM leaves the row pending and does not mark it", async () => {
    const store = new FakeStore([cand("a"), cand("b")]);
    const sender = new InMemoryRelaySender();
    sender.fail = true;
    const stats = await scanOnce({ store, backend: backend(shared), limits: limits(), sender }, NOW);
    expect(stats).toMatchObject({ written: 2, dmSent: 0, dmFailed: 2 });
    expect(store.calls).not.toContain("mark");
    expect([...store.rows.values()].every((r) => r.status === "pending")).toBe(true);
  });

  test("acked is never resent: a skipped upsert sends no DM, even after the cooldown", async () => {
    const store = new FakeStore([cand("a"), cand("b")]);
    const sender = new InMemoryRelaySender();
    await scanOnce({ store, backend: backend(shared), limits: limits({ pairCooldownMs: 0 }), sender }, NOW);
    for (const r of store.rows.values()) r.status = "acked";
    sender.sent.length = 0;
    const stats = await scanOnce({ store, backend: backend(shared), limits: limits({ pairCooldownMs: 0 }), sender }, NOW);
    expect(stats).toMatchObject({ written: 0, existing: 2, dmSent: 0 });
    expect(sender.sent).toEqual([]);
  });

  test("pair cooldown suppresses the next scan; the hourly cap stops a recipient", async () => {
    const store = new FakeStore([cand("a"), cand("b")]);
    const l = limits();
    await scanOnce({ store, backend: backend(shared), limits: l }, NOW);
    expect((await scanOnce({ store, backend: backend(shared), limits: l }, NOW)).cooldown).toBe(1);
    const capped = await scanOnce({ store: new FakeStore([cand("a"), cand("b")]), backend: backend(shared), limits: limits({ sessionHourlyCap: 0 }) }, NOW);
    expect(capped).toMatchObject({ written: 0, capped: 2 });
  });

  test("planned fleet, another project, or a single-session project: no rows", async () => {
    for (const lanes of [
      [cand("a", { parentSessionIds: ["p"] }), cand("b", { parentSessionIds: ["p"] })],
      [cand("a"), cand("b", { projectId: "other" })],
    ]) {
      const store = new FakeStore(lanes);
      const stats = await scanOnce({ store, backend: backend(shared), limits: limits() }, NOW);
      expect(stats.written).toBe(0);
      expect(store.rows.size).toBe(0);
    }
  });

  test("a truncated write stream drops shared-file findings; a backend error fails only that workspace", async () => {
    const store = new FakeStore([cand("a"), cand("b")]);
    const t = await scanOnce({ store, backend: backend(shared, { sessionsTruncated: true, notes: ["write evidence stream cap 1 reached"] }), limits: limits() }, NOW);
    expect(t).toMatchObject({ truncated: 1, written: 0 });
    const bad = backend(shared);
    bad.readFacts = async () => { throw new Error("boom"); };
    expect(await scanOnce({ store, backend: bad, limits: limits() }, NOW)).toMatchObject({ failed: 1, written: 0 });
  });

  test("no DM without a sender: HIGH rows stay pending for the pull path", async () => {
    const store = new FakeStore([cand("a"), cand("b")]);
    const stats = await scanOnce({ store, backend: backend(shared), limits: limits() }, NOW);
    expect(stats).toMatchObject({ written: 2, dmSent: 0 });
    expect(store.calls).not.toContain("mark");
  });

  test("advisorTick throttles to the interval and sweeps hourly", async () => {
    const store = new FakeStore([]);
    const tick = advisorTick(() => ({ store, backend: backend([]), limits: limits() }), { intervalMs: 120_000 });
    const db = { query: async () => { throw new Error("tick must use the store"); } };
    await tick(db, NOW);
    await tick(db, new Date(NOW.getTime() + 60_000));
    await tick(db, new Date(NOW.getTime() + 120_000));
    expect(store.calls).toEqual(["candidates", "sweep", "candidates"]);
  });
});

describe("disabled worker", () => {
  const throwingIo = (idled: { n: number }): AdvisorIo => ({
    runWorker: () => { throw new Error("runWorker must not be called"); },
    idle: async () => { idled.n++; },
    backend: () => { throw new Error("backend must not be built"); },
    sender: () => { throw new Error("sender must not be built"); },
  });
  test("unless exactly \"true\" it starts, logs, idles, and does no reads or writes", async () => {
    for (const v of [undefined, "", "false", "1", "TRUE"]) {
      const idled = { n: 0 };
      // Even a DM flag without its token does not refuse a disabled worker.
      expect(await advisorMain({ COORDINATION_ADVISOR_ENABLED: v, COORDINATION_ADVISOR_RELAY_DM_ENABLED: "true" }, throwingIo(idled))).toBe("disabled");
      expect(idled.n).toBe(1);
    }
  });
  test("enabled: a DM flag without a token refuses before HoopDb opens", async () => {
    const io = throwingIo({ n: 0 });
    let ran = false;
    io.runWorker = async () => { ran = true; };
    io.backend = () => () => backend([]);
    io.sender = () => undefined;
    await expect(advisorMain({ COORDINATION_ADVISOR_ENABLED: "true", COORDINATION_ADVISOR_RELAY_DM_ENABLED: "true" }, io)).rejects.toThrow(/RELAY_TOKEN is missing/);
    expect(ran).toBe(false);
    expect(await advisorMain({ COORDINATION_ADVISOR_ENABLED: "true" }, io)).toBe("ran");
    expect(ran).toBe(true);
  });
});
