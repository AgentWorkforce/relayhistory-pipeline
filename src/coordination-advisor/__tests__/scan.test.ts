/** The scan with a fake store, backend and sender; and the disabled worker doing nothing. */
import { describe, expect, test } from "bun:test";
import type { LaneFacts } from "../../../packages/coordination-detect/src/index.ts";
import type { CoordinationBackend, CoordinationFacts } from "../../session-read-api/coordination-backend.ts";
import type { Directory } from "../../session-read-api/directory.ts";
import type { WorkerOptions } from "../../shared/worker.ts";
import { SESSION_INTEL_FEATURES, type SessionIntelControls } from "../../shared/session-intel-controls.ts";
import { advisorMain, type AdvisorIo } from "../main.ts";
import { InMemoryRelaySender } from "../relay-sender.ts";
import { AdvisorLimits, type CandidateLane, type RecentAdvisory, type SessionRef } from "../rubric.ts";
import {
  ARTIFACT_MAX_CHARS, advisorTick, boundedArtifact, boundedEvidence, EVIDENCE_TEXT_MAX, scanOnce,
  type AdvisorStore, type AdvisoryRowInput, type UpsertResult,
} from "../scan.ts";

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
  featureControls: SessionIntelControls = new Map(SESSION_INTEL_FEATURES.map((f) => [f, true]));
  async controls() { return this.featureControls; }
  rows = new Map<string, AdvisoryRowInput & { status: string }>();
  calls: string[] = [];
  constructor(private lanes: CandidateLane[]) {}
  key = (id: string, r: SessionRef) => `${id}|${r.harness}|${r.sessionId}`;
  candidateArgs: Array<string[] | null> = [];
  upsertError: ((n: number) => unknown) | undefined;
  async candidates(_since: Date, _limit: number, workspaces: string[] | null) {
    this.calls.push("candidates");
    this.candidateArgs.push(workspaces);
    return this.lanes;
  }
  directory(): Directory { return {} as Directory; }
  async upsert(_o: string, _w: string, row: AdvisoryRowInput): Promise<UpsertResult> {
    this.calls.push("upsert");
    const err = this.upsertError?.(this.calls.filter((c) => c === "upsert").length);
    if (err) throw err;
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
  recentRows: RecentAdvisory[] = [];
  recentError: unknown = undefined;
  async recent(): Promise<RecentAdvisory[]> {
    this.calls.push("recent");
    if (this.recentError) throw this.recentError;
    return this.recentRows;
  }
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
  test("controls skip disabled workspaces and gate DMs independently on each scan", async () => {
    const store = new FakeStore([cand("a"), cand("b")]);
    const be = backend(shared);
    const sender = new InMemoryRelaySender();
    store.featureControls = new Map();
    expect(await scanOnce({ workspaces: "*", store, backend: be, limits: limits(), sender }, NOW))
      .toMatchObject({ workspaces: 0, findings: 0, written: 0, dmSent: 0 });
    expect(store.rows.size).toBe(0);
    store.featureControls = new Map([["advisor", true], ["dm", false]]);
    expect(await scanOnce({ workspaces: "*", store, backend: be, limits: limits(), sender }, NOW))
      .toMatchObject({ findings: 1, written: 2, dmSent: 0 });
    expect(sender.sent).toEqual([]);
    expect([...store.rows.values()].every((r) => r.status === "pending")).toBe(true);
    // A DM control never creates a sender when the environment flag is off.
    const noSender = new FakeStore([cand("a"), cand("b")]);
    expect(await scanOnce({ workspaces: "*", store: noSender, backend: backend(shared), limits: limits() }, NOW))
      .toMatchObject({ written: 2, dmSent: 0 });
  });
  test("a HIGH pair writes one row per recipient, DMs both, marks delivered only after the send", async () => {
    const store = new FakeStore([cand("a"), cand("b")]);
    const sender = new InMemoryRelaySender();
    const stats = await scanOnce({ workspaces: "*", store, backend: backend(shared), limits: limits(), sender }, NOW);
    expect(stats).toMatchObject({ findings: 1, written: 2, dmSent: 2, dmFailed: 0 });
    expect(store.calls.filter((c) => c === "mark")).toHaveLength(2);
    expect([...store.rows.values()].map((r) => r.status)).toEqual(["delivered", "delivered"]);
    expect(sender.sent.map((d) => d.to.sessionId).sort()).toEqual(["a", "b"]);
    for (const d of sender.sent) expect([d.orgId, d.workspaceId]).toEqual(["org", "ws"]);
    for (const d of sender.sent) expect(d.text.length).toBeLessThanOrEqual(600);
  });

  test("a failed DM leaves the row pending and does not mark it", async () => {
    const store = new FakeStore([cand("a"), cand("b")]);
    const sender = new InMemoryRelaySender();
    sender.fail = true;
    const stats = await scanOnce({ workspaces: "*", store, backend: backend(shared), limits: limits(), sender }, NOW);
    expect(stats).toMatchObject({ written: 2, dmSent: 0, dmFailed: 2 });
    expect(store.calls).not.toContain("mark");
    expect([...store.rows.values()].every((r) => r.status === "pending")).toBe(true);
  });

  test("acked is never resent: a skipped upsert sends no DM, even after the cooldown", async () => {
    const store = new FakeStore([cand("a"), cand("b")]);
    const sender = new InMemoryRelaySender();
    await scanOnce({ workspaces: "*", store, backend: backend(shared), limits: limits({ pairCooldownMs: 0 }), sender }, NOW);
    for (const r of store.rows.values()) r.status = "acked";
    sender.sent.length = 0;
    const stats = await scanOnce({ workspaces: "*", store, backend: backend(shared), limits: limits({ pairCooldownMs: 0 }), sender }, NOW);
    expect(stats).toMatchObject({ written: 0, existing: 2, dmSent: 0 });
    expect(sender.sent).toEqual([]);
  });

  test("pair cooldown suppresses the next scan; the hourly cap stops a recipient", async () => {
    const store = new FakeStore([cand("a"), cand("b")]);
    const l = limits();
    await scanOnce({ workspaces: "*", store, backend: backend(shared), limits: l }, NOW);
    expect((await scanOnce({ workspaces: "*", store, backend: backend(shared), limits: l }, NOW)).cooldown).toBe(1);
    const capped = await scanOnce({ workspaces: "*", store: new FakeStore([cand("a"), cand("b")]), backend: backend(shared), limits: limits({ sessionHourlyCap: 0 }) }, NOW);
    expect(capped).toMatchObject({ written: 0, capped: 2 });
  });

  test("durable state seeds the limits before writing: a restart keeps the cooldown and the cap", async () => {
    const A = { harness: "claude", sessionId: "a" }, B = { harness: "claude", sessionId: "b" };
    const cooled = new FakeStore([cand("a"), cand("b")]);
    cooled.recentRows = [{ kind: "pair", recipient: A, peer: B, urgency: "high", latestAt: NOW.getTime() - 60_000 }];
    expect(await scanOnce({ workspaces: "*", store: cooled, backend: backend(shared), limits: limits() }, NOW)).toMatchObject({ cooldown: 1, written: 0 });
    expect(cooled.calls).toEqual(["candidates", "recent"]);
    const full = new FakeStore([cand("a"), cand("b")]);
    full.recentRows = [{ kind: "recipient", recipient: A, latestAt: NOW.getTime() - 60_000, sentLastHour: 3 }];
    expect(await scanOnce({ workspaces: "*", store: full, backend: backend(shared), limits: limits() }, NOW)).toMatchObject({ capped: 1, written: 1 });
    // No findings: no durable read.
    const quiet = new FakeStore([cand("a"), cand("b")]);
    await scanOnce({ workspaces: "*", store: quiet, backend: backend([]), limits: limits() }, NOW);
    expect(quiet.calls).toEqual(["candidates"]);
  });

  test("durable state: 062070 not applied (42883) keeps in-memory limits; any other error fails the workspace unwritten", async () => {
    const missing = new FakeStore([cand("a"), cand("b")]);
    missing.recentError = Object.assign(new Error("function does not exist"), { code: "42883" });
    expect(await scanOnce({ workspaces: "*", store: missing, backend: backend(shared), limits: limits() }, NOW)).toMatchObject({ written: 2, failed: 0 });
    const broken = new FakeStore([cand("a"), cand("b")]);
    broken.recentError = Object.assign(new Error("permission denied"), { code: "42501" });
    expect(await scanOnce({ workspaces: "*", store: broken, backend: backend(shared), limits: limits() }, NOW)).toMatchObject({ written: 0, failed: 1 });
    expect(broken.calls).not.toContain("upsert");
  });

  test("planned fleet, another project, or a single-session project: no rows", async () => {
    for (const lanes of [
      [cand("a", { parentSessionIds: ["p"] }), cand("b", { parentSessionIds: ["p"] })],
      [cand("a"), cand("b", { projectId: "other" })],
    ]) {
      const store = new FakeStore(lanes);
      const stats = await scanOnce({ workspaces: "*", store, backend: backend(shared), limits: limits() }, NOW);
      expect(stats.written).toBe(0);
      expect(store.rows.size).toBe(0);
    }
  });

  test("a truncated write stream drops shared-file findings; a backend error fails only that workspace", async () => {
    const store = new FakeStore([cand("a"), cand("b")]);
    const t = await scanOnce({ workspaces: "*", store, backend: backend(shared, { sessionsTruncated: true, notes: ["write evidence stream cap 1 reached"] }), limits: limits() }, NOW);
    expect(t).toMatchObject({ truncated: 1, written: 0 });
    const bad = backend(shared);
    bad.readFacts = async () => { throw new Error("boom"); };
    expect(await scanOnce({ workspaces: "*", store, backend: bad, limits: limits() }, NOW)).toMatchObject({ failed: 1, written: 0 });
  });

  test("no DM without a sender: HIGH rows stay pending for the pull path", async () => {
    const store = new FakeStore([cand("a"), cand("b")]);
    const stats = await scanOnce({ workspaces: "*", store, backend: backend(shared), limits: limits() }, NOW);
    expect(stats).toMatchObject({ written: 2, dmSent: 0 });
    expect(store.calls).not.toContain("mark");
  });

  test("a durable LOW cooldown never holds back a HIGH for the same pair", async () => {
    const A = { harness: "claude", sessionId: "a" }, B = { harness: "claude", sessionId: "b" };
    const store = new FakeStore([cand("a"), cand("b")]);
    store.recentRows = [{ kind: "pair", recipient: A, peer: B, urgency: "low", latestAt: NOW.getTime() - 60_000 }];
    expect(await scanOnce({ workspaces: "*", store, backend: backend(shared), limits: limits() }, NOW)).toMatchObject({ cooldown: 0, written: 2 });
  });

  test("workspace allowlist: empty reads nothing; a list is passed to the query and enforced on its rows", async () => {
    const none = new FakeStore([cand("a"), cand("b")]);
    expect(await scanOnce({ workspaces: new Set(), store: none, backend: backend(shared), limits: limits() }, NOW)).toMatchObject({ workspaces: 0, written: 0 });
    expect(none.calls).toEqual([]);
    const listed = new FakeStore([cand("a"), cand("b"), cand("c", { workspaceId: "other" }), cand("d", { workspaceId: "other" })]);
    const stats = await scanOnce({ workspaces: new Set(["ws2", "ws"]), store: listed, backend: backend(shared), limits: limits() }, NOW);
    expect(listed.candidateArgs).toEqual([["ws", "ws2"]]);
    expect(stats).toMatchObject({ workspaces: 1, written: 2 });
    const all = new FakeStore([]);
    await scanOnce({ workspaces: "*", store: all, backend: backend([]), limits: limits() }, NOW);
    expect(all.candidateArgs).toEqual([null]);
  });

  test("bounds: artifact clipped by code point to 512; evidence trimmed until its jsonb text fits 4096", () => {
    expect(boundedArtifact("x".repeat(600))).toHaveLength(ARTIFACT_MAX_CHARS);
    const emoji = boundedArtifact("\u{1F600}".repeat(600));
    expect(Array.from(emoji)).toHaveLength(ARTIFACT_MAX_CHARS);
    expect(emoji.endsWith("\u{1F600}")).toBe(true);
    expect(boundedArtifact("short")).toBe("short");
    const big = Array.from({ length: 10 }, (_, i) => ({ sessionId: "s".repeat(200), eventId: `e${i}`.repeat(100), ts: at(i), kind: "tool" }));
    const kept = boundedEvidence(big);
    expect(kept.length).toBeGreaterThan(0);
    expect(kept.length).toBeLessThan(10);
    expect(kept).toEqual(big.slice(0, kept.length));
    // jsonb's text form: ": " after each key and ", " between items and keys.
    const jsonbText = JSON.stringify(kept).replace(/","/g, '", "').replace(/":/g, '": ').replace(/\},\{/g, "}, {");
    expect(jsonbText.length).toBeLessThanOrEqual(EVIDENCE_TEXT_MAX);
    const small = big.slice(0, 2).map((e) => ({ ...e, sessionId: "s", eventId: "e" }));
    expect(boundedEvidence(small)).toEqual(small);
  });

  test("an oversized artifact is written clipped; a row the database refuses is skipped and the rest go on", async () => {
    const long = `src/${"d".repeat(700)}.ts`;
    const store = new FakeStore([cand("a"), cand("b")]);
    const lanes = [lane("a", [[long, -40]]), lane("b", [[long, -20]])];
    expect(await scanOnce({ workspaces: "*", store, backend: backend(lanes), limits: limits() }, NOW)).toMatchObject({ written: 2, rejected: 0 });
    for (const r of store.rows.values()) expect(Array.from(r.artifact).length).toBeLessThanOrEqual(ARTIFACT_MAX_CHARS);
    const refusing = new FakeStore([cand("a"), cand("b")]);
    refusing.upsertError = (n) => (n === 1 ? Object.assign(new Error("check violation"), { code: "23514" }) : undefined);
    const sender = new InMemoryRelaySender();
    expect(await scanOnce({ workspaces: "*", store: refusing, backend: backend(shared), limits: limits(), sender }, NOW))
      .toMatchObject({ rejected: 1, written: 1, dmSent: 1, failed: 0 });
    // Not a row error (connection, permission): the workspace fails as before.
    const down = new FakeStore([cand("a"), cand("b")]);
    down.upsertError = () => Object.assign(new Error("connection reset"), { code: "08006" });
    expect(await scanOnce({ workspaces: "*", store: down, backend: backend(shared), limits: limits() }, NOW)).toMatchObject({ failed: 1, rejected: 0, written: 0 });
    // A class 22 row error (over-long value, untranslatable text) skips the row like a check violation.
    for (const code of ["22001", "22P05"]) {
      const clipped = new FakeStore([cand("a"), cand("b")]);
      clipped.upsertError = (n) => (n === 1 ? Object.assign(new Error("value too long"), { code }) : undefined);
      expect(await scanOnce({ workspaces: "*", store: clipped, backend: backend(shared), limits: limits() }, NOW))
        .toMatchObject({ rejected: 1, written: 1, failed: 0 });
    }
    // The upsert's own 22023 raises (invalid tenant, malformed evidence) mean the worker is wrong: the workspace fails.
    const malformed = new FakeStore([cand("a"), cand("b")]);
    malformed.upsertError = () => Object.assign(new Error("invalid tenant"), { code: "22023" });
    expect(await scanOnce({ workspaces: "*", store: malformed, backend: backend(shared), limits: limits() }, NOW)).toMatchObject({ failed: 1, rejected: 0, written: 0 });
  });

  test("advisorTick throttles to the interval and sweeps hourly", async () => {
    const store = new FakeStore([]);
    const tick = advisorTick(() => ({ workspaces: "*", store, backend: backend([]), limits: limits() }), { intervalMs: 120_000 });
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
  test("enabled: READ_API_DIRECTORY_WORKSPACES reaches the scan's candidates query", async () => {
    for (const [raw, want] of [["ws-b, ws-a", ["ws-a", "ws-b"]], ["*", null]] as const) {
      const io = throwingIo({ n: 0 });
      let tick: WorkerOptions["tick"];
      io.runWorker = async (opts) => { tick = opts.tick; };
      io.backend = () => () => backend([]);
      io.sender = () => undefined;
      expect(await advisorMain({ COORDINATION_ADVISOR_ENABLED: "true", READ_API_DIRECTORY_WORKSPACES: raw }, io)).toBe("ran");
      // The first query is the candidates read ($3 = the sorted allowlist, null = every workspace); the sweep follows.
      const seen: unknown[][] = [];
      const db = { query: async (_sql: string, params?: unknown[]) => { seen.push(params ?? []); return { rows: [] }; } };
      await tick!(db as unknown as Parameters<NonNullable<WorkerOptions["tick"]>>[0], NOW);
      expect(seen[0]?.[2]).toEqual(want);
    }
    // Unset: the worker runs and reads no candidates; only the retention sweep runs.
    const io = throwingIo({ n: 0 });
    let tick: WorkerOptions["tick"];
    io.runWorker = async (opts) => { tick = opts.tick; };
    io.backend = () => () => backend([]);
    io.sender = () => undefined;
    await advisorMain({ COORDINATION_ADVISOR_ENABLED: "true" }, io);
    const sqls: string[] = [];
    await tick!({ query: async (sql: string) => { sqls.push(sql); return { rows: [] }; } } as unknown as Parameters<NonNullable<WorkerOptions["tick"]>>[0], NOW);
    expect(sqls).toHaveLength(1);
    expect(sqls[0]).toContain("coordination_advisories_sweep");
  });
});
