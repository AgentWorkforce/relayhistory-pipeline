/** Pure table tests for the advisor rubric, limits, template, config and sender (docs/coordination-live-advisories.md). */
import { describe, expect, test } from "bun:test";
import { detectCoordination, type LaneFacts } from "../../../packages/coordination-detect/src/index.ts";
import { advisorConfig, advisorEnabled } from "../config.ts";
import { httpRelaySender, InMemoryRelaySender } from "../relay-sender.ts";
import {
  ADVISOR_DETECTOR_PARAMS, AdvisorLimits, findingUrgency, highAdvisories, isPlannedFleet, lowAdvisories,
  orderAdvisories, pairKey, refKey, similarSummaries, topLevelDir, truncatedStreams, type Advisory, type CandidateLane,
} from "../rubric.ts";
import { ADVISORY_MESSAGE_MAX, advisoryMessage } from "../template.ts";

const T0 = Date.UTC(2026, 10, 17, 10);
const at = (min: number) => new Date(T0 + min * 60_000).toISOString();
const cand = (sessionId: string, over: Partial<CandidateLane> = {}): CandidateLane => ({
  orgId: "org", workspaceId: "ws", harness: "claude", sessionId, projectId: "proj", repo: "o/r",
  parentSessionIds: [], shortSummary: null, lastActivityAt: at(60), ...over,
});
const lane = (sessionId: string, over: Partial<LaneFacts> = {}): LaneFacts => ({
  sessionId, source: "claude", userId: null, projectId: "proj", repo: "o/r", branch: `b/${sessionId}`, taskRef: null,
  title: null, parentSessionId: null, startedAt: at(0), lastActionAt: at(60), writes: [], commits: [], outcomes: [], usage: [],
  ...over,
});
const w = (sessionId: string, path: string, min: number) => ({ path, ts: at(min), eventId: `${sessionId}-${min}`, kind: "tool", branch: `b/${sessionId}` });
const detect = (lanes: LaneFacts[]) => detectCoordination({ workspaceId: "ws", window: { since: at(-60), until: at(120) }, lanes },
  { params: ADVISOR_DETECTOR_PARAMS, limit: 500 });
const cmap = (...c: CandidateLane[]) => new Map(c.map((x) => [refKey(x), x]));
const none = { writes: false, outputs: false };

describe("rubric: HIGH", () => {
  test("T1 duplicate migration, T2 revert and T3 shared file within 60 minutes are HIGH", () => {
    const lanes = [
      lane("m1", { writes: [w("m1", "db/migrations/0042_a.sql", 0)] }), lane("m2", { writes: [w("m2", "db/migrations/0042_b.sql", 5)] }),
      lane("c1", { commits: [{ sha: "1234567abcdef", ts: at(0), eventId: "c", reverted: null }] }),
      lane("c2", { outcomes: [{ key: "1234567abcdef", ts: at(10), eventId: "o", reverted: true }] }),
      lane("e1", { writes: [w("e1", "src/x/shared.ts", 0)] }), lane("e2", { writes: [w("e2", "src/x/shared.ts", 30)] }),
    ];
    const out = detect(lanes);
    const kinds = out.findings.filter((f) => findingUrgency(f) === "high").map((f) => f.kind).sort();
    expect(kinds).toEqual(["concurrent_edit", "duplicate_work", "superseded_work"]);
    const high = highAdvisories(out, cmap(...lanes.map((l) => cand(l.sessionId))), none);
    expect(high.map((a) => a.kind).sort()).toEqual(kinds);
    for (const a of high) expect(a.advisoryId).toMatch(/^[0-9a-f]{16,}$/);
  });

  test("a shared file more than 60 minutes apart is not HIGH", () => {
    const out = detect([lane("e1", { writes: [w("e1", "src/x/shared.ts", 0)] }), lane("e2", { writes: [w("e2", "src/x/shared.ts", 90)] })]);
    expect(highAdvisories(out, cmap(cand("e1"), cand("e2")), none)).toEqual([]);
  });

  test("planned fleet (shared parent, or parent and child) and different projects are excluded", () => {
    const lanes = [lane("e1", { writes: [w("e1", "src/x/a.ts", 0)] }), lane("e2", { writes: [w("e2", "src/x/a.ts", 10)] })];
    const out = detect(lanes);
    expect(highAdvisories(out, cmap(cand("e1"), cand("e2")), none)).toHaveLength(1);
    expect(highAdvisories(out, cmap(cand("e1", { parentSessionIds: ["p"] }), cand("e2", { parentSessionIds: ["p"] })), none)).toEqual([]);
    expect(highAdvisories(out, cmap(cand("e1", { parentSessionIds: ["e2"] }), cand("e2")), none)).toEqual([]);
    expect(highAdvisories(out, cmap(cand("e1"), cand("e2", { projectId: "other" })), none)).toEqual([]);
    expect(highAdvisories(out, cmap(cand("e1")), none)).toEqual([]);
    expect(isPlannedFleet(cand("a", { harness: "codex", parentSessionIds: ["b"] }), cand("b"))).toBe(false);
  });

  test("demoted (same-root) findings are SILENT", () => {
    const out = detect([
      lane("s1", { parentSessionId: "root", writes: [w("s1", "db/migrations/0099_a.sql", 0)] }),
      lane("s2", { parentSessionId: "root", writes: [w("s2", "db/migrations/0099_b.sql", 5)] }),
    ]);
    expect(out.findings.length).toBeGreaterThan(0);
    expect(out.findings.every((f) => findingUrgency(f) === null)).toBe(true);
  });

  test("findings from a truncated stream are dropped, never guessed", () => {
    expect(truncatedStreams(["write evidence stream cap 5000 reached", "commit/outcome stream cap 5000 reached"]))
      .toEqual({ writes: true, outputs: true });
    expect(truncatedStreams(["usage stream cap 1 reached"])).toEqual(none);
    const out = detect([lane("e1", { writes: [w("e1", "src/x/a.ts", 0)] }), lane("e2", { writes: [w("e2", "src/x/a.ts", 10)] })]);
    expect(highAdvisories(out, cmap(cand("e1"), cand("e2")), { writes: true, outputs: false })).toEqual([]);
    expect(highAdvisories(out, cmap(cand("e1"), cand("e2")), { writes: false, outputs: true })).toHaveLength(1);
  });
});

describe("rubric: LOW pre-filter", () => {
  const nearby = [lane("a", { writes: [w("a", "src/session-read-api/x.ts", 0)] }), lane("b", { writes: [w("b", "src/session-read-api/y.ts", 5)] })];
  test("same top-level directory, no shared file: LOW nearby_work", () => {
    const low = lowAdvisories("ws", nearby, cmap(cand("a"), cand("b")), new Set(), at(-60), none);
    expect(low).toHaveLength(1);
    expect(low[0]).toMatchObject({ urgency: "low", kind: "nearby_work", artifact: "src/session-read-api" });
  });
  test("a shared file, a HIGH pair, a fleet, another project or truncated writes: nothing", () => {
    const shared = [lane("a", { writes: [w("a", "src/x/same.ts", 0)] }), lane("b", { writes: [w("b", "src/x/same.ts", 5)] })];
    expect(lowAdvisories("ws", shared, cmap(cand("a"), cand("b")), new Set(), at(-60), none)).toEqual([]);
    const high = new Set([pairKey({ harness: "claude", sessionId: "a" }, { harness: "claude", sessionId: "b" })]);
    expect(lowAdvisories("ws", nearby, cmap(cand("a"), cand("b")), high, at(-60), none)).toEqual([]);
    expect(lowAdvisories("ws", nearby, cmap(cand("a", { parentSessionIds: ["p"] }), cand("b", { parentSessionIds: ["p"] })), new Set(), at(-60), none)).toEqual([]);
    expect(lowAdvisories("ws", nearby, cmap(cand("a"), cand("b", { projectId: "q" })), new Set(), at(-60), none)).toEqual([]);
    expect(lowAdvisories("ws", nearby, cmap(cand("a"), cand("b")), new Set(), at(-60), { writes: true, outputs: false })).toEqual([]);
  });
  test("similar short summaries without a shared directory: LOW similar_summary", () => {
    const lanes = [lane("a"), lane("b")];
    const c = cmap(cand("a", { shortSummary: "fix coordination advisor cooldown tests" }), cand("b", { shortSummary: "coordination advisor cooldown tests" }));
    expect(lowAdvisories("ws", lanes, c, new Set(), at(-60), none).map((a) => a.kind)).toEqual(["similar_summary"]);
    expect(similarSummaries("alpha beta", "alpha beta")).toBe(false);
    expect(similarSummaries(null, "x")).toBe(false);
  });
  test("topLevelDir", () => {
    expect(topLevelDir("./src/a/b.ts")).toBe("src/a");
    expect(topLevelDir("docs/x.md")).toBe("docs");
    expect(topLevelDir("README.md")).toBeNull();
  });
});

describe("limits", () => {
  const A = { harness: "claude", sessionId: "a" }, B = { harness: "codex", sessionId: "b" };
  test("pair cooldown holds for the configured time and is order-independent", () => {
    const l = new AdvisorLimits({ pairCooldownMs: 6 * 3_600_000, sessionHourlyCap: 3 });
    expect(pairKey(A, B)).toBe(pairKey(B, A));
    l.touchPair(pairKey(A, B), "high", T0);
    expect(l.inCooldown(pairKey(B, A), "high", T0 + 6 * 3_600_000 - 1)).toBe(true);
    expect(l.inCooldown(pairKey(A, B), "high", T0 + 6 * 3_600_000)).toBe(false);
  });
  test("cooldown per urgency: a LOW never holds back a HIGH; a HIGH holds back both", () => {
    const l = new AdvisorLimits({ pairCooldownMs: 6 * 3_600_000, sessionHourlyCap: 3 });
    l.touchPair(pairKey(A, B), "low", T0);
    expect(l.inCooldown(pairKey(A, B), "low", T0 + 1)).toBe(true);
    expect(l.inCooldown(pairKey(A, B), "high", T0 + 1)).toBe(false);
    l.touchPair(pairKey(A, B), "high", T0 + 2);
    expect(l.inCooldown(pairKey(A, B), "high", T0 + 3)).toBe(true);
    expect(l.inCooldown(pairKey(A, B), "low", T0 + 6 * 3_600_000 + 1)).toBe(true);
    expect(l.inCooldown(pairKey(A, B), "low", T0 + 6 * 3_600_000 + 2)).toBe(false);
  });
  test("seed: durable pair times start the cooldown; durable last-hour counts fill the cap, never lower it", () => {
    const l = new AdvisorLimits({ pairCooldownMs: 6 * 3_600_000, sessionHourlyCap: 3 });
    l.recordSent(A, T0);
    l.seed([
      { kind: "pair", recipient: A, peer: B, urgency: "high", latestAt: T0 - 3_600_000 },
      { kind: "pair", recipient: A, peer: { harness: "codex", sessionId: "c" }, urgency: "low", latestAt: T0 - 60_000 },
      { kind: "recipient", recipient: A, latestAt: T0 - 60_000, sentLastHour: 3 },
      { kind: "recipient", recipient: B, latestAt: T0 - 60_000, sentLastHour: 1 },
    ], T0);
    expect(l.inCooldown(pairKey(B, A), "low", T0 + 5 * 3_600_000 - 1)).toBe(true);
    expect(l.inCooldown(pairKey(A, B), "high", T0 + 5 * 3_600_000)).toBe(false);
    // A durable LOW seeds only the LOW cooldown.
    expect(l.inCooldown(pairKey(A, { harness: "codex", sessionId: "c" }), "low", T0)).toBe(true);
    expect(l.inCooldown(pairKey(A, { harness: "codex", sessionId: "c" }), "high", T0)).toBe(false);
    expect(l.underCap(A, T0)).toBe(false);
    expect(l.underCap(B, T0)).toBe(true);
    // A later in-memory pair time wins; a smaller durable count does not drop in-memory sends.
    l.touchPair(pairKey(A, B), "high", T0);
    l.seed([{ kind: "pair", recipient: A, peer: B, urgency: "high", latestAt: T0 - 7_200_000 }, { kind: "recipient", recipient: A, latestAt: T0, sentLastHour: 1 }], T0);
    expect(l.inCooldown(pairKey(A, B), "high", T0 + 6 * 3_600_000 - 1)).toBe(true);
    expect(l.underCap(A, T0)).toBe(false);
  });
  test("per-recipient hourly cap, rolling", () => {
    const l = new AdvisorLimits({ pairCooldownMs: 0, sessionHourlyCap: 3 });
    for (let i = 0; i < 3; i++) l.recordSent(A, T0 + i);
    expect(l.underCap(A, T0 + 10)).toBe(false);
    expect(l.underCap(B, T0 + 10)).toBe(true);
    expect(l.underCap(A, T0 + 3_600_001)).toBe(true);
    l.prune(T0 + 3_600_003);
    expect(l.underCap(A, T0 + 3_600_003)).toBe(true);
  });
  test("ordering: HIGH first, then alertAt, then id", () => {
    const mk = (id: string, urgency: "high" | "low", alertAt: string): Advisory =>
      ({ advisoryId: id, urgency, kind: "k", artifact: "x", lanes: [A, B], evidence: [], alertAt });
    expect(orderAdvisories([mk("3", "low", at(0)), mk("2", "high", at(5)), mk("1", "high", at(5)), mk("0", "high", at(9))])
      .map((a) => a.advisoryId)).toEqual(["1", "2", "0", "3"]);
  });
});

describe("DM template", () => {
  const base = { advisoryId: "abcdef0123456789", urgency: "high" as const, kind: "concurrent_edit", artifact: "src/x/a.ts",
    harness: "claude", sessionId: "s-a", peerHarness: "codex", peerSessionId: "s-b", firstSeen: at(0) };
  test("HIGH asks, LOW does not; four lines; ids and artifact present", () => {
    const high = advisoryMessage(base);
    expect(high.split("\n")).toHaveLength(4);
    expect(high).toContain("claude:s-a");
    expect(high).toContain("codex:s-b");
    expect(high).toContain("src/x/a.ts");
    expect(high).toContain("Please check with the other agent");
    const low = advisoryMessage({ ...base, urgency: "low", kind: "nearby_work", artifact: "src/x" });
    expect(low).toContain("may be working near");
    expect(low).not.toContain("Please check");
  });
  test("bounded at 600 characters and no injected lines, for adversarial inputs", () => {
    const long = "x".repeat(5_000);
    for (const over of [{ artifact: long }, { sessionId: long, peerSessionId: long }, { artifact: `a\nFAKE LINE\r\n${long}` }, { kind: long, advisoryId: long }]) {
      const text = advisoryMessage({ ...base, ...over });
      expect(text.length).toBeLessThanOrEqual(ADVISORY_MESSAGE_MAX);
      expect(text.split("\n")).toHaveLength(4);
      expect(text).not.toContain("\r");
    }
  });
});

describe("config and sender", () => {
  test("enabled only by the exact string true", () => {
    for (const v of [undefined, "", "1", "TRUE", "yes", " true"]) expect(advisorEnabled({ COORDINATION_ADVISOR_ENABLED: v })).toBe(false);
    expect(advisorEnabled({ COORDINATION_ADVISOR_ENABLED: "true" })).toBe(true);
  });
  test("defaults", () => {
    expect(advisorConfig({})).toEqual({ intervalMs: 120_000, pairCooldownMs: 21_600_000, sessionHourlyCap: 3, factsBackend: "clickhouse", relayDm: { enabled: false }, workspaces: new Set() });
  });
  test("workspaces: the routes' canary allowlist; absent or empty scans none", () => {
    expect(advisorConfig({ READ_API_DIRECTORY_WORKSPACES: "*" }).workspaces).toBe("*");
    expect([...(advisorConfig({ READ_API_DIRECTORY_WORKSPACES: " w1, w2 ,," }).workspaces as Set<string>)].sort()).toEqual(["w1", "w2"]);
    expect((advisorConfig({ READ_API_DIRECTORY_WORKSPACES: "" }).workspaces as Set<string>).size).toBe(0);
  });
  test("relay URL validated at config time, value never echoed", () => {
    const dm = (url: string) => () => advisorConfig({ COORDINATION_ADVISOR_RELAY_DM_ENABLED: "true", COORDINATION_ADVISOR_RELAY_TOKEN: "t", COORDINATION_ADVISOR_RELAY_URL: url });
    for (const [url, msg] of [["not a url secretpart", /not a valid URL/], ["http://relay.example/secretpart", /must be https/], ["https://u:secretpart@relay.example/", /credentials/], ["https://relay.example/v1?token=secretpart", /query or fragment/], ["https://relay.example/#secretpart", /query or fragment/]] as const) {
      expect(dm(url)).toThrow(msg);
      try { dm(url)(); } catch (e) { expect(String((e as Error).message)).not.toContain("secretpart"); }
    }
    expect(advisorConfig({ COORDINATION_ADVISOR_RELAY_DM_ENABLED: "true", COORDINATION_ADVISOR_RELAY_TOKEN: "t", COORDINATION_ADVISOR_RELAY_URL: "http://127.0.0.1:9/dm" }).relayDm.enabled).toBe(true);
  });
  test("refuses to start: DM on without token or URL; malformed numbers; unknown backend", () => {
    expect(() => advisorConfig({ COORDINATION_ADVISOR_RELAY_DM_ENABLED: "true", COORDINATION_ADVISOR_RELAY_URL: "https://r.example" }))
      .toThrow(/COORDINATION_ADVISOR_RELAY_TOKEN is missing/);
    expect(() => advisorConfig({ COORDINATION_ADVISOR_RELAY_DM_ENABLED: "true", COORDINATION_ADVISOR_RELAY_TOKEN: "t" }))
      .toThrow(/COORDINATION_ADVISOR_RELAY_URL is missing/);
    expect(() => advisorConfig({ COORDINATION_ADVISOR_INTERVAL_SECONDS: "0" })).toThrow(/INTERVAL_SECONDS/);
    expect(() => advisorConfig({ COORDINATION_ADVISOR_SESSION_HOURLY_CAP: "3.5" })).toThrow(/HOURLY_CAP/);
    expect(() => advisorConfig({ COORDINATION_ADVISOR_FACTS_BACKEND: "mysql" })).toThrow(/FACTS_BACKEND/);
    // DM flag not exactly "true": no token needed.
    expect(advisorConfig({ COORDINATION_ADVISOR_RELAY_DM_ENABLED: "1" }).relayDm).toEqual({ enabled: false });
  });
  test("http sender: https only off loopback; bearer; tenant in the payload; no redirects; body discarded; throws on non-ok and over-bound text; token never in errors", async () => {
    expect(() => httpRelaySender({ url: "http://relay.example/dm", token: "secret-token" })).toThrow(/https/);
    const calls: { url: string; init: RequestInit }[] = [];
    const cancelled: number[] = [];
    let status = 200;
    const fake = (async (url: URL, init: RequestInit) => {
      calls.push({ url: String(url), init });
      const body = new ReadableStream({ cancel() { cancelled.push(status); } });
      return new Response(body, { status });
    }) as unknown as typeof fetch;
    const s = httpRelaySender({ url: "http://127.0.0.1:9/dm", token: "secret-token", fetch: fake });
    const dm = (text: string) => ({ orgId: "org-1", workspaceId: "ws-1", to: { harness: "claude", sessionId: "a" }, text, advisoryId: "id" });
    await s.send(dm("hi"));
    expect((calls[0]!.init.headers as Record<string, string>).authorization).toBe("Bearer secret-token");
    expect(calls[0]!.init.redirect).toBe("error");
    expect(JSON.parse(String(calls[0]!.init.body))).toEqual({
      from: "relayhistory-coordination", orgId: "org-1", workspaceId: "ws-1", to: { harness: "claude", sessionId: "a" }, text: "hi", advisoryId: "id",
    });
    status = 503;
    const err = await s.send(dm("hi")).catch((e: Error) => e);
    expect(String(err)).toContain("503");
    expect(String(err)).not.toContain("secret-token");
    expect(cancelled).toEqual([200, 503]);
    await expect(s.send(dm("x".repeat(601)))).rejects.toThrow(/bound/);
    // A redirect answer (what fetch's redirect "error" raises) is a failed send, never followed.
    const redirecting = httpRelaySender({ url: "https://relay.example/dm", token: "secret-token",
      fetch: (async () => { throw new TypeError("unexpected redirect"); }) as unknown as typeof fetch });
    await expect(redirecting.send(dm("hi"))).rejects.toThrow(/redirect/);
    const mem = new InMemoryRelaySender();
    mem.fail = true;
    await expect(mem.send({ orgId: "o", workspaceId: "w", to: { harness: "c", sessionId: "s" }, text: "t", advisoryId: "i" })).rejects.toThrow();
  });
});
