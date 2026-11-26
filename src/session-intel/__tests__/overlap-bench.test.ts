import { describe, expect, test } from "bun:test";
import { existsSync } from "node:fs";
import type { EventFacts } from "../cloud-harvest/features.ts";
import type { DirectoryRow, SessionFacts } from "../cloud-harvest/harvest.ts";
import { type Context, concurrentEdits, type ForgePr, openPrOverlap, sharedTrigger } from "../overlap-bench/detectors.ts";
import { buildModel, communicated } from "../overlap-bench/model.ts";
import { repoOf, repoRelative } from "../overlap-bench/paths.ts";
import { score } from "../overlap-bench/score.ts";
import { mergeHarvests } from "../overlap-bench/cli.ts";

const REPO = "AgentWorkforce/relayhistory";
const row = (id: string, owner: string, extra: Partial<DirectoryRow> = {}): DirectoryRow =>
  ({ sessionId: id, source: "codex", lastTs: "2026-09-02T00:00:00.000Z", createdAt: "2026-09-01T00:00:00.000Z", project: { name: REPO }, userIds: [owner], parentSessionIds: [], ...extra });
const fact = (ts: string, extra: Partial<EventFacts> = {}): EventFacts => ({
  eventId: ts, ts, kind: null, type: null, role: null, tool: null, toolStatus: null, subagentId: null, userId: null,
  files: [], touched: [], git: [], gh: [], branches: [], prRefs: [], shas: [], relay: null, ...extra,
});
const facts = (id: string, events: EventFacts[]): SessionFacts => ({ source: "codex", sessionId: id, eventsRead: events.length, censored: false, error: null, events });
const pr = (number: number, author: string, extra: Partial<ForgePr> = {}): ForgePr => ({
  repo: REPO, number, author, title: `t${number}`, headRef: `b${number}`, createdAt: "2026-09-01T16:00:00.000Z", mergedAt: null, closedAt: null,
  files: ["crates/core/src/lib.rs"], ...extra,
});
const ctxOf = (rows: DirectoryRow[], f: SessionFacts[], prs: ForgePr[]): Context => {
  const sessions = buildModel(rows, f);
  return { sessions, prs, communicated: communicated(sessions) };
};

describe("paths", () => {
  test("resolves repos and repo-relative paths across checkouts and worktrees", () => {
    expect(repoOf("~/Projects/AgentWorkforce/cloud")).toBe("AgentWorkforce/cloud");
    expect(repoOf("https://github.com/AgentWorkforce/Relay.git")).toBe("AgentWorkforce/relay");
    expect(repoRelative("/Volumes/Ext/Projects/AgentWorkforce/worktrees/cloud/fix-x/packages/web/a.ts", "AgentWorkforce/cloud")).toBe("packages/web/a.ts");
    expect(repoRelative("~/Projects/AgentWorkforce/cloud/.claude/worktrees/plan/packages/web/a.ts", "AgentWorkforce/cloud")).toBe("packages/web/a.ts");
    expect(repoRelative("packages/web/a.ts", "AgentWorkforce/cloud")).toBe("packages/web/a.ts");
    expect(repoRelative("/private/tmp/claude-501/x/scratchpad/a.ts", "AgentWorkforce/cloud")).toBeNull();
    expect(repoRelative("/Users/me/Projects/AgentWorkforce/relay/src/a.rs", "AgentWorkforce/cloud")).toBeNull();
  });
});

describe("open-pr overlap (D1)", () => {
  test("a session editing a file of another author's open PR alerts at that edit, before the harm", () => {
    const ctx = ctxOf([row("bob", "W")], [facts("bob", [fact("2026-09-01T21:00:00.000Z", { touched: [{ path: "crates/core/src/lib.rs", via: "status" }] })])], [pr(93, "machine-c")]);
    const alerts = openPrOverlap(ctx, { minShared: 1, maxPrFiles: 100, skipHot: true, suppressCommunicated: true });
    expect(alerts).toHaveLength(1);
    expect(alerts[0]!.sides).toEqual(["root:codex:bob", "pr:agentworkforce/relayhistory#93"]);
    const s = score("open-pr", ctx, alerts, [{ id: "i1", repo: REPO, kind: "intent_conflict", lines: ["pr:93", "commit:d00dfee"], harmAt: "2026-09-09T14:43:00.000Z", split: "dev" }], [], { sessions: 100, perHundred: 5 });
    // The commit line links through the sha the session printed; this session printed none, so it is unlinkable.
    expect(s.linkable).toBe(0);
    const ctx2 = ctxOf([row("bob", "W")], [facts("bob", [fact("2026-09-01T21:00:00.000Z", { touched: [{ path: "crates/core/src/lib.rs", via: "status" }] }), fact("2026-09-01T21:18:00.000Z", { shas: ["d00dfee"] })])], [pr(93, "machine-c")]);
    const s2 = score("open-pr", ctx2, openPrOverlap(ctx2, { minShared: 1, maxPrFiles: 100, skipHot: true, suppressCommunicated: true }), [{ id: "i1", repo: REPO, kind: "intent_conflict", lines: ["pr:93", "commit:d00dfee"], harmAt: "2026-09-09T14:43:00.000Z", split: "dev" }], [], { sessions: 100, perHundred: 5 });
    expect(s2.hits.map((h) => h.id)).toEqual(["i1"]);
    expect(Math.round(s2.hits[0]!.leadH)).toBe(186);
  });
  test("a PR the session itself created, or one already merged, never alerts", () => {
    const own = ctxOf([row("k", "K")], [facts("k", [fact("2026-09-01T17:00:00.000Z", { gh: ["pr-create"], prRefs: ["agentworkforce/relayhistory#93"] }), fact("2026-09-01T18:00:00.000Z", { touched: [{ path: "crates/core/src/lib.rs", via: "add" }] })])], [pr(93, "k")]);
    expect(openPrOverlap(own, { minShared: 1, maxPrFiles: 100, skipHot: true, suppressCommunicated: true })).toEqual([]);
    const merged = ctxOf([row("w", "W")], [facts("w", [fact("2026-09-03T00:00:00.000Z", { touched: [{ path: "crates/core/src/lib.rs", via: "add" }] })])], [pr(93, "x", { mergedAt: "2026-09-02T00:00:00.000Z" })]);
    expect(openPrOverlap(merged, { minShared: 1, maxPrFiles: 100, skipHot: true, suppressCommunicated: true })).toEqual([]);
  });
});

describe("concurrent edits (D3) and relay suppression", () => {
  const rows = [row("a", "K"), row("b", "W")];
  const edits = (id: string, ts: string) => facts(id, [fact(ts, { touched: [{ path: "package.json", via: "status" }, { path: "packages/web/package.json", via: "status" }] })]);
  test("two owners editing the same files within the window alert at the second edit", () => {
    const ctx = ctxOf(rows, [edits("a", "2026-10-01T17:00:00.000Z"), edits("b", "2026-10-01T18:30:00.000Z")], []);
    const alerts = concurrentEdits(ctx, { windowH: 6, minShared: 2, maxRootsPerPath: 8, skipHot: true, crossOwnerOnly: true, suppressCommunicated: true });
    expect(alerts.map((a) => a.at)).toEqual(["2026-10-01T18:30:00.000Z"]);
  });
  test("a relay message between the two sessions before the edit suppresses the alert", () => {
    const ctx = ctxOf(rows, [
      facts("a", [fact("2026-10-01T16:00:00.000Z", { relay: { from: "lead-b", to: "worker-a", messageId: "1" } }), ...edits("a", "2026-10-01T17:00:00.000Z").events]),
      facts("b", [fact("2026-10-01T15:00:00.000Z", { relay: { from: "x", to: "lead-b", messageId: "2" } }), ...edits("b", "2026-10-01T18:30:00.000Z").events]),
    ], []);
    expect(concurrentEdits(ctx, { windowH: 6, minShared: 2, maxRootsPerPath: 8, skipHot: true, crossOwnerOnly: true, suppressCommunicated: true })).toEqual([]);
    expect(concurrentEdits(ctx, { windowH: 6, minShared: 2, maxRootsPerPath: 8, skipHot: true, crossOwnerOnly: true, suppressCommunicated: false })).toHaveLength(1);
  });
  test("sessions sharing a parent are one root and never pair", () => {
    const ctx = ctxOf([row("lead", "K"), row("w1", "K", { parentSessionIds: ["lead"] }), row("w2", "K", { parentSessionIds: ["lead"] })],
      [edits("w1", "2026-10-01T17:00:00.000Z"), edits("w2", "2026-10-01T17:10:00.000Z")], []);
    expect(concurrentEdits(ctx, { windowH: 6, minShared: 1, maxRootsPerPath: 8, skipHot: true, crossOwnerOnly: false, suppressCommunicated: true })).toEqual([]);
  });
});

describe("shared trigger (D2)", () => {
  test("two roots opening fixes for the same red train PR within the window alert when the second acts", () => {
    const react = (id: string, t1: string, t2: string) => facts(id, [fact(t1, { prRefs: ["agentworkforce/cloud#6483"] }), fact(t2, { gh: ["pr-create"] })]);
    const ctx = ctxOf([row("x", "K", { project: { name: "AgentWorkforce/cloud" } }), row("y", "K", { project: { name: "AgentWorkforce/cloud" } })],
      [react("x", "2026-11-23T07:02:00.000Z", "2026-11-23T07:20:00.000Z"), react("y", "2026-11-23T07:03:00.000Z", "2026-11-23T07:25:00.000Z")], []);
    const alerts = sharedTrigger(ctx, { windowMin: 60, requireEdit: false, suppressCommunicated: true });
    expect(alerts.map((a) => [a.at, a.artifact])).toEqual([["2026-11-23T07:25:00.000Z", "pr:agentworkforce/cloud#6483"]]);
  });
});

describe("model", () => {
  test("an edit by absolute path into another repository is attributed to that repository", () => {
    const m = buildModel([row("lead", "K", { project: { name: "AgentWorkforce/chief" } })], [facts("lead", [fact("2026-09-09T18:00:00.000Z", {
      files: ["/Users/k/Projects/AgentWorkforce/cloud/packages/web/a.ts"], touched: [{ path: "docs/x.md", via: "status" }],
    })])]);
    expect(m.get("codex:lead")!.edits.map((e) => `${e.repo}:${e.path}`)).toEqual(["AgentWorkforce/cloud:packages/web/a.ts", "AgentWorkforce/chief:docs/x.md"]);
  });
});

describe("PR ownership", () => {
  test("a create call claims the PR whose URL its result prints; a session that only views a PR does not own it", async () => {
    const { prOwners } = await import("../overlap-bench/detectors.ts");
    const ctx = ctxOf([row("author", "K"), row("reviewer", "K")], [
      facts("author", [fact("2026-10-01T10:00:00.000Z", { gh: ["pr-create"] }), fact("2026-10-01T10:00:05.000Z", { prRefs: ["agentworkforce/relayhistory#4083"] })]),
      facts("reviewer", [fact("2026-10-01T11:00:00.000Z", { gh: ["pr-view"], prRefs: ["#4083"] }), fact("2026-10-01T11:05:00.000Z", { gh: ["pr-view"], prRefs: ["#4087"] })]),
    ], []);
    expect(ctx.sessions.get("codex:author")!.prs).toEqual([{ ts: "2026-10-01T10:00:00.000Z", repo: "agentworkforce/relayhistory", number: 4083, action: "create" }]);
    const owners = prOwners(ctx);
    expect([...(owners.get("pr:agentworkforce/relayhistory#4083") ?? [])]).toEqual(["codex:author"]);
    expect(owners.get("pr:agentworkforce/relayhistory#4087")).toBeUndefined();
  });
});

describe("merged harvests", () => {
  test("a session in both stores keeps the first row's parents, every owner, the latest activity and the union of events", () => {
    const m = mergeHarvests([
      { rows: [row("s", "K", { parentSessionIds: ["lead"], lastTs: "2026-11-04T00:00:00.000Z" })], facts: [facts("s", [fact("2026-11-04T00:00:00.000Z", { gh: ["pr-create"] })])] },
      { rows: [row("s", "W", { parentSessionIds: [], lastTs: "2026-11-20T00:00:00.000Z" }), row("t", "W")], facts: [facts("s", [fact("2026-11-04T00:00:00.000Z", { gh: ["pr-create"] }), fact("2026-11-20T00:00:00.000Z", { git: ["push"] })])] },
    ]);
    const s = m.rows.find((r) => r.sessionId === "s")!;
    expect([s.parentSessionIds, s.userIds, s.lastTs, m.rows.length]).toEqual([["lead"], ["K", "W"], "2026-11-20T00:00:00.000Z", 2]);
    expect(m.facts[0]!.events.map((e) => e.ts)).toEqual(["2026-11-04T00:00:00.000Z", "2026-11-20T00:00:00.000Z"]);
  });
});

describe("observed paths", () => {
  // The session starts in a checkout already dirty with a.ts, writes b.ts, then a later status shows c.ts too.
  const events = [
    fact("2026-09-09T18:00:00.000Z", { touched: [{ path: "src/a.ts", via: "status" }] }),
    fact("2026-09-09T18:05:00.000Z", { touched: [{ path: "src/b.ts", via: "patch" }] }),
    fact("2026-09-09T18:10:00.000Z", { touched: [{ path: "src/a.ts", via: "status" }, { path: "src/b.ts", via: "status" }, { path: "src/c.ts", via: "status" }] }),
    fact("2026-09-09T18:12:00.000Z", { touched: [{ path: "src/d.ts", via: "stat" }] }),
  ];
  const edited = (policy: "all" | "fresh" | "none") =>
    [...new Set(buildModel([row("s", "K")], [facts("s", events)], policy).get("codex:s")!.edits.map((e) => e.path))].sort();

  test("all counts every listed path as an edit", () => expect(edited("all")).toEqual(["src/a.ts", "src/b.ts", "src/c.ts", "src/d.ts"]));
  test("fresh drops the starting dirt and --stat listings, keeps writes and newly dirty paths", () => expect(edited("fresh")).toEqual(["src/b.ts", "src/c.ts"]));
  test("none keeps only writes", () => expect(edited("none")).toEqual(["src/b.ts"]));
});

describe("bench outputs", () => {
  test("refuse an --out inside the repository", async () => {
    const { assertOutsideRepo } = await import("../overlap-bench/cli.ts");
    expect(() => assertOutsideRepo("docs/bench-out")).toThrow("outside this repository");
    expect(existsSync("docs/bench-out")).toBe(false);
  });
});
