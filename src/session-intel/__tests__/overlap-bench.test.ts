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
  // Line C creates PR 93 at 17:00 and edits lib.rs at 17:30; bob, another line, edits lib.rs at 21:00.
  const creator = facts("cee", [
    fact("2026-09-01T17:00:00.000Z", { gh: ["pr-create"], prRefs: ["agentworkforce/relayhistory#93"] }),
    fact("2026-09-01T17:30:00.000Z", { touched: [{ path: "crates/core/src/lib.rs", via: "patch" }] }),
  ]);
  const P = { minShared: 1, maxPrFiles: 100, skipHot: true, suppressCommunicated: true };
  test("a session editing a file another line's open PR had worked on alerts at that edit, before the harm", () => {
    const ctx = ctxOf([row("bob", "W"), row("cee", "C")], [creator, facts("bob", [fact("2026-09-01T21:00:00.000Z", { touched: [{ path: "crates/core/src/lib.rs", via: "patch" }] })])], [pr(93, "machine-c")]);
    const alerts = openPrOverlap(ctx, P);
    expect(alerts).toHaveLength(1);
    expect(alerts[0]!.sides).toEqual(["root:codex:bob", "pr:agentworkforce/relayhistory#93"]);
    expect(alerts[0]!.at).toBe("2026-09-01T21:00:00.000Z");
    const incident = [{ id: "i1", repo: REPO, kind: "intent_conflict", lines: ["pr:93", "commit:d00dfee"], harmAt: "2026-09-09T14:43:00.000Z", split: "dev" as const }];
    // The commit line links through the sha the session printed; this session printed none, so it is unlinkable.
    expect(score("open-pr", ctx, alerts, incident, [], { sessions: 100, perHundred: 5 }).linkable).toBe(0);
    const ctx2 = ctxOf([row("bob", "W"), row("cee", "C")], [creator, facts("bob", [fact("2026-09-01T21:00:00.000Z", { touched: [{ path: "crates/core/src/lib.rs", via: "patch" }] }), fact("2026-09-01T21:18:00.000Z", { shas: ["d00dfee"] })])], [pr(93, "machine-c")]);
    const s2 = score("open-pr", ctx2, openPrOverlap(ctx2, P), incident, [], { sessions: 100, perHundred: 5 });
    expect(s2.hits.map((h) => h.id)).toEqual(["i1"]);
    expect(Math.round(s2.hits[0]!.leadH)).toBe(186);
  });
  test("never uses hindsight: a PR's final file list, or edits and ownership dated after the alert", () => {
    const bob = facts("bob", [fact("2026-09-01T21:00:00.000Z", { touched: [{ path: "crates/core/src/lib.rs", via: "patch" }] })]);
    // No owning line: the forge file list alone is known only in hindsight.
    expect(openPrOverlap(ctxOf([row("bob", "W")], [bob], [pr(93, "machine-c")]), P)).toEqual([]);
    // The owner edits lib.rs only after bob does.
    const late = facts("cee", [fact("2026-09-01T17:00:00.000Z", { gh: ["pr-create"], prRefs: ["agentworkforce/relayhistory#93"] }), fact("2026-09-01T22:00:00.000Z", { touched: [{ path: "crates/core/src/lib.rs", via: "patch" }] })]);
    expect(openPrOverlap(ctxOf([row("bob", "W"), row("cee", "C")], [bob, late], [pr(93, "machine-c")]), P)).toEqual([]);
    // The line edits lib.rs early but takes the PR (pushes its branch) only after bob's edit.
    const later = facts("cee", [fact("2026-09-01T17:30:00.000Z", { touched: [{ path: "crates/core/src/lib.rs", via: "patch" }] }), fact("2026-09-01T23:00:00.000Z", { branches: ["b93"] })]);
    expect(openPrOverlap(ctxOf([row("bob", "W"), row("cee", "C")], [bob, later], [pr(93, "machine-c")]), P)).toEqual([]);
  });
  test("a PR the session itself created, or one already merged, never alerts", () => {
    const own = ctxOf([row("k", "K")], [facts("k", [fact("2026-09-01T17:00:00.000Z", { gh: ["pr-create"], prRefs: ["agentworkforce/relayhistory#93"] }), fact("2026-09-01T18:00:00.000Z", { touched: [{ path: "crates/core/src/lib.rs", via: "add" }] })])], [pr(93, "k")]);
    expect(openPrOverlap(own, P)).toEqual([]);
    const merged = ctxOf([row("w", "W"), row("cee", "C")], [creator, facts("w", [fact("2026-09-03T00:00:00.000Z", { touched: [{ path: "crates/core/src/lib.rs", via: "add" }] })])], [pr(93, "x", { mergedAt: "2026-09-02T00:00:00.000Z" })]);
    expect(openPrOverlap(merged, P)).toEqual([]);
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
      facts("author", [fact("2026-10-01T10:00:00.000Z", { gh: ["pr-create"] }), fact("2026-10-01T10:00:05.000Z", { type: "tool_result", prRefs: ["agentworkforce/relayhistory#4083"] })]),
      facts("reviewer", [fact("2026-10-01T11:00:00.000Z", { gh: ["pr-view"], prRefs: ["#4083"] }), fact("2026-10-01T11:05:00.000Z", { gh: ["pr-view"], prRefs: ["#4087"] })]),
    ], []);
    // Owned from the result's time, when the number became known.
    expect(ctx.sessions.get("codex:author")!.prs).toEqual([{ ts: "2026-10-01T10:00:05.000Z", repo: "agentworkforce/relayhistory", number: 4083, action: "create" }]);
    const owners = prOwners(ctx);
    expect([...(owners.get("pr:agentworkforce/relayhistory#4083") ?? [])]).toEqual(["codex:author"]);
    expect(owners.get("pr:agentworkforce/relayhistory#4087")).toBeUndefined();
  });
  test("only the create call's own successful result claims a number", () => {
    const prsOf = (events: ReturnType<typeof fact>[]) => ctxOf([row("a", "K")], [facts("a", events)], []).sessions.get("codex:a")!.prs.filter((p) => p.action === "create" && p.number > 0);
    const call = fact("2026-10-01T10:00:00.000Z", { gh: ["pr-create"] });
    // A failed result, then a later unrelated URL: nothing is created.
    expect(prsOf([call, fact("2026-10-01T10:00:05.000Z", { type: "tool_result", toolStatus: "error" }), fact("2026-10-01T10:01:00.000Z", { type: "tool_result", prRefs: ["agentworkforce/relayhistory#5000"] })])).toEqual([]);
    // Another event intervenes before any result (an assistant message naming a PR): the wait ends.
    expect(prsOf([call, fact("2026-10-01T10:00:02.000Z", { type: "assistant", prRefs: ["agentworkforce/relayhistory#5001"] })])).toEqual([]);
    // A result past the window does not claim.
    expect(prsOf([call, fact("2026-10-01T10:30:00.000Z", { type: "tool_result", prRefs: ["agentworkforce/relayhistory#5002"] })])).toEqual([]);
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

describe("as-of bundles", () => {
  test("carry dated facts only: no title, description, file list or outcome, and ownership as of the alert", async () => {
    const { describe: describePair } = await import("../overlap-bench/bundle.ts");
    const ctx = ctxOf([row("cee", "C"), row("bob", "W")], [
      facts("cee", [fact("2026-09-01T17:00:00.000Z", { gh: ["pr-create"], prRefs: ["agentworkforce/relayhistory#93"] }), fact("2026-09-01T23:00:00.000Z", { gh: ["pr-create"], prRefs: ["agentworkforce/relayhistory#94"] })]),
      facts("bob", [fact("2026-09-01T21:00:00.000Z", { touched: [{ path: "src/a.ts", via: "patch" }] })]),
    ], [
      pr(93, "c", { title: "HINDSIGHT-TITLE", body: "HINDSIGHT-BODY", mergedAt: "2026-09-02T00:00:00.000Z", comments: [{ author: "x", at: "2026-09-01T18:00:00.000Z", body: "before" }, { author: "x", at: "2026-09-01T22:00:00.000Z", body: "after" }] }),
      pr(94, "c", { createdAt: "2026-09-01T16:00:00.000Z" }),
    ]);
    const b = describePair(ctx, ["root:codex:cee", "root:codex:bob"], "2026-09-01T21:00:00.000Z", () => "member", true);
    const text = JSON.stringify(b);
    for (const leak of ["HINDSIGHT", "mergedAt", "crates/core", "after"]) expect(text).not.toContain(leak);
    // PR 94 existed by then but its line only took it at 23:00.
    expect(b.sides[0]!.prsCreated.map((p) => [p.pr, p.comments?.map((c) => c.body)])).toEqual([["agentworkforce/relayhistory#93", ["before"]]]);
  });
});

describe("held-out freeze", () => {
  test("binds inputs, labels, budget and split", async () => {
    const { freezeTerms } = await import("../overlap-bench/cli.ts");
    const { mkdtempSync, mkdirSync, writeFileSync } = await import("node:fs");
    const { tmpdir } = await import("node:os");
    const { join } = await import("node:path");
    const d = mkdtempSync(join(tmpdir(), "freeze-")), h = join(d, "h"), f = join(d, "forge"), l = join(d, "labels.json");
    mkdirSync(h, { mode: 0o700 }); mkdirSync(join(f, "prs"), { recursive: true, mode: 0o700 });
    const P = { mode: 0o600 };
    writeFileSync(join(h, "directory.jsonl"), "a\n", P); writeFileSync(join(h, "facts.jsonl"), "b\n", P); writeFileSync(join(f, "prs", "r.json"), "[]", P); writeFileSync(l, "{}", P);
    const a = freezeTerms(h, f, l, 5);
    expect(a).toMatchObject({ perHundred: 5, split: "heldout", observed: "all" });
    writeFileSync(join(h, "facts.jsonl"), "c\n");
    expect(freezeTerms(h, f, l, 5).inputsHash).not.toBe(a.inputsHash);
    writeFileSync(l, "{ }");
    expect(freezeTerms(h, f, l, 5).labelsHash).not.toBe(a.labelsHash);
  });
});

describe("harvest scopes", () => {
  test("merged harvests must be of one workspace and within their delete-by", async () => {
    const { harvestScopes } = await import("../overlap-bench/cli.ts");
    const { mkdtempSync, writeFileSync } = await import("node:fs");
    const { tmpdir } = await import("node:os");
    const { join } = await import("node:path");
    const mk = (workspace: string, deleteBy: string) => {
      const d = mkdtempSync(join(tmpdir(), "scope-"));
      writeFileSync(join(d, "manifest.json"), JSON.stringify({ params: { workspace, since: "2026-09-01T00:00:00Z", until: "2026-10-01T00:00:00Z", deleteBy } }), { mode: 0o600 });
      return d;
    };
    const a = mk("w1", "2026-12-09T00:00:00Z"), b = mk("w1", "2026-12-09T00:00:00Z"), c = mk("w2", "2026-12-09T00:00:00Z"), old = mk("w1", "2026-11-01T00:00:00Z");
    expect(harvestScopes(`${a},${b}`, "2026-11-26T00:00:00Z").map((x) => x.workspace)).toEqual(["w1", "w1"]);
    expect(() => harvestScopes(`${a},${c}`, "2026-11-26T00:00:00Z")).toThrow("different workspaces");
    expect(() => harvestScopes(old, "2026-11-26T00:00:00Z")).toThrow("past its delete-by");
  });
});

describe("prefix invariance", () => {
  // Two lines converge on one file around a PR; the world then keeps going. Anything a detector or an as-of bundle
  // says about time T must be the same whether or not the world after T exists.
  const world = {
    rows: [row("a", "K"), row("b", "W"), row("c", "W", { createdAt: "2026-09-02T05:00:00.000Z" }), row("d", "W")],
    facts: [
      facts("a", [
        fact("2026-09-01T10:00:00.000Z", { kind: "tool_call", gh: ["pr-create"] }),
        fact("2026-09-01T10:00:04.000Z", { type: "tool_result", prRefs: ["agentworkforce/relayhistory#93"] }),
        fact("2026-09-01T10:30:00.000Z", { touched: [{ path: "src/x.ts", via: "patch" }, { path: "src/y.ts", via: "patch" }] }),
        fact("2026-09-01T13:00:00.000Z", { touched: [{ path: "src/z.ts", via: "patch" }] }),
      ]),
      facts("b", [
        fact("2026-09-01T11:00:00.000Z", { prRefs: ["agentworkforce/relayhistory#93"] }),
        fact("2026-09-01T11:30:00.000Z", { touched: [{ path: "src/x.ts", via: "patch" }] }),
        fact("2026-09-01T12:30:00.000Z", { touched: [{ path: "src/y.ts", via: "patch" }, { path: "src/z.ts", via: "patch" }] }),
        fact("2026-09-01T12:40:00.000Z", { kind: "tool_call", gh: ["pr-create"] }),
        fact("2026-09-01T12:40:03.000Z", { type: "tool_result", prRefs: ["agentworkforce/relayhistory#94"] }),
      ]),
      facts("c", [fact("2026-09-02T06:00:00.000Z", { prRefs: ["agentworkforce/relayhistory#93"], touched: [{ path: "src/x.ts", via: "patch" }] })]),
      // d edits a file of PR 93 while it is open, and only later takes the PR over (pushes its head branch).
      facts("d", [fact("2026-09-01T11:45:00.000Z", { touched: [{ path: "src/x.ts", via: "patch" }] }), fact("2026-09-01T13:00:00.000Z", { kind: "tool_call", branches: ["b93"] })]),
    ],
    prs: [
      pr(93, "a", { createdAt: "2026-09-01T10:00:02.000Z", mergedAt: "2026-09-01T14:00:00.000Z", files: ["src/x.ts", "src/y.ts", "src/z.ts", "src/w.ts"], comments: [{ author: "r", at: "2026-09-01T11:10:00.000Z", body: "early" }, { author: "r", at: "2026-09-01T13:30:00.000Z", body: "late" }],
        history: { titles: [{ at: "2026-09-01T10:00:02.000Z", title: "first title" }, { at: "2026-09-01T12:00:00.000Z", title: "renamed title" }], bodies: [{ at: "2026-09-01T10:00:02.000Z", body: "v1" }, { at: "2026-09-01T13:10:00.000Z", body: "v2" }] } }),
      pr(94, "b", { createdAt: "2026-09-01T12:40:01.000Z", closedAt: "2026-09-01T15:00:00.000Z", files: ["src/x.ts"] }),
    ],
  };
  const cut = (T: string) => ({
    rows: world.rows.filter((r) => String(r.createdAt) <= T),
    facts: world.facts.map((f) => ({ ...f, events: f.events.filter((e) => e.ts <= T) })),
    prs: world.prs.filter((p) => p.createdAt <= T).map((p) => ({ ...p, mergedAt: p.mergedAt && p.mergedAt <= T ? p.mergedAt : null, closedAt: p.closedAt && p.closedAt <= T ? p.closedAt : null, comments: (p.comments ?? []).filter((c) => c.at <= T),
      ...(p.history ? { history: { titles: p.history.titles.filter((x) => x.at <= T), bodies: p.history.bodies.filter((x) => x.at <= T) } } : {}),
      // The current (fetched) title and description are hindsight; a cut world only knows them as of T.
      title: p.history ? (p.history.titles.filter((x) => x.at <= T).at(-1)?.title ?? p.title) : p.title, body: p.history ? (p.history.bodies.filter((x) => x.at <= T).at(-1)?.body ?? p.body) : p.body })),
  });
  const ctxFrom = (w: { rows: DirectoryRow[]; facts: SessionFacts[]; prs: ForgePr[] }) => ctxOf(w.rows, w.facts, w.prs);
  const key = (a: { detector: string; sides: string[]; at: string; score: number }) => `${a.detector} ${a.sides.join(",")} ${a.at} ${a.score}`;

  test("every detector's alerts up to T, and every as-of bundle at T, ignore the world after T", async () => {
    const { components, rank } = await import("../overlap-bench/ranker.ts");
    const { describe: describePair } = await import("../overlap-bench/bundle.ts");
    const run = (c: Context) => {
      const comps = components(c);
      return [
        ...openPrOverlap(c, { minShared: 1, maxPrFiles: 30, skipHot: true, suppressCommunicated: true }),
        ...sharedTrigger(c, { windowMin: 240, requireEdit: false, suppressCommunicated: true }),
        ...concurrentEdits(c, { windowH: 6, minShared: 1, maxRootsPerPath: 8, skipHot: true, crossOwnerOnly: false, suppressCommunicated: true }),
        ...rank(c, comps, { wOpenPr: 1, wTrigger: 1, wRarity: 1, rarityCap: 3, wAgree: 0, wCrossOwner: 0, threshold: 0.5 }),
      ];
    };
    const full = run(ctxFrom(world));
    expect(full.length).toBeGreaterThan(0);
    for (const T of ["2026-09-01T11:00:00.000Z", "2026-09-01T11:30:00.000Z", "2026-09-01T12:35:00.000Z", "2026-09-01T12:41:00.000Z", "2026-09-01T13:45:00.000Z"]) {
      const part = run(ctxFrom(cut(T)));
      expect(part.filter((a) => a.at <= T).map(key).sort()).toEqual(full.filter((a) => a.at <= T).map(key).sort());
      for (const sides of [["root:codex:a", "root:codex:b"], ["root:codex:b", "pr:agentworkforce/relayhistory#93"], ["root:codex:a", "root:codex:c"]] as Array<[string, string]>) {
        expect(describePair(ctxFrom(cut(T)), sides, T, () => "member", true)).toEqual(describePair(ctxFrom(world), sides, T, () => "member", true));
      }
    }
  });
});

describe("dated PR history", () => {
  test("rebuilds titles from renames and descriptions from edits, and bundles show the version at the alert", async () => {
    const { historyOf } = await import("../forge/fetch.ts");
    const h = historyOf({
      number: 7, createdAt: "2026-09-01T10:00:00Z", title: "final", body: "final body",
      userContentEdits: { nodes: [{ editedAt: "2026-09-01T12:00:00Z", diff: "second", deletedAt: null }, { editedAt: "2026-09-01T10:00:00Z", diff: "first", deletedAt: null }] },
      timelineItems: { nodes: [{ __typename: "RenamedTitleEvent", createdAt: "2026-09-01T11:00:00Z", previousTitle: "original", currentTitle: "final" }] },
    });
    expect(h.titles).toEqual([{ at: "2026-09-01T10:00:00Z", title: "original" }, { at: "2026-09-01T11:00:00Z", title: "final" }]);
    expect(h.bodies.map((b) => b.body)).toEqual(["first", "second"]);
    const never = historyOf({ number: 8, createdAt: "2026-09-01T10:00:00Z", title: "t", body: "b", userContentEdits: { nodes: [] }, timelineItems: { nodes: [] } });
    expect([never.titles, never.bodies]).toEqual([[{ at: "2026-09-01T10:00:00Z", title: "t" }], [{ at: "2026-09-01T10:00:00Z", body: "b" }]]);

    const { describe: describePair } = await import("../overlap-bench/bundle.ts");
    const ctx = ctxOf([row("a", "K"), row("b", "W")], [
      facts("a", [fact("2026-09-01T10:00:00.000Z", { kind: "tool_call", gh: ["pr-create"] }), fact("2026-09-01T10:00:01.000Z", { type: "tool_result", prRefs: ["agentworkforce/relayhistory#7"] })]),
      facts("b", [fact("2026-09-01T10:30:00.000Z", { touched: [{ path: "src/a.ts", via: "patch" }] })]),
    ], [pr(7, "a", { createdAt: "2026-09-01T10:00:00.000Z", title: "final", body: "final body", history: { titles: h.titles, bodies: h.bodies } })]);
    const at = (t: string) => describePair(ctx, ["root:codex:a", "root:codex:b"], t, () => "member", true).sides[0]!.prsCreated[0]!;
    expect([at("2026-09-01T10:30:00.000Z").title, at("2026-09-01T10:30:00.000Z").body]).toEqual(["original", "first"]);
    expect([at("2026-09-01T12:30:00.000Z").title, at("2026-09-01T12:30:00.000Z").body]).toEqual(["final", "second"]);
  });
});
