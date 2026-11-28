import { describe, expect, test } from "bun:test";
import { existsSync } from "node:fs";
import type { EventFacts } from "../cloud-harvest/features.ts";
import type { DirectoryRow, SessionFacts } from "../cloud-harvest/harvest.ts";
import { type Context, concurrentEdits, type ForgePr, openPrOverlap, sharedTrigger } from "../overlap-bench/detectors.ts";
import { buildModel, communicated } from "../overlap-bench/model.ts";
import { repoOf, repoRelative } from "../overlap-bench/paths.ts";
import { score } from "../overlap-bench/score.ts";
import { canonicalPr, mergeHarvests } from "../overlap-bench/cli.ts";

const REPO = "AgentWorkforce/relayhistory";
const row = (id: string, owner: string, extra: Partial<DirectoryRow> = {}): DirectoryRow =>
  ({ sessionId: id, source: "codex", lastTs: "2026-09-02T00:00:00.000Z", createdAt: "2026-09-01T00:00:00.000Z", project: { name: REPO }, userIds: [owner], parentSessionIds: [], ...extra });
const fact = (ts: string, extra: Partial<EventFacts> = {}): EventFacts => ({
  eventId: ts, ts, kind: null, type: null, role: null, tool: null, toolStatus: null, subagentId: null, userId: null,
  files: [], touched: [], git: [], gh: [], branches: [], prRefs: [], shas: [], relay: null, ...extra,
});
/** Events are numbered in order (`seq`) unless a test sets the raw sequence itself. */
const facts = (id: string, events: EventFacts[]): SessionFacts => ({ source: "codex", sessionId: id, eventsRead: events.length, censored: false, error: null, events: events.map((e, i) => ({ ...e, seq: e.seq ?? i })) });
const pr = (number: number, author: string, extra: Partial<ForgePr> = {}): ForgePr => ({
  repo: REPO, number, author, title: `t${number}`, headRef: `b${number}`, createdAt: "2026-09-01T16:00:00.000Z", mergedAt: null, closedAt: null,
  files: ["crates/core/src/lib.rs"], ...extra,
});
const ctxOf = (rows: DirectoryRow[], f: SessionFacts[], prs: ForgePr[]): Context => {
  const sessions = buildModel(rows, f);
  // As loadContext does: forge times take the model's one form.
  return { sessions, prs: prs.map(canonicalPr), communicated: communicated(sessions) };
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
    fact("2026-09-01T17:00:00.000Z", { gh: ["pr-create", "pr-create-sole"] }), fact("2026-09-01T17:00:05.000Z", { type: "tool_result", toolStatus: "success", prRefs: ["agentworkforce/relayhistory#93"] }),
    fact("2026-09-01T17:30:00.000Z", { touched: [{ path: "crates/core/src/lib.rs", via: "patch" }] }),
  ]);
  const P = { minShared: 1, maxPrFiles: 100, skipHot: true, suppressCommunicated: true };
  test("a session editing a file another line's open PR had worked on alerts at that edit, before the harm", () => {
    const ctx = ctxOf([row("bob", "W"), row("cee", "C")], [creator, facts("bob", [fact("2026-09-01T21:00:00.000Z", { touched: [{ path: "crates/core/src/lib.rs", via: "patch" }] })])], [pr(93, "machine-c")]);
    const alerts = openPrOverlap(ctx, P);
    expect(alerts).toHaveLength(1);
    expect(alerts[0]!.sides).toEqual(["root:codex:bob", "pr:agentworkforce/relayhistory#93"]);
    expect(alerts[0]!.at).toBe("2026-09-01T21:00:00.000000000Z");
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
    const late = facts("cee", [fact("2026-09-01T17:00:00.000Z", { gh: ["pr-create", "pr-create-sole"] }), fact("2026-09-01T17:00:05.000Z", { type: "tool_result", toolStatus: "success", prRefs: ["agentworkforce/relayhistory#93"] }), fact("2026-09-01T22:00:00.000Z", { touched: [{ path: "crates/core/src/lib.rs", via: "patch" }] })]);
    expect(openPrOverlap(ctxOf([row("bob", "W"), row("cee", "C")], [bob, late], [pr(93, "machine-c")]), P)).toEqual([]);
    // The line edits lib.rs early but takes the PR (pushes its branch) only after bob's edit.
    const later = facts("cee", [fact("2026-09-01T17:30:00.000Z", { touched: [{ path: "crates/core/src/lib.rs", via: "patch" }] }), fact("2026-09-01T23:00:00.000Z", { branches: ["b93"] })]);
    expect(openPrOverlap(ctxOf([row("bob", "W"), row("cee", "C")], [bob, later], [pr(93, "machine-c")]), P)).toEqual([]);
  });
  test("a PR the session itself created, or one already merged, never alerts", () => {
    const own = ctxOf([row("k", "K")], [facts("k", [fact("2026-09-01T17:00:00.000Z", { gh: ["pr-create", "pr-create-sole"] }), fact("2026-09-01T17:00:05.000Z", { type: "tool_result", toolStatus: "success", prRefs: ["agentworkforce/relayhistory#93"] }), fact("2026-09-01T18:00:00.000Z", { touched: [{ path: "crates/core/src/lib.rs", via: "add" }] })])], [pr(93, "k")]);
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
    expect(alerts.map((a) => a.at)).toEqual(["2026-10-01T18:30:00.000000000Z"]);
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
    const react = (id: string, t1: string, t2: string) => facts(id, [fact(t1, { prRefs: ["agentworkforce/cloud#6483"] }), fact(t2, { gh: ["pr-create", "pr-create-sole"] })]);
    const ctx = ctxOf([row("x", "K", { project: { name: "AgentWorkforce/cloud" } }), row("y", "K", { project: { name: "AgentWorkforce/cloud" } })],
      [react("x", "2026-11-23T07:02:00.000Z", "2026-11-23T07:20:00.000Z"), react("y", "2026-11-23T07:03:00.000Z", "2026-11-23T07:25:00.000Z")], []);
    const alerts = sharedTrigger(ctx, { windowMin: 60, requireEdit: false, suppressCommunicated: true });
    expect(alerts.map((a) => [a.at, a.artifact])).toEqual([["2026-11-23T07:25:00.000000000Z", "pr:agentworkforce/cloud#6483"]]);
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
      facts("author", [fact("2026-10-01T10:00:00.000Z", { gh: ["pr-create", "pr-create-sole"] }), fact("2026-10-01T10:00:05.000Z", { type: "tool_result", toolStatus: "success", prRefs: ["agentworkforce/relayhistory#4083"] })]),
      facts("reviewer", [fact("2026-10-01T11:00:00.000Z", { gh: ["pr-view"], prRefs: ["#4083"] }), fact("2026-10-01T11:05:00.000Z", { gh: ["pr-view"], prRefs: ["#4087"] })]),
    ], []);
    // Owned from the result's time, when the number became known.
    expect(ctx.sessions.get("codex:author")!.prs).toEqual([{ ts: "2026-10-01T10:00:05.000000000Z", repo: "agentworkforce/relayhistory", number: 4083, action: "create" }]);
    const owners = prOwners(ctx);
    expect([...(owners.get("pr:agentworkforce/relayhistory#4083") ?? [])]).toEqual(["codex:author"]);
    expect(owners.get("pr:agentworkforce/relayhistory#4087")).toBeUndefined();
  });
  test("only the create call's own successful result claims a number", () => {
    const prsOf = (events: ReturnType<typeof fact>[]) => ctxOf([row("a", "K")], [facts("a", events)], []).sessions.get("codex:a")!.prs.filter((p) => p.action === "create" && p.number > 0);
    const call = fact("2026-10-01T10:00:00.000Z", { gh: ["pr-create", "pr-create-sole"] });
    // A failed result, then a later unrelated URL: nothing is created.
    expect(prsOf([call, fact("2026-10-01T10:00:05.000Z", { type: "tool_result", toolStatus: "error" }), fact("2026-10-01T10:01:00.000Z", { type: "tool_result", toolStatus: "success", prRefs: ["agentworkforce/relayhistory#5000"] })])).toEqual([]);
    // Another event intervenes before any result (an assistant message naming a PR): the wait ends.
    expect(prsOf([call, fact("2026-10-01T10:00:02.000Z", { type: "assistant", prRefs: ["agentworkforce/relayhistory#5001"] })])).toEqual([]);
    // A result past the window does not claim.
    expect(prsOf([call, fact("2026-10-01T10:30:00.000Z", { type: "tool_result", toolStatus: "success", prRefs: ["agentworkforce/relayhistory#5002"] })])).toEqual([]);
    // A discarded raw event between the call and the result (seq 1 was not kept): not the call's own result.
    expect(prsOf([{ ...call, seq: 0 }, fact("2026-10-01T10:00:05.000Z", { type: "tool_result", toolStatus: "success", prRefs: ["agentworkforce/relayhistory#5003"], seq: 2 })])).toEqual([]);
    // Parallel calls: the result after the second call may be the first's; the create abstains.
    expect(prsOf([{ ...call, seq: 0 }, fact("2026-10-01T10:00:01.000Z", { kind: "tool_call", gh: ["pr-view"], seq: 1 }), fact("2026-10-01T10:00:05.000Z", { type: "tool_result", toolStatus: "success", prRefs: ["agentworkforce/relayhistory#5004"], seq: 2 })])).toEqual([]);
    // Facts harvested before `seq` existed: abstain.
    const noSeq = ctxOf([row("a", "K")], [{ source: "codex", sessionId: "a", eventsRead: 2, censored: false, error: null, events: [call, fact("2026-10-01T10:00:05.000Z", { type: "tool_result", toolStatus: "success", prRefs: ["agentworkforce/relayhistory#5005"] })] }], []);
    expect(noSeq.sessions.get("codex:a")!.prs.filter((p) => p.number > 0 && p.action === "create")).toEqual([]);
    // The adjacent, successful result claims.
    expect(prsOf([{ ...call, seq: 0 }, fact("2026-10-01T10:00:05.000Z", { type: "tool_result", toolStatus: "success", prRefs: ["agentworkforce/relayhistory#5006"], seq: 1 })]).map((p) => p.number)).toEqual([5006]);
  });
});

describe("merged harvests", () => {
  test("a session in both stores keeps the first row's parents, every owner, the latest activity and the union of events", () => {
    const m = mergeHarvests([
      { rows: [row("s", "K", { parentSessionIds: ["lead"], lastTs: "2026-11-04T00:00:00.000Z" })], facts: [facts("s", [fact("2026-11-04T00:00:00.000Z", { gh: ["pr-create", "pr-create-sole"] })])] },
      { rows: [row("s", "W", { parentSessionIds: [], lastTs: "2026-11-20T00:00:00.000Z" }), row("t", "W")], facts: [facts("s", [fact("2026-11-04T00:00:00.000Z", { gh: ["pr-create", "pr-create-sole"] }), fact("2026-11-20T00:00:00.000Z", { git: ["push"] })])] },
    ]);
    const s = m.rows.find((r) => r.sessionId === "s")!;
    expect([s.parentSessionIds, s.userIds, s.lastTs, m.rows.length]).toEqual([["lead"], ["K", "W"], "2026-11-20T00:00:00.000000000Z", 2]);
    expect(m.facts[0]!.events.map((e) => e.ts)).toEqual(["2026-11-04T00:00:00.000000000Z", "2026-11-20T00:00:00.000000000Z"]);
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
      facts("cee", [fact("2026-09-01T17:00:00.000Z", { gh: ["pr-create", "pr-create-sole"] }), fact("2026-09-01T17:00:05.000Z", { type: "tool_result", toolStatus: "success", prRefs: ["agentworkforce/relayhistory#93"] }), fact("2026-09-01T23:00:00.000Z", { gh: ["pr-create", "pr-create-sole"] }), fact("2026-09-01T23:00:05.000Z", { type: "tool_result", toolStatus: "success", prRefs: ["agentworkforce/relayhistory#94"] })]),
      facts("bob", [fact("2026-09-01T21:00:00.000Z", { touched: [{ path: "src/a.ts", via: "patch" }] })]),
    ], [
      pr(93, "c", { title: "HINDSIGHT-TITLE", body: "HINDSIGHT-BODY", mergedAt: "2026-09-02T00:00:00.000Z", comments: [{ author: "x", at: "2026-09-01T18:00:00.000Z", body: "before" }, { author: "x", at: "2026-09-01T22:00:00.000Z", body: "after" }] }),
      pr(94, "c", { createdAt: "2026-09-01T16:00:00.000Z" }),
    ]);
    const b = describePair(ctx, ["root:codex:cee", "root:codex:bob"], "2026-09-01T21:00:00.000Z", () => "member", true);
    const text = JSON.stringify(b);
    for (const leak of ["HINDSIGHT", "mergedAt", "crates/core", "after"]) expect(text).not.toContain(leak);
    // PR 94 existed by then but its line only took it at 23:00.
    // Comment text is not dated (fetched in its latest form), so none appears, before the alert or after.
    expect(b.sides[0]!.prsCreated.map((p) => p.pr)).toEqual(["agentworkforce/relayhistory#93"]);
    expect(text).not.toContain("before");
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
    writeFileSync(join(h, "manifest.json"), "{}", P); writeFileSync(join(h, "directory.jsonl"), "a\n", P); writeFileSync(join(h, "facts.jsonl"), "b\n", P); writeFileSync(join(f, "prs", "r.json"), "[]", P); writeFileSync(l, "{}", P);
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
        fact("2026-09-01T10:00:00.000Z", { kind: "tool_call", gh: ["pr-create", "pr-create-sole"] }),
        fact("2026-09-01T10:00:04.000Z", { type: "tool_result", toolStatus: "success", prRefs: ["agentworkforce/relayhistory#93"] }),
        fact("2026-09-01T10:30:00.000Z", { touched: [{ path: "src/x.ts", via: "patch" }, { path: "src/y.ts", via: "patch" }] }),
        fact("2026-09-01T13:00:00.000Z", { touched: [{ path: "src/z.ts", via: "patch" }] }),
      ]),
      facts("b", [
        fact("2026-09-01T11:00:00.000Z", { prRefs: ["agentworkforce/relayhistory#93"] }),
        fact("2026-09-01T11:30:00.000Z", { touched: [{ path: "src/x.ts", via: "patch" }] }),
        fact("2026-09-01T12:30:00.000Z", { touched: [{ path: "src/y.ts", via: "patch" }, { path: "src/z.ts", via: "patch" }] }),
        fact("2026-09-01T12:40:00.000Z", { kind: "tool_call", gh: ["pr-create", "pr-create-sole"] }),
        fact("2026-09-01T12:40:03.000Z", { type: "tool_result", toolStatus: "success", prRefs: ["agentworkforce/relayhistory#94"] }),
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
    const { outcomeFactsBaseline } = await import("../overlap-bench/baseline.ts");
    const { describe: describePair } = await import("../overlap-bench/bundle.ts");
    const run = (c: Context) => {
      const comps = components(c);
      return [
        ...openPrOverlap(c, { minShared: 1, maxPrFiles: 30, skipHot: true, suppressCommunicated: true }),
        ...sharedTrigger(c, { windowMin: 240, requireEdit: false, suppressCommunicated: true }),
        ...concurrentEdits(c, { windowH: 6, minShared: 1, maxRootsPerPath: 8, skipHot: true, crossOwnerOnly: false, suppressCommunicated: true }),
        ...rank(c, comps, { wOpenPr: 1, wTrigger: 1, wRarity: 1, rarityCap: 3, wAgree: 0, wCrossOwner: 0, threshold: 0.5 }),
        ...outcomeFactsBaseline(c, { since: "2026-09-01T00:00:00.000Z", until: "2026-09-03T00:00:00.000Z" }, { limit: 200 }),
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
      facts("a", [fact("2026-09-01T10:00:00.000Z", { kind: "tool_call", gh: ["pr-create", "pr-create-sole"] }), fact("2026-09-01T10:00:01.000Z", { type: "tool_result", toolStatus: "success", prRefs: ["agentworkforce/relayhistory#7"] })]),
      facts("b", [fact("2026-09-01T10:30:00.000Z", { touched: [{ path: "src/a.ts", via: "patch" }] })]),
    ], [pr(7, "a", { createdAt: "2026-09-01T10:00:00.000Z", title: "final", body: "final body", history: { titles: h.titles, bodies: h.bodies } })]);
    const at = (t: string) => describePair(ctx, ["root:codex:a", "root:codex:b"], t, () => "member", true).sides[0]!.prsCreated[0]!;
    expect([at("2026-09-01T10:30:00.000Z").title, at("2026-09-01T10:30:00.000Z").body]).toEqual(["original", "first"]);
    expect([at("2026-09-01T12:30:00.000Z").title, at("2026-09-01T12:30:00.000Z").body]).toEqual(["final", "second"]);
  });
});

describe("history gaps", () => {
  test("a deleted or empty description version is unknown, never filled from the current text", async () => {
    const { historyOf } = await import("../forge/fetch.ts");
    const { asOf } = await import("../forge/types.ts");
    const h = historyOf({
      number: 9, createdAt: "2026-09-01T10:00:00Z", title: "t", body: "CURRENT-BODY",
      userContentEdits: { nodes: [{ editedAt: "2026-09-01T10:00:00Z", diff: null, deletedAt: "2026-09-02T00:00:00Z" }, { editedAt: "2026-09-01T12:00:00Z", diff: "CURRENT-BODY", deletedAt: null }] },
      timelineItems: { nodes: [] },
    });
    expect(asOf(h.bodies, "2026-09-01T11:00:00Z")?.body).toBeNull();
    expect(asOf(h.bodies, "2026-09-01T13:00:00Z")?.body).toBe("CURRENT-BODY");
    const only = historyOf({ number: 10, createdAt: "2026-09-01T10:00:00Z", title: "t", body: "CURRENT-BODY", userContentEdits: { nodes: [{ editedAt: "2026-09-01T10:00:00Z", diff: null, deletedAt: "2026-09-01T10:30:00Z" }] }, timelineItems: { nodes: [] } });
    expect(only.bodies).toEqual([{ at: "2026-09-01T10:00:00Z", body: null }]);
  });
});

describe("time order (canonical full-precision times)", () => {
  test("canonicalTs is one fixed width at full precision, so string order is time order", async () => {
    const { canonicalTs } = await import("../cloud-harvest/instant.ts");
    expect(canonicalTs("2026-11-26T12:00:00Z")).toBe("2026-11-26T12:00:00.000000000Z");
    expect(canonicalTs("2026-11-26T12:00:00.5Z")).toBe("2026-11-26T12:00:00.500000000Z");
    expect(canonicalTs("2026-11-26T12:00:00.000900Z")).toBe("2026-11-26T12:00:00.000900000Z");
    expect(canonicalTs("2026-11-26T13:00:00.000000001+01:00")).toBe("2026-11-26T12:00:00.000000001Z");
    for (const bad of ["2026-02-30T00:00:00Z", "yesterday", "", null, 5]) expect(canonicalTs(bad)).toBeNull();
    // Mixed widths sort a later instant first; canonical forms do not.
    const raw = ["2026-11-26T12:00:00.000900Z", "2026-11-26T12:00:00Z", "2026-11-26T12:00:00.5Z", "2026-11-26T12:00:00.000Z"];
    expect([...raw].sort()[0]).toBe("2026-11-26T12:00:00.000900Z"); // the raw strings misorder
    const canon = raw.map((t) => canonicalTs(t)!).sort();
    expect(canon).toEqual(["2026-11-26T12:00:00.000000000Z", "2026-11-26T12:00:00.000000000Z", "2026-11-26T12:00:00.000900000Z", "2026-11-26T12:00:00.500000000Z"]);
  });
  test("an edit 900 microseconds after an alert, in the same millisecond, is hidden from the as-of bundle", async () => {
    const { eventFacts } = await import("../cloud-harvest/features.ts");
    const { describe: describePair } = await import("../overlap-bench/bundle.ts");
    // The production reducer, from raw events.
    const raw = (id: string, ts: string) => eventFacts({ eventId: id, ts, kind: "tool_call", type: "tool_call", actorRole: "assistant", toolName: "apply_patch",
      content: "*** Begin Patch\n*** Update File: crates/core/src/lib.rs\n@@\n-a\n+b\n*** End Patch" });
    const early = raw("e1", "2026-09-01T11:00:00.000Z"), late = raw("e2", "2026-09-01T12:00:00.000900Z");
    expect(late.ts).toBe("2026-09-01T12:00:00.000900000Z");
    const ctx = ctxOf([row("a", "K"), row("b", "W")], [facts("a", [early]), facts("b", [{ ...late, touched: late.touched.map((t) => ({ ...t, path: "crates/core/src/other.rs" })) }, raw("e3", "2026-09-01T12:00:00.000800Z")])], []);
    const edited = (at: string) => describePair(ctx, ["root:codex:a", "root:codex:b"], at, () => "member", true).sides[1]!.editedFiles;
    expect(edited("2026-09-01T12:00:00.000Z")).toEqual([]);
    expect(edited("2026-09-01T12:00:00.000800Z")).toEqual(["crates/core/src/lib.rs"]);
    expect(edited("2026-09-01T12:00:00.000900Z").sort()).toEqual(["crates/core/src/lib.rs", "crates/core/src/other.rs"]);
  });
  test("a forge PR created at a whole second is open for an edit later in that second", () => {
    // GitHub times carry no fraction and event times do; as raw strings "21:00:00Z" sorts after "21:00:00.5Z", which
    // would make the PR look not yet created. Line C owns PR 93 (created at 17:00 by its facts); bob edits lib.rs.
    const creator = facts("cee", [
      fact("2026-09-01T17:00:00.000Z", { gh: ["pr-create", "pr-create-sole"] }), fact("2026-09-01T17:00:05.000Z", { type: "tool_result", toolStatus: "success", prRefs: ["agentworkforce/relayhistory#93"] }),
      fact("2026-09-01T17:30:00.000Z", { touched: [{ path: "crates/core/src/lib.rs", via: "patch" }] }),
    ]);
    const edit = facts("bob", [fact("2026-09-01T21:00:00.5Z", { touched: [{ path: "crates/core/src/lib.rs", via: "patch" }] })]);
    const ctx = ctxOf([row("bob", "W"), row("cee", "C")], [creator, edit], [pr(93, "machine-c", { createdAt: "2026-09-01T21:00:00Z" })]);
    const P = { minShared: 1, maxPrFiles: 100, skipHot: true, suppressCommunicated: true };
    expect(openPrOverlap(ctx, P).map((a) => a.at)).toEqual(["2026-09-01T21:00:00.500000000Z"]);
    // Raw (not canonical) PR times misorder: the same world without canonicalPr raises nothing.
    expect(openPrOverlap({ ...ctx, prs: [pr(93, "machine-c", { createdAt: "2026-09-01T21:00:00Z" })] }, P)).toEqual([]);
  });
});

describe("windows at full precision, and validated forge and label times", () => {
  const edit = (id: string, ts: string) => facts(id, [fact(ts, { touched: [{ path: "crates/core/src/lib.rs", via: "patch" }, { path: "crates/core/src/main.rs", via: "patch" }] })]);
  const D3 = { windowH: 1, minShared: 1, maxRootsPerPath: 8, skipHot: true, crossOwnerOnly: true, suppressCommunicated: true };
  test("D3: edits exactly 1 h apart alert at windowH 1; 1 h + 1 µs apart do not", () => {
    const at = (t: string) => concurrentEdits(ctxOf([row("a", "K"), row("b", "W")], [edit("a", "2026-10-01T17:00:00.000000Z"), edit("b", t)], []), D3).length;
    expect(at("2026-10-01T18:00:00.000000Z")).toBe(1);
    expect(at("2026-10-01T18:00:00.000001Z")).toBe(0);
  });
  test("D2: an act exactly windowMin after the reference counts; 1 µs later does not", () => {
    const react = (id: string, t1: string, t2: string) => facts(id, [fact(t1, { prRefs: ["agentworkforce/cloud#6483"] }), fact(t2, { gh: ["pr-create", "pr-create-sole"] })]);
    const rows = [row("x", "K", { project: { name: "AgentWorkforce/cloud" } }), row("y", "K", { project: { name: "AgentWorkforce/cloud" } })];
    const n = (late: string) => sharedTrigger(ctxOf(rows, [react("x", "2026-11-23T07:02:00Z", "2026-11-23T07:10:00Z"), react("y", "2026-11-23T07:03:00Z", late)], []),
      { windowMin: 15, requireEdit: false, suppressCommunicated: true }).length;
    expect(n("2026-11-23T07:18:00.000000Z")).toBe(1);
    expect(n("2026-11-23T07:18:00.000001Z")).toBe(0);
  });
  test("a create call's result claims its PR within 10 minutes at full precision", () => {
    const owned = (resultAt: string) => buildModel([row("author", "K")], [facts("author", [
      fact("2026-10-01T10:00:00.000000Z", { gh: ["pr-create", "pr-create-sole"] }),
      fact(resultAt, { type: "tool_result", toolStatus: "success", prRefs: ["agentworkforce/relayhistory#4083"] }),
    ])]).get("codex:author")!.prs.filter((x) => x.action === "create" && x.number === 4083).length;
    expect(owned("2026-10-01T10:10:00.000000Z")).toBe(1);
    expect(owned("2026-10-01T10:10:00.000001Z")).toBe(0);
  });
  test("forge and label times are validated on load: an invalid one is refused, never compared as a string", () => {
    const bad = [
      pr(1, "u", { createdAt: "" }), pr(2, "u", { mergedAt: "soon" }), pr(3, "u", { closedAt: "2026-02-30T00:00:00Z" }),
      pr(4, "u", { history: { titles: [{ at: "", title: "x" }], bodies: [] } }), pr(5, "u", { history: { titles: [], bodies: [{ at: "nope", body: "x" }] } }),
    ];
    for (const p of bad) expect(() => canonicalPr(p)).toThrow(/is not a valid time/);
    const ok = canonicalPr(pr(6, "u", { createdAt: "2026-09-01T16:00:00Z", mergedAt: null, history: { titles: [{ at: "2026-09-01T16:00:00Z", title: "t" }], bodies: [] } }));
    expect([ok.createdAt, ok.mergedAt, ok.history!.titles[0]!.at]).toEqual(["2026-09-01T16:00:00.000000000Z", null, "2026-09-01T16:00:00.000000000Z"]);
  });
});

describe("as-of membership with no valid time", () => {
  test("a session whose directory row has no valid time and no dated facts is never known as of an alert", async () => {
    const { describe: describePair } = await import("../overlap-bench/bundle.ts");
    const ctx = ctxOf([row("a", "K"), row("ghost", "W", { createdAt: "", lastTs: "" })], [facts("a", [fact("2026-09-01T10:00:00Z", { touched: [{ path: "crates/core/src/lib.rs", via: "patch" }] })])], []);
    const side = describePair(ctx, ["root:codex:a", "root:codex:ghost"], "2026-09-01T12:00:00Z", () => "member", true).sides[1]!;
    expect(side.kind === "work" && side.sessions).toBe(0);
  });
});

describe("PR create claims: the result, at or after its call", () => {
  const created = (events: EventFacts[]) => buildModel([row("author", "K")], [facts("author", events)]).get("codex:author")!.prs;
  test("an adjacent result timestamped before its call (by 1 ns) claims nothing", () => {
    const prs = created([fact("2026-10-01T10:00:00.000000001Z", { gh: ["pr-create", "pr-create-sole"] }), fact("2026-10-01T10:00:00.000000000Z", { type: "tool_result", toolStatus: "success", prRefs: ["agentworkforce/relayhistory#10"] })]);
    expect(prs.filter((p) => p.action === "create" && p.number === 10)).toEqual([]);
    const ok = created([fact("2026-10-01T10:00:00.000000001Z", { gh: ["pr-create", "pr-create-sole"] }), fact("2026-10-01T10:00:00.000000001Z", { type: "tool_result", toolStatus: "success", prRefs: ["agentworkforce/relayhistory#10"] })]);
    expect(ok.filter((p) => p.action === "create").map((p) => p.number)).toEqual([10]);
  });
  test("a PR a create call cites in its body is a mention; the result's new PR is the one created", async () => {
    const { eventFacts } = await import("../cloud-harvest/features.ts");
    const call = eventFacts({ eventId: "c", ts: "2026-10-01T10:00:00Z", kind: "tool_call", type: "tool_call", toolName: "Bash",
      content: 'gh pr create --title "Follow up" --body "Follows #7 and https://github.com/AgentWorkforce/relayhistory/pull/7"' });
    const result = eventFacts({ eventId: "r", ts: "2026-10-01T10:00:04Z", type: "tool_result", actorRole: "tool_result", toolStatus: "success", content: "https://github.com/AgentWorkforce/relayhistory/pull/10\n" });
    const prs = created([call, result]);
    expect(prs.filter((p) => p.action === "create").map((p) => p.number)).toEqual([10]);
    expect(prs.filter((p) => p.number === 7).every((p) => p.action === "mention")).toBe(true);
    expect(prs.some((p) => p.number === 7)).toBe(true);
  });
  test("a failed adjacent result removes the placeholder and claims nothing, even if it prints a URL", () => {
    const prs = created([fact("2026-10-01T10:00:00Z", { gh: ["pr-create", "pr-create-sole"] }), fact("2026-10-01T10:00:03Z", { type: "tool_result", toolStatus: "error", prRefs: ["agentworkforce/relayhistory#10"] })]);
    expect(prs.filter((p) => p.action === "create")).toEqual([]);
  });
});

describe("executed gh commands only, and one identifiable created PR", () => {
  test("gh counts only as a command word: echoed, quoted or prose gh is not a command", async () => {
    const { ghCommands } = await import("../cloud-harvest/commands.ts");
    const verbs = (t: string) => ghCommands(t).map((w) => w.slice(0, 2).join(" "));
    expect(verbs("gh pr create --title x")).toEqual(["pr create"]);
    expect(verbs("cd repo && GH_TOKEN=x gh pr create --fill; gh pr merge 98")).toEqual(["pr create", "pr merge"]);
    expect(verbs("bash -lc 'git push && gh pr create --fill'")).toEqual(["pr create"]);
    expect(verbs('{"command":"gh pr create --title \\"x\\"","description":"open it"}')).toEqual(["pr create"]);
    expect(verbs('{"cmd":"gh pr view 12","yield_time_ms":1000}')).toEqual(["pr view"]);
    expect(verbs("exec_command gh pr create --fill")).toEqual(["pr create"]);
    expect(verbs("gh pr create > out.txt 2>&1")).toEqual(["pr create"]);
    expect(verbs('git commit -m "fix #12" && gh pr create --body "a;b # c"')).toEqual(["pr create"]);
    for (const t of ['echo "gh pr create https://github.com/example/demo/pull/7"', "printf '%s' 'gh pr create'", "grep -r 'gh pr create' docs",
      "I will run gh pr create next", 'git commit -m "run gh pr create later"', "echo 'unbalanced",
      // JSON anywhere but the envelope is data; heredoc bodies are data; code-mode wrappers are not read.
      `printf '%s\\n' '{"cmd":"gh pr create"}' https://github.com/example/demo/pull/7`,
      "cat <<'EOF'\ngh pr create https://github.com/example/demo/pull/7\nEOF",
      "cat <<EOF > notes.md\ngh pr create\nEOF\ngh pr view 3",
      "grep x <<< 'gh pr create'",
      'exec {"input":"text(await tools.exec_command({cmd:`gh pr view 12`}))"}',
      '{"cmd": 5}', "[1,2]", "{not json",
      // Outside the grammar, the whole call yields nothing: comments, wrappers, other shell forms, substitution.
      "printf ok # ignored; gh pr create", "command -v gh pr create", "env --help gh pr create", "bash helper -c 'gh pr create'",
      "sudo -E gh pr create", "gh pr create &", 'echo "$(gh pr create)"', "(gh pr create)", "nohup gh pr create",
      '{"cmd":"bash -lc \'printf ok # x; gh pr create\'"}',
    ]) expect(verbs(t)).toEqual([]);
  });
  test("an echoed create command and its echoed output own nothing", async () => {
    const { eventFacts } = await import("../cloud-harvest/features.ts");
    const call = eventFacts({ eventId: "c", ts: "2026-10-01T10:00:00Z", kind: "tool_call", type: "tool_call", toolName: "Bash", content: 'echo "gh pr create https://github.com/example/demo/pull/7"' });
    const out = eventFacts({ eventId: "r", ts: "2026-10-01T10:00:01Z", type: "tool_result", actorRole: "tool_result", content: "gh pr create https://github.com/example/demo/pull/7\n" });
    expect(call.gh).toEqual([]);
    const prs = buildModel([row("author", "K")], [facts("author", [call, out])]).get("codex:author")!.prs;
    expect(prs.filter((p) => p.action === "create")).toEqual([]);
  });
  test("a create result printing two distinct PR URLs claims neither", () => {
    const prs = buildModel([row("author", "K")], [facts("author", [
      fact("2026-10-01T10:00:00Z", { gh: ["pr-create", "pr-create-sole"] }),
      fact("2026-10-01T10:00:02Z", { type: "tool_result", toolStatus: "success", prRefs: ["agentworkforce/relayhistory#9", "agentworkforce/relayhistory#10"] }),
    ])]).get("codex:author")!.prs;
    expect(prs.filter((p) => p.action === "create" && p.number > 0)).toEqual([]);
    expect(prs.filter((p) => p.action === "mention").map((p) => p.number).sort()).toEqual([10, 9]);
    // The same URL printed twice still identifies one PR.
    const one = buildModel([row("author", "K")], [facts("author", [
      fact("2026-10-01T10:00:00Z", { gh: ["pr-create", "pr-create-sole"] }),
      fact("2026-10-01T10:00:02Z", { type: "tool_result", toolStatus: "success", prRefs: ["agentworkforce/relayhistory#10", "agentworkforce/relayhistory#10"] }),
    ])]).get("codex:author")!.prs;
    expect(one.filter((p) => p.action === "create").map((p) => p.number)).toEqual([10]);
  });
});

describe("command envelopes: printf'd JSON and heredoc bodies own nothing", () => {
  for (const [name, content] of [
    ["printf of literal JSON", `printf '%s\\n' '{"cmd":"gh pr create"}' 'https://github.com/example/demo/pull/7'`],
    ["cat of a quoted heredoc", "cat <<'EOF'\ngh pr create\nhttps://github.com/example/demo/pull/7\nEOF"],
  ] as const) test(name, async () => {
    const { eventFacts } = await import("../cloud-harvest/features.ts");
    const call = eventFacts({ eventId: "c", ts: "2026-10-01T10:00:00Z", kind: "tool_call", type: "tool_call", toolName: "Bash", content });
    const out = eventFacts({ eventId: "r", ts: "2026-10-01T10:00:01Z", type: "tool_result", actorRole: "tool_result", content: "https://github.com/example/demo/pull/7\n" });
    expect(call.gh).toEqual([]);
    const prs = buildModel([row("author", "K")], [facts("author", [call, out])]).get("codex:author")!.prs;
    expect(prs.filter((p) => p.action === "create")).toEqual([]);
  });
});

describe("a gh inside a shell comment owns nothing, direct or wrapped", () => {
  for (const [name, content] of [
    ["direct", "printf ok # ignored; gh pr create"],
    ["JSON cmd wrapping bash -c", '{"cmd":"bash -c \'printf ok # ignored; gh pr create\'"}'],
  ] as const) test(name, async () => {
    const { eventFacts } = await import("../cloud-harvest/features.ts");
    const call = eventFacts({ eventId: "c", ts: "2026-10-01T10:00:00Z", kind: "tool_call", type: "tool_call", toolName: "Bash", content });
    const out = eventFacts({ eventId: "r", ts: "2026-10-01T10:00:01Z", type: "tool_result", actorRole: "tool_result", content: "https://github.com/example/demo/pull/7\n" });
    expect(call.gh).toEqual([]);
    expect(buildModel([row("author", "K")], [facts("author", [call, out])]).get("codex:author")!.prs.filter((p) => p.action === "create")).toEqual([]);
  });
});

describe("only a sole gh pr create invocation may claim its PR", () => {
  const URL = "https://github.com/example/demo/pull/7";
  const run = async (content: string) => {
    const { eventFacts } = await import("../cloud-harvest/features.ts");
    const call = eventFacts({ eventId: "c", ts: "2026-10-01T10:00:00Z", kind: "tool_call", type: "tool_call", toolName: "Bash", content });
    const out = eventFacts({ eventId: "r", ts: "2026-10-01T10:00:01Z", type: "tool_result", actorRole: "tool_result", toolStatus: "success", content: `${URL}\n` });
    return buildModel([row("author", "K")], [facts("author", [call, out])]).get("codex:author")!.prs.filter((p) => p.action === "create");
  };
  test("compound calls that can print a URL without running gh claim nothing, but still count as an act", async () => {
    for (const c of [`true || gh pr create; printf '${URL}'`, `gh pr create <missing || printf '${URL}'`, `{"cmd":"sh -c 'true || gh pr create; printf ${URL}'"}`,
      `gh pr create; echo ${URL}`, `cd r; gh pr create`, `git push && gh pr create`]) {
      const prs = await run(c);
      expect(prs.filter((p) => p.number > 0)).toEqual([]);
      expect(prs.map((p) => p.number)).toEqual([-1]); // an act of opening a PR, owning nothing
    }
  });
  test("a sole create, optionally after cd steps or inside one sh -c script, claims the PR its result prints", async () => {
    for (const c of ["gh pr create --fill", "cd repo && gh pr create --fill", "bash -lc 'cd r && gh pr create'", '{"cmd":"gh pr create --fill"}'])
      expect((await run(c)).map((p) => p.number)).toEqual([7]);
  });
});

describe("a claim needs a shell tool, a creating invocation and a successful result", () => {
  const URL = "https://github.com/example/demo/pull/7";
  const run = async (tool: string, content: string, status: string | null) => {
    const { eventFacts } = await import("../cloud-harvest/features.ts");
    const call = eventFacts({ eventId: "c", ts: "2026-10-01T10:00:00Z", kind: "tool_call", type: "tool_call", toolName: tool, content });
    const out = eventFacts({ eventId: "r", ts: "2026-10-01T10:00:01Z", type: "tool_result", actorRole: "tool_result", toolStatus: status, content: `${URL}\n` });
    return buildModel([row("author", "K")], [facts("author", [call, out])]).get("codex:author")!.prs.filter((p) => p.action === "create" && p.number > 0);
  };
  test("a non-shell tool whose text reads gh pr create claims nothing", async () => {
    for (const tool of ["Read", "Write", "WebFetch", "exec", "mcp__x__y"]) expect(await run(tool, "gh pr create", "success")).toEqual([]);
  });
  test("a cancelled, unknown or missing status claims nothing; only success does", async () => {
    for (const status of ["cancelled", "canceled", "pending", "unknown", "", null, "error", "failed"]) expect(await run("Bash", "gh pr create --fill", status)).toEqual([]);
    expect((await run("Bash", "gh pr create --fill", "success")).map((p) => p.number)).toEqual([7]);
  });
  test("a non-creating invocation (--dry-run, --web, --help) claims nothing, even when its result prints a PR URL", async () => {
    for (const c of [`gh pr create --dry-run --body ${URL}`, "gh pr create --dry-run=true", "gh pr create --web", "gh pr create -w", "gh pr create --help", "gh pr create -h",
      "gh pr create --web=true", "gh pr create -wf", "gh pr create -fh", "gh pr create -- --dry-run",
      // Arguments known only when the shell runs: an expansion, a redirection glued to a flag, globs, tildes.
      `MODE=--dry-run bash -c 'gh pr create "$MODE" --body ${URL}'`, `gh pr create --dry-run>out --body ${URL}`, "gh pr create $FLAGS", 'gh pr create "$(cat flags)"',
      "gh pr create --title x*", "cd ~ && gh pr create --fill", "gh pr create --body-file <(echo x)", "gh pr create > out.txt",
      // A quoted line continuation or escape the shell rewrites, and boolean shorthand assignments.
      `gh pr create "--dry-\\\nrun" --body ${URL}`, `gh pr create --dry\\-run`, "gh pr create -w=true", "gh pr create -h=true", "gh pr create -fw=1",
      `bash -c 'gh pr create -w=true'`, `{"cmd":"gh pr create -h=true"}`, `{"cmd":"bash -lc 'gh pr create \\"--dry-\\\\\\nrun\\"'"}`])
      expect(await run("Bash", c, "success")).toEqual([]);
  });
  test("each shell tool name the harvest records may claim", async () => {
    for (const tool of ["Bash", "bash", "Shell", "shell", "exec_command", "run_terminal_command"]) expect((await run(tool, "gh pr create --fill", "success")).map((p) => p.number)).toEqual([7]);
  });
});
