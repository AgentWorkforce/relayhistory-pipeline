/** Linking signals added with the first corpus's fragment diagnosis: families, coordinator caps, mentioned commits. */
import { describe, expect, test } from "bun:test";
import { buildFeatures, factsAsOf, withCommitBranches, withoutBaseCommits } from "../features/build.ts";
import { branchStem, DEFAULT_LINK_PARAMS, linkSessions } from "../features/link.ts";
import { parseForge } from "../bench/corpus.ts";
import type { ForgeFacts, LinkKeys, SessionFacts } from "../features/types.ts";

const REPO = "example/widgets";
function facts(sessionId: string, keys: Partial<LinkKeys> = {}, startedAt = "2026-10-01T10:00:00.000Z", built: SessionFacts["built"] = []): SessionFacts {
  return {
    source: "claude", sessionId, analyzedAt: startedAt, startedAt, endedAt: startedAt.replace("10:00", "11:00"), title: sessionId,
    asked: [], built, gaps: [], rationale: [], outcomeStatuses: [], dropped: {},
    keys: { repo: REPO, branches: [], prs: [], commits: [], files: [], taskRefs: [], parents: [], ...keys },
  };
}
const groupOf = (groups: SessionFacts[][], id: string) => groups.find((g) => g.some((s) => s.sessionId === id))!.map((s) => s.sessionId).sort();
const sha = (c: string) => c.repeat(40).slice(0, 40);

describe("family links", () => {
  test("a parent and its few children are one feature", () => {
    const { groups, links } = linkSessions([facts("lead"), facts("child1", { parents: ["lead"] }), facts("child2", { parents: ["lead"] }), facts("other")]);
    expect(groupOf(groups, "child1")).toEqual(["child1", "child2", "lead"]);
    expect(groupOf(groups, "other")).toEqual(["other"]);
    expect(links.every((l) => l.signal === "family")).toBe(true);
  });
  test("siblings link without the parent in the corpus", () => {
    const { groups } = linkSessions([facts("c1", { parents: ["gone"] }), facts("c2", { parents: ["gone"] })]);
    expect(groupOf(groups, "c1")).toEqual(["c1", "c2"]);
  });
  test("a lead with more children than the fanout bound links none of them", () => {
    const kids = Array.from({ length: DEFAULT_LINK_PARAMS.maxKeyFanout + 1 }, (_, i) => facts(`k${i}`, { parents: ["lead"] }));
    const { groups } = linkSessions([facts("lead"), ...kids]);
    expect(groups.length).toBe(kids.length + 1);
  });
  test("a family key never crosses sources", () => {
    const codex = { ...facts("c2", { parents: ["p"] }), source: "codex" };
    const { groups } = linkSessions([facts("c1", { parents: ["p"] }), codex]);
    expect(groups.length).toBe(2);
  });
});

describe("coordinator caps", () => {
  test("a session naming more branches than the cap does not bridge features", () => {
    const lander = facts("lander", { branches: ["feat/a", "feat/b", "feat/c", "feat/d", "feat/e"] });
    const { groups } = linkSessions([facts("a", { branches: ["feat/a"] }), facts("b", { branches: ["feat/b"] }), lander]);
    expect(groups.length).toBe(3);
  });
  test("at the cap, branches still link", () => {
    const s = facts("s", { branches: ["feat/a", "feat/b", "feat/c", "feat/d"] });
    const { groups } = linkSessions([facts("a", { branches: ["feat/a"] }), s]);
    expect(groupOf(groups, "a")).toEqual(["a", "s"]);
  });
  test("a session naming more commits than the cap does not link on commits", () => {
    const many = facts("many", { commits: ["a1b2c3d", "b1b2c3d", "c1b2c3d", "d1b2c3d", "e1b2c3d"] });
    const { groups } = linkSessions([facts("x", { commits: ["a1b2c3d9"] }), many]);
    expect(groups.length).toBe(2);
  });
});

describe("branchStem", () => {
  test("review rounds share a stem; a truncated name never links", () => {
    expect(branchStem("review/fix-clock-r2")).toBe("review/fix-clock");
    expect(branchStem("review/fix-clock-v3")).toBe("review/fix-clock");
    expect(branchStem("review/fix-clock")).toBe("review/fix-clock");
    expect(branchStem("review/fix-clock-246a")).toBe("review/fix-clock-246a");
    expect(branchStem("release/schema-")).toBeNull();
    const { groups } = linkSessions([facts("r1", { branches: ["review/x-r1"] }), facts("r2", { branches: ["review/x-r2"] }), facts("t", { branches: ["release/schema-"] }), facts("t2", { branches: ["release/schema-"] })]);
    expect(groupOf(groups, "r1")).toEqual(["r1", "r2"]);
    expect(groupOf(groups, "t")).toEqual(["t"]);
  });
});

describe("mentioned commits", () => {
  const ev = { source: "claude", sessionId: "s", refId: "r", kind: "tool", at: null };
  test("the as-of cut keeps a mentioned commit and drops a built one from after asOf", () => {
    const late = { id: "b", kind: "commit" as const, what: "commit", ref: "bbbbbbb", at: "2026-10-01T10:30:00.000Z", session: { source: "claude", sessionId: "s" }, outcomeId: null, evidence: [ev] };
    const f = facts("s", { commits: ["aaaaaaa", "bbbbbbb"] }, "2026-10-01T10:00:00.000Z", [late]);
    expect(factsAsOf(f, "2026-10-01T10:20:00.000Z")!.keys.commits).toEqual(["aaaaaaa"]);
    expect(factsAsOf(f, "2026-10-02T00:00:00.000Z")!.keys.commits).toEqual(["aaaaaaa", "bbbbbbb"]);
  });
  test("a commit already on the default branch when the session started is its base, not its work", () => {
    const forge: ForgeFacts = { prs: [], flags: [], commits: [
      { sha: sha("1"), at: "2026-09-30T00:00:00.000Z", onDefaultBranch: true },
      { sha: sha("2"), at: "2026-10-01T10:30:00.000Z", onDefaultBranch: true },
    ] };
    const f = facts("s", { commits: ["1111111", "2222222", "3333333"] });
    expect(withoutBaseCommits(f, forge).keys.commits).toEqual(["2222222", "3333333"]);
    // Two reviews cut from one main tip stay apart.
    const built = buildFeatures([facts("r1", { commits: ["1111111"] }), facts("r2", { commits: ["1111111"] })], { asOf: "2026-11-01T00:00:00.000Z", forge });
    expect(built.length).toBe(2);
  });
  test("a reviewed commit names its branch when at most two branches hold it", () => {
    const forge: ForgeFacts = { prs: [], flags: [], commits: [], branches: [
      { name: "review/x", landed: false, landedAt: null, tipAt: "2026-10-01T00:00:00.000Z", commits: [sha("a")] },
      { name: "review/y", landed: false, landedAt: null, tipAt: "2026-10-01T00:00:00.000Z", commits: [sha("b")] },
      { name: "review/z", landed: false, landedAt: null, tipAt: "2026-10-01T00:00:00.000Z", commits: [sha("b")] },
      { name: "review/w", landed: false, landedAt: null, tipAt: "2026-10-01T00:00:00.000Z", commits: [sha("b")] },
    ] };
    expect(withCommitBranches(facts("s", { commits: ["aaaaaaaa"] }), forge).keys.branches).toEqual(["review/x"]);
    expect(withCommitBranches(facts("s", { commits: ["bbbbbbbb"] }), forge).keys.branches).toEqual([]);
    const built = buildFeatures([facts("pusher", { branches: ["review/x"] }), facts("reviewer", { commits: ["aaaaaaa"] })], { asOf: "2026-11-01T00:00:00.000Z", forge });
    expect(built.length).toBe(1);
  });
  test("forge branch commits are validated", () => {
    const base = { prs: [], commits: [], flags: [] };
    const branch = { name: "b/x", landed: false, landedAt: null, tipAt: "2026-10-01T00:00:00.000Z" };
    expect(() => parseForge({ ...base, branches: [{ ...branch, commits: [sha("a")] }] })).not.toThrow();
    expect(() => parseForge({ ...base, branches: [{ ...branch, commits: ["abc"] }] })).toThrow();
    expect(() => parseForge({ ...base, branches: [{ ...branch, commits: Array(201).fill(sha("a")) }] })).toThrow();
  });
});
