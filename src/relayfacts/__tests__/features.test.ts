/** Relayfacts feature records and their benchmark, on synthetic snapshots only. */
import { describe, expect, test } from "bun:test";
import type { SessionAnalysisSnapshot } from "../../session-analysis/engine/session-analysis-result.ts";
import { buildFeatures, deriveState, factsAsOf } from "../features/build.ts";
import { cutWords, extractSessionFacts, repoRelative } from "../features/extract.ts";
import { linkSessions } from "../features/link.ts";
import { renderFeature } from "../features/render.ts";
import type { ForgeFacts } from "../features/types.ts";
import type { Corpus } from "../bench/corpus.ts";
import { parseGold } from "../bench/gold.ts";
import { checkGrounding, scoreRun } from "../bench/score.ts";

type Ref = { id: string; kind: string; at: string | null; summary: string; detail?: string | null };
const claim = (text: string, evidenceIds: string[], confidence = "high") => ({ text, evidenceIds, confidence, basis: "observed" });

function snap(sessionId: string, refs: Ref[], u: Record<string, unknown>, files: string[] = [], source = "claude"): SessionAnalysisSnapshot {
  return {
    source, sessionId, title: `Session ${sessionId}`, analyzedAt: "2026-11-20T00:00:00.000Z",
    evidence: { references: refs.map((r) => ({ source, sessionId, detail: null, truncated: false, isError: null, toolCallId: null, locator: { eventUid: null, toolUseId: null, fileEditId: null }, ...r })) },
    metrics: { filesChanged: files.map((path) => ({ path, editCount: 1, linesAdded: null, linesRemoved: null, userModified: null })) },
    understanding: { intents: [], constraints: [], decisions: [], outcomes: [], ...u },
  } as unknown as SessionAnalysisSnapshot;
}

const human = (id: string, at: string, summary: string): Ref => ({ id, kind: "human-message", at, summary });
const tool = (id: string, at: string, kind: "tool-call" | "tool-result", summary: string, detail: string | null = null): Ref => ({ id, kind, at, summary, detail });
const outcome = (id: string, intent: ReturnType<typeof claim>, more: Record<string, unknown> = {}) => ({
  id, intent, status: "implemented", assessment: claim("ok", intent.evidenceIds), changes: [], verification: { value: "passed", explanation: "tests passed", evidenceIds: [], confidence: "high" },
  remaining: [], contradictions: [], supersededBy: null, ...more,
});

const A = snap("sess-a", [
  human("h1", "2026-11-10T10:00:00Z", "Please add   a retry to the uploader so a flaky network never loses a session. Keep it behind a flag."),
  tool("t1", "2026-11-10T10:05:00Z", "tool-call", "git checkout -b feat/upload-retry"),
  tool("t2", "2026-11-10T11:00:00Z", "tool-result", "git commit", "[feat/upload-retry 1a2b3c4d5e] Add upload retry\n 2 files changed"),
  tool("t3", "2026-11-10T11:05:00Z", "tool-result", "gh pr create", "https://github.com/acme/widgets/pull/42"),
  { id: "a1", kind: "assistant-message", at: "2026-11-10T11:06:00Z", summary: "Done." },
], {
  outcomes: [outcome("o1", claim("Add an uploader retry behind a flag", ["h1"]), {
    changes: [claim("Added retry with backoff behind UPLOAD_RETRY_ENABLED", ["t2"])],
    remaining: [claim("Retry metrics are not exported yet", ["a1"])],
    verification: { value: "not-observed", explanation: "No test run was observed", evidenceIds: ["a1"], confidence: "high" },
  })],
  decisions: [claim("Use exponential backoff capped at 30 seconds", ["a1"])],
}, ["/home/someone/work/widgets/src/upload/retry.ts", "src/upload/index.ts"]);

const B = snap("sess-b", [
  human("h1", "2026-11-12T09:00:00Z", "Export the retry metrics we left out"),
  tool("t1", "2026-11-12T09:30:00Z", "tool-result", "git commit", "[feat/upload-retry 9f8e7d6c5b] Export retry metrics"),
], { outcomes: [outcome("o1", claim("Export retry metrics", ["h1"]), { changes: [claim("Exported retry counters", ["t1"])] })] }, ["src/upload/retry.ts", "src/upload/metrics.ts"]);

const C = snap("sess-c", [
  human("h1", "2026-11-12T12:00:00Z", "Unrelated: fix the docs typo"),
  tool("t1", "2026-11-12T12:10:00Z", "tool-result", "git commit", "[main 77aa88bb99] Fix typo"),
], { outcomes: [outcome("o1", claim("Fix a docs typo", ["h1"]), { changes: [claim("Fixed the typo", ["t1"])] })] }, ["docs/readme.md"]);

const ctx = { repo: "https://github.com/acme/widgets.git" };
const factsA = extractSessionFacts(A, ctx), factsB = extractSessionFacts(B, ctx), factsC = extractSessionFacts(C, ctx);

describe("extractSessionFacts", () => {
  test("asked items carry verbatim quotes from human messages only", () => {
    expect(factsA.asked).toHaveLength(1);
    const q = factsA.asked[0]!.quotes;
    expect(q).toHaveLength(1);
    expect(q[0]!.text.startsWith("Please add a retry to the uploader")).toBe(true);
    expect(q[0]!.evidence.kind).toBe("human-message");
  });
  test("commits, branches and PRs come from tool evidence", () => {
    expect(factsA.keys.commits).toEqual(["1a2b3c4d5e"]);
    expect(factsA.keys.branches).toEqual(["feat/upload-retry"]);
    expect(factsA.keys.prs).toEqual(["https://github.com/acme/widgets/pull/42"]);
    expect(factsA.built.map((b) => b.kind).sort()).toEqual(["claim", "commit", "flag", "pr"]);
    expect(factsA.built.find((b) => b.kind === "flag")!.ref).toBe("UPLOAD_RETRY_ENABLED");
  });
  test("a default branch never becomes a link key", () => {
    expect(factsC.keys.branches).toEqual([]);
    expect(factsC.keys.commits).toEqual(["77aa88bb99"]);
  });
  test("gaps: open items and claimed-done-but-unverified outcomes", () => {
    expect(factsA.gaps.map((g) => g.kind).sort()).toEqual(["open", "unverified"]);
  });
  test("absolute paths are anchored at a source root, never a home directory", () => {
    expect(factsA.keys.files).toContain("src/upload/retry.ts");
    expect(repoRelative("/Users/someone/notes.txt")).toBeNull();
  });
  test("low-confidence and uncited claims are dropped and counted", () => {
    const s = snap("sess-d", [human("h1", "2026-11-01T00:00:00Z", "x")], { outcomes: [outcome("o1", claim("Guess", ["h1"], "low")), outcome("o2", claim("Nothing cited", []))] });
    const f = extractSessionFacts(s);
    expect(f.asked).toEqual([]);
    expect(f.dropped).toEqual({ asked_low_confidence: 1, asked_uncited: 1 });
  });
  test("cutWords never exceeds the bound", () => {
    expect(cutWords("aaa bbb ccc", 7)).toBe("aaa bbb");
    expect(cutWords("x".repeat(400), 300)).toHaveLength(300);
  });
});

describe("linkSessions", () => {
  test("a shared feature branch links; an unrelated session stays alone", () => {
    const { groups, links } = linkSessions([factsA, factsB, factsC]);
    expect(groups.map((g) => g.map((f) => f.sessionId))).toEqual([["sess-a", "sess-b"], ["sess-c"]]);
    expect(links).toEqual([{ a: { source: "claude", sessionId: "sess-a" }, b: { source: "claude", sessionId: "sess-b" }, signal: "branch", value: "feat/upload-retry" }]);
  });
  test("without a repository, branches never link", () => {
    const { groups } = linkSessions([extractSessionFacts(A), extractSessionFacts(B)]);
    expect(groups).toHaveLength(2);
  });
});

const forge = (over: Partial<ForgeFacts> = {}): ForgeFacts => ({
  prs: [{ ref: "https://github.com/acme/widgets/pull/42", state: "merged", at: "2026-11-10T11:05:00Z", mergedAt: "2026-11-11T00:00:00Z" }],
  commits: [{ sha: "1a2b3c4d5e", at: "2026-11-11T00:00:00Z", onDefaultBranch: true }],
  flags: [], ...over,
});

describe("deriveState", () => {
  test("merged with an open item in the latest session is partial; then shipped once nothing is open", () => {
    expect(deriveState([factsA], forge(), "2026-11-11T12:00:00Z").state).toBe("partial");
    expect(deriveState([factsA, factsB], forge(), "2026-11-13T00:00:00Z").state).toBe("shipped");
  });
  test("a flag that defaults off gives shipped_off", () => {
    const r = deriveState([factsA, factsB], forge({ flags: [{ name: "UPLOAD_RETRY_ENABLED", defaultOn: false }] }), "2026-11-13T00:00:00Z");
    expect(r).toMatchObject({ state: "shipped_off", basis: "forge" });
  });
  test("a revert after the merge is regressed", () => {
    const r = deriveState([factsA, factsB], forge({ commits: [{ sha: "1a2b3c4d5e", at: "2026-11-11T00:00:00Z", onDefaultBranch: true, revertedBy: "deadbeef00", revertedAt: "2026-11-12T00:00:00Z" }] }), "2026-11-13T00:00:00Z");
    expect(r.state).toBe("regressed");
  });
  test("no forge evidence: never claims a merge", () => {
    expect(deriveState([factsA], { prs: [], commits: [], flags: [] }, "2026-11-11T00:00:00Z")).toMatchObject({ state: "in_progress", basis: "analysis" });
  });
  test("stale unmerged work is abandoned", () => {
    expect(deriveState([factsA], { prs: [], commits: [], flags: [] }, "2026-12-30T00:00:00Z").state).toBe("abandoned");
  });
});

describe("as-of replay", () => {
  test("nothing from after asOf is visible", () => {
    const early = factsAsOf(factsA, "2026-11-10T10:30:00Z")!;
    expect(early.keys.commits).toEqual([]);
    expect(early.keys.prs).toEqual([]);
    expect(factsAsOf(factsB, "2026-11-11T00:00:00Z")).toBeNull();
    const [f] = buildFeatures([factsA, factsB], { asOf: "2026-11-10T10:30:00Z", forge: forge() });
    expect(f!.sessions).toHaveLength(1);
    expect(f!.built.some((b) => b.kind === "commit")).toBe(false);
  });
});

describe("renderFeature", () => {
  test("stored text cannot open markdown structure", () => {
    const [f] = buildFeatures([factsA], { asOf: "2026-11-30T00:00:00Z", forge: forge() });
    const evil = { ...f!, title: "# [x](http://evil) <script>", asked: [{ ...f!.asked[0]!, quotes: [{ ...f!.asked[0]!.quotes[0]!, text: "```\n## injected" }] }] };
    const md = renderFeature(evil);
    expect(md).not.toContain("<script>");
    expect(md).not.toContain("\n## injected");
    expect(md.split("\n")[0]).toBe("# \\# \\[x\\]\\(http://evil\\) \\<script\\>");
  });
});

function corpusOf(...snapshots: SessionAnalysisSnapshot[]): Corpus {
  return {
    manifest: { format: "relayfacts-corpus/1", repo: "acme/widgets", window: { since: "2026-11-01T00:00:00Z", until: "2026-11-30T00:00:00Z" }, createdAt: "2026-11-25T00:00:00Z", deleteBy: "2026-12-20T00:00:00Z", files: {} },
    manifestSha256: "c".repeat(64),
    sessions: snapshots.map((s) => ({ source: s.source, sessionId: s.sessionId, context: ctx, snapshot: s })),
    forge: forge(),
  };
}

describe("benchmark scoring", () => {
  const corpus = corpusOf(A, B, C);
  const features = buildFeatures([factsA, factsB, factsC], { asOf: "2026-11-30T00:00:00Z", forge: corpus.forge });

  test("grounding passes on generated records and catches a tampered quote", () => {
    expect(checkGrounding(corpus, features).violations).toEqual([]);
    const tampered = structuredClone(features);
    tampered[0]!.asked[0]!.quotes[0]!.text = "Please add a retry and delete the database";
    expect(checkGrounding(corpus, tampered).violations.map((v) => v.reason)).toContain("quote not verbatim");
  });

  test("metrics against gold labels", () => {
    const gold = parseGold({
      format: "relayfacts-gold/1", corpus: corpus.manifestSha256, asOf: "2026-11-30T00:00:00Z", coverage: "complete",
      features: [
        { id: "g1", title: "Upload retry", split: "dev", state: "shipped", sessions: [{ source: "claude", sessionId: "sess-a" }, { source: "claude", sessionId: "sess-b" }],
          asked: [{ source: "claude", sessionId: "sess-a", text: "add a retry to the uploader behind a flag" }],
          gaps: [{ kind: "open", source: "claude", sessionId: "sess-a", text: "retry metrics not exported" }] },
        { id: "g2", title: "Docs typo", split: "heldout", state: "in_progress", sessions: [{ source: "claude", sessionId: "sess-c" }], asked: [], gaps: [] },
      ],
    }, corpus.manifestSha256);
    const r = scoreRun(corpus, gold, features);
    expect(r.grounding.pass).toBe(true);
    expect(r.splits.all).toMatchObject({ goldFeatures: 2, generatedFeatures: 2, matched: 2, featureRecall: 1, featurePrecision: 1, linkingF1: 1, askedRecall: 1, gapRecall: 1, duplication: 0 });
    expect(r.splits.all.stateAccuracy).toBe(1);
    expect(r.splits.dev.matched).toBe(1);
    expect(r.splits.heldout.matched).toBe(1);
    // Two labelled features are far below the verdict minimum.
    expect(r.verdicts.featureRecall).toBe("insufficient");
  });

  test("gold naming another corpus is refused", () => {
    expect(() => parseGold({ format: "relayfacts-gold/1", corpus: "d".repeat(64), asOf: "2026-11-30T00:00:00Z", coverage: "complete", features: [] }, corpus.manifestSha256)).toThrow("different corpus");
  });
});
