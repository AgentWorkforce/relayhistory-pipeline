/** Synthetic observations: descriptive candidates must never turn themselves into outcome labels. */
import { afterAll, describe, expect, test } from "bun:test";
import { rmSync } from "node:fs";
import { analyseCorpus } from "../patterns/analyse.ts";
import { buildCorpus } from "../patterns/corpus.ts";
import { reportMarkdown, reviewQueue } from "../patterns/report.ts";
import { loadSnapshot, parseSession, type Snapshot } from "../replay/snapshot.ts";
import { canonicalJson } from "../replay/util.ts";
import { session, tmp as makeTmp, writeSnapshot } from "./helpers.ts";

const ownDirs: string[] = [];
const tmp = () => { const dir = makeTmp(); ownDirs.push(dir); return dir; };
afterAll(() => { for (const dir of ownDirs) rmSync(dir, { recursive: true, force: true }); });

const now = new Date("2026-11-02T00:00:00Z");
const ts = (minute: string) => `2026-11-01T09:${minute}:00.000Z`;
const w = (id: string, minute: string, path = "src/shared.ts", branch: string | null = "feature") => ({ eventId: id, ts: ts(minute), path, kind: "write", branch });
const s = (id: string, extra = {}) => session(id, { repo: "fixture-repo", createdAt: ts("00"), lastActivityAt: ts("40"), ...extra });
function snapshot(rows: unknown[], digest = "a".repeat(64)): Snapshot {
  return { dir: "/unused-fixture", manifestSha256: digest, manifest: { format: "session-intel-snapshot/1", createdAt: now.toISOString(), deleteBy: "2026-11-10T00:00:00.000Z",
    window: { since: ts("00"), until: ts("30") }, files: { "sessions.jsonl": "b".repeat(64) } }, sessions: rows.map((r) => parseSession(r, "fixture")) };
}
const analyse = (rows: unknown[], opts = {}) => analyseCorpus(buildCorpus([snapshot(rows)], { now }), opts);

describe("pattern corpus", () => {
  test("verified fixtures load through the existing snapshot reader", async () => {
    const dir = tmp(); writeSnapshot(dir, [s("one")]);
    expect(buildCorpus([await loadSnapshot(dir, { now })], { now }).sessions).toHaveLength(1);
  });
  test("multi-input identity assertions do not become coverage proof; equal duplicates dedupe, conflicts fail", () => {
    const a = snapshot([s("one")]);
    expect(() => buildCorpus([a, a], { now })).toThrow("identity-space");
    expect(() => buildCorpus([a, a], { now, sameIdentitySpace: true })).toThrow("repeated input");
    const c = buildCorpus([a, snapshot([s("one")], "c".repeat(64))], { now, sameIdentitySpace: true });
    expect([c.sessions.length, c.duplicateSessions, c.identity]).toEqual([1, 1, "operator_asserted_shared_namespace"]);
    expect(analyseCorpus(c).coverage.workspaceCompleteness).toBe("not_established");
    expect(() => buildCorpus([a, snapshot([s("one", { writes: [w("x", "01")] })], "c".repeat(64))], { now, sameIdentitySpace: true })).toThrow("conflicting duplicate");
  });
  test("tenant/workspace, expiry and window mismatch fail closed without exposing identifiers", () => {
    expect(() => buildCorpus([snapshot([s("a"), s("b", { workspaceId: "other-fixture" })])], { now })).toThrow("one tenant");
    expect(() => buildCorpus([snapshot([])], { now: new Date("2026-11-10") })).toThrow("deleteBy");
    const a = snapshot([]), b = snapshot([], "c".repeat(64)); b.manifest.window.until = ts("29");
    expect(() => buildCorpus([a, b], { sameIdentitySpace: true, now })).toThrow("same window");
  });
});

describe("descriptive patterns", () => {
  test("evidence stays bounded, parent/branch metadata is descriptive, text is not copied", () => {
    const a = s("a", { writes: [w("a1", "01"), w("a2", "02")], parentSessionIds: ["fixture-parent"],
      title: "PRIVATE TITLE", shortSummary: "PRIVATE SUMMARY", claims: [{ at: ts("01"), kind: "intent", text: "PRIVATE CLAIM", confidence: 1, evidenceIds: ["a1"] }] });
    const b = s("b", { harness: "claude", writes: [w("b1", "01"), w("b2", "03", "src/shared.ts", "other")], parentSessionIds: ["fixture-parent"] });
    const r = analyse([a, b]);
    expect(r.candidates).toHaveLength(1);
    const c = r.candidates[0]!;
    expect(c).toMatchObject({ review: "unreviewed", eventPairs: 4, sharedFiles: 1, parentLink: "shared_parent", branchRelation: "mixed", closestGapMs: 0 });
    expect(c.evidence).toHaveLength(3);
    expect(c.evidence[0]!.a.eventId).toBe("a1");
    expect(c.firstObservedAt).toBe(ts("01"));
    for (const text of ["PRIVATE TITLE", "PRIVATE SUMMARY", "PRIVATE CLAIM"]) expect(JSON.stringify([r, reviewQueue(r), reportMarkdown(r)])).not.toContain(text);
    expect(reviewQueue(r).cases[0]!.verdict).toBeNull();
    expect(r.coverage.quality).toBe("not_evaluated");
  });
  test("same paths in different repositories and unscoped sessions never become pairs", () => {
    const r = analyse([s("a", { writes: [w("a", "01")] }), s("b", { repo: "another", writes: [w("b", "01")] }), s("c", { repo: null, projectId: null, writes: [w("c", "01")] })]);
    expect(r.candidates).toEqual([]); expect(r.coverage.sessionsWithoutScope).toBe(1);
    expect(r.profile.all.writes).toBe(3);
  });
  test("project fallback, half-open windows, exact paths, duplicate writes and temporal bounds", () => {
    const r = analyse([s("a", { repo: null, writes: [w("a", "00"), w("a", "00"), w("end", "30")] }),
      s("b", { repo: null, writes: [w("b", "01"), w("case", "01", "src/Shared.ts")] })], { nearbyMs: 60_000 });
    expect(r.candidates).toHaveLength(1); expect(r.candidates[0]!.scope.kind).toBe("project");
    expect(r.candidates[0]!.eventPairs).toBe(1);
    expect(r.coverage).toMatchObject({ duplicateWrites: 1, factsOutsideWindow: 1 });
    expect(analyse([s("a", { writes: [w("a", "00")] }), s("b", { writes: [w("b", "02")] })], { nearbyMs: 60_000 }).candidates).toEqual([]);
  });
  test("input and event order do not change reports; display caps do not rename candidates", () => {
    const a = s("a", { writes: [w("a2", "03"), w("a1", "01")] }), b = s("b", { writes: [w("b", "02")] });
    const r = analyse([a, b]); a.writes.reverse();
    expect(canonicalJson(analyse([b, a]))).toBe(canonicalJson(r));
    expect(analyse([a, b], { maxCandidates: 1 }).candidates[0]!.id).toBe(r.candidates[0]!.id);
    expect(() => analyse([s("inconsistent", { writes: [w("one", "01"), w("one", "01", "src/shared.ts", "contradictory")] })])).toThrow("conflicting duplicate write");
  });
  test("comparison caps censor explicitly and exact exhausted caps do not claim censoring", () => {
    const rows = ["a", "b", "c"].map((id) => s(id, { writes: [w(id, "01")] }));
    const r = analyse(rows, { maxComparisons: 1 });
    expect(r.coverage).toMatchObject({ scanComplete: false, comparisons: 1, reasons: ["comparison_budget"] });
    expect(analyse(rows.slice(0, 2), { maxComparisons: 1 }).coverage.scanComplete).toBe(true);
    expect(() => analyse(rows, { nearbyMs: -1 })).toThrow("integer bounds");
  });
  test("display omission is distinct from a censored scan, and pair storage caps stay explicit", () => {
    const rows = ["a", "b", "c"].map((id) => s(id, { writes: [w(id, "01")] }));
    const r = analyse(rows, { maxCandidates: 1 });
    expect(r.candidates).toHaveLength(1);
    expect(r.coverage).toMatchObject({ scanComplete: true, candidatePairsRetained: 3, candidatesOmittedFromReport: 2 });
    const many = analyse(Array.from({ length: 143 }, (_, i) => s(`session-${i}`, { writes: [w(`event-${i}`, "01")] })));
    expect(many.coverage).toMatchObject({ scanComplete: false, candidatePairsRetained: 10_000, reasons: ["candidate_pair_budget"] });
    expect(many.activityGroups[0]!.members).toHaveLength(20);
    expect(many.activityGroups[0]!.membersOmitted).toBeGreaterThan(0);
  });
  test("groups are transitive activity components, not all-to-all task claims", () => {
    const r = analyse([s("a", { writes: [w("a", "01", "src/a.ts")] }),
      s("b", { writes: [w("b1", "02", "src/a.ts"), w("b2", "03", "src/b.ts")] }), s("c", { writes: [w("c", "04", "src/b.ts")] })]);
    expect(r.candidates).toHaveLength(2); expect(r.activityGroups[0]).toMatchObject({ sessions: 3, candidateEdges: 2 });
    expect(r.limitations.some((l) => l.includes("transitive"))).toBe(true);
  });
  test("span endpoints do not count adjacent sessions as simultaneous; no evidence is not success", () => {
    const r = analyse([s("a", { lastActivityAt: ts("10") }), s("b", { createdAt: ts("10"), lastActivityAt: ts("20") }), s("point", { createdAt: ts("10"), lastActivityAt: ts("10") })]);
    expect(r.profile.peakSessionSpans).toBe(1); expect(r.profile.zeroLengthSpans).toBe(1);
    expect(r.profile.all.sessionsWithWrites).toBe(0);
    expect(r.limitations.some((l) => l.includes("No outcome"))).toBe(true);
    expect(r.limitations.some((l) => l.includes("No claim"))).toBe(true);
  });
  test("commit attempts with unknown keys are not counted as SHA-shaped evidence", () => {
    const r = analyse([s("a", { commits: [{ sha: "unknown", ts: ts("01"), eventId: "attempt" }, { sha: "abcdef12", ts: ts("02"), eventId: "key" }] })]);
    expect(r.profile.all).toMatchObject({ commits: 2, shaShapedCommits: 1 });
    expect(r.limitations.some((l) => l.includes("not independent proof"))).toBe(true);
  });
});

test("busy same-session activity cannot consume the cross-session budget or hide later files", () => {
  const r = analyse([s("busy", { writes: Array.from({ length: 700 }, (_, i) => w(`busy-${i}`, "01", "src/a.ts")) }),
    s("later-a", { writes: [w("later-a", "02", "src/z.ts")] }), s("later-b", { writes: [w("later-b", "03", "src/z.ts")] })], { maxComparisons: 1 });
  expect(r.coverage).toMatchObject({ comparisons: 1, scanComplete: true });
  expect(r.candidates).toHaveLength(1);
  expect(r.candidates[0]!.participants.map((p) => p.sessionId)).toEqual(["later-a", "later-b"]);
});

test("window eviction keeps exact nearby boundaries and direct links differ from siblings", () => {
  const r = analyse([s("a", { writes: [w("old", "00"), w("near", "10")] }),
    s("b", { writes: [w("b", "11")], parentSessionIds: ["a"] })], { nearbyMs: 60_000 });
  expect(r.coverage.comparisons).toBe(1);
  expect(r.candidates[0]).toMatchObject({ parentLink: "direct", eventPairs: 1 });
});

test("all omitted display rows are counted and outside-window sessions do not dilute coverage", () => {
  const rows = Array.from({ length: 1001 }, (_, i) => s(`repo-${i}`, { repo: `fixture-${i}`, writes: [w(`write-${i}`, "01")] }));
  rows.push(s("old", { createdAt: "2026-10-31T09:00:00.000Z", lastActivityAt: "2026-10-31T09:01:00.000Z" }));
  const r = analyse(rows);
  expect(r.coverage).toMatchObject({ repositoriesTotal: 1001, repositoriesOmitted: 1, filesTotal: 1001, filesOmitted: 951, sessionsOutsideWindow: 1 });
  expect(r.profile.all.sessions).toBe(1001);
  expect(r.input.sessions).toBe(1002);
  expect(r.profile.byRepository).toHaveLength(1000);
  const groups = analyse(Array.from({ length: 102 }, (_, i) => s(`group-${i}`, { writes: [w(`group-write-${i}`, "01", `src/pair-${Math.floor(i / 2)}.ts`)] })));
  expect(groups.coverage).toMatchObject({ groupsTotal: 51, groupsOmitted: 1 });
  expect(groups.activityGroups).toHaveLength(50);
});
