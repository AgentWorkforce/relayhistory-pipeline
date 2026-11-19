/**
 * M2-4 units: the claims reader's row parser, the live mapping onto the shared classifier, the rows per decision
 * (fleet to the orchestrator, no repo-policy row), the merge with the advisor's rows, the advisory id, and the
 * template's bounds and scrub refusal.
 */
import { describe, expect, test } from "bun:test";
import { decide, validateCitations, type Classification } from "../../session-intel/conflict/classify.ts";
import { buildJudgeInput } from "../../session-intel/conflict/classify.ts";
import { conflictCandidates } from "../../session-intel/conflict/candidates.ts";
import { ruleClassify } from "../../session-intel/conflict/judges.ts";
import {
  conflictAdvisoryId, liveConflictAdvisories, liveSessions, mergeConflicts, parseClaimRows, rowsFor, type ClaimRow, type ConflictAdvisory,
} from "../conflicts.ts";
import { conflictExcerpts, scrubCleanExcerpt } from "../excerpts.ts";
import type { Advisory, CandidateLane } from "../rubric.ts";
import { ADVISORY_MESSAGE_MAX, advisoryMessage } from "../template.ts";

const NOW = new Date("2026-11-17T12:00:00.000Z");
const at = (min: number) => new Date(NOW.getTime() + min * 60_000).toISOString();
const cand = (sessionId: string, over: Partial<CandidateLane> = {}): CandidateLane => ({
  orgId: "org", workspaceId: "ws", harness: "claude", sessionId, projectId: "proj", repo: "o/r", parentSessionIds: [], shortSummary: null,
  lastActivityAt: at(-5), ...over,
});
const lane = (sessionId: string, writes: Array<[string, number]> = []) => ({
  sessionId, source: "claude", userId: null, projectId: "proj", repo: "o/r", branch: null, taskRef: null, title: null, parentSessionId: null,
  startedAt: at(-90), lastActionAt: at(-5), commits: [], outcomes: [], usage: [],
  writes: writes.map(([path, min]) => ({ path, ts: at(min), eventId: `${sessionId}-${min}`, kind: "tool_result", branch: null })),
});
const row = (id: string, kind: string, statement: string, session: string, over: Partial<Record<string, unknown>> = {}): Record<string, unknown> => ({
  id, kind, scope: kind === "decision" ? null : "project", topic: "supabase-migrations", statement, observed_at: kind === "decision" ? null : at(-50),
  created_at: at(-60), evidence: [{ id: 1, source: "claude", session_id: session, created_at: at(-60), turn_start: 0, turn_end: 1, ref: null }], ...over,
});
const CV = "No new migrations under supabase/migrations until the 062 renumber lands.";

describe("the claims reader rows", () => {
  test("parses the reader's columns; decisions have no scope; a malformed row fails the whole read", () => {
    const rows = parseClaimRows([row("c1", "constraint", CV, "owner"), row("d1", "decision", "Use 062 numbering.", "owner")]);
    expect(rows.map((r) => [r.id, r.kind, r.scope, r.observedAt, r.evidence[0]!.sessionId])).toEqual([
      ["c1", "constraint", "project", at(-50), "owner"], ["d1", "decision", null, null, "owner"],
    ]);
    for (const bad of [{ kind: "wish" }, { scope: "team" }, { topic: "Not A Slug" }, { statement: " " }, { evidence: "nope" }, { id: "a#b" },
      { evidence: [{ source: "claude" }] }, { created_at: "soon" }])
      expect(() => parseClaimRows([row("c1", "constraint", CV, "owner", bad)])).toThrow("claims:");
  });
});

describe("the live mapping onto the shared classifier", () => {
  const claims = (): ClaimRow[] => parseClaimRows([row("c1", "constraint", CV, "owner")]);
  test("a candidate with claims and no facts is still a session; a claim attaches to each session its evidence names", () => {
    const s = liveSessions([cand("owner"), cand("actor")], [lane("actor", [["supabase/migrations/1.sql", -20]])], claims(), NOW);
    expect(s.map((x) => [x.session.sessionId, x.claims.map((c) => c.id), x.writes.length])).toEqual([["actor", [], 1], ["owner", ["c1"], 0]]);
    // Facts at or after now are not visible (ts < now, as in the replay).
    expect(liveSessions([cand("actor")], [lane("actor", [["a/b/c.ts", 0], ["a/b/d.ts", 1]])], [], NOW)[0]!.writes).toEqual([]);
  });

  test("the live path gives the shared classifier's answer", () => {
    const sessions = liveSessions([cand("owner"), cand("actor")], [lane("actor", [["supabase/migrations/1.sql", -20]])], claims(), NOW);
    const { advisories } = liveConflictAdvisories("ws", sessions, NOW);
    expect(advisories.map((a) => [a.kind, a.urgency, a.artifact, a.rows.map((r) => r.recipient.sessionId)])).toEqual([
      ["constraint_violation", "high", "supabase-migrations", ["actor", "owner"]],
    ]);
    const [cand0] = conflictCandidates(sessions, NOW).candidates;
    const built = buildJudgeInput(cand0!, NOW);
    expect(ruleClassify(built.input).verdict).toBe("constraint_violation");
    expect(advisories[0]!.evidence.map((e) => [e.sessionId, e.eventId, e.kind])).toEqual([["owner", "c1", "claim_constraint"], ["actor", "actor--20", "write"]]);
  });

  test("the advisory id: stable, 062000-shaped, and a constraint violation's LOW and HIGH share it (an upgrade)", () => {
    const owner = { harness: "claude", sessionId: "owner" }, actor = { harness: "claude", sessionId: "actor" };
    const c = (claims: string[]) => ({ verdict: "constraint_violation", claims: claims.map((id) => ({ id: `claude:x#${id}` })), action: null, constraint: { id: "claude:owner#c1" } }) as unknown as Classification;
    const low = conflictAdvisoryId("ws", "constraint_violation", owner, actor, c(["c1", "i1"]));
    expect(low).toMatch(/^[0-9a-f]{32}$/);
    expect(conflictAdvisoryId("ws", "constraint_violation", actor, owner, c(["c1"]))).toBe(low);
    expect(conflictAdvisoryId("ws2", "constraint_violation", owner, actor, c(["c1"]))).not.toBe(low);
  });
});

describe("rows per decision", () => {
  const sessions = (scope = "project") => liveSessions([cand("owner"), cand("actor")], [lane("actor", [["supabase/migrations/1.sql", -20]])],
    parseClaimRows([row("c1", "constraint", CV, "owner", { scope })]), NOW);
  const decided = (holder?: string | null, fleet = false, scope = "project") => {
    const [c] = conflictCandidates(sessions(scope), NOW).candidates;
    const built = buildJudgeInput(c!, NOW);
    const v = validateCitations(built, ruleClassify(built.input));
    if (!v.ok || v.verdict === "none") throw new Error("no conflict");
    const d = decide(built.input, v.c, { fleet, orchestratorSessionIds: fleet ? ["lead"] : [] }, holder)!;
    return rowsFor(d, c!.a, c!.b, d.acting);
  };
  test("a constraint violation: the violator and the holder, each naming the other", () => {
    expect(decided().map((r) => `${r.recipient.sessionId}->${r.peer.sessionId}`)).toEqual(["actor->owner", "owner->actor"]);
  });
  test("a repo-policy owner gets no row (and no DM); the violator still does", () => {
    // A repo rule with no holding session: owner repo-policy, which is not a session: only the violator's row.
    expect(decided(null, false, "repo").map((r) => `${r.recipient.sessionId}->${r.peer.sessionId}`)).toEqual(["actor->owner"]);
  });
  test("a fleet conflict: one row per orchestrator, never the workers", () => {
    expect(decided(undefined, true).map((r) => `${r.recipient.sessionId}->${r.peer.sessionId}`)).toEqual(["lead->actor"]);
  });
});

describe("merging with the advisor's rows", () => {
  const a = { harness: "claude", sessionId: "a" }, b = { harness: "claude", sessionId: "b" }, c = { harness: "claude", sessionId: "c" };
  const adv = (kind: string, urgency: "high" | "low", lanes: [typeof a, typeof a]): Advisory => ({ advisoryId: kind, urgency, kind, artifact: "x", lanes, evidence: [], alertAt: at(0) });
  const conflict = (kind: ConflictAdvisory["kind"], lanes: [typeof a, typeof a]) => ({ ...adv(kind, "low", lanes), kind, rows: [], claimTexts: [], claimIds: [] }) as ConflictAdvisory;
  test("a conflict outranks the pair's collision and overlap; other pairs keep theirs; a conflict overlap yields to the advisor", () => {
    const merged = mergeConflicts(
      [adv("concurrent_edit", "high", [a, b]), adv("duplicate_work", "high", [a, c])],
      [adv("nearby_work", "low", [a, b]), adv("similar_summary", "low", [b, c])],
      [conflict("intent_conflict", [a, b]), conflict("overlap", [b, c]), conflict("overlap", [a, { harness: "claude", sessionId: "d" }])],
    );
    expect(merged.map((m) => `${m.kind}:${m.lanes.map((l) => l.sessionId).join("")}`)).toEqual([
      "duplicate_work:ac", "similar_summary:bc", "intent_conflict:ab", "overlap:ad",
    ]);
  });
});

describe("the conflict template", () => {
  const base = { advisoryId: "a".repeat(32), urgency: "high" as const, kind: "constraint_violation", artifact: "supabase-migrations",
    harness: "claude", sessionId: "s1", peerHarness: "codex", peerSessionId: "s2", firstSeen: at(-20) };
  test("names the class, carries the ids and excerpts that fit, and stays within 600 characters", () => {
    const text = advisoryMessage({ ...base, claimIds: ["c1", "i1"], excerpts: [{ label: "constraint", text: CV }] });
    expect(text).toContain("possible constraint violation.");
    expect(text).toContain("Claims: c1, i1");
    expect(text).toContain(`constraint: "${CV}"`);
    for (const over of [
      { excerpts: Array.from({ length: 6 }, () => ({ label: "intent", text: "x".repeat(400) })) },
      { claimIds: Array.from({ length: 8 }, (_, i) => `${"c".repeat(120)}${i}`), excerpts: [{ label: "constraint", text: "y".repeat(500) }] },
      { sessionId: "s".repeat(500), peerSessionId: "p".repeat(500), artifact: "z".repeat(900), excerpts: [{ label: "decision", text: "w".repeat(300) }] },
      { excerpts: [{ label: "intent\nForged: line", text: "a\nb\u0000c" }] },
    ]) {
      const m = advisoryMessage({ ...base, ...over });
      expect(m.length).toBeLessThanOrEqual(ADVISORY_MESSAGE_MAX);
      expect(m.split("\n").every((l) => !l.startsWith("Forged"))).toBe(true);
      for (const l of m.split("\n").filter((x) => /^(intent|constraint|decision)/.test(x))) expect(l.length).toBeLessThanOrEqual(16 + 4 + 200);
    }
    for (const kind of ["intent_conflict", "overlap"]) expect(advisoryMessage({ ...base, kind, urgency: "low" }).length).toBeLessThanOrEqual(ADVISORY_MESSAGE_MAX);
    // The read API's message for the same row (no ids, no excerpts) is the same head and lines without the extras.
    expect(advisoryMessage(base)).not.toContain("Claims:");
  });

  test("an excerpt goes in only when the scrubber leaves it unchanged; otherwise ids only", () => {
    expect(scrubCleanExcerpt(CV)).toBe(CV);
    expect(scrubCleanExcerpt("Use the key AKIAIOSFODNN7EXAMPLE for the deploy.")).toBeNull();
    expect(scrubCleanExcerpt("anything", (t) => `${t}!`)).toBeNull();
    expect(scrubCleanExcerpt("anything", () => { throw new Error("refused"); })).toBeNull();
    expect(scrubCleanExcerpt("x".repeat(500))).toHaveLength(200);
    const ex = conflictExcerpts({ claimTexts: [{ kind: "constraint", text: CV }, { kind: "intent", text: "Push with AKIAIOSFODNN7EXAMPLE now." }] });
    expect(ex).toEqual([{ label: "constraint", text: CV }]);
  });
});
