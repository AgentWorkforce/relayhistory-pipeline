/**
 * M2-1 and M2-2 (docs/session-intel-conflicts.md): the candidate generator, the vetoes, citation validation, the
 * rule judge, the claim fields in the snapshot, and the claim leak test.
 */
import { describe, expect, test } from "bun:test";
import { conflictCandidates, fleetOf, MAX_PAIRS_PER_WORKSPACE } from "../conflict/candidates.ts";
import {
  buildJudgeInput, citable, constraintOwner, decide, REPO_POLICY_OWNER, validateCitations, type BuiltInput, type JudgeOutput,
} from "../conflict/classify.ts";
import { oppositeOf, parseConstraint, pathMatches, ruleClassify, segmentMatches } from "../conflict/judges.ts";
import { claimPathTopics, claimTopics, pathTopic, writeTopics } from "../conflict/topics.ts";
import { parseSession, SnapshotError } from "../replay/snapshot.ts";
import { asOfSession, type AsOfSession } from "../replay/views.ts";
import { at, session } from "./helpers.ts";

type Over = Record<string, unknown>;
const claim = (id: string, hm: string, kind: string, text: string, more: Over = {}) => ({ id, at: at(hm), kind, text, confidence: 0.9, evidenceIds: [`ev-${id}`], ...more });
const write = (path: string, hm: string, eventId: string) => ({ path, ts: at(hm), eventId, kind: "tool_result", branch: null });
const snap = (id: string, over: Over = {}) => parseSession(session(id, { usage: [{ ts: at("09:01"), eventId: `u-${id}`, tokens: 1, costUsdMicros: 1 }], ...over }), id);
const view = (id: string, now: string, over: Over = {}): AsOfSession => asOfSession(snap(id, over), new Date(at(now)))!;
const NOW = "10:00";
const cands = (views: AsOfSession[], now = NOW, opts = {}) => conflictCandidates(views, new Date(at(now)), opts);

const CONSTRAINT = "No new migrations under supabase/migrations until the 062 renumber lands.";
const owner = (scope: string, more: Over = {}) => ({ claims: [claim("k1", "09:10", "constraint", CONSTRAINT, { scope })], ...more });
const actor = (hm = "09:40", more: Over = {}) => ({ writes: [write("supabase/migrations/062140_x.sql", hm, "w1")], ...more });

/** The built input for the one candidate of two sessions as of `now`. */
function built(a: Over, b: Over, now = NOW, ids: [string, string] = ["s-a", "s-b"]): BuiltInput {
  const r = cands([view(ids[0], now, a), view(ids[1], now, b)], now);
  expect(r.candidates).toHaveLength(1);
  return buildJudgeInput(r.candidates[0]!, new Date(at(now)));
}

describe("topics", () => {
  test("decisionTopic-style slugs: first two directories, file names never", () => {
    expect(pathTopic("supabase/migrations/0001.sql")).toBe("supabase-migrations");
    expect(pathTopic("src/session-read-api/routes/x.ts")).toBe("src-session-read-api");
    expect(pathTopic("docs/guide.md")).toBe("docs");
    expect(pathTopic("README.md")).toBeNull();
    expect(pathTopic("/home/someone/repo/src/api/x.ts")).toBe("src-api");
    expect(pathTopic("../outside/x.ts")).toBeNull();
    expect(claimPathTopics("touch src/api/routes.ts and supabase/migrations")).toEqual(["src-api", "supabase-migrations"]);
    expect(claimPathTopics("no */migrations/* at all")).toEqual([]);
    // A word names a topic the other side wrote, through a non-generic directory segment, plural-tolerant.
    expect(claimTopics("no new migration until Monday", writeTopics([{ path: "db/migrations/1.sql" }]))).toEqual(["db-migrations"]);
    expect(claimTopics("refactor the src tree", writeTopics([{ path: "src/x/1.ts" }]))).toEqual([]);
  });
});

describe("M2-1 candidates", () => {
  test("same project, both active within 2h, a live claim, and a shared topic", () => {
    const r = cands([view("s-a", NOW, owner("project")), view("s-b", NOW, actor())]);
    expect(r.candidates.map((c) => ({ pair: c.pair, fleet: c.fleet, topics: c.topics }))).toEqual([{ pair: ["claude:s-a", "claude:s-b"], fleet: false, topics: ["supabase-migrations"] }]);
  });

  const table: Array<[string, AsOfSession[]]> = [
    ["another project", [view("s-a", NOW, owner("project")), view("s-b", NOW, actor("09:40", { projectId: "proj-y" }))]],
    ["another workspace", [view("s-a", NOW, owner("project")), view("s-b", NOW, actor("09:40", { workspaceId: "ws-other" }))]],
    ["no project", [view("s-a", NOW, owner("project", { projectId: null })), view("s-b", NOW, { ...actor(), projectId: null })]],
    ["no claim on either side", [view("s-a", NOW, actor("09:30")), view("s-b", NOW, actor())]],
    ["no shared topic", [view("s-a", NOW, owner("project")), view("s-b", NOW, { writes: [write("src/other/x.ts", "09:40", "w1")] })]],
    ["the only claim is superseded", [view("s-a", NOW, { claims: [claim("k1", "09:10", "constraint", CONSTRAINT, { scope: "project", supersededAt: at("09:20") })] }), view("s-b", NOW, actor())]],
    ["the only claim is in the future", [view("s-a", NOW, { claims: [claim("k1", "10:30", "constraint", CONSTRAINT, { scope: "project" })], lastActivityAt: at("12:00") }), view("s-b", NOW, actor())]],
    ["one side idle for more than 2h", [view("s-a", "11:30", owner("project", { lastActivityAt: at("13:00") })), view("s-b", "11:30", actor("11:00", { lastActivityAt: at("13:00") }))]],
  ];
  for (const [name, views] of table) test(`no candidate: ${name}`, () => {
    const now = name.includes("idle") ? "11:30" : NOW;
    expect(cands(views, now).candidates).toEqual([]);
  });

  test("self pairs never form, even from a repeated view", () => {
    const v = view("s-a", NOW, { ...owner("project"), ...actor() });
    expect(cands([v, v]).candidates).toEqual([]);
  });

  test("a planned fleet is routed with fleet: true and its orchestrator, not dropped", () => {
    const lead = view("lead", NOW);
    const a = view("s-a", NOW, owner("project", { parentSessionIds: ["lead"] }));
    const b = view("s-b", NOW, actor("09:40", { parentSessionIds: ["lead"] }));
    const r = cands([lead, a, b]);
    expect(r.candidates.map((c) => [c.pair, c.fleet, c.orchestratorSessionIds])).toEqual([[["claude:s-a", "claude:s-b"], true, ["lead"]]]);
    expect(fleetOf(a.session, lead.session)).toEqual({ fleet: true, orchestratorSessionIds: ["lead"] });
    expect(fleetOf(a.session, { ...lead.session, harness: "codex" }).fleet).toBe(false);
  });

  test("at most 50 pairs per workspace per tick, deterministic, most recently active first", () => {
    const views = Array.from({ length: 12 }, (_, i) => view(`s-${String(i).padStart(2, "0")}`, NOW, {
      ...owner("project"), ...actor(`09:${String(30 + i).padStart(2, "0")}`),
    }));
    const r = cands(views);
    expect(r.candidates).toHaveLength(MAX_PAIRS_PER_WORKSPACE);
    expect(r.truncated).toBe(66 - 50);
    const again = cands([...views].reverse());
    expect(again.candidates.map((c) => c.pair)).toEqual(r.candidates.map((c) => c.pair));
    // The newest pair (s-11 with s-10) leads; the oldest pairs are the ones cut.
    expect(r.candidates[0]!.pair).toEqual(["claude:s-10", "claude:s-11"]);
    expect(r.candidates.some((c) => c.pair[0] === "claude:s-00" && c.pair[1] === "claude:s-01")).toBe(false);
    // The cap is per workspace: a second workspace gets its own 50.
    const other = views.map((v) => ({ ...v, session: { ...v.session, workspaceId: "ws-2" } }));
    expect(cands([...views, ...other]).candidates).toHaveLength(100);
    expect(cands(views, NOW, { maxPairsPerWorkspace: 3 }).candidates).toHaveLength(3);
  });
});

describe("M2-2 vetoes (deterministic, before and after the judge)", () => {
  test("pre-judge: a superseded claim and a session-scope constraint never reach the judge", () => {
    const b = built({ claims: [
      claim("k1", "09:10", "constraint", CONSTRAINT, { scope: "session" }),
      claim("d1", "09:05", "decision", "Number migrations in supabase/migrations from 062140.", { supersededAt: at("09:20") }),
      claim("d2", "09:06", "decision", "Write the docs in docs/guides.", {}),
    ] }, actor("09:40", { claims: [claim("i1", "09:35", "intent", "Add a migration in supabase/migrations.")] }));
    expect(b.input.a.claims).toEqual([]);
    expect([...b.excluded]).toEqual([["claude:s-a#k1", "session_scope"], ["claude:s-a#d1", "superseded"], ["claude:s-a#d2", "off_topic"]]);
    expect(citable(b.input)).toBe(false);
  });

  test("a supersession still in the future is not visible: the claim is live, and the view hides supersededAt", () => {
    const v = view("s-a", NOW, { claims: [claim("d1", "09:05", "decision", "x", { supersededAt: at("10:30") })] });
    expect(v.claims[0]).not.toHaveProperty("supersededAt");
    const later = view("s-a", "10:32", { claims: [claim("d1", "09:05", "decision", "x", { supersededAt: at("10:30") })] });
    expect(later.claims[0]!.supersededAt).toBe(at("10:30"));
  });

  const b = built(owner("project", { claims: [
    claim("k1", "09:10", "constraint", CONSTRAINT, { scope: "project" }),
    claim("k2", "09:11", "constraint", CONSTRAINT, { scope: "session" }),
    claim("d0", "09:05", "decision", "Migrations in supabase/migrations are numbered by the lander.", { supersededAt: at("09:20") }),
    claim("d1", "09:12", "decision", "Migrations in supabase/migrations use the 062 range."),
  ], writes: [write("supabase/migrations/062100_old.sql", "09:15", "wa")] }),
  actor("09:40", { writes: [write("supabase/migrations/062140_x.sql", "09:05", "w0"), write("supabase/migrations/062141_y.sql", "09:40", "w1")],
    claims: [claim("i1", "09:35", "intent", "Add a migration in supabase/migrations."), claim("i2", "09:36", "intent", "Write supabase/migrations/062142.")] }));
  const A = (id: string) => `claude:s-a#${id}`, B = (id: string) => `claude:s-b#${id}`;
  const act = (id: string, side = "s-b") => `claude:${side}@${id}`;
  const cases: Array<[string, JudgeOutput, string]> = [
    ["a judge none is none", { verdict: "none", claimIds: [] }, "none"],
    ["a verdict outside the closed set", { verdict: "collision", claimIds: [A("k1"), B("i1")] }, "invalid_verdict"],
    ["an invalid reply", { verdict: "constraint_violation", claimIds: [A("k1")], actionId: act("w1"), invalid: "not_json" }, "invalid_reply"],
    ["a superseded claim cited", { verdict: "intent_conflict", claimIds: [A("d0"), B("i1")] }, "superseded"],
    ["a session-scope constraint cited", { verdict: "constraint_violation", claimIds: [A("k2")], actionId: act("w1") }, "session_scope"],
    ["an id not in the input", { verdict: "overlap", claimIds: [A("nope"), B("i1")] }, "unknown_id"],
    ["an action id not in the input", { verdict: "constraint_violation", claimIds: [A("k1")], actionId: act("zz") }, "unknown_id"],
    ["a repeated id", { verdict: "overlap", claimIds: [B("i1"), B("i1")] }, "duplicate_id"],
    ["claim ids not strings", { verdict: "overlap", claimIds: [1 as never, B("i1")] }, "bad_ids"],
    ["self-conflict: both claims on one side", { verdict: "intent_conflict", claimIds: [B("i1"), B("i2")] }, "self_conflict"],
    ["self-conflict: a constraint and its holder's own write", { verdict: "constraint_violation", claimIds: [A("k1")], actionId: act("wa", "s-a") }, "self_conflict"],
    ["a constraint alone", { verdict: "constraint_violation", claimIds: [A("k1"), A("d1")] }, "self_conflict"],
    ["a violation with no constraint", { verdict: "constraint_violation", claimIds: [A("d1"), B("i1")] }, "bad_citation"],
    ["an action before the constraint existed", { verdict: "constraint_violation", claimIds: [A("k1")], actionId: act("w0") }, "action_before_constraint"],
    ["an intent conflict citing a constraint", { verdict: "intent_conflict", claimIds: [A("k1"), B("i1")] }, "bad_citation"],
    ["an intent conflict citing one claim", { verdict: "intent_conflict", claimIds: [A("d1")], actionId: act("w1") }, "bad_citation"],
    ["an intent conflict with the non-acting side's action", { verdict: "intent_conflict", claimIds: [A("d1"), B("i1")], actionId: act("wa", "s-a") }, "bad_citation"],
    ["an overlap with an action", { verdict: "overlap", claimIds: [A("d1"), B("i1")], actionId: act("w1") }, "bad_citation"],
    ["valid: constraint plus action", { verdict: "constraint_violation", claimIds: [A("k1")], actionId: act("w1") }, "constraint_violation"],
    ["valid: constraint plus claim", { verdict: "constraint_violation", claimIds: [A("k1"), B("i1")] }, "constraint_violation"],
    ["valid: intent conflict", { verdict: "intent_conflict", claimIds: [A("d1"), B("i1")] }, "intent_conflict"],
    ["valid: overlap", { verdict: "overlap", claimIds: [A("d1"), B("i1")] }, "overlap"],
  ];
  for (const [name, out, want] of cases) test(`citations: ${name} -> ${want}`, () => {
    const v = validateCitations(b, out);
    expect(v.ok ? v.verdict : v.reason).toBe(want);
  });

  test("urgency: HIGH needs an action in the topic by the acting side; an intent alone is LOW", () => {
    const ok = (out: JudgeOutput) => { const v = validateCitations(b, out); if (!v.ok || v.verdict === "none") throw new Error("invalid"); return v.c; };
    const solo = { fleet: false, orchestratorSessionIds: [] };
    expect(decide(b.input, ok({ verdict: "constraint_violation", claimIds: [A("k1"), B("i1")] }), solo)).toMatchObject({ urgency: "high", action: { eventId: "w1" } });
    const noWrites = { ...b.input, b: { ...b.input.b, actions: [] } };
    expect(decide(noWrites, ok({ verdict: "constraint_violation", claimIds: [A("k1"), B("i1")] }), solo)).toMatchObject({ urgency: "low", action: null, delivery: { dm: [] } });
    // An action before the constraint does not make it HIGH.
    const early = { ...b.input, b: { ...b.input.b, actions: b.input.b.actions.filter((x) => x.eventId === "w0") } };
    expect(decide(early, ok({ verdict: "constraint_violation", claimIds: [A("k1"), B("i1")] }), solo)!.urgency).toBe("low");
    expect(decide(b.input, ok({ verdict: "intent_conflict", claimIds: [A("d1"), B("i1")] }), solo)).toMatchObject({ urgency: "high", acting: "b" });
    expect(decide(noWrites, ok({ verdict: "intent_conflict", claimIds: [A("d1"), B("i1")] }), solo)!.urgency).toBe("low");
    expect(decide(b.input, ok({ verdict: "overlap", claimIds: [A("d1"), B("i1")] }), solo)).toMatchObject({ urgency: "low", delivery: { dm: [] } });
  });

  test("fleet: a conflict is LOW to the orchestrator only; an overlap is SILENT", () => {
    const fleet = { fleet: true, orchestratorSessionIds: ["lead"] };
    const v = validateCitations(b, { verdict: "constraint_violation", claimIds: [A("k1")], actionId: act("w1") });
    if (!v.ok || v.verdict === "none") throw new Error("invalid");
    expect(decide(b.input, v.c, fleet)).toMatchObject({ urgency: "low", delivery: { recipient: "orchestrator", dm: [], pull: [], orchestratorSessionIds: ["lead"] } });
    const o = validateCitations(b, { verdict: "overlap", claimIds: [A("d1"), B("i1")] });
    if (!o.ok || o.verdict === "none") throw new Error("invalid");
    expect(decide(b.input, o.c, fleet)).toBeNull();
  });

  test("delivery: a violation goes to the violator and the constraint's holder; a repo rule with no holder is repo-policy, never DMed", () => {
    const v = validateCitations(b, { verdict: "constraint_violation", claimIds: [A("k1")], actionId: act("w1") });
    if (!v.ok || v.verdict === "none") throw new Error("invalid");
    const solo = { fleet: false, orchestratorSessionIds: [] };
    expect(decide(b.input, v.c, solo)!.delivery).toEqual({ recipient: "sessions", dm: ["claude:s-b", "claude:s-a"], pull: ["claude:s-b", "claude:s-a"], violator: "claude:s-b", owner: "claude:s-a" });
    const repo = { ...v.c, constraint: { ...v.c.constraint!, scope: "repo" as const } };
    expect(decide(b.input, repo, solo, null)!.delivery).toEqual({ recipient: "sessions", dm: ["claude:s-b"], pull: ["claude:s-b"], violator: "claude:s-b", owner: REPO_POLICY_OWNER });
    expect(constraintOwner("repo", null)).toEqual({ owner: "repo-policy", dm: undefined });
    expect(constraintOwner("project", null)).toBeNull();
    expect(decide(b.input, v.c, solo, null)!.delivery).not.toHaveProperty("owner");
  });
});

describe("which actions may make an alert HIGH", () => {
  const commit = (eventId: string, hm: string) => ({ sha: `sha-${eventId}`, ts: at(hm), eventId });
  test("a commit or outcome counts only in an in-topic write's event; an off-topic commit is never an action", () => {
    const b = built({ claims: [claim("k1", "09:10", "constraint", CONSTRAINT, { scope: "project" })] },
      actor("09:40", { commits: [commit("w1", "09:40"), commit("other", "09:45")], outcomes: [{ key: "k", ts: at("09:46"), eventId: "other2", reverted: false }] }));
    // The commit in the write's own event is the same action (one event id); the other commit and outcome are not actions.
    expect(b.input.b.actions.map((x) => [x.kind, x.eventId, x.topic])).toEqual([["write", "w1", "supabase-migrations"]]);
    // A side whose only in-topic action is elsewhere: its unrelated commit does not make a violation HIGH.
    const off = built({ claims: [claim("k1", "09:10", "constraint", CONSTRAINT, { scope: "project" })] },
      { writes: [write("src/elsewhere/a.ts", "09:20", "wx")], commits: [commit("cx", "09:50")],
        claims: [claim("i1", "09:30", "intent", "Add a migration in supabase/migrations.")] });
    expect(off.input.b.actions).toEqual([]);
    const v = validateCitations(off, ruleClassify(off.input));
    if (!v.ok || v.verdict === "none") throw new Error("expected a violation");
    expect(decide(off.input, v.c, { fleet: false, orchestratorSessionIds: [] })).toMatchObject({ urgency: "low", action: null });
    expect(validateCitations(off, { verdict: "constraint_violation", claimIds: ["claude:s-a#k1"], actionId: "claude:s-b@cx" })).toEqual({ ok: false, reason: "unknown_id" });
  });

  test("an action earlier than the acting claim never makes an intent conflict HIGH; the earliest later one is chosen", () => {
    const decision = { claims: [claim("d1", "09:15", "decision", "Advisories return 501 when the flag is off in src/advisories.")] };
    const intent = (writes: ReturnType<typeof write>[]) => ({ writes, claims: [claim("i1", "09:40", "intent", "Return 404 when the flag is off in src/advisories.")] });
    const early = built(decision, intent([write("src/advisories/a.ts", "09:20", "w-early")]));
    const v = validateCitations(early, ruleClassify(early.input));
    if (!v.ok || v.verdict === "none") throw new Error("expected a conflict");
    expect(v.verdict).toBe("intent_conflict");
    expect(decide(early.input, v.c, { fleet: false, orchestratorSessionIds: [] })).toMatchObject({ urgency: "low", action: null });
    expect(validateCitations(early, { verdict: "intent_conflict", claimIds: ["claude:s-a#d1", "claude:s-b#i1"], actionId: "claude:s-b@w-early" }))
      .toEqual({ ok: false, reason: "action_before_claim" });
    const mixed = built(decision, intent([write("src/advisories/a.ts", "09:20", "w-early"), write("src/advisories/c.ts", "09:50", "w-late2"), write("src/advisories/b.ts", "09:45", "w-late1")]));
    const m = validateCitations(mixed, ruleClassify(mixed.input));
    if (!m.ok || m.verdict === "none") throw new Error("expected a conflict");
    expect(decide(mixed.input, m.c, { fleet: false, orchestratorSessionIds: [] })).toMatchObject({ urgency: "high", action: { eventId: "w-late1" } });
  });
});

describe("a claim shared by both sessions (replay)", () => {
  test("the same recordId on both sides is excluded from both as shared; another claim is unaffected", () => {
    const b = built({ claims: [claim("k1", "09:10", "constraint", CONSTRAINT, { scope: "project", recordId: "rec-1" })] },
      actor("09:40", { claims: [claim("k9", "09:12", "constraint", CONSTRAINT, { scope: "project", recordId: "rec-1" })] }));
    expect([...b.excluded]).toEqual([["claude:s-a#k1", "shared"], ["claude:s-b#k9", "shared"]]);
    expect(ruleClassify(b.input).verdict).toBe("none");
    const other = built({ claims: [claim("k1", "09:10", "constraint", CONSTRAINT, { scope: "project", recordId: "rec-1" })] },
      actor("09:40", { claims: [claim("k9", "09:12", "intent", "Write in supabase/migrations.", { recordId: "rec-2" })] }));
    expect(ruleClassify(other.input).verdict).toBe("constraint_violation");
    // A recordId must look like a claim id.
    expect(() => snap("s1", { claims: [claim("x", "09:10", "intent", "a", { recordId: "a#b" })] })).toThrow("recordId must match");
  });
});

describe("the rule judge (conservative lexical baseline)", () => {
  test("constraint parsing: negation, verb, target", () => {
    expect(parseConstraint(CONSTRAINT)).toEqual({ verb: null, writeish: true, paths: ["supabase/migrations"], head: null });
    expect(parseConstraint("Never apply migrations to a real DB.")).toEqual({ verb: "apply", writeish: false, paths: [], head: "migrations" });
    expect(parseConstraint("Don't touch src/billing/ledger.ts, please")).toMatchObject({ verb: "touch", writeish: true, paths: ["src/billing/ledger.ts"] });
    expect(parseConstraint("Keep the migrations tidy.")).toBeNull();
  });

  test("path globs", () => {
    expect(pathMatches("supabase/migrations", "supabase/migrations/1.sql")).toBe(true);
    expect(pathMatches("supabase/migrations", "supabase/migrations-old/1.sql")).toBe(false);
    expect(pathMatches("*/migrations/*", "db/migrations/1.sql")).toBe(true);
    expect(pathMatches("*/migrations/*", "a/b/migrations/1.sql")).toBe(true);
    expect(pathMatches("*/migrations/*", "src/migrations-tool/1.ts")).toBe(false);
    expect(pathMatches("src/**/*.sql", "src/a/b/c.sql")).toBe(true);
    expect(pathMatches("src/**/*.sql", "src/a/b/c.ts")).toBe(false);
  });

  test("a pathological glob from claim text stays fast and never matches", () => {
    const started = performance.now();
    expect(pathMatches(`${"**/".repeat(150)}x`, `${"a/".repeat(200)}b`)).toBe(false);
    expect(pathMatches(`${"*a".repeat(60)}`, `${"a".repeat(300)}b`)).toBe(false);
    expect(pathMatches("**/**/**/migrations/*", "db/migrations/1.sql")).toBe(true);
    expect(performance.now() - started).toBeLessThan(500);
  });

  test("the lander's pattern: x/*a*a*a*a*a*a*a*b against 80 a's finishes in under 50 ms (no regex backtracking)", () => {
    const started = performance.now();
    expect(pathMatches("x/*a*a*a*a*a*a*a*b", `x/${"a".repeat(80)}`)).toBe(false);
    expect(pathMatches("x/*a*a*a*a*a*a*a*b", `x/${"a".repeat(4_000)}`)).toBe(false);
    expect(performance.now() - started).toBeLessThan(50);
    expect(pathMatches("x/*a*a*a*a*a*a*a*b", `x/${"a".repeat(80)}b`)).toBe(true);
    expect(pathMatches("x/*", "x".repeat(5_000))).toBe(false); // the subject cap
  });

  test("segment star match", () => {
    const table: Array<[string, string, boolean]> = [
      ["*", "", true], ["*", "abc", true], ["a*c", "abc", true], ["a*c", "abd", false], ["*.sql", "1.sql", true], ["*.sql", "1.sq", false],
      ["a*b*c", "aXbYc", true], ["a*b*c", "acb", false], ["", "", true], ["", "a", false], ["abc", "abc", true], ["*a*", "bbb", false],
    ];
    for (const [p, s, want] of table) expect([p, s, segmentMatches(p, s)]).toEqual([p, s, want]);
  });

  test("an action id present on both sides is ambiguous and refused", () => {
    // Session "x@w1" event "e" and session "x" event "w1@e" would print as the same ref.
    const b = built({ claims: [claim("k1", "09:10", "constraint", CONSTRAINT, { scope: "project" })] }, actor());
    const dup = { ...b.input.b.actions[0]!, side: "a" as const };
    const amb: BuiltInput = { ...b, input: { ...b.input, a: { ...b.input.a, actions: [dup] } } };
    expect(validateCitations(b, { verdict: "constraint_violation", claimIds: ["claude:s-a#k1"], actionId: dup.id })).toMatchObject({ ok: true });
    expect(validateCitations(amb, { verdict: "constraint_violation", claimIds: ["claude:s-a#k1"], actionId: dup.id })).toEqual({ ok: false, reason: "unknown_id" });
  });

  test("explicit opposites only", () => {
    const table: Array<[string, string, string | null]> = [
      ["Enable the retry queue.", "Disable the retry queue.", "enable/disable"],
      ["The flag is on by default.", "The flag stays off.", "on/off"],
      ["Advisories return 501 when off.", "Return 404 when off.", "501/404"],
      ["Use pgbouncer for the pool.", "Don't use pgbouncer.", "use pgbouncer/not pgbouncer"],
      ["Work on the queue.", "Turn the queue off.", null],
      ["Return 501 when off.", "Return 501 when off, always.", null],
      ["Limit to 200 rows.", "Limit to 100 rows.", null],
      ["Use pgbouncer.", "Use pgbouncer too.", null],
    ];
    for (const [x, y, want] of table) expect(oppositeOf(x, y)).toBe(want);
  });

  test("a non-write verb never matches a write ('never apply migrations' is not 'no migration files')", () => {
    const b = built({ claims: [claim("k1", "09:10", "constraint", "Never apply migrations in supabase/migrations to a real DB.", { scope: "project" })] }, actor());
    expect(ruleClassify(b.input).verdict).toBe("none");
  });

  test("a keyword constraint against a write under */migrations/*", () => {
    const b = built({ claims: [claim("k1", "09:10", "constraint", "No new migrations until Monday.", { scope: "repo" })] },
      { writes: [write("db/migrations/7.sql", "09:40", "w1")] });
    expect(ruleClassify(b.input)).toEqual({ verdict: "constraint_violation", claimIds: ["claude:s-a#k1"], actionId: "claude:s-b@w1" });
  });

  test("compatible claims on the shared topic are an overlap; unrelated wording is never a conflict", () => {
    const b = built({ claims: [claim("i1", "09:10", "intent", "Add pagination in src/catalog.")], writes: [write("src/catalog/a.ts", "09:12", "wa")] },
      { claims: [claim("d1", "09:20", "decision", "Search in src/catalog uses trigrams.")], writes: [write("src/catalog/b.ts", "09:22", "wb")] });
    expect(ruleClassify(b.input).verdict).toBe("overlap");
  });
});

describe("snapshot claim fields (format /1, optional fields)", () => {
  test("scope defaults to session, id to c<index>; supersededAt is kept", () => {
    const s = snap("s1", { claims: [claim("x", "09:10", "intent", "a"), { at: at("09:11"), kind: "decision", text: "b", confidence: 1, evidenceIds: [] }] });
    expect(s.claims.map((c) => [c.id, c.scope])).toEqual([["x", "session"], ["c1", "session"]]);
    expect(snap("s1", { claims: [claim("x", "09:10", "intent", "a", { supersededAt: at("09:20"), scope: "repo" })] }).claims[0]).toMatchObject({ scope: "repo", supersededAt: at("09:20") });
  });
  const bad: Array<[Over, string]> = [
    [{ claims: [claim("x", "09:10", "intent", "a", { scope: "team" })] }, "scope must be"],
    [{ claims: [claim("x", "09:10", "intent", "a"), claim("x", "09:11", "intent", "b")] }, "repeats a claim id"],
    [{ claims: [claim("a#b", "09:10", "intent", "a")] }, ".id must match"],
    [{ claims: [claim("x", "09:10", "intent", "a", { supersededAt: at("09:05") })] }, "supersededAt is before"],
    [{ claims: [claim("x", "09:10", "intent", "a", { supersededAt: "soon" })] }, "supersededAt"],
    [{ claims: [claim("x", "09:10", "intent", "a", { owner: "me" })] }, "unknown field"],
    [{ claims: [claim("x", "09:10", "intent", "a", { topic: "Not A Slug" })] }, "topic must be a topic slug"],
  ];

  test("a stored claim's topic slug counts like a path it names; general never does", () => {
    const withTopic = (topic: string) => cands([
      view("s-a", NOW, { claims: [claim("i1", "09:10", "intent", "Tidy things up.", { topic })] }),
      view("s-b", NOW, { writes: [write("src/pager/x.ts", "09:40", "w1")] }),
    ]).candidates.map((c) => c.topics);
    expect(withTopic("src-pager")).toEqual([["src-pager"]]);
    expect(withTopic("general")).toEqual([]);
  });
  for (const [over, msg] of bad) test(`refused: ${msg}`, () => {
    expect(() => snap("s1", over)).toThrow(SnapshotError);
    expect(() => snap("s1", over)).toThrow(msg);
  });
});
