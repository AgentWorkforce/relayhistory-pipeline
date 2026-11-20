/** Relayfacts claims (062150, M1-2): kinds, scope, observed_at, scrub refusal, flag-off parity, supersession, with fakes. */
import { describe, expect, test } from "bun:test";
import type { AnalysisClaim, SessionAnalysisSnapshot } from "../engine/session-analysis-result.ts";
import type { AnalysisJob, AnalysisJobOutput } from "../processor.ts";
import {
  claimObservedAt,
  claimScope,
  claimsHook,
  CLAIMS_READ_MAX,
  type ClaimsPort,
  type CurrentClaim,
  extractClaimRecords,
  postgresClaimsPort,
  SCOPE_RULES,
  sessionIntelClaimsEnabled,
  supersedeDroppedClaims,
} from "../claims.ts";
import { extractDecisionRecords, postgresDecisionRecordsSink } from "../decision-records.ts";
import { analysisTickDeps } from "../runtime-deps.ts";

type Kind = "human-message" | "delegated-instruction" | "assistant-message" | "tool-call";
const ref = (id: string, kind: Kind = "assistant-message", sessionId = "sess-root", at: string | null = null, source = "claude") => ({
  id, source, kind, sessionId, at,
  summary: "TRANSCRIPT SUMMARY MUST NOT BE READ", detail: "TRANSCRIPT DETAIL MUST NOT BE READ",
  truncated: false, isError: null, toolCallId: null, locator: { eventUid: null, toolUseId: null, fileEditId: null },
});
const claim = (text: string, evidenceIds: string[], confidence: AnalysisClaim["confidence"] = "high"): AnalysisClaim =>
  ({ text, evidenceIds, confidence, basis: "observed" });

const REFS = [
  ref("h0", "human-message", "sess-root", "2026-11-18T10:05:00Z"),
  ref("a0", "assistant-message", "sess-root", "2026-11-18T10:01:00Z"),
  ref("a1", "assistant-message", "sess-root", "not a time"),
  ref("d0", "delegated-instruction", "sess-root", "2026-11-18T09:00:00Z"),
  ref("c0", "human-message", "sess-child", "2026-11-18T08:00:00Z"),
];
function snapshot(
  u: { decisions?: AnalysisClaim[]; constraints?: AnalysisClaim[]; intents?: AnalysisClaim[] },
  over: { analyzedAt?: string; refs?: ReturnType<typeof ref>[] } = {},
) {
  return {
    source: "claude",
    sessionId: "sess-root",
    analyzedAt: over.analyzedAt ?? "2026-11-18T12:00:00.000Z",
    evidence: { references: over.refs ?? REFS },
    metrics: { filesChanged: [{ path: "src/session-read-api/app.ts", editCount: 4 }] },
    understanding: { decisions: u.decisions ?? [], constraints: u.constraints ?? [], intents: u.intents ?? [] },
  } as unknown as SessionAnalysisSnapshot;
}

describe("sessionIntelClaimsEnabled", () => {
  test("exactly the string true", () => {
    expect(sessionIntelClaimsEnabled({})).toBe(false);
    for (const v of ["1", "TRUE", "True", " true", "yes", "", "on"]) expect(sessionIntelClaimsEnabled({ SESSION_INTEL_CLAIMS_ENABLED: v })).toBe(false);
    expect(sessionIntelClaimsEnabled({ SESSION_INTEL_CLAIMS_ENABLED: "true" })).toBe(true);
  });
});

describe("extractClaimRecords: each kind under the decision-records rules", () => {
  test("constraints and intents become claims of their kind, in that order, with the same topic and cuts; decisions are not claims", () => {
    const records = extractClaimRecords(snapshot({
      intents: [claim("Add a claims reader for the advisor.", ["a0"])],
      constraints: [claim("Do not touch src/session-intel.", ["a0"], "medium")],
      decisions: [claim("Use one registration call.", ["a0"])],
    }));
    expect(records.map((r) => [r.kind, r.topic, r.title, r.status])).toEqual([
      ["constraint", "src-session-read-api", "Do not touch src/session-intel.", "decided"],
      ["intent", "src-session-read-api", "Add a claims reader for the advisor.", "decided"],
    ]);
    // The claims writer's record shape, key order included.
    expect(Object.keys(records[0]!)).toEqual(["kind", "scope", "observed_at", "topic", "title", "statement", "status", "evidence"]);
  });

  test("low confidence, no cited reference, unknown ids: not recorded, for every kind", () => {
    const none = [claim("Low.", ["a0"], "low"), claim("Uncited.", []), claim("Unknown.", ["zz"])];
    expect(extractClaimRecords(snapshot({ decisions: none, constraints: none, intents: none }))).toEqual([]);
  });

  test("scrub refusal: a claim the scrubber would change is not recorded at all, for every kind", () => {
    const secret = claim(`Rotate the deploy key ghp_${"a".repeat(36)} after the move.`, ["a0"]);
    const records = extractClaimRecords(snapshot({ decisions: [secret], constraints: [secret], intents: [secret, claim("Ship it.", ["a0"])] }));
    expect(records.map((r) => r.title)).toEqual(["Ship it."]);
    expect(JSON.stringify(records)).not.toContain("ghp_");
  });

  test("kind is part of the batch dedupe; a repeat within a kind merges, widening the scope and keeping the earliest time", () => {
    const records = extractClaimRecords(snapshot({
      intents: [claim("Never apply migrations to a real database.", ["a0"])],
      constraints: [
        claim("Never apply migrations to a real database.", ["a1"]),
        claim("never apply migrations to a real database", ["h0"]),
      ],
    }));
    expect(records.map((r) => [r.kind, r.scope, r.observed_at, r.evidence.map((e) => [e.turn_start, e.turn_end])])).toEqual([
      // a1 (unparseable at) then h0 (root human message, "never"): project, earliest parseable at.
      ["constraint", "project", "2026-11-18T10:05:00.000Z", [[0, 2]]],
      ["intent", "session", "2026-11-18T10:01:00.000Z", [[1, 1]]],
    ]);
  });

  test("at most 100 claims across the kinds, constraints first; decisions never count", () => {
    const many = (p: string) => Array.from({ length: 60 }, (_, i) => claim(`${p} ${i}.`, ["a0"]));
    const records = extractClaimRecords(snapshot({ decisions: many("D"), constraints: many("C"), intents: many("I") }));
    expect(records.length).toBe(100);
    expect(records.filter((r) => r.kind === "constraint").length).toBe(60);
    expect(records.filter((r) => r.kind === "intent").length).toBe(40);
  });
});

describe("claimScope: table-driven, ambiguity is session", () => {
  const root = { source: "claude", sessionId: "sess-root" };
  const at = (kind: Kind, sessionId = "sess-root") => ({ source: "claude", sessionId, index: 0, kind, at: null });
  const cases: [string, string, ReturnType<typeof at>[], "session" | "project" | "repo"][] = [
    ["plain session decision", "Use keyset paging.", [at("assistant-message")], "session"],
    ["root human, beyond-session marker", "Never apply migrations to a real database.", [at("human-message")], "project"],
    ["root human, from now on", "From now on run the full lane before pushing.", [at("human-message")], "project"],
    ["root human, in this repo", "In this repo every route answers 501 when off.", [at("human-message")], "project"],
    ["root human, for everyone", "Tests run per file for everyone.", [at("human-message")], "project"],
    ["marker but no human message: ambiguous", "Never round the cursor.", [at("assistant-message")], "session"],
    ["human message, no marker: ambiguous", "Rename the helper.", [at("human-message")], "session"],
    ["human message from a child session only", "Never push main.", [at("human-message", "sess-child")], "session"],
    ["a delegated instruction beats every marker", "Per AGENTS.md never push main.", [at("human-message"), at("delegated-instruction")], "session"],
    ["limited to this session beats project", "Never touch my branch for now.", [at("human-message")], "session"],
    ["limited to this task beats repo", "Follow CLAUDE.md for this task only.", [at("human-message")], "session"],
    ["names AGENTS.md", "AGENTS.md forbids force-pushing main.", [at("assistant-message")], "repo"],
    ["names CLAUDE.md", "Per CLAUDE.md, run typecheck before commits.", [at("tool-call")], "repo"],
    ["names CONTRIBUTING.md", "CONTRIBUTING.md asks for one issue per change.", [at("human-message")], "repo"],
    ["names .cursorrules", "The .cursorrules file bans default exports.", [at("assistant-message")], "repo"],
    ["calls itself a repository rule", "It is a repository rule that migrations take the next slot.", [at("assistant-message")], "repo"],
    ["calls itself a project policy", "Project policy: no credentials on disk.", [at("assistant-message")], "repo"],
    ["a file merely like AGENTS.md is not one", "Edit MYAGENTS.md.bak later.", [at("human-message")], "session"],
    ["no cited reference from the root session", "AGENTS.md forbids force-pushing main.", [at("assistant-message", "sess-child")], "session"],
  ];
  for (const [name, text, cited, scope] of cases) test(name, () => expect(claimScope({ text, cited, root })).toBe(scope));
  test("every rule is described", () => {
    for (const rule of SCOPE_RULES) expect(rule.when.length).toBeGreaterThan(10);
  });
});

describe("N1: a bad timestamp drops its claim, never the batch", () => {
  const NOW = Date.parse("2026-11-18T12:00:00Z");
  const r = (at: string | null) => ({ source: "claude", sessionId: "s", index: 0, kind: null, at });
  test("before 1970 or more than a day ahead is a bad time (undefined); the bounds themselves are kept", () => {
    expect(claimObservedAt([r("1969-12-31T23:59:59Z")], NOW)).toBeUndefined();
    expect(claimObservedAt([r("-000500-01-01T00:00:00Z")], NOW)).toBeUndefined();
    expect(claimObservedAt([r("+275760-09-13T00:00:00Z")], NOW)).toBeUndefined();
    expect(claimObservedAt([r("2026-11-19T12:00:00.001Z")], NOW)).toBeUndefined();
    expect(claimObservedAt([r("1970-01-01T00:00:00Z")], NOW)).toBe("1970-01-01T00:00:00.000Z");
    expect(claimObservedAt([r("2026-11-19T12:00:00Z")], NOW)).toBe("2026-11-19T12:00:00.000Z");
    // The earliest decides: a good later time does not rescue a bad earlier one.
    expect(claimObservedAt([r("2026-11-18T10:00:00Z"), r("1900-01-01T00:00:00Z")], NOW)).toBeUndefined();
  });
  test("the hook drops only the bad claim, writes the rest and counts the drop", async () => {
    const refs = [ref("h0", "human-message", "sess-root", "2026-11-18T10:05:00Z"), ref("bad", "assistant-message", "sess-root", "1900-01-01T00:00:00Z")];
    const s = snapshot({ constraints: [claim("Never push main.", ["h0"]), claim("A claim from 1900.", ["bad"])] }, { refs });
    const { extractClaims } = await import("../claims.ts");
    expect(extractClaims(s, NOW)).toMatchObject({ badTime: 1, records: [{ title: "Never push main." }] });
    const sent: number[] = [];
    const { port } = fakePort([], { insert: async (_s, recs) => { sent.push(recs.length); return { inserted: recs.length, merged: 0, evidence_added: recs.length }; } });
    const counts: Record<string, number> = {};
    await claimsHook(port, counts)({ orgId: "o", workspaceId: "w" } as AnalysisJob, { result: { analysis: s } } as unknown as AnalysisJobOutput);
    expect(sent).toEqual([1]);
    expect(counts).toEqual({ claim_records: 1, claim_records_bad_time: 1 });
  });
});

describe("claimObservedAt", () => {
  test("the earliest parseable time among the cited references, else null", () => {
    const r = (at: string | null) => ({ source: "claude", sessionId: "s", index: 0, kind: null, at });
    expect(claimObservedAt([])).toBeNull();
    expect(claimObservedAt([r(null), r("garbage")])).toBeNull();
    expect(claimObservedAt([r("2026-11-18T10:05:00Z"), r(null), r("2026-11-18T09:59:59.5+00:00")])).toBe("2026-11-18T09:59:59.500Z");
  });
  test("only references the claim's evidence rows cite count", () => {
    // c0 (child, 08:00) is cited too, d0 is not cited: the earliest cited time wins.
    const [r] = extractClaimRecords(snapshot({ intents: [claim("Add the reader.", ["a0", "c0"])] }));
    expect(r!.observed_at).toBe("2026-11-18T08:00:00.000Z");
  });
});

describe("flag off: byte-identical to decisions only", () => {
  const golden = snapshot({
    decisions: [claim("Use one registration call. It keeps app.ts small.", ["a0", "a1"]), claim("Never round the cursor.", ["h0"])],
    constraints: [claim("Do not add a route.", ["h0"])],
    intents: [claim("Add the reader.", ["a0"])],
  });
  test("the decisions extractor ignores constraints and intents and keeps the exact v1 record JSON", () => {
    expect(JSON.stringify(extractDecisionRecords(golden))).toBe(JSON.stringify([
      { topic: "src-session-read-api", title: "Use one registration call.", statement: "Use one registration call. It keeps app.ts small.", status: "decided",
        evidence: [{ source: "claude", session_id: "sess-root", turn_start: 1, turn_end: 2, claim: "Use one registration call. It keeps app.ts small.", ref: null }] },
      { topic: "src-session-read-api", title: "Never round the cursor.", statement: "Never round the cursor.", status: "decided",
        evidence: [{ source: "claude", session_id: "sess-root", turn_start: 0, turn_end: 0, claim: "Never round the cursor.", ref: null }] },
    ]));
  });
  const inputs = (env: NodeJS.ProcessEnv) => analysisTickDeps({
    env, db: { query: async () => ({ rows: [] }) } as never, store: {} as never, bucket: "b", region: "r", keyPrefix: "p",
    readerMaxRuleset: 1, credentials: async () => ({}) as never, modelKey: undefined,
  } as never);
  test("runtime deps: the decisions sink with the outer flag, as before; the claims port next to it only with both flags exactly true", () => {
    expect("decisionRecords" in inputs({}) || "claims" in inputs({})).toBe(false);
    expect("claims" in inputs({ SESSION_INTEL_CLAIMS_ENABLED: "true" })).toBe(false);
    expect("decisionRecords" in inputs({ SESSION_INTEL_CLAIMS_ENABLED: "true" })).toBe(false);
    for (const v of [undefined, "1", "TRUE", ""]) {
      const d = inputs({ SUMMARY_DECISION_RECORDS_ENABLED: "true", ...(v === undefined ? {} : { SESSION_INTEL_CLAIMS_ENABLED: v }) });
      expect("claims" in d).toBe(false);
      expect(typeof (d as { decisionRecords?: unknown }).decisionRecords).toBe("function");
    }
    const on = inputs({ SUMMARY_DECISION_RECORDS_ENABLED: "true", SESSION_INTEL_CLAIMS_ENABLED: "true" });
    expect(typeof (on as { decisionRecords?: unknown }).decisionRecords).toBe("function");
    expect(typeof (on as { claims?: ClaimsPort }).claims?.insert).toBe("function");
  });
  test("the decisions sink makes exactly the v1 call", async () => {
    const calls: [string, unknown[]][] = [];
    const sink = postgresDecisionRecordsSink({ query: async (text: string, params: unknown[]) => { calls.push([text, params]); return { rows: [{ inserted: 2, merged: 0, evidence_added: 2 }] }; } } as never);
    await sink({ orgId: "org-a", workspaceId: "ws-a" }, extractDecisionRecords(golden));
    expect(calls).toEqual([["SELECT inserted, merged, evidence_added FROM sessions.relayfacts_insert_v1($1, $2, $3::jsonb)",
      ["org-a", "ws-a", JSON.stringify(extractDecisionRecords(golden))]]]);
  });
});

/** An in-memory port: `rows` is what relayfacts_claims_v1 returns; calls are recorded. */
function fakePort(rows: CurrentClaim[], over: Partial<ClaimsPort> = {}) {
  const calls: string[] = [];
  const port: ClaimsPort = {
    insert: async (_s, records) => { calls.push(`insert:${records.length}`); return { inserted: records.length, merged: 0, evidence_added: records.length }; },
    current: async (_s, f) => { calls.push(`current:${f.kind}:${f.topic}`); return rows.filter((r) => r.kind === f.kind); },
    supersede: async (_s, o, n) => { calls.push(`supersede:${o}->${n}`); return true; },
    ...over,
  };
  return { port, calls };
}
const row = (id: string, kind: CurrentClaim["kind"], title: string, ev: [string, string, string][] = [["claude", "sess-root", "2026-11-18T11:00:00Z"]], supersedes: string | null = null): CurrentClaim =>
  ({ id, kind, title, supersedes, evidence: ev.map(([source, session_id, created_at]) => ({ source, session_id, created_at })) });
const SCOPE = { orgId: "org-a", workspaceId: "ws-a" };

describe("mutation guards (lander mutants T3, T7, T8, T12)", () => {
  const snap = snapshot({ constraints: [claim("No new migrations until the renumber lands.", ["h0"])] });
  const records = extractClaimRecords(snap);
  const fresh = row("new", "constraint", "No new migrations until the renumber lands.", [["claude", "sess-root", "2026-11-18T12:00:01Z"]]);
  test("T3: a repeated claim in one batch keeps the EARLIEST observed time, in either order", () => {
    const refs = [ref("late", "assistant-message", "sess-root", "2026-11-18T10:00:00Z"), ref("early", "assistant-message", "sess-root", "2026-11-18T09:00:00Z")];
    for (const order of [["late", "early"], ["early", "late"]]) {
      const [r] = extractClaimRecords(snapshot({ intents: order.map((id) => claim("Add the reader.", [id])) }, { refs }));
      expect([order.join(","), r!.observed_at]).toEqual([order.join(","), "2026-11-18T09:00:00.000Z"]);
    }
  });
  test("T7: evidence written at exactly analyzedAt is not older than the analysis: nothing superseded", async () => {
    const { port, calls } = fakePort([fresh, row("old", "constraint", "No migrations.", [["claude", "sess-root", "2026-11-18T12:00:00.000Z"]])]);
    expect(await supersedeDroppedClaims(port, SCOPE, snap, records)).toBe(0);
    expect(calls.filter((c) => c.startsWith("supersede"))).toEqual([]);
    // One millisecond earlier is older: superseded.
    const { port: p2 } = fakePort([fresh, row("old", "constraint", "No migrations.", [["claude", "sess-root", "2026-11-18T11:59:59.999Z"]])]);
    expect(await supersedeDroppedClaims(p2, SCOPE, snap, records)).toBe(1);
  });
  test("T8: a full read page supersedes nothing, even when exactly one candidate qualifies in it", async () => {
    // The page holds the new claim, one qualifying candidate, and others that are not candidates (another session).
    const others = Array.from({ length: CLAIMS_READ_MAX - 2 }, (_, i) => row(`x${i}`, "constraint", `Other ${i}.`, [["claude", "sess-other", "2026-11-18T11:00:00Z"]]));
    const page = [fresh, row("old", "constraint", "No migrations.")].concat(others);
    expect(page.length).toBe(CLAIMS_READ_MAX);
    const { port, calls } = fakePort(page);
    expect(await supersedeDroppedClaims(port, SCOPE, snap, records)).toBe(0);
    expect(calls.filter((c) => c.startsWith("supersede"))).toEqual([]);
    // One row fewer: the same candidate is superseded.
    const { port: p2 } = fakePort(page.slice(0, -1));
    expect(await supersedeDroppedClaims(p2, SCOPE, snap, records)).toBe(1);
  });
  test("T12: a candidate without evidence rows is never superseded (an empty list proves nothing)", async () => {
    const { port, calls } = fakePort([fresh, row("old", "constraint", "No migrations.", [])]);
    expect(await supersedeDroppedClaims(port, SCOPE, snap, records)).toBe(0);
    expect(calls.filter((c) => c.startsWith("supersede"))).toEqual([]);
  });
});

describe("supersedeDroppedClaims: only the narrow, unambiguous case", () => {
  const snap = snapshot({ constraints: [claim("No new migrations until the renumber lands.", ["h0"])] });
  const records = extractClaimRecords(snap);
  const fresh = row("new", "constraint", "No new migrations until the renumber lands.", [["claude", "sess-root", "2026-11-18T12:00:01Z"]]);
  test("the one dropped claim of this session's older analysis, same kind and topic: superseded by the one new claim", async () => {
    const { port, calls } = fakePort([fresh, row("old", "constraint", "No migrations at all.")]);
    expect(await supersedeDroppedClaims(port, SCOPE, snap, records)).toBe(1);
    expect(calls).toEqual(["current:constraint:src-session-read-api", "supersede:old->new"]);
  });
  const none: [string, CurrentClaim[], SessionAnalysisSnapshot?][] = [
    ["the old record also has evidence from another session", [fresh, row("old", "constraint", "No migrations.", [["claude", "sess-root", "2026-11-18T11:00:00Z"], ["claude", "sess-other", "2026-11-18T11:00:00Z"]])]],
    ["the old record is from another history source", [fresh, row("old", "constraint", "No migrations.", [["codex", "sess-root", "2026-11-18T11:00:00Z"]])]],
    ["the old record was written after this analysis (a newer analysis already ran)", [fresh, row("old", "constraint", "No migrations.", [["claude", "sess-root", "2026-11-18T12:30:00Z"]])]],
    ["two dropped candidates: no guess", [fresh, row("o1", "constraint", "No migrations."), row("o2", "constraint", "Ask first.")]],
    ["the new record already supersedes another", [{ ...fresh, supersedes: "x" }, row("old", "constraint", "No migrations.")]],
    ["the new record is not found", [row("old", "constraint", "No migrations.")]],
    ["another kind is never a candidate", [fresh, row("old", "intent", "No migrations.")]],
    ["the claim is still present: nothing dropped", [fresh]],
    ["an unparseable analyzedAt", [fresh, row("old", "constraint", "No migrations.")], snapshot({ constraints: [claim("No new migrations until the renumber lands.", ["h0"])] }, { analyzedAt: "yesterday" })],
    ["a full read page", [fresh, ...Array.from({ length: CLAIMS_READ_MAX - 1 }, (_, i) => row(`o${i}`, "constraint", `Other ${i}.`))]],
  ];
  for (const [name, rows, s] of none)
    test(`nothing superseded: ${name}`, async () => {
      const { port, calls } = fakePort(rows);
      expect(await supersedeDroppedClaims(port, SCOPE, s ?? snap, records)).toBe(0);
      expect(calls.filter((c) => c.startsWith("supersede"))).toEqual([]);
    });
  test("nothing superseded: two records of the same kind in the snapshot (no pairing)", async () => {
    const two = snapshot({ constraints: [claim("A first rule.", ["h0"]), claim("A second rule.", ["h0"])] });
    const { port, calls } = fakePort([row("n1", "constraint", "A first rule."), row("n2", "constraint", "A second rule."), row("old", "constraint", "Old rule.")]);
    expect(await supersedeDroppedClaims(port, SCOPE, two, extractClaimRecords(two))).toBe(0);
    expect(calls).toEqual([]);
  });
  test("a refused supersede (already superseded meanwhile) counts nothing", async () => {
    const { port } = fakePort([fresh, row("old", "constraint", "No migrations.")], { supersede: async () => false });
    expect(await supersedeDroppedClaims(port, SCOPE, snap, records)).toBe(0);
  });
});

describe("claimsHook", () => {
  const job = { orgId: "org-j", workspaceId: "ws-j" } as AnalysisJob;
  const out = (s: SessionAnalysisSnapshot) => ({ result: { analysis: s } }) as unknown as AnalysisJobOutput;
  const snap = snapshot({ decisions: [claim("Use one call.", ["a0"])], intents: [claim("Add the reader.", ["a0"])] });
  test("writes in the job's own workspace and counts; no claims means no call", async () => {
    const scopes: unknown[] = [];
    const { port, calls } = fakePort([], { insert: async (s, r) => { scopes.push(s); return { inserted: r.length, merged: 0, evidence_added: r.length }; } });
    const counts: Record<string, number> = {};
    await claimsHook(port, counts)(job, out(snap));
    expect(scopes).toEqual([{ orgId: "org-j", workspaceId: "ws-j" }]);
    expect(counts).toEqual({ claim_records: 1 });
    expect(calls).toEqual(["current:intent:src-session-read-api"]);
    const empty: Record<string, number> = {};
    const { port: p2, calls: c2 } = fakePort([]);
    await claimsHook(p2, empty)(job, out(snapshot({})));
    expect([empty, c2]).toEqual([{}, []]);
  });
  test("a fenced or failed write is counted on its own keys, and supersession is skipped", async () => {
    for (const [err, key] of [[Object.assign(new Error("x"), { code: "RH016" }), "claim_records_fenced"], [new Error("boom"), "claim_records_failed"]] as const) {
      const { port, calls } = fakePort([], { insert: async () => { throw err; } });
      const counts: Record<string, number> = {};
      await claimsHook(port, counts)(job, out(snap));
      expect(counts).toEqual({ [key]: 1 });
      expect(calls).toEqual([]);
    }
  });
  test("a supersession failure after the write is contained and counted", async () => {
    const { port } = fakePort([], { current: async () => { throw new Error("read failed"); } });
    const counts: Record<string, number> = {};
    await claimsHook(port, counts)(job, out(snap));
    expect(counts).toEqual({ claim_records: 1, claim_supersede_failed: 1 });
  });
});

describe("postgresClaimsPort", () => {
  test("the claims writer, the claims reader and the claims supersede, bound parameters only", async () => {
    const calls: [string, unknown[]][] = [];
    const db = { query: async (text: string, params: unknown[]) => { calls.push([text, params]); return { rows: [{ inserted: 1, merged: 0, evidence_added: 1, ok: true }] }; } } as never;
    const port = postgresClaimsPort(db);
    expect(await port.insert(SCOPE, [])).toBeNull();
    const records = extractClaimRecords(snapshot({ intents: [claim("Add the reader.", ["a0"])] }));
    await port.insert(SCOPE, records);
    await port.current(SCOPE, { topic: "general", kind: "intent" });
    expect(await port.supersede(SCOPE, "o", "n")).toBe(true);
    expect(calls).toEqual([
      ["SELECT inserted, merged, evidence_added FROM sessions.relayfacts_claims_insert_v1($1, $2, $3::jsonb)", ["org-a", "ws-a", JSON.stringify(records)]],
      ["SELECT id, kind, title, supersedes, evidence FROM sessions.relayfacts_claims_v1($1, $2, $3, $4, NULL, $5)", ["org-a", "ws-a", "general", "intent", CLAIMS_READ_MAX]],
      ["SELECT sessions.relayfacts_claims_supersede_v1($1, $2, $3::uuid, $4::uuid) AS ok", ["org-a", "ws-a", "o", "n"]],
    ]);
  });
});
