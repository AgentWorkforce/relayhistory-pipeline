/** workspace-synthesis/1 (docs/session-intel-workspace-synthesis.md) over the synthetic F1 cohort fixture. */
import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import {
  decodeSessionAnalysisResult,
  validateEvidenceBundle,
  validateSemanticAnalysis,
  type SessionAnalysisResult,
} from "../../session-analysis/engine/session-analysis-result.ts";
import { cohortJson, FIXTURE_NOW, FIXTURE_WINDOW, FIXTURE_WORKSPACE, fixtureResult } from "./fixtures/synthesis/make-cohort.ts";
import { schemaErrors, invariantErrors, validateWorkspaceSynthesis, WORKSPACE_SYNTHESIS_SCHEMA } from "../synthesis/schema.ts";
import {
  byteLength, capText, issueKeys, SKIP_REASONS, synthesisJson, synthesize, SynthesisInputError, SYNTHESIS_BOUNDS
} from "../synthesis/synthesize.ts";

const FIXTURE_DIR = new URL("./fixtures/synthesis/", import.meta.url);
const cohortText = readFileSync(new URL("cohort.json", FIXTURE_DIR), "utf8");
const cohort = () => JSON.parse(cohortText) as { workspace: string; window: { since: string; until: string }; eligible: number | null; sessions: Array<Record<string, any>> };
const run = (c: unknown = cohort(), opts: Partial<Parameters<typeof synthesize>[1]> = {}) => synthesize(c, { now: FIXTURE_NOW, ...opts });
const refused = (f: () => unknown, code: string) => {
  try { f(); } catch (err) {
    expect(err).toBeInstanceOf(SynthesisInputError);
    expect((err as SynthesisInputError).code).toBe(code);
    return;
  }
  throw new Error(`expected refusal ${code}`);
};
/** A deterministic shuffle (LCG), so a failure reproduces. */
function shuffled<T>(xs: T[], seed: number): T[] {
  const a = [...xs];
  let s = seed;
  for (let i = a.length - 1; i > 0; i--) {
    s = (s * 1_103_515_245 + 12_345) % 2 ** 31;
    const j = s % (i + 1);
    [a[i], a[j]] = [a[j]!, a[i]!];
  }
  return a;
}
const good = () => cohort().sessions.filter((e) => e.status === "analyzed" && !String(e.sessionId).startsWith("fx-bad-"));

describe("fixture", () => {
  test("cohort.json is exactly what make-cohort.ts builds", () => {
    expect(cohortText).toBe(cohortJson());
  });

  test("every good analyzed entry passes the full F1 decode, the evidence bundle and the semantic analysis checks", () => {
    const entries = good();
    expect(entries.length).toBe(40);
    for (const e of entries) {
      const decoded = decodeSessionAnalysisResult(e.result, { source: e.source, sessionId: e.sessionId });
      expect(decoded.status).toBe("ready");
      const r = e.result as SessionAnalysisResult;
      validateEvidenceBundle({ references: r.analysis.evidence.references, coverage: r.analysis.evidence.coverage }, e.source, e.sessionId);
      validateSemanticAnalysis(r.analysis.understanding, r.analysis.evidence.references);
      expect(e.workspace).toBe(FIXTURE_WORKSPACE);
    }
  });

  test("the fx-bad-* entries fail as intended and the declared skips cover every reason", () => {
    const c = cohort();
    const status = (id: string) => {
      const e = c.sessions.find((s) => s.sessionId === id)!;
      return decodeSessionAnalysisResult(e.result, { source: e.source, sessionId: e.sessionId }).status;
    };
    expect(status("fx-bad-dangling")).toBe("malformed");
    expect(status("fx-bad-identity")).toBe("malformed");
    expect(status("fx-bad-schema")).toBe("unsupported");
    expect(status("fx-bad-null")).toBe("empty");
    expect(status("fx-bad-version")).toBe("ready"); // valid, but not an accepted analysisVersion
    expect(status("fx-bad-window")).toBe("ready"); // valid, but outside the window
    expect(new Set(c.sessions.filter((s) => s.status === "skipped").map((s) => s.reason))).toEqual(new Set(SKIP_REASONS));
  });

  test("the fixture covers every sessionType and outcome status, null usage, and observed usage with a null cost", () => {
    const rs = good().map((e) => e.result as SessionAnalysisResult);
    expect(new Set(rs.map((r) => r.analysis.understanding.sessionType.value)).size).toBe(9);
    expect(new Set(rs.flatMap((r) => r.analysis.understanding.outcomes.map((o) => o.status))).size).toBe(8);
    expect(rs.some((r) => !r.analysis.metrics.usage.observed)).toBe(true);
    expect(rs.some((r) => r.analysis.metrics.usage.observed && r.analysis.metrics.usage.costUSD === null)).toBe(true);
  });
});

describe("output", () => {
  test("matches the committed golden byte for byte", () => {
    expect(synthesisJson(run())).toBe(readFileSync(new URL("expected-synthesis.json", FIXTURE_DIR), "utf8"));
  });

  test("validates against the schema and its invariants; the schema rejects drift", () => {
    const s = run();
    expect(schemaErrors(s)).toEqual([]);
    expect(invariantErrors(s)).toEqual([]);
    expect(validateWorkspaceSynthesis(JSON.parse(synthesisJson(s)))).toBeTruthy();
    expect(WORKSPACE_SYNTHESIS_SCHEMA.$id).toBe("workspace-synthesis/1");
    const extra = { ...JSON.parse(synthesisJson(s)), themes: null };
    expect(schemaErrors(extra)).toContain("$: undeclared themes");
    const bad = JSON.parse(synthesisJson(s));
    bad.inputs.analyzed += 1;
    expect(() => validateWorkspaceSynthesis(bad)).toThrow(/considered != analyzed/);
  });

  test("the same cohort in any order gives byte-identical JSON", () => {
    const base = synthesisJson(run());
    for (const seed of [1, 7, 42, 2026]) {
      const c = cohort();
      c.sessions = shuffled(c.sessions, seed);
      expect(synthesisJson(run(c))).toBe(base);
    }
  });

  test("echoes the workspace and window, and generatedAt is the injected now (no clock read)", () => {
    const s = run(cohort(), { now: "2030-01-02T03:04:05.678Z" });
    expect(s.generatedAt).toBe("2030-01-02T03:04:05.678Z");
    expect(s.workspace).toBe(FIXTURE_WORKSPACE);
    expect(s.window).toEqual(FIXTURE_WINDOW);
    expect(Object.keys(s)).toEqual(["contract", "workspace", "window", "generatedAt", "inputs", "freshness", "usageCoverage", "categories", "issues"]);
    refused(() => synthesize(cohort(), {} as never), "invalid_option");
  });

  test("the synthesis sources read no environment, clock or network", () => {
    for (const f of ["synthesize.ts", "schema.ts"]) {
      const src = readFileSync(new URL(`../synthesis/${f}`, import.meta.url), "utf8");
      expect(src).not.toMatch(/process\.env|Bun\.env|Date\.now\(|new Date\(\)|fetch\(|performance\.now/u);
    }
  });
});

describe("denominators", () => {
  test("considered = analyzed + sum(skipped); eligible is the wrapper's, separate", () => {
    const s = run();
    const skipped = Object.values(s.inputs.skipped).reduce((a, b) => a + b, 0);
    expect(s.inputs.considered).toBe(53);
    expect(s.inputs.analyzed).toBe(40);
    expect(s.inputs.considered).toBe(s.inputs.analyzed + skipped);
    expect(s.inputs.skipped).toEqual({ not_analyzed: 2, failed: 1, unsupported: 2, invalid: 3, too_large: 1, stale_version: 2, out_of_window: 2 });
    expect(s.inputs.eligible).toBe(60);
    expect(s.inputs.notes).toContain("7 eligible sessions are not in the cohort.");
  });

  test("a null eligible stays null and is said in the notes; an eligible below the cohort size is refused", () => {
    const c = cohort();
    c.eligible = null;
    const s = run(c);
    expect(s.inputs.eligible).toBeNull();
    expect(s.inputs.notes.some((n) => n.startsWith("eligible is unknown"))).toBe(true);
    const low = cohort();
    low.eligible = 10;
    refused(() => run(low), "invalid_cohort");
  });

  test("categories partition the analyzed sessions by (sessionType, outcome status), with mixed and none", () => {
    const s = run();
    expect(s.categories.reduce((a, c) => a + c.count, 0)).toBe(s.inputs.analyzed);
    const statuses = new Set(s.categories.map((c) => c.outcomeStatus));
    for (const v of ["mixed", "none", "blocked", "verified", "superseded", "not-started", "in-progress", "implemented", "reported-complete", "unknown"]) expect(statuses.has(v as never)).toBe(true);
    expect(new Set(s.categories.map((c) => c.sessionType)).size).toBe(9);
    const debuggingBlocked = s.categories.find((c) => c.sessionType === "debugging" && c.outcomeStatus === "blocked")!;
    expect(debuggingBlocked.count).toBe(debuggingBlocked.sessions.length);
  });

  test("usage: observed and cost are separate axes; observed + unobserved = knownCost + nullCost = analyzed", () => {
    const s = run();
    for (const u of Object.values(s.usageCoverage)) {
      expect(u.observed + u.unobserved).toBe(u.analyzed);
      expect(u.knownCost + u.nullCost).toBe(u.analyzed);
    }
    expect(s.usageCoverage["fx-claude"]).toEqual({ considered: 26, analyzed: 16, observed: 12, unobserved: 4, knownCost: 8, nullCost: 8, observedRate: 0.75, nullCostRate: 0.5 });
    // nullCost (8) exceeds unobserved (4): observed sessions with a null cost count as null cost, never as zero.
    expect(s.usageCoverage["fx-claude"]!.nullCost).toBeGreaterThan(s.usageCoverage["fx-claude"]!.unobserved);
  });

  test("rates are null, not 0, for a source with nothing analyzed; rates have 4 decimals", () => {
    const c = cohort();
    c.sessions = c.sessions.filter((e) => e.source === "fx-codex" || e.status === "skipped");
    const s = run(c);
    expect(s.usageCoverage["fx-claude"]).toMatchObject({ analyzed: 0, observedRate: null, nullCostRate: null });
    const thirds = cohort();
    thirds.sessions = thirds.sessions.filter((e) => ["fx-s01", "fx-s02", "fx-s05"].includes(e.sessionId));
    const t = run(thirds);
    for (const u of Object.values(t.usageCoverage)) for (const r of [u.observedRate, u.nullCostRate]) if (r !== null) expect(Math.round(r * 1e4) / 1e4).toBe(r);
  });
});

describe("freshness", () => {
  test("full watermark: indexedThroughMs is the MIN; stale counts follow the injected now and threshold", () => {
    const s = run();
    const marks = good().map((e) => (e.result as SessionAnalysisResult).analysis.evidence.indexedThroughMs as number);
    expect(s.freshness.indexedThroughMissing).toBe(0);
    expect(s.freshness.indexedThroughMs).toBe(Math.min(...marks));
    expect(s.freshness.analyzedAtMin).toBe("2026-11-01T06:00:00.000Z");
    expect(s.freshness.analyzedAtMax).toBe("2026-11-07T18:00:00.000Z");
    expect(s.freshness.staleCount).toBe(35);
    expect(run(cohort(), { staleAfterMs: 0 }).freshness.staleCount).toBe(40);
    expect(run(cohort(), { staleAfterMs: 7 * 86_400_000 }).freshness.staleCount).toBe(0);
    expect(run(cohort(), { now: "2026-11-07T00:00:00.000Z" }).freshness.analyzedAfterNow).toBe(5);
    const bySource = s.freshness.bySource;
    expect(Object.keys(bySource)).toEqual(["fx-claude", "fx-codex"]);
    expect(bySource["fx-claude"]!.staleCount + bySource["fx-codex"]!.staleCount).toBe(35);
  });

  test("partial watermark: null when any analyzed session lacks one, with the missing count", () => {
    const c = cohort();
    const e = c.sessions.find((x) => x.sessionId === "fx-s05")!;
    e.result.analysis.evidence.indexedThroughMs = null;
    e.result.analysis.evidence.coverage.indexedThroughMs = null;
    const s = run(c);
    expect(s.freshness.indexedThroughMs).toBeNull();
    expect(s.freshness.indexedThroughMissing).toBe(1);
    expect(invariantErrors(s)).toEqual([]);
  });

  test("zero analyzed: every freshness value and rate is null, missing is 0", () => {
    const c = cohort();
    c.sessions = c.sessions.filter((e) => e.status === "skipped");
    const s = run(c);
    expect(s.inputs.analyzed).toBe(0);
    expect(s.freshness).toMatchObject({ analyzedAtMin: null, analyzedAtMax: null, indexedThroughMs: null, indexedThroughMissing: 0, staleCount: 0, bySource: {} });
    for (const u of Object.values(s.usageCoverage)) expect([u.observedRate, u.nullCostRate]).toEqual([null, null]);
    expect(s.categories).toEqual([]);
    expect(s.issues).toEqual([]);
    expect(validateWorkspaceSynthesis(s)).toBeTruthy();
  });
});

describe("issues", () => {
  test("exact-key candidates: a failed command over four roots, an artifact and a second command over two", () => {
    const s = run();
    expect(s.issues.map((i) => [i.kind, i.key, i.sessionCount, i.roots])).toEqual([
      ["failed-command", "Bash: bun test src/fx", 4, 4],
      ["artifact", "migrations/0042_fx_widgets.sql", 3, 2],
      ["failed-command", "Bash: cargo test --workspace", 3, 2],
    ]);
    for (const i of s.issues) {
      expect(i.label).toBe("candidate");
      expect(i.basis).toBe("deterministic");
      expect(i.id).toMatch(new RegExp(`^${i.kind}:[0-9a-f]{16}$`, "u"));
    }
  });

  test("two different commands with the same failure text stay separate issues", () => {
    const s = run();
    const tests = s.issues.filter((i) => i.text.startsWith("Tests failed with exit code 1"));
    expect(tests.map((i) => i.key).sort()).toEqual(["Bash: bun test src/fx", "Bash: cargo test --workspace"]);
    expect(new Set(tests.map((i) => i.id)).size).toBe(2);
  });

  test("the single-root decoy, the uncited blocker and the command that did not fail are not candidates", () => {
    const keys = run().issues.map((i) => i.key);
    expect(keys).not.toContain("src/fx/lint-config.json");
    expect(keys).not.toContain("Bash: bun run build");
    expect(run().issues.some((i) => i.text.includes("Preview deploy"))).toBe(false);
    // The decoy recurs in four sessions: it fails on roots alone.
    expect(run(cohort(), { minRecurrence: 2 }).issues.map((i) => i.key)).not.toContain("src/fx/lint-config.json");
  });

  test("keys are exact: a path or command differing in one character is a different key", () => {
    const byId = new Map([
      ["a", { id: "a", source: "s", sessionId: "x", kind: "file-edit", at: null, summary: "Edit: src/a.ts", detail: null, truncated: false, isError: null, toolCallId: null, locator: { eventUid: null, toolUseId: null, fileEditId: 1 } }],
      ["b", { id: "b", source: "s", sessionId: "x", kind: "file-edit", at: null, summary: "Edit: src/A.ts", detail: null, truncated: false, isError: null, toolCallId: null, locator: { eventUid: null, toolUseId: null, fileEditId: 2 } }],
      ["c", { id: "c", source: "s", sessionId: "x", kind: "tool-call", at: null, summary: "Read: docs/x.md", detail: null, truncated: false, isError: false, toolCallId: null, locator: { eventUid: null, toolUseId: "t", fileEditId: null } }],
    ] as const) as never;
    expect(issueKeys("blocker", ["a", "b", "c"], byId).map((k) => k.key)).toEqual(["src/a.ts", "src/A.ts", "docs/x.md"]);
    expect(issueKeys("blocker", ["missing"], byId)).toEqual([]);
  });

  test("every reference resolves inside the same workspace and window, to a real event of that session", () => {
    const c = cohort(), s = run(c);
    const entry = (source: string, sessionId: string) => c.sessions.find((e) => e.source === source && e.sessionId === sessionId)!;
    const since = Date.parse(FIXTURE_WINDOW.since), until = Date.parse(FIXTURE_WINDOW.until);
    for (const i of s.issues) {
      for (const r of i.sessions) expect(entry(r.source, r.sessionId).workspace).toBe(s.workspace);
      for (const ev of i.evidence) {
        const e = entry(ev.source, ev.sessionId);
        expect(e.workspace).toBe(s.workspace);
        const ref = (e.result as SessionAnalysisResult).analysis.evidence.references.find((r) => r.id === ev.eventId)!;
        expect(ref).toBeTruthy();
        expect(ev.kind).toBe(ref.kind);
        const t = Date.parse(ev.ts!);
        expect(t >= since && t < until).toBe(true);
        expect(Object.keys(ev).sort()).toEqual(["eventId", "kind", "sessionId", "source", "ts"]);
      }
    }
    for (const cat of s.categories) for (const r of cat.sessions) {
      const e = entry(r.source, r.sessionId);
      expect(e.status).toBe("analyzed");
      expect(String(r.sessionId).startsWith("fx-bad-")).toBe(false);
    }
  });
});

describe("scrub regression", () => {
  test("no text enters that is not already in an F1 result, or built from ids, enums and counts", () => {
    const c = cohort(), s = run(c);
    const inputStrings: string[] = [];
    const walk = (v: unknown) => {
      if (typeof v === "string") inputStrings.push(v);
      else if (Array.isArray(v)) v.forEach(walk);
      else if (v && typeof v === "object") Object.values(v).forEach(walk);
    };
    for (const e of c.sessions) if (e.status === "analyzed") walk(e.result);
    const ids = new Set(c.sessions.flatMap((e) => [e.source, e.sessionId]));
    const NOTE = [/^Aggregates cover/u, /^Usage is observed only/u, /^Issues are recurrence/u, /^eligible is unknown/u, /^\d+ eligible sessions are not in the cohort\.$/u,
      /^\d+ sessions beyond the cap of \d+ were not considered\.$/u, /^Drilldowns were shortened to \d+ sessions/u, /^\d+ analyzed sessions carry no indexedThroughMs/u,
      /^No session was analyzed/u, /^\d+ sessions were analyzed after generatedAt\.$/u, /^\d+ results failed validation and were not trusted\.$/u];
    const ENUM = new Set(["workspace-synthesis/1", "candidate", "deterministic", "artifact", "failed-command", "mixed", "none", "planning", "implementation", "debugging", "review", "testing", "research", "conversation", "unknown",
      "not-started", "in-progress", "implemented", "reported-complete", "verified", "blocked", "superseded", "tool-call", "tool-result", "file-edit", "human-message", "assistant-message", "delegated-instruction", "delegation"]);
    const leaks: string[] = [];
    const check = (v: unknown, path: string) => {
      if (typeof v === "string") {
        if (/\.(key|text)$/u.test(path)) { if (!inputStrings.some((x) => x.includes(v))) leaks.push(path); }
        else if (/\.notes\[\d+\]$/u.test(path)) { if (!NOTE.some((re) => re.test(v))) leaks.push(path); }
        else if (/\.eventId$/u.test(path)) { if (!inputStrings.includes(v)) leaks.push(path); }
        else if (!(ENUM.has(v) || ids.has(v) || v === s.workspace || /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/u.test(v) || /^(artifact|failed-command):[0-9a-f]{16}$/u.test(v))) leaks.push(`${path}=${v}`);
      } else if (Array.isArray(v)) v.forEach((x, i) => check(x, `${path}[${i}]`));
      else if (v && typeof v === "object") for (const [k, x] of Object.entries(v)) check(x, `${path}.${k}`);
    };
    check(JSON.parse(synthesisJson(s)), "$");
    expect(leaks).toEqual([]);
    // Object keys that carry data (bySource, usageCoverage) are sources from the cohort.
    for (const k of [...Object.keys(s.usageCoverage), ...Object.keys(s.freshness.bySource)]) expect(ids.has(k)).toBe(true);
  });

  test("a long claim text is cut to a 280-character prefix, still a substring of the input", () => {
    const long = `Fixture failure ${"x".repeat(400)}`;
    const cut = capText(long, 280);
    expect([...cut.text].length).toBe(280);
    expect(long.startsWith(cut.text)).toBe(true);
    expect(cut.truncated).toBe(true);
    const c = cohort();
    for (const id of ["fx-s10", "fx-s14", "fx-s15"]) c.sessions.find((e) => e.sessionId === id)!.result.analysis.understanding.blockers[0].text = long;
    const s = run(c);
    const issue = s.issues.find((i) => i.kind === "artifact")!;
    expect(issue.textTruncated).toBe(true);
    expect(long.startsWith(issue.text)).toBe(true);
    expect(s.inputs.caps.textChars.truncated).toBe(true);
    expect(s.inputs.truncated).toBe(true);
  });
});

describe("refusals and bounds", () => {
  test("a window over 30 days, or empty, is refused; exactly 30 days is accepted", () => {
    const long = cohort();
    long.window = { since: "2026-10-01T00:00:00.000Z", until: "2026-11-01T00:00:01.000Z" };
    refused(() => run(long), "window_too_long");
    const empty = cohort();
    empty.window = { since: FIXTURE_WINDOW.until, until: FIXTURE_WINDOW.until };
    refused(() => run(empty), "invalid_window");
    const thirty = cohort();
    thirty.window = { since: "2026-10-09T00:00:00.000Z", until: "2026-11-08T00:00:00.000Z" };
    expect(run(thirty).inputs.considered).toBe(53);
  });

  test("a cross-workspace entry, a duplicate, a non-plain id or an unknown field refuses the whole cohort", () => {
    const cross = cohort();
    cross.sessions[0]!.workspace = "fx-ws-other";
    refused(() => run(cross), "cross_workspace");
    const missing = cohort();
    delete missing.sessions[0]!.workspace;
    refused(() => run(missing), "cross_workspace");
    const dup = cohort();
    dup.sessions.push({ ...dup.sessions[0]! });
    refused(() => run(dup), "duplicate_session");
    const badId = cohort();
    badId.sessions[0]!.sessionId = "fx s01/../x";
    refused(() => run(badId), "invalid_id");
    const badSource = cohort();
    badSource.sessions[0]!.source = "fx:claude";
    refused(() => run(badSource), "invalid_id");
    const extra = cohort() as Record<string, unknown>;
    extra.themes = null;
    refused(() => run(extra), "invalid_cohort");
    const extraEntry = cohort();
    extraEntry.sessions[0]!.transcript = "raw";
    refused(() => run(extraEntry), "invalid_entry");
    const badReason = cohort();
    badReason.sessions.find((e) => e.status === "skipped")!.reason = "erased";
    refused(() => run(badReason), "invalid_entry");
  });

  test("an evidence id missing from the session's bundle makes the entry invalid, never trusted", () => {
    const s = run();
    expect(s.inputs.skipped.invalid).toBe(3); // fx-bad-dangling, fx-bad-identity, the declared one
    expect(JSON.stringify(s)).not.toContain("fx-bad-");
  });

  test("above 500 sessions the first 500 in (source, sessionId) order are considered and inputs.truncated is set", () => {
    const c = cohort();
    for (let i = 0; i < 460; i++) c.sessions.push({ status: "skipped", workspace: FIXTURE_WORKSPACE, source: "fx-zz", sessionId: `fx-extra-${String(i).padStart(3, "0")}`, reason: "not_analyzed" });
    c.eligible = null;
    const s = run(c);
    expect(s.inputs.considered).toBe(500);
    expect(s.inputs.caps.sessions).toEqual({ limit: 500, offered: 513, truncated: true });
    expect(s.inputs.truncated).toBe(true);
    expect(s.inputs.analyzed).toBe(40); // the dropped tail is the fx-zz source, last in order
    expect(invariantErrors(s)).toEqual([]);
    const huge = { ...cohort(), eligible: null, sessions: Array.from({ length: SYNTHESIS_BOUNDS.maxCohortEntries + 1 }, (_, i) => ({ status: "skipped", workspace: FIXTURE_WORKSPACE, source: "fx-zz", sessionId: `fx-h-${i}`, reason: "failed" })) };
    refused(() => run(huge), "cohort_too_large");
  });

  test("drilldown, issue and evidence caps truncate with flags", () => {
    const s = run(cohort(), { bounds: { maxDrilldown: 2, maxIssues: 1, maxEvidencePerIssue: 2 } });
    expect(s.issues.length).toBe(1);
    expect(s.issues[0]!).toMatchObject({ sessionCount: 4, sessionsTruncated: true, evidenceTruncated: true });
    expect(s.issues[0]!.sessions.length).toBe(2);
    expect(s.issues[0]!.evidence.length).toBe(2);
    expect(s.inputs.caps.issues.truncated).toBe(true);
    expect(s.inputs.caps.evidencePerIssue.truncated).toBe(true);
    expect(s.inputs.truncated).toBe(true);
    expect(validateWorkspaceSynthesis(s)).toBeTruthy();
    refused(() => run(cohort(), { bounds: { maxIssues: 11 } }), "invalid_option");
  });

  test("the 256 KB artifact cap: drilldowns shorten to fit, with a flag; an artifact that cannot fit is refused", () => {
    expect(SYNTHESIS_BOUNDS.maxArtifactBytes).toBe(262_144);
    expect(byteLength(run())).toBeLessThan(SYNTHESIS_BOUNDS.maxArtifactBytes);
    // Real size: 500 analyzed sessions with 255-character ids, 500 long source names and nine full categories.
    const types = ["planning", "implementation", "debugging", "review", "testing", "research", "conversation", "mixed", "unknown"] as const;
    const sessions = Array.from({ length: 500 }, (_, i) => {
      const sessionId = `fx-${String(i).padStart(3, "0")}-${"q".repeat(248)}`;
      const source = `fx-src-${String(i).padStart(3, "0")}-${"w".repeat(50)}`;
      const result = fixtureResult({ sessionId, source, sessionType: types[i % 9]!, outcomes: [{ status: "implemented", verification: "passed" }], usage: "cost", indexed: true }, i % 40);
      result.analysis.title = "Fixture session"; // the per-session title is capped at 90 characters
      return { status: "analyzed", workspace: FIXTURE_WORKSPACE, source, sessionId, result };
    });
    const big = run({ format: "workspace-synthesis-cohort/1", workspace: FIXTURE_WORKSPACE, window: FIXTURE_WINDOW, eligible: null, sessions });
    expect(byteLength(big)).toBeLessThanOrEqual(SYNTHESIS_BOUNDS.maxArtifactBytes);
    expect(big.inputs.caps.artifactBytes.truncated).toBe(true);
    expect(big.inputs.truncated).toBe(true);
    expect(big.inputs.analyzed).toBe(500);
    expect(big.categories.every((c) => c.truncated && c.sessions.length < c.count)).toBe(true);
    expect(validateWorkspaceSynthesis(big)).toBeTruthy();
    refused(() => run(cohort(), { bounds: { maxArtifactBytes: 2_000 } }), "artifact_too_large");
  }, 30_000);
});
