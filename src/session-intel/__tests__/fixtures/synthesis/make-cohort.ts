/**
 * Builds the synthetic workspace-synthesis-cohort/1 fixture (cohort.json beside this file). Every id, path and text
 * is invented (fx-*, example.test). Regenerate with:
 *
 *   bun src/session-intel/__tests__/fixtures/synthesis/make-cohort.ts
 *
 * synthesis.test.ts checks that the committed cohort.json equals this builder's output byte for byte.
 */
import { writeFileSync } from "node:fs";
import {
  EVIDENCE_KINDS,
  type AnalysisClaim,
  type OutcomeStatus,
  type SessionAnalysisResult,
  type SessionEvidenceReference,
  type SessionOutcome,
  type SessionType,
  type VerificationStatus,
} from "../../../../session-analysis/engine/session-analysis-result.ts";
import { ANALYZER_VERSION } from "../../../../session-analysis/engine/model.ts";
import { SESSION_ANALYSIS_VERSION } from "../../../../session-analysis/versions.ts";
import { COHORT_FORMAT, SESSION_TYPES, OUTCOME_STATUSES, type SkipReason } from "../../../synthesis/synthesize.ts";

export const FIXTURE_WORKSPACE = "fx-ws-synthesis";
export const FIXTURE_WINDOW = { since: "2026-11-01T00:00:00.000Z", until: "2026-11-08T00:00:00.000Z" } as const;
export const FIXTURE_NOW = "2026-11-08T00:00:00.000Z";
const VERIFICATIONS: readonly VerificationStatus[] = ["not-observed", "passed", "failed", "inconclusive"];

export type Usage = "cost" | "no-cost" | "none";
export type Cite = { tool: string; command: string; isError: boolean; viaResult?: boolean } | { edit: string } | { uncited: true };
export interface Spec {
  sessionId: string;
  source: string;
  repo?: string;
  projectId?: string;
  sessionType: SessionType;
  outcomes: Array<{ status: OutcomeStatus; verification: VerificationStatus }>;
  blockers?: Array<{ text: string; cite: Cite }>;
  failedVerification?: { text: string; cite: Cite };
  usage: Usage;
  indexed: boolean;
}

const ALPHA = "example.test/fx/alpha", BETA = "example.test/fx/beta", GAMMA = "example.test/fx/gamma";
const DELTA = "fx-proj-delta";
const claim = (text: string, evidenceIds: string[]): AnalysisClaim => ({ text, evidenceIds, confidence: "high", basis: "observed" });
const atOf = (i: number) => new Date(Date.parse(FIXTURE_WINDOW.since) + 6 * 3_600_000 + i * 4 * 3_600_000).toISOString();

export function fixtureResult(spec: Spec, i: number): SessionAnalysisResult {
  const { source, sessionId } = spec;
  const analyzedAt = atOf(i);
  const eventAt = new Date(Date.parse(analyzedAt) - 3_600_000).toISOString();
  const refs: SessionEvidenceReference[] = [];
  const add = (r: Omit<SessionEvidenceReference, "source" | "sessionId" | "truncated" | "detail" | "at"> & { at?: string | null }) => {
    refs.push({ source, sessionId, at: eventAt, detail: null, truncated: false, ...r } as SessionEvidenceReference);
    return r.id;
  };
  const human = add({ id: `event:${source}:${sessionId}:h1`, kind: "human-message", summary: "Fixture request for the synthetic workspace.", isError: null, toolCallId: null, locator: { eventUid: `fx-uid-${sessionId}-h1`, toolUseId: null, fileEditId: null } });
  const assistant = add({ id: `event:${source}:${sessionId}:a1`, kind: "assistant-message", summary: "Fixture reply describing the work.", isError: null, toolCallId: null, locator: { eventUid: `fx-uid-${sessionId}-a1`, toolUseId: null, fileEditId: null } });
  let n = 0;
  const cite = (c: Cite): string[] => {
    if ("uncited" in c) return [];
    n++;
    if ("edit" in c) return [add({ id: `edit:${source}:${sessionId}:${n}`, kind: "file-edit", summary: `Edit: ${c.edit}`, isError: null, toolCallId: null, locator: { eventUid: null, toolUseId: `fx-tu-${sessionId}-${n}`, fileEditId: n } })];
    const call = add({ id: `tool:${source}:${sessionId}:${n}`, kind: "tool-call", summary: `${c.tool}: ${c.command}`, isError: c.viaResult ? null : c.isError, toolCallId: null, locator: { eventUid: null, toolUseId: `fx-tu-${sessionId}-${n}`, fileEditId: null } });
    if (!c.viaResult) return [call];
    return [add({ id: `result:${source}:${sessionId}:${n}`, kind: "tool-result", summary: "Fixture tool output.", isError: c.isError, toolCallId: call, locator: { eventUid: `fx-uid-${sessionId}-r${n}`, toolUseId: `fx-tu-${sessionId}-${n}`, fileEditId: null } })];
  };
  const blockers = (spec.blockers ?? []).map((b) => claim(b.text, cite(b.cite)));
  const outcomes: SessionOutcome[] = spec.outcomes.map((o, k) => {
    const failed = o.verification === "failed" && spec.failedVerification;
    const verificationIds = failed ? cite(spec.failedVerification!.cite) : [assistant];
    return {
      id: `fx-outcome-${k + 1}`,
      intent: claim("Fixture intent for the outcome.", [human]),
      status: o.status,
      assessment: claim(`Fixture assessment: the outcome is ${o.status}.`, [assistant]),
      changes: [],
      verification: { value: o.verification, explanation: failed ? spec.failedVerification!.text : `Fixture verification is ${o.verification}.`, evidenceIds: verificationIds, confidence: "medium", basis: "observed" },
      remaining: [],
      contradictions: [],
      // A superseded outcome must name a replacement: the next one in the list.
      supersededBy: o.status === "superseded" && k + 1 < spec.outcomes.length ? `fx-outcome-${k + 2}` : null,
    };
  });
  const coverage = {
    byKind: EVIDENCE_KINDS.map((kind) => {
      const included = refs.filter((r) => r.kind === kind).length;
      return { kind, available: included, included, truncated: 0 };
    }),
    metricsOmitted: { models: 0, tools: 0, filesChanged: 0, interactionPeriods: 0 },
    metricsShortened: 0,
    treeTruncated: false,
    unlinkedDelegationCount: 0,
    hydrationStatus: "complete",
    diagnosticCount: 0,
    sourceStamp: null,
    indexedThroughMs: spec.indexed ? Date.parse(analyzedAt) - 30 * 60_000 : null,
  };
  const usage = spec.usage === "none"
    ? { observed: false, inputTokens: 0, outputTokens: 0, cachedInputTokens: 0, totalTokens: 0, costUSD: null }
    : { observed: true, inputTokens: 1_000 + i * 10, outputTokens: 200 + i, cachedInputTokens: 100, totalTokens: 1_200 + i * 11, costUSD: spec.usage === "cost" ? 0.0125 + i / 1_000 : null };
  return {
    schemaVersion: 1,
    analysisVersion: SESSION_ANALYSIS_VERSION,
    sourceFingerprint: `fx-fingerprint-${sessionId}`,
    durationMs: 1_500,
    analysis: {
      schemaVersion: 2,
      title: `Fixture session ${sessionId}`,
      description: "A synthetic session for the workspace synthesis fixture.",
      snapshotId: `fx-snapshot-${sessionId}`,
      inputFingerprint: `fx-input-${sessionId}`,
      source,
      sessionId,
      analyzedAt,
      analyzerVersion: ANALYZER_VERSION,
      backend: i % 5 === 0 ? null : { requestId: `fx-request-${sessionId}`, model: "fx-model", inputTokens: 900, outputTokens: 150, totalTokens: 1_050, modelDurationMs: 1_200, totalDurationMs: 1_400 },
      evidence: {
        capability: "full",
        discoveryState: "complete",
        sourceStamp: null,
        indexedThroughMs: coverage.indexedThroughMs,
        rootEventCount: refs.length,
        eventCount: refs.length,
        toolCallCount: refs.filter((r) => r.kind === "tool-call").length,
        fileEditCount: refs.filter((r) => r.kind === "file-edit").length,
        relatedSessionIds: [],
        unlinkedDelegationCount: 0,
        treeTruncated: false,
        references: refs,
        coverage,
        diagnostics: [],
      },
      metrics: { models: ["fx-model"], usage, tools: [], filesChanged: [], interactionPeriods: [], longestInteractionPeriodMs: null },
      understanding: {
        description: claim("Fixture description of the session.", [assistant]),
        intents: [claim("Fixture intent of the session.", [human])],
        constraints: [],
        outcome: claim("Fixture overall outcome.", [assistant]),
        outcomes,
        codeChangeAssessment: claim("Fixture code change assessment.", [assistant]),
        decisions: [],
        testing: [],
        risks: [],
        blockers,
        skillsUsed: [],
        state: { value: "idle", explanation: "Fixture state.", evidenceIds: [assistant], confidence: "high", basis: "observed" },
        sessionType: { value: spec.sessionType, explanation: "Fixture session type.", evidenceIds: [assistant], confidence: "high", basis: "observed" },
      },
    },
  };
}

/** The good sessions: every sessionType and outcome status, mixed and none categories, and the recurrence cases. */
function specs(): Spec[] {
  const roots: Array<Pick<Spec, "repo" | "projectId" | "source">> = [
    { source: "fx-claude", repo: ALPHA }, { source: "fx-codex", repo: BETA }, { source: "fx-claude", repo: GAMMA },
    { source: "fx-codex", projectId: DELTA }, { source: "fx-codex" },
  ];
  const usages: Usage[] = ["cost", "cost", "no-cost", "none"];
  const out: Spec[] = [];
  for (let i = 0; i < 40; i++) {
    const status = OUTCOME_STATUSES[i % OUTCOME_STATUSES.length]!;
    const outcomes = i % 13 === 7 ? [] // no outcome: category `none`
      : i % 11 === 3 ? [{ status: "superseded" as const, verification: "not-observed" as const }, { status: "implemented" as const, verification: "passed" as const }] // `mixed`
      : [{ status, verification: VERIFICATIONS[i % VERIFICATIONS.length]! }];
    out.push({
      sessionId: `fx-s${String(i + 1).padStart(2, "0")}`,
      ...roots[i % roots.length]!,
      sessionType: SESSION_TYPES[i % SESSION_TYPES.length]!,
      outcomes,
      usage: usages[i % usages.length]!,
      indexed: true,
    });
  }
  const at = (id: string) => out.find((s) => s.sessionId === id)!;
  const failVerify = (id: string, text: string, cite: Cite) => {
    const s = at(id);
    s.outcomes = [{ status: "blocked", verification: "failed" }];
    s.failedVerification = { text, cite };
  };
  // Candidate: one failed command in four sessions over four roots (alpha, beta, gamma, delta); its text varies.
  failVerify("fx-s01", "Tests failed with exit code 1 in run fx-run-0001.", { tool: "Bash", command: "bun test src/fx", isError: true });
  failVerify("fx-s02", "Tests failed with exit code 1 in run fx-run-0002.", { tool: "Bash", command: "bun test src/fx", isError: true, viaResult: true });
  failVerify("fx-s03", "Tests failed with exit code 1 in run fx-run-0003.", { tool: "Bash", command: "bun test src/fx", isError: true });
  failVerify("fx-s04", "Tests failed with exit code 1 in run fx-run-0004.", { tool: "Bash", command: "bun test src/fx", isError: true });
  // Candidate: another command, the same failure text, in three sessions over two roots (beta, gamma). Stays separate.
  failVerify("fx-s07", "Tests failed with exit code 1 in run fx-run-0001.", { tool: "Bash", command: "cargo test --workspace", isError: true });
  failVerify("fx-s08", "Tests failed with exit code 1 in run fx-run-0002.", { tool: "Bash", command: "cargo test --workspace", isError: true });
  failVerify("fx-s12", "Tests failed with exit code 1 in run fx-run-0003.", { tool: "Bash", command: "cargo test --workspace", isError: true });
  // Candidate: one artifact blocked in three sessions over two roots (project delta, source fx-codex).
  for (const id of ["fx-s10", "fx-s14", "fx-s15"]) at(id).blockers = [{ text: `Migration blocked: a lock is held on the widgets table (${id}).`, cite: { edit: "migrations/0042_fx_widgets.sql" } }];
  // Decoy: one artifact blocked in four sessions, all in the alpha repository. One root: never a candidate.
  for (const id of ["fx-s06", "fx-s11", "fx-s16", "fx-s21"]) at(id).blockers = [{ text: "Lint configuration rejects the fixture module.", cite: { edit: "src/fx/lint-config.json" } }];
  // Not a candidate: an uncited blocker in three roots.
  for (const id of ["fx-s17", "fx-s18", "fx-s19"]) at(id).blockers = [{ text: "Preview deploy is unavailable.", cite: { uncited: true } }];
  // Not a candidate: a failed verification whose cited command did not fail (no failure evidence), in three roots.
  failVerify("fx-s22", "The build check reported a problem.", { tool: "Bash", command: "bun run build", isError: false });
  failVerify("fx-s23", "The build check reported a problem.", { tool: "Bash", command: "bun run build", isError: false });
  failVerify("fx-s24", "The build check reported a problem.", { tool: "Bash", command: "bun run build", isError: false });
  return out;
}

export function makeCohort(): unknown {
  const good = specs();
  const sessions: unknown[] = good.map((s, i) => ({
    status: "analyzed", workspace: FIXTURE_WORKSPACE, source: s.source, sessionId: s.sessionId,
    ...(s.projectId ? { projectId: s.projectId } : {}), ...(s.repo ? { repo: s.repo } : {}), result: fixtureResult(s, i),
  }));
  const bad = (sessionId: string, i: number, mutate: (r: SessionAnalysisResult & Record<string, unknown>) => unknown) => {
    const r = fixtureResult({ sessionId, source: "fx-claude", repo: ALPHA, sessionType: "implementation", outcomes: [{ status: "implemented", verification: "passed" }], usage: "cost", indexed: true }, i) as SessionAnalysisResult & Record<string, unknown>;
    sessions.push({ status: "analyzed", workspace: FIXTURE_WORKSPACE, source: "fx-claude", sessionId, repo: ALPHA, result: mutate(r) });
  };
  // Analyzed entries the synthesis must refuse to trust, one per derived skip reason.
  bad("fx-bad-dangling", 3, (r) => { r.analysis.understanding.blockers = [claim("Fixture blocker citing nothing real.", ["event:fx-claude:fx-bad-dangling:missing"])]; return r; });
  bad("fx-bad-identity", 4, (r) => { r.analysis.sessionId = "fx-someone-else"; return r; });
  bad("fx-bad-schema", 5, (r) => { (r.analysis as unknown as { schemaVersion: number }).schemaVersion = 3; return r; });
  bad("fx-bad-version", 6, (r) => { r.analysisVersion = "session-analysis-v1.0:fx-old"; return r; });
  bad("fx-bad-window", 7, (r) => { r.analysis.analyzedAt = "2026-10-30T12:00:00.000Z"; return r; });
  bad("fx-bad-null", 8, () => null);
  const reasons: SkipReason[] = ["not_analyzed", "failed", "unsupported", "invalid", "too_large", "stale_version", "out_of_window"];
  reasons.forEach((reason, i) => sessions.push({ status: "skipped", workspace: FIXTURE_WORKSPACE, source: i % 2 ? "fx-codex" : "fx-claude", sessionId: `fx-skip-${reason.replace(/_/gu, "-")}`, reason }));
  // A fixed shuffle, so the fixture is not already in output order.
  const order = sessions.map((s, i) => ({ s, k: (i * 37) % sessions.length })).sort((a, b) => a.k - b.k).map((x) => x.s);
  return { format: COHORT_FORMAT, workspace: FIXTURE_WORKSPACE, window: FIXTURE_WINDOW, eligible: 60, sessions: order };
}

export const cohortJson = () => `${JSON.stringify(makeCohort(), null, 1)}\n`;

if (import.meta.main) writeFileSync(new URL("./cohort.json", import.meta.url), cohortJson());
