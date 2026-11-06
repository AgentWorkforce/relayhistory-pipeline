import { z } from "zod";
/** Pure JSON contract shared by the local SDK and the Worker. No provider or Node imports. */
export const SESSION_ANALYSIS_SCHEMA_VERSION = 2 as const;
/** One model request. Clients allow additional time for authentication and persistence. */
export const SESSION_ANALYSIS_TIMEOUT_MS = 120_000;
export const EVIDENCE_KINDS = [
  "human-message",
  "delegated-instruction",
  "assistant-message",
  "tool-call",
  "tool-result",
  "file-edit",
  "delegation",
] as const;
export type EvidenceKind = (typeof EVIDENCE_KINDS)[number];
export type ClaimConfidence = "high" | "medium" | "low";
export type ClaimBasis = "observed" | "inferred" | "unknown";
export type SessionState =
  | "idle"
  | "working"
  | "blocked-by-agent"
  | "blocked-by-human"
  | "unknown";
export type SessionType =
  | "planning"
  | "implementation"
  | "debugging"
  | "review"
  | "testing"
  | "research"
  | "conversation"
  | "mixed"
  | "unknown";
export type OutcomeStatus =
  | "not-started"
  | "in-progress"
  | "implemented"
  | "reported-complete"
  | "verified"
  | "blocked"
  | "superseded"
  | "unknown";
export type VerificationStatus =
  | "not-observed"
  | "passed"
  | "failed"
  | "inconclusive";

export interface AnalysisClaim {
  text: string;
  evidenceIds: string[];
  confidence: ClaimConfidence;
  basis: ClaimBasis;
}

export interface AnalysisClassification<T extends string> {
  value: T;
  explanation: string;
  evidenceIds: string[];
  confidence: ClaimConfidence;
  basis: ClaimBasis;
}

export interface SessionOutcome {
  /** Unique within this snapshot. Use request evidence, not this ID, to match across snapshots. */
  id: string;
  intent: AnalysisClaim;
  status: OutcomeStatus;
  assessment: AnalysisClaim;
  changes: AnalysisClaim[];
  verification: AnalysisClassification<VerificationStatus>;
  remaining: AnalysisClaim[];
  contradictions: AnalysisClaim[];
  supersededBy: string | null;
}

export interface SessionSemanticAnalysis {
  description: AnalysisClaim;
  intents: AnalysisClaim[];
  constraints: AnalysisClaim[];
  outcome: AnalysisClaim;
  outcomes: SessionOutcome[];
  codeChangeAssessment: AnalysisClaim;
  decisions: AnalysisClaim[];
  testing: AnalysisClaim[];
  risks: AnalysisClaim[];
  blockers: AnalysisClaim[];
  skillsUsed: AnalysisClaim[];
  state: AnalysisClassification<SessionState>;
  sessionType: AnalysisClassification<SessionType>;
}

export interface SessionEvidenceReference {
  id: string;
  source: string;
  kind: EvidenceKind;
  sessionId: string;
  at: string | null;
  summary: string;
  /** Bounded command arguments, tool output, or patch excerpt. Whitespace is preserved. */
  detail: string | null;
  truncated: boolean;
  isError: boolean | null;
  /** Only set when normalized history supplies an unambiguous link. */
  toolCallId: string | null;
  locator: {
    eventUid: string | null;
    toolUseId: string | null;
    fileEditId: number | null;
  };
}

export interface EvidenceCoverage {
  byKind: Array<{
    kind: EvidenceKind;
    available: number;
    included: number;
    truncated: number;
  }>;
  metricsOmitted: {
    models: number;
    tools: number;
    filesChanged: number;
    interactionPeriods: number;
  };
  metricsShortened: number;
  treeTruncated: boolean;
  unlinkedDelegationCount: number;
  hydrationStatus: string;
  diagnosticCount: number;
  sourceStamp: string | null;
  indexedThroughMs: number | null;
}

export interface EvidenceBundle {
  references: SessionEvidenceReference[];
  coverage: EvidenceCoverage;
}

export interface SessionToolUsage {
  name: string;
  count: number;
  errorCount: number;
  targets: string[];
}

export interface SessionFileChangeSummary {
  path: string;
  editCount: number;
  linesAdded: number | null;
  linesRemoved: number | null;
  userModified: boolean | null;
}

export interface SessionUsageSummary {
  observed: boolean;
  inputTokens: number;
  outputTokens: number;
  cachedInputTokens: number;
  totalTokens: number;
  costUSD: number | null;
}

export interface SessionInteractionPeriod {
  startedAt: string;
  endedAt: string;
  durationMs: number;
  triggerEvidenceId: string;
}

export interface SessionAnalysisMetrics {
  models: string[];
  usage: SessionUsageSummary;
  tools: SessionToolUsage[];
  filesChanged: SessionFileChangeSummary[];
  interactionPeriods: SessionInteractionPeriod[];
  longestInteractionPeriodMs: number | null;
}

export interface SessionSemanticInput extends EvidenceBundle {
  source: string;
  sessionId: string;
  capability: "full" | "partial" | "shallow_only";
  metrics: SessionAnalysisMetrics;
}

export interface SessionAnalysisBackendTelemetry {
  requestId: string;
  model: string;
  inputTokens: number;
  outputTokens: number;
  totalTokens: number;
  modelDurationMs: number | null;
  totalDurationMs: number | null;
}

export interface SessionAnalysisSnapshot {
  schemaVersion: typeof SESSION_ANALYSIS_SCHEMA_VERSION;
  title: string;
  description: string;
  snapshotId: string;
  inputFingerprint: string;
  source: string;
  sessionId: string;
  analyzedAt: string;
  analyzerVersion: string;
  backend: SessionAnalysisBackendTelemetry | null;
  evidence: {
    capability: "full" | "partial" | "shallow_only";
    discoveryState: string;
    sourceStamp: string | null;
    indexedThroughMs: number | null;
    rootEventCount: number;
    eventCount: number;
    toolCallCount: number;
    fileEditCount: number;
    relatedSessionIds: string[];
    unlinkedDelegationCount: number;
    treeTruncated: boolean;
    references: SessionEvidenceReference[];
    coverage: EvidenceCoverage;
    diagnostics: unknown[];
  };
  metrics: SessionAnalysisMetrics;
  understanding: SessionSemanticAnalysis;
}

const text = (max = 10_000) =>
  z
    .string()
    .max(max)
    .refine((value) => Boolean(value.trim()));
const count = z.number().int().nonnegative();
const metric = z.number().nonnegative();
const timestamp = text(64).refine((value) =>
  Number.isFinite(Date.parse(value)),
);
const support = {
  evidenceIds: z.array(text(1024)).max(100),
  confidence: z.enum(["high", "medium", "low"]),
  basis: z.enum(["observed", "inferred", "unknown"]),
};
const claim = z.strictObject({ text: text(), ...support });
const claims = z.array(claim).max(100);
const classification = <T extends string>(values: readonly T[]) =>
  z.strictObject({ value: z.enum(values), explanation: text(), ...support });
const semanticAnalysis = z.strictObject({
  description: claim,
  intents: claims,
  constraints: claims,
  outcome: claim,
  outcomes: z
    .array(
      z.strictObject({
        id: text(128),
        intent: claim,
        status: z.enum([
          "not-started",
          "in-progress",
          "implemented",
          "reported-complete",
          "verified",
          "blocked",
          "superseded",
          "unknown",
        ]),
        assessment: claim,
        changes: claims,
        verification: classification([
          "not-observed",
          "passed",
          "failed",
          "inconclusive",
        ]),
        remaining: claims,
        contradictions: claims,
        supersededBy: text(128).nullable(),
      }),
    )
    .max(100),
  codeChangeAssessment: claim,
  decisions: claims,
  testing: claims,
  risks: claims,
  blockers: claims,
  skillsUsed: claims,
  state: classification([
    "idle",
    "working",
    "blocked-by-agent",
    "blocked-by-human",
    "unknown",
  ]),
  sessionType: classification([
    "planning",
    "implementation",
    "debugging",
    "review",
    "testing",
    "research",
    "conversation",
    "mixed",
    "unknown",
  ]),
});
const evidenceReference = z.strictObject({
  id: text(1024),
  source: text(64),
  kind: z.enum(EVIDENCE_KINDS),
  sessionId: text(512),
  at: timestamp.nullable(),
  summary: text(1200),
  detail: text(4000).nullable(),
  truncated: z.boolean(),
  isError: z.boolean().nullable(),
  toolCallId: text(1024).nullable(),
  locator: z.strictObject({
    eventUid: text(512).nullable(),
    toolUseId: text(512).nullable(),
    fileEditId: count.nullable(),
  }),
});
const coverage = z.strictObject({
  byKind: z
    .array(
      z.strictObject({
        kind: z.enum(EVIDENCE_KINDS),
        available: count,
        included: count,
        truncated: count,
      }),
    )
    .max(EVIDENCE_KINDS.length),
  metricsOmitted: z.strictObject({
    models: count,
    tools: count,
    filesChanged: count,
    interactionPeriods: count,
  }),
  metricsShortened: count,
  treeTruncated: z.boolean(),
  unlinkedDelegationCount: count,
  hydrationStatus: text(64),
  diagnosticCount: count,
  sourceStamp: text(2048).nullable(),
  indexedThroughMs: metric.nullable(),
});
const evidenceBundle = z.strictObject({
  references: z.array(evidenceReference).max(500),
  coverage,
});
const metrics = z.strictObject({
  models: z.array(text(Number.MAX_SAFE_INTEGER)),
  usage: z.strictObject({
    observed: z.boolean(),
    inputTokens: metric,
    outputTokens: metric,
    cachedInputTokens: metric,
    totalTokens: metric,
    costUSD: metric.nullable(),
  }),
  tools: z.array(
    z.strictObject({
      name: text(Number.MAX_SAFE_INTEGER),
      count: metric,
      errorCount: metric,
      targets: z.array(text(Number.MAX_SAFE_INTEGER)),
    }),
  ),
  filesChanged: z.array(
    z.strictObject({
      path: text(Number.MAX_SAFE_INTEGER),
      editCount: metric,
      linesAdded: metric.nullable(),
      linesRemoved: metric.nullable(),
      userModified: z.boolean().nullable(),
    }),
  ),
  interactionPeriods: z.array(
    z.strictObject({
      startedAt: timestamp,
      endedAt: timestamp,
      durationMs: metric,
      triggerEvidenceId: text(Number.MAX_SAFE_INTEGER),
    }),
  ),
  longestInteractionPeriodMs: metric.nullable(),
});
const snapshot = z.strictObject({
  schemaVersion: z.literal(2),
  title: text(90),
  description: text(),
  snapshotId: text(1024),
  inputFingerprint: text(1024),
  source: text(64),
  sessionId: text(512),
  analyzedAt: timestamp,
  analyzerVersion: text(128),
  backend: z
    .strictObject({
      requestId: text(1024),
      model: text(256),
      inputTokens: metric,
      outputTokens: metric,
      totalTokens: metric,
      modelDurationMs: metric.nullable(),
      totalDurationMs: metric.nullable(),
    })
    .nullable(),
  evidence: z.strictObject({
    capability: z.enum(["full", "partial", "shallow_only"]),
    discoveryState: text(64),
    sourceStamp: text(2048).nullable(),
    indexedThroughMs: metric.nullable(),
    rootEventCount: count,
    eventCount: count,
    toolCallCount: count,
    fileEditCount: count,
    relatedSessionIds: z.array(text(512)),
    unlinkedDelegationCount: count,
    treeTruncated: z.boolean(),
    references: evidenceBundle.shape.references,
    coverage,
    diagnostics: z.array(z.unknown()),
  }),
  metrics,
  understanding: semanticAnalysis,
});
const envelope = z.strictObject({
  schemaVersion: z.literal(1),
  analysisVersion: text(128),
  sourceFingerprint: text(2048),
  analysis: snapshot,
  durationMs: metric,
});
export interface SessionAnalysisResult {
  schemaVersion: 1;
  analysisVersion: string;
  sourceFingerprint: string;
  analysis: SessionAnalysisSnapshot;
  durationMs: number;
}
export type SessionAnalysisDecodeResult =
  | { status: "ready"; result: SessionAnalysisResult }
  | { status: "empty" | "unsupported" | "malformed" };
/** Stored results stay JSON on /brief. Unknown versions cannot break the independent brief. */
export function decodeSessionAnalysisResult(
  value: unknown,
  identity: { source: string; sessionId: string },
): SessionAnalysisDecodeResult {
  if (value === null) return { status: "empty" };
  if (value && typeof value === "object" && "schemaVersion" in value) {
    if (value.schemaVersion !== 1) return { status: "unsupported" };
    const nested = "analysis" in value ? value.analysis : null;
    if (
      nested &&
      typeof nested === "object" &&
      "schemaVersion" in nested &&
      nested.schemaVersion !== 2
    )
      return { status: "unsupported" };
  }
  try {
    const result = envelope.parse(value) as SessionAnalysisResult;
    const analysis = result.analysis;
    if (
      analysis.source !== identity.source ||
      analysis.sessionId !== identity.sessionId
    )
      return { status: "malformed" };
    validateEvidenceBundle(
      {
        references: analysis.evidence.references,
        coverage: analysis.evidence.coverage,
      },
      identity.source,
      identity.sessionId,
    );
    validateSemanticAnalysis(
      analysis.understanding,
      analysis.evidence.references,
    );
    return { status: "ready", result };
  } catch {
    return { status: "malformed" };
  }
}
/** Validate the allow-listed payload before it reaches a model. */
export function validateEvidenceBundle(
  value: unknown,
  source: string,
  rootSessionId: string,
): EvidenceBundle {
  evidenceBundle.parse(value);
  const bundle = value as EvidenceBundle;
  const byId = new Map(
    bundle.references.map((reference) => [reference.id, reference]),
  );
  if (byId.size !== bundle.references.length)
    fail("evidence.references", "duplicate IDs");
  for (const reference of bundle.references) {
    if (reference.source !== source)
      fail(reference.id, "source differs from the session");
    if (
      reference.kind === "human-message" &&
      reference.sessionId !== rootSessionId
    )
      fail(reference.id, "child instructions are not human intent");
    if (reference.at !== null && !Number.isFinite(Date.parse(reference.at)))
      fail(reference.id, "invalid timestamp");
    if (reference.toolCallId !== null) {
      const tool = byId.get(reference.toolCallId);
      if (
        !tool ||
        tool.kind !== "tool-call" ||
        tool.sessionId !== reference.sessionId
      )
        fail(reference.id, "invalid tool-call link");
    }
  }
  const counts = new Map(
    bundle.coverage.byKind.map((entry) => [entry.kind, entry]),
  );
  if (
    counts.size !== EVIDENCE_KINDS.length ||
    counts.size !== bundle.coverage.byKind.length
  )
    fail("coverage.byKind", "each kind is required exactly once");
  for (const kind of EVIDENCE_KINDS) {
    const entry = counts.get(kind)!;
    const included = bundle.references.filter(
      (reference) => reference.kind === kind,
    );
    if (
      entry.available < entry.included ||
      entry.included !== included.length ||
      entry.truncated !==
        included.filter((reference) => reference.truncated).length
    )
      fail("coverage.byKind", "counts do not match evidence");
  }
  return bundle;
}

/** Validate the data format and reference integrity. Interpretation quality belongs in evals, not request rejection. */
export function validateSemanticAnalysis(
  value: unknown,
  references: readonly SessionEvidenceReference[],
): SessionSemanticAnalysis {
  semanticAnalysis.parse(value);
  const analysis = value as SessionSemanticAnalysis;
  const evidence = new Set(references.map((reference) => reference.id));
  const check = (item: Pick<AnalysisClaim, "evidenceIds">, path: string) => {
    for (const id of item.evidenceIds)
      if (!evidence.has(id)) fail(path, `unknown evidence ${id}`);
  };
  for (const key of [
    "description",
    "outcome",
    "codeChangeAssessment",
    "state",
    "sessionType",
  ] as const)
    check(analysis[key], key);
  for (const key of [
    "intents",
    "constraints",
    "decisions",
    "testing",
    "risks",
    "blockers",
    "skillsUsed",
  ] as const)
    analysis[key].forEach((item, i) => check(item, `${key}[${i}]`));
  const outcomes = new Map(
    analysis.outcomes.map((outcome) => [outcome.id, outcome]),
  );
  if (outcomes.size !== analysis.outcomes.length)
    fail("outcomes", "duplicate IDs");
  for (const outcome of analysis.outcomes) {
    check(outcome.intent, `${outcome.id}.intent`);
    check(outcome.assessment, `${outcome.id}.assessment`);
    for (const key of ["changes", "remaining", "contradictions"] as const)
      outcome[key].forEach((item, i) =>
        check(item, `${outcome.id}.${key}[${i}]`),
      );
    check(outcome.verification, `${outcome.id}.verification`);
    if (outcome.supersededBy !== null) {
      const replacement = outcomes.get(outcome.supersededBy);
      if (!replacement || replacement.id === outcome.id)
        fail(outcome.id, "superseded outcomes require a replacement outcome");
    }
    const seen = new Set<string>([outcome.id]);
    let next = outcome.supersededBy;
    while (next !== null) {
      if (seen.has(next)) fail(outcome.id, "cyclic supersession");
      seen.add(next);
      next = outcomes.get(next)?.supersededBy ?? null;
    }
  }
  return analysis;
}

function fail(path: string, message: string): never {
  throw new Error(`${path}: ${message}`);
}
