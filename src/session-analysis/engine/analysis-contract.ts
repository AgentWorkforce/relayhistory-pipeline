import {
  SESSION_ANALYSIS_SCHEMA_VERSION,
  SESSION_ANALYSIS_TIMEOUT_MS,
  EVIDENCE_KINDS,
  type EvidenceKind,
  type ClaimConfidence,
  type ClaimBasis,
  type SessionState,
  type SessionType,
  type OutcomeStatus,
  type VerificationStatus,
  type AnalysisClaim,
  type AnalysisClassification,
  type SessionOutcome,
  type SessionSemanticAnalysis,
  type SessionEvidenceReference,
  type EvidenceCoverage,
  type EvidenceBundle,
  type SessionToolUsage,
  type SessionFileChangeSummary,
  type SessionUsageSummary,
  type SessionInteractionPeriod,
  type SessionAnalysisMetrics,
  type SessionSemanticInput,
  type SessionAnalysisBackendTelemetry,
  validateEvidenceBundle,
  validateSemanticAnalysis,
} from "./session-analysis-result.ts";
export {
  SESSION_ANALYSIS_SCHEMA_VERSION,
  SESSION_ANALYSIS_TIMEOUT_MS,
  EVIDENCE_KINDS,
  type EvidenceKind,
  type ClaimConfidence,
  type ClaimBasis,
  type SessionState,
  type SessionType,
  type OutcomeStatus,
  type VerificationStatus,
  type AnalysisClaim,
  type AnalysisClassification,
  type SessionOutcome,
  type SessionSemanticAnalysis,
  type SessionEvidenceReference,
  type EvidenceCoverage,
  type EvidenceBundle,
  type SessionToolUsage,
  type SessionFileChangeSummary,
  type SessionUsageSummary,
  type SessionInteractionPeriod,
  type SessionAnalysisMetrics,
  type SessionSemanticInput,
  type SessionAnalysisBackendTelemetry,
  validateEvidenceBundle,
  validateSemanticAnalysis,
} from "./session-analysis-result.ts";
export const SESSION_ANALYSIS_TITLE_SCHEMA = {
  type: "string",
  minLength: 1,
  maxLength: 90,
} as const;

export function validateAnalysisTitle(value: unknown): string {
  if (
    typeof value !== "string" ||
    !value.trim() ||
    value.length > 90 ||
    /[\r\n]/.test(value)
  ) {
    throw new TypeError(
      "Analysis title must be a non-empty single line of at most 90 characters",
    );
  }
  return value.trim();
}

type Schema = {
  type?:
    | "object"
    | "array"
    | "string"
    | "number"
    | "integer"
    | "boolean"
    | "null";
  properties?: Record<string, Schema>;
  required?: readonly string[];
  additionalProperties?: boolean;
  items?: Schema;
  enum?: readonly string[];
  anyOf?: readonly Schema[];
  maxItems?: number;
  minLength?: number;
  maxLength?: number;
  minimum?: number;
};
const string = (maxLength = 10_000): Schema => ({
  type: "string",
  minLength: 1,
  maxLength,
});
const enumeration = (values: readonly string[]): Schema => ({
  type: "string",
  enum: values,
});
const array = (items: Schema, maxItems = 100): Schema => ({
  type: "array",
  items,
  maxItems,
});
const nullable = (schema: Schema): Schema => ({
  anyOf: [schema, { type: "null" }],
});
const object = (properties: Record<string, Schema>): Schema => ({
  type: "object",
  additionalProperties: false,
  required: Object.keys(properties),
  properties,
});
const count: Schema = { type: "integer", minimum: 0 };
const ids = array(string(1_024));
const support = {
  evidenceIds: ids,
  confidence: enumeration(["high", "medium", "low"]),
  basis: enumeration(["observed", "inferred", "unknown"]),
};
const claim = object({ text: string(), ...support });
const classification = (values: readonly string[]) =>
  object({ value: enumeration(values), explanation: string(), ...support });

export const SESSION_ANALYSIS_JSON_SCHEMA = object({
  description: claim,
  intents: array(claim),
  constraints: array(claim),
  outcome: claim,
  outcomes: array(
    object({
      id: string(128),
      intent: claim,
      status: enumeration([
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
      changes: array(claim),
      verification: classification([
        "not-observed",
        "passed",
        "failed",
        "inconclusive",
      ]),
      remaining: array(claim),
      contradictions: array(claim),
      supersededBy: nullable(string(128)),
    }),
  ),
  codeChangeAssessment: claim,
  decisions: array(claim),
  testing: array(claim),
  risks: array(claim),
  blockers: array(claim),
  skillsUsed: array(claim),
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

const EVIDENCE_REFERENCE_SCHEMA = object({
  id: string(1_024),
  source: string(64),
  sessionId: string(512),
  kind: enumeration(EVIDENCE_KINDS),
  at: nullable(string(64)),
  summary: string(1_200),
  detail: nullable(string(4_000)),
  truncated: { type: "boolean" },
  isError: nullable({ type: "boolean" }),
  toolCallId: nullable(string(1_024)),
  locator: object({
    eventUid: nullable(string(512)),
    toolUseId: nullable(string(512)),
    fileEditId: nullable(count),
  }),
});

const EVIDENCE_BUNDLE_SCHEMA = object({
  references: array(EVIDENCE_REFERENCE_SCHEMA, 500),
  coverage: object({
    byKind: array(
      object({
        kind: enumeration(EVIDENCE_KINDS),
        available: count,
        included: count,
        truncated: count,
      }),
      EVIDENCE_KINDS.length,
    ),
    metricsOmitted: object({
      models: count,
      tools: count,
      filesChanged: count,
      interactionPeriods: count,
    }),
    metricsShortened: count,
    treeTruncated: { type: "boolean" },
    unlinkedDelegationCount: count,
    hydrationStatus: string(64),
    diagnosticCount: count,
    sourceStamp: nullable(string(2_048)),
    indexedThroughMs: nullable({ type: "number", minimum: 0 }),
  }),
});

const nonnegative: Schema = { type: "number", minimum: 0 };
const METRICS_SCHEMA = object({
  models: array(string(128), 32),
  usage: object({
    observed: { type: "boolean" },
    inputTokens: nonnegative,
    outputTokens: nonnegative,
    cachedInputTokens: nonnegative,
    totalTokens: nonnegative,
    costUSD: nullable(nonnegative),
  }),
  tools: array(
    object({
      name: string(256),
      count: nonnegative,
      errorCount: nonnegative,
      targets: array(string(1_024), 10),
    }),
  ),
  filesChanged: array(
    object({
      path: string(2_048),
      editCount: nonnegative,
      linesAdded: nullable(nonnegative),
      linesRemoved: nullable(nonnegative),
      userModified: nullable({ type: "boolean" }),
    }),
    500,
  ),
  interactionPeriods: array(
    object({
      startedAt: string(64),
      endedAt: string(64),
      durationMs: nonnegative,
      triggerEvidenceId: string(1_024),
    }),
  ),
  longestInteractionPeriodMs: nullable(nonnegative),
});

/** Reject undeclared fields at the local upload boundary as well as on the server. */
export function validateSemanticRequest(value: unknown): SessionSemanticInput {
  validateShape(
    value,
    object({
      source: string(64),
      sessionId: string(512),
      capability: enumeration(["full", "partial", "shallow_only"]),
      ...EVIDENCE_BUNDLE_SCHEMA.properties,
      metrics: METRICS_SCHEMA,
    }),
    "input",
  );
  const input = value as SessionSemanticInput;
  validateEvidenceBundle(
    { references: input.references, coverage: input.coverage },
    input.source,
    input.sessionId,
  );
  return input;
}

function validateShape(value: unknown, schema: Schema, path: string): void {
  if (schema.anyOf) {
    if (value === null && schema.anyOf.some((option) => option.type === "null"))
      return;
    return validateShape(
      value,
      schema.anyOf.find((option) => option.type !== "null")!,
      path,
    );
  }
  if (schema.type === "object") {
    if (!value || typeof value !== "object" || Array.isArray(value))
      fail(path, "expected an object");
    const record = value as Record<string, unknown>;
    if (
      Object.keys(record).some((key) => !Object.hasOwn(schema.properties!, key))
    )
      fail(path, "undeclared field");
    for (const [key, child] of Object.entries(schema.properties!))
      validateShape(record[key], child, `${path}.${key}`);
  } else if (schema.type === "array") {
    if (!Array.isArray(value) || value.length > schema.maxItems!)
      fail(path, "invalid array");
    value.forEach((item, index) =>
      validateShape(item, schema.items!, `${path}[${index}]`),
    );
  } else if (schema.type === "string") {
    if (
      typeof value !== "string" ||
      !value.trim() ||
      (schema.maxLength !== undefined && value.length > schema.maxLength) ||
      (schema.enum && !schema.enum.includes(value))
    )
      fail(path, "invalid string");
  } else if (schema.type === "boolean") {
    if (typeof value !== "boolean") fail(path, "expected a boolean");
  } else if (schema.type === "integer" || schema.type === "number") {
    if (
      typeof value !== "number" ||
      !Number.isFinite(value) ||
      value < (schema.minimum ?? -Infinity) ||
      (schema.type === "integer" && !Number.isSafeInteger(value))
    )
      fail(path, "invalid number");
  } else if (value !== null) fail(path, "expected null");
}

function fail(path: string, message: string): never {
  throw new Error(`${path}: ${message}`);
}
