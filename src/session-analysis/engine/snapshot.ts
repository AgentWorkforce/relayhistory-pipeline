import {
  SESSION_ANALYSIS_SCHEMA_VERSION,
  validateAnalysisTitle,
  validateSemanticAnalysis,
  type EvidenceCoverage,
  type SessionAnalysisBackendTelemetry,
  type SessionAnalysisMetrics,
  type SessionEvidenceReference,
  type SessionSemanticAnalysis,
} from "./analysis-contract.ts";
import { buildSemanticInput } from "./session-evidence.ts";
import { sessionMetrics } from "./session-metrics.ts";
import { analyzeSessionInput, type AnalysisModelTransport } from "./model.ts";
import type {
  CollectedEvidence,
  NormalizedHydration,
} from "../../session-brief/engine/normalized-history.ts";

import type { SessionAnalysisSnapshot } from "./session-analysis-result.ts";
export type { SessionAnalysisSnapshot } from "./session-analysis-result.ts";

export interface SessionAnalysisSnapshotOptions {
  source: string;
  sessionId: string;
  model: string;
  requestId?: string;
  modelDurationMs?: number | null;
  totalDurationMs?: number | null;
  analyzedAt?: Date;
}

export async function buildSessionAnalysisSnapshot(
  evidence: CollectedEvidence,
  hydration: NormalizedHydration,
  transport: AnalysisModelTransport,
  options: SessionAnalysisSnapshotOptions,
): Promise<SessionAnalysisSnapshot> {
  const totalStartedAt = performance.now();
  const analyzedAt = options.analyzedAt ?? new Date();
  // Skip's collector sorts globally before metrics, evidence sampling, and interaction periods.
  const ordered: CollectedEvidence = {
    ...evidence,
    events: [...evidence.events].sort((a, b) => a.tsMs - b.tsMs || a.id - b.id),
    tools: [...evidence.tools].sort(
      (a, b) =>
        (a.tsMs ?? Number.MAX_SAFE_INTEGER) -
          (b.tsMs ?? Number.MAX_SAFE_INTEGER) || a.id - b.id,
    ),
    edits: [...evidence.edits].sort(
      (a, b) =>
        (a.tsMs ?? Number.MAX_SAFE_INTEGER) -
          (b.tsMs ?? Number.MAX_SAFE_INTEGER) || a.id - b.id,
    ),
  };
  const metrics = sessionMetrics(options.sessionId, ordered);
  const input = buildSemanticInput(
    options.source,
    options.sessionId,
    ordered,
    hydration,
    metrics,
  );
  const modelStartedAt = performance.now();
  const modelResult = await analyzeSessionInput(
    input,
    transport,
    options.model,
  );
  const measuredModelDurationMs = Math.round(
    performance.now() - modelStartedAt,
  );
  const understanding = validateSemanticAnalysis(
    modelResult.analysis,
    input.references,
  );
  const inputFingerprint = await sha256Hex(JSON.stringify(input));
  const fingerprint = [
    options.source,
    options.sessionId,
    inputFingerprint,
    modelResult.analyzerVersion,
    analyzedAt.toISOString(),
  ].join(":");
  const snapshotId = `session-analysis-${await sha256Hex(fingerprint)}`;
  const measuredTotalDurationMs = Math.round(
    performance.now() - totalStartedAt,
  );
  return {
    schemaVersion: SESSION_ANALYSIS_SCHEMA_VERSION,
    title: validateAnalysisTitle(modelResult.title),
    description: understanding.description.text,
    snapshotId,
    inputFingerprint,
    source: options.source,
    sessionId: options.sessionId,
    analyzedAt: analyzedAt.toISOString(),
    analyzerVersion: modelResult.analyzerVersion,
    backend: {
      requestId:
        options.requestId ??
        modelResult.metering.providerRequestId ??
        snapshotId,
      model: modelResult.metering.model,
      inputTokens: modelResult.metering.inputTokens,
      outputTokens: modelResult.metering.outputTokens,
      totalTokens: modelResult.metering.totalTokens,
      modelDurationMs:
        options.modelDurationMs === undefined
          ? measuredModelDurationMs
          : options.modelDurationMs,
      totalDurationMs:
        options.totalDurationMs === undefined
          ? measuredTotalDurationMs
          : options.totalDurationMs,
    },
    evidence: {
      capability: hydration.capability,
      discoveryState: hydration.discoveryState ?? hydration.status,
      sourceStamp: hydration.indexedThrough.sourceStamp,
      indexedThroughMs: hydration.indexedThrough.lastEventAtMs,
      rootEventCount: ordered.events.filter(
        (event) => event.sessionId === options.sessionId,
      ).length,
      eventCount: ordered.events.length,
      toolCallCount: ordered.tools.length,
      fileEditCount: ordered.edits.length,
      relatedSessionIds: ordered.tree.nodes
        .slice(1)
        .map((node) => node.sessionId),
      unlinkedDelegationCount: ordered.tree.unlinked.length,
      treeTruncated: ordered.tree.truncated,
      references: input.references,
      coverage: input.coverage,
      diagnostics: hydration.diagnostics,
    },
    metrics,
    understanding,
  };
}

async function sha256Hex(value: string): Promise<string> {
  const digest = await crypto.subtle.digest(
    "SHA-256",
    new TextEncoder().encode(value),
  );
  return [...new Uint8Array(digest)]
    .map((byte) => byte.toString(16).padStart(2, "0"))
    .join("");
}
