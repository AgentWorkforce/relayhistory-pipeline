/**
 * One full-analysis job: relayhistory processor.ts `processSessionAnalysis` and
 * `runSessionJob` (kind "analysis"), with two adaptations:
 *
 *   1. Evidence. relayhistory froze the whole evidence snapshot into the job row's
 *      jsonb `source` at collection time. Transcript text may not live in Postgres here
 *      (0004 rule), so the job row carries only the scope, and the evidence is read from
 *      canonical S3 when the job runs (evidence.ts). The result records the fingerprint
 *      and source generation it was produced from, so a reader can tell which evidence a
 *      result describes, exactly as relayhistory's `sourceFingerprint` did.
 *   2. Result. relayhistory stored the result jsonb on the job row. Here it is written to
 *      object storage (results.ts) and the job row keeps only its key, SHA-256 and size.
 *
 * The prompt, model, schema, result envelope and the error classification (controlled
 * codes only, never model content or exception text) are relayhistory's, unchanged.
 */
import { buildSessionAnalysisSnapshot } from "./engine/snapshot.ts";
import type { AnalysisModelTransport } from "./engine/model.ts";
import type { SessionAnalysisResult } from "./engine/session-analysis-result.ts";
import { summaryInput } from "../session-brief/engine/summary-input.ts";
import { SessionAnalysisProviderError } from "../session-brief/engine/transport.ts";
import { CanonicalReadError } from "../session-read-api/canonical.ts";
import {
  loadSessionSource,
  SessionSourceIncompleteError,
  SessionSourceNotFoundError,
  type AnalysisEvidenceDeps,
} from "./evidence.ts";
import {
  SESSION_ANALYSIS_ENVELOPE_VERSION,
  SESSION_ANALYSIS_RESULT_SCHEMA_VERSION,
  SESSION_ANALYSIS_VERSION,
} from "./versions.ts";

/** A claimed analysis job, as the claim RPC returns it. No evidence, no text. */
export interface AnalysisJob {
  id: string;
  orgId: string;
  workspaceId: string;
  sessionId: string;
  analysisVersion: string;
  schemaVersion: number;
  modelVersion: string;
  attempts: number;
  leaseToken: string;
}

export interface AnalysisJobOutput {
  result: SessionAnalysisResult;
  harness: string;
  sourceFingerprint: string;
  sourceGeneration: string;
  inputTokens: number;
  outputTokens: number;
}

export class UnsupportedAnalysisJobError extends Error {}

/** relayhistory processSessionAnalysis over canonical evidence. */
export async function processSessionAnalysis(
  job: AnalysisJob,
  deps: { evidence: AnalysisEvidenceDeps; transport: AnalysisModelTransport },
): Promise<AnalysisJobOutput> {
  if (
    job.analysisVersion !== SESSION_ANALYSIS_VERSION ||
    job.schemaVersion !== SESSION_ANALYSIS_RESULT_SCHEMA_VERSION
  ) {
    throw new UnsupportedAnalysisJobError("unsupported_analysis_version");
  }
  const source = await loadSessionSource(deps.evidence, {
    orgId: job.orgId,
    workspaceId: job.workspaceId,
    sessionId: job.sessionId,
  });
  // relayhistory assertOwnedSource: the evidence must be the job's own scope.
  if (
    source.organizationId !== job.orgId ||
    source.workspaceId !== job.workspaceId ||
    source.sessionId !== job.sessionId
  )
    throw new UnsupportedAnalysisJobError("invalid_source_snapshot");
  // relayhistory request-processor: "Every analyzed session must also be briefable."
  try {
    summaryInput(source.sourceProvider, source.sessionId, source.evidence.events);
  } catch {
    throw new UnsupportedAnalysisJobError("session_analysis_ineligible");
  }
  const startedAt = Date.now();
  const analysis = await buildSessionAnalysisSnapshot(
    source.evidence,
    source.hydration,
    deps.transport,
    {
      source: source.sourceProvider,
      sessionId: source.sessionId,
      model: job.modelVersion,
      requestId: job.id,
    },
  );
  return {
    result: {
      schemaVersion: SESSION_ANALYSIS_ENVELOPE_VERSION,
      analysisVersion: job.analysisVersion,
      sourceFingerprint: source.sourceFingerprint,
      analysis,
      durationMs: Date.now() - startedAt,
    },
    harness: source.sourceProvider,
    sourceFingerprint: source.sourceFingerprint,
    sourceGeneration: source.sourceGeneration,
    inputTokens: analysis.backend?.inputTokens ?? 0,
    outputTokens: analysis.backend?.outputTokens ?? 0,
  };
}

/** The result written to object storage, as the job row then records it. */
export interface StoredAnalysisResult {
  key: string;
  sha256: string;
  bytes: number;
}

export interface AnalysisJobStore {
  claim(options: { jobId?: string; leaseSeconds: number }): Promise<AnalysisJob | null>;
  /** Write the result object. Called only while the lease is held. */
  writeResult(job: AnalysisJob, output: AnalysisJobOutput): Promise<StoredAnalysisResult>;
  /** Lease-fenced; false when the lease was lost. */
  complete(input: {
    jobId: string;
    leaseToken: string;
    output: AnalysisJobOutput;
    stored: StoredAnalysisResult;
  }): Promise<boolean>;
  /** Lease-fenced; false when the lease was lost. `error` is a controlled code. */
  fail(input: {
    jobId: string;
    leaseToken: string;
    error: string;
    retryable: boolean;
  }): Promise<boolean>;
}

/** relayhistory repository.ts: the analysis lease. */
export const ANALYSIS_LEASE_SECONDS = 600;

/** A controlled failure code and whether it is retried (relayhistory runSessionJob). */
export function classifyAnalysisFailure(error: unknown): { code: string; retryable: boolean } {
  if (error instanceof UnsupportedAnalysisJobError) return { code: error.message, retryable: false };
  if (error instanceof SessionSourceNotFoundError) return { code: "session_not_found", retryable: false };
  if (error instanceof SessionSourceIncompleteError)
    return { code: "session_evidence_incomplete", retryable: false };
  // The canonical read refused. Unscrubbed raw is quarantined and never retried (the
  // brief stage's `session_quarantined`); anything else (a generation change mid-read, a
  // missing object, a transient store error) is retried within the attempt bound.
  if (error instanceof CanonicalReadError)
    return error.details.includes("manifest_unscrubbed")
      ? { code: "session_quarantined", retryable: false }
      : { code: "session_evidence_unavailable", retryable: true };
  if (error instanceof SessionAnalysisProviderError)
    return { code: error.code, retryable: error.retryable };
  if (error instanceof TypeError) return { code: "model_output_invalid", retryable: false };
  const retryable =
    error && typeof error === "object" && "retryable" in error ? error.retryable === true : true;
  return { code: "analysis_processing_failed", retryable };
}

/** One bounded job; process death is recovered by the lease. relayhistory runSessionJob. */
export async function runAnalysisJob(
  store: AnalysisJobStore,
  deps: { evidence: AnalysisEvidenceDeps; transport: AnalysisModelTransport },
  options: { jobId?: string; leaseSeconds?: number } = {},
): Promise<"idle" | "completed" | "failed" | "lease-lost"> {
  const job = await store.claim({
    ...(options.jobId ? { jobId: options.jobId } : {}),
    leaseSeconds: options.leaseSeconds ?? ANALYSIS_LEASE_SECONDS,
  });
  if (!job) return "idle";
  if (!job.leaseToken) throw new Error("Claimed analysis has no lease token");
  try {
    const output = await processSessionAnalysis(job, deps);
    const stored = await store.writeResult(job, output);
    return (await store.complete({ jobId: job.id, leaseToken: job.leaseToken, output, stored }))
      ? "completed"
      : "lease-lost";
  } catch (error) {
    // Persist only controlled codes, never model content, credentials or exception text.
    const { code, retryable } = classifyAnalysisFailure(error);
    const saved = await store.fail({ jobId: job.id, leaseToken: job.leaseToken, error: code, retryable });
    return saved ? "failed" : "lease-lost";
  }
}
