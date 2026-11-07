/**
 * One analysis job: relayhistory processor.ts `processSessionAnalysis` +
 * `processSessionBrief` and `runSessionJob`, with these adaptations:
 *
 *   1. Evidence. relayhistory froze the whole evidence snapshot into the job row's
 *      jsonb `source` at collection time. Transcript text may not live in Postgres here
 *      (0004 rule), so the job row carries only the scope, and the evidence is read from
 *      canonical S3 when the job runs (evidence.ts). The result records the fingerprint
 *      and source generation it was produced from.
 *   2. One job, both halves. relayhistory linked a brief job and an analysis job to each
 *      request and served `a.result || {brief: b.result}`. Here one job reads the evidence
 *      once and produces both from that same read (so the brief can never describe another
 *      revision): the full analysis (gpt-5.4) and relayhistory's brief (gpt-5.6-luna,
 *      session-summary-v3), stored together as the v1 `result` with `brief`. The brief's
 *      `output` metrics (relayhistory getSessionOutput) are omitted until lane F4 serves them.
 *   3. Erase-safe publication. relayhistory stored the result jsonb on the job row. Here
 *      it is an object in storage, published by reserve -> put -> complete: `reserve`
 *      re-checks the lease, the session's liveness and the source revision and records a
 *      cleanup ticket in one transaction BEFORE any byte is written, so an erase that wins
 *      the race leaves a ticket the sweeper uses to delete the object; `complete` re-checks
 *      all three and retires the ticket in the same transaction it records the result.
 *
 * The prompts, models, schema, envelope and the error classification (controlled codes
 * only, never model content or exception text) are relayhistory's, unchanged.
 */
import { createHash } from "node:crypto";
import { buildSessionAnalysisSnapshot } from "./engine/snapshot.ts";
import { summarizeSessionInput, type AnalysisModelTransport } from "./engine/model.ts";
import type { SessionAnalysisResult } from "./engine/session-analysis-result.ts";
import { summaryInput } from "../session-brief/engine/summary-input.ts";
import { SESSION_SUMMARY_PROMPT_VERSION, SESSION_SUMMARY_VERSION } from "../session-brief/engine/summary-contract.ts";
import { SessionAnalysisProviderError } from "../session-brief/engine/transport.ts";
import { CanonicalReadError } from "../session-read-api/canonical.ts";
import {
  loadSessionSource,
  SessionSourceIncompleteError,
  SessionSourceNotFoundError,
  type AnalysisEvidenceDeps,
} from "./evidence.ts";
import { encodeAnalysisResult } from "./results.ts";
import {
  SESSION_ANALYSIS_ENVELOPE_VERSION,
  SESSION_ANALYSIS_RESULT_SCHEMA_VERSION,
  SESSION_ANALYSIS_SUMMARY_MODEL_VERSION,
  SESSION_ANALYSIS_SUMMARY_VERSION,
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
  summaryVersion: string;
  summaryModelVersion: string;
  attempts: number;
  leaseToken: string;
  /** The live pointer-set revision when the job was claimed (sessions.session_analysis_source_revision). */
  sourceRevision: string;
}

/** relayhistory processSessionBrief's result, from the same evidence read. */
export interface AnalysisBrief {
  schemaVersion: typeof SESSION_SUMMARY_VERSION;
  promptVersion: typeof SESSION_SUMMARY_PROMPT_VERSION;
  title: string;
  description: string;
  state: string;
  inputFingerprint: string;
  generatedAt: string;
  messageCount: number;
  shortenedMessages: number;
  backend: { model: string; inputTokens: number; outputTokens: number; durationMs: number };
}

export type AnalysisJobResult = SessionAnalysisResult & { brief: AnalysisBrief };

export interface AnalysisJobOutput {
  result: AnalysisJobResult;
  harness: string;
  sourceFingerprint: string;
  sourceGeneration: string;
  inputTokens: number;
  outputTokens: number;
}

export class UnsupportedAnalysisJobError extends Error {}

/** relayhistory processSessionAnalysis + processSessionBrief over one canonical evidence read. */
export async function processSessionAnalysis(
  job: AnalysisJob,
  deps: { evidence: AnalysisEvidenceDeps; transport: AnalysisModelTransport; assumeNoLegacyTurns?: boolean },
): Promise<AnalysisJobOutput> {
  if (
    job.analysisVersion !== SESSION_ANALYSIS_VERSION ||
    job.schemaVersion !== SESSION_ANALYSIS_RESULT_SCHEMA_VERSION ||
    job.summaryVersion !== SESSION_ANALYSIS_SUMMARY_VERSION ||
    job.summaryModelVersion !== SESSION_ANALYSIS_SUMMARY_MODEL_VERSION
  ) {
    throw new UnsupportedAnalysisJobError("unsupported_analysis_version");
  }
  const source = await loadSessionSource(
    deps.evidence,
    { orgId: job.orgId, workspaceId: job.workspaceId, sessionId: job.sessionId },
    { assumeNoLegacyTurns: deps.assumeNoLegacyTurns === true },
  );
  // relayhistory assertOwnedSource: the evidence must be the job's own scope.
  if (
    source.organizationId !== job.orgId ||
    source.workspaceId !== job.workspaceId ||
    source.sessionId !== job.sessionId
  )
    throw new UnsupportedAnalysisJobError("invalid_source_snapshot");
  // relayhistory request-processor: "Every analyzed session must also be briefable."
  let input;
  try {
    input = summaryInput(source.sourceProvider, source.sessionId, source.evidence.events);
  } catch {
    throw new UnsupportedAnalysisJobError("session_analysis_ineligible");
  }
  // relayhistory processor.ts summarize(), the brief half (no `output` until F4).
  const briefStarted = Date.now();
  const summary = await summarizeSessionInput(input, deps.transport, job.summaryModelVersion);
  const brief: AnalysisBrief = {
    schemaVersion: SESSION_SUMMARY_VERSION,
    promptVersion: SESSION_SUMMARY_PROMPT_VERSION,
    ...summary.summary,
    inputFingerprint: createHash("sha256").update(JSON.stringify(input)).digest("hex"),
    generatedAt: new Date().toISOString(),
    messageCount: input.messages.length,
    shortenedMessages: input.messages.filter((m) => m.truncated).length,
    backend: {
      model: summary.model,
      inputTokens: summary.inputTokens,
      outputTokens: summary.outputTokens,
      durationMs: Date.now() - briefStarted,
    },
  };
  const startedAt = Date.now();
  const analysis = await buildSessionAnalysisSnapshot(source.evidence, source.hydration, deps.transport, {
    source: source.sourceProvider,
    sessionId: source.sessionId,
    model: job.modelVersion,
    requestId: job.id,
  });
  return {
    result: {
      schemaVersion: SESSION_ANALYSIS_ENVELOPE_VERSION,
      analysisVersion: job.analysisVersion,
      sourceFingerprint: source.sourceFingerprint,
      analysis,
      durationMs: Date.now() - startedAt,
      brief,
    },
    harness: source.sourceProvider,
    sourceFingerprint: source.sourceFingerprint,
    sourceGeneration: source.sourceGeneration,
    inputTokens: (analysis.backend?.inputTokens ?? 0) + summary.inputTokens,
    outputTokens: (analysis.backend?.outputTokens ?? 0) + summary.outputTokens,
  };
}

/** The result object the job row records. */
export interface StoredAnalysisResult {
  key: string;
  sha256: string;
  bytes: number;
}

export interface AnalysisJobStore {
  claim(options: { jobId?: string; leaseSeconds: number }): Promise<AnalysisJob | null>;
  /**
   * One transaction: the lease is current, the session is live, the source revision is
   * unchanged; then a cleanup ticket for the planned key is recorded. Returns the key to
   * write, or null when any check fails (nothing may be written).
   */
  reserve(job: AnalysisJob, output: AnalysisJobOutput, planned: { sha256: string; bytes: number }): Promise<string | null>;
  /** Create-only put of the planned bytes at the reserved key. */
  put(key: string, bytes: Uint8Array, sha256: string): Promise<void>;
  /** One transaction: the same three checks, the result recorded, the ticket retired. */
  complete(input: { job: AnalysisJob; output: AnalysisJobOutput; stored: StoredAnalysisResult }): Promise<boolean>;
  /** Lease-fenced (token and expiry); false when the lease was lost. `error` is a controlled code. */
  fail(input: { jobId: string; leaseToken: string; error: string; retryable: boolean }): Promise<boolean>;
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
  deps: { evidence: AnalysisEvidenceDeps; transport: AnalysisModelTransport; assumeNoLegacyTurns?: boolean },
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
    const { bytes, sha256 } = encodeAnalysisResult(output.result);
    // Nothing is written unless the reservation (lease + liveness + revision + ticket) holds.
    const key = await store.reserve(job, output, { sha256, bytes: bytes.length });
    if (!key) return "lease-lost";
    await store.put(key, bytes, sha256);
    return (await store.complete({ job, output, stored: { key, sha256, bytes: bytes.length } }))
      ? "completed"
      : "lease-lost";
  } catch (error) {
    // Persist only controlled codes, never model content, credentials or exception text.
    const { code, retryable } = classifyAnalysisFailure(error);
    const saved = await store.fail({ jobId: job.id, leaseToken: job.leaseToken, error: code, retryable });
    return saved ? "failed" : "lease-lost";
  }
}
