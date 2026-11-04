import { prepareAssessment } from "../../packages/coordination-eval/src/intake.ts";
import {
  executeAnalysis,
  type AnalysisOptions,
  type AnalysisTransport,
} from "../../packages/coordination-eval/src/analysis.ts";
import {
  buildReport,
  renderMarkdown,
} from "../../packages/coordination-eval/src/report.ts";
import {
  buildCanonicalReportExport,
  type ReportSourceDeps,
  type ReportSourceRequest,
} from "./source.ts";

export interface ReportJobClaim {
  jobId: string;
  leaseId: string;
  request: ReportSourceRequest;
}
export interface ReportJobStore {
  claim(jobId: string, leaseSeconds: number): Promise<ReportJobClaim | null>;
  complete(
    claim: ReportJobClaim,
    directoryGeneration: string,
    result: unknown,
  ): Promise<boolean>;
  fail(claim: ReportJobClaim, errorCode: string): Promise<void>;
}
export interface ReportWorkerDeps {
  source: ReportSourceDeps;
  database: ReportJobStore;
  transport: AnalysisTransport;
  analysisOptions?: AnalysisOptions;
  providerProvenance?: () => unknown;
}
export type ReportJobOutcome = {
  jobId: string;
  status: "completed" | "not_claimed" | "failed";
  analysisStatus?: "complete" | "partial";
  errorCode?:
    | "source_failed"
    | "analysis_incomplete"
    | "publication_rejected"
    | "report_failed";
};
export const REPORT_WORKER_VERSION = "canonical-coordination-worker/0.1.0";

/** A leased, on-demand job. All published content passes the canonical source and model boundaries. */
export async function runCoordinationReportJob(
  deps: ReportWorkerDeps,
  jobId: string,
): Promise<ReportJobOutcome> {
  // Leases are fixed and bounded; a late writer must still pass the database expiry and source fence.
  let claim: ReportJobClaim | null;
  try {
    claim = await deps.database.claim(jobId, 300);
  } catch {
    return { jobId, status: "failed", errorCode: "report_failed" };
  }
  if (!claim) return { jobId, status: "not_claimed" };
  let errorCode: NonNullable<ReportJobOutcome["errorCode"]> = "source_failed";
  const deadline = new AbortController();
  const timer = setTimeout(() => deadline.abort(), 240000);
  const cancel = () => deadline.abort();
  deps.analysisOptions?.signal?.addEventListener("abort", cancel, {
    once: true,
  });
  if (deps.analysisOptions?.signal?.aborted) deadline.abort();
  try {
    const canonical = await buildCanonicalReportExport(
      { ...deps.source, signal: deadline.signal },
      claim.request,
    );
    if (deadline.signal.aborted) throw new Error("job deadline exceeded");
    const intake = prepareAssessment(canonical.export);
    errorCode = "analysis_incomplete";
    const options: AnalysisOptions = {
      maxAttempts: 1,
      ...deps.analysisOptions,
      concurrency: Math.min(deps.analysisOptions?.concurrency ?? 4, 4),
      maxCalls: Math.min(deps.analysisOptions?.maxCalls ?? 24, 24),
      timeoutMs: Math.min(deps.analysisOptions?.timeoutMs ?? 30000, 30000),
      packetLimits: {
        ...deps.analysisOptions?.packetLimits,
        maxPackets: Math.min(
          deps.analysisOptions?.packetLimits?.maxPackets ?? 24,
          24,
        ),
      },
      signal: deadline.signal,
    };
    const analysis = await executeAnalysis(
      intake.assessment,
      deps.transport,
      options,
    );
    if (deadline.signal.aborted) throw new Error("analysis deadline exceeded");
    const sampling = packetSampling(analysis);
    const analysisStatus =
      analysis.execution.completedPackets !== analysis.execution.totalPackets ||
      analysis.packets.omittedWorkstreamIds.length ||
      sampling.affectedPackets
        ? "partial"
        : "complete";
    if (analysisStatus === "partial") {
      analysis.assessment.coverage.status = "partial";
      analysis.assessment.coverage.notes +=
        " Semantic analysis is incomplete: failed or omitted packets and omitted or truncated packet context remain unresolved.";
    }
    errorCode = "report_failed";
    const report = buildReport(analysis.assessment);
    const { evidence: _evidence, ...reportWithoutEvidence } = report;
    const artifact = {
      schemaVersion: 1,
      workerVersion: REPORT_WORKER_VERSION,
      providerProvenance: deps.providerProvenance?.() ?? null,
      sourceSnapshot: canonical.sourceSnapshot,
      intake: intake.inventory,
      assessment: analysis.assessment,
      analysis: {
        coverage: {
          status: analysisStatus,
          sampling,
          omittedWorkstreamIds: analysis.packets.omittedWorkstreamIds,
        },
        packetResults: analysis.packetResults,
        missingContextRequests: analysis.missingContextRequests,
        execution: analysis.execution,
        packetAccounting: analysis.packets.accounting,
      },
      report: {
        ...reportWithoutEvidence,
        evidenceArtifact: "assessment.evidence",
      },
      markdown:
        renderMarkdown(report) +
        renderAnalysisAppendix(analysis, analysisStatus),
    };
    errorCode = "publication_rejected";
    if (
      deadline.signal.aborted ||
      !(await deps.database.complete(
        claim,
        canonical.sourceSnapshot.directoryGeneration,
        artifact,
      ))
    )
      throw new Error("publication fence rejected");
    return { jobId, status: "completed", analysisStatus };
  } catch {
    // Provider/parser/storage messages can contain transcript data or credentials. Persist fixed codes only.
    try {
      await deps.database.fail(claim, errorCode);
    } catch {
      /* The lease expires; no incomplete artifact was published. */
    }
    return { jobId, status: "failed", errorCode };
  } finally {
    clearTimeout(timer);
    deps.analysisOptions?.signal?.removeEventListener("abort", cancel);
  }
}

function renderAnalysisAppendix(
  analysis: Awaited<ReturnType<typeof executeAnalysis>>,
  status: "complete" | "partial",
): string {
  // Model text is data: HTML and Markdown delimiters cannot become executable report markup.
  const safe = (value: string) =>
    value
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/[\\`*_{}\[\]()#!|]/g, "\\$&")
      .replace(/\r?\n/g, " ");
  const citations = (ids: string[]) =>
    ids.map(safe).join(", ") || "none captured";
  const sampling = packetSampling(analysis);
  const lines = [
    "",
    "## Draft semantic analysis — unreviewed",
    "",
    `Analysis coverage: ${status}. Publication does not confirm any model proposal or complete human review.`,
    "",
    `Bounded sampling: ${sampling.affectedPackets} packet(s) omit or truncate context; ${sampling.omittedEvidence} omitted event references, ${sampling.truncatedEvidence} truncated event references, ${sampling.omittedSessions} omitted session references, ${sampling.omittedObservations} omitted observations, ${sampling.metadataTruncations} truncated metadata fields, and ${sampling.referenceOmissions} omitted reference groups. Counts are packet occurrences and may overlap across packets. ${analysis.packets.omittedWorkstreamIds.length} workstream(s) were not analyzed.`,
    "",
  ];
  for (const row of analysis.packetResults) {
    lines.push(
      `### Packet ${safe(row.packetId)}`,
      "",
      `Execution: ${row.status}.`,
      "",
    );
    if (!row.result) continue;
    const result = row.result;
    lines.push(
      `Proposed summary (${result.workstreamSummary.state}): ${safe(result.workstreamSummary.text)}`,
      `Evidence: ${citations(result.workstreamSummary.evidenceIds)}.`,
      "",
    );
    for (const observation of result.observationReviews)
      lines.push(
        `Observation ${safe(observation.observationId)} (${safe(observation.disposition)}): ${safe(observation.rationale)}`,
        `Evidence: ${citations(observation.evidenceIds)}.`,
        "",
      );
    for (const candidate of result.candidateFindings)
      lines.push(
        `Pending ${candidate.kind}: ${safe(candidate.description)}`,
        `Facts: ${citations(candidate.factEvidenceIds)}. Actions: ${citations(candidate.actionEvidenceIds)}. Outcomes: ${citations(candidate.outcomeEvidenceIds)}.`,
        `Alternative: ${safe(candidate.strongestAlternativeExplanation)}`,
        `Uncertainty: ${safe(candidate.uncertainty)}`,
        "",
      );
  }
  for (const request of analysis.missingContextRequests)
    lines.push(
      `Context needed: ${safe(request.requestedContext)}`,
      `Reason: ${safe(request.reason)} Evidence: ${citations(request.evidenceIds)}.`,
      "",
    );
  if (analysis.packets.omittedWorkstreamIds.length)
    lines.push(
      `Workstreams without analysis: ${citations(analysis.packets.omittedWorkstreamIds)}.`,
      "",
    );
  return lines.join("\n");
}

function packetSampling(analysis: Awaited<ReturnType<typeof executeAnalysis>>) {
  const rows = Object.values(analysis.packets.accounting);
  return {
    affectedPackets: rows.filter(
      (r) =>
        r.omittedEvidenceIds.length ||
        r.omittedSessionKeys.length ||
        r.missingCitedEvidenceIds.length ||
        r.packetTruncatedEvidenceIds.length ||
        r.upstreamTruncatedEvidenceIds.length ||
        r.omittedObservationIds.length ||
        r.metadataTruncations.length ||
        r.referenceOmissions.length,
    ).length,
    omittedEvidence: rows.reduce((n, r) => n + r.omittedEvidenceIds.length, 0),
    truncatedEvidence: rows.reduce(
      (n, r) =>
        n +
        new Set([
          ...r.packetTruncatedEvidenceIds,
          ...r.upstreamTruncatedEvidenceIds,
        ]).size,
      0,
    ),
    omittedSessions: rows.reduce((n, r) => n + r.omittedSessionKeys.length, 0),
    omittedObservations: rows.reduce(
      (n, r) => n + r.omittedObservationIds.length,
      0,
    ),
    metadataTruncations: rows.reduce(
      (n, r) => n + r.metadataTruncations.length,
      0,
    ),
    referenceOmissions: rows.reduce(
      (n, r) => n + r.referenceOmissions.length,
      0,
    ),
  };
}
