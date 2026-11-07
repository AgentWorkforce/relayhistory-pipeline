/**
 * The read side of the analysis lifecycle for the session read API: relayhistory's
 * GET /v1/sessions/:id/analysis body (cloud-client contract session-analysis.ts
 * sessionAnalysesResponse) assembled from sessions.analysis_read_v1 rows and the verified
 * result objects, and the POST outcome map (Cloud lib/relayhistory-analysis.ts:35-78).
 */
import type { StoredAnalysisResult } from "./processor.ts";

export interface ReadRpcAnalysis {
  id: string;
  sessionId: string;
  sourceProvider: string;
  sourceFingerprint: string;
  status: "queued" | "running" | "completed" | "failed";
  attempts: number;
  analysisVersion: string;
  schemaVersion: number;
  summaryVersion: string;
  modelVersion: string;
  summaryModelVersion: string;
  resultRef: StoredAnalysisResult | null;
  origin: "native" | "neon_backfill";
  harness: string | null;
  error: string | null;
  createdAt: string;
  updatedAt: string;
  completedAt: string | null;
}

export type ReadRpc =
  | { outcome: "not_found" }
  | { outcome: "ok"; sessionId: string; requests: Record<string, unknown>[]; analyses: ReadRpcAnalysis[] };

export type AdmitRpc =
  | { outcome: "accepted"; request: Record<string, unknown> }
  | { outcome: "not_found" | "rate_limited" | "invalid_request" };


const RESULT_KEY = /\/normalized\/session-analysis-v2\/[0-9a-f-]{36}\/[0-9a-f]{64}\.json$/u;

export class AnalysisReadError extends Error {
  constructor(readonly code: string) {
    super(code);
    this.name = "AnalysisReadError";
  }
}

/**
 * relayhistory's v1 analyses[]: `result` is the stored object for a completed job, served
 * verbatim. A native object already holds the analysis envelope AND the brief produced from
 * the same evidence read (relayhistory 0031's `a.result || {brief: b.result}`); a backfilled
 * one holds Neon's exact v1 `result`. Nothing is merged here, so no brief of another revision
 * can ever be attached.
 */
export async function assembleAnalyses(
  rpc: Extract<ReadRpc, { outcome: "ok" }>,
  deps: { result(ref: StoredAnalysisResult): Promise<unknown> },
): Promise<{ sessionId: string; requests: Record<string, unknown>[]; analyses: Record<string, unknown>[] }> {
  const analyses: Record<string, unknown>[] = [];
  for (const row of rpc.analyses) {
    const { resultRef, origin: _origin, harness: _harness, ...wire } = row;
    let result: unknown = null;
    if (row.status === "completed") {
      if (!resultRef || !RESULT_KEY.test(resultRef.key)) throw new AnalysisReadError("analysis_result_ref_invalid");
      result = await deps.result(resultRef);
    }
    analyses.push({ ...wire, result });
  }
  return { sessionId: rpc.sessionId, requests: rpc.requests, analyses };
}

/** The POST answer, as Cloud's direct SQL path mapped request_session_analysis_v1. */
export function admitResponse(rpc: AdmitRpc): { status: 202 | 404 | 429 | 400; body: Record<string, unknown>; retryAfter?: string } {
  switch (rpc.outcome) {
    case "accepted":
      return { status: 202, body: { request: rpc.request } };
    case "not_found":
      return { status: 404, body: { error: { code: "not_found", message: "Session not found" } } };
    case "rate_limited":
      return {
        status: 429,
        retryAfter: "86400",
        body: { error: { code: "rate_limited", message: "Daily session analysis request limit reached" } },
      };
    default:
      return { status: 400, body: { error: { code: "invalid_request", message: "Invalid session analysis request" } } };
  }
}
