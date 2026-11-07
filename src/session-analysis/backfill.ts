/**
 * Backfill of Neon session-analysis history into migration 20261105046000 (lane F1,
 * map F1.7) and the byte-parity check (F1.8). Run once per canonical scope before the
 * Neon drop, by an operator, against the Neon read-only role or the frozen copy.
 *
 * Input: the exact public v1 payload Neon's read_session_analysis_v1 serves for the scope
 * (`readLegacyAnalysisSnapshot`, the SELECT-only 0024/0031 oracle already in
 * src/session-pipeline/analysis-v1-select.ts). Output: requests_v2 / jobs_v2 rows with
 * origin 'neon_backfill' that keep every public v1 value, and one result object per
 * completed analysis holding Neon's exact v1 `result` (brief already merged). Nothing is
 * recomputed, so SP's GET answers the same bytes Neon did; `parityEqual` proves it.
 *
 * Idempotent: rows are keyed by Neon's ids (ON CONFLICT DO NOTHING), and result objects
 * are create-only by content hash.
 */
import { createHash } from "node:crypto";
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import { objectKey, readAll, type ObjectStore } from "../../packages/object-store/src/index.ts";
import { analysisResultKey, ANALYSIS_RESULT_MAX_BYTES } from "./results.ts";

type Json = Record<string, unknown>;
export type NeonV1Payload = { outcome: "not_found" } | { outcome: "ok"; sessionId: string; requests: Json[]; analyses: Json[] };
export interface BackfillScope {
  orgId: string;
  workspaceId: string;
  sessionId: string;
}

export class BackfillError extends Error {
  constructor(readonly code: string) {
    super(code);
    this.name = "BackfillError";
  }
}

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/u;
const STATUS = new Set(["queued", "running", "completed", "failed"]);
const str = (v: unknown, code: string): string => {
  if (typeof v !== "string") throw new BackfillError(code);
  return v;
};
const nullableStr = (v: unknown, code: string): string | null => (v === null ? null : str(v, code));

export interface BackfillPlan {
  jobs: Array<Json & { id: string; result: unknown }>;
  requests: Json[];
}

/** Validate the Neon payload and plan v2 rows. Refuses anything it cannot keep exactly. */
export function planBackfill(scope: BackfillScope, payload: NeonV1Payload): BackfillPlan {
  if (payload.outcome === "not_found") return { jobs: [], requests: [] };
  if (payload.sessionId !== scope.sessionId) throw new BackfillError("backfill_session_mismatch");
  const jobs = payload.analyses.map((a) => {
    const id = str(a.id, "backfill_job_id");
    if (!UUID.test(id) || a.sessionId !== scope.sessionId) throw new BackfillError("backfill_job_invalid");
    const status = str(a.status, "backfill_job_status");
    if (!STATUS.has(status)) throw new BackfillError("backfill_job_status");
    if (status === "completed" && (a.result === null || a.result === undefined))
      throw new BackfillError("backfill_completed_without_result");
    return {
      id,
      harness: str(a.sourceProvider, "backfill_job_source") || null,
      status,
      attempts: Number(a.attempts),
      analysis_version: str(a.analysisVersion, "backfill_job_version"),
      schema_version: Number(a.schemaVersion),
      summary_version: str(a.summaryVersion, "backfill_job_version"),
      model_version: str(a.modelVersion, "backfill_job_version"),
      summary_model_version: str(a.summaryModelVersion, "backfill_job_version"),
      source_fingerprint: str(a.sourceFingerprint, "backfill_job_fingerprint") || null,
      last_error: nullableStr(a.error ?? null, "backfill_job_error"),
      created_at: str(a.createdAt, "backfill_job_time"),
      updated_at: str(a.updatedAt, "backfill_job_time"),
      completed_at: nullableStr(a.completedAt ?? null, "backfill_job_time"),
      result: status === "completed" ? a.result : null,
    };
  });
  const known = new Set(jobs.map((j) => j.id));
  const requests = payload.requests.map((r) => {
    const id = str(r.id, "backfill_request_id");
    const analysisId = nullableStr(r.analysisId ?? null, "backfill_request_job");
    if (!UUID.test(id) || r.sessionId !== scope.sessionId) throw new BackfillError("backfill_request_invalid");
    if (analysisId !== null && !known.has(analysisId)) throw new BackfillError("backfill_request_job_unknown");
    return {
      id,
      status: str(r.status, "backfill_request_status"),
      analysis_job_id: analysisId,
      v1_job_id: analysisId,
      last_error: nullableStr(r.error ?? null, "backfill_request_error"),
      created_at: str(r.createdAt, "backfill_request_time"),
      updated_at: str(r.updatedAt, "backfill_request_time"),
    };
  });
  return { jobs, requests };
}

/** Write result objects, then the rows, in one transaction the caller owns. */
export async function applyBackfill(
  db: SqlExecutor,
  store: ObjectStore,
  scope: BackfillScope,
  resultPrefix: string,
  plan: BackfillPlan,
): Promise<{ jobs: number; requests: number; objects: number }> {
  let objects = 0;
  for (const job of plan.jobs) {
    let ref: { key: string; sha256: string; bytes: number } | null = null;
    if (job.result !== null) {
      const bytes = new TextEncoder().encode(JSON.stringify(job.result));
      if (bytes.length > ANALYSIS_RESULT_MAX_BYTES) throw new BackfillError("backfill_result_bound");
      const sha256 = createHash("sha256").update(bytes).digest("hex");
      const key = analysisResultKey(resultPrefix, job.id, sha256);
      await store.put(objectKey(key), bytes, { ifNoneMatch: true, contentSha256: sha256, contentType: "application/json" });
      const read = await readAll(await store.get(objectKey(key)));
      if (createHash("sha256").update(read).digest("hex") !== sha256) throw new BackfillError("backfill_write_verification_failed");
      ref = { key, sha256, bytes: bytes.length };
      objects += 1;
    }
    await db.query(
      `INSERT INTO sessions.session_analysis_jobs_v2
         (id, org_id, workspace_id, session_id, harness, status, attempts, analysis_version, schema_version,
          model_version, summary_version, summary_model_version, source_fingerprint, result_s3_key, result_sha256,
          result_bytes, last_error, origin, created_at, updated_at, completed_at)
       VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, 'neon_backfill',
               $18::timestamptz, $19::timestamptz, $20::timestamptz)
       ON CONFLICT (id) DO NOTHING`,
      [job.id, scope.orgId, scope.workspaceId, scope.sessionId, job.harness, job.status, job.attempts,
       job.analysis_version, job.schema_version, job.model_version, job.summary_version, job.summary_model_version,
       job.source_fingerprint, ref?.key ?? null, ref?.sha256 ?? null, ref?.bytes ?? null, job.last_error,
       job.created_at, job.updated_at, job.completed_at],
    );
  }
  for (const r of plan.requests) {
    await db.query(
      `INSERT INTO sessions.session_analysis_requests_v2
         (id, org_id, workspace_id, session_id, requested_by_user_id, status, analysis_job_id, v1_job_id,
          last_error, origin, created_at, updated_at)
       VALUES ($1::uuid, $2, $3, $4, 'neon_backfill', $5, $6::uuid, $7::uuid, $8, 'neon_backfill',
               $9::timestamptz, $10::timestamptz)
       ON CONFLICT (id) DO NOTHING`,
      [r.id, scope.orgId, scope.workspaceId, scope.sessionId, r.status, r.analysis_job_id, r.v1_job_id,
       r.last_error, r.created_at, r.updated_at],
    );
  }
  return { jobs: plan.jobs.length, requests: plan.requests.length, objects };
}

/** relayhistory evidence.ts canonical JSON: sorted keys, so key order never decides parity. */
export function canonicalJson(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  if (value !== null && typeof value === "object")
    return `{${Object.keys(value as Json).sort().map((k) => `${JSON.stringify(k)}:${canonicalJson((value as Json)[k])}`).join(",")}}`;
  return JSON.stringify(value) ?? "null";
}

/**
 * F1.8 parity for one scope: Neon's v1 payload against SP's GET body (requests and
 * analyses only; correlationId is per call). Returns no content, only the verdict.
 */
export function parityEqual(neon: NeonV1Payload, sp: { sessionId: string; requests: unknown[]; analyses: unknown[] } | null) {
  if (neon.outcome === "not_found") return { equal: sp === null, requests: 0, analyses: 0 };
  if (!sp) return { equal: false, requests: neon.requests.length, analyses: neon.analyses.length };
  const pick = (v: { sessionId: string; requests: unknown[]; analyses: unknown[] }) =>
    canonicalJson({ sessionId: v.sessionId, requests: v.requests, analyses: v.analyses });
  return { equal: pick(neon) === pick(sp), requests: neon.requests.length, analyses: neon.analyses.length };
}
