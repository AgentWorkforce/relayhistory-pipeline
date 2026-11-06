/**
 * The worker side of sessions.session_analysis_jobs_v2 (20261105046000): relayhistory
 * repository.ts claim / complete / fail (lease 600 s, 3 attempts, backoff
 * LEAST(300, 15 * 2^(n-1)) s), as service_role, the way session-brief uses its tables.
 * Values only travel as bound parameters; no result or transcript text is written here.
 */
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import type { ObjectStore } from "../../packages/object-store/src/index.ts";
import type { AnalysisJob, AnalysisJobOutput, AnalysisJobStore, StoredAnalysisResult } from "./processor.ts";
import { analysisResultPrefix, writeAnalysisResult } from "./results.ts";

/** relayhistory SESSION_ANALYSIS_MAX_ATTEMPTS. */
export const SESSION_ANALYSIS_MAX_ATTEMPTS = 3;
const MAX_LEASE_SECONDS = 3_600;

type JobRow = {
  id: string;
  org_id: string;
  workspace_id: string;
  session_id: string;
  analysis_version: string;
  schema_version: number;
  model_version: string;
  attempts: number;
  lease_token: string;
};

const CLAIM_SQL = `
WITH expired AS (
  UPDATE sessions.session_analysis_jobs_v2
     SET status = 'failed', lease_token = NULL, lease_expires_at = NULL,
         last_error = COALESCE(last_error, 'analysis_lease_expired'), updated_at = now()
   WHERE origin = 'native' AND status = 'running' AND lease_expires_at <= now() AND attempts >= $3
  RETURNING id
), next AS (
  SELECT id FROM sessions.session_analysis_jobs_v2
   WHERE origin = 'native' AND attempts < $3
     AND ($1::uuid IS NULL OR id = $1::uuid)
     AND ((status = 'queued' AND available_at <= now())
          OR (status = 'running' AND lease_expires_at <= now()))
   ORDER BY available_at, created_at, id
   LIMIT 1
   FOR UPDATE SKIP LOCKED
)
UPDATE sessions.session_analysis_jobs_v2 AS job
   SET status = 'running', attempts = job.attempts + 1, lease_token = gen_random_uuid(),
       lease_expires_at = now() + $2::integer * interval '1 second', updated_at = now()
  FROM next WHERE job.id = next.id
RETURNING job.id, job.org_id, job.workspace_id, job.session_id, job.analysis_version,
          job.schema_version, job.model_version, job.attempts, job.lease_token`;

const COMPLETE_SQL = `
UPDATE sessions.session_analysis_jobs_v2
   SET status = 'completed', harness = $3, source_fingerprint = $4, source_generation = $5,
       result_s3_key = $6, result_sha256 = $7, result_bytes = $8,
       input_tokens = $9, output_tokens = $10, last_error = NULL,
       lease_token = NULL, lease_expires_at = NULL, completed_at = now(), updated_at = now()
 WHERE id = $1::uuid AND status = 'running' AND lease_token = $2::uuid AND lease_expires_at > now()
RETURNING id`;

const FAIL_SQL = `
UPDATE sessions.session_analysis_jobs_v2
   SET status = CASE WHEN $4::boolean AND attempts < $5 THEN 'queued' ELSE 'failed' END,
       available_at = now() + (LEAST(300, 15 * power(2, attempts - 1)) * interval '1 second'),
       lease_token = NULL, lease_expires_at = NULL, last_error = $3, updated_at = now()
 WHERE id = $1::uuid AND status = 'running' AND lease_token = $2::uuid
RETURNING id`;

const ERROR_CODE = /^[a-z][a-z0-9_]{0,63}$/u;

export interface AnalysisJobStoreDeps {
  db: SqlExecutor;
  store: ObjectStore;
  /** The anchor manifest key of the job's session (its first live pointer). */
  anchorManifestKey(job: AnalysisJob, harness: string): Promise<string>;
}

export function postgresAnalysisJobStore(deps: AnalysisJobStoreDeps): AnalysisJobStore {
  return {
    async claim(options) {
      const lease = Math.min(MAX_LEASE_SECONDS, Math.max(1, Math.floor(options.leaseSeconds)));
      const { rows } = await deps.db.query<JobRow>(CLAIM_SQL, [
        options.jobId ?? null,
        lease,
        SESSION_ANALYSIS_MAX_ATTEMPTS,
      ]);
      const row = rows[0];
      if (!row) return null;
      return {
        id: row.id,
        orgId: row.org_id,
        workspaceId: row.workspace_id,
        sessionId: row.session_id,
        analysisVersion: row.analysis_version,
        schemaVersion: Number(row.schema_version),
        modelVersion: row.model_version,
        attempts: Number(row.attempts),
        leaseToken: row.lease_token,
      };
    },
    async writeResult(job: AnalysisJob, output: AnalysisJobOutput): Promise<StoredAnalysisResult> {
      const prefix = analysisResultPrefix(await deps.anchorManifestKey(job, output.harness));
      return writeAnalysisResult(deps.store, prefix, job.id, output.result);
    },
    async complete({ jobId, leaseToken, output, stored }) {
      const { rows } = await deps.db.query(COMPLETE_SQL, [
        jobId,
        leaseToken,
        output.harness,
        output.sourceFingerprint,
        output.sourceGeneration,
        stored.key,
        stored.sha256,
        stored.bytes,
        output.inputTokens,
        output.outputTokens,
      ]);
      return rows.length === 1;
    },
    async fail({ jobId, leaseToken, error, retryable }) {
      const code = ERROR_CODE.test(error) ? error : "analysis_processing_failed";
      const { rows } = await deps.db.query(FAIL_SQL, [
        jobId,
        leaseToken,
        code,
        retryable,
        SESSION_ANALYSIS_MAX_ATTEMPTS,
      ]);
      return rows.length === 1;
    },
  };
}
