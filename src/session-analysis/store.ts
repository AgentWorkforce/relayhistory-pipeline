/**
 * The worker side of sessions.session_analysis_jobs_v2 (20261105046000): relayhistory
 * repository.ts claim / complete / fail (lease 600 s, 3 attempts, backoff
 * LEAST(300, 15 * 2^(n-1)) s), as service_role, the way session-brief uses its tables,
 * plus the erase-safe publication protocol (processor.ts):
 *
 *   reserve   one statement: the job row is locked under a current lease (token AND
 *             expiry), the session's live pointer-set revision still equals the claim's,
 *             and only then a 'publishing' ticket for the planned key is recorded.
 *   put       the object at the reserved key (create-only, verified).
 *   complete  one statement: the same lease and revision checks, the result recorded and
 *             the ticket retired. An erase in between deletes the job row, so completion
 *             finds nothing and the ticket stays for the sweeper.
 *   sweep     deletes the object of every ticket older than the lease that no completed
 *             job owns (an erased, lost or crashed publication), then the ticket.
 *
 * Values only travel as bound parameters; no result or transcript text is written here.
 */
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import { objectKey, type ObjectStore } from "../../packages/object-store/src/index.ts";
import type { AnalysisJob, AnalysisJobStore } from "./processor.ts";
import { analysisResultKey, analysisResultPrefix, putAnalysisResult } from "./results.ts";

/** relayhistory SESSION_ANALYSIS_MAX_ATTEMPTS. */
export const SESSION_ANALYSIS_MAX_ATTEMPTS = 3;
const MAX_LEASE_SECONDS = 3_600;
/** A ticket younger than this may still belong to a live publication. */
export const TICKET_GRACE_SECONDS = 2 * 600;

type JobRow = {
  id: string;
  org_id: string;
  workspace_id: string;
  session_id: string;
  analysis_version: string;
  schema_version: number;
  model_version: string;
  summary_version: string;
  summary_model_version: string;
  attempts: number;
  lease_token: string;
  source_revision: string | null;
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
          job.schema_version, job.model_version, job.summary_version, job.summary_model_version,
          job.attempts, job.lease_token,
          sessions.session_analysis_source_revision(job.org_id, job.workspace_id, job.session_id) AS source_revision`;

const RESERVE_SQL = `
WITH job AS (
  SELECT id, org_id, workspace_id, session_id FROM sessions.session_analysis_jobs_v2
   WHERE id = $1::uuid AND status = 'running' AND lease_token = $2::uuid AND lease_expires_at > now()
   FOR UPDATE
), live AS (
  SELECT job.*, sessions.session_analysis_source_revision(job.org_id, job.workspace_id, job.session_id) AS rev FROM job
)
INSERT INTO sessions.session_analysis_result_tickets (result_key, job_id, org_id, workspace_id, session_id, reason)
SELECT $4, live.id, live.org_id, live.workspace_id, live.session_id, 'publishing'
  FROM live WHERE live.rev IS NOT NULL AND live.rev = $3
ON CONFLICT (result_key) DO UPDATE SET created_at = now()
  WHERE sessions.session_analysis_result_tickets.job_id = excluded.job_id
    AND sessions.session_analysis_result_tickets.reason = 'publishing'
RETURNING result_key`;

const COMPLETE_SQL = `
WITH done AS (
  UPDATE sessions.session_analysis_jobs_v2 AS j
     SET status = 'completed', harness = $3, source_fingerprint = $4, source_generation = $5,
         source_revision = $11, result_s3_key = $6, result_sha256 = $7, result_bytes = $8,
         input_tokens = $9, output_tokens = $10, last_error = NULL,
         lease_token = NULL, lease_expires_at = NULL, completed_at = now(), updated_at = now()
   WHERE j.id = $1::uuid AND j.status = 'running' AND j.lease_token = $2::uuid AND j.lease_expires_at > now()
     AND sessions.session_analysis_source_revision(j.org_id, j.workspace_id, j.session_id) = $11
     AND EXISTS (SELECT 1 FROM sessions.session_analysis_result_tickets AS t
                  WHERE t.result_key = $6 AND t.job_id = j.id AND t.reason = 'publishing')
  RETURNING j.id
), retired AS (
  DELETE FROM sessions.session_analysis_result_tickets AS t USING done
   WHERE t.result_key = $6 AND t.job_id = done.id
  RETURNING t.result_key
)
SELECT (SELECT count(*) FROM done)::int AS n, (SELECT count(*) FROM retired)::int AS r`;

const FAIL_SQL = `
UPDATE sessions.session_analysis_jobs_v2
   SET status = CASE WHEN $4::boolean AND attempts < $5 THEN 'queued' ELSE 'failed' END,
       available_at = now() + (LEAST(300, 15 * power(2, attempts - 1)) * interval '1 second'),
       lease_token = NULL, lease_expires_at = NULL, last_error = $3, updated_at = now()
 WHERE id = $1::uuid AND status = 'running' AND lease_token = $2::uuid AND lease_expires_at > now()
RETURNING id`;

/** Tickets past the grace period that no completed job owns; locked so two sweepers never race. */
const SWEEP_SQL = `
SELECT t.result_key FROM sessions.session_analysis_result_tickets AS t
 WHERE t.created_at < now() - make_interval(secs => $1::integer)
   AND (t.lease_expires_at IS NULL OR t.lease_expires_at <= now())
   AND NOT EXISTS (SELECT 1 FROM sessions.session_analysis_jobs_v2 AS j
                    WHERE j.id = t.job_id AND j.status = 'completed' AND j.result_s3_key = t.result_key)
 ORDER BY t.created_at
 LIMIT $2
 FOR UPDATE SKIP LOCKED`;

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
      const { rows } = await deps.db.query<JobRow>(CLAIM_SQL, [options.jobId ?? null, lease, SESSION_ANALYSIS_MAX_ATTEMPTS]);
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
        summaryVersion: row.summary_version,
        summaryModelVersion: row.summary_model_version,
        attempts: Number(row.attempts),
        leaseToken: row.lease_token,
        // No live pointer at claim: reserve can never succeed; the job fails as not found.
        sourceRevision: row.source_revision ?? "",
      };
    },
    async reserve(job, output, planned) {
      if (!job.sourceRevision) return null;
      const key = analysisResultKey(analysisResultPrefix(await deps.anchorManifestKey(job, output.harness)), job.id, planned.sha256);
      const { rows } = await deps.db.query<{ result_key: string }>(RESERVE_SQL, [job.id, job.leaseToken, job.sourceRevision, key]);
      return rows[0]?.result_key ?? null;
    },
    put: (key, bytes, sha256) => putAnalysisResult(deps.store, key, bytes, sha256),
    async complete({ job, output, stored }) {
      const { rows } = await deps.db.query<{ n: number }>(COMPLETE_SQL, [
        job.id,
        job.leaseToken,
        output.harness,
        output.sourceFingerprint,
        output.sourceGeneration,
        stored.key,
        stored.sha256,
        stored.bytes,
        output.inputTokens,
        output.outputTokens,
        job.sourceRevision,
      ]);
      return Number(rows[0]?.n ?? 0) === 1;
    },
    async fail({ jobId, leaseToken, error, retryable }) {
      const code = ERROR_CODE.test(error) ? error : "analysis_processing_failed";
      const { rows } = await deps.db.query(FAIL_SQL, [jobId, leaseToken, code, retryable, SESSION_ANALYSIS_MAX_ATTEMPTS]);
      return rows.length === 1;
    },
  };
}

/**
 * Delete the objects of orphan tickets (erased, lost or crashed publications), then the
 * tickets. Each ticket is deleted only after its object delete succeeded.
 */
export async function sweepAnalysisTickets(
  store: ObjectStore,
  options: {
    graceSeconds?: number;
    limit?: number;
    transact: <T>(work: (tx: SqlExecutor) => Promise<T>) => Promise<T>;
    /**
     * Every-version erase of exactly one key (src/migration/neon-s3-erasure/eraser.ts
     * eraseObject): the transcript bucket is versioned, so a plain DELETE would only add a
     * delete marker and leave the bytes. Required in production; absent = a plain delete
     * (unversioned stores and tests only).
     */
    eraseObject?: (key: string) => Promise<number>;
  },
): Promise<number> {
  return options.transact(async (tx) => {
    const { rows } = await tx.query<{ result_key: string }>(SWEEP_SQL, [options.graceSeconds ?? TICKET_GRACE_SECONDS, options.limit ?? 50]);
    for (const { result_key } of rows) {
      if (options.eraseObject) {
        await options.eraseObject(result_key);
        // Acknowledged only when a second inventory finds no version or marker left: until
        // then the ticket stays, and the next sweep retries.
        if ((await options.eraseObject(result_key)) !== 0) throw new Error("analysis_sweep_versions_remain");
      } else {
        await store.delete(objectKey(result_key));
      }
      await tx.query("DELETE FROM sessions.session_analysis_result_tickets WHERE result_key = $1", [result_key]);
    }
    return rows.length;
  });
}
