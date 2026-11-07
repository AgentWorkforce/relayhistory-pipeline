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
import type { ObjectStore } from "../../packages/object-store/src/index.ts";
import type { EraseBudget, EraseCheckpoint, PagedEraser } from "../migration/neon-s3-erasure/eraser.ts";
import {
  nextSweepDelayMs,
  reconcileObligation,
  type ObligationAuthority,
  type ObligationReason,
  type ObligationStore,
  type ObligationWriter,
  type WriteObligation,
} from "../migration/neon-s3-erasure/write-obligation.ts";
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
INSERT INTO sessions.session_analysis_result_tickets (result_key, job_id, org_id, workspace_id, session_id, reason, writer_token, generation)
SELECT $4, live.id, live.org_id, live.workspace_id, live.session_id, 'publishing', $2::uuid, $3
  FROM live WHERE live.rev IS NOT NULL AND live.rev = $3
ON CONFLICT (result_key) DO UPDATE SET created_at = now(), writer_token = excluded.writer_token, generation = excluded.generation
  WHERE sessions.session_analysis_result_tickets.job_id = excluded.job_id
    AND sessions.session_analysis_result_tickets.reason = 'publishing'
RETURNING result_key`;

/** An aborted, timed-out or errored PUT: its outcome is unknown (it may commit, even late). */
const MARK_UNKNOWN_SQL = `
UPDATE sessions.session_analysis_result_tickets
   SET reason = 'unknown', lease_expires_at = NULL, next_sweep_at = now()
 WHERE result_key = $1 AND job_id = $2::uuid AND reason = 'publishing' AND writer_token = $3::uuid`;

const COMPLETE_SQL = `
WITH done AS (
  UPDATE sessions.session_analysis_jobs_v2 AS j
     SET status = 'completed', harness = $3, source_fingerprint = $4, source_generation = $5,
         source_revision = $11, result_s3_key = $6, result_sha256 = $7, result_bytes = $8,
         input_tokens = $9, output_tokens = $10, last_error = NULL, result_version_id = $12,
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

/**
 * Tickets past the grace period with no live lease and due for a sweep; locked so two
 * sweepers never race. Each comes with the versions published jobs own at its key.
 */
const SWEEP_SQL = `
SELECT t.result_key, t.reason, t.org_id, t.workspace_id, t.session_id, t.job_id::text AS job_id, t.generation,
       t.writer_token::text AS writer_token, t.erase_checkpoint, t.sweeps,
       EXISTS (SELECT 1 FROM sessions.session_analysis_ticket_strays AS s WHERE s.result_key = t.result_key) AS stray_found
  FROM sessions.session_analysis_result_tickets AS t
 WHERE t.created_at < now() - make_interval(secs => $1::integer)
   AND (t.lease_expires_at IS NULL OR t.lease_expires_at <= now())
   AND (t.next_sweep_at IS NULL OR t.next_sweep_at <= now())
 ORDER BY t.created_at
 LIMIT $2
 FOR UPDATE OF t SKIP LOCKED`;

const ERROR_CODE = /^[a-z][a-z0-9_]{0,63}$/u;

export interface AnalysisJobStoreDeps {
  db: SqlExecutor;
  store: ObjectStore;
  /** The anchor manifest key of the job's session (its first live pointer). */
  anchorManifestKey(job: AnalysisJob, harness: string): Promise<string>;
  /** The result PUT's hard deadline (default ANALYSIS_PUT_TIMEOUT_MS, well inside the lease). */
  putTimeoutMs?: number;
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
    async put(key, bytes, sha256, job) {
      try {
        return await putAnalysisResult(deps.store, key, bytes, sha256, deps.putTimeoutMs);
      } catch (error) {
        // Aborted, timed out, lost response or a failed read-back: the PUT may have committed
        // (even after any grace). The pending-write ticket becomes 'unknown' for the sweeper.
        if (job) await analysisObligationWriter.markUnknown(deps.db, key, workerIdentity(job));
        throw error;
      }
    },
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
        stored.versionId ?? null,
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

/** The worker's obligation identity: the job and its lease token (the ticket's writer). */
export const workerIdentity = (job: Pick<AnalysisJob, "id" | "leaseToken">): readonly unknown[] => [job.id, job.leaseToken];

/**
 * F1's ObligationWriter (write-obligation.ts) over session_analysis_result_tickets. The worker's
 * record and publish are the fenced CTEs RESERVE_SQL / COMPLETE_SQL (record inside the job-lease
 * + source-revision transaction; publish records result_version_id and retires the ticket in the
 * completion statement); the backfill's are its writer/generation/purge-fenced ticket and job
 * inserts. These methods are the same contract for callers outside those statements.
 * identity = [job id, writer token].
 */
export const analysisObligationWriter: ObligationWriter<SqlExecutor> = {
  async record(tx, key, identity) {
    const [jobId, writer] = identity as [string, string];
    await tx.query(
      `INSERT INTO sessions.session_analysis_result_tickets (result_key, job_id, org_id, workspace_id, session_id, reason, writer_token)
       SELECT $1, j.id, j.org_id, j.workspace_id, j.session_id, 'publishing', $3::uuid FROM sessions.session_analysis_jobs_v2 AS j WHERE j.id = $2::uuid
       ON CONFLICT (result_key) DO NOTHING`,
      [key, jobId, writer],
    );
  },
  async markUnknown(tx, key, identity) {
    const [jobId, writer] = identity as [string, string];
    await tx.query(MARK_UNKNOWN_SQL, [key, jobId, writer]);
  },
  async publish(tx, key, identity, versionId) {
    const [jobId, writer] = identity as [string, string];
    await tx.query(
      `WITH owned AS (
         UPDATE sessions.session_analysis_jobs_v2 SET result_version_id = $4 WHERE id = $2::uuid AND result_s3_key = $1 RETURNING id
       ), gone AS (
         DELETE FROM sessions.session_analysis_result_tickets
          WHERE result_key = $1 AND job_id = $2::uuid AND writer_token = $3::uuid AND EXISTS (SELECT 1 FROM owned) RETURNING result_key
       )
       DELETE FROM sessions.session_analysis_ticket_strays WHERE result_key IN (SELECT result_key FROM gone)`,
      [key, jobId, writer, versionId],
    );
  },
};

/** One ticket's authority: reason, identity, and the versions completed jobs own at its key. */
const TICKET_AUTHORITY_SQL = `
SELECT t.reason, t.job_id::text AS job_id, t.writer_token::text AS writer_token,
       ARRAY(SELECT j.result_version_id FROM sessions.session_analysis_jobs_v2 AS j
              WHERE j.result_s3_key = t.result_key AND j.status = 'completed' AND j.result_version_id IS NOT NULL
              ORDER BY 1) AS owned,
       (SELECT count(*) FROM sessions.session_analysis_jobs_v2 AS j
         WHERE j.result_s3_key = t.result_key AND j.status = 'completed' AND j.result_version_id IS NULL)::int AS without_version
  FROM sessions.session_analysis_result_tickets AS t WHERE t.result_key = $1`;

interface TicketRow {
  result_key: string; reason: ObligationReason; org_id: string; workspace_id: string; session_id: string; job_id: string;
  generation: string | null; writer_token: string | null; erase_checkpoint: EraseCheckpoint | null; sweeps: number; stray_found: boolean;
}
const ticketIdentity = (t: Pick<TicketRow, "org_id" | "workspace_id" | "session_id" | "job_id" | "generation" | "writer_token">) =>
  ["analysis-result-ticket", t.org_id, t.workspace_id, t.session_id, t.job_id, t.generation, t.writer_token] as const;

/**
 * F1's ObligationStore (write-obligation.ts) for one locked ticket. `tx` holds the ticket row
 * (FOR UPDATE); `durable` commits on its own: markStray writes session_analysis_ticket_strays
 * (no lock on the ticket row) so the evidence is durable before the page's deletes even if the
 * sweep transaction later rolls back.
 */
function ticketObligationStore(tx: SqlExecutor, durable: SqlExecutor, t: TicketRow): ObligationStore {
  return {
    async load(): Promise<ObligationAuthority | null> {
      const row = (await tx.query<{ reason: ObligationReason; job_id: string; writer_token: string | null; owned: string[] | null; without_version: number }>(
        TICKET_AUTHORITY_SQL, [t.result_key],
      )).rows[0];
      if (!row) return null;
      return {
        reason: row.reason,
        identity: ticketIdentity({ ...t, job_id: row.job_id, writer_token: row.writer_token }),
        ownedVersions: row.owned ?? [],
        ownersWithoutVersion: Number(row.without_version),
      };
    },
    async markStray() {
      await durable.query("INSERT INTO sessions.session_analysis_ticket_strays (result_key) VALUES ($1) ON CONFLICT DO NOTHING", [t.result_key]);
    },
    async saveCheckpoint(checkpoint) {
      await tx.query("UPDATE sessions.session_analysis_result_tickets SET erase_checkpoint = $2::jsonb WHERE result_key = $1", [t.result_key, JSON.stringify(checkpoint)]);
    },
    async reschedule(strayFound) {
      if (strayFound) await tx.query("INSERT INTO sessions.session_analysis_ticket_strays (result_key) VALUES ($1) ON CONFLICT DO NOTHING", [t.result_key]);
      await tx.query(
        `UPDATE sessions.session_analysis_result_tickets
            SET sweeps = sweeps + 1, erase_checkpoint = NULL, next_sweep_at = now() + $2::double precision * interval '1 millisecond'
          WHERE result_key = $1`,
        [t.result_key, nextSweepDelayMs(Number(t.sweeps))],
      );
    },
    async retire() {
      await tx.query("DELETE FROM sessions.session_analysis_result_tickets WHERE result_key = $1", [t.result_key]);
      await tx.query("DELETE FROM sessions.session_analysis_ticket_strays WHERE result_key = $1", [t.result_key]);
    },
  };
}

/**
 * Sweep the result tickets: each ticket is one durable write obligation, reconciled by THE
 * shared implementation (src/migration/neon-s3-erasure/write-obligation.ts reconcileObligation,
 * over the shared strict paged eraser). Successor-safe (versions completed jobs own are never
 * selected), checkpointed per ticket, authority re-read before every page's deletes, durable
 * stray evidence, and the shared completion predicate (forgotten: a fresh strict zero
 * inventory of unowned versions; unknown/publishing: additionally the write accounted for, by a
 * found stray or by ownership). A clean sweep only reschedules; a failed step keeps the ticket,
 * its checkpoint and its stray evidence, and the sweep continues. Returns the tickets retired.
 */
export async function sweepAnalysisTickets(
  store: ObjectStore,
  options: {
    graceSeconds?: number;
    limit?: number;
    transact: <T>(work: (tx: SqlExecutor) => Promise<T>) => Promise<T>;
    /** REQUIRED: the shared paged eraser (createS3Eraser / storeEraser). No plain-delete fallback. */
    eraser: PagedEraser;
    /** Commits on its own (the worker's pool): durable stray evidence. Default: the sweep transaction. */
    durable?: SqlExecutor;
    /** Per-ticket budget for one step (the eraser's hard ceilings still apply). */
    budget?: EraseBudget;
    /** A ticket's step failed (it stays, with its last checkpoint); the sweep continues. */
    onError?: (key: string, error: unknown) => void;
  },
): Promise<number> {
  if (typeof options?.eraser?.eraseStep !== "function" || typeof options.eraser.verifyErased !== "function")
    throw new Error("analysis_sweep_eraser_required");
  void store;
  return options.transact(async (tx) => {
    const { rows } = await tx.query<TicketRow>(SWEEP_SQL, [options.graceSeconds ?? TICKET_GRACE_SECONDS, options.limit ?? 50]);
    let retired = 0;
    for (const t of rows) {
      const obligation: WriteObligation = {
        key: t.result_key,
        reason: t.reason,
        identity: ticketIdentity(t),
        checkpoint: t.erase_checkpoint,
        strayFound: t.stray_found === true,
      };
      try {
        const outcome = await reconcileObligation(options.eraser, ticketObligationStore(tx, options.durable ?? tx, t), obligation, {
          ...(options.budget ? { budget: options.budget } : {}),
        });
        if (outcome === "retired") retired += 1;
      } catch (error) {
        options.onError?.(t.result_key, error);
      }
    }
    return retired;
  });
}
