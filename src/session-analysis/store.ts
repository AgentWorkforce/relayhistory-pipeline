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
import { randomUUID } from "node:crypto";
import type { ObjectStore } from "../../packages/object-store/src/index.ts";
import type { EraseBudget, EraseCheckpoint, PagedEraser } from "../migration/neon-s3-erasure/eraser.ts";
import {
  nextSweepDelayMs,
  reconcileObligation,
  type Cas,
  type InvocationOutcome,
  type ObligationState,
  type ObligationStore,
  type ObligationWriter,
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
), ticket AS (
  INSERT INTO sessions.session_analysis_result_tickets (result_key, job_id, org_id, workspace_id, session_id, reason, writer_token, generation)
  SELECT $4, live.id, live.org_id, live.workspace_id, live.session_id, 'publishing', $2::uuid, $3
    FROM live WHERE live.rev IS NOT NULL AND live.rev = $3
  ON CONFLICT (result_key) DO UPDATE SET created_at = now(), writer_token = excluded.writer_token, generation = excluded.generation,
      rev = nextval('sessions.session_analysis_ticket_rev_seq')
    WHERE sessions.session_analysis_result_tickets.job_id = excluded.job_id
      AND sessions.session_analysis_result_tickets.reason = 'publishing'
  RETURNING result_key
), dispatched AS (
  -- The invocation, committed with the reservation BEFORE the PUT is sent (write-obligation v3).
  INSERT INTO sessions.session_analysis_ticket_invocations (invocation_id, result_key, writer_token, generation)
  SELECT $5::uuid, ticket.result_key, $2::uuid, $3 FROM ticket
  RETURNING result_key
)
SELECT result_key FROM dispatched`;

/** The request's OWN evidence (writer + generation scoped): acked with its exact version id, or refused. */
const INVOCATION_ACKED_SQL = `
UPDATE sessions.session_analysis_ticket_invocations SET outcome = 'acked', version_id = $3
 WHERE invocation_id = $2::uuid AND result_key = $1 AND outcome = 'dispatched'
   AND writer_token IS NOT DISTINCT FROM $4::uuid AND generation IS NOT DISTINCT FROM $5`;
const INVOCATION_REFUSED_SQL = `
UPDATE sessions.session_analysis_ticket_invocations SET outcome = 'refused'
 WHERE invocation_id = $2::uuid AND result_key = $1 AND outcome = 'dispatched'
   AND writer_token IS NOT DISTINCT FROM $3::uuid AND generation IS NOT DISTINCT FROM $4`;

/**
 * Publish (write-obligation v3 ObligationWriter.publish): under the ticket ROW LOCK that the
 * sweep's reserve takes, refused for a version reserved for deletion; records the owned version,
 * resolves the publishing invocation (acked + owned) and bumps the ticket rev; the ticket is
 * retired only when no other invocation is left at the key.
 */
const COMPLETE_SQL = `
WITH lk AS (
  SELECT t.result_key FROM sessions.session_analysis_result_tickets AS t
   WHERE t.result_key = $6 AND t.job_id = $1::uuid AND t.reason = 'publishing' AND t.writer_token = $2::uuid
     AND ($12::text IS NULL OR NOT ($12::text = ANY (t.reserved_versions)))
   FOR UPDATE
), done AS (
  UPDATE sessions.session_analysis_jobs_v2 AS j
     SET status = 'completed', harness = $3, source_fingerprint = $4, source_generation = $5,
         source_revision = $11, result_s3_key = $6, result_sha256 = $7, result_bytes = $8,
         input_tokens = $9, output_tokens = $10, last_error = NULL, result_version_id = $12,
         lease_token = NULL, lease_expires_at = NULL, completed_at = now(), updated_at = now()
   WHERE j.id = $1::uuid AND j.status = 'running' AND j.lease_token = $2::uuid AND j.lease_expires_at > now()
     AND sessions.session_analysis_source_revision(j.org_id, j.workspace_id, j.session_id) = $11
     AND EXISTS (SELECT 1 FROM lk)
     AND NOT EXISTS (SELECT 1 FROM sessions.session_analysis_ticket_reservations AS v
                      WHERE v.result_key = $6 AND v.version_id = $12)
  RETURNING j.id
), own AS (
  DELETE FROM sessions.session_analysis_ticket_invocations AS i USING done
   WHERE i.invocation_id = $13::uuid AND i.result_key = $6
  RETURNING i.invocation_id
), retired AS (
  DELETE FROM sessions.session_analysis_result_tickets AS t USING done
   WHERE t.result_key = $6 AND t.job_id = done.id
     AND NOT EXISTS (SELECT 1 FROM sessions.session_analysis_ticket_invocations AS i
                      WHERE i.result_key = $6 AND i.invocation_id <> $13::uuid)
  RETURNING t.result_key
), bumped AS (
  UPDATE sessions.session_analysis_result_tickets AS t SET rev = nextval('sessions.session_analysis_ticket_rev_seq')
    FROM done
   WHERE t.result_key = $6 AND NOT EXISTS (SELECT 1 FROM retired)
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
       t.writer_token::text AS writer_token
  FROM sessions.session_analysis_result_tickets AS t
 WHERE t.created_at < now() - make_interval(secs => $1::integer)
   AND (t.lease_expires_at IS NULL OR t.lease_expires_at <= now())
   AND (t.next_sweep_at IS NULL OR t.next_sweep_at <= now())
 ORDER BY t.created_at
 LIMIT $2`;

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
  // key -> this tick's dispatched invocation id (one PUT attempt per reservation).
  const invocations = new Map<string, string>();
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
      const invocationId = randomUUID();
      const { rows } = await deps.db.query<{ result_key: string }>(RESERVE_SQL, [job.id, job.leaseToken, job.sourceRevision, key, invocationId]);
      if (rows[0]) invocations.set(key, invocationId);
      return rows[0]?.result_key ?? null;
    },
    async put(key, bytes, sha256, job) {
      const invocationId = invocations.get(key);
      // The shared bounded PUT: a definite refusal is the request's own refused evidence; a
      // timeout, abort, error or lost response leaves the invocation DISPATCHED (unresolved).
      const out = await putAnalysisResult(deps.store, key, bytes, sha256, deps.putTimeoutMs, {
        onUnknown: async () => {},
        onRefused: async () => {
          if (job && invocationId) await analysisObligationWriter.refused(deps.db, key, invocationId, workerFence(job));
        },
      });
      // Only the PUT's OWN ack (its exact version id) is evidence; a version learnt from a read is not.
      if (job && invocationId && out.ackedVersionId)
        await analysisObligationWriter.acked(deps.db, key, invocationId, out.ackedVersionId, workerFence(job));
      return out.versionId ? { versionId: out.versionId } : {};
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
        invocations.get(stored.key) ?? "00000000-0000-0000-0000-000000000000",
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

/** A writer's fence the request evidence must match (worker: lease token + claimed source revision). */
export interface WriterFence {
  writer: string;
  generation: string | null;
}
export const workerFence = (job: Pick<AnalysisJob, "leaseToken" | "sourceRevision">): WriterFence => ({ writer: job.leaseToken, generation: job.sourceRevision });

/**
 * F1's ObligationWriter (write-obligation.ts v3) over the ticket + invocation tables.
 *   dispatch  the worker's is RESERVE_SQL (the ticket + a NEW invocation id, in the job-lease and
 *             source-revision statement, committed before the PUT); the backfill's is its fenced
 *             ticket insert. This method is the same contract for other callers.
 *   acked / refused  only the request's OWN evidence, writer- and generation-scoped.
 *   publish  the worker's is COMPLETE_SQL, the backfill's its job insert: both take the ticket row
 *             lock, refuse a reserved version and record the owned version.
 */
export function analysisObligationWriterFor(fence: WriterFence) {
  return {
    async dispatch(tx: SqlExecutor, key: string, identity: readonly unknown[], invocationId: string) {
      const [orgId, workspaceId, sessionId, jobId] = identity as [string, string, string, string];
      await tx.query(
        `WITH t AS (
           INSERT INTO sessions.session_analysis_result_tickets (result_key, job_id, org_id, workspace_id, session_id, reason, writer_token, generation)
           VALUES ($1, $2::uuid, $3, $4, $5, 'publishing', $6::uuid, $7)
           ON CONFLICT (result_key) DO UPDATE SET rev = nextval('sessions.session_analysis_ticket_rev_seq')
           RETURNING result_key
         )
         INSERT INTO sessions.session_analysis_ticket_invocations (invocation_id, result_key, writer_token, generation)
         SELECT $8::uuid, result_key, $6::uuid, $7 FROM t`,
        [key, jobId, orgId, workspaceId, sessionId, fence.writer, fence.generation, invocationId],
      );
    },
    acked: (tx: SqlExecutor, key: string, invocationId: string, versionId: string) =>
      tx.query(INVOCATION_ACKED_SQL, [key, invocationId, versionId, fence.writer, fence.generation]).then(() => {}),
    refused: (tx: SqlExecutor, key: string, invocationId: string) =>
      tx.query(INVOCATION_REFUSED_SQL, [key, invocationId, fence.writer, fence.generation]).then(() => {}),
    async publish(tx: SqlExecutor, key: string, identity: readonly unknown[], versionId: string) {
      const [, , , jobId] = identity as [string, string, string, string];
      const { rows } = await tx.query<{ ok: boolean }>(
        `WITH lk AS (
           SELECT result_key FROM sessions.session_analysis_result_tickets
            WHERE result_key = $1 AND NOT ($3::text = ANY (reserved_versions)) FOR UPDATE
         ), owned AS (
           UPDATE sessions.session_analysis_jobs_v2 SET result_version_id = $3
            WHERE id = $2::uuid AND result_s3_key = $1 AND EXISTS (SELECT 1 FROM lk)
              AND NOT EXISTS (SELECT 1 FROM sessions.session_analysis_ticket_reservations WHERE result_key = $1 AND version_id = $3)
           RETURNING id
         ), bump AS (
           UPDATE sessions.session_analysis_result_tickets SET rev = nextval('sessions.session_analysis_ticket_rev_seq')
            WHERE result_key = $1 AND EXISTS (SELECT 1 FROM owned) RETURNING result_key
         )
         SELECT EXISTS (SELECT 1 FROM owned) AS ok`,
        [key, jobId, versionId],
      );
      if (!rows[0]?.ok) throw new Error("analysis_publish_refused");
    },
  } satisfies ObligationWriter<SqlExecutor> & Record<string, unknown>;
}
/** Back-compat shape used by the worker's put (fence passed per call). */
export const analysisObligationWriter = {
  acked: (tx: SqlExecutor, key: string, invocationId: string, versionId: string, fence: WriterFence) =>
    analysisObligationWriterFor(fence).acked(tx, key, invocationId, versionId),
  refused: (tx: SqlExecutor, key: string, invocationId: string, fence: WriterFence) =>
    analysisObligationWriterFor(fence).refused(tx, key, invocationId),
};

const TICKET_STATE_SQL = `
SELECT t.rev::text AS rev, t.org_id, t.workspace_id, t.session_id, t.job_id::text AS job_id, t.generation, t.writer_token::text AS writer_token,
       t.erase_checkpoint,
       ARRAY(SELECT jsonb_build_object('id', i.invocation_id::text, 'outcome', i.outcome, 'versionId', i.version_id)
               FROM sessions.session_analysis_ticket_invocations AS i WHERE i.result_key = t.result_key ORDER BY i.invocation_id) AS invocations,
       ARRAY(SELECT j.result_version_id FROM sessions.session_analysis_jobs_v2 AS j
              WHERE j.result_s3_key = t.result_key AND j.status = 'completed' AND j.result_version_id IS NOT NULL
              ORDER BY 1) AS owned,
       (SELECT count(*) FROM sessions.session_analysis_jobs_v2 AS j
         WHERE j.result_s3_key = t.result_key AND j.status = 'completed' AND j.result_version_id IS NULL)::int AS without_version
  FROM sessions.session_analysis_result_tickets AS t WHERE t.result_key = $1`;

/** One ticket CAS: lock the row, require the loaded rev, apply `work`, bump the rev; commits on its own. */
async function casTicket(
  transact: <T>(work: (tx: SqlExecutor) => Promise<T>) => Promise<T>,
  key: string,
  rev: number,
  work: (tx: SqlExecutor) => Promise<boolean>,
  deleteRow = false,
): Promise<Cas> {
  return transact(async (tx) => {
    const cur = (await tx.query<{ rev: string }>("SELECT rev::text AS rev FROM sessions.session_analysis_result_tickets WHERE result_key = $1 FOR UPDATE", [key])).rows[0];
    if (!cur || Number(cur.rev) !== rev) return { ok: false } as const;
    if (!(await work(tx))) return { ok: false } as const;
    if (deleteRow) {
      await tx.query("DELETE FROM sessions.session_analysis_ticket_invocations WHERE result_key = $1", [key]);
      await tx.query("DELETE FROM sessions.session_analysis_result_tickets WHERE result_key = $1", [key]);
      return { ok: true, rev } as const;
    }
    const next = (await tx.query<{ rev: string }>(
      "UPDATE sessions.session_analysis_result_tickets SET rev = nextval('sessions.session_analysis_ticket_rev_seq') WHERE result_key = $1 RETURNING rev::text AS rev", [key],
    )).rows[0]!;
    return { ok: true, rev: Number(next.rev) } as const;
  });
}

/**
 * F1's ObligationStore (write-obligation.ts v3) for one ticket. Every mutation is a CAS on the
 * loaded rev in ITS OWN committed transaction (no remote await inside it), so it is durable before
 * the deletes that follow. `reserve` takes the same ticket row lock as every publishing path
 * (COMPLETE_SQL, the backfill's job insert, analysisObligationWriterFor().publish) and refuses if
 * any version is owned now; reservations are permanent.
 */
export function ticketObligationStore(transact: <T>(work: (tx: SqlExecutor) => Promise<T>) => Promise<T>, key: string): ObligationStore {
  return {
    async load(): Promise<ObligationState | null> {
      const row = await transact(async (tx) =>
        (await tx.query<{
          rev: string; org_id: string; workspace_id: string; session_id: string; job_id: string; generation: string | null;
          writer_token: string | null; erase_checkpoint: EraseCheckpoint | null; invocations: { id: string; outcome: InvocationOutcome; versionId: string | null }[] | null;
          owned: string[] | null; without_version: number;
        }>(TICKET_STATE_SQL, [key])).rows[0]);
      if (!row) return null;
      return {
        key,
        identity: ["analysis-result-ticket", row.org_id, row.workspace_id, row.session_id, row.job_id, row.generation, row.writer_token],
        rev: Number(row.rev),
        invocations: (row.invocations ?? []).map((i) => ({ id: i.id, outcome: i.outcome, ...(i.versionId ? { versionId: i.versionId } : {}) })),
        ownedVersions: row.owned ?? [],
        ownersWithoutVersion: Number(row.without_version),
        checkpoint: row.erase_checkpoint,
      };
    },
    reserve: (rev, versionIds) =>
      casTicket(transact, key, rev, async (tx) => {
        const owned = (await tx.query("SELECT 1 FROM sessions.session_analysis_jobs_v2 WHERE result_s3_key = $1 AND status = 'completed' AND result_version_id = ANY($2::text[]) LIMIT 1", [key, [...versionIds]])).rows;
        if (owned.length) return false;
        await tx.query(
          "INSERT INTO sessions.session_analysis_ticket_reservations (result_key, version_id) SELECT $1, v FROM unnest($2::text[]) AS v ON CONFLICT DO NOTHING",
          [key, [...versionIds]],
        );
        // On the locked row too, where every publish's FOR UPDATE re-checks it (no stale snapshot).
        await tx.query(
          "UPDATE sessions.session_analysis_result_tickets SET reserved_versions = ARRAY(SELECT DISTINCT unnest(reserved_versions || $2::text[]) ORDER BY 1) WHERE result_key = $1",
          [key, [...versionIds]],
        );
        return true;
      }),
    saveCheckpoint: (rev, checkpoint) =>
      casTicket(transact, key, rev, async (tx) => {
        await tx.query("UPDATE sessions.session_analysis_result_tickets SET erase_checkpoint = $2::jsonb WHERE result_key = $1", [key, JSON.stringify(checkpoint)]);
        return true;
      }),
    resolve: (rev, ids) =>
      casTicket(transact, key, rev, async (tx) => {
        await tx.query("DELETE FROM sessions.session_analysis_ticket_invocations WHERE result_key = $1 AND invocation_id::text = ANY($2::text[])", [key, [...ids]]);
        return true;
      }),
    reschedule: (rev) =>
      casTicket(transact, key, rev, async (tx) => {
        await tx.query(
          `UPDATE sessions.session_analysis_result_tickets
              SET erase_checkpoint = NULL, next_sweep_at = now() + $2::double precision * interval '1 millisecond', sweeps = sweeps + 1
            WHERE result_key = $1`,
          [key, nextSweepDelayMs(Number((await tx.query<{ s: number }>("SELECT sweeps s FROM sessions.session_analysis_result_tickets WHERE result_key = $1", [key])).rows[0]?.s ?? 0))],
        );
        return true;
      }),
    retire: (rev) => casTicket(transact, key, rev, async () => true, true),
  };
}

/**
 * Sweep the result tickets: each ticket is one key's write obligation, reconciled by THE shared
 * implementation (write-obligation.ts v3 reconcileObligation over the strict paged eraser).
 * Successor-safe (owned versions never selected), every doomed version reserved under the publish
 * row lock before its delete, every mutation a CAS committed on its own, the checkpoint scope bound
 * to identity + owned + invocation set. A dispatched invocation (lost response, timeout, crash) is
 * unresolved indefinitely: the ticket is rescheduled, never retired, until that request's own
 * acked/refused evidence. A failed step keeps everything; the sweep continues. Returns retired.
 */
export async function sweepAnalysisTickets(
  store: ObjectStore,
  options: {
    graceSeconds?: number;
    limit?: number;
    transact: <T>(work: (tx: SqlExecutor) => Promise<T>) => Promise<T>;
    /** REQUIRED: the shared paged eraser (createS3Eraser / storeEraser). No plain-delete fallback. */
    eraser: PagedEraser;
    /** Per-ticket budget for one step (the eraser's hard ceilings still apply). */
    budget?: EraseBudget;
    /** A ticket's step failed or its CAS was refused (it stays); the sweep continues. */
    onError?: (key: string, error: unknown) => void;
  },
): Promise<number> {
  if (typeof options?.eraser?.eraseStep !== "function" || typeof options.eraser.verifyErased !== "function")
    throw new Error("analysis_sweep_eraser_required");
  void store;
  const { rows } = await options.transact((tx) =>
    tx.query<{ result_key: string }>(SWEEP_SQL, [options.graceSeconds ?? TICKET_GRACE_SECONDS, options.limit ?? 50]));
  let retired = 0;
  for (const { result_key } of rows) {
    try {
      const outcome = await reconcileObligation(options.eraser, ticketObligationStore(options.transact, result_key), options.budget ? { budget: options.budget } : {});
      if (outcome === "retired") retired += 1;
      else if (outcome === "refused") options.onError?.(result_key, new Error("analysis_sweep_cas_refused"));
    } catch (error) {
      options.onError?.(result_key, error);
    }
  }
  return retired;
}
