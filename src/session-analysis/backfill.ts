/**
 * Backfill of Neon session-analysis history into migration 20261105046000 (lane F1,
 * map F1.7) and the byte-parity check (F1.8). Run once per canonical scope before the
 * Neon drop, by an operator, against the Neon read-only role or the frozen copy.
 *
 * Input: the exact public v1 payload Neon's read_session_analysis_v1 serves for the scope
 * (`readLegacyAnalysisSnapshot`, the SELECT-only 0024/0031 oracle already in
 * src/session-pipeline/analysis-v1-select.ts). Output: requests_v2 / jobs_v2 rows with
 * origin 'neon_backfill' that keep every public v1 value, and one result object per
 * completed analysis holding Neon's exact v1 `result` (brief already merged).
 *
 * Rules (independent review P1-3 / P0-2 / P2):
 *   - In-flight Neon work (a queued or running job or request) can never finish once Neon
 *     is retired, so it is terminalized: `failed` with `analysis_migration_interrupted`.
 *     `expectedAfterBackfill(neon)` is the same terminalization of Neon's payload, and is
 *     what parity compares against.
 *   - The result prefix is derived from the scope's live anchor pointer, never supplied by
 *     the caller; a scope with no live pointer (erased) is refused.
 *   - Erase-safe: each object is ticketed (with the scope live) before it is written, and
 *     its row is inserted and the ticket retired in one statement that re-checks liveness.
 *     An erase that wins leaves a ticket the worker's sweeper uses to delete the object.
 *   - Idempotent under drift: a row that already exists must equal the plan exactly
 *     (`backfill_existing_row_drift` otherwise), and `backfillScope` enforces parityEqual
 *     between the expected payload and SP's GET before it reports success.
 */
import { createHash } from "node:crypto";
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import { objectKey, readAll, type ObjectStore } from "../../packages/object-store/src/index.ts";
import { randomUUID } from "node:crypto";
import { analysisResultKey, analysisResultPrefix, ANALYSIS_RESULT_MAX_BYTES, boundedPut, type AnalysisPutAck } from "./results.ts";

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

export const MIGRATION_INTERRUPTED = "analysis_migration_interrupted";
/** A backfill ticket's writer lease; the sweeper never sweeps a ticket under a live lease. */
export const BACKFILL_TICKET_LEASE_SECONDS = 600;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/u;
const STATUS = new Set(["queued", "running", "completed", "failed"]);
const IN_FLIGHT = new Set(["queued", "running"]);
const str = (v: unknown, code: string): string => {
  if (typeof v !== "string") throw new BackfillError(code);
  return v;
};
const nullableStr = (v: unknown, code: string): string | null => (v === null ? null : str(v, code));

/** Neon's payload as SP serves it after the backfill: in-flight work terminalized. */
export function expectedAfterBackfill(payload: NeonV1Payload): NeonV1Payload {
  if (payload.outcome === "not_found") return payload;
  const end = <T extends Json>(row: T): T =>
    IN_FLIGHT.has(String(row.status)) ? { ...row, status: "failed", error: MIGRATION_INTERRUPTED } : row;
  return { ...payload, requests: payload.requests.map(end), analyses: payload.analyses.map(end) };
}

export interface BackfillPlan {
  jobs: Array<Json & { id: string; result: unknown }>;
  requests: Json[];
  terminalized: number;
}

/** Validate the Neon payload and plan v2 rows. Refuses anything it cannot keep exactly. */
export function planBackfill(scope: BackfillScope, payload: NeonV1Payload): BackfillPlan {
  if (payload.outcome === "not_found") return { jobs: [], requests: [], terminalized: 0 };
  if (payload.sessionId !== scope.sessionId) throw new BackfillError("backfill_session_mismatch");
  for (const row of [...payload.analyses, ...payload.requests]) {
    if (!STATUS.has(String(row.status)) && row.status !== "resolved") throw new BackfillError("backfill_status_invalid");
  }
  const terminalized = [...payload.analyses, ...payload.requests].filter((r) => IN_FLIGHT.has(String(r.status))).length;
  const expected = expectedAfterBackfill(payload) as Extract<NeonV1Payload, { outcome: "ok" }>;
  const jobs = expected.analyses.map((a) => {
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
  const requests = expected.requests.map((r) => {
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
  return { jobs, requests, terminalized };
}

const LIVE = `EXISTS (SELECT 1 FROM sessions.session_index AS p
   WHERE p.org_id = $2 AND p.workspace_id = $3
     AND (coalesce(p.canonical_session_id, p.session_id) = $4 OR p.session_id = $4) AND p.deleted_at IS NULL)`;
/** The planned source generation ($8) is still current: pointer set and every proof input. */
const GENERATION = "sessions.session_analysis_source_revision($2, $3, $4) = $8::text";

async function sourceRevision(db: SqlExecutor, scope: BackfillScope): Promise<string | null> {
  const { rows } = await db.query<{ r: string | null }>(
    "SELECT sessions.session_analysis_source_revision($1, $2, $3) AS r",
    [scope.orgId, scope.workspaceId, scope.sessionId],
  );
  return rows[0]?.r ?? null;
}

/** The scope's anchor manifest (first live pointer), from which the result prefix derives. */
async function anchorPrefix(db: SqlExecutor, scope: BackfillScope): Promise<string> {
  const { rows } = await db.query<{ manifest_key: string }>(
    `SELECT p.manifest_key FROM sessions.session_index AS p
      WHERE p.org_id = $1 AND p.workspace_id = $2
        AND (coalesce(p.canonical_session_id, p.session_id) = $3 OR p.session_id = $3) AND p.deleted_at IS NULL
      ORDER BY p.harness, coalesce(p.canonical_machine_id, p.machine_id), p.manifest_key LIMIT 1`,
    [scope.orgId, scope.workspaceId, scope.sessionId],
  );
  if (!rows[0]) throw new BackfillError("backfill_session_not_live");
  return analysisResultPrefix(rows[0].manifest_key);
}

const JOB_COLUMNS = `id::text, harness, status, attempts, analysis_version, schema_version, model_version,
  summary_version, summary_model_version, source_fingerprint, result_s3_key, result_sha256, result_bytes,
  last_error, origin,
  (created_at = $5::timestamptz AND updated_at = $6::timestamptz
   AND completed_at IS NOT DISTINCT FROM $7::timestamptz) AS times_equal`;

/**
 * Per job: an existing row is proven identical BEFORE anything is written (drift refuses, no
 * object); otherwise ticket -> write -> insert, and the ticket is retired in the same
 * statement only when that statement inserted the row. A lost race (the row appeared
 * meanwhile) keeps the ticket until the winner is proven identical; a drifted winner leaves
 * the ticket, so the sweeper deletes the orphan object. Then the requests; then verify all.
 */
export async function applyBackfill(
  db: SqlExecutor,
  store: ObjectStore,
  scope: BackfillScope,
  plan: BackfillPlan,
  options: {
    /**
     * REQUIRED every-version erase of exactly one key (production: createS3Eraser().eraseObject).
     * The transcript bucket is versioned: a plain DELETE would only add a delete marker and
     * leave the bytes, so there is no fallback and the backfill refuses to run without it.
     * Returns the versions (and markers) removed; 0 means nothing is left.
     */
    eraseObject: (key: string) => Promise<number>;
    /**
     * Successor-safe cleanup of exactly the version this run's put created (production:
     * createS3Eraser().eraseVersion). Absent (an unversioned store): a fence-lost write is
     * left to the sweeper under a durable 'forgotten' ticket instead.
     */
    eraseVersion?: (key: string, versionId: string, opts?: { keepIfSole?: boolean }) => Promise<"gone" | "kept_sole">;
    /** The PUT's hard deadline (default ANALYSIS_PUT_TIMEOUT_MS, well inside the ticket lease). */
    putTimeoutMs?: number;
  },
): Promise<{ jobs: number; requests: number; objects: number }> {
  if (typeof options?.eraseObject !== "function") throw new BackfillError("backfill_eraser_required");
  const prefix = await anchorPrefix(db, scope);
  const scopeParams = [scope.orgId, scope.workspaceId, scope.sessionId];
  // This run's writer identity: every ticket fence names it, so a successor's live lease is
  // never mistaken for ours, and a takeover is possible only after our lease expired.
  const writer = randomUUID();
  // The source generation this backfill was planned against (pointer set + every proof
  // input): every ticket fence also requires it unchanged.
  const generation = await sourceRevision(db, scope);
  if (generation === null) throw new BackfillError("backfill_session_not_live");
  let objects = 0;
  const refs = new Map<string, { key: string; sha256: string; bytes: number }>();
  for (const job of plan.jobs) {
    try {
      await applyJob(job);
    } catch (error) {
      // A failed job releases its lease at once, so the sweeper may clear any orphan now
      // instead of after the lease runs out. Never touches a forgotten or foreign ticket.
      const key = refs.get(job.id)?.key;
      if (key)
        await db
          .query(
            "UPDATE sessions.session_analysis_result_tickets SET lease_expires_at = now() WHERE result_key = $1 AND job_id = $2::uuid AND reason = 'publishing' AND writer_token = $3::uuid",
            [key, job.id, writer],
          )
          .catch(() => {});
      throw error;
    }
  }
  /**
   * A write whose ownership was lost (lease, generation, scope) or whose PUT did not complete.
   * Successor-safe: never touches a successor's ticket and never deletes a version this run
   * did not create.
   *   1. Durable: if the ticket is still ours, it becomes a lease-free 'forgotten' ticket;
   *      with no ticket at all (swept) and no job row naming the key, one is created. A
   *      'forgotten' ticket cannot be taken over, so no successor can publish the key until
   *      the sweeper (every-version erase, verified) clears it. A successor's ticket or a
   *      published job row is left alone.
   *   2. Exact: when the PUT returned the version id it created, that one version is erased
   *      (kept only when it is the key's sole object version and a successor holds the key:
   *      a successor whose create-only put found identical bytes publishes that version), and
   *      our durable ticket is retired only after the erase is verified. Otherwise (timeout,
   *      error, unversioned store) the ticket stays for the sweeper.
   */
  async function abandonWrite(key: string, jobId: string, ack: AnalysisPutAck | null) {
    const mine = await db.query(
      `/* backfill:fence-lost */ WITH ours AS (
         UPDATE sessions.session_analysis_result_tickets
            SET reason = 'forgotten', lease_expires_at = NULL
          WHERE result_key = $1 AND job_id = $5::uuid AND writer_token = $6::uuid
          RETURNING result_key
       ), adopted AS (
         INSERT INTO sessions.session_analysis_result_tickets
           (result_key, job_id, org_id, workspace_id, session_id, reason, lease_expires_at, writer_token)
         SELECT $1, $5::uuid, $2, $3, $4, 'forgotten', NULL, $6::uuid
          WHERE NOT EXISTS (SELECT 1 FROM ours)
            AND NOT EXISTS (SELECT 1 FROM sessions.session_analysis_jobs_v2 AS j WHERE j.result_s3_key = $1)
         ON CONFLICT (result_key) DO NOTHING
         RETURNING result_key
       )
       SELECT (SELECT count(*) FROM ours) + (SELECT count(*) FROM adopted) AS n`,
      [key, ...scopeParams, jobId, writer],
    );
    const held = Number((mine.rows[0] as { n?: number | string } | undefined)?.n ?? 0) > 0;
    if (!ack?.created || !ack.versionId || !options.eraseVersion) return;
    const outcome = await options.eraseVersion(key, ack.versionId, { keepIfSole: !held });
    if (held && outcome === "gone")
      await db.query(
        "DELETE FROM sessions.session_analysis_result_tickets WHERE result_key = $1 AND job_id = $2::uuid AND reason = 'forgotten' AND writer_token = $3::uuid",
        [key, jobId, writer],
      );
  }
  async function applyJob(job: BackfillPlan["jobs"][number]) {
    let ref: { key: string; sha256: string; bytes: number } | null = null;
    let bytes: Uint8Array | null = null;
    if (job.result !== null) {
      bytes = new TextEncoder().encode(JSON.stringify(job.result));
      if (bytes.length > ANALYSIS_RESULT_MAX_BYTES) throw new BackfillError("backfill_result_bound");
      const sha256 = createHash("sha256").update(bytes).digest("hex");
      ref = { key: analysisResultKey(prefix, job.id, sha256), sha256, bytes: bytes.length };
      refs.set(job.id, ref);
    }
    // An existing row (a re-run, or another backfill) must equal the plan before any write.
    if (await jobRowExists(db, scope, job.id)) {
      await verifyJob(db, scope, job, ref);
      return;
    }
    if (ref && bytes) {
      // Ticket first, only while the scope is live: an erase from here on leaves the ticket.
      // Leased: the sweeper skips it while the lease is live (P1-c).
      const ticket = await db.query(
        `INSERT INTO sessions.session_analysis_result_tickets
           (result_key, job_id, org_id, workspace_id, session_id, reason, lease_expires_at, writer_token)
         SELECT $1, $5::uuid, $2, $3, $4, 'publishing', now() + make_interval(secs => $6::integer), $7::uuid
          WHERE ${LIVE} AND ${GENERATION}
         ON CONFLICT (result_key) DO UPDATE
           SET created_at = now(), lease_expires_at = excluded.lease_expires_at, writer_token = excluded.writer_token
           WHERE sessions.session_analysis_result_tickets.reason = 'publishing'
             AND sessions.session_analysis_result_tickets.job_id = excluded.job_id
             -- A takeover only after the holder's lease expired (its PUT was bounded inside it).
             AND (sessions.session_analysis_result_tickets.lease_expires_at IS NULL
                  OR sessions.session_analysis_result_tickets.lease_expires_at <= now()
                  OR sessions.session_analysis_result_tickets.writer_token = excluded.writer_token)
         RETURNING result_key`,
        [ref.key, ...scopeParams, job.id, BACKFILL_TICKET_LEASE_SECONDS, writer, generation],
      );
      if (!ticket.rows.length) throw new BackfillError("backfill_session_not_live");
      // Erase barrier: immediately before the Put, the ticket must still be ours, 'publishing'
      // and under a LIVE lease (heartbeat extends it). A lapsed, swept or forgotten ticket
      // refuses the Put, so no object can land after a sweep that already ran.
      // Erase barrier: immediately before the Put, the ticket must still be OURS (writer),
      // 'publishing', under a live lease, at the planned source generation.
      const beat = await db.query(
        `/* backfill:heartbeat */ UPDATE sessions.session_analysis_result_tickets
            SET lease_expires_at = now() + make_interval(secs => $6::integer)
          WHERE result_key = $1 AND job_id = $5::uuid AND reason = 'publishing' AND writer_token = $7::uuid
            AND lease_expires_at > now() AND ${GENERATION}
          RETURNING result_key`,
        [ref.key, ...scopeParams, job.id, BACKFILL_TICKET_LEASE_SECONDS, writer, generation],
      );
      if (!beat.rows.length) throw new BackfillError("backfill_ticket_lease_lost");
      // Bounded: aborted at the deadline, well inside the lease the heartbeat just renewed.
      let ack: AnalysisPutAck | null = null;
      let putError: unknown = null;
      try {
        ack = await boundedPut(store, ref.key, bytes, ref.sha256, options.putTimeoutMs);
      } catch (error) {
        putError = error;
      }
      // Post-write ownership fence: ours (writer), 'publishing', lease live, scope live, and the
      // source generation unchanged.
      const owned = putError
        ? { rows: [] }
        : await db.query(
            `/* backfill:post-put */ UPDATE sessions.session_analysis_result_tickets
                SET lease_expires_at = now() + make_interval(secs => $6::integer)
              WHERE result_key = $1 AND job_id = $5::uuid AND reason = 'publishing' AND writer_token = $7::uuid
                AND lease_expires_at > now() AND ${LIVE} AND ${GENERATION}
              RETURNING result_key`,
            [ref.key, ...scopeParams, job.id, BACKFILL_TICKET_LEASE_SECONDS, writer, generation],
          );
      if (!owned.rows.length) {
        await abandonWrite(ref.key, job.id, ack);
        if (putError) throw putError;
        throw new BackfillError("backfill_ticket_lease_lost");
      }
      const read = await readAll(await store.get(objectKey(ref.key)));
      if (createHash("sha256").update(read).digest("hex") !== ref.sha256) throw new BackfillError("backfill_write_verification_failed");
      objects += 1;
    }
    const { rows } = await db.query<{ live: boolean; inserted: number; current: boolean }>(
      `WITH ins AS (
         INSERT INTO sessions.session_analysis_jobs_v2
           (id, org_id, workspace_id, session_id, harness, status, attempts, analysis_version, schema_version,
            model_version, summary_version, summary_model_version, source_fingerprint, result_s3_key, result_sha256,
            result_bytes, last_error, origin, created_at, updated_at, completed_at)
         SELECT $1::uuid, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, 'neon_backfill',
                $18::timestamptz, $19::timestamptz, $20::timestamptz
          WHERE ${LIVE} AND sessions.session_analysis_source_revision($2, $3, $4) = $22::text
         ON CONFLICT (id) DO NOTHING RETURNING id
       ), retired AS (
         DELETE FROM sessions.session_analysis_result_tickets AS t
          WHERE $14::text IS NOT NULL AND t.result_key = $14 AND t.job_id = $1::uuid AND t.reason = 'publishing'
            AND t.writer_token = $21::uuid AND EXISTS (SELECT 1 FROM ins)
         RETURNING t.result_key
       )
       SELECT ${LIVE} AS live, (SELECT count(*) FROM ins)::int AS inserted,
              sessions.session_analysis_source_revision($2, $3, $4) IS NOT DISTINCT FROM $22::text AS current`,
      [job.id, ...scopeParams, job.harness, job.status, job.attempts, job.analysis_version, job.schema_version,
       job.model_version, job.summary_version, job.summary_model_version, job.source_fingerprint, ref?.key ?? null,
       ref?.sha256 ?? null, ref?.bytes ?? null, job.last_error, job.created_at, job.updated_at, job.completed_at, writer, generation],
    );
    if (!rows[0]?.live) throw new BackfillError("backfill_session_not_live");
    if (!(rows[0] as { current?: boolean }).current) throw new BackfillError("backfill_generation_changed");
    if (Number(rows[0]?.inserted) !== 1) {
      // Lost the race: the winner must be identical (throws drift and KEEPS the ticket otherwise).
      await verifyJob(db, scope, job, ref);
      // Retire atomically: only our still-'publishing' ticket, only while the scope is live, and
      // only if the winning row is still this job holding exactly this object. A ticket that an
      // erase turned 'forgotten' (or a scope erased meanwhile) is never deleted here.
      if (ref)
        await db.query(
          `/* backfill:retire-lost-race */ DELETE FROM sessions.session_analysis_result_tickets AS t
            WHERE t.result_key = $5 AND t.job_id = $1::uuid AND t.reason = 'publishing' AND t.writer_token = $8::uuid AND ${LIVE}
              AND EXISTS (SELECT 1 FROM sessions.session_analysis_jobs_v2 AS j
                           WHERE j.id = $1::uuid AND j.org_id = $2 AND j.workspace_id = $3 AND j.session_id = $4
                             AND j.result_s3_key = $5 AND j.result_sha256 = $6 AND j.result_bytes = $7)`,
          [job.id, ...scopeParams, ref.key, ref.sha256, ref.bytes, writer],
        );
    }
  }
  for (const r of plan.requests) {
    await db.query(
      `INSERT INTO sessions.session_analysis_requests_v2
         (id, org_id, workspace_id, session_id, requested_by_user_id, status, analysis_job_id, v1_job_id,
          last_error, origin, created_at, updated_at)
       SELECT $1::uuid, $2, $3, $4, 'neon_backfill', $5, $6::uuid, $7::uuid, $8, 'neon_backfill',
              $9::timestamptz, $10::timestamptz WHERE ${LIVE}
       ON CONFLICT (id) DO NOTHING`,
      [r.id, ...scopeParams, r.status, r.analysis_job_id, r.v1_job_id, r.last_error, r.created_at, r.updated_at],
    );
  }
  await verifyBackfill(db, scope, plan, refs);
  return { jobs: plan.jobs.length, requests: plan.requests.length, objects };
}

/** Whether a row with this id exists; one in another scope is drift, never this scope's. */
async function jobRowExists(db: SqlExecutor, scope: BackfillScope, id: string): Promise<boolean> {
  const { rows } = await db.query<{ same: boolean }>(
    "SELECT (org_id = $2 AND workspace_id = $3 AND session_id = $4) AS same FROM sessions.session_analysis_jobs_v2 WHERE id = $1::uuid",
    [id, scope.orgId, scope.workspaceId, scope.sessionId],
  );
  if (!rows.length) return false;
  if (rows[0]!.same !== true) throw new BackfillError("backfill_existing_row_drift");
  return true;
}

/** One planned job row exists exactly as planned, result key, digest and size included. */
async function verifyJob(
  db: SqlExecutor,
  scope: BackfillScope,
  job: BackfillPlan["jobs"][number],
  ref: { key: string; sha256: string; bytes: number } | null,
): Promise<void> {
  const { rows } = await db.query<Json>(
    `SELECT ${JOB_COLUMNS} FROM sessions.session_analysis_jobs_v2 WHERE id = $1::uuid AND org_id = $2 AND workspace_id = $3 AND session_id = $4`,
    [job.id, scope.orgId, scope.workspaceId, scope.sessionId, job.created_at, job.updated_at, job.completed_at],
  );
  const row = rows[0];
  if (
    !row ||
    row.origin !== "neon_backfill" ||
    row.status !== job.status ||
    Number(row.attempts) !== job.attempts ||
    row.harness !== job.harness ||
    row.analysis_version !== job.analysis_version ||
    Number(row.schema_version) !== job.schema_version ||
    row.model_version !== job.model_version ||
    row.summary_version !== job.summary_version ||
    row.summary_model_version !== job.summary_model_version ||
    row.source_fingerprint !== job.source_fingerprint ||
    row.last_error !== job.last_error ||
    (row.result_s3_key ?? null) !== (ref?.key ?? null) ||
    (row.result_sha256 ?? null) !== (ref?.sha256 ?? null) ||
    (row.result_bytes === null || row.result_bytes === undefined ? null : Number(row.result_bytes)) !== (ref?.bytes ?? null) ||
    row.times_equal !== true
  )
    throw new BackfillError("backfill_existing_row_drift");
}

/** Every planned row exists exactly as planned (an existing row that drifted is refused). */
export async function verifyBackfill(
  db: SqlExecutor,
  scope: BackfillScope,
  plan: BackfillPlan,
  refs: Map<string, { key: string; sha256: string; bytes: number }>,
): Promise<void> {
  for (const job of plan.jobs) await verifyJob(db, scope, job, refs.get(job.id) ?? null);
  for (const r of plan.requests) {
    const { rows } = await db.query<Json>(
      `SELECT status, analysis_job_id::text, v1_job_id::text, last_error, origin,
              (created_at = $5::timestamptz AND updated_at = $6::timestamptz) AS times_equal
         FROM sessions.session_analysis_requests_v2
        WHERE id = $1::uuid AND org_id = $2 AND workspace_id = $3 AND session_id = $4`,
      [r.id, scope.orgId, scope.workspaceId, scope.sessionId, r.created_at, r.updated_at],
    );
    const row = rows[0];
    if (!row || row.origin !== "neon_backfill" || row.status !== r.status || row.analysis_job_id !== r.analysis_job_id ||
        row.v1_job_id !== r.v1_job_id || row.last_error !== r.last_error || row.times_equal !== true)
      throw new BackfillError("backfill_existing_row_drift");
  }
}

/** relayhistory evidence.ts canonical JSON: sorted keys, so key order never decides parity. */
export function canonicalJson(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  if (value !== null && typeof value === "object")
    return `{${Object.keys(value as Json).sort().map((k) => `${JSON.stringify(k)}:${canonicalJson((value as Json)[k])}`).join(",")}}`;
  return JSON.stringify(value) ?? "null";
}

/**
 * F1.8 parity for one scope: the expected payload against SP's GET body (requests and
 * analyses only; correlationId is per call). Returns no content, only the verdict.
 */
export function parityEqual(neon: NeonV1Payload, sp: { sessionId: string; requests: unknown[]; analyses: unknown[] } | null) {
  if (neon.outcome === "not_found") return { equal: sp === null, requests: 0, analyses: 0 };
  if (!sp) return { equal: false, requests: neon.requests.length, analyses: neon.analyses.length };
  const pick = (v: { sessionId: string; requests: unknown[]; analyses: unknown[] }) =>
    canonicalJson({ sessionId: v.sessionId, requests: v.requests, analyses: v.analyses });
  return { equal: pick(neon) === pick(sp), requests: neon.requests.length, analyses: neon.analyses.length };
}

/** One scope end to end: plan, apply, then the parity gate against SP's own GET. */
export async function backfillScope(
  deps: {
    db: SqlExecutor;
    store: ObjectStore;
    readSp(): Promise<{ sessionId: string; requests: unknown[]; analyses: unknown[] } | null>;
    /** REQUIRED every-version erase (see applyBackfill). */
    eraseObject: (key: string) => Promise<number>;
    /** Exact-version cleanup of this run's own write (see applyBackfill). */
    eraseVersion?: (key: string, versionId: string, opts?: { keepIfSole?: boolean }) => Promise<"gone" | "kept_sole">;
    putTimeoutMs?: number;
  },
  scope: BackfillScope,
  neon: NeonV1Payload,
) {
  const plan = planBackfill(scope, neon);
  const applied = await applyBackfill(deps.db, deps.store, scope, plan, {
    eraseObject: deps.eraseObject,
    ...(deps.eraseVersion ? { eraseVersion: deps.eraseVersion } : {}),
    ...(deps.putTimeoutMs !== undefined ? { putTimeoutMs: deps.putTimeoutMs } : {}),
  });
  const parity = parityEqual(expectedAfterBackfill(neon), await deps.readSp());
  if (!parity.equal) throw new BackfillError("backfill_parity_failed");
  return { ...applied, terminalized: plan.terminalized, parity };
}
