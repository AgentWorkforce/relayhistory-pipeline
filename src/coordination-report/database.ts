import { createHash } from "node:crypto";
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import { canonicalPointerGeneration } from "../session-read-api/canonical.ts";
import type { HoopDb } from "../shared/db.ts";
import type {
  CanonicalIdentity,
  CanonicalPointer,
} from "../session-read-api/canonical.ts";
import type {
  ReportSourceDeps,
  ReportSourceRequest,
  ReportDirectorySnapshot,
} from "./source.ts";
export interface ReportClaim {
  jobId: string;
  leaseId: string;
  request: ReportSourceRequest;
}
export interface StoredReportJob {
  id: string;
  status: string;
  result: unknown | null;
  error_code: string | null;
}

/** Keep the report lane independent of shared/db's filesystem-backed verifier assets. */
async function reportTransaction<T>(
  db: Pick<HoopDb, "pool">,
  work: (tx: SqlExecutor) => Promise<T>,
): Promise<T> {
  const client = await db.pool.connect();
  try {
    await client.query("BEGIN");
    const tx: SqlExecutor = {
      async query<R = Record<string, unknown>>(text: string, params?: unknown[]) {
        const result = await client.query(text, params as unknown[]);
        return { rows: result.rows as R[] };
      },
    };
    const result = await work(tx);
    await client.query("COMMIT");
    return result;
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    throw error;
  } finally {
    client.release();
  }
}

/** Tenant authority is supplied by trusted worker configuration, never request JSON. */
export function createReportDatabase(
  db: HoopDb,
  trustedScope: ReportSourceDeps["trustedScope"],
) {
  const scope = [trustedScope.organizationId, trustedScope.workspaceId];
  function authorize(request: ReportSourceRequest) {
    if (
      request.scope.organizationId !== scope[0] ||
      request.scope.workspaceId !== scope[1]
    )
      throw new Error("coordination_database_scope_denied");
    if (
      !Number.isInteger(request.maxSessions) ||
      request.maxSessions < 1 ||
      request.maxSessions > 10000 ||
      !Number.isInteger(request.maxEvents) ||
      request.maxEvents < 1 ||
      request.maxEvents > 10000
    )
      throw new Error("coordination_database_budget_invalid");
    for (const v of [request.window.since, request.window.until])
      if (!Number.isFinite(Date.parse(v)) || new Date(v).toISOString() !== v)
        throw new Error("coordination_database_window_invalid");
    if (request.window.since >= request.window.until)
      throw new Error("coordination_database_window_invalid");
  }
  const api = {
    async listSessions(
      request: ReportSourceRequest,
      executor: Pick<HoopDb, "query"> = db,
    ): Promise<ReportDirectorySnapshot> {
      authorize(request);
      const { rows } = await executor.query<{
        complete: boolean;
        sessions: CanonicalIdentity[];
      }>(
        `SELECT NOT EXISTS(SELECT 1 FROM sessions.session_directory WHERE org_id=$1 AND workspace_id=$2 AND deleted_at IS NULL AND (NOT event_projection_ready OR event_projection_version<>1))
         AND NOT EXISTS(SELECT 1 FROM sessions.session_directory_event_pending WHERE org_id=$1 AND workspace_id=$2 AND dirty) complete,
         coalesce((SELECT jsonb_agg(jsonb_build_object('orgId',d.org_id,'workspaceId',d.workspace_id,'source',d.harness,'sessionId',d.session_id) ORDER BY d.harness,d.session_id)
          FROM (SELECT d.org_id,d.workspace_id,d.harness,d.session_id FROM sessions.session_directory d
           WHERE d.org_id=$1 AND d.workspace_id=$2 AND d.deleted_at IS NULL AND d.event_projection_ready
            AND EXISTS(SELECT 1 FROM sessions.session_directory_event_times t WHERE t.org_id=d.org_id AND t.workspace_id=d.workspace_id AND t.harness=d.harness AND t.session_id=d.session_id AND t.event_ts >= $3::timestamptz AND t.event_ts < $4::timestamptz)
           ORDER BY d.harness,d.session_id LIMIT $5) d),'[]'::jsonb) sessions`,
        [
          ...scope,
          request.window.since,
          request.window.until,
          request.maxSessions + 1,
        ],
      );
      const row = rows[0];
      if (!row) throw new Error("coordination_database_inventory_missing");
      const complete =
        row.complete && row.sessions.length <= request.maxSessions;
      return {
        sessions: row.sessions,
        complete,
        generation: createHash("sha256")
          .update(JSON.stringify([complete, row.sessions]))
          .digest("hex"),
      };
    },
    async pointers(
      identity: CanonicalIdentity,
      executor: Pick<HoopDb, "query"> = db,
    ): Promise<CanonicalPointer[]> {
      if (identity.orgId !== scope[0] || identity.workspaceId !== scope[1])
        throw new Error("coordination_database_scope_denied");
      const { rows } = await executor.query<CanonicalPointer>(
        `SELECT org_id,workspace_id,harness,coalesce(canonical_session_id,session_id) session_id,machine_id,user_id,
       storage_provider,bucket,manifest_key,encode(content_sha256,'hex') content_sha256,raw_format,revision::text revision,deleted_at
       FROM sessions.session_index WHERE org_id=$1 AND workspace_id=$2 AND harness=$3 AND coalesce(canonical_session_id,session_id)=$4 AND deleted_at IS NULL
       ORDER BY machine_id,manifest_key LIMIT 64`,
        [...scope, identity.source, identity.sessionId],
      );
      return rows;
    },
    async enqueue(
      request: ReportSourceRequest,
      idempotencyKey: string,
    ): Promise<string> {
      authorize(request);
      if (!/^[a-zA-Z0-9_.:/-]{1,256}$/.test(idempotencyKey))
        throw new Error("coordination_database_idempotency_invalid");
      const { rows } = await db.query<{ id: string }>(
        `INSERT INTO sessions.coordination_report_jobs(org_id,workspace_id,idempotency_key,request) VALUES($1,$2,$3,$4::jsonb)
       ON CONFLICT(org_id,workspace_id,idempotency_key) DO UPDATE SET updated_at=sessions.coordination_report_jobs.updated_at
       WHERE sessions.coordination_report_jobs.request=excluded.request RETURNING id`,
        [...scope, idempotencyKey, JSON.stringify(request)],
      );
      if (!rows[0])
        throw new Error("coordination_database_idempotency_conflict");
      return rows[0].id;
    },
    async claim(
      jobId: string,
      leaseSeconds = 300,
    ): Promise<ReportClaim | null> {
      if (
        !Number.isInteger(leaseSeconds) ||
        leaseSeconds < 1 ||
        leaseSeconds > 3600
      )
        throw new Error("coordination_database_lease_invalid");
      const { rows } = await db.query<{
        id: string;
        lease_id: string;
        request: ReportSourceRequest;
      }>(
        `UPDATE sessions.coordination_report_jobs SET status='running',lease_id=gen_random_uuid(),lease_expires_at=now()+make_interval(secs=>$4::int),attempts=attempts+1,error_code=NULL,updated_at=now()
       WHERE org_id=$1 AND workspace_id=$2 AND id=$3::uuid AND (status IN ('queued','failed','invalidated') OR (status='running' AND lease_expires_at<now())) RETURNING id,lease_id,request`,
        [...scope, jobId, leaseSeconds],
      );
      const row = rows[0];
      if (!row) return null;
      authorize(row.request);
      return { jobId: row.id, leaseId: row.lease_id, request: row.request };
    },
    async complete(
      claim: ReportClaim,
      directoryGeneration: string,
      result: unknown,
    ): Promise<boolean> {
      authorize(claim.request);
      if (typeof directoryGeneration !== "string" || !directoryGeneration)
        throw new Error("coordination_database_generation_invalid");
      const serialized = JSON.stringify(result);
      if (!serialized || Buffer.byteLength(serialized) > 16 * 1024 * 1024)
        throw new Error("coordination_database_result_bound");
      return reportTransaction(db, async (tx) => {
        const deadline = Date.now() + 20000;
        await tx.query("SET LOCAL statement_timeout = '10s'");
        await tx.query("SET LOCAL lock_timeout = '5s'");
        await tx.query(
          `SELECT pg_advisory_xact_lock(hashtextextended('coordination-report:'||length($1::text)::text||':'||$1::text||':'||$2::text,0))`,
          scope,
        );
        await tx.query(
          `INSERT INTO sessions.coordination_report_epochs(org_id,workspace_id,generation) VALUES($1,$2,0) ON CONFLICT DO NOTHING`,
          scope,
        );
        const { rows: epochs } = await tx.query<{ generation: string }>(
          `SELECT (generation+1)::text generation FROM sessions.coordination_report_epochs WHERE org_id=$1 AND workspace_id=$2 FOR UPDATE`,
          scope,
        );
        if (!epochs[0]) return false;
        // An exclusive advisory fence pairs with compatible shared ingestion fences.
        // The epoch row counts publications only; exact historical dependencies decide validity.
        const snapshot = (
          result as {
            sourceSnapshot?: {
              sessions?: Array<{
                identity: CanonicalIdentity;
                generation: string;
              }>;
            };
          }
        )?.sourceSnapshot;
        if (
          !snapshot ||
          !Array.isArray(snapshot.sessions) ||
          snapshot.sessions.length > claim.request.maxSessions
        )
          throw new Error("coordination_database_snapshot_invalid");
        const inventory = await api.listSessions(claim.request, tx);
        if (!inventory.complete) return false;
        const identityKey = (i: CanonicalIdentity) =>
          JSON.stringify([i.orgId, i.workspaceId, i.source, i.sessionId]);
        const selected = inventory.sessions.map(identityKey).sort();
        const dependencies = snapshot.sessions
          .map((s) => identityKey(s.identity))
          .sort();
        if (
          new Set(dependencies).size !== dependencies.length ||
          JSON.stringify(selected) !== JSON.stringify(dependencies)
        )
          return false;
        for (const dependency of snapshot.sessions) {
          if (Date.now() >= deadline)
            throw new Error("coordination_database_publication_timeout");
          const pointers = await api.pointers(dependency.identity, tx);
          if (
            pointers.length >= 64 ||
            canonicalPointerGeneration(dependency.identity, pointers) !==
              dependency.generation
          )
            return false;
        }
        if (Date.now() >= deadline)
          throw new Error("coordination_database_publication_timeout");
        const { rows } = await tx.query(
          `UPDATE sessions.coordination_report_jobs SET status='ready',result=$6::jsonb,publication_sequence=$5::bigint,dependency_sessions=$8::jsonb,lease_id=NULL,lease_expires_at=NULL,updated_at=now()
         WHERE org_id=$1 AND workspace_id=$2 AND id=$3::uuid AND lease_id=$4::uuid AND status='running' AND lease_expires_at>clock_timestamp() AND request=$7::jsonb RETURNING id`,
          [
            ...scope,
            claim.jobId,
            claim.leaseId,
            epochs[0].generation,
            serialized,
            JSON.stringify(claim.request),
            JSON.stringify(snapshot.sessions.map((s) => s.identity)),
          ],
        );
        if (rows.length === 1)
          await tx.query(
            `UPDATE sessions.coordination_report_epochs SET generation=$3::bigint WHERE org_id=$1 AND workspace_id=$2`,
            [...scope, epochs[0].generation],
          );
        return rows.length === 1;
      });
    },
    async fail(claim: ReportClaim, errorCode: string): Promise<void> {
      if (!/^[a-z][a-z0-9_]{0,63}$/.test(errorCode))
        throw new Error("coordination_database_error_invalid");
      await db.query(
        `UPDATE sessions.coordination_report_jobs SET status='failed',error_code=$5,lease_id=NULL,lease_expires_at=NULL,updated_at=now()
       WHERE org_id=$1 AND workspace_id=$2 AND id=$3::uuid AND lease_id=$4::uuid AND status='running'`,
        [...scope, claim.jobId, claim.leaseId, errorCode],
      );
    },
    async read(jobId: string): Promise<StoredReportJob | null> {
      const { rows } = await db.query<StoredReportJob>(
        `SELECT id,status,result,error_code FROM sessions.coordination_report_jobs WHERE org_id=$1 AND workspace_id=$2 AND id=$3::uuid`,
        [...scope, jobId],
      );
      return rows[0] ?? null;
    },
  };
  return api;
}
