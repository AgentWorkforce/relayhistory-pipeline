/**
 * The regeneration audit writer: one row per enqueue in sessions.summary_regeneration_audit (append-only,
 * no summary text, no prompt) and one structured log line with the session id hashed. Shared by the
 * Bob Costas sweep, the brief sweep and (push B) the operator requeue route.
 *
 * The log line is always emitted when a regeneration is queued; SUMMARY_REGEN_AUDIT_ENABLED gates only
 * the database write, so the code can land before the table exists.
 */
import { createHash } from "node:crypto";
import { log } from "../shared/log.ts";
import type { Queryable } from "./detect.ts";
import type { RegenReason } from "./settings.ts";

export type RegenSystem = "bob_costas" | "brief";

export interface RegenAuditEntry {
  orgId: string;
  workspaceId: string;
  harness: string;
  sessionId: string;
  system: RegenSystem;
  kind: "short" | "long" | "brief";
  reason: RegenReason;
  priorStatus: string | null;
  priorFailureCode: string | null;
  priorPipelineVersion: string | null;
  /** Operator requeue only (push B): request id and the sha256 of its scope. */
  requestId?: string | null;
  scopeHash?: string | null;
  requestedBy?: string | null;
}

/** sha256 over the session's identity; what logs carry instead of the session id. */
export function sessionHash(e: Pick<RegenAuditEntry, "orgId" | "workspaceId" | "harness" | "sessionId">): string {
  return createHash("sha256").update(JSON.stringify([e.orgId, e.workspaceId, e.harness, e.sessionId])).digest("hex");
}

/** Codes only: a prior status or failure code outside this shape is not logged or stored. */
const CODE = /^[a-z][a-z0-9_]{0,63}$/;
const STATUS = /^[a-z][a-z0-9_]{0,31}$/;
const code = (v: string | null | undefined) => (v && CODE.test(v) ? v : null);
const status = (v: string | null | undefined) => (v && STATUS.test(v) ? v : null);
const version = (v: string | null | undefined) => (v && v.length <= 128 ? v : null);

export function logRegenQueued(e: RegenAuditEntry, stage: string): void {
  log.info("summary regeneration queued", {
    stage,
    system: e.system,
    kind: e.kind,
    reason: e.reason,
    org_id: e.orgId,
    workspace_id: e.workspaceId,
    harness: e.harness,
    session_hash: sessionHash(e),
    ...(e.requestId ? { request_id: e.requestId } : {}),
    ...(status(e.priorStatus) ? { prior_status: status(e.priorStatus)! } : {}),
    ...(code(e.priorFailureCode) ? { prior_failure_code: code(e.priorFailureCode)! } : {}),
  });
}

/** Rows go through the SECURITY DEFINER append function (062040); the runtime has no table write grant. */
export const AUDIT_APPEND_SQL = "SELECT sessions.summary_regeneration_audit_append_v1($1::jsonb) AS n";

/** One batch per sweep (the SQL function caps a call at 1000 rows). Returns the number of rows written. */
export async function writeRegenAudit(db: Queryable, entries: readonly RegenAuditEntry[]): Promise<number> {
  if (!entries.length) return 0;
  const rows = entries.map((e) => ({
    org_id: e.orgId,
    workspace_id: e.workspaceId,
    harness: e.harness,
    session_id: e.sessionId,
    system: e.system,
    kind: e.kind,
    reason: e.reason,
    prior_status: status(e.priorStatus),
    prior_failure_code: code(e.priorFailureCode),
    prior_pipeline_version: version(e.priorPipelineVersion),
    request_id: e.requestId ?? null,
    scope_hash: e.scopeHash ?? null,
    requested_by: e.requestedBy ?? null,
  }));
  const r = await db.query<{ n: string | number }>(AUDIT_APPEND_SQL, [JSON.stringify(rows)]);
  return Number(r.rows[0]?.n ?? 0);
}

/** Retention purge (bounded per call by the SQL function). Returns rows deleted. */
export async function purgeRegenAudit(db: Queryable, retentionDays: number): Promise<number> {
  const r = await db.query<{ n: string | number }>("SELECT sessions.summary_regeneration_audit_purge_v1($1::int) AS n", [retentionDays]);
  return Number(r.rows[0]?.n ?? 0);
}
