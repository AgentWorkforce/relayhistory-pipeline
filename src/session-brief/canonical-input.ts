/** Self-host input for real model briefs. Reuse the read service's current,
 * tenant/pointer/hash/generation-verified canonical object reader, not the
 * retention-filtered search index or the legacy raw transcript heuristic. */
import { PipelineError } from "../shared/vendor.ts";
import type { HoopDb } from "../shared/db.ts";
import type { SessionDirectoryKey } from "../hoopdb/session-directory-summary.ts";
import { poolTenantRunner } from "../session-read-api/liveness.ts";
import { serviceCanonicalDeps, type ServiceCanonicalDeps } from "../session-read-api/service-canonical.ts";
import { CanonicalReadError, canonicalReasonCodeFields } from "../session-read-api/canonical.ts";
import { log } from "../shared/log.ts";
import { briefInputFromEvents, eventFromConvergenceRow, type BriefInput, type BriefPointer } from "./input.ts";
import { SESSION_SUMMARY_SOURCES } from "./engine/summary-contract.ts";
import { briefWorkspaceIds } from "./config.ts";
import type { TenantRunner } from "../session-read-api/liveness.ts";
import type { CanonicalRows } from "../session-read-api/session-timing-source.ts";

/**
 * A CanonicalReadError from the canonical object reader: quarantine an explicitly
 * unscrubbed manifest, otherwise `canonical_input_refused` (retryable on 503). The
 * PipelineError carries only its code, so the canonical reason and its projection
 * reason codes (codes only, never ids or values) are logged for the operator.
 */
export function canonicalInputRefusal(error: CanonicalReadError, pointers: readonly BriefPointer[]): BriefInput {
  // Match the legacy input's quarantine outcome: never retain derived
  // text when the current authoritative manifest is explicitly unscrubbed.
  if (error.details.includes("manifest_unscrubbed")) return {
    kind: "quarantined", error: "session_quarantined",
    sourceRevisions: Object.fromEntries(pointers.map(p => [p.machineId, p.revision])),
  };
  const retryable = error.status === 503;
  log.warn("canonical brief input refused", {
    stage: "session-brief", error_code: "canonical_input_refused", retryable,
    reason: error.reason, ...canonicalReasonCodeFields(error.details),
  });
  throw new PipelineError("canonical_input_refused", "canonical input refused", retryable);
}

export type CanonicalBriefInput = (db: HoopDb, key: SessionDirectoryKey, pointers: readonly BriefPointer[]) => Promise<BriefInput>;

export function briefInputFromCanonical(
  key: SessionDirectoryKey, pointers: readonly BriefPointer[],
  snapshot: CanonicalRows & { exists: boolean; generation: string },
): BriefInput {
  const sourceRevisions = Object.fromEntries(pointers.map(p => [p.machineId, p.revision]));
  if (!SESSION_SUMMARY_SOURCES.includes(key.harness)) return { kind: "ineligible", error: "session_source_unsupported", sourceRevisions };
  if (!snapshot.exists || !/^[a-f0-9]{64}$/.test(snapshot.generation)) throw new PipelineError("canonical_source_changed", "canonical input is unavailable", true);
  const events = snapshot.rows.filter(r => r.table === "convergence_events").flatMap(({ row }) => {
    if (row.session_id !== key.sessionId || row.source !== key.harness ||
        (row.org_id !== undefined && row.org_id !== key.orgId) ||
        (row.workspace_id !== undefined && row.workspace_id !== key.workspaceId))
      throw new PipelineError("canonical_input_scope", "canonical input scope refused", false);
    const event = eventFromConvergenceRow(row, key.harness, key.sessionId);
    return event ? [event] : [];
  });
  return briefInputFromEvents(key, sourceRevisions, events);
}


/** One physical source only. Never summarize a canonical union while publishing
 * freshness for only one of its physical aliases. */
export async function canonicalBriefScope(run: TenantRunner, key: SessionDirectoryKey): Promise<SessionDirectoryKey> {
  return run(key.orgId, key.workspaceId, async tx => {
    const mapped = await tx.query<{ id: string }>(`SELECT DISTINCT coalesce(canonical_session_id,session_id) AS id
      FROM sessions.session_index WHERE org_id=$1 AND workspace_id=$2 AND harness=$3 AND session_id=$4
        AND deleted_at IS NULL AND manifest_key IS NOT NULL LIMIT 2`, [key.orgId,key.workspaceId,key.harness,key.sessionId]);
    if (mapped.rows.length !== 1) throw new PipelineError("canonical_input_scope_changed", "canonical source unavailable", true);
    const logical = mapped.rows[0]!.id;
    const physical = await tx.query<{ id: string }>(`SELECT DISTINCT session_id AS id FROM sessions.session_index
      WHERE org_id=$1 AND workspace_id=$2 AND harness=$3 AND coalesce(canonical_session_id,session_id)=$4
        AND deleted_at IS NULL AND manifest_key IS NOT NULL LIMIT 2`, [key.orgId,key.workspaceId,key.harness,logical]);
    if (physical.rows.length !== 1 || physical.rows[0]!.id !== key.sessionId)
      throw new PipelineError("canonical_input_multiple_sources", "multiple physical sources are not qualified", false);
    return { ...key, sessionId: logical };
  });
}

/** Default off. Configuration is validated before the worker opens or claims
 * anything. Credentials remain in the existing Locker/HoopDb bindings. */
export function canonicalBriefInputFromEnv(env: NodeJS.ProcessEnv = process.env): CanonicalBriefInput | undefined {
  const flag = env.SESSION_BRIEF_CANONICAL_INPUT?.trim();
  if (flag === undefined || flag === "" || flag === "false") return;
  if (flag !== "true") throw new Error("SESSION_BRIEF_CANONICAL_INPUT must be true or false");
  if (env.SESSION_BRIEF_ENABLED !== "true" || !env.LOCKER_BUCKET?.trim() || env.PIPELINE_ORG_FROM_PREFIX !== "true")
    throw new Error("canonical brief input requires SESSION_BRIEF_ENABLED, LOCKER_BUCKET and org-prefix layout");
  const workspaces = briefWorkspaceIds(env.SESSION_BRIEF_WORKSPACE_IDS);
  if (!workspaces?.length) throw new Error("canonical brief input requires SESSION_BRIEF_WORKSPACE_IDS");
  const cache = new WeakMap<HoopDb, Promise<ServiceCanonicalDeps | undefined>>();
  return async (db, key, pointers) => {
    if (!workspaces.includes(key.workspaceId)) throw new PipelineError("canonical_input_workspace", "workspace is not selected", false);
    const run = poolTenantRunner(db.pool as never);
    const logicalKey = await canonicalBriefScope(run, key);
    let canonical = cache.get(db);
    if (!canonical) { canonical = serviceCanonicalDeps(env, poolTenantRunner(db.pool as never)); cache.set(db, canonical); }
    const bound = await canonical;
    if (!bound) throw new PipelineError("canonical_input_unconfigured", "canonical input not configured", false);
    try {
      const snapshot = await bound.events.source({ orgId: key.orgId, workspaceId: key.workspaceId, source: key.harness, sessionId: logicalKey.sessionId }, AbortSignal.timeout(15000));
      const after = await canonicalBriefScope(run, key);
      if (after.sessionId !== logicalKey.sessionId) throw new PipelineError("canonical_input_scope_changed", "canonical source changed", true);
      return { ...briefInputFromCanonical(logicalKey, pointers, snapshot), canonicalSessionId: logicalKey.sessionId };
    } catch (error) {
      if (error instanceof CanonicalReadError) return canonicalInputRefusal(error, pointers);
      throw error;
    }
  };
}
