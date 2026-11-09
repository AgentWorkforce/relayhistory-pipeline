/** Self-host input for real model briefs. Reuse the read service's current,
 * tenant/pointer/hash/generation-verified canonical object reader, not the
 * retention-filtered search index or the legacy raw transcript heuristic. */
import { PipelineError } from "../shared/vendor.ts";
import type { HoopDb } from "../shared/db.ts";
import type { SessionDirectoryKey } from "../hoopdb/session-directory-summary.ts";
import { poolTenantRunner } from "../session-read-api/liveness.ts";
import { serviceCanonicalDeps, type ServiceCanonicalDeps } from "../session-read-api/service-canonical.ts";
import { CanonicalReadError } from "../session-read-api/canonical.ts";
import { briefInputFromEvents, eventFromConvergenceRow, type BriefInput, type BriefPointer } from "./input.ts";
import { SESSION_SUMMARY_SOURCES } from "./engine/summary-contract.ts";
import type { CanonicalRows } from "../session-read-api/session-timing-source.ts";

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

/** Default off. Configuration is validated before the worker opens or claims
 * anything. Credentials remain in the existing Locker/HoopDb bindings. */
export function canonicalBriefInputFromEnv(env: NodeJS.ProcessEnv = process.env): CanonicalBriefInput | undefined {
  const flag = env.SESSION_BRIEF_CANONICAL_INPUT?.trim();
  if (flag === undefined || flag === "" || flag === "false") return;
  if (flag !== "true") throw new Error("SESSION_BRIEF_CANONICAL_INPUT must be true or false");
  if (env.SESSION_BRIEF_ENABLED !== "true" || !env.LOCKER_BUCKET?.trim() || env.PIPELINE_ORG_FROM_PREFIX !== "true")
    throw new Error("canonical brief input requires SESSION_BRIEF_ENABLED, LOCKER_BUCKET and org-prefix layout");
  const cache = new WeakMap<HoopDb, Promise<ServiceCanonicalDeps | undefined>>();
  return async (db, key, pointers) => {
    let canonical = cache.get(db);
    if (!canonical) { canonical = serviceCanonicalDeps(env, poolTenantRunner(db.pool as never)); cache.set(db, canonical); }
    const bound = await canonical;
    if (!bound) throw new PipelineError("canonical_input_unconfigured", "canonical input not configured", false);
    try {
      const snapshot = await bound.events.source({ orgId: key.orgId, workspaceId: key.workspaceId, source: key.harness, sessionId: key.sessionId }, AbortSignal.timeout(15000));
      return briefInputFromCanonical(key, pointers, snapshot);
    } catch (error) {
      if (error instanceof CanonicalReadError) {
        // Match the legacy input's quarantine outcome: never retain derived
        // text when the current authoritative manifest is explicitly unscrubbed.
        if (error.details.includes("manifest_unscrubbed")) return {
          kind: "quarantined", error: "session_quarantined",
          sourceRevisions: Object.fromEntries(pointers.map(p => [p.machineId, p.revision])),
        };
        throw new PipelineError("canonical_input_refused", "canonical input refused", error.status === 503);
      }
      throw error;
    }
  };
}
