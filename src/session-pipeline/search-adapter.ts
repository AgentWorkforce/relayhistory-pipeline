/** Service composition for a registered portable index. Completeness/retention
 * state is a separate explicit binding: an index alone cannot declare a partial
 * canonical read complete. No storage, clients or schema are created here. */
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import { assertPosition, type SearchIndexAdapter, type SearchEventDocument, type SearchTenant, type SearchFingerprint } from "@rhc/search-index/contracts.ts";
import type { SessionScope } from "./pointer.ts";
import type { IndexRow } from "./vendor/search-index/rows.ts";

export interface SearchProjectionState {
  /** Inclusive coverage floor, also bound into the read API's history floor. */
  fromMs(at: Date): Promise<number>;
  /** Record only after projection and metadata writes finish; use the caller's
   * scope-locked transaction when the state is in the metadata database. */
  record(tx: SqlExecutor, scope: SessionScope, value: { revision: number; rows: number; complete: boolean; at: Date; canonicalSessionId: string }): Promise<void>;
  /** Must include unknown/missing projection state for live pointer scopes,
   * including a first failed pass that wrote no index rows. Tenant is verified.
   * Return cap+1 rows or overflow; never silently truncate a completeness check. */
  partials(tenant: SearchTenant, options: { sessionId?: string; limit: number }): Promise<{ sessions: readonly string[]; overflow: boolean }>;
}

export interface SearchServiceBinding {
  readonly index: SearchIndexAdapter;
  readonly state: SearchProjectionState;
}

export function assertSearchServiceBinding(binding: SearchServiceBinding, backend: string): void {
  if (!binding || binding.index?.backend !== backend || typeof binding.index.query !== "function" ||
      !binding.state || typeof binding.state.fromMs !== "function" || typeof binding.state.record !== "function" || typeof binding.state.partials !== "function")
    throw new Error("search service composition is incomplete");
  const capabilities = binding.index.capabilities();
  const requiredFilters = ["session", "source", "kind", "project", "since", "until"] as const;
  if (capabilities.backend !== backend || capabilities.readAfterWrite !== "immediate" || !capabilities.versionedUpserts || !capabilities.synchronousErasure || requiredFilters.some(filter => !capabilities.filters.includes(filter))) throw new Error("search service requires complete filters, visible versioned writes and synchronous erasure");
}

export function searchDocument(row: IndexRow): SearchEventDocument {
  const tsMs = Date.parse(row.ts.replace(" ", "T") + "Z");
  if (!Number.isSafeInteger(tsMs)) throw new Error("search projection timestamp invalid");
  return {
    orgId: row.org_id, workspaceId: row.workspace_id, tsMs, sessionId: row.session_id,
    source: row.source, machineId: row.machine_id, kind: row.kind, eventId: row.event_id,
    type: row.type, userId: row.user_id, actorName: row.actor_name, actorRole: row.actor_role,
    subagentId: row.subagent_id, projectId: row.project_id, taskTitle: row.task_title,
    taskRef: row.task_ref, taskRefId: row.task_ref_id, tags: row.tags, filesTouched: row.files_touched,
    toolName: row.tool_name, toolStatus: row.tool_status, deliveryRecordId: row.delivery_record_id,
    content: row.content, fingerprint: row.fingerprint, version: row.version,
  };
}

export interface SearchProjectionIo {
  fromMs(at: Date): Promise<number>;
  scan(scope: SessionScope, canonicalId: string, fromMs: number, limit: number): Promise<{ rows: readonly SearchFingerprint[]; truncated: boolean }>;
  upsert(rows: readonly IndexRow[], scope: SessionScope, canonicalId: string): Promise<number>;
  remove(scope: SessionScope, positions: readonly SearchFingerprint[]): Promise<number>;
  erase(scope: SessionScope, canonicalId: string): Promise<void>;
  record(scope: SessionScope, revision: number, rows: number, complete: boolean, at: Date, canonicalId: string): Promise<void>;
}

export function adapterProjectionIo(binding: SearchServiceBinding, tx: SqlExecutor): SearchProjectionIo {
  const scopeFor = (scope: SessionScope, canonicalId: string) => ({ orgId: scope.orgId, workspaceId: scope.workspaceId, source: scope.harness, sessionIds: [...new Set([scope.sessionId, canonicalId])] });
  return {
    async fromMs(at) {
      const value = await binding.state.fromMs(at);
      if (!Number.isSafeInteger(value) || value < 0) throw new Error("search coverage floor invalid");
      return value;
    },
    async scan(scope, canonicalId, fromMs, limit) {
      const result = await binding.index.scanFingerprints(scopeFor(scope, canonicalId), { fromMs, limit });
      if (!result || !Array.isArray(result.rows) || typeof result.truncated !== "boolean" || result.rows.length > limit) throw new Error("search scan result invalid");
      for (const row of result.rows) {
        assertPosition(row);
        if (row.workspaceId !== scope.workspaceId || row.source !== scope.harness || ![scope.sessionId, canonicalId].includes(row.sessionId) || row.tsMs < fromMs || typeof row.fingerprint !== "string") throw new Error("search scan scope invalid");
      }
      return result;
    },
    async upsert(rows, scope, canonicalId) {
      if (rows.some(row => row.org_id !== scope.orgId || row.workspace_id !== scope.workspaceId || row.source !== scope.harness || ![scope.sessionId, canonicalId].includes(row.session_id))) throw new Error("search projection scope invalid");
      const { written } = await binding.index.upsert(rows.map(searchDocument));
      if (!Number.isInteger(written) || written < 0 || written > rows.length) throw new Error("search write result invalid");
      return written;
    },
    async remove(scope, positions) {
      const { deleted } = await binding.index.deleteEvents({ orgId: scope.orgId, workspaceId: scope.workspaceId }, positions);
      if (!Number.isInteger(deleted) || deleted < 0 || deleted > positions.length) throw new Error("search delete result invalid");
      return deleted;
    },
    erase: (scope, canonicalId) => binding.index.eraseScope(scopeFor(scope, canonicalId)),
    record: (scope, revision, rows, complete, at, canonicalSessionId) => binding.state.record(tx, scope, { revision, rows, complete, at, canonicalSessionId }),
  };
}
