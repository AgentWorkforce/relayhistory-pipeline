/** Service composition for a registered portable index. Completeness/retention
 * state is a separate explicit binding: an index alone cannot declare a partial
 * canonical read complete. No storage, clients or schema are created here. */
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import { assertPosition, comparePositions, MAX_SCAN_PAGE, MAX_UPSERT_BATCH, type SearchIndexAdapter, type SearchEventDocument, type SearchTenant, type SearchFingerprint, type SearchPosition } from "@rhc/search-index/contracts.ts";
import type { IndexRow } from "./vendor/search-index/rows.ts";

/** Minimal public service scope. Importing pointer.ts here would pull its
 * Node-only database/bootstrap graph into the shared Cloudflare read API. */
export interface SearchProjectionScope {
  orgId: string;
  workspaceId: string;
  harness: string;
  sessionId: string;
}

export interface SearchProjectionState {
  /** Inclusive coverage floor, also bound into the read API's history floor. */
  fromMs(at: Date): Promise<number>;
  /** Record only after projection and metadata writes finish; use the caller's
   * scope-locked transaction when the state is in the metadata database. */
  record(tx: SqlExecutor, scope: SearchProjectionScope, value: { revision: number; rows: number; complete: boolean; at: Date; canonicalSessionId: string }): Promise<void>;
  /** Must include unknown/missing projection state for live pointer scopes,
   * including a first failed pass that wrote no index rows. Tenant is verified.
   * Return cap+1 rows or overflow; never silently truncate a completeness check. */
  partials(tenant: SearchTenant, options: { sessionId?: string; limit: number; after?: string }): Promise<{ sessions: readonly string[]; overflow: boolean }>;
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
  scan(scope: SearchProjectionScope, canonicalId: string, fromMs: number, limit: number): Promise<{ rows: readonly SearchFingerprint[]; truncated: boolean }>;
  upsert(rows: readonly IndexRow[], scope: SearchProjectionScope, canonicalId: string): Promise<number>;
  remove(scope: SearchProjectionScope, positions: readonly SearchFingerprint[]): Promise<number>;
  erase(scope: SearchProjectionScope, canonicalId: string): Promise<void>;
  record(scope: SearchProjectionScope, revision: number, rows: number, complete: boolean, at: Date, canonicalId: string): Promise<void>;
  /**
   * The scope's in-window index identities in keyset pages of `pageSize`, in scan order,
   * `between` awaited before each page after the first. Stops at `maxRows + 1` rows:
   * `truncated` then, and the caller must not delete. Present on the ClickHouse io, and on
   * the adapter io only when asked for (the staged reconcile, SEARCH_INDEX_STAGED_RECONCILE_ENABLED).
   */
  scanPages?(
    scope: SearchProjectionScope,
    canonicalId: string,
    fromMs: number,
    pageSize: number,
    maxRows: number,
    between: () => Promise<void>,
  ): Promise<{ rows: SearchFingerprint[]; truncated: boolean }>;
}

/**
 * One keyset page of a scope's in-window index identities (the staged reconcile's index scan,
 * one page per committed transaction): rows strictly after `after` in scan order, at most `limit`
 * (1..MAX_SCAN_PAGE), and the position to continue from (absent: the last page). Every row is
 * checked to lie in the scope and the window, and to advance.
 */
export async function adapterScanPage(
  binding: SearchServiceBinding,
  scope: SearchProjectionScope,
  canonicalId: string,
  fromMs: number,
  limit: number,
  after?: SearchPosition,
): Promise<{ rows: SearchFingerprint[]; next?: SearchPosition }> {
  const page = binding.index.scanFingerprintsPage?.bind(binding.index);
  if (!page) throw new Error("search paged scan unsupported by this index");
  if (!Number.isSafeInteger(limit) || limit < 1 || limit > MAX_SCAN_PAGE) throw new Error("search keyset scan bounds invalid");
  const result = await page(
    { orgId: scope.orgId, workspaceId: scope.workspaceId, source: scope.harness, sessionIds: [...new Set([scope.sessionId, canonicalId])] },
    { fromMs, limit, ...(after ? { after } : {}) },
  );
  if (!result || !Array.isArray(result.rows) || result.rows.length > limit) throw new Error("search scan result invalid");
  let previous = after;
  for (const row of result.rows) {
    assertPosition(row);
    if (row.workspaceId !== scope.workspaceId || row.source !== scope.harness || ![scope.sessionId, canonicalId].includes(row.sessionId) || row.tsMs < fromMs || typeof row.fingerprint !== "string")
      throw new Error("search scan scope invalid");
    if (previous && comparePositions(previous, row) >= 0) throw new Error("search scan order invalid");
    previous = row;
  }
  if (!result.next) return { rows: [...result.rows] };
  if (result.rows.length === 0) throw new Error("search scan result invalid");
  assertPosition(result.next);
  if (comparePositions(result.next, result.rows[result.rows.length - 1]!) !== 0) throw new Error("search scan result invalid");
  return { rows: [...result.rows], next: { ...result.next } };
}

/**
 * `pagedScan` (the staged reconcile only) adds `scanPages` over the adapter's
 * scanFingerprintsPage, and refuses an adapter without it. Absent: the io is exactly the
 * one without paging.
 */
export function adapterProjectionIo(binding: SearchServiceBinding, tx: SqlExecutor, options?: { pagedScan: true }): SearchProjectionIo {
  const scopeFor = (scope: SearchProjectionScope, canonicalId: string) => ({ orgId: scope.orgId, workspaceId: scope.workspaceId, source: scope.harness, sessionIds: [...new Set([scope.sessionId, canonicalId])] });
  const checkRow = (row: SearchFingerprint, scope: SearchProjectionScope, canonicalId: string, fromMs: number) => {
    assertPosition(row);
    if (row.workspaceId !== scope.workspaceId || row.source !== scope.harness || ![scope.sessionId, canonicalId].includes(row.sessionId) || row.tsMs < fromMs || typeof row.fingerprint !== "string") throw new Error("search scan scope invalid");
  };
  const paged: Pick<SearchProjectionIo, "scanPages"> = {};
  if (options?.pagedScan) {
    const page = binding.index.scanFingerprintsPage?.bind(binding.index);
    if (!page) throw new Error("search paged scan unsupported by this index");
    paged.scanPages = async (scope, canonicalId, fromMs, pageSize, maxRows, between) => {
      if (!Number.isSafeInteger(pageSize) || pageSize < 1 || pageSize > MAX_SCAN_PAGE || !Number.isSafeInteger(maxRows) || maxRows < 0)
        throw new Error("search keyset scan bounds invalid");
      const rows: SearchFingerprint[] = [];
      let after: SearchPosition | undefined;
      for (;;) {
        if (after) await between();
        const want = Math.min(pageSize, maxRows + 1 - rows.length);
        const result = await page(scopeFor(scope, canonicalId), { fromMs, limit: want, ...(after ? { after } : {}) });
        if (!result || !Array.isArray(result.rows) || result.rows.length > want) throw new Error("search scan result invalid");
        for (const row of result.rows) {
          checkRow(row, scope, canonicalId, fromMs);
          // Strictly after the previous row: the walk always advances and never repeats a row.
          const previous = rows[rows.length - 1] ?? after;
          if (previous && comparePositions(previous, row) >= 0) throw new Error("search scan order invalid");
          rows.push(row);
        }
        if (rows.length > maxRows) return { rows, truncated: true };
        if (!result.next) return { rows, truncated: false };
        if (result.rows.length === 0) throw new Error("search scan result invalid");
        after = { ...result.next };
        assertPosition(after);
        if (comparePositions(after, rows[rows.length - 1]!) !== 0) throw new Error("search scan result invalid");
      }
    };
  }
  return {
    ...paged,
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
      let deleted = 0;
      // The public Postgres adapter bounds exact-position deletion like upsert.
      // Calls may commit separately; propagate failures so only a complete pass
      // records completeness, and a retry rescans the remaining positions.
      for (let i = 0; i < positions.length; i += MAX_UPSERT_BATCH) {
        const batch = positions.slice(i, i + MAX_UPSERT_BATCH);
        const result = await binding.index.deleteEvents({ orgId: scope.orgId, workspaceId: scope.workspaceId }, batch);
        if (!result || !Number.isSafeInteger(result.deleted) || result.deleted < 0 || result.deleted > batch.length) throw new Error("search delete result invalid");
        deleted += result.deleted;
      }
      if (!Number.isSafeInteger(deleted) || deleted > positions.length) throw new Error("search delete result invalid");
      return deleted;
    },
    erase: (scope, canonicalId) => binding.index.eraseScope(scopeFor(scope, canonicalId)),
    record: (scope, revision, rows, complete, at, canonicalSessionId) => binding.state.record(tx, scope, { revision, rows, complete, at, canonicalSessionId }),
  };
}
