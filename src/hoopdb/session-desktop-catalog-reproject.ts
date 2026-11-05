/**
 * The bounded, cursor-resumable repair path for desktop catalog parents that migration
 * 20261105031000 marked pending (a child they named was forgotten, or a forgotten child came
 * back). Owner-only: `sessions.session_desktop_catalog_reproject_page` has no grants, so this
 * runs on the schema owner's connection during activation/repair, never from the pipeline.
 *
 * Each page is one read-only statement that takes no row lock. `reproject` is called for each
 * parent with NO transaction or lock held by this driver, so it may read canonical sources over
 * the network; it must project the parent through `writeDirectoryCatalog` (desktop writer) with
 * the returned epoch, whose completion trigger settles the queue row. A reprojector that
 * throws, or returns `{ written: false }` (for example a stale epoch: the session's events
 * moved on since the page was read), counts as failed. A parent whose reprojection fails
 * stays pending (its workspace keeps refusing) and is retried by the next
 * run from a NULL cursor; the cursor still advances past it so one bad parent cannot stall
 * the rest.
 */
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import type { SessionDirectoryKey } from "./session-directory-summary.ts";

export type DesktopCatalogReprojectCursor = SessionDirectoryKey;
export type PendingDesktopCatalogParent = SessionDirectoryKey & {
  /** event_projection_epoch when the page was read; pass it as the writer's expectedEpoch. */
  epoch: string;
};
export type DesktopCatalogReprojectResult = {
  pages: number;
  processed: number;
  failed: number;
  /** Resume from here; null once the queue was read to its end. */
  cursor: DesktopCatalogReprojectCursor | null;
  done: boolean;
};

export const MAX_DESKTOP_CATALOG_REPROJECT_PAGE = 500;

export async function reprojectPendingDesktopCatalogParents(
  db: SqlExecutor,
  reproject: (parent: PendingDesktopCatalogParent) => Promise<{ written: boolean } | void>,
  options: {
    cursor?: DesktopCatalogReprojectCursor | null;
    pageSize?: number;
    maxPages?: number;
    onError?: (parent: PendingDesktopCatalogParent, error: unknown) => void;
  } = {},
): Promise<DesktopCatalogReprojectResult> {
  const pageSize = options.pageSize ?? 100;
  const maxPages = options.maxPages ?? 10;
  if (!Number.isInteger(pageSize) || pageSize < 1 || pageSize > MAX_DESKTOP_CATALOG_REPROJECT_PAGE)
    throw new Error("desktop_catalog_reproject_page_size_invalid");
  if (!Number.isInteger(maxPages) || maxPages < 1)
    throw new Error("desktop_catalog_reproject_max_pages_invalid");
  let cursor = options.cursor ?? null;
  let pages = 0;
  let processed = 0;
  let failed = 0;
  while (pages < maxPages) {
    const { rows } = await db.query<{
      org_id: string;
      workspace_id: string;
      harness: string;
      session_id: string;
      event_projection_epoch: string;
    }>(
      "SELECT org_id,workspace_id,harness,session_id,event_projection_epoch FROM sessions.session_desktop_catalog_reproject_page($1,$2,$3,$4,$5)",
      [cursor?.orgId ?? null, cursor?.workspaceId ?? null, cursor?.harness ?? null, cursor?.sessionId ?? null, pageSize],
    );
    pages++;
    for (const row of rows) {
      const parent: PendingDesktopCatalogParent = {
        orgId: row.org_id,
        workspaceId: row.workspace_id,
        harness: row.harness,
        sessionId: row.session_id,
        epoch: row.event_projection_epoch,
      };
      try {
        const result = await reproject(parent);
        if (result && result.written === false) {
          failed++;
          options.onError?.(parent, new Error("desktop_catalog_reproject_not_written"));
        } else processed++;
      } catch (error) {
        failed++;
        options.onError?.(parent, error);
      }
      cursor = { orgId: parent.orgId, workspaceId: parent.workspaceId, harness: parent.harness, sessionId: parent.sessionId };
    }
    if (rows.length < pageSize) return { pages, processed, failed, cursor: null, done: true };
  }
  return { pages, processed, failed, cursor, done: false };
}
