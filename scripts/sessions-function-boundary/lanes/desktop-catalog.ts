/**
 * Lane: desktop catalog forget tracking and bounded reprojection (registry slot 20261105031000).
 * Every owned function is owner-only (no grants). The read RPC is replaced, not owned: its ACL
 * (EXECUTE to session_directory_search_worker) stays with 20261105022000, and the hand-written
 * rollback restores that file's body (src/hoopdb/__tests__/session-desktop-catalog-forget.test.ts
 * pins the restored text to it).
 */
import type { LaneManifest } from "../types.ts";

const MIGRATION = "supabase/migrations/20261105031000_desktop_catalog_forget_reproject.sql";

export const desktopCatalog: LaneManifest = {
  lane: "desktop-catalog",
  owner: "fix/desktop-catalog-forget-reproject-031000",
  migration: MIGRATION,
  prefixes: ["sessions.session_desktop_catalog_", "sessions.set_session_desktop_catalog_"],
  grantSources: [{ path: MIGRATION, roles: [] }],
  functions: {
    "sessions.set_session_desktop_catalog_forget_tracking(boolean)": { secdef: true, grants: [] },
    "sessions.session_desktop_catalog_mark_parents()": { secdef: true, grants: [] },
    "sessions.session_desktop_catalog_requeue_watchers()": { secdef: true, grants: [] },
    "sessions.session_desktop_catalog_projected()": { secdef: true, grants: [] },
    "sessions.session_desktop_catalog_reproject_page(text,text,text,text,integer)": { secdef: true, grants: [] },
  },
  replaces: {
    "sessions.session_directory_desktop_catalog(text,text,text)": {
      secdef: true,
      original: "supabase/migrations/20261105022000_session_desktop_catalog.sql",
    },
  },
};
