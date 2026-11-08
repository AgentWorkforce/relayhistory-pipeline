/**
 * Lane: buffered-search-feed (registry slot 043870).
 * supabase/migrations/20261105043870_buffered_search_feed_cursor.sql owns three functions:
 *   buffered_search_feed_page      SECURITY DEFINER; keyset page of active buffered stages after a cursor
 *   buffered_search_feed_stage     SECURITY DEFINER; one stage's indexability + GC-authority retention
 *   buffered_search_feed_advance   SECURITY DEFINER; monotonic cursor advance
 * EXECUTE to service_role only (the session pipeline's feeder, INGEST_BUFFERED_SEARCH_FEED, default off).
 * The prefix is "sessions.buffered_search_feed_", so it claims nothing else under sessions.buffered_*
 * (buffered_search_gate_for is lane buffered-search-gate).
 */
import type { LaneManifest } from "../types.ts";

const MIGRATION = "supabase/migrations/20261105043870_buffered_search_feed_cursor.sql";

export const bufferedSearchFeed: LaneManifest = {
  lane: "buffered-search-feed",
  owner: "PR #339 (feat/buffered-search-feeder)",
  migration: MIGRATION,
  prefixes: ["sessions.buffered_search_feed_"],
  grantSources: [{ path: MIGRATION, roles: ["service_role"] }],
  functions: {
    "sessions.buffered_search_feed_advance(jsonb)": { secdef: true, grants: ["service_role"] },
    "sessions.buffered_search_feed_page(jsonb)": { secdef: true, grants: ["service_role"] },
    "sessions.buffered_search_feed_stage(jsonb)": { secdef: true, grants: ["service_role"] },
  },
  replaces: {},
};
