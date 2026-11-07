/**
 * Lane: digest-narratives (lane F4, the daily digest narrative cache off Neon; slot 045000).
 * supabase/migrations/20261105045000_digest_narrative_cache.sql owns four functions:
 *   digest_narrative_scope_workspace   scope-key parser; not SECURITY DEFINER; owner-only EXECUTE
 *   digest_narrative_read              SECURITY DEFINER; EXECUTE to bulls96_read_api
 *   digest_narrative_upsert            SECURITY DEFINER; EXECUTE to bulls96_digest_narrative
 *   purge_daily_digest_narratives      not SECURITY DEFINER; owner-only EXECUTE (upload erasure)
 */
import type { LaneManifest } from "../types.ts";

const MIGRATION = "supabase/migrations/20261105045000_digest_narrative_cache.sql";

export const digestNarratives: LaneManifest = {
  lane: "digest-narratives",
  owner: "lane F4 (feat/f4-digest-20261106)",
  migration: MIGRATION,
  prefixes: [
    "sessions.digest_narrative_scope_workspace(",
    "sessions.digest_narrative_read(",
    "sessions.digest_narrative_upsert(",
    "sessions.purge_daily_digest_narratives(",
  ],
  grantSources: [{ path: MIGRATION, roles: ["bulls96_read_api", "bulls96_digest_narrative"] }],
  functions: {
    "sessions.digest_narrative_scope_workspace(text)": { secdef: false, grants: [] },
    "sessions.digest_narrative_read(text,date,text,text)": { secdef: true, grants: ["bulls96_read_api"] },
    "sessions.digest_narrative_upsert(text,text,date,text,text,text,text,text,timestamp with time zone)": {
      secdef: true,
      grants: ["bulls96_digest_narrative"],
    },
    "sessions.purge_daily_digest_narratives(text,text)": { secdef: false, grants: [] },
  },
  replaces: {},
};
