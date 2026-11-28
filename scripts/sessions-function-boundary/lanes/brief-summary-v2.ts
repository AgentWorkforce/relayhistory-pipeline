import type { LaneManifest } from "../types.ts";
/** T130 Reader provenance (provisional slot 062500): the v2 summary read, v1's authority plus the served revision. */
const migration = "supabase/migrations/20261105062500_session_brief_summary_v2_read.sql";
export const briefSummaryV2: LaneManifest = {
 lane: "brief-summary-v2", owner: "native brief summary read API, revision provenance (T130)", migration,
 prefixes: ["sessions.session_brief_summary_v2_for("],
 grantSources: [{path: migration, roles: ["bulls96_read_api"]}],
 functions: {"sessions.session_brief_summary_v2_for(text,text,text)": {secdef:true, grants:["bulls96_read_api"]}},
 replaces: {},
};
