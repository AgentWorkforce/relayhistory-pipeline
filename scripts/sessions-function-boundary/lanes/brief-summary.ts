import type { LaneManifest } from "../types.ts";
const migration = "supabase/migrations/20261105044040_session_brief_summary_read.sql";
export const briefSummary: LaneManifest = {
 lane: "brief-summary", owner: "native brief summary read API", migration,
 prefixes: ["sessions.session_brief_summary_for("],
 grantSources: [{path: migration, roles: ["bulls96_read_api"]}],
 functions: {"sessions.session_brief_summary_for(text,text,text)": {secdef:true, grants:["bulls96_read_api"]}},
 replaces: {},
};
