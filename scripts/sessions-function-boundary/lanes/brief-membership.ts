import type { LaneManifest } from "../types.ts";
/** T130 consumed-record membership (slot 062510): the read-role membership answer and the owner-only retirement trigger. */
const migration = "supabase/migrations/20261105062510_session_brief_membership.sql";
export const briefMembership: LaneManifest = {
 lane: "brief-membership", owner: "native brief consumed-record membership (T130)", migration,
 prefixes: ["sessions.session_brief_membership_for(", "sessions.session_brief_evidence_retire("],
 grantSources: [{path: migration, roles: ["bulls96_read_api"]}],
 functions: {"sessions.session_brief_membership_for(text,text,text,text,text)": {secdef:true, grants:["bulls96_read_api"]},
  "sessions.session_brief_evidence_retire()": {secdef:false, grants:[]}},
 replaces: {},
};
