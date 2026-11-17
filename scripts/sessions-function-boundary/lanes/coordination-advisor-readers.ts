/**
 * Lane: coordination-advisor-readers (slot 062070).
 * The coordination advisor worker's own SECURITY DEFINER readers (docs/coordination-live-advisories.md):
 * tenant-explicit mirrors of the 047000/045040 overlaps facts readers for the postgres facts backend,
 * and coordination_advisories_recent_v1 for the durable cooldown and cap. EXECUTE for service_role only.
 */
import type { LaneManifest } from "../types.ts";

const MIGRATION = "supabase/migrations/20261105062070_coordination_advisor_readers.sql";
const FACTS = "(text,text,timestamp with time zone,timestamp with time zone,text,integer)";

export const coordinationAdvisorReaders: LaneManifest = {
  lane: "coordination-advisor-readers",
  owner: "coordination advisor worker readers (slot 062070)",
  migration: MIGRATION,
  prefixes: ["sessions.coordination_advisor_", "sessions.coordination_advisories_recent_v1("],
  grantSources: [{ path: MIGRATION, roles: ["service_role"] }],
  functions: {
    [`sessions.coordination_advisor_write_facts_v1${FACTS}`]: { secdef: true, grants: ["service_role"] },
    [`sessions.coordination_advisor_output_facts_v1${FACTS}`]: { secdef: true, grants: ["service_role"] },
    [`sessions.coordination_advisor_usage_facts_v1${FACTS}`]: { secdef: true, grants: ["service_role"] },
    "sessions.coordination_advisories_recent_v1(text,text,timestamp with time zone)": { secdef: true, grants: ["service_role"] },
  },
  replaces: {},
};
