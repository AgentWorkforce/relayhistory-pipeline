/** Lane: owner-only retirement of terminal ingest_claim_attempts.records (slot 038000, assigned by layered-sessions-pipeline). */
import type { LaneManifest } from "../types.ts";

const MIGRATION = "supabase/migrations/20261105038000_claim_attempt_records_retirement.sql";

export const claimRecordsRetirement: LaneManifest = {
  lane: "claim-records-retirement",
  owner: "claude/claim-attempts-shrink-20261105 (draft)",
  migration: MIGRATION,
  prefixes: ["sessions.ingest_claim_attempt_records_", "sessions.ingest_stage_intent_retired_attempt_guard("],
  grantSources: [],
  functions: {
    "sessions.ingest_claim_attempt_records_guard()": { secdef: false, grants: [] },
    "sessions.ingest_stage_intent_retired_attempt_guard()": { secdef: false, grants: [] },
    "sessions.ingest_claim_attempt_records_key(text,text,text,timestamp with time zone,uuid,text)": { secdef: false, grants: [] },
    "sessions.ingest_claim_attempt_records_terminal(uuid)": { secdef: false, grants: [] },
    "sessions.ingest_claim_attempt_records_candidates(jsonb)": { secdef: false, grants: [] },
    "sessions.ingest_claim_attempt_records_retire(jsonb)": { secdef: false, grants: [] },
  },
  replaces: {},
};
