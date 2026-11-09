/**
 * Lane: selfhost-output-facts (PR1 of the self-host digest/output plan; slot 045020).
 * supabase/migrations/20261105045020_selfhost_output_facts.sql owns nine functions:
 *   output_facts_writer_scope / output_facts_stored_version   helpers; not SECURITY DEFINER; owner-only
 *   output_facts_state / _reconcile / _record_state / _erase  SECURITY DEFINER; EXECUTE to search_consumer_runtime
 *   output_facts_events_for / _outputs_for / _coverage_for    SECURITY DEFINER; EXECUTE to bulls96_read_api
 */
import type { LaneManifest } from "../types.ts";

const MIGRATION = "supabase/migrations/20261105045020_selfhost_output_facts.sql";

export const selfhostOutputFacts: LaneManifest = {
  lane: "selfhost-output-facts",
  owner: "self-host digest/output PR1 (feat/selfhost-output-facts-store-045020)",
  migration: MIGRATION,
  prefixes: ["sessions.output_facts_"],
  grantSources: [{ path: MIGRATION, roles: ["search_consumer_runtime", "bulls96_read_api"] }],
  functions: {
    "sessions.output_facts_writer_scope(text,text,text,text[],text)": { secdef: false, grants: [] },
    "sessions.output_facts_stored_version(text,text,text,text)": { secdef: false, grants: [] },
    "sessions.output_facts_state(text,text,text,text[],text)": { secdef: true, grants: ["search_consumer_runtime"] },
    "sessions.output_facts_reconcile(text,text,text,text[],text,jsonb,jsonb,boolean,bigint)": {
      secdef: true,
      grants: ["search_consumer_runtime"],
    },
    "sessions.output_facts_record_state(text,text,text,text[],text,boolean,timestamp with time zone,timestamp with time zone,bigint,timestamp with time zone)": {
      secdef: true,
      grants: ["search_consumer_runtime"],
    },
    "sessions.output_facts_erase(text,text,text,text[])": { secdef: true, grants: ["search_consumer_runtime"] },
    "sessions.output_facts_events_for(text,timestamp with time zone,timestamp with time zone,text,integer)": {
      secdef: true,
      grants: ["bulls96_read_api"],
    },
    "sessions.output_facts_outputs_for(text,timestamp with time zone,timestamp with time zone,text,integer)": {
      secdef: true,
      grants: ["bulls96_read_api"],
    },
    "sessions.output_facts_coverage_for(text,timestamp with time zone,timestamp with time zone)": {
      secdef: true,
      grants: ["bulls96_read_api"],
    },
  },
  replaces: {},
};
