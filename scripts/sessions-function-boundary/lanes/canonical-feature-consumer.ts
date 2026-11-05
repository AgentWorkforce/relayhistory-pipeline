/**
 * Lane: canonical-feature-consumer (610 consumer ACKs).
 * EMPTY SLOT, filled by migration-completeness when #233 restacks onto this generator: set `migration`
 * to supabase/migrations/20261105030000_canonical_feature_journal_ack_finish.sql, list every function it
 * defines with its SECURITY DEFINER flag, and declare its grant source with the producer role ONLY
 * (`roles: ["bulls96_canonical_feature_producer"]`, per supporter-cloud's signed-off 687e076).
 * The generator then refuses any other grantee in that file. Its prefixes must not overlap
 * ingest-history's (sessions.ingest_history_ / session_history_ / session_directory_history_).
 */
import type { LaneManifest } from "../types.ts";

export const canonicalFeatureConsumer: LaneManifest = {
  lane: "canonical-feature-consumer",
  owner: "#233 (migration-completeness)",
  migration: null,
  prefixes: [],
  grantSources: [],
  functions: {},
  replaces: {},
};
