/**
 * Lane: operator per-user erase of buffered desktop uploads (slot 058010). Owner-only: no EXECUTE grant; the
 * one-shot SERVICE=user-erase job connects as the schema owner (docs/operator-user-erase.md). Also replaces
 * ingest_lock_fences (the RH041 ingest fence), the buffered GC claim and 041000's two source-control begins (the
 * erase gate); the rollback restores all four bodies.
 */
import type { LaneManifest } from "../types.ts";

const MIGRATION = "supabase/migrations/20261105058010_operator_user_erase.sql";

export const operatorUserErase: LaneManifest = {
  lane: "operator-user-erase",
  owner: "feat/operator-user-erase (slot 058010)",
  migration: MIGRATION,
  prefixes: ["sessions.operator_user_erase_"],
  grantSources: [],
  functions: {
    "sessions.operator_user_erase_count(text,jsonb)": { secdef: false, grants: [] },
    "sessions.operator_user_erase_render(text)": { secdef: false, grants: [] },
    "sessions.operator_user_erase_remaining(text,text,text,text[])": { secdef: false, grants: [] },
    "sessions.operator_user_erase_seat_ambiguity(text,text,text,text[])": { secdef: false, grants: [] },
    "sessions.operator_user_erase_purge(text,text,text,text[],boolean)": { secdef: false, grants: [] },
    "sessions.operator_user_erase_begin(text,text,text,text)": { secdef: false, grants: [] },
    "sessions.operator_user_erase_step(text,integer)": { secdef: false, grants: [] },
    "sessions.operator_user_erase_complete(text)": { secdef: false, grants: [] },
    "sessions.operator_user_erase_control_gate(text,text,text)": { secdef: false, grants: [] },
    "sessions.operator_user_erase_heads_guard()": { secdef: true, grants: [] },
  },
  // The ingest fence (RH041 while the user has an active erase), the GC claim filter and the control-begin gate; ACLs unchanged.
  replaces: {
    "sessions.ingest_lock_fences(text,text,text)": { secdef: false, original: "supabase/migrations/20261104030000_ingest_fence_ownership.sql" },
    "sessions.ingest_buffer_gc_claim(jsonb)": { secdef: true, original: "supabase/migrations/20261105050010_operator_session_erase_fence_check.sql" },
    "sessions.ingest_privacy_authority_begin(jsonb)": { secdef: true, original: "supabase/migrations/20261105041000_ingest_privacy_authority.sql" },
    "sessions.ingest_source_control_begin(jsonb)": { secdef: true, original: "supabase/migrations/20261105041000_ingest_privacy_authority.sql" },
  },
};
