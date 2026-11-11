/**
 * Lane: operator per-user erase of buffered desktop uploads (slot 058010). Owner-only: no EXECUTE grant; the
 * one-shot SERVICE=user-erase job connects as the schema owner (docs/operator-user-erase.md).
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
    "sessions.operator_user_erase_begin(text,text,text,text)": { secdef: false, grants: [] },
    "sessions.operator_user_erase_step(text,integer)": { secdef: false, grants: [] },
    "sessions.operator_user_erase_complete(text)": { secdef: false, grants: [] },
  },
  replaces: {},
};
