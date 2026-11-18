/**
 * Lane: relayfacts (slot 062080).
 * Four SECURITY DEFINER entry points over sessions.relayfacts_v1 and sessions.relayfacts_evidence_v1
 * (docs/relayfacts.md): insert and supersede for the worker (service_role); topics and list for
 * bulls96_read_api, pinned to its request scope. No runtime role has a table privilege.
 */
import type { LaneManifest } from "../types.ts";

const MIGRATION = "supabase/migrations/20261105062080_memory_documents.sql";

export const relayfacts: LaneManifest = {
  lane: "relayfacts",
  owner: "relayfacts decision records and readers (slot 062080)",
  migration: MIGRATION,
  // One exact prefix per function: a bare "sessions.relayfacts_" would overlap the erase lane's
  // "sessions.relayfacts_evidence_" (062090), and the generator refuses overlapping prefixes.
  prefixes: ["sessions.relayfacts_insert_", "sessions.relayfacts_supersede_", "sessions.relayfacts_topics_", "sessions.relayfacts_list_"],
  grantSources: [{ path: MIGRATION, roles: ["service_role", "bulls96_read_api"] }],
  functions: {
    "sessions.relayfacts_insert_v1(text,text,jsonb)": { secdef: true, grants: ["service_role"] },
    "sessions.relayfacts_supersede_v1(text,text,uuid,uuid)": { secdef: true, grants: ["service_role"] },
    "sessions.relayfacts_topics_v1(text,text)": { secdef: true, grants: ["bulls96_read_api"] },
    "sessions.relayfacts_list_v1(text,text,text,timestamp with time zone,uuid,integer)": { secdef: true, grants: ["bulls96_read_api"] },
  },
  replaces: {},
};

const ERASE_MIGRATION = "supabase/migrations/20261105062090_relayfacts_erase.sql";

/**
 * Lane: relayfacts-erase (slot 062090).
 * The operator per-session erase of decision evidence, run by the self-host ingest login
 * (bulls96_session_ingest) inside its erase lease, and the trigger that refuses evidence for a fenced session.
 */
export const relayfactsErase: LaneManifest = {
  lane: "relayfacts-erase",
  owner: "relayfacts operator erase of decision evidence (slot 062090)",
  migration: ERASE_MIGRATION,
  prefixes: ["sessions.relayfacts_evidence_"],
  grantSources: [{ path: ERASE_MIGRATION, roles: ["bulls96_session_ingest"] }],
  functions: {
    "sessions.relayfacts_evidence_erase_session_v1(text,text,text,text,uuid)": { secdef: true, grants: ["bulls96_session_ingest"] },
    "sessions.relayfacts_evidence_fence_guard_v1()": { secdef: true, grants: [] },
  },
  replaces: {},
};

const USER_ERASE_MIGRATION = "supabase/migrations/20261105062100_relayfacts_user_erase.sql";

/**
 * Lane: relayfacts-user-erase (slot 062100).
 * The hosted operator per-user erase of decision evidence, run by the schema-owner login of the per-user erase
 * job before operator_user_erase_begin (owner-only: no EXECUTE grant), the statement triggers that serialize
 * relayfacts writes against it, and the trigger that refuses evidence for a user-erased session. Its own prefix:
 * "sessions.relayfacts_evidence_" belongs to the relayfacts-erase lane and the generator refuses overlaps.
 */
export const relayfactsUserErase: LaneManifest = {
  lane: "relayfacts-user-erase",
  owner: "relayfacts operator per-user erase of decision evidence (slot 062100)",
  migration: USER_ERASE_MIGRATION,
  prefixes: ["sessions.relayfacts_user_erase_"],
  grantSources: [],
  functions: {
    "sessions.relayfacts_user_erase_v1(text,text,text,text)": { secdef: false, grants: [] },
    "sessions.relayfacts_user_erase_lock_v1()": { secdef: true, grants: [] },
    "sessions.relayfacts_user_erase_guard_v1()": { secdef: true, grants: [] },
  },
  replaces: {},
};

const CLAIMS_MIGRATION = "supabase/migrations/20261105062150_relayfacts_claims.sql";

/**
 * Lane: relayfacts-claims (slot 062150, Session Intelligence M1-1).
 * Intent and constraint claims in two new tables (relayfacts_claims_v1, relayfacts_claim_evidence_v1): insert,
 * supersede and the claims reader (claims plus the 062080 decisions) for the worker and the advisor (service_role),
 * and the owner-pinned fence guard trigger function. REPLACES the 062090 per-session erase (ACL: bulls96_session_ingest)
 * and the 062100 per-user erase (owner only) so both reach the new tables; their ACLs are kept.
 */
export const relayfactsClaims: LaneManifest = {
  lane: "relayfacts-claims",
  owner: "relayfacts claims: intents and constraints with scope and observed_at (slot 062150)",
  migration: CLAIMS_MIGRATION,
  prefixes: ["sessions.relayfacts_claims_"],
  grantSources: [{ path: CLAIMS_MIGRATION, roles: ["service_role"] }],
  functions: {
    "sessions.relayfacts_claims_insert_v1(text,text,jsonb)": { secdef: true, grants: ["service_role"] },
    "sessions.relayfacts_claims_supersede_v1(text,text,uuid,uuid)": { secdef: true, grants: ["service_role"] },
    "sessions.relayfacts_claims_v1(text,text,text,text,timestamp with time zone,integer)": { secdef: true, grants: ["service_role"] },
    "sessions.relayfacts_claims_fence_guard_v1()": { secdef: true, grants: [] },
  },
  replaces: {
    "sessions.relayfacts_evidence_erase_session_v1(text,text,text,text,uuid)": { secdef: true, original: ERASE_MIGRATION },
    "sessions.relayfacts_user_erase_v1(text,text,text,text)": { secdef: false, original: USER_ERASE_MIGRATION },
  },
};
