/** Lane (slot 041000): C3 Supabase-first privacy authority. New owner/ingest functions + the legacy mirror begin/complete and the 040000 lazy-current checks replaced. */
import { createHash } from "node:crypto";
import type { LaneContext, LaneManifest } from "../types.ts";

const MIGRATION = "supabase/migrations/20261105041000_ingest_privacy_authority.sql";
const ORIGINAL = "supabase/migrations/20261105010000_frozen_source_custody.sql";
/** 041000 replaces the 040000 lazy-current bodies (exact source-table list); its rollback restores them exactly. */
const LAZY_ORIGINAL = "supabase/migrations/20261105040000_ingest_lazy_source_tables.sql";
const ROLLBACK = "supabase/rollbacks/20261105041000_ingest_privacy_authority.down.sql";
const NAME = "upstream:20261105041000_ingest_privacy_authority.sql";
const INGEST = "bulls96_session_ingest";
const ERASER = "bulls96_privacy_eraser";

/** The exact 010000 legacy mirror begin/complete, as CREATE OR REPLACE (owner/ACL stay unchanged). */
export function originalMirrorBodies(source: string): string[] {
  return [
    /CREATE FUNCTION sessions\.ingest_source_control_begin\(p jsonb\)[\s\S]*?\$control_begin\$;/,
    /CREATE FUNCTION sessions\.ingest_source_control_complete\(p jsonb\)[\s\S]*?\$control_complete\$;/,
  ].map((re) => {
    const m = source.match(re);
    if (!m) throw Error("privacy authority rollback: exact prior mirror body missing");
    return m[0].replace("CREATE FUNCTION", "CREATE OR REPLACE FUNCTION");
  });
}

/** The exact 040000 lazy-current bodies (already CREATE OR REPLACE there; owner/ACL stay unchanged). */
export function originalLazyBodies(source: string): string[] {
  return [
    /CREATE OR REPLACE FUNCTION sessions\.ingest_lazy_user\(p_w jsonb,p_kind text,p_control jsonb\)[\s\S]*?\$lazy_user\$;/,
    /CREATE OR REPLACE FUNCTION sessions\.ingest_lazy_begin\(p jsonb,p_kind text\)[\s\S]*?\$lazy_begin\$;/,
  ].map((re) => {
    const m = source.match(re);
    if (!m) throw Error("privacy authority rollback: exact prior 040000 lazy body missing");
    return m[0];
  });
}

async function rollback(ctx: LaneContext): Promise<Record<string, string>> {
  const bodies = originalMirrorBodies(await ctx.read(ORIGINAL));
  const lazy = originalLazyBodies(await ctx.read(LAZY_ORIGINAL));
  const checksum = createHash("sha256").update(ctx.sql).digest("hex");
  return { [ROLLBACK]: [
    "-- Generated: reverse 041000. Only while Neon is the privacy authority (no sessions.ingest_privacy_authority row):",
    "-- once Supabase was active its heads/frozen privacy may be ahead of Neon, so dropping the authority would hand",
    "-- privacy back to a stale Neon. Release first (sessions.ingest_privacy_authority_release).",
    "-- Explicit transaction: guards, marker fence, restore and ledger removal are atomic.",
    "-- On an error the caller must ROLLBACK (or disconnect); nothing is committed.",
    "BEGIN;",
    "SELECT pg_advisory_xact_lock(961996,1); -- same namespace as the real migrator",
    "DO $rollback_guard$ DECLARE v_sha text; BEGIN",
    " IF to_regclass('sessions.__96bulls_migrations')IS NULL OR to_regclass('sessions.__96bulls_role_validation')IS NULL",
    " THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='privacy authority: migration ledger/marker required';END IF;",
    " LOCK TABLE sessions.__96bulls_migrations IN SHARE ROW EXCLUSIVE MODE;",
    " IF EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE 'upstream:%' AND name COLLATE \"C\">'" + NAME + "')",
    " THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='privacy authority: a later migration is applied; roll it back first';END IF;",
    " SELECT sha256 INTO v_sha FROM sessions.__96bulls_migrations WHERE name='" + NAME + "' FOR UPDATE;",
    " IF NOT FOUND OR v_sha<>'" + checksum + "'",
    " THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='privacy authority: own applied checksum required';END IF;",
    " PERFORM pg_advisory_xact_lock(1919249529,96); -- the authority lock: no begin/transition in flight",
    " IF EXISTS(SELECT 1 FROM sessions.ingest_privacy_authority)",
    " THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='privacy authority: release to neon first (ingest_privacy_authority_release)';END IF;",
    " UPDATE sessions.__96bulls_role_validation SET status='pending',updated_at=clock_timestamp() WHERE name='session_directory_search_worker';",
    " IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='privacy authority: validation marker required';END IF;",
    "END $rollback_guard$;",
    "DROP TRIGGER ingest_privacy_authority_guard ON sessions.ingest_source_control_heads;",
    "DROP TRIGGER ingest_privacy_authority_guard ON sessions.ingest_frozen_source_deletions;",
    "DROP TRIGGER ingest_privacy_authority_guard ON sessions.ingest_frozen_source_resets;",
    "DROP TRIGGER ingest_privacy_authority_guard ON sessions.ingest_frozen_source_records;",
    "DROP TRIGGER ingest_privacy_authority_guard ON sessions.ingest_frozen_source_owned_sessions;",
    "DROP FUNCTION sessions.ingest_privacy_eraser_still_fenced(jsonb);",
    "DROP FUNCTION sessions.ingest_privacy_eraser_page(jsonb);",
    "DROP FUNCTION sessions.ingest_privacy_eraser_groups();",
    "DROP FUNCTION sessions.ingest_privacy_eraser_require_active();",
    "DROP FUNCTION sessions.ingest_privacy_eraser_state();",
    "REVOKE USAGE ON SCHEMA sessions FROM bulls96_privacy_eraser;",
    "DROP FUNCTION sessions.ingest_privacy_authority_begin(jsonb);",
    "DROP FUNCTION sessions.ingest_privacy_authority_complete(jsonb);",
    "DROP FUNCTION sessions.ingest_privacy_authority_apply(text,text,text,text,jsonb);",
    "DROP FUNCTION sessions.ingest_privacy_authority_purge_derived(text,text);",
    "DROP FUNCTION sessions.ingest_privacy_authority_release(jsonb);",
    "DROP FUNCTION sessions.ingest_privacy_authority_activate(jsonb);",
    "DROP FUNCTION sessions.ingest_privacy_authority_fence(jsonb);",
    "DROP FUNCTION sessions.ingest_privacy_authority_state();",
    "DROP FUNCTION sessions.ingest_privacy_authority_owned_digest();",
    "DROP FUNCTION sessions.ingest_privacy_authority_packet(jsonb,jsonb,jsonb);",
    "DROP FUNCTION sessions.ingest_privacy_authority_guard();",
    "-- The legacy mirror begin/complete exactly as 20261105010000 created them (they call the lock function: restore first).",
    ...bodies,
    "-- The lazy-current adoption checks exactly as 20261105040000 left them (prefix exclusion).",
    ...lazy,
    "DROP FUNCTION sessions.ingest_privacy_authority_lock(boolean);",
    "DROP TABLE sessions.ingest_privacy_authority;",
    "DROP TABLE sessions.ingest_frozen_source_owned_sessions;",
    "ALTER TABLE sessions.ingest_source_control_heads DROP COLUMN deleted;",
    "-- The post-apply index job's index, if it was built (scripts/ops/c3-privacy-authority/ingest_privacy_authority_index.sql).",
    "DROP INDEX IF EXISTS sessions.ingest_frozen_source_records_user_session;",
    "DELETE FROM sessions.__96bulls_migrations WHERE name='" + NAME + "' AND sha256='" + checksum + "';",
    "COMMIT;",
  ].join("\n") + "\n" };
}

const owner = { secdef: false, grants: [] as string[] };
export const privacyAuthority: LaneManifest = {
  lane: "privacy-authority",
  owner: "C3 lane A, sessions-pipeline #289 (slot 041000)",
  migration: MIGRATION,
  prefixes: ["sessions.ingest_privacy_authority_", "sessions.ingest_privacy_eraser_"],
  grantSources: [{ path: MIGRATION, roles: [INGEST, ERASER] }],
  functions: {
    "sessions.ingest_privacy_authority_lock(boolean)": owner,
    "sessions.ingest_privacy_authority_guard()": { secdef: true, grants: [] },
    "sessions.ingest_privacy_authority_packet(jsonb,jsonb,jsonb)": owner,
    "sessions.ingest_privacy_authority_state()": owner,
    "sessions.ingest_privacy_authority_owned_digest()": owner,
    "sessions.ingest_privacy_authority_fence(jsonb)": owner,
    "sessions.ingest_privacy_authority_activate(jsonb)": owner,
    "sessions.ingest_privacy_authority_release(jsonb)": owner,
    "sessions.ingest_privacy_authority_apply(text,text,text,text,jsonb)": owner,
    "sessions.ingest_privacy_authority_purge_derived(text,text)": owner,
    "sessions.ingest_privacy_authority_begin(jsonb)": { secdef: true, grants: [INGEST] },
    "sessions.ingest_privacy_authority_complete(jsonb)": { secdef: true, grants: [INGEST] },
    "sessions.ingest_privacy_eraser_state()": { secdef: true, grants: [ERASER] },
    "sessions.ingest_privacy_eraser_require_active()": owner,
    "sessions.ingest_privacy_eraser_groups()": { secdef: true, grants: [ERASER] },
    "sessions.ingest_privacy_eraser_page(jsonb)": { secdef: true, grants: [ERASER] },
    "sessions.ingest_privacy_eraser_still_fenced(jsonb)": { secdef: true, grants: [ERASER] },
  },
  replaces: {
    "sessions.ingest_source_control_begin(jsonb)": { secdef: true, original: ORIGINAL },
    "sessions.ingest_source_control_complete(jsonb)": { secdef: true, original: ORIGINAL },
    "sessions.ingest_lazy_user(jsonb,text,jsonb)": { secdef: false, original: LAZY_ORIGINAL },
    "sessions.ingest_lazy_begin(jsonb,text)": { secdef: false, original: LAZY_ORIGINAL },
  },
  extraOutputs: rollback,
};
