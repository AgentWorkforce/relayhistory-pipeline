/**
 * Lane: ingest-generation-overlap (NEW slot 053020, zero-503 schema windows;
 * docs/design/zero-503-schema-windows.md).
 * supabase/migrations/20261105053020_ingest_generation_overlap.sql owns four OWNER-ONLY functions
 * (no runtime grant):
 *   ingest_generation_overlap_open       owner opens one bounded window (from -> to) before an apply
 *   ingest_generation_overlap_close      owner (or the rebind) closes it
 *   ingest_generation_overlap_admits     the one admission predicate the two replaced fences call
 *   ingest_generation_overlap_immutable  append-only trigger on the window table
 * and CREATE OR REPLACEs two owner-only fence helpers (ACL kept): ingest_require_generation (050000) and
 * ingest_lazy_guard (055000). The generated rollback restores both exact prior bodies, then drops the
 * table and the four functions.
 */
import { createHash } from "node:crypto";
import type { LaneContext, LaneManifest } from "../types.ts";

const MIGRATION = "supabase/migrations/20261105053020_ingest_generation_overlap.sql";
const GENERATION_ORIGINAL = "supabase/migrations/20261104050000_buffered_upload_receipts.sql";
const GUARD_ORIGINAL = "supabase/migrations/20261104055000_lazy_current_custody.sql";
const ROLLBACK = "supabase/rollbacks/20261105053020_ingest_generation_overlap.down.sql";
const NAME = "upstream:20261105053020_ingest_generation_overlap.sql";

/** The exact prior bodies, as CREATE OR REPLACE (owner/ACL stay unchanged). */
export function originalBodies(generationSource: string, guardSource: string): string[] {
  const pick = (source: string, re: RegExp) => {
    const m = source.match(re);
    if (!m) throw Error("generation overlap rollback: exact prior body missing");
    return m[0].replace("CREATE FUNCTION", "CREATE OR REPLACE FUNCTION");
  };
  return [
    pick(generationSource, /CREATE FUNCTION sessions\.ingest_require_generation\(p_expected text\)[\s\S]*?\$generation\$;/),
    pick(guardSource, /CREATE FUNCTION sessions\.ingest_lazy_guard\(p_org text\)[\s\S]*?\$lazy_guard\$;/),
  ];
}

async function rollback(ctx: LaneContext): Promise<Record<string, string>> {
  const bodies = originalBodies(await ctx.read(GENERATION_ORIGINAL), await ctx.read(GUARD_ORIGINAL));
  const checksum = createHash("sha256").update(ctx.sql).digest("hex");
  return { [ROLLBACK]: [
    "-- Generated: restore the exact 050000 ingest_require_generation and 055000 ingest_lazy_guard (strict PASS",
    "-- equality, no overlap window), then drop the overlap window table and its four owner-only functions.",
    "-- Owner/ACL of the two restored helpers stay unchanged. Close any open window first (it stops admitting",
    "-- the moment the restored bodies commit either way). Code only: running it against a hosted database",
    "-- needs the user's OK. Explicit transaction: guard, marker fence, replacement and ledger removal are atomic.",
    "-- On an error the caller must ROLLBACK (or disconnect); nothing is committed.",
    "BEGIN;",
    "SELECT pg_advisory_xact_lock(961996,1); -- same namespace as the real migrator",
    "DO $rollback_guard$ DECLARE v_sha text; BEGIN",
    " IF to_regclass('sessions.__96bulls_migrations')IS NULL OR to_regclass('sessions.__96bulls_role_validation')IS NULL",
    " THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='generation overlap: migration ledger/marker required';END IF;",
    " LOCK TABLE sessions.__96bulls_migrations IN SHARE ROW EXCLUSIVE MODE;",
    " IF EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE 'upstream:%' AND name COLLATE \"C\">'" + NAME + "')",
    " THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='generation overlap: a later migration is applied; roll it back first';END IF;",
    " SELECT sha256 INTO v_sha FROM sessions.__96bulls_migrations WHERE name='" + NAME + "' FOR UPDATE;",
    " IF NOT FOUND OR v_sha<>'" + checksum + "'",
    " THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='generation overlap: own applied checksum required';END IF;",
    " UPDATE sessions.__96bulls_role_validation SET status='pending',updated_at=clock_timestamp() WHERE name='session_directory_search_worker';",
    " IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='generation overlap: validation marker required';END IF;",
    "END $rollback_guard$;",
    ...bodies,
    "DROP TABLE sessions.ingest_generation_overlap_windows;",
    "DROP FUNCTION sessions.ingest_generation_overlap_immutable();",
    "DROP FUNCTION sessions.ingest_generation_overlap_admits(text,text,text);",
    "DROP FUNCTION sessions.ingest_generation_overlap_open(jsonb);",
    "DROP FUNCTION sessions.ingest_generation_overlap_close(jsonb);",
    "DELETE FROM sessions.__96bulls_migrations WHERE name='" + NAME + "' AND sha256='" + checksum + "';",
    "COMMIT;",
  ].join("\n") + "\n" };
}

export const ingestGenerationOverlap: LaneManifest = {
  lane: "ingest-generation-overlap",
  owner: "zero-503 schema windows, feat/zero-503-schema-windows (NEW slot 053020)",
  migration: MIGRATION,
  prefixes: ["sessions.ingest_generation_overlap_"],
  grantSources: [],
  functions: {
    "sessions.ingest_generation_overlap_admits(text,text,text)": { secdef: false, grants: [] },
    "sessions.ingest_generation_overlap_close(jsonb)": { secdef: false, grants: [] },
    "sessions.ingest_generation_overlap_immutable()": { secdef: false, grants: [] },
    "sessions.ingest_generation_overlap_open(jsonb)": { secdef: false, grants: [] },
  },
  replaces: {
    "sessions.ingest_require_generation(text)": { secdef: false, original: GENERATION_ORIGINAL },
    "sessions.ingest_lazy_guard(text)": { secdef: false, original: GUARD_ORIGINAL },
  },
  extraOutputs: rollback,
};
