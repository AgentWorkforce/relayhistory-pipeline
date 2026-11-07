/** Replacement-only GC lane: preserve the existing owner's exact runtime ACL. */
import { createHash } from "node:crypto";
import type { LaneContext, LaneManifest } from "../types.ts";

const MIGRATION = "supabase/migrations/20261105039000_buffered_gc_single_candidate.sql";
const ORIGINAL = "supabase/migrations/20261104055000_lazy_current_custody.sql";
const ROLLBACK = "supabase/rollbacks/20261105039000_buffered_gc_single_candidate.down.sql";
const NAME = "upstream:20261105039000_buffered_gc_single_candidate.sql";

async function rollback(ctx: LaneContext): Promise<Record<string, string>> {
  const source = await ctx.read(ORIGINAL);
  const match = source.match(/CREATE OR REPLACE FUNCTION sessions\.ingest_buffer_gc_claim\(p jsonb\)[\s\S]*?END \$claim\$;/);
  if (!match) throw Error("GC rollback: exact prior claim body missing");
  const checksum = createHash("sha256").update(ctx.sql).digest("hex");
  return { [ROLLBACK]: [
    "-- Generated: restore the exact latest pre-039 claim; owner/ACL stay unchanged.",
    "-- Explicit transaction: guard, marker fence, replacement and ledger removal are atomic.",
    "-- On an error the caller must ROLLBACK (or disconnect); nothing is committed.",
    "BEGIN;",
    "SELECT pg_advisory_xact_lock(961996,1); -- same namespace as the real migrator",
    "DO $rollback_guard$ DECLARE v_sha text; BEGIN",
    " IF to_regclass('sessions.__96bulls_migrations')IS NULL OR to_regclass('sessions.__96bulls_role_validation')IS NULL",
    " THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='buffer gc single candidate: migration ledger/marker required';END IF;",
    " LOCK TABLE sessions.__96bulls_migrations IN SHARE ROW EXCLUSIVE MODE;",
    " IF EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE 'upstream:%' AND name COLLATE \"C\">'" + NAME + "')",
    " THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='buffer gc single candidate: a later migration is applied; roll it back first';END IF;",
    " SELECT sha256 INTO v_sha FROM sessions.__96bulls_migrations WHERE name='" + NAME + "' FOR UPDATE;",
    " IF NOT FOUND OR v_sha<>'" + checksum + "'",
    " THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='buffer gc single candidate: own applied checksum required';END IF;",
    " UPDATE sessions.__96bulls_role_validation SET status='pending',updated_at=clock_timestamp() WHERE name='session_directory_search_worker';",
    " IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='buffer gc single candidate: validation marker required';END IF;",
    "END $rollback_guard$;",
    match[0],
    "DELETE FROM sessions.__96bulls_migrations WHERE name='" + NAME + "' AND sha256='" + checksum + "';",
    "COMMIT;",
  ].join("\n") + "\n" };
}

export const bufferedGcSingleCandidate: LaneManifest = {
  lane: "buffered-gc-single-candidate",
  owner: "gc-single-candidate-039 (offline prototype 8a82c8d4)",
  migration: MIGRATION,
  prefixes: [],
  grantSources: [],
  functions: {},
  replaces: {
    "sessions.ingest_buffer_gc_claim(jsonb)": { secdef: true, original: ORIGINAL },
  },
  extraOutputs: rollback,
};
