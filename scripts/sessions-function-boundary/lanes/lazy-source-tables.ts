/** Replacement-only lane (slot 040000): lazy-current adoption ignores source-side tables. ACLs unchanged. */
import { createHash } from "node:crypto";
import type { LaneContext, LaneManifest } from "../types.ts";

const MIGRATION = "supabase/migrations/20261105040000_ingest_lazy_source_tables.sql";
const ORIGINAL = "supabase/migrations/20261104055000_lazy_current_custody.sql";
const ROLLBACK = "supabase/rollbacks/20261105040000_ingest_lazy_source_tables.down.sql";
const NAME = "upstream:20261105040000_ingest_lazy_source_tables.sql";

/** The exact 055000 bodies, as CREATE OR REPLACE (owner/ACL stay unchanged). */
export function originalBodies(source: string): string[] {
  return [
    /CREATE FUNCTION sessions\.ingest_lazy_user\(p_w jsonb,p_kind text,p_control jsonb\)[\s\S]*?\$lazy_user\$;/,
    /CREATE FUNCTION sessions\.ingest_lazy_begin\(p jsonb,p_kind text\)[\s\S]*?\$lazy_begin\$;/,
  ].map((re) => {
    const m = source.match(re);
    if (!m) throw Error("lazy source rollback: exact prior body missing");
    return m[0].replace("CREATE FUNCTION", "CREATE OR REPLACE FUNCTION");
  });
}

async function rollback(ctx: LaneContext): Promise<Record<string, string>> {
  const bodies = originalBodies(await ctx.read(ORIGINAL));
  const checksum = createHash("sha256").update(ctx.sql).digest("hex");
  return { [ROLLBACK]: [
    "-- Generated: restore the exact 055000 lazy-current adoption checks; owner/ACL stay unchanged.",
    "-- Explicit transaction: guard, marker fence, replacement and ledger removal are atomic.",
    "-- On an error the caller must ROLLBACK (or disconnect); nothing is committed.",
    "BEGIN;",
    "SELECT pg_advisory_xact_lock(961996,1); -- same namespace as the real migrator",
    "DO $rollback_guard$ DECLARE v_sha text; BEGIN",
    " IF to_regclass('sessions.__96bulls_migrations')IS NULL OR to_regclass('sessions.__96bulls_role_validation')IS NULL",
    " THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='lazy source tables: migration ledger/marker required';END IF;",
    " LOCK TABLE sessions.__96bulls_migrations IN SHARE ROW EXCLUSIVE MODE;",
    " IF EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE 'upstream:%' AND name COLLATE \"C\">'" + NAME + "')",
    " THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='lazy source tables: a later migration is applied; roll it back first';END IF;",
    " SELECT sha256 INTO v_sha FROM sessions.__96bulls_migrations WHERE name='" + NAME + "' FOR UPDATE;",
    " IF NOT FOUND OR v_sha<>'" + checksum + "'",
    " THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='lazy source tables: own applied checksum required';END IF;",
    " UPDATE sessions.__96bulls_role_validation SET status='pending',updated_at=clock_timestamp() WHERE name='session_directory_search_worker';",
    " IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='lazy source tables: validation marker required';END IF;",
    "END $rollback_guard$;",
    ...bodies,
    "DELETE FROM sessions.__96bulls_migrations WHERE name='" + NAME + "' AND sha256='" + checksum + "';",
    "COMMIT;",
  ].join("\n") + "\n" };
}

export const lazySourceTables: LaneManifest = {
  lane: "lazy-source-tables",
  owner: "C3 lane A, sessions-pipeline #288 (slot 040000)",
  migration: MIGRATION,
  prefixes: [],
  grantSources: [],
  functions: {},
  replaces: {
    "sessions.ingest_lazy_user(jsonb,text,jsonb)": { secdef: false, original: ORIGINAL },
    "sessions.ingest_lazy_begin(jsonb,text)": { secdef: false, original: ORIGINAL },
  },
  extraOutputs: rollback,
};
