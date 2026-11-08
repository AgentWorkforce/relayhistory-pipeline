/** Replacement-only lane (slot 043800): the staged ACK locks the seat clock FOR NO KEY UPDATE, not FOR UPDATE, then the bounded origin
 *  advisory lock (fence -> clock -> origin -> slot). ACLs unchanged. */
import { createHash } from "node:crypto";
import type { LaneContext, LaneManifest } from "../types.ts";

const MIGRATION = "supabase/migrations/20261105043800_lazy_ack_clock_no_key_update.sql";
const ORIGINAL = "supabase/migrations/20261105043500_lazy_claim_clock_narrowing.sql";
const ROLLBACK = "supabase/rollbacks/20261105043800_lazy_ack_clock_no_key_update.down.sql";
const NAME = "upstream:20261105043800_lazy_ack_clock_no_key_update.sql";

/** The exact 043500 ACK body (already CREATE OR REPLACE there; owner/ACL stay unchanged). */
export function originalBodies(source: string): string[] {
  const m = source.match(/CREATE OR REPLACE FUNCTION sessions\.ingest_lazy_ack_batch\(p jsonb\)[\s\S]*?\$lazy_ack_batch\$;/);
  if (!m) throw Error("lazy ack clock rollback: exact prior body missing");
  return [m[0]];
}

async function rollback(ctx: LaneContext): Promise<Record<string, string>> {
  const bodies = originalBodies(await ctx.read(ORIGINAL));
  const checksum = createHash("sha256").update(ctx.sql).digest("hex");
  return { [ROLLBACK]: [
    "-- Generated: restore the exact 043500 staged-ACK clock locking (FOR UPDATE); owner/ACL stay unchanged.",
    "-- Explicit transaction: guard, marker fence, replacement and ledger removal are atomic.",
    "-- On an error the caller must ROLLBACK (or disconnect); nothing is committed.",
    "BEGIN;",
    "SELECT pg_advisory_xact_lock(961996,1); -- same namespace as the real migrator",
    "DO $rollback_guard$ DECLARE v_sha text; BEGIN",
    " IF to_regclass('sessions.__96bulls_migrations')IS NULL OR to_regclass('sessions.__96bulls_role_validation')IS NULL",
    " THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='lazy ack clock: migration ledger/marker required';END IF;",
    " LOCK TABLE sessions.__96bulls_migrations IN SHARE ROW EXCLUSIVE MODE;",
    " IF EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE 'upstream:%' AND name COLLATE \"C\">'" + NAME + "')",
    " THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='lazy ack clock: a later migration is applied; roll it back first';END IF;",
    " SELECT sha256 INTO v_sha FROM sessions.__96bulls_migrations WHERE name='" + NAME + "' FOR UPDATE;",
    " IF NOT FOUND OR v_sha<>'" + checksum + "'",
    " THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='lazy ack clock: own applied checksum required';END IF;",
    " UPDATE sessions.__96bulls_role_validation SET status='pending',updated_at=clock_timestamp() WHERE name='session_directory_search_worker';",
    " IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='lazy ack clock: validation marker required';END IF;",
    "END $rollback_guard$;",
    ...bodies,
    "DELETE FROM sessions.__96bulls_migrations WHERE name='" + NAME + "' AND sha256='" + checksum + "';",
    "COMMIT;",
  ].join("\n") + "\n" };
}

export const lazyAckClockNoKeyUpdate: LaneManifest = {
  lane: "lazy-ack-clock-no-key-update",
  owner: "claim-503 follow-up, fix/lazy-ack-clock-lock-043800 (slot 043800)",
  migration: MIGRATION,
  prefixes: [],
  grantSources: [],
  functions: {},
  replaces: {
    "sessions.ingest_lazy_ack_batch(jsonb)": { secdef: false, original: ORIGINAL },
  },
  extraOutputs: rollback,
};
