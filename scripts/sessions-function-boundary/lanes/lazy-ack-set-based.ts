/** Replacement-only lane (slot 062155): the staged ACK's per-record loop gains a set-based twin behind a function-local
 *  gate (bulls96.lazy_ack_set_based in proconfig, installed 'off'). Lock order, modes and ACLs unchanged. */
import { createHash } from "node:crypto";
import type { LaneContext, LaneManifest } from "../types.ts";

const MIGRATION = "supabase/migrations/20261105062155_lazy_ack_set_based.sql";
const ORIGINAL = "supabase/migrations/20261105043800_lazy_ack_clock_no_key_update.sql";
const ROLLBACK = "supabase/rollbacks/20261105062155_lazy_ack_set_based.down.sql";
const NAME = "upstream:20261105062155_lazy_ack_set_based.sql";

/** The exact 043800 ACK body (CREATE OR REPLACE there too; its SET list resets proconfig, dropping the gate). */
export function originalBodies(source: string): string[] {
  const m = source.match(/CREATE OR REPLACE FUNCTION sessions\.ingest_lazy_ack_batch\(p jsonb\)[\s\S]*?\$lazy_ack_batch\$;/);
  if (!m) throw Error("lazy ack set-based rollback: exact prior body missing");
  return [m[0]];
}

async function rollback(ctx: LaneContext): Promise<Record<string, string>> {
  const bodies = originalBodies(await ctx.read(ORIGINAL));
  const checksum = createHash("sha256").update(ctx.sql).digest("hex");
  return { [ROLLBACK]: [
    "-- Generated: restore the exact 043800 staged ACK (per-record loop, no gate); owner/ACL stay unchanged.",
    "-- Explicit transaction: guard, marker fence, replacement and ledger removal are atomic.",
    "-- On an error the caller must ROLLBACK (or disconnect); nothing is committed.",
    "BEGIN;",
    "SELECT pg_advisory_xact_lock(961996,1); -- same namespace as the real migrator",
    "DO $rollback_guard$ DECLARE v_sha text; BEGIN",
    " IF to_regclass('sessions.__96bulls_migrations')IS NULL OR to_regclass('sessions.__96bulls_role_validation')IS NULL",
    " THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='lazy ack set-based: migration ledger/marker required';END IF;",
    " LOCK TABLE sessions.__96bulls_migrations IN SHARE ROW EXCLUSIVE MODE;",
    " IF EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE 'upstream:%' AND name COLLATE \"C\">'" + NAME + "')",
    " THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='lazy ack set-based: a later migration is applied; roll it back first';END IF;",
    " SELECT sha256 INTO v_sha FROM sessions.__96bulls_migrations WHERE name='" + NAME + "' FOR UPDATE;",
    " IF NOT FOUND OR v_sha<>'" + checksum + "'",
    " THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='lazy ack set-based: own applied checksum required';END IF;",
    " UPDATE sessions.__96bulls_role_validation SET status='pending',updated_at=clock_timestamp() WHERE name='session_directory_search_worker';",
    " IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='lazy ack set-based: validation marker required';END IF;",
    "END $rollback_guard$;",
    ...bodies,
    "DELETE FROM sessions.__96bulls_migrations WHERE name='" + NAME + "' AND sha256='" + checksum + "';",
    "COMMIT;",
  ].join("\n") + "\n" };
}

export const lazyAckSetBased: LaneManifest = {
  lane: "lazy-ack-set-based",
  owner: "upload 55P03 incident, review/receipt-clock-fix-set-based (slot 062155)",
  migration: MIGRATION,
  prefixes: [],
  grantSources: [],
  functions: {},
  replaces: {
    "sessions.ingest_lazy_ack_batch(jsonb)": { secdef: false, original: ORIGINAL },
  },
  extraOutputs: rollback,
};
