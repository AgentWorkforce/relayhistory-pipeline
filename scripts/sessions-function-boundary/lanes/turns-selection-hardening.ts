/** Lane: relay-turn selection lock/origin hardening (slot 062015). */
import { createHash } from "node:crypto";
import type { LaneContext, LaneManifest } from "../types.ts";

const MIGRATION = "supabase/migrations/20261105062015_turns_selection_hardening.sql";
const ORIGINAL = "supabase/migrations/20261104030000_ingest_fence_ownership.sql";
const ROLLBACK = "supabase/rollbacks/20261105062015_turns_selection_hardening.down.sql";
const NAME = "upstream:20261105062015_turns_selection_hardening.sql";

function originalBody(source: string): string {
  const match = source.match(
    /CREATE OR REPLACE FUNCTION sessions\.ingest_apply_selection\(p_org text, p_ws text, p_user text, p_origin text,[\s\S]*?\n\$apply\$;/,
  );
  if (!match) throw Error("turns selection hardening rollback: exact prior body missing");
  return match[0];
}

async function rollback(ctx: LaneContext): Promise<Record<string, string>> {
  const checksum = createHash("sha256").update(ctx.sql).digest("hex");
  return {
    [ROLLBACK]: [
      "-- Generated turns selection hardening rollback. Restores 030000's exact ingest_apply_selection body.",
      "-- Run only after rolling back 062020 through its operator wrapper. Code only: hosted execution needs Khaliq's OK.",
      "BEGIN;",
      "SET LOCAL lock_timeout='5s';",
      "SET LOCAL statement_timeout='120s';",
      "SELECT pg_advisory_xact_lock(961996,1);",
      "DO $rollback_guard$ DECLARE v_sha text; BEGIN",
      " IF to_regclass('sessions.__96bulls_migrations') IS NULL",
      " THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='turns selection hardening: migration ledger required';END IF;",
      " LOCK TABLE sessions.__96bulls_migrations IN SHARE ROW EXCLUSIVE MODE;",
      " IF EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE 'upstream:%' AND name COLLATE \"C\">'" + NAME + "')",
      " THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='turns selection hardening: a later migration is applied; roll it back first';END IF;",
      " SELECT sha256 INTO v_sha FROM sessions.__96bulls_migrations WHERE name='" + NAME + "' FOR UPDATE;",
      " IF NOT FOUND OR v_sha IS DISTINCT FROM '" + checksum + "'",
      " THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='turns selection hardening: own applied checksum required';END IF;",
      "END $rollback_guard$;",
      originalBody(await ctx.read(ORIGINAL)),
      "DELETE FROM sessions.__96bulls_migrations WHERE name='" + NAME + "' AND sha256='" + checksum + "';",
      "COMMIT;",
    ].join("\n") + "\n",
  };
}

export const turnsSelectionHardening: LaneManifest = {
  lane: "turns-selection-hardening",
  owner: "turns-gates post-merge hardening (slot 062015)",
  migration: MIGRATION,
  prefixes: [],
  grantSources: [],
  functions: {},
  replaces: {
    "sessions.ingest_apply_selection(text,text,text,text,jsonb,boolean)": {
      secdef: false,
      original: ORIGINAL,
    },
  },
  extraOutputs: rollback,
};
