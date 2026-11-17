/** Lane: relay-turn public-selection origin guard (slot 062060). */
import { createHash } from "node:crypto";
import type { LaneContext, LaneManifest } from "../types.ts";

const MIGRATION = "supabase/migrations/20261105062060_turns_selection_origin_guard.sql";
const SELECTION_ORIGINAL = "supabase/migrations/20261104055000_lazy_current_custody.sql";
const SELFHOST_ORIGINAL = "supabase/migrations/20261105057030_selfhost_erase_directory_membership.sql";
const ROLLBACK = "supabase/rollbacks/20261105062060_turns_selection_origin_guard.down.sql";
const NAME = "upstream:20261105062060_turns_selection_origin_guard.sql";

function originalSelection(source: string): string {
  const match = source.match(
    /CREATE OR REPLACE FUNCTION sessions\.ingest_set_session_uploads\(p jsonb\)[\s\S]*?\nEND \$selection\$;/,
  );
  if (!match) throw Error("turns selection origin guard rollback: exact public selection body missing");
  return match[0];
}

function originalSelfhost(source: string): string {
  const match = source.match(
    /CREATE OR REPLACE FUNCTION sessions\.ingest_selfhost_set_session_uploads\(p jsonb\)[\s\S]*?\nEND \$selfhost_selection\$;/,
  );
  if (!match) throw Error("turns selection origin guard rollback: exact self-host selection body missing");
  return match[0];
}

async function rollback(ctx: LaneContext): Promise<Record<string, string>> {
  const checksum = createHash("sha256").update(ctx.sql).digest("hex");
  return {
    [ROLLBACK]: [
      "-- Generated turns selection origin-guard rollback. Restores the exact public and self-host selection bodies.",
      "-- Code only: hosted execution needs Khaliq's separate explicit approval.",
      "BEGIN;",
      "SET LOCAL lock_timeout='5s';",
      "SET LOCAL statement_timeout='120s';",
      "SELECT pg_advisory_xact_lock(961996,1);",
      "DO $rollback_guard$ DECLARE v_sha text; BEGIN",
      " IF to_regclass('sessions.__96bulls_migrations') IS NULL",
      " THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='turns selection origin guard: migration ledger required';END IF;",
      " LOCK TABLE sessions.__96bulls_migrations IN SHARE ROW EXCLUSIVE MODE;",
      " IF EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE 'upstream:%' AND name COLLATE \"C\">'" + NAME + "')",
      " THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='turns selection origin guard: a later migration is applied; roll it back first';END IF;",
      " SELECT sha256 INTO v_sha FROM sessions.__96bulls_migrations WHERE name='" + NAME + "' FOR UPDATE;",
      " IF NOT FOUND OR v_sha IS DISTINCT FROM '" + checksum + "'",
      " THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='turns selection origin guard: own applied checksum required';END IF;",
      "END $rollback_guard$;",
      originalSelection(await ctx.read(SELECTION_ORIGINAL)),
      originalSelfhost(await ctx.read(SELFHOST_ORIGINAL)),
      "DROP FUNCTION sessions.ingest_set_session_uploads_internal(jsonb);",
      "DELETE FROM sessions.__96bulls_migrations WHERE name='" + NAME + "' AND sha256='" + checksum + "';",
      "COMMIT;",
    ].join("\n") + "\n",
  };
}

export const turnsSelectionOriginGuard: LaneManifest = {
  lane: "turns-selection-origin-guard",
  owner: "turns-gates post-merge hardening (slot 062060)",
  migration: MIGRATION,
  prefixes: ["sessions.ingest_set_session_uploads_internal("],
  grantSources: [],
  functions: {
    "sessions.ingest_set_session_uploads_internal(jsonb)": { secdef: true, grants: [] },
  },
  replaces: {
    "sessions.ingest_set_session_uploads(jsonb)": {
      secdef: true,
      original: SELECTION_ORIGINAL,
    },
    "sessions.ingest_selfhost_set_session_uploads(jsonb)": {
      secdef: true,
      original: SELFHOST_ORIGINAL,
    },
  },
  extraOutputs: rollback,
};
