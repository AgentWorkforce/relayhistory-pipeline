/**
 * Lane: session-directory-binding (slot 062380).
 * The session binding (one server-issued id per incarnation of a logical session): the trigger function that is the
 * columns' only writer (no grants), and the SECURITY DEFINER reader for GET /v1/session-bindings, the directory list's
 * read-role tenant scope, EXECUTE to bulls96_read_api only. The rollback is derived here: it drops the reader, the
 * trigger and its function, the pair constraint, the three columns and the revision sequence, behind the usual guards.
 */
import { createHash } from "node:crypto";
import type { LaneContext, LaneManifest } from "../types.ts";

const MIGRATION = "supabase/migrations/20261105062380_session_directory_binding.sql";
const ROLLBACK = "supabase/rollbacks/20261105062380_session_directory_binding.down.sql";
const NAME = "upstream:20261105062380_session_directory_binding.sql";

async function rollback(ctx: LaneContext): Promise<Record<string, string>> {
  const checksum = createHash("sha256").update(ctx.sql).digest("hex");
  return {
    [ROLLBACK]: [
      "-- Generated session directory binding rollback (scripts/sessions-function-boundary/lanes/session-directory-binding.ts).",
      "-- Drops the 062380 bindings reader, the binding trigger and its function, the pair constraint, the three columns and the revision sequence:",
      "-- every issued binding id is forgotten, so a consumer holding one fails closed. Turn",
      "-- READ_API_SESSION_BINDINGS_ENABLED off first.",
      "-- Code only: hosted execution needs Khaliq's separate explicit approval. On refusal the caller must ROLLBACK or disconnect.",
      "BEGIN;",
      "SET LOCAL lock_timeout='5s';",
      "SELECT pg_advisory_xact_lock(961996,1);",
      "DO $rollback_guard$ DECLARE v_sha text; BEGIN",
      " IF to_regclass('sessions.__96bulls_migrations') IS NULL",
      " THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session directory binding: migration ledger required';END IF;",
      " LOCK TABLE sessions.__96bulls_migrations IN SHARE ROW EXCLUSIVE MODE;",
      " IF EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE 'upstream:%' AND name COLLATE \"C\">'" + NAME + "')",
      " THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session directory binding: a later migration is applied; roll it back first';END IF;",
      " SELECT sha256 INTO v_sha FROM sessions.__96bulls_migrations WHERE name='" + NAME + "' FOR UPDATE;",
      " IF NOT FOUND OR v_sha IS DISTINCT FROM '" + checksum + "'",
      " THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session directory binding: own applied checksum required';END IF;",
      "END $rollback_guard$;",
      "DROP FUNCTION sessions.session_directory_bindings_for(text,text[],text[]);",
      "DROP TRIGGER zz_session_directory_binding ON sessions.session_directory;",
      "DROP FUNCTION sessions.session_directory_binding();",
      "ALTER TABLE sessions.session_directory DROP CONSTRAINT session_directory_binding_pair,",
      "  DROP COLUMN session_binding_created_at, DROP COLUMN session_binding_revision, DROP COLUMN session_binding_id;",
      "DROP SEQUENCE sessions.session_binding_revision;",
      "DELETE FROM sessions.__96bulls_migrations WHERE name='" + NAME + "' AND sha256='" + checksum + "';",
      "COMMIT;",
    ].join("\n") + "\n",
  };
}

export const sessionDirectoryBinding: LaneManifest = {
  lane: "session-directory-binding",
  owner: "session binding: per-incarnation id and its read API reader (slot 062380)",
  migration: MIGRATION,
  // Exact names: the "session_directory_" stem is shared with many lanes.
  prefixes: ["sessions.session_directory_binding(", "sessions.session_directory_bindings_for("],
  grantSources: [{ path: MIGRATION, roles: ["bulls96_read_api"] }],
  functions: {
    "sessions.session_directory_binding()": { secdef: true, grants: [] },
    "sessions.session_directory_bindings_for(text,text[],text[])": { secdef: true, grants: ["bulls96_read_api"] },
  },
  replaces: {},
  extraOutputs: rollback,
};
