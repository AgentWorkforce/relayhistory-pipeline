import { createHash } from "node:crypto";
import type { LaneContext, LaneManifest } from "../types.ts";
const MIGRATION = "supabase/migrations/20261105050020_operator_session_erase_runtime.sql";
const NAME = "upstream:20261105050020_operator_session_erase_runtime.sql";
const GRANTED = { secdef: true, grants: ["bulls96_session_ingest"] } as const;

async function rollback(ctx: LaneContext): Promise<Record<string, string>> {
  const checksum = createHash("sha256").update(ctx.sql).digest("hex");
  return { "supabase/rollbacks/20261105050020_operator_session_erase_runtime.down.sql": [
    "-- Generated operator erase runtime rollback. Never remove active write obligations or used erase state.",
    "BEGIN;",
    "SELECT pg_advisory_xact_lock(961996,1);",
    "DO $guard$ DECLARE v_sha text;t record;BEGIN",
    " IF to_regclass('sessions.__96bulls_migrations') IS NULL THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='operator runtime: migration ledger required';END IF;",
    " LOCK TABLE sessions.__96bulls_migrations IN SHARE ROW EXCLUSIVE MODE;",
    " IF EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE 'upstream:%' AND name COLLATE \"C\">'" + NAME + "') THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='operator runtime: later migration applied';END IF;",
    " SELECT sha256 INTO v_sha FROM sessions.__96bulls_migrations WHERE name='" + NAME + "' FOR UPDATE;",
    " IF v_sha IS DISTINCT FROM '" + checksum + "' THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='operator runtime: own applied checksum required';END IF;",
    " IF EXISTS(SELECT 1 FROM sessions.session_erasure_writes) OR EXISTS(SELECT 1 FROM sessions.session_erasure_runs) OR EXISTS(SELECT 1 FROM sessions.session_erasure_scopes) THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='operator runtime: used erase or write state exists';END IF;",
    " FOR t IN SELECT c.relname FROM pg_trigger g JOIN pg_class c ON c.oid=g.tgrelid WHERE g.tgfoid='sessions.ingest_operator_runtime_metadata_guard()'::regprocedure LOOP",
    "  EXECUTE format('DROP TRIGGER operator_erase_metadata_guard ON sessions.%I',t.relname);END LOOP;",
    "END $guard$;",
    "DROP TABLE sessions.session_erasure_writes,sessions.session_erasure_scopes,sessions.session_erasure_runs;",
    "DROP FUNCTION " + [...ctx.defined].reverse().join(",\n ") + ";",
    "DELETE FROM sessions.__96bulls_migrations WHERE name='" + NAME + "';",
    "COMMIT;",
  ].join("\n") + "\n" };
}

export const operatorSessionEraseRuntime: LaneManifest = {
  lane: "operator-session-erase-runtime", owner: "hard-delete-owner (slot 050020)", migration: MIGRATION,
  prefixes: ["sessions.ingest_operator_runtime_"], grantSources: [{ path: MIGRATION, roles: ["bulls96_session_ingest", "search_consumer_runtime", "bulls96_read_api"] }],
  functions: {
    "sessions.ingest_operator_runtime_erased(text,text,text,text)": { secdef: true, grants: ["bulls96_read_api"] },
    "sessions.ingest_operator_runtime_upsert(jsonb,text,text)": { secdef: true, grants: ["search_consumer_runtime"] },
    "sessions.ingest_operator_runtime_ids(text,text,text,text)": { secdef: false, grants: [] },
    "sessions.ingest_operator_runtime_location_valid(text,text,text,text,text,text,text)": { secdef: false, grants: [] },
    "sessions.ingest_operator_runtime_catalog()": { secdef: false, grants: [] },
    "sessions.ingest_operator_runtime_metadata_guard()": { secdef: true, grants: [] },
    "sessions.ingest_operator_runtime_write_begin(jsonb)": GRANTED,
    "sessions.ingest_operator_runtime_write_finish(text,text,uuid)": GRANTED,
    "sessions.ingest_operator_runtime_write_failed(text,text,uuid)": GRANTED,
    "sessions.ingest_operator_runtime_begin(text,text,text,text,text,text,text,text,text,text)": GRANTED,
    "sessions.ingest_operator_runtime_next(uuid)": GRANTED,
    "sessions.ingest_operator_runtime_authorize(text,text,text,text,uuid)": GRANTED,
    "sessions.ingest_operator_runtime_release(text,text,text,text,uuid)": GRANTED,
    "sessions.ingest_operator_runtime_purge(text,text,text,text,uuid)": GRANTED,
    "sessions.ingest_operator_runtime_step(text,text,text,text,uuid,text,text,bigint,text)": GRANTED,
  }, replaces: {}, extraOutputs: rollback,
};
