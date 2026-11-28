/**
 * Lane: event-facts-observed-paths (slot 062400).
 * Observed paths on event facts (docs/event-facts-observed-paths.md): adds
 * sessions.session_event_facts.observed_paths and REPLACES 045040's output_facts_reconcile and 062140's
 * staged_facts_reconcile_v1 (same signatures, ACLs kept) so they carry the column through. It owns no new function.
 * The rollback is derived here: it restores the two exact original bodies and drops the column, behind the usual
 * guards (ledger present, no later migration applied, this step's own checksum).
 */
import { createHash } from "node:crypto";
import type { LaneContext, LaneManifest } from "../types.ts";

const MIGRATION = "supabase/migrations/20261105062400_event_facts_observed_paths.sql";
const OUTPUT_FACTS = "supabase/migrations/20261105045040_selfhost_output_facts.sql";
const STAGED_FACTS = "supabase/migrations/20261105062140_staged_output_facts.sql";
const ROLLBACK = "supabase/rollbacks/20261105062400_event_facts_observed_paths.down.sql";
const NAME = "upstream:20261105062400_event_facts_observed_paths.sql";

/** An original migration's exact definition of `name`, as CREATE OR REPLACE. */
export function originalBody(source: string, name: string, tag: string): string {
  const start = source.indexOf(`CREATE FUNCTION sessions.${name}(`);
  const open = start < 0 ? -1 : source.indexOf(tag, start);
  const close = open < 0 ? -1 : source.indexOf(tag, open + tag.length);
  if (close < 0 || !source.startsWith(";", close + tag.length)) throw Error(`event facts observed paths rollback: exact original ${name} body missing`);
  return "CREATE OR REPLACE FUNCTION" + source.slice(start + "CREATE FUNCTION".length, close + tag.length + 1);
}

async function rollback(ctx: LaneContext): Promise<Record<string, string>> {
  const checksum = createHash("sha256").update(ctx.sql).digest("hex");
  const staged = await ctx.read(STAGED_FACTS);
  const stagedTag = /CREATE FUNCTION sessions\.staged_facts_reconcile_v1\([^]*?AS (\$\w*\$)/.exec(staged)?.[1];
  if (!stagedTag) throw Error("event facts observed paths rollback: staged_facts_reconcile_v1 body tag missing");
  return {
    [ROLLBACK]: [
      "-- Generated event facts observed paths rollback (scripts/sessions-function-boundary/lanes/event-facts-observed-paths.ts).",
      "-- Restores 045040's exact output_facts_reconcile and 062140's exact staged_facts_reconcile_v1 (ACLs kept) and drops",
      "-- sessions.session_event_facts.observed_paths (derived values; a facts pass rebuilds them once the step is back).",
      "-- Code only: hosted execution needs Khaliq's separate explicit approval. On refusal the caller must ROLLBACK or disconnect.",
      "BEGIN;",
      "SET LOCAL lock_timeout='5s';",
      "SELECT pg_advisory_xact_lock(961996,1);",
      "DO $rollback_guard$ DECLARE v_sha text; BEGIN",
      " IF to_regclass('sessions.__96bulls_migrations') IS NULL",
      " THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='event facts observed paths: migration ledger required';END IF;",
      " LOCK TABLE sessions.__96bulls_migrations IN SHARE ROW EXCLUSIVE MODE;",
      " IF EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE 'upstream:%' AND name COLLATE \"C\">'" + NAME + "')",
      " THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='event facts observed paths: a later migration is applied; roll it back first';END IF;",
      " SELECT sha256 INTO v_sha FROM sessions.__96bulls_migrations WHERE name='" + NAME + "' FOR UPDATE;",
      " IF NOT FOUND OR v_sha IS DISTINCT FROM '" + checksum + "'",
      " THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='event facts observed paths: own applied checksum required';END IF;",
      "END $rollback_guard$;",
      originalBody(await ctx.read(OUTPUT_FACTS), "output_facts_reconcile", "$reconcile$"),
      originalBody(staged, "staged_facts_reconcile_v1", stagedTag),
      "ALTER TABLE sessions.session_event_facts DROP COLUMN observed_paths;",
      "DELETE FROM sessions.__96bulls_migrations WHERE name='" + NAME + "' AND sha256='" + checksum + "';",
      "COMMIT;",
    ].join("\n") + "\n",
  };
}

export const eventFactsObservedPaths: LaneManifest = {
  lane: "event-facts-observed-paths",
  owner: "observed paths on event facts (slot 062400)",
  migration: MIGRATION,
  // It owns no function: both are replacements, and their stems belong to the 045040 and 062140 lanes.
  prefixes: [],
  grantSources: [],
  functions: {},
  replaces: {
    "sessions.output_facts_reconcile(text,text,text,text[],text,jsonb,jsonb,boolean,bigint)": { secdef: true, original: OUTPUT_FACTS },
    "sessions.staged_facts_reconcile_v1(bigint,uuid,text,text,text,text[],text,boolean,bigint)": { secdef: true, original: STAGED_FACTS },
  },
  extraOutputs: rollback,
};
