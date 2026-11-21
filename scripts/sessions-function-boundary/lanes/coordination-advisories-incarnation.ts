/**
 * Lane: coordination-advisories-incarnation (slot 062220).
 * The advisory instance (docs/coordination-live-advisories.md, "Advisory instances"): owns upsert_v2 (service_role),
 * ack_v2 and pending_for_v3 (bulls96_read_api, pinned to its read scope), and REPLACES 062000's upsert_v1, ack_v1 and
 * mark_delivered_v1 (ACLs kept) to fence them to legacy rows (incarnation IS NULL). The rollback is derived here: it
 * restores the three exact 062000 bodies, drops the three new functions and the column, behind the usual guards.
 */
import { createHash } from "node:crypto";
import type { LaneContext, LaneManifest } from "../types.ts";

const MIGRATION = "supabase/migrations/20261105062220_coordination_advisories_incarnation.sql";
const ORIGINAL = "supabase/migrations/20261105062000_coordination_advisories.sql";
const ROLLBACK = "supabase/rollbacks/20261105062220_coordination_advisories_incarnation.down.sql";
const NAME = "upstream:20261105062220_coordination_advisories_incarnation.sql";

const REPLACED: ReadonlyArray<{ name: string; tag: string }> = [
  { name: "coordination_advisories_upsert_v1", tag: "$coordination_advisories_upsert$" },
  { name: "coordination_advisories_mark_delivered_v1", tag: "$coordination_advisories_mark_delivered$" },
  { name: "coordination_advisories_ack_v1", tag: "$coordination_advisories_ack$" },
];

/** 062000's exact definitions of the three fenced functions, as CREATE OR REPLACE. */
export function originalBodies(source: string): string[] {
  return REPLACED.map(({ name, tag }) => {
    const start = source.indexOf(`CREATE FUNCTION sessions.${name}(`);
    const open = start < 0 ? -1 : source.indexOf(tag, start);
    const close = open < 0 ? -1 : source.indexOf(tag, open + tag.length);
    if (close < 0 || !source.startsWith(";", close + tag.length))
      throw Error(`coordination advisories incarnation rollback: exact 062000 ${name} body missing`);
    return "CREATE OR REPLACE FUNCTION" + source.slice(start + "CREATE FUNCTION".length, close + tag.length + 1);
  });
}

async function rollback(ctx: LaneContext): Promise<Record<string, string>> {
  const checksum = createHash("sha256").update(ctx.sql).digest("hex");
  return {
    [ROLLBACK]: [
      "-- Generated coordination advisories incarnation rollback (scripts/sessions-function-boundary/lanes/coordination-advisories-incarnation.ts).",
      "-- Restores 062000's exact upsert_v1, mark_delivered_v1 and ack_v1 bodies (ACLs kept), drops upsert_v2, ack_v2,",
      "-- pending_for_v3 and the incarnation column: every row is a legacy row again, served and acked by v1.",
      "-- Turn READ_API_ADVISORY_INSTANCE_ENABLED and COORDINATION_ADVISOR_INCARNATION_ENABLED off first.",
      "-- Code only: hosted execution needs Khaliq's separate explicit approval. On refusal the caller must ROLLBACK or disconnect.",
      "BEGIN;",
      "SET LOCAL lock_timeout='5s';",
      "SELECT pg_advisory_xact_lock(961996,1);",
      "DO $rollback_guard$ DECLARE v_sha text; BEGIN",
      " IF to_regclass('sessions.__96bulls_migrations') IS NULL",
      " THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='coordination advisories incarnation: migration ledger required';END IF;",
      " LOCK TABLE sessions.__96bulls_migrations IN SHARE ROW EXCLUSIVE MODE;",
      " IF EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE 'upstream:%' AND name COLLATE \"C\">'" + NAME + "')",
      " THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='coordination advisories incarnation: a later migration is applied; roll it back first';END IF;",
      " SELECT sha256 INTO v_sha FROM sessions.__96bulls_migrations WHERE name='" + NAME + "' FOR UPDATE;",
      " IF NOT FOUND OR v_sha IS DISTINCT FROM '" + checksum + "'",
      " THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='coordination advisories incarnation: own applied checksum required';END IF;",
      "END $rollback_guard$;",
      ...originalBodies(await ctx.read(ORIGINAL)),
      "DROP FUNCTION sessions.coordination_advisories_upsert_v2(text,text,jsonb);",
      "DROP FUNCTION sessions.coordination_advisories_ack_v2(text,text,text,text,text,uuid);",
      "DROP FUNCTION sessions.coordination_advisories_pending_for_v3(text,text,text,text,integer);",
      "ALTER TABLE sessions.coordination_advisories DROP COLUMN incarnation;",
      "DELETE FROM sessions.__96bulls_migrations WHERE name='" + NAME + "' AND sha256='" + checksum + "';",
      "COMMIT;",
    ].join("\n") + "\n",
  };
}

export const coordinationAdvisoriesIncarnation: LaneManifest = {
  lane: "coordination-advisories-incarnation",
  owner: "coordination advisories instance and atomic ack (slot 062220)",
  migration: MIGRATION,
  // Exact names: the "coordination_advisories_" stem is shared with 062000, 062070 and 062190.
  prefixes: [
    "sessions.coordination_advisories_upsert_v2(", "sessions.coordination_advisories_ack_v2(",
    "sessions.coordination_advisories_pending_for_v3(",
  ],
  grantSources: [{ path: MIGRATION, roles: ["service_role", "bulls96_read_api"] }],
  functions: {
    "sessions.coordination_advisories_upsert_v2(text,text,jsonb)": { secdef: true, grants: ["service_role"] },
    "sessions.coordination_advisories_ack_v2(text,text,text,text,text,uuid)": { secdef: true, grants: ["bulls96_read_api"] },
    "sessions.coordination_advisories_pending_for_v3(text,text,text,text,integer)": { secdef: true, grants: ["bulls96_read_api"] },
  },
  replaces: {
    "sessions.coordination_advisories_upsert_v1(text,text,jsonb)": { secdef: true, original: ORIGINAL },
    "sessions.coordination_advisories_mark_delivered_v1(text,text,text,text,text)": { secdef: true, original: ORIGINAL },
    "sessions.coordination_advisories_ack_v1(text,text,text,text,text)": { secdef: true, original: ORIGINAL },
  },
  extraOutputs: rollback,
};
