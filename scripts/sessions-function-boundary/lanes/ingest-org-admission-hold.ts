/**
 * Lane: ingest-org-admission-hold (slot 062320; docs/design/org-admission-hold.md revision 4, joint APPROVE 892f089e).
 * supabase/migrations/20261105062320_ingest_org_admission_fences.sql owns the owner-only hold functions (key, check,
 * lifecycle, install, seal, marker attempt, release, status, the batch backstop trigger function and the history
 * guard) and replaces 17 entry functions with their latest prior bodies plus one marked check line. ACLs unchanged.
 *
 * The generated rollback restores every replaced body exactly from the migration named in `replaces`, drops the
 * backstop trigger and the slot's functions, keeps both hold tables with their immutability guard as inert history
 * (the design's choice; a reapply reuses them), resets readiness to pending as 062210 does, and removes its ledger row.
 * It refuses while any hold is not released and while a later slot is applied.
 */
import { createHash } from "node:crypto";
import type { LaneContext, LaneManifest, ReplacedFunction } from "../types.ts";

const MIGRATION = "supabase/migrations/20261105062320_ingest_org_admission_fences.sql";
const ROLLBACK = "supabase/rollbacks/20261105062320_ingest_org_admission_fences.down.sql";
const NAME = "upstream:20261105062320_ingest_org_admission_fences.sql";
const M = (f: string) => `supabase/migrations/${f}`;

/** The 17 replaced entry functions (org-admission-hold-classes.ts), each with the migration holding its prior body. */
const REPLACES: Readonly<Record<string, ReplacedFunction>> = {
  "sessions.ingest_claim_batch_mode(jsonb,text)": { secdef: false, original: M("20261105050010_operator_session_erase_fence_check.sql") },
  "sessions.ingest_claim_buffered_batch(jsonb)": { secdef: true, original: M("20261105043850_ingest_acquisition_owner.sql") },
  "sessions.ingest_commit_batch(jsonb)": { secdef: true, original: M("20261105050010_operator_session_erase_fence_check.sql") },
  "sessions.ingest_commit_buffered_batch(jsonb)": { secdef: true, original: M("20261105050010_operator_session_erase_fence_check.sql") },
  "sessions.ingest_prepare_buffer_stage(jsonb)": { secdef: true, original: M("20261105043850_ingest_acquisition_owner.sql") },
  "sessions.ingest_commit_revisions(jsonb)": { secdef: true, original: M("20261104060000_ingest_canonical_publication.sql") },
  "sessions.ingest_fanout_fence(jsonb)": { secdef: true, original: M("20261104060000_ingest_canonical_publication.sql") },
  "sessions.ingest_seed_proven_records(jsonb)": { secdef: true, original: M("20261104040000_ingest_s3_proof_seed.sql") },
  "sessions.ingest_publication_publish(jsonb)": { secdef: true, original: M("20261105059000_ingest_publication_gcs_provider.sql") },
  "sessions.ingest_publication_claim(jsonb)": { secdef: true, original: M("20261105043850_ingest_acquisition_owner.sql") },
  "sessions.ingest_history_plan_claim(jsonb)": { secdef: true, original: M("20261105026000_canonical_history_lineage.sql") },
  "sessions.ingest_set_session_uploads(jsonb)": { secdef: true, original: M("20261105062060_turns_selection_origin_guard.sql") },
  "sessions.ingest_selfhost_set_session_uploads(jsonb)": { secdef: true, original: M("20261105062060_turns_selection_origin_guard.sql") },
  "sessions.ingest_reset_uploaded_sessions(jsonb)": { secdef: true, original: M("20261104055000_lazy_current_custody.sql") },
  "sessions.ingest_turn_register(jsonb)": { secdef: true, original: M("20261105062020_turns_forward_gates.sql") },
  "sessions.ingest_operator_runtime_begin(text,text,text,text,text,text,text,text,text,text)": { secdef: true, original: M("20261105050020_operator_session_erase_runtime.sql") },
  "sessions.operator_user_erase_begin(text,text,text,text)": { secdef: false, original: M("20261105058010_operator_user_erase.sql") },
};

/** The slot's own functions dropped by the rollback (the history guard is retained with its tables). */
const DROPPED = [
  "sessions.ingest_org_admission_hold_batch_backstop()",
  "sessions.ingest_org_admission_hold_status(text)",
  "sessions.ingest_org_admission_hold_release(text,uuid,text,text,text,bigint)",
  "sessions.ingest_org_admission_hold_marker_attempt(text,uuid,text)",
  "sessions.ingest_org_admission_hold_seal(text,uuid,text)",
  "sessions.ingest_org_admission_hold_install(text,uuid)",
  "sessions.ingest_org_admission_hold_lifecycle(text)",
  "sessions.ingest_org_admission_hold_check(text,text)",
  "sessions.ingest_org_admission_hold_key(text)",
];

/**
 * The LAST definition of `fn` in `source` (a later CREATE OR REPLACE in the same file supersedes an earlier one), as
 * written, from its CREATE to its closing dollar tag and semicolon, turned into CREATE OR REPLACE so it restores.
 */
export function lastBody(source: string, fn: string): string {
  const name = fn.slice(0, fn.indexOf("("));
  const starts = [...source.matchAll(new RegExp(`^CREATE (?:OR REPLACE )?FUNCTION ${name.replace(".", "\\.")}\\(`, "gmu"))];
  if (starts.length === 0) throw Error(`org admission hold rollback: no prior body for ${fn}`);
  const start = starts.at(-1)!.index!;
  const rest = source.slice(start);
  const tag = /AS \$([a-z_]+)\$/u.exec(rest);
  if (!tag) throw Error(`org admission hold rollback: no body tag for ${fn}`);
  const open = rest.indexOf(`$${tag[1]}$`);
  const close = rest.indexOf(`$${tag[1]}$`, open + tag[1]!.length + 2);
  const end = rest.indexOf(";", close);
  if (open < 0 || close < 0 || end < 0) throw Error(`org admission hold rollback: unterminated body for ${fn}`);
  return rest.slice(0, end + 1).replace(/^CREATE FUNCTION/u, "CREATE OR REPLACE FUNCTION");
}

async function rollback(ctx: LaneContext): Promise<Record<string, string>> {
  const checksum = createHash("sha256").update(ctx.sql).digest("hex");
  const restored: string[] = [];
  for (const [fn, r] of Object.entries(REPLACES)) {
    restored.push(`-- ${fn}: restored from ${r.original}.`, lastBody(await ctx.read(r.original), fn));
  }
  return { [ROLLBACK]: [
    "-- Generated by the ingest-org-admission-hold lane. Reverse of 20261105062320: restores the 17 replaced entry",
    "-- functions' exact prior bodies, drops the batch backstop trigger and the slot's hold functions, KEEPS",
    "-- sessions.ingest_org_admission_holds and sessions.ingest_org_admission_hold_events with their immutability guard as",
    "-- inert history (docs/design/org-admission-hold.md section 6, \"Schema down\"), and resets readiness to pending.",
    "-- Refused (55000) while any hold is not released (a held org would silently reopen), unless 062320 is applied with",
    "-- this exact checksum, and while a later upstream migration is applied. Every lifecycle function takes (961996,1)",
    "-- SHARED then re-checks this slot's ledger row in a fresh statement, so one paused before its first write refuses",
    "-- after this commits. Code only: running it against any real database needs Khaliq's explicit OK.",
    "-- After it commits, readiness is pending until the operator re-validates at the rollback target's generation (as",
    "-- after 062210's down: run that tree's migrator as the owner, rebind, serve that tree's Workers).",
    "BEGIN;",
    "SET LOCAL lock_timeout='5s';",
    "SELECT pg_advisory_xact_lock(961996,1); -- same namespace as the real migrator; lifecycle functions take it SHARED",
    "DO $rollback_guard$ DECLARE v_sha text; BEGIN",
    " IF to_regclass('sessions.__96bulls_migrations')IS NULL OR to_regclass('sessions.__96bulls_role_validation')IS NULL",
    " THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='org admission hold: migration ledger/marker required';END IF;",
    " LOCK TABLE sessions.__96bulls_migrations IN SHARE ROW EXCLUSIVE MODE;",
    " IF EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE 'upstream:%' AND name COLLATE \"C\">'" + NAME + "')",
    " THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='org admission hold: a later migration is applied; roll it back first';END IF;",
    " SELECT sha256 INTO v_sha FROM sessions.__96bulls_migrations WHERE name='" + NAME + "' FOR UPDATE;",
    " IF NOT FOUND OR v_sha IS DISTINCT FROM '" + checksum + "'",
    " THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='org admission hold: own applied checksum required';END IF;",
    " LOCK TABLE sessions.ingest_org_admission_holds IN SHARE ROW EXCLUSIVE MODE;",
    " IF EXISTS(SELECT 1 FROM sessions.ingest_org_admission_holds WHERE state<>'released')",
    " THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='org admission hold: a hold is not released; nothing changed';END IF;",
    " UPDATE sessions.__96bulls_role_validation SET status='pending',updated_at=clock_timestamp() WHERE name='session_directory_search_worker';",
    " IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='org admission hold: validation marker required';END IF;",
    "END $rollback_guard$;",
    ...restored,
    "DROP TRIGGER ingest_org_admission_hold_batch_backstop ON sessions.ingest_batches;",
    ...DROPPED.map((f) => `DROP FUNCTION ${f};`),
    "DELETE FROM sessions.__96bulls_migrations WHERE name='" + NAME + "' AND sha256='" + checksum + "';",
    "COMMIT;",
  ].join("\n") + "\n" };
}

export const ingestOrgAdmissionHold: LaneManifest = {
  lane: "ingest-org-admission-hold",
  owner: "review/org-admission-hold-062320 (slot 062320)",
  migration: MIGRATION,
  prefixes: ["sessions.ingest_org_admission_hold_"],
  grantSources: [],
  functions: {
    "sessions.ingest_org_admission_hold_guard()": { secdef: false, grants: [] },
    "sessions.ingest_org_admission_hold_key(text)": { secdef: false, grants: [] },
    "sessions.ingest_org_admission_hold_check(text,text)": { secdef: false, grants: [] },
    "sessions.ingest_org_admission_hold_lifecycle(text)": { secdef: false, grants: [] },
    "sessions.ingest_org_admission_hold_install(text,uuid)": { secdef: false, grants: [] },
    "sessions.ingest_org_admission_hold_seal(text,uuid,text)": { secdef: false, grants: [] },
    "sessions.ingest_org_admission_hold_marker_attempt(text,uuid,text)": { secdef: false, grants: [] },
    "sessions.ingest_org_admission_hold_release(text,uuid,text,text,text,bigint)": { secdef: false, grants: [] },
    "sessions.ingest_org_admission_hold_status(text)": { secdef: false, grants: [] },
    "sessions.ingest_org_admission_hold_batch_backstop()": { secdef: false, grants: [] },
  },
  replaces: REPLACES,
  extraOutputs: rollback,
};
