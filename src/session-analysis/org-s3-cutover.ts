/**
 * The operator recorder for sessions.org_s3_cutover (migration 20261105062120), the input of the
 * session analysis's condition (d) (docs/session-analysis-completeness.md). Entry point:
 * scripts/record-org-s3-cutover.ts. Dry-run by default; `--execute` inserts exactly one row on
 * the migration owner connection. Khaliq-gated: recording a cutover for a real org needs his
 * explicit OK, as applying the migration does.
 *
 * Both conditions must be attested, each with its own time, or nothing is recorded:
 *   --attest-uploads-fenced     the org's fence-authority marker is written and its Worker HTTP
 *                               admission stopped, both at or before --uploads-cutover-at;
 *   --attest-turns-forwarded    the cloud turns route no longer writes the org's turns to Neon
 *                               (TURNS_WRITE_PATH 'forward'), at or before --turns-cutover-at.
 * An org without turns forwarding cannot get a row. Output is counts only: never an org, session
 * or workspace id (the operator already holds the org id they passed).
 *
 * Corrections (`--correct`) go through the owner-only sessions.org_s3_cutover_correct_v1, which
 * copies the prior row into sessions.org_s3_cutover_history and may only move cutover_at LATER
 * (stricter). Moving it earlier is refused unless `--khaliq-approved-lowering` is passed, which needs
 * Khaliq's explicit approval and prints a loud warning. Deleting a row is not offered here at all
 * (runbook-only owner action, Khaliq-gated).
 */

/** The fixed clock-skew margin the proof adds to cutover_at (062120). */
export const ORG_S3_CUTOVER_MARGIN_MS = 3_600_000;

export const ORG_S3_CUTOVER_ATTESTATION = "uploads_fenced_worker_admission_stopped+neon_turn_writes_forwarded";

export interface OrgS3CutoverInput {
  orgId: string;
  uploadsCutoverAt: string;
  turnsCutoverAt: string;
  recordedBy: string;
  note: string | null;
  execute: boolean;
  /** --correct: change an existing row through org_s3_cutover_correct_v1 instead of inserting. */
  correct: boolean;
  /** --khaliq-approved-lowering (only with --correct): permit an earlier cutover_at. */
  approvedLowering: boolean;
  /**
   * --pointer-fix-deployed-at (required): when the pointer-move first-seen fix (least created_at
   * across a move, in one transaction) was deployed in this org's session pipeline. The effective
   * cutover_at may not be earlier: moves before the fix may have raised first-seen.
   */
  pointerFixDeployedAt: string;
}

export interface OrgS3CutoverReceipt {
  mode: "dry-run" | "execute";
  /** Rows already recorded for the org before this run (0 or 1). */
  existing: number;
  /** Rows this run inserted (0 on dry-run, refusal or correction). */
  inserted: number;
  /** --correct only: rows this run corrected (0 on dry-run). */
  corrected?: number;
  /** --correct only: how the effective cutover moves against the recorded one. */
  direction?: "later" | "unchanged" | "lowered";
  /** The org's sessions whose first-seen time is strictly after the effective cutover + the 1 hour margin (would be eligible). */
  sessionsAfter: number;
  /** The org's sessions first seen at or before it (stay blocked until the census seal). */
  sessionsAtOrBefore: number;
}

export class OrgS3CutoverRefusal extends Error {
  constructor(readonly code: string) {
    super(code);
  }
}

const ISO = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+-]\d{2}:\d{2})$/u;
const ACTOR = /^[A-Za-z0-9][A-Za-z0-9._@-]{0,127}$/u;
const ORG = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,511}$/u;

const USAGE =
  "usage: record-org-s3-cutover --org <id> --uploads-cutover-at <iso> --turns-cutover-at <iso> " +
  "--recorded-by <operator> --pointer-fix-deployed-at <iso> --attest-uploads-fenced --attest-turns-forwarded [--note <text>] " +
  "[--correct [--khaliq-approved-lowering]] [--execute]";

/** Parses and validates the operator's arguments; throws OrgS3CutoverRefusal with a fixed code. */
export function parseOrgS3CutoverArgs(argv: readonly string[], now: Date = new Date()): OrgS3CutoverInput {
  const values = new Map<string, string>();
  const flags = new Set<string>();
  const VALUE = new Set(["--org", "--uploads-cutover-at", "--turns-cutover-at", "--recorded-by", "--note", "--pointer-fix-deployed-at"]);
  const FLAG = new Set(["--attest-uploads-fenced", "--attest-turns-forwarded", "--execute", "--correct", "--khaliq-approved-lowering"]);
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i]!;
    if (VALUE.has(arg)) {
      const value = argv[i + 1];
      if (value === undefined || values.has(arg)) throw new OrgS3CutoverRefusal("cutover_args");
      values.set(arg, value);
      i += 1;
    } else if (FLAG.has(arg) && !flags.has(arg)) flags.add(arg);
    else throw new OrgS3CutoverRefusal("cutover_args");
  }
  const orgId = values.get("--org") ?? "";
  const uploadsCutoverAt = values.get("--uploads-cutover-at") ?? "";
  const turnsCutoverAt = values.get("--turns-cutover-at") ?? "";
  const recordedBy = values.get("--recorded-by") ?? "";
  const note = values.get("--note") ?? null;
  if (!ORG.test(orgId)) throw new OrgS3CutoverRefusal("cutover_org");
  // Both Neon write paths must be attested closed: an org whose turns still go to Neon gets no row.
  if (!flags.has("--attest-uploads-fenced")) throw new OrgS3CutoverRefusal("cutover_uploads_not_attested");
  if (!flags.has("--attest-turns-forwarded")) throw new OrgS3CutoverRefusal("cutover_turns_not_attested");
  for (const at of [uploadsCutoverAt, turnsCutoverAt]) {
    if (!ISO.test(at) || !Number.isFinite(Date.parse(at))) throw new OrgS3CutoverRefusal("cutover_time");
    if (Date.parse(at) > now.getTime()) throw new OrgS3CutoverRefusal("cutover_time_future");
  }
  const pointerFixDeployedAt = values.get("--pointer-fix-deployed-at") ?? "";
  if (!ISO.test(pointerFixDeployedAt) || !Number.isFinite(Date.parse(pointerFixDeployedAt)))
    throw new OrgS3CutoverRefusal("cutover_pointer_fix_time");
  if (Date.parse(pointerFixDeployedAt) > now.getTime()) throw new OrgS3CutoverRefusal("cutover_time_future");
  if (Math.max(Date.parse(uploadsCutoverAt), Date.parse(turnsCutoverAt)) < Date.parse(pointerFixDeployedAt))
    throw new OrgS3CutoverRefusal("cutover_before_pointer_fix");
  if (!ACTOR.test(recordedBy)) throw new OrgS3CutoverRefusal("cutover_recorded_by");
  if (note !== null && (note.length > 1000 || /[\u0000-\u001f]/u.test(note))) throw new OrgS3CutoverRefusal("cutover_note");
  if (flags.has("--khaliq-approved-lowering") && !flags.has("--correct")) throw new OrgS3CutoverRefusal("cutover_args");
  return {
    orgId, uploadsCutoverAt, turnsCutoverAt, recordedBy, note, pointerFixDeployedAt,
    execute: flags.has("--execute"),
    correct: flags.has("--correct"),
    approvedLowering: flags.has("--khaliq-approved-lowering"),
  };
}

export { USAGE as ORG_S3_CUTOVER_USAGE };

interface Queryable {
  query<T = Record<string, unknown>>(text: string, params?: unknown[]): Promise<{ rows: T[] }>;
}

/** The org's sessions by first-seen time against a cutover: the same first-seen the proof uses (least created_at, live and tombstoned). */
const COUNTS_SQL = `
  WITH first AS (
    SELECT p.session_id, min(p.created_at) AS first_seen FROM sessions.session_index AS p
     WHERE p.org_id = $1 GROUP BY p.session_id)
  SELECT count(*) FILTER (WHERE first_seen > $2::timestamptz + interval '1 hour')::int AS after,
         count(*) FILTER (WHERE first_seen <= $2::timestamptz + interval '1 hour')::int AS at_or_before
    FROM first`;

/**
 * Dry-run: reads (0/1 existing row, eligible and blocked session counts, and for --correct the
 * direction) and writes nothing. Execute: the same reads, then one INSERT (or, with --correct, one
 * org_s3_cutover_correct_v1 call) in one transaction. Insert refuses when the org already has a row;
 * correct refuses when it has none, and refuses a lowering without approvedLowering. The caller owns
 * the connection.
 */
export async function recordOrgS3Cutover(db: Queryable, input: OrgS3CutoverInput): Promise<OrgS3CutoverReceipt> {
  const effective =
    Date.parse(input.uploadsCutoverAt) >= Date.parse(input.turnsCutoverAt) ? input.uploadsCutoverAt : input.turnsCutoverAt;
  if (!(Date.parse(effective) >= Date.parse(input.pointerFixDeployedAt))) throw new OrgS3CutoverRefusal("cutover_before_pointer_fix");
  await db.query("BEGIN");
  try {
    const existing = (await db.query<{ n: number }>("SELECT count(*)::int AS n FROM sessions.org_s3_cutover WHERE org_id = $1", [input.orgId]))
      .rows[0]!.n;
    const counts = (await db.query<{ after: number; at_or_before: number }>(COUNTS_SQL, [input.orgId, effective])).rows[0]!;
    const receipt: OrgS3CutoverReceipt = {
      mode: input.execute ? "execute" : "dry-run",
      existing,
      inserted: 0,
      sessionsAfter: counts.after,
      sessionsAtOrBefore: counts.at_or_before,
    };
    if (input.correct) {
      if (existing === 0) throw new OrgS3CutoverRefusal("cutover_not_recorded");
      const { direction } = (
        await db.query<{ direction: "later" | "unchanged" | "lowered" }>(
          `SELECT CASE WHEN $2::timestamptz < c.cutover_at THEN 'lowered' WHEN $2::timestamptz = c.cutover_at THEN 'unchanged' ELSE 'later' END AS direction
             FROM sessions.org_s3_cutover AS c WHERE c.org_id = $1`,
          [input.orgId, effective],
        )
      ).rows[0]!;
      receipt.direction = direction;
      receipt.corrected = 0;
      if (direction === "lowered" && !input.approvedLowering) throw new OrgS3CutoverRefusal("cutover_lowering_not_approved");
      if (!input.execute) {
        await db.query("ROLLBACK");
        return receipt;
      }
      const result = (
        await db.query<{ r: { direction: string } }>(
          "SELECT sessions.org_s3_cutover_correct_v1($1, $2::timestamptz, $3::timestamptz, $4, $5, $6) AS r",
          [input.orgId, input.uploadsCutoverAt, input.turnsCutoverAt, input.recordedBy, input.note, input.approvedLowering],
        )
      ).rows[0]!.r;
      if (result.direction !== direction) throw new OrgS3CutoverRefusal("cutover_correction_direction");
      await db.query("COMMIT");
      return { ...receipt, corrected: 1 };
    }
    if (!input.execute) {
      await db.query("ROLLBACK");
      return receipt;
    }
    if (existing > 0) throw new OrgS3CutoverRefusal("cutover_already_recorded");
    const inserted = (
      await db.query<{ n: number }>(
        `WITH ins AS (
           INSERT INTO sessions.org_s3_cutover (org_id, uploads_cutover_at, turns_cutover_at, attestation, recorded_by, note, pointer_fix_deployed_at)
           VALUES ($1, $2::timestamptz, $3::timestamptz, $4, $5, $6, $7::timestamptz) RETURNING 1)
         SELECT count(*)::int AS n FROM ins`,
        [input.orgId, input.uploadsCutoverAt, input.turnsCutoverAt, ORG_S3_CUTOVER_ATTESTATION, input.recordedBy, input.note, input.pointerFixDeployedAt],
      )
    ).rows[0]!.n;
    if (inserted !== 1) throw new OrgS3CutoverRefusal("cutover_insert_count");
    await db.query("COMMIT");
    return { ...receipt, inserted };
  } catch (error) {
    await db.query("ROLLBACK").catch(() => {});
    throw error;
  }
}
