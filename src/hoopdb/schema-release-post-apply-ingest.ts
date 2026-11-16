/**
 * The guarded schema release's POST-apply buffered-ingest check (defence in depth behind the pre-apply
 * overlap gate in schema-release-overlap.ts; runbooks/guarded-session-schema-release.md, apply step).
 *
 * After a successful apply (release_apply_verified: readiness PASS at the target generation H), the apply
 * mode reads the live ingest state READ ONLY (inspectPostApplyIngest in schema-release-inspect.ts) and
 * classifies it:
 *   - no_lazy_policy                 ok: buffered ingest is not installed;
 *   - ingest_at_target               ok: the lazy policy is already at H;
 *   - ingest_overlap_rebind_pending  ok, with a required next step: the policy is at G and an open,
 *                                    unexpired window G -> H keeps admitting G until the rebind (warning
 *                                    when fewer than RELEASE_OVERLAP_MIN_REMAINING_SECONDS are left);
 *   - ingest_fenced_post_apply       FAIL: the policy is at G and no usable window G -> H: every buffered
 *                                    claim refuses 55000 (docs/design/zero-503-schema-windows.md
 *                                    "Post-apply recovery");
 *   - ingest_post_apply_unexpected   FAIL, fail-closed: any other state (marker not PASS at H, admission
 *                                    rows off the policy generation, an invalid target, an unreadable state).
 *
 * The apply has already committed. This check never writes and never rolls back; a failure only marks the
 * run red. Its evidence carries generations, counts and flags only (no org ids, no window id, no secrets).
 */
import type { MigrationClient } from "./migrate.ts";
import {
  RELEASE_OVERLAP_MIN_REMAINING_SECONDS,
  readIngestOverlapState,
  type IngestOverlapState,
} from "./schema-release-overlap.ts";
import { refuse } from "./schema-release-contract.ts";

export const INGEST_FENCED_POST_APPLY = "ingest_fenced_post_apply";
export const INGEST_POST_APPLY_UNEXPECTED = "ingest_post_apply_unexpected";
export const POST_APPLY_RECOVERY_DOC = 'docs/design/zero-503-schema-windows.md "Post-apply recovery"';
/** Below this many seconds left on the window the pending rebind is a warning (not a failure). */
export const POST_APPLY_REBIND_WARN_SECONDS = RELEASE_OVERLAP_MIN_REMAINING_SECONDS;

const HEX64 = /^[0-9a-f]{64}$/;

export type PostApplyIngestVerdict =
  | "no_lazy_policy"
  | "ingest_at_target"
  | "ingest_overlap_rebind_pending"
  | typeof INGEST_FENCED_POST_APPLY
  | typeof INGEST_POST_APPLY_UNEXPECTED;
export type PostApplyIngestReason =
  | "target_generation_invalid"
  | "marker_not_pass_at_target"
  | "admission_off_policy"
  | "overlap_not_installed"
  | "no_window"
  | "window_from_mismatch"
  | "window_to_mismatch"
  | "window_expired"
  | "state_unreadable";

/** Admission rows by count only: total, and how many are not at the policy generation. */
export interface AdmissionCounts { rows: number; offPolicy: number }
export type PostApplyIngestState = IngestOverlapState & { admission: AdmissionCounts | null };

export interface PostApplyIngestEvidence {
  check: "post_apply_ingest";
  verdict: PostApplyIngestVerdict;
  ok: boolean;
  /** ingest_overlap_rebind_pending with fewer than POST_APPLY_REBIND_WARN_SECONDS left. */
  warn: boolean;
  reasons: PostApplyIngestReason[];
  targetGeneration: string;
  policyGeneration: string | null;
  marker: { generation: string; status: string } | null;
  overlapInstalled: boolean;
  window:
    | { present: true; from: string; to: string; expired: boolean; expiresAt: string; remainingSeconds: number }
    | { present: false };
  admission: AdmissionCounts | null;
}

/** The artifact record: the evidence, or the bare verdict when the state could not be read. */
export type PostApplyIngestRecord =
  | PostApplyIngestEvidence
  | { check: "post_apply_ingest"; verdict: typeof INGEST_POST_APPLY_UNEXPECTED; ok: false; reasons: ["state_unreadable"] };

/** The pure classification. Never throws. */
export function classifyPostApplyIngest(state: PostApplyIngestState, targetGeneration: string): PostApplyIngestEvidence {
  const H = targetGeneration;
  const G = state.policyGeneration;
  const w = state.window;
  const base: PostApplyIngestEvidence = {
    check: "post_apply_ingest",
    verdict: INGEST_POST_APPLY_UNEXPECTED,
    ok: false,
    warn: false,
    reasons: [],
    targetGeneration: H,
    policyGeneration: G,
    marker: state.marker,
    overlapInstalled: state.overlapInstalled,
    window: w
      ? { present: true, from: w.from, to: w.to, expired: w.expired, expiresAt: w.expiresAt, remainingSeconds: w.remainingSeconds }
      : { present: false },
    admission: state.admission,
  };
  const fail = (verdict: PostApplyIngestVerdict, reasons: PostApplyIngestReason[]) => ({ ...base, verdict, reasons });
  const ok = (verdict: PostApplyIngestVerdict, warn = false) => ({ ...base, verdict, ok: true, warn });
  if (!HEX64.test(H)) return fail(INGEST_POST_APPLY_UNEXPECTED, ["target_generation_invalid"]);
  if (G === null) return ok("no_lazy_policy");
  if (state.marker?.status !== "pass" || state.marker.generation !== H)
    return fail(INGEST_POST_APPLY_UNEXPECTED, ["marker_not_pass_at_target"]);
  // A buffered claim needs its org's admission row at the policy generation (044030 lazy guard); the
  // rebind moves the policy and every admission row together, so a split is never a state to accept.
  if (!state.admission || state.admission.offPolicy !== 0) return fail(INGEST_POST_APPLY_UNEXPECTED, ["admission_off_policy"]);
  if (G === H) return ok("ingest_at_target");
  const reasons: PostApplyIngestReason[] = [];
  if (!state.overlapInstalled) reasons.push("overlap_not_installed");
  else if (!w) reasons.push("no_window");
  else {
    if (w.from !== G) reasons.push("window_from_mismatch");
    if (w.to !== H) reasons.push("window_to_mismatch");
    if (w.expired || !(w.remainingSeconds > 0)) reasons.push("window_expired");
  }
  if (reasons.length) return fail(INGEST_FENCED_POST_APPLY, reasons);
  return ok("ingest_overlap_rebind_pending", w!.remainingSeconds < POST_APPLY_REBIND_WARN_SECONDS);
}

/** Plain reads (no transaction control): the caller holds a READ ONLY transaction. */
export async function readPostApplyIngestState(client: MigrationClient): Promise<PostApplyIngestState> {
  const state = await readIngestOverlapState(client);
  let admission: AdmissionCounts | null = null;
  const hasAdmission =
    (await client.query<{ t: string | null }>("SELECT to_regclass('sessions.ingest_buffer_admission')::text AS t")).rows[0]?.t != null;
  if (hasAdmission) {
    const r = (await client.query<{ rows: string; off: string }>(
      "SELECT count(*)::text AS rows, count(*) FILTER (WHERE schema_generation IS DISTINCT FROM $1)::text AS off FROM sessions.ingest_buffer_admission",
      [state.policyGeneration],
    )).rows[0];
    admission = { rows: Number(r?.rows ?? 0), offPolicy: Number(r?.off ?? 0) };
  }
  return { ...state, admission };
}

/** Annotation and summary lines (fixed tokens, the window expiry and seconds only; never generations). */
export function postApplyIngestReport(e: PostApplyIngestEvidence): { log: string[]; summary: string[] } {
  const line = "release_post_apply_ingest " + e.verdict + (e.reasons.length ? " " + e.reasons.join(",") : "");
  const log = [line];
  const summary = ["", "#### post-apply ingest check", "", "verdict: `" + e.verdict + "`" + (e.reasons.length ? " (" + e.reasons.join(", ") + ")" : "")];
  if (e.verdict === "ingest_overlap_rebind_pending" && e.window.present) {
    const next =
      "rebind required before window expires at " + e.window.expiresAt + " (" + e.window.remainingSeconds + "s remaining)";
    log.push("::notice title=ingest_overlap_rebind_pending::" + next);
    if (e.warn)
      log.push(
        "::warning title=ingest_overlap_rebind_pending::window has fewer than " +
          POST_APPLY_REBIND_WARN_SECONDS +
          "s left: run the rebind now",
      );
    summary.push("", "**Required next step:** " + next + (e.warn ? " (**under " + POST_APPLY_REBIND_WARN_SECONDS + "s: rebind now**)" : ""));
  } else if (e.verdict === INGEST_FENCED_POST_APPLY) {
    const msg =
      "apply committed, but buffered ingest is fenced out (policy behind the PASS generation, no usable overlap window): every buffered claim refuses 55000. Recover per " +
      POST_APPLY_RECOVERY_DOC;
    log.push("::error title=" + INGEST_FENCED_POST_APPLY + "::" + msg);
    summary.push("", "**" + INGEST_FENCED_POST_APPLY + "**: " + msg);
  } else if (!e.ok) {
    const msg = "apply committed, but the live ingest state is not one the release accepts (fail-closed); see the post-apply ingest artifact";
    log.push("::error title=" + INGEST_POST_APPLY_UNEXPECTED + "::" + msg);
    summary.push("", "**" + INGEST_POST_APPLY_UNEXPECTED + "**: " + msg);
  }
  return { log, summary };
}

/**
 * Emit the verdict (log, step summary, evidence) and refuse with the verdict as the code when it fails.
 * `read` failing is itself ingest_post_apply_unexpected (state_unreadable). Never writes to the database.
 */
export async function runPostApplyIngestCheck(
  read: () => Promise<PostApplyIngestEvidence>,
  sink: { log: (line: string) => void; summary: (lines: string[]) => Promise<void>; evidence: (e: PostApplyIngestRecord) => Promise<void> },
): Promise<PostApplyIngestEvidence> {
  let e: PostApplyIngestEvidence;
  try {
    e = await read();
  } catch {
    await sink.evidence({ check: "post_apply_ingest", verdict: INGEST_POST_APPLY_UNEXPECTED, ok: false, reasons: ["state_unreadable"] });
    sink.log("release_post_apply_ingest " + INGEST_POST_APPLY_UNEXPECTED + " state_unreadable");
    sink.log("::error title=" + INGEST_POST_APPLY_UNEXPECTED + "::apply committed, but the post-apply ingest state could not be read (fail-closed)");
    await sink.summary(["", "#### post-apply ingest check", "", "verdict: `" + INGEST_POST_APPLY_UNEXPECTED + "` (state_unreadable)"]);
    return refuse(INGEST_POST_APPLY_UNEXPECTED);
  }
  await sink.evidence(e);
  const report = postApplyIngestReport(e);
  for (const l of report.log) sink.log(l);
  await sink.summary(report.summary);
  if (!e.ok) refuse(e.verdict);
  return e;
}
