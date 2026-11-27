/**
 * The per-org EMPTY-ERASURE-WINDOW proof (`erasure_window_empty`): the gating criterion layered-sessions-pipeline accepts
 * for an org to reach bridge_complete WITHOUT the operator-erase replay manifest. It proves that no erasure-lane record of
 * the org exists at or after the FULL cutoff (FULL.created_at minus the margin), and none is still in flight, across EVERY
 * authoritative erasure source in the schema. A positive, incomplete, unreadable or stale result is never "empty": the
 * org stays HELD for the manifest and reconciliation. Code only, dormant.
 *
 * Binding: org, the hold epoch, the FULL cutoff, the read time, and the completion watermark: the 062320 status
 * function's erasure count (operator_user_erasures plus session_erasure_fences, which _release recomputes under the
 * exclusive hold key). The counts and the watermark come from ONE statement, so one snapshot. The read must happen with
 * the hold `sealed` or `marker_attempted` at that epoch; bridge_complete passes the same watermark to _release, so an
 * erase committed after the proof changes the count and the release refuses.
 *
 * Limit, stated: the watermark covers only the two tables _release counts. The other sources below (user controls,
 * analysis purges, relayfacts fences, frozen custody) are checked by the proof itself; one committed between the proof
 * and the release is not caught by the watermark. Controls are S3-mirrored while sealed (design section 6.3), so their S3
 * effect is already present; the rest is a residual the reviewer should weigh. Re-reading the proof immediately before the
 * release (proofFresh) narrows it to that window.
 */
import { createHash } from "node:crypto";

/**
 * Every per-org erasure-lane source, enumerated from the schema (supabase/migrations), with how a row counts as "in the
 * window": `since` (a timestamp at or after the cutoff), `open` (a row still in progress, whatever its age), or `any`
 * (a table with no timestamp: any row of the org counts, fail-closed).
 */
export const ERASURE_SOURCES = {
  operator_user_erasures: "created_at >= $2 OR updated_at >= $2 OR state <> 'bytes_verified'",
  session_erasure_fences: "created_at >= $2",
  session_erasure_receipts: "requested_at >= $2 OR updated_at >= $2 OR status <> 'completed'",
  session_erasure_events: "at >= $2",
  session_erasure_runs: "true",
  session_erasure_scopes: "true",
  session_erasure_writes: "started_at >= $2 OR finished_at IS NULL",
  upload_deletions: "deleted_at >= $2",
  upload_resets: "reset_at >= $2",
  upload_erasure_rearm: "true",
  relayfacts_erasure_fences: "created_at >= $2",
  session_analysis_purge_fences: "purged_at >= $2",
  session_analysis_purge_authority: "created_at >= $2",
  ingest_frozen_source_deletions: "true",
  ingest_frozen_source_resets: "true",
} as const;
export type ErasureSource = keyof typeof ERASURE_SOURCES;
/**
 * Tables whose names look like erasure records but are not per-org erasure sources: singletons with no org_id. Any OTHER
 * table matching ERASURE_TABLE_NAME must be classified above (the test fails the build otherwise).
 */
export const ERASURE_SOURCE_EXCLUSIONS: Record<string, string> = {
  legacy_export_privacy_eraser_readiness: "singleton eraser heartbeat, no org",
  babysitter_purge_cursor: "singleton purge keyset cursor, no org",
};
export const ERASURE_TABLE_NAME = /eras|delet|rearm|purge|tombstone|forget|reset/u;

/**
 * One statement: every source's in-window count for the org, the hold status (062320; with slot 062340 also its
 * erasure history), and, when 062340 has bound a cutoff to the epoch, whether that bound cutoff is exactly this run's
 * FULL cutoff: compared IN SQL at full timestamptz (microsecond) precision, with the cutoff built exactly (epoch + ms
 * intervals, no float division).
 */
export const ERASURE_WINDOW_SQL = `
WITH c AS (SELECT timestamptz 'epoch' + $2::bigint * interval '1 millisecond' AS cut),
st AS (SELECT sessions.ingest_org_admission_hold_status($1::text) AS j)
SELECT ${(Object.entries(ERASURE_SOURCES) as [string, string][]).map(([t, w]) =>
  `(SELECT count(*) FROM sessions.${t} x CROSS JOIN c WHERE x.org_id = $1::text AND (${w.replaceAll("$2", "c.cut").replace(/(^|[ (])([a-z_]+) (>=|<>|IS)/gu, "$1x.$2 $3")}))::bigint AS ${t}`).join(",\n       ")},
       st.j AS status,
       CASE WHEN st.j->'erasure_history'->>'cutoff' IS NULL THEN NULL
            ELSE (st.j->'erasure_history'->>'cutoff')::timestamptz = c.cut END AS history_cutoff_matches,
       floor(extract(epoch FROM statement_timestamp()) * 1000)::bigint AS read_at_ms
FROM st CROSS JOIN c`;

export type ErasureWindowStatus = "erasure_window_empty" | "erasure_window_nonempty" | "erasure_window_unproven";
export type ErasureWindowReason = "ok" | "erasures_in_window" | "erasures_in_history" | "hold_missing" | "hold_epoch_mismatch" | "hold_not_sealed"
  | "read_invalid" | "read_before_seal" | "history_unbound" | "history_cutoff_mismatch" | "history_not_covering_cutoff" | "history_not_recording";
export const ERASURE_WINDOW_REASONS: readonly ErasureWindowReason[] = ["ok", "erasures_in_window", "erasures_in_history", "hold_missing", "hold_epoch_mismatch",
  "hold_not_sealed", "read_invalid", "read_before_seal", "history_unbound", "history_cutoff_mismatch", "history_not_covering_cutoff", "history_not_recording"];

export interface ErasureWindowProof {
  v: 1; kind: "erasure_window_proof"; status: ErasureWindowStatus; reason: ErasureWindowReason;
  /** The hold epoch the read bound, as a typed hash (never the raw UUID: receipts carry codes and hashes only), or null. */
  org_hash: string; epoch_hash: string | null; cutoff_ms: number; read_at_ms: number | null; watermark_erasures: number | null;
  hold_state: "held" | "sealed" | "marker_attempted" | null; sources: Record<ErasureSource, number> | null;
  /**
   * What `watermark_erasures` counts, read from SQL in the same statement, never asserted by a caller: with slot 062340's
   * `erasure_history` present, its history rows for the org since the bound cutoff (erasure_history_all_sources);
   * without it (062320 alone), surviving rows of two tables (surviving_two_tables, snapshot-only).
   */
  watermark_kind: ReleaseWatermark;
}

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/u;
const count = (v: unknown): number | null => {
  const n = typeof v === "string" && /^(0|[1-9][0-9]{0,15})$/u.test(v) ? Number(v) : v;
  return typeof n === "number" && Number.isSafeInteger(n) && n >= 0 ? n : null;
};
const tsMs = (v: unknown): number | null => {
  if (v === null || v === undefined) return null;
  const t = typeof v === "string" ? Date.parse(v) : NaN;
  return Number.isFinite(t) && t > 0 ? t : NaN;
};
export const epochHash = (epoch: string) => "h_" + createHash("sha256").update(`hold-bridge/v1\nepoch\n${epoch}`).digest("hex").slice(0, 32);
export const orgHash = (org: string) => "h_" + createHash("sha256").update(`authority-evidence/v1\norg\n${org}`).digest("hex").slice(0, 32);

/**
 * ERASURE_WINDOW_SQL's one row into the closed proof. Empty only when every source counts 0, the row and the status are
 * well formed, the hold is live at `epoch` in `sealed` or `marker_attempted`, and the read happened after the seal.
 */
/** `epoch` null: bind the live hold's own epoch as read (a dry read takes no epoch input); else it must match. */
export function erasureWindowProof(org: string, epoch: string | null, cutoffMs: number, rows: readonly Record<string, unknown>[]): ErasureWindowProof {
  if (!UUID.test(org) || (epoch !== null && !UUID.test(epoch)) || !Number.isSafeInteger(cutoffMs) || cutoffMs < 1) throw Object.assign(new Error("erasure_window_input"), { code: "erasure_window_input" });
  const base: ErasureWindowProof = { v: 1, kind: "erasure_window_proof", status: "erasure_window_unproven", reason: "read_invalid", org_hash: orgHash(org), epoch_hash: epoch === null ? null : epochHash(epoch),
    cutoff_ms: cutoffMs, read_at_ms: null, watermark_erasures: null, hold_state: null, sources: null, watermark_kind: "surviving_two_tables" };
  if (rows.length !== 1) return base;
  const r = rows[0]!;
  const sources = {} as Record<ErasureSource, number>;
  for (const t of Object.keys(ERASURE_SOURCES) as ErasureSource[]) {
    const n = count(r[t]);
    if (n === null) return base;
    sources[t] = n;
  }
  const readAt = count(r.read_at_ms);
  let st: unknown = r.status;
  if (typeof st === "string") { try { st = JSON.parse(st); } catch { return base; } }
  const s = st as Record<string, unknown> | null;
  if (!s || typeof s !== "object" || Array.isArray(s) || readAt === null || readAt < 1) return base;
  // 062340 present: its erasure_history object, and `erasures` = history rows since the bound cutoff (NULL unbound).
  // 062320 alone: no erasure_history, and `erasures` = surviving rows of two tables.
  const eh = s.erasure_history as Record<string, unknown> | undefined;
  const historic = eh !== undefined;
  if (historic && (eh === null || typeof eh !== "object" || Array.isArray(eh))) return base;
  const watermark = s.erasures === null && historic ? null : count(s.erasures);
  if (watermark === null && !(historic && s.erasures === null)) return base;
  const out = { ...base, read_at_ms: readAt, watermark_erasures: watermark, sources,
    watermark_kind: historic ? "erasure_history_all_sources" as const : "surviving_two_tables" as const };
  const h = s!.hold as Record<string, unknown> | null | undefined;
  if (h === undefined) return out;
  if (h === null || h.state === "released") return { ...out, reason: "hold_missing" };
  if (typeof h.epoch !== "string" || !UUID.test(h.epoch) || (epoch !== null && h.epoch !== epoch)) return { ...out, reason: "hold_epoch_mismatch" };
  out.epoch_hash = epochHash(h.epoch);
  if (h.state !== "sealed" && h.state !== "marker_attempted") return { ...out, hold_state: h.state === "held" ? "held" : null, reason: "hold_not_sealed" };
  const sealedMs = tsMs(h.sealed_at);
  const state = h.state as "sealed" | "marker_attempted";
  if (sealedMs === null || Number.isNaN(sealedMs)) return { ...out, hold_state: state };
  if (!(readAt > sealedMs)) return { ...out, hold_state: state, reason: "read_before_seal" };
  const nonzero = Object.values(sources).some((n) => n > 0);
  if (!historic) {
    // 062320 alone: snapshot-only corroboration (no history claim; a stray cutoff comparison is inconsistent).
    if (r.history_cutoff_matches !== null && r.history_cutoff_matches !== undefined) return { ...out, hold_state: state };
    return { ...out, hold_state: state, status: nonzero ? "erasure_window_nonempty" : "erasure_window_empty", reason: nonzero ? "erasures_in_window" : "ok" };
  }
  // 062340: the history must be bound to this epoch at exactly this run's FULL cutoff (SQL compares it), cover that
  // cutoff, be recording, and agree with itself (by_source sums to erasures, only known sources).
  const held = { ...out, hold_state: state };
  if (eh!.cutoff === null || watermark === null) return { ...held, reason: "history_unbound" };
  if (typeof r.history_cutoff_matches !== "boolean") return held;
  if (r.history_cutoff_matches !== true) return { ...held, reason: "history_cutoff_mismatch" };
  if (eh!.covered !== true) return { ...held, reason: eh!.covered === false ? "history_not_covering_cutoff" : "read_invalid" };
  if (eh!.recording !== true) return { ...held, reason: eh!.recording === false ? "history_not_recording" : "read_invalid" };
  const bs = eh!.by_source as Record<string, unknown> | null;
  if (!bs || typeof bs !== "object" || Array.isArray(bs)) return held;
  let sum = 0;
  for (const [k, v] of Object.entries(bs)) {
    const n = count(v);
    if (!Object.hasOwn(ERASURE_SOURCES, k) || n === null || n === 0) return held;
    sum += n;
  }
  if (sum !== watermark) return held;
  if (watermark > 0) return { ...held, status: "erasure_window_nonempty", reason: "erasures_in_history" };
  return { ...held, status: nonzero ? "erasure_window_nonempty" : "erasure_window_empty", reason: nonzero ? "erasures_in_window" : "ok" };
}

/** The closed shape of a proof (exact keys; every source present). */
export function validErasureWindowProof(v: unknown): v is ErasureWindowProof {
  if (!v || typeof v !== "object" || Array.isArray(v)) return false;
  const p = v as Record<string, unknown>;
  const keys = ["v", "kind", "status", "reason", "org_hash", "epoch_hash", "cutoff_ms", "read_at_ms", "watermark_erasures", "hold_state", "sources", "watermark_kind"];
  if (Object.keys(p).length !== keys.length || !keys.every((k) => Object.hasOwn(p, k))) return false;
  const src = p.sources as Record<string, unknown> | null;
  const srcOk = src === null || (typeof src === "object" && Object.keys(src).length === Object.keys(ERASURE_SOURCES).length
    && Object.keys(ERASURE_SOURCES).every((k) => count(src[k]) !== null));
  return p.v === 1 && p.kind === "erasure_window_proof" && ["erasure_window_empty", "erasure_window_nonempty", "erasure_window_unproven"].includes(p.status as string)
    && (ERASURE_WINDOW_REASONS as readonly unknown[]).includes(p.reason)
    && typeof p.org_hash === "string" && /^h_[0-9a-f]{32}$/u.test(p.org_hash) && (p.epoch_hash === null || (typeof p.epoch_hash === "string" && /^h_[0-9a-f]{32}$/u.test(p.epoch_hash)))
    && count(p.cutoff_ms) !== null && (p.read_at_ms === null || count(p.read_at_ms) !== null)
    && (p.watermark_erasures === null || count(p.watermark_erasures) !== null)
    && (p.hold_state === null || ["held", "sealed", "marker_attempted"].includes(p.hold_state as string)) && srcOk
    && (p.watermark_kind === "surviving_two_tables" || p.watermark_kind === "erasure_history_all_sources")
    && (p.status !== "erasure_window_empty" || (p.reason === "ok" && p.epoch_hash !== null && src !== null && Object.values(src).every((n) => n === 0) && p.watermark_erasures !== null
      && (p.hold_state === "sealed" || p.hold_state === "marker_attempted")));
}

/** A proof older than this at the release is stale: re-read it immediately before the release. */
export const ERASURE_PROOF_MAX_AGE_MS = 5 * 60 * 1000;
/**
 * What the release-side check (062320 _release, recomputed under the exclusive org key) actually covers. A count of
 * surviving rows cannot prove that no event happened (a row deleted again, e.g. ingest_apply_selection removing an
 * upload_deletions row on re-enable, leaves the count unchanged), so a release on an empty window needs a DURABLE,
 * insert-only erasure history over every source, counted under the key. Today's 062320 counts two tables of surviving
 * rows: "surviving_two_tables", which never permits a release. The history is pending in the 062320 owner's lane.
 */
export type ReleaseWatermark = "surviving_two_tables" | "erasure_history_all_sources";

/**
 * May bridge_complete release on this proof alone (no manifest)? NOT ON THIS HEAD: it refuses unconditionally
 * (release_watermark_partial). No durable erasure history from the FULL cutoff through release exists, nor serialized
 * writer checks, so no proof here can authorise a release, whatever the proof or its declared watermark_kind says. The
 * future release is a separate, reviewed consumer of the follow-up history slot; it must also compare the dry receipt's
 * full_run to its selected FULL. The signature takes no watermark: a caller cannot assert complete history.
 */
export function releaseOnEmptyWindow(_proof: unknown, _org: string, _epoch: string, _fullCutoffMs: number, _hold: { epoch: string; state: string } | null,
  _nowMs: number): { ok: false; code: "release_watermark_partial" } {
  return { ok: false, code: "release_watermark_partial" };
}
