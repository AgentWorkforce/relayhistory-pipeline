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
import { BACKLOG_OPS_CATCHUP_MARGIN_MS } from "./backlog-ops.ts";

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
/**
 * Slot 062340's own tables: the record of writes to the sources above, not a source. They have no org_id and the proof
 * never reads them directly; it reads them only through ingest_org_admission_hold_status (erasure_history,
 * selected_full).
 */
export const ERASURE_HISTORY_TABLES: Record<string, string> = {
  ingest_org_erasure_history: "062340 history of source writes (erased_org), read via hold_status",
  ingest_org_erasure_history_coverage: "062340 coverage periods, no org, read via hold_status",
  ingest_org_erasure_history_bindings: "062340 per-epoch fence and selected FULL (bound_org), read via hold_status",
};
export const ERASURE_TABLE_NAME = /eras|delet|rearm|purge|tombstone|forget|reset/u;

/**
 * One statement: every source's in-window count for the org (at the gate-derived FULL cutoff: snapshot corroboration),
 * the hold status (062320; with slot 062340 also its erasure history), and, when 062340 has bound a FULL to the epoch,
 * whether its cutoff is exactly the hold's installed_at and whether that FULL started after the install and no later
 * than the seal: all compared IN SQL at full timestamptz (microsecond) precision. The FULL cutoff is built exactly
 * (epoch + ms intervals, no float division).
 */
export const ERASURE_WINDOW_SQL = `
WITH c AS (SELECT timestamptz 'epoch' + $2::bigint * interval '1 millisecond' AS cut),
st AS (SELECT sessions.ingest_org_admission_hold_status($1::text) AS j)
SELECT ${(Object.entries(ERASURE_SOURCES) as [string, string][]).map(([t, w]) =>
  `(SELECT count(*) FROM sessions.${t} x CROSS JOIN c WHERE x.org_id = $1::text AND (${w.replaceAll("$2", "c.cut").replace(/(^|[ (])([a-z_]+) (>=|<>|IS)/gu, "$1x.$2 $3")}))::bigint AS ${t}`).join(",\n       ")},
       st.j AS status,
       -- 062340 (revised): the bound cutoff IS the hold's installed_at; the bound FULL started strictly after the install
       -- and no later than the seal. Both compared in SQL at full timestamptz precision.
       CASE WHEN st.j->'erasure_history'->>'fence_at' IS NULL THEN NULL
            ELSE (st.j->'erasure_history'->>'fence_at')::timestamptz = (st.j->'hold'->>'installed_at')::timestamptz END AS history_cutoff_matches,
       -- installed_at < the FULL's start <= sealed_at, the start built exactly from selected_full.started_ms (integer ms).
       CASE WHEN st.j->'selected_full'->>'started_ms' IS NULL THEN NULL
            ELSE (st.j->'hold'->>'installed_at')::timestamptz < timestamptz 'epoch' + (st.j->'selected_full'->>'started_ms')::bigint * interval '1 millisecond'
             AND timestamptz 'epoch' + (st.j->'selected_full'->>'started_ms')::bigint * interval '1 millisecond' <= (st.j->'hold'->>'sealed_at')::timestamptz
            END AS history_full_ordered,
       floor(extract(epoch FROM statement_timestamp()) * 1000)::bigint AS read_at_ms
FROM st CROSS JOIN c`;

export type ErasureWindowStatus = "erasure_window_empty" | "erasure_window_nonempty" | "erasure_window_unproven";
export type ErasureWindowReason = "ok" | "erasures_in_window" | "erasures_in_history" | "hold_missing" | "hold_epoch_mismatch" | "hold_not_sealed"
  | "read_invalid" | "read_before_seal" | "history_unbound" | "history_cutoff_mismatch" | "history_not_covering_cutoff" | "history_not_recording"
  | "history_full_mismatch" | "history_full_order_invalid";
export const ERASURE_WINDOW_REASONS: readonly ErasureWindowReason[] = ["ok", "erasures_in_window", "erasures_in_history", "hold_missing", "hold_epoch_mismatch",
  "hold_not_sealed", "read_invalid", "read_before_seal", "history_unbound", "history_cutoff_mismatch", "history_not_covering_cutoff", "history_not_recording",
  "history_full_mismatch", "history_full_order_invalid"];

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
/**
 * `fullRun`: the run id of the FULL this run binds (the dry dispatch's full_backfill_run). With 062340 present, the
 * history's bound full_run must equal it exactly (history_full_mismatch otherwise); null when the caller binds none.
 */
export function erasureWindowProof(org: string, epoch: string | null, cutoffMs: number, rows: readonly Record<string, unknown>[],
  fullRun: string | null = null, fullEvidence: string | null = null, fullStartedMs: number | null = null): ErasureWindowProof {
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
  // 062340 (final names, adc8f8f5): erasure_history {fence_at, covered, recording, coverage_started_at, by_source};
  // selected_full {run, started_at, started_ms, evidence_sha256}, null until _bind.
  const sf = s.selected_full as Record<string, unknown> | null | undefined;
  if (eh!.fence_at === null || eh!.fence_at === undefined || watermark === null || sf === null || sf === undefined) return { ...held, reason: "history_unbound" };
  if (typeof sf !== "object" || Array.isArray(sf)) return held;
  // The cutoff is the hold's install (SQL); the bound FULL is this run's FULL, started after the install and no later
  // than the seal (SQL).
  if (typeof r.history_cutoff_matches !== "boolean") return held;
  if (r.history_cutoff_matches !== true) return { ...held, reason: "history_cutoff_mismatch" };
  // The bound FULL is exactly this run's: its run id, its start (integer ms), and its receipt's sha256 (the seal's
  // evidence). Compared only to selected_full, never to fence_at.
  if (fullRun === null || typeof sf.run !== "string" || sf.run !== fullRun) return { ...held, reason: "history_full_mismatch" };
  const startedMs = count(sf.started_ms);
  if (fullStartedMs === null || startedMs === null || startedMs !== fullStartedMs) return { ...held, reason: "history_full_mismatch" };
  if (fullEvidence === null || typeof sf.evidence_sha256 !== "string" || sf.evidence_sha256 !== fullEvidence) return { ...held, reason: "history_full_mismatch" };
  if (typeof r.history_full_ordered !== "boolean") return held;
  if (r.history_full_ordered !== true) return { ...held, reason: "history_full_order_invalid" };
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
    // Empty against the all-source history means the history counted 0 (a legacy two-table snapshot may carry a
    // nonzero historical total); a non-empty status never claims "ok".
    && (p.status !== "erasure_window_empty" || p.watermark_kind !== "erasure_history_all_sources" || p.watermark_erasures === 0)
    // An empty proof was read: a real read time (positive) and a cutoff no later than it.
    && (p.status !== "erasure_window_empty" || (typeof p.read_at_ms === "number" && p.read_at_ms > 0 && (p.cutoff_ms as number) <= p.read_at_ms))
    && (p.status === "erasure_window_empty") === (p.reason === "ok")
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

/** The closed refusals of an empty-window release (codes only; never an identifier). */
export const EMPTY_RELEASE_REFUSALS = ["release_org_not_in_wave", "release_proof_invalid", "release_watermark_partial", "release_binding_mismatch", "release_proof_stale",
  "release_hold_not_attempted", "release_window_nonempty", "release_window_unproven", "release_sql_refused"] as const;
export type EmptyReleaseRefusal = (typeof EMPTY_RELEASE_REFUSALS)[number];
/** The release's evidence: sha256 over a domain string and the exact proof the release acted on. */
export const emptyReleaseEvidence = (proof: ErasureWindowProof) =>
  createHash("sha256").update(`erasure-window-release/v1\n${JSON.stringify(proof)}`).digest("hex");

/**
 * May bridge_complete release on this proof alone (no manifest)? Only on a proof that is valid, empty with reason ok,
 * read from 062340's durable all-source history (watermark 0) at exactly this org, epoch and FULL cutoff, read no
 * longer ago than ERASURE_PROOF_MAX_AGE_MS, with the hold marker_attempted (062340 _release refuses bridge_complete
 * from any other state). The FULL itself (run, integer start, receipt digest) is bound inside the proof:
 * erasureWindowProof is "ok" only when 062340's selected_full is exactly the caller's FULL. A 062320-only snapshot
 * (surviving_two_tables) never releases. The signature takes no watermark: a caller cannot assert complete history.
 * This is the decision only; releaseEmptyWindowIn makes the read it decides on, under the org key.
 */
export function releaseOnEmptyWindow(proof: unknown, org: string, epoch: string, fullCutoffMs: number, nowMs: number):
  { ok: true; evidence: string } | { ok: false; code: EmptyReleaseRefusal } {
  if (!validErasureWindowProof(proof)) return { ok: false, code: "release_proof_invalid" };
  const p = proof as ErasureWindowProof;
  if (p.watermark_kind !== "erasure_history_all_sources") return { ok: false, code: "release_watermark_partial" };
  if (!UUID.test(org) || !UUID.test(epoch) || p.org_hash !== orgHash(org) || p.epoch_hash !== epochHash(epoch) || p.cutoff_ms !== fullCutoffMs)
    return { ok: false, code: "release_binding_mismatch" };
  if (p.read_at_ms === null || !Number.isSafeInteger(nowMs) || nowMs - p.read_at_ms > ERASURE_PROOF_MAX_AGE_MS || p.read_at_ms > nowMs + 60_000)
    return { ok: false, code: "release_proof_stale" };
  if (p.status === "erasure_window_nonempty") return { ok: false, code: "release_window_nonempty" };
  if (p.status !== "erasure_window_empty" || p.reason !== "ok" || p.watermark_erasures !== 0) return { ok: false, code: "release_window_unproven" };
  if (p.hold_state !== "marker_attempted") return { ok: false, code: "release_hold_not_attempted" };
  return { ok: true, evidence: emptyReleaseEvidence(p) };
}

/**
 * The org key, EXCLUSIVE, taken first in the release transaction (062320 _lifecycle: (961996,1) SHARED, then the org
 * key EXCLUSIVE). Every erasure write holds that key SHARED until it commits (062340's recorder, 062320's 'erasure'
 * check), so this waits for every in-flight erasure, and every later one waits for this transaction.
 */
export const EMPTY_RELEASE_LOCK_SQL = "SELECT sessions.ingest_org_admission_hold_lifecycle($1::text)";
/**
 * 062340 _release with bridge_complete on an empty window, p_erasures from the final proof read (0 on a release): it
 * recounts the history under the same key and refuses unless that count, and refuses erasure_window_empty unless 0.
 */
export const EMPTY_RELEASE_SQL =
  "SELECT sessions.ingest_org_admission_hold_release($1::text, $2::uuid, 'bridge_complete', $3::text, 'erasure_window_empty', $4::bigint)";
/**
 * The orgs (orgHash, never an id: the repository is public) for which an empty window alone may release: the wave-1
 * exception, and no other. EMPTY on this head, so every release refuses release_org_not_in_wave before any read; a
 * separately reviewed commit fills in exactly wave 1's two hashes. Every other org needs the manifest path (062360).
 */
export const EMPTY_RELEASE_WAVE_ORG_HASHES: readonly string[] = Object.freeze([]);
export interface EmptyReleaseInput { org: string; epoch: string; cutoffMs: number; fullRun: string; fullEvidence: string; fullStartedMs: number }
export type EmptyReleaseResult = { status: "released"; evidence: string; proof: ErasureWindowProof }
  | { status: "refused"; code: EmptyReleaseRefusal; sqlstate?: string; proof: ErasureWindowProof | null };
type Tx = { query(text: string, params?: unknown[]): Promise<{ rows: Record<string, unknown>[] }> };

/**
 * The release's body, inside the caller's READ COMMITTED transaction (062320's lifecycle refuses any other level):
 *   1. EMPTY_RELEASE_LOCK_SQL: the org key EXCLUSIVE (waits for every in-flight erasure write);
 *   2. ERASURE_WINDOW_SQL: the final proof read, a new statement snapshot taken after the key was granted, so it sees
 *      every erasure that committed before the key and none can commit until this transaction ends;
 *   3. releaseOnEmptyWindow on that proof; on ok, EMPTY_RELEASE_SQL in the same transaction (062340 recounts under the
 *      key and refuses unless the history since the fence is 0).
 * A refusal writes nothing (the caller rolls back). A SQL refusal of the release keeps only its SQLSTATE digits. `wave`
 * is EMPTY_RELEASE_WAVE_ORG_HASHES; only tests pass their own synthetic orgs' hashes. A production caller (the tool's
 * bridge-release, when wired) must never pass it, and must never take it from an input.
 */
export async function releaseEmptyWindowIn(tx: Tx, a: EmptyReleaseInput, now: () => number = Date.now,
  wave: readonly string[] = EMPTY_RELEASE_WAVE_ORG_HASHES): Promise<EmptyReleaseResult> {
  // The wave-1 exception only: an org outside it is refused before the lock or any read.
  if (!UUID.test(a.org) || !wave.includes(orgHash(a.org))) return { status: "refused", code: "release_org_not_in_wave", proof: null };
  // The window's cutoff is not a free input: it is the bound FULL's start minus the gate's catch-up margin (the gate's
  // t, backlog-ops.ts BACKLOG_OPS_CATCHUP_MARGIN_MS), exactly as bridge-dry derives it. A later cutoff would hide an
  // erasure between the true cutoff and the install from the snapshot sources. Refused before the lock or any read.
  if (!Number.isSafeInteger(a.cutoffMs) || !Number.isSafeInteger(a.fullStartedMs) || a.cutoffMs !== a.fullStartedMs - BACKLOG_OPS_CATCHUP_MARGIN_MS)
    return { status: "refused", code: "release_binding_mismatch", proof: null };
  const sqlRefusal = (e: unknown, proof: ErasureWindowProof | null): EmptyReleaseResult => {
    const c = (e as { code?: unknown }).code;
    return { status: "refused", code: "release_sql_refused", ...(typeof c === "string" && /^[0-9A-Z]{5}$/u.test(c) ? { sqlstate: c } : {}), proof };
  };
  try { await tx.query(EMPTY_RELEASE_LOCK_SQL, [a.org]); } catch (e) { return sqlRefusal(e, null); }
  let rows: readonly Record<string, unknown>[] = [];
  try { rows = (await tx.query(ERASURE_WINDOW_SQL, [a.org, String(a.cutoffMs)])).rows; } catch { return { status: "refused", code: "release_proof_invalid", proof: null }; }
  const proof = erasureWindowProof(a.org, a.epoch, a.cutoffMs, rows, a.fullRun, a.fullEvidence, a.fullStartedMs);
  const d = releaseOnEmptyWindow(proof, a.org, a.epoch, a.cutoffMs, now());
  if (!d.ok) return { status: "refused", code: d.code, proof };
  try { await tx.query(EMPTY_RELEASE_SQL, [a.org, a.epoch, d.evidence, String(proof.watermark_erasures)]); } catch (e) { return sqlRefusal(e, proof); }
  return { status: "released", evidence: d.evidence, proof };
}

/** releaseEmptyWindowIn in its own READ COMMITTED transaction: COMMIT only on a release, ROLLBACK otherwise. */
export async function releaseEmptyWindow(client: Tx, a: EmptyReleaseInput, now: () => number = Date.now,
  wave: readonly string[] = EMPTY_RELEASE_WAVE_ORG_HASHES): Promise<EmptyReleaseResult> {
  await client.query("BEGIN ISOLATION LEVEL READ COMMITTED");
  let r: EmptyReleaseResult;
  try { r = await releaseEmptyWindowIn(client, a, now, wave); }
  catch (e) { await client.query("ROLLBACK").catch(() => {}); throw e; }
  await client.query(r.status === "released" ? "COMMIT" : "ROLLBACK");
  return r;
}
