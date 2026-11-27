/**
 * marker-dry / marker-execute: the per-org fence-authority marker (ledger-s3.ts validOrgFenceAuthority, decision 0005
 * rollout step 4) published ONLY under the org's enforced 062320 admission hold, for a wave of up to 64 orgs with an
 * independent verdict per org (org-wave.ts). Run by scripts/backlog-marker.ts; wired as backlog-ops modes
 * (backlog-ops.ts); docs/backlog-ops-workflow.md "Marker publication".
 *
 * Authority is the hold, not the evidence. The live evidence (authority-evidence.ts, bound to the catch-up's baseline)
 * stays corroboration; what makes a marker safe is that slot 062320 refuses every admission ('admission' class) and
 * publication of the org while its hold is sealed or marker_attempted, and the state machine is monotone (a released
 * hold never comes back). Per org, marker-execute:
 *
 *   1. reads the hold (sessions.ingest_org_admission_hold_status): a live hold whose epoch hashes to the epoch the
 *      marker-dry recorded for this org (the approved wave), state `sealed` or (a retry) `marker_attempted`.
 *      Anything else refuses that org before any write: hold_absent, hold_released, hold_not_sealed (still `held`),
 *      hold_epoch_changed, hold_state_invalid, hold_unreadable.
 *   2. checks every marker field's attestation (MARKER_ATTESTATIONS) and that no marker is already there that this
 *      epoch cannot own; the bound evidence report must still be fresh (BACKLOG_OPS_AUTHORITY_REPORT_MAX_AGE_MS).
 *   3. commits `_marker_attempt(org, epoch, sha256(final catch-up receipt))` in its own READ COMMITTED transaction.
 *      This is the point of no return: after it only `bridge_complete` releases (hold-bridge.ts releaseAllowed), so a
 *      failure from here on leaves the org HELD (fail safe) and a rerun retries it.
 *   4. opens a READ COMMITTED READ ONLY transaction, takes the org's hold key SHARED through
 *      `_check(org, 'erasure')` (never refused; erasures still proceed, they take it SHARED too), and re-reads the hold
 *      in a later statement: it must be `marker_attempted` at the same epoch. While that transaction is open no
 *      `_release` (EXCLUSIVE) can commit, so a concurrent release waits for the publication instead of racing it;
 *      a release that committed before the lock refuses the org (hold_released_before_put) with nothing written.
 *   5. inside that transaction: GET the marker; absent -> PUT create-only (If-None-Match) and read it back
 *      (backlog-authority.ts writeOrgMarker); present -> ADOPT it only when it is this epoch's (markerAdoptable), else
 *      `marker_conflict`. Every S3 call is bounded (MARKER_S3_TIMEOUT_MS); a timeout is `marker_put_unknown`.
 *   6. re-reads the hold in the SAME transaction after the PUT (still holding the key): `marker_attempted` at the same
 *      epoch proves the hold was live from before the PUT to after it. If the transaction died (the lock may have
 *      gone with it), the hold is re-read on a fresh statement; still live at the epoch is the same proof (monotone);
 *      released is `marker_hold_lapsed` (incomplete, needs the hold owner).
 *
 * Retry. A rerun binds the same marker-dry and the same wave id. `_marker_attempt` is idempotent at the same epoch.
 * A marker already present is adopted when (markerAdoptable): it is a valid marker for this org; the hold is live at
 * the approved epoch in `marker_attempted`; its writtenAtMs is no earlier than the epoch's first attempt minus
 * MARKER_CLOCK_SKEW_MS and no later than now plus that skew; and no OTHER epoch of this org ever recorded a marker
 * attempt (so no other cutover could have written it). Anything else is a conflict and is never overwritten.
 *
 * marker-dry runs steps 1 and 2 read-only (no attempt, no PUT), plus the sweeper and the bound evidence, and records
 * per org its readiness and its epoch's hash. Its `wave_id` (org-wave.ts waveId over the commit, scope, bound runs and
 * the READY orgs with their epoch hashes) is what the execute's typed confirmation must name (`WAVE <id>`), so the
 * environment approval covers exactly that set of orgs at exactly those epochs; marker-execute acts on those orgs
 * only (any other scope org: not_in_wave) and refuses an org whose epoch moved.
 *
 * Receipts are counts, codes and hashes only; they never name an org, epoch, key or bucket. Errors carry fixed codes.
 */
import { createHash } from "node:crypto";
import { isObjectStoreError, objectKey, readAll, type ObjectStore } from "../shared/vendor.ts";
import {
  authorityEvidence,
  evidenceHash,
  type AuthorityBaseline,
  type AuthorityEvidenceReport,
  type AuthorityEvidenceSources,
} from "./authority-evidence.ts";
import {
  BACKLOG_OPS_AUTHORITY_REPORT_MAX_AGE_MS,
  BacklogAuthorityRefusal,
  readServingSpec,
  sweeperExpectation,
  toReceiptShape,
  writeOrgMarker,
} from "./backlog-authority.ts";
import { BACKLOG_OPS_SWEEPER_PASS_MAX_AGE_MS } from "./backlog-sweeper-limits.ts";
import { EVIDENCE_SCHEMA, conforms, type ReceiptSchema } from "./backlog-authority-schema.ts";
import { epochHash } from "./erasure-window.ts";
import { HOLD_FUNCTIONS, holdSql } from "./hold-bridge.ts";
import { orgFenceAuthorityKey, validOrgFenceAuthority } from "./ledger-s3.ts";
import { runWave, waveId, waveOrgList, WAVE_DEFAULT_CONCURRENCY } from "./org-wave.ts";
import { freshSweeperPass, type RearmPassExpectation } from "./rearm-sweeper-receipt.ts";
import { isAccessDenied } from "./rearm-sweeper.ts";

/** Each S3 call inside the guarded transaction is bounded by this (the transaction holds the org key meanwhile). */
export const MARKER_S3_TIMEOUT_MS = 60_000;
/** Runner clock versus database clock, for adopting a marker this epoch wrote (writtenAtMs vs marker_attempted_at). */
export const MARKER_CLOCK_SKEW_MS = 2 * 60 * 1000;
/** The guarded transaction's lock and statement bounds (a release holding the key makes the SHARED request wait). */
export const MARKER_LOCK_TIMEOUT_MS = 30_000;
export const MARKER_STATEMENT_TIMEOUT_MS = 60_000;
const ORG_UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/u;
const UUID = ORG_UUID;
const HEX64 = /^[0-9a-f]{64}$/u;
const HASH = /^h_[0-9a-f]{32}$/u;

export class MarkerRefusal extends Error {
  constructor(readonly code: string) { super(code); this.name = "MarkerRefusal"; }
}
const refuse = (code: string): never => { throw new MarkerRefusal(code); };

// ---- SQL ----------------------------------------------------------------------------------------------------------

/** The 062320 statements this module runs (owner-only functions; the tool's login is the migration owner). */
export const MARKER_SQL = {
  status: (org: string) => holdSql.status(org),
  markerAttempt: (org: string, epoch: string, evidence: string) => holdSql.markerAttempt(org, epoch, evidence),
  /** Takes the org key SHARED for the rest of the transaction (the erasure class is never refused). */
  lockShared: (org: string) => {
    if (!ORG_UUID.test(org)) refuse("marker_org");
    return { text: "SELECT sessions.ingest_org_admission_hold_check($1::text, 'erasure') AS locked", params: [org] };
  },
  /** How many OTHER epochs of the org ever recorded a marker attempt (an earlier cutover's marker is not ours). */
  otherAttempts: (org: string, epoch: string) => {
    if (!ORG_UUID.test(org) || !UUID.test(epoch)) refuse("marker_org");
    return { text: `SELECT count(*)::int AS n FROM sessions.ingest_org_admission_hold_events e
      JOIN sessions.ingest_org_admission_holds h ON h.epoch = e.epoch
      WHERE h.held_org = $1::text AND e.kind = 'marker_attempted' AND h.epoch <> $2::uuid`, params: [org, epoch] };
  },
} as const;
export { HOLD_FUNCTIONS };

type Rows = readonly Record<string, unknown>[];
/**
 * One database session (one connection) for one org at a time. `read` and `write` each run one statement alone in its
 * own READ COMMITTED transaction (`write` READ WRITE, for the marker attempt only). `guarded` opens a READ COMMITTED
 * READ ONLY transaction, runs `lockShared(org)`, then `body` with a `read` that runs inside that same transaction,
 * and commits after `body` returns (or rolls back when it throws). If the transaction fails at commit, `guarded`
 * throws a MarkerRefusal `marker_guard_lost`.
 */
export interface MarkerSqlSession {
  read(text: string, params: readonly unknown[]): Promise<Rows>;
  write(text: string, params: readonly unknown[]): Promise<Rows>;
  guarded<T>(org: string, body: (inTx: (text: string, params: readonly unknown[]) => Promise<Rows>) => Promise<T>): Promise<T>;
}

export interface LiveHold { epoch: string; state: "held" | "sealed" | "marker_attempted"; attemptedMs: number | null }
const tsMs = (v: unknown): number | null | typeof NaN => {
  if (v === null || v === undefined) return null;
  if (typeof v !== "string") return NaN;
  const t = Date.parse(v);
  return Number.isFinite(t) && t > 0 ? t : NaN;
};
/**
 * The hold from `_status`'s one row: the live hold, or null when there is none or the latest is released (the marker
 * reads the hold only: the 062340 erasure watermark may be null before the bridge binds, and is the bridge's). Refuses
 * `hold_state_invalid` on any other shape, including timestamps that contradict the state.
 */
export function parseLiveHold(rows: Rows): { hold: LiveHold | null; released: boolean } {
  const st = rows.length === 1 ? rows[0]!.status : undefined;
  const o = typeof st === "string" ? (() => { try { return JSON.parse(st); } catch { return undefined; } })() : st;
  if (!o || typeof o !== "object" || Array.isArray(o) || !Object.hasOwn(o, "hold")) return refuse("hold_state_invalid");
  const h = (o as Record<string, unknown>).hold as Record<string, unknown> | null;
  if (h === null) return { hold: null, released: false };
  if (typeof h !== "object" || Array.isArray(h) || typeof h.epoch !== "string" || !UUID.test(h.epoch)) return refuse("hold_state_invalid");
  if (h.state === "released") return { hold: null, released: true };
  if (!["held", "sealed", "marker_attempted"].includes(h.state as string)) return refuse("hold_state_invalid");
  const sealed = tsMs(h.sealed_at), attempted = tsMs(h.marker_attempted_at);
  if (Number.isNaN(sealed) || Number.isNaN(attempted) || tsMs(h.installed_at) === null || Number.isNaN(tsMs(h.installed_at)) ||
      (h.state === "held" && (sealed !== null || attempted !== null)) || (h.state === "sealed" && (sealed === null || attempted !== null)) ||
      (h.state === "marker_attempted" && (sealed === null || attempted === null)))
    return refuse("hold_state_invalid");
  return { hold: { epoch: h.epoch, state: h.state as LiveHold["state"], attemptedMs: attempted as number | null }, released: false };
}

// ---- attestations ---------------------------------------------------------------------------------------------------

/** Every field of the marker (ledger-s3.ts validOrgFenceAuthority) and what attests it. */
export const MARKER_ATTESTATIONS = {
  fence_backfill: "the FULL backfill-execute and the final catch-up both completed the fences section, failed 0",
  ownership_backfill: "the FULL and the catch-up both completed the ownership section, failed 0",
  revision_backfill: "the FULL and the catch-up both completed the revisions section, failed 0",
  control_mirror: "the bound evidence: every serving ingest Worker version carries INGEST_S3_FENCE_MIRROR=on",
  rearm_sweeper: "freshSweeperPass for the org against the serving sweeper's spec",
  revision_layout: "the bound reshard-dry verified the org's 4096-shard revision layout (missing, invalid, failed 0)",
  worker_http_admission: "the bound evidence (stop, queue drained, SQL quiet, turns on Neon) AND the 062320 hold enforcing it",
} as const;
export type MarkerAttestation = keyof typeof MARKER_ATTESTATIONS;
export const MARKER_ATTESTATION_FIELDS = Object.keys(MARKER_ATTESTATIONS) as MarkerAttestation[];

/** What the credential step proved from the bound receipts (it checked each before any credential). */
export interface MarkerBound {
  /** Sections complete with failed 0 in BOTH the FULL and the final catch-up. */
  sections: { fences: boolean; ownership: boolean; revisions: boolean };
  /** The org hashes the bound reshard-dry verified. */
  reshardVerified: readonly string[];
  /** sha256 of the final catch-up receipt's exact bytes: `_marker_attempt`'s evidence. */
  catchupEvidence: string;
}
export function validMarkerBound(v: unknown): v is MarkerBound {
  const o = v as MarkerBound;
  return !!o && typeof o === "object" && !!o.sections && typeof o.sections === "object" &&
    ["fences", "ownership", "revisions"].every((k) => typeof (o.sections as Record<string, unknown>)[k] === "boolean") &&
    Object.keys(o.sections).length === 3 && Array.isArray(o.reshardVerified) && o.reshardVerified.length <= 64 &&
    o.reshardVerified.every((h) => typeof h === "string" && HASH.test(h)) && typeof o.catchupEvidence === "string" && HEX64.test(o.catchupEvidence) &&
    Object.keys(o).length === 3;
}

// ---- the marker object ----------------------------------------------------------------------------------------------

const withTimeout = async <T>(p: Promise<T>, ms: number, code: string): Promise<T> => {
  let t: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([p, new Promise<never>((_, rej) => { t = setTimeout(() => rej(new MarkerRefusal(code)), ms); })]);
  } finally { if (t) clearTimeout(t); }
};

/** The org's marker: null when absent; `{doc}` (doc null when not JSON) when present. Fixed codes otherwise. */
export async function readMarkerObject(store: ObjectStore, keyRoot: string, org: string, timeoutMs = MARKER_S3_TIMEOUT_MS): Promise<{ doc: unknown } | null> {
  try {
    const bytes = await withTimeout((async () => readAll((await store.getObject(objectKey(orgFenceAuthorityKey(keyRoot, org)))).body))(), timeoutMs, "marker_read_unknown");
    try { return { doc: JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes)) }; } catch { return { doc: null }; }
  } catch (error) {
    if (error instanceof MarkerRefusal) throw error;
    if (isObjectStoreError(error, "not_found")) return null;
    return refuse(isAccessDenied(error) ? "marker_access_denied" : "marker_unreadable");
  }
}

/**
 * Whether an existing marker is THIS epoch's to adopt on a retry (header, "Retry"). `hold` must be the live hold at
 * the approved epoch; `otherAttempts` the count of other epochs of the org that recorded an attempt.
 */
export function markerAdoptable(doc: unknown, org: string, hold: LiveHold | null, approvedEpoch: string, otherAttempts: number, nowMs: number): boolean {
  if (!validOrgFenceAuthority(doc) || doc.orgId !== org) return false;
  if (!hold || hold.epoch !== approvedEpoch || hold.state !== "marker_attempted" || hold.attemptedMs === null) return false;
  if (!Number.isSafeInteger(otherAttempts) || otherAttempts !== 0) return false;
  return doc.writtenAtMs >= hold.attemptedMs - MARKER_CLOCK_SKEW_MS && doc.writtenAtMs <= nowMs + MARKER_CLOCK_SKEW_MS;
}

// ---- the run --------------------------------------------------------------------------------------------------------

export type MarkerOrgVerdict = "ready" | "refused" | "published" | "adopted" | "incomplete" | "not_in_wave";
export interface MarkerOrgResult {
  org_hash: string;
  epoch_hash: string | null;
  hold: "absent" | "released" | "held" | "sealed" | "marker_attempted" | "invalid" | "unreadable" | "not_read";
  sweeper: string;
  attest: Record<MarkerAttestation, boolean>;
  /** The marker object as found/left: absent, present (dry), written, adopted, conflict, unknown, not_read. */
  marker: "absent" | "present" | "written" | "adopted" | "conflict" | "unknown" | "not_read";
  attempted: boolean;
  verdict: MarkerOrgVerdict;
  refused: string | null;
}

/** Every per-org refusal code (closed: the receipt schema allows only these). */
export const MARKER_ORG_REFUSALS = [
  "hold_absent", "hold_released", "hold_not_sealed", "hold_epoch_changed", "hold_state_invalid", "hold_unreadable", "hold_epoch_stale",
  "hold_attempt_failed", "hold_released_before_put", "hold_lock_failed", "marker_guard_lost", "marker_hold_lapsed",
  "evidence_not_corroborated", "evidence_org_missing", "attestation_missing", "marker_handoff_lapsed", "marker_conflict", "marker_present",
  "marker_org", "marker_access_denied", "marker_unreadable", "marker_write_failed", "marker_readback_mismatch", "marker_put_unknown", "marker_read_unknown",
  "sweeper_spec_missing", "sweeper_spec_stale", "sweeper_spec_future", "sweeper_spec_invalid", "sweeper_spec_unreadable", "sweeper_spec_access_denied",
  "sweeper_suspended", "sweeper_home_mismatch", "sweeper_out_of_scope", "sweeper_invalid_request", "sweeper_missing", "sweeper_malformed",
  "sweeper_mismatch", "sweeper_stale", "sweeper_future", "sweeper_access_denied", "sweeper_unreadable", "not_in_wave", "marker_org_failed",
] as const;
const ORG_REFUSALS = new Set<string>(MARKER_ORG_REFUSALS);
export const MARKER_SWEEPER_CODES = ["ok", "not_run", ...MARKER_ORG_REFUSALS.filter((c) => c.startsWith("sweeper_"))] as const;

export interface MarkerRunDeps {
  mode: "dry_run" | "execute";
  /** The wave's scope orgs (validated, sorted by waveOrgList). */
  orgs: readonly string[];
  sha: string;
  scopeId: string;
  /** The bound runs (authority, backfill, full, reshard) the wave id covers. */
  runs: Readonly<Record<string, string>>;
  sources: AuthorityEvidenceSources;
  baseline: AuthorityBaseline;
  bound: MarkerBound;
  /** Execute only: the marker-dry's approved wave (its wave id and, per READY org, the epoch hash it recorded). */
  wave?: { id: string; orgs: readonly { orgHash: string; epochHash: string }[] };
  store: ObjectStore;
  keyRoot: string;
  bucket: string;
  ledgerBucket: string | null;
  /** A database session for one org at a time; at most `concurrency` are open at once. */
  withSession<T>(fn: (s: MarkerSqlSession) => Promise<T>): Promise<T>;
  now(): number;
  concurrency?: number;
  observationWindowMs?: number;
  /** Tests only. */
  evidence?: typeof authorityEvidence;
  s3TimeoutMs?: number;
}

const blankAttest = (): Record<MarkerAttestation, boolean> =>
  Object.fromEntries(MARKER_ATTESTATION_FIELDS.map((k) => [k, false])) as Record<MarkerAttestation, boolean>;

function holdCode(r: { hold: LiveHold | null; released: boolean }): MarkerOrgResult["hold"] {
  return r.hold ? r.hold.state : r.released ? "released" : "absent";
}

/** The SQLSTATE behind a pg error (or null). */
const sqlstate = (e: unknown) => { const c = (e as { code?: unknown } | null)?.code; return typeof c === "string" && /^[0-9A-Z]{5}$/u.test(c) ? c : null; };

/**
 * marker-dry (mode dry_run) or marker-execute (mode execute) over the wave. Never throws for one org: every org gets
 * its own verdict. Throws (a fixed code) only on wave-level input errors before any read.
 */
export async function markerRun(d: MarkerRunDeps) {
  const orgs = waveOrgList([...d.orgs]);
  if (!validMarkerBound(d.bound)) refuse("marker_bound_invalid");
  const execute = d.mode === "execute";
  if (execute !== (d.wave !== undefined)) refuse("marker_wave_invalid");
  if (d.wave && (!HASH.test(d.wave.id) || d.wave.orgs.length === 0 || d.wave.orgs.length > orgs.length ||
      !d.wave.orgs.every((o) => HASH.test(o.orgHash) && HASH.test(o.epochHash)) || new Set(d.wave.orgs.map((o) => o.orgHash)).size !== d.wave.orgs.length))
    refuse("marker_wave_invalid");
  const hashOf = new Map(orgs.map((o) => [o, evidenceHash("org", o)]));
  if (d.wave && !d.wave.orgs.every((w) => [...hashOf.values()].includes(w.orgHash))) refuse("marker_wave_invalid");
  // The approved wave pins the runs too: the execute recomputes its id over the same commit, scope, runs and orgs.
  if (d.wave && waveId({ sha: d.sha, scope: d.scopeId, runs: d.runs, orgs: d.wave.orgs }) !== d.wave.id) refuse("marker_wave_mismatch");
  const approved = new Map((d.wave?.orgs ?? []).map((o) => [o.orgHash, o.epochHash]));
  const reshard = new Set(d.bound.reshardVerified);

  // The bound evidence (corroboration) over the whole scope, as the baseline was taken.
  const report: AuthorityEvidenceReport = await (d.evidence ?? authorityEvidence)(orgs, d.sources,
    { bound: d.baseline, ...(d.observationWindowMs ? { observationWindowMs: d.observationWindowMs } : {}) });
  const verdictOf = new Map(report.orgs.map((o) => [o.orgHash, o]));

  // The serving sweeper's spec (wave-wide), then each org's fresh pass.
  let expectation: RearmPassExpectation | string;
  try { expectation = sweeperExpectation(await readServingSpec(d.store, d.keyRoot), { bucket: d.bucket, ledgerBucket: d.ledgerBucket, orgs }, d.now()); }
  catch (error) { expectation = error instanceof BacklogAuthorityRefusal ? error.code : "sweeper_spec_unreadable"; }

  const s3Timeout = d.s3TimeoutMs ?? MARKER_S3_TIMEOUT_MS;
  const settled = await runWave(orgs, d.concurrency ?? WAVE_DEFAULT_CONCURRENCY, async (org): Promise<MarkerOrgResult> => {
    const orgHash = hashOf.get(org)!;
    const r: MarkerOrgResult = { org_hash: orgHash, epoch_hash: null, hold: "not_read", sweeper: "not_run", attest: blankAttest(), marker: "not_read",
      attempted: false, verdict: "refused", refused: null };
    const stop = (code: string, verdict: MarkerOrgVerdict = r.attempted ? "incomplete" : "refused") => { r.refused = code; r.verdict = verdict; return r; };
    if (execute && !approved.has(orgHash)) return stop("not_in_wave", "not_in_wave");

    // Attestations that do not need the hold.
    const v = verdictOf.get(orgHash);
    r.attest.fence_backfill = d.bound.sections.fences;
    r.attest.ownership_backfill = d.bound.sections.ownership;
    r.attest.revision_backfill = d.bound.sections.revisions;
    r.attest.revision_layout = reshard.has(orgHash);
    r.attest.control_mirror = v?.mirror.ok === true;
    const evidenceOk = v !== undefined && v.ok === true && v.refusals.length === 0 && v.stop.ok && v.queueDrained.ok && v.sqlQuiet.ok && v.turnsNeon.ok;
    if (typeof expectation === "string") r.sweeper = expectation;
    else {
      try {
        const p = await freshSweeperPass(d.store, d.keyRoot, org, d.now(), BACKLOG_OPS_SWEEPER_PASS_MAX_AGE_MS, expectation);
        r.sweeper = p.ok ? "ok" : `sweeper_${p.reason}`;
      } catch (error) { r.sweeper = (error as { code?: string })?.code === "access_denied" ? "sweeper_access_denied" : "sweeper_unreadable"; }
    }
    r.attest.rearm_sweeper = r.sweeper === "ok";

    return d.withSession(async (sql) => {
      // 1. the hold.
      let live: { hold: LiveHold | null; released: boolean };
      try { live = parseLiveHold(await sql.read(MARKER_SQL.status(org).text, MARKER_SQL.status(org).params)); }
      catch (error) { r.hold = error instanceof MarkerRefusal ? "invalid" : "unreadable"; return stop(error instanceof MarkerRefusal ? error.code : "hold_unreadable"); }
      r.hold = holdCode(live);
      if (!live.hold) return stop(live.released ? "hold_released" : "hold_absent");
      r.epoch_hash = epochHash(live.hold.epoch);
      if (execute && approved.get(orgHash) !== r.epoch_hash) return stop("hold_epoch_changed");
      if (live.hold.state === "held") return stop("hold_not_sealed");
      // The hold enforces the Worker-side stop in SQL (admission refused RH042) on top of the observed stop.
      r.attest.worker_http_admission = evidenceOk;
      if (v === undefined) return stop("evidence_org_missing");
      if (typeof expectation === "string") return stop(expectation);
      if (r.sweeper !== "ok") return stop(ORG_REFUSALS.has(r.sweeper) ? r.sweeper : "sweeper_unreadable");
      if (!evidenceOk) return stop("evidence_not_corroborated");
      if (!MARKER_ATTESTATION_FIELDS.every((k) => r.attest[k])) return stop("attestation_missing");

      // 2. an existing marker must be this epoch's (only possible on a retry).
      const otherAttempts = async (q: (t: string, p: readonly unknown[]) => Promise<Rows>) => {
        const s = MARKER_SQL.otherAttempts(org, live.hold!.epoch);
        const n = (await q(s.text, s.params))[0]?.n;
        return typeof n === "number" && Number.isSafeInteger(n) ? n : -1;
      };
      let existing: { doc: unknown } | null;
      try { existing = await readMarkerObject(d.store, d.keyRoot, org, s3Timeout); }
      catch (error) { return stop(error instanceof MarkerRefusal ? error.code : "marker_unreadable"); }
      r.marker = existing ? "present" : "absent";
      if (existing) {
        let others: number;
        try { others = await otherAttempts(sql.read); } catch { return stop("hold_unreadable"); }
        if (!markerAdoptable(existing.doc, org, live.hold, live.hold.epoch, others, d.now())) return stop(execute ? "marker_conflict" : "marker_present");
      }
      if (!execute) { r.verdict = "ready"; return r; }

      // The bound evidence is still fresh at the point of no return.
      const age = d.now() - report.finishedAtMs;
      if (!(age >= 0 && age <= BACKLOG_OPS_AUTHORITY_REPORT_MAX_AGE_MS)) return stop("marker_handoff_lapsed");

      // 3. the marker attempt (committed before EVERY PUT attempt; idempotent at the same epoch).
      try {
        const a = MARKER_SQL.markerAttempt(org, live.hold.epoch, d.bound.catchupEvidence);
        await sql.write(a.text, a.params);
      } catch (error) {
        return stop(sqlstate(error) === "55000" ? "hold_epoch_stale" : "hold_attempt_failed");
      }
      r.attempted = true;

      // 4-6. the guarded publication.
      const epoch = live.hold.epoch;
      try {
        await sql.guarded(org, async (inTx) => {
          let now: { hold: LiveHold | null; released: boolean };
          try { now = parseLiveHold(await inTx(MARKER_SQL.status(org).text, MARKER_SQL.status(org).params)); }
          catch (error) { throw new MarkerRefusal(error instanceof MarkerRefusal ? error.code : "hold_unreadable"); }
          r.hold = holdCode(now);
          if (!now.hold) throw new MarkerRefusal(now.released ? "hold_released_before_put" : "hold_absent");
          if (now.hold.epoch !== epoch) throw new MarkerRefusal("hold_epoch_changed");
          if (now.hold.state !== "marker_attempted") throw new MarkerRefusal("hold_state_invalid");
          const found = await readMarkerObject(d.store, d.keyRoot, org, s3Timeout);
          if (found) {
            const others = await otherAttempts(inTx);
            if (!markerAdoptable(found.doc, org, now.hold, epoch, others, d.now())) { r.marker = "conflict"; throw new MarkerRefusal("marker_conflict"); }
            r.marker = "adopted";
          } else {
            let outcome: "written" | "conflict";
            try { outcome = await withTimeout(writeOrgMarker(d.store, d.keyRoot, org, d.now()), s3Timeout, "marker_put_unknown"); }
            catch (error) {
              if (error instanceof MarkerRefusal) { r.marker = "unknown"; throw error; }
              throw new MarkerRefusal(error instanceof BacklogAuthorityRefusal ? error.code : "marker_write_failed");
            }
            if (outcome === "conflict") {
              // Another writer between our GET and PUT: adopt only if it is this epoch's.
              const again = await readMarkerObject(d.store, d.keyRoot, org, s3Timeout);
              const others = await otherAttempts(inTx);
              if (!again || !markerAdoptable(again.doc, org, now.hold, epoch, others, d.now())) { r.marker = "conflict"; throw new MarkerRefusal("marker_conflict"); }
              r.marker = "adopted";
            } else r.marker = "written";
          }
          // 6. still live at the epoch AFTER the PUT, in the same transaction (the key is still held).
          const after = parseLiveHold(await inTx(MARKER_SQL.status(org).text, MARKER_SQL.status(org).params));
          if (!after.hold || after.hold.epoch !== epoch || after.hold.state !== "marker_attempted") throw new MarkerRefusal("marker_hold_lapsed");
        });
      } catch (error) {
        const code = error instanceof MarkerRefusal ? error.code : sqlstate(error) !== null ? "hold_lock_failed" : "marker_guard_lost";
        // A lost transaction after a PUT: the hold is re-read on a fresh statement; live at the epoch proves it held
        // throughout (a released hold never comes back).
        const left = r.marker as MarkerOrgResult["marker"];
        if ((code === "marker_guard_lost" || code === "hold_lock_failed") && (left === "written" || left === "adopted")) {
          try {
            const again = parseLiveHold(await sql.read(MARKER_SQL.status(org).text, MARKER_SQL.status(org).params));
            if (again.hold && again.hold.epoch === epoch && again.hold.state === "marker_attempted") {
              r.verdict = left === "written" ? "published" : "adopted";
              return r;
            }
            return stop("marker_hold_lapsed");
          } catch { return stop("marker_guard_lost"); }
        }
        return stop(ORG_REFUSALS.has(code) ? code : "marker_write_failed");
      }
      r.verdict = (r.marker as MarkerOrgResult["marker"]) === "written" ? "published" : "adopted";
      return r;
    });
  }, { allowed: ORG_REFUSALS, fallback: "marker_org_failed" });

  const per: MarkerOrgResult[] = settled.map((s) => s.ok ? s.value : {
    org_hash: hashOf.get(s.org)!, epoch_hash: null, hold: "not_read", sweeper: "not_run", attest: blankAttest(), marker: "not_read",
    attempted: false, verdict: "refused", refused: s.code });
  const n = (v: MarkerOrgVerdict) => per.filter((p) => p.verdict === v).length;
  const ready = per.filter((p) => p.verdict === "ready" && p.epoch_hash !== null);
  // The dry's wave: the READY orgs at their epochs (the execute's approval pins exactly these).
  const id = execute ? d.wave!.id : ready.length > 0
    ? waveId({ sha: d.sha, scope: d.scopeId, runs: d.runs, orgs: ready.map((p) => ({ orgHash: p.org_hash, epochHash: p.epoch_hash! })) }) : null;
  const done = n("published") + n("adopted"), inWave = execute ? d.wave!.orgs.length : orgs.length;
  const status = execute
    ? (done === inWave ? "marker_wave_published" : done > 0 || n("incomplete") > 0 ? "marker_wave_incomplete" : "marker_wave_refused")
    : (ready.length === orgs.length ? "marker_wave_ready" : ready.length > 0 ? "marker_wave_partial" : "marker_wave_refused");
  return {
    mode: d.mode, tool: "marker_publication" as const, status, wave_id: id, org_count: orgs.length, wave_org_count: execute ? inWave : ready.length,
    counts: { ready: n("ready"), refused: n("refused"), published: n("published"), adopted: n("adopted"), incomplete: n("incomplete"), not_in_wave: n("not_in_wave") },
    evidence: toReceiptShape(report), per_org: per,
  };
}
export type MarkerSummary = Awaited<ReturnType<typeof markerRun>>;

/** A dry succeeds when at least one org is ready; an execute only when every org of its wave is published or adopted. */
export const markerOk = (s: MarkerSummary) => s.mode === "dry_run" ? s.wave_id !== null : s.status === "marker_wave_published";

// ---- receipt schema --------------------------------------------------------------------------------------------------

const codes = (...c: readonly string[]): ReceiptSchema => ({ code: new Set(c) });
const nul = (s: ReceiptSchema): ReceiptSchema => ({ nullable: s });
export const MARKER_SUMMARY_SCHEMA: ReceiptSchema = {
  obj: {
    mode: codes("dry_run", "execute"), tool: codes("marker_publication"),
    status: codes("marker_wave_ready", "marker_wave_partial", "marker_wave_refused", "marker_wave_published", "marker_wave_incomplete"),
    wave_id: nul("hash"), org_count: "count", wave_org_count: "count",
    counts: { obj: { ready: "count", refused: "count", published: "count", adopted: "count", incomplete: "count", not_in_wave: "count" } },
    evidence: EVIDENCE_SCHEMA,
    per_org: { list: { obj: {
      org_hash: "hash", epoch_hash: nul("hash"),
      hold: codes("absent", "released", "held", "sealed", "marker_attempted", "invalid", "unreadable", "not_read"),
      sweeper: codes(...MARKER_SWEEPER_CODES),
      attest: { obj: Object.fromEntries(MARKER_ATTESTATION_FIELDS.map((k) => [k, "bool" as const])) },
      marker: codes("absent", "present", "written", "adopted", "conflict", "unknown", "not_read"),
      attempted: "bool",
      verdict: codes("ready", "refused", "published", "adopted", "incomplete", "not_in_wave"),
      refused: nul(codes(...MARKER_ORG_REFUSALS)),
    } }, max: 64 },
  },
};
/** The tool's own refusals (scripts/backlog-marker.ts), kept by the receipt; anything else becomes internal_error. */
export const MARKER_TOOL_REFUSALS = ["usage", "orgs", "marker_execute_held", "stdin_too_large", "stdin_invalid", "db_url_required", "cloudflare_token_invalid",
  "cloudflare_config", "cloudflare_path", "aws_credentials_required", "marker_bound_invalid", "marker_wave_invalid", "marker_wave_mismatch",
  "wave_orgs_empty", "wave_orgs_oversized", "wave_org_invalid", "wave_org_duplicate", "wave_concurrency_invalid", "wave_identity_invalid",
  "internal_error", "ledger_bucket_invalid", "ledger_home_guard_invalid", "ledger_home_mismatch", "ledger_home_missing", "ledger_home_moved",
  "ledger_home_invalid", "ledger_home_unavailable", "ledger_home_store_error", "access_denied"] as const;

/**
 * The summary a marker receipt may carry (applied by the credential step before the receipt is persisted or logged):
 * the tool's summary when it conforms exactly and is self-consistent for this run (its per-org hashes are exactly the
 * scope's, each once; the counts add up; the mode matches; an execute's wave id is the approved one); a refusal with a
 * code from MARKER_TOOL_REFUSALS; otherwise {"refused": "backlog_ops_receipt_schema"}.
 */
export function markerReceiptSummary(summary: unknown, ctx: { mode: "dry_run" | "execute"; orgs: readonly string[] | null; waveId: string | null }):
  { summary: unknown; ok: boolean } {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const o = summary as any;
  if (o && typeof o === "object" && !Array.isArray(o) && Object.keys(o).length === 1 && Object.hasOwn(o, "refused"))
    return { summary: { refused: (MARKER_TOOL_REFUSALS as readonly string[]).includes(o.refused) ? o.refused : "internal_error" }, ok: false };
  const bad = { summary: { refused: "backlog_ops_receipt_schema" }, ok: false };
  if (!conforms(summary, MARKER_SUMMARY_SCHEMA) || o.mode !== ctx.mode || !ctx.orgs || ctx.orgs.length === 0) return bad;
  const want = ctx.orgs.map((x) => evidenceHash("org", x)).sort(), got = (o.per_org as { org_hash: string }[]).map((p) => p.org_hash).sort();
  if (got.length !== want.length || !got.every((h, i) => h === want[i]) || o.org_count !== want.length) return bad;
  const per = o.per_org as MarkerOrgResult[];
  const sum = Object.values(o.counts as Record<string, number>).reduce((a, b) => a + b, 0);
  if (sum !== per.length || per.some((p) => (p.verdict === "ready" || p.verdict === "published" || p.verdict === "adopted") !== (p.refused === null)) ||
      per.some((p) => (p.verdict === "published" && p.marker !== "written") || (p.verdict === "adopted" && p.marker !== "adopted")) ||
      (ctx.mode === "dry_run" && per.some((p) => p.attempted || ["published", "adopted", "incomplete", "not_in_wave"].includes(p.verdict))) ||
      (ctx.mode === "execute" && (per.some((p) => p.verdict === "ready") || o.wave_id !== ctx.waveId)) ||
      (ctx.mode === "dry_run" && ((o.wave_id === null) !== (o.counts.ready === 0) || o.wave_org_count !== o.counts.ready)))
    return bad;
  const ok = ctx.mode === "dry_run" ? o.wave_id !== null : o.status === "marker_wave_published";
  return { summary, ok };
}

/** A marker-dry receipt bound by marker-execute: its approved wave (id and READY orgs with epoch hashes). */
export function checkMarkerDryReceipt(raw: string, orgs: readonly string[], runs: Readonly<Record<string, string>>, waveIdWanted: string) {
  let r: Record<string, any> | null;
  try { r = JSON.parse(raw); } catch { r = null; }
  const s = r?.summary;
  if (!r || !s || typeof s !== "object") refuse("backlog_ops_bound_marker_dry");
  if (!markerReceiptSummary(s, { mode: "dry_run", orgs, waveId: null }).ok) refuse("backlog_ops_bound_marker_dry");
  // The dry bound exactly the runs the execute binds (the receipt's top-level fields, written by the credential step).
  if (r!.authority_run !== runs.authority || r!.after_run !== runs.backfill || r!.full_run !== runs.full || r!.reshard_run !== runs.reshard)
    refuse("backlog_ops_bound_marker_runs");
  if (s.wave_id !== waveIdWanted) refuse("backlog_ops_bound_marker_wave");
  const wave = (s.per_org as MarkerOrgResult[]).filter((p) => p.verdict === "ready").map((p) => ({ orgHash: p.org_hash, epochHash: p.epoch_hash! }));
  return { id: s.wave_id as string, orgs: wave };
}

/** sha256 hex of exact bytes (the marker attempt's evidence: the final catch-up receipt as downloaded). */
export const receiptSha256 = (bytes: Uint8Array) => createHash("sha256").update(bytes).digest("hex");

/**
 * What a marker mode's bound receipts prove (the credential step calls this after checkBoundReceipt accepted each):
 * per section, complete with failed 0 in BOTH the FULL and the final catch-up; the org hashes the reshard-dry verified
 * (bound to exactly that catch-up, per_org exactly the scope's hashes; an org that did not verify is left out, so it
 * alone is refused); and the catch-up receipt's sha256 (the marker attempt's evidence).
 */
export function markerBoundFrom(fullRaw: string, catchupRaw: string, reshardRaw: string, orgs: readonly string[], afterRun: string, catchupEvidence: string): MarkerBound {
  const parse = (raw: string, code: string) => { try { const r = JSON.parse(raw); if (r && typeof r === "object" && r.summary && typeof r.summary === "object") return r; } catch { /* below */ } return refuse(code); };
  const full = parse(fullRaw, "backlog_ops_bound_full_receipt").summary, cu = parse(catchupRaw, "backlog_ops_bound_backfill_receipt").summary;
  const done = (s: Record<string, any>, k: string) => s.sections?.[k] === "complete" && s[k]?.failed === 0;
  const sections = { fences: done(full, "fences") && done(cu, "fences"), ownership: done(full, "ownership") && done(cu, "ownership"),
    revisions: done(full, "revisions") && done(cu, "revisions") };
  const rr = parse(reshardRaw, "backlog_ops_reshard_not_verified");
  if (rr.after_run !== afterRun) refuse("backlog_ops_reshard_not_after");
  const rs = rr.summary, want = orgs.map((o) => evidenceHash("org", o)).sort();
  const per = Array.isArray(rs.per_org) ? rs.per_org as Record<string, unknown>[] : null;
  const got = per?.map((p) => p?.org_hash).sort();
  if (rs.mode !== "dry_run" || rs.tool !== "reshard_verify" || !per || rs.org_count !== want.length || got!.length !== want.length || !got!.every((h, i) => h === want[i]))
    refuse("backlog_ops_reshard_not_verified");
  const reshardVerified = per!.filter((p) => p.verify_passed === true && p.missing === 0 && p.invalid === 0 && p.failed === 0).map((p) => p.org_hash as string);
  if (!HEX64.test(catchupEvidence)) refuse("backlog_ops_bound_backfill_receipt");
  return { sections, reshardVerified, catchupEvidence };
}
