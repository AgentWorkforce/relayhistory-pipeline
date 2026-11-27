/**
 * The bridge side of the per-org admission hold (docs/design/org-admission-hold.md, revision 4 at 892f089e, sections 6.1
 * and 6.2; slot 20261105062320): what backlog-ops does between a hold's marker attempt and its release. CODE ONLY
 * and DORMANT: no workflow mode calls it yet, and nothing here applies a migration or writes a marker.
 *
 *   1. Lifecycle adapter (holdSql): the four owner-only lifecycle functions of slot 062320, behind one table of names
 *      and parameter shapes, so a rename is one edit. The bridge itself only reads the hold's state and calls
 *      `_release`. Install, seal and marker attempt belong to the authority steps.
 *   2. Erasure set (ERASURE_SET_SQL): every operator user erase and operator session erase of the org recorded at or
 *      after FULL.created_at minus the margin. Read-only, and re-read until a read finds nothing new.
 *   3. Replay (replayErasures): each erasure's S3-ledger equivalent, idempotent, with a create-only receipt keyed by
 *      the erasure's request id (hashed) under the hold's epoch:
 *        user erase     resetUploadedSessions(tenant, request id), then eraseLogPayloads(tenant)
 *        session erase  for every target of the session's COMPLETE erase manifest (every alias x owner x origin, the
 *                       turns origin included), the fence deletion setSessionUploads({..., enabled: false}). Coverage
 *                       must be proven complete: S3 owner stubs alone can be partial (the SQL erase unions owners
 *                       from ownership, index and directory rows over the alias closure), so the erase-lane review
 *                       (layered-sessions-pipeline) requires a durable target manifest captured under the erase lock
 *                       before destructive cleanup. No such manifest exists yet, so every session erase replays to
 *                       session_manifest_missing (no receipt), which blocks bridge_complete. Fail closed.
 *        user erase     only once the SQL erase reached bytes_verified (the artifact lane's own byte receipt, a
 *                       separate prerequisite): erasure_bytes_unverified otherwise, no receipt.
 *   4. bridge_complete gate (bridgeCompleteVerdict): every erasure in the set, re-read just before the release, has a
 *      receipt for this epoch. Otherwise the release is refused. After any marker attempt the only release is
 *      `bridge_complete` (releaseAllowed): there is no rollback after an attempt.
 *   5. Waves (waveOrgs): an org whose hold is marker_attempted and not released is stuck and never joins a new wave.
 *   6. Monitoring (holdAlarms): a hold sealed for longer than HOLD_SEAL_MAX_MS (60 min: publication for the org is
 *      refused while sealed, so a long seal stalls its search), or marker_attempted for longer than HOLD_ATTEMPT_MAX_MS,
 *      raises an alarm naming its owner, which is configuration (holdAlarmOwner: the environment variable
 *      BACKLOG_OPS_HOLD_ALARM_OWNER, an Agent Relay name), never a code constant. Counts, codes and hashes only.
 */
import { createHash } from "node:crypto";
import { isObjectStoreError, objectKey, readAll, type ObjectStore } from "../shared/vendor.ts";
import type { LedgerTenant, SelectionInput } from "./ledger.ts";

// ---- 1. lifecycle adapter -------------------------------------------------------------------------------------------

/**
 * Slot 062320's lifecycle functions: review/org-admission-hold-062320 @ 1c95258e plus the final signatures confirmed
 * by sessions-authority-evidence (release gains p_erasures; a read-only status function). Owner-only, READ COMMITTED.
 */
export const HOLD_FUNCTIONS = {
  status: "sessions.ingest_org_admission_hold_status",
  install: "sessions.ingest_org_admission_hold_install",
  seal: "sessions.ingest_org_admission_hold_seal",
  markerAttempt: "sessions.ingest_org_admission_hold_marker_attempt",
  release: "sessions.ingest_org_admission_hold_release",
} as const;
export const HOLD_STATES = ["held", "sealed", "marker_attempted", "released"] as const;
export type HoldState = (typeof HOLD_STATES)[number];
export type HoldReleaseReason = "bridge_complete" | "verified_rollback";
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/u;
const ORG = UUID;
const HEX64 = /^[0-9a-f]{64}$/u;

export class HoldBridgeRefusal extends Error {
  constructor(readonly code: string) { super(code); this.name = "HoldBridgeRefusal"; }
}
const refuse = (code: string): never => { throw new HoldBridgeRefusal(code); };

/** The statement text and parameters of each lifecycle call. Every call runs alone in a READ COMMITTED transaction. */
export const holdSql = {
  install: (org: string, requestId: string) => {
    if (!ORG.test(org) || !UUID.test(requestId)) refuse("hold_input");
    return { text: `SELECT ${HOLD_FUNCTIONS.install}($1::text, $2::uuid)::text AS epoch`, params: [org, requestId] };
  },
  seal: (org: string, epoch: string, evidenceSha256: string) => {
    if (!ORG.test(org) || !UUID.test(epoch) || !HEX64.test(evidenceSha256)) refuse("hold_input");
    return { text: `SELECT ${HOLD_FUNCTIONS.seal}($1::text, $2::uuid, $3::text)`, params: [org, epoch, evidenceSha256] };
  },
  markerAttempt: (org: string, epoch: string, evidenceSha256: string) => {
    if (!ORG.test(org) || !UUID.test(epoch) || !HEX64.test(evidenceSha256)) refuse("hold_input");
    return { text: `SELECT ${HOLD_FUNCTIONS.markerAttempt}($1::text, $2::uuid, $3::text)`, params: [org, epoch, evidenceSha256] };
  },
  /**
   * `erasures`: for bridge_complete, the org's erasure record count (operator_user_erasures plus
   * session_erasure_fences) the replay set was built against; the function recomputes it under the exclusive key and
   * refuses (55000) when an erasure committed since. Ignored for verified_rollback.
   */
  release: (org: string, epoch: string, reason: HoldReleaseReason, evidenceSha256: string, detail: string | null, erasures: number) => {
    if (!ORG.test(org) || !UUID.test(epoch) || !HEX64.test(evidenceSha256) || (reason !== "bridge_complete" && reason !== "verified_rollback") ||
        (detail !== null && !/^[a-z][a-z0-9_]{0,63}$/u.test(detail)) || !Number.isSafeInteger(erasures) || erasures < 0) refuse("hold_input");
    return { text: `SELECT ${HOLD_FUNCTIONS.release}($1::text, $2::uuid, $3::text, $4::text, $5::text, $6::bigint)`,
      params: [org, epoch, reason, evidenceSha256, detail, String(erasures)] };
  },
  /** Read-only: {hold: the live (or latest released) row, erasures: the org's erasure record count}. */
  status: (org: string) => {
    if (!ORG.test(org)) refuse("hold_input");
    return { text: `SELECT ${HOLD_FUNCTIONS.status}($1::text) AS status`, params: [org] };
  },
} as const;

export interface Hold { epoch: string; state: Exclude<HoldState, "released">; installedMs: number; sealedMs: number | null; attemptedMs: number | null; attemptEvent: boolean }
export interface HoldStatus { hold: Hold | null; erasures: number }
const tsMs = (v: unknown): number | null => {
  if (v === null || v === undefined) return null;
  if (typeof v === "number") return Number.isSafeInteger(v) && v > 0 ? v : NaN;
  if (typeof v !== "string") return NaN;
  const t = Date.parse(v);
  return Number.isFinite(t) && t > 0 ? t : NaN;
};
/**
 * holdSql.status's one row: {status: {hold: {epoch, state, installed_at, sealed_at, marker_attempted_at, ...} | null,
 * erasures}}. A released hold is no live hold (null). Shape-checked; the timestamps' order follows the state machine.
 */
export function parseHoldStatus(rows: readonly Record<string, unknown>[]): HoldStatus {
  const st = rows.length === 1 ? rows[0]!.status : undefined;
  const o = typeof st === "string" ? (() => { try { return JSON.parse(st); } catch { return undefined; } })() : st;
  if (!o || typeof o !== "object" || Array.isArray(o)) return refuse("hold_state_invalid");
  const erasures = (o as Record<string, unknown>).erasures, h = (o as Record<string, unknown>).hold as Record<string, unknown> | null | undefined;
  if (!Number.isSafeInteger(erasures) || (erasures as number) < 0 || h === undefined) return refuse("hold_state_invalid");
  if (h === null || h.state === "released") return { hold: null, erasures: erasures as number };
  if (typeof h !== "object" || typeof h.epoch !== "string" || !UUID.test(h.epoch) || !["held", "sealed", "marker_attempted"].includes(h.state as string)) refuse("hold_state_invalid");
  const installedMs = tsMs(h.installed_at), sealedMs = tsMs(h.sealed_at), attemptedMs = tsMs(h.marker_attempted_at);
  if (installedMs === null || [installedMs, sealedMs, attemptedMs].some((x) => Number.isNaN(x)) ||
      (h.state === "held" && (sealedMs !== null || attemptedMs !== null)) || (h.state === "sealed" && (sealedMs === null || attemptedMs !== null)) ||
      (h.state === "marker_attempted" && (sealedMs === null || attemptedMs === null)))
    refuse("hold_state_invalid");
  // An attempt is recorded by its timestamp (the state machine never clears it): the point of no return.
  return { hold: { epoch: h.epoch as string, state: h.state as Hold["state"], installedMs: installedMs!, sealedMs, attemptedMs, attemptEvent: attemptedMs !== null },
    erasures: erasures as number };
}

/** Section 6.1: from marker_attempted (or with any attempt event) only bridge_complete; verified_rollback only before. */
export function releaseAllowed(hold: Hold, reason: HoldReleaseReason): boolean {
  const attempted = hold.state === "marker_attempted" || hold.attemptEvent;
  return attempted ? reason === "bridge_complete" : reason === "verified_rollback";
}

// ---- 2. the erasure set ---------------------------------------------------------------------------------------------

/**
 * Every operator erase of the org recorded at or after $2 (epoch ms: FULL.created_at minus the margin), oldest first.
 * Read-only. User erases from sessions.operator_user_erasures (058010, keyed by request_id), session erases from
 * sessions.session_erasure_fences (050000: one row per erased session, the table _release counts; keyed by the
 * session's identity, since a fence carries no request id). Ids never leave this process: receipts carry hashes.
 */
export const ERASURE_SET_SQL = `
SELECT 'user' AS kind, u.request_id, u.workspace_id, u.user_id, NULL::text AS source, NULL::text AS session_id,
       floor(extract(epoch FROM u.created_at) * 1000)::bigint AS at_ms, u.state = 'bytes_verified' AS bytes_verified
FROM sessions.operator_user_erasures u WHERE u.org_id = $1::text AND u.created_at >= to_timestamp($2::bigint / 1000.0)
UNION ALL
SELECT 'session', NULL, f.workspace_id, NULL, f.source, f.session_id, floor(extract(epoch FROM f.created_at) * 1000)::bigint, NULL::boolean
FROM sessions.session_erasure_fences f WHERE f.org_id = $1::text AND f.created_at >= to_timestamp($2::bigint / 1000.0)
ORDER BY at_ms, request_id`;

export type Erasure =
  | { kind: "user"; requestId: string; workspaceId: string; userId: string; atMs: number; bytesVerified: boolean }
  | { kind: "session"; requestId: string; workspaceId: string; source: string; sessionId: string; atMs: number };
const str = (v: unknown): v is string => typeof v === "string" && v.length > 0 && v.length <= 512;
export function parseErasures(rows: readonly Record<string, unknown>[]): Erasure[] {
  return rows.map((r) => {
    const atMs = tsMs(typeof r.at_ms === "string" && /^[0-9]{1,16}$/u.test(r.at_ms) ? Number(r.at_ms) : r.at_ms);
    if (!str(r.workspace_id) || atMs === null || Number.isNaN(atMs)) refuse("erasure_set_invalid");
    if (r.kind === "user" && str(r.request_id) && str(r.user_id) && typeof r.bytes_verified === "boolean")
      return { kind: "user", requestId: r.request_id, workspaceId: r.workspace_id as string, userId: r.user_id, atMs: atMs!, bytesVerified: r.bytes_verified };
    if (r.kind === "session" && r.request_id === null && str(r.source) && str(r.session_id))
      return { kind: "session", requestId: `session\n${r.workspace_id}\n${r.source}\n${r.session_id}`, workspaceId: r.workspace_id as string,
        source: r.source, sessionId: r.session_id, atMs: atMs! };
    return refuse("erasure_set_invalid");
  });
}
export const requestHash = (requestId: string) => "h_" + createHash("sha256").update(`hold-bridge/v1\nrequest\n${requestId}`).digest("hex").slice(0, 32);

// ---- 3. replay -------------------------------------------------------------------------------------------------------

/** <root>/.ledger/hold-bridge/<org>/<epoch>/erasures/<request hash>.json, create-only. */
export const erasureReceiptKey = (keyRoot: string, org: string, epoch: string, requestId: string) =>
  `${keyRoot}/.ledger/hold-bridge/${org}/${epoch}/erasures/${requestHash(requestId)}.json`;
export interface ErasureReceipt {
  v: 1; kind: "hold-bridge-erasure"; epoch: string; request: string; erasure: "user" | "session";
  /** user: sessions reset by the fence reset, log payloads deleted. session: owners fenced. */
  counts: { reset: number; payloads: number; owners: number };
  appliedAtMs: number;
}
export function validErasureReceipt(v: unknown, epoch: string, requestId: string): v is ErasureReceipt {
  if (!v || typeof v !== "object" || Array.isArray(v)) return false;
  const r = v as Record<string, any>, c = r.counts;
  const n = (x: unknown) => Number.isSafeInteger(x) && (x as number) >= 0;
  return Object.keys(r).length === 7 && r.v === 1 && r.kind === "hold-bridge-erasure" && r.epoch === epoch && r.request === requestHash(requestId)
    && (r.erasure === "user" || r.erasure === "session") && c && typeof c === "object" && Object.keys(c).length === 3 && n(c.reset) && n(c.payloads)
    && n(c.owners) && Number.isSafeInteger(r.appliedAtMs) && r.appliedAtMs > 0 && (r.erasure !== "session" || c.owners > 0);
}

export interface ReplayDeps {
  store: ObjectStore;
  keyRoot: string;
  org: string;
  epoch: string;
  /** ledger-s3.ts s3Ledger(...).resetUploadedSessions: the user's S3 fence reset (idempotent per request id). */
  resetUploadedSessions(tenant: LedgerTenant, requestId: string): Promise<number>;
  /** ledger-log-erase.ts eraseLogPayloads for the user. */
  eraseLogPayloads(tenant: LedgerTenant): Promise<number>;
  /** ledger-s3.ts s3Ledger(...).setSessionUploads: the session's S3 fence deletion for one owner. */
  setSessionUploads(input: SelectionInput): Promise<void>;
  /**
   * The session's erase targets from a COMPLETE manifest: `complete` true only when the source proves full coverage
   * (a durable manifest captured under the erase lock). Absent: no source can prove it yet, and every session erase
   * replays to session_manifest_missing.
   */
  resolveSessionTargets?(org: string, workspaceId: string, source: string, sessionId: string):
    Promise<{ complete: boolean; targets: { userId: string; originId: string; source: string; sessionId: string }[] }>;
  now(): number;
}

export type ReplayOutcome = "replayed" | "already" | "session_manifest_missing" | "session_coverage_partial" | "erasure_bytes_unverified" | "replay_failed";
async function readReceipt(store: ObjectStore, key: string): Promise<unknown | null> {
  try { return JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(await readAll((await store.getObject(objectKey(key))).body))); }
  catch (error) { if (isObjectStoreError(error, "not_found")) return null; return undefined; }
}

/**
 * One erasure's replay. Already receipted for this epoch: `already` (no effect re-applied). Otherwise apply its S3
 * equivalent (each step idempotent), then write the receipt create-only; a receipt that a concurrent replay wrote first
 * counts as `already` when valid. Failures are codes only (`replay_failed`), and the erasure stays unreceipted.
 */
export async function replayErasure(d: ReplayDeps, e: Erasure): Promise<ReplayOutcome> {
  if (!ORG.test(d.org) || !UUID.test(d.epoch)) refuse("hold_input");
  const key = erasureReceiptKey(d.keyRoot, d.org, d.epoch, e.requestId);
  const existing = await readReceipt(d.store, key);
  if (existing !== null) return validErasureReceipt(existing, d.epoch, e.requestId) ? "already" : "replay_failed";
  const counts = { reset: 0, payloads: 0, owners: 0 };
  try {
    if (e.kind === "user") {
      if (!e.bytesVerified) return "erasure_bytes_unverified";
      const tenant = { orgId: d.org, workspaceId: e.workspaceId, userId: e.userId };
      counts.reset = await d.resetUploadedSessions(tenant, e.requestId);
      counts.payloads = await d.eraseLogPayloads(tenant);
    } else {
      if (!d.resolveSessionTargets) return "session_manifest_missing";
      const m = await d.resolveSessionTargets(d.org, e.workspaceId, e.source, e.sessionId);
      if (!m.complete || m.targets.length === 0) return "session_coverage_partial";
      // Every alias of the session for every owner at every recorded origin; only this session's pairs.
      for (const t of m.targets)
        await d.setSessionUploads({ tenant: { orgId: d.org, workspaceId: e.workspaceId, userId: t.userId }, originId: t.originId, sessions: [[t.source, t.sessionId]], enabled: false });
      counts.owners = m.targets.length;
    }
  } catch { return "replay_failed"; }
  const receipt: ErasureReceipt = { v: 1, kind: "hold-bridge-erasure", epoch: d.epoch, request: requestHash(e.requestId), erasure: e.kind, counts, appliedAtMs: d.now() };
  try {
    await d.store.put(objectKey(key), new TextEncoder().encode(JSON.stringify(receipt)), { contentType: "application/json", ifNoneMatch: true });
  } catch (error) {
    if (isObjectStoreError(error, "precondition_failed") || isObjectStoreError(error, "already_exists"))
      return validErasureReceipt(await readReceipt(d.store, key), d.epoch, e.requestId) ? "already" : "replay_failed";
    return "replay_failed";
  }
  return "replayed";
}

/** The most re-reads of the erasure set before giving up (a set that keeps growing: settled false). */
export const ERASURE_REREAD_LIMIT = 8;
/**
 * Replay the org's erasure set until it is settled: the org's erasure record count (holdSql.status) is the same before
 * and after a pass and the pass found nothing new. `readCount` reads the count, `readSet` runs ERASURE_SET_SQL (both
 * read-only). Returns counts by outcome and the settled count, which bridge_complete's `_release` re-checks under
 * the exclusive key; the set is never logged.
 */
export async function replayErasures(d: ReplayDeps, readCount: () => Promise<number>, readSet: () => Promise<Erasure[]>) {
  const out: Record<ReplayOutcome, number> = { replayed: 0, already: 0, session_manifest_missing: 0, session_coverage_partial: 0, erasure_bytes_unverified: 0, replay_failed: 0 };
  const done = new Set<string>();
  for (let i = 0; i < ERASURE_REREAD_LIMIT; i++) {
    const before = await readCount();
    const fresh = (await readSet()).filter((e) => !done.has(e.requestId));
    for (const e of fresh) { const o = await replayErasure(d, e); out[o]++; done.add(e.requestId); }
    const after = await readCount();
    if (fresh.length === 0 && before === after) return { settled: true, rereads: i + 1, erasureCount: after, ...out };
  }
  return { settled: false, rereads: ERASURE_REREAD_LIMIT, erasureCount: null, ...out };
}

// ---- 4. the bridge_complete gate ------------------------------------------------------------------------------------

/**
 * May bridge_complete be released? Only with the hold marker_attempted at `epoch`, and every erasure in `set` (re-read
 * immediately before the release) receipted for that epoch. Returns a code: ok, or the first reason it may not.
 */
export async function bridgeCompleteVerdict(store: ObjectStore, keyRoot: string, org: string, hold: Hold | null, epoch: string, set: readonly Erasure[]):
  Promise<{ code: "ok" | "hold_missing" | "hold_epoch_mismatch" | "hold_not_attempted" | "erasure_unreplayed"; erasures: number; receipted: number }> {
  const base = { erasures: set.length, receipted: 0 };
  if (hold === null) return { code: "hold_missing", ...base };
  if (hold.epoch !== epoch) return { code: "hold_epoch_mismatch", ...base };
  if (hold.state !== "marker_attempted") return { code: "hold_not_attempted", ...base };
  let receipted = 0;
  for (const e of set) if (validErasureReceipt(await readReceipt(store, erasureReceiptKey(keyRoot, org, epoch, e.requestId)), epoch, e.requestId)) receipted++;
  return { code: receipted === set.length ? "ok" : "erasure_unreplayed", erasures: set.length, receipted };
}

// ---- 5. waves and 6. monitoring -----------------------------------------------------------------------------------

/** An org with a live hold in marker_attempted (or with an attempt event) is stuck: never part of a new wave. */
export function waveOrgs(orgs: readonly string[], holds: ReadonlyMap<string, Hold | null>): { wave: string[]; stuck: number } {
  const wave: string[] = [];
  let stuck = 0;
  for (const o of orgs) {
    const h = holds.get(o);
    if (h === undefined) refuse("hold_state_missing");
    if (h && (h.state === "marker_attempted" || h.attemptEvent)) stuck++;
    else wave.push(o);
  }
  return { wave, stuck };
}

/** A hold sealed this long without an attempt, or attempted this long without bridge_complete, alarms. */
export const HOLD_SEAL_MAX_MS = 60 * 60 * 1000;
export const HOLD_ATTEMPT_MAX_MS = 24 * 60 * 60 * 1000;
/** The configuration variable naming who an alarm pages (an Agent Relay agent name; for this rollout, b1). */
export const HOLD_ALARM_OWNER_VAR = "BACKLOG_OPS_HOLD_ALARM_OWNER";
const OWNER_NAME = /^[a-z0-9][a-z0-9-]{1,62}[a-z0-9]$/u;
/** The alarm owner from configuration: required, an Agent Relay name; missing or malformed refuses. */
export function holdAlarmOwner(env: Record<string, string | undefined>): string {
  const v = (env[HOLD_ALARM_OWNER_VAR] ?? "").trim();
  if (!OWNER_NAME.test(v)) refuse("hold_alarm_owner");
  return v;
}
export type HoldAlarm = { alarm: "hold_seal_overdue" | "hold_attempt_overdue"; owner: string; ageMs: number; org: string };
export function holdAlarms(holds: ReadonlyMap<string, Hold | null>, nowMs: number, owner: string): HoldAlarm[] {
  if (!OWNER_NAME.test(owner)) refuse("hold_alarm_owner");
  const out: HoldAlarm[] = [];
  for (const [org, h] of holds) {
    if (!h) continue;
    if (h.state === "sealed" && nowMs - h.sealedMs! > HOLD_SEAL_MAX_MS) out.push({ alarm: "hold_seal_overdue", owner, ageMs: nowMs - h.sealedMs!, org: orgHash(org) });
    if (h.state === "marker_attempted" && nowMs - h.attemptedMs! > HOLD_ATTEMPT_MAX_MS)
      out.push({ alarm: "hold_attempt_overdue", owner, ageMs: nowMs - h.attemptedMs!, org: orgHash(org) });
  }
  return out;
}
const orgHash = (org: string) => "h_" + createHash("sha256").update(`authority-evidence/v1\norg\n${org}`).digest("hex").slice(0, 32);
