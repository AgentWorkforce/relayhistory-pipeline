/**
 * CORROBORATING EVIDENCE ONLY, NEVER MARKER AUTHORITY (layered-sessions-pipeline's decision, message
 * 251933101733961728). Read-only observation cannot establish that no admission lands between its last read and a
 * marker write, so nothing here authorises writing a per-org fence-authority marker (ledger-s3.ts
 * validOrgFenceAuthority), and backlog-ops authority-execute stays dormant. Marker authority will bind to the enforced
 * per-org admission hold, a separate migration track, not to this report. What this module reports is live
 * corroboration of decision 0005's cutover preconditions for an org:
 * Decision 0005's cutover order (docs/selfhost-composition.md, "Stopping the Worker's HTTP admission for an org"):
 *
 *   1. stop          every traffic-serving version of 96bulls-session-ingest lists the org in
 *                    INGEST_HTTP_ADMISSION_STOPPED_ORGS, continuously back to a PROVEN boundary: the walk back
 *                    through the deployments must reach a deployment whose serving versions are all readable and of
 *                    which at least one does not list the org. The deployment just after it established the stop; its
 *                    created_on is the effective stop. An unreadable version, an exhausted or bounded history, or any
 *                    other unproven boundary refuses (stop_boundary_unproven): a later anchor would hide a claim.
 *   2. mirror        every current serving version also carries INGEST_S3_FENCE_MIRROR=on (the marker's controlMirror).
 *   3. queueDrained  routing first. Every serving version of every deployment since the stop, and of the boundary
 *                    deployment just before it, binds INGEST_ADMISSION to exactly the measured queue (a route change at
 *                    the stop is unproven drainage: queue_route_changed), and no queue in the account but the admission
 *                    and fan-out queues has the ingest Worker as a consumer. The measured queue's only consumer is the
 *                    ingest Worker dead-lettering to the measured DLQ, its producers are all the ingest Worker, and the
 *                    DLQ has no consumer. Every inventory is complete (the arrays equal the provider's
 *                    consumers_total_count / producers_total_count). Then backlog_count is 0 (global: a message is
 *                    opaque, never per org) at two reads at least the observation window apart, MEASURED on the injected
 *                    clock, with the same queue ids. The DLQ backlog not falling is auxiliary only (a replay offset by
 *                    arrivals hides in it). A replay that was admitted claims a batch row, which (4) can show only if
 *                    the row still exists when a SQL sample is taken; nothing here excludes one between samples.
 *   4. sqlQuiet      one read-only statement per org (READ ONLY is checked: transaction_read_only), whose first CTE
 *                    proves the reader sees every row of the three tables (SELECT and no RLS filtering, in the same
 *                    role, statement and snapshot as the counts). No ingest_batches row claimed or committed, and no
 *                    ingest_record_revisions row updated, at or after the effective stop minus a clock-skew margin; no
 *                    pending batch; no row of the org in any of the three tables possibly written after the base
 *                    snapshot (xmin at or above its xmin: round 1's own snapshot for round 2, the bound baseline's for
 *                    both); counts and digests equal across rounds and to the baseline. All of this is as of the two
 *                    SQL samples (round 1 and round 2, one statement each), not between or after them: a row
 *                    inserted, deleted or truncated away between samples, or written after round 2, is not seen.
 *                    Separately, ingest_batches' n_tup_del (AUTHORITY_STATS_SQL) is read before round 1 (or taken
 *                    from the baseline) and again AUTHORITY_STATS_SETTLE_MS after round 2, and any change refuses;
 *                    its limits are stated at AUTHORITY_STATS_SQL.
 *   5. turnsNeon     every traffic-serving version of relayhistory-api resolves the org's turns write path to `neon`
 *                    with relayhistory-cloud's resolver (packages/relayhistory/src/lib/turns-forward.ts
 *                    turnsWritePath, mirrored exactly by resolveTurnsWritePath). An absent binding on a verified
 *                    version is the documented default (neon); a binding whose value cannot be read (a secret, a
 *                    duplicate, a non-text type) is UNKNOWN and refuses.
 *
 * Bound baseline. Endpoint fingerprints cannot show continuity: a row deleted and restored between two reads leaves
 * every count and digest equal. Row versions can: every insert, update or delete-and-restore writes a tuple with a new
 * xmin. authorityBaseline records a snapshot (with counts and digests) taken INSIDE the backfill's input snapshot (a
 * REPEATABLE READ transaction, checked); the bound authorityEvidence run then counts the org's rows NOT visible in
 * that snapshot. For ingest_record_revisions and ingest_session_ownership (the only tables whose counts and digests
 * are fingerprinted), zero such rows and equal counts mean the org's row set AT EACH SAMPLE equals the baseline's set.
 * ingest_batches is NOT fingerprinted: for it the samples check only that no row of the org was claimed or committed
 * at or after the stop anchor, none is pending, and none was written after the base snapshot; a batch row removed
 * (for example truncated) is not detected by them. Nothing here covers changes undone between samples or made after
 * the last one. An unbound run is at best authority_evidence_unbound (sole refusal sql_unbound), the input
 * authorityBaseline takes; only a bound run can be authority_evidence_corroborated. Order: unbound run (after the stop
 * and the drain) -> backfills with authorityBaseline in their snapshot -> bound run (corroboration).
 * Authenticating the baseline's issuer and run, its exact revision and scope, and the backfill's ordering and
 * freshness is the caller's gate, as for every other bound backlog-ops receipt.
 *
 * No marker handoff: the module exports nothing that authorises a marker, and its best status,
 * authority_evidence_corroborated, is deliberately not named "pass". Every Cloudflare read takes the whole API body
 * (success checked; the queue list's result_info proves it is one complete page).
 *
 * Nothing here comes from an input except the org list (the caller's BACKLOG_OPS_ORGS scope, validated) and the bound
 * baseline (a receipt the caller's gate binds, as backlog-ops binds every other run). The module is pure: it builds every
 * Cloudflare path and SQL text itself and takes only transports (AuthorityEvidenceSources), so any response shape it
 * does not recognise refuses with a fixed code; a guess about a provider shape costs a refusal, never a false pass.
 * Fetcher errors are mapped to fixed codes, their messages discarded.
 *
 * Output is counts, booleans, epoch milliseconds, snapshot xids, fixed codes, row digests (`h` + 32 hex) and salted id
 * hashes (`h_` + 32 hex) only: no org, deployment, version or queue id and no raw configuration value, so it fits a backlog-ops receipt.
 *
 * Fail-closed limits: the anchor is strict (a claim at or after the stop, e.g. during propagation, refuses for that
 * stop for good); the walk is bounded (a stop established before the retained history refuses); a user control during
 * the window, or between baseline and bound run, writes a row and refuses (retryable with a new baseline); backlog_count
 * is documented as "messages currently in the queue", whether leased messages count is not, and (4) covers them.
 */
import { createHash } from "node:crypto";
import { ADMISSION_DLQ, ADMISSION_QUEUE, FANOUT_QUEUE } from "../../stacks/cloudflare/session-ingest.ts";
import { PRIVATE_INGEST_RELEASE, liveCutover, type CloudflareWorkerVersion } from "./private-ingest-release.ts";

export const AUTHORITY_EVIDENCE = {
  accountId: PRIVATE_INGEST_RELEASE.accountId,
  ingestWorker: PRIVATE_INGEST_RELEASE.worker,
  relayhistoryWorker: PRIVATE_INGEST_RELEASE.relayhistoryWorker,
  admissionQueue: ADMISSION_QUEUE,
  admissionDlq: ADMISSION_DLQ,
  fanoutQueue: FANOUT_QUEUE,
  admissionBinding: "INGEST_ADMISSION",
} as const;

/** The most deployments walked back to find the stop's boundary. */
export const AUTHORITY_MAX_DEPLOYMENT_WALK = 25;
/** The observation window between the two queue/SQL reads: default and the floor a caller may not go below. */
export const AUTHORITY_OBSERVATION_WINDOW_MS = 120_000;
export const AUTHORITY_MIN_OBSERVATION_WINDOW_MS = 30_000;
/**
 * The SQL anchor is the effective stop minus this: Cloudflare's created_on and the database's now() are different
 * clocks, and a claim stamped by a database clock running behind must still fall inside the window. Stricter, never
 * looser: a claim up to this long before the stop also refuses.
 */
export const AUTHORITY_CLOCK_SKEW_MS = 60_000;
/**
 * The wait after round 2 before the deletion counter is read again. PostgreSQL (15+) backends flush pending table
 * statistics on a timer (PGSTAT_MAX_INTERVAL, 60 s) when they get to report, so waiting past it makes it more likely,
 * not certain, that a delete committed before round 2 is reflected: a backend that is busy or never idle can report
 * later. A heuristic delay, not a flush guarantee.
 */
export const AUTHORITY_STATS_SETTLE_MS = 61_000;
/** The most orgs one evidence run evaluates (backlog-ops BACKLOG_OPS_MAX_ORGS). */
export const AUTHORITY_MAX_ORGS = 64;
/** Queues listed per page; a full page means the list may be truncated, which refuses. */
export const AUTHORITY_QUEUE_PAGE = 500;

export const AUTHORITY_REFUSALS = [
  // stop / mirror (ingest Worker)
  "stop_deployments_unreadable", "stop_deployment_invalid", "stop_deployment_order", "stop_version_unreadable",
  "stop_cutover_unreadable", "stop_org_not_listed", "stop_boundary_unproven", "stop_deployment_changed", "mirror_off",
  // queue
  "queue_list_unreadable", "queue_list_truncated", "queue_missing", "queue_identity_changed", "queue_binding_mismatch",
  "queue_consumer_mismatch", "queue_producer_mismatch", "queue_route_changed", "queue_inventory_incomplete", "queue_metrics_unreadable", "queue_backlog_nonzero",
  "dlq_unreadable", "dlq_consumer_attached", "dlq_backlog_decreased", "observation_window_short",
  // sql
  "sql_no_stop_anchor", "sql_unreadable", "sql_rows_not_visible", "sql_not_read_only", "sql_rows_rewritten", "baseline_not_snapshot",
  "sql_stats_unreadable", "sql_stats_reset", "sql_batches_deleted", "sql_batches_claimed_after_stop",
  "sql_batches_committed_after_stop", "sql_batches_pending", "sql_revisions_after_stop", "sql_revisions_changed",
  "sql_ownership_changed", "sql_unbound", "bound_invalid", "bound_not_ready", "bound_stop_mismatch",
  // turns (relayhistory-api)
  "turns_deployments_unreadable", "turns_deployment_invalid", "turns_version_unreadable", "turns_path_unknown",
  "turns_path_not_neon", "turns_deployment_changed",
] as const;
export type AuthorityRefusal = (typeof AUTHORITY_REFUSALS)[number];

/** Input refusals (thrown, not part of a verdict: there is no org to attach them to). */
export const AUTHORITY_INPUT_REFUSALS = ["authority_orgs", "authority_window"] as const;

export interface AuthorityEvidenceSources {
  /**
   * GET https://api.cloudflare.com/client/v4/accounts/<AUTHORITY_EVIDENCE.accountId><path>; resolves the WHOLE parsed
   * JSON body ({success, result, result_info?, ...}). The module checks success and reads result_info (pagination).
   */
  cfGet(path: string): Promise<unknown>;
  /** One read-only statement (the caller's READ ONLY transaction, the backlog-ops dry role); resolves its rows. */
  sql(text: string, params: readonly (string | null)[]): Promise<readonly Record<string, unknown>[]>;
  /** The real clock: the window is measured on it, never assumed from sleep. */
  nowMs(): number;
  sleep(ms: number): Promise<void>;
}

export interface AuthorityEvidenceOptions {
  /** At least AUTHORITY_MIN_OBSERVATION_WINDOW_MS. */
  observationWindowMs?: number;
  /** A ready authorityBaseline, bound by the caller's gate. Absent: an unbound run (never a pass). */
  bound?: unknown;
}

export interface Check { ok: boolean; code: AuthorityRefusal | null }
export interface StopCheck extends Check {
  servingVersions: number;
  deploymentsWalked: number;
  /** Deployments since (and including) the one that established the stop. */
  stopDeployments: number;
  /** created_on of the deployment that established the stop (epoch ms), or null. */
  effectiveStopMs: number | null;
  stopDeploymentHash: string | null;
}
export interface QueueCheck extends Check {
  backlogReads: number[]; dlqBacklogReads: number[]; dlqConsumers: number | null; observedWindowMs: number | null;
}
export interface SqlCounts {
  batchesClaimedSince: number; batchesCommittedSince: number; batchesPending: number;
  revisionsUpdatedSince: number; revisionsCount: number; ownershipCount: number;
}
export interface SqlFingerprint { revisionsCount: number; revisionsDigest: string; ownershipCount: number; ownershipDigest: string }
export interface SqlCheck extends Check {
  reads: number; counts: SqlCounts | null; fingerprint: SqlFingerprint | null;
  revisionsStable: boolean | null; ownershipStable: boolean | null; bound: boolean;
}
export interface TurnsCheck extends Check { servingVersions: number; path: "neon" | "dual" | "forward" | "unknown" | null }

export interface OrgAuthorityVerdict {
  orgHash: string;
  ok: boolean;
  refusals: AuthorityRefusal[];
  stop: StopCheck;
  mirror: Check;
  queueDrained: QueueCheck;
  sqlQuiet: SqlCheck;
  turnsNeon: TurnsCheck;
}

export type AuthorityEvidenceStatus = "authority_evidence_corroborated" | "authority_evidence_unbound" | "authority_evidence_refused";
export interface AuthorityEvidenceReport {
  /** corroborated: bound and every check ok (still never marker authority). unbound: every live check ok, no baseline. */
  status: AuthorityEvidenceStatus;
  orgCount: number;
  observationWindowMs: number;
  startedAtMs: number;
  finishedAtMs: number;
  ingestDeploymentHash: string | null;
  relayhistoryDeploymentHash: string | null;
  orgs: OrgAuthorityVerdict[];
}

// ---------------------------------------------------------------------------------------------------------------

/** The API body's `result` when success is exactly true; otherwise the read refuses with `code`. */
function resultOf(body: unknown, code: AuthorityRefusal): unknown {
  if (!isObj(body) || body.success !== true || !("result" in body)) refuse(code);
  return (body as Record<string, unknown>).result;
}
/** cfGet, unwrapped: any transport failure or unsuccessful body becomes `code`. */
const cfResult = (src: AuthorityEvidenceSources, path: string, code: AuthorityRefusal) =>
  read(code, async () => resultOf(await src.cfGet(path), code));

class Refused extends Error {
  constructor(readonly code: AuthorityRefusal) { super(code); }
}
const refuse = (code: AuthorityRefusal): never => { throw new Refused(code); };
/** Run a transport call; any failure (including a non-Refused throw) becomes `code`. */
async function read<T>(code: AuthorityRefusal, f: () => Promise<T>): Promise<T> {
  try { return await f(); } catch (error) { throw error instanceof Refused ? error : new Refused(code); }
}
const codeOf = (error: unknown, fallback: AuthorityRefusal): AuthorityRefusal => error instanceof Refused ? error.code : fallback;

/** A salted, truncated hash: never an id, stable within this contract, a valid receipt code. */
export function evidenceHash(kind: string, value: string): string {
  return "h_" + createHash("sha256").update(`authority-evidence/v1\n${kind}\n${value}`).digest("hex").slice(0, 32);
}

const ORG_UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/u;
const UUID = ORG_UUID;
const isObj = (v: unknown): v is Record<string, unknown> => typeof v === "object" && v !== null && !Array.isArray(v);
const count = (v: unknown): number | null => {
  if (typeof v === "number" && Number.isSafeInteger(v) && v >= 0) return v;
  if (typeof v === "string" && /^(0|[1-9][0-9]{0,15})$/u.test(v) && Number.isSafeInteger(Number(v))) return Number(v);
  return null;
};

/** Validated org list: distinct lowercase UUIDs, 1..AUTHORITY_MAX_ORGS, order kept. */
export function authorityOrgs(orgs: readonly unknown[]): string[] {
  if (!Array.isArray(orgs) || orgs.length === 0 || orgs.length > AUTHORITY_MAX_ORGS ||
      !orgs.every((o) => typeof o === "string" && ORG_UUID.test(o)) || new Set(orgs).size !== orgs.length)
    throw Object.assign(new Error("authority_orgs"), { code: "authority_orgs" });
  return [...orgs] as string[];
}

// ---- Cloudflare deployments and versions ----------------------------------------------------------------------

export interface ParsedDeployment { id: string; createdMs: number; serving: string[] }

/**
 * A worker's deployments, newest first (the API's order, the same contract privateIngestActiveVersion relies on).
 * Each deployment's versions must be UUIDs with numeric percentages summing to 100; a version at 0% serves nothing.
 * At most `limit` deployments are parsed; created_on must strictly decrease. `complete` is false when the list held
 * more than `limit`.
 */
export function parseDeployments(value: unknown, limit: number, invalid: AuthorityRefusal, order: AuthorityRefusal): { list: ParsedDeployment[]; complete: boolean } {
  const raw = isObj(value) ? value.deployments : undefined;
  if (!Array.isArray(raw) || raw.length === 0) refuse(invalid);
  const out: ParsedDeployment[] = [];
  for (const d of (raw as unknown[]).slice(0, limit)) {
    if (!isObj(d) || typeof d.id !== "string" || !UUID.test(d.id) || typeof d.created_on !== "string" || !Array.isArray(d.versions) ||
        d.versions.length === 0) refuse(invalid);
    const dep = d as { id: string; created_on: string; versions: unknown[] };
    const createdMs = Date.parse(dep.created_on);
    if (!Number.isFinite(createdMs) || createdMs <= 0) refuse(invalid);
    let total = 0;
    const serving: string[] = [];
    for (const v of dep.versions) {
      if (!isObj(v) || typeof v.version_id !== "string" || !UUID.test(v.version_id) || typeof v.percentage !== "number" ||
          !Number.isFinite(v.percentage) || v.percentage < 0 || v.percentage > 100) refuse(invalid);
      const ver = v as { version_id: string; percentage: number };
      total += ver.percentage;
      if (ver.percentage > 0) serving.push(ver.version_id);
    }
    if (Math.abs(total - 100) > 1e-6 || serving.length === 0 || new Set(serving).size !== serving.length) refuse(invalid);
    if (out.length > 0 && !(createdMs < out[out.length - 1]!.createdMs)) refuse(order);
    out.push({ id: dep.id, createdMs, serving });
  }
  return { list: out, complete: (raw as unknown[]).length <= limit };
}

const deploymentsPath = (worker: string) => `/workers/scripts/${worker}/deployments`;
const versionPath = (worker: string, id: string) => `/workers/scripts/${worker}/versions/${id}`;

/** A version read whose identity and binding inventory are verified. */
function verifiedVersion(value: unknown, id: string, code: AuthorityRefusal): CloudflareWorkerVersion {
  const v = value as CloudflareWorkerVersion;
  if (!isObj(value) || v.id !== id || !isObj(v.resources) || !Array.isArray(v.resources.bindings)) refuse(code);
  const names = v.resources!.bindings!.map((b) => (isObj(b) ? b.name : undefined));
  if (names.some((n) => typeof n !== "string") || new Set(names).size !== names.length) refuse(code);
  return v;
}

function versionReader(src: AuthorityEvidenceSources, worker: string, code: AuthorityRefusal) {
  const cache = new Map<string, Promise<CloudflareWorkerVersion>>();
  return (id: string) => {
    let p = cache.get(id);
    if (!p) {
      p = read(code, async () => verifiedVersion(await cfResult(src, versionPath(worker, id), code), id, code));
      cache.set(id, p);
    }
    return p;
  };
}

/** The ingest Worker's stop and mirror for one version, read with the release contract's own parser. */
function versionStop(version: CloudflareWorkerVersion, org: string): { listed: boolean; mirror: boolean } {
  let cutover;
  try { cutover = liveCutover(version); } catch { return refuse("stop_cutover_unreadable"); }
  return { listed: (cutover.httpAdmissionStoppedOrgs ?? []).includes(org), mirror: cutover.s3FenceMirror === "on" };
}

/** True when the version binds INGEST_ADMISSION to exactly the measured admission queue. */
function bindsMeasuredQueue(version: CloudflareWorkerVersion): boolean {
  const rows = version.resources!.bindings!.filter((b) => b.name === AUTHORITY_EVIDENCE.admissionBinding);
  return rows.length === 1 && rows[0]!.type === "queue" && rows[0]!.queue_name === AUTHORITY_EVIDENCE.admissionQueue;
}

interface StopResult { stop: StopCheck; mirror: Check; queueBound: boolean; routeKept: boolean }

async function stopCheck(
  org: string, deployments: { list: ParsedDeployment[]; complete: boolean },
  version: (id: string) => Promise<CloudflareWorkerVersion>, nowMs: number,
): Promise<StopResult> {
  const list = deployments.list, current = list[0]!;
  const stop: StopCheck = { ok: false, code: null, servingVersions: current.serving.length, deploymentsWalked: 0, stopDeployments: 0,
    effectiveStopMs: null, stopDeploymentHash: null };
  const fail = (code: AuthorityRefusal, mirror: Check = { ok: false, code }, walked = 0): StopResult =>
    ({ stop: { ...stop, code, deploymentsWalked: walked }, mirror, queueBound: false, routeKept: false });
  if (!(current.createdMs <= nowMs)) return fail("stop_deployment_invalid");
  let queueBound = true;
  try {
    // The current deployment: every serving version carries the stop AND the mirror.
    let listed = true, mirrored = true;
    for (const id of current.serving) {
      const v = await version(id), s = versionStop(v, org);
      listed &&= s.listed; mirrored &&= s.mirror; queueBound &&= bindsMeasuredQueue(v);
    }
    const mirror: Check = { ok: mirrored, code: mirrored ? null : "mirror_off" };
    if (!listed) return fail("stop_org_not_listed", mirror, 1);
    // Walk back to a PROVEN boundary: a deployment whose serving versions are all readable, one of them without the
    // org. Anything else (an unreadable version, the end of the retained list, the walk bound) is unproven.
    let anchor = 0;
    for (let i = 1; i < list.length; i++) {
      let all = true;
      const queues: boolean[] = [];
      for (const id of list[i]!.serving) {
        let v: CloudflareWorkerVersion, s: { listed: boolean };
        try { v = await version(id); s = versionStop(v, org); }
        catch { return fail("stop_boundary_unproven", mirror, i + 1); }
        if (!s.listed) all = false;
        queues.push(bindsMeasuredQueue(v));
      }
      if (!all) {
        // The boundary served right before the stop: whatever it enqueued is what the stop must wait for, so it too
        // must produce to the measured queue. A route change at the stop is unproven drainage (queue_route_changed).
        return {
          stop: { ...stop, ok: true, deploymentsWalked: i + 1, stopDeployments: anchor + 1, effectiveStopMs: list[anchor]!.createdMs,
            stopDeploymentHash: evidenceHash("deployment", list[anchor]!.id) },
          mirror, queueBound, routeKept: queues.every(Boolean),
        };
      }
      // A deployment inside the stop window: it served while the stop held, so it must produce to the measured queue.
      queueBound &&= queues.every(Boolean);
      anchor = i;
    }
    return fail("stop_boundary_unproven", mirror, list.length);
  } catch (error) {
    return fail(codeOf(error, "stop_version_unreadable"));
  }
}

// ---- relayhistory turns write path ----------------------------------------------------------------------------

/** relayhistory-cloud turnsWritePath, verbatim semantics: anything but an exact "dual"/"forward" (trimmed) is neon. */
export function resolveTurnsWritePath(raw: string | undefined, orgsRaw: string | undefined, orgId: string): "neon" | "dual" | "forward" {
  const mode = raw?.trim();
  if (mode !== "dual" && mode !== "forward") return "neon";
  const orgs = (orgsRaw ?? "").split(",").map((s) => s.trim()).filter(Boolean);
  return orgs.length === 0 || orgs.includes(orgId) ? mode : "neon";
}

const UNREADABLE = Symbol("unreadable");
/** A binding's text: undefined when absent; UNREADABLE when present but not one plain_text string (e.g. a secret). */
function bindingText(version: CloudflareWorkerVersion, name: string): string | undefined | typeof UNREADABLE {
  const rows = version.resources!.bindings!.filter((b) => b.name === name);
  if (rows.length === 0) return undefined;
  if (rows.length !== 1 || rows[0]!.type !== "plain_text" || typeof rows[0]!.text !== "string") return UNREADABLE;
  return rows[0]!.text as string;
}

/** One version's path for the org, or "unknown" when a binding the resolver would read cannot be read. */
export function versionTurnsPath(version: CloudflareWorkerVersion, org: string): "neon" | "dual" | "forward" | "unknown" {
  const mode = bindingText(version, "TURNS_WRITE_PATH");
  if (mode === UNREADABLE) return "unknown";
  // The resolver never reads TURNS_FORWARD_ORGS when the mode is neon, so an unreadable list cannot change that answer.
  if (resolveTurnsWritePath(mode, "", org) === "neon") return "neon";
  const orgs = bindingText(version, "TURNS_FORWARD_ORGS");
  if (orgs === UNREADABLE) return "unknown";
  return resolveTurnsWritePath(mode, orgs, org);
}

async function turnsCheck(org: string, current: ParsedDeployment, version: (id: string) => Promise<CloudflareWorkerVersion>): Promise<TurnsCheck> {
  const base: TurnsCheck = { ok: false, code: null, servingVersions: current.serving.length, path: null };
  try {
    const paths = new Set<string>();
    for (const id of current.serving) paths.add(versionTurnsPath(await version(id), org));
    // Worst case across serving versions: unknown, then any non-neon path.
    if (paths.has("unknown")) return { ...base, path: "unknown", code: "turns_path_unknown" };
    const other = [...paths].find((p) => p !== "neon") as TurnsCheck["path"] | undefined;
    if (other) return { ...base, path: other, code: "turns_path_not_neon" };
    return { ...base, ok: true, path: "neon" };
  } catch (error) {
    return { ...base, code: codeOf(error, "turns_version_unreadable") };
  }
}

// ---- queues ---------------------------------------------------------------------------------------------------
// Shapes per https://developers.cloudflare.com/api/resources/queues/methods/get/: a Worker consumer is
// {type: "worker", script_name, dead_letter_queue (top level; "" when none), ...}, a Worker producer is
// {type: "worker", script}; every queue carries consumers_total_count and producers_total_count, which must agree with
// the enumerated arrays (an incomplete inventory is never read as a complete one).

interface QueueIds { main: string; dlq: string }
interface QueueRead { ids: QueueIds; backlog: number; dlqBacklog: number; dlqConsumers: number }
type QueueRoutingCode = "queue_consumer_mismatch" | "queue_producer_mismatch" | "queue_route_changed" | "queue_inventory_incomplete" | null;

/**
 * A queue object's consumers and producers, complete (arrays equal to their provider totals) and every identity
 * readable (a Worker consumer names its script_name, an HTTP pull consumer is a pull; a Worker producer names its
 * script, an R2 producer its bucket_name), or null: an identity that cannot be read can never be excluded.
 */
function inventory(q: Record<string, unknown>): { consumers: unknown[]; producers: unknown[] } | null {
  const { consumers, producers } = q;
  if (!Array.isArray(consumers) || !Array.isArray(producers) || count(q.consumers_total_count) !== consumers.length ||
      count(q.producers_total_count) !== producers.length) return null;
  const consumerOk = (c: unknown) => isObj(c) && ((c.type === "worker" && typeof c.script_name === "string" && c.script_name !== "") ||
    c.type === "http_pull");
  const producerOk = (p: unknown) => isObj(p) && ((p.type === "worker" && typeof p.script === "string" && p.script !== "") ||
    (p.type === "r2_bucket" && typeof p.bucket_name === "string" && p.bucket_name !== ""));
  if (!consumers.every(consumerOk) || !producers.every(producerOk)) return null;
  return { consumers, producers };
}
const isIngestConsumer = (c: unknown): boolean =>
  isObj(c) && c.type === "worker" && c.script_name === AUTHORITY_EVIDENCE.ingestWorker;
const isIngestProducer = (p: unknown): boolean =>
  isObj(p) && p.type === "worker" && p.script === AUTHORITY_EVIDENCE.ingestWorker;

/**
 * The account's queue list (one page; a full page refuses): the measured queue and DLQ by name, and the route check:
 * no queue but the admission queue and the fan-out queue may have the ingest Worker as a consumer, so admission
 * messages cannot be waiting on a former route (queue_route_changed).
 */
async function queueList(src: AuthorityEvidenceSources): Promise<{ ids: QueueIds; route: QueueRoutingCode }> {
  const body = await read("queue_list_unreadable", () => src.cfGet(`/queues?page=1&per_page=${AUTHORITY_QUEUE_PAGE}`));
  const list = resultOf(body, "queue_list_unreadable");
  if (!Array.isArray(list)) refuse("queue_list_unreadable");
  const rows = list as unknown[];
  // Complete only when the provider's own pagination says this one page is everything (result_info).
  const info = (body as Record<string, unknown>).result_info;
  if (rows.length >= AUTHORITY_QUEUE_PAGE || !isObj(info) || count(info.total_count) !== rows.length || info.page !== 1 ||
      !(info.total_pages === 1 || (rows.length === 0 && info.total_pages === 0))) refuse("queue_list_truncated");
  if (!rows.every((q) => isObj(q) && typeof q.queue_name === "string")) refuse("queue_list_unreadable");
  const find = (name: string) => {
    const hits = rows.filter((q) => (q as Record<string, unknown>).queue_name === name);
    if (hits.length !== 1) refuse("queue_missing");
    const id = (hits[0] as Record<string, unknown>).queue_id;
    if (typeof id !== "string" || !/^[0-9a-f]{32}$/u.test(id)) refuse("queue_list_unreadable");
    return id as string;
  };
  const ids = { main: find(AUTHORITY_EVIDENCE.admissionQueue), dlq: find(AUTHORITY_EVIDENCE.admissionDlq) };
  let route: QueueRoutingCode = null;
  for (const q of rows as Record<string, unknown>[]) {
    const inv = inventory(q);
    if (!inv) { route ??= "queue_inventory_incomplete"; continue; }
    if (inv.consumers.some(isIngestConsumer) && q.queue_name !== AUTHORITY_EVIDENCE.admissionQueue && q.queue_name !== AUTHORITY_EVIDENCE.fanoutQueue)
      route ??= "queue_route_changed";
  }
  return { ids, route };
}

/** GET /queues/<id>/metrics: `backlog_count` must be a non-negative integer. */
async function backlog(src: AuthorityEvidenceSources, id: string, code: AuthorityRefusal): Promise<number> {
  const m = await cfResult(src, `/queues/${id}/metrics`, code);
  const n = isObj(m) ? count(m.backlog_count) : null;
  return n === null ? refuse(code) : n;
}

/** GET /queues/<main>: exactly one consumer, the ingest Worker dead-lettering to the measured DLQ; producers all the Worker. */
async function queueRouting(src: AuthorityEvidenceSources, ids: QueueIds): Promise<QueueRoutingCode> {
  const q = await cfResult(src, `/queues/${ids.main}`, "queue_list_unreadable");
  if (!isObj(q) || q.queue_id !== ids.main || q.queue_name !== AUTHORITY_EVIDENCE.admissionQueue) refuse("queue_list_unreadable");
  const inv = inventory(q as Record<string, unknown>);
  if (!inv) return "queue_inventory_incomplete";
  if (inv.consumers.length !== 1 || !isIngestConsumer(inv.consumers[0]) ||
      (inv.consumers[0] as Record<string, unknown>).dead_letter_queue !== AUTHORITY_EVIDENCE.admissionDlq) return "queue_consumer_mismatch";
  if (inv.producers.length === 0 || !inv.producers.every(isIngestProducer)) return "queue_producer_mismatch";
  return null;
}

async function queueRead(src: AuthorityEvidenceSources): Promise<{ read: QueueRead; routing: QueueRoutingCode }> {
  const { ids, route } = await queueList(src);
  const routing = route ?? await queueRouting(src, ids);
  const main = await backlog(src, ids.main, "queue_metrics_unreadable");
  const dlqBacklog = await backlog(src, ids.dlq, "dlq_unreadable");
  const dlq = await cfResult(src, `/queues/${ids.dlq}`, "dlq_unreadable");
  if (!isObj(dlq) || dlq.queue_id !== ids.dlq || dlq.queue_name !== AUTHORITY_EVIDENCE.admissionDlq) refuse("dlq_unreadable");
  const inv = inventory(dlq as Record<string, unknown>);
  if (!inv) refuse("dlq_unreadable");
  return { read: { ids, backlog: main, dlqBacklog, dlqConsumers: inv!.consumers.length }, routing };
}

function queueVerdict(reads: QueueRead[], routing: QueueRoutingCode, failure: AuthorityRefusal | null, observed: number | null, windowMs: number): QueueCheck {
  const out: QueueCheck = {
    ok: false, code: failure, backlogReads: reads.map((r) => r.backlog), dlqBacklogReads: reads.map((r) => r.dlqBacklog),
    dlqConsumers: reads.length ? Math.max(...reads.map((r) => r.dlqConsumers)) : null, observedWindowMs: observed,
  };
  if (failure) return out;
  if (reads.length !== 2) return { ...out, code: "queue_list_unreadable" };
  if (reads[0]!.ids.main !== reads[1]!.ids.main || reads[0]!.ids.dlq !== reads[1]!.ids.dlq) return { ...out, code: "queue_identity_changed" };
  if (routing) return { ...out, code: routing };
  if (observed === null || observed < windowMs) return { ...out, code: "observation_window_short" };
  if (reads.some((r) => r.backlog !== 0)) return { ...out, code: "queue_backlog_nonzero" };
  if (reads.some((r) => r.dlqConsumers !== 0)) return { ...out, code: "dlq_consumer_attached" };
  if (reads[1]!.dlqBacklog < reads[0]!.dlqBacklog) return { ...out, code: "dlq_backlog_decreased" };
  return { ...out, ok: true };
}

// ---- SQL ------------------------------------------------------------------------------------------------------

/** The three tables AUTHORITY_SQL reads. */
export const AUTHORITY_SQL_TABLES = ["ingest_batches", "ingest_record_revisions", "ingest_session_ownership"] as const;

/**
 * Per table: whether the current role sees EVERY row: SELECT privilege, and row-level security not applying to it
 * (RLS off, or the role is a superuser, has BYPASSRLS, or holds the owner's privileges on a table that does not FORCE
 * RLS: Postgres's check_enable_rls). It is a CTE of AUTHORITY_SQL, so it is answered by the same role, in the same
 * statement and snapshot as the counts; a filtered read can never pass as an empty org.
 */
export const AUTHORITY_VISIBILITY_SQL = `
SELECT c.relname AS table_name,
       (has_table_privilege(c.oid, 'SELECT')
        AND (NOT c.relrowsecurity OR r.rolsuper OR r.rolbypassrls
             OR (NOT c.relforcerowsecurity AND pg_has_role(current_user, c.relowner, 'USAGE')))) AS visible
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
JOIN pg_catalog.pg_roles r ON r.rolname = current_user
WHERE n.nspname = 'sessions' AND c.relkind IN ('r', 'p')
  AND c.relname IN ('ingest_batches', 'ingest_record_revisions', 'ingest_session_ownership')`;

/**
 * A row's xmin as a full xid8, from the statement's own snapshot xmax (s.cur): the row's 32-bit xmin is at most 2^31
 * transactions in the past (wraparound protection), so full = cur - ((cur mod 2^32 - xmin) mod 2^32). Every tuple
 * version an insert, update or delete-and-restore writes has a new xmin. A row counts as possibly written after the
 * base snapshot iff full >= pg_snapshot_xmin(base). Not pg_visible_in_snapshot: a tuple written in a subtransaction
 * carries the SUBxid, which a pg_snapshot's xip list does not hold, so that test can call a row of a then in-progress
 * parent visible (PostgreSQL documents subtransaction ids as excluded from it). Below the base's xmin every
 * transaction had finished when the snapshot was taken, subtransactions included, so this rule never needs the
 * parent. It is conservative: a row committed before the base by a transaction at or above its xmin also counts
 * (a false refusal, never a false pass).
 */
const xid8 = (a: string) => `((s.cur - mod(mod(s.cur, 4294967296) - ${a}.xmin::text::numeric + 4294967296, 4294967296))::text::xid8)`;
/** sha256 of a typed, unambiguous row encoding (jsonb array: strings escaped, nulls distinct, numbers typed). */
const rowSha = (row: string) => `encode(sha256(convert_to(jsonb_build_array(${row})::text, 'UTF8')), 'hex')`;
/** An order-independent multiset digest of the rows' sha256 (two 60-bit lane sums with the count), as h + 32 hex. */
const digest = (n: string, alias: string) =>
  `'h' || left(encode(sha256(convert_to(${n}::text || ':' || coalesce(${alias}.l1, 0)::text || ':' || coalesce(${alias}.l2, 0)::text, 'UTF8')), 'hex'), 32)`;
const lanes = `sum(('x' || substr(y.h, 1, 15))::bit(60)::bigint::numeric) AS l1, sum(('x' || substr(y.h, 16, 15))::bit(60)::bigint::numeric) AS l2`;
const newSince = `count(*) FILTER (WHERE s.base IS NOT NULL AND y.xid >= pg_snapshot_xmin(s.base))`;

/**
 * One read-only statement per org: the visibility answer, then one org-leading scan of each table (each primary key
 * leads with org_id). $1 the org id; $2 the SQL anchor in epoch ms (the effective stop minus AUTHORITY_CLOCK_SKEW_MS);
 * $3 a base snapshot (pg_snapshot text) or NULL: rows written after it are counted as *_new_since_base. It also returns
 * its own snapshot, the transaction's isolation and read-only setting, and the statement time.
 */
export const AUTHORITY_SQL = `
WITH s AS (
  SELECT to_timestamp($2::bigint / 1000.0) AS at, $3::text::pg_snapshot AS base, pg_current_snapshot() AS snap,
         pg_snapshot_xmax(pg_current_snapshot())::text::numeric AS cur),
v AS (${AUTHORITY_VISIBILITY_SQL}),
b AS (
  SELECT count(*) FILTER (WHERE y.claimed_at >= s.at) AS claimed_since, count(*) FILTER (WHERE y.committed_at >= s.at) AS committed_since,
         count(*) FILTER (WHERE y.state = 'pending') AS pending, ${newSince} AS new_since_base
  FROM (SELECT x.claimed_at, x.committed_at, x.state, ${xid8("x")} AS xid
        FROM sessions.ingest_batches x CROSS JOIN s WHERE x.org_id = $1::text) y CROSS JOIN s),
r AS (
  SELECT count(*) FILTER (WHERE y.updated_at >= s.at) AS updated_since, count(*) AS n, ${newSince} AS new_since_base, ${lanes}
  FROM (SELECT x.updated_at, ${xid8("x")} AS xid,
               ${rowSha("x.workspace_id, x.origin_id, x.record_id, x.revision, x.revision_id, x.digest, x.user_id, x.source, x.session_id, extract(epoch FROM x.updated_at)")} AS h
        FROM sessions.ingest_record_revisions x CROSS JOIN s WHERE x.org_id = $1::text) y CROSS JOIN s),
w AS (
  SELECT count(*) AS n, ${newSince} AS new_since_base, ${lanes}
  FROM (SELECT ${xid8("x")} AS xid, ${rowSha("x.workspace_id, x.user_id, x.origin_id, x.source, x.session_id")} AS h
        FROM sessions.ingest_session_ownership x CROSS JOIN s WHERE x.org_id = $1::text) y CROSS JOIN s)
SELECT (SELECT count(*) FROM v WHERE v.visible)::bigint AS visible_tables, (SELECT count(*) FROM v)::bigint AS tables,
       b.claimed_since::bigint AS batches_claimed_since, b.committed_since::bigint AS batches_committed_since,
       b.pending::bigint AS batches_pending, b.new_since_base::bigint AS batches_new_since_base,
       r.updated_since::bigint AS revisions_updated_since, r.n::bigint AS revisions_count,
       r.new_since_base::bigint AS revisions_new_since_base, ${digest("r.n", "r")} AS revisions_digest,
       w.n::bigint AS ownership_count, w.new_since_base::bigint AS ownership_new_since_base, ${digest("w.n", "w")} AS ownership_digest,
       s.snap::text AS snapshot, current_setting('transaction_isolation') AS isolation,
       current_setting('transaction_read_only') AS read_only,
       floor(extract(epoch FROM statement_timestamp()) * 1000)::bigint AS read_at_ms
FROM b, r, w, s`;

/** A pg_snapshot, receipt-safe: xmin, xmax and the in-progress xids as numbers. */
export interface SqlSnapshot { xmin: number; xmax: number; xip: number[] }
const MAX_XIP = 4096;
function parseSnapshot(v: unknown): SqlSnapshot | null {
  if (typeof v !== "string") return null;
  const m = /^([1-9][0-9]{0,15}):([1-9][0-9]{0,15}):((?:[1-9][0-9]{0,15})(?:,[1-9][0-9]{0,15})*)?$/u.exec(v);
  if (!m) return null;
  const xmin = Number(m[1]), xmax = Number(m[2]), xip = m[3] ? m[3].split(",").map(Number) : [];
  if (![xmin, xmax, ...xip].every(Number.isSafeInteger) || xmin > xmax || xip.length > MAX_XIP ||
      xip.some((x, i) => x < xmin || x >= xmax || (i > 0 && x <= xip[i - 1]!))) return null;
  return { xmin, xmax, xip };
}
const snapshotText = (s: SqlSnapshot) => `${s.xmin}:${s.xmax}:${s.xip.join(",")}`;
function checkedSnapshot(v: unknown): SqlSnapshot | null {
  if (!isObj(v) || !Array.isArray(v.xip)) return null;
  return parseSnapshot(`${v.xmin}:${v.xmax}:${(v.xip as unknown[]).join(",")}`);
}
const DIGEST = /^h[0-9a-f]{32}$/u;

/**
 * Deletion counter, a corroborating signal. Surviving-row samples cannot show a batch row that was inserted and then
 * deleted between them. pg_stat_all_tables.n_tup_del for ingest_batches counts DELETEd rows (by any role) as the
 * statistics system has received them. What the run does: it reads the counter before round 1 (unbound) or takes it
 * from the baseline (bound), reads it again AUTHORITY_STATS_SETTLE_MS after round 2, and refuses on any difference
 * (sql_batches_deleted), on a different stats_reset or postmaster start, on a lower value (sql_stats_reset), or with
 * track_counts off. The counter covers the whole table, so another org's erase in the interval also refuses.
 * Limits, so it is not read as more: an unchanged counter does NOT establish that no admission happened. A delete
 * whose statistics have not been flushed by the second read is not counted (the settle is a heuristic delay, not a
 * guarantee); TRUNCATE is not counted at all, and a TRUNCATE between samples can leave the sampled counts unchanged;
 * a row inserted and still present after round 2 (for example a pending claim committed during the settle) is not
 * sampled; and a per-table statistics reset back to the same value would go unseen. The only code path that deletes
 * ingest_batches rows is the owner-only operator user erase (20261105058010).
 */
export const AUTHORITY_STATS_SQL = `
SELECT (SELECT count(*) FROM pg_catalog.pg_stat_all_tables t WHERE t.schemaname = 'sessions' AND t.relname = 'ingest_batches')::bigint AS tables,
       (SELECT t.n_tup_del FROM pg_catalog.pg_stat_all_tables t WHERE t.schemaname = 'sessions' AND t.relname = 'ingest_batches')::bigint AS batches_deleted,
       coalesce((SELECT floor(extract(epoch FROM d.stats_reset) * 1000) FROM pg_catalog.pg_stat_database d WHERE d.datname = current_database()), 0)::bigint AS stats_reset_ms,
       floor(extract(epoch FROM pg_postmaster_start_time()) * 1000)::bigint AS started_ms,
       current_setting('track_counts') AS track_counts`;

export interface SqlStats { batchesDeleted: number; statsResetMs: number; startedMs: number }
async function statsRead(src: AuthorityEvidenceSources): Promise<SqlStats> {
  const rows = await read("sql_stats_unreadable", () => src.sql(AUTHORITY_STATS_SQL, []));
  if (!Array.isArray(rows) || rows.length !== 1 || !isObj(rows[0])) refuse("sql_stats_unreadable");
  const r = rows[0] as Record<string, unknown>;
  const [n, del, reset, started] = [r.tables, r.batches_deleted, r.stats_reset_ms, r.started_ms].map(count);
  if (n !== 1 || del === null || reset === null || started === null || r.track_counts !== "on") refuse("sql_stats_unreadable");
  return { batchesDeleted: del!, statsResetMs: reset!, startedMs: started! };
}
const checkedStats = (v: unknown): SqlStats | null => {
  if (!isObj(v)) return null;
  const [a, b, c] = [v.batchesDeleted, v.statsResetMs, v.startedMs].map(count);
  return a === null || b === null || c === null ? null : { batchesDeleted: a, statsResetMs: b, startedMs: c };
};
/** The deletion verdict between a reference and a later read. */
function statsCode(ref: SqlStats, later: SqlStats): AuthorityRefusal | null {
  if (ref.statsResetMs !== later.statsResetMs || ref.startedMs !== later.startedMs || later.batchesDeleted < ref.batchesDeleted) return "sql_stats_reset";
  return later.batchesDeleted === ref.batchesDeleted ? null : "sql_batches_deleted";
}

interface SqlRead extends SqlCounts {
  batchesNewSinceBase: number; revisionsNewSinceBase: number; ownershipNewSinceBase: number;
  revisionsDigest: string; ownershipDigest: string; snapshot: SqlSnapshot; readAtMs: number;
  repeatable: boolean;
}

/** One read; refuses sql_rows_not_visible before any count can be used, and sql_not_read_only outside READ ONLY. */
async function sqlRead(src: AuthorityEvidenceSources, org: string, stopMs: number, base: SqlSnapshot | null): Promise<SqlRead> {
  const rows = await read("sql_unreadable", () =>
    src.sql(AUTHORITY_SQL, [org, String(stopMs - AUTHORITY_CLOCK_SKEW_MS), base === null ? null : snapshotText(base)] ));
  if (!Array.isArray(rows) || rows.length !== 1 || !isObj(rows[0])) refuse("sql_unreadable");
  const r = rows[0] as Record<string, unknown>;
  if (count(r.tables) !== AUTHORITY_SQL_TABLES.length) refuse("sql_unreadable");
  if (count(r.visible_tables) !== AUTHORITY_SQL_TABLES.length) refuse("sql_rows_not_visible");
  if (r.read_only !== "on") refuse("sql_not_read_only");
  const keys = ["batches_claimed_since", "batches_committed_since", "batches_pending", "batches_new_since_base", "revisions_updated_since",
    "revisions_count", "revisions_new_since_base", "ownership_count", "ownership_new_since_base", "read_at_ms"] as const;
  const v = keys.map((k) => count(r[k]));
  const snapshot = parseSnapshot(r.snapshot);
  if (v.some((n) => n === null) || !snapshot || typeof r.revisions_digest !== "string" || !DIGEST.test(r.revisions_digest) ||
      typeof r.ownership_digest !== "string" || !DIGEST.test(r.ownership_digest) || typeof r.isolation !== "string") refuse("sql_unreadable");
  const [a, b, c, d, e, f, g, h, i, j] = v as number[];
  return { batchesClaimedSince: a!, batchesCommittedSince: b!, batchesPending: c!, batchesNewSinceBase: d!, revisionsUpdatedSince: e!,
    revisionsCount: f!, revisionsNewSinceBase: g!, ownershipCount: h!, ownershipNewSinceBase: i!, readAtMs: j!,
    revisionsDigest: r.revisions_digest as string, ownershipDigest: r.ownership_digest as string, snapshot: snapshot!,
    repeatable: r.isolation === "repeatable read" || r.isolation === "serializable" };
}

const fingerprintOf = (r: SqlRead): SqlFingerprint =>
  ({ revisionsCount: r.revisionsCount, revisionsDigest: r.revisionsDigest, ownershipCount: r.ownershipCount, ownershipDigest: r.ownershipDigest });
const sameFp = (a: SqlFingerprint, b: SqlFingerprint, k: "revisions" | "ownership") =>
  k === "revisions" ? a.revisionsCount === b.revisionsCount && a.revisionsDigest === b.revisionsDigest
    : a.ownershipCount === b.ownershipCount && a.ownershipDigest === b.ownershipDigest;

/** The since-stop refusals every read (evidence or baseline) applies. */
function sinceStopCode(c: SqlCounts): AuthorityRefusal | null {
  if (c.batchesClaimedSince) return "sql_batches_claimed_after_stop";
  if (c.batchesCommittedSince) return "sql_batches_committed_after_stop";
  if (c.batchesPending) return "sql_batches_pending";
  if (c.revisionsUpdatedSince) return "sql_revisions_after_stop";
  return null;
}

// ---- baseline -------------------------------------------------------------------------------------------------

export interface BaselineOrg {
  orgHash: string; ok: boolean; refusals: AuthorityRefusal[];
  stopDeploymentHash: string | null; effectiveStopMs: number | null;
  readAtMs: number | null; snapshot: SqlSnapshot | null; fingerprint: SqlFingerprint | null; stats: SqlStats | null;
}
export interface AuthorityBaseline {
  kind: "authority_baseline"; v: 1;
  status: "authority_baseline_ready" | "authority_baseline_refused";
  orgCount: number; orgs: BaselineOrg[];
}

/** One org's entry of an unbound evidence report: its stop, if that report passed every live check. */
function unboundStop(report: unknown, orgHash: string): { stopDeploymentHash: string; effectiveStopMs: number } | AuthorityRefusal {
  if (!isObj(report) || !Array.isArray(report.orgs) || report.status !== "authority_evidence_unbound") return "bound_invalid";
  const hits = (report.orgs as unknown[]).filter((o) => isObj(o) && o.orgHash === orgHash);
  if (hits.length !== 1) return "bound_invalid";
  const o = hits[0] as Record<string, unknown>;
  if (!isObj(o.stop) || !Array.isArray(o.refusals)) return "bound_invalid";
  if (o.refusals.length !== 1 || o.refusals[0] !== "sql_unbound" || o.stop.ok !== true) return "bound_not_ready";
  if (typeof o.stop.stopDeploymentHash !== "string" || count(o.stop.effectiveStopMs) === null) return "bound_invalid";
  return { stopDeploymentHash: o.stop.stopDeploymentHash, effectiveStopMs: count(o.stop.effectiveStopMs)! };
}

/**
 * The baseline a later authorityEvidence run binds. Run it INSIDE the backfill's input snapshot: the same REPEATABLE
 * READ (or SERIALIZABLE) READ ONLY transaction the backfill reads in, or one that imported the backfill's snapshot
 * (SET TRANSACTION SNAPSHOT); a READ COMMITTED read refuses (baseline_not_snapshot), because its snapshot is the
 * statement's and the backfill's reads would not be bound to it. `evidence` is an unbound authorityEvidence report
 * (every live check passed) that supplies each org's stop. The bound run then checks, at each sample, that the org's
 * revision and ownership rows are exactly this snapshot's (none written after it by xmin, counts equal, which together
 * leave the same set; the digests are a further cross-check). For ingest_batches it checks only the since-stop,
 * pending and written-after-the-snapshot predicates, not set equality: a removed batch row is not detected. Nothing
 * covers changes undone between samples or made after the last one.
 */
export async function authorityBaseline(
  orgsInput: readonly unknown[], evidence: unknown, sql: AuthorityEvidenceSources["sql"],
): Promise<AuthorityBaseline> {
  const orgs = authorityOrgs(orgsInput);
  const src = { sql } as AuthorityEvidenceSources;
  const out: BaselineOrg[] = [];
  for (const org of orgs) {
    const orgHash = evidenceHash("org", org);
    const base: BaselineOrg = { orgHash, ok: false, refusals: [], stopDeploymentHash: null, effectiveStopMs: null, readAtMs: null, snapshot: null,
      fingerprint: null, stats: null };
    const stop = unboundStop(evidence, orgHash);
    if (typeof stop === "string") { out.push({ ...base, refusals: [stop] }); continue; }
    let r: SqlRead, stats: SqlStats;
    try { r = await sqlRead(src, org, stop.effectiveStopMs, null); stats = await statsRead(src); }
    catch (error) { out.push({ ...base, ...stop, refusals: [codeOf(error, "sql_unreadable")] }); continue; }
    const code = !r.repeatable ? "baseline_not_snapshot" : sinceStopCode(r) ?? (r.readAtMs < stop.effectiveStopMs ? "bound_stop_mismatch" : null);
    out.push({ ...base, ...stop, ok: code === null, refusals: code ? [code] : [], readAtMs: r.readAtMs, snapshot: r.snapshot,
      fingerprint: fingerprintOf(r), stats });
  }
  return { kind: "authority_baseline", v: 1, status: out.every((o) => o.ok) ? "authority_baseline_ready" : "authority_baseline_refused",
    orgCount: orgs.length, orgs: out };
}

type BoundOrg = { fingerprint: SqlFingerprint; effectiveStopMs: number; stopDeploymentHash: string; readAtMs: number; snapshot: SqlSnapshot; stats: SqlStats };
function boundOrg(bound: unknown, orgHash: string): BoundOrg | AuthorityRefusal {
  if (!isObj(bound) || bound.kind !== "authority_baseline" || bound.v !== 1 || !Array.isArray(bound.orgs)) return "bound_invalid";
  const hits = (bound.orgs as unknown[]).filter((o) => isObj(o) && o.orgHash === orgHash);
  if (hits.length !== 1) return "bound_invalid";
  const o = hits[0] as Record<string, unknown>;
  if (o.ok !== true || bound.status !== "authority_baseline_ready" || !Array.isArray(o.refusals) || o.refusals.length !== 0) return "bound_not_ready";
  const fp = isObj(o.fingerprint) ? o.fingerprint : null, snapshot = checkedSnapshot(o.snapshot), stats = checkedStats(o.stats);
  if (!fp || !stats || count(fp.revisionsCount) === null || count(fp.ownershipCount) === null || typeof fp.revisionsDigest !== "string" ||
      !DIGEST.test(fp.revisionsDigest) || typeof fp.ownershipDigest !== "string" || !DIGEST.test(fp.ownershipDigest) || !snapshot ||
      count(o.effectiveStopMs) === null || count(o.readAtMs) === null || typeof o.stopDeploymentHash !== "string") return "bound_invalid";
  return { fingerprint: { revisionsCount: count(fp.revisionsCount)!, revisionsDigest: fp.revisionsDigest, ownershipCount: count(fp.ownershipCount)!,
      ownershipDigest: fp.ownershipDigest }, effectiveStopMs: count(o.effectiveStopMs)!, stopDeploymentHash: o.stopDeploymentHash,
    readAtMs: count(o.readAtMs)!, snapshot, stats: stats! };
}

function sqlVerdict(
  reads: SqlRead[], failure: AuthorityRefusal | null, observed: number | null, windowMs: number,
  bound: BoundOrg | AuthorityRefusal | null, stop: StopCheck, startedAtMs: number,
  stats: { reference: SqlStats | null; settled: SqlStats | null; failure: AuthorityRefusal | null; settleMs: number | null },
): SqlCheck {
  const last = reads[reads.length - 1];
  const counts: SqlCounts | null = last ? {
    batchesClaimedSince: Math.max(...reads.map((r) => r.batchesClaimedSince)),
    batchesCommittedSince: Math.max(...reads.map((r) => r.batchesCommittedSince)),
    batchesPending: Math.max(...reads.map((r) => r.batchesPending)),
    revisionsUpdatedSince: Math.max(...reads.map((r) => r.revisionsUpdatedSince)),
    revisionsCount: last.revisionsCount, ownershipCount: last.ownershipCount,
  } : null;
  const fps = reads.map(fingerprintOf);
  const stable = (k: "revisions" | "ownership") => fps.length < 2 ? null : fps.every((f) => sameFp(f, fps[0]!, k));
  const revisionsStable = stable("revisions"), ownershipStable = stable("ownership");
  const out: SqlCheck = { ok: false, code: failure, reads: reads.length, counts, fingerprint: last ? fingerprintOf(last) : null,
    revisionsStable, ownershipStable, bound: false };
  if (failure) return out;
  if (!counts || reads.length !== 2) return { ...out, code: "sql_unreadable" };
  if (observed === null || observed < windowMs) return { ...out, code: "observation_window_short" };
  const since = sinceStopCode(counts);
  if (since) return { ...out, code: since };
  // Rows written after the base snapshot (round 1's own when unbound, the baseline's when bound): a rewrite that
  // leaves every count and digest equal (delete and restore, an update back to the same values) is still a write.
  if (reads.some((r) => r.batchesNewSinceBase || r.revisionsNewSinceBase || r.ownershipNewSinceBase)) return { ...out, code: "sql_rows_rewritten" };
  if (revisionsStable !== true) return { ...out, code: "sql_revisions_changed" };
  if (ownershipStable !== true) return { ...out, code: "sql_ownership_changed" };
  // Deletions since the reference: the baseline's counters when bound, round 1's when unbound.
  if (stats.failure) return { ...out, code: stats.failure };
  if (stats.settleMs === null || stats.settleMs < AUTHORITY_STATS_SETTLE_MS) return { ...out, code: "observation_window_short" };
  const ref = bound && typeof bound !== "string" ? bound.stats : stats.reference;
  if (!ref || !stats.settled) return { ...out, code: "sql_stats_unreadable" };
  const deleted = statsCode(ref, stats.settled);
  if (deleted) return { ...out, code: deleted };
  if (bound === null) return { ...out, code: "sql_unbound" };
  if (typeof bound === "string") return { ...out, code: bound };
  if (bound.effectiveStopMs !== stop.effectiveStopMs || bound.stopDeploymentHash !== stop.stopDeploymentHash ||
      bound.readAtMs < bound.effectiveStopMs) return { ...out, code: "bound_stop_mismatch" };
  // Database clock vs the injected one: allow the skew margin, no more.
  if (bound.readAtMs > startedAtMs + AUTHORITY_CLOCK_SKEW_MS) return { ...out, code: "bound_not_ready" };
  if (!fps.every((f) => sameFp(f, bound.fingerprint, "revisions"))) return { ...out, code: "sql_revisions_changed" };
  if (!fps.every((f) => sameFp(f, bound.fingerprint, "ownership"))) return { ...out, code: "sql_ownership_changed" };
  return { ...out, ok: true, bound: true };
}

// ---- the evidence run -----------------------------------------------------------------------------------------

/**
 * Read every piece of evidence for `orgs` and return one verdict per org (input order). Never throws for evidence;
 * throws only `authority_orgs` / `authority_window` for bad arguments. The best status, authority_evidence_corroborated
 * (bound to a ready authorityBaseline, every org ok), is corroboration only and never authorises a marker (header).
 * authority_evidence_unbound (every live check passed, no baseline bound) is what authorityBaseline takes.
 *
 * Order: ingest deployments (stop, mirror, queue binding) -> relayhistory deployments (turns) -> round 1 (queue list,
 * routing and backlog; SQL per org) -> wait -> round 2 -> both deployments re-read (the current one must be
 * unchanged). The window is the injected clock's elapsed time from the end of round 1 to the start of round 2. Each
 * round's SQL is one statement per org; run them in one READ ONLY transaction per round with the backlog-ops dry role.
 */
export async function authorityEvidence(
  orgsInput: readonly unknown[], src: AuthorityEvidenceSources, options: AuthorityEvidenceOptions = {},
): Promise<AuthorityEvidenceReport> {
  const orgs = authorityOrgs(orgsInput);
  const windowMs = options.observationWindowMs ?? AUTHORITY_OBSERVATION_WINDOW_MS;
  if (!Number.isSafeInteger(windowMs) || windowMs < AUTHORITY_MIN_OBSERVATION_WINDOW_MS || windowMs > 30 * 60_000)
    throw Object.assign(new Error("authority_window"), { code: "authority_window" });
  const { ingestWorker, relayhistoryWorker } = AUTHORITY_EVIDENCE;
  const startedAtMs = src.nowMs();

  // 1, 2 (and the queue binding of every version that served since the stop, and of the boundary).
  let ingest: { list: ParsedDeployment[]; complete: boolean } | null = null, ingestFailure: AuthorityRefusal | null = null;
  try {
    ingest = parseDeployments(await cfResult(src, deploymentsPath(ingestWorker), "stop_deployments_unreadable"),
      AUTHORITY_MAX_DEPLOYMENT_WALK, "stop_deployment_invalid", "stop_deployment_order");
  } catch (error) { ingestFailure = codeOf(error, "stop_deployments_unreadable"); }
  const ingestVersion = versionReader(src, ingestWorker, "stop_version_unreadable");
  const stops = new Map<string, StopResult>();
  for (const org of orgs) {
    stops.set(org, ingest ? await stopCheck(org, ingest, ingestVersion, startedAtMs) : {
      stop: { ok: false, code: ingestFailure, servingVersions: 0, deploymentsWalked: 0, stopDeployments: 0, effectiveStopMs: null, stopDeploymentHash: null },
      mirror: { ok: false, code: ingestFailure }, queueBound: false, routeKept: false,
    });
  }

  // 5: turns.
  let rh: ParsedDeployment | null = null, rhFailure: AuthorityRefusal | null = null;
  try {
    rh = parseDeployments(await cfResult(src, deploymentsPath(relayhistoryWorker), "turns_deployments_unreadable"),
      1, "turns_deployment_invalid", "turns_deployment_invalid").list[0]!;
  } catch (error) { rhFailure = codeOf(error, "turns_deployments_unreadable"); }
  const rhVersion = versionReader(src, relayhistoryWorker, "turns_version_unreadable");
  const turns = new Map<string, TurnsCheck>();
  for (const org of orgs)
    turns.set(org, rh ? await turnsCheck(org, rh, rhVersion) : { ok: false, code: rhFailure, servingVersions: 0, path: null });

  // The bound baseline per org (null when unbound).
  const bounds = new Map<string, BoundOrg | AuthorityRefusal | null>(
    orgs.map((o) => [o, options.bound === undefined ? null : boundOrg(options.bound, evidenceHash("org", o))]));

  // 3, 4: two rounds a measured window apart.
  const queueReads: QueueRead[] = [];
  let queueFailure: AuthorityRefusal | null = null, routing: QueueRoutingCode = null;
  const sqlReads = new Map<string, SqlRead[]>(orgs.map((o) => [o, []]));
  const sqlFailure = new Map<string, AuthorityRefusal | null>(orgs.map((o) => [o, null]));
  const round = async (n: 1 | 2) => {
    if (queueFailure === null) {
      try { const q = await queueRead(src); queueReads.push(q.read); routing ??= q.routing; }
      catch (error) { queueFailure = codeOf(error, "queue_list_unreadable"); }
    }
    for (const org of orgs) {
      if (sqlFailure.get(org) !== null) continue;
      const anchor = stops.get(org)!.stop.effectiveStopMs;
      if (anchor === null) { sqlFailure.set(org, "sql_no_stop_anchor"); continue; }
      const b = bounds.get(org);
      const base = b && typeof b !== "string" ? b.snapshot : n === 2 ? sqlReads.get(org)![0]?.snapshot ?? null : null;
      try { sqlReads.get(org)!.push(await sqlRead(src, org, anchor, base)); }
      catch (error) { sqlFailure.set(org, codeOf(error, "sql_unreadable")); }
    }
  };
  let statsFailure: AuthorityRefusal | null = null, statsReference: SqlStats | null = null, statsSettled: SqlStats | null = null;
  try { statsReference = await statsRead(src); } catch (error) { statsFailure = codeOf(error, "sql_stats_unreadable"); }
  await round(1);
  const round1EndMs = src.nowMs();
  await src.sleep(windowMs);
  const round2StartMs = src.nowMs();
  await round(2);
  const round2EndMs = src.nowMs();
  const observed = Number.isFinite(round2StartMs - round1EndMs) ? round2StartMs - round1EndMs : null;
  // Wait AUTHORITY_STATS_SETTLE_MS, then read the deletion counter again. A heuristic delay: a delete committed before
  // round 2 is likely, not certain, to be reflected by then (AUTHORITY_STATS_SETTLE_MS).
  await src.sleep(AUTHORITY_STATS_SETTLE_MS);
  const settleStartMs = src.nowMs();
  if (statsFailure === null) {
    try { statsSettled = await statsRead(src); } catch (error) { statsFailure = codeOf(error, "sql_stats_unreadable"); }
  }
  const settleMs = Number.isFinite(settleStartMs - round2EndMs) ? settleStartMs - round2EndMs : null;

  // Same deployments after the window.
  let ingestChanged = false, rhChanged = false;
  if (ingest) {
    try {
      const again = parseDeployments(await cfResult(src, deploymentsPath(ingestWorker), "stop_deployments_unreadable"),
        1, "stop_deployment_invalid", "stop_deployment_order").list[0]!;
      ingestChanged = again.id !== ingest.list[0]!.id || again.serving.join() !== ingest.list[0]!.serving.join();
    } catch { ingestChanged = true; }
  }
  if (rh) {
    try {
      const again = parseDeployments(await cfResult(src, deploymentsPath(relayhistoryWorker), "turns_deployments_unreadable"),
        1, "turns_deployment_invalid", "turns_deployment_invalid").list[0]!;
      rhChanged = again.id !== rh.id || again.serving.join() !== rh.serving.join();
    } catch { rhChanged = true; }
  }

  const queueGlobal = queueVerdict(queueReads, routing, queueFailure, observed, windowMs);
  const verdicts = orgs.map((org): OrgAuthorityVerdict => {
    const orgHash = evidenceHash("org", org);
    let { stop } = stops.get(org)!;
    const { mirror, queueBound, routeKept } = stops.get(org)!;
    if (ingestChanged && stop.ok) stop = { ...stop, ok: false, code: "stop_deployment_changed" };
    let turnsNeon = turns.get(org)!;
    if (rhChanged && turnsNeon.ok) turnsNeon = { ...turnsNeon, ok: false, code: "turns_deployment_changed" };
    // The binding is checked on the versions this org's stop window and boundary cover; without a window, nothing is proven.
    let queueDrained = queueGlobal;
    if (queueGlobal.ok && stop.effectiveStopMs !== null) {
      if (!queueBound) queueDrained = { ...queueGlobal, ok: false, code: "queue_binding_mismatch" };
      else if (!routeKept) queueDrained = { ...queueGlobal, ok: false, code: "queue_route_changed" };
    }
    const sqlQuiet = sqlVerdict(sqlReads.get(org)!, sqlFailure.get(org)!, observed, windowMs, bounds.get(org)!, stop, startedAtMs,
      { reference: statsReference, settled: statsSettled, failure: statsFailure, settleMs });
    const checks = [stop, mirror, queueDrained, sqlQuiet, turnsNeon];
    const refusals = [...new Set(checks.map((c) => c.code).filter((c): c is AuthorityRefusal => c !== null))];
    const ok = refusals.length === 0 && checks.every((c) => c.ok);
    return { orgHash, ok, refusals, stop, mirror, queueDrained, sqlQuiet, turnsNeon };
  });
  const status: AuthorityEvidenceStatus = verdicts.every((v) => v.ok) ? "authority_evidence_corroborated"
    : options.bound === undefined && verdicts.every((v) => v.refusals.length === 1 && v.refusals[0] === "sql_unbound")
      ? "authority_evidence_unbound" : "authority_evidence_refused";
  return {
    status,
    orgCount: orgs.length,
    observationWindowMs: windowMs,
    startedAtMs,
    finishedAtMs: src.nowMs(),
    ingestDeploymentHash: ingest ? evidenceHash("deployment", ingest.list[0]!.id) : null,
    relayhistoryDeploymentHash: rh ? evidenceHash("deployment", rh.id) : null,
    orgs: verdicts,
  };
}
