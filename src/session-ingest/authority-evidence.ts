/**
 * Live evidence for the per-org fence-authority marker (ledger-s3.ts validOrgFenceAuthority): what must hold, read
 * from production at the moment of the decision, before a backlog-ops authority mode may write an org's marker.
 * Decision 0005's cutover order (docs/selfhost-composition.md, "Stopping the Worker's HTTP admission for an org"):
 *
 *   1. stop          every traffic-serving version of 96bulls-session-ingest lists the org in
 *                    INGEST_HTTP_ADMISSION_STOPPED_ORGS, continuously back to a PROVEN boundary: the walk back
 *                    through the deployments must reach a deployment whose serving versions are all readable and of
 *                    which at least one does not list the org. The deployment just after it established the stop; its
 *                    created_on is the effective stop. An unreadable version, an exhausted or bounded history, or any
 *                    other unproven boundary refuses (stop_boundary_unproven): a later anchor would hide a claim.
 *   2. mirror        every current serving version also carries INGEST_S3_FENCE_MIRROR=on (the marker's controlMirror).
 *   3. queueDrained  routing first: every serving version of every deployment since the stop binds INGEST_ADMISSION to
 *                    exactly the measured queue; that queue's only consumer is the ingest Worker, dead-lettering to
 *                    the measured DLQ, and its only producers are the ingest Worker; the DLQ has no consumer (no
 *                    replay path). Then the queue's backlog_count is 0 (global: a message is opaque, never per org)
 *                    at two reads at least the observation window apart, MEASURED on the injected clock; the queue
 *                    ids are the same at both reads. The DLQ backlog not falling is an auxiliary signal only: a replay
 *                    offset by arrivals would hide in it. What excludes a replay that reached admission is (4): any
 *                    admission, replayed or not, claims an ingest_batches row.
 *   4. sqlQuiet      read-only SQL for the org, after proving the reader sees every row of the three tables (SELECT
 *                    privilege and no row-level security filtering it: AUTHORITY_VISIBILITY_SQL), so a filtered read
 *                    can never pass as an empty one. No ingest_batches row claimed or committed, and no
 *                    ingest_record_revisions row updated, at or after the effective stop minus a clock-skew margin;
 *                    no pending batch; the revision and ownership fingerprints equal at both reads AND equal to a
 *                    BOUND baseline (below).
 *   5. turnsNeon     every traffic-serving version of relayhistory-api resolves the org's turns write path to `neon`
 *                    with relayhistory-cloud's resolver (packages/relayhistory/src/lib/turns-forward.ts
 *                    turnsWritePath, mirrored exactly by resolveTurnsWritePath). An absent binding on a verified
 *                    version is the documented default (neon); a binding whose value cannot be read (a secret, a
 *                    duplicate, a non-text type) is UNKNOWN and refuses.
 *
 * Bound baseline. ingest_session_ownership has no timestamp, so no read taken now can show it unchanged since the
 * stop. A run without `bound` is therefore at best a BASELINE (status authority_evidence_baseline: every check passed
 * except `sql_unbound`), never a pass. A later run binds that baseline's report (`options.bound`, the caller's gated,
 * downloaded receipt of an earlier run of this function): same stop (deployment hash and effective stop), baseline
 * taken after the stop and finished before this run started, and the revision and ownership fingerprints unchanged
 * since. What that proves: the SQL state did not change between the baseline and now. The caller must order baseline
 * -> backfills -> bound run, so anything that changed between the stop and the baseline was read by the backfills.
 *
 * Nothing here comes from an input except the org list (the caller's BACKLOG_OPS_ORGS scope, validated) and the bound
 * report (a receipt the caller's gate binds, as backlog-ops binds every other run). The module is pure: it builds every
 * Cloudflare path and SQL text itself and takes only transports (AuthorityEvidenceSources), so any response shape it
 * does not recognise refuses with a fixed code; a guess about a provider shape costs a refusal, never a false pass.
 * Fetcher errors are mapped to fixed codes, their messages discarded.
 *
 * Output is counts, booleans, epoch milliseconds, fixed codes, row-content hash sums and salted id hashes (`h_` + 32
 * hex) only: no org, deployment, version or queue id and no raw configuration value, so it fits a backlog-ops receipt.
 *
 * Fail-closed limits: the anchor is strict (a claim at or after the stop, e.g. during propagation, refuses for that
 * stop for good); the walk is bounded (a stop established before the retained history refuses); a user control during
 * the window, or between baseline and bound run, changes a fingerprint and refuses (retryable with a new baseline).
 */
import { createHash } from "node:crypto";
import { ADMISSION_DLQ, ADMISSION_QUEUE } from "../../stacks/cloudflare/session-ingest.ts";
import { PRIVATE_INGEST_RELEASE, liveCutover, type CloudflareWorkerVersion } from "./private-ingest-release.ts";

export const AUTHORITY_EVIDENCE = {
  accountId: PRIVATE_INGEST_RELEASE.accountId,
  ingestWorker: PRIVATE_INGEST_RELEASE.worker,
  relayhistoryWorker: PRIVATE_INGEST_RELEASE.relayhistoryWorker,
  admissionQueue: ADMISSION_QUEUE,
  admissionDlq: ADMISSION_DLQ,
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
/** The most orgs one evidence run evaluates (backlog-ops BACKLOG_OPS_MAX_ORGS). */
export const AUTHORITY_MAX_ORGS = 64;
/** Queues listed per page; a full page means the list may be truncated, which refuses. */
export const AUTHORITY_QUEUE_PAGE = 100;

export const AUTHORITY_REFUSALS = [
  // stop / mirror (ingest Worker)
  "stop_deployments_unreadable", "stop_deployment_invalid", "stop_deployment_order", "stop_version_unreadable",
  "stop_cutover_unreadable", "stop_org_not_listed", "stop_boundary_unproven", "stop_deployment_changed", "mirror_off",
  // queue
  "queue_list_unreadable", "queue_list_truncated", "queue_missing", "queue_identity_changed", "queue_binding_mismatch",
  "queue_consumer_mismatch", "queue_producer_mismatch", "queue_metrics_unreadable", "queue_backlog_nonzero",
  "dlq_unreadable", "dlq_consumer_attached", "dlq_backlog_decreased", "observation_window_short",
  // sql
  "sql_no_stop_anchor", "sql_unreadable", "sql_rows_not_visible", "sql_batches_claimed_after_stop",
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
  /** GET https://api.cloudflare.com/client/v4/accounts/<AUTHORITY_EVIDENCE.accountId><path>; resolves the body's `result`. */
  cfGet(path: string): Promise<unknown>;
  /** One read-only statement (the caller's READ ONLY transaction, the backlog-ops dry role); resolves its rows. */
  sql(text: string, params: readonly string[]): Promise<readonly Record<string, unknown>[]>;
  /** The real clock: the window is measured on it, never assumed from sleep. */
  nowMs(): number;
  sleep(ms: number): Promise<void>;
}

export interface AuthorityEvidenceOptions {
  /** At least AUTHORITY_MIN_OBSERVATION_WINDOW_MS. */
  observationWindowMs?: number;
  /** An earlier report of this function (the baseline), bound by the caller's gate. Absent: a baseline run. */
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
export interface SqlFingerprint { revisionsCount: number; revisionsHash: number; ownershipCount: number; ownershipHash: number }
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

export type AuthorityEvidenceStatus = "authority_evidence_pass" | "authority_evidence_baseline" | "authority_evidence_refused";
export interface AuthorityEvidenceReport {
  /** pass: bound and every check ok (the only status a marker may be written on). baseline: unbound, every check ok. */
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
      p = read(code, async () => verifiedVersion(await src.cfGet(versionPath(worker, id)), id, code));
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

interface StopResult { stop: StopCheck; mirror: Check; queueBound: boolean }

async function stopCheck(
  org: string, deployments: { list: ParsedDeployment[]; complete: boolean },
  version: (id: string) => Promise<CloudflareWorkerVersion>, nowMs: number,
): Promise<StopResult> {
  const list = deployments.list, current = list[0]!;
  const stop: StopCheck = { ok: false, code: null, servingVersions: current.serving.length, deploymentsWalked: 0, stopDeployments: 0,
    effectiveStopMs: null, stopDeploymentHash: null };
  const fail = (code: AuthorityRefusal, mirror: Check = { ok: false, code }, walked = 0): StopResult =>
    ({ stop: { ...stop, code, deploymentsWalked: walked }, mirror, queueBound: false });
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
        return {
          stop: { ...stop, ok: true, deploymentsWalked: i + 1, stopDeployments: anchor + 1, effectiveStopMs: list[anchor]!.createdMs,
            stopDeploymentHash: evidenceHash("deployment", list[anchor]!.id) },
          mirror, queueBound,
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

interface QueueIds { main: string; dlq: string }
interface QueueRead { ids: QueueIds; backlog: number; dlqBacklog: number; dlqConsumers: number }
type QueueRoutingCode = "queue_consumer_mismatch" | "queue_producer_mismatch" | null;

async function queueIds(src: AuthorityEvidenceSources): Promise<QueueIds> {
  const list = await read("queue_list_unreadable", () => src.cfGet(`/queues?page=1&per_page=${AUTHORITY_QUEUE_PAGE}`));
  if (!Array.isArray(list)) refuse("queue_list_unreadable");
  const rows = list as unknown[];
  if (rows.length >= AUTHORITY_QUEUE_PAGE) refuse("queue_list_truncated");
  const find = (name: string) => {
    const hits = rows.filter((q) => isObj(q) && q.queue_name === name);
    if (hits.length !== 1) refuse("queue_missing");
    const id = (hits[0] as Record<string, unknown>).queue_id;
    if (typeof id !== "string" || !/^[0-9a-f]{32}$/u.test(id)) refuse("queue_list_unreadable");
    return id as string;
  };
  return { main: find(AUTHORITY_EVIDENCE.admissionQueue), dlq: find(AUTHORITY_EVIDENCE.admissionDlq) };
}

/** GET /queues/<id>/metrics: `backlog_count` must be a non-negative integer. */
async function backlog(src: AuthorityEvidenceSources, id: string, code: AuthorityRefusal): Promise<number> {
  const m = await read(code, () => src.cfGet(`/queues/${id}/metrics`));
  const n = isObj(m) ? count(m.backlog_count) : null;
  return n === null ? refuse(code) : n;
}

/** A consumer or producer is the ingest Worker: type worker, its script named by `script` and/or `script_name`. */
function isIngestWorker(x: unknown): boolean {
  if (!isObj(x) || x.type !== "worker") return false;
  const names = [x.script, x.script_name].filter((n) => n !== undefined);
  return names.length > 0 && names.every((n) => n === AUTHORITY_EVIDENCE.ingestWorker);
}
const dlqOf = (c: Record<string, unknown>): unknown =>
  c.dead_letter_queue ?? (isObj(c.settings) ? c.settings.dead_letter_queue : undefined);

/** GET /queues/<main>: its one consumer is the ingest Worker dead-lettering to the measured DLQ; its producers are the Worker. */
async function queueRouting(src: AuthorityEvidenceSources, ids: QueueIds): Promise<QueueRoutingCode> {
  const q = await read("queue_list_unreadable", () => src.cfGet(`/queues/${ids.main}`));
  if (!isObj(q) || q.queue_id !== ids.main || q.queue_name !== AUTHORITY_EVIDENCE.admissionQueue) refuse("queue_list_unreadable");
  const queue = q as Record<string, unknown>;
  const consumers = queue.consumers, producers = queue.producers;
  if (!Array.isArray(consumers) || consumers.length !== 1 || !isIngestWorker(consumers[0]) ||
      dlqOf(consumers[0] as Record<string, unknown>) !== AUTHORITY_EVIDENCE.admissionDlq) return "queue_consumer_mismatch";
  if (!Array.isArray(producers) || producers.length === 0 || !producers.every(isIngestWorker)) return "queue_producer_mismatch";
  return null;
}

async function queueRead(src: AuthorityEvidenceSources): Promise<{ read: QueueRead; routing: QueueRoutingCode }> {
  const ids = await queueIds(src);
  const routing = await queueRouting(src, ids);
  const main = await backlog(src, ids.main, "queue_metrics_unreadable");
  const dlqBacklog = await backlog(src, ids.dlq, "dlq_unreadable");
  const dlq = await read("dlq_unreadable", () => src.cfGet(`/queues/${ids.dlq}`));
  if (!isObj(dlq) || dlq.queue_id !== ids.dlq || dlq.queue_name !== AUTHORITY_EVIDENCE.admissionDlq || !Array.isArray(dlq.consumers))
    refuse("dlq_unreadable");
  return { read: { ids, backlog: main, dlqBacklog, dlqConsumers: ((dlq as Record<string, unknown>).consumers as unknown[]).length }, routing };
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
 * Whether the current role sees EVERY row of each table: SELECT privilege, and row-level security not applying to it
 * (RLS off on the table, or the role is a superuser, has BYPASSRLS, or holds the owner's privileges on a table that
 * does not FORCE RLS: Postgres's own check_enable_rls). A role that RLS filters would read zero rows as an empty org.
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
  AND c.relname IN ('ingest_batches', 'ingest_record_revisions', 'ingest_session_ownership')
ORDER BY c.relname`;

async function sqlVisible(src: AuthorityEvidenceSources): Promise<void> {
  const rows = await read("sql_unreadable", () => src.sql(AUTHORITY_VISIBILITY_SQL, []));
  if (!Array.isArray(rows)) refuse("sql_unreadable");
  const seen = new Map<string, unknown>();
  for (const r of rows) if (isObj(r) && typeof r.table_name === "string") seen.set(r.table_name, r.visible);
  if (rows.length !== AUTHORITY_SQL_TABLES.length || seen.size !== AUTHORITY_SQL_TABLES.length ||
      !AUTHORITY_SQL_TABLES.every((t) => seen.has(t))) refuse("sql_unreadable");
  if (!AUTHORITY_SQL_TABLES.every((t) => seen.get(t) === true)) refuse("sql_rows_not_visible");
}

const MOD = "1099511627776"; // 2^40, as upload-fence-backfill.ts FINGERPRINT_HASH_MODULUS
const hashSum = (row: string) => `(((coalesce(sum(hashtext(${row})::bigint), 0) % ${MOD}) + ${MOD}) % ${MOD})::bigint`;

/**
 * One read-only statement per org: one org-leading scan of each table (each primary key leads with org_id), the
 * counts as FILTERs over it. $1 the org id, $2 the SQL anchor in epoch ms (decimal text): the effective stop minus
 * AUTHORITY_CLOCK_SKEW_MS. Timestamps compare `>=`, so a row stamped at the anchor's own instant counts against it.
 */
export const AUTHORITY_SQL = `
WITH s AS (SELECT to_timestamp($2::bigint / 1000.0) AS at),
b AS (
  SELECT count(*) FILTER (WHERE x.claimed_at >= s.at)::bigint AS claimed_since,
         count(*) FILTER (WHERE x.committed_at >= s.at)::bigint AS committed_since,
         count(*) FILTER (WHERE x.state = 'pending')::bigint AS pending
  FROM sessions.ingest_batches x CROSS JOIN s WHERE x.org_id = $1::text),
r AS (
  SELECT count(*) FILTER (WHERE x.updated_at >= s.at)::bigint AS updated_since, count(*)::bigint AS n,
         ${hashSum("concat_ws(E'\\t', x.workspace_id, x.origin_id, x.record_id, x.revision::text, coalesce(x.revision_id, '~'), coalesce(x.digest, '~'), x.user_id, x.source, coalesce(x.session_id, '~'), x.updated_at::text)")} AS h
  FROM sessions.ingest_record_revisions x CROSS JOIN s WHERE x.org_id = $1::text),
w AS (
  SELECT count(*)::bigint AS n, ${hashSum("concat_ws(E'\\t', x.workspace_id, x.user_id, x.origin_id, x.source, x.session_id)")} AS h
  FROM sessions.ingest_session_ownership x WHERE x.org_id = $1::text)
SELECT b.claimed_since AS batches_claimed_since, b.committed_since AS batches_committed_since, b.pending AS batches_pending,
       r.updated_since AS revisions_updated_since, r.n AS revisions_count, r.h AS revisions_hash,
       w.n AS ownership_count, w.h AS ownership_hash
FROM b, r, w`;

interface SqlRead extends SqlCounts { revisionsHash: number; ownershipHash: number }

async function sqlRead(src: AuthorityEvidenceSources, org: string, stopMs: number): Promise<SqlRead> {
  const rows = await read("sql_unreadable", () => src.sql(AUTHORITY_SQL, [org, String(stopMs - AUTHORITY_CLOCK_SKEW_MS)]));
  if (!Array.isArray(rows) || rows.length !== 1 || !isObj(rows[0])) refuse("sql_unreadable");
  const r = rows[0] as Record<string, unknown>;
  const keys = ["batches_claimed_since", "batches_committed_since", "batches_pending", "revisions_updated_since", "revisions_count",
    "revisions_hash", "ownership_count", "ownership_hash"] as const;
  const v = keys.map((k) => count(r[k]));
  if (v.some((n) => n === null)) refuse("sql_unreadable");
  const [a, b, c, d, e, f, g, h] = v as number[];
  return { batchesClaimedSince: a!, batchesCommittedSince: b!, batchesPending: c!, revisionsUpdatedSince: d!, revisionsCount: e!,
    revisionsHash: f!, ownershipCount: g!, ownershipHash: h! };
}

const fingerprintOf = (r: SqlRead): SqlFingerprint =>
  ({ revisionsCount: r.revisionsCount, revisionsHash: r.revisionsHash, ownershipCount: r.ownershipCount, ownershipHash: r.ownershipHash });

/** The bound baseline for one org, from an earlier report; a code when it cannot serve. */
type BoundOrg = { fingerprint: SqlFingerprint; effectiveStopMs: number; stopDeploymentHash: string; startedAtMs: number; finishedAtMs: number };
function boundOrg(bound: unknown, orgHash: string): BoundOrg | AuthorityRefusal {
  if (!isObj(bound) || !Array.isArray(bound.orgs) || count(bound.startedAtMs) === null || count(bound.finishedAtMs) === null ||
      (bound.status !== "authority_evidence_baseline" && bound.status !== "authority_evidence_pass")) return "bound_invalid";
  const hits = (bound.orgs as unknown[]).filter((o) => isObj(o) && o.orgHash === orgHash);
  if (hits.length !== 1) return "bound_invalid";
  const o = hits[0] as Record<string, unknown>;
  const stop = o.stop, sqlQ = o.sqlQuiet, refusals = o.refusals;
  if (!isObj(stop) || !isObj(sqlQ) || !Array.isArray(refusals) || !isObj(sqlQ.fingerprint)) return "bound_invalid";
  const fp = sqlQ.fingerprint as Record<string, unknown>;
  const values = [fp.revisionsCount, fp.revisionsHash, fp.ownershipCount, fp.ownershipHash].map(count);
  if (values.some((n) => n === null) || count(stop.effectiveStopMs) === null || typeof stop.stopDeploymentHash !== "string") return "bound_invalid";
  // Its only open refusal may be its own missing binding: every other check passed when it was taken.
  if (!refusals.every((c) => c === "sql_unbound") || stop.ok !== true) return "bound_not_ready";
  const [revisionsCount, revisionsHash, ownershipCount, ownershipHash] = values as number[];
  return { fingerprint: { revisionsCount: revisionsCount!, revisionsHash: revisionsHash!, ownershipCount: ownershipCount!, ownershipHash: ownershipHash! },
    effectiveStopMs: count(stop.effectiveStopMs)!, stopDeploymentHash: stop.stopDeploymentHash as string,
    startedAtMs: count(bound.startedAtMs)!, finishedAtMs: count(bound.finishedAtMs)! };
}

const sameFp = (a: SqlFingerprint, b: SqlFingerprint, k: "revisions" | "ownership") =>
  k === "revisions" ? a.revisionsCount === b.revisionsCount && a.revisionsHash === b.revisionsHash
    : a.ownershipCount === b.ownershipCount && a.ownershipHash === b.ownershipHash;

function sqlVerdict(
  reads: SqlRead[], failure: AuthorityRefusal | null, observed: number | null, windowMs: number,
  bound: BoundOrg | AuthorityRefusal | null, stop: StopCheck, startedAtMs: number,
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
  if (counts.batchesClaimedSince) return { ...out, code: "sql_batches_claimed_after_stop" };
  if (counts.batchesCommittedSince) return { ...out, code: "sql_batches_committed_after_stop" };
  if (counts.batchesPending) return { ...out, code: "sql_batches_pending" };
  if (counts.revisionsUpdatedSince) return { ...out, code: "sql_revisions_after_stop" };
  if (revisionsStable !== true) return { ...out, code: "sql_revisions_changed" };
  if (ownershipStable !== true) return { ...out, code: "sql_ownership_changed" };
  if (bound === null) return { ...out, code: "sql_unbound" };
  if (typeof bound === "string") return { ...out, code: bound };
  if (bound.finishedAtMs > startedAtMs) return { ...out, code: "bound_not_ready" };
  if (bound.effectiveStopMs !== stop.effectiveStopMs || bound.stopDeploymentHash !== stop.stopDeploymentHash ||
      bound.startedAtMs < bound.effectiveStopMs) return { ...out, code: "bound_stop_mismatch" };
  if (!fps.every((f) => sameFp(f, bound.fingerprint, "revisions"))) return { ...out, code: "sql_revisions_changed" };
  if (!fps.every((f) => sameFp(f, bound.fingerprint, "ownership"))) return { ...out, code: "sql_ownership_changed" };
  return { ...out, ok: true, bound: true };
}

// ---- the evidence run -----------------------------------------------------------------------------------------

/**
 * Read every piece of evidence for `orgs` and return one verdict per org (input order). Never throws for evidence;
 * throws only `authority_orgs` / `authority_window` for bad arguments. A marker may be written for an org only when the
 * report's status is authority_evidence_pass (bound, every org ok); authority_evidence_baseline is the receipt a later
 * run binds, never authority.
 *
 * Order: ingest deployments (stop, mirror, queue binding) -> relayhistory deployments (turns) -> round 1 (queue
 * routing + backlog, SQL visibility, SQL per org) -> wait -> round 2 -> both deployments re-read (the current one must
 * be unchanged). The window is the injected clock's elapsed time from the end of round 1 to the start of round 2.
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

  // 1, 2 (and the queue binding of every version that served since the stop).
  let ingest: { list: ParsedDeployment[]; complete: boolean } | null = null, ingestFailure: AuthorityRefusal | null = null;
  try {
    ingest = parseDeployments(await read("stop_deployments_unreadable", () => src.cfGet(deploymentsPath(ingestWorker))),
      AUTHORITY_MAX_DEPLOYMENT_WALK, "stop_deployment_invalid", "stop_deployment_order");
  } catch (error) { ingestFailure = codeOf(error, "stop_deployments_unreadable"); }
  const ingestVersion = versionReader(src, ingestWorker, "stop_version_unreadable");
  const stops = new Map<string, StopResult>();
  for (const org of orgs) {
    stops.set(org, ingest ? await stopCheck(org, ingest, ingestVersion, startedAtMs) : {
      stop: { ok: false, code: ingestFailure, servingVersions: 0, deploymentsWalked: 0, stopDeployments: 0, effectiveStopMs: null, stopDeploymentHash: null },
      mirror: { ok: false, code: ingestFailure }, queueBound: false,
    });
  }

  // 5: turns.
  let rh: ParsedDeployment | null = null, rhFailure: AuthorityRefusal | null = null;
  try {
    rh = parseDeployments(await read("turns_deployments_unreadable", () => src.cfGet(deploymentsPath(relayhistoryWorker))),
      1, "turns_deployment_invalid", "turns_deployment_invalid").list[0]!;
  } catch (error) { rhFailure = codeOf(error, "turns_deployments_unreadable"); }
  const rhVersion = versionReader(src, relayhistoryWorker, "turns_version_unreadable");
  const turns = new Map<string, TurnsCheck>();
  for (const org of orgs)
    turns.set(org, rh ? await turnsCheck(org, rh, rhVersion) : { ok: false, code: rhFailure, servingVersions: 0, path: null });

  // 3, 4: two rounds a measured window apart.
  const queueReads: QueueRead[] = [];
  let queueFailure: AuthorityRefusal | null = null, routing: QueueRoutingCode = null;
  const sqlReads = new Map<string, SqlRead[]>(orgs.map((o) => [o, []]));
  const sqlFailure = new Map<string, AuthorityRefusal | null>(orgs.map((o) => [o, null]));
  const round = async () => {
    if (queueFailure === null) {
      try { const q = await queueRead(src); queueReads.push(q.read); routing ??= q.routing; }
      catch (error) { queueFailure = codeOf(error, "queue_list_unreadable"); }
    }
    let visible: AuthorityRefusal | null = null;
    try { await sqlVisible(src); } catch (error) { visible = codeOf(error, "sql_unreadable"); }
    for (const org of orgs) {
      if (sqlFailure.get(org) !== null) continue;
      const anchor = stops.get(org)!.stop.effectiveStopMs;
      if (anchor === null) { sqlFailure.set(org, "sql_no_stop_anchor"); continue; }
      if (visible) { sqlFailure.set(org, visible); continue; }
      try { sqlReads.get(org)!.push(await sqlRead(src, org, anchor)); }
      catch (error) { sqlFailure.set(org, codeOf(error, "sql_unreadable")); }
    }
  };
  await round();
  const round1EndMs = src.nowMs();
  await src.sleep(windowMs);
  const round2StartMs = src.nowMs();
  await round();
  const observed = Number.isFinite(round2StartMs - round1EndMs) ? round2StartMs - round1EndMs : null;

  // Same deployments after the window.
  let ingestChanged = false, rhChanged = false;
  if (ingest) {
    try {
      const again = parseDeployments(await read("stop_deployments_unreadable", () => src.cfGet(deploymentsPath(ingestWorker))),
        1, "stop_deployment_invalid", "stop_deployment_order").list[0]!;
      ingestChanged = again.id !== ingest.list[0]!.id || again.serving.join() !== ingest.list[0]!.serving.join();
    } catch { ingestChanged = true; }
  }
  if (rh) {
    try {
      const again = parseDeployments(await read("turns_deployments_unreadable", () => src.cfGet(deploymentsPath(relayhistoryWorker))),
        1, "turns_deployment_invalid", "turns_deployment_invalid").list[0]!;
      rhChanged = again.id !== rh.id || again.serving.join() !== rh.serving.join();
    } catch { rhChanged = true; }
  }

  const queueGlobal = queueVerdict(queueReads, routing, queueFailure, observed, windowMs);
  const verdicts = orgs.map((org): OrgAuthorityVerdict => {
    const orgHash = evidenceHash("org", org);
    let { stop } = stops.get(org)!;
    const { mirror, queueBound } = stops.get(org)!;
    if (ingestChanged && stop.ok) stop = { ...stop, ok: false, code: "stop_deployment_changed" };
    let turnsNeon = turns.get(org)!;
    if (rhChanged && turnsNeon.ok) turnsNeon = { ...turnsNeon, ok: false, code: "turns_deployment_changed" };
    // The binding is checked on the versions this org's stop window covers; without a window there is nothing proven.
    const queueDrained: QueueCheck = queueGlobal.ok && !queueBound && stop.effectiveStopMs !== null
      ? { ...queueGlobal, ok: false, code: "queue_binding_mismatch" } : queueGlobal;
    const bound = options.bound === undefined ? null : boundOrg(options.bound, orgHash);
    const sqlQuiet = sqlVerdict(sqlReads.get(org)!, sqlFailure.get(org)!, observed, windowMs, bound, stop, startedAtMs);
    const checks = [stop, mirror, queueDrained, sqlQuiet, turnsNeon];
    const refusals = [...new Set(checks.map((c) => c.code).filter((c): c is AuthorityRefusal => c !== null))];
    const ok = refusals.length === 0 && checks.every((c) => c.ok);
    return { orgHash, ok, refusals, stop, mirror, queueDrained, sqlQuiet, turnsNeon };
  });
  const status: AuthorityEvidenceStatus = verdicts.every((v) => v.ok) ? "authority_evidence_pass"
    : options.bound === undefined && verdicts.every((v) => v.refusals.length === 1 && v.refusals[0] === "sql_unbound")
      ? "authority_evidence_baseline" : "authority_evidence_refused";
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
