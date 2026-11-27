/**
 * Live evidence for the per-org fence-authority marker (ledger-s3.ts validOrgFenceAuthority): what must hold, read
 * from production at the moment of the decision, before a backlog-ops authority mode may write an org's marker.
 * Decision 0005's cutover order (docs/selfhost-composition.md, "Stopping the Worker's HTTP admission for an org"):
 *
 *   1. stop          every traffic-serving version of 96bulls-session-ingest lists the org in
 *                    INGEST_HTTP_ADMISSION_STOPPED_ORGS, and the stop has held continuously since an identified
 *                    deployment (the one that established it). Its created_on is the effective stop time.
 *   2. mirror        every one of those versions also carries INGEST_S3_FENCE_MIRROR=on (the marker's controlMirror).
 *   3. queueDrained  the INGEST_ADMISSION queue's backlog is 0 (global: a message is opaque, so never per org) at two
 *                    reads an observation window apart, its dead-letter queue has no consumer (no replay path) and
 *                    the dead-letter backlog did not fall during the window (nothing drained it).
 *   4. sqlQuiet      read-only SQL for the org: no ingest_batches row claimed or committed, and no
 *                    ingest_record_revisions row updated, at or after the effective stop; no pending batch; and the
 *                    ownership and revision content fingerprints unchanged across the window. Zero uncommitted SQL
 *                    batches alone proves nothing about the queue, which is why (3) is separate.
 *   5. turnsNeon     every traffic-serving version of relayhistory-api resolves the org's turns write path to `neon`
 *                    with relayhistory-cloud's resolver (packages/relayhistory/src/lib/turns-forward.ts
 *                    turnsWritePath, mirrored exactly by resolveTurnsWritePath). An absent binding on a verified
 *                    version is the documented default (neon); a binding whose value cannot be read (a secret, a
 *                    duplicate, a non-text type) is UNKNOWN and refuses.
 *
 * Nothing here comes from an input except the org list (the caller's BACKLOG_OPS_ORGS scope, validated). The module
 * is pure: it builds every Cloudflare path and SQL text itself and takes only transports (AuthorityEvidenceSources),
 * so any response shape it does not recognise refuses with a fixed code; a guess about a provider shape can cost a
 * refusal, never a false pass. Fetcher errors are mapped to fixed codes, their messages discarded.
 *
 * Output is counts, booleans, epoch milliseconds, fixed codes and salted hashes (`h_` + 32 hex) only: no org,
 * deployment, version or queue id and no raw configuration value, so it fits a backlog-ops receipt (every string
 * leaf a short lowercase code).
 *
 * Limits, stated so nobody reads more into a PASS than it proves:
 *   - ingest_session_ownership has no timestamp, so "no ownership change since the stop" is proven only for the
 *     observation window (fingerprint equal at both reads). Its admission-path writer (the claim) is covered since
 *     the stop by ingest_batches.claimed_at.
 *   - The anchor is strict: a claim made at or after the effective stop by a version still propagating refuses.
 *     That is fail-closed by design; it is the owner's literal rule ("no new ingest_batches after the stop").
 *   - The Cloudflare deployments list holds a bounded history. When every retained deployment carries the stop, the
 *     oldest retained one is the anchor: the stop held continuously from then, so the SQL window is still valid
 *     (it is merely shorter than the true one).
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
} as const;

/** The most deployments walked back to find the one that established the stop. */
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
  "stop_cutover_unreadable", "stop_org_not_listed", "stop_deployment_changed", "mirror_off",
  // queue
  "queue_list_unreadable", "queue_list_truncated", "queue_missing", "queue_metrics_unreadable",
  "queue_backlog_nonzero", "dlq_unreadable", "dlq_consumer_attached", "dlq_backlog_decreased",
  // sql
  "sql_no_stop_anchor", "sql_unreadable", "sql_batches_claimed_after_stop", "sql_batches_committed_after_stop",
  "sql_batches_pending", "sql_revisions_after_stop", "sql_revisions_changed", "sql_ownership_changed",
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
  nowMs(): number;
  sleep(ms: number): Promise<void>;
}

export interface AuthorityEvidenceOptions {
  /** At least AUTHORITY_MIN_OBSERVATION_WINDOW_MS. */
  observationWindowMs?: number;
}

export interface Check { ok: boolean; code: AuthorityRefusal | null }
export interface StopCheck extends Check {
  servingVersions: number;
  deploymentsWalked: number;
  /** created_on of the deployment that established the stop (epoch ms), or null. */
  effectiveStopMs: number | null;
  stopDeploymentHash: string | null;
}
export interface QueueCheck extends Check { backlogReads: number[]; dlqBacklogReads: number[]; dlqConsumers: number | null }
export interface SqlCounts {
  batchesClaimedSince: number; batchesCommittedSince: number; batchesPending: number;
  revisionsUpdatedSince: number; revisionsCount: number; ownershipCount: number;
}
export interface SqlCheck extends Check { reads: number; counts: SqlCounts | null; revisionsStable: boolean | null; ownershipStable: boolean | null }
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

export interface AuthorityEvidenceReport {
  status: "authority_evidence_pass" | "authority_evidence_refused";
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
 * At most `limit` deployments are parsed; created_on must strictly decrease.
 */
export function parseDeployments(value: unknown, limit: number, invalid: AuthorityRefusal, order: AuthorityRefusal): ParsedDeployment[] {
  const list = isObj(value) ? value.deployments : undefined;
  if (!Array.isArray(list) || list.length === 0) refuse(invalid);
  const out: ParsedDeployment[] = [];
  for (const d of (list as unknown[]).slice(0, limit)) {
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
  return out;
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

async function stopCheck(
  org: string, deployments: ParsedDeployment[], version: (id: string) => Promise<CloudflareWorkerVersion>, nowMs: number,
): Promise<{ stop: StopCheck; mirror: Check }> {
  const current = deployments[0]!;
  if (!(current.createdMs <= nowMs)) return {
    stop: { ok: false, code: "stop_deployment_invalid", servingVersions: current.serving.length, deploymentsWalked: 0, effectiveStopMs: null, stopDeploymentHash: null },
    mirror: { ok: false, code: "stop_deployment_invalid" },
  };
  const stop: StopCheck = { ok: false, code: null, servingVersions: current.serving.length, deploymentsWalked: 0, effectiveStopMs: null, stopDeploymentHash: null };
  const mirror: Check = { ok: false, code: null };
  try {
    // The current deployment: every serving version carries the stop AND the mirror.
    let listed = true, mirrored = true;
    for (const id of current.serving) {
      const s = versionStop(await version(id), org);
      listed &&= s.listed; mirrored &&= s.mirror;
    }
    mirror.ok = mirrored; mirror.code = mirrored ? null : "mirror_off";
    if (!listed) return { stop: { ...stop, code: "stop_org_not_listed", deploymentsWalked: 1 }, mirror };
    // Walk back while every serving version still lists the org: the oldest such deployment established the stop.
    let anchor = current, walked = 1;
    // An older version that cannot be read (deleted, malformed cutover) ends the walk like one without the stop: the
    // anchor is then a later deployment, which only shortens the SQL window and never lets an unproven stop count.
    for (const d of deployments.slice(1)) {
      let all = true;
      for (const id of d.serving) {
        try { if (!versionStop(await version(id), org).listed) { all = false; break; } }
        catch { all = false; break; }
      }
      walked++;
      if (!all) break;
      anchor = d;
    }
    return {
      stop: { ...stop, ok: true, deploymentsWalked: walked, effectiveStopMs: anchor.createdMs, stopDeploymentHash: evidenceHash("deployment", anchor.id) },
      mirror,
    };
  } catch (error) {
    const code = codeOf(error, "stop_version_unreadable");
    return { stop: { ...stop, code }, mirror: mirror.code === null && !mirror.ok ? { ok: false, code } : mirror };
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

const count = (v: unknown): number | null => {
  if (typeof v === "number" && Number.isSafeInteger(v) && v >= 0) return v;
  if (typeof v === "string" && /^(0|[1-9][0-9]{0,15})$/u.test(v) && Number.isSafeInteger(Number(v))) return Number(v);
  return null;
};

interface QueueIds { main: string; dlq: string }
interface QueueRead { backlog: number; dlqBacklog: number; dlqConsumers: number }

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

async function queueRead(src: AuthorityEvidenceSources, ids: QueueIds): Promise<QueueRead> {
  const main = await backlog(src, ids.main, "queue_metrics_unreadable");
  const dlqBacklog = await backlog(src, ids.dlq, "dlq_unreadable");
  const dlq = await read("dlq_unreadable", () => src.cfGet(`/queues/${ids.dlq}`));
  if (!isObj(dlq) || dlq.queue_id !== ids.dlq || !Array.isArray(dlq.consumers)) refuse("dlq_unreadable");
  return { backlog: main, dlqBacklog, dlqConsumers: ((dlq as Record<string, unknown>).consumers as unknown[]).length };
}

function queueVerdict(reads: QueueRead[], failure: AuthorityRefusal | null): QueueCheck {
  const out: QueueCheck = {
    ok: false, code: failure, backlogReads: reads.map((r) => r.backlog), dlqBacklogReads: reads.map((r) => r.dlqBacklog),
    dlqConsumers: reads.length ? Math.max(...reads.map((r) => r.dlqConsumers)) : null,
  };
  if (failure) return out;
  if (reads.some((r) => r.backlog !== 0)) return { ...out, code: "queue_backlog_nonzero" };
  if (reads.some((r) => r.dlqConsumers !== 0)) return { ...out, code: "dlq_consumer_attached" };
  if (reads.some((r, i) => i > 0 && r.dlqBacklog < reads[i - 1]!.dlqBacklog)) return { ...out, code: "dlq_backlog_decreased" };
  return { ...out, ok: true };
}

// ---- SQL ------------------------------------------------------------------------------------------------------

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

function sqlVerdict(reads: SqlRead[], failure: AuthorityRefusal | null): SqlCheck {
  const last = reads[reads.length - 1];
  const counts: SqlCounts | null = last ? {
    batchesClaimedSince: Math.max(...reads.map((r) => r.batchesClaimedSince)),
    batchesCommittedSince: Math.max(...reads.map((r) => r.batchesCommittedSince)),
    batchesPending: Math.max(...reads.map((r) => r.batchesPending)),
    revisionsUpdatedSince: Math.max(...reads.map((r) => r.revisionsUpdatedSince)),
    revisionsCount: last.revisionsCount, ownershipCount: last.ownershipCount,
  } : null;
  const stable = (f: (r: SqlRead) => string) => reads.length < 2 ? null : reads.every((r) => f(r) === f(reads[0]!));
  const revisionsStable = stable((r) => `${r.revisionsCount}/${r.revisionsHash}`);
  const ownershipStable = stable((r) => `${r.ownershipCount}/${r.ownershipHash}`);
  const out: SqlCheck = { ok: false, code: failure, reads: reads.length, counts, revisionsStable, ownershipStable };
  if (failure || !counts) return { ...out, code: failure ?? "sql_unreadable" };
  if (counts.batchesClaimedSince) return { ...out, code: "sql_batches_claimed_after_stop" };
  if (counts.batchesCommittedSince) return { ...out, code: "sql_batches_committed_after_stop" };
  if (counts.batchesPending) return { ...out, code: "sql_batches_pending" };
  if (counts.revisionsUpdatedSince) return { ...out, code: "sql_revisions_after_stop" };
  if (revisionsStable !== true) return { ...out, code: "sql_revisions_changed" };
  if (ownershipStable !== true) return { ...out, code: "sql_ownership_changed" };
  return { ...out, ok: true };
}

// ---- the evidence run -----------------------------------------------------------------------------------------

/**
 * Read every piece of evidence for `orgs` and return one verdict per org (input order). Never throws for evidence;
 * throws only `authority_orgs` / `authority_window` for bad arguments. The caller writes a marker for an org only when
 * its verdict is ok AND the report's status is authority_evidence_pass (the report refuses as a whole if any org does).
 *
 * Order: ingest deployments (stop, mirror) -> relayhistory deployments (turns) -> queue read 1 -> SQL read 1 ->
 * wait the window -> queue read 2 -> SQL read 2 -> both deployments re-read; the current deployment must be the same
 * one (a deploy during the window could have dropped the stop or changed the turns path).
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

  // 1, 2: stop and mirror.
  let ingest: ParsedDeployment[] | null = null, ingestFailure: AuthorityRefusal | null = null;
  try {
    ingest = parseDeployments(await read("stop_deployments_unreadable", () => src.cfGet(deploymentsPath(ingestWorker))),
      AUTHORITY_MAX_DEPLOYMENT_WALK, "stop_deployment_invalid", "stop_deployment_order");
  } catch (error) { ingestFailure = codeOf(error, "stop_deployments_unreadable"); }
  const ingestVersion = versionReader(src, ingestWorker, "stop_version_unreadable");
  const stops = new Map<string, { stop: StopCheck; mirror: Check }>();
  for (const org of orgs) {
    stops.set(org, ingest ? await stopCheck(org, ingest, ingestVersion, startedAtMs) : {
      stop: { ok: false, code: ingestFailure, servingVersions: 0, deploymentsWalked: 0, effectiveStopMs: null, stopDeploymentHash: null },
      mirror: { ok: false, code: ingestFailure },
    });
  }

  // 5: turns.
  let rh: ParsedDeployment | null = null, rhFailure: AuthorityRefusal | null = null;
  try {
    rh = parseDeployments(await read("turns_deployments_unreadable", () => src.cfGet(deploymentsPath(relayhistoryWorker))),
      1, "turns_deployment_invalid", "turns_deployment_invalid")[0]!;
  } catch (error) { rhFailure = codeOf(error, "turns_deployments_unreadable"); }
  const rhVersion = versionReader(src, relayhistoryWorker, "turns_version_unreadable");
  const turns = new Map<string, TurnsCheck>();
  for (const org of orgs)
    turns.set(org, rh ? await turnsCheck(org, rh, rhVersion) : { ok: false, code: rhFailure, servingVersions: 0, path: null });

  // 3, 4: two rounds of queue + SQL reads, a window apart.
  const queueReads: QueueRead[] = [];
  let queueFailure: AuthorityRefusal | null = null, ids: QueueIds | null = null;
  const sqlReads = new Map<string, SqlRead[]>(orgs.map((o) => [o, []]));
  const sqlFailure = new Map<string, AuthorityRefusal | null>(orgs.map((o) => [o, null]));
  const round = async () => {
    if (queueFailure === null) {
      try { ids ??= await queueIds(src); queueReads.push(await queueRead(src, ids)); }
      catch (error) { queueFailure = codeOf(error, "queue_list_unreadable"); }
    }
    for (const org of orgs) {
      const anchor = stops.get(org)!.stop.effectiveStopMs;
      if (anchor === null) { sqlFailure.set(org, "sql_no_stop_anchor"); continue; }
      if (sqlFailure.get(org) !== null) continue;
      try { sqlReads.get(org)!.push(await sqlRead(src, org, anchor)); }
      catch (error) { sqlFailure.set(org, codeOf(error, "sql_unreadable")); }
    }
  };
  await round();
  await src.sleep(windowMs);
  await round();

  // Same deployments after the window.
  let ingestChanged = false, rhChanged = false;
  if (ingest) {
    try {
      const again = parseDeployments(await read("stop_deployments_unreadable", () => src.cfGet(deploymentsPath(ingestWorker))),
        1, "stop_deployment_invalid", "stop_deployment_order")[0]!;
      ingestChanged = again.id !== ingest[0]!.id || again.serving.join() !== ingest[0]!.serving.join();
    } catch { ingestChanged = true; }
  }
  if (rh) {
    try {
      const again = parseDeployments(await read("turns_deployments_unreadable", () => src.cfGet(deploymentsPath(relayhistoryWorker))),
        1, "turns_deployment_invalid", "turns_deployment_invalid")[0]!;
      rhChanged = again.id !== rh.id || again.serving.join() !== rh.serving.join();
    } catch { rhChanged = true; }
  }

  const queue = queueVerdict(queueReads, queueFailure);
  const verdicts = orgs.map((org): OrgAuthorityVerdict => {
    let { stop, mirror } = stops.get(org)!;
    if (ingestChanged && stop.ok) stop = { ...stop, ok: false, code: "stop_deployment_changed" };
    let turnsNeon = turns.get(org)!;
    if (rhChanged && turnsNeon.ok) turnsNeon = { ...turnsNeon, ok: false, code: "turns_deployment_changed" };
    const sqlQuiet = sqlVerdict(sqlReads.get(org)!, sqlFailure.get(org)!);
    const checks = [stop, mirror, queue, sqlQuiet, turnsNeon];
    const refusals = [...new Set(checks.map((c) => c.code).filter((c): c is AuthorityRefusal => c !== null))];
    const ok = refusals.length === 0 && checks.every((c) => c.ok);
    return { orgHash: evidenceHash("org", org), ok, refusals, stop, mirror, queueDrained: queue, sqlQuiet, turnsNeon };
  });
  return {
    status: verdicts.every((v) => v.ok) ? "authority_evidence_pass" : "authority_evidence_refused",
    orgCount: orgs.length,
    observationWindowMs: windowMs,
    startedAtMs,
    finishedAtMs: src.nowMs(),
    ingestDeploymentHash: ingest ? evidenceHash("deployment", ingest[0]!.id) : null,
    relayhistoryDeploymentHash: rh ? evidenceHash("deployment", rh.id) : null,
    orgs: verdicts,
  };
}
