/**
 * The backlog-ops authority modes (src/session-ingest/backlog-ops.ts; docs/backlog-ops-workflow.md "Authority"): the
 * last step of decision 0005's per-org cutover, writing an org's fence-authority marker (ledger-s3.ts
 * validOrgFenceAuthority) only on live evidence. Three modes, run by scripts/backlog-authority.ts:
 *
 *   reshard-dry        dry only, read role. The revision reshard's verify (revision-reshard.ts reshardRevisions with
 *                      verify), per scope org, against the effective revision home (the ledger bucket when
 *                      BACKLOG_OPS_LEDGER_* is set, routed exactly as the edge routes it, with the tool fork guard
 *                      assertToolLedgerHome first). Receipt: per org (hashed) verify_passed and the counts.
 *   authority-dry      the UNBOUND live evidence (authority-evidence.ts authorityEvidence without a baseline):
 *                      at best authority_evidence_unbound, which is what authorityBaseline takes.
 *   authority-execute  binds, at the same sha and scope, its DRY (an authority-dry), the catch-up that ran
 *                      authorityBaseline inside its input snapshot bound to that same DRY (BACKFILL), the FULL that
 *                      catch-up names, and a RESHARD whose verify passed for every org. Inside the gated job it re-reads
 *                      the live evidence BOUND to the catch-up's baseline (must be authority_evidence_corroborated),
 *                      then the re-arm sweeper's fresh pass for every org against the sweeper's serving spec, then,
 *                      per org and immediately before its write, the ENFORCED WRITER FENCE (writerFence), then writes
 *                      that org's marker CREATE-ONLY (If-None-Match) and reads it back. ANY object already there is
 *                      a `conflict`: never overwritten, never taken as this run's (reconciling an earlier or delayed
 *                      write needs the epoch-bound hold protocol, not built), and the run fails.
 *
 * The catch-up's baseline is a SAMPLED fingerprint baseline: one read in the catch-up's input fingerprint transaction.
 * The backfill's later pages and slot reads run in their own snapshots, so the baseline cannot attest the inputs the
 * backfill actually read; it is a sample at that instant, compared later (authority-evidence.ts header).
 *
 * The evidence is CORROBORATION ONLY (authority-evidence.ts header): observation cannot exclude an admission between
 * reads or after the last one, so it never authorises a marker by itself. Marker authority comes from an enforced,
 * durable per-org writer hold that admission paths must honour (being designed; not in this tree). Until it exists the
 * tool has no writerFence and refuses before any write (`writer_fence_unavailable`), and authority-execute is held
 * in code (backlog-ops.ts BACKLOG_OPS_AUTHORITY_EXECUTE_ENABLED) on top of that.
 *
 * The order is the evidence module's own: authority-dry (unbound, after the stop and the drain) -> catch-up with
 * the baseline in its snapshot (backfill-catchup-* with AUTHORITY <authority-dry run>) -> authority-execute bound to
 * that baseline -> marker.
 *
 * The sweeper's serving spec (REARM_SERVING_SPEC_KEY): what the CURRENT serving sweeper is, for freshSweeperPass's
 * expectation. The GitHub job has no kubectl, so it reads a document an in-cluster observer writes from the live
 * CronJob (its uid, suspend flag, the container's INGEST_REARM_ORGS / _BUCKET / _LEDGER_BUCKET and its image pinned by
 * digest). It is evidence only where a bucket-policy deny keeps every principal but that observer from writing the key
 * (not created here; docs/backlog-ops-workflow.md). Its homes are cross-checked against this run's own protected
 * variables (BACKLOG_OPS_BUCKET, BACKLOG_OPS_LEDGER_*), which the observer cannot set.
 *
 * Receipts are counts, codes and hashes only (backlog-ops countsOnly): the evidence module's camelCase output is
 * stored snake_case (toReceiptShape) and read back for the next run (fromReceiptShape). Errors carry fixed codes.
 */
import { createHash } from "node:crypto";
import { isObjectStoreError, objectKey, readAll, type ObjectStore } from "../shared/vendor.ts";
import {
  AUTHORITY_REFUSALS,
  authorityEvidence,
  evidenceHash,
  type AuthorityBaseline,
  type AuthorityEvidenceReport,
  type AuthorityEvidenceSources,
} from "./authority-evidence.ts";
import { orgFenceAuthorityKey, validOrgFenceAuthority, type OrgFenceAuthorityDoc } from "./ledger-s3.ts";
import { freshSweeperPass, type RearmPassExpectation } from "./rearm-sweeper-receipt.ts";
import { reshardRevisions, type ReshardSummary } from "./revision-reshard.ts";
import { isAccessDenied } from "./rearm-sweeper.ts";

export const BACKLOG_AUTHORITY_KEY_ROOT = "96bulls";
/** A sweeper pass older than this is not "live". */
export const BACKLOG_OPS_SWEEPER_PASS_MAX_AGE_MS = 6 * 60 * 60 * 1000;
/** The bound evidence must have finished this recently when a marker is written (re-checked before each write). */
export const BACKLOG_OPS_AUTHORITY_REPORT_MAX_AGE_MS = 5 * 60 * 1000;
/** The serving spec must have been observed this recently (the observer refreshes it every 10-15 min). */
export const BACKLOG_OPS_SWEEPER_SPEC_MAX_AGE_MS = 60 * 60 * 1000;
export const BACKLOG_OPS_SWEEPER_SPEC_FUTURE_SKEW_MS = 2 * 60 * 1000;
export const rearmServingSpecKey = (keyRoot: string) => `${keyRoot}/.ledger/rearm-sweeper/serving-spec.json`;

export class BacklogAuthorityRefusal extends Error {
  constructor(readonly code: string) { super(code); this.name = "BacklogAuthorityRefusal"; }
}
const refuse = (code: string): never => { throw new BacklogAuthorityRefusal(code); };

const isObj = (v: unknown): v is Record<string, any> => v !== null && typeof v === "object" && !Array.isArray(v);
const ORG_UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/u;

// ---- receipt shape: camelCase <-> snake_case, values untouched ------------------------------------------------

const toSnake = (k: string) => k.replace(/[A-Z]/gu, (c) => "_" + c.toLowerCase());
const toCamel = (k: string) => k.replace(/_([a-z0-9])/gu, (_, c: string) => c.toUpperCase());
function mapKeys(v: unknown, f: (k: string) => string): unknown {
  if (Array.isArray(v)) return v.map((x) => mapKeys(x, f));
  if (!isObj(v)) return v;
  return Object.fromEntries(Object.entries(v).map(([k, x]) => [f(k), mapKeys(x, f)]));
}
/** The evidence module's output as receipt keys (lowercase, underscores). Values are unchanged. */
export const toReceiptShape = (v: unknown): unknown => mapKeys(v, toSnake);
/** A receipt's stored evidence back into the module's shape (the inverse of toReceiptShape on its keys). */
export const fromReceiptShape = (v: unknown): unknown => mapKeys(v, toCamel);

/** The scope's org hashes (authority-evidence evidenceHash("org", ...)), sorted. */
export const orgHashes = (orgs: readonly string[]) => orgs.map((o) => evidenceHash("org", o)).sort();
const sameSet = (a: readonly string[], b: readonly string[]) => a.length === b.length && [...a].sort().every((x, i) => x === [...b].sort()[i]);

function parseReceipt(raw: string, code: string): Record<string, any> {
  let r: unknown;
  try { r = JSON.parse(raw); } catch { r = null; }
  if (!isObj(r) || !isObj(r.summary)) refuse(code);
  return r as Record<string, any>;
}

// ---- bound receipt checks (after backlog-ops checkBoundReceipt has authenticated mode, sha, run, scope, exit) ----

/** A reshard-dry receipt: verify_passed for exactly the scope's orgs, each with missing, invalid and failed 0. */
export function checkReshardReceipt(raw: string, orgs: readonly string[]): void {
  const s = parseReceipt(raw, "backlog_ops_reshard_not_verified").summary;
  const per = s.per_org;
  if (s.mode !== "dry_run" || s.verify_passed !== true || !Array.isArray(per) || s.org_count !== orgs.length ||
      !per.every((o: unknown) => isObj(o) && typeof o.org_hash === "string") ||
      !sameSet(per.map((o: Record<string, any>) => o.org_hash), orgHashes(orgs)) ||
      !per.every((o: Record<string, any>) => o.verify_passed === true && o.missing === 0 && o.invalid === 0 && o.failed === 0))
    refuse("backlog_ops_reshard_not_verified");
}

/**
 * An authority-dry receipt: the unbound evidence, authority_evidence_unbound, over exactly the scope's orgs (each
 * refused for sql_unbound alone). Returns the report in the evidence module's shape (authorityBaseline's input).
 */
export function checkAuthorityDryReceipt(raw: string, orgs: readonly string[]): AuthorityEvidenceReport {
  const s = parseReceipt(raw, "backlog_ops_authority_not_ready").summary;
  const report = fromReceiptShape(s.evidence) as Record<string, any>;
  if (s.mode !== "dry_run" || !isObj(report) || report.status !== "authority_evidence_unbound" || report.orgCount !== orgs.length ||
      !Array.isArray(report.orgs) || !report.orgs.every((o: unknown) => isObj(o) && typeof o.orgHash === "string") ||
      !sameSet(report.orgs.map((o: Record<string, any>) => o.orgHash), orgHashes(orgs)) ||
      !report.orgs.every((o: Record<string, any>) => Array.isArray(o.refusals) && o.refusals.length === 1 && o.refusals[0] === "sql_unbound"))
    refuse("backlog_ops_authority_not_ready");
  return report as unknown as AuthorityEvidenceReport;
}

/**
 * The baseline a catch-up recorded (its receipt's summary.authority_baseline): bound to `authorityRun` (the
 * receipt's top-level authority_run, written by the credential step from the dispatch, never by the tool), ready, over
 * exactly the scope's orgs. Returns it in the evidence module's shape (authorityEvidence's `bound`).
 */
export function checkBaselineReceipt(raw: string, orgs: readonly string[], authorityRun: string, fullRun: string): AuthorityBaseline {
  const r = parseReceipt(raw, "backlog_ops_bound_baseline_missing");
  if (r.authority_run !== authorityRun) refuse("backlog_ops_bound_baseline_run");
  // The exact FULL the catch-up followed (its t alone could alias two FULL runs created in the same second).
  if (r.full_run !== fullRun) refuse("backlog_ops_bound_baseline_full");
  const b = fromReceiptShape(r.summary.authority_baseline) as Record<string, any>;
  if (!isObj(b)) refuse("backlog_ops_bound_baseline_missing");
  if (b.kind !== "authority_baseline" || b.v !== 1 || b.status !== "authority_baseline_ready" || b.orgCount !== orgs.length ||
      !Array.isArray(b.orgs) || !b.orgs.every((o: unknown) => isObj(o) && typeof o.orgHash === "string" && o.ok === true) ||
      !sameSet(b.orgs.map((o: Record<string, any>) => o.orgHash), orgHashes(orgs)))
    refuse("backlog_ops_bound_baseline_not_ready");
  return b as unknown as AuthorityBaseline;
}

// ---- reshard-dry ------------------------------------------------------------------------------------------------

export interface ReshardVerifyOrg extends ReshardSummary { org_hash: string; verify_passed: boolean; refused: string | null }
/** revision-reshard.ts's own refusals; any other failure (a store error, whatever its message) is `reshard_failed`. */
export const RESHARD_REFUSALS = ["reshard_prefix_refused", "reshard_concurrency_refused", "reshard_mode_refused"] as const;
/** One verify per org (each its own prefix), never stopping early: every org gets a verdict. */
export async function reshardVerifyOrgs(store: ObjectStore, keyRoot: string, orgs: readonly string[], concurrency = 8) {
  const per: ReshardVerifyOrg[] = [];
  for (const org of orgs) {
    if (!ORG_UUID.test(org)) refuse("backlog_ops_reshard_org");
    const empty: ReshardSummary = { listed: 0, legacy_shards: 0, entries: 0, merged: 0, would_merge: 0, shards: 0, invalid: 0, failed: 0, verified: 0, missing: 0 };
    try {
      const s = await reshardRevisions(store, keyRoot, { prefix: `${keyRoot}/.admission-inbox/${org}/`, execute: false, verify: true, concurrency });
      per.push({ ...s, org_hash: evidenceHash("org", org), verify_passed: s.failed === 0 && s.invalid === 0 && s.missing === 0, refused: null });
    } catch (error) {
      const m = (error as Error)?.message;
      per.push({ ...empty, failed: 1, org_hash: evidenceHash("org", org), verify_passed: false,
        refused: (RESHARD_REFUSALS as readonly string[]).includes(m) ? m : "reshard_failed" });
    }
  }
  return { mode: "dry_run" as const, tool: "reshard_verify", org_count: orgs.length, verify_passed: per.every((p) => p.verify_passed), per_org: per };
}

// ---- the sweeper's serving spec ---------------------------------------------------------------------------------

export interface RearmServingSpec {
  v: 1; kind: "rearm-sweeper-serving-spec"; observedAtMs: number; namespace: string; cronJobName: string; cronJobUid: string;
  suspended: boolean; orgs: string[] | null; bucket: string; ledgerBucket: string | null; imageDigests: string[]; gitShas?: string[];
}
const DNS = /^[a-z0-9]([a-z0-9.-]{0,251}[a-z0-9])?$/u;
const UUID = ORG_UUID;
const BUCKET = /^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/u;
const SPEC_KEYS = ["v", "kind", "observedAtMs", "namespace", "cronJobName", "cronJobUid", "suspended", "orgs", "bucket", "ledgerBucket", "imageDigests", "gitShas"];
export function validServingSpec(v: unknown): v is RearmServingSpec {
  if (!isObj(v) || !Object.keys(v).every((k) => SPEC_KEYS.includes(k)) || !SPEC_KEYS.filter((k) => k !== "gitShas").every((k) => Object.hasOwn(v, k))) return false;
  const list = (l: unknown, re: RegExp, min: number) => Array.isArray(l) && l.length >= min && l.length <= 64 && l.every((x) => typeof x === "string" && re.test(x));
  return v.v === 1 && v.kind === "rearm-sweeper-serving-spec" && Number.isSafeInteger(v.observedAtMs) && v.observedAtMs > 0
    && typeof v.namespace === "string" && DNS.test(v.namespace) && typeof v.cronJobName === "string" && DNS.test(v.cronJobName)
    && typeof v.cronJobUid === "string" && UUID.test(v.cronJobUid) && typeof v.suspended === "boolean"
    && (v.orgs === null || list(v.orgs, ORG_UUID, 1)) && typeof v.bucket === "string" && BUCKET.test(v.bucket)
    && (v.ledgerBucket === null || (typeof v.ledgerBucket === "string" && BUCKET.test(v.ledgerBucket) && v.ledgerBucket !== v.bucket))
    && list(v.imageDigests, /^sha256:[0-9a-f]{64}$/u, 1) && (v.gitShas === undefined || list(v.gitShas, /^[0-9a-f]{40}$/u, 0));
}

/** Read the serving spec (null when absent). Unreadable or invalid refuses; denied access refuses. */
export async function readServingSpec(store: ObjectStore, keyRoot: string): Promise<RearmServingSpec | null> {
  let bytes: Uint8Array;
  try { bytes = await readAll((await store.getObject(objectKey(rearmServingSpecKey(keyRoot)))).body); }
  catch (error) {
    if (isObjectStoreError(error, "not_found")) return null;
    return refuse(isAccessDenied(error) ? "sweeper_spec_access_denied" : "sweeper_spec_unreadable");
  }
  let doc: unknown;
  try { doc = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes)); } catch { return refuse("sweeper_spec_invalid"); }
  if (!validServingSpec(doc)) refuse("sweeper_spec_invalid");
  return doc as RearmServingSpec;
}

/**
 * freshSweeperPass's expectation from the serving spec, cross-checked against this run's own protected variables (the
 * Locker bucket and the ledger target) and scope. Returns a refusal code instead when the spec cannot vouch.
 */
export function sweeperExpectation(spec: RearmServingSpec | null, run: { bucket: string; ledgerBucket: string | null; orgs: readonly string[] }, nowMs: number):
  RearmPassExpectation | string {
  if (spec === null) return "sweeper_spec_missing";
  const age = nowMs - spec.observedAtMs;
  if (age < -BACKLOG_OPS_SWEEPER_SPEC_FUTURE_SKEW_MS) return "sweeper_spec_future";
  if (age > BACKLOG_OPS_SWEEPER_SPEC_MAX_AGE_MS) return "sweeper_spec_stale";
  if (spec.suspended) return "sweeper_suspended";
  if (spec.bucket !== run.bucket || spec.ledgerBucket !== run.ledgerBucket) return "sweeper_home_mismatch";
  if (spec.orgs !== null && !run.orgs.every((o) => spec.orgs!.includes(o))) return "sweeper_out_of_scope";
  return { orgs: spec.orgs, bucket: spec.bucket, ledgerBucket: spec.ledgerBucket, writer: { namespace: spec.namespace, cronJobName: spec.cronJobName },
    revisions: { imageDigests: spec.imageDigests, ...(spec.gitShas?.length ? { gitShas: spec.gitShas } : {}) } };
}

// ---- the marker ---------------------------------------------------------------------------------------------------

export const orgFenceAuthorityDoc = (orgId: string, writtenAtMs: number): OrgFenceAuthorityDoc => ({
  v: 1, fenceBackfill: "complete", revisionBackfill: "complete", ownershipBackfill: "complete", controlMirror: "on", rearmSweeper: "live",
  revisionLayout: "4096", workerHttpAdmission: "off", writtenAtMs, orgId,
});
const conflict = (e: unknown) => isObjectStoreError(e, "precondition_failed") || isObjectStoreError(e, "already_exists");

async function readMarker(store: ObjectStore, key: string): Promise<{ doc: unknown } | null> {
  try {
    const bytes = await readAll((await store.getObject(objectKey(key))).body);
    try { return { doc: JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes)) }; } catch { return { doc: null }; }
  } catch (error) {
    if (isObjectStoreError(error, "not_found")) return null;
    return refuse(isAccessDenied(error) ? "marker_access_denied" : "marker_unreadable");
  }
}

export type MarkerOutcome = "written" | "conflict";
/**
 * One org's marker, CREATE-ONLY. ANY object already at the key is a `conflict` and is never overwritten (a valid
 * marker from an earlier or a delayed write cannot be told from this run's without the epoch-bound hold protocol).
 * A written marker is read back and must equal what was written (marker_readback_mismatch otherwise). Access denied
 * and other store errors refuse with fixed codes.
 */
export async function writeOrgMarker(store: ObjectStore, keyRoot: string, org: string, nowMs: number): Promise<MarkerOutcome> {
  if (!ORG_UUID.test(org)) refuse("marker_org");
  const key = orgFenceAuthorityKey(keyRoot, org), doc = orgFenceAuthorityDoc(org, nowMs);
  try {
    await store.put(objectKey(key), new TextEncoder().encode(JSON.stringify(doc)), { contentType: "application/json", ifNoneMatch: true });
  } catch (error) {
    if (!conflict(error)) return refuse(isAccessDenied(error) ? "marker_access_denied" : "marker_write_failed");
    return "conflict";
  }
  const back = await readMarker(store, key);
  if (!back || !validOrgFenceAuthority(back.doc) || back.doc.orgId !== org || back.doc.writtenAtMs !== nowMs) refuse("marker_readback_mismatch");
  return "written";
}

// ---- authority-execute ----------------------------------------------------------------------------------------------

export interface AuthorityExecuteDeps {
  orgs: readonly string[];
  sources: AuthorityEvidenceSources;
  baseline: AuthorityBaseline;
  /** The Locker (transcripts) store, SSE-KMS: the markers, the sweeper's spec and its pass receipts. */
  store: ObjectStore;
  keyRoot: string;
  /** This run's own protected homes (BACKLOG_OPS_BUCKET, the ledger target's bucket or null). */
  bucket: string;
  ledgerBucket: string | null;
  now(): number;
  observationWindowMs?: number;
  /** Tests only: the evidence run (default authority-evidence.ts authorityEvidence). */
  evidence?: typeof authorityEvidence;
  /**
   * A PLACEHOLDER seam for the enforced per-org writer hold: resolves true only when it proves, at `nowMs`, that the
   * org's admission is held by an enforced fence every admission path honours. A boolean cannot carry the epoch the
   * baseline, catch-up, marker and replay must share, nor reconcile a delayed PUT: the real protocol replaces this
   * seam before enablement (docs/backlog-ops-workflow.md "Authority"). NOT BUILT: absent, the run refuses before
   * any write (writer_fence_unavailable).
   */
  writerFence?: (org: string, nowMs: number) => Promise<boolean>;
}

/**
 * The bound evidence, then the sweeper, then per org the writer fence and the marker; nothing is written unless every
 * org passed the evidence and the sweeper and a writer fence exists. Counts, codes and hashes only. `status`:
 * authority_written (every org written), authority_refused (nothing written), authority_incomplete (a
 * marker conflict, a fence refusal or a lapsed report after some writes).
 */
export async function authorityExecute(d: AuthorityExecuteDeps) {
  const hashes = new Map(d.orgs.map((o) => [o, evidenceHash("org", o)]));
  const per = new Map(d.orgs.map((o) => [o, { org_hash: hashes.get(o)!, sweeper: "not_run", marker: "not_written" }]));
  const out = (status: string, report: AuthorityEvidenceReport | null, refusal: string | null) => {
    const list = [...per.values()];
    const n = (k: string) => list.filter((p) => p.marker === k).length;
    return { mode: "execute" as const, status, refused_code: refusal, org_count: d.orgs.length,
      evidence: report === null ? null : toReceiptShape(report),
      markers: { written: n("written"), conflict: n("conflict"), not_written: n("not_written") }, per_org: list };
  };
  const report = await (d.evidence ?? authorityEvidence)(d.orgs, d.sources, { bound: d.baseline, ...(d.observationWindowMs ? { observationWindowMs: d.observationWindowMs } : {}) });
  if (report.status !== "authority_evidence_corroborated") return out("authority_refused", report, "evidence_not_corroborated");
  // The sweeper, after the evidence (the freshest read before the writes); every org must pass before any write.
  let expectation: RearmPassExpectation | string;
  try { expectation = sweeperExpectation(await readServingSpec(d.store, d.keyRoot), { bucket: d.bucket, ledgerBucket: d.ledgerBucket, orgs: d.orgs }, d.now()); }
  catch (error) { expectation = error instanceof BacklogAuthorityRefusal ? error.code : "sweeper_spec_unreadable"; }
  if (typeof expectation === "string") {
    for (const p of per.values()) p.sweeper = expectation;
    return out("authority_refused", report, expectation);
  }
  for (const org of d.orgs) {
    let verdict: string;
    try {
      const r = await freshSweeperPass(d.store, d.keyRoot, org, d.now(), BACKLOG_OPS_SWEEPER_PASS_MAX_AGE_MS, expectation);
      verdict = r.ok ? "ok" : `sweeper_${r.reason}`;
    } catch (error) { verdict = (error as { code?: string })?.code === "access_denied" ? "sweeper_access_denied" : "sweeper_unreadable"; }
    per.get(org)!.sweeper = verdict;
  }
  const failed = [...per.values()].find((p) => p.sweeper !== "ok");
  if (failed) return out("authority_refused", report, failed.sweeper);
  // Corroboration is not authority: without an enforced writer fence nothing is written.
  if (d.writerFence === undefined) return out("authority_refused", report, "writer_fence_unavailable");
  const hashOk = new Map(report.orgs.map((o) => [o.orgHash, o.ok === true && o.refusals.length === 0]));
  // A stop after some writes leaves the run incomplete (those markers stay); before any, refused.
  const stopped = (code: string) => out([...per.values()].some((x) => x.marker !== "not_written") ? "authority_incomplete" : "authority_refused", report, code);
  for (const org of d.orgs) {
    const p = per.get(org)!;
    // Re-checked right before EACH write: the report is recent, the org's verdict ok, and the fence holds now.
    const age = d.now() - report.finishedAtMs;
    if (!(age >= 0 && age <= BACKLOG_OPS_AUTHORITY_REPORT_MAX_AGE_MS) || hashOk.get(hashes.get(org)!) !== true) return stopped("marker_handoff_lapsed");
    let fenced: boolean;
    try { fenced = await d.writerFence(org, d.now()); } catch { fenced = false; }
    if (!fenced) return stopped("writer_fence_not_held");
    try { p.marker = await writeOrgMarker(d.store, d.keyRoot, org, d.now()); }
    catch (error) { return out("authority_incomplete", report, error instanceof BacklogAuthorityRefusal ? error.code : "marker_write_failed"); }
  }
  const conflicted = [...per.values()].some((p) => p.marker === "conflict");
  return out(conflicted ? "authority_incomplete" : "authority_written", report, conflicted ? "marker_conflict" : null);
}

// ---- transports ---------------------------------------------------------------------------------------------------

/** A read-only Cloudflare API token's shape (never printed). */
export const CLOUDFLARE_TOKEN = /^[A-Za-z0-9_-]{32,256}$/u;
/**
 * cfGet for authority-evidence: GET the account path with the token; resolves the WHOLE parsed body (the module checks
 * success and result_info). The account id and origin are fixed; the path is the module's own.
 */
export function cloudflareGet(accountId: string, token: string, fetcher: typeof fetch = fetch): AuthorityEvidenceSources["cfGet"] {
  if (!/^[0-9a-f]{32}$/u.test(accountId) || !CLOUDFLARE_TOKEN.test(token)) refuse("cloudflare_config");
  return async (path: string) => {
    if (!path.startsWith("/") || path.includes("..") || path.includes("//")) refuse("cloudflare_path");
    const res = await fetcher(`https://api.cloudflare.com/client/v4/accounts/${accountId}${path}`, {
      headers: { Authorization: `Bearer ${token}`, Accept: "application/json" }, redirect: "error",
    });
    // A non-2xx body still parses (success false); a non-JSON body throws, which the module maps to a code.
    return await res.json();
  };
}

// ---- the closed strings of an authority receipt -------------------------------------------------------------------

/** scripts/backlog-authority.ts's own refusal codes (its `{"refused": ...}` line); anything else is internal_error. */
export const AUTHORITY_TOOL_REFUSALS = ["usage", "orgs", "authority_execute_held", "stdin_too_large", "stdin_invalid", "db_url_required",
  "cloudflare_token_invalid", "cloudflare_config", "cloudflare_path", "aws_credentials_required", "backlog_ops_reshard_org", "internal_error",
  "ledger_bucket_invalid", "ledger_home_guard_invalid", "ledger_home_mismatch", "ledger_home_missing", "ledger_home_moved",
  "ledger_home_invalid", "ledger_home_unavailable", "ledger_home_store_error", "access_denied"] as const;
/**
 * Every string an authority receipt may hold (beyond the hashes `h_<32 hex>` / `h<32 hex>`): the evidence module's
 * statuses, refusals and turns paths, and this module's own codes. countsOnly alone accepts any code-SHAPED string
 * (one from an error message, say); the credential step also requires this closed set before the receipt is logged.
 */
export const AUTHORITY_RECEIPT_STRINGS: ReadonlySet<string> = new Set<string>([
  ...AUTHORITY_REFUSALS, ...RESHARD_REFUSALS,
  "authority_evidence_corroborated", "authority_evidence_unbound", "authority_evidence_refused", "authority_baseline",
  "authority_baseline_ready", "authority_baseline_refused", "neon", "dual", "forward", "unknown",
  "dry_run", "execute", "reshard_verify", "reshard_failed", "authority_written", "authority_refused", "authority_incomplete",
  "ok", "not_run", "written", "conflict", "not_written", "evidence_not_corroborated", "writer_fence_unavailable", "writer_fence_not_held",
  "marker_handoff_lapsed", "marker_conflict", "marker_org", "marker_access_denied", "marker_unreadable", "marker_write_failed",
  "marker_readback_mismatch", "sweeper_spec_missing", "sweeper_spec_stale", "sweeper_spec_future", "sweeper_spec_invalid",
  "sweeper_spec_unreadable", "sweeper_spec_access_denied", "sweeper_suspended", "sweeper_home_mismatch", "sweeper_out_of_scope",
  "sweeper_invalid_request", "sweeper_missing", "sweeper_malformed", "sweeper_mismatch", "sweeper_stale", "sweeper_future",
  "sweeper_access_denied", "sweeper_unreadable", ...AUTHORITY_TOOL_REFUSALS,
]);
const HASH = /^h_?[0-9a-f]{32}$/u;
/** True when every string leaf of `v` is a hash or in AUTHORITY_RECEIPT_STRINGS. */
export function authorityStringsClosed(v: unknown, depth = 0): boolean {
  if (depth > 8) return false;
  if (typeof v === "string") return HASH.test(v) || AUTHORITY_RECEIPT_STRINGS.has(v);
  if (Array.isArray(v)) return v.every((x) => authorityStringsClosed(x, depth + 1));
  if (isObj(v)) return Object.values(v).every((x) => authorityStringsClosed(x, depth + 1));
  return true;
}

/** A stable short id of the receipt this run binds, for logs (never an org). */
export const bindingDigest = (raw: string) => "h_" + createHash("sha256").update(raw).digest("hex").slice(0, 32);
