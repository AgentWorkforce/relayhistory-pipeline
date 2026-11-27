/**
 * Per-org PASS RECEIPTS of the S3 ledger re-arm sweeper (rearm-sweeper.ts): durable evidence that the sweeper is
 * live for an org, for the fence-authority marker's `rearmSweeper: "live"` precondition (ledger-s3.ts
 * FenceAuthorityDoc). The rearm sweeper's cursor says where a scope stands, and its completion records say a marker
 * was handled; neither says "a full, clean pass over this org finished at T". A receipt says exactly that.
 *
 *   <keyRoot>/.ledger/rearm-sweeper/passes/<org>/<startedAtMs>.json   RearmPassReceipt, create-only (If-None-Match)
 *   <keyRoot>/.ledger/rearm-sweeper/passes/<org>/latest.json          RearmPassLatest, CAS (If-Match / If-None-Match),
 *                                                                     only ever moved to a LATER pass
 *
 * `<org>` is a tenant org id (INGEST_REARM_ORGS), or `.all` for a sweeper scoped to every org (an org id never
 * starts with a dot). The keys sit beside the sweeper's cursor (`.ledger/rearm-sweeper/`), so they live in the same
 * store and need no grant the sweeper does not already hold (stacks/aws/rearm-sweeper-policy.ts
 * RearmSweeperReads / RearmSweeperWritesProgress on `96bulls/.ledger/rearm-sweeper/*`).
 *
 * A receipt is written for a pass ONLY when all of these hold (rearmPassReceiptEligibility); otherwise nothing is:
 *   - the sweeper is enabled and the pass is not a dry run;
 *   - the pass started at the beginning of its scope (the saved cursor said "start over"): a pass that continued a
 *     previous, incomplete one did not itself cover the whole scope;
 *   - it listed every scope to its end (`complete`), with no failed marker and no failure code;
 *   - its cursor saves were its own (`cursor: "saved"`, not `"contended"` by another sweeper).
 * A multi-org pass with any failure writes no receipt for any of its orgs (failures are not attributed per org).
 *
 * Counts and codes only: the receipt holds no key, marker, session, user or workspace id. The buckets are hashed.
 * The org id appears only in the key (its own prefix), never in a log line.
 *
 * Provenance (optional, recorded when present and well formed, dropped otherwise; never a reason to refuse a pass):
 *   GITHUB_SHA                        the code revision (40 hex), as the image build stamps it
 *   INGEST_REARM_IMAGE_DIGEST         the image digest (`sha256:<64 hex>`)
 *   INGEST_REARM_CRONJOB_NAME         the CronJob's name: a literal in the manifest (the downward API has no owner refs)
 *   INGEST_REARM_JOB_NAME             downward API fieldRef metadata.labels['batch.kubernetes.io/job-name']
 *   INGEST_REARM_JOB_UID              downward API fieldRef metadata.labels['batch.kubernetes.io/controller-uid']
 *   INGEST_REARM_POD_NAME             downward API fieldRef metadata.name
 *   INGEST_REARM_POD_UID              downward API fieldRef metadata.uid
 *   INGEST_REARM_POD_NAMESPACE        downward API fieldRef metadata.namespace
 *   AWS_LAMBDA_FUNCTION_NAME          set by Lambda for the hosted Lambda entry
 * The CronJob's own UID is not available through the downward API; the Job UID (controller-uid) is the identity.
 */
import { createHash } from "node:crypto";
import { isObjectStoreError, objectKey, readAll, type ObjectStore, type ObjectVersion } from "../shared/vendor.ts";
import { isAccessDenied, RearmSweepRefused, type RearmSweepCounts, type RearmSweepResult } from "./rearm-sweeper.ts";

/** The scope segment of a sweeper that covers every org (INGEST_REARM_ORGS unset). */
export const REARM_ALL_ORGS = ".all";
/** A receipt finished more than this far in the reader's future is refused (clock skew allowance). */
export const REARM_RECEIPT_FUTURE_SKEW_MS = 2 * 60_000;
/** The longest freshness window a caller may ask for. */
export const REARM_RECEIPT_MAX_AGE_LIMIT_MS = 7 * 24 * 60 * 60_000;

const ORG = /^[A-Za-z0-9._-]{1,128}$/;
const KEY_ROOT = /^[a-z0-9][a-z0-9-]{0,62}$/;
const HEX32 = /^[0-9a-f]{32}$/;
const HEX64 = /^[0-9a-f]{64}$/;
const validOrg = (org: unknown): org is string => typeof org === "string" && ORG.test(org) && !org.startsWith(".");
const validScope = (s: unknown): s is string => s === REARM_ALL_ORGS || validOrg(s);

export const rearmPassesPrefix = (keyRoot: string) => `${keyRoot}/.ledger/rearm-sweeper/passes`;
export const rearmPassReceiptKey = (keyRoot: string, scope: string, startedAtMs: number) => `${rearmPassesPrefix(keyRoot)}/${scope}/${startedAtMs}.json`;
export const rearmPassLatestKey = (keyRoot: string, scope: string) => `${rearmPassesPrefix(keyRoot)}/${scope}/latest.json`;

/** A one-way id for a bucket name (receipts never carry the name itself). */
export const rearmBucketHash = (bucket: string) => createHash("sha256").update(`rearm-sweeper-bucket:${bucket}`).digest("hex").slice(0, 32);
const sha256 = (b: Uint8Array) => createHash("sha256").update(b).digest("hex");

export interface RearmPassRevision { gitSha?: string; imageDigest?: string }
export interface RearmPassWorkload {
  cronJobName?: string; jobName?: string; jobUid?: string; podName?: string; podUid?: string; namespace?: string; lambdaFunction?: string;
}

export interface RearmPassReceipt {
  v: 1;
  kind: "rearm-sweeper-pass";
  /** The scope segment it is stored under: an org id, or `.all`. */
  org: string;
  enabled: true;
  dryRun: false;
  complete: true;
  fromStart: true;
  failures: 0;
  startedAtMs: number;
  finishedAtMs: number;
  /** The sweeper's scope set hash (= the cursor key's, rearm-sweeper.ts rearmCursorKey). */
  scopeHash: string;
  /** Scopes the pass listed (orgs, or 1 for every org). */
  scopes: number;
  keyRoot: string;
  /** rearmBucketHash of the transcripts bucket the eraser deletes from. */
  bucketHash: string;
  /** rearmBucketHash of the separate ledger bucket (INGEST_REARM_LEDGER_BUCKET), when there is one. */
  ledgerBucketHash?: string;
  revision: RearmPassRevision;
  workload: RearmPassWorkload;
  counts: RearmSweepCounts;
}

export interface RearmPassLatest {
  v: 1;
  kind: "rearm-sweeper-latest";
  org: string;
  startedAtMs: number;
  finishedAtMs: number;
  /** sha256 (hex) of the receipt's exact bytes. */
  receiptSha256: string;
}

// ---- provenance from the environment ----

const DNS_NAME = /^[a-z0-9]([a-z0-9.-]{0,251}[a-z0-9])?$/;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const LAMBDA_NAME = /^[A-Za-z0-9_-]{1,64}$/;

export function rearmPassRevision(env: NodeJS.ProcessEnv): RearmPassRevision {
  const sha = env.GITHUB_SHA?.trim().toLowerCase(), digest = env.INGEST_REARM_IMAGE_DIGEST?.trim().toLowerCase();
  return {
    ...(sha && /^[0-9a-f]{40}$/.test(sha) ? { gitSha: sha } : {}),
    ...(digest && /^sha256:[0-9a-f]{64}$/.test(digest) ? { imageDigest: digest } : {}),
  };
}

export function rearmPassWorkload(env: NodeJS.ProcessEnv): RearmPassWorkload {
  const pick = (name: string, re: RegExp) => { const v = env[name]?.trim(); return v && re.test(v) ? v : undefined; };
  const out: RearmPassWorkload = {};
  const set = <K extends keyof RearmPassWorkload>(k: K, v: string | undefined) => { if (v !== undefined) out[k] = v; };
  set("cronJobName", pick("INGEST_REARM_CRONJOB_NAME", DNS_NAME));
  set("jobName", pick("INGEST_REARM_JOB_NAME", DNS_NAME));
  set("jobUid", pick("INGEST_REARM_JOB_UID", UUID));
  set("podName", pick("INGEST_REARM_POD_NAME", DNS_NAME));
  set("podUid", pick("INGEST_REARM_POD_UID", UUID));
  set("namespace", pick("INGEST_REARM_POD_NAMESPACE", DNS_NAME));
  set("lambdaFunction", pick("AWS_LAMBDA_FUNCTION_NAME", LAMBDA_NAME));
  return out;
}

// ---- the writer's gate ----

/** Why a pass earns no receipt (codes only). */
export type RearmReceiptSkip = "dry_run" | "resumed" | "incomplete" | "failed" | "contended";

export function rearmPassReceiptEligibility(p: { dryRun: boolean; fromStart: boolean; cursor: "saved" | "contended" | "memory" } & Pick<RearmSweepResult, "complete" | "counts" | "failures">): RearmReceiptSkip | null {
  if (p.dryRun || p.cursor === "memory") return "dry_run";
  if (!p.fromStart) return "resumed";
  if (!p.complete) return "incomplete";
  if (p.counts.failed !== 0 || Object.keys(p.failures).length !== 0) return "failed";
  if (p.cursor !== "saved") return "contended";
  return null;
}

// ---- validation ----

const COUNT_KEYS: readonly (keyof RearmSweepCounts)[] = ["listed", "erased", "superseded", "alreadyDone", "pending", "resetRunning", "reenabled",
  "fenceAbsent", "fenceUnreadable", "corrupt", "ownerMissing", "machineMismatch", "machineUnbound", "layoutCollision", "layoutScanTooLarge",
  "machineDerived", "emptyUnbound", "deferred", "gone", "failed", "wouldErase", "wouldSupersede", "versions"];
const isObj = (v: unknown): v is Record<string, unknown> => v !== null && typeof v === "object" && !Array.isArray(v);
const ms = (v: unknown): v is number => Number.isSafeInteger(v) && (v as number) > 0;
const nonNeg = (v: unknown) => Number.isSafeInteger(v) && (v as number) >= 0;
const onlyKeys = (o: Record<string, unknown>, allowed: readonly string[], required: readonly string[]) =>
  Object.keys(o).every((k) => allowed.includes(k)) && required.every((k) => k in o);

function validCounts(c: unknown): c is RearmSweepCounts {
  return isObj(c) && onlyKeys(c, COUNT_KEYS, COUNT_KEYS) && COUNT_KEYS.every((k) => nonNeg(c[k])) && c.failed === 0
    && c.deferred === 0 && c.wouldErase === 0 && c.wouldSupersede === 0;
}
function validRevision(r: unknown): r is RearmPassRevision {
  return isObj(r) && onlyKeys(r, ["gitSha", "imageDigest"], [])
    && (r.gitSha === undefined || (typeof r.gitSha === "string" && /^[0-9a-f]{40}$/.test(r.gitSha)))
    && (r.imageDigest === undefined || (typeof r.imageDigest === "string" && /^sha256:[0-9a-f]{64}$/.test(r.imageDigest)));
}
function validWorkload(w: unknown): w is RearmPassWorkload {
  if (!isObj(w) || !onlyKeys(w, ["cronJobName", "jobName", "jobUid", "podName", "podUid", "namespace", "lambdaFunction"], [])) return false;
  const ok = (k: string, re: RegExp) => w[k] === undefined || (typeof w[k] === "string" && re.test(w[k] as string));
  return ok("cronJobName", DNS_NAME) && ok("jobName", DNS_NAME) && ok("jobUid", UUID) && ok("podName", DNS_NAME) && ok("podUid", UUID)
    && ok("namespace", DNS_NAME) && ok("lambdaFunction", LAMBDA_NAME);
}

const RECEIPT_KEYS = ["v", "kind", "org", "enabled", "dryRun", "complete", "fromStart", "failures", "startedAtMs", "finishedAtMs", "scopeHash",
  "scopes", "keyRoot", "bucketHash", "ledgerBucketHash", "revision", "workload", "counts"] as const;
const RECEIPT_REQUIRED = RECEIPT_KEYS.filter((k) => k !== "ledgerBucketHash");

/** A receipt exactly as the writer produces it, stored under `keyRoot` / `scope`. */
export function validRearmPassReceipt(v: unknown, keyRoot: string, scope: string): v is RearmPassReceipt {
  return isObj(v) && onlyKeys(v, RECEIPT_KEYS, RECEIPT_REQUIRED) && v.v === 1 && v.kind === "rearm-sweeper-pass" && v.org === scope
    && v.enabled === true && v.dryRun === false && v.complete === true && v.fromStart === true && v.failures === 0
    && ms(v.startedAtMs) && ms(v.finishedAtMs) && (v.finishedAtMs as number) >= (v.startedAtMs as number)
    && typeof v.scopeHash === "string" && HEX32.test(v.scopeHash) && Number.isSafeInteger(v.scopes) && (v.scopes as number) >= 1
    && v.keyRoot === keyRoot && typeof v.bucketHash === "string" && HEX32.test(v.bucketHash)
    && (v.ledgerBucketHash === undefined || (typeof v.ledgerBucketHash === "string" && HEX32.test(v.ledgerBucketHash)))
    && validRevision(v.revision) && validWorkload(v.workload) && validCounts(v.counts);
}

const LATEST_KEYS = ["v", "kind", "org", "startedAtMs", "finishedAtMs", "receiptSha256"] as const;
export function validRearmPassLatest(v: unknown, scope: string): v is RearmPassLatest {
  return isObj(v) && onlyKeys(v, LATEST_KEYS, LATEST_KEYS) && v.v === 1 && v.kind === "rearm-sweeper-latest" && v.org === scope
    && ms(v.startedAtMs) && ms(v.finishedAtMs) && (v.finishedAtMs as number) >= (v.startedAtMs as number)
    && typeof v.receiptSha256 === "string" && HEX64.test(v.receiptSha256);
}

// ---- store I/O ----

const json = (b: Uint8Array): unknown => JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(b));
const PUT = { contentType: "application/json" } as const;

/** The object's bytes and version, or null when absent. Denied access refuses; other store errors propagate. */
async function fetchObject(store: ObjectStore, key: string): Promise<{ bytes: Uint8Array; version?: ObjectVersion } | null> {
  try {
    const got = await store.getObject(objectKey(key));
    return { bytes: await readAll(got.body), version: got.meta.version };
  } catch (error) {
    if (isObjectStoreError(error, "not_found")) return null;
    if (isAccessDenied(error)) throw new RearmSweepRefused("access_denied");
    throw error;
  }
}

const conflict = (e: unknown) => isObjectStoreError(e, "precondition_failed") || isObjectStoreError(e, "already_exists");

export type RearmReceiptWrite = "written" | "contended";

/**
 * Write one scope's receipt (create-only) and move its latest pointer to it, unless the pointer already names a pass
 * that started at or after this one. "contended": a receipt already exists at this pass's key (another sweeper, same
 * start ms) and nothing was changed. Denied access refuses (`access_denied`); other store errors propagate.
 */
export async function writeRearmPassReceipt(store: ObjectStore, receipt: RearmPassReceipt): Promise<RearmReceiptWrite> {
  if (!KEY_ROOT.test(receipt.keyRoot) || !validScope(receipt.org) || !validRearmPassReceipt(receipt, receipt.keyRoot, receipt.org)) {
    throw new RearmSweepRefused("receipt_invalid");
  }
  const bytes = new TextEncoder().encode(JSON.stringify(receipt));
  try {
    await store.put(objectKey(rearmPassReceiptKey(receipt.keyRoot, receipt.org, receipt.startedAtMs)), bytes, { ...PUT, ifNoneMatch: true });
  } catch (error) {
    if (conflict(error)) return "contended";
    if (isAccessDenied(error)) throw new RearmSweepRefused("access_denied");
    throw error;
  }
  const latest: RearmPassLatest = { v: 1, kind: "rearm-sweeper-latest", org: receipt.org, startedAtMs: receipt.startedAtMs, finishedAtMs: receipt.finishedAtMs, receiptSha256: sha256(bytes) };
  const body = new TextEncoder().encode(JSON.stringify(latest));
  const key = rearmPassLatestKey(receipt.keyRoot, receipt.org);
  for (let attempt = 0; attempt < 4; attempt++) {
    const cur = await fetchObject(store, key);
    if (cur) {
      let doc: unknown;
      try { doc = json(cur.bytes); } catch { doc = null; }
      // A newer (or the same) pass is already the latest: leave it. An unreadable pointer is replaced.
      if (validRearmPassLatest(doc, receipt.org) && doc.startedAtMs >= receipt.startedAtMs) return "written";
    }
    try {
      await store.put(objectKey(key), body, cur?.version ? { ...PUT, ifMatch: cur.version } : { ...PUT, ifNoneMatch: true });
      return "written";
    } catch (error) {
      if (conflict(error)) continue; // another sweeper moved it meanwhile: re-read and compare
      if (isAccessDenied(error)) throw new RearmSweepRefused("access_denied");
      throw error;
    }
  }
  return "contended";
}

// ---- the reader (for the backlog-ops fence-authority mode) ----

export type FreshSweeperPassRefusal = "invalid_request" | "missing" | "malformed" | "stale" | "future";
export type FreshSweeperPass =
  | { ok: true; scope: "org" | "all"; ageMs: number; receipt: RearmPassReceipt }
  | { ok: false; reason: FreshSweeperPassRefusal };

type ScopeVerdict = { ok: true; receipt: RearmPassReceipt } | { ok: false; reason: "missing" | "malformed" };

async function readScope(store: ObjectStore, keyRoot: string, scope: string): Promise<ScopeVerdict> {
  const latest = await fetchObject(store, rearmPassLatestKey(keyRoot, scope));
  if (!latest) return { ok: false, reason: "missing" };
  let pointer: unknown;
  try { pointer = json(latest.bytes); } catch { return { ok: false, reason: "malformed" }; }
  if (!validRearmPassLatest(pointer, scope)) return { ok: false, reason: "malformed" };
  const got = await fetchObject(store, rearmPassReceiptKey(keyRoot, scope, pointer.startedAtMs));
  if (!got || sha256(got.bytes) !== pointer.receiptSha256) return { ok: false, reason: "malformed" };
  let receipt: unknown;
  try { receipt = json(got.bytes); } catch { return { ok: false, reason: "malformed" }; }
  if (!validRearmPassReceipt(receipt, keyRoot, scope) || receipt.startedAtMs !== pointer.startedAtMs || receipt.finishedAtMs !== pointer.finishedAtMs) {
    return { ok: false, reason: "malformed" };
  }
  return { ok: true, receipt };
}

/**
 * Is there a fresh, clean, full sweeper pass for `org`? Reads (never writes) the org's latest pointer and receipt,
 * and the every-org (`.all`) ones. A malformed pointer or receipt at EITHER scope refuses (`malformed`): corruption is
 * never outvoted. Otherwise the most recent valid pass counts; it must have finished within `maxAgeMs` of `nowMs`
 * (`stale`) and not more than REARM_RECEIPT_FUTURE_SKEW_MS after it (`future`). No receipt anywhere: `missing`.
 * Denied access throws RearmSweepRefused("access_denied"); any other store error propagates (fail closed).
 */
export async function freshSweeperPass(store: ObjectStore, keyRoot: string, org: string, nowMs: number, maxAgeMs: number): Promise<FreshSweeperPass> {
  if (!KEY_ROOT.test(keyRoot) || !validOrg(org) || !ms(nowMs) || !Number.isSafeInteger(maxAgeMs) || maxAgeMs < 1 || maxAgeMs > REARM_RECEIPT_MAX_AGE_LIMIT_MS) {
    return { ok: false, reason: "invalid_request" };
  }
  const [own, all] = await Promise.all([readScope(store, keyRoot, org), readScope(store, keyRoot, REARM_ALL_ORGS)]);
  if ((!own.ok && own.reason === "malformed") || (!all.ok && all.reason === "malformed")) return { ok: false, reason: "malformed" };
  const candidates = ([[own, "org"], [all, "all"]] as const)
    .flatMap(([v, scope]) => (v.ok ? [{ receipt: v.receipt, scope }] : []))
    .sort((a, b) => b.receipt.finishedAtMs - a.receipt.finishedAtMs);
  const best = candidates[0];
  if (!best) return { ok: false, reason: "missing" };
  const ageMs = nowMs - best.receipt.finishedAtMs;
  if (ageMs < -REARM_RECEIPT_FUTURE_SKEW_MS) return { ok: false, reason: "future" };
  if (ageMs > maxAgeMs) return { ok: false, reason: "stale" };
  return { ok: true, scope: best.scope, ageMs: Math.max(0, ageMs), receipt: best.receipt };
}
