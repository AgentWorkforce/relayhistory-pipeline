/**
 * sweeper-pass-verify: the READ-ONLY verifier of the re-arm sweeper's pass receipts (rearm-sweeper-receipt.ts), run
 * by scripts/backlog-sweeper-verify.ts (CLI only: not a backlog-ops dispatch mode). It reads one scope's `latest.json`
 * and the receipt it names (readRearmPassScope: the same validated read freshSweeperPass makes) and checks them
 * against expectations the CALLER obtained independently of the store (the serving CronJob's live spec and the digest
 * it is pinned to), never from a receipt.
 *
 * Everything freshSweeperPass checks, with authority-execute's window (BACKLOG_OPS_SWEEPER_PASS_MAX_AGE_MS, no caller
 * override) and REARM_RECEIPT_FUTURE_SKEW_MS on the verifier's clock, plus:
 *   - the serving identity, field by field: namespace and CronJob equal; the image digest equal to the ONE trusted
 *     digest (a git sha never stands in); a Job name of that CronJob and a Job UID; a Pod name of that Job and a Pod
 *     UID (distinct from the Job's); no Lambda function name; any jobName / jobUid / podName / podUid / gitSha the
 *     caller pins, equal;
 *   - WRITER PROVENANCE for the exact versions read. PASS needs independently trusted evidence that the sweeper wrote
 *     both the pointer version and the receipt version read (docs/aws-stage.md "D5": LastModified strictly inside a
 *     documented enforcement interval for the sweeper's signing role, or a writer record). Body fields, user metadata,
 *     ETag, version id and bucket policy alone are not that. This tree has no trusted interval or writer-record
 *     source, so the production path has no provider and every otherwise-valid pass is refused `writer_unverified`
 *     (exit 1). The provider parameter is a seam for tests (and a future trusted source); the CLI never passes one and
 *     no input (stdin, argv, env) can supply one.
 *
 * Inputs are validated before any store access. It takes a RearmReceiptReadStore (getObject only) and never writes.
 * The summary is closed and versioned (validSweeperPassSummary): codes, timestamps, booleans and domain-separated
 * hashes; explicit nulls where unavailable; never an org, key, bucket, identity, digest, ARN, version id or error text.
 */
import { createHash } from "node:crypto";
import { BACKLOG_OPS_SWEEPER_PASS_MAX_AGE_MS } from "./backlog-sweeper-limits.ts";
import {
  REARM_ALL_ORGS, REARM_RECEIPT_FUTURE_SKEW_MS, readRearmPassScope, rearmBucketHash, rearmPassMatches, type RearmPassExpectation,
  type RearmPassReceipt, type RearmReceiptObjectRead, type RearmReceiptReadStore,
} from "./rearm-sweeper-receipt.ts";
import { rearmScopeHash, rearmScopes, RearmSweepRefused } from "./rearm-sweeper.ts";

export const SWEEPER_PASS_VERIFY_KEY_ROOT = "96bulls";
export const SWEEPER_PASS_VERIFY_MODE = "sweeper-pass-verify";

/** What the caller knows about the CURRENT serving sweeper (closed: any other key is `invalid_request`). */
export interface SweeperPassVerifyRequest {
  /** The org whose pass is verified. */
  org: string;
  /** The serving sweeper's INGEST_REARM_ORGS, or null when it covers every org (then the `.all` scope is read). */
  orgs: string[] | null;
  /** Its INGEST_REARM_BUCKET and INGEST_REARM_LEDGER_BUCKET (null: the ledger is in `bucket`). */
  bucket: string;
  ledgerBucket: string | null;
  namespace: string;
  cronJobName: string;
  /** The ONE digest the serving CronJob is pinned to (`sha256:<64 hex>`). */
  imageDigest: string;
  gitSha?: string;
  jobName?: string;
  jobUid?: string;
  podName?: string;
  podUid?: string;
}

/** The exact versions read, as handed to a writer-provenance provider (nothing else is). */
export interface SweeperPassReadVersions { pointer: RearmReceiptObjectRead; receipt: RearmReceiptObjectRead }
/**
 * Independently trusted writer evidence for EXACTLY these versions: `verified` only when it attributes both to the
 * sweeper; `mismatch` when it attributes either to another writer (or holds a different version / bytes for it);
 * `unverified` when it cannot say. Not available in production (see the header).
 */
export type WriterProvenance = (read: SweeperPassReadVersions) => Promise<"verified" | "mismatch" | "unverified">;

export const SWEEPER_PASS_REASONS = [
  "ok",
  // input and tool
  "invalid_request", "usage", "stdin_invalid", "stdin_too_large", "aws_credentials_required", "internal_error",
  // read
  "out_of_scope", "missing", "malformed", "access_denied", "store_error",
  // the receipt against the serving sweeper
  "scope_mismatch", "home_mismatch", "namespace_mismatch", "cronjob_mismatch", "image_digest_mismatch", "git_sha_mismatch",
  "job_identity_invalid", "job_identity_mismatch", "pod_identity_invalid", "pod_identity_mismatch", "workload_unexpected", "mismatch",
  // freshness
  "future", "stale",
  // who wrote the versions read
  "writer_unverified", "writer_mismatch",
] as const;
export type SweeperPassReason = (typeof SWEEPER_PASS_REASONS)[number];

const CHECKS = ["scope", "homes", "namespace", "cron_job", "image_digest", "git_sha", "job", "pod", "workload", "match", "fresh", "writer"] as const;
type Check = (typeof CHECKS)[number];
type CheckState = "ok" | "fail" | "unchecked";

export interface SweeperPassSummary {
  v: 1;
  mode: typeof SWEEPER_PASS_VERIFY_MODE;
  verdict: "verified" | "refused";
  reason: SweeperPassReason;
  /** Hash of the scope segment read (the org, or `.all`). */
  scope_hash: string | null;
  /** Hash of the whole validated request (the expectation). */
  expectation_hash: string | null;
  /** sha256 of the latest.json bytes read, and of the receipt bytes read (= the pointer's receiptSha256). */
  latest_sha256: string | null;
  receipt_sha256: string | null;
  /** Hash of each version read (version id, ETag and LastModified), when the store reported a version id. */
  latest_version_hash: string | null;
  receipt_version_hash: string | null;
  started_at_ms: number | null;
  finished_at_ms: number | null;
  age_ms: number | null;
  max_age_ms: number;
  checks: Record<Check, CheckState>;
}

/** A domain-separated one-way id (32 hex): the only form an identifier takes in a summary. */
export const sweeperVerifyHash = (field: string, value: string) =>
  createHash("sha256").update(`backlog-ops-sweeper-pass-verify:${field}:${value}`).digest("hex").slice(0, 32);

const isObj = (v: unknown): v is Record<string, unknown> => v !== null && typeof v === "object" && !Array.isArray(v);
const closed = (o: Record<string, unknown>, allowed: readonly string[], required: readonly string[]) =>
  Object.keys(o).every((k) => allowed.includes(k)) && required.every((k) => Object.hasOwn(o, k));
const ORG = /^[A-Za-z0-9._-]{1,128}$/;
const validOrg = (o: unknown): o is string => typeof o === "string" && ORG.test(o) && !o.startsWith(".");
const BUCKET = /^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/;
const DNS_NAME = /^[a-z0-9]([a-z0-9.-]{0,251}[a-z0-9])?$/;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const DIGEST = /^sha256:[0-9a-f]{64}$/;
const SHA40 = /^[0-9a-f]{40}$/;
const REQ_KEYS = ["org", "orgs", "bucket", "ledgerBucket", "namespace", "cronJobName", "imageDigest", "gitSha", "jobName", "jobUid", "podName", "podUid"];
const REQ_REQUIRED = ["org", "orgs", "bucket", "ledgerBucket", "namespace", "cronJobName", "imageDigest"];

export function validSweeperPassRequest(v: unknown): v is SweeperPassVerifyRequest {
  if (!isObj(v) || !closed(v, REQ_KEYS, REQ_REQUIRED)) return false;
  const opt = (k: string, re: RegExp) => v[k] === undefined || (typeof v[k] === "string" && re.test(v[k] as string));
  return validOrg(v.org)
    && (v.orgs === null || (Array.isArray(v.orgs) && v.orgs.length >= 1 && v.orgs.length <= 1000 && v.orgs.every(validOrg)))
    && typeof v.bucket === "string" && BUCKET.test(v.bucket)
    && (v.ledgerBucket === null || (typeof v.ledgerBucket === "string" && BUCKET.test(v.ledgerBucket) && v.ledgerBucket !== v.bucket))
    && typeof v.namespace === "string" && DNS_NAME.test(v.namespace) && typeof v.cronJobName === "string" && DNS_NAME.test(v.cronJobName)
    && typeof v.imageDigest === "string" && DIGEST.test(v.imageDigest) && opt("gitSha", SHA40)
    && opt("jobName", DNS_NAME) && opt("jobUid", UUID) && opt("podName", DNS_NAME) && opt("podUid", UUID);
}

/** The canonical form hashed into expectation_hash (sorted keys, orgs sorted). */
const canonicalRequest = (q: SweeperPassVerifyRequest) =>
  JSON.stringify(Object.fromEntries(Object.entries({ ...q, orgs: q.orgs === null ? null : [...new Set(q.orgs)].sort() }).sort(([a], [b]) => (a < b ? -1 : 1))));

const freshChecks = (): Record<Check, CheckState> => Object.fromEntries(CHECKS.map((c) => [c, "unchecked"])) as Record<Check, CheckState>;

/** A refused summary with every evidence field null (usage and tool errors before or without a read). */
export function refusedSweeperPassSummary(reason: SweeperPassReason): SweeperPassSummary {
  return { v: 1, mode: SWEEPER_PASS_VERIFY_MODE, verdict: "refused", reason: reason === "ok" ? "internal_error" : reason, scope_hash: null,
    expectation_hash: null, latest_sha256: null, receipt_sha256: null, latest_version_hash: null, receipt_version_hash: null, started_at_ms: null,
    finished_at_ms: null, age_ms: null, max_age_ms: BACKLOG_OPS_SWEEPER_PASS_MAX_AGE_MS, checks: freshChecks() };
}

/** The identity checks, in order; the first failing one is the refusal. */
function identityChecks(r: RearmPassReceipt, keyRoot: string, q: SweeperPassVerifyRequest): [Check, SweeperPassReason, boolean][] {
  const w = r.workload;
  const scopes = rearmScopes(keyRoot, q.orgs);
  const pinned = (want: string | undefined, got: string | undefined) => want === undefined || want === got;
  return [
    ["scope", "scope_mismatch", r.scopeHash === rearmScopeHash(scopes) && r.scopes === scopes.length],
    ["homes", "home_mismatch", r.bucketHash === rearmBucketHash(q.bucket)
      && (q.ledgerBucket === null ? r.ledgerBucketHash === undefined : r.ledgerBucketHash === rearmBucketHash(q.ledgerBucket))],
    ["namespace", "namespace_mismatch", w.namespace === q.namespace],
    ["cron_job", "cronjob_mismatch", w.cronJobName === q.cronJobName],
    ["image_digest", "image_digest_mismatch", r.revision.imageDigest !== undefined && r.revision.imageDigest === q.imageDigest],
    ["git_sha", "git_sha_mismatch", q.gitSha === undefined || r.revision.gitSha === q.gitSha],
    ["job", "job_identity_invalid", w.jobName !== undefined && w.jobName.startsWith(`${q.cronJobName}-`) && w.jobUid !== undefined],
    ["job", "job_identity_mismatch", pinned(q.jobName, w.jobName) && pinned(q.jobUid, w.jobUid)],
    ["pod", "pod_identity_invalid", w.podName !== undefined && w.jobName !== undefined && w.podName.startsWith(`${w.jobName}-`)
      && w.podUid !== undefined && w.podUid !== w.jobUid],
    ["pod", "pod_identity_mismatch", pinned(q.podName, w.podName) && pinned(q.podUid, w.podUid)],
    ["workload", "workload_unexpected", w.lambdaFunction === undefined],
  ];
}

const versionHash = (o: RearmReceiptObjectRead) =>
  o.versionId === undefined ? null : sweeperVerifyHash("version", JSON.stringify([o.versionId, o.eTag ?? null, o.lastModifiedMs]));

/**
 * Verify the serving sweeper's latest pass over the request's org at `nowMs`. Never throws (store errors become
 * refusals) and never writes. `writerProvenance`: absent in production (=> `writer_unverified`).
 */
export async function verifySweeperPass(store: RearmReceiptReadStore, request: unknown, nowMs: number,
  writerProvenance?: WriterProvenance, keyRoot = SWEEPER_PASS_VERIFY_KEY_ROOT): Promise<SweeperPassSummary> {
  const s = refusedSweeperPassSummary("internal_error");
  const checks = s.checks;
  const refuse = (reason: SweeperPassReason): SweeperPassSummary => ({ ...s, verdict: "refused", reason });
  // Inputs first: nothing is read for an invalid request or clock.
  if (!Number.isSafeInteger(nowMs) || nowMs <= 0 || !validSweeperPassRequest(request)) return refuse("invalid_request");
  const q = request;
  const scope = q.orgs === null ? REARM_ALL_ORGS : q.org;
  s.scope_hash = sweeperVerifyHash("scope", scope);
  s.expectation_hash = sweeperVerifyHash("expectation", canonicalRequest(q));
  if (q.orgs !== null && !q.orgs.includes(q.org)) { checks.scope = "fail"; return refuse("out_of_scope"); }

  let read: Awaited<ReturnType<typeof readRearmPassScope>>;
  try { read = await readRearmPassScope(store, keyRoot, scope); }
  catch (error) { return refuse(error instanceof RearmSweepRefused && error.code === "access_denied" ? "access_denied" : "store_error"); }
  if (!read.ok) return refuse(read.reason);
  const r = read.receipt;
  s.latest_sha256 = read.pointer.sha256;
  s.receipt_sha256 = read.object.sha256;
  s.latest_version_hash = versionHash(read.pointer);
  s.receipt_version_hash = versionHash(read.object);
  s.started_at_ms = r.startedAtMs;
  s.finished_at_ms = r.finishedAtMs;

  // Identity: every check runs (the summary shows each), the first failure is the refusal.
  let first: SweeperPassReason | null = null;
  for (const [check, code, ok] of identityChecks(r, keyRoot, q)) {
    if (checks[check] !== "fail") checks[check] = ok ? "ok" : "fail";
    if (!ok && first === null) first = code;
  }
  // freshSweeperPass's own match must agree: the verifier never accepts what authority-execute's reader would refuse.
  const expectation: RearmPassExpectation = { orgs: q.orgs, bucket: q.bucket, ledgerBucket: q.ledgerBucket,
    writer: { namespace: q.namespace, cronJobName: q.cronJobName },
    revisions: { imageDigests: [q.imageDigest], ...(q.gitSha ? { gitShas: [q.gitSha] } : {}) } };
  const matched = rearmPassMatches(r, keyRoot, expectation);
  checks.match = matched ? "ok" : "fail";
  if (first === null && !matched) first = "mismatch";
  if (first !== null) return refuse(first);

  const ageMs = nowMs - r.finishedAtMs;
  s.age_ms = ageMs;
  if (ageMs < -REARM_RECEIPT_FUTURE_SKEW_MS) { checks.fresh = "fail"; return refuse("future"); }
  if (ageMs > BACKLOG_OPS_SWEEPER_PASS_MAX_AGE_MS) { checks.fresh = "fail"; return refuse("stale"); }
  checks.fresh = "ok";

  // Who wrote the exact versions read: only a trusted provider can say, and only about these reads.
  // Evidence binds to versions: without a version id for BOTH objects there is nothing to bind it to, and no provider
  // is asked.
  let writer: "verified" | "mismatch" | "unverified" = "unverified";
  if (writerProvenance !== undefined && read.pointer.versionId !== undefined && read.object.versionId !== undefined) {
    try {
      const got = await writerProvenance(Object.freeze({ pointer: Object.freeze({ ...read.pointer }), receipt: Object.freeze({ ...read.object }) }));
      writer = got === "verified" || got === "mismatch" ? got : "unverified";
    } catch { writer = "unverified"; }
  }
  if (writer === "mismatch") { checks.writer = "fail"; return refuse("writer_mismatch"); }
  if (writer !== "verified") return refuse("writer_unverified");
  checks.writer = "ok";
  return { ...s, verdict: "verified", reason: "ok" };
}

// ---- the summary's closed schema ----

const HASH = /^[0-9a-f]{32}$/;
const HEX64 = /^[0-9a-f]{64}$/;
const SUMMARY_KEYS = ["v", "mode", "verdict", "reason", "scope_hash", "expectation_hash", "latest_sha256", "receipt_sha256", "latest_version_hash",
  "receipt_version_hash", "started_at_ms", "finished_at_ms", "age_ms", "max_age_ms", "checks"];
const nullOr = (v: unknown, ok: (x: unknown) => boolean) => v === null || ok(v);
const isHash = (x: unknown) => typeof x === "string" && HASH.test(x);
const isHex64 = (x: unknown) => typeof x === "string" && HEX64.test(x);

/** Exactly the summary shape: every key present, no other key, every value a code, timestamp, state or hash. */
export function validSweeperPassSummary(v: unknown): v is SweeperPassSummary {
  if (!isObj(v) || !closed(v, SUMMARY_KEYS, SUMMARY_KEYS) || v.v !== 1 || v.mode !== SWEEPER_PASS_VERIFY_MODE) return false;
  if (!(SWEEPER_PASS_REASONS as readonly unknown[]).includes(v.reason)) return false;
  if (!(v.verdict === "verified" ? v.reason === "ok" : v.verdict === "refused" && v.reason !== "ok")) return false;
  if (![v.scope_hash, v.expectation_hash, v.latest_version_hash, v.receipt_version_hash].every((x) => nullOr(x, isHash))) return false;
  if (![v.latest_sha256, v.receipt_sha256].every((x) => nullOr(x, isHex64))) return false;
  if (![v.started_at_ms, v.finished_at_ms, v.age_ms].every((x) => nullOr(x, Number.isSafeInteger))) return false;
  if (v.max_age_ms !== BACKLOG_OPS_SWEEPER_PASS_MAX_AGE_MS) return false;
  const c = v.checks;
  if (!isObj(c) || !closed(c, CHECKS, CHECKS) || !CHECKS.every((k) => c[k] === "ok" || c[k] === "fail" || c[k] === "unchecked")) return false;
  // A verified summary carries every piece of evidence and every check passed.
  return v.verdict !== "verified" || (CHECKS.every((k) => c[k] === "ok") && [v.scope_hash, v.expectation_hash, v.latest_sha256, v.receipt_sha256,
    v.latest_version_hash, v.receipt_version_hash, v.started_at_ms, v.finished_at_ms, v.age_ms].every((x) => x !== null));
}
