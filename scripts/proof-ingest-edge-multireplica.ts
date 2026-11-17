/**
 * Multi-replica proof for the self-host session ingest edge with the S3 upload ledger (INGEST_LEDGER=s3).
 * Run by an operator against a DEV or STAGE deployment that already has 2+ ingest-edge replicas
 * (runbooks/ingest-edge-multireplica-proof.md). It never deploys, scales or dispatches anything.
 * scripts/proof-ingest-edge-multireplica-local.ts runs the same runProof() against two local processes.
 *
 *   bun scripts/proof-ingest-edge-multireplica.ts --plan-only ...      # static preflight + the plan; no network
 *   bun scripts/proof-ingest-edge-multireplica.ts --i-know-this-is-dev --target-env dev --deployment <name> \
 *     --ingest-url https://<ingest origin> --org <test org> --workspace <test workspace> \
 *     --user <userId>:<TOKEN_ENV> --user <userId>:<TOKEN_ENV> [--user ...] \
 *     --bucket <canonical bucket> --region <region> [--ledger-bucket <b> --ledger-region <r>] [--sse-kms-key-id <arn>] \
 *     (--replicas-confirmed | --kube-namespace <ns> --kube-deployment <name> --kube-context <ctx>) \
 *     [--control-url https://<relay-control origin>]   # adds the presigned direct-upload path
 *
 * What it drives, reusing the pipeline e2e's Desktop-shaped batch (e2e/pipeline/fixtures.ts deliveryBatch):
 *   - per synthetic user, two origins uploading concurrently (origin a: POST /v1/delivery/batches; origin b: the
 *     presigned upload inbox when --control-url is set, else HTTP too), several sessions each, every session a
 *     chain of record revisions sent in order (the next revision only after the previous one is terminal);
 *   - every other revision sent as N concurrent copies of the SAME batch (a retry storm racing both replicas);
 *   - mid-burst, an S3-ledger reset of the first user (ledger-s3.ts resetUploadedSessions, as the e2e's
 *     controls.s3 step: self-host serves no reset route under INGEST_LEDGER=s3), raced by in-flight uploads,
 *     then re-uploads of the reset sessions (must not land) and new "after" sessions (must land).
 * Then, after quiescence, it reads the S3 ledger and canonical objects back and asserts exactly-once admission
 * (evaluate() below), and finally tombstones its own after sessions (a second reset, applied only if nothing else
 * of user 0's is live), so the next run with the same synthetic user 0 starts from nothing live.
 * Output: one JSON receipt with counts only (no tokens, keys, tenant ids or bodies) and a verdict. Exit codes:
 * 0 PASS or PASS_UNATTRIBUTED (no per-replica evidence: not a two-replica proof), 1 FAIL, 2 refused before any
 * upload (a static or runtime preflight refusal).
 *
 * Storage credentials (read-back, and the reset's fence write): `--credentials keys` (default) reads
 * AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY [/ AWS_SESSION_TOKEN]; `podIdentity` uses EKS Pod Identity (the same
 * helper as scripts/ledger-reshard-revisions.ts). User tokens come from the named environment variables only.
 */
import { spawnSync } from "node:child_process";
import { createHash, randomBytes } from "node:crypto";
import { writeFileSync } from "node:fs";
import { parseArgs } from "node:util";
import { zstdCompressSync } from "node:zlib";
import { deliveryBatch } from "../e2e/pipeline/fixtures.ts";
import { INBOX_MIN_BYTES, INBOX_NAMESPACE, originInboxDir } from "../src/session-ingest/async-admission.ts";
import {
  fenceAuthorityKey, fenceAuthorityStopKey, fenceEpoch, isDeleted, orgFenceAuthorityKey, s3Ledger, s3LedgerKeys, s3LedgerObjects, sessionKeyHash,
  validFenceAuthority, validOrgFenceAuthority, type FenceDoc, type OwnerDoc,
} from "../src/session-ingest/ledger-s3.ts";
import { ledgerRoutedStore } from "../src/session-ingest/ledger-store.ts";
import type { LedgerTenant } from "../src/session-ingest/ledger.ts";
import {
  batchSegment, parseUploadAdmissionStatus, parseUploadCredentialsResponse, uploadChecksumSha256, uploadKeyPrefix, uploadObjectSha256Hex,
  UPLOAD_WITNESS_METADATA_HEADER,
} from "../src/session-ingest/upload-inbox-contract.ts";
import { DEAD_LETTER_NAMESPACE } from "../src/session-ingest/selfhost-admission.ts";
import { liveMachineId } from "../src/session-ingest/writer.ts";
import { layoutComponent } from "../src/shared/layout-component.ts";
import { createS3Store, isObjectStoreError, objectKey, readAll, type ObjectStore } from "../src/shared/vendor.ts";
import { reshardCredentials, type ReshardCredentialMode } from "./ledger-reshard-revisions.ts";

// ======================================================================================================
// Pure: configuration and preflight
// ======================================================================================================

export const TARGET_ENVS = ["dev", "stage", "sandbox"] as const;
export type TargetEnv = (typeof TARGET_ENVS)[number];
/** A production-looking name anywhere in a target identifier refuses the run. */
const PROD = /(^|[^a-z0-9])(prod|production|prd|live)([^a-z0-9]|$)/i;
/** The deployment name must carry a non-production marker. */
const NON_PROD = /(^|[^a-z0-9])(dev|development|stage|staging|sandbox|test)([^a-z0-9]|$)/i;
const SEGMENT = /^[A-Za-z0-9_-][A-Za-z0-9._-]{0,127}$/;
const KEY_ROOT = /^[a-z0-9][a-z0-9-]{0,62}$/;
const ENV_NAME = /^[A-Z_][A-Z0-9_]{0,63}$/;
const BUCKET = /^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/;
const REGION = /^[a-z0-9-]{1,64}$/;

export interface ProofUser { userId: string; tokenEnv: string }
export interface ProofConfig {
  targetEnv: string;
  deployment: string;
  iKnowThisIsDev: boolean;
  ingestUrl: string;
  controlUrl?: string;
  org: string;
  workspace: string;
  users: ProofUser[];
  keyRoot: string;
  bucket: string;
  region: string;
  endpoint?: string;
  ledgerBucket?: string;
  ledgerRegion?: string;
  sseKmsKeyId?: string;
  credentials: string;
  replicasConfirmed: boolean;
  kubeNamespace?: string;
  kubeDeployment?: string;
  kubeContext?: string;
  sessionsPerOrigin: number;
  revisions: number;
  retryCopies: number;
  concurrency: number;
  quiesceTimeoutS: number;
  allowRetryCodes: string[];
  /** The operator declares user 0 a dedicated synthetic user: the mid-run reset tombstones EVERY session it owns. */
  resetUserIsSynthetic: boolean;
  planOnly: boolean;
  receiptOut?: string;
}

const OPTIONS = {
  "target-env": { type: "string" }, deployment: { type: "string" }, "i-know-this-is-dev": { type: "boolean", default: false },
  "ingest-url": { type: "string" }, "control-url": { type: "string" }, org: { type: "string" }, workspace: { type: "string" },
  user: { type: "string", multiple: true }, "key-root": { type: "string", default: "96bulls" },
  bucket: { type: "string" }, region: { type: "string" }, endpoint: { type: "string" },
  "ledger-bucket": { type: "string" }, "ledger-region": { type: "string" }, "sse-kms-key-id": { type: "string" },
  credentials: { type: "string", default: "keys" }, "replicas-confirmed": { type: "boolean", default: false },
  "kube-namespace": { type: "string" }, "kube-deployment": { type: "string" }, "kube-context": { type: "string" },
  "sessions-per-origin": { type: "string", default: "4" }, revisions: { type: "string", default: "4" },
  "retry-copies": { type: "string", default: "4" }, concurrency: { type: "string", default: "12" },
  "quiesce-timeout-s": { type: "string", default: "600" }, "allow-retry-code": { type: "string", multiple: true },
  "plan-only": { type: "boolean", default: false }, "receipt-out": { type: "string" },
  "reset-user-is-synthetic": { type: "boolean", default: false },
} as const;

/** Parse argv into a config. Throws only on unknown flags; every semantic check is preflightRefusals'. */
export function parseProofArgs(argv: string[]): ProofConfig {
  const { values: v } = parseArgs({ args: argv, options: OPTIONS, strict: true, allowPositionals: false });
  const users = (v.user ?? []).map((s) => {
    const at = s.lastIndexOf(":");
    return at > 0 ? { userId: s.slice(0, at), tokenEnv: s.slice(at + 1) } : { userId: s, tokenEnv: "" };
  });
  const int = (s: string | undefined) => (s !== undefined && /^[0-9]{1,6}$/.test(s) ? Number(s) : NaN);
  return {
    targetEnv: v["target-env"] ?? "", deployment: v.deployment ?? "", iKnowThisIsDev: v["i-know-this-is-dev"] === true,
    ingestUrl: v["ingest-url"] ?? "", ...(v["control-url"] ? { controlUrl: v["control-url"] } : {}),
    org: v.org ?? "", workspace: v.workspace ?? "", users, keyRoot: v["key-root"] ?? "96bulls",
    bucket: v.bucket ?? "", region: v.region ?? "", ...(v.endpoint ? { endpoint: v.endpoint } : {}),
    ...(v["ledger-bucket"] ? { ledgerBucket: v["ledger-bucket"] } : {}), ...(v["ledger-region"] ? { ledgerRegion: v["ledger-region"] } : {}),
    ...(v["sse-kms-key-id"] ? { sseKmsKeyId: v["sse-kms-key-id"] } : {}), credentials: v.credentials ?? "keys",
    replicasConfirmed: v["replicas-confirmed"] === true,
    ...(v["kube-namespace"] ? { kubeNamespace: v["kube-namespace"] } : {}), ...(v["kube-deployment"] ? { kubeDeployment: v["kube-deployment"] } : {}),
    ...(v["kube-context"] ? { kubeContext: v["kube-context"] } : {}),
    sessionsPerOrigin: int(v["sessions-per-origin"]), revisions: int(v.revisions), retryCopies: int(v["retry-copies"]),
    concurrency: int(v.concurrency), quiesceTimeoutS: int(v["quiesce-timeout-s"]), allowRetryCodes: v["allow-retry-code"] ?? [],
    resetUserIsSynthetic: v["reset-user-is-synthetic"] === true,
    planOnly: v["plan-only"] === true, ...(v["receipt-out"] ? { receiptOut: v["receipt-out"] } : {}),
  };
}

const isLoopback = (host: string) => host === "localhost" || host === "127.0.0.1" || host === "[::1]";

/** An https URL, or plain http to a loopback host only (the local mode); no credentials, query or fragment. */
function urlHost(u: string): string | null {
  try {
    const url = new URL(u);
    if (url.username || url.password || url.search || url.hash) return null;
    if (url.protocol !== "https:" && !(url.protocol === "http:" && isLoopback(url.hostname))) return null;
    return url.hostname;
  } catch { return null; }
}

/**
 * The deny-list AND the positive marker, for one target name: a production token anywhere refuses, and the name must
 * carry a non-production marker unless `exempt` (a loopback host, or a bucket on a loopback object store).
 */
function targetRefusals(what: string, name: string, exempt = false): string[] {
  if (PROD.test(name)) return [`${what}_looks_like_production`];
  if (!exempt && !NON_PROD.test(name)) return [`${what}_not_marked_non_production`];
  return [];
}

/**
 * The presigned PUT URL relay-control answers for the direct path: the same target checks as every other target
 * (https, or plain http to loopback only; no production token; a non-production marker unless loopback). A presigned
 * URL carries its signature in the query, so only credentials and a fragment are refused there. The name checked is
 * the host and, for a path-style URL, the bucket (the first path segment), so either may carry the marker; the
 * deny-list applies to both.
 */
export function uploadUrlRefusals(u: string): string[] {
  let url: URL;
  try { url = new URL(u); } catch { return ["upload_url_invalid"]; }
  if (url.username || url.password || url.hash) return ["upload_url_invalid"];
  const loopback = isLoopback(url.hostname);
  if (url.protocol !== "https:" && !(url.protocol === "http:" && loopback)) return [url.protocol === "http:" ? "upload_url_plain_http_not_loopback" : "upload_url_invalid"];
  const bucket = decodeURIComponent(url.pathname.split("/")[1] ?? "");
  if (PROD.test(url.hostname) || PROD.test(bucket)) return ["upload_url_looks_like_production"];
  if (!loopback && !NON_PROD.test(url.hostname) && !NON_PROD.test(bucket)) return ["upload_url_not_marked_non_production"];
  return [];
}

/**
 * Every reason not to run, as fixed codes (never a value). Empty = go. `env` supplies the token variables, which
 * are only checked for presence and shape. `planOnly` skips the token and replica checks (no request is sent).
 */
export function preflightRefusals(c: ProofConfig, env: Record<string, string | undefined>): string[] {
  const r: string[] = [];
  if (!c.iKnowThisIsDev) r.push("missing_i_know_this_is_dev");
  if (!(TARGET_ENVS as readonly string[]).includes(c.targetEnv)) r.push("target_env_not_dev_or_stage");
  if (!c.deployment) r.push("deployment_missing");
  else r.push(...targetRefusals("deployment", c.deployment));
  const ingestHost = urlHost(c.ingestUrl);
  if (!ingestHost) r.push("ingest_url_invalid");
  else r.push(...targetRefusals("ingest_url", ingestHost, isLoopback(ingestHost)));
  if (c.controlUrl !== undefined) {
    const h = urlHost(c.controlUrl);
    if (!h) r.push("control_url_invalid");
    else r.push(...targetRefusals("control_url", h, isLoopback(h)));
  }
  // The object store endpoint: https to a non-production host, or plain http to loopback only (the local mode).
  let localStore = false;
  if (c.endpoint !== undefined) {
    const h = urlHost(c.endpoint);
    if (!h) r.push(/^http:/i.test(c.endpoint) ? "endpoint_plain_http_not_loopback" : "endpoint_invalid");
    else { localStore = isLoopback(h); r.push(...targetRefusals("endpoint", h, localStore)); }
  }
  if (!SEGMENT.test(c.org)) r.push("org_invalid");
  if (!SEGMENT.test(c.workspace)) r.push("workspace_invalid");
  if (!KEY_ROOT.test(c.keyRoot)) r.push("key_root_invalid");
  if (!BUCKET.test(c.bucket)) r.push("bucket_invalid");
  else r.push(...targetRefusals("bucket", c.bucket, localStore));
  if (!REGION.test(c.region)) r.push("region_invalid");
  if (c.ledgerBucket !== undefined) {
    if (!BUCKET.test(c.ledgerBucket) || c.ledgerBucket === c.bucket) r.push("ledger_bucket_invalid");
    else r.push(...targetRefusals("ledger_bucket", c.ledgerBucket, localStore));
    if (c.ledgerRegion !== undefined && !REGION.test(c.ledgerRegion)) r.push("ledger_region_invalid");
  } else if (c.ledgerRegion !== undefined) r.push("ledger_region_without_ledger_bucket");
  if (c.credentials !== "keys" && c.credentials !== "podIdentity") r.push("credentials_mode_invalid");
  if (c.users.length < 2) r.push("need_at_least_two_users");
  if (c.users.length > 8) r.push("too_many_users");
  if (new Set(c.users.map((u) => u.userId)).size !== c.users.length) r.push("users_not_distinct");
  for (const u of c.users) {
    if (!u.userId || u.userId.length > 512 || /[\x00-\x1f\x7f]/.test(u.userId)) { r.push("user_id_invalid"); break; }
    if (!ENV_NAME.test(u.tokenEnv)) { r.push("user_token_env_invalid"); break; }
  }
  if (!c.planOnly) {
    for (const u of c.users) {
      const t = ENV_NAME.test(u.tokenEnv) ? env[u.tokenEnv] : undefined;
      if (!t) { r.push("user_token_missing"); break; }
      if (!t.startsWith("sh_at_")) { r.push("user_token_not_relaycontrol_access_token"); break; }
    }
    const kube = c.kubeNamespace !== undefined || c.kubeDeployment !== undefined;
    if (kube && !(c.kubeNamespace && c.kubeDeployment)) r.push("kube_namespace_and_deployment_both_required");
    if (!c.replicasConfirmed && !kube) r.push("replicas_unconfirmed");
  }
  for (const [what, k] of [["kube_namespace", c.kubeNamespace], ["kube_deployment", c.kubeDeployment], ["kube_context", c.kubeContext]] as const)
    if (k !== undefined) r.push(...targetRefusals(what, k));
  // kubectl evidence (and the read-only kubectl calls) only against an explicitly named, checked context: never the
  // operator's current context, which could be any cluster.
  if ((c.kubeNamespace !== undefined || c.kubeDeployment !== undefined) && c.kubeContext === undefined) r.push("kube_context_required_with_kubectl");
  // The vendored S3 client reads no environment: it ignores AWS_ENDPOINT_URL / AWS_ENDPOINT_URL_S3 and, without
  // --endpoint, always talks to AWS S3. Refuse them without --endpoint, so an operator who set one for another store
  // is not silently sent elsewhere; the only object-store endpoint is the checked --endpoint.
  if (c.endpoint === undefined && (env.AWS_ENDPOINT_URL || env.AWS_ENDPOINT_URL_S3)) r.push("aws_endpoint_env_without_endpoint_flag");
  // The reset is user-wide (resetUploadedSessions tombstones every session user 0 owns): only on a declared synthetic user.
  if (!c.resetUserIsSynthetic) r.push("reset_user_not_declared_synthetic");
  const range = (n: number, lo: number, hi: number) => Number.isInteger(n) && n >= lo && n <= hi;
  if (!range(c.sessionsPerOrigin, 1, 16)) r.push("sessions_per_origin_out_of_range");
  if (!range(c.revisions, 2, 32)) r.push("revisions_out_of_range");
  if (!range(c.retryCopies, 1, 16)) r.push("retry_copies_out_of_range");
  if (!range(c.concurrency, 1, 64)) r.push("concurrency_out_of_range");
  if (!range(c.quiesceTimeoutS, 30, 3600)) r.push("quiesce_timeout_out_of_range");
  for (const code of c.allowRetryCodes) if (!/^[a-z0-9_]{1,64}$/.test(code)) { r.push("allow_retry_code_invalid"); break; }
  return r;
}

/** What the run prints before it starts: the target and shape only (no token, no user id). */
export function configSummary(c: ProofConfig) {
  return {
    target_env: c.targetEnv, ingest_loopback: isLoopback(urlHost(c.ingestUrl) ?? ""), direct_upload: c.controlUrl !== undefined,
    key_root: c.keyRoot, ledger_bucket_split: c.ledgerBucket !== undefined, sse_kms: c.sseKmsKeyId !== undefined, credentials: c.credentials,
    users: c.users.length, sessions_per_origin: c.sessionsPerOrigin, revisions: c.revisions, retry_copies: c.retryCopies,
    concurrency: c.concurrency, quiesce_timeout_s: c.quiesceTimeoutS,
    replicas_check: c.kubeNamespace ? "kubectl" : c.replicasConfirmed ? "operator_flag" : "none",
  };
}

// ======================================================================================================
// Pure: the burst plan
// ======================================================================================================

export type UploadPath = "http" | "direct";
/** admitted: must be admitted and land; blocked: must not land (reset-superseded); either: races the reset. */
export type Expect = "admitted" | "blocked" | "either";
export interface PlannedBatch { label: string; revision: number; copies: number; expect: Expect }
export interface Chain {
  user: number; originId: string; path: UploadPath; sessionId: string; marker: string;
  /** "main": the burst; "reset": the reset user's sessions; "after": new sessions made after the reset. */
  role: "main" | "reset" | "after";
  /** Phase A for the reset user's sessions, or the whole chain for the others: sent in order, each terminal first. */
  batches: PlannedBatch[];
  /** Reset user only: fired just before the reset (expect either), then after it (expect blocked). */
  race?: PlannedBatch; blocked?: PlannedBatch;
}
/** Post-reset revisions start here, so anything at or above it in the ledger for a reset session is a leak. */
export const BLOCKED_REVISION = 1000;

function uuidFrom(seed: string): string {
  const h = createHash("sha256").update(seed).digest("hex");
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-7${h.slice(13, 16)}-${((parseInt(h[16]!, 16) & 0x3) | 0x8).toString(16)}${h.slice(17, 20)}-${h.slice(20, 32)}`;
}

/** The deterministic plan for one run. User 0 is the reset user; the others upload twice as many revisions. */
export function planBurst(runId: string, users: number, o: { sessionsPerOrigin: number; revisions: number; retryCopies: number; direct: boolean }): Chain[] {
  const chains: Chain[] = [];
  const copiesFor = (i: number) => (i % 2 === 0 ? o.retryCopies : 1);
  for (let u = 0; u < users; u++) {
    const reset = u === 0;
    for (const side of ["a", "b"] as const) {
      const originId = `mrp-${runId}-u${u}-${side}`;
      const path: UploadPath = side === "b" && o.direct ? "direct" : "http";
      const n = reset ? o.revisions : o.revisions * 2;
      for (let s = 0; s < o.sessionsPerOrigin; s++) {
        const base = { user: u, originId, path, sessionId: uuidFrom(`${originId}:s${s}`), marker: `mrproof${runId}u${u}${side}s${s}` };
        const batches = Array.from({ length: n }, (_, i) => ({ label: `${runId}-a-${s}-${i + 1}`, revision: i + 1, copies: copiesFor(i), expect: "admitted" as const }));
        if (!reset) { chains.push({ ...base, role: "main", batches }); continue; }
        chains.push({
          ...base, role: "reset", batches,
          race: { label: `${runId}-race-${s}`, revision: n + 1, copies: 2, expect: "either" },
          // The racing batch may land (claimed before the reset) or not; the blocked one never may.
          blocked: { label: `${runId}-blocked-${s}`, revision: BLOCKED_REVISION, copies: 2, expect: "blocked" },
        });
      }
      if (reset) {
        chains.push({
          user: u, originId, path, sessionId: uuidFrom(`${originId}:after`), marker: `mrproof${runId}u${u}${side}after`, role: "after",
          batches: [{ label: `${runId}-after-1`, revision: 1, copies: 2, expect: "admitted" }],
        });
      }
    }
  }
  return chains;
}

export function planCounts(chains: Chain[]) {
  let unique = 0, requests = 0;
  for (const c of chains) for (const b of [...c.batches, ...(c.race ? [c.race] : []), ...(c.blocked ? [c.blocked] : [])]) { unique++; requests += b.copies; }
  return { chains: chains.length, sessions: chains.length, unique_batches: unique, upload_requests: requests, origins: new Set(chains.map((c) => c.originId)).size };
}

// ======================================================================================================
// Pure: the assertions
// ======================================================================================================

export type Terminal = "admitted" | "refused" | "pending" | "unsent";
export interface BatchObservation {
  expect: Expect;
  path: UploadPath;
  terminal: Terminal;
  refusedCode?: string;
  /** The S3 ledger batch record for (origin, batch id). */
  record: "committed" | "pending" | "absent" | "invalid";
  /** The record's receiptId equals the receipt the upload answered (null: no receipt known to compare). */
  receiptMatches: boolean | null;
  /** The concurrent copies answered more than one receipt id (two different admissions of one batch). */
  receiptsDiverged?: boolean;
}
/**
 * One record's revision-index entry. It must lie in [min, max]. `absentOk`: a reset session's entries are forgotten
 * by the reset and a later merge of the same shard may drop them, so absence is fine there (presence above max is not).
 */
export interface RecordObservation { role: Chain["role"]; min: number; max: number; absentOk: boolean; indexRevision: number | null }
export interface SessionObservation { role: Chain["role"]; min: number; max: number; manifestRevision: number | null; manifests: number }
export interface LogTally { lines: number; admitted: number; refused: number; retry: number; deferred: number; deadLettered: number; uploads: number; promoted: number;
  /** Admissions that claimed and wrote the batch (deliver outcome accepted*), not a replay of one already committed. */
  firstWrites: number;
  /**
   * Admissions answered `replay_fast`: the claim found every revision already indexed and committed the batch record
   * at once. Under contention that is how a replica TAKES OVER a batch another replica wrote but had not committed
   * yet (that one's commit then answers ledger_attempt_superseded and its retry a replay): committed, no accepted line.
   */
  replayFast: number; codes: Record<string, number> }
export interface Observation {
  batches: BatchObservation[];
  records: RecordObservation[];
  sessions: SessionObservation[];
  storage: { inboxPending: number; uploadInboxPending: number; deadLetters: number };
  /** `targetRefusals`: direct-path PUT URLs refused by uploadUrlRefusals (never sent to), by code. */
  http: { statuses: Record<string, number>; retryCodes: Record<string, number>; unexpected: number; targetRefusals?: Record<string, number> };
  /** `refused`: the reset was not applied because user 0 owned sessions outside the run (resetOwnershipCheck). */
  reset: { count: number | null; replay: number | null; resetSessions: number; fenceResets: number | null; refused?: "foreign_sessions" | "owners_unreadable" };
  /**
   * The end-of-run reset of the run's own after sessions (resetAfterSessions), so a rerun with the same synthetic
   * user 0 starts from nothing live. `live`: the after sessions user 0 owned live just before it.
   */
  cleanup: AfterReset;
  replicas: { by: "kubectl" | "operator_flag" | "local_compose"; ready: number | null; env?: Record<string, string | null> };
  logs?: Record<string, LogTally>;
  quiesced: boolean;
}

/** Retry codes contention between replicas is expected to produce; anything else in the logs is a finding. */
export const EXPECTED_RETRY_CODES = new Set([
  "tenant_limit", "delivery_busy", "ledger_batch_contended", "ledger_revision_contended", "ledger_fence_contended",
  "ledger_owner_contended", "ledger_rearm_contended", "ledger_attempt_superseded", "ledger_control_pending",
  "privacy_control_pending", "delivery_unavailable",
]);
/** Refusals allowed only on a batch that races the reset or is superseded by it. */
export const RESET_REFUSAL_CODES = new Set(["delivery_privacy_superseded"]);

/**
 * PASS_UNATTRIBUTED: every storage assertion held, but there is no per-replica evidence (no logs: --replicas-confirmed
 * without kubectl), so nothing shows a second replica did any of the work. Exit 0, but it is not a two-replica proof.
 */
export interface Verdict { verdict: "PASS" | "PASS_UNATTRIBUTED" | "FAIL"; failures: Record<string, number>; counts: Record<string, unknown> }

/**
 * The read-only check before the user-wide reset: the sessions user 0 owns that the reset would tombstone (an owner
 * stub of the current generation, not deleted: ledger-s3.ts finishResets' rule) must be exactly `runSessions`
 * (`originId\nsessionId`). Before the burst, pass an empty set: a dedicated synthetic user owns nothing live yet.
 * An absent fence (`null`) means no control ever ran: every stub is live.
 */
export function resetOwnershipCheck(owners: readonly OwnerDoc[], fence: FenceDoc | null, runSessions: ReadonlySet<string>): { live: number; foreign: number; missing: number } {
  const live = new Set<string>();
  for (const d of owners) {
    if (fence && (isDeleted(fence, d.originId, d.source, d.sessionId) || fenceEpoch(fence, d.originId, sessionKeyHash(d.source, d.sessionId)) !== d.epoch)) continue;
    live.add(`${d.originId}\n${d.sessionId}`);
  }
  let foreign = 0;
  for (const k of live) if (!runSessions.has(k)) foreign++;
  let missing = 0;
  for (const k of runSessions) if (!live.has(k)) missing++;
  return { live: live.size, foreign, missing };
}

/**
 * `requireAdmissionsPerReplica`: every replica's log must show at least one admission of its own (the local mode,
 * where the logs are this run's only traffic), not just a request served.
 */
export function evaluate(o: Observation, opts: { allowRetryCodes?: readonly string[]; requireAdmissionsPerReplica?: boolean } = {}): Verdict {
  // requireAdmissionsPerReplica also means the logs are this run's only traffic. Then across the replicas: only the
  // attempt that commits a batch answers `accepted`, so more accepted lines than admitted batches is a double admission;
  // and every admitted batch was committed by an accepted or a replay_fast (takeover) admission somewhere.
  const f: Record<string, number> = {};
  const fail = (code: string, n = 1) => { if (n > 0) f[code] = (f[code] ?? 0) + n; };
  if (!o.quiesced) fail("not_quiesced_within_timeout");

  for (const b of o.batches) {
    if (b.terminal === "unsent") { fail("batch_not_sent"); continue; }
    if (b.terminal === "pending") fail(b.expect === "either" ? "race_batch_never_terminal" : "batch_never_terminal");
    if (b.terminal === "refused") {
      const allowed = b.expect !== "admitted" && b.refusedCode !== undefined && RESET_REFUSAL_CODES.has(b.refusedCode);
      if (!allowed) fail("unexpected_refusal");
    }
    if (b.record === "invalid") fail("batch_record_invalid");
    else if (b.record === "pending") fail("batch_record_left_pending");
    else if (b.record === "absent" && b.terminal === "admitted") fail("admitted_without_batch_record");
    if (b.receiptMatches === false) fail("batch_record_receipt_mismatch");
    if (b.receiptsDiverged) fail("retry_copies_got_different_receipts");
  }
  for (const r of o.records) {
    if (r.indexRevision === null) { if (!r.absentOk) fail("revision_missing_from_index"); continue; }
    if (r.indexRevision < 0) fail("revision_index_unreadable");
    else if (r.indexRevision >= BLOCKED_REVISION && r.role === "reset") fail("reset_superseded_revision_admitted");
    else if (r.indexRevision > r.max) fail("revision_ahead_of_any_admitted");
    else if (r.indexRevision < r.min) fail("revision_regressed_or_lost");
  }
  for (const s of o.sessions) {
    if (s.manifests > 1) fail("more_than_one_manifest_for_session");
    if (s.manifestRevision === null) { fail("manifest_missing"); continue; }
    if (s.manifestRevision > s.max) fail(s.role === "reset" ? "reset_superseded_write_landed" : "duplicate_admission_manifest_written_twice");
    if (s.manifestRevision < s.min) fail("admitted_batch_not_written");
  }
  fail("inbox_left_unprocessed", o.storage.inboxPending);
  fail("upload_inbox_left_unpromoted", o.storage.uploadInboxPending);
  fail("dead_lettered", o.storage.deadLetters);
  fail("unexpected_http_answer", o.http.unexpected);
  for (const [code, n] of Object.entries(o.http.targetRefusals ?? {})) fail(code, n);
  const allowed = new Set([...EXPECTED_RETRY_CODES, ...(opts.allowRetryCodes ?? [])]);
  for (const [code, n] of Object.entries(o.http.retryCodes)) if (!allowed.has(code)) fail("unexpected_retry_code", n);

  // The after sessions must be tombstoned, or the next run with this user 0 refuses before any upload.
  if (o.cleanup.refused) fail(`cleanup_reset_refused_${o.cleanup.refused}`);
  else if (o.cleanup.count === null) fail("cleanup_reset_not_run");
  else if (o.cleanup.count !== o.cleanup.live) fail("cleanup_count_not_equal_live_after_sessions");

  if (o.reset.refused) fail(`reset_refused_${o.reset.refused}`);
  else if (o.reset.count === null) fail("reset_not_run");
  else {
    if (o.reset.replay !== o.reset.count) fail("reset_replay_answered_differently");
    // Exactly the run's sessions: fewer is a missed tombstone, more means the reset reached a session outside the run.
    if (o.reset.count !== o.reset.resetSessions) fail("reset_count_not_equal_run_sessions");
    if (o.reset.fenceResets === null || o.reset.fenceResets < 1) fail("reset_not_recorded_in_fence");
  }

  let podsServing = 0;
  const logCodes: Record<string, number> = {};
  if (o.logs) {
    for (const t of Object.values(o.logs)) {
      if (t.admitted + t.uploads > 0) podsServing++;
      if (opts.requireAdmissionsPerReplica && t.firstWrites === 0) fail("replica_admitted_nothing");
      fail("dead_lettered_in_logs", t.deadLettered);
      for (const [code, n] of Object.entries(t.codes)) {
        logCodes[code] = (logCodes[code] ?? 0) + n;
        if (!allowed.has(code) && !RESET_REFUSAL_CODES.has(code)) fail("unexpected_log_error_code", n);
      }
    }
    if (podsServing < 2) fail("fewer_than_two_replicas_did_work");
    if (opts.requireAdmissionsPerReplica) {
      const firstWrites = Object.values(o.logs).reduce((n, t) => n + t.firstWrites, 0);
      const admitted = o.batches.filter((b) => b.terminal === "admitted").length;
      const fast = Object.values(o.logs).reduce((n, t) => n + t.replayFast, 0);
      if (firstWrites > admitted) fail("more_first_writes_than_admitted_batches", firstWrites - admitted);
      if (firstWrites + fast < admitted) fail("admitted_batches_without_a_logged_commit", admitted - firstWrites - fast);
    }
  }
  if (o.replicas.by === "kubectl") {
    if (o.replicas.ready === null || o.replicas.ready < 2) fail("fewer_than_two_ready_replicas");
    if (o.replicas.env && o.replicas.env.INGEST_LEDGER !== "s3") fail("ingest_ledger_not_s3");
    const lc = o.replicas.env?.INGEST_LEDGER_LOG_COMMIT;
    if (lc !== undefined && lc !== null && lc.toLowerCase() !== "off" && lc !== "") fail("ledger_log_commit_not_off");
  }

  const tally = <T>(xs: T[], key: (x: T) => string) => xs.reduce<Record<string, number>>((m, x) => ((m[key(x)] = (m[key(x)] ?? 0) + 1), m), {});
  return {
    verdict: Object.keys(f).length > 0 ? "FAIL" : o.logs ? "PASS" : "PASS_UNATTRIBUTED",
    failures: f,
    counts: {
      batches: o.batches.length,
      batches_by_expect_and_terminal: tally(o.batches, (b) => `${b.expect}:${b.terminal}`),
      batches_by_path: tally(o.batches, (b) => b.path),
      batch_records: tally(o.batches, (b) => b.record),
      refusals_by_code: tally(o.batches.filter((b) => b.terminal === "refused"), (b) => b.refusedCode ?? "none"),
      records_checked: o.records.length, sessions_checked: o.sessions.length,
      storage: o.storage, http: o.http, reset: o.reset, cleanup: o.cleanup,
      replicas: { by: o.replicas.by, ready: o.replicas.ready, pods_with_work: o.logs ? podsServing : null },
      ...(o.logs ? { log_codes: logCodes, logs_by_pod: Object.values(o.logs) } : {}),
    },
  };
}

// ======================================================================================================
// Pure: kubectl output
// ======================================================================================================

export const WATCHED_ENV = ["INGEST_LEDGER", "INGEST_LEDGER_LOG_COMMIT", "SELFHOST_INGEST_ADMISSION", "SELFHOST_ADMISSION_CONCURRENCY",
  "INGEST_MAX_PER_USER", "INGEST_MAX_PER_ORG", "INGEST_ASYNC_MAX_PER_ORG", "INGEST_S3_TENANT_LIMIT", "HISTORY_UPLOAD_DIRECT", "INGEST_KEY_ROOT"] as const;

/** `kubectl get deployment -o json`: ready replicas, the selector, and the watched env (null = from a Secret / unset). */
export function parseDeployment(json: string): { ready: number; desired: number; selector: string; env: Record<string, string | null> } | null {
  try {
    const d = JSON.parse(json) as any;
    const labels = d?.spec?.selector?.matchLabels as Record<string, string> | undefined;
    if (!labels || !Object.keys(labels).length) return null;
    const env: Record<string, string | null> = {};
    for (const c of d?.spec?.template?.spec?.containers ?? []) for (const e of c?.env ?? []) {
      if ((WATCHED_ENV as readonly string[]).includes(e?.name)) env[e.name] = typeof e.value === "string" ? e.value : null;
    }
    return { ready: Number(d?.status?.readyReplicas ?? 0), desired: Number(d?.spec?.replicas ?? 0), selector: Object.entries(labels).map(([k, v]) => `${k}=${v}`).join(","), env };
  } catch { return null; }
}

/** One pod's log since the run started: the ingest's JSON lines tallied by route and outcome (no ids are kept). */
export function tallyLog(text: string): LogTally {
  const t: LogTally = { lines: 0, admitted: 0, refused: 0, retry: 0, deferred: 0, deadLettered: 0, uploads: 0, promoted: 0, firstWrites: 0, replayFast: 0, codes: {} };
  for (const line of text.split("\n")) {
    const at = line.indexOf("{");
    if (at < 0) continue;
    let rec: Record<string, unknown>;
    try { rec = JSON.parse(line.slice(at)); } catch { continue; }
    t.lines++;
    if (rec.route === "delivery.batches") t.uploads++;
    if (rec.route === "ingest.upload_admission" && typeof rec.promoted === "number") t.promoted += rec.promoted;
    if (rec.route !== "ingest.admission") continue;
    const outcome = rec.outcome, code = typeof rec.error_code === "string" ? rec.error_code : undefined;
    if (outcome === "admitted") { t.admitted++; if (code?.startsWith("accepted")) t.firstWrites++; else if (code === "replay_fast") t.replayFast++; }
    else if (outcome === "refused") t.refused++;
    else if (outcome === "deferred") t.deferred++;
    else if (outcome === "dead_lettered") t.deadLettered++;
    else if (outcome === "retry") t.retry++;
    if (code && outcome !== "admitted" && outcome !== "already_terminal") t.codes[code] = (t.codes[code] ?? 0) + 1;
  }
  return t;
}

// ======================================================================================================
// IO
// ======================================================================================================

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const decoder = new TextDecoder();

function semaphore(n: number) {
  let active = 0;
  const waiting: (() => void)[] = [];
  return async <T>(fn: () => Promise<T>): Promise<T> => {
    while (active >= n) await new Promise<void>((r) => waiting.push(r));
    active++;
    try { return await fn(); } finally { active--; waiting.shift()?.(); }
  };
}

interface Http { statuses: Record<string, number>; retryCodes: Record<string, number>; unexpected: number; targetRefusals: Record<string, number> }
const bump = (m: Record<string, number>, k: string) => { m[k] = (m[k] ?? 0) + 1; };
const retryAfterMs = (res: Response) => Math.min(15_000, Math.max(1_000, Number(res.headers.get("retry-after") ?? "2") * 1000 || 2_000));
const errorCode = (json: any): string => {
  const c = json?.error?.code ?? json?.error ?? json?.code;
  return typeof c === "string" && /^[a-z0-9_]{1,64}$/.test(c) ? c : "unknown";
};

/**
 * The local mode's stand-in for relay-control's upload credentials plus the desktop's create-only PUT: writes
 * `copies` concurrent create-only copies of the object into the upload inbox and answers its full key (or a code).
 */
export type DirectPut = (user: number, originId: string, body: { batch: { batch_id: string } }, bytes: Uint8Array, copies: number) => Promise<{ uploadKey: string } | { code: string }>;

class Run {
  readonly http: Http = { statuses: {}, retryCodes: {}, unexpected: 0, targetRefusals: {} };
  /** Requests sent per ingest URL (index into ingestUrls): proof the client spread its load. */
  readonly byTarget: number[];
  readonly limit: ReturnType<typeof semaphore>;
  readonly deadline: number;
  private witnessSeq = new Map<number, number>();
  private rr = 0;
  constructor(readonly c: ProofConfig, readonly tokens: string[], readonly startedMs: number, readonly ingestUrls: string[], readonly directPut?: DirectPut) {
    this.limit = semaphore(c.concurrency);
    this.deadline = startedMs + c.quiesceTimeoutS * 1000;
    this.byTarget = ingestUrls.map(() => 0);
  }
  /** Round-robin over the ingest URLs, one pick per HTTP attempt (a single URL is a load balancer's). */
  ingest(): string {
    const i = this.rr++ % this.ingestUrls.length;
    this.byTarget[i]!++;
    return this.ingestUrls[i]!;
  }
  tenant(u: number) { return { sub: this.c.users[u]!.userId, org: this.c.org, workspace: this.c.workspace }; }
  ledgerTenant(u: number): LedgerTenant { return { orgId: this.c.org, workspaceId: this.c.workspace, userId: this.c.users[u]!.userId }; }

  async fetchJson(url: string, init: RequestInit): Promise<{ res: Response; json: any }> {
    const res = await this.limit(() => fetch(url, { ...init, signal: AbortSignal.timeout(30_000) }));
    const text = await res.text();
    let json: any = null;
    try { json = JSON.parse(text); } catch {}
    bump(this.http.statuses, String(res.status));
    return { res, json };
  }

  /** POST one batch until a 200 (429/503 retried as the Desktop does). The receipt id, whether it was queued, or null. */
  async postBatch(u: number, body: unknown): Promise<{ receiptId: string; queued: boolean } | null> {
    for (let attempt = 0; attempt < 30 && Date.now() < this.deadline; attempt++) {
      let r;
      try {
        r = await this.fetchJson(`${this.ingest()}/v1/delivery/batches`, {
          method: "POST", headers: { authorization: `Bearer ${this.tokens[u]}`, "content-type": "application/json" }, body: JSON.stringify(body),
        });
      } catch { bump(this.http.retryCodes, "client_network_error"); await sleep(2_000); continue; }
      if (r.res.status === 200 && typeof r.json?.receiptId === "string") {
        return { receiptId: r.json.receiptId, queued: (r.res.headers.get("x-relay-admission") ?? "").includes("queued") };
      }
      if (r.res.status === 429 || r.res.status === 503) { bump(this.http.retryCodes, errorCode(r.json)); await sleep(retryAfterMs(r.res)); continue; }
      this.http.unexpected++;
      return null;
    }
    return null;
  }

  /** Poll GET /v1/delivery/admission (receipt or upload-key form) until a terminal state or the deadline. */
  async pollAdmission(u: number, query: string): Promise<{ terminal: Terminal; code?: string; receiptId?: string }> {
    let unknownSince = 0;
    while (Date.now() < this.deadline) {
      let r;
      try { r = await this.fetchJson(`${this.ingest()}/v1/delivery/admission?${query}`, { headers: { authorization: `Bearer ${this.tokens[u]}` } }); }
      catch { await sleep(2_000); continue; }
      if (r.res.status === 200) {
        const s = r.json?.state;
        if (s === "admitted" || s === "refused") return { terminal: s, ...(typeof r.json.code === "string" ? { code: r.json.code } : {}), ...(typeof r.json.receiptId === "string" ? { receiptId: r.json.receiptId } : {}) };
      } else if (r.res.status === 404) {
        // upload_unknown: a just-PUT object may not be listed yet; a long 404 is a lost upload.
        unknownSince ||= Date.now();
        if (Date.now() - unknownSince > 120_000) return { terminal: "pending", code: "upload_unknown" };
      } else if (r.res.status !== 503 && r.res.status !== 429) { this.http.unexpected++; return { terminal: "pending" }; }
      await sleep(1_500);
    }
    return { terminal: "pending" };
  }

  /** The presigned direct path (relay-upload-inbox-v1 sections 2, 4, 5): credentials, `copies` concurrent PUTs, poll. */
  async direct(u: number, originId: string, body: { batch: { batch_id: string } }, copies: number): Promise<{ terminal: Terminal; code?: string; receiptId?: string }> {
    const bytes = new Uint8Array(zstdCompressSync(Buffer.from(JSON.stringify(body))));
    if (this.directPut) {
      const put = await this.directPut(u, originId, body, bytes, copies);
      if ("code" in put) return { terminal: "unsent", code: put.code };
      return this.pollAdmission(u, `uploadKey=${encodeURIComponent(put.uploadKey)}`);
    }
    const sha = uploadObjectSha256Hex(bytes), checksum = uploadChecksumSha256(bytes);
    let creds: NonNullable<ReturnType<typeof parseUploadCredentialsResponse>> | null = null;
    for (let attempt = 0; attempt < 20 && !creds && Date.now() < this.deadline; attempt++) {
      const seq = this.witnessSeq.get(u) ?? 0;
      const rel = `o=${originId}/b=${batchSegment(body.batch.batch_id)}/d=${sha.slice(0, 32)}-w${seq}.json.zst`;
      let r;
      try {
        r = await this.fetchJson(`${this.c.controlUrl}/api/v1/workspaces/${encodeURIComponent(this.c.workspace)}/relayhistory/upload-credentials`, {
          method: "POST", headers: { authorization: `Bearer ${this.tokens[u]}`, "content-type": "application/json" },
          body: JSON.stringify({ originId, kind: "presigned", keys: [{ key: rel, sha256: checksum }] }),
        });
      } catch { await sleep(2_000); continue; }
      if (r.res.status === 429 || r.res.status === 503) { bump(this.http.retryCodes, errorCode(r.json)); await sleep(retryAfterMs(r.res)); continue; }
      const parsed = r.res.status === 200 ? parseUploadCredentialsResponse(r.json) : null;
      if (!parsed || parsed.kind !== "presigned") { this.http.unexpected++; return { terminal: "unsent", code: r.res.status === 404 ? "history_upload_direct_off" : "credentials_refused" }; }
      // The key carries the witness seq; learn the user's seq from the first answer and ask again with it.
      if (parsed.witnessSeq !== seq) { this.witnessSeq.set(u, parsed.witnessSeq); continue; }
      creds = parsed;
    }
    if (!creds) return { terminal: "unsent", code: "credentials_unavailable" };
    const target = creds.urls[0]!;
    // The PUT target is relay-control's answer, not an operator flag: check it like every other target before sending.
    const refusedUrl = uploadUrlRefusals(target.url);
    if (refusedUrl.length) { for (const code of refusedUrl) bump(this.http.targetRefusals, code); return { terminal: "unsent", code: refusedUrl[0]! }; }
    const headers: Record<string, string> = {
      "if-none-match": "*", "content-type": "application/json", "content-encoding": "zstd", "x-amz-checksum-sha256": checksum,
      [UPLOAD_WITNESS_METADATA_HEADER]: creds.witness,
      ...(creds.kmsKeyId ? { "x-amz-server-side-encryption": "aws:kms", "x-amz-server-side-encryption-aws-kms-key-id": creds.kmsKeyId } : {}),
    };
    const put = async () => {
      for (let attempt = 0; attempt < 6; attempt++) {
        try {
          const res = await this.limit(() => fetch(target.url, { method: "PUT", headers, body: bytes, signal: AbortSignal.timeout(30_000) }));
          await res.arrayBuffer().catch(() => undefined);
          bump(this.http.statuses, `put_${res.status}`);
          if (res.status === 200 || res.status === 412) return true;
          if (res.status < 500 && res.status !== 429) { this.http.unexpected++; return false; }
        } catch { bump(this.http.retryCodes, "client_network_error"); }
        await sleep(2_000);
      }
      return false;
    };
    const ok = (await Promise.all(Array.from({ length: copies }, put))).some(Boolean);
    if (!ok) return { terminal: "unsent", code: "put_failed" };
    return this.pollAdmission(u, `uploadKey=${encodeURIComponent(creds.keyPrefix + target.key)}`);
  }
}

interface Sent { chain: Chain; batch: PlannedBatch; batchId: string; recordIds: string[]; terminal: Terminal; code?: string; receiptId?: string; diverged?: boolean }

async function send(run: Run, chain: Chain, batch: PlannedBatch): Promise<Sent> {
  const body = await deliveryBatch(run.tenant(chain.user), { sessionId: chain.sessionId, originId: chain.originId, marker: chain.marker }, { batch: batch.label, revision: batch.revision });
  const base = { chain, batch, batchId: body.batch.batch_id, recordIds: body.batch.records.map((r) => r.record_id) };
  if (chain.path === "direct") return { ...base, ...(await run.direct(chain.user, chain.originId, body, batch.copies)) };
  const answers = await Promise.all(Array.from({ length: batch.copies }, () => run.postBatch(chain.user, body)));
  const got = answers.filter((a): a is NonNullable<typeof a> => a !== null);
  if (!got.length) return { ...base, terminal: "unsent" };
  const diverged = new Set(got.map((a) => a.receiptId)).size > 1;
  const { receiptId, queued } = got[0]!;
  if (!queued) return { ...base, terminal: "admitted", receiptId, diverged };
  const polled = await run.pollAdmission(chain.user, `originId=${encodeURIComponent(chain.originId)}&receiptId=${encodeURIComponent(receiptId)}`);
  return { ...base, ...polled, receiptId, diverged };
}

async function runChain(run: Run, chain: Chain, out: Sent[]): Promise<void> {
  for (const b of chain.batches) {
    const s = await send(run, chain, b);
    out.push(s);
    if (s.terminal !== "admitted") return; // the next revision would not be ordered after it
  }
}

/** Every object under `prefix` (which ends in `/`). An object key cannot end in `/`, so list the bare prefix and keep the slash-bounded keys. */
export async function listAll(store: ObjectStore, prefix: string, cap = 50_000) {
  const bare = prefix.endsWith("/") ? prefix.slice(0, -1) : prefix;
  const bounded = bare + "/";
  const items: { key: string; size: number }[] = [];
  let cursor: string | undefined;
  do {
    const page = await store.list(objectKey(bare), cursor);
    items.push(...page.items.filter((i) => String(i.key).startsWith(bounded)).map((i) => ({ key: String(i.key), size: i.size })));
    cursor = page.nextCursor;
  } while (cursor && items.length < cap);
  return items;
}

async function readJson(store: ObjectStore, key: string): Promise<any | null> {
  try { return JSON.parse(decoder.decode(await readAll((await store.getObject(objectKey(key))).body))); }
  catch (e) { if (isObjectStoreError(e, "not_found")) return null; throw e; }
}

function kubectl(c: ProofConfig, args: string[]): string | null {
  // preflightRefusals requires --kube-context with kubectl: never the operator's current context.
  if (!c.kubeContext || !c.kubeNamespace) return null;
  const r = spawnSync("kubectl", ["--context", c.kubeContext, "-n", c.kubeNamespace, ...args], { encoding: "utf8", timeout: 60_000, maxBuffer: 256 * 1024 * 1024 });
  return r.status === 0 ? r.stdout : null;
}

const INBOX_ATTEMPT = /\/r=[0-9a-f]{64}\/a=\d{15}-[0-9a-f]{16}\.ndjson\.zst$/;

async function main(argv: string[]): Promise<number> {
  let c: ProofConfig;
  try { c = parseProofArgs(argv); } catch { process.stdout.write(JSON.stringify({ refused: ["unknown_or_malformed_flag"] }) + "\n"); return 2; }
  const refusals = preflightRefusals(c, process.env);
  const runId = randomBytes(5).toString("hex");
  const chains = planBurst(runId, c.users.length, { sessionsPerOrigin: c.sessionsPerOrigin, revisions: c.revisions, retryCopies: c.retryCopies, direct: c.controlUrl !== undefined });
  process.stderr.write(JSON.stringify({ config: configSummary(c), plan: Number.isInteger(c.revisions) ? planCounts(chains) : null, run_id: runId }) + "\n");
  if (refusals.length) { process.stdout.write(JSON.stringify({ refused: refusals }) + "\n"); return 2; }
  if (c.planOnly) { process.stdout.write(JSON.stringify({ plan_only: true, run_id: runId, plan: planCounts(chains) }) + "\n"); return 0; }

  // ---- stores ----
  let credentials;
  try { credentials = await reshardCredentials(c.credentials as ReshardCredentialMode, process.env); }
  catch (e) { process.stdout.write(JSON.stringify({ refused: [/^[a-z0-9_]{1,64}$/.test((e as Error).message) ? (e as Error).message : "credentials_unavailable"] }) + "\n"); return 2; }
  const sse = c.sseKmsKeyId ? { serverSideEncryption: "aws:kms" as const, sseKmsKeyId: c.sseKmsKeyId } : {};
  const mainStore = createS3Store({ bucket: c.bucket, region: c.region, ...(c.endpoint ? { endpoint: c.endpoint } : {}), credentials, ...sse }) as unknown as ObjectStore;
  const store = c.ledgerBucket
    ? ledgerRoutedStore(mainStore, createS3Store({ bucket: c.ledgerBucket, region: c.ledgerRegion ?? c.region, ...(c.endpoint ? { endpoint: c.endpoint } : {}), credentials, ...sse }) as unknown as ObjectStore, c.keyRoot)
    : mainStore;

  // ---- runtime preflight: fence authority for the org, kill switch, replicas ----
  const pre: string[] = [];
  try {
    if (await readJson(store, fenceAuthorityStopKey(c.keyRoot)) !== null) pre.push("fence_authority_stop_present");
    const org = await readJson(store, orgFenceAuthorityKey(c.keyRoot, c.org));
    const covered = org !== null ? validOrgFenceAuthority(org) && org.orgId === c.org : validFenceAuthority(await readJson(store, fenceAuthorityKey(c.keyRoot)));
    if (!covered) pre.push("org_not_covered_by_fence_authority_marker");
  } catch { pre.push("store_unreadable"); }
  pre.push(...(await resetUserPrerunRefusals(store, c)));
  let replicas: Observation["replicas"] = { by: "operator_flag", ready: null };
  let selector: string | undefined;
  if (c.kubeNamespace && c.kubeDeployment) {
    const d = parseDeployment(kubectl(c, ["get", "deployment", c.kubeDeployment, "-o", "json"]) ?? "");
    if (!d) pre.push("kubectl_deployment_unreadable");
    else {
      replicas = { by: "kubectl", ready: d.ready, env: d.env };
      selector = d.selector;
      if (d.ready < 2) pre.push("fewer_than_two_ready_replicas");
      if (d.env.INGEST_LEDGER !== "s3") pre.push("ingest_ledger_not_s3");
      const lc = d.env.INGEST_LEDGER_LOG_COMMIT;
      if (lc !== undefined && lc !== null && lc !== "" && lc.toLowerCase() !== "off") pre.push("ledger_log_commit_not_off");
      process.stderr.write(JSON.stringify({ deployment: { ready: d.ready, desired: d.desired, env: d.env } }) + "\n");
    }
  }
  if (pre.length) { process.stdout.write(JSON.stringify({ refused: pre }) + "\n"); return 2; }

  const collectLogs = selector ? async (startedMs: number) => {
    const logs: Record<string, LogTally> = {};
    const pods = JSON.parse(kubectl(c, ["get", "pods", "-l", selector, "-o", "json"]) ?? "{}")?.items ?? [];
    let i = 0;
    for (const p of pods) {
      const name = p?.metadata?.name;
      if (typeof name !== "string") continue;
      const text = kubectl(c, ["logs", name, "--all-containers", `--since-time=${new Date(startedMs - 5_000).toISOString()}`]);
      if (text !== null) logs[`pod_${i++}`] = tallyLog(text);
    }
    return logs;
  } : undefined;
  const { receipt } = await runProof(c, {
    runId, chains, store, tokens: c.users.map((u) => process.env[u.tokenEnv]!), ingestUrls: [c.ingestUrl], replicas,
    ...(collectLogs ? { collectLogs } : {}),
  });
  const text = JSON.stringify(receipt, null, 2);
  process.stdout.write(text + "\n");
  if (c.receiptOut) writeFileSync(c.receiptOut, text + "\n");
  return receipt.verdict === "FAIL" ? 1 : 0;
}

/** resetOwnershipCheck over the store (read-only: one owners LIST + GETs and the fence). null: unreadable. */
export async function ownershipCheck(store: ObjectStore, keyRoot: string, tenant: LedgerTenant, runSessions: ReadonlySet<string>) {
  try {
    const io = s3LedgerObjects(store, keyRoot);
    const keys = s3LedgerKeys(keyRoot, tenant);
    const [owners, fence] = await Promise.all([io.listOwners(keys), io.readFence(keys.fence)]);
    return resetOwnershipCheck(owners, fence.state === "present" ? fence.fence : null, runSessions);
  } catch { return null; }
}

/**
 * Before any upload: the reset user must own no live session yet, so everything the mid-run reset tombstones is this
 * run's. A previous run's sessions are not live: its mid-run reset tombstoned the reset sessions and its end-of-run
 * reset (resetAfterSessions) the after sessions. A run that stopped before that leaves live stubs: this refuses then.
 */
export async function resetUserPrerunRefusals(store: ObjectStore, c: Pick<ProofConfig, "keyRoot" | "org" | "workspace" | "users">): Promise<string[]> {
  const own = await ownershipCheck(store, c.keyRoot, { orgId: c.org, workspaceId: c.workspace, userId: c.users[0]!.userId }, new Set());
  if (own === null) return ["reset_user_owners_unreadable"];
  return own.foreign > 0 ? ["reset_user_owns_sessions_outside_the_run"] : [];
}

export interface AfterReset { count: number | null; live: number | null; refused?: "foreign_sessions" | "owners_unreadable" }

/**
 * The end-of-run reset: the run's after sessions (created after the mid-run reset, so still live) are tombstoned by a
 * second user-wide reset of user 0, applied ONLY if the live sessions user 0 owns are a subset of `afterSessions`
 * (`originId\nsessionId`; the same read-only check as the mid-run reset). It never reaches further than the mid-run
 * reset could: anything else live refuses it. An after session that never landed has no stub and is simply not live.
 */
export async function resetAfterSessions(
  ledger: Pick<ReturnType<typeof s3Ledger>, "resetUploadedSessions">, store: ObjectStore, keyRoot: string, tenant: LedgerTenant,
  afterSessions: ReadonlySet<string>, requestId: string,
): Promise<AfterReset> {
  const own = await ownershipCheck(store, keyRoot, tenant, afterSessions);
  if (own === null) return { count: null, live: null, refused: "owners_unreadable" };
  if (own.foreign > 0) return { count: null, live: own.live, refused: "foreign_sessions" };
  try { return { count: await ledger.resetUploadedSessions(tenant, requestId), live: own.live }; }
  catch { return { count: null, live: own.live }; }
}

/** What a run needs besides its config: the stores, the tokens, where to send, and how to read each replica's log. */
export interface ProofContext {
  runId: string;
  chains: Chain[];
  /** The canonical store (ledger-routed when split): read-back and the reset's fence write. */
  store: ObjectStore;
  tokens: string[];
  /** One URL (a load balancer) or one per replica: every HTTP attempt picks the next one round-robin. */
  ingestUrls: string[];
  replicas: Observation["replicas"];
  /** Every replica's log since the run started, tallied (tallyLog), keyed by a non-identifying replica name. */
  collectLogs?: (startedMs: number) => Promise<Record<string, LogTally>>;
  /** The direct path without relay-control (local mode); unset, the presigned path through --control-url. */
  directPut?: DirectPut;
  requireAdmissionsPerReplica?: boolean;
}

/** The burst, the mid-burst reset, quiescence, the read-back and the verdict. Never throws on a failed check. */
export async function runProof(c: ProofConfig, ctx: ProofContext) {
  const { runId, chains, store } = ctx;
  const startedMs = Date.now();
  const run = new Run(c, ctx.tokens, startedMs, ctx.ingestUrls, ctx.directPut);
  const sent: Sent[] = [];

  // ---- burst ----
  const resetChains = chains.filter((ch) => ch.role === "reset");
  const others = chains.filter((ch) => ch.role === "main");
  const burst = Promise.all(others.map((ch) => runChain(run, ch, sent)));
  await Promise.all(resetChains.map((ch) => runChain(run, ch, sent)));

  // ---- mid-burst reset of user 0, raced by in-flight uploads of its sessions ----
  const reset: Observation["reset"] = { count: null, replay: null, resetSessions: resetChains.length, fenceResets: null };
  // The reset is user-wide: apply it only if what user 0 owns is exactly this run's sessions (read-only check).
  const own = await ownershipCheck(store, c.keyRoot, run.ledgerTenant(0), new Set(resetChains.map((ch) => `${ch.originId}\n${ch.sessionId}`)));
  if (own === null) reset.refused = "owners_unreadable";
  else if (own.foreign > 0) reset.refused = "foreign_sessions";
  const races = resetChains.map((ch) => send(run, ch, ch.race!));
  const ledger = s3Ledger(store, c.keyRoot, { acceptanceMode: "canonical", fenceAuthority: "required", claimCompletesResets: false });
  const requestId = `mrp-reset-${runId}`;
  if (!reset.refused) {
    try {
      reset.count = await ledger.resetUploadedSessions(run.ledgerTenant(0), requestId);
      reset.replay = await ledger.resetUploadedSessions(run.ledgerTenant(0), requestId);
    } catch { /* reset_not_run */ }
  }
  sent.push(...(await Promise.all(races)));
  // After the reset returned: re-uploads of the reset sessions must not land; new sessions must.
  const after = await Promise.all([
    ...resetChains.map((ch) => send(run, ch, ch.blocked!)),
    ...chains.filter((ch) => ch.role === "after").map((ch) => send(run, ch, ch.batches[0]!)),
  ]);
  sent.push(...after);
  await burst;

  // ---- quiescence: every batch record committed and nothing left in the inbox, DLQ or upload inbox ----
  const io = s3LedgerObjects(store, c.keyRoot);
  const wsPath = layoutComponent(c.workspace);
  const origins = [...new Map(chains.map((ch) => [ch.originId, ch])).values()];
  const scanStorage = async () => {
    let inboxPending = 0, deadLetters = 0, uploadInboxPending = 0;
    for (const ch of origins) {
      const tenant = run.ledgerTenant(ch.user);
      const inbox = originInboxDir(c.keyRoot, tenant, ch.originId);
      inboxPending += (await listAll(store, `${inbox}/`)).filter((i) => INBOX_ATTEMPT.test(i.key) && i.size >= INBOX_MIN_BYTES).length;
      // The dead-letter marker sits at the inbox path under the DLQ namespace (selfhost-admission.ts deadLetterKey).
      deadLetters += (await listAll(store, `${c.keyRoot}/${DEAD_LETTER_NAMESPACE}/${inbox.slice(`${c.keyRoot}/${INBOX_NAMESPACE}/`.length)}/`)).length;
      if (ch.path === "direct") uploadInboxPending += (await listAll(store, `${uploadKeyPrefix({ keyRoot: c.keyRoot, ...tenant })}o=${ch.originId}/`)).length;
    }
    return { inboxPending, uploadInboxPending, deadLetters };
  };
  const readRecord = async (s: Sent): Promise<{ record: BatchObservation["record"]; receiptMatches: boolean | null }> => {
    try {
      const got = await io.readBatch(s3LedgerKeys(c.keyRoot, run.ledgerTenant(s.chain.user)).batch(s.chain.originId, s.batchId));
      if (!got) return { record: "absent", receiptMatches: null };
      return { record: got.doc.state === "committed" ? "committed" : "pending", receiptMatches: s.receiptId ? got.doc.receiptId === s.receiptId : null };
    } catch { return { record: "invalid", receiptMatches: null }; }
  };
  let storage = { inboxPending: -1, uploadInboxPending: -1, deadLetters: -1 };
  let records: Awaited<ReturnType<typeof readRecord>>[] = [];
  let quiesced = false;
  while (true) {
    storage = await scanStorage();
    records = await Promise.all(sent.map(readRecord));
    const clean = storage.inboxPending === 0 && storage.uploadInboxPending === 0 && records.every((r) => r.record !== "pending");
    if (clean) { quiesced = true; break; }
    if (Date.now() > run.deadline + 120_000) break;
    await sleep(5_000);
  }

  // ---- read back: the revision index per record, the canonical manifest per session, the user-0 fence ----
  const sentByChain = new Map<Chain, Sent[]>();
  for (const s of sent) sentByChain.set(s.chain, [...(sentByChain.get(s.chain) ?? []), s]);
  const recordObs: RecordObservation[] = [];
  const sessionObs: SessionObservation[] = [];
  for (const ch of chains) {
    const mine = sentByChain.get(ch) ?? [];
    const keys = s3LedgerKeys(c.keyRoot, run.ledgerTenant(ch.user));
    const admittedRevs = mine.filter((s) => s.batch.expect === "admitted" && s.terminal === "admitted").map((s) => s.batch.revision);
    const raceLanded = mine.some((s) => s.batch.expect === "either" && s.terminal === "admitted");
    // Each admitted batch carries a new, higher revision of the same records, sent only after the previous one was
    // terminal: so the index ends on the highest admitted revision and the manifest was written once per batch.
    // A duplicate admission of a retried batch would write the manifest a second time (revision above max).
    const top = admittedRevs.length ? Math.max(...admittedRevs) : 0;
    const writes = admittedRevs.length;
    const range = ch.role === "reset" ? { min: top, max: top + (raceLanded ? 1 : 0) } : { min: top, max: top };
    for (const recordId of mine[0]?.recordIds ?? []) {
      let indexRevision: number | null = null;
      try { indexRevision = (await io.readIndex(keys.revisions(ch.originId, recordId)))?.doc.records[recordId]?.[0] ?? null; } catch { indexRevision = -1; }
      recordObs.push({ role: ch.role, ...range, absentOk: ch.role === "reset", indexRevision });
    }
    const machine = liveMachineId(c.workspace, ch.originId, c.users[ch.user]!.userId);
    const manifests = (await listAll(store, `${c.keyRoot}/${c.org}/v1/ws=${wsPath}/m=${machine}/`)).filter((i) => i.key.includes(`/s=${ch.sessionId}/`) && i.key.endsWith("/manifest.json"));
    const doc = manifests[0] ? await readJson(store, manifests[0].key) : null;
    sessionObs.push({ role: ch.role, min: writes, max: writes + (ch.role === "reset" && raceLanded ? 1 : 0), manifestRevision: typeof doc?.revision === "number" ? doc.revision : null, manifests: manifests.length });
  }
  try {
    const fence = await io.readFence(s3LedgerKeys(c.keyRoot, run.ledgerTenant(0)).fence);
    reset.fenceResets = fence.state === "present" ? fence.fence.resets : 0;
  } catch { reset.fenceResets = null; }

  // ---- end-of-run reset of the run's own after sessions (all read back above), so a rerun with this user 0 can start ----
  const afterSessions = new Set(chains.filter((ch) => ch.role === "after").map((ch) => `${ch.originId}\n${ch.sessionId}`));
  const cleanup = await resetAfterSessions(ledger, store, c.keyRoot, run.ledgerTenant(0), afterSessions, `mrp-cleanup-${runId}`);

  // ---- per-replica logs: evidence that both replicas did the work, and their retry codes ----
  const logs = ctx.collectLogs ? await ctx.collectLogs(startedMs) : undefined;

  const obs: Observation = {
    batches: sent.map((s, i) => ({ expect: s.batch.expect, path: s.chain.path, terminal: s.terminal, ...(s.code ? { refusedCode: s.code } : {}), ...records[i]!, ...(s.diverged ? { receiptsDiverged: true } : {}) })),
    records: recordObs, sessions: sessionObs, storage, http: run.http, reset, cleanup, replicas: ctx.replicas, ...(logs ? { logs } : {}), quiesced,
  };
  // Batches planned but never sent (a chain stopped at a non-admitted revision) are failures too.
  const planned = planCounts(chains).unique_batches;
  for (let k = sent.length; k < planned; k++) obs.batches.push({ expect: "admitted", path: "http", terminal: "unsent", record: "absent", receiptMatches: null });
  const verdict = evaluate(obs, { allowRetryCodes: c.allowRetryCodes, ...(ctx.requireAdmissionsPerReplica ? { requireAdmissionsPerReplica: true } : {}) });
  const receipt = {
    proof: "ingest-edge-multireplica", run_id: runId, started_at: new Date(startedMs).toISOString(), wall_s: Math.round((Date.now() - startedMs) / 1000),
    config: { ...configSummary(c), ingest_targets: ctx.ingestUrls.length, direct_upload: c.controlUrl !== undefined || ctx.directPut !== undefined },
    plan: planCounts(chains), http_requests_by_target: run.byTarget, ...verdict,
  };
  return { receipt, observation: obs };
}

if (import.meta.main) process.exit(await main(process.argv.slice(2)));
