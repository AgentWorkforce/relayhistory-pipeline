/**
 * The deliberate production path of the ledger-log stream operator (docs/design/s3-ledger-log-commit.md, "Production
 * pilot"): pure contract pieces only, no store access. The operations themselves (preflight, switch, close) live in
 * ledger-log-stream-ops.ts and rebuild the confirmation from their OWN preflight at apply time; the CLI is
 * scripts/ledger-log-stream.ts, the dormant GitHub path .github/workflows/ledger-log-pilot.yml.
 *
 * A production switch or close (a bucket WITHOUT the operator-written non-production marker) needs ALL of:
 *   - LEDGER_LOG_PRODUCTION_ENABLED (this file; false until a reviewed one-line change flips it);
 *   - the dedicated --production flag (the CLI; never together with --i-know-this-is-not-prod);
 *   - a plan (the same command without --apply): it runs the preflight and prints the confirmation;
 *   - the exact typed confirmation, which binds the action, the bucket and key root (sha256), the stream's hashed
 *     identity, the code sha, the preflight state digest, the plan reference and an expiry at most
 *     LEDGER_LOG_PRODUCTION_PLAN_TTL_MS after the plan. Apply rebuilds it byte for byte from a fresh preflight.
 *
 * No raw id, bucket name or key ever appears in the phrase, the plan or a receipt: only these digests.
 */
import { createHash } from "node:crypto";
import type { LedgerTenant } from "./ledger.ts";

/**
 * Dormant switch for production switch and close (both the CLI and the workflow). While false, a production apply
 * refuses with `production_dormant` before it writes anything; plans, status and measure (all read-only) stay allowed.
 * Enabling is a separate reviewed one-line change, made only with the owner's explicit OK for the pilot.
 */
export const LEDGER_LOG_PRODUCTION_ENABLED: boolean = false;

/** How long a plan's confirmation stays valid. Apply refuses an expiry in the past or further out than this. */
export const LEDGER_LOG_PRODUCTION_PLAN_TTL_MS = 30 * 60_000;
/**
 * A batch arrival rate at which a stream's admission latency becomes visible on the legacy chain (in-region, about
 * 13 batches/s with 8 in flight but serialized per user; docs/design/s3-ledger-log-commit.md "Why"). `measure`
 * reports whether a stream's peak reaches it.
 */
export const LOG_STREAM_VISIBLE_BATCHES_PER_S = 1.4;

export const LEDGER_LOG_PILOT_WORKFLOW = ".github/workflows/ledger-log-pilot.yml";
/**
 * The protected environment every dispatch of the pilot workflow runs in (its read and write roles trust only this
 * environment's OIDC subject). It does not exist until the owner creates it with the roles (infra in code):
 * the gate refuses until then.
 */
export const LEDGER_LOG_PILOT_ENVIRONMENT = "ledger-log-pilot";
export const LEDGER_LOG_PILOT_MODES = ["status", "measure", "plan-switch", "switch", "plan-close", "close"] as const;
export type LedgerLogPilotMode = (typeof LEDGER_LOG_PILOT_MODES)[number];
export const LEDGER_LOG_PILOT_PLAN_ARTIFACT = "ledger-log-pilot-plan";
export const LEDGER_LOG_PILOT_RECEIPT_ARTIFACT = "ledger-log-pilot-receipt";
export const ledgerLogPilotRunName = (mode: string, sha: string) => `ledger log pilot ${mode} ${sha}`;

export type ProductionAction = "switch" | "close";

const sha256 = (v: string) => createHash("sha256").update(v).digest("hex");
const HEX64 = /^[0-9a-f]{64}$/u, SHA40 = /^[0-9a-f]{40}$/u, PLAN_REF = /^(?:local|[1-9][0-9]{0,19})$/u;
/** Canonical millisecond ISO timestamp (what Date#toISOString prints). */
const ISO_MS = /^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z$/u;

/** Binds the bucket AND the key root: a phrase planned for one never applies to another. */
export const bucketBindingSha256 = (bucket: string, keyRoot: string) => sha256(JSON.stringify(["ledger-log-bucket-v1", bucket, keyRoot]));
/** The stream's hashed identity (the only stream identity a phrase, plan, receipt or log line carries). */
export const streamIdentitySha256 = (keyRoot: string, tenant: LedgerTenant, originId: string) =>
  sha256(JSON.stringify(["ledger-log-stream-v1", keyRoot, tenant.orgId, tenant.workspaceId, tenant.userId, originId]));
/**
 * The preflight state a plan was made against: the action, the stream's mode and (log mode) the entry-0 cutover, which
 * names this log instance. Deliberately NOT the head or legacy counts: on a live stream they move every second, and the
 * apply re-runs every preflight check anyway.
 */
export const preflightStateSha256 = (action: ProductionAction, mode: string, legacyCutover: string | null) =>
  sha256(JSON.stringify(["ledger-log-preflight-v1", action, mode, legacyCutover]));

export interface ProductionPhraseFields {
  action: ProductionAction;
  bucketSha256: string;
  streamSha256: string;
  codeSha: string;
  stateSha256: string;
  expiresAt: string;
  planRef: string;
}
export function productionConfirmation(f: ProductionPhraseFields): string {
  return `LEDGER-LOG-${f.action.toUpperCase()} BUCKET ${f.bucketSha256} STREAM ${f.streamSha256} SHA ${f.codeSha}` +
    ` STATE ${f.stateSha256} EXPIRES ${f.expiresAt} PLAN ${f.planRef}`;
}

/** The expiry carried in a confirmation, or undefined when the phrase is not shaped as one. */
export function confirmationExpiry(phrase: string): string | undefined {
  const m = / EXPIRES ([^ ]+) PLAN [^ ]+$/u.exec(phrase);
  return m && ISO_MS.test(m[1]!) && new Date(m[1]!).toISOString() === m[1] ? m[1] : undefined;
}

export interface ProductionContext {
  /** The bucket the store was created for (never printed: only its binding digest is). */
  bucket: string;
  /** The exact commit the operator runs (operatorCodeSha). */
  codeSha: string;
  /** "local" for the CLI, the plan run id for the workflow. */
  planRef: string;
  /** Apply only: the typed confirmation. A plan leaves it unset. */
  confirmation?: string;
  nowMs?: number;
  /** Tests only: overrides LEDGER_LOG_PRODUCTION_ENABLED. */
  enabled?: boolean;
}

/** Shape checks on the context, before any read. Returns a refusal code or undefined. */
export function productionContextRefusal(ctx: ProductionContext, apply: boolean): string | undefined {
  if (!SHA40.test(ctx.codeSha)) return "production_code_sha";
  if (!PLAN_REF.test(ctx.planRef)) return "production_plan_ref";
  if (!ctx.bucket) return "production_bucket";
  if (!apply) return ctx.confirmation === undefined ? undefined : "production_confirmation_on_plan";
  if (!(ctx.enabled ?? LEDGER_LOG_PRODUCTION_ENABLED)) return "production_dormant";
  if (typeof ctx.confirmation !== "string" || !ctx.confirmation.startsWith("LEDGER-LOG-")) return "production_confirmation_required";
  return undefined;
}

/**
 * The apply-time check: the confirmation equals the phrase rebuilt from the fresh preflight (with the expiry the
 * phrase carries), and that expiry is in the future but no further than one plan TTL away.
 */
export function confirmationRefusal(ctx: ProductionContext, rebuilt: Omit<ProductionPhraseFields, "expiresAt">): string | undefined {
  const phrase = ctx.confirmation ?? "";
  const expiresAt = confirmationExpiry(phrase);
  if (expiresAt === undefined) return "production_confirmation_mismatch";
  if (!HEX64.test(rebuilt.bucketSha256) || !HEX64.test(rebuilt.streamSha256) || !HEX64.test(rebuilt.stateSha256))
    return "production_confirmation_mismatch";
  if (phrase !== productionConfirmation({ ...rebuilt, expiresAt })) return "production_confirmation_mismatch";
  const now = ctx.nowMs ?? Date.now(), at = Date.parse(expiresAt);
  if (!(at > now)) return "production_confirmation_expired";
  if (at - now > LEDGER_LOG_PRODUCTION_PLAN_TTL_MS) return "production_confirmation_expiry_too_far";
  return undefined;
}

/**
 * The commit the operator actually runs. In GitHub Actions: GITHUB_SHA (the gate pins it to the live main head).
 * Locally: `git rev-parse HEAD`, refused unless the worktree is clean (no local edits run under a reviewed sha) and HEAD
 * is on the local origin/main history (reviewed, landed code only). `git` runs one git command and returns its stdout,
 * or throws on a non-zero exit.
 */
export function operatorCodeSha(env: Record<string, string | undefined>, git: (args: string[]) => string): string {
  const refuse = (code: string): never => { throw Object.assign(new Error(code), { code }); };
  if (env.GITHUB_ACTIONS === "true") {
    if (!SHA40.test(env.GITHUB_SHA ?? "")) refuse("production_code_sha");
    return env.GITHUB_SHA!;
  }
  let head: string;
  try { head = git(["rev-parse", "HEAD"]).trim(); } catch { return refuse("production_code_sha"); }
  if (!SHA40.test(head)) refuse("production_code_sha");
  let dirty: string;
  try { dirty = git(["status", "--porcelain", "--untracked-files=no"]); } catch { return refuse("production_code_sha"); }
  if (dirty.trim() !== "") refuse("production_worktree_dirty");
  try { git(["merge-base", "--is-ancestor", "HEAD", "origin/main"]); } catch { return refuse("production_code_not_on_main"); }
  return head;
}

const refuseCode = (code: string): never => { throw Object.assign(new Error(code), { code }); };
const RUN_ID = /^[1-9][0-9]{0,19}$/u;

export interface LedgerLogPilotDispatch {
  mode: LedgerLogPilotMode;
  sha: string;
  /** switch/close: the plan-<action> run this apply is bound to; null otherwise. */
  planRun: string | null;
  confirmation: string;
  /** measure only (seconds); 600 by default. */
  windowSeconds: number;
}
export const pilotAction = (mode: LedgerLogPilotMode): ProductionAction | null =>
  mode === "switch" || mode === "plan-switch" ? "switch" : mode === "close" || mode === "plan-close" ? "close" : null;
export const isPilotApply = (mode: LedgerLogPilotMode) => mode === "switch" || mode === "close";

/**
 * The workflow's dispatch inputs (env LEDGER_LOG_PILOT_*), shape-checked before any GitHub read or credential. Apply
 * modes refuse while LEDGER_LOG_PRODUCTION_ENABLED is false; the confirmation must already carry this sha and plan run
 * (the operation then rebuilds it byte for byte).
 */
export function ledgerLogPilotDispatch(env: Record<string, string | undefined>, enabled: boolean = LEDGER_LOG_PRODUCTION_ENABLED): LedgerLogPilotDispatch {
  const mode = env.LEDGER_LOG_PILOT_MODE as LedgerLogPilotMode, sha = env.LEDGER_LOG_PILOT_SHA ?? "",
    planRun = env.LEDGER_LOG_PILOT_PLAN_RUN ?? "", confirmation = env.LEDGER_LOG_PILOT_CONFIRMATION ?? "",
    window = env.LEDGER_LOG_PILOT_WINDOW_SECONDS ?? "";
  if (!LEDGER_LOG_PILOT_MODES.includes(mode) || !SHA40.test(sha)) refuseCode("ledger_log_pilot_inputs");
  const apply = isPilotApply(mode);
  if (apply && enabled !== true) refuseCode("ledger_log_pilot_dormant");
  if (apply ? !RUN_ID.test(planRun) : planRun !== "") refuseCode("ledger_log_pilot_plan_run");
  if (!apply && confirmation !== "") refuseCode("ledger_log_pilot_confirmation");
  if (apply && !(confirmation.startsWith(`LEDGER-LOG-${pilotAction(mode)!.toUpperCase()} BUCKET `) &&
      confirmation.includes(` SHA ${sha} STATE `) && confirmation.endsWith(` PLAN ${planRun}`) && confirmationExpiry(confirmation) !== undefined))
    refuseCode("ledger_log_pilot_confirmation");
  const windowSeconds = window === "" ? 600 : /^[1-9][0-9]{0,5}$/u.test(window) && Number(window) <= 86_400 ? Number(window) : NaN;
  if (Number.isNaN(windowSeconds) || (mode !== "measure" && window !== "")) refuseCode("ledger_log_pilot_window");
  return { mode, sha, planRun: apply ? planRun : null, confirmation, windowSeconds };
}

const BUCKET = /^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/u;
const REGION = /^[a-z]{2}(-gov)?-[a-z]+-[0-9]$/u;
const KMS_KEY_ARN = /^arn:aws:kms:([a-z]{2}(?:-gov)?-[a-z]+-[0-9]):([0-9]{12}):key\/(?:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}|mrk-[0-9a-f]{32})$/u;
const ROLE_ARN = /^arn:aws:iam::([0-9]{12}):role\/[A-Za-z0-9+=,.@_-]{1,64}$/u;
const KEY_ROOT = /^[a-z0-9_.-]{1,64}$/u;

/**
 * Bucket, region, key root, KMS key and the two roles, from the pilot environment's protected variables
 * (LEDGER_LOG_PILOT_*), shape-checked; never printed. The read role serves status, measure and the plans; the write
 * role (the operator grants of stacks/aws/ledger-log-commit-policy.ts) only switch and close.
 */
export interface LedgerLogPilotTarget { bucket: string; region: string; keyRoot: string; kmsKeyArn: string; readRoleArn: string; writeRoleArn: string }
export function ledgerLogPilotTarget(env: Record<string, string | undefined>): LedgerLogPilotTarget {
  const bucket = env.LEDGER_LOG_PILOT_BUCKET ?? "", region = env.LEDGER_LOG_PILOT_REGION ?? "", kmsKeyArn = env.LEDGER_LOG_PILOT_KMS_KEY_ARN ?? "",
    keyRoot = env.LEDGER_LOG_PILOT_KEY_ROOT || "96bulls",
    readRoleArn = env.LEDGER_LOG_PILOT_READ_ROLE_ARN ?? "", writeRoleArn = env.LEDGER_LOG_PILOT_WRITE_ROLE_ARN ?? "";
  if (!BUCKET.test(bucket) || bucket.includes("..")) refuseCode("ledger_log_pilot_target_bucket");
  if (!REGION.test(region)) refuseCode("ledger_log_pilot_target_region");
  if (!KEY_ROOT.test(keyRoot)) refuseCode("ledger_log_pilot_target_key_root");
  const key = KMS_KEY_ARN.exec(kmsKeyArn);
  if (!key || key[1] !== region) return refuseCode("ledger_log_pilot_target_kms");
  const read = ROLE_ARN.exec(readRoleArn), write = ROLE_ARN.exec(writeRoleArn);
  if (!read || !write || read[1] !== key[2] || write[1] !== key[2] || readRoleArn === writeRoleArn) refuseCode("ledger_log_pilot_target_roles");
  return { bucket, region, keyRoot, kmsKeyArn, readRoleArn, writeRoleArn };
}

/**
 * The pilot stream for the workflow: the protected environment's SECRET LEDGER_LOG_PILOT_STREAM (so this public
 * repository's logs mask it), `{"org":..,"workspace":..,"user":..,"origin":..}`. Never a dispatch input, never argv.
 */
export const PILOT_ID = /^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$/u;
export function pilotStream(raw: string | undefined): { tenant: LedgerTenant; originId: string } {
  const refuse = (): never => { throw Object.assign(new Error("ledger_log_pilot_stream"), { code: "ledger_log_pilot_stream" }); };
  let v: unknown;
  try { v = JSON.parse(raw ?? ""); } catch { return refuse(); }
  const o = v as Record<string, unknown> | null;
  if (o === null || typeof o !== "object" || Array.isArray(o)) return refuse();
  const keys = Object.keys(o).sort();
  if (keys.join(",") !== "org,origin,user,workspace") refuse();
  for (const k of keys) if (typeof o[k] !== "string" || !PILOT_ID.test(o[k] as string)) refuse();
  return { tenant: { orgId: o.org as string, workspaceId: o.workspace as string, userId: o.user as string }, originId: o.origin as string };
}
