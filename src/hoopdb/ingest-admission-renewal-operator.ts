/**
 * CI operator that renews buffered-ingest admission: sessions.ingest_lazy_policy.valid_until (055000) and every
 * sessions.ingest_buffer_admission.valid_until (050000) move together to one new expiry in ONE owner transaction.
 * No migration: plain statements over the migrator's owner connection (the guarded schema release's verify-full
 * TLS client, proven owner by requireReleaseOwner). Workflow: .github/workflows/ingest-admission-renewal.yml;
 * docs/design/zero-503-schema-windows.md "Admission renewal".
 *
 *   readback  READ ONLY snapshot; always allowed (also while ADMISSION_RENEWAL_ENABLED is false). Reports the
 *             verdicts and the first drift code instead of refusing on drift.
 *   plan      READ ONLY; writes nothing. Refuses on any drift. Emits the plan artifact: capturedAt (database
 *             clock, canonical milliseconds), oldValidUntil, renewTo = capturedAt + exactly 30 days,
 *             admissionCount, orgsSha256, descriptorSha256, generation, sha, planRun.
 *   renew     typed `RENEW-ADMISSION <project> <sha> GENERATION <g> FROM <old> TO <renewTo> ROWS <n> ORGS <d>
 *             DESCRIPTOR <d> PLAN <run>` built from the plan artifact (byte equality). Locks, in order:
 *             pg_advisory_xact_lock(961996,1); LOCK TABLE ingest_buffer_admission IN SHARE ROW EXCLUSIVE MODE
 *             (conflicts with INSERT/UPDATE/DELETE and with itself, so no admission row appears or changes and
 *             two renewals serialize; plain reads and the guard's FOR SHARE still proceed); policy FOR UPDATE;
 *             rows FOR UPDATE. Recomputes the snapshot and requires it to equal the plan, sets the policy through
 *             the owner-only sessions.ingest_lazy_policy_set with identical immutable fields, CAS-updates every
 *             admission row from the exact old expiry and identity (row count asserted), re-reads every
 *             postcondition and runs sessions.ingest_lazy_guard for every org, then commits. Any error rolls
 *             back. After COMMIT there is no inverse: an emergency closure is separately authorized.
 *
 * Canonical timestamps are ISO-8601 UTC with exactly three fractional digits and Z, rendered by the database
 * and compared both as those bytes and as database epoch microseconds; an expiry with sub-millisecond digits is
 * refused (it would have two renderings). Never prints org ids or identity hashes; receipts carry counts,
 * canonical timestamps, an 8-character generation prefix, the bound digests and verdicts.
 */
import { createHash } from "node:crypto";
import type { MigrationClient } from "./migrate.ts";
import { ROLE_VALIDATION_NAME, ROLE_VALIDATION_TABLE } from "../shared/db.ts";
import { SCHEMA_RELEASE } from "./schema-release-contract.ts";
import {
  ADMISSION_RENEWAL_ENABLED,
  ADMISSION_RENEWAL_HORIZON_MS,
  ADMISSION_RENEWAL_PLAN_MAX_AGE_MS,
} from "./admission-renewal-contract.ts";

export type AdmissionRenewalMode = "plan" | "renew" | "readback";
export interface AdmissionRenewalPlan {
  capturedAt: string;
  oldValidUntil: string;
  renewTo: string;
  admissionCount: number;
  orgsSha256: string;
  descriptorSha256: string;
  generation: string;
  sha: string;
  planRun: string;
}
export const ADMISSION_RENEWAL_PLAN_KEYS = [
  "capturedAt", "oldValidUntil", "renewTo", "admissionCount", "orgsSha256", "descriptorSha256", "generation", "sha",
  "planRun",
] as const;

/** The dispatch inputs alone (what the GitHub gate can check before the plan artifact is downloaded). */
export interface AdmissionRenewalDispatch {
  mode: AdmissionRenewalMode;
  sha: string;
  /** renew: the plan run id input; plan/readback: null. */
  planRun: string | null;
  confirmation: string;
}
export interface AdmissionRenewalInput {
  mode: AdmissionRenewalMode;
  sha: string;
  project: string;
  /** generation() of the repository at the dispatched sha. */
  expectedGeneration: string;
  /** plan: this run's id (recorded in the artifact); renew: the plan run id; readback: null. */
  planRun: string | null;
  plan: AdmissionRenewalPlan | null;
  approvalSha256: string | null;
}

const HEX64 = /^[0-9a-f]{64}$/u, SHA40 = /^[0-9a-f]{40}$/u, RUN_ID = /^[1-9][0-9]{0,19}$/u;
/** The one canonical timestamp form: what Date#toISOString renders for a whole-millisecond UTC instant. */
const CANONICAL = /^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z$/u;
export const refuseAdmissionRenewal = (code: string): never => {
  throw Object.assign(new Error(code), { code });
};
const refuse = refuseAdmissionRenewal;
const sha256 = (text: string) => createHash("sha256").update(text).digest("hex");

/** A canonical timestamp's epoch milliseconds, or null when the bytes are not the one canonical rendering. */
export function canonicalMs(value: unknown): number | null {
  if (typeof value !== "string" || !CANONICAL.test(value)) return null;
  const ms = Date.parse(value);
  return Number.isFinite(ms) && new Date(ms).toISOString() === value ? ms : null;
}

/** The exact typed confirmation for renew; plan and readback take "". */
export function admissionRenewalConfirmation(mode: AdmissionRenewalMode, project: string, plan: AdmissionRenewalPlan | null): string {
  if (mode !== "renew" || plan === null) return "";
  return `RENEW-ADMISSION ${project} ${plan.sha} GENERATION ${plan.generation} FROM ${plan.oldValidUntil} TO ${plan.renewTo}` +
    ` ROWS ${plan.admissionCount} ORGS ${plan.orgsSha256} DESCRIPTOR ${plan.descriptorSha256} PLAN ${plan.planRun}`;
}

/** Mode, sha, plan run and dormant switch; refuses before any credential is read. */
export function admissionRenewalDispatch(
  env: Record<string, string | undefined>,
  enabled: boolean = ADMISSION_RENEWAL_ENABLED,
): AdmissionRenewalDispatch {
  const mode = env.RENEWAL_MODE as AdmissionRenewalMode, sha = env.RENEWAL_SHA ?? "",
    planRun = env.RENEWAL_PLAN_RUN ?? "", confirmation = env.RENEWAL_CONFIRMATION ?? "";
  if ((mode !== "plan" && mode !== "renew" && mode !== "readback") || !SHA40.test(sha))
    refuse("admission_renewal_inputs");
  if (mode !== "readback" && enabled !== true) refuse("admission_renewal_dormant");
  if (mode === "renew" ? !RUN_ID.test(planRun) : planRun !== "") refuse("admission_renewal_plan_run");
  if (mode !== "renew" && confirmation !== "") refuse("admission_renewal_confirmation");
  if (mode === "renew" && !confirmation.startsWith("RENEW-ADMISSION ")) refuse("admission_renewal_confirmation");
  return { mode, sha, planRun: mode === "renew" ? planRun : null, confirmation };
}

/** Strict plan artifact: exact keys, canonical timestamps, renewTo = capturedAt + exactly the horizon, renewTo > old. */
export function parseAdmissionRenewalPlan(value: unknown): AdmissionRenewalPlan {
  if (value === null || typeof value !== "object" || Array.isArray(value)) return refuse("admission_renewal_plan");
  const p = value as Record<string, unknown>, keys = Object.keys(p).sort();
  if (keys.length !== ADMISSION_RENEWAL_PLAN_KEYS.length ||
      keys.some((k, i) => k !== [...ADMISSION_RENEWAL_PLAN_KEYS].sort()[i]))
    refuse("admission_renewal_plan");
  const captured = canonicalMs(p.capturedAt), old = canonicalMs(p.oldValidUntil), to = canonicalMs(p.renewTo);
  if (captured === null || old === null || to === null || !Number.isSafeInteger(p.admissionCount) ||
      (p.admissionCount as number) < 1 || typeof p.orgsSha256 !== "string" || !HEX64.test(p.orgsSha256) ||
      typeof p.descriptorSha256 !== "string" || !HEX64.test(p.descriptorSha256) ||
      typeof p.generation !== "string" || !HEX64.test(p.generation) || typeof p.sha !== "string" || !SHA40.test(p.sha) ||
      typeof p.planRun !== "string" || !RUN_ID.test(p.planRun))
    refuse("admission_renewal_plan");
  if (to !== captured! + ADMISSION_RENEWAL_HORIZON_MS) refuse("admission_renewal_plan_target");
  if (!(to! > old!)) refuse("admission_renewal_not_extension");
  return p as unknown as AdmissionRenewalPlan;
}

/**
 * Full inputs. plan: `GITHUB_RUN_ID` becomes the recorded plan run. renew: `plan` is the downloaded artifact, bound
 * to the dispatched sha, plan run and exact-SHA generation, under 24h old, and the confirmation must equal the
 * phrase built from it byte for byte.
 */
export function admissionRenewalInputs(
  env: Record<string, string | undefined>,
  options: { expectedGeneration: string; plan?: unknown; enabled?: boolean; project?: string; now?: number },
): AdmissionRenewalInput {
  const d = admissionRenewalDispatch(env, options.enabled ?? ADMISSION_RENEWAL_ENABLED);
  const project = options.project ?? SCHEMA_RELEASE.project, now = options.now ?? Date.now();
  if (!HEX64.test(options.expectedGeneration ?? "")) refuse("admission_renewal_generation");
  if (!/^[a-z0-9]{20}$/u.test(project)) refuse("admission_renewal_project");
  let planRun: string | null = null, plan: AdmissionRenewalPlan | null = null;
  if (d.mode === "plan") {
    if (!RUN_ID.test(env.GITHUB_RUN_ID ?? "")) refuse("admission_renewal_plan_run");
    planRun = env.GITHUB_RUN_ID!;
  } else if (d.mode === "renew") {
    plan = parseAdmissionRenewalPlan(options.plan);
    if (plan.sha !== d.sha) refuse("admission_renewal_plan_sha");
    if (plan.planRun !== d.planRun) refuse("admission_renewal_plan_run");
    if (plan.generation !== options.expectedGeneration) refuse("admission_renewal_generation_stale");
    const age = now - Date.parse(plan.capturedAt);
    if (!(age >= -5 * 60 * 1000 && age < ADMISSION_RENEWAL_PLAN_MAX_AGE_MS)) refuse("admission_renewal_plan_stale");
    planRun = d.planRun;
  }
  if (d.confirmation !== admissionRenewalConfirmation(d.mode, project, plan)) refuse("admission_renewal_confirmation");
  return {
    mode: d.mode, sha: d.sha, project, expectedGeneration: options.expectedGeneration, planRun, plan,
    approvalSha256: d.mode === "renew" ? sha256(d.confirmation) : null,
  };
}

const ts = (column: string) => `to_char(${column} AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')`;
const us = (column: string) => `(extract(epoch FROM ${column})*1000000)::bigint::text`;
const POLICY_SQL = `SELECT baseline_mode,generation,source_identity_sha256,control_epoch,privacy_baseline_sha256,
 retention_contract_sha256,source_proof_sha256,issuer,${ts("valid_until")} AS valid_until,${us("valid_until")} AS valid_until_us,
 ${ts("updated_at")} AS updated_at,${us("updated_at")} AS updated_at_us FROM sessions.ingest_lazy_policy`;
const ROWS_SQL = `SELECT org_id,schema_generation,control_epoch,privacy_baseline_sha256,retention_contract_sha256,
 ${ts("valid_until")} AS valid_until,${us("valid_until")} AS valid_until_us FROM sessions.ingest_buffer_admission
 ORDER BY org_id COLLATE "C"`;
const NOW_SQL = `SELECT ${ts("n")} AS now,${us("n")} AS now_us FROM (SELECT date_trunc('milliseconds',clock_timestamp()) AS n) t`;

interface PolicyRow {
  baseline_mode: string; generation: string; source_identity_sha256: string; control_epoch: string;
  privacy_baseline_sha256: string; retention_contract_sha256: string; source_proof_sha256: string; issuer: string;
  valid_until: string; valid_until_us: string; updated_at: string; updated_at_us: string;
}
interface AdmissionRow {
  org_id: string; schema_generation: string; control_epoch: string; privacy_baseline_sha256: string;
  retention_contract_sha256: string; valid_until: string; valid_until_us: string;
}
export interface AdmissionRenewalVerdicts {
  singlePolicy: boolean; markerPass: boolean; generationMatchesMarker: boolean; generationMatchesSha: boolean;
  rowsPresent: boolean; identityMatches: boolean; uniformExpiry: boolean; canonicalExpiry: boolean; unexpired: boolean;
}
export interface AdmissionRenewalSnapshot {
  drift: string | null;
  verdicts: AdmissionRenewalVerdicts;
  policy: PolicyRow | null;
  generation: string | null;
  oldValidUntil: string | null;
  oldValidUntilUs: bigint | null;
  admissionCount: number;
  orgsSha256: string;
  descriptorSha256: string | null;
  minValidUntil: string | null;
  maxValidUntil: string | null;
  nowUs: bigint;
  now: string;
}

/** The ordered descriptor: every immutable policy field, updated_at, generation, old expiry, count and roster. */
export function admissionRenewalDescriptor(policy: PolicyRow, count: number, orgsSha256: string): string {
  return sha256(JSON.stringify([
    ["descriptor", "ingest-admission-renewal/v1"],
    ["baselineMode", policy.baseline_mode], ["issuer", policy.issuer], ["generation", policy.generation],
    ["sourceIdentitySha256", policy.source_identity_sha256], ["sourceProofSha256", policy.source_proof_sha256],
    ["controlEpoch", policy.control_epoch], ["privacyBaselineSha256", policy.privacy_baseline_sha256],
    ["retentionContractSha256", policy.retention_contract_sha256],
    ["updatedAt", policy.updated_at], ["updatedAtMicros", policy.updated_at_us],
    ["validUntil", policy.valid_until], ["validUntilMicros", policy.valid_until_us],
    ["admissionCount", count], ["orgsSha256", orgsSha256],
  ]));
}

/** sha256 of the org ids joined with "\n", in C (byte) order as the database sorted them. */
export const admissionRenewalOrgsDigest = (orgs: readonly string[]) => sha256(orgs.join("\n"));

/** One consistent read of the authority; `lock` takes policy and rows FOR UPDATE (renew only). */
async function snapshot(client: MigrationClient, expectedGeneration: string, lock: boolean): Promise<AdmissionRenewalSnapshot> {
  const clock = (await client.query<{ now: string; now_us: string }>(NOW_SQL)).rows[0]!;
  const nowUs = BigInt(clock.now_us);
  const marker = (await client.query<{ generation: string; status: string }>(
    `SELECT generation,status FROM ${ROLE_VALIDATION_TABLE} WHERE name=$1${lock ? " FOR SHARE" : ""}`,
    [ROLE_VALIDATION_NAME])).rows;
  const policies = (await client.query<PolicyRow>(POLICY_SQL + (lock ? " FOR UPDATE" : ""))).rows;
  const rows = (await client.query<AdmissionRow>(ROWS_SQL + (lock ? " FOR UPDATE" : ""))).rows;
  const policy = policies.length === 1 ? policies[0]! : null;
  const orgs = rows.map((r) => r.org_id), orgsSha256 = admissionRenewalOrgsDigest(orgs);
  const v: AdmissionRenewalVerdicts = {
    singlePolicy: policies.length === 1,
    markerPass: marker.length === 1 && marker[0]!.status === "pass",
    generationMatchesMarker: policy !== null && marker.length === 1 && marker[0]!.generation === policy.generation,
    generationMatchesSha: policy !== null && policy.generation === expectedGeneration,
    rowsPresent: rows.length > 0,
    identityMatches: policy !== null && rows.every((r) => r.schema_generation === policy.generation &&
      r.control_epoch === policy.control_epoch && r.privacy_baseline_sha256 === policy.privacy_baseline_sha256 &&
      r.retention_contract_sha256 === policy.retention_contract_sha256),
    canonicalExpiry: policy !== null && canonicalMs(policy.valid_until) !== null &&
      BigInt(canonicalMs(policy.valid_until)!) * 1000n === BigInt(policy.valid_until_us),
    uniformExpiry: policy !== null && rows.every((r) => r.valid_until === policy.valid_until &&
      r.valid_until_us === policy.valid_until_us),
    unexpired: policy !== null && BigInt(policy.valid_until_us) > nowUs &&
      rows.every((r) => BigInt(r.valid_until_us) > nowUs),
  };
  const drift = !v.singlePolicy ? (policies.length === 0 ? "admission_renewal_policy_missing" : "admission_renewal_policy_extra")
    : !v.markerPass ? "admission_renewal_marker_not_pass"
    : !v.generationMatchesMarker ? "admission_renewal_generation_drift"
    : !v.generationMatchesSha ? "admission_renewal_generation_stale"
    : !v.rowsPresent ? "admission_renewal_no_rows"
    : !v.identityMatches ? "admission_renewal_identity_drift"
    : !v.canonicalExpiry ? "admission_renewal_timestamp_precision"
    : !v.uniformExpiry ? "admission_renewal_mixed_expiry"
    : !v.unexpired ? "admission_renewal_expired"
    : null;
  const sorted = rows.map((r) => r.valid_until_us).sort((a, b) => (BigInt(a) < BigInt(b) ? -1 : BigInt(a) > BigInt(b) ? 1 : 0));
  const byUs = (u: string | undefined) => rows.find((r) => r.valid_until_us === u)?.valid_until ?? null;
  return {
    drift, verdicts: v, policy,
    generation: policy?.generation ?? null,
    oldValidUntil: policy?.valid_until ?? null,
    oldValidUntilUs: policy ? BigInt(policy.valid_until_us) : null,
    admissionCount: rows.length,
    orgsSha256,
    descriptorSha256: policy ? admissionRenewalDescriptor(policy, rows.length, orgsSha256) : null,
    minValidUntil: byUs(sorted[0]), maxValidUntil: byUs(sorted[sorted.length - 1]),
    nowUs, now: clock.now,
  };
}

export interface AdmissionRenewalReceipt {
  status: "ADMISSION_RENEWAL_READBACK" | "ADMISSION_RENEWAL_PLANNED" | "ADMISSION_RENEWED";
  mode: AdmissionRenewalMode;
  sha: string;
  generationPrefix: string | null;
  policyValidUntilBefore: string | null;
  policyValidUntilAfter: string | null;
  admissionRows: number;
  admissionMinValidUntil: string | null;
  admissionMaxValidUntil: string | null;
  orgsSha256: string;
  descriptorSha256: string | null;
  renewTo: string | null;
  planRun: string | null;
  drift: string | null;
  verdicts: AdmissionRenewalVerdicts;
  committed: boolean;
  approvalSha256: string | null;
}
export interface AdmissionRenewalResult {
  receipt: AdmissionRenewalReceipt;
  /** plan mode only: the artifact renew consumes. */
  plan: AdmissionRenewalPlan | null;
}

function receiptOf(input: AdmissionRenewalInput, before: AdmissionRenewalSnapshot, after: AdmissionRenewalSnapshot,
  status: AdmissionRenewalReceipt["status"], renewTo: string | null, committed: boolean): AdmissionRenewalReceipt {
  return {
    status, mode: input.mode, sha: input.sha,
    generationPrefix: after.generation?.slice(0, 8) ?? null,
    policyValidUntilBefore: before.oldValidUntil,
    policyValidUntilAfter: after.oldValidUntil,
    admissionRows: after.admissionCount,
    admissionMinValidUntil: after.minValidUntil,
    admissionMaxValidUntil: after.maxValidUntil,
    orgsSha256: after.orgsSha256,
    descriptorSha256: before.descriptorSha256,
    renewTo, planRun: input.planRun, drift: after.drift, verdicts: after.verdicts, committed,
    approvalSha256: input.approvalSha256,
  };
}

/** Run one mode. The caller has already proved the owner identity and the dispatch gates. */
export async function runAdmissionRenewal(client: MigrationClient, input: AdmissionRenewalInput): Promise<AdmissionRenewalResult> {
  const readOnly = input.mode !== "renew";
  await client.query(readOnly ? "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY" : "BEGIN");
  try {
    await client.query("SET LOCAL statement_timeout = '30s'");
    await client.query("SET LOCAL lock_timeout = '5s'");
    await client.query("SELECT pg_advisory_xact_lock(961996, 1)");
    if (input.mode === "readback") {
      const s = await snapshot(client, input.expectedGeneration, false);
      await client.query("ROLLBACK");
      return { receipt: receiptOf(input, s, s, "ADMISSION_RENEWAL_READBACK", null, false), plan: null };
    }
    if (input.mode === "plan") {
      const s = await snapshot(client, input.expectedGeneration, false);
      if (s.drift !== null) refuse(s.drift);
      const capturedMs = canonicalMs(s.now);
      if (capturedMs === null || BigInt(capturedMs) * 1000n !== s.nowUs) refuse("admission_renewal_timestamp_precision");
      const renewTo = new Date(capturedMs! + ADMISSION_RENEWAL_HORIZON_MS).toISOString();
      if (!(BigInt(Date.parse(renewTo)) * 1000n > s.oldValidUntilUs!)) refuse("admission_renewal_not_extension");
      const plan: AdmissionRenewalPlan = {
        capturedAt: s.now, oldValidUntil: s.oldValidUntil!, renewTo, admissionCount: s.admissionCount,
        orgsSha256: s.orgsSha256, descriptorSha256: s.descriptorSha256!, generation: s.generation!, sha: input.sha,
        planRun: input.planRun!,
      };
      parseAdmissionRenewalPlan(plan);
      await client.query("ROLLBACK");
      return { receipt: receiptOf(input, s, s, "ADMISSION_RENEWAL_PLANNED", renewTo, false), plan };
    }
    const plan = input.plan ?? refuse("admission_renewal_plan");
    await client.query("LOCK TABLE sessions.ingest_buffer_admission IN SHARE ROW EXCLUSIVE MODE");
    const before = await snapshot(client, input.expectedGeneration, true);
    if (before.drift !== null) refuse(before.drift);
    const capturedUs = BigInt(Date.parse(plan.capturedAt)) * 1000n, renewToUs = BigInt(Date.parse(plan.renewTo)) * 1000n;
    const ageUs = before.nowUs - capturedUs;
    if (ageUs < 0n || ageUs >= BigInt(ADMISSION_RENEWAL_PLAN_MAX_AGE_MS) * 1000n) refuse("admission_renewal_plan_stale");
    if (before.oldValidUntil !== plan.oldValidUntil || before.oldValidUntilUs !== BigInt(Date.parse(plan.oldValidUntil)) * 1000n)
      refuse("admission_renewal_plan_expiry_mismatch");
    if (before.generation !== plan.generation || before.admissionCount !== plan.admissionCount ||
        before.orgsSha256 !== plan.orgsSha256)
      refuse("admission_renewal_plan_roster_mismatch");
    if (before.descriptorSha256 !== plan.descriptorSha256) refuse("admission_renewal_plan_descriptor_mismatch");
    if (!(renewToUs > before.oldValidUntilUs!) || !(renewToUs > before.nowUs)) refuse("admission_renewal_not_extension");
    const p = before.policy!;
    const set = (await client.query<{ r: { accepted?: unknown; generation?: unknown } }>(
      "SELECT sessions.ingest_lazy_policy_set($1::jsonb) AS r", [JSON.stringify({
        baselineMode: p.baseline_mode, generation: p.generation, sourceIdentitySha256: p.source_identity_sha256,
        controlEpoch: p.control_epoch, privacyBaselineSha256: p.privacy_baseline_sha256,
        retentionContractSha256: p.retention_contract_sha256, sourceProofSha256: p.source_proof_sha256,
        validUntil: plan.renewTo, issuer: p.issuer,
      })])).rows[0]?.r;
    if (set?.accepted !== true || set?.generation !== p.generation) refuse("admission_renewal_policy_set");
    const moved = (await client.query<{ org_id: string }>(
      `UPDATE sessions.ingest_buffer_admission SET valid_until=$1::timestamptz
        WHERE valid_until=$2::timestamptz AND schema_generation=$3 AND control_epoch=$4
          AND privacy_baseline_sha256=$5 AND retention_contract_sha256=$6 RETURNING org_id`,
      [plan.renewTo, plan.oldValidUntil, p.generation, p.control_epoch, p.privacy_baseline_sha256,
        p.retention_contract_sha256])).rows;
    if (moved.length !== plan.admissionCount) refuse("admission_renewal_partial_update");
    const after = await snapshot(client, input.expectedGeneration, false);
    const q = after.policy;
    if (after.drift !== null || q === null || after.oldValidUntil !== plan.renewTo || after.oldValidUntilUs !== renewToUs ||
        after.admissionCount !== plan.admissionCount || after.orgsSha256 !== plan.orgsSha256 ||
        q.baseline_mode !== p.baseline_mode || q.issuer !== p.issuer || q.generation !== p.generation ||
        q.source_identity_sha256 !== p.source_identity_sha256 || q.source_proof_sha256 !== p.source_proof_sha256 ||
        q.control_epoch !== p.control_epoch || q.privacy_baseline_sha256 !== p.privacy_baseline_sha256 ||
        q.retention_contract_sha256 !== p.retention_contract_sha256 || !(BigInt(q.updated_at_us) > BigInt(p.updated_at_us)))
      refuse("admission_renewal_postcondition");
    let guarded: number;
    try {
      guarded = (await client.query(
        `SELECT sessions.ingest_lazy_guard(org_id) FROM sessions.ingest_buffer_admission ORDER BY org_id COLLATE "C"`)).rows.length;
    } catch {
      return refuse("admission_renewal_postcondition_guard");
    }
    if (guarded !== plan.admissionCount) refuse("admission_renewal_postcondition_guard");
    try {
      await client.query("COMMIT");
    } catch {
      return refuse("admission_renewal_commit_unknown");
    }
    return { receipt: receiptOf(input, before, after, "ADMISSION_RENEWED", plan.renewTo, true), plan: null };
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    throw error;
  }
}
