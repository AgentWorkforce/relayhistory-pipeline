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
 *             admissionCount, orgsSha256, descriptorSha256, generation (the live PASS marker's), ledgerTail,
 *             stepCount, ledgerPrefixDigest, sha, planRun.
 *   renew     typed `RENEW-ADMISSION <project> <sha> GENERATION <g> TAIL <ledgerTail> FROM <old> TO <renewTo>
 *             ROWS <n> ORGS <d> DESCRIPTOR <d> PLAN <run>` built from the plan artifact (byte equality). Takes the
 *             locks in the order below and re-checks under them: the ledger proof equals the plan's tail, count and
 *             prefix digest (else admission_renewal_ledger_drift), the marker is PASS at the plan's generation
 *             (else admission_renewal_generation_stale), and the full snapshot and descriptor equal the plan; sets the policy through
 *             the owner-only sessions.ingest_lazy_policy_set with identical immutable fields, CAS-updates every
 *             admission row from the exact old expiry and identity (row count asserted), re-reads every
 *             postcondition and runs sessions.ingest_lazy_guard for every org, then commits. Any error rolls
 *             back. After COMMIT there is no inverse: an emergency closure is separately authorized.
 *
 * Generation. GENERATION is the live readiness marker's generation (status must be pass), discovered by plan and
 * re-read by renew under lock; it is never an operator input and never recomputed from the dispatched tree. A
 * marker's generation also hashes the PHASE2_VERIFY files of the checkout that applied it, and those files may
 * change on main while the schema prefix stays checksum-equal, so a recomputed value may legitimately differ
 * from a valid historical PASS. policy.generation and every admission row's schema_generation must equal it
 * (admission_renewal_generation_drift, admission_renewal_identity_drift).
 *
 * Ledger proof (admissionRenewalLedger), separate from the generation: inside the transaction, after the
 * advisory lock, the operator reads sessions.__96bulls_migrations (name, sha256), resolves renamed rows
 * (resolveRenamedLedger), takes the ledger tail = the last recorded step before the first unrecorded one in
 * migrator order, holds every unrecorded step after it exactly as a release target would (migrationTargetSplit,
 * the rule preflightHeld applies; a recorded step behind an unrecorded one in the same lane is ledger_gap), and
 * runs sequencePreflight over the remaining steps (ledger_unknown_step, also for a ledger longer than this tree;
 * ledger_gap; migration_changed: every recorded checksum must match the dispatched file). It binds ledgerTail,
 * stepCount and ledgerPrefixDigest = sha256 over the JSON of the ordered [name, file sha256] pairs of that prefix
 * as the dispatched tree has them; all three are in the plan and its descriptor. Readback reports a ledger
 * refusal code as its drift instead of refusing. The exact SHA binds the audited operator and workflow and the
 * current source prefix only; it is not the preimage of the historical schema generation.
 *
 * Lock order (deadlock freedom against ingest). The ingest admission path takes no advisory lock and locks in
 * this order: ingest_lazy_begin (041000) runs ingest_require_generation (marker row FOR SHARE, 044030), then the
 * policy row FOR SHARE, then INSERT INTO ingest_buffer_admission (ROW EXCLUSIVE on the table, the new row), then
 * the admission row FOR SHARE; ingest_lazy_guard (044030) takes the marker row FOR SHARE, the policy row FOR
 * SHARE, then the admission row FOR SHARE (ROW SHARE on the table). renew takes the same objects in the same
 * order: pg_advisory_xact_lock(961996,1) first (ingest never takes it; the migrator and the overlap open/close
 * take it first too, so it also excludes ledger and marker writes), then the marker row FOR SHARE, the policy
 * row FOR UPDATE, LOCK TABLE ingest_buffer_admission IN SHARE ROW EXCLUSIVE MODE (conflicts with INSERT/UPDATE/
 * DELETE and with itself; plain reads and the guard's FOR SHARE proceed), then the admission rows FOR UPDATE.
 * Taking the table lock before the policy row would deadlock: an ingest transaction holding the policy row FOR
 * SHARE waits for ROW EXCLUSIVE behind the renewal's table lock while the renewal waits for the policy row. With
 * the policy row first, an ingest transaction either holds the policy row before renew (renew waits, holding no
 * admission lock) or waits for it behind renew (holding no admission lock). The one-shot seat initializer
 * (055000) reads admission rows before the policy and so is not ordered with ingest_lazy_begin either; it is
 * resolved by the deadlock detector and lock_timeout '5s' (renew rolls back whole, nothing is half written).
 *
 * Canonical timestamps are ISO-8601 UTC with exactly three fractional digits and Z, rendered by the database
 * and compared both as those bytes and as database epoch microseconds; an expiry with sub-millisecond digits is
 * refused (it would have two renderings). Never prints org ids or identity hashes; receipts carry counts,
 * canonical timestamps, an 8-character generation prefix, the bound digests and verdicts.
 */
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import {
  expectedMigrationNames,
  MIGRATION_LEDGER,
  migrationTargetSplit,
  RELEASE_TREE,
  sha256Text,
  type MigrationClient,
} from "./migrate.ts";
import { guardError, resolveRenamedLedger, sequencePreflight, type GuardStep } from "./migrate-guards.ts";
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
  /** The live PASS readiness marker's generation (discovered, never recomputed). */
  generation: string;
  /** The ledger proof: tail (a public schema step name), prefix step count and prefix digest. */
  ledgerTail: string;
  stepCount: number;
  ledgerPrefixDigest: string;
  sha: string;
  planRun: string;
}
export const ADMISSION_RENEWAL_PLAN_KEYS = [
  "capturedAt", "oldValidUntil", "renewTo", "admissionCount", "orgsSha256", "descriptorSha256", "generation",
  "ledgerTail", "stepCount", "ledgerPrefixDigest", "sha", "planRun",
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
  /** plan: this run's id (recorded in the artifact); renew: the plan run id; readback: null. */
  planRun: string | null;
  plan: AdmissionRenewalPlan | null;
  approvalSha256: string | null;
}

const HEX64 = /^[0-9a-f]{64}$/u, STEP = /^(upstream|96bulls):[0-9A-Za-z_.-]{1,200}\.sql$/u, SHA40 = /^[0-9a-f]{40}$/u, RUN_ID = /^[1-9][0-9]{0,19}$/u;
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
  return `RENEW-ADMISSION ${project} ${plan.sha} GENERATION ${plan.generation} TAIL ${plan.ledgerTail}` +
    ` FROM ${plan.oldValidUntil} TO ${plan.renewTo}` +
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
      typeof p.generation !== "string" || !HEX64.test(p.generation) || typeof p.ledgerTail !== "string" ||
      !STEP.test(p.ledgerTail) || !Number.isSafeInteger(p.stepCount) || (p.stepCount as number) < 1 ||
      typeof p.ledgerPrefixDigest !== "string" || !HEX64.test(p.ledgerPrefixDigest) || typeof p.sha !== "string" || !SHA40.test(p.sha) ||
      typeof p.planRun !== "string" || !RUN_ID.test(p.planRun))
    refuse("admission_renewal_plan");
  if (to !== captured! + ADMISSION_RENEWAL_HORIZON_MS) refuse("admission_renewal_plan_target");
  if (!(to! > old!)) refuse("admission_renewal_not_extension");
  return p as unknown as AdmissionRenewalPlan;
}

/**
 * Full inputs. plan: `GITHUB_RUN_ID` becomes the recorded plan run. renew: `plan` is the downloaded artifact, bound
 * to the dispatched sha and plan run, under 24h old, and the confirmation must equal the phrase built from it byte
 * for byte. Its generation (marker) and ledger proof are re-read inside the renew transaction.
 */
export function admissionRenewalInputs(
  env: Record<string, string | undefined>,
  options: { plan?: unknown; enabled?: boolean; project?: string; now?: number } = {},
): AdmissionRenewalInput {
  const d = admissionRenewalDispatch(env, options.enabled ?? ADMISSION_RENEWAL_ENABLED);
  const project = options.project ?? SCHEMA_RELEASE.project, now = options.now ?? Date.now();
  if (!/^[a-z0-9]{20}$/u.test(project)) refuse("admission_renewal_project");
  let planRun: string | null = null, plan: AdmissionRenewalPlan | null = null;
  if (d.mode === "plan") {
    if (!RUN_ID.test(env.GITHUB_RUN_ID ?? "")) refuse("admission_renewal_plan_run");
    planRun = env.GITHUB_RUN_ID!;
  } else if (d.mode === "renew") {
    plan = parseAdmissionRenewalPlan(options.plan);
    if (plan.sha !== d.sha) refuse("admission_renewal_plan_sha");
    if (plan.planRun !== d.planRun) refuse("admission_renewal_plan_run");
    const age = now - Date.parse(plan.capturedAt);
    if (!(age >= -5 * 60 * 1000 && age < ADMISSION_RENEWAL_PLAN_MAX_AGE_MS)) refuse("admission_renewal_plan_stale");
    planRun = d.planRun;
  }
  if (d.confirmation !== admissionRenewalConfirmation(d.mode, project, plan)) refuse("admission_renewal_confirmation");
  return {
    mode: d.mode, sha: d.sha, project, planRun, plan,
    approvalSha256: d.mode === "renew" ? sha256(d.confirmation) : null,
  };
}

const ROOT = fileURLToPath(new URL("../..", import.meta.url));

/** The ledger proof (see the header): the tail, the prefix step count and the prefix digest. */
export interface AdmissionRenewalLedger {
  ledgerTail: string;
  stepCount: number;
  ledgerPrefixDigest: string;
}

/** The dispatched tree's schema steps in migrator order (expectedMigrationNames; paths from RELEASE_TREE.lanes). */
async function treeSchemaSteps(root: string): Promise<GuardStep[]> {
  return (await expectedMigrationNames()).map((name) => {
    const lane = RELEASE_TREE.lanes.find((l) => name.startsWith(l.prefix)) ?? refuse("admission_renewal_tree");
    return { name, path: join(root, lane.dir, name.slice(lane.prefix.length)) };
  });
}

/**
 * Reads the ledger in the caller's transaction (after the advisory lock) and proves it is a gap-free,
 * checksum-equal prefix of this tree's schema steps. Refuses with the migrator's own preflight codes; never marks
 * or writes anything. `root` is the tree the step files are read from (tests pass a copy).
 */
export async function admissionRenewalLedger(client: MigrationClient, root: string = ROOT): Promise<AdmissionRenewalLedger> {
  const there = (await client.query<{ r: string | null }>("SELECT to_regclass($1)::text AS r", [MIGRATION_LEDGER])).rows[0]?.r;
  if (!there) refuse("admission_renewal_ledger_empty");
  const physical = new Map((await client.query<{ name: string; sha256: string }>(
    `SELECT name, sha256 FROM ${MIGRATION_LEDGER}`)).rows.map((r) => [r.name, r.sha256] as const));
  const steps = await treeSchemaSteps(root);
  const shas = new Map<string, string>();
  for (const s of steps) shas.set(s.name, sha256Text(await readFile(s.path, "utf8")));
  const renamed = resolveRenamedLedger(physical, (name) => shas.get(name));
  const refusal = renamed.refusals[0];
  if (refusal) throw guardError(refusal.code, refusal.message);
  const ledger = renamed.effective;
  if (!steps.some((s) => ledger.has(s.name))) refuse("admission_renewal_ledger_empty");
  const first = steps.findIndex((s) => !ledger.has(s.name));
  const at = first < 0 ? steps.length - 1 : first - 1;
  if (at < 0)
    throw guardError("ledger_gap", `${steps[0]!.name} is not recorded but later steps are; nothing is renewed`);
  const ledgerTail = steps[at]!.name;
  let held: Set<string>;
  try {
    held = new Set(migrationTargetSplit(
      steps.map((s) => ({ name: s.name, pending: !ledger.has(s.name) || s.name === ledgerTail })), ledgerTail).held);
  } catch (error) {
    if ((error as { code?: unknown }).code === "release_target_passed")
      throw guardError("ledger_gap", `${steps[first]!.name} is not recorded but a later step of its lane is; nothing is renewed`);
    throw error;
  }
  const prefix = steps.filter((s) => !held.has(s.name));
  await sequencePreflight(client, prefix, ledger);
  return {
    ledgerTail, stepCount: prefix.length,
    ledgerPrefixDigest: sha256(JSON.stringify(prefix.map((s) => [s.name, shas.get(s.name)!]))),
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
  singlePolicy: boolean; markerPass: boolean; generationMatchesMarker: boolean; ledgerProven: boolean;
  rowsPresent: boolean; identityMatches: boolean; uniformExpiry: boolean; canonicalExpiry: boolean; unexpired: boolean;
}
export interface AdmissionRenewalSnapshot {
  drift: string | null;
  verdicts: AdmissionRenewalVerdicts;
  policy: PolicyRow | null;
  /** The PASS marker's generation (null unless exactly one marker row with status pass). */
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

/**
 * The ordered descriptor: every immutable policy field, updated_at, generation, old expiry, count and roster, and
 * the ledger proof (tail, step count, prefix digest).
 */
export function admissionRenewalDescriptor(policy: PolicyRow, count: number, orgsSha256: string,
  ledger: AdmissionRenewalLedger): string {
  return sha256(JSON.stringify([
    ["descriptor", "ingest-admission-renewal/v2"],
    ["baselineMode", policy.baseline_mode], ["issuer", policy.issuer], ["generation", policy.generation],
    ["sourceIdentitySha256", policy.source_identity_sha256], ["sourceProofSha256", policy.source_proof_sha256],
    ["controlEpoch", policy.control_epoch], ["privacyBaselineSha256", policy.privacy_baseline_sha256],
    ["retentionContractSha256", policy.retention_contract_sha256],
    ["updatedAt", policy.updated_at], ["updatedAtMicros", policy.updated_at_us],
    ["validUntil", policy.valid_until], ["validUntilMicros", policy.valid_until_us],
    ["admissionCount", count], ["orgsSha256", orgsSha256],
    ["ledgerTail", ledger.ledgerTail], ["stepCount", ledger.stepCount], ["ledgerPrefixDigest", ledger.ledgerPrefixDigest],
  ]));
}

/** sha256 of the org ids joined with "\n", in C (byte) order as the database sorted them. */
export const admissionRenewalOrgsDigest = (orgs: readonly string[]) => sha256(orgs.join("\n"));

/**
 * One consistent read of the authority with the ledger proof (`ledger`, or the ledger refusal code in
 * `ledgerDrift`). `lock` (renew only): the marker row FOR SHARE, the policy row FOR UPDATE, the admission table
 * SHARE ROW EXCLUSIVE, the admission rows FOR UPDATE, in the ingest path's order (see the header).
 */
async function snapshot(client: MigrationClient, ledger: AdmissionRenewalLedger | null, ledgerDrift: string | null,
  lock: boolean): Promise<AdmissionRenewalSnapshot> {
  const clock = (await client.query<{ now: string; now_us: string }>(NOW_SQL)).rows[0]!;
  const nowUs = BigInt(clock.now_us);
  const marker = (await client.query<{ generation: string; status: string }>(
    `SELECT generation,status FROM ${ROLE_VALIDATION_TABLE} WHERE name=$1${lock ? " FOR SHARE" : ""}`,
    [ROLE_VALIDATION_NAME])).rows;
  const policies = (await client.query<PolicyRow>(POLICY_SQL + (lock ? " FOR UPDATE" : ""))).rows;
  if (lock) await client.query("LOCK TABLE sessions.ingest_buffer_admission IN SHARE ROW EXCLUSIVE MODE");
  const rows = (await client.query<AdmissionRow>(ROWS_SQL + (lock ? " FOR UPDATE" : ""))).rows;
  const policy = policies.length === 1 ? policies[0]! : null;
  const orgs = rows.map((r) => r.org_id), orgsSha256 = admissionRenewalOrgsDigest(orgs);
  const v: AdmissionRenewalVerdicts = {
    singlePolicy: policies.length === 1,
    markerPass: marker.length === 1 && marker[0]!.status === "pass",
    generationMatchesMarker: policy !== null && marker.length === 1 && marker[0]!.generation === policy.generation,
    ledgerProven: ledger !== null,
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
    : !v.ledgerProven ? (ledgerDrift ?? "admission_renewal_ledger")
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
    generation: v.markerPass ? marker[0]!.generation : null,
    oldValidUntil: policy?.valid_until ?? null,
    oldValidUntilUs: policy ? BigInt(policy.valid_until_us) : null,
    admissionCount: rows.length,
    orgsSha256,
    descriptorSha256: policy && ledger ? admissionRenewalDescriptor(policy, rows.length, orgsSha256, ledger) : null,
    minValidUntil: byUs(sorted[0]), maxValidUntil: byUs(sorted[sorted.length - 1]),
    nowUs, now: clock.now,
  };
}

export interface AdmissionRenewalReceipt {
  status: "ADMISSION_RENEWAL_READBACK" | "ADMISSION_RENEWAL_PLANNED" | "ADMISSION_RENEWED";
  mode: AdmissionRenewalMode;
  sha: string;
  generationPrefix: string | null;
  ledgerTail: string | null;
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

function receiptOf(input: AdmissionRenewalInput, ledger: AdmissionRenewalLedger | null, before: AdmissionRenewalSnapshot,
  after: AdmissionRenewalSnapshot, status: AdmissionRenewalReceipt["status"], renewTo: string | null,
  committed: boolean): AdmissionRenewalReceipt {
  return {
    status, mode: input.mode, sha: input.sha,
    generationPrefix: after.generation?.slice(0, 8) ?? null,
    ledgerTail: ledger?.ledgerTail ?? null,
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
      let ledger: AdmissionRenewalLedger | null = null, ledgerDrift: string | null = null;
      try {
        ledger = await admissionRenewalLedger(client);
      } catch (error) {
        const c = (error as { code?: unknown }).code;
        ledgerDrift = typeof c === "string" && /^[a-z0-9_]{1,64}$/u.test(c) ? c : "admission_renewal_ledger";
      }
      const s = await snapshot(client, ledger, ledgerDrift, false);
      await client.query("ROLLBACK");
      return { receipt: receiptOf(input, ledger, s, s, "ADMISSION_RENEWAL_READBACK", null, false), plan: null };
    }
    const ledger = await admissionRenewalLedger(client);
    if (input.mode === "plan") {
      const s = await snapshot(client, ledger, null, false);
      if (s.drift !== null) refuse(s.drift);
      const capturedMs = canonicalMs(s.now);
      if (capturedMs === null || BigInt(capturedMs) * 1000n !== s.nowUs) refuse("admission_renewal_timestamp_precision");
      const renewTo = new Date(capturedMs! + ADMISSION_RENEWAL_HORIZON_MS).toISOString();
      if (!(BigInt(Date.parse(renewTo)) * 1000n > s.oldValidUntilUs!)) refuse("admission_renewal_not_extension");
      const plan: AdmissionRenewalPlan = {
        capturedAt: s.now, oldValidUntil: s.oldValidUntil!, renewTo, admissionCount: s.admissionCount,
        orgsSha256: s.orgsSha256, descriptorSha256: s.descriptorSha256!, generation: s.generation!,
        ledgerTail: ledger.ledgerTail, stepCount: ledger.stepCount, ledgerPrefixDigest: ledger.ledgerPrefixDigest,
        sha: input.sha, planRun: input.planRun!,
      };
      parseAdmissionRenewalPlan(plan);
      await client.query("ROLLBACK");
      return { receipt: receiptOf(input, ledger, s, s, "ADMISSION_RENEWAL_PLANNED", renewTo, false), plan };
    }
    const plan = input.plan ?? refuse("admission_renewal_plan");
    if (plan.ledgerTail !== ledger.ledgerTail || plan.stepCount !== ledger.stepCount ||
        plan.ledgerPrefixDigest !== ledger.ledgerPrefixDigest)
      refuse("admission_renewal_ledger_drift");
    const before = await snapshot(client, ledger, null, true);
    if (before.drift !== null) refuse(before.drift);
    if (before.generation !== plan.generation) refuse("admission_renewal_generation_stale");
    const capturedUs = BigInt(Date.parse(plan.capturedAt)) * 1000n, renewToUs = BigInt(Date.parse(plan.renewTo)) * 1000n;
    const ageUs = before.nowUs - capturedUs;
    if (ageUs < 0n || ageUs >= BigInt(ADMISSION_RENEWAL_PLAN_MAX_AGE_MS) * 1000n) refuse("admission_renewal_plan_stale");
    if (before.oldValidUntil !== plan.oldValidUntil || before.oldValidUntilUs !== BigInt(Date.parse(plan.oldValidUntil)) * 1000n)
      refuse("admission_renewal_plan_expiry_mismatch");
    if (before.admissionCount !== plan.admissionCount || before.orgsSha256 !== plan.orgsSha256)
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
    const after = await snapshot(client, ledger, null, false);
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
    return { receipt: receiptOf(input, ledger, before, after, "ADMISSION_RENEWED", plan.renewTo, true), plan: null };
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    throw error;
  }
}
