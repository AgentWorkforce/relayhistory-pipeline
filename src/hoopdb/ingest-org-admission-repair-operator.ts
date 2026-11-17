/**
 * CI operator that repairs buffered-ingest org admission for an explicit org list: one
 * sessions.ingest_buffer_admission row per org, at the live custody policy's identity and expiry, in ONE owner
 * transaction. No migration: plain statements over the migrator's owner connection (the guarded schema release's
 * verify-full TLS client, proven owner by requireReleaseOwner). Workflow: .github/workflows/ingest-org-admission-repair.yml;
 * docs/design/zero-503-schema-windows.md "Org admission repair"; runbooks/history-620-operations.md.
 *
 * Why. sessions.ingest_lazy_begin (041000) admits an org on its first claim, but only when no other
 * sessions.ingest_%/upload_% table with an org_id column has rows for it; otherwise it raises 55000 "unbound prior
 * org authority requires recovery" and the insert rolls back on every claim. An org whose ONLY prior rows are in
 * sessions.ingest_seed_source_epochs (040000's per-session change counter, written by the ingest_seed_source_changed
 * trigger on sessions.session_index; no authority) can never self-admit. This operator admits exactly such orgs,
 * the way the cutover ceremony's --admit does, and refuses everything else. The class fix (a forward migration
 * that drops ingest_seed_source_epochs from the scan) is separate.
 *
 *   readback  READ ONLY; always allowed (also while ORG_ADMISSION_REPAIR_ENABLED is false). Per org (by index in
 *             the sorted list, never the id): admission present, and the row count of every table the live
 *             lazy_begin scan sees; the policy expiry, the PASS marker's generation prefix, the policy descriptor,
 *             the ledger proof and verdicts. Reports the first drift code instead of refusing.
 *   plan      READ ONLY; refuses on drift. Refuses an org that already has an admission row
 *             (org_admission_repair_already_admitted), an org with rows in any scanned table outside
 *             ORG_ADMISSION_REPAIR_ALLOWLIST (org_admission_repair_unbound_authority, with the table name), and an
 *             org with no scanned rows at all (org_admission_repair_nothing_to_repair: a typo'd id would otherwise be
 *             admitted and renewed forever). Refuses unless the marker is PASS, policy.generation equals it and the
 *             policy is unexpired. Emits the plan artifact: capturedAt, orgsSha256 (sorted list), orgCount, per-org
 *             scan digests, scanTablesSha256, generation, validUntil (policy), policyDescriptorSha256 (every policy
 *             field incl. control epoch, privacy baseline, retention contract and updated_at), ledger proof, sha,
 *             planRun.
 *   admit     typed `ADMIT-ORGS <project> <sha> GENERATION <g> VALID_UNTIL <v> ORGS <digest> COUNT <n> PLAN <run>`
 *             built from the plan artifact (byte equality) and the same org list input (its digest and count must be
 *             the plan's). Takes the locks below, re-proves the ledger and every plan precondition under them (each
 *             change refuses with its own code), INSERTs one row per org copying the locked live policy row's
 *             privacy_baseline_sha256, retention_contract_sha256, generation, valid_until and control_epoch (plain
 *             INSERT: a conflict is drift and rolls back), re-reads every postcondition, runs
 *             sessions.ingest_lazy_guard for every org, then commits. Any error rolls back the whole transaction.
 *             After COMMIT there is no inverse in this workflow: a closure is a separately authorized operation.
 *
 * Scan identity. The tables checked are selected with the exact pg_catalog query 041000's lazy_begin loops over
 * (LAZY_BEGIN_SCAN_SELECT, verbatim). Every mode first proves the live function body still contains that query,
 * its EXISTS probe and its refusal (verdict scanMatchesLive); plan and admit refuse otherwise
 * (org_admission_repair_scan_drift), so a later migration that changes the scan (the class fix) retires this
 * operator instead of silently diverging from it.
 *
 * Lock order (the renewal operator's, see ingest-admission-renewal-operator.ts): pg_advisory_xact_lock(961996,1)
 * first (ingest never takes it; the migrator, overlap and renewal take it first, so it also excludes ledger,
 * marker and policy rewrites by them), then the marker row FOR SHARE, the policy row FOR SHARE (this operator
 * never writes the policy; SHARE is compatible with ingest's FOR SHARE and blocks any policy UPDATE), then
 * LOCK TABLE sessions.ingest_buffer_admission IN SHARE ROW EXCLUSIVE MODE (no concurrent admission insert or
 * update; reads and the guard's FOR SHARE proceed), in ingest_lazy_begin's order (marker, policy, admission
 * table), so the two cannot wait on each other in opposite orders. lock_timeout 5s, statement_timeout 30s.
 *
 * Residual race (the same one lazy_begin itself has): rows for an org can appear in a scanned table between the
 * check and COMMIT only through a path that does not need that org's admission; the check runs under the table
 * lock in the same transaction as the insert, exactly as lazy_begin's does.
 *
 * Canonical timestamps and ledger proof are the renewal operator's (canonicalMs, admissionRenewalLedger). Never
 * prints org ids or identity hashes; receipts carry indexes, table names (never per-org row counts), canonical timestamps, an
 * 8-character generation prefix, the bound digests and verdicts.
 */
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import type { MigrationClient } from "./migrate.ts";
import { ROLE_VALIDATION_NAME, ROLE_VALIDATION_TABLE } from "../shared/db.ts";
import { SCHEMA_RELEASE } from "./schema-release-contract.ts";
import {
  admissionRenewalLedger,
  canonicalMs,
  type AdmissionRenewalLedger,
} from "./ingest-admission-renewal-operator.ts";
import {
  ORG_ADMISSION_REPAIR_ALLOWLIST,
  ORG_ADMISSION_REPAIR_ENABLED,
  ORG_ADMISSION_REPAIR_MAX_ORGS,
  ORG_ADMISSION_REPAIR_PLAN_MAX_AGE_MS,
} from "./org-admission-repair-contract.ts";

export type OrgAdmissionRepairMode = "readback" | "plan" | "admit";
export interface OrgAdmissionRepairPlan {
  capturedAt: string;
  orgsSha256: string;
  orgCount: number;
  /** Per org, in the sorted list's order: sha256 of the scanned table list, the tables with rows and admitted=false. */
  orgScanSha256: string[];
  scanTablesSha256: string;
  /** The live PASS readiness marker's generation (discovered, never recomputed); policy.generation equals it. */
  generation: string;
  /** The policy's valid_until (canonical); every admitted row gets exactly this expiry. */
  validUntil: string;
  policyDescriptorSha256: string;
  ledgerTail: string;
  stepCount: number;
  ledgerPrefixDigest: string;
  sha: string;
  planRun: string;
}
export const ORG_ADMISSION_REPAIR_PLAN_KEYS = [
  "capturedAt", "orgsSha256", "orgCount", "orgScanSha256", "scanTablesSha256", "generation", "validUntil",
  "policyDescriptorSha256", "ledgerTail", "stepCount", "ledgerPrefixDigest", "sha", "planRun",
] as const;

export interface OrgAdmissionRepairDispatch {
  mode: OrgAdmissionRepairMode;
  sha: string;
  /** admit: the plan run id input; plan/readback: null. */
  planRun: string | null;
  confirmation: string;
  /** Validated, deduplicated, C (byte) ordered. */
  orgs: string[];
}
export interface OrgAdmissionRepairInput extends OrgAdmissionRepairDispatch {
  project: string;
  orgsSha256: string;
  plan: OrgAdmissionRepairPlan | null;
  approvalSha256: string | null;
}

const HEX64 = /^[0-9a-f]{64}$/u, STEP = /^(upstream|96bulls):[0-9A-Za-z_.-]{1,200}\.sql$/u, SHA40 = /^[0-9a-f]{40}$/u,
  RUN_ID = /^[1-9][0-9]{0,19}$/u;
/** One org id: ASCII only (so JS and C ordering agree), no separators, whitespace or control characters. */
export const ORG_ID = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u;
const TABLE = /^[a-z0-9_]{1,63}$/u;

export const refuseOrgAdmissionRepair = (code: string, detail?: string): never => {
  throw Object.assign(new Error(detail === undefined ? code : `${code} ${detail}`), { code, detail: detail ?? null });
};
const refuse = refuseOrgAdmissionRepair;
const sha256 = (text: string) => createHash("sha256").update(text).digest("hex");
const byteOrder = (a: string, b: string) => (a < b ? -1 : a > b ? 1 : 0);

/**
 * The exact table selection 041000's ingest_lazy_begin loops over (byte for byte, comments included; it must
 * appear verbatim in that migration and in the live function body). Executed here with an ORDER BY appended.
 */
export const LAZY_BEGIN_SCAN_SELECT = [
  "SELECT t.relname FROM pg_class t JOIN pg_namespace n ON n.oid=t.relnamespace",
  "   WHERE n.nspname='sessions' AND t.relkind='r' AND(t.relname LIKE 'ingest_%' OR t.relname LIKE 'upload_%')",
  "    AND t.relname<>'ingest_buffer_admission'",
  "    -- Source-side state is not prior TARGET authority: EXACTLY the six 010000 frozen-copy tables, 041000's owned-set",
  "    -- copy and the Supabase-first control heads. Any other ingest_/upload_ table (even one sharing the prefix) is checked.",
  "    AND t.relname NOT IN('ingest_frozen_source_records','ingest_frozen_source_deletions','ingest_frozen_source_resets',",
  "     'ingest_frozen_source_org_seals','ingest_frozen_source_completeness','ingest_frozen_source_receipts',",
  "     'ingest_frozen_source_owned_sessions') AND t.relname<>'ingest_source_control_heads'",
  "    AND EXISTS(SELECT 1 FROM pg_attribute a WHERE a.attrelid=t.oid AND a.attname='org_id' AND a.attnum>0 AND NOT a.attisdropped)",
].join("\n");
/** The loop as it stands in the live body: the selection, its per-table EXISTS probe and the refusal it raises. */
export const LAZY_BEGIN_SCAN_LOOP = "  FOR k IN " + LAZY_BEGIN_SCAN_SELECT + "\n  LOOP\n" +
  "   EXECUTE format('SELECT EXISTS(SELECT 1 FROM sessions.%I WHERE org_id=$1)',k.relname) INTO v_exists USING v_org;\n" +
  "   IF v_exists THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: unbound prior org authority requires recovery';END IF;";
const SCAN_SQL = LAZY_BEGIN_SCAN_SELECT + "\n ORDER BY t.relname COLLATE \"C\"";
const LIVE_BODY_SQL = "SELECT prosrc FROM pg_proc WHERE oid=to_regprocedure('sessions.ingest_lazy_begin(jsonb,text)')";

/** Comma-separated org ids: trimmed, validated, deduplicated, C ordered, 1..ORG_ADMISSION_REPAIR_MAX_ORGS. */
export function parseOrgAdmissionRepairOrgs(raw: unknown): string[] {
  if (typeof raw !== "string" || raw.length > 64 * 1024) return refuse("org_admission_repair_orgs");
  const parts = raw.split(",").map((s) => s.trim());
  if (parts.some((p) => !ORG_ID.test(p))) refuse("org_admission_repair_orgs");
  const orgs = [...new Set(parts)].sort(byteOrder);
  if (orgs.length < 1 || orgs.length > ORG_ADMISSION_REPAIR_MAX_ORGS) refuse("org_admission_repair_orgs");
  return orgs;
}
/**
 * The workflow never puts the org list in `env:` (GitHub prints step env in public logs). The scripts read it from
 * the dispatch event payload (`inputs.orgs` in GITHUB_EVENT_PATH) unless ORG_REPAIR_ORGS is already set (tests).
 */
export function orgAdmissionRepairEnv(env: Record<string, string | undefined>): Record<string, string | undefined> {
  if (env.ORG_REPAIR_ORGS !== undefined || !env.GITHUB_EVENT_PATH) return env;
  let orgs: unknown;
  try {
    orgs = JSON.parse(readFileSync(env.GITHUB_EVENT_PATH, "utf8"))?.inputs?.orgs;
  } catch {
    return refuse("org_admission_repair_orgs");
  }
  return { ...env, ORG_REPAIR_ORGS: typeof orgs === "string" ? orgs : undefined };
}
/** GitHub workflow commands that mask every org id in the rest of the job's log (defence in depth). */
export const orgAdmissionRepairMasks = (orgs: readonly string[]) => orgs.map((o) => `::add-mask::${o}`).join("\n");

/** sha256 of the org ids joined with "\n", in C (byte) order (the renewal roster digest's form). */
export const orgAdmissionRepairOrgsDigest = (orgs: readonly string[]) => sha256(orgs.join("\n"));

/** The exact typed confirmation for admit; plan and readback take "". */
export function orgAdmissionRepairConfirmation(mode: OrgAdmissionRepairMode, project: string,
  plan: OrgAdmissionRepairPlan | null): string {
  if (mode !== "admit" || plan === null) return "";
  return `ADMIT-ORGS ${project} ${plan.sha} GENERATION ${plan.generation} VALID_UNTIL ${plan.validUntil}` +
    ` ORGS ${plan.orgsSha256} COUNT ${plan.orgCount} PLAN ${plan.planRun}`;
}

/** Mode, sha, plan run, org list and dormant switch; refuses before any credential is read. */
export function orgAdmissionRepairDispatch(
  env: Record<string, string | undefined>,
  enabled: boolean = ORG_ADMISSION_REPAIR_ENABLED,
): OrgAdmissionRepairDispatch {
  const mode = env.ORG_REPAIR_MODE as OrgAdmissionRepairMode, sha = env.ORG_REPAIR_SHA ?? "",
    planRun = env.ORG_REPAIR_PLAN_RUN ?? "", confirmation = env.ORG_REPAIR_CONFIRMATION ?? "";
  if ((mode !== "plan" && mode !== "admit" && mode !== "readback") || !SHA40.test(sha))
    refuse("org_admission_repair_inputs");
  if (mode !== "readback" && enabled !== true) refuse("org_admission_repair_dormant");
  if (mode === "admit" ? !RUN_ID.test(planRun) : planRun !== "") refuse("org_admission_repair_plan_run");
  if (mode !== "admit" && confirmation !== "") refuse("org_admission_repair_confirmation");
  if (mode === "admit" && !confirmation.startsWith("ADMIT-ORGS ")) refuse("org_admission_repair_confirmation");
  const orgs = parseOrgAdmissionRepairOrgs(env.ORG_REPAIR_ORGS);
  return { mode, sha, planRun: mode === "admit" ? planRun : null, confirmation, orgs };
}

/** Strict plan artifact: exact keys and types, canonical timestamps, validUntil after capturedAt. */
export function parseOrgAdmissionRepairPlan(value: unknown): OrgAdmissionRepairPlan {
  if (value === null || typeof value !== "object" || Array.isArray(value)) return refuse("org_admission_repair_plan");
  const p = value as Record<string, unknown>, keys = Object.keys(p).sort(byteOrder);
  const want = [...ORG_ADMISSION_REPAIR_PLAN_KEYS].sort(byteOrder);
  if (keys.length !== want.length || keys.some((k, i) => k !== want[i])) refuse("org_admission_repair_plan");
  const captured = canonicalMs(p.capturedAt), valid = canonicalMs(p.validUntil);
  const n = p.orgCount as number;
  if (captured === null || valid === null || !Number.isSafeInteger(n) || n < 1 || n > ORG_ADMISSION_REPAIR_MAX_ORGS ||
      typeof p.orgsSha256 !== "string" || !HEX64.test(p.orgsSha256) ||
      !Array.isArray(p.orgScanSha256) || p.orgScanSha256.length !== n ||
      p.orgScanSha256.some((d) => typeof d !== "string" || !HEX64.test(d)) ||
      typeof p.scanTablesSha256 !== "string" || !HEX64.test(p.scanTablesSha256) ||
      typeof p.generation !== "string" || !HEX64.test(p.generation) ||
      typeof p.policyDescriptorSha256 !== "string" || !HEX64.test(p.policyDescriptorSha256) ||
      typeof p.ledgerTail !== "string" || !STEP.test(p.ledgerTail) || !Number.isSafeInteger(p.stepCount) ||
      (p.stepCount as number) < 1 || typeof p.ledgerPrefixDigest !== "string" || !HEX64.test(p.ledgerPrefixDigest) ||
      typeof p.sha !== "string" || !SHA40.test(p.sha) || typeof p.planRun !== "string" || !RUN_ID.test(p.planRun))
    refuse("org_admission_repair_plan");
  if (!(valid! > captured!)) refuse("org_admission_repair_plan");
  return p as unknown as OrgAdmissionRepairPlan;
}

/**
 * Full inputs. plan: `GITHUB_RUN_ID` becomes the recorded plan run. admit: `plan` is the downloaded artifact, bound
 * to the dispatched sha, plan run and org list, under 24h old, and the confirmation must equal the phrase built
 * from it byte for byte. Everything else in it is re-read inside the admit transaction.
 */
export function orgAdmissionRepairInputs(
  env: Record<string, string | undefined>,
  options: { plan?: unknown; enabled?: boolean; project?: string; now?: number } = {},
): OrgAdmissionRepairInput {
  const d = orgAdmissionRepairDispatch(env, options.enabled ?? ORG_ADMISSION_REPAIR_ENABLED);
  const project = options.project ?? SCHEMA_RELEASE.project, now = options.now ?? Date.now();
  if (!/^[a-z0-9]{20}$/u.test(project)) refuse("org_admission_repair_project");
  const orgsSha256 = orgAdmissionRepairOrgsDigest(d.orgs);
  let planRun: string | null = null, plan: OrgAdmissionRepairPlan | null = null;
  if (d.mode === "plan") {
    if (!RUN_ID.test(env.GITHUB_RUN_ID ?? "")) refuse("org_admission_repair_plan_run");
    planRun = env.GITHUB_RUN_ID!;
  } else if (d.mode === "admit") {
    plan = parseOrgAdmissionRepairPlan(options.plan);
    if (plan.sha !== d.sha) refuse("org_admission_repair_plan_sha");
    if (plan.planRun !== d.planRun) refuse("org_admission_repair_plan_run");
    if (plan.orgsSha256 !== orgsSha256 || plan.orgCount !== d.orgs.length) refuse("org_admission_repair_plan_orgs");
    const age = now - Date.parse(plan.capturedAt);
    if (!(age >= -5 * 60 * 1000 && age < ORG_ADMISSION_REPAIR_PLAN_MAX_AGE_MS)) refuse("org_admission_repair_plan_stale");
    planRun = d.planRun;
  }
  if (d.confirmation !== orgAdmissionRepairConfirmation(d.mode, project, plan)) refuse("org_admission_repair_confirmation");
  return {
    ...d, project, planRun, plan, orgsSha256,
    approvalSha256: d.mode === "admit" ? sha256(d.confirmation) : null,
  };
}

const ts = (column: string) => `to_char(${column} AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')`;
const us = (column: string) => `(extract(epoch FROM ${column})*1000000)::bigint::text`;
const POLICY_SQL = `SELECT baseline_mode,generation,source_identity_sha256,control_epoch,privacy_baseline_sha256,
 retention_contract_sha256,source_proof_sha256,issuer,${ts("valid_until")} AS valid_until,${us("valid_until")} AS valid_until_us,
 ${ts("updated_at")} AS updated_at,${us("updated_at")} AS updated_at_us FROM sessions.ingest_lazy_policy`;
const NOW_SQL = `SELECT ${ts("n")} AS now,${us("n")} AS now_us FROM (SELECT date_trunc('milliseconds',clock_timestamp()) AS n) t`;
/** The orgs' admission rows (JSON array parameter, so no driver-specific array encoding). */
const ROWS_SQL = `SELECT org_id,schema_generation,control_epoch,privacy_baseline_sha256,retention_contract_sha256,
 ${ts("valid_until")} AS valid_until,${us("valid_until")} AS valid_until_us FROM sessions.ingest_buffer_admission
 WHERE org_id IN (SELECT jsonb_array_elements_text($1::jsonb)) ORDER BY org_id COLLATE "C"`;
const quoteIdent = (name: string) => '"' + name.replace(/"/gu, '""') + '"';

interface PolicyRow {
  baseline_mode: string; generation: string; source_identity_sha256: string; control_epoch: string;
  privacy_baseline_sha256: string; retention_contract_sha256: string; source_proof_sha256: string; issuer: string;
  valid_until: string; valid_until_us: string; updated_at: string; updated_at_us: string;
}
interface AdmissionRow {
  org_id: string; schema_generation: string; control_epoch: string; privacy_baseline_sha256: string;
  retention_contract_sha256: string; valid_until: string; valid_until_us: string;
}

/** Every policy field, updated_at and the expiry (both renderings): any policy rewrite changes it. */
export function orgAdmissionRepairPolicyDescriptor(policy: PolicyRow): string {
  return sha256(JSON.stringify([
    ["descriptor", "ingest-org-admission-repair-policy/v1"],
    ["baselineMode", policy.baseline_mode], ["issuer", policy.issuer], ["generation", policy.generation],
    ["sourceIdentitySha256", policy.source_identity_sha256], ["sourceProofSha256", policy.source_proof_sha256],
    ["controlEpoch", policy.control_epoch], ["privacyBaselineSha256", policy.privacy_baseline_sha256],
    ["retentionContractSha256", policy.retention_contract_sha256],
    ["updatedAt", policy.updated_at], ["updatedAtMicros", policy.updated_at_us],
    ["validUntil", policy.valid_until], ["validUntilMicros", policy.valid_until_us],
  ]));
}
export const orgAdmissionRepairTablesDigest = (tables: readonly string[]) =>
  sha256(JSON.stringify(["ingest-org-admission-repair-scan-tables/v1", tables]));
/** One org's scan, as plan binds it (no org id: the roster digest binds the ids and their order). */
export const orgAdmissionRepairScanDigest = (tables: readonly string[], withRows: readonly string[], admitted: boolean) =>
  sha256(JSON.stringify([["scan", "ingest-org-admission-repair/v1"], ["tables", tables], ["tablesWithRows", withRows],
    ["admitted", admitted]]));

export interface OrgAdmissionRepairOrgScan {
  /** Position in the sorted, deduplicated input list (the id itself is never reported). */
  index: number;
  admitted: boolean;
  tablesWithRows: string[];
  scanSha256: string;
}
export interface OrgAdmissionRepairVerdicts {
  singlePolicy: boolean; markerPass: boolean; generationMatchesMarker: boolean; ledgerProven: boolean;
  scanMatchesLive: boolean; canonicalExpiry: boolean; unexpired: boolean; noneAdmitted: boolean;
  onlyAllowlistedRows: boolean; everyOrgHasRows: boolean;
}
export interface OrgAdmissionRepairSnapshot {
  drift: string | null;
  driftDetail: string | null;
  verdicts: OrgAdmissionRepairVerdicts;
  policy: PolicyRow | null;
  generation: string | null;
  policyDescriptorSha256: string | null;
  tables: string[];
  scanTablesSha256: string;
  orgs: OrgAdmissionRepairOrgScan[];
  rows: AdmissionRow[];
  admissionTotal: number;
  nowUs: bigint;
  now: string;
}

/**
 * One consistent read: clock, marker, policy, (lock: the admission table), live scan body, the scanned tables, the
 * orgs' admission rows and, per table, which orgs have rows (presence only, never counts; skipped with scan: false, whose
 * drift and per-org scans are then not meaningful). `lock` (admit only): the marker row
 * FOR SHARE, the policy row FOR SHARE, then the admission table SHARE ROW EXCLUSIVE, in ingest_lazy_begin's order.
 */
async function snapshot(client: MigrationClient, orgs: readonly string[], ledger: AdmissionRenewalLedger | null,
  ledgerDrift: string | null, opts: { lock: boolean; scan?: boolean }): Promise<OrgAdmissionRepairSnapshot> {
  const clock = (await client.query<{ now: string; now_us: string }>(NOW_SQL)).rows[0]!;
  const nowUs = BigInt(clock.now_us);
  const marker = (await client.query<{ generation: string; status: string }>(
    `SELECT generation,status FROM ${ROLE_VALIDATION_TABLE} WHERE name=$1${opts.lock ? " FOR SHARE" : ""}`,
    [ROLE_VALIDATION_NAME])).rows;
  const policies = (await client.query<PolicyRow>(POLICY_SQL + (opts.lock ? " FOR SHARE" : ""))).rows;
  if (opts.lock) await client.query("LOCK TABLE sessions.ingest_buffer_admission IN SHARE ROW EXCLUSIVE MODE");
  const body = (await client.query<{ prosrc: string }>(LIVE_BODY_SQL)).rows[0]?.prosrc ?? "";
  const scanMatchesLive = body.split("FOR k IN ").length === 2 && body.includes(LAZY_BEGIN_SCAN_LOOP);
  const tables = (await client.query<{ relname: string }>(SCAN_SQL)).rows.map((r) => r.relname);
  if (tables.some((t) => !TABLE.test(t))) refuse("org_admission_repair_scan_table");
  const orgsJson = JSON.stringify(orgs);
  const rows = (await client.query<AdmissionRow>(ROWS_SQL, [orgsJson])).rows;
  const admissionTotal = Number((await client.query<{ n: string }>(
    "SELECT count(*)::text AS n FROM sessions.ingest_buffer_admission")).rows[0]!.n);
  const withRows = orgs.map(() => [] as string[]);
  const index = new Map(orgs.map((o, i) => [o, i] as const));
  for (const t of opts.scan === false ? [] : tables) {
    // Presence only: per-org row counts would publish session volumes in a public run's receipt.
    const sql = `SELECT o AS org_id,'1' AS n FROM jsonb_array_elements_text($1::jsonb) o
          WHERE EXISTS(SELECT 1 FROM sessions.${quoteIdent(t)} WHERE org_id=o)`;
    for (const r of (await client.query<{ org_id: string; n: string }>(sql, [orgsJson])).rows) {
      const i = index.get(r.org_id), n = Number(r.n);
      if (i === undefined || !(n > 0)) continue;
      withRows[i]!.push(t);
    }
  }
  const admittedSet = new Set(rows.map((r) => r.org_id));
  const scans: OrgAdmissionRepairOrgScan[] = orgs.map((o, i) => ({
    index: i, admitted: admittedSet.has(o), tablesWithRows: withRows[i]!,
    scanSha256: orgAdmissionRepairScanDigest(tables, withRows[i]!, admittedSet.has(o)),
  }));
  const policy = policies.length === 1 ? policies[0]! : null;
  const allow = new Set(ORG_ADMISSION_REPAIR_ALLOWLIST);
  const admitted = scans.find((s) => s.admitted);
  const foreign = scans.map((s) => ({ s, t: s.tablesWithRows.find((t) => !allow.has(t)) })).find((x) => x.t !== undefined);
  const empty = scans.find((s) => s.tablesWithRows.length === 0);
  const v: OrgAdmissionRepairVerdicts = {
    singlePolicy: policies.length === 1,
    markerPass: marker.length === 1 && marker[0]!.status === "pass",
    generationMatchesMarker: policy !== null && marker.length === 1 && marker[0]!.generation === policy.generation,
    ledgerProven: ledger !== null,
    scanMatchesLive,
    canonicalExpiry: policy !== null && canonicalMs(policy.valid_until) !== null &&
      BigInt(canonicalMs(policy.valid_until)!) * 1000n === BigInt(policy.valid_until_us),
    unexpired: policy !== null && BigInt(policy.valid_until_us) > nowUs,
    noneAdmitted: admitted === undefined,
    onlyAllowlistedRows: foreign === undefined,
    everyOrgHasRows: empty === undefined,
  };
  const [drift, driftDetail]: [string | null, string | null] =
    !v.singlePolicy ? [policies.length === 0 ? "org_admission_repair_policy_missing" : "org_admission_repair_policy_extra", null]
    : !v.markerPass ? ["org_admission_repair_marker_not_pass", null]
    : !v.generationMatchesMarker ? ["org_admission_repair_generation_drift", null]
    : !v.ledgerProven ? [ledgerDrift ?? "org_admission_repair_ledger", null]
    : !v.scanMatchesLive ? ["org_admission_repair_scan_drift", null]
    : !v.canonicalExpiry ? ["org_admission_repair_timestamp_precision", null]
    : !v.unexpired ? ["org_admission_repair_policy_expired", null]
    : admitted ? ["org_admission_repair_already_admitted", `org#${admitted.index}`]
    : foreign ? ["org_admission_repair_unbound_authority", `${foreign.t} org#${foreign.s.index}`]
    : empty ? ["org_admission_repair_nothing_to_repair", `org#${empty.index}`]
    : [null, null];
  return {
    drift, driftDetail, verdicts: v, policy,
    generation: v.markerPass ? marker[0]!.generation : null,
    policyDescriptorSha256: policy ? orgAdmissionRepairPolicyDescriptor(policy) : null,
    tables, scanTablesSha256: orgAdmissionRepairTablesDigest(tables), orgs: scans, rows, admissionTotal,
    nowUs, now: clock.now,
  };
}

export interface OrgAdmissionRepairReceipt {
  status: "ORG_ADMISSION_REPAIR_READBACK" | "ORG_ADMISSION_REPAIR_PLANNED" | "ORG_ADMISSION_REPAIRED";
  mode: OrgAdmissionRepairMode;
  sha: string;
  generationPrefix: string | null;
  ledgerTail: string | null;
  policyValidUntil: string | null;
  policyDescriptorSha256: string | null;
  orgCount: number;
  orgsSha256: string;
  scanTablesSha256: string;
  scanTableCount: number;
  orgs: OrgAdmissionRepairOrgScan[];
  admissionRowsBefore: number;
  admissionRowsAfter: number;
  planRun: string | null;
  drift: string | null;
  driftDetail: string | null;
  verdicts: OrgAdmissionRepairVerdicts;
  committed: boolean;
  approvalSha256: string | null;
}
export interface OrgAdmissionRepairResult {
  receipt: OrgAdmissionRepairReceipt;
  /** plan mode only: the artifact admit consumes. */
  plan: OrgAdmissionRepairPlan | null;
}

function receiptOf(input: OrgAdmissionRepairInput, ledger: AdmissionRenewalLedger | null, before: OrgAdmissionRepairSnapshot,
  after: OrgAdmissionRepairSnapshot, status: OrgAdmissionRepairReceipt["status"], committed: boolean): OrgAdmissionRepairReceipt {
  return {
    status, mode: input.mode, sha: input.sha,
    generationPrefix: after.generation?.slice(0, 8) ?? null,
    ledgerTail: ledger?.ledgerTail ?? null,
    policyValidUntil: after.policy?.valid_until ?? null,
    policyDescriptorSha256: after.policyDescriptorSha256,
    orgCount: input.orgs.length, orgsSha256: input.orgsSha256,
    scanTablesSha256: after.scanTablesSha256, scanTableCount: after.tables.length,
    orgs: before.orgs.map((o) => ({ ...o, admitted: after.orgs[o.index]!.admitted })),
    admissionRowsBefore: before.admissionTotal, admissionRowsAfter: after.admissionTotal,
    planRun: input.planRun, drift: before.drift, driftDetail: before.driftDetail, verdicts: before.verdicts, committed,
    approvalSha256: input.approvalSha256,
  };
}

/** Run one mode. The caller has already proved the owner identity and the dispatch gates. */
export async function runOrgAdmissionRepair(client: MigrationClient, input: OrgAdmissionRepairInput): Promise<OrgAdmissionRepairResult> {
  const readOnly = input.mode !== "admit";
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
        ledgerDrift = typeof c === "string" && /^[a-z0-9_]{1,64}$/u.test(c) ? c : "org_admission_repair_ledger";
      }
      const s = await snapshot(client, input.orgs, ledger, ledgerDrift, { lock: false });
      await client.query("ROLLBACK");
      return { receipt: receiptOf(input, ledger, s, s, "ORG_ADMISSION_REPAIR_READBACK", false), plan: null };
    }
    const ledger = await admissionRenewalLedger(client);
    if (input.mode === "plan") {
      const s = await snapshot(client, input.orgs, ledger, null, { lock: false });
      if (s.drift !== null) refuse(s.drift, s.driftDetail ?? undefined);
      const capturedMs = canonicalMs(s.now);
      if (capturedMs === null || BigInt(capturedMs) * 1000n !== s.nowUs) refuse("org_admission_repair_timestamp_precision");
      const plan: OrgAdmissionRepairPlan = {
        capturedAt: s.now, orgsSha256: input.orgsSha256, orgCount: input.orgs.length,
        orgScanSha256: s.orgs.map((o) => o.scanSha256), scanTablesSha256: s.scanTablesSha256,
        generation: s.generation!, validUntil: s.policy!.valid_until, policyDescriptorSha256: s.policyDescriptorSha256!,
        ledgerTail: ledger.ledgerTail, stepCount: ledger.stepCount, ledgerPrefixDigest: ledger.ledgerPrefixDigest,
        sha: input.sha, planRun: input.planRun!,
      };
      parseOrgAdmissionRepairPlan(plan);
      await client.query("ROLLBACK");
      return { receipt: receiptOf(input, ledger, s, s, "ORG_ADMISSION_REPAIR_PLANNED", false), plan };
    }
    const plan = input.plan ?? refuse("org_admission_repair_plan");
    if (plan.orgsSha256 !== input.orgsSha256 || plan.orgCount !== input.orgs.length) refuse("org_admission_repair_plan_orgs");
    if (plan.ledgerTail !== ledger.ledgerTail || plan.stepCount !== ledger.stepCount ||
        plan.ledgerPrefixDigest !== ledger.ledgerPrefixDigest)
      refuse("org_admission_repair_ledger_drift");
    const before = await snapshot(client, input.orgs, ledger, null, { lock: true });
    if (before.drift !== null) refuse(before.drift, before.driftDetail ?? undefined);
    if (before.generation !== plan.generation) refuse("org_admission_repair_generation_stale");
    const ageUs = before.nowUs - BigInt(Date.parse(plan.capturedAt)) * 1000n;
    if (ageUs < 0n || ageUs >= BigInt(ORG_ADMISSION_REPAIR_PLAN_MAX_AGE_MS) * 1000n) refuse("org_admission_repair_plan_stale");
    const p = before.policy!;
    if (p.valid_until !== plan.validUntil || BigInt(p.valid_until_us) !== BigInt(Date.parse(plan.validUntil)) * 1000n)
      refuse("org_admission_repair_plan_expiry_mismatch");
    if (before.policyDescriptorSha256 !== plan.policyDescriptorSha256) refuse("org_admission_repair_plan_policy_mismatch");
    if (before.scanTablesSha256 !== plan.scanTablesSha256) refuse("org_admission_repair_plan_scan_tables_mismatch");
    const changed = before.orgs.find((o, i) => o.scanSha256 !== plan.orgScanSha256[i]);
    if (changed) refuse("org_admission_repair_plan_scan_mismatch", `org#${changed.index}`);
    let inserted = 0;
    for (const org of input.orgs) {
      let n: number;
      try {
        n = (await client.query<{ org_id: string }>(
          `INSERT INTO sessions.ingest_buffer_admission(org_id,privacy_baseline_sha256,retention_contract_sha256,
            schema_generation,valid_until,control_epoch)
           SELECT $1,privacy_baseline_sha256,retention_contract_sha256,generation,valid_until,control_epoch
             FROM sessions.ingest_lazy_policy WHERE singleton RETURNING org_id`, [org])).rows.length;
      } catch {
        return refuse("org_admission_repair_insert");
      }
      if (n !== 1) refuse("org_admission_repair_insert");
      inserted += n;
    }
    // No per-table rescan here (the table lock is held): the rows, totals, policy and scanned-table set are re-read.
    const after = await snapshot(client, input.orgs, ledger, null, { lock: false, scan: false });
    const byOrg = new Map(after.rows.map((r) => [r.org_id, r] as const));
    if (inserted !== input.orgs.length || after.admissionTotal !== before.admissionTotal + input.orgs.length ||
        after.rows.length !== input.orgs.length || after.policyDescriptorSha256 !== before.policyDescriptorSha256 ||
        after.generation !== plan.generation || after.scanTablesSha256 !== plan.scanTablesSha256 ||
        input.orgs.some((o) => {
          const r = byOrg.get(o);
          return !r || r.schema_generation !== p.generation || r.control_epoch !== p.control_epoch ||
            r.privacy_baseline_sha256 !== p.privacy_baseline_sha256 ||
            r.retention_contract_sha256 !== p.retention_contract_sha256 ||
            r.valid_until !== p.valid_until || r.valid_until_us !== p.valid_until_us;
        }))
      refuse("org_admission_repair_postcondition");
    try {
      for (const org of input.orgs) await client.query("SELECT sessions.ingest_lazy_guard($1)", [org]);
    } catch {
      return refuse("org_admission_repair_postcondition_guard");
    }
    try {
      await client.query("COMMIT");
    } catch {
      return refuse("org_admission_repair_commit_unknown");
    }
    return { receipt: receiptOf(input, ledger, before, after, "ORG_ADMISSION_REPAIRED", true), plan: null };
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    throw error;
  }
}
