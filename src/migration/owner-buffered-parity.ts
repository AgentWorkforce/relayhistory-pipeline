/**
 * Option C G1/G2, contract v5.3.
 *
 * G1 compares one user's current Neon privacy state with the target's ordered
 * control lineage and full privacy sets. G2 may persist the returned proof only
 * when `equal` is true. Database reads/writes are deliberately separate: a
 * census is never allowed to write as a side effect.
 */

import type { Queryable } from "./relayhistory-export.ts";

const HEX64 = /^[0-9a-f]{64}$/u;
const DECIMAL = /^(0|[1-9][0-9]*)$/u;

const DELETION_SET_DIGEST = `encode(sha256(convert_to(jsonb_build_array(
  'owner-buffered-deletion-set-v1',$1::text,$2::text,$3::text,
  coalesce(jsonb_agg(jsonb_build_array(origin_id,source,session_id)
    ORDER BY origin_id COLLATE "C",source COLLATE "C",session_id COLLATE "C"),'[]'::jsonb))::text,'UTF8')),'hex')`;
const RESET_SET_DIGEST = `encode(sha256(convert_to(jsonb_build_array(
  'owner-buffered-reset-set-v1',$1::text,$2::text,$3::text,
  coalesce(jsonb_agg(to_jsonb(request_id)
    ORDER BY request_id COLLATE "C"),'[]'::jsonb))::text,'UTF8')),'hex')`;

/** One exact-user snapshot on the reviewed Neon read-only connection. */
export const NEON_OWNER_BUFFERED_CENSUS_SQL = `
WITH status AS MATERIALIZED (SELECT sessions.delivery_cutover_status() AS value),
head AS MATERIALIZED (
  SELECT epoch,version::text AS version,digest,kind,pending
    FROM sessions.delivery_cutover_control_heads
   WHERE org_id=$1 AND workspace_id=$2 AND user_id=$3
), deletion_rows AS MATERIALIZED (
  SELECT origin_id,source,session_id
    FROM sessions.deleted_upload_sessions
   WHERE org_id=$1 AND workspace_id=$2 AND user_id=$3
), reset_rows AS MATERIALIZED (
  SELECT request_id
    FROM sessions.session_resets
   WHERE org_id=$1 AND workspace_id=$2 AND user_id=$3
)
SELECT (SELECT value->>'phase' FROM status) AS cutover_status,
       (SELECT value->>'epoch' FROM status) AS control_epoch,
       (SELECT value->>'privacyBaselineSha256' FROM status) AS privacy_baseline_sha256,
       (SELECT to_jsonb(head) FROM head) AS head,
       (SELECT count(*)::text FROM deletion_rows) AS deletion_count,
       (SELECT ${DELETION_SET_DIGEST} FROM deletion_rows) AS deletion_set_sha256,
       (SELECT count(*)::text FROM reset_rows) AS reset_count,
       (SELECT ${RESET_SET_DIGEST} FROM reset_rows) AS reset_set_sha256`;

/** One exact-user target snapshot through 043700's canonical digest helper. */
export const TARGET_OWNER_BUFFERED_CENSUS_SQL = `
WITH policy AS MATERIALIZED (
  SELECT control_epoch,privacy_baseline_sha256
    FROM sessions.ingest_lazy_policy WHERE singleton
), expected AS MATERIALIZED (
  SELECT * FROM sessions.owner_buffered_parity_expected($1,$2,$3,$4,$5::bigint,$6,$7)
)
SELECT (SELECT control_epoch FROM policy) AS control_epoch,
       (SELECT privacy_baseline_sha256 FROM policy) AS privacy_baseline_sha256,
       (SELECT target_generation FROM expected) AS target_generation,
       (SELECT receipts_ok FROM expected) AS receipts_contiguous,
       CASE WHEN $5::bigint IS NULL THEN NULL ELSE
         jsonb_build_object('epoch',$4::text,'version',$5::bigint::text,'digest',$6::text,'kind',$7::text) END AS latest_receipt,
       (SELECT target_lineage_digest FROM expected) AS target_lineage_digest,
       (SELECT deletion_set_digest FROM expected) AS deletion_set_sha256,
       (SELECT reset_set_digest FROM expected) AS reset_set_sha256`;

export interface ParityTenant {
  orgId: string;
  workspaceId: string;
  userId: string;
}

export interface ControlHead {
  epoch: string;
  version: string;
  digest: string;
  kind: "selection" | "reset";
  pending: boolean;
}

export interface ControlReceipt {
  epoch: string;
  version: string;
  digest: string;
  kind: "selection" | "reset";
}

export interface ParityCensusInput {
  tenant: ParityTenant;
  controlEpoch: string;
  privacyBaselineSha256: string;
  head: ControlHead | null;
  sourceDeletionSetSha256: string;
  sourceResetSetSha256: string;
  targetControlEpoch: string;
  targetPrivacyBaselineSha256: string;
  targetGeneration: string;
  targetLatestReceipt: ControlReceipt | null;
  targetReceiptVersionsContiguousFromOne: boolean;
  targetDeletionSetSha256: string;
  targetResetSetSha256: string;
  targetLineageDigest: string;
}

export interface OwnerBufferedParityProof {
  contract: "owner-buffered-parity/v1";
  tenant: ParityTenant;
  controlEpoch: string;
  privacyBaselineSha256: string;
  explicitNoHead: boolean;
  head: Omit<ControlHead, "pending"> | null;
  deletionSetSha256: string;
  resetSetSha256: string;
  targetLineageDigest: string;
  targetGeneration: string;
}

export type ParityMismatch =
  | "invalid_shape"
  | "source_head_pending"
  | "source_head_epoch_mismatch"
  | "control_epoch_mismatch"
  | "privacy_baseline_mismatch"
  | "target_lineage_gap"
  | "target_receipt_unexpected"
  | "target_receipt_missing"
  | "target_receipt_epoch_mismatch"
  | "target_receipt_version_mismatch"
  | "target_receipt_digest_mismatch"
  | "target_receipt_kind_mismatch"
  | "deletion_set_mismatch"
  | "reset_set_mismatch";

export interface ParityCensusReceipt {
  contract: "owner-buffered-parity-census/v1";
  tenant: ParityTenant;
  equal: boolean;
  mismatches: ParityMismatch[];
  proof: OwnerBufferedParityProof | null;
}

export class ParityCensusRefusal extends Error {
  constructor(readonly code: string) {
    super(code);
    this.name = "ParityCensusRefusal";
  }
}

export type ProofWritePlan = "insert" | "update" | "unchanged";

export const INSERT_OWNER_BUFFERED_PARITY_SQL = `
INSERT INTO sessions.owner_buffered_parity(
  org_id,workspace_id,user_id,cutover_epoch,head_epoch,head_version,head_digest,head_kind,no_head,
  deletion_set_digest,reset_set_digest,target_lineage_digest,target_generation,privacy_baseline_sha256
) VALUES($1,$2,$3,$4,$5,$6::bigint,$7,$8,$9,$10,$11,$12,$13,$14)
ON CONFLICT (org_id,workspace_id,user_id) DO NOTHING
RETURNING written_at`;

export const UPDATE_OWNER_BUFFERED_PARITY_CAS_SQL = `
UPDATE sessions.owner_buffered_parity SET
  cutover_epoch=$4,head_epoch=$5,head_version=$6::bigint,head_digest=$7,head_kind=$8,no_head=$9,
  deletion_set_digest=$10,reset_set_digest=$11,target_lineage_digest=$12,target_generation=$13,
  privacy_baseline_sha256=$14,written_at=clock_timestamp()
WHERE org_id=$1 AND workspace_id=$2 AND user_id=$3
  AND (head_epoch,head_version,head_digest,head_kind,no_head,privacy_baseline_sha256,target_generation,
       deletion_set_digest,reset_set_digest,target_lineage_digest)
      IS NOT DISTINCT FROM
      ($15,$16::bigint,$17,$18,$19,$20,$21,$22,$23,$24)
  AND ($6::bigint > coalesce($16::bigint,0) OR $13 IS DISTINCT FROM $21)
RETURNING written_at`;

export const READ_OWNER_BUFFERED_PARITY_SQL = `
SELECT cutover_epoch,head_epoch,head_version::text AS head_version,head_digest,head_kind,no_head,
       deletion_set_digest,reset_set_digest,target_lineage_digest,target_generation,privacy_baseline_sha256
  FROM sessions.owner_buffered_parity
 WHERE org_id=$1 AND workspace_id=$2 AND user_id=$3`;

function proofParams(proof: OwnerBufferedParityProof): unknown[] {
  return [
    proof.tenant.orgId, proof.tenant.workspaceId, proof.tenant.userId, proof.controlEpoch,
    proof.head?.epoch ?? null, proof.head?.version ?? null, proof.head?.digest ?? null, proof.head?.kind ?? null,
    proof.explicitNoHead, proof.deletionSetSha256, proof.resetSetSha256, proof.targetLineageDigest,
    proof.targetGeneration, proof.privacyBaselineSha256,
  ];
}

function sameHead(a: OwnerBufferedParityProof, b: OwnerBufferedParityProof): boolean {
  if (a.explicitNoHead !== b.explicitNoHead || (a.head === null) !== (b.head === null)) return false;
  return a.head === null || (b.head !== null && a.head.epoch === b.head.epoch && a.head.version === b.head.version &&
    a.head.digest === b.head.digest && a.head.kind === b.head.kind);
}

/** Pipeline-lead's G2 CAS ruling, before any SQL is issued. */
export function planProofWrite(
  prior: OwnerBufferedParityProof | null,
  next: OwnerBufferedParityProof,
): ProofWritePlan {
  if (prior === null) return "insert";
  if (!sameTenant(prior.tenant, next.tenant)) throw new ParityCensusRefusal("prior_tenant_mismatch");
  const headUnchanged = sameHead(prior, next);
  const setsUnchanged = prior.deletionSetSha256 === next.deletionSetSha256 &&
    prior.resetSetSha256 === next.resetSetSha256 && prior.targetLineageDigest === next.targetLineageDigest;
  if (headUnchanged && !setsUnchanged) throw new ParityCensusRefusal("same_head_digest_divergence");
  const identical = headUnchanged && setsUnchanged && prior.controlEpoch === next.controlEpoch &&
    prior.privacyBaselineSha256 === next.privacyBaselineSha256 && prior.targetGeneration === next.targetGeneration;
  if (identical) return "unchanged";
  const priorVersion = prior.head === null ? 0n : BigInt(prior.head.version);
  const nextVersion = next.head === null ? 0n : BigInt(next.head.version);
  if (nextVersion > priorVersion || next.targetGeneration !== prior.targetGeneration) return "update";
  throw new ParityCensusRefusal("proof_not_advanced");
}

/** Execute the already-planned mutation inside the caller's open target transaction. */
export async function writeProofRowCas(
  target: Queryable,
  prior: OwnerBufferedParityProof | null,
  next: OwnerBufferedParityProof,
): Promise<{ outcome: ProofWritePlan; writtenAt: unknown | null }> {
  const outcome = planProofWrite(prior, next);
  if (outcome === "unchanged") return { outcome, writtenAt: null };
  if (outcome === "insert") {
    const row = (await target.query<{ written_at: unknown }>(INSERT_OWNER_BUFFERED_PARITY_SQL, proofParams(next))).rows[0];
    if (!row) throw new ParityCensusRefusal("proof_insert_conflict");
    return { outcome, writtenAt: row.written_at };
  }
  if (!prior) throw new ParityCensusRefusal("prior_proof_missing");
  const priorCas = [
    prior.head?.epoch ?? null, prior.head?.version ?? null, prior.head?.digest ?? null, prior.head?.kind ?? null,
    prior.explicitNoHead, prior.privacyBaselineSha256, prior.targetGeneration, prior.deletionSetSha256,
    prior.resetSetSha256, prior.targetLineageDigest,
  ];
  const row = (await target.query<{ written_at: unknown }>(UPDATE_OWNER_BUFFERED_PARITY_CAS_SQL, [
    ...proofParams(next), ...priorCas,
  ])).rows[0];
  if (!row) throw new ParityCensusRefusal("proof_update_cas_conflict");
  return { outcome, writtenAt: row.written_at };
}

interface NeonCensusRow {
  cutover_status: unknown;
  control_epoch: unknown;
  privacy_baseline_sha256: unknown;
  head: unknown;
  deletion_count: unknown;
  deletion_set_sha256: unknown;
  reset_count: unknown;
  reset_set_sha256: unknown;
}

interface TargetCensusRow {
  control_epoch: unknown;
  privacy_baseline_sha256: unknown;
  target_generation: unknown;
  receipts_contiguous: unknown;
  latest_receipt: unknown;
  target_lineage_digest: unknown;
  deletion_set_sha256: unknown;
  reset_set_sha256: unknown;
}

async function readOnly<T>(db: Queryable, sql: string, params: unknown[], label: string): Promise<T> {
  await db.query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
  try {
    await db.query("SET LOCAL statement_timeout = '2s'");
    const row = (await db.query<T>(sql, params)).rows[0];
    if (!row) throw new ParityCensusRefusal(`${label}_row_missing`);
    await db.query("COMMIT");
    return row;
  } catch (error) {
    await db.query("ROLLBACK").catch(() => undefined);
    throw error;
  }
}

function boundedCount(value: unknown, max: bigint, code: string): void {
  if (typeof value !== "string" || !DECIMAL.test(value) || BigInt(value) > max) throw new ParityCensusRefusal(code);
}

function parseHead(value: unknown, code: string): ControlHead | null {
  if (value === null) return null;
  if (typeof value !== "object" || Array.isArray(value)) throw new ParityCensusRefusal(code);
  const row = value as Record<string, unknown>;
  const head: ControlHead = {
    epoch: String(row.epoch ?? ""),
    version: String(row.version ?? ""),
    digest: String(row.digest ?? ""),
    kind: row.kind as ControlHead["kind"],
    pending: row.pending as boolean,
  };
  if (!validReceipt(head) || typeof head.pending !== "boolean") throw new ParityCensusRefusal(code);
  return head;
}

function parseReceipt(value: unknown, code: string): ControlReceipt | null {
  if (value === null) return null;
  if (typeof value !== "object" || Array.isArray(value)) throw new ParityCensusRefusal(code);
  const row = value as Record<string, unknown>;
  const receipt: ControlReceipt = {
    epoch: String(row.epoch ?? ""),
    version: String(row.version ?? ""),
    digest: String(row.digest ?? ""),
    kind: row.kind as ControlReceipt["kind"],
  };
  if (!validReceipt(receipt)) throw new ParityCensusRefusal(code);
  return receipt;
}

function censusInput(tenant: ParityTenant, source: NeonCensusRow, destination: TargetCensusRow): ParityCensusInput {
  if (source.cutover_status !== "active") throw new ParityCensusRefusal("neon_cutover_not_active");
  boundedCount(source.deletion_count, 200_000n, "neon_deletion_set_too_large");
  boundedCount(source.reset_count, 100_000n, "neon_reset_set_too_large");
  if (typeof destination.receipts_contiguous !== "boolean") throw new ParityCensusRefusal("target_lineage_shape_invalid");
  return {
    tenant,
    controlEpoch: String(source.control_epoch ?? ""),
    privacyBaselineSha256: String(source.privacy_baseline_sha256 ?? ""),
    head: parseHead(source.head, "neon_head_shape_invalid"),
    sourceDeletionSetSha256: String(source.deletion_set_sha256 ?? ""),
    sourceResetSetSha256: String(source.reset_set_sha256 ?? ""),
    targetControlEpoch: String(destination.control_epoch ?? ""),
    targetPrivacyBaselineSha256: String(destination.privacy_baseline_sha256 ?? ""),
    targetGeneration: String(destination.target_generation ?? ""),
    targetLatestReceipt: parseReceipt(destination.latest_receipt, "target_receipt_shape_invalid"),
    targetReceiptVersionsContiguousFromOne: destination.receipts_contiguous,
    targetDeletionSetSha256: String(destination.deletion_set_sha256 ?? ""),
    targetResetSetSha256: String(destination.reset_set_sha256 ?? ""),
    targetLineageDigest: String(destination.target_lineage_digest ?? ""),
  };
}

function targetParams(tenant: ParityTenant, source: NeonCensusRow): unknown[] {
  const head = parseHead(source.head, "neon_head_shape_invalid");
  return [tenant.orgId, tenant.workspaceId, tenant.userId, String(source.control_epoch ?? ""),
    head?.version ?? null, head?.digest ?? null, head?.kind ?? null];
}

function sourceSnapshotKey(source: NeonCensusRow): string {
  const head = parseHead(source.head, "neon_head_shape_invalid");
  return JSON.stringify([
    source.cutover_status, source.control_epoch, source.privacy_baseline_sha256,
    head === null ? null : [head.epoch, head.version, head.digest, head.kind, head.pending],
    source.deletion_count, source.deletion_set_sha256, source.reset_count, source.reset_set_sha256,
  ]);
}

function storedProof(tenant: ParityTenant, row: Record<string, unknown> | undefined): OwnerBufferedParityProof | null {
  if (!row) return null;
  const noHead = row.no_head;
  if (typeof noHead !== "boolean") throw new ParityCensusRefusal("stored_proof_shape_invalid");
  const head: OwnerBufferedParityProof["head"] = noHead ? null : {
    epoch: String(row.head_epoch ?? ""), version: String(row.head_version ?? ""),
    digest: String(row.head_digest ?? ""), kind: row.head_kind as "selection" | "reset",
  };
  const proof: OwnerBufferedParityProof = {
    contract: "owner-buffered-parity/v1", tenant: { ...tenant },
    controlEpoch: String(row.cutover_epoch ?? ""),
    privacyBaselineSha256: String(row.privacy_baseline_sha256 ?? ""), explicitNoHead: noHead, head,
    deletionSetSha256: String(row.deletion_set_digest ?? ""), resetSetSha256: String(row.reset_set_digest ?? ""),
    targetLineageDigest: String(row.target_lineage_digest ?? ""), targetGeneration: String(row.target_generation ?? ""),
  };
  const headValid = head === null || validReceipt({ ...head });
  if (!HEX64.test(proof.controlEpoch) || !HEX64.test(proof.privacyBaselineSha256) || !HEX64.test(proof.deletionSetSha256) ||
      !HEX64.test(proof.resetSetSha256) || !HEX64.test(proof.targetLineageDigest) || !HEX64.test(proof.targetGeneration) ||
      !headValid) throw new ParityCensusRefusal("stored_proof_shape_invalid");
  return proof;
}

/** G1: read both databases only, then emit one content-free proof receipt. */
export async function readParityCensus(
  neon: Queryable,
  target: Queryable,
  tenant: ParityTenant,
): Promise<ParityCensusReceipt> {
  if (!validTenant(tenant)) throw new ParityCensusRefusal("tenant_invalid");
  const params = [tenant.orgId, tenant.workspaceId, tenant.userId];
  const sourceA = await readOnly<NeonCensusRow>(neon, NEON_OWNER_BUFFERED_CENSUS_SQL, params, "neon");
  const destination = await readOnly<TargetCensusRow>(target, TARGET_OWNER_BUFFERED_CENSUS_SQL, targetParams(tenant, sourceA), "target");
  const sourceB = await readOnly<NeonCensusRow>(neon, NEON_OWNER_BUFFERED_CENSUS_SQL, params, "neon_recheck");
  if (sourceSnapshotKey(sourceA) !== sourceSnapshotKey(sourceB)) throw new ParityCensusRefusal("neon_changed_during_census");
  return evaluateParityCensus(censusInput(tenant, sourceB, destination));
}

/**
 * G2 orchestration. Caller supplies pinned Neon-read and target-owner sessions;
 * credentials never enter the proof or logs. This function is the only writer.
 */
export async function writeCurrentParityProof(
  neon: Queryable,
  targetOwner: Queryable,
  tenant: ParityTenant,
  expectedTargetGeneration?: string,
): Promise<{ census: ParityCensusReceipt; outcome: ProofWritePlan; writtenAt: unknown | null }> {
  if (!validTenant(tenant)) throw new ParityCensusRefusal("tenant_invalid");
  const params = [tenant.orgId, tenant.workspaceId, tenant.userId];
  await targetOwner.query("BEGIN ISOLATION LEVEL REPEATABLE READ");
  try {
    await targetOwner.query("SET LOCAL statement_timeout = '2s'");
    // REPEATABLE READ takes its snapshot on the first data statement, not BEGIN/SET.
    await targetOwner.query("SELECT control_epoch AS snapshot_anchor FROM sessions.ingest_lazy_policy WHERE singleton");
    const sourceA = await readOnly<NeonCensusRow>(neon, NEON_OWNER_BUFFERED_CENSUS_SQL, params, "neon");
    const destination = (await targetOwner.query<TargetCensusRow>(TARGET_OWNER_BUFFERED_CENSUS_SQL, targetParams(tenant, sourceA))).rows[0];
    if (!destination) throw new ParityCensusRefusal("target_row_missing");
    const prior = storedProof(tenant, (await targetOwner.query<Record<string, unknown>>(READ_OWNER_BUFFERED_PARITY_SQL, params)).rows[0]);
    // Equality proves Neon did not move across the target-side census.
    const sourceB = await readOnly<NeonCensusRow>(neon, NEON_OWNER_BUFFERED_CENSUS_SQL, params, "neon_recheck");
    if (sourceSnapshotKey(sourceA) !== sourceSnapshotKey(sourceB)) throw new ParityCensusRefusal("neon_changed_during_census");
    const census = evaluateParityCensus(censusInput(tenant, sourceB, destination));
    if (!census.equal || !census.proof) throw new ParityCensusRefusal(`parity_mismatch:${census.mismatches.join(",")}`);
    if (expectedTargetGeneration !== undefined && census.proof.targetGeneration !== expectedTargetGeneration)
      throw new ParityCensusRefusal("target_generation_mismatch");
    const result = await writeProofRowCas(targetOwner, prior, census.proof);
    await targetOwner.query("COMMIT");
    return { census, ...result };
  } catch (error) {
    await targetOwner.query("ROLLBACK").catch(() => undefined);
    throw error;
  }
}

function validTenant(tenant: ParityTenant): boolean {
  return [tenant.orgId, tenant.workspaceId, tenant.userId]
    .every((value) => typeof value === "string" && value.length >= 1 && value.length <= 128);
}

function validReceipt(receipt: ControlReceipt): boolean {
  return HEX64.test(receipt.epoch) && DECIMAL.test(receipt.version) && receipt.version !== "0" &&
    HEX64.test(receipt.digest) && (receipt.kind === "selection" || receipt.kind === "reset");
}

function validInput(input: ParityCensusInput): boolean {
  return validTenant(input.tenant) && HEX64.test(input.controlEpoch) &&
    HEX64.test(input.privacyBaselineSha256) && HEX64.test(input.targetControlEpoch) &&
    HEX64.test(input.targetPrivacyBaselineSha256) && HEX64.test(input.targetGeneration) &&
    HEX64.test(input.sourceDeletionSetSha256) && HEX64.test(input.sourceResetSetSha256) &&
    HEX64.test(input.targetDeletionSetSha256) && HEX64.test(input.targetResetSetSha256) &&
    HEX64.test(input.targetLineageDigest) &&
    (input.head === null || (validReceipt(input.head) && typeof input.head.pending === "boolean")) &&
    (input.targetLatestReceipt === null || validReceipt(input.targetLatestReceipt));
}

/** Pure G1 comparison. The returned proof is the only value accepted by G2. */
export function evaluateParityCensus(input: ParityCensusInput): ParityCensusReceipt {
  const mismatches: ParityMismatch[] = [];
  if (!validInput(input)) {
    mismatches.push("invalid_shape");
  } else {
    if (input.head?.pending) mismatches.push("source_head_pending");
    if (input.head !== null && input.head.epoch !== input.controlEpoch) mismatches.push("source_head_epoch_mismatch");
    if (input.targetControlEpoch !== input.controlEpoch) mismatches.push("control_epoch_mismatch");
    if (input.targetPrivacyBaselineSha256 !== input.privacyBaselineSha256) mismatches.push("privacy_baseline_mismatch");
    if (!input.targetReceiptVersionsContiguousFromOne) mismatches.push("target_lineage_gap");
    if (input.head === null) {
      if (input.targetLatestReceipt !== null) mismatches.push("target_receipt_unexpected");
    } else if (input.targetLatestReceipt === null) {
      mismatches.push("target_receipt_missing");
    } else {
      if (input.targetLatestReceipt.epoch !== input.head.epoch) mismatches.push("target_receipt_epoch_mismatch");
      if (input.targetLatestReceipt.version !== input.head.version) mismatches.push("target_receipt_version_mismatch");
      if (input.targetLatestReceipt.digest !== input.head.digest) mismatches.push("target_receipt_digest_mismatch");
      if (input.targetLatestReceipt.kind !== input.head.kind) mismatches.push("target_receipt_kind_mismatch");
    }
    if (input.targetDeletionSetSha256 !== input.sourceDeletionSetSha256) mismatches.push("deletion_set_mismatch");
    if (input.targetResetSetSha256 !== input.sourceResetSetSha256) mismatches.push("reset_set_mismatch");
  }

  const proof: OwnerBufferedParityProof | null = mismatches.length === 0 ? {
    contract: "owner-buffered-parity/v1",
    tenant: { ...input.tenant },
    controlEpoch: input.controlEpoch,
    privacyBaselineSha256: input.privacyBaselineSha256,
    explicitNoHead: input.head === null,
    head: input.head === null ? null : {
      epoch: input.head.epoch,
      version: input.head.version,
      digest: input.head.digest,
      kind: input.head.kind,
    },
    deletionSetSha256: input.sourceDeletionSetSha256,
    resetSetSha256: input.sourceResetSetSha256,
    targetLineageDigest: input.targetLineageDigest,
    targetGeneration: input.targetGeneration,
  } : null;

  return {
    contract: "owner-buffered-parity-census/v1",
    tenant: { ...input.tenant },
    equal: mismatches.length === 0,
    mismatches,
    proof,
  };
}

function sameTenant(a: ParityTenant, b: ParityTenant): boolean {
  return a.orgId === b.orgId && a.workspaceId === b.workspaceId && a.userId === b.userId;
}

/** Rule 4's current-row check, independent of the admission witness in rule 3. */
export function proofMatchesCurrent(proof: OwnerBufferedParityProof | null, current: ParityCensusInput): boolean {
  if (!proof || proof.contract !== "owner-buffered-parity/v1" || !validInput(current)) return false;
  if (!sameTenant(proof.tenant, current.tenant) || proof.controlEpoch !== current.controlEpoch ||
      proof.privacyBaselineSha256 !== current.privacyBaselineSha256 ||
      current.targetControlEpoch !== current.controlEpoch ||
      current.targetPrivacyBaselineSha256 !== current.privacyBaselineSha256 ||
      proof.targetGeneration !== current.targetGeneration ||
      proof.deletionSetSha256 !== current.sourceDeletionSetSha256 ||
      proof.deletionSetSha256 !== current.targetDeletionSetSha256 ||
      proof.resetSetSha256 !== current.sourceResetSetSha256 ||
      proof.resetSetSha256 !== current.targetResetSetSha256 ||
      proof.targetLineageDigest !== current.targetLineageDigest ||
      !current.targetReceiptVersionsContiguousFromOne || current.head?.pending ||
      (current.head !== null && current.head.epoch !== current.controlEpoch)) return false;
  if (current.head === null) return proof.explicitNoHead && proof.head === null && current.targetLatestReceipt === null;
  return !proof.explicitNoHead && proof.head !== null && current.targetLatestReceipt !== null &&
    proof.head.epoch === current.head.epoch && proof.head.version === current.head.version &&
    proof.head.digest === current.head.digest && proof.head.kind === current.head.kind &&
    current.targetLatestReceipt.epoch === current.head.epoch &&
    current.targetLatestReceipt.version === current.head.version &&
    current.targetLatestReceipt.digest === current.head.digest &&
    current.targetLatestReceipt.kind === current.head.kind;
}

/** Rules 1-4 visibility core. Rule 5 remains an independent SQL defense. */
export function bufferedParityVisible(
  admission: { epoch: string; version: string } | null,
  current: ParityCensusInput,
  proof: OwnerBufferedParityProof | null,
): boolean {
  if (!admission || !HEX64.test(admission.epoch) || !DECIMAL.test(admission.version) ||
      current.head?.pending || !proofMatchesCurrent(proof, current)) return false;
  const version = current.head?.version ?? "0";
  return admission.epoch === current.controlEpoch && admission.version === version;
}
