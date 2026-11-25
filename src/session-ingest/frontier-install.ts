/** One-shot attester boundary. No runtime, credentials, SQL writer or production adapter is installed here. */
import {
  evaluateCanonicalFrontier, frontierCanonicalJson, frontierSha256,
  type CanonicalFrontierCapture, type FrontierExportBinding, type FrontierTenant,
} from "./frontier-plan.ts";

const SHA = /^[a-f0-9]{64}$/;
const NONCE = /^[a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12}$/;

/** Returned only by the trusted, independent approval reader, by exact nonce.
 * The reader must verify reviewer, pinned verifier, independent whole-user evidence
 * and unconsumed status, returning null for unavailable/unverified approvals.
 * Inputs supplied by a publisher or read from a capture file are never a reader. */
export interface FrontierInstallApproval {
  nonce: string;
  planSha256: string;
  verifierReceiptSha256: string;
  wholeUserEvidenceSha256: string;
  tenant: FrontierTenant;
  /** Null pins exact absence (insert); a hash pins the exact existing row (renewal). */
  predecessorSha256: string | null;
}

interface FrontierInstallPorts {
  now(): Date;
  readVerifiedApproval(nonce: string): Promise<FrontierInstallApproval | null>;
  /** A SINGLE owner-only guarded RPC, with both DB-side plan/evidence/nonce checks
   * and the user-fence + frontier CAS. Recheck expiry, leases, generation,
   * ownership, privacy and approval; consume nonce and append audit atomically.
   * Return the persisted command digest only on committed success. */
  installGuarded(commandJson: string): Promise<string>;
}

type InstallOutcome = "disabled" | "held" | "committed" | "audit_lookup_required";

export async function installApprovedFrontier(
  enabled: boolean,
  nonce: string,
  capture: CanonicalFrontierCapture,
  verifiedExportBindings: FrontierExportBinding[],
  ports: FrontierInstallPorts,
): Promise<InstallOutcome> {
  if (enabled !== true) return "disabled";
  if (!NONCE.test(nonce)) return "held";
  const approval = await ports.readVerifiedApproval(nonce);
  if (!approval || approval.nonce !== nonce ||
      !SHA.test(approval.planSha256) || !SHA.test(approval.verifierReceiptSha256) ||
      !SHA.test(approval.wholeUserEvidenceSha256) ||
      (approval.predecessorSha256 !== null && !SHA.test(approval.predecessorSha256))) return "held";

  // Run the actual planner on capture and independently checked immutable export bindings.
  // A recomputed plan hash gains meaning only by comparison with the separate approval.
  const now = ports.now();
  const plan = evaluateCanonicalFrontier(capture, { now, exportBindings: verifiedExportBindings });
  if (plan.status !== "ready" || plan.planSha256 !== approval.planSha256 ||
      Date.parse(plan.validUntil) - now.getTime() < 150_000) return "held";
  const { tenant } = approval;
  const row = plan.frontiers.find(r => r.org_id === tenant.orgId && r.workspace_id === tenant.workspaceId &&
    r.user_id === tenant.userId && r.origin_id === tenant.originId);
  if (!row) return "held";

  const command = {
    protocol: "publication-frontier-guarded-install-v1",
    nonce, verifierReceiptSha256: approval.verifierReceiptSha256,
    wholeUserEvidenceSha256: approval.wholeUserEvidenceSha256,
    predecessorSha256: approval.predecessorSha256,
    captureId: plan.captureId, evidenceSha256: plan.evidenceSha256, planSha256: plan.planSha256, row,
  };
  const json = frontierCanonicalJson(command);
  const digest = frontierSha256(command);
  try {
    return await ports.installGuarded(json) === digest ? "committed" : "audit_lookup_required";
  } catch {
    // Even a transport rejection can follow a commit. Never assume rollback or retry the write here.
    return "audit_lookup_required";
  }
}
