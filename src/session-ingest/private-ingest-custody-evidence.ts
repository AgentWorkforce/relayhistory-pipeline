/** Reviewed, privacy-safe custody evidence. No receipt is qualified at this source revision. */
import { createHash } from "node:crypto";

export interface ShadowMatchReceipt {
  version: 1;
  kind: "custody-shadow-match";
  generation: string;
  deploymentId: string;
  versionId: string;
  observedFrom: string;
  observedThrough: string;
  batchMatches: number;
  controlMatches: number;
  mismatches: 0;
  errors: 0;
  copySealReceiptSha256: string;
  mirrorReceiptSha256: string;
}

export interface ReviewedShadowMatch {
  receipt: ShadowMatchReceipt;
  /** SHA-256 of the exact compact JSON receipt, without a trailing newline. */
  receiptSha256: string;
}

// Populate only through an independently reviewed source change backed by the retained
// genuine receipt. A workflow input, environment variable or mode readback cannot set it.
export const REVIEWED_CUSTODY_SHADOW_MATCH: ReviewedShadowMatch | null = null;

const HEX = /^[0-9a-f]{64}$/u;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/u;
const KEYS = ["version", "kind", "generation", "deploymentId", "versionId", "observedFrom",
  "observedThrough", "batchMatches", "controlMatches", "mismatches", "errors",
  "copySealReceiptSha256", "mirrorReceiptSha256"] as const;
function refuse(code: string): never { throw Object.assign(new Error(code), { code }); }
const hash = (text: string) => createHash("sha256").update(text).digest("hex");
const digest = (value: unknown) => typeof value === "string" && HEX.test(value) && value !== "0".repeat(64);

/** Closed fields prevent tenant/content data from entering the retained plan artifact. */
export function custodyEvidenceText(reviewed = REVIEWED_CUSTODY_SHADOW_MATCH): string {
  if (reviewed === null) return JSON.stringify({ version: 1, shadowMatch: null }) + "\n";
  if (!reviewed || Object.keys(reviewed).sort().join() !== ["receipt", "receiptSha256"].sort().join())
    refuse("private_ingest_shadow_receipt_shape");
  const receipt = reviewed.receipt;
  if (!receipt || Object.keys(receipt).sort().join() !== [...KEYS].sort().join() ||
      receipt.version !== 1 || receipt.kind !== "custody-shadow-match" ||
      !digest(receipt.generation) || typeof receipt.deploymentId !== "string" || !UUID.test(receipt.deploymentId) ||
      typeof receipt.versionId !== "string" || !UUID.test(receipt.versionId) ||
      !digest(receipt.copySealReceiptSha256) || !digest(receipt.mirrorReceiptSha256) ||
      !Number.isSafeInteger(receipt.batchMatches) || receipt.batchMatches < 1 ||
      !Number.isSafeInteger(receipt.controlMatches) || receipt.controlMatches < 1 ||
      receipt.mismatches !== 0 || receipt.errors !== 0 || !digest(reviewed.receiptSha256))
    refuse("private_ingest_shadow_receipt_shape");
  for (const date of [receipt.observedFrom, receipt.observedThrough]) {
    if (typeof date !== "string" || !Number.isFinite(Date.parse(date)) ||
        new Date(date).toISOString() !== date) refuse("private_ingest_shadow_receipt_time");
  }
  // The retained receipt's byte order is fixed by this schema, not object insertion order.
  const canonical = JSON.stringify(Object.fromEntries(KEYS.map((key) => [key, receipt[key]])));
  if (hash(canonical) !== reviewed.receiptSha256) refuse("private_ingest_shadow_receipt_digest");
  return JSON.stringify({ version: 1, shadowMatch: { receipt: JSON.parse(canonical), receiptSha256: reviewed.receiptSha256 } }) + "\n";
}

export function custodyEvidenceConfirmationSuffix(reviewed = REVIEWED_CUSTODY_SHADOW_MATCH): string {
  custodyEvidenceText(reviewed);
  return reviewed === null ? "" : ` SHADOW ${reviewed.receiptSha256}`;
}

export function requireCustodyEvidenceText(text: string): void {
  if (text !== custodyEvidenceText()) refuse("private_ingest_custody_evidence_drift");
}

/** Only the first frozen promotion consumes a fresh shadow proof; later phases retain their own gates. */
export function requireCustodyShadowMatch(
  live: { phase: string; deploymentId: string; versionId: string; createdOn: string | null },
  generation: string,
  reviewed = REVIEWED_CUSTODY_SHADOW_MATCH,
  now = Date.now(),
): string {
  custodyEvidenceText(reviewed);
  if (reviewed === null) refuse("private_ingest_shadow_receipt_missing");
  const r = reviewed.receipt;
  if (live.phase !== "step-0-shadow" || r.generation !== generation ||
      r.deploymentId !== live.deploymentId || r.versionId !== live.versionId)
    refuse("private_ingest_shadow_receipt_predecessor");
  const from = Date.parse(r.observedFrom), through = Date.parse(r.observedThrough),
    created = Date.parse(live.createdOn ?? "");
  if (!Number.isFinite(now) || !Number.isFinite(created) || from < created ||
      through - from < 10 * 60_000 || through > now || now - through > 24 * 60 * 60_000)
    refuse("private_ingest_shadow_receipt_stale");
  return reviewed.receiptSha256;
}

/** RHC must resume the Neon witness before a frozen Worker can return to shadow. */
export function requireRelayhistoryWitness(
  active: { deploymentId: string; versionId: string },
  version: { id?: unknown; resources?: { bindings?: Array<{ name?: unknown; type?: unknown; text?: unknown }> } },
) {
  if (!UUID.test(active.deploymentId) || !UUID.test(active.versionId) || version.id !== active.versionId ||
      !Array.isArray(version.resources?.bindings)) refuse("private_ingest_rollback_witness_readback");
  const neon = version.resources.bindings.filter((b) => b.name === "NEON_APP_DATABASE_URL");
  if (neon.length !== 1 || neon[0]!.type !== "secret_text")
    refuse("private_ingest_rollback_witness_runtime_binding");
  const modes = version.resources.bindings.filter((b) => b.name === "DELIVERY_CUSTODY_SOURCE_MODE");
  if (modes.length > 1 || (modes.length === 1 &&
      (modes[0]!.type !== "plain_text" || (modes[0]!.text !== "" && modes[0]!.text !== "relayhistory"))))
    refuse("private_ingest_rollback_witness_mode");
  return { deploymentId: active.deploymentId, versionId: active.versionId, mode: "relayhistory" as const };
}
