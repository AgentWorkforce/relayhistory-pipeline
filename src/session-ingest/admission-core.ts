/**
 * The one admission core the request path (POST /v1/delivery/batches, queued-v1) and the upload inbox
 * (upload-admission.ts, relay-upload-inbox-v1) share before an inbox object is written: the database-free
 * prepare (account check, normalisation, scrub, digests, receipt), the tenant key-segment check, and the
 * destination's per-request budget. Same order, same errors, same prepared bytes on both paths.
 */
import { assertWithinBudget, type DeliverDeps } from "./deliver.ts";
import { assertTenantSegment } from "./writer.ts";
import { DeliveryError, type HistoryExportBatch } from "./vendor/delivery/delivery-contracts.ts";
import { prepareDelivery, type PreparedDelivery } from "./vendor/delivery/prepare.ts";

/**
 * The verified org is not a valid key segment for this key root (writer.ts assertTenantSegment). The
 * request is not retryable as sent, so it is a 400 invalid_delivery; `reason` keeps the precise cause
 * for the upload inbox, which records it as `tenant_not_a_segment`.
 */
export class TenantNotASegment extends DeliveryError {
  readonly reason = "tenant_not_a_segment";
  constructor() {
    super("invalid_delivery", 400, "The tenant is not a valid storage key segment");
  }
}

export async function prepareForAdmission(
  deliver: Pick<DeliverDeps, "target" | "babysitterRetention" | "replica" | "subrequestCap" | "subrequestRedirects" | "acceptanceMode" | "inlineMaxSessions" | "retentionGeneration">,
  tenant: { orgId: string; workspaceId: string },
  batch: HistoryExportBatch,
  receivedAtMs: number,
): Promise<PreparedDelivery> {
  const prepared = await prepareDelivery({ orgId: tenant.orgId, workspaceId: tenant.workspaceId }, batch);
  try {
    assertTenantSegment(deliver.target.keyRoot, tenant.orgId);
  } catch {
    throw new TenantNotASegment();
  }
  assertWithinBudget(deliver, prepared.records, new Date(receivedAtMs));
  return prepared;
}
