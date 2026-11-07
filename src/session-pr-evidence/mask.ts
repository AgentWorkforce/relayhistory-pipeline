/**
 * The history record-winner mask for a history-authoritative scope (05026000). The mask is the
 * scope's exact record set: a record id absent from it belongs to another scope (a move) and must
 * not contribute evidence here. A non-null winner names the copy that is current: only that origin
 * and revision is kept, and a `delete` winner removes the record.
 *
 * The mask is applied to the RAW normalized records, BEFORE projectRelayhistorySession: the
 * projection collapses catalog records (session, commit_link, ...) by logical key, so masking
 * afterwards would let an out-of-mask record win the collapse and then be dropped, suppressing the
 * valid in-mask record it displaced.
 */
import { CONVERGENCE_EVENT_KIND, projectRelayhistorySession } from "@rhc/lib/pipeline/relayhistory-projection.ts";
import type { RelayhistoryStreamRecord } from "@rhc/lib/pipeline/relayhistory-transform.ts";
import type { PrEvidenceProjectionRow } from "./extract.ts";

export interface MaskWinner { originId: string; operation: "upsert" | "delete"; revision?: string }
export type HistoryMask = ReadonlyMap<string, MaskWinner | null>;

/** The records the mask admits: in the scope's record set, and the winning copy when one is named. */
export function maskRecords(records: readonly RelayhistoryStreamRecord[], mask: HistoryMask): RelayhistoryStreamRecord[] {
  return records.filter((r) => {
    if (!mask.has(r.record_id)) return false;
    const w = mask.get(r.record_id) ?? null;
    if (w === null) return true;
    return w.operation !== "delete" && r.origin_id === w.originId && (w.revision === undefined || String(r.revision) === w.revision);
  });
}

/**
 * The evidence input rows of a history scope: delivery records are masked raw and then projected
 * (catalog collapse happens over admitted records only). Direct `convergence_event` lines are
 * already rows; the reader projected them with their own manifest's identity, so those rows are
 * kept when their raw record is admitted.
 */
export function maskedProjection(
  read: { records: readonly RelayhistoryStreamRecord[]; projectionRows: readonly PrEvidenceProjectionRow[] },
  scope: { workspaceId: string; source: string; sessionId: string },
  mask: HistoryMask,
): PrEvidenceProjectionRow[] {
  const admitted = maskRecords(read.records, mask);
  const delivery = admitted.filter((r) => r.kind !== CONVERGENCE_EVENT_KIND);
  const direct = new Set(admitted.filter((r) => r.kind === CONVERGENCE_EVENT_KIND).map((r) => r.record_id));
  const allDirect = new Set(read.records.filter((r) => r.kind === CONVERGENCE_EVENT_KIND).map((r) => r.record_id));
  const projected = projectRelayhistorySession(delivery, {
    workspaceId: scope.workspaceId, source: scope.source, sessionId: scope.sessionId,
    // Used only to validate direct lines, which are not passed here.
    machineId: "pr-evidence", userId: "",
  }).rows;
  const directRows = read.projectionRows.filter((r) =>
    r.table === "convergence_events" && allDirect.has(String(r.row.event_id)) && direct.has(String(r.row.event_id)));
  return [...projected, ...directRows];
}
