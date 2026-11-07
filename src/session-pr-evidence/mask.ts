/**
 * The history record-winner mask for a history-authoritative scope (05026000). The mask is the
 * scope's exact record set: a record id absent from it belongs to another scope (a move) and must
 * not contribute evidence here. A non-null winner names the copy that is current: its origin must be
 * the copy read, and a `delete` winner removes the record. Rows are filtered before extraction, so a
 * moved or deleted record can never be attributed to this scope.
 *
 * Revision is not checked: projected rows do not carry it. A copy from the winner's origin is kept;
 * a copy from another origin is dropped (under-attribution, never mis-attribution).
 */
import { deliveryMachine } from "@rhc/lib/pipeline/relayhistory-projection.ts";
import type { PrEvidenceProjectionRow } from "./extract.ts";

export interface MaskWinner { originId: string; operation: "upsert" | "delete" }
export type HistoryMask = ReadonlyMap<string, MaskWinner | null>;

const text = (v: unknown) => (typeof v === "string" && v !== "" ? v : null);

/** The record id and origin a projected row came from, or null when it has none. */
function provenance(row: PrEvidenceProjectionRow, workspaceId: string): { recordId: string; originMatches: (origin: string) => boolean } | null {
  const r = row.row;
  if (row.table === "convergence_events") {
    const delivery = text(r.delivery_record_id);
    const machine = text(r.machine_id);
    if (delivery && machine) return { recordId: delivery, originMatches: (o) => deliveryMachine(workspaceId, o) === machine };
    const direct = text(r.event_id);
    if (direct && machine) return { recordId: direct, originMatches: (o) => o === `machine:${machine}` };
    return null;
  }
  const recordId = text(r.record_id), origin = text(r.origin_id);
  return recordId && origin ? { recordId, originMatches: (o) => o === origin } : null;
}

export function applyHistoryMask(rows: readonly PrEvidenceProjectionRow[], workspaceId: string, mask: HistoryMask): PrEvidenceProjectionRow[] {
  return rows.filter((row) => {
    const p = provenance(row, workspaceId);
    if (!p || !mask.has(p.recordId)) return false;
    const winner = mask.get(p.recordId) ?? null;
    if (winner === null) return true;
    return winner.operation !== "delete" && p.originMatches(winner.originId);
  });
}
