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
import { CONVERGENCE_EVENT_KIND, canonicalTimestamp, projectRelayhistorySession } from "@rhc/lib/pipeline/relayhistory-projection.ts";
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
 * A direct `convergence_event` line as its row, exactly as the reader's projection (directEvent)
 * builds it from the RAW record: tenant/embedding fields dropped, times canonicalized. Identity
 * must be the line's own (event id = record id, origin = machine:<machine_id>, this scope's
 * source); anything else is dropped (under-attribution, never another copy's row).
 */
function directRow(record: RelayhistoryStreamRecord, source: string): PrEvidenceProjectionRow | null {
  const p = record.payload;
  if (!p) return null;
  const { org_id: _o, workspace_id: _w, embedding: _e, ts_ms: _t, ...row } = p;
  if (row.event_id !== record.record_id || typeof row.machine_id !== "string" ||
      record.origin_id !== `machine:${row.machine_id}` || row.source !== source) return null;
  try {
    return { table: "convergence_events", row: { ...row,
      ts: canonicalTimestamp(String(row.ts)), ingested_at: canonicalTimestamp(String(row.ingested_at ?? record.received_at)) } };
  } catch {
    return null;
  }
}

/**
 * The evidence input rows of a history scope, built ONLY from admitted raw records: delivery
 * records are masked and then projected (the catalog collapse sees admitted records only), and
 * admitted direct lines become rows from their own raw payload. The reader's projected rows are
 * never consulted, so no reconstructed (machine_id, event_id) key can re-admit a masked-out copy:
 * a direct line from machine M = deliveryMachine(W, O) with event id M:r1 and the delivery record
 * r1 from origin O project to the same key, but only the admitted raw record contributes.
 */
export function maskedProjection(
  read: { records: readonly RelayhistoryStreamRecord[] },
  scope: { workspaceId: string; source: string; sessionId: string },
  mask: HistoryMask,
): PrEvidenceProjectionRow[] {
  const admitted = maskRecords(read.records, mask);
  const projected = projectRelayhistorySession(admitted.filter((r) => r.kind !== CONVERGENCE_EVENT_KIND), {
    workspaceId: scope.workspaceId, source: scope.source, sessionId: scope.sessionId,
    // Used only to validate direct lines, which are not passed here.
    machineId: "pr-evidence", userId: "",
  }).rows;
  const direct = admitted.filter((r) => r.kind === CONVERGENCE_EVENT_KIND)
    .map((r) => directRow(r, scope.source)).filter((r): r is PrEvidenceProjectionRow => r !== null);
  // The reader's single-row winner rule (canonical-projection.ts readCanonicalScope: direct groups
  // first, the activity group last, byKey/metadataByKey.set): for one canonical event identity
  // (machine_id, source, kind, event_id) the delivery/activity row overwrites the direct row, as
  // Neon's one-row primary key does. Catalog rows are kept as they are.
  const winners = new Map<string, PrEvidenceProjectionRow>();
  const catalog: PrEvidenceProjectionRow[] = [];
  for (const r of [...direct, ...projected]) {
    if (r.table !== "convergence_events") { catalog.push(r); continue; }
    winners.set(canonicalEventKey(r.row), r);
  }
  return [...catalog, ...winners.values()];
}

/** readCanonicalScope's keyOf: [machine_id, source, kind, event_id] joined by U+001F. */
export function canonicalEventKey(row: Record<string, unknown>): string {
  return [String(row.machine_id), String(row.source), String(row.kind), String(row.event_id)].join("\u001f");
}
