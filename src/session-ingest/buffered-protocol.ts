/** Internal metadata only. No payload or queue TTL is delivery acknowledgement. */
export type IngestAcceptanceMode = "canonical" | "buffered";
export interface BufferedStageSpec {
  key: string;
  sha256: string;
  bytes: number;
  /** Each stored g maps to original claim record ordinals, never scrubbed ids. */
  groupOrdinals: number[][];
}
export interface BufferedStageIntent {
  stageToken: string;
  expiresAt: string;
}
export interface BufferedClaimRecord {
  ord: number;
  source: string;
  session_id: string | null;
  record_id: string;
  revision_id: string;
  revision: number;
  digest: string;
}
/** Exact internal JSON contract shared by040510 and Root's GC runtime. */
export interface BufferedGcTicket {
  stageToken: string;
  claimToken: string;
  leaseToken: string;
  receiptState: "preparing" | "acknowledged";
  key: string;
  sha256: string;
  bytes: number;
  groupOrdinals: number[][];
  keepGroupIndexes: number[];
  resetEpoch: string;
  rowVersion: string;
  phase: "plan" | "cleanup";
  cleanupState: "live" | "retired" | "redacted";
  replacement?: BufferedStageSpec & { stageToken: string };
}
