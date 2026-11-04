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
/** Root's GC RPC maps attemptToken to stage_token, distinct from claimToken. */
export interface BufferedGcTicket {
  attemptToken: string;
  claimToken: string;
  leaseToken: string;
  state: "preparing" | "acknowledged";
  oldKey: string;
  sha256: string;
  bytes: number;
  groupOrdinals: number[][];
  keepGroupIndexes: number[];
  resetEpoch: string;
  currentVersion: string;
  cleanupState: "live" | "retired" | "redacted";
}

