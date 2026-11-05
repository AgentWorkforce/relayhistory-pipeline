/**
 * Durable canonical feature journal contract (040610 migration).
 *
 * The legacy copier's ReplacingMergeTree `version` derives from the Neon
 * feature clock (featureVersion(rev) = 2*rev+1). 040600 publishes
 * generation-qualified images but has NO global canonical-change clock —
 * The owner-only 620 scope-install transaction allocates and appends journal
 * revisions. Producers receive a read-only ordered stream and cannot write or
 * advance that authority.
 */

/** Durable publication + retirement journal — the only valid input. */
export interface CanonicalJournalPort {
  /**
   * Committed entries in order: publication or retirement, CAS-fenced.
   * hasMore distinguishes a bounded page from a complete enumeration —
   * a returned limit is never implicit completeness.
   */
  entries(after: string, limit: number): Promise<{
    entries: CanonicalJournalEntry[];
    nextRevision: string;
    hasMore: boolean;
  }>;
}
export type CanonicalJournalEntry =
  | { kind: "publish"; revision: string; imageKey: string;
      schemaGeneration: string; sourceHash: string; scope: CanonicalScope;
      sourceAuthority: CanonicalSourceAuthority; pointerSet: CanonicalPointerSet }
  | { kind: "retire"; revision: string; imageKey: string;
      schemaGeneration: string; priorKeysetHash: string; scope: CanonicalScope;
      priorSourceAuthority: CanonicalSourceAuthority; pointerSet: CanonicalPointerSet };

/** One logical scope the journal is always scoped to (native contract). */
export interface CanonicalScope {
  orgId: string; workspaceId: string; harness: string; sessionId: string;
}
/** Replay source authority: publication epoch + snapshot fingerprint. */
export interface CanonicalSourceAuthority {
  epoch: string; fingerprint: string;
}
/** Exact captured inventory identity — hash and count, never payload bytes. */
export interface CanonicalPointerSet { hash: string; count: number; }

/** Per-page acknowledgment — idempotent restart-safe progress. */
export interface CanonicalPageAckPort {
  ackPage(input: {
    entry: CanonicalJournalEntry; kind: "event" | "brief";
    cursor: string; pageSha256: string;
  }): Promise<void>;
  /** finish CAS: both pool terminal descriptors + all page ACKs + current
   *  source authority + unchanged target/profile bindings. */
  finish(input: {
    entry: CanonicalJournalEntry;
    terminalEvent: string; terminalBrief: string;
    sourceAuthority: CanonicalSourceAuthority;
    profileBinding: string;
  }): Promise<void>;
}
