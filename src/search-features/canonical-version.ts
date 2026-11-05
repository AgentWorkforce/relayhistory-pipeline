/**
 * Durable canonical feature version/outbox contract (040610 proposal).
 *
 * The legacy copier's ReplacingMergeTree `version` derives from the Neon
 * feature clock (featureVersion(rev) = 2*rev+1). 040600 publishes
 * generation-qualified images but has NO global canonical-change clock —
 * so this port is the managed counter the pools need, initialized ABOVE the
 * last legacy clock value before the copier is stopped/drained.
 *
 * Ownership: this file declares the CONTRACT only. The 040610 migration
 * proposal (below, in the docstring) owns a metadata-only journal/outbox
 * table covering publication + retirement + source CAS. Do not reserve or
 * push the migration until Root/Native coordinate — DDL is held.
 */

/** A durable managed counter: strictly increasing, shared with legacy space. */
export interface CanonicalVersionPort {
  /** Last allocated revision (decimal string), or null when uninitialized. */
  last(): Promise<string | null>;
  /**
   * Seed the counter above the last legacy feature clock (drain-time only).
   * Refuses to seed at or below a recorded last value — the counter never
   * moves backwards and never overlaps a legacy-issued revision.
   */
  seed(aboveLegacyClock: string): Promise<void>;
  /** Allocate the next revision for one publication/retirement event. */
  next(): Promise<string>;
}

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
      sourceAuthority: CanonicalSourceAuthority; pointerSet: CanonicalPointerSet;
      /** Replacement only: prior authority + prior SET hash (retire context). */
      priorSourceAuthority?: CanonicalSourceAuthority;
      priorKeysetHash?: string }
  | { kind: "retire"; revision: string; imageKey: string;
      schemaGeneration: string; sourceHash: string; priorKeysetHash: string;
      scope: CanonicalScope;
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
export interface CanonicalPointerSet {
  hash: string; count: number; pointers: unknown[]; }

/** The 610 transaction port: OWNER-ONLY INVOKER inside the 620 final CAS. */
export interface CanonicalFeatureChangedPort {
  canonicalFeatureChanged(input: {
    scope: CanonicalScope;
    source: CanonicalSourceAuthority;     // priorSource null = new scope
    priorSource: CanonicalSourceAuthority | null;
    pointerSet: CanonicalPointerSet;
  }): Promise<string>; // returns the allocated managed revision
}

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

/**
 * 040610 proposal (metadata-only, tentative — DO NOT reserve/push yet):
 *
 *   CREATE TABLE sessions.canonical_feature_journal (
 *     revision bigint PRIMARY KEY,            -- managed counter, seed > legacy
 *     kind text NOT NULL CHECK (kind IN ('publish','retire')),
 *     schema_generation text NOT NULL,        -- same-generation fence
 *     image_key text NOT NULL,                -- publication image identity
 *     source_hash char(64) NOT NULL,          -- image content CAS
 *     prior_keyset_hash char(64),             -- retire: exact prior keyset
 *     recorded_at timestamptz NOT NULL DEFAULT now()
 *   );
 *
 *   - revision allocated under a row lock on a single-counter table or an
 *     advisory xact lock; seed() writes the legacy high-water once.
 *   - The journal is the ONLY source the producer trusts for publication +
 *     retirement ordering; S3 events/Neon tombstones are ignored by contract.
 *   - version column in CH pools = featureVersion(revision) — same space, so
 *     seed() MUST set last = legacy_clock_last (never below).
 */
export const __canon040610 = "proposal-only/no-reservation";
