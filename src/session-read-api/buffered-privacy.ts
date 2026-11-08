/**
 * PR B implementation seam for buffered reads.  The executable contract lives in
 * `__tests__/buffered-privacy-fixtures.test.ts`; the registry implementation replaces
 * the two fail-closed stubs and wires `BufferedReads` into createReadApi.
 *
 * Keeping the input as data (instead of teaching the tests implementation details)
 * lets the same 26 causal vectors cover list, turns and metadata, and lets the 16 G2
 * vectors cover the owner-written parity proof independently.
 */

export interface BufferedScope {
  orgId: string;
  workspaceId: string;
  userId: string;
}

export interface BufferedAdmissionWitness {
  controlEpoch: string | null;
  sourceControlVersion: string | null;
}

/** Owner-written G2 row plus the target-side checks evaluated by 043700. */
export interface BufferedParityWitness {
  contract: "owner-buffered-parity/v1";
  tenant: BufferedScope;
  controlEpoch: string;
  privacyBaselineSha256: string;
  sourceControlVersion: string;
  sourceControlDigest: string | null;
  sourceControlKind: "selection" | "reset" | null;
  explicitNoHead: boolean;
  targetGeneration: string;
  deletionSetSha256: string;
  resetSetSha256: string;
  targetLineageDigest: string;
}

export interface BufferedControlHead {
  tenant: BufferedScope;
  controlEpoch: string;
  privacyBaselineSha256: string;
  sourceControlVersion: string;
  sourceControlDigest: string | null;
  sourceControlKind: "selection" | "reset" | null;
  pending: boolean;
}

export type BufferedControlHeadResult =
  | { ok: true; value: unknown }
  | { ok: false; reason: "timeout" | "non2xx" | "unbound_service" | "invalid_response" };

export interface BufferedGateInput {
  scope: BufferedScope;
  /** The immutable witness returned by 043700 for the original claim. */
  admission: BufferedAdmissionWitness;
  /** False when any pre-S3 custody/ownership/digest/revision predicate failed. */
  targetEligible: boolean;
  /** Exact current G2 proof returned by 043700; stale/missing proof is null. */
  parity: BufferedParityWitness | null;
  /** Read from the named service binding after every S3 object finished. */
  controlHead: BufferedControlHeadResult;
}

/**
 * Fail-closed placeholder.  PR B implements the v5 exact-shape parser and gate here.
 * It must compare decimal versions as BigInt (never Number).
 */
export function bufferedVisibleAfterFetch(_input: BufferedGateInput): boolean {
  throw new Error("PR B buffered privacy gate is not implemented");
}

/** Materialized state used by the release-owner's 16 G2 parity vectors. */
export interface BufferedParityState {
  [key: string]: unknown;
}

/** Fail-closed placeholder for the G2 proof/current-target predicate. */
export function bufferedParityProofCurrent(_state: BufferedParityState): boolean {
  throw new Error("PR B buffered parity proof predicate is not implemented");
}

export interface BufferedSessionCandidate {
  source: string;
  sessionId: string;
  firstTs: string;
  lastTs: string;
  stageCount: number;
  bytes: string;
  userId: string;
}

export interface BufferedStageCandidate extends BufferedAdmissionWitness {
  batchId: string;
  key: string;
  sha256: string;
  bytes: number;
  groupIndexes: number[];
  keptOrdinals: number[];
  parity: BufferedParityWitness | null;
  targetEligible: boolean;
}

export interface BufferedStageBody {
  /** Parsed, scrubbed records from the selected groups/ordinals only. */
  rows: Record<string, unknown>[];
}

/**
 * Dependency boundary used by the contract harness.  `readStage` is the complete,
 * integrity-checked S3 read.  `controlHead` must be called after it and before a
 * buffered row is included in any HTTP response.
 */
export interface BufferedReads {
  list(scope: BufferedScope, signal: AbortSignal): Promise<BufferedSessionCandidate[]>;
  stages(scope: BufferedScope & { source: string; sessionId: string }, signal: AbortSignal): Promise<BufferedStageCandidate[]>;
  readStage(stage: BufferedStageCandidate, signal: AbortSignal): Promise<BufferedStageBody>;
  controlHead(scope: BufferedScope, signal: AbortSignal): Promise<BufferedControlHeadResult>;
  deadlineMs?: number;
}
