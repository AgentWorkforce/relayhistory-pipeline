/** Offline exploration contracts. Observations are not alerts, labels or confirmed workstreams. */
import type { Snapshot, SnapshotSession } from "../replay/snapshot.ts";

export const PATTERN_FORMAT = "session-intel-patterns/1";
export const ANALYSIS_VERSION = "1";
export const ANALYSIS_BOUNDS = { inputs: 16, sessions: 100_000, facts: 500_000, pairs: 10_000, evidencePairs: 3, groups: 50, members: 20 } as const;
export interface Corpus {
  inputs: Array<{ manifestSha256: string; sessions: number; coverage: "not_declared" | "declared_verified" | "declared_gap" }>;
  sessions: SnapshotSession[];
  window: Snapshot["manifest"]["window"];
  deleteBy: string;
  identity: "single_snapshot" | "operator_asserted_shared_namespace";
  duplicateSessions: number;
}
export interface AnalysisOptions { nearbyMs?: number; maxComparisons?: number; maxCandidates?: number }
export interface SessionRef { harness: string; sessionId: string }
export interface EvidenceRef { session: SessionRef; eventId: string; path: string; at: string }
export interface EvidencePair { a: EvidenceRef; b: EvidenceRef }
export interface PatternCandidate {
  id: string;
  kind: "nearby_shared_file_activity";
  review: "unreviewed";
  scope: { kind: "repo" | "project"; value: string };
  participants: [SessionRef, SessionRef];
  sharedFiles: number;
  eventPairs: number;
  firstObservedAt: string;
  lastObservedAt: string;
  closestGapMs: number;
  sessionSpanOverlapMs: number;
  parentLink: "observed" | "not_observed";
  branchRelation: "same" | "different" | "unknown" | "mixed";
  evidence: EvidencePair[];
}
export interface ProfileRow {
  key: string; sessions: number; sessionsWithWrites: number; sessionsWithCommits: number;
  sessionsWithOutcomes: number; sessionsWithClaims: number; sessionsWithSummaries: number;
  writes: number; commits: number; shaShapedCommits: number; outcomes: number; claims: number;
}
export interface PatternReport {
  format: typeof PATTERN_FORMAT;
  definition: { version: string; hash: string; nearbyMs: number; maxComparisons: number; maxCandidates: number };
  input: Omit<Corpus, "sessions"> & { sessions: number };
  coverage: {
    workspaceCompleteness: "not_established";
    quality: "not_evaluated";
    factsOutsideWindow: number; duplicateWrites: number; sessionsWithoutScope: number;
    comparisons: number; scanComplete: boolean; reasons: string[];
    candidatePairsRetained: number; candidatesOmittedFromReport: number;
  };
  profile: {
    all: ProfileRow; byHarness: ProfileRow[]; byRepository: ProfileRow[];
    peakSessionSpans: number; zeroLengthSpans: number;
    days: Array<{ day: string; writes: number; sessionsWithWrites: number }>;
    files: Array<{ scope: PatternCandidate["scope"]; path: string; writes: number; sessions: number; harnesses: number }>;
  };
  candidates: PatternCandidate[];
  activityGroups: Array<{ id: string; sessions: number; candidateEdges: number; members: SessionRef[]; membersOmitted: number }>;
  limitations: string[];
}
