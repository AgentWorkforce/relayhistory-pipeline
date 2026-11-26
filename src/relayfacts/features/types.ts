/**
 * Relayfacts feature records (docs/proposals/relayfacts-features.md): the types shared by the pure
 * stages (extract, link, state, build, render) and the benchmark. No I/O, clock or model.
 */

export type FeatureState =
  | "planned"
  | "in_progress"
  | "shipped"
  | "shipped_off"
  | "partial"
  | "abandoned"
  | "superseded"
  | "regressed";
export const FEATURE_STATES: readonly FeatureState[] = [
  "planned", "in_progress", "shipped", "shipped_off", "partial", "abandoned", "superseded", "regressed",
];

/** One pointer into a session analysis's evidence references. */
export interface EvidencePointer {
  source: string;
  sessionId: string;
  refId: string;
  kind: string | null;
  at: string | null;
}

export interface SessionKey {
  source: string;
  sessionId: string;
}

/** A bounded, scrub-clean excerpt of what a person wrote, from a human-message reference. */
export interface Quote {
  text: string;
  at: string | null;
  evidence: EvidencePointer;
}

/** What the human asked for: one outcome intent of one session. */
export interface AskedItem {
  id: string;
  text: string;
  quotes: Quote[];
  at: string | null;
  session: SessionKey;
  outcomeId: string | null;
  evidence: EvidencePointer[];
}

export type BuiltKind = "claim" | "commit" | "pr" | "flag" | "migration";

/** What was done. `claim` comes from the analysis; every other kind is read from tool evidence. */
export interface BuiltItem {
  id: string;
  kind: BuiltKind;
  what: string;
  /** A commit sha, a PR URL, a flag name or a migration file name; null for a claim. */
  ref: string | null;
  at: string | null;
  session: SessionKey;
  outcomeId: string | null;
  evidence: EvidencePointer[];
}

export type GapKind = "unmet" | "diverged" | "unverified" | "open";

export interface GapItem {
  id: string;
  kind: GapKind;
  text: string;
  askedId: string | null;
  at: string | null;
  session: SessionKey;
  evidence: EvidencePointer[];
}

export interface RationaleItem {
  id: string;
  kind: "decision" | "constraint";
  text: string;
  at: string | null;
  session: SessionKey;
  evidence: EvidencePointer[];
}

/** What links a session to others: every value is read from the directory row or from tool evidence. */
export interface LinkKeys {
  repo: string | null;
  /** Feature branches only: the default branch names never link. */
  branches: string[];
  prs: string[];
  commits: string[];
  files: string[];
  /** A delegated task id or relay task brief reference, when the directory or evidence names one. */
  taskRefs: string[];
}

/** Directory facts the snapshot does not carry. All optional. */
export interface SessionContext {
  repo?: string | null;
  branch?: string | null;
  taskRef?: string | null;
  userRole?: string | null;
}

export interface SessionFacts {
  source: string;
  sessionId: string;
  analyzedAt: string;
  /** Earliest timed reference, else analyzedAt. */
  startedAt: string;
  /** Latest timed reference, else analyzedAt. */
  endedAt: string;
  title: string;
  asked: AskedItem[];
  built: BuiltItem[];
  gaps: GapItem[];
  rationale: RationaleItem[];
  outcomeStatuses: string[];
  keys: LinkKeys;
  /** Counts of what extraction dropped, by reason. */
  dropped: Record<string, number>;
}

/** Repository facts, independent of any session (the forge and the code). */
export interface ForgeFacts {
  defaultBranch?: string;
  prs: Array<{ ref: string; state: "open" | "merged" | "closed"; at: string; mergedAt?: string | null; closedAt?: string | null; branch?: string | null }>;
  commits: Array<{ sha: string; at: string; onDefaultBranch: boolean; revertedBy?: string | null; revertedAt?: string | null }>;
  flags: Array<{ name: string; defaultOn: boolean }>;
  /** Branches of the repository and whether their work reached the default branch (by ancestry or patch id). */
  branches?: Array<{ name: string; landed: boolean; landedAt: string | null; tipAt: string; flagsAdded?: string[] }>;
}

export const EMPTY_FORGE: ForgeFacts = { prs: [], commits: [], flags: [] };

export interface FeatureLink {
  a: SessionKey;
  b: SessionKey;
  signal: "task" | "pr" | "branch" | "commit" | "files";
  value: string;
}

export interface FeatureRecord {
  id: string;
  title: string;
  repos: string[];
  state: FeatureState;
  stateBasis: "forge" | "analysis";
  stateReason: string;
  asked: AskedItem[];
  built: BuiltItem[];
  gaps: GapItem[];
  rationale: RationaleItem[];
  sessions: Array<SessionKey & { role: "founding" | "contributing"; startedAt: string }>;
  links: FeatureLink[];
  firstAt: string;
  updatedAt: string;
}

export const sessionKeyString = (k: SessionKey) => `${k.source}\u0000${k.sessionId}`;
export const textCompare = (a: string, b: string) => (a < b ? -1 : a > b ? 1 : 0);
