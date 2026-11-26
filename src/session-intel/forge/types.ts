/**
 * Forge outcome records (docs/session-intel-forge-outcomes.md). These are the raw material for ground truth:
 * what happened on the forge, never what a detector said. Identities are commit authors and PR logins as the
 * forge records them. They name machines and bots, not people; people come from the session directory later.
 */

export interface FileChange { status: "A" | "M" | "D" | "R" | "C" | "T"; path: string }

export interface Commit {
  sha: string;
  author: string;
  authoredAt: string;
  parents: string[];
  subject: string;
  body: string;
  files: FileChange[];
}

/**
 * A PR's title and description as they stood over time (forge.history): each entry holds from `at` until the
 * next. The first entry is the PR's creation. Rebuilt from title-rename events and the description's edit
 * history, so an as-of reader can show the text a live system would have seen.
 */
export interface PrHistory { number: number; titles: Array<{ at: string; title: string }>; bodies: Array<{ at: string; body: string }> }

/** The entry in effect at `at`, or null before the first. */
export function asOf<T extends { at: string }>(entries: T[] | undefined, at: string): T | null {
  let cur: T | null = null;
  for (const e of entries ?? []) { if (e.at <= at) cur = e; else break; }
  return cur;
}

export interface PullRequest {
  number: number;
  author: string;
  title: string;
  body: string;
  state: "OPEN" | "CLOSED" | "MERGED";
  headRef: string;
  createdAt: string;
  closedAt: string | null;
  mergedAt: string | null;
  /** Changed files when known; `null` means not fetched, never "no files". */
  files: string[] | null;
  comments: Array<{ author: string; at: string; body: string }>;
}

export type CandidateSignal =
  | "migration_number_collision"
  | "cross_identity_revert"
  | "superseded_pr"
  | "collision_note";

export interface CandidateSide {
  identity: string;
  ref: string;
  at: string;
  title: string;
}

/** A forge event that may be an overlap incident. Adjudication decides; nothing here is a label. */
export interface Candidate {
  id: string;
  repo: string;
  signal: CandidateSignal;
  /** Two or more sides; `crossIdentity` is true when at least two distinct identities appear. */
  sides: CandidateSide[];
  crossIdentity: boolean;
  /** When the cost was paid (the second write, the revert, the close, the note). */
  harmAt: string;
  artifacts: string[];
  /** Short machine-readable reason, e.g. a matched phrase or a migration key. */
  detail: string;
}
