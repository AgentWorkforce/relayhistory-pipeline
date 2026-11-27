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
  /** The directory's parent sessions (the session that spawned this one, as a subagent or delegate). */
  parents: string[];
}

/** Directory facts the snapshot does not carry. All optional. */
export interface SessionContext {
  repo?: string | null;
  branch?: string | null;
  taskRef?: string | null;
  userRole?: string | null;
  /** The directory row's parent_session_ids. */
  parentSessionIds?: string[];
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
  /**
   * `defaultAt`: when the commit became reachable from the default branch; `defaultAtBasis` says how that is known
   * (`merge` and `patch` from the landing commit, `committer` only the commit's own time: see backfill/forge.ts).
   */
  commits: Array<{
    sha: string; at: string; onDefaultBranch: boolean; defaultAt?: string | null; defaultAtBasis?: "merge" | "patch" | "committer"; revertedBy?: string | null; revertedAt?: string | null;
    /**
     * Its whole effective history on the default branch when there is more than one landing or any revert: each
     * landing of its patch and each revert of a copy, dated by when that commit reached the default branch, oldest
     * first. `defaultAt` and `revertedAt` are the first landing and first revert.
     */
    transitions?: Array<{ at: string; kind: "landed" | "reverted"; by: string }>;
  }>;
  flags: Array<{ name: string; defaultOn: boolean }>;
  /** Branches of the repository and whether their work reached the default branch (by ancestry or patch id). */
  branches?: Array<{
    name: string; landed: boolean; landedAt: string | null; tipAt: string; flagsAdded?: string[];
    /** The branch's own commits (not on the default branch by ancestry), newest first, at most 200. */
    commits?: string[];
  }>;
}

export const EMPTY_FORGE: ForgeFacts = { prs: [], commits: [], flags: [] };

export interface FeatureLink {
  a: SessionKey;
  b: SessionKey;
  signal: "task" | "pr" | "branch" | "commit" | "family" | "files" | "intent";
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

const INSTANT = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})(?::(\d{2})(?:\.(\d{1,9}))?)?(Z|[+-](\d{2}):(\d{2}))$/u;

/**
 * One instant as canonical UTC (`YYYY-MM-DDTHH:MM:SS.sssZ`, a finer fraction floored to the millisecond; see
 * `utcCut` for why that is exact at every cut), so that text order is time order everywhere: git
 * writes offsets (`+00:00`, `-05:00`), analyses write `Z`. Only a zoned ISO 8601 date-time whose calendar fields
 * are real (no 2026-02-30, no hour 24, offsets within ±14:00) is an instant; anything else is null, never rolled
 * over to a neighbouring day.
 */
export function utc(t: string | null | undefined): string | null {
  if (typeof t !== "string") return null;
  const m = INSTANT.exec(t);
  if (!m) return null;
  const [y, mo, d, h, mi, s = "0"] = m.slice(1, 7).map((x) => Number(x ?? 0));
  const day = new Date(Date.UTC(y!, mo! - 1, d!));
  if (day.getUTCFullYear() !== y || day.getUTCMonth() !== mo! - 1 || day.getUTCDate() !== d) return null;
  if (h! > 23 || mi! > 59 || Number(s) > 59) return null;
  if (m[8] !== "Z" && (Number(m[9]) > 14 || Number(m[10]) > 59 || (Number(m[9]) === 14 && Number(m[10]) > 0))) return null;
  const ms = Date.parse(floored(m));
  return Number.isFinite(ms) ? new Date(ms).toISOString() : null;
}

/** The instant's text with its fraction cut to milliseconds: flooring, by construction, on every engine. */
function floored(m: RegExpExecArray): string {
  const frac = (m[7] ?? "").slice(0, 3).padEnd(3, "0");
  return `${m[1]}-${m[2]}-${m[3]}T${m[4]}:${m[5]}:${m[6] ?? "00"}.${frac}${m[8]}`;
}

/**
 * A cut (an as-of instant, a gold or run cut): canonical like `utc`, but only at the precision `utc` keeps. Evidence
 * finer than a millisecond (PostgreSQL writes microseconds) is floored, and with a millisecond cut that is exact
 * for every comparison relayfacts makes: `floor(t) < c` exactly when `t < c`, and `floor(t) >= c` exactly when
 * `t >= c`. A finer cut would not be, and two cuts within one millisecond would compare equal, so it is refused.
 */
export function utcCut(t: string | null | undefined): string | null {
  if (typeof t !== "string") return null;
  const m = INSTANT.exec(t);
  if (!m || /[1-9]/u.test((m[7] ?? "").slice(3))) return null;
  return utc(t);
}

/** The cut, or a refusal naming only its role: every as-of entry point takes its cut through this, never `requireUtc`. */
export function requireCut(t: string | null | undefined, role: string): string {
  const v = utcCut(t);
  if (v === null) throw new TypeError(`${role} is not a valid zoned ISO 8601 instant at millisecond precision`);
  return v;
}

/** The instant, or a refusal naming only the role of the value (never the value). */
export function requireUtc(t: string | null | undefined, role: string): string {
  const v = utc(t);
  if (v === null) throw new TypeError(`${role} is not a valid zoned ISO 8601 instant`);
  return v;
}

const normalized = new WeakMap<ForgeFacts, ForgeFacts>();
/** The forge facts with every time canonical (cached per object). */
export function utcForge(forge: ForgeFacts): ForgeFacts {
  const hit = normalized.get(forge);
  if (hit) return hit;
  const req = (t: string) => requireUtc(t, "a forge time");
  const opt = (t: string | null | undefined) => (t === null || t === undefined ? null : requireUtc(t, "a forge time"));
  const out: ForgeFacts = {
    ...forge,
    prs: forge.prs.map((p) => ({ ...p, at: req(p.at), ...(p.mergedAt !== undefined ? { mergedAt: opt(p.mergedAt) } : {}), ...(p.closedAt !== undefined ? { closedAt: opt(p.closedAt) } : {}) })),
    commits: forge.commits.map((c) => ({ ...c, at: req(c.at), ...(c.transitions ? { transitions: c.transitions.map((t) => ({ ...t, at: req(t.at) })) } : {}), ...(c.defaultAt !== undefined ? { defaultAt: opt(c.defaultAt) } : {}), ...(c.revertedAt !== undefined ? { revertedAt: opt(c.revertedAt) } : {}) })),
    ...(forge.branches ? { branches: forge.branches.map((b) => ({ ...b, tipAt: req(b.tipAt), landedAt: opt(b.landedAt) })) } : {}),
  };
  normalized.set(forge, out);
  normalized.set(out, out);
  return out;
}
