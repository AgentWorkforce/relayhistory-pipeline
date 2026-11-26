/**
 * Stage 2 (docs/proposals/relayfacts-features.md, "Derivation"): group sessions into features.
 *
 * Deterministic union-find over evidence keys, strongest first: a shared task reference, a shared pull
 * request, a shared feature branch in the same repository, a shared commit, and (only when enabled) a
 * strong file overlap in the same repository within a time window. Every merge records the signal and
 * value that caused it, so a record can say why two sessions are one feature and the benchmark can
 * measure which signal over-merges.
 *
 * Delegation links through a family: a parent session and the sessions it spawned share the family key, and
 * like every key it links only while the family is small (at most `maxKeyFanout` members). A lead session
 * coordinates many features through dozens of subagents and never links that way; a parent with a handful of
 * reviewers or workers is one piece of work (dev split of the first corpus: every sibling pair in a family of
 * at most 12 shared a gold feature, against 223 of 1,281 pairs in the two families over 30).
 */
import type { FeatureLink, SessionFacts, SessionKey } from "./types.ts";
import { sessionKeyString, textCompare } from "./types.ts";

export interface LinkParams {
  /** Link on file overlap at all. */
  files: boolean;
  /** Minimum shared repo-relative files. */
  filesMinShared: number;
  /** Minimum Jaccard of the two file sets. */
  filesMinJaccard: number;
  /** Maximum gap between the two sessions' activity. */
  filesWindowMs: number;
  /** A key shared by more sessions than this is too common to link on (a hot branch, a shared file). */
  maxKeyFanout: number;
  /**
   * A session naming more feature branches than this coordinates work (a lander, a lead, a release): its branch
   * keys would bridge unrelated features, so none of them links.
   */
  maxSessionBranches: number;
  /** The same for commits: a session naming more commits than this (a release, a lander's queue) links on none. */
  maxSessionCommits: number;
  /** The same for pull requests (a `gh pr list` names dozens). */
  maxSessionPrs: number;
}

export const DEFAULT_LINK_PARAMS: LinkParams = {
  files: true,
  filesMinShared: 3,
  filesMinJaccard: 0.5,
  filesWindowMs: 72 * 3_600_000,
  maxKeyFanout: 12,
  maxSessionBranches: 4,
  maxSessionCommits: 4,
  maxSessionPrs: 4,
};

export interface LinkResult {
  /** Each group's sessions, oldest first; groups ordered by their first session. */
  groups: SessionFacts[][];
  links: FeatureLink[];
}

class UnionFind {
  private parent = new Map<string, string>();
  find(x: string): string {
    let p = this.parent.get(x) ?? x;
    if (p !== x) { p = this.find(p); this.parent.set(x, p); }
    return p;
  }
  union(a: string, b: string): boolean {
    const ra = this.find(a), rb = this.find(b);
    if (ra === rb) return false;
    // Deterministic: the smaller key is the root.
    if (textCompare(ra, rb) < 0) this.parent.set(rb, ra); else this.parent.set(ra, rb);
    return true;
  }
}

const keyOf = (f: SessionFacts): SessionKey => ({ source: f.source, sessionId: f.sessionId });
const byStart = (a: SessionFacts, b: SessionFacts) => textCompare(a.startedAt, b.startedAt) || textCompare(sessionKeyString(keyOf(a)), sessionKeyString(keyOf(b)));

/**
 * The link key of a branch name: review rounds of one change (`-r2`, `-r3`, `-v2`) share it. A name cut off
 * mid-word in a transcript (ending in `-`, `_` or `/`) is not a branch and never links.
 */
export function branchStem(name: string): string | null {
  if (/[-_/]$/u.test(name)) return null;
  return name.replace(/-(?:r|v|round)\d+$/u, "");
}

export function linkSessions(facts: readonly SessionFacts[], params: LinkParams = DEFAULT_LINK_PARAMS): LinkResult {
  const sorted = [...facts].sort(byStart);
  const uf = new UnionFind();
  const links: FeatureLink[] = [];
  const index = (signal: FeatureLink["signal"], keysOf: (f: SessionFacts) => string[]) => {
    const by = new Map<string, SessionFacts[]>();
    for (const f of sorted) for (const k of keysOf(f)) {
      const list = by.get(k) ?? [];
      list.push(f);
      by.set(k, list);
    }
    for (const [value, list] of [...by].sort((a, b) => textCompare(a[0], b[0]))) {
      if (list.length < 2 || list.length > params.maxKeyFanout) continue;
      for (let i = 1; i < list.length; i++) {
        const a = list[0]!, b = list[i]!;
        if (uf.union(sessionKeyString(keyOf(a)), sessionKeyString(keyOf(b))))
          links.push({ a: keyOf(a), b: keyOf(b), signal, value: value.slice(value.indexOf("\u0001") + 1) });
      }
    }
  };
  const scoped = (f: SessionFacts, values: string[]) => values.map((v) => `${f.keys.repo ?? ""}\u0001${v}`);
  const branchKeys = (f: SessionFacts) => [...new Set(f.keys.branches.map(branchStem).filter((b): b is string => b !== null))];
  index("task", (f) => f.keys.taskRefs);
  index("pr", (f) => (f.keys.prs.length <= params.maxSessionPrs ? f.keys.prs : []));
  index("branch", (f) => (f.keys.repo && f.keys.branches.length <= params.maxSessionBranches ? scoped(f, branchKeys(f)) : []));
  // Commit ids are written at different lengths (push output, briefs, logs): link on the 7-character prefix.
  index("commit", (f) => {
    const short = [...new Set(f.keys.commits.map((c) => c.slice(0, 7)))];
    return short.length <= params.maxSessionCommits ? short : [];
  });
  // A session's own id is its family key as a parent; its parents' ids are its family keys as a child.
  index("family", (f) => [f.sessionId, ...f.keys.parents].map((p) => `${f.source}\u0001${p}`));

  if (params.files) {
    const fileFanout = new Map<string, number>();
    for (const f of sorted) if (f.keys.repo) for (const p of f.keys.files) {
      const k = `${f.keys.repo}\u0001${p}`;
      fileFanout.set(k, (fileFanout.get(k) ?? 0) + 1);
    }
    const rare = (f: SessionFacts) => new Set(f.keys.files.filter((p) => (fileFanout.get(`${f.keys.repo}\u0001${p}`) ?? 0) <= params.maxKeyFanout));
    const sets = new Map(sorted.map((f) => [sessionKeyString(keyOf(f)), rare(f)]));
    for (let i = 0; i < sorted.length; i++) for (let j = i + 1; j < sorted.length; j++) {
      const a = sorted[i]!, b = sorted[j]!;
      if (!a.keys.repo || a.keys.repo !== b.keys.repo) continue;
      const gap = Date.parse(b.startedAt) - Date.parse(a.endedAt);
      if (gap > params.filesWindowMs) continue;
      const sa = sets.get(sessionKeyString(keyOf(a)))!, sb = sets.get(sessionKeyString(keyOf(b)))!;
      let shared = 0;
      for (const p of sa) if (sb.has(p)) shared++;
      const union = sa.size + sb.size - shared;
      if (shared < params.filesMinShared || !union || shared / union < params.filesMinJaccard) continue;
      if (uf.union(sessionKeyString(keyOf(a)), sessionKeyString(keyOf(b))))
        links.push({ a: keyOf(a), b: keyOf(b), signal: "files", value: `${shared} shared files` });
    }
  }

  const groups = new Map<string, SessionFacts[]>();
  for (const f of sorted) {
    const root = uf.find(sessionKeyString(keyOf(f)));
    const list = groups.get(root) ?? [];
    list.push(f);
    groups.set(root, list);
  }
  return { groups: [...groups.values()].sort((a, b) => byStart(a[0]!, b[0]!)), links };
}
