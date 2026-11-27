/**
 * Overlap detectors for the benchmark (docs/session-intel-overlap-bench.md, "Detectors"). Each one is a pure
 * function of the model, the forge state and its params, and emits alerts no earlier than its evidence. A side
 * is a work root (`root:<session key>`) or a pull request (`pr:<owner/repo>#<n>`).
 */
import type { PrHistory, PullRequest } from "../forge/types.ts";
import type { Session } from "./model.ts";
import { elapsedNs, NS_PER_MINUTE } from "../cloud-harvest/instant.ts";

export interface Alert { detector: string; sides: [string, string]; at: string; artifact: string; score: number; owners: [string | null, string | null] }
/** Detectors read only the PR's shape; the description and comments are carried for adjudication bundles. */
export interface ForgePr extends Pick<PullRequest, "number" | "author" | "createdAt" | "mergedAt" | "closedAt" | "headRef" | "files" | "title">, Partial<Pick<PullRequest, "body" | "comments">> {
  repo: string;
  /** Dated title and description versions (forge `history`), when fetched; as-of bundles read only these. */
  history?: Pick<PrHistory, "titles" | "bodies">;
}

export interface Context {
  sessions: Map<string, Session>;
  prs: ForgePr[];
  /** True when the two roots exchanged a relay message before `at`. */
  communicated: (a: string, b: string, at: string) => boolean;
}

export const rootSide = (root: string) => `root:${root}`;
export const prSide = (repo: string, n: number) => `pr:${repo.toLowerCase()}#${n}`;
const pairKey = (a: string, b: string) => (a < b ? `${a}\u0001${b}` : `${b}\u0001${a}`);
/** Windows are checked at full precision (instant.ts elapsedNs). */
const H_NS = 60n * NS_PER_MINUTE;

/**
 * Which roots a PR belongs to, and since when: a root that created it (from the create call's time), or created or
 * pushed its head branch (from that command's time). A root that only names it (views, reviews, merges, a merge
 * train listing it) does not own it: one reviewer touching two PRs would otherwise make both look like one line
 * of work. Detectors ask `ownersAt`, so no alert leans on ownership established after it.
 */
export function prOwnership(ctx: Context): Map<string, Map<string, string>> {
  const byBranch = new Map<string, Map<string, string>>();
  const out = new Map<string, Map<string, string>>();
  const note = (m: Map<string, string>, root: string, ts: string) => { const o = m.get(root); if (!o || ts < o) m.set(root, ts); };
  for (const s of ctx.sessions.values()) {
    for (const b of s.branches) {
      const k = `${s.repo?.toLowerCase()}\u0000${b.name}`;
      note(byBranch.get(k) ?? byBranch.set(k, new Map()).get(k)!, s.root, b.ts);
    }
    for (const p of s.prs) if (p.repo && p.number > 0 && p.action === "create") {
      const k = prSide(p.repo, p.number);
      note(out.get(k) ?? out.set(k, new Map()).get(k)!, s.root, p.ts);
    }
  }
  for (const p of ctx.prs) {
    const roots = byBranch.get(`${p.repo.toLowerCase()}\u0000${p.headRef}`);
    if (!roots) continue;
    const k = prSide(p.repo, p.number), m = out.get(k) ?? out.set(k, new Map()).get(k)!;
    for (const [r, ts] of roots) note(m, r, ts);
  }
  return out;
}

/** The roots owning `side` as of `at`. */
export const ownersAt = (own: Map<string, Map<string, string>>, side: string, at: string): string[] =>
  [...(own.get(side) ?? [])].filter(([, ts]) => ts <= at).map(([r]) => r);

/** Every root that ever owned each PR. For scoring and adjudication in hindsight only, never for detectors. */
export function prOwners(ctx: Context): Map<string, Set<string>> {
  return new Map([...prOwnership(ctx)].map(([k, m]) => [k, new Set(m.keys())]));
}

/** Keeps the earliest alert per side pair (a later alert for the same pair adds nothing to lead time). */
function earliest(alerts: Alert[]): Alert[] {
  const by = new Map<string, Alert>();
  for (const a of alerts) { const k = pairKey(...a.sides); const o = by.get(k); if (!o || a.at < o.at) by.set(k, a); }
  return [...by.values()];
}

const rootsOf = (ctx: Context) => {
  const byRoot = new Map<string, Session[]>();
  for (const s of ctx.sessions.values()) byRoot.set(s.root, [...(byRoot.get(s.root) ?? []), s]);
  return byRoot;
};
const ownerOf = (ctx: Context, root: string) => ctx.sessions.get(root)?.owner ?? null;

export interface OpenPrParams { minShared: number; maxPrFiles: number; skipHot: boolean; suppressCommunicated: boolean }

/**
 * D1: a root edits files that another line's open pull request is working on. What a PR is working on at time t
 * is only what is dated by then: the paths its owning roots (owners as of t) had edited before t. The PR's final
 * file list is never used, since it is only known in hindsight; a PR with no owning root gives no evidence. The
 * alert fires at the edit that brings the shared count to `minShared`, while the PR is open, not the root's own,
 * and has at most `maxPrFiles` such paths so far.
 */
export function openPrOverlap(ctx: Context, p: OpenPrParams): Alert[] {
  const own = prOwnership(ctx), roots = rootsOf(ctx);
  const prs = new Map(ctx.prs.map((pr) => [prSide(pr.repo, pr.number), pr]));
  // Per (repo, path): the first time each candidate owning root edited it, with the PRs that root could own.
  const firstEdit = new Map<string, Map<string, string>>();
  for (const [root, members] of roots) for (const s of members) for (const e of s.edits) {
    if (p.skipHot && e.hot) continue;
    const k = `${e.repo.toLowerCase()}\u0000${e.path}`, m = firstEdit.get(k) ?? firstEdit.set(k, new Map()).get(k)!;
    const o = m.get(root); if (!o || e.ts < o) m.set(root, e.ts);
  }
  const prsOfRoot = new Map<string, string[]>();
  for (const [side, m] of own) if (prs.has(side)) for (const r of m.keys()) prsOfRoot.set(r, [...(prsOfRoot.get(r) ?? []), side]);
  // A PR's dated path count as of t: distinct paths its owners (as of t) had edited before t.
  const sizeAt = (side: string, repo: string, t: string) => {
    const paths = new Set<string>();
    for (const r of ownersAt(own, side, t)) for (const s of roots.get(r) ?? []) for (const e of s.edits) {
      if (e.ts < t && e.repo.toLowerCase() === repo && !(p.skipHot && e.hot)) paths.add(e.path);
    }
    return paths.size;
  };
  const alerts: Alert[] = [];
  for (const s of ctx.sessions.values()) {
    const shared = new Map<string, Set<string>>();
    for (const e of s.edits) {
      if (p.skipHot && e.hot) continue;
      const repo = e.repo.toLowerCase();
      for (const [other, first] of firstEdit.get(`${repo}\u0000${e.path}`) ?? []) {
        if (other === s.root || !(first < e.ts)) continue;
        for (const side of prsOfRoot.get(other) ?? []) {
          const pr = prs.get(side)!;
          if (pr.repo.toLowerCase() !== repo) continue;
          const owners = ownersAt(own, side, e.ts);
          if (!owners.includes(other) || owners.includes(s.root)) continue;
          const end = pr.mergedAt ?? pr.closedAt;
          if (!(pr.createdAt <= e.ts && (!end || e.ts < end))) continue;
          const set = shared.get(side) ?? new Set<string>();
          if (set.has(e.path)) continue;
          set.add(e.path); shared.set(side, set);
          if (set.size !== p.minShared || sizeAt(side, repo, e.ts) > p.maxPrFiles) continue;
          if (p.suppressCommunicated && owners.some((o) => ctx.communicated(s.root, o, e.ts))) continue;
          alerts.push({ detector: "open-pr", sides: [rootSide(s.root), side], at: e.ts, artifact: e.path, score: set.size, owners: [s.owner, ownerOf(ctx, owners[0]!)] });
        }
      }
    }
  }
  return earliest(alerts);
}

export interface ConcurrentParams { windowH: number; minShared: number; maxRootsPerPath: number; skipHot: boolean; crossOwnerOnly: boolean; suppressCommunicated: boolean }

/**
 * D3: two roots edit the same uncommon paths in one repository within `windowH` hours of each other. Fires at
 * the edit that brings the pair's shared count to `minShared`. A path edited by more than `maxRootsPerPath`
 * roots so far is treated as hot. The score sums each shared path's rarity (1 / roots that had edited it by
 * then), so two lines meeting on files few others touch outrank a meeting on a common file.
 */
export function concurrentEdits(ctx: Context, p: ConcurrentParams): Alert[] {
  const byPath = new Map<string, Array<{ ts: string; root: string; owner: string | null }>>();
  for (const s of ctx.sessions.values()) {
    for (const e of s.edits) {
      if (p.skipHot && e.hot) continue;
      const k = `${e.repo.toLowerCase()}\u0000${e.path}`;
      byPath.set(k, [...(byPath.get(k) ?? []), { ts: e.ts, root: s.root, owner: s.owner }]);
    }
  }
  const shared = new Map<string, { paths: Set<string>; rarity: number; owners: [string | null, string | null] }>();
  const alerts: Alert[] = [];
  const events: Array<{ ts: string; a: string; b: string; path: string; rarity: number; owners: [string | null, string | null] }> = [];
  for (const [k, rows] of byPath) {
    rows.sort((x, y) => (x.ts < y.ts ? -1 : 1));
    const lastByRoot = new Map<string, { ts: string; owner: string | null }>();
    for (const r of rows) {
      lastByRoot.set(r.root, { ts: r.ts, owner: r.owner });
      if (lastByRoot.size > p.maxRootsPerPath) continue;
      for (const [other, o] of lastByRoot) {
        if (other === r.root || elapsedNs(o.ts, r.ts) > BigInt(p.windowH) * H_NS) continue;
        if (p.crossOwnerOnly && (r.owner === null || r.owner === o.owner)) continue;
        events.push({ ts: r.ts, a: r.root, b: other, path: k.split("\u0000")[1]!, rarity: 1 / lastByRoot.size, owners: [r.owner, o.owner] });
      }
    }
  }
  events.sort((x, y) => (x.ts < y.ts ? -1 : 1));
  for (const ev of events) {
    const key = pairKey(ev.a, ev.b);
    const st = shared.get(key) ?? { paths: new Set<string>(), rarity: 0, owners: ev.owners };
    if (!st.paths.has(ev.path)) st.rarity += ev.rarity;
    st.paths.add(ev.path); shared.set(key, st);
    if (st.paths.size !== p.minShared) continue;
    if (p.suppressCommunicated && ctx.communicated(ev.a, ev.b, ev.ts)) continue;
    alerts.push({ detector: "concurrent", sides: [rootSide(ev.a), rootSide(ev.b)], at: ev.ts, artifact: ev.path, score: st.rarity, owners: st.owners });
  }
  return earliest(alerts);
}

export interface SharedTriggerParams { windowMin: number; requireEdit: boolean; suppressCommunicated: boolean }

/**
 * D2: two roots react to the same pull request they do not own (a red merge train, a failing PR) by opening a PR
 * or editing within `windowMin` of referencing it. Fires when the second root starts acting.
 */
export function sharedTrigger(ctx: Context, p: SharedTriggerParams): Alert[] {
  const own = prOwnership(ctx);
  const actsAfter = new Map<string, Array<{ root: string; at: string; owner: string | null }>>();
  const byRoot = rootsOf(ctx);
  for (const [root, members] of byRoot) {
    const refs = members.flatMap((s) => s.prs.filter((x) => x.repo && x.number > 0 && !x.undated).map((x) => ({ ...x, s })));
    for (const ref of refs) {
      const side = prSide(ref.repo!, ref.number);
      if (ref.action === "create" && ownersAt(own, side, ref.ts).includes(root)) continue;
      const limit = BigInt(p.windowMin) * NS_PER_MINUTE;
      const act = members.flatMap((s) => [
        ...s.prs.filter((x) => x.action === "create").map((x) => x.ts),
        ...(p.requireEdit ? s.edits.map((e) => e.ts) : []),
      ]).filter((t) => t >= ref.ts && elapsedNs(ref.ts, t) <= limit).sort()[0];
      if (!act) continue;
      const list = actsAfter.get(side) ?? [];
      if (!list.some((x) => x.root === root)) list.push({ root, at: act, owner: ref.s.owner });
      actsAfter.set(side, list);
    }
  }
  const alerts: Alert[] = [];
  for (const [side, list] of actsAfter) {
    list.sort((a, b) => (a.at < b.at ? -1 : 1));
    for (let i = 1; i < list.length; i++) for (let j = 0; j < i; j++) {
      const a = list[j]!, b = list[i]!;
      if (a.root === b.root || (p.suppressCommunicated && ctx.communicated(a.root, b.root, b.at))) continue;
      alerts.push({ detector: "shared-trigger", sides: [rootSide(a.root), rootSide(b.root)], at: b.at, artifact: side, score: 1, owners: [a.owner, b.owner] });
    }
  }
  return earliest(alerts);
}
