/**
 * Overlap detectors for the benchmark (docs/session-intel-overlap-bench.md, "Detectors"). Each one is a pure
 * function of the model, the forge state and its params, and emits alerts no earlier than its evidence. A side
 * is a work root (`root:<session key>`) or a pull request (`pr:<owner/repo>#<n>`).
 */
import type { PullRequest } from "../forge/types.ts";
import type { Session } from "./model.ts";

export interface Alert { detector: string; sides: [string, string]; at: string; artifact: string; score: number; owners: [string | null, string | null] }
/** Detectors read only the PR's shape; the description and comments are carried for adjudication bundles. */
export interface ForgePr extends Pick<PullRequest, "number" | "author" | "createdAt" | "mergedAt" | "closedAt" | "headRef" | "files" | "title">, Partial<Pick<PullRequest, "body" | "comments">> { repo: string }

export interface Context {
  sessions: Map<string, Session>;
  prs: ForgePr[];
  /** True when the two roots exchanged a relay message before `at`. */
  communicated: (a: string, b: string, at: string) => boolean;
}

export const rootSide = (root: string) => `root:${root}`;
export const prSide = (repo: string, n: number) => `pr:${repo.toLowerCase()}#${n}`;
const pairKey = (a: string, b: string) => (a < b ? `${a}\u0001${b}` : `${b}\u0001${a}`);
const H = 3_600_000;

/**
 * Which roots a PR belongs to: a root that created it, or created or pushed its head branch. A root that only
 * names it (views, reviews, merges, a merge train listing it) does not own it: one reviewer touching two PRs would
 * otherwise make both look like one line of work.
 */
export function prOwners(ctx: Context): Map<string, Set<string>> {
  const byBranch = new Map<string, Set<string>>();
  const out = new Map<string, Set<string>>();
  for (const s of ctx.sessions.values()) {
    for (const b of s.branches) byBranch.set(`${s.repo?.toLowerCase()}\u0000${b.name}`, (byBranch.get(`${s.repo?.toLowerCase()}\u0000${b.name}`) ?? new Set()).add(s.root));
    for (const p of s.prs) if (p.repo && p.number > 0 && p.action === "create") {
      const k = prSide(p.repo, p.number);
      out.set(k, (out.get(k) ?? new Set()).add(s.root));
    }
  }
  for (const p of ctx.prs) {
    const roots = byBranch.get(`${p.repo.toLowerCase()}\u0000${p.headRef}`);
    if (roots) { const k = prSide(p.repo, p.number); for (const r of roots) out.set(k, (out.get(k) ?? new Set()).add(r)); }
  }
  return out;
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
 * D1: a root edits files that another line's open pull request changes. The alert fires at the edit that brings
 * the shared count to `minShared`, while the PR is open and not the root's own.
 */
export function openPrOverlap(ctx: Context, p: OpenPrParams): Alert[] {
  const owners = prOwners(ctx);
  const byRepoPath = new Map<string, ForgePr[]>();
  for (const pr of ctx.prs) {
    if (!pr.files || pr.files.length > p.maxPrFiles) continue;
    for (const f of pr.files) {
      const k = `${pr.repo.toLowerCase()}\u0000${f}`;
      byRepoPath.set(k, [...(byRepoPath.get(k) ?? []), pr]);
    }
  }
  const alerts: Alert[] = [];
  for (const s of ctx.sessions.values()) {
    const shared = new Map<string, Set<string>>();
    for (const e of s.edits) {
      if (p.skipHot && e.hot) continue;
      for (const pr of byRepoPath.get(`${e.repo.toLowerCase()}\u0000${e.path}`) ?? []) {
        const side = prSide(pr.repo, pr.number);
        if (owners.get(side)?.has(s.root)) continue;
        const end = pr.mergedAt ?? pr.closedAt;
        if (!(pr.createdAt <= e.ts && (!end || e.ts < end))) continue;
        const set = shared.get(side) ?? new Set<string>();
        set.add(e.path); shared.set(side, set);
        if (set.size !== p.minShared) continue;
        const others = [...(owners.get(side) ?? [])];
        if (p.suppressCommunicated && others.some((o) => ctx.communicated(s.root, o, e.ts))) continue;
        alerts.push({ detector: "open-pr", sides: [rootSide(s.root), side], at: e.ts, artifact: e.path, score: set.size, owners: [s.owner, others.length ? ownerOf(ctx, others[0]!) : null] });
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
        if (other === r.root || Date.parse(r.ts) - Date.parse(o.ts) > p.windowH * H) continue;
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
  const owners = prOwners(ctx);
  const actsAfter = new Map<string, Array<{ root: string; at: string; owner: string | null }>>();
  const byRoot = rootsOf(ctx);
  for (const [root, members] of byRoot) {
    const refs = members.flatMap((s) => s.prs.filter((x) => x.repo && x.number > 0).map((x) => ({ ...x, s })));
    for (const ref of refs) {
      const side = prSide(ref.repo!, ref.number);
      if (owners.get(side)?.has(root) && ref.action === "create") continue;
      const limit = Date.parse(ref.ts) + p.windowMin * 60_000;
      const act = members.flatMap((s) => [
        ...s.prs.filter((x) => x.action === "create").map((x) => x.ts),
        ...(p.requireEdit ? s.edits.map((e) => e.ts) : []),
      ]).filter((t) => t >= ref.ts && Date.parse(t) <= limit).sort()[0];
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
