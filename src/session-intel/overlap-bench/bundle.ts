/**
 * Adjudication bundles (docs/session-intel-overlap-bench.md, "Precision"). Each alerted pair is described from
 * derived facts and forge metadata only: which PRs each side created, their titles, files and outcomes, the
 * branches it worked on, the files it edited and when. No transcript text. The detector that raised the pair is
 * withheld, so the verdict cannot lean on it.
 */
import type { Context } from "./detectors.ts";
import { prOwners, prSide } from "./detectors.ts";
import { pairId } from "./eval.ts";

const MAX_FILES = 40;

interface PrInfo {
  pr: string; title: string; author: string; createdAt: string; mergedAt: string | null; closedAt: string | null; files: string[] | null;
  /** As-of bundles only: the description, and comments made before the alert. */
  body?: string; comments?: Array<{ at: string; body: string }>;
}
const MAX_BODY = 1500, MAX_COMMENT = 400, MAX_COMMENTS = 5;
export interface SideInfo {
  side: string;
  kind: "work" | "pr";
  owner: string | null;
  sessions?: number;
  active?: { from: string; to: string };
  prsCreated: PrInfo[];
  branches?: string[];
  editedFiles?: string[];
}
export interface BundleItem { pair: string; at: string; sides: [SideInfo, SideInfo]; sharedFiles: string[] }

/**
 * `asOf` describes the pair as a live system would see it when the alert fired: no PR created later, no merge or
 * close after the alert, only comments made before it, and only the work's activity up to then. PR descriptions are
 * included (as fetched: a later edit is the one hindsight left in). Without `asOf` the bundle shows outcomes too,
 * for adjudicating what really happened.
 */
export function describe(ctx: Context, sides: [string, string], at: string, ownerLabel: (u: string | null) => string | null, asOf = false): BundleItem {
  const owners = prOwners(ctx);
  const prs = new Map(ctx.prs.map((p) => [prSide(p.repo, p.number), p]));
  const by = (ts: string | null) => (asOf && ts && ts > at ? null : ts);
  const prInfo = (side: string): PrInfo | null => {
    const p = prs.get(side);
    if (!p || (asOf && p.createdAt > at)) return null;
    const base: PrInfo = { pr: side.slice(3), title: p.title, author: p.author, createdAt: p.createdAt, mergedAt: by(p.mergedAt), closedAt: by(p.closedAt), files: p.files?.slice(0, MAX_FILES) ?? null };
    if (!asOf) return base;
    return { ...base, body: (p.body ?? "").slice(0, MAX_BODY), comments: (p.comments ?? []).filter((c) => c.at <= at).slice(0, MAX_COMMENTS).map((c) => ({ at: c.at, body: c.body.slice(0, MAX_COMMENT) })) };
  };
  const files: Array<Set<string>> = [];
  const info = sides.map((side): SideInfo => {
    if (side.startsWith("pr:")) {
      const p = prInfo(side);
      files.push(new Set(p?.files ?? []));
      return { side, kind: "pr", owner: p?.author ?? null, prsCreated: p ? [p] : [] };
    }
    const root = side.slice(5);
    const members = [...ctx.sessions.values()].filter((s) => s.root === root);
    const edits = members.flatMap((s) => s.edits).filter((e) => !e.hot && (!asOf || e.ts <= at));
    const counts = new Map<string, number>();
    for (const e of edits) counts.set(e.path, (counts.get(e.path) ?? 0) + 1);
    const edited = [...counts.entries()].sort((a, b) => b[1] - a[1]).map(([p]) => p);
    files.push(new Set(edited));
    const created = [...owners.entries()].filter(([, r]) => r.has(root)).map(([k]) => prInfo(k)).filter((x): x is PrInfo => !!x);
    const times = members.flatMap((s) => [...s.edits.map((e) => e.ts), ...s.prs.map((p) => p.ts)]).filter((t) => !asOf || t <= at).sort();
    return {
      side: `work:${createHashless(root)}`, kind: "work", owner: ownerLabel(members[0]?.owner ?? null), sessions: members.length,
      active: times.length ? { from: times[0]!, to: times.at(-1)! } : undefined, prsCreated: created,
      branches: [...new Set(members.flatMap((s) => s.branches.filter((b) => !asOf || b.ts <= at).map((b) => b.name)))].slice(0, 20), editedFiles: edited.slice(0, MAX_FILES),
    };
  }) as [SideInfo, SideInfo];
  const shared = [...files[0]!].filter((f) => files[1]!.has(f)).slice(0, MAX_FILES);
  // `pair` is the verdict key (eval.ts pairId); the displayed sides carry only short tokens.
  return { pair: pairId(sides), at, sides: info, sharedFiles: shared };
}

/** Session keys name a harness and a session id; the bundle shows a short stable token instead. */
function createHashless(root: string): string {
  let h = 0;
  for (const c of root) h = (h * 31 + c.charCodeAt(0)) >>> 0;
  return h.toString(36);
}
