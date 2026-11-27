/**
 * Adjudication bundles (docs/session-intel-overlap-bench.md, "Precision"). Each alerted pair is described from
 * derived facts and forge metadata only: which PRs each side created, their titles, files and outcomes, the
 * branches it worked on, the files it edited and when. No transcript text. The detector that raised the pair is
 * withheld, so the verdict cannot lean on it.
 */
import type { Context } from "./detectors.ts";
import type { Session } from "./model.ts";
import { asOf as asOfEntry } from "../forge/types.ts";
import { ownersAt, prOwners, prOwnership, prSide } from "./detectors.ts";
import { pairId } from "./eval.ts";
import { canonicalTs } from "../cloud-harvest/instant.ts";

const MAX_FILES = 40;

interface PrInfo {
  pr: string; author: string; createdAt: string;
  /** In hindsight bundles as fetched; in as-of bundles only from dated history, the version in effect at the alert. */
  title?: string; mergedAt?: string | null; closedAt?: string | null; files?: string[] | null;
  /** As-of bundles: the description in effect at the alert, from dated history only (absent without history). */
  body?: string;
  /** Hindsight bundles never carry comments; as-of bundles leave them out (see `describe`). */
}
const MAX_BODY = 1500;
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
 * `asOf` describes the pair as a live system would see it when the alert fired, from dated facts only: PRs that
 * existed and that the side owned by then, their author and creation time, and the side's own branches, edits and
 * activity up to then (derived from its sessions). A PR's title and description appear only from dated history (the
 * version in effect at the alert); as fetched they would leak a later edit. Comment text, file lists, merges and
 * closes are left out: comments are fetched in their latest form, so their text is not dated. Without `asOf`
 * the bundle shows titles, files and outcomes, for adjudicating in hindsight what really happened.
 */
export function describe(ctx: Context, sides: [string, string], atIn: string, ownerLabel: (u: string | null) => string | null, asOf = false): BundleItem {
  // Compared as strings with the model's times, so it takes their one form (canonicalTs); an invalid time hides all.
  const at = canonicalTs(atIn) ?? (asOf ? "" : atIn);
  const owners = prOwners(ctx), own = prOwnership(ctx);
  const prs = new Map(ctx.prs.map((p) => [prSide(p.repo, p.number), p]));
  const prInfo = (side: string): PrInfo | null => {
    const p = prs.get(side);
    if (!p || (asOf && p.createdAt > at)) return null;
    const base: PrInfo = { pr: side.slice(3), author: p.author, createdAt: p.createdAt };
    if (!asOf) return { ...base, title: p.title, mergedAt: p.mergedAt, closedAt: p.closedAt, files: p.files?.slice(0, MAX_FILES) ?? null };
    // Title and description only from dated history (forge `history`): the version in effect at the alert.
    // Comment text is left out: comments are fetched in their latest form and dated only by creation, so a later
    // edit would leak. An unknown description interval (null) shows nothing.
    const title = asOfEntry(p.history?.titles, at)?.title, body = asOfEntry(p.history?.bodies, at)?.body;
    return { ...base, ...(title !== undefined ? { title } : {}), ...(typeof body === "string" ? { body: body.slice(0, MAX_BODY) } : {}) };
  };
  const files: Array<Set<string>> = [];
  const info = sides.map((side): SideInfo => {
    if (side.startsWith("pr:")) {
      const p = prInfo(side);
      // As of the alert, a PR side's files are not known (see above), so no shared files are claimed for it.
      files.push(new Set(asOf ? [] : p?.files ?? []));
      return { side, kind: "pr", owner: p?.author ?? null, prsCreated: p ? [p] : [] };
    }
    const root = side.slice(5);
    // As of the alert, a session is known only once it has dated activity by then: a session that started later
    // is not counted, and its owner is not shown.
    const members = [...ctx.sessions.values()].filter((s) => s.root === root && (!asOf || firstSeen(s) <= at));
    const edits = members.flatMap((s) => s.edits).filter((e) => !e.hot && (!asOf || e.ts <= at));
    const counts = new Map<string, number>();
    for (const e of edits) counts.set(e.path, (counts.get(e.path) ?? 0) + 1);
    const edited = [...counts.entries()].sort((a, b) => b[1] - a[1]).map(([p]) => p);
    files.push(new Set(edited));
    const ownedSides = asOf ? [...own.keys()].filter((k) => ownersAt(own, k, at).includes(root)) : [...owners.entries()].filter(([, r]) => r.has(root)).map(([k]) => k);
    const created = ownedSides.map((k) => prInfo(k)).filter((x): x is PrInfo => !!x);
    const times = members.flatMap((s) => [...s.edits.map((e) => e.ts), ...s.prs.filter((p) => !p.undated).map((p) => p.ts)]).filter((t) => !asOf || t <= at).sort();
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

/** When a session is first known to exist: its earliest dated fact, else its directory creation time. */
function firstSeen(s: Session): string {
  const ts = [...s.edits.map((e) => e.ts), ...s.prs.filter((p) => !p.undated).map((p) => p.ts), ...s.branches.map((b) => b.ts), ...s.shas.map((x) => x.ts), ...s.received.map((r) => r.ts)];
  return ts.length ? ts.reduce((a, b) => (a < b ? a : b)) : s.startedAt;
}
