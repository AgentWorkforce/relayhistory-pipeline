/**
 * Scoring (docs/session-intel-overlap-bench.md, "Scoring"). Incidents are pairs of forge lines (a PR or a commit);
 * a line links to its PR node and to every work root that created, named or pushed it. An alert hits an incident
 * when its two sides link to the two lines and it fires before the harm. Alerts are cut to the budget first.
 */
import { CODING_SOURCES } from "../cloud-harvest/harvest.ts";
import { type Alert, type Context, prOwners, prSide, rootSide } from "./detectors.ts";

export interface IncidentLabel {
  id: string;
  repo: string;
  kind: string;
  lines: string[];
  harmAt: string;
  split: "dev" | "heldout";
  owners?: string;
}
/** A pair of lines adjudicated as not overlap (landing race, self-iteration, process). Alerts on it cost precision. */
export interface NegativeLabel { repo: string; lines: string[]; reason: string }

export interface Score {
  detector: string;
  alerts: number;
  budget: number;
  linkable: number;
  hits: Array<{ id: string; at: string; leadH: number }>;
  missed: string[];
  negativeHits: number;
  recall: number;
  medianLeadH: number | null;
}

export function linker(ctx: Context) {
  const owners = prOwners(ctx);
  const shaRoots = new Map<string, Set<string>>();
  for (const s of ctx.sessions.values()) for (const x of s.shas) for (const n of [7, 8, 9, 10, 12, 40]) {
    if (x.sha.length >= n) shaRoots.set(x.sha.slice(0, n), (shaRoots.get(x.sha.slice(0, n)) ?? new Set()).add(s.root));
  }
  return (repo: string, line: string): Set<string> => {
    const [kind, ref] = line.split(":", 2) as [string, string];
    const out = new Set<string>();
    if (kind === "pr") {
      const side = prSide(repo, Number(ref));
      out.add(side);
      for (const r of owners.get(side) ?? []) out.add(rootSide(r));
    } else if (kind === "commit") {
      for (const n of [40, 12, 10, 9, 8, 7]) for (const r of shaRoots.get(ref.toLowerCase().slice(0, n)) ?? []) out.add(rootSide(r));
    }
    return out;
  };
}

/** Linkable: both lines link to at least one side, and at least one of them to a work root (sessions exist). */
export const isLinkable = (a: Set<string>, b: Set<string>) =>
  a.size > 0 && b.size > 0 && [...a, ...b].some((s) => s.startsWith("root:"));

const WEEK = 7 * 86_400_000;
const weekOf = (ts: string) => Math.floor(Date.parse(ts) / WEEK);

/**
 * The alert budget as a live system would spend it: each week gets `perHundred` alerts per hundred sessions
 * active that week, and within a week the highest-scoring alerts win (ties: earliest). A single global cut would
 * let one busy or mis-stamped stretch of the window take the whole budget.
 */
export function budgetCut(alerts: Alert[], sessionsByWeek: Map<number, number>, perHundred: number): Alert[] {
  const byWeek = new Map<number, Alert[]>();
  for (const a of alerts) { const w = weekOf(a.at); byWeek.set(w, [...(byWeek.get(w) ?? []), a]); }
  const out: Alert[] = [];
  for (const [w, list] of byWeek) {
    const budget = Math.max(1, Math.floor(((sessionsByWeek.get(w) ?? 0) * perHundred) / 100));
    out.push(...list.sort((x, y) => y.score - x.score || (x.at < y.at ? -1 : x.at > y.at ? 1 : 0)).slice(0, budget));
  }
  return out;
}

/** Coding sessions active per week (by last activity), the denominator of each week's budget. */
export function sessionsByWeek(ctx: Context): Map<number, number> {
  const m = new Map<number, number>();
  for (const s of ctx.sessions.values()) if (CODING_SOURCES.has(s.source) && Number.isFinite(Date.parse(s.lastTs))) m.set(weekOf(s.lastTs), (m.get(weekOf(s.lastTs)) ?? 0) + 1);
  return m;
}

export function score(detector: string, ctx: Context, raw: Alert[], incidents: IncidentLabel[], negatives: NegativeLabel[], opts: { sessions: number; perHundred: number }): Score {
  const budget = Math.max(1, Math.floor((opts.sessions * opts.perHundred) / 100));
  const alerts = opts.sessions >= 1e9 ? raw : budgetCut(raw, sessionsByWeek(ctx), opts.perHundred);
  const link = linker(ctx);
  const joins = (a: Alert, x: Set<string>, y: Set<string>) => (x.has(a.sides[0]) && y.has(a.sides[1])) || (y.has(a.sides[0]) && x.has(a.sides[1]));
  const hits: Score["hits"] = [], missed: string[] = [];
  let linkable = 0;
  for (const inc of incidents) {
    const [l1, l2] = [link(inc.repo, inc.lines[0]!), link(inc.repo, inc.lines[1] ?? "")];
    if (!isLinkable(l1, l2)) continue;
    linkable++;
    const first = alerts.filter((a) => a.at < inc.harmAt && joins(a, l1, l2)).sort((a, b) => (a.at < b.at ? -1 : 1))[0];
    if (first) hits.push({ id: inc.id, at: first.at, leadH: (Date.parse(inc.harmAt) - Date.parse(first.at)) / 3_600_000 });
    else missed.push(inc.id);
  }
  let negativeHits = 0;
  for (const n of negatives) {
    const [l1, l2] = [link(n.repo, n.lines[0]!), link(n.repo, n.lines[1] ?? "")];
    if (l1.size && l2.size && alerts.some((a) => joins(a, l1, l2))) negativeHits++;
  }
  const leads = hits.map((h) => h.leadH).sort((a, b) => a - b);
  return {
    detector, alerts: alerts.length, budget, linkable, hits, missed, negativeHits,
    recall: linkable ? hits.length / linkable : 0, medianLeadH: leads.length ? leads[leads.length >> 1]! : null,
  };
}
