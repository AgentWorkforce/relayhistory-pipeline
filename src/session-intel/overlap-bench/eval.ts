/**
 * The overlap eval (docs/session-intel-overlap-bench.md, "Eval"). One report per detector point:
 *
 *  - recall at several alert budgets (alerts per hundred coding sessions, spent per week), with lead time and
 *    alerts on adjudicated non-overlaps: what a team would get at each alert rate;
 *  - precision at the headline budget, from blind verdicts on the alerts themselves, with a Wilson interval
 *    (the incident labels only cover overlaps the forge shows, so they cannot measure precision);
 *  - a ceiling analysis of every incident: is a miss the data's fault (no session on a side) or the detector's.
 */
import { createHash } from "node:crypto";
import type { Alert, Context } from "./detectors.ts";
import { budgetCut, type IncidentLabel, isLinkable, linker, type NegativeLabel, score, sessionsByWeek } from "./score.ts";

export const BUDGETS = [1, 5, 20, Infinity] as const;
export const HEADLINE_BUDGET = 5;

/** A blind verdict on one alerted pair of sides, keyed by `pairId`. */
export interface AlertVerdict { pair: string; verdict: "overlap" | "not_overlap" | "unclear"; note?: string }

export const pairId = (sides: readonly [string, string]) => [...sides].sort().join(" | ");

export interface CurvePoint { perHundred: number | "all"; alerts: number; hits: number; recall: number; negativeHits: number; medianLeadH: number | null; hitIds: string[] }
export interface Precision { judged: number; overlap: number; unclear: number; unjudged: number; rate: number | null; low: number | null; high: number | null }
export interface EvalReport { detector: string; params: unknown; linkable: number; curve: CurvePoint[]; precision: Precision | null }

/** Wilson score interval at 95%. */
export function wilson(k: number, n: number): { low: number; high: number } | null {
  if (!n) return null;
  const z = 1.96, p = k / n, d = 1 + (z * z) / n;
  const c = p + (z * z) / (2 * n), m = z * Math.sqrt((p * (1 - p)) / n + (z * z) / (4 * n * n));
  return { low: Math.max(0, (c - m) / d), high: Math.min(1, (c + m) / d) };
}

export function budgeted(ctx: Context, raw: Alert[], perHundred: number): Alert[] {
  return Number.isFinite(perHundred) ? budgetCut(raw, sessionsByWeek(ctx), perHundred) : raw;
}

export function precisionOf(alerts: Alert[], verdicts: Map<string, AlertVerdict>): Precision {
  let judged = 0, overlap = 0, unclear = 0, unjudged = 0;
  for (const a of alerts) {
    const v = verdicts.get(pairId(a.sides));
    if (!v) unjudged++;
    else if (v.verdict === "unclear") unclear++;
    else { judged++; if (v.verdict === "overlap") overlap++; }
  }
  const w = wilson(overlap, judged);
  return { judged, overlap, unclear, unjudged, rate: judged ? overlap / judged : null, low: w?.low ?? null, high: w?.high ?? null };
}

export function evaluate(detector: string, params: unknown, ctx: Context & { codingSessions: number }, raw: Alert[], incidents: IncidentLabel[], negatives: NegativeLabel[], verdicts?: Map<string, AlertVerdict>): EvalReport {
  const curve: CurvePoint[] = [];
  let linkable = 0;
  for (const b of BUDGETS) {
    const s = score(detector, ctx, raw, incidents, negatives, Number.isFinite(b) ? { sessions: ctx.codingSessions, perHundred: b } : { sessions: 1e9, perHundred: 100 });
    linkable = s.linkable;
    curve.push({ perHundred: Number.isFinite(b) ? b : "all", alerts: s.alerts, hits: s.hits.length, recall: s.recall, negativeHits: s.negativeHits, medianLeadH: s.medianLeadH, hitIds: s.hits.map((h) => h.id).sort() });
  }
  return { detector, params, linkable, curve, precision: verdicts ? precisionOf(budgeted(ctx, raw, HEADLINE_BUDGET), verdicts) : null };
}

export type CeilingClass = "no_session" | "one_side" | "both_sides";
/** Where each incident sits: how much session evidence exists for its two lines at all. */
export function ceiling(ctx: Context, incidents: IncidentLabel[]): Array<{ id: string; cls: CeilingClass; linkable: boolean }> {
  const link = linker(ctx);
  return incidents.map((i) => {
    const [a, b] = [link(i.repo, i.lines[0]!), link(i.repo, i.lines[1] ?? "")];
    const roots = [a, b].filter((s) => [...s].some((x) => x.startsWith("root:"))).length;
    return { id: i.id, cls: roots === 2 ? "both_sides" : roots === 1 ? "one_side" : "no_session", linkable: isLinkable(a, b) };
  });
}

/**
 * The adjudication sample: the top `perDetector` alerts of each detector at the headline budget, merged by pair
 * and shuffled by a seeded hash, with the detector names withheld. Pairs already judged are skipped.
 */
export function adjudicationSample(ctx: Context, byDetector: Map<string, Alert[]>, perDetector: number, seed: string, judged: Set<string>): Array<{ pair: string; sides: [string, string]; at: string }> {
  const out = new Map<string, { pair: string; sides: [string, string]; at: string }>();
  for (const raw of byDetector.values()) {
    const top = budgeted(ctx, raw, HEADLINE_BUDGET).sort((x, y) => y.score - x.score || (x.at < y.at ? -1 : 1));
    let n = 0;
    for (const a of top) {
      if (n >= perDetector) break;
      const id = pairId(a.sides);
      if (judged.has(id)) continue;
      n++;
      const o = out.get(id);
      if (!o || a.at < o.at) out.set(id, { pair: id, sides: a.sides, at: a.at });
    }
  }
  const h = (s: string) => createHash("sha256").update(`${seed}\u0000${s}`).digest("hex");
  return [...out.values()].sort((x, y) => (h(x.pair) < h(y.pair) ? -1 : 1));
}
