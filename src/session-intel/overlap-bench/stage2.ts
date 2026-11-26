/**
 * Stage 2, the intent judge, as an eval (docs/session-intel-overlap-bench.md, "Stage 2"). A batch is a blind,
 * shuffled set of as-of bundles: positives (incident pairs the loose ranker reaches before the harm, and, on dev,
 * pairs judged overlap in hindsight) and fresh negatives (top-ranked alerts at the headline budget that no earlier
 * round judged). The judge itself runs outside this module, from a versioned prompt; its verdicts are scored here
 * against the batch's key at a frozen cutoff. Nothing here calls a model.
 */
import { createHash } from "node:crypto";
import type { Alert, Context } from "./detectors.ts";
import { budgeted, HEADLINE_BUDGET, pairId, wilson } from "./eval.ts";
import { type IncidentLabel, linker } from "./score.ts";

/** The ranker point that collects candidates for positives: as loose as the ranker goes. */
export const LOOSE_RANKER = { wOpenPr: 1, wTrigger: 1, wRarity: 1, rarityCap: 3, wAgree: 0, wCrossOwner: 0, threshold: 0.01 } as const;

export type Truth = `incident:${string}` | "hindsight-overlap" | "fresh";
export interface BatchItem { sides: [string, string]; at: string; truth: Truth }

export interface BatchInputs {
  incidents: IncidentLabel[];
  /** Alerts of the loose ranker, for positives; alerts of the frozen ranker, for fresh negatives. */
  loose: Alert[]; frozen: Alert[];
  /** Pairs judged overlap in hindsight (dev only). */
  hindsightOverlaps: string[];
  /** Every pair any earlier round judged: never sampled as fresh. */
  judged: Set<string>;
  fresh: number; seed: string;
}

export function stage2Batch(ctx: Context, b: BatchInputs): BatchItem[] {
  const link = linker(ctx), items: BatchItem[] = [], seen = new Set<string>();
  for (const i of b.incidents) {
    const [l1, l2] = [link(i.repo, i.lines[0]!), link(i.repo, i.lines[1] ?? "")];
    const a = b.loose.filter((x) => x.at < i.harmAt && ((l1.has(x.sides[0]) && l2.has(x.sides[1])) || (l2.has(x.sides[0]) && l1.has(x.sides[1]))))
      .sort((p, q) => (p.at < q.at ? -1 : 1))[0];
    if (a && !seen.has(pairId(a.sides))) { seen.add(pairId(a.sides)); items.push({ sides: a.sides, at: a.at, truth: `incident:${i.id}` }); }
  }
  for (const pair of b.hindsightOverlaps) {
    const a = b.loose.find((x) => pairId(x.sides) === pair);
    if (a && !seen.has(pair)) { seen.add(pair); items.push({ sides: a.sides, at: a.at, truth: "hindsight-overlap" }); }
  }
  const h = (s: string) => createHash("sha256").update(`${b.seed}\u0000${s}`).digest("hex");
  const fresh = budgeted(ctx, b.frozen, HEADLINE_BUDGET).filter((a) => !b.judged.has(pairId(a.sides)) && !seen.has(pairId(a.sides)));
  for (const a of fresh.sort((x, y) => (h(pairId(x.sides)) < h(pairId(y.sides)) ? -1 : 1)).slice(0, b.fresh)) items.push({ sides: a.sides, at: a.at, truth: "fresh" });
  return items.sort((x, y) => (h(pairId(x.sides) + x.at) < h(pairId(y.sides) + y.at) ? -1 : 1));
}

export interface Stage2Verdict { pair: string; verdict: "overlap" | "not_overlap" | "unclear"; p_overlap: number }
export interface Stage2Score {
  cutoff: number; positives: number; positivesKept: number; fresh: number; freshKept: number; unjudged: number;
  /** 95% Wilson interval on the share of fresh alerts the judge would still raise. */
  freshKeptLow: number | null; freshKeptHigh: number | null;
}

/** Scores verdicts against a batch's key at `cutoff` (a pair is kept when p_overlap >= cutoff). */
export function scoreStage2(key: Array<{ pair: string; truth: Truth }>, verdicts: Stage2Verdict[], cutoff: number): Stage2Score {
  const v = new Map(verdicts.map((x) => [x.pair, x]));
  let positives = 0, positivesKept = 0, fresh = 0, freshKept = 0, unjudged = 0;
  for (const k of key) {
    const x = v.get(k.pair);
    if (!x || !Number.isFinite(x.p_overlap)) { unjudged++; continue; }
    const kept = x.p_overlap >= cutoff;
    if (k.truth === "fresh") { fresh++; if (kept) freshKept++; } else { positives++; if (kept) positivesKept++; }
  }
  const w = wilson(freshKept, fresh);
  return { cutoff, positives, positivesKept, fresh, freshKept, unjudged, freshKeptLow: w?.low ?? null, freshKeptHigh: w?.high ?? null };
}
