/**
 * D4, the combined ranker (docs/session-intel-overlap-bench.md, "Combined ranker"). D1, D2 and D3 see different
 * evidence for the same underlying event (two lines of work converging on one change), so their alerts are merged
 * per pair of lines and their weights summed in time order. The pair alerts when the running total first reaches
 * `threshold`, with that total as its score: only evidence seen by then counts, so lead time stays honest.
 *
 * A D1 alert pairs a root with a pull request; it is also counted against the roots owning that PR at the
 * alert's time, so it can corroborate D2 and D3 evidence about the same two lines.
 */
import { type Alert, type Context, concurrentEdits, openPrOverlap, ownersAt, prOwnership, rootSide, sharedTrigger } from "./detectors.ts";

export interface RankerParams {
  /** Weight of a D1 alert (a root editing another line's open PR files). */
  wOpenPr: number;
  /** Weight of a D2 alert (two roots reacting to the same PR). */
  wTrigger: number;
  /** Weight per unit of D3 rarity (two roots editing rare shared files), capped at `rarityCap`. */
  wRarity: number;
  rarityCap: number;
  /** Added once a pair has evidence from two or more detectors. */
  wAgree: number;
  /** Added when the two sides belong to different people. */
  wCrossOwner: number;
  threshold: number;
}

/** The component detectors run loose; the ranker does the selecting. */
const COMPONENTS = {
  openPr: { minShared: 1, maxPrFiles: 30, skipHot: true, suppressCommunicated: true },
  trigger: { windowMin: 240, requireEdit: false, suppressCommunicated: true },
  concurrent: { windowH: 6, minShared: 1, maxRootsPerPath: 8, skipHot: true, crossOwnerOnly: false, suppressCommunicated: true },
} as const;

export interface Components { openPr: Alert[]; trigger: Alert[]; concurrent: Alert[] }

/** Runs the component detectors once; `rank` can then be re-run cheaply over many weightings. */
export function components(ctx: Context): Components {
  return {
    openPr: openPrOverlap(ctx, COMPONENTS.openPr),
    trigger: sharedTrigger(ctx, COMPONENTS.trigger),
    // D3 is re-emitted at every shared path (minShared 1 keeps only the first), so its running rarity is recovered
    // here from alerts at increasing thresholds.
    concurrent: [1, 2, 3, 5, 8].flatMap((minShared) => concurrentEdits(ctx, { ...COMPONENTS.concurrent, minShared })),
  };
}

const pairKey = (a: string, b: string) => (a < b ? `${a}\u0001${b}` : `${b}\u0001${a}`);
interface Evidence { at: string; kind: "openPr" | "trigger" | "concurrent"; value: number; sides: [string, string]; owners: [string | null, string | null] }

export function rank(ctx: Context, c: Components, p: RankerParams): Alert[] {
  const own = prOwnership(ctx);
  const byPair = new Map<string, Evidence[]>();
  const add = (e: Evidence) => { const k = pairKey(...e.sides); byPair.set(k, [...(byPair.get(k) ?? []), e]); };
  for (const a of c.openPr) {
    add({ at: a.at, kind: "openPr", value: 1, sides: a.sides, owners: a.owners });
    for (const r of ownersAt(own, a.sides[1], a.at)) if (rootSide(r) !== a.sides[0]) add({ at: a.at, kind: "openPr", value: 1, sides: [a.sides[0], rootSide(r)], owners: a.owners });
  }
  for (const a of c.trigger) add({ at: a.at, kind: "trigger", value: 1, sides: a.sides, owners: a.owners });
  for (const a of c.concurrent) add({ at: a.at, kind: "concurrent", value: a.score, sides: a.sides, owners: a.owners });

  const alerts: Alert[] = [];
  for (const evidence of byPair.values()) {
    evidence.sort((x, y) => (x.at < y.at ? -1 : x.at > y.at ? 1 : 0));
    let openPr = 0, trigger = 0, rarity = 0;
    const kinds = new Set<string>();
    for (const e of evidence) {
      kinds.add(e.kind);
      if (e.kind === "openPr") openPr = 1;
      else if (e.kind === "trigger") trigger = 1;
      else rarity = Math.max(rarity, e.value);
      const [o1, o2] = e.owners;
      const total = p.wOpenPr * openPr + p.wTrigger * trigger + p.wRarity * Math.min(rarity, p.rarityCap)
        + (kinds.size >= 2 ? p.wAgree : 0) + (o1 && o2 && o1 !== o2 ? p.wCrossOwner : 0);
      if (total >= p.threshold) {
        alerts.push({ detector: "ranker", sides: e.sides, at: e.at, artifact: [...kinds].sort().join("+"), score: total, owners: e.owners });
        break;
      }
    }
  }
  return alerts;
}

/** The dev grid for the ranker. Small on purpose: every point is one more look at the dev split. */
export function rankerGrid(): RankerParams[] {
  const out: RankerParams[] = [];
  for (const wOpenPr of [0.5, 1, 2]) for (const wTrigger of [0.5, 1, 2]) for (const wRarity of [0.5, 1, 2]) for (const wAgree of [0, 1, 2]) for (const threshold of [0.5, 1, 2]) {
    out.push({ wOpenPr, wTrigger, wRarity, rarityCap: 3, wAgree, wCrossOwner: 1, threshold });
  }
  return out;
}
