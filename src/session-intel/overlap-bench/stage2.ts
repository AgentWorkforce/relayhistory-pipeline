/**
 * Stage 2, the intent judge, as an eval (docs/session-intel-overlap-bench.md, "Stage 2"). A batch is a blind,
 * shuffled set of as-of bundles: positives (incident pairs the loose ranker reaches before the harm, and, on dev,
 * pairs judged overlap in hindsight) and fresh negatives (top-ranked alerts at the headline budget that no earlier
 * round judged). The judge itself runs outside this module, from a versioned prompt; its verdicts are scored here
 * against the batch's key at a frozen cutoff. Nothing here calls a model.
 */
import { createHash } from "node:crypto";
import type { Alert, Context } from "./detectors.ts";
import { budgeted, pairId, wilson } from "./eval.ts";
import { type IncidentLabel, linker } from "./score.ts";
import { join } from "node:path";
import { assertPrivateInput, readPrivate } from "../cloud-harvest/private-fs.ts";
import { parseInstant } from "../cloud-harvest/instant.ts";

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
  /** The frozen alert budget: fresh negatives are drawn from what the frozen ranker raises within it. */
  perHundred: number;
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
  const fresh = budgeted(ctx, b.frozen, b.perHundred).filter((a) => !b.judged.has(pairId(a.sides)) && !seen.has(pairId(a.sides)));
  for (const a of fresh.sort((x, y) => (h(pairId(x.sides)) < h(pairId(y.sides)) ? -1 : 1)).slice(0, b.fresh)) items.push({ sides: a.sides, at: a.at, truth: "fresh" });
  return items.sort((x, y) => (h(pairId(x.sides) + x.at) < h(pairId(y.sides) + y.at) ? -1 : 1));
}

export interface Stage2Verdict { pair: string; verdict: "overlap" | "not_overlap" | "unclear"; p_overlap: number }
export interface Stage2Score {
  cutoff: number; positives: number; positivesKept: number; fresh: number; freshKept: number;
  /**
   * The share of fresh top-ranked alerts the judge would still raise, with a 95% Wilson interval. It is an observed
   * keep rate, not a false-alert rate: fresh alerts have no independent truth (a few may be real overlaps), and the
   * interval assumes independent draws.
   */
  freshKeptLow: number | null; freshKeptHigh: number | null;
}

/**
 * Validates verdicts against a key: exactly one verdict per key pair and none for any other pair, a known verdict,
 * and a probability in [0, 1]. Anything else is refused: a partial, duplicated or malformed answer is no score.
 */
export function validVerdicts(key: Array<{ pair: string }>, verdicts: unknown[]): Stage2Verdict[] {
  const pairs = new Set(key.map((k) => k.pair));
  if (pairs.size !== key.length) throw new Error("the key names a pair twice");
  const seen = new Set<string>();
  for (const v of verdicts as Array<Partial<Stage2Verdict>>) {
    if (!v || typeof v.pair !== "string" || !pairs.has(v.pair)) throw new Error("a verdict names a pair that is not in the batch");
    if (seen.has(v.pair)) throw new Error("a pair has more than one verdict");
    if (!["overlap", "not_overlap", "unclear"].includes(v.verdict as string)) throw new Error("a verdict is not overlap, not_overlap or unclear");
    if (typeof v.p_overlap !== "number" || !Number.isFinite(v.p_overlap) || v.p_overlap < 0 || v.p_overlap > 1) throw new Error("a p_overlap is not a probability in [0, 1]");
    seen.add(v.pair);
  }
  if (seen.size !== pairs.size) throw new Error(`verdicts cover ${seen.size} of ${pairs.size} pairs; every pair needs exactly one`);
  return verdicts as Stage2Verdict[];
}

/** Scores validated verdicts against a batch's key at `cutoff` (a pair is kept when p_overlap >= cutoff). */
export function scoreStage2(key: Array<{ pair: string; truth: Truth }>, verdicts: unknown[], cutoff: number): Stage2Score {
  const v = new Map(validVerdicts(key, verdicts).map((x) => [x.pair, x]));
  let positives = 0, positivesKept = 0, fresh = 0, freshKept = 0;
  for (const k of key) {
    const kept = v.get(k.pair)!.p_overlap >= cutoff;
    if (k.truth === "fresh") { fresh++; if (kept) freshKept++; } else { positives++; if (kept) positivesKept++; }
  }
  const w = wilson(freshKept, fresh);
  return { cutoff, positives, positivesKept, fresh, freshKept, freshKeptLow: w?.low ?? null, freshKeptHigh: w?.high ?? null };
}

export interface BatchManifest {
  format: string; split: "dev" | "heldout"; cutoff: number; promptHash: string; seed: string; judge: string; labelsHash: string;
  /** The exact terms the batch was made (and, on held-out, claimed) under. */
  terms: Record<string, unknown>;
  /** The earliest delete-by of the data the batch was drawn from. */
  deleteBy: string;
  items: number; keyHash: string; parts: Record<string, string>; prompts: Record<string, string>; manifestHash: string;
}

/**
 * A batch as it was made: the manifest by its own hash, every part, prompt and the key by theirs, the key's length
 * by the manifest's item count, and the verdicts (judge/verdicts-NN.json, one per part) validated against the key.
 */
export function verifiedBatch(dir: string, now = Date.now()): { manifest: BatchManifest; key: Array<{ pair: string; truth: Truth }>; verdicts: Stage2Verdict[] } {
  const sha = (t: string) => createHash("sha256").update(t).digest("hex");
  // The batch's directories are private, owned, real directories before anything in them is opened.
  for (const d of [dir, join(dir, "key"), join(dir, "judge")]) assertPrivateInput(d, "dir");
  const manifest = JSON.parse(readPrivate(join(dir, "key", "stage2.json"))) as BatchManifest;
  const { manifestHash, ...body } = manifest;
  if (manifest.format !== "overlap-bench-stage2/3" || manifestHash !== sha(JSON.stringify(body))) throw new Error("the batch manifest was changed after the batch was made");
  // Retention before anything derived from the data is opened: an expired batch must be deleted, not scored.
  const deadline = parseInstant(manifest.deleteBy);
  if (!(deadline > now)) throw new Error("the batch is past (or has no valid) delete-by; it must be deleted, not scored");
  const keyText = readPrivate(join(dir, "key", "key.json"));
  if (sha(keyText) !== manifest.keyHash) throw new Error("the batch key was changed after the batch was made");
  for (const [name, h] of [...Object.entries(manifest.parts), ...Object.entries(manifest.prompts)]) {
    if (sha(readPrivate(join(dir, "judge", name))) !== h) throw new Error(`${name} was changed after the batch was made`);
  }
  const key = JSON.parse(keyText) as Array<{ pair: string; truth: Truth }>;
  if (key.length !== manifest.items) throw new Error("the key does not match the manifest's item count");
  const verdicts: unknown[] = [];
  for (const part of Object.keys(manifest.parts)) {
    const file = join(dir, "judge", part.replace("part-", "verdicts-") + ".json");
    const parsed = JSON.parse(readPrivate(file));
    if (!Array.isArray(parsed)) throw new Error(`${file} is not a JSON array`);
    verdicts.push(...parsed);
  }
  return { manifest, key, verdicts: validVerdicts(key, verdicts) };
}
