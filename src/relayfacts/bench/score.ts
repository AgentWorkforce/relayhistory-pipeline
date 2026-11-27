/**
 * The relayfacts benchmark scorer (docs/proposals/relayfacts-features.md, "Metrics and proposed bars").
 *
 * Deterministic. Grounding is checked against the corpus itself: every cited evidence id must exist in
 * the cited session's snapshot with the cited kind, and every quote must be a verbatim substring of the
 * whitespace-collapsed text of a human-message reference. One grounding violation fails the run.
 * Semantic matches (asked items, gaps) use a token-overlap proxy here; the signal metric and any
 * disputed match go to blind adjudication (adjudicationPackets), not to this scorer.
 */
import type { FeatureRecord, SessionKey } from "../features/types.ts";
import { sessionKeyString, textCompare } from "../features/types.ts";
import { createCallIds } from "../features/extract.ts";
import type { Corpus } from "./corpus.ts";
import type { Gold, GoldFeature } from "./gold.ts";

export const BARS = {
  grounding: 1,
  featureRecall: 0.8,
  featurePrecision: 0.9,
  linkingF1: 0.8,
  stateAccuracy: 0.85,
  askedRecall: 0.8,
  gapRecall: 0.7,
  gapPrecision: 0.85,
  duplicationMax: 0.05,
} as const;

/** A gold and a generated feature match when each holds at least this share of the other's sessions. */
export const MATCH_SHARE = 0.5;
/** Overlap coefficient at or above which two texts in the same session count as the same item. */
export const TEXT_MATCH = 0.35;
/** A generated feature holding at least this share of a gold feature's sessions counts as a fragment of it. */
export const FRAGMENT_SHARE = 0.3;

export interface GroundingViolation { feature: string; item: string; reason: string }

export interface SplitScore {
  goldFeatures: number;
  generatedFeatures: number;
  matched: number;
  featureRecall: number | null;
  featurePrecision: number | null;
  linkingF1: number | null;
  stateAccuracy: number | null;
  stateConfusion: Record<string, number>;
  askedRecall: number | null;
  gapRecall: number | null;
  gapPrecision: number | null;
  gapKindAgreement: number | null;
  duplication: number | null;
}

export interface ScoreReport {
  format: "relayfacts-score/1";
  corpus: string;
  asOf: string;
  grounding: { checkedItems: number; violations: GroundingViolation[]; pass: boolean };
  splits: Record<"dev" | "heldout" | "all", SplitScore>;
  bars: typeof BARS;
  /**
   * Bars per split. `heldout` is the quality decision (scored once); `dev` is tuning evidence only. Each split
   * needs MIN_FEATURES_FOR_VERDICT of its own labelled features; splits are never pooled for a verdict.
   */
  verdicts: { heldout: Record<string, Verdict>; dev: Record<string, Verdict> };
  matches: Array<{ gold: string; generated: string; goldState: string; generatedState: string }>;
}

const collapse = (t: string) => t.replace(/\s+/gu, " ").trim();
const tokens = (t: string) => new Set(t.toLowerCase().split(/[^a-z0-9]+/u).filter((w) => w.length > 2));
export function textSimilarity(a: string, b: string): number {
  const ta = tokens(a), tb = tokens(b);
  let shared = 0;
  for (const w of ta) if (tb.has(w)) shared++;
  const union = ta.size + tb.size - shared;
  return union ? shared / union : 0;
}

/**
 * Overlap coefficient: shared content words over the smaller text's words. A labeller's paraphrase and the
 * analysis's wording of one request differ a lot in length, which Jaccard punishes; this does not.
 */
export function textOverlap(a: string, b: string): number {
  const ta = tokens(a), tb = tokens(b);
  let shared = 0;
  for (const w of ta) if (tb.has(w)) shared++;
  const small = Math.min(ta.size, tb.size);
  return small ? shared / small : 0;
}
const textMatches = (a: string, b: string) => textOverlap(a, b) >= TEXT_MATCH;
const ratio = (n: number, d: number) => (d ? n / d : null);

/**
 * Every cited pointer resolves in the corpus, every quote is verbatim, and every commit or PR built item is carried
 * by its own cited evidence (the sha or URL is in the reference; a transcript PR event must be a created PR).
 * Grounding proves citation, not truth: an analysis `claim` is the model's statement about its cited evidence.
 */
export function checkGrounding(corpus: Corpus, features: readonly FeatureRecord[]) {
  // Per session: each reference's kind, its collapsed summary (quotes), its raw text (build refs), and for transcript
  // events the build values they carry (`sha`, `pr` for a created PR only).
  type Ref = { kind: string; text: string; raw: string; values: Set<string> };
  const refs = new Map<string, Map<string, Ref>>();
  for (const s of corpus.sessions) {
    const m = new Map<string, Ref>();
    const creates = createCallIds(s.snapshot);
    for (const r of s.snapshot.evidence?.references ?? []) {
      if (m.has(r.id)) continue;
      const raw = `${r.summary ?? ""}\n${r.detail ?? ""}`.toLowerCase();
      const values = new Set<string>();
      // A PR is carried only by the successful result paired with the session's own `gh pr create` call.
      if (r.kind === "tool-result" && r.isError !== true && r.toolCallId && creates.has(r.toolCallId))
        for (const u of raw.matchAll(/https:\/\/github\.com\/[a-z0-9_.-]+\/[a-z0-9_.-]+\/pull\/[0-9]{1,10}/gu)) values.add(`pr:${u[0]}`);
      m.set(r.id, { kind: r.kind, text: collapse(r.summary ?? ""), raw, values });
    }
    // Transcript build evidence (backfill/artifacts.ts) is cited as `event:<eventId>` with kind "event".
    const a = s.artifacts;
    const ev = (id: string) => {
      const k = `event:${id}`;
      if (!m.has(k)) m.set(k, { kind: "event", text: "", raw: "", values: new Set() });
      return m.get(k)!;
    };
    for (const e of a?.pushes ?? []) if (e.sha) ev(e.eventId).values.add(`sha:${e.sha}`); else ev(e.eventId);
    for (const e of a?.commits ?? []) ev(e.eventId).values.add(`sha:${e.sha}`);
    for (const e of a?.prs ?? []) { const r = ev(e.eventId); if (e.via === "created") r.values.add(`pr:${e.url}`); }
    for (const e of [...(a?.taskRefs ?? []), ...(a?.files ?? [])]) ev(e.eventId);
    refs.set(sessionKeyString(s), m);
  }
  /** A commit or PR built item must be carried by at least one of its cited references, not just point at one. */
  const carries = (ref: Ref, kind: "commit" | "pr", value: string) =>
    kind === "pr"
      ? ref.values.has(`pr:${value.toLowerCase()}`)
      : ref.kind === "event" ? ref.values.has(`sha:${value}`) : ref.raw.includes(value.toLowerCase());
  const violations: GroundingViolation[] = [];
  let checked = 0;
  for (const f of features) {
    const members = new Set(f.sessions.map(sessionKeyString));
    for (const s of f.sessions) if (!refs.has(sessionKeyString(s))) violations.push({ feature: f.id, item: sessionKeyString(s), reason: "session not in corpus" });
    const items = [...f.asked, ...f.built, ...f.gaps, ...f.rationale];
    for (const item of items) {
      checked++;
      if (!members.has(sessionKeyString(item.session))) violations.push({ feature: f.id, item: item.id, reason: "item from a session outside the feature" });
      if (item.evidence.length === 0 && !("kind" in item && item.kind === "migration")) violations.push({ feature: f.id, item: item.id, reason: "no evidence" });
      for (const p of item.evidence) {
        const ref = refs.get(sessionKeyString(item.session))?.get(p.refId);
        if (!ref) violations.push({ feature: f.id, item: item.id, reason: `evidence ${p.refId} not in the session` });
        else if (p.kind !== null && ref.kind !== p.kind) violations.push({ feature: f.id, item: item.id, reason: `evidence ${p.refId} kind differs` });
      }
      if ("kind" in item && (item.kind === "commit" || item.kind === "pr") && item.ref) {
        const session = refs.get(sessionKeyString(item.session));
        const kind = item.kind;
        if (!item.evidence.some((p) => { const r = session?.get(p.refId); return r !== undefined && carries(r, kind, item.ref!); }))
          violations.push({ feature: f.id, item: item.id, reason: `${kind} ${item.ref.slice(0, 40)} not carried by its cited evidence` });
      }
    }
    for (const a of f.asked) for (const q of a.quotes) {
      checked++;
      const ref = refs.get(sessionKeyString(a.session))?.get(q.evidence.refId);
      if (!ref || ref.kind !== "human-message") violations.push({ feature: f.id, item: a.id, reason: "quote not from a human message" });
      else if (!ref.text.includes(q.text)) violations.push({ feature: f.id, item: a.id, reason: "quote not verbatim" });
    }
  }
  return { checkedItems: checked, violations, pass: violations.length === 0 };
}

function matchFeatures(gold: readonly GoldFeature[], generated: readonly FeatureRecord[]) {
  const cands: Array<{ g: GoldFeature; f: FeatureRecord; score: number }> = [];
  for (const g of gold) {
    const gs = new Set(g.sessions.map(sessionKeyString));
    for (const f of generated) {
      const shared = f.sessions.filter((s) => gs.has(sessionKeyString(s))).length;
      if (!shared) continue;
      if (shared / gs.size >= MATCH_SHARE && shared / f.sessions.length >= MATCH_SHARE)
        cands.push({ g, f, score: shared / (gs.size + f.sessions.length - shared) });
    }
  }
  cands.sort((a, b) => b.score - a.score || textCompare(a.g.id, b.g.id) || textCompare(a.f.id, b.f.id));
  const usedG = new Set<string>(), usedF = new Set<string>();
  const pairs: Array<{ g: GoldFeature; f: FeatureRecord }> = [];
  for (const c of cands) {
    if (usedG.has(c.g.id) || usedF.has(c.f.id)) continue;
    usedG.add(c.g.id); usedF.add(c.f.id);
    pairs.push({ g: c.g, f: c.f });
  }
  return pairs;
}

function scoreSplit(gold: readonly GoldFeature[], generatedAll: readonly FeatureRecord[], coverage: Gold["coverage"], allGold: readonly GoldFeature[]): { score: SplitScore; pairs: Array<{ g: GoldFeature; f: FeatureRecord }> } {
  const goldKeys = new Set(gold.flatMap((g) => g.sessions.map(sessionKeyString)));
  const otherGoldKeys = new Set(allGold.filter((g) => !gold.includes(g)).flatMap((g) => g.sessions.map(sessionKeyString)));
  // A generated record belongs to this split when most of its labelled sessions are this split's.
  const generated = generatedAll.filter((f) => {
    const mine = f.sessions.filter((s) => goldKeys.has(sessionKeyString(s))).length;
    const other = f.sessions.filter((s) => otherGoldKeys.has(sessionKeyString(s))).length;
    if (mine === 0 && other === 0) return coverage === "complete" && gold.length === allGold.length;
    return mine >= other && mine > 0;
  });
  const pairs = matchFeatures(gold, generated);

  // B-cubed over labelled sessions of this split.
  const goldOf = new Map<string, Set<string>>();
  for (const g of gold) { const set = new Set(g.sessions.map(sessionKeyString)); for (const k of set) goldOf.set(k, set); }
  const genOf = new Map<string, Set<string>>();
  for (const f of generatedAll) { const set = new Set(f.sessions.map(sessionKeyString).filter((k) => goldKeys.has(k))); for (const k of set) genOf.set(k, set); }
  let p = 0, r = 0, n = 0;
  for (const [k, gset] of goldOf) {
    const fset = genOf.get(k) ?? new Set([k]);
    let both = 0;
    for (const x of fset) if (gset.has(x)) both++;
    p += both / fset.size; r += both / gset.size; n++;
  }
  const bp = n ? p / n : null, br = n ? r / n : null;
  const linkingF1 = bp !== null && br !== null && bp + br > 0 ? (2 * bp * br) / (bp + br) : null;

  const confusion: Record<string, number> = {};
  let stateRight = 0;
  for (const { g, f } of pairs) {
    const k = `${g.state}->${f.state}`;
    confusion[k] = (confusion[k] ?? 0) + 1;
    if (g.state === f.state) stateRight++;
  }

  let askedTotal = 0, askedFound = 0, gapTotal = 0, gapFound = 0, gapKindAgree = 0, genGaps = 0, genGapsReal = 0;
  for (const { g, f } of pairs) {
    for (const a of g.asked) {
      askedTotal++;
      if (f.asked.some((x) => sessionKeyString(x.session) === sessionKeyString(a) && (textMatches(x.text, a.text) || x.quotes.some((q) => textMatches(q.text, a.text))))) askedFound++;
    }
    for (const gap of g.gaps) {
      gapTotal++;
      const hit = f.gaps.find((x) => sessionKeyString(x.session) === sessionKeyString(gap) && textMatches(x.text, gap.text));
      if (hit) { gapFound++; if (hit.kind === gap.kind) gapKindAgree++; }
    }
    for (const x of f.gaps) {
      if (!goldKeys.has(sessionKeyString(x.session))) continue;
      genGaps++;
      if (g.gaps.some((gap) => sessionKeyString(gap) === sessionKeyString(x.session) && textMatches(x.text, gap.text))) genGapsReal++;
    }
  }

  let duplicated = 0;
  for (const g of gold) {
    const gs = new Set(g.sessions.map(sessionKeyString));
    const fragments = generatedAll.filter((f) => f.sessions.filter((s) => gs.has(sessionKeyString(s))).length / gs.size >= FRAGMENT_SHARE);
    if (fragments.length >= 2) duplicated++;
  }

  return {
    pairs,
    score: {
      goldFeatures: gold.length,
      generatedFeatures: generated.length,
      matched: pairs.length,
      featureRecall: ratio(pairs.length, gold.length),
      featurePrecision: ratio(pairs.length, generated.length),
      linkingF1,
      stateAccuracy: ratio(stateRight, pairs.length),
      stateConfusion: confusion,
      askedRecall: ratio(askedFound, askedTotal),
      gapRecall: ratio(gapFound, gapTotal),
      gapPrecision: ratio(genGapsReal, genGaps),
      gapKindAgreement: ratio(gapKindAgree, gapFound),
      duplication: ratio(duplicated, gold.length),
    },
  };
}

/** Bars need enough labelled features in the split being judged to mean anything. */
export const MIN_FEATURES_FOR_VERDICT = 10;
export type Verdict = "pass" | "fail" | "insufficient";

function splitVerdicts(s: SplitScore, groundingPass: boolean): Record<string, Verdict> {
  const enough = s.goldFeatures >= MIN_FEATURES_FOR_VERDICT;
  const atLeast = (v: number | null, bar: number): Verdict => (v === null || !enough ? "insufficient" : v >= bar ? "pass" : "fail");
  return {
    grounding: groundingPass ? "pass" : "fail",
    featureRecall: atLeast(s.featureRecall, BARS.featureRecall),
    featurePrecision: atLeast(s.featurePrecision, BARS.featurePrecision),
    linkingF1: atLeast(s.linkingF1, BARS.linkingF1),
    stateAccuracy: atLeast(s.stateAccuracy, BARS.stateAccuracy),
    askedRecall: atLeast(s.askedRecall, BARS.askedRecall),
    gapRecall: atLeast(s.gapRecall, BARS.gapRecall),
    gapPrecision: atLeast(s.gapPrecision, BARS.gapPrecision),
    duplication: s.duplication === null || !enough ? "insufficient" : s.duplication <= BARS.duplicationMax ? "pass" : "fail",
  };
}

export function scoreRun(corpus: Corpus, gold: Gold, generated: readonly FeatureRecord[]): ScoreReport {
  const grounding = checkGrounding(corpus, generated);
  const dev = scoreSplit(gold.features.filter((g) => g.split === "dev"), generated, gold.coverage, gold.features);
  const heldout = scoreSplit(gold.features.filter((g) => g.split === "heldout"), generated, gold.coverage, gold.features);
  const all = scoreSplit(gold.features, generated, gold.coverage, gold.features);
  const verdicts: ScoreReport["verdicts"] = { heldout: splitVerdicts(heldout.score, grounding.pass), dev: splitVerdicts(dev.score, grounding.pass) };
  return {
    format: "relayfacts-score/1",
    corpus: corpus.manifestSha256,
    asOf: gold.asOf,
    grounding,
    splits: { dev: dev.score, heldout: heldout.score, all: all.score },
    bars: BARS,
    verdicts,
    matches: all.pairs.map(({ g, f }) => ({ gold: g.id, generated: f.id, goldState: g.state, generatedState: f.state })),
  };
}

/** Blind packets for the signal metric and disputed matches: rendered records with no generator name, shuffled by a seed. */
export function adjudicationPackets(features: readonly FeatureRecord[], render: (f: FeatureRecord) => string, seed: number, limit = 30) {
  let x = seed >>> 0 || 1;
  const rand = () => ((x = (x * 1664525 + 1013904223) >>> 0) / 2 ** 32);
  const order = [...features].sort((a, b) => textCompare(a.id, b.id)).map((f) => ({ f, k: rand() })).sort((a, b) => a.k - b.k).slice(0, limit);
  return order.map(({ f }, i) => ({ packet: `p${String(i + 1).padStart(3, "0")}`, featureId: f.id, sessions: f.sessions.map((s: SessionKey) => ({ source: s.source, sessionId: s.sessionId })), markdown: render(f) }));
}
