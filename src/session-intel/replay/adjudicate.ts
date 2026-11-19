/**
 * Blind adjudication (docs/session-intel-replay.md, "Adjudication"; docs/coordination-overlap-detection.md,
 * "Blind adjudication"). Pools each detector's top-K alerts at the budget, merges the same (pair, artifact)
 * across detectors, strips detector names, kinds, urgencies and alert ids, and shuffles with a seeded PRNG.
 * `packet.jsonl` goes to the adjudicators; `key.json` stays private. Verdicts are imported against the key.
 */
import { alertClassOf, classesOfVerdict, isAlertClass, verdictsAgree, type AlertClass } from "./classes.ts";
import type { ReplayResult } from "./replay.ts";
import { budgetK, DEFAULT_BUDGET_PER_100, rankAlerts } from "./score.ts";
import { boundedString, isRecord, onlyKeys, pairId, seededShuffle, sha256Hex, textCompare } from "./util.ts";

export interface PacketEntry {
  alertKey: string;
  pair: [string, string];
  artifact: string;
  /** Evidence refs only: no event kinds, which can hint at the detector. */
  evidence: Array<{ sessionId: string; eventId: string; ts: string }>;
}
export interface KeyMember {
  detector: string; detectorHash: string; alertId: string; kind: string; urgency: "high" | "low"; sentAt: string;
  /** The alert's class (classes.ts). A key written before classes existed lacks it; it is then derived. */
  alertClass?: AlertClass;
}
export interface PacketKey {
  format: "session-intel-adjudication-key/1";
  seed: string;
  budgetPer100: number;
  sources: Array<{ manifestSha256: string; detectors: Array<{ name: string; hash: string; k: number }> }>;
  entries: Array<{ alertKey: string; pair: [string, string]; artifact: string; members: KeyMember[] }>;
}

export function buildPacket(results: ReplayResult[], opts: { seed: string; budgetPer100?: number }): { packet: PacketEntry[]; key: PacketKey } {
  if (!opts.seed) throw new Error("packet: a seed is required");
  const budgetPer100 = opts.budgetPer100 ?? DEFAULT_BUDGET_PER_100;
  type Pooled = { pair: [string, string]; artifact: string; members: KeyMember[]; evidence: Map<string, PacketEntry["evidence"][number]> };
  const pool = new Map<string, Pooled>();
  const sources: PacketKey["sources"] = [];
  // The same replay given twice would pool every alert twice and double its detector's counts.
  const seenSources = new Set<string>();
  for (const r of results) for (const d of r.detectors) {
    const k = `${r.snapshot.manifestSha256}\u0000${d.hash}`;
    if (seenSources.has(k)) throw new Error(`packet: detector ${d.name} (${d.hash.slice(0, 12)}) on snapshot ${r.snapshot.manifestSha256.slice(0, 12)} is given twice`);
    seenSources.add(k);
  }
  for (const r of results) {
    const k = budgetK(r.snapshot.sessions, budgetPer100);
    const src: PacketKey["sources"][number] = { manifestSha256: r.snapshot.manifestSha256, detectors: [] };
    for (const d of r.detectors) {
      src.detectors.push({ name: d.name, hash: d.hash, k });
      for (const a of rankAlerts(r.alerts.filter((x) => x.detector === d.name)).slice(0, k)) {
        const id = `${pairId(a.pair)}\u0002${a.artifact}`;
        const cur: Pooled = pool.get(id) ?? { pair: [...a.pair].sort(textCompare) as [string, string], artifact: a.artifact, members: [], evidence: new Map() };
        // One member per detector per pooled entry: a LOW upgraded to HIGH on the same artifact is one alert to judge.
        if (cur.members.some((m) => m.detectorHash === d.hash)) continue;
        cur.members.push({ detector: d.name, detectorHash: d.hash, alertId: a.alertId, kind: a.kind, urgency: a.urgency, sentAt: a.sentAt, alertClass: alertClassOf(a) });
        for (const e of a.evidence) cur.evidence.set(`${e.sessionId}\u0000${e.eventId}\u0000${e.ts}`, { sessionId: e.sessionId, eventId: e.eventId, ts: e.ts });
        pool.set(id, cur);
      }
    }
    sources.push(src);
  }
  // A key derived from the seed and the pair: stable for one seed, unrelated to any detector or alert id.
  const ids = [...pool.keys()].sort(textCompare);
  const order = seededShuffle(ids, opts.seed);
  const entries = order.map((id) => {
    const p = pool.get(id)!;
    return { alertKey: sha256Hex(`${opts.seed}\u0000${id}`).slice(0, 16), p };
  });
  const packet = entries.map(({ alertKey, p }): PacketEntry => ({
    alertKey, pair: p.pair, artifact: p.artifact,
    evidence: [...p.evidence.values()].sort((a, b) => textCompare(a.ts, b.ts) || textCompare(a.sessionId, b.sessionId) || textCompare(a.eventId, b.eventId)),
  }));
  const key: PacketKey = {
    format: "session-intel-adjudication-key/1", seed: opts.seed, budgetPer100, sources,
    entries: entries.map(({ alertKey, p }) => ({
      alertKey, pair: p.pair, artifact: p.artifact,
      members: [...p.members].sort((a, b) => textCompare(a.detector, b.detector) || textCompare(a.alertId, b.alertId)),
    })),
  };
  return { packet, key };
}

/**
 * `collision` and `overlap` are the taxonomy's precise forms; `real_overlap` (the M0 verdict) confirms either
 * (classes.ts classesOfVerdict).
 */
export const VERDICTS = ["real_overlap", "collision", "overlap", "intent_conflict", "constraint_violation", "planned", "unrelated", "unclear"] as const;
export type VerdictValue = typeof VERDICTS[number];
export interface VerdictRecord { alertKey: string; verdict: VerdictValue; adjudicator: string }
const POSITIVE: ReadonlySet<VerdictValue> = new Set(["real_overlap", "collision", "overlap", "intent_conflict", "constraint_violation"]);

/** Validates a key.json read back from disk: shape, bounds and every member's class. */
export function parsePacketKey(raw: unknown): PacketKey {
  if (!isRecord(raw)) throw new Error("key must be an object");
  if (raw.format !== "session-intel-adjudication-key/1") throw new Error("key: format must be session-intel-adjudication-key/1");
  if (typeof raw.seed !== "string" || !raw.seed) throw new Error("key: seed must be a non-empty string");
  if (typeof raw.budgetPer100 !== "number" || !Number.isFinite(raw.budgetPer100)) throw new Error("key: budgetPer100 must be a number");
  if (!Array.isArray(raw.sources)) throw new Error("key: sources must be an array");
  if (!Array.isArray(raw.entries) || raw.entries.length > 1_000_000) throw new Error("key: entries must be an array of at most 1,000,000");
  const keys = new Set<string>();
  raw.entries.forEach((e: unknown, i: number) => {
    const where = `key.entries[${i}]`;
    if (!isRecord(e)) throw new Error(`${where} must be an object`);
    if (typeof e.alertKey !== "string" || !/^[0-9a-f]{16}$/.test(e.alertKey)) throw new Error(`${where}.alertKey must be 16 hex characters`);
    if (keys.has(e.alertKey)) throw new Error(`${where}.alertKey repeats`);
    keys.add(e.alertKey);
    if (!Array.isArray(e.pair) || e.pair.length !== 2 || !e.pair.every((x) => typeof x === "string")) throw new Error(`${where}.pair must be two session refs`);
    if (typeof e.artifact !== "string") throw new Error(`${where}.artifact must be a string`);
    if (!Array.isArray(e.members) || !e.members.length) throw new Error(`${where}.members must be a non-empty array`);
    e.members.forEach((m: unknown, j: number) => {
      const mw = `${where}.members[${j}]`;
      if (!isRecord(m)) throw new Error(`${mw} must be an object`);
      for (const f of ["detector", "alertId", "kind", "sentAt"] as const) if (typeof m[f] !== "string") throw new Error(`${mw}.${f} must be a string`);
      if (typeof m.detectorHash !== "string" || !/^[0-9a-f]{64}$/.test(m.detectorHash)) throw new Error(`${mw}.detectorHash must be a sha256`);
      if (m.urgency !== "high" && m.urgency !== "low") throw new Error(`${mw}.urgency must be high or low`);
      if (m.alertClass !== undefined && !isAlertClass(m.alertClass)) throw new Error(`${mw}.alertClass is not a class`);
      // A member without a class must have a kind that maps to one.
      alertClassOf(m as { kind: string; urgency: "high" | "low"; alertClass?: AlertClass });
    });
  });
  return raw as unknown as PacketKey;
}

export function parseVerdicts(raw: unknown[], key: PacketKey): VerdictRecord[] {
  const keys = new Set(key.entries.map((e) => e.alertKey));
  const seen = new Set<string>();
  return raw.map((v, i) => {
    const where = `verdicts[${i}]`;
    if (!isRecord(v)) throw new Error(`${where} must be an object`);
    onlyKeys(v, ["alertKey", "verdict", "adjudicator", "note"], where);
    const alertKey = boundedString(v.alertKey, `${where}.alertKey`, 64)!;
    if (!keys.has(alertKey)) throw new Error(`${where}.alertKey is not in the key`);
    if (!VERDICTS.includes(v.verdict as VerdictValue)) throw new Error(`${where}.verdict must be one of ${VERDICTS.join(", ")}`);
    const adjudicator = boundedString(v.adjudicator, `${where}.adjudicator`, 200)!;
    const dup = `${alertKey}\u0000${adjudicator}`;
    if (seen.has(dup)) throw new Error(`${where} repeats a verdict by the same adjudicator`);
    seen.add(dup);
    return { alertKey, verdict: v.verdict as VerdictValue, adjudicator };
  });
}

export interface AgreementRow { a: string; b: string; shared: number; agreed: number; rate: number | null; kappa: number | null }
/**
 * Per detector. `precision` is per pooled entry (positive / (positive + negative)); `pairPrecision` counts each
 * (detector, pair) once: a pair is positive when any of its entries is, negative when none is and one is negative.
 */
export interface AdjudicatedDetector {
  detector: string; detectorHash: string; pooled: number; adjudicated: number; positive: number; negative: number; unclear: number; precision: number | null;
  pairs: number; positivePairs: number; negativePairs: number; pairPrecision: number | null;
}
/**
 * Per (detector, alert class). `positive`: the consensus verdict confirms the alert's class. `misclassified`: it
 * confirms real work of another class. Precision is positive / (positive + misclassified + negative).
 */
export interface AdjudicatedClass {
  detector: string; detectorHash: string; alertClass: AlertClass; pooled: number; adjudicated: number;
  positive: number; misclassified: number; negative: number; unclear: number; precision: number | null;
}
export interface VerdictReport {
  format: "session-intel-verdicts/2";
  adjudicators: string[];
  entries: number;
  adjudicatedEntries: number;
  agreement: AgreementRow[];
  consensus: Record<VerdictValue, number>;
  detectors: AdjudicatedDetector[];
  classes: AdjudicatedClass[];
}

/** The verdict that confirms exactly one class (planned_fleet is confirmed by `planned`). */
const verdictForClass = (c: AlertClass): VerdictValue => c === "planned_fleet" ? "planned" : c as VerdictValue;

/**
 * The majority verdict, computed on the classes verdicts confirm rather than on their strings, so `real_overlap`
 * and `collision` support the same class. Each class is supported by every verdict that confirms it; a verdict
 * that confirms no class (`unrelated`, `unclear`) supports only itself. One leader wins. Leaders tied only
 * because the same verdicts support them all (two `real_overlap` votes support collision and overlap equally)
 * resolve to the verdict confirming exactly that set. Any other tie is "unclear".
 */
export function consensus(verdicts: VerdictValue[]): VerdictValue | null {
  if (!verdicts.length) return null;
  const support = new Map<string, number[]>();
  verdicts.forEach((v, i) => {
    const classes = classesOfVerdict(v);
    for (const k of classes.length ? classes.map((c) => `class:${c}`) : [`verdict:${v}`]) support.set(k, [...(support.get(k) ?? []), i]);
  });
  const top = Math.max(...[...support.values()].map((x) => x.length));
  const leaders = [...support].filter(([, x]) => x.length === top);
  const toVerdict = (k: string): VerdictValue => k.startsWith("class:") ? verdictForClass(k.slice(6) as AlertClass) : k.slice(8) as VerdictValue;
  if (leaders.length === 1) return toVerdict(leaders[0]![0]);
  const sameSupporters = leaders.every(([, x]) => x.join() === leaders[0]![1].join());
  if (sameSupporters && leaders.every(([k]) => k.startsWith("class:"))) {
    const set = leaders.map(([k]) => k.slice(6)).sort().join();
    const exact = VERDICTS.find((v) => [...classesOfVerdict(v)].sort().join() === set);
    if (exact) return exact;
  }
  return "unclear";
}

/**
 * Cohen's kappa with 0/1 agreement weights from verdictsAgree: observed agreement over the shared keys against
 * the agreement expected from each adjudicator's verdict frequencies. With exact-match verdicts it is the plain
 * kappa.
 */
function kappa(pairs: Array<[VerdictValue, VerdictValue]>): number | null {
  if (!pairs.length) return null;
  const n = pairs.length;
  const po = pairs.filter(([x, y]) => verdictsAgree(x, y)).length / n;
  let pe = 0;
  for (const x of VERDICTS) for (const y of VERDICTS) {
    if (!verdictsAgree(x, y)) continue;
    pe += (pairs.filter(([a]) => a === x).length / n) * (pairs.filter(([, b]) => b === y).length / n);
  }
  return pe === 1 ? (po === 1 ? 1 : null) : (po - pe) / (1 - pe);
}

export function verdictReport(key: PacketKey, verdicts: VerdictRecord[]): VerdictReport {
  const byKey = new Map<string, Map<string, VerdictValue>>();
  for (const v of verdicts) {
    const m = byKey.get(v.alertKey) ?? new Map<string, VerdictValue>();
    m.set(v.adjudicator, v.verdict);
    byKey.set(v.alertKey, m);
  }
  const adjudicators = [...new Set(verdicts.map((v) => v.adjudicator))].sort(textCompare);
  const agreement: AgreementRow[] = [];
  for (let i = 0; i < adjudicators.length; i++) for (let j = i + 1; j < adjudicators.length; j++) {
    const a = adjudicators[i]!, b = adjudicators[j]!;
    const pairs: Array<[VerdictValue, VerdictValue]> = [];
    for (const m of byKey.values()) if (m.has(a) && m.has(b)) pairs.push([m.get(a)!, m.get(b)!]);
    const agreed = pairs.filter(([x, y]) => verdictsAgree(x, y)).length;
    agreement.push({ a, b, shared: pairs.length, agreed, rate: pairs.length ? agreed / pairs.length : null, kappa: kappa(pairs) });
  }
  const verdictOf = new Map(key.entries.map((e) => [e.alertKey, consensus([...(byKey.get(e.alertKey)?.values() ?? [])])]));
  const counts = Object.fromEntries(VERDICTS.map((v) => [v, 0])) as Record<VerdictValue, number>;
  for (const v of verdictOf.values()) if (v) counts[v]++;
  const det = new Map<string, AdjudicatedDetector>();
  for (const e of key.entries) for (const m of e.members) {
    const k = `${m.detector}\u0000${m.detectorHash}`;
    const d = det.get(k) ?? { detector: m.detector, detectorHash: m.detectorHash, pooled: 0, adjudicated: 0, positive: 0, negative: 0, unclear: 0, precision: null, pairs: 0, positivePairs: 0, negativePairs: 0, pairPrecision: null };
    d.pooled++;
    const v = verdictOf.get(e.alertKey);
    if (v) {
      d.adjudicated++;
      if (v === "unclear") d.unclear++;
      else if (POSITIVE.has(v)) d.positive++;
      else d.negative++;
    }
    det.set(k, d);
  }
  // Per (detector, pair): 2 positive, 1 negative, 0 unclear or not adjudicated.
  const pairOutcome = new Map<string, Map<string, number>>();
  for (const e of key.entries) for (const m of e.members) {
    const k = `${m.detector}\u0000${m.detectorHash}`;
    const v = verdictOf.get(e.alertKey);
    const rank = !v || v === "unclear" ? 0 : POSITIVE.has(v) ? 2 : 1;
    const pairs = pairOutcome.get(k) ?? new Map<string, number>();
    const pk = pairId(e.pair);
    pairs.set(pk, Math.max(pairs.get(pk) ?? 0, rank));
    pairOutcome.set(k, pairs);
  }
  const detectors = [...det].map(([k, d]) => {
    const ranks = [...(pairOutcome.get(k)?.values() ?? [])];
    const positivePairs = ranks.filter((r) => r === 2).length, negativePairs = ranks.filter((r) => r === 1).length;
    return {
      ...d, precision: d.positive + d.negative > 0 ? d.positive / (d.positive + d.negative) : null,
      pairs: ranks.length, positivePairs, negativePairs, pairPrecision: positivePairs + negativePairs > 0 ? positivePairs / (positivePairs + negativePairs) : null,
    };
  })
    .sort((a, b) => textCompare(a.detector, b.detector) || textCompare(a.detectorHash, b.detectorHash));
  const cls = new Map<string, AdjudicatedClass>();
  for (const e of key.entries) for (const m of e.members) {
    const alertClass = m.alertClass ?? alertClassOf(m);
    const k = `${m.detector}\u0000${m.detectorHash}\u0000${alertClass}`;
    const c = cls.get(k) ?? { detector: m.detector, detectorHash: m.detectorHash, alertClass, pooled: 0, adjudicated: 0, positive: 0, misclassified: 0, negative: 0, unclear: 0, precision: null };
    c.pooled++;
    const v = verdictOf.get(e.alertKey);
    if (v) {
      c.adjudicated++;
      if (v === "unclear") c.unclear++;
      else if (classesOfVerdict(v).includes(alertClass)) c.positive++;
      else if (POSITIVE.has(v)) c.misclassified++;
      else c.negative++;
    }
    cls.set(k, c);
  }
  const classes = [...cls.values()].map((c) => {
    const judged = c.positive + c.misclassified + c.negative;
    return { ...c, precision: judged > 0 ? c.positive / judged : null };
  }).sort((a, b) => textCompare(a.detector, b.detector) || textCompare(a.detectorHash, b.detectorHash) || textCompare(a.alertClass, b.alertClass));
  return {
    format: "session-intel-verdicts/2", adjudicators, entries: key.entries.length,
    adjudicatedEntries: [...verdictOf.values()].filter(Boolean).length, agreement, consensus: counts, detectors, classes,
  };
}

const pct = (v: number | null) => v === null ? "n/a" : `${(v * 100).toFixed(1)}%`;
export function verdictMarkdown(r: VerdictReport): string {
  const lines = [
    "# Adjudication", "", `${r.adjudicatedEntries} of ${r.entries} pooled alerts adjudicated by ${r.adjudicators.length} adjudicators.`, "",
    "## Agreement", "", "| A | B | Shared | Agreed | Rate | Cohen's kappa |", "|---|---|---|---|---|---|",
    ...r.agreement.map((a) => `| ${a.a} | ${a.b} | ${a.shared} | ${a.agreed} | ${pct(a.rate)} | ${a.kappa === null ? "n/a" : a.kappa.toFixed(2)} |`),
    "", "## Precision from consensus verdicts (unclear excluded)", "", "| Detector | Hash | Pooled | Adjudicated | Positive | Negative | Unclear | Precision per entry | Pairs (positive / negative) | Precision per pair |", "|---|---|---|---|---|---|---|---|---|---|",
    ...r.detectors.map((d) => `| ${d.detector} | ${d.detectorHash.slice(0, 12)} | ${d.pooled} | ${d.adjudicated} | ${d.positive} | ${d.negative} | ${d.unclear} | ${pct(d.precision)} | ${d.pairs} (${d.positivePairs} / ${d.negativePairs}) | ${pct(d.pairPrecision)} |`),
    "", "## Precision per alert class (the verdict must confirm the class; unclear excluded)", "",
    "| Detector | Hash | Class | Pooled | Adjudicated | Positive | Misclassified | Negative | Unclear | Precision |", "|---|---|---|---|---|---|---|---|---|---|",
    ...r.classes.map((c) => `| ${c.detector} | ${c.detectorHash.slice(0, 12)} | ${c.alertClass} | ${c.pooled} | ${c.adjudicated} | ${c.positive} | ${c.misclassified} | ${c.negative} | ${c.unclear} | ${pct(c.precision)} |`),
    "", "## Consensus", "", ...Object.entries(r.consensus).map(([k, v]) => `- ${k}: ${v}`),
  ];
  return `${lines.join("\n")}\n`;
}
