/**
 * Scores replay alerts against labels (docs/session-intel-replay.md, "Scoring"), per detector and per
 * (urgency, kind) row. Held-out labels are scored only when asked, and at most once per detector hash
 * (a scored-hashes ledger in the output directory) unless forced.
 *
 * For a split S, an alert whose pair (order-insensitive) is an S incident is a true positive; anything else in
 * scope is a false positive, counted as a hard-negative hit or as unlabeled. Held-out scoring puts alerts on
 * dev-labeled pairs out of scope; dev scoring never reads held-out labels, so an alert on a held-out pair is
 * unlabeled there. Unlabeled alerts count against the split, so precision is a lower bound until adjudicated.
 */
import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import type { ReplayAlert } from "./detector.ts";
import type { Incident, Labels, Split } from "./labels.ts";
import type { ReplayResult } from "./replay.ts";
import { median, pairId, sha256Hex, textCompare, writeAtomic } from "./util.ts";

export const DEFAULT_BUDGET_PER_100 = 5;
export const LEDGER_FILE = "scored-hashes.json";
export const LEDGER_FORMAT = "session-intel-scored-hashes/1";

export interface RowScore {
  detector: string;
  urgency: "high" | "low" | "all";
  kind: string;
  /** Alerts in scope for the split. */
  alerts: number;
  outOfScope: number;
  truePositives: number;
  falsePositives: number;
  hardNegativeHits: number;
  unlabeled: number;
  precision: number | null;
  incidents: number;
  detected: number;
  recall: number | null;
  /** Detected incidents whose first matching alert was sent before harmAt, and at or after it. */
  beforeHarm: number;
  afterHarm: number;
  /** harmAt - first sentAt per detected incident, sorted ascending; negative is after the harm. */
  leadTimesMs: number[];
  medianLeadMs: number | null;
  alertsPer100Sessions: number;
}

export interface TopKScore { budgetPer100: number; k: number; alerts: number; truePositives: number; precision: number | null; detected: number; recall: number | null; beforeHarm: number }

export interface DetectorScore { detector: string; hash: string; total: RowScore; rows: RowScore[]; topK: TopKScore }

export interface ScoreReport {
  format: "session-intel-score/1";
  split: Split;
  sessions: number;
  manifestSha256: string;
  labelsSha256: string;
  incidents: number;
  detectors: DetectorScore[];
}

type Verdict = { scope: "tp"; incidents: Incident[] } | { scope: "hard_negative" } | { scope: "unlabeled" } | { scope: "out" };

export function classify(alert: Pick<ReplayAlert, "pair">, labels: Labels, split: Split): Verdict {
  const id = pairId(alert.pair);
  // Dev scoring never sees held-out labels: an alert on a held-out pair is just unlabeled there, so no dev
  // report (not even an out-of-scope count) reveals which pairs are held out.
  if (split === "dev") {
    const mine = labels.incidents.filter((x) => x.split === "dev" && pairId(x.pair) === id);
    if (mine.length) return { scope: "tp", incidents: mine };
    if (labels.hardNegatives.some((n) => n.split !== "heldout" && pairId(n.pair) === id)) return { scope: "hard_negative" };
    return { scope: "unlabeled" };
  }
  const incidents = labels.incidents.filter((x) => pairId(x.pair) === id);
  const mine = incidents.filter((x) => x.split === split);
  if (mine.length) return { scope: "tp", incidents: mine };
  if (incidents.length) return { scope: "out" };
  const negatives = labels.hardNegatives.filter((n) => pairId(n.pair) === id);
  if (negatives.some((n) => n.split === undefined || n.split === split)) return { scope: "hard_negative" };
  if (negatives.length) return { scope: "out" };
  return { scope: "unlabeled" };
}

const ratio = (n: number, d: number) => d > 0 ? n / d : null;

function scoreRow(detector: string, urgency: RowScore["urgency"], kind: string, alerts: ReplayAlert[], labels: Labels, split: Split, sessions: number): RowScore {
  const inSplit = labels.incidents.filter((x) => x.split === split);
  let tp = 0, hn = 0, un = 0, out = 0;
  const firstSent = new Map<string, number>();
  for (const a of alerts) {
    const v = classify(a, labels, split);
    if (v.scope === "out") { out++; continue; }
    if (v.scope === "hard_negative") { hn++; continue; }
    if (v.scope === "unlabeled") { un++; continue; }
    tp++;
    const at = Date.parse(a.sentAt);
    for (const x of v.incidents) firstSent.set(x.id, Math.min(firstSent.get(x.id) ?? at, at));
  }
  const leads = inSplit.filter((x) => firstSent.has(x.id)).map((x) => Date.parse(x.harmAt) - firstSent.get(x.id)!).sort((a, b) => a - b);
  const inScope = tp + hn + un;
  return {
    detector, urgency, kind, alerts: inScope, outOfScope: out, truePositives: tp, falsePositives: hn + un, hardNegativeHits: hn, unlabeled: un,
    precision: ratio(tp, inScope), incidents: inSplit.length, detected: leads.length, recall: ratio(leads.length, inSplit.length),
    beforeHarm: leads.filter((l) => l > 0).length, afterHarm: leads.filter((l) => l <= 0).length,
    leadTimesMs: leads, medianLeadMs: median(leads), alertsPer100Sessions: sessions > 0 ? (inScope * 100) / sessions : 0,
  };
}

/** The top-K order: HIGH first, then the earliest sentAt, then the id. Shared with the adjudication pool. */
export const rankAlerts = (alerts: ReplayAlert[]) => [...alerts].sort((a, b) => (a.urgency === b.urgency ? 0 : a.urgency === "high" ? -1 : 1)
  || textCompare(a.sentAt, b.sentAt) || textCompare(a.alertId, b.alertId));
export const budgetK = (sessions: number, budgetPer100: number) => sessions > 0 ? Math.max(1, Math.ceil((budgetPer100 * sessions) / 100)) : 0;

export function scoreReplay(result: ReplayResult, labels: Labels, opts: { split: Split; labelsSha256: string; budgetPer100?: number }): ScoreReport {
  const budgetPer100 = opts.budgetPer100 ?? DEFAULT_BUDGET_PER_100;
  if (!(budgetPer100 > 0 && budgetPer100 <= 100)) throw new Error("score: the budget must be in (0, 100] alerts per 100 sessions");
  const sessions = result.snapshot.sessions;
  const detectors = result.detectors.map((d): DetectorScore => {
    const mine = result.alerts.filter((a) => a.detector === d.name);
    const rowKeys = [...new Set(mine.map((a) => `${a.urgency}\u0000${a.kind}`))].sort(textCompare);
    const rows = rowKeys.map((k) => {
      const [urgency, kind] = k.split("\u0000") as ["high" | "low", string];
      return scoreRow(d.name, urgency, kind, mine.filter((a) => a.urgency === urgency && a.kind === kind), labels, opts.split, sessions);
    }).sort((a, b) => (a.urgency === b.urgency ? 0 : a.urgency === "high" ? -1 : 1) || textCompare(a.kind, b.kind));
    const k = budgetK(sessions, budgetPer100);
    const scoped = rankAlerts(mine).filter((a) => classify(a, labels, opts.split).scope !== "out");
    const top = scoreRow(d.name, "all", "top-k", scoped.slice(0, k), labels, opts.split, sessions);
    return {
      detector: d.name, hash: d.hash, total: scoreRow(d.name, "all", "all", mine, labels, opts.split, sessions), rows,
      topK: { budgetPer100, k, alerts: top.alerts, truePositives: top.truePositives, precision: top.precision, detected: top.detected, recall: top.recall, beforeHarm: top.beforeHarm },
    };
  });
  return {
    format: "session-intel-score/1", split: opts.split, sessions, manifestSha256: result.snapshot.manifestSha256, labelsSha256: opts.labelsSha256,
    incidents: labels.incidents.filter((x) => x.split === opts.split).length, detectors,
  };
}

export interface LedgerEntry { detector: string; detectorHash: string; split: "heldout"; labelsSha256: string; manifestSha256: string; scoredAt: string; forced: boolean }
export interface Ledger { format: typeof LEDGER_FORMAT; entries: LedgerEntry[] }

export function readLedger(outDir: string): Ledger {
  const path = join(outDir, LEDGER_FILE);
  if (!existsSync(path)) return { format: LEDGER_FORMAT, entries: [] };
  const raw = JSON.parse(readFileSync(path, "utf8")) as Ledger;
  if (raw?.format !== LEDGER_FORMAT || !Array.isArray(raw.entries)) throw new Error(`score: ${LEDGER_FILE} is not a ${LEDGER_FORMAT} ledger`);
  return raw;
}

/**
 * The held-out-once rule: refuses (before anything is written) when a detector hash in `result` was already
 * scored on held-out, unless `force`; then records every hash. Dev scoring never touches the ledger.
 */
export async function recordHeldoutScoring(outDir: string, result: ReplayResult, labelsSha256: string, opts: { force?: boolean; now?: Date } = {}): Promise<Ledger> {
  const ledger = readLedger(outDir);
  const done = new Set(ledger.entries.filter((e) => e.split === "heldout").map((e) => e.detectorHash));
  const repeat = result.detectors.filter((d) => done.has(d.hash));
  if (repeat.length && !opts.force)
    throw new Error(`score: held-out was already scored for ${repeat.map((d) => `${d.name} (${d.hash.slice(0, 12)})`).join(", ")}; a detector is scored on held-out once (--force overrides and is recorded)`);
  const scoredAt = (opts.now ?? new Date()).toISOString();
  const next: Ledger = {
    format: LEDGER_FORMAT,
    entries: [...ledger.entries, ...result.detectors.map((d): LedgerEntry => ({
      detector: d.name, detectorHash: d.hash, split: "heldout", labelsSha256, manifestSha256: result.snapshot.manifestSha256, scoredAt, forced: done.has(d.hash),
    }))],
  };
  await writeAtomic(join(outDir, LEDGER_FILE), `${JSON.stringify(next, null, 2)}\n`);
  return next;
}

export const labelsDigest = (bytes: string | Uint8Array) => sha256Hex(bytes);

const pct = (v: number | null) => v === null ? "n/a" : `${(v * 100).toFixed(1)}%`;
const mins = (ms: number | null) => ms === null ? "n/a" : `${(ms / 60_000).toFixed(1)}m`;

export function scoreMarkdown(r: ScoreReport): string {
  const lines = [
    `# Replay score (${r.split})`, "",
    `Snapshot manifest ${r.manifestSha256.slice(0, 16)}, labels ${r.labelsSha256.slice(0, 16)}, ${r.sessions} sessions, ${r.incidents} ${r.split} incidents.`,
    "Unlabeled alerts count as false positives, so precision is a lower bound until they are adjudicated.", "",
  ];
  for (const d of r.detectors) {
    lines.push(`## ${d.detector} (${d.hash.slice(0, 12)})`, "",
      "| Urgency | Kind | Alerts | TP | FP (hard neg / unlabeled) | Precision | Recall | Before / after harm | Median lead | Alerts per 100 sessions |",
      "|---|---|---|---|---|---|---|---|---|---|");
    for (const row of [...d.rows, d.total]) lines.push(`| ${row.urgency} | ${row.kind} | ${row.alerts} | ${row.truePositives} | ${row.falsePositives} (${row.hardNegativeHits} / ${row.unlabeled}) | ${pct(row.precision)} | ${pct(row.recall)} | ${row.beforeHarm} / ${row.afterHarm} | ${mins(row.medianLeadMs)} | ${row.alertsPer100Sessions.toFixed(2)} |`);
    lines.push("", `Top ${d.topK.k} at ${d.topK.budgetPer100} per 100 sessions: ${d.topK.truePositives} of ${d.topK.alerts} true (${pct(d.topK.precision)}), recall ${pct(d.topK.recall)}, ${d.topK.beforeHarm} before harm.`, "");
  }
  return `${lines.join("\n")}\n`;
}
