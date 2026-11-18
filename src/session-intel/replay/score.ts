/**
 * Scores replay alerts against labels (docs/session-intel-replay.md, "Scoring"), per detector, per
 * (urgency, kind) row and per alert class, in two modes. Held-out labels are scored only when asked, and at
 * most once per detector hash (a scored-hashes ledger in the output directory) unless forced.
 *
 * For a split S, an alert whose pair (order-insensitive) is an S incident is a pair match. In pair-match mode
 * that is a true positive; in class-match mode it is one only when an S incident on the pair has the alert's
 * class (classes.ts), and otherwise `misclassified`. Anything else in scope is a false positive, counted as a
 * hard-negative hit or as unlabeled. Held-out scoring puts alerts on dev-labeled pairs out of scope; dev
 * scoring never reads held-out labels, so an alert on a held-out pair is unlabeled there. Unlabeled alerts
 * count against the split, so precision is a lower bound until adjudicated.
 */
import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { ALERT_CLASSES, alertClassOf, classOfIncidentKind, type AlertClass } from "./classes.ts";
import type { ReplayAlert } from "./detector.ts";
import type { HardNegative, Incident, Labels, Split } from "./labels.ts";
import type { ReplayResult } from "./replay.ts";
import { median, pairId, sha256Hex, textCompare, writeAtomic } from "./util.ts";

export const DEFAULT_BUDGET_PER_100 = 5;
export const LEDGER_FILE = "scored-hashes.json";
export const LEDGER_FORMAT = "session-intel-scored-hashes/1";
export const SCORE_FORMAT = "session-intel-score/2";

/** pair-match: the pair only (the M0 behaviour). class-match: the pair and the class. */
export type ScoreMode = "pair" | "class";

export interface RowScore {
  detector: string;
  urgency: "high" | "low" | "all";
  kind: string;
  /** The alert class the row covers, or "all" (a total, or a kind whose alerts span classes). */
  alertClass: AlertClass | "all";
  /** Alerts in scope for the split. */
  alerts: number;
  outOfScope: number;
  truePositives: number;
  /** Hard-negative hits plus unlabeled alerts; misclassified alerts are counted on their own. */
  falsePositives: number;
  hardNegativeHits: number;
  unlabeled: number;
  /** class-match only: the pair is an incident of the split, but none of those incidents has the alert's class. */
  misclassified: number;
  /** truePositives / alerts, so misclassified alerts are in the denominator. */
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

export interface TopKScore {
  budgetPer100: number; k: number; alerts: number; truePositives: number; misclassified: number;
  precision: number | null; detected: number; recall: number | null; beforeHarm: number;
}

export interface ModeScore {
  mode: ScoreMode;
  total: RowScore;
  /** Per (urgency, kind). */
  rows: RowScore[];
  /**
   * Per alert class. In class-match, recall is over the split's incidents of that class; in pair-match, over all
   * of the split's incidents.
   */
  classRows: RowScore[];
  topK: TopKScore;
}

/** Confusion columns: the labeled classes, plus `unlabeled` for alerts on pairs with no label. */
export const CONFUSION_COLUMNS = [...ALERT_CLASSES, "unlabeled"] as const;
export type ConfusionColumn = typeof CONFUSION_COLUMNS[number];
/** In-scope alerts by alert class (rows) against what their pair is labeled (columns). */
export interface Confusion { rows: AlertClass[]; columns: readonly ConfusionColumn[]; counts: Record<AlertClass, Record<ConfusionColumn, number>> }

export interface DetectorScore { detector: string; hash: string; pairMatch: ModeScore; classMatch: ModeScore; confusion: Confusion }

export interface ScoreReport {
  format: typeof SCORE_FORMAT;
  split: Split;
  sessions: number;
  manifestSha256: string;
  labelsSha256: string;
  incidents: number;
  detectors: DetectorScore[];
}

type Scope =
  | { scope: "tp"; incidents: Incident[] }
  | { scope: "hard_negative"; reasons: HardNegative["reason"][] }
  | { scope: "unlabeled" }
  | { scope: "out" };

/** An alert's pair-level scope for a split; both modes start here. */
export function classify(alert: Pick<ReplayAlert, "pair">, labels: Labels, split: Split): Scope {
  const id = pairId(alert.pair);
  // Dev scoring never sees held-out labels: an alert on a held-out pair is just unlabeled there, so no dev
  // report (not even an out-of-scope count or a confusion cell) reveals which pairs are held out.
  if (split === "dev") {
    const mine = labels.incidents.filter((x) => x.split === "dev" && pairId(x.pair) === id);
    if (mine.length) return { scope: "tp", incidents: mine };
    const negatives = labels.hardNegatives.filter((n) => n.split !== "heldout" && pairId(n.pair) === id);
    if (negatives.length) return { scope: "hard_negative", reasons: negatives.map((n) => n.reason) };
    return { scope: "unlabeled" };
  }
  const incidents = labels.incidents.filter((x) => pairId(x.pair) === id);
  const mine = incidents.filter((x) => x.split === split);
  if (mine.length) return { scope: "tp", incidents: mine };
  if (incidents.length) return { scope: "out" };
  const negatives = labels.hardNegatives.filter((n) => pairId(n.pair) === id);
  const inSplit = negatives.filter((n) => n.split === undefined || n.split === split);
  if (inSplit.length) return { scope: "hard_negative", reasons: inSplit.map((n) => n.reason) };
  if (negatives.length) return { scope: "out" };
  return { scope: "unlabeled" };
}

/** One alert's outcome in a mode: the incidents it detects, or why it does not count. */
export type Outcome =
  | { scope: "tp"; incidents: Incident[] }
  | { scope: "misclassified"; incidents: Incident[] }
  | { scope: "hard_negative" }
  | { scope: "unlabeled" }
  | { scope: "out" };

export function outcome(alert: Pick<ReplayAlert, "pair" | "kind" | "urgency" | "alertClass">, labels: Labels, split: Split, mode: ScoreMode): Outcome {
  const v = classify(alert, labels, split);
  if (v.scope === "hard_negative") return { scope: "hard_negative" };
  if (v.scope !== "tp" || mode === "pair") return v;
  const cls = alertClassOf(alert);
  const same = v.incidents.filter((x) => classOfIncidentKind(x.kind) === cls);
  return same.length ? { scope: "tp", incidents: same } : { scope: "misclassified", incidents: v.incidents };
}

const ratio = (n: number, d: number) => d > 0 ? n / d : null;

interface RowKey { urgency: RowScore["urgency"]; kind: string; alertClass: RowScore["alertClass"] }

function scoreRow(detector: string, key: RowKey, alerts: ReplayAlert[], labels: Labels, split: Split, sessions: number, mode: ScoreMode, recallOver: Incident[]): RowScore {
  let tp = 0, hn = 0, un = 0, out = 0, mis = 0;
  const firstSent = new Map<string, number>();
  for (const a of alerts) {
    const v = outcome(a, labels, split, mode);
    if (v.scope === "out") { out++; continue; }
    if (v.scope === "hard_negative") { hn++; continue; }
    if (v.scope === "unlabeled") { un++; continue; }
    if (v.scope === "misclassified") { mis++; continue; }
    tp++;
    const at = Date.parse(a.sentAt);
    for (const x of v.incidents) firstSent.set(x.id, Math.min(firstSent.get(x.id) ?? at, at));
  }
  const leads = recallOver.filter((x) => firstSent.has(x.id)).map((x) => Date.parse(x.harmAt) - firstSent.get(x.id)!).sort((a, b) => a - b);
  const inScope = tp + hn + un + mis;
  return {
    detector, ...key, alerts: inScope, outOfScope: out, truePositives: tp, falsePositives: hn + un, hardNegativeHits: hn, unlabeled: un,
    misclassified: mis, precision: ratio(tp, inScope), incidents: recallOver.length, detected: leads.length, recall: ratio(leads.length, recallOver.length),
    beforeHarm: leads.filter((l) => l > 0).length, afterHarm: leads.filter((l) => l <= 0).length,
    leadTimesMs: leads, medianLeadMs: median(leads), alertsPer100Sessions: sessions > 0 ? (inScope * 100) / sessions : 0,
  };
}

/** The top-K order: HIGH first, then the earliest sentAt, then the id. Shared with the adjudication pool. */
export const rankAlerts = (alerts: ReplayAlert[]) => [...alerts].sort((a, b) => (a.urgency === b.urgency ? 0 : a.urgency === "high" ? -1 : 1)
  || textCompare(a.sentAt, b.sentAt) || textCompare(a.alertId, b.alertId));
export const budgetK = (sessions: number, budgetPer100: number) => sessions > 0 ? Math.max(1, Math.ceil((budgetPer100 * sessions) / 100)) : 0;

const urgencyOrder = (a: RowScore, b: RowScore) => (a.urgency === b.urgency ? 0 : a.urgency === "high" ? -1 : 1);

function modeScore(detector: string, alerts: ReplayAlert[], labels: Labels, split: Split, sessions: number, mode: ScoreMode, budgetPer100: number): ModeScore {
  const inSplit = labels.incidents.filter((x) => x.split === split);
  const groups = new Map<string, ReplayAlert[]>();
  for (const a of alerts) {
    const k = `${a.urgency}\u0000${a.kind}`;
    groups.set(k, [...(groups.get(k) ?? []), a]);
  }
  const rows = [...groups.values()].map((mine) => {
    const classes = [...new Set(mine.map(alertClassOf))];
    return scoreRow(detector, { urgency: mine[0]!.urgency, kind: mine[0]!.kind, alertClass: classes.length === 1 ? classes[0]! : "all" },
      mine, labels, split, sessions, mode, inSplit);
  }).sort((a, b) => urgencyOrder(a, b) || textCompare(a.kind, b.kind));
  const classRows = ALERT_CLASSES.filter((c) => alerts.some((a) => alertClassOf(a) === c)).map((c) =>
    scoreRow(detector, { urgency: "all", kind: "all", alertClass: c }, alerts.filter((a) => alertClassOf(a) === c), labels, split, sessions, mode,
      mode === "class" ? inSplit.filter((x) => classOfIncidentKind(x.kind) === c) : inSplit));
  const k = budgetK(sessions, budgetPer100);
  const scoped = rankAlerts(alerts).filter((a) => classify(a, labels, split).scope !== "out");
  const top = scoreRow(detector, { urgency: "all", kind: "top-k", alertClass: "all" }, scoped.slice(0, k), labels, split, sessions, mode, inSplit);
  return {
    mode, total: scoreRow(detector, { urgency: "all", kind: "all", alertClass: "all" }, alerts, labels, split, sessions, mode, inSplit), rows, classRows,
    topK: {
      budgetPer100, k, alerts: top.alerts, truePositives: top.truePositives, misclassified: top.misclassified,
      precision: top.precision, detected: top.detected, recall: top.recall, beforeHarm: top.beforeHarm,
    },
  };
}

/**
 * Each in-scope alert counts once per distinct class its pair is labeled with: an incident's class; a hard
 * negative's `planned_fleet`, or `none` for the other reasons; or `unlabeled`.
 */
export function confusion(alerts: ReplayAlert[], labels: Labels, split: Split): Confusion {
  const counts = Object.fromEntries(ALERT_CLASSES.map((c) => [c, Object.fromEntries(CONFUSION_COLUMNS.map((col) => [col, 0]))])) as Confusion["counts"];
  for (const a of alerts) {
    const v = classify(a, labels, split);
    if (v.scope === "out") continue;
    const cols = new Set<ConfusionColumn>(v.scope === "tp" ? v.incidents.map((x) => classOfIncidentKind(x.kind))
      : v.scope === "hard_negative" ? v.reasons.map((r) => r === "planned_fleet" ? "planned_fleet" : "none") : ["unlabeled"]);
    for (const col of cols) counts[alertClassOf(a)][col]++;
  }
  return { rows: [...ALERT_CLASSES], columns: CONFUSION_COLUMNS, counts };
}

export function scoreReplay(result: ReplayResult, labels: Labels, opts: { split: Split; labelsSha256: string; budgetPer100?: number }): ScoreReport {
  const budgetPer100 = opts.budgetPer100 ?? DEFAULT_BUDGET_PER_100;
  if (!(budgetPer100 > 0 && budgetPer100 <= 100)) throw new Error("score: the budget must be in (0, 100] alerts per 100 sessions");
  const sessions = result.snapshot.sessions;
  const detectors = result.detectors.map((d): DetectorScore => {
    const mine = result.alerts.filter((a) => a.detector === d.name);
    return {
      detector: d.name, hash: d.hash,
      pairMatch: modeScore(d.name, mine, labels, opts.split, sessions, "pair", budgetPer100),
      classMatch: modeScore(d.name, mine, labels, opts.split, sessions, "class", budgetPer100),
      confusion: confusion(mine, labels, opts.split),
    };
  });
  return {
    format: SCORE_FORMAT, split: opts.split, sessions, manifestSha256: result.snapshot.manifestSha256, labelsSha256: opts.labelsSha256,
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
const HEADER = "| Urgency | Kind | Class | Alerts | TP | Misclassified | FP (hard neg / unlabeled) | Precision | Recall | Before / after harm | Median lead | Alerts per 100 sessions |";
const RULE = "|---|---|---|---|---|---|---|---|---|---|---|---|";
const rowLine = (row: RowScore) => `| ${row.urgency} | ${row.kind} | ${row.alertClass} | ${row.alerts} | ${row.truePositives} | ${row.misclassified} | ${row.falsePositives} (${row.hardNegativeHits} / ${row.unlabeled}) | ${pct(row.precision)} | ${pct(row.recall)} | ${row.beforeHarm} / ${row.afterHarm} | ${mins(row.medianLeadMs)} | ${row.alertsPer100Sessions.toFixed(2)} |`;

export function scoreMarkdown(r: ScoreReport): string {
  const lines = [
    `# Replay score (${r.split})`, "",
    `Snapshot manifest ${r.manifestSha256.slice(0, 16)}, labels ${r.labelsSha256.slice(0, 16)}, ${r.sessions} sessions, ${r.incidents} ${r.split} incidents.`,
    "Unlabeled alerts count as false positives, so precision is a lower bound until they are adjudicated.",
    "pair-match needs the pair only. class-match also needs the class; a pair match of another class is misclassified, not a true positive.", "",
  ];
  for (const d of r.detectors) {
    lines.push(`## ${d.detector} (${d.hash.slice(0, 12)})`, "");
    for (const m of [d.pairMatch, d.classMatch]) {
      lines.push(`### ${m.mode === "pair" ? "pair-match" : "class-match"}`, "", HEADER, RULE, ...[...m.rows, ...m.classRows, m.total].map(rowLine), "",
        `Top ${m.topK.k} at ${m.topK.budgetPer100} per 100 sessions: ${m.topK.truePositives} of ${m.topK.alerts} true (${pct(m.topK.precision)}), ${m.topK.misclassified} misclassified, recall ${pct(m.topK.recall)}, ${m.topK.beforeHarm} before harm.`, "");
    }
    const c = d.confusion;
    const rows = c.rows.filter((cls) => c.columns.some((col) => c.counts[cls][col] > 0));
    lines.push("### Confusion (alert class by labeled class)", "", `| Alert class | ${c.columns.join(" | ")} |`, `|---|${c.columns.map(() => "---").join("|")}|`,
      ...rows.map((cls) => `| ${cls} | ${c.columns.map((col) => c.counts[cls][col]).join(" | ")} |`), "");
  }
  return `${lines.join("\n")}\n`;
}
