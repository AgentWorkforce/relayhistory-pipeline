/**
 * Scores replay alerts against labels (docs/session-intel-replay.md, "Scoring"), per detector, per
 * (urgency, kind) row and per alert class, in two modes. Held-out labels are scored only when asked, and at
 * most once per detector hash (a scored-hashes ledger in the output directory) unless forced.
 *
 * For a split S, an alert whose pair (order-insensitive) is an S incident is a pair match. In pair-match mode
 * that is a true positive; in class-match mode it is one only when an S incident on the pair has the alert's
 * class (classes.ts), and otherwise `misclassified`. A hard negative is a false positive in pair-match; in
 * class-match it is a true positive (with no incident to credit) for an alert of the negative's own class (an
 * `overlap` alert on a benign overlap, a `planned_fleet` alert on a planned fleet), and a false positive for
 * every other class. Anything else in scope is a false positive, counted as a hard-negative hit or as unlabeled. Held-out scoring puts alerts on dev-labeled pairs out of scope; dev
 * scoring never reads held-out labels, so an alert on a held-out pair is unlabeled there. Unlabeled alerts
 * count against the split, so precision is a lower bound until adjudicated.
 */
import { existsSync, readFileSync } from "node:fs";
import { mkdir, open, rm, unlink } from "node:fs/promises";
import { join } from "node:path";
import { ALERT_CLASSES, alertClassOf, CLASS_MAP_SHA256, classOfHardNegative, classOfIncidentKind, type AlertClass } from "./classes.ts";
import type { ReplayAlert } from "./detector.ts";
import type { HardNegative, Incident, Labels, Split } from "./labels.ts";
import type { ReplayResult } from "./replay.ts";
import { isoTime, median, pairId, sha256Hex, textCompare, writeAtomic, writeNew } from "./util.ts";

export const DEFAULT_BUDGET_PER_100 = 5;
export const LEDGER_FILE = "scored-hashes.json";
export const LEDGER_FORMAT = "session-intel-scored-hashes/1";
export const SCORE_FORMAT = "session-intel-score/2";

/**
 * sha256 of the scorer's code (score.ts, classes.ts, labels.ts, and util.ts, whose pair identity, median and
 * timestamp helpers decide scores): recorded in every report and ledger entry. util.ts also holds the atomic
 * writers, so an edit to them changes this hash too; that only over-reports a scorer change, never hides one.
 */
export const SCORER_SOURCES = ["./score.ts", "./classes.ts", "./labels.ts", "./util.ts"] as const;
export const SCORER_SHA256 = sha256Hex(SCORER_SOURCES
  .map((f) => sha256Hex(readFileSync(new URL(f, import.meta.url)))).join("\n"));

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
  /** Per alert: truePositives / alerts, so misclassified alerts are in the denominator. */
  alertPrecision: number | null;
  /**
   * Per (detector, pair): several alerts on one pair (a LOW then a HIGH, two artifacts) count once. A pair is a
   * true positive when any of its alerts in the row is; otherwise misclassified when any is; otherwise false.
   */
  pairs: number;
  truePositivePairs: number;
  misclassifiedPairs: number;
  pairPrecision: number | null;
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
  alertPrecision: number | null; pairs: number; truePositivePairs: number; pairPrecision: number | null;
  detected: number; recall: number | null; beforeHarm: number;
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
  /** Per (alert class, urgency): the rows the spec's bars apply to. Recall as for classRows. */
  classUrgencyRows: RowScore[];
  topK: TopKScore;
}

/** Confusion columns: the labeled classes, plus `unlabeled` for alerts on pairs with no label. */
export const CONFUSION_COLUMNS = [...ALERT_CLASSES, "unlabeled"] as const;
export type ConfusionColumn = typeof CONFUSION_COLUMNS[number];
/** In-scope alerts by alert class (rows) against what their pair is labeled (columns). */
export interface Confusion { rows: AlertClass[]; columns: readonly ConfusionColumn[]; counts: Record<AlertClass, Record<ConfusionColumn, number>> }

export interface DetectorScore {
  detector: string; hash: string; pairMatch: ModeScore; classMatch: ModeScore; confusion: Confusion;
  /** The replay's diagnostics for the detector (for the advisor: dmSent, written, ...), so the true DM volume is visible. */
  diagnostics: Record<string, number>;
}

export interface ScoreReport {
  format: typeof SCORE_FORMAT;
  split: Split;
  /** sha256 of classes.ts: the class map every class-match number here depends on. */
  classMapSha256: string;
  /** sha256 of score.ts, classes.ts and labels.ts. */
  scorerSha256: string;
  sessions: number;
  manifestSha256: string;
  labelsSha256: string;
  incidents: number;
  /** The replay's incomplete reasons (dev scoring reports them; held-out scoring refuses them). */
  incomplete: string[];
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
  if (v.scope === "hard_negative") {
    // class-match: the negative says what the pair is; an alert of exactly that class is correct (never `none`).
    const cls = mode === "class" ? alertClassOf(alert) : null;
    return cls && cls !== "none" && v.reasons.some((r) => classOfHardNegative(r) === cls) ? { scope: "tp", incidents: [] } : { scope: "hard_negative" };
  }
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
  // The best outcome per pair: tp over misclassified over a false positive.
  const pairRank = new Map<string, number>();
  const rankPair = (a: ReplayAlert, rank: number) => { const k = pairId(a.pair); pairRank.set(k, Math.max(pairRank.get(k) ?? 0, rank)); };
  for (const a of alerts) {
    const v = outcome(a, labels, split, mode);
    if (v.scope === "out") { out++; continue; }
    if (v.scope === "hard_negative") { hn++; rankPair(a, 0); continue; }
    if (v.scope === "unlabeled") { un++; rankPair(a, 0); continue; }
    if (v.scope === "misclassified") { mis++; rankPair(a, 1); continue; }
    tp++;
    rankPair(a, 2);
    const at = Date.parse(a.sentAt);
    for (const x of v.incidents) firstSent.set(x.id, Math.min(firstSent.get(x.id) ?? at, at));
  }
  const leads = recallOver.filter((x) => firstSent.has(x.id)).map((x) => Date.parse(x.harmAt) - firstSent.get(x.id)!).sort((a, b) => a - b);
  const inScope = tp + hn + un + mis;
  return {
    detector, ...key, alerts: inScope, outOfScope: out, truePositives: tp, falsePositives: hn + un, hardNegativeHits: hn, unlabeled: un,
    misclassified: mis, alertPrecision: ratio(tp, inScope),
    pairs: pairRank.size, truePositivePairs: [...pairRank.values()].filter((r) => r === 2).length,
    misclassifiedPairs: [...pairRank.values()].filter((r) => r === 1).length,
    pairPrecision: ratio([...pairRank.values()].filter((r) => r === 2).length, pairRank.size), incidents: recallOver.length, detected: leads.length, recall: ratio(leads.length, recallOver.length),
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
  const classUrgencyRows = ALERT_CLASSES.flatMap((c) => (["high", "low"] as const).flatMap((u) => {
    const mine = alerts.filter((a) => alertClassOf(a) === c && a.urgency === u);
    return mine.length ? [scoreRow(detector, { urgency: u, kind: "all", alertClass: c }, mine, labels, split, sessions, mode,
      mode === "class" ? inSplit.filter((x) => classOfIncidentKind(x.kind) === c) : inSplit)] : [];
  }));
  const k = budgetK(sessions, budgetPer100);
  const scoped = rankAlerts(alerts).filter((a) => classify(a, labels, split).scope !== "out");
  const top = scoreRow(detector, { urgency: "all", kind: "top-k", alertClass: "all" }, scoped.slice(0, k), labels, split, sessions, mode, inSplit);
  return {
    mode, total: scoreRow(detector, { urgency: "all", kind: "all", alertClass: "all" }, alerts, labels, split, sessions, mode, inSplit), rows, classRows, classUrgencyRows,
    topK: {
      budgetPer100, k, alerts: top.alerts, truePositives: top.truePositives, misclassified: top.misclassified,
      alertPrecision: top.alertPrecision, pairs: top.pairs, truePositivePairs: top.truePositivePairs, pairPrecision: top.pairPrecision, detected: top.detected, recall: top.recall, beforeHarm: top.beforeHarm,
    },
  };
}

/**
 * Each in-scope alert counts once per distinct class its pair is labeled with: an incident's class; a hard
 * negative's class (benign_overlap is `overlap`, planned_fleet is `planned_fleet`, unrelated is `none`); or
 * `unlabeled`.
 */
export function confusion(alerts: ReplayAlert[], labels: Labels, split: Split): Confusion {
  const counts = Object.fromEntries(ALERT_CLASSES.map((c) => [c, Object.fromEntries(CONFUSION_COLUMNS.map((col) => [col, 0]))])) as Confusion["counts"];
  for (const a of alerts) {
    const v = classify(a, labels, split);
    if (v.scope === "out") continue;
    const cols = new Set<ConfusionColumn>(v.scope === "tp" ? v.incidents.map((x) => classOfIncidentKind(x.kind))
      : v.scope === "hard_negative" ? v.reasons.map(classOfHardNegative) : ["unlabeled"]);
    for (const col of cols) counts[alertClassOf(a)][col]++;
  }
  return { rows: [...ALERT_CLASSES], columns: CONFUSION_COLUMNS, counts };
}

export function scoreReplay(result: ReplayResult, labels: Labels, opts: { split: Split; labelsSha256: string; budgetPer100?: number }): ScoreReport {
  const budgetPer100 = opts.budgetPer100 ?? DEFAULT_BUDGET_PER_100;
  if (!(budgetPer100 > 0 && budgetPer100 <= 100)) throw new Error("score: the budget must be in (0, 100] alerts per 100 sessions");
  assertScorable(result, opts.split);
  const sessions = result.snapshot.sessions;
  const detectors = result.detectors.map((d): DetectorScore => {
    const mine = result.alerts.filter((a) => a.detector === d.name);
    return {
      detector: d.name, hash: d.hash,
      pairMatch: modeScore(d.name, mine, labels, opts.split, sessions, "pair", budgetPer100),
      classMatch: modeScore(d.name, mine, labels, opts.split, sessions, "class", budgetPer100),
      confusion: confusion(mine, labels, opts.split),
      diagnostics: { ...d.diagnostics },
    };
  });
  return {
    format: SCORE_FORMAT, split: opts.split, classMapSha256: CLASS_MAP_SHA256, scorerSha256: SCORER_SHA256, sessions, manifestSha256: result.snapshot.manifestSha256, labelsSha256: opts.labelsSha256,
    incidents: labels.incidents.filter((x) => x.split === opts.split).length, incomplete: [...result.incomplete], detectors,
  };
}

/** Held-out is scored only from a complete replay: a missing alert would be scored as a miss, once and for good. */
export function assertScorable(result: Pick<ReplayResult, "incomplete">, split: Split): void {
  if (split === "heldout" && result.incomplete.length)
    throw new Error(`score: the replay is incomplete (${result.incomplete.join("; ")}); held-out is never scored from an incomplete replay`);
}

/**
 * The detector-hash part of a held-out report's name: the first 12 hex of the detector's hash, or, for a
 * replay of several detectors, of the sha256 of their sorted hashes. A second detector, never scored, can then
 * be scored into the same --out (and so the same ledger) without a name clash.
 */
export function heldoutHash12(result: Pick<ReplayResult, "detectors">): string {
  const hashes = result.detectors.map((d) => d.hash).sort(textCompare);
  return (hashes.length === 1 ? hashes[0]! : sha256Hex(hashes.join("\n"))).slice(0, 12);
}

/**
 * Writes a held-out report pair, `report.heldout.<hash12>.{json,md}`, or for a forced repeat the first free
 * `report.heldout.<hash12>.forced-<n>.{json,md}`. Never overwrites a file; a half-written pair is removed. Returns
 * the base name and a function that removes the pair (run when the ledger entry then cannot be written).
 */
export async function writeHeldoutReports(outDir: string, result: Pick<ReplayResult, "detectors">, forced: boolean, json: string, md: string)
  : Promise<{ base: string; remove: () => Promise<void> }> {
  let base = `report.heldout.${heldoutHash12(result)}`;
  if (forced) {
    let n = 1;
    while (existsSync(join(outDir, `${base}.forced-${n}.json`)) || existsSync(join(outDir, `${base}.forced-${n}.md`))) n++;
    base = `${base}.forced-${n}`;
  }
  const remove = async () => {
    await rm(join(outDir, `${base}.json`), { force: true });
    await rm(join(outDir, `${base}.md`), { force: true });
  };
  await writeNew(join(outDir, `${base}.json`), json);
  try {
    await writeNew(join(outDir, `${base}.md`), md);
  } catch (err) {
    // Leave no half-written pair: the hash is unspent, and a retry must find the name free. Only our own json
    // is removed: the md that made writeNew fail was not ours.
    await rm(join(outDir, `${base}.json`), { force: true });
    throw err;
  }
  return { base, remove };
}

export interface LedgerEntry {
  detector: string; detectorHash: string; split: "heldout"; labelsSha256: string; manifestSha256: string; scoredAt: string;
  /** The class map the held-out score used (absent on entries written before class maps). Recorded, never part of the repeat key. */
  classMapSha256?: string;
  /** The scorer's code (absent on older entries). */
  scorerSha256?: string;
  /** A repeat that --force let through. */
  forced: boolean;
  /** The --reason text, when given (on a forced repeat or a first score). */
  reason?: string;
}

export const MAX_FORCE_REASON = 500;
export interface Ledger { format: typeof LEDGER_FORMAT; entries: LedgerEntry[] }

const SHA = /^[0-9a-f]{64}$/;

/** Reads and validates the ledger. A damaged entry is an error naming the file, never skipped. */
export function readLedger(outDir: string): Ledger {
  const path = join(outDir, LEDGER_FILE);
  if (!existsSync(path)) return { format: LEDGER_FORMAT, entries: [] };
  const bad = (why: string) => new Error(`score: ${path} is damaged: ${why}; fix or restore it before scoring held-out`);
  let raw: Ledger;
  try { raw = JSON.parse(readFileSync(path, "utf8")) as Ledger; } catch { throw bad("not valid JSON"); }
  if (raw?.format !== LEDGER_FORMAT || !Array.isArray(raw.entries)) throw bad(`not a ${LEDGER_FORMAT} ledger`);
  raw.entries.forEach((e, i) => {
    const where = `entry ${i}`;
    if (!e || typeof e !== "object") throw bad(`${where} is not an object`);
    if (e.split !== "heldout") throw bad(`${where}.split must be heldout`);
    if (typeof e.detector !== "string" || !e.detector) throw bad(`${where}.detector must be a name`);
    for (const k of ["detectorHash", "labelsSha256", "manifestSha256"] as const) if (typeof e[k] !== "string" || !SHA.test(e[k])) throw bad(`${where}.${k} must be a sha256`);
    for (const k of ["classMapSha256", "scorerSha256"] as const) if (e[k] !== undefined && (typeof e[k] !== "string" || !SHA.test(e[k]!))) throw bad(`${where}.${k} must be a sha256`);
    try { isoTime(e.scoredAt, `${where}.scoredAt`); } catch { throw bad(`${where}.scoredAt must be an ISO timestamp`); }
    if (typeof e.forced !== "boolean") throw bad(`${where}.forced must be a boolean`);
    if (e.reason !== undefined && (typeof e.reason !== "string" || e.reason.length > MAX_FORCE_REASON)) throw bad(`${where}.reason must be at most ${MAX_FORCE_REASON} characters`);
  });
  return raw;
}

export const LOCK_FILE = `${LEDGER_FILE}.lock`;

/** Runs `work` holding an exclusive lock file next to the ledger; a lock already there (running or stale) is refused. */
export async function withLedgerLock<T>(outDir: string, work: () => Promise<T>): Promise<T> {
  await mkdir(outDir, { recursive: true });
  const lockPath = join(outDir, LOCK_FILE);
  let handle;
  try {
    handle = await open(lockPath, "wx");
  } catch (err) {
    if ((err as { code?: string }).code === "EEXIST")
      throw new Error(`score: ${lockPath} exists: another held-out scoring is running, or it crashed and left a stale lock; remove the file only once no scoring is running`);
    throw err;
  }
  try {
    await handle.writeFile(`${process.pid}\n`);
    return await work();
  } finally {
    await handle.close().catch(() => {});
    await unlink(lockPath).catch(() => {});
  }
}

/**
 * The held-out-once rule, under the ledger lock: refuses (before anything is written) when a detector hash in
 * `result` was already scored on held-out, whatever the class map: re-scoring after a class-map edit is tuning
 * on held-out. `force` lets the repeat through and records it as forced. `beforeRecord` (the report writer)
 * runs after the check and before the entry is written, so a failed report write never burns the hash. It may
 * return a cleanup, run when the ledger write then fails, so no report exists for a score the ledger does not
 * record (the report pair is removed and the hash stays unspent). The ledger is read inside the lock. Every
 * entry records the detector hash, the class map, the scorer hash, and the reason when given. Dev scoring
 * never touches the ledger.
 */
export async function recordHeldoutScoring(outDir: string, result: ReplayResult, labelsSha256: string,
  opts: RecordOptions = {}): Promise<Ledger> {
  assertScorable(result, "heldout");
  if (opts.reason !== undefined && (!opts.reason.trim() || opts.reason.length > MAX_FORCE_REASON))
    throw new Error(`score: --reason must be 1..${MAX_FORCE_REASON} characters`);
  return withLedgerLock(outDir, () => recordLocked(outDir, result, labelsSha256, opts));
}

export interface RecordOptions {
  force?: boolean; reason?: string; now?: Date; classMapSha256?: string;
  /** Writes the reports; may return a cleanup that removes them if the ledger write then fails. */
  beforeRecord?: (forced: boolean) => Promise<void | (() => Promise<void>)>;
  /** The ledger writer (tests inject a failing one); default the atomic writer. */
  writeLedger?: (path: string, data: string) => Promise<void>;
}

async function recordLocked(outDir: string, result: ReplayResult, labelsSha256: string,
  opts: RecordOptions): Promise<Ledger> {
  const classMapSha256 = opts.classMapSha256 ?? CLASS_MAP_SHA256;
  const ledger = readLedger(outDir);
  const done = new Set(ledger.entries.filter((e) => e.split === "heldout").map((e) => e.detectorHash));
  const repeat = result.detectors.filter((d) => done.has(d.hash));
  if (repeat.length && !opts.force)
    throw new Error(`score: held-out was already scored for ${repeat.map((d) => `${d.name} (${d.hash.slice(0, 12)})`).join(", ")}; a detector is scored on held-out once, whatever the class map (--force overrides and is recorded)`);
  const cleanup = await opts.beforeRecord?.(repeat.length > 0);
  const scoredAt = (opts.now ?? new Date()).toISOString();
  const next: Ledger = {
    format: LEDGER_FORMAT,
    entries: [...ledger.entries, ...result.detectors.map((d): LedgerEntry => ({
      detector: d.name, detectorHash: d.hash, split: "heldout", labelsSha256, manifestSha256: result.snapshot.manifestSha256, scoredAt, classMapSha256,
      scorerSha256: SCORER_SHA256, forced: done.has(d.hash), ...(opts.reason !== undefined ? { reason: opts.reason } : {}),
    }))],
  };
  try {
    await (opts.writeLedger ?? writeAtomic)(join(outDir, LEDGER_FILE), `${JSON.stringify(next, null, 2)}\n`);
  } catch (err) {
    if (typeof cleanup === "function") {
      try { await cleanup(); } catch (cleanErr) {
        throw new Error(`score: the ledger write failed (${(err as Error).message}) and the reports could not be removed (${(cleanErr as Error).message}); remove them by hand, the hash is unspent`);
      }
    }
    throw err;
  }
  return next;
}

export const labelsDigest = (bytes: string | Uint8Array) => sha256Hex(bytes);

const pct = (v: number | null) => v === null ? "n/a" : `${(v * 100).toFixed(1)}%`;
const mins = (ms: number | null) => ms === null ? "n/a" : `${(ms / 60_000).toFixed(1)}m`;
const HEADER = "| Urgency | Kind | Class | Alerts | TP | Misclassified | FP (hard neg / unlabeled) | Precision per alert | Pairs (TP) | Precision per pair | Recall | Before / after harm | Median lead | Alerts per 100 sessions |";
const RULE = "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|";
const rowLine = (row: RowScore) => `| ${row.urgency} | ${row.kind} | ${row.alertClass} | ${row.alerts} | ${row.truePositives} | ${row.misclassified} | ${row.falsePositives} (${row.hardNegativeHits} / ${row.unlabeled}) | ${pct(row.alertPrecision)} | ${row.pairs} (${row.truePositivePairs}) | ${pct(row.pairPrecision)} | ${pct(row.recall)} | ${row.beforeHarm} / ${row.afterHarm} | ${mins(row.medianLeadMs)} | ${row.alertsPer100Sessions.toFixed(2)} |`;

export function scoreMarkdown(r: ScoreReport): string {
  const lines = [
    `# Replay score (${r.split})`, "",
    `Snapshot manifest ${r.manifestSha256.slice(0, 16)}, labels ${r.labelsSha256.slice(0, 16)}, class map ${r.classMapSha256.slice(0, 16)}, scorer ${r.scorerSha256.slice(0, 16)}, ${r.sessions} sessions, ${r.incidents} ${r.split} incidents.`,
    "Unlabeled alerts count as false positives, so precision is a lower bound until they are adjudicated.",
    "pair-match needs the pair only. class-match also needs the class; a pair match of another class is misclassified, not a true positive.",
    "Precision per alert counts every alert; precision per pair counts each (detector, pair) once. The spec's precision bars use precision per pair.",
    ...(r.incomplete.length ? ["", `INCOMPLETE replay: ${r.incomplete.join("; ")}. Alerts may be missing.`] : []), "",
  ];
  for (const d of r.detectors) {
    lines.push(`## ${d.detector} (${d.hash.slice(0, 12)})`, "");
    if (d.diagnostics.dmSent !== undefined || d.diagnostics.written !== undefined)
      lines.push(`DM volume: ${d.diagnostics.dmSent ?? 0} DMs sent and ${d.diagnostics.written ?? 0} rows written over the replay. Re-alerts after the 7-day sweep are counted here, not in the alert rows.`, "");
    for (const m of [d.pairMatch, d.classMatch]) {
      lines.push(`### ${m.mode === "pair" ? "pair-match" : "class-match"}`, "", HEADER, RULE, ...[...m.rows, ...m.classRows, m.total].map(rowLine), "",
        "Per class and urgency:", "", HEADER, RULE, ...m.classUrgencyRows.map(rowLine), "",
        `Top ${m.topK.k} at ${m.topK.budgetPer100} per 100 sessions: ${m.topK.truePositives} of ${m.topK.alerts} alerts true (${pct(m.topK.alertPrecision)}), ${m.topK.truePositivePairs} of ${m.topK.pairs} pairs (${pct(m.topK.pairPrecision)}), ${m.topK.misclassified} misclassified, recall ${pct(m.topK.recall)}, ${m.topK.beforeHarm} before harm.`, "");
    }
    const c = d.confusion;
    const rows = c.rows.filter((cls) => c.columns.some((col) => c.counts[cls][col] > 0));
    lines.push("### Confusion (alert class by labeled class)", "", `| Alert class | ${c.columns.join(" | ")} |`, `|---|${c.columns.map(() => "---").join("|")}|`,
      ...rows.map((cls) => `| ${cls} | ${c.columns.map((col) => c.counts[cls][col]).join(" | ")} |`), "");
  }
  return `${lines.join("\n")}\n`;
}
