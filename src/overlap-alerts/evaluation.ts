import { type AlertKind, type Dataset, type Judge, KINDS, pairId, record, sessionKey, timestamp } from "./contracts.ts";
import { collectAlerts } from "./collector.ts";
export interface EvalCase {
  id: string;
  /** Keep all related tasks in one split; otherwise a near-duplicate leaks into held-out results. */
  workstream: string;
  split: "development" | "held_out";
  sessions: [string, string];
  asOf: string;
  mode: "replay" | "retrospective";
  expected: "suggest" | "suppress" | "uncertain";
  kind: AlertKind | null;
  rationale: string;
  evidenceIds: string[];
  labelSource: "human" | "agent_reviewed" | "synthetic";
}
export function parseCases(v: unknown, dataset: Dataset): EvalCase[] {
  if (!Array.isArray(v) || !v.length || v.length > 1000) throw new Error("invalid_eval_cases");
  const ids = new Set<string>(), splits = new Map<string, string>(), sessionSplits = new Map<string, string>();
  for (const c of v) {
    if (!record(c) || typeof c.id !== "string" || !c.id || ids.has(c.id) || typeof c.workstream !== "string" || !c.workstream ||
        !["development", "held_out"].includes(String(c.split)) || !Array.isArray(c.sessions) || c.sessions.length !== 2 || c.sessions[0] === c.sessions[1] ||
        !timestamp(c.asOf) || !["replay", "retrospective"].includes(String(c.mode)) || !["suggest", "suppress", "uncertain"].includes(String(c.expected)) ||
        !(c.kind === null || KINDS.includes(c.kind as never)) || (c.expected === "suggest" && c.kind === null) ||
        typeof c.rationale !== "string" || !c.rationale.trim() || !Array.isArray(c.evidenceIds) || !c.evidenceIds.length ||
        !["human", "agent_reviewed", "synthetic"].includes(String(c.labelSource))) throw new Error("invalid_eval_case");
    const sessions = c.sessions.map(k => dataset.sessions.find(s => sessionKey(s) === k));
    if (sessions.some(s => !s)) throw new Error("unknown_eval_session");
    const available = new Set(sessions.flatMap(s => s!.evidence.filter(e => Date.parse(e.at) <= Date.parse(c.asOf as string) &&
      (c.expected !== "suggest" || c.mode !== "replay" || (e.availableAt !== null && Date.parse(e.availableAt) <= Date.parse(c.asOf as string)))).map(e => e.id)));
    if (c.evidenceIds.some(id => typeof id !== "string" || !available.has(id))) throw new Error("unknown_or_future_label_evidence");
    const previous = splits.get(c.workstream);
    if (previous && previous !== c.split) throw new Error("workstream_split_leakage");
    for (const key of c.sessions as string[]) {
      if (sessionSplits.has(key) && sessionSplits.get(key) !== c.split) throw new Error("session_split_leakage");
      sessionSplits.set(key, c.split as string);
    }
    splits.set(c.workstream, c.split as string); ids.add(c.id);
  }
  return v as EvalCase[];
}
export async function evaluate(dataset: Dataset, cases: EvalCase[], judge: Judge) {
  const rows: { id: string; split: EvalCase["split"]; workstream: string; expected: EvalCase["expected"]; expectedKind: AlertKind | null;
    predicted: string; predictedKind: AlertKind | null; retrieved: boolean; failed: boolean; labelSource: EvalCase["labelSource"] }[] = [];
  // A case uses the whole snapshot, including distractors, so retrieval misses count.
  const runs = new Map<string, Awaited<ReturnType<typeof collectAlerts>>>();
  for (const c of cases) {
    const runKey = `${c.asOf}/${c.mode}`;
    let run = runs.get(runKey);
    if (!run) {
      run = await collectAlerts(dataset, judge, { asOf: c.asOf, mode: c.mode, lookbackHours: 168, maxPairs: 200 });
      runs.set(runKey, run);
    }
    const id = pairId(dataset.scope, ...c.sessions);
    const alert = run.alerts.find(a => a.pairId === id);
    const assessment = run.assessments.find(a => a.pairId === id);
    rows.push({ id: c.id, split: c.split, workstream: c.workstream, expected: c.expected, expectedKind: c.kind,
      predicted: alert ? "suggest" : assessment ? (assessment.verdict.decision === "suppress" ? "suppress" : "uncertain") : "not_retrieved", predictedKind: alert?.kind ?? null,
      retrieved: !!assessment, failed: assessment?.verdict.reason === "Assessment failed or returned ungrounded evidence.", labelSource: c.labelSource });
  }
  function metrics(selected: typeof rows) {
    const known = selected.filter(r => r.expected !== "uncertain");
    const tp = known.filter(r => r.predicted === "suggest" && r.expected === "suggest").length;
    const fp = known.filter(r => r.predicted === "suggest" && r.expected === "suppress").length;
    const fn = known.filter(r => r.predicted !== "suggest" && r.expected === "suggest").length;
    return { cases: selected.length, truePositive: tp, falsePositive: fp, falseNegative: fn,
      precision: tp + fp ? tp / (tp + fp) : null, recall: tp + fn ? tp / (tp + fn) : null,
      kindCorrect: known.filter(r => r.expected === "suggest" && r.predictedKind === r.expectedKind).length,
      retrievalMisses: selected.filter(r => r.expected === "suggest" && !r.retrieved).length,
      suggestionsOnUncertain: selected.filter(r => r.expected === "uncertain" && r.predicted === "suggest").length,
      failures: selected.filter(r => r.failed).length };
  }
  return { version: 1, source: dataset.provenance.kind, judge: judge.version, labelCounts: Object.fromEntries(["human", "agent_reviewed", "synthetic"].map(k => [k, cases.filter(c => c.labelSource === k).length])),
    development: metrics(rows.filter(r => r.split === "development")), heldOut: metrics(rows.filter(r => r.split === "held_out")),
    byKind: Object.fromEntries(KINDS.map(k => [k, metrics(rows.filter(r => r.expectedKind === k))])),
    unlabelledSuggestedPairs: [...new Set([...runs.values()].flatMap(run => run.alerts.filter(a => !cases.some(c => c.asOf === run.asOf && c.mode === run.mode && pairId(dataset.scope, ...c.sessions) === a.pairId)).map(a => a.pairId)))],
    coverage: [...runs.values()].map(r => ({ asOf: r.asOf, mode: r.mode, ...r.coverage })),
    rows };
}
