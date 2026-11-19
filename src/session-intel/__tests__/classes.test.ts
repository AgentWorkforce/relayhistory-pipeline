/** Class-aware scoring: the taxonomy mappings, class-match misclassification, the confusion table and verdict classes. */
import { describe, expect, test } from "bun:test";
import { buildPacket, parseVerdicts, verdictReport } from "../replay/adjudicate.ts";
import { ALERT_CLASSES, alertClassOf, classesOfVerdict, classOfIncidentKind, classOfKind } from "../replay/classes.ts";
import type { ReplayAlert } from "../replay/detector.ts";
import { parseLabels } from "../replay/labels.ts";
import { parseReplayResult, type ReplayResult } from "../replay/replay.ts";
import { confusion, outcome, scoreReplay } from "../replay/score.ts";
import { at } from "./helpers.ts";

describe("mappings", () => {
  const kinds: Array<[string, "high" | "low", string]> = [
    ["duplicate_work", "high", "collision"],
    ["duplicate_work", "low", "collision"],
    ["superseded_work", "high", "collision"],
    ["concurrent_edit", "high", "collision"], // the 60-minute HIGH row
    ["concurrent_edit", "low", "overlap"], // T3 outside the HIGH row is only overlap
    ["nearby_work", "low", "overlap"],
    ["similar_summary", "low", "overlap"],
    ["intent_conflict", "high", "intent_conflict"],
    ["constraint_violation", "low", "constraint_violation"],
    ["planned_fleet", "low", "planned_fleet"],
    ["none", "low", "none"],
  ];
  for (const [kind, urgency, cls] of kinds) test(`kind ${kind} at ${urgency} is ${cls}`, () => expect(classOfKind(kind, urgency)).toBe(cls));
  for (const kind of ["repeated_investigation", "stale_base", "something_new", ""])
    test(`unmapped kind ${JSON.stringify(kind)} throws unless the detector sets alertClass`, () => {
      expect(() => classOfKind(kind, "high")).toThrow("has no class");
      expect(() => alertClassOf({ kind, urgency: "low" })).toThrow("has no class");
      expect(alertClassOf({ kind, urgency: "low", alertClass: "intent_conflict" })).toBe("intent_conflict");
    });

  const incidents: Array<[string, string]> = [
    ["duplicate_work", "collision"], ["superseded_work", "collision"], ["overlap", "overlap"],
    ["intent_conflict", "intent_conflict"], ["constraint_violation", "constraint_violation"],
  ];
  for (const [kind, cls] of incidents) test(`incident kind ${kind} is ${cls}`, () => expect(classOfIncidentKind(kind)).toBe(cls));

  const verdicts: Array<[string, string[]]> = [
    ["real_overlap", ["collision", "overlap"]], ["collision", ["collision"]], ["overlap", ["overlap"]],
    ["intent_conflict", ["intent_conflict"]], ["constraint_violation", ["constraint_violation"]],
    ["planned", ["planned_fleet"]], ["unrelated", []], ["unclear", []],
  ];
  for (const [v, cls] of verdicts) test(`verdict ${v} confirms ${cls.join(", ") || "nothing"}`, () => expect([...classesOfVerdict(v)]).toEqual(cls));

  test("a detector-set alertClass wins over the kind mapping", () => {
    expect(alertClassOf({ kind: "nearby_work", urgency: "low" })).toBe("overlap");
    expect(alertClassOf({ kind: "claim_pair", urgency: "high", alertClass: "intent_conflict" })).toBe("intent_conflict");
    expect(ALERT_CLASSES).toEqual(["constraint_violation", "intent_conflict", "collision", "overlap", "planned_fleet", "none"]);
  });
});

const H = "a".repeat(64);
const alert = (pair: [string, string], kind: string, urgency: "high" | "low", over: Partial<ReplayAlert> = {}): ReplayAlert => ({
  detector: "d1", alertId: `${pair.join("|")}/${kind}`, kind, urgency, orgId: "o", workspaceId: "w", pair, artifact: "f",
  sentAt: at("10:00"), alertAt: at("10:00"), channel: "pull", evidence: [], ...over,
});
const result = (alerts: ReplayAlert[]): ReplayResult => ({
  format: "session-intel-replay/2", incomplete: [], snapshot: { manifestSha256: H, createdAt: at("00:00"), window: { since: at("09:00"), until: at("13:00") }, sessions: 100 },
  intervalMs: 120_000, ticks: 121, detectors: [{ name: "d1", version: "1", params: {}, hash: H, diagnostics: {} }], alerts,
});
const labels = parseLabels({
  incidents: [
    { id: "dup", pair: ["c:1", "c:2"], kind: "duplicate_work", harmAt: at("11:00"), split: "dev", workstream: "w1" },
    { id: "ic", pair: ["c:3", "c:4"], kind: "intent_conflict", harmAt: at("11:00"), split: "dev", workstream: "w2" },
    // Two incidents on one pair, of different classes: an alert of either class matches.
    { id: "ov", pair: ["c:5", "c:6"], kind: "overlap", harmAt: at("11:00"), split: "dev", workstream: "w3" },
    { id: "cv", pair: ["c:5", "c:6"], kind: "constraint_violation", harmAt: at("11:00"), split: "dev", workstream: "w3" },
    { id: "held", pair: ["c:7", "c:8"], kind: "intent_conflict", harmAt: at("11:00"), split: "heldout", workstream: "w4" },
  ],
  hardNegatives: [{ pair: ["c:9", "c:10"], reason: "planned_fleet" }, { pair: ["c:11", "c:12"], reason: "benign_overlap" }],
});

describe("class-match", () => {
  const cases: Array<[string, ReplayAlert, string, string]> = [
    ["collision on a duplicate_work incident", alert(["c:1", "c:2"], "duplicate_work", "high"), "tp", "tp"],
    ["overlap on a duplicate_work incident", alert(["c:2", "c:1"], "nearby_work", "low"), "tp", "misclassified"],
    ["overlap on an intent_conflict incident", alert(["c:3", "c:4"], "nearby_work", "low"), "tp", "misclassified"],
    ["a detector-set intent_conflict on it", alert(["c:3", "c:4"], "claim_pair", "low", { alertClass: "intent_conflict" }), "tp", "tp"],
    ["overlap on an overlap + constraint pair", alert(["c:5", "c:6"], "similar_summary", "low"), "tp", "tp"],
    ["collision on an overlap + constraint pair", alert(["c:5", "c:6"], "duplicate_work", "high"), "tp", "misclassified"],
    ["a collision on a planned_fleet hard negative", alert(["c:9", "c:10"], "concurrent_edit", "high"), "hard_negative", "hard_negative"],
    ["a planned_fleet alert on a planned_fleet hard negative", alert(["c:9", "c:10"], "fleet", "low", { alertClass: "planned_fleet" }), "hard_negative", "tp"],
    ["an overlap alert on a benign_overlap hard negative", alert(["c:11", "c:12"], "nearby_work", "low"), "hard_negative", "tp"],
    ["a collision on a benign_overlap hard negative", alert(["c:11", "c:12"], "duplicate_work", "high"), "hard_negative", "hard_negative"],
    ["an unlabeled pair", alert(["c:1", "c:3"], "nearby_work", "low"), "unlabeled", "unlabeled"],
    ["a held-out pair on dev", alert(["c:7", "c:8"], "nearby_work", "low"), "unlabeled", "unlabeled"],
  ];
  for (const [name, a, pair, cls] of cases) test(`${name}: pair-match ${pair}, class-match ${cls}`, () => {
    expect(outcome(a, labels, "dev", "pair").scope).toBe(pair as never);
    expect(outcome(a, labels, "dev", "class").scope).toBe(cls as never);
  });

  test("misclassified is not a true positive: precision, recall and per-class rows", () => {
    const r = scoreReplay(result([
      alert(["c:1", "c:2"], "duplicate_work", "high", { sentAt: at("10:30") }), // collision TP, 30 min lead
      alert(["c:3", "c:4"], "nearby_work", "low"), // misclassified
      alert(["c:5", "c:6"], "similar_summary", "low", { sentAt: at("10:50") }), // overlap TP (credits "ov" only)
      alert(["c:9", "c:10"], "concurrent_edit", "high"), // hard negative
    ]), labels, { split: "dev", labelsSha256: H }).detectors[0]!;
    expect(d(r.pairMatch.total)).toEqual({ alerts: 4, truePositives: 3, misclassified: 0, falsePositives: 1, alertPrecision: 0.75, detected: 4, recall: 1 });
    expect(d(r.classMatch.total)).toEqual({ alerts: 4, truePositives: 2, misclassified: 1, falsePositives: 1, alertPrecision: 0.5, detected: 2, recall: 0.5 });
    const byClass = Object.fromEntries(r.classMatch.classRows.map((row) => [row.alertClass, d(row)]));
    // collision: 2 alerts (1 TP, 1 hard negative) against the 1 collision incident; overlap: 2 alerts (1 TP, 1 misclassified) against 1.
    expect(byClass).toEqual({
      collision: { alerts: 2, truePositives: 1, misclassified: 0, falsePositives: 1, alertPrecision: 0.5, detected: 1, recall: 1 },
      overlap: { alerts: 2, truePositives: 1, misclassified: 1, falsePositives: 0, alertPrecision: 0.5, detected: 1, recall: 1 },
    });
    expect(r.classMatch.rows.map((row) => `${row.urgency}/${row.kind}/${row.alertClass}`)).toEqual([
      "high/concurrent_edit/collision", "high/duplicate_work/collision", "low/nearby_work/overlap", "low/similar_summary/overlap",
    ]);
    expect(r.classMatch.topK).toMatchObject({ k: 5, alerts: 4, truePositives: 2, misclassified: 1 });
  });

  test("the confusion table counts each in-scope alert per distinct labeled class", () => {
    const c = confusion([
      alert(["c:1", "c:2"], "duplicate_work", "high"),
      alert(["c:3", "c:4"], "nearby_work", "low"),
      alert(["c:5", "c:6"], "similar_summary", "low"), // both "overlap" and "constraint_violation"
      alert(["c:9", "c:10"], "concurrent_edit", "high"), // planned_fleet negative
      alert(["c:11", "c:12"], "nearby_work", "low"), // benign_overlap negative: overlap
      alert(["c:1", "c:3"], "nearby_work", "low"), // unlabeled
      alert(["c:7", "c:8"], "claim_pair", "high", { alertClass: "intent_conflict" }), // held-out: unlabeled on dev
    ], labels, "dev");
    const nonzero = Object.fromEntries(Object.entries(c.counts).map(([row, cols]) => [row, Object.fromEntries(Object.entries(cols).filter(([, n]) => n > 0))]));
    expect(nonzero).toEqual({
      constraint_violation: {}, planned_fleet: {}, none: {},
      collision: { collision: 1, planned_fleet: 1 },
      // The benign_overlap hard negative lands in the overlap column (G1).
      overlap: { intent_conflict: 1, overlap: 2, constraint_violation: 1, unlabeled: 1 },
      intent_conflict: { unlabeled: 1 },
    });
    // On held-out the dev pairs are out of scope and the held-out incident is matched.
    const held = confusion([alert(["c:7", "c:8"], "claim_pair", "high", { alertClass: "intent_conflict" }), alert(["c:1", "c:2"], "duplicate_work", "high")], labels, "heldout");
    expect(held.counts.intent_conflict.intent_conflict).toBe(1);
    expect(held.counts.collision.collision).toBe(0);
  });

  test("an unknown alertClass is refused when reading alerts", () => {
    const bad = result([alert(["c:1", "c:2"], "x", "high", { alertClass: "nonsense" as never })]);
    expect(() => parseReplayResult(JSON.parse(JSON.stringify(bad)))).toThrow("malformed");
  });
});

const d = (row: { alerts: number; truePositives: number; misclassified: number; falsePositives: number; alertPrecision: number | null; detected: number; recall: number | null }) =>
  ({ alerts: row.alerts, truePositives: row.truePositives, misclassified: row.misclassified, falsePositives: row.falsePositives, alertPrecision: row.alertPrecision, detected: row.detected, recall: row.recall });

describe("verdict precision per class", () => {
  test("a verdict must confirm the alert's class; another real class is misclassified", () => {
    const { key, packet } = buildPacket([result([
      alert(["c:1", "c:2"], "duplicate_work", "high", { artifact: "a1" }),
      alert(["c:3", "c:4"], "nearby_work", "low", { artifact: "a2" }),
      alert(["c:5", "c:6"], "similar_summary", "low", { artifact: "a3" }),
      alert(["c:7", "c:8"], "claim_pair", "low", { artifact: "a4", alertClass: "intent_conflict" }),
      alert(["c:9", "c:10"], "nearby_work", "low", { artifact: "a5" }),
    ])], { seed: "s", budgetPer100: 10 });
    expect(JSON.stringify(packet)).not.toMatch(/collision|overlap|intent_conflict|alertClass/);
    const byArtifact = new Map(key.entries.map((e) => [e.artifact, e.alertKey]));
    const v = (artifact: string, verdict: string) => ({ alertKey: byArtifact.get(artifact)!, verdict, adjudicator: "a" });
    const r = verdictReport(key, parseVerdicts([
      v("a1", "real_overlap"), // collision confirmed by the M0 verdict
      v("a2", "intent_conflict"), // overlap alert, real intent conflict: misclassified
      v("a3", "overlap"), // overlap confirmed
      v("a4", "intent_conflict"), // detector-set class confirmed
      v("a5", "planned"), // negative
    ], key));
    expect(r.classes.map((c) => [c.alertClass, c.positive, c.misclassified, c.negative, c.precision])).toEqual([
      ["collision", 1, 0, 0, 1], ["intent_conflict", 1, 0, 0, 1], ["overlap", 1, 1, 1, 1 / 3],
    ]);
    // The pair-level precision is unchanged: any real verdict is positive.
    expect(r.detectors[0]).toMatchObject({ positive: 4, negative: 1, precision: 0.8 });
  });
});
