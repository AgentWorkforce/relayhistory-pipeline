/** Scorer math, table-driven, and the held-out-once ledger. */
import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import type { ReplayAlert } from "../replay/detector.ts";
import { parseLabels, type Labels } from "../replay/labels.ts";
import type { ReplayResult } from "../replay/replay.ts";
import { budgetK, classify, LEDGER_FILE, recordHeldoutScoring, scoreReplay } from "../replay/score.ts";
import { at, tmp } from "./helpers.ts";

const H = "a".repeat(64), H2 = "b".repeat(64);
const alert = (pair: [string, string], sentAt: string, over: Partial<ReplayAlert> = {}): ReplayAlert => ({
  detector: "d1", alertId: `${pair.join("|")}@${sentAt}`, kind: "concurrent_edit", urgency: "high", orgId: "o", workspaceId: "w",
  pair, artifact: "f", sentAt, alertAt: sentAt, channel: "dm", evidence: [], ...over,
});
const result = (alerts: ReplayAlert[], sessions = 100): ReplayResult => ({
  format: "session-intel-replay/2", incomplete: [], snapshot: { manifestSha256: H, createdAt: at("00:00"), window: { since: at("09:00"), until: at("13:00") }, sessions },
  intervalMs: 120_000, ticks: 121, detectors: [{ name: "d1", version: "1", params: {}, hash: H, diagnostics: {} }], alerts,
});
const labels: Labels = parseLabels({
  incidents: [
    { id: "i1", pair: ["c:1", "c:2"], kind: "overlap", harmAt: at("11:00"), split: "dev", workstream: "w1" },
    { id: "i2", pair: ["c:3", "c:4"], kind: "duplicate_work", harmAt: at("10:00"), split: "dev", workstream: "w2" },
    { id: "i3", pair: ["c:5", "c:6"], kind: "overlap", harmAt: at("12:00"), split: "dev", workstream: "w3" },
    { id: "h1", pair: ["c:7", "c:8"], kind: "overlap", harmAt: at("12:00"), split: "heldout", workstream: "w4" },
  ],
  hardNegatives: [
    { pair: ["c:9", "c:10"], reason: "planned_fleet" },
    { pair: ["c:11", "c:12"], reason: "benign_overlap", split: "heldout" },
  ],
});

describe("classify", () => {
  const cases: Array<[[string, string], "dev" | "heldout", string]> = [
    [["c:2", "c:1"], "dev", "tp"], // order-insensitive
    [["c:7", "c:8"], "dev", "unlabeled"], // dev never sees held-out labels
    [["c:7", "c:8"], "heldout", "tp"],
    [["c:9", "c:10"], "dev", "hard_negative"], // no split: counts in both
    [["c:9", "c:10"], "heldout", "hard_negative"],
    [["c:11", "c:12"], "dev", "unlabeled"],
    [["c:1", "c:2"], "heldout", "out"], // a dev incident is out of scope on held-out
    [["c:11", "c:12"], "heldout", "hard_negative"],
    [["c:1", "c:3"], "dev", "unlabeled"],
  ];
  for (const [pair, split, scope] of cases)
    test(`${pair.join(" ")} on ${split} is ${scope}`, () => expect(classify({ pair }, labels, split).scope).toBe(scope));
});

describe("scoreReplay", () => {
  test("per-row and total precision, recall, lead times and alerts per 100 sessions", () => {
    const r = scoreReplay(result([
      alert(["c:1", "c:2"], at("10:30")), // i1, 30 min before harm
      alert(["c:2", "c:1"], at("10:50"), { alertId: "later" }), // i1 again: the first sentAt counts
      alert(["c:3", "c:4"], at("10:20"), { kind: "duplicate_work" }), // i2, 20 min after harm
      alert(["c:9", "c:10"], at("10:00"), { urgency: "low", kind: "nearby_work" }), // hard negative
      alert(["c:1", "c:5"], at("10:10"), { urgency: "low", kind: "nearby_work" }), // unlabeled
      alert(["c:7", "c:8"], at("10:00")), // a held-out incident: only unlabeled on dev
    ]), labels, { split: "dev", labelsSha256: H2 });
    const d = r.detectors[0]!;
    expect(r.incidents).toBe(3);
    expect(d.pairMatch.total).toMatchObject({
      alerts: 6, outOfScope: 0, truePositives: 3, falsePositives: 3, hardNegativeHits: 1, unlabeled: 2, alertPrecision: 0.5,
      incidents: 3, detected: 2, recall: 2 / 3, beforeHarm: 1, afterHarm: 1, leadTimesMs: [-20 * 60_000, 30 * 60_000], medianLeadMs: 5 * 60_000,
      alertsPer100Sessions: 6,
    });
    const row = (u: string, k: string) => d.pairMatch.rows.find((x) => x.urgency === u && x.kind === k)!;
    expect(d.pairMatch.rows.map((x) => `${x.urgency}/${x.kind}`)).toEqual(["high/concurrent_edit", "high/duplicate_work", "low/nearby_work"]);
    expect(row("high", "concurrent_edit")).toMatchObject({ alerts: 3, truePositives: 2, alertPrecision: 2 / 3, detected: 1, recall: 1 / 3, leadTimesMs: [30 * 60_000], outOfScope: 0 });
    expect(row("high", "duplicate_work")).toMatchObject({ truePositives: 1, beforeHarm: 0, afterHarm: 1, medianLeadMs: -20 * 60_000 });
    expect(row("low", "nearby_work")).toMatchObject({ alerts: 2, truePositives: 0, alertPrecision: 0, recall: 0, medianLeadMs: null });
    // Budget 5 per 100 sessions on 100 sessions: K = 5, HIGH first then earliest.
    expect(d.pairMatch.topK).toMatchObject({ k: 5, alerts: 5, truePositives: 3, detected: 2, beforeHarm: 1 });
  });

  test("a dev score is the same with or without the held-out labels (nothing about them leaks)", () => {
    const alerts = [alert(["c:1", "c:2"], at("10:30")), alert(["c:7", "c:8"], at("10:00")), alert(["c:11", "c:12"], at("10:05"), { urgency: "low" })];
    const devOnly = parseLabels({
      incidents: labels.incidents.filter((x) => x.split === "dev"),
      hardNegatives: labels.hardNegatives.filter((n) => n.split !== "heldout"),
    });
    const a = scoreReplay(result(alerts), labels, { split: "dev", labelsSha256: H2 });
    const b = scoreReplay(result(alerts), devOnly, { split: "dev", labelsSha256: H2 });
    expect(b.detectors).toEqual(a.detectors);
    expect(b.incidents).toBe(a.incidents);
  });

  test("top-K takes HIGH before LOW and the earliest first", () => {
    const r = scoreReplay(result([
      alert(["c:9", "c:10"], at("09:00"), { urgency: "low" }),
      alert(["c:1", "c:2"], at("10:59")),
      alert(["c:1", "c:5"], at("10:00")),
    ], 20), labels, { split: "dev", labelsSha256: H2 });
    expect(r.detectors[0]!.pairMatch.topK).toMatchObject({ k: 1, alerts: 1, truePositives: 0, alertPrecision: 0 });
    const r2 = scoreReplay(result([alert(["c:1", "c:2"], at("10:59")), alert(["c:1", "c:5"], at("11:00"))], 20), labels, { split: "dev", labelsSha256: H2 });
    expect(r2.detectors[0]!.pairMatch.topK).toMatchObject({ k: 1, truePositives: 1, beforeHarm: 1 });
  });

  test("empty and edge cases: no alerts gives null precision, zero recall; the budget is validated", () => {
    const r = scoreReplay(result([]), labels, { split: "heldout", labelsSha256: H2 });
    expect(r.detectors[0]!.pairMatch.total).toMatchObject({ alerts: 0, alertPrecision: null, pairPrecision: null, recall: 0, medianLeadMs: null });
    expect(() => scoreReplay(result([]), labels, { split: "dev", labelsSha256: H2, budgetPer100: 0 })).toThrow("budget");
    expect([budgetK(0, 5), budgetK(1, 5), budgetK(100, 5), budgetK(101, 5), budgetK(5_008, 5)]).toEqual([0, 1, 5, 6, 251]);
  });
});

describe("labels", () => {
  test("refuses malformed labels", () => {
    const inc = { id: "i", pair: ["c:1", "c:2"], kind: "overlap", harmAt: at("10:00"), split: "dev", workstream: "w" };
    const bad: Array<[unknown, string]> = [
      [{ incidents: [{ ...inc, pair: ["c:1", "c:1"] }], hardNegatives: [] }, "same session twice"],
      [{ incidents: [{ ...inc, pair: ["nocolon", "c:2"] }], hardNegatives: [] }, "<harness>:<sessionId>"],
      [{ incidents: [inc, inc], hardNegatives: [] }, "repeats an incident id"],
      [{ incidents: [{ ...inc, kind: "bad" }], hardNegatives: [] }, "kind must be"],
      [{ incidents: [{ ...inc, split: "test" }], hardNegatives: [] }, "split must be"],
      [{ incidents: [inc], hardNegatives: [{ pair: ["c:2", "c:1"], reason: "unrelated" }] }, "also an incident pair"],
      [{ incidents: [], hardNegatives: [{ pair: ["c:1", "c:2"], reason: "nope" }] }, "reason must be"],
      [{ incidents: [], hardNegatives: [], extra: 1 }, "unknown field"],
    ];
    for (const [raw, message] of bad) expect(() => parseLabels(raw)).toThrow(message);
  });
});

describe("held-out ledger", () => {
  test("held-out is scored once per detector hash; --force overrides and is recorded; another hash is free", async () => {
    const out = tmp("si-ledger-");
    const r = result([]);
    await recordHeldoutScoring(out, r, H2, { now: new Date(at("12:00")) });
    await expect(recordHeldoutScoring(out, r, H2)).rejects.toThrow("already scored");
    // The refusal wrote nothing.
    expect(JSON.parse(readFileSync(join(out, LEDGER_FILE), "utf8")).entries).toHaveLength(1);
    await recordHeldoutScoring(out, r, H2, { force: true });
    const other = { ...r, detectors: [{ ...r.detectors[0]!, hash: "c".repeat(64) }] };
    await recordHeldoutScoring(out, other, H2);
    const ledger = JSON.parse(readFileSync(join(out, LEDGER_FILE), "utf8"));
    expect(ledger.entries.map((e: { detectorHash: string; forced: boolean }) => [e.detectorHash[0], e.forced])).toEqual([["a", false], ["a", true], ["c", false]]);
    expect(ledger.entries[0]).toMatchObject({ split: "heldout", labelsSha256: H2, manifestSha256: H, scoredAt: at("12:00") });
  });
});
