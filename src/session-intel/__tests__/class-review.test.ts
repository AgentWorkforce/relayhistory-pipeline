/**
 * M0-5 lander findings: hard negatives by class (G1), verdict agreement on class sets (G2), the class-map hash
 * (G3), class x urgency rows (G4), unmapped kinds (G5), key validation and the driver's class check (G7), and
 * the verdict report format (G8).
 */
import { describe, expect, test } from "bun:test";
import { readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { buildPacket, consensus, parsePacketKey, parseVerdicts, verdictReport } from "../replay/adjudicate.ts";
import { CLASS_MAP_SHA256, classOfHardNegative, verdictsAgree } from "../replay/classes.ts";
import { main } from "../replay/cli.ts";
import type { ReplayAlert, ReplayDetector } from "../replay/detector.ts";
import { parseLabels } from "../replay/labels.ts";
import { parseReplayResult, replaySnapshot, type ReplayResult } from "../replay/replay.ts";
import { confusion, LEDGER_FILE, recordHeldoutScoring, SCORER_SHA256, scoreReplay } from "../replay/score.ts";
import { loadSnapshot } from "../replay/snapshot.ts";
import { sha256Hex } from "../replay/util.ts";
import { at, FIXTURE_DIR, tmp } from "./helpers.ts";

const H = "a".repeat(64);
const alert = (pair: [string, string], kind: string, urgency: "high" | "low", over: Partial<ReplayAlert> = {}): ReplayAlert => ({
  detector: "d1", alertId: `${pair.join("|")}/${kind}/${urgency}`, kind, urgency, orgId: "o", workspaceId: "w", pair, artifact: `${kind}-${urgency}`,
  sentAt: at("10:00"), alertAt: at("10:00"), channel: "pull", evidence: [], ...over,
});
const result = (alerts: ReplayAlert[]): ReplayResult => ({
  format: "session-intel-replay/2", snapshot: { manifestSha256: H, createdAt: at("00:00"), window: { since: at("09:00"), until: at("13:00") }, sessions: 100 },
  intervalMs: 120_000, ticks: 121, detectors: [{ name: "d1", version: "1", params: {}, hash: H, diagnostics: {} }], incomplete: [], alerts,
});
const labels = parseLabels({
  incidents: [
    { id: "dup", pair: ["c:1", "c:2"], kind: "duplicate_work", harmAt: at("11:00"), split: "dev", workstream: "w1" },
    { id: "ic", pair: ["c:3", "c:4"], kind: "intent_conflict", harmAt: at("11:00"), split: "dev", workstream: "w2" },
  ],
  hardNegatives: [
    { pair: ["c:5", "c:6"], reason: "benign_overlap" },
    { pair: ["c:7", "c:8"], reason: "planned_fleet" },
    { pair: ["c:9", "c:10"], reason: "unrelated" },
  ],
});

describe("G1: hard negatives by class", () => {
  test("the reasons map to overlap, planned_fleet and none", () => {
    expect(["benign_overlap", "planned_fleet", "unrelated"].map(classOfHardNegative)).toEqual(["overlap", "planned_fleet", "none"]);
  });

  test("class-match: a benign_overlap is correct for overlap only, a planned fleet for planned_fleet only; pair-match is unchanged", () => {
    const alerts = [
      alert(["c:5", "c:6"], "nearby_work", "low"), // overlap on benign_overlap: class-correct
      alert(["c:5", "c:6"], "duplicate_work", "high"), // collision on benign_overlap: false
      alert(["c:7", "c:8"], "fleet", "low", { alertClass: "planned_fleet" }), // planned_fleet on planned_fleet: class-correct
      alert(["c:7", "c:8"], "similar_summary", "low"), // overlap on planned_fleet: false
      alert(["c:9", "c:10"], "none", "low"), // a `none` alert is never correct, even on an unrelated pair
    ];
    const d = scoreReplay(result(alerts), labels, { split: "dev", labelsSha256: H }).detectors[0]!;
    expect(d.pairMatch.total).toMatchObject({ alerts: 5, truePositives: 0, hardNegativeHits: 5, alertPrecision: 0 });
    expect(d.classMatch.total).toMatchObject({ alerts: 5, truePositives: 2, hardNegativeHits: 3, alertPrecision: 2 / 5, detected: 0 });
    // A class-correct hard negative credits no incident: recall and lead times are unchanged.
    expect(d.classMatch.total.leadTimesMs).toEqual([]);
    const byClass = Object.fromEntries(d.classMatch.classRows.map((r) => [r.alertClass, [r.truePositives, r.hardNegativeHits]]));
    expect(byClass).toEqual({ collision: [0, 1], overlap: [1, 1], planned_fleet: [1, 0], none: [0, 1] });
    const c = confusion(alerts, labels, "dev");
    expect(c.counts.overlap).toMatchObject({ overlap: 1, planned_fleet: 1, none: 0 });
    expect(c.counts.collision.overlap).toBe(1);
    expect(c.counts.planned_fleet.planned_fleet).toBe(1);
    expect(c.counts.none.none).toBe(1);
  });

  test("the planned verdict confirms a planned_fleet alert and is a negative for any other class", () => {
    const { key } = buildPacket([result([
      alert(["c:7", "c:8"], "fleet", "low", { alertClass: "planned_fleet" }),
      alert(["c:5", "c:6"], "nearby_work", "low"),
    ])], { seed: "s", budgetPer100: 10 });
    const r = verdictReport(key, parseVerdicts(key.entries.map((e) => ({ alertKey: e.alertKey, verdict: "planned", adjudicator: "a" })), key));
    expect(r.classes.map((c) => [c.alertClass, c.positive, c.negative])).toEqual([["overlap", 0, 1], ["planned_fleet", 1, 0]]);
  });
});

describe("G2: agreement on confirmed classes", () => {
  const agree: Array<[string, string, boolean]> = [
    ["real_overlap", "collision", true], ["real_overlap", "overlap", true], ["collision", "overlap", false],
    ["planned", "planned", true], ["unrelated", "unrelated", true], ["unrelated", "unclear", false],
    ["unrelated", "planned", false], ["intent_conflict", "real_overlap", false],
  ];
  for (const [a, b, want] of agree) test(`${a} and ${b} ${want ? "agree" : "disagree"}`, () => {
    expect(verdictsAgree(a, b)).toBe(want);
    expect(verdictsAgree(b, a)).toBe(want);
  });

  const cases: Array<[string[], string]> = [
    [["real_overlap", "collision"], "collision"],
    [["real_overlap", "real_overlap"], "real_overlap"],
    [["real_overlap", "collision", "overlap"], "unclear"],
    [["collision", "unrelated"], "unclear"],
    [["planned", "planned", "real_overlap"], "planned"],
    [["unrelated", "unrelated", "collision"], "unrelated"],
  ];
  for (const [votes, want] of cases) test(`consensus of ${votes.join(", ")} is ${want}`, () => expect(consensus(votes as never)).toBe(want as never));

  test("mixed vocabularies are not counted as disagreement, and kappa uses the same rule", () => {
    const { key } = buildPacket([result([1, 2, 3, 4].map((n) => alert([`c:${n}`, `c:${n + 10}`], "duplicate_work", "high")))], { seed: "s", budgetPer100: 10 });
    const ks = key.entries.map((e) => e.alertKey);
    const r = verdictReport(key, parseVerdicts([
      { alertKey: ks[0]!, verdict: "real_overlap", adjudicator: "a" }, { alertKey: ks[0]!, verdict: "collision", adjudicator: "b" },
      { alertKey: ks[1]!, verdict: "collision", adjudicator: "a" }, { alertKey: ks[1]!, verdict: "real_overlap", adjudicator: "b" },
      { alertKey: ks[2]!, verdict: "unrelated", adjudicator: "a" }, { alertKey: ks[2]!, verdict: "unrelated", adjudicator: "b" },
      { alertKey: ks[3]!, verdict: "unrelated", adjudicator: "a" }, { alertKey: ks[3]!, verdict: "planned", adjudicator: "b" },
    ], key));
    expect(r.agreement[0]).toMatchObject({ shared: 4, agreed: 3, rate: 0.75 });
    // po = 0.75. a: ro .25, co .25, un .5; b: co .25, ro .25, un .25, pl .25.
    // pe = ro(.25)x(co .25 + ro .25) + co(.25)x(co .25 + ro .25) + un(.5)x(un .25) = .125 + .125 + .125 = .375.
    expect(r.agreement[0]!.kappa).toBeCloseTo((0.75 - 0.375) / (1 - 0.375), 10);
    expect(r.format).toBe("session-intel-verdicts/2");
  });
});

describe("G3: the class-map hash", () => {
  test("is the sha256 of classes.ts and is in the score report", () => {
    expect(CLASS_MAP_SHA256).toBe(sha256Hex(readFileSync(new URL("../replay/classes.ts", import.meta.url))));
    expect(scoreReplay(result([]), labels, { split: "dev", labelsSha256: H }).classMapSha256).toBe(CLASS_MAP_SHA256);
  });

  test("held-out once is per detector hash whatever the class map; a forced re-score records both hashes and the reason", async () => {
    const out = tmp();
    const r = result([]);
    await recordHeldoutScoring(out, r, H, { now: new Date(at("09:00")) });
    // The same detector with the same map, and with an edited map: both refused (editing the map is tuning on held-out).
    await expect(recordHeldoutScoring(out, r, H)).rejects.toThrow("whatever the class map");
    await expect(recordHeldoutScoring(out, r, H, { classMapSha256: "c".repeat(64) })).rejects.toThrow("already scored");
    // The refusals wrote nothing.
    expect(JSON.parse(readFileSync(join(out, LEDGER_FILE), "utf8")).entries).toHaveLength(1);
    await recordHeldoutScoring(out, r, H, { classMapSha256: "c".repeat(64), force: true, reason: "class map fix after review", now: new Date(at("10:00")) });
    const entries = JSON.parse(readFileSync(join(out, LEDGER_FILE), "utf8")).entries;
    expect(entries).toEqual([
      { detector: "d1", detectorHash: H, split: "heldout", labelsSha256: H, manifestSha256: H, scoredAt: at("09:00"), classMapSha256: CLASS_MAP_SHA256, scorerSha256: SCORER_SHA256, forced: false },
      { detector: "d1", detectorHash: H, split: "heldout", labelsSha256: H, manifestSha256: H, scoredAt: at("10:00"), classMapSha256: "c".repeat(64), scorerSha256: SCORER_SHA256, forced: true, reason: "class map fix after review" },
    ]);
    // A forced repeat without a reason is recorded as forced, with no reason.
    await recordHeldoutScoring(out, r, H, { force: true });
    expect(JSON.parse(readFileSync(join(out, LEDGER_FILE), "utf8")).entries[2]).toMatchObject({ forced: true, classMapSha256: CLASS_MAP_SHA256 });
    expect(JSON.parse(readFileSync(join(out, LEDGER_FILE), "utf8")).entries[2].reason).toBeUndefined();
    await expect(recordHeldoutScoring(out, r, H, { force: true, reason: " " })).rejects.toThrow("--reason");
    // A legacy entry (no class map) also blocks.
    const legacy = tmp();
    writeFileSync(join(legacy, LEDGER_FILE), JSON.stringify({ format: "session-intel-scored-hashes/1", entries: [
      { detector: "d1", detectorHash: H, split: "heldout", labelsSha256: H, manifestSha256: H, scoredAt: at("09:00"), forced: false },
    ] }));
    await expect(recordHeldoutScoring(legacy, r, H, { classMapSha256: "d".repeat(64) })).rejects.toThrow("already scored");
  });

  test("the CLI records --reason on a first held-out score and on a forced repeat, and refuses both flags on dev", async () => {
    const dir = tmp();
    const file = join(dir, "alerts.json");
    writeFileSync(file, JSON.stringify(result([])));
    const labelsFile = join(dir, "labels.json");
    writeFileSync(labelsFile, JSON.stringify({ incidents: [], hardNegatives: [] }));
    const io = { out: () => {}, err: () => {} };
    const args = ["score", "--alerts", file, "--labels", labelsFile, "--out", join(dir, "s"), "--split", "heldout"];
    const dev = ["score", "--alerts", file, "--labels", labelsFile, "--out", join(dir, "s")];
    await expect(main([...dev, "--force"], io)).rejects.toThrow("held-out scoring only");
    await expect(main([...dev, "--reason", "x"], io)).rejects.toThrow("held-out scoring only");
    expect(await main([...args, "--reason", "first look, frozen at review"], io)).toBe(0);
    await expect(main(args, io)).rejects.toThrow("already scored");
    expect(await main([...args, "--force", "--reason", "relabel of inc-7"], io)).toBe(0);
    expect(JSON.parse(readFileSync(join(dir, "s", LEDGER_FILE), "utf8")).entries.map((e: { forced: boolean; reason?: string }) => [e.forced, e.reason ?? null]))
      .toEqual([[false, "first look, frozen at review"], [true, "relabel of inc-7"]]);
  });
});

describe("G4: class x urgency rows", () => {
  test("both modes report a row per (class, urgency) with its own lead times and rate", () => {
    const d = scoreReplay(result([
      alert(["c:1", "c:2"], "duplicate_work", "high", { sentAt: at("10:30") }),
      alert(["c:1", "c:2"], "concurrent_edit", "low", { sentAt: at("10:50") }), // overlap at LOW
      alert(["c:3", "c:4"], "nearby_work", "low", { sentAt: at("10:40") }),
      alert(["c:5", "c:6"], "concurrent_edit", "high"), // collision on a benign overlap
    ]), labels, { split: "dev", labelsSha256: H }).detectors[0]!;
    const rows = (m: typeof d.pairMatch) => Object.fromEntries(m.classUrgencyRows.map((r) => [`${r.alertClass}/${r.urgency}`,
      [r.alerts, r.truePositives, r.misclassified, r.leadTimesMs.map((l) => l / 60_000), r.alertsPer100Sessions]]));
    expect(rows(d.pairMatch)).toEqual({ "collision/high": [2, 1, 0, [30], 2], "overlap/low": [2, 2, 0, [10, 20], 2] });
    expect(rows(d.classMatch)).toEqual({ "collision/high": [2, 1, 0, [30], 2], "overlap/low": [2, 0, 2, [], 2] });
    expect(d.classMatch.classUrgencyRows.find((r) => r.alertClass === "collision")).toMatchObject({ incidents: 1, recall: 1 });
  });
});

describe("G5 and G7: unmapped kinds, the driver's class check and key validation", () => {
  const once = (a: Partial<ReplayAlert>): ReplayDetector => ({
    name: "one", version: "1", params: {},
    async run(_v, now) {
      return now.toISOString() === at("09:00") ? [{ ...alert(["c:1", "c:2"], "nearby_work", "low"), detector: "one", sentAt: now.toISOString(), ...a }] : [];
    },
  });

  test("the driver accepts a mapped kind with no class, and refuses an unmapped kind with no class or an unknown class", async () => {
    const snap = await loadSnapshot(FIXTURE_DIR);
    const ok = await replaySnapshot(snap, { detectors: [once({})], intervalMs: 3_600_000 });
    expect(ok.alerts.map((a) => [a.kind, a.alertClass])).toEqual([["nearby_work", undefined]]);
    expect((await replaySnapshot(snap, { detectors: [once({ kind: "claim_pair", alertClass: "intent_conflict" })], intervalMs: 3_600_000 })).alerts).toHaveLength(1);
    await expect(replaySnapshot(snap, { detectors: [once({ kind: "claim_pair" })], intervalMs: 3_600_000 })).rejects.toThrow("has no class");
    await expect(replaySnapshot(snap, { detectors: [once({ alertClass: "bogus" as never })], intervalMs: 3_600_000 })).rejects.toThrow("unknown alert class");
    const unmapped = { ...ok, alerts: [{ ...ok.alerts[0]!, kind: "claim_pair" }] };
    expect(() => parseReplayResult(JSON.parse(JSON.stringify(unmapped)))).toThrow("has no class");
  });

  test("key.json is validated, including each member's class", async () => {
    const { key } = buildPacket([result([alert(["c:1", "c:2"], "nearby_work", "low")])], { seed: "s" });
    const copy = () => JSON.parse(JSON.stringify(key));
    expect(parsePacketKey(copy()).entries).toHaveLength(1);
    const bad: Array<[(k: any) => void, string]> = [
      [(k) => { k.entries[0].members[0].alertClass = "bogus"; }, "not a class"],
      [(k) => { delete k.entries[0].members[0].alertClass; k.entries[0].members[0].kind = "claim_pair"; }, "has no class"],
      [(k) => { k.entries[0].members[0].urgency = "medium"; }, "urgency"],
      [(k) => { k.entries[0].alertKey = "../x"; }, "16 hex"],
      [(k) => { k.entries.push(k.entries[0]); }, "repeats"],
      [(k) => { k.format = "x"; }, "format"],
    ];
    for (const [mutate, message] of bad) {
      const k = copy();
      mutate(k);
      expect(() => parsePacketKey(k)).toThrow(message);
    }
    // A legacy member without a class but with a mapped kind is accepted.
    const legacy = copy();
    delete legacy.entries[0].members[0].alertClass;
    expect(() => parsePacketKey(legacy)).not.toThrow();
    const dir = tmp();
    const k = copy();
    k.entries[0].members[0].alertClass = "bogus";
    writeFileSync(join(dir, "key.json"), JSON.stringify(k));
    writeFileSync(join(dir, "v.jsonl"), "");
    await expect(main(["verdicts", "--key", join(dir, "key.json"), "--verdicts", join(dir, "v.jsonl"), "--out", join(dir, "o")], { out: () => {}, err: () => {} }))
      .rejects.toThrow("not a class");
  });
});
