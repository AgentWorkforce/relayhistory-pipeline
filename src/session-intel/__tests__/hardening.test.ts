/**
 * Review hardening (M0-1 lander findings): per-pair precision, duplicate replays in a packet, incomplete
 * replays, the --out guard, the retention sweep, and readFacts' session-existence filter.
 */
import { describe, expect, test } from "bun:test";
import { readFileSync, symlinkSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { buildPacket } from "../replay/adjudicate.ts";
import { isInside, main, outDir } from "../replay/cli.ts";
import { ADVISOR_SOURCES, advisorDetector, type ReplayAlert, type ReplayDetector } from "../replay/detector.ts";
import { parseLabels } from "../replay/labels.ts";
import { parseReplayResult, replaySnapshot, type ReplayResult } from "../replay/replay.ts";
import { recordHeldoutScoring, scoreReplay } from "../replay/score.ts";
import { loadSnapshot, type Snapshot } from "../replay/snapshot.ts";
import { ReplayView, snapshotCoordinationBackend, snapshotDirectory, type SimClock } from "../replay/views.ts";
import { at, FIXTURE_DIR, FIXTURE_LABELS, heldoutName, session, tmp, writeSnapshot } from "./helpers.ts";

const H = "a".repeat(64);
const REPO = join(FIXTURE_DIR, "..", "..", "..", "..", "..");
const alert = (pair: [string, string], kind: string, urgency: "high" | "low", over: Partial<ReplayAlert> = {}): ReplayAlert => ({
  detector: "d1", alertId: `${pair.join("|")}/${kind}/${urgency}`, kind, urgency, orgId: "o", workspaceId: "w", pair, artifact: "f",
  sentAt: at("10:00"), alertAt: at("10:00"), channel: "pull", evidence: [], ...over,
});
const result = (alerts: ReplayAlert[], over: Partial<ReplayResult> = {}): ReplayResult => ({
  format: "session-intel-replay/2", snapshot: { manifestSha256: H, createdAt: at("00:00"), window: { since: at("09:00"), until: at("13:00") }, sessions: 100 },
  intervalMs: 120_000, ticks: 121, detectors: [{ name: "d1", version: "1", params: {}, hash: H, diagnostics: {} }], incomplete: [], alerts, ...over,
});
const labels = parseLabels({
  incidents: [
    { id: "i1", pair: ["c:1", "c:2"], kind: "overlap", harmAt: at("11:00"), split: "dev", workstream: "w1" },
    { id: "i2", pair: ["c:3", "c:4"], kind: "duplicate_work", harmAt: at("11:00"), split: "dev", workstream: "w2" },
    { id: "h1", pair: ["c:7", "c:8"], kind: "overlap", harmAt: at("11:00"), split: "heldout", workstream: "w3" },
  ],
  hardNegatives: [{ pair: ["c:5", "c:6"], reason: "benign_overlap" }],
});

describe("F1: several alerts on one pair", () => {
  test("precision per alert and per (detector, pair) are both reported", () => {
    const d = scoreReplay(result([
      alert(["c:1", "c:2"], "nearby_work", "low"), alert(["c:1", "c:2"], "concurrent_edit", "high", { artifact: "g" }), // one pair, two alerts
      alert(["c:3", "c:4"], "duplicate_work", "high"),
      alert(["c:5", "c:6"], "nearby_work", "low"), // hard negative
      alert(["c:1", "c:9"], "nearby_work", "low"), // unlabeled
    ]), labels, { split: "dev", labelsSha256: H }).detectors[0]!;
    expect(d.pairMatch.total).toMatchObject({ alerts: 5, truePositives: 3, alertPrecision: 3 / 5, pairs: 4, truePositivePairs: 2, pairPrecision: 2 / 4 });
    // class-match: the c:1/c:2 HIGH (collision) is misclassified on an overlap incident, but the pair still has its
    // LOW TP; the LOW nearby_work on the benign_overlap hard negative is class-correct.
    expect(d.classMatch.total).toMatchObject({ truePositives: 3, misclassified: 1, alertPrecision: 3 / 5, truePositivePairs: 3, misclassifiedPairs: 0, pairPrecision: 3 / 4 });
    const only = scoreReplay(result([alert(["c:1", "c:2"], "concurrent_edit", "high")]), labels, { split: "dev", labelsSha256: H }).detectors[0]!;
    expect(only.classMatch.total).toMatchObject({ pairs: 1, truePositivePairs: 0, misclassifiedPairs: 1, pairPrecision: 0 });
  });

  test("a packet keeps one member per detector per entry and refuses the same replay twice", () => {
    const r = result([alert(["c:1", "c:2"], "nearby_work", "low"), alert(["c:1", "c:2"], "nearby_work", "high", { alertId: "upgraded" })]);
    const { key } = buildPacket([r], { seed: "s" });
    expect(key.entries).toHaveLength(1);
    expect(key.entries[0]!.members).toHaveLength(1);
    expect(key.entries[0]!.members[0]!.urgency).toBe("high"); // ranked HIGH first
    expect(() => buildPacket([r, structuredClone(r)], { seed: "s" })).toThrow("given twice");
    // The same detector on another snapshot is a separate source.
    expect(() => buildPacket([r, { ...r, snapshot: { ...r.snapshot, manifestSha256: "b".repeat(64) } }], { seed: "s" })).not.toThrow();
  });

  test("the packet CLI refuses the same alerts file twice", async () => {
    const dir = tmp();
    const file = join(dir, "alerts.json");
    writeFileSync(file, JSON.stringify(result([alert(["c:1", "c:2"], "nearby_work", "low")])));
    await expect(main(["packet", "--alerts", file, "--alerts", join(dir, ".", "alerts.json"), "--seed", "s", "--out", join(dir, "p")], { out: () => {}, err: () => {} }))
      .rejects.toThrow("given twice");
  });
});

describe("F2: an incomplete replay", () => {
  const failing: ReplayDetector = {
    name: "failing", version: "1", params: {}, async run() { return []; }, diagnostics: () => ({ failed: 2, rejected: 0 }),
  };

  test("carries its reasons in the result, and alerts.json must state them", async () => {
    const r = await replaySnapshot(await loadSnapshot(FIXTURE_DIR), { detectors: [failing], intervalMs: 3_600_000 });
    expect(r.incomplete).toEqual(["failing failed=2"]);
    const { incomplete: _, ...missing } = r;
    expect(() => parseReplayResult(missing)).toThrow("incomplete");
    expect(() => parseReplayResult({ ...r, format: "session-intel-replay/1" })).toThrow("format");
  });

  test("held-out scoring refuses it (scorer, ledger and CLI); dev scoring reports it", async () => {
    const r = result([], { incomplete: ["d1 failed=1"] });
    expect(() => scoreReplay(r, labels, { split: "heldout", labelsSha256: H })).toThrow("incomplete");
    const out = tmp();
    await expect(recordHeldoutScoring(out, r, H)).rejects.toThrow("incomplete");
    expect(scoreReplay(r, labels, { split: "dev", labelsSha256: H }).incomplete).toEqual(["d1 failed=1"]);
    const file = join(out, "alerts.json");
    writeFileSync(file, JSON.stringify(r));
    const io = { out: () => {}, err: () => {} };
    await expect(main(["score", "--alerts", file, "--labels", FIXTURE_LABELS, "--out", join(out, "s"), "--split", "heldout"], io)).rejects.toThrow("incomplete");
    // Nothing was recorded: a later complete replay of the same detector can still be scored once.
    expect(() => readFileSync(join(out, "s", "scored-hashes.json"))).toThrow();
  });
});

describe("F3: split-named score reports", () => {
  test("a dev run after the held-out run leaves the held-out report as it was", async () => {
    const out = tmp();
    const io = { out: () => {}, err: () => {}, now: new Date("2026-11-18T00:00:00Z") };
    await main(["replay", "--snapshot", FIXTURE_DIR, "--out", join(out, "r")], io);
    const alerts = join(out, "r", "alerts.json");
    await main(["score", "--alerts", alerts, "--labels", FIXTURE_LABELS, "--out", join(out, "s"), "--split", "heldout"], io);
    const held = readFileSync(join(out, "s", heldoutName(alerts)), "utf8");
    await main(["score", "--alerts", alerts, "--labels", FIXTURE_LABELS, "--out", join(out, "s")], io);
    expect(readFileSync(join(out, "s", heldoutName(alerts)), "utf8")).toBe(held);
    expect(JSON.parse(readFileSync(join(out, "s", "report.dev.json"), "utf8")).split).toBe("dev");
  });
});

describe("F4: the detector hash covers the replay code and the interval", () => {
  test("the hashed sources include the driver, the adapter and the relay sender", () => {
    for (const f of ["./detector.ts", "./replay.ts", "../../coordination-advisor/relay-sender.ts", "./views.ts", "../../coordination-advisor/scan.ts"])
      expect(ADVISOR_SOURCES).toContain(f);
  });
});

describe("F5: the --out guard", () => {
  test("a name starting with '..' inside the repository is inside", () => {
    expect(isInside(REPO, join(REPO, "..cache", "out"))).toBe(true);
    expect(() => outDir(join(REPO, "..cache", "out"), false, REPO)).toThrow("inside the repository");
    expect(isInside(REPO, join(REPO, "..", "elsewhere"))).toBe(false);
    expect(isInside(REPO, REPO)).toBe(true);
  });

  test("a symlink outside the repository that points into it is inside; a plain temp dir is outside", () => {
    const dir = tmp();
    symlinkSync(join(REPO, "src", "session-intel", "__tests__", "fixtures"), join(dir, "link"));
    expect(() => outDir(join(dir, "link", "not-yet", "out"), false, REPO)).toThrow("inside the repository");
    expect(outDir(join(dir, "real", "out"), false, REPO)).toBe(join(dir, "real", "out"));
    expect(outDir(join(dir, "link", "x"), true, REPO)).toBe(join(dir, "link", "x"));
  });
});

describe("F6: the replay sweeps hourly, as advisorTick does", () => {
  test("a row older than 7 days expires and the same finding is written and DMed again", async () => {
    const day = (d: number, hm: string) => new Date(Date.parse(at(hm)) + d * 86_400_000).toISOString();
    const lane = (sessionId: string, branch: string, min: string) => session(sessionId, {
      createdAt: day(0, "09:00"), lastActivityAt: day(8, "12:00"), projectId: "proj-sweep", repo: "fixture-org/sweep",
      writes: [0, 8].map((d) => ({ path: "src/sweep/file.ts", ts: day(d, min), eventId: `${sessionId}-${d}`, kind: "tool_result", branch })),
    });
    const dir = writeSnapshot(tmp(), [lane("s1", "b1", "10:00"), lane("s2", "b2", "10:30")],
      { window: { since: day(0, "09:00"), until: day(8, "13:00") } });
    const r = await replaySnapshot(await loadSnapshot(dir), { detectors: [advisorDetector()], intervalMs: 3_600_000 });
    // One alert (the first sentAt wins), but the store wrote it twice: day 0 and, after the sweep, day 8.
    expect(r.alerts.map((a) => [a.kind, a.sentAt])).toEqual([["concurrent_edit", day(0, "11:00")]]);
    expect(r.detectors[0]!.diagnostics).toMatchObject({ written: 4, dmSent: 4, sweptExpired: 2, sweptDeleted: 2, failed: 0 });
  });
});

describe("F9: readFacts never returns a session created after now", () => {
  test("a fact before now on a session created after now is invisible", async () => {
    // The loader refuses a fact before createdAt, so this snapshot is built by hand: the filter is defence in depth.
    const base = await loadSnapshot(writeSnapshot(tmp(), [
      session("early", { writes: [{ path: "src/a/x.ts", ts: at("10:00"), eventId: "w1", kind: "tool" }] }),
    ]));
    const late = { ...base.sessions[0]!, sessionId: "late", createdAt: at("11:30"), writes: [{ path: "src/a/x.ts", ts: at("10:30"), eventId: "w2", kind: "tool", branch: null }] };
    const snapshot: Snapshot = { ...base, sessions: [...base.sessions, late] };
    const view = new ReplayView(snapshot);
    const clock: SimClock = { now: new Date(at("11:00")) };
    const facts = await snapshotCoordinationBackend(view, clock).readFacts(snapshotDirectory(view, clock),
      { orgId: "org_fixture", workspaceId: "ws_fixture", since: new Date(at("09:00")), until: new Date(at("11:00")) });
    expect(facts.lanes.map((l) => l.sessionId)).toEqual(["early"]);
    clock.now = new Date(at("12:00"));
    const later = await snapshotCoordinationBackend(view, clock).readFacts(snapshotDirectory(view, clock),
      { orgId: "org_fixture", workspaceId: "ws_fixture", since: new Date(at("09:00")), until: new Date(at("12:00")) });
    expect(later.lanes.map((l) => l.sessionId).sort()).toEqual(["early", "late"]);
  });
});
