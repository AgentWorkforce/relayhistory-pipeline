/** End-to-end: the real scanOnce replayed over the synthetic fixture, and the CLI around it. */
import { describe, expect, test } from "bun:test";
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { main } from "../replay/cli.ts";
import { advisorDetector, detectorHash, type ReplayDetector } from "../replay/detector.ts";
import { replaySnapshot } from "../replay/replay.ts";
import { loadSnapshot } from "../replay/snapshot.ts";
import { at, FIXTURE_DIR, FIXTURE_LABELS, heldoutName, tmp } from "./helpers.ts";

const EXPECTED = [
  { sentAt: at("09:32"), urgency: "high", kind: "duplicate_work", pair: ["claude:sess-a", "codex:sess-b"], artifact: "number:0042", channel: "dm", alertAt: at("09:31") },
  { sentAt: at("10:22"), urgency: "low", kind: "nearby_work", pair: ["claude:sess-k", "codex:sess-l"], artifact: "src/api", channel: "pull", alertAt: at("10:20") },
  { sentAt: at("10:46"), urgency: "high", kind: "concurrent_edit", pair: ["claude:sess-c", "claude:sess-d"], artifact: "src/billing/invoice.ts", channel: "dm", alertAt: at("10:45") },
  { sentAt: at("11:12"), urgency: "low", kind: "nearby_work", pair: ["claude:sess-e", "codex:sess-f"], artifact: "docs/guide", channel: "pull", alertAt: at("11:10") },
  // The leak test: sess-h's summary matches only from 12:00, so the LOW is not sent at 11:42 when both lanes exist.
  { sentAt: at("12:00"), urgency: "low", kind: "similar_summary", pair: ["claude:sess-g", "codex:sess-h"], artifact: "similar short summaries", channel: "pull", alertAt: at("12:00") },
  // The 12:30 write is invisible until the 12:32 tick; the earlier LOW's cooldown does not hold back a HIGH.
  { sentAt: at("12:32"), urgency: "high", kind: "concurrent_edit", pair: ["claude:sess-g", "codex:sess-h"], artifact: "src/leak/retry.ts", channel: "dm", alertAt: at("12:30") },
];

const fixture = () => loadSnapshot(FIXTURE_DIR, { now: new Date("2026-11-18T00:00:00Z") });

describe("replay of the fixture", () => {
  test("the advisor raises exactly the expected alerts at the expected simulated times", async () => {
    const r = await replaySnapshot(await fixture(), { detectors: [advisorDetector()] });
    expect(r.ticks).toBe(121);
    expect(r.alerts.map(({ sentAt, urgency, kind, pair, artifact, channel, alertAt }) => ({ sentAt, urgency, kind, pair, artifact, channel, alertAt }))).toEqual(EXPECTED);
    for (const a of r.alerts) expect(a).toMatchObject({ detector: "coordination-advisor", orgId: "org_fixture", workspaceId: "ws_fixture" });
    expect(r.detectors[0]!.diagnostics).toMatchObject({ failed: 0, rejected: 0, truncated: 0, dmSent: 6, dmRecorded: 6 });
  });

  test("the planned fleet and the unrelated pair stay silent", async () => {
    const r = await replaySnapshot(await fixture(), { detectors: [advisorDetector()] });
    const named = r.alerts.flatMap((a) => a.pair);
    for (const s of ["sess-lead", "sess-fleet-1", "sess-fleet-2", "sess-i", "sess-j"]) expect(named.some((p) => p.endsWith(`:${s}`))).toBe(false);
  });

  test("deterministic: two replays are identical; a fresh detector per replay has the same hash", async () => {
    const a = await replaySnapshot(await fixture(), { detectors: [advisorDetector()] });
    const b = await replaySnapshot(await fixture(), { detectors: [advisorDetector()] });
    expect(JSON.stringify(b)).toBe(JSON.stringify(a));
    expect(detectorHash(advisorDetector(), { intervalMs: 120_000 })).toBe(a.detectors[0]!.hash);
    expect(detectorHash(advisorDetector(), { intervalMs: 600_000 })).not.toBe(a.detectors[0]!.hash);
    expect(detectorHash(advisorDetector({ sessionHourlyCap: 1 }), { intervalMs: 120_000 })).not.toBe(a.detectors[0]!.hash);
  });

  test("a coarser interval delays sentAt to the next tick, never earlier than the evidence", async () => {
    const r = await replaySnapshot(await fixture(), { detectors: [advisorDetector()], intervalMs: 10 * 60_000 });
    expect(r.alerts.map((a) => a.sentAt)).toEqual([at("09:40"), at("10:30"), at("10:50"), at("11:20"), at("12:00"), at("12:40")]);
  });

  test("with the DM sender off every alert is pull-only", async () => {
    const r = await replaySnapshot(await fixture(), { detectors: [advisorDetector({ dm: false })] });
    expect(r.alerts.every((a) => a.channel === "pull")).toBe(true);
    expect(r.alerts).toHaveLength(6);
  });

  test("dedupes by alert id keeping the first sentAt, and rejects bad options", async () => {
    let n = 0;
    const repeat: ReplayDetector = {
      name: "repeat", version: "1", params: {},
      async run(_v, now) {
        n++;
        return [{ detector: "repeat", alertId: "x", kind: "nearby_work", urgency: "low", orgId: "o", workspaceId: "w", pair: ["a:1", "a:2"], artifact: "f", sentAt: now.toISOString(), alertAt: now.toISOString(), channel: "pull", evidence: [] }];
      },
    };
    const snap = await fixture();
    const r = await replaySnapshot(snap, { detectors: [repeat], intervalMs: 60 * 60_000 });
    expect(n).toBe(5);
    expect(r.alerts.map((a) => a.sentAt)).toEqual([at("09:00")]);
    await expect(replaySnapshot(snap, { detectors: [repeat], intervalMs: 10 })).rejects.toThrow("intervalMs");
    await expect(replaySnapshot(snap, { detectors: [repeat, repeat] })).rejects.toThrow("unique names");
  });
});

describe("the M3-1 controls in replay", () => {
  test("controls default ON within the eval, are hashed, and the advisor feature off gives no alerts", async () => {
    const on = await replaySnapshot(await fixture(), { detectors: [advisorDetector()] });
    expect(on.detectors[0]!.params.controls).toEqual({ advisor: true, conflicts: true, dm: true, claims: true, regen: true });
    const off = await replaySnapshot(await fixture(), { detectors: [advisorDetector({ controls: { advisor: false } })] });
    expect(off.alerts).toEqual([]);
    expect(off.detectors[0]!.diagnostics).toMatchObject({ workspaces: 0, written: 0, dmSent: 0 });
    expect(off.detectors[0]!.hash).not.toBe(on.detectors[0]!.hash);
  });

  test("the dm feature off keeps every alert on the pull path", async () => {
    const r = await replaySnapshot(await fixture(), { detectors: [advisorDetector({ controls: { dm: false } })] });
    expect(r.alerts).toHaveLength(6);
    expect(r.alerts.every((a) => a.channel === "pull")).toBe(true);
  });
});

describe("cli", () => {
  test("replay, score dev and held-out (once), packet and verdicts end to end", async () => {
    const out = tmp("si-cli-");
    const lines: string[] = [];
    const io = { out: (l: string) => lines.push(l), err: (l: string) => lines.push(l), now: new Date("2026-11-18T00:00:00Z") };
    expect(await main(["replay", "--snapshot", FIXTURE_DIR, "--out", join(out, "replay")], io)).toBe(0);
    for (const f of ["alerts.json", "report.json", "report.md"]) expect(existsSync(join(out, "replay", f))).toBe(true);
    const alerts = join(out, "replay", "alerts.json");

    expect(await main(["score", "--alerts", alerts, "--labels", FIXTURE_LABELS, "--out", join(out, "score")], io)).toBe(0);
    const dev = JSON.parse(readFileSync(join(out, "score", "report.dev.json"), "utf8"));
    // The held-out billing pair is only unlabeled on dev: dev never reads held-out labels.
    expect(dev.detectors[0].pairMatch.total).toMatchObject({ alerts: 6, truePositives: 4, misclassified: 0, hardNegativeHits: 1, unlabeled: 1, outOfScope: 0, alertPrecision: 4 / 6, recall: 1, beforeHarm: 3, medianLeadMs: 58 * 60_000 });
    // class-match: the sess-k/sess-l LOW nearby_work (overlap) on an intent_conflict incident is misclassified,
    // and so is the sess-g/sess-h HIGH shared file (collision) on an overlap incident. The sess-e/sess-f LOW
    // nearby_work on its benign_overlap hard negative is class-correct (a TP with no incident to credit).
    expect(dev.detectors[0].classMatch.total).toMatchObject({ alerts: 6, truePositives: 3, misclassified: 2, falsePositives: 1, alertPrecision: 3 / 6, detected: 2, recall: 2 / 3 });
    expect(dev.detectors[0].confusion.counts.overlap).toMatchObject({ intent_conflict: 1, overlap: 2, none: 0, unlabeled: 0 });
    expect(dev.detectors[0].confusion.counts.collision).toMatchObject({ collision: 1, overlap: 1, unlabeled: 1 });
    expect(readFileSync(join(out, "score", "report.dev.md"), "utf8")).toContain("### Confusion (alert class by labeled class)");
    expect(existsSync(join(out, "score", "scored-hashes.json"))).toBe(false);

    expect(await main(["score", "--alerts", alerts, "--labels", FIXTURE_LABELS, "--out", join(out, "score"), "--split", "heldout"], io)).toBe(0);
    const held = JSON.parse(readFileSync(join(out, "score", heldoutName(alerts)), "utf8"));
    expect(held.detectors[0].pairMatch.total).toMatchObject({ alerts: 1, truePositives: 1, alertPrecision: 1, recall: 1, beforeHarm: 0, afterHarm: 1, medianLeadMs: -16 * 60_000 });
    await expect(main(["score", "--alerts", alerts, "--labels", FIXTURE_LABELS, "--out", join(out, "score"), "--split", "heldout"], io)).rejects.toThrow("already scored");
    expect(await main(["score", "--alerts", alerts, "--labels", FIXTURE_LABELS, "--out", join(out, "score"), "--split", "heldout", "--force"], io)).toBe(0);
    const ledger = JSON.parse(readFileSync(join(out, "score", "scored-hashes.json"), "utf8"));
    expect(ledger.entries.map((e: { forced: boolean }) => e.forced)).toEqual([false, true]);

    expect(await main(["packet", "--alerts", alerts, "--seed", "fixture-seed", "--budget", "50", "--out", join(out, "packet")], io)).toBe(0);
    const packet = readFileSync(join(out, "packet", "packet.jsonl"), "utf8").trim().split("\n").map((l) => JSON.parse(l));
    expect(packet).toHaveLength(6);
    const verdicts = packet.flatMap((p: { alertKey: string }, i: number) => [
      { alertKey: p.alertKey, verdict: "real_overlap", adjudicator: "adj-1" },
      { alertKey: p.alertKey, verdict: i === 0 ? "planned" : "real_overlap", adjudicator: "adj-2" },
    ]);
    writeFileSync(join(out, "verdicts.jsonl"), verdicts.map((v: unknown) => JSON.stringify(v)).join("\n"));
    expect(await main(["verdicts", "--key", join(out, "packet", "key.json"), "--verdicts", join(out, "verdicts.jsonl"), "--out", join(out, "verdicts")], io)).toBe(0);
    const vr = JSON.parse(readFileSync(join(out, "verdicts", "report.json"), "utf8"));
    expect(vr.agreement[0]).toMatchObject({ a: "adj-1", b: "adj-2", shared: 6, agreed: 5 });
  });

  test("refuses an output directory inside the repository, a past-deleteBy snapshot, and unknown flags", async () => {
    const io = { out: () => {}, err: () => {}, now: new Date("2026-11-18T00:00:00Z") };
    await expect(main(["replay", "--snapshot", FIXTURE_DIR, "--out", join(FIXTURE_DIR, "..", "out")], io)).rejects.toThrow("inside the repository");
    await expect(main(["replay", "--snapshot", FIXTURE_DIR, "--out", tmp()], { ...io, now: new Date("2100-01-01T00:00:00Z") })).rejects.toThrow("deleteBy");
    await expect(main(["replay", "--snapshot", FIXTURE_DIR, "--out", tmp(), "--token", "x"], io)).rejects.toThrow();
    expect(await main(["nope"], io)).toBe(2);
  });
});
