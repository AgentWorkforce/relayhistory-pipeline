/**
 * Lander follow-up on be087573: the ledger lock (N1), held-out report naming and ordering (N2), the alerts
 * reader (N3), the key reader (N4), ledger validation (N5), the scorer hash and the interval (N7), and the DM
 * volume in the report (N8). N6 is in class-review.test.ts.
 */
import { describe, expect, test } from "bun:test";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { buildPacket, parsePacketKey } from "../replay/adjudicate.ts";
import { main } from "../replay/cli.ts";
import { ADVISOR_SOURCES, advisorDetector, type ReplayAlert } from "../replay/detector.ts";
import { parseReplayResult, replaySnapshot, type ReplayResult } from "../replay/replay.ts";
import { LEDGER_FILE, LOCK_FILE, readLedger, recordHeldoutScoring, SCORER_SHA256 } from "../replay/score.ts";
import { loadSnapshot } from "../replay/snapshot.ts";
import { at, FIXTURE_DIR, FIXTURE_LABELS, tmp } from "./helpers.ts";

const H = "a".repeat(64);
const alert = (n: number, over: Partial<ReplayAlert> = {}): ReplayAlert => ({
  detector: "d1", alertId: `a${n}`, kind: "nearby_work", urgency: "low", orgId: "o", workspaceId: "w", pair: [`c:${n}`, `c:${n + 100}`],
  artifact: "f", sentAt: at("10:00"), alertAt: at("10:00"), channel: "pull", evidence: [], ...over,
});
const result = (alerts: ReplayAlert[] = [], over: Partial<ReplayResult> = {}): ReplayResult => ({
  format: "session-intel-replay/2", snapshot: { manifestSha256: H, createdAt: at("00:00"), window: { since: at("09:00"), until: at("13:00") }, sessions: 100 },
  intervalMs: 120_000, ticks: 121, detectors: [{ name: "d1", version: "1", params: {}, hash: H, diagnostics: {} }], incomplete: [], alerts, ...over,
});
const io = { out: () => {}, err: () => {}, now: new Date("2026-11-18T00:00:00Z") };
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

describe("N1: the ledger lock", () => {
  test("two concurrent held-out scorings: exactly one records, and the lock is gone afterwards", async () => {
    const out = tmp();
    const slow = { beforeRecord: () => sleep(50) };
    const runs = await Promise.allSettled([recordHeldoutScoring(out, result(), H, slow), recordHeldoutScoring(out, result(), H, slow)]);
    expect(runs.map((r) => r.status).sort()).toEqual(["fulfilled", "rejected"]);
    expect(String((runs.find((r) => r.status === "rejected") as PromiseRejectedResult).reason)).toMatch(/lock|already scored/);
    expect(readLedger(out).entries).toHaveLength(1);
    expect(existsSync(join(out, LOCK_FILE))).toBe(false);
  });

  test("a stale lock is refused with a message naming the file; nothing is recorded", async () => {
    const out = tmp();
    writeFileSync(join(out, LOCK_FILE), "12345\n");
    await expect(recordHeldoutScoring(out, result(), H)).rejects.toThrow(join(out, LOCK_FILE));
    expect(existsSync(join(out, LEDGER_FILE))).toBe(false);
    expect(existsSync(join(out, LOCK_FILE))).toBe(true); // never removed by the refused run
  });

  test("a failing report write releases the lock and leaves the hash unspent", async () => {
    const out = tmp();
    await expect(recordHeldoutScoring(out, result(), H, { beforeRecord: async () => { throw new Error("disk full"); } })).rejects.toThrow("disk full");
    expect(existsSync(join(out, LOCK_FILE))).toBe(false);
    expect(existsSync(join(out, LEDGER_FILE))).toBe(false);
    await recordHeldoutScoring(out, result(), H);
    expect(readLedger(out).entries).toHaveLength(1);
  });
});

describe("N2: held-out reports", () => {
  test("a forced re-score gets its own report name; nothing is overwritten", async () => {
    const out = tmp();
    await main(["replay", "--snapshot", FIXTURE_DIR, "--out", join(out, "r")], io);
    const args = ["score", "--alerts", join(out, "r", "alerts.json"), "--labels", FIXTURE_LABELS, "--out", join(out, "s"), "--split", "heldout"];
    expect(await main(args, io)).toBe(0);
    const first = readFileSync(join(out, "s", "report.heldout.json"), "utf8");
    expect(await main([...args, "--force", "--reason", "one"], io)).toBe(0);
    expect(await main([...args, "--force", "--reason", "two"], io)).toBe(0);
    expect(readFileSync(join(out, "s", "report.heldout.json"), "utf8")).toBe(first);
    for (const f of ["report.heldout.forced-1.json", "report.heldout.forced-1.md", "report.heldout.forced-2.json", "report.heldout.forced-2.md"])
      expect(existsSync(join(out, "s", f))).toBe(true);
    expect(readLedger(join(out, "s")).entries.map((e) => e.forced)).toEqual([false, true, true]);
  });

  test("the report is written before the ledger entry: a report that cannot be written does not burn the hash", async () => {
    const out = tmp();
    await main(["replay", "--snapshot", FIXTURE_DIR, "--out", join(out, "r")], io);
    const args = ["score", "--alerts", join(out, "r", "alerts.json"), "--labels", FIXTURE_LABELS, "--out", join(out, "s"), "--split", "heldout"];
    mkdirSync(join(out, "s"), { recursive: true });
    writeFileSync(join(out, "s", "report.heldout.md"), "left over");
    await expect(main(args, io)).rejects.toThrow("never overwritten");
    expect(existsSync(join(out, "s", LEDGER_FILE))).toBe(false);
    expect(existsSync(join(out, "s", "report.heldout.json"))).toBe(false); // the half-written pair was removed
    const { rmSync } = await import("node:fs");
    rmSync(join(out, "s", "report.heldout.md"));
    expect(await main(args, io)).toBe(0);
    expect(readLedger(join(out, "s")).entries).toHaveLength(1);
  });
});

describe("N3: the alerts reader", () => {
  test("refuses two detectors with one name and a repeated (detector, alertId)", () => {
    const two = result([], { detectors: [result().detectors[0]!, { ...result().detectors[0]!, hash: "b".repeat(64) }] });
    expect(() => parseReplayResult(two)).toThrow("same name");
    expect(() => parseReplayResult(result([alert(1), alert(2, { alertId: "a1" })]))).toThrow("repeats");
    expect(() => parseReplayResult(result([alert(1), alert(2)]))).not.toThrow();
  });

  test("derives incomplete from the diagnostics, whatever the stored flag says", () => {
    const edited = result([], { detectors: [{ ...result().detectors[0]!, diagnostics: { failed: 3, rejected: 0 } }], incomplete: [] });
    expect(parseReplayResult(edited).incomplete).toEqual(["d1 failed=3"]);
    expect(parseReplayResult(result([], { incomplete: ["d1 failed=3"], detectors: [{ ...result().detectors[0]!, diagnostics: { failed: 3 } }] })).incomplete)
      .toEqual(["d1 failed=3"]);
    expect(() => parseReplayResult(result([], { detectors: [{ ...result().detectors[0]!, diagnostics: { failed: "x" as never } }] }))).toThrow("numbers");
  });
});

describe("N4: the key reader", () => {
  test("refuses two members with the same detectorHash in one entry", () => {
    const { key } = buildPacket([result([alert(1)])], { seed: "s" });
    const k = JSON.parse(JSON.stringify(key));
    k.entries[0].members.push({ ...k.entries[0].members[0], alertId: "other" });
    expect(() => parsePacketKey(k)).toThrow("repeats a detectorHash");
  });
});

describe("N5: ledger validation", () => {
  const good = { detector: "d1", detectorHash: H, split: "heldout", labelsSha256: H, manifestSha256: H, scoredAt: at("09:00"), forced: false };
  const cases: Array<[unknown, string]> = [
    [{ ...good, split: "dev" }, "split"],
    [{ ...good, detectorHash: "abc" }, "detectorHash"],
    [{ ...good, labelsSha256: undefined }, "labelsSha256"],
    [{ ...good, classMapSha256: "x" }, "classMapSha256"],
    [{ ...good, scorerSha256: 1 }, "scorerSha256"],
    [{ ...good, scoredAt: "yesterday" }, "scoredAt"],
    [{ ...good, forced: "no" }, "forced"],
    [{ ...good, reason: "r".repeat(501) }, "reason"],
    [null, "not an object"],
  ];
  for (const [entry, field] of cases) test(`a damaged entry (${field}) is an error naming the file`, async () => {
    const out = tmp();
    writeFileSync(join(out, LEDGER_FILE), JSON.stringify({ format: "session-intel-scored-hashes/1", entries: [good, entry] }));
    expect(() => readLedger(out)).toThrow(field);
    expect(() => readLedger(out)).toThrow(join(out, LEDGER_FILE));
    await expect(recordHeldoutScoring(out, result([], { detectors: [{ ...result().detectors[0]!, hash: "e".repeat(64) }] }), H)).rejects.toThrow("damaged");
  });
  test("invalid JSON is an error naming the file", () => {
    const out = tmp();
    writeFileSync(join(out, LEDGER_FILE), "{");
    expect(() => readLedger(out)).toThrow(`${join(out, LEDGER_FILE)} is damaged`);
  });
});

describe("N7: hashes and the interval", () => {
  test("the detector hash covers snapshot.ts and util.ts, and another interval is another hash", async () => {
    expect(ADVISOR_SOURCES).toEqual(expect.arrayContaining(["./snapshot.ts", "./util.ts"]));
    const snap = await loadSnapshot(FIXTURE_DIR);
    const a = await replaySnapshot(snap, { detectors: [advisorDetector()], intervalMs: 120_000 });
    const b = await replaySnapshot(snap, { detectors: [advisorDetector()], intervalMs: 300_000 });
    expect(a.detectors[0]!.params).toEqual(b.detectors[0]!.params);
    expect(a.detectors[0]!.hash).not.toBe(b.detectors[0]!.hash);
  });

  test("the scorer hash is in the report and in the ledger entry", async () => {
    const out = tmp();
    await main(["replay", "--snapshot", FIXTURE_DIR, "--out", join(out, "r")], io);
    await main(["score", "--alerts", join(out, "r", "alerts.json"), "--labels", FIXTURE_LABELS, "--out", join(out, "s"), "--split", "heldout"], io);
    expect(JSON.parse(readFileSync(join(out, "s", "report.heldout.json"), "utf8")).scorerSha256).toBe(SCORER_SHA256);
    expect(readLedger(join(out, "s")).entries[0]!.scorerSha256).toBe(SCORER_SHA256);
  });
});

describe("N8: DM volume in the report", () => {
  test("report.md shows the diagnostics' dmSent and written counts", async () => {
    const out = tmp();
    await main(["replay", "--snapshot", FIXTURE_DIR, "--out", join(out, "r")], io);
    await main(["score", "--alerts", join(out, "r", "alerts.json"), "--labels", FIXTURE_LABELS, "--out", join(out, "s")], io);
    expect(readFileSync(join(out, "s", "report.dev.md"), "utf8")).toContain("DM volume: 6 DMs sent and 12 rows written");
    expect(JSON.parse(readFileSync(join(out, "s", "report.dev.json"), "utf8")).detectors[0].diagnostics).toMatchObject({ dmSent: 6, written: 12 });
  });
});
