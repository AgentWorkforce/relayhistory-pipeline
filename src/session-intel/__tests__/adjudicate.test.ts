/** Blind packets: pooled, stripped, deterministically shuffled; verdicts: validated, agreement and precision. */
import { describe, expect, test } from "bun:test";
import { buildPacket, consensus, parseVerdicts, verdictReport } from "../replay/adjudicate.ts";
import type { ReplayAlert } from "../replay/detector.ts";
import type { ReplayResult } from "../replay/replay.ts";
import { at } from "./helpers.ts";

const alert = (detector: string, n: number, over: Partial<ReplayAlert> = {}): ReplayAlert => ({
  detector, alertId: `${detector}-secret-${n}`, kind: "secret_kind", urgency: "high", orgId: "o", workspaceId: "w",
  pair: [`c:${n}`, `c:${n + 100}`], artifact: `src/f${n}.ts`, sentAt: at(`1${n % 10}:00`), alertAt: at(`1${n % 10}:00`), channel: "dm",
  evidence: [{ sessionId: `${n}`, eventId: `e${n}`, ts: at("09:00"), kind: "secret_event_kind" }], alertClass: "collision", ...over,
});
const result = (alerts: ReplayAlert[], names: string[]): ReplayResult => ({
  format: "session-intel-replay/2", incomplete: [], snapshot: { manifestSha256: "f".repeat(64), createdAt: at("00:00"), window: { since: at("09:00"), until: at("13:00") }, sessions: 100 },
  intervalMs: 120_000, ticks: 121, detectors: names.map((name, i) => ({ name, version: "1", params: {}, hash: String(i).repeat(64), diagnostics: {} })), alerts,
});
const pooled = () => result([
  ...[1, 2, 3, 4, 5, 6].map((n) => alert("detector-alpha", n)),
  alert("detector-beta", 1, { alertId: "beta-secret-1", pair: ["c:101", "c:1"] }), // the same pair and artifact as alpha's 1
  alert("detector-beta", 9),
], ["detector-alpha", "detector-beta"]);

describe("buildPacket", () => {
  test("pools the top-K per detector, merges the same pair and artifact, and strips detector, kind, urgency and ids", () => {
    const { packet, key } = buildPacket([pooled()], { seed: "s1" });
    // K = 5 per detector at 5 per 100 on 100 sessions; alpha's 6th is cut; beta's first merges with alpha's.
    expect(packet).toHaveLength(6);
    const text = JSON.stringify(packet);
    for (const secret of ["detector-alpha", "detector-beta", "secret", "high", "dm", "concurrent"]) expect(text).not.toContain(secret);
    for (const p of packet) expect(Object.keys(p).sort()).toEqual(["alertKey", "artifact", "evidence", "pair"]);
    const merged = key.entries.find((e) => e.artifact === "src/f1.ts")!;
    expect(merged.members.map((m) => m.detector)).toEqual(["detector-alpha", "detector-beta"]);
    expect(packet.find((p) => p.artifact === "src/f6.ts")).toBeUndefined();
  });

  test("the shuffle is deterministic for a seed and changes with it", () => {
    const a = buildPacket([pooled()], { seed: "s1" }).packet;
    const b = buildPacket([pooled()], { seed: "s1" }).packet;
    expect(b).toEqual(a);
    const orders = new Set(["s2", "s3", "s4", "s5"].map((seed) => buildPacket([pooled()], { seed }).packet.map((p) => p.artifact).join()));
    expect(orders.size).toBeGreaterThan(1);
    expect(buildPacket([pooled()], { seed: "s2" }).packet.map((p) => p.alertKey)).not.toEqual(a.map((p) => p.alertKey));
    expect(() => buildPacket([pooled()], { seed: "" })).toThrow("seed");
  });
});

describe("verdicts", () => {
  test("refuses unknown keys, bad verdicts and repeats by one adjudicator", () => {
    const { key } = buildPacket([pooled()], { seed: "s1" });
    const k = key.entries[0]!.alertKey;
    expect(() => parseVerdicts([{ alertKey: "nope", verdict: "planned", adjudicator: "x" }], key)).toThrow("not in the key");
    expect(() => parseVerdicts([{ alertKey: k, verdict: "maybe", adjudicator: "x" }], key)).toThrow("verdict must be");
    expect(() => parseVerdicts([{ alertKey: k, verdict: "planned", adjudicator: "x" }, { alertKey: k, verdict: "unrelated", adjudicator: "x" }], key)).toThrow("repeats");
  });

  test("consensus, agreement and Cohen's kappa, and precision per detector", () => {
    expect(consensus(["planned", "planned", "real_overlap"])).toBe("planned");
    expect(consensus(["planned", "real_overlap"])).toBe("unclear");
    expect(consensus([])).toBeNull();
    const { key } = buildPacket([pooled()], { seed: "s1" });
    const byArtifact = new Map(key.entries.map((e) => [e.artifact, e.alertKey]));
    const v = (artifact: string, verdict: string, adjudicator: string) => ({ alertKey: byArtifact.get(artifact)!, verdict, adjudicator });
    const verdicts = parseVerdicts([
      v("src/f1.ts", "real_overlap", "a"), v("src/f1.ts", "real_overlap", "b"),
      v("src/f2.ts", "planned", "a"), v("src/f2.ts", "planned", "b"),
      v("src/f3.ts", "unrelated", "a"), v("src/f3.ts", "real_overlap", "b"),
      v("src/f4.ts", "intent_conflict", "a"), v("src/f4.ts", "intent_conflict", "b"),
      v("src/f9.ts", "unclear", "a"),
    ], key);
    const r = verdictReport(key, verdicts);
    expect(r.adjudicators).toEqual(["a", "b"]);
    expect(r.agreement).toEqual([{ a: "a", b: "b", shared: 4, agreed: 3, rate: 0.75, kappa: expect.any(Number) }]);
    // po = 0.75; pe = a{ro .25, pl .25, un .25, ic .25} x b{ro .5, pl .25, ic .25} = .125 + .0625 + .0625 = .25; kappa = (.75 - .25) / .75.
    expect(r.agreement[0]!.kappa).toBeCloseTo(2 / 3, 10);
    expect(r.adjudicatedEntries).toBe(5);
    const alpha = r.detectors.find((d) => d.detector === "detector-alpha")!;
    // alpha pooled f1..f5: f1 positive, f2 planned, f3 tie -> unclear, f4 positive, f5 not adjudicated.
    expect(alpha).toMatchObject({ pooled: 5, adjudicated: 4, positive: 2, negative: 1, unclear: 1, precision: 2 / 3 });
    const beta = r.detectors.find((d) => d.detector === "detector-beta")!;
    expect(beta).toMatchObject({ pooled: 2, adjudicated: 2, positive: 1, negative: 0, unclear: 1, precision: 1 });
  });
});
