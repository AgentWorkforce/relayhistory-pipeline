import { describe, expect, test } from "bun:test";
import type { EventFacts } from "../cloud-harvest/features.ts";
import type { DirectoryRow, SessionFacts } from "../cloud-harvest/harvest.ts";
import type { Alert, Context } from "../overlap-bench/detectors.ts";
import { adjudicationSample, pairId, precisionOf, wilson } from "../overlap-bench/eval.ts";
import { buildModel, communicated } from "../overlap-bench/model.ts";
import { rank, type Components } from "../overlap-bench/ranker.ts";
import { budgetCut } from "../overlap-bench/score.ts";

const alert = (at: string, score: number, sides: [string, string] = ["root:a", "root:b"]): Alert => ({ detector: "t", sides, at, artifact: "x", score, owners: ["K", "K"] });

describe("eval metrics", () => {
  test("wilson brackets the rate and is empty without judgements", () => {
    expect(wilson(0, 0)).toBeNull();
    const w = wilson(8, 10)!;
    expect(w.low).toBeGreaterThan(0.44);
    expect(w.high).toBeLessThan(0.95);
  });
  test("precision counts only judged alerts and reports unclear and unjudged apart", () => {
    const v = new Map([[pairId(["root:a", "root:b"]), { pair: "", verdict: "overlap" as const }], [pairId(["root:c", "root:d"]), { pair: "", verdict: "unclear" as const }]]);
    const p = precisionOf([alert("2026-10-01T00:00:00Z", 1), alert("2026-10-01T00:00:00Z", 1, ["root:d", "root:c"]), alert("2026-10-01T00:00:00Z", 1, ["root:e", "root:f"])], v);
    expect([p.judged, p.overlap, p.unclear, p.unjudged, p.rate]).toEqual([1, 1, 1, 1, 1]);
  });
});

describe("weekly budget", () => {
  test("each week spends its own budget on its best alerts, so one busy week cannot take the window's", () => {
    const wk = (ts: string) => Math.floor(Date.parse(ts) / (7 * 86_400_000));
    const busy = Array.from({ length: 10 }, (_, i) => alert(`2026-09-0${(i % 3) + 7}T00:00:00Z`, i, [`root:x${i}`, "root:y"]));
    const quiet = [alert("2026-10-15T00:00:00Z", 0.1, ["root:q", "root:r"])];
    const kept = budgetCut([...busy, ...quiet], new Map([[wk("2026-09-07T00:00:00Z"), 40], [wk("2026-10-15T00:00:00Z"), 20]]), 5);
    expect(kept.map((a) => a.score).sort((a, b) => b - a)).toEqual([9, 8, 0.1]);
  });
});

describe("combined ranker", () => {
  const ctx: Context = { sessions: new Map(), prs: [], communicated: () => false };
  const c = (over: Partial<Components>): Components => ({ openPr: [], trigger: [], concurrent: [], ...over });
  const p = { wOpenPr: 1, wTrigger: 1, wRarity: 1, rarityCap: 3, wAgree: 1, wCrossOwner: 0, threshold: 2 };
  test("fires when corroborating evidence first reaches the threshold, never earlier", () => {
    const a = rank(ctx, c({ concurrent: [alert("2026-10-01T10:00:00Z", 0.5)], trigger: [alert("2026-10-01T12:00:00Z", 1)] }), p);
    // 0.5 rarity alone is below 2; the trigger at 12:00 adds 1 and the agreement bonus 1.
    expect(a.map((x) => [x.at, x.score, x.artifact])).toEqual([["2026-10-01T12:00:00Z", 2.5, "concurrent+trigger"]]);
  });
  test("one weak signal alone stays below the threshold", () => {
    expect(rank(ctx, c({ trigger: [alert("2026-10-01T12:00:00Z", 1)] }), p)).toEqual([]);
  });
});

describe("adjudication sample", () => {
  test("merges pairs across detectors, skips judged pairs and hides which detector raised them", () => {
    const rows: DirectoryRow[] = [{ sessionId: "s", source: "codex", lastTs: "2026-10-01T00:00:00Z", userIds: ["K"] }];
    const f: SessionFacts[] = [{ source: "codex", sessionId: "s", eventsRead: 0, censored: false, error: null, events: [] as EventFacts[] }];
    const sessions = buildModel(rows, f);
    const ctx: Context = { sessions, prs: [], communicated: communicated(sessions) };
    const byDetector = new Map([["d1", [alert("2026-10-01T00:00:00Z", 2), alert("2026-10-01T00:00:00Z", 1, ["root:c", "root:d"])]], ["d2", [alert("2026-10-01T01:00:00Z", 5, ["root:b", "root:a"])]]]);
    const s = adjudicationSample(ctx, byDetector, 5, "seed", new Set([pairId(["root:c", "root:d"])]));
    expect(s.map((x) => x.pair)).toEqual(["root:a | root:b"]);
    expect(Object.keys(s[0]!)).toEqual(["pair", "sides", "at"]);
  });
});

describe("stage 2", () => {
  test("scores kept positives and fresh alerts at the cutoff, with unjudged items counted apart", async () => {
    const { scoreStage2 } = await import("../overlap-bench/stage2.ts");
    const key = [{ pair: "a", truth: "incident:1" as const }, { pair: "b", truth: "hindsight-overlap" as const }, { pair: "c", truth: "fresh" as const }, { pair: "d", truth: "fresh" as const }, { pair: "e", truth: "fresh" as const }];
    const s = scoreStage2(key, [
      { pair: "a", verdict: "overlap", p_overlap: 0.6 }, { pair: "b", verdict: "not_overlap", p_overlap: 0.2 },
      { pair: "c", verdict: "not_overlap", p_overlap: 0.25 }, { pair: "d", verdict: "overlap", p_overlap: 0.3 },
    ], 0.3);
    expect([s.positives, s.positivesKept, s.fresh, s.freshKept, s.unjudged]).toEqual([2, 1, 2, 1, 1]);
    expect(s.freshKeptHigh).toBeGreaterThan(0.5);
  });
});
