import { describe, expect, test } from "bun:test";
import { markdown, merge, parseLines, thresholds, type Record_ } from "../report.ts";

const lat = (p50: number, p99: number, n = 100) => ({ n, min_ms: 1, p50_ms: p50, p90_ms: p99, p99_ms: p99, max_ms: p99 * 2, mean_ms: p50 });
const cell = (run: string, step: number, name: string, target: string, params: Record<string, string | number>, result: Record<string, unknown>): Record_ => ({ ledger_bench: 1, run, lane: "x", step, cell: name, target, params, result });
const p13 = (run: string, target: string, p50: number, p99: number, ops: number) => cell(run, 3, "p13", target, { streams: 1, in_flight: 8 }, { latency: lat(p50, p99), ops_per_sec: ops, batches_per_op: 1, errors: {}, outcomes: { ok: 100 } });
const verdicts = (records: Record_[]) => Object.fromEntries(thresholds(merge(records)).map((t) => [`${t.option}: ${t.check}`, t.verdict]));

describe("report", () => {
  test("merges repeats by the median of each repeat's percentile, and sums errors", () => {
    const [m] = merge([p13("a", "R2", 100, 300, 10), p13("b", "R2", 80, 500, 12), { ...p13("c", "R2", 120, 400, 11), result: { latency: lat(120, 400), ops_per_sec: 11, batches_per_op: 1, errors: { http_503: 2 }, outcomes: {} } }]);
    expect(m).toMatchObject({ repeats: 3, p50_ms: 100, p99_ms: 400, ops_per_sec: 11, batches_per_sec: 11, samples: 300, errors: { http_503: 2 }, max_ms: 1000 });
  });

  test("ignores lines that are not result records", () => {
    expect(parseLines('noise\n{"other":1}\n{"ledger_bench":1,"run":"a","lane":"r2","event":"start"}\n').length).toBe(1);
  });

  test("A1 and C pass on a 13x in-region gain and a 20x log commit; missing inputs are inconclusive", () => {
    const records = [
      p13("a", "R1", 1300, 4000, 6),
      p13("a", "R2", 100, 400, 80),
      cell("a", 3, "plog", "R2", { streams: 1, group: 16, writers: 1 }, { latency: lat(10, 30), ops_per_sec: 100, batches_per_op: 16, commit_latency: lat(20, 60), errors: {}, outcomes: {} }),
      cell("a", 2, "race_create", "R2", { writers: 8 }, { violations: 0 }),
      cell("a", 3, "racing_writers", "R2", { writers: 2 }, { violations: 0 }),
      cell("a", 2, "ambiguous_commit", "R2", { kib: 256 }, { torn: 0, acknowledged_lost: 0, in_flight: 3 }),
    ];
    const v = verdicts(records);
    expect(v["A1: P13 p50 on R2 at most 1/8 of R1"]).toBe("pass");
    expect(v["A1: P13 p99 on R2 at most 1/4 of R1"]).toBe("pass");
    expect(v["A1: every step 2 cell passes on R2"]).toBe("pass");
    expect(v["C: PLOG G=16 on R2 at least 10x P13 on R2 in batches/s, 1 stream"]).toBe("pass");
    expect(v["C: racing-writer, readback and ambiguous-commit checks on R2"]).toBe("pass");
    expect(v["B: P13 batches/s on R3 at least 1.3x R2"]).toBe("inconclusive");
    expect(v["G: create-if-absent p99 on R4 at most 1/3 of R2"]).toBe("inconclusive");
    expect(v["E: 0 lost or duplicate commits"]).toBe("inconclusive");
  });

  test("a violation, a torn write or a small gain fails its threshold", () => {
    const records = [
      p13("a", "R1", 1300, 4000, 6),
      p13("a", "R2", 400, 2000, 20),
      cell("a", 3, "plog", "R2", { streams: 1, group: 16, writers: 1 }, { latency: lat(10, 30), ops_per_sec: 5, batches_per_op: 16, errors: {}, outcomes: {} }),
      cell("a", 2, "race_create", "R2", { writers: 8 }, { violations: 1 }),
      cell("a", 3, "racing_writers", "R2", { writers: 2 }, { violations: 0 }),
      cell("a", 2, "ambiguous_commit", "R2", { kib: 256 }, { torn: 1, acknowledged_lost: 0 }),
    ];
    const v = verdicts(records);
    expect(v["A1: P13 p50 on R2 at most 1/8 of R1"]).toBe("fail");
    expect(v["A1: every step 2 cell passes on R2"]).toBe("fail");
    expect(v["C: PLOG G=16 on R2 at least 10x P13 on R2 in batches/s, 1 stream"]).toBe("fail");
    expect(v["C: racing-writer, readback and ambiguous-commit checks on R2"]).toBe("fail");
  });

  test("the write-up lists lanes, thresholds and each step, and flags an incomplete lane", () => {
    const md = markdown([p13("a", "R2", 100, 400, 80), cell("a", 2, "if_match_support", "R4", {}, { support: "rejected", fresh: "http_501", stale: "http_501" }), { ledger_bench: 1, run: "a", lane: "r2", event: "end", complete: false, requests: 10, request_budget: 20 } as Record_]);
    expect(md).toContain("| a | r2 | NO | 10 | 20 |");
    expect(md).toContain("## Step 3");
    expect(md).toContain("R4: rejected");
    expect(md).toContain("| p13 | R2 |");
  });
});
