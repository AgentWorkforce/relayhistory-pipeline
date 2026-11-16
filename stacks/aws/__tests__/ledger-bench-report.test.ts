import { describe, expect, test } from "bun:test";
import { markdown, merge, parseLines, thresholds, type Record_ } from "../../../benchmarks/ledger-bench/report.ts";

const lat = (p50: number, p99: number, n = 100) => ({ n, min_ms: 1, p50_ms: p50, p90_ms: p99, p99_ms: p99, max_ms: p99 * 2, mean_ms: p50 });
const cell = (run: string, step: number, name: string, target: string, params: Record<string, string | number>, result: Record<string, unknown>): Record_ => ({ ledger_bench: 1, run, lane: "x", step, cell: name, target, params, result });
const p13 = (run: string, target: string, p50: number, p99: number, ops: number) => cell(run, 3, "p13", target, { streams: 1, in_flight: 8 }, { latency: lat(p50, p99), ops_per_sec: ops, batches_per_op: 1, errors: {}, outcomes: { ok: 100 } });
const step2Ok = (target: string) => [
  cell("a", 2, "race_create", target, { writers: 8 }, { violations: 0 }),
  cell("a", 2, "if_match_support", target, {}, { support: "supported" }),
  cell("a", 2, "race_cas", target, { writers: 8 }, { violations: 0 }),
  cell("a", 2, "race_delete", target, {}, { violations: 0 }),
  cell("a", 2, "ambiguous_commit", target, { kib: 256 }, { torn: 0, acknowledged_lost: 0, in_flight: 3 }),
];
const start = (run: string, lane: string, profile = "standard"): Record_ => ({ ledger_bench: 1, run, lane, event: "start", profile_name: profile });
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
      ...step2Ok("R2"),
      cell("a", 3, "racing_writers", "R2", { writers: 2 }, { violations: 0 }),
      cell("a", 3, "plog_readback", "R2", { streams: 1, group: 16 }, { violations: 0 }),
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
      cell("a", 2, "ambiguous_commit", "R2", { kib: 256 }, { torn: 1, acknowledged_lost: 0, in_flight: 2 }),
    ];
    const v = verdicts(records);
    expect(v["A1: P13 p50 on R2 at most 1/8 of R1"]).toBe("fail");
    expect(v["A1: every step 2 cell passes on R2"]).toBe("fail");
    expect(v["C: PLOG G=16 on R2 at least 10x P13 on R2 in batches/s, 1 stream"]).toBe("fail");
    expect(v["C: racing-writer, readback and ambiguous-commit checks on R2"]).toBe("fail");
  });

  test("a correctness check never passes on partial or empty evidence", () => {
    const C = "C: racing-writer, readback and ambiguous-commit checks on R2";
    const A1 = "A1: every step 2 cell passes on R2";
    const full = [...step2Ok("R2"), cell("a", 3, "racing_writers", "R2", { writers: 2 }, { violations: 0 }), cell("a", 3, "plog_readback", "R2", { streams: 1, group: 16 }, { violations: 0 })];
    expect(verdicts(full)[C]).toBe("pass");
    // The racing-writers stage failed or was skipped: the readback alone is not a pass.
    expect(verdicts(full.filter((r) => r.cell !== "racing_writers"))[C]).toBe("inconclusive");
    // Every ambiguous-commit child crashed: nothing was caught in flight.
    expect(verdicts(full.map((r) => (r.cell === "ambiguous_commit" ? { ...r, result: { torn: 0, acknowledged_lost: 0, in_flight: 0 } } : r)))[C]).toBe("inconclusive");
    // If-Match ignored on a general-purpose bucket: the CAS cells never ran, and that is a failure.
    const ignored = full.filter((r) => r.cell !== "race_cas" && r.cell !== "race_delete").map((r) => (r.cell === "if_match_support" ? { ...r, result: { support: "ignored" } } : r));
    expect(verdicts(ignored)[A1]).toBe("fail");
    // A torn write fails A1 too.
    expect(verdicts(full.map((r) => (r.cell === "ambiguous_commit" ? { ...r, result: { torn: 1, acknowledged_lost: 0, in_flight: 2 } } : r)))[A1]).toBe("fail");
    expect(verdicts(full.filter((r) => r.cell !== "race_delete"))[A1]).toBe("inconclusive");
  });

  test("a cell with no steady-state sample has no rate, so a threshold on it is inconclusive", () => {
    const hot = cell("a", 5, "hot_head", "R6", { in_flight: 512 }, { latency: { n: 0, p50_ms: null, p99_ms: null }, ops_per_sec: 0, errors: {}, outcomes: {} });
    expect(merge([hot])[0]!.ops_per_sec).toBeNull();
    expect(verdicts([hot])["E: one hot head sustains at least 2x the observed peak per-stream commit rate"]).toBe("inconclusive");
  });

  test("a smoke run is not a repeat, and a failure record's text is printed as given", () => {
    const md = markdown([start("s", "r2", "smoke"), p13("s", "R2", 1, 2, 999), start("a", "r2"), p13("a", "R2", 100, 400, 80), { ledger_bench: 1, run: "a", lane: "r2", event: "stage_failed", stage: "step1 R2", error: "http_403" } as Record_]);
    expect(md).toContain("Repeats (run ids): a.");
    expect(md).toContain("| p13 | R2 | streams: 1, in_flight: 8 | 1 | 100 |");
    expect(md).toContain("stage_failed step1 R2: http_403");
    expect(md).not.toContain("999");
  });

  test("the write-up lists lanes, thresholds and each step, and flags an incomplete lane", () => {
    const md = markdown([start("a", "x"), start("a", "r2"), p13("a", "R2", 100, 400, 80), cell("a", 2, "if_match_support", "R4", {}, { support: "rejected", fresh: "http_501", stale: "http_501" }), { ledger_bench: 1, run: "a", lane: "r2", event: "end", complete: false, requests: 10, request_budget: 20, cpu_stat: { nr_throttled: 7 }, max_rss_kib: 5 } as Record_]);
    expect(md).toContain("| a | r2 | NO | 10 | 20 | 7 | 5 |");
    expect(md).toContain("CPU-throttled in 1 lane run");
    expect(md).toContain("## Step 3");
    expect(md).toContain("R4: rejected");
    expect(md).toContain("| p13 | R2 |");
  });
});
