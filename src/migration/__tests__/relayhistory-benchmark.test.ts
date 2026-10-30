import { describe, expect, test } from "bun:test";
import { EVENT_TEXT_SEARCH_SQL, latencyStats } from "../relayhistory-benchmark.ts";

describe("relayhistory benchmark stats", () => {
  test("uses nearest-rank percentiles and rounds output", () => {
    expect(latencyStats([10, 2, 4, 8, 6])).toEqual({
      minMs: 2,
      p50Ms: 6,
      p95Ms: 10,
      p99Ms: 10,
      maxMs: 10,
      meanMs: 6,
    });
  });

  test("benchmarks the production bounded content and task-title search predicate", () => {
    expect(EVENT_TEXT_SEARCH_SQL).toContain("left(content, 4096) ILIKE");
    expect(EVENT_TEXT_SEARCH_SQL).toContain("task_title ILIKE");
    expect(EVENT_TEXT_SEARCH_SQL).not.toMatch(/\bcontent ILIKE/);
  });
});
