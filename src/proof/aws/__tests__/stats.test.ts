import { describe, expect, test } from "bun:test";
import { percentile, stageLatency, summarize } from "../stats.ts";

const range = (n: number) => Array.from({ length: n }, (_, i) => i + 1);

describe("percentile (nearest rank, same definition as #59)", () => {
  test("1..100 gives the rank itself", () => {
    const s = range(100);
    expect(percentile(s, 50)).toBe(50);
    expect(percentile(s, 95)).toBe(95);
    expect(percentile(s, 99)).toBe(99);
    expect(percentile(s, 100)).toBe(100);
  });
  test("small samples round the rank up, never interpolate", () => {
    expect(percentile([10, 20, 30], 50)).toBe(20);
    expect(percentile([10, 20, 30], 95)).toBe(30);
    expect(percentile([10, 20, 30, 40], 50)).toBe(20);
    expect(percentile([7], 99)).toBe(7);
  });
  test("empty is null, and p outside (0, 100] is a bug", () => {
    expect(percentile([], 50)).toBeNull();
    expect(() => percentile([1], 0)).toThrow();
    expect(() => percentile([1], 101)).toThrow();
  });
});

describe("summarize", () => {
  test("sorts, and reports n, errors and an unrounded error rate", () => {
    const s = summarize([30, 10, 20], 1);
    expect(s).toEqual({ n: 3, errors: 1, errorRate: 0.25, p50: 20, p95: 30, p99: 30, max: 30 });
    expect(summarize(range(999), 1).errorRate).toBe(0.001);
  });
  test("no attempts at all has no error rate", () => {
    expect(summarize([], 0)).toMatchObject({ n: 0, errors: 0, errorRate: null, p50: null });
  });
  test("all errors is an error rate of 1, not a latency", () => {
    expect(summarize([], 4)).toMatchObject({ n: 0, errorRate: 1, p95: null });
  });
  test("negative or non-finite samples are refused", () => {
    expect(() => summarize([-1])).toThrow();
    expect(() => summarize([Number.NaN])).toThrow();
  });
});

describe("stageLatency", () => {
  test("missing ends and clock-skewed pairs count as errors, never as 0 ms", () => {
    const items = [
      { a: 100, b: 150 },
      { a: 100, b: undefined },
      { a: 200, b: 150 },
      { a: 0, b: 10 },
    ];
    const s = stageLatency(items, (i) => i.a, (i) => i.b);
    expect(s.n).toBe(2);
    expect(s.errors).toBe(2);
    expect(s.p50).toBe(10);
    expect(s.max).toBe(50);
  });
});
