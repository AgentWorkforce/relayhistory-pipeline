import { describe, expect, test } from "bun:test";
import { healthVerdict } from "../worker.ts";

const opts = { maxFailures: 3, maxTickAgeMs: 60_000 };
describe("worker health", () => {
  test("healthy while ticking and the database answers", () => {
    expect(healthVerdict({ ready: true, lastTickAt: 1_000, consecutiveDbFailures: 0 }, 2_000, opts)).toEqual({ ok: true, reason: "ok" });
  });
  test("unhealthy after N consecutive database failures (rotated password)", () => {
    expect(healthVerdict({ ready: true, lastTickAt: 1_000, consecutiveDbFailures: 3 }, 2_000, opts).reason).toBe("hoopdb_unreachable");
  });
  test("unhealthy when the poll loop stalls", () => {
    expect(healthVerdict({ ready: true, lastTickAt: 0, consecutiveDbFailures: 0 }, 61_000, opts).reason).toBe("loop_stalled");
  });
});
