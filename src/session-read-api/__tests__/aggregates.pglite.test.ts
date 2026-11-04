/**
 * Pure window helpers for the aggregate lane — RPC-path behavior lives in
 * aggregate-parity.pglite.test.ts (worker-scope bind, readiness, oracle parity).
 */
import { describe, expect, test } from "bun:test";
import { activityDayCount, activityDates } from "../session-activity.ts";

describe("aggregate window helpers", () => {
  test("window helpers match the contract caps", () => {
    expect(activityDayCount(new Date("2026-01-01Z"), new Date("2026-01-01Z"))).toBe(1);
    expect(activityDayCount(new Date("2026-01-02Z"), new Date("2026-01-01Z"))).toBe(0);
    expect(activityDates(new Date("2026-01-30Z"), new Date("2026-02-02Z")).length).toBe(4);
  });
});
