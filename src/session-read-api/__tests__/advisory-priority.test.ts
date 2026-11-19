/** advisoryPriority: every row of the spec's priority table ("Priority and the human notice"), table-driven. */
import { describe, expect, test } from "bun:test";
import { advisoryPriority } from "../advisory-priority.ts";

const TABLE: Array<[urgency: string, kind: string, priority: 1 | 2 | 3]> = [
  // P1: the HIGH conflicts and the HIGH T1/T2 collisions.
  ["high", "constraint_violation", 1],
  ["high", "intent_conflict", 1],
  ["high", "duplicate_work", 1],
  ["high", "superseded_work", 1],
  // P2: the HIGH 60-minute T3, and any other HIGH kind (never P1 by default).
  ["high", "concurrent_edit", 2],
  ["high", "nearby_work", 2],
  ["high", "similar_summary", 2],
  ["high", "overlap", 2],
  ["high", "collision", 2],
  ["high", "repeated_investigation", 2],
  ["high", "stale_base", 2],
  ["high", "made_up_kind", 2],
  // P3: a fleet row at any urgency.
  ["high", "planned_fleet", 3],
  // P3: every LOW, whatever the kind.
  ["low", "constraint_violation", 3],
  ["low", "intent_conflict", 3],
  ["low", "duplicate_work", 3],
  ["low", "superseded_work", 3],
  ["low", "concurrent_edit", 3],
  ["low", "nearby_work", 3],
  ["low", "similar_summary", 3],
  ["low", "overlap", 3],
  ["low", "planned_fleet", 3],
  ["low", "made_up_kind", 3],
  // An urgency that is not exactly "high" is not HIGH.
  ["HIGH", "duplicate_work", 3],
  ["", "duplicate_work", 3],
];

describe("advisoryPriority", () => {
  for (const [urgency, kind, priority] of TABLE)
    test(`${urgency || "(empty)"} ${kind} is P${priority}`, () => {
      expect(advisoryPriority({ urgency, kind })).toBe(priority);
    });

  test("only the four HIGH P1 kinds are P1", () => {
    expect(TABLE.filter(([, , p]) => p === 1).map(([u, k]) => `${u}:${k}`)).toEqual([
      "high:constraint_violation", "high:intent_conflict", "high:duplicate_work", "high:superseded_work",
    ]);
  });
});
