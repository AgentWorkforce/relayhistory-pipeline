/** advisoryPriority: every row of the spec's priority table ("Priority and the human notice"), table-driven. */
import { describe, expect, test } from "bun:test";
import { AdvisoryNotifyRowsError, advisoryNotifyEligible, advisoryPriority, parseAdvisoryNotifyRows } from "../advisory-priority.ts";

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

describe("READ_API_ADVISORY_NOTIFY_ROWS (parseAdvisoryNotifyRows, advisoryNotifyEligible)", () => {
  const ALL = ["duplicate_work", "superseded_work", "concurrent_edit", "nearby_work", "similar_summary", "constraint_violation", "intent_conflict", "overlap"]
    .flatMap((kind) => ["high", "low"].map((urgency) => ({ kind, urgency })));

  test("default empty (unset, empty or whitespace): a Set, and nothing is eligible", () => {
    for (const raw of [undefined, "", "   ", "\t"]) {
      const rows = parseAdvisoryNotifyRows(raw);
      expect(rows instanceof Set).toBe(true);
      expect(rows.size).toBe(0);
      expect(ALL.filter((a) => advisoryNotifyEligible(a, rows))).toEqual([]);
    }
  });

  test("one row allowed: exactly that (kind, urgency) is eligible", () => {
    const rows = parseAdvisoryNotifyRows("duplicate_work:high");
    expect([...rows]).toEqual(["duplicate_work:high"]);
    expect(ALL.filter((a) => advisoryNotifyEligible(a, rows))).toEqual([{ kind: "duplicate_work", urgency: "high" }]);
    // Anything but a Set fails closed (an array index from `.map(advisoryItem)`, a plain object).
    expect(advisoryNotifyEligible({ kind: "duplicate_work", urgency: "high" }, 0 as never)).toBe(false);
    expect(advisoryNotifyEligible({ kind: "duplicate_work", urgency: "high" }, { has: () => true } as never)).toBe(false);
    // Prototype names are not keys of a Set.
    expect(advisoryNotifyEligible({ kind: "toString", urgency: "high" }, rows)).toBe(false);
    expect(advisoryNotifyEligible({ kind: "constructor", urgency: "" }, rows)).toBe(false);
  });

  test("whitespace around entries and around the colon is ignored; duplicates collapse", () => {
    expect([...parseAdvisoryNotifyRows(" duplicate_work : high ,superseded_work:high\t, duplicate_work:high ")]).toEqual(["duplicate_work:high", "superseded_work:high"]);
  });

  test("case is exact: uppercase kinds or urgencies are refused, not folded", () => {
    for (const raw of ["Duplicate_Work:high", "duplicate_work:HIGH", "DUPLICATE_WORK:HIGH"])
      expect(() => parseAdvisoryNotifyRows(raw)).toThrow(AdvisoryNotifyRowsError);
  });

  test("malformed lists are refused with the entry position and at most the one bad token", () => {
    const cases: Array<[string, RegExp]> = [
      ["duplicate_work", /entry 1 is not <kind>:<urgency>/],
      ["duplicate_work:high,", /entry 2 is not <kind>:<urgency>/],
      [",duplicate_work:high", /entry 1 is not/],
      ["duplicate_work:high:extra", /entry 1 is not/],
      [":high", /entry 1 is not/],
      ["duplicate_work:", /entry 1 is not/],
      ["duplicate_work:high;superseded_work:high", /entry 1 is not/],
      ["made_up:high", /entry 1 has an unknown kind "made_up"/],
      ["planned_fleet:low", /unknown kind "planned_fleet"/],
      ["duplicate_work:high,duplicate_work:urgent", /entry 2 has an unknown urgency "urgent"/],
    ];
    for (const [raw, message] of cases) {
      let error: unknown;
      try { parseAdvisoryNotifyRows(raw); } catch (e) { error = e; }
      expect([raw, error instanceof AdvisoryNotifyRowsError]).toEqual([raw, true]);
      expect((error as Error).message).toMatch(message);
      expect((error as Error).message.startsWith("READ_API_ADVISORY_NOTIFY_ROWS: ")).toBe(true);
    }
    // The other entries are never echoed, and a long bad token is cut.
    let msg = "";
    try { parseAdvisoryNotifyRows(`duplicate_work:high,secret_one:high_${"x".repeat(500)}`); } catch (e) { msg = (e as Error).message; }
    expect(msg).toMatch(/entry 2 has an unknown kind "secret_one"/);
    try { parseAdvisoryNotifyRows(`duplicate_work:high,${"y".repeat(500)}:high`); } catch (e) { msg = (e as Error).message; }
    expect(msg).not.toContain("duplicate_work");
    expect(msg.length).toBeLessThan(200);
  });
});
