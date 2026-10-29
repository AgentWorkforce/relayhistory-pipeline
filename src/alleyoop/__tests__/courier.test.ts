import { describe, expect, test } from "bun:test";
import { manifestReconciliation, sessionsForStartMode } from "../courier.ts";

describe("courier enrollment", () => {
  const rows = [
    { id: "before", first_activity_ms: 100 },
    { id: "after", first_activity_ms: 300 },
    { id: "unknown", first_activity_ms: null },
  ];

  test("new mode keeps sessions that started after enrollment", () => {
    expect(sessionsForStartMode(rows, "new", new Date(200).toISOString()).map((row) => row.id)).toEqual(["after"]);
  });

  test("switching to explicit all mode ignores a persisted enrollment cutoff", () => {
    expect(sessionsForStartMode(rows, "all", new Date(200).toISOString())).toEqual(rows);
  });
});

describe("courier manifest reconciliation", () => {
  test("only a definitive hit or miss can advance the session", () => {
    expect(manifestReconciliation(204)).toBe("verified");
    expect(manifestReconciliation(404)).toBe("reset");
    expect(manifestReconciliation(401)).toBe("defer");
    expect(manifestReconciliation(503)).toBe("defer");
    expect(manifestReconciliation()).toBe("defer");
  });
});
