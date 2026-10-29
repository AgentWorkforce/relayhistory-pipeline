import { describe, expect, test } from "bun:test";
import { initializeEnrollment, manifestReconciliation, sessionsForStartMode } from "../courier.ts";

describe("courier enrollment", () => {
  const rows = [
    { id: "before", first_activity_ms: 100, last_activity_ms: 150 },
    { id: "after", first_activity_ms: 300, last_activity_ms: 350 },
    { id: "last-only", first_activity_ms: null, last_activity_ms: 400 },
    { id: "unknown", first_activity_ms: null, last_activity_ms: null },
  ];

  test("new mode keeps sessions that started after enrollment", () => {
    expect(sessionsForStartMode(rows, "new", new Date(200).toISOString()).map((row) => row.id)).toEqual(["after", "last-only"]);
  });

  test("switching to explicit all mode ignores a persisted enrollment cutoff", () => {
    expect(sessionsForStartMode(rows, "all", new Date(200).toISOString())).toEqual(rows);
  });

  test("new mode adds a cutoff to an existing partial ledger before selection", () => {
    const state: { enrolled_at?: string } = {};
    expect(initializeEnrollment(state, "new", new Date(200))).toBe(true);
    expect(state.enrolled_at).toBe(new Date(200).toISOString());
    expect(sessionsForStartMode(rows, "new", state.enrolled_at).map((row) => row.id)).toEqual(["after", "last-only"]);
    expect(initializeEnrollment(state, "new", new Date(400))).toBe(false);
    expect(state.enrolled_at).toBe(new Date(200).toISOString());
  });
});

describe("courier manifest reconciliation", () => {
  test("only a definitive hit or miss can advance the session", () => {
    expect(manifestReconciliation(200)).toBe("verified");
    expect(manifestReconciliation(404)).toBe("reset");
    expect(manifestReconciliation(401)).toBe("defer");
    expect(manifestReconciliation(503)).toBe("defer");
    expect(manifestReconciliation()).toBe("defer");
  });
});
