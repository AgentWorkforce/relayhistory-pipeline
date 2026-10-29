import { describe, expect, test } from "bun:test";
import { sessionsForStartMode } from "../courier.ts";

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
