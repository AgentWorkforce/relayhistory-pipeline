import { describe, expect, test } from "bun:test";
import { claimlessWindowEnv } from "../claimless-window.ts";

describe("claimlessWindowEnv", () => {
  const from = "2026-11-01T00:00:00.000Z";

  test("a stage that names no window sets nothing", () => {
    expect(claimlessWindowEnv(undefined)).toEqual([]);
  });

  test("a window sets all three variables", () => {
    const until = "2026-11-04T00:00:00.000Z";
    expect(claimlessWindowEnv({ from, until })).toEqual([
      { name: "MANIFEST_MACHINE_CLAIM", value: "compat" },
      { name: "MANIFEST_MACHINE_CLAIM_COMPAT_FROM", value: from },
      { name: "MANIFEST_MACHINE_CLAIM_COMPAT_UNTIL", value: until },
    ]);
  });

  test("refuses over the cap, backwards and unreadable windows", () => {
    for (const until of [
      "2026-11-04T00:00:00.001Z",
      from,
      "2026-10-31T00:00:00.000Z",
      "soon",
    ])
      expect(() => claimlessWindowEnv({ from, until })).toThrow(/72 hours/);
  });
});
