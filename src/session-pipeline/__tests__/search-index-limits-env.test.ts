import { describe, expect, test } from "bun:test";
import { REPLAY_SEARCH_INDEX_LIMITS, SEARCH_INDEX_DEFAULT_LIMITS, searchIndexLimitsFromEnv } from "../search-index.ts";

const MiB = 1024 * 1024;
const NAMES = ["SEARCH_INDEX_MAX_SCOPE_BYTES", "SEARCH_INDEX_MAX_SCOPE_EVENTS", "SEARCH_INDEX_MAX_ROWS"] as const;

/** The one fixed refusal per variable: it names the variable and its range, so it can never carry the value. */
const REFUSAL: Record<(typeof NAMES)[number], string> = {
  SEARCH_INDEX_MAX_SCOPE_BYTES: "pipeline consumer: SEARCH_INDEX_MAX_SCOPE_BYTES must be a whole number of bytes (optional MiB or GiB suffix) in 64MiB..256MiB",
  SEARCH_INDEX_MAX_SCOPE_EVENTS: "pipeline consumer: SEARCH_INDEX_MAX_SCOPE_EVENTS must be a whole number in 100000..1000000",
  SEARCH_INDEX_MAX_ROWS: "pipeline consumer: SEARCH_INDEX_MAX_ROWS must be a whole number in 500000..1000000",
};

/** Asserts `name`=`value` refuses with exactly the fixed message for `name`. */
function refusal(name: (typeof NAMES)[number], value: string): string {
  let message = "";
  try { searchIndexLimitsFromEnv({ [name]: value }); } catch (error) { message = (error as Error).message; }
  expect(message).toBe(REFUSAL[name]);
  return message;
}

describe("searchIndexLimitsFromEnv", () => {
  test("unset and empty keep the defaults", () => {
    expect(searchIndexLimitsFromEnv({})).toEqual({});
    expect(searchIndexLimitsFromEnv({ SEARCH_INDEX_MAX_SCOPE_BYTES: "", SEARCH_INDEX_MAX_SCOPE_EVENTS: "", SEARCH_INDEX_MAX_ROWS: "" })).toEqual({});
    // Unrelated variables are not read.
    expect(searchIndexLimitsFromEnv({ SEARCH_INDEX_MAX_SCOPE: "1", ANALYTICS_SINK: "x" })).toEqual({});
  });

  test("each limit is read on its own and overlays the defaults", () => {
    const limits = searchIndexLimitsFromEnv({ SEARCH_INDEX_MAX_SCOPE_BYTES: String(128 * MiB) });
    expect(limits).toEqual({ maxScopeBytes: 128 * MiB });
    expect({ ...SEARCH_INDEX_DEFAULT_LIMITS, ...limits }.maxScopeEvents).toBe(SEARCH_INDEX_DEFAULT_LIMITS.maxScopeEvents);
    expect(searchIndexLimitsFromEnv({
      SEARCH_INDEX_MAX_SCOPE_BYTES: String(REPLAY_SEARCH_INDEX_LIMITS.maxScopeBytes),
      SEARCH_INDEX_MAX_SCOPE_EVENTS: "1000000",
      SEARCH_INDEX_MAX_ROWS: "1000000",
    })).toEqual({ maxScopeBytes: REPLAY_SEARCH_INDEX_LIMITS.maxScopeBytes, maxScopeEvents: 1_000_000, maxRows: 1_000_000 });
  });

  test("bytes take an optional MiB or GiB suffix, like READ_API_CANONICAL_SCOPE_BYTES", () => {
    expect(searchIndexLimitsFromEnv({ SEARCH_INDEX_MAX_SCOPE_BYTES: "128MiB" })).toEqual({ maxScopeBytes: 128 * MiB });
    expect(searchIndexLimitsFromEnv({ SEARCH_INDEX_MAX_SCOPE_BYTES: "256MiB" })).toEqual({ maxScopeBytes: 256 * MiB });
    expect(searchIndexLimitsFromEnv({ SEARCH_INDEX_MAX_SCOPE_BYTES: "64MiB" })).toEqual({ maxScopeBytes: SEARCH_INDEX_DEFAULT_LIMITS.maxScopeBytes });
  });

  test("the bounds are exactly the defaults and the replay limits", () => {
    expect(SEARCH_INDEX_DEFAULT_LIMITS).toMatchObject({ maxScopeBytes: 64 * MiB, maxScopeEvents: 100_000, maxRows: 500_000 });
    expect(REPLAY_SEARCH_INDEX_LIMITS).toMatchObject({ maxScopeBytes: 256 * MiB, maxScopeEvents: 1_000_000, maxRows: 1_000_000 });
    expect(searchIndexLimitsFromEnv({ SEARCH_INDEX_MAX_SCOPE_EVENTS: "100000", SEARCH_INDEX_MAX_ROWS: "500000" })).toEqual({ maxScopeEvents: 100_000, maxRows: 500_000 });
  });

  test("a value that is not a whole number refuses startup, naming the variable and never the value", () => {
    for (const bad of ["0", "-1", "1e9", " 5", "5 ", "5.5", "0x10", "64MB", "64mib", "abc", "+100000000", "0100000000", "99999999999999"])
      refusal("SEARCH_INDEX_MAX_SCOPE_BYTES", bad);
    for (const name of ["SEARCH_INDEX_MAX_SCOPE_EVENTS", "SEARCH_INDEX_MAX_ROWS"] as const)
      for (const bad of ["0", "-1", "1e6", "5.5", "abc", "1MiB", "500000MiB", " 600000"]) refusal(name, bad);
    expect(refusal("SEARCH_INDEX_MAX_ROWS", "s3cret-value")).not.toContain("s3cret");
  });

  test("a value below the default refuses (never clamped)", () => {
    refusal("SEARCH_INDEX_MAX_SCOPE_BYTES", String(64 * MiB - 1));
    refusal("SEARCH_INDEX_MAX_SCOPE_BYTES", "63MiB");
    refusal("SEARCH_INDEX_MAX_SCOPE_EVENTS", "99999");
    refusal("SEARCH_INDEX_MAX_ROWS", "499999");
  });

  test("a value above the replay ceiling refuses (never clamped)", () => {
    refusal("SEARCH_INDEX_MAX_SCOPE_BYTES", String(256 * MiB + 1));
    refusal("SEARCH_INDEX_MAX_SCOPE_BYTES", "257MiB");
    refusal("SEARCH_INDEX_MAX_SCOPE_BYTES", "1GiB");
    refusal("SEARCH_INDEX_MAX_SCOPE_EVENTS", "1000001");
    refusal("SEARCH_INDEX_MAX_ROWS", "1000001");
  });

  test("one bad variable refuses even when the others are valid", () => {
    for (const name of NAMES)
      expect(() => searchIndexLimitsFromEnv({ SEARCH_INDEX_MAX_SCOPE_BYTES: "128MiB", SEARCH_INDEX_MAX_SCOPE_EVENTS: "200000", SEARCH_INDEX_MAX_ROWS: "600000", [name]: "nope" })).toThrow(REFUSAL[name]);
  });
});
