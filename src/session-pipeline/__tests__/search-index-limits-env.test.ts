import { describe, expect, test } from "bun:test";
import { REPLAY_SEARCH_INDEX_LIMITS, SEARCH_INDEX_DEFAULT_LIMITS, searchIndexLimitsFromEnv } from "../search-index.ts";

describe("searchIndexLimitsFromEnv", () => {
  test("unset and empty keep the defaults", () => {
    expect(searchIndexLimitsFromEnv({})).toEqual({});
    expect(searchIndexLimitsFromEnv({ SEARCH_INDEX_MAX_SCOPE_BYTES: "", SEARCH_INDEX_MAX_ROWS: "" })).toEqual({});
  });

  test("each limit is read on its own and overlays the defaults", () => {
    const limits = searchIndexLimitsFromEnv({ SEARCH_INDEX_MAX_SCOPE_BYTES: String(128 * 1024 * 1024) });
    expect(limits).toEqual({ maxScopeBytes: 128 * 1024 * 1024 });
    expect({ ...SEARCH_INDEX_DEFAULT_LIMITS, ...limits }.maxScopeEvents).toBe(SEARCH_INDEX_DEFAULT_LIMITS.maxScopeEvents);
    expect(searchIndexLimitsFromEnv({
      SEARCH_INDEX_MAX_SCOPE_BYTES: String(REPLAY_SEARCH_INDEX_LIMITS.maxScopeBytes),
      SEARCH_INDEX_MAX_SCOPE_EVENTS: "1000000",
      SEARCH_INDEX_MAX_ROWS: "1000000",
    })).toEqual({ maxScopeBytes: REPLAY_SEARCH_INDEX_LIMITS.maxScopeBytes, maxScopeEvents: 1_000_000, maxRows: 1_000_000 });
  });

  test("a value that is not a positive integer fails startup", () => {
    for (const bad of ["0", "-1", "64MiB", "1e9", " 5", "5.5", "0x10"])
      expect(() => searchIndexLimitsFromEnv({ SEARCH_INDEX_MAX_SCOPE_BYTES: bad })).toThrow("SEARCH_INDEX_MAX_SCOPE_BYTES must be a positive integer");
  });

  test("a value above the replay bound fails startup", () => {
    expect(() => searchIndexLimitsFromEnv({ SEARCH_INDEX_MAX_SCOPE_BYTES: String(REPLAY_SEARCH_INDEX_LIMITS.maxScopeBytes + 1) })).toThrow("at most");
    expect(() => searchIndexLimitsFromEnv({ SEARCH_INDEX_MAX_SCOPE_EVENTS: "1000001" })).toThrow("at most");
    expect(() => searchIndexLimitsFromEnv({ SEARCH_INDEX_MAX_ROWS: "1000001" })).toThrow("at most");
  });
});
