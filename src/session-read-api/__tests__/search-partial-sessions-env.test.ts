import { describe, expect, test } from "bun:test";
import { READ_API_SEARCH_PARTIAL_SESSIONS_VAR, searchSkipsPartialSessions } from "../lambda.ts";

describe("searchSkipsPartialSessions", () => {
  const env = (value?: string) => (value === undefined ? {} : { [READ_API_SEARCH_PARTIAL_SESSIONS_VAR]: value }) as NodeJS.ProcessEnv;

  test("unset, empty and refuse keep the refusal", () => {
    for (const value of [undefined, "", "refuse"]) expect(searchSkipsPartialSessions(env(value))).toBe(false);
  });

  test("skip turns it on", () => {
    expect(searchSkipsPartialSessions(env("skip"))).toBe(true);
  });

  test("any other value refuses startup and never prints the value", () => {
    for (const value of ["true", "1", "Skip", "skip "]) {
      expect(() => searchSkipsPartialSessions(env(value))).toThrow(`${READ_API_SEARCH_PARTIAL_SESSIONS_VAR} must be unset`);
    }
    try { searchSkipsPartialSessions(env("s3cret")); } catch (error) { expect(String(error)).not.toContain("s3cret"); }
  });
});
