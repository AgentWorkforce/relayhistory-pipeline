/** READ_API_CANONICAL_SCOPE_BYTES / _EVENTS (service-canonical.ts): self-host canonical snapshot budget. */
import { describe, expect, test } from "bun:test";
import { CANONICAL_SCOPE_BYTES, CANONICAL_SCOPE_EVENTS } from "../canonical.ts";
import { canonicalScopeBudget, serviceCanonicalDeps } from "../service-canonical.ts";

const MiB = 1024 * 1024, GiB = 1024 * MiB;
const bytes = (v: string | undefined) => canonicalScopeBudget(v === undefined ? {} : { READ_API_CANONICAL_SCOPE_BYTES: v });
const events = (v: string | undefined) => canonicalScopeBudget(v === undefined ? {} : { READ_API_CANONICAL_SCOPE_EVENTS: v });

describe("canonicalScopeBudget", () => {
  test("unset or empty: no override, so canonicalSnapshot keeps its defaults (16 MiB, 100000)", () => {
    expect(CANONICAL_SCOPE_BYTES).toBe(16 * MiB);
    expect(CANONICAL_SCOPE_EVENTS).toBe(100000);
    expect(canonicalScopeBudget({})).toEqual({});
    expect(canonicalScopeBudget({ READ_API_CANONICAL_SCOPE_BYTES: "", READ_API_CANONICAL_SCOPE_EVENTS: "" })).toEqual({});
    expect(Object.keys(canonicalScopeBudget({}))).toEqual([]);
  });

  test.each([
    ["16777216", 16 * MiB],
    ["16MiB", 16 * MiB],
    ["64MiB", 64 * MiB],
    ["67108864", 64 * MiB],
    ["1GiB", GiB],
    ["2GiB", 2 * GiB],
    ["2048MiB", 2 * GiB],
    [String(2 * GiB), 2 * GiB],
  ])("bytes %p is accepted as %p", (raw, want) => {
    expect(bytes(raw)).toEqual({ scopeBytes: want });
  });

  test.each([["100000", 100000], ["250000", 250000], ["4000000", 4_000_000]])("events %p is accepted", (raw, want) => {
    expect(events(raw)).toEqual({ scopeEvents: want });
  });

  test("both set together", () => {
    expect(canonicalScopeBudget({ READ_API_CANONICAL_SCOPE_BYTES: "64MiB", READ_API_CANONICAL_SCOPE_EVENTS: "200000" })).toEqual({ scopeBytes: 64 * MiB, scopeEvents: 200000 });
  });

  test.each(["16777215", "15MiB", "1", "1MiB", "0GiB"])("bytes below the default %p fails, naming the variable", (raw) => {
    expect(() => bytes(raw)).toThrow("READ_API_CANONICAL_SCOPE_BYTES must be a whole number of bytes (optional MiB or GiB suffix) in 16MiB..2GiB");
  });
  test.each([String(2 * GiB + 1), "3GiB", "2049MiB", "9999999999999"])("bytes above 2 GiB %p fails", (raw) => {
    expect(() => bytes(raw)).toThrow("READ_API_CANONICAL_SCOPE_BYTES");
  });
  test.each(["99999", "1"])("events below the default %p fails", (raw) => {
    expect(() => events(raw)).toThrow("READ_API_CANONICAL_SCOPE_EVENTS must be a whole number of records in 100000..4000000");
  });
  test.each(["4000001", "99999999"])("events above 4000000 %p fails", (raw) => {
    expect(() => events(raw)).toThrow("READ_API_CANONICAL_SCOPE_EVENTS");
  });

  test.each([
    "0", "-16777216", "+16777216", "016777216", " 16777216", "16777216 ", "16777216.0", "1.6e7", "0x1000000",
    "16 MiB", "16mib", "16MB", "16M", "16KiB", "1TiB", "GiB", "MiB", "abc", "Infinity", "NaN", " ", "1_000_000_000",
    "99999999999999999999",
  ])("bytes malformed %j fails startup without echoing the value", (raw) => {
    let message = "";
    try { bytes(raw); } catch (e) { message = (e as Error).message; }
    // The message is fixed: the variable and its range, never the value.
    expect(message).toBe("session read api: READ_API_CANONICAL_SCOPE_BYTES must be a whole number of bytes (optional MiB or GiB suffix) in 16MiB..2GiB");
  });

  test.each(["0", "-100000", "100000.5", "1e6", "200000 ", " 200000", "200000MiB", "1GiB", "100_000", "abc", " "])(
    "events malformed %j fails startup (no suffix for records)", (raw) => {
      expect(() => events(raw)).toThrow("READ_API_CANONICAL_SCOPE_EVENTS");
    },
  );
});

describe("serviceCanonicalDeps reads the budget at startup", () => {
  const env = {
    LOCKER_BUCKET: "bucket",
    LOCKER_ENDPOINT: "http://127.0.0.1:9",
    LOCKER_REGION: "us-east-1",
    LOCKER_ACCESS_KEY_ID: "synthetic-access-key",
    LOCKER_SECRET_ACCESS_KEY: "synthetic-secret-key",
    PIPELINE_KEY_PREFIX: "96bulls",
    PIPELINE_ORG_FROM_PREFIX: "true",
  };
  const run = (async () => { throw new Error("no query at startup"); }) as never;
  test("valid and unset values start", async () => {
    expect(await serviceCanonicalDeps(env, run)).toBeDefined();
    expect(await serviceCanonicalDeps({ ...env, READ_API_CANONICAL_SCOPE_BYTES: "64MiB", READ_API_CANONICAL_SCOPE_EVENTS: "200000" }, run)).toBeDefined();
  });
  test.each([
    [{ READ_API_CANONICAL_SCOPE_BYTES: "8MiB" }, "READ_API_CANONICAL_SCOPE_BYTES"],
    [{ READ_API_CANONICAL_SCOPE_BYTES: "4GiB" }, "READ_API_CANONICAL_SCOPE_BYTES"],
    [{ READ_API_CANONICAL_SCOPE_EVENTS: "50000" }, "READ_API_CANONICAL_SCOPE_EVENTS"],
    [{ READ_API_CANONICAL_SCOPE_EVENTS: "lots" }, "READ_API_CANONICAL_SCOPE_EVENTS"],
  ])("%p refuses startup", async (over, name) => {
    await expect(serviceCanonicalDeps({ ...env, ...over }, run)).rejects.toThrow(name);
  });
  test("LOCKER_BUCKET unset: nothing is wired, so the budget is not read (as READ_API_CANONICAL_PIN_LAGGING_POINTER)", async () => {
    expect(await serviceCanonicalDeps({ READ_API_CANONICAL_SCOPE_BYTES: "bad" }, run)).toBeUndefined();
  });
});
