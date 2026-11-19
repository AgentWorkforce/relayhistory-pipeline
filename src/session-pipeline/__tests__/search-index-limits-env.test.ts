import { describe, expect, test } from "bun:test";
import { REPLAY_SEARCH_INDEX_LIMITS, SEARCH_INDEX_DEFAULT_LIMITS, SEARCH_INDEX_STREAM_CH_CEILING, STAGED_KEYED_RECORDS_CAP, STREAM_READ_CEILING, StagedRefusals, searchIndexLimitsFromEnv, searchIndexStagedForIndex, searchIndexStagedOn, searchIndexStagedStartup, searchIndexStagedState, searchIndexStreamChFromEnv, searchIndexStreamFlag, searchIndexStreamFromEnv, searchIndexStreamStartup } from "../search-index.ts";

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

describe("SEARCH_INDEX_STREAMING_CH (hosted Lambda streaming reconcile)", () => {
  const on = { SEARCH_INDEX_STREAMING_CH: "1" };
  const message = (env: Record<string, string>, limits = SEARCH_INDEX_DEFAULT_LIMITS) => {
    try { searchIndexStreamChFromEnv(env, limits); } catch (error) { return (error as Error).message; }
    return "";
  };

  test("off unless exactly 1; off reads none of the knobs and ignores the self-host flag", () => {
    for (const flag of [undefined, "", "0"])
      expect(searchIndexStreamChFromEnv({ SEARCH_INDEX_STREAMING_CH: flag, SEARCH_INDEX_STREAM_CH_MAX_READ_BYTES: "nonsense", SEARCH_INDEX_STREAM_CH_MAX_READ_EVENTS: "-1", SEARCH_INDEX_STREAM_DEADLINE_MARGIN_MS: "1" }, SEARCH_INDEX_DEFAULT_LIMITS)).toBeUndefined();
    expect(searchIndexStreamChFromEnv({ SEARCH_INDEX_STREAMING_ENABLED: "true", SEARCH_INDEX_STREAM_MAX_READ_BYTES: "2GiB" }, SEARCH_INDEX_DEFAULT_LIMITS)).toBeUndefined();
    for (const flag of ["true", "yes", "on", " 1", "1 ", "01", "2"])
      expect(message({ SEARCH_INDEX_STREAMING_CH: flag })).toBe("session pipeline: SEARCH_INDEX_STREAMING_CH must be 1 or 0");
  });

  test("defaults 1 GiB / 1,000,000 / 60 s; set values within the floor and the 2 GiB / 1,000,000 hosted cap", () => {
    expect(searchIndexStreamChFromEnv(on, SEARCH_INDEX_DEFAULT_LIMITS)).toEqual({ maxReadBytes: 1024 * MiB, maxReadEvents: 1_000_000, deadlineMarginMs: 60_000 });
    // Set to the self-host names: still the defaults.
    expect(searchIndexStreamChFromEnv({ ...on, SEARCH_INDEX_STREAM_MAX_READ_BYTES: "2GiB", SEARCH_INDEX_STREAM_MAX_READ_EVENTS: "2000000" }, SEARCH_INDEX_DEFAULT_LIMITS)).toEqual({ maxReadBytes: 1024 * MiB, maxReadEvents: 1_000_000, deadlineMarginMs: 60_000 });
    expect(searchIndexStreamChFromEnv({ ...on, SEARCH_INDEX_STREAM_CH_MAX_READ_BYTES: "2GiB", SEARCH_INDEX_STREAM_CH_MAX_READ_EVENTS: "1000000", SEARCH_INDEX_STREAM_DEADLINE_MARGIN_MS: "240000" }, SEARCH_INDEX_DEFAULT_LIMITS))
      .toEqual({ maxReadBytes: 2048 * MiB, maxReadEvents: 1_000_000, deadlineMarginMs: 240_000 });
    // The hosted events cap is memory-bound (SEARCH_INDEX_STREAM_CH_CEILING), below the self-host 2,000,000.
    expect(SEARCH_INDEX_STREAM_CH_CEILING).toEqual({ maxReadBytes: STREAM_READ_CEILING.maxReadBytes, maxReadEvents: 1_000_000 });
    expect(STREAM_READ_CEILING.maxReadEvents).toBe(2_000_000);
    // The floor is four whole reads (256 MiB / 400,000 at the default limits).
    expect(searchIndexStreamChFromEnv({ ...on, SEARCH_INDEX_STREAM_CH_MAX_READ_BYTES: "256MiB", SEARCH_INDEX_STREAM_CH_MAX_READ_EVENTS: "400000", SEARCH_INDEX_STREAM_DEADLINE_MARGIN_MS: "10000" }, SEARCH_INDEX_DEFAULT_LIMITS))
      .toEqual({ maxReadBytes: 256 * MiB, maxReadEvents: 400_000, deadlineMarginMs: 10_000 });
    expect(searchIndexStreamChFromEnv({ ...on, SEARCH_INDEX_STREAM_CH_MAX_READ_BYTES: "268435456" }, SEARCH_INDEX_DEFAULT_LIMITS)!.maxReadBytes).toBe(256 * MiB);
    // The replay profile's events floor is past the hosted cap: it is clamped to the cap.
    expect(searchIndexStreamChFromEnv(on, REPLAY_SEARCH_INDEX_LIMITS)).toEqual({ maxReadBytes: 1024 * MiB, maxReadEvents: 1_000_000, deadlineMarginMs: 60_000 });
    expect(message({ ...on, SEARCH_INDEX_STREAM_CH_MAX_READ_EVENTS: "999999" }, REPLAY_SEARCH_INDEX_LIMITS)).toBe("session pipeline: SEARCH_INDEX_STREAM_CH_MAX_READ_EVENTS must be a whole number in 1000000..1000000");
    expect(message({ ...on, SEARCH_INDEX_STREAM_CH_MAX_READ_EVENTS: "2000000" }, REPLAY_SEARCH_INDEX_LIMITS)).toBe("session pipeline: SEARCH_INDEX_STREAM_CH_MAX_READ_EVENTS must be a whole number in 1000000..1000000");
  });

  test("out-of-range and malformed values refuse with a fixed message that never carries the value", () => {
    const BYTES = "session pipeline: SEARCH_INDEX_STREAM_CH_MAX_READ_BYTES must be a whole number of bytes (optional MiB or GiB suffix) in 256MiB..2048MiB";
    const EVENTS = "session pipeline: SEARCH_INDEX_STREAM_CH_MAX_READ_EVENTS must be a whole number in 400000..1000000";
    const MARGIN = "session pipeline: SEARCH_INDEX_STREAM_DEADLINE_MARGIN_MS must be a whole number of milliseconds in 10000..240000";
    for (const [name, value, expected] of [
      ["SEARCH_INDEX_STREAM_CH_MAX_READ_BYTES", "255MiB", BYTES],
      ["SEARCH_INDEX_STREAM_CH_MAX_READ_BYTES", "2049MiB", BYTES],
      ["SEARCH_INDEX_STREAM_CH_MAX_READ_BYTES", "3GiB", BYTES],
      ["SEARCH_INDEX_STREAM_CH_MAX_READ_BYTES", "1.5GiB", BYTES],
      ["SEARCH_INDEX_STREAM_CH_MAX_READ_BYTES", "1gib", BYTES],
      ["SEARCH_INDEX_STREAM_CH_MAX_READ_EVENTS", "399999", EVENTS],
      ["SEARCH_INDEX_STREAM_CH_MAX_READ_EVENTS", "1000001", EVENTS],
      ["SEARCH_INDEX_STREAM_CH_MAX_READ_EVENTS", "2000000", EVENTS],
      ["SEARCH_INDEX_STREAM_CH_MAX_READ_EVENTS", "1MiB", EVENTS],
      ["SEARCH_INDEX_STREAM_CH_MAX_READ_EVENTS", "0x7a120", EVENTS],
      ["SEARCH_INDEX_STREAM_DEADLINE_MARGIN_MS", "9999", MARGIN],
      ["SEARCH_INDEX_STREAM_DEADLINE_MARGIN_MS", "240001", MARGIN],
      ["SEARCH_INDEX_STREAM_DEADLINE_MARGIN_MS", "60s", MARGIN],
      ["SEARCH_INDEX_STREAM_DEADLINE_MARGIN_MS", "-60000", MARGIN],
      ["SEARCH_INDEX_STREAM_DEADLINE_MARGIN_MS", "060000", MARGIN],
    ] as const) {
      const got = message({ ...on, [name]: value });
      expect(got).toBe(expected);
      expect(got).not.toContain(value);
    }
  });
});

describe("SEARCH_INDEX_STAGED_RECONCILE_ENABLED (self-host staged reconcile, default on since Phase 3 slice 4)", () => {
  const VAR = "SEARCH_INDEX_STAGED_RECONCILE_ENABLED";
  const stream = { ...STREAM_READ_CEILING };
  const refusal = (env: Record<string, string | undefined>, streaming: typeof stream | undefined, outputFacts: boolean) => {
    try {
      searchIndexStagedStartup(env, streaming, outputFacts);
    } catch (error) {
      return (error as Error).message;
    }
    return "accepted";
  };
  const ON_LINE = "pipeline consumer staged reconcile on (keyed records cap 500000)";
  const DEFAULT_LINE = "pipeline consumer staged reconcile on by default (keyed records cap 500000; SEARCH_INDEX_STAGED_RECONCILE_ENABLED=false turns it off)";
  const OFF_STREAMING_LINE = "pipeline consumer staged reconcile off: SEARCH_INDEX_STREAMING_ENABLED=false (it runs only with streaming)";
  const STAGED_INVALID = "pipeline consumer: SEARCH_INDEX_STAGED_RECONCILE_ENABLED must be true or false";
  const STAGED_NEEDS_STREAM = "pipeline consumer: SEARCH_INDEX_STAGED_RECONCILE_ENABLED=true requires the streaming reconcile on (SEARCH_INDEX_STREAMING_ENABLED)";
  const STREAM_WITH_FACTS = "pipeline consumer: SEARCH_INDEX_STREAMING_ENABLED=true cannot be combined with RELAYHISTORY_SELFHOST_OUTPUT_FACTS=true";

  // The full truth table, as serve() resolves it (streaming first, then the staged reconcile), written out by hand:
  // staged x streaming x output facts -> the streaming flag and the staged state, or the startup error.
  type Cell = { stream: string; staged: string } | { error: string };
  const U = undefined;
  const TABLE: Array<[string | undefined, string | undefined, boolean, Cell]> = [
    // staged unset
    [U, U, false, { stream: "default_on", staged: "default_on" }],
    [U, U, true, { stream: "default_on", staged: "default_on" }],
    [U, "true", false, { stream: "explicit_on", staged: "default_on" }],
    [U, "true", true, { stream: "explicit_on", staged: "default_on" }],
    [U, "false", false, { stream: "explicit_off", staged: "default_off_streaming" }],
    [U, "false", true, { stream: "explicit_off", staged: "default_off_streaming" }],
    // staged ""
    ["", U, false, { stream: "default_on", staged: "default_on" }],
    ["", U, true, { stream: "default_on", staged: "default_on" }],
    ["", "true", false, { stream: "explicit_on", staged: "default_on" }],
    ["", "true", true, { stream: "explicit_on", staged: "default_on" }],
    ["", "false", false, { stream: "explicit_off", staged: "default_off_streaming" }],
    ["", "false", true, { stream: "explicit_off", staged: "default_off_streaming" }],
    // staged "true"
    ["true", U, false, { stream: "default_on", staged: "explicit_on" }],
    ["true", U, true, { stream: "default_on", staged: "explicit_on" }],
    ["true", "true", false, { stream: "explicit_on", staged: "explicit_on" }],
    ["true", "true", true, { stream: "explicit_on", staged: "explicit_on" }],
    ["true", "false", false, { error: STAGED_NEEDS_STREAM }],
    ["true", "false", true, { error: STAGED_NEEDS_STREAM }],
    // staged "false": exactly the pre-staged answers
    ["false", U, false, { stream: "default_on", staged: "explicit_off" }],
    ["false", U, true, { stream: "default_off_output_facts", staged: "explicit_off" }],
    ["false", "true", false, { stream: "explicit_on", staged: "explicit_off" }],
    ["false", "true", true, { error: STREAM_WITH_FACTS }],
    ["false", "false", false, { stream: "explicit_off", staged: "explicit_off" }],
    ["false", "false", true, { stream: "explicit_off", staged: "explicit_off" }],
    // staged "1": refused (streaming answers as with the staged reconcile off, and may refuse first)
    ["1", U, false, { error: STAGED_INVALID }],
    ["1", U, true, { error: STAGED_INVALID }],
    ["1", "true", false, { error: STAGED_INVALID }],
    ["1", "true", true, { error: STREAM_WITH_FACTS }],
    ["1", "false", false, { error: STAGED_INVALID }],
    ["1", "false", true, { error: STAGED_INVALID }],
  ];

  test("the truth table: staged {unset, \"\", true, false, 1} x streaming {unset, true, false} x output facts {off, on}", () => {
    expect(TABLE).toHaveLength(30);
    for (const [staged, streaming, facts, want] of TABLE) {
      const env: Record<string, string | undefined> = {
        ...(staged === undefined ? {} : { [VAR]: staged }),
        ...(streaming === undefined ? {} : { SEARCH_INDEX_STREAMING_ENABLED: streaming }),
      };
      let got: Cell;
      try {
        const flag = searchIndexStreamFlag(env, facts);
        const resolved = searchIndexStreamFromEnv(env, {}, facts);
        const start = searchIndexStagedStartup(env, resolved, facts);
        got = { stream: flag, staged: start.state };
        // On exactly when the state says so, and then with streaming resolved on.
        expect(!!start.staged, JSON.stringify(env)).toBe(start.state === "explicit_on" || start.state === "default_on");
        if (start.staged) expect(resolved).toBeDefined();
        expect(searchIndexStagedOn(env)).toBe(start.state === "explicit_on" || start.state === "default_on");
        expect(searchIndexStagedState(env)).toBe(start.state);
      } catch (error) {
        got = { error: (error as Error).message };
      }
      expect(got, `staged=${String(staged)} streaming=${String(streaming)} facts=${facts}`).toEqual(want);
    }
  });

  test("one startup line per state but explicit_off; explicit_off logs nothing and stages nothing", () => {
    expect(searchIndexStagedStartup({ [VAR]: "false" }, stream, false)).toEqual({ state: "explicit_off" });
    expect(searchIndexStagedStartup({ [VAR]: "false" }, undefined, true)).toEqual({ state: "explicit_off" });
    expect(searchIndexStagedStartup({ SEARCH_INDEX_STREAMING_ENABLED: "false" }, undefined, false)).toEqual({
      state: "default_off_streaming",
      notice: { level: "info", message: OFF_STREAMING_LINE, fields: { state: "off", reason: "streaming_off" } },
    });
    for (const env of [{}, { [VAR]: "" }])
      expect(searchIndexStagedStartup(env, stream, false)).toEqual({
        state: "default_on",
        staged: { keyedRecordsCap: STAGED_KEYED_RECORDS_CAP, refusals: expect.any(StagedRefusals) },
        notice: { level: "info", message: DEFAULT_LINE, fields: { state: "on" } },
      });
    expect(searchIndexStagedStartup({ [VAR]: "true" }, stream, false)).toEqual({
      state: "explicit_on",
      staged: { keyedRecordsCap: STAGED_KEYED_RECORDS_CAP, refusals: expect.any(StagedRefusals) },
      notice: { level: "info", message: ON_LINE, fields: { state: "on" } },
    });
  });

  test("on: the interim keyed-records cap, a fresh StagedRefusals per start; explicitly on needs streaming", () => {
    expect(STAGED_KEYED_RECORDS_CAP).toBe(500_000);
    // The cap never asks for more key memory than the default streaming budgets already stream.
    expect(STAGED_KEYED_RECORDS_CAP).toBeLessThanOrEqual(STREAM_READ_CEILING.maxReadEvents / 4);
    const a = searchIndexStagedStartup({}, stream, true).staged!.refusals;
    const b = searchIndexStagedStartup({ [VAR]: "true" }, stream, true).staged!.refusals;
    expect(a).not.toBe(b);
    expect([a!.size, b!.size]).toEqual([0, 0]);
    expect(refusal({ [VAR]: "true" }, undefined, false)).toBe(STAGED_NEEDS_STREAM);
    expect(refusal({ [VAR]: "true" }, undefined, true)).toBe(STAGED_NEEDS_STREAM);
  });

  test("an index without keyset pages: explicitly on refuses to start, by default the staged reconcile is off with a warn line", () => {
    const without = {};
    const withPages = { scanFingerprintsPage: () => undefined };
    const explicit = searchIndexStagedStartup({ [VAR]: "true" }, stream, false);
    const byDefault = searchIndexStagedStartup({}, stream, false);
    expect(() => searchIndexStagedForIndex(explicit, without)).toThrow("pipeline consumer: SEARCH_INDEX_STAGED_RECONCILE_ENABLED=true requires a search index with paged scans");
    expect(searchIndexStagedForIndex(byDefault, without)).toEqual({
      notice: { level: "warn", message: expect.stringContaining("no paged scans"), fields: { state: "off", reason: "no_paged_scan" } },
    });
    expect(searchIndexStagedForIndex(explicit, withPages)).toEqual({ staged: explicit.staged });
    expect(searchIndexStagedForIndex(byDefault, withPages)).toEqual({ staged: byDefault.staged });
    expect(searchIndexStagedForIndex(searchIndexStagedStartup({ [VAR]: "false" }, stream, false), without)).toEqual({});
    // With output facts the fallback puts streaming back off, with exactly the line the flag off gives:
    // the consumer is then on today's path (facts from a whole read, a too-large scope scope_too_large).
    const factsDefault = searchIndexStagedStartup({}, stream, true);
    const today = searchIndexStreamStartup({ [VAR]: "false" }, {}, true);
    expect(today.stream).toBeUndefined();
    expect(searchIndexStagedForIndex(factsDefault, without, true)).toEqual({
      notice: expect.objectContaining({ fields: { state: "off", reason: "no_paged_scan" } }),
      streamOff: today.notice,
    });
    expect(searchIndexStagedForIndex(factsDefault, withPages, true)).toEqual({ staged: factsDefault.staged });
    expect(searchIndexStagedForIndex(byDefault, without, false).streamOff).toBeUndefined();
    // An explicit SEARCH_INDEX_STREAMING_ENABLED=true is never turned off silently: with output facts the
    // fallback fails startup with the error the staged reconcile off gives; without them streaming stays on.
    const STREAM_WITH_FACTS = "pipeline consumer: SEARCH_INDEX_STREAMING_ENABLED=true cannot be combined with RELAYHISTORY_SELFHOST_OUTPUT_FACTS=true";
    expect(() => searchIndexStreamFlag({ [VAR]: "false", SEARCH_INDEX_STREAMING_ENABLED: "true" }, true)).toThrow(STREAM_WITH_FACTS);
    for (const staged of [undefined, ""]) {
      const env = { [VAR]: staged, SEARCH_INDEX_STREAMING_ENABLED: "true" };
      expect(searchIndexStreamFlag(env, true)).toBe("explicit_on");
      const started = searchIndexStagedStartup(env, stream, true);
      expect(started.streamingExplicitOn).toBe(true);
      expect(() => searchIndexStagedForIndex(started, without, true)).toThrow(STREAM_WITH_FACTS);
      expect(searchIndexStagedForIndex(started, withPages, true)).toEqual({ staged: started.staged });
      expect(searchIndexStagedForIndex(started, without, false)).toEqual({
        notice: expect.objectContaining({ fields: { state: "off", reason: "no_paged_scan" } }),
      });
    }
    expect(factsDefault.streamingExplicitOn).toBeUndefined();
    // Explicitly on with explicit streaming still refuses with its own (paged scans) error first.
    expect(() => searchIndexStagedForIndex(searchIndexStagedStartup({ [VAR]: "true", SEARCH_INDEX_STREAMING_ENABLED: "true" }, stream, true), without, true)).toThrow(
      "requires a search index with paged scans",
    );
  });

  test("any other value fails startup with a fixed message that never carries the value", () => {
    for (const value of ["1", "0", "TRUE", "True", "FALSE", " true", "true ", "yes", "on", "off", "enabled"]) {
      for (const streaming of [stream, undefined]) {
        // One fixed message for every value: it cannot carry the value.
        expect(refusal({ [VAR]: value }, streaming, false)).toBe("pipeline consumer: SEARCH_INDEX_STAGED_RECONCILE_ENABLED must be true or false");
      }
    }
  });
});
