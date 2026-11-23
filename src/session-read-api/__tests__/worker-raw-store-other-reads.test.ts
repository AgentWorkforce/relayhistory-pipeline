/**
 * The read Worker's READ_API_RAW_STORE_OTHER_READS switch (packages/session-read-worker/src/index.ts,
 * canonical.ts): unset or empty = the raw-store reads other than the summary fallback and analysis
 * behave as before; exactly "off" = timing, turns, metadata (and owner-buffered turns, which build
 * on turns), hosted canonical events, babysitter evidence and /v1/canonical/* answer as without a
 * raw store; any other value refuses every request that reaches the configuration step (503, logged
 * by field name) and withholds the store from /v1/canonical/*.
 */
import { afterEach, describe, expect, spyOn, test } from "bun:test";
import { analysisDeps, babysitterEvidenceDeps, canonicalEventsDeps, handle, rawStoreOtherReadsEnabled, timingDeps, turnsDeps, type Env } from "../../../packages/session-read-worker/src/index.ts";
import { rawStoreForCanonical } from "../../../packages/session-read-worker/src/canonical.ts";
import type { TenantRunner } from "../liveness.ts";

const noPointers: TenantRunner = async (_o, _w, work) => work({ async query<R>() { return { rows: [] as R[] }; } });
const rawStore = { PIPELINE_RAW_S3_BUCKET: "example-bucket", PIPELINE_RAW_S3_ENDPOINT: "https://objects.test.example", PIPELINE_RAW_S3_REGION: "auto", PIPELINE_RAW_S3_ACCESS_KEY_ID: "synthetic", PIPELINE_RAW_S3_SECRET_ACCESS_KEY: "synthetic" };
const noFetch = (async () => { throw new Error("no network in this test"); }) as unknown as typeof fetch;
const JUNK = ["OFF", "Off", "on", "false", "0", " off", "off ", "true", "disable"];
const SWITCH = "READ_API_RAW_STORE_OTHER_READS";

describe("rawStoreOtherReadsEnabled", () => {
  test("unset and empty are as before (enabled); exactly off disables; anything else is a configuration error naming the variable", () => {
    expect(rawStoreOtherReadsEnabled(undefined)).toBe(true);
    expect(rawStoreOtherReadsEnabled("")).toBe(true);
    expect(rawStoreOtherReadsEnabled("off")).toBe(false);
    for (const value of JUNK) expect(() => rawStoreOtherReadsEnabled(value), value).toThrow(SWITCH);
  });
});

describe("timingDeps / turnsDeps", () => {
  test("unset or empty with the raw store: built as before; without the raw store: undefined", () => {
    for (const value of [undefined, ""]) {
      const env = { ...rawStore, [SWITCH]: value } as Env;
      expect(Object.keys(timingDeps(env, noPointers, noFetch)!).sort()).toEqual(["assumeNoTurns", "source", "state"]);
      expect(Object.keys(turnsDeps(env, noPointers, noFetch)!).sort()).toEqual(["harnesses", "source"]);
      expect(timingDeps({ [SWITCH]: value } as Env, noPointers, noFetch)).toBeUndefined();
      expect(turnsDeps({ [SWITCH]: value } as Env, noPointers, noFetch)).toBeUndefined();
    }
  });

  test("exactly off: undefined even with the raw store (501 timing_not_configured / turns_not_configured)", () => {
    const env = { ...rawStore, [SWITCH]: "off" } as Env;
    expect(timingDeps(env, noPointers, noFetch)).toBeUndefined();
    expect(turnsDeps(env, noPointers, noFetch)).toBeUndefined();
  });

  test("any other value throws, never serves", () => {
    for (const value of JUNK) {
      expect(() => timingDeps({ ...rawStore, [SWITCH]: value } as Env, noPointers, noFetch), value).toThrow(SWITCH);
      expect(() => turnsDeps({ ...rawStore, [SWITCH]: value } as Env, noPointers, noFetch), value).toThrow(SWITCH);
    }
  });
});

describe("the other raw-store reads follow the switch; summary fallback and analysis do not", () => {
  test("hosted canonical events and babysitter evidence: built unset, undefined at off", () => {
    const events = { ...rawStore, READ_API_CANONICAL_EVENTS_HOSTED: "true" };
    const evidence = { ...rawStore, READ_API_BABYSITTER_EVIDENCE: "on" };
    expect(canonicalEventsDeps(events as Env, noPointers, noFetch)).toBeDefined();
    expect(babysitterEvidenceDeps(evidence as Env, noPointers, noFetch)).toBeDefined();
    expect(canonicalEventsDeps({ ...events, [SWITCH]: "off" } as Env, noPointers, noFetch)).toBeUndefined();
    expect(babysitterEvidenceDeps({ ...evidence, [SWITCH]: "off" } as Env, noPointers, noFetch)).toBeUndefined();
  });

  test("analysis keeps its raw store at off (the switch is not its gate)", () => {
    expect(analysisDeps({ ...rawStore, [SWITCH]: "off" } as Env, noPointers, undefined, noFetch)).toBeDefined();
    expect(analysisDeps({ [SWITCH]: "off" } as Env, noPointers, undefined, noFetch)).toBeUndefined();
  });

  test("/v1/canonical/*: the store only with the switch unset or empty; any other value withholds it (not_configured)", () => {
    for (const value of [undefined, ""]) expect(rawStoreForCanonical({ ...rawStore, [SWITCH]: value } as never, noFetch)).toBeDefined();
    for (const value of ["off", ...JUNK]) expect(rawStoreForCanonical({ ...rawStore, [SWITCH]: value } as never, noFetch), value).toBeUndefined();
    expect(rawStoreForCanonical({} as never, noFetch)).toBeUndefined();
  });
});

describe("handle(): the switch value", () => {
  afterEach(() => { (console.error as { mockRestore?: () => void }).mockRestore?.(); });
  const base = {
    CLICKHOUSE_URL: "https://ch.test.example", CLICKHOUSE_USER: "u", CLICKHOUSE_PASSWORD: "p", CLICKHOUSE_DATABASE: "d",
    READ_API_ORIGIN: "https://read.test.example", READ_API_COMPLETE_SINCE: "epoch",
    RELAYAUTH_JWKS_URL: "https://auth.test.example/jwks", RELAYAUTH_ISSUER: "https://relayauth.dev",
  };
  const ctx = { waitUntil: () => {}, passThroughOnException: () => {} } as never;
  const overrides = { connect: async () => { throw new Error("no database in this test"); }, fetcher: (async () => new Response("{}", { status: 500 })) as never };
  const status = async (env: Record<string, string | undefined>, path: string) =>
    (await handle(new Request(`https://read.test.example${path}`), env as unknown as Env, ctx, overrides as never)).status;
  const PATHS = ["/v1/sessions/s-1/timing?workspace=ws", "/v1/sessions/s-1/turns?workspace=ws", "/v1/sessions?workspace=ws"];

  test("unset, empty and exactly off are served (401 without a token)", async () => {
    for (const value of [undefined, "", "off"]) {
      for (const path of PATHS) expect([value, path, await status({ ...base, ...rawStore, [SWITCH]: value }, path)]).toEqual([value, path, 401]);
    }
  });

  test("any other value refuses every request with a 503 naming the field, never the value", async () => {
    for (const value of JUNK) {
      const errors = spyOn(console, "error").mockImplementation(() => {});
      for (const path of PATHS) expect([value, path, await status({ ...base, ...rawStore, [SWITCH]: value }, path)]).toEqual([value, path, 503]);
      expect(errors.mock.calls.some((c) => String(c[0]).includes(`missing_${SWITCH}`))).toBe(true);
      expect(errors.mock.calls.some((c) => String(c[0]).includes(JSON.stringify(value)))).toBe(false);
      errors.mockRestore();
    }
  });
});
