/**
 * The read Worker's hosted canonical events wiring (packages/session-read-worker/src/index.ts):
 * READ_API_CANONICAL_EVENTS_HOSTED is off unset or empty, on only for exactly "true", and any
 * other value refuses every request (503, logged by field name). READ_API_CANONICAL_EVENTS_WORKSPACES
 * unset or empty allows no workspace. Timing keeps its own answer shape.
 */
import { afterEach, describe, expect, spyOn, test } from "bun:test";
import { canonicalEventsHostedEnabled, handle, timingDeps, workerCanonicalEventsHosted, type Env } from "../../../packages/session-read-worker/src/index.ts";
import type { TenantRunner } from "../liveness.ts";

const identity = { orgId: "org-a", workspaceId: "ws-a", sessionId: "s-1", source: "codex" };
const noPointers: TenantRunner = async (_o, _w, work) => work({ async query<R>() { return { rows: [] as R[] }; } });
const rawStore = { PIPELINE_RAW_S3_BUCKET: "bucket", PIPELINE_RAW_S3_ENDPOINT: "https://objects.test.example", PIPELINE_RAW_S3_REGION: "auto", PIPELINE_RAW_S3_ACCESS_KEY_ID: "synthetic", PIPELINE_RAW_S3_SECRET_ACCESS_KEY: "synthetic" };
const noFetch = (async () => { throw new Error("no network in this test"); }) as unknown as typeof fetch;
const JUNK = ["1", "TRUE", "True", "false", "on", " true", "true ", "yes"];

describe("canonicalEventsHostedEnabled", () => {
  test("unset and empty are off; exactly true is on; anything else is a configuration error", () => {
    expect(canonicalEventsHostedEnabled(undefined)).toBe(false);
    expect(canonicalEventsHostedEnabled("")).toBe(false);
    expect(canonicalEventsHostedEnabled("true")).toBe(true);
    for (const value of JUNK) expect(() => canonicalEventsHostedEnabled(value)).toThrow("READ_API_CANONICAL_EVENTS_HOSTED");
  });
});

describe("workerCanonicalEventsHosted", () => {
  test("flag unset or empty: the deps carry neither key, with or without the raw store", () => {
    for (const flag of [undefined, ""]) {
      expect(workerCanonicalEventsHosted({ READ_API_CANONICAL_EVENTS_HOSTED: flag, READ_API_CANONICAL_EVENTS_WORKSPACES: "*" } as Env, noPointers, noFetch)).toEqual({});
      expect(workerCanonicalEventsHosted({ ...rawStore, READ_API_CANONICAL_EVENTS_HOSTED: flag, READ_API_CANONICAL_EVENTS_WORKSPACES: "*" } as Env, noPointers, noFetch)).toEqual({});
    }
  });

  test("exactly true without the raw-store secrets stays off", () => {
    expect(workerCanonicalEventsHosted({ READ_API_CANONICAL_EVENTS_HOSTED: "true", READ_API_CANONICAL_EVENTS_WORKSPACES: "*" } as Env, noPointers, noFetch)).toEqual({});
  });

  test("exactly true with the raw store: both keys; an unset or empty allowlist allows no workspace", async () => {
    for (const list of [undefined, "", "  "]) {
      const deps = workerCanonicalEventsHosted({ ...rawStore, READ_API_CANONICAL_EVENTS_HOSTED: "true", READ_API_CANONICAL_EVENTS_WORKSPACES: list } as Env, noPointers, noFetch);
      expect(Object.keys(deps).sort()).toEqual(["canonicalEvents", "canonicalEventsHosted"]);
      const workspaces = deps.canonicalEventsHosted!.workspaces;
      expect(workspaces !== "*" && workspaces.size).toBe(0);
    }
    const listed = workerCanonicalEventsHosted({ ...rawStore, READ_API_CANONICAL_EVENTS_HOSTED: "true", READ_API_CANONICAL_EVENTS_WORKSPACES: "ws-a" } as Env, noPointers, noFetch);
    expect(listed.canonicalEventsHosted!.workspaces).toEqual(new Set(["ws-a"]));
    // The object source carries generation and existence for the events read.
    expect(await listed.canonicalEvents!.source(identity, new AbortController().signal)).toMatchObject({ rows: [], exists: false, legacyCoverage: null });
    expect(await listed.canonicalEventsHosted!.sources("org-a", "ws-a", "s-1")).toEqual([]);
  });

  test("origin labels follow MESSAGE_ORIGIN_LABELS_ENABLED exactly true", () => {
    for (const value of [undefined, "", "1", "TRUE"]) {
      const deps = workerCanonicalEventsHosted({ ...rawStore, READ_API_CANONICAL_EVENTS_HOSTED: "true", MESSAGE_ORIGIN_LABELS_ENABLED: value } as Env, noPointers, noFetch);
      expect(deps.canonicalEvents!.originLabels).toBeUndefined();
    }
    const on = workerCanonicalEventsHosted({ ...rawStore, READ_API_CANONICAL_EVENTS_HOSTED: "true", MESSAGE_ORIGIN_LABELS_ENABLED: "true" } as Env, noPointers, noFetch);
    expect(on.canonicalEvents!.originLabels).toBe(true);
  });
});

describe("timing is unchanged by the hosted flag", () => {
  test("the timing source answers only rows and legacy coverage, flag on or off", async () => {
    for (const flag of [undefined, "true"]) {
      const deps = timingDeps({ ...rawStore, READ_API_CANONICAL_EVENTS_HOSTED: flag } as Env, noPointers, noFetch)!;
      expect(Object.keys(deps).sort()).toEqual(["assumeNoTurns", "source", "state"]);
      const answer = await deps.source(identity, new AbortController().signal);
      expect(answer).toEqual({ rows: [], legacyCoverage: null });
      expect(Object.keys(answer).sort()).toEqual(["legacyCoverage", "rows"]);
    }
    expect(timingDeps({ READ_API_CANONICAL_EVENTS_HOSTED: "true" } as Env, noPointers, noFetch)).toBeUndefined();
  });
});

describe("handle(): the hosted flag value", () => {
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
  const PATHS = ["/v1/sessions/s-1/events?workspace=ws&source=codex", "/v1/sessions/s-1/events?workspace=ws", "/v1/sessions?workspace=ws"];

  test("unset, empty and exactly true are served (401 without a token), with or without the raw store and allowlist", async () => {
    for (const env of [base, { ...base, READ_API_CANONICAL_EVENTS_HOSTED: "" }, { ...base, READ_API_CANONICAL_EVENTS_HOSTED: "true" },
      { ...base, ...rawStore, READ_API_CANONICAL_EVENTS_HOSTED: "true", READ_API_CANONICAL_EVENTS_WORKSPACES: "" },
      { ...base, ...rawStore, READ_API_CANONICAL_EVENTS_HOSTED: "true", READ_API_CANONICAL_EVENTS_WORKSPACES: "ws" }])
      for (const path of PATHS) expect([env, path, await status(env, path)]).toEqual([env, path, 401]);
  });

  test("any other value refuses every request with a 503 naming the field", async () => {
    for (const value of JUNK) {
      const errors = spyOn(console, "error").mockImplementation(() => {});
      for (const path of PATHS) expect([value, path, await status({ ...base, ...rawStore, READ_API_CANONICAL_EVENTS_HOSTED: value }, path)]).toEqual([value, path, 503]);
      expect(errors.mock.calls.some((c) => String(c[0]).includes("missing_READ_API_CANONICAL_EVENTS_HOSTED"))).toBe(true);
      expect(errors.mock.calls.some((c) => String(c[0]).includes(JSON.stringify(value)))).toBe(false);
      errors.mockRestore();
    }
  });
});
