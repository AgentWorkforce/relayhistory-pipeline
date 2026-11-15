import { afterEach, describe, expect, test } from "bun:test";
import pg from "pg";
import { READ_API_SEARCH_PARTIAL_SESSIONS_VAR, readApiDepsFromSecret, searchSkipsPartialSessions } from "../lambda.ts";

const env = (value?: string) => (value === undefined ? {} : { [READ_API_SEARCH_PARTIAL_SESSIONS_VAR]: value }) as NodeJS.ProcessEnv;
const INVALID = `session read api: ${READ_API_SEARCH_PARTIAL_SESSIONS_VAR} must be unset, "", "refuse" or "skip"`;
const SERVICE_ONLY = `session read api: ${READ_API_SEARCH_PARTIAL_SESSIONS_VAR} other than "refuse" is served only by the self-host service`;

describe("searchSkipsPartialSessions", () => {
  test("unset, empty and refuse keep the refusal on either entry", () => {
    for (const entry of ["service", "lambda"] as const)
      for (const value of [undefined, "", "refuse"]) expect(searchSkipsPartialSessions(env(value), entry)).toBe(false);
  });

  test("skip turns it on for the self-host service", () => {
    expect(searchSkipsPartialSessions(env("skip"), "service")).toBe(true);
  });

  test("skip refuses the hosted Lambda rather than being ignored", () => {
    expect(() => searchSkipsPartialSessions(env("skip"), "lambda")).toThrow(SERVICE_ONLY);
  });

  test("any other value refuses startup on either entry, naming the variable and never the value", () => {
    for (const entry of ["service", "lambda"] as const) {
      for (const value of ["true", "1", "Skip", "SKIP", "skip ", " skip", "Refuse", "refuse ", "s3cret"]) {
        let message = "";
        try { searchSkipsPartialSessions(env(value), entry); } catch (error) { message = (error as Error).message; }
        expect(message).toBe(INVALID);
      }
    }
  });
});

describe("the hosted Lambda assembly", () => {
  const RealPool = pg.Pool;
  afterEach(() => { pg.Pool = RealPool; });

  test("refuses skip before any pool is built; refuse and unset build as before", async () => {
    let pools = 0;
    pg.Pool = class { constructor() { pools++; } on() { return this; } async query() { return { rows: [] }; } async end() {} } as unknown as typeof pg.Pool;
    const secret = JSON.stringify({ supabase_url: "postgresql://reader:pw@db.example.internal:5432/history", clickhouse: { url: "https://ch.example.internal:8443", user: "u", password: "p", database: "relay_search" } });
    const base = { READ_API_COMPLETE_SINCE: "epoch" } as NodeJS.ProcessEnv;
    const error = await readApiDepsFromSecret(secret, { ...base, ...env("skip") }).then(() => null, (e: Error) => e);
    expect(error?.message).toBe(SERVICE_ONLY);
    expect(pools).toBe(0);
    for (const value of [undefined, "refuse"]) {
      const built = await readApiDepsFromSecret(secret, { ...base, ...env(value) });
      expect("searchSkipsPartialSessions" in built.deps).toBe(false);
      await built.close();
    }
  });
});
