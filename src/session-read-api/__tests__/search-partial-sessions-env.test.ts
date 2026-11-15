import { afterEach, describe, expect, test } from "bun:test";
import pg from "pg";
import { createOpenSearchSearchIndex } from "@rhc/search-index/opensearch.ts";
import { createPostgresSearchIndex, type SqlClient } from "@rhc/search-index/postgres.ts";
import type { SearchIndexAdapter } from "@rhc/search-index/contracts.ts";
import type { SearchServiceBinding } from "../../session-pipeline/search-adapter.ts";
import { READ_API_SEARCH_PARTIAL_SESSIONS_VAR, assertSkipExcludes, readApiDepsFromSecret, searchSkipsPartialSessions } from "../lambda.ts";

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

describe("assertSkipExcludes", () => {
  const bind = (index: SearchIndexAdapter): SearchServiceBinding => ({ index, state: { async fromMs() { return 0; }, async record() {}, async partials() { return { sessions: [], overflow: false }; } } });

  test("the postgres and opensearch backends exclude sessions, so skip starts", () => {
    const client = { async query() { return { rows: [] }; } } as unknown as SqlClient;
    expect(() => assertSkipExcludes(bind(createPostgresSearchIndex({ client })))).not.toThrow();
    expect(() => assertSkipExcludes(bind(createOpenSearchSearchIndex({ url: "http://search.test:9200", fetch: async () => new Response("{}") })))).not.toThrow();
  });

  test("a backend that cannot exclude refuses skip at startup, naming the variable", () => {
    const filters = ["session", "source", "kind", "project", "since", "until"] as const;
    const index = { backend: "partial-backend", capabilities: () => ({ backend: "partial-backend", filters }) } as unknown as SearchIndexAdapter;
    expect(() => assertSkipExcludes(bind(index))).toThrow(`session read api: ${READ_API_SEARCH_PARTIAL_SESSIONS_VAR}=skip needs a search backend that excludes sessions; partial-backend does not`);
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
