/**
 * The read Worker (packages/session-read-worker/src/index.ts) serves only the ClickHouse summary search:
 * READ_API_SUMMARY_SEARCH_BACKEND unset, "" or "clickhouse" changes nothing; "postgres" (self-host
 * service only) or any other value refuses every request with the misconfiguration 503, logging the
 * variable name and never the value.
 */
import { afterEach, describe, expect, spyOn, test } from "bun:test";
import { handle, type Env } from "../../../packages/session-read-worker/src/index.ts";

const BASE = {
  CLICKHOUSE_URL: "https://ch.test.example", CLICKHOUSE_USER: "u", CLICKHOUSE_PASSWORD: "p", CLICKHOUSE_DATABASE: "d",
  READ_API_ORIGIN: "https://read.test.example", READ_API_COMPLETE_SINCE: "epoch",
  RELAYAUTH_JWKS_URL: "https://auth.test.example/jwks", RELAYAUTH_ISSUER: "https://relayauth.dev",
  READ_API_SUMMARY_SEARCH_ENABLED: "true",
};
const ctx = { waitUntil: () => {}, passThroughOnException: () => {} } as never;
const overrides = { connect: async () => { throw new Error("no database in this test"); }, fetcher: (async () => new Response("{}", { status: 500 })) as never };
const status = async (env: Record<string, string>, path: string) =>
  (await handle(new Request(`https://read.test.example${path}`), env as unknown as Env, ctx, overrides as never)).status;

describe("READ_API_SUMMARY_SEARCH_BACKEND on the read Worker", () => {
  afterEach(() => { (console.error as { mockRestore?: () => void }).mockRestore?.(); });

  test("unset, empty or clickhouse: served as before (401 without a token)", async () => {
    for (const extra of [{}, { READ_API_SUMMARY_SEARCH_BACKEND: "" }, { READ_API_SUMMARY_SEARCH_BACKEND: "clickhouse" }])
      for (const path of ["/v1/sessions/summaries/search?workspace=ws&q=erasure", "/v1/sessions?workspace=ws"])
        expect([extra, path, await status({ ...BASE, ...extra }, path)]).toEqual([extra, path, 401]);
  });

  test("postgres or an unknown value refuses every request, naming the variable, never the value", async () => {
    for (const value of ["postgres", "Postgres", "pg", "synthetic-value-x"]) {
      const errors = spyOn(console, "error").mockImplementation(() => {});
      for (const path of ["/v1/sessions/summaries/search?workspace=ws&q=erasure", "/v1/sessions?workspace=ws"])
        expect([value, path, await status({ ...BASE, READ_API_SUMMARY_SEARCH_BACKEND: value }, path)]).toEqual([value, path, 503]);
      const logged = errors.mock.calls.map((c) => c.map(String).join(" ")).join("\n");
      expect(logged).toContain("missing_READ_API_SUMMARY_SEARCH_BACKEND");
      expect(logged).not.toContain(value);
      errors.mockRestore();
    }
  });
});
