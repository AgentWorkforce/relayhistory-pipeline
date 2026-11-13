/**
 * The self-host service's Output / digest backend selection (lambda.ts factsBackends and
 * readApiDepsFromSecret): READ_API_OUTPUT_BACKEND / READ_API_DIGEST_BACKEND=postgres with the
 * route's ENABLED flag ("1") wires the Postgres facts backends on the read pool, with no
 * ClickHouse; anything else wires nothing new; an unknown value refuses startup without echoing
 * it, and the hosted Lambda refuses postgres. No database is touched: pg.Pool is a recorder.
 */
import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import pg from "pg";
import { TENANT_SETTINGS_SQL } from "../liveness.ts";
import { factsBackends, READER_LOGIN_SQL, READER_PG16_SQL, readApiDepsFromSecret } from "../lambda.ts";
import { READER_LOGIN_ROW, readerLoginAnswer } from "./reader-login-support.ts";
import { selfhostSearchComposition } from "../server.ts";

const PG_URL = "postgresql://reader:pw@db.selfhost.internal:6543/history";
const ENV = {
  RELAYHISTORY_SELFHOST_READ_API: "true",
  RELAYHISTORY_READ_API_SECRET: JSON.stringify({ supabase_url: PG_URL }),
  READ_API_COMPLETE_SINCE: "epoch",
  SESSION_POSTGRES_TLS_PROFILE: "supabase",
  SEARCH_INDEX_POSTGRES_BACKEND_ID: "selfhost-pg",
} as const;

type Recorded = { text: string; params?: unknown[] };
let sql: Recorded[] = [];
const RealPool = pg.Pool;
const isAdmission = (text: string) => text === READER_LOGIN_SQL || text === READER_PG16_SQL;
const answer = async (text: string) => ({ rows: text === READER_LOGIN_SQL ? [{ ...READER_LOGIN_ROW }] : readerLoginAnswer(text) ?? [] });
class RecordingPool {
  constructor(_config: pg.PoolConfig) {}
  on() { return this; }
  async query(text: string, params?: unknown[]) { if (!isAdmission(text)) sql.push({ text, params }); return answer(text); }
  async connect() {
    return { query: async (text: string, params?: unknown[]) => { sql.push({ text, params }); return answer(text); }, release() {} };
  }
  async end() {}
}
beforeEach(() => {
  sql = [];
  pg.Pool = RecordingPool as unknown as typeof pg.Pool;
});
afterEach(() => {
  pg.Pool = RealPool;
});

const build = (env: NodeJS.ProcessEnv) => readApiDepsFromSecret(ENV.RELAYHISTORY_READ_API_SECRET, env, selfhostSearchComposition(env), { entry: "service" });

describe("self-host service facts backends", () => {
  test("postgres + ENABLED=1 wires both Postgres backends on the read pool, no ClickHouse", async () => {
    const built = await build({
      ...ENV, READ_API_OUTPUT_BACKEND: "postgres", READ_API_OUTPUT_ENABLED: "1",
      READ_API_DIGEST_BACKEND: "postgres", READ_API_DIGEST_ENABLED: "1",
      READ_API_COORDINATION_OVERLAPS_BACKEND: "postgres", READ_API_COORDINATION_OVERLAPS_ENABLED: "true",
    });
    try {
      expect(built.deps.ch).toBeUndefined();
      expect(built.deps.output?.backend?.kind).toBe("postgres");
      expect(built.deps.digest?.backend?.kind).toBe("postgres");
      expect(built.deps.coordination?.backend?.kind).toBe("postgres");
      expect(built.deps.digest?.cache).toBeDefined();
      expect(built.deps.digest?.generator).toBeUndefined();
      // The readiness read goes to 045040's coverage reader in a READ ONLY tenant transaction.
      sql = [];
      expect(await built.deps.output!.backend!.windowReadiness("org-a", "ws-a", new Date("2026-11-05T00:00:00Z"), new Date("2026-11-06T00:00:00Z"))).toBe("backfill_incomplete");
      const at = sql.findIndex((s) => s.text.includes("sessions.output_facts_coverage_for"));
      expect(at).toBeGreaterThan(1);
      expect(sql[at]!.params).toEqual(["ws-a", "2026-11-05T00:00:00.000000Z", "2026-11-06T00:00:00.000001Z"]);
      expect(sql.slice(at - 2, at).map((s) => [s.text, s.params])).toEqual([["BEGIN READ ONLY", undefined], [TENANT_SETTINGS_SQL, ["org-a", "ws-a"]]]);
    } finally {
      await built.close();
    }
  });

  test("each route is independent: one backend set, or ENABLED other than \"1\", wires only what is asked", async () => {
    const outputOnly = await build({ ...ENV, READ_API_OUTPUT_BACKEND: "postgres", READ_API_OUTPUT_ENABLED: "1", READ_API_DIGEST_ENABLED: "1" });
    expect([outputOnly.deps.output?.backend?.kind, outputOnly.deps.digest]).toEqual(["postgres", undefined]);
    await outputOnly.close();
    for (const enabled of [undefined, "", "0", "true", "yes"]) {
      const built = await build({ ...ENV, READ_API_OUTPUT_BACKEND: "postgres", READ_API_DIGEST_BACKEND: "postgres",
        ...(enabled === undefined ? {} : { READ_API_OUTPUT_ENABLED: enabled, READ_API_DIGEST_ENABLED: enabled }) });
      expect([enabled, built.deps.output, built.deps.digest]).toEqual([enabled, undefined, undefined]);
      await built.close();
    }
  });

  test("the default (unset, \"\" or clickhouse) wires nothing new on the service", async () => {
    for (const value of [undefined, "", "clickhouse"]) {
      const extra = value === undefined ? {} : { READ_API_OUTPUT_BACKEND: value, READ_API_DIGEST_BACKEND: value };
      const built = await build({ ...ENV, ...extra, READ_API_OUTPUT_ENABLED: "1", READ_API_DIGEST_ENABLED: "1" });
      expect([value, built.deps.output, built.deps.digest]).toEqual([value, undefined, undefined]);
      await built.close();
    }
    expect(sql.filter((s) => s.text.includes("output_facts_"))).toEqual([]);
  });

  test("an unknown value refuses startup, naming the flag and never echoing the value", async () => {
    for (const name of ["READ_API_OUTPUT_BACKEND", "READ_API_DIGEST_BACKEND", "READ_API_COORDINATION_OVERLAPS_BACKEND"])
      for (const value of ["Postgres", "pg", "clickhouse ", "neon"]) {
        const error = await build({ ...ENV, [name]: value }).then(() => null, (e: Error) => e);
        expect(error?.message).toContain(name);
        expect(error?.message).not.toContain(value);
      }
  });

  test("the hosted Lambda entry refuses postgres; clickhouse and unset pass", () => {
    expect(() => factsBackends({ READ_API_OUTPUT_BACKEND: "postgres" }, "lambda")).toThrow("served only by the self-host service");
    expect(() => factsBackends({ READ_API_DIGEST_BACKEND: "postgres" }, "lambda")).toThrow("served only by the self-host service");
    expect(() => factsBackends({ READ_API_COORDINATION_OVERLAPS_BACKEND: "postgres" }, "lambda")).toThrow("served only by the self-host service");
    expect(factsBackends({}, "lambda")).toEqual({ output: "clickhouse", digest: "clickhouse", coordination: "clickhouse" });
    expect(factsBackends({ READ_API_OUTPUT_BACKEND: "clickhouse" }, "lambda")).toEqual({ output: "clickhouse", digest: "clickhouse", coordination: "clickhouse" });
    expect(factsBackends({ READ_API_OUTPUT_BACKEND: "postgres", READ_API_DIGEST_BACKEND: "postgres", READ_API_COORDINATION_OVERLAPS_BACKEND: "postgres" }, "service"))
      .toEqual({ output: "postgres", digest: "postgres", coordination: "postgres" });
  });
});

describe("brief-summary and list short-summary flags (READ_API_BRIEF_SUMMARY_ENABLED, READ_API_LIST_SHORT_SUMMARY_ENABLED)", () => {
  const LAMBDA_SECRET = JSON.stringify({
    supabase_url: PG_URL,
    clickhouse: { url: "https://ch.example.invalid:8443", user: "reader", password: "p", database: "relay_search" },
  });
  const buildLambda = (env: NodeJS.ProcessEnv) => readApiDepsFromSecret(LAMBDA_SECRET, { READ_API_COMPLETE_SINCE: "epoch", ...env }, undefined, { entry: "lambda" });

  test("the hosted Lambda entry wires neither unless the flag is exactly \"true\"", async () => {
    for (const value of [undefined, "", "1", "TRUE", "True", "yes", " true", "true "]) {
      const env = value === undefined ? {} : { READ_API_BRIEF_SUMMARY_ENABLED: value, READ_API_LIST_SHORT_SUMMARY_ENABLED: value };
      const built = await buildLambda(env);
      try {
        expect([value, built.deps.briefSummary, built.deps.listShortSummary]).toEqual([value, undefined, undefined]);
      } finally {
        await built.close();
      }
    }
  });

  test("\"true\" wires the brief-summary read on the hosted Lambda entry, and each flag is independent", async () => {
    const summary = await buildLambda({ READ_API_BRIEF_SUMMARY_ENABLED: "true" });
    try {
      expect(typeof summary.deps.briefSummary?.read).toBe("function");
      expect(typeof summary.deps.briefSummary?.activity).toBe("function");
      expect(summary.deps.listShortSummary).toBeUndefined();
    } finally {
      await summary.close();
    }
    const list = await buildLambda({ READ_API_LIST_SHORT_SUMMARY_ENABLED: "true" });
    try {
      expect(list.deps.listShortSummary).toBe(true);
      expect(list.deps.briefSummary).toBeUndefined();
    } finally {
      await list.close();
    }
  });

  test("the self-host service always wires the brief-summary read; the list opt-in still needs its flag", async () => {
    for (const value of [undefined, "false"]) {
      const built = await build({ ...ENV, ...(value === undefined ? {} : { READ_API_BRIEF_SUMMARY_ENABLED: value }) });
      try {
        expect(built.deps.briefSummary).toBeDefined();
        expect(built.deps.listShortSummary).toBeUndefined();
      } finally {
        await built.close();
      }
    }
    const built = await build({ ...ENV, READ_API_LIST_SHORT_SUMMARY_ENABLED: "true" });
    try {
      expect(built.deps.listShortSummary).toBe(true);
    } finally {
      await built.close();
    }
  });
});
