/**
 * The self-host service's Output / digest backend selection (lambda.ts factsBackends and
 * readApiDepsFromSecret): READ_API_OUTPUT_BACKEND / READ_API_DIGEST_BACKEND=postgres with the
 * route's ENABLED flag ("1") wires the Postgres facts backends on the read pool, with no
 * ClickHouse; anything else wires nothing new; an unknown value refuses startup without echoing
 * it, and the hosted Lambda refuses postgres. The same for READ_API_SUMMARY_SEARCH_BACKEND (the
 * summary search store). No database is touched: pg.Pool is a recorder.
 */
import { afterEach, beforeEach, describe, expect, spyOn, test } from "bun:test";
import pg from "pg";
import { TENANT_SETTINGS_SQL } from "../liveness.ts";
import { createReadApiLambdaHandler, factsBackends, type FunctionUrlEvent, READER_LOGIN_SQL, READER_PG16_SQL, readApiDepsFromSecret, summarySearchBackend } from "../lambda.ts";
import { parseSummarySearchBackend } from "../config.ts";
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

describe("list readiness flag (READ_API_LIST_READINESS_ENABLED)", () => {
  const LAMBDA_SECRET = JSON.stringify({
    supabase_url: PG_URL,
    clickhouse: { url: "https://ch.example.invalid:8443", user: "reader", password: "p", database: "relay_search" },
  });
  const buildLambda = (env: NodeJS.ProcessEnv) => readApiDepsFromSecret(LAMBDA_SECRET, { READ_API_COMPLETE_SINCE: "epoch", ...env }, undefined, { entry: "lambda" });

  test("the hosted Lambda and the self-host service wire listReadiness only for the exact \"true\", independent of the short-summary flag", async () => {
    for (const value of [undefined, "", "1", "TRUE", "True", "yes", " true", "true ", "false"]) {
      const env = value === undefined ? {} : { READ_API_LIST_READINESS_ENABLED: value };
      for (const built of [await buildLambda(env), await build({ ...ENV, ...env })]) {
        try {
          expect([value, built.deps.listReadiness]).toEqual([value, undefined]);
        } finally {
          await built.close();
        }
      }
    }
    for (const built of [await buildLambda({ READ_API_LIST_READINESS_ENABLED: "true" }), await build({ ...ENV, READ_API_LIST_READINESS_ENABLED: "true" })]) {
      try {
        expect(built.deps.listReadiness).toBe(true);
        expect(built.deps.listShortSummary).toBeUndefined();
      } finally {
        await built.close();
      }
    }
    const short = await build({ ...ENV, READ_API_LIST_SHORT_SUMMARY_ENABLED: "true" });
    try {
      expect(short.deps.listReadiness).toBeUndefined();
    } finally {
      await short.close();
    }
  });
});

describe("coordination advisories wiring", () => {
  test("only the exact \"true\" wires the advisories source; the ack opens READ WRITE on the read pool", async () => {
    for (const flag of [undefined, "", "false", "1", "TRUE"]) {
      const off = await build({ ...ENV, ...(flag === undefined ? {} : { READ_API_COORDINATION_ADVISORIES_ENABLED: flag }) });
      try {
        expect([flag, off.deps.coordinationAdvisories]).toEqual([flag, undefined]);
      } finally {
        await off.close();
      }
    }
    const on = await build({ ...ENV, READ_API_COORDINATION_ADVISORIES_ENABLED: "true" });
    try {
      expect(on.deps.coordinationAdvisories?.enabled).toBe(true);
      sql = [];
      const recipient = { orgId: "org-a", workspaceId: "ws-a", source: "codex", sessionId: "s-1" };
      expect(await on.deps.coordinationAdvisories!.source.pending(recipient)).toEqual([]);
      expect(sql.map((s) => s.text.split("(")[0]!.trim())).toEqual([
        "BEGIN READ ONLY", "SELECT set_config", expect.stringContaining("SELECT advisory_id"), "COMMIT"]);
      sql = [];
      expect(await on.deps.coordinationAdvisories!.source.ack(recipient, "9f3c2a71d04be6e5")).toBe(false);
      expect(sql.map((s) => s.text.split("(")[0]!.trim())).toEqual([
        "BEGIN READ WRITE", "SELECT set_config", "SELECT sessions.coordination_advisories_ack_v1", "COMMIT"]);
    } finally {
      await on.close();
    }
  });
});

describe("relayfacts flag (READ_API_RELAYFACTS_ENABLED)", () => {
  const LAMBDA_SECRET = JSON.stringify({
    supabase_url: PG_URL,
    clickhouse: { url: "https://ch.example.invalid:8443", user: "reader", password: "p", database: "relay_search" },
  });
  const buildLambda = (env: NodeJS.ProcessEnv) => readApiDepsFromSecret(LAMBDA_SECRET, { READ_API_COMPLETE_SINCE: "epoch", ...env }, undefined, { entry: "lambda" });

  test("neither entry wires it unless the flag is exactly \"true\"", async () => {
    for (const value of [undefined, "", "1", "TRUE", "True", "yes", " true", "true "]) {
      const env = value === undefined ? {} : { READ_API_RELAYFACTS_ENABLED: value };
      for (const built of [await buildLambda(env), await build({ ...ENV, ...env })]) {
        try {
          expect([value, built.deps.relayfacts]).toEqual([value, undefined]);
        } finally {
          await built.close();
        }
      }
    }
  });

  test("\"true\" wires the two reads on the hosted Lambda entry and the self-host service", async () => {
    for (const built of [await buildLambda({ READ_API_RELAYFACTS_ENABLED: "true" }), await build({ ...ENV, READ_API_RELAYFACTS_ENABLED: "true" })]) {
      try {
        expect(typeof built.deps.relayfacts?.topics).toBe("function");
        expect(typeof built.deps.relayfacts?.list).toBe("function");
      } finally {
        await built.close();
      }
    }
  });
});

describe("advisory notify allowlist wiring (READ_API_ADVISORY_NOTIFY_ROWS)", () => {
  const LAMBDA_SECRET = JSON.stringify({
    supabase_url: PG_URL,
    clickhouse: { url: "https://ch.example.invalid:8443", user: "reader", password: "p", database: "relay_search" },
  });
  const buildLambda = (env: NodeJS.ProcessEnv) => readApiDepsFromSecret(LAMBDA_SECRET, { READ_API_COMPLETE_SINCE: "epoch", ...env }, undefined, { entry: "lambda" });

  test("unset or empty wires nothing (all false); a list is parsed on both entries; a malformed list fails startup naming the entry only", async () => {
    for (const env of [{}, { READ_API_ADVISORY_NOTIFY_ROWS: "" }, { READ_API_ADVISORY_NOTIFY_ROWS: "  " }])
      for (const built of [await buildLambda(env), await build({ ...ENV, ...env })]) {
        try { expect(built.deps.advisoryNotifyRows).toBeUndefined(); } finally { await built.close(); }
      }
    for (const built of [await buildLambda({ READ_API_ADVISORY_NOTIFY_ROWS: "duplicate_work:high" }), await build({ ...ENV, READ_API_ADVISORY_NOTIFY_ROWS: "duplicate_work:high" })]) {
      try { expect([...built.deps.advisoryNotifyRows!]).toEqual(["duplicate_work:high"]); } finally { await built.close(); }
    }
    for (const make of [buildLambda, (env: NodeJS.ProcessEnv) => build({ ...ENV, ...env })]) {
      const error = await make({ READ_API_ADVISORY_NOTIFY_ROWS: "duplicate_work:high,nope" }).then(() => null, (e: Error) => e);
      expect(error?.message).toBe("READ_API_ADVISORY_NOTIFY_ROWS: entry 2 is not <kind>:<urgency>");
    }
  });
});

describe("session intelligence workspace wiring (READ_API_SESSION_INTEL_ENABLED)", () => {
  const LAMBDA_SECRET = JSON.stringify({
    supabase_url: PG_URL,
    clickhouse: { url: "https://ch.example.invalid:8443", user: "reader", password: "p", database: "relay_search" },
  });
  const buildLambda = (env: NodeJS.ProcessEnv) => readApiDepsFromSecret(LAMBDA_SECRET, { READ_API_COMPLETE_SINCE: "epoch", ...env }, undefined, { entry: "lambda" });

  test("neither entry wires it unless the flag is exactly \"true\"; on, both wire the SQL source", async () => {
    for (const value of [undefined, "", "1", "TRUE", "yes", " true"]) {
      const env = value === undefined ? {} : { READ_API_SESSION_INTEL_ENABLED: value };
      for (const built of [await buildLambda(env), await build({ ...ENV, ...env })]) {
        try {
          expect([value, built.deps.sessionIntel]).toEqual([value, undefined]);
        } finally {
          await built.close();
        }
      }
    }
    for (const built of [await buildLambda({ READ_API_SESSION_INTEL_ENABLED: "true" }), await build({ ...ENV, READ_API_SESSION_INTEL_ENABLED: "true" })]) {
      try {
        expect(built.deps.sessionIntel?.enabled).toBe(true);
        expect(typeof built.deps.sessionIntel?.source.alerts).toBe("function");
        expect(typeof built.deps.sessionIntel?.source.claims).toBe("function");
      } finally {
        await built.close();
      }
    }
  });
});

describe("workspace synthesis wiring (READ_API_WORKSPACE_SYNTHESIS_ENABLED)", () => {
  const LAMBDA_SECRET = JSON.stringify({
    supabase_url: PG_URL,
    clickhouse: { url: "https://ch.example.invalid:8443", user: "reader", password: "p", database: "relay_search" },
  });
  const buildLambda = (env: NodeJS.ProcessEnv) => readApiDepsFromSecret(LAMBDA_SECRET, { READ_API_COMPLETE_SINCE: "epoch", ...env }, undefined, { entry: "lambda" });

  test("off unless exactly \"true\"; on without READ_API_ANALYSIS_BUCKET and PIPELINE_KEY_PREFIX it has no source; with both it reads the bucket", async () => {
    for (const value of [undefined, "", "1", "TRUE", " true"]) {
      const built = await buildLambda(value === undefined ? {} : { READ_API_WORKSPACE_SYNTHESIS_ENABLED: value });
      try { expect([value, built.deps.workspaceSynthesis]).toEqual([value, undefined]); } finally { await built.close(); }
    }
    const bare = await buildLambda({ READ_API_WORKSPACE_SYNTHESIS_ENABLED: "true" });
    try { expect(bare.deps.workspaceSynthesis).toEqual({ enabled: true }); } finally { await bare.close(); }
    // A bucket without the key root the job writes under is not configured either.
    const noRoot = await buildLambda({ READ_API_WORKSPACE_SYNTHESIS_ENABLED: "true", READ_API_ANALYSIS_BUCKET: "fx-bucket", AWS_REGION: "us-east-1" });
    try { expect(noRoot.deps.workspaceSynthesis).toEqual({ enabled: true }); } finally { await noRoot.close(); }
    const bucket = await buildLambda({ READ_API_WORKSPACE_SYNTHESIS_ENABLED: "true", READ_API_ANALYSIS_BUCKET: "fx-bucket", AWS_REGION: "us-east-1", PIPELINE_KEY_PREFIX: "fxroot" });
    try {
      expect(bucket.deps.workspaceSynthesis?.enabled).toBe(true);
      expect(typeof bucket.deps.workspaceSynthesis?.source?.latest).toBe("function");
    } finally { await bucket.close(); }
  });
});

describe("summary search backend selector (READ_API_SUMMARY_SEARCH_BACKEND)", () => {
  const LAMBDA_SECRET = JSON.stringify({
    supabase_url: PG_URL,
    clickhouse: { url: "https://ch.example.invalid:8443", user: "reader", password: "p", database: "relay_search" },
  });
  const buildLambda = (env: NodeJS.ProcessEnv) => readApiDepsFromSecret(LAMBDA_SECRET, { READ_API_COMPLETE_SINCE: "epoch", ...env }, undefined, { entry: "lambda" });

  test("unset, empty or clickhouse = clickhouse; postgres = postgres; anything else throws without echoing the value", () => {
    expect(parseSummarySearchBackend({})).toBe("clickhouse");
    expect(parseSummarySearchBackend({ READ_API_SUMMARY_SEARCH_BACKEND: "" })).toBe("clickhouse");
    expect(parseSummarySearchBackend({ READ_API_SUMMARY_SEARCH_BACKEND: "clickhouse" })).toBe("clickhouse");
    expect(parseSummarySearchBackend({ READ_API_SUMMARY_SEARCH_BACKEND: "postgres" })).toBe("postgres");
    for (const value of ["Postgres", " postgres", "postgres ", "pg", "clickhouse ", "neon", "1", "synthetic-value-x"]) {
      let error: Error | null = null;
      try { parseSummarySearchBackend({ READ_API_SUMMARY_SEARCH_BACKEND: value }); } catch (e) { error = e as Error; }
      expect(error?.message).toBe('session read api: READ_API_SUMMARY_SEARCH_BACKEND must be unset, "", "clickhouse" or "postgres"');
    }
  });

  test("the service wires the Postgres backend only for postgres; the default wires nothing new", async () => {
    for (const value of [undefined, "", "clickhouse"]) {
      const built = await build({ ...ENV, READ_API_SUMMARY_SEARCH_ENABLED: "true", ...(value === undefined ? {} : { READ_API_SUMMARY_SEARCH_BACKEND: value }) });
      try {
        expect([value, built.deps.summarySearch, built.deps.summarySearchBackend]).toEqual([value, true, undefined]);
      } finally {
        await built.close();
      }
    }
    const pg = await build({ ...ENV, READ_API_SUMMARY_SEARCH_ENABLED: "true", READ_API_SUMMARY_SEARCH_BACKEND: "postgres" });
    try {
      expect([pg.deps.summarySearch, pg.deps.summarySearchBackend, pg.deps.ch]).toEqual([true, "postgres", undefined]);
    } finally {
      await pg.close();
    }
    // The selector does not turn the route on: READ_API_SUMMARY_SEARCH_ENABLED stays required.
    const off = await build({ ...ENV, READ_API_SUMMARY_SEARCH_BACKEND: "postgres" });
    try {
      expect(off.deps.summarySearch).toBeUndefined();
    } finally {
      await off.close();
    }
  });

  test("an unknown value refuses service startup, naming the flag and never echoing the value", async () => {
    for (const value of ["Postgres", "pg", "clickhouse ", "neon"]) {
      const error = await build({ ...ENV, READ_API_SUMMARY_SEARCH_BACKEND: value }).then(() => null, (e: Error) => e);
      expect(error?.message).toContain("READ_API_SUMMARY_SEARCH_BACKEND");
      expect(error?.message).not.toContain(value);
    }
  });

  test("the hosted Lambda entry refuses postgres; clickhouse and unset pass unchanged", async () => {
    expect(() => summarySearchBackend({ READ_API_SUMMARY_SEARCH_BACKEND: "postgres" }, "lambda")).toThrow("served only by the self-host service");
    expect(summarySearchBackend({ READ_API_SUMMARY_SEARCH_BACKEND: "postgres" }, "service")).toBe("postgres");
    expect(summarySearchBackend({}, "lambda")).toBe("clickhouse");
    const error = await buildLambda({ READ_API_SUMMARY_SEARCH_ENABLED: "true", READ_API_SUMMARY_SEARCH_BACKEND: "postgres" }).then(() => null, (e: Error) => e);
    expect(error?.message).toBe("session read api: READ_API_SUMMARY_SEARCH_BACKEND=postgres is served only by the self-host service");
    for (const value of [undefined, "", "clickhouse"]) {
      const built = await buildLambda({ READ_API_SUMMARY_SEARCH_ENABLED: "true", ...(value === undefined ? {} : { READ_API_SUMMARY_SEARCH_BACKEND: value }) });
      try {
        expect([value, built.deps.summarySearch, built.deps.summarySearchBackend, Boolean(built.deps.ch)]).toEqual([value, true, undefined, true]);
      } finally {
        await built.close();
      }
    }
  });
});

describe("the hosted Lambda handler refuses READ_API_SUMMARY_SEARCH_BACKEND=postgres before the secret lookup", () => {
  // buildApp (reached through the exported handler factory) checks the selector before it asks Secrets Manager.
  // fetch is stubbed for the whole test, so nothing leaves the process; the unset run proves the stub sees the
  // lookup, so zero calls with "postgres" means the refusal came first. Synthetic values only.
  const VARS: Record<string, string> = {
    AWS_REGION: "us-east-1", SESSION_READ_API_SECRET_ARN: "arn:aws:secretsmanager:us-east-1:000000000000:secret:synthetic",
    AWS_ACCESS_KEY_ID: "synthetic-access-key", AWS_SECRET_ACCESS_KEY: "synthetic-secret-key", READ_API_COMPLETE_SINCE: "epoch",
    READ_API_SUMMARY_SEARCH_ENABLED: "true",
  };
  const touched = [...Object.keys(VARS), "AWS_SESSION_TOKEN", "READ_API_SUMMARY_SEARCH_BACKEND"];
  let saved: Record<string, string | undefined> = {};
  beforeEach(() => {
    saved = Object.fromEntries(touched.map((k) => [k, process.env[k]]));
    for (const k of touched) delete process.env[k];
    Object.assign(process.env, VARS);
  });
  afterEach(() => {
    for (const k of touched) {
      if (saved[k] === undefined) delete process.env[k];
      else process.env[k] = saved[k];
    }
    (globalThis.fetch as { mockRestore?: () => void }).mockRestore?.();
  });

  test("postgres: 503 init failure with no secret lookup; unset: the lookup is attempted", async () => {
    const urls: string[] = [];
    spyOn(globalThis, "fetch").mockImplementation((async (input: Parameters<typeof fetch>[0]) => {
      urls.push(String(input instanceof Request ? input.url : input));
      throw new Error("no network in this test");
    }) as typeof fetch);
    const event = {} as FunctionUrlEvent;
    process.env.READ_API_SUMMARY_SEARCH_BACKEND = "postgres";
    expect((await createReadApiLambdaHandler()(event)).statusCode).toBe(503);
    expect(urls).toEqual([]);
    delete process.env.READ_API_SUMMARY_SEARCH_BACKEND;
    expect((await createReadApiLambdaHandler()(event)).statusCode).toBe(503);
    expect(urls).toHaveLength(1);
    expect(urls[0]).toContain("secretsmanager");
  });
});
