/**
 * SERVICE=session-analysis (runner.ts): config fails closed, the loop's cadence and drain, the
 * probes, deps parity with the session-pipeline Lambda's builder, and no secret in any log line.
 */
import { afterEach, beforeEach, expect, test } from "bun:test";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { zstdCompressSync } from "node:zlib";
import {
  AnalysisRunnerRefused,
  analysisRunnerConfig,
  analysisRunnerEnabled,
  analysisRunnerService,
  runAnalysisLoop,
  runnerAnalysisDeps,
  runnerCredentials,
  runnerExitCode,
  runnerLiveness,
  runnerMaxTickAgeMs,
  runnerReadiness,
  runnerSecret,
  runnerSecrets,
  RUNNER_MAX_TICK_FAILURES,
  type RunnerHealthState,
} from "../runner.ts";
import { lambdaAnalysisDeps } from "../../session-pipeline/lambda.ts";
import { analysisTick } from "../worker.ts";
import type { HoopDb } from "../../shared/db.ts";
import type { ObjectStore } from "../../../packages/object-store/src/index.ts";
import type { S3EraserConfig, PagedEraser } from "../../migration/neon-s3-erasure/eraser.ts";

const MODEL_KEY = "sk-test-RUNNERSECRETKEY0123456789";
const DB_PASSWORD = "DBPASSWORDsecret987";
const AWS_SECRET = "AWSSECRETaccesskey555";
const BUCKET = "example-pipeline-bucket";
const DB_HOST = "db.example.test";

let dir = "";
beforeEach(() => { dir = mkdtempSync(join(tmpdir(), "analysis-runner-")); });
afterEach(() => rmSync(dir, { recursive: true, force: true }));

const code = (fn: () => unknown): string => {
  try { fn(); } catch (e) { if (e instanceof AnalysisRunnerRefused) return e.code; throw e; }
  return "no_refusal";
};

function baseEnv(extra: Record<string, string | undefined> = {}): NodeJS.ProcessEnv {
  return {
    SESSION_ANALYSIS_ENABLED: "true",
    SESSION_ANALYSIS_S3_BUCKET: BUCKET,
    SESSION_ANALYSIS_S3_REGION: "us-east-1",
    PIPELINE_ORG_FROM_PREFIX: "true",
    PIPELINE_KEY_PREFIX: "/96bulls/",
    ...extra,
  } as NodeJS.ProcessEnv;
}

// ---- config: fail closed ----------------------------------------------------------------------

test("the flag is exactly \"true\"; unset, empty and \"false\" are dormant; anything else refuses", () => {
  expect(analysisRunnerEnabled({})).toBe(false);
  expect(analysisRunnerEnabled({ SESSION_ANALYSIS_ENABLED: "" })).toBe(false);
  expect(analysisRunnerEnabled({ SESSION_ANALYSIS_ENABLED: "false" })).toBe(false);
  expect(analysisRunnerEnabled({ SESSION_ANALYSIS_ENABLED: "true" })).toBe(true);
  for (const v of ["TRUE", "1", "yes", " true", "true ", "on"])
    expect(code(() => analysisRunnerEnabled({ SESSION_ANALYSIS_ENABLED: v }))).toBe("flag_invalid");
});

test("an enabled config has the documented defaults and the Lambda's key prefix normalisation", () => {
  const cfg = analysisRunnerConfig(baseEnv());
  expect(cfg).toEqual({
    tickMs: 30_000, batch: 2, poolMax: 4, bucket: BUCKET, region: "us-east-1", keyPrefix: "96bulls",
    credentials: "podIdentity", assumeNoLegacyTurns: false, cutoverProof: false,
  });
  const set = analysisRunnerConfig(baseEnv({
    SESSION_ANALYSIS_TICK_MS: "1000", SESSION_ANALYSIS_BATCH: "20", SESSION_ANALYSIS_PG_POOL_MAX: "1",
    SESSION_ANALYSIS_S3_CREDENTIALS: "role", READER_MAX_RULESET: "9", SESSION_ANALYSIS_CUTOVER_PROOF: "anything",
  }));
  expect(set).toMatchObject({ tickMs: 1000, batch: 20, poolMax: 1, credentials: "role", readerMaxRuleset: 9, cutoverProof: true });
});

test("every malformed setting refuses with a code (no silent default)", () => {
  const cases: [Record<string, string | undefined>, string][] = [
    [{ SESSION_ANALYSIS_S3_BUCKET: undefined }, "bucket_invalid"],
    [{ SESSION_ANALYSIS_S3_BUCKET: "Bad_Bucket" }, "bucket_invalid"],
    [{ SESSION_ANALYSIS_S3_REGION: undefined }, "region_invalid"],
    [{ SESSION_ANALYSIS_S3_REGION: "east" }, "region_invalid"],
    [{ PIPELINE_ORG_FROM_PREFIX: undefined }, "org_from_prefix_required"],
    [{ PIPELINE_ORG_FROM_PREFIX: "1" }, "org_from_prefix_required"],
    [{ PIPELINE_KEY_PREFIX: undefined }, "key_prefix_invalid"],
    [{ PIPELINE_KEY_PREFIX: "a/b" }, "key_prefix_invalid"],
    [{ SESSION_ANALYSIS_TICK_MS: "999" }, "tick_ms_invalid"],
    [{ SESSION_ANALYSIS_TICK_MS: "600001" }, "tick_ms_invalid"],
    [{ SESSION_ANALYSIS_TICK_MS: "30s" }, "tick_ms_invalid"],
    [{ SESSION_ANALYSIS_TICK_MS: "-5" }, "tick_ms_invalid"],
    [{ SESSION_ANALYSIS_BATCH: "0" }, "batch_invalid"],
    [{ SESSION_ANALYSIS_BATCH: "21" }, "batch_invalid"],
    [{ SESSION_ANALYSIS_BATCH: "two" }, "batch_invalid"],
    // A leading zero: analysisConfig would run 2 while a looser parser logged and sized for 5.
    [{ SESSION_ANALYSIS_BATCH: "005" }, "batch_invalid"],
    [{ SESSION_ANALYSIS_BATCH: "05" }, "batch_invalid"],
    [{ SESSION_ANALYSIS_TICK_MS: "030000" }, "tick_ms_invalid"],
    [{ SESSION_ANALYSIS_TICK_MS: "3e4" }, "tick_ms_invalid"],
    [{ SESSION_ANALYSIS_PG_POOL_MAX: "04" }, "pool_max_invalid"],
    [{ SESSION_ANALYSIS_PG_POOL_MAX: "17" }, "pool_max_invalid"],
    [{ SESSION_ANALYSIS_S3_CREDENTIALS: "irsa" }, "credentials_mode_invalid"],
    [{ SESSION_ANALYSIS_ASSUME_NO_LEGACY_TURNS: "yes" }, "assume_no_legacy_turns_invalid"],
    [{ READER_MAX_RULESET: "7" }, "reader_max_ruleset_invalid"],
    [{ SESSION_POSTGRES_TLS_PROFILE: "none" }, "postgres_tls_invalid"],
    [{ WORKER_HEALTH_PORT: "70000" }, "no_refusal"], // the port is the service's, checked there
  ];
  for (const [extra, want] of cases) expect([extra, code(() => analysisRunnerConfig(baseEnv(extra)))]).toEqual([extra, want]);
});

test("BATCH, TICK_MS and POOL_MAX are trimmed like the port, and BATCH is what analysisConfig runs", async () => {
  const { analysisConfig } = await import("../worker.ts");
  for (const raw of ["1", " 7 ", "20", "\t12"]) {
    const env = baseEnv({ SESSION_ANALYSIS_BATCH: raw });
    expect(analysisRunnerConfig(env).batch).toBe(analysisConfig(env).batch);
  }
  expect(analysisRunnerConfig(baseEnv({ SESSION_ANALYSIS_TICK_MS: " 5000 ", SESSION_ANALYSIS_PG_POOL_MAX: " 3 " }))).toMatchObject({ tickMs: 5000, poolMax: 3 });
});

test("SESSION_ANALYSIS_ASSUME_NO_LEGACY_TURNS is passed through, never defaulted on", () => {
  expect(analysisRunnerConfig(baseEnv()).assumeNoLegacyTurns).toBe(false);
  expect(analysisRunnerConfig(baseEnv({ SESSION_ANALYSIS_ASSUME_NO_LEGACY_TURNS: "false" })).assumeNoLegacyTurns).toBe(false);
  expect(analysisRunnerConfig(baseEnv({ SESSION_ANALYSIS_ASSUME_NO_LEGACY_TURNS: "true" })).assumeNoLegacyTurns).toBe(true);
});

test("secrets: exactly one source, absolute regular file, no whitespace; codes never carry the value", () => {
  const file = join(dir, "key");
  writeFileSync(file, `${MODEL_KEY}\n`);
  expect(runnerSecret({ X_FILE: file }, "X", "model_key")).toBe(MODEL_KEY);
  expect(runnerSecret({ X: MODEL_KEY }, "X", "model_key")).toBe(MODEL_KEY);
  expect(code(() => runnerSecret({}, "X", "model_key"))).toBe("model_key_missing");
  expect(code(() => runnerSecret({ X: MODEL_KEY, X_FILE: file }, "X", "model_key"))).toBe("model_key_ambiguous");
  expect(code(() => runnerSecret({ X_FILE: "relative/key" }, "X", "model_key"))).toBe("model_key_invalid");
  expect(code(() => runnerSecret({ X_FILE: join(dir, "absent") }, "X", "model_key"))).toBe("model_key_unreadable");
  expect(code(() => runnerSecret({ X_FILE: dir }, "X", "model_key"))).toBe("model_key_invalid");
  expect(code(() => runnerSecret({ X: `${MODEL_KEY} extra` }, "X", "model_key"))).toBe("model_key_invalid");
  writeFileSync(join(dir, "empty"), "");
  expect(code(() => runnerSecret({ X_FILE: join(dir, "empty") }, "X", "model_key"))).toBe("model_key_invalid");
  let thrown: unknown;
  try { runnerSecret({ X: `${MODEL_KEY} x` }, "X", "model_key"); } catch (e) { thrown = e; }
  expect(thrown).toBeInstanceOf(AnalysisRunnerRefused);
  expect(String((thrown as Error).message)).toBe("model_key_invalid");
  expect(String((thrown as Error).message)).not.toContain(MODEL_KEY);
});

test("runnerSecrets: both secrets required; a -disabled key is no model; a non-postgres URL refuses without echoing it", () => {
  const url = `postgresql://worker:${DB_PASSWORD}@${DB_HOST}:5432/postgres`;
  expect(runnerSecrets({ SESSION_ANALYSIS_DATABASE_URL: url, SESSION_ANALYSIS_OPENAI_API_KEY: MODEL_KEY })).toEqual({ databaseUrl: url, modelKey: MODEL_KEY });
  expect(runnerSecrets({ SESSION_ANALYSIS_DATABASE_URL: url, SESSION_ANALYSIS_OPENAI_API_KEY: "sk-disabled" }).modelKey).toBeNull();
  expect(code(() => runnerSecrets({ SESSION_ANALYSIS_DATABASE_URL: url }))).toBe("model_key_missing");
  expect(code(() => runnerSecrets({ SESSION_ANALYSIS_OPENAI_API_KEY: MODEL_KEY }))).toBe("database_url_missing");
  const bad = `mysql://worker:${DB_PASSWORD}@${DB_HOST}/x`;
  let message = "";
  try { runnerSecrets({ SESSION_ANALYSIS_DATABASE_URL: bad, SESSION_ANALYSIS_OPENAI_API_KEY: MODEL_KEY }); } catch (e) { message = String((e as Error).message); }
  expect(message).toBe("database_url_invalid");
});

test("Pod Identity credentials need a trusted container endpoint; role mode needs env keys", () => {
  expect(code(() => runnerCredentials({}, "podIdentity"))).toBe("credentials_missing");
  expect(code(() => runnerCredentials({ AWS_CONTAINER_CREDENTIALS_FULL_URI: "http://credentials.example.test/v1" }, "podIdentity"))).toBe("credentials_endpoint_untrusted");
  expect(code(() => runnerCredentials({ AWS_CONTAINER_CREDENTIALS_FULL_URI: "http://169.254.170.23/v1/credentials" }, "podIdentity"))).toBe("no_refusal");
  expect(code(() => runnerCredentials({}, "role"))).toBe("credentials_missing");
  expect(runnerExitCode(new AnalysisRunnerRefused("x"))).toBe(2);
  expect(runnerExitCode(new Error("boom"))).toBe(1);
});

test("Pod Identity credentials are fetched once and cached for the store and the eraser", async () => {
  let calls = 0;
  const fetcher = (async () => {
    calls += 1;
    return new Response(JSON.stringify({ AccessKeyId: "AKIDPOD", SecretAccessKey: AWS_SECRET, Token: "tok", Expiration: new Date(Date.now() + 3_600_000).toISOString() }));
  }) as unknown as typeof fetch;
  const creds = runnerCredentials({ AWS_CONTAINER_CREDENTIALS_FULL_URI: "http://169.254.170.23/v1/credentials" }, "podIdentity", fetcher);
  expect((await creds()).accessKeyId).toBe("AKIDPOD");
  await creds();
  expect(calls).toBe(1);
});

// ---- the loop ---------------------------------------------------------------------------------

const freshState = (): RunnerHealthState => ({ enabled: true, lastProgressAt: 0, ticks: 0, consecutiveFailures: 0 });

test("cadence: one tick, then a wait of tickMs, until the signal aborts", async () => {
  const stop = new AbortController();
  const waits: number[] = [];
  let ticks = 0;
  const state = freshState();
  await runAnalysisLoop({
    tickMs: 30_000,
    signal: stop.signal,
    state,
    tick: async () => { ticks += 1; return { completed: 1, failed: 0, "lease-lost": 0, idle: 1 }; },
    sleep: async (ms) => { waits.push(ms); if (waits.length === 3) stop.abort(); },
  });
  expect(ticks).toBe(3);
  expect(waits).toEqual([30_000, 30_000, 30_000]);
  expect(state.ticks).toBe(3);
});

test("a failing tick is counted and the loop goes on; liveness reports the streak; a good tick resets it", async () => {
  const stop = new AbortController();
  const state = freshState();
  let n = 0;
  await runAnalysisLoop({
    tickMs: 1000,
    signal: stop.signal,
    state,
    tick: async () => { n += 1; if (n <= RUNNER_MAX_TICK_FAILURES) throw Object.assign(new Error(`text ${MODEL_KEY}`), { code: "ECONNREFUSED" }); return {}; },
    sleep: async () => {
      if (n === RUNNER_MAX_TICK_FAILURES) expect(runnerLiveness(state, state.lastProgressAt, 10_000)).toEqual({ ok: false, enabled: true, reason: "tick_failing" });
      if (n === RUNNER_MAX_TICK_FAILURES + 1) stop.abort();
    },
  });
  expect(state.consecutiveFailures).toBe(0);
  expect(state.ticks).toBe(1);
});

test("liveness and readiness verdicts", () => {
  const s = freshState();
  s.lastProgressAt = 1000;
  expect(runnerLiveness(s, 1000, 5000)).toEqual({ ok: true, enabled: true, reason: "starting" });
  expect(runnerReadiness(s)).toEqual({ ok: false, enabled: true, reason: "starting" });
  s.ticks = 1;
  expect(runnerLiveness(s, 1000, 5000)).toEqual({ ok: true, enabled: true, reason: "ok" });
  expect(runnerReadiness(s)).toEqual({ ok: true, enabled: true });
  expect(runnerLiveness(s, 7000, 5000)).toEqual({ ok: false, enabled: true, reason: "loop_stalled" });
  expect(runnerLiveness({ ...s, enabled: false }, 1e12, 0)).toEqual({ ok: true, enabled: false });
  // A tick may run `batch` jobs of one 600 s lease each.
  expect(runnerMaxTickAgeMs({ tickMs: 30_000, batch: 2 })).toBe(30_000 + 2 * 600_000 + 60_000);
});

test("analysisTick claims no job once `stopping` is true; absent (the Lambda), it claims", async () => {
  const run = async (stopping?: () => boolean) => {
    let queries = 0;
    const db = { query: async () => { queries += 1; return { rows: [] }; } };
    const tick = analysisTick({
      config: { enabled: true, batch: 3, leaseSeconds: 600, assumeNoLegacyTurns: false },
      db,
      store: {} as ObjectStore,
      canonical: {} as never,
      transport: { complete: async () => { throw new Error("unused"); } } as never,
      ...(stopping ? { stopping } : {}),
    });
    const counts = await tick();
    return { queries, counts };
  };
  const stopped = await run(() => true);
  expect(stopped.queries).toBe(0);
  expect(stopped.counts.idle).toBe(0);
  const lambda = await run();
  expect(lambda.queries).toBeGreaterThan(0);
  expect(lambda.counts.idle).toBe(1);
});

// ---- the service ------------------------------------------------------------------------------

async function get(port: number, path: string) {
  const res = await fetch(`http://127.0.0.1:${port}${path}`);
  return { status: res.status, body: await res.json().catch(() => null) };
}

test("dormant: no other setting or secret is read, probes say enabled:false, stop resolves", async () => {
  const stop = new AbortController();
  let port = 0;
  const done = analysisRunnerService({}, { host: "127.0.0.1", port: 0, signal: stop.signal, onListening: (p) => { port = p; } });
  while (!port) await Bun.sleep(5);
  expect(await get(port, "/healthz")).toEqual({ status: 200, body: { ok: true, enabled: false } });
  expect(await get(port, "/readyz")).toEqual({ status: 200, body: { ok: true, enabled: false } });
  expect((await get(port, "/other")).status).toBe(404);
  stop.abort();
  expect(await done).toEqual({ enabled: false, ticks: 0 });
});

test("a refused config rejects before anything listens", async () => {
  await expect(analysisRunnerService({ SESSION_ANALYSIS_ENABLED: "yes" }, { port: 0 })).rejects.toBeInstanceOf(AnalysisRunnerRefused);
  await expect(analysisRunnerService(baseEnv({ SESSION_ANALYSIS_S3_REGION: undefined }), { port: 0 })).rejects.toThrow("region_invalid");
  // Enabled with no secrets at all: refused (never an enabled runner without a key and a database).
  await expect(analysisRunnerService(baseEnv(), { port: 0 })).rejects.toThrow("database_url_missing");
});

test("the scrub engine is selected only when enabled, after config; its failure is a refusal (exit 2)", async () => {
  let inits = 0;
  const scrubInit = async () => { inits += 1; throw new Error("bad selector"); };
  // Dormant with a broken scrub selector and READER_MAX_RULESET: still dormant, nothing initialised.
  const stop = new AbortController();
  let port = 0;
  const dormant = analysisRunnerService({ READER_MAX_RULESET: "99", SCRUB_ENGINE: "nonsense" }, { host: "127.0.0.1", port: 0, signal: stop.signal, scrubInit, onListening: (p) => { port = p; } });
  while (!port) await Bun.sleep(5);
  stop.abort();
  expect(await dormant).toEqual({ enabled: false, ticks: 0 });
  expect(inits).toBe(0);
  // Enabled with a bad READER_MAX_RULESET: its own refusal fires first, before any engine selection.
  const tick = () => async () => ({});
  await expect(analysisRunnerService(baseEnv({ READER_MAX_RULESET: "99" }), { port: 0, scrubInit, tick })).rejects.toThrow("reader_max_ruleset_invalid");
  expect(inits).toBe(0);
  // Enabled with a valid config: an engine failure is a refusal, so exit 2, not 1.
  let refused: unknown;
  try { await analysisRunnerService(baseEnv(), { port: 0, scrubInit, tick }); } catch (e) { refused = e; }
  expect(refused).toBeInstanceOf(AnalysisRunnerRefused);
  expect((refused as AnalysisRunnerRefused).code).toBe("scrub_engine_invalid");
  expect(runnerExitCode(refused)).toBe(2);
  expect(inits).toBe(1);
});

test("a second stop signal is ignored while draining; the third exits 143", async () => {
  let release!: () => void, port = 0, started = 0;
  const exits: number[] = [];
  const done = analysisRunnerService(baseEnv({ SESSION_ANALYSIS_TICK_MS: "1000" }), {
    host: "127.0.0.1", port: 0, onListening: (p) => { port = p; }, hardExit: (c) => { exits.push(c); },
    tick: () => async () => { started += 1; await new Promise<void>((r) => { release = r; }); return {}; },
  });
  while (!port || !started) await Bun.sleep(5);
  process.emit("SIGTERM");
  process.emit("SIGINT");
  await Bun.sleep(10);
  expect(exits).toEqual([]);
  process.emit("SIGTERM");
  expect(exits).toEqual([143]);
  release();
  expect(await done).toEqual({ enabled: true, ticks: 1 });
});

test("SIGTERM drains: the tick in flight finishes, no further job or tick starts, the service exits clean", async () => {
  let started = 0, finished = 0, release!: () => void;
  let sawStopping = false;
  let port = 0;
  const done = analysisRunnerService(baseEnv({ SESSION_ANALYSIS_TICK_MS: "1000" }), {
    host: "127.0.0.1",
    port: 0,
    onListening: (p) => { port = p; },
    tick: (stopping) => async () => {
      started += 1;
      await new Promise<void>((r) => { release = r; });
      // analysisTick checks this before each claim: the next job would not be claimed.
      sawStopping = stopping();
      finished += 1;
      return { completed: 1 };
    },
  });
  while (!port || !started) await Bun.sleep(5);
  expect((await get(port, "/readyz")).status).toBe(503);
  process.emit("SIGTERM");
  await Bun.sleep(20);
  expect(finished).toBe(0); // still draining
  release();
  expect(await done).toEqual({ enabled: true, ticks: 1 });
  expect({ started, finished, sawStopping }).toEqual({ started: 1, finished: 1, sawStopping: true });
});

// ---- deps parity with the Lambda --------------------------------------------------------------

const fakeDb = (): HoopDb => ({ pool: {} as never, query: async () => ({ rows: [] }), end: async () => {} }) as unknown as HoopDb;

test("deps parity: the runner and the Lambda build identical analysisTick deps from one builder", async () => {
  const saved = { ...process.env };
  try {
    for (const flags of [{}, { SUMMARY_DECISION_RECORDS_ENABLED: "true", SESSION_ANALYSIS_BATCH: "5", SESSION_ANALYSIS_ASSUME_NO_LEGACY_TURNS: "true" }]) {
      // The Lambda reads process.env; the runner is handed the same env.
      const env = baseEnv({ ...flags, READER_MAX_RULESET: "9", SESSION_ANALYSIS_S3_CREDENTIALS: "role", AWS_ACCESS_KEY_ID: "AKIDROLE", AWS_SECRET_ACCESS_KEY: AWS_SECRET });
      for (const k of Object.keys(process.env)) if (k.startsWith("SESSION_ANALYSIS_") || k === "SUMMARY_DECISION_RECORDS_ENABLED") delete process.env[k];
      Object.assign(process.env, env);
      const db = fakeDb();
      const store = { marker: "store" } as unknown as ObjectStore;
      const cfg = analysisRunnerConfig(env);
      const erasers: Record<"runner" | "lambda", S3EraserConfig[]> = { runner: [], lambda: [] };
      const factory = (side: "runner" | "lambda") => (c: S3EraserConfig): PagedEraser => {
        erasers[side].push(c);
        return { eraseStep: async () => ({ side }) as never, verifyErased: async () => ({ side }) as never };
      };
      const runner = runnerAnalysisDeps(env, cfg, { databaseUrl: "postgresql://unused", modelKey: MODEL_KEY }, { pool: () => db, store: () => store, eraserFactory: factory("runner") }).deps;
      const lambda = lambdaAnalysisDeps({ bucket: cfg.bucket, region: cfg.region, keyPrefix: cfg.keyPrefix, readerMaxRuleset: cfg.readerMaxRuleset }, db, store, MODEL_KEY, { eraserFactory: factory("lambda") });
      expect(Object.keys(runner).sort()).toEqual(Object.keys(lambda).sort());
      expect(runner.config).toEqual(lambda.config);
      expect(runner.db).toBe(lambda.db);
      expect(runner.store).toBe(lambda.store);
      const { decompress: rd, ...rc } = runner.canonical;
      const { decompress: ld, ...lc } = lambda.canonical;
      expect(rc).toEqual(lc);
      expect(rc).toEqual({ store, provider: "s3", bucket: BUCKET, prefix: { PIPELINE_ORG_FROM_PREFIX: "true", PIPELINE_KEY_PREFIX: "96bulls" }, readerMaxRuleset: 9 });
      const sample = new Uint8Array(zstdCompressSync(Buffer.from("evidence bytes")));
      expect(Buffer.from(rd!(sample, 1024)).toString()).toBe(Buffer.from(ld!(sample, 1024)).toString());
      expect(runner.transport === null).toBe(lambda.transport === null);
      expect(runner.transport).not.toBeNull();
      expect("decisionRecords" in runner).toBe("decisionRecords" in lambda);
      expect("decisionRecords" in runner).toBe("SUMMARY_DECISION_RECORDS_ENABLED" in flags);
      expect(Object.keys(runner.eraser!).sort()).toEqual(Object.keys(lambda.eraser!).sort());
      // Both sides build the sweeper's eraser for the same bucket, region and credentials, on every call.
      for (const deps of [runner, lambda]) {
        await deps.eraser!.eraseStep("96bulls/org", {} as never);
        await deps.eraser!.verifyErased("96bulls/org", {} as never);
      }
      expect(erasers.runner).toHaveLength(2);
      expect(erasers.lambda).toHaveLength(2);
      expect(erasers.runner).toEqual(erasers.lambda);
      expect(erasers.runner[0]).toEqual({ bucket: BUCKET, region: "us-east-1", credentials: { accessKeyId: "AKIDROLE", secretAccessKey: AWS_SECRET } });
      // The transports carry the same key to the same request (the vendored engine's one URL).
      const sent: { side: string; auth: string | null; url: string }[] = [];
      const realFetch = globalThis.fetch;
      for (const [side, deps] of [["runner", runner], ["lambda", lambda]] as const) {
        globalThis.fetch = (async (input: string | URL | Request, init?: RequestInit) => {
          sent.push({ side, auth: new Headers(init?.headers).get("authorization"), url: String(input instanceof Request ? input.url : input) });
          return new Response("{}", { status: 500 });
        }) as typeof fetch;
        try { await deps.transport!.complete({ model: "m", instructions: "i", input: "x", timeoutMs: 1000 } as never).catch(() => {}); }
        finally { globalThis.fetch = realFetch; }
      }
      expect(sent).toHaveLength(2);
      expect(sent[0]!.auth).toContain(MODEL_KEY);
      expect(sent[0]!.auth).toBe(sent[1]!.auth);
      expect(sent[0]!.url).toBe(sent[1]!.url);
    }
  } finally {
    for (const k of Object.keys(process.env)) if (!(k in saved)) delete process.env[k];
    Object.assign(process.env, saved);
  }
});

test("no model key on either side means no transport (the tick claims nothing)", () => {
  const env = baseEnv({ SESSION_ANALYSIS_S3_CREDENTIALS: "role", AWS_ACCESS_KEY_ID: "AKIDROLE", AWS_SECRET_ACCESS_KEY: AWS_SECRET });
  const db = fakeDb();
  const cfg = analysisRunnerConfig(env);
  expect(runnerAnalysisDeps(env, cfg, { databaseUrl: "postgresql://unused", modelKey: null }, { pool: () => db, store: () => ({}) as ObjectStore }).deps.transport).toBeNull();
  expect(lambdaAnalysisDeps(cfg, db, {} as ObjectStore, null).transport).toBeNull();
});

test("the sweeper's eraser signs for the pipeline bucket in ITS region with the pod's current credentials", async () => {
  const seen: S3EraserConfig[] = [];
  const eraserFactory = (c: S3EraserConfig): PagedEraser => {
    seen.push(c);
    return { eraseStep: async () => ({}) as never, verifyErased: async () => ({}) as never };
  };
  const fetcher = (async () => new Response(JSON.stringify({ AccessKeyId: "AKIDPOD", SecretAccessKey: AWS_SECRET, Token: "t", Expiration: new Date(Date.now() + 3_600_000).toISOString() }))) as unknown as typeof fetch;
  const env = baseEnv({ AWS_REGION: "us-west-2", AWS_CONTAINER_CREDENTIALS_FULL_URI: "http://169.254.170.23/v1/credentials" });
  const cfg = analysisRunnerConfig(env);
  const { deps } = runnerAnalysisDeps(env, cfg, { databaseUrl: "postgresql://unused", modelKey: MODEL_KEY }, { pool: fakeDb, store: () => ({}) as ObjectStore, eraserFactory, fetcher });
  await deps.eraser!.eraseStep("96bulls/org/x", {} as never);
  expect(seen).toHaveLength(1);
  expect(seen[0]).toMatchObject({ bucket: BUCKET, region: "us-east-1", credentials: { accessKeyId: "AKIDPOD" } });
});

// ---- no secret in logs ------------------------------------------------------------------------

test("no secret, URL, bucket or host in any line the runner writes, on a failing real tick", async () => {
  const keyFile = join(dir, "key"), urlFile = join(dir, "url");
  writeFileSync(keyFile, MODEL_KEY);
  writeFileSync(urlFile, `postgresql://worker:${DB_PASSWORD}@${DB_HOST}:5432/postgres`);
  const env = baseEnv({
    SESSION_ANALYSIS_TICK_MS: "1000",
    SESSION_ANALYSIS_OPENAI_API_KEY_FILE: keyFile,
    SESSION_ANALYSIS_DATABASE_URL_FILE: urlFile,
    SESSION_ANALYSIS_S3_CREDENTIALS: "role",
    AWS_ACCESS_KEY_ID: "AKIDROLE",
    AWS_SECRET_ACCESS_KEY: AWS_SECRET,
  });
  const lines: string[] = [];
  const out = process.stdout.write.bind(process.stdout), err = process.stderr.write.bind(process.stderr);
  const capture = (chunk: unknown) => { lines.push(String(chunk)); return true; };
  process.stdout.write = capture as typeof process.stdout.write;
  process.stderr.write = capture as typeof process.stderr.write;
  const failing = () => Promise.reject(Object.assign(new Error(`connect ${DB_HOST} ${DB_PASSWORD}`), { code: "ECONNREFUSED" }));
  const db = { pool: { connect: failing }, query: failing, end: async () => {} } as unknown as HoopDb;
  const stop = new AbortController();
  try {
    const done = analysisRunnerService(env, { host: "127.0.0.1", port: 0, signal: stop.signal, ports: { pool: () => db, store: () => ({}) as ObjectStore } });
    while (!lines.some((l) => l.includes("session analysis tick"))) await Bun.sleep(5);
    stop.abort();
    await done;
  } finally {
    process.stdout.write = out;
    process.stderr.write = err;
  }
  const text = lines.join("");
  expect(text).toContain("session analysis tick failed");
  expect(text).toContain("ECONNREFUSED");
  for (const secret of [MODEL_KEY, DB_PASSWORD, AWS_SECRET, DB_HOST, BUCKET, keyFile, urlFile]) expect(text).not.toContain(secret);
});
