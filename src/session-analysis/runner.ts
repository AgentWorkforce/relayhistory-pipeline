/**
 * SERVICE=session-analysis (docker/entrypoint.sh): the session-analysis job processor as a
 * long-running container, so it can run on EKS next to ingest-edge instead of the session-pipeline
 * Lambda's `{"sessionAnalysis": true}` schedule. DEFAULT OFF. It runs the Lambda's exact path:
 * worker.ts analysisTick over deps from runtime-deps.ts analysisTickDeps (the builder the Lambda
 * uses too). Contract, IAM and chart values: docs/session-analysis-runner.md.
 *
 *   SESSION_ANALYSIS_ENABLED          exactly "true" runs; unset, "" or "false" is dormant (it reads only
 *                                     WORKER_HEALTH_PORT: no other setting, scrub engine, credential,
 *                                     database or store work; probes 200 enabled:false; SIGTERM exits 0);
 *                                     anything else refuses startup (exit 2)
 *   SESSION_ANALYSIS_TICK_MS          wait between ticks, 1000..600000, default 30000
 *   SESSION_ANALYSIS_BATCH            jobs per tick, 1..20 (no leading zero), default 2; refused here
 *                                     unless worker.ts analysisConfig reads the very same value
 *   SESSION_ANALYSIS_OPENAI_API_KEY_FILE | SESSION_ANALYSIS_OPENAI_API_KEY
 *                                     the model key, exactly one source (a mounted Secret file is the
 *                                     intended one); a key ending "-disabled" means no model (the tick
 *                                     claims nothing), as session-brief/config.ts modelKey
 *   SESSION_ANALYSIS_DATABASE_URL_FILE | SESSION_ANALYSIS_DATABASE_URL
 *                                     HoopDb as the worker role (service_role or the configured runtime
 *                                     role: migration 20261105046000's caller pin), exactly one source;
 *                                     TLS from SESSION_POSTGRES_TLS_PROFILE (postgres-tls.ts)
 *   SESSION_ANALYSIS_PG_POOL_MAX      1..16, default 4 (the Lambda's HOOPDB_POOL_MAX default)
 *   SESSION_ANALYSIS_S3_BUCKET        the pipeline (canonical) bucket
 *   SESSION_ANALYSIS_S3_REGION        that bucket's region (the signing region), required: never the
 *                                     cluster's AWS_REGION by default, which may differ
 *   SESSION_ANALYSIS_S3_CREDENTIALS   "podIdentity" (default; src/shared/locker.ts podIdentityCredentials)
 *                                     or "role" (AWS_ACCESS_KEY_ID/SECRET/SESSION_TOKEN from the env)
 *   PIPELINE_KEY_PREFIX, PIPELINE_ORG_FROM_PREFIX=true, READER_MAX_RULESET   as the Lambda reads them
 *   WORKER_HEALTH_PORT                probe port, default 8081
 *   SESSION_ANALYSIS_ASSUME_NO_LEGACY_TURNS, SESSION_ANALYSIS_CUTOVER_PROOF,
 *   SUMMARY_DECISION_RECORDS_ENABLED  passed through unchanged to the shared code that reads them
 *                                     (the first must stay unset; "true"/"false"/unset only)
 *   SESSION_INTEL_CLAIMS_SCHEDULER_ENABLED and its tuning (claims-scheduler.ts): the live claims scheduler side
 *                                     tick, only with every gate exactly "true" and a model configured; off, the
 *                                     tick is the analysis tick itself
 *
 * Probes: GET /healthz 200 {"ok":true,"enabled":true,"reason":"starting"|"ok"} while a tick ended (or
 * the service started) within tickMs + batch x lease + 60 s and fewer than 5 ticks failed in a row,
 * else 503 loop_stalled / tick_failing. GET /readyz 200 after the first good tick, 503 starting before.
 * SIGTERM/SIGINT: no further job is claimed; the job in flight finishes (its lease is 600 s; a pod
 * killed earlier leaves it to lease expiry, as a Lambda timeout does), then the pool closes and the
 * process exits 0. A second signal is logged and ignored (the drain goes on); the third exits 143 at
 * once. The scrub engine (SCRUB_ENGINE, READER_MAX_RULESET) is selected only once enabled, after the
 * config is validated; its failure is a refusal (exit 2). Logs: codes and counts only (src/shared/log.ts allowlist); no key, URL, bucket or id.
 */
import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { readFileSync, statSync } from "node:fs";
import { isAbsolute } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { createS3Store, type ObjectStore } from "../../packages/object-store/src/index.ts";
import { cachedProvider } from "../../packages/object-store/src/core.ts";
import type { AwsCredentials } from "../../packages/object-store/src/sigv4.ts";
import { readerMaxRuleset } from "../../packages/pipeline/src/lib/scrub-engine/reader-policy.ts";
import type { PagedEraser, S3EraserConfig } from "../migration/neon-s3-erasure/eraser.ts";
import { lambdaCredentials } from "../session-pipeline/aws-json.ts";
import { supabasePool, transaction } from "../session-pipeline/lambda.ts";
import { modelKey } from "../session-brief/config.ts";
import { podIdentityCredentials, trustedCredentialsUri } from "../shared/locker.ts";
import { postgresConnectionString, postgresTlsOptions } from "../shared/postgres-tls.ts";
import { sessionIntelControls } from "../shared/session-intel-controls.ts";
import { claimsSchedulerEnabled, claimsSchedulerSettings, claimsSchedulerTick, withClaimsScheduler } from "./claims-scheduler.ts";
import type { HoopDb } from "../shared/db.ts";
import { errorFields, log } from "../shared/log.ts";
import { initScrubEngine } from "../../packages/pipeline/src/lib/scrub-engine/index.ts";
import { analysisConfig, analysisTick } from "./worker.ts";
import { analysisTickDeps, type AnalysisTickDeps } from "./runtime-deps.ts";
import { ANALYSIS_LEASE_SECONDS } from "./processor.ts";

export const RUNNER_ENABLE_FLAG = "SESSION_ANALYSIS_ENABLED";
const STAGE = "session-analysis";
/** Consecutive failed ticks before /healthz turns 503 (the worker runtime's default). */
export const RUNNER_MAX_TICK_FAILURES = 5;
const STALL_MARGIN_MS = 60_000;

/** A refused configuration: exit 2, do not retry unchanged. `message` is a fixed code. */
export class AnalysisRunnerRefused extends Error {
  readonly code: string;
  constructor(code: string) {
    super(code);
    this.name = "AnalysisRunnerRefused";
    this.code = code;
  }
}
const refuse = (code: string): never => { throw new AnalysisRunnerRefused(code); };

/** Exactly "true" runs; unset, "" or "false" is dormant; anything else refuses (a typo never means either). */
export function analysisRunnerEnabled(env: NodeJS.ProcessEnv): boolean {
  const raw = env[RUNNER_ENABLE_FLAG];
  if (raw === undefined || raw === "" || raw === "false") return false;
  if (raw === "true") return true;
  return refuse("flag_invalid");
}

export type RunnerCredentialMode = "podIdentity" | "role";

export interface AnalysisRunnerConfig {
  tickMs: number;
  batch: number;
  poolMax: number;
  bucket: string;
  region: string;
  keyPrefix: string;
  readerMaxRuleset?: number;
  credentials: RunnerCredentialMode;
  /** SESSION_ANALYSIS_ASSUME_NO_LEGACY_TURNS=true was passed through (it must stay unset). */
  assumeNoLegacyTurns: boolean;
  /** SESSION_ANALYSIS_CUTOVER_PROOF is set (passed through; its value is never read here). */
  cutoverProof: boolean;
}

const int = (value: string | undefined, dflt: number, min: number, max: number, code: string): number => {
  const raw = value?.trim();
  if (raw === undefined || raw === "") return dflt;
  // No sign, no leading zero, no exponent: the decimal written is the value used.
  if (!/^[1-9]\d{0,8}$/.test(raw)) return refuse(code);
  const n = Number(raw);
  return n >= min && n <= max ? n : refuse(code);
};

export function healthPort(env: NodeJS.ProcessEnv): number {
  return int(env.WORKER_HEALTH_PORT?.trim(), 8081, 1, 65_535, "health_port_invalid");
}

/** Every setting of an enabled runner, validated before anything listens. Never reads a secret. */
export function analysisRunnerConfig(env: NodeJS.ProcessEnv): AnalysisRunnerConfig {
  const bucket = env.SESSION_ANALYSIS_S3_BUCKET ?? "";
  if (!/^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/.test(bucket)) refuse("bucket_invalid");
  const region = env.SESSION_ANALYSIS_S3_REGION ?? "";
  if (!/^[a-z]{2}(-[a-z]+)+-\d{1,2}$/.test(region)) refuse("region_invalid");
  if (env.PIPELINE_ORG_FROM_PREFIX !== "true") refuse("org_from_prefix_required");
  const keyPrefix = (env.PIPELINE_KEY_PREFIX ?? "").replace(/^\/+|\/+$/g, "");
  if (!/^[A-Za-z0-9][A-Za-z0-9._-]{0,62}$/.test(keyPrefix)) refuse("key_prefix_invalid");
  const credentials = env.SESSION_ANALYSIS_S3_CREDENTIALS || "podIdentity";
  if (credentials !== "podIdentity" && credentials !== "role") refuse("credentials_mode_invalid");
  const assume = env.SESSION_ANALYSIS_ASSUME_NO_LEGACY_TURNS;
  if (assume !== undefined && assume !== "" && assume !== "false" && assume !== "true") refuse("assume_no_legacy_turns_invalid");
  // Strict here, and it must be exactly what worker.ts analysisConfig runs (1-20, default 2): the
  // stall window and the startup line are sized from it.
  const batch = int(env.SESSION_ANALYSIS_BATCH, 2, 1, 20, "batch_invalid");
  if (analysisConfig(env).batch !== batch) refuse("batch_invalid");
  let maximum: number | undefined;
  try { maximum = readerMaxRuleset(env); } catch { refuse("reader_max_ruleset_invalid"); }
  // TLS is validated now (a bad profile or CA is a startup refusal, not a failed first tick).
  try { postgresTlsOptions(env); } catch { refuse("postgres_tls_invalid"); }
  return {
    tickMs: int(env.SESSION_ANALYSIS_TICK_MS, 30_000, 1_000, 600_000, "tick_ms_invalid"),
    batch,
    poolMax: int(env.SESSION_ANALYSIS_PG_POOL_MAX, 4, 1, 16, "pool_max_invalid"),
    bucket,
    region,
    keyPrefix,
    ...(maximum === undefined ? {} : { readerMaxRuleset: maximum }),
    credentials: credentials as RunnerCredentialMode,
    assumeNoLegacyTurns: assume === "true",
    cutoverProof: env.SESSION_ANALYSIS_CUTOVER_PROOF !== undefined && env.SESSION_ANALYSIS_CUTOVER_PROOF !== "",
  };
}

/**
 * One secret from exactly one of `<name>` or `<name>_FILE` (an absolute path to a regular file of
 * 1..65536 bytes; one trailing newline is dropped). Refusal codes name the variable, never its value.
 */
export function runnerSecret(env: NodeJS.ProcessEnv, name: string, code: string): string {
  const value = env[name], file = env[`${name}_FILE`];
  if (value !== undefined && value !== "" && file !== undefined && file !== "") refuse(`${code}_ambiguous`);
  let text = value ?? "";
  if (file !== undefined && file !== "") {
    if (!isAbsolute(file)) refuse(`${code}_invalid`);
    try {
      const st = statSync(file);
      if (!st.isFile() || st.size < 1 || st.size > 65_536) refuse(`${code}_invalid`);
      text = readFileSync(file, "utf8").replace(/\r?\n$/, "");
    } catch (e) {
      if (e instanceof AnalysisRunnerRefused) throw e;
      refuse(`${code}_unreadable`);
    }
  }
  if (!text) refuse(`${code}_missing`);
  // eslint-disable-next-line no-control-regex
  if (/[\u0000-\u001f\u007f\s]/.test(text)) refuse(`${code}_invalid`);
  return text;
}

export interface RunnerSecrets {
  databaseUrl: string;
  /** null: the key says "-disabled" (no model; the tick claims nothing). */
  modelKey: string | null;
}

export function runnerSecrets(env: NodeJS.ProcessEnv): RunnerSecrets {
  const databaseUrl = runnerSecret(env, "SESSION_ANALYSIS_DATABASE_URL", "database_url");
  try { postgresConnectionString(databaseUrl); } catch { refuse("database_url_invalid"); }
  const key = runnerSecret(env, "SESSION_ANALYSIS_OPENAI_API_KEY", "model_key");
  return { databaseUrl, modelKey: modelKey({ OPENAI_API_KEY: key }) };
}

/** The store's and eraser's credentials: Pod Identity (cached, refreshed 5 min before expiry) or the env role. */
export function runnerCredentials(env: NodeJS.ProcessEnv, mode: RunnerCredentialMode, fetcher: typeof fetch = fetch): () => Promise<AwsCredentials> {
  if (mode === "podIdentity") {
    const uri = env.AWS_CONTAINER_CREDENTIALS_FULL_URI;
    if (!uri) refuse("credentials_missing");
    if (!trustedCredentialsUri(uri!)) refuse("credentials_endpoint_untrusted");
    return cachedProvider(() => podIdentityCredentials(env, fetcher));
  }
  try { lambdaCredentials(env); } catch { refuse("credentials_missing"); }
  return async () => lambdaCredentials(env);
}

export interface RunnerPorts {
  /** Test-only ports; the process entry never takes them from input. */
  store?: (config: Parameters<typeof createS3Store>[0]) => ObjectStore;
  eraserFactory?: (config: S3EraserConfig) => PagedEraser;
  pool?: (url: string, max: number) => HoopDb;
  fetcher?: typeof fetch;
}

/** The runner's analysisTick deps: the Lambda's builder over this pod's pool, bucket and credentials. */
export function runnerAnalysisDeps(env: NodeJS.ProcessEnv, cfg: AnalysisRunnerConfig, secrets: RunnerSecrets, ports: RunnerPorts = {}):
  { deps: AnalysisTickDeps; db: HoopDb } {
  const credentials = runnerCredentials(env, cfg.credentials, ports.fetcher);
  const db = (ports.pool ?? ((url, max) => supabasePool(url, max, postgresTlsOptions(env))))(secrets.databaseUrl, cfg.poolMax);
  const store = (ports.store ?? createS3Store)({ bucket: cfg.bucket, region: cfg.region, credentials }) as unknown as ObjectStore;
  const deps = analysisTickDeps({
    db,
    transact: (work) => transaction(db, work),
    store,
    bucket: cfg.bucket,
    region: cfg.region,
    keyPrefix: cfg.keyPrefix,
    readerMaxRuleset: cfg.readerMaxRuleset,
    modelKey: secrets.modelKey,
    credentials,
    env,
    ...(ports.eraserFactory ? { eraserFactory: ports.eraserFactory } : {}),
  });
  return { deps, db };
}

export interface RunnerHealthState {
  enabled: boolean;
  /** Epoch ms of service start or the end of the last tick (good or failed). */
  lastProgressAt: number;
  ticks: number;
  consecutiveFailures: number;
}

export function runnerLiveness(s: RunnerHealthState, now: number, maxAgeMs: number) {
  if (!s.enabled) return { ok: true, enabled: false };
  if (s.consecutiveFailures >= RUNNER_MAX_TICK_FAILURES) return { ok: false, enabled: true, reason: "tick_failing" };
  if (now - s.lastProgressAt > maxAgeMs) return { ok: false, enabled: true, reason: "loop_stalled" };
  return { ok: true, enabled: true, reason: s.ticks > 0 ? "ok" : "starting" };
}

export function runnerReadiness(s: RunnerHealthState) {
  if (!s.enabled) return { ok: true, enabled: false };
  return s.ticks > 0 ? { ok: true, enabled: true } : { ok: false, enabled: true, reason: "starting" };
}

/** A tick may legitimately run `batch` jobs of up to one lease each before it reports progress. */
export function runnerMaxTickAgeMs(cfg: Pick<AnalysisRunnerConfig, "tickMs" | "batch">): number {
  return cfg.tickMs + cfg.batch * ANALYSIS_LEASE_SECONDS * 1000 + STALL_MARGIN_MS;
}

export function runnerHealthServer(s: RunnerHealthState, maxAgeMs: number, now: () => number = Date.now): Server {
  return createServer((req, res) => {
    const path = (req.url ?? "").split("?")[0];
    if (path !== "/healthz" && path !== "/readyz") return void res.writeHead(404).end();
    if (req.method !== "GET" && req.method !== "HEAD") return void res.writeHead(405, { allow: "GET" }).end();
    const v = path === "/healthz" ? runnerLiveness(s, now(), maxAgeMs) : runnerReadiness(s);
    res.writeHead(v.ok ? 200 : 503, { "content-type": "application/json" });
    res.end(req.method === "HEAD" ? undefined : JSON.stringify(v));
  });
}

export type AnalysisTickFn = () => Promise<Record<string, number>>;

/**
 * The loop: one tick, then wait `tickMs` (cut short by the signal), until the signal aborts. A tick
 * that throws is logged by code and counted; the loop goes on (liveness reports a failing streak).
 * A tick in flight when the signal aborts runs to its end (its in-flight job finishes; the tick
 * claims no further job because analysisTick checks `stopping` before each claim).
 */
export async function runAnalysisLoop(io: {
  tick: AnalysisTickFn;
  tickMs: number;
  signal: AbortSignal;
  state: RunnerHealthState;
  now?: () => number;
  sleep?: (ms: number, signal: AbortSignal) => Promise<void>;
}): Promise<void> {
  const now = io.now ?? Date.now;
  const sleep = io.sleep ?? (async (ms: number, signal: AbortSignal) => {
    try { await delay(ms, undefined, { signal }); } catch (error) { if (!signal.aborted) throw error; }
  });
  while (!io.signal.aborted) {
    const started = now();
    try {
      const counts = await io.tick();
      io.state.ticks += 1;
      io.state.consecutiveFailures = 0;
      log.info("session analysis tick", {
        stage: STAGE,
        count: counts.completed ?? 0,
        failed: counts.failed ?? 0,
        skipped: counts["lease-lost"] ?? 0,
        deleted: counts.swept ?? 0,
        retried: counts.sweep_failed ?? 0,
        duration_ms: now() - started,
      });
    } catch (error) {
      io.state.consecutiveFailures += 1;
      log.error("session analysis tick failed", { stage: STAGE, ...errorFields(error), count: io.state.consecutiveFailures, duration_ms: now() - started });
    }
    io.state.lastProgressAt = now();
    if (io.signal.aborted) break;
    await sleep(io.tickMs, io.signal);
  }
}

export interface AnalysisRunnerOptions {
  host?: string;
  port?: number;
  /** Aborting it stops the service as SIGTERM does (tests). */
  signal?: AbortSignal;
  now?: () => number;
  onListening?: (port: number) => void;
  ports?: RunnerPorts;
  /** Replaces the scrub engine selection (tests). Runs only when enabled, after config validation. */
  scrubInit?: () => Promise<unknown>;
  /** Replaces the hard exit on the third stop signal (tests). */
  hardExit?: (code: number) => void;
  /** Replaces the real tick built from the deps (tests). Receives the drain check. */
  tick?: (stopping: () => boolean) => AnalysisTickFn;
}

/** Resolves when the service stops cleanly; rejects (AnalysisRunnerRefused: exit 2) on a refused config. */
export async function analysisRunnerService(env: NodeJS.ProcessEnv = process.env, options: AnalysisRunnerOptions = {}):
  Promise<{ enabled: boolean; ticks: number }> {
  const now = options.now ?? Date.now;
  const port = healthPort(env);
  const enabled = analysisRunnerEnabled(env);
  // Enabled: every setting and both secrets are validated before anything listens.
  const cfg = enabled ? analysisRunnerConfig(env) : undefined;
  const secrets = enabled && !options.tick ? runnerSecrets(env) : undefined;
  // SCRUB_ENGINE selection, only for an enabled runner and after READER_MAX_RULESET was refused by code above.
  if (cfg) {
    try { await (options.scrubInit ?? initScrubEngine)(); } catch { refuse("scrub_engine_invalid"); }
  }
  const state: RunnerHealthState = { enabled, lastProgressAt: now(), ticks: 0, consecutiveFailures: 0 };
  const stop = new AbortController();
  // Every stop signal is handled, so a second one never kills the job in flight; the third exits now.
  let signals = 0;
  const onSignal = () => {
    signals += 1;
    if (signals === 2) log.warn("session analysis runner draining", { stage: STAGE, reason: "signal_repeated", count: signals });
    if (signals >= 3) {
      log.error("session analysis runner hard stop", { stage: STAGE, reason: "signal_repeated", count: signals });
      (options.hardExit ?? ((c: number) => process.exit(c)))(143);
    }
    stop.abort();
  };
  const onAbort = () => stop.abort();
  process.on("SIGTERM", onSignal);
  process.on("SIGINT", onSignal);
  if (options.signal) {
    if (options.signal.aborted) stop.abort();
    else options.signal.addEventListener("abort", onAbort, { once: true });
  }
  let db: HoopDb | undefined;
  const server = runnerHealthServer(state, cfg ? runnerMaxTickAgeMs(cfg) : 0, now);
  try {
    let tick: AnalysisTickFn | undefined;
    const stopping = () => stop.signal.aborted;
    if (cfg && options.tick) tick = options.tick(stopping);
    else if (cfg && secrets) {
      const built = runnerAnalysisDeps(env, cfg, secrets, options.ports);
      db = built.db;
      tick = runnerTick(env, analysisTick({ ...built.deps, stopping }), built.db, secrets.modelKey !== null, now);
    }
    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(options.port ?? port, options.host ?? "0.0.0.0", () => { server.off("error", reject); resolve(); });
    });
    const bound = (server.address() as AddressInfo).port;
    log.info("session analysis runner", {
      stage: STAGE,
      ok: enabled,
      port: bound,
      state: enabled ? "enabled" : "dormant",
      ...(cfg ? { status: secrets && !secrets.modelKey ? "model_not_configured" : "model_configured", count: cfg.batch } : {}),
      ...(cfg?.assumeNoLegacyTurns ? { reason: "assume_no_legacy_turns" } : {}),
    });
    options.onListening?.(bound);
    if (!tick || !cfg) {
      await new Promise<void>((resolve) => (stop.signal.aborted ? resolve() : stop.signal.addEventListener("abort", () => resolve(), { once: true })));
      return { enabled: false, ticks: 0 };
    }
    await runAnalysisLoop({ tick, tickMs: cfg.tickMs, signal: stop.signal, state, now });
    log.info("session analysis runner stopped", { stage: STAGE, ok: true, count: state.ticks });
    return { enabled: true, ticks: state.ticks };
  } finally {
    stop.abort();
    process.removeListener("SIGTERM", onSignal);
    process.removeListener("SIGINT", onSignal);
    options.signal?.removeEventListener("abort", onAbort);
    await new Promise<void>((resolve) => { server.close(() => resolve()); server.closeAllConnections?.(); });
    await db?.end?.().catch(() => {});
  }
}

/**
 * The runner's tick: the analysis tick, with the claims scheduler side tick (claims-scheduler.ts) around it
 * only when every gate is exactly "true" and a model is configured. Otherwise the analysis tick itself.
 */
export function runnerTick(env: NodeJS.ProcessEnv, tick: AnalysisTickFn, db: HoopDb, model: boolean, now: () => number = Date.now): AnalysisTickFn {
  if (!model || !claimsSchedulerEnabled(env)) return tick;
  const settings = claimsSchedulerSettings(env);
  const controls = sessionIntelControls(db);
  return withClaimsScheduler(tick, {
    run: () => claimsSchedulerTick({ db, controls, settings }),
    intervalMs: settings.intervalSeconds * 1000,
    now,
  });
}

/** 2: refused configuration (do not retry unchanged); 1: any other startup failure. */
export function runnerExitCode(error: unknown): 1 | 2 {
  return error instanceof AnalysisRunnerRefused ? 2 : 1;
}

if (import.meta.main)
  analysisRunnerService()
    .then(() => process.exit(0))
    .catch((error) => {
      log.error("session analysis runner refused", { stage: STAGE, ...errorFields(error), ok: false });
      process.exit(runnerExitCode(error));
    });
