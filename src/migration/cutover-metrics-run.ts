/**
 * Samples the cutover metrics, emits one allowlisted log line per point
 * (docs/cutover-observability.md) and sends alerts to nightcto's evidence webhook
 * when it is configured. Read-only on the database. `--once` takes one sample
 * and exits non-zero if a source failed, for an operator's terminal or a cron.
 *
 * Usage: CUTOVER_METRICS_DB_URL=... bun run cutover:metrics [--once] [--interval=60] [--window=300]
 *
 * The role reads through the `sessions.cutover_metrics_*` functions as a member
 * of `cutover_metrics_reader`. A role that could bypass RLS is refused.
 */
import pg from "pg";
import { required } from "../shared/env.ts";
import { errorFields, log } from "../shared/log.ts";
import {
  AlertTracker,
  evidenceFor,
  evidenceTarget,
  findProblems,
  sendEvidence,
  type Alert,
  type EvidenceTarget,
} from "./cutover-alerts.ts";
import {
  assertSamplerRole,
  DEFAULT_WINDOW_SECONDS,
  MAX_WINDOW_SECONDS,
  readOnlyQuery,
  readPipelineMetrics,
  type ReadOnlyQuery,
} from "./cutover-metrics.ts";

const MIN_INTERVAL_SECONDS = 15;
const DEFAULT_INTERVAL_SECONDS = 60;

export interface AlertSink {
  target: EvidenceTarget;
  tracker: AlertTracker;
  fetcher?: typeof fetch;
}

async function deliver(sink: AlertSink, alert: Alert, state: "firing" | "recovered", now: Date): Promise<boolean> {
  const evidence = evidenceFor(alert, state, {
    environment: process.env.DEPLOYMENT ?? "selfhost",
    version: process.env.DEPLOYMENT_ID ?? "local",
    now,
  });
  const status = await sendEvidence(sink.target, evidence, sink.fetcher);
  const delivered = status >= 200 && status < 300;
  const fields = { kind: "cutover_alert", rule: alert.rule, stage: alert.subject, state, status };
  if (delivered) log.info("cutover alert sent", fields);
  else log.error("cutover alert not delivered", fields);
  return delivered;
}

/** Returns whether every source was read and every alert accepted. */
export async function emitPipelineMetrics(
  query: ReadOnlyQuery,
  windowSeconds = DEFAULT_WINDOW_SECONDS,
  sink?: AlertSink,
  now = new Date(),
): Promise<boolean> {
  const { points, failures } = await readPipelineMetrics(query, windowSeconds);
  points.push({ metric: "cutover.alerts.configured", value: sink ? 1 : 0, unit: "1" });
  for (const point of points) log.info("cutover metric", { ...point });
  for (const { kind, error } of failures) log.error("cutover metric read failed", { kind, ...errorFields(error) });
  let delivered = true;
  if (sink) {
    const { firing, recovered } = sink.tracker.update(findProblems(points), points);
    for (const alert of firing) delivered = (await deliver(sink, alert, "firing", now)) && delivered;
    for (const alert of recovered) delivered = (await deliver(sink, alert, "recovered", now)) && delivered;
  }
  return failures.length === 0 && delivered;
}

export function parseArgs(argv: string[]) {
  const option = (name: string, fallback: number) => {
    const raw = argv.find((arg) => arg.startsWith(`--${name}=`))?.slice(name.length + 3);
    if (raw === undefined) return fallback;
    if (!/^\d+$/.test(raw)) throw new Error(`--${name} must be a whole number of seconds`);
    return Number(raw);
  };
  const once = argv.includes("--once");
  const intervalSeconds = option("interval", DEFAULT_INTERVAL_SECONDS);
  const windowSeconds = option("window", DEFAULT_WINDOW_SECONDS);
  if (intervalSeconds < MIN_INTERVAL_SECONDS) throw new Error(`--interval must be at least ${MIN_INTERVAL_SECONDS}`);
  if (windowSeconds < 1 || windowSeconds > MAX_WINDOW_SECONDS) throw new Error(`--window must be 1 to ${MAX_WINDOW_SECONDS}`);
  return { once, intervalSeconds, windowSeconds };
}

async function main() {
  const { once, intervalSeconds, windowSeconds } = parseArgs(process.argv.slice(2));
  process.env.OTEL_SERVICE_NAME ||= "cutover-metrics";
  const target = evidenceTarget(process.env);
  if (!target) log.warn("cutover alerts not configured", { kind: "cutover_alert", reason: "no_evidence_target" });
  const sink = target ? { target, tracker: new AlertTracker() } : undefined;
  const pool = new pg.Pool({ connectionString: required("CUTOVER_METRICS_DB_URL"), max: 1, application_name: "cutover-metrics" });
  pool.on("error", (err) => log.error("cutover metrics pool error", errorFields(err)));
  const query = readOnlyQuery(() => pool.connect() as never);
  await assertSamplerRole(query);
  if (once) {
    const ok = await emitPipelineMetrics(query, windowSeconds, sink);
    await pool.end();
    process.exit(ok ? 0 : 1);
  }
  process.on("SIGTERM", () => process.exit(0));
  process.on("SIGINT", () => process.exit(0));
  for (;;) {
    const started = Date.now();
    await emitPipelineMetrics(query, windowSeconds, sink).catch((err) => log.error("cutover metric read failed", errorFields(err)));
    const wait = intervalSeconds * 1000 - (Date.now() - started);
    await new Promise((resolve) => setTimeout(resolve, Math.max(0, wait)));
  }
}

if (import.meta.main)
  main().catch((err) => {
    log.error("cutover metrics failed", errorFields(err));
    process.exit(1);
  });
