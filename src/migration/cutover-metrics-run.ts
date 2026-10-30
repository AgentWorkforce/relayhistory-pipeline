/**
 * Samples the cutover metrics and emits one allowlisted log line per point
 * (docs/cutover-observability.md). Read-only. `--once` takes one sample and exits
 * non-zero if a source failed, for an operator's terminal or a cron.
 *
 * Usage: CUTOVER_METRICS_DB_URL=... bun run cutover:metrics [--once] [--interval=60] [--window=300]
 *
 * The role needs SELECT on the three tables and must bypass RLS (`service_role`):
 * they have RLS on and no policy, so any other role reads zero rows and every
 * metric looks healthy.
 */
import pg from "pg";
import { required } from "../shared/env.ts";
import { errorFields, log } from "../shared/log.ts";
import {
  DEFAULT_WINDOW_SECONDS,
  readOnlyQuery,
  readPipelineMetrics,
  type ReadOnlyQuery,
} from "./cutover-metrics.ts";

const MIN_INTERVAL_SECONDS = 15;
const DEFAULT_INTERVAL_SECONDS = 60;

/** Returns whether every source was read. */
export async function emitPipelineMetrics(query: ReadOnlyQuery, windowSeconds = DEFAULT_WINDOW_SECONDS): Promise<boolean> {
  const { points, failures } = await readPipelineMetrics(query, windowSeconds);
  for (const point of points) log.info("cutover metric", { ...point });
  for (const { kind, error } of failures) log.error("cutover metric read failed", { kind, ...errorFields(error) });
  return failures.length === 0;
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
  if (windowSeconds < 1) throw new Error("--window must be at least 1");
  return { once, intervalSeconds, windowSeconds };
}

async function main() {
  const { once, intervalSeconds, windowSeconds } = parseArgs(process.argv.slice(2));
  process.env.OTEL_SERVICE_NAME ||= "cutover-metrics";
  const pool = new pg.Pool({ connectionString: required("CUTOVER_METRICS_DB_URL"), max: 1, application_name: "cutover-metrics" });
  pool.on("error", (err) => log.error("cutover metrics pool error", errorFields(err)));
  const query = readOnlyQuery(() => pool.connect() as never);
  if (once) {
    const ok = await emitPipelineMetrics(query, windowSeconds);
    await pool.end();
    process.exit(ok ? 0 : 1);
  }
  for (;;) {
    const started = Date.now();
    await emitPipelineMetrics(query, windowSeconds).catch((err) => log.error("cutover metric read failed", errorFields(err)));
    const wait = intervalSeconds * 1000 - (Date.now() - started);
    await new Promise((resolve) => setTimeout(resolve, Math.max(0, wait)));
  }
}

if (import.meta.main)
  main().catch((err) => {
    log.error("cutover metrics failed", errorFields(err));
    process.exit(1);
  });
