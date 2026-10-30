/**
 * Cutover metrics derivable from the Supabase `sessions` schema
 * (docs/cutover-observability.md): stage lag and queue depth from `pipeline_jobs`,
 * DLQ depth from `pipeline_dead_letters` and `pipeline_jobs`, stage outcomes from
 * `pipeline_events`.
 *
 * Every statement is a SELECT in its own READ ONLY transaction under a statement
 * timeout, so this cannot write and a slow read cannot pile up on the database.
 * The role reads through SECURITY DEFINER aggregates and never bypasses RLS. A
 * failed or RLS-blind read yields no points for that source plus
 * `cutover.metrics.read_ok = 0`: a zero must always mean zero, never "unknown".
 *
 * This module does no logging, so PGlite tests can import it. The CLI that emits
 * the points through the log allowlist is `cutover-metrics-run.ts`.
 */
import { QUEUED_STAGES } from "../../packages/pipeline/src/lib/pipeline/registry.js";

export interface Queryable {
  query<T = Record<string, unknown>>(sql: string, params?: unknown[]): Promise<{ rows: T[] }>;
}
/** One connection for one transaction: `pool.connect()`, or a PGlite instance in tests. */
export type Connect = () => Promise<Queryable & { release?(err?: Error): void }>;
export type ReadOnlyQuery = Queryable["query"];

export const STATEMENT_TIMEOUT_MS = 5_000;
export const DEFAULT_WINDOW_SECONDS = 300;
export const MAX_WINDOW_SECONDS = 3600;

export function readOnlyQuery(connect: Connect, timeoutMs = STATEMENT_TIMEOUT_MS): ReadOnlyQuery {
  return async <T>(sql: string, params?: unknown[]) => {
    const client = await connect();
    let failure: Error | undefined;
    try {
      await client.query("BEGIN READ ONLY");
      await client.query(`SET LOCAL statement_timeout = ${Math.floor(timeoutMs)}`);
      const result = await client.query<T>(sql, params);
      await client.query("COMMIT");
      return result;
    } catch (err) {
      failure = err as Error;
      await client.query("ROLLBACK").catch(() => {});
      throw err;
    } finally {
      client.release?.(failure);
    }
  };
}

/** The reads, all SECURITY DEFINER aggregates: supabase/migrations/20261030000100_cutover_metrics.sql. */
export const JOBS_SQL = "SELECT * FROM sessions.cutover_metrics_jobs()";
export const DEAD_LETTERS_SQL = "SELECT * FROM sessions.cutover_metrics_dead_letters()";
export const EVENTS_SQL = "SELECT * FROM sessions.cutover_metrics_events($1::integer)";
export const ERRORS_SQL = "SELECT * FROM sessions.cutover_metrics_errors($1::integer)";

/** Refuses a role that could read past RLS: the sampler gets aggregates, never the tables. */
export async function assertSamplerRole(query: ReadOnlyQuery): Promise<void> {
  const { rows } = await query<{ bypass: boolean }>(
    "SELECT rolbypassrls OR rolsuper AS bypass FROM pg_roles WHERE rolname = current_user",
  );
  if (rows[0]?.bypass !== false) throw Object.assign(new Error("sampler role must not bypass RLS"), { code: "sampler_role_bypasses_rls" });
}

export interface MetricPoint {
  metric: string;
  value: number;
  unit: "1" | "s" | "ms";
  stage?: string;
  status?: string;
  state?: string;
  error_code?: string;
  kind?: string;
}

type Num = string | number | null;
const num = (value: Num) => Math.round(Number(value ?? 0) * 1000) / 1000;

async function jobPoints(query: ReadOnlyQuery): Promise<MetricPoint[]> {
  const { rows } = await query<{
    stage: string; depth: Num; ready: Num; leased: Num; dlq_pending: Num; lag_seconds: Num; oldest_seconds: Num;
  }>(JOBS_SQL);
  const byStage = new Map(rows.map((row) => [row.stage, row]));
  // Every stage, every time: an empty queue has no row, and it must read 0, not absent.
  return QUEUED_STAGES.flatMap((stage) => {
    const row = byStage.get(stage);
    return [
      { metric: "pipeline.stage.queue_depth", value: num(row?.depth ?? 0), unit: "1", stage },
      { metric: "pipeline.stage.ready_depth", value: num(row?.ready ?? 0), unit: "1", stage },
      { metric: "pipeline.stage.leased_depth", value: num(row?.leased ?? 0), unit: "1", stage },
      { metric: "pipeline.stage.lag_seconds", value: num(row?.lag_seconds ?? 0), unit: "s", stage },
      { metric: "pipeline.stage.oldest_job_age_seconds", value: num(row?.oldest_seconds ?? 0), unit: "s", stage },
      { metric: "pipeline.dlq.depth", value: num(row?.dlq_pending ?? 0), unit: "1", stage, state: "pending" },
    ] satisfies MetricPoint[];
  });
}

async function deadLetterPoints(query: ReadOnlyQuery): Promise<MetricPoint[]> {
  const { rows } = await query<{ stage: string; state: string; depth: Num; oldest_seconds: Num }>(DEAD_LETTERS_SQL);
  const found = new Map(rows.map((row) => [`${row.stage}:${row.state}`, row]));
  const stages = new Set<string>([...QUEUED_STAGES, ...rows.map((row) => row.stage)]);
  const points: MetricPoint[] = [];
  for (const stage of stages) {
    const letters = ["dead", "replaying"].map((state) => found.get(`${stage}:${state}`));
    for (const [index, state] of ["dead", "replaying"].entries())
      points.push({ metric: "pipeline.dlq.depth", value: num(letters[index]?.depth ?? 0), unit: "1", stage, state });
    const ages = letters.flatMap((row) => (row ? [num(row.oldest_seconds)] : []));
    points.push({ metric: "pipeline.dlq.oldest_age_seconds", value: Math.max(0, ...ages), unit: "s", stage });
  }
  return points;
}

async function eventPoints(query: ReadOnlyQuery, windowSeconds: number): Promise<MetricPoint[]> {
  const { rows } = await query<{ stage: string; status: string; events: Num; p95_ms: Num; last_seconds: Num }>(EVENTS_SQL, [windowSeconds]);
  return rows.flatMap((row) => {
    const labels = { stage: row.stage, status: row.status };
    return [
      { metric: "pipeline.stage.events", value: num(row.events), unit: "1", ...labels },
      ...(row.p95_ms === null ? [] : [{ metric: "pipeline.stage.duration_p95_ms", value: num(row.p95_ms), unit: "ms", ...labels } as const]),
      { metric: "pipeline.stage.last_event_age_seconds", value: num(row.last_seconds), unit: "s", ...labels },
    ] satisfies MetricPoint[];
  });
}

async function errorPoints(query: ReadOnlyQuery, windowSeconds: number): Promise<MetricPoint[]> {
  const { rows } = await query<{ stage: string; status: string; code: string; events: Num }>(ERRORS_SQL, [windowSeconds]);
  return rows.map((row) => ({
    metric: "pipeline.stage.errors",
    value: num(row.events),
    unit: "1",
    stage: row.stage,
    status: row.status,
    error_code: row.code,
  }));
}

export interface PipelineMetricsRead {
  points: MetricPoint[];
  /** The database error of each failed source; the caller logs its code, never its text. */
  failures: { kind: string; error: unknown }[];
}

/** One sample. Sources fail independently, so a slow `pipeline_events` scan never hides the DLQ. */
export async function readPipelineMetrics(
  query: ReadOnlyQuery,
  windowSeconds = DEFAULT_WINDOW_SECONDS,
): Promise<PipelineMetricsRead> {
  if (!Number.isInteger(windowSeconds) || windowSeconds < 1 || windowSeconds > MAX_WINDOW_SECONDS) throw new Error(`window must be 1 to ${MAX_WINDOW_SECONDS} seconds`);
  const sources: [string, () => Promise<MetricPoint[]>][] = [
    ["jobs", () => jobPoints(query)],
    ["dead_letters", () => deadLetterPoints(query)],
    ["events", () => eventPoints(query, windowSeconds)],
    ["errors", () => errorPoints(query, windowSeconds)],
  ];
  const points: MetricPoint[] = [{ metric: "cutover.metrics.window_seconds", value: windowSeconds, unit: "s" }];
  const failures: PipelineMetricsRead["failures"] = [];
  for (const [kind, read] of sources) {
    try {
      points.push(...(await read()));
      points.push({ metric: "cutover.metrics.read_ok", value: 1, unit: "1", kind });
    } catch (error) {
      failures.push({ kind, error });
      points.push({ metric: "cutover.metrics.read_ok", value: 0, unit: "1", kind });
    }
  }
  return { points, failures };
}
