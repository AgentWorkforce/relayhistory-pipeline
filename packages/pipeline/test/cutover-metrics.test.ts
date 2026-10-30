/**
 * The cutover metrics against the real `sessions` schema on PGlite, read by a
 * SELECT-only role, like the operator's read-only connection.
 */
import { PGlite } from "@electric-sql/pglite";
import { readdirSync, readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import {
  readOnlyQuery,
  readPipelineMetrics,
  type MetricPoint,
} from "../../../src/migration/cutover-metrics.js";
import { QUEUED_STAGES } from "../src/lib/pipeline/registry.js";

const SUPABASE = new URL("../../../supabase/", import.meta.url).href;
const supabaseFile = (path: string) =>
  fileURLToPath(new URL(path, SUPABASE).href);
const TRACE = "0192f000-0000-7000-8000-000000000001";

let db: PGlite;
const query = readOnlyQuery(async () => db as never);
const find = (points: MetricPoint[], metric: string, labels: Partial<MetricPoint> = {}) =>
  points.filter(
    (point) =>
      point.metric === metric &&
      Object.entries(labels).every(([k, v]) => point[k as keyof MetricPoint] === v),
  );
const value = (points: MetricPoint[], metric: string, labels: Partial<MetricPoint> = {}) => {
  const [point, ...rest] = find(points, metric, labels);
  expect(rest).toEqual([]);
  return point?.value;
};

const job = (stage: string, columns: string, values: string) =>
  db.exec(`INSERT INTO sessions.pipeline_jobs
    (stage, work_key, trace_id, org_id, workspace_id, message${columns})
    VALUES ('${stage}', gen_random_uuid()::text, '${TRACE}', 'org', 'ws', '{}'${values})`);
const letter = (id: string, stage: string, state: string, age: string) =>
  db.exec(`INSERT INTO sessions.pipeline_dead_letters (id, stage, message, state, first_dead_at)
    VALUES ('${id}', '${stage}', '{}', '${state}', now() - interval '${age}')`);
const event = (
  stage: string,
  status: string,
  age: string,
  options: { key?: string | null; ms?: number; code?: string } = {},
) =>
  db.query(
    `INSERT INTO sessions.pipeline_events
       (trace_id, stage, status, at, idempotency_key, duration_ms, error)
     VALUES ($1, $2, $3, now() - $4::interval, $5, $6, $7)`,
    [
      TRACE,
      stage,
      status,
      age,
      options.key === undefined ? crypto.randomUUID() : options.key,
      options.ms ?? null,
      options.code ? JSON.stringify({ code: options.code }) : null,
    ],
  );

beforeAll(async () => {
  db = new PGlite();
  await db.exec(readFileSync(supabaseFile("tests/supabase_shim.sql"), "utf8"));
  for (const file of readdirSync(supabaseFile("migrations/")).sort())
    await db.exec(readFileSync(supabaseFile(`migrations/${file}`), "utf8"));
  // The tables have RLS on and no policy: only a BYPASSRLS role sees rows.
  await db.exec(`CREATE ROLE metrics_ro BYPASSRLS NOLOGIN;
    GRANT USAGE ON SCHEMA sessions TO metrics_ro;
    GRANT SELECT ON sessions.pipeline_jobs, sessions.pipeline_dead_letters,
      sessions.pipeline_events TO metrics_ro`);
});
afterAll(async () => {
  await db?.close();
});
beforeEach(async () => {
  await db.exec(`RESET ROLE;
    TRUNCATE sessions.pipeline_jobs, sessions.pipeline_dead_letters;
    TRUNCATE sessions.pipeline_events`);
});
const read = async (window?: number) => {
  await db.exec("SET ROLE metrics_ro");
  try {
    return await readPipelineMetrics(query, window);
  } finally {
    await db.exec("RESET ROLE");
  }
};

describe("readPipelineMetrics", () => {
  it("reports a real zero for every stage when the pipeline is empty", async () => {
    const { points, failures } = await read();
    expect(failures).toEqual([]);
    for (const stage of QUEUED_STAGES) {
      expect(value(points, "pipeline.stage.queue_depth", { stage })).toBe(0);
      expect(value(points, "pipeline.stage.lag_seconds", { stage })).toBe(0);
      expect(value(points, "pipeline.dlq.depth", { stage, state: "dead" })).toBe(0);
      expect(value(points, "pipeline.dlq.depth", { stage, state: "pending" })).toBe(0);
    }
    expect(find(points, "cutover.metrics.read_ok").map((p) => [p.kind, p.value])).toEqual([
      ["jobs", 1],
      ["dead_letters", 1],
      ["events", 1],
      ["errors", 1],
    ]);
  });

  it("separates ready lag from backoff, leases and the DLQ path", async () => {
    await job("object-landed", ", visible_at, enqueued_at", ", now() - interval '90 seconds', now() - interval '100 seconds'");
    await job("object-landed", ", visible_at, enqueued_at", ", now() - interval '10 seconds', now() - interval '10 seconds'");
    // Retry backoff: queued, not ready, no lease.
    await job("object-landed", ", visible_at, enqueued_at", ", now() + interval '60 seconds', now() - interval '400 seconds'");
    // Held by a consumer that may be dead: invisible to lag, visible to the job's age.
    await job(
      "session-indexed",
      ", visible_at, lease_id, enqueued_at",
      ", now() + interval '200 seconds', gen_random_uuid(), now() - interval '1000 seconds'",
    );
    await job("session-indexed", ", dlq, enqueued_at", ", true, now() - interval '9000 seconds'");

    const { points } = await read();
    const landed = { stage: "object-landed" };
    expect(value(points, "pipeline.stage.queue_depth", landed)).toBe(3);
    expect(value(points, "pipeline.stage.ready_depth", landed)).toBe(2);
    expect(value(points, "pipeline.stage.leased_depth", landed)).toBe(0);
    expect(value(points, "pipeline.stage.lag_seconds", landed)).toBeGreaterThanOrEqual(90);
    expect(value(points, "pipeline.stage.lag_seconds", landed)).toBeLessThan(120);
    expect(value(points, "pipeline.stage.oldest_job_age_seconds", landed)).toBeGreaterThanOrEqual(400);

    const indexed = { stage: "session-indexed" };
    expect(value(points, "pipeline.stage.queue_depth", indexed)).toBe(1);
    expect(value(points, "pipeline.stage.leased_depth", indexed)).toBe(1);
    expect(value(points, "pipeline.stage.lag_seconds", indexed)).toBe(0);
    expect(value(points, "pipeline.stage.oldest_job_age_seconds", indexed)).toBeLessThan(2000);
    expect(value(points, "pipeline.dlq.depth", { ...indexed, state: "pending" })).toBe(1);
  });

  it("counts unresolved dead letters by state, including span stages", async () => {
    await letter("a", "session-indexed", "dead", "1 hour");
    await letter("b", "session-indexed", "replaying", "2 hours");
    await letter("c", "session-indexed", "resolved", "9 hours");
    await letter("d", "ingest", "dead", "1 minute");

    const { points } = await read();
    const stage = "session-indexed";
    expect(value(points, "pipeline.dlq.depth", { stage, state: "dead" })).toBe(1);
    expect(value(points, "pipeline.dlq.depth", { stage, state: "replaying" })).toBe(1);
    const age = value(points, "pipeline.dlq.oldest_age_seconds", { stage })!;
    expect(age).toBeGreaterThanOrEqual(7200);
    expect(age).toBeLessThan(7300);
    expect(value(points, "pipeline.dlq.depth", { stage: "ingest", state: "dead" })).toBe(1);
  });

  it("counts events in the window, folding redeliveries and skipping the future", async () => {
    await event("session-indexed", "ok", "10 seconds", { key: "same", ms: 100 });
    await event("session-indexed", "ok", "9 seconds", { key: "same", ms: 100 });
    await event("session-indexed", "ok", "8 seconds", { key: null, ms: 200 });
    await event("session-indexed", "ok", "20 minutes");
    await event("session-indexed", "ok", "-4 minutes");

    const { points } = await read(300);
    const labels = { stage: "session-indexed", status: "ok" };
    expect(value(points, "pipeline.stage.events", labels)).toBe(2);
    expect(value(points, "pipeline.stage.duration_p95_ms", labels)).toBeGreaterThan(100);
    expect(value(points, "pipeline.stage.last_event_age_seconds", labels)).toBeLessThan(60);
  });

  it("labels errors by code and never emits a value that isn't code-shaped", async () => {
    await event("session-indexed", "error", "5 seconds", { code: "index_failed" });
    await event("session-indexed", "dead_lettered", "5 seconds", { code: "stage_not_configured" });
    await event("deliver.put", "error", "5 seconds", { code: "Bearer sk-live-planted secret" });
    await event("deliver.put", "error", "5 seconds", { code: "/Users/someone/project" });

    const { points } = await read();
    expect(value(points, "pipeline.stage.errors", { stage: "session-indexed", error_code: "index_failed" })).toBe(1);
    expect(value(points, "pipeline.stage.errors", { stage: "deliver.put", error_code: "other" })).toBe(2);
    const text = JSON.stringify(points);
    expect(text).not.toContain("sk-live");
    expect(text).not.toContain("/Users");
  });

  it("carries no trace, org, session or content field on any point", async () => {
    await job("object-landed", "", "");
    await letter("a", "session-indexed", "dead", "1 hour");
    await event("session-indexed", "error", "5 seconds", { code: "index_failed", ms: 3 });
    const { points } = await read();
    const allowed = ["metric", "value", "unit", "stage", "status", "state", "error_code", "kind"];
    for (const point of points)
      expect(Object.keys(point).filter((key) => !allowed.includes(key))).toEqual([]);
  });

  it("emits read_ok 0 and no zeros for a source that fails", async () => {
    await letter("a", "session-indexed", "dead", "1 hour");
    await db.exec("REVOKE SELECT ON sessions.pipeline_jobs FROM metrics_ro");
    try {
      const { points, failures } = await read();
      expect(failures.map((failure) => failure.kind)).toEqual(["jobs"]);
      expect(value(points, "cutover.metrics.read_ok", { kind: "jobs" })).toBe(0);
      expect(find(points, "pipeline.stage.queue_depth")).toEqual([]);
      expect(find(points, "pipeline.stage.lag_seconds")).toEqual([]);
      expect(value(points, "pipeline.dlq.depth", { stage: "session-indexed", state: "dead" })).toBe(1);
    } finally {
      await db.exec("GRANT SELECT ON sessions.pipeline_jobs TO metrics_ro");
    }
  });
});

describe("readOnlyQuery", () => {
  it("refuses a write even for a role allowed to make it", async () => {
    await expect(
      query("DELETE FROM sessions.pipeline_jobs"),
    ).rejects.toMatchObject({ code: "25006" });
    expect((await db.query("SELECT 1 AS ok")).rows).toEqual([{ ok: 1 }]);
  });

  // PGlite has no signal timer, so the setting is checked rather than a real cancel.
  it("runs under the statement timeout, scoped to its transaction", async () => {
    const { rows } = await readOnlyQuery(async () => db as never, 50)<{ timeout: string }>(
      "SELECT current_setting('statement_timeout') AS timeout",
    );
    expect(rows).toEqual([{ timeout: "50ms" }]);
    expect((await db.query("SHOW statement_timeout")).rows).toEqual([{ statement_timeout: "0" }]);
  });

  it("rolls back and hands the failed connection back to the pool", async () => {
    const released: unknown[] = [];
    const seen: string[] = [];
    const failure = new Error("boom");
    const client = {
      async query(sql: string) {
        seen.push(sql);
        if (sql === "SELECT 1") throw failure;
        return { rows: [] };
      },
      release: (err?: Error) => released.push(err),
    };
    await expect(readOnlyQuery(async () => client as never)("SELECT 1")).rejects.toBe(failure);
    expect(seen[0]).toBe("BEGIN READ ONLY");
    expect(seen.at(-1)).toBe("ROLLBACK");
    expect(released).toEqual([failure]);
  });
});
