import { afterEach, describe, expect, spyOn, test } from "bun:test";
import { emitPipelineMetrics, parseArgs } from "../cutover-metrics-run.ts";
import { AlertTracker } from "../cutover-alerts.ts";
import type { ReadOnlyQuery } from "../cutover-metrics.ts";

const captured = () => {
  const lines: string[] = [];
  const write = (chunk: unknown) => (lines.push(String(chunk)), true);
  spyOn(process.stdout, "write").mockImplementation(write as never);
  spyOn(process.stderr, "write").mockImplementation(write as never);
  return lines;
};
afterEach(() => {
  (process.stdout.write as unknown as { mockRestore(): void }).mockRestore?.();
  (process.stderr.write as unknown as { mockRestore(): void }).mockRestore?.();
});

describe("emitPipelineMetrics", () => {
  test("every label survives the log allowlist", async () => {
    const lines = captured();
    const query = (async (sql: string) => ({
      rows: sql.includes("cutover_metrics_jobs")
        ? [{ stage: "object-landed", depth: "2", ready: "1", leased: "1", dlq_pending: "0", lag_seconds: "12.5", oldest_seconds: "40" }]
        : sql.includes("cutover_metrics_errors")
          ? [{ stage: "deliver.put", status: "error", code: "put_failed", events: "3" }]
          : [],
    })) as ReadOnlyQuery;
    expect(await emitPipelineMetrics(query, 300)).toBe(true);
    const parsed = lines.map((line) => JSON.parse(line));
    expect(parsed.find((l) => l.metric === "pipeline.stage.lag_seconds" && l.stage === "object-landed")).toMatchObject({
      message: "cutover metric", value: 12.5, unit: "s",
    });
    expect(parsed.find((l) => l.metric === "pipeline.stage.errors")).toMatchObject({
      stage: "deliver.put", status: "error", error_code: "put_failed", value: 3,
    });
  });

  test("a failed read logs the code, never the database's text", async () => {
    const lines = captured();
    const query = (async () => {
      throw Object.assign(new Error("connect postgres://user:hunter2@db.example/x"), { code: "57014" });
    }) as ReadOnlyQuery;
    expect(await emitPipelineMetrics(query)).toBe(false);
    const text = lines.join("");
    expect(text).toContain('"error_code":"57014"');
    expect(text).not.toContain("hunter2");
    expect(lines.map((line) => JSON.parse(line)).filter((l) => l.metric === "cutover.metrics.read_ok").map((l) => l.value)).toEqual([0, 0, 0, 0]);
  });
});

describe("alert delivery", () => {
  const blind = (async () => { throw Object.assign(new Error("x"), { code: "CM001" }); }) as ReadOnlyQuery;
  const sink = (status: number, sent: string[]) => ({
    target: { url: "https://n.example/webhooks/cloud-runtime", token: "planted-token" },
    tracker: new AlertTracker(),
    fetcher: (async (_: string, init: RequestInit) => (sent.push(String(init.body)), new Response(null, { status }))) as never,
  });
  test("an RLS-blind read alerts with the fail-loud code, and the sink is reported", async () => {
    const lines = captured();
    const sent: string[] = [];
    expect(await emitPipelineMetrics(blind, 300, sink(202, sent))).toBe(false);
    expect(sent).toHaveLength(4);
    expect(JSON.parse(sent[0]!).incidentFingerprint).toBe("96bulls:cutover:metrics_blind:jobs");
    expect(lines.join("")).toContain('"error_code":"CM001"');
    expect(lines.map((l) => JSON.parse(l)).find((l) => l.metric === "cutover.alerts.configured").value).toBe(1);
  });
  test("a refused alert is logged at error with its status, never the token", async () => {
    const lines = captured();
    await emitPipelineMetrics(blind, 300, sink(401, []));
    const refused = lines.map((l) => JSON.parse(l)).filter((l) => l.message === "cutover alert not delivered");
    expect(refused).toHaveLength(4);
    expect(refused[0]).toMatchObject({ level: "error", status: 401, rule: "metrics_blind" });
    expect(lines.join("")).not.toContain("planted-token");
  });
  test("no sink is reported as such rather than reading as quiet", async () => {
    const lines = captured();
    await emitPipelineMetrics((async () => ({ rows: [] })) as ReadOnlyQuery, 300);
    expect(lines.map((l) => JSON.parse(l)).find((l) => l.metric === "cutover.alerts.configured").value).toBe(0);
  });
});

describe("parseArgs", () => {
  test("defaults, and refuses a poll tight enough to load the database", () => {
    expect(parseArgs([])).toEqual({ once: false, intervalSeconds: 60, windowSeconds: 300 });
    expect(parseArgs(["--once", "--window=60"])).toEqual({ once: true, intervalSeconds: 60, windowSeconds: 60 });
    expect(() => parseArgs(["--interval=1"])).toThrow();
    expect(() => parseArgs(["--interval=abc"])).toThrow();
    expect(() => parseArgs(["--window=3601"])).toThrow();
  });
});
