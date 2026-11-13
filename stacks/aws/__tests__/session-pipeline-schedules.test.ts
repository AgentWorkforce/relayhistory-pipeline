import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import {
  SESSION_ANALYSIS_CACHE_RESOURCES,
  SESSION_ANALYSIS_JOB_RESOURCES,
  SESSION_BUFFERED_ERASURE_DELETE_RESOURCES,
  SESSION_BUFFERED_SEARCH_FEED_RESOURCES,
  SESSION_PIPELINE_SCHEDULE_GROUPS,
  SESSION_SEMANTIC_COPY_RESOURCES,
  assertSessionPipelineInputs,
  declaredScheduleResources,
} from "../session-pipeline";

const refusedBy = (v: unknown) => {
  try { assertSessionPipelineInputs(v, (w: string) => { throw new Error(w); }); return ""; } catch (e) { return (e as Error).message; }
};

describe("session pipeline: optional schedule groups are declared one at a time (sessionPipeline.schedules)", () => {
  test("absent or empty: no optional schedule resources, so a plan from main adds none", () => {
    expect(declaredScheduleResources({})).toEqual([]);
    expect(declaredScheduleResources({ schedules: {} })).toEqual([]);
    // Lane settings alone never declare a schedule.
    expect(declaredScheduleResources({ searchIndex: true, analysisJobs: { enabled: false }, bufferedSearchFeed: { enabled: false } })).toEqual([]);
  });

  test("each group declares exactly its own resources, independently", () => {
    const expected = {
      analysisCache: SESSION_ANALYSIS_CACHE_RESOURCES,
      semanticCopy: SESSION_SEMANTIC_COPY_RESOURCES,
      analysisJob: SESSION_ANALYSIS_JOB_RESOURCES,
      bufferedSearchFeed: SESSION_BUFFERED_SEARCH_FEED_RESOURCES,
      bufferedErasureDelete: SESSION_BUFFERED_ERASURE_DELETE_RESOURCES,
    };
    for (const g of SESSION_PIPELINE_SCHEDULE_GROUPS) expect(declaredScheduleResources({ schedules: { [g]: true } })).toEqual([...expected[g]]);
    const all = Object.fromEntries(SESSION_PIPELINE_SCHEDULE_GROUPS.map((g) => [g, true as const]));
    expect(declaredScheduleResources({ schedules: all }).sort()).toEqual(Object.values(expected).flat().sort());
  });

  test("the input is exact: known groups only, each true", () => {
    expect(refusedBy({ schedules: { analysisJob: true, semanticCopy: true } })).toBe("");
    expect(refusedBy({ schedules: { briefs: true } })).toMatch(/schedules.briefs is not a known schedule group/);
    expect(refusedBy({ schedules: { analysisJob: false } })).toMatch(/schedules.analysisJob must be true/);
    expect(refusedBy({ schedules: { analysisJob: "yes" } })).toMatch(/must be true/);
    expect(refusedBy({ schedules: [] })).toMatch(/schedules must be a plain object/);
    expect(refusedBy({ schedules: null })).toMatch(/schedules must be a plain object/);
  });

  test("a lane cannot be enabled without its schedule group", () => {
    const gate = { enabled: false, gateDeclared: "buffered-search-gate/v1" };
    expect(refusedBy({ analysisJobs: { enabled: true } })).toMatch(/analysisJobs.enabled requires schedules.analysisJob/);
    expect(refusedBy({ semanticCopy: { enabled: true, sourceId: "s" } })).toMatch(/semanticCopy.enabled requires schedules.semanticCopy/);
    expect(refusedBy({ searchIndex: true, bufferedSearchFeed: { ...gate, enabled: true } })).toMatch(/bufferedSearchFeed.enabled requires schedules.bufferedSearchFeed/);
    expect(refusedBy({ searchIndex: true, bufferedSearchFeed: gate, bufferedErasureDelete: { enabled: true } })).toMatch(/bufferedErasureDelete.enabled requires schedules.bufferedErasureDelete/);
    // Another group's declaration does not count.
    expect(refusedBy({ analysisJobs: { enabled: true }, schedules: { semanticCopy: true } })).toMatch(/requires schedules.analysisJob/);
    // Declared but disabled is fine: the rule exists, DISABLED.
    expect(refusedBy({ analysisJobs: { enabled: false }, schedules: { analysisJob: true } })).toBe("");
  });

  test("the stack renders every optional rule only behind its declaration", () => {
    const src = readFileSync(new URL("../session-pipeline.ts", import.meta.url), "utf8");
    const rules = [...src.matchAll(/AWS\.EventBridge\.Rule\("([^"]+)"/g)].map((m) => m[1]!);
    const optional = rules.filter((r) => r !== "session-reconcile-schedule" && r !== "session-brief-schedule");
    expect(optional.sort()).toEqual(SESSION_PIPELINE_SCHEDULE_GROUPS.flatMap((g) => declaredScheduleResources({ schedules: { [g]: true } })).filter((r) => r.endsWith("-schedule")).sort());
    for (const r of optional) {
      const line = src.split("\n").find((l) => l.includes(`AWS.EventBridge.Rule("${r}"`))!;
      expect(line).toMatch(/declaresSchedule\(i\.p, "[A-Za-z]+"\) \? yield\* AWS\.EventBridge\.Rule/);
    }
  });
});
