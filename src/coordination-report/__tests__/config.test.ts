import { describe, expect, test } from "bun:test";
import { coordinationConfig, parseCoordinationInvocation } from "../config.ts";
import { handler } from "../../session-pipeline/lambda.ts";
import {
  assertSessionPipelineInputs,
  sessionPipelineEnv,
  sessionPipelineNames,
} from "../../../stacks/aws/session-pipeline.ts";

describe("coordination operator boundary", () => {
  test("disabled handler does not initialize secrets or storage", async () => {
    const saved = process.env.COORDINATION_REPORT_ENABLED;
    try {
      delete process.env.COORDINATION_REPORT_ENABLED;
      expect(await handler({ coordinationReport: {} })).toEqual({
        ok: false,
        reason: "coordination_disabled",
      });
      expect(await handler({ coordinationReportRead: {} })).toEqual({
        ok: false,
        reason: "coordination_disabled",
      });
    } finally {
      if (saved === undefined) delete process.env.COORDINATION_REPORT_ENABLED;
      else process.env.COORDINATION_REPORT_ENABLED = saved;
    }
  });
  test("requires operator scope and rejects malformed or oversized budgets", () => {
    expect(coordinationConfig({})).toBeNull();
    expect(() =>
      coordinationConfig({ COORDINATION_REPORT_ENABLED: "true" }),
    ).toThrow();
    const env = {
      COORDINATION_REPORT_ENABLED: "true",
      COORDINATION_REPORT_ORG_ID: "org",
      COORDINATION_REPORT_WORKSPACE_ID: "workspace",
    };
    expect(coordinationConfig(env)?.scope).toEqual({
      organizationId: "org",
      workspaceId: "workspace",
    });
    expect(() =>
      coordinationConfig({ ...env, COORDINATION_REPORT_MAX_CALLS: "25" }),
    ).toThrow();
    expect(() =>
      coordinationConfig({ ...env, COORDINATION_REPORT_MAX_CALLS: "garbage" }),
    ).toThrow();
  });
  test("stack inputs preserve the disabled default and enforce the same budget ceiling", () => {
    const refuse = (reason: string): never => {
      throw new Error(reason);
    };
    const settings = {
      enabled: true,
      organizationId: "org",
      workspaceId: "workspace",
      maxCalls: 24,
      model: "gpt-6.1-sol",
    };
    expect(() =>
      assertSessionPipelineInputs({ coordinationReports: settings }, refuse),
    ).not.toThrow();
    expect(() =>
      assertSessionPipelineInputs(
        { coordinationReports: { ...settings, maxCalls: 25 } },
        refuse,
      ),
    ).toThrow();
    expect(() =>
      assertSessionPipelineInputs(
        { coordinationReports: { ...settings, arbitraryScope: "other" } },
        refuse,
      ),
    ).toThrow();
    const base = {
      stage: "test",
      bucket: "bucket",
      names: sessionPipelineNames("test", "123456789012", "us-east-1"),
    };
    expect(
      sessionPipelineEnv({ ...base, p: {} }).COORDINATION_REPORT_ENABLED,
    ).toBe("false");
    const configured = sessionPipelineEnv({
      ...base,
      p: { coordinationReports: settings },
    });
    expect(coordinationConfig(configured)?.maxCalls).toBe(24);
    expect(coordinationConfig(configured)?.scope.workspaceId).toBe("workspace");
  });
  test("invocation cannot supply a tenant, enlarge budgets or use a future/unbounded window", () => {
    const request = {
      idempotencyKey: "week-1",
      since: "2026-10-28T07:00:00.000Z",
      until: "2026-11-04T08:00:00.000Z",
    };
    const now = new Date("2026-11-05T00:00:00.000Z");
    expect(parseCoordinationInvocation(request, now).window.until).toBe(
      request.until,
    );
    for (const extra of [
      { scope: "other" },
      { maxCalls: 200 },
      { until: "2026-11-06T00:00:00.000Z" },
      { since: "2026-01-01T00:00:00.000Z" },
      { since: request.until },
    ])
      expect(() =>
        parseCoordinationInvocation({ ...request, ...extra }, now),
      ).toThrow();
  });
});
