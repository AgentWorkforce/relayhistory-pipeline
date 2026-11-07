/**
 * Lane F1 P0-1: the analysis worker is wired to a real trigger (one-minute schedule into the
 * session pipeline Lambda), default-off, and its plan is its own reviewed phase.
 */
import { describe, expect, test } from "bun:test";
import {
  SESSION_ANALYSIS_JOB_RESOURCES,
  analysisJobScheduleProps,
  assertSessionPipelineInputs,
  sessionPipelineEnv,
  sessionPipelineNames,
  sessionPipelinePermissions,
} from "../session-pipeline.ts";

const names = sessionPipelineNames("aws-sessions-prod", "941433935742", "us-east-1");
const FN = "arn:aws:lambda:us-east-1:941433935742:function:fn";
const refuse = (why: string): never => {
  throw new Error(why);
};

describe("session analysis job schedule", () => {
  test("DISABLED and SESSION_ANALYSIS_ENABLED=false unless analysisJobs.enabled", () => {
    expect(analysisJobScheduleProps({ names, functionArn: FN, p: {}, tags: {} })).toMatchObject({
      name: "96bulls-aws-sessions-prod-session-analysis-jobs",
      scheduleExpression: "rate(1 minute)",
      state: "DISABLED",
      targets: [{ Id: "session-pipeline", Arn: FN, Input: JSON.stringify({ sessionAnalysis: true }) }],
    });
    expect(sessionPipelineEnv({ stage: "s", bucket: "b", names, p: {} }).SESSION_ANALYSIS_ENABLED).toBe("false");
    expect(analysisJobScheduleProps({ names, functionArn: FN, p: { analysisJobs: { enabled: true } }, tags: {} }).state).toBe("ENABLED");
    expect(sessionPipelineEnv({ stage: "s", bucket: "b", names, p: { analysisJobs: { enabled: true } } }).SESSION_ANALYSIS_ENABLED).toBe("true");
  });

  test("the input shape is exact and the permission names only its own rule", () => {
    expect(() => assertSessionPipelineInputs({ analysisJobs: { enabled: true, model: "x" } }, refuse)).toThrow(/analysisJobs/);
    expect(() => assertSessionPipelineInputs({ analysisJobs: { enabled: true } }, refuse)).not.toThrow();
    const perms = sessionPipelinePermissions({ accountId: "a", functionName: "f", ruleArn: "r", briefRuleArn: "b", analysisJobRuleArn: "aj", roleArn: "role" });
    expect(perms.find((p) => p.id === "session-analysis-job-permission")?.props).toMatchObject({ sourceArn: "aj", principal: "events.amazonaws.com" });
    expect(SESSION_ANALYSIS_JOB_RESOURCES).toEqual(["session-analysis-job-schedule", "session-analysis-job-permission"]);
  });
});
