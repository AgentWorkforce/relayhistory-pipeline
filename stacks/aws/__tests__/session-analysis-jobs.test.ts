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

import { pipelineBucketStatements, pipelineAnalysisKeyStatement } from "../session-pipeline.ts";
import { transcriptKeyPolicy } from "../transcript-store.ts";

describe("P0-2: the analysis-enabled pipeline S3/KMS policy", () => {
  const bucketArn = "arn:aws:s3:::96bulls-aws-sessions-prod-transcripts-941433935742";
  const roleArn = "arn:aws:iam::941433935742:role/pipeline";
  test("off: read-only, exactly as before", () => {
    const off = pipelineBucketStatements({ bucketArn, roleArn });
    expect(off.map((s) => s.Sid)).toEqual(["SessionPipelineLists", "SessionPipelineReads"]);
    expect(JSON.stringify(off)).not.toMatch(/PutObject|DeleteObject/);
  });
  test("on: Put/Delete/DeleteObjectVersion and version listing only under normalized/session-analysis-v2/", () => {
    const on = pipelineBucketStatements({ bucketArn, roleArn, analysisResults: true });
    const write = on.find((s) => s.Sid === "SessionPipelineWritesAnalysisResults")!;
    expect(write.Action).toEqual(["s3:PutObject", "s3:DeleteObject", "s3:DeleteObjectVersion"]);
    expect(write.Resource).toEqual([`${bucketArn}/96bulls/*/v1/*/normalized/session-analysis-v2/*`]);
    const list = on.find((s) => s.Sid === "SessionPipelineListsAnalysisResultVersions")! as any;
    expect(list.Action).toEqual(["s3:ListBucketVersions"]);
    expect(list.Condition.StringLike["s3:prefix"]).toEqual(["96bulls/*/v1/*/normalized/session-analysis-v2/*"]);
    // The read grants are unchanged.
    expect(on.find((s) => s.Sid === "SessionPipelineReads")!.Action).toEqual(["s3:GetObject"]);
  });
  test("on: SSE-KMS data keys bound to that object root by encryption context", () => {
    const k = pipelineAnalysisKeyStatement({ accountId: "941433935742", region: "us-east-1", bucketName: "b", roleArn });
    expect(k.Action).toEqual(["kms:GenerateDataKey", "kms:Encrypt"]);
    expect(k.Condition.StringLike["kms:EncryptionContext:aws:s3:arn"]).toBe("arn:aws:s3:::b/96bulls/*/v1/*/normalized/session-analysis-v2/*");
    const base = { accountId: "941433935742", region: "us-east-1", bucketName: "b", writerArn: "w", srvWriterArn: "s", readerArn: "r", pipelineRoleArn: roleArn };
    expect(JSON.stringify(transcriptKeyPolicy(base))).not.toContain("SessionPipelineEncryptsAnalysisResultsViaS3");
    expect(JSON.stringify(transcriptKeyPolicy({ ...base, analysisResults: true }))).toContain("SessionPipelineEncryptsAnalysisResultsViaS3");
  });
});

import {
  assertSessionReadApiInputs,
  readApiAnalysisBucketStatement,
  readApiAnalysisKeyStatement,
  sessionReadApiEnv,
  sessionReadApiNames,
} from "../session-read-api.ts";
import { ANALYSIS_RESULT_OBJECT_GLOB } from "../session-pipeline.ts";

describe("P0-3: the read API serves analysis results only when analysisBucket is on", () => {
  const rn = sessionReadApiNames("aws-sessions-prod");
  const p = { completeSince: "epoch" } as const;
  test("env: READ_API_ANALYSIS_BUCKET only with analysisBucket (no change for existing deploys)", () => {
    expect(sessionReadApiEnv({ stage: "s", names: rn, p, analysisBucketName: "bkt" })).not.toHaveProperty("READ_API_ANALYSIS_BUCKET");
    expect(sessionReadApiEnv({ stage: "s", names: rn, p: { ...p, analysisBucket: true }, analysisBucketName: "bkt" }).READ_API_ANALYSIS_BUCKET).toBe("bkt");
    expect(() => assertSessionReadApiInputs({ ...p, analysisBucket: "yes" }, refuse)).toThrow(/analysisBucket/);
  });
  test("GetObject and Decrypt on the analysis result root only", () => {
    const b = readApiAnalysisBucketStatement({ bucketArn: "arn:aws:s3:::bkt", roleArn: "r", glob: ANALYSIS_RESULT_OBJECT_GLOB });
    expect(b).toMatchObject({ Action: ["s3:GetObject"], Resource: ["arn:aws:s3:::bkt/96bulls/*/v1/*/normalized/session-analysis-v2/*"] });
    const k = readApiAnalysisKeyStatement({ accountId: "a", region: "us-east-1", bucketName: "bkt", roleArn: "r", glob: ANALYSIS_RESULT_OBJECT_GLOB });
    expect(k.Action).toEqual(["kms:Decrypt"]);
    expect(k.Condition.StringLike["kms:EncryptionContext:aws:s3:arn"]).toBe("arn:aws:s3:::bkt/96bulls/*/v1/*/normalized/session-analysis-v2/*");
  });
});
