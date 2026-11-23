import { describe, expect, it } from "bun:test";
import { createHash } from "node:crypto";
import {
  SESSION_ANALYSIS_RUNNER_ACCOUNT_ID,
  SESSION_ANALYSIS_RUNNER_ROLE_ARN,
  assertSessionAnalysisRunnerRoleArn,
  sessionAnalysisRunnerBucketStatements,
  sessionAnalysisRunnerKeyStatements,
} from "../session-analysis-runner-policy.ts";
import {
  TRANSCRIPT_ACCOUNT_ID,
  TRANSCRIPT_REGION,
  TRANSCRIPT_STAGE,
  assertTranscriptStorePlanContract,
  transcriptBucketPolicy,
  transcriptKeyPolicy,
  transcriptNames,
} from "../transcript-store.ts";

const names = transcriptNames(TRANSCRIPT_STAGE, TRANSCRIPT_ACCOUNT_ID);
const eraserArn = `arn:aws:iam::${TRANSCRIPT_ACCOUNT_ID}:user/96bulls/transcript-eraser`;
const keyArgs = {
  accountId: TRANSCRIPT_ACCOUNT_ID,
  region: TRANSCRIPT_REGION,
  bucketName: names.bucket,
  writerArn: "writer",
  srvWriterArn: "srv-writer",
  readerArn: "reader",
};

describe("session-analysis runner resource policies", () => {
  it("pins the one reviewed role ARN, not merely its account", () => {
    expect(() => assertSessionAnalysisRunnerRoleArn(SESSION_ANALYSIS_RUNNER_ROLE_ARN)).not.toThrow();
    for (const bad of [
      "*",
      "",
      `arn:aws:iam::${SESSION_ANALYSIS_RUNNER_ACCOUNT_ID}:role/AdminRole`,
      `arn:aws:iam::${SESSION_ANALYSIS_RUNNER_ACCOUNT_ID}:role/path/agent-relay-prod-session-analysis`,
      `arn:aws:iam::${SESSION_ANALYSIS_RUNNER_ACCOUNT_ID}:user/agent-relay-prod-session-analysis`,
      "arn:aws:iam::941433935742:role/agent-relay-prod-session-analysis",
    ]) expect(() => assertSessionAnalysisRunnerRoleArn(bad)).toThrow();
  });

  it("grants canonical reads/results only and explicitly denies every reserved dot namespace", () => {
    const bucket = sessionAnalysisRunnerBucketStatements({ bucketArn: names.bucketArn, roleArn: SESSION_ANALYSIS_RUNNER_ROLE_ARN });
    expect(bucket.map((s) => s.Sid)).toEqual([
      "SessionAnalysisInspectsBucket",
      "SessionAnalysisReads",
      "SessionAnalysisWritesResults",
      "SessionAnalysisListsResultVersions",
      "SessionAnalysisDeniesReservedNamespaces",
    ]);
    expect(bucket.find((s) => s.Sid === "SessionAnalysisReads")!.Resource).toBe(`${names.bucketArn}/96bulls/*/v1/*`);
    expect(bucket.find((s) => s.Sid === "SessionAnalysisWritesResults")!.Resource)
      .toBe(`${names.bucketArn}/96bulls/*/v1/*/normalized/session-analysis-v2/*`);
    expect(bucket.find((s) => s.Sid === "SessionAnalysisDeniesReservedNamespaces")!.Resource)
      .toBe(`${names.bucketArn}/96bulls/.*`);

    const key = sessionAnalysisRunnerKeyStatements({ region: TRANSCRIPT_REGION, bucketName: names.bucket, roleArn: SESSION_ANALYSIS_RUNNER_ROLE_ARN });
    expect(key.map((s) => s.Sid)).toEqual([
      "SessionAnalysisDecryptsViaS3",
      "SessionAnalysisEncryptsViaS3",
      "SessionAnalysisDeniesReservedNamespacesViaS3",
    ]);
    expect(key.at(-1)!.Effect).toBe("Deny");
    expect(key.at(-1)!.Condition.StringLike["kms:EncryptionContext:aws:s3:arn"])
      .toBe(`${names.bucketArn}/96bulls/.*`);
  });

  it("is byte-identical while omitted and folds the runner into the existing analysis delete deny", () => {
    const baseBucket = transcriptBucketPolicy({ accountId: TRANSCRIPT_ACCOUNT_ID, bucketArn: names.bucketArn, keyArn: "key", eraserArn });
    expect(transcriptBucketPolicy({ accountId: TRANSCRIPT_ACCOUNT_ID, bucketArn: names.bucketArn, keyArn: "key", eraserArn, sessionAnalysisRunnerRoleArn: undefined })).toEqual(baseBucket);
    const withRunner = transcriptBucketPolicy({ accountId: TRANSCRIPT_ACCOUNT_ID, bucketArn: names.bucketArn, keyArn: "key", eraserArn, sessionAnalysisRunnerRoleArn: SESSION_ANALYSIS_RUNNER_ROLE_ARN });
    expect(withRunner.find((s) => s.Sid === "OnlyTheEraserOrAnalysisWorkerDeletesAnalysisResults")!.Condition)
      .toEqual({ ArnNotEquals: { "aws:PrincipalArn": [eraserArn, SESSION_ANALYSIS_RUNNER_ROLE_ARN] } });
    const pipeline = `arn:aws:iam::${TRANSCRIPT_ACCOUNT_ID}:role/pipeline`;
    const both = transcriptBucketPolicy({ accountId: TRANSCRIPT_ACCOUNT_ID, bucketArn: names.bucketArn, keyArn: "key", eraserArn, analysisDeleterArn: pipeline, sessionAnalysisRunnerRoleArn: SESSION_ANALYSIS_RUNNER_ROLE_ARN });
    expect(both.find((s) => s.Sid === "OnlyTheEraserOrAnalysisWorkerDeletesAnalysisResults")!.Condition)
      .toEqual({ ArnNotEquals: { "aws:PrincipalArn": [eraserArn, pipeline, SESSION_ANALYSIS_RUNNER_ROLE_ARN] } });

    const baseKey = transcriptKeyPolicy(keyArgs);
    expect(transcriptKeyPolicy({ ...keyArgs, sessionAnalysisRunnerRoleArn: undefined })).toEqual(baseKey);
    expect(transcriptKeyPolicy({ ...keyArgs, sessionAnalysisRunnerRoleArn: SESSION_ANALYSIS_RUNNER_ROLE_ARN }).Statement.slice(baseKey.Statement.length))
      .toEqual(sessionAnalysisRunnerKeyStatements({ region: TRANSCRIPT_REGION, bucketName: names.bucket, roleArn: SESSION_ANALYSIS_RUNNER_ROLE_ARN }));
  });
});

const RESOURCES = [
  "access-logs", "budget", "trail", "trail-key", "trail-key-alias", "trail-logs",
  "transcript-eraser", "transcript-reader", "transcript-srv-writer", "transcript-writer", "transcripts",
  "transcripts-key", "transcripts-key-alias",
];
const hash = (text: string) => createHash("sha256").update(text).digest("hex");
const scalar = (value: unknown) => typeof value === "string" && /^[A-Za-z][A-Za-z0-9:_./*=-]*$/.test(value) ? value : JSON.stringify(value);
function yaml(value: unknown, indent: number): string[] {
  const pad = " ".repeat(indent);
  if (Array.isArray(value)) return value.flatMap((item) => {
    if (item !== null && typeof item === "object") {
      const [first, ...rest] = yaml(item, indent + 2);
      return [`${pad}- ${first!.trimStart()}`, ...rest];
    }
    return [`${pad}- ${scalar(item)}`];
  });
  return Object.keys(value as object).sort().flatMap((key) => {
    const item = (value as Record<string, unknown>)[key];
    return item !== null && typeof item === "object"
      ? [`${pad}${key}:`, ...yaml(item, indent + 2)]
      : [`${pad}${key}: ${scalar(item)}`];
  });
}
const mark = (sign: "+" | "-", lines: string[]) => lines.map((line) => `  ${sign} ${line.slice(2)}`);
const bucket = (runner?: string) => transcriptBucketPolicy({
  accountId: TRANSCRIPT_ACCOUNT_ID,
  bucketArn: names.bucketArn,
  keyArn: "key",
  eraserArn,
  ...(runner ? { sessionAnalysisRunnerRoleArn: runner } : {}),
});
const key = (runner?: string) => transcriptKeyPolicy({ ...keyArgs, ...(runner ? { sessionAnalysisRunnerRoleArn: runner } : {}) });
const bucketBlock = (live: unknown[], planned: unknown[]) => [
  "  [transcripts] update", "  properties:", `    bucketName: ${names.bucket}`,
  ...mark("-", ["    policy:", ...yaml(live, 6)]),
  ...mark("+", ["    policy:", ...yaml(planned, 6)]),
];
const keyBlock = (live: { Statement: unknown[] }, planned: { Statement: unknown[] }) => [
  "  [transcripts-key] update", "  properties:", "    policy:",
  ...mark("-", yaml(live, 6)),
  ...mark("+", yaml(planned, 6)),
];
function check(blocks: Record<string, string[]>, actions: Record<string, string>, input?: unknown) {
  const counts = new Map<string, number>();
  for (const action of Object.values(actions)) counts.set(action, (counts.get(action) ?? 0) + 1);
  const summary = counts.size ? `Plan: ${[...counts].map(([action, count]) => `${count} to ${action}`).join(", ")}` : "Plan: 0 changes";
  const compact = `${summary}\n${RESOURCES.map((r) => `[${r}] ${actions[r] ?? "noop"}`).join("\n")}\n`;
  const detailed = `${summary}\n${RESOURCES.map((r) => blocks[r] ? blocks[r]!.join("\n") : `  [${r}] ${actions[r] ?? "noop"}`).join("\n")}\n`;
  const provenanceText = JSON.stringify({
    schema: 1,
    stage: TRANSCRIPT_STAGE,
    summary,
    outputs: {
      plan: { path: `docs/plans/${TRANSCRIPT_STAGE}.plan.txt`, sha256: hash(compact) },
      detailed: { path: `docs/plans/${TRANSCRIPT_STAGE}.plan.detailed.txt`, sha256: hash(detailed) },
    },
  });
  return assertTranscriptStorePlanContract(compact, {
    detailedPlanText: detailed,
    provenanceText,
    uploadInputs: input === undefined ? {} : { sessionAnalysisRunnerRoleArn: input },
  });
}

describe("session-analysis runner plan contract", () => {
  const liveBucket = bucket();
  const plannedBucket = bucket(SESSION_ANALYSIS_RUNNER_ROLE_ARN);
  const liveKey = key();
  const plannedKey = key(SESSION_ANALYSIS_RUNNER_ROLE_ARN);
  const exact = {
    transcripts: bucketBlock(liveBucket, plannedBucket),
    "transcripts-key": keyBlock(liveKey, plannedKey),
  };

  it("accepts both exact policy sides and harmless no-declared-change cascades", () => {
    expect(check(exact, { transcripts: "update", "transcripts-key": "update" }, SESSION_ANALYSIS_RUNNER_ROLE_ARN))
      .toBe("session analysis runner grants");
    expect(check({ ...exact, "transcript-reader": ["  [transcript-reader] update", "  no declared property changes"] },
      { transcripts: "update", "transcripts-key": "update", "transcript-reader": "update" }, SESSION_ANALYSIS_RUNNER_ROLE_ARN))
      .toBe("session analysis runner grants");
  });

  it("accepts the exact revocation only after the input is removed", () => {
    expect(check({
      transcripts: bucketBlock(plannedBucket, liveBucket),
      "transcripts-key": keyBlock(plannedKey, liveKey),
    }, { transcripts: "update", "transcripts-key": "update" })).toBe("session analysis runner grants");
    expect(() => check({
      transcripts: bucketBlock(plannedBucket, liveBucket),
      "transcripts-key": keyBlock(plannedKey, liveKey),
    }, { transcripts: "update", "transcripts-key": "update" }, SESSION_ANALYSIS_RUNNER_ROLE_ARN)).toThrow(/cannot remove/);
  });

  it("ports cleanly over the live multi-principal and upload-inbox deny shape", () => {
    const gc = `arn:aws:iam::${TRANSCRIPT_ACCOUNT_ID}:role/test-buffered-gc`;
    const rearm = `arn:aws:iam::${TRANSCRIPT_ACCOUNT_ID}:role/test-rearm-sweeper`;
    const upload = {
      uploadRoleArn: `arn:aws:iam::${TRANSCRIPT_ACCOUNT_ID}:role/test-upload`,
      consumerRoleArn: `arn:aws:iam::${TRANSCRIPT_ACCOUNT_ID}:role/test-consumer`,
    };
    const commonBucket = { accountId: TRANSCRIPT_ACCOUNT_ID, bucketArn: names.bucketArn, keyArn: "key", eraserArn, bufferedGcRoleArn: gc, rearmSweeperRoleArn: rearm, uploadInbox: upload };
    const commonKey = { ...keyArgs, bufferedGcRoleArn: gc, rearmSweeperRoleArn: rearm, uploadInbox: upload };
    const complexLiveBucket = transcriptBucketPolicy(commonBucket);
    const complexNextBucket = transcriptBucketPolicy({ ...commonBucket, sessionAnalysisRunnerRoleArn: SESSION_ANALYSIS_RUNNER_ROLE_ARN });
    const complexLiveKey = transcriptKeyPolicy(commonKey);
    const complexNextKey = transcriptKeyPolicy({ ...commonKey, sessionAnalysisRunnerRoleArn: SESSION_ANALYSIS_RUNNER_ROLE_ARN });
    expect(check({
      transcripts: bucketBlock(complexLiveBucket, complexNextBucket),
      "transcripts-key": keyBlock(complexLiveKey, complexNextKey),
    }, { transcripts: "update", "transcripts-key": "update" }, SESSION_ANALYSIS_RUNNER_ROLE_ARN)).toBe("session analysis runner grants");
  });

  it("refuses a missing side, a wrong role, or any declared ride-along", () => {
    expect(() => check({ transcripts: exact.transcripts }, { transcripts: "update" }, SESSION_ANALYSIS_RUNNER_ROLE_ARN)).toThrow(/must update together/);
    expect(() => check(exact, { transcripts: "update", "transcripts-key": "update" }, `arn:aws:iam::${SESSION_ANALYSIS_RUNNER_ACCOUNT_ID}:role/AdminRole`)).toThrow(/exact dedicated runner role/);
    expect(() => check({ ...exact, "transcript-reader": ["  [transcript-reader] update", "  properties:", "    policy: widened"] },
      { transcripts: "update", "transcripts-key": "update", "transcript-reader": "update" }, SESSION_ANALYSIS_RUNNER_ROLE_ARN)).toThrow(/declared change/);
  });

  it("refuses a wildcard KMS ride-along even beside the exact grants", () => {
    const widened = structuredClone(plannedKey);
    widened.Statement.push({ Sid: "WildcardKmsRideAlong", Effect: "Allow", Principal: { AWS: "*" }, Action: ["kms:*"], Resource: "*" });
    expect(() => check({ transcripts: exact.transcripts, "transcripts-key": keyBlock(liveKey, widened) },
      { transcripts: "update", "transcripts-key": "update" }, SESSION_ANALYSIS_RUNNER_ROLE_ARN)).toThrow(/unreviewed statement/);
  });
});
