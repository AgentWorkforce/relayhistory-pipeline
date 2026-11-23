import { describe, expect, it } from "bun:test";
import { createHash } from "node:crypto";
import {
  SESSION_ANALYSIS_RUNNER_ACCOUNT_ID,
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

const bucketAccount = "941433935742";
const bucketName = `96bulls-aws-sessions-prod-transcripts-${bucketAccount}`;
const bucketArn = `arn:aws:s3:::${bucketName}`;
const roleArn = `arn:aws:iam::${SESSION_ANALYSIS_RUNNER_ACCOUNT_ID}:role/agent-relay-prod-session-analysis`;
const eraserArn = `arn:aws:iam::${bucketAccount}:user/96bulls/transcript-eraser`;
const keyArgs = {
  accountId: bucketAccount,
  region: "us-east-1",
  bucketName,
  writerArn: "writer",
  srvWriterArn: "srv-writer",
  readerArn: "reader",
};

describe("session-analysis runner resource policies", () => {
  it("accepts only an exact role in agentrelay-prod", () => {
    expect(() => assertSessionAnalysisRunnerRoleArn(roleArn)).not.toThrow();
    for (const bad of [
      "*",
      "",
      `arn:aws:iam::${SESSION_ANALYSIS_RUNNER_ACCOUNT_ID}:user/session-analysis`,
      "arn:aws:iam::941433935742:role/session-analysis",
      `arn:aws-us-gov:iam::${SESSION_ANALYSIS_RUNNER_ACCOUNT_ID}:role/session-analysis`,
    ]) expect(() => assertSessionAnalysisRunnerRoleArn(bad)).toThrow();
  });

  it("matches the reviewed bucket-side contract exactly", () => {
    const Principal = { AWS: roleArn };
    expect(sessionAnalysisRunnerBucketStatements({ bucketArn, roleArn })).toEqual([
      { Sid: "SessionAnalysisInspectsBucket", Effect: "Allow", Principal, Action: ["s3:ListBucket", "s3:GetBucketVersioning"], Resource: bucketArn },
      { Sid: "SessionAnalysisReads", Effect: "Allow", Principal, Action: ["s3:GetObject"], Resource: `${bucketArn}/96bulls/*/v1/*` },
      { Sid: "SessionAnalysisWritesResults", Effect: "Allow", Principal, Action: ["s3:PutObject", "s3:DeleteObject", "s3:DeleteObjectVersion"], Resource: `${bucketArn}/96bulls/*/v1/*/normalized/session-analysis-v2/*` },
      { Sid: "SessionAnalysisListsResultVersions", Effect: "Allow", Principal, Action: ["s3:ListBucketVersions"], Resource: bucketArn, Condition: { StringLike: { "s3:prefix": "96bulls/*/v1/*/normalized/session-analysis-v2/*" } } },
    ]);
  });

  it("matches the reviewed KMS-side contract exactly", () => {
    const Principal = { AWS: roleArn };
    expect(sessionAnalysisRunnerKeyStatements({ region: "us-east-1", bucketName, roleArn })).toEqual([
      {
        Sid: "SessionAnalysisDecryptsViaS3", Effect: "Allow", Principal, Action: ["kms:Decrypt"], Resource: "*",
        Condition: { StringEquals: { "kms:ViaService": "s3.us-east-1.amazonaws.com", "kms:CallerAccount": SESSION_ANALYSIS_RUNNER_ACCOUNT_ID }, StringLike: { "kms:EncryptionContext:aws:s3:arn": `${bucketArn}/96bulls/*/v1/*` } },
      },
      {
        Sid: "SessionAnalysisEncryptsViaS3", Effect: "Allow", Principal, Action: ["kms:Encrypt", "kms:GenerateDataKey"], Resource: "*",
        Condition: { StringEquals: { "kms:ViaService": "s3.us-east-1.amazonaws.com", "kms:CallerAccount": SESSION_ANALYSIS_RUNNER_ACCOUNT_ID }, StringLike: { "kms:EncryptionContext:aws:s3:arn": `${bucketArn}/96bulls/*/v1/*/normalized/session-analysis-v2/*` } },
      },
    ]);
  });

  it("is byte-identical when absent and appends only the six grants when present", () => {
    const bucket = transcriptBucketPolicy({ bucketArn, keyArn: "key", eraserArn });
    expect(transcriptBucketPolicy({ bucketArn, keyArn: "key", eraserArn, sessionAnalysisRunnerRoleArn: undefined })).toEqual(bucket);
    const key = transcriptKeyPolicy(keyArgs);
    expect(transcriptKeyPolicy({ ...keyArgs, sessionAnalysisRunnerRoleArn: undefined })).toEqual(key);

    const withBucket = [
      ...transcriptBucketPolicy({ bucketArn, keyArn: "key", eraserArn, sessionAnalysisRunnerRoleArn: roleArn }),
      ...sessionAnalysisRunnerBucketStatements({ bucketArn, roleArn }),
    ];
    expect(withBucket.filter((s) => s.Sid?.startsWith("SessionAnalysis"))).toEqual(sessionAnalysisRunnerBucketStatements({ bucketArn, roleArn }));
    const deny = withBucket.find((s) => s.Sid === "OnlyTheEraserDeletes")!;
    expect(deny).toHaveProperty("NotResource", [`${bucketArn}/96bulls/*/v1/*/normalized/session-analysis-v2/*`]);
    expect(withBucket.find((s) => s.Sid === "OnlyTheEraserOrAnalysisWorkerDeletesAnalysisResults")!.Condition)
      .toEqual({ ArnNotEquals: { "aws:PrincipalArn": [eraserArn, roleArn] } });

    const withKey = transcriptKeyPolicy({ ...keyArgs, sessionAnalysisRunnerRoleArn: roleArn });
    expect(withKey.Statement.slice(key.Statement.length)).toEqual(sessionAnalysisRunnerKeyStatements({ region: "us-east-1", bucketName, roleArn }));
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
const production = transcriptNames(TRANSCRIPT_STAGE, TRANSCRIPT_ACCOUNT_ID);
const prodRole = `arn:aws:iam::${SESSION_ANALYSIS_RUNNER_ACCOUNT_ID}:role/agent-relay-prod-session-analysis`;
const prodEraser = `arn:aws:iam::${TRANSCRIPT_ACCOUNT_ID}:user/96bulls/transcript-eraser`;
const prodKeyArgs = {
  accountId: TRANSCRIPT_ACCOUNT_ID,
  region: TRANSCRIPT_REGION,
  bucketName: production.bucket,
  writerArn: "writer",
  srvWriterArn: "srv",
  readerArn: "reader",
};
const prodBucket = (runner?: string) => [
  ...transcriptBucketPolicy({ bucketArn: production.bucketArn, keyArn: "key", eraserArn: prodEraser, ...(runner ? { sessionAnalysisRunnerRoleArn: runner } : {}) }),
  ...(runner ? sessionAnalysisRunnerBucketStatements({ bucketArn: production.bucketArn, roleArn: runner }) : []),
];
const prodKey = (runner?: string) => transcriptKeyPolicy({ ...prodKeyArgs, ...(runner ? { sessionAnalysisRunnerRoleArn: runner } : {}) });
const bucketBlock = (live: unknown[], planned: unknown[]) => [
  "  [transcripts] update", "  properties:", `    bucketName: ${production.bucket}`,
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
  const compact = `${summary}\n${RESOURCES.map((resource) => `[${resource}] ${actions[resource] ?? "noop"}`).join("\n")}\n`;
  const detailed = `${summary}\n${RESOURCES.map((resource) => blocks[resource] ? blocks[resource]!.join("\n") : `  [${resource}] ${actions[resource] ?? "noop"}`).join("\n")}\n`;
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
    ...(input === undefined ? {} : { sessionAnalysisRunnerRoleArn: input }),
  });
}

describe("session-analysis runner plan contract", () => {
  const liveBucket = prodBucket();
  const plannedBucket = prodBucket(prodRole);
  const liveKey = prodKey();
  const plannedKey = prodKey(prodRole);
  const grant = {
    transcripts: bucketBlock(liveBucket, plannedBucket),
    "transcripts-key": keyBlock(liveKey, plannedKey),
  };

  it("accepts the exact grant alone for the input role", () => {
    expect(check(grant, { transcripts: "update", "transcripts-key": "update" }, prodRole)).toBe("session analysis runner grants");
  });

  it("accepts exact revocation only after the input is removed", () => {
    const revoke = {
      transcripts: bucketBlock(plannedBucket, liveBucket),
      "transcripts-key": keyBlock(plannedKey, liveKey),
    };
    expect(check(revoke, { transcripts: "update", "transcripts-key": "update" })).toBe("session analysis runner grants");
    expect(() => check(revoke, { transcripts: "update", "transcripts-key": "update" }, prodRole)).toThrow(/removes the grants while/);
  });

  it("refuses an unset or different input, another resource, or a modified grant", () => {
    expect(() => check(grant, { transcripts: "update", "transcripts-key": "update" })).toThrow(/is unset/);
    expect(() => check(grant, { transcripts: "update", "transcripts-key": "update" }, `arn:aws:iam::${SESSION_ANALYSIS_RUNNER_ACCOUNT_ID}:role/other`)).toThrow(/must name exactly/);
    expect(() => check(grant, { transcripts: "update", "transcripts-key": "update", "transcript-reader": "update" }, prodRole)).toThrow(/must deploy alone/);
    const tampered = plannedBucket.map((statement) => statement.Sid === "SessionAnalysisReads" ? { ...statement, Resource: `${production.bucketArn}/*` } : statement);
    expect(() => check({ transcripts: bucketBlock(liveBucket, tampered) }, { transcripts: "update" }, prodRole)).toThrow(/SessionAnalysisReads must be exactly/);
  });
});

