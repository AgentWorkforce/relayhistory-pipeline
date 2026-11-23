/**
 * Bucket-owner resource-policy grants for the session-analysis runner in the
 * agentrelay-prod EKS account. The role and its identity policy are created by
 * AgentWorkforce/infra; this stack owns the sessions-prod bucket and CMK sides.
 *
 * Default off: omit transcriptStore.sessionAnalysisRunnerRoleArn and neither
 * policy contains any of these statements.
 */
import { LAYOUT_VERSION } from "../../packages/object-store/src/layout.ts";
import { TENANT_KEY_BASE } from "./tenants.ts";

export const SESSION_ANALYSIS_RUNNER_ACCOUNT_ID = "786098391924";
export const SESSION_ANALYSIS_RESULT_PREFIX = "normalized/session-analysis-v2";
export const SESSION_ANALYSIS_RUNNER_BUCKET_SIDS = [
  "SessionAnalysisInspectsBucket",
  "SessionAnalysisReads",
  "SessionAnalysisWritesResults",
  "SessionAnalysisListsResultVersions",
] as const;
export const SESSION_ANALYSIS_RUNNER_KEY_SIDS = [
  "SessionAnalysisDecryptsViaS3",
  "SessionAnalysisEncryptsViaS3",
] as const;

const ROLE_ARN = new RegExp(
  `^arn:aws:iam::${SESSION_ANALYSIS_RUNNER_ACCOUNT_ID}:role/(?:[A-Za-z0-9_+=,.@-]+/)*[A-Za-z0-9_+=,.@-]{1,64}$`,
);

export function assertSessionAnalysisRunnerRoleArn(value: unknown): asserts value is string {
  if (typeof value !== "string" || value.length > 1024 || !ROLE_ARN.test(value))
    throw new Error(
      `transcriptStore.sessionAnalysisRunnerRoleArn must be an exact named IAM role ARN in account ${SESSION_ANALYSIS_RUNNER_ACCOUNT_ID} (omit for none)`,
    );
}

const canonical = (bucketArn: string) => `${bucketArn}/${TENANT_KEY_BASE}/*/${LAYOUT_VERSION}/*`;
const results = (bucketArn: string) => `${canonical(bucketArn)}/${SESSION_ANALYSIS_RESULT_PREFIX}/*`;

export function sessionAnalysisRunnerBucketStatements(i: { bucketArn: string; roleArn: string }) {
  assertSessionAnalysisRunnerRoleArn(i.roleArn);
  const Principal = { AWS: i.roleArn };
  return [
    {
      Sid: "SessionAnalysisInspectsBucket",
      Effect: "Allow" as const,
      Principal,
      Action: ["s3:ListBucket", "s3:GetBucketVersioning"],
      Resource: i.bucketArn,
    },
    {
      Sid: "SessionAnalysisReads",
      Effect: "Allow" as const,
      Principal,
      Action: ["s3:GetObject"],
      Resource: canonical(i.bucketArn),
    },
    {
      Sid: "SessionAnalysisWritesResults",
      Effect: "Allow" as const,
      Principal,
      Action: ["s3:PutObject", "s3:DeleteObject", "s3:DeleteObjectVersion"],
      Resource: results(i.bucketArn),
    },
    {
      Sid: "SessionAnalysisListsResultVersions",
      Effect: "Allow" as const,
      Principal,
      Action: ["s3:ListBucketVersions"],
      Resource: i.bucketArn,
      Condition: { StringLike: { "s3:prefix": `${TENANT_KEY_BASE}/*/${LAYOUT_VERSION}/*/${SESSION_ANALYSIS_RESULT_PREFIX}/*` } },
    },
  ];
}

export function sessionAnalysisRunnerKeyStatements(i: { region: string; bucketName: string; roleArn: string }) {
  assertSessionAnalysisRunnerRoleArn(i.roleArn);
  const Principal = { AWS: i.roleArn };
  const exact = (glob: string) => ({
    StringEquals: {
      "kms:ViaService": `s3.${i.region}.amazonaws.com`,
      "kms:CallerAccount": SESSION_ANALYSIS_RUNNER_ACCOUNT_ID,
    },
    StringLike: { "kms:EncryptionContext:aws:s3:arn": `arn:aws:s3:::${i.bucketName}/${glob}` },
  });
  return [
    {
      Sid: "SessionAnalysisDecryptsViaS3",
      Effect: "Allow" as const,
      Principal,
      Action: ["kms:Decrypt"],
      Resource: "*",
      Condition: exact(`${TENANT_KEY_BASE}/*/${LAYOUT_VERSION}/*`),
    },
    {
      Sid: "SessionAnalysisEncryptsViaS3",
      Effect: "Allow" as const,
      Principal,
      Action: ["kms:Encrypt", "kms:GenerateDataKey"],
      Resource: "*",
      Condition: exact(`${TENANT_KEY_BASE}/*/${LAYOUT_VERSION}/*/${SESSION_ANALYSIS_RESULT_PREFIX}/*`),
    },
  ];
}

