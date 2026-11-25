/**
 * Bucket-owner resource-policy grants for the dedicated session-analysis
 * runner in agentrelay-prod. The role and its identity policy are created in
 * AgentWorkforce/infra; this module owns only the sessions-prod bucket/CMK
 * half. Omit the input for byte-identical, default-off policies.
 */
import { LAYOUT_VERSION } from "../../packages/object-store/src/layout.ts";
import { TENANT_KEY_BASE } from "./tenants.ts";

export const SESSION_ANALYSIS_RUNNER_ACCOUNT_ID = "786098391924";
export const SESSION_ANALYSIS_RUNNER_ROLE_ARN =
  `arn:aws:iam::${SESSION_ANALYSIS_RUNNER_ACCOUNT_ID}:role/agent-relay-prod-session-analysis`;
export const SESSION_ANALYSIS_RESULT_PREFIX = "normalized/session-analysis-v2";
export const SESSION_ANALYSIS_RUNNER_BUCKET_SIDS = [
  "SessionAnalysisInspectsBucket",
  "SessionAnalysisReads",
  "SessionAnalysisWritesResults",
  "SessionAnalysisListsResultVersions",
  "SessionAnalysisDeniesReservedNamespaces",
] as const;
export const SESSION_ANALYSIS_RUNNER_KEY_SIDS = [
  "SessionAnalysisDecryptsViaS3",
  "SessionAnalysisEncryptsViaS3",
  "SessionAnalysisDeniesReservedNamespacesViaS3",
] as const;

export function assertSessionAnalysisRunnerRoleArn(value: unknown): asserts value is string {
  if (value !== SESSION_ANALYSIS_RUNNER_ROLE_ARN)
    throw new Error(
      `transcriptStore.sessionAnalysisRunnerRoleArn must be exactly ${SESSION_ANALYSIS_RUNNER_ROLE_ARN} (omit for none)`,
    );
}

const canonical = (bucketArn: string) => `${bucketArn}/${TENANT_KEY_BASE}/*/${LAYOUT_VERSION}/*`;
const results = (bucketArn: string) => `${canonical(bucketArn)}/${SESSION_ANALYSIS_RESULT_PREFIX}/*`;
const reserved = (bucketArn: string) => `${bucketArn}/${TENANT_KEY_BASE}/.*`;

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
    {
      // IAM wildcards cross '/'. This explicit deny keeps every current and
      // future reserved dot namespace out of the broad canonical read glob.
      Sid: "SessionAnalysisDeniesReservedNamespaces",
      Effect: "Deny" as const,
      Principal,
      Action: ["s3:*"],
      Resource: reserved(i.bucketArn),
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
    {
      Sid: "SessionAnalysisDeniesReservedNamespacesViaS3",
      Effect: "Deny" as const,
      Principal,
      Action: ["kms:Decrypt", "kms:Encrypt", "kms:GenerateDataKey"],
      Resource: "*",
      Condition: exact(`${TENANT_KEY_BASE}/.*`),
    },
  ];
}
