/** Resource-policy grants for a sessions worker that runs in ANOTHER account (decision 0007: the
 * summary worker on agentrelay EKS, reading this stack's transcripts bucket). The role, its trust
 * and its own identity policy live in that account (infra terraform/examples/aws-sessions-worker-cross-account);
 * cross-account S3 and KMS access also needs the bucket owner's side, which this stack owns, so the
 * statements are declared here rather than hand-merged (a hand-merged statement is dropped by the next deploy).
 *
 * Same grant as the pipeline's own worker (hardening.ts workerPolicy): list, read session objects,
 * write summaries, the bucket key through S3 only. Never delete (OnlyTheEraserDeletes still denies it),
 * never old versions, never outside `96bulls/<org>/v1/`.
 */
import { TENANT_KEY_BASE } from "./tenants.ts";

export const CROSS_ACCOUNT_WORKER_BUCKET_SIDS = ["SessionsWorkerLists", "SessionsWorkerReads", "SessionsWorkerWritesSummaries"] as const;
export const CROSS_ACCOUNT_WORKER_KEY_SIDS = ["SessionsWorkerDecryptsViaS3", "SessionsWorkerEncryptsSummariesViaS3"] as const;
export const CROSS_ACCOUNT_WORKER_SIDS: readonly string[] = [...CROSS_ACCOUNT_WORKER_BUCKET_SIDS, ...CROSS_ACCOUNT_WORKER_KEY_SIDS];

const ROLE_ARN = /^arn:aws:iam::(\d{12}):role\/(?:[A-Za-z0-9_+=,.@-]+\/)*[A-Za-z0-9_+=,.@-]{1,64}$/;

/** An exact named IAM role in another 12-digit account (partition aws). Returns that account. */
export function assertCrossAccountWorkerRoleArn(value: unknown, bucketAccountId: string): string {
  const m = typeof value === "string" && value.length <= 1024 ? ROLE_ARN.exec(value) : null;
  if (!m || !/^\d{12}$/.test(bucketAccountId) || m[1] === bucketAccountId)
    throw new Error("transcriptStore.crossAccountWorkerRoleArn must be an exact named IAM role in another account (omit for none)");
  return m[1]!;
}

const sessionObjects = (bucketArn: string) => `${bucketArn}/${TENANT_KEY_BASE}/*/v1/*`;
const summaryObjects = (bucketArn: string) => `${bucketArn}/${TENANT_KEY_BASE}/*/v1/*/summary/*`;

export function crossAccountWorkerBucketStatements(i: { bucketAccountId: string; bucketArn: string; roleArn: string }) {
  assertCrossAccountWorkerRoleArn(i.roleArn, i.bucketAccountId);
  const Principal = { AWS: i.roleArn };
  return [
    // Unconditioned: S3 answers 404 (not 403) for a missing key only with ListBucket.
    { Sid: "SessionsWorkerLists", Effect: "Allow" as const, Principal, Action: "s3:ListBucket", Resource: i.bucketArn },
    { Sid: "SessionsWorkerReads", Effect: "Allow" as const, Principal, Action: "s3:GetObject", Resource: sessionObjects(i.bucketArn) },
    { Sid: "SessionsWorkerWritesSummaries", Effect: "Allow" as const, Principal, Action: "s3:PutObject", Resource: summaryObjects(i.bucketArn) },
  ];
}

export function crossAccountWorkerKeyStatements(i: { bucketAccountId: string; region: string; bucketName: string; roleArn: string }) {
  const workerAccountId = assertCrossAccountWorkerRoleArn(i.roleArn, i.bucketAccountId);
  const Principal = { AWS: i.roleArn };
  const bucketArn = `arn:aws:s3:::${i.bucketName}`;
  const via = { "kms:ViaService": `s3.${i.region}.amazonaws.com`, "kms:CallerAccount": workerAccountId };
  return [
    {
      Sid: "SessionsWorkerDecryptsViaS3", Effect: "Allow" as const, Principal, Action: "kms:Decrypt", Resource: "*",
      Condition: { StringEquals: via, StringLike: { "kms:EncryptionContext:aws:s3:arn": sessionObjects(bucketArn) } },
    },
    {
      Sid: "SessionsWorkerEncryptsSummariesViaS3", Effect: "Allow" as const, Principal, Action: "kms:GenerateDataKey", Resource: "*",
      Condition: { StringEquals: via, StringLike: { "kms:EncryptionContext:aws:s3:arn": summaryObjects(bucketArn) } },
    },
  ];
}
