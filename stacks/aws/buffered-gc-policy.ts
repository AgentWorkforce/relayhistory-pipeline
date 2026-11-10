/** Resource-policy continuity for a separately managed buffered GC role.
 * No role, runtime, credentials or permissions outside buffered storage are created here.
 */
import { TENANT_KEY_BASE } from "./tenants.ts";

export function assertBufferedGcRoleArn(value: unknown, accountId: string): asserts value is string {
  if (
    !/^\d{12}$/.test(accountId) ||
    typeof value !== "string" ||
    value.length > 1024 ||
    !new RegExp(`^arn:aws:iam::${accountId}:role/(?:[A-Za-z0-9_+=,.@-]+/)*[A-Za-z0-9_+=,.@-]{1,64}$`).test(value)
  ) throw new Error("transcriptStore.bufferedGcRoleArn must be an exact named IAM role in this account; omission cannot remove an active GC grant");
}

export const bufferedGcObjectArn = (bucketArn: string) =>
  `${bucketArn}/${TENANT_KEY_BASE}/*/buffered-batches/v1/ws=*/m=srv_*/b=*.staged.ndjson.zst`;
export const bufferedGcCanonicalArn = (bucketArn: string) => `${bucketArn}/${TENANT_KEY_BASE}/*/upload-generations/*`;

export function bufferedGcBucketStatements(i: { accountId: string; bucketArn: string; roleArn: string }) {
  assertBufferedGcRoleArn(i.roleArn, i.accountId);
  const principal = { ArnEquals: { "aws:PrincipalArn": i.roleArn } };
  return [
    {
      Sid: "BufferedGcCannotDeleteOutsideBuffer", Effect: "Deny" as const, Principal: "*",
      Action: ["s3:DeleteObject", "s3:DeleteObjectVersion"], NotResource: bufferedGcObjectArn(i.bucketArn), Condition: principal,
    },
    {
      Sid: "BufferedGcCannotPlainDeleteBarrier", Effect: "Deny" as const, Principal: "*",
      Action: "s3:DeleteObject", Resource: `${i.bucketArn}/*`, Condition: principal,
    },
    {
      // IAM * includes '/': a canonical family named buffered-batches overlaps the allow.
      Sid: "BufferedGcCannotAccessCanonical", Effect: "Deny" as const, Principal: "*",
      Action: "s3:*", Resource: bufferedGcCanonicalArn(i.bucketArn), Condition: principal,
    },
    {
      Sid: "BufferedGcCannotListCanonicalVersions", Effect: "Deny" as const, Principal: "*",
      Action: "s3:ListBucketVersions", Resource: i.bucketArn,
      Condition: { ...principal, StringLike: { "s3:prefix": `${TENANT_KEY_BASE}/*/upload-generations/*` } },
    },
  ];
}

export function bufferedGcKeyStatements(i: { accountId: string; region: string; bucketName: string; roleArn: string }) {
  assertBufferedGcRoleArn(i.roleArn, i.accountId);
  const principal = { AWS: i.roleArn };
  const bucketArn = `arn:aws:s3:::${i.bucketName}`;
  return [
    {
      Sid: "BufferedGcDataViaS3", Effect: "Allow" as const, Principal: principal,
      Action: ["kms:Decrypt", "kms:GenerateDataKey"], Resource: "*",
      Condition: {
        StringEquals: { "kms:CallerAccount": i.accountId, "kms:ViaService": `s3.${i.region}.amazonaws.com` },
        // Keep the reviewed six-literal-character KMS wildcard prefix; S3 IAM pins the full suffix.
        StringLike: { "kms:EncryptionContext:aws:s3:arn": `${bucketArn}/${TENANT_KEY_BASE}/*/buffered-batches/v1/ws=*/m=srv_*` },
      },
    },
    {
      Sid: "BufferedGcNeverCanonicalCrypto", Effect: "Deny" as const, Principal: principal,
      Action: ["kms:Decrypt", "kms:GenerateDataKey"], Resource: "*",
      Condition: { StringLike: { "kms:EncryptionContext:aws:s3:arn": bufferedGcCanonicalArn(bucketArn) } },
    },
  ];
}
