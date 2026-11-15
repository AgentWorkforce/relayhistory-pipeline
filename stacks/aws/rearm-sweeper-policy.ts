/** Resource-policy grants for the rearm sweeper role (transcriptStore.rearmSweeperRoleArn), default off.
 * The sweeper re-arms stalled admissions: it lists the bucket and its versions, reads the admission
 * ledgers, fences and canonical manifests, writes its own progress markers, and deletes old versions
 * of canonical session objects. The role, its trust and its identity policy are created outside the
 * stack. The transcripts bucket and key policies are owned here, so the bucket owner's side is declared
 * here too: a hand-merged statement is dropped by the next deploy that updates either policy.
 *
 * The statements are the six that were first applied to prod by hand, byte-for-byte (Sid, Effect,
 * Principal, Action, Resource, Condition). Never a delete under a dot prefix (`96bulls/.*`: the upload
 * inbox, the ledgers), whatever any other statement allows. The hand edit also exempted the role from
 * the bucket's `OnlyTheEraserDeletes` deny (the last `aws:PrincipalArn` entry, after the eraser and the
 * GC); transcriptBucketPolicy renders that exemption when the input is set.
 */
import { TENANT_KEY_BASE } from "./tenants.ts";

export const REARM_SWEEPER_BUCKET_SIDS = [
  "RearmSweeperLists", "RearmSweeperReads", "RearmSweeperWritesProgress", "RearmSweeperDeletesCanonicalVersions", "RearmSweeperNeverDotPrefixes",
] as const;
export const REARM_SWEEPER_KEY_SIDS = ["RearmSweeperReadsViaS3"] as const;
export const REARM_SWEEPER_SIDS: readonly string[] = [...REARM_SWEEPER_BUCKET_SIDS, ...REARM_SWEEPER_KEY_SIDS];

// The same exact-role shape as cross-account-worker-policy.ts (optional path, IAM name charset).
const ROLE_ARN = /^arn:aws:iam::(\d{12}):role\/(?:[A-Za-z0-9_+=,.@-]+\/)*[A-Za-z0-9_+=,.@-]{1,64}$/;

/** An exact named IAM role in a 12-digit account (partition aws). Returns that account. */
export function assertRearmSweeperRoleArn(value: unknown): string {
  const m = typeof value === "string" && value.length <= 1024 ? ROLE_ARN.exec(value) : null;
  if (!m) throw new Error("transcriptStore.rearmSweeperRoleArn must be an exact named IAM role ARN (omit for none)");
  return m[1]!;
}

export function rearmSweeperBucketStatements(i: { bucketArn: string; roleArn: string }) {
  assertRearmSweeperRoleArn(i.roleArn);
  const Principal = { AWS: i.roleArn };
  const base = `${i.bucketArn}/${TENANT_KEY_BASE}`;
  const inbox = `${base}/.admission-inbox/*`;
  return [
    { Sid: "RearmSweeperLists", Effect: "Allow" as const, Principal, Action: ["s3:ListBucket", "s3:ListBucketVersions"], Resource: i.bucketArn },
    {
      Sid: "RearmSweeperReads", Effect: "Allow" as const, Principal, Action: "s3:GetObject",
      Resource: [`${inbox}/ledger/rearm/*`, `${inbox}/ledger/owners/*`, `${inbox}/upload-fences.json`, `${base}/.ledger/rearm-sweeper/*`, `${base}/*/v1/ws=*/manifest.json`],
    },
    {
      Sid: "RearmSweeperWritesProgress", Effect: "Allow" as const, Principal, Action: "s3:PutObject",
      Resource: [`${inbox}/ledger/rearm/*.done/*`, `${inbox}/ledger/rearm/*.corrupt/*`, `${base}/.ledger/rearm-sweeper/*`],
    },
    {
      Sid: "RearmSweeperDeletesCanonicalVersions", Effect: "Allow" as const, Principal, Action: "s3:DeleteObjectVersion",
      Resource: [`${base}/*/v1/ws=*`, `${base}/*/live-sessions/v1/ws=*`],
    },
    { Sid: "RearmSweeperNeverDotPrefixes", Effect: "Deny" as const, Principal, Action: ["s3:DeleteObject", "s3:DeleteObjectVersion"], Resource: `${base}/.*` },
  ];
}

/** Decrypt and data keys through S3 in the bucket's region, for objects under `96bulls/`, from the role's own account. */
export function rearmSweeperKeyStatement(i: { region: string; bucketName: string; roleArn: string }) {
  const callerAccount = assertRearmSweeperRoleArn(i.roleArn);
  return {
    Sid: "RearmSweeperReadsViaS3", Effect: "Allow" as const, Principal: { AWS: i.roleArn }, Action: ["kms:Decrypt", "kms:GenerateDataKey"], Resource: "*",
    Condition: {
      StringEquals: { "kms:ViaService": `s3.${i.region}.amazonaws.com`, "kms:CallerAccount": callerAccount },
      StringLike: { "kms:EncryptionContext:aws:s3:arn": `arn:aws:s3:::${i.bucketName}/${TENANT_KEY_BASE}/*` },
    },
  };
}
