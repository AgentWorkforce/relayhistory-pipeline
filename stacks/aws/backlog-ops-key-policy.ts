/** Key-policy grants for the guarded `backlog ops` workflow (decision 0005: the fence backfill, the
 * accepted-stage export and the buffered drain, run from GitHub, never from an operator's machine).
 *
 * The two roles, their GitHub OIDC trust and their identity policies live OUTSIDE this stack (created by
 * hand in the transcript account with Khaliq's OK, 2026-11-13; docs/backlog-ops-workflow.md). The stack does
 * not declare them: the plan contract refuses adoption and imports, and creating them here would collide
 * with the live roles. The transcripts key policy delegates no key use to IAM, though, so the roles also need
 * statements on the key, and this stack owns that policy: a hand-merged statement is dropped by the next
 * deploy that updates the key. So the two key statements are declared here, from the role ARNs.
 *
 * The bucket policy needs nothing: it only denies (insecure transport, TLS < 1.2, non-KMS or other-key
 * writes, and deletes by anyone but the eraser and GC), and the roles' identity policies grant the rest.
 *
 * The statements are byte-for-byte the live ones, so declaring them converges state on what is live.
 */
export const BACKLOG_OPS_KEY_SIDS = ["BacklogOpsReadDecryptsViaS3", "BacklogOpsWriteEncryptsViaS3"] as const;

export interface BacklogOpsRoles { readRoleArn: string; writeRoleArn: string }

/** The only accepted role ARNs: same account, the stage's exact names. */
export function backlogOpsRoleArns(accountId: string, stage: string): BacklogOpsRoles {
  return {
    readRoleArn: `arn:aws:iam::${accountId}:role/96bulls-${stage}-backlog-ops-read`,
    writeRoleArn: `arn:aws:iam::${accountId}:role/96bulls-${stage}-backlog-ops-write`,
  };
}

const MESSAGE = "transcriptStore.backlogOpsRoles must be exactly { readRoleArn, writeRoleArn } naming this account's 96bulls-<stage>-backlog-ops-read and -write roles (omit for none)";

/** Exactly the two keys, each exactly the stage's role ARN in this account. Fails closed. */
export function assertBacklogOpsRoles(value: unknown, accountId: string, stage: string): BacklogOpsRoles {
  const want = backlogOpsRoleArns(accountId, stage);
  if (value === null || typeof value !== "object" || Array.isArray(value)) throw new Error(MESSAGE);
  const keys = Object.keys(value);
  const v = value as Record<string, unknown>;
  if (!/^\d{12}$/.test(accountId) || !/^[a-z0-9-]{1,40}$/.test(stage) || keys.length !== 2
    || v.readRoleArn !== want.readRoleArn || v.writeRoleArn !== want.writeRoleArn) throw new Error(MESSAGE);
  return want;
}

export function backlogOpsKeyStatements(i: { accountId: string; stage: string; region: string; roles: BacklogOpsRoles }) {
  const roles = assertBacklogOpsRoles(i.roles, i.accountId, i.stage);
  const Condition = { StringEquals: { "kms:ViaService": `s3.${i.region}.amazonaws.com` } };
  return [
    { Sid: "BacklogOpsReadDecryptsViaS3", Effect: "Allow" as const, Principal: { AWS: roles.readRoleArn }, Action: "kms:Decrypt", Resource: "*", Condition },
    { Sid: "BacklogOpsWriteEncryptsViaS3", Effect: "Allow" as const, Principal: { AWS: roles.writeRoleArn }, Action: ["kms:GenerateDataKey", "kms:Decrypt"], Resource: "*", Condition },
  ];
}
