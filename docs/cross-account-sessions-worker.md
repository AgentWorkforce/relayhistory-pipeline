# Cross-account sessions worker grants

The summary worker and status worker can run on the agentrelay EKS clusters (decision 0007), in a
different AWS account from the transcripts bucket. Their role, its Pod Identity trust and its own
identity policy live in that account (infra `terraform/examples/aws-sessions-worker-cross-account`).
Cross-account access also needs the bucket owner's side: the transcripts bucket policy and the
transcripts key policy. This stack owns both policies, so the statements are declared here. A
statement merged by hand would be dropped by the next stack deploy.

## Input

`transcriptStore.crossAccountWorkerRoleArn` in the stage inputs: the worker role's exact ARN,
`arn:aws:iam::<other account>:role/[path/]<name>`. Omitted, both policies are byte-for-byte as
before. A role in the bucket's own account, a wildcard, a user, an assumed-role session or any
other shape refuses at input validation.

Set, it appends (`stacks/aws/cross-account-worker-policy.ts`):

| Policy | Sid | Grant |
| --- | --- | --- |
| bucket | `SessionsWorkerLists` | `s3:ListBucket` on the bucket (404, not 403, for a missing key) |
| bucket | `SessionsWorkerReads` | `s3:GetObject` on `96bulls/*/v1/*` |
| bucket | `SessionsWorkerWritesSummaries` | `s3:PutObject` on `96bulls/*/v1/*/summary/*` |
| key | `SessionsWorkerDecryptsViaS3` | `kms:Decrypt` through S3 in the bucket's region, for objects under `96bulls/*/v1/*`, from the worker's account only |
| key | `SessionsWorkerEncryptsSummariesViaS3` | `kms:GenerateDataKey` the same way, for `…/summary/*` only |

These match the pipeline's own worker grant. There is no delete, no old-version access and no
bucket change. `OnlyTheEraserDeletes` and the upload-inbox wall still deny the worker.

## Deploying it

The deploy plan contract accepts it as its own phase, **cross-account worker grants**. In that phase,
everything is noop except the `transcripts` and/or `transcripts-key` updates. Each update's detailed
diff must leave every other statement byte-identical, and add exactly that policy's worker Sids. To
revoke, unset the input: the same phase then accepts removing exactly those Sids. A plan that mixes
this with any other change is refused, so deploy it alone.

Order:
1. Create the role in the worker's account. The bucket and key policies reject a principal that does not exist yet.
2. Set the input, capture the plan, and deploy.
3. Start the worker.
