# Production transcript store (`aws-sessions-prod`)

The S3 bucket that RelayHistory transcript content lands in, in the
`sessions-prod` account (`941433935742`), region `us-east-1`. Code:
`stacks/aws/transcript-store.ts`. Tests: `stacks/aws/__tests__/transcript-store.test.ts`
and the `transcripts:` entries in `scripts/aws-hardening-mutations.ts`.

Sources: decision 0001 (production AWS is S3 only), the migration plan
(`docs/relayhistory-supabase-migration-plan.md` on #90: S5, S6, P9, K-2, K-33,
K-45, section 6 regional boundaries).

Every step that changes the account (state bucket, apply, access keys,
Cloudflare secrets, teardown) needs Khaliq's explicit OK for that exact step,
recorded in the inputs' `approval` record where the stack checks it
(`stacks/aws/deploy-guard.ts`).

## What the stage creates

| Resource | Name | Settings |
| --- | --- | --- |
| S3 bucket | `96bulls-aws-sessions-prod-transcripts-941433935742` | Versioned. SSE-KMS with its own CMK, **S3 Bucket Key off**, SSE-C blocked. Block Public Access (all four). BucketOwnerEnforced. Object Lock **off**. Server access logs to the log bucket. Lifecycle: noncurrent versions expire after 1 day, expired delete markers removed, incomplete multipart uploads aborted after 1 day. |
| Bucket policy | (on the bucket) | Deny non-TLS; deny TLS < 1.2; deny `DeleteObject`/`DeleteObjectVersion` to everyone but the eraser; deny a PUT whose encryption header is anything but SSE-KMS naming this key (including `aws:kms` with no key id, which S3 would encrypt with the AWS managed `aws/s3` key). A PUT with no encryption header gets the bucket default. No Allow statements. |
| KMS key + alias | `alias/96bulls-aws-sessions-prod-transcripts` | Rotation on, 30-day deletion window. Root administers but cannot use. Writer and srv-writer: `GenerateDataKey` + `Decrypt`; reader: `Decrypt`; all only via `s3.us-east-1.amazonaws.com`, from this account, for objects under `96bulls/` in this bucket (encryption context `aws:s3:arn`). |
| S3 bucket | `96bulls-aws-sessions-prod-access-logs-941433935742` | Versioned, SSE-S3 (server access logs cannot be delivered to an SSE-KMS bucket), Block Public Access, owner-enforced, TLS only. Accepts logs only from the transcripts bucket. Logs expire after 90 days. |
| CloudTrail | `96bulls-aws-sessions-prod-trail` | Multi-region, log file validation, own KMS key (`alias/96bulls-aws-sessions-prod-trail`) and versioned bucket (`96bulls-aws-sessions-prod-trail-941433935742`, 365-day retention). Management events plus **S3 data events for the transcripts bucket only**. Both bucket service grants require the exact trail `SourceArn` and `SourceAccount`. Trail-log decrypt/re-encrypt is absent unless `transcriptStore.auditPrincipalArn` names one IAM role in this account. |
| IAM user | `/96bulls/96bulls-aws-sessions-prod-transcript-writer` | The RelayHistory Worker. `ListBucket`; `GetObject` on `96bulls/*/v1/*`; `PutObject` on `…/raw/*`, `…/manifest.json`, `…/summary/*`. Explicit deny on delete, old versions, bucket configuration. |
| IAM user | `/96bulls/96bulls-aws-sessions-prod-transcript-srv-writer` | The relayhistory-api Worker's live writer. `GetObject`/`PutObject` only on `96bulls/*/v1/ws=*/m=srv_*`, `96bulls/*/live-sessions/*`, `96bulls/*/live-batches/*`. A PUT must send `x-amz-server-side-encryption: aws:kms` and a key id in this account and region; the bucket policy pins it to the transcripts key. `GenerateDataKey`/`Decrypt` via S3 on the key's alias. `ListBucket` on the bucket, as the writer has, so a missing key answers 404 (the Worker's create-only pointer, manifest and batch-marker reads need it; HEAD/GET carry no `s3:prefix`, so it cannot be prefix-scoped). No version listing, no delete, no old versions, no bucket configuration. Added after the first apply: the plan contract's `srv-writer addition` phase is exactly `transcript-srv-writer` create plus `transcripts-key` update. |
| IAM user | `/96bulls/96bulls-aws-sessions-prod-transcript-reader` | The history indexer. `ListBucket`; `GetObject` on `96bulls/*/v1/*`. Explicit deny on put, delete, old versions. |
| IAM user | `/96bulls/96bulls-aws-sessions-prod-transcript-eraser` | The erasure job. `ListBucket`/`ListBucketVersions` under `96bulls/`; `DeleteObject`/`DeleteObjectVersion` on `96bulls/*`. Explicit deny on reading or writing content. No key use. |
| Budget | `96bulls-aws-sessions-prod-monthly` | Alarm at 80% actual and 100% forecast of `budgetUsd`. |

Nothing else: no Locker, ECR, GitHub OIDC, EKS or Supabase (the inputs refuse
them), and no access keys (see "Credentials").

Key layout (the 96Bulls pipeline's org-prefix mode, `stacks/aws/tenants.ts`):

```text
96bulls/<org>/v1/ws=<workspace>/m=<machine>/h=<harness>/d=<yyyy>/<mm>/<dd>/s=<session>/
    manifest.json | raw/<seq>-<offset>-<sha8>.<fmt>.zst | summary/{short,long}.json.enc
```

The Worker runs with `PIPELINE_KEY_PREFIX=96bulls` and
`PIPELINE_ORG_FROM_PREFIX=true`, so its prefix is `96bulls/<caller's org>`
(relayhistory-cloud `prefixAllowed`).

## Design choices

### One CMK with per-object data keys, not a CMK per workspace

P9 asked for a key per workspace. This stage uses **one dedicated CMK for the
bucket with S3 Bucket Keys off**. S3 generates a separate data key for every
object, and the encryption context is the object ARN, which carries
`96bulls/<org>/v1/ws=<workspace>/`. This provides per-object KMS request
attribution and avoids sharing one cached S3 Bucket Key. It is **not tenant
cryptographic isolation**: every tenant's data key is protected by the same
CMK, and the writer/reader principals span every tenant prefix. Compromise or
disablement of that CMK affects all tenants.

Per-workspace CMKs were rejected for now:

- **Erasure.** They do not help the K-45 SLA. A CMK cannot be deleted in under
  7 days, so crypto-shredding cannot meet 48 h. Disabling a key is immediate but
  reversible, so it is not a purge. Version deletion is needed either way.
- **Privilege.** Keys per workspace must be created when a workspace appears.
  The Worker would need `kms:CreateKey` and `PutKeyPolicy`, far more than it has
  now, or a separate provisioning service, which 0001 rules out in AWS.
- **Worker change.** The Worker would have to send
  `x-amz-server-side-encryption-aws-kms-key-id` per PUT, mapping workspace to
  key. Production leaves `PIPELINE_RAW_S3_SSE` unset so the bucket's dedicated
  default CMK applies; `aws:kms` without a key id selects `aws/s3` and is denied.
- **Cost.** 289 workspaces × $1/month, plus up to $2/month per key after
  rotations, roughly $290-870/month and growing, against about $1-3 for one key.
  (The 100,000-keys-per-account quota is not the limit; operations are.)

KMS request cost with Bucket Keys off is one request per PUT and per GET:
$0.03 per 10,000. The whole export (about 485 GiB in 4 MiB chunks, about
120,000 objects plus manifests) is under $1.

**Upgrade path.** A workspace that later needs its own key (customer-managed
keys, BYOK) gets a CMK and a bucket-policy/IAM change, and the Worker names that
key for its prefix. Existing objects need a copy-in-place to re-encrypt, so
decide this before the bulk export if any workspace needs it. **This replaces
P9's "key per workspace" and needs Khaliq's confirmation before the export
(A6).** The bucket can be created first either way: new writes can switch keys.

### Credential model: IAM users with access keys

A Cloudflare Worker cannot assume an IAM role. Options considered:

| Option | Verdict |
| --- | --- |
| **IAM user with a scoped access key** (chosen) | Works with the Worker's adapter as is (`PIPELINE_RAW_S3_ACCESS_KEY_ID`/`SECRET_ACCESS_KEY`). The blast radius is the user policy: put/get under `96bulls/`, no delete, no old versions, the key only via S3 for this bucket. Every use is in CloudTrail data events with the access key id. Rotated by two-key overlap (below). |
| IAM Roles Anywhere | The Worker still holds a long-lived secret (the X.509 private key) that can mint credentials at will, so a leak has the same reach until the certificate is revoked. It adds a CA (ACM PCA is about $400/month, or a self-run CA key to protect), a trust anchor, a profile, a role, and custom `CreateSession` signing in the Worker (no SDK there). Not worth it at this stage. |
| Presigned-URL broker | The broker needs AWS credentials itself. In AWS (Lambda) it breaks 0001 (production AWS is S3 only); in Cloudflare it holds the same long-lived key, plus an extra hop on every write. No gain. |

A `aws:SourceIp` condition is not used: Cloudflare's egress ranges are shared by
every Cloudflare customer and change over time, so it would add outage risk
without stopping an attacker who also runs a Worker.

Separate users per job (writer, reader, eraser) follow S5. The Worker holds only
the writer key. Reader and eraser keys are created only when their consumers
exist (the history indexer; the erasure job), each with its own approval.

### Residual risks (accepted, with detection)

- **The writer key covers every org.** One Worker serves every tenant, so its
  key can put under any `96bulls/<org>/` prefix. Tenant isolation for writes is
  the Worker's: the org comes from the verified RelayAuth token and
  `prefixAllowed` binds the key prefix to it. Per-org IAM users would mean one
  key per org held by the same Worker, which gains nothing. A leaked key cannot
  delete or read old versions. An overwrite leaves the previous version
  noncurrent for about a day (the erasure-SLA trade-off), and every PUT is in
  the CloudTrail data events with its access key id. Follow-up: once the
  Worker's conditional headers are confirmed end to end (it sends
  `If-None-Match: *` on creates, relayhistory-cloud `object-store/src/s3.ts`),
  make `raw/` and `summary/` create-only in the bucket policy
  (`s3:if-none-match`), so even the writer cannot overwrite a chunk.
- **Key administrators can grant themselves decryption.** The root statement
  keeps `kms:PutKeyPolicy` so the key cannot be locked out; the
  `AdministratorAccess` SSO role can therefore edit the policy. That is a
  logged, deliberate act (`PutKeyPolicy` is a CloudTrail management event), not
  a silent read path. Review `PutKeyPolicy` and `CreateGrant` events on this key
  with the trail.

### Erasure (K-45: logical delete immediately, physical purge within 48 h)

- The **erasure job** (eraser user) lists every version under the session's or
  workspace's prefix and deletes each with `DeleteObjectVersion`. That is a
  permanent, immediate delete, well inside 48 h.
- The **lifecycle rule is the backstop** for everything else that leaves old
  bytes behind: superseded manifest versions and plain deletes that only add a
  delete marker. S3 makes a noncurrent version due `NoncurrentDays` after it
  became noncurrent, rounded up to the next 00:00 UTC, so with 1 day it is due
  within 48 h. S3 runs lifecycle asynchronously, so removal can trail the due
  time; that is why the erasure job deletes versions itself instead of relying
  on lifecycle.
- Writer and reader are explicitly denied `GetObjectVersion` and
  `ListBucketVersions`, so a logically deleted object cannot be read back
  through them before it is purged.
- **Object Lock is off**: retention would make erasure impossible.
- **Tails** (not transcript content): server access logs (90 days) and CloudTrail
  (365 days) contain object keys, i.e. org, workspace, machine and session ids.
  Record them as part of Q27 (backup and log tails).

## Prerequisites (each needs its own OK)

1. **Alchemy state bucket.** The account has none. `plan --capture` requires
   `alchemy-state-941433935742-us-east-1-an` to exist and be versioned (it
   detects any write during the capture from the version listing). Alchemy
   creates and hardens it (versioning, encryption, Block Public Access,
   owner-enforced) on first use: run `bin/96bulls plan aws-sessions-prod` once
   without `--capture`. That plain plan writes only the state bucket (and a
   lease object), never a stack resource. (Done 2026-10-31.)
2. **Account-level S3 Block Public Access** (recommended, not in the stack:
   Alchemy has no resource for it). The account has none set today:

   ```sh
   aws s3control put-public-access-block --account-id 941433935742 \
     --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
   ```

## Apply

```sh
export AWS_PROFILE=sessions-prod AWS_REGION=us-east-1 NINETY_SIX_BULLS_CONFIRM_ACCOUNT=941433935742
aws sts get-caller-identity --query Account --output text          # 941433935742
mkdir -p .96bulls/aws-sessions-prod
cp stacks/aws/examples/aws-sessions-prod.inputs.example.json .96bulls/aws-sessions-prod/aws-inputs.json
# fill the approval record (decision, date, quote verbatim); leave capture for after the capture
bin/96bulls plan aws-sessions-prod --capture     # read only; docs/plans/aws-sessions-prod.*
```

The capture and deploy gates accept exactly four operation shapes: all 12
resources created (initial); the known three-resource continuation after IAM
principal propagation (`transcripts`, `transcripts-key`,
`transcripts-key-alias`); the one post-merge policy remediation described
below (`trail-key` and `trail-logs` update, the other ten resources `noop`);
or all 12 resources `noop` (post-apply). Every resource must appear exactly
once in both the compact and detailed plan. The production gate requires the
exact compact and detailed paths and sha256 values recorded in provenance;
the approval binds that provenance and compact plan.

Any other update count or resource, replacement, deletion, import, adoption,
unknown resource or other partial create set stops the capture and apply.
Commit the capture, write
`approval.capture` from the provenance (`provenanceSha256`, `planSha256`,
`inputsSha256`, `sourceCommit`), then:

```sh
NINETY_SIX_BULLS_CONFIRM_STAGE=aws-sessions-prod bin/96bulls up aws-sessions-prod
```

`up` re-verifies the capture (source commit and tree, live state digest, plan
files, inputs) and refuses on any difference.

## Verify (read only)

```sh
B=96bulls-aws-sessions-prod-transcripts-941433935742
aws s3api get-bucket-versioning --bucket $B                       # Enabled
aws s3api get-bucket-encryption --bucket $B                       # aws:kms, the alias's key, BucketKeyEnabled false
aws s3api get-public-access-block --bucket $B                     # all four true
aws s3api get-bucket-ownership-controls --bucket $B               # BucketOwnerEnforced
aws s3api get-object-lock-configuration --bucket $B               # ObjectLockConfigurationNotFoundError
aws s3api get-bucket-lifecycle-configuration --bucket $B          # NoncurrentDays 1, abort 1, delete markers
aws s3api get-bucket-policy --bucket $B --query Policy --output text | jq .
aws s3api get-bucket-logging --bucket $B                          # target the access-log bucket
K=$(aws kms describe-key --key-id alias/96bulls-aws-sessions-prod-transcripts --query KeyMetadata.KeyId --output text)
aws kms get-key-rotation-status --key-id $K                       # aliases are refused here
aws kms get-key-policy --key-id $K --policy-name default --output text | jq .
aws cloudtrail get-event-selectors --trail-name 96bulls-aws-sessions-prod-trail
aws cloudtrail get-trail-status --name 96bulls-aws-sessions-prod-trail       # IsLogging true
```

Least privilege, with the IAM policy simulator (no credentials needed):

```sh
U=arn:aws:iam::941433935742:user/96bulls/96bulls-aws-sessions-prod-transcript-writer
O=arn:aws:s3:::$B/96bulls/org/v1/ws=w/m=m/h=h/d=2026/10/31/s=s
aws s3api get-bucket-policy --bucket $B --query Policy --output text > bucket-policy.json
for r in $O/raw/x $O/manifest.json $O/summary/short.json.enc $O/other arn:aws:s3:::$B/v1/x; do   # one resource per call:
  aws iam simulate-principal-policy --policy-source-arn $U --action-names s3:PutObject \
    --resource-arns $r --resource-policy file://bucket-policy.json \
    --query 'EvaluationResults[0].EvalDecision' --output text                                   # several at once with
done   # allowed, allowed, allowed, implicitDeny, implicitDeny                                    # --resource-policy mis-evaluates
aws iam simulate-principal-policy --policy-source-arn $U --action-names s3:DeleteObject s3:GetObjectVersion \
  --resource-arns $O/raw/x --query 'EvaluationResults[].[EvalActionName,EvalDecision]' --output text   # explicitDeny
```

Add `--context-entries ContextKeyName=s3:x-amz-server-side-encryption,ContextKeyValues=aws:kms,ContextKeyType=string`
to see the bucket policy refuse `aws:kms` without this key id (explicitDeny).

## Applied 2026-10-31

- **First apply:** created 9 of the 12 resources. KMS then refused the transcripts key policy with
  `MalformedPolicyDocumentException: invalid principals`, because the IAM users it names had not
  propagated yet; the trail-ordering delay was not long enough.
- **Recovery:** a fresh capture showed 3 creates, with the 9 existing resources unchanged (noop) and
  nothing updated or replaced. The second apply created the remaining 3.
- **Post-merge hardening applied:** merged source `681c302b0e54f5aca1ccd564fb8ae851508cc95a`
  produced exactly `trail-key update`, `trail-logs update` and ten `noop` actions. The reviewed
  capture is preserved by commit `20f8ac676d4950d82311c5aa4952ea012d0d4493`; its compact and
  detailed plan hashes are `57e939bd82de5b01f1cee8c79edb7f6daf98b5f726c9ec57e8d9b8532862d82a`
  and `bbef78b16f47d1f0792f8c5d3d2fcac2af7d754ec6e1a2ce2503e8e318966fde`.
  The guarded deploy recomputed that exact action set, updated only those two resources, and converged
  Alchemy's persisted properties. Live read-back showed exact trail `SourceArn` and `SourceAccount`
  conditions on both CloudTrail bucket grants and no KMS decrypt/re-encrypt statement (no audit role
  is configured). CloudTrail log and digest delivery remained healthy with no reported error.
- **Final no-op proof:** the immediate version-pinned capture at source
  `20f8ac676d4950d82311c5aa4952ea012d0d4493` shows all 12 expected resources exactly once as `noop`
  (`Plan: no changes`). Its compact and detailed plan hashes are
  `f1e9b424b025bc0f65e94b33458dcde02f7abcf7e00135da4145d79238c66e64` and
  `0ae5e1b5f8a137a76c8c867485bbbeb7a4bf2cadaabcad68012f3b6401396cb1`.
- **Next time:** if this happens on a new stage, wait a minute, re-capture and re-apply. Nothing
  partial needs cleaning up.
- **Account-level S3 Block Public Access:** set separately on 2026-10-31 with
  `s3control put-public-access-block` (all four true).

## Credentials (1Password, never in logs)

Access keys are never declared in the stack: Alchemy state is plaintext JSON
in the state bucket. Create a key only when its consumer is ready, and pipe it
straight into 1Password in the `96Bulls-prod` vault: no file, no echo, no shell
variable. One `ssh` call means one approval prompt for Khaliq.

```sh
U=96bulls-aws-sessions-prod-transcript-writer
T="sessions-prod transcript writer"
aws iam create-access-key --user-name $U --output json \
  | jq --arg t "$T" --arg u "$U" '{title:$t, category:"API_CREDENTIAL", fields:[
      {id:"access_key_id",     type:"STRING",    label:"access-key-id",     value:.AccessKey.AccessKeyId},
      {id:"secret_access_key", type:"CONCEALED", label:"secret-access-key", value:.AccessKey.SecretAccessKey},
      {id:"iam_user",          type:"STRING",    label:"iam-user",          value:$u},
      {id:"bucket",            type:"STRING",    label:"bucket",            value:"96bulls-aws-sessions-prod-transcripts-941433935742"},
      {id:"region",            type:"STRING",    label:"region",            value:"us-east-1"}]}' \
  | ssh kjg-lap "zsh -lc 'op item create --vault 96Bulls-prod - >/dev/null && op item get \"$T\" --vault 96Bulls-prod --format json | jq \"{id, title, fields: [.fields[] | {label, type}]}\"'"
```

The last command prints field labels and types only. If the `ssh` fails after
the key was created, the secret is gone: delete that access key
(`aws iam delete-access-key --user-name $U --access-key-id <id from list-access-keys>`)
and run it again.

### Cloudflare secret wiring (later, its own OK)

The Worker (relayhistory-cloud, production environment) reads:

| Setting | Value | Kind |
| --- | --- | --- |
| `PIPELINE_RAW_PROVIDER` | `s3` | var |
| `PIPELINE_RAW_S3_BUCKET` | `96bulls-aws-sessions-prod-transcripts-941433935742` | var |
| `PIPELINE_RAW_S3_REGION` | `us-east-1` | var |
| `PIPELINE_RAW_S3_SSE` | **unset**. The bucket default (this CMK) applies. `aws:kms` would send the header without a key id, which means the AWS managed `aws/s3` key, and the bucket policy refuses it with 403. | (none) |
| `PIPELINE_KEY_PREFIX` | `96bulls` | var |
| `PIPELINE_ORG_FROM_PREFIX` | `true` | var |
| `PIPELINE_RAW_S3_ACCESS_KEY_ID` | from 1Password | secret |
| `PIPELINE_RAW_S3_SECRET_ACCESS_KEY` | from 1Password | secret |

Leave `PIPELINE_RAW_S3_ENDPOINT` unset (AWS default endpoint). Pipe each
secret from `op read` into `wrangler secret put`, never through argv:

```sh
ssh kjg-lap "zsh -lc 'op read \"op://96Bulls-prod/sessions-prod transcript writer/secret-access-key\"'" \
  | npx wrangler secret put PIPELINE_RAW_S3_SECRET_ACCESS_KEY --env production
```

(and the same for `access-key-id`, in the same `ssh` call if possible).

### Rotation (every 90 days, or at once on suspected exposure)

1. `create-access-key` into a new 1Password item as above (a user holds at most
   two keys).
2. Update the Worker secrets; deploy.
3. Confirm writes use the new key: CloudTrail data events,
   `userIdentity.accessKeyId`.
4. `aws iam update-access-key --status Inactive` on the old key; after 7 quiet
   days, `delete-access-key` and archive the old 1Password item.

On exposure: set the old key `Inactive` first, then rotate.

## Rollback and teardown

`NON_DESTROYABLE` blocks the explicit Alchemy `destroy` route for
`aws-sessions-prod`, and `teardown.purgeBucketsOnDestroy` is refused. It does
**not** make resources deletion-proof: an `up` can reconcile a resource removed
from the declarations by deleting it. The exact phase-aware plan contract must
therefore pass before every deploy. Apart from the named two-policy remediation,
updates are refused; every replacement or deletion is refused. Roll back a
property change only through a separately reviewed contract change and capture.

Removing an **empty** store (before any Worker write), with its own OK:

1. Delete access keys, then the three users (inline policies go with
   `delete-user-policy`).
2. Delete the transcripts bucket's policy first (`OnlyTheEraserDeletes` blocks
   every other principal, operators included), then the bucket. **A versioned
   bucket is deleted only when it holds no versions and no delete markers**
   (`BucketNotEmpty` otherwise). Emptying a bucket that holds transcripts is a
   data deletion and needs a separate, explicit approval.
3. Stop and delete the trail; empty and delete the trail and access-log
   buckets (same versioning rule).
4. Delete the aliases and schedule both keys for deletion (30-day window; it
   can be cancelled until then). Objects encrypted with a deleted key are
   unreadable forever.
5. Delete the budget, then the stage's objects under
   `96bulls/96bulls-aws/aws-sessions-prod/` in the Alchemy state bucket.

## The RelayHistory export (later, not now)

The export from the frozen Neon T0 child branch (`docs/relayhistory-export-design.md`,
gated by A6 and K-2) writes into this bucket:

- under its own principal: a fourth user, `…-transcript-exporter`, with the
  writer's put/get grants and a key-policy statement of its own, added in a
  reviewed change when the exporter is ready. The Worker's key is never shared.
- with the write guard: `--execute`, the expected bucket and account
  (`941433935742`), and the key alias `alias/96bulls-aws-sessions-prod-transcripts`;
  without them it is a dry run;
- to `96bulls/<org>/v1/...`, create-only (`If-None-Match: *`), idempotent by
  `raw.content_sha256`, newest sessions first;
- with SSE-KMS by the bucket default: no encryption header, or `aws:kms` with the key ARN (the bucket policy refuses any other);
- from a host in `us-east-1`, so transcript content does not leave the region
  (regional boundaries).

The exporter's key is created, stored and rotated exactly as the writer's, and
deleted when the export is complete.
