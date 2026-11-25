# P1 split rehearsal in a Pippen sandbox: stage `aws-rehearse-p1-1126`

Code only. Nothing here has been run against AWS. The account id and the lease session stay out of
the repository: `<account>` below is the sandbox account, `<session_id>` the lease UUID.

## Inventory

| item | value |
| --- | --- |
| stage | `aws-rehearse-p1-1126` |
| region | `us-west-2` (the lease denies every other region) |
| account | `<account>` (from `.96bulls/aws-rehearse-p1-1126/aws-inputs.json`, gitignored; must equal the broker's `PIPPEN_SANDBOX_ACCOUNT_ID`) |
| caller | `arn:aws:sts::<account>:assumed-role/agent-session/agent-<session_id>`; `<session_id>` = `NINETY_SIX_BULLS_PIPPEN_SESSION` = the broker's `PIPPEN_SANDBOX_SESSION_ID` |
| state bucket (shared, never changed) | `pippen-sbx-alchemy-state-<account>-us-west-2-an` |
| state prefix (this stage only) | `96bulls/96bulls-aws/aws-rehearse-p1-1126/` |
| resource tag | `rehearsal=p1-1126` on every resource (required on create and on every act by the lease) |
| function | `96bulls-aws-rehearse-p1-1126-session-pipeline` |
| queues (four) | `96bulls-aws-rehearse-p1-1126-session-events`, `96bulls-aws-rehearse-p1-1126-session-events-dlq`, `96bulls-aws-rehearse-p1-1126-session-search`, `96bulls-aws-rehearse-p1-1126-session-search-dlq` |
| mappings (two) | events queue -> function, search queue -> function (no names; Lambda UUIDs, recorded in state) |
| alarms (five) | `96bulls-aws-rehearse-p1-1126-session-events-dlq-not-empty`, `96bulls-aws-rehearse-p1-1126-session-events-backlog-age`, `96bulls-aws-rehearse-p1-1126-session-pipeline-errors` (the three the pipeline always has), `96bulls-aws-rehearse-p1-1126-session-search-dlq-not-empty`, `96bulls-aws-rehearse-p1-1126-session-search-backlog-age` (the two search alarms) |
| log group | `/aws/lambda/96bulls-aws-rehearse-p1-1126-session-pipeline` |
| not in the inventory | the pipeline's reconcile and brief EventBridge rules and its Secrets Manager secret: the lease grants nothing on them, so the non-prod render must leave them out for this stage |
| lease policies | `docs/handoffs/pippen-rehearsal-p1-1126.lease-policy.json` (`rehearsalLeasePolicies`: one inline session policy and three managed session policies; placeholders only; a test fails when stale) |
| lease hard cutoff | `2026-11-27T08:00:00Z`, exclusive (end of day 2026-11-26 Pacific, the authority's end): every Allow holds only before it (`DateLessThan`), a Deny on everything applies at and after it (`DateGreaterThanEquals`) |
| effective lease end | the **earliest** of teardown completion, issuance + 6 h, and the cutoff (`rehearsalLeaseEnd`; a later requested end is refused) |

These are the names `stacks/aws/pippen-rehearsal.ts` `rehearsalResourceNames` returns (the stack's
own naming, `stacks/aws/session-pipeline.ts`); a test checks this table lists every one of them,
five alarms included.

## The shared state bucket: validate-only

`stacks/aws/state-bucket.ts` `ensurePippenStateBucket` creates the sandbox's state bucket and
converges its versioning, encryption, public access block, ownership controls and tags bucket-wide,
even when it exists. Every stage in the account shares that bucket, so a per-stage prefix cannot
bound it. Hence:

- The backend is decided from **validated** inputs before any backend is chosen
  (`stateBackendFor`, used by both entry points, `src/cli/aws-provenance-io.ts` right after reading
  the inputs and before any AWS call, and `stacks/aws/alchemy.aws.run.ts` at module load). For
  `aws-rehearse-p1-1126` it requires a 12-digit account, region `us-west-2`, and `pippenSandbox` with
  a policy-ARN boundary in that account and `stateBucket: "validate-only"`; missing or malformed
  inputs throw (`state_inputs_invalid`) and never fall through to ensure or to Alchemy's default
  store. Every other stage keeps its old choice (the proof stage and fresh-account sandbox stages
  such as `aws-proof` ensure; stages without `pippenSandbox` use the default store); a
  `stateBucket` value outside a declared stage is refused.
- `prepareStateBackend` then either validates (validate-only: the attested caller, the exact
  prefix, the bucket already in shape; no writes) or runs the old ensure path. Only reads:
  `sts:GetCallerIdentity`, `GetBucketVersioning` (it also answers NoSuchBucket),
  `GetBucketEncryption`, `GetPublicAccessBlock`, `GetBucketOwnershipControls`, `GetBucketTagging`.
  Never CreateBucket, never any Put. `HeadBucket` is not used: it needs `s3:ListBucket` without an
  `s3:prefix` condition, which would let the lease list every stage's keys.
- Expected settings: versioning `Enabled`; one SSE rule, `AES256`, no KMS key, bucket key off; all
  four public access blocks on; `BucketOwnerEnforced`; tags `pippen:sandbox=true` and
  `96bulls:component=alchemy-state` (the `96bulls:stack` tag names the stage that made it, so it is
  not required).
- Refusals carry fixed codes and no CLI text: `state_bucket_absent`, `state_bucket_unreadable`,
  `state_bucket_versioning_drift`, `state_bucket_encryption_drift`,
  `state_bucket_public_access_drift`, `state_bucket_ownership_drift`, `state_bucket_tagging_drift`,
  `state_prefix_mismatch`, `state_caller_mismatch`. If the bucket is absent or drifted, the
  rehearsal stops; the proof stage (or an operator) fixes the bucket, never this stage.

## Teardown (this stage's state only)

`bun scripts/rehearsal-state-teardown.ts` (dry run) or `... --execute`, after `alchemy destroy` for
the stage (or the pippen sweeper) has removed the stage's resources. In order:

1. Inputs validated as the state layer does (`stateBackendFor`); the caller is the attested lease
   session (`assertLeaseCaller`); the shared bucket is in shape (`validatePippenStateBucket`).
2. **Physical resources gone** (`--execute` only, before any state is discarded):
   `assertRehearsalResourcesGone` reads back the function, each queue and each mapping UUID recorded
   in this stage's state (`recordedMappingUuids`) and requires not-found for each; the five alarms
   must list empty and no log group of the function's exact name may remain. Anything still present
   refuses (`state_resources_remain`); any other failure refuses (`state_teardown_failed`).
3. List every object version and delete marker under exactly
   `96bulls/96bulls-aws/aws-rehearse-p1-1126/` (`ListObjectVersions --prefix`, key and version-id
   markers). Every page is validated strictly: blank or unparseable output, a missing or non-boolean
   `IsTruncated`, a truncated page without both markers, a non-array `Versions`/`DeleteMarkers`, an
   unknown field or another `Prefix` refuses. Any key outside the prefix (a sibling stage,
   `aws-rehearse-p1-1126x/`, a parent key, the prefix itself) refuses before anything is deleted.
4. `DeleteObjects` by key and version id, 1000 per call, rechecking each batch.
5. **Empty-prefix evidence**: an independent re-list must show zero versions and zero delete markers
   (`state_teardown_not_empty` otherwise) before success (`{ listed, deleted, remaining: 0 }`).

The bucket itself and sibling stages are never touched.

## Lease: issuer mechanism and actions

The broker (layered-infra-ops) issues the lease with `sts:AssumeRole` on the sandbox's
`agent-session` role, session name `agent-<session_id>`, passing:

- `Policy`: the inline session policy (`inline`, 768 compact characters, under the 2,048 packed
  limit with room for the ARNs and tags), holding the denies: everything at and after the effective
  end, every non-IAM/STS/S3 call outside `us-west-2`, any write to a `pippen-sbx-alchemy-state-*`
  bucket, and removing the `rehearsal` tag;
- `PolicyArns`: three customer-managed policies the broker creates from `managed`
  (`96bulls-rehearse-p1-1126-state`, `-create`, `-act`; each well under 6,144 characters);
- `DurationSeconds`: no more than `rehearsalLeaseEnd({ issuedAt })` minus issuance (at most 6 h).

The session's permissions are the role's policies intersected with these session policies, so
nothing the role grants beyond them is usable. The actions:

- `-state`: `s3:ListBucket`/`ListBucketVersions` only with `s3:prefix` like the stage prefix;
  `s3:GetObject`, `GetObjectVersion`, `PutObject`, `DeleteObject`, `DeleteObjectVersion` only on
  `<bucket>/96bulls/96bulls-aws/aws-rehearse-p1-1126/*`; the bucket's five settings readable;
  explicit denies on listing other prefixes and on object access outside the prefix.
- `-create` (only with `aws:RequestTag/rehearsal = p1-1126`, names `96bulls-aws-rehearse-p1-1126-*`):
  `sqs:CreateQueue`, `TagQueue`; `lambda:CreateFunction`, `TagResource`;
  `lambda:CreateEventSourceMapping` (only for the stage's function, `lambda:FunctionArn`);
  `cloudwatch:PutMetricAlarm`, `TagResource`; `logs:CreateLogGroup`, `TagResource`.
- `-act` (only on resources tagged `rehearsal = p1-1126`): queue attributes, messages and deletion;
  the function's reads, updates, `PublishVersion`, `ListAliases`, `ListVersionsByFunction`,
  permissions, tags, invoke and deletion; the mappings' get, update and delete (by
  `lambda:FunctionArn`, as mappings are not tag-scoped here); alarm put, delete and tag reads; the
  log group's retention, deletion, tag reads, `StartQuery` and `FilterLogEvents`; and reads that
  cannot be scoped: `sqs:GetQueueUrl`, `sqs:ListQueues`, `lambda:ListEventSourceMappings`,
  `cloudwatch:DescribeAlarms`, `GetMetricStatistics`, `GetMetricData`, `logs:DescribeLogGroups`,
  `GetQueryResults`.
- Not granted: any EventBridge, Secrets Manager or IAM action (see the inventory).

Tag-condition support (`aws:RequestTag`/`aws:ResourceTag` per action) follows the AWS service
authorization reference; it was not exercised against AWS here.

## Lease lifetime

The effective lease must end at the **earliest** of:

1. teardown completion (step 5 above reported; the broker revokes the lease then);
2. 6 hours from issuance (`REHEARSAL_LEASE_TTL_HOURS`: the broker's issuance parameter and the
   policies' end when generated with `issuedAt`);
3. `2026-11-27T08:00:00Z` (end of day 2026-11-26 Pacific), exclusive: allowed at
   `2026-11-27T07:59:59Z`, denied at `2026-11-27T08:00:00Z`.

`rehearsalLeasePolicies({ accountId, issuedAt })` writes the earliest of 2 and 3 into every Allow
(`DateLessThan`) and the inline Deny (`DateGreaterThanEquals`); a requested end later than that is
refused, never silently shortened. The checked-in document is the template without `issuedAt` (the
absolute cutoff); the broker generates the issued one.

## What the broker must attest

Code checks the caller (STS: exactly `agent-session/agent-<session_id>` in `<account>`) and requires
`PIPPEN_SANDBOX_ACCOUNT_ID` and `PIPPEN_SANDBOX_SESSION_ID` from `pippen sandbox exec`, equal to the
inputs' account and `NINETY_SIX_BULLS_PIPPEN_SESSION`. The real account id is not in the repository,
so the code cannot prove the account is the approved sandbox; the broker must attest, for the
recorded run: the account is the approved non-prod sandbox; the lease UUID is the one issued for
this rehearsal; the lease was issued with exactly these session policies and `DurationSeconds`; its
issuance time; and its revocation at teardown.

## Not covered (blockers before the rehearsal can run)

- **The stack cannot render the pipeline for this stage yet.** The session pipeline (and so the
  search split) is declared only inside the production transcript store, which is pinned to
  `aws-sessions-prod` (`stacks/aws/transcript-store.ts`). A non-prod stage that declares it is new
  work.
- **The proof account pin.** `stacks/aws/inputs.ts` `assertPippenProofSandbox` lets only
  `aws-proof-sandbox` target the proof sandbox's account. If the rehearsal uses that account, the
  pin must be widened by an owner decision; this branch does not change it.
- **The function's execution role.** Alchemy names and creates the Lambda role itself
  (`96bulls-aws-session-pipeline-<stage>-<suffix>`, path `/`, no boundary), which matches neither
  `96bulls-aws-rehearse-p1-1126-*` nor the sandbox's `/pippen/workload/` boundary rule. The lease
  grants no IAM role actions; how the role is made in the sandbox needs a decision.
- **The renamed scope.** The owner's decision on the rehearsal scope as renamed here (stage
  `aws-rehearse-p1-1126`, the synthetic inventory without schedules and secret) is still open.
- The lease policies are documents and a generator; creating and attaching them is the broker's step.
- The broker reviewed source and Node only; the Bun test counts are author-reported (reproduction
  commands in the handoff report).
