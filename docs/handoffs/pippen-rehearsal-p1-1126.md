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
| execution role | `pippenSandbox.executionRole` in the inputs: `{ "arn": "<execution-role-arn>", "disposition": "deleted" \| "retained" }` (retained only under `/pippen/workload/`); pinned by the issuer |
| issuer pin | `PIPPEN_REHEARSAL_ISSUER_PIN_SHA256` = sha256 of `["aws-rehearse-p1-1126","us-west-2","<account>","<boundary ARN>","<execution-role-arn>","<disposition>","<session_id>","<issued_at>"]` (JSON), set by the broker at issuance from its own records; the inputs and the lease env must hash to it |
| lease issuance time | `PIPPEN_REHEARSAL_ISSUED_AT` (ISO, UTC), set by the broker; the run is refused before it and at or after the effective end |
| run id | `NINETY_SIX_BULLS_REHEARSAL_RUN_ID` (a UUID the operator chooses for the run; the same for the manifest and the teardown) |
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
- Before **any** AWS call (and before git), `rehearsalRunContext` checks everything that needs no
  AWS: the inputs, the lease env (account, session, issuance time), the issuer pin over all of them,
  and that the lease has not ended. The capture calls it right after reading the inputs; the stack
  calls it at module load; a full-entry test drives `capturePlan` with fake `aws` and `git` and
  asserts zero calls for malformed inputs, a wrong pin and an ended lease.
- `prepareStateBackend` then either validates (validate-only: the attested caller, the exact
  prefix, the bucket already in shape, and, while this stage's state is empty, that none of the
  stage's names is in use; no writes) or runs the old ensure path. It is the state layer of both the
  capture and the Alchemy plan/deploy for the stage, so stage A's creates cannot run before it. Only reads:
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

`scripts/rehearsal-state-teardown.ts`, in three uses:

1. **Before any destroy**, `--manifest --role-disposition deleted|retained`: reads every resource
   id from AWS (`readPhysicalManifest`) and writes the physical manifest
   `.96bulls/aws-rehearse-p1-1126/physical-manifest.json` (gitignored; it names the account). It
   records the function ARN, the execution role ARN and its disposition (deleted, or retained as the
   boundary-pathed role by decision), the four queues (name, URL, ARN), exactly two mapping UUIDs
   (one on the events queue, one on the search queue), the five alarms and the log group. Every one
   must exist; fewer ids refuse. The file is written once (`wx`) and never overwritten, so it
   survives partial destroys.
2. `alchemy destroy` for the stage (or the pippen sweeper), possibly in several partial passes.
3. `--execute`: inputs, issuer pin, attested caller and bucket checked; then the manifest must be
   present and complete (`assertManifestComplete`; missing, or fewer than the expected ids, refuses)
   and **every recorded id must read back as gone** (`assertManifestGone`): the function by ARN, each
   queue by URL, each mapping by UUID, the alarms list empty, no log group of the exact name, and
   the role per its disposition (deleted: not found; retained: present at `/pippen/workload/`). A
   remaining id refuses (`state_resources_remain`); rerun after the next destroy pass. Only then the
   state teardown: list every version and delete marker under exactly
   `96bulls/96bulls-aws/aws-rehearse-p1-1126/` (strict page validation as before; any key outside the
   prefix refuses before anything is deleted), `DeleteObjects` by key and version id, and an
   independent re-list that must show zero versions and zero delete markers.

Without `--execute` it is a dry run (counts only). The bucket itself and sibling stages are never
touched.

The manifest is bound to its run: it carries the run id, the lease session, the issuance time and
the issuer pin, and a digest of its own content. `--execute` refuses a manifest from another run,
lease or pin, or one whose content no longer matches its digest. The role in it is exactly the
pinned ARN (account, path, name), the pinned disposition and the pinned boundary: deleted means
`iam get-role` on that exact name is not found; retained means it shows exactly that ARN, path and
permissions boundary. The digest is printed when the manifest is written and belongs in the run's
handoff and the broker receipt (the file is local and an editor could recompute the digest; the
recorded digest is what detects that).

Non-adoption: while this stage's state is empty (stage A not yet applied), the state layer refuses
if any of the stage's names is in use (`assertRehearsalNamesUnused`). That preflight cannot enforce
non-adoption against a resource created concurrently after it ran, or against the lease used
outside this tooling: the issuer must scope the grant (below) and that residual is the issuer's.

## Lease: issuer mechanism and actions

The broker (layered-infra-ops) issues the lease with `sts:AssumeRole` on the sandbox's
`agent-session` role, session name `agent-<session_id>`, passing:

- `Policy`: the inline session policy (`inline`, 768 compact characters, under the 2,048 packed
  limit), holding the denies: everything at and after the effective end, every non-IAM/STS/S3 call
  outside `us-west-2`, any write to a `pippen-sbx-alchemy-state-*` bucket, and removing the
  `rehearsal` tag;
- `PolicyArns`: three customer-managed policies the broker creates from `managed`
  (`96bulls-rehearse-p1-1126-state` 1,500, `-create` 3,919, `-act` about 4,700 (it names the role ARN); limit 6,144);
- `DurationSeconds`: no more than `rehearsalLeaseEnd({ issuedAt })` minus issuance (at most 6 h);
- the environment `pippen sandbox exec` gives the session: `PIPPEN_SANDBOX_ACCOUNT_ID`,
  `PIPPEN_SANDBOX_SESSION_ID` and `PIPPEN_REHEARSAL_ISSUER_PIN_SHA256`.

The session's permissions are the role's policies intersected with these session policies. The actions:

- `-state`: `s3:ListBucket`/`ListBucketVersions` only with `s3:prefix` like the stage prefix;
  object reads, writes and deletes only on `<bucket>/96bulls/96bulls-aws/aws-rehearse-p1-1126/*`;
  the bucket's five settings readable; explicit denies on other prefixes and other objects.
- Every resource is exact: the function
  `arn:aws:lambda:us-west-2:<account>:function:96bulls-aws-rehearse-p1-1126-session-pipeline`, the
  four queue ARNs, the five alarm ARNs, the log group. Only mapping ARNs are `event-source-mapping:*`
  (their UUIDs do not exist before creation), always conditioned on `lambda:FunctionArn` equal
  (`ArnEquals`) to that exact function.
- `-create`, only with `aws:RequestTag/rehearsal = p1-1126`: `sqs:CreateQueue`,
  `lambda:CreateFunction`, `lambda:CreateEventSourceMapping`, `cloudwatch:PutMetricAlarm`,
  `logs:CreateLogGroup`. Standalone tagging (`sqs:TagQueue`, `lambda:TagResource`,
  `cloudwatch:TagResource`, `logs:TagResource`, on the exact queues, function, alarms and log group,
  never on mappings) only where the resource already carries `rehearsal = p1-1126` and the request
  sets it (no `IfExists`): an untagged resource cannot be tagged into the rehearsal. **Residual for
  the issuer:** if a service also authorizes its tag action at create time against a resource tag
  that does not exist yet, creating with tags fails closed; the issuer must confirm the create calls
  succeed under these policies, or grant create-time tagging in a way it can bind to the create.
- `-act`, only on resources tagged `rehearsal = p1-1126`: queue attributes, messages and deletion;
  the function's reads, updates, `PublishVersion`, `ListAliases`, `ListVersionsByFunction`,
  permissions, tag reads, invoke and deletion; the mappings' get, update and delete on
  `event-source-mapping:*` by their own tag **and** `lambda:FunctionArn` equal to the exact function
  (teardown checks each recorded UUID); alarm put and delete; the log group's
  retention, deletion, tag reads, `StartQuery` and `FilterLogEvents`.
- Reads narrowed to the stage's names (no tag condition, since they must also answer "not found"):
  `sqs:GetQueueUrl` and `cloudwatch:DescribeAlarms` on the exact queues and alarms; `iam:GetRole`
  only on the pinned execution role ARN (the role-disposition check).
- **Reads IAM cannot narrow** (resource `*`): `lambda:ListEventSourceMappings` (no resource-level
  permission), `cloudwatch:GetMetricStatistics` and `cloudwatch:GetMetricData` (no resource-level
  permission and no namespace or dimension condition key for reads; `cloudwatch:namespace` applies
  to `PutMetricData` only), `logs:DescribeLogGroups` and `logs:GetQueryResults` (no resource-level
  permission; the query itself, `logs:StartQuery`, is limited to the stage's log group, so results
  can only come from it).
- Not granted: any EventBridge or Secrets Manager action, and no IAM action besides `GetRole`.

Tag-condition support (`aws:RequestTag`/`aws:ResourceTag` per action and resource type) follows the
AWS service authorization reference; it was not exercised against AWS here.

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

## What the broker must attest, and the receipts required

Code checks: the issuer pin (the inputs' account, boundary, execution role and disposition, and the
lease's session and issuance time hash to the broker's pin), the lease env
(`PIPPEN_SANDBOX_ACCOUNT_ID`, `PIPPEN_SANDBOX_SESSION_ID` equal to the inputs' account and
`NINETY_SIX_BULLS_PIPPEN_SESSION`), the lease's end, and STS (exactly
`agent-session/agent-<session_id>` in that account).

**Provenance:** the code compares the inputs with a digest it receives in the environment. That
proves consistency, not origin: anyone who can set the environment can set a matching digest. Only
the broker can establish where the pin came from (its issuance receipt, below), so the code-side
check is necessary but not sufficient. A broker-signed attestation verified against a broker
public key pinned in the repository would close this; no broker key exists yet, so it is not
implemented. The pin must be computed from the broker's own records, never from the stage inputs.

Generating policies does not issue or revoke anything. The rehearsal record needs these broker
receipts (kept outside the repository, referenced by hash in the run's handoff):

1. The issuer pin and the records it was computed from (approved account, boundary, execution role
   and disposition, lease session and issuance time).
2. For each managed session policy: its ARN, the default version id, and the sha256 of that
   version's document, equal to the generated `managed[n].document` (canonical JSON).
3. The sha256 of the inline session policy passed, equal to the generated `inline`.
4. The AssumeRole receipt: request id, role and session name, `DurationSeconds`, and the
   credentials' `Expiration` (not later than `rehearsalLeaseEnd({ issuedAt })`).
5. Revocation at teardown completion: the revocation action taken (for example the role's
   revoke-older-sessions deny, or deleting the managed policy versions) with its time, and a
   failed call with the lease's credentials after it.
6. The physical manifest's digest as printed at `--manifest`, and the run id.

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
- The issuance itself (pin, policy versions, STS duration) and the revocation are the broker's, and
  their receipts (above) are not produced by this code; the pin's provenance is the broker's alone.
- Non-adoption under concurrent creation or standalone use of the lease, and create-time tagging
  under the strict tag statement, are residuals for the issuer (see Teardown and Lease).
