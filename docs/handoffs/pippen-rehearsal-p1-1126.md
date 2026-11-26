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
- `prepareStateBackend` then either validates (validate-only) or runs the old ensure path. It is the
  state layer of both the capture and the Alchemy plan/deploy for the stage. In validate-only it is
  **read-only**: it checks the attested caller (STS), the exact prefix and the bucket, then lists the
  stage prefix (`ListObjectsV2 --max-keys 2`; the reply must be well formed, `KeyCount` exactly 0, 1
  or 2 matching its `Contents`, every key under the prefix; anything else refuses):
  - **empty**: refused (`state_not_claimed`): run the explicit claim first;
  - otherwise the current marker must be exactly this run's (run id, lease session, issuance time,
    pin), else `state_provenance_mismatch`;
  - while the marker is the only current object (stage A not applied), none of the stage's names may
    be in use: the function, the four queues, the five names as metric **or composite** alarms, the
    log group (alarm and log-group replies read strictly: both alarm arrays, no pagination token, no
    unknown field). AccessDenied is never "unused" (`state_gone_unproven`).
  Reads only: `sts:GetCallerIdentity`, `GetBucketVersioning` (it also answers NoSuchBucket),
  `GetBucketEncryption`, `GetPublicAccessBlock`, `GetBucketOwnershipControls`, `GetBucketTagging`,
  `ListObjectsV2`, the marker's `GetObject`, and the names' reads. `HeadBucket` is not used: it needs
  `s3:ListBucket` without an `s3:prefix` condition, which would let the lease list every stage's keys.
- **Which call writes.** The capture (`96bulls plan --capture`) and plain plans write nothing to
  the state bucket. The only state-tooling writes are the explicit **claim**
  (`scripts/rehearsal-state-teardown.ts --claim`, before the first capture: one conditional
  `PutObject --if-none-match "*"` of the marker) and the **teardown** (`--execute`: `DeleteObjects`
  of the stage's versions). Alchemy's own deploy writes its state under the prefix as usual.
- The claim (`claimStageState`) requires an **empty version inventory** under the prefix
  (`ListObjectVersions`: zero versions and zero delete markers), so a prefix holding only
  noncurrent versions or delete markers, which looks empty to `ListObjectsV2`, can never be claimed
  (and its history later deleted); then none of the stage's names may be in use; then the marker
  `96bulls/96bulls-aws/aws-rehearse-p1-1126/__rehearsal_run__.marker` is written, holding the run id,
  lease session, issuance time and pin. It is not `.json`: Alchemy's state store and the capture's
  snapshot layer list `*.json` resources only, so they ignore it. A later lease needs a new run
  against a torn-down prefix (or a teardown authorization, below).
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

`scripts/rehearsal-state-teardown.ts`; every step checks the run context (inputs, lease env, issuer
pin, lease end) before any AWS call:

1. `--claim`, under the run's lease A, before the first capture (above).
2. `--manifest`, under lease A, **before any destroy**: reads every resource id from AWS
   (`readPhysicalManifest`) and writes `.96bulls/aws-rehearse-p1-1126/physical-manifest.json`
   (gitignored; it names the account) once (`wx`, never overwritten, so it survives partial
   destroys). It records the function ARN, the pinned execution role (ARN, disposition, boundary),
   the four queues, exactly two mapping UUIDs (one per queue), the five alarms (no same-name
   composite alarm allowed) and the log group, bound to the run (run id, lease A's session,
   issuance time and pin) with a digest. It prints the digest; the broker records it.
3. `alchemy destroy` for the stage (or the pippen sweeper), possibly in several partial passes.
4. `--execute --expect-manifest-digest <sha256>` (and, under a teardown lease B,
   `--authorization <file> --expect-authorization-digest <sha256>`): `runRehearsalTeardown`, which
   deletes nothing until every check has passed, in this order:
   1. the run context of the current lease and STS (the current caller is exactly that lease);
   2. the shared bucket's settings (reads);
   3. under lease B, the **teardown authorization** (below); without one, the current lease must be
      the run's own lease A;
   4. the **independent receipt**: the manifest digest the broker recorded at step 2 (not one
      recomputed from the local file) must equal the manifest's digest, and, with an authorization,
      its manifest digest and mapping UUIDs must be the manifest's;
   5. the manifest complete and bound to the original run (`assertManifestComplete`);
   6. every recorded id gone (`assertManifestGone`): the function by ARN, each queue by URL, each
      mapping by UUID, no metric or composite alarm of the five names, no log group of the exact
      name, and the role per its pinned disposition (deleted: not found; retained: exactly the pinned
      ARN, path and boundary). A remaining id refuses (`state_resources_remain`); AccessDenied is
      never "gone" (`state_gone_unproven`); rerun after the next destroy pass;
   7. no capture or deploy may run while a teardown marker exists (`state_teardown_in_progress`):
      the state layer refuses, which fences Alchemy writers against an open teardown;
   8. the resume state, under these receipts: no version left (an earlier execute completed and
      only its last response was lost): complete, nothing deleted; only this teardown's marker left:
      continue with step 8's last call; otherwise the run marker must be current and the original
      run's (lease A's session, issuance time, pin). Anything else keeps the state;
   9. only then, in this order (`teardownStageState`), with **generation fencing**:
      1. the **teardown marker** `__rehearsal_teardown__.marker` (run id, manifest digest,
         authorization digest) is written only over this run's own marker, if absent
         (`--if-none-match "*"`), or must already be exactly this teardown's;
      2. the generation is recorded: the exact version ids of the teardown marker (exactly one) and
         of the run marker (exactly one, and its body this run's; none on a teardown-marker-only
         resume);
      3. before **every** delete step the fence is re-checked: the teardown marker is still exactly
         that one version with this teardown's body, and no run-marker version other than the
         recorded one exists; otherwise the executor stops;
      4. every other version and delete marker, by the key and version ids listed at the start of
         the generation (strict page validation; any key outside the prefix refuses before
         anything is deleted); the re-list must then show only the markers;
      5. the run marker, alone, by its recorded version id only;
      6. the teardown marker, last and alone, by its recorded version id only; the final re-list
         must show nothing.

   Version-id fencing is what makes a stale executor harmless: if teardown T1 pauses, a second
   teardown T2 with the same receipts completes, and a new run claims the now-empty prefix, T1's
   next fence check finds its teardown marker gone and stops, and any delete it still issues names
   only the old generation's version ids, never the new run's marker (tested at every pause point).

   A partial batch, or a delete that reports success and leaves a version, stops with the run marker
   intact, and the next execute resumes. A claim cannot interleave: it requires an empty version
   inventory, and the teardown marker exists until the very last call. Once the prefix is empty a new
   claim is a new run; a retry of a completed teardown before that is recognised as complete.

   So the digest check runs after the STS and bucket reads and before any deletion. Without
   `--execute` it is a dry run (counts only). The bucket itself and sibling stages are never touched.
   The CLI wrapper keeps the error class (not found, AccessDenied, other), never the CLI's text, so
   a denied read refuses as `state_gone_unproven`, not a generic failure.

### Teardown authorization (lease A to lease B)

The teardown lease B (issued with the manifest's two mapping UUIDs for the absence reads) is a
different session from lease A, so the manifest and the marker, which name A, cannot be checked
against B's context directly. The broker writes a **teardown authorization** at B's issuance and
records its digest (`authorizationDigest`, sha256 of the canonical JSON) in its receipt:

```json
{ "schema": 1, "stage": "aws-rehearse-p1-1126", "runId": "<run id>",
  "original": { "sessionId": "<lease A>", "issuedAt": "<A issuance>", "pin": "<A pin>" },
  "manifestDigest": "<manifest digest>", "mappingUuids": ["<uuid 1>", "<uuid 2>"],
  "teardown": { "sessionId": "<lease B>", "callerArn": "arn:aws:sts::<account>:assumed-role/agent-session/agent-<lease B>",
                "issuedAt": "<B issuance>", "windowEnd": "<approved teardown window end>",
                "expiresAt": "<B effective expiry>", "policyDigest": "<B cleanup-only policy digest>" } }
```

`assertTeardownAuthorization` refuses (`state_authorization_mismatch`) unless the record's digest is
the receipt's; it is this stage and run; B is exactly the current session, STS caller ARN and
issuance; and A's pin is the issuer pin of the same approved account, boundary, role and
disposition with A's session and issuance. B's validity is capped at the **authorized run
deadline** (`teardownDeadline`): the earliest of A's issuance + 6 h, the hard cutoff
2026-11-27T08:00:00Z and the approved teardown window end the record carries explicitly. B must be
issued before it, its effective expiry must not be later than it (nor than B's own lease end), and
the teardown runs only before that expiry. B's policy digest must be the digest of the
**cleanup-only** teardown policies generated for B's effective expiry and the two mapping UUIDs
(`rehearsalTeardownPolicies`, `teardownPolicyDigest`), so the receipt binds both the expiry and the
action set. The manifest and the marker are then checked against lease A's context. Any failure
keeps the state.

The cleanup-only policies (inline 1,150 characters; two managed documents, 1,773 and about 4,500; limit 6,144):
deletes of the stage's own resources (`sqs:DeleteQueue`, `lambda:DeleteFunction`,
`cloudwatch:DeleteAlarms`, `logs:DeleteLogGroup` on tagged resources; `lambda:DeleteEventSourceMapping`
and `GetEventSourceMapping` on the two exact mapping ARNs; the role's deletion on its exact ARN only
when its pinned disposition is deleted); the exact-name absence reads; the state prefix's list, reads
and version deletes; one `s3:PutObject`, on the teardown marker's exact key. No create, update, tag,
publish or invoke action is allowed, and an explicit Deny (`CleanupOnly`) lists them. Every Allow
holds only before B's effective expiry and a Deny on everything applies at and after it.

## Lease: issuer mechanism and actions

The broker (layered-infra-ops) issues the lease with `sts:AssumeRole` on the sandbox's
`agent-session` role, session name `agent-<session_id>`, passing:

- `Policy`: the inline session policy (`inline`, 768 compact characters, under the 2,048 packed
  limit), holding the denies: everything at and after the effective end, every non-IAM/STS/S3 call
  outside `us-west-2`, any write to a `pippen-sbx-alchemy-state-*` bucket, and removing the
  `rehearsal` tag;
- `PolicyArns`: three customer-managed policies the broker creates from `managed`
  (`96bulls-rehearse-p1-1126-state` 1,500, `-create` 3,919, `-act` about 5,300 (it names the role ARN; about 5,650 with the two mapping UUIDs of the teardown lease); limit 6,144);
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
- Absence readbacks (untagged, exact names, so a deleted resource answers NotFound, not
  AccessDenied): `lambda:GetFunctionConfiguration` on the exact function and
  `sqs:GetQueueAttributes` on the four exact queues (`GoneReads`); `lambda:GetEventSourceMapping`
  on the two exact mapping ARNs (`GoneMappings`), which exist only after creation: the teardown
  lease is issued with the manifest's two UUIDs (`rehearsalLeasePolicies({ mappingUuids })`), and
  the earlier lease omits it.
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
6. The physical manifest's digest as printed at `--manifest`, and the run id: this is the value
   passed to `--execute --expect-manifest-digest`, and it is evidence only because the broker
   recorded it independently of the local file.
8. For a teardown under lease B: the teardown authorization record and its digest (the value
   passed to `--expect-authorization-digest`). The issuer pin needs the same trusted provenance:
   the environment value is checked, its origin is the broker's receipt.
7. The teardown lease, issued with the manifest's two mapping UUIDs (`GoneMappings`).

## Broker and live-evidence items (not provable offline)

- **Teardown lease discovery reads**, resource `*` (IAM cannot narrow them): `lambda:ListEventSourceMappings`
  and `logs:DescribeLogGroups`. The broker decides whether to keep them; without them the
  physical-manifest and absence checks for mappings and the log group cannot run. The teardown
  lease's role rights (`iam:DeleteRole`, `ListAttachedRolePolicies`, `DetachRolePolicy`,
  `ListRolePolicies`, `DeleteRolePolicy`) are on the exact pinned role ARN only, and only when its
  disposition is "deleted"; a retained role gets none of them.
- **Broad regional reads**, resource `*` because IAM cannot narrow them: `lambda:ListEventSourceMappings`,
  `cloudwatch:GetMetricStatistics`, `cloudwatch:GetMetricData`, `logs:DescribeLogGroups`,
  `logs:GetQueryResults`. The broker decides each: keep (read-only metadata and metric/log data of
  the sandbox account), or drop and accept the guard or runbook query it serves cannot run.
- **Live AWS behaviour** to observe in the sandbox and record: that the create calls succeed with
  the request-tag conditions (including whether SQS, Lambda, CloudWatch and Logs also authorize
  their tag action at create against the strict tag statement); that event source mappings accept
  the tag and the `lambda:FunctionArn` condition on create, update, delete and get; that the
  untagged exact-name reads answer NotFound for deleted resources; and that the session ends at the
  issued expiry and is refused after revocation.

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
- **Gates still open:** the non-prod render of the session pipeline; the proof-account stage rule;
  the execution-role wiring (path and boundary); the revised scope and teardown window, including
  lease B and its authorization record; trusted issuance, expiry and revocation; the cost cap; and
  live evidence (budget and teardown) from a real run.
