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
  **read-only**: it checks the attested caller (STS), the exact prefix and the bucket, lists the
  stage prefix (`ListObjectsV2 --max-keys 1000`, one page that must be the whole listing: well
  formed, not truncated, `KeyCount` matching its `Contents`, every key under the prefix; anything
  else refuses), then reads the **generation object** (below):
  - **no generation object**: refused (`state_not_claimed`): run the explicit claim first;
  - generation `tearing-down`: refused (`state_teardown_in_progress`): no capture or deploy runs
    while a teardown is open;
  - otherwise it must be exactly this run's `claimed` generation (run id, lease session, issuance
    time, pin), else `state_provenance_mismatch`;
  - while the generation object (and Alchemy's `__lease__.json` / `__stack_output__.json`
    bookkeeping) are the only current objects (stage A not applied), none of the stage's names may
    be in use: the function, the four queues, the five names as metric **or composite** alarms, the
    log group (alarm and log-group replies read strictly: both alarm arrays, no pagination token, no
    unknown field). AccessDenied is never "unused" (`state_gone_unproven`).
  Reads only: `sts:GetCallerIdentity`, `GetBucketVersioning` (it also answers NoSuchBucket),
  `GetBucketEncryption`, `GetPublicAccessBlock`, `GetBucketOwnershipControls`, `GetBucketTagging`,
  `ListObjectsV2`, the generation object's `GetObject`, and the names' reads. `HeadBucket` is not
  used: it needs `s3:ListBucket` without an `s3:prefix` condition, which would let the lease list
  every stage's keys.
- **Which call writes.** The capture (`96bulls plan --capture`) and plain plans write nothing to
  the state bucket. The only state-tooling writes are the explicit **claim**
  (`scripts/rehearsal-state-teardown.ts --claim`, before the first capture: one create-only
  `PutObject --if-none-match "*"` of the generation object) and the **teardown** (`--execute`: one
  compare-and-swap `PutObject --if-match <ETag>` of the generation object, then `DeleteObjects` of
  the stage's versions). Alchemy's own deploy writes its state under the prefix as usual.
- **The generation lock.** A single object,
  `96bulls/96bulls-aws/aws-rehearse-p1-1126/__rehearsal_generation__.marker`, holds
  `{ schema, stage, runId, sessionId, issuedAt, pin, state: "claimed" | "tearing-down", receipts }`
  (receipts: the teardown's manifest and authorization digests, set exactly when tearing down). It
  is changed only by compare-and-swap: created with `If-None-Match: *`, replaced with `If-Match`
  on the ETag just read, deleted by its recorded version id. It serializes the claim-to-cleanup
  transition: a claim fails while any generation object (or any other version) exists; a teardown
  must move this run's generation from `claimed` to `tearing-down` before it touches anything; a
  stale teardown's swap or fence fails once a new generation exists. It is not `.json`: Alchemy's
  state store and the capture's snapshot layer list `*.json` resources only, so they ignore it.
- The claim (`claimStageState`) requires an **empty version inventory** under the prefix
  (`ListObjectVersions`: zero versions and zero delete markers), so a prefix holding only
  noncurrent versions or delete markers, which looks empty to `ListObjectsV2`, can never be claimed
  (and its history later deleted); then none of the stage's names may be in use; then the
  generation object is created `claimed` (`If-None-Match: *`; a concurrent claim's create fails). A
  later lease needs a new run against a torn-down prefix (or a teardown authorization, below).
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
3. `alchemy destroy` for the stage (or the pippen sweeper), possibly in several partial passes,
   still under lease A.
4. The broker **revokes lease A**, waits until A's sessions have expired, checks that every
   operation started under A has finished (its **drain** check, below), then issues the
   cleanup-only teardown lease B and the teardown authorization (below).
5. `--execute --expect-manifest-digest <sha256> --authorization <file>
   --expect-authorization-digest <sha256>`, under lease B only (all four are required; lease A
   cannot execute): `runRehearsalTeardown`, which deletes nothing until every check has passed, in
   this order:
   1. the run context of the current lease and STS (the current caller is exactly lease B);
   2. the shared bucket's settings (reads);
   3. the **teardown authorization** (below), including distinct leases A and B, A's revocation and
      session expiry no later than B's issuance, and the broker's drain receipt (and, for a
      successor lease, the superseded authorization);
   4. the **independent receipt**: the manifest digest the broker recorded at step 2 (not one
      recomputed from the local file) must equal the manifest's digest, and the authorization's
      manifest digest and mapping UUIDs must be the manifest's;
   5. the manifest complete and bound to the original run (`assertManifestComplete`);
   6. STS again: the caller is still B, immediately before the drain check and the physical readback;
   7. the code's own **drain check** (`assertStateDrained`, `state_not_drained`): every state version
      and delete marker under the prefix except the generation object was last modified strictly
      before A's revocation, and no Alchemy deploy lease (`__lease__.json`, held and refreshed by a
      running deploy and deleted on close) is current unless its `expiresAt` has passed. Every
      listed entry must carry a boolean `IsLatest` and every key exactly one current entry across
      versions and delete markers; a current delete marker means the lease is released, a current
      version is read by its version id and evaluated;
   8. every recorded id gone (`assertManifestGone`): the function by ARN, each queue by URL, each
      mapping by UUID, no metric or composite alarm of the five names, no log group of the exact
      name, and the role per its pinned disposition (deleted: not found; retained: exactly the pinned
      ARN, path and boundary). A remaining id refuses (`state_resources_remain`); AccessDenied is
      never "gone" (`state_gone_unproven`); rerun after the next destroy pass;
   9. no version left (an earlier execute with these receipts completed and only its last response
      was lost; the generation object is deleted last, so an empty inventory means it ended):
      complete, nothing deleted;
   10. otherwise the state under the **generation lock** (`teardownStageState`), STS re-checked
      before the swap and inside every fence:
      1. read the generation object with its ETag and version id. This run's `claimed` generation
         (A's run id, session, issuance, pin): swap it to `tearing-down` with these receipts by
         `PutObject --if-match <that ETag>`; the swap's ETag and version id are recorded. Already
         `tearing-down` with exactly these receipts (a retry): resume, recording its current ETag
         and version id. A successor lease B2 swaps B's `tearing-down` generation instead (below).
         No generation, another run's, or another teardown's: refused
         (`state_provenance_mismatch`), nothing written or deleted;
      2. the **fence**, before **each DeleteObjects batch** (at most 1,000 objects) and after the
         bulk delete: the caller is B, and the generation object's
         current ETag, version id and body are still the recorded `tearing-down` ones; otherwise the
         executor stops;
      3. every other version and delete marker (including the generation's old `claimed` version),
         by the key and version ids listed after the swap (strict page validation; any key outside
         the prefix refuses before anything is deleted), fenced per batch; fence; the re-list must
         then show exactly the recorded generation version;
      4. fenced, the generation object, last and alone, by its recorded version id only; the final
         re-list must show nothing.

   Why a stale executor is harmless: if teardown T1 has read A's `claimed` generation and pauses,
   and meanwhile T2 completes the teardown and a new run B claims the empty prefix, T1's swap
   (`If-Match` on A's ETag) fails against B's generation and T1 stops with B's generation intact.
   If T1 pauses later (after its swap, before any delete), its next fence sees B's ETag and stops;
   any delete it already has in flight names only A's generation's version ids, never B's (tested:
   this interleaving, a replaced generation between the swap and the first delete, a pause before
   each delete, and a replaced generation or changed caller between two 1,000-object batches).

   A partial batch, or a delete that reports success and leaves a version, stops with the
   `tearing-down` generation intact, so no capture or deploy can start; the next execute with the
   same receipts resumes. A claim cannot interleave: it requires an empty version inventory and its
   create fails while any generation object exists. Once the prefix is empty a new claim is a new
   run; a retry of a completed teardown before that is recognised as complete.

   **Recovery from a wedged state** is receipt-bound. With lease B still valid, re-run `--execute`
   with the same manifest receipt and authorization. When B has ended (expired or revoked) before
   the teardown finished, the generation records B's authorization digest, so a plain new lease
   cannot resume it; the **successor path** can: the broker issues lease B2 with an authorization
   that carries `supersedes: { authorizationDigest: <B's digest>, endedAt: <when B ended> }`, and
   `--execute` is run with `--previous-authorization <B's record>`. `assertSuccessor` requires B's
   record to have exactly that digest, the same run, original lease A, manifest and mapping UUIDs;
   B2 on a different session from A and B; and B attested ended after its issuance, no later than
   its own expiry and no later than B2's issuance (so B2 while B is still valid is refused). Then
   the generation must still be `tearing-down` under B's receipts for the same run (a `claimed` or
   foreign generation is refused), and it is swapped to B2's receipts by `If-Match` on its ETag
   before anything else is touched. B2 obeys every other check (A's revocation, the drain, the run
   deadline). There is no code path that clears or overrides a generation object; a manual removal,
   if ever approved, is outside the code and must be recorded with the object's version id and
   body.

   So the digest check runs after the STS and bucket reads and before any deletion. Without
   `--execute` it is a dry run (counts only). The bucket itself and sibling stages are never touched.
   The CLI wrapper keeps the error class (not found, AccessDenied, other), never the CLI's text, so
   a denied read refuses as `state_gone_unproven`, not a generic failure.

   **S3 conditional writes (not provable offline).** The lock relies on `PutObject` honouring
   `If-None-Match: *` and `If-Match: <ETag>` (failing with 412 PreconditionFailed, or 409 on a
   concurrent conditional write) for this bucket in us-west-2. AWS documents conditional writes for
   general purpose buckets; whether this bucket's type (its name ends in `-an`) supports both on
   PutObject cannot be shown from this repository. It must be observed in the sandbox before the rehearsal: a create over an existing
   generation and a replace with a stale ETag must both fail. If either succeeds, the lock does not
   hold and the rehearsal must not run.

### Writer exclusion

In-flight lease A writers (a deploy or destroy still running) are excluded **by revocation, not by
code**: the teardown runs only under lease B, which the broker issues only after A is revoked and
A's sessions have expired, and the authorization record attests both (`revokedAt`,
`sessionsExpireAt`, each no later than B's issuance) and the broker's drain check (below). The
code checks those attested times, that
the current caller is B at the start, before the physical readback and before every delete, and
it refuses an execute without an authorization; it cannot itself observe that A's credentials no
longer work (that is broker receipt 5).

A deploy that holds A does not re-check the generation object before each Alchemy apply step:
Alchemy (`alchemy.run`) exposes no per-resource pre-apply hook this stack uses, so the check runs
once, at the state layer's entry (`prepareStateBackend`, which refuses a `tearing-down` or foreign
generation). A deploy already past that entry when a teardown starts is excluded only by A's
revocation. This is a documented limit.

### Drain

Revocation and expiry stop A from starting anything new; they do not prove that work A already
started has finished (an Alchemy apply or destroy mid-flight, or AWS asynchronous work it
triggered). Two halves:

- **The broker's drain receipt**, in the authorization: `drain: { checkedAt, receiptDigest }`,
  checked no earlier than A's revocation and A's session expiry and no later than B's issuance;
  `receiptDigest` is the sha256 of the broker's drain record (what it checked: no Alchemy process
  for the stage running, no Lambda function or event source mapping update or delete pending, the
  sweeper idle).
- **The code's own check** before the physical readback (`assertStateDrained`): no state write
  under the prefix at or after A's revocation (by `LastModified` of every version and delete
  marker, the generation object excepted) and no current, unexpired `__lease__.json`. The physical
  readback then requires every recorded resource gone: a function or event source mapping still
  updating or deleting still answers its read and refuses (`state_resources_remain`), which covers
  the "no Lambda update in progress, mappings stable" checks by requiring absence.

**Residual:** AWS-side work that leaves no readable trace on the recorded names (for example a
deletion AWS accepted but has not finished propagating, or CloudWatch/Logs ingestion of an already
deleted resource) and a writer outside this account's lease system are not observable by this code;
`LastModified` is S3's clock and is trusted as such. The drain receipt is the broker's evidence for
these.

### Teardown authorization (lease A to lease B)

The teardown lease B (issued with the manifest's two mapping UUIDs for the absence reads) is a
different session from lease A, so the manifest and the generation object, which name A, cannot be checked
against B's context directly. The broker writes a **teardown authorization** at B's issuance and
records its digest (`authorizationDigest`, sha256 of the canonical JSON) in its receipt:

```json
{ "schema": 1, "stage": "aws-rehearse-p1-1126", "runId": "<run id>",
  "original": { "sessionId": "<lease A>", "issuedAt": "<A issuance>", "pin": "<A pin>",
                "revokedAt": "<A revocation time>", "sessionsExpireAt": "<latest expiry of A's sessions>" },
  "drain": { "checkedAt": "<broker drain check time>", "receiptDigest": "<sha256 of the drain record>" },
  "supersedes": { "authorizationDigest": "<superseded B digest>", "endedAt": "<when B ended>" },
  "manifestDigest": "<manifest digest>", "mappingUuids": ["<uuid 1>", "<uuid 2>"],
  "teardown": { "sessionId": "<lease B>", "callerArn": "arn:aws:sts::<account>:assumed-role/agent-session/agent-<lease B>",
                "issuedAt": "<B issuance>", "windowEnd": "<approved teardown window end>",
                "expiresAt": "<B effective expiry>", "policyDigest": "<B cleanup-only policy digest>" } }
```

(`supersedes` only in a successor lease B2's record; see Recovery above.)

`assertTeardownAuthorization` refuses (`state_authorization_mismatch`) unless the record's digest is
the receipt's; it is this stage and run; B is exactly the current session, STS caller ARN and
issuance; A and B are different sessions; A was revoked at or after its issuance and no later than
B's issuance; A's sessions expire after A's issuance and no later than B's issuance; the drain was
checked after A's revocation and session expiry and no later than B's issuance; and A's pin is the issuer pin of the same approved account, boundary, role and
disposition with A's session and issuance. B's validity is capped at the **authorized run
deadline** (`teardownDeadline`): the earliest of A's issuance + 6 h, the hard cutoff
2026-11-27T08:00:00Z and the approved teardown window end the record carries explicitly. B must be
issued before it, its effective expiry must not be later than it (nor than B's own lease end), and
the teardown runs only before that expiry. B's policy digest must be the digest of the
**cleanup-only** teardown policies generated for B's effective expiry and the two mapping UUIDs
(`rehearsalTeardownPolicies`, `teardownPolicyDigest`), so the receipt binds both the expiry and the
action set. The manifest and the generation object are then checked against lease A's context. Any failure
keeps the state.

The cleanup-only policies (inline 1,150 characters; two managed documents, 1,773 and about 4,500; limit 6,144):
deletes of the stage's own resources (`sqs:DeleteQueue`, `lambda:DeleteFunction`,
`cloudwatch:DeleteAlarms`, `logs:DeleteLogGroup` on tagged resources; `lambda:DeleteEventSourceMapping`
and `GetEventSourceMapping` on the two exact mapping ARNs; the role's deletion on its exact ARN only
when its pinned disposition is deleted); the exact-name absence reads; the state prefix's list, reads
and version deletes; one `s3:PutObject`, on the generation object's exact key (the swap). No create, update, tag,
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
5. Revocation of lease A **before lease B is issued** (and the drain record whose digest is the
   authorization's `drain.receiptDigest`): the revocation action taken (for example the
   role's revoke-older-sessions deny, or deleting the managed policy versions) with its time (the
   authorization's `revokedAt`), the latest expiry of A's sessions (`sessionsExpireAt`), and a
   failed call with A's credentials after it; then the same for B at teardown completion.
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
  below).

## Reproduction (offline)

Bun 1.4.0 or later (`package.json` `engines`), from a fresh checkout of this branch. The install
must be a real one in that checkout (the tests import `@alchemy.run/aws` and `alchemy`, which a
bare checkout does not have):

```sh
bun install --frozen-lockfile
bun test ./stacks src/cli
bun test ./stacks/aws/__tests__/pippen-rehearsal.test.ts
bunx tsc --noEmit
bun scripts/search-split-staged-render.ts --check
bun scripts/aws-hardening-mutations.ts --only "rehearsal:"
```

None of these call AWS: every AWS interaction in the tests is a recorded fake.
- The issuance itself (pin, policy versions, STS duration) and the revocation are the broker's, and
  their receipts (above) are not produced by this code; the pin's provenance is the broker's alone.
- Non-adoption under concurrent creation or standalone use of the lease, and create-time tagging
  under the strict tag statement, are residuals for the issuer (see Teardown and Lease).
- **S3 conditional writes** on this bucket in us-west-2 (`If-None-Match: *` and
  `If-Match` on PutObject) are not provable offline; observe both refusals in the sandbox first.
- **Drain residual:** AWS-side work with no readable trace on the recorded names, and writers
  outside the lease system, are covered only by the broker's drain receipt (see Drain).
- **No per-apply generation check:** Alchemy has no pre-apply hook here; a deploy under A is
  checked once at entry and otherwise excluded only by A's revocation.
- **Gates still open:** the non-prod render of the session pipeline; the proof-account stage rule;
  the execution-role wiring (path and boundary); the revised scope and teardown window, including
  lease B and its authorization record; trusted issuance, expiry and revocation; the cost cap; the issuer receipts (pin, policies, revocation of A and B, the drain record, any
  successor B2 record); real conditional-write evidence for this bucket; and live evidence
  (budget and teardown) from a real run.
