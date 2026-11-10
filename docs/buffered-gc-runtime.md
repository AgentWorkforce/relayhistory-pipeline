# Permanent buffered GC runtime (dormant)

This source is a prerequisite for buffered acceptance. It performs no deployment,
grant, secret creation, admission change, production SQL or routing change.

The deployable entry is `src/session-ingest/buffered-gc-lambda.ts`. The portable
`runBufferedGcJob` uses the same code. Invocation payloads cannot supply factories,
database URLs, credentials, generation or keys. `INGEST_GC_ENABLED` defaults false;
the CloudFormation template uses one explicit switch for both entry and permanent
one-minute EventBridge schedule. An EC2 proof runner and Queue TTL do not provide
this retention service.

## Immutable artifact

Run on an isolated installed worktree:
```sh
bun scripts/build-buffered-gc.ts /home/khaliqgant/relay-briefs/gc-runtime-build-<unique>
```
This uses the actual Alchemy Node bundler and derived-schema build plugin. The ZIP
contains the compiled migration names/generation and all four pure verifier SQL
scripts (search Worker, token verifier, ingest seeder, ingest runtime). The manifest
records verifier, bundle and ZIP SHA256. Build refuses changing source assets.
Detached Node24 default-off and armed portable-job proofs exercise the generated
bundle; the latter uses owned PostgreSQL/MinIO factories, not AWS credentials or
production resources. A test factory does not prove AWS IAM or cloud networking.

Before any deployment, an operator must verify account <account-id>/us-east-1,
match the immutable S3 artifact ObjectVersion and ZIP SHA to the reviewed manifest,
and independently review the exact template change set. The template records
ArtifactSha256 but CloudFormation does not verify it. Never treat a parameter
value as proof of uploaded bytes. No deploy command is authorized by this document.

## Database and generation

The one secret is JSON with exactly `hoopdb_url`. It must use session pooler
`aws-0-us-east-1.pooler.supabase.com:5432/postgres`, project <supabase-project>,
and dedicated `bulls96_session_ingest.<supabase-project>` login. The runtime pins
CA/hostname, rejects arbitrary URL options and owner credentials, and verifies both
current_user/session_user. Pool max2; connect<=1s; operation statement/lock<=1.8s;
global job budget45s/default, <=50s. Cancellation uses the reviewed job-owned
PostgreSQL canceler; failed operations cannot queue a late COMMIT. Pools close in
finally. No role-default modification or backend termination is used.

`INGEST_GC_EXPECT_GENERATION` is required and must match the embedded compiled
generation. GC claim takes {limit,generation}; its lease stores gc_generation.
Every metadata step requires that exact current PASS under SHARE lock before
USER/BATCH/INTENT locks. Buffered producer begin/prepare/commit and selected
selection/reset controls carry their deployment's internal generation. A later
040600/schema upgrade must deliberately rebuild/redeploy Worker and GC with the
new final compiled generation and PASS. Brief503 retries are preferable to a stale
ACK; acknowledged receipts remain metadata inputs for qualified drain.

Org admission has exactly these owner-only fields:
org_id, privacy_baseline_sha256, retention_contract_sha256, schema_generation,
valid_until;040520 adds owner-bound control_epoch. There is no mode column. The ceremony must prove every frozen-Neon
owner packet, seeder ACK, matching compiled PASS and permanent retention evidence.
Hashes alone do not establish completeness. Close by retained-row UPDATE setting
valid_until in the past, never DELETE. Close waits producer/control SHARE locks;
subsequent mutations refuse. Retained enrollment/history also prevents an omitted
acceptanceMode from becoming canonical publication. Qualified040600 drain owns
that separate transition. Initial reads and erasure remain subject to the reviewed
dual-fence contract; this GC does not move Desktop reads.

## S3, KMS and secret prerequisites

The complete transcript bucket and CMK policies are owned by
`stacks/aws/transcript-store.ts`. After a reviewed resource-policy repair, retain
the exact separately managed GC role in the production private input
`transcriptStore.bufferedGcRoleArn` before another whole-stack deployment. The
production validator refuses omission so a future Lambda role-ARN cascade cannot
silently remove that grant. See the continuity procedure in
`runbooks/sessions-prod-transcript-store.md`. This does not create or arm a GC
runtime, change credentials, or relax the existing plan/capture gate.

The template is default off and has no Function URL. Its dedicated Lambda role
has GetSecretValue on one exact secret; GetObject/PutObject/DeleteObjectVersion
only under `96bulls/*/buffered-batches/*`; ListBucketVersions with that prefix;
and exact-CMK Decrypt/GenerateDataKey through S3 with matching object-prefix
encryption context. PUT requires SSE-KMS/exact-key headers. There is no plain
DeleteObject, bucket-wide ListBucket, canonical-prefix access or app role grant.
All runtime keys are additionally checked against buffered-stage grammar.

New approved secret provisioning/resource policies, artifact upload permission,
Lambda role creation/pass-role, bucket versioning and CMK/bucket policies remain
explicit operator prerequisites. A Secrets Manager customer-managed CMK needs
separate reviewed exact-key permission; this template does not invent one.
S3 Bucket Keys change KMS encryption context to the bucket ARN: prove compatibility
or use a reviewed object-context configuration; never silently broaden the prefix
policy. No current raw-reader or eraser credential is assumed to have GC writes.

S3 may return403 for absent objects without ListBucket. GC uses only a conditional
IfNoneMatch create to prove absence; denied reads of existing objects preserve
the object and fail. A successful barrier must be read back with exact body/hash
and concrete current version. Cleanup permanently deletes all other versions and
delete markers, preserves that non-delete barrier forever, then re-lists to prove
only it remains. Late old IfNoneMatch uploads fail. Malformed/incomplete version
inventories refuse before deletion. Inventory has explicit page/version bounds;
overflow is a held cleanup requiring operator investigation, never a successful
erasure.

## Operational acceptance

The permanent minute schedule, dedicated secret, exact IAM/KMS/bucket policies,
enabled function, healthy alarm and actual production-cloud transport must be
reviewed and measured before admission attestation. Source is default off.
Default batch5 (configurable1..20),45s budget and reservedConcurrency1 bound work;
they do not establish an erasure/backlog SLO. Measure healthy-prefix scans,
preparing expiry, mixed replacement and version-delete latency at expected load.
Each successful invocation emits only sanitized examined/finished/deferred counts
and Healthy; no payload, tenant, key, URL, credentials or raw exception is logged.
The missing-heartbeat alarm breaches after the configured enabled window. Alarm
notification/response ownership and backlog coverage remain operator duties.

Rollback closes retained admission, drains/rejects producers and controls, and
keeps GC scheduled until every acknowledged/retired object has verified retention
or erasure evidence. Never disable GC as the first rollback step.

## Existing production policy prerequisites verified read-only

The reviewed template fixes RoleName to `96bulls-sessions-prod-buffered-gc` so exact resource-policy changes can be reviewed before creation. This requires CAPABILITY_NAMED_IAM; the role was absent at the read-only check. Actual bucket versioning is enabled and BucketKeyEnabled=false. The existing bucket policy explicitly denies version deletion to every principal except the old eraser user; the CMK root-administration statement does not delegate data cryptography to arbitrary identity policies. Therefore this new role additionally needs a reviewed buffered-prefix-only bucket exception and an exact-role CMK Decrypt/GenerateDataKey statement. Identity policy alone cannot provide access. No policy was applied.

The operator proposal preserves existing principals/statements, denies this GC role any deletion outside buffered storage and all plain DeleteObject, and grants data cryptography only via S3/account/object-prefix context. Re-read and compare exact semantic preimage hashes before applying; stop on drift. Create the default-off role first; do not enable/admit while policies or transport proof are missing. AWS Access Analyzer accepted the v3 KMS policy without findings; the v3 bucket policy has the documented shorter-prefix warning on the specific canonical-prefix deny. CloudFormation validate-template accepted the named-role template. These checks do not establish effective live permission.

The existing transcript-store K-45 objective is immediate logical deletion and physical purge within 48 hours; it does not impose a waiting canary. The owned capacity case now creates 20 acknowledged batches with 500 records/groups each through the actual producer, then checks the default claim scans all 20 inside its 1.8 second metadata budget. Root measured 305 ms on owned PostgreSQL17/Node24/MinIO (106 S3 requests across 14 groups, about 299 MiB peak RSS). This does not prove production latency, sustained arrival capacity or the physical purge objective. Review actual backlog/throughput and retain active GC while admission closes.

### Claim timeout correction (2026-11-05)

From about 06Z on 2026-11-05 the production function failed every run with `buffered_gc_sql_57014`. The `sql_` prefix comes only from the SQL ledger, and `bufferedGcTick` defers collect-phase `sql_57014`, so the error that escaped was the claim RPC. `ingest_buffer_gc_claim` scans `4*limit` candidates (20 at the template's limit of 5). For each candidate it does the full retention computation in one statement: two original-record normalizations of up to 500 records, plus the ACK-decision and native-slot joins. The job caps that statement at 1.8 s. A healthy backlog of 500-record batches costs about 25 ms per candidate on owned PG17, about 490 ms for 20. That cost grows linearly with the candidate count and slower hosts multiply it. The timeout also rolls back every `gc_checked_at` rotation, so the next run rescans the same head of the queue and fails again. The tick now makes `limit` single-ticket claims. Each claim scans at most four candidates and commits its rotation. Per-tick coverage (`4*limit` candidates), the ticket bound, lease semantics and SQL are unchanged, and any claim error still fails the tick. This change has no schema change, so it does not change the compiled generation. `buffered-gc-claim-budget.postgres.test.ts` reproduces the stuck `claim(5)` against the actual lazy-current producer and shows that the tick makes progress under the same bounded executor and budget.

Ordered dual-control epoch/version/digest contract: [ordered-ingest-controls.md](ordered-ingest-controls.md).

### Buffered GC scope correction (2026-11-04)

IAM glob `*` matches `/`, so `96bulls/*/buffered-batches/*` also matched canonical derived objects whose family was `buffered-batches`. The identity policy now permits only the actual stage producer shape `96bulls/*/buffered-batches/v1/ws=*/m=srv_*/b=*.staged.ndjson.zst`, with explicit canonical `upload-generations` denies for object actions, version listing and KMS encryption contexts. The proposed bucket/CMK policies carry the same boundaries; their v3 proposals supersede the earlier broad-prefix and invalid-KMS-pattern proposals. Nothing has been applied.

Four scope tests use the actual stage-key producer and an IAM wildcard/condition evaluator. Reverting the bounded allow or removing any one of the three canonical denies fails its corresponding adversarial assertion. This checks the declared policy boundary, not effective AWS access: reviewed resource policies, a live transport probe and admission checks are still required before activation.

AWS Access Analyzer rejects KMS context patterns with fewer than six literal characters before a wildcard (`/b=*`). KMS uses the stage namespace through `ws=*/m=srv_*`, pinned to S3 in this account; S3 object permissions still enforce the complete immutable stage suffix. Explicit canonical KMS denial remains. This does not grant direct KMS decryption: the `kms:ViaService` restriction must match S3. The canonical-prefix listing deny may produce the Analyzer shorter-prefix warning; the independent bounded listing allow also excludes shorter/broad prefixes. Neither static validation nor that warning establishes live transport permissions.
### Original-record retention masks

Lazy-current custody migration 040550 supplies a required `keepOrdinals` field on every GC ticket: sorted, unique original claim ordinals. `keepGroupIndexes` is derived from that mask. A replacement is necessary when any record is removed, including a partial removal that leaves every group present. Replacement objects retain the original sparse ordinal maps and matching line metadata; subsequent rewrites never renumber records. Cleanup tickets contain empty masks. This runtime must compose with the ordinal-aware SQL contract; it refuses older tickets missing the mask.

GC verifies the stored object, selects the retained lines and reframes them without running the scrubber again. Initial admission still scrubs. Reapplying a non-idempotent scrubber would alter already acknowledged retained values. Four focused regression groups fail when the previous group-only runtime is restored, covering mixed groups, all-groups-present partial removal, invalid masks and a synthetic non-idempotent scrub value. These codec checks do not establish SQL current-seat correctness, deletion races, live transport or production retention; those require the composed PostgreSQL/versioned-object-store proof before admission.
