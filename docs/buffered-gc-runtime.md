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

Before any deployment, an operator must verify account941433935742/us-east-1,
match the immutable S3 artifact ObjectVersion and ZIP SHA to the reviewed manifest,
and independently review the exact template change set. The template records
ArtifactSha256 but CloudFormation does not verify it. Never treat a parameter
value as proof of uploaded bytes. No deploy command is authorized by this document.

## Database and generation

The one secret is JSON with exactly `hoopdb_url`. It must use session pooler
`aws-0-us-east-1.pooler.supabase.com:5432/postgres`, project pyamkrhoyszgzirqqffq,
and dedicated `bulls96_session_ingest.pyamkrhoyszgzirqqffq` login. The runtime pins
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

Ordered dual-control epoch/version/digest contract: [ordered-ingest-controls.md](ordered-ingest-controls.md).

### Original-record retention masks

Lazy-current custody migration 040550 supplies a required `keepOrdinals` field on every GC ticket: sorted, unique original claim ordinals. `keepGroupIndexes` is derived from that mask. A replacement is necessary when any record is removed, including a partial removal that leaves every group present. Replacement objects retain the original sparse ordinal maps and matching line metadata; subsequent rewrites never renumber records. Cleanup tickets contain empty masks. This runtime must compose with the ordinal-aware SQL contract; it refuses older tickets missing the mask.

GC verifies the stored object, selects the retained lines and reframes them without running the scrubber again. Initial admission still scrubs. Reapplying a non-idempotent scrubber would alter already acknowledged retained values. Four focused regression groups fail when the previous group-only runtime is restored, covering mixed groups, all-groups-present partial removal, invalid masks and a synthetic non-idempotent scrub value. These codec checks do not establish SQL current-seat correctness, deletion races, live transport or production retention; those require the composed PostgreSQL/versioned-object-store proof before admission.
