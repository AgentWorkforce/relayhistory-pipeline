# C auth store isolation: offline policy proposal

This pure builder and stdin plan command describe a dedicated C-only bucket, CMK and IAM user. They create nothing, read no credentials or environment, import no provider SDK and offer no apply/execute mode. Every result is `deployable:false` with explicit technical holds. Existing E, transcript storage, the ordinary stack dispatcher and frozen C/E/B source remain unchanged.

The dedicated bucket permits bucket-scoped `s3:ListBucket` without exposing E/transcript key names. C's actual service-list call remains prefix-scoped. This deliberately grants C visibility into **all key names in its own auth bucket**, not tenant-level isolation. Keep unrelated apps, E/transcript objects and raw owner source exports out of that bucket.

AWS documents that missing-object GET returns404 with effective ListBucket and403 without it. A prefix-conditioned ListObjectsV2 policy does not establish the behavior of a missing GET. Dedicated bucket-level listing is a candidate to qualify, not proof of effective IAM; bucket policy, identity, SCP, endpoint and KMS constraints still matter. Never translate arbitrary403 into absence. [AWS GetObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html), [prefix conditions](https://docs.aws.amazon.com/AmazonS3/latest/userguide/amazon-s3-policy-keys.html).

## Frozen contract and input boundary

The source contract is C `3b8d86c3045cda422d3a20994a4cf400dc9ec62a`. Exact-source peer reports identify these families, but independent recovery of the critical auth/env blobs and complete deployment source remains a gate:

- Get only: `<root>/authority.json`.
- Get/Put: `<root>/sessions/{cli|service}/<org-hash>/<session-id-hash>.json`; `<root>/index/{id|access|refresh}/<hash>.json`; `<root>/auth-transitions/<session-key-hash>/{<transition-id>,active,<transition-id>.complete}.json`.
- List: runtime calls `<root>/sessions/service/<org-hash>/`; transition recovery uses point reads. Code retains create-only locators and conditional session/head writes; IAM PutObject alone does not prove CAS correctness.

The plan input is one exact flat object with these fields, all non-secret:

```
version: 1
accountId, region
bucketArn, keyRoot, kmsKeyArn
runtimePrincipalArn, ownerBootstrapPrincipalArn, keyAdminPrincipalArn
ePrincipalArn, eBucketArn, eAuthorityKeyRoot, eCandidateKeyRoot
transcriptBucketArn
```

`region` is exactly `us-east-1`; `eCandidateKeyRoot` is exactly `96bulls/upload-inbox/v1`. Supply actual reviewed resource identifiers through stdin, not committed hosted inputs. The builder validates canonical S3/key/IAM ARN shapes, same-account principals/key, a dedicated C bucket, distinct runtime/E/owner principals, safe roots and logical root disjointness. Even across distinct buckets, C root cannot equal or contain/be contained by E's authority/candidate root; this intentionally stronger condition avoids configuration mixups. E and the transcript bucket may be the same bucket. Owner bootstrap and key administrator may be the same trusted owner; neither may be C or E.

Only an IAM **user** is accepted for C's current primitive access-key/secret-key interface. A role/STS broker cannot be inferred because the reported C binding contract has no SessionToken. This selects a proposal shape, not a principal, credentials or resource existence. The public engine does not embed the hosted account ID; root must independently verify the selected account against frozen C's private runtime validator before any release.

Unknown/missing fields, accessors, hidden/symbol fields, credential fields/recognized formats, wildcard inputs, namespace overlap and principal reuse refuse with fixed error codes. No raw rejected input is printed. Exact source provenance, provider identity and full native baseline are not inferred from valid-looking inputs.

## Rendered policies and lifecycle

- Runtime Get is limited to seal/session/index/transition families; Put is limited to mutable families with exact SSE-KMS headers. Runtime seal mutation is denied in both identity and bucket policy. Owner bootstrap gets separate inventory/import/seal grants; these are never installed in C.
- Bucket listing is scoped to the dedicated bucket ARN. Explicit runtime denies prevent Get/Put/List outside the selected scope and crypto use of another CMK. The bucket denies E's data, listing, history, deletion and bucket-administration access even if another identity grant is accidentally widened.
- Current object reads only: no runtime delete, GetObjectVersion, ListBucketVersions, ACL, bucket administration or key administration. Actual effective permissions and missing-object behavior require a real provider probe.
- C and owner data-key/decrypt actions use the exact CMK ARN, S3 regional service/account and auth-object encryption context. The key policy's `Resource:"*"` denotes only the key to which that policy attaches; identity policy names the exact ARN. Named key administration grants no implicit cryptographic use to a separate administrator.
- Proposed bucket settings block public access, enforce ownership, disable Bucket Keys and Object Lock, and propose versioning. **Retention/erasure/restore is unqualified:** no automatic lifecycle expiry is selected. Current auth sessions, recovery intents, locators, tombstones and seal must not expire by importing transcript defaults.

AWS specifies GenerateDataKey for SSE-KMS writes and Decrypt for reads. Without Bucket Keys the encryption context uses the object ARN; enabling Bucket Keys changes it to the bucket ARN and requires a new policy review. [AWS SSE-KMS](https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingKMSEncryption.html).

## Offline command and validation

```
bun scripts/ops/rhc-auth-store-plan.ts < reviewed-nonsecret-input.json
bun test ./stacks/aws/__tests__/rhc-auth-store-policy.test.ts
```

The input file in this example contains only the public fields above; stdin may be piped directly. Credential input is refused, not accepted as a secret-provisioning path. The command supports only no arguments and `--help`; `--execute` and every other argument refuse. The input is capped at64KiB. Parsing/read errors are sanitized. Output is deterministic canonical JSON with a SHA256 over the full proposal excluding its `sha256` field. Results and nested policies are frozen and captured independently of caller mutation.

Tests use synthetic non-secret resource identifiers, a bounded statement-shape oracle, a trapped fetch and a real CLI stdin pipe. They are not AWS IAM simulation or managed404/KMS proof. Negative cases remove/break boundaries and assert refusal; owner Get without write headers and broad accidental E grants are explicitly tested. No resource adapter, key creation, service call, environment loader or normal-stack registration is present.

## Deployment and managed proof gates

Frozen peer-reported C selection is `AUTH_BACKEND=s3` plus `DELIVERY_INBOX=authority-v1`. Its `AUTH_S3_EPOCH` is E's expected **authEpoch**, not controlEpoch. The five S3 public settings are bucket/region/prefix/epoch/key ARN. The three private primitive strings are `AUTH_S3_ACCESS_KEY_ID`, `AUTH_S3_SECRET_ACCESS_KEY`, `DELIVERY_INGEST_SIGNING_KEY`; E's async Secrets Store binding objects are a different contract. `DELIVERY_INGEST.fetch` must target the exact reviewed private E Worker.

A separate accepted deployment map must keep new credential material in RAM/stdin and1Password/CF secret storage: no `.env`, temporary secrets JSON, command arguments, persisted AWS credentials, Alchemy/SST plaintext state or debug logs. The frozen C deploy helper writes a temporary0600 secrets JSON and is excluded from that new credential path. A future RAM-only version uploader needs exact locked API/SDK review and proof that existing bindings, secrets, queues, domains and unrelated public config are preserved. Ordinary secret-put can immediately deploy a Worker; it is not harmless staging. [Cloudflare secrets](https://developers.cloudflare.com/workers/configuration/secrets/).

Root-owned managed tests must cover:

1. Actual C/E/owner identity and exact bucket/key/policy/config/source/bundle/version tuple. Cross-boundary C↔E/transcript Get/Put/List is denied; C cannot mutate the owner seal.
2. Real missing seal, locator, current session, transition active head and completion GET; absence404 and access denial403 remain distinct. Peer-reported C returns null only on404 and storage_unavailable on401/403/timeouts; independent critical-source and provider confirmation remains required. An omitted List source call does not establish GET behavior.
3. Actual create-only and ETag CAS conflicts/readback, response-loss recovery, service-list trailing prefix/pagination, correct CMK and denied/missing/wrong encryption permissions. A rendered policy or successful PUT is insufficient.
4. Owner inventory/import/drain/seal with genuine source/floor/epoch/barrier proof. Provisioning does not authorize fabricating complete/oldWritersDrained/baseline fields or initial consent. Runtime cannot create its own seal.
5. Actual public login/refresh/revoke/upload/ACK/replay under SQL outage and zero SQL initialization/dispatch, with invalid/stale/closed/pending cases refusing. Distinguish RHC auth SQL from any Cloud identity whoami dependency.
6. Reviewed versioning/noncurrent retention, erasure and fenced restoration. Never roll back accepted C lifecycle writes to stale SQL authority.

Human provisioning/deploy authorization is already recorded. This source proposal creates no new human confirmation step; root's exact review, independent source/provider checks and managed readiness are technical gates. No automatic live action follows from generating a plan.
