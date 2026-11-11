# 0004: Desktop uploads go straight to S3 through the upload inbox

**Status:** accepted for implementation (Khaliq, 2026-11-11, relayed by lead-desktop; the exact AWS/IAM/flag actions below still need his own OK each)
**Supersedes:** the "Production AWS: S3 only" clause of
`docs/decisions/0001-production-data-plane.md` (S3 stays the only data store;
STS-minted upload roles, SQS plus a consumer Lambda and KMS grants are added
for ingest); the rest of 0001 stands.

**Source:** the brief lead-desktop relayed on 2026-11-11, which carried
Khaliq's decision and the frozen contract choices.

## Context

Today every desktop delivery goes through the Cloudflare Worker
(`POST /v1/delivery/batches`, `acceptBatch` at `src/session-ingest/app.ts:587`).
The Worker authenticates the request, prepares it, writes an admission inbox
object and enqueues it (`enqueueAdmission`, called at `app.ts:611`, defined at
`src/session-ingest/async-admission.ts:240`). Supabase is on that path for the
identity and privacy-journal reads. The upload bytes therefore cross two
services that do not need to carry them.

The goal is to take Supabase and the CF Worker off the upload hot path. The
desktop writes the delivery straight to S3, and this pipeline admits it
asynchronously through the admission path it already has. RelayHistory Cloud
(RHC) is retired last, after the hosted pipeline carries all traffic.

## Decision

1. **Flow.**
   1. relay-control mints short-lived credentials scoped to one tenant prefix,
      together with a signed witness carrying the user's privacy-journal seq.
   2. relay-desktop PUTs one zstd-compressed delivery request per batch under
      that prefix, create-only.
   3. An S3 notification feeds SQS, and a consumer Lambda re-validates the
      object.
   4. The consumer re-scrubs the object through the same preparation the
      Worker runs (`prepareDelivery`, `src/session-ingest/vendor/delivery/prepare.ts:581`)
      and hands it to the existing async-admission path.
   5. The desktop polls the admission status by upload key.
2. **Contract:** `docs/contracts/relay-upload-inbox-v1.md` (frozen, with test
   vectors). The reference implementation is
   `src/session-ingest/upload-inbox-contract.ts`. Keys, the witness format,
   both credential response shapes, the status shape and the closed
   refusal-code enum are defined there and nowhere else.
3. **Naming:** the area is called the **upload inbox** everywhere. The key
   namespace is `<keyRoot>/.upload-inbox/`. Its leading dot keeps it disjoint
   from tenant prefixes, as with `INBOX_NAMESPACE = ".admission-inbox"`
   (`async-admission.ts:75-80`). It is not the desktop's local
   `delivery_quarantine`. It is also not the `96bulls/upload-inbox/v1`
   candidate root used by the RHC auth store
   (`stacks/aws/rhc-auth-store-policy.ts:5`, `stacks/aws/transcript-store.ts:93`).

## Workstreams and owners

1. **relay-control credentials (relay-control owner).**
   - Add `POST /api/v1/workspaces/{ws}/relayhistory/upload-credentials`.
   - Assume the desktop-upload role with `sts:SetSourceIdentity` and a session
     policy limited to the caller's key prefix.
   - Mint the witness with the shared `UPLOAD_WITNESS_KEY`.
   - Answer 404 while `HISTORY_UPLOAD_DIRECT` excludes the caller.
2. **relay-desktop client (relay-desktop owner).**
   - Fetch credentials, and self-check the response with the contract rules.
   - PUT create-only with the checksum, KMS and witness headers; treat
     200 or 412 as Queued.
   - Poll admission status, and fall back to the HTTP POST path on 404 from
     control.
3. **Hosted pipeline Alchemy stacks (this repository).**
   - **Desktop-upload role:** an IAM role that trusts only the control role,
     with `sts:SetSourceIdentity`. It may PutObject only under
     `96bulls/.upload-inbox/*`, with the session policy narrowing it further.
   - **KMS grant:** lets that role encrypt under the bucket key.
   - **Second bucket notification:** Id `96bulls-upload-inbox`, prefix
     `96bulls/.upload-inbox/`, suffix `.json.zst`, `s3:ObjectCreated:*`. It
     does not overlap the existing manifest rule, which has prefix `96bulls/`
     and suffix `/manifest.json` (`stacks/aws/session-pipeline.ts:441-452`).
   - **Queue:** an SQS queue plus a DLQ with 14 days' retention and an alarm
     on depth. The visibility timeout is 6x the function timeout, the
     convention already used for the existing queue
     (`stacks/aws/session-pipeline.ts:166`).
4. **3-selfhost (AgentWorkforce/infra).**
   - Provide the equivalent role or credentials, the bucket policy and the
     Helm values for the self-host chart.
   - Self-host has no S3 notification, so the consumer finds objects by
     scanning (workstream 5).
5. **Consumer (this repository).**
   - **promoteUpload:** reads the object and applies the contract checks in
     their order: key, witness, binding, bytes, decode, prepare, account,
     origin.
   - **prepareForAdmission refactor:** extracts the part of `acceptBatch` that
     runs before admission, `prepareDelivery`, `assertTenantSegment` and
     `assertWithinBudget` (`app.ts:607-609`), so the Worker and the consumer
     share it.
   - **Upload-admission Lambda:** reserved concurrency 4, SQS batch size 1.
   - **Self-host scan root:** adds `<keyRoot>/.upload-inbox/` next to the
     `.admission-inbox` ListBucket scan
     (`src/session-ingest/selfhost-admission.ts:8`, `:46`).

## Rollout

- **Flag:** `HISTORY_UPLOAD_DIRECT` = `off` | `allowlist` | `on`. The default
  is `off`. Each new behaviour gets its own default-off flag.
- **Canary:** `allowlist` with one workspace first. Compare its admitted
  receipts and refusals against the HTTP path.
- **Widening:** widen organisation by organisation, then switch to `on`.
- **Worker:** retired last. Before that, the selection, reset, erase, limits
  and status routes it still serves must move elsewhere.

## Risks

- **Unscrubbed bytes at rest.** Inbox objects are the desktop's bytes before
  the server-side re-scrub, so for a while unscrubbed content sits in S3.
  Mitigations:
  - Only the consumer may read the prefix. The upload role is PutObject-only,
    and no reader role gets `.upload-inbox/*`. Today's reader and RHC-writer
    policies grant `s3:GetObject` on `96bulls/*/v1/*`
    (`sessionObjects`, `stacks/aws/transcript-store.ts:173`, used at
    `:1361` and `:1428`), which matches
    `96bulls/.upload-inbox/<org>/v1/...`. The workstream-3 stack change must
    add an explicit `Deny` on `96bulls/.upload-inbox/*` to every role except
    the consumer, or the layout must stop matching that wildcard; the
    prefix literal itself stays as frozen.
  - Erase must cover inbox objects and their outcomes (see Consequences).
  - A lifecycle rule expires inbox objects shortly after promotion. Today the
    bucket only expires noncurrent versions, after
    `NONCURRENT_VERSION_DAYS = 1` (`stacks/aws/transcript-store.ts:81`,
    `:1169-1171`).
- **No content-length cap with STS.** A presigned POST can enforce
  `content-length-range`; STS credentials cannot cap object size. The
  consumer refuses objects over `MAX_DELIVERY_BYTES`
  (`src/session-ingest/vendor/delivery/delivery-contracts.ts:115`) before it
  decodes them, but storage abuse up to the credential lifetime is possible.
  Mitigations: per-tenant prefix scoping, the short TTL, and the lifecycle
  rule.
- **Role-chaining TTL and revocation lag.** Credentials chained from the
  control role last at most 1 h. A revoked user or device can keep writing
  until expiry. Admission still re-checks identity and privacy state, and the
  witness `exp` bounds what is accepted.
- **Receipt fidelity under Queued semantics.** The desktop gets "Queued"
  (200 or 412) instead of a synchronous receipt, so a receipt only exists
  after the consumer admits the batch, via the status poll. The status route
  must map refusals and outcomes faithfully. Today's route reads outcomes by
  origin and receipt (`app.ts:869-879`, `admissionStatus` at
  `async-admission.ts:460-468`).
- **Supabase pooling from AWS.** The consumer Lambda reaches Supabase from
  AWS for identity and journal reads. It needs the pooled (transaction-mode)
  connection, and reserved concurrency 4 bounds the connection count.
- **Live `AUTH_BACKEND` is unverified.** The production relayhistory-api
  Worker binds `AUTH_BACKEND` as a secret and no sanitized receipt of its
  value exists; the source default (`neon`) proves nothing about the live
  binding. The deployed RHC head accepts only `neon|dual|supabase`, and the
  S3 auth backend (RHC #319) is not merged. relayhistory-cloud-owner's
  condition, recorded here: the Supabase verifier bridge for the upload
  consumer stays gated until an operator supplies a sanitized receipt
  confirming `AUTH_BACKEND=supabase`. Until then the consumer must not assume
  Supabase is the identity authority for admission.

## Consequences for existing code

- **Admission-consumer key-prefix check.** The consumer accepts only messages
  whose key starts with `<keyRoot>/.admission-inbox/`
  (`src/session-ingest/admission-consumer.ts:73-74`). Upload-inbox events
  need their own entry point, `promoteUpload`. The existing check stays as
  is.
- **srv-writer grant shape.** The srv-writer object grant lists tenant data
  and the inbox-authority roots (`stacks/aws/transcript-store.ts:176-187`).
  The consumer needs read and delete on `96bulls/.upload-inbox/*`. That is a
  separate grant on the consumer role, not a widening of the srv-writer list.
- **The bucket-notification script refuses foreign targets.**
  `scripts/sessions-prod-bucket-notification.sh:80-82` refuses to `put`
  whenever the bucket carries a notification other than
  `96bulls-session-pipeline-manifests`, because `put` replaces the whole
  configuration. Adding `96bulls-upload-inbox` therefore needs that script to
  own both Ids, so that neither run deletes the other's rule.
- **Erase coverage.** The operator session erase is contract-only today and
  answers 501 (`src/session-ingest/session-erase.ts:1-15`). When it ships, it
  must cover upload-inbox objects and their admission outcomes, not only
  tenant data.
- **Unchanged by this decision PR:** `app.ts`, `async-admission.ts`, the
  Worker, the stacks and the vendored delivery files. This PR adds only the
  contract module, its test, the contract and vectors, this record and one
  line in 0001.

## Still needs Khaliq's explicit OK for each exact action

- Creating the desktop-upload IAM role and its trust policy, including
  `sts:SetSourceIdentity`.
- The KMS grant for that role.
- Adding the `96bulls-upload-inbox` bucket notification, and the SQS queue
  and DLQ.
- Deploying the upload-admission Lambda.
- Every `HISTORY_UPLOAD_DIRECT` change (`allowlist` entries, `on`).
- The STS session TTL.
- Pod Identity for the self-host consumer.
- The self-host Helm values.

## Addendum 2026-11-11: S3-compatible self-host stores

Self-host stores such as MinIO have no STS and often no KMS. The
`relay-upload-inbox-v1` contract keeps its id, and three amendments let such
a store use the presigned path:

1. **`kmsKeyId` is `string | null` and always present.** `null` is allowed
   only with `kind: "presigned"`, and it means the desktop signs and sends no
   SSE headers. `kind: "sts"` still needs a non-empty printable key id of at
   most 2048 characters.
2. **`endpoint` may carry a path prefix.** It is `null`, or an http(s)
   `scheme://host[:port]` with an optional path prefix, with no trailing slash
   after the prefix and no query, fragment or userinfo, at most 2048
   characters. relay-control emits it normalised, without a trailing slash. A
   non-null endpoint means path-style addressing for `sts`.
3. **501 `unsupported_by_backend`.** A backend without STS answers a
   `kind: "sts"` request with 501, and the desktop retries with
   `kind: "presigned"`. The 404 `history_upload_direct_off` meaning is
   unchanged.

The existing vectors are byte-identical. One new vector,
`credentials.presignedSelfhost`, covers a presigned body with a path-prefixed
endpoint and `kmsKeyId: null`.

**Consumer.** A new setting, `HISTORY_UPLOAD_INBOX_SSE`, is default-off. Unset,
`""` or `none` requires nothing. `aws:kms` refuses an object whose own HEAD/GET
metadata lacks `x-amz-server-side-encryption: aws:kms`, with the new terminal
refusal code `upload_unencrypted`, before the witness check. Any other value is
a startup error. `ObjectMeta` gains the optional `serverSideEncryption` and
`sseKmsKeyId` fields, which the S3 store fills only when the headers are
present.

**Hosted is unchanged.** Hosted bodies still carry a key id and
`endpoint: null`. Hosted SSE-KMS stays enforced by the bucket policy and the
PUT headers. Turning the consumer check on anywhere is a configuration change
under the same explicit-OK rule as the other settings above.
