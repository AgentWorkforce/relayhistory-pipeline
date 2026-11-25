# Session-analysis runner on EKS (`SERVICE=session-analysis`)

The session-analysis job processor (lane F1) runs today inside the session-pipeline Lambda: the
one-minute `session-analysis-job-schedule` invokes it with `{"sessionAnalysis": true}`
(`src/session-pipeline/lambda.ts`). This document covers the same processor as a long-running pod.
It runs on the agentrelay EKS cluster next to ingest-edge, as [decision 0007](decisions/0007-hosted-sessions-on-agentrelay-eks.md)
asks ("no Lambdas for the pipeline").

**Status: code only, default off.** Nothing here is deployed, and no chart, IAM, Secret, database
or Lambda setting is changed by this repository. Every activation step at the end needs Khaliq's
explicit OK in his own words (AGENTS.md).

## One processor, two runtimes

Both runtimes build the tick's dependencies with the same builder,
`src/session-analysis/runtime-deps.ts` `analysisTickDeps`:

- the Lambda calls it through `lambdaAnalysisDeps` (lambda.ts);
- the runner calls it through `runnerAnalysisDeps` (`src/session-analysis/runner.ts`).

Each passes its own pool, store, credentials and model key. The builder supplies everything else:

- the canonical evidence read (org-from-prefix layout, `READER_MAX_RULESET`, the zstd bound);
- the model transport;
- the decision-records sink (`SUMMARY_DECISION_RECORDS_ENABLED`);
- the orphan-ticket sweeper's versioned eraser.

Both then run `worker.ts` `analysisTick`, which calls `processor.ts` `runAnalysisJob`. That path
is: claim, canonical evidence, model analysis and brief, reserve, create-only put, complete. There
is no fork. `src/session-analysis/__tests__/runner.test.ts` pins the parity.

The runner adds one hook, an optional `stopping()` passed to `analysisTick`. Once SIGTERM arrives,
no further job is claimed in the current tick. The Lambda does not pass it, so its behaviour is
unchanged.

## Process contract

| | |
| --- | --- |
| Entry | `docker/entrypoint.sh` `SERVICE=session-analysis` runs `bun run /app/src/session-analysis/runner.ts` (services image) |
| Kind | Deployment, `replicas: 1` (see "Concurrency") |
| Port | `WORKER_HEALTH_PORT`, default 8081, probes only, never exposed outside the cluster |
| `GET /healthz` | Dormant: 200 `{"ok":true,"enabled":false}`. Enabled: 200 `{"ok":true,"enabled":true,"reason":"starting"\|"ok"}`. Two 503 cases: `tick_failing` after 5 failed ticks in a row (a rotated DB password, an unreachable database), and `loop_stalled` when no tick has ended within `TICK_MS + BATCH x 600 s + 60 s`. That window is sized for a full batch of lease-long jobs, so it is coarse: about 21 min at the default `BATCH=2`, and about 3.4 h at `BATCH=20`. A hung job is therefore detected late at large batches; keep `BATCH` small (the default) |
| `GET /readyz` | 200 once the first tick has succeeded; 503 `starting` before that. Dormant: 200 |
| Cadence | One tick, then a wait of `SESSION_ANALYSIS_TICK_MS`, repeated. A tick sweeps orphan result tickets, then claims and runs up to `SESSION_ANALYSIS_BATCH` jobs one after another (600 s lease each) and stops at the first idle claim |
| SIGTERM / SIGINT | No further job is claimed. The job in flight finishes, then the probe server and the pool close and the process exits 0. A second signal is logged and ignored, so the drain goes on; a third exits 143 at once. There is no lease-release RPC. A pod killed before its job finishes leaves the job to lease expiry (600 s): the job becomes claimable again, with the attempt already counted, exactly as after a Lambda timeout |
| Exit codes | 0 clean stop; 2 refused configuration (do not retry unchanged); 1 any other startup failure |
| Logs | JSON lines through `src/shared/log.ts` (field allowlist). Each tick logs `count` (completed), `failed`, `skipped` (lease lost), `deleted` (orphan tickets swept), `retried` (sweep failures) and `duration_ms`. A failed tick logs `error_code` and the failure streak as `count`. Startup logs `state`, `status` (`model_configured` or `model_not_configured`), `count` (batch) and `port`. No log line carries a key, URL, bucket, transcript text or ids beyond the existing conventions |

## Env contract

Flags are strict. `SESSION_ANALYSIS_ENABLED` must be exactly `true` to run. Unset, empty or `false`
leaves the runner dormant: it reads only `WORKER_HEALTH_PORT` (a malformed port still refuses), and
no other setting or secret. It selects no scrub engine, opens no pool, builds no client and idles
with green probes. Any other flag value refuses startup (exit 2). Every malformed setting below also
refuses startup with a code; none falls back to a silent default. Numbers are trimmed, then must be
plain decimals with no sign, exponent or leading zero.

Once enabled, the order is: settings (including `READER_MAX_RULESET`, refused as
`reader_max_ruleset_invalid`), then both secrets, then the scrub engine (`SCRUB_ENGINE`, as every
services entry selects it). An engine selection failure is the refusal `scrub_engine_invalid`
(exit 2), never exit 1. When the versioning check runs (below), it comes last, before the
pool, the store or the eraser is built and before the probe server listens: its GET is the only store
request made before any refusal. The store, the eraser and the check are built from one resolved target
(`runner.ts` `runnerS3Target`: bucket, region, endpoint, credentials provider), so they cannot differ.

| Variable | Required | Meaning |
| --- | --- | --- |
| `SESSION_ANALYSIS_ENABLED` | yes | `true` runs; see above |
| `SESSION_ANALYSIS_TICK_MS` | no | Wait between ticks, 1000..600000 ms, default 30000 |
| `SESSION_ANALYSIS_BATCH` | no | Jobs per tick, 1..20, default 2. Refused unless `worker.ts` `analysisConfig`, which the Lambda also reads, runs exactly that value, so `005` is refused rather than logged as 5 and run as 2 |
| `SESSION_ANALYSIS_OPENAI_API_KEY_FILE` or `SESSION_ANALYSIS_OPENAI_API_KEY` | exactly one | The model key. The file form (absolute path to a mounted k8s Secret key, 1..65536 bytes, one trailing newline dropped) is the intended one. The key is read once at startup, so a rotation needs a rollout. A key ending in `-disabled` means no model: the runner stays up and claims nothing, as `modelKey` does. Never logged |
| `SESSION_ANALYSIS_DATABASE_URL_FILE` or `SESSION_ANALYSIS_DATABASE_URL` | exactly one | `postgres(ql)://` URL for the worker role (next section). No `ssl*` or `options` query parameters (`postgres-tls.ts`). Never logged |
| `SESSION_POSTGRES_TLS_PROFILE` | no | `supabase` (default, the pinned Supabase CA) or `custom-ca` with `SESSION_POSTGRES_CA_FILE` and `SESSION_POSTGRES_CA_SHA256` |
| `SESSION_ANALYSIS_PG_POOL_MAX` | no | 1..16, default 4 (the Lambda's `HOOPDB_POOL_MAX` default) |
| `SESSION_ANALYSIS_S3_BUCKET` | yes | The pipeline (canonical) bucket the Lambda reads as `PIPELINE_RAW_S3_BUCKET` |
| `SESSION_ANALYSIS_S3_REGION` | yes | That bucket's region, used as the SigV4 signing region and endpoint. There is deliberately no default from `AWS_REGION`: the canonical bucket is in us-east-1 and the EKS cluster in us-west-2 |
| `SESSION_ANALYSIS_S3_CREDENTIALS` | no | `podIdentity` (default): EKS Pod Identity through `src/shared/locker.ts` `podIdentityCredentials`, which needs `AWS_CONTAINER_CREDENTIALS_FULL_URI` (trusted endpoint only) and `AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE`. Credentials are cached and refreshed 5 min before expiry, and the sweeper's eraser asks for them on each step. `role` reads `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and `AWS_SESSION_TOKEN` from the env (local tests only). `static` reads the keys below (MinIO or any S3-compatible store). IRSA is not supported. Any static-key variable set in a mode other than `static` refuses (`credentials_mode_conflict`) |
| `SESSION_ANALYSIS_S3_ACCESS_KEY_ID_FILE` or `SESSION_ANALYSIS_S3_ACCESS_KEY_ID` | with `static`, exactly one | Static access key id, read like the model key (absolute path, 1..65536 bytes, one trailing newline dropped). Read with the secrets, never logged. Refusals: `s3_access_key_id_missing`, `_ambiguous`, `_unreadable`, `_invalid` |
| `SESSION_ANALYSIS_S3_SECRET_ACCESS_KEY_FILE` or `SESSION_ANALYSIS_S3_SECRET_ACCESS_KEY` | with `static`, exactly one | Static secret key (8..256 characters, no whitespace), as above (`s3_secret_access_key_*`). A rotation needs a rollout |
| `SESSION_ANALYSIS_S3_SESSION_TOKEN_FILE` or `SESSION_ANALYSIS_S3_SESSION_TOKEN` | no | Optional session token for temporary static keys; at most one source (`s3_session_token_*`) |
| `SESSION_ANALYSIS_S3_ENDPOINT` | no | Bare origin of an S3-compatible store (the bundled MinIO). Unset is AWS S3 in `SESSION_ANALYSIS_S3_REGION` (the hosted default, unchanged). When set, the store and the sweeper's eraser both use it, path-style, signed for `SESSION_ANALYSIS_S3_REGION` (MinIO's default is `us-east-1`). Allowed: https; http to loopback; http to a Kubernetes Service name `<svc>.<ns>.svc[.cluster.local]` only with `ALLOW_IN_CLUSTER_HTTP=true` (`src/shared/destination-url.ts`, the rule the publisher and the erase job use). No credentials, path, query or fragment. Refusals: `endpoint_invalid` (one code for several causes: plain http outside the allowed cases, in-cluster http without `ALLOW_IN_CLUSTER_HTTP=true`, a path, a query or fragment, userinfo, not a URL), `allow_in_cluster_http_invalid`, and `provider_unsupported` for the GCS API host (any case, with or without a trailing dot) |
| `SESSION_ANALYSIS_S3_VERSIONING_CHECK` | no | Unset: the startup versioning check runs when `SESSION_ANALYSIS_S3_ENDPOINT` is set or the credentials are `static`, and not on the hosted default (whose role has no `s3:GetBucketVersioning` today). `true` forces it. Any other value refuses (`versioning_check_invalid`). See "Versioning" below |
| `LOCKER_PROVIDER` | no | Unset or `s3`: this S3 path, and the hosted path is unchanged. `azure`: the Azure Blob path ([below](#azure-blob-slice-s2)). `gcs` and `fs` refuse startup as `provider_unsupported` (until their versioned erasers exist, slice S3), anything else as `provider_invalid` |
| `ALLOW_IN_CLUSTER_HTTP` | no | Read only when `SESSION_ANALYSIS_S3_ENDPOINT` is set; `true`/`false`/unset, as everywhere |
| `PIPELINE_KEY_PREFIX` | yes | The bare key root, as the Lambda reads it |
| `PIPELINE_ORG_FROM_PREFIX` | yes, `true` | The org-prefix layout, as the Lambda requires |
| `READER_MAX_RULESET` | no | As the Lambda: unset keeps legacy sealed reads; `8`..`10` enforces |
| `SCRUB_ENGINE` | no | As every services entry (`docs/scrub-engine.md`); read only when enabled |
| `WORKER_HEALTH_PORT` | no | Probe port, default 8081 |
| `SESSION_ANALYSIS_ASSUME_NO_LEGACY_TURNS` | **must stay unset** | Passed through to `analysisConfig` unchanged. Only unset, `false` or `true` are accepted, and the startup line logs `reason: assume_no_legacy_turns` when it is `true` |
| `SESSION_ANALYSIS_CUTOVER_PROOF` | per the cutover branch | Passed through unread (condition (d) is read by `analysisConfig`; the runner never interprets it) |
| `SUMMARY_DECISION_RECORDS_ENABLED` | no | Passed through to the shared builder, exactly as the Lambda reads it |

### Store support (slice S1)

The runner works against AWS S3 and any S3-compatible store, including the bundled MinIO:

- **Pod Identity** (default, hosted), the env role (`role`, local tests), or **static keys** (`static`).
- An optional **custom endpoint**, path-style, used by the store and by the sweeper's eraser alike, with the
  same credentials.
- **Versioning is required.** The orphan-result sweeper erases every version of a ticketed result key by
  exact version id. A bucket that is not versioned cannot be swept correctly.
- **GCS and fs are refused** at startup (`provider_unsupported`), pending their versioned erasers
  (slice S3). Azure Blob is slice S2, below.

With no endpoint and Pod Identity (or `role`) the requests are exactly the ones before S1: same URLs
(virtual-hosted on `s3.<region>.amazonaws.com`), same signing region, same credentials, and no new
request at startup.

### Versioning check

When the check runs (see `SESSION_ANALYSIS_S3_VERSIONING_CHECK`), the runner sends one signed, read-only
`GetBucketVersioning` (`GET /<bucket>?versioning`) after reading its secrets and before anything listens
(`src/session-analysis/bucket-versioning.ts`):

| Answer | Result |
| --- | --- |
| `Status` `Enabled` | starts |
| `Status` `Suspended`, or no `Status` (never enabled) | refusal `bucket_not_versioned` (exit 2) |
| MinIO exclusions in effect (`ExcludeFolders` `true`, or any non-empty `Prefix` under `ExcludedPrefixes`): the excluded keys are unversioned | refusal `bucket_versioning_unverified` (exit 2) |
| 4xx, 501, a body over 64 KiB, or a document outside the known shape (one `VersioningConfiguration` root holding only `Status`, `MfaDelete`, `ExcludeFolders`, `ExcludedPrefixes`/`Prefix`; any other element, a repeated one or an unknown value; XML comments are ignored, so a commented-out `Status` never counts) | refusal `bucket_versioning_unverified` (exit 2) |
| 3xx (the endpoint or region is wrong; the signed GET is never redirected) | refusal `bucket_versioning_redirect` (exit 2) |
| other 5xx, network failure, timeout (10 s, including a stalled body) | error `bucket_versioning_unavailable` (exit 1; the pod restarts and retries) |
| the credentials cannot be fetched (e.g. the Pod Identity agent is not ready) | error `bucket_versioning_credentials_unavailable` (exit 1; the pod restarts and retries) |

Limits: the check runs only at startup, so a bucket suspended later is not noticed until the next start.
`MfaDelete` is accepted and not checked; with MFA delete on, the sweeper's version deletes fail loudly
(an S3 error code), never silently. Redirect refusal applies to this GET only; the store and the eraser
are unchanged by S1.

`GetBucketVersioning` rather than a write/list-versions probe: it is read-only (a probe would write an
object and need a version delete to clean it up, leaving residue if the pod dies), it tells `Enabled` from
`Suspended` directly (a probe on a suspended bucket still sees version ids), and AWS S3 and MinIO both
implement it. The credentials need `s3:GetBucketVersioning` on the bucket. On the bundled MinIO,
`SERVICE=locker-init` already enables versioning on the bucket it creates (`src/locker/init.ts`).

### Azure Blob (slice S2)

`LOCKER_PROVIDER=azure` runs the same processor over Azure Blob Storage (`createAzureBlobStore`, Shared Key)
with the strict Azure eraser (`src/migration/azure-blob-erasure/eraser.ts`) for the sweeper. It is default
off like everything here: nothing changes unless `LOCKER_PROVIDER` is exactly `azure`, and unset or `s3`
reads none of the settings below.

| Variable | Required | Meaning |
| --- | --- | --- |
| `SESSION_ANALYSIS_AZURE_ACCOUNT` | yes | The storage account: 3-24 lowercase letters or digits (`azure_account_invalid`) |
| `SESSION_ANALYSIS_AZURE_CONTAINER` | yes | The container, an Azure container name (`azure_container_invalid`). It is the canonical pointers' bucket (`session_index.bucket`), as the Azure Locker writes it |
| `SESSION_ANALYSIS_AZURE_ENDPOINT` | no | Blob endpoint. Unset: `https://<account>.blob.core.windows.net`. The shared destination policy, as for `SESSION_ANALYSIS_S3_ENDPOINT` (https; loopback http; in-cluster http only with `ALLOW_IN_CLUSTER_HTTP=true`); no credentials, query or fragment; the path is empty or exactly `/<account>` (Azurite's path style). Refusals `azure_endpoint_invalid`, `allow_in_cluster_http_invalid` |
| `SESSION_ANALYSIS_AZURE_ACCOUNT_KEY_FILE` | yes | Absolute path to a mounted Secret file holding the account key (one trailing newline dropped): strict padded base64 of at least 32 bytes. The only source: a plaintext `SESSION_ANALYSIS_AZURE_ACCOUNT_KEY` refuses (`azure_account_key_plaintext`). Refusals `azure_account_key_missing`, `_unreadable`, `_invalid`. The key is kept behind a closure and never appears in a config object, an error or a log line |

With `LOCKER_PROVIDER=azure`, every `SESSION_ANALYSIS_S3_*` setting must be unset (`azure_s3_settings_conflict`), and
`SUMMARY_DECISION_RECORDS_ENABLED=true` refuses startup (`azure_decision_records_unsupported`) until the self-host
operator erase can erase decision records on Azure. Shared Key is the only auth: SAS is out of scope, and Entra
workload identity is a later slice.

**What S2 and S2b deliver, and what they do not.** S2 delivers the Azure store, the startup proof, the sweeper and
the strict erase. S2b wires the read API to the same Azure store, so `POST /v1/sessions/:id/analysis` admits jobs on
Azure and `GET` serves their results ([below](#read-api-on-azure-slice-s2b)). The claims scheduler stays off on Azure: it
needs `SUMMARY_DECISION_RECORDS_ENABLED=true`, which this path refuses until the operator-erase slice.

**Account prerequisites.**

- **Blob versioning** enabled on the storage account (an account setting; the startup probe proves it, `blob_not_versioned`).
- **The container** exists (`blob_container_missing`), and the account key is the account's (`blob_auth_refused`).
- **Blob soft delete:** either off, or on with **permanent delete for soft-deleted blobs enabled** on the account
  (the blob service's soft-delete policy, "allow permanent delete"). Otherwise a deleted version stays recoverable and
  erasure cannot complete: the probe refuses `blob_soft_delete_retained` (the account keeps the copy), or
  `blob_permanent_delete_forbidden` (a 403 on `deletetype=permanent`: the permanent-delete permission is not granted,
  which for an Entra identity is the data action
  `Microsoft.Storage/storageAccounts/blobServices/containers/blobs/permanentDelete/action`; the Shared Key carries
  every data permission, so with S2's Shared Key a 403 there points at the account policy).
- The key needs read, write, list and delete on the container; the probe exercises all four.

**Startup proof.** Blob versioning is an account (ARM) setting the data plane cannot read, so the runner proves it
on the data plane before the pool, the store or a listener exists (`src/session-analysis/blob-versioning.ts`),
under the reserved prefix `<PIPELINE_KEY_PREFIX>/.runner-probe/session-analysis/` (never a session key; the
consumer ignores it as `not_a_session_key`):

1. erase probe leftovers there older than 15 minutes (a pod killed mid-probe). Each probe writes its own
   `<uuid>/` directory and a younger copy is another runner's probe in flight, so runners that start together
   never delete each other's probe. Two limits: a pod clock more than 15 minutes ahead of the storage
   service's clock could treat another runner's in-flight probe as a leftover and erase it (that runner then refuses
   `blob_versioning_unverified`: fail closed, and its next start passes); and a crash loop faster than 15 minutes
   leaves one probe directory per start until a later start finds them old enough to erase;
2. put one small create-only probe blob: the answer must carry `x-ms-version-id`;
3. see it in a List Blobs inventory, erase it through the strict eraser and verify that no copy remains.

| Outcome | Result |
| --- | --- |
| version id, erased and verified | starts |
| no `x-ms-version-id` (versioning off) | refusal `blob_not_versioned` (exit 2) |
| soft delete keeps the deleted version and the account refuses permanent delete (400/409: retention) | refusal `blob_soft_delete_retained` (exit 2) |
| soft delete keeps the deleted version and the permanent delete answers 403 (permission) | refusal `blob_permanent_delete_forbidden` (exit 2) |
| 403 (wrong key or account) | refusal `blob_auth_refused` (exit 2) |
| 404 (the container does not exist) | refusal `blob_container_missing` (exit 2) |
| a 3xx anywhere (never followed with credentials) | refusal `blob_versioning_redirect` (exit 2) |
| other 4xx, a malformed version id, a probe the inventory does not show, or an erase that does not verify | refusal `blob_versioning_unverified` (exit 2) |
| 5xx, network failure, timeout | error `blob_versioning_unavailable` (exit 1; the pod restarts and retries) |
| any other failure of the probe itself | refusal `blob_versioning_unverified` (exit 2; a restart would not fix it) |

Like the S3 check it runs only at startup.

**Strict erasure.** The sweeper's eraser lists every copy under a prefix with `include=versions,deleted,snapshots`
and deletes each one by exact identity: snapshots by time, the current version through a base-blob delete
conditional on its listed ETag (a 412 means a successor became current: nothing is deleted, the step fails, the next
sweep relists), then the version itself by id, then older versions by id. On an account with blob soft delete,
a deleted version or snapshot stays listed as soft-deleted and recoverable, so `verifyErased` keeps reporting
pending. The next pass deletes it with `deletetype=permanent`, which needs the account's permanent-delete
setting; without that, the eraser fails with `azure_soft_delete_retained` and the ticket is never retired. A
soft-deleted base blob without a version id can only be undeleted, so it also blocks erasure. Every request
uses redirect `manual`: a 3xx is `azure_redirect`. Budgets, hard ceilings and checkpoints are the shared
traversal's (`createPagedEraser`). Azure's continuation marker is opaque, so a checkpoint resumed by a fresh
eraser relists from the start of the prefix and skips at most 100 provider pages (`azure_resume_unbounded`).
The sweeper erases one key at a time, so in practice this is one or two pages.

**Version ids.** The runner's store turns on the Azure adapter's `versionIds` option
(`packages/object-store/src/azure.ts`): put, head and get then report `x-ms-version-id` as the object version, so a
published result records the version it created, and the sweeper keeps exactly that version. The option is off by
default, so every other Azure store (the Locker, the consumer, the publisher) keeps its ETag-only versions and its
comparisons unchanged. The runner's store never follows a redirect: a 3xx is `azure_redirect`, and a job that
meets one fails as `analysis_store_redirect`, non-retryable, rather than burning attempts.

**Not covered yet on Azure.**

- **`SUMMARY_DECISION_RECORDS_ENABLED` is refused on Azure** (`azure_decision_records_unsupported`) until the
  self-host operator erase adopts the Azure eraser (its own slice). Today that erase refuses anything but S3.
- **Proof.** CI (`pg-proof.yml`) runs `runner-store.azurite.test.ts` against Azurite 3.38.0 pinned by digest.
  With versioning on, it covers the probe, the owned-version sweep and the full erase; with versioning off, the
  `blob_not_versioned` refusal. Azurite has no soft delete or permanent delete, so those paths are proven only
  against the in-memory fake server (`azure-blob-eraser.test.ts`, `blob-versioning.test.ts`). **A run against a
  real Azure storage account (soft delete on and off, permanent delete on and off) is still owed** and is not
  part of S2.

### Read API on Azure (slice S2b)

On `LOCKER_PROVIDER=azure` the read API builds its analysis store with **the runner's own settings parser**
(`src/shared/azure-blob-settings.ts`: `SESSION_ANALYSIS_AZURE_ACCOUNT`, `_CONTAINER`, optional `_ENDPOINT`, and
`_ACCOUNT_KEY_FILE`), so the account, container, endpoint and key rules are the runner's. The read API never reads
`SESSION_ANALYSIS_S3_*`, so unlike the runner it does not refuse them. It uses Shared Key and never follows a redirect. `READ_API_ANALYSIS_BUCKET` switches analysis on,
exactly as on S3, and must name that same container. Admission (`POST`) uses the existing admission pool. Default
off: with `READ_API_ANALYSIS_BUCKET` unset, the routes answer 501 `analysis_not_configured` as before.

A setting the parser refuses, or a `READ_API_ANALYSIS_BUCKET` naming another container, leaves analysis unconfigured
(501) and logs one startup warning: `read_api_analysis_azure_settings_invalid` (with the parser's `error_code`) or
`read_api_analysis_container_mismatch`. The read API still starts; analysis never takes reads down.

**Result reads are the current object at the job's result key**, verified against the recorded sha256 and size
(`readAnalysisResult`), exactly as the S3 read API does. They are not pinned to the owned version id. That is safe
because result keys are written create-only, one per job and digest. A later version at the same key can only be
a re-put of identical bytes, which is served and is correct, or a write from outside the runner, which fails the
digest check. A version with different bytes makes the result unavailable (503 `backend_unavailable`); its bytes
are never served. What this does not give is a fallback to the owned version when someone else overwrites the key.
A result read buffers the whole object before its digest check, without a size bound, as on S3; an oversized
foreign object at a result key is read in full before it is refused (a bound belongs with S2c).
**Version-pinned reads are slice S2c** (a migration exposing `result_version_id` in the read RPC, plus a versioned
get on both stores). S2c needs a migration author and is listed as an unassigned follow-up.

The runner does not read `OPENAI_BASE_URL`, `AWS_REGION` or the Lambda's
`SESSION_PIPELINE_SECRET_ARN`. This keeps the model endpoint and the bucket region exactly the
Lambda's, and leaves Secrets Manager out of the pod.

## Database role

Migration `20261105046000_analysis_jobs.sql` gives the worker's tables and functions to
`service_role`:

- `GRANT ALL` on `session_analysis_jobs_v2`, `session_analysis_requests_v2`,
  `session_analysis_result_tickets` and `session_analysis_ticket_invocations`;
- `SELECT, INSERT` on `session_analysis_ticket_reservations`;
- the ticket sequence;
- `EXECUTE` on `session_analysis_source_revision` and `session_analysis_neon_free`.

The functions' caller pin, `session_analysis_require_caller`, admits `service_role` or the
migrator's configured runtime role (`HOOPDB_RUNTIME_ROLE`, the `<runtime>` entry). The runtime ACL
in `src/hoopdb/migrate-guards.ts` gives that role the same tables.

The Lambda connects with the pipeline secret's `hoopdb_url`, the HoopDb runtime role. **The runner
must use that same role, with its own login and Secret**: the role `hoopdb_url` names, or
`service_role` where that is the role in use. It must not be the migration owner, and must not be
`bulls96_analysis_admit` (that role is the read path's admission principal, EXECUTE on
`analysis_request_v1` only) or `bulls96_read_api`. If the worker's claim fails with SQLSTATE 42501
`session analysis: caller is not a role this function serves`, the role is wrong.

The runner does not run the worker runtime's schema-ledger gate (`/readyz` in `src/shared/worker.ts`),
because the Lambda does not either. The schema it needs is the applied migration set.

## S3 and KMS (Pod Identity role)

The pod role needs exactly what the Lambda's execution role has with `analysisJobs` enabled. See
`stacks/aws/session-pipeline.ts`: `pipelineBucketStatements`, `pipelineAnalysisBucketStatements`,
`pipelineKeyStatement` and `pipelineAnalysisKeyStatement`. Here `<root>` is the key root (`96bulls`)
and `v1` is the layout version.

| Action | Resource | Why |
| --- | --- | --- |
| `s3:ListBucket` | the bucket | Unconditioned, as for the Lambda: a missing manifest answers 404, not 403 |
| `s3:GetObject` | `<bucket>/<root>/*/v1/*` | Canonical manifests and chunks; result verification reads |
| `s3:PutObject` | `<bucket>/<root>/*/v1/*/normalized/session-analysis-v2/*` | Create-only (`If-None-Match`) result puts |
| `s3:DeleteObject`, `s3:DeleteObjectVersion` | the same result glob | The sweeper erases every version of an orphaned result key (the bucket is versioned) |
| `s3:ListBucketVersions` | the bucket, condition `s3:prefix` StringLike the result glob | The sweeper's strict version listing |
| `s3:GetBucketVersioning` | the bucket | Only when the startup versioning check runs (`SESSION_ANALYSIS_S3_VERSIONING_CHECK=true`; off on the hosted default) |
| `kms:Decrypt` | the bucket's SSE-KMS key, `kms:ViaService = s3.<bucket region>.amazonaws.com`, encryption context `arn:aws:s3:::<bucket>/<root>/*` | Reading SSE-KMS objects |
| `kms:GenerateDataKey`, `kms:Encrypt` | the same key, ViaService as above, encryption context restricted to the result glob | Writing results |

Cross-account and cross-region notes:

- **Cross-account.** The bucket and its KMS key live in the sessions account; the EKS pod role lives
  in agentrelay-prod. The pod role needs an identity policy with the statements above, and the
  bucket policy and the key policy must also name the pod role's ARN with the same statements. The
  Lambda's grants are written as bucket-policy and key-policy principals.
- **`kms:CallerAccount`.** The Lambda's key-policy statements pin `kms:CallerAccount` to the
  sessions account. Cross-account, the caller is the pod role's account, so that condition must not
  be copied as is. In the key policy, the pod role's statements replace it with the exact
  principal (the pod role ARN), keep `kms:ViaService` and the encryption-context `StringLike`, and
  may add `kms:CallerAccount` equal to the cluster's account. The pod role's identity policy names
  the key ARN as its resource, not `*`. Both are infra-side changes that need Khaliq's OK.
- **Cross-region.** The pod runs in us-west-2 and the bucket is in us-east-1. Requests are signed for
  and sent to the bucket's region (`SESSION_ANALYSIS_S3_REGION`). The KMS `ViaService` condition
  stays `s3.us-east-1.amazonaws.com`, because S3 calls KMS in the bucket's region. Expect
  inter-region transfer cost and latency on every evidence read, and the same latency on every
  database round trip if the database stays in us-east-1.

No SQS, Secrets Manager, Lambda or EventBridge permission is needed.

## Chart values to request from infra

The chart lives in AgentWorkforce/helm-charts and infra. This repository changes neither. These are
the values the sessions umbrella would need to expose, with nothing enabled by default:

| Value (suggested) | Renders |
| --- | --- |
| `sessionAnalysis.enabled` (default `false`) | Whether the Deployment renders at all. When rendered, `SESSION_ANALYSIS_ENABLED` is a separate value, `sessionAnalysis.run` (default `"false"`), so the pod can ship dormant first |
| `sessionAnalysis.image` | The services image tag, the same one ingest-edge runs; `SERVICE=session-analysis` |
| `sessionAnalysis.replicas` | `1` (see "Concurrency") |
| `sessionAnalysis.tickMs`, `sessionAnalysis.batch` | `SESSION_ANALYSIS_TICK_MS`, `SESSION_ANALYSIS_BATCH` |
| `sessionAnalysis.bucket`, `sessionAnalysis.bucketRegion` | `SESSION_ANALYSIS_S3_BUCKET`, `SESSION_ANALYSIS_S3_REGION` (set from private values, never committed in this repository) |
| `sessionAnalysis.s3Endpoint` (proposed, name owned by the chart; self-host only, unset on hosted) | `SESSION_ANALYSIS_S3_ENDPOINT`, plus `ALLOW_IN_CLUSTER_HTTP=true` for an in-cluster MinIO Service |
| `sessionAnalysis.s3KeysSecret` (proposed, name owned by the chart; self-host only, unset on hosted) | `SESSION_ANALYSIS_S3_CREDENTIALS=static` and a Secret volume with `SESSION_ANALYSIS_S3_ACCESS_KEY_ID_FILE` and `SESSION_ANALYSIS_S3_SECRET_ACCESS_KEY_FILE` |
| `sessionAnalysis.keyPrefix` | `PIPELINE_KEY_PREFIX`, plus a fixed `PIPELINE_ORG_FROM_PREFIX=true` |
| `sessionAnalysis.readerMaxRuleset` | `READER_MAX_RULESET` (omit to match the Lambda today) |
| `sessionAnalysis.modelKeySecret` (`name`, `key`) | A Secret volume mounted read-only, with `SESSION_ANALYSIS_OPENAI_API_KEY_FILE` set to its path |
| `sessionAnalysis.databaseSecret` (`name`, `key`) | A Secret volume with `SESSION_ANALYSIS_DATABASE_URL_FILE`; the worker role's own login |
| `sessionAnalysis.postgresTls` | `SESSION_POSTGRES_TLS_PROFILE` and, for `custom-ca`, a CA ConfigMap plus its SHA-256 |
| `sessionAnalysis.serviceAccount` | A dedicated ServiceAccount bound by an EKS Pod Identity association to the role in the S3 and KMS section. Not shared with ingest-edge, which must not gain result-write or version-delete rights |
| `sessionAnalysis.decisionRecords` | `SUMMARY_DECISION_RECORDS_ENABLED` (default unset, as on the Lambda today) |
| `sessionAnalysis.cutoverProof` | `SESSION_ANALYSIS_CUTOVER_PROOF`, passed through (the Lambda's counterpart is the stack input `transcriptStore.sessionPipeline.analysisJobs.cutoverProof`, default off) |
| probes | `livenessProbe` and `readinessProbe` as HTTP GET `/healthz` and `/readyz` on 8081, with liveness `periodSeconds` 30 and `failureThreshold` 3 |
| `terminationGracePeriodSeconds` | `660` (one 600 s lease plus margin), so a draining job can finish instead of waiting out its lease |
| resources | A starting point of requests 250m / 512Mi and limit 1Gi. Canonical evidence and the model request are held in memory per job, and jobs run one at a time |
| NetworkPolicy | Egress only to the database endpoint, S3 (us-east-1), the Pod Identity agent and the model API. No ingress except the kubelet probes |

The chart must never set `SESSION_ANALYSIS_ASSUME_NO_LEGACY_TURNS`.

## How long one job can run (drain bound)

One job is **not** bounded end to end by its 600 s lease in code. These parts are bounded:

| Part | Bound |
| --- | --- |
| Analysis model call | `SESSION_ANALYSIS_TIMEOUT_MS`, 120 s, abort signal |
| Brief model call | `SESSION_SUMMARY_TIMEOUT_MS`, 20 s, abort signal |
| Result put | `ANALYSIS_PUT_TIMEOUT_MS`, 120 s, aborted |
| Orphan-ticket sweep transactions | `statement_timeout` 60 s per transaction (lambda.ts `transaction`) |

These parts have no deadline of their own:

- the canonical evidence read (manifest and chunk GETs);
- the claim, reserve, complete and fail statements and the pointer and harness queries, which run
  as single statements on the pool (no statement timeout);
- the decision-records insert after completion.

So the typical job is far below 600 s, but a stuck S3 GET or a stuck plain query can hold a job past
its lease. The lease then lapses, another worker may claim the job, and the late `complete` is
refused by the lease and revision checks, so correctness holds. `terminationGracePeriodSeconds: 660`
makes a clean drain likely, not certain. A pod killed in that case falls back to lease expiry. The
Lambda is bounded differently: its function timeout is 300 s for the whole tick. Adding a per-job
wall deadline in the runner is follow-up work, if wanted before activation.

## Concurrency with the Lambda

Claims are leased (`FOR UPDATE SKIP LOCKED` with a 600 s lease and 3 attempts), so a runner and the
Lambda schedule can run at the same time without double-processing a job. Each would still spend
model budget, though. The cutover order is therefore: start the runner dormant, then enable it,
then disable the Lambda schedule (`transcriptStore.sessionPipeline.analysisJobs.enabled=false`, a
stack change). Rollback is the reverse. Keep `replicas: 1`; more replicas are safe but only add
throughput the admission limits do not need.

The Lambda has no reserved concurrency (`stacks/aws/session-pipeline.ts` sets none;
`maxConcurrency` there is the SQS event source's `MaximumConcurrency`, not the function's). One
function serves SQS, every schedule and the analysis tick, so analysis ticks can already overlap:
a tick runs up to 300 s and the schedule fires every minute. Leases keep that safe. While both the
runner and the Lambda schedule are on, the claim rate is the sum of both.

## Claims scheduler (Session Intelligence M1-3)

A side tick of this runner (never the Lambda) that enqueues an analysis for live sessions, so their claims
(`docs/relayfacts.md`, Claims) stay current. Code: `src/session-analysis/claims-scheduler.ts`, wired in
`runner.ts` `runnerTick`. Store side: migration `20261105062180_claims_scheduler.sql` (applying it to a real
database needs Khaliq's OK).

**Pre-apply check (owner).** The migration replaces `analysis_request_v1` (SECURITY DEFINER), which now calls two
owner-only helpers (`session_analysis_admit_live_v1`, `session_analysis_admit_job_v1`) as its own owner. The
helpers are owned by the role that applies the file. If `pg_get_userbyid(proowner)` of
`sessions.analysis_request_v1(text,text,text,text)` is not that role, every user analysis request fails `42501`
after the apply. Compare it with `current_user` before applying.

Gates, all required, each exactly `"true"`: `SESSION_ANALYSIS_ENABLED`, `SUMMARY_DECISION_RECORDS_ENABLED`,
`SESSION_INTEL_CLAIMS_ENABLED`, `SESSION_INTEL_CLAIMS_SCHEDULER_ENABLED` (default off), a configured model key, and
per workspace the M3-1 `claims` control (global AND workspace row; read through `src/shared/session-intel-controls.ts`,
fail-closed, counted as `skipped_control`). With any gate off the runner's tick is the analysis tick itself: no
query, no log line. The `claims` control is checked again by the admission (`control_off`) and by the worker
before it runs a scheduled job (see Kill switch below).

| Env | Default | Bounds | Meaning |
|---|---|---|---|
| `SESSION_INTEL_CLAIMS_SCHEDULER_INTERVAL_SECONDS` | 120 | 30..3600 | at most one scheduler pass per interval, after an analysis tick |
| `SESSION_INTEL_CLAIMS_SCHEDULER_MIN_INTERVAL_SECONDS` | 600 | 60..86400 | per session: at most one scheduled job per interval, and none within it of a completed analysis |
| `SESSION_INTEL_CLAIMS_WORKSPACE_HOURLY_CAP` | 5 | 1..100 | scheduled jobs per workspace in the last hour |
| `SESSION_INTEL_CLAIMS_GLOBAL_DAILY_CAP` | 40 | 1..1000 | scheduled jobs across every org per UTC day |
| `SESSION_INTEL_CLAIMS_DAILY_TOKEN_CEILING` | 5600000 | 1..50000000 | sum of the jobs' token reservations per UTC day |

A value out of range, or not a plain decimal, falls back to its default (as the regen settings do). The database
clamps the same values again.

- **Selection.** `session_analysis_schedule_candidates_v1`: sessions of workspaces whose `claims` control is on,
  with directory activity in the last 2 hours (by canonical id), no native job queued or running, activity newer
  than their last completed or failed analysis (a session whose analysis keeps failing waits for new activity
  instead of being re-enqueued every interval), and neither a completed analysis nor a scheduled job within the
  minimum interval; at most 50 per pass, **round-robin across workspaces**: every workspace's newest session
  first, then every workspace's second, and so on, newest activity first within a round. One busy workspace
  therefore cannot fill the 50 and starve the others. Fallback, documented: the analysis jobs carry no turn coverage, so
  "at least N new user turns since the last analysis" is not derivable from analysis data and is not enforced.
  Future work: the 3-new-turns rule, once the analysis records its turn coverage.
- **Enqueue.** `session_analysis_schedule_v1`, per candidate, as `service_role` (or the runtime role): the same
  locks, evidence check, purge fence and 30-minute reuse as a user request (`analysis_request_v1`'s own blocks, now
  shared helpers), a job-level in-flight check, then the budgets under the per-org lock plus one global scheduler
  lock. Answers `control_off` (the `claims` control is not on globally and for the workspace; checked first, no
  lock), `enqueued`, `reused`, `in_flight`, `not_found` or `over_budget` with a reason (`session_interval`,
  `workspace_hourly`, `global_daily`, `daily_tokens`); over budget is skipped and counted, never deferred. The
  locks make the budget counts atomic only under READ COMMITTED, so the function refuses (`25000`) in a
  REPEATABLE READ or SERIALIZABLE transaction; the tick's calls are single statements at the default level. It writes
  no request row, so it never spends the user request limits. Known limit: `analysis_request_v1` finds an
  in-flight analysis only through a request row (its 046000 check joins requests to jobs), so a user request while
  a scheduled job is queued or running may start a second job; once the scheduled job completes, a request within
  30 minutes on the same pointer set reuses it. Scheduled jobs do not count toward the user's per-session daily
  limit (30 requests), which counts request rows. The scheduler never sets a model: jobs carry the
  pinned versions of `src/session-analysis/versions.ts`.
- **Known limits.** Candidates are read by canonical session id; a job a user requested under a physical session
  id is not seen as covering that session, so the scheduler may enqueue one for it too. At most 50 candidates are
  read per pass (round-robin across workspaces, across every organisation); the database budgets then decide how
  many are enqueued, and the rest are read again on a later pass. With more than 50 eligible workspaces, the 50
  with the newest activity are read first; the others surface as those are enqueued (in flight is excluded).
- **Caps and the UTC day.** The global daily cap and the token ceiling reset at UTC midnight, not on a rolling 24
  hours; the workspace cap is a rolling hour. One busy workspace at the default 5 per hour can fill the default
  global 40 per day in 8 hours, after which no workspace is scheduled until UTC midnight. Size the global cap for
  the number of workspaces enabled.
- **Budget ledger.** `sessions.session_analysis_schedule_ledger_v1`, one row per enqueued job with its token
  reservation; durable, so a restart keeps the counts. A row holds org, workspace, the job's id, the reservation
  and the time, and no session id (the session is read through the job). It references its job `ON DELETE SET
  NULL`: when an erase removes a session's analysis jobs (per-session operator purge, per-user erase, C3 purge, the
  v2 forget trigger) the row stays without its job and keeps counting toward the workspace, daily and token
  budgets, so an erase never refunds spend. Rows older than 8 days are pruned only inside
  `session_analysis_schedule_v1`, opportunistically on each admission; nothing else deletes them, so while the
  scheduler does not run, old rows simply stay (they no longer count toward any budget, all of which are at most a
  day long).
- **Kill switch.** Turning the `claims` control off stops new scheduling (candidates, the tick's own read, and the
  admission's `control_off`) and also stops scheduled jobs already queued: before the worker runs a claimed job
  (on every worker that runs analysis, the Lambda and this runner, whatever `SESSION_INTEL_CLAIMS_ENABLED` and the
  scheduler flag say), it asks `session_analysis_schedule_job_v1` whether the scheduler enqueued it; a scheduled
  job whose workspace control is off is failed as `claims_control_off` (not retried) with no evidence read and no
  provider call, counted `cancelled` and `scheduled_control_off`. User-requested jobs are never stopped by it.
  Without migration `062180` (`42883`) every job runs; the gate remembers that answer for the rest of the tick, so
  such a database sees at most one failing call (one Postgres ERROR line) per tick that claims a job, not one per job. If the gate's read
  fails otherwise, the job's request row decides (counted `scheduled_gate_failed`): a user-requested job (it always
  has one) runs; a job without one is a scheduled job (the worker claims only `native` jobs) and is failed retryable
  (`claims_scheduler_gate_failed`) and tried again after its backoff (15 s, then 30 s). It fails for good on its third attempt (any mix of
  holds and other retryable failures), and the candidates query then skips that session until it has new activity. If the
  request-row read fails too, the job runs. Known limit: the gate finds a scheduled job through its ledger
  row, so a scheduled job still queued after its ledger row was pruned (more than 8 days, and only once a later
  admission ran) reads as unscheduled and runs even with the control off.
- **Token reservation.** Each job reserves its worst case: `ceil(480 KiB / 4)` input tokens (the engine's
  `MAXIMUM_INPUT_BYTES`) plus 16,384 for the response (the full analysis call has no output cap of its own), that is
  139,264 tokens. It is a reservation, not actual spend; reconciling it against the job's recorded
  `input_tokens`/`output_tokens` is a follow-up. The default ceiling, 5,600,000, fits 40 reservations (5,570,560), so
  the daily cap of 40 is reachable. The hosted value is Khaliq's open decision.
- **Fail-open.** The pass runs after the analysis tick in its own try/catch; each call is one statement in its own
  transaction. A failure is counted and logged and never fails the analysis tick, a summary, a brief, a
  user-requested analysis or the read API.
- **Log.** One line per pass, `claims scheduler tick`, stage `claims-scheduler`, with `selected`, `enqueued`,
  `skipped_budget`, `skipped_control`, `skipped` (in flight, reused, not found), `failed` and `duration_ms`; no ids.
  At `warn` when `failed` is above 0, otherwise `info`.

## Activation checklist (blocked)

Each step needs Khaliq's explicit OK in his own words. A request relayed by another agent is not
that OK, except as "Relayed approvals" in AGENTS.md describes.

1. Migrations `062120` and `062130` are applied to the target database.
2. The cutover rows are in place, and condition (d), `SESSION_ANALYSIS_CUTOVER_PROOF`, is proven.
3. The admission role (`bulls96_analysis_admit`) has LOGIN, so the read path can enqueue jobs at all.
4. Decide whether a per-job wall deadline (section "How long one job can run") is required first.
5. The database path from the cluster is settled. Hyperdrive is Cloudflare-only and 0007 removes it,
   so the runner connects directly over TLS. The decision covers which endpoint, which region and
   which pooler.
6. The worker role's login and Secret exist for the runner.
7. The Pod Identity role, the bucket-policy and key-policy grants (cross-account) and the
   association exist.
8. The model-key Secret exists in the namespace, loaded from 1Password and never written to disk.
9. The chart values above are merged in infra and deployed with `sessionAnalysis.run: "false"`.
   Probes are green and the pod is dormant.
10. Set `sessionAnalysis.run: "true"`. `/readyz` is 200 after the first tick, and the tick logs show
   `count` and `failed` as expected.
11. Disable the Lambda's `session-analysis-job-schedule` (section "Concurrency with the Lambda").
12. Khaliq's words for each of steps 1 to 11.
