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
(exit 2), never exit 1.

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
| `SESSION_ANALYSIS_S3_CREDENTIALS` | no | `podIdentity` (default): EKS Pod Identity through `src/shared/locker.ts` `podIdentityCredentials`, which needs `AWS_CONTAINER_CREDENTIALS_FULL_URI` (trusted endpoint only) and `AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE`. Credentials are cached and refreshed 5 min before expiry, and the sweeper's eraser asks for them on each step. `role` reads `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and `AWS_SESSION_TOKEN` from the env (local tests only). IRSA is not supported |
| `PIPELINE_KEY_PREFIX` | yes | The bare key root, as the Lambda reads it |
| `PIPELINE_ORG_FROM_PREFIX` | yes, `true` | The org-prefix layout, as the Lambda requires |
| `READER_MAX_RULESET` | no | As the Lambda: unset keeps legacy sealed reads; `8`..`10` enforces |
| `SCRUB_ENGINE` | no | As every services entry (`docs/scrub-engine.md`); read only when enabled |
| `WORKER_HEALTH_PORT` | no | Probe port, default 8081 |
| `SESSION_ANALYSIS_ASSUME_NO_LEGACY_TURNS` | **must stay unset** | Passed through to `analysisConfig` unchanged. Only unset, `false` or `true` are accepted, and the startup line logs `reason: assume_no_legacy_turns` when it is `true` |
| `SESSION_ANALYSIS_CUTOVER_PROOF` | per the cutover branch | Passed through unread (condition (d) lives on its own branch; the runner never interprets it) |
| `SUMMARY_DECISION_RECORDS_ENABLED` | no | Passed through to the shared builder, exactly as the Lambda reads it |

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
| `sessionAnalysis.keyPrefix` | `PIPELINE_KEY_PREFIX`, plus a fixed `PIPELINE_ORG_FROM_PREFIX=true` |
| `sessionAnalysis.readerMaxRuleset` | `READER_MAX_RULESET` (omit to match the Lambda today) |
| `sessionAnalysis.modelKeySecret` (`name`, `key`) | A Secret volume mounted read-only, with `SESSION_ANALYSIS_OPENAI_API_KEY_FILE` set to its path |
| `sessionAnalysis.databaseSecret` (`name`, `key`) | A Secret volume with `SESSION_ANALYSIS_DATABASE_URL_FILE`; the worker role's own login |
| `sessionAnalysis.postgresTls` | `SESSION_POSTGRES_TLS_PROFILE` and, for `custom-ca`, a CA ConfigMap plus its SHA-256 |
| `sessionAnalysis.serviceAccount` | A dedicated ServiceAccount bound by an EKS Pod Identity association to the role in the S3 and KMS section. Not shared with ingest-edge, which must not gain result-write or version-delete rights |
| `sessionAnalysis.decisionRecords` | `SUMMARY_DECISION_RECORDS_ENABLED` (default unset, as on the Lambda today) |
| `sessionAnalysis.cutoverProof` | `SESSION_ANALYSIS_CUTOVER_PROOF`, passed through |
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

## Activation checklist (blocked)

Each step needs Khaliq's explicit OK in his own words. A request relayed by another agent is not
that OK.

1. Migrations `062120` and `062130` are applied to the target database.
2. The cutover rows are in place, and condition (d), `SESSION_ANALYSIS_CUTOVER_PROOF`, is proven on
   its own branch.
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
