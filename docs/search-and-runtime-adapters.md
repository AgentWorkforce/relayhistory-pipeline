# Search index and pipeline runtime adapters

Status: draft, 2026-11-05. Nothing here changes a running install. The
contracts and OSS defaults are additive. The only wired change is a boot
check that refuses a mis-set selection variable. With the variables unset,
which is every current install, behavior is unchanged.

Two seams are pluggable:

- **`SearchIndexAdapter`** decides where the derived transcript search index
  lives: Postgres `pg_trgm`, ClickHouse, OpenSearch, or Elasticsearch.
- **`PipelineRuntimeAdapter`** decides how stage work is triggered and run:
  worker pods, Lambda + SQS, Cloud Run + Pub/Sub, or Terse durable actors.

Both follow the destination registry pattern (#207,
`docs/destination-adapters.md`). Adapters are registered explicitly. There is
no plugin discovery, and registration and selection do no I/O. Selection fails
closed at boot.

## Where each part lives

Khaliq's direction: the provider adapters live in the `*-cloud` repo. This
follows the default split in build-plans#1 (OSS engine, hosted composition).
The split was proposed to `oss-cloud-split-codex` before any code was written.

| Part | Repo | Path |
| --- | --- | --- |
| `SearchIndexAdapter` contract, validation, reference q semantics | sessions-pipeline (OSS) | `packages/pipeline/src/search-index/contracts.ts` |
| Backend-independent scope reconcile (`planScopeReconcile`) | OSS | `packages/pipeline/src/search-index/reconcile.ts` |
| **Postgres + pg_trgm adapter (default)** | OSS | `packages/pipeline/src/search-index/postgres.ts` |
| Shared search conformance suite | OSS | `packages/pipeline/src/search-index/conformance.ts` |
| `PipelineRuntimeAdapter` contract, `settle()` failure semantics | OSS | `packages/pipeline/src/runtime/contracts.ts` |
| **Worker-pod runtime (default)** | OSS | `packages/pipeline/src/runtime/workers.ts` |
| Shared runtime conformance suite | OSS | `packages/pipeline/src/runtime/conformance.ts` |
| Selection (`SEARCH_INDEX_BACKEND`, `PIPELINE_RUNTIME`) and default registries | OSS | `packages/pipeline/src/adapters/` |
| OpenSearch / Elasticsearch adapter (SigV4, basic, API key, none) | sessions-pipeline-cloud | `src/search/opensearch.ts` |
| ClickHouse adapter (wraps the engine's vendored client and read queries) | -cloud | `src/search/clickhouse.ts` |
| Lambda + SQS runtime | -cloud | `src/runtime/lambda.ts` |
| Cloud Run + Pub/Sub runtime | -cloud | `src/runtime/cloudrun.ts` |
| Terse durable-actors runtime (**stub**) | -cloud | `src/runtime/terse.ts`, `docs/runtime-terse.md` |
| Registration of the provider adapters into the engine registries | -cloud | `src/register.ts` |

The -cloud repo consumes the engine at a pinned commit (`ENGINE_REF`), checked
out into a git-ignored `.engine/`. It copies no engine source, and the
ClickHouse adapter imports the engine's vendored code where it already is.
Moving `src/session-pipeline/vendor/search-index/*` into -cloud is a separate,
reviewed source move. Until then, the hosted Lambda keeps calling that code
directly.

## SearchIndexAdapter

```ts
interface SearchIndexAdapter {
  id; backend; capabilities(): SearchIndexCapabilities;
  ensureSchema();                                   // explicit operator step, idempotent
  upsert(docs);                                     // version-guarded, <= 5000 per call
  deleteEvents(tenant, positions);                  // exact stale rows (reconcile)
  eraseScope({ orgId, workspaceId, source, sessionIds });  // erasure, any ts
  scanFingerprints(scope, { fromMs, limit });       // reconcile scan; truncated => never delete
  query({ tenant, sessionId?, source?, kind?, projectId?, q?, sinceMs?, untilMs?,
          order, cursor?, limit, maxContentChars? }); // -> { hits, next? }
  health();
}
```

Every adapter must reproduce these semantics. The conformance suite checks
each one against the reference oracle (`matchesQuery`):

- **Rows:** one row per event, keyed by tenant (`orgId`, `workspaceId`) plus
  `(tsMs, sessionId, source, machineId, kind, eventId)`. If an event's ts or
  workspace moves, it becomes a new row, and reconcile deletes the old one.
- **Search (`q`):** a case-insensitive *literal* substring of the first 4096
  code points of `content`, or of `taskTitle`. `%`, `_`, `*`, `?` and `\` have
  no wildcard meaning. This is the ClickHouse read path's
  `positionCaseInsensitiveUTF8` semantics. RelayHistory's recall path does not
  escape `q`; this contract does.
- **Order and cursor:** page order is code-point (byte) order over the
  position tuple, ascending or descending. A cursor is the last position
  served. A page can come back short and still carry `next`; the ClickHouse
  read path behaves the same way.
- **Versions:** an upsert never replaces a row with a greater `version`.
  ReplacingMergeTree, `ON CONFLICT ... WHERE version <=` and OpenSearch
  `external_gte` all enforce this.
- **Erasure:** `eraseScope` returns only once no row of the scope is
  readable (`synchronousErasure`).
- **Results:** a hit has the read API's row shape. The index-only
  `taskRefId` and `deliveryRecordId` columns are stored but not returned.
- **Errors:** errors carry bounded codes only. They never include content,
  credentials or backend error bodies.

Tenant ids must come from verified auth. The adapter filters on them; it does
not authenticate them.

### Implementations

| Backend | Where | Text index | Auth | Notes |
| --- | --- | --- | --- | --- |
| `postgres` (default) | OSS | `pg_trgm` GIN on `left(content, 4096)` and `task_title`, the same expressions RelayHistory indexes | the DB role | Own schema (`search_index`, configurable), `COLLATE "C"` key columns, never `session_directory` (decision 0002). `ensureSchema` never creates the extension. Works on Cloud SQL and AlloyDB, which support `pg_trgm`. |
| `clickhouse` | -cloud | the engine's `ngrams(3)` text index | user/password (secret) | Wraps the existing schema, migration, `deleteRows` and two-round-trip read queries. The schema needs a ClickHouse with the `text(tokenizer = ngrams(3), preprocessor = ...)` index: 26.3 passes, 25.10 rejects it. |
| `opensearch` | -cloud | 3-gram tokenizer + lowercase on `content_prefix` and `task_title`; `match_phrase` over consecutive 3-grams is an exact substring match; q under 3 chars uses `wildcard` on a lowercase keyword subfield | SigV4 (`es`, `aoss`), basic, API key, none | Strict mapping, index template, `_bulk` with `external_gte` versions, `search_after` cursor, `_delete_by_query` plus a `_count` check for erasure. Serverless (`aoss`) has no external versioning or `refresh=wait_for`, so its capabilities say so. |

### GCP search: decision

The GCP equivalent of OpenSearch is **not a new adapter**:

1. **Standard: `postgres` on Cloud SQL or AlloyDB.** `values-gcp.yaml`
   already uses Cloud SQL for HoopDb, and both services support `pg_trgm`.
   This adds no new service, so it is the GCP default, like the self-host
   default.
2. **Scale: `opensearch` against an Elasticsearch-API search cluster on GCP.**
   That is either Elastic Cloud on Google Cloud (Marketplace, billed through
   GCP; `OPENSEARCH_AUTH=apikey`) or self-managed OpenSearch on GKE through
   the OpenSearch Kubernetes Operator (`basic` or `none` inside the VPC). The
   adapter uses only APIs common to OpenSearch 2.x and Elasticsearch 8, and
   it passes the same conformance suite on both (see Test evidence).

Rejected:

- **Vertex AI Search.** It is a semantic, document-level retrieval product.
  It has no exact substring or 3-gram semantics and no keyset cursor in our
  page order, its indexing is eventual, and erasure is not synchronous. It
  cannot meet the contract. Treat it as a possible future semantic search
  feature, not this index.
- **BigQuery search indexes.** These are token-based, so they cannot do
  arbitrary substring matching, and per-query latency and cost do not suit an
  interactive read API.
- **Firestore and Datastore.** They have no substring search.

## PipelineRuntimeAdapter

```ts
type WorkItem = { type: "manifest-landed", id, key, bucket?, sequencer?, etag?, version?, deleted?, attempt }
              | { type: "schedule", id, job, firedAtMs, attempt };
type WorkOutcome = done | retry(delaySeconds, code) | dead(code);
interface PipelineWork { handle(items, ctx): Promise<Map<id, WorkOutcome>>; schedules: { job, everySeconds }[] }
interface PipelineRuntimeAdapter<Binding> { kind; capabilities(); bind(work): Binding }
```

The engine supplies the work, an idempotent batch handler. The runtime
translates its platform's triggers into work items, and translates outcomes
into the platform's ack, retry and dead-letter mechanics. Every runtime calls
`settle()`, so the failure handling is identical on every platform: a handler
that throws retries the whole batch, and a missing or bogus outcome is retried.
Delivery is at-least-once everywhere.

| Runtime | Where | Trigger | Batch / partial | Retry delay | Schedules | Scope serialization | Dead |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `workers` (default) | OSS | poll (HoopDb queue claim) | n / yes | yes | in-process, one run per wall-clock window | advisory lock | queue table |
| `lambda` | -cloud | S3 notification → SQS → Lambda | 10 / partial batch response | yes, re-sent with `DelaySeconds` (at most 900) when `requeue` is wired | EventBridge: `{"schedule":job}` plus the legacy `{"reconcile":true}` and `{"brief":true}` | advisory lock | SQS DLQ after `maxReceiveCount` |
| `cloudrun` | -cloud | GCS notification → Pub/Sub push → Cloud Run | 1 / n/a | no: a nack falls back to the subscription's backoff | Cloud Scheduler `POST /schedule/<job>` | advisory lock | dead-letter topic (`maxDeliveryAttempts`), or an explicit publish |
| `terse` (stub) | -cloud | ingress routes each manifest to a per-scope actor | 1 / yes | actor alarm | clock actor alarm | the actor | persisted actor state |

The Lambda adapter reuses the engine's `parseQueueBody`. S3 test events,
reconcile bodies, URL-encoded keys and delete events therefore behave exactly
as in today's `src/session-pipeline/lambda.ts`. The Cloud Run adapter maps
`OBJECT_FINALIZE` to a manifest that landed, `OBJECT_DELETE` and
`OBJECT_ARCHIVE` to `deleted`, and ignores `OBJECT_METADATA_UPDATE`.

Terse is accounted for in the interface. `scopeSerialization: "actor"`,
`scheduler: "actor-alarm"` and `deadLetter: "actor-state"` exist for it. The
design and the blocking network constraint are in
sessions-pipeline-cloud `docs/runtime-terse.md`.

## Configuration

| Variable | Values | Default | Validated |
| --- | --- | --- | --- |
| `SEARCH_INDEX_BACKEND` | `postgres`, `clickhouse`, `opensearch` | `postgres` for workers; `clickhouse` for the Lambda entry | boot. Unknown names, names the entry cannot run, and unregistered names fail |
| `PIPELINE_RUNTIME` | `workers`, `lambda`, `cloudrun`, `terse` | `workers`; `lambda` for the Lambda entry | boot |
| `SEARCH_INDEX_POSTGRES_SCHEMA` | identifier | `search_index` | adapter construction |
| `OPENSEARCH_ENDPOINT`, `OPENSEARCH_INDEX`, `OPENSEARCH_AUTH` (`sigv4`, `basic`, `apikey`, `none`), `OPENSEARCH_REGION`, `OPENSEARCH_SERVICE` (`es`, `aoss`), `OPENSEARCH_USERNAME` | | auth `sigv4`, index `sessions-transcript-events` | -cloud `openSearchConfigFromEnv` |

Secrets (the OpenSearch password or API key, ClickHouse credentials) are
never environment values. They reach the factory resolved from mounted files
or the existing Secrets Manager secret. With SigV4, credentials come from the
pod identity chain (EKS Pod Identity or IRSA), and no static key is used.

Validation is wired into two places. `runWorker` (`src/shared/worker.ts`)
accepts only the `workers` runtime. `lambdaEnv` (`src/session-pipeline/lambda.ts`)
accepts only `lambda` + `clickhouse`. Neither entry dispatches through the
adapters yet: moving `indexSearchScope` onto `SearchIndexAdapter` +
`planScopeReconcile`, and the tick loops onto `createWorkerRuntime`, is the
next reviewed step.

## Capability matrix per cloud

| | AWS | GCP | Any Kubernetes (self-host) |
| --- | --- | --- | --- |
| Search, standard | `postgres` on RDS/Aurora PG 17 | `postgres` on Cloud SQL or AlloyDB | `postgres` on CNPG (chart default) |
| Search, scale | `opensearch` on Amazon OpenSearch Service (SigV4 `es`) or Serverless (`aoss`) | `opensearch` on Elastic Cloud on GCP (`apikey`) or OpenSearch on GKE (`basic`) | `opensearch` (operator), or `clickhouse` OSS |
| Runtime | `lambda` (S3 → SQS → Lambda, EventBridge) or `workers` on EKS | `cloudrun` (GCS → Pub/Sub → Cloud Run, Cloud Scheduler) or `workers` on GKE | `workers` |
| Terse | `terse` on EKS (durable-actors S3 backend, per-actor STS) | `terse` on GKE (GCS backend, CAB tokens) | `terse` with the durable-actors chart |

## What Helm and Terraform need

The Terraform modules in helm-charts `terraform/` (a separate lane) should
expose these outputs, so an adapter's configuration is a direct mapping:

| Module | Outputs the adapters read | Maps to |
| --- | --- | --- |
| `aws/postgres` | `endpoint`, `port`, `database`, `runtime_role`, secret ARN | `HOOPDB_URL` (Secret); the postgres search uses the same database |
| `aws/search` (OpenSearch) | `endpoint` (https, VPC), `domain_arn`, `region`, `index_prefix` (optional), an IAM policy ARN granting `es:ESHttpGet/Put/Post/Delete/Head` on `domain_arn/*` to the pipeline role | `OPENSEARCH_ENDPOINT`, `OPENSEARCH_REGION`, `OPENSEARCH_SERVICE=es`, `OPENSEARCH_AUTH=sigv4`; attach the policy to the Pod Identity / Lambda role |
| `aws/functions` (Lambda + SQS) | `queue_url`, `queue_arn`, `dlq_arn`, `function_role_arn`, schedule rule names; the bucket notification on the manifest suffix | `SESSION_PIPELINE_QUEUE_URL`; `PIPELINE_RUNTIME=lambda`; `requeue` sends to `queue_url` |
| `aws/object-storage` | `bucket`, `region`, KMS key ARN | `PIPELINE_RAW_S3_BUCKET` |
| `gcp/postgres` (Cloud SQL) | `connection_name`, `database`, IAM user | `CLOUD_SQL_CONNECTION_NAME`, `HOOPDB_URL` |
| `gcp/search` | `endpoint`; for Elastic Cloud, a Secret Manager secret id holding the API key; for OpenSearch on GKE, the Service DNS name and a basic-auth secret | `OPENSEARCH_ENDPOINT`, `OPENSEARCH_AUTH=apikey` or `basic` |
| `gcp/functions` (Cloud Run + Pub/Sub) | `service_url`, `topic`, `push_subscription` (OIDC service account, dead-letter topic, `maxDeliveryAttempts`), Cloud Scheduler job names that target `/schedule/<job>`, the invoker service account | `PIPELINE_RUNTIME=cloudrun`; the GCS notification on the manifest prefix publishes to `topic` |

The Helm values that would follow are planned, not in this PR. The existing
plan reserves `search.backend: postgres | clickhouse`. Adding `opensearch`
needs:

- `search.backend: opensearch`;
- `search.opensearch.{endpoint, index, auth, region, service, username}`;
- `search.opensearch.existingSecret` and `search.opensearch.secretKeys`
  (file-mounted, never inline);
- a `worker.pipelineRuntime` value, `workers` only, because the chart runs
  pods.

The chart renders those values into the environment variables above. Changing
`search.backend` back to `postgres` is the rollback.

## Test evidence

Synthetic data only, local containers, 2026-11-05:

- OSS: `bun run --filter @96bulls/pipeline test`. The search conformance
  suite (11 cases) runs on PGlite + pg_trgm in every CI run. It ran locally
  against real PostgreSQL 17 (`postgres:17` container), and CI's pg-proof job
  runs it on 15, 16 and 17 (`SEARCH_CONFORMANCE_PG_URL`). The suite also has
  the runtime conformance cases (7, workers) and registry tests. The root
  `bun test src/session-pipeline/__tests__/units.test.ts` covers the Lambda
  selection guard.
- -cloud: `bun run test`. The search suite passed on OpenSearch 2.19.1,
  Elasticsearch 8.17.0 and ClickHouse 26.3. The runtime suite passed for the
  `lambda` and `cloudrun` runtimes, and SigV4 matches AWS's published test
  vectors.
- Not proven: a SigV4 request against a real Amazon OpenSearch domain (no
  account access in this lane); OpenSearch Serverless; Elastic Cloud with an
  API key; a deployed Cloud Run or Lambda binding.
