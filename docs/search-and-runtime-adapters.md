# Search index and pipeline runtime adapters

Scope: portable contracts, Postgres search, and worker-runtime integration with
the existing Postgres stage queue. Worker entry points bind each stage to the
portable runtime while retaining readiness gates, trace context and side ticks.
Search service routing and additional provider bindings remain separate work.

Two seams are pluggable:

- **`SearchIndexAdapter`** decides where the derived transcript search index
  lives: Postgres `pg_trgm`, ClickHouse, OpenSearch, or Elasticsearch.
- **`PipelineRuntimeAdapter`** decides how stage work is triggered and run:
  worker pods, Lambda + SQS, Cloud Run + Pub/Sub, or Terse durable actors.

Both follow the destination registry pattern (#207,
`docs/destination-adapters.md`). Adapters are registered explicitly. There is
no plugin discovery, and registration and selection do no I/O. Selection fails
closed at boot.

## Included implementations

| Part | Path |
| --- | --- |
| Search contract, validation and query reference semantics | `packages/pipeline/src/search-index/contracts.ts` |
| Backend-independent scope reconcile | `packages/pipeline/src/search-index/reconcile.ts` |
| Postgres search using pg_trgm | `packages/pipeline/src/search-index/postgres.ts` |
| OpenSearch / Elasticsearch search over REST (`fetch` only) | `packages/pipeline/src/search-index/opensearch.ts` |
| Shared search conformance | `packages/pipeline/src/search-index/conformance.ts` |
| Runtime contract and shared failure handling | `packages/pipeline/src/runtime/contracts.ts` |
| Worker poll loop and in-process schedules | `packages/pipeline/src/runtime/workers.ts` |
| Shared runtime conformance | `packages/pipeline/src/runtime/conformance.ts` |
| Explicit registration and boot selection | `packages/pipeline/src/adapters/` |

The default search registry (`createSearchIndexRegistry`) contains only
Postgres. It backs `createPostgresSearchComposition` and is the registry the
Lambda composition ABI (`src/lambda-artifact/composition.ts`) exports.
`createOpenSearchCapableSearchIndexRegistry` adds OpenSearch and is opt-in: the
ABI does not export it, and no shipped entry point uses it. The default runtime
registry contains only the worker runtime. Recognizing a provider name does not
install that implementation. Creating an unregistered adapter fails. Each entry
point declares the implementations it can run. Workers refuse
`SEARCH_INDEX_BACKEND=opensearch` whatever is registered. The Lambda entries
allow `clickhouse` plus the names their composition's registry holds, so they
refuse `opensearch` with no composition or with any composition built on the
ABI's registry (`src/session-pipeline/__tests__/lambda-search-backends.test.ts`).
A future composition that runs OpenSearch must choose the opensearch-capable
registry explicitly, as a reviewed change.

ClickHouse, Lambda, Cloud Run and Terse implementation ownership, distribution
and service integration, and wiring OpenSearch into an entry point, are separate
follow-ups. This foundation
does not select their repository placement or advertise them as Helm profiles.

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
  no wildcard meaning. Text and the query use JavaScript `toLowerCase()` as
  specified by `matchesQuery`, including its Unicode behavior; a backend's
  native case matching must reproduce that result.
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

### OpenSearch / Elasticsearch implementation

`createOpenSearchSearchIndex` speaks the REST API with `fetch` only (no client
library) and sends requests only to the configured URL (redirects are refused).
One adapter serves both products; `SEARCH_INDEX_OPENSEARCH_DIALECT` names which
one, because GCP and Azure have no managed OpenSearch (Elastic Cloud only).
`ensureSchema` checks the product (`GET /`: `version.distribution` for
OpenSearch, the `X-Elastic-Product` header for Elasticsearch) and refuses a
mismatch. The backend name stays `opensearch` for both; the adapter `id` is the
dialect.

- One index, `<prefix>-transcript-events`, with every field mapped explicitly
  and `dynamic: strict`. Tenant, session and kind filters are `term` clauses in
  `bool.filter`; nothing is scored.
- `q` is matched like the Postgres reference (lower-cased substring of the first
  4,096 characters of content, or the title) by a `wildcard` over lower-cased
  keyword windows of 4,096 characters at a 2,048 stride. A 500-character `q`
  lower-cases to at most 1,000 code points (`İ` becomes two), and any substring
  of up to 2,049 code points falls wholly inside one window. That arithmetic is
  unit-tested over a fake `fetch`. The real-server tests run three kinds of `q`:
  a 500-character `q` containing one `İ` at four window boundaries of a 9,000-character
  title; `'a'.repeat(500)`; and the maximal folded `'İ'.repeat(500)` and `'ab'.repeat(250)`,
  within one window and past the first window boundary. Each is compared with
  `matchesQuery`, plus a repetitive `q` exactly at and one byte past the
  wildcard bound below. The real-server proof is the `opensearch-proof` CI job.
- Long-literal fallback: a folded `q` longer than 64 UTF-8 bytes
  (`WILDCARD_MAX_LITERAL_BYTES`) is not sent as a `wildcard`. Lucene
  determinizes the `*literal*` automaton under a fixed work limit (10,000), and
  a repetitive literal such as `'a'.repeat(500)` or `'İ'.repeat(500)` costs
  roughly the square of its length, so both products reject it with HTTP 400
  (`too_complex_to_determinize_exception`). The adapter's source comment has the
  arithmetic for the bound (worst case about 4,800 units at 64 bytes, a 2x
  margin). Past the bound, `q` is one painless `script` filter that runs
  `String.contains` over every window value of the two folded fields. `q` is
  passed as `params.q`, and the script source is fixed, so it compiles once
  and `q` never becomes code. This is exact (the same windows, and Java and
  JavaScript both compare UTF-16 code units). It needs doc values on
  `content_prefix_folded` and `task_title_folded`, which the mapping now enables,
  and no other index or cluster setting: inline painless is on by default on
  both products. An index created with the earlier `doc_values: false` mapping
  fails `ensureSchema` with `mapping_conflict` and has to be recreated. The doc
  values store the folded windows a second time on disk. Cost: a
  script filter runs once per candidate document that has already passed the
  tenant, session, source, kind, project and time filters (Lucene checks it
  last, as a two-phase check). So its cost grows with the tenant's matching
  rows, not with the whole index, but every candidate pays a JVM string scan of
  up to about 4,096 characters per window. A cluster that sets
  `search.allow_expensive_queries: false` rejects both the leading-wildcard
  path and the script path.
- Errors: a 4xx/5xx reply surfaces as `http_<status>`, followed by the server's
  `error.type` and `error.caused_by.type` names when it sends them (for example
  `http_400:illegal_argument_exception:too_complex_to_determinize_exception`).
  The server's `reason` text is never included, because it can quote `q` or
  content.
- Cost: a short `q` (the wildcard path) is a leading-wildcard query, so each search enumerates the whole
  folded-term dictionary of every shard it hits. The tenant `filter` does not
  narrow that walk, so the cost grows with the whole index, not the tenant,
  and one tenant's long queries (each up to the request timeout) slow search
  for every tenant sharing the index. The adapter declares this
  (`minIndexedQueryChars` is `Number.MAX_SAFE_INTEGER`) but does not limit it.
  Before shared production use, at least one of these must be in place: a
  per-tenant index or alias (so the term walk covers one tenant), an n-gram
  field that turns `q` into term lookups, or a per-tenant rate limit on search
  at the read API.
- Upserts are `_bulk?refresh=wait_for` with `version_type=external_gte`, so a
  newer stored version wins and reads see writes on return. `index.gc_deletes`
  is `0s`, so a deleted key can be rewritten at any version, as in Postgres.
  The engine only drops a delete tombstone on a refresh after its coarse clock
  (200 ms) ticks, so a write that conflicts with no live document (checked by
  `_mget`) is retried after `_refresh` for up to about 4 s, then fails
  retryably with `deleted_key_conflict`. A conflict whose `_mget` item has no
  boolean `found` (an error item) fails with `bad_response` instead of being
  counted as a newer row. `_bulk` is not atomic, unlike the single Postgres
  `INSERT`: when one item fails, the request throws `bulk_item_<status>` after
  earlier items in the same request have been written. A retry is safe, because
  external versions make each write idempotent. Callers must not assume a failed
  upsert wrote nothing. A basic-auth user needs
  read, write, `_delete_by_query`, `_mget` and `_refresh` on the index, plus
  index creation, mapping updates and reading settings for `ensureSchema`.
- `ensureSchema` reads `index.gc_deletes` back after creating or confirming the
  index. An index that already existed or came from a template may keep the
  60 s default, which the tombstone handling above does not expect. Anything
  other than `0`, `0s` or `0ms` fails `schema_mismatch` (detail `gc_deletes`).
  The adapter never applies settings. An operator sets
  `index.gc_deletes: 0s` (a dynamic setting) and reruns `ensureSchema`.
- `deleteEvents` and `eraseScope` are `_delete_by_query?refresh=true&conflicts=proceed`
  under the tenant filter; an erase repeats while version conflicts remain and
  fails rather than reporting partial erasure.
- Ordering is the contract key order on keyword/long sort fields; paging and
  `scanFingerprints` use `search_after`, so results past `max_result_window`
  are reachable.
- Errors map to `SearchIndexError` codes without backend text: 404 index →
  `schema_missing`, 429/5xx/network/timeout → retryable `unavailable`.
- Auth: `none`, `basic` (password from the secret loader), `sigv4` (not
  implemented: construction fails `unsupported`). `basic` needs an `https` URL
  unless the host is loopback (`localhost`, `127.0.0.0/8`, `::1`). Otherwise
  construction fails `invalid_argument` (detail `basic_auth_needs_https`), so
  credentials never cross a network in cleartext. `CA_FILE` needs Bun.

`.github/workflows/opensearch-proof.yml` runs the shared conformance suite and an
end-to-end upload → scrub → index → search → erase proof on real OpenSearch 2
and Elasticsearch 8 containers (`docs/session-search-proof.md`).

### Postgres implementation

Postgres uses pg_trgm GIN indexes on normalized content-prefix and title columns.
Normalization applies the same lowercase operation as the query reference to the
first 4096 code points of content and to the complete title. Literal `LIKE` under
`COLLATE "C"` avoids database-locale differences for Unicode text.
The derived index has its own schema (`search_index`, configurable); transcript
text never goes in the metadata directory, `session_directory` (decision 0002).
Key columns use `COLLATE "C"` to preserve cursor order. `ensureSchema` requires
pg_trgm to be installed already and performs DDL only when explicitly called.
The table has forced row-level security with a `tenant_only` policy (the same
shape as `projection_state`): rows are visible and writable only where
`org_id`/`workspace_id` equal the transaction's `bulls96_read.org_id`/
`bulls96_read.workspace_id` settings. `ensureSchema` creates it and verifies it
exactly, refusing drift with `schema_missing (tenant_policy_mismatch)`. Every
tenant-scoped statement binds its own tenant: through the configured
`tenantRunner` (a reader's tenant transaction), or else by setting the settings
on the client just before the statement, which only holds inside an open
transaction (a writer's scope pass). The adapter checks that the settings held
and otherwise throws `unavailable (tenant_unbound)`, so a bare pool never reads,
deletes or erases nothing and reports success. Deletes also report, from the
deleting statement, that they were bound and removed every matching row they
saw, and an erase then confirms no scope row is left; a shortfall throws
`unavailable (delete_incomplete)` (retryable). A superuser or `BYPASSRLS` role
skips the policy.

Under RLS the trigram GIN indexes are not used for `q`: `LIKE` (`textlike`) is
not leakproof, so PostgreSQL will not evaluate it before the policy, and a
search filters the tenant's rows from the scope index instead. Search cost is
therefore linear in one tenant's size (about 20-27 ms per query at 200,000 rows
in one tenant on PostgreSQL 17, against 2 s `statement_timeout`). That is
accepted at self-host scale for now; a tenant-scoped search path that keeps
trigram acceleration is a follow-up.

This adapter does not replace RelayHistory's existing transcript search path.
Selecting which service owns self-host transcript queries, migrating any data,
and routing requests through an adapter require a separate reviewed change.

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

The included `workers` runtime claims work from a caller-provided queue,
settles each outcome independently, and fires schedules once per wall-clock
window. The caller supplies the actual queue and idempotent work handler.
Several replicas may run only when queue claims are leased and the work
handler serializes each scope. Schedule handlers must be safe to run on more
than one replica.

`WorkSource.settle` acknowledges completed items, releases retries with their
delay, and parks dead items. The worker binding has a `tick()` that runs one
iteration for callers that own their loop and a `run(signal)` loop for standalone use. Idle
waits release their abort listeners and stop promptly when signaled.

The contract can represent platform queues, push delivery and durable actors,
but it does not implement those bindings. In particular, the Sessions Terse
runtime remains a stub in a separate draft; existing Relaycast/Relayfile actor
profiles do not establish a working Sessions actor runtime.

### Postgres stage delivery

`postgresStageConsumer` bridges `sessions.pipeline_jobs` to a worker-runtime
source and work handler. It carries the existing `StageMessage` in memory and
reuses the existing stage handler, retry policy, event recorder and quarantine
store. The queue schema and serialized message format stay the same.

Claiming a batch leases rows without spending delivery attempts. Immediately
before handling one row, the source renews the waiting rows and counts that
delivery's attempt. Each acknowledgment or retry finishes before the next row
starts. Every state change is fenced by the original lease token; a late
consumer cannot acknowledge another consumer's claim. Settlement counters count
only successful fenced writes.

The generic runtime supports deployment-specific item types alongside schedules.
Sources with a `start` hook use serial delivery and settlement; other sources
retain batch handling. Concurrent `tick()` calls share one active iteration.
The compatibility `pollPostgresStage` entry point also uses this runtime.

On SIGTERM/SIGINT, workers stop starting new deliveries, finish the active
delivery or side tick, and drain health requests before closing database and
telemetry resources. An idle wait wakes immediately. Unstarted leased rows keep
their attempt counts and become available when the lease expires; a new worker
uses the same recovery path after a crash or forced termination. Allow enough
termination grace for the slowest handler; handlers remain idempotent because
delivery is at least once.

Existing per-stage readiness checks and side-tick scheduling remain in the outer
worker loop. A closed gate blocks later stage claims and the side tick. Periodic
components retain their existing timing checks.

## Configuration

| Variable | Recognized names | Current entry-point behavior |
| --- | --- | --- |
| `SEARCH_INDEX_BACKEND` | `postgres`, `clickhouse`, `opensearch` | workers allow `postgres`; the existing Lambda entries allow `clickhouse`, plus `postgres` with a composition from the ABI; no shipped entry allows `opensearch` |
| `PIPELINE_RUNTIME` | `workers`, `lambda`, `cloudrun`, `terse` | workers allow `workers`; the existing Lambda entry allows `lambda` |
| `SEARCH_INDEX_POSTGRES_SCHEMA` | SQL identifier | `search_index` unless explicitly configured; validated when constructing the adapter |
| `SEARCH_INDEX_OPENSEARCH_URL` | `http(s)://host[:port][/path]` | required when `SEARCH_INDEX_BACKEND=opensearch`; creation fails without it. No credentials, query or fragment in the URL |
| `SEARCH_INDEX_OPENSEARCH_DIALECT` | `opensearch`, `elasticsearch` | `opensearch` |
| `SEARCH_INDEX_OPENSEARCH_INDEX_PREFIX` | lower-case `[a-z0-9][a-z0-9_-]*` | `relayhistory` (index `relayhistory-transcript-events`) |
| `SEARCH_INDEX_OPENSEARCH_AUTH` | `none`, `basic`, `sigv4` | `none`; `basic` needs an `https` or loopback URL; `sigv4` is not implemented and fails at creation |
| `SEARCH_INDEX_OPENSEARCH_USERNAME` | string without `:` | required for `basic` |
| `SEARCH_INDEX_OPENSEARCH_PASSWORD` | secret, from the secret loader (`secrets`), never env | required for `basic` |
| `SEARCH_INDEX_OPENSEARCH_CA_FILE` | path to a PEM bundle | unset; https only, Bun runtime only |
| `SEARCH_INDEX_OPENSEARCH_REQUEST_TIMEOUT_MS` | integer, 1 to 600000 | `10000` |

Empty or unset selection variables use each entry point's existing behavior.
Unknown names and recognized but unsupported choices fail at boot. Workers use
the `workers` adapter over their existing stage queues; the Lambda service keeps
its existing implementation. A future entry point must explicitly declare any
broader choices.

The existing worker settings keep their defaults: `WORKER_BATCH_SIZE=10`,
`WORKER_IDLE_MS=1000`, and `WORKER_LEASE_SECONDS=300`. Keep the lease longer than
the slowest delivery. No new deployment setting is required for this wiring.

Secrets arrive through the caller's existing file-mounted secret/client
mechanism. Registry construction, registration and name selection do not read
credentials or perform I/O.

## Future provider implementations and distribution

The [OSS/cloud boundary](https://github.com/AgentWorkforce/build-plans/blob/52920781bb3c99ca0211b025df1b5d922cb2ebb3/proposals/2026-11-04-oss-cloud-split.md)
requires Helm to consume versioned OSS artifacts and lists an OSS
durable-actors/Terse profile. Reusable self-host OpenSearch and a real Terse
runtime must fit that boundary or have an explicitly reviewed distribution
exception before a customer profile can use them. Their placement decision
remains separate from this foundation. Hosted deployment/account policy,
managed credentials and private service composition belong in the hosted layer.

Draft provider implementations currently use a private engine checkout during
development. That is not a versioned consumer artifact or an approved customer
image distribution path. No new Helm selection values, image pins, Terraform
resources, migrations or service routing are provided here.

## Validation

Run the package checks with the repository's pinned toolchain:

```sh
bun run --filter @96bulls/pipeline typecheck
bun run --filter @96bulls/pipeline test
bun test src/shared src/session-pipeline
```

The OpenSearch backend's unit tests use a fake `fetch`
(`test/search-index-opensearch.test.ts`); its real-server suite runs when
`SEARCH_INDEX_OPENSEARCH_TEST_URL` names a disposable server
(`SEARCH_INDEX_OPENSEARCH_TEST_DIALECT=elasticsearch` for Elasticsearch).

The shared search suite exercises PGlite with pg_trgm and can run against a
disposable PostgreSQL database using `SEARCH_CONFORMANCE_PG_URL`. CI runs that
suite on PostgreSQL 15, 16 and 17 before the role-hardening proofs. The runtime
suite checks delivery outcomes, scheduling and overlapping ticks in memory. Stage
queue integration tests use the real queue schema in PGlite to exercise delayed
retry, dead letters, replay, stale leases, shutdown and restart recovery. Worker
orchestration tests cover draining, idle cancellation and side-tick ordering.

These checks cover adapter semantics and existing entry-point compatibility.
Search routing, a deployed provider binding and customer image distribution
need their own integration validation before those features are available.
