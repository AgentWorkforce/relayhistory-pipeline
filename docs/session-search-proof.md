# Transcript search proof, Stage 1

**SHAPE FIDELITY: PARTIAL.** This uses sanitized aggregate anchors only (about 4.08M
events, about 1.3M in the whale org), not a fresh production-derived shape artifact. It
is not production-scale parity and cannot close T0. G6 and the production-shape gate
stay open.

Design and proof only. Synthetic data on vanilla ClickHouse 26.8.34.13, one local node.
Nothing here ran on ClickHouse Cloud, ObsessionDB or any hosted service, and nothing is
deployed. `benchmarks/session-search/guard.ts` refuses non-local targets. Design:
relayhistory-cloud #233.

```sh
clickhouse server -- --http_port=8123 --custom_settings_prefixes=SQL_   # Docker image has it set
bun test ./benchmarks/session-search/e2e
BENCH_SCALE=M BENCH_SHAPE_FILE=benchmarks/session-search/e2e/shape-2026-10-31.json \
  bun benchmarks/session-search/e2e/proof.ts
```

**Overall verdict: INCOMPLETE/HOLD. G6 PARTIAL, G7 and G8 RELATIVE, G9 inherited FAIL, G11 NOT RUN.** The run reports this whenever any gate is not PASS.
A green CI job means the proof executed, not that it passed.

## What runs for real

Real: `runCourierOnce`, `buildIngestApp` with its scrubbing chunk hop, the pipeline
queue through `object-landed` and `session-indexed`, the migrations, ClickHouse.

Substituted or modelled: PGlite for Postgres, an fs store for object storage, the
legacy pool (substring on the first 4,096 characters, PGlite), hydration through
Supabase (not exercised; #72 and #73 cover RLS). `search-index` is proof code called
from `session-indexed`; no such queue stage exists. The MCP shapes are matched in the
store, but the MCP server was not run.

## Gates

Numbers are from the local run and CI on superseded head `60507a8`, before the machine
claim change, at 4.04M events, whale 1.28M. Local data was on an external drive. Timings
are relative. Check this PR's CI at its exact head for current logic gate results.

| Gate | Result | Evidence and limits |
| --- | --- | --- |
| G0 Negative controls | PASS | Removing the org predicate, the substring recheck or the dedup token each changes the answer; a wrong result scores recall below 1 |
| G1 Courier to search | PASS | First hit, planted secret absent with a positive control, append, retry dedup, stale, quarantine, opt-out. Uploads carry the signed machine claim; absent and mismatched claims are refused with exactly `machine_claim_refused` and no job, pointer or row, and a refusal reports G1 BLOCKED. A second org runs through the real ingest with `PIPELINE_ORG_FROM_PREFIX` and `TENANT_ORG_IDS`, stays hidden from the first, and an unlisted org is refused with exactly `org_not_allowed` |
| G2 Lexical recall | PASS | Recall@5 and @10 of 1.0 and exact counts against the generator on 14 query classes in 2 orgs: phrase, error, path, PR number, case, substring, CJK, accents, 2-character, punctuation, past 4 KB, ultra-rare, zero hits, common |
| G2b Adversarial paging | PASS | Scattered grams newest with true hits older, full pages, tenant, workspace and machine scope, equal timestamps across cursors, token boundaries, case and punctuation |
| G3 Context | PASS | Window contains the match, focus finds the event, chronological, snippets at most 320 characters, citations, cursors do not repeat |
| G4 Federation | PASS on a small fixture | Before and after T0, straddling session and backfilled copy once each, pagination, each pool down is named. Event ids are shared by construction; the real legacy-to-store id mapping is unproven. Pre-T0 text past 4 KB stays unsearchable without a backfill |
| G12 Filter matrix | PASS, store level | Nine filters alone, every pair, all together, and workspace scope, each paged and compared with an oracle. Project, agent, task ref and tags are copied at index time, so a later rename is unhandled. Filter-only listing stays in Supabase |
| G13 Product read path | PASS, narrow scope | Real `requireAuth`, scope from verified claims, a hit without a live pointer dropped; 408, 500, 200 with an exception, and an interrupted stream each refused whole with no retry; one 5 s budget across windows. The token borrows the upload assertion shape, because this ingest has no read-token class. Hydration is an in-memory set, not Supabase. This is not the MCP server, Cloud or a full product e2e |
| G5 Tenant isolation | PASS | Canaries across orgs and workspaces, injection strings, context, crafted cursor, reader cannot write, missing setting fails closed. The row policy is a backstop: per #233 anyone who can set the value reads that tenant |
| G6 Shape | **PARTIAL, gate open** | Event totals within 5%. Event sizes, session sizes, share over 4 KB and growth are assumed |
| G7 Latency and load | RELATIVE, not a pass | See below |
| G8 Freshness | RELATIVE, not a pass | Store only, one node. Same-run p95 114 to 151 ms at 8 clients and 423 to 498 ms at 32, with no layout penalty. Pipeline queue time on the real service is unmeasured |
| G9 Noisy neighbour | **Inherited FAIL** | #233 gate 7 fails 2x. Not rerun |
| G10 Delete | Logical PASS | Gone from search and context. Physical purge is #70 gate 3, on vanilla ClickHouse only |
| G11 Cost and teardown | NOT RUN | Plan below; no account exists |

| Clients, with concurrent ingest | QPS | p50 ms | p95 ms | p99 ms | Errors |
| --- | --- | --- | --- | --- | --- |
| 1 | 25 | 37.0 | 52.2 | 156.8 | 0 |
| 8 | 90 | 75.5 | 157.7 | 378.1 | 0 |
| 32 | 144 | 219.0 | 342.5 | 420.8 | 0 |

The GitHub runner, at the same corpus and small enough to be CPU-bound, was much slower:
1 clients 11 QPS, p95 149 ms; 8 clients 11 QPS, p95 1156.4 ms; 32 clients 12 QPS, p95 4376 ms, errors 0.
It misses the proposed DB p95 of 300 ms from 8 clients. Local misses it at 32. Neither
machine is the target, so neither passes or fails G7; the target-service run decides.

### Search right after a bulk load

Local first search after loading: 8.9 s at 2.4M events and 13.4 s at 4.04M, over the 5 s
query limit, with 99 and 119 active parts. One local recall query timed out at 2.4M.
After merges settled, the same query took 341 ms.

**CI does not reproduce this:** at 4.04M events with 49 active parts it took 435 ms. The
local data directory is on an external drive, which is the likely difference, but this
was not isolated. Treat a post-backfill timeout as a risk to check on the target
service, not a measured property of the store.

## Acceptance contract

helm-chart-chief's contract of 2026-10-31. Nothing here is met for T0.

| Requirement | State |
| --- | --- |
| 1. Separate e2e for engine and full product | Engine: G0, G2, G5, G7. Product path covers courier to store only; **no product e2e through the MCP server, Cloud API or dashboard** |
| 2. Same Supabase and Neon baseline for every comparison | **Not met.** The legacy pool is a PGlite model of one rule; #59 has the Postgres arms but on a separate red stack |
| 3. Server-side tenant isolation | Store chokepoint, reader user, and real `requireAuth` to scope in G13. Supabase hydrate and a real read-token class are not exercised |
| 4. Every Cloud, desktop and MCP filter plus compound queries | Partly. G12 covers nine filters, pairs and all together at store level. Cloud and desktop surfaces are not exercised, and filter-only listing stays in Supabase |
| 5. Plain text first, embeddings optional | Met by design; embeddings are not built |
| 6. Synthetic corpus measured to match production | PARTIAL. Two event totals only |
| 7. Heavy tenant and quota tests | **Not met.** G9 inherited FAIL; quotas in #233 are not implemented |

## Proposed fixes (proposals only)

**p95 miss: diagnosed, not fixed.** On a 2-core runner, 4.04M events, every class
holds about 9 to 14 QPS from 1 to 32 clients (common terms a little more), so latency is
queueing: about 5 QPS per core, and about 190 ms of CPU per query. The cause is the
substring recheck, which reads `text` in every surviving granule. Hits are scattered, so
the index keeps most granules of the tenant (152 of 182 for a term with 307 hits).

| One client, reader user, 307-hit term | p50 ms | MB read | CPU ms |
| --- | --- | --- | --- |
| Current | 110 | 179 | 203 |
| No snippet in the query | 99 | 177 | 177 |
| Index only, no recheck (not usable: false matches) | 39 | 36 | 63 |
| Top-k filtering off | 149 | 303 | 277 |
| Explicit PREWHERE | 155 | 317 | 292 |

The row policy costs about 10%, so it is not the cause. `read_rows` stays above 1.2M in
every variant, including index only, so it does not measure text read; use bytes and CPU.

Candidate-first search was tried and rejected at `b734698`, run 41184915870: the index
picks 200 candidates and only those are rechecked. It passed the recall gates, but it
helped only common terms (14 to 23 QPS) and hurt rare ones (307 hits, 10.7 to 7.5 QPS),
because rare candidates span all history and the second phase still reads `text` across
that range. Each search also became two queries. The mixed load did not improve, so the
simpler first-phase recheck stays, which also keeps exact counts as #233 specifies.

Exact-dispatch timeouts at `675c209` (run 41186253718): 135 code 159 plus 3 HTTP 408
partial-body responses, all rejected before row parsing. A non-2xx or interrupted response
is refused as a whole; the product e2e must keep that assertion.

**Windows and granularity, within one run** (41188007941, `07e8e24`): 4.04M events, 2
cores, 6 parts per table after `OPTIMIZE`, no ingest, one class at a time, first page
only. Recall 1.0 and no errors in every cell.

| p95 ms at 32 clients | Baseline | Windowed | Granularity 256 | Both |
| --- | --- | --- | --- | --- |
| common | 2,589 | 304 | 1,999 | 234 |
| rare, 307 hits | 2,526 | 687 | 1,236 | 384 |
| phrase, 1,092 hits | 2,663 | 747 | 1,818 | 424 |
| 2-character term | 2,668 | 1,423 | 3,523 | 1,306 |
| ultra-rare | 399 | 717 | 315 | 710 |
| zero hits | 144 | 479 | 254 | 589 |

Terms with few or no hits scan all four windows, so they lose. Corpus ages are uniform, so
recency gains here are not production gains. Size was unchanged at granularity 256. Sort
by session gave no gain and was rejected. The store uses both, with one 5 s budget across
all windows and an injected clock so the fixed corpus keeps its ages.

Same-run mixed load, 2 cores, two passes (41189553499, 41190541876), p95 at 8 and 32
clients: single query on granularity 8192, 1,222 and 4,656 ms, with 9 to 13 HTTP 408 per
cell at 32; windowed on 8192, 620 and 2,561 ms; windowed on 256, 428 and 1,695 ms, 0
errors, 35 to 39 QPS, about 19 per core, about 42 ms CPU per search.

- Ingest does not explain the mix being slower than the sweep: QPS barely moves at one
  small insert about every 300 ms. Heavier ingest is untested.
- The query condition cache does not explain it either: turning it off changed nothing.
  One repeated query runs at about 2x the mix (20 ms CPU against 42 ms), so the gap is
  workload composition. Single-class sweep QPS is not a capacity figure; use the mix.
- The G8 rise in proof runs is probably the proof's polling, since a page that never
  fills scans all four windows. The same-run A/B, polling one window, shows no change.
- Two probes for terms with few or no hits were rejected: an upfront count and a count
  after a short first window. Each helped zero hits and made the rest worse (common 718
  to 1,870 ms at 32 clients). So windows cost those terms more than one query does: a
  known, accepted trade-off that needs real query-mix data to judge.

**First search after load.** Not reproduced in CI (435 ms). Run the load on CI with the
same probe at several points while parts merge, and log parts and merges with it. If it
recurs on the target, throttle backfills to bound active parts and hold reads on a
backfilling tenant to the legacy pool until merges settle.

**Ingest orgs.** The earlier refusal was a harness error, not a product limit:
`TENANT_ORG_IDS` applies with `PIPELINE_ORG_FROM_PREFIX=true`, and the proof had a static
`PIPELINE_ORG_ID`, so ownership correctly refused the second org. Repeat G5 through the
real ingest next.

## Not proven

- Anything on the target service: latency, `SharedMergeTree`, physical deletion.
- Real transcript text. Generated words do not stress the tokenizer as real text would.
- MCP server and Cloud integration, Supabase hydrate, a real read-token class.
- Dashboard keyword ranking and nDCG@10 against legacy hybrid. Synthetic data has no
  relevance judgments, so nDCG needs Stage 2 or a labelled set. Hybrid is an appendix.
- The 30-day keyword cap and four pools of today's dashboard are not reproduced.

## OpenSearch and Elasticsearch backend proof

`.github/workflows/opensearch-proof.yml` (on pull requests and `workflow_dispatch`)
proves the `opensearch` search backend on real servers, one matrix entry per dialect.
Each entry starts one disposable single-node container bound to `127.0.0.1:9200`, with
security off and no credential anywhere, and uses no hosted service:

| Dialect | Image | Container settings |
| --- | --- | --- |
| `opensearch` | `opensearchproject/opensearch:2.17.1` | `discovery.type=single-node`, `DISABLE_SECURITY_PLUGIN=true`, `DISABLE_INSTALL_DEMO_CONFIG=true`, `cluster.routing.allocation.disk.threshold_enabled=false`, `OPENSEARCH_JAVA_OPTS=-Xms1g -Xmx1g` |
| `elasticsearch` | `docker.elastic.co/elasticsearch/elasticsearch:8.15.3` | `discovery.type=single-node`, `xpack.security.enabled=false`, `cluster.routing.allocation.disk.threshold_enabled=false`, `ES_JAVA_OPTS=-Xms1g -Xmx1g` |

It then runs:

1. The shared `SearchIndexAdapter` conformance suite plus OpenSearch-specific parity
   cases: `test/search-index-opensearch-real.test.ts`. The cases are rewrite after
   delete; a 500-character `q` containing one `İ` across window boundaries;
   `'a'.repeat(500)`, `'İ'.repeat(500)` (folds to 1,000 code points) and
   `'ab'.repeat(250)` against `matchesQuery`; and a scan past `max_result_window`.
2. `scripts/opensearch-proof/e2e.ts`, in one process: records carrying a content
   marker and the placeholder secrets from `src/concourse/seed.ts` pass through the
   pipeline scrubber into a zstd chunk and manifest in a temp-directory store;
   the same three repetitive or maximal `q` are checked against `matchesQuery` in a
   separate tenant that is then deleted; `indexSearchScope` indexes the scope through
   the opensearch-capable registry with `SEARCH_INDEX_BACKEND=opensearch`; the marker query returns the event; no
   placeholder secret is found by query or in any stored `_source`; with the live
   pointers gone, `indexSearchScope` erases the scope, the query is empty and
   `_count` for the tenant is 0. It writes a JSON result (head, dialect, server
   version, each case). The job requires exactly 19 passing cases and that the detected
   distribution matches the dialect. Detection uses `version.distribution`, or the
   `X-Elastic-Product` header, and is `unknown` otherwise.

The basic-auth and failure paths are covered by the fake-`fetch` unit tests
(`test/search-index-opensearch.test.ts`). To run the proof locally against any
disposable server:

```sh
docker run -d --rm --name search -p 127.0.0.1:9200:9200 \
  -e discovery.type=single-node -e DISABLE_SECURITY_PLUGIN=true \
  -e DISABLE_INSTALL_DEMO_CONFIG=true opensearchproject/opensearch:2.17.1
# or: -e discovery.type=single-node -e xpack.security.enabled=false \
#     docker.elastic.co/elasticsearch/elasticsearch:8.15.3   (and DIALECT=elasticsearch)
export SEARCH_INDEX_OPENSEARCH_TEST_URL=http://127.0.0.1:9200
export SEARCH_INDEX_OPENSEARCH_TEST_DIALECT=opensearch   # or elasticsearch
bun run --filter @96bulls/pipeline test test/search-index-opensearch-real.test.ts
bun scripts/opensearch-proof/e2e.ts
```

Not proven here: authenticated or TLS servers, managed services (Amazon OpenSearch
Service, Elastic Cloud), multi-node clusters, SigV4 (not implemented), and scale.

## Vendor capability matrix

Read 2026-10-31. ObsessionDB live items are from obsession-agent, read-only on its dev
service; they were not repeated here.

| | ClickHouse Cloud | ObsessionDB |
| --- | --- | --- |
| Text index | GA from 26.2 in Cloud per #233; not run by us | Unproven. Live 26.8.2.1 with `enable_full_text_index=1` and an `ngrambf_v1` definition accepted. `text` index not materialized, and `ngrambf_v1` is a different index. Absent from its feature status page |
| Vector | Not reviewed | Distance functions live, settings on. No ANN index built |
| SOC 2 | Not confirmed on the pricing page; check before transcript use | **None.** "We do not hold a SOC 2 or ISO 27001 certification yet" |
| Encryption at rest | Yes per #233; TDE and CMEK are Enterprise | **Incomplete.** SSE still rolling out |
| Isolation | Row policies; a shared table here | Per-tenant namespace, compute and bucket claimed. No scoped principal or row policy proved live |
| Erasure | Purge unmeasured on Cloud | 24 h time travel keeps replaced parts by default; offboarding target 7 days; metadata backups 14 days. No live test |
| us-east-1 | Published rates; tier availability not stated clearly | Claim only. Existing service is EU-central; no US resource |
| Audit logging | Not reviewed | Individual attribution "being introduced" |

## Trial plan: cost and teardown

Blocked until the approval and the cap below are settled. The pricing page offers $300
of credits for 30 days with no card, then pay-as-you-go. It shows **no hard dollar cap**:
only a compute autoscaling ceiling and alerts at 50, 75, 90 and 100% of a threshold. So a
$50 hard cap is not a console setting. The closest real control is a trial with **no card
on file**, so exposure stays in credits, with an alert at $50 and the run stopped there.

1. One service, AWS us-east-1, smallest size, autoscaling ceiling set, idling on.
2. Alerts confirmed before loading data. Synthetic data only.
3. `guard.ts` gets a single-host allowlist for the trial, by explicit environment value.
4. Run G0 to G5, G7, G8, G10 and record spend after each.
5. Teardown: delete the service, confirm it is gone by API and console, delete the
   account, rotate nothing shared because no shared secret is used, record final spend.

## Appendix: hybrid, proposed only

Not a gate. If relevance loss is measured later, embeddings would be a ClickHouse
add-on: deterministic reciprocal rank fusion of keyword and semantic pools
(`SESSION_SEARCH_RRF_K`), versioned model and index, `degradedPools` per pool.
