# Session search benchmark packet

Compares the current Neon search with the V1 `session_directory` search, using
synthetic data on a local engine. Results are relative. They are not Neon or Supabase
numbers and not an end-to-end proof. Nothing here connects to a hosted service:
`guard.ts` refuses any non-local target.

Candidate contract: `docs/session-search-index-contract.md`, this branch's base (#48).
Baseline source: relayhistory-cloud `7343404`.

```sh
bun test ./benchmarks                        # harness checks, PGlite
BENCH_SCALE=S bun benchmarks/session-search/run.ts
BENCH_PG_URL=postgres://postgres:pw@127.0.0.1:5432/postgres BENCH_SCALE=M bun benchmarks/session-search/run.ts
```

The `search-benchmark` workflow runs scales S and M on Postgres 17 and uploads each report.

## Corpus

`corpus.ts` is seeded and deterministic; no value comes from production. Scales:

| Scale | Sessions | Orgs | Workspaces per org | Mean events per session |
| --- | --- | --- | --- | --- |
| test | 2,000 | 3 | 2 | 4 |
| S | 20,000 | 20 | 3 | 12 |
| M | 200,000 | 20 | 3 | 12 |
| L | 1,000,000 | 20 | 3 | 12 |

All scales: one whale workspace with 50% of sessions, 5% multi-machine, 1% tombstoned,
80% with a summary, 180-day window. These are assumptions. Override any of them with
`BENCH_SHAPE_FILE` (aggregate counts only, no raw data); until then the corpus is not
representative of production.

Multi-machine sessions are one directory row with two `machine_ids`, as the projector
writes them. The projector itself is not run, and its precedence rules are provisional.

## Lanes

Lexical only. Embeddings are not in V1, and the baseline's semantic pools and RRF are
left out, so nothing here says how the production hybrid ranking behaves.

| Lane | Question | Neon baseline | 2a: directory, no summary | 2b: directory + short_summary (#48) | ClickHouse/ObsessionDB |
| --- | --- | --- | --- | --- | --- |
| A | Text in title, summary, metadata | Brief keyword pool | Measured | Measured | Not applicable to the gates |
| B | Text in transcript content | `/v1/events?q=` | UNSUPPORTED | UNSUPPORTED | Vanilla ClickHouse gates only |
| C | Filter-only listing | NOT MEASURED | Same indexes as 2b | Measured under RLS | NOT MEASURED |

UNSUPPORTED is a contract boundary, not a miss. `clickhouse-proposed.sql` is PROPOSED:
no schema, version or deployment exists.

`clickhouse-gates.ts` covers gates 1 to 7 of relayhistory-cloud #233 on vanilla ClickHouse
26.8 with the corpus's events. It is not comparable with the Postgres arms: it answers
store-design questions, and says nothing about ObsessionDB.

`clickhouse-replicated-purge.ts` times gate 3 on two local replicas at default cleanup.
It needs both data directories on one host, so it runs by hand, not in CI.

Lane A rows do not compare directly on recall. Neon briefs exist only for sessions with
a summary, so Neon's Lane A recall mostly restates `summaryReadyShare`, an assumption.

## Filters by surface

| Surface | Path | Text | Filters | Page |
| --- | --- | --- | --- | --- |
| MCP `search_shared_sessions` | `GET /v1/events?q=` | 1 to 500 chars; substring on `left(content, 4096)` or `task_title` | workspace, source, project, since, until, cursor, max_content | 1 to 20, default 10 |
| Dashboard | `GET /v1/sessions?q=&search=hybrid` | 2+ chars; 4 pools, keyword pool limited to 30 days and 50,000 events | workspace, cursor, plus the shared filters on the route | up to 25 |
| History API | `readFilters` in `routes/recall.ts` | `q` | since, until, agent, project, noProject, projectContains, source, kind, taskRef, noTaskRef, tag | by route |
| Desktop | Local SQLite FTS5 trigram | 3+ chars for indexed candidates, plus a query syntax | Local only; not a cloud query | up to `PAGE_ROWS` |
| V1 directory RPC | `search_session_directory` | 3 to 200 chars, substring | workspace only | 50, no cursor |
| V1 directory lists | Table under RLS | None | harness, project, no project, agent, created, state, user, machine | keyset |

Gaps in V1 against today's surfaces: the RPC takes no source, project or time filter
and no cursor, text plus filter is not expressible, 1- and 2-character queries are
refused, and transcript text is unsupported.

## Checks

These fail the run. The oracle reads the generator, never an engine.

- Lane A results equal the oracle's first page, in order, for the whale and a small tenant.
- No row from another tenant: per-tenant canaries, a term shared by all tenants, and a
  workspace outside the JWT claims, which must be refused.
- Tombstoned sessions never appear.
- Transcript-only terms and a planted secret reach no directory column. This covers the
  loader and RPC only: `short_summary` here is synthetic, so it does not test the scrubber.
- Queries under 3 characters are refused.
- Every Lane C shape returns rows, for both tenants and both identities. Filter values
  come from the tenant's own rows; a query that returns nothing is not a measurement.
- The agent gate passes as `authenticated` and as owner.

Lane C runs the contract's `SESSION_DIRECTORY_QUERIES` texts with only the projection
changed, because `authenticated` cannot read `search_text`. The contract has no text for
org-recent or session-ID lookup, so those are written from `REQUIRED_SEARCH_INDEXES`.
Reports record the head SHA and corpus hash.

Mutation controls run by hand: removing the RPC's tenant predicate, copying event
content into the summary, and leaving tombstones live each turned the tests red.

## Metrics

| Metric | How | Limit |
| --- | --- | --- |
| Cold p50/p95/p99 | First run on a new connection | Connection-cold only; server buffers stay warm |
| Warm p50/p95/p99 | `BENCH_REPEATS` runs on one connection | |
| Throughput | 8 connections for 10 s, mixed Lane A, whale | Postgres only |
| Query plans | `EXPLAIN (ANALYZE, BUFFERS)` | The RPC's SQL run as owner; the Worker cannot read the table. Lane C runs as `authenticated` under RLS and as owner, since a definer path sees what owner sees |
| Agent gate | Name in an `Index Cond` for exact and selective prefix; broad prefix within 100x | Empty plans are NOT MEASURED and fail the run |
| Freshness | `writeShortSummary` call to first RPC hit | Database only, excludes the pipeline |
| Rebuild | Reload and `REINDEX` of the tenant index | Stands in for replay; manifests are not replayed |
| Resources | Table and index bytes | No dollar figure |

Not measured, also listed in every report: cache-cold latency, pipeline landed-to-searchable
freshness, failure recovery and manifest replay, Neon filter listing, ObsessionDB, and dollar cost. The first three need a proof stack, which needs Khaliq's direct OK.

## Stand-in limits

`neon-baseline.sql` copies the columns and indexes the search reads, not the full schema;
the embedding columns and HNSW indexes are absent, and the write load Neon carries is not
modelled. The baseline SQL in `arms.ts` is a hand port of `session-search.ts` and `recall.ts`
and can drift from them. PGlite has one connection and no shared buffers worth tuning; treat
its numbers as a smoke test.
