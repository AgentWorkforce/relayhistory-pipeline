# RelayHistory to the 96Bulls pipeline on Supabase + S3: production migration plan

**Status: DRAFT for Khaliq's review.** Written 2026-10-30 (America/Los_Angeles).
It is a plan, not an approval. Nothing in it has been run against production,
and nothing in it may be run until Khaliq approves that exact step in his own
words (section 15). No cloud, database, DNS, IAM or secret change was made
while writing it.

**Scope.** This plan covers moving hosted RelayHistory session **ingest,
storage, indexing, V1 search (short_summary and metadata) and retrieval** from
today's stack (a Cloudflare Worker on Neon) to the 96Bulls pipeline:

- production database: **Supabase**;
- production object store: **AWS S3**.

**Full-transcript search is now a hard pre-T0 gate.** Khaliq decided on
2026-10-31 ("Transcript search before T0", decision K-27): **there is no T0
write switch until new sessions are transcript-searchable on ObsessionDB.**

- K-28 makes ObsessionDB the **intended** store. It is **conditional** on the ObsessionDB evidence gate (G-T0E, section 6).
- K-39 takes it **off the hot path**. Hosted search merges a Postgres
  adapter, holding a bounded recent transcript window in Supabase, with a
  store-agnostic history adapter for full history. The candidates are
  ObsessionDB (intended, conditional), ClickHouse Cloud and Amazon
  OpenSearch Service (K-40). When the
  latter fails, search returns the Postgres results as a **silent partial**:
  no user-facing label, with internal coverage metadata (K-46).
- K-33 placed it in AWS `us-east-1`.
- K-34, narrowed by K-36 ("No embeddings pre-T0"), adds the dashboard's ranked search to the same gate, as keyword ranking over ObsessionDB full-text. There are no vector pools before T0.

- Building that store is its own lane (relayhistory-cloud #233, a draft).
- This plan defines the gate it must pass before T0 (section 6, G-T).
- Pre-T0 history keeps legacy transcript search, served from the frozen
  Neon T0 child branch (K-6).

Khaliq's earlier deferral ("clickhouse endpoint -- we don't need to prove
now") covered the AWS self-host proof only. It does not cover this
cutover.

**What it supersedes and what it builds on.**

| Document | Status after this plan |
| --- | --- |
| `docs/relayhistory-cutover.md` (uncommitted, linuyc only) | **Superseded.** It targets Aurora on EKS, with Neon logical replication and a 485.6 GiB content copy. None of that is the production path (decision 0001). |
| `docs/relayhistory-migration-status.md` | The Aurora rows are marked superseded. The board now points here. |
| `docs/decisions/0001-production-data-plane.md` | In force. Production is Supabase plus S3, and AWS is S3 only. |
| `docs/decisions/0002-relayhistory-onto-the-pipeline.md` | In force. Content goes to S3, Supabase holds the index, sequencing is switch-first. |
| `docs/relayhistory-switch-first-dag.md` | In force. It defines the T0 protocol and invariants I1 to I6. This plan adds per-tenant phases, benchmark and drill gates, and the approval list. |
| `docs/relayhistory-t0-runbook.md`, `docs/relayhistory-t0-write-hold.md` | In force for the gate, hold, fence and flip mechanics. Steps 13, 13b and part of 14 (copying content tables, reprojecting, rebuilding) are replaced by the S3 export (decision 0002). |
| `docs/relayhistory-pipeline-migration.md` | In force. Lanes P1 to P9 are referenced by number below. |
| `docs/relayhistory-supabase-migration.md` | Its content-copy parts were superseded by 0002. Its correctness rules and driver contract still apply. |

## Decision log

| Date | Id | Decision (chosen option, verbatim) | Source | Effect on this plan |
| --- | --- | --- | --- | --- |
| 2026-10-31 | K-6 | "Serve old sessions from T0 branch" | Khaliq, lead session, relayed by the coordinator | Section 6. Pre-T0 sessions keep legacy transcript search, served from the frozen Neon T0 child branch. **Amended by K-27:** post-T0 sessions are not left on V1 metadata-only search, because T0 waits until they are transcript-searchable on the new store. |
| 2026-10-31 | K-27 | "Transcript search before T0" | Khaliq, lead session, relayed by the coordinator | **No T0 write switch until new sessions are transcript-searchable on ClickHouse or ObsessionDB.** K-6 still applies to pre-T0 history but is no longer sufficient on its own. This adds the hard pre-T0 gate G-T (section 6), puts it on the critical path, and makes section 10 the cutover benchmark gate. |
| 2026-10-31 | K-28 (restated) | "Yes, add ObsessionDB", restated by Khaliq as: ObsessionDB is the **intended** production transcript store, **confirmed only after capability and security evidence** | Khaliq, direct approval (partly in helm-chart-chief's session), confirmed to the coordinator 2026-10-31 | **Conditional.** The evidence gate G-T0E (section 6) must pass before ObsessionDB is confirmed or production is provisioned (K-29). The draft 0001 amendment says "conditional". |
| 2026-10-31 | K-33 | Region: AWS `us-east-1` | Khaliq, lead session, relayed by the coordinator | The production transcript-search service runs in AWS `us-east-1`, next to Neon prod and Supabase `sessions`. The proof service is `relay-transcript-proof` in `us-west-2`; it is evidence only and holds no production data. |
| 2026-10-31 | K-34 | "Include it in the T0 gate" | Khaliq, lead session, relayed by the coordinator | **Superseded as written by K-36.** The dashboard's semantic/hybrid search (keyword + vector, ranked) must also be served on the new stack for post-T0 sessions before T0. This adds G-T8 and a hybrid benchmark lane (nDCG@10 against legacy on the hybrid query set). |
| 2026-10-31 | K-36 | "No embeddings pre-T0" | Khaliq, lead session (final ruling). **helm-chart-chief now records it as directly approved.** | **Supersedes K-34 as written.** G-T8 becomes keyword-ranked dashboard search on ObsessionDB full-text over complete transcripts, with **no vector pools before T0**. The nDCG@10 benchmark against legacy hybrid stays as the gate. If it shows a real loss, embeddings come back as a follow-up decision (section 17). Embedding versioning, K-35, Q19 and Q20 move to section 17. |
| 2026-10-31 | K-37 | Synthetic trials approved: **ClickHouse Cloud with a $50 hard cap**, and the **ObsessionDB free dev service (eu-central, functional testing only)** | Khaliq, direct approval, confirmed 2026-10-31 | Synthetic data only. **A trial approval is not production provisioning approval** (K-29 stays separate). Trial results feed G-T0E. ObsessionDB's dev service is outside `us-east-1`, so it cannot prove performance or region. |
| 2026-10-31 | K-38 | Counts-only, read-only shape extraction from production is approved; **no corpus movement** | Khaliq, direct approval, confirmed 2026-10-31 | Allows aggregate counts and sizes (per-org session/event counts, size distributions, table sizes) to answer Q7, Q9 and Q11 and to shape the synthetic corpus. No content, query text or identifiers leave production. **Done 2026-10-31 07:13-07:25 UTC**; results in `~/selfhost-proof/notes/prod-shape-2026-10-31.md` (section 1). |
| 2026-10-31 | K-39 | "ObsessionDB off the hot path" | Khaliq, direct, lead session | Restructures search (section 6, "Search architecture").<br>- S3 is the source of truth, and ObsessionDB is a derived, rebuildable index fed by an async indexer. Ingest never waits on it.<br>- One search interface with two adapters. The Postgres adapter is the self-host default; on hosted it serves a **bounded recent window** of transcript text in Supabase (trigram/FTS) plus metadata and summary search. The ObsessionDB adapter (any ClickHouse-protocol store) serves full history.<br>- Hosted search queries both and merges. ObsessionDB gets a roughly 1-2 s budget. On its error or timeout, the response returns the Postgres results labelled "partial: older history unavailable": never an unlabelled empty result, and never a 5xx for that case. **The user-facing label is superseded by K-46 (silent partial).**<br>- Recent-window freshness no longer depends on the indexer.<br>- This lowers the stakes of the conditional K-28.<br>- A draft amendment to 0002 is in this PR, and the Supabase disk sizing for the window is folded into K-32. |
| 2026-10-31 | K-40 | **Amazon OpenSearch Service is a third candidate** for the full-history transcript index, alongside ObsessionDB (conditional, K-28) and ClickHouse Cloud (trial, K-37). A **synthetic-only OpenSearch trial is approved in the proof account 500268966557 / `us-west-2`, about $50 cap.** | Khaliq, direct, 2026-10-31 | The store is decided by the same benchmark and evidence gate for every candidate: G-T0E is generalised (section 6). OpenSearch is expected to score E4, E5 and E6 through AWS VPC, IAM and compliance, and to be weaker on the cost of substring indexing (E1). The history adapter interface is **store-agnostic**, not just ClickHouse-protocol. A trial is not production provisioning. |
| 2026-10-31 | K-42 | **Window = 14 days** *(superseded by K-47: 48 h)*; **complete event text** in the window, not a 4 KB prefix (closes Q23); **history-store budget = 1.5 s** (closes Q24) | Khaliq, direct, 2026-10-31 | Recent window = the last 14 days of complete scrubbed event text. The final byte size is still to be measured by the K-38 counts-only extraction before the disk resize (K-32). The merge/fallback (G-T1M) uses a 1.5 s hard budget for the history store. |
| 2026-10-31 | K-44 | Transcript-store isolation: **application-layer scoping is primary, with a DB row-policy backstop.** Ask the vendor to enable row policies and user creation. **The gate requires both.** | Khaliq, direct, 2026-10-31 | G-T6, G-T0E E2 and S11 updated. |
| 2026-10-31 | K-45 | Erasure SLA: **logical delete immediately; physical purge within 48 h**, including ObsessionDB's 24 h time travel | Khaliq, direct, 2026-10-31 | E3, G-T6, the 0001 and 0002 amendments and the S3 lifecycle updated. Backup and PITR tails are open (Q27). |
| 2026-10-31 | K-46 | Degraded search returns **silent partial results**: no user-facing "older history unavailable" label in the UI or in MCP text. An **internal coverage field** is kept in response metadata, logs and metrics, with an alarm on the partial-response rate. | Khaliq, direct, 2026-10-31 | Supersedes K-39's labelling as written. The degraded-behaviour table, G-T1M, the merge spec, SLO row 14 and the alarms are updated. |
| 2026-10-31 | K-47 | **The full-history store is PRIMARY for all transcript search.** Postgres keeps a **48-hour complete-text window** as a safety net, for indexer lag and short history-store outages, plus metadata and summary search. | Khaliq, direct, 2026-10-31 | **Supersedes K-42's 14-day window** (complete text and the 1.5 s budget still stand) **and the "Postgres primary" framing in K-39.** Reason: Postgres is weak here. Legacy Neon search is slow and returns 503s, and the prototype's selective queries on the window ran at 3.1 s p95, while the history store was about 10-30× faster in the prototype (relayhistory-cloud #236). Re-derives K-32 to 32-64 GB. |
| 2026-10-31 | K-22 | "Forward-only after flip" | Khaliq, lead session, relayed by the coordinator | Option R2 (section 11). T0 can be aborted up to the flip. After the flip there is no data rollback to Neon, and no reverse-sync (G-V) is built. |

**Relay status.** helm-chart-chief records relayed rulings as **PROPOSED**
until Khaliq confirms them in its own session.
- **Confirmed directly (2026-10-31):** K-28 (restated as conditional), K-36,
  K-37, K-38, K-39, K-40, K-42, K-44, K-45, K-46 and K-47.
- Recorded together under K-44 to K-46: isolation (app-layer primary with a
  vendor row-policy backstop), erasure (logical delete immediately, physical
  purge within 48 h including the 24 h time travel), and silent partial
  results.
- The other rows were relayed by the coordinator from Khaliq's lead
  session. They stay proposed in that sense until helm-chart-chief records
  them as confirmed.

These records capture decisions. They do not approve any action:
every step in section 15 still needs its own OK.

---

## 0. Recommended path in one page

1. **Method: switch-first plus export (option D, section 5).** New writes move
   to the pipeline at one atomic instant, T0. History is exported afterwards
   from a Neon child branch created at T0 into S3, in the pipeline layout.
   Supabase holds only the index, directory and summaries, plus a small set
   of legacy state tables. There is no Neon logical replication, and no
   content table is copied into Supabase.
2. **Transcript search comes first (K-27), structured by K-39 and K-47.**
   There is one search interface with two adapters:
   - **History store (PRIMARY, K-47), store-agnostic:** all transcript
     search over full history. The candidates are ObsessionDB (intended,
     K-28 conditional), ClickHouse Cloud and Amazon OpenSearch Service
     (K-40), chosen by one benchmark and evidence gate (G-T0E). An async
     indexer fills it from S3, and ingest never waits on it.
   - **Postgres (Supabase), safety net:** a **48-hour complete-text window**
     that covers indexer lag and short history-store outages, plus metadata
     and summary search. It is fed in the pipeline.

   Search queries the history store first, with a **1.5 s** budget. On its
   error or timeout, the response falls back to the 48 h window plus
   metadata/summary search as a **silent partial**: no user-facing label,
   but internal coverage metadata, logs, metrics and an alarm (K-46).
   Pre-T0 history is served from the frozen Neon T0 child branch (K-6)
   until it is loaded. The dashboard's ranked search is keyword-only before
   T0 (K-36). Gate G-T, section 6.
3. **Per-tenant phases happen around a single write flip** (section 7):
   - **Before T0, per tenant:** export a rehearsal snapshot of the internal
     orgs to a staging Supabase project and staging bucket, and check parity
     and benchmarks there.
   - **At T0:** one global write flip, with an abort boundary that keeps it
     reversible up to the flip.
   - **After T0, per tenant:** export waves, then removing the Neon read
     fallback and enabling V1 search, one wave at a time. Each wave has its
     own rollback.
4. **Critical-path gates**, in order:
   0. **G-T: transcript search for new sessions is live**, on the K-39
      shape. That covers:
      - the history store as the primary (K-47);
      - the Postgres 48 h safety-net window;
      - the async indexer into the chosen history store (store-agnostic adapter; G-T0E picks among ObsessionDB, ClickHouse Cloud and OpenSearch);
      - the merge with silent-partial fallback (K-46);
      - MCP excerpts and the in-session window;
      - recall parity;
      - keyword-ranked dashboard search (K-36);
      - the cross-T0 spec.

      This is the longest pole. The 0001 and 0002 amendments are drafted
      here and not merged.
   1. SEC-01 is fixed in production: cloud #5886 has been promoted and
      deployed, and the deployed SHA is verified.
   2. The production S3 bucket, per-workspace KMS keys (P9) and the Worker's
      S3 identity exist.
   3. #38, #48 and #72 are applied to Supabase, and the `session_directory`
      projector exists.
   4. The hosted search issuer (relayauth #116, plus a key-creation path) is
      live.
   5. G1 to G8 have merged and been deployed dark (relayhistory-cloud #223,
      #224, #225, #226 and #231; cloud #5850).
   6. The rehearsal benchmark is green.
   7. The T0 abort path has been rehearsed. After the flip the system is
      forward-only (K-22, decided), so the abort boundary is the only data
      rollback.
   8. The child-branch transcript search (K-6) passes the
      legacy lane of section 10.
   9. **G-F: a post-flip write-freeze lever** is built and rehearsed
      (section 11). Forward-only (K-22) makes it the main emergency control,
      and #224 has no `supabase → holding` move.

---

## 1. Current production (evidence)

| Fact | Evidence |
| --- | --- |
| The RelayHistory API is a Cloudflare Worker on Neon HTTP: `@neondatabase/serverless` with `drizzle-orm/neon-http`. | relayhistory-cloud `packages/relayhistory/src/db/client.ts:8-9` (origin/main `45d53b1`) |
| Neon project `divine-snow-84812652`, branch `production` (`br-damp-mud-apbr6ifp`), database `neondb`, PostgreSQL 17, `aws-us-east-1`, endpoint `ep-floral-pond-apkvat38`. | `docs/evidence/2026-10-30-neon-capacity/README.md`; the uncommitted cutover doc |
| **Tenants and sessions:** 282 orgs, 289 workspaces (nearly 1:1). 203,671 sessions and 28.07M events. About 2,800 new sessions a day over 30 days (peak 5,951). Events per session: p50 21, p99 1,731, max 326,877. Concentration: the top org has 23.2% of events, and the top 10 orgs hold 74% of all-time content bytes. | K-38, exact counts from `session_rollups` (bytes from the sample) |
| RelayHistory has no database of its own. It writes the `sessions` schema in the same `neondb` as Cloud's `public` schema. | Cloud memory note `relayhistory-revision-publisher-convoy.md`; runbook step 9 ("Cloud's `public` schema is untouched") |
| Neon is under stress: 91.39% cache hit, 1.35 TB temp spill, 56 deadlocks, 8.9M dead tuples in `delivery_projection_acceptance`. Compute is pinned at 16/16 CU, the plan maximum. | `load-results.json`, capacity README (2026-10-30) |
| There was a lock convoy on `session_revision_events` on 2026-10-23. It was fixed by relayhistory-cloud #164 (migration 0049, SKIP LOCKED). Still open: advisory-lock waits on `accept_delivery_batch`, and a backlog of about 1.6M messages for Cloud's `project-summary-worker`. | Cloud memory note (2026-10-24) |
| Neon outbound logical replication is disabled. Enabling it is irreversible and restarts the endpoints. | Uncommitted cutover doc |
| **Production transcripts are Neon rows.** The `sessions` schema is **495 GB** of a **506 GB** database. `convergence_events` 210 GB, `delivery_records` 164 GB, `session_markers` 25 GB. | K-38 extraction, catalog sizes, 2026-10-31 (`~/selfhost-proof/notes/prod-shape-2026-10-31.md`) |
| **No production S3 bucket exists.** The existing R2 bucket `relayhistory-content` is **empty**, so no session content is in object storage today. | Coordinator, 2026-10-31 |
| Pipeline code exists in relayhistory-cloud but is inert: storage adapter #211/#216 (S3 API supported); pointer index #212; summary-short stage (`docs/pipeline-summary-short.md`); single-use migration assertions #228 (merged 2026-10-31 UTC). | PR states, read 2026-10-30 |
| Supabase project `sessions` (`pyamkrhoyszgzirqqffq`): **Pro plan, Large compute, but a 2 GB disk, and empty.** The disk must be sized before A3 and any load (K-32). Roles `relayhistory_owner` and `relayhistory_runtime` (BYPASSRLS) exist, as does the Neon role `relayhistory_readonly`. | `docs/relayhistory-pipeline-migration.md` "Done" (A1, A2) |
| Supabase `sessions` region: **AWS `us-east-1`**, next to Neon prod. | Khaliq, 2026-10-31 (K-33 wording). Not yet confirmed read-only: check with `scripts/sb projects list`. |
| Production S3 bucket account: not recorded. `sessions-prod` (`941433935742`) is the existing workload account named in the cutover doc. | Open question Q2 |

### Data volume (measured, K-38)

Source: `~/selfhost-proof/notes/prod-shape-2026-10-31.md`. It is aggregates only.
- Run 2026-10-31 07:13-07:25 UTC, read-only transactions, low load.
- Catalog sizes and session/org counts are exact.
- Content bytes are a 1% `TABLESAMPLE SYSTEM` of `convergence_events`,
  extrapolated ×100 (see the caveats below).

| Class | Size | Tables | Destination |
| --- | --- | --- | --- |
| Transcript-bearing | **~399 GB (~81%)** | `convergence_events` 210 GB, `delivery_records` 164 GB, `session_markers` 25 GB | content to **S3** (export) |
| Acceptance and identity bookkeeping (no transcript content) | **~90 GB (~18%)** | `uploaded_support_identities` 37 GB, `delivery_projection_acceptance` 28 GB, `delivery_catalog_acceptance` 13 GB, `delivery_receipts` 12 GB | **open (Q25):** move to Supabase, or drop or re-derive |
| True session metadata | **~5-6 GB (~1%)** | `session_catalog`, rollups, revisions, timing cache, outbox, `auth_sessions`, … | Supabase (index or copy set) |
| **`sessions` total** | **495 GB** (heap 160, indexes 194, rest TOAST) | | |
| Whole database | 506 GB | | |

- **Content column:** `convergence_events.content` is 59.1 GB logical
  (~35 GB stored, 0.59 compression) over 28.3M events. The rest of that
  table's TOAST is the `record` jsonb.
- **Not split by column:** the `delivery_records` and `session_markers`
  payloads.
- **Growth:** content per day went from 0.06 GB in August to 0.51 GB in
  September to 1.21 GB in October (2.4× month over month, Sep→Oct). The
  busiest week, 10-19 to 10-25, ran at 1.48 GB/day.
- **Earlier figures are superseded:**
  - 494 GB (coordinator) and 484 GB (2026-10-30);
  - 485.6 GiB (the uncommitted cutover doc). It is the whole-branch logical
    size from 2026-10-29, now stale.

**Sampling caveats** (from the source note):
- SYSTEM sampling is block-clustered:
  - 14- and 30-day byte totals carry roughly ±10-15% error;
  - single-day values carry ±20-30%.
- Per-org bytes are sampled. Orgs with fewer than ~100 events are
  under-represented, which skews the per-org percentiles high.
- The trigram ratio is extrapolated from a 4 KB-prefix index.
- Retention deletes add GIN and heap bloat between vacuums. The 50% headroom
  covers this.
- A 1% sample cannot find the single largest row. The true maximum event
  size is not measured.
- The FTS overhead is assumed, not measured.
- The copy-set size is not measured.
- Stats were last analyzed 2026-10-30 23:22 UTC, with 4.4M dead tuples on
  `convergence_events`.

Under option D none of this volume is copied into Supabase: most is
re-derived or exported to S3 (section 4). The search store (G-T), however,
must hold transcript text for every post-T0 session, and eventually for
exported history. No embeddings before T0 (K-36). Size that
store from the same measurement (Q16).

The **Supabase 48 h safety-net window** (K-47) holds bounded transcript
text in Supabase: about 2.4 GB of text (section 6, window sizing). It is
folded into K-32.

---

## 2. Target architecture

```
 Relay Desktop / CLI / Cloud
        │  (unchanged API hostname and contracts)
        ▼
 RelayHistory Worker (Cloudflare)  ── CutoverGate DO: neon → holding → supabase
   │  writes after T0                         reads after T0
   ├─► /v1/pipeline/chunks (scrub) ─► S3 raw   ├─► Supabase session_index / session_directory (TCP, Supavisor or PostgREST)
   ├─► /v1/pipeline/manifests ──────► S3 manifest ├─► S3 objects by pointer (replay, context, briefs)
   │     │                                     └─► Neon T0 child branch (read-only, bounded, until the session is exported)
   │     ▼ Cloudflare Queues (+ DLQs)
   │   object-landed → session-indexed → Supabase session_index
   │                 → summary-short   → S3 summary/short.json.enc (+ bounded plaintext short_summary on session_directory, #48)
   ▼
 Cloud (Sessions board, shared-session MCP) ── reads through RelayHistory; cloud #5850 gates its three direct `sessions` calls
 RelayAuth (hosted, cloud packages/relayauth, Worker + D1)
   ├─ general issuer (unchanged)
   └─ session-directory search issuer (relayauth #116): 60 s single-workspace token → Supabase PostgREST RPC search_session_directory
```

| Component | Production placement | Status and evidence |
| --- | --- | --- |
| Database | Supabase project `sessions` (`pyamkrhoyszgzirqqffq`), Large compute. **Region unknown.** Recommendation: same region as the S3 bucket and Neon (`us-east-1`) for the export path and Worker latency. If it is elsewhere, decide before A3 whether to recreate it; it holds no production data yet. | 0001; pipeline-migration A1/A2 |
| Holds | `sessions.session_index` (pointers plus metadata); `session_directory` plus memberships (#38); bounded `short_summary` and `search_text` (#48); `session_summaries` pointers; `pipeline_events`, queue and DLQ tables; the legacy state copy set (section 4); the cutover marker. | `supabase/migrations/`, #38, #48 |
| Object store | One production S3 bucket, versioned, with SSE-KMS and a key per workspace (P9). TLS-only bucket policy. Lifecycle that expires noncurrent versions (SEC-08). v1 layout `{prefix}/v1/ws=/m=/h=/d=/s=/{manifest.json, raw/, summary/}`. **Bucket name and account are unknown and not created; the region must be `us-east-1` (section 6, data-flow boundaries).** | `docs/pipeline-storage-adapter.md`; relayhistory-cloud `object-layout.ts` |
| Pipeline compute | **Inferred, needs confirmation (Q1):** the relayhistory-cloud Worker and Cloudflare Queues run the pipeline stages (object-landed, session-indexed, summary-short). 0001 limits production AWS to S3, so the 96Bulls EKS services (AlleyOop, Bob Costas, hoopdb-indexer, Referee) are not in hosted production. They are self-host proof only (`aws-proof`, 500268966557/us-west-2). | 0001; relayhistory-cloud #231 binds the session-indexed queue in the Worker |
| Worker to Supabase | Direct TCP through Supavisor (relayhistory-cloud #225), one client per request. Metadata reads go through PostgREST (#226). Hyperdrive only if the benchmark justifies it. | `docs/relayhistory-supabase-migration.md` |
| Worker to S3 | The S3 API with `PIPELINE_RAW_S3_{ENDPOINT,REGION,BUCKET,ACCESS_KEY_ID,SECRET_ACCESS_KEY,SSE}`. A Worker cannot use IAM roles, so this is a long-lived IAM access key. Security gate S5 covers it. | relayhistory-cloud `lib/pipeline/adapter.ts` |
| Auth | Hosted RelayAuth (cloud `packages/relayauth`, Cloudflare Worker + D1), `@relayauth/server` 0.2.35 on cloud `trunk` (#5886). A **separate** search issuer and key (relayauth #116, draft). Supabase trusts only that issuer for `session_directory_search_worker`. | #63, #72, #73, relayauth #116 |
| Transcript search: one interface, two adapters (K-39, K-47) | **History adapter: PRIMARY (K-47)** for all transcript search. It uses a store-agnostic interface; the candidates are ObsessionDB (intended, K-28, conditional), ClickHouse Cloud and Amazon OpenSearch Service (K-40), chosen by G-T0E, in AWS `us-east-1` (K-33). It holds full history and is fed by an **async indexer** from S3 normalized objects; ingest never waits on it. **Postgres adapter: safety net.** On hosted it uses Supabase `sessions`: a **48 h complete-text window** (trigram/FTS) for indexer lag and short outages, plus `session_directory` metadata and summary search. On self-host it is the default, with an unbounded window. Search asks the history store first, with a 1.5 s budget, and on failure falls back to the window plus metadata/summary as a silent partial with internal coverage metadata (K-46). Design and prototype: relayhistory-cloud #236 (draft). Proof service: `relay-transcript-proof`, `us-west-2`. Open: hosting and account (Q21). | K-39, K-47; 0002 Amendment 1 (draft) |
| Transcript search, pre-T0 sessions | Legacy `ILIKE`/trigram, run on the frozen T0 child branch (K-6) | 0002; #226 currently fails it closed, which G3 must change |
| Neon after T0 | `sessions` is fenced read-only. A child branch at the T0 LSN serves bounded fallback reads and the export. Cloud's `public` schema stays on Neon, so "Neon retirement" (A10) means retiring the `sessions` schema, **not** deleting the project. | switch-first I6 |

---

## 3. Producers and consumers that must move with the data

These are easy to miss because they are not routes:

- **Revision feed to Cloud.** `publishSessionRevisions` reads Neon
  `session_revision_events` and feeds Cloud's `relayhistory-project-events`
  queue, whose consumer is `project-summary-worker`. After T0, Neon takes no
  writes, so this feed stops unless the pipeline emits the equivalent event.
  **Gap: no PR covers it.** This is gate G-R1 (section 7, phase 0).
- **Cron and queue consumers.** The per-minute cron, the session-events
  consumer and DLQ, brief admission, and relaycron `reflex/learn`
  (`docs/relayhistory-t0-write-hold.md` "Route classes"). Each needs a
  post-T0 home or an explicit retirement.
- **Cloud's direct `sessions` calls.** There are three, gated by cloud #5850
  (open, `merge-train:hold`).
- **Analysis and briefs.** These are LLM-produced state. Decide per table
  whether to export, re-derive or drop (section 4). Re-deriving costs model
  calls.

---

## 4. What must move, and what can be re-derived

Sizes are the K-38 catalog figures (section 1, `~/selfhost-proof/notes/prod-shape-2026-10-31.md`). Row counts
are live tuples from `load-results.json`.

| Data | Size, rows | Moves? | Where and how |
| --- | --- | --- | --- |
| `delivery_records` (the client's own records, with revisions: session, session_event, tool_call, file_edit, …) | 164 GB, 54.0M (raw `payload` jsonb holds transcript content) | **Must move.** It is the only source of session content. | Exported from the T0 child branch to S3 as scrubbed raw chunks plus a manifest per session (`docs/relayhistory-export-design.md`, #40, E1 to E4). Not copied into Supabase. |
| `convergence_events` | 210 GB (heap 44, TOAST 87, indexes 79, of which the prefix trigram index is 39 GB), 27.8M | **Re-derived, not moved.** 99.8% of sampled rows reference a `delivery_record_id`. | Rebuilt by the pipeline's normalizer from the S3 raw stream. The derived replay comes from S3 objects. |
| `session_rollups`, `session_catalog`, `session_relationships`, `session_commit_links`, `session_catalog_agents`, `session_timing_cache`, | ~5-6 GB of metadata | **Re-derived.** | `session_index` and `session_directory` are rebuilt from manifests by session-indexed and the directory projector. Timing comes from manifest activity times. |
| `session_markers` | 25 GB, 25.0M (`text` and `payload` columns hold transcript content) | **Content must reach S3.** It counts as transcript-bearing (K-38). | Confirm that the `delivery_records` export carries every marker's content (kind `session_marker`); if not, export the markers too (Q26). |
| Acceptance and identity bookkeeping: `uploaded_support_identities` 37 GB, `delivery_projection_acceptance` 28 GB, `delivery_catalog_acceptance` 13 GB, `delivery_receipts` 12 GB | **~90 GB** | **Open (Q25).** No transcript content. | Move to Supabase, drop, or re-derive. Moving it would dwarf metadata plus the 48 h window on Supabase disk (K-32). `delivery_receipts` is in today's copy set. |
| Copy set: `auth_sessions`, `deleted_upload_sessions`, `session_resets`, `upload_cleanup_origins`, `upload_cleanup_tombstones`, `machines`, `probe_connections`, `project_aliases`, `github_org_links`, `delivery_origins`, `delivery_receipts` | small (measure; runbook step 3) | **Must move before the flip.** These are write-path state: tokens, tombstones, cursors. | `migration:copy --mode=mirror` pre-copied (A4), then caught up inside the hold (runbook steps 4 and 10). |
| Analysis, briefs, memory state | unknown | **Decide (Q6).** | Either export as derived objects, or re-run after the export behind `MODEL_CALLS_ENABLED` and a budget. |
| Summaries | n/a | **Re-derived.** | summary-short runs from S3 after the export. That costs model calls; budget it per wave. |
| Cloud `public` schema | ~1.6 GiB (inferred) | **Does not move.** | Stays on Neon. |

What this means for capacity:
- Supabase holds metadata (~5-6 GB), the 48 h window (section 6), and
  whatever Q25 decides for the ~90 GB of bookkeeping.
- The 2 GB disk must be resized first (K-32: 32-64 GB, excluding Q25). Separately, **the export's
object size and PUT count are unknown** (export-design O5). Estimate them on
the rehearsal snapshot (phase 2) before sizing the bucket's KMS request
budget.

---

## 5. Migration method options (Neon to Supabase + S3)

| Option | How | Data into Supabase | Neon risk | Write downtime | Rollback | Fits 0002? |
| --- | --- | --- | --- | --- | --- | --- |
| **A. Logical replication** | Publication on Neon, subscription on Supabase, `copy_data=true`, WAL stream, flip at zero lag | Whole `sessions` schema, ~484 GiB | **High.** The setting is irreversible and restarts endpoints. WAL retention and replication-slot load fall on a compute already at 16/16 CU. | Seconds, with a durable hold | Needs a reverse stream | **No.** It copies content into Supabase, which 0002 forbids. |
| **B. Dump/restore** | `pg_dump` from a frozen source, `pg_restore` into Supabase | ~484 GiB | Medium. A long read on production compute, unless run on a branch. | **Hours.** Writes are frozen for the whole dump and restore. | Repoint, because Neon is unchanged | **No.** Same content problem, plus the downtime. |
| **C. Dual-write plus backfill** | The Worker writes both Neon and the pipeline, history is backfilled, reads switch per tenant | Index only | Medium. Neon keeps every write and gains a second write path. | None | Easy: stop writing to the pipeline | Partly. It **breaks invariant I2** (no RelayHistory write reaches the pipeline before the flip), and it needs a dual-write reconciler for divergence on partial failure. No PR for it exists. |
| **D. Switch-first plus export (recommended)** | Durable hold, Neon fence, an immutable marker and a child branch at the marker's LSN, then the flip. History is exported from the child to S3 afterwards, with bounded read-only fallback to the child. | Index plus the small copy set | **Lowest.** No logical replication. Production compute takes no RelayHistory reads or writes after T0, and the export uses the child's own compute. | The hold only (target under 5 minutes; replay-safe uploads are buffered, other writes get 503 with `Retry-After`) | Before the flip: abort to `neon`, and the HoldLog replays to Neon. After the flip: forward-only (K-22, section 11). | **Yes.** It is the design Khaliq approved on 2026-10-30. |

**Recommendation: D.** It is already the decided design, and three things
make it the right one under the Supabase decision:

- it keeps content out of Supabase;
- it never puts load on the stressed Neon production compute;
- it bounds the irreversible moment to one flip, with an abort boundary
  before it.

The only "dump-like" step left is the small copy set, mirrored inside the
hold.

**What D costs.** Writes are not per-tenant: the flip is global, because
#224's gate holds one phase. Section 7 lays out the per-tenant alternative
(B-waves) and why it is not recommended.

---

## 6. Transcript search across T0 (decided: K-27 plus K-6)

**Decisions.**
- **K-27** (2026-10-31, "Transcript search before T0"): no T0 write switch
  until new sessions are transcript-searchable on ClickHouse or ObsessionDB.
  K-28 then made ObsessionDB the intended store, conditional on G-T0E. K-40 added Amazon OpenSearch Service as a candidate on the same gate.
- **K-6** (2026-10-31, "Serve old sessions from T0 branch"): pre-T0 history
  is served by legacy transcript search on the frozen Neon T0 child branch.

- **K-39** (2026-10-31, "ObsessionDB off the hot path"): one interface,
  two adapters, and partial fallback (below). K-46 made the partial silent
  to users.

So after T0:
- **All transcript search:** the chosen history store (PRIMARY, K-47).
- **The last 48 h:** also in the Supabase safety-net window, which covers
  indexer lag and short history-store outages.
- **Pre-T0 sessions:** searched on the child branch until loaded.
- **Metadata and short_summary:** V1, for everything.

### Search architecture (K-39, K-47)

```
 ingest → S3 (source of truth) ─┬─ async indexer ──► history store (full history; store-agnostic)  ← history adapter (PRIMARY)
                                └─ pipeline stage ─► Supabase 48 h window (trigram/FTS)             ← Postgres adapter (safety net)
 query → search interface ─┬─ history adapter first (budget 1.5 s)                        ─┐
                           ├─ Postgres: 48 h window (lag gap) + metadata/summary         ├─► merge (G-T5) ─► response (+ internal coverage metadata)
                           └─ Neon T0 child (pre-T0, until loaded; K-6)                  ┘
   on history error/timeout → 48 h window + metadata/summary only (silent partial, K-46)
```

- **The history store is primary for search (K-47)** but off the ingest
  hot path (K-39). The hot path is ingest → S3 → the 48 h window, and the
  history indexer is asynchronous.
- **The 48 h window is the safety net.** It covers indexer lag: events newer
  than the history store's indexed watermark are found in the window. It
  also covers short history-store outages, alongside metadata and summary
  search. **It is not a performance path.** Postgres is the weaker engine:
  legacy Neon search is slow and returns 503s, and the prototype's
  selective window queries ran at 3.1 s p95.
- **The 48 h window** is fed by a pipeline stage from the same normalized
  objects, idempotent by content hash. It holds **complete scrubbed event
  text** (K-42), for the **last 48 hours** only (K-47). An hourly
  retention job *(proposed; daily is too coarse for a 48 h window)* deletes
  rows older than the window.
  Tombstones and resets delete immediately. RLS applies, there are no
  embeddings, and erasure follows the 0002 Amendment 1 rules.
- **Store-agnostic history adapter (K-39, K-40):** one interface, defined
  by the search contract (section 6, items 1 to 9), not by any wire
  protocol. It covers query by org and workspace scope, keyword and
  substring match, excerpts, a composite cursor, tombstone deletes and bulk
  rebuild.
  - The implementations are a ClickHouse-protocol driver (ObsessionDB,
    ClickHouse Cloud or self-run ClickHouse) and an OpenSearch driver.
  - The candidate that passes G-T0E is what gets provisioned. Losing a
    vendor changes a driver, not the design.
- **Self-host:** the Postgres adapter with an unbounded window is the
  default. The history adapter is optional.

### Safety-net window sizing (input to K-32): 48 h, complete text (K-47), from the K-38 measurements

Source: `~/selfhost-proof/notes/prod-shape-2026-10-31.md` (1% sample ×100;
caveats in section 1). D, the average over 14 and 30 days, is about
1.18 GB/day; the peak week ran at 1.48 GB/day.

| Window | Text | With indexes | Status |
| --- | --- | --- | --- |
| **48 h (decided, K-47)** | **≈ 2.4 GB** (≈ 3.0 GB at the peak rate) | **≈ 3-4 GB** at ~1.5× trigram-only overhead (the relayed figure); **≈ 8 GB (≈ 10 GB at peak)** at the full ~3.3× ratio implied by the 14-day estimate, which adds trigram 1.73×, assumed FTS and btrees | **chosen** |
| 14 days | 16.5 GB | 52-56 GB (≈ 70 GB at peak) | superseded (K-42 → K-47) |
| 7 / 30 days | 6.6 / 35.5 GB | 22-28 / 115-125 GB | reference only |

**Range:** the two 48 h index estimates differ because of the overhead
assumed. **Plan on the upper one (≈ 8-10 GB)** until the phase 2 snapshot
measures the real overhead. The FTS overhead is not measured.

**Event size** (14 days, sampled):

| p50 | p95 | p99 | p99.9 | max seen | mean |
| --- | --- | --- | --- | --- | --- |
| 261 B | 7.7 KB | 27 KB | 160 KB | 1.19 MB | 1,884 B |

The true maximum is not measured.

**Disk (K-32), re-derived for K-47:**
- metadata ~6 GB + the 48 h window ~8-10 GB (upper estimate) + 50%
  headroom ≈ 21-24 GB;
- **propose 32 GB now, and plan for 64 GB**, given 2.4× month-over-month
  growth (Sep→Oct) and the unmeasured copy set;
- the 14-day component (52-70 GB) is dropped;
- **Q25 is still open:** if the ~90 GB of acceptance and identity
  bookkeeping moves to Supabase, the disk is dominated by it and must be
  re-derived first (≈ 128-160 GB).

**Watch item:** complete text puts very large events into trigram GIN in
the window:
p99.9 is 160 KB, the largest seen is 1.19 MB, and the true maximum is
unknown. The phase 2 benchmark (SLO row 15) must cover
the largest-event tail. If index cost is extreme, a per-event cap needs a
new decision; it is not assumed here.

### Why V1 alone is not enough

These tables are copied from `~/selfhost-proof/notes/search-recall-comparison-2026-10-31.md`
(a read-only audit, 2026-10-31). That report's sources: cloud `main`
`865ca4050`, relayhistory-cloud `45d53b1`, 96BullSessions `890e9a0`, #38
`fa3272dc`, #59 `4102156f`.

**Entry points: what is searchable today, and on V1:**

| Entry point | Today (legacy on Neon, via Cloud HTTP) | V1 `search_session_directory` |
| --- | --- | --- |
| MCP `search_shared_sessions` → `GET /v1/events?q=` | `left(content,4096) ILIKE '%q%' OR task_title ILIKE` (trigram GIN). Covers prompts, assistant text and tool output, but only the first 4 KB of each event. Query length 1-500. Newest first, no ranking. Filters: workspace, source, project, since, until, cursor, limit ≤20. Returns event excerpts (`max_content`) with a `citation_url`. | Matches `search_text` only: title, task_title, agent_name, project_id, git_repo, git_branch, model and short_summary, capped at 8 KB. The column "MUST NOT contain transcript". Query length 3-200, ordered by `last_activity_at`. **No event excerpt and no snippet.** |
| MCP `get_shared_session_context` | `/v1/events?session=&q=&limit=1` finds the latest match inside one session, then returns a window of events around it | **No V1 equivalent.** |
| MCP `get_shared_session`, `list_team_activity` | Event pages by exact session id; latest event per agent | Depends on the new transcript read path (unmeasured) |
| MCP `list_session_timings` | `/v1/sessions` plus timing | The directory has `p_user_id`; no timing data |
| Dashboard `sessions?q=&ranking=hybrid` | Hybrid of four pools: event semantic (pgvector HNSW), event keyword, brief semantic and brief keyword. Fusion-ranked, snippet ≤320 chars, `degradedPools`. | Lexical only, no embeddings, no ranking |
| Dashboard `sessions/activity` histogram | `/v1/sessions/activity?q` | Not in V1 |
| Recall/memory pack | recent + lexical (`content`/`task_title`) + `files_touched` pools | Not in V1 |
| Desktop search | Local SQLite FTS5; never searched transcripts | Unaffected |

**Filter gap:**

| Filter | Legacy | V1 RPC | Status |
| --- | --- | --- | --- |
| text in transcript | yes | no | **Gap (by contract)** |
| text in title/metadata | task_title only | title, task, agent, project, repo, branch, model, summary | V1 broader |
| query length 1-2 | yes | refused | Gap |
| workspace / source / project / since / until / cursor | yes | yes | Same signature; **result parity unmeasured** |
| agent | History API | exact/prefix | Prefix fails the index gate |
| kind / taskRef / noTaskRef / tag | History API | no | Gap |
| files_touched (memory pack) | yes | no | Gap |
| semantic / hybrid ranking | dashboard | no | Gap |
| per-session match window (`get_shared_session_context`) | yes | no | Gap |
| excerpt / snippet / citation payload | yes | summary row only | Gap |
| user / machine / state | partial | yes | V1 broader |

**Queries that would return nothing or less for a post-T0 session on V1:**
- transcript phrases: error strings, stack traces, commands, function names;
- file paths;
- PR or issue numbers;
- 1-2 character tokens;
- jump-to-match;
- conceptual (semantic) queries;
- recent sessions whose short_summary has not been generated yet;
- queries spanning T0, because no merge, order or dedup spec existed.

**Nothing in Cloud routes to V1 today.** The MCP tools' move (P4) has not
started.

### Gate G-T: transcript search for new sessions is live (hard pre-T0, critical path)

All of the following must hold in production, dark, before K-17. Each is
proven on the phase 2 snapshot and with the live-protocol writer.

| # | Requirement | Exit evidence |
| --- | --- | --- |
| G-T0 | **History store: store-agnostic (K-39, K-40).** The candidates are ObsessionDB (intended, conditional, K-28), ClickHouse Cloud and Amazon OpenSearch Service, all in AWS `us-east-1` for production (K-33). The choice is made by **G-T0E** and section 10, applied the same way to every candidate. The 0001 and 0002 amendments are merged. | G-T0E evidence pack per candidate; Khaliq chooses and confirms the store; #233 updated |
| G-T1 | **Async history indexer.** Pipeline normalized objects (the file-normalize / E4 stage) load into the history store per session revision, **off the hot path**: ingest never waits on it. It is idempotent by content hash, handles tombstones and resets, and has its own queue, DLQ and replay. Enabled before T0 (K-30). | Feed tests; replay from S3 rebuilds the store; indexer lag within SLO row 8b; an ingest test proves no ingest latency change when the indexer is stopped |
| G-T1R | **48 h safety-net window writer and adapter (K-39, K-47).** A pipeline stage writes complete scrubbed event text into the Supabase 48 h window (trigram/FTS), with the retention job and immediate tombstone/reset deletes. The Postgres adapter serves it, plus metadata and summary search. | Migration plus RLS tests; retention-job tests; window freshness within SLO row 8a with the history indexer stopped |
| G-T1M | **History-first merge with silent-partial fallback (K-39, K-46, K-47).** Every transcript search asks the **history store first** (primary), with a **hard 1.5 s budget**. The 48 h window fills only the indexer-lag gap: events newer than the history store's indexed watermark. On a history error or timeout, the response returns the **48 h window plus metadata/summary results**, plus the T0 child for pre-T0, **with no user-facing label in the UI or MCP text**, and never a 5xx for that case. An internal `coverage` field in the response metadata (for example `{history: "unavailable", reason: "timeout"}`), logs, metrics and the partial-rate alarm record it. | Fault-injection tests (history store down, slow past 1.5 s, erroring); UI and MCP snapshot tests showing no partial text; metric and alarm fire tests; SLO rows 4, 14 and 15 |
| G-T2 | **`search_shared_sessions` parity.** Same request contract: query length 1-500, filters, cursor, limit ≤20. Same response: event excerpts (`max_content`), `citation_url`, newest-first order. Same workspace confirmation that Cloud enforces. | The Cloud MCP contract tests pass unchanged against the new backend |
| G-T3 | **`get_shared_session_context` in-session window.** For post-T0 sessions it reads that session's S3 objects and filters in the service, one session at a time (0002's surface table), so it **does not depend on the history store**. For pre-T0 sessions it uses the child branch. | Contract tests; window equality against legacy on the snapshot |
| G-T4 | **Recall parity** against legacy on the frozen snapshot | The section 10 recall thresholds |
| G-T5 | **Merge, ordering and dedup spec across adapters and T0**, written, reviewed and implemented (below) | Spec doc plus tests for straddling sessions, duplicate hits, paging across sources and one-source failure |
| G-T6 | **Isolation and erasure (K-44, K-45).** **Isolation needs both layers:** application-layer scoping as the primary control (every adapter query is bound to org and workspace from the verified claims), plus a **DB row-policy backstop** on a per-tenant or scoped DB user, so the store itself refuses cross-tenant rows. Ask the vendor to enable row policies and user creation. **Erasure:** a logical delete (tombstone, reset, workspace erasure) hides rows **immediately**, and **physical purge is complete within 48 h**, including ObsessionDB's 24 h time travel. | Planted canaries: zero cross-tenant rows **with the app-layer filter deliberately removed** (proving the backstop) and through the app layer. Erasure tests: immediate invisibility; physical absence confirmed by 48 h, including after the time-travel window. |
| G-T7 | **Recall consumers.** The memory-pack lexical and `files_touched` pools go through the search interface: the history store first, with the 48 h window for the lag gap. | Consumer tests |
| G-T8 | **Dashboard ranked search, keyword only (K-34 as narrowed by K-36).** `sessions?q=&ranking=hybrid` is served on the new stack for post-T0 sessions through the search interface: the **history store's full-text over complete transcripts (primary, K-47)**, with the 48 h window for the lag gap and the same silent-partial fallback. It fuses the remaining legacy keyword pools (`event_keyword`, `brief_keyword`) per the serving contract, with snippets ≤ 320 chars. There are **no vector pools before T0**. The response says which pools were offered, so the missing semantic pools are not reported as a failure. | Hybrid benchmark lane (section 10, SLO rows 12 and 13) **against legacy hybrid, semantic pools included**; the dashboard read-API contract tests pass |

### Gate G-T0E: history-store capability and security evidence, for every candidate (before K-29)

The full-history store is chosen only from candidates with evidence for
every item. The same benchmark (section 10) and the same evidence items
apply to each one. If no candidate passes, the choice goes back to Khaliq.
ObsessionDB is the intended choice (K-28), but it is confirmed only through
this gate.

**Trials.** All are synthetic data only, and **none is approval to
provision production** (K-29 stays separate).

| Candidate | Trial | Scope | Approval |
| --- | --- | --- | --- |
| ObsessionDB | free dev service, eu-central | functional only (no performance or region evidence) | K-37 |
| ClickHouse Cloud | trial, **$50 hard cap** | engine performance on ClickHouse text indexes | K-37 |
| Amazon OpenSearch Service | proof account **500268966557 / `us-west-2`**, **about $50 cap** | functional, plus performance at trial scale | K-40 |

**Evidence items.** The "expected" column is a prior assumption to verify,
not evidence.

| # | Evidence item | How it is shown | ObsessionDB (expected) | ClickHouse Cloud (expected) | OpenSearch (expected) |
| --- | --- | --- | --- | --- | --- |
| E1 | Full-text and **substring** performance at production scale: legacy-style `%term%`, tokens, n-grams; common, selective and no-match terms; whale shape | p50/p95/p99 at 1/8/32 clients against section 10, on a synthetic corpus shaped by K-38 counts (scale M plus the whale shape at least) | dev service is functional only; scale needs vendor evidence or a later approved test | trial measures engine behaviour | **weaker on substring cost:** needs n-gram or wildcard fields, with index size and indexing cost to measure; trial measures it |
| E2 | Tenant isolation, **both layers (K-44):** application-layer scoping (primary), and a DB-level backstop through row policies on scoped users (document-level security on OpenSearch). The vendor must enable row policies and user creation. | Planted cross-tenant canaries: zero rows through the app path **and** with the app filter removed (backstop alone); misconfigured-claim negatives | row policies + user creation: **request from vendor** | row policies (native) | document-level security / fine-grained access control via IAM |
| E3 | Erasure **SLA (K-45):** logical delete immediately; **physical purge within 48 h**, including time travel, merges, segments and snapshots | Delete tests; the documented and observed physical-removal time ≤ 48 h | 24 h time travel must fit inside 48 h; merge timing to verify | `ALTER … DELETE` or lightweight delete, plus merge timing | delete-by-query plus segment merge; snapshot retention ≤ 48 h or excluded |
| E4 | IP allowlisting and **private networking** from the Worker or ingest path in `us-east-1` | Config on the production tier | vendor docs | PrivateLink (vendor) | **VPC domain plus IAM** (expected strong) |
| E5 | SOC 2 Type II, encryption at rest and in transit, audit logs, DPA | Reports on file | vendor | vendor | **AWS compliance programmes** (expected strong) |
| E6 | `us-east-1` availability of the production tier, with SLA | Confirmation | vendor | vendor | **native AWS region** (expected strong) |
| E7 | Backup and **rebuild from S3** to parity (P-1, P-3), with the duration recorded | Rebuild run on synthetic objects | dev service | trial | trial (plus snapshot restore) |
| E8 | The functional contract: excerpts, keyword pools, cursor, tombstones (G-T2, G-T8) | Contract tests through the adapter | dev service | trial | trial |

**Decision rule** *(proposed)*:
- A candidate must pass E2 to E8 outright and meet the section 10 SLOs
  (rows 4, 12 to 15) on E1.
- Among passing candidates, Khaliq chooses on cost and operations.
- Because the store is off the hot path (K-39), a later swap is a rebuild
  from S3, not a migration.

The corpus is synthetic, sized and shaped from K-38's counts-only
extraction. No production content moves, and every trial stays inside its
approved scope, account and cap.

### Merge, ordering and dedup spec across adapters and T0 (G-T5, minimum contents)

1. **Source selection, per query (K-47: history first):**
   - **History store (primary):** every indexed event, post-T0 and exported
     pre-T0.
   - **48 h window (Postgres):** only events newer than the history
     store's indexed watermark (the lag gap). If the history store failed
     on this request, every in-window event.
   - **T0 child branch:** pre-T0 sessions not yet loaded (coverage marker
     unset).

   A straddling session draws on all of these. Events at or before the
   marker come from the child or the history store, later events from the
   window and the history store.
2. **Dedup key:** `(org, workspace, session, record id, revision)`. On a
   duplicate, keep the highest revision. With equal revisions the history
   store wins (it is primary), then the window, then the child. A post-T0 tombstone suppresses the hit from
   every source.
3. **Order (MCP):** event time descending, with a stable tiebreak on the
   source record id. This matches legacy.
4. **Paging:** a composite opaque cursor carrying each source's position.
   The limit applies after the merge.
5. **Excerpts:** identical shape from every source (`max_content`,
   `citation_url`). Matching uses **complete** event text in the history
   store and in the 48 h window (K-42), where legacy matches only the
   first 4 KB. New-stack hits are therefore a superset of legacy's, and the
   extra hits are reported as additive in parity runs.
6. **Partial results (K-39):**
   - **History store errors or exceeds its budget:** return the **48 h
     window's results plus metadata/summary matches** (plus the child's for
     pre-T0) as a **silent partial**
     (K-46). Users see no label in the UI or MCP text. The response
     metadata carries the internal `coverage` field, and the case is logged,
     metered and alarmed. It is never a 5xx.
   - **Child branch down:** also a silent partial, with
     `coverage.pre_t0 = "unavailable"`.
   - **Postgres adapter down, history store up (K-47):** transcript search
     still runs on the history store. Only the lag gap (events newer than
     the watermark) is missing, so it is a silent partial with
     `coverage.window = "unavailable"`. Metadata and filter routes (V1) are
     down and return retryable 503s.
7. **Budgets:**
   - the history adapter: **1.5 s hard** (K-42);
   - the child branch: its breaker and a 6 s pool budget;
   - the whole request: within legacy's 12 s and Cloud's 15 s / 25 s.
8. **Ranked dashboard queries (K-36):** RRF (k=60) over `event_keyword`
   (weight 1) and `brief_keyword` (weight 3). Each pool's candidates are the
   deduped union of the window, the history store and the child.
   - For merged queries the child's semantic pools are not used.
     *(Proposed.)*
   - A query whose whole window is pre-T0 may use full legacy hybrid on the
     child. *(Proposed; Khaliq to confirm.)*

### Search serving contract (reviewer requirements, G-T)

These definitions are binding on G-T. Values marked *proposed* are plan
defaults that Khaliq confirms with the section 10 thresholds (K-5).

**1. Legacy behaviour to match.** Legacy hybrid search is in
relayhistory-cloud `packages/relayhistory/src/lib/session-search.ts`
(origin/main `45d53b1`). The new stack must reproduce it exactly for parity
runs.

| Item | Legacy value |
| --- | --- |
| Pools, in report order | `event_semantic`, `event_keyword`, `brief_semantic`, `brief_keyword` (`SESSION_SEARCH_POOLS`). **On the new stack before T0, only `event_keyword` and `brief_keyword` exist (K-36).** |
| Fusion | Reciprocal-rank fusion: each pool adds `weight / (60 + rank)` per session (`SESSION_SEARCH_RRF_K = 60`; rank is 1-based) |
| Weights | event pools 1 each, brief pools 3 each (`SESSION_BRIEF_POOL_WEIGHT`), so a brief match outranks a session matched only by events at the same place in both event pools |
| Per pool | the first (best) row per `(source, session)`; at most 1,000 sessions per pool (`SESSION_POOL_LIMIT`) |
| Tie-break | score descending, then brief match first, then latest match time descending, then session id |
| Keyword-pool bound | events in the last 30 days and among the org's newest 50,000 events (`KEYWORD_POOL_WINDOW_MS`, `KEYWORD_POOL_ROW_CAP`). Semantic and brief pools reach older sessions. |
| Budgets | 12 s for the whole request (Cloud gives up at 15 s); 6 s per pool; `503 search_timeout` past the total |
| Limits | at most 25 per page and 100 results (`MAX_SESSION_SEARCH_LIMIT`, `MAX_SESSION_SEARCH_RESULTS`) |
| Embeddings (legacy only) | `text-embedding-3-small`, 1,536 dimensions (`lib/embed.ts`), stored per row in `embedding_model`. There are none on the new stack before T0 (K-36); see section 17. |

**New-stack fusion before T0 (K-36):** RRF k=60 over `event_keyword`
(weight 1) and `brief_keyword` (weight 3), with the same per-pool cap,
tie-break, budgets and limits as legacy. The keyword pool searches
**complete transcripts** on ObsessionDB, not legacy's 4 KB event prefix.

The keyword-pool bound is a performance workaround on Neon. Parity runs
keep it. An unbounded keyword pool on ObsessionDB is measured as a separate
variant, and adopted only with Khaliq's OK (it changes recall).

**2. Embedding and model versioning.** Moved to the post-T0 follow-up
(section 17). There are no embeddings before T0 (K-36).

**3. Snippets and `citation_url`.**
- **MCP `search_shared_sessions`:**
  - the excerpt is taken from the event's content around the match, which
    may lie beyond legacy's 4 KB, since complete text is searched (K-42);
  - it is cut to the caller's `max_content`;
  - it is identical from the child branch and from ObsessionDB.
- **Dashboard hybrid snippets:**
  - snippet precedence is brief over event, and a literal (keyword) match
    over a semantic one (before T0 only keyword pools exist, so the brief
    excerpt wins, then the event excerpt);
  - whitespace is collapsed and the snippet is at most 320 characters;
  - when the match lies more than 80 characters in, the snippet starts 80
    characters before it, with `…` at any cut edge (`boundedText`).
- **Scrubbing:** all excerpts are scrubbed text. Content is scrubbed before
  storage, and ObsessionDB never receives unscrubbed text.
- **`citation_url`:**
  - Cloud keeps generating it (`sessionCitation` in
    `packages/web/lib/mcp/shared-sessions.ts`):
    `<app origin>/dashboard/sessions/<sessionId>` plus the source;
  - the backends return only `(source, session_id)`;
  - after dedup, each hit cites one session, and only sessions in the
    caller's confirmed workspace are returned.
- **Briefs:** the brief pools' new-stack text source is the pipeline
  summary, short and long. That puts summary plaintext into ObsessionDB.
  Q22 covers this.

**4. Degraded behaviour (K-39).**

| Failure | Dashboard ranked search | MCP `search_shared_sessions` | MCP `get_shared_session_context` | V1 metadata |
| --- | --- | --- | --- | --- |
| Semantic pools (none before T0, K-36) | Not offered. `pools_offered: ["event_keyword","brief_keyword"]`, and they are not counted as degraded. The UI labels the mode "Keyword search". | n/a | n/a | unaffected |
| **History store errors or times out (1.5 s budget)** | **48 h window + metadata/summary** results, plus child results for pre-T0, **with no banner or label** (K-46). Internal `coverage.history = "unavailable"` in the response metadata, not rendered. Never 5xx. | the same sources, **with no partial text in the tool output** (K-46). Internal `coverage` in the structured metadata only. | unaffected (S3 read path for post-T0) | unaffected |
| History indexer lagging (store up) | The lag gap is covered by the 48 h window. Internal `coverage.history_lag_s` is recorded; nothing is shown to users. **A lag above 48 h loses events from search, so lag is alarmed well before that** (section 13). | same | unaffected | unaffected |
| Child branch down or breaker open | Results without pre-T0 sessions, **silent partial** (K-46), with internal `coverage.pre_t0 = "unavailable"` | same, with no partial text | for a pre-T0 session: retryable `503` (single-session read, not a search) | unaffected |
| Supabase (Postgres adapter) down | Transcript search continues on the history store, as a silent partial (lag gap missing, internal `coverage.window`). Metadata-only filters return a retryable 503. | same | Post-T0 sessions are still served from S3 if the pointer lookup is cached; otherwise 503 | down |
| Whole request budget exceeded | `503 search_timeout` (legacy) | same | same | — |

The new metadata fields (`coverage`, `pools_offered`) need a Cloud MCP
and dashboard schema change (P4). They are **metadata only and never
rendered** (K-46).

**Every partial response must:**
- carry `coverage`;
- emit a structured log line with codes only;
- increment `search_partial_total{reason,surface}`.

The partial-response-rate alarm is in section 13. The "Keyword search" mode
label for the missing semantic pools describes the mode, not a
degradation; whether to keep it is for Khaliq (Q28).

**5. Federation across adapters and T0.** See the merge spec above (G-T5,
items 1 to 8):
- which source serves each session;
- dedup;
- newest-first order for MCP, and RRF for hybrid;
- the composite cursor;
- partial failure (also item 4 above);
- budgets.

**6. Relevance gates.** These are section 10 rows 2, 4, 5, 12, 13 and 14:
- recall@10 and recall@20 against legacy per query class: at least 95% on
  lexical queries;
- recall tasks hit at k=5 and k=10 wherever legacy hits;
- nDCG@10 of keyword-only new-stack ranking at least legacy **hybrid**
  nDCG@10 − 0.02, with pooled blind
  judging;
- V1 lexical overlap at least 95%.

**7. Freshness per pool.** Measured from the transcript write on the desktop
to the first hit; *proposed*. K-39 makes recent freshness independent of
the history indexer.

| Pool or surface | p95 | p99 |
| --- | --- | --- |
| V1 metadata (list and filters) | 60 s | 120 s |
| V1 short_summary | 5 min | 10 min |
| **48 h window**, transcript keyword (Postgres safety net; covers the lag gap) | 60 s | 120 s |
| `brief_keyword` (needs a summary) | 5 min | 10 min |
| **History store (primary)**, indexer lag (hidden by the window while under 48 h) | 15 min | 60 min, **alarm at 6 h**, critical at 24 h |

Sessions not yet in a pool still appear through the faster pools, so
freshness is per pool, not per session.

**8. Rollback for search.**
- **Before T0:** any red G-T result holds T0. Every search component is
  dark, and the history store and 48 h window can each be rebuilt from S3
  without user impact.
- **After T0:** forward-only (K-22, section 11). The levers:
  - **turn the history adapter off.** This is a short-outage lever only:
    search then serves the 48 h window plus metadata/summary and the child,
    as a silent partial (K-46, K-47). Older transcripts are not searchable
    while it is off;
  - rebuild the history store from S3, on the chosen candidate or another one that passed G-T0E;
  - rebuild the 48 h window from S3;
  - the G-F write freeze.

  There is never a return to Neon for post-T0 sessions.

**9. Regional data-flow boundaries.**

| Store or service | Region | Holds transcript content? | Status |
| --- | --- | --- | --- |
| Neon production and the T0 child branch | AWS `us-east-1` | yes (rows) | exists |
| Supabase `sessions` | AWS `us-east-1` (K-33; verify) | metadata, bounded short_summary, and **a 48 h window of complete scrubbed transcript text** (K-47; 0002 Amendment 1); no embeddings | exists, empty |
| Production S3 bucket | **must be `us-east-1`** (required by this boundary) | yes (canonical) | not created (K-2) |
| History store production (store-agnostic; ObsessionDB intended, ClickHouse Cloud or OpenSearch per G-T0E) | AWS `us-east-1` (K-33) | yes (full-history full-text projection, derived from S3; no embeddings before T0) | not provisioned. K-29 comes only after G-T0E. |
| RelayHistory Worker, Queues | Cloudflare edge (global) | in transit only; no persistent copy | exists |
| Summary model provider (embeddings only after section 17) | provider's region (unknown) | in transit to the provider; retention per its terms | summary-short enablement (K-13); embeddings K-35 |
| `relay-transcript-proof` | AWS `us-west-2` | **synthetic data only** | proof |
| ObsessionDB free dev service | eu-central | **synthetic data only**, functional only (K-37) | trial |
| ClickHouse Cloud trial | trial region (unknown) | **synthetic data only**, $50 hard cap (K-37) | trial |
| Amazon OpenSearch Service trial | AWS `us-west-2`, proof account 500268966557 | **synthetic data only**, about $50 cap (K-40) | trial |
| `aws-proof` EKS stack | AWS `us-west-2` (500268966557) | synthetic orgs (`org_aws_proof_*`) only | proof |

**Rules:**
- No production transcript content is copied out of `us-east-1`, except in
  transit through Cloudflare or the model provider.
- The `us-west-2` proof services and the K-37 and K-40 trials (eu-central
  ObsessionDB dev, ClickHouse Cloud, OpenSearch in 500268966557 /
  `us-west-2`) take only synthetic data. Exporting or
  loading a real org into them is forbidden.
- The K-38 shape extraction moves aggregate counts only, never content.
- Staging stores for phase 2 hold real W0 data (K-12), so they must also be
  in `us-east-1`.

---

## 7. Phases and per-tenant rollout

The tenant unit is the org (`org_id`). Workspace scoping inside an org follows
SEC-10's open decision. Waves are proposed; Khaliq sets their membership.

| Wave | Members | Why |
| --- | --- | --- |
| W0 | Internal AgentWorkforce orgs | Real data, a tolerant audience, and us as the observers |
| W1 | Up to 5 small external orgs, by session count | Small blast radius, real diversity |
| W2 | Remaining orgs below the 95th percentile of `delivery_records` per org | Bulk |
| W3 | The largest orgs, one at a time | Hottest keys, and the largest export and fallback load |

**Measured (K-38):**
- 282 orgs and 289 workspaces;
- sessions per org: p50 6.5, p90 725, p99 17,041;
- the top org has 23.2% of events, and the top 10 hold 74% of content bytes;
- the largest org has only 12% of the last 14 days' content.

So W3 (the largest orgs, one at a time) carries most of the export bytes,
and W1 and W2 are many small orgs. Wave membership is still Khaliq's to
set.

### Phase 0: code prerequisites (no live action except merges)

Each item's exit gate is its own two-pass review plus CI. Every merge is K-1.

- relayhistory-cloud:
  - #223 (runner over TCP);
  - #224 (gate plus HoldLog plus the data-loss fix; migration renumbered off
    0066 and 0067);
  - #225 (TCP seam);
  - #227 then #231 (pointer index activation, flag-gated);
  - #226 (G3: terminal reads; it must route transcript search to the child branch for pre-T0 sessions and to the search interface (history store primary + 48 h window, K-39, K-47) for post-T0 sessions, per the G-T5 spec, instead of failing closed);
  - #222 or its replacement (P5).
- 96BullSessions:
  - #38 (directory), #48 (short_summary, already merged into the #38
    branch), #72 (token pins), #73 (PostgREST isolation suite);
  - #40 (exporter core);
  - #63 (issuer design: Khaliq has **not** approved the separate-issuer
    direction).
- relayauth: #116 (search issuer), #110 (migration grants; E2).
- cloud: #5850 (post-T0 write gating, on hold).
- **New, unowned work:**
  - **G-R1:** a pipeline producer for Cloud's `relayhistory-project-events`
    (section 3).
  - **G-P:** the `session_directory` projector from `session_index` and
    manifests. #38 has "no projector", and its merge-precedence rule needs
    Khaliq (K-4).
  - ~~G-V~~: the reverse path is **not built**. K-22 decided forward-only
    after the flip.
  - **G-S:** shadow-read compare mode in the Worker (section 9).
  - **G-H:** a hosted creation path for the search-issuer API key and
    signing key (section 14, S3).
  - **G-T:** the transcript-search lane (section 6): store, feed, MCP
    parity, context window and cross-T0 merge. **Critical path.**
  - **G-F:** a post-flip write-freeze lever (section 11). **Required before
    T0.**

### Phase 1: provision, dark

Approvals K-2, K-3, K-7 to K-10.

1. Create the production S3 bucket and per-workspace KMS scheme (P9), the
   bucket policy (TLS-only, no public access) and the lifecycle rule. Create
   the Worker's IAM identity, least-privilege per prefix (S5).
2. Apply the 96Bulls Supabase migrations to `sessions` (A3), together with
   #38, #48 and #72. Seed the `session_directory_search_token_policy` row
   (issuer, audience, max lifetime of 60 s); until that row exists, search
   stays refused.
3. Create a staging Supabase project and a staging bucket that mirror
   production config. The rehearsal uses them, never the production ones.
4. Release relayauth with #116, deploy it to hosted (via a cloud bump PR),
   create the search issuer key (G-H), and configure Supabase to trust only
   that issuer.

- **Exit:**
  - `bun run audit:search-indexes` passes on production Supabase;
  - #73's suite passes against the hosted project (its test 25, which has
    never run);
  - an unauthenticated or wrong-issuer RPC call is refused with `42501`.
- **Rollback:** drop the empty schema and bucket, and revoke the keys. No
  user data exists yet.

**Also in phase 1 (K-29, K-32), after G-T0E passes and Khaliq confirms the history store:** resize the Supabase disk from 2 GB.
Provision the transcript search store, production and staging, with
per-org isolation and credentials held as Worker secrets.

### Phase 2: rehearsal snapshot, parity and benchmark (W0 data, staging only)

Approvals K-11 and K-12.

1. Create a **rehearsal** Neon child branch, `rehearsal-<date>`, from
   production. It is copy-on-write with its own compute. Record its LSN.
2. Export the W0 orgs from the rehearsal branch into the staging bucket and
   staging Supabase project with `migration:export` (#40 plus the courier).
   This run also checks the export throughput, object size and PUT count (O5).
3. Run the parity checks (section 9) and the benchmark gate (section 10).
   Legacy is measured on the rehearsal branch and the new path on staging,
   both against **the same snapshot**, so parity is exact, not approximate.
4. Load the snapshot's normalized objects into the **staging search
   store** through the real G-T1 feed. Then run the section 10 transcript
   lanes and the G-T5 cross-T0 merge tests. For the merge tests, treat part
   of the snapshot as "pre-T0" (served from the rehearsal branch) and part
   as "post-T0" (served from the store).
5. Rehearse T0 end to end against staging: hold, fence on a scratch branch,
   marker, child, flip, and the abort path. The abort path is the only data
   rollback (K-22), so rehearse it at least twice, including an abort taken
   at step 6 (child branch validation fails).

- **Exit:** every parity check is exact, the benchmark gate is green, and
  the T0 and abort rehearsals are green. **A red result does not proceed and
  is not explained away.**
- **Rollback:** delete the staging objects and rows, and the rehearsal
  branch.

### Phase 3: deploy dark

Approvals K-13 to K-15.

- Deploy relayhistory-cloud with the gate bound, `CUTOVER_GATE_ENABLED=true`,
  in phase `neon`.
- Flags stay off: `PIPELINE_SESSION_INDEX_ENABLED`,
  `PIPELINE_SUMMARY_SHORT_ENABLED` and the post-T0 ingress.
- Deploy cloud #5850 with its flag off.
- Enable the G-T1 feed into the production search store (K-30). Before T0
  it receives only canary sessions from the live-protocol writer.
- Prove the G-F write-freeze lever in production on a canary route (K-25
  rehearsal).
- Enable **shadow-read** (G-S) for W0 orgs only, against the staging copy
  from phase 2. This measures latency on live request shapes; it does not
  serve responses.

**Exit:**
- `GET /v1/internal/cutover/phase` returns `neon`;
- canaries for health, upload and read are unchanged;
- the 5xx rate is at baseline for 24 h;
- the shadow-read mismatch rate is below threshold.

**Rollback:** unset `CUTOVER_GATE_ENABLED` and redeploy.

### Phase 4: T0, the global write flip

Approval K-16; each step is K-17. See section 8.

**Exit:** canaries for every route class pass; HoldLogs are empty; 5xx is
back at baseline; a fresh W0 session is visible in V1 search within the
freshness SLO.

**Rollback:** before the flip, abort. After the flip the system is
forward-only (K-22): fix forward (section 11).

### Phase 5: per-tenant export waves W0 to W3

Approval K-18, once per wave.

- Export the wave's orgs from the **T0** child branch to the production
  bucket, newest session first, deletion-fenced and checkpointed. The ingest
  accepts them only with a migration assertion (#228, #79, relayauth #110).
- Summaries are generated per wave, within a model budget.
- Exported sessions also flow through the G-T1 feed into the search store.
  Once a session is loaded, the G-T5 coverage marker moves its transcript
  search off the child branch.
- **Per-wave exit:**
  - `check` reports 0 missing for the wave's orgs;
  - per-org parity against the child branch is exact (section 9);
  - the fallback hit rate for those orgs is trending to 0.
- **Per-wave rollback:** stop the export. The wave's reads keep falling back
  to the child, so nothing is lost. Exported objects are additive and
  idempotent, so they can stay, or be deleted by prefix once relayhistory-cloud #213 (open) lands.

### Phase 6: per-tenant fallback removal and V1 search on

Approval K-19, once per wave.

- For each wave, drop the replay and metadata fallback for those orgs (A9,
  scoped per org) and enable V1 search as the primary search path.
- Pre-T0 transcript search stays on the child branch (K-6) until the
  wave's sessions are loaded into the search store. After that, the wave's
  transcript search moves to the store, and the child serves it only for
  sessions not yet loaded.
- **Rollback:** turn the per-org fallback back on. This is a config change,
  and the child branch still exists.

### Phase 7: retirement

Approvals K-20 and K-21. These happen after V2 or after Khaliq accepts the
loss of pre-T0 transcript search.

- Archive the final marker and reports, then remove the child branch.
- Drop Neon `sessions`, once a Neon snapshot has been taken. Cloud's `public`
  schema stays.

### Alternative: per-tenant write waves (B-waves), not recommended

Each wave would get its own hold, marker and child branch, and only that
wave's orgs would flip.

**Costs:**
- #224 would need per-org phases. Today there is one global phase.
- The Neon DB fence is schema-wide and cannot fence one org. Until the last
  wave, the fence would be application-only, which weakens I2.
- One child branch per wave.
- Neon production keeps taking writes, and the stress, until the last wave.

**Benefit:** the blast radius of a bad flip is limited to one wave.

**Recommendation:** use option D's single flip. The blast radius is
controlled instead by:
- the phase 2 rehearsal on real W0 data;
- the abort boundary;
- per-wave read and search phases.

If Khaliq wants B-waves, that is a #224 redesign. It would move T0 out.

---

## 8. Write fencing and the T0 sequence

This is the T0 protocol in `docs/relayhistory-switch-first-dag.md`, with
commands from `docs/relayhistory-t0-runbook.md` (steps 8 to 12). Changes
under 0002 are in **bold**.

1. **Pre-flip readiness.** Everything in the switch-first doc's "Pre-flip
   readiness", plus:
   - section 10 is green;
   - phase 2's rehearsal is green;
   - G-R1 is deployed;
   - **G-T is green (K-27):** the feed is live in production, the MCP
     search and context-window contracts and recall parity pass, and the
     cross-T0 merge is implemented;
   - **G-F is built and rehearsed;**
   - the child-branch transcript route (K-6) passes the legacy lane in
     section 10;
   - the abort path has been rehearsed (K-22: no rollback after the flip);
   - the NTP offset is under 1 s on the RelayAuth, ingest and courier hosts
     (runbook "Export assertions: clock health").
2. **Hold.** Move the gate `neon → holding` (compare-and-set,
   `CUTOVER_ADMIN_SECRET`).
   - The four replay-safe uploads go to the HoldLog DO, ordered per
     credential digest.
   - Other writes get 503 with `Retry-After`.
   - Reads stay on Neon.
   - Crons skip, and consumers `retryAll`.
3. **Drain.** Wait until `pg_stat_activity` shows no active Worker or Cloud
   runtime-role session, up to the 125 s statement ceiling. Then apply the
   reviewed `migration:neon-fence` `.fence`, which revokes DML and EXECUTE on
   volatile `sessions` functions. Keep `.restore`.
4. **Catch up the copy set** (`migration:copy --mode=mirror`). Re-apply and
   check `migration:sequence-fence`. Re-run `migration:verify --mode=schema`
   for the legacy state tables that remain on Supabase.
5. **Marker.** Record `pg_current_wal_lsn()`, the timestamp and the gate
   generation. It is written once to S3 and to Supabase and never edited.
6. **Child branch** at the marker's LSN. Validate:
   - the marker reads back from the child;
   - the ledger and row counts equal production at the LSN;
   - the child answers read-only queries.
7. **Flip.** Move `holding → supabase`. At the same moment:
   - **enable the post-T0 ingress and `PIPELINE_SESSION_INDEX_ENABLED`**;
   - **route post-T0 transcript search to the search interface (K-39), per the G-T5
     spec**;
   - flip cloud #5850's flag.

   HoldLogs replay in order into the pipeline, and crons and consumers
   resume.
8. **Verify.** Run canaries for: login, ingest, delivery batch, turns,
   session list, V1 search, replay of a post-T0 session from S3, replay of a
   pre-T0 session through the child, transcript search through the child
   (K-6), transcript search on a post-T0 session through the search interface, plus a forced history-store timeout returning a silent partial with internal coverage metadata and the metric incremented (G-T, G-T1M, K-46),
   a cross-T0 merged query (G-T5), brief, and a Cloud revision event (G-R1).

**Abort.** Steps 2 to 6 can be aborted:
1. apply `.restore`;
2. move the gate `holding → neon`;
3. the HoldLogs replay to Neon;
4. discard the child.

**Hold budget.** The target is under 5 minutes. Rehearse the copy-set
catch-up time in phase 2, and keep it under about 60 s (runbook step 3).

---

## 9. Dual-run, shadow-read and parity checks

**Exact parity on a frozen snapshot.** This is the gate. It runs in phase 2
on the rehearsal branch, and in phase 5 on the T0 child branch:

| Check | Legacy side | New side | Pass |
| --- | --- | --- | --- |
| P-1 Session set per org/workspace | distinct `(workspace, machine, harness, session)` from live `delivery_records` | `session_index` rows | equal sets; tombstoned and deleted sessions absent on both |
| P-2 Metadata | Neon rollup and catalog: title, agent, model, repo, branch, usage, first and last activity | `session_index` plus `session_directory` | equal per field. The collation difference (`C.UTF-8` vs `en_US.UTF-8`) only matters for ordering and prefixes, so check those under `en_US`. |
| P-3 Replay | ordered `delivery_records` for the session | S3 raw stream, normalized | equal record sequence by content hash (export-design gate) |
| P-4 Lists and filters | Cloud Sessions board and MCP list queries | same API on the new path | same session ids and order for each filter in `docs/session-search-projection.md` |
| P-5 V1 search | legacy title, agent and metadata match (the ILIKE subset of the same fields) | `search_session_directory` | the new results ⊇ the legacy matches on those fields. Summary hits are additive and are reported, not failed. |
| P-6 Tombstones and resets | `deleted_upload_sessions`, `session_resets` | the export skips them, and post-T0 deletes win | zero resurrected sessions |
| P-7 Tenant isolation | — | cross-org and cross-workspace probes (section 14) | zero rows |

Reports contain counts, hashes and ids only. They never contain content or
query text (`benchmark:relayhistory` convention).

**Shadow-read on live traffic** (G-S, phases 3 and 5). For a sampled
fraction of read requests from allowlisted orgs, the Worker also runs the
new-path read. It compares a response hash, logs only a match/mismatch code
and the latency, and always serves the legacy response.

- Before T0 it can only compare sessions frozen at the rehearsal LSN.
  Mismatches on newer sessions are expected and excluded by `last_activity
  < rehearsal LSN time`.
- Mismatch threshold: 0 on frozen sessions. Any mismatch blocks the next
  phase until it is explained.

**No dual-write.** Option C is rejected (section 5), so there is no
write-side dual run. Ingest-to-searchable freshness for the new path is
measured on staging in phase 2, through a synthetic writer that follows the
live client's protocol, and on W0 after T0.

---

## 10. Benchmark gate: the cutover gate (legacy vs new)

This adopts the design and proposed SLOs from
`~/selfhost-proof/notes/search-recall-comparison-2026-10-31.md` §5. Two
changes are made to it:
1. Transcript lanes for the search interface are added (K-27): each adapter alone, merged, and fallback (K-39).
2. **V1 is measured on staging Supabase, which is production-shaped,** not
   on EKS/CNPG. EKS numbers are self-host evidence only (0001).

**What exists today** (the report's §3):
- **Legacy:** single-query EXPLAINs on Neon branches cut from production
  (#146, #152). They show 4-140 s failures fixed to sub-second warm, a
  6.8-11 s cold path and incident 503s. There are no percentiles or
  concurrency figures.
- **V1:** synthetic data on GitHub-runner Postgres only (#59). Its own
  agent-prefix gate is red at 1409× read amplification.
- **The new transcript store:** relayhistory-cloud **#236** (draft: design
  plus prototype, "transcript search on ObsessionDB, off the hot path").
  Its numbers are **indicative only, not gate evidence**:
  - local prototype at 10M events: p95 ≤ 105 ms;
  - ObsessionDB free dev service: 200-470 ms (eu-central, functional tier);
  - ClickHouse Cloud trial: 60-305 ms;
  - **Postgres window, selective queries: 3.1 s p95**, measured on the
    larger window. This is a known gap, and part of K-47's reason for
    making the history store primary. SLO row 15 is re-measured on the 48 h
    window.
  - The history store was **about 10-30× faster** than the Postgres window
    in the prototype (K-47 rationale).
- **Recall and freshness:** none, on either side.

**Data.**
- One frozen snapshot (phase 2): a Neon child branch at a recorded LSN.
- The same orgs, loaded into staging Supabase and the staging search store
  **through the real pipeline**, not a hand loader, and summarised by the
  real summary stage.
- Include the whale org (about 1.3M+ events in that org; about 4M+ events
  total) and small tenants.
- Use #59's synthetic S/M corpus only for scale shape.

**Environment.**
- Legacy: the Neon child at production CU, through the deployed
  relayhistory-cloud Worker.
- New: the Worker plus the JWT path, against staging Supabase and the
  staging search store.
- Measure end to end from the Cloud MCP and dashboard handlers, plus
  DB-only timings, from each representative user region.

**Query set** (fixed and versioned, in the repo; hashes only in reports):
- about 200 queries sampled from anonymised `search_shared_sessions` and
  dashboard `q` logs (PostHog), split into common, selective and no-match;
- about 50 recall tasks with known target sessions: error strings, file
  paths, PR numbers, conceptual queries;
- every filter combination in section 6's filter-gap table, for the whale
  and for a small tenant;
- text plus filter;
- the 1-2 character case;
- `get_shared_session_context` windows;
- cross-T0 federated queries (G-T5).

**Relevance.**
- Legacy is ground truth for lexical queries. Report overlap and recall@10
  and @20 per query class.
- For recall tasks: target-session hit rate at k=5 and k=10.
- **Hybrid lane (the gate, K-36):**
  - nDCG@10 of the new keyword-only ranking against **legacy hybrid
    (semantic pools included)**, on the dashboard hybrid query set: about
    100
    queries sampled from dashboard `ranking=hybrid` logs, plus conceptual
    recall tasks.
  - Relevance is graded by pooled judging: the top 10 from both systems,
    labelled blind on a 0-3 scale.
  - Legacy is the comparison, not ground truth.
  - **A real loss** means SLO row 12 fails and the bootstrap 95%
    confidence interval of the nDCG@10 difference lies wholly below −0.02,
    or conceptual recall tasks regress. *(Proposed.)* A real loss holds T0
    and goes to Khaliq as the section 17 follow-up decision: accept the
    loss, or bring embeddings back.
- Hits lost to V1's transcript boundary are reported separately as a
  product cost. Under K-27 the search interface must close that gap.

**Latency and load:** p50/p95/p99 and the error and timeout rate, both cold
(after a restart) and warm, at 1, 8 and 32 concurrent clients, while
production-like ingest runs at the same time.

**Freshness:** from the transcript write on the desktop to the first MCP
hit, measured separately for metadata, short_summary and transcript text.

**Isolation:** planted canary sessions per tenant; zero cross-tenant rows.

**Adapter lanes (K-39).** Each lane runs separately and then merged:
- Postgres recent-window adapter alone;
- history adapter alone;
- the merged interface;
- **fallback:** the history store is stopped, slowed past its budget, and
  made to return errors, while load runs.

**Search contract.** Parity runs reproduce legacy fusion exactly (section
6, "Search serving contract", item 1): RRF k=60, weights 1/1/3/3,
1,000-session pools and the keyword-pool bound. Per-pool freshness targets
are in item 7 of that list.

**SLOs.** These are proposed in the report; Khaliq sets them (K-5). All must
pass.

| # | Lane | SLO |
| --- | --- | --- |
| 1 | V1 metadata/summary search | p95 ≤ legacy p95, **and** ≤ 300 ms DB / ≤ 1 s end to end; p99 ≤ 2 s; errors < 0.1% |
| 2 | V1 lexical overlap on metadata queries | ≥ 95% |
| 3 | V1 agent-prefix gate under RLS | passes (today it is red: 1409× rows read at M) |
| 4 | **Transcript search through the interface** (`search_shared_sessions`; **history store primary (K-47)**, alone and merged with the window) | Same latency SLO as row 1. Recall@10 ≥ 95% against legacy on lexical queries. Every recall task hits at k=10 where legacy hits. *(The recall threshold is proposed here; the report sets only the overlap figure.)* |
| 5 | **New-store `get_shared_session_context`** | The window equals legacy on the snapshot; latency SLO as row 1 |
| 6 | Legacy transcript on the child branch (K-6) | child p95 ≤ 1.25× production p95, and within Cloud's 25 s MCP timeout at p99 |
| 7 | Cross-T0 merged query (G-T5) | No duplicates, correct order, no skipped hits when paging; latency ≤ the slower source + 20% |
| 8 | Freshness | **8a** 48 h window: transcript p95 ≤ 60 s with the history indexer stopped. **8b** history indexer lag (primary store): p95 ≤ 15 min, never above 48 h. Also metadata p95 ≤ 60 s and summary p95 ≤ 5 min. *(Proposed.)* |
| 9 | Isolation | zero cross-tenant rows across V1, the 48 h window, the history store and the merge |
| 10 | Replay of one session (S3) | p95 ≤ legacy p95 for size deciles 1 to 9; decile 10 within 1.5× |
| 11 | Capacity | ≥ 30% CPU headroom on Supabase (including the 48 h window) and the history store; no temp-spill regression; Supavisor connections ≤ 70% of the pool |
| 12 | **Hybrid relevance (G-T8)** | New nDCG@10 ≥ legacy nDCG@10 − 0.02 on the hybrid query set. Conceptual recall tasks hit at k=10 at least as often as legacy. *(Proposed here.)* |
| 13 | **Hybrid latency and degradation (G-T8)** | p95 ≤ legacy hybrid p95, and ≤ 1.5 s end to end; p99 ≤ 3 s. Rate of `degradedPools` responses ≤ legacy. *(Proposed here.)* |
| 14 | **Fallback (G-T1M, K-46)** | With the history store down, slow or erroring: **zero 5xx** for that case; **zero user-visible partial text** in the UI or MCP output; 100% of these responses carry internal `coverage` and increment the partial metric; the alarm fires; p95 ≤ the Postgres-only p95 + 1.5 s; recall inside the window unchanged |
| 15 | **Postgres 48 h window alone (safety net, K-47)** | p95 ≤ 1 s end to end, and ≤ 300 ms DB on the 48 h window (largest org), so the fallback stays usable; recall@10 ≥ 95% against legacy for queries whose targets fall inside 48 h *(proposed)*. The 3.1 s prototype figure was on a larger window, so re-measure. |

Not measured, and not gating unless Khaliq adds them: dollar cost and
scale L (1M sessions). Ranked-search relevance against legacy hybrid is
gating (rows 12 and 13, K-36).

---

## 11. Rollback (decided: forward-only after the flip)

**K-22, Khaliq, 2026-10-31: "Forward-only after flip".** T0 can be aborted
up to the flip. After the flip there is no data rollback to Neon, and no
reverse-sync is built (G-V is dropped).

| Point | Rollback | Data |
| --- | --- | --- |
| Phases 0 to 3 | Unset flags, redeploy, drop empty staging | No user data on the new side |
| T0, steps 2 to 6 | Abort (section 8) | The HoldLog replays to Neon. Nothing was lost or duplicated (I3). |
| After the flip | **No data rollback. Fix forward.** Supabase and S3 hold the only copy of post-T0 writes. Never repoint to Neon. | Neon `sessions` stays fenced and intact until A10, as an audit and export source, not as a rollback target. |
| Phases 5 and 6, per wave | Stop the export, or turn the per-org fallback back on | No data movement |

**What forward-only means for the gates.**
- Every pre-flip gate is now the last chance to stop. A red or amber result
  at section 8 step 1, or at any of steps 2 to 6, means abort, not "flip
  and fix".
- `migration:reverse` (from #28) stays in the repo as self-host and history
  tooling only. It is not part of the production rollback.

**Fix-forward levers after the flip**, each needing its own OK (K-25):
- **Code:** roll the Worker back to an earlier build that is still in phase
  `supabase`. #224's gate is forward-only, so no build can return to `neon`.
- **Pipeline:** replay DLQs (`scripts/pipeline-dlq-replay.mjs`) and run the
  reconcile job to re-index from S3 (#216).
- **Features:** route-level feature flags, for example to disable V1 search
  for one org or turn the child fallback back on.
- **Writes: G-F, the post-flip write freeze. Required before T0.** #224
  allows no `supabase → holding` move, so this needs its own mechanism. The
  minimum design:
  - an authenticated, compare-and-set admin switch, audited like the
    cutover gate;
  - it makes every write route return 503 with `Retry-After`, while the
    replay-safe uploads are either buffered (a reused HoldLog) or refused
    retryably;
  - reads, including transcript search, stay up;
  - pipeline consumers pause, without losing queued messages;
  - unfreezing resumes consumers and replays any buffered writes in order.

  Built and tested in phase 0, proven on a canary route in phase 3 and
  rehearsed in phase 2. Each use needs K-25.


---

## 12. Backup and restore drill (gate before T0, and again before A9)

Supabase can be rebuilt from S3 except for the copy set and the marker, so
**S3 is the system of record**. The drill has to prove both directions:

1. **Rebuild Supabase from S3 (staging).** Truncate the staging index tables
   and replay the reconcile job over the bucket (#216's persisted cursor).
   Assert that P-1 and P-2 parity is restored, and record the duration per
   million sessions.
2. **Restore Supabase from backup (staging).**
   - Use PITR or a daily backup to restore into a new project.
   - Verify the copy set: tokens, tombstones, `machines`.
   - Verify `migration:verify --mode=schema`.
   - Record the RTO and RPO.
   - **Supabase plan tier, PITR add-on and retention are unknown (Q3).**
3. **Restore an S3 object.** Delete a staging object, then restore the
   noncurrent version. Separately, confirm the lifecycle expires noncurrent
   versions after the agreed window. That expiry is the erasure guarantee
   (SEC-08).
4. **KMS.** Disable, then re-enable, a staging workspace key. Assert that
   reads fail closed, and recover. Key deletion (crypto-shred) is tested only
   in staging.
5. **Neon.** Before T0, take a Neon snapshot or a protected branch at the
   marker. The child branch is not a backup: it is deleted at phase 7.

**Exit:** a written report with RTO and RPO for each store, checked in under
`docs/evidence/`.

---

## 13. Observability and SLO alarms

There is no metrics or SLO backend for the hosted pipeline today. The
`aws-checkpoint` notes say "durable observability: metrics/SLO/log backend
absent", for AWS, and nothing is recorded for hosted. **Where these alarms
live is open (Q8):** Cloudflare Workers analytics plus a log sink, or a
hosted backend. Build them before phase 3.

| Signal | Source | Alarm (proposed) |
| --- | --- | --- |
| API 5xx rate per route class | Worker | > baseline + 0.5 pp for 5 min |
| p95 latency, list, search and replay | Worker | > 1.25× the phase 2 benchmark for 10 min |
| Gate phase and HoldLog depth and age | CutoverGate and HoldLog DOs | any entry > `CUTOVER_HOLD_WAIT_MS` outside T0; phase change without an approval record |
| Queue lag and DLQ depth per stage (object-landed, session-indexed, summary-short) | Cloudflare Queues, `pipeline_events` | DLQ > 0 for 15 min; oldest message > 5 min |
| Ingest-to-searchable freshness | `pipeline_events` spans (`docs/pipeline-observability.md`) | p95 > SLO (section 10, SLO row 8) |
| Fallback hit rate per route and per org; breaker trips | Worker | hit rate rising for an exported org; any breaker trip |
| Export coverage and throughput per wave | `migration:export` checkpoint | stalled > 30 min |
| Supabase CPU, connections, temp, replication/PITR health | Supabase metrics | CPU > 70% for 15 min; connections > 70% of the pool |
| S3 4xx/5xx and KMS throttling | CloudWatch (the bucket's account) | any `AccessDenied` from the Worker identity; KMS throttle > 0 |
| History indexer lag (primary store, K-47), DLQ depth and load errors (G-T1) | indexer queue and store | lag p95 > 15 min (ticket); **lag > 6 h (page); > 24 h (critical: approaching the 48 h window edge)**; DLQ > 0 for 15 min |
| **Partial-response rate (K-46)**: `search_partial_total` / searches, by reason and surface; history-adapter timeouts | Worker metrics, from internal `coverage` | partial rate > 1% for 10 min (page); any partial rate above 0.1% sustained for 1 h (ticket) |
| 48 h window writer lag, retention-job failures (G-T1R) | pipeline, Supabase | freshness p95 > 60 s; the retention job did not run in 2 h |
| Cross-T0 merge degraded responses (G-T5) | Worker | any degraded marker for 5 min |
| Write-freeze state (G-F) | Worker | any change without an approval record |
| Shadow-read mismatches | Worker (G-S) | > 0 on frozen sessions |
| Revision feed to Cloud (G-R1) | Cloud queue | producer silent for 10 min while ingest is active |
| Clock offset on the RelayAuth, ingest and courier hosts | NTP | > 1 s |

---

## 14. Security gates (all required before T0 unless noted)

| # | Gate | State on 2026-10-30 | Exit evidence |
| --- | --- | --- | --- |
| S1 | **SEC-01** (cross-org History assertion mint) fixed in hosted RelayAuth | cloud #5886, which bumps `@relayauth/server` to 0.2.35 (#114, #119, #115), **merged into `trunk` at 2026-10-31 05:21Z. It was not on `main` when checked, and train #5887 was open.** "Deployed" is **unverified**. | #5887 (or a later train) merged to `main`. The deployed SHA is confirmed from the deploy run's "Resolve deploy commit" log plus health, not from the run's headSha. Then a negative probe: an org-admin key is refused minting an assertion for another org. That probe is a write attempt against production, so it needs K-23. |
| S2 | SEC-02 (observer SSE) off in hosted | #115 is in 0.2.35. Hosted exposure is unverified. | After S1, `GET /v1/observer/events` returns 404 on hosted |
| S3 | **Hosted search issuer key creation** | #116 creates the first key only at boot (`RELAYAUTH_SESSION_DIRECTORY_BOOTSTRAP_API_KEY`). Hosted RelayAuth is a Worker with no boot step. | G-H: a reviewed, one-shot, Khaliq-run path. Either an admin route that requires the existing bootstrap scope and is idempotent, or a reviewed D1 insert of a hashed key. Plus the signing key as a secret (if it is an SST secret, that is the six-place change in cloud `CLAUDE.md`). Plus the `current`/`next`/`previous` rotation runbook. Supabase trusts **only** this issuer. #116 has no OIDC discovery, and hosted Supabase may need it (#63 unknown: whether third-party tokens can carry a custom Postgres `role`). |
| S4 | **Tenant isolation proof** | PGlite and real-PostgREST suites: #73, 52/52 locally on PostgREST 16.5. **The hosted Supabase PostgREST version is unconfirmed, and test 25 needs a hosted project and has not run.** | #73's suite against staging Supabase (hosted). P-7 cross-org and cross-workspace probes through the Worker's API. A leak scan of logs, traces and summaries for planted markers (#74 pattern). Zero cross-tenant rows. |
| S5 | Worker S3 identity | Not created | Separate IAM identities per write role (raw and manifest; summary), plus read. Least-privilege prefixes like #75's pod roles. No `DeleteObject` except for the reviewed purge path. Stored as Worker secrets. A rotation runbook. CloudTrail data events on the bucket. |
| S6 | S3 bucket hardening | Not created | SEC-08 items: TLS-only deny, a lifecycle rule expiring noncurrent versions within the K-45 48 h purge SLA (for example 1 day, allowing for S3 lifecycle lag), explicit KMS key policy, per-workspace keys (P9) |
| S7 | Supabase RLS and roles | Runtime role has BYPASSRLS (A1) | Only the documented exceptions (`machines`). The search RPC refuses without a policy row (#72). The service-role key is not in the Worker. |
| S8 | Erasure path for the V1 projection | 0002 open item, deferred to sessionspipelinegrok | Recorded decision. A tombstone-driven row delete is tested. The Supabase backup and PITR retention is documented as the erasure tail. |
| S9 | SEC-10 (workspace scoping) and SEC-11 (365-day service tokens) | Open | Decision recorded (K-24). The new read path must not widen either. |
| S11 | History-store isolation and erasure (G-T6), `us-east-1` | Not provisioned | **Both isolation layers (K-44)**: app-layer scoping plus DB row policies on scoped users, with vendor enablement of row policies and user creation requested. Tombstone and reset propagation. **Erasure SLA (K-45):** logical delete immediately, physical purge ≤ 48 h, including time travel. Least-privilege credentials. TLS only; encrypted at rest. |
| S10 | Dependency CVEs on the History Worker | SEC-16: critical `tar`, high `axios` and `follow-redirects` | Bumped. For axios, avoid 1.20 under workerd. |

---

## 15. Approval boundary: every step that needs Khaliq's direct OK

Each item needs Khaliq's OK in his own words, for that exact action, at the
time it happens. **This document is not that approval.** The existing runbook
ids (A#) are shown where they apply.

| K# | Action | Existing id | Reversible? |
| --- | --- | --- | --- |
| K-1 | Each merge of a production-affecting PR (the phase 0 list) | — | revert PR |
| K-2 | Create the production S3 bucket, KMS keys and key policy, and the Worker's IAM identities (S5/S6/P9) | part of A4 | yes, while empty |
| K-3 | Apply the 96Bulls migrations plus #38/#48/#72 to Supabase `sessions`, and seed the search token policy row | A3 | while empty, drop the schema |
| K-4 | #38's logical-session merge-precedence rule, and sum vs deduplicate for usage | — | design |
| K-5 | Set the benchmark thresholds in section 10 | — | — |
| K-6 | Transcript-search behaviour for pre-T0 history. **Decided 2026-10-31: "Serve old sessions from T0 branch"; amended by K-27.** | — | — |
| K-7 | Approve the separate search-issuer direction (#63) | — | — |
| K-8 | Release relayauth with #116 and bump hosted RelayAuth (cloud PR, train) | — | revert or redeploy |
| K-9 | Create the hosted search-issuer API key and signing key (G-H), and configure Supabase to trust the issuer | — | revoke or rotate |
| K-10 | Create the staging Supabase project and staging bucket | — | yes |
| K-11 | Create the rehearsal Neon child branch | — | delete the branch |
| K-12 | Export W0 org data into **staging** Supabase, the staging bucket and the staging search store (production content leaves Neon, into staging), and benchmark against Neon production read-only | — | delete the staging data |
| K-13 | Set the Worker secrets `SUPABASE_DATABASE_URL`, `CUTOVER_ADMIN_SECRET` and `PIPELINE_RAW_S3_*`, and the variables `CUTOVER_GATE_ENABLED` and `SUPABASE_PROJECT_REF`; deploy, phase `neon` | A5 | unset and redeploy |
| K-14 | Deploy cloud #5850 with its flag off (remove `merge-train:hold`) | A6 | yes |
| K-15 | Enable shadow-read for W0 | — | yes |
| K-16 | Pre-copy the copy set and apply the sequence fence on Supabase | A4 | while Supabase takes no traffic |
| K-17 | **T0:** hold, apply the Neon fence, catch up, write the marker, create the child branch, flip, flip Cloud's flag, and enable the pipeline flags. Each sub-step is confirmed live. | A7 | abort until the flip |
| K-18 | Start each export wave W0 to W3 into the **production** bucket | A8 | stop, and delete by prefix |
| K-19 | Remove the fallback and enable V1 search per wave | A9 | turn the fallback back on |
| K-20 | Remove the T0 child branch. Allowed only once every pre-T0 session is loaded into the search store, or with Khaliq's acceptance of losing pre-T0 transcript search. | — | **no** |
| K-21 | Drop Neon `sessions` after the final snapshot | A10 | **no** |
| K-22 | The reverse-data decision. **Decided 2026-10-31: R2, "Forward-only after flip". No reverse-sync is built.** | — | — |
| K-23 | The negative security probe against hosted RelayAuth production (S1) | — | — |
| K-24 | The SEC-10 workspace-boundary decision | — | — |
| K-25 | Each post-flip fix-forward action (Worker rollback within phase `supabase`, flags; write freeze is K-31) | — | depends on the action |
| K-26 | Any Neon load relief before T0: compute, `work_mem`, vacuum | — | yes |
| K-27 | Transcript search before T0. **Decided 2026-10-31: "Transcript search before T0".** No T0 until new sessions are transcript-searchable on ClickHouse or ObsessionDB. | — | — |
| K-28 | Store choice. **Approved 2026-10-31 as "intended, conditional":** ObsessionDB is confirmed only after G-T0E. K-40 adds OpenSearch as a candidate on the same gate. Merging the 0001 amendment is still K-1. | — | — |
| K-29 | Provision the **chosen** production history store in `us-east-1` and staging, and their credentials. **Only after G-T0E and Khaliq's choice. The K-37 and K-40 trial approvals do not cover this.** | — | yes, while empty |
| K-30 | Enable the G-T1 feed into the production store (dark, canaries only, before T0) | — | disable the feed |
| K-31 | Each use of the G-F write freeze, including the phase 3 canary proof | — | unfreeze |
| K-32 | Resize the Supabase `sessions` disk from 2 GB. **Re-derived for K-47: propose 32 GB now, and plan for 64 GB.** That is metadata ~6 GB + the 48 h window ~8-10 GB (upper estimate) + 50% headroom ≈ 21-24 GB, plus growth. **Excludes Q25's ~90 GB of bookkeeping**: if that moves, re-derive first (≈ 128-160 GB). | — | grow only |
| K-33 | Transcript-search region. **Decided 2026-10-31: AWS `us-east-1`.** | — | — |
| K-34 | Ranked dashboard search in the T0 gate. **Decided 2026-10-31: "Include it in the T0 gate", superseded as written by K-36.** | — | — |
| K-36 | **Decided 2026-10-31: "No embeddings pre-T0"** (helm-chart-chief: directly approved). G-T8 is keyword-ranked; nDCG@10 against legacy hybrid is the gate. | — | — |
| K-37 | Synthetic trials. **Approved 2026-10-31:** ClickHouse Cloud with a $50 hard cap, and the ObsessionDB free dev service (eu-central, functional only). Synthetic data only; not production provisioning. | — | delete the trial services |
| K-38 | **Approved 2026-10-31:** counts-only, read-only shape extraction from production. No corpus movement. | — | n/a (read-only) |
| K-39 | **Decided 2026-10-31: "ObsessionDB off the hot path".** Two adapters, async indexer, partial fallback (made silent by K-46). | — | — |
| K-40 | **Decided 2026-10-31:** Amazon OpenSearch Service is a third history-store candidate. Synthetic-only trial approved in 500268966557 / `us-west-2`, about $50 cap. Not production provisioning. | — | delete the trial domain |
| K-42 | **Decided 2026-10-31:** a 14-day window (**superseded by K-47: 48 h**), complete event text, and a 1.5 s history-store budget. K-38 measured ~16.5 GB of text, 52-56 GB with indexes and ~70 GB at the peak rate. | — | the window can shrink or grow via retention and backfill from S3 |
| K-44 | **Decided 2026-10-31:** app-layer isolation plus a DB row-policy backstop; the gate needs both. Asking the vendor to enable row policies and user creation is a vendor contact, so Khaliq or the account owner sends it. | — | — |
| K-45 | **Decided 2026-10-31:** logical delete immediately, physical purge ≤ 48 h, including ObsessionDB's 24 h time travel | — | — |
| K-46 | **Decided 2026-10-31:** silent partial results; internal coverage metadata, logs, metrics and alarm only | — | — |
| K-47 | **Decided 2026-10-31:** the history store is primary for all transcript search; Postgres keeps a 48 h complete-text safety-net window plus metadata/summary search | — | — |
| K-43 | Apply the 48 h window migration and RLS to Supabase, and enable the writer stage and retention job | — | drop the table, disable the stage |

Read-only work needs no approval beyond normal access: catalog queries,
`neonctl` reads, `scripts/sb` reads, benchmarks on staging, and drafting PRs.

---

## 16. Open questions and unknowns

- **Q1.** Where the hosted pipeline stages run: in the relayhistory-cloud
  Worker on Cloudflare Queues (inferred), or elsewhere? Where Referee and
  `summary-long` run in hosted, if anywhere. What #226's "scoped 96Bulls
  ingress" is concretely.
- **Q2.** The production S3 bucket's account (`sessions-prod`
  `941433935742`?), region and name.
- **Q3.** Supabase `sessions` region, PITR add-on and backup retention.
  Known: Pro plan, Large compute, 2 GB disk, empty.
- **Q4.** The hosted Supabase PostgREST version, and whether third-party JWTs
  can set a custom role (#63, #73).
- **Q5.** What reads `uploaded_support_identities` (37 GB) after T0? Now part of Q25.
- **Q6.** Analysis, briefs and memory: export, re-derive (model cost), or
  drop?
- **Q7.** Answered by K-38: 282 orgs and 289 workspaces, heavily concentrated (section 1).
- **Q8.** The hosted metrics, SLO and log backend for the alarms in section
  13.
- **Q9.** Partly informed by K-38 (~399 GB of transcript-bearing tables). Still open: the export's total zstd size and PUT count, and the KMS request
  cost (O5).
- **Q10.** The Cloud revision-feed replacement (G-R1): owner, and the event
  contract.
- **Q11.** Answered by K-38: `sessions` is 495 GB of a 506 GB database
  (catalog). The earlier figures are superseded.
- **Q12.** Whether child-branch transcript search can meet SLO row 6
  within Neon plan limits (16 CU maximum).
- **Q13.** Owners for the unowned items G-R1, G-P, G-S, G-H, G-T and G-F.
- **Q14.** Mostly closed: K-27 removes the post-T0 transcript limit. Before T0, dashboard ranking is keyword-only (K-36), and the UI labels it.
- **Q15.** Resolved into gate G-F (section 11). It is a required pre-T0
  item; the owner is open (Q13).
- **Q16.** Region decided (K-33: `us-east-1`). Still open: ObsessionDB
  sizing and cost for post-T0 transcripts and exported history. Embeddings
  are covered in section 17.
- **Q17.** Resolved by K-34 and K-36: keyword-ranked dashboard search is in the T0 gate (G-T8).
- **Q19, Q20:** moved to section 17.
- **Q27.** The backup tails against the 48 h purge SLA (K-45):
  - Supabase daily backups and PITR typically retain at least 7 days;
  - Neon's child branch and snapshot are kept until A10;
  - S3 noncurrent versions are covered by a ≤ 1-day lifecycle rule.

  Do the backups fall under the 48 h SLA, or is the SLA for live stores
  with documented backup expiry? If they fall under it, Supabase retention
  must be ≤ 48 h, or erased rows must be excluded at restore.
- **Q28.** Keep or drop the UI's "Keyword search" mode label (the semantic
  pools are absent before T0). It is not a degradation label, but K-46's
  intent may cover it.
- **Q25.** The ~90 GB of acceptance and identity bookkeeping
  (`uploaded_support_identities`, `delivery_projection_acceptance`,
  `delivery_catalog_acceptance`, `delivery_receipts`): **move it to
  Supabase, or drop or re-derive it?** It holds no transcript content.
  Moving it would make it the largest item on Supabase disk (≈ 90 GB
  against ≈ 16 GB for metadata plus the 48 h window), so it is excluded
  from the K-32 recommendation.
- **Q26.** Does the `delivery_records` export carry every `session_markers`
  row's content (25 GB, transcript-bearing), or do the markers need their
  own export?
- **Q23.** Closed (K-42): complete event text.
- **Q24.** Closed (K-42): 1.5 s.
- **Q22.** The brief pools' text on the new stack: the pipeline's short and
  long summaries in ObsessionDB as plaintext, with erasure through
  tombstones? Legacy briefs are LLM output stored in Neon.
- **Q21.** ObsessionDB hosting in `us-east-1`: vendor-managed or
  self-operated, and which AWS account. 0001 limits our own AWS to S3
  unless the amendment says otherwise.
- **Q18.** The pipeline normalized-object (file-normalize / E4) stage in
  hosted production. G-T1 loads from it, and it is not yet enabled in
  hosted.

---

## 17. Post-T0 follow-up: semantic search (embeddings)

Not part of T0 (K-36, "No embeddings pre-T0"). This section opens only if:
- the section 10 hybrid lane shows a real loss, which holds T0 until
  Khaliq decides; or
- Khaliq asks for semantic search after T0.

Either way, adding embeddings is a new decision with its own approvals.

### Embedding and model versioning, and the re-embedding policy (moved here from section 6)
- **Row metadata.** Every vector row stores:
  - `embedding_model` and `embedding_dim`;
  - an `embedding_generation` id;
  - the chunking version and `scrub_ruleset_version`;
  - `input_sha256`.

  Queries filter on one active generation, and the query text is embedded
  with that generation's model.
- **Initial generation (proposed):** legacy's `text-embedding-3-small` at
  1,536 dimensions.
  - One query embedding can then serve both the T0 child branch and
    ObsessionDB in a cross-T0 query.
  - Fusion is rank-based, so raw scores are never compared across sources.
- **What triggers a re-embed:**
  - a new session revision: only the changed chunks, by `input_sha256`;
  - a chunking-version change;
  - a scrub-ruleset bump that changes the text;
  - a model change.
- **A model change is a new generation:**
  1. Write the new embeddings alongside the current ones.
  2. Re-embed in the background, newest first, within the K-35 budget.
  3. Switch the query generation only when coverage is at least 99.9% of
     sessions active in the last 90 days and SLO rows 12 and 13 pass again.
  4. Keep the old generation for 7 days so the switch can be reversed, then
     drop it.
- **Deletion:** tombstones and resets delete vectors in every generation.
- **Transcripts only, in this decision.** The child branch's pgvector pools
  are never re-embedded. Pre-T0 sessions get vectors when they are exported
  and loaded.

### Approval, security and questions moved here

| K# | Action | Existing id | Reversible? |
| --- | --- | --- | --- |
| K-35 | The embedding model and provider, the backfill budget, and enabling the embedding stage (production model calls on transcript content) | disable the stage |

| # | Gate | State | Exit evidence |
| --- | --- | --- | --- |
| S12 | Embedding provider (K-35) | Not decided | Scrubbed text only goes to the model provider. A data-processing record for that provider. Per-tenant opt-out honoured, as in today's `MODEL_CALLS_ENABLED` behaviour. |

- **Q19.** Whether ObsessionDB supports vector similarity search with the
  latency and isolation a semantic lane would need, verified on
  `relay-transcript-proof`. If it does not, Khaliq has to decide where the
  vectors live. 0002 forbids putting them in Supabase.
- **Q20.** The embedding model:
  - Keep legacy's model, so that pre-T0 child-branch and post-T0 vector
    scores stay comparable, or change it, which needs rank-only fusion?
  - Where does the embedding stage run (inferred: the relayhistory-cloud
    Worker or queue)?

---

## 18. Sources read for this plan

- 96BullSessions `origin/main` `890e9a0`:
  - `docs/decisions/0001-…`, `0002-…`;
  - `docs/relayhistory-{supabase-migration,pipeline-migration,switch-first-dag,t0-runbook,t0-write-hold,export-design}.md`;
  - `docs/session-search-{projection,index-contract}.md`;
  - `docs/aws-stage.md`, `docs/supabase-access.md`,
    `docs/pipeline-storage-adapter.md`;
  - `docs/evidence/2026-10-30-neon-capacity/`;
  - `package.json` scripts.
- Uncommitted files on linuyc: `docs/relayhistory-cutover.md` and
  `docs/relayhistory-migration-status.md` (both 2026-10-29), and the local
  `docs/aws-stage.md` diff (Aurora wording).
- relayhistory-cloud #236 (draft, head `380b756e`): transcript-search design and prototype (K-39 shape).
- relayhistory-cloud `origin/main` `45d53b1`: `db/client.ts`,
  `lib/pipeline/*`, `docs/pipeline-summary-short.md`. PRs #211, #212, #213,
  #214, #216, #222, #223, #224, #225, #226, #227, #228, #231 and #233.
- 96BullSessions PRs #28, #38, #40, #48, #59, #63, #72, #73, #74, #75 and
  #79. relayauth #110 and #116. cloud #5850, #5886 and #5887.
- `~/selfhost-proof/notes/aws-checkpoint-2026-10-30.md`,
  `security-sweep-2026-10-30.md` and
  `search-recall-comparison-2026-10-31.md` (sections 6 and 10 copy its
  tables, benchmark design and proposed SLOs).
- Coordinator relay, 2026-10-31:
  - Khaliq's decisions K-6, K-22, K-27, K-28 (restated), K-33, K-34, K-36, K-37, K-38, K-39, K-40 and K-42;
  - the K-38 shape extraction results, `~/selfhost-proof/notes/prod-shape-2026-10-31.md`;
  - storage facts: `sessions` 494 GB, `convergence_events` 210 GB, no
    production S3 bucket, R2 `relayhistory-content` empty, Supabase Pro /
    Large / 2 GB disk / empty.
- Cloud memory note `relayhistory-revision-publisher-convoy.md`
  (2026-10-24).
