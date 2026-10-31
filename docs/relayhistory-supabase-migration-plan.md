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

- K-28 chose ObsessionDB as the store.
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
| 2026-10-31 | K-28 | "Yes, add ObsessionDB" | Khaliq, lead session, relayed by the coordinator | **The transcript search store is ObsessionDB.** Decision 0001 is amended: production = Supabase + S3 + ObsessionDB. The amendment text is drafted in this PR, in `docs/decisions/0001-production-data-plane.md`. ClickHouse is no longer a production candidate. |
| 2026-10-31 | K-33 | Region: AWS `us-east-1` | Khaliq, lead session, relayed by the coordinator | The production transcript-search service runs in AWS `us-east-1`, next to Neon prod and Supabase `sessions`. The proof service is `relay-transcript-proof` in `us-west-2`; it is evidence only and holds no production data. |
| 2026-10-31 | K-34 | "Include it in the T0 gate" | Khaliq, lead session, relayed by the coordinator | **Superseded as written by K-36.** The dashboard's semantic/hybrid search (keyword + vector, ranked) must also be served on the new stack for post-T0 sessions before T0. This adds G-T8 and a hybrid benchmark lane (nDCG@10 against legacy on the hybrid query set). |
| 2026-10-31 | K-36 | "No embeddings pre-T0" | Khaliq, lead session (final ruling), relayed by the coordinator | **Supersedes K-34 as written.** G-T8 becomes keyword-ranked dashboard search on ObsessionDB full-text over complete transcripts, with **no vector pools before T0**. The nDCG@10 benchmark against legacy hybrid stays as the gate. If it shows a real loss, embeddings come back as a follow-up decision (section 17). Embedding versioning, K-35, Q19 and Q20 move to section 17. |
| 2026-10-31 | K-22 | "Forward-only after flip" | Khaliq, lead session, relayed by the coordinator | Option R2 (section 11). T0 can be aborted up to the flip. After the flip there is no data rollback to Neon, and no reverse-sync (G-V) is built. |

**Relay status.** These rulings were relayed by the coordinator from
Khaliq's lead session. helm-chart-chief records relayed rulings as
**PROPOSED** until Khaliq confirms them in its own session. Treat the rows
above as proposed in that sense until it does.

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
2. **Transcript search comes first (K-27).** T0 waits until the
   ObsessionDB feed (AWS `us-east-1`) is live and new sessions are
   transcript-searchable on it, with parity to legacy MCP search. The
   dashboard's ranked search must also work on it, keyword-ranked with no
   embeddings before T0 (K-36) (gate G-T, section 6). Pre-T0 history is served from the frozen Neon T0 child branch
   (K-6). Results across T0 are merged by a written spec.
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
   0. **G-T: transcript search for new sessions is live on ObsessionDB**
      (`us-east-1`). That covers:
      - the ingest feed;
      - `search_shared_sessions` excerpts;
      - the `get_shared_session_context` in-session window;
      - recall parity;
      - **keyword-ranked dashboard search** on ObsessionDB full-text over
        complete transcripts, gated by nDCG@10 against legacy hybrid
        (K-34 as narrowed by K-36: no embeddings before T0);
      - the cross-T0 merge, ordering and dedup spec (section 6).

      This is the longest pole. The 0001 amendment is drafted here but not
      yet merged. No ObsessionDB benchmark has run.
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
| RelayHistory has no database of its own. It writes the `sessions` schema in the same `neondb` as Cloud's `public` schema. | Cloud memory note `relayhistory-revision-publisher-convoy.md`; runbook step 9 ("Cloud's `public` schema is untouched") |
| Neon is under stress: 91.39% cache hit, 1.35 TB temp spill, 56 deadlocks, 8.9M dead tuples in `delivery_projection_acceptance`. Compute is pinned at 16/16 CU, the plan maximum. | `load-results.json`, capacity README (2026-10-30) |
| There was a lock convoy on `session_revision_events` on 2026-10-23. It was fixed by relayhistory-cloud #164 (migration 0049, SKIP LOCKED). Still open: advisory-lock waits on `accept_delivery_batch`, and a backlog of about 1.6M messages for Cloud's `project-summary-worker`. | Cloud memory note (2026-10-24) |
| Neon outbound logical replication is disabled. Enabling it is irreversible and restarts the endpoints. | Uncommitted cutover doc |
| **Production transcripts are Neon rows.** The `sessions` schema is **494 GB**, of which `convergence_events` is **210 GB**. | Coordinator, 2026-10-31. No checked-in capture; unit (GB vs GiB) to confirm. |
| **No production S3 bucket exists.** The existing R2 bucket `relayhistory-content` is **empty**, so no session content is in object storage today. | Coordinator, 2026-10-31 |
| Pipeline code exists in relayhistory-cloud but is inert: storage adapter #211/#216 (S3 API supported); pointer index #212; summary-short stage (`docs/pipeline-summary-short.md`); single-use migration assertions #228 (merged 2026-10-31 UTC). | PR states, read 2026-10-30 |
| Supabase project `sessions` (`pyamkrhoyszgzirqqffq`): **Pro plan, Large compute, but a 2 GB disk, and empty.** The disk must be sized before A3 and any load (K-32). Roles `relayhistory_owner` and `relayhistory_runtime` (BYPASSRLS) exist, as does the Neon role `relayhistory_readonly`. | `docs/relayhistory-pipeline-migration.md` "Done" (A1, A2) |
| Supabase `sessions` region: **AWS `us-east-1`**, next to Neon prod. | Khaliq, 2026-10-31 (K-33 wording). Not yet confirmed read-only: check with `scripts/sb projects list`. |
| Production S3 bucket account: not recorded. `sessions-prod` (`941433935742`) is the existing workload account named in the cutover doc. | Open question Q2 |

### Data volume: which figure to use

| Figure | Covers | Source | Status |
| --- | --- | --- | --- |
| **494 GB `sessions`, 210 GB `convergence_events`** | `sessions` schema | Coordinator, 2026-10-31 | **Current; use this.** No checked-in capture, unit unconfirmed. |
| 484 GB `sessions`: `convergence_events` 205 GB, `delivery_records` 161 GB, `uploaded_support_identities` 37 GB | `sessions` schema | `docs/relayhistory-pipeline-migration.md` (2026-10-30) | Older. It shows roughly 10 GB of growth in about a day. |
| 485.6 GiB (521,364,119,552 logical bytes; 521,364,119,552 / 2^30 = 485.56) | the whole Neon branch, `public` + `sessions` | uncommitted `docs/relayhistory-cutover.md` (2026-10-29), Neon API | **Stale.** No artifact. It is below the current `sessions` figure if both are binary units, so it predates the growth. |

Before any capacity decision, re-measure read-only and check in the output
under `docs/evidence/`:
- `neonctl branches get br-damp-mud-apbr6ifp --project-id divine-snow-84812652 -o json` (`logical_size`);
- `SELECT n.nspname, sum(pg_total_relation_size(c.oid)) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relkind IN ('r','m') GROUP BY 1`;
- per-table sizes for `sessions`.

Under option D none of this volume is copied into Supabase: most is
re-derived or exported to S3 (section 4). The search store (G-T), however,
must hold transcript text for every post-T0 session, and eventually for
exported history. No embeddings before T0 (K-36). Size that
store from the same measurement (Q16).

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
| Transcript search, post-T0 sessions | **ObsessionDB (K-28) in AWS `us-east-1` (K-33)**, loaded from the pipeline's normalized S3 objects. It serves lexical transcript search and keyword-ranked dashboard search over complete transcripts (K-34 as narrowed by K-36: no vector pools before T0), and must be live before T0 (K-27, G-T). 0001 is amended (drafted in this PR) to add it to the production data plane. Proof service: `relay-transcript-proof`, `us-west-2`. Unknowns: hosting model and account (Q21). | relayhistory-cloud #233 (draft; no benchmark has run) |
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

Sizes come from `docs/relayhistory-pipeline-migration.md` (2026-10-30). The
current totals are larger: `sessions` 494 GB, `convergence_events` 210 GB
(section 1). Row counts are live tuples from `load-results.json`.

| Data | Size, rows | Moves? | Where and how |
| --- | --- | --- | --- |
| `delivery_records` (the client's own records, with revisions: session, session_event, tool_call, file_edit, …) | 161 GB, 54.0M | **Must move.** It is the only source of session content. | Exported from the T0 child branch to S3 as scrubbed raw chunks plus a manifest per session (`docs/relayhistory-export-design.md`, #40, E1 to E4). Not copied into Supabase. |
| `convergence_events` | 210 GB (205 GB on 2026-10-30), 27.7M | **Re-derived, not moved.** 99.8% of sampled rows reference a `delivery_record_id`. | Rebuilt by the pipeline's normalizer from the S3 raw stream. The derived replay comes from S3 objects. |
| `session_rollups`, `session_catalog`, `session_markers` (25.0M), `session_relationships`, `session_commit_links`, `session_catalog_agents`, `session_timing_cache`, projection/acceptance tables (`delivery_projection_acceptance` 50.5M, `delivery_catalog_acceptance` 25.6M) | in the 494 GB | **Re-derived.** | `session_index` and `session_directory` are rebuilt from manifests by session-indexed and the directory projector. Timing comes from manifest activity times. |
| `uploaded_support_identities` | 37 GB, 54.3M | **Unknown (Q5).** It is the third-largest table, and no doc says what reads it after T0. | Decide: export, re-derive, or leave on the child branch until A10. |
| Copy set: `auth_sessions`, `deleted_upload_sessions`, `session_resets`, `upload_cleanup_origins`, `upload_cleanup_tombstones`, `machines`, `probe_connections`, `project_aliases`, `github_org_links`, `delivery_origins`, `delivery_receipts` | small (measure; runbook step 3) | **Must move before the flip.** These are write-path state: tokens, tombstones, cursors. | `migration:copy --mode=mirror` pre-copied (A4), then caught up inside the hold (runbook steps 4 and 10). |
| Analysis, briefs, memory state | unknown | **Decide (Q6).** | Either export as derived objects, or re-run after the export behind `MODEL_CALLS_ENABLED` and a budget. |
| Summaries | n/a | **Re-derived.** | summary-short runs from S3 after the export. That costs model calls; budget it per wave. |
| Cloud `public` schema | ~1.6 GiB (inferred) | **Does not move.** | Stays on Neon. |

What this means for capacity: Supabase holds pointers and metadata, roughly
one row per session plus memberships. That is small, but the project's
**2 GB disk is too small** even for that plus the copy set, so it must be
resized first (K-32). Separately, **the export's
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
  K-28 then chose ObsessionDB.
- **K-6** (2026-10-31, "Serve old sessions from T0 branch"): pre-T0 history
  is served by legacy transcript search on the frozen Neon T0 child branch.

So after T0:
- post-T0 sessions are searched on the new store;
- pre-T0 sessions are searched on the child branch, until each has been
  exported and loaded into the new store;
- V1 (Supabase) covers metadata and short_summary for everything.

Earlier options T1 (fail closed), T2 alone (post-T0 sessions metadata-only) and T4
are rejected. T3 (delay T0 until V2) is in effect for post-T0 content.

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
| G-T0 | **Store: ObsessionDB (K-28), AWS `us-east-1` (K-33).** Still to do: merge the 0001 amendment, and complete the 0002 checks on ObsessionDB (text, token and n-gram indexes; S3 load; per-org isolation; tombstone propagation). Hosting model and account to settle (Q21). | 0001 amendment merged; #233 updated to ObsessionDB and resolved; checks evidenced on `relay-transcript-proof` |
| G-T1 | **Ingest feed.** Pipeline normalized objects (the file-normalize / E4 transform stage) load into the store per session revision. It is idempotent by content hash, handles tombstones and resets, and has its own queue, DLQ and replay. It is enabled in production before T0 so the first post-T0 write is indexed (K-30). | Feed tests. Replay from S3 rebuilds the store. Freshness within the section 10 SLO on the live-protocol writer. |
| G-T2 | **`search_shared_sessions` parity.** Same request contract: query length 1-500, filters, cursor, limit ≤20. Same response: event excerpts (`max_content`), `citation_url`, newest-first order. Same workspace confirmation that Cloud enforces. | The Cloud MCP contract tests pass unchanged against the new backend |
| G-T3 | **`get_shared_session_context` in-session window.** Find the latest match inside one session, then return the event window around it, for post-T0 sessions (store plus S3 read path). | Contract tests; window equality against legacy on the snapshot |
| G-T4 | **Recall parity** against legacy on the frozen snapshot | The section 10 recall thresholds |
| G-T5 | **Cross-T0 merge, ordering and dedup spec**, written, reviewed and implemented (below) | Spec doc plus tests for straddling sessions, duplicate hits, paging across sources and one-source failure |
| G-T6 | **Isolation and erasure.** Per-org isolation proven with planted canaries. Tombstone and reset propagation. Erasure semantics recorded (0002 open item; per-workspace encryption if the store supports it). | Zero cross-tenant rows; tombstone tests |
| G-T7 | **Recall consumers.** The memory-pack lexical and `files_touched` pools read ObsessionDB for post-T0 sessions. | Consumer tests |
| G-T8 | **Dashboard ranked search, keyword only (K-34 as narrowed by K-36).** `sessions?q=&ranking=hybrid` is served on the new stack for post-T0 sessions by ObsessionDB full-text over **complete transcripts**, not only the 4 KB prefix. It fuses the remaining legacy keyword pools (`event_keyword`, `brief_keyword`) per the serving contract, with snippets ≤ 320 chars. There are **no vector pools before T0**. The response says which pools were offered, so the missing semantic pools are not reported as a failure. | Hybrid benchmark lane (section 10, SLO rows 12 and 13) **against legacy hybrid, semantic pools included**; the dashboard read-API contract tests pass |

### Cross-T0 merge, ordering and dedup spec (G-T5, minimum contents)

1. **Source selection, per session.**
   - A session whose events are all after T0 is served from the new store
     only.
   - A pre-T0 session not yet loaded is served from the child branch only.
   - A straddling session is served from both: events at or before the
     marker timestamp/LSN come from the child, later events from the store.
   - Once a session's export is loaded into the store (coverage marker set),
     only the store serves it.
2. **Dedup key:** `(org, workspace, session, record id, revision)`. On a
   duplicate, keep the highest revision; if revisions are equal, prefer the
   store. A post-T0 tombstone suppresses the hit from both sources.
3. **Order:** event time descending, then a stable tiebreak (source record
   id). This matches legacy's newest-first, unranked order.
4. **Paging:** a composite opaque cursor carrying each source's position.
   The limit applies after the merge, so a page never exceeds the limit or
   skips hits.
5. **Excerpts:** identical shape from both sources (4 KB content prefix
   rule, `max_content`, `citation_url`).
6. **Partial failure:** if one source fails or trips its breaker, return the
   other source's hits with an explicit degraded marker. Never silently
   return partial results as complete.
7. **Budgets:** per-source timeouts that fit inside Cloud's 25 s MCP
   timeout, and the child branch's breaker (section 8).
8. **Ranked dashboard queries (K-36):** a ranked result, not
   time-ordered.
   - Post-T0 sessions come from ObsessionDB `event_keyword` and
     `brief_keyword`.
   - Pre-T0 sessions come from the child branch's legacy `event_keyword`
     and `brief_keyword` only. **For a merged query, the child's semantic
     pools are not used**, so that both sides rank with the same pool set
     and pre-T0 sessions are not boosted by vector matches that post-T0
     sessions cannot get. *(Proposed.)*
   - A query whose whole window is before T0 may use full legacy hybrid on
     the child. *(Proposed; Khaliq to confirm.)*
   - The two lists are merged by reciprocal-rank fusion (k=60) with the
     legacy keyword weights (event 1, brief 3), deduped by the rule-2 key
     before the cut.

---

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
  - the excerpt is the event's content, limited to the 4 KB prefix that
    legacy searches;
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

**4. Degraded-pool behaviour.**

| Failure | Dashboard hybrid | MCP `search_shared_sessions` | MCP `get_shared_session_context` | V1 metadata |
| --- | --- | --- | --- | --- |
| Semantic pools (none before T0, K-36) | Not offered. The response lists `pools_offered: ["event_keyword","brief_keyword"]`, and the semantic pools are **not** reported in `degradedPools`, so no permanent degraded banner appears. The UI labels the mode "Keyword search". | n/a | n/a | unaffected |
| ObsessionDB is down, for a query that includes post-T0 time | Serve the child-branch pools for pre-T0 sessions. `degradedPools` lists every post-T0 pool. The UI shows "Recent sessions unavailable." | Return child results with `complete: false` and `degraded_sources: ["post_t0_transcripts"]`, and add a line to the tool's text output saying the results are incomplete | **Fail closed** for a post-T0 session: retryable `503 transcript_search_unavailable` | unaffected |
| The child branch is down or its breaker is open | The mirror image: ObsessionDB pools only, with pre-T0 marked degraded | Mirror image: `degraded_sources: ["pre_t0_transcripts"]` | Fail closed for a pre-T0 session | unaffected |
| Every source for the requested window is down | **Fail closed:** retryable 503, never an empty 200 | Fail closed: retryable 503 | Fail closed | unaffected |
| The total budget is exceeded | `503 search_timeout` (legacy) | same | same | — |

The new response fields (`complete`, `degraded_sources`) need a Cloud MCP
schema change in P4. Nothing may return partial results without a label.

**5. Cross-T0 federation.** See the merge, ordering and dedup spec above
(G-T5, items 1 to 8):
- which source serves each session;
- dedup;
- newest-first order for MCP, and RRF for hybrid;
- the composite cursor;
- partial failure (also item 4 above);
- budgets.

**6. Relevance gates.** These are section 10 rows 2, 4, 5, 12 and 13:
- recall@10 and recall@20 against legacy per query class: at least 95% on
  lexical queries;
- recall tasks hit at k=5 and k=10 wherever legacy hits;
- nDCG@10 of keyword-only new-stack ranking at least legacy **hybrid**
  nDCG@10 − 0.02, with pooled blind
  judging;
- V1 lexical overlap at least 95%.

**7. Freshness per pool.** Measured from the transcript write on the desktop
to the first hit; *proposed*.

| Pool or surface | p95 | p99 |
| --- | --- | --- |
| V1 metadata (list and filters) | 60 s | 120 s |
| V1 short_summary | 5 min | 10 min |
| MCP transcript keyword (`event_keyword`, ObsessionDB) | 60 s | 120 s |
| `brief_keyword` (needs a summary) | 5 min | 10 min |

Sessions not yet in a pool still appear through the faster pools, so
freshness is per pool, not per session.

**8. Rollback for search.**
- **Before T0:** any red G-T result holds T0. Every search component is
  dark, so it can be switched off, or ObsessionDB rebuilt from S3, without
  user impact.
- **After T0:** forward-only (K-22, section 11). The levers are:
  - rebuild ObsessionDB by replaying the S3 normalized objects;
  - degrade to keyword-only per item 4;
  - the G-F write freeze.

  There is never a return to Neon for post-T0 sessions.

**9. Regional data-flow boundaries.**

| Store or service | Region | Holds transcript content? | Status |
| --- | --- | --- | --- |
| Neon production and the T0 child branch | AWS `us-east-1` | yes (rows) | exists |
| Supabase `sessions` | AWS `us-east-1` (K-33; verify) | metadata, bounded short_summary; no transcripts or embeddings (0002) | exists, empty |
| Production S3 bucket | **must be `us-east-1`** (required by this boundary) | yes (canonical) | not created (K-2) |
| ObsessionDB production | AWS `us-east-1` (K-33) | yes (full-text projection; no embeddings before T0) | not provisioned (K-29) |
| RelayHistory Worker, Queues | Cloudflare edge (global) | in transit only; no persistent copy | exists |
| Summary model provider (embeddings only after section 17) | provider's region (unknown) | in transit to the provider; retention per its terms | summary-short enablement (K-13); embeddings K-35 |
| `relay-transcript-proof` | AWS `us-west-2` | **synthetic data only** | proof |
| `aws-proof` EKS stack | AWS `us-west-2` (500268966557) | synthetic orgs (`org_aws_proof_*`) only | proof |

**Rules:**
- No production transcript content is copied out of `us-east-1`, except in
  transit through Cloudflare or the model provider.
- The `us-west-2` proof services take only synthetic data. Exporting or
  loading a real org into them is forbidden.
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

**The number of orgs and their size distribution is unknown (Q7).** Measure
it read-only on the rehearsal branch: count per `org_id` from
`delivery_records`, no content.

### Phase 0: code prerequisites (no live action except merges)

Each item's exit gate is its own two-pass review plus CI. Every merge is K-1.

- relayhistory-cloud:
  - #223 (runner over TCP);
  - #224 (gate plus HoldLog plus the data-loss fix; migration renumbered off
    0066 and 0067);
  - #225 (TCP seam);
  - #227 then #231 (pointer index activation, flag-gated);
  - #226 (G3: terminal reads; it must route transcript search to the child branch for pre-T0 sessions and to the new store for post-T0 sessions, per the G-T5 spec, instead of failing closed);
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

**Also in phase 1 (K-28, K-29, K-32):** resize the Supabase disk from 2 GB.
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
   - **route post-T0 transcript search to the new store, per the G-T5
     spec**;
   - flip cloud #5850's flag.

   HoldLogs replay in order into the pipeline, and crons and consumers
   resume.
8. **Verify.** Run canaries for: login, ingest, delivery batch, turns,
   session list, V1 search, replay of a post-T0 session from S3, replay of a
   pre-T0 session through the child, transcript search through the child
   (K-6), transcript search on a post-T0 session in the new store (G-T),
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
1. Transcript lanes for the new store are added (K-27).
2. **V1 is measured on staging Supabase, which is production-shaped,** not
   on EKS/CNPG. EKS numbers are self-host evidence only (0001).

**What exists today** (the report's §3):
- **Legacy:** single-query EXPLAINs on Neon branches cut from production
  (#146, #152). They show 4-140 s failures fixed to sub-second warm, a
  6.8-11 s cold path and incident 503s. There are no percentiles or
  concurrency figures.
- **V1:** synthetic data on GitHub-runner Postgres only (#59). Its own
  agent-prefix gate is red at 1409× read amplification.
- **The new transcript store:** none ("No benchmark has run", #233).
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
  product cost. Under K-27 the new store must close that gap.

**Latency and load:** p50/p95/p99 and the error and timeout rate, both cold
(after a restart) and warm, at 1, 8 and 32 concurrent clients, while
production-like ingest runs at the same time.

**Freshness:** from the transcript write on the desktop to the first MCP
hit, measured separately for metadata, short_summary and transcript text.

**Isolation:** planted canary sessions per tenant; zero cross-tenant rows.

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
| 4 | **New-store transcript search** (`search_shared_sessions` on post-T0 sessions) | Same latency SLO as row 1. Recall@10 ≥ 95% against legacy on lexical queries. Every recall task hits at k=10 where legacy hits. *(The recall threshold is proposed here; the report sets only the overlap figure.)* |
| 5 | **New-store `get_shared_session_context`** | The window equals legacy on the snapshot; latency SLO as row 1 |
| 6 | Legacy transcript on the child branch (K-6) | child p95 ≤ 1.25× production p95, and within Cloud's 25 s MCP timeout at p99 |
| 7 | Cross-T0 merged query (G-T5) | No duplicates, correct order, no skipped hits when paging; latency ≤ the slower source + 20% |
| 8 | Freshness | metadata p95 ≤ 60 s; summary p95 ≤ 5 min; **transcript p95 ≤ 60 s** *(proposed here)* |
| 9 | Isolation | zero cross-tenant rows across V1, the new store and the merge |
| 10 | Replay of one session (S3) | p95 ≤ legacy p95 for size deciles 1 to 9; decile 10 within 1.5× |
| 11 | Capacity | ≥ 30% CPU headroom on Supabase and the store; no temp-spill regression; Supavisor connections ≤ 70% of the pool |
| 12 | **Hybrid relevance (G-T8)** | New nDCG@10 ≥ legacy nDCG@10 − 0.02 on the hybrid query set. Conceptual recall tasks hit at k=10 at least as often as legacy. *(Proposed here.)* |
| 13 | **Hybrid latency and degradation (G-T8)** | p95 ≤ legacy hybrid p95, and ≤ 1.5 s end to end; p99 ≤ 3 s. Rate of `degradedPools` responses ≤ legacy. *(Proposed here.)* |

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
| Search-store feed lag, DLQ depth and load errors (G-T1) | the feed's queue and the store | transcript freshness p95 > SLO; DLQ > 0 for 15 min |
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
| S6 | S3 bucket hardening | Not created | SEC-08 items: TLS-only deny, lifecycle, explicit KMS key policy, per-workspace keys (P9) |
| S7 | Supabase RLS and roles | Runtime role has BYPASSRLS (A1) | Only the documented exceptions (`machines`). The search RPC refuses without a policy row (#72). The service-role key is not in the Worker. |
| S8 | Erasure path for the V1 projection | 0002 open item, deferred to sessionspipelinegrok | Recorded decision. A tombstone-driven row delete is tested. The Supabase backup and PITR retention is documented as the erasure tail. |
| S9 | SEC-10 (workspace scoping) and SEC-11 (365-day service tokens) | Open | Decision recorded (K-24). The new read path must not widen either. |
| S11 | ObsessionDB isolation and erasure (G-T6), `us-east-1` | Not provisioned | Per-org isolation proof. Tombstone and reset propagation. Least-privilege feed and query credentials. Erasure decision recorded. Store is TLS only and encrypted at rest. |
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
| K-28 | Store choice. **Decided 2026-10-31: "Yes, add ObsessionDB".** Merging the 0001 amendment is still K-1. | — | — |
| K-29 | Provision production ObsessionDB in `us-east-1` and staging ObsessionDB, and their credentials | — | yes, while empty |
| K-30 | Enable the G-T1 feed into the production store (dark, canaries only, before T0) | — | disable the feed |
| K-31 | Each use of the G-F write freeze, including the phase 3 canary proof | — | unfreeze |
| K-32 | Resize the Supabase `sessions` disk from 2 GB | — | grow only |
| K-33 | Transcript-search region. **Decided 2026-10-31: AWS `us-east-1`.** | — | — |
| K-34 | Ranked dashboard search in the T0 gate. **Decided 2026-10-31: "Include it in the T0 gate", superseded as written by K-36.** | — | — |
| K-36 | **Decided 2026-10-31: "No embeddings pre-T0".** G-T8 is keyword-ranked; nDCG@10 against legacy hybrid is the gate. | — | — |

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
- **Q5.** What reads `uploaded_support_identities` (37 GB) after T0.
- **Q6.** Analysis, briefs and memory: export, re-derive (model cost), or
  drop?
- **Q7.** The number of orgs and their size distribution, for the waves.
- **Q8.** The hosted metrics, SLO and log backend for the alarms in section
  13.
- **Q9.** The export's total zstd size and PUT count, and the KMS request
  cost (O5).
- **Q10.** The Cloud revision-feed replacement (G-R1): owner, and the event
  contract.
- **Q11.** A checked-in re-measure of the 494 GB / 210 GB figures, with
  their unit. The 485.6 GiB branch figure is stale (section 1).
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
  - Khaliq's decisions K-6, K-22, K-27, K-28, K-33, K-34 and K-36;
  - storage facts: `sessions` 494 GB, `convergence_events` 210 GB, no
    production S3 bucket, R2 `relayhistory-content` empty, Supabase Pro /
    Large / 2 GB disk / empty.
- Cloud memory note `relayhistory-revision-publisher-convoy.md`
  (2026-10-24).
