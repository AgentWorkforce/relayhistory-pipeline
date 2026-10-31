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

**Out of scope: full-transcript search.** It stays on the legacy system until
ClickHouse or ObsessionDB is proven (relayhistory-cloud #233, a draft;
Khaliq deferred it on 2026-10-31: "clickhouse endpoint -- we don't need to
prove now"). Section 6 covers what that deferral means at T0.

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
| 2026-10-31 | K-6 | "Serve old sessions from T0 branch" | Khaliq, lead session, relayed by the coordinator | Option T2 (section 6). Pre-T0 sessions keep legacy transcript search, served from the frozen Neon T0 child branch. Post-T0 sessions get V1 metadata and short_summary search only, until ClickHouse or ObsessionDB is proven. |
| 2026-10-31 | K-22 | "Forward-only after flip" | Khaliq, lead session, relayed by the coordinator | Option R2 (section 11). T0 can be aborted up to the flip. After the flip there is no data rollback to Neon, and no reverse-sync (G-V) is built. |

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
2. **Per-tenant phases happen around a single write flip** (section 7):
   - **Before T0, per tenant:** export a rehearsal snapshot of the internal
     orgs to a staging Supabase project and staging bucket, and check parity
     and benchmarks there.
   - **At T0:** one global write flip, with an abort boundary that keeps it
     reversible up to the flip.
   - **After T0, per tenant:** export waves, then removing the Neon read
     fallback and enabling V1 search, one wave at a time. Each wave has its
     own rollback.
3. **Critical-path gates**, in order:
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
   8. The child-branch transcript search (K-6, decided: T2) passes metric 1b.

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
| Pipeline code exists in relayhistory-cloud but is inert: storage adapter #211/#216 (S3 API supported); pointer index #212; summary-short stage (`docs/pipeline-summary-short.md`); single-use migration assertions #228 (merged 2026-10-31 UTC). | PR states, read 2026-10-30 |
| Supabase project `sessions` (`pyamkrhoyszgzirqqffq`) has Large compute (`ci_large`). Roles `relayhistory_owner` and `relayhistory_runtime` (BYPASSRLS) exist, as does the Neon role `relayhistory_readonly`. | `docs/relayhistory-pipeline-migration.md` "Done" (A1, A2) |
| **Supabase region: unknown.** Not recorded in any doc or PR read for this plan. | Read-only check: `scripts/sb projects list` |
| **Production S3 bucket: does not exist yet, as far as any record shows.** The account is not recorded. `sessions-prod` (`941433935742`) is the existing workload account named in the cutover doc. | Open question Q2 |

### Verifying the 485.6 GiB figure

- **Source.** The uncommitted `docs/relayhistory-cutover.md` (linuyc,
  2026-10-29) says "Neon reports 521,364,119,552 logical bytes". That figure
  is the Neon branch's logical size, which covers the **whole branch**, both
  `public` and `sessions`. Converted: 521,364,119,552 / 2^30 = **485.56 GiB**
  (521.4 GB decimal). The conversion is correct.
- **No captured artifact backs it.** No API response or query output for
  this number is checked in. The `docs/evidence/2026-10-30-neon-capacity/`
  files do not contain it.
- **A second, independent measurement.**
  `docs/relayhistory-pipeline-migration.md` gives:
  - `sessions` = 484 GB;
  - `convergence_events` 205 GB, `delivery_records` 161 GB and
    `uploaded_support_identities` 37 GB.

  These are most likely `pg_size_pretty`, which prints binary units labelled
  "GB". No captured output is checked in for these either.
- **Conclusion.** Both figures say `sessions` is about 484 GiB of a 485.6 GiB
  branch, which would leave roughly 1.6 GiB for Cloud's `public` schema.
  That is plausible but **unverified**.
- **Before any capacity decision**, re-measure read-only and check in the
  output:
  - `neonctl branches get br-damp-mud-apbr6ifp --project-id divine-snow-84812652 -o json` (`logical_size`);
  - `SELECT n.nspname, sum(pg_total_relation_size(c.oid)) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relkind IN ('r','m') GROUP BY 1`.
- **The figure matters less than it used to.** Under option D most of it is
  re-derived or exported, and none of it is copied into Supabase (section 4).

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
| Object store | One production S3 bucket, versioned, with SSE-KMS and a key per workspace (P9). TLS-only bucket policy. Lifecycle that expires noncurrent versions (SEC-08). v1 layout `{prefix}/v1/ws=/m=/h=/d=/s=/{manifest.json, raw/, summary/}`. **Bucket name and account are unknown and not created.** | `docs/pipeline-storage-adapter.md`; relayhistory-cloud `object-layout.ts` |
| Pipeline compute | **Inferred, needs confirmation (Q1):** the relayhistory-cloud Worker and Cloudflare Queues run the pipeline stages (object-landed, session-indexed, summary-short). 0001 limits production AWS to S3, so the 96Bulls EKS services (AlleyOop, Bob Costas, hoopdb-indexer, Referee) are not in hosted production. They are self-host proof only (`aws-proof`, 500268966557/us-west-2). | 0001; relayhistory-cloud #231 binds the session-indexed queue in the Worker |
| Worker to Supabase | Direct TCP through Supavisor (relayhistory-cloud #225), one client per request. Metadata reads go through PostgREST (#226). Hyperdrive only if the benchmark justifies it. | `docs/relayhistory-supabase-migration.md` |
| Worker to S3 | The S3 API with `PIPELINE_RAW_S3_{ENDPOINT,REGION,BUCKET,ACCESS_KEY_ID,SECRET_ACCESS_KEY,SSE}`. A Worker cannot use IAM roles, so this is a long-lived IAM access key. Security gate S5 covers it. | relayhistory-cloud `lib/pipeline/adapter.ts` |
| Auth | Hosted RelayAuth (cloud `packages/relayauth`, Cloudflare Worker + D1), `@relayauth/server` 0.2.35 on cloud `trunk` (#5886). A **separate** search issuer and key (relayauth #116, draft). Supabase trusts only that issuer for `session_directory_search_worker`. | #63, #72, #73, relayauth #116 |
| Transcript search | Legacy: `ILIKE`/trigram over Neon. After T0 it can only serve the T0 child branch (section 6). | 0002; #226 fails it closed |
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

Sizes come from `docs/relayhistory-pipeline-migration.md` (unverified,
section 1). Row counts are live tuples from `load-results.json`.

| Data | Size, rows | Moves? | Where and how |
| --- | --- | --- | --- |
| `delivery_records` (the client's own records, with revisions: session, session_event, tool_call, file_edit, …) | 161 GB, 54.0M | **Must move.** It is the only source of session content. | Exported from the T0 child branch to S3 as scrubbed raw chunks plus a manifest per session (`docs/relayhistory-export-design.md`, #40, E1 to E4). Not copied into Supabase. |
| `convergence_events` | 205 GB, 27.7M | **Re-derived, not moved.** 99.8% of sampled rows reference a `delivery_record_id`. | Rebuilt by the pipeline's normalizer from the S3 raw stream. The derived replay comes from S3 objects. |
| `session_rollups`, `session_catalog`, `session_markers` (25.0M), `session_relationships`, `session_commit_links`, `session_catalog_agents`, `session_timing_cache`, projection/acceptance tables (`delivery_projection_acceptance` 50.5M, `delivery_catalog_acceptance` 25.6M) | in the 484 GB | **Re-derived.** | `session_index` and `session_directory` are rebuilt from manifests by session-indexed and the directory projector. Timing comes from manifest activity times. |
| `uploaded_support_identities` | 37 GB, 54.3M | **Unknown (Q5).** It is the third-largest table, and no doc says what reads it after T0. | Decide: export, re-derive, or leave on the child branch until A10. |
| Copy set: `auth_sessions`, `deleted_upload_sessions`, `session_resets`, `upload_cleanup_origins`, `upload_cleanup_tombstones`, `machines`, `probe_connections`, `project_aliases`, `github_org_links`, `delivery_origins`, `delivery_receipts` | small (measure; runbook step 3) | **Must move before the flip.** These are write-path state: tokens, tombstones, cursors. | `migration:copy --mode=mirror` pre-copied (A4), then caught up inside the hold (runbook steps 4 and 10). |
| Analysis, briefs, memory state | unknown | **Decide (Q6).** | Either export as derived objects, or re-run after the export behind `MODEL_CALLS_ENABLED` and a budget. |
| Summaries | n/a | **Re-derived.** | summary-short runs from S3 after the export. That costs model calls; budget it per wave. |
| Cloud `public` schema | ~1.6 GiB (inferred) | **Does not move.** | Stays on Neon. |

What this means for capacity: Supabase holds pointers and metadata, roughly
one row per session plus memberships. That is small, but **the export's
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

## 6. Transcript search across T0 (decided: T2)

Two things conflict:

- **The requirement:** transcript search stays on the legacy system until V2.
- **The design:** switch-first means Neon `sessions` takes no writes after
  T0, so the legacy transcript index stops receiving new content at T0.

Today's #226 fails transcript and hybrid search closed after T0. That
**disables** transcript search (`search_shared_sessions`, `/v1/events?q=`,
dashboard transcript search) for every session.

| Option | Pre-T0 sessions | Post-T0 sessions | Cost |
| --- | --- | --- | --- |
| T1. Fail closed (today's #226) | metadata + summary only | metadata + summary only | Visible product regression on day one |
| **T2. Legacy over the T0 child branch (recommended)** | **full transcript search**, run on the child's compute, read-only, bounded and breaker-protected | metadata + summary (V1) only, until V2 | A G3 change to #226. Child compute runs for as long as V2 takes. Results must be merged with V1 results and labelled by coverage. |
| T3. Delay T0 until V2 is proven | full (on Neon) | full | T0 waits on ClickHouse. Neon stays under stress for longer. |
| T4. Keep writing post-T0 transcript rows to Neon or Supabase | full | full | Violates I2 and I6, or 0002 (no content in Supabase). **Rejected.** |

**Decided: T2 (K-6, Khaliq, 2026-10-31: "Serve old sessions from T0
branch").**

- Pre-T0 sessions keep legacy transcript search, served from the frozen
  Neon T0 child branch.
- Post-T0 sessions get V1 metadata and short_summary search only, until
  ClickHouse or ObsessionDB is proven.
- Search responses must say which coverage applies, so a missing post-T0
  transcript hit does not look like a real "no match".
- Tell users before T0 that content written after the cutover is not
  searchable by transcript text until V2 (Q14).

- **Prerequisite:** the child branch's compute must pass the transcript-query
  benchmark (section 10, metric 1b).
- **If metric 1b fails:** the decision does not cover T1 or T3. Size the
  child's compute within Neon plan limits (Q12), or go back to Khaliq.
- **Consequence:** the export does not remove the child branch while
  transcript search depends on it. A9 (fallback removal) becomes "removed for
  replay and metadata", and the child stays until V2. Its compute is a
  running cost for that whole period.

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
  - #226 (G3: terminal reads plus the T2 change from section 6);
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
4. Rehearse T0 end to end against staging: hold, fence on a scratch branch,
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
- Pre-T0 transcript search stays on the child branch (T2, K-6). Removing
  the replay and metadata fallback for a wave does not remove transcript
  search from that wave.
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
   - the T2 child-branch transcript route (K-6) passes metric 1b;
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
   - flip cloud #5850's flag.

   HoldLogs replay in order into the pipeline, and crons and consumers
   resume.
8. **Verify.** Run canaries for: login, ingest, delivery batch, turns,
   session list, V1 search, replay of a post-T0 session from S3, replay of a
   pre-T0 session through the child, transcript search through the child
   (T2), brief, and a Cloud revision event (G-R1).

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

## 10. Benchmark gate (legacy vs new)

**Data.** Two inputs, both measured from the same region as the Worker.
Workers run at the edge, so "from the Worker" means a measurement Worker or
client in each representative user region:

1. The rehearsal snapshot (phase 2): W0 orgs, real shapes and real sizes.
2. #59's synthetic corpus at scales S and M, for the cross-tenant scale shape.
   It is a stand-in, not a production number.

Use several org/query/session samples. One term is a smoke test, not a
conclusion.

**Tools:**
- `benchmark:relayhistory`: the database path, with p50/p95/p99, QPS, parity
  and redacted EXPLAIN.
- `benchmark:relayhistory-api`: HTTP, both endpoints in the same window.
- #74's planted-trace pattern: freshness.

**Two passes:** concurrency 1, and agreed peak concurrency.

| # | Metric | Legacy measured on | New measured on | Proposed threshold (Khaliq to set, K-5) |
| --- | --- | --- | --- | --- |
| 1a | Metadata/V1 search: p50, p95, p99 | Neon rehearsal branch, legacy ILIKE on title and agent, through the API | staging Supabase `search_session_directory` through the API | new p95 ≤ legacy p95 on every critical query, and p99 ≤ 1.5× legacy p99 |
| 1b | Transcript search on the child (T2): p50, p95, p99 | Neon production (read-only, off-peak, only with K-12) | the rehearsal child branch's compute | child p95 ≤ 1.25× production p95. Otherwise size the child's compute; T1 and T3 need a new decision from Khaliq. |
| 2 | Session list and filters: p50, p95, p99 | legacy API | new API | new p95 ≤ legacy p95 |
| 3 | Replay of one session: p50, p95, p99, by session-size decile | legacy keyset | S3 objects plus normalization | p95 ≤ legacy p95 for deciles 1 to 9; decile 10 within 1.5× |
| 4 | Ingest-to-searchable freshness (accepted upload → visible in list or V1 search) | legacy: upload → visible in `search_shared_sessions` | manifest POST → `session_index` → `session_directory` | metadata p95 ≤ 60 s, p99 ≤ 120 s. short_summary p95 ≤ 5 min (it depends on the model). |
| 5 | Error rate (non-2xx, excluding expected 4xx) | legacy API, same window | new API | ≤ legacy + 0.1 percentage points; zero 5xx caused by the driver or pool |
| 6 | Capacity at peak | Neon CPU and temp | Supabase CPU, connections (Supavisor), temp spill | ≥ 30% CPU headroom; no temp-spill regression; Supavisor connections ≤ 70% of the pool |
| 7 | Plans | — | EXPLAIN | No seq scan where a selective index exists. **Known red:** #59 shows the agent-prefix filter running as an RLS filter, not an index condition, under `authenticated`. It must be fixed before this gate can pass. |

**Out of scope for this gate:** V2 transcript search (deferred), dollar cost,
and cache-cold Postgres latency (#59 "not measured"). If those are required,
add them.

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
- **Writes:** a write freeze if writes are corrupting data. **Unknown:**
  #224 allows no `supabase → holding` move, so a post-flip freeze needs a
  separate mechanism, such as a route flag returning 503. That mechanism
  needs a design before T0 (Q15).


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
| Ingest-to-searchable freshness | `pipeline_events` spans (`docs/pipeline-observability.md`) | p95 > SLO (section 10, metric 4) |
| Fallback hit rate per route and per org; breaker trips | Worker | hit rate rising for an exported org; any breaker trip |
| Export coverage and throughput per wave | `migration:export` checkpoint | stalled > 30 min |
| Supabase CPU, connections, temp, replication/PITR health | Supabase metrics | CPU > 70% for 15 min; connections > 70% of the pool |
| S3 4xx/5xx and KMS throttling | CloudWatch (the bucket's account) | any `AccessDenied` from the Worker identity; KMS throttle > 0 |
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
| K-6 | Transcript-search behaviour across T0. **Decided 2026-10-31: T2, "Serve old sessions from T0 branch".** | — | — |
| K-7 | Approve the separate search-issuer direction (#63) | — | — |
| K-8 | Release relayauth with #116 and bump hosted RelayAuth (cloud PR, train) | — | revert or redeploy |
| K-9 | Create the hosted search-issuer API key and signing key (G-H), and configure Supabase to trust the issuer | — | revoke or rotate |
| K-10 | Create the staging Supabase project and staging bucket | — | yes |
| K-11 | Create the rehearsal Neon child branch | — | delete the branch |
| K-12 | Export W0 org data into **staging** (production content leaves Neon, into staging), and benchmark against Neon production read-only | — | delete the staging data |
| K-13 | Set the Worker secrets `SUPABASE_DATABASE_URL`, `CUTOVER_ADMIN_SECRET` and `PIPELINE_RAW_S3_*`, and the variables `CUTOVER_GATE_ENABLED` and `SUPABASE_PROJECT_REF`; deploy, phase `neon` | A5 | unset and redeploy |
| K-14 | Deploy cloud #5850 with its flag off (remove `merge-train:hold`) | A6 | yes |
| K-15 | Enable shadow-read for W0 | — | yes |
| K-16 | Pre-copy the copy set and apply the sequence fence on Supabase | A4 | while Supabase takes no traffic |
| K-17 | **T0:** hold, apply the Neon fence, catch up, write the marker, create the child branch, flip, flip Cloud's flag, and enable the pipeline flags. Each sub-step is confirmed live. | A7 | abort until the flip |
| K-18 | Start each export wave W0 to W3 into the **production** bucket | A8 | stop, and delete by prefix |
| K-19 | Remove the fallback and enable V1 search per wave | A9 | turn the fallback back on |
| K-20 | Remove the T0 child branch (ends T2 transcript search) | — | **no** |
| K-21 | Drop Neon `sessions` after the final snapshot | A10 | **no** |
| K-22 | The reverse-data decision. **Decided 2026-10-31: R2, "Forward-only after flip". No reverse-sync is built.** | — | — |
| K-23 | The negative security probe against hosted RelayAuth production (S1) | — | — |
| K-24 | The SEC-10 workspace-boundary decision | — | — |
| K-25 | Each post-flip fix-forward action (Worker rollback within phase `supabase`, write freeze, flags) | — | depends on the action |
| K-26 | Any Neon load relief before T0: compute, `work_mem`, vacuum | — | yes |

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
- **Q3.** Supabase `sessions` region, plan tier, PITR add-on and backup
  retention.
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
- **Q11.** Whether the 485.6 GiB and 484 GB figures reconcile once they are
  re-measured with checked-in output (section 1).
- **Q12.** Whether T2 child-branch transcript search can meet metric 1b
  within Neon plan limits (16 CU maximum).
- **Q13.** Owners for the unowned items G-R1, G-P, G-S and G-H.
- **Q14.** Who announces the post-T0 transcript-search limit (K-6) to users,
  and how.
- **Q15.** The post-flip write-freeze mechanism, given that #224 allows no
  `supabase → holding` move (section 11).

---

## 17. Sources read for this plan

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
- `~/selfhost-proof/notes/aws-checkpoint-2026-10-30.md` and
  `security-sweep-2026-10-30.md`.
- Cloud memory note `relayhistory-revision-publisher-convoy.md`
  (2026-10-24).
