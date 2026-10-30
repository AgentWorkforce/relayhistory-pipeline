# RelayHistory onto the 96Bulls pipeline: execution plan and gates

Implements `docs/decisions/0002-relayhistory-onto-the-pipeline.md`. Session
content goes to S3 in the pipeline layout, and Supabase holds
`session_index` plus its companion tables. Nothing here authorizes a
production action: each `[A#]` needs Khaliq's explicit OK for that exact
action.

## Done (evidence recorded 2026-10-30)

- **A1:** Supabase `relayhistory_owner` and `relayhistory_runtime` (with
  BYPASSRLS), and Neon `relayhistory_readonly` (not `neon_superuser`, read-only
  by default, refused writes). Credentials are only in 1Password
  `96Bulls-prod`.
- **A2:** Supabase `sessions` on Large compute (`ci_large`), healthy.
- **Measured on Neon:**
  - `sessions` totals 484 GB: `convergence_events` 205 GB,
    `delivery_records` 161 GB, `uploaded_support_identities` 37 GB.
  - Collation is `C.UTF-8` (Supabase is `en_US.UTF-8`). That matters only
    for queries Supabase still serves, which are metadata only.
  - No resets, and 5 deleted uploads.
- **Tooling merged in #28** that carries over: the write guards, deletion
  fences, plan-bound checkpoints, the sequence fence, the Neon write fence,
  the benchmark gates and the rehearsal harness.

## Lanes

| # | Lane | Owner | Gate to close |
| --- | --- | --- | --- |
| P1 | Exporter design: which Neon rows form a session's raw stream (`delivery_records` payloads by origin and revision, versus `convergence_events`), the `raw_format`, chunking, the scrub rule set, and the manifest fields `session_index` needs (title, user, machine, repo, model, usage, times) | 96-no-bull-migration | Design doc, plus a test exporting a fixture session that passes the manifest validator, with its index row equal to the legacy rollup |
| P2 | `migration:export`: Neon (read-only role) to S3, newest session first, deletion-fenced (no deleted uploads or reset users), checkpointed, guarded (`--execute`, expected bucket and project), idempotent by manifest revision and content hash | 96-no-bull-migration | Rehearsal: MinIO plus local Supabase, pipeline-built `session_index` rows match Neon metadata, `check` reports 0 missing |
| P3 | Worker reads re-pointed: metadata routes to `session_index`, per-session content routes to S3 objects, transcript search to the metadata fallback | relayhistory-cloud owner | Route contract tests against `session_index` plus fixture S3 objects; the API contract suite unchanged except the documented search degradation |
| P4 | Cloud consumers (Sessions board, shared-session MCP: `search_shared_sessions`, `get_shared_session`, `_context`, `list_session_timings`, `list_team_activity`) on the new reads | helm-chart-chief | `docs/session-search-index-contract.md` updated to the pointer model, with consumer tests |
| P5 | Agent name: on `session_index` or a companion table, and its index (open; #222/0067 is not applied until this is decided) | helm-chart-chief + Khaliq | Schema decision recorded; index audit passes |
| P6 | Cutover gate and durable hold (relayhistory-cloud #224, hosted CI green at `1a3d4139`, draft) | helm-chart-chief fix lane | Independent PASS (per-org ordering, zero-write hold, fencing) |
| P7 | Worker TCP driver to Supavisor (relayhistory-cloud #225, hosted CI green at `5872005b`, draft) | helm-helper | Independent PASS (TLS, cancellation, pooler, role evidence) |
| P8 | Search store: ClickHouse or ObsessionDB loaded from normalized S3 objects | later | Text/n-gram indexes, S3 ingest, per-org isolation and tombstone propagation all proven |

## Sequence

1. P1 to P5 in code, plus rehearsal. P6 and P7 reach PASS.
2. **[A3]** Apply the 96Bulls Supabase migrations (index tables) to
   `sessions`. **[A4]** Enable pipeline ingest to the production bucket.
3. **[A5]** Deploy the gate and the re-pointed Worker. The phase stays
   `neon`, and the Neon fallback serves content until the export catches up.
4. **[A6]** Start `migration:export`, newest first. Run `check` until 0
   sessions are missing.
5. **[A7] T0:** hold, drain, Neon write fence, last export catch-up, flip.
   New uploads go through the pipeline: S3, then `session_index`.
6. **[A8]** Remove the Neon fallback once `check` is 0 and the consumer
   tests are green. **[A10]** Retire Neon later, under its own approval.

## Audit findings

- **2026-10-30, migration-number collision on Neon `dev` (read-only
  evidence):** the `dev` branch (`br-rough-bread-apkej62v`) has
  `0066_nested_exec_pr_evidence.sql` applied at 2026-10-30 07:08:29 UTC. It is
  not on relayhistory-cloud `main` or any fetched remote branch, so it came
  from an unpushed or deleted branch. The applier is unknown. It is left
  unaltered. #224's unapplied `0066_cutover_database_fence` is being
  renumbered away from 0066 and from #222's 0067. The same check found #224's
  `sessions.cutover_fence` absent from all 8 Neon branches and both Supabase
  projects.

## Open questions for Khaliq

- Whether a cutover ships with metadata-only search, or waits for P8.
- Where the agent name lives (P5).
