# Release package: 062300 + 062320 + 062340 in one planned window

Status: draft for the window owner and the consumer owners. Prepared by sessions-authority-evidence. Executor: b1
(claude-cloud-cloud-b1). Approval: b1 reports Khaliq's own-words approval in #owner-decisions for applying 062300 and
062320 together in one planned Friday window, with b1 executing. The executor records that message, its id and its
reference next to each action (AGENTS.md "Relayed approvals"). This document is not an approval, and it authorizes
nothing by itself. Per-slot detail: [062320-org-admission-hold.md](062320-org-admission-hold.md),
[062340-org-erasure-history.md](062340-org-erasure-history.md) (b1, release owner: 062340 is in this window if its review
round passes and the final combined head has one green exact-head CI, local and hosted),
`runbooks/guarded-session-schema-release.md` (the route this follows),
`runbooks/session-directory-search-worker-bootstrap.md` (062300's down and ledger rows).

## 1. Generations

| | Value | Source |
|---|---|---|
| G (live, to observe) | expected `778af686…`, the 062220 release target | read the live marker and policy at plan time. Never take it from this table |
| H (target) | NOT FINAL: re-derive at the landed sha. Candidate: `22ae5a883e9b11c5dd6587c4c2cf7351b0a780c5d619054f54d4a8ef76e79dfb` with 062340 (fix round; 0832700d… was 8b8ab09e); `a9662ebac63a3487…` was 062300 + 062320 only | re-run `bun src/hoopdb/migrate.ts --generation` at the release sha. H changes if any migration byte changes |
| Steps G -> H | `upstream:20261105062300_search_index_stage.sql`, then `upstream:20261105062320_ingest_org_admission_fences.sql`, then `upstream:20261105062340_ingest_org_erasure_history.sql` | the migrator applies them in ledger order in one apply. 062300, then 062320, then 062340 (it replaces 062320's release and status) |

**Prerequisite P0: the final 062220 step.** b1 reports that the live Worker is already H-only at `778af686…`: step-6
commit 191c16db, plan run 45938486616 and deploy run 45938773082 at main 8dd6775f, with `READBACK_PASS`
baseline-drain. `expectedGeneration` is `778af686…` and the tracked `overlapGeneration` was removed, but the reviewed
pair `{3eef01d7…, 778af686…}` is still retained in source. P0 is therefore only lifecycle step 3 of that release:
1. A reviewed source change sets `PRIVATE_INGEST_OVERLAP = null`.
2. A deploy reads back exactly `{778af686…}`.

Re-verify the live Worker, policy and marker at `778af686…` before the window. Step 1 below then starts from a clean
H-only base and sets the new pair `{778af686…, H}`.

## 2. Consumer inventory: rebuild at H in the same window

Every consumer below pins the schema generation and pauses or fails when readiness moves off its build generation.
The rule (runbook, "Schema-gated worker images"):
1. **Before the plan:** build each image at the release sha and record, per environment, every deployment running it
   with its current image and Ready count.
2. **Straight after the PASS at H, not before, in parallel with the rebind:** roll each one. An H image waits in
   `waitForSchema` until the PASS at H; a G image pauses from the PENDING marker onwards.

The pause is silent: NotReady, no crash, no alert. 062010 paused the summary and status workers for about four hours
this way. Owners must confirm every row today, so the H images are pre-built.

| # | Consumer | Pins the generation via | Rebuild owner (proposed by b1; each confirms on Relay) | Verify after the roll |
|---|---|---|---|---|
| 1 | private-ingest Worker (96bulls-session-ingest) | `expectedGeneration` / `INGEST_ACCEPT_GENERATIONS` (`PRIVATE_INGEST_OVERLAP`) | b1 | the deploy readback accepts exactly {G, H}, then exactly {H} |
| 2 | sessions-pipeline indexing Lambda (ONE deployable: the "feed" is its every-minute schedule with input `{"bufferedSearchFeed":true}`, not a second function) | `publicationSchema.expected.generation`; `ingest_require_generation` on every pointer write | pipeline-handoff-sfmini | one H build from the LANDED sha (plus that day's feed changes, re-derived H); the release doc records the zip sha256 and the Lambda CodeSha256; pointer writes and the feed schedule succeed after the PASS at H, no 55000. Counted once in the Ready readback |
| 3 | prod EKS: ONE shared pin `sessions-pipeline.worker.image`, used by the enabled bob-costas (summaries) and referee | schema-gated SERVICE (`docker/entrypoint.sh`) | layered-sessions-pipeline (one H image at the landed sha); envs-lead-3 (ONE EO pin-bump PR via infragrok, moving `sessions-pipeline.worker.image` only) | bob-costas and referee back at their recorded Ready counts on the H digest, no `worker paused` log; summary dead-letter count not grown since step 1. `sessionReadApi.image` shares the digest today but is not schema-gated (layered-sessions-pipeline's source answer to envs-lead-3): it stays pinned at its current digest, and the PR must not change it |
| 4 | session-brief, hoopdb-indexer, alleyoop-ingest, searchIndexer, nativeConsumer (session-pipeline-consumer) | schema-gated SERVICE | none: DISABLED on prod EKS (envs-lead-3's read-only EO check at 3e5bb8d5) | re-confirm disabled at plan time. Anything enabled before the window gets a row and an H image |
| 5 | buffered GC job | `INGEST_GC_EXPECT_GENERATION` (CloudFormation `ExpectedGeneration`) | DEPLOYED and SCHEDULED (b1's read-only check, 2026-11-27): stack SessionsBufferedGc with `EnableGc=true`, Lambda sessions-buffered-gc, an every-minute rule ENABLED. Its `ExpectedGeneration` is `78c7b28e…`, NOT the live `778af686…`, so EVERY invocation currently fails `buffered_gc_55000` (Healthy=0): no buffered GC is running (storage only, no data loss). Owner to confirm: layered-sessions-pipeline (proposed) | deploy BOTH a Lambda package compiled at the landed sha (embedded `expected.generation` = H) AND `ExpectedGeneration=H`; never a config-only roll, which refuses `compiled_generation_mismatch` (`src/session-ingest/buffered-gc-job.ts` runBufferedGcJob). This also ends the current failure. Readback: the Lambda CodeSha256 matches the H build; the next run starts at H with no `validation_pending` and Healthy > 0 |
| 6 | publication job | `INGEST_PUBLISH_EXPECT_GENERATION` | NOT DEPLOYED: b1's read-only check found no running EC2 host in sessions-prod or agentrelay-prod | out of the consumer set. Re-check at plan time |
| 7 | canonical-project-events relay | `INGEST_EXPECT_GENERATION` | NOT DEPLOYED: b1's read-only check found no Cloudflare Worker `96bulls-canonical-project-events` on the account (`wrangler deployments list`: code 10007, "This Worker does not exist") | out of the consumer set. Re-check at plan time; if it exists then, it needs its own reviewed Wrangler deploy at H (`src/canonical-project-events/deployment.ts`) with the Worker owner's receipt |
| 8 | Neon-to-S3 erasure babysitter, retention phase | `INGEST_EXPECT_GENERATION` (only when `BABYSITTER_RETENTION_SWEEP` is not `off`) | NOT DEPLOYED: b1's read-only check found no Lambda, rule or host in sessions-prod | out of the consumer set. Re-check at plan time |

Rows 5 to 8 are answered by b1's read-only checks (2026-11-27). Re-check each at plan time; a change is a new row.
Also seen in sessions-prod: session-brief's schedule DISABLED, and session-reconcile ENABLED (every 30 min), which runs
row 2's Lambda.

Not schema pins (no rebuild needed): the read Worker (`generation` is a client parameter), semantic copy (its own
counter). The 062280 scoped publication RPC is not landed, so it is not in this window. Re-grep
`expectedGeneration|EXPECT_GENERATION|ingest_require_generation|expected.generation` at the release sha before the plan.
Any new hit is added here.

## 3. Order, with the rollback of each step

The plan binds the exact landed release sha as `target_sha`. Its `expectedPending` must be exactly and only
`upstream:20261105062300_search_index_stage.sql`, `upstream:20261105062320_ingest_org_admission_fences.sql`, then
`upstream:20261105062340_ingest_org_erasure_history.sql`.
G as observed live is an executor prerequisite, never taken from this document.

| Step | Action | Readback | Rollback while at this step |
|---|---|---|---|
| P0 | the 062220 pair -> `null` (section 1) | the live Worker reads back exactly `{778af686…}` | restore the pair (the reviewed source change reverted) |
| 1 | Worker G+H. A reviewed source change sets `PRIVATE_INGEST_OVERLAP = {from: G, to: H}` and the tracked `overlapGeneration: H`. It lands, and the private-ingest deploy moves the Worker to accept {G, H} | the live deploy accepts exactly {G, H} | deploy G+H -> G (allowed only while readiness is PASS at G: `src/session-ingest/private-ingest-release.ts` generation moves) |
| 2 | Pre-build every H artifact (section 2) and record, per row: the deployment, its current artifact id (the G image digest, Lambda CodeSha256, Worker version id) and Ready count, plus the owner's receipt | the list is recorded, one owner receipt per artifact, config and roll | nothing deployed: none |
| 3 | Open the overlap window G -> H (`.github/workflows/ingest-generation-overlap.yml` mode `open`). The TTL must cover plan, review, apply and the rebind dispatches: up to 4 h, at least 15 min left at plan and at apply | the window row shows G -> H, the TTL, `approval_sha256` and `opened_by` | close the window (owner close); then step 1's rollback |
| 4 | Plan at the release sha. The plan's `ingestOverlap` section shows the gate passes, and both steps, and only them, are listed in order | plan artifact | discard the plan |
| 5 | Apply, inside the window. The migrator writes PENDING at H, applies 062300, then 062320, then 062340, then writes PASS at H. Keep `PRIVATE_INGEST_ORIGIN_CLAIM_FENCE` **off** for the apply | the marker is PASS at H; the ledger shows the three steps and no others; `release_post_apply_ingest ingest_overlap_rebind_pending` | before PASS: a failed apply leaves readiness pending at H. Re-plan and re-apply while the window has at least 900 s left, else "Failed apply without a window". After PASS: see "After the PASS at H" below |
| 6 | **Rebind, immediately** on `ingest_overlap_rebind_pending` (`runbooks/guarded-session-schema-release.md`, post-apply ingest check): `rebind-dry-run`, then `rebind`, which closes the window. It never waits for step 7, a failed roll or dead letters | policy and admission rows at H; window `closed_reason = 'rebind'` | if the window expires first, uploads fail closed (55000): "Post-apply recovery", `docs/design/zero-503-schema-windows.md` section 3.2 (the rebind needs no window) |
| 7 | **Consumer rolls, started straight after the PASS, in parallel with step 6** (section 2). Rows 2, 3, 5, 6 and 8 per their owners. The private-ingest Worker's accept set (row 1) is NOT changed here: it stays {G, H} until step 8. A disabled or optional job is verified in its existing mode (disabled, `off`, not deployed); it is never enabled to prove readiness | each roll at its recorded Ready count on the H artifact; the summary dead-letter count recorded; a disabled job confirmed still disabled | a failed roll is fixed forward on an H artifact (a G artifact cannot pass at H). It does not block step 6 |
| 8 | Worker H-only, only after the rebind (step 6): deploy `expectedGeneration = H` (G+H -> H) | the deploy accepts exactly {H} | **no reverse deploy**: H -> G+H is refused by `private-ingest-release.ts` (the only moves are G -> G+H, G+H -> H, G+H -> G). Before this step the Worker simply stays G+H; after it, recovery is state-specific (below) |
| 9 | Close out: a reviewed source change sets the pair back to `null` once the live Worker reads back exactly {H} | the live Worker reads back exactly H | none needed |

**Pause budget.** A schema-gated worker passes only while the marker is PASS at its own image's generation, so every
G image pauses from the PENDING marker at the start of step 5, through the PASS, until its H roll in step 7 is
Ready. Budget that whole span (apply plus the slowest roll) as consumer downtime, and have every H artifact built
and its owner standing by before step 5.

**After the PASS at H.** There is no generic rollback. Running the three downs, newest first (062340, 062320, 062300), only removes the
schema and leaves readiness PENDING; it does not restore service. The default response to a post-PASS problem is a
forward fix at H. A schema rollback is an escalation to Khaliq, attempted only if the executor can establish every
one of these, and otherwise stopped:
1. The down files' checksums match the pinned ones (`supabase/rollbacks/20261105062340_ingest_org_erasure_history.down.sql`,
   then `supabase/rollbacks/20261105062320_ingest_org_admission_fences.down.sql`, then
   `supabase/rollbacks/20261105062300_search_index_stage.down.sql`), and they are owner-run.
2. 062340's down guard, then 062320's, pass: no later slot applied, no hold that is not released (none is installed
   in this window). 062340's down keeps its history tables and records the end of coverage.
3. 062300's down guard passes: writers off (`SEARCH_WAKE_ENQUEUE`, the indexer's managed search wake, the
   catch-up), the stage drained, the consumer off (`SEARCH_JOBS_ENABLED`), and no search-index row in
   `pipeline_jobs`, `pipeline_events` or `pipeline_dead_letters`, and no claim receipt or contention row
   (`runbooks/session-directory-search-worker-bootstrap.md`, "Rollback and removal"). Parked letters are replayed,
   resolved and archived first.
4. The step 2 record holds every previous artifact id (G images, the G Lambda zip and CodeSha256, the Worker
   version).
5. A reviewed procedure exists to write a verified PASS at the rollback generation G. The downs do not do it, and
   this package does not supply one.
6. The policy and admission rows can be brought back to G for the actual rebind state. After step 6 they are at H,
   and no reviewed H -> G rebind exists in this package.
7. The Worker can accept G again in its actual state. Before step 8 it is still {G, H}; after step 8 it is {H}, and
   no reviewed move returns it to G.

Items 5 to 7 are not available today, so a post-PASS rollback is not executable from this package. Plan the window
on that basis.

**062300's worker pause.** 062300's stage CHECKs and queue guards change what schema-gated workers accept, and every
G image pauses from the PENDING marker onwards (the pause budget above). The overlap keeps buffered ingest
accepting both generations (steps 1 and 3), but it does not un-pause G-image workers. Step 7 is what handles that,
which is why the H artifacts are built in step 2 and rolled straight after the PASS. Until the infra alert exists,
step 7's readback is the only check. Treat any shortfall as an incident, but never hold the rebind for it.

**062340 in this window.** Its recorders start writing the erasure history at the apply (a row per write to the 15
erasure sources, plus a shared org-key lock per write; TRUNCATE of a source is refused only while a hold is live, and none
is). Every later wave's FULL must start after the apply plus the 15-minute margin, or bind refuses its cutoff.

**062320 in this window.** It is inert until a hold is installed per org, and none is installed in this window. Its
mirror role is not provisioned here: until it is, controls of a SEALED org are refused, and no org is sealed.

## 4. What the executor records

- Khaliq's message id and reference.
- G as observed, H as derived.
- The window row.
- The plan and apply runs.
- Every consumer's previous and new artifact id, its readback, and the dead-letter counts before and after.
- One direct owner receipt per artifact, config and roll, not relayed; for a row found not running, the receipt for that finding.
- The rebind run.
- Each Worker deploy readback.

Hashes and ids only: no credentials, and no production identifiers in this public repository. Operator notes go to
the private ops repository.
