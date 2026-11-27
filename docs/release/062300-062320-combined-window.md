# Release package: 062300 + 062320 in one planned window

Status: draft for the window owner and the consumer owners. Prepared by sessions-authority-evidence. Executor: b1
(claude-cloud-cloud-b1). Approval: b1 reports Khaliq's own-words approval in #owner-decisions for applying 062300 and
062320 together in one planned Friday window, with b1 executing. The executor records that message, its id and its
reference next to each action (AGENTS.md "Relayed approvals"). This document is not an approval, and it authorizes
nothing by itself. Per-slot detail: [062320-org-admission-hold.md](062320-org-admission-hold.md),
`runbooks/guarded-session-schema-release.md` (the route this follows),
`runbooks/session-directory-search-worker-bootstrap.md` (062300's down and ledger rows).

## 1. Generations

| | Value | Source |
|---|---|---|
| G (live, to observe) | expected `778af686…`, the 062220 release target | read the live marker and policy at plan time. Never take it from this table |
| H (target) | `a9662ebac63a3487e64065d0d99a0fb819a2f5058e2f29df421403c8592e753c` at the integration head `ec9c9448` | re-run `bun src/hoopdb/migrate.ts --generation` at the release sha. H changes if any migration byte changes |
| Steps G -> H | `upstream:20261105062300_search_index_stage.sql`, then `upstream:20261105062320_ingest_org_admission_fences.sql` | the migrator applies them in ledger order in one apply. 062300 always precedes 062320 |

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
2. **Straight after the PASS at H, not before:** roll each one. An H image waits in `waitForSchema` until the PASS at
   H; a G image pauses once the marker is at H.

The pause is silent: NotReady, no crash, no alert. 062010 paused the summary and status workers for about four hours
this way. Owners must confirm every row today, so the H images are pre-built.

| # | Consumer | Pins the generation via | Rebuild owner (proposed by b1; each confirms on Relay) | Verify after the roll |
|---|---|---|---|---|
| 1 | private-ingest Worker (96bulls-session-ingest) | `expectedGeneration` / `INGEST_ACCEPT_GENERATIONS` (`PRIVATE_INGEST_OVERLAP`) | b1 | the deploy readback accepts exactly {G, H}, then exactly {H} |
| 2 | sessions-pipeline indexing Lambda (778a) | `publicationSchema.expected.generation`; `ingest_require_generation` on every pointer write | pipeline-handoff-sfmini | pointer writes succeed after the PASS at H; no 55000 |
| 3 | sessions-pipeline feed Lambda (if live) | the same runtime | pipeline-handoff-sfmini | as row 2 |
| 4 | session-pipeline-consumer | schema-gated SERVICE (`docker/entrypoint.sh`) | layered-sessions-pipeline (image); envs-lead-3 (EKS pin bump) | Ready count restored; no `worker paused` log |
| 5 | bob-costas (summaries) | schema-gated SERVICE | layered-sessions-pipeline (image); envs-lead-3 (EKS pin bump) | Ready restored; summary dead-letter count not grown since step 1 |
| 6 | session-brief | schema-gated SERVICE | layered-sessions-pipeline (image); envs-lead-3 (EKS pin bump) | Ready restored |
| 7 | referee | schema-gated SERVICE | layered-sessions-pipeline (image); envs-lead-3 (EKS pin bump) | Ready restored |
| 8 | alleyoop-ingest | schema-gated SERVICE | layered-sessions-pipeline (image); envs-lead-3 (EKS pin bump) | Ready restored |
| 9 | hoopdb-indexer | schema-gated SERVICE | layered-sessions-pipeline (image); envs-lead-3 (EKS pin bump) | Ready restored |
| 10 | buffered GC job | `INGEST_GC_EXPECT_GENERATION` | layered-sessions-pipeline | next run starts at H with no `validation_pending` |
| 11 | canonical-project-events relay | `INGEST_EXPECT_GENERATION` | layered-sessions-pipeline (image); envs-lead-3 (pin bump) | relay claims at H |
| 12 | publication job | `INGEST_PUBLISH_EXPECT_GENERATION` (`compiled_generation_mismatch` otherwise) | layered-sessions-pipeline | a publish run succeeds at H |
| 13 | Neon-to-S3 erasure babysitter, retention phase | `INGEST_EXPECT_GENERATION` (only when its mode is not `off`) | layered-sessions-pipeline (image); envs-lead-3 (pin bump) | the phase runs at H, or is confirmed `off` |

Not schema pins (no rebuild needed): the read Worker (`generation` is a client parameter), semantic copy (its own
counter). The 062280 scoped publication RPC is not landed, so it is not in this window. Re-grep
`expectedGeneration|EXPECT_GENERATION|ingest_require_generation|expected.generation` at the release sha before the plan.
Any new hit is added here.

## 3. Order, with the rollback of each step

| Step | Action | Readback | Rollback while at this step |
|---|---|---|---|
| P0 | the 062220 pair -> `null` (section 1) | the live Worker reads back exactly `{778af686…}` | restore the pair (the reviewed source change reverted) |
| 1 | Worker G+H. A reviewed source change sets `PRIVATE_INGEST_OVERLAP = {from: G, to: H}` and the tracked `overlapGeneration: H`. It lands, and the private-ingest deploy moves the Worker to accept {G, H} | the live deploy accepts exactly {G, H} | deploy G+H -> G (the reviewed rollback transition) |
| 2 | Pre-build every H image (section 2) and record the deployments, images and Ready counts | the list is recorded | nothing deployed: none |
| 3 | Open the overlap window G -> H (`.github/workflows/ingest-generation-overlap.yml` mode `open`). The TTL must cover plan, review, apply, worker rolls and rebind: up to 4 h, at least 15 min left at plan and at apply | the window row shows G -> H, the TTL, `approval_sha256` and `opened_by` | close the window (owner close); Worker back to G |
| 4 | Plan at the release sha. The plan's `ingestOverlap` section shows the gate passes, and both steps are listed in order | plan artifact | discard the plan |
| 5 | Apply, inside the window. The migrator applies 062300, then 062320, then writes PASS at H. Keep `PRIVATE_INGEST_ORIGIN_CLAIM_FENCE` **off** for the apply | the marker is PASS at H; the ledger shows both steps | before PASS: a failed apply leaves readiness pending at H. Re-plan and re-apply while the window has at least 900 s left, else "Failed apply without a window". After PASS: the reverse downs, newest first. 062320's down refuses while any hold is not released (none is installed in this window) and resets readiness to pending. Then 062300's down. Both are checksum-pinned and owner-run |
| 6 | Roll every recorded consumer to its H image, immediately after the PASS (section 2) | each is at its recorded Ready count on the H image; the summary dead-letter count has not grown | roll back the images only together with a schema rollback: a G image cannot pass once the marker is at H |
| 7 | Rebind G -> H (`rebind-dry-run`, then `rebind`), which closes the window | policy and admission rows at H; the window `closed_reason = 'rebind'` | Post-apply recovery, `docs/design/zero-503-schema-windows.md` section 3.2 |
| 8 | Worker H-only: deploy `expectedGeneration = H` (G+H -> H) | the deploy accepts exactly {H} | deploy H -> G+H (the reviewed transition) while the pair is still set |
| 9 | Close out: a reviewed source change sets the pair back to `null` | the live Worker reads back exactly H | none needed |

**062300's worker pause.** 062300's stage CHECKs and queue guards change what schema-gated workers accept, and every
G image pauses from the PASS at H onwards. The overlap keeps buffered ingest accepting both generations (steps 1 and
3), but it does not un-pause G-image workers. Step 6 is what handles that, which is why the H images are built in
step 2 and rolled immediately after the PASS, not later. Until the infra alert exists, step 6's readback is the only
check. Treat any shortfall as an incident.

**062320 in this window.** It is inert until a hold is installed per org, and none is installed in this window. Its
mirror role is not provisioned here: until it is, controls of a SEALED org are refused, and no org is sealed.

## 4. What the executor records

- Khaliq's message id and reference.
- G as observed, H as derived.
- The window row.
- The plan and apply runs.
- Every consumer's image and readback, and the dead-letter counts before and after.
- The rebind run.
- Each Worker deploy readback.

Hashes and ids only: no credentials, and no production identifiers in this public repository. Operator notes go to
the private ops repository.
