# Release plan: slot 062320 (per-org admission hold)

Design: `docs/design/org-admission-hold.md` (revision 4, joint APPROVE at 892f089e). Code only. Applying the slot to
any real database, installing a hold, and provisioning the `bulls96_session_ingest_mirror` role each need Khaliq's
explicit OK in his own words. A relayed approval counts only as AGENTS.md "Relayed approvals" allows.

## Generation: the standard G+H overlap is required

Applying 062320 moves the role-validation marker to a new schema generation H. Every schema-gated consumer pinned
to the current G refuses a G-only marker change: `ingest_require_generation` raises 55000 on every gated write. So the
release follows `runbooks/guarded-session-schema-release.md`, "Buffered ingest: required order", with the G+H overlap
contract: `ingest_generation_overlap_admits` admits G during the transition. The order:
1. Deploy every consumer below at G+H (accepting both).
2. Open the overlap window.
3. Apply 062320.
4. Rebind G -> H.
5. Rebuild every consumer at H-only, then close the window.

## Generations (candidate, integration head)

Re-derive each value at the release sha before approval. These are the values at the integration head
`ec9c9448` (main `8840659b` plus 062320).

| | Value | How |
|---|---|---|
| H (target) | `a9662ebac63a3487e64065d0d99a0fb819a2f5058e2f29df421403c8592e753c` | `bun src/hoopdb/migrate.ts --generation` at the integration head |
| main's tree without 062320 | `f31d73e2a286e712175fa8d4417f07895702012f80c1b3ff88a210381418e44a` | the same command at main `8840659b` (includes 062300) |
| Worker's tracked overlap pair | `3eef01d7…` -> `778af686…` | `PRIVATE_INGEST_OVERLAP` in `private-ingest-release.ts` |

G is the generation the live marker and policy hold at release time, observed by the release owner and not taken from
the repository. If the live G is `778af686…` (the 062220 release), then 062300 is unapplied, and a release to this H
carries 062300 and 062320 together. Release 062300 first, or plan both in one reviewed window. The release owner
decides.

## Where the approved overlap is recorded

The release owner records the approved overlap durably in two places, both before any marker transition:
- **The database window.** `sessions.ingest_generation_overlap_windows` (044030) holds one row per window, opened by
  the overlap operator (`src/hoopdb/ingest-generation-overlap-operator.ts`). The row carries `from_generation` (G),
  `to_generation` (H), `admit_during_apply`, `approval_sha256` (the hash of the approval text), `opened_by`,
  `opened_at` and `expires_at`, at most 4 hours after it opens. Its `closed_at` and `closed_reason`
  (`rebind` | `owner` | `expired`) record the close.
- **The Worker overlap pair.** `PRIVATE_INGEST_OVERLAP` in `src/session-ingest/private-ingest-release.ts` records the
  reviewed G and H pair for the private-ingest Worker, through tracked-profile deploys G -> G+H -> H. A rollback is
  G+H -> G.

Scope and expiry must cover pointer writes, the 778a indexing Lambda's included, for the whole rollout and the
rollback path. If the rollout can outlast one window, the plan names each re-open before it starts. Hand this to the
release owner and sfmini together with the release sha, the G observed at release time, and the H derived from that
sha (`bun src/hoopdb/migrate.ts --generation`).

## Schema-gated consumer inventory

Every consumer must be rebuilt at H before H-only. The release owner confirms each row against the live deployments.
Deployment names per environment live in the infra repository, not here.

| Consumer | Where the generation is pinned | Notes |
|---|---|---|
| private-ingest Worker (96bulls-session-ingest) | `expectedGeneration` / `INGEST_ACCEPT_GENERATIONS` (`src/session-ingest/worker.ts`, `private-ingest-release.ts` `PRIVATE_INGEST_OVERLAP`) | the reviewed overlap pair moves G -> G+H -> H through tracked-profile deploys |
| sessions-pipeline indexing Lambda | `publicationSchema.expected.generation` (`src/session-pipeline/lambda.ts`) | calls `ingest_require_generation` on every pointer write; fails 55000 on every write without the overlap |
| sessions-pipeline feed Lambda | same runtime (`src/session-pipeline/lambda.ts`) | only if it is live in the environment |
| buffered GC job | `INGEST_GC_EXPECT_GENERATION` (`src/session-ingest/buffered-gc-job.ts`) | |
| schema-gated SERVICEs (`docker/entrypoint.sh`) | runbook line 46 | `session-pipeline-consumer`, `bob-costas`, `session-brief`, `referee`, `alleyoop-ingest`, `hoopdb-indexer` |
| canonical-project-events relay | `INGEST_EXPECT_GENERATION` (`src/canonical-project-events/deployment.ts`) | |
| publication job | `INGEST_PUBLISH_EXPECT_GENERATION` (`src/session-ingest/publication-runtime.ts`, `compiled_generation_mismatch`) | |
| Neon-to-S3 erasure babysitter, retention phase | `INGEST_EXPECT_GENERATION` (`src/migration/neon-s3-erasure/babysitter-phase.ts`) | only when its mode is not `off` |
| scoped publication RPC (proposal 062280, not landed) | `expected.generation` (`src/session-ingest/publication-scoped-rpc.ts`) | lands after 062320 only (see below) |

Any consumer not listed that reads `ingest_require_generation` or the role-validation marker must be added before the
plan is approved. Re-grep `expectedGeneration|EXPECT_GENERATION|ingest_require_generation` at the release sha.

## Additional 062320 steps (after H is live)

- **No hold is installed by the release.** Holds are installed per org by the cutover tooling (backlog-ops), each
  under its own approval.
- **Mirror role.** Provisioning the NOLOGIN role `bulls96_session_ingest_mirror`, and the membership of the Worker's
  mirrored control login, is a separate, approved provisioning step. Until it exists, every selection, reset or ordered
  control of a SEALED org is refused (fail closed); unsealed orgs are unaffected.
- **Rollback.** The generated down refuses while any hold is not released or a later slot is applied. It resets
  readiness to pending and retains the hold history.

## Install preconditions (per org, checked by the cutover tooling before `_install`)

- No pending batch: `_install` refuses on any (RH042).
- No canonical fanout outstanding for the org. Buffered production orgs refuse canonical claims, so none should
  exist. A committed canonical batch whose fanout is still pending would retry RH042 until release
  (`ingest_fanout_fence` is admission-class).
- Every lifecycle step carries its bound receipt's sha256:
  - `_seal`: the FULL backfill-execute receipt.
  - `_marker_attempt`: the final catch-up receipt.
  - `_release`: the terminal receipt, plus the erasure count read under the replay.

  NULL evidence is refused.

## Pending reconciliation before landing alongside other work

- `supabase/proposals/continuous-publication/062280-scoped-kernels` (layered-sessions-pipeline) pins the canonical
  publication bodies that 062320 replaces. It must land after 062320, re-pinned to its bodies so the copied kernels
  keep the hold check, or give its scoped entry points their own publication-class check. The classification test
  refuses any runtime-granted function left unclassified.
