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
