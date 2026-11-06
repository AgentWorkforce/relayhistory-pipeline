# Hosted-ops public cleanup (2026-11-06)

## Purpose

Remove from the public tree the hosted-operations files that the frozen path map
classifies as `move` and whose bytes are already preserved, with provenance, in
the private companion repo `AgentWorkforce/sessions-pipeline-cloud` under
`hosted-ops/`. This is source preparation only: it changes no runtime, deploy,
permission, secret, flag or visibility setting, and it is not approval of any
switch.

## Exact pair

- Source commit: `97bc33f6462f62c57ed0165fdb893106cc6026ca`
- Path map commit: `dda92513072a422f9c8749e3c85d47c98c24b883`
  (`docs/hosted-ops/path-map-20261106.csv`, which carries the committed private
  destination for every `move` row)
- Preservation: `AgentWorkforce/sessions-pipeline-cloud#5`, branch
  `hosted-ops/preserve-20261106`, head
  `966e923736a9c2305b9b0881ade9d75035a1d96f`
  (`hosted-ops/preserve-manifest.csv`)

The map has 65 `move` rows. 46 are removed here, 16 are deliberately held, and
3 that the map flags `needs_reconciliation=true` are restored and held pending
a named-owner resolution (see below).

## Removed paths (46)

For each path below, the git blob at this branch's base, the map's `blob`, the
preservation manifest's `source_blob` and the blob at the manifest's
`destination_path` in the preservation head were verified identical before
removal (46 of 46 matched).

- `docs/activation/session-directory-parity-reprojection.md`
- `docs/activation/supabase-g6-session-directory-activation.md`
- `docs/migration-ec2-runner-plan.md`
- `docs/private-ingest-operator.md`
- `docs/private-state-bootstrap-stdio.md`
- `docs/private-state-bootstrap.md`
- `docs/supabase-owner-operator-apply.md`
- `docs/supabase-owner-operator-plan.md`
- `scripts/ops/cutover-20261105/custody-capture.ts`
- `scripts/ops/cutover-20261105/cutover-ceremony.README.md`
- `scripts/ops/cutover-20261105/cutover-ceremony.ts`
- `scripts/ops/cutover-20261105/drain-apply-0078.mjs`
- `scripts/ops/cutover-20261105/drain-proof-creds.py`
- `scripts/ops/cutover-20261105/ingest-login.ts`
- `scripts/ops/cutover-20261105/ledger-check.ts`
- `scripts/ops/cutover-20261105/legacy-drain-proof.README.md`
- `scripts/ops/cutover-20261105/legacy-drain-proof.mjs`
- `scripts/ops/cutover-20261105/members.ts`
- `scripts/ops/cutover-20261105/mini/admit-new-orgs.sh`
- `scripts/ops/cutover-20261105/mini/after-1password-unlock.py`
- `scripts/ops/cutover-20261105/mini/cf-vars.py`
- `scripts/ops/cutover-20261105/mini/launchd/dev.agentworkforce.bulls-admit-new-orgs.plist`
- `scripts/ops/cutover-20261105/mini/neon-creds.py`
- `scripts/ops/cutover-20261105/mini/op-shim.sh`
- `scripts/ops/cutover-20261105/mini/replica-concurrency.py`
- `scripts/ops/cutover-20261105/mini/run-ceremony.py`
- `scripts/ops/cutover-20261105/mini/run-drain.py`
- `scripts/ops/cutover-20261105/neon-authtime.ts`
- `scripts/ops/cutover-20261105/neon-holders.ts`
- `scripts/ops/cutover-20261105/neon-live.ts`
- `scripts/ops/cutover-20261105/neon-post0078.ts`
- `scripts/ops/cutover-20261105/neon-rates.ts`
- `scripts/ops/cutover-20261105/neon-ro.ts`
- `scripts/ops/cutover-20261105/neon-role.ts`
- `scripts/ops/cutover-20261105/neon-sample.ts`
- `scripts/ops/cutover-20261105/neon-slow.ts`
- `scripts/ops/cutover-20261105/neon-top.ts`
- `scripts/ops/cutover-20261105/neon-who.ts`
- `scripts/ops/search-ingest-20261105/README.md`
- `scripts/ops/search-ingest-20261105/backfill-enqueue.py`
- `scripts/ops/search-ingest-20261105/backfill-keys.ts`
- `scripts/ops/search-ingest-20261105/ch-freshness.py`
- `scripts/ops/search-ingest-20261105/ch-kinds.py`
- `scripts/ops/search-ingest-20261105/ch-trajectory-sessions.py`
- `scripts/state-bootstrap-broker.py`
- `scripts/test_state_bootstrap_broker.py`

## Held paths (16), unchanged and byte-identical

Q3 operator reference and rollback docs, held for the operator:

- `docs/relayhistory-pipeline-migration.md`
- `docs/relayhistory-supabase-migration.md`
- `docs/relayhistory-switch-first-dag.md`
- `docs/relayhistory-t0-runbook.md`
- `docs/relayhistory-t0-write-hold.md`
- `docs/runbooks/buffered-cutover-20261105.md`
- `runbooks/neon-s3-sync.md`
- `runbooks/session-ingest-rollout.md`
- `runbooks/sessions-prod-session-pipeline.md`
- `runbooks/sessions-prod-session-read-api.md`
- `runbooks/sessions-prod-session-read-worker.md`
- `runbooks/sessions-prod-transcript-store.md`

Live-caller workflows, held until the reviewed switch (the originals stay
active until then):

- `.github/workflows/apply-session-schema.yml`
- `.github/workflows/deploy-session-read-worker.yml`
- `.github/workflows/publish-image-ar.yml`
- `.github/workflows/publish-image-ecr.yml`

The held `docs/runbooks/buffered-cutover-20261105.md` still names scripts from
the removed `scripts/ops/cutover-20261105/` directory. It is left untouched
because it is held; those scripts are preserved in the private companion.

## Restored needs_reconciliation rows (3), held pending a named owner

The first draft removed these three `move` rows although the frozen map still
marks them `needs_reconciliation=true`. They are restored here byte-identical
from the base commit, stay public, and are held until a named owner resolves
them. No decision about them is implied by this cleanup.

| path | owner |
| --- | --- |
| `docs/supabase-access.md` | unassigned, see spc issue #4 |
| `scripts/ingest-generation-rebind.ts` | unassigned, see spc issue #4 |
| `scripts/session-directory-title-backfill.ts` | unassigned, see spc issue #4 |

After this change no `needs_reconciliation=true` row of the map is in the
deletion set.

## Open operator gate

- The held, historical ("as run") `docs/runbooks/buffered-cutover-20261105.md`
  names ceremony, drain and admission helpers from the removed
  `scripts/ops/cutover-20261105/` directory. It is not edited. Pending:
  migration-handover-agent to confirm the pinned replacement/source and
  rehearsal, or name paths to keep.

## Consumer edits

Public procedures stay self-contained: none requires a private CLI or a private
activation note.

- `docs/decisions/0001-production-data-plane.md`: the first draft's edit is
  reverted; its link to the restored `docs/supabase-access.md` is valid again.
- `docs/p1-frozen-source-custody.md`: the first draft's edit is reverted; the
  generation-rebind CLI it invokes (`scripts/ingest-generation-rebind.ts`) is
  restored.
- `docs/session-directory-list.md` (kept, rewritten): the pointer to the
  removed `docs/activation/session-directory-parity-reprojection.md` now points
  to the doc's own "Backfill" section, which is rewritten as target-neutral
  public instructions (invariants, preconditions, re-projection through the
  retained `projectRelayhistorySession`, `directoryCatalogFromProjection`,
  `writeDirectoryCatalog` and desktop-catalog re-projection driver,
  verification, activation and rollback). A private hosted copy is mentioned
  as optional context only.

No `package.json` script, tsconfig entry, import, Dockerfile, workflow step,
bundle list or `scripts/check-*` allowlist referenced a removed path. The removed
Python broker test (`scripts/test_state_bootstrap_broker.py`) only exercised the
removed broker and was not run by CI.

## Base drift

This branch is based on `5bd61f12`. At review time `main` was `f12a963a`: 17
files differ from the base, none of them a `move` row, so the removed bytes are
unchanged on `main`. `main` has since moved to `795f5ddd`, which also changes
the held `move` row `runbooks/sessions-prod-transcript-store.md`. That row is
not removed here and the branch still merges cleanly, but its blob on `main` no
longer equals the map's blob and needs a refresh before it is ever moved.

## Retained unchanged

- Self-contained public checks and guidance, including
  `scripts/check-engine-identifiers.ts` and `scripts/aws-hardening-mutations.ts`.
- PG15/16/17 proofs and the `pg-proof` workflow.
- `versions.lock`, generation and verifier fixtures, compared production-plan
  fixtures.
- Migrations and `src/migration/`.
- `packages/session-read-worker/`.
- `publish-image.yml` and the image-release lane.
- Library modules `src/hoopdb/ingest-generation-rebind.ts` and
  `src/hoopdb/session-directory-title-backfill.ts`, and their restored driver
  scripts (above).

## Reviewer caveats

1. Amp pins: `provider-origin.json` hashes the provider source, not the CLI
   binary. `AMP_CLI_VERSION` is a separate constant.
2. The `PENDING_MOVE` exclusions in `scripts/check-engine-identifiers.ts` are
   guard exclusions only. They are not move authority and not disclosure
   clearance. The `scripts/ops/` prefix entry is left in place: it is a
   directory prefix rather than an exact removed path.
3. `src/shared/__tests__/engine-identifiers-guard.test.ts` keeps the fixture
   string `scripts/ops/cutover-20261105/members.ts` (a removed path) to exercise
   prefix matching; it is a string literal and reads no file.
4. The held runbook `docs/runbooks/buffered-cutover-20261105.md` keeps stale
   references to removed helpers (see "Open operator gate").

## Rollback

Revert this commit normally. The removed bytes also remain in git history and
in the private preservation copy.

## Status

HELD. This cleanup is a draft for review. It must not be merged without the
named reviewers, and it does not authorize any switch, deploy, runtime,
permission, visibility or disclosure action.
