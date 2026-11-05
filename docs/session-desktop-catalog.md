# Scoped Desktop catalog metadata

The additive 20261105022000 migration exposes one claim-bound `session_directory_desktop_catalog(workspace,harness,session)` RPC to the existing search Worker role. It grants no table access and uses the existing JWT issuer/audience/tenant/role policy. The selected RHC catalog route serves only the audited Desktop metadata fields; it does not port unused firstPrompt/marker/commit-link enrichment or fan out to parent transcripts.

`modelsComplete:true` means the deduplicated union of complete catalog_models and171event_models, including a valid empty union. The old catalog writer silently retained only64models; the new writer records modelsComplete=false on overflow, rejected/changed strings or old writer input. An incomplete union refuses instead of acknowledging a partial array.

Parent/child IDs and only the needed childModel/childAgentName/childAgentType are stored as metadata. Real projected values are preserved; no guardian/model metadata is guessed from prompt text. The old500-edge cap can hide an incoming parent entirely, so the read RPC requires the workspace relationship index to be complete. The new writer explicitly records truncation/missing metadata; any unready live row blocks this readmodel. A source with >500outgoing edges needs a complete metadata representation before this gate can pass. This is an explicit readiness limit, not an empty-parent fallback.

Each catalog marker binds to the directory event epoch and catalog projection timestamp. Old/mixed writer output cannot reuse the prior readiness marker. Existing rows default unready; controlled complete-source reprojection is required after schema/code installation. Tombstones and hard deletes retain existing companion erasure semantics. The RPC refuses foreign claims, dirty/unready171metadata and stale/incomplete catalog markers; it returns authenticated absence only for a missing live logical row. It hashes the bounded returned metadata snapshot; RHC rechecks that fingerprint before responding.

The Supabase function ACL manifest, frozen event-trigger body/hash, worker grant verifier and migration sentinel are updated together. Rollbacks run 20261105022000 before 20261105020000 before 171 (after any later 20261105023000 rollback). Before the 20261105022000 rollback, disable selected metadata readers and retire code that writes the new columns; the down file is not an authorization to change production schema. No deployment, role creation or production access is included.

Validation: 6 new PGlite tests in `src/hoopdb/__tests__/session-desktop-catalog.test.ts` (actual SQL, worker role) and root TypeScript. The other controls are not in that file: existing list/catalog writer compatibility and the reverse-order up/down/up (20261105023000, then 20261105022000, 20261105020000, 20261104021000 down, reapplied in order) live in `src/hoopdb/__tests__/session-directory-list.test.ts`; the exact SECURITY DEFINER/ACL manifest and frozen trigger hash checks live in `src/hoopdb/__tests__/migration-profile.test.ts`. Omitting171event_models makes1model-union control fail; removing workspace edge readiness makes1parent-absence control fail. The separate RHC route has5newtests including actual caller shape, empty complete union, foreign/incorrect edges, changed fingerprint, absence and no-Neon controls. Independent review, applied schema/backfill, all production source capacity and the coordinating operator gates remain required.

## Activation blockers (adversarial review 2026-11-05)

The catalog must not be served until these are resolved:

1. **Tombstone-then-resurrect leaves a parent reading ready with the edge gone.** The forget trigger in 20261101050000 deletes the edge without marking the parent incomplete; the writer's edges CTE drops tombstoned children yet records `relationshipsComplete=true`; and nothing re-projects the parent. The fix needs both the trigger and the writer to mark affected live parents incomplete, plus a re-queue path. That is a new migration (slot allocated via the registry) before the catalog is served.
2. **One bad session blocks the whole workspace indefinitely.** A single session with >500 outgoing edges, an over-long or control-character child label, or an invalid relationship id makes the whole workspace's catalog refuse with no bound on duration.
3. **The writer has no epoch check of its own.** It relies on the single caller's `expectedEpoch` check in the same transaction; any other caller would bypass it.
4. **>500 incoming parents returns a truncated success** rather than a refusal.

### Writer switch: `SESSION_DESKTOP_CATALOG_ENABLED`

Flag off = pre-#182 writer; flag on = complete catalog with 20261105022000 columns. The search stage (`src/session-pipeline/search-index.ts`) picks its catalog writer with `directoryCatalogWriter` (`src/hoopdb/session-directory-catalog.ts`) from the session pipeline Lambda's `SESSION_DESKTOP_CATALOG_ENABLED`. Only the literal `"1"` turns it on; unset or any other value is off, with no error.

- **Off (default):** `writeLegacyDirectoryCatalog`, the writer as it was before #182 (0c12036, unchanged until a49219d^1). It writes the same columns, values and query shape: agent names, catalog models, project key and method, initial commit, originator, agent version, project id, repo, branch, the scalar agent and search text, `catalog_projected_at`, agent rows, and parent -> child edges (uid and child id only). It writes none of 20261105022000's columns (no `child_*` edge metadata, no `catalog_read_*`, no completeness flags) and runs on a schema without that migration.
- **On:** `writeDirectoryCatalog`, #182's complete desktop catalog writer, which needs 20261105022000 and records the readiness markers this read model checks.

`SEARCH_INDEX_ENABLED=1` alone must never select the desktop writer: the search stage is live in production and must keep writing exactly what it wrote before #182.

The stack (`stacks/aws/session-pipeline.ts`) always sets `SESSION_DESKTOP_CATALOG_ENABLED="0"` and refuses `transcriptStore.sessionPipeline.desktopCatalog: true` ("desktopCatalog requires the forget/reproject fix (migration 20261105031000) and its review; not wired yet"). Activation is: land and review the forget/reproject fix (follow-up migration 20261105031000, blocker 1 above), resolve or accept the other blockers, then lift the stack refusal and wire the env to the input in a reviewed PR, and only then flip the flag. Applying migrations and deploying still need the owner's separate approval.
