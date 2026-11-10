# Dormant legacy canonical evidence

The continuous exporter can explicitly select `S3X_INCLUDE_LEGACY_SOURCES=true`. Default remains false. No running exporter or eraser is changed by this patch.

The optional path discovers conversation-turn/link/outcome-only groups, includes their workspace/source units, pages each table by its source primary key and writes scoped legacy evidence records through the existing scrub/seal/export pipeline. Legacy envelope IDs are namespaced by table and the raw source primary key stays in the payload. Legacy rows have their own normalization domain, with no fabricated delivery acceptance order. This prevents both cross-table IDs and a marker-shaped delivery ID from suppressing one another. It preserves original IDs (including64bit link IDs as strings), timestamps, payload metadata and source fields. A `legacy_scope` record states version1/all3table coverage inside the hashed canonical raw stream. Empty tables are explicit; without that marker, readers report coverage unknown. Legacy changes are forced through each activity sweep because updates need not bump session_revisions.

The canonical provider can return `conversation_turns`, `session_links` and `session_outcomes` table pages, plus legacyCoverage on every response. It validates every row's org/workspace/source/session against the authenticated manifest scope before returning it. Existing projection remains pipeline-owned and unchanged. These records are source prerequisites; public derived route migration is separate.

The eraser recognizes legacy evidence only with `S3X_ERASER_RECOGNIZES_LEGACY=true` (default false). Without it the existence guard counts only rollup/catalog rows, exactly as before this patch, so an eraser without the switch could remove a valid legacy-only export. Enable `S3X_ERASER_RECOGNIZES_LEGACY` on the eraser before enabling `S3X_INCLUDE_LEGACY_SOURCES` on the exporter; the exporter flag does not enable it. Do not restart competing processes as part of this source change. Existing erasure ownership/keys and deletion fences remain unchanged.

Before activation, also prove the reader role can SELECT all3tables, complete/reconcile legacy exports and enforce the future legacy writer fence or redirection. Current source export bounds/scrubbing and the16MiB/100k readWorker capacity limit remain explicit gates. A raw coverage marker states what was scanned, not that future Neon writes are impossible. No credentials, schema grants, deployment, flag or production data action is included.

## Activation gates (adversarial review 2026-11-05)

Resolve each of these (numbered as in the review) before `S3X_INCLUDE_LEGACY_SOURCES` is enabled. Detail and file references are in the [PR #176 review comment](https://github.com/AgentWorkforce/sessions-pipeline/pull/176#issuecomment-6654719392); finding 7 (the eraser guard) is fixed by `S3X_ERASER_RECOGNIZES_LEGACY` above.

- (1) One unattributable turn (stored source = session_owner, no metadata, no events) makes `assertBound` fail the whole legacy page/group and crash-loops the sweep instead of marking that scope unbound.
- (2) `seed-proof.ts` rejects `legacy_*` kinds (`seed_record_kind_unsupported`), and `includeLegacyUnits` moves `firstMs`, so manifest keys change for ordinary sessions.
- (3) `forceChanged` on every legacy page re-exports every legacy group each sweep, and `legacyActivityPage` re-evaluates the three-table union across all tenants before paging.
- (4) Legacy records bypass cross-origin acceptance and per-manifest owner binding and all carry revision "1", so a same-scope forged or stale copy replaces content or silently empties the legacy rows.
- (5) `legacy-turn-source` hard-codes event workspace = turn workspace, unlike the public oracle's `p_event_workspace`.
- (6) Canonical-legacy validation rejects schema-legal values (empty `link_ref`, negative `turn_index`) with a plain `Error` that fails the whole read.
- (8) Legacy-only manifests get user "unknown" and the `legacy:records` placeholder machine id.
- (9) The stored-source tiebreak uses byte order, and the read worker ignores `order=desc` for legacy tables.
