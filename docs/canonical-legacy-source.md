# Dormant legacy canonical evidence

The continuous exporter can explicitly select `S3X_INCLUDE_LEGACY_SOURCES=true`. Default remains false. No running exporter or eraser is changed by this patch.

The optional path discovers conversation-turn/link/outcome-only groups, includes their workspace/source units, pages each table by its source primary key and writes scoped legacy evidence records through the existing scrub/seal/export pipeline. It preserves original IDs (including64bit link IDs as strings), timestamps, payload metadata and source fields. A `legacy_scope` record states version1/all3table coverage inside the hashed canonical raw stream. Empty tables are explicit; without that marker, readers report coverage unknown. Legacy changes are forced through each activity sweep because updates need not bump session_revisions.

The canonical provider can return `conversation_turns`, `session_links` and `session_outcomes` table pages, plus legacyCoverage on every response. It validates every row's org/workspace/source/session against the authenticated manifest scope before returning it. Existing projection remains pipeline-owned and unchanged. These records are source prerequisites; public derived route migration is separate.

Deploy the updated eraser existence guard before enabling legacy export. An older eraser only recognizes rollup/catalog groups and could remove a valid legacy-only export. The new guard recognizes legacy evidence too. Do not restart competing processes as part of this source change. Existing erasure ownership/keys and deletion fences remain unchanged.

Before activation, also prove the reader role can SELECT all3tables, complete/reconcile legacy exports and enforce the future legacy writer fence or redirection. Current source export bounds/scrubbing and the16MiB/100k readWorker capacity limit remain explicit gates. A raw coverage marker states what was scanned, not that future Neon writes are impossible. No credentials, schema grants, deployment, flag or production data action is included.
