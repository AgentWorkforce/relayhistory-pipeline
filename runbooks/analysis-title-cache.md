# Cached brief title metadata (dormant)

This runbook covers both halves of the cached Desktop brief title: the producer added by #245 (analysis source snapshot and index metadata) and the reader added by #181 (authenticated canonical analysis GET with view=title). Both are default off.

## Producer: title source query

The analysis source snapshot also captures the exact old Cloud sessionBriefTitles query (the original per-workspace/provider Cloud title query) in the same readonly repeatable-read transaction. Its SQL is generated from the pinned legacy Cloud export by scripts/sync-analysis-title-select.ts; --check verifies the artifact. It selects the latest resolved request with a completed kind=brief job, exact org/session join, requested job workspace/provider and evidenceWorkspaceIds exactly [workspace], ordered request.created_at DESC,request.id DESC. It intentionally does not add request.workspace filtering and is independent of the v1 fifty-request history window. JavaScript trim/nonempty behavior is preserved; no match or blank title is attested known null. A failed read never invents null.

## Index metadata and generation

SessionAnalysisIndex optionally carries paired cachedBriefTitle:{version:1,complete:true,title:string|null} and sourceSnapshotSha256. The latter hashes analysisSourceSnapshotJson(publicSnapshotSha256,title), which combines the unchanged public v1 snapshot digest with the title metadata. A six-argument analysisGenerationJson (the optional sixth input) uses the v2 generation domain; five-argument persisted v1 remains exact. Public v1 payload/hash, full v1 response bodies, virtual IDs, history and status are unchanged. Index SHA and SQL equal-timestamp index/generation comparison bind title-only changes without a metadata migration/RPC change.

Missing metadata is unavailable, distinct from known null: a known-null title is returned only when the readonly source query attests it, and a missing legacy attestation is unavailable.

## Reader: view=title

The authenticated canonical analysis GET accepts view=title. It reads only the SHA-bound compact analysis index, verifies the cachedBriefTitle attestation, composite source digest and v2 generation, then rechecks the exact current publication. No result graph is hydrated. Older v1 indices remain readable by the updated full-envelope reader, and old full GET wrappers remain valid on the new consumer. No title read calls Neon directly.

Claims retain their existing SQL/RPC contract. The generation formula is validated after the authenticated index is read because the composite digest is inside that index. Scope, reference/SHA and freshness guards precede graph reads, and the final claim must still match generation, source, index and snapshot fields. Any other consumer must validate the same paired fields, composite digest, generation, exact index SHA and final live claim.

## Deployment order

Deploy the updated reader, then the updated default-off publisher, then title metadata backfill/coverage and freshness verification, then the reviewed RHC/Cloud selection. New v2 publications require the updated reader, so compatible consumers must be deployed before this publisher is enabled. Existing analysis publication/refresh is still default off. Missing/stale publications fail closed while warming. No migration, grant, flag, deployment or production operation is performed by either change.

## Size bound

The index remains 256KiB. An oversized compact title explicitly refuses publication before writes and is never truncated or replaced with null. Titles under the necessary title-only bound can still refuse if the complete index exceeds 256KiB. The currently measured candidate titles fit the existing ceiling (#181), but population-wide max-title coverage still needs an aggregate measurement (#245). No universal/unbounded title parity or Worker memory claim is made.

## Producer verification (#245)

39 SQL/codec checks (162 assertions) cover exact source namespace/evidence/status/kind filters, request-workspace compatibility, latest-request ordering including blank-newest behavior, legal duplicate identities, no fifty-request cutoff, trim semantics, v1 hash preservation, v2 binding and oversized refusal. Reverting job.workspace filtering, imposing a fifty-row source window, or omitting the title generation digest each makes one discriminating check fail. Root typecheck and generator check pass. The actual leased Node source→S3/SQL claim fixture is captured separately and consumed by the Worker/Cloud reviewer; no production actions or additional grants are performed.

## Activation gate: history-lineage sessions (raise with #245)

Under main's 20261105026000_canonical_history_lineage.sql, sessions.capture_session_replay_pages returns blocked=true whenever the captured snapshot has history.required=true. Only sessions.session_directory_replay_pages clears that flag once history is ready (and the directory row is live). The analysis path in 20261105023000_session_analysis_publications.sql reads capture_session_replay_pages directly and treats blocked as fatal: publish_session_analysis_cache, session_analysis_cache_demand, claim_session_analysis_cache, the heartbeat/finish fences and session_directory_analysis_cache all refuse or report not current. A history-lineage session therefore never receives an analysis publication, and this reader answers analysis_cache_not_ready (503) for it indefinitely, for both the full view and view=title. This fails closed (no leak), but it is a coverage gap. Do not treat analysis coverage as complete for history-lineage sessions until #245's producer applies the same history-ready clear. Raise this with #245 before activation.
