# ClickHouse search ingest re-enable (2026-11-05)

Root cause: relay_search.transcript_events was fed only by relayhistory's Neon tail
(SEARCH_INGEST_ENABLED). Under s3_first, Neon trailed by ~24-30 h (replica lag), and after the
05:51Z cutover Neon is frozen, so nothing after 2026-11-04 09:05Z could reach ClickHouse. The
S3 path (this session pipeline's ClickHouse stage) was off (searchIndex false).

Fix (runbook runbooks/sessions-prod-session-pipeline.md step 7):
1. #227: the pipeline diff never deletes ClickHouse kinds S3 cannot regenerate (~87k
   tool_result/user/assistant/... rows from non-delivery ingest). ClickHouse held 0
   trajectory/commit_link rows, so the pre-5e498e0 re-export gate was vacuous.
2. SEARCH_INGEST_ENABLED deleted on relayhistory-api (Neon tail off).
3. searchIndex true, captured as a session pipeline update on deploy/search-index-on-20261105
   (566b086 + guard, plan 246ae49/ef0e294, read API noop) and applied 09:08Z.
4. Backfill: `backfill-keys.ts` lists one live manifest key per scope indexed since a time;
   `backfill-enqueue.py` enqueues {"reconcile":{"key":K}} throttled (10/2 s, pause >300 queued,
   stop on DLQ growth). Credentials: pipeline secret via Secrets Manager into RAM only.
5. Proof: `ch-freshness.py` (max ts past 2026-11-04 09:05Z).

Not covered: post-cutover buffered uploads (buffered-batches/) until publication (P4).
