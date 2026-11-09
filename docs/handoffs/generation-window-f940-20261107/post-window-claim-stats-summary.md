# Post-window claim statistics, one read-only observation

Receipt SHA-256 `4a9fd9da38b291cec3ca24bc848d2321a37a0ae0743e55a14c2f9503b384471a`, observation 2026-11-07T22:58:08Z–22:58:10Z, SQL `24fa86fe`. Same pinned existing-owner route, BEGIN READ ONLY/ROLLBACK, 3 s statement and 250 ms lock timeouts; no configuration/reset/EXPLAIN/application row query.

`pg_stat_statements.track=top`, `track_functions=none`. Nested statement and function statistics are unavailable. No setting was changed. Consequently this does not identify the inner SQL statement that waits.

Cumulative recorded successful claim calls: 252,317, mean 398.347 ms, maximum 4995.188 ms. Commit: 248,101 calls, mean 246.442 ms, max 4966.172 ms. Prepare stage: 248,841 calls, mean 84.654 ms, max 3847.890 ms. Slot acquire: 251,752 calls, mean 0.478 ms, max 11.533 ms. These are cumulative since reset, not a post-rebind delta; failed/timed-out work may be unrecorded. No root-cause conclusion follows solely from these totals.

Fresh 120 s observer `bc058ed2` after rebind: private 101/116 HTTP200, 12/116 HTTP503 (10.34%), 3 HTTP429. Public 64/78 HTTP200, 12/78 HTTP503 (15.38%), 2 HTTP429. No observed 55000/generation-not-ready. Private 503 classification: 8 lock timeouts 55P03 and 4 statement timeouts 57014, all stage claim. Samples are not tenant-correlated. This confirms resumed acceptance and remaining timeout failures, not a complete fix or canonical readback.

Full verbatim statistic rows are in post-window-claim-stats-result.json. A separate reviewed lock-wait observation can find current blocking shapes without enabling new statistics or exposing identifiers/query text; coordinate that with pipeline-lead, with no duplicate actor.
