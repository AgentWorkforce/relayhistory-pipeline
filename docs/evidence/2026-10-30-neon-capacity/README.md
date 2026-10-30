# Neon capacity change and restart impact, 2026-10-30

Neon project `divine-snow-84812652`, branch `production`
(`br-damp-mud-apbr6ifp`), endpoint `ep-floral-pond-apkvat38`. Khaliq approved
installing `pg_stat_statements` and raising capacity in session
`96-no-bull-migration`.

| Time (UTC) | Event |
| --- | --- |
| about 20:10 | Read-only load check (`load-query.mjs` → `load-results.json`): 91% cache hit, 1.35 TB temp spill, millions of dead tuples, 56 deadlocks |
| about 20:19 | `CREATE EXTENSION IF NOT EXISTS pg_stat_statements` (`pg-stat-statements-install.mjs`); 1.11 installed |
| about 20:31 | PATCH to fixed 32 CU refused (HTTP 400, plan limit 16 CU); no change |
| 20:31:43 | PATCH autoscaling min 8 / max 16 → min 16 / max 16 (HTTP 200) |
| 20:32:02 to 20:33:37 | Neon operations: start_compute 41a23f52 finished; prewarm_replica f407c6b9 cancelled; promote_replica 9e1a8f52 skipped; suspend_compute 50a4df0d finished; start_compute 7e3d8a09 finished |
| 20:33:29.23 | Postmaster start (compute restarted; not the hot hand-off) |
| about 20:36 | Health (`health-query.mjs`): connect 677 ms, shared_buffers 1012 MB, 7 active / 18 idle |
| about 20:38 | Impact (`impact-query.mjs` → `impact-results.json`) |

## Findings

- **Short degradation around 20:33. No full write stop seen:** `delivery_records` arrived every minute, with a minimum of 767 at 20:33, and `convergence_events` dipped to 176 at 20:33.
- **The decline started before the change:** throughput fell from 20:29, before the operation began at 20:31:43.
- **Unsent outbox (47) and unacked revision feed (142) predate the restart.**
- **Counts alone don't prove delivery continuity.** The per-origin revision check was deliberately not run while Neon is stressed.
- **Worker and Cloudflare error-window audit:** routed separately by helm-chart-chief.

## Rollback (not applied)

- Compute: PATCH min 8 / max 16 CU.
- Extension: `DROP EXTENSION pg_stat_statements`.

Queries are read-only, except the extension install. Connection strings came
from `neonctl` or 1Password at run time and are not recorded. `SHA256SUMS`
covers the queries and results as captured.
