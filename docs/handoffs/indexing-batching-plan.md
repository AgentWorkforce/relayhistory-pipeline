# Hosted indexing latency: batching plan

Goal: a session readable and searchable by agents within about 30 s of its upload commit, for every org.
This plan covers the session-events queue and its Lambda event source mapping (ESM). Code: the stage
settings `deliveryDelaySeconds`, `batchingWindowSeconds`, `maxConcurrency` (stacks/aws/session-pipeline.ts)
and `busyRequeueDelaySeconds` (SESSION_PIPELINE_BUSY_REQUEUE_DELAY_SECONDS). All default to today's values.

## Measured floor (hosted, queue idle, 2026-11-25 05:25-05:50Z)

| Path | p50 | p90 |
|---|---|---|
| Pointer indexed (event age) | 53 s | 389 s |
| Search reconciled (event age) | 196 s | 452 s |

- About 50 s of the pointer p50 is fixed: a 30 s queue delivery delay plus a batching window of up to 20 s.
- Search lags the pointer mainly through scope_busy requeues: about 40% of search attempts, each with a 60 s delay.
- The p90 tail comes from long invocations (streamed scopes of about 234 s, acceptance-missing reconciles at p50
  27 s and p99 about 3 min) that fill all of the concurrency.

## 30 s budget (target, p50)

| Step | Today | Target |
|---|---|---|
| Queue delivery delay | 30 s | 5 s |
| ESM batching window | up to 20 s | up to 2 s |
| ESM poll (long poll, idle queue) | up to 20 s receive wait, about 1 s when busy | unchanged |
| Pointer stage | about 0.7 s | unchanged |
| Search reconcile (no collision) | about 0.4 s | unchanged |
| scope_busy retry | 60 s per collision | 20 s, then 10 s (busy requeue setting) |

The p90 under 30 s also needs work outside this plan: incremental indexing, the acceptance-missing fix, and
moving streamed scopes off the shared concurrency.

## Steps (one at a time; observe each before the next)

1. Queue delay 30 to 5 s, and window 20 to 2 s. Concurrency unchanged (20).
2. Busy requeue 60 to 20 s (needs the deploy that carries the setting). After a clean hour, 10 s.
3. Concurrency above 20 only after the connection budget below is proven.

Observation window: at least 60 min per step, with latency reports at +30 and +60 min.

## Connection budget (blocks step 3)

Each invocation holds up to HOOPDB_POOL_MAX = 4 connections. The pooler runs in session mode: one server
connection per client connection. At concurrency 20, the pipeline role was seen holding 44 server connections
(2026-11-25 06:5xZ), 21 of them idle in transaction. Postgres max_connections is 240; about 76 were in use in total.
The pooler's own pool size for this role is NOT known. A connect stall that timed out six invocations at
06:25-06:32Z is consistent with pool exhaustion.

Before raising the cap:
- read the pooler pool size;
- confirm 4 x the new cap, plus the other roles, fits under it with 25% headroom;
- add a connect timeout to the pipeline pool, so a stall fails fast and the message retries instead of
  holding the invocation until its 300 s limit.

The stack bound allows 30 so the setting exists; production does not start at 30.

## Rollback values and stop lines

Rollback for every step is today's values: delay 30, window 20, concurrency 20, busy requeue unset (60 s). Roll back
everything at once if any of these hold:
- hard ingest DB refusals above 1% over 10 min;
- ingest busy+unavailable more than 5 points above the pre-change sample over 10 min;
- DB lock waits above 8 per poll on two polls in a row;
- Lambda errors (including timeouts) above 10 per 10 min;
- DLQ growth above 20;
- main-queue oldest-message age rising for 15 min;
- for step 2: scope_busy requeues above 3x the baseline rate for 10 min.
