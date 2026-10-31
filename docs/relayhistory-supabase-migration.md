# RelayHistory (Neon) to Supabase: critical path

> **Superseded in part by `docs/decisions/0002-relayhistory-onto-the-pipeline.md`
> (Khaliq, 2026-10-30).** Session content moves to S3 in the 96Bulls layout,
> and Supabase holds only the index. The content-table copy, catalog
> reprojection and rollup rebuild below are no longer the production path.
> The T0 gate and hold, fences, guards and deletion handling still apply. A
> revised plan for the export-based backfill follows 0002.

Updated 2026-10-30 by `96-no-bull-migration` (sf-mac-mini). This plan applies
`docs/decisions/0001-production-data-plane.md` and does not authorize any
production action. Moving data, changing secrets/IAM/DNS, deploying, merging
production-affecting PRs and cutover each need Khaliq's direct OK for the
exact action.

## Shape: cut over fresh data, backfill history (Khaliq, 2026-10-30)

New writes move to Supabase at a cutover instant **T0**. History from before
T0 is copied from Neon in the background afterwards. This replaces the
copy-everything-then-switch runbook (linuyc, uncommitted
`docs/relayhistory-cutover.md`, which targeted Aurora on EKS):

- **No Neon logical replication.** The backfill is a read-only, chunked,
  resumable copy over an ordinary connection. Neon's irreversible setting
  change and endpoint restart are avoided entirely.
- **Write fence shrinks to the switch.** Writes are held durably for the
  seconds it takes the Worker to move its database from Neon to Supabase,
  then replayed against Supabase in order. Every current writer is covered, and
  no single-host store is allowed. Small write-path state (tokens, deletion
  tombstones, cursors) is copied inside the hold. See
  `docs/relayhistory-t0-write-hold.md`.
- **The 485.6 GiB copy leaves the deadline.** Only the empty schema, indexes
  and the sequence fence have to exist on Supabase before T0.
- **Target database:** Supabase `sessions` (`pyamkrhoyszgzirqqffq`). AWS is S3
  only. Aurora/EKS are self-host proof and stay off this path.
- **API:** the existing Cloudflare Worker. A driver seam swaps its Neon HTTP
  driver (`packages/relayhistory/src/db/client.ts`) for node-postgres over
  TCP (`cloudflare:sockets`) straight to Supabase's Supavisor pooler.
  Hyperdrive is not required. It is an optional later optimization (warm
  pooled connections near the database), adopted only if the benchmark shows
  direct TCP losing to Neon HTTP on p95. Queues and crons stay on Cloudflare.
  helm-helper's node-postgres factory and row normalization carry over. The
  Node/Hono server and the SQLite hold do not.

## Correctness rules for the split

1. **No key collisions.** Before T0, every Supabase sequence starts well above
   Neon's current maximum (the sequence fence from `migration:verify
   --emit-sequence-fence`). Neon accepts no writes after T0.
2. **Backfill never overwrites fresh data.** It inserts with
   `ON CONFLICT DO NOTHING` on the primary key. Rows written after T0 always
   win.
3. **Triggers stay off for copies.** The backfill runs with
   `session_replication_role = replica` in its own session. The T0 copy set
   (mirror) does not: the Supabase admin may not set that parameter, so the
   mirror lands through the live triggers and refuses any table whose enabled
   triggers are not reviewed side-effect-free guards or that is in a foreign
   key (`prepareGuardedTargetSession`). It never disables a trigger. Historical
   rows fire no projections, revision feed, wakeups or brief jobs. Every
   table, derived ones included, is copied as Neon has it. Sessions that
   straddle T0 have their rollups and caches rebuilt afterwards through the
   live `*_rollup_dirty` triggers.
4. **Ordering.** Parents are backfilled before children, newest first, so
   recent sessions become complete soonest. A per-table watermark is
   checkpointed so the job can resume.
5. **Verification.** For each table, the pre-T0 key range is fingerprinted in
   Neon (now frozen) and compared with the same range in Supabase, so it is
   exact rather than a live-CDC approximation.

## Decisions still needed

1. **Reads of pre-T0 data during the backfill (decided 2026-10-30):** read
   from Neon. The fallback is read-only and covers only sessions or ranges from
   before T0 that the backfill hasn't reached yet; no write ever routes to Neon
   after T0. It is removed once the backfill passes the verifier thresholds.
2. **API transport (decided 2026-10-30):** direct TCP to Supavisor;
   Hyperdrive only if the benchmark justifies it.
3. **Supabase compute and disk size** for the eventual 485.6 GiB plus growth.
4. **Exact-action approvals**, each at its time: the Worker's Supabase
   database secret; the sequence fence; T0 cutover; starting the backfill.

## Critical path to T0

Status as of 2026-10-30 (late). Tool commands and gates are in
`docs/relayhistory-t0-runbook.md`.

| # | Lane | Owner | State | Exit evidence |
| --- | --- | --- | --- | --- |
| 1 | Decision record + this plan | 96-no-bull-migration | done | 0001 + this file |
| 2 | Schema parity + live-schema contract | 96-no-bull-migration | done; pinned to relayhistory-cloud #222 `7264a70` (independent PASS) | `relayhistory-schema-parity.test.ts`, `relayhistory-verify-schema.test.ts`; `migration:verify --mode=schema` with role mapping |
| 3 | Verifier and benchmarks (gate exit codes) | 96-no-bull-migration | done | `src/migration/__tests__` |
| 4 | Worker driver seam (TCP to Supavisor, read-only Neon fallback client) | helm-helper | relayhistory-cloud #225, on hold | Contract suite on PG17; Supavisor, TLS and cancellation evidence |
| 5 | 0067 agent index, Cloud #5835, filter-to-index contract | helm-chart-chief | #222 PASS at `7264a70`, not merged or applied | #222, 96BullSessions #27 |
| 6 | Cutover gate + durable hold/replay | 96-no-bull-migration (code), helm-chart-chief (fix lane) | relayhistory-cloud #224, on hold | Gate and hold tests in Miniflare; per-org ordering, zero-write hold |
| 6a | Cloud direct Neon calls behind a flag | helm-chart-chief | Cloud #5850, code PASS, held | Draft PR, flag off |
| 6b | Runner, rollouts and checks over TCP | 96-no-bull-migration | relayhistory-cloud #223, CI green | Runner builds the full schema on PG17 over TCP |
| 6c | T0 copy set, sequence fence, Neon write fence | 96-no-bull-migration | done (tools); sizes need A1 | `migration:copy --mode=mirror`, `migration:sequence-fence`, `migration:neon-fence` |
| 8 | T0 runbook + rollback | 96-no-bull-migration | done | Runbook; `migration:reverse` |

**T0 gate (mandatory):** the parity and live-schema tests above, green
against the legacy migrations pinned at the commit being cut over, plus
`migration:verify --mode=schema` and `migration:sequence-fence` passing
against the real databases.

## After T0

| # | Lane | Owner | Exit evidence |
| --- | --- | --- | --- |
| 9 | Resumable backfill (newest first, deletion-fenced, plan-bound checkpoints) | 96-no-bull-migration | `migration:copy --mode=check` exits 0 |
| 10 | Rebuild straddling rollups; reproject the catalog | 96-no-bull-migration | `migration:rebuild`, `migration:reproject` (idempotent) |
| 11 | Neon retirement | separate approval | Final verification report archived |

## Rollback

Before T0: nothing changes. After T0, Supabase holds the only copy of fresh
writes. Rolling the Worker back to Neon therefore needs those writes copied
back first: the same copier run in reverse for keys above the sequence fence,
under a switch hold. Neon is never simply repointed.

## Open technical checks

- The Worker's Supabase role: `sessions.machines` has RLS (96Bulls policy), so
  the RelayHistory runtime role must own the table or have `BYPASSRLS`, and it
  needs the grants relayhistory-cloud's runner issues to its runtime role.
- Machines that only the pipeline has seen get `workspace_id = 'default'`, so
  RelayHistory's workspace-scoped reads don't list them.
- Live catalog parity, including out-of-band indexes and the 0029 rollout
  triggers, is checked by the read-only verifier and helm-chart-chief's #27,
  not by the offline test.

- Writer inventory: every Worker route, queue consumer and cron that writes,
  plus any non-Worker writer to Neon.
- Supavisor transaction mode and the Worker's transactions and prepared
  statements; connection counts at peak.
- Backfill throughput at the chosen Supabase compute, and its impact on live
  p95.

## Driver contract (lane 4 tests and the T0 runbook must make these explicit)

- Supavisor pool mode and port (transaction mode on 6543 vs. session mode on
  5432), and `sslmode=verify-full` with Supabase's CA.
- `nodejs_compat` on the Worker, and the connection lifecycle: open one
  client per request, close it in `ctx.waitUntil`, and never share it across
  requests.
- Timeouts and retry: connect and statement timeouts, retrying only
  idempotent reads, and mapping Neon HTTP's batch `transaction([...])` to real
  `BEGIN`/`COMMIT`.
- No named prepared statements under transaction mode.
- Neon fallback: a read-only Neon client type that has no write methods, plus
  a runtime guard that rejects any non-`SELECT` statement. The fallback is
  removed automatically when the verifier reports every pre-T0 range equal.
