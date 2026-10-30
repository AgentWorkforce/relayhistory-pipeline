# RelayHistory (Neon) to Supabase: critical path

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
3. **Sessions that straddle T0** have rows in both databases. Derived tables
   (rollups, catalogs, agent index, search projections) are recomputed for
   every touched session once its history lands, not copied blindly.
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

| # | Lane | Owner @ machine | State | Exit evidence |
| --- | --- | --- | --- | --- |
| 1 | Decision record + this plan | 96-no-bull-migration @ sf-mac-mini | done (local) | 0001 + this file on `feat/supabase-production-migration` |
| 2 | Schema/index parity test (relayhistory-cloud migrations vs. Supabase, including 0067) | 96-no-bull-migration @ sf-mac-mini | green at relayhistory-cloud 7343404; re-pin when 0067 (#222) lands | `packages/pipeline/test/relayhistory-schema-parity.test.ts` green in both apply orders |
| 3 | Port migrator + verifier + benchmarks from the linuyc dirty checkout into reviewed commits | 96-no-bull-migration @ sf-mac-mini | todo | Focused tests green on the clean branch |
| 4 | Worker driver seam: TCP node-postgres behind a flag, Neon HTTP default; pre-T0 read-only Neon fallback; legacy contract suite on PostgreSQL 17 | helm-helper @ linuyc (proposed) | local partial | Contract suite green on PG17 for both drivers |
| 5 | 0067 agent index (push off `/tmp` as a draft PR), Cloud #5835, filter-to-index contract, read-only target readiness audit | helm-chart-chief @ linuyc (accepted) | in progress | 0067 draft PR; contract agreed with lane 2 |
| 6 | Durable switch hold + ordered replay covering every writer | 96-no-bull-migration (accountable until assigned) | design | Writer inventory; synthetic-tenant hold/replay proof |
| 7 | DB/API benchmarks, Neon HTTP vs. Supabase direct TCP | 96-no-bull-migration @ sf-mac-mini | code exists | Supabase p95 no worse than Neon on each critical query |
| 6a | Cloud: move its direct Neon calls (`request_session_analysis_v1`, the two acks) to Supabase behind a flag; tolerate missing Neon-era event IDs | unassigned (Cloud repo) | todo | Draft Cloud PR; flag off by default |
| 6b | Migration runner, rollouts and checks reach Supabase over TCP (node-postgres for non-Neon URLs) | 96-no-bull-migration @ sf-mac-mini | in progress | relayhistory-cloud draft PR; runner applies the full schema to PostgreSQL 17 |
| 6c | T0 copy set (auth tokens, deletion tombstones, cursors, delivery dedupe): measure sizes on Neon read-only, then copy inside the hold | 96-no-bull-migration | todo | Size report; copy rehearsal on a static clone |
| 8 | T0 runbook + rollback | 96-no-bull-migration @ sf-mac-mini | todo | Reviewed runbook (below) |

**T0 gate (mandatory):** `relayhistory-schema-parity.test.ts`, which covers
schema parity in both apply orders and the tenant permission contract on
`sessions.machines`, must be green against the legacy migrations re-pinned at
the commit being cut over, including 0067 from #222.

## After T0

| # | Lane | Owner | Exit evidence |
| --- | --- | --- | --- |
| 9 | Resumable backfill (newest first, `ON CONFLICT DO NOTHING`, watermarks, throttled) | 96-no-bull-migration (accountable until assigned) | Per-table range fingerprints equal |
| 10 | Recompute derived tables for straddling and backfilled sessions | same | Rollup/catalog/search parity on samples |
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
