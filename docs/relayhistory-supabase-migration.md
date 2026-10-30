# RelayHistory (Neon) to Supabase: critical path

Updated 2026-10-30 by `96-no-bull-migration` (sf-mac-mini). This plan applies
`docs/decisions/0001-production-data-plane.md` and does not authorize any
production action. Enabling Neon logical replication, moving data, changing
secrets/IAM/DNS, deploying, merging production-affecting PRs and cutover each
need Khaliq's direct OK for the exact action.

## What changed from the Aurora runbook

The earlier runbook (linuyc, uncommitted `docs/relayhistory-cutover.md`) moved
Neon into private Aurora behind a new compatibility API on EKS. Under 0001,
production AWS is S3 only. That runbook therefore no longer holds:

- **Target database:** Supabase, not Aurora. Aurora/EKS remain self-host proof
  only and leave the critical path.
- **Compatibility API:** the existing Cloudflare Worker stays the API. Its
  Neon HTTP driver (`packages/relayhistory/src/db/client.ts`, `drizzle-orm/neon-http`)
  is replaced by a TCP driver through Cloudflare Hyperdrive to Supabase's
  direct or pooled endpoint. No EKS hop and no new public surface. The
  node-postgres factory and row-shape normalization on helm-helper's
  `feat/relayhistory-tcp-compatibility` carry over. The Node/Hono server and
  the SQLite write-hold do not.
- **Queues and crons:** these stay on Cloudflare (`SESSION_EVENTS` queue, DLQ,
  the per-minute cron). Nothing needs an AWS replacement.

Unchanged and reused: the schema-superset migrator, the read-only verifier
(`migration:verify`), the DB and HTTP benchmark harnesses, the sequence fence,
and the CDC sequence (publication on Neon, subscription on the target,
zero-lag gate, final full verify under a write hold).

## Decisions needed from Khaliq

1. **Target project (decided 2026-10-30):** Supabase `sessions`
   (`pyamkrhoyszgzirqqffq`). Its compute size and disk still need a decision,
   because the source is about 485.6 GiB of logical data.
2. **API path (open):** the Worker opens TCP to Supabase either directly to
   Supavisor through `cloudflare:sockets`, or through Hyperdrive (a pooled,
   warm connection near the database). The driver seam supports both, and the
   benchmark decides.
3. **Write fence (decided 2026-10-30):** a durable hold and replay. Writes are
   buffered durably, then replayed after the switch. Every current writer is
   covered, and no single-host store (such as the SQLite fixture) is allowed.
4. **Exact-action approvals**, each at its time: Neon logical replication;
   creating the Hyperdrive config and its credentials; the initial copy;
   cutover.

## Critical path

| # | Lane | Owner @ machine | State | Exit evidence |
| --- | --- | --- | --- | --- |
| 1 | Decision record + this plan | 96-no-bull-migration @ sf-mac-mini | done (local) | 0001 + this file on `feat/supabase-production-migration` |
| 2 | Schema/index parity test (relayhistory-cloud migrations vs. Supabase target, including 0067 agent index) | 96-no-bull-migration @ sf-mac-mini | next | Failing test listing every missing table/index, then green |
| 3 | Port migrator + verifier + benchmarks from the linuyc dirty checkout into reviewed commits; re-point docs from Aurora to Supabase | 96-no-bull-migration @ sf-mac-mini | todo | Focused tests green on the clean branch |
| 4 | Worker DB driver seam: Hyperdrive + node-postgres behind a flag, Neon HTTP stays default; legacy contract suite on PostgreSQL 17 | helm-helper @ linuyc (proposed) | local partial | Contract suite green on PG17 for both drivers |
| 5 | Agent-name index 0067 (`codex/session-agent-index-20261029`, unpushed in `/tmp`) + Cloud PR #5835 + filter-to-index contract + read-only target readiness audit (`/tmp/96bulls-prod-index-contract:fix/prod-supabase-index-contract`) | helm-chart-chief @ linuyc (accepted) | 0067 committed, not pushed; #5835 draft, green | 0067 pushed as a draft PR; contract agreed with lane 2 |
| 6 | Read-only source readiness audit: replica identity per updated table, extensions, PG versions, long transactions | 96-no-bull-migration (accountable until sessionspipelinegrok names an owner) | todo | Audit report with no secrets |
| 7 | Neon to Supabase CDC: prove the inbound mechanism and privileges on Supabase, publication/replica-identity coverage (including the agent catalog), initial-copy behaviour, lag monitoring | 96-no-bull-migration (accountable until assigned) | blocked on approvals | All relations synced, zero lag |
| 8 | Durable write hold + replay covering every current writer | 96-no-bull-migration (accountable until assigned) | design | Writer inventory; synthetic-tenant hold/replay proof; ordering and idempotency |
| 9 | Verification + DB/API benchmarks (Neon vs. Supabase) | 96-no-bull-migration @ sf-mac-mini | code exists | Parity true; Supabase p95 no worse than Neon on each critical query |
| 10 | Cutover + rollback runbook (Supabase) | 96-no-bull-migration @ sf-mac-mini | todo | Reviewed runbook; rollback keeps Neon read-only and never repoints after Supabase accepts writes |

Coordination channel: `sessionspipelinegrok`. Shared dirty checkouts on linuyc
are read-only sources. Reviewed hunks move into clean branches, never whole
checkouts.

## Open technical checks

- Whether Supabase lets the project's `postgres` role create a subscription to
  an external publisher (`pg_create_subscription` on PG17), and what limits it
  sets on worker and slot counts.
- Direct TCP vs. Hyperdrive: per-request latency against Neon HTTP, connection
  counts at peak, and how prepared statements and transactions behave through
  Supavisor's transaction mode.
- 485.6 GiB initial copy time into Supabase at the chosen compute size,
  rehearsed on a static clone first.
