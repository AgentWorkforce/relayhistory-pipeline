# 0007: Hosted sessions run on the agentrelay EKS clusters, on durable actors, with no Cloudflare

**Status:** accepted (Khaliq, 2026-11-13, in the relayhistory-pipeline session: "we essentially for aws
want to move to the architecture found in ../infra and move completely off of cloudflare, and also use
terseai/durable actors instead of the postgres implementation"). Each deploy, IAM change, data move and
account retirement below still needs his explicit OK, as AGENTS.md says.

**Builds on:** 0005 (upload accounting lives in S3). **Folds in:** the planned 0006 (a portable
summarizer and interceptor), which becomes the workspace actor described below.

## Context

The hosted sessions pipeline grew up AWS-first and Cloudflare-fronted. Today it spans:

- **Cloudflare:**
  - the ingest Worker (`stacks/cloudflare/session-ingest.ts`) and its admission and buffered Queues
  - the read API Worker (`packages/session-read-worker`) over Hyperdrive
  - the amp-transport Worker
  - outside this repo: the relayhistory-cloud Worker (where Desktops upload today) and the Cloud web
    Worker (the dashboard)
- **A dedicated sessions AWS account:**
  - the transcripts bucket
  - the session-pipeline and read-API Lambdas, plus the buffered-GC Lambda
  - SQS
- **A dedicated dev account:** its pipeline stack is unused. It hosts agent EC2 hosts and an old image
  registry.
- **Supabase:** the session index, the pipeline job queue (`pipeline_jobs`), summary pointers, briefs
  and the upload bookkeeping that 0005 removes.

Meanwhile the product itself already runs the way `../infra` describes. The agentrelay-dev and
agentrelay-prod accounts each have an EKS cluster, created from the infra repo's Terraform and deployed
with its `agent-relay` umbrella chart: relay-control, relay-webapp, relaycast, relaychief and relayfile.
In agentrelay-dev the sessions components already run as pods from the same chart:

- `session-ingest-api` and ingest-edge
- `read-api`
- the `session-summary-worker` (bob-costas)
- the `session-metadata-indexer` and `session-status-worker`
- session-brief and a flows worker

The dev summary worker writes model summaries. Production has none of these pods.

The infra repo is also moving stateful cores from Postgres to TerseAI durable-actors. Relaycast and
Relayfile each run one `Workspace` actor per workspace, behind a stateless gateway, with state in actor
SQLite and snapshots in S3 or GCS (`docs/self-hosted-durable-actors.md`). The decision there followed the
Postgres engine deadlocking under fan-out (`docs/relaycast-postgres-to-actors-migration.md`).
`values-eks-actors.yaml` already runs that control plane on EKS with S3 storage. agentrelay-prod's
cluster has an `actors` node group.

## Decision

1. **The infra repo is the AWS architecture.** Hosted sessions are the `agent-relay` umbrella's sessions
   components, deployed to the agentrelay-dev cluster (dev) and the agentrelay-prod cluster (prod) from
   `../infra`'s charts and Terraform. Dev and prod run the same chart, image and values shape. Self-host
   customers get the same thing on EKS or GKE.
2. **No Cloudflare.** Nothing on the sessions request path, upload path or work path runs on Cloudflare:
   no Workers, Queues, Hyperdrive or R2.
3. **No Lambdas for the pipeline.** The read API, upload admission and the workers run as pods. The
   sessions Lambdas are retired once nothing calls them.
4. **Durable actors replace the Postgres implementation for pipeline state.** Actors run on the
   durable-actors control plane that relaycast and relayfile use. They are **sharded so that no single
   actor is on a workspace's hot path**: one actor executes one call at a time. The Relaycast benchmark
   measured about 1.5-3 channel posts a second per workspace actor, against about 10-16 on Postgres,
   with ~80 s queue waits under overload (infra branch `bench/relaycast-postgres-vs-actors`,
   `docs/benchmarks/relaycast-postgres-vs-actors.md`). So:
   - **A `Session` actor per session** takes that session's appends and derived work: index row,
     summary, category and status. Writes arrive in batches (one per upload), each with an idempotency
     key, so a retried call never double-applies.
   - **A `Workspace` actor per workspace** holds only the aggregate: the directory, the running ledger,
     the live lanes for overlap detection, and the alert queue. It is fed small, batched updates from
     session actors, never raw events.
   - Actor memory is sized above the shipped 256 MiB (the benchmark peaked at 230-247 MiB), and the
     shared tier (gateway replicas, control plane, start-rate limits) is sized for the session count.

   Between them, these actors own each workspace's derived session state and work:
   - **Session directory and index:** today's Supabase `session_index` and `session_directory`, held
     in actor SQLite.
   - **Summaries and categories:** the actor runs the summarizer on new session data, writes the
     encrypted summary object next to the session in object storage (the existing format), and keeps
     the pointer in its own SQLite.
   - **Running ledger of work:** per workspace, tagged by human and by agent.
   - **Overlap detection and alerts (the interceptor):** live lanes are in the actor, so detection runs
     on new data. Alerts go to agents as Relay messages, by priority, and to humans as desktop
     notifications. The overlap and alert steps ship off by default, as
     `docs/coordination-live-advisories.md` requires, until a replay shows they are precise enough.
   - **Its own work queue:** actor alarms and invocations replace `pipeline_jobs` and the polling
     workers.
5. **Object storage stays the record.** Transcripts and manifests stay in the bucket, and so does 0005's
   upload ledger. 0005's S3 accounting needs no database, and actors don't change it. An upload
   becoming durable in S3 is the event that wakes the workspace's actor.
6. **Gateways stay stateless.** read-api and ingest-edge route each request to the right session or
   workspace actor by a verified credential, as the relaycast gateway does. Actors cannot call into the
   cluster (the sandbox NetworkPolicy allows only public egress and the control plane), so gateways
   push facts to actors and never the other way.
7. **No Neon.** The Neon path is removed completely: custody checks against Neon, the legacy-receipt
   reads, relayhistory-cloud's Neon reads and writes, and the Neon replica queue
   (`docs/neon-exit-edge-map.md`). Nothing on the sessions upload, read or work path may touch Neon.

## What moves where

| Today | Target |
| --- | --- |
| Ingest Worker plus Cloudflare Queues | ingest-edge pod; direct-to-S3 uploads (0004/0005) admitted by the ingest-edge consumer from SQS |
| relayhistory-cloud Worker (the Desktop upload front door) | direct-to-S3, with credentials from relay-control (already in agentrelay-prod) |
| Read API Worker or Lambda, over Hyperdrive | read-api pod, reading from the workspace actor (index, directory, summaries) and ClickHouse (search) |
| Session-pipeline Lambda (manifest event, index) | upload event, then the workspace actor |
| bob-costas, metadata-indexer and status-worker polling `pipeline_jobs` | the workspace actor (summaries, categories, status, ledger, overlap) |
| Supabase session index, directory, job queue, summary pointers | actor SQLite with snapshots in object storage |
| Cloud web Worker's 96Bulls read routes | the read-api pod (a Cloud change, outside this repo) |
| Sessions AWS accounts | agentrelay-dev and agentrelay-prod |

**Not decided here:**
- **ClickHouse full-text search** stays where it is. Whether search also moves to actors is a later
  decision.
- **Supabase's remaining roles** stay: identity and privacy reads, the privacy journal's SQL side, and
  the read-side deletion state until 0005's read-side move. Each leaves on its own decision.

## Phases

Each phase lands in dev first, then goes to prod on his OK.

1. **Finish 0005** (#580, #581, #579, #578). Unchanged.
2. **Summaries now, on the existing pods:**
   - Deploy the dev pod set's summary worker and indexer into agentrelay-prod, with the same chart and
     image as dev.
   - They point at the current transcripts bucket (cross-account, with a pod role, bucket policy and
     KMS grant) and the current Supabase.
   - This is a stopgap that still uses the Postgres queue. It exists so users get summaries before the
     actor work lands, and it is retired in phase 4. It is the only new Postgres use this decision
     allows.
3. **The read API and upload admission as pods in prod:**
   - Deploy read-api and ingest-edge into agentrelay-prod.
   - Point the Cloud dashboard's read routes at read-api.
   - Admit direct-to-S3 uploads through ingest-edge (0005's self-host path, already wired to the S3
     ledger), not the Lambda.
4. **The `Sessions` workspace actor:**
   - Build it in this repo on the durable-actors SDK: index and directory, summaries and categories,
     the work ledger, overlap and alerts.
   - Prove it in agentrelay-dev next to the existing workers.
   - Migrate each workspace's derived state with a full copy that never drops data, following the
     Relaycast precedent. A workspace's derived state can be rebuilt from object storage, so the
     migration can also be a rebuild.
   - Switch read-api to read from the actors.
   - Retire the Postgres job queue, the polling workers and the phase-2 stopgap.
5. **Off Neon:** remove every Neon read and write on the sessions paths (custody, legacy receipts,
   relayhistory-cloud's session tables, the replica queue). This needs the custody and privacy
   authority moved first, so it runs alongside phases 1 and 3.
6. **Off Cloudflare:** retire the ingest, read and amp-transport Workers, the Queues and Hyperdrive, then
   the relayhistory-cloud Worker. The Cloud web Worker follows the Cloud owners' plan.
7. **Consolidate the accounts:**
   - Give agentrelay-prod a sessions bucket in the transcript region, and copy the transcripts and the
     drained backlog with versions and re-encryption.
   - Repoint everything and run old and new side by side until reads match.
   - Retire the sessions Lambdas and the sessions-prod account.
   - Move the agent hosts out of sessions-dev, then retire it.

## Consequences and open points

- **Region.** Processing in the agentrelay-prod cluster's region is allowed (Khaliq, 2026-11-13). The
  transcript-region rule for stored content stays until phase 6 decides where the new bucket lives.
- **The durable-actors control plane still uses Postgres** (`credentialsSecret.postgres-url`) for its
  registry. That is the platform's own metadata, shared with relaycast and relayfile, not pipeline
  state.
- **Durable actors on EKS**, from the Relaycast move (infra#271/#272/#274):
  - **Images:** they need the AgentWorkforce fork's control-plane image (S3 provider). Mirror its images
    into the registry the nodes pull from.
  - **TLS to RDS:** the control plane needs a CA bundle of the system roots plus the RDS CA, with
    `sslmode=require`.
  - **Node group:** a dedicated actor node group, labelled and tainted, in one zone that matches
    `placement.zone`.
  - **Sockets:** set `DURABLE_ACTORS_SOCKET_ORIGIN` so the gateway can reach the control plane's socket
    path inside the VPC.
  - **Migration:** a full copy that never drops data. Each table is verified by count and digest, and a
    workspace goes live only on an explicit `open`.
- **Not yet measured for this workload:** per-actor SQLite size, snapshot size and time, and start
  latency at session scale (the benchmark measured 0.9 s median and 7.5 s p95 to create, and seconds
  after idle). Phase 4 starts with a sessions-shaped benchmark in agentrelay-dev before any design is
  final.
- **Portability.** Self-host stays on the infra repo's EKS and GKE profiles. durable-actors has GCS and
  S3 storage providers. Azure would need an Azure Blob provider in durable-actors and in this repo's
  object store, and is not planned.
- **Superseded in flight:** #574 (migration slot `060000`, a Postgres column for the long summary) is
  dropped (Khaliq, 2026-11-13). Long summaries are read from the encrypted summary object next to the
  session, which bob-costas already writes and the actors keep. The publication-frontier attester
  (slot `061000`) is held, because 0005's S3 drain replaces the outbox path for the backlog.
- **Superseded:** the AWS-only Lambda paths (#561's hosted runtime, the session-pipeline and read-API
  Lambdas) and the Cloudflare stack in `stacks/cloudflare`. They stay in the tree, unused, until their
  phase retires them.
