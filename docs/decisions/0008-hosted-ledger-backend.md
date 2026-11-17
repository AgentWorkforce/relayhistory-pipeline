# 0008: The hosted S3 ledger runs in the compute's region, commits by log entry, and may use AWS-native accelerators behind the portable interface

**Status:** proposed and provisional. Khaliq approved writing this record on 2026-11-16 ("after research
yes"), and set its scope the same day: benchmark and test before any large migration plan. No option is
chosen. Every cloud resource, IAM or KMS change, bucket move and deploy below
needs his explicit OK in his own words, as AGENTS.md says, and all of it lands as code (stacks, charts,
Terraform), never by hand.

**As of 2026-11-17:** the benchmark harness and its stacks are on main, and the benchmark ran three times on
2026-11-16 (results kept in the private ops docs). Option C is built on main behind `INGEST_LEDGER_LOG_COMMIT`
(off by default; slices 1-6, `docs/design/s3-ledger-log-commit.md`, whose smoke-run table is at lines 18-26). It is
not on in any hosted environment. No option has been chosen.

**Builds on:** 0005 (upload accounting lives in S3) and 0007 (hosted sessions on the agentrelay EKS
clusters, on durable actors). **Refines:** the group-commit design
(`docs/design/s3-ledger-group-commit.md`, on main as "Revision 2, design only", not implemented). **Measured by:** `docs/design/hosted-ledger-benchmark-plan.md`.

## Context

On 2026-11-15/16 the hosted S3 ledger became the bottleneck for one hot user. The fleet outage fixes
(witness expiry judged at the upload's `LastModified`, concurrent and fair promotion, the per-user
pre-claim hint, the pass-aware `/readyz`) removed the data loss and the wasted work. Admission still
ran at about 46 batches/min against about 140/min arriving, with a backlog of about 50k inbox objects.

Measurements, taken read-only from the prod ingest-edge pod on 2026-11-16:

| Round trip from ingest-edge (EKS, us-west-2) | p50 | max of 8 |
| --- | --- | --- |
| The ledger bucket, in us-east-1 | 66 ms | 216 ms |
| S3 in us-west-2 (same region) | 5 ms | 15 ms |
| KMS in us-east-1 | 187 ms | 200 ms |

Each batch admission is a chain of about 13 dependent S3 round trips (about 75 requests per batch in total) (the table in §1 of the group-commit
design), several of them SSE-KMS. 13 × 66 ms ≈ 860 ms, and two or three KMS calls at 187 ms add about
450 ms, so a batch costs about 1.3 s. 60 s / 1.3 s ≈ 46 per serial lane, which matches what we observed.
This is arithmetic on our own measurements, not a measured breakdown per call. The ceiling is latency
multiplied by serial depth, not S3 capacity: S3 supports at least 3,500 writes/s and 5,500 reads/s per
partitioned prefix ([AWS](https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance.html)).

Two more facts:

- **Bucket Keys are off** on the ledger bucket, so every SSE-KMS GET and PUT is its own KMS call. The KMS
  key policy scopes access by the per-object encryption context (`kms:EncryptionContext:aws:s3:arn` on
  object or prefix ARNs).
- The 2026-11-15 tuning showed that more parallelism makes things worse. Admission concurrency 16 and
  ledger parallelism 32 over-subscribed the single Bun process's sockets. That caused fetch-level
  transient failures carrying no HTTP status, not S3 throttling. Concurrency 8 removed the deferral
  churn, and ledger parallelism 8 removed the transients but slowed each batch.

Two direction-setting statements from Khaliq:

- 2026-11-16: **order across one user's machines is not required.** A hot user may be split into
  per-machine streams keyed (org, workspace, user, origin). Order within a session still holds.
- The hosted cloud must scale to millions of users and **may use any AWS service**. Self-hosted
  customers (S3, GCS, Azure Blob) keep a portable core.

The prior-art research ("Event data lake storage patterns", 2026-11-16; kept outside this public repo)
found one storage shape across the industry. The client is acknowledged on a durable log append. Writers
batch many events per tenant and time window into one immutable payload object. Visibility, ordering and
dedupe are decided by one small atomic commit, either a create-if-absent of the next numbered log entry
or a metadata-store transaction. Examples are the Delta `_delta_log`, the Iceberg catalog pointer swap,
WarpStream, Turbopuffer and Datadog Husky with FoundationDB. Our inbox write is already the durable
acknowledgement, so the backlog is admission lag, not a durability risk.

## Options

Each option lists its benefit, cost, risk, portability impact and rollback. They are not exclusive.

### A. Co-locate compute, ledger and key in one region

- **A1: Move the ledger working set to us-west-2.** This covers fences, journals, batch docs, revision
  shards, owners, the inbox and the outcome objects, in a new bucket and KMS key in the agentrelay-prod
  account (0007 phase 7's sessions bucket, brought forward for the ledger only). Transcripts and
  manifests can stay until 0007 phase 7.
- **A2: Run ingest-edge (or an admission-only deployment) in us-east-1, next to the data.** That needs a
  new cluster or compute footprint in us-east-1. 0007 puts hosted sessions on the agentrelay clusters,
  and the only prod cluster is in us-west-2.

| | A1 (ledger to us-west-2) | A2 (compute to us-east-1) |
| --- | --- | --- |
| Benefit | About 13× lower S3 latency per request (66 → 5 ms) and in-region KMS; the largest single lever | The same latency gain, with no data move |
| Cost | A new bucket, a key and grants in code; a per-org migration of the ledger working set with a dual-read window | A new cluster or compute in us-east-1, which contradicts 0007's single prod cluster shape; new ops surface |
| Risk | Two ledgers live for one org during the move. A per-org cutover is required: stop, copy, verify, marker, switch, exactly like the 2026-11-15 marker flow | Splits the sessions deployment across regions; the read API and actors stay in us-west-2, so the cross-region trips move elsewhere instead of disappearing |
| Portability | None: a deployment choice. The same chart values work for self-host | None |
| Rollback | Per org: point it back at the old prefix; the old objects stay until the move is verified | Scale the us-east-1 deployment to zero |

**Working hypothesis: A1,** because it keeps 0007's single-cluster shape and is the bucket 0007 phase 7
needs anyway. The benchmark tests it.

### B. S3 Bucket Keys, after a key-policy redesign

- **Benefit:** KMS requests from S3 drop by up to 99%
  ([AWS](https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucket-key.html)). That removes the
  187 ms calls from most requests and keeps the per-account KMS quota (100k rps in us-east-1 and
  us-west-2) from binding at fleet scale.
- **Cost:** a rewrite of the key policy and grants. With Bucket Keys, the encryption context is the
  bucket ARN, not the object ARN, so every statement that conditions on object or prefix ARNs stops
  matching ([AWS](https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucket-key.html)). Prefix-level
  isolation moves to IAM conditions on S3 actions plus application-level envelope keys (option F).
- **Risk:**
  - Turning it on before rewriting the policy makes PUTs fail closed.
  - A revocation only takes effect when the cached bucket-level key ages out.
  - Existing objects keep their per-object keys until they are rewritten.
- **Portability:** none (AWS only). Self-host leaves it off.
- **Rollback:** turn Bucket Keys off for new writes. Objects written with a bucket key stay readable
  under the same KMS key.
- **Note:** if A1 is chosen, a new bucket could start with Bucket Keys on and a bucket-ARN policy, so the
  old bucket's policy would never have to change in place.

### C. Reshape group commit into a payload PUT plus a create-if-absent numbered log entry

The group-commit design (revision 2, design only) groups up to 16 batches per machine directory. It keeps every
per-batch object (batch doc, outcome, status) and shares the fence, journal and shard reads and the
manifest CAS per session. C goes further, following the Delta log, Turbopuffer and Husky:

1. The writer gathers due batches for one stream and PUTs **one payload object** to a unique key with no
   precondition. It can be pipelined while the previous commit is in flight.
2. It then does **one create-if-absent PUT of log entry N+1** for that stream. The entry carries the
   payload key and checksum, the batch ids and digests it covers, the writer's ownership epoch and the
   inbox cursor.
3. The per-session manifest and the revision index become **checkpoints**, written off the critical path
   from the log. Readers fold the log since the last checkpoint.

The fence check moves too. A new owner writes an "epoch change" entry at the next sequence number, so a
stale writer's attempt at that number fails with 412, the way Husky resets on an ownership conflict. A
per-batch fence CAS is no longer needed.

- **Benefit:** about two round trips per group on the critical path instead of about 13 per batch. It
  also works on GCS: the GCS limit of about one write per second per object name
  ([GCS quotas](https://docs.cloud.google.com/storage/quotas)) caps any design that does CAS on one
  per-user manifest, but every log entry is a new key.
- **Cost:** a layout change (log entries plus checkpoints) and a reader change, so it needs a migration
  and a dual-read period. It is the biggest code change in this record.
- **Risk:**
  - Mixed old and new writers on one stream corrupt it. This is the Delta LogStore warning: "only safe
    when all writers use this LogStore". A per-stream cutover marker is required.
  - An ambiguous commit (timeout or 5xx) must be resolved by reading entry N+1 back, never assumed
    failed.
  - Every privacy invariant of rev 3 §4 has to be re-proven against the log form: the reset and seq
    gates, deletions, consent at admit and at commit, and the SQL control mirror.
- **Portability:** good. It uses only create-if-absent and a read, available as S3 If-None-Match, GCS
  `ifGenerationMatch=0` and Azure `If-None-Match: *`. Commit entries must be small single-part PUTs.
  Conditional multipart behaves differently on each cloud.
- **Rollback:** the stream's cutover marker points the stream back at the per-batch path. Entries already
  written are folded into a checkpoint first, so nothing written is lost.

### D. One writer per stream: a durable actor per (org, workspace, user, origin)

- **Benefit:**
  - A single writer per stream removes the intra-stream contention that the per-user cap and the
    deferral memo work around today.
  - Per-machine streams (allowed by Khaliq's ordering decision) turn a hot user with 29 machines into
    29 independent lanes.
  - It fits 0007's actor model and its sharding rule (no single actor on a hot path).
- **Cost:** a stream actor type, routing from ingest-edge, and ownership handoff on failover.
- **Risk:** split-brain during an actor failover. The epoch entry (C) or the fence epoch (today) must
  catch it, and it needs negative tests where two writers race.
- **Portability:** durable actors run on EKS and GKE with S3 and GCS storage (0007). Azure is still not
  planned. The ledger protocol stays correct without actors: the stream key alone partitions the
  existing consumer.
- **Rollback:** route admissions back to the shared consumer.
- **Interim, no actors:** key the existing consumer's per-user cap and pre-claim hint per machine stream
  instead of per user. That is a small code change on today's path.

### E. DynamoDB as the sequencer or metadata store

- **Benefit:** a single-digit-ms conditional write, and `TransactWriteItems` (up to 100 actions or 4 MB,
  atomically, with a 10-minute idempotency token). That is the multi-key transaction S3 does not have.
  On-demand capacity scales to millions of users.
- **Cost:** a new storage dependency and its IAM. It amends 0005, which keeps upload accounting in object storage rather than a database.
- **Risk:**
  - A partition, or one hot item, caps at about 1,000 WCU.
  - No transaction spans DynamoDB and S3, so orphaned payloads need garbage collection.
  - Self-host would need a different backend, or the portable path only.
- **Portability:** AWS only, behind the ledger interface. Self-host keeps the S3/GCS/Azure log (C).
- **Rollback:** the stream marker switches the stream back to the object-store log, after a checkpoint.
- **Status:** measured by the benchmark (H5); no commitment.

### F. Envelope encryption per user and a suppression gate (needed by B)

- **Benefit:**
  - Per-user data keys wrapped by a few KMS keys give per-user isolation at the application layer, which
    B removes from SSE-KMS.
  - Erasure becomes crypto-shredding: delete the wrapped key and let it age out of backups.
  - An HMAC-keyed suppression list, checked before dedupe at the edge and at admission, stops re-sent or
    replayed batches from resurrecting erased data.
- **Cost:** a key store, a rotation policy and suppression checks on the hot path.
- **Risk:**
  - The key store's backups keeping shredded keys past the deadline.
  - Plaintext identifiers left in dedupe state, indexes or summaries, which still need physical deletion
    or keyed hashing.
- **Portability:** identical on all three clouds (application code).
- **Rollback:** additive; keys can be left unwrapped for new data only on an explicit decision.

### G. S3 Express One Zone as an accelerator

- **Benefit:** single-digit-ms access (it claims up to 10× faster than S3 Standard) and lower request
  cost after the April 2025 price cuts.
- **Cost:**
  - A second storage tier.
  - Session-based auth (`CreateSession`, 5-minute credentials).
  - One customer-managed KMS key per bucket for life, with Bucket Keys always on.
- **Risk:**
  - It is single-AZ (99.95% availability design target). WarpStream gets durability by writing to two
    directory buckets in different zones, then compacting to Standard.
  - **No primary source confirms `If-Match` (CAS) on directory buckets**, only `If-None-Match`. That has
    to be measured (benchmark plan, step 4).
- **Portability:** AWS only. GCS Rapid and Azure Premium are not equivalent (GCS Rapid is gRPC-only with
  a single writer), so it is never the system of record.
- **Rollback:** stop writing to the accelerator; Standard stays authoritative.
- **Status:** measured by the benchmark (H4); no commitment. If adopted it would suit C, which needs only
  create-if-absent.

## Decision: benchmark first

Khaliq, 2026-11-16: "we have to benchmark and test this before setting out a large migration plan."
So this record decides only that **no option is chosen and no migration is planned until the benchmark
(`docs/design/hosted-ledger-benchmark-plan.md`) has run and its results have been reviewed.** The
options above are hypotheses for that benchmark to confirm or reject.

| Hypothesis | What would confirm it | Option it supports |
| --- | --- | --- |
| H1: the critical path is per-request latency multiplied by serial depth, not S3 capacity | In-region chain p50 at least 8× faster than cross-region, with per-request throughput far from prefix limits | A (co-location) |
| H2: KMS is a large share of each round trip | SSE-KMS GET/PUT p50 drops by at least 50% with Bucket Keys on | B (Bucket Keys) |
| H3: a two-op log commit beats the per-batch chain by an order of magnitude | Log-commit groups/s × group size at least 10× the per-batch chain on the same backend; correct 412 on a racing writer | C (log-entry commit) |
| H4: S3 Express is worth a second tier | Create-if-absent p99 at most a third of standard us-west-2, and `If-Match` support confirmed or ruled out | G (Express) |
| H5: DynamoDB is worth a new dependency | `TransactWriteItems` p99 below the C log commit on standard us-west-2, at the same correctness | E (DynamoDB) |

The benchmark plan sets the exact thresholds and the matrix. **A migration plan follows only after the
benchmark results are reviewed.**

The one change this record expects ahead of the benchmark's review is the per-machine stream key on
today's consumer (the option D interim). It follows from Khaliq's ordering decision and needs no infra.
It lands through normal review on its own branch behind its own default-off flag; turning it on in any
hosted environment needs Khaliq's explicit OK, like every other change here.

### What this amends

- **0005:** nothing yet. Accounting stays in object storage. Adopting E would amend 0005 for the hosted
  backend only, and would need its own record.
- **0007:** nothing. If A1 is later chosen it would bring forward the agentrelay-prod sessions
  bucket for the ledger working set. The stream key (org, workspace, user, origin) is the natural shard
  for phase 4's ledger actors. The transcript-region rule for stored content stays until 0007 decides where the new bucket lives (0007 line 160 says phase 6, while its phase list puts the bucket in phase 7; 0007 looks off by one, and this record follows its phase list elsewhere).

### Needs Khaliq's explicit OK (each separately, all as code)

- The benchmark's throwaway resources in sessions-dev, and the scratch namespace, role and pod in the
  agentrelay-prod cluster.
- Anything after the benchmark (any new production bucket, key, policy, table, migration or protocol
  switch) waits for the reviewed results and a migration plan, each with its own OK.
- Every deploy and image rollout.

## Consequences and open points

- **Self-host guidance.** Compute and object store in the same region becomes a documented requirement.
  ingest-edge should also measure its store round-trip time at startup and warn, or report degraded,
  when it is high. That would have caught our own us-west-2 to us-east-1 split.
- **Code defaults stay portable.** Hosted tuning (concurrency 8, ledger parallelism 8 as measured above; any later value such as 16 is a benchmark result, not this record's) lives in the
  hosted chart values, not in code defaults.
- **Not measured yet:** the KMS latency per call with Bucket Keys, the Bucket Key cache lifetime,
  directory-bucket `If-Match`, and DynamoDB transaction latency from the cluster. The benchmark plan
  covers all four.
- **Erasure:** shredding does not remove plaintext identifiers in dedupe state, indexes or summaries.
  Those still need physical deletion or keyed hashing (F).
