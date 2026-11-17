# Hosted ledger benchmark plan

**As of 2026-11-17:** this plan has run. The harness and stacks are on main, and three repeats ran on 2026-11-16
(results in the private ops docs). Both sides are torn down and verified gone: the cluster side on 2026-11-16,
the data side on 2026-11-17 by layered-infra-ops, which reports acting on Khaliq's confirmation to it. Only the
three KMS keys remain, in their pending-deletion window.
Differences from this plan as built: four CloudFormation stacks instead of one; a finished Job is
kept 2 days (`stacks/aws/ledger-bench/job.template.yaml`, `ttlSecondsAfterFinished`), not the 1 hour in "Teardown"; the budget alarm only notifies and does not stop the run.

**Status:** proposed. It is the deliverable of decision 0008 ("benchmark first"). Nothing here runs until
Khaliq gives his explicit OK to the resources in "Resources" below. All of them are created and destroyed
by code, never by hand. The results decide between the 0008 options. A migration plan is written only
after they have been reviewed.

**Goal:** measure, from where hosted ingest actually runs, what each candidate backend costs per ledger
operation and per committed batch, and whether each one gives the conditional-write semantics the
ledger needs. The answers have to be good enough to pick an option with numbers rather than opinion.

## Where it runs

- **Client:** a short-lived Kubernetes Job in the agentrelay-prod EKS cluster (us-west-2), in a scratch
  namespace `ledger-bench`, on the same node type as ingest-edge. Its pod runs the same Bun version and
  object-store package as ingest-edge, so the client stack matches production.
- **Identity:** its own IRSA role, `ledger-bench`, that can reach only the throwaway resources below. It
  has no access to any production bucket, table or key. One read-only exception, a HEAD of the
  production ledger bucket's endpoint (no object access), is allowed only to reproduce the
  2026-11-16 cross-region round-trip baseline.
- **Target resources:** in the **sessions-dev** account, never in a production account. The role
  assumes a sessions-dev role through a cross-account trust scoped to the Job's service account.
- **Data:** synthetic only: random bytes and synthetic ids. No customer data and no production keys.

## Resources (throwaway; planned as one stack, built as four; see the status line)

| Id | Resource | Region | Purpose |
| --- | --- | --- | --- |
| R1 | S3 general-purpose bucket, SSE-KMS, Bucket Keys **off** | us-east-1 | Cross-region baseline (production's shape) |
| R2 | S3 general-purpose bucket, SSE-KMS, Bucket Keys **off** | us-west-2 | In-region, production encryption shape |
| R3 | S3 general-purpose bucket, SSE-KMS, Bucket Keys **on** | us-west-2 | The Bucket Keys effect (H2) |
| R4 | S3 Express One Zone directory bucket, SSE-KMS (Bucket Keys forced on) | us-west-2, the cluster's node AZ | Accelerator (H4) |
| R5 | A second directory bucket in another us-west-2 AZ | us-west-2 | WarpStream-style dual-AZ write |
| R6 | DynamoDB on-demand table `ledger_bench` (pk, sk), no streams | us-west-2 | Sequencer and metadata option (H5) |
| K1, K2 | Customer-managed KMS keys, one per region | us-east-1, us-west-2 | Matching production's SSE-KMS |
| N1 | The `ledger-bench` namespace, ServiceAccount, IRSA role and Job | agentrelay-prod EKS | The client |

Every resource is tagged `purpose=ledger-bench`, `ttl=<date + 2 days>` and `owner=<session>`.

Everything lives in a stack directory that holds the bucket, table and key definitions. The chart
values hold the Job. Teardown is the same stack's `destroy`, plus deleting the namespace.

## Test matrix

Each cell runs for 5 minutes of steady state after a 30-second warm-up. It repeats 3 times at different
times of day, and the median of the three p50s and p99s is reported. The concurrency levels are **1, 8,
16 and 64** in-flight requests. Object sizes are **1 KiB** (commit records, fences, entries) and
**64 KiB** (payload batches; the p50 production batch is in this range). The 256 KiB size is run only at
concurrency 8.

### Step 1: raw request latency (H1, H2)

On R1, R2, R3 and R4: unconditioned GET, unconditioned PUT, HEAD and LIST (1,000 keys, one page). Record
p50, p90, p99 and max per operation, and errors by class (HTTP status, or the `store_cause` class for
failures below HTTP). On R1 and R2, also time the KMS calls S3 makes indirectly, by comparing SSE-KMS
against SSE-S3 objects in the same bucket.

### Step 2: conditional semantics and latency (the ledger's correctness needs)

On R2, R3, R4 and R5, measure each of the following, both for correctness and for p50/p99:

- `If-None-Match: *` create-if-absent, single writer.
- Create-if-absent with 2 and 8 writers racing on one key. Exactly one must succeed and the rest must get
  412; count the outcomes.
- `If-Match: <etag>` CAS on an existing object, single writer and 8 racing writers. Exactly one must win.
  **On R4 and R5 this records whether `If-Match` is supported at all.** It is the open question from the
  research. An unsupported or ignored precondition is a failed cell, not a pass.
- A delete racing a conditional write (the documented 409 case).
- An ambiguous commit: kill the client mid-PUT (SIGKILL the pod during a write), then read back.
  Readback must show the write either fully present or absent.

### Step 3: the per-batch chain against the log commit (H1, H3)

Replay the two protocols with synthetic objects. These are not the production code paths; they use the
same request sequence and object sizes.

- **P13: today's per-batch chain.** About 13 dependent requests in the order of the group-commit design
  §1 table (reads of the fence, batch doc, revision shards and owner; claim CAS; journal read; session
  pointer, manifest and chunk; manifest CAS; commit; outcome).
- **PLOG: the 0008 option C log commit.** Per group of G batches: one unconditioned payload PUT of G ×
  64 KiB, then one create-if-absent PUT of a 1 KiB entry N+1. The next group's payload PUT may overlap
  the current entry PUT. Run G = 1, 4, 16 and 30.
- **PDDB: the 0008 option E variant.** The payload PUT to R2, then one `TransactWriteItems` on R6 with a
  condition `attribute_not_exists(seq)` on the stream head plus up to 25 batch-id items.

Run each protocol on R1 (P13 only, as the baseline), R2, R3 and R4 (PLOG only). Use 1 stream (a hot
user) and 64 streams (many users), with 8 concurrent streams per client. Report committed **batches per
second**, commit latency p50/p99 and the error and retry rate.

Racing-writer check: two clients write the same stream with the same epoch. Exactly one commit per
sequence number must land, the loser must get 412, and a readback must show no gap and no duplicate.

### Step 4: S3 Express specifics (H4)

- The `CreateSession` cost and the refresh behaviour across the 5-minute credential expiry, with the same
  object-store client ingest-edge uses. Count any request failures at the refresh boundary.
- Dual-AZ write (R4 + R5), WarpStream style: write both, acknowledge when both succeed, then copy to R2
  in the background. Report the added latency over a single write.
- KMS: confirm that the bucket's single CMK is enforced, and record any per-request KMS cost.

### Step 5: DynamoDB specifics (H5)

- `TransactWriteItems` with 2, 25 and 100 items, single writer and 8 writers on one stream head. Record
  p50/p99, `TransactionCanceledException` by reason, and the throttling rate.
- One hot stream head driven to 1,000 writes/s and above, to observe the per-partition limit and
  adaptive capacity.
- Idempotency token replay inside 10 minutes, which must not apply twice.

### Step 6: socket and pool headroom (ties back to the 2026-11-15 transients)

On R2, ramp in-flight requests from 64 to 512 in one Bun process. Record where `store_cause`
transients start and where latency rises. That gives the safe ceiling for hosted ledger parallelism
times admission concurrency.

## Success thresholds (which result selects which 0008 option)

The thresholds compare against the R1 per-batch chain (P13), today's production shape.

| Option | Selected if all of these hold | Rejected if any of these hold |
| --- | --- | --- |
| **A1** (ledger to us-west-2) | P13 on R2 has p50 ≤ ⅛ of P13 on R1 and p99 ≤ ¼ of it. Conditional semantics on R2 pass every step 2 cell | The in-region gain is under 4× at p50 |
| **B** (Bucket Keys) | GET/PUT p50 on R3 is ≤ 50% of R2, and P13 batches/s on R3 is ≥ 1.3× R2 | Any conditional cell behaves differently on R3, or the gain is under 15% |
| **C** (log commit) | PLOG at G = 16 on R2 is ≥ 10× P13 on R2 in batches/s for 1 stream. Every racing-writer and ambiguous-commit check passes | Any duplicate or gap in a racing-writer run, or a readback that can't settle an ambiguous commit |
| **G** (Express) | PLOG on R4 has entry p99 ≤ ⅓ of PLOG on R2, `If-None-Match` is correct under races, and the session refresh causes 0 failed requests | `If-None-Match` is incorrect under races, the dual-AZ write adds more than R2's own p50, or refresh failures are above 0 |
| **E** (DynamoDB) | PDDB p99 < PLOG p99 on R2 at the same G, with 0 lost or duplicate commits, and one hot head sustains ≥ 2× the observed peak per-stream commit rate | Any lost or duplicate commit, or the hot head throttles below the observed peak |

The **observed peak** is production's arrival rate for the hot user on 2026-11-15/16: about 140
batches/min, roughly 2.3/s. A selected option must also clear **10× that rate for 1 stream** on its own.
That is headroom for millions of users, where hot users are the norm, not the exception.

`If-Match` on directory buckets is reported as one of three results: supported, ignored or rejected.
Option C does not need it. Any other use of Express does.

## Reporting

- **Results file:** one results file per run under a private ops path (not this repo). It holds the raw
  per-request timings, a summary table per step and the threshold outcomes, labelled pass, fail or
  inconclusive.
- **Summary:** a short write-up with the numbers, reviewed with Khaliq. Only after that review does a
  migration plan get written, as a follow-up to 0008.
- **This repo:** no account ids, ARNs or bucket names. The results file refers to resources by the R-ids
  above.

## Teardown

1. Wait for the Job to finish, or delete it on its deadline: `activeDeadlineSeconds` is 4 hours, with a
   TTL of 1 hour after it finishes.
2. Empty every bucket. Directory buckets need an explicit delete of every object. General-purpose buckets
   use versioning off and a lifecycle expiry of 1 day as a backstop.
3. Run the stack `destroy`: buckets, the table, the keys (KMS keys go into the minimum 7-day pending
   deletion) and the roles.
4. Delete the `ledger-bench` namespace, service account and role binding.
5. Verify by listing the sessions-dev resources tagged `purpose=ledger-bench`, which must return none
   except the keys pending deletion, and the IRSA role, which must be gone. Record the listing in the
   results file.
6. If the run is abandoned, the `ttl` tag plus a scheduled check flags anything older than its TTL.

## Cost bound

The total is bounded by request volume, not storage.

- **Requests:** about 5 min × 3 repeats × the matrix cells, at up to about 2k requests/s for the
  largest cells. That is on the order of tens of millions of requests overall.
- **S3 Standard:** PUTs cost about $0.005 per 1k, so this stays in the low hundreds of dollars at the
  very worst. It is likely tens.
- **S3 Express and DynamoDB on-demand:** both are cheaper per request at these volumes.
- **Hard caps:**
  - a sessions-dev budget alarm at **$300** for the `purpose=ledger-bench` tag (as built it only notifies;
    the operator stops the run);
  - the Job's `activeDeadlineSeconds` limit;
  - DynamoDB on-demand with a table-level maximum throughput setting;
  - no data kept past teardown.

## Out of scope

- Any change to production buckets, keys, policies, tables, chart values or images.
- Any migration plan.
- GCS and Azure. Their conformance stays in CI (MinIO, the GCS emulator, Azurite). A self-host
  benchmark, if wanted, is a separate plan.
