# ledger-bench: the hosted-ledger benchmark's throwaway stack

Infrastructure for [the hosted-ledger benchmark plan](../../../docs/design/hosted-ledger-benchmark-plan.md)
(decision 0008). The client is in [`benchmarks/ledger-bench`](../../../benchmarks/ledger-bench/).

**Nothing here may be applied without Khaliq's own OK for these resources** (AGENTS.md). The stack is
temporary: it is created for one benchmark and deleted when the benchmark has run. It is not part of the
`96bulls` AWS stages and shares nothing with them.

## What it creates

| Where | Template | Resources |
| --- | --- | --- |
| Throwaway data account, the compute's Region | `data-west.yaml` | R2, R3 (general-purpose buckets, Bucket Keys off and on), R4, R5 (S3 Express directory buckets in two zones), R6 (DynamoDB on-demand table, capped at 10,000 write and read units), key K2, a probe key that no bucket uses, the role `ledger-bench-client`, optionally a budget |
| Throwaway data account, another Region | `data-east.yaml` | R1 (general-purpose bucket, Bucket Keys off) and key K1 |
| The EKS cluster's account | `cluster-identity.yaml` | The role `ledger-bench-pod` and its EKS Pod Identity association |
| The EKS cluster | `namespace.yaml`, `job.template.yaml` | Namespace `ledger-bench` with a ServiceAccount, a NetworkPolicy and a quota; three ConfigMaps (the client bundle, its config, the spend ledger); one Job per lane |

Every taggable resource carries `purpose=ledger-bench`, `ttl=<apply date + 2 days>` and `owner=<session>`.

## Who can reach what

- The pod's role (`ledger-bench-pod`) has one permission: assume `ledger-bench-client` in the data account.
  It has no access to any bucket, key or table in the cluster's account.
- `ledger-bench-client` is trusted only by that pod role (`sts:AssumeRole` with `sts:TagSession`, which the
  Pod Identity session tags need). It can read and write `bench/*` in R1, R2 and R3,
  open sessions on R4 and R5, use the bench keys (K2 by ARN, the others by their tag), and read and write
  the one table.
- The client refuses any target whose name lacks `ledger-bench` before its first request
  (`benchmarks/ledger-bench/config.ts`).
- The pod runs the ingest-edge image by digest, with the client bundled into a ConfigMap. No image is built
  or pushed. It runs as a non-root user on a read-only root filesystem with no Kubernetes API token.

## Bounds

- **Cost.** Each lane refuses to make more than its request budget (2.5 million requests in the standard
  profile). Priced as the dearest request type, three repeats of all lanes cost at most the figure printed
  by `bun benchmarks/ledger-bench/main.ts plan`, which a test keeps under 300 USD. `ops.ts run` keeps a
  spend ledger in the namespace and refuses any run (smoke runs and re-runs included) that would take the
  total bound past 300 USD. The optional AWS budget is a backstop only: its data lags by hours.
- **Time.** Each Job has `activeDeadlineSeconds: 14400` (4 hours). A finished Job is kept for 2 days,
  because its log is the results.
- **Shared nodes.** The pod has a CPU limit, so it cannot take more than that from a node it shares with
  other workloads. Run one lane at a time (`--lanes`) on a cluster that serves production.
- **Throughput.** The table has a maximum on-demand throughput, and a cell stops at its operation cap.

## Differences from the plan

- **Identity.** EKS Pod Identity, not IRSA: it is what every other workload of these stacks uses.
- **Cell length.** 60 s of steady state per latency cell and 120 s per protocol cell, not 300 s, so that
  a lane ends well inside the 4-hour deadline. Each cell also stops at an operation cap.
- **Timings.** Each cell records a 2%-precision latency histogram, not one row per request.
- **The per-batch chain** follows the group-commit design's table for a 1-session batch with two revision
  shards: 17 dependent round trips and 22 requests, not "about 13".
- **KMS share.** Measured as SSE-KMS against SSE-S3 objects in the same bucket, at concurrency 8.
- **No production endpoint is contacted.** The plan's optional HEAD of the production bucket's endpoint is
  left out; R1 reproduces the cross-region baseline.
- **Budget.** The AWS budget cannot filter by the `purpose` tag in time (cost-allocation tags take a day to
  activate) and only notifies; the client's request budget and the spend ledger are the cap.
- **S3 Express** goes through the object-store package with a fetch wrapper that re-signs for
  `s3express`; the package itself cannot sign for directory buckets today.

## Running it

The operator's inputs (account ids, the cluster's name, the image digest, zone ids) go in a JSON file kept
outside this repository; `ops.ts` documents the fields (`Inputs`). Credentials come from the AWS CLI
profiles and the kube context named there. `ops.ts` stores nothing.

The data side and the cluster side need different access and can be run by two operators. The data side's
operator runs:

```sh
bun stacks/aws/ledger-bench/ops.ts up-data      --inputs <file> --confirm-account <data account id>
bun stacks/aws/ledger-bench/ops.ts outputs-data --inputs <file>   # key ARNs for the cluster side's inputs
```

The cluster side's operator, with `keys` from `outputs-data` in their inputs, runs:

```sh
bun stacks/aws/ledger-bench/ops.ts up-identity  --inputs <file> --confirm-account <cluster account id>
bun stacks/aws/ledger-bench/ops.ts up-namespace --inputs <file>
bun stacks/aws/ledger-bench/ops.ts run     --inputs <file> --run-id smoke-1 --profile smoke
bun stacks/aws/ledger-bench/ops.ts run     --inputs <file> --run-id <date>-a --lanes r2
bun stacks/aws/ledger-bench/ops.ts collect --inputs <file> --run-id <date>-a --out <private results dir>
```

The plan's three repeats at different times of day are three run ids.
`bun benchmarks/ledger-bench/report.ts <private results dir>` writes the tables and the threshold outcomes.

## Teardown

```sh
bun stacks/aws/ledger-bench/ops.ts down-cluster   --inputs <file> --confirm-account <cluster account id> --collected <private results dir>
bun stacks/aws/ledger-bench/ops.ts down-data      --inputs <file> --confirm-account <data account id>
bun stacks/aws/ledger-bench/ops.ts verify-cluster --inputs <file> --confirm-account <cluster account id>
bun stacks/aws/ledger-bench/ops.ts verify-data    --inputs <file> --confirm-account <data account id>
```

`down-cluster` goes first, so nothing is writing while the data is removed. It refuses while a lane in the
spend ledger has no collected results (`--discard-results` overrides), then deletes the namespace and the
identity stack. `down-data` empties all five buckets and deletes the two data stacks. Each `verify` then
looks every resource of its side up by name and lists everything tagged `purpose=ledger-bench`. It exits
non-zero unless every lookup said "gone"; the only resources allowed to remain are the three KMS keys in
their 7-day pending-deletion window. Its output names no account, ARN or bucket, and goes into the results
file.

Results go to the private ops repository, never here.
