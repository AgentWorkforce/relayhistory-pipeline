# ledger-bench: the hosted-ledger benchmark's throwaway stack

Infrastructure for [the hosted-ledger benchmark plan](../../../docs/design/hosted-ledger-benchmark-plan.md)
(decision 0008). The client is in [`benchmarks/ledger-bench`](../../../benchmarks/ledger-bench/).

**Nothing here may be applied without Khaliq's own OK for these resources** (AGENTS.md). The stack is
temporary: it is created for one benchmark and deleted when the benchmark has run. It is not part of the
`96bulls` AWS stages and shares nothing with them.

## What it creates

| Where | Template | Resources |
| --- | --- | --- |
| Throwaway data account, the compute's Region | `data-west.yaml` | R2, R3 (general-purpose buckets, Bucket Keys off and on), R4, R5 (S3 Express directory buckets in two zones), R6 (DynamoDB on-demand table, capped at 10,000 write and read units), key K2, the role `ledger-bench-client`, optionally a budget |
| Throwaway data account, another Region | `data-east.yaml` | R1 (general-purpose bucket, Bucket Keys off) and key K1 |
| The EKS cluster's account | `cluster-identity.yaml` | The role `ledger-bench-pod` and its EKS Pod Identity association |
| The EKS cluster | `namespace.yaml`, `job.template.yaml` | Namespace `ledger-bench` with a ServiceAccount, a NetworkPolicy and a quota; two ConfigMaps; one Job per lane |

Every taggable resource carries `purpose=ledger-bench`, `ttl=<apply date + 2 days>` and `owner=<session>`.

## Who can reach what

- The pod's role (`ledger-bench-pod`) has one permission: assume `ledger-bench-client` in the data account.
  It has no access to any bucket, key or table in the cluster's account.
- `ledger-bench-client` is trusted only by that pod role. It can read and write `bench/*` in R1, R2 and R3,
  open sessions on R4 and R5, use the two bench keys (matched by tag, through S3 only), and read and write
  the one table.
- The client refuses any target whose name lacks `ledger-bench` before its first request
  (`benchmarks/ledger-bench/config.ts`).
- The pod runs the ingest-edge image by digest, with the client bundled into a ConfigMap. No image is built
  or pushed. It runs as a non-root user on a read-only root filesystem with no Kubernetes API token.

## Bounds

- **Cost.** Each lane refuses to make more than its request budget (2.5 million requests in the standard
  profile). Priced as the dearest request type, three repeats of all lanes cost at most the figure printed
  by `bun benchmarks/ledger-bench/main.ts plan`, which a test keeps under 300 USD. The optional AWS budget
  is a backstop only: its data lags by hours.
- **Time.** Each Job has `activeDeadlineSeconds: 14400` (4 hours) and is removed an hour after it ends.
- **Throughput.** The table has a maximum on-demand throughput, and a cell stops at its operation cap.

## Running it

The operator's inputs (account ids, the cluster's name, the image digest, zone ids) go in a JSON file kept
outside this repository; `ops.ts` documents the fields (`Inputs`). Credentials come from two AWS CLI
profiles and a kube context named there. `ops.ts` stores nothing.

```sh
bun stacks/aws/ledger-bench/ops.ts up-data      --inputs <file> --confirm-account <data account id>
bun stacks/aws/ledger-bench/ops.ts up-identity  --inputs <file> --confirm-account <cluster account id>
bun stacks/aws/ledger-bench/ops.ts up-namespace --inputs <file>
bun stacks/aws/ledger-bench/ops.ts run     --inputs <file> --run-id smoke-1 --profile smoke
bun stacks/aws/ledger-bench/ops.ts run     --inputs <file> --run-id <date>-a
bun stacks/aws/ledger-bench/ops.ts collect --inputs <file> --run-id <date>-a --out <private results dir>
```

The plan's three repeats at different times of day are three `run`s with three run ids.

## Teardown

```sh
bun stacks/aws/ledger-bench/ops.ts down   --inputs <file> --confirm-account <data account id> --confirm-cluster-account <cluster account id>
bun stacks/aws/ledger-bench/ops.ts verify --inputs <file>
```

`down` deletes the namespace, empties all five buckets, and deletes the three stacks. `verify` then looks
every resource up by name and lists everything tagged `purpose=ledger-bench` in both accounts and Regions.
It exits non-zero unless everything is gone; the only resources allowed to remain are the two KMS keys in
their 7-day pending-deletion window. Its output names no account, ARN or bucket, and goes into the results
file.

Results go to the private ops repository, never here.
