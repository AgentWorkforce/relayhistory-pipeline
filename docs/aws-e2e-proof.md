# AWS self-host E2E proof (harness and runbook)

This harness proves the self-hosted 96Bulls deployment end to end on one
isolated AWS stage: account `500268966557`, EKS cluster `96bulls-aws-proof`.
It has not been run against AWS. Nothing in this repository has created the
proof stage. The only local run so far was a read-only `fresh-install` against
`kind-96bulls-e2e` (see [Local evidence](#local-evidence)).

The harness lives in `src/proof/aws/`. The entry point is:

```sh
bun run proof:aws [checks...|all] [--read-only] [--confirm-live] [--target kind-readonly] [--inputs path] [--sessions n] [--out dir]
bun run proof:aws:teardown [--confirm-live]
```

Related work: #59 is the search benchmark on a local stand-in, and its
nearest-rank percentile definition is reused here. #38 and #48 define the
session directory, `short_summary` and the V1 search RPC. #63 is the design for
the tenant JWT issuer.

## Safety model

**The default is a dry run.** It prints every exact command and in-process
action, writes a report with every check `planned`, and touches nothing.

**Modes:**

| Flags | Mode | What runs |
| --- | --- | --- |
| *(none)* | dry-run | nothing; prints the plan |
| `--read-only --confirm-live` | read-only | non-mutating steps on the proof cluster |
| `--confirm-live` | live | every step the guard allows, mutating steps included |
| `--target kind-readonly --read-only` | read-only | `fresh-install` only, on `kind-96bulls-e2e` |

**The guard** (`src/proof/aws/guard.ts`) runs before the first step and again
before every step. It has no override. Any non-dry run against AWS needs all of
the following:

- `--confirm-live` on the command line;
- `aws sts get-caller-identity` returns the account `500268966557`. The
  harness checks the observed identity, not a configured value;
- the cluster name is exactly `96bulls-aws-proof`;
- the kube context points at
  `arn:aws:eks:<region>:500268966557:cluster/96bulls-aws-proof`;
- no name is production-shaped. A name containing `prod`, `production`,
  `sessions-dev`, `hosted` or `live` as a segment is refused.

**The target fence** (`src/proof/aws/fence.ts`) checks *what* each step
touches, not just who runs it. It applies twice:

- **On the config, before anything is planned.** The values must be the proof
  stage's own:
  - ingest host `proof-sessions.agentrelay.com`, and origin
    `https://proof-sessions.agentrelay.com`;
  - Locker bucket `96bulls-aws-proof-locker-500268966557`, and any backup bucket
    `96bulls-aws-proof-<name>-500268966557`;
  - release `96bulls` and the AgentWorkforce chart repository;
  - namespaces from the fixed set that `stacks/aws` creates (`96bulls`,
    `relayauth`, `relaycast`, `cnpg-system`);
  - deployments that belong to the `96bulls` release;
  - a transcript database whose name starts with `proof`.

  The config is also rejected if any string in it is production- or
  shared-shaped (`prod`, `production`, `sessions-prod`, `sessions-dev`,
  `staging`, `hosted`), or if any URL carries userinfo (`https://user:pass@host`).
  This applies even inside the proof account.
- **On every step's argv, in every mode including dry-run.** These must be
  inside the fence:
  - context, region, namespace and bucket flags;
  - `s3://` URLs;
  - `deployment/` names;
  - HTTP hosts, including in-cluster `<svc>.<ns>.svc` names;
  - account ids in ARNs.

  A step outside the fence is recorded `refused` and never spawned. Every
  printed or recorded command is sanitized, so a credential never appears in
  output, dry-run plans included.

`inputs.json` cannot change the account, the cluster or the stage. The loader
discards those keys. It also rejects any key that looks like a secret.

The local kind cluster is read-only. The harness refuses every mutating step
and every live run on it, and only `fresh-install` may run there.

**Approvals.** Every mutating step is tagged `MUTATE*` in the plan and
`approval: "khaliq"` in the report. Khaliq must OK it directly, in his own
words, before anyone passes `--confirm-live` without `--read-only`. Read-only
steps are tagged `operator`. They still need `--confirm-live` and the guard.
Running even the read-only steps against AWS needs Khaliq's OK first, because
the proof stage does not exist yet.

**Secrets.** Secrets come only from environment variables, piped from 1Password
for the length of one command. They never come from a file. The harness reads
these variables:

| Variable | Used by | Content |
| --- | --- | --- |
| `PROOF_RELAYAUTH_BOOTSTRAP_KEY` | planted-trace | the stage's RelayAuth bootstrap key (`relayauth:assertion:create:relayhistory`), sent only through `kubectl port-forward` |
| `PROOF_SPONSOR_ID` | planted-trace | sponsor user id for the minted assertions (not secret) |
| `PROOF_TRANSCRIPT_TENANT_CREDENTIALS` (name configurable) | transcript-search | JSON `{"<org>/<workspace>": "user:password"}`, one read-only backend user per tenant |
| `PROOF_TRANSCRIPT_WRITER` (name configurable) | transcript-search, synthetic feed | `user:password` for the synthetic writer |

The harness strips every minted token and credential from the report.
`validateReport` refuses to write a report that contains a secret-shaped value,
a run secret or a planted canary.

## Prerequisites on the proof stage (each needs Khaliq's OK)

The harness verifies the stage. It does not create the stage. Before a live
run, the stage needs:

1. `.96bulls/aws-proof/aws-inputs.json` with the proof account and Khaliq's
   clearance, then `bin/96bulls up aws-proof`. This creates the account
   baseline (KMS, the versioned Locker bucket `96bulls-aws-proof-locker-500268966557`,
   CloudTrail, ECR, the deployer role) and EKS with the `96bulls` umbrella chart.
2. In-cluster HoopDb through CloudNativePG, with `hoopdb.cluster.instances >= 2`
   (failover needs a replica; the chart default is 1) and
   `hoopdb.cluster.backup.barmanObjectStore` pointing at a backup bucket (the
   chart default has backups off).
3. The session directory migrations from #38/#48 applied to that HoopDb
   (`sessions.session_directory.short_summary` and
   `sessions.search_session_directory`). Without them, `planted-trace` is
   `blocked` at the summary and search stages.
4. RelayAuth (`relayauth` namespace) and Relaycast (`relaycast` namespace) from
   their released charts.
5. The public ingest at `proof-sessions.agentrelay.com`: HTTPS :443 only, an ACM
   certificate, a WAFv2 web ACL with a blocking rate-based rule on the ALB, and
   no HTTP listener.
6. For the transcript gate: a ClickHouse or ObsessionDB endpoint, a synthetic
   ClickHouse Cloud service (approved for the proof), one read-only user per
   proof tenant, and a row policy that restricts each user to its own `org_id`.
   In synthetic-feed mode it also needs a writer user.

## Inputs

`.96bulls/aws-proof/inputs.json` is gitignored and holds no secrets. Every key
is optional, and the defaults are in `src/proof/aws/config.ts`. An example:

```json
{
  "region": "us-west-2",
  "chartVersion": "0.3.0",
  "candidateChartVersion": "0.3.1",
  "expectedImages": { "<account>.dkr.ecr.us-west-2.amazonaws.com/96bulls-services": "sha256:<eks.imageDigest>" },
  "probeImage": "<account>.dkr.ecr.us-west-2.amazonaws.com/96bulls-services@sha256:<digest>",
  "ingress": { "hostname": "proof-sessions.agentrelay.com", "burst": { "maxRequests": 400 } },
  "transcriptSearch": {
    "backend": "clickhouse",
    "feed": "harness-synthetic",
    "endpoint": "https://<service>.us-west-2.aws.clickhouse.cloud:8443",
    "database": "proof",
    "table": "transcripts",
    "credentials": { "tenants": "PROOF_TRANSCRIPT_TENANT_CREDENTIALS", "writer": "PROOF_TRANSCRIPT_WRITER" }
  }
}
```

`probeImage` must be digest-pinned and must contain `bun`; the services image
qualifies. `expectedImages` adds the per-account ECR digest on top of the
released chart defaults. An operator input always wins over a chart default.

## Checks

The harness runs these checks in this order. Each one emits a `CheckResult`
with a status (`pass`, `fail`, `unsupported`, `blocked`, `planned` or
`skipped`), metrics, failures, sanitized evidence (SHA-256, byte count and a
redacted excerpt) and every step it planned or ran.

The table lists every command, grouped by check. `R` marks a read. `M*` marks a
mutating step that needs Khaliq's OK. `<ctx>` stands for `--context
arn:aws:eks:us-west-2:500268966557:cluster/96bulls-aws-proof`.

### 1. fresh-install

The check asserts the following:

- every running container, init containers included, is pinned by `@sha256:`;
- the kubelet's `imageID` matches the pinned digest;
- each digest equals the released chart default or an operator input.

It records chart versions and digests. It is `blocked` when it has nothing to
compare against.

| | Command | Touches |
| --- | --- | --- |
| R | `kubectl <ctx> -n {96bulls,relayauth,relaycast,cnpg-system} get pods -o json` | pod lists |
| R | `helm --kube-context <ctx> list -A -o json` | release list |
| R | `helm show values <chart> --repo https://AgentWorkforce.github.io/helm-charts --version <v>` for each deployed chart | public chart repo |

### 2. planted-trace

The check plants N synthetic sessions (100 by default) with UUIDv7 ids across
two tenants. It uses the product's authenticated ingest path: the chunk hop
(Technical Foul scrubs before storage), then the manifest POST, which writes to
S3 through the relayhistory-cloud storage adapter. It then follows each session
through four stages:

1. the S3 object is present;
2. the HoopDb `session_index` row exists;
3. `session_directory.short_summary` is populated, scrubbed, and equal to the
   extractive summary when `BOB_COSTAS_MODEL=off`;
4. V1 search returns the session to its own tenant.

Per-stage timestamps come from `sessions.pipeline_events` (server clock). The
check reports p50, p95 and p99 plus the error rate for landed→indexed,
landed→summary and the search query. The query time is the RPC's server-side
execution time.

| | Command | Touches |
| --- | --- | --- |
| R | `kubectl <ctx> -n 96bulls port-forward svc/96bulls-alleyoop 18080:8080`, `kubectl <ctx> -n relayauth port-forward svc/relayauth 18787:8787` | tunnels through the API server |
| M* | `HTTP POST <relayauth>/v1/tokens/relayhistory-assertion` for each tenant | RelayAuth (1h tokens) |
| M* | `HTTP POST <ingest>/v1/pipeline/chunks` + `/v1/pipeline/manifests` ×N | Locker bucket, HoopDb (through the pipeline) |
| R | `kubectl <ctx> -n 96bulls get clusters.postgresql.cnpg.io hoopdb -o jsonpath={.status.currentPrimary}` | CNPG |
| R | `kubectl <ctx> -n 96bulls exec -i <primary> -c postgres -- psql -d hoopdb -X -At -v ON_ERROR_STOP=1 -f -`, with the SQL on stdin: index rows, stage times, directory rows | HoopDb (read) |
| R | the same psql call running `BEGIN READ ONLY; SET LOCAL ROLE session_directory_search_worker; set_config('request.jwt.claims', <tenant>) … sessions.search_session_directory(...) … ROLLBACK` | HoopDb (read) |
| R | `aws s3api list-objects-v2 --bucket 96bulls-aws-proof-locker-500268966557 --prefix v1/ws=<ws>/m=<run machine>/` | Locker (keys only) |

### 3. public-ingress

The customer-facing ingest stays public. The check covers five things:

- **(a) Negative probes.** Every non-health ingest route must return 401 or 403
  without credentials, and again with tenant A's token against tenant B's
  objects. A 404 is accepted only in two cases: a by-id lookup with a
  wrong-tenant token (AlleyOop scopes the lookup and does not confirm that the
  object exists), and a `/v1/internal/*` route that is not routed publicly.
  `/healthz` must return 200.
- **(b) No plaintext.** The ALB must have no HTTP listener, and TCP :80 must
  not accept a connection.
- **(c) TLS.** The ALB SSL policy must allow nothing below TLS 1.2. A TLS 1.0 or
  1.1 handshake must fail, and a TLS 1.2+ handshake must verify the certificate
  for the host. A refused legacy handshake may be the local OpenSSL's own floor
  rather than the server's, so the SSL policy is the authoritative evidence.
- **(d) WAF.** A WAFv2 web ACL must be associated with the ALB and must have a
  blocking rate-based rule.
- **(d) Burst.** A bounded burst must trigger that rule. The burst stays above
  the rule's limit, sends at most 5000 requests, runs for at most 300 s, and
  stops at the first block.

The active probes run only when the host resolves to the addresses of an ALB
that `describe-load-balancers` lists in the proof account. A proxied or
repointed host is refused, and then the check verifies only the ALB
configuration.

| | Command | Touches |
| --- | --- | --- |
| R | `kubectl <ctx> -n 96bulls get ingress -o json` | Ingress |
| R | `aws elbv2 describe-load-balancers`, `describe-listeners --load-balancer-arn <alb>`, `describe-ssl-policies --names <policy>` | ELB (read) |
| R | `aws wafv2 get-web-acl-for-resource --resource-arn <alb>` | WAF (read) |
| R | `GET https://<host>/healthz`, 9 routes with no credentials, 8 with a wrong-tenant token (empty bodies), TCP :80, TLS handshakes | public host |
| M* | bounded `GET https://<host>/healthz` burst | public host; it may rate-limit the operator's IP for the rule window |

### 4. tenant-isolation and leak scan

Tenant A's token must never retrieve tenant B's sessions through search (B's
canaries and summary terms), list (a prefix shared by the whole run), by-id
(`/v1/pipeline/traces/:id`) or manifest-status. The check fails on any
cross-tenant id or canary in a response. It also requires positive controls:
each tenant must see its own sessions.

The check then scans service logs and the S3 trace archive
(`telemetry/traces/`, gzip OTLP/JSON, held in memory only) for planted
text:

- transcript-only canaries;
- summary terms;
- the fake AWS key;
- every minted token.

Hits are reported by canary tag (a SHA-256 prefix) and offset, never by text.
An empty trace archive fails the scan, because a scan of nothing proves nothing.

| | Command | Touches |
| --- | --- | --- |
| R | batched psql search as each tenant (as in planted-trace) | HoopDb (read) |
| R | `HTTP GET <ingest>/v1/pipeline/traces/<id>` and `/v1/pipeline/manifests/status?key=…` with each tenant's token | AlleyOop (read) |
| R | `kubectl <ctx> -n <ns> logs deployment/<d> --all-containers --since=3h --prefix` for each core deployment and RelayAuth | logs |
| R | `aws s3api list-objects-v2 --prefix telemetry/traces/`, then `aws s3 cp s3://…/<key> - \| gzip -dcf` for each batch in the window | trace archive (read) |

### 5. v1-search-scope (metadata gate)

This check labels V1, honestly. Full-transcript search is UNSUPPORTED on V1
(HoopDb Postgres). The report states this in `scope` and in this check's
`unsupported` status. The probe behind the label searches each tenant's
transcript-only canaries through V1 and must get zero rows. Any hit is a
failure, because it means transcript text reached `search_text`.

### 6. transcript-search (transcript gate)

This gate runs full-transcript search end to end against a configured
ClickHouse or ObsessionDB backend over the ClickHouse HTTP interface. It
requires two things:

- every planted session's transcript-only canary is found by its own tenant;
- no tenant's credentials return another tenant's session or canary.

It reports landed→searchable freshness and query p50, p95 and p99 plus the
error rate.

Tenancy must be enforced by the backend. The query carries no tenant predicate,
so a missing row policy fails the gate. A cross-tenant probe that errors also
fails the gate ("isolation unproven"). An error is never counted as a clean,
empty result.

The feed can be `pipeline` or `harness-synthetic`:

- `pipeline` waits for the product to project transcripts into the backend. No
  V2 projector exists in this repository, so today this feed times out and the
  gate fails honestly.
- `harness-synthetic` writes the scrubbed planted transcripts itself, and the
  report says so in the backend label.

| | Command | Touches |
| --- | --- | --- |
| M* | `HTTP POST <endpoint>/?query=INSERT INTO <db>.<table> FORMAT JSONEachRow` ×N (synthetic feed only) | transcript backend |
| R | `HTTP POST <endpoint>/?param_q=<canary>&readonly=1` with each tenant's own credentials | transcript backend (read) |

**Verdict rule.** The report records which backend each search gate exercised,
in `searchGates.metadata.backend` (for example `HoopDb Postgres (CNPG
96bulls/hoopdb, …)`) and `searchGates.transcript.backend` (for example
`ClickHouse Cloud <host> (<db>.<table>; feed: harness-synthetic)`, `ObsessionDB
<host> (…)`, or `none`). In a dry run each label is prefixed `planned, not
exercised:`.

V1's UNSUPPORTED label never produces an overall PASS. The proof passes only
when every check passes, with `v1-search-scope` allowed to be `unsupported`,
and when `transcript-search` passes against a configured backend. With
`backend: none`, the verdict is INCOMPLETE.

### 7. stale-summary (#38 HOLD regression)

This check covers the #38 HOLD fixes `99e96d1` and `134afa8`. Two sessions per
tenant get revision 2, which appends turns and so changes the extractive
summary's counts. For half of them, the check deletes the revision-1
`summary/short.json.enc` before the re-ingest; the bucket is versioned, so this
leaves a delete marker.

The directory, and V1 search, must never keep serving revision 1's
`short_summary`. Two end states are acceptable: revision 2's text, or a cleared
summary. The check also fails if `generated_at` goes backwards.

| | Command | Touches |
| --- | --- | --- |
| M* | `aws s3api delete-object --bucket … --key …/summary/short.json.enc` | Locker (delete marker) |
| M* | `HTTP POST` chunk + manifest revision 2, carrying `previousVersion` | Locker, HoopDb |
| R | psql directory and search polls | HoopDb (read) |

### 8. restart-persistence

Every Relaycast assertion must PASS in both phases, or the check fails. A
missing, failed or unrun assertion is a failure, never a partial pass.

- **Before the restart:** `health`, `workspace`, `agent`, `message.send` and
  `message.receive`.
- **After the restart:** the same five, plus `message.persisted`. The workspace
  key and agent token must still work, and the pre-restart message must still
  be there.

The check runs these steps in order:

1. Run a Relaycast roundtrip from inside the cluster (workspace, agent, message
   write and read).
2. Run `kubectl rollout restart` on each core deployment.
3. Fail over the CNPG primary by deleting the primary pod.
4. Restart Relaycast.
5. Read the Relaycast message back. The probe keeps its workspace key in the
   probe pod's in-memory `emptyDir`, never on the operator machine.
6. Search every session that was searchable before. Each one must still be
   searchable.

The failover is `blocked` when the cluster has only 1 instance.

| | Command | Touches |
| --- | --- | --- |
| M* | `kubectl <ctx> -n relaycast run proof-relaycast-aws-proof --image=<probeImage> … sleep 7200` | probe pod (label `96bulls.proof=true`) |
| M* / R | `kubectl <ctx> -n relaycast exec -i proof-relaycast-aws-proof -- bun run - before\|after` (`before` writes) | Relaycast |
| M* | `kubectl <ctx> -n 96bulls rollout restart deployment/<each core deployment>` (+ `rollout status`) | pods replaced |
| M* | `kubectl <ctx> -n 96bulls delete pod <primary> --wait=false` (+ `wait --for=condition=Ready cluster/hoopdb`) | CNPG failover |
| M* | `kubectl <ctx> -n relaycast rollout restart deployment/relaycast` | pod replaced; PVC kept |

### 9. upgrade-rollback

The check runs `helm upgrade` to `candidateChartVersion` with
`--reuse-values --wait` and without `--atomic`, so a failed upgrade stays
visible. It then rolls back to the prior revision. Every planted session must
stay searchable after each step.

| | Command | Touches |
| --- | --- | --- |
| R | `helm --kube-context <ctx> -n 96bulls history 96bulls -o json` | release |
| M* | `helm … upgrade 96bulls 96bulls --repo … --version <candidate> --reuse-values --wait --timeout 10m` | release |
| M* | `helm … rollback 96bulls <prior> --wait --timeout 10m` | release |

### 10. backup-restore

The check first confirms that S3 versioning is `Enabled` on the Locker bucket.
It then runs an on-demand CNPG `Backup` and recovers it into a separate cluster
`hoopdb-restore-proof`, never in place. It counts the planted sessions in the
restored cluster and measures two numbers:

- **RPO**: the data-loss window for planted writes committed before the backup
  stopped. It is 0 when nothing was lost.
- **RTO**: the time from the restore request until the restored cluster is
  Ready.

Afterwards it deletes the restored cluster and the `Backup` CR. The backup
objects stay under the bucket's retention.

The check fails, and never reports an RPO of 0, in any of these cases:

- no planted write exists in the source before the backup stopped;
- the restored cluster cannot be queried, or holds none of the planted
  sessions;
- either cleanup step fails.

| | Command | Touches |
| --- | --- | --- |
| R | `aws s3api get-bucket-versioning`, `kubectl … get cluster hoopdb -o jsonpath={.spec.backup.barmanObjectStore.destinationPath}` | config (read) |
| M* | `kubectl <ctx> apply -f -` Backup (stdin) + `wait` | backup bucket (base backup + WAL) |
| M* | `kubectl <ctx> apply -f -` Cluster `hoopdb-restore-proof` (recovery) + `wait` | new 10Gi PVC |
| M* | `kubectl … delete clusters.postgresql.cnpg.io hoopdb-restore-proof`, `delete backups… <name>` | cleanup |

### 11. teardown

The teardown runs in two phases:

1. **While the cluster exists**, the check:
   - deletes every object labelled `96bulls.proof=true`;
   - uninstalls the release;
   - deletes the namespace;
   - verifies that nothing labelled remains.

   The check is then `blocked` and lists the tagged AWS resources that are
   still pending.
2. **After Khaliq runs `bin/96bulls down aws-proof`** (an Alchemy destroy, which
   this harness never runs), rerun the teardown and pass it the report of the
   full run:
   `bun run proof:aws:teardown --confirm-live --read-only --prior-report .96bulls/aws-proof/<run id>/report.json`. The check passes only if `aws eks list-clusters` no longer
   names the cluster and no resource tagged `96bulls:stack=aws-proof` remains.
   A KMS key in `PendingDeletion` (the 30-day window) is the expected end state.

A failed cluster listing never counts as "gone".

**One report for the whole proof.** `--prior-report` merges an earlier report of
the same proof into this run's report, and the merge is deterministic:

- for each check, this run's result replaces the prior's;
- the prior's file SHA-256 and run id are recorded in `phases`;
- the verdict is recomputed over the merged checks.

The merge is refused when the prior report:

- targets another stage, cluster, context, account or region;
- is a dry run being carried into a live run;
- is newer than this run;
- does not validate.

So the full live run (with teardown `blocked`), merged with the phase-2
teardown run, produces one report whose verdict covers both.

| | Command | Touches |
| --- | --- | --- |
| R | `aws eks list-clusters` | EKS (list) |
| M* | `kubectl <ctx> delete backups…,clusters…,pods -A -l 96bulls.proof=true` | proof objects |
| M* | `helm --kube-context <ctx> -n 96bulls uninstall 96bulls --wait` | release |
| M* | `kubectl <ctx> delete namespace 96bulls --wait=true` | namespace and PVCs |
| R | `kubectl … get all,pvc,backups…,clusters… -A -l 96bulls.proof=true`, `get namespace 96bulls` | verify |
| R | `aws resourcegroupstaggingapi get-resources --tag-filters Key=96bulls:stack,Values=aws-proof`, `aws kms describe-key` | verify |
| Khaliq | `bin/96bulls down aws-proof` | the whole stage; printed, never run |

## Steps that need Khaliq's direct OK

- Creating the proof stage: `aws-inputs.json` clearance and `bin/96bulls up aws-proof`.
- Every `--confirm-live` run, read-only runs included, until the stage exists
  and he has cleared it.
- Each `M*` step above, in particular:
  - minting the tenant tokens;
  - planting sessions;
  - deleting the summary objects;
  - the rollout restarts;
  - deleting the CNPG primary pod;
  - the Helm upgrade and rollback;
  - the Backup and restore cluster;
  - the WAF burst;
  - the synthetic transcript insert;
  - every teardown deletion.
- Putting the RelayAuth bootstrap key and the transcript backend credentials
  into 1Password, and piping them into the environment.
- `bin/96bulls down aws-proof`.

## Report

`.96bulls/aws-proof/<run id>/report.json` is gitignored and written with mode
0600. Its schema is `96bulls.aws-e2e-proof/v1`, defined in
`src/proof/aws/report.ts`. The report records the following:

- `mode`;
- `target`, including the observed account;
- `scope`, the V1 statement;
- `searchGates`;
- one entry per check;
- `verdict` (`pass`, `fail` or `incomplete`).

The CLI exits with 0 on pass, 1 on fail, 2 on a usage or guard refusal, and 3
on incomplete.

## Local evidence

On 2026-10-30, `bun run proof:aws fresh-install --target kind-readonly
--read-only` ran against `kind-96bulls-e2e`. It ran only `kubectl get pods`,
`helm list` and `helm show values`.

It found 7 running images that are not digest-pinned:

- the four `96bulls-services:summary-4386bee` services;
- the CNPG `bootstrap-controller`;
- the CNPG operator `manager`;
- `otel/opentelemetry-collector-contrib:0.163.2`.

It matched the released digests for CNPG Postgres, MinIO, RelayAuth and
Relaycast. That kind install is a dev install and not a proof target. The result
shows that the check distinguishes tag references from pinned digests.

## Known limits and open questions

- **Search RPC claims.** In `rpc-sql` mode, V1 search calls the RPC as the
  Worker role, with claims set by the harness. This exercises the RPC's tenant
  binding but not JWT signatures. The `postgrest` mode, the real PostgREST
  path, needs the #63 issuer. That mode is configurable but not implemented
  here.
- **Relaycast field names.** The probe uses `POST /v1/channels/general/messages
  {text}`, taken from the self-hosting docs. The field names are not verified
  against v8.0.0.
- **Synthetic transcript feed.** The synthetic feed proves the backend's
  search, isolation and freshness. It does not prove a product projector, and
  none exists here yet.
- **`proof-sessions.agentrelay.com`.** The record and its ACM certificate must
  exist and point at the proof ALB. Until they do, the DNS guard refuses the
  active ingress probes.
- **Multi-org ingest.** AlleyOop's org resolver (`PIPELINE_ORG_ID` and prefix
  ownership) must map both proof tenants on this stage. A single-org
  configuration makes tenant B's planting fail with 403, which the report shows.
