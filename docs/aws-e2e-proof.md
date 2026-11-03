# AWS self-host E2E proof (harness and runbook)

This harness proves the self-hosted 96Bulls deployment end to end on one
isolated AWS stage: account `500268966557`, EKS cluster `96bulls-aws-proof`,
or, with `--target pippen-sandbox`, the Pippen Tools sandbox of agent
`aws-proof` (account `361835732085`, EKS cluster `96bulls-aws-proof-sandbox`;
see [Pippen sandbox target](#pippen-sandbox-target)).
It has not been run against AWS. Nothing in this repository has created the
proof stage. The only local run so far was a read-only `fresh-install` against
`kind-96bulls-e2e` (see [Local evidence](#local-evidence)).

The harness lives in `src/proof/aws/`. The entry point is:

```sh
bun run proof:aws [checks...|all] [--read-only] [--confirm-live] [--target proof|pippen-sandbox|kind-readonly] [--inputs path] [--sessions n] [--out dir]
bun run proof:aws:teardown [--confirm-live]
```

Related work: #59 is the search benchmark on a local stand-in, and its
nearest-rank percentile definition is reused here. #38 and #48 define the
session directory, `short_summary` and the V1 search RPC. #63 is the design for
the tenant JWT issuer.

## Pippen sandbox target

Khaliq decided on 2026-11-01 to run the EKS proof in a Pippen Tools sandbox,
so that teardown is a verified sweep. The harness has exactly two AWS targets,
both hardcoded in `src/proof/aws/guard.ts` (`PROOF_TARGETS`) and
`src/proof/aws/fence.ts`, and `--target` chooses one. Nothing in config or
the environment can add a third target or widen either one.

| | `--target proof` (default) | `--target pippen-sandbox` |
| --- | --- | --- |
| Account | `500268966557` (beside aws-sessions-dev) | `361835732085` (Sandbox OU, 96Bulls org) |
| Region | `us-west-2` | `us-west-2` |
| Stage / cluster | `aws-proof` / `96bulls-aws-proof` | `aws-proof-sandbox` / `96bulls-aws-proof-sandbox` |
| Caller | any principal in the account | exactly `arn:aws:sts::361835732085:assumed-role/agent-session/agent-<session_id>`, `<session_id>` from `--pippen-session` (required); post-destroy: any session of the recreated `agent-session` role in `361835732085` |
| Host | `proof-sessions.agentrelay.com` | `proof-sandbox-sessions.agentrelay.com` |
| Buckets | `96bulls-aws-proof-{locker,hoopdb-backups}-500268966557` | `pippen-sbx-96bulls-aws-proof-sandbox-{locker,backups}-361835732085` (the pippen-sbx- prefix is what pippen-sweeper may delete; `backups`, since `hoopdb-backups` with the prefix is 64 characters, over S3's 63) |
| Stage plan | `docs/plans/aws-proof.*` | `docs/plans/aws-proof-sandbox.*`, must carry `pippenSandbox`, never `accountShared` |
| Inputs | `.96bulls/aws-proof/inputs.json` | `.96bulls/aws-proof-sandbox/inputs.json` |
| Teardown | `bin/96bulls down aws-proof` | Ingress and DNS first, harness phase 1, `bin/96bulls down aws-proof-sandbox`, then `pippen sandbox down aws-proof` (the sweep of record; steps 4-11 below) |

Every check that pins the sessions-dev target pins the sandbox target with
the same strictness: the STS account, the cluster name, the cluster ARN from
`describe-cluster`, the kube context binding, the region, the config fence,
the stage plan and the prior report. The sandbox adds one more: before the
cluster is described, and again before every step, the caller's
`sts get-caller-identity` ARN must be exactly the pippen agent-session role
session of the current lease,
`arn:aws:sts::361835732085:assumed-role/agent-session/agent-<session_id>`
(`wrong_caller` otherwise). `<session_id>` is the lease UUID shown as
`session_id` by `pippen sandbox status aws-proof` after `up`. Pass it as
`--pippen-session <session_id>`. It is not secret, it can only narrow the
caller, and it must be a lowercase v4 UUID. A live or read-only sandbox run
without it, or with a malformed one, is refused (`missing_pippen_session`). Each target refuses the other's account,
cluster, context, buckets, ARNs and host. The sessions-dev target's behavior is
unchanged.

**Credentials: two `pippen sandbox exec` modes, one per kind of step.**
Alchemy (`bin/96bulls plan|up|down aws-proof-sandbox`) reads only static env
keys: `@alchemy.run/aws` resolves `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`
(+ `AWS_SESSION_TOKEN`) from the environment, or else its own `~/.alchemy`
profile (stored keys, SSO, console login). It never reads `AWS_PROFILE` and
has no `credential_process` path, so under the default `pippen sandbox exec`
it fails with "Provider 'AWS' is not configured in profile 'default'". Run
every Alchemy step with `pippen sandbox exec aws-proof --static-credentials --`:
fresh credentials per invocation, at most 1 hour, no refresh, so one step per
invocation. The harness (`bun run proof:aws ...`) and kubectl use the default
`pippen sandbox exec aws-proof --` (a refreshing `credential_process`
profile), because a proof run can outlast an hour.

`bin/96bulls` checks this before anything runs (`src/cli/sandbox-credentials.ts`),
and the stack checks it again at load, so a direct `alchemy` run is held to it
too (`stacks/aws/sandbox-credentials.ts`):

- static keys and no profile: used as given. All three keys are required, and
  if `AWS_CREDENTIAL_EXPIRATION` (set by `--static-credentials`) leaves less
  than 50 minutes, the step is refused.
- the default exec's `pippen-sandbox` profile and no keys: `bin/96bulls`
  resolves it once with
  `aws configure export-credentials --profile pippen-sandbox --format process`.
  That runs pippen's `credentials-process`, which issues a fresh agent-session
  session on each call. The keys go into the env of the child processes only,
  with `AWS_CONFIG_FILE=/dev/null` and no `AWS_PROFILE`, and are never
  written to disk, put in argv or printed. This is accepted only when the
  profile passes the harness's own check (`resolveToolCredentials`: exactly
  pippen's profile for this lease and account) and the credentials have at
  least 50 minutes left. They do not refresh either. `--static-credentials`
  is still the documented path, because pippen gives that contract directly.
- anything else is refused before Alchemy runs, with the
  `pippen sandbox exec aws-proof --static-credentials -- bin/96bulls <verb> aws-proof-sandbox`
  command: any other profile, keys mixed with a profile, partial keys, no
  credentials, or a resolution that fails or comes back short-lived.

Either way, the stack's caller pin still applies on top: the caller must be
exactly `assumed-role/agent-session/agent-$NINETY_SIX_BULLS_PIPPEN_SESSION`,
and `PIPPEN_SANDBOX_SESSION_ID`, which exec sets in both modes, must name the
same lease.

The flow (each live step needs Khaliq's OK; none has been run). `pippen sandbox exec`
replaces every inherited `AWS_*` variable and keeps the rest, so the
`NINETY_SIX_BULLS_*` exports reach the child:

```sh
# 0. Lease and session (once; again after every `pippen sandbox up`).
pippen sandbox up aws-proof
pippen sandbox status aws-proof            # session_id
export NINETY_SIX_BULLS_PIPPEN_SESSION=<session_id>
export NINETY_SIX_BULLS_CONFIRM_ACCOUNT=361835732085   # AWS_REGION comes from exec

# 1. Capture the plan (Alchemy: static, < 1 h). Review docs/plans/aws-proof-sandbox.*.
pippen sandbox exec aws-proof --static-credentials -- bin/96bulls plan aws-proof-sandbox --capture

# 2. Apply under the approval bound to that capture (Alchemy: static, < 1 h; the
#    EKS Auto Mode create alone is ~15-30 min, so start with a fresh invocation).
NINETY_SIX_BULLS_CONFIRM_STAGE=aws-proof-sandbox \
  pippen sandbox exec aws-proof --static-credentials -- bin/96bulls up aws-proof-sandbox

# 3. The proof checks, teardown last and separately (harness: refreshing profile).
pippen sandbox exec aws-proof -- bun run proof:aws \
  fresh-install planted-trace public-ingress tenant-isolation v1-search-scope transcript-search \
  stale-summary restart-persistence upgrade-rollback backup-restore \
  --target pippen-sandbox --pippen-session "$NINETY_SIX_BULLS_PIPPEN_SESSION" --confirm-live
```

If `up` runs out of its hour part-way, do not rerun it as it stands. The state
has changed, so capture again (step 1) and apply that capture.

Teardown, in this order:

```sh
# 4. Delete the Ingress first, so EKS Auto Mode removes the ALB, target groups and
#    security groups it made from it before the cluster and VPC go (refreshing profile).
pippen sandbox exec aws-proof -- sh -c 'k=$(mktemp) && aws eks update-kubeconfig --name 96bulls-aws-proof-sandbox --region us-west-2 --kubeconfig "$k" >/dev/null && kubectl --kubeconfig "$k" -n 96bulls delete ingress 96bulls-ingest --wait=true --timeout=10m; s=$?; rm -f "$k"; exit $s'
pippen sandbox exec aws-proof -- aws elbv2 describe-load-balancers --region us-west-2 --query 'LoadBalancers[].LoadBalancerName'   # must be []

# 5. DNS: external-dns runs with policy upsert-only, so it never deletes its records.
#    Remove the CNAME and its ownership TXT records by hand (Cloudflare, not AWS;
#    CF_API_TOKEN is the cloudflare-dns token, in the env for these commands only).
ZONE=34afb0e8fc19f2b6fd025668e880a889 H=proof-sandbox-sessions.agentrelay.com
curl -fsS -H "Authorization: Bearer $CF_API_TOKEN" "https://api.cloudflare.com/client/v4/zones/$ZONE/dns_records?per_page=100&name.contains=$H" \
  | jq -r '.result[] | "\(.id) \(.type) \(.name)"'
#    Delete each id whose name is $H, _externaldns.$H or _externaldns.cname-$H, and nothing else:
curl -fsS -X DELETE -H "Authorization: Bearer $CF_API_TOKEN" "https://api.cloudflare.com/client/v4/zones/$ZONE/dns_records/<id>"
#    Re-run the list: it must print nothing.

# 6. Harness phase 1: proof objects, release, namespace (refreshing profile). This
#    report must stay unmerged, because it is the post-destroy prior in step 10.
pippen sandbox exec aws-proof -- bun run proof:aws teardown \
  --target pippen-sandbox --pippen-session "$NINETY_SIX_BULLS_PIPPEN_SESSION" --confirm-live

# 7. Alchemy destroy, under an approval whose operations include "destroy" (static, < 1 h).
NINETY_SIX_BULLS_CONFIRM_DESTROY=aws-proof-sandbox \
  pippen sandbox exec aws-proof --static-credentials -- bin/96bulls down aws-proof-sandbox

# 8. The sweep of record: must report clean: true.
pippen sandbox down aws-proof

# 9. A fresh lease for verification (recreates only role/pippen/agent-session).
pippen sandbox up aws-proof
export NINETY_SIX_BULLS_PIPPEN_SESSION=<new session_id from `pippen sandbox status aws-proof`>

# 10. Post-destroy absence check (refreshing profile; any session of the recreated role).
pippen sandbox exec aws-proof -- bun run proof:aws:teardown --target pippen-sandbox --post-destroy \
  --confirm-live --prior-report .96bulls/aws-proof-sandbox/<step 6 run id>/report.json

# 11. Remove the verification session's role.
pippen sandbox down aws-proof
```

The post-destroy check (step 10) accepts any session of the
`agent-session` role in `361835732085`, because the session name differs
per session; live steps still need the pinned caller above. Besides the
checks in [teardown](#11-teardown), it lists resources tagged
`96bulls:stack=aws-proof-sandbox`, `alchemy::stage=aws-proof-sandbox` and
`pippen:sandbox=true`, and requires none. The one exception is the exact
ARN of the fresh agent-session role; exemptions match by exact ARN, never
by tag. A second `pippen:sandbox` resource, a look-alike role ARN or any
stage-tagged leftover fails the check. Pippen's baseline (StackSet
resources such as the `pippen-agent-session-boundary` policy, tagged
`pippen:baseline=true`) carries none of the queried tags, so it is never
listed.

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

**Stage plan binding** (`src/proof/aws/provenance.ts`). A live or read-only
run on the proof target refuses to start unless the captured Stage A
create-only plan from #75 is present and verifies:

- `docs/plans/aws-proof.provenance.json`, `aws-proof.plan.txt` and
  `aws-proof.inputs.sanitized.json` must all be present.
- Each file's SHA-256 must equal the one recorded in the provenance file.
- The provenance must be captured from a clean commit, for stage `aws-proof`,
  account `500268966557` and region `us-west-2`.
- The plan must create `96bulls-aws-proof`.
- The inputs must name `proof-sessions.agentrelay.com` and pin the services and
  Relaycast image digests.

The plan text is also parsed: its `Plan: …` summary, and every
`[resource] op` line. Only `create` and `noop` are accepted, and the summary
must agree with the lines. An unknown line, a missing `[cluster]`, or any
`update`, `replace`, `delete`, `import` or `adopt` is refused, even when the
cluster appears. For example, the #75 continuation capture at `ecf70fd` (one
`replace` of a never-created record) is refused, as intended. A reviewed stage
change may carry `update` only under an approval bound to the exact capture:
`--allow-plan-update <provenanceSha256>:<planSha256>:<inputsSha256>:<sourceCommit>`.
The harness recomputes each value from the loaded files and refuses any
mismatch. Even with a matching approval, `update` is admitted only for
`REVIEWED_PLAN_UPDATES` (`96bulls` and `alleyoop-role`, for the two-tenant
change). A resource name alone is never accepted, and `replace`, `delete`,
`import` and `adopt` are refused unconditionally.

The live cluster must also be the one that plan created. `describe-cluster`
must show the cluster was created after the plan was captured when the plan
creates it, or before the capture when the plan leaves it as `noop` (a
continuation plan), and `fresh-install`
requires the running images to include the plan's digests. Plan digests take
precedence over chart defaults and operator inputs. On this branch the plan
files are on #75, so live runs refuse until #75 lands.

**Cluster identity is bound to live AWS** (`src/proof/aws/cluster-binding.ts`).
Before the guard runs, the harness makes these reads:

1. It runs `aws sts get-caller-identity`.
2. It runs `aws eks describe-cluster --name 96bulls-aws-proof --region
   us-west-2`, but only when the caller is in the proof account.
3. It runs `kubectl config view --raw -o json`, parsed in memory only.

The described cluster must be the proof ARN, `ACTIVE`, an EKS endpoint in
`us-west-2`, and carry a CA. The operator's kube context must point at exactly
that server with exactly that `certificate-authority-data`, and with no TLS
override or proxy. Otherwise the run is `cluster_unbound` and refused. The
cluster ARN the guard checks comes from AWS, not from how a kubeconfig names it.

Every kubectl and helm step then runs with `KUBECONFIG` set to a kubeconfig the
harness generates from the describe result. That file has one cluster, one
context and `aws eks get-token` exec credentials. It is written to the run
directory with mode 0600 and deleted after the run. The operator's kubeconfig
is never used by a step.

Every aws, kubectl and helm call, and the kubeconfig's exec plugin, gets an
environment built from scratch, not filtered from the operator's:

- AWS credentials only as the explicit `AWS_ACCESS_KEY_ID`,
  `AWS_SECRET_ACCESS_KEY` and `AWS_SESSION_TOKEN` variables. A live run without
  them is refused; use `eval "$(aws configure export-credentials --format env)"`.
- `AWS_CONFIG_FILE` and `AWS_SHARED_CREDENTIALS_FILE` set to `/dev/null`, so no
  profile, `endpoint_url`, `ca_bundle`, `credential_process` or role chain from
  a file can apply.
- The region pinned to `us-west-2`, regional STS, FIPS and dual-stack off, and
  instance metadata disabled.
- No `AWS_PROFILE`, `AWS_ENDPOINT_URL*`, `AWS_CA_BUNDLE`, proxy, `HELM_KUBE*` or
  operator `KUBECONFIG`. Only `PATH`, `HOME`, `LANG` and `TMPDIR` pass through. A kubectl or helm step without the generated
kubeconfig is refused with `kubeconfig_unbound`.

On `--target pippen-sandbox` only, the harness also accepts the refreshing
profile that `pippen sandbox exec aws-proof -- bun run src/proof/aws/cli.ts ...`
gives its child by default (pippen-tools 327c263): `AWS_PROFILE=pippen-sandbox`,
`AWS_CONFIG_FILE=<exec's private dir>/config`, `AWS_SDK_LOAD_CONFIG=1`,
`AWS_SHARED_CREDENTIALS_FILE=/dev/null` and no static keys
(`resolveToolCredentials` in `cluster-binding.ts`). The config must hold exactly
one section, `[profile pippen-sandbox]`, with only `region` and a
`credential_process` that runs `pippen sandbox credentials-process aws-proof`;
SSO settings, role chains, `endpoint_url`, `ca_bundle` or any other key are
refused. In that mode every aws, kubectl and helm call (and the generated
kubeconfig's `aws eks get-token` exec plugin) gets that `AWS_CONFIG_FILE` and
`AWS_PROFILE` instead of `/dev/null`, so credentials refresh through the
credential_process for the length of the lease; every other pin above stays.
Refused on the sandbox: any other profile (ambient `default`, saved or SSO),
`AWS_DEFAULT_PROFILE`, static keys mixed with a profile, a shared credentials
file other than `/dev/null`, and a `PIPPEN_SANDBOX_SESSION_ID` other than
`--pippen-session` (or a `PIPPEN_SANDBOX_ACCOUNT_ID` other than 361835732085).
`pippen sandbox exec --static-credentials` (explicit keys) works as before. The
caller must still be exactly
`arn:aws:sts::361835732085:assumed-role/agent-session/agent-<--pippen-session>`.
The proof (sessions-dev) target is unchanged: explicit keys only.
The stack (`stacks/aws/sandbox-rules.ts`) applies the same lease check:
`PIPPEN_SANDBOX_SESSION_ID`, when set, must equal
`NINETY_SIX_BULLS_PIPPEN_SESSION`.

**The guard** (`src/proof/aws/guard.ts`) runs before the first step and again
before every step. It has no override. Any non-dry run against AWS needs all of
the following:

- `--confirm-live` on the command line;
- `aws sts get-caller-identity` returns the account `500268966557` (on
  `--target pippen-sandbox`: `361835732085`, and the ARN is exactly
  `arn:aws:sts::361835732085:assumed-role/agent-session/agent-<--pippen-session>`). The
  harness checks the observed identity, not a configured value;
- the cluster name is exactly `96bulls-aws-proof`, and the region of every EKS
  ARN (the kube context's cluster entry and the context name itself) is
  `us-west-2`;
- the kube context points at
  `arn:aws:eks:<region>:500268966557:cluster/96bulls-aws-proof`;
- no name is production-shaped. A name containing `prod`, `production`,
  `sessions-dev`, `hosted` or `live` as a segment is refused.

**The target fence** (`src/proof/aws/fence.ts`) checks *what* each step
touches, not just who runs it. Every allowed target is hardcoded there for the
aws-proof stage, and nothing in the fence is derived from `inputs.json`.
Changing a target means a reviewed code change to `fence.ts`.

| Target | Allowed value |
| --- | --- |
| account and region | `500268966557`, `us-west-2` |
| kube context | `arn:aws:eks:us-west-2:500268966557:cluster/96bulls-aws-proof` |
| PostgREST (V1 search, #73) | `96bulls/session-directory-postgrest:3000` |
| probe images | `PROOF_PROBE_IMAGES` (Stage A services image) |
| ingest host and origin | `proof-sessions.agentrelay.com`, `https://proof-sessions.agentrelay.com` |
| buckets | `96bulls-aws-proof-locker-500268966557`, `96bulls-aws-proof-hoopdb-backups-500268966557` |
| release and chart repo | `96bulls`, `https://AgentWorkforce.github.io/helm-charts` |
| namespaces | `96bulls`, `relayauth`, `relaycast`, `cnpg-system` |
| namespaces a delete may name | `96bulls`, `relaycast` |
| services | `96bulls-alleyoop`, `relayauth`, `relaycast` |
| deployments | the four `96bulls-*` core deployments, `relayauth`, `relaycast` |
| CNPG clusters | `hoopdb`, `hoopdb-restore-proof` |
| psql databases | `hoopdb` |
| transcript backend | `PROOF_TRANSCRIPT_HOSTS` (empty), table `proof.transcripts` |

The transcript host list is empty until the synthetic ClickHouse Cloud host is
added in a reviewed commit, so until then the transcript gate is refused and the
proof stays INCOMPLETE.

The fence applies twice:

- **On the config.** Every target field must equal its constant. The config is
  also refused if any string in it is production- or shared-shaped (`prod`,
  `production`, `sessions-prod`, `sessions-dev`, `staging`, `hosted`), or if
  any URL carries userinfo (`https://user:pass@host`). This applies even in the
  proof account.
- **On every step, in every mode including dry-run** (`src/proof/aws/ops.ts`).
  Before anything spawns, each command is parsed into one typed operation, and
  that operation must match an explicit allowlist:
  - **Programs.** Only `kubectl`, `helm` and `aws` are spawned, without a shell.
    `sh -c`, `bash -c`, `env`, absolute paths and every other program are
    refused, so nothing can hide inside a wrapper. In-process `HTTP` and
    `PROBE` actions are never spawned, and their hosts are fenced.
  - **Flag normalization.** Every argv is normalized first:
    - `--x v`, `--x=v`, `-n v`, `-nv` and `-n=v` all become one canonical
      flag, and aliases such as `--namespace` and `--selector` map to it.
    - Any canonical flag that appears twice is refused, so last-wins forms
      like `--context A --context=B` or `-n 96bulls -n=x` cannot sneak through.
    - Unknown flags and combined short flags (`-it`) are refused.
    - Flags that can retarget a tool are refused outright, in any spelling:
      - kubectl: `--server`/`-s`, `--cluster`, `--user`, `--token`, `--as*`,
        `--kubeconfig`, CA/TLS overrides, `--username`/`--password`;
      - helm: `--kube-*`, `--kubeconfig`;
      - aws: `--profile`, `--endpoint-url`, `--no-verify-ssl`, `--ca-bundle`.
  - **Target flags.**
    - kubectl must start with exactly `--context` and the proof cluster ARN. A
      missing, other or repeated context is refused.
    - helm must start with `--kube-context` and the same ARN; only `helm show
      values` of the AgentWorkforce charts is exempt.
    - Every aws call must carry exactly `--region us-west-2`. Every ARN must
      name account `500268966557` and region `us-west-2`.
    - These are always refused: `-A`, `--all-namespaces`, `--kubeconfig`,
      `--server`, `--token`, `--as`, `--user`, `--cluster`, `--dbname`,
      `--profile`, `--endpoint-url`.
  - **Who decides "mutating".** The parser decides, not the step's label. A
    mutating operation is admitted only as an exact entry:
    - `rollout restart` of a named deployment in its namespace;
    - `delete pod` of `hoopdb-<n>` or the Relaycast probe pod;
    - `delete` of the restore cluster, a `proof-*` Backup, or the
      proof-labelled objects in `96bulls` or `relaycast`;
    - `delete namespace 96bulls`;
    - `apply` of a proof-labelled CNPG `Backup` (cluster `hoopdb`) or the
      restore `Cluster`, with the manifest checked;
    - `run` and `exec` of the Relaycast probe. The image must be in the
      hardcoded allowlist `PROOF_PROBE_IMAGES` (the Stage A services image),
      not just any digest-pinned image, and must be the stage plan's services
      digest;
    - `helm upgrade`, `rollback` and `uninstall` of release `96bulls`;
    - `delete-object` of a planted session's `summary/short.json.enc` in the
      Locker bucket.

    A step that mutates but is labelled as a read is refused. Read-only mode
    skips everything the parser classifies as mutating.
  - **relayauth** is read-only, except `rollout restart deployment/relayauth`.
  - **Reads** are allowlisted the same way:
    - psql must be exactly `psql -d hoopdb -X -At -v ON_ERROR_STOP=1 -f -` in a
      `hoopdb-<n>` pod;
    - port-forwards go only to the named services;
    - logs come only from the named deployments.

  A refused step is recorded `refused` and never spawned. Every printed or
  recorded command is sanitized, so a credential never appears in output,
  dry-run plans included.

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
   `hoopdb.cluster.backup.barmanObjectStore` pointing at `96bulls-aws-proof-hoopdb-backups-500268966557` (the
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
| R | `helm --kube-context <ctx> -n <ns> list -o json` for each proof namespace | release lists |
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
| M* | `HTTP POST <relayauth>/v1/tokens/relayhistory-assertion` for each planted machine | RelayAuth (60s machine-bound tokens) |
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

### V1 search evidence

The only V1 search evidence that counts toward a gate is the path a customer's
request takes:

1. The tenant's RelayAuth token is exchanged at `POST
   /v1/tokens/session-directory-search` (RelayAuth #116). The `x-api-key` comes
   from `PROOF_SEARCH_EXCHANGE_KEY`, through `kubectl port-forward` only.
2. The exchange returns a 60-second tenant search token.
3. With that token, the harness calls `POST /rpc/search_session_directory` on
   the self-hosted PostgREST (#73), with `Content-Profile: sessions`. This is
   `search.mode: "postgrest-jwt"`, the default.

The exchange runs inside the cluster, from the search client pod
`96bulls/proof-search-client`, and goes to RelayAuth's TLS Service:

- **Labels.** The pod carries both caller labels,
  `relayauth.agentrelay.com/client=tls` and
  `96bulls.agentrelay.com/search-client=true`.
- **Trust.** It mounts ConfigMap `relayauth-ca` and calls
  `https://relayauth-tls.relayauth.svc:8443` with that CA. It never calls the
  plain `relayauth:8787`.
- **PostgREST.** It then calls
  `http://session-directory-postgrest.96bulls.svc:3000`.
- **Secrets.** The exchange key and the tenant's token reach the pod only on
  stdin over `kubectl exec`.
- **Exact spec.** The pod spec, labels and override are pinned exactly, so an
  extra volume or a privileged container is refused.

The `--stack-outputs <file>` option reads `eks.harness` from the stack's
outputs. Its values must equal the hardcoded fence, and they never widen it.

Stage A has `sessionDirectory.enabled: false`, so PostgREST is not deployed.
Every check that needs search is therefore BLOCKED, never PASS.
`search.mode: "rpc-sql"` (psql `SET ROLE` with claims set by the harness)
remains as a diagnostic. Any check that used it is downgraded to BLOCKED,
prefixed "diagnostic only".

### 5. v1-search-scope (metadata gate)

This check labels V1, honestly. Full-transcript search is UNSUPPORTED on V1
(HoopDb Postgres). The report states this in `scope` and in this check's
`unsupported` status. The probe behind the label searches each tenant's
transcript-only canaries through V1 and must get zero rows. Any hit is a
failure, because it means transcript text reached `search_text`.

### 6. transcript-search (transcript gate, DEFERRED)

Khaliq deferred this gate for this AWS proof on 2026-10-31. With no backend
configured, the check reports `deferred` with the reason "deferred by owner
2026-10-31". A deferred gate does not block the overall verdict and is never
reported as PASS. The code below stays in place. Only the `pipeline` feed
(product ingest → index → transcript search) can pass. A `harness-synthetic`
direct insert is a diagnostic only and always yields BLOCKED. The V1
metadata/short_summary search gate remains mandatory.

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

Writes are compared by identity: the planted trace id plus its server
timestamp, kept through the restore query. The source and restore samples are
compared as multisets, so two sessions that land in the same millisecond are
two writes, and any missing or unmatched row fails.

The check fails, and never reports an RPO of 0, in any of these cases:

- no planted write exists in the source before the backup stopped;
- the restored cluster cannot be queried, or holds none of the planted
  sessions;
- either cleanup step fails.

| | Command | Touches |
| --- | --- | --- |
| R | `aws s3api get-bucket-versioning`, `kubectl … get cluster hoopdb -o jsonpath={.spec.backup.barmanObjectStore.destinationPath}` | config (read) |
| M* | `kubectl <ctx> -n 96bulls apply -f -` Backup (stdin) + `wait` | backup bucket (base backup + WAL) |
| M* | `kubectl <ctx> -n 96bulls apply -f -` Cluster `hoopdb-restore-proof` (recovery) + `wait` | new 10Gi PVC |
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
   this harness never runs; on the Pippen sandbox target,
   `pippen sandbox down aws-proof`), verify absence with the post-destroy path:
   `bun run proof:aws:teardown --post-destroy --confirm-live --prior-report .96bulls/aws-proof/<run id>/report.json`.
   - **No live cluster needed.** There is no describe preflight, no kube
     context and no kubectl or helm. Identity is the STS account, the region is
     pinned on every call, and the target comes from the admitted prior report.
   - **What it checks.** `aws eks describe-cluster` must answer
     `ResourceNotFoundException`, and `list-clusters` must not name the cluster.
     No resource tagged `96bulls:stack=aws-proof` may remain; a KMS key in
     `PendingDeletion` is the expected end state. Both proof buckets must
     answer `head-bucket` 404; on the sandbox target so must the stage's own
     Alchemy state bucket `pippen-sbx-alchemy-state-361835732085-us-west-2-an`
     (docs/aws-stage.md, deleted by `pippen sandbox down` with every version).
     On the sandbox target the stage tag is
     `96bulls:stack=aws-proof-sandbox`, and nothing tagged
     `alchemy::stage=aws-proof-sandbox` or `pippen:sandbox=true` may remain
     either, except the exact ARN of the agent-session role that the fresh
     `pippen sandbox up` created (see [Pippen sandbox target](#pippen-sandbox-target)).
   - **What counts as gone.** Only the matching error class counts. The
     harness reads stderr only for that match and never keeps it. AccessDenied,
     a 403 or a network error is "absence unproven", never gone. The check passes only if `aws eks list-clusters` no longer
   names the cluster and no resource tagged `96bulls:stack=aws-proof` remains.
   A KMS key in `PendingDeletion` (the 30-day window) is the expected end state.

A failed cluster listing never counts as "gone".

**One report for the whole proof.** `--prior-report` merges an earlier report
of the same proof. The prior is admitted completely before any AWS call or
step of the current run, so a prior that is invalid or aimed at the wrong
target is refused (exit 2) before teardown can start. Nothing in the prior is
trusted:

- The prior is read as exact bytes. Its SHA-256 is computed over them, and the
  bytes must be the canonical serialization the CLI writes.
- It must validate. Timestamps must be strict, calendar-valid RFC 3339
  (`YYYY-MM-DDTHH:MM:SS.sssZ`). Every check must start and finish before the
  report's `generatedAt`, and the report may not be dated in the future.
- It must be a single unmerged live phase whose run id, timestamp, mode and
  check list match the report.
- Its target must be exactly the immutable proof target: stage `aws-proof`,
  cluster `96bulls-aws-proof`, the proof context, account `500268966557` and
  region `us-west-2`. Matching the current run is not enough.
- A carried `pass` or `unsupported` result must have executed: at least one
  step, and every step `ran`. A planned-only result never counts toward a PASS,
  in a merge or in any report.

At merge time only the relations remain to check: the current run is a single
live phase with the same target and a different run id, and every current
check started after the prior was generated.

Checks merge in a fixed order, with the current run's result winning. The
verdict is recomputed, never carried, and the prior's digest is recorded in
`phases`.

So the full live run (with teardown `blocked`), merged with the phase-2
teardown run, produces one report whose verdict covers both.

| | Command | Touches |
| --- | --- | --- |
| R | `aws eks list-clusters` | EKS (list) |
| M* | `kubectl <ctx> -n {96bulls,relaycast} delete backups…,clusters…,pods -l 96bulls.proof=true` (one per namespace, never `-A`) | proof objects |
| M* | `helm --kube-context <ctx> -n 96bulls uninstall 96bulls --wait` | release |
| M* | `kubectl <ctx> delete namespace 96bulls --wait=true` | namespace and PVCs |
| R | `kubectl … -n {96bulls,relaycast} get all,pvc,backups…,clusters… -l 96bulls.proof=true`, `get namespace 96bulls` | verify |
| R | `aws resourcegroupstaggingapi get-resources --tag-filters Key=96bulls:stack,Values=aws-proof`, `aws kms describe-key` | verify |
| Khaliq | `bin/96bulls down aws-proof` (sandbox: `pippen sandbox down aws-proof`) | the whole stage; printed, never run |

## Steps that need Khaliq's direct OK

- Creating the proof stage: `aws-inputs.json` clearance and `bin/96bulls up aws-proof`.
- Every `--confirm-live` run, read-only runs included, until the stage exists
  and he has cleared it.
- Each `M*` step above, in particular:
  - minting the machine-bound tenant tokens;
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
- For the Pippen sandbox target: `pippen sandbox up aws-proof` (both times),
  `bin/96bulls up aws-proof-sandbox`, and both `pippen sandbox down aws-proof`.
  The ECR pull grant for `361835732085` was approved and applied on
  2026-11-01. It is kept by `ecr.pullAccounts` in the aws-sessions-dev inputs
  (docs/aws-stage.md).

## Report

Every report states the phase-1 limitation: **NetworkPolicy is not enforced in
phase 1**. The stage declares NetworkPolicies, but this proof does not show
that the cluster's network plugin enforces them, so isolation between pods
rests on authentication.


**Provenance.** Every report records:
- the harness commit and dirty bit;
- the SHA-256 of `inputs.json` (or of the defaults when there is none) and of
  the effective config;
- the bound stage plan: its provenance digest, plan digest, `generatedAt` and
  commit;
- the chart package digest for each deployed chart version, from the chart
  repository's `index.yaml`.

A run whose checks all pass is still INCOMPLETE until provenance is complete:
a clean tree, a known commit, a bound stage plan, and every chart digest. Any
gap is listed in `verdictGaps`. A prior report merges only if it has the same
harness commit and the same stage plan as the current run.


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
- **Tenants.** The tenants are `org_aws_proof_1` and `org_aws_proof_2` (#75
  `eks.tenantOrgIds`), with objects under `96bulls/<org>/v1/...`. planted-trace
  also mints a token for `org_aws_proof_unlisted` and requires 403
  `org_not_allowed` from the ingest. The probe sends an empty chunk, so it
  cannot store a byte.
- **A finding on this branch's `main`.** `chunksAreServerSealed`
  (`src/alleyoop/manifests.ts`) appears to rebuild chunk keys without the key
  prefix. With `PIPELINE_ORG_FROM_PREFIX`, a prefixed manifest then fails with
  409 `chunks_unverified`. #75 needs to cover this before planting can pass.
- **Pippen sandbox sweep coverage.** The post-destroy check sees only what the
  Resource Groups Tagging API lists. The stage's AWS Budget is declared
  without tags, so it is outside the tag check (and outside the pippen tag
  sweep). IAM is covered by `pippen sandbox down`'s own check: zero
  non-baseline roles, where the baseline is `pippen-sweeper` (path `/pippen/`),
  `/aws-service-role/*` and `stacksets-exec-*`. The harness does not repeat
  that check. `proof-sandbox-sessions.agentrelay.com` and its ACM
  certificate must exist before the ingress probes can run there.
- **The `eks.harness` output shape** is the harness's expectation of #75. Any
  difference refuses the run.
