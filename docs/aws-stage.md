# AWS stage (not cleared)

Stages named `aws-*` are declared in `alchemy.run.ts` and refuse to run. Nothing
in this repository creates a cloud resource, credential or IAM entity until
Khaliq clears the AWS account work in his own words.

The published portable Relaycast chart has a completed local kind proof; see
[Relaycast self-hosted conformance evidence](relaycast-self-hosted-conformance.md).
That proof is local-only and does not clear this AWS stage or change the
production Supabase and AWS S3 boundary.

The proof harness for an isolated self-host E2E on this account
(`aws-proof`, account 500268966557) is in [AWS self-host E2E proof](aws-e2e-proof.md).
It defaults to a dry run and has not been run against AWS.

What the stage will need (each item needs an explicit OK): a target account
(per-agent sandbox under the Sandbox OU via Pippin Tools, or a named dev
account); which credential to use (SSO profile or a `96bulls-deployer` role
with GitHub OIDC trust scoped to this repo); an Alchemy state bucket; HoopDb
(Supabase for hosted production; in-cluster CloudNativePG for the isolated
proof stage); RelayAuth JWKS + issuer; image push
rights (GHCR/ECR, by digest); compute (ECS Fargate first, EKS via the
`96bulls-worker` chart later); secrets names in 1Password; a budget cap.

Sandbox rules (settled Oct 28, 2026): accounts are per agent; a sweeper on a
schedule and on session close wipes everything sandbox-tagged and quarantines
any account it cannot verify empty (zero sandbox-tagged resources, zero
non-baseline IAM entities, empty workspace bucket prefix); quiet timeout 3h,
extensions in 3h blocks with no cap, any activity or deployment resets the
clock. `96bulls sandbox up|down|extend <agent>` is the planned interface; the
provisioning API lives in Pippin Tools.

## Decisions (Khaliq, 2026-10-28)

Hosted production is Supabase plus S3 only. Everything on this AWS stage
beyond S3 exists to prove the self-hosted deployment
(`docs/decisions/0001-production-data-plane.md`, 2026-10-30).

- **Production HoopDb:** Supabase, with AWS S3 as the only AWS service in the
  hosted production data plane (Khaliq, 2026-10-30;
  `docs/decisions/0001-production-data-plane.md`). Production is not Aurora,
  RDS or an in-cluster database; the stack refuses `eks.hoopdb.backend: "cnpg"`
  on any stage whose name contains `prod`.
- **Proof-stage HoopDb:** in-cluster CloudNativePG (Postgres 17) on the
  encrypted gp3 class, for the isolated AWS proof stage only
  (`eks.hoopdb.backend: "cnpg"`; see "Proof-stage hardening" below). It is
  disposable proof data and never holds production data.
- **Compute:** EKS (Auto Mode) running the `96bulls` umbrella chart from
  AgentWorkforce/helm-charts, pinned to the published services image digest
  (`eks.imageDigest` + `eks.imageSourceRevision`, `stacks/aws/eks.ts`).
- **Dev HoopDb:** a Supabase project in the org the 96Bulls team owns, declared
  with the `Supabase.Project` resource; its password comes from an `op://`
  reference at create time only.
- **Sandbox HoopDb:** Postgres (pgvector) as a container inside the sandbox's own
  compute; RDS only when a test needs managed Postgres.
- **Services image:** each account's ECR digest (`eks.imageDigest`) with the
  full commit it was built from (`eks.imageSourceRevision`). The stage refuses
  tag-only references and an image without a source commit.

Still gated: nothing here runs until `.96bulls/<stage>/aws-inputs.json` carries
Khaliq's clearance and the account id is confirmed on the command line.

## Image registry (Khaliq, 2026-10-28)

The services image is the hosted product and stays private. The AWS stage
creates a private, tag-immutable ECR repository (`96bulls-services`, scan on
push, last 50 images kept); EKS nodes pull from it with their node role. The
`publish-image-ecr` workflow pushes to it via GitHub OIDC (repo variables
`AWS_DEPLOYER_ROLE_ARN`, `AWS_REGION`), manual and Khaliq-only. The EKS stage refuses
to declare anything until `aws-inputs.json` carries `eks.imageDigest`, the digest
published to that account's repository (each account builds its own). Only
jobs in the `image-publish` GitHub environment can assume the deployer role,
and it can only push to that ECR repository (no Locker or KMS access).
Sandbox stages create neither the OIDC provider nor the role. The GHCR
copy stays private for kind and self-host tests.

Order for the first environment: apply the baseline (creates ECR and the
deployer role) -> set the two repo variables -> run publish-image-ecr -> put the
digest in `aws-inputs.json` -> apply again with `eks` inputs.

## Self-hosted RelayAuth (Khaliq, 2026-10-29: "the entire stack self hostable")

Each environment can run its own RelayAuth (helm-charts `relayauth`, one
replica on an encrypted gp3 volume) instead of trusting hosted relayauth.dev.
Turn it on with `eks.relayauth` in `aws-inputs.json`:

```json
"relayauth": { "chartVersion": "0.1.0", "secretName": "relayauth" }
```

and remove `eks.relayauthIssuer` (the stack refuses both). AlleyOop's issuer and
JWKS URL then become the in-cluster Service, `http://relayauth.relayauth.svc:8787`,
whatever `relayauthJwksUrl` says. Neither the token API nor JWKS is public:
on plain HTTP only AlleyOop's pods and the TLS proxy may call it, and secret-
bearing calls use the TLS endpoint (see "Transport" below; NetworkPolicy is
enforced, see "NetworkPolicies").

**Keys (once per environment, before the deploy).** They are generated on the
Mac straight into 1Password and reach the cluster through a pipe, never a file
on an agent machine. The 1Password item is `<stage> relayauth` in the
environment's vault (`96Bulls-dev` for sessions-dev), with fields
`internal-secret`, `signing-key.pem` (unencrypted PKCS#8 RSA 3072,
`BEGIN PRIVATE KEY`) and `bootstrap-api-key` (`rak_` + 64 hex):

```sh
# on the Mac (Khaliq approves each op call)
k=$(openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:3072 2>/dev/null)
op item create --vault 96Bulls-dev --category "API Credential" --title "sessions-dev relayauth" \
  "internal-secret[password]=$(openssl rand -hex 32)" \
  "bootstrap-api-key[password]=rak_$(openssl rand -hex 32)" \
  "signing-key\.pem[password]=$k"; unset k
```

`op` assignment syntax reads an unescaped `.` as `section.field`, so the field
name is written `signing-key\.pem` (double quotes keep `$k` expanding and the
backslash literal). Without it, `op` creates section `signing-key` with field
`pem`, and the extraction below finds no `signing-key.pem`.

```sh
# from the operator shell, cluster credentials in env vars only
kubectl create namespace relayauth --dry-run=client -o yaml | kubectl apply -f -
ssh kjg-lap "zsh -lc 'op item get \"sessions-dev relayauth\" --vault 96Bulls-dev --reveal --format json'" \
  | jq '{apiVersion:"v1",kind:"Secret",type:"Opaque",metadata:{name:"relayauth",namespace:"relayauth"},
         stringData:(.fields|map(select(.label|IN("internal-secret","signing-key.pem","bootstrap-api-key")))|map({(.label):.value})|add)}' \
  | kubectl apply -f -
```

RelayAuth reads the keys at boot: after changing the Secret, run
`kubectl -n relayauth rollout restart deploy/relayauth`. Rotation:
`signingKeyNext` / `signingKeyPrevious` in the chart (helm-charts README).

**Minting a RelayHistory assertion** (the bootstrap key has exactly the scope
`relayauth:assertion:create:relayhistory`):

```sh
kubectl -n relayauth port-forward svc/relayauth 18787:8787 &
curl -s http://127.0.0.1:18787/v1/tokens/relayhistory-assertion \
  -H "x-api-key: $BOOTSTRAP_KEY" -H 'content-type: application/json' \
  -d '{"orgId":"org_sessions_dev","workspaceId":"default","machineId":"<exact courier MACHINE_ID>","sponsorId":"<user id>","scopes":["rth:sync","rth:read"],"expiresIn":60,"historyOrigin":"https://sessions-dev.agentrelay.com"}'
# -> 201 {"accessToken": ...}; send it to AlleyOop as Authorization: Bearer <token>
```

The token's `aud` is exactly `historyOrigin`, which must equal the stage's
`relayhistoryOrigin` (AlleyOop refuses anything else). RelayAuth also signs the
canonical request `machineId` as `meta.machineId`; it must equal the courier's
persisted `MACHINE_ID`. Never reuse the assertion for another machine.

**Transport.** The chart's Service is plain HTTP and now admits only
AlleyOop (which fetches the public JWKS) and the TLS proxy. Anything that
carries a key or a user token, such as the relayauth #116 tenant search-token
exchange (API key plus user token) or an in-cluster minting client, must call
`https://relayauth-tls.relayauth.svc:8443` instead:

- **TLS 1.3 only**, terminated by a digest-pinned `nginx-unprivileged` proxy
  that is required to run on RelayAuth's node, so the plaintext hop never
  crosses the VPC (`stacks/aws/relayauth-tls.ts`). Chart 0.1.0 cannot serve TLS
  or take a sidecar; the proxy goes once a chart can.
- **Private CA** from cert-manager v1.21.3 (images by digest, CRDs kept,
  namespace-scoped `Issuer`s only, no ACME, no `ClusterIssuer`). The Service
  certificate is ECDSA P-256 for `relayauth-tls.relayauth.svc`, 90 days,
  renewed 30 days early; the proxy loads it per handshake, so rotation needs no
  restart. The CA key stays in the `relayauth-ca` Secret; the proxy mounts only
  the leaf key and chain.
- **Who may connect:** pods labelled `relayauth.agentrelay.com/client: tls` in
  the `96bulls` or `relayhistory` namespaces (NetworkPolicy both ways).
- **Trusting the CA.** A caller trusts the public CA certificate only, never the
  `relayauth-ca` Secret. Publish it to the caller's namespace as a ConfigMap
  and point the runtime at it (`NODE_EXTRA_CA_CERTS`, or `SSL_CERT_FILE`):

  ```sh
  kubectl -n relayauth get secret relayauth-tls -o jsonpath='{.data.ca\.crt}' | base64 -d \
    | kubectl -n 96bulls create configmap relayauth-ca --from-file=ca.crt=/dev/stdin --dry-run=client -o yaml | kubectl apply -f -
  ```

  Re-run it when the CA renews (yearly). Automating this (trust-manager) is a
  follow-up.

Minting through `kubectl port-forward` (above) stays as it is: it tunnels inside
the API server's TLS.

## Summary encryption key (Bob Costas)

Summaries are stored only as encrypted objects next to the session's raw,
`{session prefix}/summary/{short,long}.json.enc`, in the upstream envelope
(relayhistory-cloud `docs/pipeline-summary-short.md`: AES-256-GCM, AAD bound
to key, kind and revision). `sessions.session_summaries` holds the pointer
only: object key, ciphertext SHA-256, key id, status and revision. It never
holds the text or the model name.

Each workspace's key is derived with HKDF-SHA256 from a deployment master key
(`src/shared/keyring.ts`), so no cloud KMS is needed and the stack stays
self-hostable. Bob Costas refuses to start without it. The master key is the
`SUMMARY_MASTER_KEYS` key of the runtime Secret (`96bulls-runtime`):
`<id>:<64 hex>`, with the newest first. Keep older entries after it until
everything has been rewritten; removing an entry crypto-shreds what it wrote.

Before deploying a version with migration 0004, add the key from 1Password
through a pipe, never a file:

```sh
# on the Mac: create it once
op item edit "sessions-dev hoopdb" --vault 96Bulls-dev "summary-master-keys[password]=sd1:$(openssl rand -hex 32)"
# operator shell: merge it into the runtime Secret
v=$(ssh kjg-lap "zsh -lc 'op read \"op://96Bulls-dev/sessions-dev hoopdb/summary-master-keys\"'")
kubectl -n 96bulls patch secret 96bulls-runtime --type merge -p "$(jq -nc --arg v "$(printf %s "$v" | base64 -w0)" '{data:{SUMMARY_MASTER_KEYS:$v}}')"; unset v
```

The umbrella chart must be **0.3.0 or later** (worker 0.4.0): its
`worker.summaryEncryption` projects that Secret key as a read-only file into
Bob Costas only, and the stack refuses older chart versions.

**Rollout order for 0004.** The migrate Job runs before the new pods, so an old
Bob Costas briefly writes the removed `summary` column and fails until it is
replaced. In local kind, the queue recovered on attempt 3. To avoid burning
retries on a busy environment, scale it to zero first:
`kubectl -n 96bulls scale deploy/96bulls-worker-bob-costas --replicas=0`.
**Then, after the deploy, always scale it back and check.** Do not rely on the
apply to restore it: the stack applies rendered manifests (helm template plus
server-side apply), and a manual scale may be preserved.

```sh
kubectl -n 96bulls scale deploy/96bulls-worker-bob-costas --replicas=1
kubectl -n 96bulls rollout status deploy/96bulls-worker-bob-costas --timeout=300s
kubectl -n 96bulls get deploy 96bulls-worker-bob-costas -o jsonpath='{.status.readyReplicas}'   # 1
```

Migration 0004 deletes existing `session_summaries` rows, because they hold
plaintext. They are derived: Bob Costas's summary sweep (at startup, then every
`SUMMARY_SWEEP_SECONDS`, default 300) re-queues every indexed session without a
summary and rebuilds each as an encrypted object.

## Production transcript store (`aws-sessions-prod`)

Production AWS is S3 only (decision 0001). The `aws-sessions-prod` stage in
`sessions-prod` (`941433935742`, `us-east-1`) sets `transcriptStore` in its
inputs and declares only the transcript bucket, its CMK, access logs,
CloudTrail with S3 data events, a budget and four IAM users (writer, reader,
eraser, srv-writer for the relayhistory-api live writer), none of the baseline above (`stacks/aws/transcript-store.ts`). Design,
apply, credentials, Cloudflare wiring, rotation, teardown and the later export:
`runbooks/sessions-prod-transcript-store.md`.

## AWS observability

The EKS stage enables the umbrella chart's pinned OpenTelemetry contrib
collector and points every 96Bulls service at its in-cluster OTLP/HTTP Service.
The collector stores gzip-compressed, lossless OTLP/JSON trace batches under the
Locker bucket's encrypted `telemetry/traces/` prefix. It has its own EKS Pod
Identity role with only `s3:PutObject`, multipart-abort and KMS encrypt/data-key
permissions (including the bucket-key-required KMS decrypt); it has no
`s3:GetObject` or delete permission and cannot fetch telemetry or session objects. The
KMS grant is conditioned on calls through S3 in the environment's region, so
collector credentials cannot call KMS decrypt directly.
collector repeats the service allowlist safeguards: it drops content-bearing
attributes and masks secret-shaped values before persistence.

The S3 archive deliberately preserves the canonical UUIDv7 W3C trace id. Do not
substitute the X-Ray exporter without a separate id mapping: X-Ray interprets a
trace id's first 32 bits as epoch seconds, which is incompatible with UUIDv7's
48-bit millisecond prefix.

The AWS proof must retrieve the exact planted UUIDv7 trace from that archive as
well as the durable `pipeline_events` rows in HoopDb. It must also scan the
retrieved trace data and service logs for the planted secrets, content marker
and prompt text, matching the local smoke test. Metrics/SLO alarms and a durable
hosted stdout log backend remain separate follow-up work; do not claim those
are present.

## Proof-stage hardening (2026-10-30)

Pure contracts: `stacks/aws/hardening.ts` and `stacks/aws/proof-addons.ts`,
tested in `stacks/aws/__tests__/`. Every check below runs before the stage
declares a resource.

### EKS API access

The private endpoint is always on. The public endpoint is **off** unless
`eks.apiAccess.publicAccessCidrs` lists operator addresses. There is no
`0.0.0.0/0` default; it, anything broader than `/16`, and malformed entries are
refused. Alchemy applies the charts and manifests through the API server, so
the apply itself must run from an address that can reach it:

- **Allowlist the operator's IP** (simplest): add `"<your public IP>/32"`
  (`curl -s https://checkip.amazonaws.com`) to `publicAccessCidrs`, apply, then
  `aws eks update-kubeconfig --name 96bulls-<stage> --region <region>`. Remove
  the entry when the window closes and apply again.
- **Private only (no public endpoint):** run kubectl and the apply from inside
  the VPC, for example an SSM-managed instance in a private subnet with no
  inbound rules (`aws ssm start-session --target <instance id>`). Its security
  group must be allowed to reach the cluster security group on 443. This stage
  does not create that instance; it is a separate, reviewed addition.

Because the default is private only, a stage whose live cluster already
allows an operator CIDR must list it in `publicAccessCidrs`; otherwise the next
apply turns the public endpoint off and the apply loses its own route to the
API server.

Endpoint changes are `UpdateClusterConfig` calls, applied in place (no cluster
replacement, no workload restart); the control plane stays available while
EKS updates.

### Control-plane logs

All five types (`api`, `audit`, `authenticator`, `controllerManager`,
`scheduler`) go to `/aws/eks/96bulls-<stage>/cluster`. The stage declares that
log group first with 90-day retention, so EKS never creates it with no expiry.
It uses CloudWatch's default encryption; a customer-managed key would need a
key-policy grant for the Logs service, which is a separate decision.

### Pod roles (EKS Pod Identity), one per service account

Derived from the code that uses the Locker (key layout `v1/ws=…/s=…/`); no
role has `s3:DeleteObject` because the object store has no delete.

| Service account | S3 | KMS (via S3 only) |
| --- | --- | --- |
| `96bulls-worker` (indexer, referee, Bob Costas, migrate) | ListBucket; GetObject `v1/*`; PutObject `v1/*/summary/*` | Decrypt, GenerateDataKey |
| `96bulls-alleyoop` | ListBucket; GetObject `v1/*`; PutObject `v1/*/raw/*`, `v1/*/manifest.json` | Decrypt, GenerateDataKey |
| `96bulls-search-worker` | explicit Deny `s3:*` on the Locker | explicit Deny `kms:*` on its key |
| `otel-collector` | PutObject, AbortMultipartUpload `telemetry/traces/*` only | unchanged |

`s3:ListBucket` is unconditioned on purpose: S3 returns 404 for a missing key
only to callers that hold it (403 otherwise), and both conditional writers HEAD
first. Trust is bound to the account and the cluster ARN. Worker 0.4.0 runs all
worker components under one service account, so that is the finest split Pod
Identity can express until the chart gives each component its own.

### NetworkPolicies

Default deny (ingress and egress) in `96bulls`, `relayauth` (when self-hosted)
and `relaycast` (when enabled),
with explicit allows: DNS by port (Auto Mode serves DNS from the node), the Pod
Identity agent, HTTPS egress (S3 through the gateway endpoint, the EKS API,
JWKS) to anywhere except instance metadata, OTLP inside the namespace, the ALB
to AlleyOop's port and health-only ports from the VPC CIDR (pod IPs are VPC
addresses, so no data port is opened by CIDR; kubelet probes from a pod's own
node are admitted by the VPC CNI agent), AlleyOop to RelayAuth (JWKS), labelled
callers to the RelayAuth TLS endpoint, apps to HoopDb
(CNPG: in-namespace only, plus the operator; external: Postgres/pooler ports),
and `96bulls` to Relaycast. AlleyOop's port 8080 admits only the ALB's
**public** subnets (`10.42.0.0/24`, `10.42.1.0/24`), where no pod runs, so
in-cluster traffic cannot reach the ingest around the WAF. (An `internal` ALB
shares the private subnets with pods; that scheme cannot make this
distinction.)

Enforcement is a **separate, later apply**, and the stack enforces the order.
Step 1 applies every policy while Auto Mode still ignores them
(`eks.networkPolicyEnforcement` absent). Check them (`kubectl get networkpolicy
-A`, the workloads still healthy), then step 2 sets a verification record:

```json
"networkPolicyEnforcement": { "policySetSha256": "<hash>", "verifiedBy": "<who checked>", "verifiedOn": "YYYY-MM-DD" }
```

The stack refuses step 2 (stacks/aws/preflight.ts) unless the hash names the
exact current policy set (a wrong hash is refused with the right one in the
message) **and** Alchemy state for the stage already holds every one of those
policies as applied, with the same spec. A fresh stage, or a policy changed
since step 1, cannot enable enforcement. Step 2 writes
`kube-system/amazon-vpc-cni` `enable-network-policy-controller: "true"`; undo it
by removing the record and applying (or deleting that ConfigMap).

### Ingest exposure (public contract)

Customers and their bots reach the ingest from the internet, so the ALB is
**internet-facing** by default (`eks.ingress.scheme`, `"internal"` keeps it in
the VPC). The public contract, enforced by the stack and by tests:

- **TLS only.** One HTTPS :443 listener, `ELBSecurityPolicy-TLS13-1-2-2021-06`,
  ACM certificate. No :80 listener and no redirect.
- **Authentication on every route except health.** Only `/health`, `/healthz`,
  `/readyz` and the public `/.well-known/jwks.json` answer without credentials
  (`src/alleyoop/routes.ts` `PUBLIC_INGEST_PATHS`). Every `/v1` route requires a
  RelayAuth JWT; the operator routes under `/v1/internal/pipeline/*` require
  `PIPELINE_OPS_SECRET` (503 where it is unset).
  `src/alleyoop/__tests__/routes-auth.test.ts` enumerates every mounted route
  and requires 401 without a token and with a non-JWT bearer.
- **WAFv2 web ACL** on the ALB (`alb.ingress.kubernetes.io/wafv2-acl-arn`):
  `AWSManagedRulesAmazonIpReputationList`, `AWSManagedRulesKnownBadInputsRuleSet`,
  `AWSManagedRulesCommonRuleSet`, a per-IP rate limit (default 2000 requests per
  5 minutes, `eks.ingress.rateLimitPer5Min`), and a block on `/v1/internal/` so
  operator routes are reachable only through `kubectl port-forward`. Managed
  rules that inspect request **bodies** count instead of block: bodies are
  authenticated session transcripts and chunks, which legitimately contain code,
  paths, URLs and more than 8 KB. URI, query, header and reputation rules block.
  Watch the `Count` metrics after the first apply before tightening further.

The ALB also writes access logs to its own bucket
(`96bulls-<stage>-alb-logs-<account>`, SSE-S3 because ALB log delivery does not
support KMS keys, TLS only, 90 days) and drops header fields that are not valid
HTTP names (`routing.http.drop_invalid_header_fields.enabled`).

The internet-facing class keeps its name `alb-public`, so applying this to a
live stage changes the Ingress annotations (WAF association) and does not
replace the ALB.

### Proof HoopDb (CloudNativePG)

With `eks.hoopdb.backend: "cnpg"` the stage installs the CloudNativePG operator
(chart 0.29.1, operator 1.30.1 by digest) in `cnpg-system`, then enables the
umbrella chart's `hoopdb` Cluster: one Postgres 17.11 instance
(`postgresql:17-standard-trixie` by digest, `vector` + `pg_trgm`), 20 GiB on
`96bulls-gp3`. The apps then read `hoopdb-app/uri`, which CNPG generates.
Backups to S3 are **not** enabled: the pinned `standard` image ships no
barman-cloud, so they need the Barman Cloud plugin and cert-manager. The proof
database is disposable; recreating it re-runs the migrations on an empty
database.

### Relaycast

`eks.relaycast` installs the released `relaycast` chart: one replica,
`ClusterIP`, ingress off, SQLite on a 10 GiB `96bulls-gp3` volume, only the
`96bulls` namespace admitted. `chartVersion` and `imageDigest` are inputs so a
later chart (0.2.0) needs no code change. Chart 0.1.0 (Relaycast 8.0.0)
accepts no secret material; if a later chart does, it must come from an
existing Secret by name.

### CloudTrail and buckets

The trail bucket is versioned and encrypted with the trail's own KMS key
(rotation on). Its key policy is least privilege: the account root may
administer the key (including `PutKeyPolicy`, so there is no lockout) but not
use it; CloudTrail may generate data keys for this trail only; account
principals may decrypt trail log files. The Locker key keeps the default
account policy for now (its users are IAM roles conditioned on S3).

Every bucket (Locker, trail, ALB logs) denies requests without TLS
(`aws:SecureTransport`). The Locker aborts incomplete multipart uploads after
7 days; expiring noncurrent versions (superseded manifests; nothing reads old
versions) is opt-in per stage and irreversible (`lockerNoncurrentVersionDays`).

### GitHub image publisher

The `96bulls-<stage>-deployer` role is ECR push only, trusted for this
repository's jobs in the `image-publish` environment. The workflow's
`github.actor` check is not a security boundary (any branch can edit it), so
the `image-publish` environment must carry a required reviewer and a
deployment branch policy of `main`; that is a GitHub setting, not IaC, and is
currently **not** set.

### Collector pod

The OpenTelemetry collector runs by digest, with no Kubernetes API token
(`automountServiceAccountToken: false`; EKS Pod Identity mounts its own
audience-bound token), non-root, read-only root filesystem, all capabilities
dropped, `RuntimeDefault` seccomp.

### Sandbox permissions boundary

`stacks/aws/policies/sandbox-boundary.draft.json` no longer allows
`iam:UpdateAssumeRolePolicy` or creating identity providers, and denies both
explicitly: a sandbox role's trust is fixed when it is created.

### Tenant orgs

`eks.tenantOrgId` (one org) keeps the single-tenant layout: keys at `v1/...`,
values unchanged. `eks.tenantOrgIds` (several, e.g. `["org_aws_proof_1",
"org_aws_proof_2"]` for the proof's isolation tests) switches both apps to the
pipeline's org-derived prefixes: every object lands under
`96bulls/<org>/v1/...`, the org taken from the caller's RelayAuth token and
applied by the ingest, and the workers read it back from the key
(`PIPELINE_ORG_FROM_PREFIX=true`, `PIPELINE_KEY_PREFIX=96bulls`). AlleyOop
refuses (403 `org_not_allowed`) a valid token for any org not in the list
(`TENANT_ORG_IDS`), and the pod roles' S3 scopes become `96bulls/*/v1/...`
(`stacks/aws/tenants.ts`). Switching an existing stage between the two layouts
changes where objects live, so do it only on a fresh stage.

### In-cluster endpoints for the proof harness

The stack outputs them under `eks.harness`:

- **RelayAuth over TLS:** `https://relayauth-tls.relayauth.svc:8443`
  (Service `relayauth-tls`, namespace `relayauth`, port 8443). Every
  secret-bearing call (the relayauth #116 exchange) goes here, never to the
  plain `relayauth:8787` Service. The caller pod must carry
  `relayauth.agentrelay.com/client: tls` (NetworkPolicy) and trust the CA from
  the `relayauth-ca` ConfigMap (see "Transport").
- **Session directory PostgREST (#73, when wired):** Service
  `session-directory-postgrest`, namespace `96bulls`, port 3000,
  `http://session-directory-postgrest.96bulls.svc:3000`; pods labelled
  `app.kubernetes.io/name: session-directory-postgrest`; callers labelled
  `96bulls.agentrelay.com/search-client: "true"`.

### Session directory search path (slot for #73)

The session-directory PostgREST profile (96BullSessions #73, stacked on #72 and
#48) is not deployed by this stage yet. `eks.sessionDirectory.enabled` applies
its fence ahead of time: search callers (label
`96bulls.agentrelay.com/search-client: "true"`) may reach PostgREST
(`app.kubernetes.io/name: session-directory-postgrest`, port 3000), and
PostgREST may reach the CNPG HoopDb; nothing else. The stage refuses it unless
`eks.hoopdb.backend` is `cnpg` and RelayAuth is self-hosted with in-cluster TLS
(the search-token exchange carries an API key and a user token). Wiring the
PostgREST Deployment is a follow-up once #48, #72 and #73 land.

### Fresh proof stage `aws-proof`

The end-to-end proof runs on its own stage, `aws-proof` (cluster
`96bulls-aws-proof`), in the same account and region as `aws-sessions-dev`,
which keeps running undisturbed and gets the same hardening in place. Start
from `stacks/aws/examples/aws-proof.inputs.example.json` (copy it to
`.96bulls/aws-proof/aws-inputs.json` and fill every `<...>`). It turns on
everything: CNPG HoopDb, self-hosted RelayAuth with in-cluster TLS, Relaycast,
the WAF'd internet-facing ingest on its own hostname
(`proof-sessions.agentrelay.com`; DNS zone, token Secret and an optional
`certValidation` are inputs), private API plus the operator CIDR.

The stage owns its VPC, cluster, KMS keys, Locker, trail, ALB-log bucket and
budget (all named `96bulls-aws-proof-*`). `accountShared.ownerStage:
"aws-sessions-dev"` makes it reuse the account-wide pieces that stage already
owns: the `96bulls-services` ECR repository (so `eks.imageDigest` is a digest
in that repository), the GitHub OIDC provider and the publisher role. Before
the first apply, create its Secrets from 1Password as for sessions-dev
(`96bulls-runtime` without `HOOPDB_URL`, which CNPG supplies; `relayauth`;
`kube-system/cloudflare-dns`).

### Apply, verify and roll back

The stack itself refuses, before declaring anything and whatever started it
(`bin/96bulls` or a direct `alchemy` command; stacks/aws/deploy-guard.ts):

- a placeholder anywhere in the inputs, `$comment` included;
- `NINETY_SIX_BULLS_CONFIRM_ACCOUNT` other than `accountId`; a live STS caller
  or provider region other than `accountId`/`region`;
- for deploy and destroy, a missing or mismatched **approval record**:
  `{ "decision": "approved", "by": "Khaliq Gant", "date": "YYYY-MM-DD",
  "operations": ["deploy"], "stage", "accountId", "region", "quote": "<his
  message verbatim>" }`. `decision` must be exactly `"approved"`; anything
  else refuses. The date must be a real calendar day, not in the future, at
  most 30 days old; `operations`, `stage`, `accountId` and `region` must match
  exactly. `quote` is kept for the audit trail and never interpreted. A
  deploy approval also names the reviewed capture it is for:
  `"capture": { "provenanceSha256", "planSha256", "inputsSha256",
  "sourceCommit" }` (the sha256 of `docs/plans/<stage>.provenance.json` and
  `.plan.txt`, and the provenance's `inputs.sha256` and `source.commit`). A
  recapture, a changed plan or inputs, or another commit refuses, even with
  the same words; `inputs.sha256` excludes the approval record itself. The
  free-text `clearedBy` field is informational only;
- deploy without `NINETY_SIX_BULLS_CONFIRM_STAGE=<stage>`; destroy without
  `NINETY_SIX_BULLS_CONFIRM_DESTROY=<stage>`, and destroy of `aws-sessions-dev`
  or any `*prod*` stage at all. The operation comes from Alchemy's own argv, so
  a direct `alchemy destroy` cannot pass as a deploy;
- (plan and deploy) a services image that is not an approved
  `images.services-aws` entry in versions.lock.json (digest + source commit +
  repository), or whose ECR tags do not include `sha-<imageSourceRevision>`; an
  `accountShared.ownerStage` that does not own the account's ECR repository.

Credentials stay in environment variables, exported from the SSO session:

```sh
export AWS_REGION=us-west-2 NINETY_SIX_BULLS_CONFIRM_ACCOUNT=500268966557
eval "$(aws configure export-credentials --profile sessions-dev --format env)"
bin/96bulls plan aws-proof [--detailed]                       # read only
bin/96bulls plan aws-proof --capture                          # writes docs/plans/aws-proof.*
NINETY_SIX_BULLS_CONFIRM_STAGE=aws-proof bin/96bulls up aws-proof
NINETY_SIX_BULLS_CONFIRM_DESTROY=aws-proof bin/96bulls down aws-proof   # rollback of a proof stage
```

**Captured plans.** `plan --capture` writes the plan, the `--detailed` plan,
sanitized inputs and `<stage>.provenance.json` to `docs/plans/`. The record
holds the commit and a sha256 of the committed stack source (stacks, src,
packages, supabase, package.json, bun.lock, versions.lock.json, tsconfig.json;
capture refuses uncommitted changes there), the inputs file sha256, the STS
caller, account and region, the alchemy, provider, bun, aws CLI and helm
versions, the sha256 of each plan file, and the state evidence below. `up`
re-reads all of it and refuses unless every value still matches: a reviewed
plan is what gets applied, or nothing. Write the approval record into the
inputs before capturing (it is part of the inputs hash).

For `aws-sessions-prod`, the stack additionally parses the reviewed plan. It
accepts only the exact 13 transcript-store resources in the initial create,
known three-resource recovery, one capture-bound post-merge remediation
(`trail-key` and `trail-logs` update with every other resource `noop`), the
srv-writer addition (`transcript-srv-writer` create and `transcripts-key`
update, everything else `noop`), or final all-noop phase. Both compact and detailed plans must list the same actions and
match the exact paths and sha256 values in the approval-bound provenance. The
capture command refuses an invalid shape before replacing the reviewed files,
and deploy checks it again. Any other update count or resource, replacement,
deletion, import or adoption is rejected. A direct AWS repair does not update
Alchemy's persisted `oldProps`; the exact two-update Alchemy reconciliation is
still required, followed by a fresh exact all-noop capture. `NON_DESTROYABLE`
only blocks the explicit destroy command; it does not stop an ordinary deploy
from reconciling a removed declaration, which is why this operation-level
check is required.

**How a capture stays read-only.** `alchemy plan` against `AWS.state` is not
read-only: Alchemy takes a lease (PUT then DELETE of `__lease__.json`) and
reconciles the state bucket's versioning, ownership and encryption if they
drifted. So a capture never plans against it. It GETs the stage's current
state objects, pinned to their version ids, into a local snapshot
(`.96bulls/<stage>/state-snapshot.json`, gitignored, mode 600) and plans
against Alchemy's in-memory state built from it
(`stacks/aws/state-snapshot.ts`; refused for anything but a plan). AWS
providers still read live resources (Describe/List/Get). The state bucket must
be versioned: capture records a digest of every version and delete marker under
the stage's prefix and requires it to be identical before and after the run,
so any write in the window, even one undone (a lease PUT then DELETE leaves a
version and a marker), is detected. `up` requires the same digest, i.e. no
state write by anyone since the capture.

Reversibility:

- **Proof teardown.** `teardown.purgeBucketsOnDestroy` (proof and sandbox
  stages only) sets `forceDestroy` on the Locker, trail and ALB-log buckets, so
  `down` empties every version and delete marker and completes; it also makes
  the StorageClass delete volumes with their claims. It must be applied before
  the `down` (Alchemy reads it from the bucket's state). Without it a non-empty
  bucket stops `down` with BucketNotEmpty, by design. Everything in a proof
  stage is disposable; nothing is recoverable after `down`.
- **IAM split.** With `eks.legacyPodsRole: "keep"` the old `…-pods` role stays,
  unattached and unchanged, while the associations move to the per-SA roles.
  Roll back by pointing the associations back
  (`aws eks update-pod-identity-association --role-arn …-pods`) or reverting.
  Retire it only in a later apply (drop the field) after the pods are verified.
- **NetworkPolicies:** reversible; see the two steps above.
- **Locker noncurrent-version expiry** (`lockerNoncurrentVersionDays`) is
  **irreversible** once applied (expired versions are gone), so it is opt-in:
  the proof stage sets it; sessions-dev does not.
- **sessions-dev never moves to CNPG** (it would start on an empty database):
  the stack allows `eks.hoopdb.backend: "cnpg"` only on `aws-proof*` and
  `aws-sandbox-*` stages. `aws-sessions-dev` is never destroyed: roll it back by
  reverting and running `up`.

CI checks out the literal PR head commit (not the merge ref) and records it.
A mutation check (`bun scripts/aws-hardening-mutations.ts`, also in CI) reverts
each control in turn and requires a named test to fail with an assertion.

### aws-inputs.json additions (proof stage)

```json
"eks": {
  "imageDigest": "sha256:<digest publish-image-ecr pushed to this account>",
  "imageSourceRevision": "<40-hex commit it was built from>",
  "apiAccess": { "publicAccessCidrs": ["<operator IP>/32"] },
  "legacyPodsRole": "keep",
  "hoopdb": { "backend": "cnpg" },
  "sessionDirectory": { "enabled": false },
  "relaycast": {
    "chartVersion": "0.1.0",
    "imageDigest": "sha256:5c86c095a335259060dc34ce7aa2e90809267395af915977a1ae608f93a83b98",
    "baseUrl": "https://relaycast.sessions-dev.agentrelay.com"
  },
  "ingress": { "...": "unchanged", "scheme": "internet-facing", "rateLimitPer5Min": 2000 }
}
```
