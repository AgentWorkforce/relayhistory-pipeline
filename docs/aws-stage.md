# AWS stage (not cleared)

Stages named `aws-*` are declared in `alchemy.run.ts` and refuse to run. Nothing
in this repository creates a cloud resource, credential or IAM entity until
Khaliq clears the AWS account work in his own words.

The published portable Relaycast chart has a completed local kind proof; see
[Relaycast self-hosted conformance evidence](relaycast-self-hosted-conformance.md).
That proof is local-only and does not clear this AWS stage or change the
production Supabase and AWS S3 boundary.

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
  "signing-key.pem[password]=$k"; unset k
```

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
  -d '{"orgId":"org_sessions_dev","workspaceId":"default","sponsorId":"<user id>","scopes":["rth:sync","rth:read"],"expiresIn":3600,"historyOrigin":"https://sessions-dev.agentrelay.com"}'
# -> 201 {"accessToken": ...}; send it to AlleyOop as Authorization: Bearer <token>
```

The token's `aud` is exactly `historyOrigin`, which must equal the stage's
`relayhistoryOrigin` (AlleyOop refuses anything else).

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
and `96bulls` to Relaycast. The stage also writes `kube-system/amazon-vpc-cni`
`enable-network-policy-controller: "true"`; without it Auto Mode accepts the
policies and enforces nothing.

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
(`aws:SecureTransport`). The Locker expires noncurrent versions after 90 days
(superseded manifests; nothing reads old versions) and incomplete multipart
uploads after 7.

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

### aws-inputs.json additions (proof stage)

```json
"eks": {
  "imageDigest": "sha256:<digest publish-image-ecr pushed to this account>",
  "imageSourceRevision": "<40-hex commit it was built from>",
  "apiAccess": { "publicAccessCidrs": ["<operator IP>/32"] },
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
