# GCP self-hosted reference lane

This lane began as a code-only conformance implementation. On 2026-10-30 an
approved substrate deployment and live in-cluster verification completed on
merge commit `dac89b587debf0f64fc16cbbde23d8d818901daf`; the exact evidence is
recorded below. It is not a product-route or Relaycast availability proof.
The only eligible target is the existing project
`agentrelay-96bulls-soc2-2610`, number `955297012355`, parent
org `929835122282`, region `us-central1`, billing account
`014D50-E96D1A-DE6360`. Do not create or select another project.

The stack entry point is `stacks/gcp/alchemy.gcp.run.ts`. It uses the same
Alchemy + AgentWorkforce Helm boundary as the AWS stack: cloud code declares
provider resources and passes non-secret endpoints/names into charts; runtime
implementations own provider adapters.

## Declared substrate

- regional standard GKE, not Autopilot. The installed provider omits node
  pools from an Autopilot create, so a node service account would not be sent
  and `disallowDefaultComputeServiceAccount` would deny the cluster. Private
  nodes, a private control-plane endpoint, operator CIDRs only, Workload
  Identity Federation, Dataplane V2, Shielded Nodes, VPC-native ranges, one
  reserved NAT address with translation logs, and VPC flow logs. The default
  pool is explicitly limited to one `e2-medium` node in `us-central1-a`; it is
  not one node per zone. The node identity is a dedicated service account with
  no key and only `roles/container.defaultNodeServiceAccount`. The deploying
  principal must separately have `roles/iam.serviceAccountUser` on that node
  identity so GKE may attach it. Private Service Access
  stays project-local peering, not Shared VPC;
- versioned GCS Locker bucket with uniform bucket-level access;
- private Artifact Registry repository with immutable tags;
- a regional Secret Manager secret container (metadata only; no payload enters
  inputs or Alchemy state);
- a least-privilege runtime Google service account and Workload Identity
  bindings for the 96Bulls worker and Alleyoop service accounts. The Cloud SQL
  proxy binding is declared only with the proxy manifests;
- optionally, a private-only Cloud SQL PostgreSQL 17 apply-time poster,
  Memorystore Redis 7.2 with AUTH off and TLS server authentication,
  Pub/Sub with retry/dead-letter handling, and a single-replica Cloud SQL Auth
  Proxy manifests whose deployment and IAM edge depend on the poster output;
- optionally, the Relaycast core chart and RelayAuth, Relayfile, or Relayflows
  charts. Relayflows runs in standalone mode and is not in Relaycast's
  availability path.

Cloud NAT currently permits general workload egress and logs every
translation. Those logs are audit evidence, not an egress allowlist. This lane
does not claim workload egress restriction or SOC 2 network conformance; a
separately proven egress proxy/firewall policy is still required.

Cloud SQL is now present and `RUNNABLE` in this lane. The successful
reconciliation adopted it rather than inserting another instance. The stack
prepares a SQL Admin `instances.insert`
for public IPv4 off, the Private Service Access network and range, and
`--private-ip` proxy routing. `ensurePrivateCloudSql` gets that instance or
inserts it only when it is absent, then polls the operation through an
injected transport. It refuses a public instance, a different existing
instance, a failed operation, and a timeout. The bearer credential stays in
the caller and the transport, not in Action inputs/state, the request object,
or error text. An apply-time Alchemy Action calls that poster only after the
Private Service Access connection resolves; its output gates the proxy IAM,
deployment, and Helm wiring. `GCP.SQL.Instance` is not called, because that
resource drops `privateNetwork` and can create a public instance.

The Private Service Access allocation is explicitly `10.80.0.0/20`. A
RUNNABLE Cloud SQL instance is accepted only when every returned address is a
syntactically valid RFC1918 `PRIVATE` IPv4 inside that exact allocation. A
public, mislabeled, whitespace-bearing, malformed, or other-private-range
address is refused.

## Truthful feature gates

The released Relaycast self-host runtime is SQLite plus local file storage. Its
chart is forced to one replica, `Recreate`, `ReadWriteOnce`, and
`runtime.backend=sqlite`. Cloud SQL, Redis, Pub/Sub, and GCS are deliberately
unattached until the Relaycast Postgres/Redis/queue/object-store adapter passes
the provider-neutral conformance suite.

Provisioning those managed services requires both:

```json
"managedDataPlane": {
  "provision": true,
  "acknowledgement": "adapter-proof-only"
}
```

This acknowledgement means "create infrastructure for adapter testing". It
does not mean Relaycast is HA.

The 96Bulls runtime selects its Locker in `src/shared/locker-selection.ts`.
Unset `LOCKER_PROVIDER` stays S3, which is local MinIO, production AWS S3, and
the EKS proof. `LOCKER_PROVIDER=gcs` builds the GCS adapter and takes a token
from the GKE metadata server. HMAC keys on GCS are refused, and an S3 client
pointed at `storage.googleapis.com` is refused. `rawStoreFromEnv` follows that
choice: unset and `s3` stay S3, `r2` stays R2, and `PIPELINE_RAW_PROVIDER=gcs`
builds the GCS adapter when the bucket is set and HMAC keys are absent. Any
other name stays unconfigured.

`bulls96.enabled` requires the acknowledged managed data plane, an external
runtime Secret, and a dockerconfigjson image-pull Secret. After Memorystore is
`READY`, an apply-time Action reads its public service-CA bundle from the exact
Redis Admin resource and passes that bundle to the chart. `/readyz` and
`/datapathz` perform a CA-validated, passwordless Redis `PING`; static
`SERVER_AUTHENTICATION` environment strings are not accepted as proof. The
verified peer chain must terminate at one of the exact per-instance CAs in the
Admin API rotation bundle; a different valid CA is refused. Redis
is a conformance/readiness dependency here, not the pipeline queue or cache.

Relaycast, when configured, uses the exact published chart `0.1.0` package
with SHA-256
`cdfc41cb6202fd81beb17cb57d5fbbdecaf27fa427f51faa07c5ec0fc116744f`
and image
`ghcr.io/agentworkforce/relaycast-self-host@sha256:5c86c095a335259060dc34ce7aa2e90809267395af915977a1ae608f93a83b98`.
The stack verifies the package bytes before provider resolution, extracts those
verified bytes into a deterministic digest-addressed directory, validates its
content manifest before reuse, and never deletes or replaces an active cache.
Concurrent invocations serialize first population and render that local chart
rather than re-resolving a mutable
repository index. Ingress and GKE ManagedCertificate are disabled. The service
is ClusterIP and the chart NetworkPolicy accepts callers only from its
namespace. `baseUrl` remains the chart-required signed-URL authority; it does
not create public exposure. This is the single-replica SQLite contract, not a
GCP HA proof.
`/readyz` also checks that data path when `LOCKER_PROVIDER=gcs` or
`WORKER_DATA_PATH_PROBE=1`. Production is not this lane: the production
database is Supabase and production object storage is AWS S3.

Relayfile and Relayflows remain opt-in add-ons. Relayfile needs its own
externally populated Kubernetes Secret and database URL. Relayflows needs an
existing ConfigMap containing the flow. Neither receives inline credentials.

## Gated inputs

Inputs live at `.96bulls/<stage>/gcp-inputs.json`, which is gitignored. The file
contains identifiers and names only, never a password, private key, token, DSN,
or secret payload. Example:

```json
{
  "projectId": "agentrelay-96bulls-soc2-2610",
  "projectNumber": "955297012355",
  "organizationId": "929835122282",
  "billingAccount": "014D50-E96D1A-DE6360",
  "region": "us-central1",
  "controlPlaneAuthorizedCidrs": [
    { "cidrBlock": "203.0.113.10/32", "displayName": "replace-with-operator-or-vpn" }
  ],
  "kubernetesExecutor": {
    "name": "replace-with-named-vpc-or-vpn-runner",
    "mode": "vpn",
    "identity": "gcp-vpn-runner@agentrelay-96bulls-soc2-2610.iam.gserviceaccount.com"
  },
  "clearedBy": "Khaliq's exact clearance words, YYYY-MM-DD",
  "managedDataPlane": { "provision": false },
  "relaycast": {
    "chartVersion": "0.1.0",
    "baseUrl": "https://relay.gcp.example.com"
  }
}
```

To request the application release after its external Secrets exist, set
`managedDataPlane` to the acknowledged provisioned form and add:

```json
"bulls96": {
  "enabled": true,
  "chartVersion": "0.3.0",
  "tenantOrgId": "org_replace_me",
  "existingSecretName": "96bulls-runtime",
  "imagePullSecretName": "ghcr-pull",
  "imageDigest": "sha256:<digest-built-from-the-reviewed-main-commit>",
  "relayauthJwksUrl": "http://relayauth.relayauth.svc:8787/.well-known/jwks.json",
  "relayauthIssuer": "http://relayauth.relayauth.svc:8787"
}
```

Those are names and identifiers only. Secret payloads never belong in this
file or Helm values.

`assertGcpInputs` accepts only those five target fields. `agentworkforce-dev1`
and any other project, number, org, billing account, or region are refused
before resources are declared. `203.0.113.10/32` is a documentation address.
Replace it with the operator or VPN range that can route to the private
control-plane endpoint. A public laptop address cannot reach that endpoint.
That replacement is not clearance to apply.

Any enabled chart also requires `kubernetesExecutor`. During apply, after GKE
exists but before the first Kubernetes resource, an apply-time Action requires
that named `in-vpc` or `vpn` executor to open TCP 443 to GKE's concrete RFC1918
endpoint within five seconds. Failure stops execution before Kubernetes
manifests or Helm charts. The probe carries no bearer or kubeconfig credential.

Every apply additionally requires
`NINETY_SIX_BULLS_GCP_APPLY_NONCE` to contain a fresh Google-signed OIDC ID
token for that exact executor service account. Its audience must be
`https://96bulls.agentworkforce.dev/gcp-apply/<project>/<stage>`, and its signed
email must equal `kubernetesExecutor.identity`. The stack verifies Google's
signature, issuer, audience, email, issue time, and expiry, then atomically
records the token digest in both a stage-scoped local ledger and an
`ifGenerationMatch=0` marker under the existing Alchemy state bucket. The
state-bucket marker makes the nonce single-use across executors and machines;
it contains only proof metadata and a digest, never the token. Tokens older
than five minutes, tokens with less than twenty minutes remaining,
wrong-stage/project/identity tokens, and replays are refused. A fifteen-minute
Effect deadline interrupts the whole stack before the signed mutation lease
expires; the remaining five minutes are safety margin. Late actAs, Cloud SQL,
and Kubernetes actions recheck the fixed lease deadline. The token is read
only by the apply-time Action and is not
written to Alchemy inputs, outputs, state, annotations, or errors. Its digest
changes the Action input and gates every declared GCP and Kubernetes mutation.
Plan does not execute or consume the nonce Action and performs no cloud
mutation.

The principal that mints or impersonates the executor identity needs the
organization-approved ID-token mechanism (for impersonation, normally
`roles/iam.serviceAccountTokenCreator` on that executor account). Separately,
the deploying principal must have `iam.serviceAccounts.actAs`, normally via
`roles/iam.serviceAccountUser`, on the dedicated GKE node service account. A
read-only `testIamPermissions` Action proves `actAs` after the node account
exists and before GKE creation; the cluster depends on that proof. These are
external IAM prerequisites, not grants made by this stack.

Before any plan, authenticate with the intended GCP identity and confirm the
project independently:

```sh
export CI=true
export ALCHEMY_STAGE=gcp-conformance
export NINETY_SIX_BULLS_CONFIRM_GCP_PROJECT=agentrelay-96bulls-soc2-2610
bin/96bulls plan gcp-conformance
```

Do not pass `--yes`. The stack now refuses before Alchemy construction unless
`CI=true`, because otherwise a missing state bucket could be created before
apply authorization. The command above is the same read-only wrapper recorded
below, and its last result is not an accepted plan.

`plan` resolves the Alchemy GCP environment and refuses before the first
resource when that live project or region differs from these inputs. The
refusal says `Nothing was created.` Static tests cover that comparison. They
do not call Google APIs, and they are not a cloud proof.

Before evaluating a Relaycast stage, place the exact public release artifact at
`.96bulls/$STAGE/artifacts/relaycast-0.1.0.tgz`. The stack refuses a missing or
digest-mismatched package before constructing the Alchemy Stack, provider
layers, or state backend. It reads the source artifact once, verifies those
captured bytes, and extracts only that buffer to a SHA-256-addressed cache
path; it never reopens the source between verification and extraction. Cache
population is atomic and serialized, and an existing cache is reused only
after its extracted content digest validates. The
pinned 8.0.0 image
predates later workspace-bootstrap Secret support, so this lane does not pass
an ineffective Secret setting. There is no Relaycast Ingress, load balancer,
ManagedCertificate, or public DNS claim in this stack.

## Exact gaps

These are the current code facts. They are not a cloud proof.

Cloud SQL, in `stacks/gcp/cloud-sql.ts`:

- `privateCloudSqlSpec` builds the private-only insert body: `edition` pinned
  to `ENTERPRISE` for the custom-core tier, `ipv4Enabled` false,
  `privateNetwork` set to `projects/{project}/global/networks/{name}`,
  `allocatedIpRange` set to the PSA range name, automated backups and
  point-in-time recovery enabled, and no authorized networks.
  Proxy args are `--private-ip`, `--address=0.0.0.0`, `--port=5432`, and the
  connection name.
- `sqlAdminInsertRequest` builds `POST https://sqladmin.googleapis.com/v1/projects/{project}/instances`
  with that body and `sent: false`. The request has no Authorization header.
  `ensurePrivateCloudSql` passes the bearer credential only to an injected
  transport. It returns when the private instance already matches, inserts
  only on HTTP 404, and follows HTTP 409 with another get. A public address,
  authorized network, disabled backup/PITR, or mismatched name, region,
  version, edition, tier, availability, VPC, or PSA range is refused with no
  update. A match also requires `state: RUNNABLE` and one or more syntactically
  valid `PRIVATE` IP addresses inside the configured `10.80.0.0/20` PSA
  allocation. A finished create polls
  `operations/{id}` until `DONE`, then gets the instance and refuses the
  result unless that private body is `RUNNABLE`. A thrown insert, a failed operation, or a
  timeout says to reconcile and does not say that nothing was created. One
  absolute monotonic deadline covers lookup, insert, 409 adoption, operation
  polling, sleeps, and final confirmation. It is checked before and after every
  request and sleep and before terminal success, including when a transport
  ignores abort or synchronously blocks the event loop. Error
  text omits the response body and the bearer. The apply-time Action calls the
  poster through an exact-host/path allowlisted transport and requires the
  separate exact apply confirmation
  `NINETY_SIX_BULLS_CONFIRM_GCP_SQL_POST=<project>/<instance>`. `@alchemy.run/gcp@2.0.0-beta.84`
  `GCP.SQL.Instance` is not called.
- The Auth Proxy deployment and its Workload Identity binding depend on the
  successful poster output, rather than a static `posted` flag.
  The PSA range is still shared with Memorystore.
- An omitted or `enabled: false` bulls96 block does not throw and does not
  install the release. An enabled block emits the release only with the
  acknowledged managed data plane and named external Secrets. Nothing in that
  path has been applied by this code change.

Release boundaries that remain deliberate:

- The runtime service account is not granted `secretAccessor`; Kubernetes
  Secrets remain externally populated. The private image requires the named
  image-pull Secret.
- `/readyz` and `/datapathz` require a real CA-validated Redis `PING` only when
  the GCP data-path probe is armed. The S3 default is unchanged.
- Relaycast stays `replicaCount: 1`, `runtime.backend: sqlite`, and
  `ReadWriteOnce`. It does not use Cloud SQL, Redis, Pub/Sub, or GCS.
- Secret Manager stores a secret container only. No secret version or payload
  is written.
- No product create, write, read, indexed search, manifest replay, or product
  teardown has run on GCP. The substrate is reconciled and its provider-path
  proof is recorded below; that does not close the application gaps.

## Apply, verify, and teardown

The substrate deployment and its live provider-path verification completed on
2026-10-30. The application charts remained disabled, so product create,
indexed search, manifest replay, application database access, Relaycast
availability, and full product teardown remain unproved. Static work does not
wait on the AWS adapter freeze. `bin/96bulls up` and `bin/96bulls down`
refuse every `gcp-*` stage. `bin/96bulls plan gcp-*` calls
`bunx alchemy plan --config stacks/gcp/alchemy.gcp.run.ts --stage <stage> --no-input`
and is not proof. A non-zero Alchemy status exits the wrapper with that
status. A child with no status exits 1. The 2026-10-30 missing-bucket run
exited 0 only because that wrapper passed `allowFail`. Do not rerun it to
prove the exit code. Do not apply, deploy, destroy, or enable an API without a
separate request. If `gcloud` asks to enable an API, stop and do not answer
yes. Set `CLOUDSDK_CORE_DISABLE_PROMPTS=1`. The binary on this machine, when
it is not on `PATH`, is `/home/khaliqgant/google-cloud-sdk/bin/gcloud`.

Use only `agentrelay-96bulls-soc2-2610`, number `955297012355`, parent org
`929835122282`, region `us-central1`, billing account `014D50-E96D1A-DE6360`.
Do not create a project. Do not reuse `agentworkforce-dev1`,
`sharp-sled-512718-s4`, `skip-506702`, `gen-lang-client-0703389434`,
`zeta-bonsai-489905-b1`, `relayroom-503219`, `agent-relay-internal`,
`hoopsheet`, `file-by-agent-relay`, `invertible-vent-493717-j7`, or
`gen-lang-client-0251203475`.

On 2026-10-30 a read-only plan was run with `CI=true`,
`NINETY_SIX_BULLS_CONFIRM_GCP_PROJECT=agentrelay-96bulls-soc2-2610`,
`GOOGLE_PROJECT_ID=agentrelay-96bulls-soc2-2610`, and no `--yes`:

```sh
export STAGE=gcp-conformance
bin/96bulls plan "$STAGE"
```

Alchemy stopped before a resource table: `Cloud Storage state bucket
'alchemy-state-agentworkforce-agentrelay-96bulls-soc2-2610' not found.`
`gcloud storage buckets describe gs://alchemy-state-agentworkforce-agentrelay-96bulls-soc2-2610`
returned 404. The bucket was not created. Lane APIs were not enabled. Compute
quota was not read because the compute API is disabled. The same describe
returned 404 again when this note was written. `CI=true` is what makes a
missing state bucket fail instead of being created.
That failed preflight predates the apply-time poster and is not evidence that
the current exact head plans or applies successfully. The wrapper exited 0
because it passed `allowFail`. A GCP plan now exits with the Alchemy status
instead.

Later on 2026-10-30, an approved `gcp-conformance` substrate apply reached 24
successful resources and five failures. GKE `awf-conformance-gke` completed
with one node and private endpoint `172.20.0.2`. The failures were Secret
Manager API propagation, a malformed Private Service Access global-address
network URL, two Workload Identity bindings racing the workload pool, and the
downstream PSA connection. A corrective plan then showed five creates, 22
in-place updates, two gated actions, and no replacement or delete. A subsequent
live attempt showed Memorystore rejecting a short network name while SQL Admin
accepted the canonical private network and began creating the private-only
Cloud SQL instance. At that point no evidence confirmed that instance reached
`RUNNABLE`.
PRs #36 and #39 contain the contemporaneous evidence; PR #41 added Redis TLS
readiness in code and tests only.

The corrective deployment at `dac89b5` then reconciled this partial state. Its
detailed plan contained 33 in-place updates and four gated actions, with no
create, replacement, or delete. The provider rendered stable names and project
values as `known after apply`, so a following plan still displays those same
updates; that is not accepted as a no-op claim. The deploy completed 37/37
operations successfully. The Cloud SQL action adopted the matching existing
instance (`created: false`, `operation: null`) instead of posting another
instance. The Redis CA lookup and node `actAs` preflight also passed.

Post-deploy inventory confirmed:

- GKE `awf-conformance-gke` is `RUNNING` with one node, private endpoint
  `172.20.0.2`, and workload pool
  `agentrelay-96bulls-soc2-2610.svc.id.goog`;
- Cloud SQL `awf-conformance-postgres` is `RUNNABLE`, PostgreSQL 17,
  `ipv4Enabled: false`, with only private address `10.80.0.3`, canonical VPC
  `awf-conformance-vpc`, and PSA range `awf-conformance-psa`;
- Memorystore `awf-conformance-redis` is `READY`, Redis 7.2, BASIC 1 GiB,
  AUTH off, and `SERVER_AUTHENTICATION` on private address `10.80.7.195:6378`;
- the versioned uniform-access Locker bucket, state bucket, immutable Artifact
  Registry repository, regional Secret Manager container, Pub/Sub topics and
  subscription, runtime/node/runner service accounts, VPC, subnet, NAT, and PSA
  allocation all exist in the approved project.

A short-lived private verifier VM used the named runner identity to create a
temporary namespace only after proving namespace `96bulls` did not already
exist. A Job ran as Kubernetes service account `96bulls-worker`, resolving the
existing runtime Google service account through Workload Identity. From that
pod it proved all of the following live paths:

1. the metadata identity was exactly
   `awf-conformance-runtime@agentrelay-96bulls-soc2-2610.iam.gserviceaccount.com`;
2. Redis returned `PONG` over TLS with the exact Redis Admin service CA;
3. the private Cloud SQL address accepted TCP on port 5432;
4. the runtime identity wrote, read, and deleted a marker in the Locker bucket;
5. the runtime identity published, pulled, and acknowledged a marker through
   the configured Pub/Sub topic and subscription.

Verification cleanup completed: the temporary namespace and Job were deleted,
the GCS marker was deleted, the Pub/Sub marker was acknowledged, the verifier
VM was deleted, and the temporary `roles/container.developer` binding was
removed from the runner identity. The managed substrate remains deployed for
the still-unproved application lane. No secret payload, database credential,
public endpoint, DNS record, or application chart was introduced.

Read-only preflight:

```sh
export CLOUDSDK_CORE_DISABLE_PROMPTS=1
export PROJECT=agentrelay-96bulls-soc2-2610
export STAGE=gcp-conformance
gcloud projects describe "$PROJECT" --format='value(projectId,projectNumber)'
gcloud billing projects describe "$PROJECT" --format='value(billingEnabled,billingAccountName)'
```

Stop unless every field matches: project `agentrelay-96bulls-soc2-2610`,
number `955297012355`, parent org `929835122282`, billing account
`014D50-E96D1A-DE6360`, and region `us-central1`. `assertGcpInputs` refuses
the stage unless `organizationId` and `billingAccount` are present and all
five fields match that target. A denied name or billing that is off is also
a mismatch. Do not enable `compute.googleapis.com` to read quota. Write
`.96bulls/$STAGE/gcp-inputs.json` with project `agentrelay-96bulls-soc2-2610`,
number `955297012355`, organization `929835122282`, billing account
`014D50-E96D1A-DE6360`, region `us-central1`, and no secret payload. A later plan
still needs `CI=true` and must not pass `--yes` until the state bucket is
allowed to exist.

The plan must show the live project and region matching the inputs before the
first resource. Resource order after that is: the listed Google APIs with
`disableOnDestroy: false`, custom VPC, subnet `10.52.0.0/20`, Cloud NAT, a
`/20` Private Service Access range, the service-networking connection, regional
standard GKE with a one-node default pool in `us-central1-a`, private nodes and
a private endpoint, the versioned GCS bucket, the empty Secret Manager
container, Artifact Registry, the runtime service account, Workload Identity for
`96bulls-worker` and `96bulls-alleyoop`, `roles/storage.objectUser`, and
`roles/cloudsql.client`. With `managedDataPlane.provision`, Memorystore Redis
7.2 is created with AUTH off and TLS server authentication, and Pub/Sub is
created with the dead-letter service agent. The Cloud SQL body is an Action
input; `ensurePrivateCloudSql` runs only during an approved apply after the PSA
connection resolves, and its non-secret output gates the proxy and 96Bulls.
`GCP.SQL.Instance` must not appear as a resource.

An apply that can insert Cloud SQL additionally needs the exact instance-scoped
confirmation printed by the refusal, for example:

```sh
export NINETY_SIX_BULLS_CONFIRM_GCP_SQL_POST=agentrelay-96bulls-soc2-2610/awf-conformance-postgres
```

Do not set that value for a read-only plan.

Verify, only after an approved apply and a wired poster:

1. `gcloud sql instances describe INSTANCE --project="$PROJECT" --format='yaml(ipAddresses,settings.ipConfiguration)'` shows `ipv4Enabled: false`, the VPC `privateNetwork`, the PSA `allocatedIpRange`, and no `authorizedNetworks` or public address.
2. The in-cluster proxy args start with `--private-ip`, and its Workload Identity binding exists only with that proxy.
3. Confirm the Redis Admin CA lookup returned only public certificates, then
   require `/datapathz` to pass a CA-validated `PING` from every workload. No
   `REDIS_AUTH`, `REDIS_PASSWORD`, or HMAC value may exist.
4. Run the object-store suite against GCS under Workload Identity. A separate
   relayhistory contract is still required before claiming create, indexed
   search, and manifest replay through product routes.
5. Prove Pub/Sub redelivery and Redis reconnect behavior. A readiness probe is
   not evidence that either service is a pipeline business-data adapter.

Teardown, only after that apply, in this order:

1. Delete the 96Bulls Helm release and the Cloud SQL proxy.
2. Delete the Cloud SQL instance before the network. The poster is not an Alchemy resource, so `alchemy destroy` will not delete it until a later change puts it in the graph. Delete that instance by the prepared name if it exists.
3. Empty the versioned GCS bucket. `forceDestroy` is false, so destroy fails while objects remain.
4. `bunx alchemy destroy --config stacks/gcp/alchemy.gcp.run.ts --stage "$STAGE" --no-input` with the same confirm env. Let Alchemy delete the resources it tracks; do not pre-delete Redis, Pub/Sub, GKE, networking, the bucket, Artifact Registry, Secret Manager, or the runtime service account and leave stale state behind. APIs stay enabled because the stack sets `disableOnDestroy: false`.
5. Read-only proof of empty: `gcloud sql instances list`, `gcloud redis instances list --region=us-central1`, `gcloud container clusters list`, `gcloud compute networks list`, and `gcloud storage ls` against `$PROJECT` return no lane resources. On this dedicated project, disable the listed APIs only after those checks are empty.

Do not delete the project until those lists are empty, and do not delete it without a separate request from Khaliq. Production remains Supabase and AWS S3. This lane is not end-to-end ready until the verify steps have passed on `agentrelay-96bulls-soc2-2610`.
