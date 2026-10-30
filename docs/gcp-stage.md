# GCP self-hosted reference lane

This lane is a code-only conformance implementation. Static work does not wait
on the AWS adapter freeze. Nothing in this document is evidence of a GCP
deployment. No project, credentials, secret payload, DNS record, or cloud
resource was created while implementing it. The only eligible target is the
existing project `agentrelay-96bulls-soc2-2610`, number `955297012355`, parent
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

Cloud SQL has not been created in this lane. On an approved apply, the stack
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

`bulls96.enabled` currently fails closed before the first provider resource.
Memorystore `SERVER_AUTHENTICATION` needs the service CA, a CA-aware runtime
client, and an in-VPC TLS round trip; static environment strings are not that
proof. Managed Redis may be created only as acknowledged external adapter-test
infrastructure and is not wired into 96Bulls.

Relaycast, when configured, uses the exact published chart `0.1.0` package
with SHA-256
`cdfc41cb6202fd81beb17cb57d5fbbdecaf27fa427f51faa07c5ec0fc116744f`
and image
`ghcr.io/agentworkforce/relaycast-self-host@sha256:5c86c095a335259060dc34ce7aa2e90809267395af915977a1ae608f93a83b98`.
The stack verifies the package bytes before provider resolution, extracts those
verified bytes into a deterministic digest-addressed directory, and renders
that local chart rather than re-resolving a mutable
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
than five minutes, expiring tokens, wrong-stage/project/identity tokens, and
replays are refused. The token is read only by the apply-time Action and is not
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
path; it never reopens the source between verification and extraction. The
pinned 8.0.0 image
predates later workspace-bootstrap Secret support, so this lane does not pass
an ineffective Secret setting. There is no Relaycast Ingress, load balancer,
ManagedCertificate, or public DNS claim in this stack.

## Exact gaps

These are the current code facts. They are not a cloud proof.

Cloud SQL, in `stacks/gcp/cloud-sql.ts`:

- `privateCloudSqlSpec` builds the private-only insert body: `edition` pinned
  to `ENTERPRISE` for the custom-core tier, `ipv4Enabled` false,
  `privateNetwork` set to `/projects/{project}/global/networks/{name}`,
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
  install the release. Stack evaluation rejects `bulls96.enabled` even with the
  acknowledged managed data plane until the external Redis TLS contract is
  implemented. Nothing in that path has been applied.

Release wiring that remains deliberately blocked:

- 96Bulls Helm values are not emitted. The Redis CA/client/round-trip gate
  rejects `bulls96.enabled` before cloud provider resolution. The runtime
  service account is not granted `secretAccessor`.
- Existing `/readyz` and `/datapathz` checks do not claim a TLS connection to
  Memorystore. The S3 default is unchanged.
- Relaycast stays `replicaCount: 1`, `runtime.backend: sqlite`, and
  `ReadWriteOnce`. It does not use Cloud SQL, Redis, Pub/Sub, or GCS.
- Secret Manager stores a secret container only. No secret version or payload
  is written.
- No create, write, read, search, replay, or teardown has run on GCP.

## Apply, verify, and teardown

Apply, verify, and teardown have not been executed. Static work does not wait
on the AWS adapter freeze. `bin/96bulls up` and `bin/96bulls down` refuse every
`gcp-*` stage. `bin/96bulls plan gcp-*` calls
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
instead. Do not bootstrap the state bucket, enable APIs, or rerun the plan.

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
3. Treat Redis as external and unattached. Before removing the code gate,
   obtain its CA, implement CA-aware client configuration, and prove a TLS
   round trip from the workload.
4. Only after that follow-up code exists, run the object-store suite against
   GCS under Workload Identity and create, write, read, search, and replay one
   planted session through 96Bulls.
5. Prove Pub/Sub redelivery and Redis reconnect behavior only after the runtime
   adapter gate is removed by reviewed code.

Teardown, only after that apply, in this order:

1. Delete the 96Bulls Helm release and the Cloud SQL proxy.
2. Delete the Cloud SQL instance before the network. The poster is not an Alchemy resource, so `alchemy destroy` will not delete it until a later change puts it in the graph. Delete that instance by the prepared name if it exists.
3. Empty the versioned GCS bucket. `forceDestroy` is false, so destroy fails while objects remain.
4. `bunx alchemy destroy --config stacks/gcp/alchemy.gcp.run.ts --stage "$STAGE" --no-input` with the same confirm env. Let Alchemy delete the resources it tracks; do not pre-delete Redis, Pub/Sub, GKE, networking, the bucket, Artifact Registry, Secret Manager, or the runtime service account and leave stale state behind. APIs stay enabled because the stack sets `disableOnDestroy: false`.
5. Read-only proof of empty: `gcloud sql instances list`, `gcloud redis instances list --region=us-central1`, `gcloud container clusters list`, `gcloud compute networks list`, and `gcloud storage ls` against `$PROJECT` return no lane resources. On this dedicated project, disable the listed APIs only after those checks are empty.

Do not delete the project until those lists are empty, and do not delete it without a separate request from Khaliq. Production remains Supabase and AWS S3. This lane is not end-to-end ready until the verify steps have passed on `agentrelay-96bulls-soc2-2610`.
