# GCP self-hosted reference lane

This lane is a code-only conformance implementation. Static work does not wait
on the AWS adapter freeze. Nothing in this document is evidence of a GCP
deployment. No project, credentials, secret payload, DNS record, or cloud
resource was created while implementing it.

The stack entry point is `stacks/gcp/alchemy.gcp.run.ts`. It uses the same
Alchemy + AgentWorkforce Helm boundary as the AWS stack: cloud code declares
provider resources and passes non-secret endpoints/names into charts; runtime
implementations own provider adapters.

## Declared substrate

- regional GKE Autopilot with private nodes, Workload Identity Federation,
  Dataplane V2, Shielded Nodes, VPC-native pod/service ranges, Cloud NAT, and
  VPC flow logs;
- versioned GCS Locker bucket with uniform bucket-level access;
- private Artifact Registry repository with immutable tags;
- a regional Secret Manager secret container (metadata only; no payload enters
  inputs or Alchemy state);
- a least-privilege runtime Google service account and Workload Identity
  bindings for the 96Bulls worker and Alleyoop service accounts. The Cloud SQL
  proxy binding is declared only with the proxy manifests;
- optionally, a private-only Cloud SQL PostgreSQL 17 specification (not posted),
  Memorystore Redis 7.2 with AUTH off and TLS server authentication,
  Pub/Sub with retry/dead-letter handling, and a single-replica Cloud SQL Auth
  Proxy manifest that is not applied while that instance is unposted;
- optionally, the Relaycast core chart and RelayAuth, Relayfile, or Relayflows
  charts. Relayflows runs in standalone mode and is not in Relaycast's
  availability path.

Cloud SQL is not created. The stack prepares a SQL Admin `instances.insert`
for public IPv4 off, the Private Service Access network and range, and
`--private-ip` proxy routing. `postPrivateCloudSqlInsert` is the only sender,
it has no default transport, and the stack does not call it. `GCP.SQL.Instance`
is not called, because that resource drops `privateNetwork` and can create a
public instance.

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

The static `bulls96.enabled` values contract turns off in-chart MinIO and
CloudNativePG, uses pod identity, and passes the bucket, Cloud SQL connection
name, Redis host, and Pub/Sub names. The live stack refuses that flag before
declaring any resource until a reviewed resource can post the private-only
Cloud SQL body; it cannot create a knowingly broken release.
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
  "projectId": "agentworkforce-dev1",
  "projectNumber": "123456789012",
  "region": "us-central1",
  "controlPlaneAuthorizedCidrs": [
    { "cidrBlock": "203.0.113.10/32", "displayName": "operator-vpn" }
  ],
  "clearedBy": "Khaliq's exact clearance words, YYYY-MM-DD",
  "managedDataPlane": { "provision": false },
  "relaycast": {
    "chartVersion": "0.1.0",
    "baseUrl": "https://relay.gcp.example.com"
  }
}
```

Before any plan, authenticate with the intended GCP identity and confirm the
project independently:

```sh
export NINETY_SIX_BULLS_CONFIRM_GCP_PROJECT=agentworkforce-dev1
bin/96bulls plan gcp-dev
```

`plan` resolves the Alchemy GCP environment and refuses before the first
resource when that live project or region differs from these inputs. The
refusal says `Nothing was created.` Static tests cover that comparison. They
do not call Google APIs, and they are not a cloud proof.

The Relaycast chart version must be published before a repository-backed plan
can render it. Its pinned 8.0.0 image predates the later workspace-bootstrap
Secret support, so this lane does not pass an ineffective Secret setting. A GKE
ManagedCertificate is declared, but DNS must point the configured hostname at
the resulting load balancer before certificate issuance can complete.

## Exact gaps

These are the current code facts. They are not a cloud proof.

Cloud SQL, in `stacks/gcp/cloud-sql.ts`:

- `privateCloudSqlSpec` builds the private-only insert body: `ipv4Enabled`
  false, `privateNetwork` set to `/projects/{project}/global/networks/{name}`,
  `allocatedIpRange` set to the PSA range name, and no authorized networks.
  Proxy args are `--private-ip`, `--address=0.0.0.0`, `--port=5432`, and the
  connection name.
- `sqlAdminInsertRequest` builds `POST https://sqladmin.googleapis.com/v1/projects/{project}/instances`
  with that body and `sent: false`. `postPrivateCloudSqlInsert` sends it only
  through an injected transport. The stack calls the builder and does not call
  the poster. `@alchemy.run/gcp@2.0.0-beta.84` `GCP.SQL.Instance` is not called.
- The Auth Proxy manifests stay unapplied while `posted` is false. The proxy
  Workload Identity binding is created with those manifests, not before them.
  The PSA range is still shared with Memorystore.
- Stack evaluation throws while `bulls96.enabled` is set. The release values
  are still a tested pure function. Nothing in that path is applied.

Release wiring that has not been applied:

- The Helm values carry the GCS bucket, the Cloud SQL connection name, the
  Redis host and port, and the Pub/Sub topic and subscription. Alleyoop gets
  `auth.jwksUrl`. HMAC keys, Redis AUTH, query strings, and the database URL
  are not in values. `HOOPDB_URL` stays in the named Secret. Memorystore AUTH
  is off. The runtime service account is not granted `secretAccessor`.
- `/readyz` and `/datapathz` check that data path only when the probe is armed.
  The S3 default is unchanged.
- Relaycast stays `replicaCount: 1`, `runtime.backend: sqlite`, and
  `ReadWriteOnce`. It does not use Cloud SQL, Redis, Pub/Sub, or GCS.
- Secret Manager stores a secret container only. No secret version or payload
  is written.
- No create, write, read, search, replay, or teardown has run on GCP.

## Apply, verify, and teardown

This runbook has not been executed. Static work does not wait on the AWS
adapter freeze. No command below has been run. `bin/96bulls up` and
`bin/96bulls down` refuse every `gcp-*` stage. `bin/96bulls plan gcp-*` calls
`bunx alchemy plan --config stacks/gcp/alchemy.gcp.run.ts --stage <stage> --no-input`
and is not proof. Do not run plan, deploy, or destroy until Khaliq names a new
project in his own words.

Create a new project. Do not reuse a project that is already visible on this
account, including `agentworkforce-dev1`, `sharp-sled-512718-s4`, `skip-506702`,
`gen-lang-client-0703389434`, `zeta-bonsai-489905-b1`, `relayroom-503219`,
`agent-relay-internal`, `hoopsheet`, `file-by-agent-relay`,
`invertible-vent-493717-j7`, or `gen-lang-client-0251203475`. Link billing to
the new project. Use region `us-central1`. If `gcloud` asks to enable an API,
stop and do not answer yes. Set `CLOUDSDK_CORE_DISABLE_PROMPTS=1`. The binary
on this machine, when it is not on `PATH`, is
`/home/khaliqgant/google-cloud-sdk/bin/gcloud`.

Read-only preflight, after that project exists:

```sh
export CLOUDSDK_CORE_DISABLE_PROMPTS=1
export PROJECT=new-project-id
export STAGE=gcp-conformance
gcloud projects describe "$PROJECT" --format='value(projectId,projectNumber)'
gcloud billing projects describe "$PROJECT" --format='value(billingEnabled,billingAccountName)'
gcloud compute regions describe us-central1 --project="$PROJECT" --format='yaml(quotas)'
```

Stop if the project is one of the denied names, billing is off, or the quota
read fails. Write `.96bulls/$STAGE/gcp-inputs.json` with that new project id,
its project number, region `us-central1`, and no secret payload. Then:

```sh
export NINETY_SIX_BULLS_CONFIRM_GCP_PROJECT="$PROJECT"
bin/96bulls plan "$STAGE"
```

The plan must show the live project and region matching the inputs before the
first resource. Resource order after that is: the listed Google APIs with
`disableOnDestroy: false`, custom VPC, subnet `10.52.0.0/20`, Cloud NAT, a
`/20` Private Service Access range, the service-networking connection, Autopilot
GKE with private nodes, the versioned GCS bucket, the empty Secret Manager
container, Artifact Registry, the runtime service account, Workload Identity for
`96bulls-worker` and `96bulls-alleyoop`, `roles/storage.objectUser`, and
`roles/cloudsql.client`. With `managedDataPlane.provision`, Memorystore Redis
7.2 is created with AUTH off and TLS server authentication, and Pub/Sub is
created with the dead-letter service agent. The Cloud SQL body is prepared as
`sqlInsert` and is not posted. `GCP.SQL.Instance` must not appear as a resource.
`bulls96.enabled` still throws before any resource, so this plan cannot install
96Bulls until a reviewed change posts the insert and then lifts that guard.

Verify, only after an approved apply and a wired poster:

1. `gcloud sql instances describe INSTANCE --project="$PROJECT" --format='yaml(ipAddresses,settings.ipConfiguration)'` shows `ipv4Enabled: false`, the VPC `privateNetwork`, the PSA `allocatedIpRange`, and no `authorizedNetworks` or public address.
2. The in-cluster proxy args start with `--private-ip`, and its Workload Identity binding exists only with that proxy.
3. Redis `authEnabled` is false and `transitEncryptionMode` is `SERVER_AUTHENTICATION`. No `REDIS_AUTH` is in the workload env.
4. Run the object-store suite against the GCS bucket under Workload Identity. Create, write, read, search, and replay one planted session through 96Bulls. `/readyz` and `/datapathz` return 200. Scan objects, traces, and logs for the planted markers.
5. Pub/Sub redelivery reaches the dead-letter topic. Redis reconnects after a restart.

Teardown, only after that apply, in this order:

1. Delete the 96Bulls Helm release and the Cloud SQL proxy.
2. Delete the Cloud SQL instance before the network. The poster is not an Alchemy resource, so `alchemy destroy` will not delete it until a later change puts it in the graph. Delete that instance by the prepared name if it exists.
3. Empty the versioned GCS bucket. `forceDestroy` is false, so destroy fails while objects remain.
4. `bunx alchemy destroy --config stacks/gcp/alchemy.gcp.run.ts --stage "$STAGE" --no-input` with the same confirm env. Let Alchemy delete the resources it tracks; do not pre-delete Redis, Pub/Sub, GKE, networking, the bucket, Artifact Registry, Secret Manager, or the runtime service account and leave stale state behind. APIs stay enabled because the stack sets `disableOnDestroy: false`.
5. Read-only proof of empty: `gcloud sql instances list`, `gcloud redis instances list --region=us-central1`, `gcloud container clusters list`, `gcloud compute networks list`, and `gcloud storage ls` against `$PROJECT` return no lane resources. On this dedicated project, disable the listed APIs only after those checks are empty.

Do not delete the project until those lists are empty, and do not delete it without a separate request from Khaliq. Production remains Supabase and AWS S3. This lane is not end-to-end ready until the verify steps have passed on that new project.
