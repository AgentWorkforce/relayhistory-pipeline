# GCP self-hosted reference lane

This lane is a concurrent, code-only implementation of the provider contract
that will be proven after the AWS adapter contract is stable. Nothing in this
document is evidence of a GCP deployment. No project, credentials, secret
payload, DNS record, or cloud resource was created while implementing it.

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
  bindings for the known 96Bulls and Cloud SQL proxy service accounts;
- optionally, Cloud SQL PostgreSQL 17, Memorystore Redis 7.2, Pub/Sub with
  retry/dead-letter handling, and a two-replica Cloud SQL Auth Proxy service;
- optionally, the Relaycast core chart and RelayAuth, Relayfile, or Relayflows
  charts. Relayflows runs in standalone mode and is not in Relaycast's
  availability path.

Cloud SQL currently has a public address with no authorized network allowlist;
pods reach it only through the IAM-authenticated, TLS Cloud SQL Auth Proxy.
The Alchemy provider used here does not yet expose Cloud SQL private-network
attachment. Moving Cloud SQL onto Private Service Access is a proof blocker,
not something this stack silently claims.

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

`bulls96.enabled` installs the chart when the managed data plane is provisioned
and the chart version, tenant org id, and existing Secret name are set. The
release turns off in-chart MinIO and CloudNativePG, uses pod identity, and
passes the bucket, Cloud SQL connection name, Redis host, and Pub/Sub names.
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
- `@alchemy.run/gcp@2.0.0-beta.84` `GCP.SQL.Instance` still writes only
  `ipv4Enabled`. The stack does not call it. `posted` is false, so apply does
  not create the instance until a resource posts that body.
- When 96Bulls is installed, the Auth Proxy Deployment uses those args. The
  PSA range is still shared with Memorystore.

Release wiring that has not been applied:

- With `bulls96.enabled`, the Helm release gets the GCS bucket, the Cloud SQL
  connection name, the Redis host and port, and the Pub/Sub topic and
  subscription. HMAC keys, Redis AUTH, and the database URL are not in values.
  `HOOPDB_URL` stays in the named Secret.
- `/readyz` and `/datapathz` check that data path only when the probe is armed.
  The S3 default is unchanged.
- Relaycast stays `replicaCount: 1`, `runtime.backend: sqlite`, and
  `ReadWriteOnce`. It does not use Cloud SQL, Redis, Pub/Sub, or GCS.
- Secret Manager stores a secret container only. No secret version or payload
  is written.
- No create, write, read, search, replay, or teardown has run on GCP.

## Proof plan

AWS remains the first complete proof. This lane does not claim that proof.
Static evidence already in this branch is the deployment-target guard, the
chart-value contract, and the Locker provider selector. The cloud proof, after
the AWS adapter contract is frozen and Khaliq approves an exact project, is:

1. run the object-store suite against GCS under Workload Identity;
2. run database migrations and queue/idempotency tests against Cloud SQL;
3. run pub/sub and Redis reconnect, redelivery, ordering, and failover tests;
4. run Relaycast core API/WebSocket/file conformance without provider branches;
5. run a planted session through every enabled 96Bulls stage and retrieve its
   exact trace and durable records;
6. scan GCS objects, traces, and logs for the planted PII/secret markers;
7. restart, node-drain, zone-failure, backup/restore, and secret-rotation tests;
8. only then mark the GCP profile end-to-end ready.

Until those gates pass, the output is a buildable reference lane, not a fully
proven self-hosted environment.
