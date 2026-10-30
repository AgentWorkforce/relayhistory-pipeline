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

The 96Bulls add-on is blocked on GCP today. The shared object-store package has
a conformant GCS driver, but `src/shared/locker.ts` and the service environment
still construct S3 unconditionally. Setting `bulls96.enabled=true` fails before
any resource is declared. The correct fix is to select the provider inside the
96Bulls runtime and then teach the chart to wire Workload Identity; using HMAC
keys to disguise GCS as S3 would defeat the adapter and identity contract.

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

## Proof order

AWS remains the first complete proof. Once its adapter/conformance contract is
committed, GCP should reuse the contract in this order:

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
