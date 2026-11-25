# Hosted continuous session publication

Review base: pipeline `34f60058`, infra `89449cb7`. This proposal stages the host independently of the one-off S3 backlog drain. Decision 0007 places hosted sessions on agentrelay EKS. Pipeline owns the publisher runtime and the frontier contract. The infra chart and environment owners must accept the service account, network and release plans before rollout.

## Existing components

Use `docker/services.Dockerfile` with `SERVICE=session-publication`. The existing entrypoint starts `publication-service.ts --continuous` and handles SIGTERM. A dormant process with `INGEST_PUBLISH_ENABLED=false` answers both probe paths with enabled=false before it connects to a database or store. When enabled, readiness requires a completed healthy cycle. An empty candidate set is healthy, so a successful readiness probe cannot establish downstream publication.

The AWS binding currently verifies a pinned EC2 instance and instance profile via IMDSv2. The portable binding uses static object-store credentials or its supported portable providers. Neither is a verified AWS EKS Pod Identity binding for production. The source-only host lane recorded under `codex/canonical-publisher-host-20261107` has no matching head on the pipeline origin at review time.

## Reviewable implementation units

1. In pipeline, add an explicit EKS target to the publication target/config dispatch. Reuse the trusted Pod Identity endpoint and rotating credential provider used by the rearm sweeper. Exercise provider refresh across a publication cycle, TLS-verified DB login as the expected ingest identity, target account/bucket/key-root/region/generation checks and the existing source/commit RPCs. Preserve existing AWS and portable behavior. Reject missing or mixed credential sources without fallback. Require object access and encryption permissions from the dedicated publisher identity only.
2. In infra, extend `charts/sessions-pipeline-worker` with a distinct publisher Deployment and service account. Proposed chart defaults omit all publisher resources; explicitly rendering the dormant component still sets the runtime flag false. Use one replica, Recreate strategy, finite termination grace, port 8081 with /healthz and /readyz, a pinned services image digest, restricted security context, named DB credential references and scoped egress. Test default render, dormant render, invalid target, gated enablement and absence of sibling workloads or selector changes. Stage the digest and service account in a separate environment values review.
3. Provision authoritatively verified frontiers through the owner lane. Before a hosted canary, establish how the candidate RPC limits the run to its approved scope. Its batch limit (default one) bounds work per tick and does not constrain tenant scope. Record generation, bucket, root and frontier provenance, privacy-control coverage, how revocation invalidates stale work, and a safe handoff from any active backlog drain. Avoid concurrent ownership of the same stages until equivalent locking and markers have been proven.

## Verification and rollback

Use a disposable DB and versioned store with synthetic data. Test acknowledgement without a frontier, invalidated generation or privacy controls during processing, role mismatch, a crash before commit, restart after commit, and removal of retired material. Verify the canonical head and bytes, index/pointer and same-caller hosted list/point reads separately. Capture counts and backend only. Require independent proof that erasure remains operational during a publisher pause.

For a separately approved dormant rollout, verify the image identity, effective flag, Pod Identity association and probe responses. For a separately approved canary, capture eligible and deferred backlog and oldest age, frontier age and coverage, last successful cycle, claim/commit outcomes, DB load, downstream lag and retirement backlog. Alert on stalled progress with eligible work even when probes remain green. Stop on a privacy/fencing failure, wrong generation or scope, missing erasure coverage, or unexplained downstream mismatch. Disable the publisher, wait for in-flight leases to resolve, and reconcile public heads with the journal before resuming. Keep erasure running.

Host implementation timing depends on acceptance of the EKS target, credential ownership and reviewed frontier scope. A hosted launch also needs an approved image, chart release, network and IAM changes and evidence from the disposable proof.

## Local dormant image preview

Build the services image locally with `docker build -f docker/services.Dockerfile -t session-publication-preview .`. From the repository root run `SESSION_PUBLICATION_PREVIEW_IMAGE=session-publication-preview docker compose -f deploy/publication-preview/compose.yaml up --wait`. The preview has no external network, credential bindings or published ports. Its in-container probe checks both endpoints report dormant. Remove it with the same environment assignment and Compose file followed by `down`.

### EKS credential boundary

`src/session-ingest/publication-pod-credentials.ts` implements the provider component for a future explicit EKS target. It requires the EKS agent address and its standard projected token mount, checks the opened inode on Linux, and verifies each new credential session with a signed regional STS GetCallerIdentity request against the expected role and account. Direct requests refuse redirects. Refreshes re-read the mounted token and require more than five minutes of remaining credential lifetime. The component runs only when its caller explicitly asks for credentials. The existing EC2 target continues to refuse pod indicators.

Before adding an EKS target to the runtime selector, separate the runner identity account from the target storage/KMS account in the publisher config. Bind the reviewed database login to the intended project and generation and integrate the rotating provider at each AWS operation, including version-specific erasure (the current runtime passes the eraser one resolved credential set). Exercise these boundaries with a disposable runtime and approved target fixture. The credential component alone does not select a deployment, grant permission or prove hosted connectivity.

### Scoped-host pause and rollback rehearsal

The reserved target `INGEST_PUBLICATION_TARGET=eks-scoped` provides a hard activation stop in the target selector. When an operator requests execution today, startup fails with `publication_eks_scoped_contract_unavailable` before opening the probe port or selecting an EC2 or portable runtime. Installing a similarly named database object does not lift this stop. Removing it requires a reviewed implementation that verifies the deployed contract and binds both candidate selection and the final write transaction to the same authorized, unexpired scope and generation. A tenant label in the pod and a read-time candidate filter cannot provide that guarantee.

The ephemeral-process test in `publication-service-portable.process.test.ts` exercises the real image entrypoint against a disposable local port: start dormant, verify both probes, SIGTERM, attempt activation, observe refusal before readiness, and return to the same dormant configuration. Its receipt covers service lifecycle only. Separate DB/store rehearsal must prove in-flight commit, retirement and erasure outcomes before activating a real cohort.
