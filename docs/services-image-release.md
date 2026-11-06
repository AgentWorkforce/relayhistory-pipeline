# Releasing the services image

Use manual `publish-image` after Khaliq approves the source commit and publication.
Both dispatcher and rerun initiator must be Khaliq. It requires main, the
confirmation phrase and the image-publish environment. Code integration approval
alone does not authorize publication or deployment.

Both jobs use the repository's configured `CI_LINUX_RUNNER`, falling back to
`ubuntu-latest`, matching the existing production session-read Worker deploy.
The owner-only main dispatch and image-publish environment still gate execution.

The full lowercase source_sha must belong to current original main. Its latest
official main/push CI run and every effective job must pass. The read-only
preflight checks every job page and latest attempt, never an older green run.
Private ancestry fetch authenticates only that process; no checkout credential
is saved to git configuration. CI is rechecked after environment approval,
before signing, before registry copy, and after registry verification.

Only sha-<source_sha> is published to ghcr.io/agentworkforce/96bulls-services.
The publisher never moves main or latest. The single-line, non-sensitive approval
reference becomes image metadata. Preserve this repository's existing package
Actions access; a denied push is a permission failure to resolve with its owner.

## Provenance for private repositories

The canonical repository can remain private. GitHub artifact attestations are
unavailable for private repositories on this plan, so the publisher uses pinned
Cosign, GitHub Actions OIDC, public Sigstore/Fulcio certificates and Rekor proofs.
It creates no long-lived signing key and does not make source/history public.
Certificate and release metadata (repository, workflow, source/CI identifiers,
artifact digest) are public in transparency infrastructure. Approval notes must
contain no sensitive information.

The job builds amd64/arm64 into a local OCI archive with SBOM and max-mode
BuildKit provenance. It extracts the exact index by the builder's output digest,
checks its bytes/platforms, and signs two in-toto statements before registry writes:

- SLSA v1 provenance binds source, immutable workflow revision, image repository,
  source dependency and publication invocation.
- https://agentrelay.com/attestations/reviewed-image-source/v1 binds the exact
  source, workflow, main ref, successful source CI run and image digest.

Both bundles must pass Cosign verification of the exact GitHub issuer, workflow
identity, repository, main ref, workflow SHA, manual trigger and subject digest.
The claim checker also requires the approved source, CI and invocation. JSON
parsing alone does not establish trust. Any failed signing/verification stops
before copying the OCI archive with Skopeo. The copied registry index must match
the signed digest; final CI must still identify the signed source run. These are
point-in-time checks, not an atomic lock on future CI activity. Registry auth
is held in a sealed Linux anonymous-memory descriptor, inherited by Skopeo. No
registry credential file or credential argument is created; no disk fallback
is available when the memory descriptor is unsupported.

## Verify a release

After successful signing, registry verification and final CI, the release-receipt
artifact contains release-receipt.json, services-index.json,
build-provenance.sigstore.json and reviewed-source.sigstore.json. These are public
verification material/metadata, without a private signing key, provider key or
source code. Find the approved dispatch and download its artifact:

```sh
gh run list --repo AgentWorkforce/sessions-pipeline --workflow publish-image.yml \
  --event workflow_dispatch --json databaseId,headSha,status,conclusion,url
gh run view "$publish_run_id" --repo AgentWorkforce/sessions-pipeline
gh run download "$publish_run_id" --repo AgentWorkforce/sessions-pipeline \
  --name release-receipt --dir "$receipt_dir"
```

Check image/source/workflow/CI identities against the approved source and actual
run metadata. Set image, source_sha, workflow_sha, ci_run_id, publish_run_id and
publish_run_attempt to those reviewed values. Use the trusted verification
script and pinned Cosign 3.1.4. Run this as one Bash script, so failure stops
promotion:

```bash
set -euo pipefail
export ARTIFACT_DIR="$receipt_dir"
export IMAGE_DIGEST="${image##*@}"
test "$image" = "ghcr.io/agentworkforce/96bulls-services@$IMAGE_DIGEST"
export SOURCE_SHA="$source_sha" WORKFLOW_SHA="$workflow_sha" SOURCE_CI_RUN="$ci_run_id"
export PUBLISH_RUN_ID="$publish_run_id" PUBLISH_RUN_ATTEMPT="$publish_run_attempt"
node scripts/image-release-provenance.mjs verify
```

This verifies the actual index and both signed bundles without private-package
pull credentials. Independently inspect both platform manifests/SBOMs, verify
copied registry bytes against that digest and smoke-test the image. Pin the
verified digest for deployment. Publication alone applies no deployment or
migration. workflow_sha identifies the signer; source_sha identifies the image
checkout. Those commits can differ. A matching label/tag, green job or parsed
predicate alone does not prove identity.

## Canonical repository

Publish from AgentWorkforce/sessions-pipeline, preserving identity/history.
The sessions-pipeline-oss import has unrelated history and cannot supply its
release source. Preserve hosted deployment, schema-release and migration pins.
Repository visibility stays a separate owner decision; this path needs no change.
