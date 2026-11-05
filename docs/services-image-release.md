# Releasing the services image

Use the manual `publish-image` workflow after the operator approves the source
commit and publication. The workflow still requires Khaliq's GitHub identity,
the `main` workflow ref, the confirmation phrase and the `image-publish`
environment. Approval to merge code does not authorize publication.

Set `source_sha` to the approved full lowercase commit SHA. The commit must be
on `main`; a tag, branch name, abbreviated SHA or off-main commit is refused.
Omitting the input builds the workflow run's commit, preserving the existing
default. Images use `sha-<source_sha>` and an immutable registry digest. An
older pinned source does not update the moving `main` image tag.

The workflow builds amd64 and arm64 images, generates an SBOM and max-mode
BuildKit provenance, and signs two statements for the published digest:

- GitHub build provenance identifies the workflow and its commit.
- `https://agentrelay.com/attestations/reviewed-image-source/v1` records the
  checked-out `source_sha`, its repository, its verified membership on `main`,
  and the `workflow_sha`. These commits can differ for a pinned release.

## Verify a release

Obtain `image` (including its `sha256:` digest), `source_sha` and `workflow_sha`
from the reviewed release receipt. Verify both the trusted workflow identity
and the source statement before promoting the image:

```bash
gh attestation verify "oci://$image" \
  --repo AgentWorkforce/sessions-pipeline \
  --signer-workflow AgentWorkforce/sessions-pipeline/.github/workflows/publish-image.yml \
  --signer-digest "$workflow_sha" --source-ref refs/heads/main

gh attestation verify "oci://$image" \
  --repo AgentWorkforce/sessions-pipeline \
  --signer-workflow AgentWorkforce/sessions-pipeline/.github/workflows/publish-image.yml \
  --signer-digest "$workflow_sha" --source-ref refs/heads/main \
  --predicate-type https://agentrelay.com/attestations/reviewed-image-source/v1 \
  --format json > source-verification.json

jq -e --arg source "$source_sha" --arg workflow "$workflow_sha" '
  any(.[]; .verificationResult.statement.predicate |
    .repository == "https://github.com/AgentWorkforce/sessions-pipeline" and
    .source_ref == "refs/heads/main" and
    .source_sha == $source and .workflow_sha == $workflow)
' source-verification.json
```

GitHub's certificate source digest describes the workflow run's source, so do
not substitute the image's older `source_sha` for `workflow_sha` when checking
the signer. Source metadata is a claim made by the trusted workflow; the
signed statement does not replace reviewing that workflow's checkout and
ancestry checks.

Inspect both platform manifests and their SBOMs, then smoke-test the published
digest. Pin that digest in deployment configuration. Publication alone does
not upgrade a Helm release or apply database migrations.
