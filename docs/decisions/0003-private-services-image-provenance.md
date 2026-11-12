# 0003: Verify services-image provenance without making source public

**Date:** 2026-11-06
**Status:** accepted for implementation; first live publication remains validation

> **Update (2026-11-11):** Khaliq decided to make this repository public, which
> is the separate visibility decision the first alternative below calls for.
> Hosted operator runbooks and handoff packets were removed from the tree first
> and preserved in the private `AgentWorkforce/sessions-pipeline-cloud` under
> `hosted-ops/preserved/`; git history keeps the originals. The Cosign/Sigstore
> path below stays in place. Switching to GitHub's native artifact attestations,
> now available for this repository, is a separate change.

## Context

The services repository is private on GitHub Free. GitHub's native artifact
attestation API requires public source on that plan; a previous image release
failed at its attestation step after publishing image bytes. The Amp rollout
requires a verified native image, and its approval does not authorize exposing
the source repository/history or upgrading an organization plan.

## Decision

Preserve the canonical private repository and private registry. Use pinned
Cosign with the existing GitHub Actions OIDC identity, public Sigstore/Fulcio
certificates and Rekor inclusion proofs. Sign two DSSE statements over the exact
amd64/arm64 OCI index before registry writes. Verify issuer, workflow identity,
repository, main ref, immutable workflow SHA, manual trigger, subject digest,
approved source, CI and invocation before copying. Record the verified bundles,
index and receipt as a release artifact for independent verification without
private-package credentials.

Retain the owner-only dispatcher/rerun, environment approval, source ancestry,
latest complete successful main CI and digest-preserving copy gates. Refuse a
changed CI run after signing, and recheck source CI after registry verification.
Keep registry credentials in a sealed Linux memory descriptor; private Git fetch
auth is process-scoped. No long-lived signing key or plaintext credential file
is introduced.

## Alternatives

- Making the repository public requires a separate readiness/visibility decision.
- An Enterprise plan change is not part of this integration.
- Publishing without verified provenance does not meet the release contract.
- A long-lived signing key adds a custody/rotation burden to the existing OIDC
  identity, which can already provide the required cryptographic provenance.

## Consequences

The source stays private. Certificate/release metadata is public in Sigstore
transparency infrastructure, as documented in the operator runbook; it contains
no source code or provider data. Publishing requires Sigstore availability and
fails closed before registry writes on signing/verification failure. Independent
verification checks the downloaded index and both bundles against approved run
metadata. Live OIDC issuance, registry copy and real deployment remain validation
steps; unit tests stub the crypto boundary and do not establish those outcomes.
