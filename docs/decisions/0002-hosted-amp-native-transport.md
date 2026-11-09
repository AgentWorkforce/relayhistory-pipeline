# 0002: Host the Amp native adapter in a reusable Cloudflare Container

**Status:** accepted for the approved Amp rollout
**Date:** 2026-11-06

## Context

Cloud and hosted ingest run in Cloudflare Workers, which cannot start the native
Amp CLI. The earlier transport runbook assumed an existing process host. The
services image supplies the fixed native adapter, but the hosted environment
still needs a place to run it. Self-hosted EKS stages are not production hosts
under decision 0001. Per-operation sandboxes and runtime CLI installation do not
meet the user's requested operating model.

## Decision

Run one named, capped Cloudflare Container in the existing production account,
fronted by a Worker exposing only the dedicated-token Amp operation route. Reuse
the verified services image, with a static entrypoint that starts only its Amp
routes. Give it no ingest/database credentials. Each operation retains the
existing isolated temporary home and native auth/limits; Cloud owns selection,
journals, automatic scheduling, History delivery and acknowledgments.

Publish and verify the image through the existing reviewed-source gates. Keep
credentials in 1Password and runtime secret bindings. Use a custom domain for
the same-account Worker call. Cap provisioned instances at one and allow idle
sleep after one hour; subsequent operations reuse the fixed identity, and a
platform restart uses the already-built image.

## Alternatives

- The existing ingest Worker cannot spawn a native executable.
- Creating a sandbox per sync/message repeats provisioning and installation.
- Self-host proof EKS is explicitly outside the hosted production data plane.
- An operator laptop or temporary tunnel does not provide hosted availability.

## Consequences

The hosted adapter needs a small Container allocation and a dedicated secret
shared with Cloud. No new AWS compute, persistent provider transcript store or
second parser is introduced. Container restarts remain possible: ambiguous sends
stay held for reconciliation. Provisioning, native discovery/export and the full
message/reply/automatic-history round trip must be verified after deployment.
