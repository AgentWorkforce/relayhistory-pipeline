# Hosted Amp transport

Production ingest runs in workerd and cannot launch the native Amp CLI. This
Worker routes the fixed Amp operation to one reusable Cloudflare Container in
the existing Agent Workforce account. It uses the reviewed services image and
overrides only its entrypoint: the existing Amp route starts without ingest,
database credentials or migrations. The CLI is already installed in that image.

Only exact `POST /v1/internal/amp/operation` requests are public. The gateway
checks the dedicated token and bounds the actual request stream before acquiring
the fixed container identity. It never retries a forwarded operation. Native
auth, fixed-command validation, output/time limits, concurrency and uncertain-send
semantics remain in `src/amp`. No session evidence or credentials are persisted
by the gateway. Temporary process homes are cleaned by the native runner.

The application is capped at one `basic` instance (1 GiB). It stays warm for one
hour after activity; the same named instance handles subsequent operations.
Platform restarts/idle stops can cold-start it again, without installing the CLI
or creating a new per-user/per-message sandbox. Cloud's delivery journal handles
uncertain outcomes. The public origin is the custom domain
`https://amp-transport.agentrelay.com`; Workers on the same account must not rely
on a workers.dev-to-workers.dev fetch.

## Release

Khaliq approved the Amp image, transport credential, exact operation ingress and
Cloud rollout in the 2026-11-06 integration session. Preserve the normal source,
CI, image-publish environment, attestation and registry gates in
`../../docs/services-image-release.md`; this approval does not bypass them.

1. Merge the reviewed native service change after required CI. Publish its
   exact approved main source with the guarded image publisher. Verify both
   attestations and the release receipt before copying the image.
2. Copy the verified image to this account's Cloudflare registry. Inspect its
   linux/amd64 manifest and verify the copied digest against that manifest.
   Preserve the immutable source identity; do not rebuild or use a mutable tag
   as evidence of equivalence. Registry credentials stay in memory or pipes.
3. Generate one production transport token in the `96Bulls-prod` 1Password vault.
   Set the same value as this Worker's `AMP_TRANSPORT_TOKEN` and Cloud's linked
   `AmpTransportToken`. Pass values via stdin/environment, never a file or argv.
4. Render the deploy config using the verified Cloudflare digest and approved
   full image source SHA, then deploy the reviewed Worker:

   ```sh
   bun scripts/render-config.ts registry.cloudflare.com/f7232cb80f6fab86a95426302af243e4/96bulls-services@sha256:<verified-digest> <approved-source-sha>
   wrangler deploy --config wrangler.generated.json
   ```

   The generated config is ignored and contains only public deployment metadata.
   The template deliberately has no usable image. The renderer validates shape
   and account, not attestation: the verification in steps 1–2 remains required.
5. Verify unauthenticated/oversized/other-path requests cannot run native work.
   Run read-only discovery/export with the owner's Amp key through the actual
   Cloud client. Then set `AMP_TRANSPORT_ORIGIN` to the custom HTTPS origin and
   deploy Cloud with its linked secret. Run the authorized disposable-thread
   injection, MCP reply, automatic upload and readback test from Cloud's Amp
   runbook. Component tests and a green deployment alone are insufficient.

Rollback stops new transport traffic by clearing Cloud's origin or disabling
the connection. Preserve pending/uncertain delivery journals and History outbox
bytes. Do not replay an ambiguous injection or erase acknowledged evidence.

## Checks

`bun run typecheck` and `bun run test` validate the gateway and boot the actual
static container entrypoint in a local Bun process with no database credentials.
Wrangler's dry run compiles the Worker and validates its resource configuration;
it does not prove container provisioning, live ingress or native CLI access.
