# Persistent Amp transport

Cloud's one-token Amp connection uses this repository's existing AlleyOop ingest
service to run the supported Amp CLI. It does not create a sandbox per sync or
message. The CLI is pinned by the RelayHistory-owned provider module and installed
under `/opt/relay-amp` when the services image is built. Requests reuse that running
container and installed binary. Each operation starts a short-lived process with
an isolated temporary home; no credentials, CLI transcript cache, or account state
are shared between requests.

Cloud owns encrypted provider keys, explicit thread selection, scheduling,
delivery journals, History revisions and acknowledged uploads. This endpoint only
executes the fixed discovery, export and remote-injection command contract owned
by RelayHistory. It does not parse or persist a second copy of session evidence.

## Platform rollout

Deploys and credential changes require Khaliq's separate approval under AGENTS.md.
No live configuration is changed by these instructions or the unit tests.

1. Build and publish the reviewed services image through the normal image workflow.
   The build verifies the pinned source hash, installs the pinned CLI once and
   checks that its native binary runs. Use the reviewed digest for the ingest.
2. Create a dedicated random `AMP_TRANSPORT_TOKEN` of at least 32 characters in
   1Password. Supply it to the ingest through its existing runtime Secret/env
   mechanism and configure the same value as Cloud's `AmpTransportToken` SST
   secret / `AMP_TRANSPORT_TOKEN` deployment secret. Do not reuse an upload JWT,
   `PIPELINE_OPS_SECRET`, or a user's Amp key. Never save secret material to disk.
3. The service's existing HTTPS ingress must forward POST
   `/v1/internal/amp/operation`, allow a 100 KiB request and 17 MiB response,
   and leave at least 60 seconds for a response. The authenticated route is
   separate from the user-upload JWT surface; it refuses calls without its
   dedicated token. Leave it disabled on unrelated/customer deployments.
4. Set Cloud's `AMP_TRANSPORT_ORIGIN` repository/environment variable to that
   ingest's bare HTTPS origin. Deploy full SST once to attach the new linked
   secret; subsequent fast-path deploys preserve the binding and origin.
5. Verify missing/wrong transport tokens are rejected, then use a disposable
   user's Amp token for discovery and export. Keep keys and transcripts out of
   command arguments, logs and artifacts. Cloud users still supply only the Amp
   token and select a thread; history catches up automatically.
6. Follow Cloud's `docs/mcp/amp-agents.md` for a separately authorized injection,
   MCP reply and automatic-history round trip in the same Amp thread.

Unset/placeholder transport credentials return 503 before running a process.
Each service instance accepts at most four concurrent operations and rejects
excess work before execution. Commands time out at 45 seconds; the outer process
group is killed at 50 seconds. Output and request sizes are bounded. Logs never
include keys, operation bodies, stdout or stderr. Each request's temporary home is
removed afterward. An interrupted injection returns `unknown`; Cloud retains it
for reconciliation instead of blindly retrying. Cloud has no sandbox fallback.

## Updating the provider command contract

Edit RelayHistory's `sdk-ts/src/amp-source.ts`, not this generated module. Then:

```sh
node scripts/sync-amp-provider.mjs --source ../relayhistory/sdk-ts/src/amp-source.ts --commit <full-upstream-commit>
node scripts/sync-amp-provider.mjs --check --source ../relayhistory/sdk-ts/src/amp-source.ts
```

CI and image builds check the provenance hash. PR CI builds the Linux services
image and runs the process-isolation/endpoint tests inside it without network
access; it does not publish an image or deploy. No arbitrary command, executable,
filesystem path or environment field is accepted from a transport request.
