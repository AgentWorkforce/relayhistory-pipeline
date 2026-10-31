# Opening the manifest epoch gate

`backfill` and `adopted` epochs are refused with `activation_required` until
every reader in `MANIFEST_READERS` has a valid attestation. Nothing here happens
automatically. Each step needs Khaliq's explicit OK.

1. Create the `MANIFEST_READINESS_KEY` credential in 1Password. It signs reader
   attestations; never put it on disk.
2. Apply `supabase/migrations/20261031000100_manifest_readiness.sql`.
3. Build each reader with `--define __RELEASE_SHA__='"<commit>"'`. A build without
   it reports `unreleased` and cannot attest. The value is a build-time constant
   so a running process cannot claim another build.
4. Deploy. Each reader attests at startup after parsing the fixture manifests.
   An attestation lapses after 7 days, so readers must keep restarting or
   re-attesting, and a rollback stops counting when its row lapses.
5. Confirm with `select reader, release_sha, attested_at from
   sessions.manifest_readiness;`. Every reader must appear.

## Client releases

`KNOWN_CLIENT_RELEASES` in `packages/object-store/src/manifest.ts` is the
allowlist. `writer.plugin_version` is still supplied by the client, so this
stops mistakes, not a hostile client, until the machine claim carries release
identity.

## Machine claim

`MANIFEST_MACHINE_CLAIM` is `compat` by default: a live write with no
`meta.machineId` is allowed. Set it to `required` once the issuer ships the
claim. A mismatched claim is refused in both modes. A takeover and
`status?manifest=1` need the claim in both modes, so **production rebase and
takeover stay blocked until the claim ships**.

The courier gets a claim only through Cloud: set `CLOUD_URL`,
`CLOUD_TOKEN_COMMAND_JSON` and `MACHINE_ID` (its claimed fleet node id). The
self-minted `RELAYAUTH_URL` path signs no machine, so it stays on `compat`. A
state file filed under an older invented id (`m-<hex>`) stops the courier with
`machine_id_mismatch`, because its keys cannot match the claim; that host needs
its machine id migrated or its state started fresh, which is an operator
decision.

## Retries

A refused backfill is retried by the `backfill-activation` stage:
`BACKFILL_ACTIVATION_MAX_ATTEMPTS` (default 12) and
`BACKFILL_ACTIVATION_MAX_DELAY_SECONDS` (default 3600), then the DLQ. When the
gate opens, the item lands in `sessions.backfill_activation_ready` for the
exporter.

## Export completion

`assertExportComplete` refuses authoritative coverage while a session is filed
under both a real machine and `unknown-origin-*`, or
`sessions.export_deferred_sessions` holds an open row. The exporter records a
deferred session with `recordExportDeferred` and clears it with
`resolveExportDeferred` once its records land. A straddling session stays
deferred until plan-entry v2 can sign its backfill.
