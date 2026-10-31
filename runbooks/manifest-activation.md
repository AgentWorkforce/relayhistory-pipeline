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

**The default requires the claim, and upgrading never turns the window on.**
Every live write needs a `meta.machineId` matching the key's machine and the
writer, always. A status read needs it too, unless the legacy window is open.

The window is read-only and version-only: it returns the object version and
nothing else. No write passes through it, so a courier without a claim cannot
deliver, window or not. It exists so such a courier can reconcile while it is
being replaced. While it is open **this system is not machine-bound**.

Rollout, in order:

1. Inventory the affected couriers: every host whose token has no claim.
2. Enable the window for the affected stage only, with a fixed UTC expiry:
   `MANIFEST_MACHINE_CLAIM=compat` and
   `MANIFEST_MACHINE_CLAIM_COMPAT_UNTIL=<RFC 3339 UTC>`. Both are needed. A bad or
   missing date keeps it shut, and it closes itself at the expiry.
3. Deploy claim-capable couriers. **Until each one is, its delivery is stopped**:
   its records wait in its local source and are not lost, but they do not reach
   the pipeline.
4. Watch claimless use reach zero in `sessions.claimless_uses`, which records each
   read by day, org, caller and client version. A read that cannot be recorded
   is refused.
5. Unset both variables before the expiry. Unsetting either, or setting
   `required`, is the kill switch.

`client_version` is self-reported. It is for this meter only, and must never
influence identity, authorization, limits or trust.

Takeover and `status?manifest=1` always need the claim, so **production rebase
and takeover stay blocked until it ships**. A mismatched claim is refused whether
the window is open or not.

After the expiry, delete `claimlessWindowOpen` and its callers rather than
leaving them shut.

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
