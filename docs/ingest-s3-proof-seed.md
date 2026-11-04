# Offline S3-proven replay bootstrap

This source is dormant. It replaces exporter-state/session-boolean seeding. Both retired bulk RPCs are revoked and their bodies refuse even an accidental future grant. The old CLI refuses EXPORT_STATE_FILE.

The producer reads actual current Supabase pointers, exact manifest identity/owner/revision, content-addressed chunk chain, compressed/raw hashes, byte/record bounds and every delivery record. It reuses the authoritative per-origin revision fold, including tombstones, acceptance conflicts and equal-revision content conflicts. SQLite stores IDs, revision seats and hashes, never transcript payloads. Historical S3 has no original revision IDs/digests; proven bounds explicitly have null originals and proof columns. Real incoming receipt IDs remain exact. Retired/direct native kinds are counted separately, never invented as delivery records.

## Ordering and fences

1. Establish a reviewed complete legacy source frontier independently. This tool does not establish it.
2. Before the user's first destination operation, import actual deletion/reset metadata from a readonly repeatable-read Neon snapshot. Pointer users without fences need an explicit empty baseline. Identical baseline replay is a no-op; changed packets or prior destination operations refuse.
3. A server-owned15-minute capture binds exact migration PASS generation, all live pointers/owners/revisions/raw hashes, persistent source epoch, per-session disable epoch and append-only reset count. It uses existing per-user locks and never truncates to64 pointers.
4. Verify S3 bytes into an on-disk index, then recapture the complete pointer image. Commit metadata under the same locked authority. Final wall-clock expiry after blocked writes rolls back the entire batch.
5. Canonical admission, disable, reset or enable permanently closes the historical baseline. Fresh capture after enable refuses. Trusted buffered admission (040500 publication_state) and buffered receipt updates preserve the baseline. Missing publication_state defaults conservatively to canonical. Every canonical batch INSERT/UPDATE closes the baseline before canonical publication; disable/reset/enable still close it. Baseline import must precede even buffered admission.
6. Resume re-reads every S3 byte. Only locally acknowledged identical metadata batches are skipped; unacknowledged replay is idempotent. Never clear destination bounds to force progress.

Cross-user/source/session/kind record-key moves refuse conservatively. Owner identity must be proven; the historical live writer exception requires EVERY origin to cryptographically derive its physical machine from captured user/workspace. Non-delivery manifests are reported separately. These are explicit coverage gates.

## Entry and prerequisites

scripts/session-ingest-seed.ts ORG defaults to read-only preview. --execute imports actual fence metadata and writes proven replay bounds. Clean exact checkout and SEED_EXPECT_GENERATION are required. Credentials arrive only through inherited /dev/fd/N or /proc/self/fd/N descriptors. SEED_PROOF_DIR is private metadata-only durable storage.

Destination URL pins existing project/pooler/TLS CA/hostname and relayhistory_owner, then explicitly SET ROLE to seeder. No postgres/admin fallback. Production operator ownership may require a separately reviewed adapter; migrator privilege does not authorize seeding. Neon uses the existing non-elevated readonly role and SELECT only on actual deletion/reset tables. S3 credentials need raw GetObject and any KMS decrypt; the facade refuses put/delete/presign. No IAM/grant/resource change occurs here.

040400 depends on040300. The unlanded040220 seed-only grant removal is intentional: the final boundary must not grant retired bulk seed during migration. Run scripts/sync-ingest-seed-boundary.ts after final union composition. Full migrator and all three phase-2 verifiers precede PASS. Seeder has exactly four RPCs and no effective table/column/PUBLIC/MAINTAIN/sequence privilege.

## Capacity and remaining gates

Bounded state: one chunk per active cohort,64MiB SQLite cache,32GiB SQLite per cohort, one1000-record packet,16MiB metadata envelope,1000-pointer union with explicit sentinel refusal,100000 discovery users. CLI concurrency1..4; helper default1. SQL connections/statements/locks have owned timeouts. Resource refusal never silently omits records.

Readonly production metadata at2026-11-04T05:08:17Z counted130386 cohorts,136787 pointers,301453 chunks,133953904905 raw bytes across313orgs. This is cost metadata, not delivery eligibility/completeness. Full verification needs roughly one manifest GET per pointer, one chunk GET per chunk, scope capture/recheck RPCs and1000-record commits. Largest org numeric total26269849263B. Per-org rollout can avoid global124.75GiB verification before initial scoped relief. Synthetic throughput is reported separately, not a cloud ETA.

S3 maxima do not prove the latest Neon-only record/tombstone frontier. Keep canonical fanout paused until that frontier, generation-safe S3 publication/erasure, final union verification and operator gates are satisfied. A buffered receipt means durable acceptance with later visibility, never permission for canonical publication. This implementation performs no production seed/migration/flag/deploy/write.
