# Incremental search watermark store (M3 proposal)

Code-only proposal. These files are deliberately outside `supabase/migrations`
and have no migration slot, checksum, generation, deployment, or production
effect.

The store records the complete live-manifest watermark set for one canonical
search scope. Its scope version is part of every compare-and-swap operation;
comparing only the manifest rows would admit an ABA race (`A -> B -> A`). The
CAS replaces the complete set in one transaction, so a caller can never commit
one manifest's watermark while leaving a sibling behind.

The two RPCs are the intended runtime boundary:

- `sessions.read_search_batch_watermarks_v1` returns the current version and
  canonical, manifest-key-sorted set.
- `sessions.cas_search_batch_watermarks_v1` locks the scope version, compares
  both the version and exact expected set, then atomically replaces the set.

The tables and canonicalization helper are owner-only. The proposal grants
only RPC execution to `service_role`; the repository's reviewed boundary
generator is responsible for any hosted-runtime mirror when this is promoted.
Promotion, applying the migration, grants, deploying an image, and enabling
`SEARCH_INDEX_INCREMENTAL_ENABLED` each remain separate production actions.

