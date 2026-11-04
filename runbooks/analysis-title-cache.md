# Cached brief title metadata

The authenticated canonical analysis GET accepts view=title. It reads only the SHA-bound compact analysis index, verifies the cachedBriefTitle attestation, composite source digest and v2 generation, then rechecks the exact current publication. No result graph is hydrated. A known-null title is returned only when the readonly source query attests it; a missing legacy attestation is unavailable.

The publisher runs the original per-workspace/provider Cloud title query in the same readonly repeatable-read source snapshot. It selects the latest resolved request linked to a completed brief job with exact organization/session, workspace/provider and single-workspace evidence. It does not apply the v1 history window. The shared analysisSourceSnapshotJson combines the unchanged public v1 snapshot digest with title metadata; optional sixth generation input selects v2. Full v1 response bodies and virtual IDs are unchanged. Older v1 indices remain readable by the updated full-envelope reader.

Claims retain their existing SQL/RPC contract. The generation formula is validated after the authenticated index is read because the composite digest is inside that index. Scope, reference/SHA and freshness guards precede graph reads, and the final claim must still match generation, source, index and snapshot fields.

Deployment order is updated reader, updated default-off publisher, title metadata backfill/coverage and freshness verification, then the reviewed RHC/Cloud selection. New v2 publications require the updated reader. Missing/stale publications fail closed while warming. No migration, grant, flag, deployment or production operation is performed by this change.

Current measured candidate titles fit the existing 256KiB index ceiling; future universal-size parity is not claimed. Oversized metadata refuses publication before writes rather than truncating or inferring a null title.
