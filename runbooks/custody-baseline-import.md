# Metadata custody import (040540)

This is the full metadata-import path. It records current legacy custody without
writing the S3-proven canonical revision ledger and without assigning publication
seats to old source rows. Initial uploads use the separately reviewed lazy-current
witness policy; they do not wait for a61M-row metadata import.

An owner first initializes040530 with the exact completed source descriptor hash.
The source descriptor declares the full ordered org inventory, including explicit
empty orgs, source identity/control epoch, real allocator floor, and per-org
count/page/hash-chain footer. The source computes page hashes from PostgreSQL
JSONB text. No JavaScript JSONB serialization is assumed.

The private owner-only RPCs are:
- ingest_custody_begin({generation,orgId,descriptorText})
- ingest_custody_page({generation,importToken,payload,pageSha256})
- ingest_custody_complete({generation,importToken})
- ingest_custody_release({generation,descriptorText})

Page ordinals start0. The chain starts64 zero hex digits and advances as SHA256 of
previousHex + newline + pageSha. Background pages have a10000-row ceiling and a
fixed16MiB JSONB UTF8 byte limit. This changes neither the500-record upload limit
nor any public/Worker body cap. The source exporter must halve its page size on
oversize without advancing its cursor. Single-row overflow fails closed.

The target validates hashes, exact metadata fields, duplicate source identity,
counts, page replay/gaps, source/epoch/proof/generation, and the original per-user
privacy baselines. It inserts metadata and custody heads in sets. Completion
locks user scopes and rechecks privacy/reset epochs. Incomplete imports cannot
admit. Global release requires all exact inventory orgs complete and admitted at
current generation; a selected-only inventory cannot release.

Legacy custody is an immutable private metadata row. A losing new candidate
receives its own real target ACK-order reference with bodyAccepted=false; its map
references the original legacy metadata/digest. After erasing the current legacy
scope, a fresh lower revision can start new custody. Earlier ignored candidates
remain ignored. Existing S3 revision state is untouched.

This path currently requires a pre-activity import. It must not be used to
overwrite heads in an active lazy-witness deployment; online background head
initialization requires the same source-control-version CAS and sticky first-touch
rules as the lazy witness path. Source and actual throughput/integration evidence
remain separate gates; PGlite tests are not a production capacity measurement.
