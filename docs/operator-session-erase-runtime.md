# Operator erase runtime (source only, default off)

Khaliq ordered implementation of per-session operator hard-delete on 2026-11-11,
relayed by infra-chief. This supersedes the implementation hold for the per-session
contract, not the historical per-user reset proposal. Production deploys, migration
applications, credentials, IAM and flag activation still need his separate explicit OK.

Migration 050020 follows 050000/050010. It is inert until its narrow entry points are
called. The ingest role gets no direct runtime-table or search-table privileges.
A separate read-only, tenant-bound RPC exposes the permanent fence to the read API;
the consumer-only upsert installs canonical identity atomically with the fence check.
The usual ledger checks, sentinels, generated ACL and guarded rollback apply. A used
runtime cannot be rolled back: durable write obligations and receipts must survive.

The supported initial runtime is synchronous canonical self-host ingest with versioned
S3/Locker and Postgres or OpenSearch search. Ordered, lazy, buffered, history-managed,
independently published analysis, active large reads and live brief-document pipelines
are refused before fencing until their writer-drain protocols exist. Source collisions
in older source-free analysis tables and ambiguous/lossy object identifiers also refuse.
No unsupported mode can report a completed hard-delete.

Before a writer starts any object I/O it registers a persistent obligation under the
same ordered advisory locks as erase admission. Only confirmed object writes and ledger
commit close it. An erase fence refuses subsequent registration and index upserts;
previous writers must finish before any deletion. There is deliberately no age-based
expiry. An interrupted or ambiguous write can block completion indefinitely. Recovery
must establish that the writer is stopped and reconcile its object writes before an
operator closes the obligation; this release does not automate that operator action.

Leased workers must reauthorize before external effects and every receipt update.
Metadata purge verifies absence and records counts atomically. Aliases remain in
content-free erase scope records after pointers are physically deleted. Relationships,
extensions, summaries, briefs, publications, facts, ownership and revision rows are
removed. Output facts use scope_source, not their event's source. A mixed daily digest
cannot be selectively redacted, so the workspace cache is evicted. Cache writes in a
workspace with an operator fence are disabled until a future generation-aware cache
protocol can prove that a late generator did not read erased content. Other workspaces
remain unaffected. Operational queue IDs, permanent fences and content-free audit
receipts remain; transcript payloads and object addresses are not retained there.

Causal coverage includes both actual PostgreSQL lock orders, durable writers, expired
leases, source/bucket/layout refusals, alias registration, role/tenant isolation,
physical purge, late metadata suppression, output-fact scope and digest isolation.
Runtime composition and the end-to-end object-store proof ship in the following lane.
