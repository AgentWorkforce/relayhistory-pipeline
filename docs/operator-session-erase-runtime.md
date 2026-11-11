# Operator erase runtime (source only, default off)

Khaliq ordered implementation of per-session operator hard-delete on 2026-11-11,
relayed by infra-chief. This supersedes the implementation hold for the per-session
contract, not the historical per-user reset proposal. Production deploys, migration
applications, credentials, IAM and flag activation still need his separate explicit OK.

Migration 050020 follows 050000/050010. It is inert until its narrow entry points are
called. The ingest role gets no direct runtime-table or search-table privileges.
Legacy erase/step helpers are owner-only from creation; the ingest login mutates
state only through admission/lease-gated runtime RPCs. A separate read-only, tenant-bound RPC exposes the permanent fence to the read API;
the consumer-only upsert installs canonical identity atomically with the fence check.
The usual ledger checks, sentinels, generated ACL and guarded rollback apply. A used
runtime cannot be rolled back: durable write obligations and receipts must survive.

The supported initial runtime is synchronous canonical self-host ingest with versioned
S3/Locker and Postgres or OpenSearch search. Direct desktop upload (`HISTORY_UPLOAD_DIRECT`), ordered, lazy, buffered, history-managed,
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
removed. Output facts use scope_source, not their event's source. Mixed digest cache
rows and an enabled independent digest writer refuse erase before fencing, preserving
other sessions' narratives. That pipeline needs its own generation-aware drain protocol.
Operational queue IDs, permanent fences and content-free audit receipts remain;
transcript payloads and object addresses are not retained there.


Causal coverage includes both actual PostgreSQL lock orders, durable writers, expired
leases, source/bucket/layout refusals, alias registration, role/tenant isolation,
physical purge, late metadata suppression, output-fact scope and digest isolation.
The composition below drives these primitives; the end-to-end proof covers its completed receipt and read behavior.

## Automatic self-host composition

The self-host ingest service binds the authenticated route to the durable SQL ledger.
With byte deletion enabled it scans the durable queue on startup and every second;
restarts resume outstanding receipts. A failed provider step returns a static retryable
failure receipt. Repeat Control DELETE to retry; GET only observes. A live writer keeps the receipt pending. Mixed staged payloads produce a bounded
`stagedObjects` failure with code `staged_objects_require_drain` on the first scan;
failed receipts are not polled again until a repeat DELETE. The worker never substitutes a timeout
for proof that a writer stopped, or a delete request for proof that all versions are gone.

All settings remain unset by default. Any deployment or activation below needs Khaliq's
separate approval; these are configuration requirements, not instructions to execute now.

| Setting | Requirement |
| --- | --- |
| SESSION_ERASE_FENCE_ENFORCED=1 | Consistent on every ingest, pipeline consumer and service read API replica before enabling the operator API. Ingest refuses async or drain admission. |
| SESSION_ERASE_API_ENABLED=1 | Requires enforcement and a dedicated SESSION_ERASE_API_SECRET (32+ non-whitespace characters). Control uses its existing sessionErasureApi URL/secret settings; internal ingress stays private. |
| SESSION_ERASE_BYTES_ENABLED=1 | Automatic versioned byte and search deletion. API can remain off while enforcing existing fences. |
| SESSION_ERASE_S3_BUCKET / REGION / ENDPOINT / KEY_ROOT | Must exactly match LOCKER_BUCKET / REGION / ENDPOINT and INGEST_KEY_ROOT. Root defaults to 96bulls. Only S3-compatible versioned storage is supported. |
| SESSION_ERASE_S3_ACCESS_KEY_ID / SECRET_ACCESS_KEY | Dedicated eraser credentials injected from the secret system. Existing ListBucketVersions + DeleteObjectVersion permissions; no transcript GET/PUT. Writer credentials are never a fallback. |
| SESSION_ERASE_SEARCH_POSTGRES_URL | Dedicated bulls96_session_erase_search login on the same host, port and database as ingest metadata. Explicit atomic source bootstrap supabase/bootstrap/session_erase_search_role.sql requires a direct superuser operator and grants DELETE plus key-column SELECT only under forced tenant RLS. LOGIN/password provisioning is separate. |
| SEARCH_INDEX_BACKEND=opensearch | Uses existing SEARCH_INDEX_OPENSEARCH_URL and INDEX_PREFIX plus dedicated SESSION_ERASE_OPENSEARCH_USERNAME/PASSWORD. The existing synchronous adapter deletes and verifies tenant/source/session documents. Basic-auth self-host OpenSearch is supported; no writer credential fallback. |

Search backend and index prefix must stay consistent across consumers/readers/eraser;
changing backend requires draining and erasing the old projection as a separate approved
operation. Database identity is pinned in the Postgres composition, and object-store
and search-target identity is persisted with the job so changed storage, backend or index
prefix cannot falsely complete an old job. Automatic deletion requires an ingest pool
of at least two connections: one holds the consumer search lock while another runs
lease and purge RPCs. Startup refuses a smaller pool.

The object eraser checks current pointers, historical dates, unindexed machines, upload
generations, every version and delete marker. IDs that cannot map unambiguously to their
stored layout are refused; a neighbor with a shared string prefix is never an erase target.
Mixed live-batch, buffered-batch, async inbox and direct-upload inbox prefixes are independently checked for
absence. Unknown sessions receive permanent fences and a verified zero-count receipt.

Search deletion holds the same metadata advisory locks as the pipeline consumer and
verifies the external projection before metadata purge. The optional Postgres search role
has no transcript-content SELECT, INSERT or UPDATE. Postgres projection state is purged
in the same scoped cleanup. Exact session read routes check the permanent fence both
before and after I/O; collection list/search endpoints return successful empty collections.

## Proof

CI runs src/session-erase/__tests__/operator-flow.minio.test.ts with the existing
FRONTIER_TEST_S3_* disposable loopback fixture. It applies actual upstream and HoopDb
migrations, uploads through real delivery, reads native directory/search/summary routes,
erases all stores, verifies the completed receipt, checks 404 exact reads and missing raw
objects, preserves a neighboring object and suppresses same/new-origin re-upload.
Source projections are seeded as the consumer would; separate real PostgreSQL tests
prove consumer identity registration and both writer/fence lock orders.

For the optional actual Control HTTP proof, set OPERATOR_ERASE_CONTROL_CHECKOUT to an
installed relay-control source checkout when running that same test. The test imports
its existing PGlite/HTTP harness, creates only synthetic data, checks member refusal,
owner DELETE -> pipeline -> complete GET /erasure, exactly one completion audit and
unchanged memberships. This cross-repo proof passed locally at Control b503eaa; CI's
pipeline-only proof remains self-contained. No real database or store is used by either.

Per-user bulk hard-delete is not included. It requires a stable all-session inventory
across users/origins and bounded durable expansion into these per-session receipts.

A later canonical ID that encodes to an erased physical object prefix is permanently
refused with HTTP 409 `operator_session_erased` before object I/O. The object layout
cannot safely distinguish these identities. Reusing that prefix could let an old
worker delete the new session. This reservation persists with the erase fence.

Direct desktop upload must remain off on all writers; existing desktop upload grants
and outstanding witnesses require a separately approved revoke/drain protocol before
switching a previously direct-upload deployment to this initial erase runtime.

## Failed writes and operator recovery

Confirmed writes remove their obligations after all object work and ledger commit
succeed, so successful traffic does not accumulate rows. Exceptions persist `failed_at`
without releasing the obligation; even a lost-response write can still reach storage.
A crash can leave `failed_at` unset. Neither age nor a failure timestamp proves stopped.
The index `(org_id, workspace_id, writer_id)` supports exact writer recovery.

An operator can inspect outstanding work as the migration owner, scoped to the tenant:

```sql
SELECT writer_id, min(started_at) AS started_at, max(failed_at) AS failed_at, count(*) AS session_keys
FROM sessions.session_erasure_writes
WHERE org_id = $1 AND workspace_id = $2
GROUP BY writer_id ORDER BY min(started_at) LIMIT 100;
```

The executable parameterized statement is
`supabase/operations/operator_session_erase_recover_writer.sql`. Bind organization,
workspace, writer UUID and a boolean attesting that EVERY process for this writer
has stopped, all in-flight storage requests settled and its writes were reconciled.
The entire batch shares one writer UUID, so this proof covers every session in it.
Only then pass true for the fourth parameter. False/NULL releases nothing; another
tenant is untouched. Service roles cannot execute the statement because they lack
direct table privileges. Applying it to a real database still needs Khaliq's explicit OK.

Recovery removes only that writer's obligations and appends `writer_recovered` /
`writer_stopped_verified` audit events to affected erasure receipts. Repeating the
statement is harmless. It does not assert byte absence: the normal worker must still
verify/delete every object and search document before completing a receipt. Failed
or crashed obligations intentionally remain until this reconciliation; they are never
silently garbage-collected. SQL and privilege tests execute this exact statement.

Mixed staged namespaces contain payloads for multiple sessions. A leftover version or
delete marker therefore refuses completion instead of being deleted for one session.
The refusal is terminal for that attempt, not an endless in-progress loop. Drain the
upstream namespace under its separately approved protocol, then repeat Control DELETE.
The versioned MinIO proof covers an old upload payload plus a delete marker, explicit
failure/status, no byte deletion, no repeated scan, and successful retry after fixture drain.
