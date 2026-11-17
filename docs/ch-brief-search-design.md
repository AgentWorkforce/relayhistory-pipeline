# Dormant ClickHouse brief/title/summary search

Composed migration: `20261105024000_session_brief_documents.sql`; the original
`030900` reservation ordered it before its directory-event column dependency,
so canonical-stack composition moved it into the post-`050100` window. This lane is dormant
and performs no production schema, credential, IAM or deployment actions.

Effective directory metadata is authoritative: the existing session-brief stage projects scrubbed model title/description into `session_directory.title` and `short_summary`; pointer/projector updates and clear/erasure paths also converge there. A separate derived ClickHouse `session_brief_documents` table avoids synthetic transcript rows that the authoritative S3 reconcile would erase.

Applying the migration creates no scope or outbox rows. Its directory trigger
first reads a singleton control row that defaults to `live=false`, then returns
before constructing documents or taking a scope lock. The owner-only
`sessions.set_session_brief_documents_live(boolean)` RPC takes a bounded
directory-write fence, seeds the existing scope keys, and flips the control row
in the same transaction. No runtime role can call it.

Once explicitly enabled, every relevant directory change creates a per-canonical-identity document epoch and dirty outbox record, including a blank-text tombstone for deletion or the old identity on alias/source/workspace changes. Per-tenant watermark locking precedes epoch allocation, so metadata commit order cannot leave an unchanged high-water mark across a late older transaction. Immutable CH ordering is org/workspace/source/session; mutable text/project/activity are applied only after newest-version collapse. Higher-epoch tombstones prevent late older writes from resurrecting data.

A controlled backend backfill keeps its cursor in SQL, never trusting a caller-supplied starting cursor; it exhausts every directory row in bounded pages. The publisher claims bounded document batches with a nonce/deadline, sends the actual CH HTTP codec, then CAS-acknowledges exactly the captured epoch/hash/lease. A newer mutation invalidates the claim. Uninitialized/dirty metadata blocks the brief pool, including scopes that would otherwise return zero candidates.

Reader candidate discovery is exclusively CH. Supabase RPCs only check aggregate metadata currentness and hydrate exact paired source/session tuples under existing signed workspace claim policy. A tenant watermark recheck rejects changes during discovery/hydration. No title/summary query or broad directory candidate scan runs in Supabase. Existing rank fusion, deterministic recency ties, brief snippet priority and frozen-rank cursor semantics remain; a source-version fingerprint invalidates pre-index cursors.

Only the existing keyword pools are claimed. No embedding/vector pool is implemented or enabled here. Existing typed unsupported organization/project/event-window/kind/task/tag/agent filters remain unsupported until their semantics have separate proof.

Activation requires reviewed Supabase migrations/ACLs, CH DDL and reader/publisher table grants, bounded backfill + zero dirty coverage, runtime tick wiring, immutable build/deploy evidence and all migration gates. Default configuration remains dormant; no Neon fallback is introduced for selected CH search.

## Runtime wiring (default off)

Two default-off switches now drive this lane; neither creates a resource, grant or schedule.

- Publisher: the session pipeline Lambda invocation `{"briefDocuments": true}` runs
  `publishBriefDocumentsTick` (bounded rounds of `publishBriefDocumentsOnce`, 20 rounds or
  40 s) only when `SESSION_BRIEF_DOCUMENTS_PUBLISH=1`, checked before any secret, pool or
  ClickHouse access. It uses the consumer's Supabase pool and its ClickHouse search binding
  (`SEARCH_INDEX_ENABLED=1`). Until the owner calls
  `sessions.set_session_brief_documents_live(true)` the backfill RPC refuses and the run
  reports `ok=false`, publishing nothing. No EventBridge rule invokes it yet.
- Reader: `READ_API_SUMMARY_SEARCH_ENABLED=true` (exactly) serves
  `GET /v1/sessions/summaries/search?workspace=&q=&limit=` on the Worker and the hosted
  Lambda (`src/session-read-api/clickhouse/summary-search-query.ts`). ClickHouse finds
  candidate keys whose newest document is live and whose folded title or summary contains
  `q`; every candidate is hydrated through the existing exact-key directory list and
  re-checked against the directory's current title/short_summary before it is served, so a
  clear or erasure that ClickHouse has not received yet is never served. Same workspace
  attestation and directory canary as the directory routes; no cursor (`truncated`).
  The read-only ClickHouse user needs SELECT on `session_brief_documents`.

Activation order (each a production step that needs Khaliq's explicit OK): apply
`briefDocumentSchemaStatements()` in the ClickHouse database and grant the reader SELECT;
confirm migration `20261105024000` is applied; owner `set_session_brief_documents_live(true)`;
schedule `{"briefDocuments": true}` with `SESSION_BRIEF_DOCUMENTS_PUBLISH=1` until the
backfill and outbox are drained; then deploy the reader flag. While capture is live, the
operator erase runtime (`20261105050020`) refuses with "independent artifact writer requires
a drain protocol"; that drain protocol is not implemented.
