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

A controlled backend backfill keeps its cursor in SQL, never trusting a caller-supplied starting cursor; it exhausts every directory row in bounded pages. The publisher claims bounded document batches with a nonce/deadline, sends the actual CH HTTP codec, then CAS-acknowledges exactly the captured epoch/hash/lease. A newer mutation invalidates the claim.

The migration also defines `sessions.session_brief_document_authority`, which reports a scope's
watermark and whether its metadata is `initialized` and `complete` (no dirty outbox row). It is
granted only to `session_directory_search_worker`. A reader built on it could block
uninitialized or dirty scopes and recheck the watermark across discovery and hydration; **the
read API route described under "Runtime wiring" does not**: the read role
(`bulls96_read_api`) holds no grant on that function or on the scope/outbox tables, so the
route does not check currentness and its results are best-effort (below).

Reader candidate discovery is exclusively CH; Supabase only hydrates exact paired
source/session tuples under the existing signed workspace claim policy. No title/summary query
or broad directory candidate scan runs in Supabase. (The opt-in self-host Postgres backend below is
the one exception, and only when its selector is set.) Existing rank fusion, deterministic recency ties, brief snippet priority and frozen-rank cursor semantics remain; a source-version fingerprint invalidates pre-index cursors.

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
  **Results are best-effort.** The route does not read the scope's authority (above): while a
  scope is uninitialized, mid-backfill or has unpublished outbox rows, a session whose current
  directory text matches `q` but whose document ClickHouse does not hold yet is missing from an
  otherwise normal 200, and `truncated` counts only what ClickHouse returned. What it never does
  is serve text the directory no longer holds (every hit is re-checked against the directory).
  A completeness marker would need the read role granted `session_brief_document_authority`
  (a reviewed migration and an owner apply); until then, enable the flag only after the
  backfill and outbox are drained (the activation order below).
  Deploy (Worker): the `summary_search` input of `deploy-session-read-worker.yml` (default off;
  not preserved from live, like the other summary inputs).

Activation order (each a production step that needs Khaliq's explicit OK): apply
`briefDocumentSchemaStatements()` in the ClickHouse database and grant the reader SELECT;
confirm migration `20261105024000` is applied; owner `set_session_brief_documents_live(true)`;
schedule `{"briefDocuments": true}` with `SESSION_BRIEF_DOCUMENTS_PUBLISH=1` until the
backfill and outbox are drained; then deploy the reader flag. While capture is live, the
operator erase runtime (`20261105050020`) refuses with "independent artifact writer requires
a drain protocol"; that drain protocol is not implemented.

## Opt-in Postgres backend (self-host service only)

`READ_API_SUMMARY_SEARCH_BACKEND=postgres`, together with `READ_API_SUMMARY_SEARCH_ENABLED=true`,
serves the same route on the self-host service without ClickHouse or this lane's documents
(`src/session-read-api/postgres/summary-search.ts`). Unset, `""` or `clickhouse` keep the
ClickHouse path above byte for byte; any other value fails startup; the hosted Lambda refuses
`postgres` at startup and the read Worker answers 503 to every request with it. There is no
automatic fallback between the backends. No migration, index or grant is added.

- Candidates come from the existing directory query function (migration `20261105046010`) as
  the read role: `q` is normalised like a directory search word (3 visible characters to 200
  code points, else 400) and sent as one phrase term, a substring match on the tenant's live
  rows' `search_text`. Erased or tombstoned sessions are never returned.
- `search_text` also holds task title, agent, project, repo, branch and model, and can be stale,
  so every row is re-checked with the same `summaryMatches` as the ClickHouse path. Only rows
  whose current title or short summary contains `q` are served; the re-check can drop rows,
  never add them.
- Bounded and best-effort: keyset pages of 200 rows, newest activity first, at most 3 pages
  (600 rows) per request, stopping once `limit + 1` matches are found. When many recent rows
  match `q` only through another field, the page can hold fewer than `limit` sessions, even
  none, with `truncated: true`; older matches past the scan are missed.
- Same response shape, attestation, canary, buffered 501 and tenant check as the ClickHouse
  path; the page is ordered last activity DESC, source DESC, session_id DESC. Which rows fill
  the page at the truncation edge can differ from ClickHouse on identical activity timestamps.
  No cursor; no ranking or snippets.
- Fresher than the ClickHouse path: the read is the directory itself, with no outbox lag.
