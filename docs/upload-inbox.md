# Database-independent upload acceptance — implementation in progress

User requirement: an authorized desktop must upload successfully when both Neon and Supabase are unavailable. Database projection and publication must catch up asynchronously. Existing clients and privacy controls must retain their guarantees.

Current deployed code does not meet this requirement. Relayhistory resolves opaque desktop access tokens through an SQL auth store, reads Neon legacy receipts and custody/control state, and forwards to ingest. Ingest claims and prepares in Supabase, writes S3, then commits a receipt/outbox in Supabase. Buffered acceptance has already deferred publication; merely deferring indexing again does not remove admission or receipt SQL.

`src/session-ingest/inbox-object.ts` is the first dormant storage component. A create-only object contains both the exact receipt and scrubbed batch bytes. The key binds tenant/user, origin and batch identity; a conflicting replay cannot overwrite it. The complete deterministic bytes are compared when an object exists, rather than trusting metadata. A lost PUT response is an error; retry reconciles the stored object. Receipt and content cannot become separated by a later SQL commit. The primitive has no SQL or Queue dependency, is not imported by any production handler, and supplies neither authentication nor privacy authorization.

Remaining work before a handler may acknowledge through it:

1. Choose and implement authoritative non-SQL upload authentication for existing opaque desktop credentials as well as signed future capabilities. An isolate cache with an SQL miss path does not satisfy outage independence. No plaintext bearer may be persisted in S3.
2. Implement the non-SQL consent/ownership/deletion/reset authority and late-write erasure barriers. An S3 object is not permission to bypass a pending or revoked control. Establish the old-state baseline and ordering; do not assume the incomplete frozen copy is sealed.
3. Compose validated/scrubbed batch preparation, the authorized control version and the immutable receipt into the real HTTP route. Preserve tenant/account checks and replay/conflict semantics. Do not reuse a general buffered object as canonical publication input.
4. Add asynchronous consumer and S3 reconciliation so lost/expired queue notifications cannot strand an acknowledged upload. Keep pending receipts and bytes until completion or verified erasure; do not add a blind lifecycle expiry.
5. Make desktop success mean durable upload acceptance, while search/publication progress remains separately observable. Verify released desktop parsing of the receipt.
6. Test the actual public handler and cold-cache authentication with both databases unreachable and SQL construction forbidden. Include S3 failure, uncertain PUT, concurrent retry, conflicting content, revoked access, reset/delete races and delayed writes; recover asynchronous processing exactly once logically after SQL returns.

Storage-component tests use the real filesystem ObjectStore adapter with synthetic bytes. They prove atomic persisted framing and negative/retry/concurrency behavior of this component, not AWS persistence, HTTP/desktop compatibility, auth independence or privacy erasure. Production routing, cloud resources, credentials, IAM and migration state are unchanged.

The 150 GB disk resize has succeeded and accepted live uploads were verified. It does not establish database-independent uploads.

A second dormant component, `src/session-ingest/inbox-snapshot.ts`, stores immutable metadata pages addressed by their actual SHA-256. Cold lookup verifies page bytes and structure; missing or corrupt pages refuse. Old roots remain readable. Input and returned values cannot mutate cached state. Inputs are bounded to 2,048 changes, 512 KiB total metadata, and 8 KiB/512 nodes per value; page bodies are at most 128 KiB. One public operation runs at a time per instance. Deadlines bound waiting, including stalled body reads. An immutable PUT may finish after a deadline refusal, leaving an orphan page; the failed operation never returns a root to adopt.

`maxOperations` and byte counters cover logical ObjectStore work. They do **not** count the adapter's hidden HTTP retries/HEAD/GET reconciliation, or establish an exact heap bound. The actual transport must separately enforce outgoing HTTP, concurrency, byte and abort budgets before production integration. Cache eviction is bounded by serialized page weights and eight pages. This component has no committed-head authority, privacy semantics or production imports.

The proposed authority must arbitrate batch identity `(org, workspace, origin, batch)` and native record identity `(org, workspace, origin, record)` across users. A per-user object key is only a physical candidate location, not that authority. A stored candidate receipt does not establish committed membership. Baseline completeness, pre-PUT inventory, a bounded conditional head/journal, privacy barriers and exact replay proofs remain required.

SQL recovery also needs a metadata-only historical decision projector or equivalent downstream changes. Re-running current-state claim/commit RPCs cannot recover an older acknowledged upload whose payload was legitimately erased before SQL returned. Seat allocation, lineage and publication ordering must survive that case without recreating erased bodies. No new migration or production scope reduction has been approved by this design.
