# Durable buffered-object redaction SQL

This is a dormant schema prerequisite.040500 owns stage admission/receipt/outbox and040400 owns historical S3 proof.040510 supplies metadata leases and atomic pointer replacement; The matching Node runtime is documented in buffered-gc-runtime.md. No production SQL, grant, flag, routing or deployment is performed here.

The four ingest-role RPCs use one JSON contract:

| RPC | Request | Result |
| --- | --- | --- |
| ingest_buffer_gc_claim | limit1..100 plus exact compiled generation | tickets with stageToken,claimToken,leaseToken,rowVersion decimalstring,key,sha256,bytes,groupOrdinals,keepGroupIndexes,receiptState,phase,resetEpoch decimalstring,cleanupState and optional replacement |
| ingest_buffer_gc_prepare | stageToken,leaseToken,key,sha256,bytes,groupOrdinals | replacementStageToken plus registered object metadata |
| ingest_buffer_gc_swap | stageToken,leaseToken,replacementStageToken ornull | swapped boolean |
| ingest_buffer_gc_finish | stageToken,leaseToken,barrierVersionId | finished boolean |

User fence precedes batch, intent and outbox locks. Candidate discovery takes no stage lock before waiting for the user fence. A nonce-bound five-minute lease owns an exact row version and captured outbox version. Healthy rows rotate through durable checked-at timestamps so an unchanged first page cannot starve later erasures. Every final write checks wall-clock expiry and rolls back on expiration after blocking.

Original immutable claim metadata determines the retained groups. The independent original reset count invalidates no-session records too. Duplicate, missing or cross-session ordinals refuse. Legal all-no-session groups may contain multiple sources, matching the authoritative writer; reset still invalidates the complete group. Acknowledged mixed stages keep every surviving group in its original order. The child retains the same claim/reset/session epochs; it never adopts current epochs. A fence change during copying refuses prepare/swap and leaves both objects tracked for retry.

Prepare registers the child and prospective parent reference BEFORE any S3 PUT. Keys retain the exact admitted tenant prefix and batch/content/claim hash grammar. A current acknowledged live parent owns its prospective child independently of lease expiry. Repeated crashes cannot retire/barrier the deterministic replacement key; skipped children advance checked-at so the parent remains discoverable. Parent GC releases the child when its original keep set shrinks or the parent swaps/retires. Swap CASes the exact active outbox pointer/version and ACKs the child in the same transaction that permanently retires the old stage. With no retained groups it keeps an erased outbox receipt. A failed swap never authorizes deleting the old current object.

An expired unacknowledged intent can be durably retired without ACK. The producer must reject retired/parent/expired intents before ACK (040500). Retired cleanup tickets do not require the old raw object to exist. Retirement is irreversible through these RPCs. Finish accepts only a non-null durable S3 barrier version for an unreferenced retired key. It records metadata after the runtime verifies the barrier and permanently removes all other versions. SQL cannot prove S3 behavior; it relies on the explicitly trusted ingest background service to perform those operations.

Overlapping cleanup workers must converge on the same conditional persistent barrier; neither may replace/delete the other's protected barrier. Late If-None-Match PUT must fail against that barrier. A SQL lease alone cannot stop in-flight S3 IO after expiry. Root's version-aware object-store/eraser controls and full service proof remain necessary.

The generated ACL grants exactly the four GC RPCs, no private helpers/tables/columns. The pure ingest phase2 derives ALL ingest RPC OIDs from the combined manifest and checks effective PUBLIC/MAINTAIN/column/sequence privileges. It accepts bare NOLOGIN with no settings or approved LOGIN with exactly statement_timeout5s/lock_timeout2s/idle_in_transaction_session_timeout10s; the operator wrapper still requires LOGIN. It mutates no role. All four verifiers run in migrator, live gate and external PASS and participate in generation hashing. Regenerate seed+GC boundary artifacts from the exact final combined manifest.

Evidence: seven actual PGlite SQL controls, real owned PostgreSQL17 lock/expiry/rollback controls, targeted mutations, typechecks and full migrator composition are recorded in the handoff. Synthetic tests are not production privacy approval. Org admission remains empty/defaultoff until operator-attested privacy and retention baselines, reviewed resource permissions, complete late-write handling, independent source review and real upload/read/erasure E2E gates are met.

040600's separate publisher ledger may share the existing intent gc_lease_token/until/version exclusion under the same lock order. Publisher claims set gc_keep_groups=NULL, so GC prepare/swap reject that nonce as a plan. GC skips any valid publisher lease; after expiry it may issue a fresh nonce for an actual cleanup plan. Publisher final CAS and heartbeat must check its own ledger plus the exact exclusion nonce/version/deadline, outbox version and original fences. This is read/copy exclusion only, never a substitute for generation-safe canonical S3 publication.

Permanent entry, compiled assets, admission-close/generation contract and remaining resource gates: see [buffered-gc-runtime.md](buffered-gc-runtime.md).
