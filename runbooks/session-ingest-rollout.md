# Session ingest release gates

This run sheet supersedes the unreleased exporter-state canary plan. It is a source/operator checklist, not permission to apply, deploy, seed or change routing. Root owns the exact final SHA/generation, operator plan, resource permissions, approval and rollback. No source helper may infer approval from a green fixture.

The chosen interim route accepts scrubbed bytes into durable buffered storage with an atomic SQL receipt/outbox. It publishes no canonical manifest and sends no canonical fanout or Neon replica. New content becomes visible only after the separately reviewed background publication path succeeds. Ending the incident requires the actual full-service upload/read/search/Bob flow; buffered receipt acceptance alone is interim relief.

| Order | Required evidence before proceeding |
| --- | --- |
|1| Freeze the exact composed source, regenerate function ACL/boundary and generation, and verify current-head required CI plus independent review. Existing auth and ingest role bootstraps are privileged prerequisites; migrator never creates/repairs roles. |
|2| Review the owner/apply plan against actual PostgreSQL owner/default ACL/event-trigger metadata. Preserve existing owners. Apply only through the separately approved guarded path, with exact source/ledger/generation pins. A CLI/operator connection must never silently fall back to another role. |
|3| Verify all four phase2 files through migrator/live/external PASS: directory search worker, token verifier, S3 seeder and ingest. Bare NOLOGIN is valid for schema preflight; runtime LOGIN needs its exact reviewed timeout profile and credential binding. The operator verifier still requires LOGIN. |
|4| BEFORE any destination claim, selection/reset mirroring or buffered receipt, establish the explicit privacy/fence baseline. Freeze source fence writers for the baseline snapshot/import. Import actual per-user deletion/reset metadata, including empty known users. A partial import with changed source fences stops for reviewed reconciliation; never delete or overwrite sealed baselines to force retry. |
|5| Prove per-org privacy and retention admission, defaultoff runtime configuration, exact private bindings and S3 permissions, durable receipt/outbox, GC recovery, version-aware erasure/barriers and late-write controls. Table sessions.ingest_buffer_admission is empty by default; no implicit org enablement. |
|6| Review and apply the exact private buffered deployment/routing plan only after operator gates. Source requires INGEST_ACCEPTANCE_MODE=buffered, INGEST_INLINE_MAX_SESSIONS=0 and INGEST_REPLICA_MODE=off. Queue delivery remains unable to publish buffered stages. The reviewed buffered deployment must pin all three settings. The source canonical default preserves existing deployments; it is not a safe substitute for omitted buffer inputs. Missing required resources or invalid buffered settings must refuse. |
|7| Historical replay bounds require actual complete S3 proof under040400, after a separately proven complete legacy source frontier. Preview is default. The old EXPORT_STATE_FILE/session-boolean seed is retired and both old bulk RPCs fail closed. Original revision IDs/digests must never be invented. |
|8| Canonical drain is a separate reviewed prerequisite. It must own source/epoch/publication authority, refilter original revisions against proven maxima, close the historical baseline on explicit canonical admission BEFORE canonical S3 IO, and prove generation-safe storage/erasure. An old row lock or metadata nonce alone cannot stop late S3 publication. |
|9| Before service completion/ramp, prove actual deployed upload→durable storage→current source projection/search/read routes→Bob output and real consuming Desktop/MCP/search behavior, with tenant/fence/erase/reset/replay/update and log-leak controls. Synthetic fixture success and an encrypted object without a consuming read path are insufficient. |

## Proof and seed

Use docs/ingest-s3-proof-seed.md. The CLI requires clean exact source, explicit generation/org/bucket/source pins and credentials via inherited descriptors. It defaults to read-only preview. The actual operator credential/membership path and S3 GetObject/KMS decrypt permission must be reviewed; no owner/admin fallback exists. Per-org rollout can bound initial verification cost, but cannot waive the full record/tombstone frontier within that scope.

Never re-run historical seed after canonical admission to close a supposed seed window. Canonical admission or a destination fence operation permanently closes that baseline; a trusted buffered receipt alone preserves an existing baseline. An identical pre-canonical retry is idempotent; changed source/fence images refuse. Existing maxima, tombstones and receipt IDs remain authoritative.

## Buffered privacy and recovery

Use docs/buffered-upload-gc.md and040500/040510. ACK requires an intent registered before PUT plus an atomic durable receipt/outbox, not queue TTL. Keep original ordinals, original source/reset epochs and the exact stored object hash. Disable/reset can retire an unacknowledged intent without ACK. Mixed acknowledged objects require a tracked replacement preserving surviving groups, followed by atomic pointer swap and durable old-object cleanup.

Prospective deterministic replacements stay owned by the current acknowledged parent through repeated lease expiry; they cannot be independently barriered while the parent still needs that key. Monotonic keep-set shrink releases obsolete children for cleanup. Reset covers no-session records and legal no-session groups may combine sources.

Erasure uses a verified persistent redaction barrier and permanent removal of all other object versions. Concurrent cleanup must reuse the same barrier/version; late conditional PUT must fail. A48-hour queue window, TTL, missing current key or a lease by itself is never proof of erasure. SQL cannot attest S3 behavior; runtime/version proof is mandatory.

## Rollback and observation

Root's reviewed routing rollback stops new acceptance/publication while preserving acknowledged durable bytes, outbox entries, replay bounds, ownership epochs and cleanup tickets. Do not drop tables, clear ledgers, lower revisions or restore stale fence snapshots. Defaultoff is required for unfinished background processors. Any unexplained receipt/storage mismatch, cross-tenant result, lost generation fence, failed cleanup recovery or log leak holds publication/ramp until independently resolved.

The existing bootstrap/operator files remain source authorities: supabase/bootstrap/session_ingest_roles.sql, session_ingest_role.sql, session_ingest_role_verify.sql and the pure session_ingest_verify.sql. They do not authorize execution merely by existing in the tree.
