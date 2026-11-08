# Supporting readiness receipts for pipeline-lead

Execution source remains f9409ccb9e46c3f100cf3017847d8ed62bc716c7, G2 e778dbb6cb68bc0e8939797691565db0b1dd8d357d7d790b375036e0070923ce. This documentation revision is not a new runtime/migration source. Production work in these receipts is read-only; all apply/bootstrap/rebind/upload tests were on the explicitly owned synthetic fixture. No production approval file was created.

## 1. Current G1 census

At 2026-11-07T19:21:51.808Z, in one REPEATABLE READ READ ONLY existing-owner transaction on sf-frame, the readiness marker was PASS at G1 78c7b28e5ebce7f5f534a1f3f41a55845251e950b6c4c49f676f2bda358c91d6; the singleton lazy policy was G1 and unexpired; all 1,028 admissions (including expired rows in the census) were G1, all 1,028 unexpired. There were zero unexpected admission generations. Policy/admission expiry was 2026-12-05T05:49:28.819Z. Identity and transaction read-only status were checked before and after; no row locks, UPDATE or rebind were used. This is timestamped evidence; refresh before the actual window.

Receipt: g1-census.json. Executed helper: g1-census-readonly.ts.txt (text evidence, not a production CLI entrypoint).

## 2. Fixed role catalog, and 3. privileged prerequisite

At 2026-11-07T19:10:34.410Z on PostgreSQL 17.11, the same existing-owner read-only route reported the following. Raw attributes, incoming and outgoing membership flags/grantors, and global/database settings are in fixed-role-inventory.json. No password catalog columns were selected; unrecognised setting values are redacted. Present means no detected violation in this catalog subset; it does not mean full effective privilege, ownership, ACL or phase-two readiness PASS.

| Role | Catalog disposition | Settings / verifier qualification |
|---|---|---|
| anon | Present platform built-in | statement_timeout=3s; dedicated NOINHERIT/NOLOGIN restrictions do not apply |
| authenticated | Present platform built-in | statement_timeout=8s; dedicated NOINHERIT/NOLOGIN restrictions do not apply |
| service_role | Present platform built-in | none; dedicated NOINHERIT/NOLOGIN restrictions do not apply |
| session_directory_search_worker | NOLOGIN, no detected catalog violation | statement_timeout=2s |
| session_token_verifier | NOLOGIN, no detected catalog violation | none |
| bulls96_session_ingest | LOGIN, no detected catalog violation | statement_timeout=5s, lock_timeout=2s, idle_in_transaction_session_timeout=10s |
| bulls96_session_ingest_seeder | NOLOGIN, no detected catalog violation | none |
| bulls96_canonical_feature_producer | MISSING — separate bare bootstrap required | not available |
| bulls96_project_events | MISSING — separate bare bootstrap required | not available |
| bulls96_read_api | LOGIN, no detected catalog violation | statement_timeout=2s, default_transaction_read_only=on |
| bulls96_privacy_eraser | MISSING — separate bare bootstrap required | not available |
| bulls96_babysitter_retention | MISSING — separate bare bootstrap required | not available |
| bulls96_runtime | LOGIN, no detected catalog violation | none |

All existing dedicated roles have no outgoing memberships and only the narrow Supabase creator incoming edge: member postgres, grantor supabase_admin, ADMIN true, INHERIT false, SET false. No database-specific override was found. bulls96_runtime has INHERIT true, which the actual runtimeRoleViolations contract does not reject by itself when it has no outgoing memberships. Do not apply bare-role restrictions to Supabase platform built-ins.

The four missing roles are bulls96_canonical_feature_producer (first needed at 025000), bulls96_project_events (033000), bulls96_privacy_eraser (041000), and bulls96_babysitter_retention (042000). In particular, do not start the 21-file apply on this inventory: absence of the producer can fail after the first five pending files have committed.

All four are provisioned by the exact f9409ccb supabase/bootstrap/session_ingest_roles.sql, SHA256 c957ae416d707d471b576bea93dee07f2035fb8b930bacaff4207d63ffc0b478 (verbatim copy session_ingest_roles.sql.txt). Its one atomic DO block creates only absent fixed roles as NOLOGIN/NOINHERIT/non-elevated, validates existing roles and outgoing memberships, and does not change an existing approved LOGIN or its settings. A separately reviewed privileged bootstrap authorization is REQUIRED and not represented by these receipts. It was NOT run in production. The current owner catalog shows postgres has CREATEROLE under Supabase, which is a capability observation, not permission to use it. Do not invent a new principal/credential fallback.

The babysitter role MUST remain NOLOGIN at installation. privacy_eraser_role.sql and babysitter_retention_role.sql are later LOGIN/operator phases, excluded here. bulls96_read_api already exists with the expected 2s/read-only settings; no read-api bootstrap/reset is requested. 036000 uses the canonical producer and does not introduce a new role named canonical consumer. The migration's own verifier/ACL checks remain authoritative.

## 4. Full 21-file rehearsal and measured fixture cost

The existing stopped container handover-w2-rehearsal-pg17-20261105 was identified by migration.owner=handover-window2-rehearsal and its loopback-only port, capped at 512 MiB / one CPU, started for this proof, then stopped with its volume retained. The finite database test ran through the Mac shared aw-heavy queue wrapping SSH to sf-frame. No other container/VM was changed.

The executed runner (window21-rehearsal.ts.txt) adapts the checked-in eight-file window2-rehearsal.ts only in the ignored evidence directory: exact f9409ccb/G2 pin, all 21 expected pending names from the real plan, source cleanliness checks that allow the canonical capture/CLI-version cache, and a transparent SQL timing observer. Legacy window2 event labels/comments remain in the actual executed evidence; expected inventory and result explicitly prove 21. It uses actual ownerProfile, owner-fenced runMigrations, wrapper audit/refresh, plan/capture binding and applyOwnerRelease. The synthetic authorization-shaped object is RAM-only and explicitly marked synthetic/offline, never production authorization. The database endpoint is fixed to the owned loopback fixture; no production credential is accepted.

Baseline source 8f087322 recreated the same 35-row ledger hash 3173f2df7f749fd79c340286f3284d91d8f5a8b988b7f1b1a11e01c3a3307641 and PASS(G1). The fixture then seeded 974 synthetic admissions and a G1 policy. Its separate history ledger was unchanged by the G2 apply. The target privileged bare-role bootstrap ran ONLY on this fixture before the actual Supabase-profile apply (shim=false, bootstrap=false, verify=true; global-default-revoke). The final marker is PASS(G2); all 21 schema transactions and all five hardened phase-two transactions committed, with no observed retries. Brief-document publication remained off with no scopes/outbox rows.

Measured applyOwnerRelease wall time: **2,893.336 ms**. Sum of 21 schema transactions: **1,109.577 ms**. Sum of five verifier transactions (including per-step ACL hardening/identity work): **1,463.742 ms**. Overall apply also includes wrapper/preflight/fences and other overhead. Setup/base/bootstrap/history and subsequent upload/rebind proof are outside that apply time. Per-file measurements below are BEGIN-to-COMMIT; bodyMs in the raw result measures just the exact step SQL.

| Migration / verifier | Transaction ms |
|---|---:|
| upstream:20261105020000_session_replay_pages.sql | 27.898 |
| upstream:20261105021000_session_large_read_jobs.sql | 30.659 |
| upstream:20261105022000_session_desktop_catalog.sql | 8.579 |
| upstream:20261105023000_session_analysis_publications.sql | 43.408 |
| upstream:20261105024000_session_brief_documents.sql | 27.276 |
| upstream:20261105025000_canonical_feature_journal.sql | 15.661 |
| upstream:20261105026000_canonical_history_lineage.sql | 289.836 |
| upstream:20261105030000_canonical_feature_consumer_acks.sql | 15.790 |
| upstream:20261105031000_desktop_catalog_forget_reproject.sql | 20.882 |
| upstream:20261105032000_session_large_read_routing_fixes.sql | 10.435 |
| upstream:20261105033000_canonical_project_events.sql | 35.805 |
| upstream:20261105034000_session_directory_read_api.sql | 22.649 |
| upstream:20261105035000_semantic_copy_state.sql | 47.891 |
| upstream:20261105036000_canonical_consumer_lease.sql | 6.835 |
| upstream:20261105037000_session_directory_document_pool.sql | 24.493 |
| upstream:20261105038000_claim_attempt_records_retirement.sql | 43.097 |
| upstream:20261105039000_buffered_gc_single_candidate.sql | 12.456 |
| upstream:20261105040000_ingest_lazy_source_tables.sql | 26.297 |
| upstream:20261105041000_ingest_privacy_authority.sql | 143.062 |
| upstream:20261105042000_babysitter_evidence_retention.sql | 215.359 |
| upstream:20261105043000_brief_titles.sql | 41.212 |
| verify:session_directory_search_worker | 296.128 |
| verify:session_token_verifier | 266.364 |
| verify:session_ingest_seeder | 261.120 |
| verify:session_ingest | 317.296 |
| verify:canonical_project_events | 322.834 |

The real deliverBatch + SQL-ledger adapter refused old G1 with 55000 generation not ready, and refused new G2 before rebind with current custody policy unavailable. The actual rebind dry run reported 974 and rolled back, execute rebound 974, and the idempotent repeat rebound zero. New G2 then returned a custody ACK, while old G1 still refused. This is not an HTTP/edge/deployed Worker readback proof.

**Production outage budget remains MISSING.** This is a largely empty synthetic PostgreSQL 17.11 fixture, not production row volume, index-build/lock pressure, customer content or runtime replacement latency. The current production census has 1,028 admissions, not the fixture's 974. The 2.89-second measurement cannot establish a live outage upper bound. The release owner still needs qualified G2 Worker/GC/publisher/other-consumer artifacts, pinned host/IAM/launcher inventory, preserved GC/enablement, deployment timing and traffic baseline. Missing runtime slots are listed explicitly in execution-targets.json. No runtime pin is invented.

## Receipt index

- g1-census.json: SHA256 71e19b00e7503440259ed1d0dcf9e0ebb40cc8a09a75103d9900f83f461a52fe
- fixed-role-inventory.json: SHA256 8b783e760171d2a9f9b9526002c5d968cf06c08ff169517a32a4d3b5588f7c4e
- bootstrap-prerequisite.json: SHA256 f9e4d9c97c60f77af58341ce81b742f80843422743977b3eb2a1013d1dccfb28
- window21-rehearsal-result.json: SHA256 058f617f93ae0175f71459277959157da0afb81675421f0a0d6159de1c1fc0c8
- rehearsal-summary.json: SHA256 213962668c8c63ecc1a4503f09cc460af35746bb054ce87eab4a5df667ea779c

The original plan/capture expire no later than 2026-11-07T19:47:10.129Z; fresh plan/capture and current pre-window checks are needed at execution. All unresolved prerequisites remain HOLD.
