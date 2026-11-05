# Existing-owner SSH schema apply adapter

This is a candidate execution route, not authorization. The original contract forbids manual schema/application deployment; the final explicit manual exception has not been granted. Do not create an approval record or run apply until Root reviews the exact final union, capture and checks and receives that exception. The protected GitHub workflow and its billing hold are untouched. PR200's plan-only helper remains unchanged.

The route uses the existing Supabase CLI2.122.0 passwordless session, parses its dry-run shell output as data, and connects to the pinned project's verified-TLS5432 session pooler as cli_login_postgres.project. It has no owner-password, Management API, URL, different-principal or role-grant fallback. Credentials remain in memory and are never printed or written to files. As with the existing plan flow, the CLI may renew its standard temporary login as part of normal authentication.

## Exact final readonly evidence

First obtain a fresh PR200 readonly plan from the exact clean final source, after040400/040500/040510 and the final generated boundary are composed. That plan must be less than one hour old, match exact source SHA/generation/project/global-default-revoke, and have ledger SHA175cd1eb198a8afc62576c7ebe5fd2bf06526b3222a94ee3c9b346e7b6a0d129. The earlier05:09 plan is baseline evidence and cannot authorize another source or an expired run.

Place those exact UTF8 bytes at ignored .96bulls/owner-apply/readonly-plan.json. From the same clean source:

```sh
bun scripts/schema-release-owner-apply.ts capture --source-sha <final40hexHEAD> --generation <final64hexgeneration>
```

Capture is readonly apart from session settings and normal temporary CLI authentication. It compares a fresh canonical inspectMigrations inventory/ACL/ledger with the original plan and captures the exact installed boundary metadata hash plus final generated wrapper hash to docs/plans/supabase-owner-apply.capture.json. It emits only safe digests. Missing/unsafe existing boundary shape refuses; it never creates a trigger/schema or changes ownership/default ACLs to repair the prerequisite.

The session explicitly sets SESSION ROLE postgres on the existing temporary CLI login, then probes RESET ROLE inside a read-only transaction. The exact live pooler baseline must be cli_login_postgres / role none on the same backend. SET LOCAL ROLE postgres restores the read-only owner; rollback must restore the prior SESSION owner before work starts. Supavisor's actual07:40Z read-only probe dropped startup role options, so the operator no longer assumes them. This grants no membership or privilege. Every canonical SQL statement is audited to reject role-control operations and fenced by exact owner/login/schema-owner/database/backend identity before and after execution. No reset occurs during canonical apply.

## Final explicit approval and apply

Only after review and the actual human exception, Root supplies ignored .96bulls/owner-apply/approval.json: version1, decisionapproved, byKhaliq Gant, fresh ISOat no earlier than capture, exact verbatim quote, execution ssh-sf-frame-owner-schema-apply, project, sourceSha, generation, ledgerSha256, planSha256, captureSha256, captureGeneration and wrapperSha256. All hashes must match the captured bytes/source. No script writes this approval.

```sh
bun scripts/schema-release-owner-apply.ts apply --source-sha <samefinalHEAD> --generation <samegeneration> --capture-sha <exactcapturefileSHA256> --approval-sha <exactapprovalfileSHA256>
```

All local source/generation/clean-tree/one-hour plan/capture/manual-exception checks happen before invoking even the CLI login. Unknown args/endpoints/principals refuse. Both plan and companion must still be fresh at the first write.

Apply holds the canonical session advisory lock, re-inspects the exact ledger/catalog, audits every canonical migration/verification SQL for role-control statements, and invokes the existing runMigrations with the fixed ledger and original profile (shim/bootstrap false, verify true, dedicated runtime role). Its beforeWrite hook rechecks source/approval and refreshes only the already-existing same-owner hoopdb_boundary.sessions_function_boundary() with one CREATE OR REPLACE using the exact generated committed body. The body hash must equal the canonical prosrc constant; full bootstrap/ALTER OWNER/default ACL/role/trigger provisioning is not run. A transaction verifies the refreshed source boundary before commit.

The adapter fences owner, role, database, schema owner and backend PID before/after canonical queries across their transactions; role-control SQL fails before submission. Rollback/unlock cleanup remains available after errors. The canonical migrator retains its own transaction, ledger, pending/PASS, ACL and verifier authority. Completion requires exact current generation/PASS, no pending inventory and current exact boundary. Failures return only a code; SQL messages/body/credentials are not logged.

The175 pin authorized only that starting ledger. A partial apply may commit earlier migrations and leave readiness pending; the old plan/approval then refuse. Recovery requires a new inspected ledger, separately reviewed exact pin/source and approval. This adapter never imports history or advances a failed step.

**What actually happened (2026-11-05).** The 462 apply committed all 13 planned migrations on the175 baseline, then its first verify transaction lost a deadlock (40P01) to autovacuum: `CREATE POLICY` on `session_directory_event_pending` waited for AccessExclusiveLock while `VACUUM ANALYZE` waited on the apply's transaction. The transaction rolled back and readiness stayed pending. A read-only proof showed the live ledger was exactly175 plus those 13, with matching checksums, so `OWNER_APPLY.ledger` is now pinned to that full ledger, `de0e2fa02ba662bbc1e0ec23e1f619100d3b7cd824e8d055b04c44076d452729` (8fa809b). With the full ledger pinned, a run can apply no schema; it only repeats the hardening, the phase-2 verification and PASS. The resume returned `owner_apply_verified` (0 applied, 33 skipped) at 01:18Z.

**Bounded retry.** `runMigrations` now retries one bootstrap or verify transaction up to `ROLE_CHECK_DEADLOCK_ATTEMPTS` (3) times, on 40P01 only, after a full ROLLBACK, with a 1 s then 2 s backoff, logging `role check deadlocked; retrying`. Other errors (for example 55P03) are not retried. Schema migration transactions are never retried.

## Evidence and limits

Pure controls cover plan/source/generation/ledger/wrapper/manual-exception hashes, expiry/future clocks, source-role audit, backend drift and cleanup, alongside unchanged PR200 cases. The dedicated owned PG17 fixture uses a nonsuper/NOCREATEROLE postgres owner and synthetic cli login, proves startup-reset ownership across actual canonical transactions, rejects no-startup before work, refresh failure rollback, all23 current schema steps, both verifier phases,0foreign session objects and unchanged defaults. Injected migration failure leaves0ledger rows/pending; verifier failure leavespending; canonical retry skips23 and returnsPASS.

Run the checked-in proof only against the explicitly labeled owned fixture:

```sh
bun scripts/pg-proof/owner-apply-proof.ts --owned-fixture
```

That proof deliberately resets synthetic schemas and temporarily provisions its fake roles; it refuses any container without its exact ownership label and loopback mapping. It never uses the production CLI/login. The final union's migrations/prerequisites must be re-proven after composition. Root ran only the actual read-only startup/reset probe; no schema write or live apply has run. Current fresh final plan, prerequisites/GC/storage/buffer scope, independent review/CI and final manual exception remain Root's execution gates.
