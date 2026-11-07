# F3 PR evidence (PR session links off Neon)

Dormant source and schema. The migration-handover lead is the only production operator. Do not
apply 044000, deploy, enable or backfill from this lane. Every apply, flag and deploy needs
Khaliq's explicit OK in his own words (AGENTS.md).

## What it replaces

Cloud's PR-session-link stamper read Neon `sessions.session_pull_request_evidence_v1`
(relayhistory 0065). It was a read over `convergence_events` tool calls and tool results,
`session_catalog.repo_url` and `session_commit_links`. SP has the same three inputs in canonical
S3, and `projectRelayhistorySession` rebuilds the Neon rows from them.
`src/session-pr-evidence/extract.ts` ports 0065 over those rows.
`packages/pipeline/test/pr-evidence-parity.test.ts` runs the real vendored 0065 function and the
port on the same delivered records and requires equal row sets.

The GitHub API is not a source. Cloud keeps its existing GitHub half: branch-only rows resolve to
PRs in cloud-web, and cloud-web stamps the PR bodies.

## Data path

1. A canonical publish (`canonical_feature_journal` `publish`) arms a
   `session_pr_evidence_wakeups` row while `session_pr_evidence_settings.enabled`. This covers
   every scope, independent of the F10 canary scope rows.
2. The worker (`SERVICE=session-pr-evidence`, `SESSION_PR_EVIDENCE_ENABLED=true`) runs these RPCs.
   They are pinned to `bulls96_session_ingest` (`ingest_require_role` plus EXECUTE), never
   `service_role`. The worker has its own ingest-actor connection, `SESSION_PR_EVIDENCE_DATABASE_URL`,
   which is a runtime secret from 1Password and never on disk. It verifies `current_user` before any
   RPC. It needs `INGEST_EXPECT_GENERATION` to read history masks.
   - `session_pr_evidence_claim_v1` leases the wakeup and captures the scope's exact
     replay-source snapshot (pointer manifest keys, epoch, fingerprint);
   - for a history-authoritative scope, the worker pages the authority's record-winner mask
     (`session_history_mask_page`);
   - `readCanonicalScope` reads those manifests. For a history scope, `maskedProjection` first
     filters the RAW normalized records through the mask (records that moved to another scope,
     delete winners, other-origin or other-revision copies), then projects. The catalog collapse
     therefore sees only admitted records. `extractPullRequestEvidence` runs last;
   - `session_pr_evidence_install_v1` CAS-installs the rows under the replay-publication row lock,
     bound to the fingerprint. For a history scope, install requires the worker to attest the
     exact live, ready authority and mask it applied.
   - A blocked, absent or not-yet-ready source is released with backoff and never installed. The
     worker never deletes evidence; only the erasure triggers do.
   - Oversize or inconsistent sources install `incomplete` with a code. A missing manifest or an
     S3 failure releases the wakeup with backoff, and the worker retries.
3. Cloud's `bulls96_project_events` actor calls `session_pr_evidence_read_v1` with
   `{schemaGeneration, scope, eventId, generation}`. It is served only for an unacknowledged F10
   event at that generation, and only when the evidence was built from that event's own source
   fingerprint:

   | Status | Meaning | Cloud action |
   |---|---|---|
   | `ready` | rows in Neon's column contract | process the rows |
   | `pending` | not built for this source yet | retry, never ack |
   | `deferred` | the event is stale | wait for the replacement event |
   | `incomplete` | the source could not be read completely | park as a coded failure, never "no PRs" |
   | `missing` | no unacknowledged event at that generation | none |

   A current `retire` event returns the distinct status `retired` (no rows). Cloud then adds no
   PR rows and leaves existing stamps alone. This is not the same as "the session had no PRs".

Erasure: deleting or tombstoning a `session_directory` row purges that scope's evidence, state
and wakeup in the same transaction. A user's `upload_resets` row does the same for every logical
session that user has any pointer in, even when the directory row stays. A wakeup-arming failure
never fails the creator transaction (an exception handler skips it), and the bounded backfill
recovers a lost wakeup. Retention-ready: rows carry `org_id` plus `built_at`.

## Activation order (lead, each step with Khaliq's OK)

1. Land 044000 only after #288 (040000), #289 (041000) and the lower slots, in numeric order.
   Apply it in a reviewed migration window. Regenerate the boundary first:
   `bun scripts/sync-sessions-function-boundary.ts --check`. The F10 actor verifier now also
   allows `session_pr_evidence_read_v1`. Re-run `canonical_project_events_verify.sql` after
   migrating.
2. Deploy the worker with `SESSION_PR_EVIDENCE_ENABLED=false`. Then set it to true. It still
   claims nothing until step 3.
3. `INSERT INTO sessions.session_pr_evidence_settings(enabled) VALUES (true);`. New publishes
   arm wakeups from this point.
4. Backfill old sessions, and recover any lost wakeup, in bounded pages. This arms every live
   scope whose evidence is missing, or stale against the current replay-source fingerprint:
   `SELECT sessions.session_pr_evidence_enqueue_v1('{"limit":1000,"after":null}')`.
   Repeat it with the returned `next` until `next` is null. Watch the wakeup depth and
   `last_error` counts.
5. Parity on the canary workspace:
   - for each canary session with PR tool calls, the Neon `session_pull_request_evidence_v1`
     row set must equal the `session_pr_evidence` rows for that session;
   - then shadow-stamp in Cloud (block hash only, no GitHub write) and compare it with the
     legacy stamp hash.
6. Cloud: set `CANONICAL_PR_EVIDENCE_ENABLED=true` (Cloud PR stacked on #6037), only after
   `CANONICAL_PROJECT_EVENTS_ENABLED` and steps 3-5.

## F10 widening beyond the canary

Today one scope is live in three places:
- the `canonical_project_event_scopes` row set;
- the publisher (`src/canonical-project-events/settings.ts`, one org/workspace);
- Cloud `canonicalProjectConfig` (one org/workspace).

Steps:

- **W0.** PR evidence on for all scopes (step 3) plus the backfill, with F10 still canary-only.
- **W1, per-workspace batches.** Add reviewed scope rows. The publisher must claim across the
  enabled rows (one claim per enabled scope; the RPC guard is already per scope), and Cloud must
  accept a set of scopes checked against the event identity. These are small code changes in
  both. No SQL change.
- **W2, all orgs.** A reviewed `scope_mode='all'` singleton consulted by
  `canonical_project_events_journal_changed` and `canonical_project_events_guard`, as a
  `CREATE OR REPLACE` in a later slot (047000+). Never hand-insert one row per org.
- **Before each step:** drain the Neon events for those workspaces (below), then check the
  queue and DLQ depth and `canonical_project_events_pending_v1`.
- **Rollback:** disable the scope rows. Events are retained.

## Neon revision-event drain

Do this before `NEON_JOB_SESSION_REVISIONS=off` (relayhistory #322) and before Cloud #6125 sets
`revisions`. Events are ephemeral, so a frozen copy does not help. The lead runs read-only SQL
with `relayhistory_readonly`.

1. **Quiesce check.** Expect 0:
   `SELECT count(*) FROM sessions.session_revision_events WHERE created_at > now() - interval '1 hour'`.
   Record the backlog N0 with
   `SELECT count(*), max(generation - coalesce(acked_generation,0)) FROM sessions.session_revision_events WHERE acknowledged_at IS NULL`.
2. **Keep the legacy path running.** The RHC publisher and Cloud's `relayhistory-project-events`
   consumer stay on, and #6125 `revisions` stays unset.
3. **Drain.**
   - The minute cron `recoverSessionRevisions` (`pending_session_revision_events_v1`, up to 100
     per tick) and queue delivery ack each row.
   - Redrive the `relayhistory-project-events` DLQ once.
   - Deterministic failures take the existing parked-ack path.
4. **Done when:**
   - `acknowledged_at IS NULL` count = 0;
   - queue and DLQ depth = 0;
   - no running `public.session_revision_attempts`;
   - the counts still hold after 30 minutes.
5. **Residue.** A row that stays pending for a retryable reason (for example a GitHub outage):
   either wait for it, or record `(org, workspace, source, session)` only and let F10 restamp the
   session from SP evidence once its workspace is covered. Never ack by hand-written SQL.
6. **Then** turn off `NEON_JOB_SESSION_REVISIONS` and set #6125 `revisions`. Re-check that the
   count stays 0 for 24 hours before the Neon drop.

## Known limits

- If the winning copy was folded away by revision normalization, the record is dropped. The
  failure mode is under-attribution, never mis-attribution.
- Extraction is CPU-bound per scope, bounded by the artifact source limits (256 MiB, 1M records).
  A larger scope installs `incomplete:scope_too_large`.

## Rollback

- Operationally: set Cloud `CANONICAL_PR_EVIDENCE_ENABLED` unset, then
  `UPDATE sessions.session_pr_evidence_settings SET enabled=false`, then disable the worker. Keep
  the evidence.
- The generated down migration refuses while any state, wakeup or enabled setting exists. It
  also refuses after a later migration has been applied.
