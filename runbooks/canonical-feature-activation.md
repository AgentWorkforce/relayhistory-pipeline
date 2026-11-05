# Canonical feature activation gates

Applying the Supabase schema does not activate canonical ClickHouse search.
The composed source remains dormant until every gate below has reviewed
evidence. Do not enable a reader merely because tables or functions exist.

## Owned delivery checklist

The Friday target means a bounded canary org/workspace can receive a new
post-cutover upload, publish it through the canonical scope, read it through the
Supabase session path, and find it through the ClickHouse search path by
2026-11-06. It does **not** mean broad traffic or a partially backfilled
workspace: a selected search scope stays fail-closed until its baseline and
catch-up are complete at one captured high-water. Every production apply,
deployment, cloud-resource change and backend-flag change still requires
Khaliq's separate explicit approval.

### Required for the Friday canary

| ID | Deliverable | Owner | Done evidence |
| --- | --- | --- | --- |
| F1 | Announced slim #230 (schema + production creator only), then restack #233 on its exact head | 620 creator agent; migration-completeness | Final base SHA; clean stacked diff; creator race suite |
| F2 | Correct owner-only 610 ABI: explicit seed above drained legacy high-water; initial publish; atomic exact prior retirement then replacement publish; no raw pointer arrays in the journal | migration-completeness + supporter-cloud + 620 creator agent | Fresh and replacement transaction tests; missing-seed refusal; rollback emits no row; restricted-role ACL/rollback/sentinel checks |
| F3 | Publish new buffered uploads into ready canonical scopes and directory/replay authorities; drain only the bounded post-cutover catch-up for the canary scope | 620 creator/publication owner + sessionspipelinegrok | Captured cutover high-water; zero unpublished/dirty rows for the selected scope; exact read hydration succeeds |
| F4 | Run a durable leased journal consumer with bounded 184 materialization, per-page ACKs, two-pool terminal finish CAS, retry/DLQ and metrics | migration-completeness + supporter-cloud | Restart/stale-worker tests; zero selected-scope journal backlog; immutable worker build identity |
| F5 | Install event, brief, directory-document, authority and profile tables with separate least-privilege writer/reader roles and existing secret bindings | migration-completeness + supporter-cloud; bulls-migrator as operator | Reviewed DDL/grants; endpoint/database/schema hashes; live restricted-role probes |
| F6 | Complete only the selected scope's directory-document baseline and journal catch-up; never expose partial workspace results | migration-completeness + sessionspipelinegrok | Event, brief and directory pools at the same captured revision; readiness clean |
| F7 | Serve the uploaded session from Supabase-backed list/detail/summary reads | bulls-migrator read-path lane + Cloud UI lane | Buffered upload visible in directory and exact session hydration; tenant/auth negative tests; rollback flag recorded |
| F8 | Land canonical reader/visibility stack #179+ and route the canary scope only after readiness succeeds | canonical #179+ owner + migration-completeness | Legacy-filter parity, cursor stability, exact hydration authority and fail-closed dirty-state tests |
| F9 | One-org canary: upload, read, keyword search, semantic search when a stored vector exists, replace, delete/tombstone, latency and rollback | bulls-migrator coordinates; all owners provide evidence | Timestamped request/results, FINAL rows, parity report, no dirty/backlog, rollback rehearsal |

### Required later for broad migration

| ID | Deliverable | Owner | Why it can follow the bounded canary |
| --- | --- | --- | --- |
| L1 | Dedupe and resume the full 61M corpus plus live catch-up using the existing S3 manifest index and pipeline backfill already running | sessionspipelinegrok + migration-completeness | The Friday canary uses a fully complete bounded scope; broad workspaces remain on the current reader |
| L2 | Complete model-summary generation and its full backfill | model lane | New-upload read/search can use existing title, short summary and stored feature vectors without claiming full model coverage |
| L3 | Finish physical A/B carrier retirement, shared-reference reconciliation, late-PUT barriers and end-to-end S3 erasure | 620 retirement owner + bulls-migrator | Search tombstones are required for the canary; broad source-retirement activation remains blocked until this is complete |
| L4 | Prove the largest real images and all oversize cohorts on the high-memory/streaming path | 620 materializer owner + supporter-cloud | The canary must select a proven bounded scope; unsupported scopes remain fail-closed |
| L5 | Configure per-org retention across Supabase, ClickHouse and S3, including journal retention | migration-completeness + retirement owner | No TTL or deletion policy is enabled merely to meet the canary date |
| L6 | Backfill every directory document, enable embeddings/model bindings where required, complete broad UI rollout and retire the legacy reader only after parity | canonical #179+ owner + Cloud UI lane + model lane | Broad cutover requires complete coverage and rollback evidence, not just a successful canary |

## Publication authority

- Merge and apply the 610/620 creator stack only in its planned generation
  window. The owner-only journal append is composed into the final install CAS,
  but schema presence alone is not activation.
- Fix the replacement journal ABI before merge. The current wrapper emits only
  one `retire` row when `priorSource` exists, never a `publish` row for the new
  image, and records the new pointer set on the retire row. It must durably
  order an exact prior-key retirement and replacement publication, carrying a
  verified descriptor for each image, without a visibility gap.
- Define and rehearse the operator-only seed row. It must put the journal above
  the drained legacy feature high-water before any scope install or backfill;
  no data-plane identity may allocate or append revisions.
- Complete the physical retirement/reconciliation worker, including cleared
  buffered slots, legacy-only privacy controls, shared references and late PUT
  barriers. Search tombstones cannot substitute for source erasure.

## Durable producer runtime

- Add rollback, sentinel and restricted-role verification for the real 610
  migration, including a producer read identity that cannot call the owner-only
  wrapper or write the table directly.
- Supply the real bounded 184 materializer adapter for `rows`, `priorKeys` and
  the complete-image descriptor. Each page must retain the exact image,
  pointer-set and page hash checked by `copyCanonicalEntry`.
- Implement the durable page-ack/terminal-finish store and a leased journal
  consumer. Checkpoints advance only after both event and brief terminal pages,
  current source CAS, and unchanged target/profile bindings.
- Implement the owner-only `publishOnce` backfill RPC behind the source port,
  then replace the JSONL-only helper with a durable resumable enumerator for the
  full corpus and catch-up. Replaying an image identity must return its existing
  revision rather than allocate a duplicate publication.
- Wire a supervised cloud job with bounded concurrency, retry/dead-letter
  policy, metrics and alerts. No current package starts this runtime.

## ClickHouse and read path

- Create the reviewed event, semantic-brief, directory-document, authority and
  profile tables in the selected ClickHouse database. Grant separate least-
  privilege writer and reader identities; bind the normalized endpoint,
  database and schema hashes as the target identity.
- Publish and verify the exact source case-fold profile. Both event and brief
  pools must be complete, fresh, at the same observed revision and paired to
  the same source/target/profile identity before serving.
- Complete the CH-only event-visibility subquery and four-pool reader wiring.
  Preserve all legacy filters, wildcard behavior, snapshot/cursor stability,
  deterministic ranking and exact hydration authority.
- Provision the existing query-embedding provider/model binding. Canonical
  publication copies stored vectors and model labels; it does not generate
  missing embeddings.
- Deploy the reader/UI route and only then enable its backend flag. Keep the
  current backend selected until canary parity, latency and rollback evidence
  pass; schema presence is never the feature flag.

## Backfill, retention and acceptance

- Run bounded historical publication, live catch-up and directory-document
  backfill; prove zero dirty/outbox/journal backlog at a captured high-water.
- Configure per-org retention and the reviewed Supabase/ClickHouse/S3 erasure
  path. A ClickHouse TTL alone does not retire source pointers or journal rows.
- Prove restricted-role PostgreSQL, object-store and ClickHouse races, including
  replacement, erase/re-enable, stale worker, late PUT, empty workspace, shared
  carrier, missing manifest and largest-image cases.
- Record immutable build/deploy identities, target/source/profile hashes,
  readiness rows, canary parity and rollback steps before traffic activation.
