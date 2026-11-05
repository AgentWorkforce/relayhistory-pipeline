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

The minimum path to the first visible post-cutover upload is ordered F1/F2,
F3, F4/F5, F6, F7, F8, then the narrow F9 proof. “First-visible gate”
means the upload must appear in canonical list/detail/summary reads and keyword
search through the production-shaped paths. It does not claim complete project
summaries, semantic coverage, replacement/erasure acceptance or broad traffic.

| ID | First-visible gate | Deliverable | Owner | Done evidence |
| --- | --- | --- | --- | --- |
| F1 | **REQUIRED** | Announced slim #230 (schema + production creator only), then compose #233 on its exact merged head | 620 creator agent; migration-completeness | #230 head `6bce87c`, merged as main `233c62b`; creator/ACK/race reviews clear; #233 final diff still pending |
| F2 | **REQUIRED** | Correct owner-only 610 ABI: explicit seed above drained legacy high-water; initial publish; atomic exact prior retirement then replacement publish; no raw pointer arrays in the journal | migration-completeness + supporter-cloud + 620 creator agent | Fresh and replacement transaction tests; missing-seed refusal; rollback emits no row; restricted-role ACL/rollback/sentinel checks |
| F3 | **REQUIRED** | Publish new buffered uploads into ready canonical scopes and directory/replay authorities; drain only the bounded post-cutover catch-up for the canary scope | 620 creator agent (scope-build driver); bulls-migrator as production operator | Captured cutover high-water; zero unpublished/dirty rows for the selected scope; exact read hydration succeeds |
| F4 | **REQUIRED** | Run a durable leased journal consumer with bounded 184 materialization, per-page ACKs, two-pool terminal finish CAS, retry/DLQ and metrics | migration-completeness + supporter-cloud | Restart/stale-worker tests; zero selected-scope journal backlog; immutable worker build identity; runtime in `src/search-features/canonical-consumer{,-runner}.ts` on lease/checkpoint/dead-letter tables from `20261105036000` |
| F5 | **REQUIRED** | Install event, brief, directory-document, authority and profile tables with separate least-privilege writer/reader roles and existing secret bindings | migration-completeness + supporter-cloud; bulls-migrator as operator | Reviewed DDL/grants; endpoint/database/schema hashes; live restricted-role probes |
| F6 | **REQUIRED** | Complete only the selected scope's directory-document baseline and journal catch-up; never expose partial workspace results | migration-completeness; bulls-migrator as production operator | Event, brief and directory pools at the same captured revision; readiness clean |
| F7 | **REQUIRED** | Serve the uploaded session from Supabase-backed list/detail/summary reads. The API path gates first visibility; the broad Cloud UI rollout does not | bulls-migrator read-path lane + Cloud UI lane | Buffered upload visible in directory and exact session hydration; tenant/auth negative tests; rollback flag recorded |
| F8 | **REQUIRED** | Land canonical reader/visibility stack #179+ and route the canary scope only after readiness succeeds | canonical #179+ owner + migration-completeness | Legacy-filter parity, cursor stability, exact hydration authority and fail-closed dirty-state tests |
| F9 | **REQUIRED subset** | First-visible proof: upload, read, keyword search, latency and rollback. Semantic search when a stored vector exists, replacement and delete/tombstone remain required for the full Friday canary but do not gate the first visible session | bulls-migrator coordinates; all owners provide evidence | Timestamped upload/read/keyword results, FINAL rows, no dirty/backlog and rollback rehearsal; then the remaining full-canary evidence |
| F10 | **LATER than first visibility; REQUIRED Friday** | In reserved post-620 migration `20261105027000_canonical_project_events.sql`, replace the Neon-trigger project feeds with durable canonical-publication events in Cloud's current queue ABI: brief completion `{eventId}` and session revision `{kind:"session.revised",eventId,generation}` with decimal-string generation | F10 owner (`codex-96bullsessions-offer-assistance`) + migration-completeness + P5b/Cloud project-events owner | Outbox commits with publication; Cloud resolves trusted event metadata from the new authority, admits once under duplicate delivery, generation-fences acknowledgements, recovers failed sends, and refreshes the canary project summary |

### Required later for broad migration

| ID | First-visible gate | Deliverable | Owner | Why it can follow the bounded canary |
| --- | --- | --- | --- | --- |
| L1 | **LATER** | Dedupe and resume the full 61M corpus plus live catch-up using the existing S3 manifest index and pipeline backfill already running | sessionspipelinegrok + migration-completeness | The Friday canary uses a fully complete bounded scope; broad workspaces remain on the current reader |
| L2 | **LATER** | Complete model-summary generation and its full backfill | model lane | New-upload read/search can use existing title, short summary and stored feature vectors without claiming full model coverage |
| L3 | **LATER** | Finish physical A/B carrier retirement, shared-reference reconciliation, late-PUT barriers and end-to-end S3 erasure | 620 retirement owner + bulls-migrator | Search tombstones are required for the full canary delete proof; broad source-retirement activation remains blocked until this is complete |
| L4 | **LATER** | Prove the largest real images and all oversize cohorts on the high-memory/streaming path | 620 materializer owner + supporter-cloud | The selected canary scope must pass the bounded 184 path; largest-cohort proof does not gate its first visible session and unsupported scopes remain fail-closed |
| L5 | **LATER** | Configure per-org retention across Supabase, ClickHouse and S3, including journal retention | migration-completeness + retirement owner | No TTL or deletion policy is enabled merely to meet the canary date |
| L6 | **LATER** | Backfill every directory document, enable embeddings/model bindings where required, complete broad UI rollout and retire the legacy reader only after parity | canonical #179+ owner + Cloud UI lane + model lane | Broad cutover requires complete coverage and rollback evidence, not just a successful canary |

## Publication authority

- #230 head `6bce87c` is merged to main as `233c62b`; no Supabase migration was
  applied. Its reviewed `20261105025000` writer and `20261105026000` install CAS
  are the authority. They emit the `hs1` key, explicit authority ID, metadata-
  only pointer set, ordered retire/publish pair, and refuse unseeded or
  identical-content replacement. Apply them only in window #2.
- #233 composes the cleared supporter consumer contract from `687e076`: parsed
  and string JSONB both decode fail-closed, retire identity comes from
  `source_authority`, publish prior fields are both-or-neither, and the
  producer cannot append journal rows. The concrete TypeScript journal writer
  remains retired; seed/allocation/CAS evidence comes from #230's SQL tests.
- Durable page ACK/copy-state tables and `finish_canonical_copy` live in the
  registry-assigned `20261105030000_canonical_feature_consumer_acks.sql`;
  the leased consumer's own lease/checkpoint/dead-letter tables live in the
  registry v3.1 `20261105036000_canonical_consumer_lease.sql` (provenance
  proposal-only 031000->033000->036000). Both are dormant metadata/schema:
  applying them does not start a consumer or authorize traffic activation.
  The runtime itself is default-off (`enabled` gate) and scope-fenced to the
  canary org/workspace only.
- Brief-document publication in `20261105024000` is independently default-off.
  Applying the migration creates no scope/outbox rows. After the window is
  applied and before its bounded backfill, the database owner enables it with
  `SELECT sessions.set_session_brief_documents_live(true);`. The call takes a
  bounded directory-write fence, seeds scope keys and flips the trigger live in
  one transaction; a lock timeout leaves it off. Disable with the same owner-
  only RPC using `false`. Either transition invalidates every scope cursor;
  after re-enabling, run the bounded backfill to completion again so updates,
  inserts and tombstones for deletes that happened while off are reconciled.
  Authority remains incomplete until that finishes. No runtime role has
  EXECUTE on this control.
- Define and rehearse the operator-only seed row. It must put the journal above
  the drained legacy feature high-water before any scope install or backfill;
  no data-plane identity may allocate or append revisions.
- Replace the Neon-trigger feeds from migrations 0034/0035/0048/0055/0062.
  Canonical publication must durably create the session-revision and brief-
  completion outbox records and publish only Cloud's current ID-only queue
  messages. The event ID must resolve through the post-cutover authority; merely
  sending a compatible JSON body while Cloud still reads the stopped Neon
  outbox is not sufficient. Preserve coalescing, retry/release, generation-
  fenced acknowledgement and tenant-attribution semantics.
- Complete the physical retirement/reconciliation worker, including cleared
  buffered slots, legacy-only privacy controls, shared references and late PUT
  barriers. Search tombstones cannot substitute for source erasure.

## Durable producer runtime

- The real 025000 writer has rollback, sentinel and restricted-role verification.
  The producer identity can read journal/pointer pages but cannot call the
  owner-only writer or mutate the journal. The companion ACK/finish DDL still
  requires its own restricted-role probe before activation; its 030000 slot,
  sentinel, rollback and generated ACL boundary are part of this composition.
- Supply the real bounded 184 materializer adapter for `rows`, `priorKeys` and
  the complete-image descriptor. Each page must retain the exact image,
  pointer-set and page hash checked by `copyCanonicalEntry`.
- `CanonicalPageAckStore` and the finish RPC contract implement revision-keyed,
  immutable page chains, durable resume and two-pool terminal finish CAS. The
  target binding requires a nonempty deployment `targetId` in addition to the
  CH database/table identities. A supervised leased consumer, retry/DLQ and
  metrics remain activation work; no package starts this runtime yet.
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
- Canonical publication currently proves exactly two managed output pools:
  event and semantic brief. Complete the directory-document pool, CH-only
  event-visibility subquery and four-pool reader wiring before claiming reader
  parity.
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
