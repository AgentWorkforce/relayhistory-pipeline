# Self-host canonical publication: owner rows contract

Status: contract and gap analysis only. This document adds no SQL, role, RPC or
ceremony. Running anything below against a real database needs Khaliq's own-words OK.

The portable publisher (`INGEST_PUBLICATION_TARGET=portable`,
`src/session-ingest/publication-portable.ts`) only consumes authority. Before any stage
is a candidate, an owner must have written three kinds of rows:

1. `sessions.ingest_buffer_admission`: per-org privacy and retention admission.
2. The receipt seat clock and custody (`sessions.ingest_receipt_seat_clock` and related):
   first-ACK acceptance seats.
3. `sessions.ingest_publication_frontiers`: per-(org, workspace, user, origin)
   completeness attestation for one target.

The publisher login (`bulls96_session_ingest`) cannot write any of them. Each table has
RLS on and `REVOKE ALL` from PUBLIC, anon, authenticated, service_role, the ingest login
and the seeder. The seat and policy functions check `current_user` = function owner.

## What hosted production does today

| Rows | Writer on main | Where it runs |
|---|---|---|
| `ingest_buffer_admission` | Owner `INSERT` in step S4 of `scripts/ops/cutover-20261105/cutover-ceremony.ts` (one transaction with the policy and seat clock). New orgs: `cutover-ceremony.ts --admit-new-orgs` (`INSERT ... ON CONFLICT (org_id) DO NOTHING`, copying the live policy's epoch, baseline, retention and generation). | Supabase owner via the CLI temporary login. `--admit-new-orgs` runs every 120 s from `mini/admit-new-orgs.sh` on sf-mac-mini. That script is outside this repository; see `docs/runbooks/buffered-cutover-20261105.md`. |
| Lazy custody policy | `sessions.ingest_lazy_policy_set(jsonb)`, `supabase/migrations/20261104055000_lazy_current_custody.sql`. Owner only, `issuer` must be `relayhistory-cloud`, and identity is immutable once set. | The same S4 transaction. |
| Seat clock | `sessions.ingest_receipt_seat_initialize(jsonb)`, created in `supabase/migrations/20261104053000_ingest_first_ack_seats.sql` and replaced in 20261104055000. Owner only. Needs protocol `delivery-acceptance-floor-v1`, the captured Neon sequence floor, and an admission row or lazy policy with the same epoch and generation. | The same S4 transaction. |
| `ingest_publication_frontiers` | **No tracked writer.** There is no migration function, RPC, ceremony step or runbook command. The only `INSERT`s are synthetic: test fixtures (`src/session-ingest/__tests__/publication.postgres.test.ts`, `publication-portable.pglite.test.ts`) and `scripts/proof-canonical-publication.ts` against a disposable owned PG17. | Nowhere in production from this repository. The attester is design-only: `docs/handoffs/frontier-attester-20261107/...` on branch `docs/frontier-attester-design-20261107` (d1f07ca2, slot 050000), and catch-up 051000 in `docs/design/bulk-catchup-publication-051000-20261107.md`. Both say not to invent the two digests. I did not query production. |

## Contract for each self-host row

### 1. `sessions.ingest_buffer_admission` (per org)

- **Who may write it:** only the migration owner (table owner), acting for the self-host
  tenant-lifecycle authority, which is the operator who creates and deletes orgs in
  relaycontrol. Never the publisher, the ingest edge or a request path.
- **What it binds:**
  - `org_id`.
  - `schema_generation`: must equal the migrate `generation()` and the current
    `__96bulls_role_validation` PASS, as checked by `ingest_require_buffer_admission`.
  - `privacy_baseline_sha256` and `retention_contract_sha256`: hashes of the
    deployment's reviewed privacy-fence baseline and retention contract.
  - `control_epoch`: the custody epoch.
  - It has no bucket, region or key-root column; those are bound by the frontier.
- **Idempotence:** `INSERT ... ON CONFLICT (org_id) DO NOTHING`, as `--admit-new-orgs`
  does. Never rewrite an existing row's hashes.
- **Renewal:** owner `UPDATE ... SET valid_until = <new>`. Do not re-insert.
- **Revocation or deactivation:** `UPDATE ... SET valid_until = clock_timestamp() - interval '1 second'`.
  Never `DELETE` (`docs/runbooks/buffered-cutover-20261105.md`, rollback step 2).
  Expired admission refuses new claims and publication (`55000`).
- **Self-host gap:** the hosted hashes come from the Neon export ceremony (S2). Self-host
  has no Neon source, so the operator must supply reviewed self-host privacy and
  retention documents to hash. There is no committed format for them yet.

### 2. Seat clock and custody (deployment-wide, plus per org)

- **Who may write it:** only the function owner, through `ingest_lazy_policy_set` and
  `ingest_receipt_seat_initialize`, or through the eager custody functions
  `ingest_custody_begin`, `_complete` and `_release` from 20261104054000, which the test
  fixtures use via `initializeFixtureSeatClock`.
- **What it binds:** `generation`, `controlEpoch`, `sourceIdentitySha256` and
  `sourceProofSha256`, plus an acceptance floor above every sequence value the legacy
  source allocated.
- **Idempotence:** the lazy policy is a singleton with immutable identity. The seat
  initializer refuses a different identity.
- **Revocation:** expire the policy or admission. Do not reset the clock: seats must stay
  monotonic.
- **Self-host gap (not supported today):** both supported mechanisms assume a legacy Neon
  source.
  - `ingest_lazy_policy_set` hard-requires `issuer = 'relayhistory-cloud'`.
  - The seat floor protocol needs real Neon sequence witnesses.
  - A fresh self-host has no prior source, and faking those witnesses is not acceptable.
  - What is needed is a reviewed "fresh install, no prior source" custody profile: a new
    issuer value and a floor of zero with an explicit empty-source statement. That is a
    schema change and needs its own registry slot.

### 3. `sessions.ingest_publication_frontiers` (per org, workspace, user, origin)

- **Who may write it:** only the migration owner, in an attended owner step, acting for the
  tenant-lifecycle authority that knows the user and origin exist and that every record
  of that origin is ingested through this deployment. Never the publisher.
  - The 050000 attester design's rule applies: a qualified owner ceremony first, and any
    automation later only through a separate narrow NOLOGIN boundary or RPC. No direct
    DML from a daemon.
- **What it binds:** `candidates` and `claim` compare each column exactly, and the
  heartbeat and publish RPCs compare the full captured row. GC tickets are bound
  separately, by the generation rows' copy of the same target. A row only authorizes
  publication when all of these match:
  - `schema_generation` = `INGEST_PUBLISH_EXPECT_GENERATION`
  - `target_bucket` = `INGEST_PUBLISH_BUCKET`
  - `target_region` = `INGEST_PUBLISH_S3_REGION`
  - `key_root` = `'96bulls'`
  - `valid_until > clock_timestamp()`

  A row written for another bucket, region, key root or generation authorizes nothing for
  this publisher. The e2e test `publication-portable.pglite.test.ts` proves the bucket
  and region cases.
- **The two digests:**
  - Hosted: `source_frontier_sha256` and `record_maxima_sha256` attest the complete
    Neon-to-S3 export frontier and the source-proven record maxima. Absence of a revision
    row is meaningful only under that attestation.
  - Self-host with no legacy source: the honest claim is "no records of this origin exist
    outside this deployment's own revision ledger". There is no committed encoding of that
    claim, and **no digest may be invented** (attester design, `.problem`).
- **Idempotence:** insert once per (org, workspace, user, origin), with
  `ON CONFLICT DO NOTHING`. Re-running the same attestation must not touch an existing row.
- **Renewal:** any `UPDATE` (to `valid_until`, a digest or the target) invalidates in-flight
  attempts and History plans, because they captured the full row. Renew only when no
  publication attempt or plan lease for the origin is live, under the user-fence lock and
  then the frontier row `FOR UPDATE` (attester design, `no_blind_ttl_touch`). Otherwise
  let the row expire.
- **Revocation or deactivation:**
  - `UPDATE ... SET valid_until = clock_timestamp() - interval '1 second'`. New claims
    refuse at once; live attempts fail at their next heartbeat or publish, and their
    registered objects stay erasable by GC.
  - Prefer expiry to `DELETE`, so the row stays as the record of what was attested
    (frontiers have no FK, so `DELETE` is also fail-closed).
  - Deleting a user or origin also goes through the existing upload-selection and reset
    controls (`ingest_set_session_uploads`, `ingest_reset_uploaded_sessions`), which retire
    publication generations independently of the frontier.
- **Generation change:** after a migrate changes `generation()`, rows for the old
  generation stop matching. The owner writes new-generation rows only after the new
  generation's PASS.

## Smallest owner-run step (proposal, not implemented)

There is no supported self-host mechanism for rows 2 and 3. Rather than invent marker
SQL, the smallest step is one attended psql script, run as the migration owner in one
transaction, with every value passed as `-v` variables that come from reviewed inputs:

1. `INSERT` the admission row per org (`ON CONFLICT (org_id) DO NOTHING`).
2. Initialize custody through a new, reviewed fresh-install profile (needs a migration;
   see the row 2 gap).
3. `INSERT` one frontier row per (org, workspace, user, origin), whose two digests are
   computed from a new, reviewed self-host attestation document. Its protocol name, fields
   and canonical encoding still have to be designed, and it binds generation, bucket,
   region and key root.
4. Read back and print row counts and hashes only.

Before writing that script, the frontier attester slot (050000) owner must decide the
self-host attestation format. Until then, a self-host stack ships the publisher enabled
but with nothing to publish, or dormant.

## GC stays off

Keep `INGEST_PUBLICATION_GC_ENABLED` unset or `false` on self-host until there is a
qualified plan for:

- MinIO bucket versioning: the redaction barrier refuses unversioned buckets with
  `version_unavailable`.
- A least-privilege policy for the GC principal (`s3:ListBucketVersions` and
  `s3:DeleteObjectVersion`, limited to `96bulls/*/upload-generations/*`).

GC is a separate process (`publication-main.ts --gc`) and is never started by
`SERVICE=session-publication`.
