# Canonical feature activation gates

Applying the Supabase schema does not activate canonical ClickHouse search.
The composed source remains dormant until every gate below has reviewed
evidence. Do not enable a reader merely because tables or functions exist.

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
