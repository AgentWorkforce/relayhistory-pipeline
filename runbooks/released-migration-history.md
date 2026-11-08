# Upgrade a released history whose migration filenames changed

The services image
`ghcr.io/agentworkforce/96bulls-services@sha256:070fe6fd46ecd1e1f9db32ff22379f6df9b9e6c4d48ede5312c8fb986a95f3a9`
(source `f1c2672581b6f88d26d0549b4d1cb78fce0d5f16`) shipped two files
under names that were subsequently renamed as though they had not shipped.
An installed ledger from that image is genuine released history, not an
unledgered schema to import.

| Recorded name | Exact recorded SHA-256 | Current name |
| --- | --- | --- |
| `upstream:20261103010000_session_directory_event_metadata.sql` | `fe07b2ef2b3e18634b024f35f4272bee9100ea39d0383ce33e8c6d969b77a476` | `upstream:20261104021000_session_directory_event_metadata.sql` |
| `upstream:20261102010000_session_ingest_ledger.sql` | `21a0be7dc69afa7d3b1d1b27e31fee9778c1a6a0c3b20246e3029fa2f1356fa6` | `upstream:20261104022000_session_ingest_ledger.sql` |

The original SQL bytes are retained as test fixtures. Event metadata differs
only by two added comments. The ingest rename additionally removed the grants
on `ingest_seed_records(jsonb)` and `ingest_seed_fences(jsonb)` to the seed role.
The forward `20261104040000_ingest_s3_proof_seed.sql` repair explicitly revokes
and replaces these unsafe APIs. The historical comments in the renamed SQL
are inaccurate for this release; their immutable bytes are retained.

`released-migration-history.ts` recognizes only the exact two name/hash pairs
and exact current targets. It returns an effective view for migration ordering
and readiness. It never inserts a replacement ledger row, changes a recorded
hash/name, or marks an unexecuted migration as applied. Owner release plans
continue to hash the physical ledger, including the old names and checksums.
The alias registry is part of the migration generation, so deploying the fix
requires a matching new image/generation and its ordinary verifier run.

A wrong hash, unknown step, old/current name collision, gap in either lane,
changed current target or missing/changed forward seed repair refuses before
writes. Supabase CLI authority checks include both the original and current
version of every selected alias; either spelling refuses before changing the
physical ledger, data or readiness marker. All later migrations still execute in order. The marker becomes PASS
only after the complete ordinary function ACL and role verification. A failure
at the repair leaves the generation pending; a normal retry resumes from the
actual committed ledger. Readiness recognizes old names only after checking
their exact physical hashes and requires the full matching PASS generation
and live role verification as usual.

## Operator procedure

1. Save a database backup, complete physical ledger, role/ACL and object-store
   inventories. Confirm the database identity and each recorded hash from the
   installed image. Keep the existing database, object root and credentials.
2. Drain writers and queues according to the normal release contract. Use the
   exact new image/source/generation and the existing schema owner; do not
   grant that owner SUPERUSER, CREATEROLE or ownership of other databases to
   make this compatibility change work.
3. Run the normal privileged role prerequisite chain for the selected image,
   followed by the normal migrator and its configured function boundary. This
   compatibility path neither skips those prerequisites nor supplies runtime
   credentials. It does not change the consumer-bootstrap ownership contract.
4. Verify all expected schema steps through the effective history, matching
   generation/PASS, zero function ACL drift, unchanged original ledger rows,
   unchanged schema owner/owner attributes and retained seeded pointer/data.
   The two renamed names remain absent physically; that is expected.
5. Re-run the migrator: no schema migration should apply. Then qualify actual
   runtime logins, TLS, upload, canonical objects, pointer/search projection,
   reader routes and dashboard readback separately. Schema acceptance alone
   is not application or privacy/deletion acceptance.

Do not rewrite ledger rows, copy a checksum from another database, remove a
marker/guard, replay the renamed DDL over existing objects, or create a second
HoopDB schema to avoid this upgrade. An unrecognized history needs its own
source-reviewed procedure. Rolling schemas backward across this historical
boundary is not supplied here; retain backups and follow a separately reviewed
reverse procedure rather than deleting either ledger spelling by hand.

## Validation

The focused PGlite fixture loads the exact released 26-row history, retains a
seeded pointer, upgrades through all pending schema/role steps, checks the
unsafe grants are gone and the ACL manifest matches, verifies readiness under
the dedicated runtime, and proves a second migrate applies nothing. A fault
at the seed repair stays pending and resumes safely. Negative cases preserve
physical history and refuse before queries/writes.

The opt-in native test runs against an isolated, disposable loopback PostgreSQL
17 server with no project connection. It migrates the same history in place
as a genuine LOGIN schema owner with NOCREATEROLE/NOSUPERUSER, preserves owner
attributes, pointer and physical ledger rows, verifies exact ACLs/readiness,
and proves a changed historical checksum cannot mutate the marker. It is a
native migration/ACL proof; its trust-only local fixture does not qualify
password login or TLS, and is never an application deployment recipe.
