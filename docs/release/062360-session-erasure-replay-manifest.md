# Slot 062360: session erasure replay-target manifest

Status: code only, on `review/session-erase-manifest-062360`, stacked on the consumer's frozen unit `f98d54af`
(`review/backlog-ops-bridge-main-fix`) with main `6d60dc4f` merged. Registry reservation receipt 252139230903828480.
Design: `docs/design/operator-erase-replay-target-manifest.md` on `review/hard-delete-owner-replay-target-manifest-design`
(hard-delete-owner). Applying it to any real database needs Khaliq's explicit OK. Raw-identifier retention is an open
owner decision (design section 8): this slot adds no TTL, GC or cleanup and claims no retention policy.

**This is a partial increment.** It proves the targets a session erase selected, not the erase's effects. It ships no
effect provenance, so the bridge co-read stays `effect_mapping_missing` with a null `binding_sha256`. Nothing here
makes `bridge_complete` possible for an org with session erasures in its window.

## Why

The SQL erase discovers aliases, owners and origins before it removes them. After the erase, nothing that survives
(the receipt, the alias fences, S3 owner stubs, `session_erasure_scopes`) can rebuild that set. Index-only and
directory-only owners are absent from the stubs. So the 062320 hold bridge answers `session_manifest_missing` for
every session erasure.

## What it adds

- Four owner-only, RLS-on, immutable tables (UPDATE, DELETE and TRUNCATE refused):
  - `session_erasure_replay_requests`: the intent. It holds a manifest UUID, the accepted receipt's `request_id`
    (bound by a compound foreign key to the receipt), the tenant, the source, the requested session and the capture
    version.
  - `session_erasure_replay_aliases`: the original alias declaration A0, one row per fence identity. Each row has a
    foreign key to its fence.
  - `session_erasure_replay_targets`: T = {(u, o, s, a) | u in U, o in O(u), a in A0}. Each row has a compound foreign
    key to its alias's manifest.
  - `session_erasure_replay_completions`: one seal per manifest. It holds the counts and the canonical digest. The
    seal trigger recomputes both from the rows under the parent row's lock, so a forged or partial seal is refused. A
    child insert into a sealed manifest is refused. The child takes the parent FOR SHARE, which conflicts with the
    seal's FOR UPDATE.
- One added constraint on `session_erasure_receipts`: `UNIQUE (org_id, workspace_id, source, session_id,
  request_id)`. It is a superset of the primary key and writes no rows.
- The fence body moves into `session_erasure_replay_fence(..., p_manifest uuid)`. With `NULL` it runs the legacy
  derivation, statement for statement. 050000's `ingest_operator_erase_fence` becomes that NULL-manifest wrapper.
  With a manifest, T is materialized once, after owner-lock convergence and under the owner locks. It is sealed
  before the first `ingest_apply_selection`, and the sealed rows then drive the selection loop, in the legacy order.
- `ingest_operator_erase_session` (050000 body) changes in three ways:
  - 062320's marked erasure check is its first statement. This matters for a direct owner call; through
    `runtime_begin` the re-take is reentrant.
  - An intent is written only by the call that inserted the receipt, and only when none of the closure's aliases was
    fenced before this call.
  - The intent is written at the fences' boundary, outside the RH010 block.
- `session_erasure_replay_read(org, workspace, source, fence_session, after_cursor, page_size)`: the design 6.1 v1
  reader. It is owner-only, STABLE and SECURITY INVOKER, with keyset pages of 1 to 512. Every page repeats the binding,
  the count and the digest, and the reader recomputes them from the rows on every page. Its closed reasons:
  `manifest_missing` (no binding), `manifest_unsealed` (an intent without a seal, which covers an empty owner set and
  any unwind or retry), `manifest_binding_invalid`, `manifest_format_unsupported`, `manifest_integrity_invalid`,
  `manifest_cursor_invalid` and `reader_input_invalid`. `manifest_empty_unproven` is reachable only with affirmative
  empty provenance, which does not exist.
- `ERASURE_PROOF_TABLES` in `src/session-ingest/erasure-window.ts` lists the four tables exactly, as proof storage,
  not erasure sources. They carry no 062340 recorder, so a manifest write never changes the release watermark. An
  erase's own source writes are recorded exactly as before.

## What never seals

The reader reports each of these as `manifest_missing` or `manifest_unsealed`, and the erase's own behaviour is
unchanged in every case:
- a retried (failed) receipt;
- an erase whose RH010 unwound the capture together with the cleanup;
- an erase whose owner set is empty;
- a closure that reaches an alias fenced before this call, whether by a pre-062360 erase or another manifest;
- every pre-062360 erase. There is no historical backfill.

## Digest (frozen with the consumer before any v1 data)

The digest is SHA-256 over these bytes, in order:
1. The raw ASCII bytes `rh-erase-replay-manifest/v1` (not framed).
2. The following fields, each framed as a big-endian uint32 octet length followed by its UTF-8 bytes:
   - capture version, as canonical decimal;
   - manifest UUID, lowercase;
   - accepted request;
   - org, workspace, source, requested session;
   - the alias count, as canonical decimal, then the aliases in bytewise order;
   - the target count, as canonical decimal, then each target's four fields (user, origin, source, session), with the
     targets in bytewise order.

The SQL implementation is `session_erasure_replay_digest(uuid)`, and the TS twin is
`src/hoopdb/session-erasure-replay-digest.ts`. The vectors are in
`src/hoopdb/__tests__/fixtures/session-erasure-replay-digest-vectors.json`. They cover the minimal case (with its exact
message bytes), out-of-order input, non-ASCII ids, bytewise-vs-locale traps, an empty workspace and counts of 10 or
more.

## Down

The generated, checksum-pinned down restores 050000's two bodies exactly and drops the captured fence body. 062320 and
062340 bodies are untouched. What happens to the tables depends on their contents:
- **All four tables empty:** the down drops them with their functions and the receipts constraint. That is exactly
  the pre-062360 schema, so 050000's chain down is not stranded behind a new dependency.
- **Any row:** the down keeps the witness (tables, foreign keys, guards, digest, reader) and verifies it against the
  pinned shape. A later reapply checks the same shape before reusing it (55000 if it has drifted).

050000's down already refuses while any receipt exists, and every manifest row implies a receipt. Dropping a parent
table without CASCADE fails with 2BP01.

## Evidence

- PGlite: `session-erasure-replay-manifest.pglite.test.ts`. Among other things it checks that:
  - T equals the `upload_deletions` effect set;
  - effects, fences, ownership, index, receipt, events and 062340 history are identical to the pre-062360 erase;
  - the retry, overlap, pre-fence and empty cases never seal;
  - the immutability guards hold and a forged seal is refused;
  - the reader's cursors are validated;
  - the tables and reader are owner-only;
  - the digest vectors match in SQL, in TS and byte by byte;
  - empty and populated down and reapply work.
- Native PostgreSQL 15, 16 and 17: `session-erasure-replay-manifest.postgres.test.ts`, run through the hold harness.
  It covers:
  - the seal is visible only at commit, and a rollback leaves nothing behind;
  - a first-call race;
  - RH010 retries;
  - org SHARED before the global gate on both entry paths, in both blocking directions, proved from `pg_locks`;
  - the compound-FK down and reapply.

## Open gates

- The raw-ID retention decision is with Khaliq (through b1).
- The kernel has not yet specified the effect-provenance association. Until it does, the co-read returns
  `effect_mapping_missing`.
- The equivalence review with layered-sessions-pipeline, and the second review with d5-gate-owner.
- Any apply to a real database.
