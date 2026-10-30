# Authoritative `event_count` write-back (issue #58)

Status: design checkpoint. No code, and no migration applied anywhere. Design
owner: mig-cutover-observability. Contract contributor: mig-e4-export-transform.

## Pinned inputs

| Input | Pin |
| --- | --- |
| Implementation base | `main` at `947d361bd1a37ab85bf892b84070f0c5ee18cad6` (carries #42) |
| #57 output contract | Candidate `bd57d4ff5d13cd74b9dafaea9b8b1e1f869e4df7`, on that base. Draft and unreviewed; the coordinator confirms the pin |

The contract itself is in the #58 comment pinned to that head, and this doc follows
its names. Until the coordinator confirms the pin, the migration, function, trigger
and their tests build on `main` at the pin above, and the handler and consumer wait.

## Problem

`session_index.event_count` is `integer NOT NULL DEFAULT 0`. For a session with a
`backfill` or `resumed` epoch, #50 stops the index stage summing
`chunks[].records`, because that sum includes superseded revisions and deletes.
The column then holds `0` or a stale count. The real number is the count of
projected `convergence_events`, known only after normalization, and the transform
has no database access.

## Format decision: `{table, row}` is canonical, under `normalized/v2`

`main` (#42) writes record envelopes, and #57 writes `{table, row}` lines. **Both
use `normalized/v1/relayhistory-{raw.content_sha256}.ndjson`,** with a create-only
put, so an object left by one format would block the other, and a reader couldn't
tell them apart from the key.

**Decision:**

- `{table, row}` from #57 is the only format. An envelope can't give this count:
  it holds records, not the rows the live triggers leave.
- `RELAYHISTORY_NORMALIZED_VERSION = 2`, so the key becomes
  `normalized/v2/relayhistory-{raw.content_sha256}.ndjson`. Done at the candidate
  #57 head. An object at that key that isn't `{table, row}` is refused.
- `normalized/v1` is retired, not supported. No reader may accept it, and nothing
  translates it. No code on `main` constructs the transform, so no worker can have
  written v1. A read-only check of the buckets is on the activation checklist.
- Any code change to projected bytes for the same input bumps the version. Without
  a bump the new bytes aren't stored: the existing object is kept and the result is
  flagged `output_divergent`.

## Flow

```
file-normalize: read manifest and chunks, project, publish the object (create-only)
  → enqueue normalized-ready (typed, ids and counts only)
normalized-ready: sessions.record_normalized_event_count(...)
```

The object is published first, so a record never names an object that isn't
there. If the enqueue fails, `file-normalize` fails and retries; the create hits the
existing object, and the record is sent again. The transform class keeps its object store
only. The stage handler around it does the send.

### Transform output

`normalize(pointer, {revision, workKey})` returns one of three results (#57):

- **`NormalizedOutput`:** send the record.
- **`{skipped: "stale_manifest"}`:** a delayed message. Nothing was published, no
  record is sent, and that version's own event normalizes it.
- **`{}`:** nothing to normalize, such as a harness log. No record is sent.

The record, using the contract's names:

```ts
interface NormalizedReady {
  v: 1;
  // From the authenticated TraceContext, never the manifest body.
  org_id: string; workspace_id: string; harness: string; session_id: string; machine_id: string;
  // From the stage message, never object metadata.
  manifest_key: string; manifest_revision: number; work_key: string; trace_id: string;
  source_content_sha256: string;
  normalized_version: number;      // 2
  output_key: string;
  output_sha256: string;           // of the stored object
  output_bytes: number;
  convergence_event_count: number; // table = convergence_events lines in the stored object
  unprojected_records: number;     // from this run's projection
  output_reused: boolean;
  output_divergent: boolean;
}
```

Count and checksum describe the stored object. On a create that's the bytes just
written, and on a hit the transform reads the object back, so the record is true of
what readers will see. The key is content-addressed, so a new revision with
identical content reuses the object, whose metadata names the first revision. That
is why `manifest_revision` comes from the stage message.

The count is per machine stream, matching the `session_index` key, not per logical
session. It is exact for projected rows when `unprojected_records > 0`, because
those records have no `convergence_events` row live either.

The record is parsed strictly at consumption, and an invalid one is not retried.
Identity must also match `objectKeyIdentity(manifest_key)`.

## Schema

One migration, not applied anywhere until separately approved:

```sql
ALTER TABLE sessions.session_index
  ADD COLUMN event_count_revision bigint,          -- NULL: never authoritative
  ADD COLUMN event_count_normalized_version integer,
  ADD COLUMN event_count_work_key text,
  ADD COLUMN event_count_object_sha256 bytea,
  ADD COLUMN event_count_recorded_at timestamptz;
```

- **On `session_index`, not a side table:** the count and its fence change in one
  row update, and the reader gate needs no join. Nullable additions don't rewrite
  the table.
- **`session_index_event_count_sticky`,** a BEFORE UPDATE trigger like
  `session_index_scrub_sticky`: once `event_count_revision` is set, any write that
  doesn't change the provenance keeps `OLD.event_count`. Without it, the index
  stage's chunk sum for the next live revision replaces the authoritative count.
- **Stage:** `normalized-ready` joins the registry and the `stage` CHECKs on
  `pipeline_jobs`, `pipeline_events` and `pipeline_dead_letters`. Check on PGlite
  whether the CHECK can be added `NOT VALID` on the partitioned table, to avoid a
  scan under lock.

Provenance columns are readable wherever `session_index` is. `work_key` repeats
`manifest_key` plus a version, so it exposes nothing new.

## Database function

```sql
sessions.record_normalized_event_count(
  p_org_id, p_workspace_id, p_harness, p_session_id, p_machine_id,
  p_revision bigint, p_normalized_version integer, p_work_key text,
  p_content_sha256 bytea, p_object_sha256 bytea, p_event_count integer)
-- p_object_sha256 is output_sha256, p_event_count is convergence_event_count
RETURNS text  -- applied | replayed | stale | deleted
```

SECURITY DEFINER with a fixed `search_path`. It raises `CN003` if RLS would hide
the row from it, because that would otherwise read as "index behind". It locks exactly one row by the full
primary key and touches nothing else. EXECUTE goes only to `session_count_writer`:
NOLOGIN, no BYPASSRLS, no table privileges, granted to the consuming worker's
login. PUBLIC, `anon` and `authenticated` get nothing.

**Limit:** the function can't authenticate a tenant. A holder of the role who
knows a session's full key, revision and `content_sha256` could write a count for
it. The tenant boundary is the worker taking identity from `ctx`, plus the match
on `content_sha256`, not the grant alone. helm-chart-chief is reviewing this
boundary.

## Revision ordering

`p_revision` is compared with the row, under its lock, in this order:

| # | Condition | Result |
| --- | --- | --- |
| 1 | No row, or `p_revision > revision` | Raise `CN001` (`count_index_behind`), retryable |
| 2 | `deleted_at` is set | `deleted`, no write |
| 3 | `p_revision < revision` | `stale`, no write |
| 4 | `manifest_key` isn't the key in `p_work_key`, or `content_sha256` differs | Raise `CN002` (`count_conflict`), not retryable |
| 5 | Same revision, stored `event_count_normalized_version > p_normalized_version` | `stale`, no write |
| 6 | Same revision and version, same `object_sha256` and count | `replayed`, no write |
| 7 | Same revision and version, different object or count | Raise `CN002` |
| 8 | Otherwise | `applied`: write the count and all provenance |

The #58 contract allows a row at or below `manifest_revision` when the content
matches. This is stricter on purpose: a row behind the record waits on case 1.

A count is only ever applied to the exact revision the row holds. Index and
normalize run on separate queues, so case 1 is normal: the count waits on retry
until the index catches up, and it leaves no orphan rows.

- **Decreases are allowed:** no rule compares counts, so a revision with deletes
  writes a lower number.
- **Replays are idempotent:** redelivery and DLQ replay land on 6, or on 3 once a
  newer revision has been indexed.
- **Late work is harmless:** an older revision lands on 3.
- **Identical content on a new revision applies** under 8, with the same checksum
  and the new revision. Otherwise that session would fail the gate.
- **Divergent output never reaches the function.** The consumer refuses a record
  with `output_divergent = true` as `count_conflict`. On a hit the checksum is
  always the stored object's, so rule 7 can't detect this case; it stays as a guard
  against a replaced object.
- **`event_count_recorded_at`** is written only on `applied`, so a replay changes
  no row.

## The `file-normalize` worker

The pipeline owner owns this worker (ruling, 2026-10-31). It doesn't exist yet.
Its credential is **queue-limited: claim, ack and retry on `file-normalize`, and
send to `normalized-ready`, with no table access.**

- **Role:** `file_normalize_worker`, NOLOGIN, no BYPASSRLS, no privileges on any
  table. No read of `session_index`, no write of it, and no EXECUTE on
  `record_normalized_event_count`.
- **Queue access through functions only.** `pollPostgresStage` and
  `postgresPipelineQueue` run SQL directly on `pipeline_jobs`, so this role can't
  use them as they stand. SECURITY DEFINER functions take over each operation,
  with the stage fixed inside the function, not passed by the caller: claim, start,
  ack and retry on `file-normalize`, and enqueue on `normalized-ready`.
- **Events and dead letters follow the same rule.** `handleStageQueue` also writes
  `pipeline_events` and `pipeline_dead_letters`. These need functions fixed to this
  stage too, or a log-only sink, which would lose quarantine. Quarantine must keep
  working, so functions are the requirement.
- **Object store:** its own IAM, separate from this role. Decided with the worker.

This is a spec. Building those functions and the worker is separate work.

## Failure and DLQ

`normalized-ready` is a normal queued stage: 6 attempts, then the DLQ, with the
same replay path. Only `applied` makes a count authoritative. `file-normalize`
`ok` means the object exists and claims nothing about the count.

| Failure | Code | Behaviour |
| --- | --- | --- |
| Index behind | `count_index_behind` | Retries with backoff, then DLQ. The DLQ means indexing is stuck |
| Database unavailable | database code | Retries, then DLQ |
| Bad record | `invalid_normalized_ready` | DLQ at once |
| Mismatch | `count_conflict` | DLQ at once, and nothing is written |
| `output_divergent` | `count_conflict` | DLQ at once, and nothing is written. The session fails the gate until a version bump or a fix and replay. Changed runtime Unicode tables can cause this too |
| Marker missing | `relayhistory_export_marker_missing` | `file-normalize` refuses without retries, so no record exists |

## Metrics

The stage flows through `pipeline_events` and the cutover sampler (#47) with no
new emitter. `QUEUED_STAGES` gives it queue, lag and DLQ panels, and
`pipeline.stage.errors` reports the codes above. Each outcome is recorded on the
stage event as `outcome`, an allowlisted field.

| Signal | Source | Alert |
| --- | --- | --- |
| `normalized-ready` lag and depth | `pipeline_jobs` | Lag ≥ 300 s |
| `normalized-ready` DLQ depth | `pipeline_dead_letters` | Any. Add the stage to the sampler's alert stages |
| `count_conflict` | `pipeline.stage.errors` | Any: a broken determinism or version rule |
| `stale` rate | `outcome` | Diagnostic |
| `unprojected` > 0 | Stage event | Diagnostic: dropped records are excluded from the count |
| Sessions not authoritative | Gate query below | Must reach 0 before activation |

The gate query scans `session_index`, so it runs by hand, not in the sampler.

## Reader activation gate

No reader of `event_count` exists, and all stay gated. A value is authoritative
only when:

```sql
event_count_revision = revision AND deleted_at IS NULL
```

A newer revision that hasn't been counted yet fails this on its own, which is the
correct answer while normalization runs or a DLQ waits.

A reader may ship only when all of these hold:

1. This work is merged and its migration applied, with its own approval.
2. A read-only listing shows no `normalized/v1` object in any bucket. This is a
   live check, so it needs Khaliq's explicit OK.
3. The reader uses the predicate above and doesn't present other rows as a count.
4. The `normalized-ready` DLQ is empty, with lag under its alert threshold.
5. A rehearsal has shown a backfilled session, a later live revision and a
   delete-driven decrease all reaching the correct count.

If a reader needs this before then, making the column nullable (option B in #50)
becomes mandatory.

The marker question is closed (#58 contract). `client.relayhistory_export` persists
across revisions, a backfill without one is refused rather than skipped, and a
`resumed` epoch always follows a backfill. `resumed` isn't in `main`'s enum until #46.

## Open questions

1. **Which sessions get normalized at all?** A harness log gets `{}`, so it keeps
   the chunk sum and no provenance, so it fails the gate permanently. If a reader
   needs those, either they need a normalizer or the gate needs a stated exception.

## Implementation plan

1. On `main` at the pin: migration, function and trigger, with PGlite tests against
   every migration. They cover each row of the ordering table, a decrease, a
   replay, the sticky trigger against `upsert_session_index`, a non-owner role and
   tenant-role denial.
2. Repin to the reviewed #57 head when the coordinator confirms it.
3. Stage registration, handler, typed parser, consumer and the DLQ path, with tests.
4. The `file-normalize` worker and its queue functions.
5. Add the stage to the sampler's alert stages once #47 lands.

Code and draft PR only. Applying the migration, enabling normalization and any
worker credential each need Khaliq's explicit OK.
