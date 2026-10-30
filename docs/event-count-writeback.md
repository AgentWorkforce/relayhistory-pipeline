# Authoritative `event_count` write-back (issue #58)

Status: design checkpoint. No code, and no migration applied anywhere. Design
owner: mig-cutover-observability. Contract contributor: mig-e4-export-transform.

## Pinned inputs

| Input | Pin |
| --- | --- |
| Implementation base | `main` at `947d361bd1a37ab85bf892b84070f0c5ee18cad6` (carries #42) |
| #57 output contract | `b2e3de78e9468aa4c32791e7e00434ad0e57e462`, one commit on that base |

Implementation stacks on that #57 head. If #57 moves, re-check the contract
below before continuing. The two changes this design needs from #57 are under
"Format decision" and "Transform output"; neither is in that head yet.

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
put. An identical retry succeeds, but different bytes at that key are refused. So
an object left by one format would make the other fail on every retry until it
dead-letters. A reader also couldn't tell the formats apart from the key.

**Decision:**

- `{table, row}` from #57 is the only format. An envelope can't give this count:
  it holds records, not the rows the live triggers leave.
- #57 sets `RELAYHISTORY_NORMALIZED_VERSION = 2`, so the key becomes
  `normalized/v2/relayhistory-{raw.content_sha256}.ndjson`.
- `normalized/v1` is retired, not supported. No reader may accept it, and nothing
  translates it. No code on `main` constructs the transform, so no worker can have
  written v1. **Confirm before activation that no v1 object exists in any bucket.**
- Any change to projected bytes for the same input bumps the version. Otherwise it
  hits the same refusal.

## Flow

```
file-normalize: read manifest and chunks, project, publish the object (create-only)
  → enqueue normalized-ready (typed, ids and counts only)
normalized-ready: sessions.record_normalized_event_count(...)
```

The object is published first, so a record never names an object that isn't
there. If the enqueue fails, `file-normalize` fails and retries; the identical put
succeeds, and the record is sent again. The transform class keeps its object store
only. The stage handler around it does the send.

### Transform output

`normalize()` returns its existing `outputKey` plus what the record needs. Counts
come from the rows actually written, not from a second read.

```ts
interface NormalizedReady {
  v: 1;
  orgId: string; workspaceId: string; harness: string; sessionId: string; machineId: string;
  revision: number;            // manifest.revision
  normalizedVersion: number;   // 2
  workKey: string;             // the manifest version's work key
  contentSha256: string;       // manifest.raw.content_sha256
  objectKey: string;
  objectSha256: string;        // sha256 of the published bytes
  convergenceEvents: number;   // lines with table = convergence_events
  unprojected: number;         // projection.unprojected
}
```

The record is parsed strictly at consumption, and an invalid one is not retried.
Identity comes from `message.ctx` and must match `objectKeyIdentity(objectKey)`,
never from the payload alone.

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
RETURNS text  -- applied | replayed | stale | deleted
```

SECURITY DEFINER with a fixed `search_path`. It locks exactly one row by the full
primary key and touches nothing else. EXECUTE goes only to `session_count_writer`:
NOLOGIN, no BYPASSRLS, no table privileges, granted to the consuming worker's
login. PUBLIC, `anon` and `authenticated` get nothing.

**Limit:** the function can't authenticate a tenant. A holder of the role who
knows a session's full key, revision and `content_sha256` could write a count for
it. The tenant boundary is the worker taking identity from `ctx`, plus the match
on `content_sha256`, not the grant alone.

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

A count is only ever applied to the exact revision the row holds. Index and
normalize run on separate queues, so case 1 is normal: the count waits on retry
until the index catches up, and it leaves no orphan rows.

- **Decreases are allowed:** no rule compares counts, so a revision with deletes
  writes a lower number.
- **Replays are idempotent:** redelivery and DLQ replay land on 6, or on 3 once a
  newer revision has been indexed.
- **Late work is harmless:** an older revision lands on 3.
- **`event_count_recorded_at`** is written only on `applied`, so a replay changes
  no row.

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
2. The reader uses the predicate above and doesn't present other rows as a count.
3. The `normalized-ready` DLQ is empty, with lag under its alert threshold.
4. A rehearsal has shown a backfilled session, a later live revision and a
   delete-driven decrease all reaching the correct count.

If a reader needs this before then, making the column nullable (option B in #50)
becomes mandatory.

## Open questions

1. **Do live revisions keep the export marker?** `RelayhistoryExportTransform`
   returns nothing unless the manifest carries `client.relayhistory_export`. A
   post-backfill live revision that drops the marker, or a `resumed` session that
   never had it, gets no object and no count, so it stays non-authoritative
   forever. #58 requires every normalized revision to count. For
   mig-e4-export-transform, this needs a contract answer.
2. **Which sessions get normalized at all?** A session with no merged stream keeps
   the chunk sum and no provenance, so it fails the gate permanently. If a reader
   needs those, either they need a normalizer or the gate needs a stated exception.
3. **Which worker owns `file-normalize`?** No worker wires it today. On the Postgres
   queue, sending needs a credential. It must be limited to the queue, or the rule
   against broad transform credentials isn't met.
4. **Does `unprojected > 0` still count as exact?** Proposed: yes for the projected
   rows, since that is what the live triggers would leave.

## Implementation plan

1. #57 settles the two contract changes, and open questions 1 and 3 are answered.
2. Stack on the pinned #57 head: migration, function, trigger and stage registration.
3. PGlite tests against every migration: each row of the ordering table, a
   decrease, a replay, the sticky trigger against `upsert_session_index`, a
   non-owner role, and tenant-role denial.
4. Handler, typed parser, consumer and the DLQ path, with tests.
5. Add the stage to the sampler's alert stages once #47 lands.

Code and draft PR only. Applying the migration, enabling normalization and any
worker credential each need Khaliq's explicit OK.
