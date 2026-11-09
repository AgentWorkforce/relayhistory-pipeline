# Post-cutover all-org daily ACK observation

One run, SQL SHA `004e9baa07cecfcf663848c0bd421cafdc94aecd3d5310a8319b3c5ba4c792a6`, helper SHA `07763dd774e41fd0f6ea72d7600160ccef277759471fb76a6026ca1fdb651f06`. Existing sf-frame owner adapter route at source `f9409ccb9e46c3f100cf3017847d8ed62bc716c7`; `BEGIN READ ONLY`, `SET LOCAL ROLE postgres`, 3 s statement timeout, 250 ms lock timeout, rollback. Aggregate at 2026-11-07T20:33:34.775Z through 2026-11-07T20:33:35.100Z. Owner/login/database/read-only/table-owner identity, SELECT and inactive RLS all qualified. No write or retry.

Bound interval: `[2026-11-05T05:33:45Z, 2026-11-07T20:00:00Z)`. The end was frozen to the run hour in UTC. The exact SQL supplied by the release owner was hash-checked before use.

Verbatim driver result rows:

```json
[
  {
    "aggregate": {
      "days": [
        {
          "day_utc": "2026-11-05",
          "staged_ack_batches": 58822,
          "durable_ack_batches": 58822,
          "suppressed_ack_batches": 0,
          "still_buffered_ack_batches": 58822,
          "newest_ingest_ack_age_at_end": "1to7d",
          "canonical_claim_or_publish_staged_batches": 0
        },
        {
          "day_utc": "2026-11-06",
          "staged_ack_batches": 90414,
          "durable_ack_batches": 90414,
          "suppressed_ack_batches": 0,
          "still_buffered_ack_batches": 90414,
          "newest_ingest_ack_age_at_end": "1to24h",
          "canonical_claim_or_publish_staged_batches": 0
        },
        {
          "day_utc": "2026-11-07",
          "staged_ack_batches": 85001,
          "durable_ack_batches": 85001,
          "suppressed_ack_batches": 0,
          "still_buffered_ack_batches": 85001,
          "newest_ingest_ack_age_at_end": "lt1h",
          "canonical_claim_or_publish_staged_batches": 0
        }
      ],
      "input_scope_valid": true
    }
  }
]
```

Result receipt SHA-256: `1b352675e8facd6116d2f9780c34ac98aeccdf2da81e71bb912bc605fce19ff1`.

The three daily current-row counts sum to 234,237 staged committed ACK batches; every counted row is still marked buffered. This is an aggregate over current visible `sessions.ingest_batches` rows, not immutable history, a physical S3 object/version inventory, an archive/recovery proof, per-person lineage or a canonical readback. The `canonical_claim_or_publish_staged_batches` column is not publication proof even if nonzero, because claiming changes the state first. Do not compare counts from separate store snapshots as exact lineage. The paused pre-cutover replica ACK backlog is a separate cohort.

## Retained Neon pin requested by lead

Project `divine-snow-84812652`, production branch `br-damp-mud-apbr6ifp`, database `neondb`, endpoint `ep-floral-pond-apkvat38.c-7.us-east-1.aws.neon.tech:5432`. Historical fixed `neonctl connection-string` producer selected **`neondb_owner`**, used in bounded READ ONLY transactions. This is an owner principal, not a restricted read-only role. Provider metadata was captured November 4; neither current authorization nor current access was revalidated. `retained-history-neon-readonly-pin.json` contains metadata provenance and exact historical argv without any credential. No Neon producer or query ran in this task; the pin does not authorize a new operation by itself.
