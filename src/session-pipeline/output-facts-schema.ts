/**
 * ClickHouse DDL for Output (`GET /v1/sessions/output`) and the daily digest
 * (`GET /v1/digest/daily`) off Neon. Design: docs/output-digest-clickhouse-design.md.
 *
 * DORMANT SKELETON. Nothing applies these statements: no deploy, migration
 * runner or Worker imports this module. It exists so the shape is reviewed and
 * exercised against a real ClickHouse in CI before any producer is written.
 *
 * Why sibling tables, not new columns on `transcript_events`: that table's DDL
 * is a verbatim copy owned by relayhistory-cloud (vendor/search-index/SOURCE.json,
 * "never edit these files here"), and its reconcile fingerprint covers exactly
 * its own columns. Adding usage columns there would fork the vendored schema and
 * make every existing row look changed.
 *
 *   transcript_event_facts  one row per projected event (the same identity as
 *                           transcript_events), with what the digest aggregates:
 *                           project, branch, model, tokens, cost, files touched.
 *   session_output_facts    message identities (deduplicated at read time) and
 *                           commit candidates (prefix-resolved and deduplicated at
 *                           read time), plus session_outcomes rows for the
 *                           digest's committed/reverted/unknown classification.
 *
 * Both are rebuildable indexes over the canonical S3 streams, written by the
 * same scope reconcile as transcript_events (session-pipeline/search-index.ts):
 * ReplacingMergeTree(version), version = epoch ms of the read, readers collapse
 * with FINAL or LIMIT 1 BY identity ORDER BY version DESC, erasure by
 * lightweight DELETE of the scope.
 *
 * Time is DateTime64(6): Neon's timestamptz is microsecond, and both routes
 * compare event times with caller instants (`ts <= until`, half-open days), so
 * millisecond truncation could move a boundary event across the window.
 *
 * Retention: tenant-first sort keys and monthly partitions keep per-org,
 * per-period deletes cheap. `retention_days` (0 = keep) is the hook for a
 * per-plan TTL; no TTL is active today. Enabling one is a reviewed ALTER:
 *   ALTER TABLE <t> MODIFY TTL toDateTime(ts) + toIntervalDay(retention_days)
 *     WHERE retention_days > 0
 */

export const EVENT_FACTS_TABLE = "transcript_event_facts";
export const OUTPUT_FACTS_TABLE = "session_output_facts";

export const OUTPUT_FACTS_SCHEMA_VERSION = 1;

/** `session_output_facts.fact_kind` values. */
export const OUTPUT_FACT_KINDS = ["message", "legacy_turn_message", "commit", "outcome"] as const;
/** `session_output_facts.origin` values for `fact_kind = 'commit'`. */
export const COMMIT_ORIGINS = ["commit_link", "outcome", "tool_result"] as const;

/** DDL in apply order. Every statement is idempotent (IF NOT EXISTS). */
export function outputFactsSchemaStatements(): string[] {
  return [
    `CREATE TABLE IF NOT EXISTS ${EVENT_FACTS_TABLE}
(
    org_id                 LowCardinality(String),
    workspace_id           LowCardinality(String),
    ts                     DateTime64(6, 'UTC'),
    session_id             String,
    source                 LowCardinality(String),
    machine_id             String,
    kind                   LowCardinality(String),
    event_id               String,
    -- NULL is a real digest bucket (spec section 0), distinct from ''.
    project_id             Nullable(String),
    -- task_ref->>'id' (display only) and the branch after its FIRST '@' (rollups.ts branchSql).
    task_ref_id            Nullable(String),
    branch                 Nullable(String),
    model                  Nullable(String),
    provider               Nullable(String),
    input_tokens           Int64,
    output_tokens          Int64,
    reasoning_tokens       Int64,
    cache_read_tokens      Int64,
    cache_create_tokens    Int64,
    cache_create_5m_tokens Nullable(Int64),
    cache_create_1h_tokens Nullable(Int64),
    -- NULL = no cost reported (relayhistory 0021); sum() skips it as Postgres does.
    cost_usd_micros        Nullable(Int64),
    files_touched          Array(String),
    retention_days         UInt16 DEFAULT 0,
    fingerprint            String,
    version                UInt64,
    INDEX session_bf session_id TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX project_bf project_id TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(ts)
ORDER BY (org_id, workspace_id, ts, session_id, source, machine_id, kind, event_id)
-- TTL toDateTime(ts) + toIntervalDay(retention_days) WHERE retention_days > 0
SETTINGS index_granularity = 1024`,
    `CREATE TABLE IF NOT EXISTS ${OUTPUT_FACTS_TABLE}
(
    org_id         LowCardinality(String),
    workspace_id   LowCardinality(String),
    fact_kind      LowCardinality(String),
    -- message/legacy_turn_message/tool_result: the event time (window-filtered before dedup).
    -- commit_link: coalesce(linked_at, received_at); outcome: coalesce(shipped_at, created_at).
    ts             DateTime64(6, 'UTC'),
    source         LowCardinality(String),
    session_id     String,
    -- message: coalesce(request_key, payload.message_id, delivery_record_id, machine_id:event_id);
    -- legacy_turn_message: the turn id; commit/outcome: lower(commit_sha), possibly abbreviated.
    fact_key       String,
    origin         LowCardinality(String),
    -- Unique within (scope, fact_kind, origin): event_id, link record id, outcome sha, ...
    row_key        String,
    -- Per-candidate numstat totals normalized as session-output.ts does; NULL = not counted.
    added          Nullable(UInt64),
    removed        Nullable(UInt64),
    -- outcome rows only: session_outcomes.reverted.
    reverted       UInt8 DEFAULT 0,
    retention_days UInt16 DEFAULT 0,
    fingerprint    String,
    version        UInt64,
    INDEX session_bf session_id TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX key_bf fact_key TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(ts)
ORDER BY (org_id, workspace_id, fact_kind, ts, source, session_id, fact_key, origin, row_key)
-- TTL toDateTime(ts) + toIntervalDay(retention_days) WHERE retention_days > 0
SETTINGS index_granularity = 1024`,
  ];
}
