/** Canonical directory-document features — the third projection pool. */
export const DOCUMENT_FEATURES_TABLE = "session_document_features";
export function documentFeatureSchemaStatements(): string[] {
  return [
    `CREATE TABLE IF NOT EXISTS ${DOCUMENT_FEATURES_TABLE}(
 org_id String,workspace_id String,source LowCardinality(String),session_id String,
 version UInt64,source_revision UInt64,source_hash FixedString(64),deleted UInt8,
 title String CODEC(ZSTD(3)),summary String CODEC(ZSTD(3)),title_folded String CODEC(ZSTD(3)),summary_folded String CODEC(ZSTD(3)),
 project_id Nullable(String),project_folded Nullable(String),
 last_activity_at DateTime64(6,'UTC'),
 INDEX title_text title_folded TYPE text(tokenizer=ngrams(3)),
 INDEX summary_text summary_folded TYPE text(tokenizer=ngrams(3)),
 retention_days UInt32 DEFAULT 0
) ENGINE=ReplacingMergeTree(version)
PARTITION BY toYYYYMM(last_activity_at)
ORDER BY(org_id,workspace_id,source,session_id)
TTL toDateTime(last_activity_at) + toIntervalDay(retention_days) DELETE WHERE retention_days > 0
SETTINGS index_granularity=1024`,
  ];
}
