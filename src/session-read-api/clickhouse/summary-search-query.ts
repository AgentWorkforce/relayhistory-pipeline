/**
 * Session summary search over the ClickHouse `session_brief_documents` table
 * (session-pipeline/brief-documents-schema.ts), behind READ_API_SUMMARY_SEARCH_ENABLED=true.
 *
 * The table is a derived copy of each session_directory row's title and scrubbed
 * short_summary (the only writer of that column is hoopdb/session-directory-summary.ts,
 * which scrubs through the shared scrub engine), published by publishBriefDocumentsOnce
 * (session-pipeline/brief-documents.ts) from the migration 20261105024000 outbox.
 * ReplacingMergeTree(epoch), ORDER BY (org_id, workspace_id, source, session_id); a deletion
 * or a cleared summary is a newer blank tombstone document.
 *
 * ClickHouse is candidate discovery only. Like queries.ts, the text predicate is evaluated
 * twice: on ANY version to pick candidate keys (a superset, so the ngram index can prune),
 * then on the NEWEST version only, after `LIMIT 1 BY` collapse, so a summary that was
 * cleared, rewritten or tombstoned is never matched through a stale predecessor. The route
 * then hydrates every candidate from the Supabase directory (the authority) and re-checks
 * the match there (summaryMatches), so text that ClickHouse still holds but the directory
 * no longer does (an unpublished clear or erasure) is never served.
 *
 * Folding: the publisher stores `summary_folded`/`title_folded` as JavaScript
 * `toLowerCase()`; the query folds `q` the same way and matches case-sensitively on the
 * folded columns, so the two sides use one fold. Tenant ids come only from verified auth.
 * Read-only: one SELECT.
 */
import type { QueryParams } from "../../session-pipeline/vendor/search-index/clickhouse.ts";
import { BRIEF_DOCUMENTS_TABLE } from "../../session-pipeline/brief-documents-schema.ts";
import { qualifiedTable, type Built } from "../queries.ts";

/** The ngrams(3) text index answers only queries at least this long. */
export const SUMMARY_SEARCH_MIN_CHARS = 3;
/** Page size bound: every candidate is hydrated through one directory call (its 1..200 keys). */
export const SUMMARY_SEARCH_MAX_LIMIT = 50;
export const SUMMARY_SEARCH_DEFAULT_LIMIT = 20;
/**
 * Bound on each column branch of the candidate subquery (any version, newest activity first), so
 * a very common q never materialises an unbounded key set for the IN. Past it the search is
 * best-effort like the rest of the route: older matches may be missed.
 */
export const SUMMARY_SEARCH_MAX_CANDIDATES = 5_000;

export interface SummarySearchInput {
  orgId: string;
  workspaceId: string;
  /** Raw query text; folded here. */
  q: string;
  /** Matches returned; the query asks for limit + 1 to tell whether more exist. */
  limit: number;
}

export interface SummaryCandidateRow {
  source: string;
  session_id: string;
}

/** The one fold both the publisher and the reader apply. */
export function foldSummaryText(text: string): string {
  return text.toLowerCase();
}

/** The authoritative re-check on the hydrated directory row: the same fold, the same columns. */
export function summaryMatches(row: { title: string | null; short_summary: string | null }, q: string): boolean {
  const folded = foldSummaryText(q);
  return foldSummaryText(row.short_summary ?? "").includes(folded) || foldSummaryText(row.title ?? "").includes(folded);
}

/**
 * Newest-first candidate keys whose NEWEST document is live (not a tombstone) and whose
 * title or summary contains q. One branch per column so each column's text index is the
 * primary filter of its own scan (a skip index cannot prune under an OR spanning columns).
 */
export function buildSummarySearchQuery(database: string, input: SummarySearchInput): Built {
  const table = qualifiedTable(database, BRIEF_DOCUMENTS_TABLE);
  const params: QueryParams = { org: input.orgId, ws: input.workspaceId, qf: foldSummaryText(input.q), n: input.limit + 1, cap: SUMMARY_SEARCH_MAX_CANDIDATES };
  const tenant = "org_id = {org:String} AND workspace_id = {ws:String}";
  const branch = (column: "summary_folded" | "title_folded") =>
    `SELECT source, session_id FROM ${table} WHERE ${tenant} AND hasAllTokens(${column}, {qf:String}) AND position(${column}, {qf:String}) > 0 ORDER BY last_activity_at DESC, source DESC, session_id DESC LIMIT {cap:UInt32}`;
  const newest = `SELECT source, session_id, deleted, title_folded, summary_folded, last_activity_at FROM ${table}
WHERE ${tenant}
  AND (source, session_id) IN (SELECT source, session_id FROM (${branch("summary_folded")}) UNION ALL SELECT source, session_id FROM (${branch("title_folded")}))
ORDER BY source, session_id, epoch DESC
LIMIT 1 BY source, session_id`;
  const query = `SELECT source, session_id FROM (${newest})
WHERE deleted = 0 AND (position(summary_folded, {qf:String}) > 0 OR position(title_folded, {qf:String}) > 0)
ORDER BY last_activity_at DESC, source DESC, session_id DESC
LIMIT {n:UInt32}`;
  return { query, params };
}

/** ClickHouse UNKNOWN_TABLE: the owner has not applied briefDocumentSchemaStatements(). */
export const SUMMARY_SCHEMA_MISSING_CODES: ReadonlySet<string> = new Set(["60"]);
