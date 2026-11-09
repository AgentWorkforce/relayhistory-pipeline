/**
 * The Output backend behind GET /v1/sessions/output (and the readiness the digest shares):
 * one interface, two stores.
 *
 *   clickhouse  today's ClickHouse facts (clickhouse/output-query.ts), unchanged; the default
 *   postgres    the self-host Postgres facts of migration 045020 (postgres/output-query.ts),
 *               READ_API_OUTPUT_BACKEND=postgres on the self-host service
 *
 * Readiness codes and their order are the contract (output-query.ts outputWindowReadiness):
 * backfill_incomplete, before_watermark (microsecond compare), session_partial,
 * session_unindexed, ready. A backend answers isSchemaMissing for its own "the facts DDL is
 * not applied" errors, which the route turns into 501 `*_schema_missing`.
 */
import type { ClickHouseClient } from "../session-pipeline/vendor/search-index/clickhouse.ts";
import type { FactsBackendKind } from "./config.ts";
import {
  isOutputSchemaShort,
  outputWindowReadiness,
  readSessionOutput,
  type OutputReadiness,
  type SessionOutput,
  type SessionOutputFilter,
  type SessionOutputScope,
} from "./clickhouse/output-query.ts";

export type { OutputReadiness, SessionOutput, SessionOutputFilter, SessionOutputScope };

/** Per-request bounds: `settings` are ClickHouse query settings (ignored by Postgres); `timeoutMs` the read deadline. */
export interface FactsReadOptions {
  timeoutMs?: number;
  settings?: Record<string, string | number>;
}

export interface OutputBackend {
  readonly kind: FactsBackendKind;
  /** Whether the facts answer the window exactly. Output: [since, until]; `untilExclusive` (the digest): [since, until). */
  windowReadiness(orgId: string, workspaceId: string, since: Date, until: Date, opts?: FactsReadOptions & { untilExclusive?: boolean }): Promise<OutputReadiness>;
  readOutput(scope: SessionOutputScope, filter: SessionOutputFilter, opts?: FactsReadOptions): Promise<SessionOutput>;
  /** The error says the facts tables/functions (or a column) are not applied: a 501, not a 500. */
  isSchemaMissing(error: unknown): boolean;
}

/** Today's ClickHouse Output, exactly (the moved output-query.ts). */
export function clickhouseOutputBackend(ch: ClickHouseClient): OutputBackend {
  return {
    kind: "clickhouse",
    windowReadiness: (orgId, workspaceId, since, until, opts = {}) => outputWindowReadiness(ch, orgId, workspaceId, since, until, opts),
    readOutput: (scope, filter, opts = {}) => readSessionOutput(ch, scope, filter, opts),
    isSchemaMissing: isOutputSchemaShort,
  };
}
