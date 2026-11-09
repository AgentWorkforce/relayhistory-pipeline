/**
 * The digest backend behind GET /v1/digest/daily: the deterministic half (stats, projects,
 * epics, sessions) over one of two facts stores, plus the readiness of the day window.
 *
 *   clickhouse  today's ClickHouse facts (clickhouse/digest-query.ts), unchanged; the default
 *   postgres    the self-host Postgres facts of migration 045020 (postgres/digest-query.ts),
 *               READ_API_DIGEST_BACKEND=postgres on the self-host service
 *
 * The narrative stays outside the backend (digest-narrative.ts, deps.digest), and so does
 * computeStatsHash: both backends return the same DailyDigest shape, so the hash, and the
 * narrative cache key it is part of, are the same function of the same facts.
 */
import type { ClickHouseClient } from "../session-pipeline/vendor/search-index/clickhouse.ts";
import type { FactsBackendKind } from "./config.ts";
import type { Directory } from "./directory.ts";
import { buildDailyDigest, type DailyDigest, type DigestParams } from "./clickhouse/digest-query.ts";
import { clickhouseOutputBackend, type FactsReadOptions, type OutputReadiness } from "./output-backend.ts";

export type { DailyDigest, DigestParams };

export interface DigestBackend {
  readonly kind: FactsBackendKind;
  /** The readiness of the day, [since, until) (the Output contract with `untilExclusive`). */
  windowReadiness(orgId: string, workspaceId: string, since: Date, until: Date, opts?: FactsReadOptions & { untilExclusive?: boolean }): Promise<OutputReadiness>;
  /** Throws DigestDirectoryUnavailable when the day has sessions and the directory cannot list them. */
  buildDailyDigest(directory: Directory | undefined, params: DigestParams, opts?: FactsReadOptions): Promise<DailyDigest>;
  isSchemaMissing(error: unknown): boolean;
}

/** Today's ClickHouse digest, exactly (the moved digest-query.ts and output-query.ts readiness). */
export function clickhouseDigestBackend(ch: ClickHouseClient): DigestBackend {
  const output = clickhouseOutputBackend(ch);
  return {
    kind: "clickhouse",
    windowReadiness: output.windowReadiness,
    buildDailyDigest: (directory, params, opts = {}) => buildDailyDigest(ch, directory, params, opts),
    isSchemaMissing: output.isSchemaMissing,
  };
}
