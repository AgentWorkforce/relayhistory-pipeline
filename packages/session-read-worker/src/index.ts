/**
 * The 96Bulls session read API as a Cloudflare Worker: the same app as the
 * #135 Lambda (src/session-read-api: routes, auth, cursors, the two-step
 * ClickHouse dedupe, the history floor and erasure), with a Worker adapter in
 * place of lambda.ts. Nothing on the request path runs on Lambda.
 *
 *   ClickHouse  ClickHouse Cloud over HTTPS with the dedicated SELECT-only read
 *               user (Worker secrets CLICKHOUSE_URL / _USER / _PASSWORD), the
 *               relayhistory-cloud search-clickhouse.ts request bounds.
 *   Supabase    the `bulls96_read_api` role through Hyperdrive (binding
 *               SUPABASE, caching disabled, verify-full against Supabase's CA).
 *               One pg client per request; every check is one READ ONLY
 *               transaction that binds the tenant settings the role's RLS
 *               policy reads (liveness.ts TENANT_SETTINGS_SQL).
 *   Admission   only with READ_API_ANALYSIS_ENABLED=true: the separate
 *               `bulls96_analysis_admit` role through its own optional Hyperdrive
 *               binding ANALYSIS_ADMIT (caching disabled, verify-full), one client
 *               per admission, one READ WRITE transaction that binds the tenant
 *               and calls sessions.analysis_request_v1. Never the read role's client.
 *
 * Methods: GET everywhere. POST only on exactly /v1/sessions/:sessionId/analysis and only
 * with READ_API_ANALYSIS_ENABLED=true (the analysis admission); every other non-GET is 405.
 *
 * Env (wrangler.jsonc vars, set by the deploy workflow): READ_API_ORIGIN (this
 * Worker's own origin, the only audience accepted; unset = every /v1 request
 * 401), READ_API_COMPLETE_SINCE (ISO timestamp or "epoch"; missing = 503),
 * CLICKHOUSE_DATABASE, READ_API_DIRECTORY_WORKSPACES (the directory list/activity
 * and timing canary allowlist; unset = none), READ_API_TIMING_ASSUME_NO_TURNS,
 * READ_API_OUTPUT_ENABLED ("1" only: GET /v1/sessions/output from the ClickHouse facts,
 * which needs the owner-applied output-facts DDL; default off = 501),
 * READ_API_DIGEST_ENABLED ("1" only: GET /v1/digest/daily; its narrative needs migration
 * 20261105045000 and the DIGEST_NARRATIVE service binding, without which every miss is
 * `provider_not_configured`; default off = 501),
 * READ_API_COORDINATION_OVERLAPS_ENABLED (exactly "true": GET /v1/coordination/overlaps from
 * the existing ClickHouse facts; every other value = 404),
 * READ_API_COORDINATION_ADVISORIES_ENABLED (exactly "true": GET /v1/coordination/advisories
 * through the 20261105062000 read functions; every other value = 501 unsupported_by_backend,
 * reason advisories_disabled). The POST ack is never served here: its tenant transactions are
 * READ ONLY, so the ack is a 405 and is served by the self-host service),
 * READ_API_SESSION_INTEL_ENABLED (exactly "true": GET /v1/session-intel/workspace through the
 * 20261105062170 read functions and the directory; every other value = 501 unsupported_by_backend,
 * reason session_intel_disabled),
 * READ_API_ANALYSIS_ENABLED (exactly "true": GET and POST /v1/sessions/:sessionId/analysis, lane
 * F1, migration 20261105046000, src/session-read-api/analysis-source.ts; see analysisDeps below;
 * every other value = GET 501 analysis_not_configured and POST 405, exactly as before the flag),
 * READ_API_BRIEF_SUMMARY_ENABLED (exactly "true": GET /v1/sessions/:id/summary through the
 * brief-summary SQL, as the hosted Lambda; every other value = 501 summary_not_configured),
 * READ_API_SUMMARY_FALLBACK_ENABLED (exactly "true", with READ_API_BRIEF_SUMMARY_ENABLED: the
 * same route also serves Bob Costas's encrypted summary objects, kind=short without a brief and
 * kind=long, src/session-read-api/summary-fallback.ts; it needs the PIPELINE_RAW_S3_* raw store
 * and the SUMMARY_MASTER_KEYS secret, without which those reads answer 501
 * summary_fallback_not_configured; every other value = the route exactly as without it),
 * READ_API_LIST_SHORT_SUMMARY_ENABLED (exactly "true": the GET /v1/sessions
 * includeShortSummary=recorded extension; every other value = 501 param_includeShortSummary),
 * READ_API_LIST_READINESS_ENABLED (exactly "true": the GET /v1/sessions includeReadiness=true and
 * readyOnly=true opt-ins over migration 20261105062200; every other value = 501
 * param_includeReadiness / param_readyOnly; unset in wrangler.jsonc),
 * READ_API_SUMMARY_SEARCH_ENABLED (exactly "true": GET /v1/sessions/summaries/search over the
 * owner-applied ClickHouse session_brief_documents; every other value = 501
 * summary_search_not_configured; unset in wrangler.jsonc),
 * READ_API_SUMMARY_SEARCH_BACKEND (unset, "" or "clickhouse" only: the Postgres summary search
 * backend is served only by the self-host service, so "postgres" or any other value is a
 * misconfigured deploy and every request answers 503; unset in wrangler.jsonc),
 * READ_API_RELAYFACTS_ENABLED (exactly "true": GET /v1/relayfacts and
 * /v1/relayfacts/:topic, migration 20261105062080; every other value = 501
 * relayfacts_disabled),
 * READ_API_SESSION_SOURCES_ENABLED (exactly "true": GET /v1/sessions/:sessionId/sources, the
 * live harnesses of one session id from session_index through the read role; every other value =
 * 501 sources_not_configured; unset in wrangler.jsonc),
 * READ_API_SESSION_PARTIAL_RETRYABLE (self-host service only: unset, "" or "false" here; any other
 * value refuses every request as missing_READ_API_SESSION_PARTIAL_RETRYABLE; unset in wrangler.jsonc),
 * READ_API_BRIEF_TITLES_WORKSPACES (the brief-titles allowlist; unset = none), and
 * RELAYAUTH_JWKS_URL / RELAYAUTH_ISSUER (production
 * values in wrangler.jsonc; the verifier's fallback JWKS does not resolve), and
 * RELAYHISTORY_ACCEPT_RELAYCONTROL_TOKENS with RELAYCONTROL_INTROSPECT_URL /
 * RELAYCONTROL_INTROSPECT_SECRET (relaycontrol `sh_at_` tokens; default off,
 * unset in wrangler.jsonc).
 *
 * Owner-only buffered reads (src/session-read-api/owner-buffered.ts, Option C):
 * RELAYHISTORY_READ_OWNER_BUFFERED (default off; only "1" or "true" enables), with
 * INGEST_S3_BUCKET / INGEST_S3_REGION naming the ingest bucket that holds the
 * buffered-batches/ stage objects, read with the PIPELINE_RAW_S3_ACCESS_KEY_ID /
 * _SECRET_ACCESS_KEY principal (GetObject + kms:Decrypt on buffered-batches/ is a
 * separate IAM grant). Off, or any of those missing: no buffered read, every route
 * exactly as without the flag. Every buffered response passes the privacy authority gate,
 * the HISTORY_CONTROL_HEAD service binding to relayhistory-cloud's named entrypoint
 * SessionReadPrivacyControls (pinned with `entrypoint` in wrangler config; never a public
 * route), R2 control-head contract v5, wire frozen at relayhistory-cloud #328 a06a58f9:
 * GET /v1/session-read/control-head, no query string, no body, identity only in
 * x-relayhistory-org-id / -workspace-id / -user-id from the verified token, 2 s deadline,
 * no-store both ways, no caching. Flag on with the binding absent: every request answers 503
 * backend_unavailable (logged missing_HISTORY_CONTROL_HEAD), the same fail-closed rule as a missing
 * Hyperdrive binding. READ_API_OWNER_BUFFERED_WORKSPACES is the owner-buffered canary allowlist (on
 * top of READ_API_DIRECTORY_WORKSPACES; unset = none; "*" = all-workspaces mode, which the deploy
 * render accepts only with READ_API_DIRECTORY_WORKSPACES="*" and which never bypasses the per-user
 * privacy gate). READ_API_OWNER_BUFFERED_EVENTS=on (default off; any value but unset/empty/"on" answers
 * 503) adds owner-buffered session events on GET /v1/sessions/:sessionId/events with `source`; it
 * has no effect without the owner-buffered flag and allowlists. READ_API_OWNER_BUFFERED_STAGE_CONCURRENCY
 * (default unset = the serial stage fetch; "0"/"1" the same; "2".."32" = that many stage GetObjects
 * in flight per buffered events read; any other value answers 503) and READ_API_OWNER_BUFFERED_PHASE_LOG
 * (default off; exactly "1" adds per-phase *_ms fields to the request log line) apply to the
 * owner-buffered events read only. READ_API_OWNER_BUFFERED_INDEX_VERIFY=on (default off; any value
 * but unset/empty/"on" answers 503) makes every owner-buffered read first prove, through the
 * catalog on the same read connection, that the out-of-band index
 * ingest_buffer_outbox_user_buffered_idx exists and is valid (cached per isolate: valid for its
 * lifetime, a failure re-probed after 60 s); not proven, the buffered read is fenced before any
 * candidate (detail routes answer as without the flag, the list as fenced).
 * READ_API_OWNER_BUFFERED_STATEMENT_TIMEOUT_MS (default unset = 2000; "2000".."6000" = the
 * per-transaction statement_timeout of both owner-buffered RPCs; any other value answers 503) never
 * changes the role default or any other read. Flag, allowlist, bucket and binding are
 * all unset in wrangler.jsonc; only the deploy workflow's owner_buffered=on render
 * (scripts/render-deploy-config.ts) adds them.
 */
import { readerMaxRuleset } from "../../pipeline/src/lib/scrub-engine/reader-policy.ts";
import pg from "pg";
import { Decompress, decompress } from "fzstd";
import { canonicalAnalysisHandle } from "./analysis-cache.ts";
import { canonicalHandle, type CanonicalEnv } from "./canonical.ts";
import { endPg, isPgEnding, pgClientErrorLog } from "./pg-close.ts";
import { rawStoreFromEnv, type RawStore } from "../../pipeline/src/lib/pipeline/adapter.ts";
import { AnalysisResultError } from "../../../src/session-analysis/results.ts";
import { canonicalSnapshot, CanonicalReadError } from "../../../src/session-read-api/canonical.ts";
import type { CanonicalEventsDeps } from "../../../src/session-read-api/canonical-events.ts";
import { sqlCanonicalPointers } from "../../../src/session-read-api/session-timing-source.ts";
import { sqlSessionHarnesses } from "../../../src/session-read-api/session-turns-source.ts";
import { sqlSessionSources } from "../../../src/session-read-api/session-sources.ts";
import type { TimingDeps, TurnsDeps } from "../../../src/session-read-api/app.ts";
import { sqlEvidenceDeps, type EvidenceDeps } from "../../../src/session-read-api/babysitter-evidence.ts";
import { objectKey, readAll } from "../../../src/shared/vendor.ts";
import { logLine, type Level } from "../../../src/shared/log.ts";
import { createClickHouseClient, type ClickHouseClient, type RequestOptions } from "../../../src/session-pipeline/vendor/search-index/clickhouse.ts";
import { createReadApi, parseDirectoryWorkspaces, type ReadApiDeps } from "../../../src/session-read-api/app.ts";
import { sqlDirectory } from "../../../src/session-read-api/directory.ts";
import { serviceNarrativeGenerator, sqlNarrativeCache } from "../../../src/session-read-api/digest-narrative.ts";
import { sqlBriefTitles } from "../../../src/session-read-api/brief-titles.ts";
import { sqlBriefSummary } from "../../../src/session-read-api/brief-summary.ts";
import { pointerSummaryFallback, type SummaryFallback } from "../../../src/session-read-api/summary-fallback.ts";
import { masterKeyring, parseMasterKeys } from "../../../src/shared/keyring.ts";
import type { WorkspaceKeyring } from "../../../src/bob-costas/summary-objects.ts";
import { sqlRelayfacts } from "../../../src/session-read-api/relayfacts.ts";
import { sqlSessionIntel } from "../../../src/session-read-api/session-intel-workspace.ts";
import { parseAdvisoryNotifyRows } from "../../../src/session-read-api/advisory-priority.ts";
import { perRequestAdmitPool, sqlAnalysisSource, type AdmitPool } from "../../../src/session-read-api/analysis-source.ts";
import type { AnalysisDeps } from "../../../src/session-read-api/app.ts";
import { readApiVerifier, type AuthConfig } from "../../../src/session-read-api/auth.ts";
import { assertDestinationUrl, destinationPolicy } from "../../../src/shared/destination-url.ts";
import { repeatableReadEnabled, sqlLiveness, TENANT_SETTINGS_SQL, tenantBeginSql, type SqlQuery, type TenantRunner } from "../../../src/session-read-api/liveness.ts";
import { coordinationOverlapsEnabled, exactTrueFlag, parseCompleteSince, parseSummarySearchBackend, sessionPartialRetryableFlag } from "../../../src/session-read-api/config.ts";
import { pendingV2Cache, sqlCoordinationAdvisories } from "../../../src/session-read-api/coordination-advisories.ts";
import { AnalyticsConfigError, analyticsConfig, analyticsCounterFields, createAnalyticsSink, type AnalyticsConfig } from "../../../src/shared/analytics-config.ts";
import {
  createBufferedSearchGate,
  parseBufferedSearchGate,
  serviceBindingControlHeads,
  sqlGateStore,
  type BufferedSearchGate,
  type ServiceBinding,
} from "../../../src/session-read-api/buffered-search-gate.ts";
import { createS3Store } from "../../../src/shared/vendor.ts";
import {
  BufferedIndexVerifier,
  boundedStreamDecompress,
  ownerBufferedEnabled,
  ownerBufferedExactSessionEnabled,
  ownerBufferedStagesPagedEnabled,
  ownerBufferedEventsEnabled,
  ownerBufferedIndexVerifyEnabled,
  ownerBufferedPhaseLogEnabled,
  ownerBufferedStageConcurrency,
  ownerBufferedStatementTimeoutMs,
  serviceBindingControlHead,
  sqlBufferedIndexProbe,
  sqlOwnerBufferedReads,
  type OwnerBufferedDeps,
} from "../../../src/session-read-api/owner-buffered.ts";

/** The read role never writes: its tenant transactions are READ ONLY (the analysis admission has its own role and client). */
const workerAckRefused: TenantRunner = async () => {
  throw new Error("session read worker: the advisory ack is served by the self-host service only");
};

export interface Env extends CanonicalEnv {
  SUPABASE: Hyperdrive;
  CLICKHOUSE_URL?: string;
  CLICKHOUSE_USER?: string;
  CLICKHOUSE_PASSWORD?: string;
  CLICKHOUSE_DATABASE?: string;
  READ_API_ORIGIN?: string;
  READ_API_COMPLETE_SINCE?: string;
  /** Directory list/activity and timing canary allowlist: unset = none, "*" = all, else comma-separated workspace ids. */
  READ_API_DIRECTORY_WORKSPACES?: string;
  /** Brief-titles allowlist (GET /v1/sessions/titles): unset = none, "*" = all, else comma-separated workspace ids. */
  READ_API_BRIEF_TITLES_WORKSPACES?: string;
  /** "true": time a session without legacy turn evidence as having none (session-timing-source.ts). */
  READ_API_TIMING_ASSUME_NO_TURNS?: string;
  /** "1": serve GET /v1/sessions/output from the ClickHouse facts; anything else = 501 output_not_configured. */
  READ_API_OUTPUT_ENABLED?: string;
  /** "1": serve GET /v1/digest/daily; anything else = 501 digest_not_configured. */
  READ_API_DIGEST_ENABLED?: string;
  /** Exactly "true": serve GET /v1/coordination/overlaps from ClickHouse facts. */
  READ_API_COORDINATION_OVERLAPS_ENABLED?: string;
  /** Exactly "true": serve GET /v1/coordination/advisories (the ack stays 405 on this GET-only Worker). */
  READ_API_COORDINATION_ADVISORIES_ENABLED?: string;
  /** Exactly "true": serve GET /v1/session-intel/workspace (sqlSessionIntel and the directory). */
  READ_API_SESSION_INTEL_ENABLED?: string;
  /**
   * `<kind>:<urgency>` rows whose advisories are notifyEligible (advisory-priority.ts); default empty. Malformed: the
   * two advisory routes (GET /v1/coordination/advisories and GET /v1/session-intel/workspace) answer 503; every other
   * route is served.
   */
  READ_API_ADVISORY_NOTIFY_ROWS?: string;
  /** Exactly "true": M3-7 advisory priority (062190's v2 ordering and the priority fields); anything else = off. */
  READ_API_ADVISORY_PRIORITY_ENABLED?: string;
  /** Exactly "true": the 062220 advisory instance on the GET (pending_for_v3, contract advisories/v3); anything else = off. */
  READ_API_ADVISORY_INSTANCE_ENABLED?: string;
  /**
   * Exactly "true": serve GET and POST /v1/sessions/:sessionId/analysis (analysisDeps); anything
   * else = GET 501 analysis_not_configured and POST 405, as before the flag.
   */
  READ_API_ANALYSIS_ENABLED?: string;
  /**
   * The analysis admission principal (bulls96_analysis_admit) through its own Hyperdrive config,
   * caching disabled. Read only with READ_API_ANALYSIS_ENABLED=true; unbound = POST 501
   * analysis_admission_not_configured while GET still serves.
   */
  ANALYSIS_ADMIT?: Hyperdrive;
  /** Exactly "true": serve GET /v1/sessions/:id/summary (sqlBriefSummary); anything else = 501 summary_not_configured. */
  READ_API_BRIEF_SUMMARY_ENABLED?: string;
  /**
   * Exactly "true" (and READ_API_BRIEF_SUMMARY_ENABLED): GET /v1/sessions/:id/summary falls back to
   * Bob Costas's summary objects (summary-fallback.ts) and serves kind=long; anything else = the
   * route unchanged.
   */
  READ_API_SUMMARY_FALLBACK_ENABLED?: string;
  /**
   * Secret: the summary master keys (src/shared/keyring.ts format, `<id>:<64 hex>[,...]`), the
   * same value Bob Costas encrypts under. Read only by the summary fallback; never logged.
   */
  SUMMARY_MASTER_KEYS?: string;
  /** Exactly "true": serve GET /v1/relayfacts[/:topic] (sqlRelayfacts); anything else = 501 relayfacts_disabled. */
  READ_API_RELAYFACTS_ENABLED?: string;
  /** Exactly "true": serve the GET /v1/sessions includeShortSummary=recorded extension; anything else = 501 param_includeShortSummary. */
  READ_API_LIST_SHORT_SUMMARY_ENABLED?: string;
  /** Exactly "true": serve the GET /v1/sessions includeReadiness=true / readyOnly=true opt-ins; anything else = 501 param_includeReadiness / param_readyOnly. */
  READ_API_LIST_READINESS_ENABLED?: string;
  READ_API_SUMMARY_SEARCH_ENABLED?: string;
  /** Unset, "" or "clickhouse" only (parseSummarySearchBackend); "postgres" is self-host service only and, like any other value, refuses every request. */
  READ_API_SUMMARY_SEARCH_BACKEND?: string;
  /** Exactly "true": serve GET /v1/sessions/:sessionId/sources (sqlSessionSources); anything else = 501 sources_not_configured. */
  READ_API_SESSION_SOURCES_ENABLED?: string;
  /** Self-host service only (config.ts READ_API_SESSION_PARTIAL_RETRYABLE): unset, "" or "false"; anything else refuses every request. */
  READ_API_SESSION_PARTIAL_RETRYABLE?: string;
  /** Exactly "true": serve GET /v1/events?origin= (message-origin.ts); anything else = 501 param_origin. */
  MESSAGE_ORIGIN_LABELS_ENABLED?: string;
  /**
   * Hosted canonical events (src/session-read-api/app.ts canonicalEventsHosted): exactly "true"
   * serves GET /v1/sessions/:sessionId/events for sessions older than READ_API_COMPLETE_SINCE from
   * the canonical objects (needs the raw-store secrets; without them it stays off). Unset or empty
   * = off (default); any other value refuses every request (503, logged
   * missing_READ_API_CANONICAL_EVENTS_HOSTED). Never predeclared in wrangler.jsonc.
   */
  READ_API_CANONICAL_EVENTS_HOSTED?: string;
  /** Its workspace allowlist (parseDirectoryWorkspaces syntax); unset/empty = none. Never predeclared in wrangler.jsonc. */
  READ_API_CANONICAL_EVENTS_WORKSPACES?: string;
  /** The digest narrative Worker (wrangler.digest-narrative.jsonc), bound at activation only. */
  DIGEST_NARRATIVE?: Fetcher;
  /** Babysitter evidence reader (GET /v1/delivery/records for its kinds): only "on" enables; default off. */
  READ_API_BABYSITTER_EVIDENCE?: string;
  /** The ingest key root the retention buckets live under (default 96bulls). */
  READ_API_BABYSITTER_KEY_ROOT?: string;
  /**
   * The per-hit R2 privacy gate for buffered-origin search rows: only "on" enables; default off
   * (src/session-read-api/buffered-search-gate.ts). It reads the same privacy authority as
   * owner-buffered reads, the HISTORY_CONTROL_HEAD binding below; on without it, every
   * buffered-origin row is dropped.
   */
  READ_API_BUFFERED_SEARCH_GATE?: string;
  /**
   * Tenant read transactions at REPEATABLE READ: only "1" enables; unset/"0" (default) keeps
   * `BEGIN READ ONLY` at READ COMMITTED (see requestTenantRunner).
   */
  READ_API_REPEATABLE_READ?: string;
  RELAYAUTH_JWKS_URL?: string;
  RELAYAUTH_ISSUER?: string;
  /** Exactly "true" also accepts relaycontrol `sh_at_` tokens; default off. */
  RELAYHISTORY_ACCEPT_RELAYCONTROL_TOKENS?: string;
  RELAYCONTROL_INTROSPECT_URL?: string;
  RELAYCONTROL_INTROSPECT_SECRET?: string;
  /** Owner-only buffered reads (Option C): only "1" or "true" enables; default off. */
  RELAYHISTORY_READ_OWNER_BUFFERED?: string;
  /**
   * Owner-buffered session events (GET /v1/sessions/:sessionId/events with `source`): unset or
   * empty = off (default), exactly "on" enables, any other value refuses every request (503,
   * logged missing_READ_API_OWNER_BUFFERED_EVENTS). No effect unless
   * RELAYHISTORY_READ_OWNER_BUFFERED is on and the workspace is on both allowlists.
   */
  READ_API_OWNER_BUFFERED_EVENTS?: string;
  /**
   * Owner-buffered events read: the most S3 stage GetObjects in flight. Unset, empty, "0" or "1"
   * = the serial fetch (default); an integer "2".."32" = bounded concurrency; any other value
   * refuses every request (503, logged missing_READ_API_OWNER_BUFFERED_STAGE_CONCURRENCY).
   */
  READ_API_OWNER_BUFFERED_STAGE_CONCURRENCY?: string;
  /**
   * Owner-buffered events read: exactly "1" adds the per-phase fields (session_start_ms ..
   * fold_ms, s3_stage_count, aborted_phase) to the request log line; anything else is off (default).
   */
  READ_API_OWNER_BUFFERED_PHASE_LOG?: string;
  /**
   * Owner-buffered RPCs (list and detail): the per-transaction statement_timeout in ms. Unset or
   * empty = 2000 (default, the read role's own bound); an integer 2000..6000 = that bound; any
   * other value refuses every request (503, logged missing_READ_API_OWNER_BUFFERED_STATEMENT_TIMEOUT_MS).
   */
  READ_API_OWNER_BUFFERED_STATEMENT_TIMEOUT_MS?: string;
  /**
   * Owner-buffered reads: verify the out-of-band index ingest_buffer_outbox_user_buffered_idx
   * (valid, per the catalog) before serving any buffered read. Unset or empty = off (default, no
   * query), exactly "on" enables, any other value refuses every request (503, logged
   * missing_READ_API_OWNER_BUFFERED_INDEX_VERIFY). No effect unless RELAYHISTORY_READ_OWNER_BUFFERED
   * is on and the workspace is on both allowlists.
   */
  READ_API_OWNER_BUFFERED_INDEX_VERIFY?: string;
  /** 047010 exact-session RPC: exactly "on" opts in; every other value uses the frozen RPC. */
  READ_API_OWNER_BUFFERED_EXACT_SESSION?: string;
  /** "on" pages the exact-session stages read through the 047020 RPC; only with READ_API_OWNER_BUFFERED_EXACT_SESSION=on. */
  READ_API_OWNER_BUFFERED_STAGES_PAGED?: string;
  /** The owner-buffered canary allowlist (parseDirectoryWorkspaces syntax); unset/empty = none. */
  READ_API_OWNER_BUFFERED_WORKSPACES?: string;
  /** The ingest bucket holding buffered-batches/ stage objects (owner-buffered reads only). */
  INGEST_S3_BUCKET?: string;
  /** Its region; default us-east-1. */
  INGEST_S3_REGION?: string;
  /**
   * The privacy authority for owner-buffered reads: a service binding to relayhistory-cloud's
   * named entrypoint SessionReadPrivacyControls (control-head contract v5, frozen wire
   * #328 a06a58f9, GET /v1/session-read/control-head). Unbound = every buffered read fenced.
   */
  HISTORY_CONTROL_HEAD?: Fetcher;
  /**
   * Product analytics (src/shared/analytics-config.ts, hosted profile): unset, empty or "noop" = off
   * (default); "posthog" sends one session_read.request_completed per verified /v1 request to
   * ANALYTICS_POSTHOG_HOST (default https://i.agentrelay.com) with the ANALYTICS_POSTHOG_KEY secret and
   * ANALYTICS_DEPLOYMENT_ID, flushed in waitUntil after the response (at most 2 subrequests). Any other
   * value, or "collector", refuses every request (503, logged missing_<variable>).
   */
  ANALYTICS_SINK?: string;
  ANALYTICS_DEPLOYMENT_ID?: string;
  ANALYTICS_POSTHOG_HOST?: string;
  ANALYTICS_POSTHOG_KEY?: string;
}

/** The read verifier's config from Worker env; the introspection URL passes the https/loopback rule. */
export function workerAuthConfig(env: Env): AuthConfig {
  const config: AuthConfig = { origin: env.READ_API_ORIGIN?.trim() || undefined, jwksUrl: env.RELAYAUTH_JWKS_URL?.trim() || undefined, issuer: env.RELAYAUTH_ISSUER?.trim() || undefined };
  if (env.RELAYHISTORY_ACCEPT_RELAYCONTROL_TOKENS === "true") {
    const url = env.RELAYCONTROL_INTROSPECT_URL?.trim() || undefined;
    if (url) assertDestinationUrl("RELAYCONTROL_INTROSPECT_URL", url, destinationPolicy({}));
    config.relaycontrol = { url, secret: env.RELAYCONTROL_INTROSPECT_SECRET?.trim() || undefined };
  }
  return config;
}

const SERVICE = "96bulls-session-read-worker";

export function log(level: Level, message: string, fields: Record<string, unknown> = {}): void {
  const line = logLine(level, message, { service: SERVICE, ...fields });
  if (level === "error" || level === "warn") console.error(line);
  else console.log(line);
}

/**
 * Per-query bounds on top of the read API's READ_SETTINGS, as relayhistory-cloud's
 * search-clickhouse.ts sets them: the server cancels a read-only query whose
 * client went away (our AbortSignal timeout), and threads per query are capped
 * so one request cannot take the whole service.
 */
export const WORKER_CH_SETTINGS = {
  cancel_http_readonly_queries_on_client_close: 1,
  max_threads: 4,
  use_skip_indexes: 1,
} as const;

/** Adds WORKER_CH_SETTINGS to every select; the read path never writes. */
export function boundedClickHouse(inner: ClickHouseClient): ClickHouseClient {
  const withBounds = <O extends RequestOptions>(o: O | undefined): O => ({ ...(o ?? ({} as O)), settings: { ...WORKER_CH_SETTINGS, ...(o?.settings ?? {}) } });
  return {
    database: inner.database,
    select: (query, options) => inner.select(query, withBounds(options)),
    command: () => Promise.reject(new Error("the session read Worker does not write to ClickHouse")),
    insert: () => Promise.reject(new Error("the session read Worker does not write to ClickHouse")),
  };
}

export class ConfigError extends Error {
  constructor(readonly field: string) {
    super(`session read worker: ${field} is not configured`);
    this.name = "ConfigError";
  }
}

/** ClickHouse connection from Worker secrets, trimmed as relayhistory-cloud reads them. Never echoes a value. */
export function clickHouseConfig(env: Env) {
  const url = env.CLICKHOUSE_URL?.trim();
  const user = env.CLICKHOUSE_USER?.trim();
  const password = env.CLICKHOUSE_PASSWORD;
  const database = env.CLICKHOUSE_DATABASE?.trim();
  if (!url) throw new ConfigError("CLICKHOUSE_URL");
  if (!user) throw new ConfigError("CLICKHOUSE_USER");
  if (!password) throw new ConfigError("CLICKHOUSE_PASSWORD");
  if (!database) throw new ConfigError("CLICKHOUSE_DATABASE");
  return { url, user, password, database };
}

/**
 * A TenantRunner over one lazily connected pg client for the request. The
 * Hyperdrive guidance is a client per request (Hyperdrive pools the origin
 * connections), closed after the response. Checks are serialized on the one
 * client, so two transactions never interleave on it.
 *
 * Isolation (READ_API_REPEATABLE_READ, default off): off, each transaction is
 * `BEGIN READ ONLY` at READ COMMITTED, so every statement takes its own
 * snapshot. On ("1"), it is REPEATABLE READ READ ONLY: every statement reads
 * ONE snapshot (taken at its first statement), so a check that issues several
 * statements (the buffered search gate's 500-hit chunks, the owner-buffered
 * scope then list) judges them all against the same database state. Every
 * consumer only reads, and a read-only REPEATABLE READ transaction never fails
 * serialization. It is flagged because Hyperdrive's handling of it is unproven.
 */
export function requestTenantRunner(
  connect: () => Promise<pg.Client>,
  options: { repeatableRead?: boolean } = {},
): { run: TenantRunner; close(): Promise<void> } {
  const begin = tenantBeginSql(options.repeatableRead === true);
  let client: Promise<pg.Client> | undefined;
  let tail: Promise<unknown> = Promise.resolve();
  const run: TenantRunner = (orgId, workspaceId, work) => {
    const next = tail.then(async () => {
      const c = await (client ??= connect());
      await c.query(begin);
      try {
        await c.query(TENANT_SETTINGS_SQL, [orgId, workspaceId]);
        const q: SqlQuery = {
          async query<R>(text: string, params?: unknown[]) {
            const r = await c.query(text, params);
            return { rows: r.rows as R[] };
          },
        };
        const result = await work(q);
        await c.query("COMMIT");
        return result;
      } catch (error) {
        await c.query("ROLLBACK").catch(() => {});
        throw error;
      }
    });
    tail = next.catch(() => {});
    return next;
  };
  return {
    run,
    async close() {
      if (!client) return;
      const c = await client.catch(() => undefined);
      if (c) await endPg(c).catch(() => {});
    },
  };
}

/**
 * A connected pg client over the Workers socket. pg-cloudflare's read loop
 * emits `error` ("This socket has been closed") on the socket once `end()` has
 * closed it, after pg has removed its own listener, which would surface as an
 * unhandled rejection on every request. The extra listener absorbs only that;
 * an error while a query runs still reaches pg's listener and fails the query.
 */
export async function connectPg(connectionString: string): Promise<pg.Client> {
  const client = new pg.Client({ connectionString, application_name: SERVICE, connectionTimeoutMillis: 2_000 });
  client.on("error", (err) => {
    const line = pgClientErrorLog(isPgEnding(client), err);
    log(line.level, line.message, { error_code: line.error_code });
  });
  await client.connect();
  (client as unknown as { connection?: { stream?: { on(event: "error", fn: () => void): void } } }).connection?.stream?.on("error", () => {});
  return client;
}

export function hyperdriveClient(hyperdrive: Hyperdrive): () => Promise<pg.Client> {
  return () => connectPg(hyperdrive.connectionString);
}

const json = (status: number, code: string, message: string, extra: Record<string, string> = {}) =>
  new Response(JSON.stringify({ error: { code, message } }), {
    status,
    headers: { "content-type": "application/json", "cache-control": "private, no-store", "x-sessions-read-backend": "96bulls", ...extra },
  });

/** The one non-GET route this Worker serves (with READ_API_ANALYSIS_ENABLED=true): the analysis admission. */
export const ANALYSIS_POST_PATH = /^\/v1\/sessions\/[^/]+\/analysis$/;

/** GET always; POST only on exactly the analysis route with the analysis flag on. */
export function methodAllowed(method: string, pathname: string, env: Pick<Env, "READ_API_ANALYSIS_ENABLED">): boolean {
  if (method === "GET") return true;
  return method === "POST" && exactTrueFlag(env.READ_API_ANALYSIS_ENABLED) && ANALYSIS_POST_PATH.test(pathname);
}

export interface Overrides {
  /** Tests: the pg client factory in place of Hyperdrive's. */
  connect?: () => Promise<pg.Client>;
  /** Tests: the admission principal's pg client factory in place of the ANALYSIS_ADMIT binding's. */
  connectAnalysisAdmit?: () => Promise<pg.Client>;
  fetcher?: typeof fetch;
}

/** The routes that read READ_API_ADVISORY_NOTIFY_ROWS (a malformed list fails only these). */
const ADVISORY_ROUTE = /^\/v1\/(coordination\/advisories(\/|$)|session-intel\/workspace$)/u;
/**
 * The source is built per request; "pending_for_v2 not installed" (and the warning's spacing) is remembered per
 * isolate here, so a database without 062190 costs one probe per minute, not a failed statement per GET.
 */
const ADVISORIES_V2_CACHE = pendingV2Cache();

/**
 * The advisory deps from the Worker's env: the advisories GET (READ_API_COORDINATION_ADVISORIES_ENABLED; the ack's
 * writer refuses, and the GET-only gate answers a POST with 405 before it), the notify rows, and the M3-7 priority
 * switch (READ_API_ADVISORY_PRIORITY_ENABLED, exactly "true"), with the v1-fallback warning logged at warn, and the
 * 062220 instance switch (READ_API_ADVISORY_INSTANCE_ENABLED, exactly "true"; the GET only, the ack stays refused).
 */
export function workerAdvisoryDeps(
  env: Pick<Env, "READ_API_COORDINATION_ADVISORIES_ENABLED" | "READ_API_ADVISORY_PRIORITY_ENABLED" | "READ_API_ADVISORY_INSTANCE_ENABLED">,
  run: TenantRunner, advisoryNotifyRows: ReadonlySet<string>,
  warnLog: (fields: Record<string, unknown>) => void = (fields) => log("warn", "coordination advisories pull fell back to v1", fields),
) {
  const priority = exactTrueFlag(env.READ_API_ADVISORY_PRIORITY_ENABLED);
  const instance = exactTrueFlag(env.READ_API_ADVISORY_INSTANCE_ENABLED);
  return {
    ...(exactTrueFlag(env.READ_API_COORDINATION_ADVISORIES_ENABLED)
      ? { coordinationAdvisories: { enabled: true as const, source: sqlCoordinationAdvisories(run, workerAckRefused, { priority, cache: ADVISORIES_V2_CACHE, warn: warnLog }) } }
      : {}),
    ...(advisoryNotifyRows.size > 0 ? { advisoryNotifyRows } : {}),
    ...(priority ? { advisoryPriority: true } : {}),
    ...(instance ? { advisoryInstance: true } : {}),
  };
}

export async function handle(request: Request, env: Env, ctx: ExecutionContext, overrides: Overrides = {}): Promise<Response> {
  const pathname = new URL(request.url).pathname;
  if (pathname.startsWith("/v1/canonical/analyses/")) return canonicalAnalysisHandle(request, env, overrides.fetcher);
  if (pathname.startsWith("/v1/canonical/")) return canonicalHandle(request, env, overrides.fetcher);
  // GET only, as the Lambda's Function URL adapter (HEAD is not a read the contract has), except
  // the analysis admission POST with READ_API_ANALYSIS_ENABLED=true (methodAllowed).
  if (!methodAllowed(request.method, pathname, env)) return json(405, "method_not_allowed", "GET only", { allow: "GET" });
  let ch: ClickHouseClient;
  let completeSinceMs: number;
  let authConfig: AuthConfig;
  let analyticsSelection: AnalyticsConfig;
  let advisoryNotifyRows: ReadonlySet<string> = new Set();
  try {
    completeSinceMs = parseCompleteSince(env.READ_API_COMPLETE_SINCE);
    try {
      analyticsSelection = analyticsConfig(env as unknown as Record<string, unknown>, "hosted");
    } catch (error) {
      throw new ConfigError(error instanceof AnalyticsConfigError ? error.variable : "ANALYTICS_SINK");
    }
    authConfig = workerAuthConfig(env);
    // The Postgres summary search backend is self-host service only (src/session-read-api/lambda.ts
    // summarySearchBackend); the Worker serves the ClickHouse one and refuses anything else.
    let summarySearchBackend: string;
    try {
      summarySearchBackend = parseSummarySearchBackend(env as unknown as Record<string, unknown>);
    } catch {
      throw new ConfigError("READ_API_SUMMARY_SEARCH_BACKEND");
    }
    if (summarySearchBackend !== "clickhouse") throw new ConfigError("READ_API_SUMMARY_SEARCH_BACKEND");
    ch = boundedClickHouse(createClickHouseClient(clickHouseConfig(env), overrides.fetcher));
    if (!overrides.connect && !env.SUPABASE?.connectionString) throw new ConfigError("SUPABASE");
    // Owner-buffered on needs its privacy authority bound; refuse every request rather than run
    // a deploy whose flag says on and whose gate can only fence.
    if (ownerBufferedEnabled(env.RELAYHISTORY_READ_OWNER_BUFFERED) && typeof env.HISTORY_CONTROL_HEAD?.fetch !== "function") throw new ConfigError("HISTORY_CONTROL_HEAD");
    // The events flag accepts only unset/empty or "on"; anything else is a misconfigured deploy.
    try {
      ownerBufferedEventsEnabled(env.READ_API_OWNER_BUFFERED_EVENTS);
    } catch {
      throw new ConfigError("READ_API_OWNER_BUFFERED_EVENTS");
    }
    // The stage concurrency accepts only unset/empty, "0", "1" or "2".."32"; anything else is a misconfigured deploy.
    try {
      ownerBufferedStageConcurrency(env.READ_API_OWNER_BUFFERED_STAGE_CONCURRENCY);
    } catch {
      throw new ConfigError("READ_API_OWNER_BUFFERED_STAGE_CONCURRENCY");
    }
    // The buffered statement timeout accepts only unset/empty or 2000..6000; anything else is a misconfigured deploy.
    try {
      ownerBufferedStatementTimeoutMs(env.READ_API_OWNER_BUFFERED_STATEMENT_TIMEOUT_MS);
    } catch {
      throw new ConfigError("READ_API_OWNER_BUFFERED_STATEMENT_TIMEOUT_MS");
    }
    // The index verify flag accepts only unset/empty or "on"; anything else is a misconfigured deploy.
    try {
      ownerBufferedIndexVerifyEnabled(env.READ_API_OWNER_BUFFERED_INDEX_VERIFY);
    } catch {
      throw new ConfigError("READ_API_OWNER_BUFFERED_INDEX_VERIFY");
    }
    // The hosted canonical events flag accepts only unset/empty or exactly "true"; anything else is a misconfigured deploy.
    canonicalEventsHostedEnabled(env.READ_API_CANONICAL_EVENTS_HOSTED);
    // The retryable session_partial 503 is served by the self-host service only: "true" here (or a
    // value that does not parse) is a misconfigured deploy, refused rather than silently ignored.
    try {
      if (sessionPartialRetryableFlag(env.READ_API_SESSION_PARTIAL_RETRYABLE)) throw new Error("service only");
    } catch {
      throw new ConfigError("READ_API_SESSION_PARTIAL_RETRYABLE");
    }
  } catch (error) {
    // Configuration errors name a field, never a value.
    log("error", "read worker not configured", { error_code: error instanceof ConfigError ? `missing_${error.field}` : (error as { name?: string })?.name ?? "config" });
    return json(503, "backend_unavailable", "read backend unavailable");
  }
  // The notify allowlist is parsed strictly. A malformed list fails only the routes that read it (the advisories
  // and the workspace view), with the same 503 as a misconfigured deploy; every other route is still served.
  try {
    advisoryNotifyRows = parseAdvisoryNotifyRows(env.READ_API_ADVISORY_NOTIFY_ROWS);
  } catch {
    if (ADVISORY_ROUTE.test(pathname)) {
      log("error", "read worker not configured", { error_code: "missing_READ_API_ADVISORY_NOTIFY_ROWS" });
      return json(503, "backend_unavailable", "read backend unavailable");
    }
  }
  const tenant = requestTenantRunner(overrides.connect ?? hyperdriveClient(env.SUPABASE), {
    repeatableRead: repeatableReadEnabled(env.READ_API_REPEATABLE_READ),
  });
  // The admission principal's own per-request connections; only with the flag on and a binding.
  const admitConnect = exactTrueFlag(env.READ_API_ANALYSIS_ENABLED)
    ? overrides.connectAnalysisAdmit ?? (env.ANALYSIS_ADMIT?.connectionString ? hyperdriveClient(env.ANALYSIS_ADMIT) : undefined)
    : undefined;
  const admit = admitConnect ? perRequestAdmitPool(admitConnect, (c) => endPg(c)) : undefined;
  const analysis = exactTrueFlag(env.READ_API_ANALYSIS_ENABLED) ? analysisDeps(env, tenant.run, admit?.pool, overrides.fetcher) : undefined;
  // One sink per invocation (no state crosses requests): captured during the request, exported once in waitUntil.
  const analytics = createAnalyticsSink(analyticsSelection, "session-read-api", "worker", {
    ...(overrides.fetcher ? { fetch: overrides.fetcher } : {}),
    report: (counters) => log("warn", "analytics export degraded", analyticsCounterFields(analyticsSelection.sink, counters)),
  });
  const app = createReadApi({
    ...(analyticsSelection.sink !== "noop" ? { analytics } : {}),
    timing: timingDeps(env, tenant.run, overrides.fetcher),
    turns: turnsDeps(env, tenant.run, overrides.fetcher),
    babysitter: babysitterEvidenceDeps(env, tenant.run, overrides.fetcher),
    ownerBuffered: ownerBufferedDeps(env, tenant.run, overrides.fetcher),
    verify: readApiVerifier(authConfig),
    ch,
    liveness: sqlLiveness(tenant.run),
    directory: sqlDirectory(tenant.run),
    directoryWorkspaces: parseDirectoryWorkspaces(env.READ_API_DIRECTORY_WORKSPACES),
    ...(env.READ_API_OUTPUT_ENABLED?.trim() === "1" ? { output: { enabled: true as const } } : {}),
    ...(env.READ_API_DIGEST_ENABLED?.trim() === "1"
      ? { digest: { cache: sqlNarrativeCache(tenant.run), ...(env.DIGEST_NARRATIVE ? { generator: serviceNarrativeGenerator(env.DIGEST_NARRATIVE) } : {}) } }
      : {}),
    ...(coordinationOverlapsEnabled(env.READ_API_COORDINATION_OVERLAPS_ENABLED) ? { coordination: { enabled: true as const } } : {}),
    ...workerAdvisoryDeps(env, tenant.run, advisoryNotifyRows),
    ...(exactTrueFlag(env.READ_API_SESSION_INTEL_ENABLED) ? { sessionIntel: { enabled: true as const, source: sqlSessionIntel(tenant.run) } } : {}),
    // Parity with the hosted Lambda (src/session-read-api/lambda.ts): each only with the exact flag.
    ...(exactTrueFlag(env.READ_API_BRIEF_SUMMARY_ENABLED) ? { briefSummary: sqlBriefSummary(tenant.run) } : {}),
    ...(exactTrueFlag(env.READ_API_BRIEF_SUMMARY_ENABLED) && exactTrueFlag(env.READ_API_SUMMARY_FALLBACK_ENABLED)
      ? { summaryFallback: summaryFallbackDeps(env, tenant.run, overrides.fetcher) }
      : {}),
    ...(exactTrueFlag(env.READ_API_RELAYFACTS_ENABLED) ? { relayfacts: sqlRelayfacts(tenant.run) } : {}),
    ...(analysis ? { analysis } : {}),
    ...(exactTrueFlag(env.READ_API_LIST_SHORT_SUMMARY_ENABLED) ? { listShortSummary: true } : {}),
    ...(exactTrueFlag(env.READ_API_LIST_READINESS_ENABLED) ? { listReadiness: true } : {}),
    ...(exactTrueFlag(env.READ_API_SUMMARY_SEARCH_ENABLED) ? { summarySearch: true } : {}),
    ...(exactTrueFlag(env.READ_API_SESSION_SOURCES_ENABLED) ? { sessionSources: sqlSessionSources(tenant.run) } : {}),
    ...(exactTrueFlag(env.MESSAGE_ORIGIN_LABELS_ENABLED) ? { messageOrigin: true } : {}),
    ...workerCanonicalEventsHosted(env, tenant.run, overrides.fetcher),
    briefTitles: sqlBriefTitles(tenant.run),
    briefTitleWorkspaces: parseDirectoryWorkspaces(env.READ_API_BRIEF_TITLES_WORKSPACES),
    completeSinceMs,
    bufferedSearchGate: bufferedSearchGate(env, tenant.run),
    log: (fields) => log("info", "session read", fields),
  });
  try {
    return await app.fetch(request);
  } finally {
    ctx.waitUntil(tenant.close());
    if (admit) ctx.waitUntil(admit.close());
    // Bounded (WORKER_LIMITS) and never rejecting; the response is already final.
    if (analyticsSelection.sink !== "noop") ctx.waitUntil(analytics.shutdown());
  }
}

/**
 * The buffered search gate, per request (control heads are never cached across requests).
 * Off unless READ_API_BUFFERED_SEARCH_GATE=on.
 */
export function bufferedSearchGate(env: Env, run: TenantRunner): BufferedSearchGate | undefined {
  if (!parseBufferedSearchGate(env.READ_API_BUFFERED_SEARCH_GATE)) return undefined;
  const binding: ServiceBinding | undefined = typeof env.HISTORY_CONTROL_HEAD?.fetch === "function" ? env.HISTORY_CONTROL_HEAD : undefined;
  if (!binding) log("warn", "buffered search gate on without HISTORY_CONTROL_HEAD: buffered rows are dropped", {});
  return createBufferedSearchGate({ controlHeads: binding ? serviceBindingControlHeads(binding) : undefined, store: sqlGateStore(run) });
}

/** Per isolate: the keyring for the last SUMMARY_MASTER_KEYS value seen (keys import non-extractable). */
let summaryKeyringCache: { raw: string; keyring: Promise<WorkspaceKeyring> } | undefined;
/** The summary object read's deadline per store call (as summary-objects.ts DEFAULT_STORE_TIMEOUT_MS). */
const SUMMARY_OBJECT_DEADLINE_MS = 5_000;

/**
 * The summary fallback (READ_API_SUMMARY_FALLBACK_ENABLED): pointers through the read role
 * (tenant-bound), objects with the raw-store secrets the timing and /v1/canonical reads use,
 * opened with SUMMARY_MASTER_KEYS. Either missing (or a malformed key value): those reads
 * answer 501 summary_fallback_not_configured; the value itself is never logged or echoed.
 */
export function summaryFallbackDeps(env: Env, run: TenantRunner, fetcher: typeof fetch = (input, init) => fetch(input, init)): SummaryFallback {
  const bounded: typeof fetch = (input, init) => {
    const deadline = AbortSignal.timeout(SUMMARY_OBJECT_DEADLINE_MS);
    return fetcher(input, { ...init, signal: init?.signal ? AbortSignal.any([init.signal, deadline]) : deadline });
  };
  return pointerSummaryFallback({
    run,
    raw: () => rawStoreFromEnv(env, bounded),
    prefix: env,
    keyring: () => {
      const raw = env.SUMMARY_MASTER_KEYS?.trim();
      if (!raw) return Promise.reject(new Error("summary keyring not configured"));
      if (summaryKeyringCache?.raw !== raw) {
        let keyring: Promise<WorkspaceKeyring>;
        try {
          keyring = masterKeyring(parseMasterKeys(raw));
        } catch {
          // parseMasterKeys names the field only; still, never pass its message on.
          return Promise.reject(new Error("summary keyring not configured"));
        }
        summaryKeyringCache = { raw, keyring };
        keyring.catch(() => { if (summaryKeyringCache?.keyring === keyring) summaryKeyringCache = undefined; });
      }
      return summaryKeyringCache.keyring;
    },
  });
}

/** One analysis result object read's deadline per store call (results are at most 8 MiB, results.ts). */
const ANALYSIS_RESULT_DEADLINE_MS = 10_000;
/**
 * The most result bytes one GET reads in total (the read returns up to 50 analyses, each at most
 * 8 MiB): past it the request is a 503 (analysis_result_budget), never a partial page. 16 MiB
 * keeps the parsed results plus the reserialized response well inside the 128 MB isolate.
 */
export const ANALYSIS_RESULT_REQUEST_BUDGET_BYTES = 16 * 1024 * 1024;

type Store = RawStore["store"];

/** The body, erroring (and cancelling the source) once more than `max` bytes have passed. */
function cappedBody<B extends ReadableStream<Uint8Array>>(body: B, max: number): B {
  let seen = 0;
  return body.pipeThrough(
    new TransformStream<Uint8Array, Uint8Array>({
      transform(chunk, controller) {
        seen += chunk.byteLength;
        if (seen > max) controller.error(new AnalysisResultError("analysis_result_mismatch"));
        else controller.enqueue(chunk);
      },
    }),
  ) as B;
}

/**
 * The raw store as the analysis result reader sees it: read only, and get() refuses an object
 * whose response size (content-length) exceeds the size the job row recorded for that key, or
 * that names no size (0) when the row recorded one, before reading its body (the body is
 * cancelled); the body it does return errors past the recorded size + 1 byte, so a response whose
 * header understates it is never buffered whole. readAnalysisResult still verifies the exact size
 * and SHA-256 of what it read.
 */
function recordedSizeStore(store: Store, recorded: ReadonlyMap<string, number>): Store {
  const refuse = () => Promise.reject(new Error("session read worker: the analysis result reader does not write"));
  return {
    backend: store.backend,
    head: (key) => store.head(key),
    list: (prefix, cursor) => store.list(prefix, cursor),
    presignGet: () => Promise.reject(new Error("session read worker: no presigned analysis reads")),
    put: refuse,
    delete: refuse,
    async getObject(key, range) {
      const got = await store.getObject(key, range);
      const want = recorded.get(String(key));
      const size = got.meta.size;
      if (want === undefined || !Number.isFinite(size) || size > want || (size === 0 && want > 0)) {
        await got.body.cancel().catch(() => {});
        throw new AnalysisResultError(want === undefined ? "analysis_result_ref_invalid" : "analysis_result_mismatch");
      }
      return { ...got, body: cappedBody(got.body, want + 1) };
    },
    async get(key, range) {
      return (await this.getObject(key, range)).body;
    },
  };
}

/**
 * Session analysis (lane F1, READ_API_ANALYSIS_ENABLED=true), the Lambda's sqlAnalysisSource:
 *   read/harnesses  sessions.analysis_read_v1 and the live harnesses through the request's
 *                   tenant runner (bulls96_read_api, READ ONLY, tenant-bound);
 *   result          the result object from the pipeline raw bucket (PIPELINE_RAW_S3_*, the
 *                   canonical store the analysis worker publishes to), at the key, SHA-256 and
 *                   size the job row recorded (never derived from the request), bounded per call;
 *                   an object larger than recorded is refused by content-length before its body
 *                   is read, and one request reads at most ANALYSIS_RESULT_REQUEST_BUDGET_BYTES;
 *   admit           the admission principal's pool (ANALYSIS_ADMIT); absent = POST 501
 *                   analysis_admission_not_configured, GET unchanged.
 * Without the raw store: undefined, so GET and POST answer 501 analysis_not_configured.
 */
export function analysisDeps(env: Env, run: TenantRunner, admit: AdmitPool | undefined, fetcher: typeof fetch = (input, init) => fetch(input, init)): AnalysisDeps | undefined {
  const bounded: typeof fetch = (input, init) => {
    const deadline = AbortSignal.timeout(ANALYSIS_RESULT_DEADLINE_MS);
    return fetcher(input, { ...init, signal: init?.signal ? AbortSignal.any([init.signal, deadline]) : deadline });
  };
  const raw = rawStoreFromEnv(env, bounded);
  if (!raw) return undefined;
  // Per request (analysisDeps is built per request): the recorded size of each key being read,
  // and what is left of the request's byte budget.
  const recorded = new Map<string, number>();
  let remaining = ANALYSIS_RESULT_REQUEST_BUDGET_BYTES;
  const source = sqlAnalysisSource(run, recordedSizeStore(raw.store, recorded), admit);
  return {
    ...source,
    async result(ref) {
      if (!Number.isSafeInteger(ref.bytes) || ref.bytes < 0 || ref.bytes > remaining) throw new AnalysisResultError("analysis_result_budget");
      remaining -= ref.bytes;
      recorded.set(ref.key, ref.bytes);
      return source.result(ref);
    },
  };
}

/**
 * One session's full canonical snapshot (rows, legacy coverage, generation, exists): pointers
 * through the read role (tenant-bound), objects with the raw-store secrets the /v1/canonical
 * route uses (PIPELINE_RAW_S3_*), every fetch bounded by the caller's signal. Shared by timing,
 * turns and the hosted canonical events read.
 */
function workerCanonicalSource(env: Env, pointers: ReturnType<typeof sqlCanonicalPointers>, fetcher: typeof fetch) {
  const maximum = readerMaxRuleset(env);
  return async (identity: Parameters<typeof canonicalSnapshot>[1], signal: AbortSignal) => {
    const bounded: typeof fetch = (input, init) => fetcher(input, { ...init, signal: init?.signal ? AbortSignal.any([init.signal, signal]) : signal });
    const raw = rawStoreFromEnv(env, bounded);
    if (!raw) throw new CanonicalReadError("not_configured");
    return canonicalSnapshot(
      {
        ...raw,
        readerMaxRuleset: maximum,
        prefix: env,
        pointers: pointers.pointers,
        decompress: (bytes, max) => {
          const output = decompress(bytes, new Uint8Array(max));
          if (output.byteLength > max) throw new CanonicalReadError("chunk_bound");
          return output;
        },
      },
      identity,
    );
  };
}

/**
 * Session timing from canonical S3: pointers through the read role (tenant-bound), objects
 * with the raw-store secrets the /v1/canonical route uses (PIPELINE_RAW_S3_*). Without
 * those secrets the route answers 501 `timing_not_configured`.
 */
export function timingDeps(env: Env, run: Parameters<typeof sqlCanonicalPointers>[0], fetcher: typeof fetch = (input, init) => fetch(input, init)): TimingDeps | undefined {
  if (!rawStoreFromEnv(env, fetcher)) return undefined;
  const pointers = sqlCanonicalPointers(run);
  const snapshot = workerCanonicalSource(env, pointers, fetcher);
  return {
    state: pointers.state,
    assumeNoTurns: env.READ_API_TIMING_ASSUME_NO_TURNS?.trim() === "true",
    async source(identity, signal) {
      const { rows, legacyCoverage } = await snapshot(identity, signal);
      return { rows, legacyCoverage };
    },
  };
}

/**
 * READ_API_CANONICAL_EVENTS_HOSTED: unset or empty = off, exactly "true" = on, anything else
 * (including "TRUE" or "false") a ConfigError: every request that reaches the configuration step
 * answers 503, logged missing_READ_API_CANONICAL_EVENTS_HOSTED (the /v1/canonical/* routes and the
 * 405 method check answer before it). The value itself is never logged.
 */
export function canonicalEventsHostedEnabled(raw: string | undefined): boolean {
  if (raw === undefined || raw === "") return false;
  if (raw === "true") return true;
  throw new ConfigError("READ_API_CANONICAL_EVENTS_HOSTED");
}

/**
 * Hosted canonical events (app.ts canonicalEventsHosted): the full snapshot on the timing
 * pointers and raw-store secrets, with the writer's origin labels when
 * MESSAGE_ORIGIN_LABELS_ENABLED is exactly "true". Built only when READ_API_CANONICAL_EVENTS_HOSTED
 * is exactly "true" and the raw store is configured; otherwise undefined and the events route
 * answers as without the flag (with the flag on but the raw store missing, without a log line).
 * The Worker binds no operator erase fence; an erase shows through the tombstone and partial checks.
 */
export function canonicalEventsDeps(env: Env, run: Parameters<typeof sqlCanonicalPointers>[0], fetcher: typeof fetch = (input, init) => fetch(input, init)): CanonicalEventsDeps | undefined {
  if (!canonicalEventsHostedEnabled(env.READ_API_CANONICAL_EVENTS_HOSTED) || !rawStoreFromEnv(env, fetcher)) return undefined;
  return {
    source: workerCanonicalSource(env, sqlCanonicalPointers(run), fetcher),
    ...(exactTrueFlag(env.MESSAGE_ORIGIN_LABELS_ENABLED) ? { originLabels: true } : {}),
  };
}

/**
 * The read API deps for hosted canonical events: `canonicalEvents` (above) plus
 * `canonicalEventsHosted` with READ_API_CANONICAL_EVENTS_WORKSPACES (unset/empty = no workspace)
 * and the session-sources reader. Off: {} (the deps carry neither key).
 */
export function workerCanonicalEventsHosted(env: Env, run: TenantRunner, fetcher?: typeof fetch): Pick<ReadApiDeps, "canonicalEvents" | "canonicalEventsHosted"> {
  const canonicalEvents = canonicalEventsDeps(env, run, fetcher);
  if (!canonicalEvents) return {};
  return { canonicalEvents, canonicalEventsHosted: { workspaces: parseDirectoryWorkspaces(env.READ_API_CANONICAL_EVENTS_WORKSPACES), sources: sqlSessionSources(run) } };
}

/**
 * Relay session turns and metadata from canonical S3, on the same pointers and raw-store
 * secrets as timing. Without the secrets the routes answer 501 `turns_not_configured`.
 */
export function turnsDeps(env: Env, run: Parameters<typeof sqlCanonicalPointers>[0], fetcher: typeof fetch = (input, init) => fetch(input, init)): TurnsDeps | undefined {
  const timing = timingDeps(env, run, fetcher);
  if (!timing) return undefined;
  return { harnesses: sqlSessionHarnesses(run), source: timing.source };
}

/**
 * Babysitter evidence from the retention buckets (src/session-read-api/babysitter-evidence.ts):
 * buckets and revisions through the read role (tenant-bound), objects with the raw-store
 * secrets. Off unless READ_API_BABYSITTER_EVIDENCE=on and the raw store is configured.
 */
export function babysitterEvidenceDeps(env: Env, run: TenantRunner, fetcher: typeof fetch = (input, init) => fetch(input, init)): EvidenceDeps | undefined {
  if ((env.READ_API_BABYSITTER_EVIDENCE ?? "").trim().toLowerCase() !== "on") return undefined;
  if (!rawStoreFromEnv(env, fetcher)) return undefined;
  const keyRoot = env.READ_API_BABYSITTER_KEY_ROOT?.trim() || "96bulls";
  if (!/^[a-z0-9][a-z0-9-]{0,62}$/.test(keyRoot)) return undefined;
  // A store per request, its fetcher bound to the request's deadline: list and get both abort.
  const storeFor = (signal: AbortSignal) =>
    rawStoreFromEnv(env, ((input, init) => fetcher(input, { ...init, signal: init?.signal ? AbortSignal.any([init.signal, signal]) : signal })) as typeof fetch)?.store;
  return sqlEvidenceDeps(run as never, storeFor as never, (body) => readAll(body as never) /* the bound fetch aborts the body stream too */, (bytes) => decompress(bytes), (key) => objectKey(key) as never, keyRoot);
}

/**
 * The isolate's index verifier (READ_API_OWNER_BUFFERED_INDEX_VERIFY=on): module scope, so a valid
 * answer is kept for the isolate's lifetime and a failure is re-probed after 60 s, across
 * requests. Each request's probe runs on that request's own pg client.
 */
const ISOLATE_INDEX_VERIFIER = new BufferedIndexVerifier();

/**
 * Owner-only buffered reads (Option C): the two SECURITY DEFINER readers through the read role
 * (tenant-bound, user bound per call) and GetObject on the ingest bucket's buffered-batches/
 * stage objects, gated by the HISTORY_CONTROL_HEAD binding (absent with the flag on: handle()
 * answers 503 before this runs; the gate still fences if it ever is). Served only to
 * workspaces on READ_API_OWNER_BUFFERED_WORKSPACES (and the directory allowlist). Undefined unless RELAYHISTORY_READ_OWNER_BUFFERED is "1"/"true", the bucket
 * is named and the raw-store keys exist.
 */
export function ownerBufferedDeps(
  env: Env,
  run: TenantRunner,
  fetcher: typeof fetch = (input, init) => fetch(input, init),
  indexVerifier: BufferedIndexVerifier = ISOLATE_INDEX_VERIFIER,
): OwnerBufferedDeps | undefined {
  if (!ownerBufferedEnabled(env.RELAYHISTORY_READ_OWNER_BUFFERED)) return undefined;
  const bucket = env.INGEST_S3_BUCKET?.trim();
  const accessKeyId = env.PIPELINE_RAW_S3_ACCESS_KEY_ID;
  const secretAccessKey = env.PIPELINE_RAW_S3_SECRET_ACCESS_KEY;
  if (!bucket || !accessKeyId || !secretAccessKey) return undefined;
  return {
    enabled: true,
    workspaces: parseDirectoryWorkspaces(env.READ_API_OWNER_BUFFERED_WORKSPACES),
    reads: sqlOwnerBufferedReads(run, { statementTimeoutMs: ownerBufferedStatementTimeoutFlag(env), exactSession: ownerBufferedExactSessionEnabled(env.READ_API_OWNER_BUFFERED_EXACT_SESSION), stagesPaged: ownerBufferedStagesPagedEnabled(env.READ_API_OWNER_BUFFERED_STAGES_PAGED) }),
    gate: serviceBindingControlHead(env.HISTORY_CONTROL_HEAD),
    store: createS3Store({ bucket, region: env.INGEST_S3_REGION?.trim() || "us-east-1", credentials: { accessKeyId, secretAccessKey }, fetch: fetcher }),
    decompress: boundedStreamDecompress(Decompress) /* stops at the bound; fzstd's one-shot form with a fixed buffer silently truncates */,
    // Session events from buffered stages only with READ_API_OWNER_BUFFERED_EVENTS=on (handle() refused any other value).
    ...(ownerBufferedEventsFlag(env) ? { events: true } : {}),
    // Bounded-concurrency stage fetch only with READ_API_OWNER_BUFFERED_STAGE_CONCURRENCY=2..32; otherwise serial.
    ...(ownerBufferedStageConcurrencyFlag(env) > 1 ? { stageConcurrency: ownerBufferedStageConcurrencyFlag(env) } : {}),
    // Per-phase log fields only with READ_API_OWNER_BUFFERED_PHASE_LOG=1.
    ...(ownerBufferedPhaseLogEnabled(env.READ_API_OWNER_BUFFERED_PHASE_LOG) ? { phaseLog: true } : {}),
    // Out-of-band index check only with READ_API_OWNER_BUFFERED_INDEX_VERIFY=on: this request's
    // read connection probes; the isolate's verifier caches the answer.
    ...(ownerBufferedIndexVerifyFlag(env) ? { indexCheck: indexVerifier.check(sqlBufferedIndexProbe(run)) } : {}),
  };
}

/** READ_API_OWNER_BUFFERED_INDEX_VERIFY as a boolean; a refused value is off here (handle() already answered 503 for it). */
function ownerBufferedIndexVerifyFlag(env: Env): boolean {
  try {
    return ownerBufferedIndexVerifyEnabled(env.READ_API_OWNER_BUFFERED_INDEX_VERIFY);
  } catch {
    return false;
  }
}

/** READ_API_OWNER_BUFFERED_STATEMENT_TIMEOUT_MS as ms; a refused value is 2000 (handle() already answered 503 for it). */
function ownerBufferedStatementTimeoutFlag(env: Env): number {
  try {
    return ownerBufferedStatementTimeoutMs(env.READ_API_OWNER_BUFFERED_STATEMENT_TIMEOUT_MS);
  } catch {
    return 2000;
  }
}

/** READ_API_OWNER_BUFFERED_STAGE_CONCURRENCY as a number; a refused value is 1, serial (handle() already answered 503 for it). */
function ownerBufferedStageConcurrencyFlag(env: Env): number {
  try {
    return ownerBufferedStageConcurrency(env.READ_API_OWNER_BUFFERED_STAGE_CONCURRENCY);
  } catch {
    return 1;
  }
}

/** READ_API_OWNER_BUFFERED_EVENTS as a boolean; a refused value is off here (handle() already answered 503 for it). */
function ownerBufferedEventsFlag(env: Env): boolean {
  try {
    return ownerBufferedEventsEnabled(env.READ_API_OWNER_BUFFERED_EVENTS);
  } catch {
    return false;
  }
}

export default {
  fetch(request, env, ctx) {
    return handle(request, env, ctx);
  },
} satisfies ExportedHandler<Env>;
