/**
 * Configuration of the self-host pipeline consumer service
 * (SERVICE=session-pipeline-consumer, consumer-service.ts) and its one-shot
 * owner bootstrap (SERVICE=session-pipeline-consumer-bootstrap,
 * consumer-bootstrap.ts), from the environment only. Mirrors the
 * session-read-api service-config pattern: dormant by default, and a typo
 * never silently means either.
 *
 *   RELAYHISTORY_SELFHOST_PIPELINE_CONSUMER  unset | "" | "false" (default: dormant) |
 *                                   exactly "true" (no trimming, case-sensitive);
 *                                   anything else is a startup error
 *   RELAYHISTORY_PIPELINE_CONSUMER_SECRET     enabled only: the consumer secret JSON
 *                                   ({ hoopdb_url, search? }, owner URL for bootstrap,
 *                                   runtime-role URL for the reconciler; never a
 *                                   reader URL). A clickhouse block is refused:
 *                                   the hosted binding has no place here
 *   SEARCH_INDEX_BACKEND                      enabled only: postgres | opensearch (managed
 *                                   AWS index, SEARCH_INDEX_OPENSEARCH_*; projection state
 *                                   stays in Postgres); anything else fails startup
 *   SEARCH_INDEX_POSTGRES_BACKEND_ID          enabled only: the projection-state backend id
 *                                   the search writer records under (required;
 *                                   not a credential). Format is validated by the
 *                                   projection state itself
 *                                   ("search state configuration invalid")
 *   SESSION_POSTGRES_TLS_PROFILE              enabled: required explicitly (supabase |
 *                                   custom-ca with SESSION_POSTGRES_CA_FILE /
 *                                   SESSION_POSTGRES_CA_SHA256). There is no implicit
 *                                   default here: the shared helper would otherwise
 *                                   silently pin the Supabase CA on a self-host install
 *   PIPELINE_KEY_PREFIX                       enabled: the bare deployment object base
 *                                   (e.g. `96bulls`; the org is appended at
 *                                   runtime as `<base>/<orgId>`), required.
 *                                   A trailing org-shaped segment (e.g.
 *                                   `96bulls/org_1`) fails startup: it would
 *                                   double the org on every key
 *   PIPELINE_RECONCILE_LOOKBACK_MINUTES       reconcile window in minutes, default 30,
 *                                   minimum 1; must be greater than EVERY
 *   CONSUMER_RECONCILE_EVERY_MINUTES          poll cadence in minutes, default 5, minimum 1
 *   PIPELINE_FANOUT                           off (only supported value: the self-host
 *                                   consumer is terminal and fans out nothing;
 *                                   postgres is refused)
 *   CONSUMER_UNHEALTHY_FILE                   path written when consecutive tick
 *                                   failures force an exit (for a compose
 *                                   healthcheck or watcher); default
 *                                   /tmp/session-pipeline-consumer.unhealthy
 *   HOOPDB_URL                                runWorker (health/readiness gates) opens its
 *                                   own pool from this variable. It must equal the
 *                                   secret's hoopdb_url (same role, same database);
 *                                   the service sets it from the secret when unset
 *                                   and refuses to start on a mismatch, so the live
 *                                   gates can never evaluate a different identity
 *                                   than the one that writes
 *   ANALYTICS_SINK                            unset | "" | "noop" (default: no analytics) |
 *                                   "collector" (one installation-scoped
 *                                   session_pipeline.index_tick_completed per completed
 *                                   tick: counts only). "posthog" and any ANALYTICS_POSTHOG_*
 *                                   value refuse startup (src/shared/analytics-config.ts)
 *
 * Dormant (flag off): nothing else is read or required; the process idles
 * without opening the database. Errors name the variable, never its value.
 */
import { searchSecret } from "../shared/search-composition.ts";
import { parseCompleteSince } from "../session-read-api/config.ts";
import { destinationPolicy } from "../shared/destination-url.ts";
import { analyticsConfig, type AnalyticsConfig } from "../shared/analytics-config.ts";

export const CONSUMER_SERVICE_FLAG = "RELAYHISTORY_SELFHOST_PIPELINE_CONSUMER";
export const CONSUMER_SECRET_VAR = "RELAYHISTORY_PIPELINE_CONSUMER_SECRET";
export const CONSUMER_BACKEND_VAR = "SEARCH_INDEX_BACKEND";
export const CONSUMER_BACKEND_ID_VAR = "SEARCH_INDEX_POSTGRES_BACKEND_ID";
export const CONSUMER_KEY_PREFIX_VAR = "PIPELINE_KEY_PREFIX";
export const CONSUMER_TLS_PROFILE_VAR = "SESSION_POSTGRES_TLS_PROFILE";

export interface ConsumerSecret {
  hoopdb_url: string;
  search?: unknown;
}

export type ConsumerSearchBackend = "postgres" | "opensearch";

export type ConsumerServiceConfig =
  | { enabled: false }
  | {
      enabled: true;
      secret: ConsumerSecret;
      searchBackend: ConsumerSearchBackend;
      backendId: string;
      /** Coverage floor shared with the reader (partials treats any other floor as stale). */
      floorMs: number;
      keyPrefix: string;
      lookbackMinutes: number;
      everyMinutes: number;
      fanout: "off";
      unhealthyFile: string;
      /** ANALYTICS_* (src/shared/analytics-config.ts, self-host profile: noop or collector). Absent = noop. */
      analytics?: AnalyticsConfig;
    };

/** `RELAYHISTORY_SELFHOST_PIPELINE_CONSUMER`: exactly "true" enables; unset, empty or "false" is off; anything throws. */
export function consumerServiceEnabled(env: NodeJS.ProcessEnv = process.env): boolean {
  const raw = env[CONSUMER_SERVICE_FLAG];
  if (raw === undefined || raw === "" || raw === "false") return false;
  if (raw === "true") return true;
  throw new Error(`96bulls: ${CONSUMER_SERVICE_FLAG} must be true or false`);
}

/** Strict positive-int knob: read from the given env (never process.env), minimum 1; anything else throws naming the variable. */
export function strictMinutes(env: NodeJS.ProcessEnv, name: string, fallback: number): number {
  const raw = env[name];
  if (raw === undefined || raw === "") return fallback;
  const value = Number(raw);
  if (!Number.isInteger(value) || value < 1) throw new Error(`96bulls: ${name} must be an integer number of minutes, at least 1`);
  return value;
}

/** Parse and check the secret's shape. Never echoes a value. */
export function parseConsumerSecret(text: string): ConsumerSecret {
  let value: unknown;
  try {
    value = JSON.parse(text);
  } catch {
    throw new Error("pipeline consumer secret is not JSON");
  }
  const v = value as Partial<ConsumerSecret> & { clickhouse?: unknown } | null;
  if (!v || typeof v.hoopdb_url !== "string" || !/^postgres(ql)?:\/\//.test(v.hoopdb_url))
    throw new Error("pipeline consumer secret has no hoopdb_url");
  if (v.clickhouse !== undefined)
    throw new Error("pipeline consumer secret must not carry a clickhouse block in self-host");
  if (v.search !== undefined) searchSecret(v.search);
  return { hoopdb_url: v.hoopdb_url, ...(v.search !== undefined ? { search: v.search } : {}) };
}

export function consumerServiceConfig(env: NodeJS.ProcessEnv = process.env): ConsumerServiceConfig {
  if (!consumerServiceEnabled(env)) return { enabled: false };
  destinationPolicy(env); // ALLOW_IN_CLUSTER_HTTP: a typo fails startup even when no URL is set
  const secretText = env[CONSUMER_SECRET_VAR];
  if (!secretText) throw new Error(`96bulls: ${CONSUMER_SERVICE_FLAG}=true needs ${CONSUMER_SECRET_VAR}`);
  const profile = env[CONSUMER_TLS_PROFILE_VAR];
  if (profile !== "supabase" && profile !== "custom-ca")
    throw new Error(`96bulls: ${CONSUMER_SERVICE_FLAG}=true needs ${CONSUMER_TLS_PROFILE_VAR} to be supabase or custom-ca`);
  const backend = env[CONSUMER_BACKEND_VAR];
  if (backend !== "postgres" && backend !== "opensearch") throw new Error(`96bulls: ${CONSUMER_BACKEND_VAR} must be postgres or opensearch for the self-host consumer`);
  const backendId = env[CONSUMER_BACKEND_ID_VAR];
  if (!backendId) throw new Error(`96bulls: ${CONSUMER_SERVICE_FLAG}=true needs ${CONSUMER_BACKEND_ID_VAR}`);
  const keyPrefix = env[CONSUMER_KEY_PREFIX_VAR];
  if (!keyPrefix) throw new Error(`96bulls: ${CONSUMER_SERVICE_FLAG}=true needs ${CONSUMER_KEY_PREFIX_VAR}`);
  // Org-from-prefix layout appends the org at runtime (`<base>/<orgId>`): a
  // configured base that already ends in an org-shaped segment would double
  // the org on every key, so fail startup instead of misrouting silently.
  // The error names the variable, never the value.
  if (/^org[-_][A-Za-z0-9_-]+$/.test(keyPrefix.split("/").pop() ?? ""))
    throw new Error(`96bulls: ${CONSUMER_KEY_PREFIX_VAR} must be the bare deployment base; it must not end in an org-shaped segment`);
  const fanout = env.PIPELINE_FANOUT ?? "off";
  if (fanout !== "off") throw new Error("pipeline consumer: PIPELINE_FANOUT must be off (the self-host consumer fans out nothing)");
  const lookbackMinutes = strictMinutes(env, "PIPELINE_RECONCILE_LOOKBACK_MINUTES", 30);
  const everyMinutes = strictMinutes(env, "CONSUMER_RECONCILE_EVERY_MINUTES", 5);
  if (lookbackMinutes <= everyMinutes)
    throw new Error("pipeline consumer: PIPELINE_RECONCILE_LOOKBACK_MINUTES must be greater than CONSUMER_RECONCILE_EVERY_MINUTES");
  // Same parser as the reader: unset or malformed fails closed, never a silent 0.
  // The writer must record the reader's floor or partials lists every session as stale.
  const floorMs = parseCompleteSince(env.READ_API_COMPLETE_SINCE);
  const unhealthyFile = env.CONSUMER_UNHEALTHY_FILE ?? "/tmp/session-pipeline-consumer.unhealthy";
  const analytics = analyticsConfig(env, "selfhost");
  return {
    enabled: true, secret: parseConsumerSecret(secretText), searchBackend: backend, backendId, floorMs, keyPrefix, lookbackMinutes, everyMinutes, fanout, unhealthyFile,
    ...(analytics.sink !== "noop" ? { analytics } : {}),
  };
}
