/**
 * Analytics destination selection, from configuration only (helm-charts
 * docs/analytics-adapter-contract.md). Default OFF: unset, empty or "noop" is the
 * NoopSink, and no event leaves the process.
 *
 *   ANALYTICS_SINK            unset | "" | "noop" | "none" (default: off) | "collector" | "posthog"; exact, case-sensitive.
 *                             Anything else refuses (startup on the Node services, every request 503 on a
 *                             Worker), so a typo never silently means either.
 *   ANALYTICS_DEPLOYMENT_ID   collector/posthog: the opaque installation identity on every envelope
 *                             (required; [A-Za-z0-9][A-Za-z0-9._:-]{0,127}; never derived or defaulted).
 *
 * Self-host (session-ingest, session-read-api, session-pipeline-consumer and session-brief) accepts
 * noop or collector only:
 *   ANALYTICS_COLLECTOR_URL   the collector's full POST URL, path included and ending in /v1/events (the
 *                             adapter never appends it): https, loopback http, or an in-cluster Service
 *                             name with ALLOW_IN_CLUSTER_HTTP=true; no credentials, query or fragment.
 *   ANALYTICS_COLLECTOR_TOKEN the per-producer bearer, Secret-injected (never on disk, never logged).
 * `posthog` is refused, and so is any ANALYTICS_POSTHOG_* value: a self-host workload never carries a
 * hosted key or endpoint, and a collector failure never falls back to the hosted destination.
 *
 * Hosted (the Cloudflare Workers: session read, session ingest) accepts noop or posthog only:
 *   ANALYTICS_POSTHOG_HOST    https origin, default https://i.agentrelay.com (our PostHog).
 *   ANALYTICS_POSTHOG_KEY     the project key, a Worker secret (Secrets Store on the ingest Worker); never logged.
 * `collector` and any ANALYTICS_COLLECTOR_* value are refused: the hosted Workers have no in-cluster collector.
 *
 * Errors name the variable and the rule, never a value.
 */
import { NOOP_ANALYTICS, type AnalyticsComponent, type AnalyticsCounters, type AnalyticsSink, opaqueId } from "./analytics.ts";
import { collectorSink, postHogSink, SERVICE_LIMITS, WORKER_LIMITS, type AnalyticsLimits, type Fetcher } from "./analytics-export.ts";
import { assertDestinationUrl, destinationPolicy } from "./destination-url.ts";

export const ANALYTICS_SINK_VAR = "ANALYTICS_SINK";
export const ANALYTICS_DEPLOYMENT_ID_VAR = "ANALYTICS_DEPLOYMENT_ID";
export const ANALYTICS_COLLECTOR_URL_VAR = "ANALYTICS_COLLECTOR_URL";
export const ANALYTICS_COLLECTOR_TOKEN_VAR = "ANALYTICS_COLLECTOR_TOKEN";
export const ANALYTICS_POSTHOG_HOST_VAR = "ANALYTICS_POSTHOG_HOST";
export const ANALYTICS_POSTHOG_KEY_VAR = "ANALYTICS_POSTHOG_KEY";
/** Our hosted PostHog (the contract's hosted destination). Only ever used with ANALYTICS_SINK=posthog. */
export const DEFAULT_POSTHOG_HOST = "https://i.agentrelay.com";
/** The collector interface: Helm supplies the complete URL, including this path. */
export const COLLECTOR_PATH = "/v1/events";

export type AnalyticsProfile = "selfhost" | "hosted";

export type AnalyticsConfig =
  | { sink: "noop" }
  | { sink: "collector"; deploymentId: string; url: string; token: string }
  | { sink: "posthog"; deploymentId: string; host: string; apiKey: string };

export const ANALYTICS_OFF: AnalyticsConfig = Object.freeze({ sink: "noop" as const });

/** A refused analytics setting; `variable` names it (the Worker logs `missing_<variable>`). */
export class AnalyticsConfigError extends Error {
  constructor(readonly variable: string, rule: string) {
    super(`96bulls: ${variable} ${rule}`);
    this.name = "AnalyticsConfigError";
  }
}

type Env = Record<string, unknown>;

/** A string setting, trimmed; unset, empty or a non-string (a binding) is undefined. */
function value(env: Env, name: string): string | undefined {
  const v = env[name];
  return typeof v === "string" && v.trim() !== "" ? v.trim() : undefined;
}

/** RFC 6750 b64token, bounded: anything that could not be one header value is refused. */
const BEARER_TOKEN = /^[A-Za-z0-9\-._~+/]{16,4096}=*$/;
/** PostHog project keys (phc_...), bounded. */
const POSTHOG_KEY = /^[A-Za-z0-9_-]{8,256}$/;

function refuseSet(env: Env, names: string[], why: string): void {
  for (const name of names) if (env[name] !== undefined && env[name] !== "") throw new AnalyticsConfigError(name, `is refused here (${why})`);
}

/** The analytics selection for `profile`. Parse once at startup (Node) or per request (Worker). */
export function analyticsConfig(env: Env, profile: AnalyticsProfile): AnalyticsConfig {
  const raw = env[ANALYTICS_SINK_VAR];
  if (raw !== undefined && typeof raw !== "string") throw new AnalyticsConfigError(ANALYTICS_SINK_VAR, "must be noop, collector or posthog");
  // "none" is the collector interface's name for the same default.
  const sink = raw === undefined || raw === "" || raw === "none" ? "noop" : raw;
  if (sink !== "noop" && sink !== "collector" && sink !== "posthog") throw new AnalyticsConfigError(ANALYTICS_SINK_VAR, "must be noop, collector or posthog");
  if (profile === "selfhost") {
    refuseSet(env, [ANALYTICS_POSTHOG_HOST_VAR, ANALYTICS_POSTHOG_KEY_VAR], "a self-host workload never carries a hosted analytics key or endpoint");
    if (sink === "posthog") throw new AnalyticsConfigError(ANALYTICS_SINK_VAR, "must be noop or collector on a self-host service (never the hosted PostHog)");
  } else {
    refuseSet(env, [ANALYTICS_COLLECTOR_URL_VAR, ANALYTICS_COLLECTOR_TOKEN_VAR], "the hosted Workers have no in-cluster collector");
    if (sink === "collector") throw new AnalyticsConfigError(ANALYTICS_SINK_VAR, "must be noop or posthog on a hosted Worker");
  }
  if (sink === "noop") return ANALYTICS_OFF;
  const deploymentId = opaqueId(value(env, ANALYTICS_DEPLOYMENT_ID_VAR));
  if (!deploymentId) throw new AnalyticsConfigError(ANALYTICS_DEPLOYMENT_ID_VAR, `is required with ${ANALYTICS_SINK_VAR}=${sink} ([A-Za-z0-9][A-Za-z0-9._:-]{0,127})`);
  if (sink === "collector") {
    const url = value(env, ANALYTICS_COLLECTOR_URL_VAR);
    if (!url) throw new AnalyticsConfigError(ANALYTICS_COLLECTOR_URL_VAR, `is required with ${ANALYTICS_SINK_VAR}=collector`);
    let parsed: URL;
    try {
      parsed = assertDestinationUrl(ANALYTICS_COLLECTOR_URL_VAR, url, destinationPolicy(env as Record<string, string | undefined>));
    } catch {
      throw new AnalyticsConfigError(ANALYTICS_COLLECTOR_URL_VAR, "must be https, loopback http, or an in-cluster Service URL with ALLOW_IN_CLUSTER_HTTP=true, with no credentials, query or fragment");
    }
    if (!parsed.pathname.endsWith(COLLECTOR_PATH)) throw new AnalyticsConfigError(ANALYTICS_COLLECTOR_URL_VAR, `must be the complete URL ending in ${COLLECTOR_PATH}`);
    const token = value(env, ANALYTICS_COLLECTOR_TOKEN_VAR);
    if (!token || !BEARER_TOKEN.test(token)) throw new AnalyticsConfigError(ANALYTICS_COLLECTOR_TOKEN_VAR, `is required with ${ANALYTICS_SINK_VAR}=collector (a bearer token, 16..4096 characters)`);
    return { sink, deploymentId, url: parsed.href, token };
  }
  const host = value(env, ANALYTICS_POSTHOG_HOST_VAR) ?? DEFAULT_POSTHOG_HOST;
  let origin: URL;
  try {
    origin = new URL(host);
  } catch {
    throw new AnalyticsConfigError(ANALYTICS_POSTHOG_HOST_VAR, "must be an https origin");
  }
  if (origin.protocol !== "https:" || origin.username || origin.password || origin.search || origin.hash || origin.pathname !== "/")
    throw new AnalyticsConfigError(ANALYTICS_POSTHOG_HOST_VAR, "must be an https origin (no path, credentials, query or fragment)");
  const apiKey = value(env, ANALYTICS_POSTHOG_KEY_VAR);
  if (!apiKey || !POSTHOG_KEY.test(apiKey)) throw new AnalyticsConfigError(ANALYTICS_POSTHOG_KEY_VAR, `is required with ${ANALYTICS_SINK_VAR}=posthog`);
  return { sink, deploymentId, host: origin.origin, apiKey };
}

export interface SinkOptions {
  fetch?: Fetcher;
  limits?: Partial<AnalyticsLimits>;
  report?: (counters: Readonly<AnalyticsCounters>) => void;
}

/**
 * The sink for `config`, bound to `component`. `runtime` picks the bounds: "service" (a long-lived
 * Node process: timer flush, shutdown flush) or "worker" (one invocation: flush in waitUntil only).
 */
export function createAnalyticsSink(config: AnalyticsConfig, component: AnalyticsComponent, runtime: "service" | "worker", options: SinkOptions = {}): AnalyticsSink {
  if (config.sink === "noop") return NOOP_ANALYTICS;
  const limits = { ...(runtime === "worker" ? WORKER_LIMITS : SERVICE_LIMITS), ...options.limits };
  const producer = { component, deploymentId: config.deploymentId };
  const common = { limits, ...(options.fetch ? { fetch: options.fetch } : {}), ...(options.report ? { report: options.report } : {}) };
  return config.sink === "collector"
    ? collectorSink(producer, { url: config.url, token: config.token, ...common })
    : postHogSink(producer, { host: config.host, apiKey: config.apiKey, ...common });
}

/** The counters as allowlisted log fields (src/shared/log.ts BOUNDED_COUNT_FIELDS `analytics_*`). */
export function analyticsCounterFields(destination: string, c: Readonly<AnalyticsCounters>): Record<string, unknown> {
  return {
    state: destination,
    analytics_accepted: c.accepted,
    analytics_dropped_queue_full: c.dropped_queue_full,
    analytics_dropped_invalid: c.dropped_invalid,
    analytics_dropped_oversize: c.dropped_oversize,
    analytics_dropped_rejected: c.dropped_rejected,
    analytics_dropped_auth: c.dropped_auth,
    analytics_dropped_exhausted: c.dropped_exhausted,
    analytics_dropped_shutdown: c.dropped_shutdown,
    analytics_export_failures: c.export_failures,
  };
}
