/**
 * Configuration of the self-host session ingest edge (SERVICE=session-ingest,
 * selfhost-server.ts), from the environment only. The hosted Cloudflare Worker
 * (worker.ts) never reads this file; nothing here changes the hosted path.
 *
 *   RELAYHISTORY_SELFHOST_INGEST     unset | "" | "false" (default: dormant) | exactly "true"; anything
 *                                    else is a startup error, so a typo never silently means either
 *   RELAYHISTORY_INGEST_PORT         listen port, default 8081 (parsed even when dormant)
 *   RELAYHISTORY_SELFHOST_SESSION_SELECTION
 *                                    unset | "" | "false" (default: off) | exactly "true"; anything else is a
 *                                    startup error. Serves POST /v1/delivery/sessions/selection with enabled:false
 *                                    (per-session erase) and needs migration 043910 applied.
 *   RELAYHISTORY_SELFHOST_HEARTBEAT_ACCEPT
 *                                    unset | "" | "false" (default: off, the route answers 501) | exactly "true";
 *                                    anything else is a startup error. Serves POST /v1/onboarding/heartbeat as an
 *                                    authenticated (rth:sync) 204 that discards the body: nothing is stored or logged.
 *   SESSION_ERASE_API_ENABLED        operator per-session erase route (session-erase.ts): exactly "1" enables;
 *                                    unset, "" or "0" is off; anything else is a startup error. With it,
 *                                    SESSION_ERASE_API_SECRET (+ _PREVIOUS during a rotation; Secret-injected, min
 *                                    32 chars). Flag off or secret unset: /internal/v1/session-erase/* answers 501.
 *
 * Enabled only (each missing or malformed value refuses startup, naming the variable, never the value):
 *   RELAYHISTORY_INGEST_ORIGIN       this edge's public origin, sent to relaycontrol as `aud` (must be the
 *                                    origin relaycontrol binds the Desktop token to)
 *   RELAYCONTROL_INTROSPECT_URL      relaycontrol's introspection base URL (destination policy:
 *                                    https, loopback, or a k8s Service name with ALLOW_IN_CLUSTER_HTTP=true)
 *   RELAYCONTROL_INTROSPECT_SECRET   the introspection service secret (Secret-injected, never on disk)
 *   RELAYHISTORY_INGEST_DATABASE_URL the bulls96_session_ingest login (Secret-injected). Only host, port,
 *                                    user, password and database are used; the query string never reaches pg
 *   SESSION_POSTGRES_TLS_PROFILE     required explicitly: supabase | custom-ca (with SESSION_POSTGRES_CA_FILE /
 *                                    SESSION_POSTGRES_CA_SHA256); no implicit default
 *   LOCKER_BUCKET (+ LOCKER_ENDPOINT, LOCKER_REGION, LOCKER_ACCESS_KEY_ID / LOCKER_SECRET_ACCESS_KEY or
 *     EKS Pod Identity, or LOCKER_PROVIDER=gcs)   the object store, exactly as every other self-host
 *                                    service builds it (src/shared/locker.ts)
 *   INGEST_KEY_ROOT                  object key root, default "96bulls" (same as the Worker)
 *   INGEST_MAX_PER_USER / INGEST_MAX_PER_ORG   in-flight batch slots, defaults 4 / 32 (same as the Worker)
 *   RELAYHISTORY_INGEST_PG_POOL_MAX  pool size, 1..32, default 4
 *
 *   SELFHOST_INGEST_ADMISSION        unset | "" | "sync" (default: today's synchronous path, unchanged) |
 *                                    "async" | "drain" (selfhost-admission.ts). Config only: no request
 *                                    header can select it. async: the upload is ACKed after one create-only
 *                                    inbox PUT, with no SQL on the request path, and an in-process consumer
 *                                    admits it. drain: the synchronous path again, while the consumer keeps
 *                                    admitting what is already in the inbox (the rollback step)
 *   With async or drain only:
 *   SELFHOST_ADMISSION_CONCURRENCY   admissions in flight in this process, 1..32, default 4
 *   SELFHOST_ADMISSION_SCAN_MS       the inbox ListBucket scan interval, 1000..600000, default 15000
 *   SELFHOST_ADMISSION_MAX_ATTEMPTS  attempts before an inbox object is dead-lettered, 1..100, default 20
 *
 *   ANALYTICS_SINK                   unset | "" | "noop" (default: no analytics) | "collector"; "posthog" and any
 *                                    ANALYTICS_POSTHOG_* value refuse startup (src/shared/analytics-config.ts).
 *                                    collector needs ANALYTICS_DEPLOYMENT_ID, ANALYTICS_COLLECTOR_URL (the full
 *                                    in-cluster URL ending in /v1/events) and ANALYTICS_COLLECTOR_TOKEN (Secret-injected)
 *
 * Refused when enabled (they describe hosted-only edges this process does not have):
 * INGEST_ACCEPTANCE_MODE other than canonical, INGEST_INLINE_MAX_SESSIONS (no fan-out queue: every
 * batch is written inline, see selfhost-server.ts), INGEST_REPLICA_MODE other than off,
 * INGEST_CUSTODY_SOURCE other than relayhistory/unset, INGEST_PRIVACY_AUTHORITY other than neon/unset,
 * INGEST_SIGNING_KEY (the relayhistory HMAC is never accepted here).
 */
import { assertDestinationUrl, destinationPolicy } from "../shared/destination-url.ts";
import { normalizedAudience } from "@rhc/auth/relaycontrol.ts";
import { postgresConnectionString, postgresTlsOptions } from "../shared/postgres-tls.ts";
import type pg from "pg";
import { sessionEraseConfig, type SessionEraseConfig } from "./session-erase.ts";
import { analyticsConfig, type AnalyticsConfig } from "../shared/analytics-config.ts";

export const SELFHOST_INGEST_FLAG = "RELAYHISTORY_SELFHOST_INGEST";
export const SELFHOST_INGEST_PORT_VAR = "RELAYHISTORY_INGEST_PORT";
export const SELFHOST_SESSION_SELECTION_FLAG = "RELAYHISTORY_SELFHOST_SESSION_SELECTION";
export const SELFHOST_HEARTBEAT_ACCEPT_FLAG = "RELAYHISTORY_SELFHOST_HEARTBEAT_ACCEPT";
export const SELFHOST_INGEST_ORIGIN_VAR = "RELAYHISTORY_INGEST_ORIGIN";
export const SELFHOST_INGEST_DATABASE_URL_VAR = "RELAYHISTORY_INGEST_DATABASE_URL";
export const SELFHOST_INGEST_POOL_MAX_VAR = "RELAYHISTORY_INGEST_PG_POOL_MAX";
export const SELFHOST_INGEST_DEFAULT_PORT = 8081;
/** The only login the ledger's SECURITY DEFINER functions admit (supabase/bootstrap/session_ingest_role.sql). */
export const SELFHOST_INGEST_ROLE = "bulls96_session_ingest";

export const SELFHOST_INGEST_ADMISSION_VAR = "SELFHOST_INGEST_ADMISSION";

/** SELFHOST_INGEST_ADMISSION=async|drain; absent for sync (the default). */
export interface SelfhostAdmissionSettings {
  mode: "async" | "drain";
  concurrency: number;
  scanIntervalMs: number;
  maxAttempts: number;
}

export interface SelfhostIngestSettings {
  port: number;
  /** Normalized origin (scheme + host + non-default port). */
  origin: string;
  introspectUrl: string;
  introspectSecret: string;
  /** Discrete pg fields; no URL query string ever reaches pg. */
  pool: pg.PoolConfig;
  keyRoot: string;
  maxPerUser: number;
  maxPerOrg: number;
  /** RELAYHISTORY_SELFHOST_SESSION_SELECTION: serve the per-session erase route. Default off. */
  sessionSelection: boolean;
  /** RELAYHISTORY_SELFHOST_HEARTBEAT_ACCEPT: answer the Desktop's onboarding heartbeat 204 (discarded). Default off. */
  heartbeatAccept: boolean;
  /** SESSION_ERASE_API_ENABLED (session-erase.ts). Default: disabled, the route answers 501. */
  sessionErase: SessionEraseConfig;
  /** Present only for SELFHOST_INGEST_ADMISSION=async|drain. */
  admission?: SelfhostAdmissionSettings;
  /** ANALYTICS_* (src/shared/analytics-config.ts, self-host profile: noop or collector). Absent = noop. */
  analytics?: AnalyticsConfig;
}

export type SelfhostIngestConfig = { enabled: false; port: number } | ({ enabled: true } & SelfhostIngestSettings);

/** Exactly "true" enables; unset, empty or "false" is off; anything else throws. */
export function selfhostIngestEnabled(env: NodeJS.ProcessEnv = process.env): boolean {
  const raw = env[SELFHOST_INGEST_FLAG];
  if (raw === undefined || raw === "" || raw === "false") return false;
  if (raw === "true") return true;
  throw new Error(`96bulls: ${SELFHOST_INGEST_FLAG} must be true or false`);
}

/** Exactly "true" enables; unset, empty or "false" is off; anything else throws (a typo never silently means either). */
export function selfhostSessionSelectionEnabled(env: NodeJS.ProcessEnv = process.env): boolean {
  const raw = env[SELFHOST_SESSION_SELECTION_FLAG];
  if (raw === undefined || raw === "" || raw === "false") return false;
  if (raw === "true") return true;
  throw new Error(`96bulls: ${SELFHOST_SESSION_SELECTION_FLAG} must be true or false`);
}

/** Exactly "true" enables; unset, empty or "false" is off; anything else throws (a typo never silently means either). */
export function selfhostHeartbeatAcceptEnabled(env: NodeJS.ProcessEnv = process.env): boolean {
  const raw = env[SELFHOST_HEARTBEAT_ACCEPT_FLAG];
  if (raw === undefined || raw === "" || raw === "false") return false;
  if (raw === "true") return true;
  throw new Error(`96bulls: ${SELFHOST_HEARTBEAT_ACCEPT_FLAG} must be true or false`);
}

function port(env: NodeJS.ProcessEnv): number {
  const raw = env[SELFHOST_INGEST_PORT_VAR]?.trim();
  if (!raw) return SELFHOST_INGEST_DEFAULT_PORT;
  const value = Number(raw);
  if (!Number.isInteger(value) || value < 1 || value > 65_535) throw new Error(`96bulls: ${SELFHOST_INGEST_PORT_VAR} must be a TCP port`);
  return value;
}

function bounded(env: NodeJS.ProcessEnv, name: string, fallback: number, lo: number, hi: number): number {
  const raw = env[name]?.trim();
  if (!raw) return fallback;
  const n = Number(raw);
  if (!Number.isSafeInteger(n) || n < lo || n > hi) throw new Error(`96bulls: ${name} must be an integer from ${lo} to ${hi}`);
  return n;
}

function need(env: NodeJS.ProcessEnv, name: string): string {
  const value = env[name]?.trim();
  if (!value) throw new Error(`96bulls: ${SELFHOST_INGEST_FLAG}=true needs ${name}`);
  return value;
}

/** SELFHOST_INGEST_ADMISSION: exactly sync (or unset / empty), async or drain; anything else refuses startup. */
export function selfhostAdmissionSettings(env: NodeJS.ProcessEnv): SelfhostAdmissionSettings | undefined {
  const raw = env[SELFHOST_INGEST_ADMISSION_VAR];
  if (raw === undefined || raw === "" || raw === "sync") return undefined;
  if (raw !== "async" && raw !== "drain") throw new Error(`96bulls: ${SELFHOST_INGEST_ADMISSION_VAR} must be sync, async or drain`);
  return {
    mode: raw,
    concurrency: bounded(env, "SELFHOST_ADMISSION_CONCURRENCY", 4, 1, 32),
    scanIntervalMs: bounded(env, "SELFHOST_ADMISSION_SCAN_MS", 15_000, 1_000, 600_000),
    maxAttempts: bounded(env, "SELFHOST_ADMISSION_MAX_ATTEMPTS", 20, 1, 100),
  };
}

/** Discrete fields from the sanitized URL (the read API service's rule): user, password and database required. */
export function ingestPoolTarget(url: string): pg.PoolConfig {
  let target: URL;
  try {
    target = new URL(postgresConnectionString(url));
  } catch {
    throw new Error(`96bulls: ${SELFHOST_INGEST_DATABASE_URL_VAR} is invalid`);
  }
  let user: string, password: string, database: string;
  try {
    user = decodeURIComponent(target.username);
    password = decodeURIComponent(target.password);
    database = decodeURIComponent(target.pathname.replace(/^\//u, ""));
  } catch {
    throw new Error(`96bulls: ${SELFHOST_INGEST_DATABASE_URL_VAR} is invalid`);
  }
  if (!user || !password || !database || database.includes("/") || target.hash)
    throw new Error(`96bulls: ${SELFHOST_INGEST_DATABASE_URL_VAR} needs a user, password and database`);
  return { host: target.hostname.replace(/^\[(.*)\]$/u, "$1"), port: target.port ? Number(target.port) : 5432, user, password, database };
}

/** Hosted-only settings that must not silently do nothing here. */
function refuseHostedOnly(env: NodeJS.ProcessEnv): void {
  const set = (name: string) => (env[name]?.trim() ?? "") !== "";
  const refuse = (name: string, why: string) => {
    throw new Error(`96bulls: the self-host session ingest refuses ${name} (${why})`);
  };
  if (set("INGEST_ACCEPTANCE_MODE") && env.INGEST_ACCEPTANCE_MODE!.trim() !== "canonical") refuse("INGEST_ACCEPTANCE_MODE", "only canonical");
  if (set("INGEST_INLINE_MAX_SESSIONS")) refuse("INGEST_INLINE_MAX_SESSIONS", "every batch is written inline; there is no fan-out queue");
  if (set("INGEST_REPLICA_MODE") && env.INGEST_REPLICA_MODE!.trim() !== "off") refuse("INGEST_REPLICA_MODE", "no Neon replica");
  if (set("INGEST_CUSTODY_SOURCE") && env.INGEST_CUSTODY_SOURCE!.trim() !== "relayhistory") refuse("INGEST_CUSTODY_SOURCE", "no custody source");
  if (set("INGEST_PRIVACY_AUTHORITY") && env.INGEST_PRIVACY_AUTHORITY!.trim() !== "neon") refuse("INGEST_PRIVACY_AUTHORITY", "no custody source");
  if (set("INGEST_SIGNING_KEY")) refuse("INGEST_SIGNING_KEY", "the relayhistory HMAC identity is never accepted here");
}

/** Parse everything once at startup. Errors name the variable, never its value. */
export function selfhostIngestConfig(env: NodeJS.ProcessEnv = process.env): SelfhostIngestConfig {
  if (!selfhostIngestEnabled(env)) return { enabled: false, port: port(env) };
  const sessionSelection = selfhostSessionSelectionEnabled(env);
  const heartbeatAccept = selfhostHeartbeatAcceptEnabled(env);
  const sessionErase = sessionEraseConfig(env);
  const policy = destinationPolicy(env);
  refuseHostedOnly(env);
  const originRaw = need(env, SELFHOST_INGEST_ORIGIN_VAR);
  assertDestinationUrl(SELFHOST_INGEST_ORIGIN_VAR, originRaw, policy, { allowPath: false, allowAnyHttpHost: true });
  const origin = normalizedAudience(originRaw);
  if (!origin) throw new Error(`96bulls: ${SELFHOST_INGEST_ORIGIN_VAR} must be an absolute http(s) origin`);
  const introspectUrl = need(env, "RELAYCONTROL_INTROSPECT_URL");
  assertDestinationUrl("RELAYCONTROL_INTROSPECT_URL", introspectUrl, policy);
  const introspectSecret = need(env, "RELAYCONTROL_INTROSPECT_SECRET");
  // The self-host service never inherits the hosted Supabase pin by omission.
  if (!env.SESSION_POSTGRES_TLS_PROFILE) throw new Error(`96bulls: ${SELFHOST_INGEST_FLAG}=true needs SESSION_POSTGRES_TLS_PROFILE set explicitly`);
  const ssl = postgresTlsOptions(env);
  const target = ingestPoolTarget(need(env, SELFHOST_INGEST_DATABASE_URL_VAR));
  need(env, "LOCKER_BUCKET");
  const keyRoot = env.INGEST_KEY_ROOT?.trim() || "96bulls";
  if (!/^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$/u.test(keyRoot)) throw new Error("96bulls: INGEST_KEY_ROOT must be a single key segment");
  const admission = selfhostAdmissionSettings(env);
  const analytics = analyticsConfig(env, "selfhost");
  return {
    enabled: true,
    port: port(env),
    origin,
    introspectUrl,
    introspectSecret,
    pool: { ...target, ssl, max: bounded(env, SELFHOST_INGEST_POOL_MAX_VAR, 4, 1, 32) },
    keyRoot,
    maxPerUser: bounded(env, "INGEST_MAX_PER_USER", 4, 1, 1000),
    maxPerOrg: bounded(env, "INGEST_MAX_PER_ORG", 32, 1, 1000),
    sessionSelection,
    heartbeatAccept,
    sessionErase,
    ...(admission ? { admission } : {}),
    ...(analytics.sink !== "noop" ? { analytics } : {}),
  };
}
