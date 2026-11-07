/**
 * Configuration of the session-ingest edge (SERVICE=session-ingest-edge), from
 * the environment only. docs/session-ingest-edge.md.
 *
 *   SESSION_INGEST_EDGE_ENABLED     unset | "" | "false" (default: dormant) | "true"; anything else is a
 *                                   startup error, so a typo never silently means either
 *   SESSION_INGEST_EDGE_ORIGIN      this edge's public origin, exactly scheme://host[:port]; sent as the
 *                                   introspection `aud` and required back verbatim. The operator lists the
 *                                   same string in relaycontrol's SELFHOST_INTROSPECT_AUDIENCES
 *   SESSION_INGEST_UPSTREAM_URL     the session-ingest Worker, a bare origin (no path); https, loopback, or
 *                                   an in-cluster Service name with ALLOW_IN_CLUSTER_HTTP=true
 *   RELAYCONTROL_INTROSPECT_URL     relaycontrol's base URL (same transport rule)
 *   RELAYCONTROL_INTROSPECT_SECRET  the dedicated introspection secret (never the user token)
 *   INGEST_SIGNING_KEY              the HMAC key the Worker verifies (src/session-ingest/app.ts signRequest)
 *   SESSION_INGEST_EDGE_PORT        listen port, default 8080
 *
 * Dormant (flag off): nothing else is read or required; the process starts and
 * answers 503 not_configured on every route but /healthz. Enabled: every value
 * above is required and validated at startup; errors name the variable, never
 * its value.
 */
import { assertDestinationUrl, destinationPolicy } from "../shared/destination-url.ts";

export const EDGE_FLAG = "SESSION_INGEST_EDGE_ENABLED";

export type EdgeConfig =
  | { enabled: false; port: number }
  | {
      enabled: true;
      port: number;
      origin: string;
      upstreamUrl: string;
      introspectUrl: string;
      introspectSecret: string;
      signingKey: string;
    };

/** `SESSION_INGEST_EDGE_ENABLED`: exactly "true" enables; unset, empty or "false" is off; anything else throws. */
export function edgeEnabled(env: NodeJS.ProcessEnv = process.env): boolean {
  const raw = env[EDGE_FLAG]?.trim();
  if (raw === undefined || raw === "" || raw === "false") return false;
  if (raw === "true") return true;
  throw new Error(`96bulls: ${EDGE_FLAG} must be true or false`);
}

function requiredValue(env: NodeJS.ProcessEnv, name: string): string {
  const value = env[name]?.trim();
  if (!value) throw new Error(`96bulls: ${EDGE_FLAG}=true needs ${name}`);
  return value;
}

/** An exact bare http(s) origin (what relaycontrol compares and echoes as `aud`). */
export function edgeOrigin(raw: string): string {
  let url: URL;
  try {
    url = new URL(raw);
  } catch {
    throw new Error("96bulls: SESSION_INGEST_EDGE_ORIGIN must be an absolute http(s) origin");
  }
  if ((url.protocol !== "https:" && url.protocol !== "http:") || url.username || url.password || url.origin !== raw)
    throw new Error("96bulls: SESSION_INGEST_EDGE_ORIGIN must be exactly scheme://host[:port] (lowercase, no path or trailing slash)");
  return raw;
}

function port(env: NodeJS.ProcessEnv): number {
  const raw = env.SESSION_INGEST_EDGE_PORT?.trim();
  if (!raw) return 8080;
  const value = Number(raw);
  if (!Number.isInteger(value) || value < 1 || value > 65_535)
    throw new Error("96bulls: SESSION_INGEST_EDGE_PORT must be a TCP port");
  return value;
}

export function edgeConfig(env: NodeJS.ProcessEnv = process.env): EdgeConfig {
  if (!edgeEnabled(env)) return { enabled: false, port: port(env) };
  const policy = destinationPolicy(env);
  const origin = edgeOrigin(requiredValue(env, "SESSION_INGEST_EDGE_ORIGIN"));
  const upstreamUrl = requiredValue(env, "SESSION_INGEST_UPSTREAM_URL");
  assertDestinationUrl("SESSION_INGEST_UPSTREAM_URL", upstreamUrl, policy, { allowPath: false });
  const introspectUrl = requiredValue(env, "RELAYCONTROL_INTROSPECT_URL");
  assertDestinationUrl("RELAYCONTROL_INTROSPECT_URL", introspectUrl, policy);
  return {
    enabled: true,
    port: port(env),
    origin,
    upstreamUrl,
    introspectUrl,
    introspectSecret: requiredValue(env, "RELAYCONTROL_INTROSPECT_SECRET"),
    signingKey: requiredValue(env, "INGEST_SIGNING_KEY"),
  };
}
