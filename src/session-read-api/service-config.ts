/**
 * Configuration of the self-host read API service (SERVICE=session-read-api,
 * server.ts), from the environment only. config.ts stays runtime-neutral for
 * the Worker; this file is the service's switch and port, in the
 * session-ingest-edge pattern.
 *
 *   RELAYHISTORY_SELFHOST_READ_API  unset | "" | "false" (default: dormant) | exactly "true" (no trimming, case-sensitive); anything else is a
 *                                   startup error, so a typo never silently means either
 *   RELAYHISTORY_READ_API_PORT      listen port, default 8080 (parsed even when dormant)
 *   RELAYHISTORY_READ_API_SECRET    enabled only: the read API secret JSON, the same shape the Lambda
 *                                   reads from Secrets Manager (lambda.ts parseReadApiSecret), injected
 *                                   from the secret store; never on disk
 *
 * Dormant (flag off): nothing else is read or required; the process starts and
 * answers 503 not_configured on every route but /healthz. Enabled: the secret is
 * required and ALLOW_IN_CLUSTER_HTTP is checked here; the remaining read API env
 * (READ_API_COMPLETE_SINCE, READ_API_ORIGIN, RELAYAUTH_*, PGSSL*, ...) is validated
 * at startup by the same assembly the Lambda uses (lambda.ts readAppFromSecret).
 * Errors name the variable, never its value.
 */
import { destinationPolicy } from "../shared/destination-url.ts";

export const READ_API_SERVICE_FLAG = "RELAYHISTORY_SELFHOST_READ_API";
export const READ_API_PORT_VAR = "RELAYHISTORY_READ_API_PORT";
export const READ_API_SECRET_VAR = "RELAYHISTORY_READ_API_SECRET";

export type ReadApiServiceConfig =
  | { enabled: false; port: number }
  | { enabled: true; port: number; secret: string };

/** `RELAYHISTORY_SELFHOST_READ_API`: exactly "true" enables; unset, empty or "false" is off; anything else throws. */
export function readApiServiceEnabled(env: NodeJS.ProcessEnv = process.env): boolean {
  const raw = env[READ_API_SERVICE_FLAG];
  if (raw === undefined || raw === "" || raw === "false") return false;
  if (raw === "true") return true;
  throw new Error(`96bulls: ${READ_API_SERVICE_FLAG} must be true or false`);
}

function port(env: NodeJS.ProcessEnv): number {
  const raw = env[READ_API_PORT_VAR]?.trim();
  if (!raw) return 8080;
  const value = Number(raw);
  if (!Number.isInteger(value) || value < 1 || value > 65_535)
    throw new Error(`96bulls: ${READ_API_PORT_VAR} must be a TCP port`);
  return value;
}

export function readApiServiceConfig(env: NodeJS.ProcessEnv = process.env): ReadApiServiceConfig {
  if (!readApiServiceEnabled(env)) return { enabled: false, port: port(env) };
  destinationPolicy(env); // ALLOW_IN_CLUSTER_HTTP: a typo fails startup even when no URL is set
  const secret = env[READ_API_SECRET_VAR]?.trim();
  if (!secret) throw new Error(`96bulls: ${READ_API_SERVICE_FLAG}=true needs ${READ_API_SECRET_VAR}`);
  return { enabled: true, port: port(env), secret };
}
