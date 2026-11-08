/**
 * Configuration of the acquisition exchange (SERVICE=acquisition), from the
 * environment only, default off. docs/acquisition-exchange.md has the table.
 *
 * Verify-only: the pipeline never mints, signs or stores acquisition
 * credentials; relaycontrol is the sole issuer. There is no issuer key, intent
 * key or native-exchange switch. Any issuer-era variable that is set refuses
 * startup (REFUSED_ISSUER_ENV), whether or not ACQUISITION_ENABLED is on, so a
 * signing key can never sit unused in this service's environment.
 *
 *   ACQUISITION_ENABLED            master switch: unset | "" | "false" (default) | "true"; anything else is a
 *                                  startup error. Off: nothing else is read; every route but /healthz is
 *                                  503 {"error":{"code":"acquisition_disabled"}}
 *   ACQUISITION_OPS_ENABLED        the private operations port (same strict boolean; default off)
 *   ACQUISITION_ORIGIN             this service's origin, exactly scheme://host[:port]; the introspection aud
 *   ACQUISITION_PORT               dormant public listener (healthz only, every other route 503), default 8080
 *   ACQUISITION_OPS_PORT           operations listener, default 8082 (must differ; 8081 is the worker
 *                                  health port the proof NetworkPolicy opens VPC-wide)
 *   ACQUISITION_OPS_HOST           operations bind address, default 127.0.0.1; exposing it further needs a
 *                                  NetworkPolicy change (docs). The dormant listener binds 0.0.0.0.
 *   (The port and host variables are read and validated even when ACQUISITION_ENABLED is off.)
 *   ACQUISITION_PINNED_KEYS        JSON [{issuerKeyId, issuerClass:"cloud", publicKeyBase64}]: relaycontrol's
 *                                  published public keys. Only cloud-class keys are accepted; a native-class
 *                                  entry refuses startup (no native issuer; relaycontrol is the sole issuer).
 *                                  Tracked gap: native (Desktop) install/rebind/revoke assertions from
 *                                  relaycontrol cannot use the ops route unchanged (cloud-class Installer;
 *                                  ops rebind always 403). Native enrollment stays unimplemented until a
 *                                  dedicated receiver class/scope is designed in a follow-up PR; issuing side
 *                                  owned by relay-control (layered-control). Do not restore pipeline signing
 *                                  or trust cloud-class issuance for native.
 *   ACQUISITION_DATABASE_URL       Postgres URL for role bulls96_acquisition_installer (never logged)
 *   RELAYCONTROL_INTROSPECT_URL    relaycontrol base URL (#313; https, loopback or in-cluster per destination policy)
 *   RELAYCONTROL_INTROSPECT_SECRET the dedicated introspection secret
 *
 * Errors name the variable, never its value.
 */
import { isIP } from "node:net";
import { normalizedAudience } from "@rhc/auth/relaycontrol.ts";
import { assertDestinationUrl, destinationPolicy } from "../shared/destination-url.ts";
import { parsePinnedKeys } from "./key-registry.ts";
import type { IssuerRegistry } from "./validation.ts";

export const ACQUISITION_FLAG = "ACQUISITION_ENABLED";

/**
 * Issuer-era variables. The pipeline is verify-only, so a deployment that
 * still sets any of them is misconfigured: refuse to start (fail closed)
 * rather than silently ignore it. Presence alone refuses (even empty); the
 * value is never read into the error. ACQUISITION_NATIVE_ENABLED is refused
 * unless it is unset, empty or "false".
 */
export const REFUSED_ISSUER_ENV = ["ACQUISITION_ISSUER_PRIVATE_KEY", "ACQUISITION_ISSUER_KEY_ID", "ACQUISITION_INTENT_KEY"] as const;

export function assertVerifyOnlyEnv(env: NodeJS.ProcessEnv): void {
  for (const name of REFUSED_ISSUER_ENV)
    if (env[name] !== undefined)
      throw new Error(`96bulls: ${name} is set but the acquisition service is verify-only (relaycontrol is the sole issuer); remove it`);
  const native = env.ACQUISITION_NATIVE_ENABLED?.trim();
  if (native !== undefined && native !== "" && native !== "false")
    throw new Error("96bulls: ACQUISITION_NATIVE_ENABLED is not supported: the acquisition service is verify-only (relaycontrol is the sole issuer); remove it");
}

export interface AcquisitionLiveConfig {
  readonly enabled: true;
  readonly opsEnabled: boolean;
  readonly port: number;
  readonly opsPort: number;
  readonly opsHost: string;
  readonly origin: string;
  readonly registry: IssuerRegistry;
  readonly databaseUrl: string;
  readonly introspectUrl: string;
  readonly introspectSecret: string;
}
export type AcquisitionConfig = { readonly enabled: false; readonly port: number; readonly opsPort: number; readonly opsHost: string } | AcquisitionLiveConfig;

/** Exactly "true" enables; unset, empty or "false" is off; anything else throws. */
export function strictFlag(env: NodeJS.ProcessEnv, name: string): boolean {
  const raw = env[name]?.trim();
  if (raw === undefined || raw === "" || raw === "false") return false;
  if (raw === "true") return true;
  throw new Error(`96bulls: ${name} must be true or false`);
}

function required(env: NodeJS.ProcessEnv, name: string): string {
  const value = env[name]?.trim();
  if (!value) throw new Error(`96bulls: ${ACQUISITION_FLAG}=true needs ${name}`);
  return value;
}

function port(env: NodeJS.ProcessEnv, name: string, fallback: number): number {
  const raw = env[name]?.trim();
  if (!raw) return fallback;
  const value = Number(raw);
  if (!/^[0-9]+$/.test(raw) || !Number.isInteger(value) || value < 1 || value > 65_535) throw new Error(`96bulls: ${name} must be a TCP port`);
  return value;
}

/** Already in #313's normalized form, so the aud sent is exactly what both sides compare. */
export function acquisitionOrigin(raw: string): string {
  if (normalizedAudience(raw) !== raw)
    throw new Error("96bulls: ACQUISITION_ORIGIN must be exactly scheme://host[:port] (lowercase, no path or trailing slash)");
  return raw;
}

/** An IPv4/IPv6 literal bind address (no hostnames: the bind must not depend on DNS). */
export function bindHost(env: NodeJS.ProcessEnv, name: string, fallback: string): string {
  const raw = env[name]?.trim();
  if (raw === undefined || raw === "") return fallback;
  if (isIP(raw) === 0) throw new Error(`96bulls: ${name} must be an IP address literal`);
  return raw;
}

export function acquisitionConfig(env: NodeJS.ProcessEnv = process.env): AcquisitionConfig {
  assertVerifyOnlyEnv(env);
  const ports = {
    port: port(env, "ACQUISITION_PORT", 8080),
    opsPort: port(env, "ACQUISITION_OPS_PORT", 8082),
    opsHost: bindHost(env, "ACQUISITION_OPS_HOST", "127.0.0.1"),
  };
  if (ports.port === ports.opsPort) throw new Error("96bulls: ACQUISITION_PORT and ACQUISITION_OPS_PORT must differ");
  if (!strictFlag(env, ACQUISITION_FLAG)) return { enabled: false, ...ports };
  const opsEnabled = strictFlag(env, "ACQUISITION_OPS_ENABLED");
  const policy = destinationPolicy(env);
  const origin = acquisitionOrigin(required(env, "ACQUISITION_ORIGIN"));
  const introspectUrl = required(env, "RELAYCONTROL_INTROSPECT_URL");
  assertDestinationUrl("RELAYCONTROL_INTROSPECT_URL", introspectUrl, policy);
  const registry = parsePinnedKeys(required(env, "ACQUISITION_PINNED_KEYS"));
  return {
    enabled: true,
    opsEnabled,
    ...ports,
    origin,
    registry,
    databaseUrl: required(env, "ACQUISITION_DATABASE_URL"),
    introspectUrl,
    introspectSecret: required(env, "RELAYCONTROL_INTROSPECT_SECRET"),
  };
}
