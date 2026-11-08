/**
 * Configuration of the acquisition exchange (SERVICE=acquisition), from the
 * environment only, default off. docs/acquisition-exchange.md has the table.
 *
 *   ACQUISITION_ENABLED            master switch: unset | "" | "false" (default) | "true"; anything else is a
 *                                  startup error. Off: nothing else is read; every route but /healthz is
 *                                  503 {"error":{"code":"acquisition_disabled"}}
 *   ACQUISITION_NATIVE_ENABLED     the Desktop-facing native exchange (same strict boolean; default off)
 *   ACQUISITION_OPS_ENABLED        the private operations port (same strict boolean; default off)
 *   ACQUISITION_ORIGIN             this service's origin, exactly scheme://host[:port]; the introspection aud
 *   ACQUISITION_PORT               native listener, default 8080
 *   ACQUISITION_OPS_PORT           operations listener, default 8082 (must differ; 8081 is the worker
 *                                  health port the proof NetworkPolicy opens VPC-wide)
 *   ACQUISITION_OPS_HOST           operations bind address, default 127.0.0.1; exposing it further needs a
 *                                  NetworkPolicy change (docs). The native listener binds 0.0.0.0.
 *   (The port and host variables are read and validated even when ACQUISITION_ENABLED is off.)
 *   ACQUISITION_PINNED_KEYS        JSON [{issuerKeyId, issuerClass:"cloud"|"native", publicKeyBase64}]
 *   ACQUISITION_ISSUER_KEY_ID      native only: this issuer's pinned native-class key id
 *   ACQUISITION_ISSUER_PRIVATE_KEY native only: PKCS8 PEM or base64 32-byte seed; read once, never logged
 *   ACQUISITION_INTENT_KEY         native only: base64 of 32 bytes, AES-256-GCM key for G at rest
 *   ACQUISITION_DATABASE_URL       Postgres URL for role bulls96_acquisition_installer (never logged)
 *   RELAYCONTROL_INTROSPECT_URL    relaycontrol base URL (#313; https, loopback or in-cluster per destination policy)
 *   RELAYCONTROL_INTROSPECT_SECRET the dedicated introspection secret
 *
 * Errors name the variable, never its value.
 */
import type { KeyObject } from "node:crypto";
import { isIP } from "node:net";
import { normalizedAudience } from "@rhc/auth/relaycontrol.ts";
import { assertDestinationUrl, destinationPolicy } from "../shared/destination-url.ts";
import { createIssuerSigner, parseIssuerPrivateKey, parsePinnedKeys, type IssuerSigner } from "./key-registry.ts";
import type { IssuerRegistry } from "./validation.ts";

export const ACQUISITION_FLAG = "ACQUISITION_ENABLED";

export interface AcquisitionLiveConfig {
  readonly enabled: true;
  readonly nativeEnabled: boolean;
  readonly opsEnabled: boolean;
  readonly port: number;
  readonly opsPort: number;
  readonly opsHost: string;
  readonly origin: string;
  readonly registry: IssuerRegistry;
  readonly databaseUrl: string;
  readonly introspectUrl: string;
  readonly introspectSecret: string;
  /** Present iff nativeEnabled. */
  readonly signer: IssuerSigner | null;
  readonly intentKey: Buffer | null;
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

export function intentKey(raw: string): Buffer {
  const key = Buffer.from(raw, "base64");
  if (!/^[A-Za-z0-9+/]+={0,2}$/.test(raw) || key.byteLength !== 32 || key.toString("base64") !== raw)
    throw new Error("96bulls: ACQUISITION_INTENT_KEY must be base64 of exactly 32 bytes");
  return key;
}

/** An IPv4/IPv6 literal bind address (no hostnames: the bind must not depend on DNS). */
export function bindHost(env: NodeJS.ProcessEnv, name: string, fallback: string): string {
  const raw = env[name]?.trim();
  if (raw === undefined || raw === "") return fallback;
  if (isIP(raw) === 0) throw new Error(`96bulls: ${name} must be an IP address literal`);
  return raw;
}

export function acquisitionConfig(env: NodeJS.ProcessEnv = process.env): AcquisitionConfig {
  const ports = {
    port: port(env, "ACQUISITION_PORT", 8080),
    opsPort: port(env, "ACQUISITION_OPS_PORT", 8082),
    opsHost: bindHost(env, "ACQUISITION_OPS_HOST", "127.0.0.1"),
  };
  if (ports.port === ports.opsPort) throw new Error("96bulls: ACQUISITION_PORT and ACQUISITION_OPS_PORT must differ");
  if (!strictFlag(env, ACQUISITION_FLAG)) return { enabled: false, ...ports };
  const nativeEnabled = strictFlag(env, "ACQUISITION_NATIVE_ENABLED");
  const opsEnabled = strictFlag(env, "ACQUISITION_OPS_ENABLED");
  const policy = destinationPolicy(env);
  const origin = acquisitionOrigin(required(env, "ACQUISITION_ORIGIN"));
  const introspectUrl = required(env, "RELAYCONTROL_INTROSPECT_URL");
  assertDestinationUrl("RELAYCONTROL_INTROSPECT_URL", introspectUrl, policy);
  const registry = parsePinnedKeys(required(env, "ACQUISITION_PINNED_KEYS"));
  let signer: IssuerSigner | null = null;
  let key: Buffer | null = null;
  if (nativeEnabled) {
    const privateKey: KeyObject = parseIssuerPrivateKey(required(env, "ACQUISITION_ISSUER_PRIVATE_KEY"));
    signer = createIssuerSigner(required(env, "ACQUISITION_ISSUER_KEY_ID"), privateKey, registry);
    key = intentKey(required(env, "ACQUISITION_INTENT_KEY"));
  }
  return {
    enabled: true,
    nativeEnabled,
    opsEnabled,
    ...ports,
    origin,
    registry,
    databaseUrl: required(env, "ACQUISITION_DATABASE_URL"),
    introspectUrl,
    introspectSecret: required(env, "RELAYCONTROL_INTROSPECT_SECRET"),
    signer,
    intentKey: key,
  };
}
