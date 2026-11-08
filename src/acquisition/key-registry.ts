/**
 * Pinned issuer public keys (ACQUISITION_PINNED_KEYS). §4.1: S verifies only
 * against a key pinned here, never a key named only by the request.
 *
 * Verify-only: this module holds public keys and nothing else. The pipeline
 * never mints, signs or stores acquisition credentials; relaycontrol is the
 * sole issuer (docs/acquisition-exchange.md, "Issuer boundary"). Errors name
 * the variable, never its value.
 */
import { createPublicKey, type KeyObject } from "node:crypto";
import { isIdentifier, type IssuerKey, type IssuerRegistry, type OwnerKind } from "./validation.ts";

/** DER prefix of an Ed25519 SPKI public key (RFC 8410). */
const SPKI_ED25519_PREFIX = Buffer.from("302a300506032b6570032100", "hex");

export interface PinnedKeySpec {
  readonly issuerKeyId: string;
  readonly issuerClass: OwnerKind;
  readonly publicKeyBase64: string;
}

function strictBase64(value: string, bytes: number, name: string): Buffer {
  const decoded = Buffer.from(value, "base64");
  if (decoded.byteLength !== bytes || decoded.toString("base64") !== value) throw new Error(`96bulls: ${name} must be base64 of ${bytes} bytes`);
  return decoded;
}

export function ed25519PublicKey(raw: Uint8Array): KeyObject {
  return createPublicKey({ key: Buffer.concat([SPKI_ED25519_PREFIX, raw]), format: "der", type: "spki" });
}

export function rawPublicKey(key: KeyObject): Buffer {
  const der = key.export({ format: "der", type: "spki" });
  return Buffer.from(der.subarray(SPKI_ED25519_PREFIX.byteLength));
}

/**
 * ACQUISITION_PINNED_KEYS: a JSON array of {issuerKeyId, issuerClass, publicKeyBase64}.
 * Ids are unique, and so are public keys: one key pinned under two ids (of the same or
 * different classes) would let a native-issued S verify as cloud-issued, so it is refused.
 */
export function parsePinnedKeys(raw: string, name = "ACQUISITION_PINNED_KEYS"): IssuerRegistry {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    throw new Error(`96bulls: ${name} must be a JSON array`);
  }
  if (!Array.isArray(parsed) || parsed.length === 0) throw new Error(`96bulls: ${name} must be a non-empty JSON array`);
  const registry = new Map<string, IssuerKey>();
  const publicKeys = new Set<string>();
  for (const entry of parsed) {
    if (entry === null || typeof entry !== "object" || Array.isArray(entry)) throw new Error(`96bulls: ${name} entries must be objects`);
    const keys = Object.keys(entry).sort().join(",");
    if (keys !== "issuerClass,issuerKeyId,publicKeyBase64") throw new Error(`96bulls: ${name} entries need exactly issuerKeyId, issuerClass, publicKeyBase64`);
    const { issuerKeyId, issuerClass, publicKeyBase64 } = entry as Record<string, unknown>;
    if (typeof issuerKeyId !== "string" || !isIdentifier(issuerKeyId)) throw new Error(`96bulls: ${name} has an invalid issuerKeyId`);
    if (issuerClass !== "cloud" && issuerClass !== "native") throw new Error(`96bulls: ${name} issuerClass must be cloud or native`);
    if (typeof publicKeyBase64 !== "string") throw new Error(`96bulls: ${name} publicKeyBase64 must be a string`);
    if (registry.has(issuerKeyId)) throw new Error(`96bulls: ${name} has a duplicate issuerKeyId`);
    const raw32 = strictBase64(publicKeyBase64, 32, name);
    const canonical = raw32.toString("hex");
    if (publicKeys.has(canonical)) throw new Error(`96bulls: ${name} pins the same public key under two issuerKeyIds`);
    publicKeys.add(canonical);
    registry.set(issuerKeyId, Object.freeze({ ownerKind: issuerClass, publicKey: ed25519PublicKey(raw32) }));
  }
  return registry;
}

/** The subset of a registry with one issuer class (route groups accept only their own class). */
export function registryOfClass(registry: IssuerRegistry, issuerClass: OwnerKind): IssuerRegistry {
  return new Map([...registry].filter(([, key]) => key.ownerKind === issuerClass));
}
