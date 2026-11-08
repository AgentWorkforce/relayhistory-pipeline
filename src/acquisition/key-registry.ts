/**
 * Pinned issuer public keys (ACQUISITION_PINNED_KEYS) and the native issuer's
 * own Ed25519 signing key (ACQUISITION_ISSUER_PRIVATE_KEY). §4.1: S verifies
 * only against a key pinned here, never a key named only by the request.
 *
 * The private key is parsed once into a non-extractable-by-API closure; the
 * signer exposes only `issuerKeyId`, `publicKey` and `sign(A)`. Errors name
 * the variable, never its value.
 */
import { createPrivateKey, createPublicKey, sign, type KeyObject } from "node:crypto";
import { isIdentifier, type IssuerKey, type IssuerRegistry, type OwnerKind } from "./validation.ts";

/** DER prefix of an Ed25519 PKCS8 private key (RFC 8410) and SPKI public key. */
const PKCS8_ED25519_PREFIX = Buffer.from("302e020100300506032b657004220420", "hex");
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

export interface IssuerSigner {
  readonly issuerKeyId: string;
  readonly publicKey: KeyObject;
  sign(A: Uint8Array): Buffer;
}

/** ACQUISITION_ISSUER_PRIVATE_KEY: a PKCS8 PEM Ed25519 key, or base64 of the 32-byte seed. */
export function parseIssuerPrivateKey(raw: string, name = "ACQUISITION_ISSUER_PRIVATE_KEY"): KeyObject {
  let key: KeyObject;
  try {
    key = raw.includes("-----BEGIN")
      ? createPrivateKey({ key: raw, format: "pem" })
      : createPrivateKey({ key: Buffer.concat([PKCS8_ED25519_PREFIX, strictBase64(raw.trim(), 32, name)]), format: "der", type: "pkcs8" });
  } catch {
    throw new Error(`96bulls: ${name} must be a PKCS8 PEM Ed25519 key or base64 of a 32-byte seed`);
  }
  if (key.asymmetricKeyType !== "ed25519") throw new Error(`96bulls: ${name} must be an Ed25519 key`);
  return key;
}

/**
 * The native issuer's signer. Its public key must be pinned in `registry`
 * under `issuerKeyId` as a native-class key, so the installer side would
 * accept what this issuer signs (and a mis-pasted key fails at startup).
 */
export function createIssuerSigner(issuerKeyId: string, privateKey: KeyObject, registry: IssuerRegistry): IssuerSigner {
  const pinned = registry.get(issuerKeyId);
  const publicKey = createPublicKey(privateKey);
  if (!pinned || pinned.ownerKind !== "native" || !rawPublicKey(pinned.publicKey).equals(rawPublicKey(publicKey)))
    throw new Error("96bulls: ACQUISITION_ISSUER_KEY_ID must name a native-class key in ACQUISITION_PINNED_KEYS whose public key matches ACQUISITION_ISSUER_PRIVATE_KEY");
  return Object.freeze({
    issuerKeyId,
    publicKey,
    sign: (A: Uint8Array) => sign(null, A, privateKey),
    toJSON: () => ({ issuerKeyId }),
  });
}

/**
 * §9 key retention: on rotation an old public key stays pinned (as the same
 * class) while any outstanding intent, reserved revoke, publication outbox or
 * receipt replay still references it. `referenced` is every issuerKeyId the
 * caller knows to be outstanding; a missing or re-classed one fails closed.
 * Key ids are not secret, so the error names them.
 */
export function assertKeysRetained(registry: IssuerRegistry, referenced: Iterable<string>, issuerClass: OwnerKind, name = "ACQUISITION_PINNED_KEYS"): void {
  const missing = [...new Set(referenced)].filter((id) => registry.get(id)?.ownerKind !== issuerClass).sort();
  if (missing.length) throw new Error(`96bulls: ${name} must retain ${issuerClass} issuer keys still referenced by outstanding intents: ${missing.join(",")}`);
}
