/**
 * Test-only signer for the relaygate v1 upstream assertion, written from the wire contract (not imported from
 * relaygate): `v1.<base64url JSON>.<base64url Ed25519 signature>` over "relaygate-upstream-v1\n" + the payload
 * segment. Keys are generated in memory per test; nothing is written to disk.
 */
import { generateKeyPairSync, sign, type KeyObject } from "node:crypto";
import { sessionScope, sha256Hex, type UpstreamAssertionAccess, type UpstreamAssertionClaims } from "../upstream-assertion.ts";

export interface TestKey {
  kid: string;
  privateKey: KeyObject;
  /** Canonical standard base64 SPKI DER, the verifier's env value. */
  publicB64: string;
}

export function testKey(kid: string): TestKey {
  const { privateKey, publicKey } = generateKeyPairSync("ed25519");
  return { kid, privateKey, publicB64: publicKey.export({ format: "der", type: "spki" }).toString("base64") };
}

export interface SignInput {
  key: TestKey;
  aud: "session-read-api" | "ingest-edge" | string;
  method: string;
  path: string;
  authorization: string;
  body?: string | Uint8Array;
  access: UpstreamAssertionAccess | string;
  sessions?: string[];
  nowS?: number;
  ttl?: number;
  /** Overrides applied to the claims after they are built (raw JSON values, may break the shape on purpose). */
  claims?: Record<string, unknown>;
}

export function claimsFor(input: SignInput): Record<string, unknown> {
  const iat = input.nowS ?? Math.floor(Date.now() / 1000);
  const scope = sessionScope(input.sessions ?? []);
  const claims: UpstreamAssertionClaims = {
    v: 1,
    alg: "Ed25519",
    iss: "relaygate",
    aud: input.aud,
    kid: input.key.kid,
    iat,
    exp: iat + (input.ttl ?? 30),
    jti: "test-jti-1",
    method: input.method,
    path: input.path,
    body_sha256: sha256Hex(input.body ?? ""),
    credential_sha256: sha256Hex(input.authorization),
    principal: { agent_id: "agent-test", gid: 1000, groups: [1000] },
    access: input.access as UpstreamAssertionAccess,
    session_ids: scope.inline,
    session_count: scope.count,
    session_ids_sha256: scope.digest,
  };
  return { ...claims, ...input.claims };
}

/** Signs a payload segment (already base64url) with the contract's prefix. */
export function signSegment(key: TestKey, payloadSegment: string): string {
  return sign(null, Buffer.from(`relaygate-upstream-v1\n${payloadSegment}`, "utf8"), key.privateKey).toString("base64url");
}

export function tokenFromJson(key: TestKey, json: string): string {
  const payload = Buffer.from(json, "utf8").toString("base64url");
  return `v1.${payload}.${signSegment(key, payload)}`;
}

export function signAssertion(input: SignInput): string {
  return tokenFromJson(input.key, JSON.stringify(claimsFor(input)));
}
