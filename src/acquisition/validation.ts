/**
 * Small shared types and request validators for src/acquisition. The A/S codec
 * and verifier are NOT here: they are the peer module
 * src/session-ingest/acquisition-assertion.ts (PR #322), imported directly.
 */
import type { AcquisitionOperation, AcquisitionOwnerKind, AcquisitionScope, TrustedAcquisitionIssuer } from "../session-ingest/acquisition-assertion.ts";

export type AcquisitionKind = AcquisitionOperation;
export type OwnerKind = AcquisitionOwnerKind;
export type GrantScope = AcquisitionScope;
export type IssuerKey = TrustedAcquisitionIssuer;
export type IssuerRegistry = ReadonlyMap<string, TrustedAcquisitionIssuer>;
export type AuthorizeScope = (scope: AcquisitionScope, issuerKeyId: string) => boolean;
/** The SQL function parameter object: the A fields plus evidenceSha256 and requestSha256 (§3.1). */
export type AcquisitionSqlParams = Readonly<Record<string, string>>;

export const UNIT_SEPARATOR = "\u001f";
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

/** ingest_identifier: non-empty, <= 512 UTF-8 bytes, no control characters, no lone surrogates. */
export function isIdentifier(value: string): boolean {
  if (value.length === 0 || Buffer.byteLength(value, "utf8") > 512 || /[\u0000-\u001f\u007f]/.test(value)) return false;
  for (const ch of value) {
    const point = ch.codePointAt(0)!;
    if (point >= 0xd800 && point <= 0xdfff) return false;
  }
  return true;
}
export function isUuid(value: string): boolean {
  return value.length === 36 && UUID.test(value);
}
