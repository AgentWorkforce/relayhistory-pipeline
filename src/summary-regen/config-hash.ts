/**
 * The summariser configuration hash: sha256 over the model id, the base URL, the reasoning effort,
 * the prompt version and a key fingerprint. The fingerprint is HMAC-SHA256(key, fixed label)
 * truncated to 16 hex, or "nokey" when there is no usable key (unset or `-disabled`). The key itself
 * is never part of the hashed input, never returned and never logged.
 */
import { createHash, createHmac } from "node:crypto";

export const KEY_FINGERPRINT_LABEL = "96bulls/summary-config-key-fingerprint/v1";

export function keyFingerprint(key: string | null | undefined): string {
  if (!key) return "nokey";
  return createHmac("sha256", key).update(KEY_FINGERPRINT_LABEL).digest("hex").slice(0, 16);
}

export interface ConfigHashInput {
  model: string;
  baseUrl: string;
  /** undefined: the reasoning field is omitted from requests. */
  reasoningEffort: string | undefined;
  promptVersion: string;
  /** Only its fingerprint enters the hash. */
  key: string | null | undefined;
}

/** The exact array that is hashed (exported for the "raw key absent" test). */
export function configHashMaterial(c: ConfigHashInput): string {
  return JSON.stringify(["summary-config/v1", c.model, c.baseUrl, c.reasoningEffort ?? "omit", c.promptVersion, keyFingerprint(c.key)]);
}

export function configHash(c: ConfigHashInput): string {
  return createHash("sha256").update(configHashMaterial(c)).digest("hex");
}
