/**
 * Short-lived AWS credentials for the neon-s3-sync exporter and neon-s3-erasure.
 *
 * The credential source is a broker that mints STS AssumeRole credentials (15-60 min) and writes each mint as a new
 * mode-0600 file in the AWS `credential_process` v1 shape:
 *   {"Version": 1, "AccessKeyId": "...", "SecretAccessKey": "...", "SessionToken": "...", "Expiration": "<UTC ISO 8601>"}
 * The operator's supervisor renews before expiry and switches the reference the process reads (for the exporter,
 * S3X_CREDS_FILE, typically a symlink it repoints). Nothing here mints, stores or logs a credential. Errors carry
 * closed codes only, never a value.
 *
 * The process behaviour is the same for both commands. It refuses to start with less than the minimum lifetime
 * left. While running, it stops taking new work once the lifetime still left falls under that minimum (in-flight
 * work finishes and the committed watermark is kept), then exits with EXIT_CREDENTIALS_RENEW, so a supervisor
 * renews and restarts it.
 */
import { readFileSync } from "node:fs";
import type { AwsCredentials } from "../../packages/object-store/src/sigv4.ts";

/** Exit status when the process stopped (or refused to start) because its credential is about to expire: EX_TEMPFAIL. */
export const EXIT_CREDENTIALS_RENEW = 75;
/** Default minimum lifetime left to start, and to keep taking new work: 10 minutes. */
export const DEFAULT_MIN_REMAINING_MS = 600_000;

/** A refusal with a closed code (`short_lived_<reason>[.<name>]`); never a credential value. */
export class ShortLivedCredentialError extends Error {
  readonly code: string;
  constructor(code: string) { super(code); this.name = "ShortLivedCredentialError"; this.code = code; }
}

const ISO_UTC = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?(?:Z|[+-]00:00)$/;
const nonEmpty = (v: unknown): v is string => typeof v === "string" && v.length > 0;

/** Parses a UTC ISO 8601 expiration; throws `short_lived_expiration_invalid.<name>` otherwise. */
export function parseExpiration(v: unknown, name: string): Date {
  if (typeof v !== "string" || !ISO_UTC.test(v)) throw new ShortLivedCredentialError(`short_lived_expiration_invalid.${name}`);
  const d = new Date(v);
  if (Number.isNaN(d.getTime())) throw new ShortLivedCredentialError(`short_lived_expiration_invalid.${name}`);
  return d;
}

/**
 * A credential document, in either shape:
 *  - `credential_process` v1 (the broker's shape): Version 1, AccessKeyId, SecretAccessKey, SessionToken and
 *    Expiration, all required;
 *  - the existing static shape: accessKeyId, secretAccessKey, optional sessionToken, optional expiration.
 * Any other key is refused, so a mistyped field never passes silently. Returns the keys with `expiresAt` when known.
 */
export function parseCredentialDocument(v: unknown, name: string): AwsCredentials {
  const c = v as Record<string, unknown> | null;
  if (!c || typeof c !== "object" || Array.isArray(c)) throw new ShortLivedCredentialError(`short_lived_shape_invalid.${name}`);
  if ("Version" in c) {
    const allowed = ["Version", "AccessKeyId", "SecretAccessKey", "SessionToken", "Expiration"];
    for (const k of Object.keys(c)) if (!allowed.includes(k)) throw new ShortLivedCredentialError(`short_lived_shape_invalid.${name}`);
    if (c.Version !== 1) throw new ShortLivedCredentialError(`short_lived_version_unsupported.${name}`);
    if (!nonEmpty(c.AccessKeyId) || !nonEmpty(c.SecretAccessKey) || !nonEmpty(c.SessionToken))
      throw new ShortLivedCredentialError(`short_lived_shape_invalid.${name}`);
    return { accessKeyId: c.AccessKeyId, secretAccessKey: c.SecretAccessKey, sessionToken: c.SessionToken, expiresAt: parseExpiration(c.Expiration, name) };
  }
  const allowed = ["accessKeyId", "secretAccessKey", "sessionToken", "expiration"];
  for (const k of Object.keys(c)) if (!allowed.includes(k)) throw new ShortLivedCredentialError(`short_lived_shape_invalid.${name}`);
  if (!nonEmpty(c.accessKeyId) || !nonEmpty(c.secretAccessKey)) throw new ShortLivedCredentialError(`short_lived_shape_invalid.${name}`);
  if (c.sessionToken !== undefined && !nonEmpty(c.sessionToken)) throw new ShortLivedCredentialError(`short_lived_shape_invalid.${name}`);
  return {
    accessKeyId: c.accessKeyId,
    secretAccessKey: c.secretAccessKey,
    ...(c.sessionToken ? { sessionToken: c.sessionToken as string } : {}),
    ...(c.expiration !== undefined ? { expiresAt: parseExpiration(c.expiration, name) } : {}),
  };
}

/** The minimum lifetime left, from `<VAR>` in seconds (bounded 120..3000), else the default. */
export function minRemainingMs(raw: string | undefined): number {
  if (raw === undefined || raw === "") return DEFAULT_MIN_REMAINING_MS;
  if (!/^\d{1,4}$/.test(raw)) throw new ShortLivedCredentialError("short_lived_min_remaining_invalid");
  const s = Number(raw);
  if (s < 120 || s > 3000) throw new ShortLivedCredentialError("short_lived_min_remaining_invalid");
  return s * 1000;
}

/** True when a credential with this expiry has less than `minMs` left at `now` (never true without an expiry). */
export function expiringSoon(c: Pick<AwsCredentials, "expiresAt">, minMs: number, now: number): boolean {
  return c.expiresAt !== undefined && c.expiresAt.getTime() - now < minMs;
}

export interface FileCredentialSource {
  /** The object-store provider: re-reads the file on every call, so a renewed (or repointed) file takes effect. */
  provider: () => Promise<AwsCredentials>;
  /** The current file's credential, re-read at most every `cacheMs` (for the stop check). */
  current(): AwsCredentials;
  /** True once the current file's credential has less than the minimum lifetime left. */
  expiring(): boolean;
}

/**
 * A credential file the supervisor renews. Every provider call re-reads `path` (following a symlink), so
 * the store picks up a new mint when it refreshes (it refreshes 5 minutes before `expiresAt`). `expiring()`
 * reads at most every `cacheMs`.
 *
 * A credential WITHOUT an expiry (the static shape, as the existing runbook feeds it through a FIFO) is read
 * exactly once and then frozen: it never expires, and re-reading a FIFO would block.
 */
export function fileCredentialSource(
  path: string,
  opts: { minMs: number; now?: () => number; read?: (p: string) => string; cacheMs?: number; name?: string },
): FileCredentialSource {
  const now = opts.now ?? Date.now;
  const read = opts.read ?? ((p: string) => readFileSync(p, "utf8"));
  const name = opts.name ?? "S3X_CREDS_FILE";
  const cacheMs = opts.cacheMs ?? 30_000;
  const load = (): AwsCredentials => {
    let text: string;
    try { text = read(path); } catch { throw new ShortLivedCredentialError(`short_lived_file_unreadable.${name}`); }
    let doc: unknown;
    try { doc = JSON.parse(text); } catch { throw new ShortLivedCredentialError(`short_lived_shape_invalid.${name}`); }
    return parseCredentialDocument(doc, name);
  };
  let cached: { at: number; c: AwsCredentials } | undefined;
  let frozen: AwsCredentials | undefined;
  const fresh = (): AwsCredentials => {
    if (frozen) return frozen;
    const c = load();
    if (c.expiresAt === undefined) frozen = c;
    cached = { at: now(), c };
    return c;
  };
  const current = () => {
    if (frozen) return frozen;
    if (!cached || now() - cached.at >= cacheMs) return fresh();
    return cached.c;
  };
  return {
    provider: async () => fresh(),
    current,
    expiring: () => expiringSoon(current(), opts.minMs, now()),
  };
}
