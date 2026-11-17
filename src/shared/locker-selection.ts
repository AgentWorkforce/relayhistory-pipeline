/**
 * Provider selection for the Locker. Unset means S3, which is local MinIO,
 * production AWS S3, and the EKS self-host proof. GCS is selected only by
 * LOCKER_PROVIDER=gcs and uses Workload Identity, never HMAC keys.
 *
 * rawStoreFromEnv follows the same choice: unset and s3 stay S3, and gcs builds
 * the GCS adapter without HMAC keys.
 *
 * Self-host only, never selected unless named (relay#2204 S3):
 *   LOCKER_PROVIDER=fs     a local directory, LOCKER_FS_ROOT (absolute path). The fs driver serializes
 *                          writes within one process only, so it is for a single-user install whose one
 *                          session-ingest replica is the only writer. Refused unless this process is a
 *                          self-host service (RELAYHISTORY_SELFHOST_INGEST, _READ_API or
 *                          _PIPELINE_CONSUMER exactly "true") and refused on Lambda
 *                          (AWS_LAMBDA_FUNCTION_NAME set).
 *   LOCKER_PROVIDER=azure  Azure Blob, Shared Key: LOCKER_AZURE_ACCOUNT, LOCKER_AZURE_ACCOUNT_KEY (secret
 *                          store), container LOCKER_BUCKET, optional LOCKER_ENDPOINT (e.g. Azurite).
 * Neither takes LOCKER_ACCESS_KEY_ID/LOCKER_SECRET_ACCESS_KEY. Errors never echo a setting's value.
 */
import { isAbsolute, normalize } from "node:path";

export type LockerProvider = "s3" | "gcs" | "fs" | "azure";

/** A process that sets any of these to exactly "true" is a self-host service (docs/selfhost-composition.md). */
export const FS_LOCKER_SELFHOST_FLAGS = [
  "RELAYHISTORY_SELFHOST_INGEST",
  "RELAYHISTORY_SELFHOST_READ_API",
  "RELAYHISTORY_SELFHOST_PIPELINE_CONSUMER",
] as const;

const DEFAULT_GCS_TOKEN_URI =
  "http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/token";

const LOOPBACK_HOSTS = new Set(["127.0.0.1", "localhost", "[::1]"]);
const TRUSTED_GCS_TOKEN_HOSTS = new Set(["169.254.169.254", ...LOOPBACK_HOSTS]);

export function lockerProvider(env: NodeJS.ProcessEnv = process.env): LockerProvider {
  const raw = env.LOCKER_PROVIDER?.trim() || "s3";
  if (raw !== "s3" && raw !== "gcs" && raw !== "fs" && raw !== "azure") {
    throw new Error("96bulls: LOCKER_PROVIDER must be s3 or gcs (or fs or azure on self-host)");
  }
  return raw;
}

export function gcsTokenUri(env: NodeJS.ProcessEnv = process.env): string {
  return env.LOCKER_GCS_TOKEN_URI?.trim() || DEFAULT_GCS_TOKEN_URI;
}

/** Metadata server and loopback only. The token request must not leave the node. */
export function trustedGcsTokenUri(uri: string): boolean {
  try {
    const u = new URL(uri);
    return (
      u.protocol === "http:" &&
      !u.username &&
      !u.password &&
      TRUSTED_GCS_TOKEN_HOSTS.has(u.hostname)
    );
  } catch {
    return false;
  }
}

/** Never send a metadata-derived bearer token to an operator-chosen remote host. */
export function trustedGcsEndpoint(uri: string): boolean {
  try {
    const u = new URL(uri);
    if (u.username || u.password || u.search || u.hash) return false;
    if (LOOPBACK_HOSTS.has(u.hostname))
      return u.protocol === "http:" || u.protocol === "https:";
    return (
      u.protocol === "https:" &&
      u.hostname === "storage.googleapis.com" &&
      !u.port &&
      (u.pathname === "" || u.pathname === "/")
    );
  } catch {
    return false;
  }
}

function endpointHost(endpoint: string | undefined): string | undefined {
  if (!endpoint) return undefined;
  try {
    return new URL(endpoint).hostname.toLowerCase();
  } catch {
    return undefined;
  }
}

function isGcsApiHost(host: string): boolean {
  return host === "storage.googleapis.com" || host.endsWith(".storage.googleapis.com") || host === "storage.google.com";
}

/** Refuse a selection that would disguise GCS as S3 or put HMAC keys on GCS. */
export function assertLockerSelection(env: NodeJS.ProcessEnv = process.env): void {
  const provider = lockerProvider(env);
  const host = endpointHost(env.LOCKER_ENDPOINT);
  if (provider === "s3" && host && isGcsApiHost(host)) {
    throw new Error(
      "96bulls: refusing to reach GCS with the S3 client. Set LOCKER_PROVIDER=gcs. HMAC-as-S3 is not the adapter.",
    );
  }
  if (provider === "gcs" && (env.LOCKER_ACCESS_KEY_ID || env.LOCKER_SECRET_ACCESS_KEY)) {
    throw new Error(
      "96bulls: GCS Locker uses Workload Identity, not HMAC keys. Unset LOCKER_ACCESS_KEY_ID and LOCKER_SECRET_ACCESS_KEY.",
    );
  }
  if (provider === "gcs" && !env.LOCKER_BUCKET?.trim()) {
    throw new Error("96bulls: LOCKER_PROVIDER=gcs requires LOCKER_BUCKET");
  }
  if (provider === "gcs" && env.LOCKER_ENDPOINT) {
    if (!trustedGcsEndpoint(env.LOCKER_ENDPOINT))
      throw new Error(
        "96bulls: GCS LOCKER_ENDPOINT must be the official HTTPS API or loopback emulator",
      );
  }
  if (provider === "gcs" && env.LOCKER_GCS_TOKEN_URI && !trustedGcsTokenUri(env.LOCKER_GCS_TOKEN_URI)) {
    throw new Error("96bulls: LOCKER_GCS_TOKEN_URI is not the GKE metadata server or loopback");
  }
  if (provider === "fs" || provider === "azure") assertSelfhostLocker(provider, env);
}

function assertSelfhostLocker(provider: "fs" | "azure", env: NodeJS.ProcessEnv): void {
  if (env.LOCKER_ACCESS_KEY_ID || env.LOCKER_SECRET_ACCESS_KEY) {
    throw new Error(
      `96bulls: LOCKER_PROVIDER=${provider} takes no S3 keys. Unset LOCKER_ACCESS_KEY_ID and LOCKER_SECRET_ACCESS_KEY.`,
    );
  }
  if (!env.LOCKER_BUCKET?.trim()) throw new Error(`96bulls: LOCKER_PROVIDER=${provider} requires LOCKER_BUCKET`);
  if (provider === "fs") {
    if (env.AWS_LAMBDA_FUNCTION_NAME) throw new Error("96bulls: LOCKER_PROVIDER=fs is refused on a hosted Lambda");
    if (!FS_LOCKER_SELFHOST_FLAGS.some((flag) => env[flag] === "true")) {
      throw new Error(`96bulls: LOCKER_PROVIDER=fs is self-host only: set one of ${FS_LOCKER_SELFHOST_FLAGS.join(", ")}=true`);
    }
    if (env.LOCKER_ENDPOINT) throw new Error("96bulls: LOCKER_PROVIDER=fs takes no LOCKER_ENDPOINT");
    const root = env.LOCKER_FS_ROOT?.trim();
    if (!root) throw new Error("96bulls: LOCKER_PROVIDER=fs requires LOCKER_FS_ROOT");
    // Absolute and already normalized (no "." or ".." segments, no doubled separators), and never the filesystem root.
    if (!isAbsolute(root) || normalize(root) !== root || root.split(/[\\/]/).includes("..") || root === "/") {
      throw new Error("96bulls: LOCKER_FS_ROOT must be an absolute path (normalized, not /)");
    }
    return;
  }
  if (!/^[a-z0-9]{3,24}$/.test(env.LOCKER_AZURE_ACCOUNT?.trim() ?? "")) {
    throw new Error("96bulls: LOCKER_PROVIDER=azure requires LOCKER_AZURE_ACCOUNT (3-24 lowercase letters or digits)");
  }
  // Azure container names: 3-63 lowercase letters, digits and single hyphens, starting and ending alphanumeric.
  if (!/^(?=.{3,63}$)[a-z0-9]+(?:-[a-z0-9]+)*$/.test(env.LOCKER_BUCKET ?? "")) {
    throw new Error("96bulls: LOCKER_PROVIDER=azure requires LOCKER_BUCKET to be an Azure container name");
  }
  // Base64, padded to a multiple of 4.
  const accountKey = env.LOCKER_AZURE_ACCOUNT_KEY?.trim() ?? "";
  if (!accountKey || !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(accountKey)) {
    throw new Error("96bulls: LOCKER_PROVIDER=azure requires LOCKER_AZURE_ACCOUNT_KEY (base64)");
  }
  if (env.LOCKER_ENDPOINT && !trustedAzureEndpoint(env.LOCKER_ENDPOINT)) {
    throw new Error("96bulls: azure LOCKER_ENDPOINT must be an https URL (http only to loopback) with no credentials, query or fragment");
  }
}

/** https anywhere, or http only to loopback (127.0.0.0/8, ::1, localhost); no userinfo ("@"), query or fragment. */
function trustedAzureEndpoint(raw: string): boolean {
  if (/[?#@]/.test(raw)) return false;
  let u: URL;
  try { u = new URL(raw); } catch { return false; }
  if (u.username || u.password || u.search || u.hash) return false;
  if (u.protocol === "https:") return true;
  if (u.protocol !== "http:") return false;
  const host = u.hostname.toLowerCase();
  return host === "localhost" || host === "[::1]" || /^127\.\d{1,3}\.\d{1,3}\.\d{1,3}$/.test(host);
}
