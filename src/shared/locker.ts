/**
 * The Locker (S3) client every service uses. Credentials:
 *   - static keys when LOCKER_ACCESS_KEY_ID/LOCKER_SECRET_ACCESS_KEY are set (local MinIO);
 *   - otherwise EKS Pod Identity: the agent injects AWS_CONTAINER_CREDENTIALS_FULL_URI and
 *     AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE; credentials are short-lived and refreshed
 *     before expiry by the S3 driver's cached provider. No key is stored anywhere.
 * LOCKER_ENDPOINT is optional: unset means AWS S3 in LOCKER_REGION.
 */
import { readFile } from "node:fs/promises";
import { createS3Store, objectStoreManifestReader, type ManifestReader, type RawStore } from "./vendor.ts";
import type { AwsCredentials } from "../../packages/object-store/src/sigv4.ts";
import { optional, required } from "./env.ts";

export type CredentialSource = "static" | "pod-identity";

export function lockerSettings() {
  return {
    endpoint: process.env.LOCKER_ENDPOINT || undefined,
    bucket: required("LOCKER_BUCKET"),
    region: optional("LOCKER_REGION", "us-east-1"),
  };
}

export function credentialSource(env: NodeJS.ProcessEnv = process.env): CredentialSource {
  if (env.LOCKER_ACCESS_KEY_ID && env.LOCKER_SECRET_ACCESS_KEY) return "static";
  if (env.AWS_CONTAINER_CREDENTIALS_FULL_URI) return "pod-identity";
  throw new Error("96bulls: no Locker credentials: set LOCKER_ACCESS_KEY_ID/LOCKER_SECRET_ACCESS_KEY, or run with EKS Pod Identity");
}

interface ContainerCredentials {
  AccessKeyId: string;
  SecretAccessKey: string;
  Token?: string;
  Expiration?: string;
}

/** EKS Pod Identity / container credentials endpoint (the same contract the AWS SDKs use). */
export async function podIdentityCredentials(env: NodeJS.ProcessEnv = process.env, fetcher: typeof fetch = fetch): Promise<AwsCredentials> {
  const uri = env.AWS_CONTAINER_CREDENTIALS_FULL_URI;
  if (!uri) throw new Error("96bulls: AWS_CONTAINER_CREDENTIALS_FULL_URI is not set");
  const tokenFile = env.AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE;
  const token = tokenFile ? (await readFile(tokenFile, "utf8")).trim() : env.AWS_CONTAINER_AUTHORIZATION_TOKEN;
  const res = await fetcher(uri, { headers: token ? { authorization: token } : {} });
  if (!res.ok) throw Object.assign(new Error(`96bulls: pod identity credentials: http ${res.status}`), { code: "credentials_unavailable", retryable: true });
  const c = (await res.json()) as ContainerCredentials;
  return {
    accessKeyId: c.AccessKeyId,
    secretAccessKey: c.SecretAccessKey,
    ...(c.Token ? { sessionToken: c.Token } : {}),
    ...(c.Expiration ? { expiresAt: new Date(c.Expiration) } : {}),
  };
}

export function lockerCredentials(env: NodeJS.ProcessEnv = process.env): AwsCredentials | (() => Promise<AwsCredentials>) {
  return credentialSource(env) === "static"
    ? { accessKeyId: env.LOCKER_ACCESS_KEY_ID!, secretAccessKey: env.LOCKER_SECRET_ACCESS_KEY! }
    : () => podIdentityCredentials(env);
}

let cached: RawStore | undefined;

/** The one Locker store for this process. */
export function lockerStore(): RawStore {
  if (cached) return cached;
  const s = lockerSettings();
  cached = {
    provider: "s3",
    bucket: s.bucket,
    store: createS3Store({
      bucket: s.bucket,
      region: s.region,
      ...(s.endpoint ? { endpoint: s.endpoint } : {}),
      credentials: lockerCredentials(),
    }),
  };
  return cached;
}

export function lockerManifestReader(): ManifestReader {
  const raw = lockerStore();
  return objectStoreManifestReader(raw.store, { provider: raw.provider, bucket: raw.bucket });
}
