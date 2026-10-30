export interface RedisAdminCredential {
  readonly bearer: string;
}

export interface RedisAdminResponse {
  status: number;
  body?: unknown;
}

export interface RedisAdminTransport {
  get(url: string, credential: RedisAdminCredential, signal?: AbortSignal): Promise<RedisAdminResponse>;
}

export interface RedisServerCaInput {
  projectId: string;
  region: string;
  instanceId: string;
}

export interface RedisServerCaResult {
  /** Public trust anchors only. Never contains a private key or AUTH material. */
  caPem: string;
  certificateCount: number;
}

type RedisAdminFetch = (input: string | URL | Request, init?: RequestInit) => Promise<Response>;

const PROJECT = /^[a-z][a-z0-9-]{4,28}[a-z0-9]$/;
const REGION = /^[a-z]+(?:-[a-z0-9]+)+[0-9]$/;
const INSTANCE = /^[a-z](?:[a-z0-9-]{0,61}[a-z0-9])?$/;
const REDIS_ORIGIN = "https://redis.googleapis.com";
const REDIS_GET_PATH = /^\/v1\/projects\/([a-z][a-z0-9-]{4,28}[a-z0-9])\/locations\/([a-z]+(?:-[a-z0-9]+)+[0-9])\/instances\/([a-z](?:[a-z0-9-]{0,61}[a-z0-9])?)$/;
const CERTIFICATE = /^-----BEGIN CERTIFICATE-----\r?\n[A-Za-z0-9+/=\r\n]+\r?\n-----END CERTIFICATE-----\r?\n?$/;

function refuse(message: string): never {
  throw new Error(`96bulls: ${message} Nothing was created or changed.`);
}

function urlFor(input: RedisServerCaInput): string {
  if (!PROJECT.test(input.projectId) || !REGION.test(input.region) || !INSTANCE.test(input.instanceId))
    refuse("Redis service CA lookup target is invalid.");
  return `${REDIS_ORIGIN}/v1/projects/${input.projectId}/locations/${input.region}/instances/${input.instanceId}`;
}

export function redisAdminFetchTransport(fetcher: RedisAdminFetch = globalThis.fetch): RedisAdminTransport {
  return {
    async get(target, credential, signal) {
      let url: URL;
      try {
        url = new URL(target);
      } catch {
        refuse("Redis Admin target is invalid.");
      }
      if (
        url.origin !== REDIS_ORIGIN ||
        url.username ||
        url.password ||
        url.search ||
        url.hash ||
        !REDIS_GET_PATH.test(url.pathname)
      ) refuse("Redis Admin target is not approved.");
      if (
        typeof credential?.bearer !== "string" ||
        credential.bearer.length < 16 ||
        credential.bearer.length > 8192 ||
        /\s/.test(credential.bearer)
      ) refuse("Redis Admin credential is missing.");
      const response = await fetcher(url, {
        method: "GET",
        headers: { accept: "application/json", authorization: `Bearer ${credential.bearer}` },
        signal,
      });
      const length = Number(response.headers.get("content-length") ?? "0");
      if (Number.isFinite(length) && length > 262_144)
        return { status: response.status };
      const text = await response.text();
      if (text.length > 262_144) return { status: response.status };
      let body: unknown;
      if (text) {
        try {
          body = JSON.parse(text);
        } catch {
          body = undefined;
        }
      }
      return { status: response.status, body };
    },
  };
}

function record(value: unknown): Record<string, unknown> | undefined {
  if (!value || typeof value !== "object" || Array.isArray(value)) return;
  return value as Record<string, unknown>;
}

/** Resolve and validate Memorystore's public service-CA bundle after the instance is READY. */
export async function fetchRedisServerCa(
  input: RedisServerCaInput,
  credential: RedisAdminCredential,
  transport: RedisAdminTransport = redisAdminFetchTransport(),
  timeoutMs = 10_000,
): Promise<RedisServerCaResult> {
  if (!Number.isInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 30_000)
    refuse("Redis service CA lookup timeout is invalid.");
  const controller = new AbortController();
  let timer: ReturnType<typeof setTimeout> | undefined;
  let response: RedisAdminResponse;
  try {
    response = await Promise.race([
      transport.get(urlFor(input), credential, controller.signal),
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => {
          controller.abort();
          reject(new Error("timeout"));
        }, timeoutMs);
      }),
    ]);
  } catch {
    refuse("Redis service CA lookup failed.");
  } finally {
    if (timer) clearTimeout(timer);
  }
  if (response.status !== 200) refuse("Redis service CA lookup failed.");
  const instance = record(response.body);
  if (
    !instance ||
    instance.state !== "READY" ||
    instance.transitEncryptionMode !== "SERVER_AUTHENTICATION" ||
    (instance.authEnabled !== false && instance.authEnabled !== undefined) ||
    (typeof instance.authString === "string" && instance.authString.length > 0)
  ) refuse("Redis instance is not ready with passwordless server-authenticated TLS.");
  if (!Array.isArray(instance.serverCaCerts) || instance.serverCaCerts.length < 1 || instance.serverCaCerts.length > 4)
    refuse("Redis service CA bundle is missing or malformed.");
  const certificates = instance.serverCaCerts.map((value) => record(value)?.cert);
  if (
    certificates.some((cert) =>
      typeof cert !== "string" ||
      cert.length > 32_768 ||
      cert.includes("PRIVATE KEY") ||
      !CERTIFICATE.test(cert),
    )
  ) refuse("Redis service CA bundle is missing or malformed.");
  const unique = [...new Set(certificates as string[])];
  if (unique.length !== certificates.length) refuse("Redis service CA bundle contains duplicates.");
  return { caPem: `${unique.map((cert) => cert.trim()).join("\n")}\n`, certificateCount: unique.length };
}
