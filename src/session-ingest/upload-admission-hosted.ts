/**
 * The hosted runtime for the upload-admission Lambda (relay-upload-inbox-v1, decision 0004): what
 * `UploadAdmissionRuntimeFactory` builds once per cold start on AWS. Default off: the handler reaches this only
 * with HISTORY_UPLOAD_DIRECT=allowlist|on; with it off nothing here runs and no secret is read.
 *
 *   store    S3 (the transcripts bucket) with the execution role's session credentials. PUTs send no SSE
 *            header, so the bucket default (the transcripts CMK) applies, as for the Worker's default path;
 *            the bucket policy refuses any other encryption.
 *   deliver  Worker parity for prepareForAdmission: buffered acceptance, inline 0, no replica, and the
 *            Worker's own Babysitter-routing budget variables (INGEST_BABYSITTER_RETENTION_ROUTING,
 *            INGEST_SUBREQUEST_CAP, INGEST_SUBREQUEST_REDIRECTS, INGEST_EXPECT_GENERATION). The hosted
 *            Worker profile sets no routing, so the budget check is the same no-op on both paths.
 *   enqueue  The hosted admission queue is the Cloudflare queue `96bulls-session-ingest-admission`, consumed by
 *            the ingest Worker (admission-consumer.ts). This Lambda has no Worker binding, so it sends through
 *            the Cloudflare Queues REST API (`POST /accounts/{account}/queues/{queue}/messages`, content type
 *            json: the same body the Worker's `INGEST_ADMISSION.send` produces).
 *   secrets  One Secrets Manager secret (UPLOAD_ADMISSION_SECRET_ID), read once per cold start with the
 *            execution role: `{"upload_witness_key": "<32 bytes, unpadded base64url>",
 *            "cloudflare_queue_token": "<API token with Queues write on the account>"}`. Never in env or state.
 *
 * Errors carry codes only: never a secret, key, body or tenant id.
 */
import { createS3Store, type ObjectStore } from "../../packages/object-store/src/index.ts";
import { awsJsonCall, lambdaCredentials } from "../session-pipeline/aws-json.ts";
import type { AdmissionMessage } from "./async-admission.ts";
import { routingFlag } from "./babysitter-retention.ts";
import type { PromoteUploadDeps, UploadDirectConfig } from "./upload-admission.ts";

export const UPLOAD_ADMISSION_BUCKET_VAR = "UPLOAD_ADMISSION_BUCKET";
/** The secret's name (`96bulls/<stage>/upload-admission`, stacks/aws/upload-inbox.ts) or its full ARN. */
export const UPLOAD_ADMISSION_SECRET_VAR = "UPLOAD_ADMISSION_SECRET_ID";
export const UPLOAD_ADMISSION_CF_ACCOUNT_VAR = "UPLOAD_ADMISSION_CF_ACCOUNT_ID";
export const UPLOAD_ADMISSION_CF_QUEUE_VAR = "UPLOAD_ADMISSION_CF_QUEUE_ID";
/** The secret's exact keys. */
export const UPLOAD_ADMISSION_SECRET_KEYS = ["cloudflare_queue_token", "upload_witness_key"] as const;

export class UploadAdmissionRuntimeError extends Error {
  constructor(readonly code: string) {
    super(`upload admission runtime: ${code}`);
    this.name = "UploadAdmissionRuntimeError";
  }
}
const no = (code: string): never => { throw new UploadAdmissionRuntimeError(code); };

export interface HostedUploadAdmissionConfig {
  region: string;
  bucket: string;
  keyRoot: string;
  secretId: string;
  cloudflareAccountId: string;
  cloudflareQueueId: string;
  deliver: Omit<PromoteUploadDeps["deliver"], "target">;
}

const HEX32 = /^[0-9a-f]{32}$/;
function boundedInt(raw: string, lo: number, hi: number): number {
  const n = Number(raw.trim());
  if (!Number.isSafeInteger(n) || n < lo || n > hi) no("worker_parity_invalid");
  return n;
}

/** The hosted settings, or a coded error naming no value. */
export function hostedUploadAdmissionConfig(env: Record<string, string | undefined>): HostedUploadAdmissionConfig {
  const need = (name: string) => env[name] || no("config_required");
  const region = need("AWS_REGION");
  const bucket = need(UPLOAD_ADMISSION_BUCKET_VAR);
  const secretId = need(UPLOAD_ADMISSION_SECRET_VAR);
  const cloudflareAccountId = need(UPLOAD_ADMISSION_CF_ACCOUNT_VAR);
  const cloudflareQueueId = need(UPLOAD_ADMISSION_CF_QUEUE_VAR);
  const keyRoot = env.INGEST_KEY_ROOT ?? "96bulls";
  if (!/^[a-z0-9-]{1,64}$/.test(region) || !/^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/.test(bucket) || !/^[a-z0-9][a-z0-9-]{0,62}$/.test(keyRoot)) no("target_invalid");
  if (!/^96bulls\/[a-z0-9-]{1,64}\/upload-admission$/.test(secretId) && !new RegExp(`^arn:aws:secretsmanager:${region}:\\d{12}:secret:96bulls/[a-z0-9-]{1,64}/upload-admission-[A-Za-z0-9]{6}$`).test(secretId)) no("target_invalid");
  if (!HEX32.test(cloudflareAccountId) || !HEX32.test(cloudflareQueueId)) no("queue_target_invalid");
  // The hosted Worker renders no SCRUB_ENGINE (the TypeScript scrubber); the upload path scrubs the same way.
  if (env.SCRUB_ENGINE !== undefined && env.SCRUB_ENGINE !== "" && env.SCRUB_ENGINE !== "ts") no("scrub_engine_unsupported");
  // The hosted Worker runs buffered acceptance only (inline 0, replica off); the upload path matches it.
  if ((env.INGEST_ACCEPTANCE_MODE ?? "buffered") !== "buffered") no("acceptance_mode_invalid");
  const generation = env.INGEST_EXPECT_GENERATION;
  if (generation !== undefined && generation !== "" && !/^[0-9a-f]{64}$/.test(generation)) no("worker_parity_invalid");
  const deliver: HostedUploadAdmissionConfig["deliver"] = {
    acceptanceMode: "buffered",
    inlineMaxSessions: 0,
    ...(routingFlag(env.INGEST_BABYSITTER_RETENTION_ROUTING) ? { babysitterRetention: true } : {}),
    ...(env.INGEST_SUBREQUEST_CAP ? { subrequestCap: boundedInt(env.INGEST_SUBREQUEST_CAP, 1, 10_000_000) } : {}),
    ...(env.INGEST_SUBREQUEST_REDIRECTS ? { subrequestRedirects: boundedInt(env.INGEST_SUBREQUEST_REDIRECTS, 0, 20) } : {}),
    ...(generation ? { retentionGeneration: generation } : {}),
  };
  return { region, bucket, keyRoot, secretId, cloudflareAccountId, cloudflareQueueId, deliver };
}

/** The secret's exact shape. The witness key is passed through raw: promoteUpload validates it (malformed = retry). */
export function parseUploadAdmissionSecret(text: string): { witnessKey: string; queueToken: string } {
  let v: unknown;
  try { v = JSON.parse(text); } catch { return no("secret_invalid"); }
  if (!v || typeof v !== "object" || Array.isArray(v)) no("secret_invalid");
  const o = v as Record<string, unknown>;
  if (Object.keys(o).sort().join(",") !== UPLOAD_ADMISSION_SECRET_KEYS.join(",")) no("secret_invalid");
  const { upload_witness_key: witnessKey, cloudflare_queue_token: queueToken } = o;
  if (typeof witnessKey !== "string" || typeof queueToken !== "string" || !/^[A-Za-z0-9_-]{20,256}$/.test(queueToken)) no("secret_invalid");
  return { witnessKey: witnessKey as string, queueToken: queueToken as string };
}

/**
 * The Cloudflare Queues REST send. One message per call (the Lambda's batch size is 1). A non-2xx or
 * `success: false` throws a coded error, so promoteUpload answers `retry` and enqueueAdmission tombstones the
 * unacknowledged attempt, exactly as for a failed Worker binding send.
 */
export function cloudflareQueueSender(i: { accountId: string; queueId: string; token: string; fetcher?: typeof fetch; timeoutMs?: number }) {
  if (!HEX32.test(i.accountId) || !HEX32.test(i.queueId)) no("queue_target_invalid");
  const url = `https://api.cloudflare.com/client/v4/accounts/${i.accountId}/queues/${i.queueId}/messages`;
  return async (message: AdmissionMessage): Promise<void> => {
    let response: Response;
    try {
      response = await (i.fetcher ?? fetch)(url, {
        method: "POST",
        headers: { authorization: `Bearer ${i.token}`, "content-type": "application/json" },
        body: JSON.stringify({ body: message, content_type: "json" }),
        signal: AbortSignal.timeout(i.timeoutMs ?? 10_000),
        redirect: "error",
      });
    } catch {
      return no("admission_queue_unreachable");
    }
    let ok = false;
    try { ok = response.ok && (await response.json() as { success?: unknown })?.success === true; } catch { ok = false; }
    if (!ok) no(`admission_queue_http_${response.status}`);
  };
}

export interface HostedRuntimeOverrides {
  fetcher?: typeof fetch;
  secret?: (config: HostedUploadAdmissionConfig) => Promise<string>;
  store?: (config: Parameters<typeof createS3Store>[0]) => ObjectStore;
}

/** The hosted UploadAdmissionRuntimeFactory (upload-admission-lambda.ts). */
export function hostedUploadAdmissionRuntime(overrides: HostedRuntimeOverrides = {}) {
  return async (env: NodeJS.ProcessEnv, _config: UploadDirectConfig): Promise<Omit<PromoteUploadDeps, "config" | "witnessKey"> & { witnessKey: string }> => {
    const config = hostedUploadAdmissionConfig(env);
    const fetcher = overrides.fetcher ?? fetch;
    const credentials = () => lambdaCredentials(env);
    const secretText = overrides.secret
      ? await overrides.secret(config)
      : await (async () => {
          const r = await awsJsonCall<{ SecretString?: string }>({
            service: "secretsmanager", target: "secretsmanager.GetSecretValue", region: config.region,
            body: { SecretId: config.secretId }, credentials: credentials(), fetcher, timeoutMs: 5_000,
          });
          return r.SecretString ?? no("secret_empty");
        })();
    const secret = parseUploadAdmissionSecret(secretText);
    const store = (overrides.store ?? createS3Store)({ bucket: config.bucket, region: config.region, credentials: async () => credentials(), fetch: fetcher });
    return {
      deliver: { ...config.deliver, target: { store, keyRoot: config.keyRoot } },
      enqueue: cloudflareQueueSender({ accountId: config.cloudflareAccountId, queueId: config.cloudflareQueueId, token: secret.queueToken, fetcher }),
      witnessKey: secret.witnessKey,
    };
  };
}
