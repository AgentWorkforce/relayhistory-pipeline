/**
 * The hosted runtime for the upload-admission Lambda (relay-upload-inbox-v1, decisions 0004 and 0005): what
 * `UploadAdmissionRuntimeFactory` builds once per cold start on AWS. Default off: the handler reaches this only
 * with HISTORY_UPLOAD_DIRECT=allowlist|on; with it off nothing here runs and no secret is read.
 *
 * Decision 0005: the Lambda ADMITS, with the S3 upload ledger (ledger-s3.ts) and zero SQL. There is no
 * Cloudflare queue hand-off and no Supabase call anywhere on this path:
 *
 *   promoteUpload (upload-admission.ts, unchanged): the §6 checks -> status `handed_off` -> the acknowledged
 *     admission-inbox object -> `enqueue`
 *   enqueue = admitInline: the unchanged admission consumer (admission-consumer.ts admitMessage) in this
 *     invocation: claim (S3 ledger) -> privacy-journal gate -> canonical write (writeBatchObjects, inline) ->
 *     commit (S3 ledger) -> outcome marker -> inbox tombstone. A retryable answer throws, so promoteUpload
 *     returns `retry` and SQS redelivers (the status object makes the redelivery resume, never fork).
 *
 *   store    S3 (the transcripts bucket) with the execution role's session credentials. PUTs send no SSE
 *            header, so the bucket default (the transcripts CMK) applies; the bucket policy refuses any other.
 *   deliver  inline canonical acceptance, the S3 ledger, no inlineMaxSessions (a Lambda has no Worker
 *            subrequest cap), no slots (reserved concurrency 4 and batch size 1 bound it), no replica.
 *   secrets  One Secrets Manager secret (UPLOAD_ADMISSION_SECRET_ID), read once per cold start with the
 *            execution role: `{"upload_witness_key": "<32 bytes, unpadded base64url>"}`. Never in env or state.
 *
 * Refuses to start (every record fails, then the DLQ) unless INGEST_LEDGER=s3, and with anything this path
 * cannot honour without SQL: buffered acceptance, Babysitter retention routing, a replica, upload consent
 * (its store is SQL) or a non-TypeScript scrub engine. And it never ADMITS until an operator has written
 * `<keyRoot>/.ledger/fence-authority.json` (ledger-s3.ts FenceAuthorityDoc: fence backfill complete, control
 * mirror on, re-arm sweeper live): until then every record is a retry. Errors carry codes only: never a
 * secret, key, body or tenant id.
 */
import { zstdDecompressSync } from "node:zlib";
import { createS3Store, type ObjectStore } from "../../packages/object-store/src/index.ts";
import { routedRawChunkSealerDeps } from "../../packages/pipeline/src/lib/scrub-engine/index.ts";
import { awsJsonCall, lambdaCredentials } from "../session-pipeline/aws-json.ts";
import { admitMessage } from "./admission-consumer.ts";
import type { AdmissionMessage } from "./async-admission.ts";
import { routingFlag } from "./babysitter-retention.ts";
import type { DeliverDeps } from "./deliver.ts";
import { INGEST_LEDGER_VAR, ingestLedgerMode, s3Ledger } from "./ledger-s3.ts";
import { uploadConsentEnforcement, UPLOAD_CONSENT_VAR } from "./upload-consent.ts";
import type { PromoteUploadDeps, UploadDirectConfig } from "./upload-admission.ts";

export const UPLOAD_ADMISSION_BUCKET_VAR = "UPLOAD_ADMISSION_BUCKET";
/** The secret's name (`96bulls/<stage>/upload-admission`, stacks/aws/upload-inbox.ts) or its full ARN. */
export const UPLOAD_ADMISSION_SECRET_VAR = "UPLOAD_ADMISSION_SECRET_ID";
/** The secret's exact keys. */
export const UPLOAD_ADMISSION_SECRET_KEYS = ["upload_witness_key"] as const;

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
}

/** The hosted settings, or a coded error naming no value. */
export function hostedUploadAdmissionConfig(env: Record<string, string | undefined>): HostedUploadAdmissionConfig {
  // Decision 0005: this consumer's only ledger is S3. sql (or unset) refuses to start, never falls back.
  let ledger;
  try { ledger = ingestLedgerMode(env[INGEST_LEDGER_VAR]); } catch { return no("ingest_ledger_invalid"); }
  if (ledger !== "s3") no("ingest_ledger_s3_required");
  const need = (name: string) => env[name] || no("config_required");
  const region = need("AWS_REGION");
  const bucket = need(UPLOAD_ADMISSION_BUCKET_VAR);
  const secretId = need(UPLOAD_ADMISSION_SECRET_VAR);
  const keyRoot = env.INGEST_KEY_ROOT ?? "96bulls";
  if (!/^[a-z0-9-]{1,64}$/.test(region) || !/^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/.test(bucket) || !/^[a-z0-9][a-z0-9-]{0,62}$/.test(keyRoot)) no("target_invalid");
  if (!/^96bulls\/[a-z0-9-]{1,64}\/upload-admission$/.test(secretId) && !new RegExp(`^arn:aws:secretsmanager:${region}:\\d{12}:secret:96bulls/[a-z0-9-]{1,64}/upload-admission-[A-Za-z0-9]{6}$`).test(secretId)) no("target_invalid");
  // The hosted Worker renders no SCRUB_ENGINE (the TypeScript scrubber); this path seals the same way.
  if (env.SCRUB_ENGINE !== undefined && env.SCRUB_ENGINE !== "" && env.SCRUB_ENGINE !== "ts") no("scrub_engine_unsupported");
  // Inline canonical acceptance only: the S3 ledger refuses buffered acceptance and the staged fan-out.
  if ((env.INGEST_ACCEPTANCE_MODE ?? "canonical") !== "canonical") no("acceptance_mode_invalid");
  if (env.INGEST_INLINE_MAX_SESSIONS !== undefined && env.INGEST_INLINE_MAX_SESSIONS !== "") no("inline_max_sessions_unsupported");
  if ((env.INGEST_REPLICA_MODE ?? "off") !== "off") no("replica_unsupported");
  // Babysitter retention needs its SQL registration; the S3 ledger refuses it.
  if (routingFlag(env.INGEST_BABYSITTER_RETENTION_ROUTING)) no("babysitter_retention_unsupported");
  // Upload consent's policy store is SQL (upload-consent-sql.ts): never silently skip an enforced check here.
  let consent;
  try { consent = uploadConsentEnforcement(env[UPLOAD_CONSENT_VAR]); } catch { return no("upload_consent_invalid"); }
  if (consent !== "off") no("upload_consent_unsupported");
  return { region, bucket, keyRoot, secretId };
}

/** The secret's exact shape. The witness key is passed through raw: promoteUpload validates it (malformed = retry). */
export function parseUploadAdmissionSecret(text: string): { witnessKey: string } {
  let v: unknown;
  try { v = JSON.parse(text); } catch { return no("secret_invalid"); }
  if (!v || typeof v !== "object" || Array.isArray(v)) no("secret_invalid");
  const o = v as Record<string, unknown>;
  if (Object.keys(o).sort().join(",") !== UPLOAD_ADMISSION_SECRET_KEYS.join(",")) no("secret_invalid");
  if (typeof o.upload_witness_key !== "string") no("secret_invalid");
  return { witnessKey: o.upload_witness_key as string };
}

const SAFE_CODE = /^[A-Za-z0-9_]{1,64}$/;

/**
 * The admission, in this invocation: the unchanged consumer over `deliver` (the S3 ledger). `ack` (admitted,
 * refused with its outcome recorded, or already terminal) resolves; `retry` throws a coded error, so
 * promoteUpload answers `retry` and enqueueAdmission retires this attempt's inbox content (a redelivery writes
 * a new attempt; the batch record makes it a resume or a replay).
 */
export function admitInline(deliver: DeliverDeps, opts: { nowMs?: () => number; log?: (fields: Record<string, unknown>) => void } = {}) {
  const decompress = (bytes: Uint8Array) => new Uint8Array(zstdDecompressSync(bytes));
  return async (message: AdmissionMessage): Promise<void> => {
    const r = await admitMessage({ mode: "on", deliver, custodySource: "relayhistory", decompress, ...(opts.nowMs ? { nowMs: opts.nowMs } : {}), ...(opts.log ? { log: opts.log } : {}) }, message, 1);
    if (r.action === "ack") return;
    throw new UploadAdmissionRuntimeError(SAFE_CODE.test(`admission_${r.code}`) ? `admission_${r.code}` : "admission_retry");
  };
}

export interface HostedRuntimeOverrides {
  fetcher?: typeof fetch;
  secret?: (config: HostedUploadAdmissionConfig) => Promise<string>;
  store?: (config: Parameters<typeof createS3Store>[0]) => ObjectStore;
  nowMs?: () => number;
}

/** The hosted UploadAdmissionRuntimeFactory (upload-admission-lambda.ts). */
export function hostedUploadAdmissionRuntime(overrides: HostedRuntimeOverrides = {}) {
  return async (env: NodeJS.ProcessEnv, _config: UploadDirectConfig): Promise<Omit<PromoteUploadDeps, "config" | "witnessKey"> & { witnessKey: string; deliver: DeliverDeps }> => {
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
    const deliver: DeliverDeps = {
      // Fail closed: no admission until an operator has written the fence-authority marker (ledger-s3.ts
      // FenceAuthorityDoc: fence backfill complete, control mirror on, re-arm sweeper live). Every record retries.
      ledger: s3Ledger(store, config.keyRoot, { acceptanceMode: "canonical", fenceAuthority: "required" }),
      acceptanceMode: "canonical",
      // SCRUB_ENGINE is refused above unless ts, so this is {} today; it stays wired if that ever widens.
      target: { store, keyRoot: config.keyRoot, ...routedRawChunkSealerDeps() },
    };
    return { deliver, enqueue: admitInline(deliver, overrides.nowMs ? { nowMs: overrides.nowMs } : {}), witnessKey: secret.witnessKey, ...(overrides.nowMs ? { nowMs: overrides.nowMs } : {}) };
  };
}
