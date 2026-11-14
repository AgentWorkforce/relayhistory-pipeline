/**
 * Upload-admission Lambda entry (relay-upload-inbox-v1, decisions 0004 and 0005): an SQS batch of S3
 * ObjectCreated notifications for `<keyRoot>/.upload-inbox/`, answered with `batchItemFailures`
 * (ReportBatchItemFailures). Default off: with HISTORY_UPLOAD_DIRECT off every record is reported as a
 * failure without reading storage, so a mis-armed queue only redrives into its DLQ.
 *
 * Per record: a malformed SQS body or S3 notification, a key that is not this key root's upload key, and
 * every `retry` from promoteUpload are failures (the queue re-drives them; maxReceiveCount routes them to the
 * DLQ). An `ack` (admitted or refused by the inline admission, refused with a recorded §6 code, already
 * promoted, gone) is a success. A retry's log label carries the admission's own code when it has one
 * (`admission_enqueue_failed:admission_<code>`). An
 * s3:TestEvent body acks. A record without a messageId is reported with an empty itemIdentifier, which
 * Lambda treats as a failure of the whole batch (never a silent ack). Logs carry codes and counts only, never a key, body, witness or tenant id.
 *
 * Like buffered-gc-lambda.ts, the handler never accepts factories or credentials from its invocation
 * payload. The hosted runtime (S3 store, the S3 upload ledger, the in-invocation admission, the witness key
 * from Secrets Manager; it refuses to build unless INGEST_LEDGER=s3) is built by
 * `UploadAdmissionRuntimeFactory`; the deployed `handler` uses the hosted
 * factory (upload-admission-hosted.ts), imported only once the flag is on. stacks/aws owns the function,
 * queue, mapping, notification and IAM. A factory that returns `witnessKey` overrides UPLOAD_WITNESS_KEY
 * from the environment (hosted never puts the key in env or state).
 */
import { declareScrubTsCapability } from "@rhc/lib/scrub-engine/ts-capability.ts";
import { promoteUpload, uploadDirectConfig, UPLOAD_WITNESS_KEY_VAR, type PromoteResult, type PromoteUploadDeps, type UploadDirectConfig } from "./upload-admission.ts";

export interface SqsRecordLike { messageId?: unknown; body?: unknown }
export interface UploadAdmissionBatchResponse { batchItemFailures: { itemIdentifier: string }[] }
/** Builds the promotion deps once per cold start (store, deliver budget deps, enqueue). */
export type UploadAdmissionRuntimeFactory = (env: NodeJS.ProcessEnv, config: UploadDirectConfig) => Promise<Omit<PromoteUploadDeps, "config" | "witnessKey"> & { witnessKey?: string }>;

/** S3 notification keys are URL-encoded with `+` for space. Null: not a notification record this handler serves. */
export function notificationKeys(body: unknown): string[] | null {
  let v: unknown;
  try { v = typeof body === "string" ? JSON.parse(body) : null; } catch { return null; }
  if (!v || typeof v !== "object" || Array.isArray(v)) return null;
  const o = v as Record<string, unknown>;
  if (o.Event === "s3:TestEvent") return [];
  if (!Array.isArray(o.Records) || o.Records.length === 0) return null;
  const keys: string[] = [];
  for (const r of o.Records) {
    const k = (r as { s3?: { object?: { key?: unknown } } } | null)?.s3?.object?.key;
    if (typeof r !== "object" || r === null || typeof (r as { eventName?: unknown }).eventName !== "string" || !(r as { eventName: string }).eventName.startsWith("ObjectCreated:") || typeof k !== "string") return null;
    try { keys.push(decodeURIComponent(k.replace(/\+/g, " "))); } catch { return null; }
  }
  return keys;
}

type Log = (line: Record<string, unknown>) => void;
const defaultLog: Log = (line) => console.log(JSON.stringify({ route: "upload.admission", ...line }));

export function createUploadAdmissionHandler(runtime: UploadAdmissionRuntimeFactory | undefined, log: Log = defaultLog) {
  let deps: ReturnType<UploadAdmissionRuntimeFactory> | undefined;
  return async function handler(event: unknown, _context?: unknown): Promise<UploadAdmissionBatchResponse> {
    declareScrubTsCapability("hosted-lambda"); // hosted Lambda zip, no relayscrubber artifacts (docs/scrub-engine.md "Default flip")
    const records: SqsRecordLike[] = Array.isArray((event as { Records?: unknown } | null)?.Records) ? (event as { Records: SqsRecordLike[] }).Records : [];
    const ids = records.map((r) => (typeof r?.messageId === "string" ? r.messageId : ""));
    const failAll = (code: string, cause?: string) => {
      log({ outcome: code, records: records.length, ...(cause ? { cause } : {}) });
      return { batchItemFailures: ids.map((itemIdentifier) => ({ itemIdentifier })) };
    };
    let config: UploadDirectConfig;
    try { config = uploadDirectConfig(process.env); } catch { return failAll("history_upload_direct_invalid"); }
    if (config.mode === "off") return failAll("history_upload_direct_off");
    if (!runtime) return failAll("runtime_unavailable");
    let base: Awaited<ReturnType<UploadAdmissionRuntimeFactory>>;
    try {
      base = await (deps ??= runtime(process.env, config));
    } catch (error) {
      deps = undefined;
      // Coded runtime errors only (upload-admission-hosted.ts UploadAdmissionRuntimeError, AwsCallError): never a message.
      const code = (error as { code?: unknown } | null)?.code;
      return failAll("runtime_unavailable", typeof code === "string" && /^[A-Za-z0-9_]{1,64}$/.test(code) ? code : undefined);
    }
    const promote: PromoteUploadDeps = { ...base, config, witnessKey: base.witnessKey ?? process.env[UPLOAD_WITNESS_KEY_VAR] };
    const failures: { itemIdentifier: string }[] = [];
    const counts: Record<string, number> = {};
    for (let i = 0; i < records.length; i++) {
      const id = ids[i]!;
      const keys = notificationKeys(records[i]?.body);
      let failed = !id || keys === null;
      if (keys === null) counts.malformed_record = (counts.malformed_record ?? 0) + 1;
      for (const key of failed ? [] : keys!) {
        const result: PromoteResult = await promoteUpload(key, promote).catch((error) => ({ action: "retry", code: "promote_failed", error }) as const);
        const cause = result.action === "retry" ? (result.error as { code?: unknown } | undefined)?.code : undefined;
        const label = result.action === "ack" ? (result.code ?? result.outcome)
          : typeof cause === "string" && /^[A-Za-z0-9_]{1,64}$/.test(cause) ? `${result.code}:${cause}` : result.code;
        counts[label] = (counts[label] ?? 0) + 1;
        if (result.action !== "ack") failed = true;
      }
      if (failed) failures.push({ itemIdentifier: id });
    }
    log({ outcome: "batch", records: records.length, failures: failures.length, ...counts });
    return { batchItemFailures: failures };
  };
}

/** The hosted factory, loaded lazily so a flag-off invocation imports no runtime code. */
export const hostedRuntime: UploadAdmissionRuntimeFactory = async (env, config) =>
  (await import("./upload-admission-hosted.ts")).hostedUploadAdmissionRuntime()(env, config);

/** The deployed entry (stacks/aws/upload-inbox.ts uploadAdmissionFunctionProps). */
export const handler = createUploadAdmissionHandler(hostedRuntime);
