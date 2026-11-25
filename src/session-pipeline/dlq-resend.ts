/**
 * Per-message dead-letter resend for the indexing queue (operator tool; CLI: scripts/dlq-resend.ts,
 * usage: docs/handoffs/dlq-resend.md; the operator procedure lives in the private ops repository).
 *
 * SQS StartMessageMoveTask moves a DLQ in queue order and cannot pick messages. This scans the DLQ
 * with ReceiveMessage (each received message is hidden while it is decided) and resends ONLY messages
 * whose session identity is selected:
 *
 *   selected, under the cap, execute   SendMessage(identical body + attributes) to the target; only after
 *                                      a VALID acknowledgement (MessageId, MD5 of the body) DeleteMessage
 *   anything else (and every dry run)  ChangeMessageVisibility 0 at once, so nothing stays held
 *
 * Before any receive (dry run included) the queue pair is proven: both URLs are canonical SQS URLs, the
 * QueueArns match the URLs and differ, the target's RedrivePolicy dead-letters into exactly this DLQ,
 * and the DLQ has no RedrivePolicy of its own (so receive counts on it trigger nothing).
 *
 * An outcome that may have committed without proof (a send timeout or lost response, a malformed
 * acknowledgement, a delete failure) is a duplicate risk: nothing more is sent, the rest of the batch is
 * released, and the summary names it for reconciliation.
 *
 * The identity is sha256_hex(utf8("<org>/<ws>/<m>/<h>/<s>")) of the key(s) the consumer itself would
 * read from the body (s3-events.ts parseQueueBody, the consumer's parser). An identity selects EVERY
 * message of that session, not one body or revision.
 *
 * Output is JSON lines carrying hashes and fixed codes only: never a key, queue URL, org, workspace or
 * session id, nor an operator-supplied class name. No production identifier lives here.
 */
import { createHash } from "node:crypto";
import { parseSessionKey, parseUploadGenerationPrefix } from "@rhc-object-store";
import { RETENTION_BUCKET_HARNESS } from "@rhc/lib/pipeline/adapter.ts";
import { awsJsonCall, AwsCallError, type AwsSessionCredentials } from "./aws-json.ts";
import { parseQueueBody, RECONCILE_BODY_KEY } from "./s3-events.ts";
import { NO_SESSION_HARNESS } from "../session-ingest/writer.ts";

export const MAX_RESEND_CAP = 100;
export const MAX_RATE_PER_SECOND = 5;
export const DEFAULT_VISIBILITY_SECONDS = 120;
export const MIN_VISIBILITY_SECONDS = 60;
export const DEFAULT_EMPTY_RECEIVES = 3;
export const CONFIRM_ENV = "DLQ_RESEND_CONFIRM";
export const CONFIRM_VALUE = "resend";
export const SEND_TIMEOUT_MS = 10_000;
export const DELETE_TIMEOUT_MS = 10_000;
/** Remaining hold required before a send: both bounded calls plus this margin. */
export const HOLD_MARGIN_MS = 10_000;
export const HOLD_NEEDED_MS = SEND_TIMEOUT_MS + DELETE_TIMEOUT_MS + HOLD_MARGIN_MS;
export const SELECTED_CLASS = "selected";

const HASH = /^[0-9a-f]{64}$/;
const sha256Hex = (s: string) => createHash("sha256").update(s, "utf8").digest("hex");
const md5Hex = (s: string) => createHash("md5").update(s, "utf8").digest("hex");
/** The printable form of an operator-supplied class name: a fixed code or a hash, never the string. */
export const classCode = (cls: string) => (cls === SELECTED_CLASS ? SELECTED_CLASS : `class_${sha256Hex(cls).slice(0, 12)}`);

export class DlqResendRefusal extends Error {
  readonly reason: string;
  constructor(reason: string) {
    super(`dlq-resend refused: ${reason}`);
    this.name = "DlqResendRefusal";
    this.reason = reason;
  }
}
const refuse = (reason: string): never => {
  throw new DlqResendRefusal(reason);
};

// ---------------------------------------------------------------------------------------------
// Identity (selector parity with the consumer)

export type IdentityReject =
  | "test_event"
  | "invalid_body"
  | "no_events"
  | "malformed_records"
  | "mixed_records"
  | "not_session_key"
  | "managed_publication_hint"
  | "retention_bucket"
  | "origin_bucket"
  | "not_a_manifest"
  | "no_org";
export type IdentityResult = { ok: true; identity: string } | { ok: false; reason: IdentityReject };

/**
 * The identity of one already-normalized object key (as the consumer holds it after parseQueueBody),
 * with the pointer stage's pre-lookup ignores (pointer.ts indexPointer) applied in the same order.
 * The org is the prefix org resolver's (adapter.ts prefixOrgResolver): the last segment of the
 * deployment prefix, upload-generation suffix removed.
 */
export function identityOfKey(key: string): IdentityResult {
  const location = parseSessionKey(key);
  if (!location) return { ok: false, reason: "not_session_key" };
  if (location.prefix?.includes("/upload-generations/")) return { ok: false, reason: "managed_publication_hint" };
  if (location.harness === RETENTION_BUCKET_HARNESS) return { ok: false, reason: "retention_bucket" };
  if (location.harness === NO_SESSION_HARNESS) return { ok: false, reason: "origin_bucket" };
  if (location.rest !== "manifest.json") return { ok: false, reason: "not_a_manifest" };
  const org = (parseUploadGenerationPrefix(location.prefix)?.base ?? location.prefix)?.split("/").filter(Boolean).pop();
  if (!org) return { ok: false, reason: "no_org" };
  return { ok: true, identity: sha256Hex(`${org}/${location.workspaceId}/${location.machineId}/${location.harness}/${location.sessionId}`) };
}

/**
 * The identity of one DLQ message body, normalized by the consumer's own parser: a TestEvent is ignored
 * first, reconcile keys are used literally, S3 records are validated (event source and name) and their
 * keys decoded exactly as the consumer decodes them. Every record must yield the same identity; a body
 * with a record the consumer drops, or with mixed identities, is rejected.
 */
export function identityOfBody(body: string): IdentityResult {
  const parsed = parseQueueBody(body);
  if (parsed.kind === "test") return { ok: false, reason: "test_event" };
  if (parsed.kind === "invalid") return { ok: false, reason: "invalid_body" };
  if (parsed.events.length === 0) return { ok: false, reason: "no_events" };
  // parseQueueBody silently drops S3 records it cannot use; the selector refuses such a body instead.
  const value = JSON.parse(body) as Record<string, unknown>;
  const reconcile = value[RECONCILE_BODY_KEY];
  if (!(reconcile && typeof reconcile === "object") && (!Array.isArray(value.Records) || value.Records.length !== parsed.events.length)) return { ok: false, reason: "malformed_records" };
  let identity: string | undefined;
  for (const event of parsed.events) {
    const result = identityOfKey(event.key);
    if (!result.ok) return result;
    if (identity !== undefined && identity !== result.identity) return { ok: false, reason: "mixed_records" };
    identity = result.identity;
  }
  return { ok: true, identity: identity! };
}

// ---------------------------------------------------------------------------------------------
// Selection files

/** `--select`: one bare lowercase identity hash per line; blank lines and `#` comments ignored. */
export function parseSelectFile(text: string): Set<string> {
  const out = new Set<string>();
  for (const [i, raw] of text.split(/\r?\n/).entries()) {
    const line = raw.replace(/#.*$/, "").trim();
    if (!line) continue;
    if (!HASH.test(line)) refuse(`select_file_line_${i + 1}_not_a_hash`);
    out.add(line);
  }
  return out;
}

/** The classes file: a JSON object mapping identity hash to class name. */
export function parseClassesFile(text: string): Map<string, string> {
  let value: unknown;
  try {
    value = JSON.parse(text);
  } catch {
    return refuse("classes_file_not_json");
  }
  if (!value || typeof value !== "object" || Array.isArray(value)) return refuse("classes_file_not_an_object");
  const out = new Map<string, string>();
  for (const [hash, cls] of Object.entries(value as Record<string, unknown>)) {
    if (!HASH.test(hash)) refuse("classes_file_key_not_a_hash");
    if (typeof cls !== "string" || !/^[A-Za-z0-9._:-]{1,64}$/.test(cls)) refuse("classes_file_bad_class_name");
    out.set(hash, cls as string);
  }
  return out;
}

// ---------------------------------------------------------------------------------------------
// Queue URLs

export interface QueueRef {
  url: string;
  region: string;
  account: string;
  name: string;
  /** The ARN the URL names; GetQueueAttributes must return exactly this. */
  arn: string;
}

/**
 * A canonical SQS queue URL in the commercial or GovCloud partition: `https://sqs.<region>.amazonaws.com/
 * <12-digit account>/<name>`. Legacy (`<region>.queue.amazonaws.com`), VPC endpoint, China, custom and
 * unknown hosts are refused: no region is ever guessed.
 */
export function parseQueueUrl(raw: string): QueueRef {
  let url: URL;
  try {
    url = new URL(raw);
  } catch {
    return refuse("queue_url_invalid");
  }
  if (url.protocol !== "https:" || url.port || url.search || url.hash || url.username || url.password) refuse("queue_url_invalid");
  const host = /^sqs\.((?:us-gov-[a-z]+-\d)|(?:[a-z]{2}-[a-z]+-\d))\.amazonaws\.com$/.exec(url.host);
  if (!host) refuse("queue_host_unsupported");
  const path = /^\/(\d{12})\/([A-Za-z0-9_-]{1,80})\/?$/.exec(url.pathname);
  if (!path) refuse("queue_url_path_invalid");
  const region = host![1]!;
  const partition = region.startsWith("us-gov-") ? "aws-us-gov" : "aws";
  const [, account, name] = path!;
  return { url: `https://${url.host}/${account}/${name}`, region, account: account!, name: name!, arn: `arn:${partition}:sqs:${region}:${account}:${name}` };
}

// ---------------------------------------------------------------------------------------------
// Options

export interface DlqResendOptions {
  dlq: QueueRef;
  target: QueueRef;
  /** Identity hash -> class name (SELECTED_CLASS when none was given). Printed only as classCode(). */
  selection: Map<string, string>;
  max: number;
  ratePerSecond: number;
  execute: boolean;
  visibilitySeconds: number;
  emptyReceivesToStop: number;
  /** Messages seen before stopping; default 2x the DLQ's ApproximateNumberOfMessages at start (at least 10). */
  maxScan?: number;
  /** ReceiveMessage long-poll seconds (0..20). */
  waitSeconds: number;
}

export interface ParsedArgs {
  "dlq-url"?: string;
  "target-url"?: string;
  select?: string;
  "select-class"?: string;
  classes?: string;
  max?: string;
  rate?: string;
  execute?: boolean;
  "visibility-seconds"?: string;
  "empty-receives"?: string;
  "max-scan"?: string;
  "wait-seconds"?: string;
}

function intArg(name: string, raw: string | undefined, fallback: number | undefined, min: number, max: number): number {
  if (raw === undefined) {
    if (fallback === undefined) refuse(`${name}_required`);
    return fallback!;
  }
  if (!/^\d+$/.test(raw)) refuse(`${name}_not_an_integer`);
  const n = Number(raw);
  if (n < min || n > max) refuse(`${name}_out_of_range_${min}_${max}`);
  return n;
}

function numArg(name: string, raw: string | undefined, fallback: number, min: number, max: number): number {
  if (raw === undefined) return fallback;
  if (!/^\d+(\.\d+)?$/.test(raw)) refuse(`${name}_not_a_number`);
  const n = Number(raw);
  if (!(n >= min && n <= max)) refuse(`${name}_out_of_range_${min}_${max}`);
  return n;
}

/**
 * Every refusal the operator can hit before any AWS call. `readFile` and `env` are injected so tests
 * need no files. The DLQ_RESEND_CONFIRM check runs only when executing.
 */
export function resolveOptions(
  args: ParsedArgs,
  env: Record<string, string | undefined>,
  readFile: (path: string) => string,
): DlqResendOptions {
  if (!args["dlq-url"]) refuse("dlq_url_required");
  if (!args["target-url"]) refuse("target_url_required");
  const dlq = parseQueueUrl(args["dlq-url"]!);
  const target = parseQueueUrl(args["target-url"]!);
  if (dlq.arn === target.arn) refuse("dlq_url_equals_target_url");
  if (dlq.region !== target.region || dlq.account !== target.account) refuse("queues_not_in_one_account_and_region");
  if (dlq.name.endsWith(".fifo") || target.name.endsWith(".fifo")) refuse("fifo_queue_unsupported");
  const max = intArg("max", args.max, undefined, 1, MAX_RESEND_CAP);
  const ratePerSecond = numArg("rate", args.rate, 1, 0.01, MAX_RATE_PER_SECOND);
  const visibilitySeconds = intArg("visibility_seconds", args["visibility-seconds"], DEFAULT_VISIBILITY_SECONDS, MIN_VISIBILITY_SECONDS, 43_200);
  const emptyReceivesToStop = intArg("empty_receives", args["empty-receives"], DEFAULT_EMPTY_RECEIVES, 1, 100);
  const maxScan = args["max-scan"] === undefined ? undefined : intArg("max_scan", args["max-scan"], undefined, 1, 1_000_000);
  const waitSeconds = intArg("wait_seconds", args["wait-seconds"], 2, 0, 20);

  if (args.select && args["select-class"]) refuse("select_and_select_class_are_exclusive");
  if (!args.select && !args["select-class"]) refuse("select_or_select_class_required");
  const classes = args.classes ? parseClassesFile(readFile(args.classes)) : undefined;
  const selection = new Map<string, string>();
  if (args.select) {
    for (const hash of parseSelectFile(readFile(args.select))) selection.set(hash, classes?.get(hash) ?? SELECTED_CLASS);
  } else {
    if (!classes) refuse("select_class_needs_classes_file");
    for (const [hash, cls] of classes!) if (cls === args["select-class"]) selection.set(hash, cls);
  }
  if (selection.size === 0) refuse("selection_empty");

  const execute = args.execute === true;
  if (execute && env[CONFIRM_ENV] !== CONFIRM_VALUE) refuse("execute_needs_DLQ_RESEND_CONFIRM");
  return { dlq, target, selection, max, ratePerSecond, execute, visibilitySeconds, emptyReceivesToStop, maxScan, waitSeconds };
}

// ---------------------------------------------------------------------------------------------
// Credentials

const ENV_KEYS = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"] as const;

/**
 * An assumed-role session from the environment (all three of AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
 * AWS_SESSION_TOKEN), or, when none of them is set, AWS_PROFILE through `aws configure
 * export-credentials --format process` (`exportProfile`), parsed and held in memory only. Some but not
 * all of the three set is refused rather than falling through to the profile.
 */
export function operatorCredentials(
  env: Record<string, string | undefined>,
  exportProfile: (profile: string) => string,
): AwsSessionCredentials {
  const set = ENV_KEYS.filter((k) => !!env[k]);
  if (set.length === ENV_KEYS.length)
    return { accessKeyId: env.AWS_ACCESS_KEY_ID!, secretAccessKey: env.AWS_SECRET_ACCESS_KEY!, sessionToken: env.AWS_SESSION_TOKEN! };
  if (set.length > 0) refuse("env_credentials_incomplete");
  if (!env.AWS_PROFILE) refuse("no_aws_credentials");
  let parsed: { AccessKeyId?: unknown; SecretAccessKey?: unknown; SessionToken?: unknown };
  try {
    parsed = JSON.parse(exportProfile(env.AWS_PROFILE!));
  } catch {
    return refuse("profile_credentials_unavailable");
  }
  if (typeof parsed.AccessKeyId !== "string" || !parsed.AccessKeyId || typeof parsed.SecretAccessKey !== "string" || !parsed.SecretAccessKey)
    refuse("profile_credentials_incomplete");
  return {
    accessKeyId: parsed.AccessKeyId as string,
    secretAccessKey: parsed.SecretAccessKey as string,
    ...(typeof parsed.SessionToken === "string" && parsed.SessionToken ? { sessionToken: parsed.SessionToken } : {}),
  };
}

// ---------------------------------------------------------------------------------------------
// SQS client

export interface SqsMessageAttribute {
  DataType: string;
  StringValue?: string;
  BinaryValue?: string;
}
export interface SqsMessage {
  MessageId: string;
  ReceiptHandle: string;
  Body: string;
  MessageAttributes?: Record<string, SqsMessageAttribute & Record<string, unknown>>;
  /** System attributes; the resend requests DeadLetterQueueSourceArn (the queue that dead-lettered it). */
  Attributes?: Record<string, string>;
}
export interface SendAck {
  MessageId?: unknown;
  MD5OfMessageBody?: unknown;
}

/** The SQS calls the resend makes. Tests inject a fake; `awsSqsClient` is the real one. */
export interface SqsClient {
  attributes(queueUrl: string, names: string[]): Promise<Record<string, string>>;
  receive(queueUrl: string, args: { max: number; visibilitySeconds: number; waitSeconds: number }): Promise<SqsMessage[]>;
  send(queueUrl: string, body: string, attributes: Record<string, SqsMessageAttribute> | undefined, timeoutMs: number): Promise<SendAck>;
  delete(queueUrl: string, receiptHandle: string, timeoutMs: number): Promise<void>;
  setVisibility(queueUrl: string, receiptHandle: string, seconds: number): Promise<void>;
}

/** SQS over the JSON protocol (awsJsonCall: SigV4, no SDK). The region comes from the validated queue URL. */
export function awsSqsClient(credentials: () => Promise<AwsSessionCredentials>, opts: { fetcher?: typeof fetch } = {}): SqsClient {
  const call = async <T>(queueUrl: string, action: string, body: Record<string, unknown>, timeoutMs: number) =>
    awsJsonCall<T>({
      service: "sqs",
      target: `AmazonSQS.${action}`,
      region: parseQueueUrl(queueUrl).region,
      body: { QueueUrl: queueUrl, ...body },
      credentials: await credentials(),
      timeoutMs,
      ...(opts.fetcher ? { fetcher: opts.fetcher } : {}),
    });
  return {
    async attributes(queueUrl, names) {
      const out = await call<{ Attributes?: Record<string, string> }>(queueUrl, "GetQueueAttributes", { AttributeNames: names }, 10_000);
      return out.Attributes ?? {};
    },
    async receive(queueUrl, { max, visibilitySeconds, waitSeconds }) {
      const out = await call<{ Messages?: SqsMessage[] }>(queueUrl, "ReceiveMessage", {
        MaxNumberOfMessages: max,
        VisibilityTimeout: visibilitySeconds,
        WaitTimeSeconds: waitSeconds,
        MessageAttributeNames: ["All"],
        MessageSystemAttributeNames: ["DeadLetterQueueSourceArn"],
      }, (waitSeconds + 10) * 1000);
      return out.Messages ?? [];
    },
    async send(queueUrl, body, attributes, timeoutMs) {
      return call<SendAck>(queueUrl, "SendMessage", { MessageBody: body, ...(attributes ? { MessageAttributes: attributes } : {}) }, timeoutMs);
    },
    async delete(queueUrl, receiptHandle, timeoutMs) {
      await call(queueUrl, "DeleteMessage", { ReceiptHandle: receiptHandle }, timeoutMs);
    },
    async setVisibility(queueUrl, receiptHandle, seconds) {
      await call(queueUrl, "ChangeMessageVisibility", { ReceiptHandle: receiptHandle, VisibilityTimeout: seconds }, 10_000);
    },
  };
}

const DATA_TYPE = /^(String|Number|Binary)(\.[A-Za-z0-9_.-]{1,250})?$/;
const BASE64 = /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/;
const emptyList = (v: unknown) => v === undefined || (Array.isArray(v) && v.length === 0);

/**
 * The received message attributes as SendMessage takes them (DataType plus StringValue or BinaryValue),
 * or `null` (fail closed) when any one of them cannot be carried over exactly: an unknown type, a
 * missing or extra value, a non-empty list value, invalid base64, or a name/type/value that does not
 * survive JSON serialization unchanged. Built on a null-prototype object so a name such as `__proto__`
 * is an ordinary key, not dropped. `undefined` when the message has none.
 */
export function sendableAttributes(attrs: SqsMessage["MessageAttributes"]): Record<string, SqsMessageAttribute> | null | undefined {
  if (attrs === undefined) return undefined;
  if (!attrs || typeof attrs !== "object" || Array.isArray(attrs)) return null;
  const entries = Object.entries(attrs);
  if (entries.length === 0) return undefined;
  const out: Record<string, SqsMessageAttribute> = Object.create(null);
  for (const [name, a] of entries) {
    if (!name || !a || typeof a !== "object" || typeof a.DataType !== "string" || !DATA_TYPE.test(a.DataType)) return null;
    if (!emptyList(a.StringListValues) || !emptyList(a.BinaryListValues)) return null;
    const binary = a.DataType.startsWith("Binary");
    if (binary) {
      if (typeof a.BinaryValue !== "string" || a.StringValue !== undefined || !BASE64.test(a.BinaryValue)) return null;
      Object.defineProperty(out, name, { value: { DataType: a.DataType, BinaryValue: a.BinaryValue }, enumerable: true, writable: true, configurable: true });
    } else {
      if (typeof a.StringValue !== "string" || a.BinaryValue !== undefined) return null;
      Object.defineProperty(out, name, { value: { DataType: a.DataType, StringValue: a.StringValue }, enumerable: true, writable: true, configurable: true });
    }
  }
  // Round-trip proof: exactly the same names, types and values after serialization.
  const back = JSON.parse(JSON.stringify(out)) as Record<string, Record<string, unknown>>;
  const names = Object.keys(back);
  if (names.length !== entries.length) return null;
  for (const [name, a] of entries) {
    if (!Object.prototype.hasOwnProperty.call(back, name)) return null;
    const b = back[name]!;
    const valueKey = a.DataType.startsWith("Binary") ? "BinaryValue" : "StringValue";
    if (Object.keys(b).length !== 2 || b.DataType !== a.DataType || b[valueKey] !== a[valueKey]) return null;
  }
  return out;
}

/**
 * The queue pair, proven before any receive: QueueArns equal to the URLs' ARNs and distinct, the
 * target's RedrivePolicy dead-lettering into exactly this DLQ, and no RedrivePolicy on the DLQ.
 * Returns the DLQ's approximate size.
 */
export async function verifyQueuePair(sqs: SqsClient, dlq: QueueRef, target: QueueRef): Promise<number> {
  const names = ["QueueArn", "RedrivePolicy", "ApproximateNumberOfMessages"];
  const d = await sqs.attributes(dlq.url, names);
  const t = await sqs.attributes(target.url, names);
  if (d.QueueArn !== dlq.arn) refuse("dlq_arn_does_not_match_url");
  if (t.QueueArn !== target.arn) refuse("target_arn_does_not_match_url");
  if (d.QueueArn === t.QueueArn) refuse("dlq_arn_equals_target_arn");
  if (d.RedrivePolicy !== undefined && d.RedrivePolicy !== "") refuse("dlq_has_redrive_policy");
  let deadLetterTargetArn: unknown;
  try {
    deadLetterTargetArn = (JSON.parse(t.RedrivePolicy ?? "") as { deadLetterTargetArn?: unknown }).deadLetterTargetArn;
  } catch {
    return refuse("target_has_no_redrive_policy");
  }
  if (deadLetterTargetArn !== dlq.arn) refuse("target_redrive_is_not_this_dlq");
  const n = Number(d.ApproximateNumberOfMessages ?? "0");
  return Number.isFinite(n) && n >= 0 ? n : 0;
}

// ---------------------------------------------------------------------------------------------
// The scan

export type StopReason = "cap_reached" | "empty_receives" | "max_scan" | "receive_failed" | "halted_ambiguous_send" | "halted_delete_failure" | "halted_hold_lapsed";

export interface DlqResendSummary {
  event: "summary";
  mode: "execute" | "dry_run";
  stop: StopReason;
  scanned: number;
  unique: number;
  selected: number;
  resent: number;
  wouldResend: number;
  skipped: number;
  skipReasons: Record<string, number>;
  errors: number;
  errorCodes: Record<string, number>;
  duplicateRisk: number;
  /** Holds extended (ChangeMessageVisibility) so a send and delete fit inside them. */
  holdExtended: number;
  /** Messages whose fate is unproven (sent or not): reconcile these by identity and bodySha256. */
  reconcile: { identity: string; bodySha256: string; class: string; outcome: string }[];
  perClass: Record<string, number>;
  maxScan: number;
}

export interface DlqResendDeps {
  sqs: SqsClient;
  /** One JSON-lines record (already hash-only). */
  emit: (record: Record<string, unknown>) => void;
  /** Monotonic milliseconds. */
  now?: () => number;
  sleep?: (ms: number) => Promise<void>;
}

const errorCode = (e: unknown) => {
  const code = (e as { code?: unknown })?.code ?? (e as { name?: unknown })?.name;
  return typeof code === "string" && /^[A-Za-z0-9_.:-]{1,64}$/.test(code) ? code : "error";
};
/** Only an HTTP error answer from SQS proves a send did not commit; a 5xx, timeout or lost response may have. */
const definitelyNotSent = (e: unknown) => e instanceof AwsCallError && e.status >= 400 && e.status < 500;
const bump = (m: Record<string, number>, k: string) => {
  m[k] = (m[k] ?? 0) + 1;
};

export async function runDlqResend(options: DlqResendOptions, deps: DlqResendDeps): Promise<DlqResendSummary> {
  const now = deps.now ?? (() => performance.now());
  const sleep = deps.sleep ?? ((ms: number) => new Promise<void>((r) => setTimeout(r, ms)));
  const { sqs, emit } = deps;
  const dlqUrl = options.dlq.url;
  const mode = options.execute ? "execute" : "dry_run";
  if (options.visibilitySeconds * 1000 <= HOLD_NEEDED_MS) refuse("visibility_shorter_than_send_and_delete");
  const sizeAtStart = await verifyQueuePair(sqs, options.dlq, options.target);
  const maxScan = options.maxScan ?? Math.max(2 * sizeAtStart, 10);
  emit({ event: "start", mode, max: options.max, rate: options.ratePerSecond, visibilitySeconds: options.visibilitySeconds, dlqApproximateSize: sizeAtStart, maxScan, selectionSize: options.selection.size, queuePair: "verified" });

  const s: DlqResendSummary = {
    event: "summary", mode, stop: "max_scan", scanned: 0, unique: 0, selected: 0, resent: 0, wouldResend: 0, skipped: 0,
    skipReasons: {}, errors: 0, errorCodes: {}, duplicateRisk: 0, holdExtended: 0, reconcile: [], perClass: {}, maxScan,
  };
  /** Message ids already decided this run: a message seen again (visibility reset, a lapsed hold) is never acted on twice. */
  const decided = new Set<string>();
  const minIntervalMs = 1000 / options.ratePerSecond;
  let lastSendAt = -Infinity;
  let attempts = 0;
  let emptyStreak = 0;
  let halted: StopReason | null = null;

  /** Visibility back to 0 at once; `reason` null (a dry-run would-resend) is not counted as skipped. */
  const release = async (m: SqsMessage, reason: string | null) => {
    if (reason) {
      s.skipped++;
      bump(s.skipReasons, reason);
    }
    try {
      await sqs.setVisibility(dlqUrl, m.ReceiptHandle, 0);
    } catch (e) {
      s.errors++;
      bump(s.errorCodes, `visibility_reset:${errorCode(e)}`);
    }
  };
  const ambiguous = (line: { identity: string; bodySha256: string; class: string }, outcome: string, stop: StopReason) => {
    s.duplicateRisk++;
    bump(s.errorCodes, outcome);
    s.reconcile.push({ ...line, outcome });
    emit({ event: "duplicate_risk", ...line, outcome });
    halted = stop;
  };

  while (true) {
    if (attempts >= options.max) { s.stop = "cap_reached"; break; }
    if (s.scanned >= maxScan) { s.stop = "max_scan"; break; }
    // The hold is measured from before the call: the conservative end of every message in the batch.
    const holdDeadline = now() + options.visibilitySeconds * 1000;
    let batch: SqsMessage[];
    try {
      batch = await sqs.receive(dlqUrl, { max: Math.min(10, maxScan - s.scanned), visibilitySeconds: options.visibilitySeconds, waitSeconds: options.waitSeconds });
    } catch (e) {
      s.errors++;
      bump(s.errorCodes, `receive:${errorCode(e)}`);
      s.stop = "receive_failed";
      break;
    }
    // "Empty" means nothing new: released messages come straight back, so repeats are no progress.
    if (!batch.some((m) => !decided.has(m.MessageId))) {
      for (const m of batch) { s.scanned++; await release(m, "repeat"); }
      if (++emptyStreak >= options.emptyReceivesToStop) { s.stop = "empty_receives"; break; }
      continue;
    }
    emptyStreak = 0;
    for (const m of batch) {
      s.scanned++;
      if (halted) { await release(m, "halted"); continue; }
      if (decided.has(m.MessageId)) { await release(m, "repeat"); continue; }
      decided.add(m.MessageId);
      s.unique++;
      // A DLQ can serve several sources: only messages dead-lettered by the verified target go back to it.
      // Absent (a direct insert) or another source: released, never sent.
      if (m.Attributes?.DeadLetterQueueSourceArn !== options.target.arn) { await release(m, "provenance_mismatch"); continue; }
      const id = identityOfBody(m.Body);
      if (!id.ok) { await release(m, id.reason); continue; }
      const cls = options.selection.get(id.identity);
      if (cls === undefined) { await release(m, "not_selected"); continue; }
      s.selected++;
      const attributes = sendableAttributes(m.MessageAttributes);
      if (attributes === null) {
        s.errors++;
        bump(s.errorCodes, "attributes_unserializable");
        await release(m, "attributes_unserializable");
        continue;
      }
      if (attempts >= options.max) { await release(m, "after_cap"); continue; }
      attempts++;
      const code = classCode(cls);
      const line = { identity: id.identity, bodySha256: sha256Hex(m.Body).slice(0, 16), class: code };
      if (!options.execute) {
        s.wouldResend++;
        bump(s.perClass, code);
        emit({ event: "would_resend", ...line });
        await release(m, null);
        continue;
      }
      const wait = lastSendAt + minIntervalMs - now();
      if (wait > 0) await sleep(wait);
      // Send and delete must both finish inside the hold, or the receipt could lapse and another receive
      // (this run or a later one) could resend it. Extend the hold when it is short.
      let deadline = holdDeadline;
      if (deadline - now() < HOLD_NEEDED_MS) {
        const before = now();
        try {
          await sqs.setVisibility(dlqUrl, m.ReceiptHandle, options.visibilitySeconds);
          deadline = before + options.visibilitySeconds * 1000;
          s.holdExtended++;
        } catch (e) {
          s.errors++;
          bump(s.errorCodes, `hold_extend:${errorCode(e)}`);
          emit({ event: "send_skipped", ...line, code: "hold_extend_failed" });
          continue;
        }
      }
      if (deadline - now() < HOLD_NEEDED_MS) {
        s.errors++;
        bump(s.errorCodes, "hold_too_short");
        emit({ event: "send_skipped", ...line, code: "hold_too_short" });
        continue;
      }
      lastSendAt = now();
      let ack: SendAck;
      try {
        ack = await sqs.send(options.target.url, m.Body, attributes, SEND_TIMEOUT_MS);
      } catch (e) {
        if (definitelyNotSent(e)) {
          // Refused by SQS: left in the DLQ (no delete); it reappears when its hold ends.
          s.errors++;
          bump(s.errorCodes, `send:${errorCode(e)}`);
          emit({ event: "send_failed", ...line, code: errorCode(e) });
          continue;
        }
        ambiguous(line, `send_unconfirmed:${errorCode(e)}`, "halted_ambiguous_send");
        continue;
      }
      if (typeof ack?.MessageId !== "string" || !ack.MessageId || ack.MD5OfMessageBody !== md5Hex(m.Body)) {
        ambiguous(line, "send_ack_invalid", "halted_ambiguous_send");
        continue;
      }
      s.resent++;
      bump(s.perClass, code);
      // A slow ack (or a suspended process) can outlive the hold; past it another receiver may hold the
      // message, so this receipt must not be used. Halt and reconcile instead of deleting.
      if (deadline - now() < DELETE_TIMEOUT_MS + HOLD_MARGIN_MS) {
        ambiguous(line, "hold_lapsed_before_delete", "halted_hold_lapsed");
        continue;
      }
      try {
        await sqs.delete(dlqUrl, m.ReceiptHandle, DELETE_TIMEOUT_MS);
      } catch (e) {
        // Sent but maybe still in the DLQ: a later run could resend it. Idempotent downstream on the same
        // key and revision, but counted, reported and nothing more is sent.
        ambiguous(line, `delete_unconfirmed:${errorCode(e)}`, "halted_delete_failure");
        continue;
      }
      emit({ event: "resent", ...line });
    }
    if (halted) { s.stop = halted; break; }
    if (attempts >= options.max) { s.stop = "cap_reached"; break; }
  }
  emit({ ...s });
  return s;
}
