/**
 * Per-message dead-letter resend for the indexing queue (operator tool; CLI: scripts/dlq-resend.ts,
 * runbook: docs/handoffs/dlq-resend.md).
 *
 * SQS StartMessageMoveTask moves a DLQ in queue order and cannot pick messages. This scans the DLQ
 * with ReceiveMessage (each received message is hidden for `visibilitySeconds` while it is decided)
 * and resends ONLY messages whose session identity is selected:
 *
 *   selected, under the cap, execute   SendMessage(identical body + attributes) to the target, then,
 *                                      only after the send succeeded, DeleteMessage from the DLQ
 *   anything else (and every dry run)  ChangeMessageVisibility 0 at once, so nothing stays held
 *
 * The identity is sha256_hex(utf8("<org>/<ws>/<m>/<h>/<s>")) from the decoded S3 key: the org is the
 * last segment of the key's deployment prefix (the prefix org resolver's rule), the ws=, m=, h= and
 * s= values without their labels. The date is not part of it. The sessionless origin bucket
 * (h=_none) has no identity and is never resent.
 *
 * Output is JSON lines carrying identity hashes, a body digest prefix and the class only: never a
 * key, queue URL, org, workspace or session id. No production identifier lives here; queue URLs come
 * from the operator at run time.
 */
import { createHash } from "node:crypto";
import { parseSessionKey, parseUploadGenerationPrefix } from "@rhc-object-store";
import { awsJsonCall, type AwsSessionCredentials } from "./aws-json.ts";
import { decodeS3Key, RECONCILE_BODY_KEY } from "./s3-events.ts";
import { NO_SESSION_HARNESS } from "../session-ingest/writer.ts";

export const MAX_RESEND_CAP = 100;
export const MAX_RATE_PER_SECOND = 5;
export const DEFAULT_VISIBILITY_SECONDS = 120;
export const DEFAULT_EMPTY_RECEIVES = 3;
export const CONFIRM_ENV = "DLQ_RESEND_CONFIRM";
export const CONFIRM_VALUE = "resend";
/** A send is not started with less than this much of the hold left (the receipt could lapse mid-send). */
export const HOLD_MARGIN_SECONDS = 10;

const HASH = /^[0-9a-f]{64}$/;
const sha256Hex = (s: string) => createHash("sha256").update(s, "utf8").digest("hex");

// ---------------------------------------------------------------------------------------------
// Identity

export type IdentityResult =
  | { ok: true; identity: string }
  | { ok: false; reason: "unparseable_body" | "no_key" | "undecodable_key" | "not_session_key" | "no_org" | "origin_bucket" | "mixed_records" };

/** The identity of one S3 object key as S3 delivers it (URL-encoded, `+` for space). */
export function identityOfKey(rawKey: string): IdentityResult {
  const key = decodeS3Key(rawKey);
  if (!key) return { ok: false, reason: "undecodable_key" };
  const location = parseSessionKey(key);
  if (!location) return { ok: false, reason: "not_session_key" };
  const org = (parseUploadGenerationPrefix(location.prefix)?.base ?? location.prefix)?.split("/").filter(Boolean).pop();
  if (!org) return { ok: false, reason: "no_org" };
  if (location.harness === NO_SESSION_HARNESS) return { ok: false, reason: "origin_bucket" };
  return { ok: true, identity: sha256Hex(`${org}/${location.workspaceId}/${location.machineId}/${location.harness}/${location.sessionId}`) };
}

/**
 * The identity of one DLQ message body: an S3 event (`Records[*].s3.object.key`) or a reconcile body
 * (`{"reconcile":{"key"}}`). The reconcile key is passed through the same decode, which leaves an
 * already-decoded key unchanged unless it contains `%` or `+` (session keys never do). A multi-record
 * event is accepted only when every record names the same identity.
 */
export function identityOfBody(body: string): IdentityResult {
  let value: unknown;
  try {
    value = JSON.parse(body);
  } catch {
    return { ok: false, reason: "unparseable_body" };
  }
  if (!value || typeof value !== "object" || Array.isArray(value)) return { ok: false, reason: "unparseable_body" };
  const v = value as Record<string, unknown>;
  const reconcile = v[RECONCILE_BODY_KEY] as { key?: unknown } | undefined;
  if (reconcile && typeof reconcile === "object") {
    return typeof reconcile.key === "string" && reconcile.key ? identityOfKey(reconcile.key) : { ok: false, reason: "no_key" };
  }
  if (!Array.isArray(v.Records) || v.Records.length === 0) return { ok: false, reason: "no_key" };
  let first: IdentityResult | undefined;
  for (const record of v.Records as { s3?: { object?: { key?: unknown } } }[]) {
    const key = record?.s3?.object?.key;
    const result: IdentityResult = typeof key === "string" && key ? identityOfKey(key) : { ok: false, reason: "no_key" };
    if (!first) first = result;
    else if (!first.ok || !result.ok || first.identity !== result.identity) return { ok: false, reason: "mixed_records" };
  }
  return first!;
}

// ---------------------------------------------------------------------------------------------
// Selection files

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
// Options

export interface DlqResendOptions {
  dlqUrl: string;
  targetUrl: string;
  /** Identity hash -> class (the class is "selected" when none was given). */
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

/** Compare queue URLs case-insensitively on scheme and host and without a trailing slash. */
export function normalizeQueueUrl(raw: string): string {
  let url: URL;
  try {
    url = new URL(raw);
  } catch {
    return refuse("queue_url_invalid");
  }
  if (url.protocol !== "https:" || url.search || url.hash || url.username || url.password) refuse("queue_url_invalid");
  const path = url.pathname.replace(/\/+$/, "");
  if (path.split("/").filter(Boolean).length !== 2) refuse("queue_url_invalid");
  return `${url.protocol}//${url.host.toLowerCase()}${path}`;
}

/** The region in an SQS queue URL (`https://sqs.<region>.amazonaws.com/...`), or the fallback. */
export function queueRegion(queueUrl: string, fallback?: string): string {
  const host = new URL(queueUrl).host.toLowerCase();
  const m = /^sqs\.([a-z0-9-]+)\.amazonaws\.com(?:\.cn)?$/.exec(host) ?? /^([a-z0-9-]+)\.queue\.amazonaws\.com$/.exec(host);
  if (m) return m[1]!;
  if (fallback) return fallback;
  return refuse("queue_region_unknown");
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
  const dlqUrl = normalizeQueueUrl(args["dlq-url"]!);
  const targetUrl = normalizeQueueUrl(args["target-url"]!);
  if (dlqUrl === targetUrl) refuse("dlq_url_equals_target_url");
  if (dlqUrl.endsWith(".fifo") || targetUrl.endsWith(".fifo")) refuse("fifo_queue_unsupported");
  const max = intArg("max", args.max, undefined, 1, MAX_RESEND_CAP);
  const ratePerSecond = numArg("rate", args.rate, 1, 0.01, MAX_RATE_PER_SECOND);
  const visibilitySeconds = intArg("visibility_seconds", args["visibility-seconds"], DEFAULT_VISIBILITY_SECONDS, 30, 43_200);
  const emptyReceivesToStop = intArg("empty_receives", args["empty-receives"], DEFAULT_EMPTY_RECEIVES, 1, 100);
  const maxScan = args["max-scan"] === undefined ? undefined : intArg("max_scan", args["max-scan"], undefined, 1, 1_000_000);
  const waitSeconds = intArg("wait_seconds", args["wait-seconds"], 2, 0, 20);

  if (args.select && args["select-class"]) refuse("select_and_select_class_are_exclusive");
  if (!args.select && !args["select-class"]) refuse("select_or_select_class_required");
  const classes = args.classes ? parseClassesFile(readFile(args.classes)) : undefined;
  const selection = new Map<string, string>();
  if (args.select) {
    for (const hash of parseSelectFile(readFile(args.select))) selection.set(hash, classes?.get(hash) ?? "selected");
  } else {
    if (!classes) refuse("select_class_needs_classes_file");
    for (const [hash, cls] of classes!) if (cls === args["select-class"]) selection.set(hash, cls);
  }
  if (selection.size === 0) refuse("selection_empty");

  const execute = args.execute === true;
  if (execute && env[CONFIRM_ENV] !== CONFIRM_VALUE) refuse("execute_needs_DLQ_RESEND_CONFIRM");
  return { dlqUrl, targetUrl, selection, max, ratePerSecond, execute, visibilitySeconds, emptyReceivesToStop, maxScan, waitSeconds };
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
}

/** The five SQS calls the resend makes. Tests inject a fake; `awsSqsClient` is the real one. */
export interface SqsClient {
  approximateSize(queueUrl: string): Promise<number>;
  receive(queueUrl: string, args: { max: number; visibilitySeconds: number; waitSeconds: number }): Promise<SqsMessage[]>;
  send(queueUrl: string, body: string, attributes?: Record<string, SqsMessageAttribute>): Promise<void>;
  delete(queueUrl: string, receiptHandle: string): Promise<void>;
  setVisibility(queueUrl: string, receiptHandle: string, seconds: number): Promise<void>;
}

/** SQS over the JSON protocol (awsJsonCall: SigV4, no SDK). The region comes from each queue URL. */
export function awsSqsClient(credentials: () => Promise<AwsSessionCredentials>, opts: { fallbackRegion?: string; fetcher?: typeof fetch } = {}): SqsClient {
  const call = async <T>(queueUrl: string, action: string, body: Record<string, unknown>) =>
    awsJsonCall<T>({
      service: "sqs",
      target: `AmazonSQS.${action}`,
      region: queueRegion(queueUrl, opts.fallbackRegion),
      body: { QueueUrl: queueUrl, ...body },
      credentials: await credentials(),
      timeoutMs: 30_000,
      ...(opts.fetcher ? { fetcher: opts.fetcher } : {}),
    });
  return {
    async approximateSize(queueUrl) {
      const out = await call<{ Attributes?: Record<string, string> }>(queueUrl, "GetQueueAttributes", { AttributeNames: ["ApproximateNumberOfMessages"] });
      const n = Number(out.Attributes?.ApproximateNumberOfMessages ?? "0");
      return Number.isFinite(n) && n >= 0 ? n : 0;
    },
    async receive(queueUrl, { max, visibilitySeconds, waitSeconds }) {
      const out = await call<{ Messages?: SqsMessage[] }>(queueUrl, "ReceiveMessage", {
        MaxNumberOfMessages: max,
        VisibilityTimeout: visibilitySeconds,
        WaitTimeSeconds: waitSeconds,
        MessageAttributeNames: ["All"],
      });
      return out.Messages ?? [];
    },
    async send(queueUrl, body, attributes) {
      await call(queueUrl, "SendMessage", { MessageBody: body, ...(attributes && Object.keys(attributes).length ? { MessageAttributes: attributes } : {}) });
    },
    async delete(queueUrl, receiptHandle) {
      await call(queueUrl, "DeleteMessage", { ReceiptHandle: receiptHandle });
    },
    async setVisibility(queueUrl, receiptHandle, seconds) {
      await call(queueUrl, "ChangeMessageVisibility", { ReceiptHandle: receiptHandle, VisibilityTimeout: seconds });
    },
  };
}

/** Only the fields SendMessage accepts; received attributes may carry list fields or checksums. */
export function sendableAttributes(attrs: SqsMessage["MessageAttributes"]): Record<string, SqsMessageAttribute> | undefined {
  if (!attrs) return undefined;
  const out: Record<string, SqsMessageAttribute> = {};
  for (const [name, a] of Object.entries(attrs)) {
    if (!a || typeof a.DataType !== "string") continue;
    out[name] = {
      DataType: a.DataType,
      ...(typeof a.StringValue === "string" ? { StringValue: a.StringValue } : {}),
      ...(typeof a.BinaryValue === "string" ? { BinaryValue: a.BinaryValue } : {}),
    };
  }
  return Object.keys(out).length ? out : undefined;
}

// ---------------------------------------------------------------------------------------------
// The scan

export type StopReason = "cap_reached" | "empty_receives" | "max_scan" | "receive_failed";

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
  perClass: Record<string, number>;
  maxScan: number;
}

export interface DlqResendDeps {
  sqs: SqsClient;
  /** One JSON-lines record (already hash-only). */
  emit: (record: Record<string, unknown>) => void;
  now?: () => number;
  sleep?: (ms: number) => Promise<void>;
}

const errorCode = (e: unknown) => {
  const code = (e as { code?: unknown })?.code;
  return typeof code === "string" && /^[A-Za-z0-9_.:-]{1,64}$/.test(code) ? code : "error";
};
const bump = (m: Record<string, number>, k: string) => {
  m[k] = (m[k] ?? 0) + 1;
};

export async function runDlqResend(options: DlqResendOptions, deps: DlqResendDeps): Promise<DlqResendSummary> {
  const now = deps.now ?? Date.now;
  const sleep = deps.sleep ?? ((ms: number) => new Promise<void>((r) => setTimeout(r, ms)));
  const { sqs, emit } = deps;
  const mode = options.execute ? "execute" : "dry_run";
  const sizeAtStart = await sqs.approximateSize(options.dlqUrl);
  const maxScan = options.maxScan ?? Math.max(2 * sizeAtStart, 10);
  emit({ event: "start", mode, max: options.max, rate: options.ratePerSecond, visibilitySeconds: options.visibilitySeconds, dlqApproximateSize: sizeAtStart, maxScan, selectionSize: options.selection.size });

  const s: DlqResendSummary = {
    event: "summary", mode, stop: "max_scan", scanned: 0, unique: 0, selected: 0, resent: 0, wouldResend: 0, skipped: 0,
    skipReasons: {}, errors: 0, errorCodes: {}, duplicateRisk: 0, perClass: {}, maxScan,
  };
  /** Message ids already decided this run: a message seen again (visibility reset, a lapsed hold) is never acted on twice. */
  const decided = new Set<string>();
  const minIntervalMs = 1000 / options.ratePerSecond;
  let lastSendAt = -Infinity;
  let attempts = 0;
  let emptyStreak = 0;

  /** Visibility back to 0 at once; `reason` null (a dry-run would-resend) is not counted as skipped. */
  const release = async (m: SqsMessage, reason: string | null) => {
    if (reason) {
      s.skipped++;
      bump(s.skipReasons, reason);
    }
    try {
      await sqs.setVisibility(options.dlqUrl, m.ReceiptHandle, 0);
    } catch (e) {
      s.errors++;
      bump(s.errorCodes, `visibility_reset:${errorCode(e)}`);
    }
  };

  scan: while (true) {
    if (attempts >= options.max) { s.stop = "cap_reached"; break; }
    if (s.scanned >= maxScan) { s.stop = "max_scan"; break; }
    let batch: SqsMessage[];
    try {
      batch = await sqs.receive(options.dlqUrl, {
        max: Math.min(10, maxScan - s.scanned),
        visibilitySeconds: options.visibilitySeconds,
        waitSeconds: options.waitSeconds,
      });
    } catch (e) {
      s.errors++;
      bump(s.errorCodes, `receive:${errorCode(e)}`);
      s.stop = "receive_failed";
      break;
    }
    const receivedAt = now();
    // "Empty" means nothing new: a receive that returns only messages already decided this run
    // (released ones come straight back) is no progress.
    if (!batch.some((m) => !decided.has(m.MessageId))) {
      for (const m of batch) { s.scanned++; await release(m, "repeat"); }
      if (++emptyStreak >= options.emptyReceivesToStop) { s.stop = "empty_receives"; break; }
      continue;
    }
    emptyStreak = 0;
    for (const m of batch) {
      s.scanned++;
      if (decided.has(m.MessageId)) { await release(m, "repeat"); continue; }
      decided.add(m.MessageId);
      s.unique++;
      const id = identityOfBody(m.Body);
      if (!id.ok) { await release(m, id.reason); continue; }
      const cls = options.selection.get(id.identity);
      if (cls === undefined) { await release(m, "not_selected"); continue; }
      s.selected++;
      if (attempts >= options.max) { await release(m, "after_cap"); continue; }
      attempts++;
      const line = { identity: id.identity, bodySha256: sha256Hex(m.Body).slice(0, 16), class: cls };
      if (!options.execute) {
        s.wouldResend++;
        bump(s.perClass, cls);
        emit({ event: "would_resend", ...line });
        await release(m, null);
        continue;
      }
      const wait = lastSendAt + minIntervalMs - now();
      if (wait > 0) await sleep(wait);
      if (now() - receivedAt > (options.visibilitySeconds - HOLD_MARGIN_SECONDS) * 1000) {
        // The hold may lapse during the send: leave it in the DLQ rather than risk a second copy.
        s.errors++;
        bump(s.errorCodes, "hold_expiring");
        emit({ event: "send_skipped", ...line, code: "hold_expiring" });
        continue;
      }
      lastSendAt = now();
      try {
        await sqs.send(options.targetUrl, m.Body, sendableAttributes(m.MessageAttributes));
      } catch (e) {
        // Left in the DLQ (no delete); it becomes visible again when the hold ends.
        s.errors++;
        bump(s.errorCodes, `send:${errorCode(e)}`);
        emit({ event: "send_failed", ...line, code: errorCode(e) });
        continue;
      }
      s.resent++;
      bump(s.perClass, cls);
      try {
        await sqs.delete(options.dlqUrl, m.ReceiptHandle);
        emit({ event: "resent", ...line });
      } catch (e) {
        // Sent but still in the DLQ: a second copy may be resent later. The consumer is idempotent
        // on the same key and revision, so this is a counted risk, not a loss.
        s.duplicateRisk++;
        bump(s.errorCodes, `delete:${errorCode(e)}`);
        emit({ event: "resent", ...line, duplicateRisk: true, code: errorCode(e) });
      }
    }
    if (attempts >= options.max) { s.stop = "cap_reached"; break scan; }
  }
  emit({ ...s });
  return s;
}
